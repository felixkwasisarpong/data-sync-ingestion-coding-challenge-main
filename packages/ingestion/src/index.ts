import { loadConfig } from "./config";
import { createEventsClient } from "./api/eventsClient";
import { runLiveDiscovery } from "./api/liveDiscovery";
import { createBulkWriter } from "./db/bulkWriter";
import { getCheckpointState } from "./db/checkpoint";
import { runMigrations } from "./db/migrations";
import { createPool } from "./db/pool";
import { runBufferedIngestion } from "./ingestion/bufferedIngestion";
import { shouldRunLiveDiscovery } from "./ingestion/liveDiscoveryGate";
import { createProgressLogger } from "./ingestion/progressLogger";
import type { IngestionConfig } from "./types";

interface BootstrapResult {
  cursor: string | null;
  totalIngested: number;
}

async function bootstrapDatabase(config: IngestionConfig): Promise<BootstrapResult> {
  const pool = createPool(config.databaseUrl);

  try {
    const applied = await runMigrations(pool);
    const state = await getCheckpointState(pool);

    console.log(
      `ingestion scaffold started (mode=${config.apiMode}, migrationsApplied=${applied}, cursor=${state.cursor ?? "null"}, totalIngested=${state.totalIngested})`
    );

    return {
      cursor: state.cursor,
      totalIngested: state.totalIngested
    };
  } finally {
    await pool.end();
  }
}

async function main(): Promise<void> {
  const config = loadConfig();
  const checkpoint = await bootstrapDatabase(config);
  const pool = createPool(config.databaseUrl);
  const eventsClient = createEventsClient(config);
  const bulkWriter = createBulkWriter(pool);
  const progressLogger = createProgressLogger({
    startTotalIngested: checkpoint.totalIngested,
    intervalMs: config.progressLogIntervalMs
  });

  console.log(
    `resume state loaded (cursor=${checkpoint.cursor ?? "null"}, totalIngested=${checkpoint.totalIngested})`
  );

  if (
    shouldRunLiveDiscovery(
      config.apiMode,
      checkpoint.totalIngested,
      config.liveDiscoveryOnResume
    )
  ) {
    const discovery = await runLiveDiscovery(config);
    console.log(
      `live discovery complete (limit=5, sampleSize=${discovery.sampleSize}, hasMore=${discovery.hasMore}, nextCursor=${discovery.nextCursor ?? "null"})`
    );
    console.log(`live discovery headers: ${JSON.stringify(discovery.headers)}`);
    console.log(
      `live discovery response shape: ${JSON.stringify(discovery.responseShape)}`
    );
  } else if (config.apiMode === "live") {
    console.log(
      `live discovery skipped (resume=true, totalIngested=${checkpoint.totalIngested})`
    );
  }

  try {
    const ingestionResult = await runBufferedIngestion(
      eventsClient,
      bulkWriter,
      {
        startCursor: checkpoint.cursor,
        batchSize: config.writeBatchSize,
        hooks: {
          onPage(page, pageNumber) {
            progressLogger.onPage(page.data.length, page.nextCursor);
            if (config.logLevel === "debug") {
              console.log(
                `page fetched (page=${pageNumber}, size=${page.data.length}, hasMore=${page.hasMore}, nextCursor=${page.nextCursor ?? "null"})`
              );
            }
          },
          onFlush(details) {
            progressLogger.onFlush(details.insertedCount, details.cursor);
            if (config.logLevel === "debug") {
              console.log(
                `batch flushed (flush=${details.flushNumber}, batchSize=${details.batchSize}, inserted=${details.insertedCount}, cursor=${details.cursor ?? "null"})`
              );
            }
          }
        }
      }
    );

    progressLogger.flush();

    console.log(
      `buffered ingestion loop complete (pages=${ingestionResult.pagesFetched}, events=${ingestionResult.eventsFetched}, inserted=${ingestionResult.insertedCount}, flushes=${ingestionResult.flushes}, finalCursor=${ingestionResult.finalCursor ?? "null"})`
    );
  } finally {
    await pool.end();
  }

  console.log("ingestion complete");
}

main().catch((error: unknown) => {
  console.error("ingestion scaffold failed", error);
  process.exit(1);
});
