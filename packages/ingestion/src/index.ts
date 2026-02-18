import { loadConfig } from "./config";
import { createEventsClient } from "./api/eventsClient";
import { createBulkWriter } from "./db/bulkWriter";
import { getCheckpointState } from "./db/checkpoint";
import { runMigrations } from "./db/migrations";
import { createPool } from "./db/pool";
import { runBufferedIngestion } from "./ingestion/bufferedIngestion";
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

  console.log(
    `resume state loaded (cursor=${checkpoint.cursor ?? "null"}, totalIngested=${checkpoint.totalIngested})`
  );

  try {
    const ingestionResult = await runBufferedIngestion(
      eventsClient,
      bulkWriter,
      {
        startCursor: checkpoint.cursor,
        batchSize: config.writeBatchSize,
        hooks: {
          onPage(page, pageNumber) {
            console.log(
              `page fetched (page=${pageNumber}, size=${page.data.length}, hasMore=${page.hasMore}, nextCursor=${page.nextCursor ?? "null"})`
            );
          },
          onFlush(details) {
            console.log(
              `batch flushed (flush=${details.flushNumber}, batchSize=${details.batchSize}, inserted=${details.insertedCount}, cursor=${details.cursor ?? "null"})`
            );
          }
        }
      }
    );

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
