import { loadConfig } from "./config";
import { createEventsClient } from "./api/eventsClient";
import { getCheckpointState } from "./db/checkpoint";
import { runMigrations } from "./db/migrations";
import { createPool } from "./db/pool";
import { runPaginationLoop } from "./ingestion/pagination";
import type { IngestionConfig } from "./types";

const HEARTBEAT_INTERVAL_MS = 60_000;

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
  const eventsClient = createEventsClient(config);

  console.log(
    `resume state loaded (cursor=${checkpoint.cursor ?? "null"}, totalIngested=${checkpoint.totalIngested})`
  );

  const paginationResult = await runPaginationLoop(
    eventsClient,
    checkpoint.cursor,
    {
      onPage(page, pageNumber) {
        console.log(
          `page fetched (page=${pageNumber}, size=${page.data.length}, hasMore=${page.hasMore}, nextCursor=${page.nextCursor ?? "null"})`
        );
      }
    }
  );

  console.log(
    `pagination loop complete (pages=${paginationResult.pagesFetched}, events=${paginationResult.eventsFetched}, finalCursor=${paginationResult.finalCursor ?? "null"})`
  );

  // Milestone 4 keeps the container alive after pagination loop validation.
  setInterval(() => {
    console.log("ingestion scaffold heartbeat");
  }, HEARTBEAT_INTERVAL_MS);
}

main().catch((error: unknown) => {
  console.error("ingestion scaffold failed", error);
  process.exit(1);
});
