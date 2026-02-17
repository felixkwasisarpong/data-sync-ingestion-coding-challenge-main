import { loadConfig } from "./config";
import { runMigrations } from "./db/migrations";
import { createPool } from "./db/pool";

const HEARTBEAT_INTERVAL_MS = 60_000;

interface IngestionStateRow {
  id: number;
  cursor: string | null;
  total_ingested: string;
  updated_at: Date;
}

async function bootstrapDatabase(): Promise<void> {
  const config = loadConfig();
  const pool = createPool(config.databaseUrl);

  try {
    const applied = await runMigrations(pool);

    const result = await pool.query<IngestionStateRow>(
      `
      SELECT id, cursor, total_ingested, updated_at
      FROM ingestion_state
      WHERE id = 1;
      `
    );

    if (result.rowCount !== 1) {
      throw new Error("ingestion_state singleton row missing (id=1)");
    }

    const state = result.rows[0];

    console.log(
      `ingestion scaffold started (mode=${config.apiMode}, migrationsApplied=${applied}, cursor=${state.cursor ?? "null"}, totalIngested=${state.total_ingested})`
    );
  } finally {
    await pool.end();
  }
}

async function main(): Promise<void> {
  await bootstrapDatabase();

  // Milestone 2 keeps the container alive after DB bootstrap.
  setInterval(() => {
    console.log("ingestion scaffold heartbeat");
  }, HEARTBEAT_INTERVAL_MS);
}

main().catch((error: unknown) => {
  console.error("ingestion scaffold failed", error);
  process.exit(1);
});
