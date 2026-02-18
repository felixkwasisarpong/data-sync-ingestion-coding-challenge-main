import { loadConfig } from "./config";
import { getCheckpointState } from "./db/checkpoint";
import { runMigrations } from "./db/migrations";
import { createPool } from "./db/pool";

const HEARTBEAT_INTERVAL_MS = 60_000;

interface BootstrapResult {
  cursor: string | null;
  totalIngested: number;
}

async function bootstrapDatabase(): Promise<BootstrapResult> {
  const config = loadConfig();
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
  const checkpoint = await bootstrapDatabase();
  console.log(
    `resume state loaded (cursor=${checkpoint.cursor ?? "null"}, totalIngested=${checkpoint.totalIngested})`
  );

  // Milestone 3 keeps the container alive after loading resume state.
  setInterval(() => {
    console.log("ingestion scaffold heartbeat");
  }, HEARTBEAT_INTERVAL_MS);
}

main().catch((error: unknown) => {
  console.error("ingestion scaffold failed", error);
  process.exit(1);
});
