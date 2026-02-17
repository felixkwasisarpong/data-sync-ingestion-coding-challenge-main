import { loadConfig } from "../config";
import { runMigrations } from "../db/migrations";
import { createPool } from "../db/pool";

async function main(): Promise<void> {
  const config = loadConfig();
  const pool = createPool(config.databaseUrl);

  try {
    const applied = await runMigrations(pool);
    console.log(`migrations applied: ${applied}`);
  } finally {
    await pool.end();
  }
}

main().catch((error: unknown) => {
  console.error("migration failed", error);
  process.exit(1);
});
