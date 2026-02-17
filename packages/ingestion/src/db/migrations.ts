import { readdir, readFile } from "node:fs/promises";
import path from "node:path";

import type { Pool, PoolClient } from "pg";

interface MigrationFile {
  name: string;
  sql: string;
}

interface MigrationQueryResult {
  rows: Array<{ name: string }>;
}

export interface QueryRunner {
  query: (text: string, values?: unknown[]) => Promise<MigrationQueryResult | unknown>;
}

export const DEFAULT_MIGRATIONS_DIR = path.resolve(
  __dirname,
  "../../migrations"
);

const CREATE_MIGRATIONS_TABLE_SQL = `
CREATE TABLE IF NOT EXISTS schema_migrations (
  name TEXT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`;

const SELECT_APPLIED_SQL = `
SELECT name
FROM schema_migrations;
`;

const INSERT_APPLIED_SQL = `
INSERT INTO schema_migrations (name)
VALUES ($1);
`;

function isSqlFile(name: string): boolean {
  return name.endsWith(".sql");
}

export async function discoverMigrations(
  migrationsDir: string
): Promise<MigrationFile[]> {
  const files = await readdir(migrationsDir);
  const sortedSqlFiles = files.filter(isSqlFile).sort((left, right) => {
    if (left < right) {
      return -1;
    }

    if (left > right) {
      return 1;
    }

    return 0;
  });

  const migrations: MigrationFile[] = [];

  for (const fileName of sortedSqlFiles) {
    const filePath = path.join(migrationsDir, fileName);
    const sql = await readFile(filePath, "utf8");
    migrations.push({ name: fileName, sql });
  }

  return migrations;
}

export async function ensureMigrationsTable(
  runner: QueryRunner
): Promise<void> {
  await runner.query(CREATE_MIGRATIONS_TABLE_SQL);
}

export async function getAppliedMigrationNames(
  runner: QueryRunner
): Promise<Set<string>> {
  const result = (await runner.query(SELECT_APPLIED_SQL)) as MigrationQueryResult;
  return new Set(result.rows.map((row) => row.name));
}

export async function applyMigration(
  client: PoolClient,
  migration: MigrationFile
): Promise<void> {
  await client.query("BEGIN");

  try {
    await client.query(migration.sql);
    await client.query(INSERT_APPLIED_SQL, [migration.name]);
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  }
}

export async function runMigrations(
  pool: Pool,
  migrationsDir: string = DEFAULT_MIGRATIONS_DIR
): Promise<number> {
  const migrations = await discoverMigrations(migrationsDir);
  const client = await pool.connect();

  try {
    await ensureMigrationsTable(client);
    const appliedNames = await getAppliedMigrationNames(client);

    let appliedCount = 0;

    for (const migration of migrations) {
      if (appliedNames.has(migration.name)) {
        continue;
      }

      await applyMigration(client, migration);
      appliedCount += 1;
      appliedNames.add(migration.name);
    }

    return appliedCount;
  } finally {
    client.release();
  }
}
