import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import { applyMigration, discoverMigrations, runMigrations } from "../src/db/migrations";

async function createTempDir(): Promise<string> {
  return mkdtemp(path.join(os.tmpdir(), "datasync-migrations-"));
}

describe("discoverMigrations", () => {
  const cleanupDirs: string[] = [];

  afterEach(async () => {
    await Promise.all(cleanupDirs.map((dir) => rm(dir, { recursive: true, force: true })));
    cleanupDirs.length = 0;
  });

  it("loads SQL files in lexical order", async () => {
    const dir = await createTempDir();
    cleanupDirs.push(dir);

    await writeFile(path.join(dir, "0002_second.sql"), "SELECT 2;");
    await writeFile(path.join(dir, "0001_first.sql"), "SELECT 1;");
    await writeFile(path.join(dir, "notes.txt"), "ignore");

    const migrations = await discoverMigrations(dir);

    expect(migrations.map((migration) => migration.name)).toEqual([
      "0001_first.sql",
      "0002_second.sql"
    ]);
  });
});

describe("applyMigration", () => {
  it("wraps each migration in a transaction", async () => {
    const query = vi.fn(async () => ({ rows: [] }));
    const client = {
      query,
      release: vi.fn()
    } as const;

    await applyMigration(client as never, {
      name: "0001_init.sql",
      sql: "SELECT 1;"
    });

    expect(query).toHaveBeenNthCalledWith(1, "BEGIN");
    expect(query).toHaveBeenNthCalledWith(2, "SELECT 1;");
    expect(query).toHaveBeenNthCalledWith(
      3,
      expect.stringContaining("INSERT INTO schema_migrations"),
      ["0001_init.sql"]
    );
    expect(query).toHaveBeenNthCalledWith(4, "COMMIT");
  });
});

describe("runMigrations", () => {
  const cleanupDirs: string[] = [];

  afterEach(async () => {
    await Promise.all(cleanupDirs.map((dir) => rm(dir, { recursive: true, force: true })));
    cleanupDirs.length = 0;
  });

  it("applies only pending migrations", async () => {
    const dir = await createTempDir();
    cleanupDirs.push(dir);

    await writeFile(path.join(dir, "0001_init.sql"), "SELECT 1;");
    await writeFile(path.join(dir, "0002_add_index.sql"), "SELECT 2;");

    const query = vi.fn(async (text: string) => {
      if (text.includes("SELECT name")) {
        return { rows: [{ name: "0001_init.sql" }] };
      }

      return { rows: [] };
    });

    const release = vi.fn();
    const pool = {
      connect: vi.fn(async () => ({ query, release }))
    };

    const appliedCount = await runMigrations(pool as never, dir);

    expect(appliedCount).toBe(1);
    expect(query).toHaveBeenCalledWith(expect.stringContaining("CREATE TABLE IF NOT EXISTS schema_migrations"));
    expect(query).toHaveBeenCalledWith(expect.stringContaining("SELECT name"));
    expect(query).toHaveBeenCalledWith("BEGIN");
    expect(query).toHaveBeenCalledWith("SELECT 2;");
    expect(query).toHaveBeenCalledWith(expect.stringContaining("INSERT INTO schema_migrations"), ["0002_add_index.sql"]);
    expect(query).toHaveBeenCalledWith("COMMIT");
    expect(release).toHaveBeenCalledOnce();
  });
});
