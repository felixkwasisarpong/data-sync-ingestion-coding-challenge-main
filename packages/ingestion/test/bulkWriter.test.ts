import { describe, expect, it, vi } from "vitest";

import {
  buildBulkInsertStatement,
  writeBatchWithClient
} from "../src/db/bulkWriter";

describe("buildBulkInsertStatement", () => {
  it("builds multi-row insert with positional parameters", () => {
    const statement = buildBulkInsertStatement([
      {
        eventId: "evt-1",
        occurredAt: "2026-01-01T00:00:00.000Z",
        foo: "bar"
      },
      {
        eventId: "evt-2",
        occurredAt: "invalid-timestamp",
        alpha: 1
      }
    ]);

    expect(statement.sql).toContain("($1, $2, $3)");
    expect(statement.sql).toContain("($4, $5, $6)");
    expect(statement.sql).toContain("ON CONFLICT (event_id) DO NOTHING");
    expect(statement.sql).not.toContain("RETURNING");

    expect(statement.values).toEqual([
      "evt-1",
      "2026-01-01T00:00:00.000Z",
      {
        eventId: "evt-1",
        occurredAt: "2026-01-01T00:00:00.000Z",
        foo: "bar"
      },
      "evt-2",
      null,
      {
        eventId: "evt-2",
        occurredAt: "invalid-timestamp",
        alpha: 1
      }
    ]);
  });

  it("rejects empty batches", () => {
    expect(() => buildBulkInsertStatement([])).toThrow(
      "Cannot build insert statement for empty event batch"
    );
  });
});

describe("writeBatchWithClient", () => {
  it("wraps insert and checkpoint update in a transaction", async () => {
    const query = vi.fn(async (text: string) => {
      if (text === "BEGIN" || text === "COMMIT") {
        return { rowCount: null, rows: [] };
      }

      if (text.includes("INSERT INTO ingested_events")) {
        return { rowCount: 1, rows: [] };
      }

      if (text.includes("UPDATE ingestion_state")) {
        return {
          rowCount: 1,
          rows: [
            {
              id: 1,
              cursor: "cursor-2",
              total_ingested: "101",
              updated_at: new Date("2026-01-01T00:00:00.000Z")
            }
          ]
        };
      }

      throw new Error(`Unexpected SQL: ${text}`);
    });

    const client = {
      query,
      release: vi.fn()
    };

    const result = await writeBatchWithClient(
      client as never,
      [{ eventId: "evt-1", occurredAt: null }],
      "cursor-2"
    );

    expect(result.insertedCount).toBe(1);
    expect(result.checkpoint.cursor).toBe("cursor-2");
    expect(result.checkpoint.totalIngested).toBe(101);

    expect(query).toHaveBeenNthCalledWith(1, "BEGIN");
    expect(query).toHaveBeenNthCalledWith(
      2,
      expect.stringContaining("INSERT INTO ingested_events"),
      expect.any(Array)
    );
    expect(query).toHaveBeenNthCalledWith(
      3,
      expect.stringContaining("UPDATE ingestion_state"),
      ["cursor-2", 1]
    );
    expect(query).toHaveBeenNthCalledWith(4, "COMMIT");
  });

  it("updates checkpoint even when batch is empty", async () => {
    const query = vi.fn(async (text: string) => {
      if (text === "BEGIN" || text === "COMMIT") {
        return { rowCount: null, rows: [] };
      }

      if (text.includes("UPDATE ingestion_state")) {
        return {
          rowCount: 1,
          rows: [
            {
              id: 1,
              cursor: null,
              total_ingested: "101",
              updated_at: new Date("2026-01-01T00:00:00.000Z")
            }
          ]
        };
      }

      throw new Error(`Unexpected SQL: ${text}`);
    });

    const client = {
      query,
      release: vi.fn()
    };

    const result = await writeBatchWithClient(client as never, [], null);

    expect(result.insertedCount).toBe(0);
    expect(result.checkpoint.cursor).toBeNull();
    expect(query).not.toHaveBeenCalledWith(
      expect.stringContaining("INSERT INTO ingested_events"),
      expect.any(Array)
    );
  });

  it("rolls back on errors", async () => {
    const query = vi.fn(async (text: string) => {
      if (text === "BEGIN") {
        return { rowCount: null, rows: [] };
      }

      if (text.includes("INSERT INTO ingested_events")) {
        throw new Error("insert failed");
      }

      if (text === "ROLLBACK") {
        return { rowCount: null, rows: [] };
      }

      throw new Error(`Unexpected SQL: ${text}`);
    });

    const client = {
      query,
      release: vi.fn()
    };

    await expect(
      writeBatchWithClient(client as never, [{ eventId: "evt-1" }], "cursor-1")
    ).rejects.toThrow("insert failed");

    expect(query).toHaveBeenCalledWith("ROLLBACK");
  });

  it("uses rowCount as inserted count when conflicts are ignored", async () => {
    const query = vi.fn(async (text: string) => {
      if (text === "BEGIN" || text === "COMMIT") {
        return { rowCount: null, rows: [] };
      }

      if (text.includes("INSERT INTO ingested_events")) {
        return { rowCount: 0, rows: [] };
      }

      if (text.includes("UPDATE ingestion_state")) {
        return {
          rowCount: 1,
          rows: [
            {
              id: 1,
              cursor: "cursor-2",
              total_ingested: "101",
              updated_at: new Date("2026-01-01T00:00:00.000Z")
            }
          ]
        };
      }

      throw new Error(`Unexpected SQL: ${text}`);
    });

    const client = {
      query,
      release: vi.fn()
    };

    const result = await writeBatchWithClient(
      client as never,
      [{ eventId: "evt-duplicate", occurredAt: null }],
      "cursor-2"
    );

    expect(result.insertedCount).toBe(0);
    expect(query).toHaveBeenNthCalledWith(
      3,
      expect.stringContaining("UPDATE ingestion_state"),
      ["cursor-2", 0]
    );
  });
});
