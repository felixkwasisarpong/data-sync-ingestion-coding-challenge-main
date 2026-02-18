import { describe, expect, it, vi } from "vitest";

import {
  MAX_INSERT_EVENTS_PER_STATEMENT,
  buildBulkInsertStatement,
  createBulkWriter,
  dedupeEventsById,
  writeBatchWithClient
} from "../src/db/bulkWriter";

describe("buildBulkInsertStatement", () => {
  it("builds UNNEST insert with column arrays", () => {
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

    expect(statement.sql).toContain(
      "FROM UNNEST($1::text[], $2::timestamptz[], $3::jsonb[])"
    );
    expect(statement.sql).toContain("ON CONFLICT (event_id) DO NOTHING");
    expect(statement.sql).not.toContain("VALUES");
    expect(statement.sql).not.toContain("RETURNING");

    expect(statement.values).toEqual([
      ["evt-1", "evt-2"],
      ["2026-01-01T00:00:00.000Z", null],
      [
        JSON.stringify({
          eventId: "evt-1",
          occurredAt: "2026-01-01T00:00:00.000Z",
          foo: "bar"
        }),
        JSON.stringify({
          eventId: "evt-2",
          occurredAt: "invalid-timestamp",
          alpha: 1
        })
      ]
    ]);
  });

  it("rejects empty batches", () => {
    expect(() => buildBulkInsertStatement([])).toThrow(
      "Cannot build insert statement for empty event batch"
    );
  });
});

describe("dedupeEventsById", () => {
  it("preserves first occurrence order while removing duplicates", () => {
    const deduped = dedupeEventsById([
      { eventId: "evt-1", foo: "a" },
      { eventId: "evt-2", foo: "b" },
      { eventId: "evt-1", foo: "c" },
      { eventId: "evt-3", foo: "d" },
      { eventId: "evt-2", foo: "e" }
    ]);

    expect(deduped).toEqual([
      { eventId: "evt-1", foo: "a" },
      { eventId: "evt-2", foo: "b" },
      { eventId: "evt-3", foo: "d" }
    ]);
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

  it("chunks large inserts and aggregates inserted count", async () => {
    const query = vi.fn(async (text: string, values?: unknown[]) => {
      if (text === "BEGIN" || text === "COMMIT") {
        return { rowCount: null, rows: [] };
      }

      if (text.includes("INSERT INTO ingested_events")) {
        const chunkSize = Array.isArray(values?.[0])
          ? (values?.[0] as unknown[]).length
          : 0;
        return { rowCount: chunkSize, rows: [] };
      }

      if (text.includes("UPDATE ingestion_state")) {
        return {
          rowCount: 1,
          rows: [
            {
              id: 1,
              cursor: "cursor-large",
              total_ingested: String(MAX_INSERT_EVENTS_PER_STATEMENT + 7),
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

    const events = Array.from(
      { length: MAX_INSERT_EVENTS_PER_STATEMENT + 7 },
      (_, index) => ({
        eventId: `evt-${index + 1}`,
        occurredAt: null as null
      })
    );

    const result = await writeBatchWithClient(
      client as never,
      events,
      "cursor-large"
    );

    const insertCalls = query.mock.calls.filter((call) =>
      String(call[0]).includes("INSERT INTO ingested_events")
    );

    expect(insertCalls).toHaveLength(2);
    expect(((insertCalls[0][1] as unknown[])[0] as unknown[]).length).toBe(
      MAX_INSERT_EVENTS_PER_STATEMENT
    );
    expect(((insertCalls[1][1] as unknown[])[0] as unknown[]).length).toBe(7);
    expect(result.insertedCount).toBe(MAX_INSERT_EVENTS_PER_STATEMENT + 7);
    expect(query).toHaveBeenCalledWith(
      expect.stringContaining("UPDATE ingestion_state"),
      ["cursor-large", MAX_INSERT_EVENTS_PER_STATEMENT + 7]
    );
  });

  it("deduplicates duplicate event ids before insert", async () => {
    const query = vi.fn(async (text: string, values?: unknown[]) => {
      if (text === "BEGIN" || text === "COMMIT") {
        return { rowCount: null, rows: [] };
      }

      if (text.includes("INSERT INTO ingested_events")) {
        const eventIds = (values?.[0] as string[]) ?? [];
        return { rowCount: eventIds.length, rows: [] };
      }

      if (text.includes("UPDATE ingestion_state")) {
        return {
          rowCount: 1,
          rows: [
            {
              id: 1,
              cursor: "cursor-dedupe",
              total_ingested: "2",
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
      [
        { eventId: "evt-1", occurredAt: null },
        { eventId: "evt-1", occurredAt: null },
        { eventId: "evt-2", occurredAt: null }
      ],
      "cursor-dedupe"
    );

    const insertCalls = query.mock.calls.filter((call) =>
      String(call[0]).includes("INSERT INTO ingested_events")
    );

    expect(insertCalls).toHaveLength(1);
    expect(((insertCalls[0][1] as unknown[])[0] as unknown[]).length).toBe(2);
    expect(result.insertedCount).toBe(2);
    expect(query).toHaveBeenCalledWith(
      expect.stringContaining("UPDATE ingestion_state"),
      ["cursor-dedupe", 2]
    );
  });
});

describe("createBulkWriter", () => {
  it("reuses one pool client across multiple writes and releases on close", async () => {
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
              total_ingested: "0",
              updated_at: new Date("2026-01-01T00:00:00.000Z")
            }
          ]
        };
      }

      throw new Error(`Unexpected SQL: ${text}`);
    });

    const release = vi.fn();
    const client = { query, release };
    const connect = vi.fn(async () => client);
    const pool = { connect };

    const writer = createBulkWriter(pool as never);

    await writer.writeBatch([], null);
    await writer.writeBatch([], null);

    expect(connect).toHaveBeenCalledTimes(1);
    expect(release).not.toHaveBeenCalled();

    await writer.close?.();
    expect(release).toHaveBeenCalledTimes(1);
  });

  it("reconnects after a failed write on cached client", async () => {
    const firstClient = {
      query: vi.fn(async (text: string) => {
        if (text === "BEGIN") {
          return { rowCount: null, rows: [] };
        }

        if (text.includes("UPDATE ingestion_state")) {
          throw new Error("checkpoint failure");
        }

        if (text === "ROLLBACK") {
          return { rowCount: null, rows: [] };
        }

        throw new Error(`Unexpected SQL (first client): ${text}`);
      }),
      release: vi.fn()
    };

    const secondClient = {
      query: vi.fn(async (text: string) => {
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
                total_ingested: "0",
                updated_at: new Date("2026-01-01T00:00:00.000Z")
              }
            ]
          };
        }

        throw new Error(`Unexpected SQL (second client): ${text}`);
      }),
      release: vi.fn()
    };

    const connect = vi
      .fn()
      .mockResolvedValueOnce(firstClient)
      .mockResolvedValueOnce(secondClient);
    const pool = { connect };

    const writer = createBulkWriter(pool as never);

    await expect(writer.writeBatch([], null)).rejects.toThrow("checkpoint failure");
    expect(firstClient.release).toHaveBeenCalledTimes(1);

    await writer.writeBatch([], null);
    expect(connect).toHaveBeenCalledTimes(2);

    await writer.close?.();
    expect(secondClient.release).toHaveBeenCalledTimes(1);
  });
});
