import { describe, expect, it, vi } from "vitest";

import { runBufferedIngestion } from "../src/ingestion/bufferedIngestion";

describe("runBufferedIngestion", () => {
  it("flushes on batch threshold and final page", async () => {
    const pages = [
      {
        data: [{ eventId: "evt-1" }, { eventId: "evt-2" }],
        hasMore: true,
        nextCursor: "cursor-1"
      },
      {
        data: [{ eventId: "evt-3" }, { eventId: "evt-4" }],
        hasMore: true,
        nextCursor: "cursor-2"
      },
      {
        data: [{ eventId: "evt-5" }],
        hasMore: false,
        nextCursor: null
      }
    ];

    let index = 0;

    const client = {
      async fetchEventsPage() {
        const page = pages[index];
        index += 1;
        return page;
      }
    };

    const writeBatch = vi.fn(async (events: Array<{ eventId: string }>) => ({
      insertedCount: events.length,
      checkpoint: {
        id: 1 as const,
        cursor: null,
        totalIngested: events.length,
        updatedAt: "2026-01-01T00:00:00.000Z"
      }
    }));

    const result = await runBufferedIngestion(
      client,
      { writeBatch },
      {
        startCursor: null,
        batchSize: 3
      }
    );

    expect(result).toEqual({
      pagesFetched: 3,
      eventsFetched: 5,
      insertedCount: 5,
      finalCursor: null,
      flushes: 2
    });

    expect(writeBatch).toHaveBeenCalledTimes(2);
    expect(writeBatch.mock.calls[0][0]).toHaveLength(4);
    expect(writeBatch.mock.calls[0][1]).toBe("cursor-2");
    expect(writeBatch.mock.calls[1][0]).toHaveLength(1);
    expect(writeBatch.mock.calls[1][1]).toBeNull();
  });

  it("flushes final cursor even when terminal page is empty", async () => {
    const pages = [
      {
        data: [{ eventId: "evt-1" }, { eventId: "evt-2" }],
        hasMore: true,
        nextCursor: "cursor-1"
      },
      {
        data: [],
        hasMore: false,
        nextCursor: null
      }
    ];

    let index = 0;

    const client = {
      async fetchEventsPage() {
        const page = pages[index];
        index += 1;
        return page;
      }
    };

    const writeBatch = vi.fn(async (events: Array<{ eventId: string }>) => ({
      insertedCount: events.length,
      checkpoint: {
        id: 1 as const,
        cursor: null,
        totalIngested: events.length,
        updatedAt: "2026-01-01T00:00:00.000Z"
      }
    }));

    await runBufferedIngestion(
      client,
      { writeBatch },
      {
        startCursor: null,
        batchSize: 2
      }
    );

    expect(writeBatch).toHaveBeenCalledTimes(2);
    expect(writeBatch.mock.calls[0][0]).toHaveLength(2);
    expect(writeBatch.mock.calls[0][1]).toBe("cursor-1");
    expect(writeBatch.mock.calls[1][0]).toHaveLength(0);
    expect(writeBatch.mock.calls[1][1]).toBeNull();
  });

  it("throws on invalid non-advancing cursor", async () => {
    const client = {
      async fetchEventsPage(cursor: string | null) {
        return {
          data: [{ eventId: "evt-1" }],
          hasMore: true,
          nextCursor: cursor
        };
      }
    };

    const writeBatch = vi.fn(async () => ({
      insertedCount: 0,
      checkpoint: {
        id: 1 as const,
        cursor: null,
        totalIngested: 0,
        updatedAt: "2026-01-01T00:00:00.000Z"
      }
    }));

    await expect(
      runBufferedIngestion(
        client,
        { writeBatch },
        {
          startCursor: "cursor-1",
          batchSize: 10
        }
      )
    ).rejects.toThrow("cursor did not advance");
  });
});
