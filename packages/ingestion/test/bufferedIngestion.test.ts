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

  it("falls back to null cursor when startup cursor is expired", async () => {
    let firstCall = true;

    const client = {
      async fetchEventsPage(cursor: string | null) {
        if (firstCall) {
          firstCall = false;
          if (cursor !== "expired-cursor") {
            throw new Error("expected initial expired cursor");
          }
          throw new Error(
            "Events API request failed with status 400: {\"code\":\"CURSOR_EXPIRED\"}"
          );
        }

        return {
          data: [{ eventId: "evt-1" }],
          hasMore: false,
          nextCursor: null
        };
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
        startCursor: "expired-cursor",
        batchSize: 1000
      }
    );

    expect(result.eventsFetched).toBe(1);
    expect(writeBatch).toHaveBeenCalledTimes(1);
    expect(writeBatch.mock.calls[0][1]).toBeNull();
  });

  it("overlaps fetches with pending batch write", async () => {
    const pages = [
      {
        data: [{ eventId: "evt-1" }],
        hasMore: true,
        nextCursor: "cursor-1"
      },
      {
        data: [{ eventId: "evt-2" }],
        hasMore: true,
        nextCursor: "cursor-2"
      },
      {
        data: [{ eventId: "evt-3" }],
        hasMore: false,
        nextCursor: null
      }
    ];

    let pageIndex = 0;
    const fetchHistory: number[] = [];

    const client = {
      async fetchEventsPage() {
        fetchHistory.push(pageIndex);
        const page = pages[pageIndex];
        pageIndex += 1;
        return page;
      }
    };

    let resolveFirstWrite: (() => void) | null = null;
    const firstWriteDone = new Promise<void>((resolve) => {
      resolveFirstWrite = resolve;
    });

    const writeBatch = vi
      .fn()
      .mockImplementationOnce(
        async (events: Array<{ eventId: string }>) =>
          await new Promise((resolve) => {
            firstWriteDone.then(() =>
              resolve({
                insertedCount: events.length,
                checkpoint: {
                  id: 1 as const,
                  cursor: "cursor-2",
                  totalIngested: events.length,
                  updatedAt: "2026-01-01T00:00:00.000Z"
                }
              })
            );
          })
      )
      .mockImplementationOnce(async (events: Array<{ eventId: string }>) => ({
        insertedCount: events.length,
        checkpoint: {
          id: 1 as const,
          cursor: null,
          totalIngested: events.length,
          updatedAt: "2026-01-01T00:00:00.000Z"
        }
      }));

    const ingestionPromise = runBufferedIngestion(
      client,
      { writeBatch },
      {
        startCursor: null,
        batchSize: 2
      }
    );

    await vi.waitFor(() => {
      expect(fetchHistory).toContain(2);
    });

    expect(writeBatch).toHaveBeenCalledTimes(1);

    resolveFirstWrite?.();

    const result = await ingestionPromise;

    expect(result.eventsFetched).toBe(3);
    expect(result.insertedCount).toBe(3);
    expect(writeBatch).toHaveBeenCalledTimes(2);
  });
});
