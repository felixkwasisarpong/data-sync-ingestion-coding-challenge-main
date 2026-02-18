import { describe, expect, it } from "vitest";

import { runPaginationLoop } from "../src/ingestion/pagination";

describe("runPaginationLoop", () => {
  it("fetches pages until hasMore is false", async () => {
    const pages = [
      {
        data: [{ eventId: "evt-1" }],
        hasMore: true,
        nextCursor: "cursor-1"
      },
      {
        data: [{ eventId: "evt-2" }, { eventId: "evt-3" }],
        hasMore: false,
        nextCursor: null
      }
    ];

    let index = 0;

    const client = {
      async fetchEventsPage(): Promise<(typeof pages)[number]> {
        const page = pages[index];
        index += 1;
        return page;
      }
    };

    const result = await runPaginationLoop(client, null);

    expect(result).toEqual({
      pagesFetched: 2,
      eventsFetched: 3,
      finalCursor: null
    });
  });

  it("throws when hasMore=true without nextCursor", async () => {
    const client = {
      async fetchEventsPage() {
        return {
          data: [{ eventId: "evt-1" }],
          hasMore: true,
          nextCursor: null
        };
      }
    };

    await expect(runPaginationLoop(client, null)).rejects.toThrow(
      "hasMore=true but nextCursor is null"
    );
  });

  it("throws when cursor does not advance", async () => {
    const client = {
      async fetchEventsPage(cursor: string | null) {
        return {
          data: [{ eventId: "evt-1" }],
          hasMore: true,
          nextCursor: cursor
        };
      }
    };

    await expect(runPaginationLoop(client, "cursor-1")).rejects.toThrow(
      "cursor did not advance"
    );
  });
});
