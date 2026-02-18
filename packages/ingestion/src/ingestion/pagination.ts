import type { EventsPage } from "../types";

export interface PaginatedEventsClient {
  fetchEventsPage: (cursor: string | null) => Promise<EventsPage>;
}

export interface PaginationResult {
  pagesFetched: number;
  eventsFetched: number;
  finalCursor: string | null;
}

export interface PaginationHooks {
  onPage?: (page: EventsPage, pageNumber: number) => void;
}

export async function runPaginationLoop(
  client: PaginatedEventsClient,
  startCursor: string | null,
  hooks: PaginationHooks = {}
): Promise<PaginationResult> {
  let cursor = startCursor;
  let pagesFetched = 0;
  let eventsFetched = 0;

  while (true) {
    const page = await client.fetchEventsPage(cursor);
    pagesFetched += 1;
    eventsFetched += page.data.length;

    hooks.onPage?.(page, pagesFetched);

    if (!page.hasMore) {
      return {
        pagesFetched,
        eventsFetched,
        finalCursor: page.nextCursor
      };
    }

    if (!page.nextCursor) {
      throw new Error("Invalid pagination state: hasMore=true but nextCursor is null");
    }

    if (page.nextCursor === cursor) {
      throw new Error("Invalid pagination state: cursor did not advance");
    }

    cursor = page.nextCursor;
  }
}
