import type { DataSyncEvent, EventsPage } from "../types";
import type { BulkWriter } from "../db/bulkWriter";
import type { PaginatedEventsClient } from "./pagination";

export interface BufferedIngestionHooks {
  onPage?: (page: EventsPage, pageNumber: number) => void;
  onFlush?: (details: {
    batchSize: number;
    insertedCount: number;
    cursor: string | null;
    flushNumber: number;
  }) => void;
}

export interface BufferedIngestionOptions {
  startCursor: string | null;
  batchSize: number;
  hooks?: BufferedIngestionHooks;
}

export interface BufferedIngestionResult {
  pagesFetched: number;
  eventsFetched: number;
  insertedCount: number;
  finalCursor: string | null;
  flushes: number;
}

function assertValidNextCursor(
  currentCursor: string | null,
  page: EventsPage
): asserts page is EventsPage & { nextCursor: string } {
  if (!page.nextCursor) {
    throw new Error("Invalid pagination state: hasMore=true but nextCursor is null");
  }

  if (page.nextCursor === currentCursor) {
    throw new Error("Invalid pagination state: cursor did not advance");
  }
}

export async function runBufferedIngestion(
  client: PaginatedEventsClient,
  writer: BulkWriter,
  options: BufferedIngestionOptions
): Promise<BufferedIngestionResult> {
  const batchSize = Math.max(1, options.batchSize);

  let cursor = options.startCursor;
  let pagesFetched = 0;
  let eventsFetched = 0;
  let insertedCount = 0;
  let flushes = 0;

  let buffer: DataSyncEvent[] = [];
  let bufferCursor = cursor;
  let pendingFlush: {
    batchSize: number;
    cursor: string | null;
    flushNumber: number;
    promise: Promise<{ insertedCount: number }>;
  } | null = null;

  const settlePendingFlush = async (): Promise<void> => {
    if (!pendingFlush) {
      return;
    }

    const current = pendingFlush;
    pendingFlush = null;

    const writeResult = await current.promise;
    insertedCount += writeResult.insertedCount;

    options.hooks?.onFlush?.({
      batchSize: current.batchSize,
      insertedCount: writeResult.insertedCount,
      cursor: current.cursor,
      flushNumber: current.flushNumber
    });
  };

  const queueFlush = async (
    events: DataSyncEvent[],
    nextCursor: string | null
  ): Promise<void> => {
    await settlePendingFlush();

    flushes += 1;
    const flushNumber = flushes;
    pendingFlush = {
      batchSize: events.length,
      cursor: nextCursor,
      flushNumber,
      promise: writer.writeBatch(events, nextCursor)
    };
  };

  while (true) {
    let page: EventsPage;

    try {
      page = await client.fetchEventsPage(cursor);
    } catch (error) {
      const isStartupCursorExpired =
        pagesFetched === 0 &&
        cursor !== null &&
        error instanceof Error &&
        error.message.includes("CURSOR_EXPIRED");

      if (isStartupCursorExpired) {
        cursor = null;
        bufferCursor = null;
        continue;
      }

      await settlePendingFlush();
      throw error;
    }

    pagesFetched += 1;
    eventsFetched += page.data.length;

    options.hooks?.onPage?.(page, pagesFetched);

    buffer.push(...page.data);
    bufferCursor = page.nextCursor;

    const shouldFlush = buffer.length >= batchSize || !page.hasMore;

    if (shouldFlush) {
      const batch = buffer;
      buffer = [];

      await queueFlush(batch, bufferCursor);
    }

    if (!page.hasMore) {
      await settlePendingFlush();
      return {
        pagesFetched,
        eventsFetched,
        insertedCount,
        finalCursor: page.nextCursor,
        flushes
      };
    }

    assertValidNextCursor(cursor, page);
    cursor = page.nextCursor;
  }
}
