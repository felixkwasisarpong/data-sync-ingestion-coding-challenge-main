export interface ProgressLoggerOptions {
  startTotalIngested: number;
  intervalMs: number;
  now?: () => number;
  log?: (message: string) => void;
}

export interface ProgressLogger {
  onPage: (pageSize: number, cursor: string | null) => void;
  onFlush: (insertedCount: number, cursor: string | null) => void;
  flush: () => void;
}

export function createProgressLogger(
  options: ProgressLoggerOptions
): ProgressLogger {
  const now = options.now ?? Date.now;
  const log = options.log ?? console.log;
  const intervalMs = Math.max(1, options.intervalMs);

  const startedAtMs = now();
  let lastLoggedAtMs = startedAtMs;
  let pagesFetched = 0;
  let eventsFetched = 0;
  let insertedCount = 0;
  let flushes = 0;
  let latestCursor: string | null = null;

  const maybeLog = (force: boolean): void => {
    const currentMs = now();
    const elapsedSinceLastMs = currentMs - lastLoggedAtMs;

    if (!force && elapsedSinceLastMs < intervalMs) {
      return;
    }

    const elapsedSeconds = Math.max(0.001, (currentMs - startedAtMs) / 1000);
    const eventsPerSecond = eventsFetched / elapsedSeconds;
    const insertedPerSecond = insertedCount / elapsedSeconds;
    const totalIngested = options.startTotalIngested + insertedCount;

    log(
      `ingestion progress (pages=${pagesFetched}, events=${eventsFetched}, inserted=${insertedCount}, totalIngested=${totalIngested}, eps=${eventsPerSecond.toFixed(1)}, ips=${insertedPerSecond.toFixed(1)}, flushes=${flushes}, cursor=${latestCursor ?? "null"})`
    );

    lastLoggedAtMs = currentMs;
  };

  return {
    onPage(pageSize: number, cursor: string | null): void {
      pagesFetched += 1;
      eventsFetched += pageSize;
      latestCursor = cursor;
      maybeLog(false);
    },
    onFlush(batchInsertedCount: number, cursor: string | null): void {
      flushes += 1;
      insertedCount += batchInsertedCount;
      latestCursor = cursor;
      maybeLog(false);
    },
    flush(): void {
      maybeLog(true);
    }
  };
}
