import { describe, expect, it } from "vitest";

import { createProgressLogger } from "../src/ingestion/progressLogger";

describe("createProgressLogger", () => {
  it("logs at configured intervals using aggregated counters", () => {
    let nowMs = 0;
    const messages: string[] = [];

    const logger = createProgressLogger({
      startTotalIngested: 100,
      intervalMs: 1000,
      now: () => nowMs,
      log: (message) => messages.push(message)
    });

    logger.onPage(1000, "cursor-1");
    logger.onFlush(900, "cursor-1");
    expect(messages).toHaveLength(0);

    nowMs = 1000;
    logger.onPage(1000, "cursor-2");

    expect(messages).toHaveLength(1);
    expect(messages[0]).toContain("pages=2");
    expect(messages[0]).toContain("events=2000");
    expect(messages[0]).toContain("inserted=900");
    expect(messages[0]).toContain("totalIngested=1000");
    expect(messages[0]).toContain("cursor=cursor-2");
  });

  it("flushes a final progress line on demand", () => {
    let nowMs = 0;
    const messages: string[] = [];

    const logger = createProgressLogger({
      startTotalIngested: 5,
      intervalMs: 5000,
      now: () => nowMs,
      log: (message) => messages.push(message)
    });

    logger.onPage(10, "cursor-final");
    logger.onFlush(10, null);
    expect(messages).toHaveLength(0);

    nowMs = 250;
    logger.flush();

    expect(messages).toHaveLength(1);
    expect(messages[0]).toContain("inserted=10");
    expect(messages[0]).toContain("totalIngested=15");
    expect(messages[0]).toContain("cursor=null");
  });
});
