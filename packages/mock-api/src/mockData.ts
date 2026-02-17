export interface MockEvent {
  eventId: string;
  occurredAt: string;
  eventType: string;
  source: string;
}

export interface MockPage {
  data: MockEvent[];
  hasMore: boolean;
  nextCursor: string | null;
}

export function buildMockEvents(total: number): MockEvent[] {
  const events: MockEvent[] = [];

  for (let index = 0; index < total; index += 1) {
    events.push({
      eventId: `evt-${index.toString().padStart(7, "0")}`,
      occurredAt: new Date(1704067200000 + index * 1000).toISOString(),
      eventType: index % 2 === 0 ? "click" : "view",
      source: "mock-api"
    });
  }

  return events;
}

function encodeCursor(value: number): string {
  return Buffer.from(String(value), "utf8").toString("base64");
}

function decodeCursor(cursor: string): number {
  const decoded = Buffer.from(cursor, "base64").toString("utf8");
  const parsed = Number.parseInt(decoded, 10);

  if (Number.isNaN(parsed) || parsed < 0) {
    throw new Error(`Invalid cursor: ${cursor}`);
  }

  return parsed;
}

export function paginateEvents(
  events: MockEvent[],
  limit: number,
  cursor: string | null
): MockPage {
  const startIndex = cursor ? decodeCursor(cursor) : 0;
  const endIndex = Math.min(startIndex + Math.max(limit, 1), events.length);
  const data = events.slice(startIndex, endIndex);
  const hasMore = endIndex < events.length;

  return {
    data,
    hasMore,
    nextCursor: hasMore ? encodeCursor(endIndex) : null
  };
}
