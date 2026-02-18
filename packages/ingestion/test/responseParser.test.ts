import { describe, expect, it } from "vitest";

import { parseEventsPage } from "../src/api/responseParser";

describe("parseEventsPage", () => {
  it("parses valid response payload", () => {
    const parsed = parseEventsPage({
      data: [
        {
          eventId: "evt-1",
          occurredAt: "2026-01-01T00:00:00.000Z",
          eventType: "click"
        }
      ],
      hasMore: false,
      nextCursor: null
    });

    expect(parsed.data[0].eventId).toBe("evt-1");
    expect(parsed.data[0].occurredAt).toBe("2026-01-01T00:00:00.000Z");
    expect(parsed.hasMore).toBe(false);
    expect(parsed.nextCursor).toBeNull();
  });

  it("accepts alternate event id key", () => {
    const parsed = parseEventsPage({
      data: [{ event_id: "evt-2", timestamp: "2026-01-01T00:00:01.000Z" }],
      hasMore: true,
      nextCursor: "abc"
    });

    expect(parsed.data[0].eventId).toBe("evt-2");
    expect(parsed.data[0].occurredAt).toBe("2026-01-01T00:00:01.000Z");
  });

  it("throws for invalid response shape", () => {
    expect(() =>
      parseEventsPage({
        data: {},
        hasMore: false,
        nextCursor: null
      })
    ).toThrow("data must be an array");
  });

  it("parses live payload shape with nested pagination and numeric timestamp", () => {
    const parsed = parseEventsPage({
      data: [
        {
          id: "evt-live-1",
          timestamp: 1769541612369
        }
      ],
      pagination: {
        hasMore: true,
        nextCursor: "next-live-cursor"
      }
    });

    expect(parsed.data[0].eventId).toBe("evt-live-1");
    expect(parsed.data[0].occurredAt).toBe("2026-01-27T19:20:12.369Z");
    expect(parsed.hasMore).toBe(true);
    expect(parsed.nextCursor).toBe("next-live-cursor");
  });

  it("parses snake_case pagination fields", () => {
    const parsed = parseEventsPage({
      data: [{ id: "evt-live-2", timestamp: "1769541612369" }],
      has_more: "true",
      next_cursor: "snake-cursor"
    });

    expect(parsed.data[0].eventId).toBe("evt-live-2");
    expect(parsed.data[0].occurredAt).toBe("2026-01-27T19:20:12.369Z");
    expect(parsed.hasMore).toBe(true);
    expect(parsed.nextCursor).toBe("snake-cursor");
  });

  it("normalizes microsecond and nanosecond timestamps", () => {
    const parsed = parseEventsPage({
      data: [
        { id: "evt-micros", timestamp: 1769541612369000 },
        { id: "evt-nanos", timestamp: 1769541612369000000 }
      ],
      hasMore: false,
      nextCursor: null
    });

    expect(parsed.data[0].occurredAt).toBe("2026-01-27T19:20:12.369Z");
    expect(parsed.data[1].occurredAt).toBe("2026-01-27T19:20:12.369Z");
  });

  it("does not throw for invalid numeric timestamps", () => {
    const parsed = parseEventsPage({
      data: [{ id: "evt-invalid-ts", timestamp: Number.POSITIVE_INFINITY }],
      hasMore: false,
      nextCursor: null
    });

    expect(parsed.data[0].eventId).toBe("evt-invalid-ts");
    expect(parsed.data[0].occurredAt).toBeNull();
  });
});
