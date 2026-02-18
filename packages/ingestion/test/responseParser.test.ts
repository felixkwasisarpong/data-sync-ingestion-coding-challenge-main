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
});
