import { describe, expect, it } from "vitest";

import { buildMockEvents, paginateEvents } from "../src/mockData";

describe("paginateEvents", () => {
  it("advances cursor until hasMore is false", () => {
    const events = buildMockEvents(5);

    const pageOne = paginateEvents(events, 2, null);
    const pageTwo = paginateEvents(events, 2, pageOne.nextCursor);
    const pageThree = paginateEvents(events, 2, pageTwo.nextCursor);

    expect(pageOne.data).toHaveLength(2);
    expect(pageOne.hasMore).toBe(true);

    expect(pageTwo.data).toHaveLength(2);
    expect(pageTwo.hasMore).toBe(true);

    expect(pageThree.data).toHaveLength(1);
    expect(pageThree.hasMore).toBe(false);
    expect(pageThree.nextCursor).toBeNull();
  });
});
