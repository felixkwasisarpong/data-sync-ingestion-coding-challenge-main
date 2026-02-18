import { describe, expect, it, vi } from "vitest";

import { advanceCheckpoint, getCheckpointState } from "../src/db/checkpoint";

describe("getCheckpointState", () => {
  it("returns mapped checkpoint state", async () => {
    const query = vi.fn(async () => ({
      rowCount: 1,
      rows: [
        {
          id: 1,
          cursor: "abc123",
          total_ingested: "42",
          updated_at: new Date("2026-01-01T00:00:00.000Z")
        }
      ]
    }));

    const state = await getCheckpointState({ query });

    expect(state).toEqual({
      id: 1,
      cursor: "abc123",
      totalIngested: 42,
      updatedAt: "2026-01-01T00:00:00.000Z"
    });
  });

  it("throws when singleton row is missing", async () => {
    const query = vi.fn(async () => ({ rowCount: 0, rows: [] }));

    await expect(getCheckpointState({ query })).rejects.toThrow(
      "ingestion_state singleton row missing (id=1)"
    );
  });
});

describe("advanceCheckpoint", () => {
  it("updates cursor and increments total_ingested", async () => {
    const query = vi.fn(async () => ({
      rowCount: 1,
      rows: [
        {
          id: 1,
          cursor: "next-cursor",
          total_ingested: "100",
          updated_at: new Date("2026-01-02T00:00:00.000Z")
        }
      ]
    }));

    const state = await advanceCheckpoint({ query }, "next-cursor", 10);

    expect(query).toHaveBeenCalledWith(expect.stringContaining("UPDATE ingestion_state"), [
      "next-cursor",
      10
    ]);
    expect(state.totalIngested).toBe(100);
    expect(state.cursor).toBe("next-cursor");
  });

  it("rejects negative inserted counts", async () => {
    const query = vi.fn();

    await expect(advanceCheckpoint({ query }, null, -1)).rejects.toThrow(
      "insertedCount cannot be negative"
    );
    expect(query).not.toHaveBeenCalled();
  });
});
