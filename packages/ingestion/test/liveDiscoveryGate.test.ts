import { describe, expect, it } from "vitest";

import { shouldRunLiveDiscovery } from "../src/ingestion/liveDiscoveryGate";

describe("shouldRunLiveDiscovery", () => {
  it("runs discovery for fresh live ingestion", () => {
    expect(shouldRunLiveDiscovery("live", 0, false)).toBe(true);
  });

  it("skips discovery for resumed live ingestion by default", () => {
    expect(shouldRunLiveDiscovery("live", 1, false)).toBe(false);
  });

  it("allows forcing discovery on resumed live ingestion", () => {
    expect(shouldRunLiveDiscovery("live", 100, true)).toBe(true);
  });

  it("never runs discovery in mock mode", () => {
    expect(shouldRunLiveDiscovery("mock", 0, true)).toBe(false);
    expect(shouldRunLiveDiscovery("mock", 100, false)).toBe(false);
  });
});
