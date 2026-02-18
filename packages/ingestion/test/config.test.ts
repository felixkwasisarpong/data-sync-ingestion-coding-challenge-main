import { afterEach, describe, expect, it } from "vitest";

import { loadConfig } from "../src/config";

const originalApiMode = process.env.API_MODE;
const originalApiPageLimit = process.env.API_PAGE_LIMIT;
const originalLiveDiscoveryOnResume = process.env.LIVE_DISCOVERY_ON_RESUME;

afterEach(() => {
  if (originalApiMode === undefined) {
    delete process.env.API_MODE;
  } else {
    process.env.API_MODE = originalApiMode;
  }

  if (originalApiPageLimit === undefined) {
    delete process.env.API_PAGE_LIMIT;
  } else {
    process.env.API_PAGE_LIMIT = originalApiPageLimit;
  }

  if (originalLiveDiscoveryOnResume === undefined) {
    delete process.env.LIVE_DISCOVERY_ON_RESUME;
  } else {
    process.env.LIVE_DISCOVERY_ON_RESUME = originalLiveDiscoveryOnResume;
  }
});

describe("loadConfig", () => {
  it("uses safe defaults for scaffold", () => {
    delete process.env.API_MODE;
    delete process.env.API_PAGE_LIMIT;

    const config = loadConfig();

    expect(config.apiMode).toBe("mock");
    expect(config.apiPageLimit).toBe(1000);
    expect(config.writeBatchSize).toBe(10000);
    expect(config.progressLogIntervalMs).toBe(5000);
    expect(config.liveDiscoveryOnResume).toBe(false);
    expect(config.databaseUrl).toContain("postgresql://postgres:postgres");
  });

  it("uses higher page limit default in live mode", () => {
    process.env.API_MODE = "live";
    delete process.env.API_PAGE_LIMIT;

    const config = loadConfig();

    expect(config.apiMode).toBe("live");
    expect(config.apiPageLimit).toBe(5000);
  });

  it("parses live discovery resume override", () => {
    process.env.LIVE_DISCOVERY_ON_RESUME = "true";

    const config = loadConfig();

    expect(config.liveDiscoveryOnResume).toBe(true);
  });
});
