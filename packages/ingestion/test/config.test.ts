import { describe, expect, it } from "vitest";

import { loadConfig } from "../src/config";

describe("loadConfig", () => {
  it("uses safe defaults for scaffold", () => {
    delete process.env.API_MODE;
    delete process.env.API_PAGE_LIMIT;

    const config = loadConfig();

    expect(config.apiMode).toBe("mock");
    expect(config.apiPageLimit).toBe(1000);
    expect(config.writeBatchSize).toBe(10000);
    expect(config.progressLogIntervalMs).toBe(5000);
    expect(config.databaseUrl).toContain("postgresql://postgres:postgres");
  });
});
