import { describe, expect, it } from "vitest";

import {
  computeExponentialBackoffMs,
  isRetriableStatus,
  parseRetryAfterMs
} from "../src/api/retryPolicy";

describe("parseRetryAfterMs", () => {
  it("parses numeric seconds", () => {
    expect(parseRetryAfterMs("5", Date.parse("2026-01-01T00:00:00.000Z"))).toBe(
      5000
    );
  });

  it("parses HTTP-date", () => {
    const now = Date.parse("2026-01-01T00:00:00.000Z");
    const value = "Thu, 01 Jan 2026 00:00:03 GMT";

    expect(parseRetryAfterMs(value, now)).toBe(3000);
  });

  it("returns null for invalid values", () => {
    expect(parseRetryAfterMs(null, Date.now())).toBeNull();
    expect(parseRetryAfterMs("", Date.now())).toBeNull();
    expect(parseRetryAfterMs("abc", Date.now())).toBeNull();
  });
});

describe("isRetriableStatus", () => {
  it("marks 429 and 5xx as retriable", () => {
    expect(isRetriableStatus(429)).toBe(true);
    expect(isRetriableStatus(500)).toBe(true);
    expect(isRetriableStatus(503)).toBe(true);
  });

  it("marks other 4xx as non-retriable", () => {
    expect(isRetriableStatus(400)).toBe(false);
    expect(isRetriableStatus(404)).toBe(false);
  });
});

describe("computeExponentialBackoffMs", () => {
  it("computes exponential delay with bounded jitter", () => {
    const delay = computeExponentialBackoffMs(2, 100, 1000, () => 0);
    expect(delay).toBe(200);
  });

  it("caps delay at max", () => {
    const delay = computeExponentialBackoffMs(10, 100, 500, () => 0.99);
    expect(delay).toBe(500);
  });
});
