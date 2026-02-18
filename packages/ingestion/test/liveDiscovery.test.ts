import { describe, expect, it, vi } from "vitest";

import { runLiveDiscovery } from "../src/api/liveDiscovery";
import type { IngestionConfig } from "../src/types";

const config: IngestionConfig = {
  databaseUrl: "postgresql://postgres:postgres@localhost:5434/ingestion",
  apiMode: "live",
  apiBaseUrl: "http://example.test/api/v1",
  apiKey: "test-key",
  apiPageLimit: 1000,
  apiTimeoutMs: 1000,
  apiMaxRetries: 5,
  apiRetryBaseMs: 100,
  apiRetryMaxMs: 1000,
  writeBatchSize: 2000,
  logLevel: "info"
};

describe("runLiveDiscovery", () => {
  it("fetches one discovery page and returns headers + shape", async () => {
    const fetchMock = vi.fn(async () =>
      new Response(
        JSON.stringify({
          data: [
            {
              eventId: "evt-1",
              occurredAt: "2026-01-01T00:00:00.000Z",
              eventType: "click"
            }
          ],
          hasMore: true,
          nextCursor: "next-cursor"
        }),
        {
          status: 200,
          headers: {
            "x-ratelimit-remaining": "99",
            "content-type": "application/json"
          }
        }
      )
    ) as unknown as typeof fetch;

    const result = await runLiveDiscovery(config, fetchMock);

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [requestUrl, requestInit] = fetchMock.mock.calls[0] as [URL, RequestInit];

    expect(requestUrl.toString()).toBe(
      "http://example.test/api/v1/events?limit=5"
    );
    expect((requestInit.headers as Record<string, string>)["X-API-Key"]).toBe(
      "test-key"
    );

    expect(result.sampleSize).toBe(1);
    expect(result.hasMore).toBe(true);
    expect(result.nextCursor).toBe("next-cursor");
    expect(result.headers["x-ratelimit-remaining"]).toBe("99");
    expect(result.responseShape).toMatchObject({
      dataType: "array",
      dataLength: 1,
      hasMoreType: "boolean"
    });
  });

  it("throws on non-2xx responses", async () => {
    const fetchMock = vi.fn(async () =>
      new Response("unauthorized", { status: 401 })
    ) as unknown as typeof fetch;

    await expect(runLiveDiscovery(config, fetchMock)).rejects.toThrow(
      "Live discovery failed with status 401"
    );
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});
