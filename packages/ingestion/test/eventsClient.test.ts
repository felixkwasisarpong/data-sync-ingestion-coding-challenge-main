import { describe, expect, it, vi } from "vitest";

import { buildEventsUrl, createEventsClient } from "../src/api/eventsClient";
import type { IngestionConfig } from "../src/types";

const config: IngestionConfig = {
  databaseUrl: "postgresql://postgres:postgres@localhost:5434/ingestion",
  apiMode: "mock",
  apiBaseUrl: "http://example.test/api/v1",
  apiKey: "test-key",
  apiPageLimit: 100,
  apiTimeoutMs: 1000,
  apiMaxRetries: 1,
  apiRetryBaseMs: 100,
  apiRetryMaxMs: 1000,
  writeBatchSize: 100,
  logLevel: "info"
};

describe("buildEventsUrl", () => {
  it("builds events URL with limit and optional cursor", () => {
    const urlWithoutCursor = buildEventsUrl(config.apiBaseUrl, 100, null);
    expect(urlWithoutCursor.toString()).toBe(
      "http://example.test/api/v1/events?limit=100"
    );

    const urlWithCursor = buildEventsUrl(config.apiBaseUrl, 100, "abc");
    expect(urlWithCursor.toString()).toBe(
      "http://example.test/api/v1/events?limit=100&cursor=abc"
    );
  });
});

describe("createEventsClient", () => {
  it("requests a page and parses the response", async () => {
    const fetchMock = vi.fn(async () =>
      new Response(
        JSON.stringify({
          data: [{ eventId: "evt-1", occurredAt: null }],
          hasMore: true,
          nextCursor: "next"
        }),
        {
          status: 200,
          headers: { "content-type": "application/json" }
        }
      )
    ) as unknown as typeof fetch;

    const client = createEventsClient(config, fetchMock);
    const page = await client.fetchEventsPage(null);

    expect(fetchMock).toHaveBeenCalledOnce();
    const [requestUrl, requestInit] = fetchMock.mock.calls[0] as [URL, RequestInit];

    expect(requestUrl.toString()).toBe(
      "http://example.test/api/v1/events?limit=100"
    );
    expect(requestInit.method).toBe("GET");
    expect((requestInit.headers as Record<string, string>)["X-API-Key"]).toBe(
      "test-key"
    );

    expect(page.hasMore).toBe(true);
    expect(page.nextCursor).toBe("next");
    expect(page.data[0].eventId).toBe("evt-1");
  });

  it("throws on non-2xx responses", async () => {
    const fetchMock = vi.fn(async () => new Response("failure", { status: 500 })) as unknown as typeof fetch;
    const client = createEventsClient(config, fetchMock);

    await expect(client.fetchEventsPage(null)).rejects.toThrow(
      "Events API request failed with status 500"
    );
  });
});
