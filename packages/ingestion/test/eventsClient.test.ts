import { afterEach, describe, expect, it, vi } from "vitest";

import { buildEventsUrl, createEventsClient } from "../src/api/eventsClient";
import type { IngestionConfig } from "../src/types";

const baseConfig: IngestionConfig = {
  databaseUrl: "postgresql://postgres:postgres@localhost:5434/ingestion",
  apiMode: "mock",
  apiBaseUrl: "http://example.test/api/v1",
  apiKey: "test-key",
  apiPageLimit: 100,
  apiTimeoutMs: 100,
  apiMaxRetries: 2,
  apiRetryBaseMs: 100,
  apiRetryMaxMs: 1000,
  writeBatchSize: 100,
  progressLogIntervalMs: 5000,
  logLevel: "info"
};

afterEach(() => {
  vi.useRealTimers();
});

describe("buildEventsUrl", () => {
  it("builds events URL with limit and optional cursor", () => {
    const urlWithoutCursor = buildEventsUrl(baseConfig.apiBaseUrl, 100, null);
    expect(urlWithoutCursor.toString()).toBe(
      "http://example.test/api/v1/events?limit=100"
    );

    const urlWithCursor = buildEventsUrl(baseConfig.apiBaseUrl, 100, "abc");
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

    const client = createEventsClient(baseConfig, { fetchImpl: fetchMock });
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

  it("fails fast on non-retriable 4xx responses", async () => {
    const fetchMock = vi.fn(async () => new Response("bad request", { status: 400 })) as unknown as typeof fetch;
    const sleepMock = vi.fn(async () => undefined);
    const client = createEventsClient(baseConfig, {
      fetchImpl: fetchMock,
      sleep: sleepMock
    });

    await expect(client.fetchEventsPage(null)).rejects.toThrow(
      "Events API request failed with status 400"
    );
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(sleepMock).not.toHaveBeenCalled();
  });

  it("retries 429 using Retry-After seconds", async () => {
    const sleepMock = vi.fn(async () => undefined);
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response("rate limited", {
          status: 429,
          headers: { "Retry-After": "2" }
        })
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            data: [{ eventId: "evt-2" }],
            hasMore: false,
            nextCursor: null
          }),
          { status: 200 }
        )
      ) as unknown as typeof fetch;

    const client = createEventsClient(baseConfig, {
      fetchImpl: fetchMock,
      sleep: sleepMock,
      random: () => 0
    });

    const page = await client.fetchEventsPage(null);

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(sleepMock).toHaveBeenCalledTimes(1);
    expect(sleepMock).toHaveBeenCalledWith(2000);
    expect(page.data[0].eventId).toBe("evt-2");
  });

  it("retries 429 using Retry-After date", async () => {
    const nowMs = Date.parse("2026-01-01T00:00:00.000Z");
    const retryDate = "Thu, 01 Jan 2026 00:00:03 GMT";

    const sleepMock = vi.fn(async () => undefined);
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response("rate limited", {
          status: 429,
          headers: { "Retry-After": retryDate }
        })
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({ data: [], hasMore: false, nextCursor: null }),
          { status: 200 }
        )
      ) as unknown as typeof fetch;

    const client = createEventsClient(baseConfig, {
      fetchImpl: fetchMock,
      sleep: sleepMock,
      now: () => nowMs,
      random: () => 0
    });

    await client.fetchEventsPage(null);

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(sleepMock).toHaveBeenCalledWith(3000);
  });

  it("retries 5xx with exponential backoff and jitter", async () => {
    const sleepMock = vi.fn(async () => undefined);
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response("server error", { status: 500 }))
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({ data: [], hasMore: false, nextCursor: null }),
          { status: 200 }
        )
      ) as unknown as typeof fetch;

    const client = createEventsClient(baseConfig, {
      fetchImpl: fetchMock,
      sleep: sleepMock,
      random: () => 0
    });

    await client.fetchEventsPage(null);

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(sleepMock).toHaveBeenCalledWith(100);
  });

  it("retries 429 using rateLimit.retryAfter body field", async () => {
    const sleepMock = vi.fn(async () => undefined);
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            error: "Too Many Requests",
            rateLimit: { retryAfter: 3 }
          }),
          { status: 429 }
        )
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({ data: [], hasMore: false, nextCursor: null }),
          { status: 200 }
        )
      ) as unknown as typeof fetch;

    const client = createEventsClient(baseConfig, {
      fetchImpl: fetchMock,
      sleep: sleepMock,
      random: () => 0
    });

    await client.fetchEventsPage(null);

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(sleepMock).toHaveBeenCalledWith(3000);
  });

  it("retries 429 using x-ratelimit-reset header", async () => {
    const sleepMock = vi.fn(async () => undefined);
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response("rate limited", {
          status: 429,
          headers: { "X-RateLimit-Reset": "4" }
        })
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({ data: [], hasMore: false, nextCursor: null }),
          { status: 200 }
        )
      ) as unknown as typeof fetch;

    const client = createEventsClient(baseConfig, {
      fetchImpl: fetchMock,
      sleep: sleepMock,
      random: () => 0
    });

    await client.fetchEventsPage(null);

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(sleepMock).toHaveBeenCalledWith(4000);
  });

  it("paces next request when rate-limit remaining reaches zero", async () => {
    const sleepMock = vi.fn(async () => undefined);
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({ data: [], hasMore: true, nextCursor: "cursor-1" }),
          {
            status: 200,
            headers: {
              "X-RateLimit-Remaining": "0",
              "X-RateLimit-Reset": "2"
            }
          }
        )
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({ data: [], hasMore: false, nextCursor: null }),
          { status: 200 }
        )
      ) as unknown as typeof fetch;

    const client = createEventsClient(baseConfig, {
      fetchImpl: fetchMock,
      sleep: sleepMock,
      now: () => 0
    });

    await client.fetchEventsPage(null);
    await client.fetchEventsPage("cursor-1");

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(sleepMock).toHaveBeenCalledWith(2000);
  });

  it("spreads request pacing when rate-limit remaining is above zero", async () => {
    const sleepMock = vi.fn(async () => undefined);
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({ data: [], hasMore: true, nextCursor: "cursor-1" }),
          {
            status: 200,
            headers: {
              "X-RateLimit-Limit": "10",
              "X-RateLimit-Remaining": "3",
              "X-RateLimit-Reset": "2"
            }
          }
        )
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({ data: [], hasMore: false, nextCursor: null }),
          { status: 200 }
        )
      ) as unknown as typeof fetch;

    const client = createEventsClient(baseConfig, {
      fetchImpl: fetchMock,
      sleep: sleepMock,
      now: () => 0
    });

    await client.fetchEventsPage(null);
    await client.fetchEventsPage("cursor-1");

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(sleepMock).toHaveBeenCalledWith(500);
  });

  it("interprets x-ratelimit-reset epoch seconds on 429", async () => {
    const sleepMock = vi.fn(async () => undefined);
    const nowMs = Date.parse("2026-02-18T00:00:00.000Z");
    const resetEpochSeconds = Math.floor((nowMs + 3000) / 1000);

    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response("rate limited", {
          status: 429,
          headers: { "X-RateLimit-Reset": String(resetEpochSeconds) }
        })
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({ data: [], hasMore: false, nextCursor: null }),
          { status: 200 }
        )
      ) as unknown as typeof fetch;

    const client = createEventsClient(baseConfig, {
      fetchImpl: fetchMock,
      sleep: sleepMock,
      now: () => nowMs,
      random: () => 0
    });

    await client.fetchEventsPage(null);

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(sleepMock).toHaveBeenCalledWith(3000);
  });

  it("retries timed out requests", async () => {
    vi.useFakeTimers();

    const sleepMock = vi.fn(async () => undefined);
    let attempts = 0;

    const fetchMock = vi.fn((_: URL, init?: RequestInit) => {
      attempts += 1;

      if (attempts === 1) {
        const signal = init?.signal as AbortSignal;

        return new Promise<Response>((_, reject) => {
          signal.addEventListener("abort", () => {
            reject(new DOMException("The operation was aborted.", "AbortError"));
          });
        });
      }

      return Promise.resolve(
        new Response(
          JSON.stringify({ data: [], hasMore: false, nextCursor: null }),
          { status: 200 }
        )
      );
    }) as unknown as typeof fetch;

    const client = createEventsClient(
      {
        ...baseConfig,
        apiTimeoutMs: 50,
        apiMaxRetries: 1
      },
      {
        fetchImpl: fetchMock,
        sleep: sleepMock,
        random: () => 0
      }
    );

    const promise = client.fetchEventsPage(null);

    await vi.advanceTimersByTimeAsync(50);

    await expect(promise).resolves.toEqual({
      data: [],
      hasMore: false,
      nextCursor: null
    });

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(sleepMock).toHaveBeenCalledWith(100);
  });
});
