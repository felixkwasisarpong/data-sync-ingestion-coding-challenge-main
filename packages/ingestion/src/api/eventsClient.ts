import type { IngestionConfig, EventsPage } from "../types";
import { parseEventsPage } from "./responseParser";
import {
  computeExponentialBackoffMs,
  isRetriableStatus,
  parseRetryAfterMs
} from "./retryPolicy";

export interface EventsClient {
  fetchEventsPage: (cursor: string | null) => Promise<EventsPage>;
}

type FetchLike = typeof fetch;
type SleepLike = (ms: number) => Promise<void>;

export interface EventsClientDependencies {
  fetchImpl?: FetchLike;
  sleep?: SleepLike;
  now?: () => number;
  random?: () => number;
}

class RequestTimeoutError extends Error {
  constructor(timeoutMs: number) {
    super(`Events API request timed out after ${timeoutMs}ms`);
    this.name = "RequestTimeoutError";
  }
}

function normalizeBaseUrl(apiBaseUrl: string): string {
  return apiBaseUrl.endsWith("/") ? apiBaseUrl : `${apiBaseUrl}/`;
}

export function buildEventsUrl(
  apiBaseUrl: string,
  limit: number,
  cursor: string | null
): URL {
  const url = new URL("events", normalizeBaseUrl(apiBaseUrl));
  url.searchParams.set("limit", String(limit));

  if (cursor) {
    url.searchParams.set("cursor", cursor);
  }

  return url;
}

function createErrorMessage(status: number, body: string): string {
  if (!body) {
    return `Events API request failed with status ${status}`;
  }

  return `Events API request failed with status ${status}: ${body}`;
}

interface RateLimitBody {
  retryAfter?: unknown;
  reset?: unknown;
}

interface ErrorBodyShape {
  rateLimit?: RateLimitBody;
}

function parseRetryAfterMsFromRateLimitBody(body: string): number | null {
  if (!body) {
    return null;
  }

  try {
    const parsed = JSON.parse(body) as ErrorBodyShape;
    const rateLimit = parsed.rateLimit;

    if (!rateLimit) {
      return null;
    }

    const retryAfter = rateLimit.retryAfter;
    if (typeof retryAfter === "number" && Number.isFinite(retryAfter)) {
      return Math.max(0, Math.floor(retryAfter * 1000));
    }

    if (typeof retryAfter === "string") {
      const asNumber = Number(retryAfter);
      if (!Number.isNaN(asNumber)) {
        return Math.max(0, Math.floor(asNumber * 1000));
      }
    }

    const reset = rateLimit.reset;
    if (typeof reset === "number" && Number.isFinite(reset)) {
      return Math.max(0, Math.floor(reset * 1000));
    }

    if (typeof reset === "string") {
      const asNumber = Number(reset);
      if (!Number.isNaN(asNumber)) {
        return Math.max(0, Math.floor(asNumber * 1000));
      }
    }
  } catch {
    return null;
  }

  return null;
}

function parseRetryAfterMsFromResetHeader(value: string | null): number | null {
  if (!value) {
    return null;
  }

  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return null;
  }

  const numeric = Number(trimmed);
  if (Number.isNaN(numeric)) {
    return null;
  }

  return Math.max(0, Math.floor(numeric * 1000));
}

function shouldRetryFetchError(error: unknown): boolean {
  if (error instanceof RequestTimeoutError) {
    return true;
  }

  if (error instanceof Error) {
    return error.name === "AbortError" || error.name === "TypeError";
  }

  return false;
}

async function sleepFor(ms: number): Promise<void> {
  await new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function fetchWithTimeout(
  fetchImpl: FetchLike,
  requestUrl: URL,
  requestInit: RequestInit,
  timeoutMs: number
): Promise<Response> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => {
    controller.abort();
  }, timeoutMs);

  try {
    return await fetchImpl(requestUrl, {
      ...requestInit,
      signal: controller.signal
    });
  } catch (error) {
    if (controller.signal.aborted) {
      throw new RequestTimeoutError(timeoutMs);
    }

    throw error;
  } finally {
    clearTimeout(timeoutId);
  }
}

export function createEventsClient(
  config: IngestionConfig,
  dependencies: EventsClientDependencies = {}
): EventsClient {
  const fetchImpl = dependencies.fetchImpl ?? fetch;
  const sleep = dependencies.sleep ?? sleepFor;
  const now = dependencies.now ?? Date.now;
  const random = dependencies.random ?? Math.random;
  const maxAttempts = Math.max(1, config.apiMaxRetries + 1);

  return {
    async fetchEventsPage(cursor: string | null): Promise<EventsPage> {
      const url = buildEventsUrl(
        config.apiBaseUrl,
        config.apiPageLimit,
        cursor
      );
      const requestInit: RequestInit = {
        method: "GET",
        headers: {
          Accept: "application/json",
          "X-API-Key": config.apiKey
        }
      };

      let attempt = 1;

      while (true) {
        let response: Response;

        try {
          response = await fetchWithTimeout(
            fetchImpl,
            url,
            requestInit,
            config.apiTimeoutMs
          );
        } catch (error) {
          const shouldRetry = shouldRetryFetchError(error) && attempt < maxAttempts;
          if (!shouldRetry) {
            throw error;
          }

          const retryDelayMs = computeExponentialBackoffMs(
            attempt,
            config.apiRetryBaseMs,
            config.apiRetryMaxMs,
            random
          );
          attempt += 1;
          await sleep(retryDelayMs);
          continue;
        }

        if (response.ok) {
          const payload = (await response.json()) as unknown;
          return parseEventsPage(payload);
        }

        const body = await response.text();
        const isRateLimited = response.status === 429;
        const shouldRetryStatusCode =
          isRateLimited ||
          (isRetriableStatus(response.status) && attempt < maxAttempts);

        if (!shouldRetryStatusCode) {
          throw new Error(createErrorMessage(response.status, body));
        }

        const retryAfterMs =
          response.status === 429
            ? parseRetryAfterMs(response.headers.get("Retry-After"), now()) ??
              parseRetryAfterMsFromRateLimitBody(body) ??
              parseRetryAfterMsFromResetHeader(
                response.headers.get("X-RateLimit-Reset")
              )
            : null;
        const backoffDelayMs = computeExponentialBackoffMs(
          attempt,
          config.apiRetryBaseMs,
          config.apiRetryMaxMs,
          random
        );
        const retryDelayMs = isRateLimited
          ? Math.max(retryAfterMs ?? 0, backoffDelayMs)
          : retryAfterMs ?? backoffDelayMs;

        attempt += 1;

        await sleep(retryDelayMs);
      }
    }
  };
}
