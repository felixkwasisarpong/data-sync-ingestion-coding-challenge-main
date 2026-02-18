import type { IngestionConfig, EventsPage } from "../types";
import { parseEventsPage } from "./responseParser";

export interface EventsClient {
  fetchEventsPage: (cursor: string | null) => Promise<EventsPage>;
}

type FetchLike = typeof fetch;

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

export function createEventsClient(
  config: IngestionConfig,
  fetchImpl: FetchLike = fetch
): EventsClient {
  return {
    async fetchEventsPage(cursor: string | null): Promise<EventsPage> {
      const url = buildEventsUrl(
        config.apiBaseUrl,
        config.apiPageLimit,
        cursor
      );

      const response = await fetchImpl(url, {
        method: "GET",
        headers: {
          Accept: "application/json",
          "X-API-Key": config.apiKey
        }
      });

      if (!response.ok) {
        const body = await response.text();
        throw new Error(createErrorMessage(response.status, body));
      }

      const payload = (await response.json()) as unknown;
      return parseEventsPage(payload);
    }
  };
}
