import type { IngestionConfig } from "../types";

import { buildEventsUrl } from "./eventsClient";
import { parseEventsPage } from "./responseParser";

const DISCOVERY_LIMIT = 5;

type FetchLike = typeof fetch;

interface RecordLike {
  [key: string]: unknown;
}

function isRecordLike(value: unknown): value is RecordLike {
  return typeof value === "object" && value !== null;
}

function normalizeHeaderRecord(headers: Headers): Record<string, string> {
  const output: Record<string, string> = {};

  for (const [key, value] of headers.entries()) {
    output[key] = value;
  }

  return output;
}

function buildResponseShape(payload: unknown): Record<string, unknown> {
  if (!isRecordLike(payload)) {
    return { payloadType: typeof payload };
  }

  const keys = Object.keys(payload);
  const data = payload.data;
  const first = Array.isArray(data) && data.length > 0 ? data[0] : null;

  return {
    topLevelKeys: keys,
    dataType: Array.isArray(data) ? "array" : typeof data,
    dataLength: Array.isArray(data) ? data.length : null,
    hasMoreType: typeof payload.hasMore,
    nextCursorType: payload.nextCursor === null ? "null" : typeof payload.nextCursor,
    firstEventKeys: isRecordLike(first) ? Object.keys(first) : null
  };
}

async function fetchWithTimeout(
  fetchImpl: FetchLike,
  url: URL,
  init: RequestInit,
  timeoutMs: number
): Promise<Response> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => {
    controller.abort();
  }, timeoutMs);

  try {
    return await fetchImpl(url, {
      ...init,
      signal: controller.signal
    });
  } finally {
    clearTimeout(timeoutId);
  }
}

export interface LiveDiscoveryResult {
  headers: Record<string, string>;
  responseShape: Record<string, unknown>;
  sampleSize: number;
  hasMore: boolean;
  nextCursor: string | null;
}

export async function runLiveDiscovery(
  config: IngestionConfig,
  fetchImpl: FetchLike = fetch
): Promise<LiveDiscoveryResult> {
  const url = buildEventsUrl(config.apiBaseUrl, DISCOVERY_LIMIT, null);

  const response = await fetchWithTimeout(
    fetchImpl,
    url,
    {
      method: "GET",
      headers: {
        Accept: "application/json",
        "X-API-Key": config.apiKey
      }
    },
    config.apiTimeoutMs
  );

  if (!response.ok) {
    const body = await response.text();
    throw new Error(
      `Live discovery failed with status ${response.status}${body ? `: ${body}` : ""}`
    );
  }

  const payload = (await response.json()) as unknown;
  const page = parseEventsPage(payload);

  return {
    headers: normalizeHeaderRecord(response.headers),
    responseShape: buildResponseShape(payload),
    sampleSize: page.data.length,
    hasMore: page.hasMore,
    nextCursor: page.nextCursor
  };
}
