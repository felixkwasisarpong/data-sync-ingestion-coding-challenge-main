import type { DataSyncEvent, EventsPage } from "../types";

interface RecordLike {
  [key: string]: unknown;
}

function isRecordLike(value: unknown): value is RecordLike {
  return typeof value === "object" && value !== null;
}

function toEventId(record: RecordLike): string {
  const candidate = record.eventId ?? record.event_id ?? record.id;

  if (typeof candidate !== "string" || candidate.length === 0) {
    throw new Error("Invalid event payload: missing event id");
  }

  return candidate;
}

function toIsoDateFromEpoch(value: number): string | null {
  if (!Number.isFinite(value)) {
    return null;
  }

  const abs = Math.abs(value);
  let millis = value;

  if (abs >= 1e17) {
    millis = value / 1_000_000;
  } else if (abs >= 1e14) {
    millis = value / 1_000;
  } else if (abs >= 1e8 && abs < 1e11) {
    millis = value * 1_000;
  }

  const date = new Date(millis);
  if (Number.isNaN(date.getTime())) {
    return null;
  }

  return date.toISOString();
}

function toOccurredAt(record: RecordLike): string | null | undefined {
  const candidate = record.occurredAt ?? record.occurred_at ?? record.timestamp;

  if (candidate === undefined) {
    return undefined;
  }

  if (candidate === null) {
    return null;
  }

  if (typeof candidate === "number") {
    return toIsoDateFromEpoch(candidate);
  }

  if (typeof candidate !== "string") {
    return null;
  }

  const trimmed = candidate.trim();
  if (trimmed.length === 0) {
    return null;
  }

  const numericCandidate = Number(trimmed);
  if (!Number.isNaN(numericCandidate)) {
    const isoFromEpoch = toIsoDateFromEpoch(numericCandidate);
    if (isoFromEpoch !== null) {
      return isoFromEpoch;
    }
  }

  const parsed = Date.parse(trimmed);
  if (!Number.isNaN(parsed)) {
    return new Date(parsed).toISOString();
  }

  return null;
}

function parseEvent(value: unknown): DataSyncEvent {
  if (!isRecordLike(value)) {
    throw new Error("Invalid event payload: event must be an object");
  }

  const eventId = toEventId(value);
  const occurredAt = toOccurredAt(value);

  return {
    ...value,
    eventId,
    occurredAt
  };
}

function coerceBoolean(value: unknown): boolean | undefined {
  if (typeof value === "boolean") {
    return value;
  }

  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (normalized === "true") {
      return true;
    }
    if (normalized === "false") {
      return false;
    }
  }

  if (typeof value === "number") {
    if (value === 1) {
      return true;
    }
    if (value === 0) {
      return false;
    }
  }

  return undefined;
}

export function parseEventsPage(payload: unknown): EventsPage {
  if (!isRecordLike(payload)) {
    throw new Error("Invalid events response: expected object");
  }

  const data = Array.isArray(payload.data)
    ? payload.data
    : Array.isArray(payload.events)
      ? payload.events
      : payload.data;
  const pagination = isRecordLike(payload.pagination) ? payload.pagination : null;
  const hasMoreValue =
    payload.hasMore ??
    payload.has_more ??
    pagination?.hasMore ??
    pagination?.has_more;
  const nextCursorValue =
    payload.nextCursor ??
    payload.next_cursor ??
    pagination?.nextCursor ??
    pagination?.next_cursor ??
    null;
  const hasMore = coerceBoolean(hasMoreValue);
  const nextCursor =
    typeof nextCursorValue === "string" || nextCursorValue === null
      ? nextCursorValue
      : null;
  const inferredHasMore =
    hasMore ??
    (typeof nextCursor === "string" && nextCursor.length > 0 ? true : false);

  if (!Array.isArray(data)) {
    throw new Error("Invalid events response: data must be an array");
  }

  if (!(typeof nextCursor === "string" || nextCursor === null)) {
    throw new Error("Invalid events response: nextCursor must be string or null");
  }

  return {
    data: data.map((item) => parseEvent(item)),
    hasMore: inferredHasMore,
    nextCursor
  };
}
