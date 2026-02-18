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

function toOccurredAt(record: RecordLike): string | null | undefined {
  const candidate = record.occurredAt ?? record.occurred_at ?? record.timestamp;

  if (candidate === undefined) {
    return undefined;
  }

  if (candidate === null) {
    return null;
  }

  if (typeof candidate !== "string") {
    throw new Error("Invalid event payload: occurredAt must be a string or null");
  }

  return candidate;
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

export function parseEventsPage(payload: unknown): EventsPage {
  if (!isRecordLike(payload)) {
    throw new Error("Invalid events response: expected object");
  }

  const data = payload.data;
  const hasMore = payload.hasMore;
  const nextCursor = payload.nextCursor;

  if (!Array.isArray(data)) {
    throw new Error("Invalid events response: data must be an array");
  }

  if (typeof hasMore !== "boolean") {
    throw new Error("Invalid events response: hasMore must be boolean");
  }

  if (!(typeof nextCursor === "string" || nextCursor === null)) {
    throw new Error("Invalid events response: nextCursor must be string or null");
  }

  return {
    data: data.map((item) => parseEvent(item)),
    hasMore,
    nextCursor
  };
}
