import type { Pool, PoolClient } from "pg";

import { advanceCheckpoint } from "./checkpoint";
import type { CheckpointState, DataSyncEvent } from "../types";

const INSERT_EVENTS_PREFIX = `
INSERT INTO ingested_events (event_id, occurred_at, payload)
VALUES
`;

const INSERT_EVENTS_SUFFIX = `
ON CONFLICT (event_id) DO NOTHING;
`;

export interface BulkWriteResult {
  insertedCount: number;
  checkpoint: CheckpointState;
}

export interface BulkWriter {
  writeBatch: (
    events: DataSyncEvent[],
    cursor: string | null
  ) => Promise<BulkWriteResult>;
}

function normalizeOccurredAt(raw: string | null | undefined): string | null {
  if (raw === null || raw === undefined) {
    return null;
  }

  const timestampMs = Date.parse(raw);
  if (Number.isNaN(timestampMs)) {
    return null;
  }

  return new Date(timestampMs).toISOString();
}

export function buildBulkInsertStatement(events: DataSyncEvent[]): {
  sql: string;
  values: unknown[];
} {
  if (events.length === 0) {
    throw new Error("Cannot build insert statement for empty event batch");
  }

  const values: unknown[] = [];
  const tuples: string[] = [];

  for (let index = 0; index < events.length; index += 1) {
    const event = events[index];
    const base = index * 3;

    tuples.push(`($${base + 1}, $${base + 2}, $${base + 3})`);
    values.push(event.eventId);
    values.push(normalizeOccurredAt(event.occurredAt));
    values.push(event);
  }

  return {
    sql: `${INSERT_EVENTS_PREFIX}${tuples.join(",\n")}\n${INSERT_EVENTS_SUFFIX}`,
    values
  };
}

export async function writeBatchWithClient(
  client: PoolClient,
  events: DataSyncEvent[],
  cursor: string | null
): Promise<BulkWriteResult> {
  await client.query("BEGIN");

  try {
    let insertedCount = 0;

    if (events.length > 0) {
      const statement = buildBulkInsertStatement(events);
      const insertResult = await client.query(
        statement.sql,
        statement.values
      );

      insertedCount = insertResult.rowCount ?? 0;
    }

    const checkpoint = await advanceCheckpoint(client, cursor, insertedCount);
    await client.query("COMMIT");

    return {
      insertedCount,
      checkpoint
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  }
}

export function createBulkWriter(pool: Pool): BulkWriter {
  return {
    async writeBatch(
      events: DataSyncEvent[],
      cursor: string | null
    ): Promise<BulkWriteResult> {
      const client = await pool.connect();

      try {
        return await writeBatchWithClient(client, events, cursor);
      } finally {
        client.release();
      }
    }
  };
}
