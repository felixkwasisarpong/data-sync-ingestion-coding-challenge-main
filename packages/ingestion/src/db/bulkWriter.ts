import type { Pool, PoolClient } from "pg";

import { advanceCheckpoint } from "./checkpoint";
import type { CheckpointState, DataSyncEvent } from "../types";

const INSERT_EVENTS_SQL = `
INSERT INTO ingested_events (event_id, occurred_at, payload)
SELECT event_id, occurred_at, payload
FROM UNNEST($1::text[], $2::timestamptz[], $3::jsonb[]) AS rows(event_id, occurred_at, payload)
ON CONFLICT (event_id) DO NOTHING;
`;

export const MAX_INSERT_EVENTS_PER_STATEMENT = 25000;

export interface BulkWriteResult {
  insertedCount: number;
  checkpoint: CheckpointState;
}

export interface BulkWriter {
  writeBatch: (
    events: DataSyncEvent[],
    cursor: string | null
  ) => Promise<BulkWriteResult>;
  close?: () => Promise<void>;
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

  const eventIds: string[] = [];
  const occurredAts: Array<string | null> = [];
  const payloads: string[] = [];

  for (let index = 0; index < events.length; index += 1) {
    const event = events[index];
    eventIds.push(event.eventId);
    occurredAts.push(normalizeOccurredAt(event.occurredAt));
    payloads.push(JSON.stringify(event));
  }

  return {
    sql: INSERT_EVENTS_SQL,
    values: [eventIds, occurredAts, payloads]
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
      for (
        let index = 0;
        index < events.length;
        index += MAX_INSERT_EVENTS_PER_STATEMENT
      ) {
        const chunk = events.slice(
          index,
          index + MAX_INSERT_EVENTS_PER_STATEMENT
        );
        const statement = buildBulkInsertStatement(chunk);
        const insertResult = await client.query(
          statement.sql,
          statement.values
        );
        insertedCount += insertResult.rowCount ?? 0;
      }
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
  let cachedClient: PoolClient | null = null;
  let connectPromise: Promise<PoolClient> | null = null;

  const getClient = async (): Promise<PoolClient> => {
    if (cachedClient) {
      return cachedClient;
    }

    if (!connectPromise) {
      connectPromise = pool.connect().then((client) => {
        cachedClient = client;
        return client;
      });
    }

    try {
      return await connectPromise;
    } finally {
      connectPromise = null;
    }
  };

  const releaseCachedClient = (): void => {
    if (!cachedClient) {
      return;
    }

    cachedClient.release();
    cachedClient = null;
  };

  return {
    async writeBatch(
      events: DataSyncEvent[],
      cursor: string | null
    ): Promise<BulkWriteResult> {
      const client = await getClient();

      try {
        return await writeBatchWithClient(client, events, cursor);
      } catch (error) {
        releaseCachedClient();
        throw error;
      }
    },
    async close(): Promise<void> {
      releaseCachedClient();
    }
  };
}
