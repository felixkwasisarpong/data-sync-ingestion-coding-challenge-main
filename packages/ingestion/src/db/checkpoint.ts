import type { QueryResult } from "pg";

import type { CheckpointState } from "../types";

interface CheckpointRow {
  id: number;
  cursor: string | null;
  total_ingested: string;
  updated_at: Date;
}

interface Queryable {
  query: (text: string, values?: unknown[]) => Promise<QueryResult<CheckpointRow>>;
}

const SELECT_CHECKPOINT_SQL = `
SELECT id, cursor, total_ingested, updated_at
FROM ingestion_state
WHERE id = 1;
`;

const ADVANCE_CHECKPOINT_SQL = `
UPDATE ingestion_state
SET
  cursor = $1,
  total_ingested = total_ingested + $2,
  updated_at = NOW()
WHERE id = 1
RETURNING id, cursor, total_ingested, updated_at;
`;

function rowToCheckpointState(row: CheckpointRow): CheckpointState {
  return {
    id: 1,
    cursor: row.cursor,
    totalIngested: Number.parseInt(row.total_ingested, 10),
    updatedAt: row.updated_at.toISOString()
  };
}

export async function getCheckpointState(
  runner: Queryable
): Promise<CheckpointState> {
  const result = await runner.query(SELECT_CHECKPOINT_SQL);

  if (result.rowCount !== 1) {
    throw new Error("ingestion_state singleton row missing (id=1)");
  }

  return rowToCheckpointState(result.rows[0]);
}

export async function advanceCheckpoint(
  runner: Queryable,
  cursor: string | null,
  insertedCount: number
): Promise<CheckpointState> {
  if (insertedCount < 0) {
    throw new Error(`insertedCount cannot be negative: ${insertedCount}`);
  }

  const result = await runner.query(ADVANCE_CHECKPOINT_SQL, [
    cursor,
    insertedCount
  ]);

  if (result.rowCount !== 1) {
    throw new Error("failed to advance ingestion checkpoint");
  }

  return rowToCheckpointState(result.rows[0]);
}
