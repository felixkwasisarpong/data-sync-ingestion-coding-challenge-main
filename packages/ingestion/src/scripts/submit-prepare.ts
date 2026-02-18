import { createWriteStream } from "node:fs";
import { mkdir } from "node:fs/promises";
import path from "node:path";

import type { QueryResult } from "pg";

import { loadConfig } from "../config";
import { createPool } from "../db/pool";

interface EventIdRow {
  event_id: string;
}

const DEFAULT_OUTPUT_PATH = path.resolve(process.cwd(), "submission/event_ids.txt");
const DEFAULT_BATCH_SIZE = 50_000;

function parseBatchSize(raw: string | undefined): number {
  if (!raw) {
    return DEFAULT_BATCH_SIZE;
  }

  const parsed = Number.parseInt(raw, 10);
  if (Number.isNaN(parsed) || parsed <= 0) {
    throw new Error(`Invalid SUBMISSION_PREPARE_BATCH_SIZE: ${raw}`);
  }

  return parsed;
}

async function writeLine(stream: ReturnType<typeof createWriteStream>, line: string): Promise<void> {
  const writable = stream.write(`${line}\n`);

  if (writable) {
    return;
  }

  await new Promise<void>((resolve, reject) => {
    stream.once("drain", resolve);
    stream.once("error", reject);
  });
}

async function main(): Promise<void> {
  const config = loadConfig();
  const pool = createPool(config.databaseUrl);

  const outputPath = process.env.SUBMISSION_INPUT_FILE ?? DEFAULT_OUTPUT_PATH;
  const batchSize = parseBatchSize(process.env.SUBMISSION_PREPARE_BATCH_SIZE);

  await mkdir(path.dirname(outputPath), { recursive: true });

  const stream = createWriteStream(outputPath, {
    encoding: "utf8",
    flags: "w"
  });

  let lastEventId: string | null = null;
  let total = 0;

  try {
    while (true) {
      const result: QueryResult<EventIdRow> = await pool.query<EventIdRow>(
        `
        SELECT event_id
        FROM ingested_events
        WHERE ($1::text IS NULL OR event_id > $1)
        ORDER BY event_id ASC
        LIMIT $2;
        `,
        [lastEventId, batchSize]
      );

      if (result.rows.length === 0) {
        break;
      }

      for (const row of result.rows) {
        await writeLine(stream, row.event_id);
      }

      lastEventId = result.rows[result.rows.length - 1].event_id;
      total += result.rows.length;
      console.log(`prepared submission chunk (size=${result.rows.length}, total=${total})`);
    }

    await new Promise<void>((resolve, reject) => {
      stream.end(() => resolve());
      stream.once("error", reject);
    });

    console.log(`submission file ready: ${outputPath}`);
    console.log(`total event ids written: ${total}`);
  } finally {
    if (!stream.closed) {
      stream.destroy();
    }

    await pool.end();
  }
}

main().catch((error: unknown) => {
  console.error("submit prepare failed", error);
  process.exit(1);
});
