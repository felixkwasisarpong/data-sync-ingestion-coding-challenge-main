# DataSync Ingestion

Production-oriented ingestion pipeline for the DataSync challenge, implemented with:
- Node.js 20 + TypeScript
- PostgreSQL (`pg`)
- Vitest
- Docker Compose orchestration via `sh run-ingestion.sh`

## Repository Layout

- `packages/ingestion`: ingestion worker, DB migrations, retry/bulk write logic
- `packages/mock-api`: deterministic mock events API used before live runs
- `docker-compose.yml`: local stack (`postgres`, `mock-api`, `ingestion`)
- `run-ingestion.sh`: one-command orchestration + progress monitor

## Quick Start (Mock Mode)

1. Install dependencies:

```bash
npm install
```

2. Run tests:

```bash
npm test
```

3. Run end-to-end ingestion in Docker:

```bash
sh run-ingestion.sh
```

Expected terminal signal on success:
- `INGESTION COMPLETE!`
- `Total events: <count>`

The ingestion service also logs exact completion marker:
- `ingestion complete`

## Environment Variables

Key runtime configuration (see `.env.example`):

- `API_MODE`: `mock` or `live`
- `API_BASE_URL`: API base URL (`http://mock-api:3100/api/v1` by default in Docker)
- `DATASYNC_API_KEY`: API key for live mode
- `DATABASE_URL`: PostgreSQL connection string
- `API_PAGE_LIMIT`: page size per fetch
- `API_TIMEOUT_MS`: per-request timeout
- `API_MAX_RETRIES`: retry attempts for timeout/429/5xx
- `API_RETRY_BASE_MS`: backoff base delay
- `API_RETRY_MAX_MS`: backoff max delay
- `WRITE_BATCH_SIZE`: buffered DB flush threshold
- `LOG_LEVEL`: log level

## Architecture Overview

### 1) Fetch Layer

`packages/ingestion/src/api/eventsClient.ts`

- Cursor-based pagination requests: `GET /events?limit=<n>&cursor=<c>`
- Timeout using `AbortController`
- Retry behavior:
  - `429`: uses `Retry-After` (seconds or HTTP-date) when present
  - `5xx` and timeout/network errors: exponential backoff + jitter
  - other `4xx`: fail fast

Supporting modules:
- `responseParser.ts`: validates/normalizes response shape
- `retryPolicy.ts`: retry-after parsing + backoff calculations

### 2) Write Layer

`packages/ingestion/src/db/bulkWriter.ts`

- Buffered multi-row insert into `ingested_events`
- Uses `ON CONFLICT (event_id) DO NOTHING` for idempotent dedupe
- Wraps insert + checkpoint update in a single transaction
- Rolls back on any write/checkpoint error

### 3) Checkpoint & Resumability

`packages/ingestion/src/db/checkpoint.ts`

- Singleton row in `ingestion_state` (`id = 1`)
- Stores:
  - `cursor`
  - `total_ingested`
  - `updated_at`
- Startup flow loads checkpoint and resumes from stored cursor

### 4) Orchestration

`packages/ingestion/src/ingestion/bufferedIngestion.ts` and `src/index.ts`

- Fetch pages sequentially from current cursor
- Buffer events across pages
- Flush when `WRITE_BATCH_SIZE` is reached or final page is reached
- Track pages fetched, events seen, inserted rows, and flush count
- On `hasMore = false`, flush final buffer and log `ingestion complete`

### 5) Database Schema

Migration: `packages/ingestion/migrations/0001_init.sql`

- `ingested_events`
  - `event_id` `TEXT PRIMARY KEY`
  - `occurred_at` `TIMESTAMPTZ NULL`
  - `payload` `JSONB NOT NULL`
  - `ingested_at` `TIMESTAMPTZ DEFAULT NOW()`
- `ingestion_state`
  - singleton key `id SMALLINT PRIMARY KEY CHECK (id = 1)`
  - `cursor TEXT NULL`
  - `total_ingested BIGINT`
  - `updated_at TIMESTAMPTZ`

## Docker Execution Model

`docker-compose.yml` provisions:
- `postgres` with healthcheck
- `mock-api` with `/health` endpoint and healthcheck
- `ingestion` (depends on healthy DB + mock API)

`run-ingestion.sh`:
- starts stack with build
- polls row count from PostgreSQL
- monitors ingestion logs for `ingestion complete`
- exits non-zero with diagnostics if ingestion container exits unexpectedly

## Testing

Run all tests:

```bash
npm test
```

Coverage includes:
- retry policy and retry-after parsing
- events client request/retry behavior
- response parsing/normalization
- pagination and buffered flush flow
- bulk insert SQL generation + transaction semantics
- checkpoint read/update behavior
- migration discovery/apply behavior

## Live API Alignment Workflow

Before live run:
- keep `API_MODE=mock`
- validate end-to-end with `sh run-ingestion.sh`

When ready for live run:
1. Set `API_MODE=live`
2. Set `DATASYNC_API_KEY`
3. Set `API_BASE_URL` to live endpoint
4. Make one discovery request with `limit=5`
5. Log headers and response shape
6. Proceed with full ingestion

## Submission Helpers

Current placeholder commands:

```bash
npm run submit:prepare
npm run submit:send
```

These are scaffolds and should be implemented/finalized when submission flow is wired.

## Trade-offs and Future Improvements

- Keep-alive transport tuning can be further optimized for maximum throughput.
- Dynamic batch size adaptation based on DB latency/backpressure is not yet implemented.
- Metrics endpoint and richer health telemetry are pending.
- Submission helper scripts are placeholders and need final implementation for automated submission packaging.
