import type { IngestionConfig } from "./types";

function readInt(name: string, fallback: number): number {
  const raw = process.env[name];

  if (!raw) {
    return fallback;
  }

  const parsed = Number.parseInt(raw, 10);
  if (Number.isNaN(parsed)) {
    throw new Error(`Invalid integer for ${name}: ${raw}`);
  }

  return parsed;
}

export function loadConfig(): IngestionConfig {
  return {
    databaseUrl:
      process.env.DATABASE_URL ??
      "postgresql://postgres:postgres@localhost:5434/ingestion",
    apiMode: process.env.API_MODE === "live" ? "live" : "mock",
    apiBaseUrl:
      process.env.API_BASE_URL ??
      "http://mock-api:3100/api/v1",
    apiKey: process.env.DATASYNC_API_KEY ?? "mock-api-key",
    apiPageLimit: readInt("API_PAGE_LIMIT", 1000),
    apiTimeoutMs: readInt("API_TIMEOUT_MS", 10000),
    apiMaxRetries: readInt("API_MAX_RETRIES", 5),
    apiRetryBaseMs: readInt("API_RETRY_BASE_MS", 200),
    apiRetryMaxMs: readInt("API_RETRY_MAX_MS", 5000),
    writeBatchSize: readInt("WRITE_BATCH_SIZE", 2000),
    logLevel: process.env.LOG_LEVEL ?? "info"
  };
}
