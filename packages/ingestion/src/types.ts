export interface DataSyncEvent {
  eventId: string;
  occurredAt?: string | null;
  [key: string]: unknown;
}

export interface EventsPage {
  data: DataSyncEvent[];
  hasMore: boolean;
  nextCursor: string | null;
}

export interface IngestionConfig {
  databaseUrl: string;
  apiMode: "mock" | "live";
  apiBaseUrl: string;
  apiKey: string;
  apiPageLimit: number;
  apiTimeoutMs: number;
  apiMaxRetries: number;
  apiRetryBaseMs: number;
  apiRetryMaxMs: number;
  writeBatchSize: number;
  progressLogIntervalMs: number;
  liveDiscoveryOnResume: boolean;
  logLevel: string;
}

export interface CheckpointState {
  id: 1;
  cursor: string | null;
  totalIngested: number;
  updatedAt: string;
}
