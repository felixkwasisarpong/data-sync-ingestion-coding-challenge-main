export function parseRetryAfterMs(
  retryAfterHeader: string | null,
  nowMs: number
): number | null {
  if (!retryAfterHeader) {
    return null;
  }

  const trimmed = retryAfterHeader.trim();

  if (trimmed.length === 0) {
    return null;
  }

  if (/^\d+$/.test(trimmed)) {
    const seconds = Number.parseInt(trimmed, 10);
    return Math.max(0, seconds * 1000);
  }

  const retryDateMs = Date.parse(trimmed);
  if (Number.isNaN(retryDateMs)) {
    return null;
  }

  return Math.max(0, retryDateMs - nowMs);
}

export function isRetriableStatus(statusCode: number): boolean {
  return statusCode === 429 || statusCode >= 500;
}

export function computeExponentialBackoffMs(
  retryAttempt: number,
  baseDelayMs: number,
  maxDelayMs: number,
  randomFn: () => number
): number {
  const base = Math.max(1, baseDelayMs);
  const max = Math.max(base, maxDelayMs);
  const exponential = Math.min(max, base * 2 ** Math.max(0, retryAttempt - 1));
  const jitterWindow = Math.floor(exponential * 0.2);
  const jitter = Math.floor(randomFn() * (jitterWindow + 1));

  return Math.min(max, exponential + jitter);
}
