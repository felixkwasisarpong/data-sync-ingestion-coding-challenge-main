export function shouldRunLiveDiscovery(
  apiMode: "mock" | "live",
  totalIngested: number,
  liveDiscoveryOnResume: boolean
): boolean {
  if (apiMode !== "live") {
    return false;
  }

  if (totalIngested <= 0) {
    return true;
  }

  return liveDiscoveryOnResume;
}
