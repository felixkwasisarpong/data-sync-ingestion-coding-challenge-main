import { loadConfig } from "./config";

function logStartup(): void {
  const config = loadConfig();
  console.log(
    `ingestion scaffold started (mode=${config.apiMode}, limit=${config.apiPageLimit}, batch=${config.writeBatchSize})`
  );
}

async function main(): Promise<void> {
  logStartup();

  // Milestone 1 scaffold process stays alive for compose wiring validation.
  await new Promise<void>(() => {
    // Intentionally unresolved promise.
  });
}

main().catch((error: unknown) => {
  console.error("ingestion scaffold failed", error);
  process.exit(1);
});
