import { readFile } from "node:fs/promises";
import path from "node:path";

import { loadConfig } from "../config";

const DEFAULT_INPUT_PATH = path.resolve(process.cwd(), "submission/event_ids.txt");

function normalizeBaseUrl(baseUrl: string): string {
  return baseUrl.endsWith("/") ? baseUrl : `${baseUrl}/`;
}

function buildSubmissionUrl(apiBaseUrl: string, githubRepoUrl: string): URL {
  const url = new URL("submissions", normalizeBaseUrl(apiBaseUrl));
  url.searchParams.set("github_repo", githubRepoUrl);
  return url;
}

async function main(): Promise<void> {
  const config = loadConfig();

  const githubRepoUrl = process.env.SUBMISSION_GITHUB_REPO_URL;
  if (!githubRepoUrl) {
    throw new Error("SUBMISSION_GITHUB_REPO_URL is required");
  }

  const inputPath = process.env.SUBMISSION_INPUT_FILE ?? DEFAULT_INPUT_PATH;
  const payload = await readFile(inputPath, "utf8");

  if (payload.trim().length === 0) {
    throw new Error(`Submission input file is empty: ${inputPath}`);
  }

  const submissionUrl = buildSubmissionUrl(config.apiBaseUrl, githubRepoUrl);

  const response = await fetch(submissionUrl, {
    method: "POST",
    headers: {
      "X-API-Key": config.apiKey,
      "Content-Type": "text/plain"
    },
    body: payload
  });

  const responseText = await response.text();

  if (!response.ok) {
    throw new Error(
      `Submission failed with status ${response.status}${responseText ? `: ${responseText}` : ""}`
    );
  }

  console.log(`submission accepted (status=${response.status})`);

  if (responseText.length > 0) {
    try {
      const parsed = JSON.parse(responseText) as unknown;
      console.log(`submission response: ${JSON.stringify(parsed)}`);
    } catch {
      console.log(`submission response: ${responseText}`);
    }
  }
}

main().catch((error: unknown) => {
  console.error("submit send failed", error);
  process.exit(1);
});
