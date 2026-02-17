import { createServer, type ServerResponse } from "node:http";

import { buildMockEvents, paginateEvents } from "./mockData";

const port = Number.parseInt(process.env.PORT ?? "3100", 10);
const totalEvents = Number.parseInt(process.env.MOCK_TOTAL_EVENTS ?? "5000", 10);

const events = buildMockEvents(totalEvents);

function writeJson(
  response: ServerResponse,
  statusCode: number,
  payload: unknown
): void {
  response.statusCode = statusCode;
  response.setHeader("Content-Type", "application/json");
  response.end(JSON.stringify(payload));
}

const server = createServer((request, response) => {
  if (!request.url) {
    writeJson(response, 400, { error: "Missing URL" });
    return;
  }

  const url = new URL(request.url, `http://localhost:${port}`);

  if (url.pathname === "/health") {
    writeJson(response, 200, { status: "ok" });
    return;
  }

  if (url.pathname !== "/api/v1/events") {
    writeJson(response, 404, { error: "Not Found" });
    return;
  }

  const limit = Number.parseInt(url.searchParams.get("limit") ?? "1000", 10);
  const cursor = url.searchParams.get("cursor");

  try {
    const page = paginateEvents(events, limit, cursor);
    writeJson(response, 200, page);
  } catch (error) {
    writeJson(response, 400, {
      error: "Invalid cursor",
      message: error instanceof Error ? error.message : "Invalid cursor"
    });
  }
});

server.listen(port, "0.0.0.0", () => {
  console.log(`mock api listening on port ${port} with ${events.length} events`);
});
