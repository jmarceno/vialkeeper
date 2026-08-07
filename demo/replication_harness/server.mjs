import { createReadStream } from "node:fs";
import { readFile, stat } from "node:fs/promises";
import { createServer } from "node:http";
import { extname, join, normalize, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(fileURLToPath(new URL(".", import.meta.url)));
const stateFile = process.env.DEMO_READY_CONFIG;
const port = Number(process.env.DEMO_WEB_PORT || 4180);
const host = process.env.DEMO_WEB_HOST || "127.0.0.1";
const maxBodyBytes = 2 * 1024 * 1024;
const maxResponseBytes = 16 * 1024 * 1024;

if (!stateFile) throw new Error("DEMO_READY_CONFIG is required");

function contentType(path) {
  return {
    ".html": "text/html; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".png": "image/png",
    ".svg": "image/svg+xml",
  }[extname(path)] || "application/octet-stream";
}

async function readConfig() {
  return JSON.parse(await readFile(stateFile, "utf8"));
}

function sendJson(response, status, value) {
  const body = JSON.stringify(value);
  response.writeHead(status, {
    "cache-control": "no-store",
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(body),
  });
  response.end(body);
}

async function readRequestBody(request) {
  const chunks = [];
  let size = 0;
  for await (const chunk of request) {
    size += chunk.length;
    if (size > maxBodyBytes) throw new Error("request body exceeds the demo proxy limit");
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

async function proxy(request, response, clientKey, suffix) {
  const config = await readConfig();
  const client = config.clients.find((value) => value.key === clientKey);
  if (!client) return sendJson(response, 404, { error: "unknown demo client" });

  const target = new URL(suffix || "/", client.endpoint);
  const body = ["GET", "HEAD", "DELETE"].includes(request.method)
    ? undefined
    : await readRequestBody(request);
  const headers = {};
  for (const name of ["accept", "content-type", "x-request-id"]) {
    if (request.headers[name]) headers[name] = request.headers[name];
  }

  try {
    const upstream = await fetch(target, {
      method: request.method,
      headers,
      body,
    });
    const upstreamBody = Buffer.from(await upstream.arrayBuffer());
    if (upstreamBody.byteLength > maxResponseBytes) {
      return sendJson(response, 502, { error: "upstream response exceeds the demo proxy limit" });
    }
    const responseHeaders = {
      "cache-control": "no-store",
      "content-length": upstreamBody.byteLength,
      "content-type": upstream.headers.get("content-type") || "application/json; charset=utf-8",
    };
    const requestId = upstream.headers.get("x-request-id");
    if (requestId) responseHeaders["x-request-id"] = requestId;
    response.writeHead(upstream.status, responseHeaders);
    response.end(upstreamBody);
  } catch (error) {
    sendJson(response, 502, { error: error instanceof Error ? error.message : String(error) });
  }
}

async function serveStatic(request, response, pathname) {
  const requested = pathname === "/" ? "/index.html" : pathname;
  const file = normalize(join(root, requested));
  if (relative(root, file).startsWith("..")) return sendJson(response, 404, { error: "not found" });
  try {
    const info = await stat(file);
    if (!info.isFile()) return sendJson(response, 404, { error: "not found" });
    response.writeHead(200, {
      "cache-control": "no-store",
      "content-type": contentType(file),
      "content-length": info.size,
    });
    if (request.method === "HEAD") return response.end();
    createReadStream(file).pipe(response);
  } catch {
    sendJson(response, 404, { error: "not found" });
  }
}

const server = createServer(async (request, response) => {
  try {
    const url = new URL(request.url, `http://${request.headers.host || "localhost"}`);
    if (url.pathname === "/config.json") return sendJson(response, 200, await readConfig());

    const proxyMatch = url.pathname.match(/^\/api\/(a|b)(\/.*)?$/);
    if (proxyMatch) return await proxy(request, response, proxyMatch[1], proxyMatch[2] || "/");

    if (request.method !== "GET" && request.method !== "HEAD") {
      return sendJson(response, 405, { error: "method not allowed" });
    }
    return await serveStatic(request, response, url.pathname);
  } catch (error) {
    sendJson(response, 500, { error: error instanceof Error ? error.message : String(error) });
  }
});

server.listen(port, host, () => {
  console.log(`Replication Lab UI listening at http://${host}:${port}`);
});

function stop() {
  server.close(() => process.exit(0));
}

process.once("SIGINT", stop);
process.once("SIGTERM", stop);
