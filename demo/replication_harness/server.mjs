import { createReadStream } from "node:fs";
import { spawn } from "node:child_process";
import { Readable } from "node:stream";
import { access, readFile, stat, unlink } from "node:fs/promises";
import { createServer } from "node:http";
import { extname, join, normalize, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { runScenario, scenarioExists, scenarioManifest } from "./scenario_runner.mjs";

const root = resolve(fileURLToPath(new URL(".", import.meta.url)));
const stateFile = process.env.DEMO_READY_CONFIG;
const privateStateFile = process.env.DEMO_PRIVATE_CONFIG;
const resultsRoot = process.env.DEMO_RESULTS_ROOT || (stateFile ? join(resolve(stateFile, ".."), "results") : join(root, "results"));
const projectRoot = process.env.DEMO_PROJECT_ROOT || resolve(root, "../..");
const port = Number(process.env.DEMO_WEB_PORT || 4180);
const host = process.env.DEMO_WEB_HOST || "127.0.0.1";
const maxBodyBytes = 16 * 1024 * 1024;
const maxResponseBytes = 32 * 1024 * 1024;
const workerRuntime = new Map();

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

async function readJsonFile(path) {
  return JSON.parse(await readFile(path, "utf8"));
}

async function readConfig() {
  return readJsonFile(stateFile);
}

async function readPrivateConfig() {
  if (!privateStateFile) return {};
  return readJsonFile(privateStateFile);
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

async function proxyEndpoint(request, response, endpoint, suffix, authToken) {
  if (!endpoint) return sendJson(response, 503, { error: "demo endpoint is not ready" });

  const target = new URL(suffix || "/", endpoint);
  const body = ["GET", "HEAD", "DELETE"].includes(request.method)
    ? undefined
    : await readRequestBody(request);
  const headers = {};
  for (const name of [
    "accept",
    "content-type",
    "x-request-id",
    "x-elixirdb-read-consistency",
    "if-none-match",
  ]) {
    if (request.headers[name]) headers[name] = request.headers[name];
  }
  if (suffix?.includes("/replication/")) headers["accept-encoding"] = "zstd";
  if (authToken) headers.authorization = `Bearer ${authToken}`;

  try {
    const upstream = await fetch(target, {
      method: request.method,
      headers,
      body,
      redirect: "manual",
    });
    const upstreamContentType = upstream.headers.get("content-type") || "application/octet-stream";
    const isStream =
      Boolean(upstream.body) &&
      (upstreamContentType.includes("application/x-ndjson") ||
        upstreamContentType.includes("text/event-stream") ||
        suffix?.includes("/attachments/get"));
    const responseHeaders = {
      "cache-control": "no-store",
      "content-type": upstreamContentType,
    };

    for (const name of [
      "etag",
      "x-request-id",
      "x-elixirdb-read-served-by",
      "x-elixirdb-source-watermark",
      "x-elixirdb-attachment-content-type",
      "content-disposition",
    ]) {
      const value = upstream.headers.get(name);
      if (value) responseHeaders[name] = value;
    }

    if (isStream) {
      const length = upstream.headers.get("content-length");
      if (length) responseHeaders["content-length"] = length;
      response.writeHead(upstream.status, responseHeaders);
      return Readable.fromWeb(upstream.body).pipe(response);
    }

    const upstreamBody = Buffer.from(await upstream.arrayBuffer());
    if (upstreamBody.byteLength > maxResponseBytes) {
      return sendJson(response, 502, { error: "upstream response exceeds the demo proxy limit" });
    }

    responseHeaders["content-length"] = upstreamBody.byteLength;
    response.writeHead(upstream.status, responseHeaders);
    response.end(upstreamBody);
  } catch (error) {
    if (!response.writableEnded) {
      sendJson(response, 502, { error: error instanceof Error ? error.message : String(error) });
    }
  }
}

async function proxy(request, response, clientKey, suffix) {
  const [config, privateConfig] = await Promise.all([readConfig(), readPrivateConfig()]);
  const client = config.clients.find((value) => value.key === clientKey);
  if (!client) return sendJson(response, 404, { error: "unknown demo client" });
  return proxyEndpoint(request, response, client.endpoint, suffix, privateConfig.source_token);
}

async function probe(endpoint, token, path, options = {}) {
  const headers = {
    accept: "application/json",
    ...(token ? { authorization: `Bearer ${token}` } : {}),
    ...(options.headers || {}),
  };
  let body = options.body;
  if (body !== undefined && body !== null && typeof body === "object") {
    headers["content-type"] ||= "application/json";
    body = JSON.stringify(body);
  }

  const response = await fetch(new URL(path, `${endpoint.replace(/\/$/, "")}/`), {
    method: options.method || "GET",
    headers,
    body,
  });
  const text = await response.text();
  let bodyValue = null;
  try {
    bodyValue = text ? JSON.parse(text) : null;
  } catch {
    bodyValue = { raw: text.slice(0, 512) };
  }
  return { status: response.status, headers: Object.fromEntries(response.headers.entries()), body: bodyValue };
}

function workerSpec(privateConfig, key) {
  return (privateConfig.workers || []).find((worker) => worker.key === key);
}

function runtimeFor(spec) {
  if (!spec) return null;
  if (!workerRuntime.has(spec.key)) {
    workerRuntime.set(spec.key, { pid: Number(spec.pid), child: null, state: "online" });
  }
  return workerRuntime.get(spec.key);
}

function processAlive(pid) {
  if (!Number.isInteger(Number(pid)) || Number(pid) <= 0) return false;
  try {
    process.kill(Number(pid), 0);
    return true;
  } catch {
    return false;
  }
}

async function waitUntil(label, predicate, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  let last;
  while (Date.now() < deadline) {
    last = await predicate();
    if (last === true) return;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`${label} timed out`);
}

async function stopWorker(key) {
  const privateConfig = await readPrivateConfig();
  const spec = workerSpec(privateConfig, key);
  if (!spec) throw new Error(`unknown shadow worker ${key}`);
  const runtime = runtimeFor(spec);
  const pid = runtime?.pid || spec.pid;

  if (processAlive(pid)) {
    process.kill(Number(pid), "SIGTERM");
    await waitUntil(`worker-${key} stop`, () => !processAlive(pid), 5_000);
  }

  runtime.state = "offline";
  runtime.child = null;
  return { key, status: "offline" };
}

async function startWorker(key) {
  const privateConfig = await readPrivateConfig();
  const spec = workerSpec(privateConfig, key);
  if (!spec) throw new Error(`unknown shadow worker ${key}`);
  const runtime = runtimeFor(spec);

  if (processAlive(runtime.pid)) return { key, status: "online", already_running: true };

  await unlink(spec.config_path).catch(() => {});
  const env = {
    ...process.env,
    MIX_ENV: "dev",
    ELIXIR_DB_ROOT: spec.database_root,
    ELIXIR_DB_PORT: String(spec.port),
    DEMO_PROJECT_ROOT: privateConfig.project_root || projectRoot,
    DEMO_WORKER_CONFIG: spec.config_path,
    DEMO_WORKER_KEY: spec.key,
    DEMO_WORKER_CONTROL_TOKEN: spec.control_token,
    DEMO_SOURCE_ENDPOINT: privateConfig.source_endpoint,
    DEMO_SOURCE_TOKEN: privateConfig.source_token,
    DEMO_ALLOWED_ATTACHMENT_ROOTS: (privateConfig.allowed_attachment_roots || []).join(":"),
  };
  const child = spawn("mix", ["run", "--no-start", "demo/replication_harness/node.exs", "worker"], {
    cwd: privateConfig.project_root || projectRoot,
    env,
    stdio: "ignore",
  });
  runtime.child = child;
  runtime.pid = child.pid;
  runtime.state = "starting";
  child.once("exit", () => {
    runtime.state = "offline";
    runtime.child = null;
  });

  await waitUntil(`worker-${key} config`, async () => {
    try {
      const config = JSON.parse(await readFile(spec.config_path, "utf8"));
      return config.pid === String(runtime.pid);
    } catch {
      return false;
    }
  }, 15_000);

  await waitUntil(`worker-${key} control plane`, async () => {
    try {
      const response = await probe(spec.control_endpoint, spec.control_token, "/v1/control-plane/capabilities");
      return response.status === 200;
    } catch {
      return false;
    }
  }, 15_000);

  runtime.state = "online";
  return { key, status: "online", restarted: true };
}

async function workerAction(key, action) {
  if (action === "stop") return stopWorker(key);
  if (action === "restart") {
    await stopWorker(key);
    return startWorker(key);
  }
  throw new Error("worker action must be stop or restart");
}

function redactShadow(value) {
  if (!value || typeof value !== "object") return value;
  const copy = JSON.parse(JSON.stringify(value));
  if (copy.desired) delete copy.desired.attachment_location;
  return copy;
}

async function labState() {
  const [config, privateConfig] = await Promise.all([readConfig(), readPrivateConfig()]);
  const sourceUuid = privateConfig.source_database_uuid;
  let shadow = null;
  let shadowError = null;

  if (privateConfig.source_endpoint && sourceUuid) {
    try {
      const response = await probe(
        privateConfig.source_endpoint,
        privateConfig.source_token,
        `/v1/databases/${sourceUuid}/shadow`,
      );
      if (response.status === 200) shadow = redactShadow(response.body?.data);
      else shadowError = `HTTP ${response.status}`;
    } catch (error) {
      shadowError = error instanceof Error ? error.message : String(error);
    }
  }

  const workers = await Promise.all((privateConfig.workers || []).map(async (spec) => {
    const runtime = runtimeFor(spec);
    const alive = processAlive(runtime?.pid || spec.pid);
    let capabilities = null;
    let error = null;
    if (alive) {
      try {
        const response = await probe(spec.control_endpoint, spec.control_token, "/v1/control-plane/capabilities");
        if (response.status === 200) capabilities = response.body?.data;
        else error = `HTTP ${response.status}`;
      } catch (probeError) {
        error = probeError instanceof Error ? probeError.message : String(probeError);
      }
    }
    return {
      key: spec.key,
      label: spec.label,
      endpoint: spec.endpoint,
      status: alive && capabilities ? "online" : "offline",
      capabilities: capabilities && {
        protocol_major: capabilities.protocol_major,
        capability_count: capabilities.capabilities?.length || 0,
      },
      error,
    };
  }));

  return {
    topology: {
      source: "A / B web node",
      native: "C native node",
      shadows: workers.map((worker) => worker.key),
    },
    source_database_uuid: sourceUuid,
    shadow,
    shadow_error: shadowError,
    workers,
    clients: config.clients,
    native_client: config.native_client,
  };
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

    if (url.pathname === "/api/lab/state" && request.method === "GET") {
      return sendJson(response, 200, { data: await labState() });
    }

    if (url.pathname === "/api/scenarios" && request.method === "GET") {
      return sendJson(response, 200, { data: scenarioManifest });
    }

    const scenarioRunMatch = url.pathname.match(/^\/api\/scenarios\/([a-z0-9-]+)\/run$/);
    if (scenarioRunMatch && request.method === "POST") {
      const scenarioId = scenarioRunMatch[1];
      if (!scenarioExists(scenarioId)) return sendJson(response, 404, { error: "unknown scenario" });
      const privateConfig = await readPrivateConfig();
      const result = await runScenario(scenarioId, {
        privateConfig,
        resultsRoot,
        workerAction,
      });
      return sendJson(response, 200, {
        data: { ...result, artifact_url: `/api/scenarios/results/${result.run_id}` },
      });
    }

    const resultMatch = url.pathname.match(/^\/api\/scenarios\/results\/([a-z0-9-]+)$/);
    if (resultMatch && request.method === "GET") {
      const path = join(resultsRoot, `${resultMatch[1]}.json`);
      if (relative(resultsRoot, path).startsWith("..")) return sendJson(response, 404, { error: "not found" });
      try {
        return sendJson(response, 200, { data: await readJsonFile(path) });
      } catch {
        return sendJson(response, 404, { error: "scenario result not found" });
      }
    }

    const workerActionMatch = url.pathname.match(/^\/api\/lab\/workers\/([ab])\/actions$/);
    if (workerActionMatch && request.method === "POST") {
      const body = JSON.parse((await readRequestBody(request)).toString("utf8") || "{}");
      return sendJson(response, 200, { data: await workerAction(workerActionMatch[1], body.action) });
    }

    const proxyMatch = url.pathname.match(/^\/api\/(a|b)(\/.*)?$/);
    if (proxyMatch) return await proxy(request, response, proxyMatch[1], proxyMatch[2] || "/");

    if (url.pathname === "/api/observability/web") {
      const [config, privateConfig] = await Promise.all([readConfig(), readPrivateConfig()]);
      return await proxyEndpoint(request, response, config.server_url, "/v1/observability/snapshot", privateConfig.source_token);
    }

    if (url.pathname === "/api/observability/native") {
      const [config, privateConfig] = await Promise.all([readConfig(), readPrivateConfig()]);
      return await proxyEndpoint(
        request,
        response,
        config.native_client?.endpoint,
        "/v1/observability/snapshot",
        privateConfig.source_token,
      );
    }

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

async function stop() {
  for (const [key] of workerRuntime) {
    try {
      await stopWorker(key);
    } catch {
      // The launcher also owns these processes and will perform final cleanup.
    }
  }
  server.close(() => process.exit(0));
}

process.once("SIGINT", () => void stop());
process.once("SIGTERM", () => void stop());
