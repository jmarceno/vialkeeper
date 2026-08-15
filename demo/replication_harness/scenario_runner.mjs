import { mkdir, writeFile } from "node:fs/promises";

const publicHeaders = [
  "content-type",
  "etag",
  "x-vialkeeper-read-served-by",
  "x-vialkeeper-source-watermark",
  "x-request-id",
];

export const scenarioManifest = [
  {
    id: "shadow-routing-and-failover",
    label: "Shadow routing + failover",
    description: "Provision a shadow copy, prove read headers and attachments, then move the route between workers after a fault.",
  },
  {
    id: "replication-queries-and-subscriptions",
    label: "Replication + queries + subscriptions",
    description: "Write through A, observe B and C, build an index, inspect a plan, and consume a real NDJSON subscription.",
  },
  {
    id: "federation-materialized-views",
    label: "Federation + materialized views",
    description: "Query A and B together, create a materialized view, and rebuild a local declarative view.",
  },
  {
    id: "retention-admission-and-integrity",
    label: "Retention + admission + integrity",
    description: "Run concurrent writes, compact the change history, collect runtime telemetry, and check durable integrity.",
  },
  {
    id: "portability-and-wire",
    label: "Portability + wire contracts",
    description: "Exercise authenticated identities, replication job state, worker capabilities, config boundaries, and integrity checks.",
  },
];

const scenarioDefinitions = {
  "shadow-routing-and-failover": runShadowRouting,
  "replication-queries-and-subscriptions": runReplicationQueries,
  "federation-materialized-views": runFederationMaterializedViews,
  "retention-admission-and-integrity": runRetentionAdmissionIntegrity,
  "portability-and-wire": runPortabilityAndWire,
};

class ScenarioError extends Error {
  constructor(message, detail = undefined) {
    super(message);
    this.name = "ScenarioError";
    this.detail = detail;
  }
}

export async function runScenario(id, context) {
  const definition = scenarioDefinitions[id];
  if (!definition) throw new ScenarioError(`unknown scenario: ${id}`);

  const runId = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const startedAt = new Date().toISOString();
  const steps = [];
  const runContext = { ...context, runId };
  let status = "passed";
  let failure = null;

  try {
    await definition(runContext, steps);
  } catch (error) {
    status = "failed";
    failure = errorSummary(error);
  }

  const finishedAt = new Date().toISOString();
  const result = {
    run_id: runId,
    scenario_id: id,
    status,
    started_at: startedAt,
    finished_at: finishedAt,
    duration_ms: Math.max(0, Date.parse(finishedAt) - Date.parse(startedAt)),
    steps,
    failure,
  };

  await mkdir(context.resultsRoot, { recursive: true });
  await writeFile(
    `${context.resultsRoot}/${runId}.json`,
    `${JSON.stringify(result, null, 2)}\n`,
    { mode: 0o600 },
  );

  return result;
}

export function scenarioExists(id) {
  return Object.hasOwn(scenarioDefinitions, id);
}

async function runShadowRouting(context, steps) {
  const source = sourceDatabase(context);
  const documentId = `shadow-${context.runId}`;
  const attachmentText = `shadow-attachment-${context.runId}`;

  await step(steps, "provision worker-a shadow", async () => {
    const response = await sourceRequest(context, `/v1/databases/${source.uuid}/shadow`, {
      method: "PUT",
      body: {
        enabled: true,
        location: "worker-a",
        attachment_location: context.privateConfig.source_attachment_location,
      },
    });
    expectStatus(response, [200, 202]);
    return { status: response.status, definition: shadowSummary(response.body?.data) };
  });

  await step(steps, "wait for worker-a ready", async () => {
    const response = await waitFor(
      "worker-a shadow ready",
      () => sourceRequest(context, `/v1/databases/${source.uuid}/shadow`),
      (candidate) => candidate.status === 200 && candidate.body?.data?.observed?.state === "ready",
      30_000,
    );
    return shadowSummary(response.body?.data);
  });

  const upload = await step(steps, "upload attachment through source API", async () => {
    const response = await rawRequest(
      context.privateConfig.source_endpoint,
      context.privateConfig.source_token,
      `/v1/databases/${source.uuid}/attachments/upload`,
      {
        method: "POST",
        body: Buffer.from(attachmentText),
        headers: { "content-type": "application/octet-stream" },
      },
    );
    expectStatus(response, [201]);
    const data = response.body?.data;
    if (!data?.blob) throw new ScenarioError("attachment upload did not return a blob digest");
    return { status: response.status, blob: data.blob, length: data.length };
  });

  const put = await step(steps, "write document with attachment", async () => {
    const response = await sourceRequest(context, `/v1/databases/${source.uuid}/documents/put`, {
      method: "POST",
      body: {
        id: documentId,
        body: { kind: "shadow-lab", run_id: context.runId, value: 1 },
        attachments: {
          "scenario.txt": { blob: upload.blob, content_type: "text/plain" },
        },
      },
    });
    expectStatus(response, [201]);
    return { status: response.status, sequence: response.body?.data?.sequence, revision: response.body?.data?.revision };
  });

  await step(steps, "wait for worker-a watermark", async () => {
    const response = await waitFor(
      "worker-a applies the source sequence",
      () => sourceRequest(context, `/v1/databases/${source.uuid}/shadow`),
      (candidate) =>
        candidate.status === 200 &&
        candidate.body?.data?.observed?.state === "ready" &&
        Number(candidate.body?.data?.observed?.applied_source_sequence || 0) >= Number(put.sequence || 0),
      30_000,
    );
    return shadowSummary(response.body?.data);
  });

  const eventualDocument = await step(steps, "read document from shadow route", async () => {
    const response = await waitFor(
      "eventual document read served by shadow",
      () => sourceRequest(context, `/v1/databases/${source.uuid}/documents/get`, {
        method: "POST",
        body: { id: documentId },
        headers: { "x-vialkeeper-read-consistency": "eventual" },
      }),
      (candidate) =>
        candidate.status === 200 &&
        candidate.headers["x-vialkeeper-read-served-by"] === "shadow" &&
        candidate.body?.data?.id === documentId,
      30_000,
    );
    return { status: response.status, headers: selectedHeaders(response.headers), id: response.body?.data?.id };
  });

  await step(steps, "read attachment from shadow route", async () => {
    const response = await waitFor(
      "eventual attachment read served by shadow",
      () => rawRequest(
        context.privateConfig.source_endpoint,
        context.privateConfig.source_token,
        `/v1/databases/${source.uuid}/attachments/get`,
        {
          method: "POST",
          body: JSON.stringify({ id: documentId, revision: null, name: "scenario.txt" }),
          headers: {
            "content-type": "application/json",
            "x-vialkeeper-read-consistency": "eventual",
          },
        },
      ),
      (candidate) =>
        candidate.status === 200 &&
        candidate.headers["x-vialkeeper-read-served-by"] === "shadow" &&
        candidate.text === attachmentText,
      30_000,
    );
    return { status: response.status, headers: selectedHeaders(response.headers), length: response.bytes.byteLength };
  });

  await step(steps, "prove primary bypasses shadow", async () => {
    const response = await sourceRequest(context, `/v1/databases/${source.uuid}/documents/get`, {
      method: "POST",
      body: { id: documentId },
      headers: { "x-vialkeeper-read-consistency": "primary" },
    });
    expectStatus(response, [200]);
    if (response.headers["x-vialkeeper-read-served-by"] !== "source") {
      throw new ScenarioError("primary read was not served by source", selectedHeaders(response.headers));
    }
    return { status: response.status, headers: selectedHeaders(response.headers) };
  });

  await step(steps, "stop worker-a", async () => context.workerAction("a", "stop"));

  await step(steps, "move shadow generation to worker-b", async () => {
    const response = await sourceRequest(context, `/v1/databases/${source.uuid}/shadow`, {
      method: "PUT",
      body: {
        enabled: true,
        location: "worker-b",
        attachment_location: context.privateConfig.source_attachment_location,
      },
    });
    expectStatus(response, [200, 202]);
    return { status: response.status, definition: shadowSummary(response.body?.data) };
  });

  await step(steps, "wait for worker-b ready", async () => {
    const response = await waitFor(
      "worker-b shadow ready",
      () => sourceRequest(context, `/v1/databases/${source.uuid}/shadow`),
      (candidate) => candidate.status === 200 && candidate.body?.data?.observed?.state === "ready",
      30_000,
    );
    return shadowSummary(response.body?.data);
  });

  await step(steps, "read after generation move", async () => {
    const response = await waitFor(
      "worker-b serves the replicated document",
      () => sourceRequest(context, `/v1/databases/${source.uuid}/documents/get`, {
        method: "POST",
        body: { id: documentId },
        headers: { "x-vialkeeper-read-consistency": "eventual" },
      }),
      (candidate) =>
        candidate.status === 200 &&
        candidate.headers["x-vialkeeper-read-served-by"] === "shadow" &&
        candidate.body?.data?.id === documentId,
      30_000,
    );
    return { status: response.status, headers: selectedHeaders(response.headers), id: response.body?.data?.id };
  });

  await step(steps, "restart worker-a and prove control plane recovery", async () => {
    const result = await context.workerAction("a", "restart");
    if (result.status !== "online") throw new ScenarioError("worker-a did not restart", result);
    return result;
  });

  return { document_id: documentId, attachment: upload, source_write: put, last_read: eventualDocument };
}

async function runReplicationQueries(context, steps) {
  const source = sourceDatabase(context);
  const target = clientDatabase(context, "b");
  const native = context.privateConfig.native_client;
  const documentId = `query-${context.runId}`;
  const body = { kind: "query-lab", run_id: context.runId, value: 7 };

  const put = await step(steps, "write query fixture to A", async () => {
    const response = await sourceRequest(context, `/v1/databases/${source.uuid}/documents/put`, {
      method: "POST",
      body: { id: documentId, body },
    });
    expectStatus(response, [201]);
    return { status: response.status, sequence: response.body?.data?.sequence, revision: response.body?.data?.revision };
  });

  await step(steps, "observe A to B replication", async () => {
    const response = await waitFor(
      "B receives query fixture",
      () => endpointRequest(context, target.endpoint, `/v1/databases/${target.uuid}/documents/get`, {
        method: "POST",
        body: { id: documentId },
      }),
      (candidate) => candidate.status === 200 && candidate.body?.data?.id === documentId,
      30_000,
    );
    return { status: response.status, id: response.body?.data?.id, sequence: put.sequence };
  });

  await step(steps, "observe A to native C replication", async () => {
    if (!native) throw new ScenarioError("native client configuration is missing");
    const response = await waitFor(
      "C receives query fixture",
      () => endpointRequest(context, native.endpoint, `/v1/databases/${native.database_uuid}/documents/get`, {
        method: "POST",
        body: { id: documentId },
      }),
      (candidate) => candidate.status === 200 && candidate.body?.data?.id === documentId,
      30_000,
    );
    return { status: response.status, id: response.body?.data?.id };
  });

  const indexName = `query-lab-${context.runId}`;
  await step(steps, "create structured query index", async () => {
    const response = await sourceRequest(context, `/v1/databases/${source.uuid}/indexes`, {
      method: "POST",
      body: {
        name: indexName,
        type: "structured",
        fields: [{ path: "/kind", type: "string" }],
      },
    });
    expectStatus(response, [201]);
    return { status: response.status, index_id: response.body?.data?.index_id, name: indexName };
  });

  await step(steps, "execute indexed query", async () => {
    const response = await sourceRequest(context, `/v1/databases/${source.uuid}/query`, {
      method: "POST",
      body: { selector: { "/kind": "query-lab" }, index: indexName, limit: 20 },
    });
    expectStatus(response, [200]);
    const results = response.body?.data?.results || [];
    if (!results.some((entry) => entry.id === documentId)) {
      throw new ScenarioError("indexed query did not return the fixture", { result_count: results.length });
    }
    return { status: response.status, result_count: results.length, matched_id: documentId };
  });

  await step(steps, "explain indexed query", async () => {
    const response = await sourceRequest(context, `/v1/databases/${source.uuid}/query/explain`, {
      method: "POST",
      body: { selector: { "/kind": "query-lab" }, index: indexName, limit: 20 },
    });
    expectStatus(response, [200]);
    return { status: response.status, plan: response.body?.data };
  });

  await step(steps, "consume subscription snapshot and caught-up events", async () => {
    const response = await readNdjson(
      context.privateConfig.source_endpoint,
      context.privateConfig.source_token,
      `/v1/databases/${source.uuid}/query/stream`,
      { query: { selector: { "/kind": "query-lab" } }, heartbeat_ms: 100 },
      ["snapshot", "caught_up"],
    );
    const types = response.events.map((event) => event.type);
    if (!types.includes("snapshot") || !types.includes("caught_up")) {
      throw new ScenarioError("subscription did not emit snapshot and caught_up", { types });
    }
    return { status: response.status, event_types: types.slice(0, 12), headers: selectedHeaders(response.headers) };
  });

  return { document_id: documentId, index: indexName, native_database_uuid: native?.database_uuid };
}

async function runFederationMaterializedViews(context, steps) {
  const source = sourceDatabase(context);
  const target = clientDatabase(context, "b");
  const documentIds = [`federation-a-${context.runId}`, `federation-b-${context.runId}`];

  await step(steps, "write federation fixtures", async () => {
    const responses = await Promise.all([
      sourceRequest(context, `/v1/databases/${source.uuid}/documents/put`, {
        method: "POST",
        body: { id: documentIds[0], body: { kind: "federation-lab", value: 1, run_id: context.runId } },
      }),
      endpointRequest(context, target.endpoint, `/v1/databases/${target.uuid}/documents/put`, {
        method: "POST",
        body: { id: documentIds[1], body: { kind: "federation-lab", value: 2, run_id: context.runId } },
      }),
    ]);
    for (const response of responses) expectStatus(response, [201]);
    return { source_status: responses[0].status, target_status: responses[1].status, document_ids: documentIds };
  });

  await step(steps, "execute federated query", async () => {
    const response = await sourceRequest(context, "/v1/federation/query", {
      method: "POST",
      body: {
        databases: [source.uuid, target.uuid],
        query: {
          selector: { "/kind": "federation-lab" },
          fields: ["/value"],
          sort: [{ path: "/value", direction: "asc" }],
          limit: 10,
        },
      },
    });
    expectStatus(response, [200]);
    const documents = response.body?.data?.documents || [];
    if (documents.length < 2) throw new ScenarioError("federated query returned fewer than two fixtures");
    return {
      status: response.status,
      document_count: documents.length,
      sources: (response.body?.data?.sources || []).map((entry) => entry.database_uuid),
    };
  });

  const materializedName = `materialized-lab-${context.runId}`;
  const materialized = await step(steps, "create materialized view", async () => {
    const response = await sourceRequest(context, "/v1/materialized-views", {
      method: "POST",
      body: {
        name: materializedName,
        sources: [source.uuid],
        map: { key: [{ path: "/kind" }], value: { path: "/value" } },
      },
    });
    expectStatus(response, [201]);
    const data = response.body?.data;
    if (!data?.database_uuid) throw new ScenarioError("materialized view did not return a database UUID");
    return { status: response.status, database_uuid: data.database_uuid, name: materializedName };
  });

  await step(steps, "read generated materialized document", async () => {
    const response = await waitFor(
      "materialized database has generated rows",
      () => sourceRequest(context, `/v1/databases/${materialized.database_uuid}/query`, {
        method: "POST",
        body: { selector: {}, limit: 20 },
      }),
      (candidate) => candidate.status === 200 && (candidate.body?.data?.results || []).length > 0,
      30_000,
    );
    return { status: response.status, result_count: response.body?.data?.results?.length || 0 };
  });

  const viewName = `view-lab-${context.runId}`;
  const view = await step(steps, "create and query declarative view", async () => {
    const created = await sourceRequest(context, `/v1/databases/${source.uuid}/views`, {
      method: "POST",
      body: { name: viewName, key: [{ path: "/kind" }], value: { path: "/value" }, reducer: "_sum" },
    });
    expectStatus(created, [201]);
    const viewId = created.body?.data?.view_id;
    if (!viewId) throw new ScenarioError("view creation did not return a view_id");
    const queried = await waitFor(
      "declarative view becomes queryable",
      () => sourceRequest(context, `/v1/databases/${source.uuid}/views/${viewId}/query`, {
        method: "POST",
        body: { start_key: ["federation-lab"], end_key: ["federation-lab"], group_level: 1, limit: 10 },
      }),
      (candidate) => candidate.status === 200 && (candidate.body?.data?.results || []).length > 0,
      30_000,
    );
    return { status: queried.status, view_id: viewId, result_count: queried.body?.data?.results?.length || 0 };
  });

  return { materialized_database_uuid: materialized.database_uuid, materialized_name: materializedName, view_name: viewName, view_id: view.view_id };
}

async function runRetentionAdmissionIntegrity(context, steps) {
  const source = sourceDatabase(context);
  const writes = Array.from({ length: 8 }, (_, index) =>
    sourceRequest(context, `/v1/databases/${source.uuid}/documents/put`, {
      method: "POST",
      body: { id: `retention-${context.runId}-${index}`, body: { kind: "retention-lab", index, run_id: context.runId } },
    }),
  );

  await step(steps, "run concurrent foreground writes", async () => {
    const responses = await Promise.all(writes);
    for (const response of responses) expectStatus(response, [201]);
    return { request_count: responses.length, sequences: responses.map((response) => response.body?.data?.sequence) };
  });

  const before = await step(steps, "read retention identity before compact", async () => {
    const response = await sourceRequest(context, `/v1/databases/${source.uuid}/replication/identity`);
    expectStatus(response, [200]);
    return { status: response.status, identity: retentionSummary(response.body?.data) };
  });

  const compact = await step(steps, "compact retained history", async () => {
    const response = await sourceRequest(context, `/v1/databases/${source.uuid}/compact`, {
      method: "POST",
      body: {},
    });
    expectStatus(response, [200]);
    return { status: response.status, stats: response.body?.data };
  });

  const integrity = await step(steps, "run source integrity check", async () => {
    const response = await sourceRequest(context, `/v1/databases/${source.uuid}/integrity-check`, {
      method: "POST",
      body: {},
    });
    expectStatus(response, [200]);
    return { status: response.status, result: response.body?.data };
  });

  const telemetry = await step(steps, "collect post-load observability", async () => {
    const response = await sourceRequest(context, "/v1/observability/snapshot");
    expectStatus(response, [200]);
    const data = response.body?.data || {};
    return { status: response.status, status_name: data.status, runtime: data.runtime, errors: data.otel?.errors };
  });

  return { before_retention: before.identity, compact: compact.stats, integrity: integrity.result, telemetry_status: telemetry.status_name };
}

async function runPortabilityAndWire(context, steps) {
  const source = sourceDatabase(context);
  const target = clientDatabase(context, "b");
  const native = context.privateConfig.native_client;

  const identities = await step(steps, "read authenticated replication identities", async () => {
    const endpoints = [
      ["a", context.privateConfig.source_endpoint, source.uuid, context.privateConfig.source_token],
      ["b", target.endpoint, target.uuid, context.privateConfig.source_token],
      ["c", native?.endpoint, native?.database_uuid, context.privateConfig.source_token],
    ];
    const values = {};
    for (const [key, endpoint, uuid, token] of endpoints) {
      if (!endpoint || !uuid) continue;
      const response = await endpointRequest(context, endpoint, `/v1/databases/${uuid}/replication/identity`, {
        token,
      });
      expectStatus(response, [200]);
      values[key] = retentionSummary(response.body?.data);
    }
    return values;
  });

  await step(steps, "inspect durable replication jobs", async () => {
    const [sourceResponse, targetResponse] = await Promise.all([
      sourceRequest(context, `/v1/databases/${source.uuid}/replications`),
      endpointRequest(context, target.endpoint, `/v1/databases/${target.uuid}/replications`),
    ]);
    expectStatus(sourceResponse, [200]);
    expectStatus(targetResponse, [200]);
    const sourceJobs = sourceResponse.body?.data || [];
    const targetJobs = targetResponse.body?.data || [];
    if (sourceJobs.length < 2 || targetJobs.length < 1) {
      throw new ScenarioError("expected A↔B and A→C durable jobs", {
        source_job_count: sourceJobs.length,
        target_job_count: targetJobs.length,
      });
    }
    return {
      source_job_count: sourceJobs.length,
      target_job_count: targetJobs.length,
      source_job_ids: sourceJobs.map((job) => job.job_id),
      target_job_ids: targetJobs.map((job) => job.job_id),
    };
  });

  await step(steps, "check worker control capabilities", async () => {
    const values = {};
    for (const worker of context.privateConfig.workers || []) {
      const response = await endpointRequest(context, worker.control_endpoint, "/v1/control-plane/capabilities", {
        token: worker.control_token,
      });
      expectStatus(response, [200]);
      values[worker.key] = {
        status: response.status,
        protocol_major: response.body?.data?.protocol_major,
        capabilities: response.body?.data?.capabilities,
      };
    }
    return values;
  });

  await step(steps, "inspect config and database boundaries", async () => {
    const config = await sourceRequest(context, `/v1/databases/${source.uuid}/config`);
    expectStatus(config, [200]);
    const databases = await sourceRequest(context, "/v1/databases");
    expectStatus(databases, [200]);
    const visible = databases.body?.data || [];
    if (visible.some((entry) => entry.database_kind === "shadow")) {
      throw new ScenarioError("shadow database leaked through the ordinary database list");
    }
    return { config_status: config.status, visible_database_count: visible.length };
  });

  await step(steps, "run integrity checks on A and B", async () => {
    const responses = await Promise.all([
      sourceRequest(context, `/v1/databases/${source.uuid}/integrity-check`, { method: "POST", body: {} }),
      sourceRequest(context, `/v1/databases/${target.uuid}/integrity-check`, { method: "POST", body: {} }),
    ]);
    for (const response of responses) expectStatus(response, [200]);
    return { a_status: responses[0].status, b_status: responses[1].status };
  });

  return { identity_keys: Object.keys(identities), workers: (context.privateConfig.workers || []).map((worker) => worker.key) };
}

async function step(steps, name, action) {
  const started = Date.now();
  try {
    const detail = await action();
    const record = {
      name,
      status: "passed",
      duration_ms: Date.now() - started,
      detail: summarize(detail),
    };
    steps.push(record);
    return detail;
  } catch (error) {
    steps.push({
      name,
      status: "failed",
      duration_ms: Date.now() - started,
      error: errorSummary(error),
    });
    throw error;
  }
}

async function waitFor(label, action, predicate, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  let last;

  while (Date.now() < deadline) {
    last = await action();
    if (predicate(last)) return last;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }

  throw new ScenarioError(`${label} timed out`, summarize(last));
}

async function sourceRequest(context, path, options = {}) {
  return endpointRequest(context, context.privateConfig.source_endpoint, path, {
    ...options,
    token: context.privateConfig.source_token,
  });
}

async function endpointRequest(context, endpoint, path, options = {}) {
  return rawRequest(
    endpoint,
    options.token ?? context.privateConfig.source_token,
    path,
    options,
  );
}

async function rawRequest(endpoint, token, path, options = {}) {
  const headers = {
    accept: "application/json",
    ...(path.includes("/replication/") ? { "accept-encoding": "zstd" } : {}),
    ...(token ? { authorization: `Bearer ${token}` } : {}),
    ...(options.headers || {}),
  };
  let body = options.body;

  if (body !== undefined && body !== null && typeof body === "object" && !Buffer.isBuffer(body) && !(body instanceof Uint8Array)) {
    headers["content-type"] ||= "application/json";
    body = JSON.stringify(body);
  }

  const response = await fetch(new URL(path, `${endpoint.replace(/\/$/, "")}/`), {
    method: options.method || "GET",
    headers,
    body,
  });
  const bytes = Buffer.from(await response.arrayBuffer());
  const responseHeaders = Object.fromEntries(response.headers.entries());
  const contentType = responseHeaders["content-type"] || "";
  const text = bytes.toString("utf8");
  let parsedBody = null;

  if (bytes.byteLength > 0 && contentType.includes("json")) {
    try {
      parsedBody = JSON.parse(text);
    } catch {
      parsedBody = { raw: text.slice(0, 512) };
    }
  }

  return { status: response.status, ok: response.ok, headers: responseHeaders, body: parsedBody, text, bytes };
}

async function readNdjson(endpoint, token, path, body, stopTypes) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 15_000);
  const response = await fetch(new URL(path, `${endpoint.replace(/\/$/, "")}/`), {
    method: "POST",
    headers: { accept: "application/x-ndjson", "content-type": "application/json", authorization: `Bearer ${token}` },
    body: JSON.stringify(body),
    signal: controller.signal,
  });

  try {
    if (!response.ok || !response.body) {
      const text = await response.text();
      throw new ScenarioError("NDJSON endpoint failed", { status: response.status, body: text.slice(0, 512) });
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    const events = [];
    let buffer = "";

    while (events.length < 50) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n");
      buffer = lines.pop() || "";

      for (const line of lines) {
        if (!line.trim()) continue;
        try {
          const event = JSON.parse(line);
          events.push(event);
          if (stopTypes.every((type) => events.some((candidate) => candidate.type === type))) {
            await reader.cancel();
            return { status: response.status, headers: Object.fromEntries(response.headers.entries()), events };
          }
        } catch {
          throw new ScenarioError("NDJSON endpoint returned an invalid event", { line: line.slice(0, 512) });
        }
      }
    }

    return { status: response.status, headers: Object.fromEntries(response.headers.entries()), events };
  } finally {
    clearTimeout(timeout);
  }
}

function expectStatus(response, statuses) {
  if (!statuses.includes(response.status)) {
    throw new ScenarioError(`expected HTTP ${statuses.join(" or ")}, got ${response.status}`, {
      status: response.status,
      body: response.body,
      headers: selectedHeaders(response.headers),
    });
  }
}

function sourceDatabase(context) {
  return { uuid: context.privateConfig.source_database_uuid, endpoint: context.privateConfig.source_endpoint };
}

function clientDatabase(context, key) {
  const client = (context.privateConfig.clients || []).find((candidate) => candidate.key === key);
  if (!client) throw new ScenarioError(`client ${key} is missing from private config`);
  return { uuid: client.database_uuid, endpoint: client.endpoint };
}

function selectedHeaders(headers) {
  return Object.fromEntries(publicHeaders.filter((name) => headers?.[name]).map((name) => [name, headers[name]]));
}

function shadowSummary(data) {
  return {
    enabled: data?.enabled,
    desired: data?.desired && {
      location: data.desired.location,
      generation: data.desired.generation,
      state: data.desired.state,
      shadow_uuid: data.desired.shadow_uuid,
    },
    observed: data?.observed && {
      state: data.observed.state,
      applied_source_sequence: data.observed.applied_source_sequence,
      worker_node_id: data.observed.worker_node_id,
      last_error_code: data.observed.last_error_code,
    },
  };
}

function retentionSummary(data) {
  return {
    database_uuid: data?.database_uuid,
    current_sequence: data?.current_sequence,
    retention_floor: data?.retention_floor,
    retention_mode: data?.retention_mode,
    history_epoch: data?.history_epoch,
  };
}

function summarize(value, depth = 0) {
  if (value === null || value === undefined || typeof value === "boolean" || typeof value === "number") return value;
  if (depth > 3) return "[truncated]";
  if (typeof value === "string") return value.length > 512 ? `${value.slice(0, 512)}…` : value;
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) return { bytes: value.byteLength };
  if (Array.isArray(value)) return value.slice(0, 20).map((entry) => summarize(entry, depth + 1));
  if (typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .slice(0, 40)
        .map(([key, entry]) => [
          key,
          sensitiveResultKey(key) ? "[redacted]" : summarize(entry, depth + 1),
        ]),
    );
  }
  return String(value);
}

function sensitiveResultKey(key) {
  return /(?:token|authorization|attachment_location|source_bearer_token|control_bearer_token)/i.test(key);
}

function errorSummary(error) {
  return {
    type: error?.name || "Error",
    message: error?.message || String(error),
    detail: summarize(error?.detail),
  };
}
