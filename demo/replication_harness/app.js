const state = {
  config: null,
  clients: new Map(),
  eventCount: 0,
  observabilityRefreshing: false,
  labState: null,
  scenarios: [],
  scenarioRunning: false,
};

const elements = {
  clientGrid: document.querySelector("#client-grid"),
  globalStatus: document.querySelector("#global-status"),
  globalStatusText: document.querySelector("#global-status-text"),
  eventCount: document.querySelector("#event-count"),
  nativeTitle: document.querySelector("#native-title"),
  nativeCopy: document.querySelector("#native-copy"),
  nativeDetails: document.querySelector("#native-details"),
  observabilityStatus: document.querySelector("#observability-status"),
  observabilityStatusText: document.querySelector("#observability-status-text"),
  observabilityNote: document.querySelector("#observability-note"),
  labStatus: document.querySelector("#lab-status"),
  labStatusText: document.querySelector("#lab-status-text"),
  topologyGrid: document.querySelector("#topology-grid"),
  shadowState: document.querySelector("#shadow-state"),
  shadowSummary: document.querySelector('[data-role="shadow-summary"]'),
  scenarioGrid: document.querySelector("#scenario-grid"),
  scenarioResult: document.querySelector("#scenario-result"),
  resultStatus: document.querySelector('[data-role="result-status"]'),
  resultBody: document.querySelector('[data-role="result-body"]'),
};

const sleep = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds));

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function pretty(value) {
  return JSON.stringify(value, null, 2);
}

function formatTime(value = Date.now()) {
  return new Date(value).toLocaleTimeString([], { hour12: false });
}

function updateGlobalStatus(stateName, text) {
  elements.globalStatus.dataset.state = stateName;
  elements.globalStatusText.textContent = text;
}

function telemetryPanel(kind) {
  return document.querySelector(`[data-telemetry-node="${CSS.escape(kind)}"]`);
}

function setTelemetryText(panel, metric, value) {
  panel?.querySelector(`[data-metric="${metric}"]`)?.replaceChildren(document.createTextNode(value));
}

function formatCount(value) {
  return Number.isFinite(Number(value)) ? Math.round(Number(value)).toLocaleString() : "—";
}

function formatDuration(value) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return "—";
  const milliseconds = Number(value);
  if (milliseconds < 0.01) return "<0.01 ms";
  if (milliseconds >= 1_000) return `${(milliseconds / 1_000).toFixed(2)} s`;
  const digits = milliseconds >= 100 ? 0 : milliseconds >= 10 ? 1 : 2;
  return `${milliseconds.toFixed(digits)} ms`;
}

function formatLatency(summary) {
  return `avg ${formatDuration(summary?.avg_ms)} · p95 ${formatDuration(summary?.p95_ms)}`;
}

function formatMemory(bytes) {
  if (!Number.isFinite(Number(bytes))) return "—";
  return `${(Number(bytes) / 1024 / 1024).toFixed(1)} MB`;
}

function renderTelemetry(kind, snapshot) {
  const panel = telemetryPanel(kind);
  if (!panel) return;

  const status = panel.querySelector('[data-role="telemetry-status"]');
  const statusText = panel.querySelector('[data-role="telemetry-status-text"]');
  const sample = panel.querySelector('[data-role="telemetry-sample"]');

  if (!snapshot?.otel?.available) {
    status.dataset.state = "starting";
    statusText.textContent = "Warming up…";
    sample.textContent = "Waiting for the OTel reader to export its first sample";
    for (const metric of ["http-count", "commands-count", "replication-count", "errors", "memory", "run-queue", "workers"]) {
      setTelemetryText(panel, metric, "—");
    }
    setTelemetryText(panel, "http-latency", "avg — · p95 —");
    setTelemetryText(panel, "commands-latency", "avg — · p95 —");
    setTelemetryText(panel, "replication-latency", "avg — · p95 —");
    setTelemetryText(panel, "error-detail", "HTTP · database · replication");
    return;
  }

  const nodeState = snapshot.status === "busy" ? "working" : "connected";
  status.dataset.state = nodeState;
  statusText.textContent = snapshot.status === "busy" ? "Busy" : "Healthy";

  const otel = snapshot.otel;
  const runtime = snapshot.runtime || {};
  setTelemetryText(panel, "http-count", formatCount(otel.http?.count));
  setTelemetryText(panel, "http-latency", formatLatency(otel.http));
  setTelemetryText(panel, "commands-count", formatCount(otel.commands?.count));
  setTelemetryText(panel, "commands-latency", formatLatency(otel.commands));
  setTelemetryText(panel, "replication-count", formatCount(otel.replication?.count));
  setTelemetryText(panel, "replication-latency", formatLatency(otel.replication));
  const errorCounts = otel.errors || {};
  const totalErrors = Object.values(errorCounts).reduce((total, value) => total + Number(value || 0), 0);
  setTelemetryText(panel, "errors", formatCount(totalErrors));
  setTelemetryText(
    panel,
    "error-detail",
    `HTTP ${formatCount(errorCounts.http)} · db ${formatCount((errorCounts.commands || 0) + (errorCounts.changes || 0))} · repl ${formatCount(errorCounts.replication)}`,
  );
  setTelemetryText(panel, "memory", formatMemory(runtime.memory_bytes));
  setTelemetryText(
    panel,
    "run-queue",
    `${formatCount(runtime.run_queue)} / ${formatCount(runtime.schedulers_online)}`,
  );
  setTelemetryText(panel, "workers", formatCount(runtime.replication_workers));
  sample.textContent = snapshot.sampled_at
    ? `Sampled ${formatTime(snapshot.sampled_at)} · ${formatCount(runtime.open_databases)} / ${formatCount(runtime.registered_databases)} databases open`
    : "Sample exported without a timestamp";
}

function renderTelemetryUnavailable(kind, error) {
  const panel = telemetryPanel(kind);
  if (!panel) return;
  const status = panel.querySelector('[data-role="telemetry-status"]');
  status.dataset.state = "error";
  panel.querySelector('[data-role="telemetry-status-text"]').textContent = "Unavailable";
  panel.querySelector('[data-role="telemetry-sample"]').textContent = error.message;
  for (const metric of ["http-count", "commands-count", "replication-count", "errors", "memory", "run-queue", "workers"]) {
    setTelemetryText(panel, metric, "—");
  }
  setTelemetryText(panel, "http-latency", "avg — · p95 —");
  setTelemetryText(panel, "commands-latency", "avg — · p95 —");
  setTelemetryText(panel, "replication-latency", "avg — · p95 —");
  setTelemetryText(panel, "error-detail", "No sample");
}

async function fetchLab(path, options = {}) {
  const response = await fetch(path, {
    cache: "no-store",
    ...options,
    headers: {
      accept: "application/json",
      ...(options.headers || {}),
    },
  });
  let envelope;
  try {
    envelope = await response.json();
  } catch {
    throw new Error(`HTTP ${response.status} returned a non-JSON lab response`);
  }
  if (!response.ok || envelope.error) {
    throw new Error(envelope.error?.message || `HTTP ${response.status}`);
  }
  return envelope.data;
}

function setLabStatus(stateName, text) {
  elements.labStatus.dataset.state = stateName;
  elements.labStatusText.textContent = text;
}

function renderLabState(data) {
  state.labState = data;
  const workers = data.workers || [];
  const topologyCards = [
    {
      label: "Source node",
      value: "A / B",
      detail: data.source_database_uuid || "Waiting for source UUID",
      status: "online",
    },
    {
      label: "Native node",
      value: "C",
      detail: data.native_client?.database_uuid || "Waiting for native UUID",
      status: data.native_client ? "online" : "starting",
    },
    ...workers.map((worker) => ({
      label: worker.label || `Shadow worker ${worker.key}`,
      workerKey: worker.key,
      value: worker.status === "online" ? "Online" : "Offline",
      detail: worker.capabilities
        ? `protocol ${worker.capabilities.protocol_major} · ${worker.capabilities.capability_count} capabilities`
        : worker.error || worker.endpoint,
      status: worker.status === "online" ? "online" : "error",
    })),
  ];

  elements.topologyGrid.innerHTML = topologyCards
    .map(
      (card) => `
        <article class="topology-card">
          <div class="topology-card-heading">
            <span class="field-label">${escapeHtml(card.label)}</span>
            <span class="topology-state" data-state="${escapeHtml(card.status)}"><span class="status-dot"></span>${escapeHtml(card.value)}</span>
          </div>
          <strong>${escapeHtml(card.detail)}</strong>
          ${
            card.workerKey
              ? `<div class="topology-actions"><button class="button" data-worker-action="stop" data-worker="${escapeHtml(card.workerKey)}">Stop</button><button class="button" data-worker-action="restart" data-worker="${escapeHtml(card.workerKey)}">Restart</button></div>`
              : ""
          }
        </article>
      `,
    )
    .join("");

  const shadow = data.shadow;
  elements.shadowSummary.textContent = pretty(
    shadow
      ? {
          enabled: shadow.enabled,
          desired: shadow.desired
            ? {
                location: shadow.desired.location,
                generation: shadow.desired.generation,
                state: shadow.desired.state,
                shadow_uuid: shadow.desired.shadow_uuid,
              }
            : null,
          observed: shadow.observed,
          error: data.shadow_error,
        }
      : { state: "absent", error: data.shadow_error },
  );

  for (const button of elements.topologyGrid.querySelectorAll("[data-worker-action]")) {
    button.addEventListener("click", () => runWorkerAction(button.dataset.worker, button.dataset.workerAction));
  }

  if (workers.length > 0 && workers.every((worker) => worker.status === "online")) {
    setLabStatus("connected", "Topology live · workers authenticated");
  } else {
    setLabStatus("working", "Topology reachable · one or more workers offline");
  }
}

function renderScenarios() {
  elements.scenarioGrid.innerHTML = state.scenarios
    .map(
      (scenario) => `
        <article class="scenario-card" data-scenario="${escapeHtml(scenario.id)}">
          <div>
            <p class="scenario-id">${escapeHtml(scenario.id)}</p>
            <h4>${escapeHtml(scenario.label)}</h4>
            <p>${escapeHtml(scenario.description)}</p>
          </div>
          <button class="button button-primary" data-action="run-scenario" ${state.scenarioRunning ? "disabled" : ""}>Run recipe</button>
        </article>
      `,
    )
    .join("");

  for (const card of elements.scenarioGrid.querySelectorAll("[data-scenario]")) {
    card.querySelector('[data-action="run-scenario"]').addEventListener("click", () => runScenario(card.dataset.scenario));
  }
}

function renderScenarioResult(result) {
  elements.scenarioResult.dataset.state = result.status || "idle";
  elements.resultStatus.textContent = result.status
    ? `${result.scenario_id} · ${result.status} · ${formatDuration(result.duration_ms)}`
    : "No scenario run yet";
  elements.resultBody.textContent = pretty(result);
}

async function refreshLabState() {
  try {
    renderLabState(await fetchLab("/api/lab/state"));
  } catch (error) {
    setLabStatus("error", error.message);
    elements.shadowSummary.textContent = error.message;
  }
}

async function loadScenarios() {
  try {
    state.scenarios = await fetchLab("/api/scenarios");
    renderScenarios();
  } catch (error) {
    elements.scenarioGrid.innerHTML = `<p class="lab-error">${escapeHtml(error.message)}</p>`;
  }
}

async function runWorkerAction(worker, action) {
  setLabStatus("working", `${action === "restart" ? "Restarting" : "Stopping"} worker-${worker}…`);
  try {
    await fetchLab(`/api/lab/workers/${encodeURIComponent(worker)}/actions`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ action }),
    });
    await refreshLabState();
  } catch (error) {
    setLabStatus("error", error.message);
  }
}

async function runScenario(id) {
  if (state.scenarioRunning) return;
  state.scenarioRunning = true;
  renderScenarios();
  elements.scenarioResult.dataset.state = "working";
  elements.resultStatus.textContent = `${id} · running…`;
  elements.resultBody.textContent = "The controller is waiting on real persisted state and bounded HTTP probes.";

  try {
    const result = await fetchLab(`/api/scenarios/${encodeURIComponent(id)}/run`, { method: "POST" });
    renderScenarioResult(result);
    await refreshLabState();
  } catch (error) {
    renderScenarioResult({ scenario_id: id, status: "failed", failure: { message: error.message } });
  } finally {
    state.scenarioRunning = false;
    renderScenarios();
  }
}

async function fetchTelemetry(path) {
  const response = await fetch(path, {
    cache: "no-store",
    headers: { accept: "application/json" },
  });
  let envelope;
  try {
    envelope = await response.json();
  } catch {
    throw new Error(`HTTP ${response.status} returned a non-JSON telemetry response`);
  }
  if (!response.ok || envelope.error) {
    throw new Error(envelope.error?.message || `HTTP ${response.status}`);
  }
  return envelope.data;
}

async function refreshObservability() {
  if (!state.config || state.observabilityRefreshing) return;
  state.observabilityRefreshing = true;

  const requests = [
    ["web", "/api/observability/web"],
    ["native", "/api/observability/native"],
  ];
  const results = await Promise.allSettled(requests.map(([, path]) => fetchTelemetry(path)));
  const successes = [];

  results.forEach((result, index) => {
    const [kind] = requests[index];
    if (result.status === "fulfilled") {
      renderTelemetry(kind, result.value);
      successes.push(kind);
    } else {
      renderTelemetryUnavailable(kind, result.reason);
    }
  });

  if (successes.length === requests.length) {
    elements.observabilityStatus.dataset.state = "connected";
    elements.observabilityStatusText.textContent = "Live · OTel samples arriving";
    elements.observabilityNote.textContent =
      "Latency values come from exported OTel histograms; p95 is bucket-based and runtime values are a point-in-time BEAM sample.";
  } else if (successes.length > 0) {
    elements.observabilityStatus.dataset.state = "working";
    elements.observabilityStatusText.textContent = "Partial telemetry";
    elements.observabilityNote.textContent = "One node is not returning telemetry yet; the available node is still live.";
  } else {
    elements.observabilityStatus.dataset.state = "error";
    elements.observabilityStatusText.textContent = "Telemetry unavailable";
    elements.observabilityNote.textContent = "The local OTel snapshot endpoints could not be reached.";
  }

  state.observabilityRefreshing = false;
}

function clientCard(client) {
  const defaultBody = {
    type: "manual-note",
    source: client.key === "a" ? "Client A" : "Client B",
    message: "Edit this JSON or use Burst writes.",
    sent_at: new Date().toISOString(),
  };

  return `
    <article class="client-panel" data-client="${client.key}">
      <div class="client-title">
        <h2>${escapeHtml(client.label)} <span>${escapeHtml(client.database_label)}</span></h2>
        <div class="client-status" data-state="working" data-role="status">
          <span class="status-dot"></span><span data-role="status-text">Connecting…</span>
        </div>
      </div>
      <div class="client-meta">
        <label>
          <span class="field-label">Database ID</span>
          <input class="text-input" value="${escapeHtml(client.database_uuid)}" readonly />
        </label>
        <label>
          <span class="field-label">HTTP endpoint</span>
          <input class="text-input" value="${escapeHtml(client.endpoint)}" readonly />
        </label>
      </div>
      <div class="compose-row">
        <label>
            <span class="field-label">Write document body · id is generated automatically</span>
          <textarea class="json-editor" data-role="body-editor" spellcheck="false">${escapeHtml(pretty(defaultBody))}</textarea>
        </label>
        <div class="button-stack">
          <button class="button button-primary" data-action="write">Write document</button>
          <button class="button" data-action="burst">⚡ Burst writes</button>
          <div class="stress-controls">
            <span class="field-label">Stress settings</span>
            <label>
              <span class="field-label">Count</span>
              <select class="select-input" data-role="burst-count">
                <option>5</option><option selected>10</option><option>25</option><option>50</option>
              </select>
            </label>
            <label>
              <span class="field-label">Interval</span>
              <select class="select-input" data-role="burst-interval">
                <option value="0">0 ms</option><option value="25">25 ms</option><option selected value="100">100 ms</option>
              </select>
            </label>
          </div>
        </div>
      </div>
      <div class="preview-row">
        <div>
          <span class="field-label">Current document · local view</span>
          <pre class="document-preview" data-role="document-preview">No document observed yet.</pre>
        </div>
        <div class="last-write" data-role="last-write">
          <div class="last-write-row"><span>Last write</span><span data-role="last-write-time">—</span></div>
          <div class="last-write-row"><span>Local revision</span><span data-role="last-write-revision">—</span></div>
          <div class="last-write-row"><span>Last sequence</span><span data-role="last-sequence">0</span></div>
        </div>
      </div>
      <div class="events-heading">
        <h3>Replication events</h3>
        <label><input type="checkbox" data-role="auto-scroll" checked /> Auto-scroll</label>
      </div>
      <div class="events-list" data-role="events" aria-live="polite">
        <div class="event-row"><span class="event-origin">Waiting for changes…</span></div>
      </div>
    </article>
  `;
}

function renderClients() {
  elements.clientGrid.innerHTML = [...state.clients.values()].map(clientCard).join("");
  for (const client of state.clients.values()) {
    const panel = panelFor(client);
    panel.querySelector('[data-action="write"]').addEventListener("click", () => writeFromEditor(client));
    panel.querySelector('[data-action="burst"]').addEventListener("click", () => burstWrites(client));
  }
}

function panelFor(client) {
  return elements.clientGrid.querySelector(`[data-client="${CSS.escape(client.key)}"]`);
}

function setClientStatus(client, stateName, text) {
  const status = panelFor(client)?.querySelector('[data-role="status"]');
  if (!status) return;
  status.dataset.state = stateName;
  status.querySelector('[data-role="status-text"]').textContent = text;
}

function setLastWrite(client, data) {
  const panel = panelFor(client);
  if (!panel) return;
  panel.querySelector('[data-role="last-write-time"]').textContent = formatTime();
  panel.querySelector('[data-role="last-write-revision"]').textContent = data.revision || "—";
  panel.querySelector('[data-role="last-sequence"]').textContent = data.sequence ?? client.lastSequence;
}

function setPreview(client, documentValue) {
  const panel = panelFor(client);
  if (!panel) return;
  panel.querySelector('[data-role="document-preview"]').textContent = pretty(documentValue);
}

function addEvent(client, event) {
  const panel = panelFor(client);
  if (!panel) return;
  const list = panel.querySelector('[data-role="events"]');
  const empty = list.querySelector(".event-row:only-child .event-origin");
  if (empty?.textContent.startsWith("Waiting")) list.replaceChildren();

  const kind = event.origin === "local" ? "LOCAL" : "IN";
  const row = document.createElement("div");
  row.className = "event-row";
  row.innerHTML = `
    <span class="event-kind ${kind === "IN" ? "in" : "local"}">${kind}</span>
    <span>${escapeHtml(formatTime(event.receivedAt))}</span>
    <span class="event-id" title="${escapeHtml(event.documentId)}">${escapeHtml(event.documentId)}</span>
    <span class="event-sequence">#${escapeHtml(event.sequence)}</span>
  `;
  list.prepend(row);
  while (list.children.length > 40) list.lastElementChild.remove();
  if (panel.querySelector('[data-role="auto-scroll"]').checked) list.scrollTop = 0;

  state.eventCount += 1;
  elements.eventCount.textContent = String(state.eventCount);
}

async function request(client, path, body) {
  const options = { headers: { accept: "application/json" } };
  if (body !== undefined) {
    options.method = "POST";
    options.headers["content-type"] = "application/json";
    options.body = JSON.stringify(body);
  }

  const response = await fetch(`${client.apiBase}${path}`, options);
  let envelope;
  try {
    envelope = await response.json();
  } catch {
    throw new Error(`HTTP ${response.status} returned a non-JSON response`);
  }

  if (!response.ok || envelope.error) {
    throw new Error(envelope.error?.message || `HTTP ${response.status}`);
  }
  return envelope.data;
}

async function checkConnection(client) {
  try {
    const identity = await request(client, `/v1/databases/${client.database_uuid}/replication/identity`);
    client.lastSequence = 0;
    client.identity = identity;
    setClientStatus(client, "connected", "Connected");
    return true;
  } catch (error) {
    client.error = error;
    setClientStatus(client, "error", "Unavailable");
    return false;
  }
}

async function writeDocument(client, id, body) {
  setClientStatus(client, "working", "Writing…");
  try {
    const data = await request(client, `/v1/databases/${client.database_uuid}/documents/put`, {
      id,
      body,
    });
    client.documents.set(id, { id, revision: data.revision, body, sequence: data.sequence });
    setPreview(client, client.documents.get(id));
    setLastWrite(client, data);
    setClientStatus(client, "connected", "Connected");
    return data;
  } catch (error) {
    client.error = error;
    setClientStatus(client, "error", error.message);
    throw error;
  }
}

async function writeFromEditor(client) {
  const panel = panelFor(client);
  const button = panel.querySelector('[data-action="write"]');
  button.disabled = true;
  try {
    const body = JSON.parse(panel.querySelector('[data-role="body-editor"]').value);
    const id = `manual-${client.key}-${Date.now()}`;
    await writeDocument(client, id, body);
  } catch (error) {
    showError(
      client,
      error.message === "Unexpected end of JSON input" ? "Body JSON is incomplete." : error.message,
    );
  } finally {
    button.disabled = false;
  }
}

async function burstWrites(client) {
  const panel = panelFor(client);
  const button = panel.querySelector('[data-action="burst"]');
  const count = Number(panel.querySelector('[data-role="burst-count"]').value);
  const interval = Number(panel.querySelector('[data-role="burst-interval"]').value);
  button.disabled = true;

  try {
    const body = JSON.parse(panel.querySelector('[data-role="body-editor"]').value);
    setClientStatus(client, "working", `Bursting ${count}…`);
    for (let index = 0; index < count; index += 1) {
      const id = `burst-${client.key}-${Date.now()}-${index}`;
      await writeDocument(client, id, { ...body, burst_index: index, burst_count: count });
      if (interval > 0) await sleep(interval);
    }
    setClientStatus(client, "connected", "Connected");
  } catch (error) {
    showError(client, error.message);
  } finally {
    button.disabled = false;
  }
}

async function readChanges(client) {
  const result = await request(client, `/v1/databases/${client.database_uuid}/changes`, {
    since: client.lastSequence,
    limit: 100,
    wait_ms: 1_000,
  });

  for (const change of result.results || []) {
    if (change.sequence <= client.lastSequence) continue;
    client.lastSequence = Math.max(client.lastSequence, change.sequence);
    const id = change.document_id;
    let documentValue;
    try {
      documentValue = await request(client, `/v1/databases/${client.database_uuid}/documents/get`, { id });
      client.documents.set(id, documentValue);
      setPreview(client, documentValue);
      setLastWrite(client, { revision: documentValue.revision, sequence: change.sequence });
    } catch {
      documentValue = null;
    }
    addEvent(client, {
      origin: change.origin,
      documentId: id,
      sequence: change.sequence,
      receivedAt: Date.now(),
    });
  }

  client.lastSequence = Math.max(client.lastSequence, result.last_sequence || 0);
}

async function pollClient(client) {
  while (client.running) {
    try {
      await readChanges(client);
      if (client.error) {
        client.error = null;
        setClientStatus(client, "connected", "Connected");
      }
    } catch (error) {
      client.error = error;
      setClientStatus(client, "error", "Retrying…");
      await sleep(750);
    }
  }
}

function showError(client, message) {
  const panel = panelFor(client);
  const status = panel?.querySelector('[data-role="status-text"]');
  if (status) status.textContent = message;
  setClientStatus(client, "error", message);
}

function renderNativeInfo(nativeClient) {
  if (!nativeClient) return;
  elements.nativeTitle.textContent = "Native client attached · replication output is in the launcher terminal";
  elements.nativeCopy.textContent =
    "Database C is owned by a separate Elixir process. It receives A's revisions over the remote wire and prints each change from ElixirDB.Changes.wait/2.";
  elements.nativeDetails.innerHTML = `
    <span>Database <strong>${escapeHtml(nativeClient.database_uuid)}</strong></span>
    <span>Endpoint <strong>${escapeHtml(nativeClient.endpoint)}</strong></span>
  `;
}

async function start() {
  try {
    const response = await fetch("/config.json", { cache: "no-store" });
    state.config = await response.json();
    for (const client of state.config.clients) {
      state.clients.set(client.key, {
        ...client,
        apiBase: `/api/${client.key}`,
        lastSequence: 0,
        documents: new Map(),
        running: true,
      });
    }
    renderClients();
    renderNativeInfo(state.config.native_client);
    await Promise.all([loadScenarios(), refreshLabState()]);

    const checks = await Promise.all([...state.clients.values()].map(checkConnection));
    if (checks.every(Boolean)) updateGlobalStatus("connected", "All connections live");
    else updateGlobalStatus("error", "One or more connections unavailable");

    for (const client of state.clients.values()) void pollClient(client);
    void refreshObservability();
    window.setInterval(() => void refreshLabState(), 2_000);
    window.setInterval(() => void refreshObservability(), 2_000);
  } catch (error) {
    updateGlobalStatus("error", "Harness configuration unavailable");
    elements.clientGrid.innerHTML = `<div class="client-panel"><p>${escapeHtml(error.message)}</p></div>`;
  }
}

void start();
