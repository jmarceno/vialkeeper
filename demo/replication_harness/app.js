const state = {
  config: null,
  clients: new Map(),
  eventCount: 0,
};

const elements = {
  clientGrid: document.querySelector("#client-grid"),
  globalStatus: document.querySelector("#global-status"),
  globalStatusText: document.querySelector("#global-status-text"),
  eventCount: document.querySelector("#event-count"),
  nativeTitle: document.querySelector("#native-title"),
  nativeCopy: document.querySelector("#native-copy"),
  nativeDetails: document.querySelector("#native-details"),
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

    const checks = await Promise.all([...state.clients.values()].map(checkConnection));
    if (checks.every(Boolean)) updateGlobalStatus("connected", "All connections live");
    else updateGlobalStatus("error", "One or more connections unavailable");

    for (const client of state.clients.values()) void pollClient(client);
  } catch (error) {
    updateGlobalStatus("error", "Harness configuration unavailable");
    elements.clientGrid.innerHTML = `<div class="client-panel"><p>${escapeHtml(error.message)}</p></div>`;
  }
}

void start();
