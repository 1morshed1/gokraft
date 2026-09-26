// gokraft dashboard logic. Vanilla JS, no framework. Each function maps to a
// query API endpoint; the shapes it expects mirror internal/query/handlers.go
// and internal/alert/handlers.go. Read the two side by side to see the full
// client/server contract.
"use strict";

const $ = (sel, root = document) => root.querySelector(sel);
const api = (path) => fetch(`/api/v1${path}`).then((r) => r.json());
const fmtTs = (t) => new Date(t).toLocaleTimeString([], { hour12: false }) + "." +
  String(new Date(t).getMilliseconds()).padStart(3, "0");
const esc = (s) => String(s ?? "").replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c]));

// --- tab switching ---
document.querySelectorAll("#tabs button").forEach((btn) => {
  btn.addEventListener("click", () => {
    document.querySelectorAll("#tabs button").forEach((b) => b.classList.remove("active"));
    document.querySelectorAll(".panel").forEach((p) => p.classList.remove("active"));
    btn.classList.add("active");
    $("#" + btn.dataset.tab).classList.add("active");
    if (btn.dataset.tab === "alerts") loadAlerts();
  });
});

// --- health indicator ---
async function pollHealth() {
  const el = $("#health");
  try {
    const r = await fetch("/healthz");
    el.className = "health " + (r.ok ? "ok" : "down");
  } catch { el.className = "health down"; }
}
setInterval(pollHealth, 5000);
pollHealth();

// --- LOGS ---
$("#log-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const f = new FormData(e.target);
  const qs = new URLSearchParams();
  for (const [k, v] of f.entries()) if (v) qs.set(k, v);
  const box = $("#log-results");
  box.innerHTML = '<div class="empty">searching…</div>';
  try {
    const { logs } = await api("/logs/search?" + qs);
    renderLogs(box, logs);
  } catch (err) { box.innerHTML = `<div class="empty">error: ${esc(err)}</div>`; }
});

function renderLogs(box, logs) {
  if (!logs || !logs.length) { box.innerHTML = '<div class="empty">no logs</div>'; return; }
  box.innerHTML = logs.map((l) => `
    <div class="row">
      <span class="ts">${esc(fmtTs(l.timestamp))}</span>
      <span class="sev-${esc(l.severity)}">${esc(l.severity)}</span>
      <span>${esc(l.service_name)}</span>
      <span class="body">${esc(l.body)}${l.trace_id ?
        ` <span class="trace-link" data-trace="${esc(l.trace_id)}">[trace]</span>` : ""}</span>
    </div>`).join("");
  box.querySelectorAll(".trace-link").forEach((el) =>
    el.addEventListener("click", () => openTrace(el.dataset.trace)));
}

// --- METRICS ---
let metricChart;
$("#metric-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const f = new FormData(e.target);
  const qs = new URLSearchParams();
  for (const [k, v] of f.entries()) if (v) qs.set(k, v);
  try {
    const data = await api("/metrics/query?" + qs);
    const points = data.points || [];
    const labels = points.map((p) => fmtTs(p.bucket));
    const values = points.map((p) => p.value);
    const ctx = $("#metric-chart");
    if (metricChart) metricChart.destroy();
    metricChart = new Chart(ctx, {
      type: "line",
      data: { labels, datasets: [{ label: `${data.aggregation}(${data.metric})`, data: values,
        borderColor: "#58a6ff", backgroundColor: "#58a6ff33", fill: true, tension: 0.25, pointRadius: 0 }] },
      options: { responsive: true, scales: { x: { ticks: { color: "#8b949e", maxTicksLimit: 8 } },
        y: { ticks: { color: "#8b949e" } } }, plugins: { legend: { labels: { color: "#e6edf3" } } } },
    });
  } catch (err) { alert("metric query failed: " + err); }
});

// --- TRACES ---
$("#trace-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const f = new FormData(e.target);
  const qs = new URLSearchParams();
  if (f.get("service")) qs.set("service", f.get("service"));
  if (f.get("only_errors")) qs.set("only_errors", "true");
  const list = $("#trace-list");
  list.innerHTML = '<div class="empty">searching…</div>';
  const { traces } = await api("/traces/search?" + qs);
  if (!traces || !traces.length) { list.innerHTML = '<div class="empty">no traces</div>'; return; }
  list.innerHTML = traces.map((t) => `
    <div class="trace-item ${t.has_error ? "err" : ""}" data-trace="${esc(t.trace_id)}">
      <strong>${esc(t.root_service)}</strong> ${esc(t.root_operation)}<br>
      <span class="ts">${(t.duration_us / 1000).toFixed(1)} ms · ${t.span_count} spans · ${esc(fmtTs(t.start_time))}</span>
    </div>`).join("");
  list.querySelectorAll(".trace-item").forEach((el) =>
    el.addEventListener("click", () => openTrace(el.dataset.trace)));
});

// openTrace fetches the correlated view (trace + its logs) and renders a
// waterfall plus the correlated logs beneath it.
async function openTrace(traceID) {
  // jump to traces tab
  document.querySelector('#tabs button[data-tab="traces"]').click();
  const detail = $("#trace-detail");
  detail.innerHTML = '<div class="empty">loading…</div>';
  const { trace, logs } = await api("/correlate/" + encodeURIComponent(traceID));
  const total = trace.duration_us || 1;
  const flat = [];
  const walk = (n) => { flat.push(n); (n.children || []).forEach(walk); };
  (trace.roots || []).forEach(walk);
  const t0 = new Date(trace.start_time).getTime();
  const bars = flat.map((s) => {
    const offset = (new Date(s.start_time).getTime() - t0) * 1000; // us
    const left = (offset / total) * 100;
    const width = Math.max((s.duration_us / total) * 100, 0.5);
    const err = s.status_code === 2 ? "err" : "";
    return `<div class="span-bar" style="margin-left:${s.depth * 14}px">
      <div class="span-fill ${err}" style="left:${left}%;width:${width}%"></div>
      <span class="span-label">${esc(s.service_name)}:${esc(s.operation_name)} (${(s.duration_us/1000).toFixed(1)}ms)</span>
    </div>`;
  }).join("");
  const logRows = (logs || []).map((l) =>
    `<div class="row" style="grid-template-columns:150px 70px 1fr">
      <span class="ts">${esc(fmtTs(l.timestamp))}</span>
      <span class="sev-${esc(l.severity)}">${esc(l.severity)}</span>
      <span class="body">${esc(l.body)}</span></div>`).join("");
  detail.innerHTML = `<h3>Trace ${esc(traceID).slice(0, 16)}… · ${(total/1000).toFixed(1)} ms</h3>
    ${bars}<h3>Correlated logs (${(logs||[]).length})</h3>${logRows || '<div class="empty">none</div>'}`;
}

// --- LIVE TAIL (WebSocket) ---
let tailSocket = null;
$("#tail-form").addEventListener("submit", (e) => {
  e.preventDefault();
  const toggle = $("#tail-toggle");
  const box = $("#tail-results");
  if (tailSocket) { tailSocket.close(); tailSocket = null; toggle.textContent = "Start tail"; return; }
  const f = new FormData(e.target);
  const qs = new URLSearchParams();
  for (const [k, v] of f.entries()) if (v) qs.set(k, v);
  const proto = location.protocol === "https:" ? "wss" : "ws";
  tailSocket = new WebSocket(`${proto}://${location.host}/api/v1/logs/tail?${qs}`);
  box.innerHTML = '<div class="empty">tailing… new logs will appear below</div>';
  toggle.textContent = "Stop tail";
  tailSocket.onmessage = (ev) => {
    const l = JSON.parse(ev.data);
    if (box.querySelector(".empty")) box.innerHTML = "";
    const row = document.createElement("div");
    row.className = "row";
    row.innerHTML = `<span class="ts">${esc(fmtTs(l.timestamp))}</span>
      <span class="sev-${esc(l.severity)}">${esc(l.severity)}</span>
      <span>${esc(l.service_name)}</span><span class="body">${esc(l.body)}</span>`;
    box.prepend(row);
    while (box.children.length > 500) box.removeChild(box.lastChild);
  };
  tailSocket.onclose = () => { toggle.textContent = "Start tail"; tailSocket = null; };
});

// --- ALERTS ---
async function loadAlerts() {
  const box = $("#alert-list");
  const { rules } = await api("/alerts/rules");
  if (!rules || !rules.length) { box.innerHTML = '<div class="empty">no alert rules</div>'; return; }
  box.innerHTML = rules.map((r) => `
    <div class="alert-card">
      <span class="badge ${esc(r.state)}">${esc(r.state)}</span>
      <div><strong>${esc(r.name)}</strong><br>
        <span class="ts">${esc(r.aggregation)}(${esc(r.metric)}${r.service ? "{" + esc(r.service) + "}" : ""})
        ${esc(r.comparator)} ${r.threshold} for ${Math.round((r.for || 0) / 1e9)}s</span></div>
    </div>`).join("");
}
