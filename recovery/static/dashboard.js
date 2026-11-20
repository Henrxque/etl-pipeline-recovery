"use strict";
const $ = (id) => document.getElementById(id);
const state = {
  selected: null,
  view: "overview",
  busy: false,
  snapshot: null,
  detail: null,
};
const labels = {
  failed: "Failed",
  failed_validation: "Validation failed",
  succeeded: "Completed",
  succeeded_with_review: "Validated · Review",
  running: "Running",
};
const actionLabels = {
  broken: "Reproduce failure",
  diagnose: "Diagnose input",
  recover: "Recover partition",
};
const checkLabels = {
  snapshot_row_count: "Spark row count matches the frozen source snapshot",
  source_row_conservation: "Every source row has one disposition",
  nonempty_accepted_partition: "The accepted partition is not empty",
  unique_business_keys: "Order / line business keys are unique",
  explicit_partition_scope: "Accepted rows match the selected partition",
  line_to_aggregate_reconciliation:
    "Spark line and aggregate amounts reconcile",
  all_rows_accounted: "The row ledger accounts for the full source",
  strict_zero_quarantine: "Strict gate: no quarantined rows",
  sqlite_target_reconciliation: "SQLite rows and aggregates match Spark output",
};
const money = (minor) =>
  new Intl.NumberFormat("en-GB", { style: "currency", currency: "BRL" }).format(
    minor / 100,
  );
const clock = (value) =>
  value
    ? new Intl.DateTimeFormat("en-GB", {
        day: "2-digit",
        month: "short",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        timeZone: "America/Sao_Paulo",
      }).format(new Date(value))
    : "—";
function node(tag, cls, text) {
  const el = document.createElement(tag);
  if (cls) el.className = cls;
  if (text !== undefined) el.textContent = text;
  return el;
}
function badge(value) {
  return node("span", `status ${value}`, labels[value] || value);
}
function message(id, text) {
  $(id).textContent = text;
  $(id).hidden = false;
}
async function api(path, options = {}) {
  const r = await fetch(path, options);
  const data = await r.json();
  if (!r.ok)
    throw new Error(
      data.error?.message || "The local server could not complete the request.",
    );
  return data;
}
function busy(value) {
  state.busy = value;
  for (const id of [
    "run-broken",
    "run-diagnose",
    "run-recover",
    "scenario",
    "strict",
  ])
    $(id).disabled = value;
}
function emptyRow(body, count, text) {
  const tr = node("tr");
  const td = node("td", "", text);
  td.colSpan = count;
  tr.append(td);
  body.append(tr);
}
function statStrip(entries) {
  const strip = node("div", "stat-strip");
  for (const [title, value] of entries) {
    const cell = node("span", "", title);
    cell.append(node("strong", "", value));
    strip.append(cell);
  }
  return strip;
}

async function refresh() {
  const snapshot = await api("/api/summary");
  state.snapshot = snapshot;
  const active = snapshot.runs.find((run) => run.state === "running");
  busy(Boolean(active));
  const published = snapshot.published.find(
    (p) => p.order_date === "2026-11-18",
  );
  $("metric-lines").textContent = published ? published.line_count : "—";
  $("metric-orders").textContent = published
    ? `${published.order_count} demo orders · 18 Nov 2026`
    : "No validated partition published";
  $("metric-value").textContent = published
    ? money(published.gross_minor)
    : "—";
  const latestRecovery = snapshot.runs.find(
    (run) => run.action === "recover" && run.report,
  );
  $("metric-review").textContent =
    latestRecovery?.report?.result?.stats?.rejected ?? "—";
  const latestPublication = snapshot.runs.find(
    (run) => run.report?.publication,
  );
  $("metric-action").textContent = latestPublication
    ? latestPublication.report.publication.action === "unchanged"
      ? "Unchanged on replay"
      : "Partition published"
    : "Not published";
  $("metric-checks").textContent = latestPublication
    ? `${latestPublication.report.result.checks.filter((c) => c.passed).length} checks passed · ${clock(latestPublication.finished_at)} São Paulo`
    : "Previous data survives a failed run";
  $("legacy-count").textContent = snapshot.legacy_rows
    ? `Isolated legacy rows: ${snapshot.legacy_rows}.`
    : "";
  const history = $("history-body");
  history.replaceChildren();
  if (!snapshot.runs.length)
    emptyRow(history, 5, "No runs yet. Start by reproducing the failure.");
  for (const run of snapshot.runs) {
    const tr = node("tr");
    const started = node("td", "", clock(run.started_at));
    started.append(node("small", "run-label", run.run_id.slice(0, 8)));
    tr.append(started);
    const action = node("td", "", actionLabels[run.action]);
    if (run.scenario)
      action.append(node("small", "", `Scenario: ${run.scenario}`));
    tr.append(action);
    const status = node("td");
    status.append(badge(run.state));
    tr.append(status);
    tr.append(
      node(
        "td",
        "",
        run.report?.publication
          ? run.report.publication.action === "unchanged"
            ? "Unchanged"
            : "Replaced partition"
          : "Curated data unchanged",
      ),
    );
    const view = node("td");
    const button = node("button", "text-button", "Inspect");
    button.addEventListener("click", () => selectRun(run.run_id));
    view.append(button);
    tr.append(view);
    history.append(tr);
  }
  const lines = $("lines-body");
  lines.replaceChildren();
  if (!snapshot.lines.length)
    emptyRow(lines, 6, "No curated records have been published.");
  for (const row of snapshot.lines) {
    const tr = node("tr");
    for (const value of [
      `${row.order_id} / ${row.line_id}`,
      row.customer_name,
      row.quantity,
      money(row.unit_price_minor),
      money(row.gross_minor),
      row.order_date,
    ])
      tr.append(node("td", "", value));
    lines.append(tr);
  }
  if (!state.selected && snapshot.runs.length)
    state.selected = snapshot.runs[0].run_id;
  if (state.selected) await loadDetail();
}
async function selectRun(id) {
  state.selected = id;
  state.view = "overview";
  await loadDetail();
}
async function loadDetail() {
  const id = state.selected;
  const run = await api(`/api/runs/${id}`);
  if (id !== state.selected) return;
  state.detail = run;
  renderDetail();
}
function renderDetail() {
  const run = state.detail;
  if (!run) return;
  $("detail-title").textContent =
    actionLabels[run.action] || "Execution evidence";
  $("run-status").className = `status ${run.state}`;
  $("run-status").textContent = labels[run.state] || run.state;
  $("detail-meta").textContent =
    `Run ${run.run_id.slice(0, 10)} · ${clock(run.started_at)} São Paulo · Partition ${run.partition_date}${run.scenario ? ` · ${run.scenario} scenario` : ""}${run.report?.duration_ms != null ? ` · ${(run.report.duration_ms / 1000).toFixed(1)}s` : ""}`;
  for (const view of ["overview", "outcomes", "logs"])
    $(`tab-${view}`).setAttribute("aria-selected", String(state.view === view));
  for (const id of ["download-report", "download-csv", "download-logs"])
    $(id).disabled = !run.report;
  const content = $("detail");
  content.replaceChildren();
  if (state.view === "logs") {
    renderLogs(content, run.events);
    return;
  }
  if (state.view === "outcomes") {
    renderOutcomes(content, run.report?.result?.outcomes);
    return;
  }
  if (!run.report) {
    const loading = node("div", "running-indicator");
    loading.append(
      node("span", "spinner"),
      node("span", "", "Executing the real PySpark workflow…"),
    );
    content.append(loading);
    renderLogs(content, run.events);
    return;
  }
  const report = run.report;
  if (report.error) {
    const box = node("div", "error-box");
    box.append(
      node("strong", "", report.error.code),
      node("pre", "", report.error.message),
    );
    content.append(box);
  }
  if (report.publication) {
    content.append(
      node(
        "h3",
        "",
        report.publication.action === "unchanged"
          ? "Replay verified. Published content is unchanged."
          : "Validated partition published.",
      ),
    );
    content.append(
      node(
        "p",
        "",
        `${report.publication.lines} order lines · ${report.publication.orders} demo orders · ${money(report.publication.gross_minor)} gross fixture value. Output and its audit report were committed together.`,
      ),
    );
  } else if (report.action === "recover")
    content.append(
      node(
        "p",
        "",
        "No curated partition was replaced. Any previously published data remains intact.",
      ),
    );
  if (report.result) {
    const s = report.result.stats;
    content.append(
      statStrip([
        ["Source rows", s.source_rows],
        ["Accepted", s.accepted],
        ["For review", s.rejected],
        ["Duplicates", s.duplicates],
        ["Superseded", s.superseded],
      ]),
    );
    for (const check of report.result.checks) {
      const row = node("div", `check-row ${check.passed ? "" : "fail"}`);
      row.append(
        node("span", "", check.passed ? "✓" : "×"),
        node("strong", "", checkLabels[check.name] || check.name),
      );
      content.append(row);
    }
  }
  if (report.legacy) {
    content.append(
      statStrip([
        ["Source rows", report.legacy.source_rows],
        ["Joined rows this run", report.legacy.joined_rows_this_run],
        ["Append target rows", report.legacy.append_target_rows],
      ]),
    );
    content.append(
      node(
        "p",
        "",
        "This is the isolated defective target. Repeating this scenario appends another copy. Curated output is untouched.",
      ),
    );
  }
  if (report.diagnosis && !report.result) {
    const d = report.diagnosis;
    content.append(
      statStrip([
        ["Schema aliases", d.schema_aliases.length],
        [
          "Historical customer keys",
          d.customer_history_keys_with_multiple_versions.length,
        ],
        ["Repeated line keys", d.repeated_business_keys.length],
        ["Unsafe direct casts", d.unsafe_direct_cast_records.length],
      ]),
    );
    for (const finding of d.findings) {
      const article = node("article", "finding");
      article.append(
        node("h3", "", finding.code.replaceAll("_", " ").toUpperCase()),
        node("p", "", finding.cause),
        node("p", "fix", finding.fix),
      );
      content.append(article);
    }
  }
  if (report.action === "broken" && !report.legacy)
    content.append(
      node(
        "p",
        "",
        "The failure above came from executing the defective Spark query. Inspect the logs, then diagnose the input to identify the correction.",
      ),
    );
}
function renderLogs(content, events = []) {
  for (const event of events) {
    const block = node("article", "event");
    block.append(
      node(
        "small",
        "",
        `${event.level} / ${event.stage} · ${clock(event.occurred_at)}`,
      ),
      node("h3", "", event.message),
    );
    if (Object.keys(event.context || {}).length)
      block.append(node("pre", "", JSON.stringify(event.context, null, 2)));
    content.append(block);
  }
}
function renderOutcomes(content, outcomes) {
  if (!outcomes) {
    content.append(
      node(
        "p",
        "",
        "Row dispositions are available after the recovery transform produces its validation ledger.",
      ),
    );
    return;
  }
  const table = node("table");
  const head = node("thead");
  const titles = node("tr");
  for (const label of ["Source row", "Order / line", "Disposition", "Reasons"])
    titles.append(node("th", "", label));
  head.append(titles);
  table.append(head);
  const body = node("tbody");
  for (const row of outcomes) {
    const tr = node("tr");
    for (const value of [
      row.source_record,
      `${row.order_id} / ${row.line_id}`,
      row.disposition,
      row.reasons.join(", ") || "Passed configured checks",
    ])
      tr.append(node("td", "", value));
    body.append(tr);
  }
  table.append(body);
  content.append(table);
}
async function start(action) {
  if (state.busy) return;
  busy(true);
  $("error").hidden = true;
  $("notice").hidden = true;
  try {
    const run = await api("/api/jobs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        action,
        scenario: $("scenario").value,
        partition: "2026-11-18",
        strict: $("strict").checked,
      }),
    });
    state.selected = run.run_id;
    state.view = "overview";
    message(
      "notice",
      "Run dispatched. This page polls persisted execution events; the first Spark startup may take longer.",
    );
    await refresh();
  } catch (error) {
    message("error", error.message);
    busy(false);
  }
}
for (const action of ["broken", "diagnose", "recover"])
  $(`run-${action}`).addEventListener("click", () => start(action));
for (const view of ["overview", "outcomes", "logs"])
  $(`tab-${view}`).addEventListener("click", () => {
    state.view = view;
    renderDetail();
  });
$("refresh").addEventListener("click", () =>
  refresh().catch((error) => message("error", error.message)),
);
for (const kind of ["report", "csv", "logs"])
  $(`download-${kind}`).addEventListener("click", () => {
    if (state.selected) {
      const link = node("a");
      link.href = `/api/runs/${state.selected}/download/${kind}`;
      link.download = kind === "csv" ? "row-outcomes.csv" : `${kind}.json`;
      document.body.append(link);
      link.click();
      link.remove();
    }
  });
let polling = false;
async function poll() {
  if (polling) return;
  polling = true;
  try {
    const wasBusy = state.busy;
    await refresh();
    if (wasBusy && !state.busy && state.detail) {
      const run = state.detail;
      message(
        "notice",
        run.state.startsWith("succeeded")
          ? "Execution completed. Inspect the saved checks, row outcomes and report below."
          : "Execution finished with a visible failure. Curated data was preserved; inspect the evidence below.",
      );
    }
  } catch (error) {
    message("error", error.message);
  } finally {
    polling = false;
  }
}
poll();
setInterval(poll, 1800);
