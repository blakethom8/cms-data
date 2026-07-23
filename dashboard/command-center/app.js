(function () {
  "use strict";

  const API = "/api";

  const evidenceProviders = [
    { npi: "1710390513", name: "Lauren DeStefano", specialty: "Surgical Oncology" },
    { npi: "1962509216", name: "Robert Vescio", specialty: "Hematology / Oncology" },
    { npi: "1740218155", name: "Joshua “Josh” Scott", specialty: "Pediatric Medicine" },
    { npi: "1659383891", name: "Jonathan Weiner", specialty: "Internal Medicine" }
  ];

  const previewCatalog = [
    { key: "dac_national", table: "raw_dac_national", title: "Doctors & Clinicians (DAC)", domain: "identity", grain: "one row per clinician × practice address", description: "CMS clinician directory connecting people, places, and organizations.", join_keys: ["NPI", "org_pac_id", "address"], row_count: 2269147, column_count: 37 },
    { key: "nppes", table: "raw_nppes", title: "NPPES (NPI Registry)", domain: "identity", grain: "one row per Type 1 individual NPI in the loaded subset", description: "The loaded individual-provider subset of NPPES with taxonomy, credentials, and registered practice or mailing addresses; Type 2 organizations are not yet loaded here.", join_keys: ["NPI"], row_count: 7144239, column_count: 34 },
    { key: "pecos_enrollment", table: "raw_pecos_enrollment", title: "PECOS Public Provider Enrollment", domain: "identity", grain: "one row per Medicare enrollment record", description: "CMS's quarterly public enrollment snapshot for Medicare fee-for-service providers and organizations. An enrollment does not establish employment, billing reassignment, or a primary practice site.", join_keys: ["NPI", "ENRLMT_ID"], row_count: 2981799, column_count: 15 },
    { key: "physician_by_provider", table: "raw_physician_by_provider", title: "Medicare Utilization (per provider)", domain: "money", grain: "one row per NPI per year", description: "Annual Medicare volume per clinician, including services, beneficiaries, payments, demographics, and chronic conditions.", join_keys: ["Rndrng_NPI"], row_count: 1253587, column_count: 96 },
    { key: "physician_by_service", table: "raw_physician_by_provider_and_service", title: "Procedures (per provider × HCPCS)", domain: "money", grain: "one row per NPI × procedure code × place of service", description: "Procedure-level billing detail by clinician, code, volume, payment, and site of service.", join_keys: ["Rndrng_NPI", "HCPCS_Cd"], row_count: 9842784, column_count: 29 },
    { key: "part_d_by_drug", table: "raw_part_d_by_provider_and_drug", title: "Part D Prescribing (per provider × drug)", domain: "rx", grain: "one row per prescriber × drug", description: "Drug-level prescribing with claim counts, day supply, and total drug cost per prescriber.", join_keys: ["Prscrbr_NPI", "Brnd_Name/Gnrc_Name"], row_count: 26180429, column_count: 31 },
    { key: "open_payments_general", table: "raw_open_payments_general", title: "Open Payments — General", domain: "industry", grain: "one row per payment (manufacturer → clinician)", description: "Industry transfers to clinicians, including manufacturer, amount, nature, and associated product.", join_keys: ["Covered_Recipient_NPI"], row_count: 14700000, column_count: 91 },
    { key: "open_payments_research", table: "raw_open_payments_research", title: "Open Payments — Research", domain: "industry", grain: "one row per research payment", description: "Industry research funding with sponsor, study context, and principal investigators.", join_keys: ["Covered_Recipient_NPI"], row_count: 983412, column_count: 119 },
    { key: "open_payments_ownership", table: "raw_open_payments_ownership", title: "Open Payments — Ownership", domain: "industry", grain: "one row per physician ownership/investment interest", description: "Physician ownership stakes in manufacturers and group purchasing organizations.", join_keys: ["Physician_NPI"], row_count: 4818, column_count: 42 },
    { key: "reassignment", table: "raw_reassignment", title: "Reassignment (clinician → group)", domain: "org", grain: "one row per clinician × group reassignment record", description: "Medicare benefit-reassignment relationships between individual clinicians and groups; ordinary reassignment records do not establish employment.", join_keys: ["Individual NPI", "Group PAC ID"], row_count: 2166048, column_count: 16 },
    { key: "mips_performance", table: "raw_mips_performance", title: "MIPS Quality Scores", domain: "quality", grain: "one row per NPI per program year", description: "Performance category scores and final MIPS score for participating clinicians.", join_keys: ["NPI", "Org_PAC_ID"], row_count: 1112032, column_count: 104 },
    { key: "dme_referring", table: "raw_dme_by_referring_provider", title: "DME Referrals", domain: "money", grain: "one row per referring provider", description: "Durable medical equipment ordering volume, suppliers, claims, and Medicare payment.", join_keys: ["Rfrg_NPI"], row_count: 391830, column_count: 22 },
    { key: "address_geocode", table: "address_geocode", title: "Address Geocodes", domain: "geo", grain: "one row per distinct practice address", description: "Derived practice-address geocodes used for proximity search and mapping.", join_keys: ["addr_key = street|zip5"], row_count: 233284, column_count: 11 }
  ];

  const previewColumns = {
    dac_national: [["NPI", "VARCHAR"], ["Provider Last Name", "VARCHAR"], ["Provider First Name", "VARCHAR"], ["pri_spec", "VARCHAR"], ["org_pac_id", "VARCHAR"], ["Facility Name", "VARCHAR"], ["adr_ln_1", "VARCHAR"], ["City/Town", "VARCHAR"], ["State", "VARCHAR"], ["ZIP Code", "VARCHAR"]],
    nppes: [["npi", "VARCHAR"], ["entity_type", "VARCHAR"], ["last_name", "VARCHAR"], ["first_name", "VARCHAR"], ["credentials", "VARCHAR"], ["taxonomy_1", "VARCHAR"], ["practice_address_1", "VARCHAR"], ["practice_city", "VARCHAR"], ["practice_state", "VARCHAR"]]
  };

  const state = {
    core: {},
    operations: {},
    catalog: [],
    tables: [],
    sources: [],
    runs: [],
    lineage: null,
    lineageFilter: "all",
    selectedLineageNode: null,
    overview: null,
    offlinePreview: false,
    selectedDataset: null,
    selectedTab: "overview",
    columnsCache: new Map(),
    sampleCache: new Map(),
    sampleLimits: new Map(),
    sampleInspector: null,
    reopenSampleInspector: false,
    providerEvidence: {
      result: null,
      data: null,
      focusedNpi: evidenceProviders[0].npi,
      selectedSourceKey: null,
      selectedRow: 0
    }
  };

  const $ = (selector, root = document) => root.querySelector(selector);
  const $$ = (selector, root = document) => Array.from(root.querySelectorAll(selector));

  function escapeHtml(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }

  function pick(object, keys, fallback = null) {
    for (const key of keys) {
      if (object && object[key] !== undefined && object[key] !== null && object[key] !== "") return object[key];
    }
    return fallback;
  }

  function arrayFrom(payload, keys) {
    if (Array.isArray(payload)) return payload;
    for (const key of keys) if (Array.isArray(payload?.[key])) return payload[key];
    return [];
  }

  async function getJson(path) {
    const response = await fetch(`${API}${path}`, { headers: { Accept: "application/json" }, credentials: "same-origin" });
    if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
    return response.json();
  }

  function compactNumber(value) {
    const number = Number(value);
    if (!Number.isFinite(number)) return "—";
    if (Math.abs(number) < 1000) return new Intl.NumberFormat("en-US").format(number);
    return new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: number >= 1e9 ? 2 : 1 }).format(number);
  }

  function fullNumber(value) {
    const number = Number(value);
    return Number.isFinite(number) ? new Intl.NumberFormat("en-US").format(number) : "—";
  }

  function formatDate(value) {
    if (!value) return "Not observed";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value);
    return new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", year: "numeric", hour: "numeric", minute: "2-digit" }).format(date);
  }

  function formatCell(value) {
    if (value === null || value === undefined || value === "") return '<span class="cell-null">null</span>';
    if (typeof value === "object") return escapeHtml(JSON.stringify(value));
    return escapeHtml(value);
  }

  function evidenceStatus(source) {
    const raw = String(pick(source, ["evidence_status", "status", "validation_state"], "unavailable")).toLowerCase();
    if (["validated_active", "active", "valid", "validated", "current", "promoted", "success", "succeeded", "passed"].some(token => raw.includes(token))) return "current";
    if (["fail", "error", "invalid"].some(token => raw.includes(token))) return "failed";
    if (["unverified", "stale", "attention", "warn", "pending"].some(token => raw.includes(token))) return "warning";
    return "unavailable";
  }

  function statusChip(status, label) {
    return `<span class="status-chip ${status}"><i class="status-dot ${status}"></i>${escapeHtml(label || status)}</span>`;
  }

  function manifestOf(source) {
    return source?.latest_manifest || source?.manifest || null;
  }

  function rowCountFrom(source) {
    const manifest = manifestOf(source);
    const counts = manifest?.row_counts;
    if (counts && typeof counts === "object") return Object.values(counts).reduce((sum, value) => sum + (Number(value) || 0), 0);
    return pick(source, ["row_count", "rows"], null);
  }

  function showToast(message) {
    const toast = $("#toast");
    toast.textContent = message;
    toast.hidden = false;
    clearTimeout(showToast.timer);
    showToast.timer = setTimeout(() => { toast.hidden = true; }, 3800);
  }

  async function initialize() {
    const requests = {
      health: getJson("/health"),
      tables: getJson("/tables"),
      catalog: getJson("/explorer/catalog"),
      overview: getJson("/operations/overview"),
      sources: getJson("/operations/sources"),
      runs: getJson("/operations/runs?limit=50"),
      lineage: getJson("/operations/lineage"),
      providerEvidence: getJson(`/explorer/provider-evidence?npis=${evidenceProviders.map(provider => provider.npi).join(",")}&limit=10`)
    };

    const entries = Object.entries(requests);
    const results = await Promise.allSettled(entries.map(([, promise]) => promise));

    results.forEach((result, index) => {
      const name = entries[index][0];
      if (name === "providerEvidence") {
        state.providerEvidence.result = result.status === "fulfilled"
          ? { ok: true, data: result.value }
          : { ok: false, error: result.reason };
        return;
      }
      const target = ["health", "tables", "catalog"].includes(name) ? state.core : state.operations;
      target[name] = result.status === "fulfilled" ? { ok: true, data: result.value } : { ok: false, error: result.reason };
    });

    const coreSuccesses = Object.values(state.core).filter(item => item.ok).length;
    state.offlinePreview = coreSuccesses === 0;
    state.catalog = state.core.catalog?.ok ? arrayFrom(state.core.catalog.data, ["catalog", "datasets", "items"]) : (state.offlinePreview ? previewCatalog : []);
    state.tables = state.core.tables?.ok ? arrayFrom(state.core.tables.data, ["tables", "items"]) : [];
    state.overview = state.operations.overview?.ok ? state.operations.overview.data : null;
    state.sources = state.operations.sources?.ok ? arrayFrom(state.operations.sources.data, ["sources", "items", "data"]) : [];
    state.runs = state.operations.runs?.ok ? arrayFrom(state.operations.runs.data, ["runs", "items", "data"]) : [];
    state.lineage = state.operations.lineage?.ok ? state.operations.lineage.data : null;

    renderConnectionState();
    renderOverview();
    renderCatalog();
    renderLineage();
    renderContracts();
    renderOperations();
    renderProviderEvidence();
    routeFromHash();
  }

  function renderConnectionState() {
    const indicator = $("#connection-state");
    const coreSuccesses = Object.values(state.core).filter(item => item.ok).length;
    const operationSuccesses = Object.values(state.operations).filter(item => item.ok).length;
    indicator.className = "connection-state";

    if (state.offlinePreview) {
      indicator.classList.add("state-offline");
      indicator.querySelector("span:last-child").textContent = "Offline preview data";
      $("#observed-time").textContent = "API not reachable";
      return;
    }

    if (coreSuccesses === Object.keys(state.core).length && operationSuccesses === Object.keys(state.operations).length) {
      indicator.classList.add("state-connected");
      indicator.querySelector("span:last-child").textContent = "API connected";
    } else {
      indicator.classList.add("state-partial");
      indicator.querySelector("span:last-child").textContent = "Evidence partial";
    }

    const observedAt = pick(state.overview, ["generated_at"], new Date().toISOString());
    $("#observed-time").textContent = `Observed ${formatDate(observedAt)}`;
  }

  function renderOverview() {
    const warehouse = state.overview?.warehouse || {};
    const contracts = state.overview?.contracts || {};
    const tableCount = pick(warehouse, ["table_count"], state.tables.length || null);
    const estimatedRows = pick(warehouse, ["estimated_rows"], state.tables.length ? state.tables.reduce((sum, table) => sum + (Number(pick(table, ["approx_rows", "estimated_size", "rows"], 0)) || 0), 0) : null);

    $("#metric-tables").textContent = tableCount === null ? "—" : fullNumber(tableCount);
    $("#metric-rows").textContent = estimatedRows === null ? "—" : compactNumber(estimatedRows);
    $("#metric-marts").textContent = state.catalog.length ? fullNumber(state.catalog.length) : "—";
    $("#metric-tables-note").textContent = state.offlinePreview ? "Unavailable in offline preview" : (state.core.tables?.ok ? "Live warehouse inventory" : "Inventory endpoint unavailable");

    const contractCount = pick(contracts, ["registered_sources"], state.sources.length || null);
    $("#metric-contracts").textContent = contractCount === null ? "—" : fullNumber(contractCount);
    $("#metric-contracts-note").textContent = state.operations.sources?.ok ? "Registered source contracts" : "Evidence not yet observed";

    const activeSources = Number(pick(contracts, ["sources_with_active_evidence"], NaN));
    const registeredSources = Number(contractCount);
    const coverage = Number.isFinite(activeSources) && Number.isFinite(registeredSources) && registeredSources > 0 ? Math.round(activeSources / registeredSources * 100) : null;
    $("#metric-manifest").textContent = coverage === null ? "—" : `${coverage}%`;
    $("#metric-manifest-note").textContent = coverage === null ? "Operations evidence required" : `${activeSources} of ${registeredSources} actively proven`;

    renderSourceHealth();
    renderRecentRuns();
    renderFlightpath();
  }

  function renderSourceHealth() {
    const container = $("#source-health");
    container.classList.remove("loading-block");
    if (!state.operations.sources?.ok) {
      container.innerHTML = '<div class="unavailable-state"><strong>Source evidence is not available</strong><p>The catalog remains usable, but source freshness and validation require <code>/operations/sources</code>.</p></div>';
      return;
    }
    if (!state.sources.length) {
      container.innerHTML = '<div class="empty-state"><strong>No source contracts returned</strong><p>The endpoint responded successfully without registered sources.</p></div>';
      return;
    }
    container.innerHTML = state.sources.slice(0, 4).map(source => {
      const status = evidenceStatus(source);
      const title = pick(source, ["title", "source_id", "name"], "Unnamed source");
      const manifest = manifestOf(source);
      const period = pick(manifest, ["source_data_period", "source_period"], "Period not observed");
      return `<div class="status-item"><i class="status-dot ${status}"></i><div><strong>${escapeHtml(title)}</strong><small>${escapeHtml(period)}</small></div><span class="status-label">${escapeHtml(status === "current" ? "validated" : status)}</span></div>`;
    }).join("");
  }

  function renderRecentRuns() {
    const container = $("#recent-runs");
    container.classList.remove("loading-block");
    if (!state.operations.runs?.ok) {
      container.innerHTML = '<div class="unavailable-state"><strong>Run ledger is not available</strong><p>No refresh history is inferred. Deploy <code>/operations/runs</code> to populate this panel.</p></div>';
      return;
    }
    if (!state.runs.length) {
      container.innerHTML = '<div class="empty-state"><strong>No runs have been recorded</strong><p>The run ledger is connected, but no manifest history exists yet.</p></div>';
      return;
    }
    container.innerHTML = state.runs.slice(0, 4).map(run => {
      const status = evidenceStatus({ status: `${pick(run, ["validation_state"], "")} ${pick(run, ["promotion_state"], "")}` });
      const source = pick(run, ["source_id", "source", "title"], "Unknown source");
      const when = pick(run, ["promotion_timestamp", "validation_timestamp", "retrieval_timestamp", "discovery_timestamp", "started_at"], null);
      return `<div class="run-item"><div><strong>${escapeHtml(source)}</strong><small>${escapeHtml(formatDate(when))}</small></div>${statusChip(status, pick(run, ["promotion_state", "validation_state"], status))}</div>`;
    }).join("");
  }

  function renderFlightpath() {
    const hasHealth = Boolean(state.core.health?.ok);
    const hasTables = Boolean(state.core.tables?.ok);
    const hasCatalog = Boolean(state.core.catalog?.ok);
    const hasSources = Boolean(state.operations.sources?.ok);
    const hasRuns = Boolean(state.operations.runs?.ok);
    const statuses = [hasSources, hasRuns, hasSources, hasTables, hasSources, hasCatalog, hasHealth];
    $$("#flightpath button").forEach((button, index) => {
      const marker = button.querySelector("span");
      marker.classList.toggle("observed", statuses[index]);
      button.title = statuses[index] ? "Evidence observed — open related workspace" : "Evidence unavailable — open related workspace";
    });
    const observed = statuses.filter(Boolean).length;
    $("#flightpath-caption").querySelector("p").textContent = state.offlinePreview
      ? "API evidence is unavailable. Catalog content is clearly marked preview data; no operational success is simulated."
      : `${observed} of 7 flightpath stages have directly observable evidence in the current API surface.`;
  }

  function renderCatalog() {
    const domainSelect = $("#catalog-domain");
    const domains = [...new Set(state.catalog.map(item => item.domain).filter(Boolean))].sort();
    domainSelect.innerHTML = '<option value="all">All domains</option>' + domains.map(domain => `<option value="${escapeHtml(domain)}">${escapeHtml(domain)}</option>`).join("");
    filterCatalog();
    if (state.catalog.length && !state.selectedDataset) selectDataset(state.catalog[0].key, false);
  }

  function filterCatalog() {
    const query = $("#catalog-search").value.trim().toLowerCase();
    const domain = $("#catalog-domain").value;
    const filtered = state.catalog.filter(item => {
      const searchable = [item.title, item.table, item.grain, item.description, ...(item.join_keys || [])].join(" ").toLowerCase();
      return (!query || searchable.includes(query)) && (domain === "all" || item.domain === domain);
    });
    $("#catalog-count").textContent = filtered.length;
    const list = $("#dataset-list");
    list.classList.remove("loading-block");
    if (!state.core.catalog?.ok && !state.offlinePreview) {
      list.innerHTML = '<div class="unavailable-state"><strong>Catalog endpoint unavailable</strong><p>The API is reachable, so preview values are not substituted.</p></div>';
      return;
    }
    if (!filtered.length) {
      list.innerHTML = '<div class="empty-state"><strong>No matching datasets</strong><p>Try a different domain or search phrase.</p></div>';
      return;
    }
    list.innerHTML = filtered.map(item => `
      <button class="dataset-button ${state.selectedDataset?.key === item.key ? "selected" : ""}" type="button" data-dataset-key="${escapeHtml(item.key)}">
        <span><strong><i class="domain-mark"></i>${escapeHtml(item.title)}</strong><small>${escapeHtml(item.table)} · ${escapeHtml(item.domain || "unclassified")}</small></span>
        <span class="row-count">${compactNumber(item.row_count)}</span>
      </button>`).join("");
  }

  function selectDataset(key, updateHash = true) {
    const dataset = state.catalog.find(item => item.key === key);
    if (!dataset) return;
    if (state.selectedDataset?.key !== key) {
      closeSampleInspector({ restoreFocus: false });
      state.reopenSampleInspector = false;
    }
    state.selectedDataset = dataset;
    state.selectedTab = "overview";
    filterCatalog();
    renderDatasetDetail();
    if (updateHash && location.hash !== `#catalog/${key}`) history.replaceState(null, "", `#catalog/${key}`);
  }

  function renderDatasetDetail() {
    const item = state.selectedDataset;
    if (!item) return;
    $("#dataset-detail").innerHTML = `
      <div class="detail-head">
        <span class="eyebrow">${state.offlinePreview ? "Preview dataset · not live" : `${escapeHtml(item.domain || "dataset")} domain`}</span>
        <h2>${escapeHtml(item.title)}</h2>
        <span class="physical-table">${escapeHtml(item.table)}</span>
        <div class="grain-band"><span>STATED GRAIN</span><strong>${escapeHtml(item.grain || "Grain not documented")}</strong></div>
      </div>
      <div class="detail-tabs" role="tablist" aria-label="Dataset inspection views">
        <button class="active" type="button" role="tab" aria-selected="true" data-detail-tab="overview">Overview</button>
        <button type="button" role="tab" aria-selected="false" data-detail-tab="columns">Columns <span>${item.column_count ? `(${fullNumber(item.column_count)})` : ""}</span></button>
        <button type="button" role="tab" aria-selected="false" data-detail-tab="sample-curated">Curated sample</button>
        <button type="button" role="tab" aria-selected="false" data-detail-tab="sample-all">All-column sample</button>
        <button type="button" role="tab" aria-selected="false" data-detail-tab="lineage">Source context</button>
      </div>
      <div class="detail-body" id="detail-body"></div>`;
    renderDetailTab("overview");
  }

  async function renderDetailTab(tab) {
    if (tab !== "sample-all") {
      closeSampleInspector({ restoreFocus: false });
      state.reopenSampleInspector = false;
    }
    state.selectedTab = tab;
    $$(".detail-tabs button").forEach(button => {
      const active = button.dataset.detailTab === tab;
      button.classList.toggle("active", active);
      button.setAttribute("aria-selected", String(active));
    });
    const body = $("#detail-body");
    const item = state.selectedDataset;
    if (!body || !item) return;

    if (tab === "overview") {
      body.innerHTML = `<p class="detail-description">${escapeHtml(item.description || "No dataset description has been published.")}</p>
        <div class="fact-grid">
          <div><span>Observed rows</span><strong>${item.row_count !== undefined ? fullNumber(item.row_count) : "Not observed"}</strong></div>
          <div><span>Columns</span><strong>${item.column_count !== undefined ? fullNumber(item.column_count) : "Not observed"}</strong></div>
          <div><span>Join keys</span><strong>${escapeHtml((item.join_keys || []).join(" · ") || "Not documented")}</strong></div>
        </div>`;
      return;
    }

    if (tab === "columns") {
      body.innerHTML = '<div class="empty-state"><strong>Loading live schema…</strong><p>Reading column names and physical types from the warehouse.</p></div>';
      let columns = state.columnsCache.get(item.key);
      if (!columns) {
        if (state.offlinePreview) {
          columns = (previewColumns[item.key] || []).map(([name, type]) => ({ name, type }));
        } else {
          try {
            const payload = await getJson(`/explorer/columns/${encodeURIComponent(item.key)}`);
            columns = arrayFrom(payload, ["columns", "items"]);
          } catch (error) {
            if (state.selectedTab === "columns") body.innerHTML = '<div class="unavailable-state"><strong>Column schema unavailable</strong><p>The live endpoint did not return schema evidence for this dataset.</p></div>';
            return;
          }
        }
        state.columnsCache.set(item.key, columns);
      }
      if (state.selectedTab !== "columns") return;
      body.innerHTML = `<div class="column-toolbar"><input id="column-search" type="search" placeholder="Filter column names or types…" aria-label="Filter columns"><span>${columns.length} columns observed</span></div><div class="column-grid" id="column-grid"></div>`;
      renderColumnGrid(columns, "");
      $("#column-search").addEventListener("input", event => renderColumnGrid(columns, event.target.value));
      return;
    }

    if (tab === "sample-curated" || tab === "sample-all") {
      await renderSampleTab(tab === "sample-all" ? "all" : "curated", body, item);
      return;
    }

    const source = sourceForDataset(item);
    body.innerHTML = `<div class="lineage-summary">
      <div><span>Domain</span><strong>${escapeHtml(item.domain || "Not documented")}</strong></div>
      <div><span>Physical landing</span><strong><code>${escapeHtml(item.table)}</code></strong></div>
      <div><span>Registered source</span><strong>${escapeHtml(source ? pick(source, ["title", "source_id"], "Observed") : "Operational source evidence unavailable")}</strong></div>
      <div><span>Downstream relation</span><strong>${escapeHtml(source ? (pick(source, ["downstream_tables"], []) || []).join(" · ") || "No downstream table listed" : "Review the Lineage workspace when source contracts are connected")}</strong></div>
    </div>`;
  }

  function renderColumnGrid(columns, query) {
    const normalized = String(query || "").toLowerCase();
    const filtered = columns.filter(column => `${column.name} ${column.type}`.toLowerCase().includes(normalized));
    $("#column-grid").innerHTML = filtered.length ? filtered.map((column, index) => `<div class="column-row"><span>${String(index + 1).padStart(2, "0")}</span><strong title="${escapeHtml(column.name)}">${escapeHtml(column.name)}</strong><code>${escapeHtml(column.type)}</code></div>`).join("") : '<div class="empty-state"><strong>No matching columns</strong></div>';
  }

  function sampleStateKey(datasetKey, kind) {
    return `${datasetKey}:${kind}`;
  }

  function selectedSampleLimit(datasetKey, kind) {
    return state.sampleLimits.get(sampleStateKey(datasetKey, kind)) || 50;
  }

  function sampleShell(kind, limit, content, meta = {}) {
    const allColumns = kind === "all";
    const dataset = state.selectedDataset || {};
    const title = allColumns ? "Physical record inspection" : "Curated record inspection";
    const description = allColumns
      ? "Every physical source column is shown without curated aliases. Wide records require horizontal scrolling."
      : "A human-readable subset of important columns, filtered to Los Angeles, California.";
    const rowLabel = meta.rows === undefined ? `Up to ${limit} rows` : `${fullNumber(meta.rows)} ${meta.rows === 1 ? "row" : "rows"}`;
    const columnLabel = meta.columns === undefined ? "Columns pending" : `${fullNumber(meta.columns)} ${meta.columns === 1 ? "column" : "columns"}`;
    return `<section class="sample-workbench ${allColumns ? "all-columns" : ""}" data-sample-workbench data-sample-kind="${escapeHtml(kind)}" data-sample-dataset-key="${escapeHtml(dataset.key || "")}" role="region" aria-labelledby="sample-workbench-title">
    <div class="sample-toolbar">
      <div class="sample-heading">
        <span id="sample-workbench-title">${escapeHtml(title)}</span>
        <p>${escapeHtml(description)}</p>
        ${allColumns ? `<div class="sample-inspector-context">
          <span>ALL-COLUMN SAMPLE · READ ONLY</span>
          <h3 id="sample-inspector-title">${escapeHtml(dataset.title || "Dataset records")}</h3>
          <code>${escapeHtml(dataset.table || "Physical table")}</code>
        </div>` : ""}
      </div>
      <div class="sample-instruments">
        <label class="row-limit-control" for="sample-row-limit">
          <span>ROWS</span>
          <select id="sample-row-limit" data-sample-limit aria-label="Number of sample rows">
            ${[25, 50, 100, 200].map(option => `<option value="${option}" ${option === limit ? "selected" : ""}>${option}</option>`).join("")}
          </select>
        </label>
        <div class="sample-result-meta" role="status" aria-live="polite">
          <strong>${escapeHtml(rowLabel)}</strong>
          <span>${escapeHtml(columnLabel)} · READ ONLY</span>
        </div>
        ${allColumns ? `<button class="sample-expand-control" type="button" data-sample-expand aria-expanded="false" aria-label="Expand all-column sample for ${escapeHtml(dataset.title || "this dataset")}">
          <span aria-hidden="true">↗</span><strong>Expand table</strong>
        </button>
        <button class="sample-close-control" type="button" data-sample-close aria-label="Exit full-screen sample inspector">
          <span aria-hidden="true">×</span><strong>Exit full screen</strong>
        </button>` : ""}
      </div>
    </div>
    <div class="sample-content">${content}</div>
    </section>`;
  }

  function openSampleInspector(trigger, { focusClose = true } = {}) {
    const workbench = trigger?.closest("[data-sample-workbench]");
    if (!workbench || workbench.dataset.sampleKind !== "all") return;
    if (state.sampleInspector?.workbench === workbench) return;
    closeSampleInspector({ restoreFocus: false });

    const placeholder = document.createElement("div");
    placeholder.className = "sample-workbench-placeholder";
    placeholder.style.height = `${workbench.offsetHeight}px`;
    workbench.before(placeholder);
    document.body.append(workbench);
    workbench.classList.add("is-expanded");
    workbench.setAttribute("role", "dialog");
    workbench.setAttribute("aria-modal", "true");
    workbench.setAttribute("aria-labelledby", "sample-inspector-title");
    trigger.setAttribute("aria-expanded", "true");
    document.body.classList.add("sample-inspector-open");
    $(".app-shell")?.setAttribute("inert", "");
    state.sampleInspector = { workbench, trigger, placeholder };

    if (focusClose) requestAnimationFrame(() => $("[data-sample-close]", workbench)?.focus());
  }

  function closeSampleInspector({ restoreFocus = true } = {}) {
    const inspector = state.sampleInspector;
    if (!inspector) return;
    const { workbench, trigger, placeholder } = inspector;
    workbench.classList.remove("is-expanded");
    workbench.setAttribute("role", "region");
    workbench.removeAttribute("aria-modal");
    workbench.setAttribute("aria-labelledby", "sample-workbench-title");
    trigger.setAttribute("aria-expanded", "false");
    if (placeholder.isConnected) placeholder.replaceWith(workbench);
    else workbench.remove();
    document.body.classList.remove("sample-inspector-open");
    $(".app-shell")?.removeAttribute("inert");
    state.sampleInspector = null;
    if (restoreFocus && trigger.isConnected) requestAnimationFrame(() => trigger.focus());
  }

  function restoreSampleInspector(kind, datasetKey) {
    if (!state.reopenSampleInspector || kind !== "all" || state.selectedDataset?.key !== datasetKey || state.selectedTab !== "sample-all") return;
    state.reopenSampleInspector = false;
    const trigger = $("[data-sample-expand]");
    if (trigger) openSampleInspector(trigger);
  }

  function bindSampleLimit(kind, datasetKey) {
    const selector = $("[data-sample-limit]");
    if (!selector) return;
    selector.addEventListener("change", event => {
      const limit = Number(event.target.value);
      if (![25, 50, 100, 200].includes(limit)) return;
      if (kind === "all" && state.sampleInspector) {
        closeSampleInspector({ restoreFocus: false });
        state.reopenSampleInspector = true;
      }
      state.sampleLimits.set(sampleStateKey(datasetKey, kind), limit);
      renderDetailTab(kind === "all" ? "sample-all" : "sample-curated");
    });
  }

  async function renderSampleTab(kind, body, item) {
    const tab = kind === "all" ? "sample-all" : "sample-curated";
    const limit = selectedSampleLimit(item.key, kind);
    const datasetKey = item.key;
    const loadingCopy = kind === "all"
      ? "Requesting raw example records with every physical column."
      : "Requesting a curated Los Angeles, California sample.";

    body.innerHTML = sampleShell(kind, limit, `<div class="empty-state sample-state" aria-busy="true"><strong>Loading sample rows…</strong><p>${escapeHtml(loadingCopy)}</p></div>`);
    bindSampleLimit(kind, datasetKey);

    if (state.offlinePreview) {
      body.innerHTML = sampleShell(kind, limit, '<div class="unavailable-state sample-state"><strong>Sample rows require a live API</strong><p>Offline preview does not fabricate warehouse records. Connect the live explorer endpoint to inspect real rows.</p></div>');
      bindSampleLimit(kind, datasetKey);
      restoreSampleInspector(kind, datasetKey);
      return;
    }

    const cacheKey = `${datasetKey}:${kind}:${limit}`;
    let sample = state.sampleCache.get(cacheKey);
    if (!sample) {
      const path = kind === "all"
        ? `/explorer/sample-all/${encodeURIComponent(datasetKey)}?limit=${limit}`
        : `/explorer/sample/${encodeURIComponent(datasetKey)}?city=Los%20Angeles&state=CA&limit=${limit}`;
      try {
        sample = await getJson(path);
        state.sampleCache.set(cacheKey, sample);
      } catch (error) {
        const requestIsCurrent = state.selectedDataset?.key === datasetKey
          && state.selectedTab === tab
          && selectedSampleLimit(datasetKey, kind) === limit;
        if (requestIsCurrent) {
          body.innerHTML = sampleShell(kind, limit, `<div class="unavailable-state sample-state"><strong>${kind === "all" ? "All-column sample unavailable" : "Curated sample unavailable"}</strong><p>The dataset remains documented, but representative rows could not be loaded from the live API.</p></div>`);
          bindSampleLimit(kind, datasetKey);
          restoreSampleInspector(kind, datasetKey);
        }
        return;
      }
    }

    const requestIsCurrent = state.selectedDataset?.key === datasetKey
      && state.selectedTab === tab
      && selectedSampleLimit(datasetKey, kind) === limit;
    if (!requestIsCurrent) return;

    const columns = (sample.columns || []).map(column => typeof column === "string" ? column : pick(column, ["name", "column_name"], String(column)));
    const rows = Array.isArray(sample.rows) ? sample.rows : [];
    const content = rows.length
      ? dataTable(columns, rows)
      : '<div class="empty-state sample-state"><strong>No sample rows returned</strong><p>The query completed successfully, but no records matched this inspection lens.</p></div>';
    body.innerHTML = sampleShell(kind, limit, content, { rows: rows.length, columns: columns.length });
    bindSampleLimit(kind, datasetKey);
    restoreSampleInspector(kind, datasetKey);
  }

  function dataTable(columns, rows) {
    if (!columns.length) return '<div class="empty-state"><strong>No columns returned</strong><p>The query completed without a tabular schema.</p></div>';
    return `<div class="sample-region" tabindex="0" aria-label="Read-only sample data. Scroll vertically to review rows and horizontally to review columns."><table class="data-table"><thead><tr>${columns.map(column => `<th scope="col">${escapeHtml(column)}</th>`).join("")}</tr></thead><tbody>${rows.map(row => {
      const values = Array.isArray(row) ? row : columns.map(column => row?.[column]);
      return `<tr>${values.map(value => `<td>${formatCell(value)}</td>`).join("")}</tr>`;
    }).join("")}</tbody></table></div>`;
  }

  function sourceForDataset(dataset) {
    return state.sources.find(source => {
      const tables = pick(source, ["downstream_tables"], []) || [];
      return tables.includes(dataset.table) || tables.some(table => dataset.table.includes(String(table).replace("raw_", ""))) || String(pick(source, ["source_id"], "")).includes(dataset.key);
    });
  }

  function lineageNodeEvidence(node) {
    const status = String(node?.evidence_status || "declared").toLowerCase();
    if (["observed", "validated_active", "active", "current", "success", "succeeded"].some(value => status.includes(value))) return "observed";
    if (["declared", "planned", "unknown"].some(value => status.includes(value))) return "declared";
    return "attention";
  }

  function lineageNodeStage(kind) {
    if (kind === "source") return 0;
    if (kind === "raw") return 1;
    if (kind === "transform") return 2;
    return 3;
  }

  function lineageDepths(nodes, edges) {
    const depths = new Map(nodes.map(node => [node.id, lineageNodeStage(node.kind)]));
    for (let pass = 0; pass < nodes.length; pass += 1) {
      let changed = false;
      edges.forEach(edge => {
        const sourceDepth = depths.get(edge.source);
        const targetDepth = depths.get(edge.target);
        if (sourceDepth === undefined || targetDepth === undefined) return;
        const nextDepth = Math.max(targetDepth, sourceDepth + 1);
        if (nextDepth !== targetDepth) {
          depths.set(edge.target, nextDepth);
          changed = true;
        }
      });
      if (!changed) break;
    }
    return depths;
  }

  function lineageStageHeading(stageNodes) {
    const kinds = new Set(stageNodes.map(node => node.kind));
    if (kinds.size === 1 && kinds.has("source")) return "Publisher source";
    if (kinds.size === 1 && kinds.has("raw")) return "Raw landing";
    if (kinds.size === 1 && kinds.has("transform")) return "Transformation";
    if (kinds.size === 1 && kinds.has("bridge")) return "Bridge / core model";
    if ([...kinds].every(kind => ["mart", "summary"].includes(kind))) return "Curated model";
    return "Downstream model";
  }

  function lineageKindLabel(kind) {
    return ({ source: "publisher source", raw: "raw landing", transform: "transform", bridge: "bridge table", mart: "curated mart", summary: "summary" })[kind] || "declared node";
  }

  function lineageStatusLabel(status) {
    return ({ observed: "observed", declared: "declared", attention: "needs attention" })[status] || status;
  }

  function lineageTopology() {
    const payload = state.lineage;
    const nodes = Array.isArray(payload?.nodes) ? payload.nodes.filter(node => node?.id) : [];
    const nodeIds = new Set(nodes.map(node => node.id));
    const edges = (Array.isArray(payload?.edges) ? payload.edges : []).filter(edge => nodeIds.has(edge?.source) && nodeIds.has(edge?.target));
    return { nodes, edges };
  }

  function lineageRelatedNodeIds(selectedId, edges) {
    if (!selectedId) return new Set();
    const upstream = new Set([selectedId]);
    const downstream = new Set([selectedId]);
    let upstreamChanged = true;
    let downstreamChanged = true;
    while (upstreamChanged || downstreamChanged) {
      upstreamChanged = false;
      downstreamChanged = false;
      edges.forEach(edge => {
        if (upstream.has(edge.target) && !upstream.has(edge.source)) {
          upstream.add(edge.source);
          upstreamChanged = true;
        }
        if (downstream.has(edge.source) && !downstream.has(edge.target)) {
          downstream.add(edge.target);
          downstreamChanged = true;
        }
      });
    }
    return new Set([...upstream, ...downstream]);
  }

  function lineageFilterNodeIds(nodes, edges) {
    if (state.lineageFilter === "all") return new Set(nodes.map(node => node.id));
    const matching = new Set(nodes.filter(node => lineageNodeEvidence(node) === state.lineageFilter).map(node => node.id));
    const contextual = new Set(matching);
    edges.forEach(edge => {
      if (matching.has(edge.source)) contextual.add(edge.target);
      if (matching.has(edge.target)) contextual.add(edge.source);
    });
    return contextual;
  }

  function renderLineageSummary(summary = {}) {
    const values = [summary.source, summary.raw, summary.transform, summary.bridge, (Number(summary.mart) || 0) + (Number(summary.summary) || 0)];
    $$("#lineage-summary strong").forEach((element, index) => { element.textContent = Number.isFinite(Number(values[index])) ? fullNumber(values[index]) : "—"; });
  }

  function renderLineage() {
    const banner = $("#lineage-banner");
    const container = $("#lineage-lanes");
    const payload = state.lineage;
    renderLineageSummary(payload?.summary);
    container.classList.remove("loading-block");

    if (!state.operations.lineage?.ok) {
      banner.textContent = "Lineage evidence is unavailable. The Command Center does not infer a topology from catalog metadata.";
      container.innerHTML = '<div class="unavailable-state lineage-empty"><strong>Lineage graph unavailable</strong><p>Connect <code>/operations/lineage</code> to inspect declared transforms and read-only inventory evidence.</p></div>';
      return;
    }

    const { nodes, edges } = lineageTopology();
    if (!nodes.length) {
      banner.textContent = payload?.evidence_error ? `Operational evidence note: ${payload.evidence_error}` : "";
      container.innerHTML = '<div class="empty-state lineage-empty"><strong>No lineage nodes returned</strong><p>The endpoint responded successfully, but it did not declare any source, table, or transform dependencies.</p></div>';
      return;
    }

    const generatedAt = payload?.generated_at ? `Generated ${formatDate(payload.generated_at)}.` : "";
    const evidenceNote = payload?.evidence_error ? ` Operational evidence note: ${payload.evidence_error}` : "";
    banner.textContent = `Edges marked declared describe pipeline intent; only observed edges have supporting active-warehouse evidence.${generatedAt ? ` ${generatedAt}` : ""}${evidenceNote}`;

    const visibleIds = lineageFilterNodeIds(nodes, edges);
    if (state.selectedLineageNode && !visibleIds.has(state.selectedLineageNode)) state.selectedLineageNode = null;
    const visibleNodes = nodes.filter(node => visibleIds.has(node.id));
    const visibleEdges = edges.filter(edge => visibleIds.has(edge.source) && visibleIds.has(edge.target));
    const relatedIds = lineageRelatedNodeIds(state.selectedLineageNode, visibleEdges);
    const depths = lineageDepths(visibleNodes, visibleEdges);
    const stageCount = Math.max(1, ...visibleNodes.map(node => depths.get(node.id) || 0)) + 1;
    const stages = Array.from({ length: stageCount }, (_, stage) => visibleNodes.filter(node => depths.get(node.id) === stage).sort((a, b) => String(a.label).localeCompare(String(b.label))));
    const stageRows = Math.max(1, ...stages.map(stage => stage.length));
    const graphHeight = Math.max(560, 98 + stageRows * 98);
    const stageWidth = 174;
    const graphWidth = Math.max(880, stageCount * stageWidth + 24);
    const positions = new Map();
    stages.forEach((stageNodes, stage) => stageNodes.forEach((node, index) => positions.set(node.id, { x: 18 + stage * stageWidth, y: 72 + index * 98 })));
    const stageNames = stages.map(lineageStageHeading);

    const connectorMarkup = visibleEdges.map(edge => {
      const from = positions.get(edge.source);
      const to = positions.get(edge.target);
      if (!from || !to) return "";
      const startX = from.x + 160;
      const startY = from.y + 34;
      const endX = to.x;
      const endY = to.y + 34;
      const curve = Math.max(28, (endX - startX) * .42);
      const evidence = String(edge.evidence_status || "declared").toLowerCase().includes("observed") ? "observed" : "declared";
      const related = !state.selectedLineageNode || (relatedIds.has(edge.source) && relatedIds.has(edge.target));
      return `<path class="lineage-edge ${evidence}${related ? " is-related" : " is-dimmed"}" d="M ${startX} ${startY} C ${startX + curve} ${startY}, ${endX - curve} ${endY}, ${endX} ${endY}" data-edge-id="${escapeHtml(edge.id || "")}" />`;
    }).join("");

    const nodeMarkup = visibleNodes.map(node => {
      const position = positions.get(node.id);
      const evidence = lineageNodeEvidence(node);
      const isSelected = node.id === state.selectedLineageNode;
      const related = !state.selectedLineageNode || relatedIds.has(node.id);
      const observedRows = node?.observed?.approx_rows;
      const subtitle = node.kind === "source"
        ? pick(node, ["source_id"], lineageKindLabel(node.kind))
        : node.table || node.transform_id || lineageKindLabel(node.kind);
      return `<button type="button" class="lineage-node ${escapeHtml(node.kind || "unknown")} evidence-${evidence}${isSelected ? " selected" : ""}${related ? " is-related" : " is-dimmed"}" data-lineage-node="${escapeHtml(node.id)}" style="--node-x:${position.x}px;--node-y:${position.y}px" aria-pressed="${isSelected}">
        <span class="lineage-node-top"><span class="lineage-node-kind">${escapeHtml(lineageKindLabel(node.kind))}</span><i class="lineage-evidence ${evidence}" aria-label="${escapeHtml(lineageStatusLabel(evidence))}"></i></span>
        <strong title="${escapeHtml(node.label || node.id)}">${escapeHtml(node.label || node.id)}</strong>
        <small title="${escapeHtml(subtitle)}">${escapeHtml(subtitle)}</small>
        ${observedRows !== null && observedRows !== undefined ? `<em>${escapeHtml(compactNumber(observedRows))} rows</em>` : ""}
      </button>`;
    }).join("");

    container.style.minWidth = `${graphWidth}px`;
    container.style.height = `${graphHeight}px`;
    container.innerHTML = `<div class="lineage-stage-headings" style="grid-template-columns:repeat(${stageCount}, ${stageWidth}px);width:${graphWidth}px" aria-hidden="true">${stageNames.map(name => `<span>${name}</span>`).join("")}</div><svg class="lineage-connectors" viewBox="0 0 ${graphWidth} ${graphHeight}" width="${graphWidth}" height="${graphHeight}" aria-hidden="true"><defs><marker id="lineage-arrowhead" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto"><path d="M 0 0 L 8 4 L 0 8 z" /></marker></defs>${connectorMarkup}</svg>${nodeMarkup}`;
    renderLineageInspector(nodes, edges);
  }

  function lineageDetails(node) {
    const details = node?.details && typeof node.details === "object" ? node.details : {};
    const observed = node?.observed && typeof node.observed === "object" ? node.observed : {};
    const manifest = node?.latest_manifest && typeof node.latest_manifest === "object" ? node.latest_manifest : {};
    const facts = [
      ["Node kind", lineageKindLabel(node?.kind)],
      ["Evidence", lineageStatusLabel(lineageNodeEvidence(node))],
      ["Table", node?.table],
      ["Source ID", node?.source_id],
      ["Transform ID", node?.transform_id],
      ["Warehouse inventory", observed.table_present === true ? "Observed in active inventory" : observed.table_present === false ? "Not observed in active inventory" : null],
      ["Approx. rows", observed.approx_rows !== null && observed.approx_rows !== undefined ? fullNumber(observed.approx_rows) : null],
      ["Source period", pick(manifest, ["source_data_period", "source_period"], null)],
      ...Object.entries(details).map(([key, value]) => [key.replaceAll("_", " "), typeof value === "object" ? JSON.stringify(value) : value])
    ];
    return facts.filter(([, value]) => value !== null && value !== undefined && value !== "");
  }

  function renderLineageInspector(nodes, edges) {
    const inspector = $("#lineage-inspector");
    const node = nodes.find(item => item.id === state.selectedLineageNode);
    if (!node) {
      inspector.innerHTML = `<span class="eyebrow">Graph inspector</span><h2>No node selected</h2><p>Select a source, landing, transform, bridge, or curated model to trace every connected upstream and downstream path.</p><div class="lineage-inspector-note"><strong>Evidence-aware</strong><span>Declared edges are implementation intent. Observed edges have active-warehouse support.</span></div>`;
      return;
    }
    const byId = new Map(nodes.map(item => [item.id, item]));
    const upstream = edges.filter(edge => edge.target === node.id).map(edge => ({ edge, node: byId.get(edge.source) })).filter(item => item.node);
    const downstream = edges.filter(edge => edge.source === node.id).map(edge => ({ edge, node: byId.get(edge.target) })).filter(item => item.node);
    const dependencyList = (items, direction) => items.length ? `<ul class="lineage-dependency-list">${items.map(({ edge, node: dependency }) => `<li><button type="button" data-lineage-node="${escapeHtml(dependency.id)}"><strong>${escapeHtml(dependency.label || dependency.id)}</strong><small>${escapeHtml(direction)} · ${escapeHtml(edge.label || edge.kind || "dependency")} · ${escapeHtml(String(edge.evidence_status || "declared"))}</small></button></li>`).join("")}</ul>` : '<p class="lineage-no-dependencies">None declared.</p>';
    inspector.innerHTML = `<span class="eyebrow">Selected ${escapeHtml(lineageKindLabel(node.kind))}</span><h2>${escapeHtml(node.label || node.id)}</h2><div class="lineage-inspector-status"><span class="lineage-kind-badge">${escapeHtml(lineageKindLabel(node.kind))}</span><span class="status-chip ${lineageNodeEvidence(node) === "observed" ? "current" : lineageNodeEvidence(node) === "attention" ? "warning" : ""}">${escapeHtml(lineageStatusLabel(lineageNodeEvidence(node)))}</span></div><dl class="inspector-facts">${lineageDetails(node).map(([label, value]) => `<div><dt>${escapeHtml(label)}</dt><dd>${escapeHtml(value)}</dd></div>`).join("")}</dl><section class="lineage-dependencies"><h3>Direct inputs</h3>${dependencyList(upstream, "reads from")}</section><section class="lineage-dependencies"><h3>Direct outputs</h3>${dependencyList(downstream, "feeds")}</section>`;
  }

  function inspectLineage(nodeId) {
    const { nodes } = lineageTopology();
    if (!nodes.some(node => node.id === nodeId)) return;
    state.selectedLineageNode = state.selectedLineageNode === nodeId ? null : nodeId;
    renderLineage();
  }

  function renderContracts() {
    filterContracts();
  }

  function filterContracts() {
    const tbody = $("#contracts-body");
    const empty = $("#contracts-empty");
    const query = $("#contract-search").value.trim().toLowerCase();
    const selectedStatus = $("#contract-status").value;
    const filtered = state.sources.filter(source => {
      const status = evidenceStatus(source);
      const manifest = manifestOf(source) || {};
      const searchable = [source.title, source.source_id, source.publisher, manifest.schema_fingerprint, manifest.publisher_version].join(" ").toLowerCase();
      return (!query || searchable.includes(query)) && (selectedStatus === "all" || status === selectedStatus);
    });

    tbody.innerHTML = "";
    empty.classList.remove("show");
    if (!state.operations.sources?.ok) {
      empty.classList.add("show");
      empty.innerHTML = '<strong>Contract evidence is unavailable</strong><p>The registry is not populated from static assumptions. Deploy <code>/operations/sources</code> to review source contracts and their latest manifest evidence.</p>';
      return;
    }
    if (!filtered.length) {
      empty.classList.add("show");
      empty.innerHTML = state.sources.length ? '<strong>No matching contracts</strong><p>Adjust the search or status filter.</p>' : '<strong>No source contracts registered</strong><p>The endpoint is connected but returned no entries.</p>';
      return;
    }

    tbody.innerHTML = filtered.map(source => {
      const manifest = manifestOf(source) || {};
      const status = evidenceStatus(source);
      const fingerprint = pick(manifest, ["schema_fingerprint"], null);
      const reason = pick(source, ["evidence_reason"], "No evidence reason returned");
      const promotion = pick(manifest, ["promotion_state"], null);
      return `<tr>
        <td><span class="cell-title">${escapeHtml(pick(source, ["title", "source_id"], "Unknown source"))}</span><span class="cell-subtitle">${escapeHtml(pick(source, ["publisher", "source_id"], "publisher unknown"))}</span></td>
        <td>${statusChip(status, status)}</td>
        <td><code>${escapeHtml(pick(manifest, ["source_data_period"], "not observed"))}</code></td>
        <td><code>${escapeHtml(pick(manifest, ["publisher_version"], "not observed"))}</code></td>
        <td><code>${rowCountFrom(source) === null ? "—" : fullNumber(rowCountFrom(source))}</code></td>
        <td><code title="${escapeHtml(fingerprint || "Not observed")}">${escapeHtml(fingerprint ? String(fingerprint).slice(0, 16) + "…" : "not observed")}</code></td>
        <td><span class="cell-title">${escapeHtml(promotion || "Not proven")}</span><span class="cell-subtitle" title="${escapeHtml(reason)}">${escapeHtml(reason)}</span></td>
      </tr>`;
    }).join("");
  }

  function renderOperations() {
    const tbody = $("#runs-body");
    const empty = $("#runs-empty");
    $("#runs-count").textContent = state.operations.runs?.ok ? `${state.runs.length} RUNS` : "EVIDENCE OFFLINE";
    tbody.innerHTML = "";
    empty.classList.remove("show");

    if (!state.operations.runs?.ok) {
      empty.classList.add("show");
      empty.innerHTML = '<strong>Run history is unavailable</strong><p>The serving API returned no operational ledger. No successful refresh is inferred or simulated.</p>';
      return;
    }
    if (!state.runs.length) {
      empty.classList.add("show");
      empty.innerHTML = '<strong>No pipeline runs recorded</strong><p>The operations endpoint is connected, but manifest history is empty.</p>';
      return;
    }

    tbody.innerHTML = state.runs.map(run => {
      const acquire = pick(run, ["retrieval_timestamp"], null) ? "current" : "unavailable";
      const validationRaw = String(pick(run, ["validation_state"], "not observed"));
      const validation = evidenceStatus({ status: validationRaw });
      const candidate = pick(run, ["release_id"], null) ? "current" : "unavailable";
      const promotionRaw = String(pick(run, ["promotion_state"], "not observed"));
      const promotion = evidenceStatus({ status: promotionRaw });
      return `<tr>
        <td><span class="cell-title"><code>${escapeHtml(String(pick(run, ["run_id"], "run unknown")).slice(0, 18))}</code></span><span class="cell-subtitle">${escapeHtml(formatDate(pick(run, ["discovery_timestamp", "started_at"], null)))}</span></td>
        <td><strong>${escapeHtml(pick(run, ["source_id", "source"], "unknown"))}</strong></td>
        <td>${statusChip(acquire, acquire === "current" ? "acquired" : "not observed")}</td>
        <td>${statusChip(validation, validationRaw)}</td>
        <td>${statusChip(candidate, candidate === "current" ? "built" : "not observed")}</td>
        <td>${statusChip(promotion, promotionRaw)}</td>
      </tr>`;
    }).join("");
  }

  function providerMeta(npi) {
    return evidenceProviders.find(provider => provider.npi === npi) || { npi, name: `NPI ${npi}`, specialty: "Specialty not listed" };
  }

  function providerTable(source, npi) {
    const table = source?.providers?.[npi];
    return {
      columns: Array.isArray(table?.columns) ? table.columns : [],
      rows: Array.isArray(table?.rows) ? table.rows : []
    };
  }

  function summaryColumnIndexes(columns) {
    const labels = columns.map(column => String(column).toLowerCase());
    const groups = [
      /facility|group.*(?:name|legal)|org(?:anization)?_?name|legal.*business/,
      /practice_address_1|adr_ln_1|address(?:_1)?$|street/,
      /city|town/,
      /org_pac|group pac|rcv_bnft_enrlmt_id|enrlmt_id/,
      /sole_proprietor/
    ];
    const indexes = [];
    groups.forEach(pattern => {
      const index = labels.findIndex((label, position) => pattern.test(label) && !indexes.includes(position));
      if (index >= 0) indexes.push(index);
    });
    return indexes.slice(0, 3);
  }

  function evidenceCellSummary(source, npi) {
    if (source.availability === "unavailable") return "Required source tables are not loaded";
    if (source.availability === "query_error") return "The source query could not be completed";
    const table = providerTable(source, npi);
    if (!table.rows.length) return "No source-native row returned";
    const indexes = summaryColumnIndexes(table.columns);
    const values = [];
    for (const row of table.rows) {
      for (const index of indexes) {
        const value = row[index];
        if (value !== null && value !== undefined && String(value).trim() && !values.includes(String(value))) values.push(String(value));
        if (values.length === 2) return values.join(" · ");
      }
    }
    return values.join(" · ") || "Open the raw fields to inspect this record";
  }

  function renderProviderCards() {
    $$("[data-provider-npi]").forEach(button => {
      const selected = button.dataset.providerNpi === state.providerEvidence.focusedNpi;
      button.classList.toggle("selected", selected);
      button.setAttribute("aria-pressed", String(selected));
    });
  }

  function evidenceAvailability(source) {
    if (source.availability === "available") return { status: "current", label: "Source loaded" };
    if (source.availability === "query_error") return { status: "failed", label: "Query error" };
    return { status: "warning", label: "Awaiting ingestion" };
  }

  function renderProviderEvidence() {
    const matrix = $("#evidence-matrix");
    const result = state.providerEvidence.result;
    renderProviderCards();
    matrix.setAttribute("aria-busy", "false");

    if (!result?.ok) {
      state.providerEvidence.data = null;
      matrix.innerHTML = `<div class="evidence-offline">
        <span class="evidence-offline-code">LIVE API REQUIRED</span>
        <strong>Provider records are unavailable in offline mode.</strong>
        <p>This page never fabricates source rows. Connect <code>/api/explorer/provider-evidence</code> to compare the four clinicians.</p>
      </div>`;
      $("#record-inspector").innerHTML = `<div class="record-inspector-empty"><span class="empty-glyph" aria-hidden="true">⌗</span><h2 id="record-inspector-title">Live evidence required</h2><p>The raw record inspector remains empty until the provider-evidence endpoint responds.</p></div>`;
      return;
    }

    state.providerEvidence.data = result.data;
    const sources = Array.isArray(result.data?.sources) ? result.data.sources : [];
    if (!sources.length) {
      matrix.innerHTML = '<div class="evidence-offline"><strong>No evidence sources were returned.</strong><p>The endpoint is connected but its source registry is empty.</p></div>';
      return;
    }

    if (!state.providerEvidence.selectedSourceKey) {
      const firstWithRows = sources.find(source => providerTable(source, state.providerEvidence.focusedNpi).rows.length);
      state.providerEvidence.selectedSourceKey = (firstWithRows || sources[0]).key;
    }

    matrix.innerHTML = `<table class="evidence-matrix-table">
      <thead><tr>
        <th scope="col" class="source-column-head"><span>PUBLIC SOURCE FILE</span><small>grain / relationship claim</small></th>
        ${evidenceProviders.map(provider => `<th scope="col" class="provider-column-head ${provider.npi === state.providerEvidence.focusedNpi ? "focused" : ""}"><strong>${escapeHtml(provider.name)}</strong><code>${escapeHtml(provider.npi)}</code></th>`).join("")}
      </tr></thead>
      <tbody>${sources.map((source, sourceIndex) => {
        const availability = evidenceAvailability(source);
        return `<tr>
          <th scope="row" class="evidence-source-head">
            <span class="source-order">${String(sourceIndex + 1).padStart(2, "0")}</span>
            <strong>${escapeHtml(source.title)}</strong>
            <code>${escapeHtml(source.table)}</code>
            <span class="source-classifiers"><i class="source-layer layer-${source.layer === "curated" ? "curated" : "raw"}">${escapeHtml(source.layer || "raw")}</i><i class="evidence-kind">${escapeHtml(String(source.evidence_kind || "publisher_asserted").replaceAll("_", " "))}</i></span>
            <p>${escapeHtml(source.grain)}</p>
            ${statusChip(availability.status, availability.label)}
          </th>
          ${evidenceProviders.map(provider => {
            const table = providerTable(source, provider.npi);
            const selected = source.key === state.providerEvidence.selectedSourceKey && provider.npi === state.providerEvidence.focusedNpi;
            const unavailable = source.availability !== "available";
            const cellStatus = unavailable ? availability.status : (table.rows.length ? "current" : "unavailable");
            const countLabel = unavailable ? availability.label : `${table.rows.length} ${table.rows.length === 1 ? "record" : "records"} returned`;
            return `<td class="evidence-cell ${provider.npi === state.providerEvidence.focusedNpi ? "focused-column" : ""}">
              <button type="button" class="evidence-cell-button ${selected ? "selected" : ""} status-${cellStatus}" data-evidence-source="${escapeHtml(source.key)}" data-evidence-npi="${escapeHtml(provider.npi)}" aria-pressed="${selected}" aria-label="Inspect ${escapeHtml(source.title)} for ${escapeHtml(provider.name)}; ${escapeHtml(countLabel)}">
                <span class="cell-record-count"><i class="status-dot ${cellStatus}"></i><strong>${escapeHtml(countLabel)}</strong><b aria-hidden="true">↘</b></span>
                <small>${escapeHtml(evidenceCellSummary(source, provider.npi))}</small>
              </button>
            </td>`;
          }).join("")}
        </tr>`;
      }).join("")}</tbody>
    </table>`;
    renderEvidenceInspector();
  }

  function renderEvidenceInspector() {
    const inspector = $("#record-inspector");
    const sources = state.providerEvidence.data?.sources || [];
    const source = sources.find(item => item.key === state.providerEvidence.selectedSourceKey);
    const provider = providerMeta(state.providerEvidence.focusedNpi);
    if (!source) return;

    const table = providerTable(source, provider.npi);
    const availability = evidenceAvailability(source);
    const rowIndex = Math.min(state.providerEvidence.selectedRow, Math.max(table.rows.length - 1, 0));
    state.providerEvidence.selectedRow = rowIndex;
    const row = table.rows[rowIndex] || [];
    const missing = Array.isArray(source.missing_tables) ? source.missing_tables : [];

    const body = source.availability !== "available"
      ? `<div class="inspector-unavailable"><span class="status-chip ${availability.status}"><i class="status-dot ${availability.status}"></i>${escapeHtml(availability.label)}</span><strong>These rows are part of the model, but not yet available.</strong><p>${missing.length ? `Required evidence: ${missing.map(item => `<code>${escapeHtml(item)}</code>`).join(" · ")}` : "The live source query did not complete."}</p></div>`
      : !table.rows.length
        ? '<div class="inspector-unavailable empty"><span class="status-chip unavailable"><i class="status-dot unavailable"></i>0 records</span><strong>No record for this provider in this source.</strong><p>An empty result is evidence too; it is not replaced with a row from another file.</p></div>'
        : `<div class="raw-record-workbench">
          ${table.rows.length > 1 ? `<div class="record-tabs" role="tablist" aria-label="Raw records">${table.rows.map((_, index) => `<button type="button" role="tab" aria-selected="${index === rowIndex}" class="${index === rowIndex ? "selected" : ""}" data-evidence-row="${index}"><span>${String(index + 1).padStart(2, "0")}</span>Record ${index + 1}</button>`).join("")}</div>` : '<div class="single-record-label">RAW RECORD 01</div>'}
          <div class="raw-field-frame" tabindex="0" aria-label="Source-native field and value table">
            <table class="raw-field-table"><thead><tr><th scope="col">Physical field</th><th scope="col">Source value</th></tr></thead><tbody>
              ${table.columns.map((column, index) => `<tr><th scope="row"><code>${escapeHtml(column)}</code></th><td>${formatCell(row[index])}</td></tr>`).join("")}
            </tbody></table>
          </div>
        </div>`;

    inspector.innerHTML = `<div class="inspector-heading">
      <div><span class="eyebrow">Selected evidence path</span><h2 id="record-inspector-title">${escapeHtml(provider.name)} <span>×</span> ${escapeHtml(source.title)}</h2><p><code>${escapeHtml(provider.npi)}</code> · <code>${escapeHtml(source.table)}</code></p></div>
      <span class="inspector-row-count">${source.availability === "available" ? `${table.rows.length} ${table.rows.length === 1 ? "ROW" : "ROWS"} RETURNED` : availability.label.toUpperCase()}</span>
    </div>
    <div class="claim-ledger">
      <article class="claim-proves"><span>What this proves</span><p>${escapeHtml(source.proves)}</p></article>
      <article class="claim-limits"><span>What this does not prove</span><p>${escapeHtml(source.does_not_prove)}</p></article>
      <article class="claim-grain"><span>Source relationship</span><p>${escapeHtml(source.relationship)}</p></article>
    </div>
    ${body}`;
  }

  function selectEvidenceCell(sourceKey, npi) {
    state.providerEvidence.selectedSourceKey = sourceKey;
    state.providerEvidence.focusedNpi = npi;
    state.providerEvidence.selectedRow = 0;
    renderProviderEvidence();
    requestAnimationFrame(() => $("#record-inspector")?.scrollIntoView({ behavior: "smooth", block: "start" }));
  }

  function routeFromHash() {
    closeSampleInspector({ restoreFocus: false });
    state.reopenSampleInspector = false;
    const raw = location.hash.replace(/^#/, "") || "overview";
    const [route, detail] = raw.split("/");
    const validRoute = ["overview", "provider-evidence", "catalog", "lineage", "contracts", "operations"].includes(route) ? route : "overview";
    $$(".view").forEach(view => {
      const active = view.dataset.view === validRoute;
      view.hidden = !active;
      view.classList.toggle("active", active);
    });
    $$(".rail-nav a").forEach(link => link.setAttribute("aria-current", link.dataset.route === validRoute ? "page" : "false"));
    if (validRoute === "catalog" && detail) selectDataset(detail, false);
    $("#mobile-menu").setAttribute("aria-expanded", "false");
    $(".rail").classList.remove("open");
    document.title = `${validRoute[0].toUpperCase()}${validRoute.slice(1)} · CMS Data Command Center`;
    window.scrollTo({ top: 0, behavior: "auto" });
  }

  function bindEvents() {
    window.addEventListener("hashchange", routeFromHash);
    $("#catalog-search").addEventListener("input", filterCatalog);
    $("#catalog-domain").addEventListener("change", filterCatalog);
    $("#contract-search").addEventListener("input", filterContracts);
    $("#contract-status").addEventListener("change", filterContracts);

    document.addEventListener("click", event => {
      const datasetButton = event.target.closest(".dataset-button[data-dataset-key]");
      if (datasetButton) selectDataset(datasetButton.dataset.datasetKey);
      const expandSample = event.target.closest("[data-sample-expand]");
      if (expandSample) openSampleInspector(expandSample);
      const closeSample = event.target.closest("[data-sample-close]");
      if (closeSample) closeSampleInspector();
      const detailTab = event.target.closest("[data-detail-tab]");
      if (detailTab) renderDetailTab(detailTab.dataset.detailTab);
      const flightButton = event.target.closest("[data-flight-target]");
      if (flightButton) location.hash = flightButton.dataset.flightTarget;
      const lineageFilter = event.target.closest("[data-lineage-filter]");
      if (lineageFilter) {
        state.lineageFilter = lineageFilter.dataset.lineageFilter;
        state.selectedLineageNode = null;
        $$("[data-lineage-filter]").forEach(button => {
          const selected = button.dataset.lineageFilter === state.lineageFilter;
          button.classList.toggle("selected", selected);
          button.setAttribute("aria-pressed", String(selected));
        });
        renderLineage();
      }
      const lineagePan = event.target.closest("[data-lineage-pan]");
      if (lineagePan?.dataset.lineagePan === "outputs") {
        const canvas = $("#lineage-graph-canvas");
        canvas?.scrollTo({ left: canvas.scrollWidth, behavior: "smooth" });
      }
      const lineageButton = event.target.closest("[data-lineage-node]");
      if (lineageButton) inspectLineage(lineageButton.dataset.lineageNode);
      const providerButton = event.target.closest("[data-provider-npi]");
      if (providerButton) {
        state.providerEvidence.focusedNpi = providerButton.dataset.providerNpi;
        state.providerEvidence.selectedRow = 0;
        renderProviderEvidence();
      }
      const evidenceCell = event.target.closest("[data-evidence-source][data-evidence-npi]");
      if (evidenceCell) selectEvidenceCell(evidenceCell.dataset.evidenceSource, evidenceCell.dataset.evidenceNpi);
      const evidenceRow = event.target.closest("[data-evidence-row]");
      if (evidenceRow) {
        state.providerEvidence.selectedRow = Number(evidenceRow.dataset.evidenceRow);
        renderEvidenceInspector();
      }
    });

    document.addEventListener("keydown", event => {
      const inspector = state.sampleInspector;
      if (!inspector) return;
      if (event.key === "Escape") {
        event.preventDefault();
        closeSampleInspector();
        return;
      }
      if (event.key !== "Tab") return;
      const focusable = $$('button:not([disabled]), select:not([disabled]), [href], [tabindex]:not([tabindex="-1"])', inspector.workbench)
        .filter(element => element.offsetParent !== null);
      if (!focusable.length) return;
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    });

    $("#mobile-menu").addEventListener("click", () => {
      const rail = $(".rail");
      const open = !rail.classList.contains("open");
      rail.classList.toggle("open", open);
      $("#mobile-menu").setAttribute("aria-expanded", String(open));
    });
  }

  bindEvents();
  initialize().catch(error => {
    console.error("Command center failed to initialize", error);
    showToast("The command center could not initialize. Reload to try again.");
  });
})();
