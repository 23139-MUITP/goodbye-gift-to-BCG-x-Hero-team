const state = {
  token: localStorage.getItem("proptech_token") || "",
  user: null,
  cityFilter: "",
  customerPhone: "",
  geo: null,
  photoBase64: "",
  cached: {
    inventory: [],
    slots: [],
    visits: [],
    customerVisits: [],
  },
};

const els = {
  authPill: document.getElementById("auth-pill"),
  loginPanel: document.getElementById("login-panel"),
  mainPanel: document.getElementById("main-panel"),
  loginForm: document.getElementById("login-form"),
  email: document.getElementById("login-email"),
  password: document.getElementById("login-password"),
  cityFilter: document.getElementById("city-filter"),
  refreshBtn: document.getElementById("refresh-button"),
  logoutBtn: document.getElementById("logout-button"),
  metricsGrid: document.getElementById("metrics-grid"),
  flagStrip: document.getElementById("flag-strip"),
  brokerSections: document.getElementById("broker-sections"),
  rmSections: document.getElementById("rm-sections"),
  srmSections: document.getElementById("srm-sections"),
  inventoryForm: document.getElementById("inventory-form"),
  inventoryList: document.getElementById("inventory-list"),
  visitsList: document.getElementById("visits-list"),
  slotForm: document.getElementById("slot-form"),
  slotsList: document.getElementById("slots-list"),
  duplicateQueue: document.getElementById("duplicate-queue"),
  emergencyQueue: document.getElementById("emergency-queue"),
  escalationQueue: document.getElementById("escalation-queue"),
  leadsList: document.getElementById("leads-list"),
  visitReport: document.getElementById("visit-report"),
  importLeadsBtn: document.getElementById("import-leads"),
  bookVisitForm: document.getElementById("book-visit-form"),
  calcTourDurationBtn: document.getElementById("calc-tour-duration"),
  tourPropertyCount: document.getElementById("tour-property-count"),
  tourDurationOutput: document.getElementById("tour-duration-output"),
  waSendForm: document.getElementById("wa-send-form"),
  waPhone: document.getElementById("wa-phone"),
  waTemplate: document.getElementById("wa-template"),
  waVisitId: document.getElementById("wa-visit-id"),
  waContext: document.getElementById("wa-context"),
  waMessages: document.getElementById("wa-messages"),
  funnelReport: document.getElementById("funnel-report"),
  brokerReliability: document.getElementById("broker-reliability"),
  exportVisitCounts: document.getElementById("export-visit-counts"),
  exportFunnel: document.getElementById("export-funnel"),
  exportBrokerRel: document.getElementById("export-broker-rel"),
  exportWa: document.getElementById("export-wa"),
  exportVisits: document.getElementById("export-visits"),
  customerLoadForm: document.getElementById("customer-load-form"),
  customerPhone: document.getElementById("customer-phone"),
  customerVisits: document.getElementById("customer-visits"),
  toast: document.getElementById("toast"),

  removeDialog: document.getElementById("remove-dialog"),
  removeForm: document.getElementById("remove-form"),
  removePropertyId: document.getElementById("remove-property-id"),
  removeReason: document.getElementById("remove-reason"),
  removeDetails: document.getElementById("remove-details"),
  removeCancel: document.getElementById("remove-cancel"),

  cancelDialog: document.getElementById("cancel-slot-dialog"),
  cancelForm: document.getElementById("cancel-slot-form"),
  cancelSlotId: document.getElementById("cancel-slot-id"),
  cancelReason: document.getElementById("cancel-slot-reason"),
  cancelEmergency: document.getElementById("cancel-emergency"),
  cancelEmergencyReason: document.getElementById("cancel-emergency-reason"),
  cancelEmergencyDetails: document.getElementById("cancel-emergency-details"),
  cancelClose: document.getElementById("cancel-slot-close"),

  completeDialog: document.getElementById("complete-visit-dialog"),
  completeForm: document.getElementById("complete-visit-form"),
  completeVisitId: document.getElementById("complete-visit-id"),
  completeOtp: document.getElementById("complete-otp"),
  fetchGeoBtn: document.getElementById("fetch-geo"),
  geoStatus: document.getElementById("geo-status"),
  completePhoto: document.getElementById("complete-photo"),
  completeClose: document.getElementById("complete-close"),
};

function showToast(message, tone = "ok") {
  els.toast.textContent = message;
  els.toast.className = `toast ${tone}`;
  els.toast.classList.remove("hidden");
  window.setTimeout(() => {
    els.toast.classList.add("hidden");
  }, 2200);
}

function formatDate(value) {
  if (!value) return "-";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleString();
}

function statusPill(value) {
  const text = value || "-";
  let cls = "status-pill";
  if (text.includes("pending") || text.includes("escalated")) cls += " warn";
  if (text.includes("rejected") || text.includes("cancel")) cls += " danger";
  return `<span class="${cls}">${text}</span>`;
}

async function api(path, options = {}) {
  const opts = {
    method: options.method || "GET",
    headers: {
      "Content-Type": "application/json",
      ...(state.token ? { Authorization: `Bearer ${state.token}` } : {}),
      ...(options.headers || {}),
    },
  };

  if (options.body !== undefined) {
    opts.body = JSON.stringify(options.body);
  }

  const res = await fetch(path, opts);
  const payload = await res.json().catch(() => ({}));
  if (!res.ok || payload.ok === false) {
    throw new Error(payload.error || `Request failed (${res.status})`);
  }
  return payload;
}

async function downloadCsv(exportType) {
  const res = await fetch(`/api/reports/export.csv?type=${encodeURIComponent(exportType)}`, {
    headers: {
      ...(state.token ? { Authorization: `Bearer ${state.token}` } : {}),
    },
  });
  if (!res.ok) {
    let errText = `Export failed (${res.status})`;
    try {
      const payload = await res.json();
      errText = payload.error || errText;
    } catch {
      // no-op
    }
    throw new Error(errText);
  }
  const blob = await res.blob();
  const disposition = res.headers.get("Content-Disposition") || "";
  const match = disposition.match(/filename=\"?([^\";]+)\"?/i);
  const filename = match?.[1] || `${exportType}.csv`;
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

function toBase64(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = reject;
    reader.readAsDataURL(file);
  });
}

function buildTable(headers, rowsHtml) {
  return `
    <table>
      <thead>
        <tr>${headers.map((h) => `<th>${h}</th>`).join("")}</tr>
      </thead>
      <tbody>
        ${rowsHtml || `<tr><td colspan="${headers.length}">No records</td></tr>`}
      </tbody>
    </table>
  `;
}

function setRoleVisibility() {
  const role = state.user?.role;
  els.brokerSections.classList.toggle("hidden", role !== "BROKER");
  els.rmSections.classList.toggle("hidden", role !== "RM");
  els.srmSections.classList.toggle("hidden", role !== "SRM");

  if (role === "BROKER") {
    state.cityFilter = state.user.city || "";
    els.cityFilter.value = state.cityFilter;
    els.cityFilter.setAttribute("disabled", "disabled");
    const citySelect = document.getElementById("p-city");
    if (citySelect && state.user.city) {
      citySelect.value = state.user.city;
    }
    const slotCity = document.getElementById("slot-city");
    if (slotCity && state.user.city) {
      slotCity.value = state.user.city;
    }
  } else {
    els.cityFilter.removeAttribute("disabled");
  }
}

function renderMetrics(metrics) {
  const keys = Object.keys(metrics || {});
  if (!keys.length) {
    els.metricsGrid.innerHTML = "<p class='muted'>No metrics.</p>";
    return;
  }

  els.metricsGrid.innerHTML = keys
    .map(
      (key) => `
      <article class="metric-card">
        <p>${key.replaceAll("_", " ")}</p>
        <strong>${metrics[key]}</strong>
      </article>
    `,
    )
    .join("");
}

function renderFlags(flags) {
  if (!flags.length) {
    els.flagStrip.innerHTML = '<span class="flag-chip">No active flags</span>';
    return;
  }

  els.flagStrip.innerHTML = flags
    .map((f) => `<span class="flag-chip">Flag ${f.level} - decays ${formatDate(f.decays_at)}</span>`)
    .join("");
}

function renderInventory(items) {
  if (!items.length) {
    els.inventoryList.innerHTML = "<p class='muted'>No properties yet.</p>";
    return;
  }

  els.inventoryList.innerHTML = items
    .map((item) => {
      const hideChip = item.hidden_from_customers ? `<span class="tag">Hidden from customers</span>` : "";
      const removeBtn = `<button class="minus-btn" data-remove-property="${item.id}" title="Remove property">-</button>`;
      return `
        <article class="property-card">
          ${removeBtn}
          <h3>${item.title}</h3>
          <p>${item.asset_type} | ${item.configuration || "-"}</p>
          <p>${item.location_text}, ${item.city}</p>
          <p>INR ${Number(item.price || 0).toLocaleString("en-IN")}</p>
          <div class="tag-row">
            <span class="tag">${item.status}</span>
            ${hideChip}
            ${item.duplicate_score ? `<span class="tag">Similarity ${item.duplicate_score}%</span>` : ""}
          </div>
        </article>
      `;
    })
    .join("");
}

function renderSlots(items) {
  const rows = items
    .map((slot) => {
      const canCancel = state.user.role === "BROKER" && ["open", "booked"].includes(slot.status);
      const actions = canCancel
        ? `<button class="ghost" data-cancel-slot="${slot.id}">Cancel Slot</button>`
        : "-";
      return `
        <tr>
          <td>${slot.id}</td>
          <td>${slot.city}</td>
          <td>${formatDate(slot.start_at)}</td>
          <td>${formatDate(slot.end_at)}</td>
          <td>${statusPill(slot.status)}</td>
          <td>${actions}</td>
        </tr>
      `;
    })
    .join("");

  els.slotsList.innerHTML = buildTable(["ID", "City", "Start", "End", "Status", "Actions"], rows);
}

function renderVisits(items) {
  const rows = items
    .map((visit) => {
      let actions = "-";
      if (state.user.role === "BROKER" && visit.status === "scheduled") {
        actions = `
          <div class="actions">
            <button class="ghost" data-send-otp="${visit.id}">Send OTP</button>
            <button class="primary" data-complete-visit="${visit.id}">Complete</button>
          </div>
        `;
      }
      return `
        <tr>
          <td>${visit.id}</td>
          <td>${visit.property_title || visit.property_id}</td>
          <td>${visit.customer_name || "-"}<br /><small>${visit.phone_norm || "-"}</small></td>
          <td>${formatDate(visit.start_at)}</td>
          <td>${statusPill(visit.status)}</td>
          <td>${visit.status === "completed" ? (visit.is_unique_visit ? "Unique" : "Non-unique") : "-"}</td>
          <td>${actions}</td>
        </tr>
      `;
    })
    .join("");

  els.visitsList.innerHTML = buildTable(
    ["ID", "Property", "Customer", "Start", "Status", "Unique?", "Actions"],
    rows,
  );
}

function renderDuplicateQueue(items) {
  const rows = items
    .map(
      (q) => `
      <tr>
        <td>${q.id}</td>
        <td>${q.property_title} (#${q.property_id})</td>
        <td>${q.matched_property_title} (#${q.matched_property_id})</td>
        <td>${q.similarity}%</td>
        <td>${q.city}</td>
        <td>${statusPill(q.status)}</td>
        <td>
          <div class="actions">
            <button class="primary" data-dup-decision="approve_visible" data-dup-id="${q.id}">Approve</button>
            <button class="ghost" data-dup-decision="keep_backup" data-dup-id="${q.id}">Keep Backup</button>
            <button class="danger" data-dup-decision="mark_duplicate" data-dup-id="${q.id}">Reject</button>
          </div>
        </td>
      </tr>
    `,
    )
    .join("");
  els.duplicateQueue.innerHTML = buildTable(
    ["Queue", "Property", "Matched", "Similarity", "City", "Status", "Actions"],
    rows,
  );
}

function renderEmergencyQueue(items) {
  const rows = items
    .map(
      (q) => `
      <tr>
        <td>${q.id}</td>
        <td>${q.broker_id}</td>
        <td>${formatDate(q.raised_at)}</td>
        <td>${q.emergency_reason || "-"}</td>
        <td>${q.emergency_details || "-"}</td>
        <td>${formatDate(q.sla_due_at)}</td>
        <td>${statusPill(q.status)}</td>
        <td>
          <div class="actions">
            <button class="primary" data-review-incident="approve" data-incident-id="${q.id}">Approve</button>
            <button class="danger" data-review-incident="reject" data-incident-id="${q.id}">Reject</button>
          </div>
        </td>
      </tr>
    `,
    )
    .join("");
  els.emergencyQueue.innerHTML = buildTable(
    ["ID", "Broker", "Raised", "Reason", "Details", "SLA", "Status", "Actions"],
    rows,
  );
}

function renderEscalations(items) {
  const rows = items
    .map(
      (q) => `
      <tr>
        <td>${q.id}</td>
        <td>${q.broker_id}</td>
        <td>${formatDate(q.raised_at)}</td>
        <td>${formatDate(q.srm_due_at)}</td>
        <td>${statusPill(q.status)}</td>
        <td>
          <div class="actions">
            <button class="primary" data-srm-review="approve" data-incident-id="${q.id}">Approve Emergency</button>
            <button class="danger" data-srm-review="reject" data-incident-id="${q.id}">Reject Emergency</button>
          </div>
        </td>
      </tr>
    `,
    )
    .join("");

  els.escalationQueue.innerHTML = buildTable(["ID", "Broker", "Raised", "SRM SLA", "Status", "Actions"], rows);
}

function renderLeads(items) {
  const rows = items
    .slice(0, 30)
    .map(
      (lead) => `
      <tr>
        <td>${lead.id}</td>
        <td>${lead.customer_name || "-"}<br /><small>${lead.phone_norm || "-"}</small></td>
        <td>${lead.city || "-"}</td>
        <td>${lead.location_pref || "-"}</td>
        <td>${lead.config_pref || "-"}</td>
        <td>${Number(lead.budget_min || 0).toLocaleString("en-IN")} - ${Number(lead.budget_max || 0).toLocaleString("en-IN")}</td>
      </tr>
    `,
    )
    .join("");

  els.leadsList.innerHTML = buildTable(["Lead", "Customer", "City", "Location", "Config", "Budget"], rows);
}

function renderVisitReport(items) {
  const rows = items
    .map(
      (row) => `
      <tr>
        <td>${row.broker_name}</td>
        <td>${row.unique_visits || 0}</td>
        <td>${row.non_unique_visits || 0}</td>
        <td>${row.total_completed || 0}</td>
      </tr>
    `,
    )
    .join("");
  els.visitReport.innerHTML = buildTable(["Broker", "Unique", "Non-unique", "Total Completed"], rows);
}

function renderWhatsAppMessages(items) {
  const rows = items
    .map(
      (m) => `
      <tr>
        <td>${m.id}</td>
        <td>${m.direction}</td>
        <td>${m.template_name || "-"}</td>
        <td>${m.to_phone || m.from_phone || "-"}</td>
        <td>${m.status}</td>
        <td>${m.related_visit_id || "-"}</td>
        <td>${formatDate(m.created_at)}</td>
      </tr>
    `,
    )
    .join("");
  els.waMessages.innerHTML = buildTable(
    ["ID", "Direction", "Template", "Phone", "Status", "Visit", "Time"],
    rows,
  );
}

function renderFunnelReport(report) {
  const keys = Object.keys(report || {});
  if (!keys.length) {
    els.funnelReport.innerHTML = "<p class='muted'>No funnel report.</p>";
    return;
  }
  els.funnelReport.innerHTML = keys
    .map(
      (key) => `
      <article class="metric-card">
        <p>${key.replaceAll("_", " ")}</p>
        <strong>${report[key]}</strong>
      </article>
    `,
    )
    .join("");
}

function renderBrokerReliability(items) {
  const rows = items
    .map(
      (item) => `
      <tr>
        <td>${item.broker_name}</td>
        <td>${item.city || "-"}</td>
        <td>${item.total_visits}</td>
        <td>${item.completed_visits}</td>
        <td>${item.completion_rate_pct}%</td>
        <td>${item.broker_cancelled_visits}</td>
        <td>${item.late_cancel_incidents}</td>
        <td>${item.active_flags}</td>
      </tr>
    `,
    )
    .join("");
  els.brokerReliability.innerHTML = buildTable(
    ["Broker", "City", "Total", "Completed", "Completion %", "Broker Cancels", "<24h Incidents", "Active Flags"],
    rows,
  );
}

function renderCustomerVisits(items) {
  state.cached.customerVisits = items || [];
  const rows = (items || [])
    .map((visit) => {
      const slotOptions = (visit.available_slots || [])
        .map(
          (slot) =>
            `<option value="${slot.slot_id}">${slot.start_at} (${slot.mode}, broker #${slot.broker_id})</option>`,
        )
        .join("");
      return `
      <tr>
        <td>${visit.id}</td>
        <td>${visit.property_title || "-"}</td>
        <td>${visit.location_text || "-"}</td>
        <td>${visit.broker_name || "-"}</td>
        <td>${formatDate(visit.start_at)}</td>
        <td>
          <select data-customer-slot-select="${visit.id}">
            <option value="">Select new slot</option>
            ${slotOptions}
          </select>
        </td>
        <td>
          <div class="actions">
            <button class="danger" data-customer-cancel="${visit.id}">Cancel</button>
            <button class="primary" data-customer-reschedule="${visit.id}">Reschedule</button>
          </div>
        </td>
      </tr>
    `;
    })
    .join("");
  els.customerVisits.innerHTML = buildTable(
    ["Visit ID", "Property", "Location", "Broker", "Current Slot", "New Slot", "Actions"],
    rows,
  );
}

async function loadDashboard() {
  const { metrics } = await api("/api/dashboard");
  renderMetrics(metrics);
}

async function loadInventory() {
  const q = new URLSearchParams({ include_hidden: "true" });
  if (state.cityFilter) q.set("city", state.cityFilter);
  const { items } = await api(`/api/inventory?${q.toString()}`);
  state.cached.inventory = items;
  if (state.user.role === "BROKER") {
    renderInventory(items);
  }
}

async function loadSlots() {
  const q = new URLSearchParams();
  if (state.cityFilter) q.set("city", state.cityFilter);
  const { items } = await api(`/api/slots?${q.toString()}`);
  state.cached.slots = items;
  if (state.user.role === "BROKER") {
    renderSlots(items);
  }
}

async function loadVisits() {
  const { items } = await api("/api/visits");
  state.cached.visits = items;
  if (state.user.role === "BROKER") {
    renderVisits(items);
  }
}

async function loadFlags() {
  const { items } = await api("/api/flags");
  const active = items.filter((f) => f.status === "active");
  renderFlags(active);
}

async function loadRmData() {
  const [dup, emergency, leads, waMessages, funnel, brokerRel] = await Promise.all([
    api("/api/rm/duplicate-queue"),
    api("/api/rm/emergency-queue"),
    api("/api/leads"),
    api("/api/integrations/whatsapp/messages"),
    api("/api/reports/funnel"),
    api("/api/reports/broker-reliability"),
  ]);
  renderDuplicateQueue(dup.items || []);
  renderEmergencyQueue(emergency.items || []);
  renderLeads(leads.items || []);
  renderWhatsAppMessages(waMessages.items || []);
  renderFunnelReport(funnel.report || {});
  renderBrokerReliability(brokerRel.items || []);
}

async function loadSrmData() {
  const { items } = await api("/api/srm/escalations");
  renderEscalations(items || []);
}

async function loadVisitReport() {
  const { items } = await api("/api/reports/visit-counts");
  renderVisitReport(items || []);
}

async function loadCustomerVisits(phone) {
  const q = new URLSearchParams({ phone });
  const { items } = await api(`/api/customer/visits?${q.toString()}`);
  renderCustomerVisits(items || []);
}

async function refreshAll() {
  if (!state.user) return;
  try {
    await loadDashboard();
    await loadVisitReport();

    if (state.user.role === "BROKER") {
      await Promise.all([loadInventory(), loadSlots(), loadVisits(), loadFlags()]);
    }
    if (state.user.role === "RM") {
      await Promise.all([loadRmData()]);
    }
    if (state.user.role === "SRM") {
      await Promise.all([loadSrmData()]);
    }
    if (state.customerPhone) {
      await loadCustomerVisits(state.customerPhone);
    }
  } catch (err) {
    showToast(err.message, "error");
  }
}

async function login(email, password) {
  const payload = await api("/api/auth/login", {
    method: "POST",
    body: { email, password },
  });
  state.token = payload.token;
  localStorage.setItem("proptech_token", state.token);
  state.user = payload.user;
  els.authPill.textContent = `${state.user.name} (${state.user.role})`;
  els.loginPanel.classList.add("hidden");
  els.mainPanel.classList.remove("hidden");
  setRoleVisibility();
  await refreshAll();
}

async function restoreSession() {
  if (!state.token) return;
  try {
    const payload = await api("/api/auth/me");
    state.user = payload.user;
    els.authPill.textContent = `${state.user.name} (${state.user.role})`;
    els.loginPanel.classList.add("hidden");
    els.mainPanel.classList.remove("hidden");
    setRoleVisibility();
    await refreshAll();
  } catch {
    localStorage.removeItem("proptech_token");
    state.token = "";
    state.user = null;
  }
}

function resetAuth() {
  state.token = "";
  state.user = null;
  localStorage.removeItem("proptech_token");
  els.authPill.textContent = "Not logged in";
  els.mainPanel.classList.add("hidden");
  els.loginPanel.classList.remove("hidden");
}

function handleInventoryActions(event) {
  const removeId = event.target.getAttribute("data-remove-property");
  if (removeId) {
    els.removePropertyId.value = removeId;
    els.removeReason.value = "Property already sold";
    els.removeDetails.value = "";
    els.removeDialog.showModal();
  }
}

function handleSlotActions(event) {
  const slotId = event.target.getAttribute("data-cancel-slot");
  if (slotId) {
    els.cancelSlotId.value = slotId;
    els.cancelReason.value = "";
    els.cancelEmergency.checked = false;
    els.cancelEmergencyReason.value = "";
    els.cancelEmergencyDetails.value = "";
    els.cancelDialog.showModal();
  }
}

function handleVisitActions(event) {
  const sendOtpId = event.target.getAttribute("data-send-otp");
  const completeVisitId = event.target.getAttribute("data-complete-visit");

  if (sendOtpId) {
    api("/api/visits/send-otp", {
      method: "POST",
      body: { visit_id: Number(sendOtpId) },
    })
      .then((res) => showToast(`OTP sent (Demo OTP: ${res.demo_otp})`, "ok"))
      .catch((err) => showToast(err.message, "error"));
  }

  if (completeVisitId) {
    state.geo = null;
    state.photoBase64 = "";
    els.completeVisitId.value = completeVisitId;
    els.completeOtp.value = "";
    els.completePhoto.value = "";
    els.geoStatus.textContent = "";
    els.completeDialog.showModal();
  }
}

function handleRmQueueActions(event) {
  const queueId = event.target.getAttribute("data-dup-id");
  const decision = event.target.getAttribute("data-dup-decision");
  if (queueId && decision) {
    const notes = prompt("Notes for audit trail (optional):", "") || "";
    api("/api/rm/duplicate-review", {
      method: "POST",
      body: { queue_id: Number(queueId), decision, notes },
    })
      .then(() => {
        showToast("Duplicate decision saved", "ok");
        refreshAll();
      })
      .catch((err) => showToast(err.message, "error"));
  }

  const incidentId = event.target.getAttribute("data-incident-id");
  const incidentDecision = event.target.getAttribute("data-review-incident");
  if (incidentId && incidentDecision) {
    const approve = incidentDecision === "approve";
    const note = prompt("RM note:", "") || "";
    api("/api/rm/emergency-review", {
      method: "POST",
      body: { incident_id: Number(incidentId), approve, note },
    })
      .then((res) => {
        if (res.flag) {
          showToast(`Flag level ${res.flag.level} applied`, "warn");
        } else {
          showToast("Emergency review submitted", "ok");
        }
        refreshAll();
      })
      .catch((err) => showToast(err.message, "error"));
  }
}

function handleSrmActions(event) {
  const incidentId = event.target.getAttribute("data-incident-id");
  const decision = event.target.getAttribute("data-srm-review");
  if (incidentId && decision) {
    const approve = decision === "approve";
    const note = prompt("SRM note:", "") || "";
    api("/api/srm/escalation-review", {
      method: "POST",
      body: { incident_id: Number(incidentId), approve, note },
    })
      .then((res) => {
        if (res.flag) {
          showToast(`Flag level ${res.flag.level} applied`, "warn");
        } else {
          showToast("Escalation resolved", "ok");
        }
        refreshAll();
      })
      .catch((err) => showToast(err.message, "error"));
  }
}

async function handleExportClick(event, exportType) {
  event.preventDefault();
  try {
    await downloadCsv(exportType);
    showToast(`${exportType} export downloaded`, "ok");
  } catch (err) {
    showToast(err.message, "error");
  }
}

async function handleCustomerActions(event) {
  const cancelVisitId = event.target.getAttribute("data-customer-cancel");
  if (cancelVisitId) {
    const reason = prompt("Reason for cancellation (optional):", "customer_requested") || "customer_requested";
    try {
      await api("/api/customer/visits/cancel", {
        method: "POST",
        body: {
          visit_id: Number(cancelVisitId),
          customer_phone: state.customerPhone,
          reason,
        },
      });
      showToast("Visit cancelled", "warn");
      await loadCustomerVisits(state.customerPhone);
    } catch (err) {
      showToast(err.message, "error");
    }
    return;
  }

  const rescheduleVisitId = event.target.getAttribute("data-customer-reschedule");
  if (rescheduleVisitId) {
    const select = document.querySelector(`[data-customer-slot-select='${rescheduleVisitId}']`);
    const slotId = Number(select?.value || 0);
    if (!slotId) {
      showToast("Select a new slot first", "warn");
      return;
    }
    const reason = prompt("Reason for reschedule (optional):", "customer_requested") || "customer_requested";
    try {
      await api("/api/customer/visits/reschedule", {
        method: "POST",
        body: {
          visit_id: Number(rescheduleVisitId),
          target_slot_id: slotId,
          customer_phone: state.customerPhone,
          reason,
        },
      });
      showToast("Visit rescheduled", "ok");
      await loadCustomerVisits(state.customerPhone);
    } catch (err) {
      showToast(err.message, "error");
    }
  }
}

function bindEvents() {
  els.loginForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    try {
      await login(els.email.value, els.password.value);
      showToast("Logged in", "ok");
    } catch (err) {
      showToast(err.message, "error");
    }
  });

  els.logoutBtn.addEventListener("click", () => {
    resetAuth();
    showToast("Logged out", "ok");
  });

  els.refreshBtn.addEventListener("click", () => refreshAll());

  els.cityFilter.addEventListener("change", () => {
    state.cityFilter = els.cityFilter.value;
    refreshAll();
  });

  els.inventoryForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    const body = {
      title: document.getElementById("p-title").value,
      asset_type: document.getElementById("p-asset-type").value,
      configuration: document.getElementById("p-configuration").value,
      location_text: document.getElementById("p-location").value,
      city: document.getElementById("p-city").value,
      price: Number(document.getElementById("p-price").value),
      latitude: document.getElementById("p-latitude").value || null,
      longitude: document.getElementById("p-longitude").value || null,
      maps_url: document.getElementById("p-maps-url").value,
      amenities: document.getElementById("p-amenities").value,
      image_url: document.getElementById("p-image-url").value,
    };

    try {
      const res = await api("/api/inventory", { method: "POST", body });
      e.target.reset();
      if (state.user.city) {
        document.getElementById("p-city").value = state.user.city;
      }
      if (res.duplicate_check?.matched) {
        showToast(`Similarity ${res.duplicate_check.score}% - sent to RM review`, "warn");
      } else {
        showToast("Property added", "ok");
      }
      refreshAll();
    } catch (err) {
      showToast(err.message, "error");
    }
  });

  els.slotForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    try {
      await api("/api/slots/add", {
        method: "POST",
        body: {
          city: document.getElementById("slot-city").value,
          start_at: document.getElementById("slot-start").value,
          end_at: document.getElementById("slot-end").value,
        },
      });
      showToast("Slot added", "ok");
      e.target.reset();
      if (state.user.city) {
        document.getElementById("slot-city").value = state.user.city;
      }
      refreshAll();
    } catch (err) {
      showToast(err.message, "error");
    }
  });

  els.removeCancel.addEventListener("click", () => els.removeDialog.close());
  els.removeForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    try {
      await api("/api/inventory/remove", {
        method: "POST",
        body: {
          property_id: Number(els.removePropertyId.value),
          reason: els.removeReason.value,
          details: els.removeDetails.value,
        },
      });
      els.removeDialog.close();
      showToast("Property removed", "ok");
      refreshAll();
    } catch (err) {
      showToast(err.message, "error");
    }
  });

  els.cancelClose.addEventListener("click", () => els.cancelDialog.close());
  els.cancelForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    try {
      await api("/api/slots/cancel", {
        method: "POST",
        body: {
          slot_id: Number(els.cancelSlotId.value),
          reason: els.cancelReason.value,
          emergency_requested: els.cancelEmergency.checked,
          emergency_reason: els.cancelEmergencyReason.value,
          emergency_details: els.cancelEmergencyDetails.value,
        },
      });
      els.cancelDialog.close();
      showToast("Slot cancelled", "warn");
      refreshAll();
    } catch (err) {
      showToast(err.message, "error");
    }
  });

  els.completeClose.addEventListener("click", () => els.completeDialog.close());
  els.fetchGeoBtn.addEventListener("click", () => {
    if (!navigator.geolocation) {
      els.geoStatus.textContent = "Geo not supported";
      return;
    }
    els.geoStatus.textContent = "Fetching...";
    navigator.geolocation.getCurrentPosition(
      (pos) => {
        state.geo = {
          lat: pos.coords.latitude,
          lng: pos.coords.longitude,
        };
        els.geoStatus.textContent = `${state.geo.lat.toFixed(5)}, ${state.geo.lng.toFixed(5)}`;
      },
      () => {
        state.geo = null;
        els.geoStatus.textContent = "Permission denied or unavailable";
      },
      { enableHighAccuracy: true, timeout: 9000 },
    );
  });

  els.completePhoto.addEventListener("change", async () => {
    const file = els.completePhoto.files?.[0];
    if (!file) {
      state.photoBase64 = "";
      return;
    }
    try {
      state.photoBase64 = await toBase64(file);
    } catch {
      state.photoBase64 = "";
      showToast("Could not read photo file", "error");
    }
  });

  els.completeForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    try {
      const body = {
        visit_id: Number(els.completeVisitId.value),
        otp: els.completeOtp.value.trim(),
        lat: state.geo?.lat ?? null,
        lng: state.geo?.lng ?? null,
        photo_base64: state.photoBase64 || "",
      };
      const res = await api("/api/visits/complete", { method: "POST", body });
      els.completeDialog.close();
      showToast(
        `Visit completed (${res.unique_visit ? "Unique" : "Non-unique"}, ${res.completion_mode})`,
        "ok",
      );
      refreshAll();
    } catch (err) {
      showToast(err.message, "error");
    }
  });

  els.inventoryList.addEventListener("click", handleInventoryActions);
  els.slotsList.addEventListener("click", handleSlotActions);
  els.visitsList.addEventListener("click", handleVisitActions);
  els.duplicateQueue.addEventListener("click", handleRmQueueActions);
  els.emergencyQueue.addEventListener("click", handleRmQueueActions);
  els.escalationQueue.addEventListener("click", handleSrmActions);

  els.importLeadsBtn?.addEventListener("click", async () => {
    try {
      const res = await api("/api/leads/import-now", { method: "POST" });
      showToast(`Leads sync: +${res.result.imported} new, ${res.result.updated} updated`, "ok");
      refreshAll();
    } catch (err) {
      showToast(err.message, "error");
    }
  });

  els.bookVisitForm?.addEventListener("submit", async (e) => {
    e.preventDefault();
    try {
      await api("/api/visits/book", {
        method: "POST",
        body: {
          property_id: Number(document.getElementById("book-property-id").value),
          slot_id: Number(document.getElementById("book-slot-id").value),
          customer_name: document.getElementById("book-customer-name").value,
          customer_phone: document.getElementById("book-customer-phone").value,
          customer_requirements: document.getElementById("book-customer-req").value,
        },
      });
      showToast("Visit booked", "ok");
      e.target.reset();
      refreshAll();
    } catch (err) {
      showToast(err.message, "error");
    }
  });

  els.calcTourDurationBtn?.addEventListener("click", async () => {
    try {
      const count = Number(els.tourPropertyCount.value || 1);
      const res = await api(`/api/scheduling/duration?property_count=${count}`);
      els.tourDurationOutput.textContent = `${res.total_duration_minutes} mins required`;
    } catch (err) {
      showToast(err.message, "error");
    }
  });

  els.waSendForm?.addEventListener("submit", async (e) => {
    e.preventDefault();
    let context = {};
    const rawContext = (els.waContext?.value || "").trim();
    if (rawContext) {
      try {
        const parsed = JSON.parse(rawContext);
        if (parsed && typeof parsed === "object") context = parsed;
      } catch {
        showToast("Context JSON is invalid", "error");
        return;
      }
    }

    try {
      await api("/api/integrations/whatsapp/send-test", {
        method: "POST",
        body: {
          to_phone: els.waPhone?.value,
          template_name: els.waTemplate?.value,
          related_visit_id: Number(els.waVisitId?.value || 0) || null,
          context,
        },
      });
      showToast("WhatsApp template queued", "ok");
      e.target.reset();
      await refreshAll();
    } catch (err) {
      showToast(err.message, "error");
    }
  });

  els.customerLoadForm?.addEventListener("submit", async (e) => {
    e.preventDefault();
    const phone = (els.customerPhone?.value || "").trim();
    if (!phone) {
      showToast("Enter customer phone", "warn");
      return;
    }
    try {
      state.customerPhone = phone;
      await loadCustomerVisits(phone);
      showToast("Customer visits loaded", "ok");
    } catch (err) {
      showToast(err.message, "error");
    }
  });

  els.customerVisits?.addEventListener("click", (event) => {
    handleCustomerActions(event);
  });

  els.exportVisitCounts?.addEventListener("click", (e) => handleExportClick(e, "visit_counts"));
  els.exportFunnel?.addEventListener("click", (e) => handleExportClick(e, "funnel"));
  els.exportBrokerRel?.addEventListener("click", (e) => handleExportClick(e, "broker_reliability"));
  els.exportWa?.addEventListener("click", (e) => handleExportClick(e, "whatsapp_messages"));
  els.exportVisits?.addEventListener("click", (e) => handleExportClick(e, "visits"));
}

bindEvents();
restoreSession();
