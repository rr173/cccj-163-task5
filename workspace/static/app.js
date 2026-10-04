/* HD-map console: vanilla JS, no build step. */
"use strict";

const $ = (s, r = document) => r.querySelector(s);
const $$ = (s, r = document) => [...r.querySelectorAll(s)];

const state = {
  status: null, candidates: [], releases: [], events: [],
  patches: [], snapshots: [], view: null, rollouts: [],
  viewMode: "published", viewRef: null,
  selectedCandidate: null, selectedSnapshot: null,
  fullSnapshot: null,
  audit: { imports: [], selected: null, state: null, branch: null,
           options: null, map: null, querying: false, mode: "state" },
};

async function api(path, opts = {}) {
  const res = await fetch("/api" + path, {
    method: opts.method || "GET",
    headers: opts.body ? { "Content-Type": "application/json" } : undefined,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw Object.assign(new Error(data?.error?.message || res.statusText),
    { code: data?.error?.code, details: data?.error?.details });
  return data;
}

let toastTimer;
function toast(msg, kind = "") {
  const el = $("#toast");
  el.textContent = msg;
  el.className = "toast show " + kind;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => el.className = "toast", 3800);
}
function showError(e) {
  console.error(e);
  let msg = e.message;
  if (e.code === "VALIDATION_FAILED" && e.details?.errors) {
    msg = `校验失败 ${e.details.errors.length} 项（见右侧报告）：` +
          e.details.errors.slice(0, 2).map(x => x.message).join("；");
  }
  toast("✗ " + msg, "error");
}
const esc = s => String(s ?? "").replace(/[&<>"]/g, c =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

// ------------------------------------------------------------- loading -----
async function refreshAll() {
  await Promise.all([
    loadStatus(), loadCandidates(), loadReleases(),
    loadEvents(), loadPatches(), loadSnapshots(), loadRollouts(),
  ]);
  await refreshView();
}
async function loadStatus() {
  state.status = await api("/status");
  $("#currentVersion").textContent = state.status.current_version;
  $("#activePatches").textContent =
    state.status.active_patches.length
      ? "活动补丁: " + state.status.active_patches.join(", ")
      : "无活动补丁";
  $("#regionTable").innerHTML =
    `<tr><th>区域</th><th>当前代际</th><th>发布版本</th><th>来源候选</th><th>瓦片</th><th>发布时间</th></tr>` +
    state.status.regions.map(r => `
      <tr><td>${esc(r.region)}</td><td class="gen">gen ${r.generation}</td>
      <td>${esc(r.release_id)}</td><td>${esc(r.candidate_id)}</td>
      <td>${r.tiles}</td><td class="meta">${esc(r.published_at.replace("T", " ").replace("+00:00", "Z"))}</td></tr>`
    ).join("");
}
async function loadCandidates() {
  ({ candidates: state.candidates } = await api("/candidates"));
  renderCandidates();
  populateVehicles();
}
async function loadReleases() {
  ({ releases: state.releases } = await api("/releases"));
  $("#releaseList").innerHTML = state.releases.slice().reverse().map(r => `
    <div class="tl-item">
      <span class="tl-gen">${Object.entries(r.generations).map(([k, v]) =>
        `${esc(k)}#${v}`).join("<br>")}</span>
      <div><b>${esc(r.release_id)}</b> ← ${esc(r.candidate_id)}
        <div class="tl-meta">${esc(r.note || "")}</div>
        <div class="tl-meta">${esc(r.created_at.replace("T", " ").replace("+00:00", "Z"))}</div>
      </div></div>`).join("") || `<div class="hint">暂无发布</div>`;
}
async function loadEvents() {
  ({ events: state.events } = await api("/events?limit=80"));
  $("#eventLog").innerHTML = state.events.map(e => `
    <div class="ev"><span class="eid">#${e.event_id}</span>
      <span class="ts">${esc(e.ts.replace("T", " ").replace("+00:00", "Z"))}</span>
      <span class="typ">${esc(e.type)}</span><span>${esc(e.summary)}</span></div>`).join("");
}
async function loadPatches() {
  ({ patches: state.patches } = await api("/patches"));
  renderPatches();
}
async function loadSnapshots() {
  ({ snapshots: state.snapshots } = await api("/snapshots"));
  renderSnapshots();
  populateVehicles();
  populateRouteNodes();
}

async function refreshView() {
  const params = new URLSearchParams({ mode: state.viewMode });
  if (state.viewRef) params.set("ref", state.viewRef);
  try {
    state.view = await api("/view?" + params);
  } catch (e) { state.view = null; }
  renderMap();
  renderValidation();
  if (state.viewMode === "snapshot" && state.selectedSnapshot) {
    try { await loadFullSnapshot(state.selectedSnapshot, { keepMap: true }); } catch {}
  }
  populatePatchTargets();
  populateRouteNodes();
}

// ------------------------------------------------------------ candidates ---
function badge(status) {
  const map = { draft: "b-draft", submitted: "b-submitted", published: "b-published",
    rejected: "b-rejected", active: "b-active", proposed: "b-proposed",
    revoked: "b-revoked", completed: "b-completed", superseded: "b-superseded",
    promoted: "b-published", rolled_back: "b-revoked" };
  return `<span class="badge ${map[status] || "b-draft"}">${esc(status)}</span>`;
}
function renderCandidates() {
  const el = $("#candidateList");
  el.innerHTML = state.candidates.map(c => {
    const n = c.validation?.errors?.length || 0;
    const v = c.status === "rejected" ? `<span class="err-code">${n} 个错误</span>`
      : c.validation?.status === "passed" ? `<span style="color:var(--green)">校验通过</span>`
      : `<span class="hint">未校验</span>`;
    const sel = state.selectedCandidate === c.candidate_id ? "selected" : "";
    return `<div class="card ${sel}" data-cid="${c.candidate_id}">
      <div class="row1"><span class="id">${esc(c.candidate_id)}</span>
        ${badge(c.status)} ${v}</div>
      <div class="note">${esc(c.note || "")}</div>
      <div class="meta">区域 ${c.regions.map(esc).join(",")} ·
        ${Object.keys(c.tiles).length} 瓦片 · 基线代 ${
          Object.entries(c.based_on_generations || {}).map(([k, v]) => `${k}#${v}`).join(",")
        } · ${esc(c.created_at.replace("T", " ").slice(5, 16))}</div>
      <div class="actions">
        <button class="ok" data-act="validate">校验接缝/走廊</button>
        <button class="pub" data-act="publish"
          ${c.status === "published" ? "disabled" : ""}>按区域代发布</button>
      </div></div>`;
  }).join("") || `<div class="hint">无候选</div>`;

  $$("#candidateList .card").forEach(card => {
    card.addEventListener("click", e => {
      if (e.target.tagName === "BUTTON") return;
      const cid = card.dataset.cid;
      state.selectedCandidate = state.selectedCandidate === cid ? null : cid;
      if (state.selectedCandidate) { state.viewMode = "candidate"; state.viewRef = cid; }
      else { state.viewMode = "published"; state.viewRef = null; }
      refreshView(); renderCandidates();
    });
  });
  $$("#candidateList button").forEach(btn => btn.addEventListener("click", async e => {
    e.stopPropagation();
    const card = btn.closest(".card");
    const cid = card.dataset.cid, act = btn.dataset.act;
    try {
      if (act === "validate") {
        const v = await api(`/candidates/${cid}/validate`, { method: "POST" });
        toast(v.status === "passed"
          ? `✓ ${cid} 接缝与跨区走廊全部通过` : `✗ ${cid} ${v.errors.length} 项校验失败`,
          v.status === "passed" ? "ok" : "error");
      } else {
        const r = await api(`/candidates/${cid}/publish`, { method: "POST" });
        toast(`✓ 已发布 ${r.release_id}：` +
          Object.entries(r.generations).map(([k, v]) => `${k}→第${v}代`).join("，"), "ok");
      }
      await refreshAll();
    } catch (err) {
      await refreshAll();
      showError(err);
    }
  }));
}

// -------------------------------------------------------------- map --------
function drawMap(svg, v) {
  if (!v) { svg.innerHTML = ""; return; }
  const P = id => v.nodes.find(n => n.id === id);
  const labelsOn = $("#toggleLabels").checked;
  const corridorsOn = $("#toggleCorridors").checked;

  let html = "";
  v.tiles.forEach((t, i) => {
    const [x1, y1, x2, y2] = t.bounds;
    html += `<rect class="tile-rect-${i % 2 ? "b" : "a"}" x="${x1}" y="${y1}"
      width="${x2 - x1}" height="${y2 - y1}" rx="10"/>
      <text class="tile-label" x="${x1 + 14}" y="${y1 + 30}">
      ${esc(t.tile_id)} · ${esc(t.region)} · ${esc(t.version_tag)}</text>`;
  });
  if (corridorsOn) v.corridors.forEach(cor => {
    if (!cor.ok) return;
    cor.segments.forEach(seg => {
      let cur = seg.start;
      seg.steps.forEach(st => {
        const a = P(cur), b = P(st.kind === "seam"
          ? `${st.to_tile}/${st.to_node}` : `${st.to_tile}/${st.to_node}`);
        if (a && b) html += `<line class="lane corridor" x1="${a.x}" y1="${a.y}"
          x2="${b.x}" y2="${b.y}"/>`;
        cur = b.id;
      });
    });
  });
  v.lanes.forEach(l => {
    let cls = "lane";
    if (l.closed) cls += " closed";
    html += `<line class="${cls}" x1="${l.x1}" y1="${l.y1}" x2="${l.x2}" y2="${l.y2}">
      <title>${esc(l.id)} speed=${l.speed}${l.closed ? " CLOSED by " + l.closed_by : ""}</title></line>`;
    if (l.closed && labelsOn)
      html += `<text class="closed-tag" x="${(l.x1 + l.x2) / 2 + 8}"
        y="${(l.y1 + l.y2) / 2 - 6}">🚧${esc(l.closed_by || "")}</text>`;
  });
  if (v.route) {
    const routeNodes = [v.route.steps.length
      ? `${v.route.steps[0].from_tile}/${v.route.steps[0].from_node}` : v.route.from_node];
    v.route.steps.forEach(st => routeNodes.push(`${st.to_tile}/${st.to_node}`));
    routeNodes.forEach((id, i) => {
      if (!i) return;
      const a = P(routeNodes[i - 1]), b = P(id);
      if (a && b) html += `<line class="lane route" x1="${a.x}" y1="${a.y}"
        x2="${b.x}" y2="${b.y}"/>`;
    });
  }
  v.seams.forEach(s => {
    html += `<circle class="seam ${s.ok ? "ok" : "bad"}" cx="${s.x}" cy="${s.y}" r="11"/>`;
    if (!s.ok) html += `<text class="closed-tag" x="${s.x + 14}" y="${s.y + 5}">接缝异常</text>`;
  });
  v.nodes.forEach(n => {
    let cls = "node" + (n.external ? " ext" : "");
    if (v.route && (n.id === v.route.from_node || n.id === v.route.to_node)) cls += "route-pt";
    html += `<circle class="${cls}" cx="${n.x}" cy="${n.y}" r="${n.external ? 6 : 7}"/>`;
    if (labelsOn) html += `<text class="nlabel" x="${n.x + 9}" y="${n.y - 9}">${
      esc(n.node)}</text>`;
  });
  svg.innerHTML = html;
}

function renderMap() {
  const svg = $("#map");
  if (!state.view) { svg.innerHTML = ""; $("#viewLabel").textContent = "（无视图数据）"; return; }
  $("#viewLabel").textContent = "· " + state.view.version_label;
  drawMap(svg, state.view);
}

// --------------------------------------------------------- validation ------
function renderValidation() {
  const el = $("#validationReport");
  const v = state.view;
  if (!v) { el.innerHTML = ""; return; }
  const msgs = [];
  if (v.mode === "candidate") {
    if (v.errors.length) {
      msgs.push(`<div class="err-item"><b>候选 ${esc(v.ref)} 不可发布</b>
        （发布将被原子拒绝，不产生任何新版本）</div>`);
      v.errors.forEach(e => msgs.push(`<div class="err-item">
        <span class="err-code">[${esc(e.code)}]</span>
        <span class="where"> @${esc(e.where)}</span><br>${esc(e.message)}
        ${e.segment ? `<div class="hint">断点: ${esc(e.segment[0])} → ${esc(e.segment[1])}</div>` : ""}
      </div>`));
    } else {
      msgs.push(`<div class="ok-item">✓ 候选 ${esc(v.ref)} 所有瓦片边界接缝一致
        （坐标/限速/车道数/相互声明），${v.corridors.filter(c => c.ok).length} 条跨区走廊均可贯通。</div>`);
    }
  } else {
    const okSeams = v.seams.filter(s => s.ok).length;
    msgs.push(`<div class="ok-item">当前视图 ${esc(v.version_label)}：${okSeams}/${
      v.seams.length} 条接缝匹配；${v.corridors.filter(c => c.ok).length}/${
      v.corridors.length} 条走廊贯通。</div>`);
  }
  v.corridors.forEach(c => {
    if (!c.ok) msgs.push(`<div class="err-item"><span class="err-code">[ROUTE_BROKEN]</span>
      跨区走廊 ${esc(c.corridor_id)}「${esc(c.name)}」在当前工作集无法贯通</div>`);
    else msgs.push(`<div class="note-item">走廊 ${esc(c.corridor_id)}「${esc(c.name)}」
      跨瓦片路径有效（绿线）</div>`);
  });
  el.innerHTML = msgs.join("");
}

// ------------------------------------------------------------- patches -----
function populatePatchTargets() {
  const sel = $("#patchTile");
  if (!state.view || !state.view.tiles.length) return;
  const cur = sel.value;
  sel.innerHTML = state.view.tiles.map(t =>
    `<option value="${esc(t.tile_id)}">${esc(t.tile_id)}（${esc(t.region)}）</option>`).join("");
  if (cur) sel.value = cur;
  renderLaneChecks();
}
function renderLaneChecks() {
  const tid = $("#patchTile").value;
  const lanes = state.view ? state.view.lanes.filter(l => l.tile === tid) : [];
  $("#patchLanes").innerHTML = lanes.map(l => `
    <label><input type="checkbox" value="${esc(l.lane)}" ${l.closed ? "disabled" : ""}/>
      ${esc(l.lane)} ${esc(l.from.split("/")[1])}↔${esc(l.to.split("/")[1])}
      ${l.closed ? `（已被 ${esc(l.closed_by)} 封闭）` : ""}</label>`).join("");
}
function renderPatches() {
  $("#patchList").innerHTML = state.patches.map(p => {
    const scope = Object.entries(p.scope.tiles).map(([t, ls]) =>
      ls.map(l => `${t}/${l}`).join(", ")).join("；");
    const compat = p.compatibility?.status === "compatible"
      ? `<span style="color:var(--green)">兼容·有绕行</span>`
      : `<span class="err-code">${esc(p.compatibility?.status || "未检查")}</span>`;
    return `<div class="card">
      <div class="row1"><span class="id">${esc(p.patch_id)} <span class="hint">p${
        p.priority}</span></span>${badge(p.status)} ${compat}</div>
      <div class="note">🚧 ${esc(scope)} — ${esc(p.reason)}</div>
      <div class="meta">固化基础: ${esc(p.base_version.base ?
        Object.values(p.base_version.base).map(b => `gen${b.generation}`).join("/") : "")}
        · ${esc(p.created_at.replace("T", " ").slice(5, 16))}
        ${p.revoked_at ? "· 已于 " + esc(p.revoked_at.replace("T", " ").slice(5, 16)) + " 撤销" : ""}
      </div>
      <div class="actions">
        ${p.status === "proposed"
          ? `<button class="ok" data-act="activate">② 生效（再次核对基础哈希）</button>` : ""}
        ${p.status === "active"
          ? `<button class="danger" data-act="revoke">撤销封路</button>` : ""}
      </div></div>`;
  }).join("") || `<div class="hint">暂无补丁</div>`;

  $$("#patchList button").forEach(btn => btn.addEventListener("click", async () => {
    const pid = btn.closest(".card").querySelector(".id").textContent.split(" ")[0];
    try {
      if (btn.dataset.act === "activate") {
        await api(`/patches/${pid}/activate`, { method: "POST" });
        toast(`✓ ${pid} 已生效；在途车辆将在节点处收到过渡建议，绝不中途拼接拓扑`, "ok");
      } else {
        await api(`/patches/${pid}/revoke`, { method: "POST", body: { reason: "管制解除" } });
        toast(`✓ ${pid} 已撤销，新路线恢复通行；历史快照仍按其固定版本可复算`, "ok");
      }
      await refreshAll();
      if (state.fullSnapshot) await loadFullSnapshot(state.selectedSnapshot, { keepMap: true });
    } catch (e) { await refreshAll(); showError(e); }
  }));
}

// ------------------------------------------------------------- rollouts ----
async function loadRollouts() {
  ({ rollouts: state.rollouts } = await api("/rollouts"));
  renderRollouts();
  populateRolloutCandidates();
}

function populateRolloutCandidates() {
  const sel = $("#rolloutCandidate");
  const cur = sel.value;
  const ok = state.candidates.filter(c =>
    c.validation?.status === "passed" && c.status !== "published");
  sel.innerHTML = ok.map(c =>
    `<option value="${esc(c.candidate_id)}">${esc(c.candidate_id)}（${
      c.regions.map(esc).join(",")}）</option>`).join("")
    || `<option value="">（无校验通过的候选）</option>`;
  if (cur && ok.some(c => c.candidate_id === cur)) sel.value = cur;
  const c = ok.find(x => x.candidate_id === sel.value);
  if (c && !$("#rolloutRegions").value) $("#rolloutRegions").value = c.regions.join(",");
}

function renderRollouts() {
  const el = $("#rolloutList");
  el.innerHTML = state.rollouts.map(b => {
    const w = b.window || {};
    const statRow = (g, label) => {
      const st = b.stats[g];
      return `<tr><td>${label}</td><td class="gen">${st.requests}</td>
        <td class="${st.unreachable ? "err-code" : "gen"}">${st.unreachable}</td>
        <td class="${st.transition_rejected ? "err-code" : "gen"}">${st.transition_rejected}</td></tr>`;
    };
    const decision = b.decision ? `<div class="meta">判定：<b>${esc(b.decision.action)}</b>
      ${b.decision.release_id ? "→ " + esc(b.decision.release_id) : ""}
      ${b.decision.reason ? "· " + esc(b.decision.reason) : ""}
      · ${esc((b.decision.at || "").replace("T", " ").replace("+00:00", "Z"))}</div>` : "";
    const breaches = (b.breaches || []).map(x =>
      `<div class="err-item" style="margin-top:6px"><span class="err-code">[THRESHOLD]</span> ${esc(x)}</div>`
    ).join("");
    return `<div class="card" data-rid="${esc(b.rollout_id)}">
      <div class="row1"><span class="id">${esc(b.rollout_id)}</span>
        ${badge(b.status)}
        ${b.status === "active" && !b.window_open ? `<span class="badge b-proposed">观察窗口已结束</span>` : ""}
      </div>
      <div class="note">候选 ${esc(b.candidate_id)} · 区域 ${b.regions.map(esc).join(",")}
        · 名单 ${b.vehicles.map(esc).join(", ") || "—"}</div>
      <div class="meta">窗口 ${esc((w.start || "").replace("T", " ").slice(5, 16))} →
        ${w.end ? esc(w.end.replace("T", " ").slice(5, 16)) : "不限"}
        ${b.trial_version_preview ? ` · 待晋级版本 <span class="gen">${esc(b.trial_version_preview)}</span>` : ""}</div>
      <table class="data" style="margin-top:6px">
        <tr><th>分组</th><th>请求量</th><th>不可达</th><th>安全过渡拒绝</th></tr>
        ${statRow("trial", "🧪 试运行组（待晋级内容）")}
        ${statRow("control", "对照组（当前内容）")}
      </table>
      ${breaches}${decision}
      <div class="actions">
        ${b.status === "active" ? `
          <button class="ok" data-act="promote" ${b.promotable ? "" : "disabled"}
            title="${b.promotable ? "按当前统计一次性晋级（原子发布全部作用区域）" : "阈值越界，禁止晋级"}">⬆ 一次性晋级</button>
          <button class="danger" data-act="rollback">⤺ 撤回批次</button>` : ""}
      </div></div>`;
  }).join("") || `<div class="hint">暂无试运行批次</div>`;

  $$("#rolloutList button").forEach(btn => btn.addEventListener("click", async () => {
    const rid = btn.closest(".card").dataset.rid;
    try {
      if (btn.dataset.act === "promote") {
        const r = await api(`/rollouts/${rid}/promote`, { method: "POST", body: {} });
        toast(`✓ ${rid} 已晋级为 ${r.decision.release_id}` +
          (r.idempotent ? "（重复操作，返回首次结果）" : ""), "ok");
      } else {
        const r = await api(`/rollouts/${rid}/rollback`,
          { method: "POST", body: { reason: "运维撤回" } });
        toast(`✓ ${rid} 已撤回；已发出快照与区域代际保持不变` +
          (r.idempotent ? "（重复操作，返回首次结果）" : ""), "ok");
      }
      await refreshAll();
    } catch (e) { await refreshAll(); showError(e); }
  }));
}

$("#createRolloutBtn").addEventListener("click", async () => {
  const cid = $("#rolloutCandidate").value;
  if (!cid) return toast("没有可试运行的候选（请先在地图审查页校验通过）", "error");
  const body = {
    candidate_id: cid,
    regions: $("#rolloutRegions").value.split(",").map(x => x.trim()).filter(Boolean),
    vehicles: $("#rolloutVehicles").value.split(",").map(x => x.trim()).filter(Boolean),
  };
  const mins = Number($("#rolloutWindow").value);
  if (mins > 0) body.window = { end: new Date(Date.now() + mins * 60000).toISOString() };
  const th = {};
  if ($("#rolloutMaxUnreach").value !== "") th.max_unreachable = Number($("#rolloutMaxUnreach").value);
  if ($("#rolloutMaxReject").value !== "") th.max_transition_rejected = Number($("#rolloutMaxReject").value);
  if (Object.keys(th).length) body.thresholds = th;
  try {
    const b = await api("/rollouts", { method: "POST", body });
    $("#rolloutCreateResult").innerHTML = `<div class="ok-item">✓ 批次 <b>${esc(b.rollout_id)}</b>
      已创建：名单内车辆的新行程将使用待晋级内容（${esc(b.trial_version_preview || "")}），
      名单外继续读取当前内容。</div>`;
    toast(`✓ 试运行批次 ${b.rollout_id} 已创建`, "ok");
    await refreshAll();
  } catch (e) {
    $("#rolloutCreateResult").innerHTML = `<div class="err-item"><span class="err-code">[${
      esc(e.code || "ERROR")}]</span> ${esc(e.message)}</div>`;
    showError(e);
  }
});

$("#rolloutCandidate").addEventListener("change", () => {
  const c = state.candidates.find(x => x.candidate_id === $("#rolloutCandidate").value);
  if (c) $("#rolloutRegions").value = c.regions.join(",");
});

// -------------------------------------------------------------- routes -----
function populateVehicles() {
  const sel = $("#vehicleSelect");
  const ids = new Set(["VH-01", ...state.snapshots.map(s => s.vehicle_id)]);
  const cur = sel.value || "VH-01";
  sel.innerHTML = [...ids].map(id => `<option>${esc(id)}</option>`).join("");
  sel.value = cur;
}
async function populateRouteNodes() {
  if (!state.view) return;
  const f = $("#fromNode"), t = $("#toNode");
  const nodes = state.view.nodes.filter(n => n.external);
  const opts = nodes.map(n => `<option value="${esc(n.id)}">${esc(n.id)}</option>`).join("");
  if (!f.options.length) {
    f.innerHTML = opts; t.innerHTML = opts;
    f.value = "A00/w"; t.value = "B01/e";
  }
}
function renderSnapshots() {
  const el = $("#snapshotList");
  el.innerHTML = state.snapshots.map(s => {
    const sel = state.selectedSnapshot === s.snapshot_id ? "selected" : "";
    return `<div class="card ${sel}" data-sid="${esc(s.snapshot_id)}">
      <div class="row1"><span class="id">${esc(s.snapshot_id)}</span>${badge(s.status)}
        ${(s.rollouts || []).map(r => `<span class="badge ${r.group === "trial" ? "b-active" : "b-superseded"}">🧪${esc(r.rollout_id)}·${r.group === "trial" ? "试运行" : "对照"}</span>`).join("")}</div>
      <div class="note">${esc(s.vehicle_id)} · ${esc(s.from_node)} → ${esc(s.to_node)}</div>
      <div class="meta">${esc(s.version)}</div>
      <div class="meta">${s.steps} 步 · ${s.tiles.map(t => t.tile_id).join(",")}
        ${s.parent_snapshot_id ? "· 由 " + esc(s.parent_snapshot_id) + " 过渡而来" : ""}
        ${s.children.length ? "· 已过渡到 " + s.children.map(esc).join(",") : ""}</div>
    </div>`;
  }).join("") || `<div class="hint">尚无快照——先请求一条路线</div>`;
  $$("#snapshotList .card").forEach(c => c.addEventListener("click", async () => {
    state.selectedSnapshot = c.dataset.sid;
    state.viewMode = "snapshot"; state.viewRef = state.selectedSnapshot;
    renderSnapshots();
    await refreshView();
    switchTab("routes");
  }));
}

async function loadFullSnapshot(sid, opts = {}) {
  state.fullSnapshot = await api(`/snapshots/${sid}`);
  state.selectedSnapshot = sid;
  renderTrip();
}

function renderTrip() {
  const s = state.fullSnapshot;
  const panel = $("#tripPanel");
  if (!s) { panel.innerHTML = `<span class="hint">选择或生成一个快照</span>`; return; }
  const stepDone = i => {
    const cur = s.progress.step;
    if (i < cur) return "done";
    return i === cur ? "now" : "";
  };
  panel.innerHTML = `
    <div><b>${esc(s.snapshot_id)}</b> ${badge(s.status)}
      <span class="hint">${esc(s.version.version)}</span></div>
    <div class="hint">${esc(s.from_node)} → ${esc(s.to_node)} · 进度 step=${
      s.progress.step}/${s.route.length} · ${esc(s.progress.state)}</div>
    <div class="tripcontrols">
      <button class="ghost" id="enterLaneBtn" ${s.status !== "active" ? "disabled" : ""}>
        ⬆ 进入下一车道 (on_lane)</button>
      <button class="ghost" id="reachNodeBtn" ${s.status !== "active" ? "disabled" : ""}>
        ⬇ 到达下一节点 (at_node)</button>
      <button class="primary warn" id="adoptBtn">🧭 在节点处采用绕行快照</button>
      <button class="ghost" id="finishBtn">🏁 完成行程</button>
    </div>
    ${s.route.map((st, i) => {
      const text = st.kind === "seam"
        ? `接缝 ${st.from_tile}:${st.from_node} → ${st.to_tile}:${st.to_node}`
        : `车道 ${st.tile}/${st.lane} ${st.from_node}→${st.to_node}`;
      return `<div class="step ${stepDone(i)}"><span class="bar">${
        i < s.progress.step ? "✓" : i === s.progress.step ? "▶" : ""}</span>${text}</div>`;
    }).join("")}`;

  const post = async path => {
    try { await api(path, { method: "POST" }); await refreshAll(); }
    catch (e) { await refreshAll(); showError(e); }
  };
  $("#enterLaneBtn").onclick = () => s.progress.step < s.route.length &&
    api(`/snapshots/${s.snapshot_id}/progress`, {
      method: "POST", body: { step: s.progress.step, state: "on_lane" }
    }).then(() => refreshAll()).catch(e => { refreshAll().then(() => showError(e)); });
  $("#reachNodeBtn").onclick = () => s.progress.step < s.route.length &&
    api(`/snapshots/${s.snapshot_id}/progress`, {
      method: "POST", body: { step: s.progress.step + 1, state: "at_node" }
    }).then(() => refreshAll()).catch(e => { refreshAll().then(() => showError(e)); });
  $("#adoptBtn").onclick = async () => {
    try {
      const child = await api(`/snapshots/${s.snapshot_id}/adopt-reroute`, { method: "POST" });
      toast(`✓ 已在节点处安全过渡到新快照 ${child.snapshot_id}（版本 ${child.version.version}）`, "ok");
      state.selectedSnapshot = child.snapshot_id;
      state.viewMode = "snapshot"; state.viewRef = child.snapshot_id;
      await refreshAll();
    } catch (e) { showError(e); }
  };
  $("#finishBtn").onclick = () => post(`/snapshots/${s.snapshot_id}/complete`);
  renderAdvisory();
}

function renderAdvisory() {
  const s = state.fullSnapshot;
  const el = $("#advisoryPanel");
  if (!s) { el.innerHTML = ""; return; }
  const adv = s.advisory;
  const items = [];
  adv.notes.forEach(n => items.push(`<div class="adv info"><b>[${esc(n.level)}]</b> ${
    esc(n.message)}</div>`));
  adv.closures_ahead.forEach(c => items.push(`<div class="adv ${
    c.must_finish_current_lane_first ? "danger" : "warn"}">
    <b>🚨 ${esc(c.patch_id)}</b> 封路预警：${esc(c.message)}<br>
    <span class="hint">${c.must_finish_current_lane_first
      ? "车辆正在车道上——必须先完成当前车道，禁止新旧拓扑拼接；到达节点后才允许过渡。"
      : "车辆在节点——可立即采用绕行快照，不发生任何车道中途拼接。"}</span></div>`));
  if (!items.length) items.push(`<div class="adv info">本快照固定版本与当前线上版本一致，
    无封路预警。即使区域发布了新一代地图，本快照也不会被改写。</div>`);
  el.innerHTML = items.join("");
}

// =============================================================== mirror ====
const MIRROR_STATUS_CN = { pulling: "拉取中", ready: "可切换", active: "已切换",
  failed: "已失败", rejected: "已拒绝" };
const MIRROR_BADGE = { pulling: "b-active", ready: "b-proposed",
  active: "b-published", failed: "b-rejected", rejected: "b-rejected" };

async function loadMirror() {
  const [ms, ts, ds, ev] = await Promise.all([
    api("/mirror/manifests"), api("/mirror/tasks"),
    api("/mirror/devices"), api("/mirror/events?limit=40")]);
  state.mirror = { manifests: ms.manifests, tasks: ts.tasks,
                   devices: ds.devices, events: ev.events };
  renderManifests(); renderDevices(); renderMirrorTasks(); renderMirrorEvents();
  populateMirrorSource();
}

function populateMirrorSource() {
  const sel = $("#mirrorSource");
  const cur = sel.value;
  const opts = [`<option value="effective">当前生效版本（${esc(
    state.status?.current_version || "…")}）</option>`];
  state.snapshots.forEach(s => opts.push(
    `<option value="snapshot:${esc(s.snapshot_id)}">行程快照 ${esc(s.snapshot_id)}`
    + `（${esc(s.vehicle_id)} · ${esc(s.version)}）</option>`));
  sel.innerHTML = opts.join("");
  if (cur) sel.value = cur;
}

function renderManifests() {
  $("#manifestList").innerHTML = state.mirror.manifests.map(m => `
    <div class="card">
      <div class="row1"><span class="id">${esc(m.manifest_id)}</span>
        <span class="badge b-proposed">不可变</span></div>
      <div class="note">${esc(m.source.version)} · ${m.objects} 对象 / ${m.chunks} 压缩块</div>
      <div class="meta">根散列 ${esc(m.root_hash.replace("sha256:", "").slice(0, 20))}…
        · 设备 ${m.devices.map(esc).join(",")}</div>
      <div class="meta">来源 ${esc(m.source.kind)}${m.source.ref ? "/" + esc(m.source.ref) : ""}
        · ${esc(m.note || "无备注")} · ${esc(m.created_at.replace("T", " ").slice(5, 19))}</div>
    </div>`).join("") || `<div class="hint">暂无清单</div>`;
}

function mirrorTaskCard(t, withActions) {
  const p = t.progress;
  const pct = p.total ? Math.round(100 * p.confirmed / p.total) : 0;
  const barCls = t.status === "active" || t.status === "ready" ? "done"
    : t.status === "failed" || t.status === "rejected" ? "bad" : "";
  const rej = t.rejection ? `<div class="err-item" style="margin-top:6px">
    <span class="err-code">[${esc(t.rejection.code)}]</span> ${esc(t.rejection.message)}</div>` : "";
  const res = t.result && t.result.outcome === "switched" ? `<div class="meta">
    切换结果：${esc(t.result.switched_at)} 原子换指针 → ${esc(t.result.new_image.version)}
    ${t.result.previous_image ? "（替换 " + esc(t.result.previous_image.version) + "）" : "（首个镜像）"}</div>` : "";
  const journal = (t.journal || []).slice().reverse().map(e => `
    <div class="jl"><span class="jt">${esc(e.ts.replace("T", " ").slice(5, 19))}</span>
      <span class="je">${esc(e.event)}</span><span>${esc(e.detail)}</span></div>`).join("");
  return `<div class="card" data-tid="${esc(t.task_id)}">
    <div class="row1"><span class="id">${esc(t.task_id)}</span>
      <span class="badge ${MIRROR_BADGE[t.status] || "b-draft"}">${
        esc(MIRROR_STATUS_CN[t.status] || t.status)}</span></div>
    <div class="note">${esc(t.device_id)} ← ${esc(t.manifest_id)} · ${esc(t.version)}</div>
    <div class="pbar ${barCls}"><i style="width:${pct}%"></i></div>
    <div class="meta">已确认 ${p.confirmed}/${p.total} 块（幂等，不重复计数）
      ${t.staged ? "· 根散列 " + esc(t.staged.root_hash.replace("sha256:", "").slice(0, 16)) + "… 已暂存" : ""}</div>
    ${rej}${res}
    ${withActions ? `<div class="actions">
      ${t.status === "pulling" ? `
        <button class="ok" data-act="agent">▶ 模拟拉取（续拉）</button>
        <button class="pub" data-act="agent-drop">⛔ 拉 1 块后断网</button>
        <button class="danger" data-act="agent-corrupt">🧨 模拟传输损坏</button>` : ""}
      ${t.status === "ready" ? `<button class="ok" data-act="switch">⇄ 原子换指针</button>` : ""}
    </div>` : ""}
    ${journal ? `<details style="margin-top:6px"><summary class="hint">处理记录（${t.journal.length}）</summary>
      <div class="journal">${journal}</div></details>` : ""}
  </div>`;
}

function renderDevices() {
  $("#deviceList").innerHTML = state.mirror.devices.map(d => {
    const cur = d.current_image;
    const img = cur ? `<div class="mirror-img">当前镜像 <span class="v">${esc(cur.version)}</span>
        <div class="meta">${esc(cur.manifest_id)} · 根散列 ${esc(cur.root_hash.replace("sha256:", "").slice(0, 16))}…
        · ${esc(cur.switched_at.replace("T", " ").slice(5, 19))} 切换</div></div>`
      : `<div class="mirror-none">当前镜像：（空，尚未切换过）</div>`;
    const pend = d.pending_task
      ? `<div class="hint" style="margin:4px 0">待切换镜像（同一盒子只允许一个）：</div>`
        + mirrorTaskCard(d.pending_task, true)
      : `<div class="mirror-none">无待切换镜像</div>`;
    return `<div class="card" style="cursor:default">
      <div class="row1"><span class="id">🖥 ${esc(d.device_id)}</span></div>
      ${img}${pend}
    </div>`;
  }).join("") || `<div class="hint">暂无盒子——先发布一份清单</div>`;

  $$("#deviceList [data-act]").forEach(btn => btn.addEventListener("click", async () => {
    const tid = btn.closest("[data-tid]").dataset.tid;
    const act = btn.dataset.act;
    try {
      if (act === "switch") {
        const r = await api(`/mirror/tasks/${tid}/switch`, { method: "POST", body: {} });
        toast(r.switched
          ? `✓ ${r.device_id} 已原子切换至 ${r.result.new_image.version}` +
            (r.idempotent ? "（重复操作，返回首次切换结果）" : "")
          : `✗ 切换未完成：${r.rejection?.message || ""}`, r.switched ? "ok" : "error");
      } else {
        const body = act === "agent-drop" ? { stop_after: 1 }
          : act === "agent-corrupt" ? { corrupt_chunk: 1 } : {};
        const r = await api(`/mirror/tasks/${tid}/agent`, { method: "POST", body });
        const bad = r.results.find(x => x.rejection);
        toast(bad ? `✗ [${bad.rejection.code}] ${bad.rejection.message}`
          : r.task.status === "ready" ? "✓ 全部对象验真，镜像已暂存、可切换"
          : `已确认 ${r.task.progress.confirmed}/${r.task.progress.total} 块`,
          bad ? "error" : "ok");
      }
      await loadMirror();
    } catch (e) { await loadMirror(); showError(e); }
  }));
}

function renderMirrorTasks() {
  $("#mirrorTaskList").innerHTML =
    state.mirror.tasks.map(t => mirrorTaskCard(t, false)).join("")
    || `<div class="hint">暂无分发任务</div>`;
}

function renderMirrorEvents() {
  $("#mirrorEvents").innerHTML = state.mirror.events.map(e => `
    <div class="ev"><span class="eid">#${e.event_id}</span>
      <span class="ts">${esc(e.ts.replace("T", " ").replace("+00:00", "Z"))}</span>
      <span class="typ">${esc(e.type)}</span><span>${esc(e.summary)}</span></div>`).join("")
    || `<div class="hint">暂无事件</div>`;
}

$("#mirrorPublishBtn").addEventListener("click", async () => {
  const src = $("#mirrorSource").value;
  const body = {
    devices: $("#mirrorDevices").value.split(",").map(x => x.trim()).filter(Boolean),
    note: $("#mirrorNote").value,
  };
  if (src.startsWith("snapshot:")) body.source = { kind: "snapshot", snapshot_id: src.slice(9) };
  try {
    const r = await api("/mirror/manifests", { method: "POST", body });
    const m = r.manifest;
    $("#mirrorPublishResult").innerHTML = `<div class="ok-item">✓ 清单 <b>${esc(m.manifest_id)}</b>
      （${esc(m.source.version)}，${m.objects.length} 对象 / ${m.chunks.length} 压缩块，
      根散列 ${esc(m.root_hash.replace("sha256:", "").slice(0, 20))}…）
      ${r.idempotent ? "<br>重复提交：返回首次清单与首次任务。" : ""}</div>`
      + r.tasks.map(t => t.rejection
        ? `<div class="err-item"><span class="err-code">[${esc(t.rejection.code)}]</span>
           ${esc(t.task_id)} → ${esc(t.device_id)}：${esc(t.rejection.message)}</div>`
        : `<div class="note-item">${esc(t.task_id)} → ${esc(t.device_id)}：已分发，
           待拉取 ${t.progress.total} 块</div>`).join("");
    toast(`✓ 清单 ${m.manifest_id} 已生成` + (r.idempotent ? "（幂等返回首次结果）" : ""), "ok");
    await loadMirror();
  } catch (e) {
    $("#mirrorPublishResult").innerHTML = `<div class="err-item"><span class="err-code">[${
      esc(e.code || "ERROR")}]</span> ${esc(e.message)}</div>`;
    showError(e);
  }
});

// =============================================================== audit =====
async function loadAuditImports() {
  const data = await api("/audit/imports");
  state.audit.imports = data.imports;
  const sel = $("#auditSel");
  const cur = sel.value;
  sel.innerHTML = state.audit.imports.map(i =>
    `<option value="${esc(i.import_key)}">${esc(i.import_key)} · ${esc(i.status)}` +
    ` · ${i.event_count} 事件</option>`).join("")
    || `<option value="">（无已导入证据链）</option>`;
  if (cur && state.audit.imports.some(i => i.import_key === cur)) sel.value = cur;
  renderAuditImportList();
  if (state.audit.selected) renderAuditDetail();
}

async function loadAuditArchives() {
  const { archives } = await api("/audit/archives?limit=40");
  $("#auditArchives").innerHTML = archives.map(a => `
    <div class="ar"><span class="aid">${esc(a.archive_id)}</span>
      <span class="k">${esc(a.kind)}${a.ref ? " → " + esc(a.ref) : ""}</span>
      <span>${esc(a.summary)}</span>
      <span class="ts">${esc(a.created_at.replace("T", " ").replace("+00:00", "Z"))}</span>
    </div>`).join("") || `<div class="hint">尚无审计档案</div>`;
}

async function loadAuditOptions() {
  const [rel, veh, snaps] = await Promise.all([
    api("/releases"), api("/status"), api("/snapshots")]);
  state.releases = rel.releases;
  $("#auditReleaseSel").innerHTML = rel.releases.slice().reverse().map(r =>
    `<option value="${esc(r.release_id)}">${esc(r.release_id)}（${
      Object.entries(r.generations).map(([k, v]) => `${k}#${v}`).join("/")}）</option>`
  ).join("");
  const vids = [...new Set(snaps.snapshots.map(s => s.vehicle_id).concat(["VH-01"]))];
  $("#auditGenVehicle").innerHTML = vids.map(v => `<option>${esc(v)}</option>`).join("");
  if (state.audit.selected) renderAuditBranches();
}

function renderAuditImportList() {
  $("#auditImportList").innerHTML = state.audit.imports.map(i => {
    const sel = state.audit.selected === i.import_key ? "selected" : "";
    const cps = (i.confirmed_checkpoints || []).map(c =>
      `<span class="cp-chip">cp@${c}</span>`).join("");
    const rej = i.status === "rejected"
      ? `<div class="err-item" style="margin-top:6px"><span class="err-code">[${
          esc(i.rejection.code)}]</span> @seq ${i.rejection.at_seq}<br>${
          esc(i.rejection.message)}</div>` : "";
    return `<div class="card ${sel}" data-key="${esc(i.import_key)}">
      <div class="row1"><span class="id">${esc(i.import_key)}</span>
        <span class="badge ${i.status === "accepted" ? "b-submitted" : "b-rejected"}">${
          esc(i.status)}</span></div>
      <div class="note">${i.event_count || 0} 事件 · seq ${i.first_seq || "–"}..${
          i.last_seq || "–"} · 车辆 ${esc(i.vehicle_id || "–")}</div>
      <div class="meta">${(i.types ? Object.entries(i.types).map(([k, v]) =>
        `${k}×${v}`).join(" ") : "")}</div>
      <div style="margin-top:5px">${cps}</div>${rej}
    </div>`;
  }).join("") || `<div class="hint">尚未导入证据链</div>`;

  $$("#auditImportList .card").forEach(c => c.addEventListener("click", async () => {
    state.audit.selected = c.dataset.key;
    $("#auditSel").value = state.audit.selected;
    renderAuditImportList();
    await renderAuditDetail();
    await renderAuditBranches();
  }));
}

async function renderAuditDetail() {
  const key = state.audit.selected;
  if (!key) { $("#auditImportDetail").innerHTML = ""; return; }
  const d = await api(`/audit/imports/${encodeURIComponent(key)}`);
  $("#auditImportDetail").innerHTML = `
    <div class="note-item">链摘要（头部事件）<br><b class="v">${esc(d.head.digest)}</b></div>
    <div class="hint">来源：${esc((d.sources || []).join(", "))} ·
      最近更新 ${esc((d.updated_at || "").replace("T", " ").replace("+00:00", "Z"))}</div>
    <div style="margin-top:6px">${d.event_digests.map(e => `
      <div class="ev" style="font-size:11px"><span class="eid">#${e.seq}</span>
        <span class="typ" style="min-width:120px">${esc(e.type)}</span>
        <span class="hint">${esc(e.source)}</span>
        <span class="hint">${esc(e.digest.replace("sha256:", "").slice(0, 12))}</span></div>`
    ).join("")}</div>`;
}

async function renderAuditBranches() {
  const key = state.audit.selected || $("#auditSel").value;
  if (!key) { $("#auditBranchList").innerHTML = ""; return; }
  try {
    const { branches } = await api(`/audit/imports/${encodeURIComponent(key)}/branches`);
    $("#auditBranchList").innerHTML = branches.slice().reverse().map(b => `
      <div class="card branch-card">
        <div class="row1"><span class="id">${esc(b.branch_id)}</span>
          <span class="badge b-proposed">${esc(b.scenario.type)}</span></div>
        <div class="note">基于检查点 seq ${b.base_seq}（${
          esc(b.scenario.release_id || b.scenario.patch_id || "")}）</div>
        <div class="dif">由 ${esc(b.created_by)} · ${
          esc(b.created_at.replace("T", " ").replace("+00:00", "Z"))}</div>
      </div>`).join("") || `<div class="hint">尚无隔离分支</div>`;
  } catch { $("#auditBranchList").innerHTML = ""; }
}

function renderAuditState(st) {
  state.audit.state = st;
  const v = st.version;
  const okTile = st.tiles.filter(t => t.content_hash_ok).length;
  const tilesHtml = st.tiles.map(t =>
    `<div class="kv"><span class="k">${t.content_hash_ok ? "✓" : "✗"} ${esc(t.tile_id)}</span>
     <span class="v ${t.content_hash_ok ? "ok" : "bad"}">${esc(t.hash.replace("sha256:", "").slice(0, 22))}</span></div>`
  ).join("");
  const warnHtml = st.warnings.map(w =>
    `<div class="adv danger" style="margin-top:6px"><b>[${esc(w.level)}]</b> ${esc(w.message)}</div>`
  ).join("");
  $("#auditStatePanel").innerHTML = `
    <div class="kv"><span class="k">车辆 / 路线</span>
      <span class="v">${esc(st.vehicle_id)} · <b>${esc(st.pin_id)}</b> (${esc(st.pin_status)})</span></div>
    <div class="kv"><span class="k">重建时刻</span>
      <span class="v">事件 #${st.at.event_seq} · ${esc(st.at.event_ts)}</span></div>
    <div class="kv"><span class="k">版本描述</span>
      <span class="v">${esc(v.version)}</span></div>
    <div class="kv"><span class="k">描述哈希</span>
      <span class="v ${v.descriptor_hash_ok ? "ok" : "bad"}">${
        v.descriptor_hash_ok ? "✓ 复核一致" : "✗ 复核失败（描述被改写）"}</span></div>
    <div class="kv"><span class="k">活动管制</span>
      <span class="v">${(v.patches || []).map(esc).join(", ") || "无"}</span></div>
    <div class="kv"><span class="k">瓦片摘要</span>
      <span class="v ${okTile === st.tiles.length ? "ok" : "bad"}">${okTile}/${st.tiles.length} 与内容寻址库一致 ·
        集合 ${esc(st.tile_summary.digest.replace("sha256:", "").slice(0, 16))}</span></div>
    <div class="kv"><span class="k">空间位置</span>
      <span class="v">${esc(st.position.state)} ${esc(st.position.node ||
        (st.position.lane ? st.position.from + " → " + st.position.to : ""))}</span></div>
    ${tilesHtml}
    <div class="audit-steps">${st.steps.map(s => `
      <div class="st ${s.phase === "done" ? "done" : s.phase.startsWith("current") ? "now" : ""}">
      [${s.index}] ${s.phase === "done" ? "✓" : s.phase === "current_on_lane" ? "▶"
        : s.phase === "current_at_node" ? "◆" : " "}
      ${s.kind === "seam" ? `接缝 ${esc(s.from)} → ${esc(s.to)}`
        : `车道 ${esc(s.tile)}/${esc(s.lane)} ${esc(s.from.split("/")[1])}→${esc(s.to.split("/")[1])} @${s.speed}`}
      </div>`).join("")}</div>
    ${warnHtml}
    <div class="hint" style="margin-top:6px">查询档案 ${esc(st.archive_id)}（只读，仅新增审计档案）</div>`;
  $("#auditViewLabel").textContent = "· " + v.version;
  drawMap($("#auditMap"), st.map);
  $("#auditBranchSeq").value = st.at.event_seq;
}

function renderBranch(cf, bid) {
  const d = cf.diffs;
  const rows = [
    d.generation_changes.map(g => `区域 ${g.region}: gen ${g.from_generation ?? "–"} → ${g.to_generation}`).join("；"),
    `路径变化: ${d.route_changed ? "是" : "否"}（${d.old_steps} 步 → ${d.new_steps} 步，可达 ${d.reachable ? "✓" : "✗"}）`,
    (d.closures_removed || []).length ? `移除管制: ${d.closures_removed.map(c => c.tile_id + "/" + c.lane_id).join(", ")}` : "",
    d.patches_dropped.length ? `悬空补丁: ${d.patches_dropped.join(", ")}` : "",
    d.tiles_changed.length ? `瓦片变化: ${d.tiles_changed.length} 张` : "",
  ].filter(Boolean);
  $("#auditBranchResult").innerHTML = `
    <div class="ok-item">✓ 隔离分支 <b>${esc(bid)}</b> 演算完成（仅写入审计档案，线上无任何变化）<br>
    假设版本：<b>${esc(cf.descriptor.version)}</b></div>
    ${rows.map(r => `<div class="note-item">${esc(r)}</div>`).join("")}`;
  drawMap($("#auditMap"), cf.map);
  $("#auditViewLabel").textContent = "· " + cf.descriptor.version + "（隔离演算）";
}

async function auditSelectedKey() {
  let key = $("#auditSel").value;
  if (!key && state.audit.imports.length) {
    key = state.audit.imports[0].import_key;
    $("#auditSel").value = key;
  }
  return key;
}

$("#auditGenBtn").addEventListener("click", async () => {
  try {
    const d = await api("/audit/vehicle-events", { method: "POST",
      body: { vehicle_id: $("#auditGenVehicle").value } });
    $("#auditImportKey").value = d.import_key;
    $("#auditEvents").value = JSON.stringify(d.events, null, 1);
    toast(`✓ 已基于线上车辆快照链生成 ${d.events.length} 条签名事件（只读，未写档案）`, "ok");
  } catch (e) {
    toast("✗ " + e.message + "（可先在路线复现页跑出快照，或用样例链）", "error");
  }
});

$("#auditGenTemplateBtn").addEventListener("click", async () => {
  try {
    const d = await api("/audit/template", { method: "POST", body: {} });
    $("#auditImportKey").value = d.import_key;
    $("#auditEvents").value = JSON.stringify(d.events, null, 1);
    toast(`✓ 已按当前线上版本 ${d.live_version} 生成 ${d.events.length} 条样例事件`, "ok");
  } catch (e) { showError(e); }
});

$("#auditImportBtn").addEventListener("click", async () => {
  const key = $("#auditImportKey").value.trim();
  let events;
  try { events = JSON.parse($("#auditEvents").value); }
  catch (e) { return toast("事件 JSON 解析失败：" + e.message, "error"); }
  if (!key) return toast("请填写 import_key", "error");
  if (!Array.isArray(events)) events = events.events;
  try {
    const d = await api("/audit/imports", { method: "POST", body: { import_key: key, events } });
    $("#auditImportResult").innerHTML = `<div class="ok-item">✓ 导入判定：<b>${esc(d.status)}</b>`
      + (d.idempotent ? "（同键再次提交，返回首次判定）" : "")
      + `，${d.event_count} 事件，链摘要 ${esc((d.chain_digest || "").replace("sha256:", "").slice(0, 20))}</div>`;
    toast("✓ 证据链导入通过" + (d.idempotent ? "（幂等返回首次判定）" : ""), "ok");
    await refreshAudit();
  } catch (e) {
    if (e.code && e.code.startsWith("AUDIT_")) {
      $("#auditImportResult").innerHTML = `<div class="err-item"><span class="err-code">[${
        esc(e.code)}]</span><br>${esc(e.message)}</div>`;
      toast("✗ 导入被拒绝：" + e.code, "error");
      await loadAuditImports(); await loadAuditArchives();
    } else showError(e);
  }
});

$("#auditAppendBtn").addEventListener("click", async () => {
  const key = $("#auditImportKey").value.trim();
  if (!key) return toast("请填写要续算的 import_key", "error");
  let events;
  try { events = JSON.parse($("#auditEvents").value); }
  catch (e) { return toast("事件 JSON 解析失败：" + e.message, "error"); }
  try {
    const d = await api(`/audit/imports/${encodeURIComponent(key)}/append`,
      { method: "POST", body: { events } });
    if (d.append === "rejected")
      toast("✗ 续算被拒绝：" + d.rejection.code + "，头部检查点保持 " + d.head_seq, "error");
    else toast(`✓ 已从检查点续算至 ${d.head_seq}`, "ok");
    await refreshAudit();
  } catch (e) { showError(e); }
});

$("#auditQueryBtn").addEventListener("click", async () => {
  const key = await auditSelectedKey();
  const seq = Number($("#auditSeq").value) || null;
  if (!key) return toast("请先选择证据链", "error");
  try {
    const st = await api(`/audit/imports/${encodeURIComponent(key)}/query`,
      { method: "POST", body: { seq } });
    renderAuditState(st);
    toast(`✓ 已重建事件 #${st.at.event_seq} 的空间状态`, "ok");
    await loadAuditArchives();
  } catch (e) { showError(e); }
});

$("#auditConfirmBtn").addEventListener("click", async () => {
  const key = await auditSelectedKey();
  const seq = Number($("#auditSeq").value);
  if (!key || !seq) return toast("请选择证据链并输入要确认的序号", "error");
  try {
    await api(`/audit/imports/${encodeURIComponent(key)}/checkpoints`,
      { method: "POST", body: { seq, note: "地图审查页确认" } });
    toast(`✓ 检查点 ${key}@${seq} 已确认，可建立隔离分支`, "ok");
    await refreshAudit();
  } catch (e) { showError(e); }
});

$("#auditSwitchBtn").addEventListener("click", async () => {
  const key = await auditSelectedKey();
  const seq = Number($("#auditBranchSeq").value);
  const release_id = $("#auditReleaseSel").value;
  try {
    const b = await api(`/audit/imports/${encodeURIComponent(key)}/branches`,
      { method: "POST", body: { seq, scenario: { type: "version_switch", release_id } } });
    renderBranch(b.counterfactual, b.branch_id);
    toast(`✓ 隔离分支 ${b.branch_id}：版本切换演算完成`, "ok");
    await renderAuditBranches(); await loadAuditArchives();
  } catch (e) { showError(e); }
});

$("#auditRevokeBtn").addEventListener("click", async () => {
  const key = await auditSelectedKey();
  const seq = Number($("#auditBranchSeq").value);
  const patch_id = $("#auditPatchInput").value.trim();
  if (!patch_id) return toast("请输入要撤销的补丁号", "error");
  try {
    const b = await api(`/audit/imports/${encodeURIComponent(key)}/branches`,
      { method: "POST", body: { seq, scenario: { type: "revoke_closure", patch_id } } });
    renderBranch(b.counterfactual, b.branch_id);
    toast(`✓ 隔离分支 ${b.branch_id}：撤销 ${patch_id} 演算完成`, "ok");
    await renderAuditBranches(); await loadAuditArchives();
  } catch (e) { showError(e); }
});

$("#auditExportBtn").addEventListener("click", async () => {
  const key = await auditSelectedKey();
  try {
    const b = await api("/audit/exports", { method: "POST", body: { import_key: key } });
    $("#auditExportResult").innerHTML = `<div class="ok-item">✓ 审计档案 <b>${
      esc(b.export_id)}</b>：${b.import_verdict.event_count} 事件 · ${
      b.branches.length} 分支<br>bundle 摘要：<span class="hint">${
      esc(b.bundle_digest)}</span></div>`;
    toast(`✓ 已导出 ${b.export_id}（只读档案，线上无变化）`, "ok");
    await loadAuditArchives();
  } catch (e) { showError(e); }
});

async function refreshAudit() {
  await Promise.all([loadAuditImports(), loadAuditArchives(), loadAuditOptions()]);
}

// --------------------------------------------------------------- tabs ------
function switchTab(name) {
  $$(".tab").forEach(t => t.classList.toggle("active", t.dataset.tab === name));
  $$(".tabpane").forEach(p => p.classList.toggle("active", p.id === "tab-" + name));
  if (name === "routes" && state.selectedSnapshot) {
    state.viewMode = "snapshot"; state.viewRef = state.selectedSnapshot;
    refreshView();
  }
  if (name === "review") {
    state.viewMode = state.selectedCandidate ? "candidate" : "published";
    state.viewRef = state.selectedCandidate;
    refreshView();
  }
  if (name === "audit") {
    refreshAudit().catch(showError);
  }
  if (name === "mirror") {
    loadMirror().catch(showError);
  }
}
$$(".tab").forEach(t => t.addEventListener("click", () => switchTab(t.dataset.tab)));

// ------------------------------------------------------------- wiring ------
$("#viewPublished").addEventListener("click", () => {
  state.viewMode = "published"; state.viewRef = null; state.selectedCandidate = null;
  renderCandidates(); refreshView();
});
$("#toggleCorridors").addEventListener("change", renderMap);
$("#toggleLabels").addEventListener("change", renderMap);

$("#patchTile").addEventListener("change", renderLaneChecks);
$("#createPatchBtn").addEventListener("click", async () => {
  const lanes = $$("#patchLanes input:checked").map(i => i.value);
  if (!lanes.length) return toast("请至少选择一条要封闭的车道", "error");
  try {
    const p = await api("/patches", { method: "POST", body: {
      scope: { tiles: { [$("#patchTile").value]: lanes } },
      reason: $("#patchReason").value || "紧急封路",
      priority: Number($("#patchPriority").value || 100),
    }});
    if (p.compatibility.status === "compatible") {
      toast(`✓ ${p.patch_id} 兼容基础版本且存在绕行路径，可在补丁队列中生效`, "ok");
    } else {
      toast(`✗ ${p.patch_id} 无法安全过渡：${p.compatibility.reason}`, "error");
    }
    await refreshAll();
  } catch (e) { showError(e); }
});

$("#planBtn").addEventListener("click", async () => {
  try {
    const snap = await api("/routes", { method: "POST", body: {
      vehicle_id: $("#vehicleSelect").value,
      from_node: $("#fromNode").value, to_node: $("#toNode").value,
    }});
    toast(`✓ 已固定快照 ${snap.snapshot_id}（${snap.version.version}）`, "ok");
    state.selectedSnapshot = snap.snapshot_id;
    state.viewMode = "snapshot"; state.viewRef = snap.snapshot_id;
    await refreshAll();
    switchTab("routes");
  } catch (e) { showError(e); }
});

$("#syncBtn").addEventListener("click", async () => {
  const vid = $("#vehicleSelect").value;
  try {
    const before = await api(`/vehicles/${vid}`);
    const last = before.snapshots.length ? 0 : 0; // demo: replay from 0 (full offline catch-up)
    const data = await api(`/vehicles/${vid}/sync?last_event_id=${last}`);
    $("#syncResult").innerHTML =
      `<div class="note-item">车辆 <b>${esc(vid)}</b> 重连：回放 ${
        data.missed_events.length} 条错过事件；当前单一有效版本为
        <b>${esc(data.current_version.version)}</b>。
        ${data.active_snapshot ? `在途快照 ${esc(data.active_snapshot.snapshot_id)}（${
          esc(data.active_snapshot.status)}）仍固定其自身版本。` : "无在途快照。"}</div>` +
      data.missed_events.slice(-12).reverse().map(e =>
        `<div class="note-item" style="margin-bottom:4px">#${e.event_id}
          <span class="err-code">${esc(e.type)}</span> ${esc(e.summary)}</div>`).join("");
    toast(`✓ 离线追赶完成，应用 ${data.missed_events.length} 条事件`, "ok");
  } catch (e) { showError(e); }
});

$("#refreshBtn").addEventListener("click", () => refreshAll().catch(showError));

$("#auditSel").addEventListener("change", async () => {
  state.audit.selected = $("#auditSel").value;
  renderAuditImportList();
  await renderAuditDetail();
  await renderAuditBranches();
});

refreshAll().catch(showError);
