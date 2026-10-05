"""
Capacity-aware pre-warm orchestration for roadside compute boxes
(STRICTLY ISOLATED — own store data/preheat.json; never reads or writes any
other subsystem).

A dispatcher submits a *preheat job*: a batch of target boxes with per-job
constraints (deadline, per-link rate cap, minimum online floor). The
orchestrator moves content over time ticks subject to three capacity
dimensions:

  * **site shared uplink** — every box at one site shares one uplink; the
    sum of granted bytes/tick across the site can never exceed it;
  * **per-box free space** — a disk reservation and the network grant for a
    replica are acquired and released *as a pair*; no grant without a
    reservation, no reservation without an enqueued replica;
  * **content reuse** — content already present at the site cache is taken
    from cache at a fast LAN rate and never re-billed to the uplink.

Fairness is starvation-free: every online replica first receives a minimum
online floor (subject to the site uplink), and the remainder is distributed by
deficit round robin (each replica keeps a persisted deficit counter), so a
long-running job can never be starved forever by a stream of newcomers.

Robustness:

  * resume after pause / link degradation / box offline / crash only
    re-sends the *remaining* bytes (per-blob receive state is persisted);
  * insufficient space, link degradation, box offline, cancellation or crash
    never leaves a ghost reservation — reservations are *derived* each tick
    from persisted replica state and are all released the moment a replica
    becomes inactive; restart rebuilds the queue, deficits (欠账) and ETA;
  * quota edits / cancel / resubmit are verdict-deterministic and serialized;
  * effective map images and pinned trip snapshots of the other subsystems
    are never touched.

The engine is driven by fixed 1-second ticks of a virtual clock so the web
console, the CLI and the tests all observe the same deterministic schedule;
the HTTP server runs one tick per wall second, the CLI can tick manually.
"""

from __future__ import annotations

import copy
import json
import os
import threading
from typing import Any, Optional

from engine import DomainError, canonical, sha256, short, utcnow_iso

# ----------------------------------------------------------------- tuning ---
TICK_SECONDS = 1                 # one virtual second per tick
QUANTUM = 8 * 1024 * 1024        # DRR quantum (bytes of service credit)
CACHE_FAST = 200 * 1024 * 1024   # site-cache (LAN) drain rate per box, bytes/s
RESUBMIT_PREFIX = "~RE-"         # marks a re-submission superseding a job

# replica statuses
ACTIVE_STATUSES = ("queued", "running", "paused", "blocked",
                   "offline", "complete", "failed", "canceled")
# statuses that still own disk reservation + queue membership
LIVE_STATUSES = ("queued", "running", "blocked", "offline")

# human-readable rate-limiting reasons (网页与命令行共用同一份字典)
REASON_CN = {
    "ELIGIBLE": "可传输",
    "RATE_CAP": "已达本作业链路速率上限",
    "FLOOR_CAP": "仅拿到最小在线保障（站点拥塞，余量不足）",
    "SITE_CONTENTION": "站点共享带宽被在保障作业占满，DRR 排队中",
    "BOX_OFFLINE": "盒子离线，暂停占额，恢复后续传",
    "LINK_DEGRADED": "链路降级，按当前有效速率限速",
    "BOX_SPACE_WAIT": "盒内可用空间不足，等待其他作业释放空间",
    "JOB_PAUSED": "作业已暂停，恢复后只补剩余数据",
    "DEADLINE_MISSED": "已超过截止时间，继续尽力传输",
    "DONE": "已完成",
}


def _mb(n: int) -> float:
    return round(n / (1024 * 1024), 2)


class PreheatEngine:
    # ============================================================ lifecycle
    def __init__(self, path: str, seed: bool = True):
        self.path = path
        self.lock = threading.RLock()
        self.state: dict = {}
        self._load(seed)

    def _blank(self) -> dict:
        return {"clock": 0,                     # virtual seconds elapsed
                "sites": {},                    # site_id -> site record
                "boxes": {},                    # box_id -> box record
                "blobs": {},                    # blob_id -> {size, hash}
                "jobs": {},                     # job_id -> job
                "job_by_submit": {},            # idempotency/submit key -> job_id
                "events": [],
                "seq": {"job": 0, "event": 0}}

    def _load(self, seed: bool):
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as f:
                self.state = json.load(f)
            for k, v in self._blank().items():
                self.state.setdefault(k, v)
        else:
            self.state = self._blank()
            if seed:
                self._seed_world(self.state)
            self._persist(self.state)

    # ------------------------------------------------------------- seed demo
    @staticmethod
    def _seed_world(s: dict):
        """Deterministic demo world: 3 sites, 8 boxes, reusable content."""
        MB = 1024 * 1024
        sites = [
            # site_id, name, shared uplink bytes/s
            ("ST-N", "北枢纽", 60 * MB),
            ("ST-S", "南枢纽", 40 * MB),
            ("ST-W", "西枢纽", 30 * MB),
        ]
        for sid, name, cap in sites:
            s["sites"][sid] = {
                "site_id": sid, "name": name, "uplink_bps": cap,
                "cache": [],                 # blob_ids already at site cache
                "created_at": utcnow_iso()}
        # box_id, site, capacity MB, link MB/s, online
        boxes = [
            ("RSU-N1", "ST-N", 400, 25, True),
            ("RSU-N2", "ST-N", 300, 25, True),
            ("RSU-N3", "ST-N", 250, 15, True),
            ("RSU-S1", "ST-S", 500, 20, True),
            ("RSU-S2", "ST-S", 200, 20, True),
            ("RSU-S3", "ST-S", 180, 12, False),   # starts offline
            ("RSU-W1", "ST-W", 260, 18, True),
            ("RSU-W2", "ST-W", 120, 18, True),    # small disk: space demo
        ]
        for bid, site, cap_mb, link_mb, online in boxes:
            s["boxes"][bid] = {
                "box_id": bid, "site_id": site,
                "capacity_bytes": cap_mb * MB,
                "link_bps": link_mb * MB,
                "effective_link_bps": (link_mb * MB) if online else 0,
                "online": online,
                "stored": [],                 # fully-landed blob_ids
                "partial": {},                # blob_id -> persisted byte offset
                                      # (canceled in-flight bytes kept for resume)
                "created_at": utcnow_iso(),
                "updated_at": utcnow_iso()}
        # content library (sizes chosen so reuse / contention are visible)
        library = [
            ("BL-MAP-BASE", 120 * MB),
            ("BL-MAP-LANE", 70 * MB),
            ("BL-MAP-POI", 30 * MB),
            ("BL-MAP-MEDIA", 160 * MB),
            ("BL-MAP-PATCH", 12 * MB),
        ]
        for bid, size in library:
            s["blobs"][bid] = {"blob_id": bid, "size": size,
                               "hash": sha256({"blob": bid, "size": size})}
        # pre-warm the north site cache with the base map (demonstrates reuse):
        # later north jobs that include BL-MAP-BASE pull from cache, no uplink.
        s["sites"]["ST-N"]["cache"].append("BL-MAP-BASE")
        # and RSU-N1 already physically holds it (disk accounting demo)
        s["boxes"]["RSU-N1"]["stored"].append("BL-MAP-BASE")

    # ========================================================== persistence
    def _persist(self, draft: dict):
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        tmp = f"{self.path}.tmp.{os.getpid()}.{threading.get_ident()}"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(draft, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)

    def commit(self, mutate) -> Any:
        """Serialised all-or-nothing: mutate(draft) -> atomically persist."""
        with self.lock:
            draft = copy.deepcopy(self.state)
            result = mutate(draft)
            self._persist(draft)
            self.state = draft
            return result

    def now(self) -> str:
        return utcnow_iso()

    def _next(self, s: dict, key: str, prefix: str) -> str:
        s["seq"][key] += 1
        return f"{prefix}-{s['seq'][key]:04d}"

    # ============================================================ journal
    def _event(self, s: dict, etype: str, summary: str, ref: str = ""):
        s["seq"]["event"] += 1
        ev = {"event_id": s["seq"]["event"], "tick": s["clock"],
              "ts": self.now(), "type": etype, "summary": summary, "ref": ref}
        s["events"].append(ev)
        if len(s["events"]) > 5000:
            del s["events"][:len(s["events"]) - 5000]
        return ev

    def list_events(self, limit: int = 80) -> list:
        with self.lock:
            return copy.deepcopy(self.state["events"][-limit:][::-1])

    # ============================================================== helpers
    @staticmethod
    def _blob_size(s: dict, blob_id: str) -> int:
        b = s["blobs"].get(blob_id)
        if not b:
            raise DomainError("PREHEAT_BLOB_UNKNOWN",
                              f"内容对象 {blob_id} 不存在于内容库", 404)
        return b["size"]

    def _replica(self, s: dict, job_id: str, box_id: str) -> dict:
        return next(r for r in s["jobs"][job_id]["replicas"]
                    if r["box_id"] == box_id)

    # ----------------------------------------------------------- admin: blobs
    def register_blob(self, body: dict) -> dict:
        blob_id = (body.get("blob_id") or "").strip()
        size = body.get("size")
        if not blob_id:
            raise DomainError("BAD_REQUEST", "blob_id 必填", 400)
        if not isinstance(size, int) or size <= 0:
            raise DomainError("BAD_REQUEST", "size 必须是正整数（字节）", 400)

        def m(s):
            if blob_id in s["blobs"]:
                old = s["blobs"][blob_id]
                if old["size"] != size:
                    raise DomainError(
                        "PREHEAT_BLOB_CONFLICT",
                        f"内容对象 {blob_id} 已存在且大小为 {old['size']}，"
                        f"与本次声明 {size} 不符（对象不可变）", 409)
                return {"blob": copy.deepcopy(old), "idempotent": True}
            blob = {"blob_id": blob_id, "size": size,
                    "hash": sha256({"blob": blob_id, "size": size})}
            s["blobs"][blob_id] = blob
            self._event(s, "BLOB_REGISTERED",
                        f"内容对象 {blob_id} 入库（{_mb(size)} MiB）", blob_id)
            return {"blob": copy.deepcopy(blob), "idempotent": False}

        return self.commit(m)

    # --------------------------------------------------------- admin: sites
    def upsert_site(self, body: dict) -> dict:
        sid = (body.get("site_id") or "").strip()
        name = body.get("name", sid)
        cap = body.get("uplink_bps")
        if not sid:
            raise DomainError("BAD_REQUEST", "site_id 必填", 400)
        if not isinstance(cap, int) or cap <= 0:
            raise DomainError("BAD_REQUEST", "uplink_bps 必须是正整数（字节/秒）", 400)

        def m(s):
            old = s["sites"].get(sid)
            site = old or {"site_id": sid, "name": name, "cache": [],
                           "created_at": self.now()}
            old_cap = site.get("uplink_bps")
            site["uplink_bps"] = cap
            site["name"] = name
            s["sites"][sid] = site
            self._event(s, "SITE_UPSERTED",
                        f"站点 {sid} 共享上行 {_mb(cap)} MiB/s"
                        + (f"（由 {_mb(old_cap)} 调整）" if old_cap else ""), sid)
            return {"site": copy.deepcopy(site), "idempotent": bool(old)}

        return self.commit(m)

    # --------------------------------------------------------- admin: boxes
    def upsert_box(self, body: dict) -> dict:
        bid = (body.get("box_id") or "").strip()
        sid = body.get("site_id") or ""
        cap = body.get("capacity_bytes")
        link = body.get("link_bps")
        if not bid:
            raise DomainError("BAD_REQUEST", "box_id 必填", 400)
        if sid not in self.state["sites"]:
            raise DomainError("PREHEAT_SITE_UNKNOWN", f"站点 {sid} 不存在", 404)
        if not isinstance(cap, int) or cap <= 0:
            raise DomainError("BAD_REQUEST", "capacity_bytes 必须是正整数", 400)
        if not isinstance(link, int) or link <= 0:
            raise DomainError("BAD_REQUEST", "link_bps 必须是正整数", 400)

        def m(s):
            if sid not in s["sites"]:
                raise DomainError("PREHEAT_SITE_UNKNOWN", f"站点 {sid} 不存在", 404)
            old = s["boxes"].get(bid)
            used = self._box_disk_used(s, bid) if old else 0
            if cap < used:
                raise DomainError(
                    "PREHEAT_BOX_SHRINK_CONFLICT",
                    f"盒子 {bid} 已占用 {used} 字节，不能把容量缩到 {cap}", 409)
            box = old or {"box_id": bid, "site_id": sid, "stored": [],
                          "online": True, "created_at": self.now()}
            box["site_id"] = sid
            box["capacity_bytes"] = cap
            box["link_bps"] = link
            if "effective_link_bps" not in box:
                box["effective_link_bps"] = link
            if box.get("online", True):
                box["effective_link_bps"] = link
            box["updated_at"] = self.now()
            s["boxes"][bid] = box
            self._event(s, "BOX_UPSERTED",
                        f"盒子 {bid}@{sid}：容量 {_mb(cap)} MiB，"
                        f"链路上限 {_mb(link)} MiB/s", bid)
            return {"box": self._box_view(s, box), "idempotent": bool(old)}

        return self.commit(m)

    def set_box_online(self, bid: str, online: bool,
                       degraded_bps: Optional[int] = None) -> dict:
        """Mark a box online/offline; offline frees every grant+reservation."""
        def m(s):
            box = s["boxes"].get(bid)
            if not box:
                raise DomainError("NOT_FOUND", f"盒子 {bid} 不存在", 404)
            was = box["online"]
            box["online"] = online
            if online:
                box["effective_link_bps"] = degraded_bps or box["link_bps"]
            else:
                box["effective_link_bps"] = 0
            box["updated_at"] = self.now()
            # mark every live replica on this box offline/online — next tick
            # releases (offline) or rebuilds (online) reservations from state.
            touched = []
            for job in s["jobs"].values():
                for r in job["replicas"]:
                    if r["box_id"] != bid:
                        continue
                    if r["status"] in LIVE_STATUSES or r["status"] == "paused":
                        if not online:
                            if r["status"] != "paused":
                                r["status"] = "offline"
                        else:
                            if r["status"] == "offline":
                                r["status"] = "queued"
                        touched.append((job["job_id"], r))
            verb = "离线（立即释放全部名额与预留）" if not online else (
                f"恢复在线（有效链路 {_mb(box['effective_link_bps'])} MiB/s，"
                f"只补剩余数据）")
            self._event(s, "BOX_" + ("OFFLINE" if not online else "ONLINE"),
                        f"盒子 {bid} {verb}；影响副本 {len(touched)} 个", bid)
            return {"box_id": bid, "online": online,
                    "effective_link_bps": box["effective_link_bps"],
                    "affected": [j for j, _ in touched]}

        return self.commit(m)

    def degrade_box_link(self, bid: str, bps: int) -> dict:
        if not isinstance(bps, int) or bps < 0:
            raise DomainError("BAD_REQUEST", "bps 必须是非负整数", 400)

        def m(s):
            box = s["boxes"].get(bid)
            if not box:
                raise DomainError("NOT_FOUND", f"盒子 {bid} 不存在", 404)
            if not box["online"]:
                raise DomainError("PREHEAT_BOX_OFFLINE",
                                  f"盒子 {bid} 当前离线，请先恢复在线", 409)
            old = box["effective_link_bps"]
            if bps == 0 or bps >= box["link_bps"]:
                box["effective_link_bps"] = box["link_bps"]
                note = "链路恢复至标称上限"
            else:
                box["effective_link_bps"] = bps
                note = f"链路降级为 {_mb(bps)} MiB/s（标称 {_mb(box['link_bps'])}）"
            box["updated_at"] = self.now()
            self._event(s, "BOX_LINK_CHANGED",
                        f"盒子 {bid} {note}（原有效 {_mb(old)} MiB/s）", bid)
            return {"box_id": bid, "effective_link_bps": box["effective_link_bps"],
                    "nominal_link_bps": box["link_bps"], "degraded": bps > 0
                    and bps < box["link_bps"]}

        return self.commit(m)

    # ============================================================ submit
    def submit_job(self, body: dict) -> dict:
        name = (body.get("name") or "预热作业").strip()
        targets = body.get("targets")
        blobs = body.get("blobs")
        if not isinstance(targets, list) or not targets or \
                not all(isinstance(x, str) and x.strip() for x in targets):
            raise DomainError("BAD_REQUEST", "targets 必须是非空盒子编号数组", 400)
        if not isinstance(blobs, list) or not blobs or \
                not all(isinstance(x, str) and x.strip() for x in blobs):
            raise DomainError("BAD_REQUEST", "blobs 必须是非空内容对象数组", 400)
        targets = list(dict.fromkeys(t.strip() for t in targets))
        blobs = list(dict.fromkeys(b.strip() for b in blobs))

        deadline_ticks = body.get("deadline_ticks")
        rate_cap_bps = body.get("rate_cap_bps")          # per-link cap, per job
        min_online_bps = body.get("min_online_bps", 0)  # per-replica floor
        weight = body.get("weight", 1)
        if deadline_ticks is not None and (
                not isinstance(deadline_ticks, int) or deadline_ticks <= 0):
            raise DomainError("BAD_REQUEST", "deadline_ticks 必须是正整数", 400)
        if rate_cap_bps is not None and (
                not isinstance(rate_cap_bps, int) or rate_cap_bps <= 0):
            raise DomainError("BAD_REQUEST", "rate_cap_bps 必须是正整数", 400)
        if not isinstance(min_online_bps, int) or min_online_bps < 0:
            raise DomainError("BAD_REQUEST", "min_online_bps 必须是非负整数", 400)
        if not isinstance(weight, int) or weight < 1:
            raise DomainError("BAD_REQUEST", "weight 必须是 >=1 的整数", 400)
        submit_key = (body.get("idempotency_key") or body.get("submit_key")
                      or "").strip()
        resubmit_of = (body.get("resubmit_of") or "").strip()

        with self.lock:
            for b in blobs:
                if b not in self.state["blobs"]:
                    raise DomainError("PREHEAT_BLOB_UNKNOWN",
                                      f"内容对象 {b} 不存在于内容库", 404)
            for t in targets:
                if t not in self.state["boxes"]:
                    raise DomainError("PREHEAT_BOX_UNKNOWN",
                                      f"目标盒 {t} 不存在", 404)
            if resubmit_of and resubmit_of not in self.state["jobs"]:
                raise DomainError("PREHEAT_JOB_UNKNOWN",
                                  f"被再次提交的作业 {resubmit_of} 不存在", 404)
            # resubmission determinism: the original must already be closed
            if resubmit_of:
                orig = self.state["jobs"][resubmit_of]
                if orig["status"] not in ("canceled", "failed", "complete"):
                    raise DomainError(
                        "PREHEAT_RESUBMIT_CONFLICT",
                        f"作业 {resubmit_of} 仍为 {orig['status']}，"
                        f"必须先取消/终结才能再次提交", 409)
            if submit_key and submit_key in self.state["job_by_submit"]:
                jid = self.state["job_by_submit"][submit_key]
                return {"job": self.get_job(jid), "idempotent": True}

        def m(s):
            if submit_key and submit_key in s["job_by_submit"]:
                jid = s["job_by_submit"][submit_key]
                return {"job": self._job_view(s, s["jobs"][jid]),
                        "idempotent": True}
            if resubmit_of and resubmit_of not in s["jobs"]:
                raise DomainError("PREHEAT_JOB_UNKNOWN",
                                  f"被再次提交的作业 {resubmit_of} 不存在", 404)

            jid = self._next(s, "job", "JH")
            total_size = sum(s["blobs"][b]["size"] for b in blobs)
            replicas, admission = [], []
            # ---- admission: pair disk reservation with queue slot per box.
            # All boxes must admit (atomic batch); otherwise nothing is taken.
            planned = []
            for bid in targets:
                box = s["boxes"][bid]
                partials = box.get("partial", {})
                already = [b for b in blobs if b in box["stored"]]
                # bytes already on disk (landed blobs + offsets left by a
                # canceled job) are never re-requested — resubmit tops up.
                have = sum(s["blobs"][b]["size"] for b in already)
                have += sum(min(partials.get(b, 0), s["blobs"][b]["size"])
                            for b in blobs if b not in box["stored"])
                need = total_size - have
                free = box["capacity_bytes"] - self._box_disk_used(s, bid)
                # reservations already held by OTHER live replicas on the box
                held = sum(r["reserve_bytes"]
                           for job in s["jobs"].values() for r in job["replicas"]
                           if r["box_id"] == bid and r["status"] in LIVE_STATUSES
                           and not (job["job_id"] == jid))
                available = free - held
                planned.append((bid, need, available, already))
            if any(need > avail for _, need, avail, _ in planned):
                worst = max(planned, key=lambda p: p[1] - p[2])
                raise DomainError(
                    "PREHEAT_BOX_SPACE",
                    f"盒内可用空间不足：{worst[0]} 需 {worst[1]} 字节，"
                    f"扣减在途预留后仅剩 {worst[2]}；整批拒绝，不占用任何名额",
                    409, {"planned": [{"box_id": b, "need": n, "available": a}
                                      for b, n, a, _ in planned]})

            for bid, need, available, already in planned:
                box = s["boxes"][bid]
                site = s["sites"][box["site_id"]]
                partials = box.get("partial", {})
                cached = [b for b in blobs if b in site["cache"]
                          and b not in box["stored"]]
                from_wan = [b for b in blobs if b not in site["cache"]
                            and b not in box["stored"]]
                # inherit canceled-job partial offsets so a re-submission only
                # sends the tail of each blob (暂停/取消后恢复只补剩余数据)
                init_progress = {b: min(partials[b], s["blobs"][b]["size"])
                                 for b in blobs if b in partials
                                 and b not in box["stored"]}
                r = {
                    "box_id": bid, "site_id": box["site_id"],
                    "status": "offline" if not box["online"] else "queued",
                    "blobs": list(blobs),
                    # per-blob state: stored (already on disk at submit) /
                    # wan / cached (landed by this job); progress[b] persists
                    # the byte offset of in-flight blobs across ticks & restarts
                    "received": {b: ("stored" if b in box["stored"] else None)
                                 for b in blobs},
                    "progress": init_progress,
                    "wan_total": sum(max(0, s["blobs"][b]["size"]
                                         - init_progress.get(b, 0))
                                     for b in from_wan),
                    "cache_total": sum(max(0, s["blobs"][b]["size"]
                                          - init_progress.get(b, 0))
                                       for b in cached),
                    "wan_done": 0, "cache_done": 0,
                    "reserve_bytes": need,        # paired with queue membership
                    "allocated_bps": 0,
                    "cache_bps": 0,
                    "granted_bps": 0,
                    "reason": ("BOX_OFFLINE" if not box["online"] else "ELIGIBLE"),
                    "deficit": 0,                 # DRR credit (欠账)
                    "starved_ticks": 0,           # ticks with zero WAN service
                    "served_ticks": 0,
                    "enqueue_tick": s["clock"],
                    "last_service_tick": None,
                }
                replicas.append(r)
                admission.append({"box_id": bid, "need_bytes": need,
                                  "available_bytes": available,
                                  "from_cache": cached, "from_wan": from_wan})
            status = "offline" if all(r["status"] == "offline"
                                      for r in replicas) else "queued"
            job = {
                "job_id": jid, "name": name,
                "blobs": list(blobs), "targets": list(targets),
                "deadline_ticks": deadline_ticks,     # relative to submit tick
                "submitted_tick": s["clock"],
                "rate_cap_bps": rate_cap_bps,
                "min_online_bps": min_online_bps,
                "weight": max(1, weight),
                "status": status,
                "replicas": replicas,
                "admission": admission,
                "resubmit_of": resubmit_of or None,
                "superseded_by": None,
                "created_at": self.now(), "updated_at": self.now(),
                "events": [{"tick": s["clock"], "event": "JOB_SUBMITTED",
                            "detail": f"提交作业：{len(blobs)} 内容 / "
                                      f"{len(targets)} 盒"}]}
            if resubmit_of:
                orig = s["jobs"][resubmit_of]
                orig["superseded_by"] = jid
                job["events"].append(
                    {"tick": s["clock"], "event": "JOB_RESUBMITTED",
                     "detail": f"由 {resubmit_of} 再次提交，未完成盒只补剩余数据"})
            s["jobs"][jid] = job
            if submit_key:
                s["job_by_submit"][submit_key] = jid
            total_wan = sum(r["wan_total"] for r in replicas)
            total_cache = sum(r["cache_total"] for r in replicas)
            self._event(s, "JOB_SUBMITTED",
                        f"作业 {jid}「{name}」：{len(blobs)} 对象 × {len(targets)} 盒，"
                        f"WAN {_mb(total_wan)} MiB / 缓存命中 {_mb(total_cache)} MiB"
                        + (f"，截止 +{deadline_ticks}s" if deadline_ticks else "")
                        + (f"（再次提交自 {resubmit_of}）" if resubmit_of else ""),
                        jid)
            return {"job": self._job_view(s, job), "idempotent": False}

        return self.commit(m)

    # ===================================================== quota / controls
    def update_quota(self, jid: str, body: dict) -> dict:
        """Deterministic quota edit; only live (non-closed) jobs are mutable."""
        fields = {}
        for k in ("deadline_ticks", "rate_cap_bps", "min_online_bps", "weight"):
            if k in body and body[k] is not None:
                v = body[k]
                if k == "weight":
                    if not isinstance(v, int) or v < 1:
                        raise DomainError("BAD_REQUEST", "weight 必须 >=1 整数", 400)
                elif not isinstance(v, int) or v <= 0:
                    raise DomainError("BAD_REQUEST", f"{k} 必须是正整数", 400)
                fields[k] = v

        def m(s):
            job = s["jobs"].get(jid)
            if not job:
                raise DomainError("NOT_FOUND", f"作业 {jid} 不存在", 404)
            if job["status"] in ("complete", "canceled", "failed"):
                raise DomainError(
                    "PREHEAT_JOB_CLOSED",
                    f"作业 {jid} 已终结（{job['status']}），配额不可修改；"
                    f"如需调整请再次提交（resubmit）", 409)
            before = {k: job.get(k) for k in fields}
            job.update(fields)
            job["updated_at"] = self.now()
            job["events"].append(
                {"tick": s["clock"], "event": "QUOTA_UPDATED",
                 "detail": "；".join(f"{k}: {before[k]} → {job[k]}"
                                     for k in fields) or "无变更"})
            self._event(s, "QUOTA_UPDATED",
                        f"作业 {jid} 配额修改："
                        + ("；".join(f"{k} {before[k]}→{job[k]}" for k in fields)),
                        jid)
            return {"job": self._job_view(s, job), "changed": fields,
                    "before": before}

        return self.commit(m)

    def pause_job(self, jid: str) -> dict:
        return self._set_paused(jid, True)

    def resume_job(self, jid: str) -> dict:
        return self._set_paused(jid, False)

    def _set_paused(self, jid: str, paused: bool) -> dict:
        def m(s):
            job = s["jobs"].get(jid)
            if not job:
                raise DomainError("NOT_FOUND", f"作业 {jid} 不存在", 404)
            want = "paused" if paused else "queued"
            if job["status"] == want:
                return {"job": self._job_view(s, job), "idempotent": True}
            if job["status"] in ("complete", "canceled", "failed"):
                return {"job": self._job_view(s, job), "idempotent": True,
                        "ignored": f"作业已 {job['status']}"}
            if paused:
                if job["status"] not in ("queued", "running", "blocked"):
                    raise DomainError(
                        "PREHEAT_JOB_STATE",
                        f"作业 {jid} 当前 {job['status']}，不可暂停"
                        + ("（离线盒恢复后自动回到排队）"
                           if job["status"] == "offline" else ""), 409)
                job["status"] = "paused"
                for r in job["replicas"]:
                    if r["status"] in ("queued", "running", "blocked"):
                        r["status"] = "paused"
                        r["allocated_bps"] = 0
                        r["reason"] = "JOB_PAUSED"
                job["events"].append(
                    {"tick": s["clock"], "event": "JOB_PAUSED",
                     "detail": "暂停：立即释放全部磁盘预留与传输名额"})
                self._event(s, "JOB_PAUSED",
                            f"作业 {jid} 暂停，全部名额与预留立即释放（无幽灵占用）", jid)
            else:
                if job["status"] != "paused":
                    raise DomainError(
                        "PREHEAT_JOB_STATE",
                        f"作业 {jid} 当前 {job['status']}，并非暂停态", 409)
                # rebuild reservations against *current* free space; resume
                # never retakes more than the remaining bytes need.
                blocked = self._rebuild_reservations(s, job)
                live = [r for r in job["replicas"]
                        if r["status"] not in ("complete", "canceled")]
                if live and all(r["status"] == "offline" for r in live):
                    job["status"] = "offline"
                elif live and all(r["status"] in ("blocked", "offline")
                                  for r in live):
                    job["status"] = "blocked"
                else:
                    job["status"] = "queued"
                job["events"].append(
                    {"tick": s["clock"], "event": "JOB_RESUMED",
                     "detail": "恢复：按当前空间重建预留，只补剩余数据"
                               + ("；部分盒空间不足进入等待" if blocked else "")})
                self._event(s, "JOB_RESUMED",
                            f"作业 {jid} 恢复，重建队列与预留，只补剩余数据", jid)
            job["updated_at"] = self.now()
            return {"job": self._job_view(s, job), "idempotent": False}

        return self.commit(m)

    def cancel_job(self, jid: str, reason: str = "") -> dict:
        """Verdict-once: canceling a closed job returns the first verdict."""
        with self.lock:
            old = self.state["jobs"].get(jid)
            if not old:
                raise DomainError("NOT_FOUND", f"作业 {jid} 不存在", 404)
            if old["status"] in ("complete", "canceled", "failed"):
                return {"job": self.get_job(jid), "idempotent": True,
                        "verdict": old["status"]}

        def m(s):
            job = s["jobs"][jid]
            if job["status"] in ("complete", "canceled", "failed"):
                return {"job": self._job_view(s, job), "idempotent": True,
                        "verdict": job["status"]}
            freed = sum(r["reserve_bytes"] for r in job["replicas"])
            for r in job["replicas"]:
                box = s["boxes"][r["box_id"]]
                # pair-release: drop reservation and grant together; bytes
                # already received stay physically on the box (content reuse).
                # Fully-landed blobs are in box.stored; in-flight offsets are
                # frozen into box.partial so a re-submission resumes from the
                # exact byte instead of re-sending the blob.
                for b, off in r.get("progress", {}).items():
                    if r["received"].get(b) in ("stored", "wan", "cached"):
                        continue
                    if off > 0 and b not in box["stored"]:
                        box.setdefault("partial", {})[b] = max(
                            box.get("partial", {}).get(b, 0), off)
                r["reserve_bytes"] = 0
                r["allocated_bps"] = 0
                if r["status"] != "complete":
                    r["status"] = "canceled"
                    r["reason"] = "JOB_PAUSED"
            job["status"] = "canceled"
            job["cancel_reason"] = reason
            job["updated_at"] = self.now()
            job["events"].append(
                {"tick": s["clock"], "event": "JOB_CANCELED",
                 "detail": f"取消（{reason or '调度员取消'}）；释放预留 {_mb(freed)} MiB，"
                           f"已传内容保留于盒内供复用"})
            self._event(s, "JOB_CANCELED",
                        f"作业 {jid} 取消：释放全部预留与名额（{_mb(freed)} MiB），"
                        f"已传内容保留供后续复用", jid)
            return {"job": self._job_view(s, job), "idempotent": False,
                    "freed_reserve_bytes": freed}

        return self.commit(m)

    # =============================================================== tick
    def _box_disk_used(self, s: dict, box_id: str) -> int:
        """Physical bytes on a box: fully-landed blobs + in-flight partials.

        Partial blob bytes occupy real disk under the paired reservation;
        counting them here makes capacity checks exact and means a crash (which
        drops in-memory reservations and rebuilds from persisted progress)
        always recomputes the true footprint — never a ghost, never over-booked.
        """
        used = sum(self._blob_size(s, b)
                   for b in s["boxes"][box_id]["stored"])
        used += sum(s["boxes"][box_id].get("partial", {}).values())
        for job in s["jobs"].values():
            for r in job["replicas"]:
                if r["box_id"] != box_id:
                    continue
                for b, off in r.get("progress", {}).items():
                    if r["received"].get(b) in ("stored", "wan", "cached"):
                        continue
                    used += off
        return used

    def _remaining(self, s: dict, r: dict) -> tuple[int, int]:
        """Return (wan_remaining, cache_remaining) bytes for a replica.

        Per-blob receive offsets are persisted, so a paused/resumed, degraded
        or restarted replica only ever owes bytes it has not yet received
        (暂停后恢复时只补剩余数据).
        """
        wan_rem = cache_rem = 0
        site_cache = s["sites"][r["site_id"]]["cache"]
        for b in r["blobs"]:
            if r["received"].get(b) in ("stored", "wan", "cached"):
                continue
            size = s["blobs"][b]["size"]
            owed = size - r.get("progress", {}).get(b, 0)
            if b in site_cache:
                cache_rem += owed
            else:
                wan_rem += owed
        return wan_rem, cache_rem

    def _rebuild_reservations(self, s: dict, job: dict) -> bool:
        """Recompute every live replica's reservation from persisted state.

        This is the *only* place reservations grow on resume/recovery, and it
        always pairs a reservation with a live queue slot. Returns True if any
        replica had to wait for space (reservation stays 0 → blocked).
        """
        blocked = False
        for r in job["replicas"]:
            if r["status"] == "complete" or r["status"] == "canceled":
                r["reserve_bytes"] = 0
                continue
            was_paused = False
            box = s["boxes"][r["box_id"]]
            wan_rem, cache_rem = self._remaining(s, r)
            need = wan_rem + cache_rem
            used = self._box_disk_used(s, r["box_id"])
            held = sum(
                o["reserve_bytes"]
                for oj in s["jobs"].values() for o in oj["replicas"]
                if o["box_id"] == r["box_id"] and o["status"] in LIVE_STATUSES
                and o is not r)
            available = box["capacity_bytes"] - used - held
            if need <= available:
                r["reserve_bytes"] = need
                if not box["online"]:
                    r["status"] = "offline"
                elif r["status"] in ("blocked", "paused", "offline", "queued"):
                    r["status"] = "queued"
                r["reason"] = "BOX_OFFLINE" if not box["online"] else "ELIGIBLE"
            else:
                # space wait: release the pair, keep position via persisted
                # receive state + deficit (queue membership is not lost, only
                # the resource pair is — re-acquired atomically once space frees)
                r["reserve_bytes"] = 0
                r["allocated_bps"] = 0
                r["reason"] = "BOX_SPACE_WAIT"
                if r["status"] != "paused" and box["online"]:
                    r["status"] = "blocked"
                blocked = True
        return blocked

    def _schedule_site(self, s: dict, site_id: str) -> dict:
        """One tick of per-site scheduling. Returns a tick report fragment.

        Phase 1  floors   : every eligible online replica with a disk
                            reservation first gets its min-online floor
                            (never beyond its remaining demand / rate cap /
                            effective link), charged to the shared uplink.
        Phase 2  DRR      : the remainder is distributed by weighted deficit
                            round robin; persisted deficits guarantee that a
                            replica with zero service accumulates credit and
                            gets served before newcomers forever (no starvation).
        """
        site = s["sites"][site_id]
        cap = site["uplink_bps"]
        elig = []                      # replicas that may take WAN this tick
        for job in s["jobs"].values():
            if job["status"] in ("canceled", "complete", "failed"):
                continue
            for r in job["replicas"]:
                if r["site_id"] != site_id or r["status"] == "complete":
                    continue
                box = s["boxes"][r["box_id"]]
                wan_rem, cache_rem = self._remaining(s, r)
                r["allocated_bps"] = 0
                r["cache_bps"] = 0
                r["granted_bps"] = 0
                if r["status"] == "paused" or job["status"] == "paused":
                    r["reason"] = "JOB_PAUSED"
                    continue
                if not box["online"]:
                    # _reconcile_all already set offline + reservation; no grant
                    continue
                # _reconcile_all paired a reservation exactly with remaining
                # demand or marked this replica blocked; only paired replicas
                # reach eligibility (no grant without a reservation)
                if r["status"] == "blocked" or r["reserve_bytes"] == 0:
                    continue
                if wan_rem <= 0 and cache_rem <= 0:
                    self._complete_replica(s, job, r)
                    continue
                link = min(box["effective_link_bps"],
                           job["rate_cap_bps"] or box["effective_link_bps"])
                if link <= 0:
                    r["reason"] = "LINK_DEGRADED"
                    continue
                r["_link"] = link
                r["_eff_link"] = box["effective_link_bps"]
                r["_wan_rem"] = wan_rem
                r["_cache_rem"] = cache_rem
                if r["reason"] not in ("LINK_DEGRADED",):
                    r["reason"] = "LINK_DEGRADED" if link < box["link_bps"] else "ELIGIBLE"
                elig.append((job, r))

        budget = cap
        # ---- phase 1: minimum online floors
        for job, r in elig:
            floor = min(job["min_online_bps"], r["_link"], r["_wan_rem"], budget)
            if floor < 0:
                floor = 0
            if floor > 0:
                r["allocated_bps"] += floor
                budget -= floor
                r["_floored"] = True
            else:
                r["_floored"] = False
                if r["_wan_rem"] > 0 and budget <= 0:
                    r["reason"] = "SITE_CONTENTION"
        # ---- phase 2: weighted deficit round robin on the remainder
        # Service order: largest deficit first (credit accumulated while
        # starved), deadline urgency tie-breaks, then submit order. A replica
        # whose deficit keeps growing can never be passed forever.
        def drr_key(item):
            job, r = item
            dl = job["deadline_ticks"]
            elapsed = s["clock"] - job["submitted_tick"]
            slack = (dl - elapsed) if dl is not None else 10 ** 9
            return (-r["deficit"] - r["starved_ticks"] * QUANTUM // 2,
                    slack, job["submitted_tick"], job["job_id"], r["box_id"])

        # Standard weighted DRR: each active replica with remaining demand is
        # in the ring every round and earns QUANTUM*weight credit on each visit;
        # it may spend up to min(deficit, link headroom, remaining demand,
        # remaining budget). A replica that cannot spend a full quantum keeps
        # its deficit and is re-queued to be revisited this same tick; one
        # whose deficit reaches 0 but still has demand is revisited next
        # round. Persisted deficits mean a starved replica accumulates credit
        # over ticks and is served before any zero-deficit newcomer.
        ring = [x for x in elig if x[1]["_wan_rem"] > 0]
        ring.sort(key=drr_key)
        progress = True
        rounds = 0
        max_rounds = 256
        while budget > 0 and progress and ring and rounds < max_rounds:
            progress = False
            rounds += 1
            next_ring = []
            for job, r in ring:
                if budget <= 0:
                    next_ring.append((job, r))
                    continue
                headroom_link = r["_link"] - r["allocated_bps"]
                if headroom_link <= 0 or r["_wan_rem"] <= 0:
                    continue  # rate-cap limited or done this tick; leaves ring
                r["deficit"] += QUANTUM * job["weight"]
                want = min(r["deficit"], headroom_link, r["_wan_rem"], budget)
                if want <= 0:
                    next_ring.append((job, r))
                    continue
                r["allocated_bps"] += want
                r["deficit"] -= want
                budget -= want
                progress = True
                # stays in the ring iff it still has demand and either keeps
                # unspent credit (revisit same round) or there is still budget
                # (earn another quantum next round)
                if r["_wan_rem"] > 0 and r["allocated_bps"] < r["_link"] \
                        and budget > 0:
                    next_ring.append((job, r))
            ring = next_ring

        # ---- transfer bytes (cache = LAN, fast; wan = shared uplink)
        granted_total = 0
        for job, r in elig:
            granted = r["allocated_bps"]
            self._transfer(s, job, r)
            used_wan = r["allocated_bps"]          # _transfer resets to actual
            granted_total += used_wan
            if used_wan == 0 and r["_wan_rem"] > 0:
                r["starved_ticks"] += 1
                if r["reason"] == "ELIGIBLE":
                    r["reason"] = ("FLOOR_CAP" if job["min_online_bps"] > 0
                                   and budget <= 0 else "SITE_CONTENTION")
            elif used_wan > 0:
                r["starved_ticks"] = 0
                r["served_ticks"] += 1
                r["last_service_tick"] = s["clock"]
                # cap reporting: grant hit the effective link / per-job cap
                if granted >= r["_link"]:
                    if (job["rate_cap_bps"] or 10 ** 18) <= r["_eff_link"]:
                        r["reason"] = "RATE_CAP"
                    else:
                        r["reason"] = "LINK_DEGRADED"
                elif job["min_online_bps"] > 0 and \
                        granted <= job["min_online_bps"] + 1 and \
                        budget > 0:
                    r["reason"] = "FLOOR_CAP"
                else:
                    r["reason"] = "ELIGIBLE"
            r["granted_bps"] = granted              # planned grant this tick
            r.pop("_link", None)
            r.pop("_eff_link", None)
            r.pop("_wan_rem", None)
            r.pop("_cache_rem", None)
            r.pop("_floored", None)
        return {"site_id": site_id, "capacity_bps": cap,
                "granted_bps": granted_total}

    def _transfer(self, s: dict, job: dict, r: dict):
        """Move up to one tick of bytes; persist per-blob receive state.

        WAN bytes are bounded by the DRR-allocated shared-uplink grant;
        cache-hit bytes travel over the site LAN at CACHE_FAST and never
        consume uplink.
        """
        box = s["boxes"][r["box_id"]]
        site = s["sites"][r["site_id"]]
        wan_budget = r["allocated_bps"] * TICK_SECONDS     # uplink grant (WAN)
        cache_budget = CACHE_FAST * TICK_SECONDS           # site LAN
        moved_wan = moved_cache = 0
        order = sorted(r["blobs"])
        for b in order:
            if r["received"].get(b) in ("stored", "wan", "cached"):
                continue
            size = s["blobs"][b]["size"]
            off = r["progress"].get(b, 0)
            if b in site["cache"]:
                take = min(size - off, cache_budget - moved_cache)
                if take > 0:
                    moved_cache += take
                    off += take
                    r["progress"][b] = off
                    if off == size:
                        r["received"][b] = "cached"
            else:
                take = min(size - off, wan_budget - moved_wan)
                if take > 0:
                    moved_wan += take
                    off += take
                    r["progress"][b] = off
                    if off == size:
                        r["received"][b] = "wan"

        # actual last-tick throughput per channel (dashboard/ETA use)
        r["allocated_bps"] = moved_wan // TICK_SECONDS
        r["cache_bps"] = moved_cache // TICK_SECONDS

        # land fully-received blobs atomically: box storage + site cache
        newly_landed = []
        for b in order:
            if r["received"][b] in ("wan", "cached"):
                if b not in box["stored"]:
                    box["stored"].append(b)
                    newly_landed.append(b)
                box.get("partial", {}).pop(b, None)
                if b not in site["cache"] and r["received"][b] == "wan":
                    site["cache"].append(b)   # uplink byte now reusable at site
        # reservation always mirrors bytes still owed (pair invariant)
        wan_rem, cache_rem = self._remaining(s, r)
        r["reserve_bytes"] = wan_rem + cache_rem
        r["wan_done"] = r["wan_total"] - wan_rem
        r["cache_done"] = r["cache_total"] - cache_rem
        if (moved_wan + moved_cache) > 0 and r["status"] != "paused":
            r["status"] = "running"
        if newly_landed:
            job["events"].append(
                {"tick": s["clock"], "event": "BLOB_LANDED",
                 "detail": f"{r['box_id']} 落地 {','.join(newly_landed)}"})
        if wan_rem == 0 and cache_rem == 0:
            self._complete_replica(s, job, r)

    def _complete_replica(self, s: dict, job: dict, r: dict):
        if r["status"] == "complete":
            return
        r["status"] = "complete"
        r["reason"] = "DONE"
        r["reserve_bytes"] = 0
        r["allocated_bps"] = 0
        r["completed_tick"] = s["clock"]
        job["events"].append(
            {"tick": s["clock"], "event": "REPLICA_COMPLETE",
             "detail": f"{r['box_id']} 全部内容到位"})
        self._event(s, "REPLICA_COMPLETE",
                    f"作业 {job['job_id']} 盒 {r['box_id']} 预热完成",
                    job["job_id"])
        statuses = {x["status"] for x in job["replicas"]}
        if statuses <= {"complete", "canceled"}:
            job["status"] = "complete"
            job["completed_tick"] = s["clock"]
            job["updated_at"] = self.now()
            job["events"].append(
                {"tick": s["clock"], "event": "JOB_COMPLETE",
                 "detail": "全部目标盒到位"})
            self._event(s, "JOB_COMPLETE",
                        f"作业 {job['job_id']}「{job['name']}」全部完成",
                        job["job_id"])

    def _reconcile_all(self, s: dict):
        """Release every pair, then re-acquire reservations per box in queue
        order against current free space.

        This global pass (instead of per-job in-place rebuilds) makes capacity
        reclaimed by cancel/complete/shrink visible deterministically: earlier
        queue positions win, later ones wait as BLOCKED with zero pair held.
        Paused replicas never re-acquire.
        """
        # pass 1: release all pairs (paired release; no ghost may survive)
        for job in s["jobs"].values():
            for r in job["replicas"]:
                if r["status"] in LIVE_STATUSES and r["status"] != "paused":
                    r["reserve_bytes"] = 0
                    r["allocated_bps"] = 0
                    r["cache_bps"] = 0
        # pass 2: per box, re-acquire in queue order (deficit/deadline/FIFO)
        for bid, box in s["boxes"].items():
            demand = []
            for job in s["jobs"].values():
                if job["status"] in ("canceled", "complete", "failed",
                                     "paused"):
                    continue
                for r in job["replicas"]:
                    if r["box_id"] != bid or r["status"] == "complete":
                        continue
                    demand.append((job, r))
            demand.sort(key=lambda jr: (
                -(jr[1]["deficit"] + jr[1]["starved_ticks"] * QUANTUM // 2),
                jr[0]["submitted_tick"], jr[0]["job_id"]))
            for job, r in demand:
                wan_rem, cache_rem = self._remaining(s, r)
                need = wan_rem + cache_rem
                if need == 0:
                    self._complete_replica(s, job, r)
                    continue
                if r["status"] == "paused" or job["status"] == "paused":
                    r["reason"] = "JOB_PAUSED"
                    continue
                if not box["online"]:
                    # offline: pair fully released immediately (no ghost slot
                    # or reservation); in-flight bytes remain physically on
                    # disk via progress offsets and are re-acquired on recovery
                    r["status"] = "offline"
                    r["reason"] = "BOX_OFFLINE"
                    r["reserve_bytes"] = 0
                    continue
                used = self._box_disk_used(s, bid)
                held = sum(o["reserve_bytes"]
                           for oj in s["jobs"].values()
                           for o in oj["replicas"]
                           if o["box_id"] == bid and o["status"] in LIVE_STATUSES
                           and o is not r)
                available = box["capacity_bytes"] - used - held
                if need <= available:
                    r["reserve_bytes"] = need
                    if r["status"] in ("blocked", "offline"):
                        r["status"] = "queued"
                    r["reason"] = "ELIGIBLE"
                else:
                    # space wait: pair stays released, queue position retained
                    r["reserve_bytes"] = 0
                    r["allocated_bps"] = 0
                    r["status"] = "blocked"
                    r["reason"] = "BOX_SPACE_WAIT"

    def tick(self, n: int = 1) -> dict:
        """Advance n virtual seconds, scheduling every site once per second."""
        reports = []
        for _ in range(max(1, n)):
            def m(s):
                s["clock"] += 1
                # deadline bookkeeping first (non-destructive: missed deadlines
                # keep running best-effort, just flagged)
                for job in s["jobs"].values():
                    if job["status"] in ("complete", "canceled", "failed"):
                        continue
                    dl = job["deadline_ticks"]
                    if dl is not None and not job.get("missed_deadline") and \
                            s["clock"] - job["submitted_tick"] > dl:
                        job["missed_deadline"] = s["clock"]
                        job["events"].append(
                            {"tick": s["clock"], "event": "DEADLINE_MISSED",
                             "detail": "超过截止时间，继续尽力传输（不抢占在保障作业）"})
                        self._event(s, "DEADLINE_MISSED",
                                    f"作业 {job['job_id']} 超过截止时间，"
                                    f"标记后继续尽力传输", job["job_id"])
                # reservations must always equal remaining demand for live
                # replicas BEFORE scheduling — global pair release/re-acquire
                # reconciles crashes, shrinks, cancels and completions without
                # ever leaving a ghost reservation (磁盘预留与名额成对)
                self._reconcile_all(s)
                rep = [self._schedule_site(s, sid)
                       for sid in sorted(s["sites"])]
                # aggregate job status for blocked-only / mixed cases
                for job in s["jobs"].values():
                    if job["status"] in ("complete", "canceled", "failed",
                                         "paused"):
                        continue
                    live = [r for r in job["replicas"]
                            if r["status"] not in ("complete", "canceled")]
                    if live and all(r["status"] == "offline" for r in live):
                        job["status"] = "offline"
                    elif live and all(r["status"] in ("blocked", "offline")
                                      for r in live):
                        job["status"] = "blocked"
                    elif any(r["status"] == "running" for r in live):
                        job["status"] = "running"
                    else:
                        job["status"] = "queued"
                    job["updated_at"] = self.now()
                return {"tick": s["clock"], "sites": rep}

            reports.append(self.commit(m))
        return {"advanced": len(reports), "clock": reports[-1]["tick"],
                "ticks": reports}

    # ================================================================ views
    def _eta(self, s: dict, job: dict) -> Optional[int]:
        """Project remaining ticks from current grants (same in web and CLI)."""
        worst = None
        for r in job["replicas"]:
            if r["status"] in ("complete", "canceled"):
                continue
            wan_rem, cache_rem = self._remaining(s, r)
            rem = wan_rem + cache_rem
            if rem <= 0:
                continue
            if r["status"] == "paused":
                return None
            # project at last tick's combined WAN+LAN throughput; if the
            # replica got no service, fall back to its configured floor
            rate = r["allocated_bps"] + r.get("cache_bps", 0)
            if rate <= 0:
                rate = job["min_online_bps"]
            if rate <= 0:
                return None
            t = -(-rem // (rate * TICK_SECONDS))
            worst = t if worst is None else max(worst, t)
        return worst

    def _replica_view(self, s: dict, r: dict) -> dict:
        wan_rem, cache_rem = self._remaining(s, r)
        box = s["boxes"][r["box_id"]]
        v = {k: r.get(k) for k in (
            "box_id", "site_id", "status", "wan_total", "cache_total",
            "wan_done", "cache_done", "reserve_bytes", "allocated_bps",
            "cache_bps", "granted_bps", "reason", "deficit", "starved_ticks",
            "served_ticks", "enqueue_tick", "last_service_tick",
            "completed_tick")}
        v["wan_remaining"] = wan_rem
        v["cache_remaining"] = cache_rem
        v["remaining_bytes"] = wan_rem + cache_rem
        total = r["wan_total"] + r["cache_total"]
        done = r["wan_done"] + r["cache_done"]
        v["total_bytes"] = total
        v["percent"] = round(100 * done / total, 1) if total else 100.0
        v["reason_cn"] = REASON_CN.get(r["reason"], r["reason"])
        v["online"] = box["online"]
        v["effective_link_bps"] = box["effective_link_bps"]
        v["progress_offsets"] = {b: off for b, off in r.get("progress", {}).items()
                                 if r["received"].get(b) not in
                                 ("stored", "wan", "cached")}
        return copy.deepcopy(v)

    def _job_view(self, s: dict, job: dict, queue_rank: Optional[int] = None) -> dict:
        wan_tot = sum(r["wan_total"] for r in job["replicas"])
        cache_tot = sum(r["cache_total"] for r in job["replicas"])
        wan_done = sum(min(r["wan_done"], r["wan_total"]) for r in job["replicas"])
        cache_done = sum(min(r["cache_done"], r["cache_total"])
                         for r in job["replicas"])
        total = wan_tot + cache_tot
        done = wan_done + cache_done
        eta = self._eta(s, job)
        v = {"job_id": job["job_id"], "name": job["name"],
             "status": job["status"], "blobs": list(job["blobs"]),
             "targets": list(job["targets"]),
             "deadline_ticks": job["deadline_ticks"],
             "submitted_tick": job["submitted_tick"],
             "rate_cap_bps": job["rate_cap_bps"],
             "min_online_bps": job["min_online_bps"], "weight": job["weight"],
             "total_bytes": total, "done_bytes": done,
             "wan_bytes": wan_tot, "cache_bytes": cache_tot,
             "percent": round(100 * done / total, 1) if total else 100.0,
             "eta_ticks": eta,
             "eta_clock": (s["clock"] + eta) if eta is not None else None,
             "missed_deadline": job.get("missed_deadline"),
             "resubmit_of": job.get("resubmit_of"),
             "superseded_by": job.get("superseded_by"),
             "cancel_reason": job.get("cancel_reason"),
             "queue_rank": queue_rank,
             "created_at": job["created_at"], "updated_at": job["updated_at"],
             "replicas": sorted((self._replica_view(s, r)
                                 for r in job["replicas"]),
                                key=lambda r: r["box_id"])}
        return copy.deepcopy(v)

    def _queue_order(self, s: dict) -> list:
        """Global queue order shown identically on web and CLI:
        urgency (deadline slack) then accumulated deficit (欠账) then FIFO."""
        rows = []
        for job in s["jobs"].values():
            if job["status"] in ("complete", "canceled", "failed"):
                continue
            dl = job["deadline_ticks"]
            elapsed = s["clock"] - job["submitted_tick"]
            slack = (dl - elapsed) if dl is not None else 10 ** 9
            deficit = sum(r["deficit"] for r in job["replicas"])
            starved = max((r["starved_ticks"] for r in job["replicas"]),
                          default=0)
            rows.append((slack, -deficit - starved * QUANTUM // 2,
                         job["submitted_tick"], job["job_id"]))
        rows.sort()
        return [jid for _, _, _, jid in
                sorted(rows, key=lambda x: (x[0], x[1], x[2], x[3]))]

    def list_jobs(self) -> list:
        with self.lock:
            s = self.state
            order = self._queue_order(s)
            rank = {jid: i + 1 for i, jid in enumerate(order)}
            out = [self._job_view(s, job, rank.get(job["job_id"]))
                   for job in s["jobs"].values()]
            out.sort(key=lambda j: (j["status"] in ("complete", "canceled", "failed"),
                                    j["submitted_tick"], j["job_id"]))
            return out

    def get_job(self, jid: str) -> dict:
        with self.lock:
            job = self.state["jobs"].get(jid)
            if not job:
                raise DomainError("NOT_FOUND", f"作业 {jid} 不存在", 404)
            order = self._queue_order(self.state)
            rank = order.index(jid) + 1 if jid in order else None
            v = self._job_view(self.state, job, rank)
            v["events"] = copy.deepcopy(job["events"])
            v["admission"] = copy.deepcopy(job["admission"])
            return v

    def _box_view(self, s: dict, box: dict, full: bool = False) -> dict:
        used = self._box_disk_used(s, box["box_id"])
        reserved = sum(r["reserve_bytes"]
                       for job in s["jobs"].values() for r in job["replicas"]
                       if r["box_id"] == box["box_id"]
                       and r["status"] in LIVE_STATUSES)
        v = {"box_id": box["box_id"], "site_id": box["site_id"],
             "capacity_bytes": box["capacity_bytes"],
             "link_bps": box["link_bps"],
             "effective_link_bps": box["effective_link_bps"],
             "online": box["online"],
             "used_bytes": used, "reserved_bytes": reserved,
             "free_bytes": box["capacity_bytes"] - used - reserved,
             "stored": list(box["stored"]),
             "partial_bytes": dict(box.get("partial", {}))}
        if full:
            v["live_replicas"] = [
                {"job_id": job["job_id"], "status": r["status"],
                 "reserve_bytes": r["reserve_bytes"],
                 "remaining_bytes": sum(self._remaining(s, r)),
                 "allocated_bps": r["allocated_bps"], "reason": r["reason"]}
                for job in s["jobs"].values() for r in job["replicas"]
                if r["box_id"] == box["box_id"] and r["status"] in LIVE_STATUSES]
        return copy.deepcopy(v)

    def list_boxes(self) -> list:
        with self.lock:
            return [self._box_view(self.state, b)
                    for _, b in sorted(self.state["boxes"].items())]

    def get_box(self, bid: str) -> dict:
        with self.lock:
            box = self.state["boxes"].get(bid)
            if not box:
                raise DomainError("NOT_FOUND", f"盒子 {bid} 不存在", 404)
            return self._box_view(self.state, box, full=True)

    def list_sites(self) -> list:
        with self.lock:
            s = self.state
            out = []
            for site in s["sites"].values():
                used = sum(r["allocated_bps"]
                           for job in s["jobs"].values() for r in job["replicas"]
                           if r["site_id"] == site["site_id"])
                out.append({"site_id": site["site_id"], "name": site["name"],
                            "uplink_bps": site["uplink_bps"],
                            "used_bps_last_tick": used,
                            "cache": list(site["cache"])})
            return sorted(out, key=lambda x: x["site_id"])

    def list_blobs(self) -> list:
        with self.lock:
            return [copy.deepcopy(b) for b in self.state["blobs"].values()]

    def dashboard(self) -> dict:
        """One deterministic snapshot consumed by the web page and the CLI."""
        with self.lock:
            s = self.state
            jobs = self.list_jobs()
            # site usage freshly recomputed from allocations would require a
            # tick; report the persisted last allocation by summing replicas
            sites = []
            for site in s["sites"].values():
                granted = sum(r["allocated_bps"]
                              for job in s["jobs"].values()
                              for r in job["replicas"]
                              if r["site_id"] == site["site_id"])
                sites.append({"site_id": site["site_id"], "name": site["name"],
                              "uplink_bps": site["uplink_bps"],
                              "granted_bps": granted,
                              "cache_hit_bytes": sum(
                                  s["blobs"][b]["size"] for b in site["cache"])})
            return {"clock": s["clock"], "tick_seconds": TICK_SECONDS,
                    "sites": sites, "boxes": self.list_boxes(),
                    "jobs": jobs, "blobs": self.list_blobs(),
                    "queue": self._queue_order(s)}

    # ============================================================ invariants
    def assert_invariants(self) -> dict:
        """Self-audit used by tests; raises if any ghost/capacity invariant breaks."""
        with self.lock:
            s = self.state
            problems = []
            # 1. every site grant <= shared uplink
            by_site = {}
            for job in s["jobs"].values():
                for r in job["replicas"]:
                    by_site.setdefault(r["site_id"], 0)
                    by_site[r["site_id"]] += r["allocated_bps"]
            for sid, used in by_site.items():
                if used > s["sites"][sid]["uplink_bps"] + 1:
                    problems.append(f"站点 {sid} 分配 {used} 超过共享上行 "
                                    f"{s['sites'][sid]['uplink_bps']}")
            # 2. used + reserved per box <= capacity; reservation iff live slot
            for bid, box in s["boxes"].items():
                used = self._box_disk_used(s, bid)
                reserved = 0
                for job in s["jobs"].values():
                    for r in job["replicas"]:
                        if r["box_id"] != bid:
                            continue
                        live = r["status"] in LIVE_STATUSES
                        if live and r["reserve_bytes"] == 0 and \
                                sum(self._remaining(s, r)) > 0 and \
                                r["status"] != "blocked":
                            # offline replicas hold reservation too (rebuilt);
                            # only queued/running must strictly hold it
                            if r["status"] in ("queued", "running"):
                                problems.append(
                                    f"{bid} 上 {job['job_id']} 在队却无磁盘预留"
                                    f"（名额/预留未成对）")
                        if not live and r["reserve_bytes"] != 0:
                            problems.append(
                                f"{bid} 上 {job['job_id']} 状态 {r['status']} "
                                f"仍持有预留 {r['reserve_bytes']}（幽灵占用）")
                        if not live and r["allocated_bps"] != 0:
                            problems.append(
                                f"{bid} 上 {job['job_id']} 状态 {r['status']} "
                                f"仍持有传输名额 {r['allocated_bps']}（幽灵名额）")
                        if live:
                            reserved += r["reserve_bytes"]
                if used + reserved > box["capacity_bytes"] + 1:
                    problems.append(
                        f"盒子 {bid} 已用 {used} + 预留 {reserved} "
                        f"> 容量 {box['capacity_bytes']}")
            # 3. no allocation without a paired reservation (space coverage)
            for job in s["jobs"].values():
                for r in job["replicas"]:
                    if r["allocated_bps"] > 0 and r["status"] != "running":
                        problems.append(
                            f"{job['job_id']}/{r['box_id']} 状态 {r['status']} "
                            f"却持有传输名额")
                    if r["allocated_bps"] > 0 and r["reserve_bytes"] == 0 \
                            and sum(self._remaining(s, r)) > 0:
                        problems.append(
                            f"{job['job_id']}/{r['box_id']} 有名额无预留"
                            f"（成对取得被破坏）")
            if problems:
                raise DomainError("PREHEAT_INVARIANT_BROKEN",
                                  "；".join(problems), 500)
            return {"ok": True, "clock": s["clock"],
                    "jobs": len(s["jobs"]), "boxes": len(s["boxes"]),
                    "sites": len(s["sites"])}
