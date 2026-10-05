"""
Capacity-aware preheat orchestration for roadside boxes (STRICTLY READ-ONLY
over the live map service and the mirror repository).

A dispatcher picks an **immutable mirror manifest** and a batch of target
boxes, sets a deadline, a per-box link rate cap and a minimum online
guarantee; the scheduler then arranges transfers according to:

  * **site shared bandwidth** — boxes are grouped into sites; the sum of
    granted rates on one site never exceeds the site capacity;
  * **box free disk** — a transfer only starts when the box can hold the
    remaining bytes; the disk reservation and the transfer slot are always
    acquired and released **as a pair**;
  * **content reuse** — objects already on the box (current mirror image or
    previously completed preheats) are never retransferred; a fully reused
    task completes instantly;
  * **fairness** — eligible-but-unserved tasks accrue debt (欠账) every tick;
    the scheduler serves the largest debt first, so a continuously eligible
    job can never starve;
  * **pause/resume** — pausing releases the (disk, slot) pair; resuming only
    sends the remaining bytes (bytes_done is never rewound).

No ghost occupancy: occupancy is *derived* from task states inside a single
atomic commit, so space shortage, link degradation, box offline, cancel or a
service crash can never leak a reservation. On startup the engine rebuilds
the queue, the debts and the remaining times from the persisted store: any
task caught mid-transfer is returned to the queue and its (disk, slot) pair
is released.

Quota changes, cancel and resubmission are deterministic: resubmitting an
identical job replays the first job (unless it was canceled — then a fresh
job is created); cancel is verdict-once. The web console and preheat_cli.py
read the same store and the same pure scheduling functions, so queue order,
rate-limit reasons and ETAs are identical.

This engine writes exclusively to data/preheat.json. It never moves region
generations, never touches patches/snapshots, and never mutates manifests.
"""

from __future__ import annotations

import copy
import json
import os
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from engine import DomainError, sha256, utcnow_iso

KB = 1000.0                      # 1 KB = 1000 bytes (rates are KB/s)
EPS = 1e-6
TASK_OPEN = ("queued", "transferring")
JOB_OPEN = ("active", "paused")

# limit / wait reason codes (same strings in web + CLI: same store, same code)
REASONS = {
    "BOX_OFFLINE": "盒子离线，传输名额与磁盘预留不会分配",
    "BOX_DISK_FULL": "盒内可用空间不足，等待释放或扩容",
    "LINK_DOWN": "链路速率上限为 0（链路降级），暂停分配",
    "SITE_SATURATED": "站点共享带宽被更高优先级（欠账更多）的作业占满，欠账累计中",
    "SITE_SHARED": "受站点共享带宽限制",
    "BOX_LINK": "受盒链路速率上限限制（链路降级）",
    "JOB_CAP": "受作业速率上限限制",
    "PAUSED": "作业已暂停，名额与预留已成对释放",
    "READY": "已获得传输名额与磁盘预留",
}


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def _parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


def _round6(x: float) -> float:
    return round(x, 6)


class PreheatEngine:
    def __init__(self, path: str, mirror=None):
        self.path = path
        self.mirror = mirror            # MirrorEngine, used READ-ONLY
        self.lock = threading.RLock()
        self.state: dict = {}
        self._load()

    # ========================================================= persistence ==
    def _load(self):
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as f:
                self.state = json.load(f)
            for k, v in self._blank().items():
                self.state.setdefault(k, v)
        else:
            self.state = self._blank()
        changed = self._recover_locked()
        before = json.dumps(self.state, sort_keys=True, ensure_ascii=False)
        self._settle(self.state)  # rebuild the schedule after any restart
        if changed or not os.path.exists(self.path) or \
                json.dumps(self.state, sort_keys=True, ensure_ascii=False) != before:
            self._persist_locked()

    @staticmethod
    def _blank() -> dict:
        return {"clock": utcnow_iso(),   # simulated clock, advanced by tick()
                "sites": {},             # site_id -> shared-bandwidth site
                "boxes": {},             # box_id -> roadside box record
                "jobs": {},              # job_id -> preheat job
                "job_by_key": {},        # content key -> job_id (deterministic resubmit)
                "tasks": {},             # task_id -> per-(job, box) transfer task
                "events": [],            # append-only orchestration journal
                "seq": {"job": 0, "task": 0, "event": 0}}

    def _persist_locked(self):
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        tmp = f"{self.path}.tmp.{os.getpid()}.{threading.get_ident()}"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.state, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)

    def commit(self, mutate) -> Any:
        """Run mutate(copy), re-settle allocation, atomically persist.

        Every committed state satisfies: per-site granted rate ≤ capacity and
        per-box reserved disk ≤ free space — concurrency can never break the
        caps because allocation is recomputed inside the same atomic commit.
        """
        with self.lock:
            draft = copy.deepcopy(self.state)
            result = mutate(draft)
            self._settle(draft)
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            tmp = f"{self.path}.tmp.{os.getpid()}.{threading.get_ident()}"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(draft, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
            self.state = draft
            return result

    def _recover_locked(self) -> bool:
        """Rebuild queue/debts/remaining after a (crash) restart.

        Any task caught mid-transfer held a (disk reservation, transfer slot)
        pair; the service is gone, so the pair is released and the task is
        re-queued — resume only sends the remaining bytes. Debts persist.
        The caller immediately re-settles, so still-eligible tasks re-acquire
        their pairs deterministically; nothing leaks either way.
        """
        crashed = [t for t in self.state["tasks"].values()
                   if t["status"] == "transferring"]
        if not crashed:
            return False
        for t in crashed:
            t["status"] = "queued"
            t["reserved_bytes"] = 0.0
            self._journal(self.state, t, "RECOVERED",
                          "服务重启：传输名额与磁盘预留已成对释放（无幽灵占用），"
                          f"续传只补剩余 {self._remaining(t):.0f} 字节；欠账保留")
            t["updated_at"] = self.state["clock"]
        self._event_locked(self.state, "SERVICE_RECOVERED",
                           f"服务重启恢复：{len(crashed)} 个传输中任务重新排队，"
                           f"磁盘预留与传输名额成对释放，队列/欠账/剩余时长已重建",
                           crashed[0]["job_id"])
        return True

    # ============================================================= helpers ==
    def _next(self, s: dict, key: str, prefix: str) -> str:
        s["seq"][key] += 1
        return f"{prefix}-{s['seq'][key]:04d}"

    def _clock(self, s: dict) -> datetime:
        return _parse(s["clock"])

    def _event_locked(self, s: dict, etype: str, summary: str, ref: str) -> dict:
        s["seq"]["event"] += 1
        ev = {"event_id": s["seq"]["event"], "ts": s["clock"],
              "type": etype, "summary": summary, "ref": ref}
        s["events"].append(ev)
        return ev

    def _journal(self, s: dict, task: dict, event: str, detail: str):
        task["journal"].append({"ts": s["clock"], "event": event, "detail": detail})

    @staticmethod
    def _remaining(t: dict) -> float:
        return max(0.0, t["bytes_total"] - t["bytes_done"])

    def list_events(self, limit: int = 80) -> list:
        with self.lock:
            return copy.deepcopy(self.state["events"][-limit:][::-1])

    # ==================================================== mirror (read-only)
    def _manifest(self, manifest_id: str) -> dict:
        """Read-only lookup of an immutable manifest in the mirror store."""
        if self.mirror is None:
            raise DomainError("PREHEAT_MIRROR_UNAVAILABLE",
                              "镜像仓库不可用，无法读取内容清单", 409)
        with self.mirror.lock:
            man = self.mirror.state.get("manifests", {}).get(manifest_id)
            if not man:
                raise DomainError("PREHEAT_MANIFEST_UNKNOWN",
                                  f"清单 {manifest_id} 不在镜像仓库中；请先在"
                                  f"镜像分发页生成不可变清单", 404)
            return copy.deepcopy(man)

    # ====================================================== sites & boxes ==
    def upsert_site(self, site_id: str, capacity_kbps: float) -> dict:
        site_id = (site_id or "").strip()
        if not site_id:
            raise DomainError("BAD_REQUEST", "站点编号不能为空", 400)
        try:
            cap = float(capacity_kbps)
        except (TypeError, ValueError):
            raise DomainError("BAD_REQUEST", "站点带宽必须是数字（KB/s）", 400)
        if cap <= 0:
            raise DomainError("BAD_REQUEST", "站点共享带宽必须 > 0", 400)

        def m(s):
            site = s["sites"].get(site_id)
            if site is None:
                site = {"site_id": site_id, "capacity_kbps": cap,
                        "created_at": s["clock"], "updated_at": s["clock"]}
                s["sites"][site_id] = site
                self._event_locked(s, "SITE_REGISTERED",
                                   f"注册站点 {site_id}（共享带宽 {cap:g} KB/s）", site_id)
            elif abs(site["capacity_kbps"] - cap) > EPS:
                old = site["capacity_kbps"]
                site["capacity_kbps"] = cap
                site["updated_at"] = s["clock"]
                self._event_locked(
                    s, "QUOTA_CHANGED",
                    f"站点 {site_id} 共享带宽 {old:g} → {cap:g} KB/s；"
                    f"超出部分将按优先级确定性抢占", site_id)
            return site_id

        self.commit(m)
        return self.get_site(site_id)

    def set_site_capacity(self, site_id: str, capacity_kbps: float) -> dict:
        with self.lock:
            if site_id not in self.state["sites"]:
                raise DomainError("PREHEAT_SITE_UNKNOWN",
                                  f"站点 {site_id} 未注册", 404)
        return self.upsert_site(site_id, capacity_kbps)

    def upsert_box(self, box_id: str, site_id: str, disk_total_mb: float,
                   link_cap_kbps: float, disk_used_mb: float = 0.0) -> dict:
        box_id = (box_id or "").strip()
        site_id = (site_id or "").strip()
        if not box_id or not site_id:
            raise DomainError("BAD_REQUEST", "盒子与站点编号不能为空", 400)
        try:
            disk_total = float(disk_total_mb) * 1e6
            used = float(disk_used_mb) * 1e6
            cap = float(link_cap_kbps)
        except (TypeError, ValueError):
            raise DomainError("BAD_REQUEST", "磁盘/链路参数必须是数字", 400)
        if disk_total <= 0 or used < 0 or used > disk_total or cap < 0:
            raise DomainError("BAD_REQUEST",
                              "磁盘总量必须 > 0、0 ≤ 已用 ≤ 总量、链路速率 ≥ 0", 400)
        with self.lock:
            if site_id not in self.state["sites"]:
                raise DomainError("PREHEAT_SITE_UNKNOWN",
                                  f"站点 {site_id} 未注册，请先注册站点", 404)

        def m(s):
            box = s["boxes"].get(box_id)
            if box is None:
                inventory = self._seed_inventory(box_id)
                box = {"box_id": box_id, "site_id": site_id,
                       "disk_total_bytes": disk_total, "disk_used_bytes": used,
                       "link_cap_kbps": cap, "online": True,
                       "inventory": inventory,
                       "created_at": s["clock"], "updated_at": s["clock"]}
                s["boxes"][box_id] = box
                self._event_locked(
                    s, "BOX_REGISTERED",
                    f"注册盒子 {box_id} @ {site_id}（磁盘 {disk_total / 1e6:g} MB，"
                    f"链路 {cap:g} KB/s，库存对象 {len(inventory)} 个）", box_id)
            else:
                changes = []
                if box["site_id"] != site_id:
                    changes.append(f"站点 {box['site_id']} → {site_id}")
                    box["site_id"] = site_id
                if abs(box["disk_total_bytes"] - disk_total) > EPS:
                    changes.append(
                        f"磁盘 {box['disk_total_bytes'] / 1e6:g} → {disk_total / 1e6:g} MB")
                    box["disk_total_bytes"] = disk_total
                if abs(box["disk_used_bytes"] - used) > EPS:
                    changes.append(
                        f"已用 {box['disk_used_bytes'] / 1e6:g} → {used / 1e6:g} MB")
                    box["disk_used_bytes"] = used
                if abs(box["link_cap_kbps"] - cap) > EPS:
                    changes.append(
                        f"链路 {box['link_cap_kbps']:g} → {cap:g} KB/s")
                    box["link_cap_kbps"] = cap
                if changes:
                    box["updated_at"] = s["clock"]
                    self._event_locked(s, "BOX_UPDATED",
                                       f"盒子 {box_id} 配置更新：" + "，".join(changes),
                                       box_id)
            return box_id

        self.commit(m)
        return self.get_box(box_id)

    def _seed_inventory(self, box_id: str) -> list:
        """Content reuse seed: objects of the box's current mirror image."""
        if self.mirror is None:
            return []
        with self.mirror.lock:
            dev = self.mirror.state.get("devices", {}).get(box_id)
            img = (dev or {}).get("current_image")
            if not img:
                return []
            man = self.mirror.state.get("manifests", {}).get(img.get("manifest_id"))
            if not man:
                return []
            return sorted(o["hash"] for o in man.get("objects", []))

    def set_box_link(self, box_id: str, link_cap_kbps: float) -> dict:
        try:
            cap = float(link_cap_kbps)
        except (TypeError, ValueError):
            raise DomainError("BAD_REQUEST", "链路速率必须是数字（KB/s）", 400)
        if cap < 0:
            raise DomainError("BAD_REQUEST", "链路速率上限不能为负", 400)

        def m(s):
            box = s["boxes"].get(box_id)
            if not box:
                raise DomainError("PREHEAT_BOX_UNKNOWN", f"盒子 {box_id} 未注册", 404)
            old = box["link_cap_kbps"]
            box["link_cap_kbps"] = cap
            box["updated_at"] = s["clock"]
            kind = "LINK_DEGRADED" if cap < old else "LINK_RESTORED"
            self._event_locked(
                s, kind,
                f"盒子 {box_id} 链路速率上限 {old:g} → {cap:g} KB/s"
                + ("（链路降级，超出部分确定性让路）" if cap < old else "（链路恢复）"),
                box_id)
            return box_id

        self.commit(m)
        return self.get_box(box_id)

    def set_box_online(self, box_id: str, online: bool) -> dict:
        def m(s):
            box = s["boxes"].get(box_id)
            if not box:
                raise DomainError("PREHEAT_BOX_UNKNOWN", f"盒子 {box_id} 未注册", 404)
            if box["online"] == bool(online):
                return box_id
            box["online"] = bool(online)
            box["updated_at"] = s["clock"]
            self._event_locked(
                s, "BOX_OFFLINE" if not online else "BOX_ONLINE",
                f"盒子 {box_id} {'离线：传输名额与磁盘预留成对释放，不留幽灵占用' if not online else '恢复在线：重新排队' }",
                box_id)
            return box_id

        self.commit(m)
        return self.get_box(box_id)

    # ============================================================ content ==
    def _needed_bytes(self, s: dict, manifest: dict, box: dict):
        """Bytes this box still needs: chunks holding ≥1 object not in inventory."""
        inv = set(box["inventory"])
        total = sum(c["size"] for c in manifest["chunks"])
        needed = 0.0
        for c in manifest["chunks"]:
            objs = [o for o in manifest["objects"] if o["object_id"] in c["objects"]]
            if any(o["hash"] not in inv for o in objs):
                needed += c["size"]
        return needed, total

    # ============================================================== submit ==
    def create_job(self, body: dict) -> dict:
        manifest_id = (body.get("manifest_id") or "").strip()
        man = self._manifest(manifest_id)  # read-only; 404 if unknown
        targets = body.get("targets")
        if not isinstance(targets, list) or not targets or \
                not all(isinstance(t, str) and t.strip() for t in targets):
            raise DomainError("PREHEAT_NO_TARGETS",
                              "targets 必须是非空盒子编号数组", 400)
        targets = list(dict.fromkeys(t.strip() for t in targets))
        try:
            link_cap = float(body.get("link_cap_kbps", 1024))
            min_online = int(body.get("min_online", len(targets)))
        except (TypeError, ValueError):
            raise DomainError("BAD_REQUEST", "速率/最小在线参数非法", 400)
        if link_cap <= 0:
            raise DomainError("BAD_REQUEST", "作业链路速率上限必须 > 0", 400)
        min_online = max(1, min(len(targets), min_online))
        note = body.get("note", "")
        created_by = body.get("created_by", "dispatcher")

        with self.lock:
            missing = [t for t in targets if t not in self.state["boxes"]]
            if missing:
                raise DomainError(
                    "PREHEAT_BOX_UNKNOWN",
                    f"目标盒子未注册：{', '.join(missing)}；请先注册盒子", 404)
            clock = self._clock(self.state)
            if body.get("deadline"):
                try:
                    deadline = _parse(body["deadline"])
                except ValueError:
                    raise DomainError("BAD_REQUEST", "deadline 必须是 ISO 时间", 400)
            else:
                secs = float(body.get("deadline_in_sec", 3600))
                if secs <= 0:
                    raise DomainError("BAD_REQUEST", "截止时间必须在未来", 400)
                deadline = clock + timedelta(seconds=secs)
            key = sha256({"manifest_hash": man["manifest_hash"],
                          "targets": sorted(targets),
                          "deadline": _iso(deadline),
                          "link_cap_kbps": link_cap,
                          "min_online": min_online, "note": note})
            existing = self.state["job_by_key"].get(key)
            if existing and self.state["jobs"][existing]["status"] != "canceled":
                # deterministic resubmit: identical request replays the first job
                return {"job": self._job_view(self.state, self.state["jobs"][existing],
                                              idempotent=True),
                        "idempotent": True}

        def m(s):
            jid = self._next(s, "job", "PJ")
            job = {"job_id": jid, "seq": s["seq"]["job"], "job_key": key,
                   "manifest_id": man["manifest_id"],
                   "manifest_hash": man["manifest_hash"],
                   "version": man["source"]["version"],
                   "targets": list(targets), "deadline": _iso(deadline),
                   "link_cap_kbps": link_cap, "min_online": min_online,
                   "note": note, "status": "active", "cancel_result": None,
                   "created_by": created_by,
                   "created_at": s["clock"], "updated_at": s["clock"]}
            s["jobs"][jid] = job
            s["job_by_key"][key] = jid
            tasks = []
            for bid in targets:
                tid = self._next(s, "task", "PT")
                box = s["boxes"][bid]
                needed, total = self._needed_bytes(s, man, box)
                task = {"task_id": tid, "seq": s["seq"]["task"], "job_id": jid,
                        "box_id": bid, "site_id": box["site_id"],
                        "status": "queued",
                        "bytes_total": needed, "bytes_done": 0.0,
                        "reserved_bytes": 0.0, "debt_kb": 0.0,
                        "reuse_ratio": (1.0 - needed / total) if total > EPS else 1.0,
                        "journal": [], "created_at": s["clock"],
                        "updated_at": s["clock"], "done_at": None}
                self._journal(s, task, "TASK_QUEUED",
                              f"作业 {jid} 下发：需传输 {needed:.0f}/{total:.0f} 字节"
                              f"（内容复用率 {(task['reuse_ratio'] * 100):.0f}%）")
                s["tasks"][tid] = task
                tasks.append(tid)
            job["tasks"] = tasks
            self._event_locked(
                s, "JOB_SUBMITTED",
                f"预热作业 {jid}：清单 {man['manifest_id']}（{man['source']['version']}）"
                f"→ {len(targets)} 台盒子，截止 {job['deadline']}，"
                f"链路 ≤{link_cap:g} KB/s，最小在线保障 {min_online}/{len(targets)}",
                jid)
            return jid

        jid = self.commit(m)
        with self.lock:
            return {"job": self._job_view(self.state, self.state["jobs"][jid],
                                          idempotent=False),
                    "idempotent": False}

    # ====================================================== job lifecycle ==
    def _get_job(self, s: dict, job_id: str) -> dict:
        job = s["jobs"].get(job_id)
        if not job:
            raise DomainError("PREHEAT_JOB_UNKNOWN", f"作业 {job_id} 不存在", 404)
        return job

    def cancel_job(self, job_id: str, by: str = "dispatcher") -> dict:
        """Verdict-once: re-canceling replays the first cancel result."""
        with self.lock:
            job0 = self._get_job(self.state, job_id)
            if job0["status"] == "canceled":
                return {"job_id": job_id, "status": "canceled",
                        "result": copy.deepcopy(job0["cancel_result"]),
                        "idempotent": True}
            if job0["status"] == "completed":
                raise DomainError("PREHEAT_JOB_CLOSED",
                                  f"作业 {job_id} 已完成，不能取消", 409)

        def m(s):
            job = s["jobs"][job_id]
            released = 0
            for tid in job["tasks"]:
                t = s["tasks"][tid]
                if t["status"] in TASK_OPEN + ("paused",):
                    if t["reserved_bytes"] > EPS:
                        released += 1
                    t["status"] = "canceled"
                    t["reserved_bytes"] = 0.0
                    t["updated_at"] = s["clock"]
                    self._journal(s, t, "TASK_CANCELED",
                                  "作业取消：磁盘预留与传输名额成对释放，"
                                  "部分数据已清除，不留幽灵占用")
            job["status"] = "canceled"
            job["updated_at"] = s["clock"]
            result = {"outcome": "canceled", "by": by, "at": s["clock"],
                      "released_pairs": released}
            job["cancel_result"] = result
            self._event_locked(
                s, "JOB_CANCELED",
                f"作业 {job_id} 已取消：{released} 对磁盘预留/传输名额成对释放", job_id)
            return copy.deepcopy(result)

        result = self.commit(m)
        return {"job_id": job_id, "status": "canceled",
                "result": result, "idempotent": False}

    def pause_job(self, job_id: str) -> dict:
        return self._pause_resume(job_id, pause=True)

    def resume_job(self, job_id: str) -> dict:
        return self._pause_resume(job_id, pause=False)

    def _pause_resume(self, job_id: str, pause: bool) -> dict:
        with self.lock:
            job0 = self._get_job(self.state, job_id)
            want = "paused" if pause else "active"
            if job0["status"] == want:
                return {"job_id": job_id, "status": want, "idempotent": True}
            if job0["status"] in ("canceled", "completed"):
                raise DomainError(
                    "PREHEAT_JOB_CLOSED",
                    f"作业 {job_id} 已终结（{job0['status']}），"
                    f"{'暂停' if pause else '恢复'}被拒绝", 409)

        def m(s):
            job = s["jobs"][job_id]
            job["status"] = "paused" if pause else "active"
            job["updated_at"] = s["clock"]
            for tid in job["tasks"]:
                t = s["tasks"][tid]
                if pause and t["status"] in TASK_OPEN:
                    t["status"] = "paused"
                    t["reserved_bytes"] = 0.0
                    t["updated_at"] = s["clock"]
                    self._journal(s, t, "TASK_PAUSED",
                                  "作业暂停：磁盘预留与传输名额成对释放；"
                                  "恢复时只补剩余数据")
                elif not pause and t["status"] == "paused":
                    t["status"] = "queued"
                    t["updated_at"] = s["clock"]
                    self._journal(s, t, "TASK_RESUMED",
                                  f"作业恢复：重新排队，只补剩余 "
                                  f"{self._remaining(t):.0f} 字节")
            self._event_locked(
                s, "JOB_PAUSED" if pause else "JOB_RESUMED",
                f"作业 {job_id} {'已暂停（名额/预留成对释放）' if pause else '已恢复（只补剩余数据）'}",
                job_id)
            return job["status"]

        status = self.commit(m)
        return {"job_id": job_id, "status": status, "idempotent": False}

    # ============================================================ the plan ==
    def _open_tasks(self, s: dict) -> list:
        return [t for t in s["tasks"].values()
                if t["status"] in TASK_OPEN
                and s["jobs"][t["job_id"]]["status"] == "active"]

    def _want(self, s: dict, t: dict) -> float:
        job = s["jobs"][t["job_id"]]
        box = s["boxes"][t["box_id"]]
        return min(job["link_cap_kbps"], box["link_cap_kbps"])

    @staticmethod
    def _priority(s: dict):
        def key(t):
            job = s["jobs"][t["job_id"]]
            return (-t["debt_kb"], job["deadline"], job["seq"], t["seq"])
        return key

    def _plan(self, s: dict, remaining: Optional[dict] = None) -> dict:
        """Pure scheduling function: {task_id: {grant_kbps, code, message}}.

        Greedy by (debt desc, deadline, seq) within each site; the disk
        reservation and the transfer slot are granted together, never apart.
        """
        plan: dict[str, dict] = {}
        by_site: dict[str, list] = {}
        for t in self._open_tasks(s):
            by_site.setdefault(t["site_id"], []).append(t)
        for site_id, tasks in by_site.items():
            site = s["sites"].get(site_id)
            if site is None:
                continue
            budget = site["capacity_kbps"]
            reserved: dict[str, float] = {}
            for t in sorted(tasks, key=self._priority(s)):
                tid = t["task_id"]
                box = s["boxes"][t["box_id"]]
                rem = (remaining.get(tid, 0.0) if remaining is not None
                       else self._remaining(t))
                if rem <= EPS:
                    plan[tid] = {"grant_kbps": 0.0, "code": "READY",
                                 "message": REASONS["READY"]}
                    continue
                if not box["online"]:
                    plan[tid] = {"grant_kbps": 0.0, "code": "BOX_OFFLINE",
                                 "message": REASONS["BOX_OFFLINE"]}
                    continue
                free = (box["disk_total_bytes"] - box["disk_used_bytes"]
                        - reserved.get(t["box_id"], 0.0))
                if rem > free + EPS:
                    plan[tid] = {"grant_kbps": 0.0, "code": "BOX_DISK_FULL",
                                 "message": REASONS["BOX_DISK_FULL"]}
                    continue
                want = self._want(s, t)
                if want <= EPS:
                    plan[tid] = {"grant_kbps": 0.0, "code": "LINK_DOWN",
                                 "message": REASONS["LINK_DOWN"]}
                    continue
                grant = min(want, budget)
                if grant <= EPS:
                    plan[tid] = {"grant_kbps": 0.0, "code": "SITE_SATURATED",
                                 "message": REASONS["SITE_SATURATED"]}
                    continue
                budget -= grant
                reserved[t["box_id"]] = reserved.get(t["box_id"], 0.0) + rem
                if grant < want - EPS:
                    code = "SITE_SHARED"
                elif box["link_cap_kbps"] < s["jobs"][t["job_id"]]["link_cap_kbps"] - EPS:
                    code = "BOX_LINK"
                else:
                    code = "JOB_CAP"
                plan[tid] = {"grant_kbps": _round6(grant), "code": code,
                             "message": REASONS[code]}
        return plan

    def _settle(self, s: dict):
        """Apply the plan to the state: acquire/release (disk, slot) pairs."""
        # content reuse: unstarted tasks shrink as box inventory grows;
        # a fully reused task completes instantly without any transfer
        changed = True
        while changed:
            changed = False
            for t in sorted(self._open_tasks(s), key=lambda x: x["seq"]):
                if t["bytes_done"] > EPS or t["status"] != "queued":
                    continue
                man = self._manifest_ref(s, t)
                if man is None:
                    continue
                box = s["boxes"][t["box_id"]]
                needed, total = self._needed_bytes(s, man, box)
                if abs(needed - t["bytes_total"]) > EPS:
                    t["bytes_total"] = needed
                    t["reuse_ratio"] = (1.0 - needed / total) if total > EPS else 1.0
                    t["updated_at"] = s["clock"]
                    changed = True
                if needed <= EPS:
                    self._complete_task(s, t, reused=True)
                    changed = True
        plan = self._plan(s)
        for t in self._open_tasks(s):
            p = plan.get(t["task_id"])
            granted = p is not None and p["grant_kbps"] > EPS
            if granted and t["status"] != "transferring":
                t["status"] = "transferring"
                t["reserved_bytes"] = self._remaining(t)
                t["updated_at"] = s["clock"]
                self._journal(s, t, "PAIR_ACQUIRED",
                              f"成对取得磁盘预留 {self._remaining(t):.0f} 字节 + "
                              f"传输名额（{p['grant_kbps']:g} KB/s，{p['message']}）")
            elif granted:
                t["reserved_bytes"] = self._remaining(t)
            elif not granted and t["status"] == "transferring":
                t["status"] = "queued"
                t["reserved_bytes"] = 0.0
                t["updated_at"] = s["clock"]
                self._journal(s, t, "PAIR_RELEASED",
                              f"成对释放磁盘预留与传输名额（{p['message'] if p else '计划外'}）；"
                              f"已传 {t['bytes_done']:.0f} 字节保留，续传只补剩余")
        # job completion
        for job in s["jobs"].values():
            if job["status"] in JOB_OPEN and job.get("tasks") and \
                    all(s["tasks"][tid]["status"] == "done" for tid in job["tasks"]):
                job["status"] = "completed"
                job["updated_at"] = s["clock"]
                self._event_locked(
                    s, "JOB_COMPLETED",
                    f"作业 {job['job_id']} 全部 {len(job['tasks'])} 台盒子预热完成",
                    job["job_id"])

    def _manifest_ref(self, s: dict, t: dict) -> Optional[dict]:
        """Manifest for a task, read-only from the mirror store."""
        if self.mirror is None:
            return None
        job = s["jobs"][t["job_id"]]
        with self.mirror.lock:
            return self.mirror.state.get("manifests", {}).get(job["manifest_id"])

    def _complete_task(self, s: dict, t: dict, reused: bool = False):
        box = s["boxes"][t["box_id"]]
        man = self._manifest_ref(s, t)
        t["status"] = "done"
        t["bytes_done"] = t["bytes_total"]
        t["reserved_bytes"] = 0.0
        t["done_at"] = s["clock"]
        t["updated_at"] = s["clock"]
        box["disk_used_bytes"] += t["bytes_total"]
        box["updated_at"] = s["clock"]
        if man is not None:
            inv = set(box["inventory"])
            inv.update(o["hash"] for o in man["objects"])
            box["inventory"] = sorted(inv)
        self._journal(
            s, t, "TASK_DONE",
            ("全部内容已在盒上（复用率 100%），无需传输即完成" if reused else
             f"传输完成：{t['bytes_total']:.0f} 字节落盘，"
             f"磁盘预留与传输名额成对释放"))
        self._event_locked(
            s, "TASK_DONE",
            f"任务 {t['task_id']}（{t['job_id']}→{t['box_id']}）完成"
            + ("（内容全部复用）" if reused else ""), t["task_id"])

    # ================================================================ tick ==
    def tick(self, seconds: float) -> dict:
        """Advance the simulated clock; transfers progress at granted rates.

        Debt (欠账) accrues for every eligible-but-underserved task, so a
        continuously progressing job can never starve; jobs short of their
        minimum online guarantee accrue double.
        """
        try:
            secs = float(seconds)
        except (TypeError, ValueError):
            raise DomainError("BAD_REQUEST", "tick 秒数必须是数字", 400)
        if secs <= 0 or secs > 86400:
            raise DomainError("BAD_REQUEST", "tick 秒数须在 (0, 86400] 内", 400)

        def m(s):
            plan = self._plan(s)
            s["clock"] = _iso(self._clock(s) + timedelta(seconds=secs))
            # debt accrual per site (fair share = min(want, capacity / contenders))
            by_site: dict[str, list] = {}
            for t in self._open_tasks(s):
                by_site.setdefault(t["site_id"], []).append(t)
            for site_id, tasks in by_site.items():
                site = s["sites"].get(site_id)
                if site is None:
                    continue
                n = len(tasks)
                for t in tasks:
                    job = s["jobs"][t["job_id"]]
                    fair = min(self._want(s, t), site["capacity_kbps"] / n)
                    grant = plan.get(t["task_id"], {}).get("grant_kbps", 0.0)
                    done_n = sum(1 for tid in job["tasks"]
                                 if s["tasks"][tid]["status"] == "done")
                    boost = 2.0 if done_n < job["min_online"] else 1.0
                    t["debt_kb"] = _round6(
                        t["debt_kb"] + (fair - grant) * secs * boost)
            # advance transfers at the granted rate
            progressed, completed = [], []
            for t in self._open_tasks(s):
                p = plan.get(t["task_id"])
                if not p or p["grant_kbps"] <= EPS or t["status"] != "transferring":
                    continue
                add = p["grant_kbps"] * KB * secs
                t["bytes_done"] = min(t["bytes_total"], t["bytes_done"] + add)
                t["updated_at"] = s["clock"]
                progressed.append({"task_id": t["task_id"],
                                   "bytes_done": round(t["bytes_done"], 1),
                                   "bytes_total": round(t["bytes_total"], 1)})
                if self._remaining(t) <= EPS:
                    self._complete_task(s, t)
                    completed.append(t["task_id"])
            return {"clock": s["clock"], "advanced": progressed,
                    "completed": completed}

        return self.commit(m)

    # ================================================================ views ==
    def _predict(self, s: dict) -> dict:
        """Deterministic ETA simulation: {task_id: seconds_from_now | None}.

        Replays the pure plan forward (debts frozen, current eligibility),
        finishing tasks one by one until nothing can progress.
        """
        remaining = {t["task_id"]: self._remaining(t)
                     for t in self._open_tasks(s) if self._remaining(t) > EPS}
        eta: dict[str, Optional[float]] = {}
        horizon = 0.0
        for _ in range(10000):
            if not remaining:
                break
            plan = self._plan(s, remaining)
            active = {tid: p["grant_kbps"] for tid, p in plan.items()
                      if tid in remaining and p["grant_kbps"] > EPS}
            if not active:
                break
            dt = min(remaining[tid] / (g * KB) for tid, g in active.items())
            horizon += dt
            for tid, g in active.items():
                remaining[tid] -= g * KB * dt
                if remaining[tid] <= EPS:
                    eta[tid] = horizon
                    del remaining[tid]
        for tid in [t["task_id"] for t in self._open_tasks(s)]:
            eta.setdefault(tid, None)
        return eta

    def _views(self, s: dict):
        """Compute plan + ETA + ranks once, consistently for all views."""
        plan = self._plan(s)
        eta = self._predict(s)
        ranks = {}
        for i, t in enumerate(sorted(self._open_tasks(s), key=self._priority(s))):
            ranks[t["task_id"]] = i + 1
        return plan, eta, ranks

    def _task_view(self, s: dict, t: dict, plan=None, eta=None, ranks=None) -> dict:
        job = s["jobs"][t["job_id"]]
        v = {"task_id": t["task_id"], "job_id": t["job_id"], "box_id": t["box_id"],
             "site_id": t["site_id"], "status": t["status"],
             "bytes_total": round(t["bytes_total"], 1),
             "bytes_done": round(t["bytes_done"], 1),
             "remaining_bytes": round(self._remaining(t), 1),
             "reserved_bytes": round(t["reserved_bytes"], 1),
             "debt_kb": round(t["debt_kb"], 3),
             "reuse_ratio": round(t["reuse_ratio"], 4),
             "progress": (round(t["bytes_done"] / t["bytes_total"], 4)
                          if t["bytes_total"] > EPS else 1.0),
             "journal": copy.deepcopy(t["journal"]),
             "created_at": t["created_at"], "updated_at": t["updated_at"],
             "done_at": t["done_at"]}
        p = (plan or {}).get(t["task_id"])
        if t["status"] == "paused":
            v["rate_kbps"] = 0.0
            v["limit"] = {"code": "PAUSED", "message": REASONS["PAUSED"]}
        elif t["status"] in TASK_OPEN and p is not None:
            # transferring ⇔ holds the (disk, slot) pair ⇔ granted rate > 0
            v["rate_kbps"] = p["grant_kbps"] if t["status"] == "transferring" else 0.0
            v["limit"] = {"code": p["code"], "message": p["message"]}
        else:
            v["rate_kbps"] = 0.0
            v["limit"] = None
        if ranks is not None and t["task_id"] in ranks:
            v["rank"] = ranks[t["task_id"]]
        secs = (eta or {}).get(t["task_id"])
        if t["status"] == "done":
            v["eta_seconds"] = 0.0
            v["eta"] = t["done_at"]
        elif secs is None:
            v["eta_seconds"] = None
            v["eta"] = None
        else:
            v["eta_seconds"] = _round6(secs)
            v["eta"] = _iso(self._clock(s) + timedelta(seconds=secs))
        if t["status"] in TASK_OPEN + ("paused",):
            v["at_risk"] = bool(v["eta"] and v["eta"] > job["deadline"])
        else:
            v["at_risk"] = False
        return v

    def _job_view(self, s: dict, job: dict, idempotent: bool = False) -> dict:
        plan, eta, ranks = self._views(s)
        tasks = [self._task_view(s, s["tasks"][tid], plan, eta, ranks)
                 for tid in job.get("tasks", [])]
        done = sum(1 for t in tasks if t["status"] == "done")
        total_b = sum(t["bytes_total"] for t in tasks)
        done_b = sum(t["bytes_done"] for t in tasks)
        by_deadline = done + sum(
            1 for t in tasks if t["status"] in TASK_OPEN + ("paused",)
            and t["eta"] is not None and t["eta"] <= job["deadline"])
        v = {"job_id": job["job_id"], "manifest_id": job["manifest_id"],
             "version": job["version"], "targets": list(job["targets"]),
             "deadline": job["deadline"], "link_cap_kbps": job["link_cap_kbps"],
             "min_online": job["min_online"], "note": job["note"],
             "status": job["status"], "created_by": job["created_by"],
             "created_at": job["created_at"], "updated_at": job["updated_at"],
             "progress": {"done_tasks": done, "total_tasks": len(tasks),
                          "done_bytes": round(done_b, 1),
                          "total_bytes": round(total_b, 1)},
             "guarantee": {"required": job["min_online"], "done": done,
                           "predicted_by_deadline": by_deadline,
                           "at_risk": by_deadline < job["min_online"]},
             "tasks": tasks, "idempotent": idempotent}
        if job["cancel_result"]:
            v["cancel_result"] = copy.deepcopy(job["cancel_result"])
        return v

    def queue(self) -> dict:
        """The scheduling queue: identical order/reasons/ETAs for web and CLI."""
        with self.lock:
            s = self.state
            plan, eta, ranks = self._views(s)
            open_tasks = sorted(self._open_tasks(s), key=self._priority(s))
            resting = sorted(
                (t for t in s["tasks"].values()
                 if t["status"] in ("paused", "done", "canceled")),
                key=lambda t: t["seq"], reverse=True)
            site_views = []
            for site in sorted(s["sites"].values(), key=lambda x: x["site_id"]):
                tids = [t["task_id"] for t in self._open_tasks(s)
                        if t["site_id"] == site["site_id"]]
                used = sum(plan.get(tid, {}).get("grant_kbps", 0.0) for tid in tids)
                site_views.append({
                    "site_id": site["site_id"],
                    "capacity_kbps": site["capacity_kbps"],
                    "granted_kbps": _round6(used),
                    "free_kbps": _round6(site["capacity_kbps"] - used),
                    "open_tasks": len(tids)})
            return {"clock": s["clock"],
                    "sites": site_views,
                    "queue": [self._task_view(s, t, plan, eta, ranks)
                              for t in open_tasks],
                    "inactive": [self._task_view(s, t, plan, eta, ranks)
                                 for t in resting]}

    def list_jobs(self) -> list:
        with self.lock:
            return [self._job_view(self.state, j)
                    for j in sorted(self.state["jobs"].values(),
                                    key=lambda j: j["seq"], reverse=True)]

    def get_job(self, job_id: str) -> dict:
        with self.lock:
            return self._job_view(self.state, self._get_job(self.state, job_id))

    def list_sites(self) -> list:
        with self.lock:
            plan = self._plan(self.state)
            out = []
            for site in sorted(self.state["sites"].values(),
                               key=lambda x: x["site_id"]):
                tids = [t["task_id"] for t in self._open_tasks(self.state)
                        if t["site_id"] == site["site_id"]]
                used = sum(plan.get(tid, {}).get("grant_kbps", 0.0) for tid in tids)
                boxes = [b for b in self.state["boxes"].values()
                         if b["site_id"] == site["site_id"]]
                out.append({"site_id": site["site_id"],
                            "capacity_kbps": site["capacity_kbps"],
                            "granted_kbps": _round6(used),
                            "boxes": len(boxes),
                            "online": sum(1 for b in boxes if b["online"]),
                            "created_at": site["created_at"]})
            return out

    def list_boxes(self) -> list:
        with self.lock:
            return [self._box_view(self.state, b)
                    for b in sorted(self.state["boxes"].values(),
                                    key=lambda b: b["box_id"])]

    def get_site(self, site_id: str) -> dict:
        with self.lock:
            site = self.state["sites"].get(site_id)
            if not site:
                raise DomainError("PREHEAT_SITE_UNKNOWN",
                                  f"站点 {site_id} 未注册", 404)
            return next(x for x in self.list_sites() if x["site_id"] == site_id)

    def get_box(self, box_id: str) -> dict:
        with self.lock:
            box = self.state["boxes"].get(box_id)
            if not box:
                raise DomainError("PREHEAT_BOX_UNKNOWN",
                                  f"盒子 {box_id} 未注册", 404)
            return self._box_view(self.state, box)

    def _box_view(self, s: dict, b: dict) -> dict:
        reserved = sum(t["reserved_bytes"] for t in s["tasks"].values()
                       if t["box_id"] == b["box_id"] and t["status"] == "transferring")
        v = copy.deepcopy(b)
        v["reserved_bytes"] = round(reserved, 1)
        v["free_bytes"] = round(b["disk_total_bytes"] - b["disk_used_bytes"]
                                - reserved, 1)
        v["disk_total_mb"] = _round6(b["disk_total_bytes"] / 1e6)
        v["disk_used_mb"] = _round6(b["disk_used_bytes"] / 1e6)
        v["reserved_mb"] = _round6(reserved / 1e6)
        v["inventory_objects"] = len(b["inventory"])
        return v

    # ======================================================== invariants ==
    def check_invariants(self) -> list:
        """Return a list of invariant violations (empty means healthy)."""
        with self.lock:
            s = self.state
            bad = []
            plan = self._plan(s)
            # 1. site bandwidth never exceeded
            for site in s["sites"].values():
                used = sum(p["grant_kbps"] for tid, p in plan.items()
                           if s["tasks"][tid]["site_id"] == site["site_id"])
                if used > site["capacity_kbps"] + EPS:
                    bad.append(f"站点 {site['site_id']} 超带宽: "
                               f"{used} > {site['capacity_kbps']}")
            # 2. paired acquire/release: transferring ⇔ reservation held
            for t in s["tasks"].values():
                if t["status"] == "transferring":
                    if t["reserved_bytes"] <= EPS < self._remaining(t):
                        bad.append(f"任务 {t['task_id']} 传输中但无磁盘预留")
                elif t["reserved_bytes"] > EPS:
                    bad.append(f"任务 {t['task_id']} 非传输中却持有预留（幽灵占用）")
            # 3. box disk never exceeded (used + reservations ≤ total)
            for b in s["boxes"].values():
                reserved = sum(t["reserved_bytes"] for t in s["tasks"].values()
                               if t["box_id"] == b["box_id"]
                               and t["status"] == "transferring")
                if b["disk_used_bytes"] + reserved > b["disk_total_bytes"] + EPS:
                    bad.append(f"盒子 {b['box_id']} 磁盘超占: "
                               f"{b['disk_used_bytes']}+{reserved} > "
                               f"{b['disk_total_bytes']}")
            return bad
