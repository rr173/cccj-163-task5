"""
Roadside-box map mirror repository (STRICTLY READ-ONLY over the live map service).

A publisher picks an *effective* map snapshot on the map service (the current
effective version, or a pinned trip snapshot) plus a set of roadside compute
boxes, and cuts an **immutable manifest**: the version descriptor and tile
objects are packed into compressed (zlib) object chunks, each chunk is
sha256-addressed, and the ordered chunk hashes roll up to one whole-package
root hash.

Device agents pull chunks, verify every chunk hash and then the package root
hash before the image is *staged* on the box:

  * pulls resume after network drops; an already-confirmed chunk is never
    counted twice (ack is idempotent);
  * competing publishes for the same box allow exactly ONE pending-switch
    image; an older generation or a corrupt object can never overwrite the
    current image;
  * the box flips its pointer atomically and only after every object has been
    re-verified; any failure before the flip leaves the previous image running;
  * every state (pulling / ready-to-switch / switched / failed / rejected) and
    every processing result is persisted, so a restart loses nothing;
  * creating/dispatching/switching is verdict-once: retrying the same manifest
    returns the first task and the first switch result.

Like the audit subsystem, this engine only ever READS the live map state (deep
copy under its lock) and persists exclusively to its own store
(data/mirror.json). It never moves region generations, never touches patches,
and never rewrites issued trip snapshots.
"""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
import threading
import zlib
from typing import Any, Optional

from engine import DomainError, canonical, sha256, short, utcnow_iso

CHUNK_OBJECTS = 2                 # objects per compressed chunk (deterministic)
TASK_OPEN = ("pulling", "ready")  # statuses occupying the box's pending slot


def sha256_bytes(b: bytes) -> str:
    return "sha256:" + hashlib.sha256(b).hexdigest()


def _b64e(b: bytes) -> str:
    return base64.b64encode(b).decode("ascii")


def _b64d(s: str) -> bytes:
    return base64.b64decode(s.encode("ascii"))


def root_hash_of(chunk_hashes: list) -> str:
    """Whole-package root hash over the ordered chunk hashes (整包根散列)."""
    return sha256({"chunks": list(chunk_hashes)})


class MirrorEngine:
    def __init__(self, path: str, live):
        self.path = path
        self.live = live                      # MapEngine, used READ-ONLY
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
            self._persist_locked()

    @staticmethod
    def _blank() -> dict:
        return {"manifests": {},        # manifest_id -> immutable manifest
                "manifest_by_hash": {},  # manifest_hash -> manifest_id (idempotency)
                "chunk_store": {},      # "mhash:index" -> {hash, data_b64}
                "tasks": {},            # task_id -> device task
                "task_by_key": {},      # "mhash|device" -> task_id (verdict-once)
                "devices": {},          # device_id -> box record
                "events": [],           # mirror-side append-only journal
                "seq": {"manifest": 0, "task": 0, "event": 0}}

    def _persist_locked(self):
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        tmp = f"{self.path}.tmp.{os.getpid()}.{threading.get_ident()}"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.state, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)

    def commit(self, mutate) -> Any:
        """Run mutate(copy) then atomically persist; all-or-nothing."""
        with self.lock:
            draft = copy.deepcopy(self.state)
            result = mutate(draft)
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            tmp = f"{self.path}.tmp.{os.getpid()}.{threading.get_ident()}"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(draft, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
            self.state = draft
            return result

    def _live_snapshot(self) -> dict:
        """Deep copy of live map state under its lock. Never mutated, never saved."""
        with self.live.lock:
            return copy.deepcopy(self.live.state)

    def _next(self, s: dict, key: str, prefix: str) -> str:
        s["seq"][key] += 1
        return f"{prefix}-{s['seq'][key]:04d}"

    def now(self) -> str:
        return utcnow_iso()

    # ============================================================= journal ==
    def _event_locked(self, s: dict, etype: str, summary: str, ref: str) -> dict:
        s["seq"]["event"] += 1
        ev = {"event_id": s["seq"]["event"], "ts": self.now(),
              "type": etype, "summary": summary, "ref": ref}
        s["events"].append(ev)
        return ev

    def _journal(self, task: dict, event: str, detail: str):
        task["journal"].append({"ts": self.now(), "event": event, "detail": detail})

    def list_events(self, limit: int = 80) -> list:
        with self.lock:
            return copy.deepcopy(self.state["events"][-limit:][::-1])

    # ================================================== manifest building ==
    def _resolve_source(self, body: dict, live: dict):
        """Read-only: build (source, generations, descriptor, tile_hashes)."""
        src = body.get("source") or {}
        snapshot_id = src.get("snapshot_id") or body.get("snapshot_id")
        if snapshot_id or src.get("kind") == "snapshot":
            snap = live["snapshots"].get(snapshot_id or "")
            if not snap:
                raise DomainError("MIRROR_SOURCE_NOT_FOUND",
                                  f"行程快照 {snapshot_id} 不存在", 404)
            descriptor = {"version": snap["version"]["version"],
                          "base": snap["version"]["base"],
                          "tiles": snap["tiles"],
                          "patches": snap["version"]["patches"],
                          "overlays": snap["overlays"]}
            source = {"kind": "snapshot", "ref": snapshot_id,
                      "version": snap["version"]["version"]}
        else:
            eff = self.live.resolve_effective(live)  # pure read, no mutation
            descriptor = {"version": eff["version"], "base": eff["base"],
                          "tiles": eff["tiles"], "patches": eff["patches"],
                          "overlays": eff["overlays"]}
            source = {"kind": "effective", "ref": None, "version": eff["version"]}
        generations = {r: b["generation"] for r, b in descriptor["base"].items()}
        return source, generations, descriptor, dict(descriptor["tiles"])

    def _build_package(self, live: dict, source: dict, generations: dict,
                       descriptor: dict, tile_hashes: dict):
        """Deterministically pack objects into compressed, hashed chunks."""
        objects = [{"object_id": "descriptor", "kind": "descriptor",
                    "hash": sha256(descriptor), "content": descriptor}]
        for tid, h in sorted(tile_hashes.items()):
            tile = live["tiles"].get(h)
            if tile is None:
                raise DomainError("TILE_MISSING",
                                  f"瓦片 {tid} 内容 {short(h)} 不在线上瓦片库", 409)
            objects.append({"object_id": f"tile:{tid}", "kind": "tile",
                            "hash": h, "content": tile})
        chunks, blobs = [], {}
        for i in range(0, len(objects), CHUNK_OBJECTS):
            group = objects[i:i + CHUNK_OBJECTS]
            idx = len(chunks)
            payload = {"index": idx,
                       "objects": [{"object_id": o["object_id"], "kind": o["kind"],
                                    "hash": o["hash"], "content": o["content"]}
                                   for o in group]}
            blob = zlib.compress(canonical(payload), 9)
            chunks.append({"index": idx, "hash": sha256_bytes(blob),
                           "size": len(blob),
                           "objects": [o["object_id"] for o in group]})
            blobs[idx] = blob
        meta_objects = [{"object_id": o["object_id"], "kind": o["kind"],
                         "hash": o["hash"]} for o in objects]
        return meta_objects, chunks, blobs

    @staticmethod
    def _norm_devices(devices: Any) -> list:
        if not isinstance(devices, list) or not devices or \
                not all(isinstance(d, str) and d.strip() for d in devices):
            raise DomainError("BAD_REQUEST", "devices 必须是非空设备编号数组", 400)
        return list(dict.fromkeys(d.strip() for d in devices))

    def create_manifest(self, body: dict) -> dict:
        """Cut an immutable manifest from a live effective snapshot + dispatch.

        Idempotent by content: an identical request replays the first manifest
        and its first tasks (重试同一清单返回第一次任务).
        """
        devices = self._norm_devices(body.get("devices"))
        note = body.get("note", "")
        created_by = body.get("created_by", "publisher")

        live = self._live_snapshot()
        source, generations, descriptor, tile_hashes = self._resolve_source(body, live)
        meta_objects, chunks, blobs = self._build_package(
            live, source, generations, descriptor, tile_hashes)
        root = root_hash_of([c["hash"] for c in chunks])
        content = {"source": source, "generations": generations,
                   "objects": meta_objects, "chunks": chunks, "root_hash": root,
                   "devices": devices, "note": note}
        mhash = sha256(content)

        with self.lock:
            existing = self.state["manifest_by_hash"].get(mhash)
            if existing:
                man = self.state["manifests"][existing]
                tasks = [self._task_view(self.state, t, idempotent=True)
                         for t in self.state["tasks"].values()
                         if t["manifest_hash"] == mhash]
                tasks.sort(key=lambda t: t["task_id"])
                return {"manifest": copy.deepcopy(man), "tasks": tasks,
                        "idempotent": True}

        def m(s):
            mid = self._next(s, "manifest", "MF")
            manifest = dict(content, manifest_id=mid, manifest_hash=mhash,
                            seq=s["seq"]["manifest"], created_by=created_by,
                            created_at=self.now())
            s["manifests"][mid] = manifest
            s["manifest_by_hash"][mhash] = mid
            for idx, blob in blobs.items():
                s["chunk_store"][f"{mhash}:{idx}"] = {
                    "hash": chunks[idx]["hash"], "data_b64": _b64e(blob)}
            tasks = [self._dispatch_locked(s, manifest, did, created_by)
                     for did in devices]
            self._event_locked(
                s, "MANIFEST_CREATED",
                f"生成不可变清单 {mid}（{source['version']}，{len(meta_objects)} 对象 / "
                f"{len(chunks)} 压缩块，根散列 {short(root)}，设备 {','.join(devices)}）",
                mid)
            return {"manifest": copy.deepcopy(manifest), "tasks": tasks,
                    "idempotent": False}

        return self.commit(m)

    def dispatch_manifest(self, manifest_id: str, devices: Any,
                          created_by: str = "publisher") -> dict:
        """Dispatch an existing manifest to more boxes (verdict-once per box)."""
        devices = self._norm_devices(devices)
        with self.lock:
            if manifest_id not in self.state["manifests"]:
                raise DomainError("NOT_FOUND", f"清单 {manifest_id} 不存在", 404)

        def m(s):
            man = s["manifests"][manifest_id]
            tasks = [self._dispatch_locked(s, man, did, created_by)
                     for did in devices]
            return {"manifest_id": manifest_id, "tasks": tasks}

        return self.commit(m)

    # =========================================================== dispatch ==
    @staticmethod
    def _stale_reason(manifest: dict, current: dict) -> Optional[str]:
        """Why this manifest must not overwrite the box's current image."""
        mg = manifest["generations"]
        for region, g in current["generations"].items():
            if mg.get(region, 0) < g:
                return (f"清单中区域 {region} 为第 {mg.get(region, 0)} 代，"
                        f"落后于盒子当前镜像的第 {g} 代")
        if manifest["source"]["version"] == current["version"]:
            return f"清单版本 {manifest['source']['version']} 与盒子当前镜像相同，并非更新镜像"
        if mg == current["generations"] and \
                manifest["seq"] <= current.get("manifest_seq", 0):
            return (f"同一代次的更早发布（清单 {manifest['manifest_id']} 不晚于当前镜像 "
                    f"{current['manifest_id']}），禁止回退")
        return None

    def _dispatch_locked(self, s: dict, manifest: dict, did: str, by: str) -> dict:
        """Verdict-once per (manifest, box): first decision always stands."""
        mhash = manifest["manifest_hash"]
        key = f"{mhash}|{did}"
        existing = s["task_by_key"].get(key)
        if existing:
            return self._task_view(s, s["tasks"][existing], idempotent=True)

        dev = s["devices"].setdefault(did, {
            "device_id": did, "current_image": None, "pending_task_id": None,
            "tasks": [], "updated_at": self.now()})
        tid = self._next(s, "task", "MT")
        task = {
            "task_id": tid, "manifest_id": manifest["manifest_id"],
            "manifest_hash": mhash, "device_id": did,
            "version": manifest["source"]["version"],
            "generations": dict(manifest["generations"]),
            "status": "pulling",
            "progress": {"total": len(manifest["chunks"]), "confirmed": 0},
            "chunks": {},               # "index" -> {status, hash, acked_at, attempts}
            "staged": None,             # set when the whole package verifies
            "rejection": None,          # 拒绝理由（与网页/CLI 同一数据源）
            "result": None,             # 每次处理结果（切换/失败）
            "journal": [],
            "created_at": self.now(), "updated_at": self.now(),
        }
        rejection = None
        pend = dev.get("pending_task_id")
        if pend and s["tasks"][pend]["status"] in TASK_OPEN:
            other = s["tasks"][pend]
            rejection = ("MIRROR_PENDING_CONFLICT",
                         f"盒子已有待切换镜像任务 {pend}（{other['version']}），"
                         f"同一时刻只允许一个待切换镜像；本次分发被拒绝")
        elif dev.get("current_image"):
            reason = self._stale_reason(manifest, dev["current_image"])
            if reason:
                rejection = ("MIRROR_STALE_GENERATION", reason)

        if rejection:
            code, message = rejection
            task["status"] = "rejected"
            task["rejection"] = {"code": code, "message": message, "at": self.now()}
            task["result"] = {"outcome": "rejected", "code": code,
                              "message": message, "at": self.now()}
            self._journal(task, "TASK_REJECTED", f"[{code}] {message}")
            self._event_locked(s, "TASK_REJECTED",
                               f"任务 {tid}（{manifest['manifest_id']}→{did}）被拒绝：{message}",
                               tid)
        else:
            dev["pending_task_id"] = tid
            self._journal(task, "TASK_DISPATCHED",
                          f"清单 {manifest['manifest_id']} 已分发，待拉取 "
                          f"{task['progress']['total']} 个压缩对象块")
            self._event_locked(
                s, "TASK_DISPATCHED",
                f"任务 {tid}：清单 {manifest['manifest_id']}（{task['version']}）"
                f"分发至盒子 {did}", tid)
        dev["tasks"].append(tid)
        dev["updated_at"] = self.now()
        s["tasks"][tid] = task
        s["task_by_key"][key] = tid
        return self._task_view(s, task, idempotent=False)

    # ============================================================== views ==
    def _task_view(self, s: dict, t: dict, idempotent: bool = False) -> dict:
        v = copy.deepcopy(t)
        v["idempotent"] = idempotent
        return v

    def list_manifests(self) -> list:
        with self.lock:
            out = []
            for man in self.state["manifests"].values():
                v = {k: copy.deepcopy(man[k]) for k in
                     ("manifest_id", "manifest_hash", "source", "generations",
                      "devices", "note", "root_hash", "created_by", "created_at")}
                v["objects"] = len(man["objects"])
                v["chunks"] = len(man["chunks"])
                out.append(v)
            return sorted(out, key=lambda x: x["manifest_id"], reverse=True)

    def get_manifest(self, mid: str) -> dict:
        with self.lock:
            man = self.state["manifests"].get(mid)
            if not man:
                raise DomainError("NOT_FOUND", f"清单 {mid} 不存在", 404)
            v = copy.deepcopy(man)
            v["tasks"] = sorted(
                (self._task_view(self.state, t) for t in self.state["tasks"].values()
                 if t["manifest_hash"] == man["manifest_hash"]),
                key=lambda t: t["task_id"])
            return v

    def list_tasks(self, device: Optional[str] = None) -> list:
        with self.lock:
            return sorted((self._task_view(self.state, t)
                           for t in self.state["tasks"].values()
                           if device is None or t["device_id"] == device),
                          key=lambda t: t["task_id"], reverse=True)

    def get_task(self, tid: str) -> dict:
        with self.lock:
            t = self.state["tasks"].get(tid)
            if not t:
                raise DomainError("NOT_FOUND", f"任务 {tid} 不存在", 404)
            v = self._task_view(self.state, t)
            man = self.state["manifests"][t["manifest_id"]]
            v["manifest"] = {"manifest_id": man["manifest_id"],
                             "root_hash": man["root_hash"],
                             "objects": copy.deepcopy(man["objects"]),
                             "chunks": copy.deepcopy(man["chunks"])}
            return v

    def list_devices(self) -> list:
        with self.lock:
            return sorted((self._device_view(self.state, d)
                           for d in self.state["devices"].values()),
                          key=lambda d: d["device_id"])

    def get_device(self, did: str) -> dict:
        with self.lock:
            d = self.state["devices"].get(did)
            if not d:
                raise DomainError("NOT_FOUND", f"盒子 {did} 不存在", 404)
            return self._device_view(self.state, d, full=True)

    def _device_view(self, s: dict, d: dict, full: bool = False) -> dict:
        v = copy.deepcopy(d)
        pend = d.get("pending_task_id")
        v["pending_task"] = self._task_view(s, s["tasks"][pend]) \
            if pend and pend in s["tasks"] else None
        if full:
            v["tasks"] = [self._task_view(s, s["tasks"][tid])
                          for tid in d["tasks"] if tid in s["tasks"]]
        return v

    # ======================================================== chunk fetch ==
    def get_chunk_payload(self, tid: str, index: int) -> dict:
        """Repository side of a device pull: the compressed object chunk."""
        with self.lock:
            t = self.state["tasks"].get(tid)
            if not t:
                raise DomainError("NOT_FOUND", f"任务 {tid} 不存在", 404)
            man = self.state["manifests"][t["manifest_id"]]
            cmeta = next((c for c in man["chunks"] if c["index"] == index), None)
            if cmeta is None:
                raise DomainError("MIRROR_CHUNK_UNKNOWN",
                                  f"清单 {man['manifest_id']} 没有第 {index} 块", 404)
            rec = self.state["chunk_store"].get(f"{t['manifest_hash']}:{index}")
            if rec is None:
                raise DomainError("MIRROR_OBJECT_CORRUPT",
                                  f"仓库缺少对象块 {index}", 409)
            return {"task_id": tid, "index": index, "hash": cmeta["hash"],
                    "size": cmeta["size"], "data_b64": rec["data_b64"]}

    # ================================================================ ack ==
    def ack_chunk(self, tid: str, index: int, presented_hash: str) -> dict:
        """Device confirms a pulled chunk by the hash it computed.

        Idempotent per chunk (已确认块不重复计数). A hash that contradicts the
        manifest is recorded as the rejection reason; if the repository's own
        bytes fail integrity the object is corrupt and the task fails for good
        (坏对象不能覆盖当前镜像). When the last chunk confirms, the whole
        package (every blob + root hash) is verified before staging.
        """
        with self.lock:
            if tid not in self.state["tasks"]:
                raise DomainError("NOT_FOUND", f"任务 {tid} 不存在", 404)

        def m(s):
            t = s["tasks"][tid]
            if t["status"] == "active":
                return {"task_id": tid, "chunk_index": index, "accepted": True,
                        "idempotent": True, "task": self._task_view(s, t)}
            if t["status"] in ("failed", "rejected"):
                raise DomainError(
                    "MIRROR_TASK_CLOSED",
                    f"任务 {tid} 已终结（{t['status']}），首次判定保持不变："
                    f"{(t['rejection'] or {}).get('message', '')}",
                    409, {"rejection": t["rejection"]})
            man = s["manifests"][t["manifest_id"]]
            cmeta = next((c for c in man["chunks"] if c["index"] == index), None)
            if cmeta is None:
                raise DomainError("MIRROR_CHUNK_UNKNOWN",
                                  f"清单 {man['manifest_id']} 没有第 {index} 块", 404)
            ck = str(index)
            prev = t["chunks"].get(ck, {})
            if prev.get("status") == "confirmed":
                # already acknowledged: never double-count a confirmed chunk
                return {"task_id": tid, "chunk_index": index, "accepted": True,
                        "idempotent": True, "task": self._task_view(s, t)}
            attempts = prev.get("attempts", 0) + 1

            if presented_hash != cmeta["hash"]:
                rec = s["chunk_store"].get(f"{t['manifest_hash']}:{index}")
                repo_ok = bool(rec) and sha256_bytes(_b64d(rec["data_b64"])) == cmeta["hash"]
                t["chunks"][ck] = {**prev, "attempts": attempts,
                                   "last_error": "MIRROR_CHUNK_HASH_MISMATCH"}
                if repo_ok:
                    # transit corruption: retryable, task keeps pulling
                    msg = (f"第 {index} 块散列与清单声明不符（设备算得 "
                           f"{short(presented_hash)}，清单声明 {short(cmeta['hash'])}）；"
                           f"判定为传输损坏，可重新拉取")
                    self._journal(t, "CHUNK_REJECTED", msg)
                    self._event_locked(s, "CHUNK_REJECTED",
                                       f"任务 {tid} {msg}", tid)
                    t["updated_at"] = self.now()
                    return {"task_id": tid, "chunk_index": index, "accepted": False,
                            "retryable": True, "idempotent": False,
                            "rejection": {"code": "MIRROR_CHUNK_HASH_MISMATCH",
                                          "message": msg},
                            "task": self._task_view(s, t)}
                # repository object itself is corrupt: permanent failure
                self._fail_task_locked(
                    s, t, "MIRROR_OBJECT_CORRUPT",
                    f"对象块 {index} 在镜像仓库中的内容已损坏"
                    f"（声明 {short(cmeta['hash'])}），任务失败；"
                    f"盒子当前镜像保持不动")
                return {"task_id": tid, "chunk_index": index, "accepted": False,
                        "retryable": False, "idempotent": False,
                        "rejection": copy.deepcopy(t["rejection"]),
                        "task": self._task_view(s, t)}

            t["chunks"][ck] = {"status": "confirmed", "hash": presented_hash,
                               "acked_at": self.now(), "attempts": attempts}
            t["progress"]["confirmed"] += 1
            self._journal(t, "CHUNK_CONFIRMED",
                          f"第 {index} 块校验通过并暂存（{short(presented_hash)}，"
                          f"第 {attempts} 次尝试）")
            self._event_locked(
                s, "CHUNK_CONFIRMED",
                f"任务 {tid} 第 {index} 块确认（{t['progress']['confirmed']}/"
                f"{t['progress']['total']}）", tid)

            if t["progress"]["confirmed"] == t["progress"]["total"]:
                rej = self._verify_package_locked(s, man)
                if rej:
                    self._fail_task_locked(s, t, rej["code"], rej["message"])
                else:
                    t["status"] = "ready"
                    t["staged"] = {"root_hash": man["root_hash"],
                                   "staged_at": self.now(),
                                   "objects": [o["object_id"] for o in man["objects"]]}
                    self._journal(
                        t, "TASK_STAGED",
                        f"全部 {t['progress']['total']} 块确认，整包根散列 "
                        f"{short(man['root_hash'])} 复核一致，镜像已暂存、可切换")
                    self._event_locked(
                        s, "TASK_STAGED",
                        f"任务 {tid} 全部对象验真，镜像已暂存（根散列 "
                        f"{short(man['root_hash'])}），等待原子切换", tid)
            t["updated_at"] = self.now()
            return {"task_id": tid, "chunk_index": index, "accepted": True,
                    "idempotent": False, "task": self._task_view(s, t)}

        return self.commit(m)

    def _verify_package_locked(self, s: dict, manifest: dict) -> Optional[dict]:
        """Re-hash every repository blob and the whole-package root hash."""
        mhash = manifest["manifest_hash"]
        for c in manifest["chunks"]:
            rec = s["chunk_store"].get(f"{mhash}:{c['index']}")
            if rec is None or sha256_bytes(_b64d(rec["data_b64"])) != c["hash"]:
                return {"code": "MIRROR_OBJECT_CORRUPT",
                        "message": f"整包验真失败：对象块 {c['index']} 在仓库中的内容"
                                   f"与清单声明 {short(c['hash'])} 不符；"
                                   f"盒子当前镜像保持不动"}
        if root_hash_of([c["hash"] for c in manifest["chunks"]]) != manifest["root_hash"]:
            return {"code": "MIRROR_ROOT_HASH_MISMATCH",
                    "message": "整包根散列复核不一致，暂存被拒绝；盒子当前镜像保持不动"}
        return None

    def _fail_task_locked(self, s: dict, t: dict, code: str, message: str):
        t["status"] = "failed"
        t["rejection"] = {"code": code, "message": message, "at": self.now()}
        t["result"] = {"outcome": "failed", "code": code,
                       "message": message, "at": self.now()}
        t["updated_at"] = self.now()
        dev = s["devices"][t["device_id"]]
        if dev.get("pending_task_id") == t["task_id"]:
            dev["pending_task_id"] = None
        dev["updated_at"] = self.now()
        self._journal(t, "TASK_FAILED", f"[{code}] {message}")
        self._event_locked(s, "TASK_FAILED",
                           f"任务 {t['task_id']} 失败：[{code}] {message}",
                           t["task_id"])

    # ============================================================= switch ==
    def switch_task(self, tid: str) -> dict:
        """Atomic pointer flip — only after every object verifies.

        Verdict-once: re-switching a closed task returns the first result.
        """
        with self.lock:
            t0 = self.state["tasks"].get(tid)
            if not t0:
                raise DomainError("NOT_FOUND", f"任务 {tid} 不存在", 404)
            if t0["status"] != "pulling" and t0["status"] != "ready":
                return self._switch_view(t0, idempotent=True)
            if t0["status"] == "pulling":
                raise DomainError(
                    "MIRROR_NOT_READY",
                    f"任务 {tid} 仍在拉取中（{t0['progress']['confirmed']}/"
                    f"{t0['progress']['total']} 块）；全部对象验真前禁止切换",
                    409, {"progress": copy.deepcopy(t0["progress"])})

        def m(s):
            t = s["tasks"][tid]
            man = s["manifests"][t["manifest_id"]]
            dev = s["devices"][t["device_id"]]
            # re-verify the ENTIRE package immediately before the flip
            rej = self._verify_package_locked(s, man)
            if rej is None and t["progress"]["confirmed"] != t["progress"]["total"]:
                rej = {"code": "MIRROR_NOT_READY",
                       "message": "尚有对象块未确认，禁止切换"}
            if rej is None and dev.get("current_image"):
                reason = self._stale_reason(man, dev["current_image"])
                if reason:
                    rej = {"code": "MIRROR_STALE_GENERATION", "message": reason}
            if rej:
                self._fail_task_locked(s, t, rej["code"], rej["message"])
                return self._switch_view(t, idempotent=False)

            prev = dev.get("current_image")
            image = {"manifest_id": man["manifest_id"],
                     "manifest_hash": man["manifest_hash"],
                     "manifest_seq": man["seq"],
                     "version": man["source"]["version"],
                     "generations": dict(man["generations"]),
                     "root_hash": man["root_hash"],
                     "task_id": tid, "switched_at": self.now()}
            # the atomic pointer flip: image + task state + journal in one commit
            dev["current_image"] = image
            dev["pending_task_id"] = None
            dev["updated_at"] = self.now()
            t["status"] = "active"
            t["result"] = {"outcome": "switched", "switched_at": image["switched_at"],
                           "previous_image": copy.deepcopy(prev),
                           "new_image": copy.deepcopy(image)}
            t["updated_at"] = self.now()
            self._journal(
                t, "TASK_SWITCHED",
                f"全部对象验真通过，原子换指针：{man['source']['version']} 已生效"
                + (f"（替换 {prev['version']}）" if prev else "（首个镜像）"))
            self._event_locked(
                s, "TASK_SWITCHED",
                f"盒子 {t['device_id']} 原子切换至 {man['source']['version']}"
                f"（清单 {man['manifest_id']}，根散列 {short(man['root_hash'])}）", tid)
            return self._switch_view(t, idempotent=False)

        return self.commit(m)

    def _switch_view(self, t: dict, idempotent: bool) -> dict:
        return {"task_id": t["task_id"], "device_id": t["device_id"],
                "status": t["status"], "switched": t["status"] == "active",
                "result": copy.deepcopy(t["result"]),
                "rejection": copy.deepcopy(t["rejection"]),
                "idempotent": idempotent}

    # ===================================================== device agent ====
    def run_agent(self, tid: str, stop_after: Optional[int] = None,
                  corrupt_chunk: Optional[int] = None) -> dict:
        """Simulate one device-agent run: pull → verify → ack, in order.

        ``stop_after`` drops the 'network' after N newly confirmed chunks;
        ``corrupt_chunk`` flips the bytes of one chunk in transit. Both the
        web console and tests drive the real pull/ack path through this.
        """
        with self.lock:
            if tid not in self.state["tasks"]:
                raise DomainError("NOT_FOUND", f"任务 {tid} 不存在", 404)
        results, newly = [], 0
        while True:
            task = self.get_task(tid)
            if task["status"] != "pulling":
                break
            pending = [c for c in task["manifest"]["chunks"]
                       if task["chunks"].get(str(c["index"]), {}).get("status")
                       != "confirmed"]
            if not pending:
                break
            c = pending[0]
            idx = c["index"]
            payload = self.get_chunk_payload(tid, idx)
            blob = _b64d(payload["data_b64"])
            if corrupt_chunk is not None and idx == corrupt_chunk:
                blob = blob[:-1] + bytes([blob[-1] ^ 0xFF])  # transit corruption
            computed = sha256_bytes(blob)
            outcome = self.ack_chunk(tid, idx, computed)
            results.append({"index": idx, "computed_hash": computed,
                            "accepted": outcome["accepted"],
                            "retryable": outcome.get("retryable"),
                            "rejection": outcome.get("rejection")})
            if not outcome["accepted"]:
                break  # device stops this run; a later run resumes/retries
            newly += 1
            if stop_after is not None and newly >= stop_after:
                results.append({"interrupted": True,
                                "message": f"网络中断：已确认 {newly} 块，连接断开"})
                break
        return {"task_id": tid, "results": results, "task": self.get_task(tid)}
