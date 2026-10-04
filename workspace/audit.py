"""
Regulatory-audit temporal-evidence engine (STRICTLY READ-ONLY over the live system).

An auditor-facing, append-only evidence subsystem that is physically separated
from the operational map engine:

  * Evidence is an operator-imported chain of signed vehicle running events
    (sequence number / source / payload digest / parent digest).
  * Import is verdict-once: sequence gaps, duplicate events, rewritten digests
    and missing parents are REJECTED; the same import key always returns the
    first verdict. Long chains can resume from a persisted head checkpoint.
  * A query reconstructs the spatial state at a given moment (by event sequence
    or timestamp): version descriptor, tile digests and driving steps, with
    every tile digest re-verified against the content-addressed tile store.
  * Confirmed checkpoints can seed isolated branches that each run exactly one
    counterfactual: a version switch (to an existing published release) or a
    road-closure revocation (drop one pinned patch).
  * Queries, branches and exports only append *audit archives*. They never
    touch live generations, vehicle records, snapshots, patches or the original
    imported event chain.

Persistence is a separate file (data/audit.json) written atomically; the live
store (data/mapstate.json) is only ever read here.
"""

from __future__ import annotations

import copy
import json
import os
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from engine import (
    DomainError,
    build_graph,
    dijkstra,
    node_key,
    sha256,
    short,
    utcnow_iso,
    EPS,
)

EVENT_TYPES = ("route_pinned", "progress", "reroute_adopted", "trip_completed")


def now_ts() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ----------------------------------------------------------------- signing ---

def event_digest(seq: int, ts: str, source: str, etype: str,
                 payload: dict, parent_digest: Optional[str]) -> str:
    """Integrity digest carried by every evidence event ('摘要校验')."""
    body = {"seq": seq, "ts": ts, "source": source, "type": etype,
            "payload": payload, "parent_digest": parent_digest}
    return sha256(body)


def descriptor_hash(version: str, base: dict, tiles: dict,
                    patches: list, overlays: dict) -> str:
    """Self-contained hash convention for an evidence version descriptor."""
    return sha256({"version": version, "base": base, "tiles": tiles,
                   "patches": patches, "overlays": overlays})


def minimal_patch(p: dict) -> dict:
    return {"patch_id": p["patch_id"], "priority": p.get("priority", 0),
            "scope": p["scope"], "reason": p.get("reason", ""),
            "content_hash": p.get("content_hash", "")}


def overlays_from_patches(patches: list, restrict_tiles: Optional[set] = None) -> dict:
    ov: dict[str, dict] = {}
    for p in patches:
        for tid, lanes in p["scope"]["tiles"].items():
            if restrict_tiles is not None and tid not in restrict_tiles:
                continue
            for lid in lanes:
                ov.setdefault(tid, {})[lid] = {
                    "closed": True, "patch_id": p["patch_id"],
                    "reason": p.get("reason", "")}
    return ov


# --------------------------------------------------------------- AuditEngine --

class AuditEngine:
    def __init__(self, audit_path: str, live):
        self.path = audit_path
        self.live = live                      # MapEngine, used READ-ONLY
        self.lock = threading.RLock()
        self.state: dict = {}
        self._load()

    # ========================================================= persistence ==
    def _load(self):
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as f:
                self.state = json.load(f)
        else:
            self.state = self._blank()
            self._persist_locked()

    @staticmethod
    def _blank() -> dict:
        return {"imports": {}, "branches": {}, "exports": {},
                "archives": [], "seq": {"archive": 0, "branch": 0, "export": 0}}

    def _persist_locked(self):
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        tmp = f"{self.path}.tmp.{os.getpid()}.{threading.get_ident()}"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.state, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)

    def commit(self, mutate) -> Any:
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
        """Deep copy of live state under its lock. Never mutated, never saved."""
        with self.live.lock:
            return copy.deepcopy(self.live.state)

    def _next(self, s: dict, key: str, prefix: str) -> str:
        s["seq"][key] += 1
        return f"{prefix}-{s['seq'][key]:04d}"

    # ============================================================ archives ==
    def _archive(self, s: dict, kind: str, request: dict, result: Any,
                 import_key: Optional[str] = None, ref: Optional[str] = None,
                 summary: str = "") -> dict:
        aid = self._next(s, "archive", "A")
        rec = {"archive_id": aid, "kind": kind, "created_at": now_ts(),
               "request": request, "result_digest": sha256(result),
               "import_key": import_key, "ref": ref,
               "summary": summary or kind}
        s["archives"].append(rec)
        return copy.deepcopy(rec)

    def list_archives(self, limit: int = 100) -> list:
        with self.lock:
            return copy.deepcopy(self.state["archives"][-limit:][::-1])

    # ============================================================== import ==
    def import_events(self, body: dict) -> dict:
        """First submission for an import key, or an idempotent replay of it."""
        key = body.get("import_key")
        if not key or not isinstance(key, str):
            raise DomainError("BAD_REQUEST", "import_key 必填（导入键，幂等判定依据）", 400)

        with self.lock:
            existing = self.state["imports"].get(key)
            if existing:
                # verdict-once: the same import key always yields the first
                # verdict, regardless of the (possibly malformed) replay body
                return self._verdict_view(existing, idempotent=True)

        events = body.get("events")
        if not isinstance(events, list) or not events:
            raise DomainError("BAD_REQUEST", "events 必须是非空事件数组", 400)

        verdict_events, rejection, compact = self._validate_chain(events)
        if rejection:
            return self.commit(lambda s: self._record_rejection(
                s, key, events, rejection, body.get("source", "")))
        return self.commit(lambda s: self._record_acceptance(
            s, key, verdict_events, compact, body.get("note", "")))

    def append_events(self, key: str, events: list) -> dict:
        """Continue an accepted chain from its persisted head checkpoint."""
        with self.lock:
            rec = self.state["imports"].get(key)
            if not rec:
                raise DomainError("NOT_FOUND", f"导入键 {key} 不存在，请先导入", 404)
            if rec["status"] == "rejected":
                return self._verdict_view(rec, idempotent=True)

        head = rec["head"]
        appended, rejection, compact = self._validate_chain(
            events, expect_first_seq=head["seq"] + 1,
            parent_digest=head["digest"], prior_state=head["state"])
        if rejection:
            def m(s):
                imp = s["imports"][key]
                imp["last_append_error"] = rejection
                # rejected append never touches the original chain / head checkpoint
                self._archive(s, "append_rejected",
                              {"import_key": key, "from_seq": head["seq"] + 1},
                              rejection, import_key=key,
                              summary=f"续算被拒绝：{rejection['code']}（头部检查点保持 {head['seq']}）")
                return {"import_key": key, "status": "accepted",
                        "append": "rejected", "rejection": rejection,
                        "head_seq": head["seq"]}
            return self.commit(m)

        def m2(s):
            imp = s["imports"][key]
            imp["events"].extend(copy.deepcopy(appended))
            imp["head"] = compact
            imp["updated_at"] = now_ts()
            imp["verdict"] = self._verdict_payload(imp)
            self._archive(s, "append_accepted",
                          {"import_key": key, "count": len(appended)},
                          {"head": compact["seq"], "digest": compact["digest"]},
                          import_key=key,
                          summary=f"从检查点 {head['seq']} 续算 {len(appended)} 条至 {compact['seq']}")
            return self._verdict_view(imp)
        return self.commit(m2)

    # ----------------------------------------------- chain-level validation ==
    def _validate_chain(self, events: list, expect_first_seq: int = 1,
                        parent_digest: Optional[str] = None,
                        prior_state: Optional[dict] = None):
        """Return (accepted_events, rejection_or_None, compact_head_state)."""
        compact = {
            "seq": expect_first_seq - 1, "digest": parent_digest,
            "state": copy.deepcopy(prior_state) or {"pins": {}},
        }
        seen_seq: set[int] = set()
        accepted: list[dict] = []
        prev_digest = parent_digest
        prev_seq = expect_first_seq - 1

        for raw in events:
            err = self._check_envelope(raw)
            if err:
                return None, self._reject(err, compact["seq"], raw), None

            seq, ts, source, etype = raw["seq"], raw["ts"], raw["source"], raw["type"]
            payload, digest = raw["payload"], raw["digest"]

            # 1) sequence continuity: duplicates first, then jumps (both rejected)
            if seq in seen_seq:
                return None, self._reject(
                    "AUDIT_DUPLICATE_EVENT", seq, raw,
                    f"序号 {seq} 重复出现，证据链禁止重复事件"), None
            if seq != prev_seq + 1:
                return None, self._reject(
                    "AUDIT_SEQ_GAP", seq, raw,
                    f"序号跳跃：期望 {prev_seq + 1}，实际 {seq}"), None
            seen_seq.add(seq)

            # 2) parent link must chain to the previous accepted event
            if raw.get("parent_digest") != prev_digest:
                return None, self._reject(
                    "AUDIT_PARENT_MISSING", seq, raw,
                    f"父事件摘要不匹配：事件 {seq} 的 parent_digest 未指向事件 {seq - 1}",
                    {"expected_parent": prev_digest,
                     "declared_parent": raw.get("parent_digest")}), None

            # 3) payload digest must match (catches any rewritten 摘要/正文)
            want = event_digest(seq, ts, source, etype, payload, prev_digest)
            if digest != want:
                return None, self._reject(
                    "AUDIT_DIGEST_MISMATCH", seq, raw,
                    f"事件 {seq} 的摘要校验失败：正文已被改写或签名错误",
                    {"declared": digest, "recomputed": want}), None

            # 4) event-type specific semantics against the replayed chain
            sem = self._check_semantic(etype, payload, compact["state"])
            if sem:
                return None, self._reject(
                    sem, seq, raw,
                    self._semantic_message(sem, etype, payload)), None

            self._apply_compact(compact["state"], etype, payload)
            compact["seq"] = seq
            compact["digest"] = digest
            prev_digest, prev_seq = digest, seq
            accepted.append(copy.deepcopy(raw))

        if not accepted:
            return None, self._reject("AUDIT_EMPTY_BATCH", expect_first_seq,
                                      None, "没有可接受的事件"), None
        return accepted, None, compact

    @staticmethod
    def _check_envelope(raw: Any) -> Optional[str]:
        if not isinstance(raw, dict):
            return "AUDIT_BAD_EVENT"
        for f in ("seq", "ts", "source", "type", "payload", "digest"):
            if f not in raw:
                return "AUDIT_BAD_EVENT"
        if not isinstance(raw["seq"], int) or isinstance(raw["seq"], bool) or raw["seq"] < 1:
            return "AUDIT_BAD_EVENT"
        if not isinstance(raw["ts"], str) or not isinstance(raw["source"], str) \
                or not raw["source"]:
            return "AUDIT_BAD_EVENT"
        if raw["type"] not in EVENT_TYPES:
            return "AUDIT_BAD_EVENT"
        if not isinstance(raw["payload"], dict):
            return "AUDIT_BAD_EVENT"
        if not isinstance(raw["digest"], str) or not raw["digest"].startswith("sha256:"):
            return "AUDIT_BAD_EVENT"
        pd = raw.get("parent_digest")
        if pd is not None and not isinstance(pd, str):
            return "AUDIT_BAD_EVENT"
        return None

    def _reject(self, code: str, seq: int, raw: Any, message: str,
                details: Optional[dict] = None) -> dict:
        d = {"code": code, "at_seq": seq, "message": message}
        if details:
            d["details"] = details
        if isinstance(raw, dict):
            d["event"] = {"seq": raw.get("seq"), "type": raw.get("type"),
                          "source": raw.get("source")}
        return d

    # --------------------------------------------- event semantic validation ==
    @staticmethod
    def _require_pin_payload(p: dict) -> Optional[str]:
        for f in ("pin_id", "version", "descriptor_hash", "base", "tiles",
                  "patches", "overlays", "route", "from_node", "to_node"):
            if f not in p:
                return "AUDIT_BAD_EVENT"
        if not isinstance(p["pin_id"], str) or not isinstance(p["tiles"], dict) \
                or not isinstance(p["base"], dict) or not isinstance(p["route"], list) \
                or not isinstance(p["patches"], list) or not isinstance(p["overlays"], dict):
            return "AUDIT_BAD_EVENT"
        for st in p["route"]:
            if not isinstance(st, dict) or st.get("kind") not in ("lane", "seam"):
                return "AUDIT_BAD_EVENT"
            tid = st.get("tile") if st["kind"] == "lane" else st.get("from_tile")
            if tid not in p["tiles"]:
                return "AUDIT_PARENT_MISSING"
        return None

    def _check_semantic(self, etype: str, p: dict, state: dict) -> Optional[str]:
        pins = state["pins"]
        if etype == "route_pinned":
            err = self._require_pin_payload(p)
            if err:
                return err
            if p["pin_id"] in pins:
                return "AUDIT_DUPLICATE_EVENT"
            return None
        if etype in ("progress", "trip_completed"):
            pin = pins.get(p.get("pin_id"))
            if pin is None:
                return "AUDIT_PARENT_MISSING"
            if etype == "trip_completed":
                if pin["status"] != "active":
                    return "AUDIT_BAD_EVENT"
                return None
            if pin["status"] != "active":
                return "AUDIT_BAD_EVENT"
            step, pstate = p.get("step"), p.get("state")
            if not isinstance(step, int) or pstate not in ("at_node", "on_lane"):
                return "AUDIT_BAD_EVENT"
            if not (0 <= step <= pin["route_len"]):
                return "AUDIT_BAD_EVENT"
            return None
        if etype == "reroute_adopted":
            parent = pins.get(p.get("parent_pin_id"))
            child = p.get("child")
            if parent is None or not isinstance(child, dict):
                return "AUDIT_PARENT_MISSING"
            if parent["status"] != "active":
                return "AUDIT_BAD_EVENT"
            err = self._require_pin_payload(child)
            if err:
                return err
            if child["pin_id"] in pins:
                return "AUDIT_DUPLICATE_EVENT"
            return None
        return "AUDIT_BAD_EVENT"

    @staticmethod
    def _semantic_message(code: str, etype: str, p: dict) -> str:
        if code == "AUDIT_PARENT_MISSING":
            target = p.get("pin_id") or p.get("parent_pin_id") or "?"
            return f"{etype} 引用的父事件/固定路线 {target} 在证据链中缺失"
        if code == "AUDIT_DUPLICATE_EVENT":
            return f"{etype} 造成重复标识（路线固定事件或子快照不得重复）"
        return f"{etype} 事件语义不合法（字段缺失、状态越界或对象已关闭）"

    @staticmethod
    def _apply_compact(state: dict, etype: str, p: dict):
        pins = state["pins"]
        if etype == "route_pinned":
            pins[p["pin_id"]] = {"status": "active", "step": 0,
                                 "state": "at_node", "route_len": len(p["route"]),
                                 "event_seq": p.get("seq")}
        elif etype == "progress":
            pin = pins[p["pin_id"]]
            pin["step"], pin["state"] = p["step"], p["state"]
        elif etype == "reroute_adopted":
            pins[p["parent_pin_id"]]["status"] = "superseded"
            ch = p["child"]
            pins[ch["pin_id"]] = {"status": "active", "step": 0,
                                  "state": "at_node", "route_len": len(ch["route"])}
        elif etype == "trip_completed":
            pins[p["pin_id"]]["status"] = "completed"

    # ---------------------------------------------------- verdict recording ==
    def _record_acceptance(self, s: dict, key: str, events: list,
                           compact: dict, note: str) -> dict:
        sources = sorted({e["source"] for e in events})
        types = {}
        for e in events:
            types[e["type"]] = types.get(e["type"], 0) + 1
        vehicle_id = next((e["payload"].get("vehicle_id")
                           for e in events if e["type"] == "route_pinned"
                           and e["payload"].get("vehicle_id")), None)
        rec = {
            "import_key": key, "status": "accepted", "note": note,
            "vehicle_id": vehicle_id, "events": events,
            "head": compact,
            "confirmed": {},   # seq -> checkpoint (digest + compact state)
            "verdict": None, "rejection": None,
            "sources": sources, "types": types,
            "created_at": now_ts(), "updated_at": now_ts(),
        }
        rec["verdict"] = self._verdict_payload(rec)
        s["imports"][key] = rec
        self._archive(s, "import_accepted", {"import_key": key, "count": len(events)},
                      rec["verdict"], import_key=key,
                      summary=f"证据链 {key} 导入通过：{len(events)} 条，链摘要 {short(compact['digest'], 12)}")
        return self._verdict_view(rec)

    def _record_rejection(self, s: dict, key: str, events: list,
                          rejection: dict, source: str) -> dict:
        rec = {"import_key": key, "status": "rejected",
               "note": "", "vehicle_id": None,
               "events": [], "head": None, "confirmed": {},
               "verdict": {"status": "rejected", "import_key": key,
                           "received_at": now_ts(), "submitted_count": len(events)},
               "rejection": {**rejection, "rejected_at": now_ts()},
               "sources": [source] if source else [], "types": {},
               "created_at": now_ts(), "updated_at": now_ts()}
        s["imports"][key] = rec
        self._archive(s, "import_rejected", {"import_key": key, "count": len(events)},
                      rejection, import_key=key,
                      summary=f"证据链 {key} 导入拒绝：{rejection['code']} @seq {rejection['at_seq']}")
        return self._verdict_view(rec)

    @staticmethod
    def _verdict_payload(rec: dict) -> dict:
        evs = rec["events"]
        return {
            "status": "accepted", "import_key": rec["import_key"],
            "event_count": len(evs), "first_seq": evs[0]["seq"], "last_seq": evs[-1]["seq"],
            "first_event_ts": evs[0]["ts"], "last_event_ts": evs[-1]["ts"],
            "head_seq": rec["head"]["seq"], "chain_digest": rec["head"]["digest"],
            "vehicle_id": rec.get("vehicle_id"),
            "sources": rec.get("sources", []), "types": rec.get("types", {}),
            "received_at": rec["created_at"],
        }

    def _verdict_view(self, rec: dict, idempotent: bool = False) -> dict:
        out = copy.deepcopy(rec["verdict"])
        out["idempotent"] = idempotent
        if rec["status"] == "rejected":
            out["rejection"] = rec["rejection"]
        else:
            out["confirmed_checkpoints"] = sorted(int(k) for k in rec["confirmed"])
        return out

    def list_imports(self) -> list:
        with self.lock:
            out = []
            for rec in self.state["imports"].values():
                v = copy.deepcopy(rec["verdict"])
                v["event_count"] = len(rec["events"])
                v["confirmed_checkpoints"] = sorted(int(k) for k in rec["confirmed"])
                if rec["status"] == "rejected":
                    v["rejection"] = {"code": rec["rejection"]["code"],
                                      "at_seq": rec["rejection"]["at_seq"],
                                      "message": rec["rejection"]["message"]}
                out.append(v)
            return sorted(out, key=lambda v: v.get("received_at", ""), reverse=True)

    def get_import(self, key: str) -> dict:
        with self.lock:
            rec = self.state["imports"].get(key)
            if not rec:
                raise DomainError("NOT_FOUND", f"导入键 {key} 不存在", 404)
            out = copy.deepcopy(rec)
            out.pop("events", None)
            out["event_digests"] = [{"seq": e["seq"], "type": e["type"],
                                     "source": e["source"], "ts": e["ts"],
                                     "digest": e["digest"],
                                     "parent_digest": e.get("parent_digest")}
                                    for e in rec["events"]]
            out["event_count"] = len(rec["events"])
            return out

    # ============================================================ checkpoints ==
    def confirm_checkpoint(self, key: str, seq: int, note: str = "",
                           by: str = "auditor") -> dict:
        def m(s):
            rec = s["imports"].get(key)
            if not rec:
                raise DomainError("NOT_FOUND", f"导入键 {key} 不存在", 404)
            if rec["status"] != "accepted":
                raise DomainError("AUDIT_CHAIN_REJECTED",
                                  "被拒绝的证据链不能确认检查点", 422)
            if not (1 <= seq <= rec["head"]["seq"]):
                raise DomainError("BAD_REQUEST",
                                  f"序号 {seq} 超出已接受范围 1..{rec['head']['seq']}", 400)
            ev = rec["events"][seq - 1]
            cp = {"seq": seq, "digest": ev["digest"],
                  "state": self._replay_compact(rec["events"], seq),
                  "confirmed_at": now_ts(), "confirmed_by": by, "note": note}
            rec["confirmed"][str(seq)] = cp
            rec["updated_at"] = now_ts()
            self._archive(s, "checkpoint_confirmed",
                          {"import_key": key, "seq": seq, "note": note},
                          {"seq": seq, "digest": ev["digest"]},
                          import_key=key,
                          summary=f"确认检查点 {key}@{seq}（链摘要 {short(ev['digest'], 12)}）")
            return copy.deepcopy(cp)
        return self.commit(m)

    def list_checkpoints(self, key: str) -> list:
        with self.lock:
            rec = self.state["imports"].get(key)
            if not rec:
                raise DomainError("NOT_FOUND", f"导入键 {key} 不存在", 404)
            return [{"seq": cp["seq"], "digest": cp["digest"],
                     "confirmed_at": cp["confirmed_at"], "confirmed_by": cp["confirmed_by"],
                     "note": cp.get("note", "")}
                    for cp in sorted(rec["confirmed"].values(), key=lambda c: c["seq"])]

    @staticmethod
    def _replay_compact(events: list, upto: int) -> dict:
        state = {"pins": {}}
        for e in events[:upto]:
            AuditEngine._apply_compact(state, e["type"], e["payload"])
        return state

    # ========================================================== reconstruction ==
    def _replay_full(self, events: list, upto: int):
        """Replay pin payloads (full) up to and including event `upto`."""
        pins: dict[str, dict] = {}
        order: list[str] = []
        for e in events[:upto]:
            t, p = e["type"], e["payload"]
            if t == "route_pinned":
                pins[p["pin_id"]] = {"payload": p, "status": "active",
                                     "progress": {"step": 0, "state": "at_node"},
                                     "event_seq": e["seq"]}
                order.append(p["pin_id"])
            elif t == "progress":
                pins[p["pin_id"]]["progress"] = {"step": p["step"], "state": p["state"]}
            elif t == "reroute_adopted":
                pins[p["parent_pin_id"]]["status"] = "superseded"
                ch = p["child"]
                pins[ch["pin_id"]] = {"payload": ch, "status": "active",
                                      "progress": {"step": 0, "state": "at_node"},
                                      "event_seq": e["seq"]}
                order.append(ch["pin_id"])
            elif t == "trip_completed":
                pins[p["pin_id"]]["status"] = "completed"
        return pins, order

    def _resolve_upto(self, rec: dict, seq: Optional[int], ts: Optional[str]) -> int:
        head = rec["head"]["seq"]
        if ts is not None:
            target = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            upto = 0
            for e in rec["events"]:
                if datetime.fromisoformat(e["ts"].replace("Z", "+00:00")) <= target:
                    upto = e["seq"]
            if upto == 0:
                raise DomainError("AUDIT_NO_STATE",
                                  f"时刻 {ts} 早于证据链首个事件 {rec['events'][0]['ts']}", 404)
            return upto
        if seq is None:
            return head
        if not (1 <= seq <= head):
            raise DomainError("BAD_REQUEST", f"序号需在 1..{head} 之间", 400)
        return seq

    def query_state(self, key: str, seq: Optional[int] = None,
                    ts: Optional[str] = None) -> dict:
        """Reconstruct spatial state at one moment. Appends a query archive."""
        with self.lock:
            rec = self.state["imports"].get(key)
            if not rec:
                raise DomainError("NOT_FOUND", f"导入键 {key} 不存在", 404)
            if rec["status"] != "accepted":
                raise DomainError("AUDIT_CHAIN_REJECTED", "被拒绝的证据链无法重建状态", 422)
            upto = self._resolve_upto(rec, seq, ts)

        result = self._build_state(rec, upto, at_kind=("ts" if ts is not None else "seq"),
                                   at_value=ts if ts is not None else upto)

        def m(s):
            arc = self._archive(s, "query",
                                {"import_key": key, "seq": seq, "ts": ts},
                                result, import_key=key, ref=result["pin_id"],
                                summary=f"时序查询 {key}@{upto}：版本 {result['version']['version']}")
            result["archive_id"] = arc["archive_id"]
            return copy.deepcopy(result)
        return self.commit(m)

    def _build_state(self, rec: dict, upto: int, at_kind: str, at_value: Any) -> dict:
        live = self._live_snapshot()
        events = rec["events"]
        pins, order = self._replay_full(events, upto)
        active_or_done = [pid for pid in order if pins[pid]["status"] in ("active", "completed")]
        if not active_or_done:
            raise DomainError("AUDIT_NO_STATE",
                              f"序号 {upto} 时尚无可重建的固定路线", 422)
        pin_id = active_or_done[-1]
        pin = pins[pin_id]
        payload, progress = pin["payload"], pin["progress"]

        # ---- tile digest verification against the content-addressed store ----
        tile_checks, tile_map = [], {}
        for tid, declared in sorted(payload["tiles"].items()):
            tobj = next((t for h, t in live["tiles"].items() if h == declared), None)
            present = tobj is not None
            ok = present and sha256(tobj) == declared
            if present:
                tile_map[tid] = tobj
            tile_checks.append({"tile_id": tid, "hash": declared,
                                "present": present, "content_hash_ok": ok})

        patches_min = [minimal_patch(p) if set(p) > {"patch_id", "priority", "scope"}
                       else p for p in payload["patches"]]
        overlays = payload.get("overlays", overlays_from_patches(
            payload["patches"], set(payload["tiles"])))
        # descriptor hash is the creator's signature over the exact declared
        # fields; recomputing it byte-for-byte detects a rewritten 版本描述.
        dh_ok = payload["descriptor_hash"] == descriptor_hash(
            payload["version"], payload["base"], payload["tiles"],
            payload["patches"], overlays)

        warnings = []
        bad = [t for t in tile_checks if not t["content_hash_ok"]]
        if bad:
            warnings.append({"level": "tile_digest_mismatch",
                             "message": f"{len(bad)} 个瓦片摘要与内容寻址库不一致（缺失或被改写）",
                             "tiles": [t["tile_id"] for t in bad]})
        if not dh_ok:
            warnings.append({"level": "descriptor_hash_mismatch",
                             "message": "版本描述符哈希复核失败：版本描述可能被改写"})

        steps_view = self._steps_view(payload["route"], progress)
        position = self._position(payload, progress)
        evidence = [{"seq": e["seq"], "type": e["type"], "source": e["source"],
                     "ts": e["ts"], "digest": e["digest"]}
                    for e in events[:upto]
                    if e["type"] in ("route_pinned", "reroute_adopted")
                    and (e["payload"].get("pin_id") == pin_id
                         or e["payload"].get("child", {}).get("pin_id") == pin_id)]
        at_event = events[upto - 1]
        result = {
            "import_key": rec["import_key"], "vehicle_id": rec.get("vehicle_id"),
            "at": {"kind": at_kind, "value": at_value, "event_seq": upto,
                   "event_ts": at_event["ts"], "chain_digest": at_event["digest"]},
            "pin_id": pin_id, "pin_event_seq": pin["event_seq"],
            "pin_status": pin["status"],
            "version": {"version": payload["version"],
                        "descriptor_hash": payload["descriptor_hash"],
                        "descriptor_hash_ok": dh_ok,
                        "base": payload["base"],
                        "patches": [p["patch_id"] if isinstance(p, dict) else p
                                    for p in payload["patches"]]},
            "tiles": tile_checks,
            "tile_summary": {"count": len(tile_checks),
                             "verified": sum(1 for t in tile_checks if t["content_hash_ok"]),
                             "digest": sha256(payload["tiles"])},
            "overlays": overlays,
            "from_node": payload["from_node"], "to_node": payload["to_node"],
            "progress": progress, "position": position,
            "steps": steps_view,
            "route": payload["route"],
            "map": self._render_view(live, tile_map, overlays, payload, progress, pin["status"]),
            "evidence": evidence, "warnings": warnings,
        }
        return result

    @staticmethod
    def _steps_view(route: list, progress: dict) -> list:
        i, state = progress["step"], progress["state"]
        out = []
        for idx, st in enumerate(route):
            mark = "remaining"
            if idx < i:
                mark = "done"
            elif idx == i:
                mark = "current_on_lane" if state == "on_lane" else "current_at_node"
            out.append({"index": idx, "kind": st["kind"],
                        "tile": st.get("tile") or st.get("from_tile"),
                        "lane": st.get("lane"),
                        "from": f"{st['from_tile']}/{st['from_node']}",
                        "to": f"{st['to_tile']}/{st['to_node']}",
                        "speed": st.get("speed"), "phase": mark})
        return out

    @staticmethod
    def _position(payload: dict, progress: dict) -> dict:
        route, i, state = payload["route"], progress["step"], progress["state"]
        if i == 0:
            return {"state": "at_node", "node": payload["from_node"], "step_index": 0}
        st = route[i - 1]
        if state == "at_node":
            return {"state": "at_node",
                    "node": f"{st['to_tile']}/{st['to_node']}", "step_index": i}
        return {"state": "on_lane", "step_index": i - 1,
                "from": f"{st['from_tile']}/{st['from_node']}",
                "to": f"{st['to_tile']}/{st['to_node']}",
                "lane": st.get("lane")}

    # ============================================================= branches ==
    def create_branch(self, key: str, body: dict) -> dict:
        seq = body.get("seq")
        scenario = body.get("scenario") or {}
        stype = scenario.get("type")
        if not isinstance(seq, int) or stype not in ("version_switch", "revoke_closure"):
            raise DomainError("BAD_REQUEST",
                              "需要 seq（整数）与 scenario.type="
                              "version_switch|revoke_closure", 400)

        with self.lock:
            rec = self.state["imports"].get(key)
            if not rec:
                raise DomainError("NOT_FOUND", f"导入键 {key} 不存在", 404)
            cp = rec["confirmed"].get(str(seq))
            if not cp:
                raise DomainError("AUDIT_CHECKPOINT_NOT_CONFIRMED",
                                  f"序号 {seq} 不是已确认检查点；"
                                  f"审计员只能从已确认检查点建立隔离分支", 422)
            if cp["digest"] != rec["events"][seq - 1]["digest"]:
                raise DomainError("AUDIT_DIGEST_MISMATCH",
                                  "检查点摘要与证据链不一致，禁止分支", 422)
            # all evidence reads happen under the lock; branch math is outside it
            rec_copy = copy.deepcopy(rec)
            base_digest = cp["digest"]

        state = self._build_state(rec_copy, seq, "seq", seq)
        pins, order = self._replay_full(rec_copy["events"], seq)
        pin_id = state["pin_id"]
        pin_payload = copy.deepcopy(pins[pin_id]["payload"])
        live = self._live_snapshot()

        if stype == "version_switch":
            cf = self._switch_version(live, state, scenario, pin_payload)
            sc_summary = f"版本切换 → 发布 {cf['release_id']}"
        else:
            cf = self._revoke_closure(live, state, scenario, pin_payload)
            sc_summary = f"撤销道路管制 {cf['revoked_patch_id']}"

        request = {"import_key": key, "seq": seq, "scenario": scenario,
                   "created_by": body.get("created_by", "auditor")}

        def m(s):
            bid = self._next(s, "branch", "B")
            record = {"branch_id": bid, "import_key": key, "base_seq": seq,
                      "base_digest": base_digest, "created_at": now_ts(),
                      "created_by": body.get("created_by", "auditor"),
                      "scenario": scenario, "counterfactual": cf,
                      "result_digest": sha256(cf)}
            s["branches"][bid] = record
            arc = self._archive(s, "branch", request, cf, import_key=key, ref=bid,
                                summary=f"隔离分支 {bid}@{key}:{seq}：{sc_summary}")
            record["archive_id"] = arc["archive_id"]
            return copy.deepcopy(record)
        return self.commit(m)

    def _counter_descriptor(self, live: dict, new_tiles: dict,
                            patches: list, overlays: dict, label_extra: str = "") -> dict:
        base = {}
        for region, rid0 in sorted(new_tiles["_region_release"].items()):
            rel = next(r for r in live["releases"] if r["release_id"] == rid0)
            base[region] = {"release_id": rid0, "generation": rel["generations"][region]}
        tile_hashes = {tid: h for tid, h in new_tiles["tiles"].items()}
        pmin = [minimal_patch(p) for p in patches]
        gen_part = "/".join(f"{r}={b['generation']}" for r, b in base.items())
        patch_part = ",".join(f"{p['patch_id']}[p{p.get('priority', 0)}]" for p in pmin)
        version = gen_part + (f"+{patch_part}" if patch_part else "") + label_extra
        return {"version": version, "base": base, "tiles": tile_hashes,
                "patches": pmin, "overlays": overlays,
                "descriptor_hash": descriptor_hash(version, base, tile_hashes, pmin, overlays)}

    def _switch_version(self, live: dict, state: dict, scenario: dict,
                        pin: dict) -> dict:
        rid = scenario.get("release_id")
        rel = next((r for r in live["releases"] if r["release_id"] == rid), None)
        if rel is None:
            raise DomainError("NOT_FOUND",
                              f"发布 {rid} 不存在；隔离演算只能指向已发布版本", 404)
        switched = set(rel["regions"])

        # assemble tile map: switched regions from target release, others from pin
        region_release = dict({r: b["release_id"] for r, b in state["version"]["base"].items()})
        tiles: dict[str, str] = {}
        for region, r_id in region_release.items():
            if region in switched:
                continue
            r0 = next(x for x in live["releases"] if x["release_id"] == r_id)
            for tid, h in r0["tiles"].items():
                if live["tiles"][h]["region"] == region:
                    tiles[tid] = h
        for tid, h in rel["tiles"].items():
            if live["tiles"][h]["region"] in switched:
                tiles[tid] = h
        for r in switched:
            region_release[r] = rid

        # pinned patches survive only while still pinned byte-for-byte
        kept, dropped = self._filter_live_patches(pin, tiles)
        overlays = overlays_from_patches(kept, set(tiles))
        desc = self._counter_descriptor(live,
                                        {"tiles": tiles, "_region_release": region_release},
                                        kept, overlays, label_extra="[what-if-switch]")

        tile_map = {tid: live["tiles"][h] for tid, h in tiles.items() if h in live["tiles"]}
        route, reachable = self._replan(live, tile_map, overlays, pin)
        diffs = self._diffs(state, tiles, pin, kept, dropped, route,
                            switched_regions=switched, target_release=rel)
        return {"type": "version_switch", "release_id": rid,
                "candidate_id": rel["candidate_id"], "regions": rel["regions"],
                "descriptor": desc,
                "tiles": [{"tile_id": t, "hash": h,
                           "present": h in live["tiles"]} for t, h in sorted(tiles.items())],
                "overlays": overlays, "route": route, "reachable": reachable,
                "dropped_patches": dropped, "diffs": diffs,
                "map": self._render_view(live, tile_map, overlays, None, None, None,
                                         route=route, from_node=pin["from_node"],
                                         to_node=pin["to_node"],
                                         label=f"what-if: {desc['version']}")}

    def _revoke_closure(self, live: dict, state: dict, scenario: dict,
                        pin: dict) -> dict:
        pid = scenario.get("patch_id")
        pinned_ids = [p["patch_id"] for p in pin["patches"]]
        if pid not in pinned_ids:
            raise DomainError("AUDIT_PATCH_NOT_PINNED",
                              f"补丁 {pid} 不在该时刻采用的版本 {state['version']['version']} 中，"
                              f"无可撤销的道路管制", 422,
                              {"pinned_patches": pinned_ids})
        kept = [dict(p) for p in pin["patches"] if p["patch_id"] != pid]
        # remove only this patch's closures; closures by other patches remain
        overlays = {}
        removed = []
        old_ov = pin.get("overlays", {})
        kept_ids = {p["patch_id"] for p in kept}
        for tid, lanes in old_ov.items():
            for lid, info in lanes.items():
                if info.get("patch_id") == pid:
                    removed.append({"tile_id": tid, "lane_id": lid})
                elif info.get("patch_id") in kept_ids:
                    overlays.setdefault(tid, {})[lid] = info
        tiles = dict(pin["tiles"])
        region_release = {r: b["release_id"] for r, b in state["version"]["base"].items()}
        desc = self._counter_descriptor(live,
                                        {"tiles": tiles, "_region_release": region_release},
                                        kept, overlays, label_extra="[what-if-revoke]")
        tile_map = {tid: live["tiles"][h] for tid, h in tiles.items() if h in live["tiles"]}
        route, reachable = self._replan(live, tile_map, overlays, pin)
        diffs = self._diffs(state, tiles, pin, kept, [], route, removed_closures=removed)
        return {"type": "revoke_closure", "revoked_patch_id": pid,
                "descriptor": desc,
                "tiles": [{"tile_id": t, "hash": h, "present": h in live["tiles"]}
                          for t, h in sorted(tiles.items())],
                "overlays": overlays, "route": route, "reachable": reachable,
                "removed_closures": removed, "diffs": diffs,
                "map": self._render_view(live, tile_map, overlays, None, None, None,
                                         route=route, from_node=pin["from_node"],
                                         to_node=pin["to_node"],
                                         label=f"what-if: revoke {pid}")}

    @staticmethod
    def _filter_live_patches(pin: dict, tiles: dict):
        kept, dropped = [], []
        for p in pin["patches"]:
            ok = True
            # a pinned closure survives the what-if only if every tile it
            # closes is still present (same id) in the switched tile set
            for tid in p["scope"]["tiles"]:
                if tid not in tiles:
                    ok = False
                    break
            if ok:
                kept.append(dict(p))
            else:
                dropped.append({"patch_id": p["patch_id"],
                                "reason": "切换后的版本缺少其固化瓦片，隔离演算中自动悬空"})
        return kept, dropped

    @staticmethod
    def _replan(live: dict, tile_map: dict, overlays: dict, pin: dict):
        g = build_graph(list(tile_map.values()), overlays)
        steps = dijkstra(g, pin["from_node"], pin["to_node"])
        return steps, steps is not None

    @staticmethod
    def _diffs(state, new_tiles, pin, kept, dropped, new_route,
               switched_regions=(), target_release=None, removed_closures=()):
        old_tiles = pin["tiles"]
        tiles_changed = [{"tile_id": tid, "from_hash": old_tiles.get(tid),
                          "to_hash": new_tiles.get(tid)}
                         for tid in sorted(set(old_tiles) | set(new_tiles))
                         if old_tiles.get(tid) != new_tiles.get(tid)]
        gen_changes = []
        if target_release:
            for r in switched_regions:
                gen_changes.append(
                    {"region": r,
                     "from_generation": state["version"]["base"].get(r, {}).get("generation"),
                     "to_generation": target_release["generations"][r],
                     "to_release": target_release["release_id"]})
        old_sig = [(st.get("kind"), st.get("tile"), st.get("lane"),
                    st.get("via_port")) for st in pin["route"]]
        new_sig = [(st.get("kind"), st.get("tile"), st.get("lane"),
                    st.get("via_port")) for st in (new_route or [])]
        return {
            "tiles_changed": tiles_changed,
            "generation_changes": gen_changes,
            "closures_removed": removed_closures or [],
            "patches_dropped": [d["patch_id"] for d in dropped],
            "old_steps": len(old_sig), "new_steps": len(new_sig),
            "route_changed": old_sig != new_sig,
            "reachable": new_route is not None,
        }

    def list_branches(self, key: str) -> list:
        with self.lock:
            return [copy.deepcopy({k: v for k, v in b.items()
                                   if k not in ("counterfactual",)})
                    for b in self.state["branches"].values()
                    if b["import_key"] == key]

    def get_branch(self, bid: str, include_map: bool = True) -> dict:
        with self.lock:
            b = self.state["branches"].get(bid)
            if not b:
                raise DomainError("NOT_FOUND", f"分支 {bid} 不存在", 404)
            out = copy.deepcopy(b)
            if not include_map:
                out["counterfactual"].pop("map", None)
            return out

    # =============================================================== export ==
    def create_export(self, key: str, body: Optional[dict] = None) -> dict:
        body = body or {}
        with self.lock:
            rec = self.state["imports"].get(key)
            if not rec:
                raise DomainError("NOT_FOUND", f"导入键 {key} 不存在", 404)
            payload = self._build_export(rec)

        def m(s):
            eid = self._next(s, "export", "E")
            payload["export_id"] = eid
            payload["bundle_digest"] = sha256({k: v for k, v in payload.items()
                                               if k != "bundle_digest"})
            s["exports"][eid] = {"export_id": eid, "import_key": key,
                                 "created_at": payload["generated_at"],
                                 "bundle_digest": payload["bundle_digest"]}
            arc = self._archive(s, "export", {"import_key": key},
                                {"bundle_digest": payload["bundle_digest"],
                                 "events": payload["import_verdict"]["event_count"],
                                 "branches": len(payload["branches"])},
                                import_key=key, ref=eid,
                                summary=f"导出审计档案 {eid}（{key}，"
                                        f"{payload['import_verdict']['event_count']} 事件）")
            payload["archive_id"] = arc["archive_id"]
            return copy.deepcopy(payload)
        return self.commit(m)

    def _build_export(self, rec: dict) -> dict:
        branches = [copy.deepcopy(b) for b in self.state["branches"].values()
                    if b["import_key"] == rec["import_key"]]
        for b in branches:
            b["counterfactual"].pop("map", None)
        return {
            "export_id": None,
            "kind": "audit_evidence_bundle",
            "generated_at": now_ts(),
            "import_key": rec["import_key"],
            "import_verdict": self._verdict_payload(rec),
            "confirmed_checkpoints": [
                {"seq": int(k), "digest": cp["digest"], "confirmed_at": cp["confirmed_at"],
                 "confirmed_by": cp["confirmed_by"], "note": cp.get("note", "")}
                for k, cp in sorted(rec["confirmed"].items(), key=lambda kv: int(kv[0]))],
            "events": copy.deepcopy(rec["events"]),
            "branches": branches,
            "bundle_digest": None,
        }

    def list_exports(self, key: Optional[str] = None) -> list:
        with self.lock:
            return [copy.deepcopy(e) for e in self.state["exports"].values()
                    if key is None or e["import_key"] == key]

    # =========================================================== map render ==
    def _render_view(self, live: dict, tile_map: dict, overlays: dict,
                     pin: Optional[dict], progress: Optional[dict], status: Optional[str],
                     route=None, from_node=None, to_node=None, label: str = "") -> dict:
        """Build the same map payload shape as /api/view, from audit-only data."""
        tiles_out, nodes_out, lanes_out, seams_out = [], [], [], []
        for tid, t in sorted(tile_map.items()):
            xs = [n["x"] for n in t["nodes"]]
            ys = [n["y"] for n in t["nodes"]]
            bounds = t.get("bounds") or [min(xs), min(ys), max(xs), max(ys)]
            tiles_out.append({"tile_id": tid, "region": t["region"], "bounds": bounds,
                              "version_tag": t.get("version_tag", "")})
            nmap = {n["node_id"]: n for n in t["nodes"]}
            ports = {p["node"]: p for p in t.get("ports", [])}
            for n in t["nodes"]:
                p = ports.get(n["node_id"])
                nodes_out.append({"id": node_key(tid, n["node_id"]), "tile": tid,
                                  "node": n["node_id"], "x": n["x"], "y": n["y"],
                                  "external": bool(n.get("external") or (p and p.get("external"))),
                                  "port": p["port_id"] if p else None})
            for ln in t["lanes"]:
                ov = overlays.get(tid, {}).get(ln["lane_id"])
                lanes_out.append({
                    "id": f"{tid}/{ln['lane_id']}", "tile": tid, "lane": ln["lane_id"],
                    "from": node_key(tid, ln["from"]), "to": node_key(tid, ln["to"]),
                    "x1": nmap[ln["from"]]["x"], "y1": nmap[ln["from"]]["y"],
                    "x2": nmap[ln["to"]]["x"], "y2": nmap[ln["to"]]["y"],
                    "speed": ln.get("speed"), "two_way": ln.get("two_way", True),
                    "closed": bool(ov and ov.get("closed")),
                    "closed_by": ov.get("patch_id") if ov else None})
        seen = set()
        for tid, t in sorted(tile_map.items()):
            for p in t.get("ports", []):
                peer = p.get("peer")
                if not peer or peer["tile"] not in tile_map:
                    continue
                k2 = tuple(sorted([f"{tid}:{p['port_id']}",
                                   f"{peer['tile']}:{peer['port']}"]))
                if k2 in seen:
                    continue
                seen.add(k2)
                t2 = tile_map[peer["tile"]]
                p2 = next((q for q in t2.get("ports", [])
                           if q["port_id"] == peer["port"]), None)
                ok = bool(p2 and p2.get("peer", {}).get("tile") == tid and
                          p2.get("peer", {}).get("port") == p["port_id"] and
                          abs(p["x"] - p2["x"]) <= EPS and abs(p["y"] - p2["y"]) <= EPS and
                          p.get("profile") == p2.get("profile"))
                seams_out.append({"a": f"{tid}:{p['port_id']}",
                                  "b": f"{peer['tile']}:{peer['port']}",
                                  "x": p["x"], "y": p["y"], "ok": ok})
        g = build_graph(list(tile_map.values()), overlays)
        corridors = []
        for cor in live.get("corridors", []):
            wps = cor["waypoints"]
            ok = True
            for a, b in zip(wps, wps[1:]):
                if a not in g["nodes"] or b not in g["nodes"] or dijkstra(g, a, b) is None:
                    ok = False
                    break
            corridors.append({"corridor_id": cor["corridor_id"], "name": cor.get("name", ""),
                              "ok": ok, "segments": []})
        route_out = None
        if pin is not None:
            route_out = {"steps": pin["route"], "progress": progress,
                         "status": status, "from_node": pin["from_node"],
                         "to_node": pin["to_node"]}
        elif route is not None:
            route_out = {"steps": route, "progress": {"step": 0, "state": "at_node"},
                         "status": "what-if", "from_node": from_node, "to_node": to_node}
        return {"mode": "audit", "ref": None, "version_label": label,
                "tiles": tiles_out, "nodes": nodes_out, "lanes": lanes_out,
                "seams": seams_out, "errors": [], "corridors": corridors,
                "route": route_out}

    # ======================================================= evidence author ==
    def build_vehicle_events(self, body: dict) -> dict:
        """READ-ONLY authoring aid: turn one live vehicle's snapshot chain into
        a correctly signed evidence bundle (pin -> progress -> node-level
        reroute -> completed). The live store is only read.

        Note: evidence descriptor hashes use this subsystem's own signing
        convention, so they are recomputed from the snapshot bytes rather than
        reusing the engine's live descriptor hash.
        """
        live = self._live_snapshot()
        vehicle = body.get("vehicle_id") or "VH-01"
        v = live["vehicles"].get(vehicle)
        if not v or not v["snapshots"]:
            raise DomainError("NOT_FOUND",
                              f"车辆 {vehicle} 没有可导出的快照链", 404)
        source = body.get("source") or f"ops-import/vehicle/{vehicle}"
        snaps = [live["snapshots"][sid] for sid in v["snapshots"]]

        def pin_payload(snap) -> dict:
            patches = [minimal_patch(p) for p in snap["version"].get("patches", [])]
            p = {"pin_id": snap["snapshot_id"], "vehicle_id": vehicle,
                 "version": snap["version"]["version"], "descriptor_hash": "",
                 "base": copy.deepcopy(snap["version"]["base"]),
                 "tiles": dict(sorted(snap["tiles"].items())),
                 "patches": patches,
                 "overlays": copy.deepcopy(snap.get("overlays", {})),
                 "route": copy.deepcopy(snap["route"]),
                 "from_node": snap["from_node"], "to_node": snap["to_node"]}
            p["descriptor_hash"] = descriptor_hash(
                p["version"], p["base"], p["tiles"], p["patches"], p["overlays"])
            return p

        base_ts = datetime.fromisoformat(snaps[0]["created_at"].replace("Z", "+00:00"))
        if base_ts.tzinfo is None:
            base_ts = base_ts.replace(tzinfo=timezone.utc)
        base_ts = base_ts.astimezone(timezone.utc)
        events = []

        def add(etype, payload, dt_seconds):
            seq = len(events) + 1
            parent = events[-1]["digest"] if events else None
            ts_s = (base_ts + timedelta(seconds=dt_seconds)).replace(microsecond=0).isoformat()
            digest = event_digest(seq, ts_s, source, etype, payload, parent)
            events.append({"seq": seq, "ts": ts_s, "source": source, "type": etype,
                           "payload": payload, "parent_digest": parent, "digest": digest})

        clock = 0
        prev = None
        for snap in snaps:
            pl = pin_payload(snap)
            if prev is None:
                add("route_pinned", pl, clock)
            else:
                clock += 20
                add("reroute_adopted",
                    {"parent_pin_id": prev, "reason": "emergency_closure_node_transition",
                     "child": pl, "vehicle_id": vehicle}, clock)
            route_len = len(snap["route"])
            if snap["status"] == "superseded" and route_len >= 1:
                clock += 40
                add("progress", {"pin_id": snap["snapshot_id"], "step": 1,
                                 "state": "on_lane", "vehicle_id": vehicle}, clock)
                clock += 40
                add("progress", {"pin_id": snap["snapshot_id"], "step": 1,
                                 "state": "at_node", "vehicle_id": vehicle}, clock)
            elif snap["status"] == "completed":
                if route_len >= 1:
                    clock += 40
                    add("progress", {"pin_id": snap["snapshot_id"], "step": 1,
                                     "state": "on_lane", "vehicle_id": vehicle}, clock)
                clock += 40
                add("trip_completed", {"pin_id": snap["snapshot_id"],
                                       "vehicle_id": vehicle}, clock)
            elif snap["progress"].get("step", 0) >= 1 and route_len >= 1:
                clock += 40
                add("progress", {"pin_id": snap["snapshot_id"],
                                 "step": snap["progress"]["step"],
                                 "state": snap["progress"]["state"],
                                 "vehicle_id": vehicle}, clock)
            prev = snap["snapshot_id"]

        import_key = body.get("import_key") or f"EV-{vehicle}-" + \
            datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        return {"import_key": import_key, "vehicle_id": vehicle, "source": source,
                "from_node": snaps[0]["from_node"], "to_node": snaps[-1]["to_node"],
                "live_version": snaps[-1]["version"]["version"], "events": events}

    def build_template_events(self, body: dict) -> dict:
        """READ-ONLY authoring aid: produce a correctly signed evidence chain
        from the current live state (for operators importing a demo bundle).
        Creates no archive and writes nothing."""
        live = self._live_snapshot()
        eff = self.live.resolve_effective(live)
        vehicle = body.get("vehicle_id") or "VH-AUDIT-01"
        start = body.get("from_node") or "A00/w"
        goal = body.get("to_node") or "B01/e"
        source = body.get("source") or "ops-import/template-v1"

        tile_objs = [live["tiles"][h] for h in eff["tiles"].values()]
        g0 = build_graph(tile_objs, eff["overlays"])
        steps0 = dijkstra(g0, start, goal)
        if steps0 is None:
            raise DomainError("ROUTE_IMPOSSIBLE",
                              f"当前线上版本 {eff['version']} 下 {start} -> {goal} 不可达", 422)
        used = self._used_tiles(steps0, eff)
        patches = [minimal_patch(p) for p in eff["patches"]]
        overlays = {t: ov for t, ov in eff["overlays"].items() if t in used}
        pin_id = f"PIN-{vehicle}-1"
        pin_payload = self._pin_payload_dict(
            pin_id, vehicle, eff, used, patches, overlays, steps0, start, goal)

        ts0 = datetime.now(timezone.utc)
        events = []

        def add(etype, payload, at, src=source):
            seq = len(events) + 1
            parent = events[-1]["digest"] if events else None
            ts = at.replace(microsecond=0).isoformat()
            digest = event_digest(seq, ts, src, etype, payload, parent)
            events.append({"seq": seq, "ts": ts, "source": src, "type": etype,
                           "payload": payload, "parent_digest": parent, "digest": digest})

        add("route_pinned", pin_payload, ts0)
        if len(steps0) > 1:
            add("progress", {"pin_id": pin_id, "step": 1, "state": "on_lane",
                             "vehicle_id": vehicle}, ts0 + timedelta(seconds=40))
            add("progress", {"pin_id": pin_id, "step": 1, "state": "at_node",
                             "vehicle_id": vehicle}, ts0 + timedelta(seconds=80))

        # counterfactual in live world: a closure ahead allows a node-level reroute
        child_steps = None
        if eff["overlays"] and len(steps0) > 1:
            node = node_key(steps0[1]["from_tile"], steps0[1]["from_node"])
            child_steps = dijkstra(g0, node, goal)
        if child_steps is not None and self._closure_between(eff["overlays"], steps0[1:]):
            used2 = self._used_tiles(child_steps, eff)
            ov2 = {t: ov for t, ov in eff["overlays"].items() if t in used2}
            child_id = f"PIN-{vehicle}-2"
            child_payload = self._pin_payload_dict(
                child_id, vehicle, eff, used2, patches, ov2, child_steps, node, goal,
                extra={"parent_snapshot_id": pin_id})
            add("reroute_adopted",
                {"parent_pin_id": pin_id, "reason": "emergency_closure_node_transition",
                 "child": child_payload, "vehicle_id": vehicle},
                ts0 + timedelta(seconds=120))
            add("progress", {"pin_id": child_id, "step": 1, "state": "on_lane",
                             "vehicle_id": vehicle}, ts0 + timedelta(seconds=160))
            add("trip_completed", {"pin_id": child_id, "vehicle_id": vehicle},
                ts0 + timedelta(seconds=220))
        else:
            add("trip_completed", {"pin_id": pin_id, "vehicle_id": vehicle},
                ts0 + timedelta(seconds=160))

        import_key = body.get("import_key") or f"EV-{vehicle}-" + \
            datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        return {"import_key": import_key, "vehicle_id": vehicle, "source": source,
                "from_node": start, "to_node": goal,
                "live_version": eff["version"], "events": events}

    @staticmethod
    def _closure_between(overlays, steps) -> bool:
        for st in steps:
            if st["kind"] == "lane" and overlays.get(st["tile"], {}).get(st["lane"], {}).get("closed"):
                return True
        return False

    @staticmethod
    def _used_tiles(steps, eff) -> dict:
        used: dict[str, str] = {}
        for st in steps:
            if st["kind"] == "lane":
                used[st["tile"]] = eff["tiles"][st["tile"]]
            else:
                used[st["from_tile"]] = eff["tiles"][st["from_tile"]]
                used[st["to_tile"]] = eff["tiles"][st["to_tile"]]
        return used

    @staticmethod
    def _pin_payload_dict(pin_id, vehicle, eff, used, patches, overlays,
                          steps, start, goal, extra=None) -> dict:
        p = {"pin_id": pin_id, "vehicle_id": vehicle,
             "version": eff["version"], "descriptor_hash": "",
             "base": copy.deepcopy(eff["base"]),
             "tiles": dict(sorted(used.items())),
             "patches": copy.deepcopy(patches),
             "overlays": copy.deepcopy(overlays),
             "route": steps, "from_node": start, "to_node": goal}
        p["descriptor_hash"] = descriptor_hash(
            p["version"], p["base"], p["tiles"], p["patches"], p["overlays"])
        if extra:
            p.update(extra)
        return p
