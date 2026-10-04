"""
HD-map tile zoned release & vehicle route snapshot engine.

Core invariants
---------------
1. Tile content is content-addressed (sha256) and immutable once stored.
2. A *release* bumps a per-region generation counter; a region generation pins a
   complete, self-consistent tile set. Multi-region releases are atomic.
3. Emergency patches are rule-only overlays (lane closure) pinned to a base
   version. They never change geometry/connectivity, so they are the only kind
   of update that may interact with an in-flight trip - via a safe transition
   plan at a node boundary, never by splicing topology mid-lane.
4. A route *snapshot* fixes one version descriptor (release heads + ordered
   active patches) and one tile set. Re-fetching a snapshot always reproduces
   the exact same bytes/path; newer releases cannot enter it.
5. Every state transition is append/audit logged and every write is
   all-or-nothing (deep-copy mutate -> atomic temp file + os.replace).
"""

from __future__ import annotations

import copy
import hashlib
import heapq
import json
import os
import threading
from datetime import datetime, timezone
from typing import Any, Callable, Optional

EPS = 1e-6


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def canonical(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def sha256(obj: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical(obj)).hexdigest()


def short(h: str, n: int = 10) -> str:
    return h.replace("sha256:", "")[:n]


class DomainError(Exception):
    def __init__(self, code: str, message: str, status: int = 400, details: Any = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status
        self.details = details or {}

    def to_body(self) -> dict:
        return {"error": {"code": self.code, "message": self.message, "details": self.details}}


# ---------------------------------------------------------------- topology ---

def node_key(tile_id: str, node_id: str) -> str:
    return f"{tile_id}/{node_id}"


def build_graph(tile_objs: list[dict], overlays: Optional[dict] = None) -> dict:
    """Return {nodes: {key: (x,y)}, adj: {key: [edge]}}.

    edge = {"kind":"lane"|"seam", "tile","lane","to","speed","closed",
            "from_tile","from_node","to_tile","to_node"}
    """
    overlays = overlays or {}
    nodes: dict[str, tuple[float, float]] = {}
    adj: dict[str, list[dict]] = {}
    by_id: dict[str, dict] = {}

    for t in tile_objs:
        by_id[t["tile_id"]] = t
        for n in t.get("nodes", []):
            nodes[node_key(t["tile_id"], n["node_id"])] = (n["x"], n["y"])
        adj.setdefault  # noqa  (placeholder to keep linters calm)
        for ln in t.get("lanes", []):
            u = node_key(t["tile_id"], ln["from"])
            v = node_key(t["tile_id"], ln["to"])
            closed = bool(overlays.get(t["tile_id"], {}).get(ln["lane_id"], {}).get("closed"))
            edge = {
                "kind": "lane",
                "tile": t["tile_id"],
                "lane": ln["lane_id"],
                "speed": ln.get("speed", 0),
                "two_way": bool(ln.get("two_way", True)),
                "closed": closed,
            }
            adj.setdefault(u, []).append({**edge, "to": v})
            if ln.get("two_way", True):
                adj.setdefault(v, []).append({**edge, "to": u})

    seam_pairs: set[tuple[str, str]] = set()
    for t in tile_objs:
        for p in t.get("ports", []):
            peer = p.get("peer")
            if not peer:
                continue
            t2 = by_id.get(peer["tile"])
            if t2 is None:
                continue
            p2 = next((q for q in t2.get("ports", []) if q["port_id"] == peer["port"]), None)
            if p2 is None:
                continue
            u = node_key(t["tile_id"], p["node"])
            v = node_key(t2["tile_id"], p2["node"])
            a, b = sorted([u, v])
            if (a, b) in seam_pairs:
                continue
            seam_pairs.add((a, b))
            adj.setdefault(u, []).append(
                {"kind": "seam", "tile": None, "lane": None, "speed": 0,
                 "two_way": True, "closed": False, "to": v,
                 "via_port": f"{t['tile_id']}:{p['port_id']}~{t2['tile_id']}:{p2['port_id']}"})
            adj.setdefault(v, []).append(
                {"kind": "seam", "tile": None, "lane": None, "speed": 0,
                 "two_way": True, "closed": False, "to": u,
                 "via_port": f"{t2['tile_id']}:{p2['port_id']}~{t['tile_id']}:{p['port_id']}"})
    return {"nodes": nodes, "adj": adj}


def _edge_cost(nodes, e: dict) -> float:
    if e["kind"] == "seam":
        return 1.0  # crossing a matched boundary is (near) free
    (x1, y1), (x2, y2) = nodes[e["_from"]], nodes[e["to"]]
    dist = ((x1 - x2) ** 2 + (y1 - y2) ** 2) ** 0.5
    speed = max(float(e.get("speed") or 1), 1.0)
    return dist / speed


def dijkstra(graph: dict, start: str, goal: str) -> Optional[list[dict]]:
    nodes, adj = graph["nodes"], graph["adj"]
    if start not in nodes or goal not in nodes:
        return None
    dist = {start: 0.0}
    prev: dict[str, Optional[tuple[str, dict]]] = {start: None}
    pq = [(0.0, start)]
    while pq:
        d, u = heapq.heappop(pq)
        if d != dist.get(u):
            continue
        if u == goal:
            break
        for e in adj.get(u, []):
            if e.get("closed"):
                continue
            v = e["to"]
            full = dict(e, _from=u)
            nd = d + _edge_cost(nodes, full)
            if nd < dist.get(v, float("inf")):
                dist[v] = nd
                prev[v] = (u, full)
                heapq.heappush(pq, (nd, v))
    if goal not in prev:
        return None
    steps: list[dict] = []
    cur = goal
    while prev[cur] is not None:
        u, e = prev[cur]
        steps.append(_step_from_edge(u, cur, e))
        cur = u
    steps.reverse()
    return steps


def _step_from_edge(u: str, v: str, e: dict) -> dict:
    ut, un = u.split("/", 1)
    vt, vn = v.split("/", 1)
    if e["kind"] == "seam":
        return {"kind": "seam", "from_tile": ut, "from_node": un,
                "to_tile": vt, "to_node": vn, "via_port": e.get("via_port")}
    return {"kind": "lane", "tile": e["tile"], "lane": e["lane"],
            "speed": e.get("speed"), "from_tile": ut, "from_node": un,
            "to_tile": vt, "to_node": vn}


# ------------------------------------------------------------------- engine ---

class MapEngine:
    def __init__(self, path: str, clock: Callable[[], str] = utcnow_iso):
        self.path = path
        self.now = clock
        self.lock = threading.RLock()
        self.state: dict = {}
        self._load()

    # ------------------------------------------------- persistence ----------
    def _load(self):
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as f:
                self.state = json.load(f)
            # forward-compatible migration for stores written before rollouts
            self.state.setdefault("rollouts", {})
            self.state.setdefault("seq", {}).setdefault("rollout", 0)
        else:
            self.state = self._blank()
            self.seed()
            self._persist_locked()

    @staticmethod
    def _blank() -> dict:
        return {
            "tiles": {},          # content_hash -> tile object
            "candidates": {},     # candidate_id -> record
            "releases": [],       # append-only release log
            "region_heads": {},   # region -> release_id
            "region_gen": {},     # region -> int
            "patches": {},        # patch_id -> record
            "snapshots": {},      # snapshot_id -> record (immutable)
            "vehicles": {},       # vehicle_id -> {snapshots:[ids], last_event_id}
            "corridors": [],      # cross-region acceptance routes
            "rollouts": {},       # rollout_id -> canary batch (vehicle-group trial)
            "events": [],
            "seq": {"candidate": 0, "release": 0, "patch": 0, "snapshot": 0,
                    "event": 0, "rollout": 0},
        }

    def _persist_locked(self):
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        tmp = f"{self.path}.tmp.{os.getpid()}.{threading.get_ident()}"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.state, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)

    def _next(self, s: dict, key: str, prefix: str) -> str:
        s["seq"][key] += 1
        return f"{prefix}-{s['seq'][key]:04d}"

    def _event_locked(self, etype: str, summary: str, ref: str) -> dict:
        self.state["seq"]["event"] += 1
        ev = {"event_id": self.state["seq"]["event"], "ts": self.now(),
              "type": etype, "summary": summary, "ref": ref}
        self.state["events"].append(ev)
        return ev

    def commit(self, mutate: Callable[[dict], Any]) -> Any:
        """Run mutate(copy) then atomically persist; all-or-nothing."""
        with self.lock:
            draft = copy.deepcopy(self.state)
            result = mutate(draft)
            # persist first; only swap state in after durable write succeeds
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            tmp = f"{self.path}.tmp.{os.getpid()}.{threading.get_ident()}"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(draft, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
            self.state = draft
            return result

    # --------------------------------------------------------- tiles --------
    @staticmethod
    def _validate_tile_schema(t: dict) -> None:
        for f in ("tile_id", "region", "nodes", "lanes", "ports"):
            if f not in t:
                raise DomainError("TILE_SCHEMA", f"tile missing field: {f}", 400)
        ids = [n["node_id"] for n in t["nodes"]]
        if len(ids) != len(set(ids)):
            raise DomainError("TILE_SCHEMA", "duplicate node ids in tile", 400)
        node_ids = set(ids)
        lane_ids = set()
        for ln in t["lanes"]:
            if ln["lane_id"] in lane_ids:
                raise DomainError("TILE_SCHEMA", "duplicate lane id", 400)
            lane_ids.add(ln["lane_id"])
            if ln["from"] not in node_ids or ln["to"] not in node_ids:
                raise DomainError("TILE_SCHEMA",
                                  f"lane {ln['lane_id']} references unknown node", 400)
        for p in t["ports"]:
            if p["node"] not in node_ids:
                raise DomainError("TILE_SCHEMA",
                                  f"port {p['port_id']} references unknown node", 400)

    def put_tile(self, tile: dict) -> dict:
        self._validate_tile_schema(tile)
        h = sha256(tile)
        existing = self.state["tiles"].get(h)
        if existing:
            return {"hash": h, "stored": False, "tile_id": tile["tile_id"]}

        def m(s):
            s["tiles"][h] = tile
            self._event_locked_run(s, "TILE_IMPORTED",
                                   f"导入瓦片 {tile['tile_id']} ({tile.get('version_tag', '?')})", h)
            return None

        self.commit(m)
        return {"hash": h, "stored": True, "tile_id": tile["tile_id"]}

    @staticmethod
    def _event_locked_run(s, etype, summary, ref):
        s["seq"]["event"] += 1
        s["events"].append({"event_id": s["seq"]["event"], "ts": utcnow_iso(),
                            "type": etype, "summary": summary, "ref": ref})

    def list_tiles(self) -> list[dict]:
        out = []
        for h, t in self.state["tiles"].items():
            out.append({"hash": h, "tile_id": t["tile_id"], "region": t["region"],
                        "version_tag": t.get("version_tag", ""),
                        "nodes": len(t["nodes"]), "lanes": len(t["lanes"]),
                        "ports": len(t["ports"])})
        return sorted(out, key=lambda x: (x["tile_id"], x["hash"]))

    # -------------------------------------------------- baseline/views ------
    def _release(self, s: dict, release_id: str) -> Optional[dict]:
        return next((r for r in s["releases"] if r["release_id"] == release_id), None)

    def _head_release_id(self, s: dict, region: str) -> Optional[str]:
        return s["region_heads"].get(region)

    def _region_tile_map(self, s: dict, region: str, release_id: Optional[str] = None) -> dict:
        """tile_id -> hash for a region at a given (or head) release."""
        rid = release_id or s["region_heads"].get(region)
        rel = self._release(s, rid) if rid else None
        if not rel:
            return {}
        return dict(rel["tiles"])

    def _working_tile_map(self, s: dict, candidate: Optional[dict] = None) -> dict:
        """tile_id -> tile_obj: candidate versions override published heads."""
        tile_map: dict[str, dict] = {}
        candidate_tiles: dict[str, str] = {}
        candidate_regions: set[str] = set()
        if candidate:
            candidate_tiles = dict(candidate["tiles"])
            candidate_regions = set(candidate["regions"])
        for region, rid in s["region_heads"].items():
            if region in candidate_regions:
                continue
            rel = self._release(s, rid)
            # an old multi-region release may carry other regions' tiles;
            # only take the tiles that belong to this region's head
            for tid, h in rel["tiles"].items():
                if s["tiles"][h]["region"] == region:
                    tile_map[tid] = s["tiles"][h]
        for tid, h in candidate_tiles.items():
            if h not in s["tiles"]:
                raise DomainError("TILE_MISSING", f"tile content {short(h)} missing", 409)
            tile_map[tid] = s["tiles"][h]
        return tile_map

    def _published_tile_map_all(self, s: dict) -> dict:
        return self._working_tile_map(s, None)

    # ------------------------------------------------- validation -----------
    def validate_tile_map(self, s: dict, tile_map: dict, candidate_regions: Optional[set] = None
                          ) -> tuple[list[dict], list[dict]]:
        errors: list[dict] = []
        warnings: list[dict] = []
        candidate_regions = candidate_regions or set()

        # 1) per-tile integrity + orphan ports
        for tid, t in sorted(tile_map.items()):
            node_ids = {n["node_id"] for n in t["nodes"]}
            seen_lanes = set()
            for ln in t["lanes"]:
                if ln["lane_id"] in seen_lanes:
                    errors.append(self._err("DANGLING_LANE", tid, f"重复车道 {ln['lane_id']}"))
                seen_lanes.add(ln["lane_id"])
                if ln["from"] not in node_ids or ln["to"] not in node_ids:
                    errors.append(self._err("DANGLING_LANE", tid,
                                            f"车道 {ln['lane_id']} 端点不在瓦片内",
                                            {"lane": ln["lane_id"]}))
            for p in t["ports"]:
                peer = p.get("peer")
                if not peer:
                    if not p.get("external"):
                        errors.append(self._err("SEAM_ORPHAN_PORT", tid,
                                                f"端口 {p['port_id']} 既无对端也非外边界",
                                                {"port": p["port_id"]}))
                    continue
                t2 = tile_map.get(peer["tile"])
                if t2 is None:
                    errors.append(self._err("SEAM_NO_PEER", tid,
                                            f"端口 {p['port_id']} 指向的瓦片 {peer['tile']} 不在工作集",
                                            {"port": p["port_id"], "peer_tile": peer["tile"]}))
                    continue
                p2 = next((q for q in t2["ports"] if q["port_id"] == peer["port"]), None)
                if p2 is None:
                    errors.append(self._err("SEAM_NO_PEER", tid,
                                            f"对端瓦片 {peer['tile']} 无端口 {peer['port']}",
                                            {"port": p["port_id"]}))
                    continue
                # reciprocal declaration
                if not p2.get("peer") or p2["peer"].get("tile") != tid or \
                        p2["peer"].get("port") != p["port_id"]:
                    errors.append(self._err("SEAM_NOT_RECIPROCAL", tid,
                                            f"接缝 {tid}:{p['port_id']} <-> "
                                            f"{peer['tile']}:{peer['port']} 未相互声明",
                                            {"port": p["port_id"]}))
                # coordinate coincidence (global frame)
                if abs(p["x"] - p2["x"]) > EPS or abs(p["y"] - p2["y"]) > EPS:
                    errors.append(self._err("SEAM_COORD_MISMATCH", tid,
                                            f"接缝端口坐标不一致 "
                                            f"({p['x']},{p['y']}) vs ({p2['x']},{p2['y']})",
                                            {"port": p["port_id"]}))
                # socket profile compatibility
                prof, prof2 = p.get("profile", {}), p2.get("profile", {})
                if prof.get("speed") != prof2.get("speed"):
                    errors.append(self._err("SEAM_PROFILE_MISMATCH", tid,
                                            f"接缝限速不一致 {prof.get('speed')} vs "
                                            f"{prof2.get('speed')} (@{tid}:{p['port_id']})",
                                            {"port": p["port_id"], "attr": "speed",
                                             "a": prof.get("speed"), "b": prof2.get("speed")}))
                if prof.get("lanes") != prof2.get("lanes"):
                    errors.append(self._err("SEAM_PROFILE_MISMATCH", tid,
                                            f"接缝车道数不一致 {prof.get('lanes')} vs "
                                            f"{prof2.get('lanes')} (@{tid}:{p['port_id']})",
                                            {"port": p["port_id"], "attr": "lanes"}))

        # 2) region completeness (candidate must restate every tile of its regions)
        for region in candidate_regions:
            rid = s["region_heads"].get(region)
            if rid:
                prev_ids = {tid for tid, h in self._release(s, rid)["tiles"].items()
                            if s["tiles"][h]["region"] == region}
                now_ids = {tid for tid, t in tile_map.items() if t["region"] == region}
                missing = prev_ids - now_ids
                if missing:
                    errors.append(self._err("REGION_INCOMPLETE", region,
                                            f"区域 {region} 候选未覆盖既有瓦片: "
                                            f"{sorted(missing)}",
                                            {"missing": sorted(missing)}))

        # 3) cross-region corridors must be traversable
        graph = build_graph(list(tile_map.values()))
        for cor in s["corridors"]:
            wps = cor["waypoints"]
            ok = True
            broken_segment = None
            for a, b in zip(wps, wps[1:]):
                if a not in graph["nodes"] or b not in graph["nodes"]:
                    ok, broken_segment = False, [a, b]
                    break
                if dijkstra(graph, a, b) is None:
                    ok, broken_segment = False, [a, b]
                    break
            if not ok:
                errors.append(self._err("ROUTE_BROKEN", cor["corridor_id"],
                                        f"跨区走廊 {cor['corridor_id']} 无法贯通: "
                                        f"{broken_segment[0]} -> {broken_segment[1]}",
                                        {"corridor": cor["corridor_id"],
                                         "segment": broken_segment}))
        return errors, warnings

    @staticmethod
    def _err(code: str, where: str, message: str, extra: Optional[dict] = None) -> dict:
        d = {"code": code, "where": where, "message": message}
        if extra:
            d.update(extra)
        return d

    # ------------------------------------------------- candidates -----------
    def create_candidate(self, body: dict) -> dict:
        regions = body.get("regions")
        tiles = body.get("tiles")
        if not regions or not tiles:
            raise DomainError("BAD_REQUEST", "regions 与 tiles 必填", 400)

        def m(s):
            for item in tiles:
                if item["hash"] not in s["tiles"]:
                    raise DomainError("TILE_MISSING",
                                      f"瓦片 {item['tile_id']} 内容 {short(item['hash'])} 未入库",
                                      400)
                t = s["tiles"][item["hash"]]
                if t["tile_id"] != item["tile_id"]:
                    raise DomainError("TILE_ID_CONFLICT",
                                      f"hash 对应 {t['tile_id']}，与声明 {item['tile_id']} 不符", 400)
                if t["region"] not in regions:
                    raise DomainError("REGION_MISMATCH",
                                      f"瓦片 {item['tile_id']} 属于区域 {t['region']}，"
                                      f"不在候选区域 {regions} 内", 400)
            cid = "RC-" + self._next(s, "candidate", "RC").split("-")[1]
            based_on = {r: s["region_gen"].get(r, 0) for r in regions}
            cand = {
                "candidate_id": cid,
                "regions": list(regions),
                "tiles": {it["tile_id"]: it["hash"] for it in tiles},
                "note": body.get("note", ""),
                "created_by": body.get("created_by", "editor"),
                "created_at": self.now(),
                "based_on_generations": based_on,
                "status": "draft",
                "validation": None,
            }
            s["candidates"][cid] = cand
            self._event_locked_run(s, "CANDIDATE_CREATED",
                                   f"创建候选 {cid}（区域 {','.join(regions)}）", cid)
            return copy.deepcopy(cand)

        return self.commit(m)

    def validate_candidate(self, cid: str) -> dict:
        if cid not in self.state["candidates"]:
            raise DomainError("NOT_FOUND", f"候选 {cid} 不存在", 404)

        def m(s):
            cand = s["candidates"][cid]
            tile_map = self._working_tile_map(s, cand)
            errors, warnings = self.validate_tile_map(s, tile_map, set(cand["regions"]))
            cand["validation"] = {
                "status": "passed" if not errors else "failed",
                "errors": errors,
                "warnings": warnings,
                "checked_at": self.now(),
                "baseline_generations": {r: s["region_gen"].get(r, 0) for r in cand["regions"]},
            }
            if errors:
                cand["status"] = "rejected"
            elif cand["status"] in ("draft", "rejected"):
                cand["status"] = "submitted"
            return copy.deepcopy(cand["validation"])

        return self.commit(m)

    def publish_candidate(self, cid: str) -> dict:
        if cid not in self.state["candidates"]:
            raise DomainError("NOT_FOUND", f"候选 {cid} 不存在", 404)

        def m(s):
            cand = s["candidates"][cid]
            if cand["status"] == "published":
                raise DomainError("ALREADY_PUBLISHED", f"候选 {cid} 已发布", 409)
            return copy.deepcopy(self._publish_locked(s, cand))

        return self.commit(m)

    def _publish_locked(self, s: dict, cand: dict) -> dict:
        """Atomic publish core shared by direct publish and rollout promote.

        Runs inside a commit(): any raise discards the whole draft, so a
        failed publish never leaves a partial region switch behind.
        """
        cid = cand["candidate_id"]
        # optimistic concurrency: regions must not have moved since baseline
        for r in cand["regions"]:
            base_gen = cand["based_on_generations"].get(r, 0)
            cur_gen = s["region_gen"].get(r, 0)
            if base_gen != cur_gen:
                raise DomainError("REGION_GENERATION_CONFLICT",
                                  f"区域 {r} 已从第 {base_gen} 代推进到第 {cur_gen} 代，"
                                  f"候选必须基于新基线重新校验后才能发布",
                                  409, {"region": r, "baseline": base_gen,
                                        "current": cur_gen})
        # re-validate atomically against the live baseline
        tile_map = self._working_tile_map(s, cand)
        errors, _ = self.validate_tile_map(s, tile_map, set(cand["regions"]))
        if errors:
            cand["validation"] = {"status": "failed", "errors": errors,
                                  "warnings": [], "checked_at": self.now(),
                                  "baseline_generations": {
                                      r: s["region_gen"].get(r, 0)
                                      for r in cand["regions"]}}
            cand["status"] = "rejected"
            self._persist_draft_failure(s)
            raise DomainError("VALIDATION_FAILED",
                              f"候选 {cid} 接缝/跨区路线校验未通过，发布已中止",
                              422, {"errors": errors})

        rid = "REL-" + self._next(s, "release", "REL").split("-")[1]
        new_gens = {}
        for r in cand["regions"]:
            s["region_gen"][r] = s["region_gen"].get(r, 0) + 1
            new_gens[r] = s["region_gen"][r]
        parent = {r: s["region_heads"].get(r) for r in cand["regions"]}
        release = {
            "release_id": rid,
            "candidate_id": cid,
            "regions": list(cand["regions"]),
            "generations": new_gens,
            "tiles": {tid: h for tid, h in cand["tiles"].items()
                      if s["tiles"][h]["region"] in cand["regions"]},
            "parent_releases": parent,
            "note": cand.get("note", ""),
            "created_at": self.now(),
        }
        s["releases"].append(release)
        for r in cand["regions"]:
            s["region_heads"][r] = rid
        cand["status"] = "published"
        cand["published_as"] = rid
        self._event_locked_run(
            s, "RELEASE_PUBLISHED",
            f"发布 {rid}：" + "，".join(f"{r} 第 {g} 代" for r, g in new_gens.items()),
            rid)
        return release

    def _persist_draft_failure(self, s):
        # validation failure itself is an auditable state change (handled by commit)
        s["seq"]["event"] += 1
        s["events"].append({"event_id": s["seq"]["event"], "ts": self.now(),
                            "type": "PUBLISH_REJECTED",
                            "summary": "发布失败：校验未通过，未产生任何新版本",
                            "ref": "publish"})

    def list_releases(self) -> list[dict]:
        return copy.deepcopy(self.state["releases"])

    # -------------------------------------------------- effective version ---
    def _active_patches(self, s: dict) -> list[dict]:
        return sorted(
            (p for p in s["patches"].values() if p["status"] == "active"),
            key=lambda p: (-int(p.get("priority", 0)), p["created_at"], p["patch_id"]))

    def resolve_effective(self, s: Optional[dict] = None,
                          trial_rollouts: Optional[list] = None) -> dict:
        """Effective version descriptor.

        With ``trial_rollouts`` (active canary batches the requester belongs
        to), the tiles of each batch's candidate regions are overlaid on the
        published heads — the *pending promotion* content. Region generations
        themselves never move during a trial. Without trial batches the
        descriptor is byte-identical to the published-only resolution.
        """
        s = s or self.state
        base = {}
        tiles: dict[str, str] = {}
        for region, rid in sorted(s["region_heads"].items()):
            rel = self._release(s, rid)
            base[region] = {"release_id": rid, "generation": rel["generations"][region]}
            for tid, h in rel["tiles"].items():
                # region heads only contribute tiles of their own region
                # (an old joint release must not override another head)
                if s["tiles"][h]["region"] == region:
                    tiles[tid] = h
        trial_refs = []
        for b in (trial_rollouts or []):
            cand = s["candidates"].get(b["candidate_id"])
            if not cand:
                continue
            for tid, h in cand["tiles"].items():
                if s["tiles"][h]["region"] in b["regions"]:
                    tiles[tid] = h
            trial_refs.append({"rollout_id": b["rollout_id"],
                               "candidate_id": b["candidate_id"],
                               "regions": list(b["regions"])})
        active = self._active_patches(s)
        overlays: dict[str, dict] = {}
        for p in active:
            for tid, lanes in p["scope"]["tiles"].items():
                for lid in lanes:
                    overlays.setdefault(tid, {})[lid] = {
                        "closed": True, "patch_id": p["patch_id"],
                        "reason": p.get("reason", "")}
        gen_part = "/".join(f"{r}={b['generation']}" for r, b in base.items())
        patch_part = ",".join(
            f"{p['patch_id']}[p{p.get('priority', 0)}]" for p in active)
        trial_part = "~" + ",".join(t["rollout_id"] for t in trial_refs) if trial_refs else ""
        version = f"{gen_part or 'empty'}" + (f"+{patch_part}" if patch_part else "") + trial_part
        descriptor = {
            "version": version,
            "descriptor_hash": "",
            "base": base,
            "tiles": tiles,
            "patches": [{"patch_id": p["patch_id"], "priority": p["priority"],
                         "scope": p["scope"], "reason": p.get("reason", ""),
                         "content_hash": p["content_hash"],
                         "activated_at": p.get("activated_at")} for p in active],
            "overlays": overlays,
            "resolved_at": self.now(),
        }
        if trial_refs:
            descriptor["rollouts"] = trial_refs
        descriptor["descriptor_hash"] = sha256({k: v for k, v in descriptor.items()
                                                if k != "descriptor_hash"})
        return descriptor

    # ------------------------------------------------------- patches --------
    def create_patch(self, body: dict) -> dict:
        scope_tiles = body.get("scope", {}).get("tiles")
        if not scope_tiles:
            raise DomainError("BAD_REQUEST", "scope.tiles 必须指定 {tile_id: [lane_id]}", 400)
        priority = int(body.get("priority", 100))

        def m(s):
            eff = self.resolve_effective(s)
            # compatibility: every target tile/lane must exist in the pinned base
            for tid, lanes in scope_tiles.items():
                if tid not in eff["tiles"]:
                    raise DomainError("PATCH_INCOMPATIBLE",
                                      f"补丁目标瓦片 {tid} 不在当前基础版本中", 422)
                tile = s["tiles"][eff["tiles"][tid]]
                lane_ids = {ln["lane_id"] for ln in tile["lanes"]}
                for lid in lanes:
                    if lid not in lane_ids:
                        raise DomainError("PATCH_INCOMPATIBLE",
                                          f"车道 {tid}/{lid} 在基础版本中不存在；"
                                          f"补丁不允许引入或删除拓扑", 422)
                    if eff["overlays"].get(tid, {}).get(lid, {}).get("closed"):
                        raise DomainError("PATCH_ALREADY_CLOSED",
                                          f"{tid}/{lid} 已被活动补丁封闭", 409)
            pid = "P-" + self._next(s, "patch", "P").split("-")[1]
            content = {"kind": "closure", "scope": {"tiles": scope_tiles},
                       "reason": body.get("reason", "emergency"),
                       "priority": priority,
                       "transition": body.get("transition", {"safe_state": "complete_current_lane"})}
            patch = {
                "patch_id": pid,
                "status": "proposed",
                "priority": priority,
                "kind": "closure",
                "scope": {"tiles": scope_tiles},
                "reason": body.get("reason", ""),
                "base_version": {"base": eff["base"], "tile_hashes": {
                    tid: eff["tiles"][tid] for tid in scope_tiles}},
                "transition": content["transition"],
                "detour": None,
                "compatibility": None,
                "content_hash": sha256(content),
                "created_by": body.get("created_by", "ops"),
                "created_at": self.now(),
                "activated_at": None,
                "revoked_at": None,
            }
            # verify a detour exists in base+overlay-with-closure (safe transition)
            tile_objs = [s["tiles"][h] for h in eff["tiles"].values()]
            ov = copy.deepcopy(eff["overlays"])
            for tid, lanes in scope_tiles.items():
                for lid in lanes:
                    ov.setdefault(tid, {})[lid] = {"closed": True, "patch_id": pid}
            g = build_graph(tile_objs, ov)
            detours = {}
            for tid, lanes in scope_tiles.items():
                tile = s["tiles"][eff["tiles"][tid]]
                for lid in lanes:
                    ln = next(x for x in tile["lanes"] if x["lane_id"] == lid)
                    a, b = node_key(tid, ln["from"]), node_key(tid, ln["to"])
                    via = dijkstra(g, a, b)
                    if via is None:
                        patch["compatibility"] = {
                            "status": "incompatible",
                            "reason": f"封闭 {tid}/{lid} 后无可用绕行，车辆无法安全过渡",
                            "checked_at": self.now()}
                    else:
                        detours[f"{tid}/{lid}"] = via
            if patch["compatibility"] is None:
                patch["detour"] = detours
                patch["compatibility"] = {
                    "status": "compatible",
                    "reason": "基础版本中存在目标车道，且封闭后存在节点间绕行路径",
                    "checked_at": self.now(),
                    "base_descriptor": eff["version"]}
            s["patches"][pid] = patch
            self._event_locked_run(s, "PATCH_PROPOSED",
                                   f"紧急补丁 {pid} 提交（{patch['compatibility']['status']}）",
                                   pid)
            return copy.deepcopy(patch)

        return self.commit(m)

    def activate_patch(self, pid: str) -> dict:
        if pid not in self.state["patches"]:
            raise DomainError("NOT_FOUND", f"补丁 {pid} 不存在", 404)

        def m(s):
            p = s["patches"][pid]
            if p["status"] == "active":
                raise DomainError("PATCH_ACTIVE", "补丁已处于活动状态", 409)
            if p["status"] == "revoked":
                raise DomainError("PATCH_REVOKED", "补丁已撤销，不可重新激活", 409)
            if p.get("compatibility", {}).get("status") != "compatible":
                raise DomainError("PATCH_INCOMPATIBLE",
                                  f"补丁 {pid} 与基础版本不兼容"
                                  f"（{p.get('compatibility', {}).get('reason') or '无安全绕行'}），"
                                  f"不允许生效；车辆无法安全过渡", 422)
            # re-check compatibility against the live base (may have moved)
            eff = self.resolve_effective(s)
            for tid, lanes in p["scope"]["tiles"].items():
                if eff["tiles"].get(tid) != p["base_version"]["tile_hashes"][tid]:
                    raise DomainError("PATCH_BASE_DRIFT",
                                      f"瓦片 {tid} 与补丁固化的基础版本不一致；"
                                      f"请基于当前版本重新生成补丁", 422)
                for lid in lanes:
                    if eff["overlays"].get(tid, {}).get(lid, {}).get("closed"):
                        raise DomainError("PATCH_ALREADY_CLOSED",
                                          f"{tid}/{lid} 已被其他补丁封闭", 409)
            p["status"] = "active"
            p["activated_at"] = self.now()
            self._event_locked_run(
                s, "PATCH_ACTIVATED",
                f"紧急补丁 {pid} 生效：" +
                "；".join(f"{t}/{l}" for t, ls in p["scope"]["tiles"].items() for l in ls) +
                f"（兼容 {eff['version']}，车辆须在节点处过渡）", pid)
            return copy.deepcopy(p)

        return self.commit(m)

    def revoke_patch(self, pid: str, reason: str = "") -> dict:
        if pid not in self.state["patches"]:
            raise DomainError("NOT_FOUND", f"补丁 {pid} 不存在", 404)

        def m(s):
            p = s["patches"][pid]
            if p["status"] != "active":
                raise DomainError("PATCH_NOT_ACTIVE",
                                  f"补丁状态为 {p['status']}，仅活动补丁可撤销", 409)
            p["status"] = "revoked"
            p["revoked_at"] = self.now()
            p["revoke_reason"] = reason
            self._event_locked_run(s, "PATCH_REVOKED",
                                   f"紧急补丁 {pid} 已撤销，车道恢复通行（既有在途快照仍保持其固定版本）",
                                   pid)
            return copy.deepcopy(p)

        return self.commit(m)

    def list_patches(self) -> list[dict]:
        return sorted((copy.deepcopy(p) for p in self.state["patches"].values()),
                      key=lambda p: p["created_at"], reverse=True)

    # -------------------------------------------------- canary rollouts -----
    # A rollout batch diverts *new* route requests from listed vehicles to a
    # validated candidate's pending content (trial group); every other vehicle
    # keeps reading the currently published content (control group). Batches
    # never move region generations and never rewrite issued snapshots; the
    # only exits are a one-shot promote (atomic publish) or rollback.

    @staticmethod
    def _norm_ts(v) -> Optional[str]:
        if not v:
            return None
        try:
            dt = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        except ValueError:
            raise DomainError("BAD_REQUEST", f"时间格式非法: {v}（需 ISO-8601）", 400)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()

    def _rollout_window_open(self, b: dict, now: Optional[str] = None) -> bool:
        now = now or self.now()
        w = b.get("window") or {}
        if w.get("start") and now < w["start"]:
            return False
        if w.get("end") and now > w["end"]:
            return False
        return True

    def _active_rollouts(self, s: dict) -> list[dict]:
        """Batches currently diverting traffic (active + inside window)."""
        now = self.now()
        return [b for b in s["rollouts"].values()
                if b["status"] == "active" and self._rollout_window_open(b, now)]

    @staticmethod
    def _rollout_group(b: dict, vehicle_id: str) -> str:
        return "trial" if vehicle_id in b["vehicles"] else "control"

    @staticmethod
    def _rollout_breaches(b: dict) -> list[str]:
        th = b.get("thresholds") or {}
        st = b["stats"]["trial"]
        breaches = []
        if th.get("max_unreachable") is not None and \
                st["unreachable"] > th["max_unreachable"]:
            breaches.append(f"试运行组不可达 {st['unreachable']} 次 > 阈值 "
                            f"{th['max_unreachable']}")
        if th.get("max_transition_rejected") is not None and \
                st["transition_rejected"] > th["max_transition_rejected"]:
            breaches.append(f"试运行组安全过渡拒绝 {st['transition_rejected']} 次 > 阈值 "
                            f"{th['max_transition_rejected']}")
        return breaches

    def _rollout_view(self, s: dict, b: dict) -> dict:
        v = copy.deepcopy(b)
        v["window_open"] = self._rollout_window_open(b)
        v["breaches"] = self._rollout_breaches(b)
        v["promotable"] = b["status"] == "active" and not v["breaches"]
        v["trial_version_preview"] = (
            self.resolve_effective(s, [b])["version"] if b["status"] == "active" else None)
        return v

    def create_rollout(self, body: dict) -> dict:
        cid = body.get("candidate_id")
        regions = body.get("regions")
        vehicles = body.get("vehicles")
        if not cid or not regions or not vehicles:
            raise DomainError("BAD_REQUEST", "candidate_id / regions / vehicles 必填", 400)
        if not isinstance(vehicles, list) or not all(isinstance(v, str) and v.strip()
                                                     for v in vehicles):
            raise DomainError("BAD_REQUEST", "vehicles 必须是非空车辆名单数组", 400)

        def m(s):
            cand = s["candidates"].get(cid)
            if not cand:
                raise DomainError("NOT_FOUND", f"候选 {cid} 不存在", 404)
            if cand["status"] == "published":
                raise DomainError("CANDIDATE_PUBLISHED",
                                  f"候选 {cid} 已发布，无需试运行", 409)
            if (cand.get("validation") or {}).get("status") != "passed":
                raise DomainError("ROLLOUT_CANDIDATE_NOT_VALIDATED",
                                  f"候选 {cid} 尚未通过接缝/跨区走廊校验，不能创建试运行批次",
                                  422)
            regions_l = list(dict.fromkeys(regions))
            unknown = [r for r in regions_l if r not in cand["regions"]]
            if unknown:
                raise DomainError("REGION_MISMATCH",
                                  f"区域 {unknown} 不在候选 {cid} 的作用区域 "
                                  f"{cand['regions']} 内", 400)
            # the pending content must still be seam-safe against the live baseline
            tile_map = self._working_tile_map(s, cand)
            errors, _ = self.validate_tile_map(s, tile_map, set(cand["regions"]))
            if errors:
                raise DomainError("VALIDATION_FAILED",
                                  f"候选 {cid} 相对当前基线校验失败，不能进入试运行",
                                  422, {"errors": errors})
            # one in-progress batch per region, no overlap
            for b in s["rollouts"].values():
                if b["status"] != "active":
                    continue
                overlap = sorted(set(b["regions"]) & set(regions_l))
                if overlap:
                    raise DomainError(
                        "ROLLOUT_REGION_CONFLICT",
                        f"区域 {','.join(overlap)} 已有进行中的试运行批次 "
                        f"{b['rollout_id']}，拒绝并发冲突",
                        409, {"regions": overlap, "conflict_with": b["rollout_id"]})
            window_in = body.get("window") or {}
            start = self._norm_ts(window_in.get("start")) or self.now()
            end = self._norm_ts(window_in.get("end"))
            if end and end <= start:
                raise DomainError("BAD_REQUEST", "观察窗口 end 必须晚于 start", 400)
            th = {}
            for k in ("max_unreachable", "max_transition_rejected"):
                v = (body.get("thresholds") or {}).get(k)
                if v is not None:
                    v = int(v)
                    if v < 0:
                        raise DomainError("BAD_REQUEST", "阈值不能为负", 400)
                    th[k] = v
            rid = self._next(s, "rollout", "RO")
            batch = {
                "rollout_id": rid,
                "candidate_id": cid,
                "regions": regions_l,
                "vehicles": list(dict.fromkeys(v.strip() for v in vehicles)),
                "window": {"start": start, "end": end},
                "thresholds": th,
                "status": "active",
                "stats": {g: {"requests": 0, "unreachable": 0, "transition_rejected": 0}
                          for g in ("trial", "control")},
                "base_generations": {r: s["region_gen"].get(r, 0) for r in regions_l},
                "created_by": body.get("created_by", "ops"),
                "created_at": self.now(),
                "decision": None,
            }
            s["rollouts"][rid] = batch
            self._event_locked_run(
                s, "ROLLOUT_CREATED",
                f"创建试运行批次 {rid}：候选 {cid} 作用于 {','.join(regions_l)}，"
                f"名单 {len(batch['vehicles'])} 台车辆使用待晋级内容"
                + (f"，观察窗口至 {end}" if end else "（无结束时间）"), rid)
            return self._rollout_view(s, batch)

        return self.commit(m)

    def list_rollouts(self) -> list[dict]:
        with self.lock:
            return sorted((self._rollout_view(self.state, b)
                           for b in self.state["rollouts"].values()),
                          key=lambda b: b["created_at"], reverse=True)

    def get_rollout(self, rid: str) -> dict:
        with self.lock:
            b = self.state["rollouts"].get(rid)
            if not b:
                raise DomainError("NOT_FOUND", f"试运行批次 {rid} 不存在", 404)
            return self._rollout_view(self.state, b)

    @staticmethod
    def _rollout_decision_result(batch: dict, idempotent: bool) -> dict:
        return {"rollout_id": batch["rollout_id"],
                "status": batch["status"],
                "decision": copy.deepcopy(batch["decision"]),
                "idempotent": idempotent}

    def _rollout_decided_replay(self, b: dict, action: str, rid: str) -> dict:
        """Verdict-once: repeating the same decision returns the first result;
        asking for the opposite exit is a conflict."""
        d = b.get("decision") or {}
        if d.get("action") == action:
            return self._rollout_decision_result(b, True)
        raise DomainError("ROLLOUT_ALREADY_DECIDED",
                          f"批次 {rid} 已{'晋级' if d.get('action') == 'promote' else '撤回'}"
                          f"（{d.get('at')}），不能再次反向操作；首次判定结果保持不变",
                          409, {"decision": d})

    def promote_rollout(self, rid: str, by: str = "ops") -> dict:
        with self.lock:
            b = self.state["rollouts"].get(rid)
            if not b:
                raise DomainError("NOT_FOUND", f"试运行批次 {rid} 不存在", 404)
            if b["status"] != "active":
                return self._rollout_decided_replay(b, "promote", rid)

        def m(s):
            batch = s["rollouts"][rid]
            breaches = self._rollout_breaches(batch)
            if breaches:
                raise DomainError("ROLLOUT_THRESHOLD_BREACH",
                                  "试运行组指标越过阈值，禁止晋级，应撤回批次："
                                  + "；".join(breaches),
                                  422, {"breaches": breaches,
                                        "stats": copy.deepcopy(batch["stats"])})
            cand = s["candidates"].get(batch["candidate_id"])
            if not cand:
                raise DomainError("NOT_FOUND",
                                  f"候选 {batch['candidate_id']} 不存在", 404)
            # atomic multi-region publish; any failure discards the whole draft
            release = self._publish_locked(s, cand)
            batch["status"] = "promoted"
            batch["decision"] = {
                "action": "promote", "at": self.now(), "by": by,
                "release_id": release["release_id"],
                "generations": release["generations"],
                "stats_at_decision": copy.deepcopy(batch["stats"])}
            self._event_locked_run(
                s, "ROLLOUT_PROMOTED",
                f"试运行批次 {rid} 一次性晋级：候选 {cand['candidate_id']} 发布为 "
                f"{release['release_id']}（" +
                "，".join(f"{r} 第 {g} 代" for r, g in release["generations"].items()) +
                "）；全部车辆此后读取新内容", rid)
            return self._rollout_decision_result(batch, False)

        return self.commit(m)

    def rollback_rollout(self, rid: str, reason: str = "", by: str = "ops") -> dict:
        with self.lock:
            b = self.state["rollouts"].get(rid)
            if not b:
                raise DomainError("NOT_FOUND", f"试运行批次 {rid} 不存在", 404)
            if b["status"] != "active":
                return self._rollout_decided_replay(b, "rollback", rid)

        def m(s):
            batch = s["rollouts"][rid]
            # Rollback only stops diverting *new* requests. It must not rewrite
            # issued trip snapshots nor move any region generation — and it
            # does neither: snapshots are immutable records and no release is
            # created here.
            batch["status"] = "rolled_back"
            batch["decision"] = {
                "action": "rollback", "at": self.now(), "by": by,
                "reason": reason,
                "stats_at_decision": copy.deepcopy(batch["stats"])}
            self._event_locked_run(
                s, "ROLLOUT_ROLLED_BACK",
                f"试运行批次 {rid} 已撤回（{reason or '未填原因'}）：名单车辆的新行程"
                f"恢复读取当前发布内容；已发出的行程快照与区域代际保持不变", rid)
            return self._rollout_decision_result(batch, False)

        return self.commit(m)

    # -------------------------------------------------- routes/snapshots ----
    def create_route(self, body: dict) -> dict:
        vehicle_id = body.get("vehicle_id", "VH-01")
        start, goal = body.get("from_node"), body.get("to_node")
        if not start or not goal:
            raise DomainError("BAD_REQUEST", "from_node / to_node 必填，格式 tile/node", 400)

        def m(s):
            active = self._active_rollouts(s)
            trial = [b for b in active
                     if self._rollout_group(b, vehicle_id) == "trial"]
            # listed vehicles read the pending-promotion content; everyone
            # else keeps reading the currently published content
            eff = self.resolve_effective(s, trial)
            tile_objs = [s["tiles"][h] for h in eff["tiles"].values()]
            g = build_graph(tile_objs, eff["overlays"])
            steps = dijkstra(g, start, goal)
            if steps is None:
                for b in active:
                    st = b["stats"][self._rollout_group(b, vehicle_id)]
                    st["requests"] += 1
                    st["unreachable"] += 1
                return {"_error": {
                    "code": "ROUTE_IMPOSSIBLE",
                    "message": f"当前有效版本 {eff['version']} 下 "
                               f"{start} -> {goal} 无可达路径（可能因封路）",
                    "status": 422,
                    "details": {"version": eff["version"]}}}
            for b in active:
                b["stats"][self._rollout_group(b, vehicle_id)]["requests"] += 1
            used_tiles = self._tiles_used_by_steps(s, steps, eff)
            sid = "S-" + self._next(s, "snapshot", "S").split("-")[1]
            snapshot = {
                "snapshot_id": sid,
                "vehicle_id": vehicle_id,
                "from_node": start,
                "to_node": goal,
                "status": "active",
                "created_at": self.now(),
                "version": {
                    "version": eff["version"],
                    "descriptor_hash": eff["descriptor_hash"],
                    "base": eff["base"],
                    "patches": eff["patches"],
                },
                "tiles": used_tiles,          # tile_id -> hash (the fixed set)
                "overlays": {t: ov for t, ov in eff["overlays"].items() if t in used_tiles},
                "route": steps,
                "progress": {"step": 0, "state": "at_node"},
                "parent_snapshot_id": None,
                "children": [],
                "completed_at": None,
                "rollouts": [{"rollout_id": b["rollout_id"],
                              "group": self._rollout_group(b, vehicle_id)}
                             for b in active],
            }
            s["snapshots"][sid] = snapshot
            s["vehicles"].setdefault(vehicle_id, {"snapshots": [], "last_event_id": 0})
            s["vehicles"][vehicle_id]["snapshots"].append(sid)
            group_note = ""
            if trial:
                group_note = ("，试运行 " + ",".join(b["rollout_id"] for b in trial)
                              + " 待晋级内容")
            elif active:
                group_note = "，对照组读取当前发布内容"
            self._event_locked_run(
                s, "SNAPSHOT_PINNED",
                f"车辆 {vehicle_id} 固定快照 {sid}（版本 {eff['version']}，"
                f"{len(used_tiles)} 瓦片，{len(steps)} 步{group_note}）", sid)
            return self._snapshot_view(s, snapshot)

        result = self.commit(m)
        if "_error" in result:  # unreachable counted & persisted, then reported
            e = result["_error"]
            raise DomainError(e["code"], e["message"], e["status"], e.get("details"))
        return result

    def _tiles_used_by_steps(self, s, steps, eff) -> dict:
        used: dict[str, str] = {}
        for st in steps:
            if st["kind"] == "lane":
                used[st["tile"]] = eff["tiles"][st["tile"]]
            else:
                used[st["from_tile"]] = eff["tiles"][st["from_tile"]]
                used[st["to_tile"]] = eff["tiles"][st["to_tile"]]
        return used

    def _snapshot_view(self, s: dict, snap: dict) -> dict:
        view = copy.deepcopy(snap)
        view["tile_contents"] = {tid: s["tiles"][h] for tid, h in snap["tiles"].items()}
        view["advisory"] = self._advisory(s, snap)
        return view

    def get_snapshot(self, sid: str) -> dict:
        with self.lock:
            snap = self.state["snapshots"].get(sid)
            if not snap:
                raise DomainError("NOT_FOUND", f"快照 {sid} 不存在", 404)
            return self._snapshot_view(self.state, snap)

    def update_progress(self, sid: str, body: dict) -> dict:
        def m(s):
            snap = s["snapshots"].get(sid)
            if not snap:
                raise DomainError("NOT_FOUND", f"快照 {sid} 不存在", 404)
            if snap["status"] != "active":
                raise DomainError("SNAPSHOT_CLOSED",
                                  f"快照状态为 {snap['status']}，不再接受进度", 409)
            step = int(body.get("step", snap["progress"]["step"]))
            state = body.get("state", snap["progress"]["state"])
            if state not in ("at_node", "on_lane"):
                raise DomainError("BAD_REQUEST", "state 仅支持 at_node / on_lane", 400)
            if not (0 <= step <= len(snap["route"])):
                raise DomainError("BAD_REQUEST", "step 超出路线范围", 400)
            snap["progress"] = {"step": step, "state": state, "ts": self.now()}
            return {"progress": copy.deepcopy(snap["progress"]),
                    "advisory": copy.deepcopy(self._advisory(s, snap))}

        return self.commit(m)

    def _advisory(self, s: dict, snap: dict) -> dict:
        """What a pinned trip must know without changing its topology."""
        notes = []
        closures_ahead = []
        current = self.resolve_effective(s)
        i = snap["progress"]["step"]
        on_lane = snap["progress"]["state"] == "on_lane"

        # newer generations: informational only, never spliced into this trip
        for region, b in snap["version"]["base"].items():
            head = current["base"].get(region)
            if head and head["generation"] > b["generation"]:
                notes.append({
                    "level": "new_generation",
                    "message": f"区域 {region} 已发布第 {head['generation']} 代"
                               f"（本快照固定第 {b['generation']} 代）；新拓扑仅在下次路线生效",
                    "region": region, "pinned_generation": b["generation"],
                    "current_generation": head["generation"]})

        pinned_ids = {p["patch_id"] for p in snap["version"]["patches"]}
        for p in self._active_patches(s):
            if p["patch_id"] in pinned_ids:
                continue
            for j, st in enumerate(snap["route"]):
                if st["kind"] != "lane":
                    continue
                if p["scope"]["tiles"].get(st["tile"]) and st["lane"] in \
                        p["scope"]["tiles"][st["tile"]]:
                    safe = not (on_lane and j == i)
                    closures_ahead.append({
                        "level": "reroute" if (j > i or (not on_lane and j >= i)) else "on_segment",
                        "patch_id": p["patch_id"], "reason": p.get("reason", ""),
                        "step_index": j,
                        "tile": st["tile"], "lane": st["lane"],
                        "must_finish_current_lane_first": on_lane,
                        "safe_to_switch_now": (not on_lane) and j >= i,
                        "message": ("封路位于剩余路线第 " if j != i else "封路位于当前车道 ")
                                   + f"{j} 步（{st['tile']}/{st['lane']}）"
                                   + ("；请完成当前车道后在节点处采用绕行" if on_lane
                                      else "；可在当前节点安全采用绕行快照")})

        # pinned-but-now-revoked patches: informational
        for pid in pinned_ids:
            if s["patches"][pid]["status"] == "revoked":
                notes.append({"level": "patch_revoked", "patch_id": pid,
                              "message": f"本快照固定的补丁 {pid} 已被撤销；"
                                         f"历史复算仍按封路执行，新路线不再封闭"})

        detour = None
        if closures_ahead:
            # plan detour from the current safe node to the original destination
            if on_lane and i < len(snap["route"]):
                from_node = node_key(snap["route"][i]["to_tile"], snap["route"][i]["to_node"])
            elif i < len(snap["route"]):
                from_node = node_key(snap["route"][i]["from_tile"], snap["route"][i]["from_node"])
            else:
                from_node = snap["to_node"]
            tile_objs = [s["tiles"][h] for h in current["tiles"].values()]
            g = build_graph(tile_objs, current["overlays"])
            detour = dijkstra(g, from_node, snap["to_node"])
        return {"closures_ahead": closures_ahead, "notes": notes,
                "detour_preview": detour,
                "current_version": current["version"]}

    def adopt_reroute(self, sid: str) -> dict:
        def m(s):
            old = s["snapshots"].get(sid)
            if not old:
                raise DomainError("NOT_FOUND", f"快照 {sid} 不存在", 404)
            if old["status"] != "active":
                raise DomainError("SNAPSHOT_CLOSED", "快照已关闭，无法过渡", 409)
            adv = self._advisory(s, old)
            if not adv["closures_ahead"]:
                raise DomainError("NO_ADVISORY", "当前无封路预警，无需过渡", 409)
            if old["progress"]["state"] != "at_node":
                # safe-transition refusal is a canary health signal: attribute
                # it to the batch groups this snapshot was pinned under
                for ref in old.get("rollouts", []):
                    b = s["rollouts"].get(ref["rollout_id"])
                    if b and ref["group"] in b["stats"]:
                        b["stats"][ref["group"]]["transition_rejected"] += 1
                return {"_error": {
                    "code": "NOT_AT_NODE",
                    "message": "车辆正在车道上：禁止拼接拓扑。"
                               "请先完成当前车道、到达节点后再过渡",
                    "status": 409,
                    "details": {"must_finish_current_lane_first": True}}}
            i = old["progress"]["step"]
            from_node = node_key(old["route"][i]["from_tile"], old["route"][i]["from_node"])
            eff = self.resolve_effective(s)
            tile_objs = [s["tiles"][h] for h in eff["tiles"].values()]
            g = build_graph(tile_objs, eff["overlays"])
            steps = dijkstra(g, from_node, old["to_node"])
            if steps is None:
                raise DomainError("ROUTE_IMPOSSIBLE", "当前版本下无安全绕行路径", 422)
            used = self._tiles_used_by_steps(s, steps, eff)
            new_id = "S-" + self._next(s, "snapshot", "S").split("-")[1]
            child = {
                "snapshot_id": new_id, "vehicle_id": old["vehicle_id"],
                "from_node": from_node, "to_node": old["to_node"], "status": "active",
                "created_at": self.now(),
                "version": {"version": eff["version"],
                            "descriptor_hash": eff["descriptor_hash"],
                            "base": eff["base"], "patches": eff["patches"]},
                "tiles": used,
                "overlays": {t: ov for t, ov in eff["overlays"].items() if t in used},
                "route": steps, "progress": {"step": 0, "state": "at_node"},
                "parent_snapshot_id": sid, "children": [], "completed_at": None,
                "adopted_reason": "emergency_patch_reroute",
            }
            old["status"] = "superseded"
            old["children"].append(new_id)
            s["snapshots"][new_id] = child
            s["vehicles"].setdefault(old["vehicle_id"], {"snapshots": [], "last_event_id": 0})
            s["vehicles"][old["vehicle_id"]]["snapshots"].append(new_id)
            self._event_locked_run(
                s, "SNAPSHOT_SUPERSEDED",
                f"车辆 {old['vehicle_id']} 在节点处由 {sid} 安全过渡至 {new_id}"
                f"（新版本 {eff['version']}，全程未拼接拓扑）", new_id)
            return self._snapshot_view(s, child)

        result = self.commit(m)
        if "_error" in result:  # refusal counted & persisted, then reported
            e = result["_error"]
            raise DomainError(e["code"], e["message"], e["status"], e.get("details"))
        return result

    def complete_snapshot(self, sid: str) -> dict:
        def m(s):
            snap = s["snapshots"].get(sid)
            if not snap:
                raise DomainError("NOT_FOUND", f"快照 {sid} 不存在", 404)
            snap["status"] = "completed"
            snap["completed_at"] = self.now()
            snap["progress"] = {"step": len(snap["route"]), "state": "at_node"}
            self._event_locked_run(s, "TRIP_COMPLETED",
                                   f"快照 {sid} 行程完成（版本 {snap['version']['version']}）",
                                   sid)
            return {"snapshot_id": sid, "status": "completed"}

        return self.commit(m)

    def vehicle_snapshots(self, vid: str) -> dict:
        with self.lock:
            v = self.state["vehicles"].get(vid)
            if not v:
                raise DomainError("NOT_FOUND", f"车辆 {vid} 不存在", 404)
            return {"vehicle_id": vid,
                    "snapshots": [self._snapshot_summary(self.state, sid)
                                  for sid in v["snapshots"]]}

    def _snapshot_summary(self, s: dict, sid: str) -> dict:
        snap = s["snapshots"][sid]
        return {"snapshot_id": sid, "status": snap["status"],
                "version": snap["version"]["version"],
                "from_node": snap["from_node"], "to_node": snap["to_node"],
                "created_at": snap["created_at"],
                "progress": snap["progress"],
                "parent_snapshot_id": snap.get("parent_snapshot_id"),
                "children": snap.get("children", []),
                "rollouts": snap.get("rollouts", []),
                "tiles": len(snap["tiles"]), "steps": len(snap["route"])}

    def vehicle_sync(self, vid: str, last_event_id: int) -> dict:
        """Offline catch-up: one explainable answer of everything missed."""
        with self.lock:
            s = self.state
            v = s["vehicles"].get(vid)
            if not v:
                raise DomainError("NOT_FOUND", f"车辆 {vid} 不存在", 404)
            missed = [copy.deepcopy(e) for e in s["events"] if e["event_id"] > last_event_id]
            current = self.resolve_effective(s)
            active_sid = next((sid for sid in reversed(v["snapshots"])
                               if s["snapshots"][sid]["status"] == "active"), None)
            payload = {"vehicle_id": vid, "replayed_from_event": last_event_id,
                       "missed_events": missed,
                       "current_version": {k: current[k] for k in
                                           ("version", "descriptor_hash", "base", "patches")},
                       "active_snapshot": None}
            if active_sid:
                snap = s["snapshots"][active_sid]
                payload["active_snapshot"] = self._snapshot_summary(s, active_sid)
                payload["active_snapshot"]["advisory"] = self._advisory(s, snap)
            v["last_event_id"] = s["seq"]["event"]
            self._persist_vehicle_checkpoint(vid, v["last_event_id"])
            return payload

    def _persist_vehicle_checkpoint(self, vid: str, eid: int):
        with self.lock:
            def m(s):
                s["vehicles"][vid]["last_event_id"] = eid
                return None
            self.commit(m)

    # ----------------------------------------------------------- views ------
    def describe_view(self, mode: str = "published", ref: Optional[str] = None) -> dict:
        """Geometry + seams + corridor paths for the web map."""
        with self.lock:
            s = self.state
            errors: list[dict] = []
            if mode == "candidate":
                cand = s["candidates"].get(ref)
                if not cand:
                    raise DomainError("NOT_FOUND", f"候选 {ref} 不存在", 404)
                tile_map = self._working_tile_map(s, cand)
                errors, _ = self.validate_tile_map(s, tile_map, set(cand["regions"]))
                overlays = {}
                version_label = f"{ref}（{'校验通过' if not errors else str(len(errors)) + ' 个错误'}）"
            elif mode == "snapshot":
                snap = s["snapshots"].get(ref)
                if not snap:
                    raise DomainError("NOT_FOUND", f"快照 {ref} 不存在", 404)
                tile_map = {tid: s["tiles"][h] for tid, h in snap["tiles"].items()}
                overlays = snap["overlays"]
                version_label = snap["version"]["version"]
            else:
                eff = self.resolve_effective(s)
                tile_map = {tid: s["tiles"][h] for tid, h in eff["tiles"].items()}
                overlays = eff["overlays"]
                version_label = eff["version"]
            return self._render_view(s, tile_map, overlays, errors, mode, ref, version_label)

    def _render_view(self, s, tile_map, overlays, errors, mode, ref, version_label) -> dict:
        tiles_out, nodes_out, lanes_out, seams_out = [], [], [], []
        for tid, t in sorted(tile_map.items()):
            bounds = t.get("bounds")
            if not bounds:
                xs = [n["x"] for n in t["nodes"]]
                ys = [n["y"] for n in t["nodes"]]
                bounds = [min(xs), min(ys), max(xs), max(ys)]
            tiles_out.append({"tile_id": tid, "region": t["region"], "bounds": bounds,
                              "version_tag": t.get("version_tag", "")})
            for n in t["nodes"]:
                ports = {p["node"]: p for p in t["ports"]}
                p = ports.get(n["node_id"])
                nodes_out.append({"id": node_key(tid, n["node_id"]), "tile": tid,
                                  "node": n["node_id"], "x": n["x"], "y": n["y"],
                                  "external": bool(n.get("external") or (p and p.get("external"))),
                                  "port": p["port_id"] if p else None})
            for ln in t["lanes"]:
                nmap = {n["node_id"]: n for n in t["nodes"]}
                ov = overlays.get(tid, {}).get(ln["lane_id"])
                lanes_out.append({"id": f"{tid}/{ln['lane_id']}", "tile": tid,
                                  "lane": ln["lane_id"],
                                  "from": node_key(tid, ln["from"]),
                                  "to": node_key(tid, ln["to"]),
                                  "x1": nmap[ln["from"]]["x"], "y1": nmap[ln["from"]]["y"],
                                  "x2": nmap[ln["to"]]["x"], "y2": nmap[ln["to"]]["y"],
                                  "speed": ln.get("speed"), "two_way": ln.get("two_way", True),
                                  "closed": bool(ov and ov.get("closed")),
                                  "closed_by": ov.get("patch_id") if ov else None})
        seen = set()
        for tid, t in sorted(tile_map.items()):
            for p in t["ports"]:
                peer = p.get("peer")
                if not peer or peer["tile"] not in tile_map:
                    continue
                key = tuple(sorted([f"{tid}:{p['port_id']}", f"{peer['tile']}:{peer['port']}"]))
                if key in seen:
                    continue
                seen.add(key)
                t2 = tile_map[peer["tile"]]
                p2 = next((q for q in t2["ports"] if q["port_id"] == peer["port"]), None)
                ok = bool(p2 and p2.get("peer", {}).get("tile") == tid and
                          p2.get("peer", {}).get("port") == p["port_id"] and
                          abs(p["x"] - p2["x"]) <= EPS and abs(p["y"] - p2["y"]) <= EPS and
                          p.get("profile") == p2.get("profile"))
                seams_out.append({"a": f"{tid}:{p['port_id']}", "b": f"{peer['tile']}:{peer['port']}",
                                  "x": p["x"], "y": p["y"], "ok": ok})

        # corridor paths on this view
        g = build_graph(list(tile_map.values()), overlays)
        corridors = []
        for cor in s["corridors"]:
            polyline, ok, wps = [], True, cor["waypoints"]
            for a, b in zip(wps, wps[1:]):
                seg = dijkstra(g, a, b) if a in g["nodes"] and b in g["nodes"] else None
                if seg is None:
                    ok = False
                    break
                polyline.append({"start": a, "steps": seg})
            corridors.append({"corridor_id": cor["corridor_id"], "name": cor.get("name", ""),
                              "ok": ok, "segments": polyline})

        route = None
        if mode == "snapshot":
            snap = s["snapshots"][ref]
            route = {"steps": snap["route"], "progress": snap["progress"],
                     "status": snap["status"], "from_node": snap["from_node"],
                     "to_node": snap["to_node"],
                     "parent_snapshot_id": snap.get("parent_snapshot_id")}

        return {"mode": mode, "ref": ref, "version_label": version_label,
                "tiles": tiles_out, "nodes": nodes_out, "lanes": lanes_out,
                "seams": seams_out, "errors": errors,
                "corridors": corridors, "route": route}

    def region_status(self) -> dict:
        with self.lock:
            s = self.state
            eff = self.resolve_effective(s)
            out = []
            for r, rid in sorted(s["region_heads"].items()):
                rel = self._release(s, rid)
                ntiles = sum(1 for tid, h in rel["tiles"].items()
                             if s["tiles"][h]["region"] == r)
                out.append({"region": r, "generation": s["region_gen"][r],
                            "release_id": rid, "tiles": ntiles,
                            "published_at": rel["created_at"],
                            "candidate_id": rel["candidate_id"]})
            return {"regions": out, "current_version": eff["version"],
                    "active_patches": [p["patch_id"] for p in self._active_patches(s)]}

    def list_candidates(self) -> list[dict]:
        return sorted((copy.deepcopy(c) for c in self.state["candidates"].values()),
                      key=lambda c: c["created_at"], reverse=True)

    def list_corridors(self) -> list[dict]:
        return copy.deepcopy(self.state["corridors"])

    def list_events(self, limit: int = 60) -> list[dict]:
        return copy.deepcopy(self.state["events"][-limit:][::-1])

    # ------------------------------------------------------------ seed ------
    def seed(self):
        """Initial 2-region world: north(A00,A01) / south(B00,B01), 600-sized."""
        s = self.state

        def tile(tid, region, origin, tag, ports_extra=None, lane_mods=None,
                 speed_override=None):
            ox, oy = origin
            center = (ox + 300, oy + 300)
            P = lambda lx, ly: (ox + lx, oy + ly)
            nodes = [
                {"node_id": "c", "x": center[0], "y": center[1]},
                {"node_id": "w", "x": P(0, 300)[0], "y": P(0, 300)[1]},
                {"node_id": "e", "x": P(600, 300)[0], "y": P(600, 300)[1]},
                {"node_id": "n", "x": P(300, 0)[0], "y": P(300, 0)[1]},
                {"node_id": "s", "x": P(300, 600)[0], "y": P(300, 600)[1]},
            ]
            lanes = [
                {"lane_id": "lcw", "from": "c", "to": "w", "two_way": True, "speed": 60},
                {"lane_id": "lce", "from": "c", "to": "e", "two_way": True, "speed": 60},
                {"lane_id": "lcn", "from": "c", "to": "n", "two_way": True, "speed": 60},
                {"lane_id": "lcs", "from": "c", "to": "s", "two_way": True, "speed": 60},
            ]
            if lane_mods:
                lane_mods(nodes, lanes)
            bounds = [ox, oy, ox + 600, oy + 600]
            return {"tile_id": tid, "region": region, "version_tag": tag,
                    "nodes": nodes, "lanes": lanes, "bounds": bounds,
                    "ports": (ports_extra or [])(P, speed_override)}

        def prof(spd):
            return {"speed": spd, "lanes": 2}

        def ports_a00(P, spd):
            spd = spd or 60
            x, y = P(600, 300)
            xs, ys = P(300, 600)
            return [
                {"port_id": "pw", "node": "w", "side": "west", "x": P(0, 300)[0],
                 "y": 300, "external": True, "peer": None},
                {"port_id": "pn", "node": "n", "side": "north", "x": 300,
                 "y": P(300, 0)[1], "external": True, "peer": None},
                {"port_id": "pe", "node": "e", "side": "east", "x": x, "y": y,
                 "profile": prof(spd), "peer": {"tile": "A01", "port": "pw"}},
                {"port_id": "ps", "node": "s", "side": "south", "x": xs, "y": ys,
                 "profile": prof(spd), "peer": {"tile": "B00", "port": "pn"}},
            ]

        def ports_a01(P, spd):
            return [
                {"port_id": "pw", "node": "w", "side": "west", "x": 600, "y": 300,
                 "profile": prof(60), "peer": {"tile": "A00", "port": "pe"}},
                {"port_id": "pe", "node": "e", "side": "east", "x": 1200, "y": 300,
                 "external": True, "peer": None},
                {"port_id": "pn", "node": "n", "side": "north", "x": 900, "y": 0,
                 "external": True, "peer": None},
                {"port_id": "ps", "node": "s", "side": "south", "x": 900, "y": 600,
                 "profile": prof(60), "peer": {"tile": "B01", "port": "pn"}},
            ]

        def ports_b00(P, spd):
            return [
                {"port_id": "pn", "node": "n", "side": "north", "x": 300, "y": 600,
                 "profile": prof(60), "peer": {"tile": "A00", "port": "ps"}},
                {"port_id": "ps", "node": "s", "side": "south", "x": 300, "y": 1200,
                 "external": True, "peer": None},
                {"port_id": "pw", "node": "w", "side": "west", "x": 0, "y": 900,
                 "external": True, "peer": None},
                {"port_id": "pe", "node": "e", "side": "east", "x": 600, "y": 900,
                 "profile": prof(60), "peer": {"tile": "B01", "port": "pw"}},
            ]

        def ports_b01(P, spd):
            return [
                {"port_id": "pn", "node": "n", "side": "north", "x": 900, "y": 600,
                 "profile": prof(60), "peer": {"tile": "A01", "port": "ps"}},
                {"port_id": "pw", "node": "w", "side": "west", "x": 600, "y": 900,
                 "profile": prof(60), "peer": {"tile": "B00", "port": "pe"}},
                {"port_id": "pe", "node": "e", "side": "east", "x": 1200, "y": 900,
                 "external": True, "peer": None},
                {"port_id": "ps", "node": "s", "side": "south", "x": 900, "y": 1200,
                 "external": True, "peer": None},
            ]

        a00 = tile("A00", "north", (0, 0), "v1", ports_a00)
        a01 = tile("A01", "north", (600, 0), "v1", ports_a01)
        b00 = tile("B00", "south", (0, 600), "v1", ports_b00)
        b01 = tile("B01", "south", (600, 600), "v1", ports_b01)

        hashes = {}
        for t in (a00, a01, b00, b01):
            h = sha256(t)
            s["tiles"][h] = t
            hashes[t["tile_id"]] = h

        # cross-region acceptance corridors
        s["corridors"] = [
            {"corridor_id": "C1", "name": "西入口→东出口（跨南北区）",
             "waypoints": ["A00/w", "B01/e"]},
            {"corridor_id": "C2", "name": "北入口→南出口（跨东西瓦）",
             "waypoints": ["A01/n", "B00/s"]},
        ]

        # generation 1: publish initial world through the normal pipeline
        cand = {"regions": ["north", "south"],
                "tiles": {tid: hashes[tid] for tid in ("A00", "A01", "B00", "B01")}}
        cid = "RC-0001"
        s["seq"]["candidate"] = 1
        s["candidates"][cid] = {
            "candidate_id": cid, "regions": cand["regions"], "tiles": cand["tiles"],
            "note": "初始地图（四区瓦，南北两个发布区域）", "created_by": "system",
            "created_at": self.now(),
            "based_on_generations": {"north": 0, "south": 0},
            "status": "draft", "validation": None}
        tile_map = {tid: s["tiles"][h] for tid, h in cand["tiles"].items()}
        errs, _ = self.validate_tile_map(s, tile_map, set(cand["regions"]))
        assert not errs, errs
        s["candidates"][cid]["validation"] = {
            "status": "passed", "errors": [], "warnings": [],
            "checked_at": self.now(),
            "baseline_generations": {"north": 0, "south": 0}}
        s["candidates"][cid]["status"] = "submitted"
        for region in ("north", "south"):
            s["region_gen"][region] = 1
        rel = {"release_id": "REL-0001", "candidate_id": cid,
               "regions": ["north", "south"],
               "generations": {"north": 1, "south": 1},
               "tiles": dict(cand["tiles"]),
               "parent_releases": {"north": None, "south": None},
               "note": "初始发布", "created_at": self.now()}
        s["releases"].append(rel)
        s["region_heads"] = {"north": "REL-0001", "south": "REL-0001"}
        s["seq"]["release"] = 1
        s["candidates"][cid]["status"] = "published"
        s["candidates"][cid]["published_as"] = "REL-0001"

        # ---- editor variant RC-good: internal-only change in A00 (v2) ----
        def good_mods(nodes, lanes):
            # add a direct diagonal lane n<->e (internal geometry, same boundary)
            lanes.append({"lane_id": "lcne", "from": "n", "to": "e",
                          "two_way": True, "speed": 50})

        a00v2 = tile("A00", "north", (0, 0), "v2", ports_a00, lane_mods=good_mods)
        h = sha256(a00v2)
        s["tiles"][h] = a00v2
        hashes["A00@v2"] = h
        s["seq"]["candidate"] = 2
        s["candidates"]["RC-0002"] = {
            "candidate_id": "RC-0002", "regions": ["north"],
            "tiles": {"A00": h, "A01": hashes["A01"]},
            "note": "北区内部新增 n-e 联络车道（边界拓扑不变，应通过校验）",
            "created_by": "editor", "created_at": self.now(),
            "based_on_generations": {"north": 1}, "status": "draft",
            "validation": None}

        # ---- editor variant RC-bad: south seam speed 80 + deletes c-w lane ----
        def bad_mods(nodes, lanes):
            # cutting the only lane to the west entrance isolates A00/w,
            # so acceptance corridor C1 (starting at A00/w) becomes untraversable
            lanes[:] = [ln for ln in lanes if ln["lane_id"] != "lcw"]

        a00v3 = tile("A00", "north", (0, 0), "v3-bad",
                     lambda P, spd: ports_a00(P, 80), lane_mods=bad_mods)
        h3 = sha256(a00v3)
        s["tiles"][h3] = a00v3
        hashes["A00@v3"] = h3
        s["seq"]["candidate"] = 3
        s["candidates"]["RC-0003"] = {
            "candidate_id": "RC-0003", "regions": ["north"],
            "tiles": {"A00": h3, "A01": hashes["A01"]},
            "note": "北区改稿：南端口限速改 80 且删除西入口车道（接缝限速+跨区走廊必失败）",
            "created_by": "editor", "created_at": self.now(),
            "based_on_generations": {"north": 1}, "status": "draft",
            "validation": None}

        s["vehicles"]["VH-01"] = {"snapshots": [], "last_event_id": 0}
        self._event_locked_run(s, "SYSTEM_SEEDED", "初始地图与示例候选已生成", "REL-0001")
