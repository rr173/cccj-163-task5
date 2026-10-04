#!/usr/bin/env python3
"""
route_replay.py — deterministic route reproduction / offline catch-up tool.

Replays a pinned snapshot: same version descriptor, same tile hashes, same
lane-level path. Also drives the full lifecycle from the command line.

Usage:
  ./route_replay.py status
  ./route_replay.py plan <vehicle> <from tile/node> <to tile/node>
  ./route_replay.py replay <snapshot_id>
  ./route_replay.py progress <snapshot_id> <step> at_node|on_lane
  ./route_replay.py advisory <snapshot_id>
  ./route_replay.py adopt-reroute <snapshot_id>
  ./route_replay.py complete <snapshot_id>
  ./route_replay.py sync <vehicle> [last_event_id]
  ./route_replay.py patch <tile_id> <lane_id> "<reason>" [priority]
  ./route_replay.py activate|revoke <patch_id>
  ./route_replay.py validate|publish <candidate_id>
  ./route_replay.py rollouts
  ./route_replay.py rollout-new <candidate> <regions csv> <vehicles csv> [window_minutes] [max_unreachable] [max_transition_rejected]
  ./route_replay.py rollout-show <rollout_id>
  ./route_replay.py rollout-promote <rollout_id>
  ./route_replay.py rollout-rollback <rollout_id> [reason]
  ./route_replay.py events [limit]

Runs directly against the local store (no HTTP needed). Add --http URL to
talk to a running server instead.
"""

from __future__ import annotations

import json
import sys

from engine import MapEngine, short

ENGINE = MapEngine("data/mapstate.json")


def j(obj):
    print(json.dumps(obj, ensure_ascii=False, indent=2))


def cmd_status(_):
    j(ENGINE.region_status())


def cmd_plan(a):
    j(ENGINE.create_route({"vehicle_id": a[0], "from_node": a[1], "to_node": a[2]}))


def cmd_replay(a):
    snap = ENGINE.get_snapshot(a[0])
    sid = snap["snapshot_id"]
    print(f"== 路线复现 {sid} ==")
    print(f"车辆:      {snap['vehicle_id']}")
    print(f"版本:      {snap['version']['version']}")
    print(f"版本哈希:  {snap['version']['descriptor_hash']}")
    print(f"固定瓦片:  " + ", ".join(
        f"{t}={short(h, 12)}" for t, h in sorted(snap["tiles"].items())))
    if snap["version"]["patches"]:
        print("活动补丁:  " + ", ".join(p["patch_id"] for p in snap["version"]["patches"]))
    print(f"状态:      {snap['status']}  进度: step={snap['progress']['step']} "
          f"{snap['progress']['state']}")
    print("路线（逐边）:")
    for i, st in enumerate(snap["route"]):
        if st["kind"] == "seam":
            print(f"  [{i:2d}] 接缝 {st['from_tile']}:{st['from_node']} → "
                  f"{st['to_tile']}:{st['to_node']}  ({st.get('via_port')})")
        else:
            closed = snap["overlays"].get(st["tile"], {}).get(st["lane"], {}).get("closed")
            print(f"  [{i:2d}] 车道 {st['tile']}/{st['lane']}  "
                  f"{st['from_node']}→{st['to_node']} @{st['speed']}"
                  f"{' [补丁封闭]' if closed else ''}")
    # determinism assertion: re-resolving does not change the pinned descriptor
    cur = ENGINE.resolve_effective()
    if cur["descriptor_hash"] != snap["version"]["descriptor_hash"]:
        print(f"\n⚠ 当前线上版本 {cur['version']} 与快照不同——这是允许的："
              f"复现仍严格使用快照固定的瓦片集合")
    else:
        print("\n✓ 当前线上版本与快照一致")
    if snap["advisory"]["closures_ahead"] or snap["advisory"]["notes"]:
        print("\n预警:")
        for n in snap["advisory"]["notes"] + snap["advisory"]["closures_ahead"]:
            print(f"  · [{n['level']}] {n['message']}")


def cmd_progress(a):
    j(ENGINE.update_progress(a[0], {"step": int(a[1]), "state": a[2]}))


def cmd_advisory(a):
    j(ENGINE.get_snapshot(a[0])["advisory"])


def cmd_adopt(a):
    j(ENGINE.adopt_reroute(a[0]))


def cmd_complete(a):
    j(ENGINE.complete_snapshot(a[0]))


def cmd_sync(a):
    last = int(a[1]) if len(a) > 1 else 0
    j(ENGINE.vehicle_sync(a[0], last))


def cmd_patch(a):
    j(ENGINE.create_patch({
        "scope": {"tiles": {a[0]: [a[1]]}}, "reason": a[2],
        "priority": int(a[3]) if len(a) > 3 else 100}))


def make_patch_state_cmd(action: str):
    def cmd(a):
        pid = a[0]
        if action == "activate":
            j(ENGINE.activate_patch(pid))
        else:
            j(ENGINE.revoke_patch(pid, a[1] if len(a) > 1 else ""))
    return cmd


def make_candidate_cmd(action: str):
    def cmd(a):
        cid = a[0]
        if action == "validate":
            j(ENGINE.validate_candidate(cid))
        else:
            j(ENGINE.publish_candidate(cid))
    return cmd


def cmd_events(a):
    j({"events": ENGINE.list_events(int(a[0]) if a else 60)})


def _fmt_rollout(b: dict) -> dict:
    """Compact, explainable per-batch view for the terminal."""
    out = dict(b)
    out["groups"] = {
        "trial（名单内·待晋级内容）": b["stats"]["trial"],
        "control（名单外·当前内容）": b["stats"]["control"],
    }
    return out


def cmd_rollouts(_):
    bs = ENGINE.list_rollouts()
    if not bs:
        print("（暂无试运行批次）")
        return
    for b in bs:
        w = b.get("window") or {}
        win = f"{w.get('start','')} ~ {w.get('end') or '无结束'}" + \
              ("" if b["window_open"] else "（观察窗口已结束）")
        print(f"{b['rollout_id']}  [{b['status']}]  候选 {b['candidate_id']}  "
              f"区域 {','.join(b['regions'])}")
        print(f"   名单: {','.join(b['vehicles'])}   窗口: {win}")
        if b.get("trial_version_preview"):
            print(f"   待晋级版本: {b['trial_version_preview']}")
        for g, label in (("trial", "试运行组"), ("control", "对照组  ")):
            st = b["stats"][g]
            print(f"   {label}: 请求 {st['requests']}  不可达 {st['unreachable']}  "
                  f"安全过渡拒绝 {st['transition_rejected']}")
        if b["breaches"]:
            print("   ⚠ 阈值越界: " + "；".join(b["breaches"]))
        if b.get("decision"):
            d = b["decision"]
            extra = f" → {d['release_id']}" if d.get("release_id") else ""
            print(f"   判定: {d['action']}{extra} @ {d['at']}")


def cmd_rollout_new(a):
    from datetime import datetime, timedelta, timezone
    body = {"candidate_id": a[0],
            "regions": [r.strip() for r in a[1].split(",") if r.strip()],
            "vehicles": [v.strip() for v in a[2].split(",") if v.strip()]}
    if len(a) > 3:
        end = (datetime.now(timezone.utc).replace(microsecond=0)
               + timedelta(minutes=int(a[3])))
        body["window"] = {"end": end.isoformat()}
    th = {}
    if len(a) > 4:
        th["max_unreachable"] = int(a[4])
    if len(a) > 5:
        th["max_transition_rejected"] = int(a[5])
    if th:
        body["thresholds"] = th
    j(ENGINE.create_rollout(body))


def cmd_rollout_show(a):
    j(_fmt_rollout(ENGINE.get_rollout(a[0])))


def cmd_rollout_promote(a):
    r = ENGINE.promote_rollout(a[0])
    j(r)
    d = r["decision"]
    print(f"✓ 批次 {r['rollout_id']} 已晋级为 {d.get('release_id')}："
          + "，".join(f"{k} 第 {v} 代" for k, v in (d.get("generations") or {}).items())
          + ("（重复操作，返回首次结果）" if r.get("idempotent") else ""),
          file=sys.stderr)


def cmd_rollout_rollback(a):
    r = ENGINE.rollback_rollout(a[0], a[1] if len(a) > 1 else "")
    j(r)
    print(f"✓ 批次 {r['rollout_id']} 已撤回；已发出快照与区域代际保持不变"
          + ("（重复操作，返回首次结果）" if r.get("idempotent") else ""),
          file=sys.stderr)


CMDS = {
    "status": (cmd_status, 0),
    "plan": (cmd_plan, 3),
    "replay": (cmd_replay, 1),
    "progress": (cmd_progress, 3),
    "advisory": (cmd_advisory, 1),
    "adopt-reroute": (cmd_adopt, 1),
    "complete": (cmd_complete, 1),
    "sync": (cmd_sync, (1, 2)),
    "patch": (cmd_patch, (3, 4)),
    "activate": (make_patch_state_cmd("activate"), 1),
    "revoke": (make_patch_state_cmd("revoke"), (1, 2)),
    "validate": (make_candidate_cmd("validate"), 1),
    "publish": (make_candidate_cmd("publish"), 1),
    "rollouts": (cmd_rollouts, 0),
    "rollout-new": (cmd_rollout_new, (3, 6)),
    "rollout-show": (cmd_rollout_show, 1),
    "rollout-promote": (cmd_rollout_promote, 1),
    "rollout-rollback": (cmd_rollout_rollback, (1, 2)),
    "events": (cmd_events, (0, 1)),
}


def main(argv):
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__)
        return 0
    fn, argc = CMDS[argv[0]]
    args = argv[1:]
    if isinstance(argc, tuple):
        ok = argc[0] <= len(args) <= argc[1]
    else:
        ok = len(args) == argc
    if not ok:
        print(f"参数数量错误（{argv[0]} 需要 {argc} 个参数）\n\n{__doc__}")
        return 2
    try:
        fn(args)
        return 0
    except Exception as e:
        code = getattr(e, "code", "ERROR")
        status = getattr(e, "status", 500)
        print(f"✗ [{status} {code}] {e}", file=sys.stderr)
        details = getattr(e, "details", None)
        if details:
            print(json.dumps(details, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
