#!/usr/bin/env python3
"""
audit_cli.py — regulatory-audit temporal-evidence command line (read-only live).

Imports operator evidence chains, reconstructs the spatial state at a given
moment, manages confirmed checkpoints, runs isolated what-if branches and
exports audit bundles. Everything here appends to data/audit.json only; the
live store data/mapstate.json is never modified.

Usage:
  ./audit_cli.py template [import_key] [from] [to] [vehicle]
                            # read-only: build a correctly signed demo chain
  ./audit_cli.py vehicle-events <vehicle> [import_key]
                            # read-only: sign a live vehicle's snapshot chain
  ./audit_cli.py import <events.json|-> [import_key]
                            # import a signed chain (verdict-once per key)
  ./audit_cli.py append <import_key> <events.json|->
                            # continue a long chain from its head checkpoint
  ./audit_cli.py imports
  ./audit_cli.py show <import_key>
  ./audit_cli.py confirm <import_key> <seq> ["note"]
  ./audit_cli.py checkpoints <import_key>
  ./audit_cli.py query <import_key> [seq] [--ts ISO]
                            # rebuild state at a moment (version/tiles/steps)
  ./audit_cli.py branch-switch <import_key> <seq> <release_id> [by]
  ./audit_cli.py branch-revoke <import_key> <seq> <patch_id> [by]
  ./audit_cli.py branches <import_key>
  ./audit_cli.py branch <branch_id>
  ./audit_cli.py export <import_key> [out.json]
  ./audit_cli.py archives [limit]

Evidence event shape:
  {"seq":1,"ts":"...Z","source":"ops/...","type":"route_pinned|progress|...",
   "payload":{...},"parent_digest":null,"digest":"sha256:..."}
"""

from __future__ import annotations

import json
import sys

from engine import MapEngine, short
from audit import AuditEngine

ENGINE = MapEngine("data/mapstate.json")
AUDIT = AuditEngine("data/audit.json", ENGINE)


def j(obj):
    print(json.dumps(obj, ensure_ascii=False, indent=2))


def load_events(path: str):
    if path == "-":
        data = json.load(sys.stdin)
    else:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    if isinstance(data, dict) and "events" in data:
        return data["events"], data.get("import_key")
    return data, None


def cmd_template(a):
    key = a[0] if a else None
    body = {"import_key": key,
            "from_node": a[1] if len(a) > 1 else None,
            "to_node": a[2] if len(a) > 2 else None,
            "vehicle_id": a[3] if len(a) > 3 else None}
    j(AUDIT.build_template_events({k: v for k, v in body.items() if v}))


def cmd_vehicle_events(a):
    body = {"vehicle_id": a[0], "import_key": a[1] if len(a) > 1 else None}
    j(AUDIT.build_vehicle_events({k: v for k, v in body.items() if v}))


def cmd_import(a):
    events, inline_key = load_events(a[0])
    key = a[1] if len(a) > 1 else inline_key
    if not key:
        print("✗ 需要 import_key（参数或 JSON 内含 import_key）", file=sys.stderr)
        sys.exit(2)
    j(AUDIT.import_events({"import_key": key, "events": events}))


def cmd_append(a):
    events, _ = load_events(a[1])
    j(AUDIT.append_events(a[0], events))


def cmd_imports(_):
    j({"imports": AUDIT.list_imports()})


def cmd_show(a):
    j(AUDIT.get_import(a[0]))


def cmd_confirm(a):
    j(AUDIT.confirm_checkpoint(a[0], int(a[1]), a[2] if len(a) > 2 else ""))


def cmd_checkpoints(a):
    j({"checkpoints": AUDIT.list_checkpoints(a[0])})


def cmd_query(a):
    key = a[0]
    seq = int(a[1]) if len(a) > 1 and not a[1].startswith("--") else None
    ts = None
    for i, x in enumerate(a):
        if x == "--ts" and i + 1 < len(a):
            ts = a[i + 1]
    st = AUDIT.query_state(key, seq, ts)
    _print_state(st)


def _print_state(st):
    print(f"== 时序状态重建 {st['import_key']} @事件 {st['at']['event_seq']} ==")
    print(f"车辆:      {st['vehicle_id']}  固定路线: {st['pin_id']} ({st['pin_status']})")
    print(f"时刻:      {st['at']['event_ts']}  链摘要: {short(st['at']['chain_digest'], 12)}")
    v = st["version"]
    print(f"版本描述:  {v['version']}")
    print(f"描述哈希:  {v['descriptor_hash']}  [复核 {'✓一致' if v['descriptor_hash_ok'] else '✗不一致'}]")
    print(f"瓦片摘要:  {st['tile_summary']['verified']}/{st['tile_summary']['count']} "
          f"复核通过  集合摘要 {short(st['tile_summary']['digest'], 12)}")
    for t in st["tiles"]:
        mark = "✓" if t["content_hash_ok"] else ("✗缺失" if not t["present"] else "✗改写")
        print(f"    {mark} {t['tile_id']} = {short(t['hash'], 14)}")
    if v["patches"]:
        print("活动管制:  " + ", ".join(v["patches"]))
    pos = st["position"]
    if pos["state"] == "on_lane":
        pos_text = f"车道 {pos['from']} → {pos['to']}（{pos['lane']}）"
    else:
        pos_text = pos.get("node", "")
    print(f"位置:      {pos['state']} {pos_text}")
    print("行驶步骤:")
    for s_ in st["steps"]:
        icon = {"done": "✓", "current_on_lane": "▶", "current_at_node": "◆",
                "remaining": " "}[s_["phase"]]
        if s_["kind"] == "seam":
            print(f"  [{s_['index']:2d}] {icon} 接缝 {s_['from']} → {s_['to']}")
        else:
            closed = st["overlays"].get(s_["tile"], {}).get(s_["lane"], {}).get("closed")
            print(f"  [{s_['index']:2d}] {icon} 车道 {s_['tile']}/{s_['lane']} "
                  f"{s_['from'].split('/')[1]}→{s_['to'].split('/')[1]} @{s_['speed']}"
                  f"{' [管制封闭]' if closed else ''}")
    for w in st["warnings"]:
        print(f"⚠ [{w['level']}] {w['message']}")
    print(f"查询档案:  {st['archive_id']}（只读查询，仅新增审计档案）")


def cmd_branch_switch(a):
    j(AUDIT.create_branch(a[0], {
        "seq": int(a[1]), "created_by": a[3] if len(a) > 3 else "auditor",
        "scenario": {"type": "version_switch", "release_id": a[2]}}))


def cmd_branch_revoke(a):
    j(AUDIT.create_branch(a[0], {
        "seq": int(a[1]), "created_by": a[3] if len(a) > 3 else "auditor",
        "scenario": {"type": "revoke_closure", "patch_id": a[2]}}))


def cmd_branches(a):
    j({"branches": AUDIT.list_branches(a[0])})


def cmd_branch(a):
    j(AUDIT.get_branch(a[0], True))


def cmd_export(a):
    bundle = AUDIT.create_export(a[0])
    if len(a) > 1:
        with open(a[1], "w", encoding="utf-8") as f:
            json.dump(bundle, f, ensure_ascii=False, indent=2)
        print(f"✓ 审计档案 {bundle['export_id']} 已写出 {a[1]} "
              f"（bundle {short(bundle['bundle_digest'], 16)}）")
    else:
        j(bundle)


def cmd_archives(a):
    j({"archives": AUDIT.list_archives(int(a[0]) if a else 100)})


CMDS = {
    "template": (cmd_template, (0, 4)),
    "vehicle-events": (cmd_vehicle_events, (1, 2)),
    "import": (cmd_import, (1, 2)),
    "append": (cmd_append, 2),
    "imports": (cmd_imports, 0),
    "show": (cmd_show, 1),
    "confirm": (cmd_confirm, (2, 3)),
    "checkpoints": (cmd_checkpoints, 1),
    "query": (cmd_query, (1, 4)),
    "branch-switch": (cmd_branch_switch, (3, 4)),
    "branch-revoke": (cmd_branch_revoke, (3, 4)),
    "branches": (cmd_branches, 1),
    "branch": (cmd_branch, 1),
    "export": (cmd_export, (1, 2)),
    "archives": (cmd_archives, (0, 1)),
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
