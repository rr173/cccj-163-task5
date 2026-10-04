#!/usr/bin/env python3
"""
mirror_cli.py — roadside-box map mirror command line (publisher + device agent).

Reads/writes the SAME store as the web console (data/mirror.json): task
progress and rejection reasons shown here are byte-identical to what the web
page shows. The live map service (data/mapstate.json) is only ever read.

Usage:
  ./mirror_cli.py publish <device,csv> [--snapshot S-0001] ["note"]
                            # cut an immutable manifest from the effective
                            # version (or a pinned trip snapshot) + dispatch
  ./mirror_cli.py dispatch <manifest_id> <device,csv>
                            # dispatch an existing manifest to more boxes
  ./mirror_cli.py manifests
  ./mirror_cli.py manifest <manifest_id>
  ./mirror_cli.py tasks [device_id]
  ./mirror_cli.py show <task_id>
  ./mirror_cli.py pull <task_id> [--stop-after N] [--corrupt N]
                            # device agent: pull chunks, verify hashes, ack;
                            # --stop-after simulates a network drop,
                            # --corrupt flips chunk N's bytes in transit
  ./mirror_cli.py switch <task_id>
                            # atomic pointer flip (only after full verification)
  ./mirror_cli.py devices
  ./mirror_cli.py device <device_id>
  ./mirror_cli.py events [limit]
"""

from __future__ import annotations

import base64
import hashlib
import json
import sys

from engine import MapEngine, short
from mirror import MirrorEngine

ENGINE = MapEngine("data/mapstate.json")
MIRROR = MirrorEngine("data/mirror.json", ENGINE)

STATUS_CN = {"pulling": "拉取中", "ready": "可切换", "active": "已切换",
             "failed": "已失败", "rejected": "已拒绝"}


def j(obj):
    print(json.dumps(obj, ensure_ascii=False, indent=2))


def _parse_opts(args, flag):
    """--flag N → (N, remaining_args)."""
    out = list(args)
    if flag in out:
        i = out.index(flag)
        val = out[i + 1]
        del out[i:i + 2]
        return val, out
    return None, out


def cmd_publish(a):
    snap, a = _parse_opts(a, "--snapshot")
    devices = [d.strip() for d in a[0].split(",") if d.strip()]
    body = {"devices": devices, "note": a[1] if len(a) > 1 else ""}
    if snap:
        body["source"] = {"kind": "snapshot", "snapshot_id": snap}
    r = MIRROR.create_manifest(body)
    j(r)
    m = r["manifest"]
    print(f"✓ 清单 {m['manifest_id']}（{m['source']['version']}，"
          f"{len(m['objects'])} 对象 / {len(m['chunks'])} 块，"
          f"根散列 {short(m['root_hash'])}）"
          + ("（重复提交，返回首次清单与任务）" if r["idempotent"] else ""),
          file=sys.stderr)
    for t in r["tasks"]:
        line = f"  {t['task_id']} → {t['device_id']}  [{STATUS_CN.get(t['status'], t['status'])}]"
        if t.get("rejection"):
            line += f"  拒绝: [{t['rejection']['code']}] {t['rejection']['message']}"
        print(line, file=sys.stderr)


def cmd_dispatch(a):
    devices = [d.strip() for d in a[1].split(",") if d.strip()]
    j(MIRROR.dispatch_manifest(a[0], devices))


def cmd_manifests(_):
    ms = MIRROR.list_manifests()
    if not ms:
        print("（暂无清单）")
        return
    for m in ms:
        print(f"{m['manifest_id']}  {m['source']['version']}  "
              f"对象 {m['objects']} · 块 {m['chunks']} · 根散列 {short(m['root_hash'])}")
        print(f"   设备: {','.join(m['devices'])}   来源: {m['source']['kind']}"
              f"{('/' + m['source']['ref']) if m['source'].get('ref') else ''}"
              f"   {m['created_at']}")


def cmd_manifest(a):
    j(MIRROR.get_manifest(a[0]))


def _task_line(t):
    p = t["progress"]
    line = (f"{t['task_id']}  {t['device_id']:<9} [{STATUS_CN.get(t['status'], t['status'])}]"
            f"  {p['confirmed']}/{p['total']} 块  {t['version']}")
    if t.get("rejection"):
        line += f"\n    拒绝理由: [{t['rejection']['code']}] {t['rejection']['message']}"
    return line


def cmd_tasks(a):
    ts = MIRROR.list_tasks(a[0] if a else None)
    if not ts:
        print("（暂无任务）")
        return
    for t in ts:
        print(_task_line(t))


def cmd_show(a):
    t = MIRROR.get_task(a[0])
    j(t)


def cmd_pull(a):
    stop, a = _parse_opts(a, "--stop-after")
    corrupt, a = _parse_opts(a, "--corrupt")
    stop = int(stop) if stop is not None else None
    corrupt = int(corrupt) if corrupt is not None else None
    tid = a[0]
    task = MIRROR.get_task(tid)
    man = task["manifest"]
    print(f"== 设备代理拉取 {tid}（{task['device_id']} ← {task['manifest_id']}）==")
    newly = 0
    while True:
        task = MIRROR.get_task(tid)
        if task["status"] != "pulling":
            break
        todo = [c for c in man["chunks"]
                if task["chunks"].get(str(c["index"]), {}).get("status") != "confirmed"]
        if not todo:
            break
        c = todo[0]
        idx = c["index"]
        payload = MIRROR.get_chunk_payload(tid, idx)
        blob = base64.b64decode(payload["data_b64"])
        if corrupt is not None and idx == corrupt:
            blob = blob[:-1] + bytes([blob[-1] ^ 0xFF])
            print(f"  [{idx}] ⚠ 模拟传输损坏（字节被翻转）")
        computed = "sha256:" + hashlib.sha256(blob).hexdigest()
        r = MIRROR.ack_chunk(tid, idx, computed)
        if r["accepted"]:
            newly += 1
            print(f"  [{idx}] ✓ 块散列校验通过并确认（{short(computed)}）"
                  f"  进度 {r['task']['progress']['confirmed']}/{r['task']['progress']['total']}")
        else:
            print(f"  [{idx}] ✗ [{r['rejection']['code']}] {r['rejection']['message']}")
            break
        if stop is not None and newly >= stop:
            print(f"  ⛔ 网络中断：已确认 {newly} 块，连接断开（可重新 pull 续拉，"
                  f"已确认块不重复计数）")
            break
    task = MIRROR.get_task(tid)
    print(f"任务状态: {STATUS_CN.get(task['status'], task['status'])}"
          f"  进度 {task['progress']['confirmed']}/{task['progress']['total']}")
    if task["status"] == "ready":
        print(f"✓ 整包根散列 {short(task['staged']['root_hash'])} 复核一致，"
              f"镜像已暂存，可执行 switch")


def cmd_switch(a):
    r = MIRROR.switch_task(a[0])
    j(r)
    if r["switched"]:
        img = r["result"]["new_image"]
        print(f"✓ 盒子 {r['device_id']} 已原子切换至 {img['version']}"
              + ("（重复操作，返回首次切换结果）" if r["idempotent"] else ""),
              file=sys.stderr)
    elif r.get("rejection"):
        print(f"✗ 切换未完成：[{r['rejection']['code']}] {r['rejection']['message']}"
              f"（盒子保持原镜像）", file=sys.stderr)


def cmd_devices(_):
    ds = MIRROR.list_devices()
    if not ds:
        print("（暂无盒子）")
        return
    for d in ds:
        cur = d["current_image"]
        print(f"{d['device_id']}  当前镜像: "
              + (f"{cur['version']}（{cur['manifest_id']}，{cur['switched_at']} 切换）"
                 if cur else "（空）"))
        p = d.get("pending_task")
        if p:
            print(f"   待切换: {p['task_id']} [{STATUS_CN.get(p['status'], p['status'])}]"
                  f" {p['progress']['confirmed']}/{p['progress']['total']} 块"
                  f" → {p['version']}")


def cmd_device(a):
    j(MIRROR.get_device(a[0]))


def cmd_events(a):
    j({"events": MIRROR.list_events(int(a[0]) if a else 80)})


CMDS = {
    "publish": (cmd_publish, (1, 5)),
    "dispatch": (cmd_dispatch, 2),
    "manifests": (cmd_manifests, 0),
    "manifest": (cmd_manifest, 1),
    "tasks": (cmd_tasks, (0, 1)),
    "show": (cmd_show, 1),
    "pull": (cmd_pull, (1, 5)),
    "switch": (cmd_switch, 1),
    "devices": (cmd_devices, 0),
    "device": (cmd_device, 1),
    "events": (cmd_events, (0, 1)),
}


def main(argv):
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__)
        return 0
    fn, argc = CMDS[argv[0]]
    args = argv[1:]
    # positional args = everything except --flag / value pairs
    pos, skip = [], False
    for x in args:
        if skip:
            skip = False
            continue
        if x.startswith("--"):
            skip = True
            continue
        pos.append(x)
    if isinstance(argc, tuple):
        ok = argc[0] <= len(pos) <= argc[1]
    else:
        ok = len(pos) == argc
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
