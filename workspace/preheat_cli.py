#!/usr/bin/env python3
"""
preheat_cli.py — capacity-aware preheat orchestration command line.

Reads/writes the SAME store as the web console (data/preheat.json): queue
order, rate-limit reasons and ETAs shown here are identical to the web page.
The mirror repository (data/mirror.json) and the live map service are only
ever read.

Units: rates in KB/s (1 KB = 1000 bytes), disk in MB.

Usage:
  ./preheat_cli.py sites
  ./preheat_cli.py site <site_id> <capacity_kbps>     # register / set shared bandwidth
  ./preheat_cli.py boxes
  ./preheat_cli.py box <box_id> <site_id> <disk_mb> <link_kbps> [used_mb]
  ./preheat_cli.py link <box_id> <kbps>               # degrade / restore the box link
  ./preheat_cli.py offline <box_id> | online <box_id>
  ./preheat_cli.py submit <manifest_id> <box,csv> <deadline_sec> <link_kbps> <min_online> ["note"]
  ./preheat_cli.py jobs
  ./preheat_cli.py show <job_id>
  ./preheat_cli.py queue                              # order / limit reasons / ETA
  ./preheat_cli.py tick <seconds>                     # advance the simulated clock
  ./preheat_cli.py pause <job_id> | resume <job_id> | cancel <job_id>
  ./preheat_cli.py events [limit]
"""

from __future__ import annotations

import json
import sys

from engine import MapEngine
from mirror import MirrorEngine
from preheat import PreheatEngine

ENGINE = MapEngine("data/mapstate.json")
MIRROR = MirrorEngine("data/mirror.json", ENGINE)
PREHEAT = PreheatEngine("data/preheat.json", MIRROR)

STATUS_CN = {"queued": "排队中", "transferring": "传输中", "paused": "已暂停",
             "done": "已完成", "canceled": "已取消"}
JOB_CN = {"active": "进行中", "paused": "已暂停", "completed": "已完成",
          "canceled": "已取消"}


def j(obj):
    print(json.dumps(obj, ensure_ascii=False, indent=2))


def _task_line(t):
    line = (f"  #{t.get('rank', '-'):<3}{t['task_id']}  {t['box_id']:<9} "
            f"[{STATUS_CN.get(t['status'], t['status'])}] "
            f"{t['bytes_done']:.0f}/{t['bytes_total']:.0f} B  "
            f"速率 {t['rate_kbps']:g} KB/s  欠账 {t['debt_kb']:g} KB")
    if t.get("limit"):
        line += f"\n       限速原因: [{t['limit']['code']}] {t['limit']['message']}"
    if t.get("eta"):
        line += f"\n       预计完成: {t['eta']}"
    elif t["status"] in ("queued", "paused"):
        line += "\n       预计完成: 暂不可预计（等待条件恢复）"
    if t.get("at_risk"):
        line += "  ⚠ 超截止风险"
    return line


def cmd_sites(_):
    ss = PREHEAT.list_sites()
    if not ss:
        print("（暂无站点，先用 site 注册）")
        return
    for s in ss:
        print(f"{s['site_id']}  共享带宽 {s['capacity_kbps']:g} KB/s  "
              f"已授予 {s['granted_kbps']:g} KB/s  盒子 {s['online']}/{s['boxes']} 在线")


def cmd_site(a):
    j(PREHEAT.upsert_site(a[0], float(a[1])))


def cmd_boxes(_):
    bs = PREHEAT.list_boxes()
    if not bs:
        print("（暂无盒子，先用 box 注册）")
        return
    for b in bs:
        print(f"{b['box_id']}  @{b['site_id']}  "
              f"{'在线' if b['online'] else '离线'}  链路 {b['link_cap_kbps']:g} KB/s  "
              f"磁盘 已用 {b['disk_used_mb']:g} + 预留 {b['reserved_mb']:g} / "
              f"{b['disk_total_mb']:g} MB  库存对象 {b['inventory_objects']}")


def cmd_box(a):
    used = float(a[4]) if len(a) > 4 else 0.0
    j(PREHEAT.upsert_box(a[0], a[1], float(a[2]), float(a[3]), used))


def cmd_link(a):
    j(PREHEAT.set_box_link(a[0], float(a[1])))


def cmd_offline(a):
    j(PREHEAT.set_box_online(a[0], False))


def cmd_online(a):
    j(PREHEAT.set_box_online(a[0], True))


def cmd_submit(a):
    note = a[5] if len(a) > 5 else ""
    r = PREHEAT.create_job({
        "manifest_id": a[0],
        "targets": [x.strip() for x in a[1].split(",") if x.strip()],
        "deadline_in_sec": float(a[2]),
        "link_cap_kbps": float(a[3]),
        "min_online": int(a[4]),
        "note": note})
    job = r["job"]
    g = job["guarantee"]
    print(f"✓ 作业 {job['job_id']}（{job['version']} → {len(job['targets'])} 台，"
          f"截止 {job['deadline']}，最小在线保障 {g['required']}）"
          + ("（重复提交，返回首次作业）" if r["idempotent"] else ""), file=sys.stderr)
    for t in job["tasks"]:
        print(_task_line(t), file=sys.stderr)
    j({"job": job, "idempotent": r["idempotent"]})


def cmd_jobs(_):
    js = PREHEAT.list_jobs()
    if not js:
        print("（暂无作业）")
        return
    for x in js:
        p, g = x["progress"], x["guarantee"]
        risk = "  ⚠ 保障风险" if g["at_risk"] and x["status"] == "active" else ""
        print(f"{x['job_id']}  [{JOB_CN.get(x['status'], x['status'])}]  "
              f"{x['version']} → {p['done_tasks']}/{p['total_tasks']} 台  "
              f"{p['done_bytes']:.0f}/{p['total_bytes']:.0f} B  "
              f"截止 {x['deadline']}  保障 {g['done']}/{g['required']}{risk}")


def cmd_show(a):
    j(PREHEAT.get_job(a[0]))


def cmd_queue(_):
    q = PREHEAT.queue()
    print(f"模拟时钟: {q['clock']}")
    for s in q["sites"]:
        print(f"站点 {s['site_id']}: 已授予 {s['granted_kbps']:g}/{s['capacity_kbps']:g} KB/s"
              f"（空余 {s['free_kbps']:g}）  排队作业 {s['open_tasks']}")
    if not q["queue"]:
        print("（排队队列为空）")
    print("排队次序（调度优先级 = 欠账↓ · 截止↑ · 提交序↑）:")
    for t in q["queue"]:
        print(_task_line(t))
    resting = [t for t in q["inactive"] if t["status"] == "paused"]
    if resting:
        print("已暂停:")
        for t in resting:
            print(_task_line(t))


def cmd_tick(a):
    r = PREHEAT.tick(float(a[0]))
    print(f"✓ 时钟推进至 {r['clock']}")
    for x in r["advanced"]:
        print(f"  {x['task_id']}  已传 {x['bytes_done']:.0f}/{x['bytes_total']:.0f} B")
    for tid in r["completed"]:
        print(f"  ✓ {tid} 传输完成（磁盘预留与传输名额成对释放）")


def cmd_pause(a):
    j(PREHEAT.pause_job(a[0]))


def cmd_resume(a):
    j(PREHEAT.resume_job(a[0]))


def cmd_cancel(a):
    r = PREHEAT.cancel_job(a[0])
    j(r)
    print(f"✓ 作业 {a[0]} 已取消（成对释放 {r['result']['released_pairs']} 对"
          f"磁盘预留/传输名额）"
          + ("（重复取消，返回首次结果）" if r["idempotent"] else ""), file=sys.stderr)


def cmd_events(a):
    j({"events": PREHEAT.list_events(int(a[0]) if a else 80)})


CMDS = {
    "sites": (cmd_sites, 0),
    "site": (cmd_site, 2),
    "boxes": (cmd_boxes, 0),
    "box": (cmd_box, (4, 5)),
    "link": (cmd_link, 2),
    "offline": (cmd_offline, 1),
    "online": (cmd_online, 1),
    "submit": (cmd_submit, (5, 6)),
    "jobs": (cmd_jobs, 0),
    "show": (cmd_show, 1),
    "queue": (cmd_queue, 0),
    "tick": (cmd_tick, 1),
    "pause": (cmd_pause, 1),
    "resume": (cmd_resume, 1),
    "cancel": (cmd_cancel, 1),
    "events": (cmd_events, (0, 1)),
}


def main(argv):
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__)
        return 0
    fn, argc = CMDS[argv[0]]
    pos = argv[1:]
    ok = argc[0] <= len(pos) <= argc[1] if isinstance(argc, tuple) else len(pos) == argc
    if not ok:
        print(f"参数数量错误（{argv[0]} 需要 {argc} 个参数）\n\n{__doc__}")
        return 2
    try:
        fn(pos)
        return 0
    except Exception as e:
        code = getattr(e, "code", "ERROR")
        status = getattr(e, "status", 500)
        print(f"✗ [{status} {code}] {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
