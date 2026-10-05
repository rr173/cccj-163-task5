#!/usr/bin/env python3
"""
preheat_cli.py — capacity-aware pre-warm orchestration command line.

Reads/writes the SAME store as the web console (data/preheat.json): the queue
order, rate-limiting reasons and ETA shown here are byte-identical to the web
page. The engine never touches map / mirror / audit state.

Usage:
  ./preheat_cli.py dashboard
  ./preheat_cli.py sites · boxes [box_id] · blobs
  ./preheat_cli.py submit <box,csv> <blob,csv> [deadline_s] [rate_cap_Bps]
                          [min_online_Bps] [weight] [name] [--key K] [--resubmit JH]
  ./preheat_cli.py jobs [--all]
  ./preheat_cli.py show <job_id>
  ./preheat_cli.py queue                     # global queue order + ETA + reason
  ./preheat_cli.py quota <job_id> [--deadline N] [--rate-cap Bps]
                               [--min-online Bps] [--weight W]
  ./preheat_cli.py pause <job_id> · resume <job_id>
  ./preheat_cli.py cancel <job_id> ["reason"]
  ./preheat_cli.py offline <box_id> · online <box_id> [effective_Bps]
  ./preheat_cli.py degrade <box_id> <Bps>
  ./preheat_cli.py tick [N]                  # advance virtual clock N seconds
  ./preheat_cli.py events [limit]
  ./preheat_cli.py invariants                # self-audit (ghost/over-book)
  ./preheat_cli.py blob <blob_id> <size_bytes>
"""

from __future__ import annotations

import json
import sys

from engine import DomainError
from preheat import PreheatEngine, REASON_CN

PREHEAT = PreheatEngine("data/preheat.json")

STATUS_CN = {"queued": "排队中", "running": "传输中", "paused": "已暂停",
             "blocked": "等空间", "offline": "盒离线", "complete": "已完成",
             "failed": "已失败", "canceled": "已取消"}


def j(obj):
    print(json.dumps(obj, ensure_ascii=False, indent=2))


def mb(n):
    return f"{n / 1048576:.1f}MiB"


def mbps(n):
    return f"{n / 1048576:.1f}MiB/s"


def _csv(s):
    return [x.strip() for x in s.split(",") if x.strip()]


def _opt(args, flag, cast=str):
    if flag in args:
        i = args.index(flag)
        v = cast(args[i + 1])
        del args[i:i + 2]
        return v
    return None


def cmd_dashboard(a):
    d = PREHEAT.dashboard()
    print(f"虚拟时钟 t={d['clock']}s（1 tick = {d['tick_seconds']}s）")
    print("── 站点共享带宽 ──")
    for s in d["sites"]:
        print(f"  {s['site_id']} {s['name']}：{mbps(s['granted_bps'])}/"
              f"{mbps(s['uplink_bps'])} 已分配，缓存命中 {mb(s['cache_hit_bytes'])}")
    print("── 盒子 ──")
    for b in d["boxes"]:
        flag = "在线" if b["online"] else "离线"
        link = b["effective_link_bps"]
        degr = "（降级）" if link < b["link_bps"] else ""
        print(f"  {b['box_id']}@{b['site_id']} [{flag}] 磁盘 "
              f"{mb(b['used_bytes'])} 用 + {mb(b['reserved_bytes'])} 预留 / "
              f"{mb(b['capacity_bytes'])}（空闲 {mb(b['free_bytes'])}）；"
              f"链路 {mbps(link)}/{mbps(b['link_bps'])}{degr}")
    print("── 全局排队次序 ──")
    _queue_lines(d["jobs"])


def _queue_lines(jobs):
    live = [x for x in jobs if x["status"] not in ("complete", "canceled", "failed")]
    live.sort(key=lambda x: (x["queue_rank"] or 9999, x["job_id"]))
    if not live:
        print("  （无在队作业）")
    for x in live:
        dl = ""
        if x["deadline_ticks"]:
            dl = f" 截止提交+{x['deadline_ticks']}s"
            if x["missed_deadline"]:
                dl += "（已超时）"
        eta = f"ETA t={x['eta_clock']}（剩 {x['eta_ticks']}s）" \
            if x["eta_ticks"] is not None else "ETA 待定（当前无速率）"
        print(f"  #{x['queue_rank'] or '-'} {x['job_id']}「{x['name']}」"
              f"[{STATUS_CN.get(x['status'], x['status'])}] "
              f"{x['percent']}% {mb(x['done_bytes'])}/{mb(x['total_bytes'])}"
              f"（WAN {mb(x['wan_bytes'])} / 复用 {mb(x['cache_bytes'])}）{dl}  {eta}")
        for r in x["replicas"]:
            if r["status"] in ("complete", "canceled"):
                continue
            print(f"      └ {r['box_id']} {r['percent']}% "
                  f"分配 {mbps(r['allocated_bps'])}（缓存 {mbps(r.get('cache_bps', 0))}）"
                  f" 欠账 {mb(r['deficit'])}  限速原因：{r['reason_cn']}")


def cmd_sites(a):
    for s in PREHEAT.list_sites():
        print(f"{s['site_id']} {s['name']}：共享上行 {mbps(s['uplink_bps'])}，"
              f"上一 tick 用 {mbps(s['used_bps_last_tick'])}，"
              f"缓存 {len(s['cache'])} 对象")


def cmd_boxes(a):
    if a:
        j(PREHEAT.get_box(a[0]))
        return
    for b in PREHEAT.list_boxes():
        print(f"{b['box_id']}@{b['site_id']} [{'在线' if b['online'] else '离线'}] "
              f"用 {mb(b['used_bytes'])} 预留 {mb(b['reserved_bytes'])} / "
              f"{mb(b['capacity_bytes'])} 空 {mb(b['free_bytes'])}；"
              f"链路 {mbps(b['effective_link_bps'])}/{mbps(b['link_bps'])}；"
              f"已存 {len(b['stored'])} 部分 {len(b['partial_bytes'])}")


def cmd_blobs(a):
    for b in PREHEAT.list_blobs():
        print(f"{b['blob_id']}  {mb(b['size'])}  {b['hash'][:18]}")


def cmd_blob(a):
    r = PREHEAT.register_blob({"blob_id": a[0], "size": int(a[1])})
    j(r)


def cmd_submit(a):
    args = list(a)
    key = _opt(args, "--key")
    resub = _opt(args, "--resubmit")
    boxes, blobs = _csv(args[0]), _csv(args[1])
    body = {"targets": boxes, "blobs": blobs}
    if len(args) > 2 and args[2] != "-":
        body["deadline_ticks"] = int(args[2])
    if len(args) > 3 and args[3] != "-":
        body["rate_cap_bps"] = int(args[3])
    if len(args) > 4 and args[4] != "-":
        body["min_online_bps"] = int(args[4])
    if len(args) > 5 and args[5] != "-":
        body["weight"] = int(args[5])
    if len(args) > 6:
        body["name"] = args[6]
    if key:
        body["idempotency_key"] = key
    if resub:
        body["resubmit_of"] = resub
    r = PREHEAT.submit_job(body)
    job = r["job"]
    print(f"✓ 作业 {job['job_id']}「{job['name']}」{job['status']} "
          f"{len(job['targets'])} 盒 × {len(job['blobs'])} 对象，"
          f"WAN {mb(job['wan_bytes'])} / 缓存复用 {mb(job['cache_bytes'])}"
          + ("（重复提交，返回首次结果）" if r["idempotent"] else ""),
          file=sys.stderr)
    for rep in job["replicas"]:
        print(f"   └ {rep['box_id']} [{STATUS_CN.get(rep['status'], rep['status'])}] "
              f"需 {mb(rep['wan_remaining'] + rep['cache_remaining'])} "
              f"预留 {mb(rep['reserve_bytes'])} {rep['reason_cn']}", file=sys.stderr)
    j({"job_id": job["job_id"], "idempotent": r["idempotent"]})


def cmd_jobs(a):
    jobs = PREHEAT.list_jobs()
    if "--all" not in a:
        jobs = [x for x in jobs if x["status"] not in ("complete", "canceled")]
    if not jobs:
        print("（无作业）")
        return
    for x in jobs:
        print(f"{x['job_id']} [{STATUS_CN.get(x['status'], x['status'])}] "
              f"{x['percent']}% {x['name']}  排队#{x['queue_rank'] or '-'}  "
              + (f"ETA t={x['eta_clock']}" if x["eta_ticks"] is not None else "ETA 待定"))


def cmd_show(a):
    j(PREHEAT.get_job(a[0]))


def cmd_queue(a):
    d = PREHEAT.dashboard()
    _queue_lines(d["jobs"])


def cmd_quota(a):
    body = {}
    args = list(a[1:])
    v = _opt(args, "--deadline", int)
    if v is not None:
        body["deadline_ticks"] = v
    v = _opt(args, "--rate-cap", int)
    if v is not None:
        body["rate_cap_bps"] = v
    v = _opt(args, "--min-online", int)
    if v is not None:
        body["min_online_bps"] = v
    v = _opt(args, "--weight", int)
    if v is not None:
        body["weight"] = v
    if not body:
        print("至少提供一个配额字段", file=sys.stderr)
        return 2
    r = PREHEAT.update_quota(a[0], body)
    j(r)


def cmd_pause(a):
    r = PREHEAT.pause_job(a[0])
    print(("（已是暂停态，首次结果不变）" if r.get("idempotent") else
           f"✓ {a[0]} 已暂停：预留与名额立即成对释放"), file=sys.stderr)
    j({"job_id": a[0], "status": r["job"]["status"], "idempotent": r.get("idempotent")})


def cmd_resume(a):
    r = PREHEAT.resume_job(a[0])
    print(("（已是在传态，首次结果不变）" if r.get("idempotent") else
           f"✓ {a[0]} 已恢复：重建预留，只补剩余数据"), file=sys.stderr)
    j({"job_id": a[0], "status": r["job"]["status"], "idempotent": r.get("idempotent")})


def cmd_cancel(a):
    r = PREHEAT.cancel_job(a[0], a[1] if len(a) > 1 else "")
    if r.get("idempotent"):
        print(f"（{a[0]} 已终结：{r.get('verdict')}，首次判定不变）", file=sys.stderr)
    else:
        print(f"✓ {a[0]} 已取消，释放预留 {mb(r['freed_reserve_bytes'])}，"
              f"已传内容（含在途偏移）保留供再次提交续传", file=sys.stderr)
    j({"job_id": a[0], "idempotent": r["idempotent"],
       "verdict": r["job"]["status"]})


def cmd_offline(a):
    j(PREHEAT.set_box_online(a[0], False))


def cmd_online(a):
    bps = int(a[1]) if len(a) > 1 else None
    j(PREHEAT.set_box_online(a[0], True, bps))


def cmd_degrade(a):
    j(PREHEAT.degrade_box_link(a[0], int(a[1])))


def cmd_tick(a):
    n = int(a[0]) if a else 1
    r = PREHEAT.tick(n)
    print(f"时钟推进至 t={r['clock']}s", file=sys.stderr)
    j({"clock": r["clock"],
       "sites": [{"site_id": t["site_id"],
                  "granted_mibps": round(t["granted_bps"] / 1048576, 1),
                  "capacity_mibps": round(t["capacity_bps"] / 1048576, 1)}
                 for t in r["ticks"][-1]["sites"]]})


def cmd_events(a):
    j({"events": PREHEAT.list_events(int(a[0]) if a else 60)})


def cmd_invariants(a):
    j(PREHEAT.assert_invariants())


CMDS = {
    "dashboard": (cmd_dashboard, 0), "sites": (cmd_sites, 0),
    "boxes": (cmd_boxes, (0, 1)), "blobs": (cmd_blobs, 0),
    "blob": (cmd_blob, 2), "submit": (cmd_submit, (2, 7)),
    "jobs": (cmd_jobs, (0, 1)), "show": (cmd_show, 1), "queue": (cmd_queue, 0),
    "quota": (cmd_quota, 1), "pause": (cmd_pause, 1), "resume": (cmd_resume, 1),
    "cancel": (cmd_cancel, (1, 2)), "offline": (cmd_offline, 1),
    "online": (cmd_online, (1, 2)), "degrade": (cmd_degrade, 2),
    "tick": (cmd_tick, (0, 1)), "events": (cmd_events, (0, 1)),
    "invariants": (cmd_invariants, 0),
}


def main(argv):
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__)
        return 0
    fn, argc = CMDS[argv[0]]
    args = argv[1:]
    pos, skip = [], False
    for x in args:
        if skip:
            skip = False
            continue
        if x.startswith("--"):
            skip = True
            continue
        pos.append(x)
    ok = (argc[0] <= len(pos) <= argc[1]) if isinstance(argc, tuple) \
        else len(pos) == argc
    if not ok:
        print(f"参数数量错误（{argv[0]} 需要 {argc} 个位置参数）\n\n{__doc__}")
        return 2
    try:
        fn(args)
        return 0
    except DomainError as e:
        print(f"✗ [{e.status} {e.code}] {e}", file=sys.stderr)
        if e.details:
            print(json.dumps(e.details, ensure_ascii=False, indent=2),
                  file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
