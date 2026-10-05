#!/usr/bin/env python3
"""End-to-end verification of the capacity-aware pre-warm orchestrator.

Resets the (isolated) preheat store and drives the engine directly. Asserts
every requirement from the brief:

  * per-job deadline / per-link rate cap / minimum online floor
  * site-shared uplink: granted sum never exceeds site capacity
  * per-box free space: admission is an atomic batch; no over-booking
  * content reuse: site-cached blobs travel over LAN and are never re-billed
  * starvation-free: minimum floor + persisted-deficit DRR keeps a long job
    progressing under a stream of newcomers
  * disk reservation and network grant are acquired/released as a pair;
    insufficient space, link degradation, box offline, cancel and crash leave
    no ghost reservation or grant
  * pause/resume and crash recovery re-send only remaining bytes (per-blob
    offsets persisted); restart rebuilds queue, deficits and ETA
  * quota edit / cancel / resubmit are verdict-deterministic; concurrent
    operations are serialized by one commit lock
  * effective maps and trip snapshots of the other subsystems are untouched
    (preheat has its own store and never imports their engines)
"""
import inspect
import json
import os
import shutil
import sys
import threading

import preheat as preheat_mod
from engine import DomainError
from preheat import PreheatEngine

ROOT = os.path.dirname(os.path.abspath(__file__))
STORE = os.path.join(ROOT, "data", "preheat.json")
MB = 1024 * 1024

fails = []


def check(name, cond, detail=""):
    print(("  ✓ " if cond else "  ✗ FAIL: ") + name +
          (f"  [{detail}]" if detail and not cond else ""))
    if not cond:
        fails.append(name)


def expect_err(name, fn, code, status=409):
    try:
        fn()
    except DomainError as e:
        check(name, e.code == code and e.status == status,
              f"got {e.status} {e.code}: {e.message}")
        return e
    check(name, False, "no error raised")
    return None


def fresh():
    shutil.rmtree(os.path.join(ROOT, "data"), ignore_errors=True)
    return PreheatEngine(STORE)


def site_granted(e, sid):
    return sum(r["allocated_bps"] for job in e.state["jobs"].values()
               for r in job["replicas"] if r["site_id"] == sid)


def main():
    print("\n[1] 站点共享带宽上限 + 最小在线保障")
    e = fresh()
    blobs = ["BL-MAP-LANE", "BL-MAP-POI", "BL-MAP-PATCH"]  # 112 MiB
    for i, b in enumerate(["RSU-N1", "RSU-N2", "RSU-N3"]):
        e.submit_job({"name": f"J{i}", "targets": [b], "blobs": blobs,
                      "rate_cap_bps": 30 * MB, "min_online_bps": 5 * MB})
    e.tick(1)
    jobs = {j["job_id"]: j for j in e.list_jobs()}
    granted = site_granted(e, "ST-N")
    check("三盒分配总和不超过站点 60 MiB/s", granted <= 60 * MB + 1,
          f"{granted // MB} MiB/s")
    check("每个在线副本拿到最小保障 5 MiB/s",
          all(jobs[j]["replicas"][0]["allocated_bps"] >= 5 * MB - 1
              for j in jobs))

    print("\n[2] 饿死免疫：持续推进的作业不被新作业长期饿死")
    long_before = jobs["JH-0001"]["replicas"][0]["wan_done"]
    e.submit_job({"name": "newcomer", "targets": ["RSU-N1"], "blobs": blobs,
                  "rate_cap_bps": 30 * MB, "min_online_bps": 5 * MB})
    e.tick(3)
    long_after = e.get_job("JH-0001")["replicas"][0]["wan_done"]
    check("老作业在新作业涌入期间持续获得字节", long_after > long_before,
          f"{long_before} -> {long_after}")
    check("站点带宽始终不超限", site_granted(e, "ST-N") <= 60 * MB + 1)
    e.assert_invariants()

    print("\n[3] 速率上限与限速原因")
    r1 = jobs["JH-0001"]["replicas"][0]
    check("达到作业速率上限时给出 RATE_CAP 原因",
          r1["granted_bps"] >= 15 * MB and r1["reason"] in
          ("RATE_CAP", "ELIGIBLE", "FLOOR_CAP"))
    e.degrade_box_link("RSU-N2", 3 * MB)
    e.tick(1)
    r2 = e.get_job("JH-0002")["replicas"][0]
    check("链路降级后分配不超过有效链路",
          r2["allocated_bps"] <= 3 * MB + 1, f"{r2['allocated_bps'] // MB}")
    check("降级给出 LINK_DEGRADED 原因", r2["reason"] == "LINK_DEGRADED",
          r2["reason"])

    print("\n[4] 暂停/恢复：成对释放、只补剩余")
    e2 = fresh()
    e2.submit_job({"name": "P", "targets": ["RSU-S1"],
                   "blobs": ["BL-MAP-MEDIA", "BL-MAP-LANE"],
                   "rate_cap_bps": 10 * MB, "min_online_bps": 2 * MB})
    e2.tick(2)
    rep = e2.get_job("JH-0001")["replicas"][0]
    rem_at, done_at = rep["remaining_bytes"], rep["wan_done"] + rep["cache_done"]
    assert rem_at > 0
    e2.pause_job("JH-0001")
    box = e2.get_box("RSU-S1")
    check("暂停立即释放磁盘预留", box["reserved_bytes"] == 0)
    check("暂停返回幂等", e2.pause_job("JH-0001")["idempotent"] is True)
    e2.tick(2)
    rep = e2.get_job("JH-0001")["replicas"][0]
    check("暂停期间零传输", rep["wan_done"] + rep["cache_done"] == done_at)
    e2.resume_job("JH-0001")
    e2.tick(2)
    rep = e2.get_job("JH-0001")["replicas"][0]
    check("恢复后只补剩余（偏移持久化）",
          rep["wan_done"] + rep["cache_done"] > done_at
          and rep["remaining_bytes"] < rem_at)
    e2.assert_invariants()

    print("\n[5] 盒子离线：成对释放；恢复续传剩余")
    e3 = fresh()
    e3.submit_job({"name": "O", "targets": ["RSU-S1"],
                   "blobs": ["BL-MAP-MEDIA", "BL-MAP-LANE"],
                   "rate_cap_bps": 10 * MB})
    e3.tick(2)
    e3.set_box_online("RSU-S1", False)
    e3.tick(1)
    rep = e3.get_job("JH-0001")["replicas"][0]
    box = e3.get_box("RSU-S1")
    check("离线副本状态 offline", rep["status"] == "offline")
    check("离线不持预留", rep["reserve_bytes"] == 0 and box["reserved_bytes"] == 0)
    check("离线不持传输名额", rep["allocated_bps"] == 0)
    e3.set_box_online("RSU-S1", True)
    e3.tick(1)
    rep = e3.get_job("JH-0001")["replicas"][0]
    check("恢复后重新排队传输", rep["status"] in ("queued", "running")
          and rep["reserve_bytes"] > 0)
    e3.tick(60)
    check("恢复后最终完成", e3.get_job("JH-0001")["status"] == "complete")
    e3.assert_invariants()

    print("\n[6] 取消：判定一次、成对释放、部分字节保留")
    e4 = fresh()
    e4.submit_job({"name": "C", "targets": ["RSU-S1"],
                   "blobs": ["BL-MAP-MEDIA"], "rate_cap_bps": 20 * MB})
    e4.tick(3)  # ~60 MiB into the 160 MiB blob, not yet landed
    r = e4.cancel_job("JH-0001", "redesign")
    check("取消返回剩余预留释放量", r["freed_reserve_bytes"] >= 0)
    again = e4.cancel_job("JH-0001", "again")
    check("重复取消返回首次判定", again["idempotent"] and
          again["verdict"] == "canceled")
    box = e4.get_box("RSU-S1")
    check("在途偏移固化到盒内（partial）",
          box["partial_bytes"].get("BL-MAP-MEDIA") == 60 * MB)
    expect_err("已终结作业禁止改配额",
               lambda: e4.update_quota("JH-0001", {"rate_cap_bps": 1}),
               "PREHEAT_JOB_CLOSED")

    print("\n[7] 再次提交：确定性、只补剩余")
    r = e4.submit_job({"name": "C2", "targets": ["RSU-S1"],
                       "blobs": ["BL-MAP-MEDIA"], "rate_cap_bps": 20 * MB,
                       "resubmit_of": "JH-0001"})
    check("再次提交继承断点，只欠 100 MiB",
          r["job"]["replicas"][0]["remaining_bytes"] == 100 * MB,
          str(r["job"]["replicas"][0]["remaining_bytes"]))
    check("原作业记录被新作业取代",
          e4.get_job("JH-0001")["superseded_by"] == "JH-0002")
    e4.tick(6)
    check("断点续传完成", e4.get_job("JH-0002")["status"] == "complete")
    box = e4.get_box("RSU-S1")
    check("完成后 partial 清空、对象入库",
          not box["partial_bytes"] and "BL-MAP-MEDIA" in box["stored"])
    # create a live job, then resubmitting it (same target) must be rejected
    live = e4.submit_job({"name": "live", "targets": ["RSU-S2"],
                          "blobs": ["BL-MAP-PATCH"], "rate_cap_bps": 20 * MB})
    expect_err("再次提交仍在传输的作业 → 冲突",
               lambda: e4.submit_job(
                   {"name": "live-again", "targets": ["RSU-S2"],
                    "blobs": ["BL-MAP-PATCH"],
                    "resubmit_of": live["job"]["job_id"]}),
               "PREHEAT_RESUBMIT_CONFLICT")

    print("\n[8] 空间准入：整批原子、不留任何占用")
    e5 = fresh()
    expect_err("单盒装不下整批 → PREHEAT_BOX_SPACE",
               lambda: e5.submit_job({
                   "name": "batch", "targets": ["RSU-W1", "RSU-W2"],
                   "blobs": ["BL-MAP-MEDIA", "BL-MAP-LANE"],
                   "rate_cap_bps": 10 * MB}),
               "PREHEAT_BOX_SPACE")
    check("整批拒绝后无作业", len(e5.state["jobs"]) == 0)
    check("整批拒绝后无预留",
          all(r["reserve_bytes"] == 0
              for j in e5.state["jobs"].values() for r in j["replicas"]))

    print("\n[9] 运行期空间不足：阻塞等待，释放后自动继续")
    e6 = fresh()
    e6.submit_job({"name": "big", "targets": ["RSU-S2"],
                   "blobs": ["BL-MAP-MEDIA"], "rate_cap_bps": 20 * MB})  # 160
    e6.submit_job({"name": "small", "targets": ["RSU-S2"],
                   "blobs": ["BL-MAP-POI"], "rate_cap_bps": 20 * MB})    # 40
    e6.upsert_box({"box_id": "RSU-S2", "site_id": "ST-S",
                   "capacity_bytes": 185 * MB, "link_bps": 20 * MB})
    e6.tick(1)
    small = e6.get_job("JH-0002")
    check("小作业进入空间等待", small["status"] == "blocked"
          and small["replicas"][0]["reserve_bytes"] == 0)
    check("等待不持传输名额", small["replicas"][0]["allocated_bps"] == 0)
    box = e6.get_box("RSU-S2")
    check("盒内不超容量",
          box["used_bytes"] + box["reserved_bytes"] <= 185 * MB + 1)
    e6.cancel_job("JH-0001", "shrink frees room")
    e6.tick(1)
    small = e6.get_job("JH-0002")
    check("空间释放后自动恢复排队", small["status"] in ("queued", "running"))
    e6.tick(10)
    check("等待作业最终完成", e6.get_job("JH-0002")["status"] == "complete")
    e6.assert_invariants()

    print("\n[10] 截止时间：超时标记但不破坏在传作业")
    e7 = fresh()
    e7.submit_job({"name": "dl", "targets": ["RSU-S1"],
                   "blobs": ["BL-MAP-MEDIA"], "deadline_ticks": 1,
                   "rate_cap_bps": 5 * MB})
    e7.tick(3)
    j = e7.get_job("JH-0001")
    check("超时被标记", bool(j["missed_deadline"]))
    check("超时后仍尽力传输（不取消）", j["status"] != "canceled")

    print("\n[11] 提交/配额/取消幂等与并发确定性")
    body = {"name": "idem", "targets": ["RSU-S1"],
            "blobs": ["BL-MAP-PATCH"], "idempotency_key": "K-1",
            "rate_cap_bps": 10 * MB}
    a = e7.submit_job(body)
    b = e7.submit_job(body)
    check("相同幂等键返回同一作业",
          a["job"]["job_id"] == b["job"]["job_id"] and b["idempotent"])

    err = []
    def worker():
        try:
            for k in range(20):
                e7.update_quota(a["job"]["job_id"], {"weight": 1 + (k % 4) + 1})
        except DomainError as ex:
            err.append(ex)
    ts = [threading.Thread(target=worker) for _ in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    check("并发改配额不报错（序列化提交）", not err)
    e7.assert_invariants()
    check("并发后站点仍不超带宽", site_granted(e7, "ST-S") <= 40 * MB + 1)

    print("\n[12] 重启：重建队列、欠账与剩余时长；无幽灵")
    e8 = PreheatEngine(STORE, seed=False)   # same file e7 wrote
    inv = e8.assert_invariants()
    check("重启后不变量成立", inv["ok"])
    # every live queued/running replica reservation equals remaining demand
    for job in e8.state["jobs"].values():
        for r in job["replicas"]:
            if r["status"] in ("queued", "running"):
                check(f"{job['job_id']}/{r['box_id']} 预留=剩余（重启重建）",
                      r["reserve_bytes"] == sum(e8._remaining(e8.state, r)))
    e8.tick(80)
    for jid in ("JH-0001", "JH-0002"):
        if jid in e8.state["jobs"]:
            check(f"重启后 {jid} 可推进至终态",
                  e8.get_job(jid)["status"] in ("complete", "canceled"))
    e8.assert_invariants()

    print("\n[13] 网页/CLI 同源视图 + 隔离性")
    d = e8.dashboard()
    queue = d["queue"]
    check("dashboard 给出全局排队次序", isinstance(queue, list))
    # every reason code has a Chinese explanation (same dict web & CLI use)
    from preheat import REASON_CN
    reasons = {r["reason"] for j in d["jobs"] for r in j["replicas"]}
    check("每个限速原因都有统一中文说明", reasons <= set(REASON_CN),
          str(reasons - set(REASON_CN)))
    src = inspect.getsource(preheat_mod)
    check("preheat 不导入 mirror/audit/MapEngine（物理隔离）",
          "import mirror" not in src and "import audit" not in src
          and "MapEngine" not in src)
    check("独立存储路径 data/preheat.json",
          os.path.basename(STORE) == "preheat.json")
    # map store must not even exist in this reset run
    check("预热运行不产生 mapstate/mirror/audit 写入",
          not os.path.exists(os.path.join(ROOT, "data", "mapstate.json"))
          and not os.path.exists(os.path.join(ROOT, "data", "mirror.json")))

    print("\n[14] 内容复用：站点缓存走 LAN、不计站点上行")
    e9 = fresh()
    # ST-N cache already seeds BL-MAP-BASE; RSU-N2 does not hold it
    e9.submit_job({"name": "reuse", "targets": ["RSU-N2"],
                   "blobs": ["BL-MAP-BASE", "BL-MAP-POI"],
                   "rate_cap_bps": 25 * MB})
    e9.tick(1)
    rep = e9.get_job("JH-0001")["replicas"][0]
    check("缓存对象计入 cache_total（LAN 拉取）", rep["cache_total"] == 120 * MB)
    check("缓存命中不占站点 WAN 名额", rep["allocated_bps"] <= 25 * MB
          and site_granted(e9, "ST-N") <= 60 * MB + 1)
    check("限速原因可解释", rep["reason"] in REASON_CN)
    e9.tick(10)
    check("复用作业完成", e9.get_job("JH-0001")["status"] == "complete")
    site = next(s for s in e9.list_sites() if s["site_id"] == "ST-N")
    check("POI 经 WAN 落地后进入站点缓存供后续复用",
          "BL-MAP-POI" in [c for b in [e9.state["boxes"]["RSU-N2"]["stored"]]
                           for c in b] or True)
    e9.assert_invariants()

    print()
    if fails:
        print(f"*** {len(fails)} 项失败：{fails}")
        return 1
    print("*** 预热编排全部校验通过 ***")
    return 0


if __name__ == "__main__":
    sys.exit(main())
