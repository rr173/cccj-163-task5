#!/usr/bin/env python3
"""End-to-end verification of capacity-aware preheat orchestration.

Resets all stores, then drives the preheat engine / CLI directly. Asserts
every requirement from the preheat brief:

  * dispatcher submits a batch of boxes with deadline / link cap / min-online
  * scheduling respects site shared bandwidth, box free disk, content reuse
  * disk reservation and transfer slot are acquired/released as a PAIR
  * no starvation: eligible-but-unserved jobs accrue debt and get served
  * pause/resume only sends the remaining bytes
  * space shortage / link degrade / box offline / cancel / crash leave no
    ghost occupancy; a restart rebuilds queue, debts and remaining times
  * quota change / cancel / resubmit are deterministic (verdict-once)
  * concurrency never exceeds site bandwidth or box disk
  * web and CLI read the same store (same order / reasons / ETAs)
  * orchestration never touches live map state or issued trip snapshots
"""
import json
import os
import shutil
import subprocess
import sys
import threading

import engine
from engine import DomainError, MapEngine
from mirror import MirrorEngine
from preheat import PreheatEngine

ROOT = os.path.dirname(os.path.abspath(__file__))
LIVE = os.path.join(ROOT, "data", "mapstate.json")
MIR = os.path.join(ROOT, "data", "mirror.json")
PRE = os.path.join(ROOT, "data", "preheat.json")
CLI = os.path.join(ROOT, "preheat_cli.py")

fails = []


def check(name, cond, detail=""):
    print(("  ✓ " if cond else "  ✗ FAIL: ") + name +
          (f"  [{detail}]" if detail and not cond else ""))
    if not cond:
        fails.append(name)


def expect_err(name, fn, code, status):
    try:
        fn()
    except DomainError as e:
        check(name, e.code == code and e.status == status,
              f"got {e.status} {e.code}: {e.message}")
        return e
    check(name, False, "no error raised")
    return None


def live_fp(eng):
    s = eng.state
    return json.dumps({"gen": s["region_gen"], "heads": s["region_heads"],
                       "snapshots": s["snapshots"], "vehicles": s["vehicles"],
                       "patches": s["patches"], "releases": s["releases"],
                       "events": len(s["events"])}, sort_keys=True)


def mirror_fp(mir):
    return json.dumps(mir.state, sort_keys=True)


def main():
    shutil.rmtree(os.path.join(ROOT, "data"), ignore_errors=True)
    eng = MapEngine(LIVE)
    mir = MirrorEngine(MIR, eng)
    ph = PreheatEngine(PRE, mir)
    fp0, mfp0 = live_fp(eng), mirror_fp(mir)

    print("\n[0] 准备：生成两份不可变清单（gen1 基线 / gen2 演进）")
    m1 = mir.create_manifest({"devices": ["RSU-09"], "note": "gen1"})["manifest"]
    eng.validate_candidate("RC-0002")
    eng.publish_candidate("RC-0002")
    m2 = mir.create_manifest({"devices": ["RSU-09"], "note": "gen2"})["manifest"]
    check("gen2 清单与 gen1 共享部分对象（内容复用的基础）",
          {o["hash"] for o in m1["objects"]} & {o["hash"] for o in m2["objects"]}
          and m1["manifest_id"] != m2["manifest_id"])
    fp_live_after_publish = live_fp(eng)

    print("\n[1] 注册站点与盒子；提交作业（截止/链路上限/最小在线保障）")
    ph.upsert_site("SITE-A", 2.0)
    ph.upsert_box("RSU-01", "SITE-A", 50, 1.0)
    ph.upsert_box("RSU-02", "SITE-A", 50, 1.0)
    j1 = ph.create_job({"manifest_id": m1["manifest_id"],
                        "targets": ["RSU-01", "RSU-02"],
                        "deadline_in_sec": 120, "link_cap_kbps": 0.8,
                        "min_online": 2, "note": "晚高峰预热"})
    job1 = j1["job"]
    check("作业 PJ-0001 已创建", job1["job_id"] == "PJ-0001", job1["job_id"])
    check("两台目标盒各得一个任务", len(job1["tasks"]) == 2)
    check("最小在线保障 2/2", job1["guarantee"]["required"] == 2)
    t1, t2 = job1["tasks"]
    check("磁盘预留与传输名额成对取得",
          t1["status"] == "transferring" and t1["reserved_bytes"] > 0
          and t2["status"] == "transferring" and t2["reserved_bytes"] > 0)
    check("站点授予带宽不超容量",
          t1["rate_kbps"] + t2["rate_kbps"] <= 2.0 + 1e-6,
          f"{t1['rate_kbps']}+{t2['rate_kbps']}")
    check("限速原因=作业速率上限", t1["limit"]["code"] == "JOB_CAP",
          t1["limit"]["code"])
    check("预计完成时间存在且早于截止",
          t1["eta"] is not None and t1["eta"] <= job1["deadline"])
    check("不变量自检通过", ph.check_invariants() == [])

    print("\n[2] 重复提交 → 返回首次作业（确定结果）")
    j1b = ph.create_job({"manifest_id": m1["manifest_id"],
                         "targets": ["RSU-01", "RSU-02"],
                         "deadline_in_sec": 120, "link_cap_kbps": 0.8,
                         "min_online": 2, "note": "晚高峰预热"})
    check("幂等返回首次作业", j1b["idempotent"] is True
          and j1b["job"]["job_id"] == "PJ-0001")

    print("\n[3] 站点共享带宽：收紧配额使站点饱和，第三个任务排队、欠账累计、不被饿死")
    ph.set_site_capacity("SITE-A", 1.6)   # 两个 0.8 任务恰好占满
    ph.upsert_box("RSU-03", "SITE-A", 50, 5.0)
    j2 = ph.create_job({"manifest_id": m1["manifest_id"], "targets": ["RSU-03"],
                        "deadline_in_sec": 600, "link_cap_kbps": 5.0,
                        "min_online": 1})
    q = ph.queue()
    t3 = next(t for t in q["queue"] if t["box_id"] == "RSU-03")
    check("站点带宽已满 → 排队", t3["status"] == "queued"
          and t3["limit"]["code"] == "SITE_SATURATED", t3["limit"]["code"])
    check("排队任务无磁盘预留（成对：未得名额即无预留）",
          t3["reserved_bytes"] == 0)
    debt0 = t3["debt_kb"]
    ph.tick(1.0)
    q = ph.queue()
    t3 = next(t for t in q["queue"] if t["box_id"] == "RSU-03")
    check("欠账随等待累计", t3["debt_kb"] > debt0,
          f"{debt0} → {t3['debt_kb']}")
    served_before_done = False
    for _ in range(40):
        ph.tick(1.0)
        q = ph.queue()
        row = next((t for t in q["queue"] if t["box_id"] == "RSU-03"), None)
        if row and row["status"] == "transferring":
            served_before_done = True
        if not row:
            break
    check("排队任务最终获得传输名额（不饿死）", served_before_done)
    check("不变量自检通过", ph.check_invariants() == [])

    print("\n[4] 盒内空间不足：BOX_DISK_FULL，不留预留；扩容后获得名额")
    ph.upsert_box("RSU-04", "SITE-A", 0.001, 5.0)   # 1000 B < 1630 B 需求
    j3 = ph.create_job({"manifest_id": m1["manifest_id"], "targets": ["RSU-04"],
                        "deadline_in_sec": 600, "link_cap_kbps": 5.0,
                        "min_online": 1})
    t4 = j3["job"]["tasks"][0]
    check("空间不足 → 排队且原因为 BOX_DISK_FULL",
          t4["status"] == "queued" and t4["limit"]["code"] == "BOX_DISK_FULL",
          t4["limit"]["code"])
    check("空间不足不占传输名额/磁盘预留", t4["reserved_bytes"] == 0
          and t4["rate_kbps"] == 0)
    ph.upsert_box("RSU-04", "SITE-A", 50, 5.0)      # 扩容
    q = ph.queue()
    t4 = next(t for t in q["queue"] if t["box_id"] == "RSU-04")
    check("扩容后按欠账优先获得名额", t4["status"] == "transferring"
          and t4["reserved_bytes"] > 0)
    check("不变量自检通过", ph.check_invariants() == [])

    print("\n[5] 暂停/恢复：成对释放，恢复只补剩余数据")
    jid3 = j3["job"]["job_id"]
    ph.tick(1.0)
    before = next(t for t in ph.queue()["queue"] if t["box_id"] == "RSU-04")
    check("暂停前已有部分进度", before["bytes_done"] > 0)
    ph.pause_job(jid3)
    q = ph.queue()
    t4 = next(t for t in q["inactive"] if t["box_id"] == "RSU-04")
    check("暂停后名额/预留成对释放", t4["status"] == "paused"
          and t4["reserved_bytes"] == 0 and t4["rate_kbps"] == 0)
    ph.resume_job(jid3)
    q = ph.queue()
    t4 = next(t for t in q["queue"] if t["box_id"] == "RSU-04")
    check("恢复后只补剩余（已传字节不回退）",
          t4["bytes_done"] == before["bytes_done"] and t4["bytes_done"] > 0,
          f"{before['bytes_done']} → {t4['bytes_done']}")
    for _ in range(30):
        ph.tick(1.0)
        if not any(t["box_id"] == "RSU-04" for t in ph.queue()["queue"]):
            break
    t4 = next(t for t in ph.queue()["inactive"] if t["box_id"] == "RSU-04")
    check("RSU-04 任务最终完成", t4["status"] == "done")
    check("不变量自检通过", ph.check_invariants() == [])

    print("\n[6] 链路降级 / 盒子离线：成对让路，恢复后续传")
    ph.set_box_link("RSU-01", 0.2)   # 慢链路，任务存活更久
    j4 = ph.create_job({"manifest_id": m2["manifest_id"],
                        "targets": ["RSU-01", "RSU-02"],
                        "deadline_in_sec": 600, "link_cap_kbps": 1.0,
                        "min_online": 2})
    ph.tick(1.0)   # RSU-01 前进 200 B；RSU-02 内容少直接完成
    ph.set_box_link("RSU-01", 0.0)
    q = ph.queue()
    t = next(t for t in q["queue"] if t["box_id"] == "RSU-01")
    check("链路降为 0 → 让路且原因为 LINK_DOWN",
          t["status"] == "queued" and t["limit"]["code"] == "LINK_DOWN"
          and t["reserved_bytes"] == 0)
    ph.set_box_link("RSU-01", 0.4)
    q = ph.queue()
    t = next(t for t in q["queue"] if t["box_id"] == "RSU-01")
    check("链路恢复（降级值）→ 重新传输且限速原因 BOX_LINK",
          t["status"] == "transferring" and t["limit"]["code"] == "BOX_LINK",
          f"{t['status']} {t['limit']['code']}")
    done_before_offline = t["bytes_done"]
    check("降级前已有部分进度", done_before_offline > 0)
    ph.set_box_online("RSU-01", False)
    q = ph.queue()
    t = next(t for t in q["queue"] if t["box_id"] == "RSU-01")
    check("盒子离线 → 成对释放，原因 BOX_OFFLINE",
          t["status"] == "queued" and t["limit"]["code"] == "BOX_OFFLINE"
          and t["reserved_bytes"] == 0)
    check("离线期间 ETA 不可预计", t["eta"] is None)
    ph.set_box_online("RSU-01", True)
    q = ph.queue()
    t = next(t for t in q["queue"] if t["box_id"] == "RSU-01")
    check("重新上线后续传（已传字节保留）",
          t["bytes_done"] == done_before_offline)
    check("不变量自检通过", ph.check_invariants() == [])

    print("\n[7] 服务崩溃重启：重建队列/欠账/剩余时长，无幽灵占用")
    ph.tick(1.0)
    snap = {t["task_id"]: (t["bytes_done"], t["debt_kb"])
            for t in ph.queue()["queue"]}
    ph2 = PreheatEngine(PRE, mir)   # 模拟崩溃后重启（同一存储）
    rec = [e for e in ph2.list_events() if e["type"] == "SERVICE_RECOVERED"]
    check("记录服务恢复事件", len(rec) == 1)
    check("重启后不变量成立（无幽灵占用）", ph2.check_invariants() == [])
    q2 = ph2.queue()
    check("队列重建且欠账保留",
          all(abs(next(t for t in q2["queue"]
                       if t["task_id"] == tid)["debt_kb"] - debt) < 1e-6
              for tid, (_, debt) in snap.items()
              if any(t["task_id"] == tid for t in q2["queue"])))
    check("剩余时长（ETA）重建", any(t["eta"] for t in q2["queue"]))
    done_map = {t["task_id"]: t["bytes_done"] for t in q2["queue"]}
    check("已传字节保留（续传只补剩余）",
          all(done_map.get(tid, -1) == done for tid, (done, _) in snap.items()))
    ph = ph2

    print("\n[8] 内容复用：已完成 gen1 的盒子对 gen2 只传增量")
    for _ in range(60):
        ph.tick(1.0)
        if not ph.queue()["queue"]:
            break
    j5 = ph.create_job({"manifest_id": m2["manifest_id"],
                        "targets": ["RSU-01", "RSU-03"],
                        "deadline_in_sec": 600, "link_cap_kbps": 1.0,
                        "min_online": 2})
    ta, tb = j5["job"]["tasks"]
    check("gen2 任务复用率 > 0（部分对象已在盒上）",
          all(t["reuse_ratio"] > 0 for t in j5["job"]["tasks"]),
          str([t["reuse_ratio"] for t in j5["job"]["tasks"]]))
    j6 = ph.create_job({"manifest_id": m2["manifest_id"], "targets": ["RSU-02"],
                        "deadline_in_sec": 600, "link_cap_kbps": 1.0,
                        "min_online": 1})
    t6 = j6["job"]["tasks"][0]
    check("全部对象已在盒上 → 复用率 100% 即时完成",
          t6["reuse_ratio"] == 1.0 and t6["status"] == "done",
          f"reuse={t6['reuse_ratio']} status={t6['status']}")
    check("不变量自检通过", ph.check_invariants() == [])

    print("\n[9] 修改配额：确定性抢占；取消：判定一次终身")
    for _ in range(60):
        ph.tick(1.0)
        if not ph.queue()["queue"]:
            break
    j7 = ph.create_job({"manifest_id": m2["manifest_id"], "targets": ["RSU-03"],
                        "deadline_in_sec": 600, "link_cap_kbps": 5.0,
                        "min_online": 1})
    ph.upsert_box("RSU-10", "SITE-A", 50, 5.0)   # 全新盒子，无复用
    j8 = ph.create_job({"manifest_id": m1["manifest_id"], "targets": ["RSU-10"],
                        "deadline_in_sec": 600, "link_cap_kbps": 5.0,
                        "min_online": 1})
    ph.tick(1.0)   # 让先排上的任务积累欠账
    ph.set_site_capacity("SITE-A", 0.5)
    q = ph.queue()
    granted = sum(t["rate_kbps"] for t in q["queue"])
    check("配额收紧后授予总量 ≤ 新配额", granted <= 0.5 + 1e-6, str(granted))
    check("不变量自检通过", ph.check_invariants() == [])
    c1 = ph.cancel_job(j8["job"]["job_id"])
    c2 = ph.cancel_job(j8["job"]["job_id"])
    check("重复取消返回首次结果", c2["idempotent"] is True
          and c1["result"] == c2["result"])
    q = ph.queue()
    check("取消后无该作业的任何占用",
          all(t["job_id"] != j8["job"]["job_id"] for t in q["queue"])
          and ph.check_invariants() == [])
    j8b = ph.create_job({"manifest_id": m1["manifest_id"], "targets": ["RSU-10"],
                         "deadline_in_sec": 600, "link_cap_kbps": 5.0,
                         "min_online": 1})
    check("取消后再次提交 → 确定性创建新作业",
          not j8b["idempotent"] and j8b["job"]["job_id"] != j8["job"]["job_id"])
    expect_err("已完成作业不能取消",
               lambda: ph.cancel_job(j6["job"]["job_id"]),
               "PREHEAT_JOB_CLOSED", 409)
    for jj in (j7, j8b):   # 清理：仅取消仍未终结的作业
        if ph.get_job(jj["job"]["job_id"])["status"] in ("active", "paused"):
            ph.cancel_job(jj["job"]["job_id"])

    print("\n[10] 并发压测：提交/tick/配额/取消并发，永不突破站点带宽与盒内空间")
    ph.upsert_box("RSU-05", "SITE-A", 50, 2.0)
    ph.upsert_box("RSU-06", "SITE-A", 50, 2.0)
    stop = threading.Event()
    errors = []

    def submitter(n):
        while not stop.is_set():
            try:
                ph.create_job({"manifest_id": m1["manifest_id"],
                               "targets": ["RSU-05", "RSU-06"][(n // 3) % 2],
                               "deadline_in_sec": 300, "link_cap_kbps": 2.0,
                               "min_online": 1, "note": f"n{n}"})
            except DomainError:
                pass
            finally:
                n += 1

    def ticker():
        while not stop.is_set():
            try:
                ph.tick(0.5)
            except DomainError as e:
                errors.append(str(e))

    def quotas():
        caps = [2.0, 0.7, 1.3, 2.0]
        i = 0
        while not stop.is_set():
            try:
                ph.set_site_capacity("SITE-A", caps[i % len(caps)])
            except DomainError as e:
                errors.append(str(e))
            i += 1

    threads = ([threading.Thread(target=submitter, args=(i,)) for i in range(3)]
               + [threading.Thread(target=ticker),
                  threading.Thread(target=quotas)])
    for t in threads:
        t.start()
    import time
    time.sleep(2.5)
    stop.set()
    for t in threads:
        t.join()
    inv = ph.check_invariants()
    check("并发期间无引擎异常", errors == [], "; ".join(errors[:3]))
    check("并发后不变量成立（站点带宽/盒内空间/成对占用）", inv == [], "; ".join(inv))
    # 取消全部活跃作业，存储必须归零占用
    for jx in ph.list_jobs():
        if jx["status"] in ("active", "paused"):
            ph.cancel_job(jx["job_id"])
    check("全部取消后无任何占用",
          ph.check_invariants() == []
          and all(b["reserved_bytes"] == 0 for b in ph.list_boxes())
          and all(s["granted_kbps"] == 0 for s in ph.list_sites()))

    print("\n[11] 隔离：编排全程不触碰线上地图与镜像仓库")
    check("线上地图状态指纹不变", live_fp(eng) == fp_live_after_publish)
    check("镜像仓库存储指纹不变", mirror_fp(mir) == mirror_fp(MirrorEngine(MIR, eng)))
    eng2 = MapEngine(LIVE)
    check("mapstate.json 未被改写", live_fp(eng2) == fp_live_after_publish)

    print("\n[12] CLI 与网页同源：同一存储的排队次序/限速原因/预计完成一致")
    shutil.rmtree(os.path.join(ROOT, "data"), ignore_errors=True)
    eng = MapEngine(LIVE)
    mir = MirrorEngine(MIR, eng)
    mm = mir.create_manifest({"devices": ["RSU-09"], "note": "gen1"})["manifest"]
    ph = PreheatEngine(PRE, mir)
    ph.upsert_site("SITE-A", 1.0)
    ph.upsert_box("RSU-01", "SITE-A", 50, 1.0)
    ph.upsert_box("RSU-02", "SITE-A", 50, 1.0)
    ph.create_job({"manifest_id": mm["manifest_id"], "targets": ["RSU-01", "RSU-02"],
                   "deadline_in_sec": 120, "link_cap_kbps": 0.8, "min_online": 2})

    def run_cli(*args):
        return subprocess.run([sys.executable, CLI, *args],
                              capture_output=True, text=True, cwd=ROOT)
    r = run_cli("queue")
    check("CLI queue 正常退出", r.returncode == 0, r.stderr[:200])
    eng_q = ph.queue()
    cli_lines = [ln for ln in r.stdout.splitlines() if ln.strip().startswith("#")]
    check("CLI 与引擎排队次序一致",
          [ln.split()[1] for ln in cli_lines]
          == [t["task_id"] for t in eng_q["queue"]],
          r.stdout[:300])
    for t in eng_q["queue"]:
        check(f"CLI 展示 {t['task_id']} 相同限速原因",
              t["limit"] is None or t["limit"]["code"] in r.stdout)
        if t["eta"]:
            check(f"CLI 展示 {t['task_id']} 相同预计完成时间", t["eta"] in r.stdout)
    r = run_cli("submit", mm["manifest_id"], "RSU-01,RSU-02", "120", "0.8", "2")
    check("CLI 重复提交返回首次作业（幂等）",
          "PJ-0001" in r.stdout and '"idempotent": true' in r.stdout,
          r.stdout[-200:])
    r = run_cli("tick", "1")
    check("CLI tick 推进时钟", r.returncode == 0 and "时钟推进" in r.stdout)
    r = run_cli("jobs")
    check("CLI jobs 列出作业", "PJ-0001" in r.stdout)
    r = run_cli("pause", "PJ-0001")
    check("CLI 暂停作业", r.returncode == 0)
    r = run_cli("resume", "PJ-0001")
    check("CLI 恢复作业", r.returncode == 0)
    r = run_cli("cancel", "PJ-0001")
    check("CLI 取消作业", r.returncode == 0)
    r = run_cli("cancel", "PJ-0001")
    check("CLI 重复取消返回首次结果", '"idempotent": true' in r.stdout)
    ph = PreheatEngine(PRE, mir)   # 如同网页后端重新读取同一存储
    check("取消后队列清空且无占用",
          ph.queue()["queue"] == [] and ph.check_invariants() == [])

    print("\n[13] 最小在线保障预警与截止风险")
    ph.upsert_site("SITE-B", 0.2)
    ph.upsert_box("RSU-07", "SITE-B", 50, 0.2)
    ph.upsert_box("RSU-08", "SITE-B", 50, 0.2)
    j9 = ph.create_job({"manifest_id": mm["manifest_id"],
                        "targets": ["RSU-07", "RSU-08"],
                        "deadline_in_sec": 2, "link_cap_kbps": 0.2,
                        "min_online": 2})
    g = j9["job"]["guarantee"]
    check("带宽不足以赶在截止前完成 → 保障预警",
          g["at_risk"] is True and g["predicted_by_deadline"] < 2, str(g))
    t9 = j9["job"]["tasks"][0]
    check("任务级超截止风险标记", t9["at_risk"] is True)

    print("\n[14] 错误码与边界")
    expect_err("未知清单", lambda: ph.create_job(
        {"manifest_id": "MF-9999", "targets": ["RSU-07"],
         "deadline_in_sec": 10, "link_cap_kbps": 1, "min_online": 1}),
        "PREHEAT_MANIFEST_UNKNOWN", 404)
    expect_err("未注册盒子", lambda: ph.create_job(
        {"manifest_id": mm["manifest_id"], "targets": ["RSU-99"],
         "deadline_in_sec": 10, "link_cap_kbps": 1, "min_online": 1}),
        "PREHEAT_BOX_UNKNOWN", 404)
    expect_err("空目标", lambda: ph.create_job(
        {"manifest_id": mm["manifest_id"], "targets": [],
         "deadline_in_sec": 10, "link_cap_kbps": 1, "min_online": 1}),
        "PREHEAT_NO_TARGETS", 400)
    expect_err("未知站点", lambda: ph.set_site_capacity("SITE-Z", 1.0),
               "PREHEAT_SITE_UNKNOWN", 404)
    expect_err("未知作业", lambda: ph.pause_job("PJ-9999"),
               "PREHEAT_JOB_UNKNOWN", 404)
    expect_err("非法 tick", lambda: ph.tick(0), "BAD_REQUEST", 400)

    print("\n" + ("=" * 60))
    if fails:
        print(f"✗ {len(fails)} 项失败: {fails}")
        return 1
    print("✓ 预热编排全部校验通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
