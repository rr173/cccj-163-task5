#!/usr/bin/env python3
"""End-to-end verification of the roadside-box map mirror repository.

Resets both stores, then drives the mirror engine / CLI directly. Asserts
every requirement from the mirror brief:

  * publisher cuts an immutable manifest from an effective snapshot + devices
  * device agent pulls compressed chunks; chunk hashes and the whole-package
    root hash are verified before staging
  * pulls resume after network drops; confirmed chunks are never double-counted
  * one pending-switch image per box; older generations / corrupt objects can
    never overwrite the current image
  * the box flips its pointer atomically and only after every object verifies;
    failures before the flip keep the previous state
  * after a restart, pulling / ready / switched and every processing result
    are still distinguishable
  * web API and device CLI read the same store (same progress, same reasons)
  * retrying the same manifest returns the first task and switch result
  * the mirror never touches live map state or issued trip snapshots
"""
import base64
import hashlib
import json
import os
import shutil
import subprocess
import sys

import engine
from engine import DomainError, MapEngine
from mirror import MirrorEngine

ROOT = os.path.dirname(os.path.abspath(__file__))
LIVE = os.path.join(ROOT, "data", "mapstate.json")
MIR = os.path.join(ROOT, "data", "mirror.json")

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


def chunk_hash(mir, tid, index):
    payload = mir.get_chunk_payload(tid, index)
    blob = base64.b64decode(payload["data_b64"])
    return "sha256:" + hashlib.sha256(blob).hexdigest()


def main():
    shutil.rmtree(os.path.join(ROOT, "data"), ignore_errors=True)
    eng = MapEngine(LIVE)
    mir = MirrorEngine(MIR, eng)
    fp0 = live_fp(eng)

    print("\n[1] 从当前生效版本生成不可变清单（gen1，设备 RSU-01/02/03）")
    r1 = mir.create_manifest({"devices": ["RSU-01", "RSU-02"], "note": "gen1 基线"})
    m1 = r1["manifest"]
    check("清单 MF-0001 已生成", m1["manifest_id"] == "MF-0001", m1["manifest_id"])
    check("清单含 5 个对象（描述符 + 4 瓦片）", len(m1["objects"]) == 5,
          str(len(m1["objects"])))
    check("对象打包为 3 个压缩块", len(m1["chunks"]) == 3)
    check("整包根散列存在", m1["root_hash"].startswith("sha256:"))
    check("清单版本为当前生效版本", m1["source"]["version"] == "north=1/south=1",
          m1["source"]["version"])
    t1, t2 = r1["tasks"]
    check("两台设备各得一个拉取任务", t1["task_id"] == "MT-0001"
          and t2["task_id"] == "MT-0002" and not r1["idempotent"])
    check("任务初始为拉取中 0/3",
          t1["status"] == "pulling" and t1["progress"] == {"total": 3, "confirmed": 0})
    dev = mir.get_device("RSU-01")
    check("盒子挂出待切换镜像任务", dev["pending_task_id"] == "MT-0001")
    r1b = mir.create_manifest({"devices": ["RSU-03"], "note": "备份清单"})
    check("第三台设备清单 MF-0002", r1b["manifest"]["manifest_id"] == "MF-0002")

    print("\n[2] 重试同一清单 → 返回第一次任务（幂等）")
    r2 = mir.create_manifest({"devices": ["RSU-01", "RSU-02"], "note": "gen1 基线"})
    check("重复提交返回同一清单", r2["idempotent"] is True
          and r2["manifest"]["manifest_id"] == "MF-0001")
    check("重复提交返回首次任务", [t["task_id"] for t in r2["tasks"]] ==
          ["MT-0001", "MT-0002"] and all(t["idempotent"] for t in r2["tasks"]))
    check("未产生新任务", len(mir.state["tasks"]) == 3)

    print("\n[3] 逐块确认：块散列校验 + 已确认块不重复计数")
    h0 = chunk_hash(mir, "MT-0001", 0)
    a = mir.ack_chunk("MT-0001", 0, h0)
    check("第 0 块确认", a["accepted"] and not a["idempotent"]
          and a["task"]["progress"]["confirmed"] == 1)
    a = mir.ack_chunk("MT-0001", 0, h0)
    check("重复确认同一块 → 幂等，不重复计数",
          a["idempotent"] and a["task"]["progress"]["confirmed"] == 1)
    expect_err("未知块 → 404 MIRROR_CHUNK_UNKNOWN",
               lambda: mir.ack_chunk("MT-0001", 9, h0),
               "MIRROR_CHUNK_UNKNOWN", 404)
    bad = mir.ack_chunk("MT-0001", 1, "sha256:" + "0" * 64)
    check("坏散列被拒绝（可重试），进度不变",
          not bad["accepted"] and bad["retryable"]
          and bad["rejection"]["code"] == "MIRROR_CHUNK_HASH_MISMATCH"
          and bad["task"]["progress"]["confirmed"] == 1)
    t = mir.get_task("MT-0001")
    check("拒绝理由写入任务处理记录（与网页同一数据源）",
          any(e["event"] == "CHUNK_REJECTED" for e in t["journal"]))

    print("\n[4] 网络中断可续拉，已确认块不重复计数")
    r = mir.run_agent("MT-0002", stop_after=2)
    check("拉 2 块后中断", r["task"]["progress"]["confirmed"] == 2
          and r["task"]["status"] == "pulling"
          and any(x.get("interrupted") for x in r["results"]))
    r = mir.run_agent("MT-0002")
    check("续拉只补剩余块（总计 3 而非 5）",
          r["task"]["progress"]["confirmed"] == 3)
    check("整包根散列复核通过 → 可切换",
          r["task"]["status"] == "ready"
          and r["task"]["staged"]["root_hash"] == m1["root_hash"])

    print("\n[5] 原子换指针：全部验真后才允许；重试返回首次切换结果")
    expect_err("拉取中切换 → 409 MIRROR_NOT_READY",
               lambda: mir.switch_task("MT-0001"), "MIRROR_NOT_READY", 409)
    sw = mir.switch_task("MT-0002")
    check("可切换任务原子换指针",
          sw["switched"] and sw["status"] == "active" and not sw["idempotent"])
    dev = mir.get_device("RSU-02")
    check("盒子当前镜像已指向新清单",
          dev["current_image"]["manifest_id"] == "MF-0001"
          and dev["current_image"]["version"] == "north=1/south=1")
    check("盒子不再有待切换任务", dev["pending_task_id"] is None)
    sw2 = mir.switch_task("MT-0002")
    check("重复切换返回首次结果",
          sw2["idempotent"] and sw2["result"] == sw["result"])
    r = mir.run_agent("MT-0001")
    check("MT-0001 续拉完成（含此前确认的块，不重复计数）",
          r["task"]["status"] == "ready" and r["task"]["progress"]["confirmed"] == 3)
    mir.switch_task("MT-0001")
    check("两台盒子均已切换", mir.get_device("RSU-01")["current_image"] is not None)
    check("镜像子系统未改动地图中心状态", live_fp(eng) == fp0)

    print("\n[6] 线上正常演进（刻意操作）：发布 gen2 + 激活补丁 + 固定行程快照")
    eng.validate_candidate("RC-0002")
    eng.publish_candidate("RC-0002")
    p1 = eng.create_patch({"scope": {"tiles": {"A00": ["lce"]}},
                           "reason": "K12 事故管制", "priority": 100})
    eng.activate_patch(p1["patch_id"])
    snap = eng.create_route({"vehicle_id": "VH-01",
                             "from_node": "A00/w", "to_node": "B01/e"})
    fp1 = live_fp(eng)
    check("线上已推进到 north=2/south=1+P-0001",
          eng.resolve_effective()["version"].startswith("north=2/south=1+P-0001"))

    print("\n[7] 多发布竞争同一盒子：只允许一个待切换镜像；较老代次被拒绝")
    r = mir.create_manifest({"devices": ["RSU-01"], "note": "gen2+patch"})
    m2, mt4 = r["manifest"], r["tasks"][0]
    check("新清单版本带补丁", "+P-0001" in m2["source"]["version"],
          m2["source"]["version"])
    mir.run_agent(mt4["task_id"], stop_after=1)
    r = mir.create_manifest({"devices": ["RSU-01"],
                             "source": {"kind": "snapshot",
                                        "snapshot_id": snap["snapshot_id"]}})
    m3, mt5 = r["manifest"], r["tasks"][0]
    check("竞争清单被拒绝：已有待切换镜像",
          mt5["status"] == "rejected"
          and mt5["rejection"]["code"] == "MIRROR_PENDING_CONFLICT",
          json.dumps(mt5["rejection"], ensure_ascii=False))
    r = mir.dispatch_manifest(m3["manifest_id"], ["RSU-01"])
    check("同一清单重试 → 返回首次（拒绝）判定",
          r["tasks"][0]["task_id"] == mt5["task_id"] and r["tasks"][0]["idempotent"])
    r = mir.dispatch_manifest(m3["manifest_id"], ["RSU-06"])
    mt6 = r["tasks"][0]
    check("同一清单可分发到无竞争的盒子", mt6["status"] == "pulling")
    mir.run_agent(mt6["task_id"])
    sw = mir.switch_task(mt6["task_id"])
    check("行程快照来源清单可独立切换",
          sw["switched"] and mir.get_device("RSU-06")["current_image"]["version"]
          == snap["version"]["version"])
    mir.run_agent(mt4["task_id"])
    mir.switch_task(mt4["task_id"])
    check("RSU-01 切换到 gen2+patch",
          mir.get_device("RSU-01")["current_image"]["version"]
          == m2["source"]["version"])
    r = mir.create_manifest({"devices": ["RSU-07"], "note": "gen2 重发"})
    m2b = r["manifest"]
    r = mir.dispatch_manifest(m2b["manifest_id"], ["RSU-01"])
    check("与当前镜像同版本的清单 → 409 拒绝（不能覆盖）",
          r["tasks"][0]["status"] == "rejected"
          and r["tasks"][0]["rejection"]["code"] == "MIRROR_STALE_GENERATION")
    r = mir.dispatch_manifest("MF-0002", ["RSU-01"])
    check("较老代次清单 → 拒绝（不能覆盖当前镜像）",
          r["tasks"][0]["status"] == "rejected"
          and r["tasks"][0]["rejection"]["code"] == "MIRROR_STALE_GENERATION")
    check("当前镜像未被较老清单覆盖",
          mir.get_device("RSU-01")["current_image"]["manifest_id"]
          == m2["manifest_id"])
    r = mir.dispatch_manifest("MF-0001", ["RSU-01"])
    check("重试已切换清单 → 返回首次任务与切换结果",
          r["tasks"][0]["task_id"] == "MT-0001" and r["tasks"][0]["idempotent"]
          and r["tasks"][0]["status"] == "active"
          and r["tasks"][0]["result"]["outcome"] == "switched")

    print("\n[8] 坏对象不能覆盖当前镜像（仓库对象损坏 → 任务失败）")
    r = mir.dispatch_manifest(m2["manifest_id"], ["RSU-02"])
    mt8 = r["tasks"][0]
    check("RSU-02 接受 gen2 任务", mt8["status"] == "pulling")
    key = f"{m2['manifest_hash']}:1"
    blob = base64.b64decode(mir.state["chunk_store"][key]["data_b64"])
    mir.state["chunk_store"][key]["data_b64"] = base64.b64encode(
        blob[:-1] + bytes([blob[-1] ^ 0xFF])).decode()
    mir._persist_locked()
    r = mir.run_agent(mt8["task_id"])
    t = r["task"]
    check("损坏对象块被识别，任务失败",
          t["status"] == "failed"
          and t["rejection"]["code"] == "MIRROR_OBJECT_CORRUPT",
          json.dumps(t["rejection"], ensure_ascii=False))
    check("坏对象未覆盖当前镜像",
          mir.get_device("RSU-02")["current_image"]["manifest_id"] == "MF-0001")
    check("失败原因留在任务处理记录中",
          any(e["event"] == "TASK_FAILED" for e in t["journal"]))
    expect_err("失败任务不再接受确认 → 409 MIRROR_TASK_CLOSED",
               lambda: mir.ack_chunk(mt8["task_id"], 2, "sha256:" + "0" * 64),
               "MIRROR_TASK_CLOSED", 409)

    print("\n[9] 传输损坏可重试（仓库对象完好）")
    p2 = eng.create_patch({"scope": {"tiles": {"A00": ["lcn"]}},
                           "reason": "占道施工", "priority": 100})
    eng.activate_patch(p2["patch_id"])
    fp2 = live_fp(eng)
    r = mir.create_manifest({"devices": ["RSU-02"], "note": "双补丁"})
    mt9 = r["tasks"][0]
    r = mir.run_agent(mt9["task_id"], corrupt_chunk=1)
    check("传输损坏 → 可重试拒绝，任务保持拉取中",
          r["results"][-1]["accepted"] is False
          and r["results"][-1]["rejection"]["code"] == "MIRROR_CHUNK_HASH_MISMATCH"
          and r["task"]["status"] == "pulling")
    r = mir.run_agent(mt9["task_id"])
    check("重拉后全部验真 → 可切换", r["task"]["status"] == "ready")
    sw = mir.switch_task(mt9["task_id"])
    check("切换成功，版本含两个补丁",
          sw["switched"] and "+P-0001" in sw["result"]["new_image"]["version"]
          and "P-0002" in sw["result"]["new_image"]["version"])

    print("\n[10] 重启后可辨别：拉取中 / 可切换 / 已切换 / 每次处理结果")
    mir2 = MirrorEngine(MIR, eng)
    ts = {t["task_id"]: t for t in mir2.list_tasks()}
    check("已切换任务可辨别", ts["MT-0001"]["status"] == "active"
          and ts["MT-0002"]["status"] == "active")
    check("拉取中任务可辨别（RSU-03/RSU-07 从未拉取）",
          ts["MT-0003"]["status"] == "pulling")
    check("已拒绝任务及理由可辨别",
          ts[mt5["task_id"]]["status"] == "rejected"
          and ts[mt5["task_id"]]["rejection"]["code"] == "MIRROR_PENDING_CONFLICT")
    check("已失败任务及理由可辨别",
          ts[mt8["task_id"]]["status"] == "failed"
          and ts[mt8["task_id"]]["rejection"]["code"] == "MIRROR_OBJECT_CORRUPT")
    check("切换结果随任务恢复",
          ts["MT-0001"]["result"]["outcome"] == "switched"
          and ts["MT-0001"]["result"]["new_image"]["version"] == "north=1/south=1")
    check("处理记录（journal）随任务恢复",
          len(ts["MT-0001"]["journal"]) >= 4)
    check("盒子当前镜像随重启恢复",
          mir2.get_device("RSU-01")["current_image"]["manifest_id"]
          == m2["manifest_id"])
    check("重启后重复切换仍返回首次结果",
          mir2.switch_task("MT-0001")["idempotent"] is True)

    print("\n[11] 隔离保证：镜像子系统绝不改动地图中心状态与行程快照")
    check("线上状态与最后一次刻意操作后完全一致", live_fp(eng) == fp2)
    check("镜像存储独立存在", os.path.exists(MIR) and os.path.exists(LIVE))
    mir_data = json.load(open(MIR, encoding="utf-8"))
    check("镜像存储不含线上结构",
          "tiles" not in mir_data and "snapshots" not in mir_data
          and "region_gen" not in mir_data)
    live_data = json.load(open(LIVE, encoding="utf-8"))
    check("线上存储不含镜像结构",
          "manifests" not in live_data and "chunk_store" not in live_data
          and "tasks" not in live_data)
    check("线上事件日志无镜像事件",
          not any(e["type"].startswith(("MIRROR", "TASK", "CHUNK", "MANIFEST"))
                  for e in eng.state["events"]))

    print("\n[12] CLI 冒烟：publish / pull / switch / 幂等 / 同一数据源")
    cli = os.path.join(ROOT, "mirror_cli.py")

    def run_cli(*args):
        return subprocess.run([sys.executable, cli, *args],
                              capture_output=True, text=True, cwd=ROOT)

    r = run_cli("publish", "RSU-09")
    check("CLI publish 生成清单", r.returncode == 0 and "MF-" in r.stderr,
          r.stderr.strip())
    mf_new = json.loads(r.stdout)["manifest"]["manifest_id"]
    tid_new = json.loads(r.stdout)["tasks"][0]["task_id"]
    r = run_cli("pull", tid_new, "--stop-after", "1")
    check("CLI 断网续拉提示", r.returncode == 0 and "网络中断" in r.stdout,
          r.stdout.strip())
    r = run_cli("pull", tid_new)
    check("CLI 续拉至可切换", r.returncode == 0 and "可执行 switch" in r.stdout,
          r.stdout.strip())
    r = run_cli("switch", tid_new)
    check("CLI 原子切换", r.returncode == 0
          and json.loads(r.stdout)["switched"] is True, r.stderr.strip())
    r = run_cli("switch", tid_new)
    check("CLI 重复切换返回首次结果",
          json.loads(r.stdout)["idempotent"] is True)
    r = run_cli("publish", "RSU-09")
    check("CLI 重复发布返回首次清单",
          json.loads(r.stdout)["manifest"]["manifest_id"] == mf_new
          and json.loads(r.stdout)["idempotent"] is True)
    r = run_cli("tasks", "RSU-01")
    check("CLI 与网页同一数据源（可见同样的拒绝理由）",
          "MIRROR_STALE_GENERATION" in r.stdout, r.stdout.strip())
    r = run_cli("devices")
    check("CLI 盒子视图含当前镜像与版本",
          "RSU-01" in r.stdout and "+P-0001" in r.stdout)
    r = run_cli("pull", "MT-0003", "--corrupt", "0")
    check("CLI 模拟传输损坏被记录", "MIRROR_CHUNK_HASH_MISMATCH" in r.stdout)
    r = run_cli("pull", "MT-0003")
    check("CLI 重拉恢复", "可执行 switch" in r.stdout)

    print()
    if fails:
        print(f"✗ {len(fails)} 项失败: {fails}")
        return 1
    print("✓ 路侧盒镜像仓库全部断言通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
