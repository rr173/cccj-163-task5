#!/usr/bin/env python3
"""End-to-end verification of the vehicle-group canary rollout feature.

Resets the store to a seeded world, then drives the engine/CLI directly.
Asserts every requirement from the rollout brief:

  * batch created only from a validation-passed candidate (regions/list/window)
  * listed vehicles' new trips pin the pending-promotion content; others keep
    reading the currently published content
  * per-group stats: requests / unreachable / safe-transition rejections
  * one in-progress batch per region (conflict -> 409)
  * one-shot promote/rollback by threshold; repeats return the first result
  * rollback never rewrites issued snapshots nor regresses region generations
  * batches + stats survive a service restart
  * a failed operation leaves no partial region switch
"""
import json
import os
import shutil
import subprocess
import sys

import engine
from engine import DomainError, MapEngine

ROOT = os.path.dirname(os.path.abspath(__file__))
LIVE = os.path.join(ROOT, "data", "mapstate.json")

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


def mk_tile(base_tile, tag, extra_lane):
    """Clone a seeded tile with one extra internal lane (boundary unchanged)."""
    t = json.loads(json.dumps(base_tile))
    t["version_tag"] = tag
    t["lanes"] = [ln for ln in t["lanes"] if ln["lane_id"] != extra_lane["lane_id"]]
    t["lanes"].append(extra_lane)
    return t


def main():
    shutil.rmtree(os.path.join(ROOT, "data"), ignore_errors=True)
    eng = MapEngine(LIVE)

    print("\n[0] 准备：校验通过好候选 RC-0002（north），坏候选 RC-0003 不校验")
    v = eng.validate_candidate("RC-0002")
    check("RC-0002 校验通过", v["status"] == "passed")
    cand2 = eng.state["candidates"]["RC-0002"]
    a00_v2 = cand2["tiles"]["A00"]
    a00_v1 = eng.state["region_heads"] and next(
        h for tid, h in eng.state["releases"][0]["tiles"].items() if tid == "A00")
    check("A00 候选内容与已发布内容不同", a00_v2 != a00_v1)

    print("\n[1] 创建批次的前置闸门")
    expect_err("未校验候选 → 422 ROLLOUT_CANDIDATE_NOT_VALIDATED",
               lambda: eng.create_rollout({"candidate_id": "RC-0003",
                                           "regions": ["north"],
                                           "vehicles": ["VH-01"]}),
               "ROLLOUT_CANDIDATE_NOT_VALIDATED", 422)
    expect_err("作用区域超出候选区域 → 400 REGION_MISMATCH",
               lambda: eng.create_rollout({"candidate_id": "RC-0002",
                                           "regions": ["south"],
                                           "vehicles": ["VH-01"]}),
               "REGION_MISMATCH", 400)
    expect_err("空车辆名单 → 400",
               lambda: eng.create_rollout({"candidate_id": "RC-0002",
                                           "regions": ["north"], "vehicles": []}),
               "BAD_REQUEST", 400)
    expect_err("观察窗口 end 早于 start → 400",
               lambda: eng.create_rollout({
                   "candidate_id": "RC-0002", "regions": ["north"],
                   "vehicles": ["VH-01"],
                   "window": {"start": "2026-10-02T00:00:00+00:00",
                              "end": "2026-10-01T00:00:00+00:00"}}),
               "BAD_REQUEST", 400)

    b1 = eng.create_rollout({
        "candidate_id": "RC-0002", "regions": ["north"],
        "vehicles": ["VH-01"], "window": {},
        "thresholds": {"max_unreachable": 0, "max_transition_rejected": 0}})
    check("批次已创建 RO-0001", b1["rollout_id"] == "RO-0001", b1["rollout_id"])
    check("初始统计为零", b1["stats"] == {
        "trial": {"requests": 0, "unreachable": 0, "transition_rejected": 0},
        "control": {"requests": 0, "unreachable": 0, "transition_rejected": 0}})
    check("待晋级版本预览带 ~RO-0001", "~RO-0001" in (b1["trial_version_preview"] or ""))

    print("\n[2] 区域冲突：任一区域已有进行中批次 → 409；不冲突区域可并行")
    expect_err("同区域第二个批次 → 409 ROLLOUT_REGION_CONFLICT",
               lambda: eng.create_rollout({"candidate_id": "RC-0002",
                                           "regions": ["north"],
                                           "vehicles": ["VH-02"]}),
               "ROLLOUT_REGION_CONFLICT", 409)
    # disjoint region (south) can run in parallel
    b00_v1 = next(h for tid, h in eng.state["releases"][0]["tiles"].items()
                  if tid == "B00")
    b00_v2_tile = mk_tile(eng.state["tiles"][b00_v1], "v2",
                          {"lane_id": "lwn", "from": "w", "to": "n",
                           "two_way": True, "speed": 50})
    r = eng.put_tile(b00_v2_tile)
    b00_v2 = r["hash"]
    b01_v1 = next(h for tid, h in eng.state["releases"][0]["tiles"].items()
                  if tid == "B01")
    cs = eng.create_candidate({"regions": ["south"],
                               "tiles": [{"tile_id": "B00", "hash": b00_v2},
                                         {"tile_id": "B01", "hash": b01_v1}],
                               "note": "南区内部联络车道"})
    eng.validate_candidate(cs["candidate_id"])
    b2 = eng.create_rollout({"candidate_id": cs["candidate_id"],
                             "regions": ["south"], "vehicles": ["VH-09"]})
    check("不冲突区域并行批次 RO-0002", b2["rollout_id"] == "RO-0002")

    print("\n[3] 分组分流：名单内读待晋级内容，名单外读当前内容")
    s_trial = eng.create_route({"vehicle_id": "VH-01",
                                "from_node": "A00/w", "to_node": "B01/e"})
    s_ctrl = eng.create_route({"vehicle_id": "VH-02",
                               "from_node": "A00/w", "to_node": "B01/e"})
    check("名单内快照版本带 ~RO-0001", "~RO-0001" in s_trial["version"]["version"],
          s_trial["version"]["version"])
    check("名单内固定 A00 候选内容", s_trial["tiles"]["A00"] == a00_v2)
    check("名单外版本无试运行标记", "~RO" not in s_ctrl["version"]["version"],
          s_ctrl["version"]["version"])
    check("名单外固定已发布内容", s_ctrl["tiles"]["A00"] == a00_v1)
    check("试运行快照携带分组标签",
          s_trial["rollouts"] == [{"rollout_id": "RO-0001", "group": "trial"},
                                  {"rollout_id": "RO-0002", "group": "control"}],
          json.dumps(s_trial["rollouts"]))
    b1 = eng.get_rollout("RO-0001")
    check("RO-0001 试运行组请求量=1", b1["stats"]["trial"]["requests"] == 1)
    check("RO-0001 对照组请求量=1", b1["stats"]["control"]["requests"] == 1)
    b2 = eng.get_rollout("RO-0002")
    check("RO-0002 对照组请求量=2（两车均不在其名单）",
          b2["stats"]["control"]["requests"] == 2)

    print("\n[4] 不可达计数（请求失败也落统计）")
    expect_err("名单内不可达 → 422 ROUTE_IMPOSSIBLE",
               lambda: eng.create_route({"vehicle_id": "VH-01",
                                         "from_node": "A00/w", "to_node": "A00/zz"}),
               "ROUTE_IMPOSSIBLE", 422)
    expect_err("名单外不可达 → 422",
               lambda: eng.create_route({"vehicle_id": "VH-02",
                                         "from_node": "A00/zz", "to_node": "B01/e"}),
               "ROUTE_IMPOSSIBLE", 422)
    b1 = eng.get_rollout("RO-0001")
    check("试运行组不可达=1", b1["stats"]["trial"]["unreachable"] == 1)
    check("对照组不可达=1", b1["stats"]["control"]["unreachable"] == 1)
    check("失败请求也计入请求量", b1["stats"]["trial"]["requests"] == 2)

    print("\n[5] 安全过渡拒绝计数")
    p = eng.create_patch({"scope": {"tiles": {"A00": ["lce"]}},
                          "reason": "K12 事故管制", "priority": 100})
    eng.activate_patch(p["patch_id"])
    eng.update_progress(s_trial["snapshot_id"], {"step": 1, "state": "on_lane"})
    expect_err("车道上过渡 → 409 NOT_AT_NODE",
               lambda: eng.adopt_reroute(s_trial["snapshot_id"]),
               "NOT_AT_NODE", 409)
    b1 = eng.get_rollout("RO-0001")
    check("试运行组安全过渡拒绝=1", b1["stats"]["trial"]["transition_rejected"] == 1)
    b2 = eng.get_rollout("RO-0002")
    check("RO-0002 对照组安全过渡拒绝=1（快照携带其分组标签）",
          b2["stats"]["control"]["transition_rejected"] == 1)
    eng.rollback_rollout("RO-0002", "并行观察结束")

    print("\n[6] 阈值闸门：越界禁止晋级；撤回幂等且首次结果可回放")
    b1 = eng.get_rollout("RO-0001")
    check("阈值越界可见", len(b1["breaches"]) == 2 and not b1["promotable"],
          json.dumps(b1["breaches"], ensure_ascii=False))
    expect_err("越界晋级 → 422 ROLLOUT_THRESHOLD_BREACH",
               lambda: eng.promote_rollout("RO-0001"),
               "ROLLOUT_THRESHOLD_BREACH", 422)
    check("越界晋级未产生任何发布", len(eng.state["releases"]) == 1)
    snap_before = eng.get_snapshot(s_trial["snapshot_id"])
    gens_before = dict(eng.state["region_gen"])
    rb = eng.rollback_rollout("RO-0001", "试运行组指标越界")
    check("撤回成功", rb["status"] == "rolled_back" and not rb["idempotent"])
    rb2 = eng.rollback_rollout("RO-0001", "重复撤回")
    check("重复撤回返回首次结果 idempotent=true",
          rb2["idempotent"] is True and rb2["decision"] == rb["decision"])
    expect_err("撤回后再晋级 → 409 ROLLOUT_ALREADY_DECIDED",
               lambda: eng.promote_rollout("RO-0001"),
               "ROLLOUT_ALREADY_DECIDED", 409)

    print("\n[7] 撤回不改写已发出快照、不倒退区域代际")
    snap_after = eng.get_snapshot(s_trial["snapshot_id"])
    check("快照版本描述未变",
          snap_after["version"] == snap_before["version"] and
          "~RO-0001" in snap_after["version"]["version"])
    check("快照路线未变", snap_after["route"] == snap_before["route"])
    check("区域代际未倒退", eng.state["region_gen"] == gens_before,
          json.dumps(eng.state["region_gen"]))
    s_back = eng.create_route({"vehicle_id": "VH-01",
                               "from_node": "A00/w", "to_node": "B01/e"})
    check("撤回后名单车辆恢复读取当前发布内容",
          "~RO" not in s_back["version"]["version"] and
          s_back["tiles"]["A00"] == a00_v1)
    eng.revoke_patch(p["patch_id"], "管制解除")

    print("\n[8] 一次性晋级：原子发布 + 幂等回放")
    b3 = eng.create_rollout({"candidate_id": "RC-0002", "regions": ["north"],
                             "vehicles": ["VH-03"]})
    check("撤回后同区域可再开批次", b3["rollout_id"] == "RO-0003")
    s3 = eng.create_route({"vehicle_id": "VH-03",
                           "from_node": "A00/w", "to_node": "B01/e"})
    check("新批次名单车辆再次读待晋级内容", "~RO-0003" in s3["version"]["version"])
    n_rel = len(eng.state["releases"])
    pr = eng.promote_rollout("RO-0003")
    check("晋级成功并发布 REL-0002",
          pr["status"] == "promoted" and pr["decision"]["release_id"] == "REL-0002",
          json.dumps(pr["decision"]))
    check("north 推进到第 2 代", eng.state["region_gen"]["north"] == 2)
    check("候选随晋级发布", eng.state["candidates"]["RC-0002"]["status"] == "published")
    pr2 = eng.promote_rollout("RO-0003")
    check("重复晋级返回首次结果",
          pr2["idempotent"] is True and pr2["decision"] == pr["decision"])
    check("重复晋级不产生新发布", len(eng.state["releases"]) == n_rel + 1)
    expect_err("晋级后再撤回 → 409",
               lambda: eng.rollback_rollout("RO-0003", "太迟了"),
               "ROLLOUT_ALREADY_DECIDED", 409)
    s5 = eng.create_route({"vehicle_id": "VH-05",
                           "from_node": "A00/w", "to_node": "B01/e"})
    check("晋级后所有车辆读取新内容",
          s5["version"]["version"] == "north=2/south=1" and
          s5["tiles"]["A00"] == a00_v2, s5["version"]["version"])

    print("\n[9] 失败操作不留部分区域切换（原子性）")
    # 9a: single-region baseline drift
    b00_v3_tile = mk_tile(eng.state["tiles"][b00_v2], "v3",
                          {"lane_id": "lws", "from": "w", "to": "s",
                           "two_way": True, "speed": 50})
    b00_v3 = eng.put_tile(b00_v3_tile)["hash"]
    cs2 = eng.create_candidate({"regions": ["south"],
                                "tiles": [{"tile_id": "B00", "hash": b00_v3},
                                          {"tile_id": "B01", "hash": b01_v1}]})
    eng.validate_candidate(cs2["candidate_id"])
    eng.create_rollout({"candidate_id": cs2["candidate_id"],
                        "regions": ["south"], "vehicles": ["VH-07"]})
    eng.publish_candidate(cs2["candidate_id"])  # ops publishes directly: south gen 2
    gens_pre = dict(eng.state["region_gen"])
    n_rel = len(eng.state["releases"])
    expect_err("基线已推进 → 晋级 409 REGION_GENERATION_CONFLICT",
               lambda: eng.promote_rollout("RO-0004"),
               "REGION_GENERATION_CONFLICT", 409)
    check("失败后区域代际纹丝不动", eng.state["region_gen"] == gens_pre)
    check("失败后无新发布", len(eng.state["releases"]) == n_rel)
    check("批次仍为进行中（可撤回）",
          eng.get_rollout("RO-0004")["status"] == "active")
    eng.rollback_rollout("RO-0004", "基线漂移，放弃")

    # 9b: multi-region promote must be all-or-nothing
    a00_v4_tile = mk_tile(eng.state["tiles"][a00_v2], "v4",
                          {"lane_id": "lws", "from": "w", "to": "s",
                           "two_way": True, "speed": 50})
    a00_v4 = eng.put_tile(a00_v4_tile)["hash"]
    a01_h = next(h for tid, h in eng.state["releases"][0]["tiles"].items()
                 if tid == "A01")
    cm = eng.create_candidate({
        "regions": ["north", "south"],
        "tiles": [{"tile_id": "A00", "hash": a00_v4},
                  {"tile_id": "A01", "hash": a01_h},
                  {"tile_id": "B00", "hash": b00_v3},
                  {"tile_id": "B01", "hash": b01_v1}]})
    eng.validate_candidate(cm["candidate_id"])
    eng.create_rollout({"candidate_id": cm["candidate_id"],
                        "regions": ["north", "south"], "vehicles": ["VH-08"]})
    # a third party moves only the north baseline under the batch's feet
    a00_v5_tile = mk_tile(eng.state["tiles"][a00_v4], "v5",
                          {"lane_id": "lne2", "from": "n", "to": "e",
                           "two_way": True, "speed": 70})
    a00_v5 = eng.put_tile(a00_v5_tile)["hash"]
    cn = eng.create_candidate({"regions": ["north"],
                               "tiles": [{"tile_id": "A00", "hash": a00_v5},
                                         {"tile_id": "A01", "hash": a01_h}]})
    eng.validate_candidate(cn["candidate_id"])
    eng.publish_candidate(cn["candidate_id"])  # north gen 3, south stays 2
    gens_pre = dict(eng.state["region_gen"])
    n_rel = len(eng.state["releases"])
    expect_err("多区域批次晋级遇单区漂移 → 409",
               lambda: eng.promote_rollout("RO-0005"),
               "REGION_GENERATION_CONFLICT", 409)
    check("south 未被部分推进", eng.state["region_gen"]["south"] == gens_pre["south"])
    check("north 保持第三方发布后的代际",
          eng.state["region_gen"]["north"] == gens_pre["north"])
    check("无半成品 release", len(eng.state["releases"]) == n_rel)
    eng.rollback_rollout("RO-0005", "重新基线后再试")

    print("\n[10] 服务重启：批次与统计必须恢复")
    eng2 = MapEngine(LIVE)
    ro2 = {b["rollout_id"]: b for b in eng2.list_rollouts()}
    check("重启后批次齐全",
          set(ro2) == {"RO-0001", "RO-0002", "RO-0003", "RO-0004", "RO-0005"},
          ",".join(sorted(ro2)))
    check("重启后统计恢复（RO-0001 试运行组 2 请求/1 不可达/1 拒绝）",
          ro2["RO-0001"]["stats"]["trial"] ==
          {"requests": 2, "unreachable": 1, "transition_rejected": 1})
    check("重启后判定结果恢复（RO-0003 → REL-0002）",
          ro2["RO-0003"]["decision"]["release_id"] == "REL-0002" and
          ro2["RO-0003"]["status"] == "promoted")
    rb3 = eng2.rollback_rollout("RO-0001")
    check("重启后重复撤回仍返回首次结果", rb3["idempotent"] is True)

    print("\n[11] 观察窗口：窗口结束即停止分流（批次仍待判定）")
    a00_v6_tile = mk_tile(eng.state["tiles"][a00_v5], "v6",
                          {"lane_id": "lwe", "from": "w", "to": "e",
                           "two_way": True, "speed": 50})
    a00_v6 = eng.put_tile(a00_v6_tile)["hash"]
    cw = eng.create_candidate({"regions": ["north"],
                               "tiles": [{"tile_id": "A00", "hash": a00_v6},
                                         {"tile_id": "A01", "hash": a01_h}]})
    eng.validate_candidate(cw["candidate_id"])
    bw = eng.create_rollout({
        "candidate_id": cw["candidate_id"], "regions": ["north"],
        "vehicles": ["VH-08"],
        "window": {"start": "2026-10-01T00:00:00+00:00",
                   "end": "2026-10-01T01:00:00+00:00"}})
    check("窗口已结束的批次 window_open=false", bw["window_open"] is False)
    sw = eng.create_route({"vehicle_id": "VH-08",
                           "from_node": "A00/w", "to_node": "B01/e"})
    check("窗口外名单车辆读当前发布内容", "~RO" not in sw["version"]["version"])
    check("窗口外不计统计",
          eng.get_rollout(bw["rollout_id"])["stats"]["trial"]["requests"] == 0)
    expect_err("窗口结束仍占用区域（进行中）→ 409",
               lambda: eng.create_rollout({"candidate_id": cw["candidate_id"],
                                           "regions": ["north"],
                                           "vehicles": ["VH-12"]}),
               "ROLLOUT_REGION_CONFLICT", 409)
    eng.rollback_rollout(bw["rollout_id"], "窗口结束，撤回")

    print("\n[12] CLI 冒烟：rollout-new / rollouts / promote 幂等 / rollback 冲突")
    cli = os.path.join(ROOT, "route_replay.py")
    cw_id = cw["candidate_id"]

    def run_cli(*args):
        return subprocess.run([sys.executable, cli, *args],
                              capture_output=True, text=True, cwd=ROOT)

    r = run_cli("rollout-new", cw_id, "north", "VH-11", "60", "0", "0")
    check("CLI rollout-new 创建 RO-0007",
          r.returncode == 0 and '"RO-0007"' in r.stdout, r.stderr.strip())
    r = run_cli("rollouts")
    check("CLI rollouts 展示分组统计",
          r.returncode == 0 and "试运行组" in r.stdout and "对照组" in r.stdout)
    r = run_cli("rollout-promote", "RO-0007")
    check("CLI 晋级成功", r.returncode == 0 and "REL-" in r.stderr, r.stderr.strip())
    r = run_cli("rollout-promote", "RO-0007")
    check("CLI 重复晋级返回首次结果",
          r.returncode == 0 and "重复操作，返回首次结果" in r.stderr
          and '"idempotent": true' in r.stdout)
    r = run_cli("rollout-rollback", "RO-0007", "太晚")
    check("CLI 晋级后撤回 → 409",
          r.returncode == 1 and "ROLLOUT_ALREADY_DECIDED" in r.stderr)
    check("CLI 晋级后代际生效",
          json.loads(run_cli("status").stdout)["current_version"].startswith("north=4"))

    print()
    if fails:
        print(f"✗ {len(fails)} 项失败: {fails}")
        return 1
    print("✓ 分组试运行全部断言通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
