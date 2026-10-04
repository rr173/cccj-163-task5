#!/usr/bin/env python3
"""End-to-end verification of the regulatory-audit temporal-evidence feature.

Resets BOTH stores to a seeded world, then drives the audit CLI/domain
directly. Asserts every requirement from the audit brief.
"""
import json
import os
import shutil
import subprocess
import sys

import engine
from audit import AuditEngine, event_digest, descriptor_hash, minimal_patch

ROOT = os.path.dirname(os.path.abspath(__file__))
LIVE = os.path.join(ROOT, "data", "mapstate.json")
AUD = os.path.join(ROOT, "data", "audit.json")

fails = []


def check(name, cond, detail=""):
    print(("  ✓ " if cond else "  ✗ FAIL: ") + name + (f"  [{detail}]" if detail and not cond else ""))
    if not cond:
        fails.append(name)


def main():
    shutil.rmtree(os.path.join(ROOT, "data"), ignore_errors=True)
    eng = engine.MapEngine(LIVE)
    au = AuditEngine(AUD, eng)

    print("\n[0] 准备线上世界：先固定快照，再封路，节点处过渡，完成行程")
    s1 = eng.create_route({"vehicle_id": "VH-77", "from_node": "A00/w",
                           "to_node": "B01/e"})
    pin1 = s1["snapshot_id"]
    check("固定初始快照", pin1 == "S-0001", pin1)
    p = eng.create_patch({"scope": {"tiles": {"A00": ["lce"]}},
                          "reason": "K12 事故管制", "priority": 100})
    eng.activate_patch(p["patch_id"])
    # on_lane transition must be refused (reuse engine semantics indirectly)
    eng.update_progress(pin1, {"step": 1, "state": "at_node"})
    s2 = eng.adopt_reroute(pin1)
    pin2 = s2["snapshot_id"]
    check("节点处生成子快照", pin2 == "S-0002" and s2["version"]["version"].endswith("+P-0001[p100]"),
          s2["version"]["version"])
    eng.update_progress(pin2, {"step": 1, "state": "on_lane"})
    eng.complete_snapshot(pin2)

    live_gen_before = dict(eng.state["region_gen"])
    live_events_before = len(eng.state["events"])
    live_veh_before = json.dumps(eng.state["vehicles"], sort_keys=True)
    live_heads_before = json.dumps(eng.state["region_heads"], sort_keys=True)
    live_snaps_before = json.dumps(eng.state["snapshots"], sort_keys=True)
    live_chain_before = json.dumps(s1["route"], sort_keys=True)

    print("\n[1] 运维导入：从线上车辆快照链签名生成证据事件")
    bundle = au.build_vehicle_events({"vehicle_id": "VH-77", "import_key": "EV-77"})
    evs = bundle["events"]
    types = [e["type"] for e in evs]
    check("事件类型序列 pin/progress/progress/reroute/progress/completed",
          types == ["route_pinned", "progress", "progress", "reroute_adopted",
                    "progress", "trip_completed"], str(types))
    check("证据来源带 source", all(e["source"].startswith("ops-import/") for e in evs))
    check("序号连续 1..N", [e["seq"] for e in evs] == list(range(1, len(evs) + 1)))
    check("父链成链", all(evs[i]["parent_digest"] == evs[i - 1]["digest"]
                          for i in range(1, len(evs))))

    verdict = au.import_events({"import_key": "EV-77", "events": evs,
                                "note": "VH-77 运行事件导入"})
    check("合法链导入通过", verdict["status"] == "accepted")
    check("链摘要为头部 digest", verdict["chain_digest"] == evs[-1]["digest"])

    print("\n[2] 同一导入键再次提交 → 返回首次判定（幂等）")
    v2 = au.import_events({"import_key": "EV-77", "events": evs})
    check("重复提交 idempotent=true", v2.get("idempotent") is True)
    check("重复提交链摘要不变", v2["chain_digest"] == verdict["chain_digest"])
    # even a *different* payload under the same key returns the first verdict
    v3 = au.import_events({"import_key": "EV-77", "events": evs[:2]})
    check("不同内容同键也返回首次判定", v3["event_count"] == len(evs))

    print("\n[3] 四类拒绝：序号跳跃 / 重复 / 摘要改写 / 父事件缺失")
    def chain(events):
        out = []
        prev = None
        for i, (etype, payload) in enumerate(events):
            d = event_digest(i + 1, f"2026-01-01T00:00:0{i}+00:00", "ops/x",
                             etype, payload, prev)
            out.append({"seq": i + 1, "ts": f"2026-01-01T00:00:0{i}+00:00",
                        "source": "ops/x", "type": etype, "payload": payload,
                        "parent_digest": prev, "digest": d})
            prev = d
        return out

    pin_pl = dict(evs[0]["payload"])
    prog = {"pin_id": "S-0001", "step": 1, "state": "on_lane", "vehicle_id": "VH-77"}

    # gap: re-sign seqs as 1,3
    bad = chain([("route_pinned", pin_pl), ("progress", prog)])
    bad[1]["seq"] = 3
    bad[1]["digest"] = event_digest(3, bad[1]["ts"], "ops/x", "progress", prog,
                                    bad[0]["digest"])
    r = au.import_events({"import_key": "BAD-GAP", "events": bad})
    check("序号跳跃被拒绝", r["status"] == "rejected"
          and r["rejection"]["code"] == "AUDIT_SEQ_GAP",
          r.get("rejection", {}).get("code"))

    # duplicate seq: 1,1 (second resigned to parent of first)
    dup = chain([("route_pinned", pin_pl), ("progress", prog)])
    dup[1]["seq"] = 1
    dup[1]["digest"] = event_digest(1, dup[1]["ts"], "ops/x", "progress", prog,
                                    dup[0]["digest"])
    r = au.import_events({"import_key": "BAD-DUP", "events": dup})
    check("重复事件被拒绝", r["rejection"]["code"] == "AUDIT_DUPLICATE_EVENT",
          r.get("rejection", {}).get("code"))

    # digest rewritten: mutate payload after signing
    tam = chain([("route_pinned", pin_pl), ("progress", prog)])
    tam[1]["payload"] = {**prog, "step": 5}
    r = au.import_events({"import_key": "BAD-TAMPER", "events": tam})
    check("摘要改写被拒绝", r["rejection"]["code"] == "AUDIT_DIGEST_MISMATCH",
          r.get("rejection", {}).get("code"))

    # missing parent: break parent_digest link
    miss = chain([("route_pinned", pin_pl), ("progress", prog)])
    miss[1]["parent_digest"] = "sha256:" + "0" * 64
    r = au.import_events({"import_key": "BAD-PARENT", "events": miss})
    check("父事件缺失被拒绝", r["rejection"]["code"] == "AUDIT_PARENT_MISSING",
          r.get("rejection", {}).get("code"))

    # progress referencing unknown pin
    ghost = chain([("progress", {"pin_id": "S-9999", "step": 0,
                                 "state": "at_node", "vehicle_id": "x"})])
    r = au.import_events({"import_key": "BAD-GHOST", "events": ghost})
    check("引用不存在的父路线被拒绝", r["rejection"]["code"] == "AUDIT_PARENT_MISSING",
          r.get("rejection", {}).get("code"))

    print("\n[4] 时序重建：指定时刻的版本描述 / 瓦片摘要 / 行驶步骤")
    st1 = au.query_state("EV-77", 1)
    check("seq1 采用版本无补丁", st1["version"]["version"] == "north=1/south=1",
          st1["version"]["version"])
    check("版本描述哈希复核通过", st1["version"]["descriptor_hash_ok"])
    check("全部瓦片摘要与内容库一致",
          all(t["content_hash_ok"] for t in st1["tiles"]))
    check("瓦片数量合理(≥3)", len(st1["tiles"]) >= 3, str(len(st1["tiles"])))
    check("展示行驶步骤数=路线长度", len(st1["steps"]) == len(st1["route"]))
    check("seq1 位置在起点节点", st1["position"] ==
          {"state": "at_node", "node": "A00/w", "step_index": 0})

    st3 = au.query_state("EV-77", 3)
    check("seq3 仍在原始 pin（节点处）",
          st3["pin_id"] == "S-0001" and st3["position"]["state"] == "at_node")

    st5 = au.query_state("EV-77", 5)
    check("seq5 已切换到子快照 S-0002（带 P-0001）",
          st5["pin_id"] == "S-0002" and "P-0001" in st5["version"]["patches"],
          st5["version"]["version"])
    check("seq5 覆盖层含封路", any(
        any(l["closed"] for l in lanes.values()) for lanes in st5["overlays"].values()))
    check("seq5 在车道上(on_lane)", st5["position"]["state"] == "on_lane")

    # timestamp query
    ts = evs[3]["ts"]
    stt = au.query_state("EV-77", ts=ts)
    check("按时刻查询等价于 <=ts 的最后事件", stt["at"]["event_seq"] == 4)

    print("\n[5] 篡改检测：改写已导入链中引用的瓦片摘要/描述哈希")
    # craft a chain whose tile digest does not exist in the content store
    bad_pin = json.loads(json.dumps(pin_pl))
    bad_pin["tiles"]["A00"] = "sha256:" + "f" * 64
    bad_pin["descriptor_hash"] = descriptor_hash(
        bad_pin["version"], bad_pin["base"], bad_pin["tiles"],
        bad_pin["patches"], bad_pin["overlays"])
    bx = chain([("route_pinned", bad_pin)])
    # structurally valid chain => accepted (import only checks chain integrity)
    rv = au.import_events({"import_key": "EV-BADTILE", "events": bx})
    check("结构合法即导入通过（瓦片存在性在重建时复核）", rv["status"] == "accepted")
    stx = au.query_state("EV-BADTILE", 1)
    check("重建时瓦片摘要不一致被标记",
          any(not t["content_hash_ok"] for t in stx["tiles"]))
    check("给出 tile_digest_mismatch 预警",
          any(w["level"] == "tile_digest_mismatch" for w in stx["warnings"]))
    # rewritten descriptor hash detected
    bad_pin2 = json.loads(json.dumps(pin_pl))
    bad_pin2["version"] = "north=9/south=9"
    bx2 = chain([("route_pinned", bad_pin2)])
    au.import_events({"import_key": "EV-BADDESC", "events": bx2})
    stx2 = au.query_state("EV-BADDESC", 1)
    check("版本描述被改写 → 描述哈希复核失败",
          stx2["version"]["descriptor_hash_ok"] is False
          and any(w["level"] == "descriptor_hash_mismatch" for w in stx2["warnings"]))

    print("\n[6] 已确认检查点 → 隔离分支（版本切换 / 撤销管制）")
    au.confirm_checkpoint("EV-77", 5, "审计员确认的分支点")
    cps = au.list_checkpoints("EV-77")
    check("检查点已持久化", any(c["seq"] == 5 for c in cps))

    # branch from unconfirmed seq must fail
    try:
        au.create_branch("EV-77", {"seq": 2,
                         "scenario": {"type": "revoke_closure", "patch_id": "P-0001"}})
        check("未确认检查点禁止建分支", False)
    except engine.DomainError as e:
        check("未确认检查点禁止建分支", e.code == "AUDIT_CHECKPOINT_NOT_CONFIRMED")

    # revoke closure at seq5 (child snapshot with P-0001)
    b1 = au.create_branch("EV-77", {"seq": 5,
                          "scenario": {"type": "revoke_closure", "patch_id": "P-0001"}})
    cf = b1["counterfactual"]
    check("撤销分支生成", b1["branch_id"].startswith("B-"))
    check("撤销后版本不含该补丁", "P-0001" not in cf["descriptor"]["version"])
    check("移除了 A00/lce 封闭",
          {"tile_id": "A00", "lane_id": "lce"} in cf["removed_closures"])
    check("撤销后路径仍可达", cf["reachable"] is True)
    check("撤销后覆盖层无任何封闭", not any(
        any(x.get("closed") for x in lanes.values()) for lanes in cf["overlays"].values()))

    # revoke a patch not pinned at that moment (seq1)
    au.confirm_checkpoint("EV-77", 1, "早期检查点")
    try:
        au.create_branch("EV-77", {"seq": 1,
                         "scenario": {"type": "revoke_closure", "patch_id": "P-0001"}})
        check("撤销未采用的管制被拒绝", False)
    except engine.DomainError as e:
        check("撤销未采用的管制被拒绝", e.code == "AUDIT_PATCH_NOT_PINNED")

    # version switch: publish RC-0002 (north gen2) then branch at seq1
    eng.validate_candidate("RC-0002")
    rel = eng.publish_candidate("RC-0002")
    check("发布 north 第2代（线上正常推进）",
          rel["generations"] == {"north": 2}, str(rel["generations"]))
    b2 = au.create_branch("EV-77", {"seq": 1,
                          "scenario": {"type": "version_switch",
                                       "release_id": rel["release_id"]}})
    cf2 = b2["counterfactual"]
    check("版本切换分支指向 north=2",
          cf2["descriptor"]["base"]["north"]["generation"] == 2,
          cf2["descriptor"]["version"])
    check("隔离演算只算一次（不产生第二代切换状态）",
          b2["base_seq"] == 1 and b2["scenario"]["type"] == "version_switch")
    check("切换后路径可达", cf2["reachable"] is True)

    # switch to nonexistent release
    try:
        au.create_branch("EV-77", {"seq": 1,
                         "scenario": {"type": "version_switch",
                                      "release_id": "REL-9999"}})
        check("切换到不存在的发布被拒绝", False)
    except engine.DomainError as e:
        check("切换到不存在的发布被拒绝", e.code == "NOT_FOUND")

    print("\n[7] 导出只产生新审计档案")
    bundle_out = au.create_export("EV-77")
    check("导出含完整证据事件", len(bundle_out["events"]) == len(evs))
    check("导出含两个分支结果", len(bundle_out["branches"]) == 2)
    check("导出带 bundle 摘要", bundle_out["bundle_digest"].startswith("sha256:"))
    check("导出含确认检查点", {c["seq"] for c in bundle_out["confirmed_checkpoints"]} >= {1, 5})

    print("\n[8] 长序列从持久检查点续算")
    # build a separate long chain: 1 pin + 40 progress + complete = 42
    long_pin = json.loads(json.dumps(pin_pl))
    long_pin["pin_id"] = "PIN-LONG"
    long_pin["descriptor_hash"] = descriptor_hash(
        long_pin["version"], long_pin["base"], long_pin["tiles"],
        long_pin["patches"], long_pin["overlays"])
    head_d = None
    part1 = []
    ts0 = "2026-02-01T00:00:00+00:00"
    from datetime import datetime, timedelta, timezone as tz
    t0 = datetime.fromisoformat(ts0)

    def mk(seq, etype, payload, parent, t):
        ts = t.isoformat()
        d = event_digest(seq, ts, "ops/long", etype, payload, parent)
        return {"seq": seq, "ts": ts, "source": "ops/long", "type": etype,
                "payload": payload, "parent_digest": parent, "digest": d}, d

    e1, head_d = mk(1, "route_pinned", long_pin, None, t0)
    part1.append(e1)
    N_MID = 20
    for i in range(1, N_MID + 1):
        ev, head_d = mk(i + 1, "progress",
                        {"pin_id": "PIN-LONG", "step": i if i <= len(long_pin["route"])
                         else len(long_pin["route"]),
                         "state": "on_lane" if i % 2 else "at_node",
                         "vehicle_id": "VH-LONG"},
                        head_d, t0 + timedelta(seconds=30 * i))
        part1.append(ev)
    rv = au.import_events({"import_key": "EV-LONG", "events": part1})
    check(f"首批 {len(part1)} 条导入，头部检查点 seq={N_MID + 1}",
          rv["head_seq"] == N_MID + 1, str(rv.get("head_seq")))

    part2 = []
    for i in range(N_MID + 1, 40):
        ev, head_d = mk(i + 1, "progress",
                        {"pin_id": "PIN-LONG", "step": min(i, len(long_pin["route"])),
                         "state": "at_node", "vehicle_id": "VH-LONG"},
                        head_d, t0 + timedelta(seconds=30 * i))
        part2.append(ev)
    ev, head_d = mk(41, "trip_completed",
                    {"pin_id": "PIN-LONG", "vehicle_id": "VH-LONG"},
                    head_d, t0 + timedelta(seconds=1300))
    part2.append(ev)
    cont = au.append_events("EV-LONG", part2)
    check("续算成功，头部推进到 41", cont["head_seq"] == 41, str(cont.get("head_seq")))
    stl = au.query_state("EV-LONG", 41)
    check("续算后状态为已完成", stl["pin_status"] == "completed")

    # resume rejection: bad chunk (gap) must not move the checkpoint
    bad_chunk = []
    ev, tmp = mk(43, "progress", {"pin_id": "PIN-LONG", "step": 1,
                                 "state": "at_node", "vehicle_id": "VH-LONG"},
                 head_d, t0 + timedelta(seconds=1400))
    bad_chunk.append(ev)
    rej = au.append_events("EV-LONG", bad_chunk)
    check("续算跳跃被拒且头部保持 41",
          rej["append"] == "rejected" and rej["head_seq"] == 41)
    rec = au.get_import("EV-LONG")
    check("被拒续算未污染原链", len(rec["event_digests"]) == 41)

    print("\n[9] 隔离保证：审计操作绝不改变线上代际/车辆/原事件链")
    # north=2/south=1 came ONLY from the explicit eng.publish_candidate above;
    # every audit op (import/query/branch/export/append/checkpoint) left no
    # further trace in the live store.
    heads_after_publish = json.dumps({"north": rel["release_id"],
                                      "south": "REL-0001"}, sort_keys=True)
    check("线上区域代际仅由正常发布推进 (north=2/south=1)",
          dict(eng.state["region_gen"]) == {"north": 2, "south": 1},
          str(eng.state["region_gen"]))
    check("审计前 north 代际为 1（差异来自正常发布而非审计）",
          live_gen_before == {"north": 1, "south": 1})
    check("区域 head 与正常发布一致（审计未写入 head）",
          json.dumps(eng.state["region_heads"], sort_keys=True) == heads_after_publish)
    check("车辆记录未被审计写入",
          json.dumps(eng.state["vehicles"], sort_keys=True) == live_veh_before)
    check("快照记录未被审计写入",
          json.dumps(eng.state["snapshots"], sort_keys=True) == live_snaps_before)
    check("原快照路线未被改写",
          json.dumps(eng.get_snapshot("S-0001")["route"], sort_keys=True) == live_chain_before)
    # audit API must never have produced an operational event in the live log
    live_tail = [e["type"] for e in eng.state["events"][live_events_before:]]
    check("线上事件日志中没有任何审计操作产生的事件",
          not any(t.startswith("AUDIT") for t in live_tail),
          str(live_tail))

    print("\n[10] 查询/分支/导出都留下了 append-only 审计档案")
    arcs = au.list_archives(200)
    kinds = {a["kind"] for a in arcs}
    for k in ("import_accepted", "query", "branch", "export",
              "checkpoint_confirmed", "append_accepted", "append_rejected"):
        check(f"档案类型 {k} 存在", k in kinds, str(sorted(kinds)))
    check("审计档案独立计数", all(a["archive_id"].startswith("A-") for a in arcs))

    print("\n[11] 审计档案文件与线上文件物理分离")
    check("audit.json 独立存在", os.path.exists(AUD) and os.path.exists(LIVE))
    aud_data = json.load(open(AUD, encoding="utf-8"))
    check("审计存储不含 tiles/candidates 等线上结构",
          "tiles" not in aud_data and "candidates" not in aud_data
          and "region_gen" not in aud_data)
    live_data = json.load(open(LIVE, encoding="utf-8"))
    check("线上存储不含 audit imports/branches",
          "imports" not in live_data and "branches" not in live_data)

    print()
    if fails:
        print(f"共 {len(fails)} 项失败：")
        for f in fails:
            print("  -", f)
        sys.exit(1)
    print("✅ 全部审计需求校验通过")


if __name__ == "__main__":
    main()
