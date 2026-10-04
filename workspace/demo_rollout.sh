#!/usr/bin/env bash
# Vehicle-group canary rollout demo (no server needed). Safe to re-run.
set -euo pipefail
cd "$(dirname "$0")"
rm -rf data
PY="python3 route_replay.py"
say() { printf '\n\033[1;36m▶ %s\033[0m\n' "$*"; }

say "1. 校验好候选 RC-0002（north 内部新增车道）"
$PY validate RC-0002 | python3 -c 'import json,sys;print("  validate:",json.load(sys.stdin)["status"])'

say "2. 创建试运行批次：名单 VH-01，窗口 30 分钟，阈值 不可达>0 / 过渡拒绝>0"
$PY rollout-new RC-0002 north VH-01 30 0 0 | python3 -c 'import json,sys;d=json.load(sys.stdin);print(" ",d["rollout_id"],d["status"],"→",d["trial_version_preview"])'

say "3. 同区域再开批次 → 409 冲突拒绝"
$PY rollout-new RC-0002 north VH-02 2>&1 | sed 's/^/  /' || true

say "4. 分组分流：VH-01（名单内）读待晋级内容，VH-02（名单外）读当前内容"
$PY plan VH-01 A00/w B01/e | python3 -c 'import json,sys;d=json.load(sys.stdin);print("  VH-01:",d["snapshot_id"],d["version"]["version"])'
$PY plan VH-02 A00/w B01/e | python3 -c 'import json,sys;d=json.load(sys.stdin);print("  VH-02:",d["snapshot_id"],d["version"]["version"])'

say "5. 各组统计（请求量 / 不可达 / 安全过渡拒绝）"
$PY plan VH-01 A00/w A00/zz 2>&1 | sed 's/^/  /' || true
$PY rollouts

say "6. 阈值越界 → 禁止晋级，应撤回"
$PY rollout-promote RO-0001 2>&1 | sed 's/^/  /' || true

say "7. 撤回批次（重复撤回返回首次结果）；已发快照与区域代际不变"
$PY rollout-rollback RO-0001 "试运行组不可达越界" >/dev/null
$PY rollout-rollback RO-0001 | python3 -c 'import json,sys;d=json.load(sys.stdin);print("  idempotent:",d["idempotent"],"decision:",d["decision"]["action"],"@",d["decision"]["at"])'
$PY status | python3 -c 'import json,sys;print("  当前版本:",json.load(sys.stdin)["current_version"])'

say "8. 重新开批次并一次性晋级（原子发布全部作用区域）"
$PY rollout-new RC-0002 north VH-03 >/dev/null
$PY rollout-promote RO-0002 | python3 -c 'import json,sys;d=json.load(sys.stdin);print("  晋级 →",d["decision"]["release_id"],d["decision"]["generations"])'
$PY rollout-promote RO-0002 | python3 -c 'import json,sys;d=json.load(sys.stdin);print("  重复晋级 idempotent:",d["idempotent"])'
$PY rollout-rollback RO-0002 2>&1 | sed 's/^/  /' || true
$PY status | python3 -c 'import json,sys;print("  当前版本:",json.load(sys.stdin)["current_version"])'

say "完成。界面演示: python3 server.py 8080 → 🧪 分组试运行 标签页"
