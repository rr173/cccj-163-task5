#!/usr/bin/env bash
# End-to-end guided demo against the local store (no server needed).
# Safe to re-run: resets data/ first.
set -euo pipefail
cd "$(dirname "$0")"
rm -rf data
PY="python3 route_replay.py"
say() { printf '\n\033[1;36m▶ %s\033[0m\n' "$*"; }

say "1. 初始状态（north=1/south=1）"
$PY status | python3 -c 'import json,sys;d=json.load(sys.stdin);print(" ",d["current_version"])'

say "2. 校验坏候选 RC-0003（接缝限速 80 + 删除西入口车道）"
$PY validate RC-0003 | python3 -c 'import json,sys
d=json.load(sys.stdin)
print("  status:", d["status"])
[print("  -",e["code"],"@",e["where"],":",e["message"]) for e in d["errors"]]'

say "3. 发布坏候选 -> 422，区域代际不变"
$PY publish RC-0003 2>&1 | sed 's/^/  /' || true

say "4. 校验并发布好候选 RC-0002（内部新增车道，边界不变）-> north 第2代"
$PY validate RC-0002 | python3 -c 'import json,sys;print("  validate:",json.load(sys.stdin)["status"])'
$PY publish RC-0002 | python3 -c 'import json,sys;print("  published:",json.load(sys.stdin)["generations"])'

say "5. 车辆 VH-01 请求 A00/w -> B01/e，固定快照 S-0001"
$PY plan VH-01 A00/w B01/e | python3 -c 'import json,sys;d=json.load(sys.stdin);print(" ",d["snapshot_id"],d["version"]["version"],len(d["tiles"]),"tiles")'

say "6. 紧急封闭 A00/lce（自动验证兼容性与绕行）并激活"
$PY patch A00 lce "K12 事故临时管制" 100 | python3 -c 'import json,sys;d=json.load(sys.stdin);print(" ",d["patch_id"],d["compatibility"]["status"]);print("  detour:",[s.get("lane","seam") for s in d["detour"]["A00/lce"]])'
$PY activate P-0001 | python3 -c 'import json,sys;print(" ",json.load(sys.stdin)["status"])'

say "7. 快照预警：途中出现新封路"
$PY advisory S-0001 | python3 -c 'import json,sys;d=json.load(sys.stdin);[print("  -",c["message"]) for c in d["closures_ahead"]]'

say "8. 在车道上禁止过渡；到达节点后采用绕行子快照"
$PY progress S-0001 1 on_lane >/dev/null
$PY adopt-reroute S-0001 2>&1 | sed 's/^/  /' || true
$PY progress S-0001 1 at_node >/dev/null
$PY adopt-reroute S-0001 | python3 -c 'import json,sys;d=json.load(sys.stdin);print("  child:",d["snapshot_id"],d["version"]["version"]);print("  route:",[s.get("lane","seam") for s in d["route"]])'

say "9. 撤销补丁；历史子快照仍按封路版本可复算"
$PY revoke P-0001 "管制解除" | python3 -c 'import json,sys;print(" ",json.load(sys.stdin)["status"])'

say "10. 确定性复现原始快照"
$PY replay S-0001 | sed -n '1,9p' | sed 's/^/  /'

say "11. 离线车辆 VH-02 重连追赶（从事件 0 回放）"
$PY plan VH-02 A01/n B00/s >/dev/null
$PY sync VH-02 0 | python3 -c 'import json,sys;d=json.load(sys.stdin);print("  replayed:",len(d["missed_events"]),"events; current:",d["current_version"]["version"])'

say "完成。启动界面: python3 server.py 8080 -> http://localhost:8080"
