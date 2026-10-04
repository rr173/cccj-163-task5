#!/usr/bin/env bash
# Roadside-box map mirror demo (no server needed). Safe to re-run.
set -euo pipefail
cd "$(dirname "$0")"
rm -rf data
PY="python3 mirror_cli.py"
LIVE="python3 route_replay.py"
say() { printf '\n\033[1;36m▶ %s\033[0m\n' "$*"; }
J() { python3 -c "$1"; }

say "1. 从当前生效版本（north=1/south=1）生成不可变清单并分发"
$PY publish RSU-01,RSU-02 "gen1 基线" 2>&1 >/dev/null | sed 's/^/  /'
$PY publish RSU-09 "gen1 备份" 2>&1 >/dev/null | grep '✓' | sed 's/^/  /'

say "2. 重试同一清单 → 返回首次清单与首次任务（幂等）"
$PY publish RSU-01,RSU-02 "gen1 基线" 2>&1 >/dev/null | sed 's/^/  /'

say "3. RSU-01 拉取：拉 1 块后断网 → 续拉（已确认块不重复计数）→ 可切换"
$PY pull MT-0001 --stop-after 1 | sed 's/^/  /'
$PY pull MT-0001 | sed 's/^/  /'

say "4. 原子换指针；重复切换返回首次结果"
$PY switch MT-0001 2>&1 | grep '✓' | sed 's/^/  /'
$PY switch MT-0001 2>&1 | grep '✓' | sed 's/^/  /'

say "5. 传输损坏可重试（RSU-02 第 1 块被翻转 → 拒绝理由落库 → 重拉恢复）"
$PY pull MT-0002 --corrupt 1 | sed 's/^/  /'
$PY pull MT-0002 | tail -3 | sed 's/^/  /'
$PY switch MT-0002 2>&1 | grep '✓' | sed 's/^/  /'

say "6. 线上演进：发布 RC-0002（north→2）并激活封路补丁"
$LIVE validate RC-0002 >/dev/null
$LIVE publish RC-0002 >/dev/null
$LIVE patch A00 lce "K12 事故管制" 100 >/dev/null
$LIVE activate P-0001 >/dev/null
$LIVE status | python3 -c 'import json,sys;print("  当前版本:",json.load(sys.stdin)["current_version"])'

say "7. 多发布竞争同一盒子：只允许一个待切换镜像"
$PY publish RSU-01 "gen2+patch" 2>&1 >/dev/null | sed 's/^/  /'
$PY pull MT-0004 --stop-after 1 | grep -E '⛔|状态' | sed 's/^/  /'
$PY publish RSU-01 "gen2 竞争清单" 2>&1 >/dev/null | sed 's/^/  /'

say "8. 完成切换后：较老代次不能覆盖当前镜像；重试同一清单返回首次结果"
$PY pull MT-0004 >/dev/null
$PY switch MT-0004 2>&1 | grep '✓' | sed 's/^/  /'
$PY dispatch MF-0002 RSU-01 | python3 -c 'import json,sys
t=json.load(sys.stdin)["tasks"][0]
print("  dispatch gen1 备份 → RSU-01:", t["status"], "["+t["rejection"]["code"]+"]", t["rejection"]["message"] if t["rejection"] else "")'
$PY dispatch MF-0001 RSU-01 | python3 -c 'import json,sys
t=json.load(sys.stdin)["tasks"][0]
print("  重试 MF-0001 → RSU-01: 返回首次任务", t["task_id"], t["status"], "幂等:", t["idempotent"], "· 切换结果:", t["result"]["outcome"])'

say "9. 同一数据源：任务进度与拒绝理由（网页与此 CLI 均读 data/mirror.json）"
$PY tasks | sed 's/^/  /'
echo
$PY devices | sed 's/^/  /'

say "完成。界面演示: python3 server.py 8080 → 📦 镜像分发 标签页"
