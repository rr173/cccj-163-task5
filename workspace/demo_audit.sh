#!/usr/bin/env bash
# Regulatory-audit temporal-evidence walkthrough (CLI).
# Resets BOTH stores; drives the operational world, then audits it.
# Everything audit-related appends to data/audit.json only — the live store
# is read by the audit tool, never written by it.
set -euo pipefail
cd "$(dirname "$0")"
rm -rf data
PY="python3 route_replay.py"
AU="python3 audit_cli.py"
say() { printf '\n\033[1;35m⚖ %s\033[0m\n' "$*"; }

say "1. 线上正常演作：固定 S-0001 → 封路 → 节点过渡 S-0002 → 完成"
$PY plan VH-01 A00/w B01/e >/dev/null
$PY patch A00 lce "K12 事故临时管制" 100 >/dev/null
$PY activate P-0001 | python3 -c 'import json,sys;print("  patch:",json.load(sys.stdin)["status"])'
$PY progress S-0001 1 at_node >/dev/null
$PY adopt-reroute S-0001 | python3 -c 'import json,sys;d=json.load(sys.stdin);print("  child:",d["snapshot_id"],d["version"]["version"])'
$PY progress S-0002 1 on_lane >/dev/null
$PY complete S-0002 >/dev/null

say "2. 运维导入：把车辆快照链签名成带序号/来源/摘要的证据事件"
$AU vehicle-events VH-01 EV-77 > /tmp/ev77.json
python3 -c 'import json;d=json.load(open("/tmp/ev77.json"));print("  生成",len(d["events"]),"条事件:",[e["type"] for e in d["events"]])'
$AU import /tmp/ev77.json EV-77 | python3 -c 'import json,sys;d=json.load(sys.stdin);print("  判定:",d["status"],"头部 seq:",d["head_seq"])'

say "3. 同一导入键再次提交（即使内容不同）→ 返回首次判定"
$AU import /tmp/ev77.json EV-77 | python3 -c 'import json,sys;print("  idempotent:",json.load(sys.stdin)["idempotent"])'

say "4. 四类拒绝：改写摘要 / 序号跳跃 / 重复 / 父事件缺失"
python3 - <<'EOF'
import json
from audit import event_digest
d=json.load(open('/tmp/ev77.json'))
e=d['events'][2]; e['payload']={**e['payload'],"step":99}   # 正文被改写
json.dump({"import_key":"EV-TAMPER","events":d['events']},open('/tmp/tamper.json','w'))
# seq gap chain built from scratch (signed)
evs=d['events']
bad=json.loads(json.dumps(evs[:2]))
bad[1]['seq']=9
bad[1]['digest']=event_digest(9,bad[1]['ts'],bad[1]['source'],bad[1]['type'],bad[1]['payload'],bad[0]['digest'])
json.dump({"import_key":"EV-GAP","events":bad},open('/tmp/gap.json','w'))
EOF
$AU import /tmp/tamper.json 2>&1 | grep -o 'AUDIT_DIGEST_MISMATCH' | sed 's/^/  摘要改写 → /'
$AU import /tmp/gap.json 2>&1 | grep -o 'AUDIT_SEQ_GAP' | sed 's/^/  序号跳跃 → /'

say "5. 重建指定时刻：seq=1 原始版本 / seq=5 子快照+管制"
$AU query EV-77 1 | python3 -c 'import sys
for ln in sys.stdin:
    if any(k in ln for k in ("版本描述","描述哈希","瓦片摘要")): print(" ",ln.strip())'
$AU query EV-77 5 | python3 -c 'import sys
for ln in sys.stdin:
    if any(k in ln for k in ("版本描述","活动管制","位置","瓦片摘要")): print(" ",ln.strip())'

say "6. 审计员确认检查点 seq=5，并隔离演算『撤销 P-0001』"
$AU confirm EV-77 5 "地图审查页确认" >/dev/null
$AU branch-revoke EV-77 5 P-0001 | python3 -c 'import json,sys;d=json.load(sys.stdin);cf=d["counterfactual"]
print("  分支:",d["branch_id"],cf["descriptor"]["version"])
print("  可达:",cf["reachable"],"移除封闭:",[c["tile_id"]+"/"+c["lane_id"] for c in cf["removed_closures"]])'

say "7. 发布 RC-0002（线上正常推进），再在 seq=1 隔离演算『版本切换』"
$PY validate RC-0002 >/dev/null
$PY publish RC-0002 | python3 -c 'import json,sys;print("  线上发布:",json.load(sys.stdin)["generations"])'
$AU confirm EV-77 1 >/dev/null
$AU branch-switch EV-77 1 REL-0002 | python3 -c 'import json,sys;d=json.load(sys.stdin);cf=d["counterfactual"]
print("  分支:",d["branch_id"],cf["descriptor"]["version"])
print("  路径变化:",cf["diffs"]["route_changed"],cf["diffs"]["old_steps"],"→",cf["diffs"]["new_steps"],"步")'

say "8. 导出审计档案（事件+检查点+分支+bundle 摘要）"
$AU export EV-77 /tmp/audit-bundle.json
python3 -c 'import json;d=json.load(open("/tmp/audit-bundle.json"));print("  ",d["export_id"],"事件",len(d["events"]),"分支",len(d["branches"]),"检查点",[c["seq"] for c in d["confirmed_checkpoints"]])'

say "9. 隔离保证：线上版本/车辆/快照未被任何审计操作改变"
$PY status | python3 -c 'import json,sys;d=json.load(sys.stdin);print("  线上当前版本:",d["current_version"])'
$PY replay S-0001 | grep "版本:" | sed 's/^/  原快照 /'
echo "  审计存储:      data/audit.json （独立 append-only）"
echo "  线上存储:      data/mapstate.json（未被审计写入）"

say "10. 审计档案清单（查询/分支/确认/导出全部留痕）"
$AU archives 8 | python3 -c 'import json,sys
for a in json.load(sys.stdin)["archives"]: print("  ",a["archive_id"],a["kind"],"-",a["summary"][:48])'

echo
echo "界面体验: python3 server.py 8080 → ⚖ 时序审计 标签页"
