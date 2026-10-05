#!/usr/bin/env bash
# 容量感知预热编排 一键演示（与网页 data/preheat.json 同源；完全独立于其他子系统）
set -u
cd "$(dirname "$0")"
rm -rf data/preheat.json
CLI="python3 preheat_cli.py"
MB=$((1024*1024))
line() { echo; echo "──────── $1 ────────"; }
jid_of() { $CLI jobs | awk -v n="$1" '$0 ~ n {print $1; exit}'; }

line "0. 初始世界：3 站点 / 8 盒 / 5 内容对象（ST-N 已缓存 BL-MAP-BASE）"
$CLI boxes

line "1. 提交三盒批次：截止 40s、速率上限 15MiB/s、最小在线保障 2MiB/s"
# 202 MiB 装得下 N3(250)/N2(300)/N1(400，已含 120 缓存底图)
$CLI submit RSU-N1,RSU-N2,RSU-N3 BL-MAP-MEDIA,BL-MAP-POI,BL-MAP-PATCH \
  40 $((15*MB)) $((2*MB)) 1 "晚高峰预热" >/dev/null
BATCH=$(jid_of 晚高峰)
echo "批次作业：$BATCH"
$CLI queue

line "2. 推进 2 秒：站点 60MiB/s 被三盒共享，最小保障兜底 + DRR 分余量"
$CLI tick 2 >/dev/null
$CLI queue

line "3. 老作业不饿死：再向 N1 提交一个新作业，老作业持续推进"
$CLI submit RSU-N1 BL-MAP-LANE 20 $((15*MB)) $((1*MB)) 1 "插队补丁" >/dev/null
$CLI tick 2 >/dev/null
$CLI queue

line "4. 暂停 N2 副本所在作业：磁盘预留与传输名额立即成对释放（无幽灵）"
# 暂停批次作业，先看整作业视角；N2 的预留随暂停一起释放
$CLI pause "$BATCH"
$CLI boxes RSU-N2 | python3 -c "import json,sys; d=json.load(sys.stdin); \
print('N2 预留字节:', d['reserved_bytes'], '在途副本:', len(d['live_replicas']))"
$CLI tick 2 >/dev/null
echo "→ 恢复，只补剩余字节（逐对象偏移已持久化）"
$CLI resume "$BATCH" >/dev/null
$CLI tick 2 >/dev/null
$CLI queue

line "5. RSU-N3 离线 → 立即释放名额；恢复在线后续传剩余"
$CLI offline RSU-N3
$CLI tick 1 >/dev/null
$CLI boxes RSU-N3 | python3 -c "import json,sys; d=json.load(sys.stdin); \
print('N3 离线: 预留', d['reserved_bytes'], '有效链路', d['effective_link_bps'])"
$CLI online RSU-N3
$CLI tick 1 >/dev/null

line "6. RSU-N2 链路降级到 3MiB/s：按有效链路限速并给出原因"
$CLI degrade RSU-N2 $((3*MB))
$CLI tick 1 >/dev/null
$CLI queue

line "7. 空间不足整批原子拒绝：小盒 RSU-W2（120MiB）装不下 241MiB"
$CLI submit RSU-W1,RSU-W2 BL-MAP-MEDIA,BL-MAP-LANE - - - 1 "超大批次" || true

line "8. 取消作业判定一次；已传部分字节保留，再次提交只补剩余"
$CLI submit RSU-S1 BL-MAP-MEDIA - $((20*MB)) - 1 "稍后取消" >/dev/null
CAN=$(jid_of 稍后取消)
$CLI tick 3 >/dev/null
$CLI cancel "$CAN" "重新规划"
$CLI boxes RSU-S1 | python3 -c "import json,sys; d=json.load(sys.stdin); \
print('S1 固化的在途偏移(MiB):', {k: round(v/1048576,1) for k,v in d['partial_bytes'].items()})"
$CLI submit RSU-S1 BL-MAP-MEDIA - $((20*MB)) - 1 "再次提交" --resubmit "$CAN" >/dev/null
RE=$(jid_of 再次提交)
$CLI show "$RE" | python3 -c "import json,sys; d=json.load(sys.stdin); \
r=d['replicas'][0]; print('再次提交剩余(MiB):', round(r['remaining_bytes']/1048576,1))"

line "9. 推进时间，观察全局排队次序与 ETA"
$CLI tick 20 >/dev/null
$CLI queue

line "10. 不变量自检：无幽灵占用、站点不超带宽、盒内不超容量"
$CLI invariants

line "11. 编排事件流（append-only，取最近 12 条）"
$CLI events 12 | python3 -c "
import json,sys
for e in json.load(sys.stdin)['events']:
    print(f\"  t={e['tick']:>3} {e['type']:<18} {e['summary']}\")"

echo
echo "演示完成。可用 '$CLI dashboard' 查看全量状态；网页 http://localhost:8080 🔥 预热编排 标签页同源。"
