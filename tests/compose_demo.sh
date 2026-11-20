#!/usr/bin/env bash
# 基于 Docker Compose 的阈值覆盖演示（无需宿主安装 psql/curl 之外的东西）。
#
# 前置: docker compose up --build -d 且 db healthy。
# 用法: bash tests/compose_demo.sh
#
# 依次演示:
#   A. 初始状态: 设备事件表 autovacuum 开启，插入阈值继承实例默认值
#   B. 申请 -> 库中实际写入表级值；原样重送沿用原截止时间；同表新申请被拒
#   C. 到期自动恢复继承
#   D. 恢复被 VACUUM 持有的表锁挡住 -> pending_restore -> 释放后自动恢复
#   E. 到期时被手工改为别的值 -> preserved，保留人工值
set -euo pipefail

API="${API:-http://localhost:8000}"
TABLE="device_events"
PSQL=(docker compose exec -T db psql -U appuser -d devicedb -qtA)

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
q()   { curl -sf "$@"; }
jqget() { python3 -c "import json,sys;d=json.load(sys.stdin);print($1)"; }

wait_api() {
  for _ in $(seq 1 60); do q "$API/health" >/dev/null 2>&1 && return 0; sleep 1; done
  echo "API 未就绪: $API" >&2; exit 1
}

wait_status() { # $1=id $2=期望状态 $3=超时秒
  local end=$(( $(date +%s) + ${3:-30} ))
  while (( $(date +%s) < end )); do
    local s; s=$(q "$API/autovacuum/overrides/$1" | jqget 'd["status"]')
    [[ "$s" == "$2" ]] && { echo "$s"; return 0; }
    sleep 0.5
  done
  echo "timeout waiting $2, last=$s" >&2; return 1
}

wait_api

say "A. 初始状态（库中实际设定）"
q "$API/autovacuum/table-setting?table_name=$TABLE" | python3 -m json.tool
SRC=$("${PSQL[@]}" -c "SELECT (SELECT option_value FROM pg_options_to_table(reloptions) WHERE option_name='autovacuum_vacuum_insert_scale_factor') FROM pg_class WHERE relname='$TABLE'")
[[ -z "$SRC" ]] && echo "表级未设置 -> 继承实例默认（符合预期）"

say "B. 申请临时阈值 + 幂等重送 + 新申请被拒"
REQ="DEMO-$(date +%s)"
RESP=$(q -X POST "$API/autovacuum/overrides" -H 'Content-Type: application/json' \
  -d "{\"request_no\":\"$REQ\",\"table_name\":\"$TABLE\",
       \"autovacuum_vacuum_insert_scale_factor\":0.05,
       \"duration_seconds\":600}")
echo "$RESP" | python3 -m json.tool
OID=$(echo "$RESP" | jqget 'd["override"]["id"]')
EXP=$(echo "$RESP" | jqget 'd["override"]["expires_at"]')
"${PSQL[@]}" -c "SELECT reloptions FROM pg_class WHERE relname='$TABLE'" \
  | tr ',' '\n' | grep insert_scale_factor || true
REPLAY=$(q -X POST "$API/autovacuum/overrides" -H 'Content-Type: application/json' \
  -d "{\"request_no\":\"$REQ\",\"table_name\":\"$TABLE\",
       \"autovacuum_vacuum_insert_scale_factor\":0.05,
       \"duration_seconds\":600}")
[[ "$(echo "$REPLAY" | jqget 'd["override"]["replayed"]')" == "True" ]] \
  && [[ "$(echo "$REPLAY" | jqget 'd["override"]["expires_at"]')" == "$EXP" ]] \
  && echo "重送: replayed=true，截止时间未变 ($EXP)"
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$API/autovacuum/overrides" \
  -H 'Content-Type: application/json' \
  -d "{\"request_no\":\"$REQ-2\",\"table_name\":\"$TABLE\",
       \"autovacuum_vacuum_insert_scale_factor\":0.07,\"duration_seconds\":600}")
echo "同表新申请 -> HTTP $CODE（预期 409）"

say "C. 到期自动恢复继承（这里用 SQL 把截止时间提前，免等 10 分钟）"
"${PSQL[@]}" -c "UPDATE maintenance.autovacuum_overrides SET expires_at=now() WHERE id=$OID" >/dev/null
wait_status "$OID" restored 30
q "$API/autovacuum/overrides/$OID" | python3 -c "import json,sys;d=json.load(sys.stdin);print(d['status'],'|',d['outcome_detail'])"
q "$API/autovacuum/table-setting?table_name=$TABLE" \
  | python3 -c "import json,sys;d=json.load(sys.stdin)['insert_scale_factor'];print('source=',d['source'],'effective=',d['effective_value'])"

say "D. 恢复被 VACUUM 表锁挡住 -> pending_restore -> 自动重试"
RESP=$(q -X POST "$API/autovacuum/overrides" -H 'Content-Type: application/json' \
  -d "{\"request_no\":\"$REQ-LOCK\",\"table_name\":\"$TABLE\",
       \"autovacuum_vacuum_insert_scale_factor\":0.06,\"duration_seconds\":600}")
OID2=$(echo "$RESP" | jqget 'd["override"]["id"]')
# 另开会话持有 SHARE UPDATE EXCLUSIVE（VACUUM 使用的锁），模拟 VACUUM 正在进行
docker compose exec -T db psql -U appuser -d devicedb -qc \
  "BEGIN; LOCK TABLE public.$TABLE IN SHARE UPDATE EXCLUSIVE MODE; SELECT pg_sleep(12); ROLLBACK;" \
  >/dev/null 2>&1 &
HOLDER=$!
sleep 1
"${PSQL[@]}" -c "UPDATE maintenance.autovacuum_overrides SET expires_at=now() WHERE id=$OID2" >/dev/null
# 锁等待上限 3s，给足时间观察 pending
sleep 6
q "$API/autovacuum/overrides/$OID2" \
  | python3 -c "import json,sys;d=json.load(sys.stdin);print(d['status'],'attempts=',d['restore_attempts'],'|',d['last_error'])"
wait "$HOLDER" || true
wait_status "$OID2" restored 30
echo "锁释放后自动恢复: $(q "$API/autovacuum/overrides/$OID2" | jqget 'd["status"]')"

say "E. 到期时参数被人工改为别的值 -> preserved"
RESP=$(q -X POST "$API/autovacuum/overrides" -H 'Content-Type: application/json' \
  -d "{\"request_no\":\"$REQ-MANUAL\",\"table_name\":\"$TABLE\",
       \"autovacuum_vacuum_insert_scale_factor\":0.08,\"duration_seconds\":600}")
OID3=$(echo "$RESP" | jqget 'd["override"]["id"]')
"${PSQL[@]}" -c "ALTER TABLE public.$TABLE SET (autovacuum_vacuum_insert_scale_factor=0.42)" >/dev/null
"${PSQL[@]}" -c "UPDATE maintenance.autovacuum_overrides SET expires_at=now() WHERE id=$OID3" >/dev/null
wait_status "$OID3" preserved 30
q "$API/autovacuum/overrides/$OID3" \
  | python3 -c "import json,sys;d=json.load(sys.stdin);print(d['status'],'|',d['outcome_detail'])"
"${PSQL[@]}" -c "SELECT reloptions FROM pg_class WHERE relname='$TABLE'" \
  | tr ',' '\n' | grep insert_scale_factor

say "清理演示用人工值（恢复继承）"
"${PSQL[@]}" -c "ALTER TABLE public.$TABLE RESET (autovacuum_vacuum_insert_scale_factor)" >/dev/null
echo "完成。注意: pgdata 为命名卷，重新初始化 demo 需 docker compose down -v。"
