#!/usr/bin/env bash
# 真实进程级“停机跨过期限”演练:
#   1) 启动 uvicorn 服务
#   2) 申请 3 秒窗口
#   3) 停掉服务进程（模拟停机），等过了截止时间
#   4) 重新启动服务，验证启动即恢复且参数回到申请前状态
#
# 依赖一个开启 autovacuum 的 device_events（见 db/init/01_device_events.sql）。
# 用法: DATABASE_URL='postgresql://...' tests/restart_demo.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PORT="${API_PORT:-18000}"
BASE="http://127.0.0.1:${PORT}"
export PYTHONPATH="$ROOT"
export OVERRIDE_RECONCILE_INTERVAL_SECONDS="${OVERRIDE_RECONCILE_INTERVAL_SECONDS:-0.5}"
export OVERRIDE_RESTORE_LOCK_TIMEOUT_SECONDS="${OVERRIDE_RESTORE_LOCK_TIMEOUT_SECONDS:-2}"

log() { printf '\n[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
json_get() { python3 -c 'import json,sys;d=json.load(sys.stdin);print(eval(sys.argv[1]))' "$1"; }

cleanup() {
  [[ -n "${SERVER_PID:-}" ]] && kill "$SERVER_PID" 2>/dev/null || true
  wait "${SERVER_PID:-}" 2>/dev/null || true
}
trap cleanup EXIT

start_server() {
  ( cd "$ROOT" && python3 -m uvicorn app.main:app --host 127.0.0.1 --port "$PORT" \
      --log-level warning ) &
  SERVER_PID=$!
  for _ in $(seq 1 50); do
    curl -sf "$BASE/health" >/dev/null && return 0
    sleep 0.2
  done
  echo "server did not start" >&2; exit 1
}

stop_server() {
  kill "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
  SERVER_PID=""
}

REQ="RESTART-$(date +%s)"
log "启动服务 1"
start_server

log "申请号 $REQ，窗口 3 秒"
curl -sf -X POST "$BASE/autovacuum/overrides" -H 'Content-Type: application/json' \
  -d "{\"request_no\":\"$REQ\",\"table_name\":\"device_events\",
       \"autovacuum_vacuum_insert_scale_factor\":0.03125,
       \"duration_seconds\":3}" | python3 -m json.tool
OID=$(curl -sf "$BASE/autovacuum/overrides?table_name=device_events&limit=1" \
        | json_get 'd["overrides"][0]["id"]')
log "申请 id=$OID；库中当前设定:"
curl -sf "$BASE/autovacuum/table-setting?table_name=device_events" \
  | json_get 'd["insert_scale_factor"]'

log "停掉服务（模拟停机），等 5 秒跨过截止时间"
stop_server
sleep 5

log "重新启动服务 2 —— 启动时立即扫描到期申请"
start_server
sleep 1
curl -sf "$BASE/autovacuum/overrides/$OID" | python3 -m json.tool
log "库中恢复后的设定:"
curl -sf "$BASE/autovacuum/table-setting?table_name=device_events" \
  | json_get 'd["insert_scale_factor"]'

STATUS=$(curl -sf "$BASE/autovacuum/overrides/$OID" | json_get 'd["status"]')
[[ "$STATUS" == "restored" ]] || { echo "FAIL: status=$STATUS" >&2; exit 1; }
log "PASS: 停机跨期后，重启即恢复（status=$STATUS）"
