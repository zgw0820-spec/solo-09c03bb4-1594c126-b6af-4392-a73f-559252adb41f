#!/usr/bin/env bash
# 对本地 PostgreSQL 16 跑阈值覆盖端到端自测。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SOCKET="${PG_SOCKET:-/workspace/.tools/run}"
PORT="${PG_PORT:-55432}"

export DATABASE_URL="postgresql://postgres@/devicedb?host=${SOCKET}&port=${PORT}"
export PYTHONPATH="${ROOT}"

exec python3 "$ROOT/tests/override_selftest.py" "$@"
