#!/usr/bin/env bash
# scripts/get-role.sh
#
# Узнать роль конкретной ноды: master(leader) / replica / unavailable.
# Использует Patroni REST API (http://<node>:8008).
#
# Запуск:
#   ./scripts/get-role.sh pg1
#   ./scripts/get-role.sh pg2
#   PG_API_PORT=8008 ./scripts/get-role.sh 10.0.0.11
#
# Подробности: 07-identify-master.md

set -euo pipefail

if [ $# -lt 1 ]; then
  echo "Usage: $0 <node-host-or-ip>" >&2
  exit 2
fi

node="$1"
port="${PG_API_PORT:-8008}"

# /primary -> 200 только у лидера; /replica -> 200 только у реплики;
# общий /  -> 200 если нода жива (роль есть в JSON).
primary_code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "http://${node}:${port}/primary" 2>/dev/null || echo 000)"
replica_code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "http://${node}:${port}/replica" 2>/dev/null || echo 000)"

if [ "$primary_code" = "200" ]; then
  echo "$node: MASTER (leader)"
  exit 0
elif [ "$replica_code" = "200" ]; then
  echo "$node: REPLICA"
  exit 1
else
  # не лидер и не реплика — либо падает, либо инициализируется
  info="$(curl -fsS --max-time 3 "http://${node}:${port}/" 2>/dev/null || echo '{}')"
  state="$(printf '%s' "$info" | sed -n 's/.*"state"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' || true)"
  echo "$node: UNAVAILABLE (http primary=$primary_code replica=$replica_code state=${state:-unknown})"
  exit 3
fi
