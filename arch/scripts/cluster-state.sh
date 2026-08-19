#!/usr/bin/env bash
# scripts/cluster-state.sh
#
# Сводное состояние кластера: лидер + роли нод + здоровье etcd + лаг репликации.
# Удобно вызывать одной командой для быстрой диагностики.
#
#   ./scripts/cluster-state.sh

set -uo pipefail

SCOPE="${SCOPE:-pgcluster}"
ETCD_ENDPOINTS="${ETCD_ENDPOINTS:-http://pg1:2379,http://pg2:2379,http://pg3:2379}"
ALL_NODES="${ALL_NODES:-pg1 pg2 pg3}"
port="${PG_API_PORT:-8008}"

echo "═══ PostgreSQL/Patroni cluster: $SCOPE ═══"
echo

# 1) Лидер
leader="$(bash "$(dirname "$0")/find-leader.sh" 2>/dev/null || true)"
echo "→ Leader:"
echo "    ${leader:-unknown}"
echo

# 2) Роли по нодам
echo "→ Roles per node (Patroni REST API):"
printf '    %-12s %-12s %-12s %-12s\n' NODE ROLE STATE LAG_MB
for h in $ALL_NODES; do
  json="$(curl -fsS --max-time 3 "http://${h}:${port}/" 2>/dev/null || echo '')"
  if [ -z "$json" ]; then
    printf '    %-12s %-12s %-12s %-12s\n' "$h" "-" "DOWN" "-"
    continue
  fi
  role="$(printf '%s' "$json" | sed -n 's/.*"role"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')"
  state="$(printf '%s' "$json" | sed -n 's/.*"state"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')"
  lag="$(printf '%s' "$json" | sed -n 's/.*"lag"[[:space:]]*:[[:space:]]*\([0-9]*\).*/\1/p')"
  [ -z "${lag:-}" ] && lag="-"
  printf '    %-12s %-12s %-12s %-12s\n' "$h" "${role:-?}" "${state:-?}" "$lag"
done
echo

# 3) etcd health
echo "→ etcd cluster health:"
if command -v etcdctl >/dev/null 2>&1; then
  ETCDCTL_API=3 etcdctl --endpoints="$ETCD_ENDPOINTS" endpoint health --cluster 2>&1 \
    | sed 's/^/    /' || echo "    etcdctl failed"
else
  echo "    (etcdctl не установлен — пропускаю; используйте 'docker exec etcd etcdctl ...')"
fi
echo

# 4) patronictl list (если есть docker)
echo "→ patronictl list (через контейнер postgres):"
if docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^postgres$'; then
  docker exec -i postgres patronictl list 2>&1 | sed 's/^/    /' || true
else
  echo "    (контейнер 'postgres' не найден локально — запусти на ноде БД или используй scripts/patronictl.sh)"
fi
