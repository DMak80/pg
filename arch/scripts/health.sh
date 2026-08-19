#!/usr/bin/env bash
# scripts/health.sh
#
# Быстрая проверка здоровья всех компонентов кластера.
# Возвращает 0 если всё ОК, ненулевой код — если что-то не так.
# Подходит для cron/мониторинга.
#
#   ./scripts/health.sh
#   ./scripts/health.sh --quiet      # печатать только при проблемах

set -uo pipefail
quiet=0
[ "${1:-}" = "--quiet" ] && quiet=1

SCOPE="${SCOPE:-pgcluster}"
ETCD_ENDPOINTS="${ETCD_ENDPOINTS:-http://pg1:2379,http://pg2:2379,http://pg3:2379}"
ALL_NODES="${ALL_NODES:-pg1 pg2 pg3}"
port="${PG_API_PORT:-8008}"
errors=0

ok()   { [ "$quiet" = 0 ] && echo "  ✅ $*"; }
warn() { echo "  ❌ $*"; errors=$((errors+1)); }

echo "Health check @ $(date -u +%FT%TZ):"

# --- etcd: кворум должен быть ---
if command -v etcdctl >/dev/null 2>&1; then
  healthy="$(ETCDCTL_API=3 etcdctl --endpoints="$ETCD_ENDPOINTS" endpoint health --cluster 2>/dev/null | grep -c 'is healthy' || echo 0)"
  if [ "$healthy" -ge 2 ]; then
    ok "etcd: кворум есть ($healthy/3 healthy)"
  else
    warn "etcd: кворума НЕТ (healthy=$healthy/3)"
  fi
else
  ok "etcd: (etcdctl нет — пропускается)"
fi

# --- должен быть ровно один лидер ---
primary_count=0
primary_node=""
for h in $ALL_NODES; do
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "http://${h}:${port}/primary" 2>/dev/null || echo 000)"
  if [ "$code" = "200" ]; then
    primary_count=$((primary_count+1))
    primary_node="$h"
  fi
done
if [ "$primary_count" -eq 1 ]; then
  ok "лидер: ровно один ($primary_node)"
elif [ "$primary_count" -eq 0 ]; then
  warn "лидер: НЕТ (кластер без мастера)"
else
  warn "лидер: БОЛЬШЕ ОДНОГО ($primary_count) — возможен split-brain!"
fi

# --- все Patroni API отвечают ---
alive=0
for h in $ALL_NODES; do
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "http://${h}:${port}/" 2>/dev/null || echo 000)"
  [ "$code" = "200" ] && alive=$((alive+1))
done
if [ "$alive" -ge 2 ]; then
  ok "Patroni API: $alive/3 нод отвечают"
else
  warn "Patroni API: только $alive/3 нод отвечают (< 2 — нет большинства)"
fi

echo
if [ "$errors" -eq 0 ]; then
  [ "$quiet" = 0 ] && echo "RESULT: OK ✅"
  exit 0
else
  echo "RESULT: PROBLEMS ($errors) ❌"
  exit 1
fi
