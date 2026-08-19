#!/usr/bin/env bash
# scripts/find-leader.sh
#
# Найти текущего ЛИДЕРА кластера Patroni.
# Перебирает способы в порядке надёжности:
#   1) DCS (etcd)   — источник правды (ключ /service/<SCOPE>/leader)
#   2) Patroni /cluster на любой ноде
#   3) Опрос /primary каждой ноды
# Печатает: Leader = <name> (<ip>)  [source: ...]
#
# Запуск:
#   ./scripts/find-leader.sh
#   SCOPE=pgcluster ./scripts/find-leader.sh
#   ETCD_ENDPOINTS=http://10.0.0.11:2379 ./scripts/find-leader.sh
#
# Подробнее: 07-identify-master.md

set -euo pipefail

# --- дефолты (можно переопределить env-переменными или topology.env) ---
SCOPE="${SCOPE:-pgcluster}"
ETCD_ENDPOINTS="${ETCD_ENDPOINTS:-http://pg1:2379,http://pg2:2379,http://pg3:2379}"
ALL_NODES="${ALL_NODES:-pg1 pg2 pg3}"

# вспомогательная: по имени ноды достать её IP из Patroni /cluster
ip_of() {
  local node="$1"
  # пробуем через getent (нужны записи в /etc/hosts или DNS)
  getent hosts "$node" 2>/dev/null | awk '{print $1; exit}' || echo "$node"
}

# --- 1) DCS (etcd) — самый надёжный источник ---
if command -v etcdctl >/dev/null 2>&1; then
  leader="$(ETCDCTL_API=3 etcdctl --endpoints="$ETCD_ENDPOINTS" \
              get "/service/${SCOPE}/leader" 2>/dev/null | head -1 || true)"
  if [ -n "${leader:-}" ]; then
    echo "Leader = $leader ($(ip_of "$leader"))   [source: etcd DCS]"
    exit 0
  fi
fi

# --- 2) Patroni /cluster на любой живой ноде ---
for h in $ALL_NODES; do
  json="$(curl -fsS --max-time 3 "http://${h}:8008/cluster" 2>/dev/null || true)"
  if [ -n "${json:-}" ] && command -v jq >/dev/null 2>&1; then
    leader="$(printf '%s' "$json" | jq -r '.members[]? | select(.role=="leader") | .name' 2>/dev/null || true)"
    leader_ip="$(printf '%s' "$json" | jq -r --arg n "$leader" '.members[]? | select(.name==$n) | .host' 2>/dev/null || true)"
    if [ -n "${leader:-}" ]; then
      echo "Leader = $leader (${leader_ip:-$(ip_of "$leader")})   [source: patroni /cluster on ${h}]"
      exit 0
    fi
  fi
done

# --- 3) Опрос /primary каждой ноды (самый «ручной» путь) ---
for h in $ALL_NODES; do
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "http://${h}:8008/primary" 2>/dev/null || echo 000)"
  if [ "$code" = "200" ]; then
    echo "Leader = $h ($(ip_of "$h"))   [source: patroni /primary == 200]"
    exit 0
  fi
done

# --- никто не отвечает ---
echo "NO LEADER found (DCS unavailable, no Patroni API responded 200 on /primary)." >&2
echo "  etcd endpoints: $ETCD_ENDPOINTS" >&2
echo "  nodes tried:    $ALL_NODES" >&2
exit 2
