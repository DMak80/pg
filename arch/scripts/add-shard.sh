#!/usr/bin/env bash
# scripts/add-shard.sh
#
# Подключить новый шард в инициализированный кластер: регистрирует строку
# подключения (без пароля) и декларативное число реплик в etcd. БАКЕТЫ НЕ
# ТРОГАЕТ: новый шард подключается пустым, бакеты мигрируются на него позже
# отдельной командой move-bucket.sh (11-bucket-sharding.md, «Жизненный цикл»).
#
# Сам кластер-шард (Patroni+Spilo, параметры §4 доки 11) поднимается заранее
# по докам 04–06; add-shard его только регистрирует. Patroni-callback шарда
# обязан писать /clusters/<C>/shards/<X>/master (on_role_change).
#
# Использование:
#   ./scripts/add-shard.sh --cluster shop shard3 \
#       --dsn 'host=10.0.3.1,10.0.3.2,10.0.3.3 port=5432 dbname=app user=bucket_admin' \
#       [--replicas 2] [--no-check]
#
#   --dsn      multi-host write-эндпоинт (HAProxy любой ноды → мастер, P2);
#              password= вырезается, пароль — SHARD_<X>_PASSWORD в buckets.env
#   --replicas декларативное число реплик (дефолт 1)
#   --no-check не проверять доступность шарда (регистрация заранее)
#
# Коды выхода: 2 — использование; 3 — guard'ы; 9 — конфиг/окружение.

set -euo pipefail
export PGCONNECT_TIMEOUT="${PGCONNECT_TIMEOUT:-5}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
. "$SCRIPT_DIR/buckets-common.sh"

usage() {
  cat >&2 <<EOF
Usage:
  $0 --cluster <C> <shard> --dsn '<host=... port=5432 dbname=... user=...>' [--replicas <R>] [--no-check]
EOF
  exit 2
}

CLUSTER="" SHARD="" DSN="" REPLICAS="" NO_CHECK=0
while [ $# -gt 0 ]; do
  case "$1" in
    --cluster)  CLUSTER="${2:-}"; shift 2 ;;
    --dsn)      DSN="${2:-}"; shift 2 ;;
    --replicas) REPLICAS="${2:-}"; shift 2 ;;
    --no-check) NO_CHECK=1; shift ;;
    -h|--help)  usage ;;
    *)          [ -z "$SHARD" ] && SHARD="$1" || usage; shift ;;
  esac
done

require_bins psql jq etcdctl
[ -n "$CLUSTER" ] && [ -n "$SHARD" ] && [ -n "$DSN" ] || usage
cluster_set "$CLUSTER"
[[ "$SHARD" =~ ^[a-z][a-z0-9_]*$ ]] || { err "неверное имя шарда '$SHARD'"; exit 2; }
REPLICAS="${REPLICAS:-1}"
[[ "$REPLICAS" =~ ^[0-9]+$ ]] || { err "--replicas: целое >= 0"; exit 2; }
DSN="$(dsn_strip_password "$DSN")"
grep -qE '(^| )host=' <<<"$DSN" && grep -qE '(^| )dbname=' <<<"$DSN" && grep -qE '(^| )user=' <<<"$DSN" \
  || { err "DSN обязан содержать host=, dbname= и user=: '$DSN'"; exit 2; }

step "1) Guards"
etcd_alive
[ -n "$(cluster_config)" ] \
  || { err "кластер '$CLUSTER_NAME' не инициализирован: нет $(config_key). Сначала init-cluster.sh."; exit 3; }
[ -z "$(etcd_value "$(shard_key "$SHARD")/dsn")" ] \
  || { err "шард '$SHARD' уже зарегистрирован: $(shard_key "$SHARD")/dsn"; exit 3; }
info "кластер '$CLUSTER_NAME' инициализирован, шард '$SHARD' свободен"

step "2) Доступность шарда"
if [ "$NO_CHECK" = 1 ]; then
  echo "  --no-check: проверка пропущена"
else
  dsn_full="$(dsn_with_password "$DSN" "$SHARD")"
  [ "$(poll_scalar "$dsn_full" 'SELECT 1' 3)" = "1" ] \
    || { err "шард '$SHARD' недоступен (пароль: SHARD_${SHARD}_PASSWORD; заранее — --no-check)"; exit 3; }
  info "шард доступен"
fi

step "3) Регистрация в etcd"
ect put "$(shard_key "$SHARD")/dsn" "$DSN" >/dev/null
ect put "$(shard_key "$SHARD")/replicas" "$REPLICAS" >/dev/null
info "$(shard_key "$SHARD")/dsn = $DSN"
info "$(shard_key "$SHARD")/replicas = $REPLICAS"

echo
echo "Готово: шард '$SHARD' подключён ПУСТЫМ (бакеты не переносились)."
echo "  Напоминание §4 доки 11: wal_level=logical на всех нодах, failover slots,"
echo "  роли bucket_mover/app, sync-standby — без них move откажется в preflight."
echo "  Перенести бакеты:  ./move-bucket.sh --cluster $CLUSTER_NAME move <бакет> --to $SHARD"
echo "  Список шардов:     $(printf '%s\n' "$(shards_list)" | tr '\n' ' ')"
