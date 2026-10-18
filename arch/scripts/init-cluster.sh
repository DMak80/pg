#!/usr/bin/env bash
# scripts/init-cluster.sh
#
# Инициализация шардированного кластера (системы) в etcd-контрол-плейне:
# фиксирует константы (число бакетов N, имя БД), регистрирует шарды со
# строками подключения и создаёт все N бакетов (пустых схем), распределяя их
# по шардам поровну — round-robin. Сами кластеры-шарды поднимаются отдельно
# по докам 04–06 (+ параметры §4 доки 11): init их только регистрирует.
#
#   N (число бакетов) и dbname — КОНСТАНТЫ: выбираются один раз и навсегда
#   (P18: смена N ломает хеш-маппинг тенантов). Шарды и реплики меняемы:
#   add-shard.sh подключает пустой шард, move-bucket.sh мигрирует бакеты
#   на него позже, remove-shard.sh удаляет пустой шард.
#
# Использование:
#   ./scripts/init-cluster.sh --cluster shop --buckets 256 --dbname app \
#       [--replicas 2] \
#       --shard shard1='host=10.0.1.1,10.0.1.2,10.0.1.3 port=5432 dbname=app user=bucket_admin' \
#       --shard shard2='host=10.0.2.1,10.0.2.2,10.0.2.3 port=5432 dbname=app user=bucket_admin'
#
# DSN — multi-host write-эндпоинт шарда (HAProxy любой ноды ведёт на текущего
# мастера, P2). Пароли в etcd НЕ пишутся: password= вырезается из DSN,
# пароль задаётся SHARD_<X>_PASSWORD в buckets.env.
#
# Бакеты называются bucket_0 .. bucket_<N-1> (шаблон фиксирован: роутер
# приложения считает bucket_id = hash(tenant_id) % N, схема = bucket_<id>).
#
# Повторный init того же кластера отказывает (константы неизменяемы).
# Сбой посередине: схемы/гранты идемпотентны (пустые — пересоздаются),
# etcd-ключи кладутся последним пакетом; полуинициализированный префикс
# безопасно очистить:  etcdctl del /clusters/<C> --prefix  и повторить.
#
# Конфиг: configs/buckets/buckets.env (ETCD_ENDPOINTS, APP_ROLE, пароли).
#
# Коды выхода: 2 — использование; 3 — guard/проверки; 4 — сбой выполнения;
# 9 — конфиг/окружение.

set -euo pipefail
export PGCONNECT_TIMEOUT="${PGCONNECT_TIMEOUT:-5}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
. "$SCRIPT_DIR/buckets-common.sh"

usage() {
  cat >&2 <<EOF
Usage:
  $0 --cluster <C> --buckets <N> --dbname <DB> [--replicas <R>] \\
     --shard <name>=<DSN> [--shard <name>=<DSN> ...]

  --cluster   name of the cluster being created (the /clusters/<C>/ prefix in etcd)
  --buckets   N — number of buckets, a CONSTANT for the cluster's lifetime (P18)
  --dbname    PG database name (one per cluster, created on the shards in advance)
  --replicas  declarative number of shard replicas (default 1; the actual
              number is configured in each shard's Patroni)
  --shard     shard name = DSN of its write endpoint (no password; repeatable).
              The DSN must contain host=, dbname= and user=. The password is
              SHARD_<X>_PASSWORD in buckets.env, never stored in etcd.
EOF
  exit 2
}

confirm() {
  [ "$ASSUME_YES" = 1 ] && return 0
  local a=""
  read -rp "$1 [type YES]: " a
  [ "${a:-}" = "YES" ] || { echo "Cancelled."; exit 1; }
}

CLUSTER="" BUCKETS="" DBNAME="" REPLICAS="" ASSUME_YES=0
SHARD_ARGS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --cluster)  CLUSTER="${2:-}"; shift 2 ;;
    --buckets)  BUCKETS="${2:-}"; shift 2 ;;
    --dbname)   DBNAME="${2:-}"; shift 2 ;;
    --replicas) REPLICAS="${2:-}"; shift 2 ;;
    --shard)    SHARD_ARGS+=("${2:-}"); shift 2 ;;
    --yes|-y)   ASSUME_YES=1; shift ;;
    -h|--help)  usage ;;
    *)          usage ;;
  esac
done

require_bins psql jq etcdctl
[ -n "$CLUSTER" ] && [ -n "$BUCKETS" ] && [ -n "$DBNAME" ] && [ "${#SHARD_ARGS[@]}" -ge 1 ] || usage
cluster_set "$CLUSTER"
valid_cluster "$CLUSTER_NAME" || usage

[[ "$BUCKETS" =~ ^[1-9][0-9]*$ ]] || { err "--buckets: integer > 0 (got '$BUCKETS')"; exit 2; }
REPLICAS="${REPLICAS:-1}"
[[ "$REPLICAS" =~ ^[0-9]+$ ]] || { err "--replicas: integer >= 0"; exit 2; }
valid_dbname "$DBNAME" || { err "invalid DB name '$DBNAME' (pattern: ^[a-z_][a-z0-9_]*$)"; exit 2; }

# ── Шарды: разбор name=dsn, валидация имён/уникальности/DSN ───────────────────
SHARD_NAMES=()
declare -A SHARD_DSN=()
for a in "${SHARD_ARGS[@]}"; do
  name="${a%%=*}"; dsn="${a#*=}"
  [ "$name" != "$dsn" ] || { err "--shard: expects <name>=<DSN> (got '$a')"; exit 2; }
  [[ "$name" =~ ^[a-z][a-z0-9_]*$ ]] || { err "invalid shard name '$name'"; exit 2; }
  [ -z "${SHARD_DSN[$name]:-}" ] || { err "shard '$name' specified twice"; exit 2; }
  dsn="$(dsn_strip_password "$dsn")"
  grep -qE '(^| )host=' <<<"$dsn" && grep -qE '(^| )dbname=' <<<"$dsn" && grep -qE '(^| )user=' <<<"$dsn" \
    || { err "shard '$name' DSN must contain host=, dbname= and user=: '$dsn'"; exit 2; }
  SHARD_NAMES+=("$name"); SHARD_DSN["$name"]="$dsn"
done
S_COUNT="${#SHARD_NAMES[@]}"

# ── 1) Guards контрол-плейна ─────────────────────────────────────────────────
step "1) etcd checks"
etcd_alive
if [ -n "$(etcd_prefix_keys "$(cluster_root)")" ]; then
  err "prefix $(cluster_root) is not empty — cluster '$CLUSTER_NAME' is already initialized (or its leftovers remain)."
  echo "  Constants are immutable (P18). A half-initialized prefix (no schemas yet):" >&2
  echo "    etcdctl del $(cluster_root) --prefix   — then rerun init." >&2
  exit 3
fi
info "prefix $(cluster_root) is empty"

# ── 2) Шарды доступны, БД существует ─────────────────────────────────────────
step "2) Checking shard and DB '$DBNAME' availability"
for s in "${SHARD_NAMES[@]}"; do
  dsn="$(dsn_with_password "${SHARD_DSN[$s]}" "$s")"
  if [ "$(poll_scalar "$dsn" 'SELECT 1' 3)" != "1" ]; then
    err "shard '$s' is unreachable via DSN (password: SHARD_${s}_PASSWORD in buckets.env, if needed)"
    exit 3
  fi
  info "shard '$s': reachable ($(grep -oE 'host=[^ ]+' <<<"${SHARD_DSN[$s]}"))"
done

# ── 3) Схемы bucket_0..N-1: существующих (непустых) быть не должно ────────────
step "3) Checking schemas bucket_0..bucket_$((BUCKETS - 1))"
bucket_shard() { printf '%s' "${SHARD_NAMES[$(( $1 % S_COUNT ))]}"; }
for s in "${SHARD_NAMES[@]}"; do
  clash="$(scalar "$(dsn_with_password "${SHARD_DSN[$s]}" "$s")" \
    "SELECT string_agg(format('%s (%s objects)', n.nspname,
            (SELECT count(*) FROM pg_class c WHERE c.relnamespace=n.oid)), ', ')
     FROM pg_namespace n
     WHERE n.nspname LIKE 'bucket_%' AND n.nspname ~ '^bucket_[0-9]+$'
       AND substr(n.nspname, 8)::int < $BUCKETS")" || clash=""
  [ -z "$clash" ] || {
    err "'$s' already has range schemas: $clash"
    echo "  Initialization expects clean schemas: drop them or choose another cluster/DB name." >&2
    exit 3
  }
done
info "no existing range schemas"

# ── 4) План ──────────────────────────────────────────────────────────────────
per=$(( BUCKETS / S_COUNT )); rem=$(( BUCKETS % S_COUNT ))
step "4) Plan"
echo "  cluster:     $CLUSTER_NAME  (etcd: $(cluster_root))"
echo "  constants:   buckets N=$BUCKETS (bucket_0..bucket_$((BUCKETS - 1))), DB '$DBNAME'"
echo "  shards:      $S_COUNT, declarative replicas: $REPLICAS"
for s in "${SHARD_NAMES[@]}"; do
  echo "    $s: ${SHARD_DSN[$s]}"
done
echo "  even distribution (round-robin):"
for i in "${!SHARD_NAMES[@]}"; do
  s="${SHARD_NAMES[$i]}"; n="$per"; [ "$i" -lt "$rem" ] && n=$((per + 1))
  echo "    $s: $n bucket(s)"
done
if [ -n "${APP_ROLE:-}" ]; then
  echo "  app role:    $APP_ROLE (CREATE ROLE if missing + GRANT USAGE on schemas)"
else
  echo "  app role:    not set (grant later per §4 of doc 11)"
fi
confirm "Initialize cluster '$CLUSTER_NAME'?"

# ── 5) Схемы + гранты (идемпотентно) ─────────────────────────────────────────
step "5) Creating schemas and grants"
for s in "${SHARD_NAMES[@]}"; do
  dsn="$(dsn_with_password "${SHARD_DSN[$s]}" "$s")"
  ddl=""; grant=""
  for (( i = 0; i < BUCKETS; i++ )); do
    [ "$(bucket_shard "$i")" = "$s" ] || continue
    ddl+="CREATE SCHEMA IF NOT EXISTS bucket_$i; "
    grant+="GRANT USAGE ON SCHEMA bucket_$i TO $APP_ROLE; "
  done
  sql "$dsn" "$ddl" || { err "failed to create schemas on '$s'"; exit 4; }
  info "schemas on '$s' are ready"
  if [ -n "${APP_ROLE:-}" ]; then
    valid_bucket "$APP_ROLE" || { err "APP_ROLE='$APP_ROLE' does not look like a role name"; exit 9; }
    if [ "$(scalar "$dsn" "SELECT count(*) FROM pg_roles WHERE rolname='$APP_ROLE'")" = "0" ]; then
      sql "$dsn" "CREATE ROLE $APP_ROLE LOGIN" || exit 4
      info "role $APP_ROLE created on '$s'"
    fi
    sql "$dsn" "$grant" || { err "failed to issue grants on '$s'"; exit 4; }
    info "GRANT USAGE on schemas — $APP_ROLE"
  fi
done

# ── 6) Регистрация в etcd ────────────────────────────────────────────────────
step "6) Registration in etcd"
now="$(date +%s)"
config_json="$(jq -n --argjson n "$BUCKETS" --arg db "$DBNAME" --argjson ts "$now" \
  '{buckets:$n, dbname:$db, created_unix:$ts}')"
ect put "$(config_key)" "$config_json" >/dev/null
for s in "${SHARD_NAMES[@]}"; do
  ect put "$(shard_key "$s")/dsn" "${SHARD_DSN[$s]}" >/dev/null
  ect put "$(shard_key "$s")/replicas" "$REPLICAS" >/dev/null
done
for (( i = 0; i < BUCKETS; i++ )); do
  ect put "$(routing_key "bucket_$i")" "$(bucket_shard "$i")" >/dev/null
done
info "$(config_key) = $config_json"
info "shards: ${SHARD_NAMES[*]} (dsn + replicas=$REPLICAS)"
echo "  routing: $BUCKETS keys $(cluster_root)/buckets/routing/bucket_0..$((BUCKETS - 1))"

echo
echo "Done: cluster '$CLUSTER_NAME' initialized ($BUCKETS buckets, DB '$DBNAME')."
echo "  Add a shard:            ./add-shard.sh --cluster $CLUSTER_NAME <shard> --dsn '<...>'"
echo "  Migrate a bucket:       ./move-bucket.sh --cluster $CLUSTER_NAME move <bucket> --to <shard>"
echo "  Remove an empty shard:  ./remove-shard.sh --cluster $CLUSTER_NAME <shard>"
