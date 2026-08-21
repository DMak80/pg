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

  --cluster   имя создаваемого кластера (префикс /clusters/<C>/ в etcd)
  --buckets   N — число бакетов, КОНСТАНТА на всю жизнь кластера (P18)
  --dbname    имя базы PG (одна на кластер, создаётся заранее на шардах)
  --replicas  декларативное число реплик шарда (дефолт 1; фактическое
              настраивается в Patroni каждого шарда)
  --shard     имя шарда = DSN его write-эндпоинта (без пароля; повторяемый).
              DSN должен содержать host=, dbname= и user=. Пароль —
              SHARD_<X>_PASSWORD в buckets.env, в etcd не попадает.
EOF
  exit 2
}

confirm() {
  [ "$ASSUME_YES" = 1 ] && return 0
  local a=""
  read -rp "$1 [введите YES]: " a
  [ "${a:-}" = "YES" ] || { echo "Отменено."; exit 1; }
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

[[ "$BUCKETS" =~ ^[1-9][0-9]*$ ]] || { err "--buckets: целое > 0 (получили '$BUCKETS')"; exit 2; }
REPLICAS="${REPLICAS:-1}"
[[ "$REPLICAS" =~ ^[0-9]+$ ]] || { err "--replicas: целое >= 0"; exit 2; }
valid_dbname "$DBNAME" || { err "неверное имя БД '$DBNAME' (шаблон: ^[a-z_][a-z0-9_]*$)"; exit 2; }

# ── Шарды: разбор name=dsn, валидация имён/уникальности/DSN ───────────────────
SHARD_NAMES=()
declare -A SHARD_DSN=()
for a in "${SHARD_ARGS[@]}"; do
  name="${a%%=*}"; dsn="${a#*=}"
  [ "$name" != "$dsn" ] || { err "--shard: ожидает <имя>=<DSN> (получили '$a')"; exit 2; }
  [[ "$name" =~ ^[a-z][a-z0-9_]*$ ]] || { err "неверное имя шарда '$name'"; exit 2; }
  [ -z "${SHARD_DSN[$name]:-}" ] || { err "шард '$name' указан дважды"; exit 2; }
  dsn="$(dsn_strip_password "$dsn")"
  grep -qE '(^| )host=' <<<"$dsn" && grep -qE '(^| )dbname=' <<<"$dsn" && grep -qE '(^| )user=' <<<"$dsn" \
    || { err "DSN шарда '$name' обязан содержать host=, dbname= и user=: '$dsn'"; exit 2; }
  SHARD_NAMES+=("$name"); SHARD_DSN["$name"]="$dsn"
done
S_COUNT="${#SHARD_NAMES[@]}"

# ── 1) Guards контрол-плейна ─────────────────────────────────────────────────
step "1) Проверки etcd"
etcd_alive
if [ -n "$(etcd_prefix_keys "$(cluster_root)")" ]; then
  err "префикс $(cluster_root) не пуст — кластер '$CLUSTER_NAME' уже инициализирован (или её остатки)."
  echo "  Константы неизменяемы (P18). Полуинициализированный префикс (схем ещё нет):" >&2
  echo "    etcdctl del $(cluster_root) --prefix   — и повтори init." >&2
  exit 3
fi
info "префикс $(cluster_root) пуст"

# ── 2) Шарды доступны, БД существует ─────────────────────────────────────────
step "2) Проверка доступности шардов и БД '$DBNAME'"
for s in "${SHARD_NAMES[@]}"; do
  dsn="$(dsn_with_password "${SHARD_DSN[$s]}" "$s")"
  if [ "$(poll_scalar "$dsn" 'SELECT 1' 3)" != "1" ]; then
    err "шард '$s' недоступен по DSN (пароль: SHARD_${s}_PASSWORD в buckets.env, если нужен)"
    exit 3
  fi
  info "шард '$s': доступен ($(grep -oE 'host=[^ ]+' <<<"${SHARD_DSN[$s]}"))"
done

# ── 3) Схемы bucket_0..N-1: существующих (непустых) быть не должно ────────────
step "3) Проверка схем bucket_0..bucket_$((BUCKETS - 1))"
bucket_shard() { printf '%s' "${SHARD_NAMES[$(( $1 % S_COUNT ))]}"; }
for s in "${SHARD_NAMES[@]}"; do
  clash="$(scalar "$(dsn_with_password "${SHARD_DSN[$s]}" "$s")" \
    "SELECT string_agg(format('%s (%s объектов)', n.nspname,
            (SELECT count(*) FROM pg_class c WHERE c.relnamespace=n.oid)), ', ')
     FROM pg_namespace n
     WHERE n.nspname LIKE 'bucket_%' AND n.nspname ~ '^bucket_[0-9]+$'
       AND substr(n.nspname, 8)::int < $BUCKETS")" || clash=""
  [ -z "$clash" ] || {
    err "на '$s' уже есть схемы диапазона: $clash"
    echo "  Инициализируем чистые схемы: очисти их или выбери другое имя кластера/БД." >&2
    exit 3
  }
done
info "существующих схем диапазона нет"

# ── 4) План ──────────────────────────────────────────────────────────────────
per=$(( BUCKETS / S_COUNT )); rem=$(( BUCKETS % S_COUNT ))
step "4) План"
echo "  кластер:     $CLUSTER_NAME  (etcd: $(cluster_root))"
echo "  константы:   бакетов N=$BUCKETS (bucket_0..bucket_$((BUCKETS - 1))), БД '$DBNAME'"
echo "  шарды:       $S_COUNT, декларативно реплик: $REPLICAS"
for s in "${SHARD_NAMES[@]}"; do
  echo "    $s: ${SHARD_DSN[$s]}"
done
echo "  распределение поровну (round-robin):"
for i in "${!SHARD_NAMES[@]}"; do
  s="${SHARD_NAMES[$i]}"; n="$per"; [ "$i" -lt "$rem" ] && n=$((per + 1))
  echo "    $s: $n бакет(ов)"
done
if [ -n "${APP_ROLE:-}" ]; then
  echo "  app-роль:    $APP_ROLE (CREATE ROLE если нет + GRANT USAGE на схемы)"
else
  echo "  app-роль:    не задана (гранты выдашь позже по §4 доки 11)"
fi
confirm "Инициализировать кластер '$CLUSTER_NAME'?"

# ── 5) Схемы + гранты (идемпотентно) ─────────────────────────────────────────
step "5) Создание схем и грантов"
for s in "${SHARD_NAMES[@]}"; do
  dsn="$(dsn_with_password "${SHARD_DSN[$s]}" "$s")"
  ddl=""; grant=""
  for (( i = 0; i < BUCKETS; i++ )); do
    [ "$(bucket_shard "$i")" = "$s" ] || continue
    ddl+="CREATE SCHEMA IF NOT EXISTS bucket_$i; "
    grant+="GRANT USAGE ON SCHEMA bucket_$i TO $APP_ROLE; "
  done
  sql "$dsn" "$ddl" || { err "не удалось создать схемы на '$s'"; exit 4; }
  info "схемы на '$s' готовы"
  if [ -n "${APP_ROLE:-}" ]; then
    valid_bucket "$APP_ROLE" || { err "APP_ROLE='$APP_ROLE' не похоже на имя роли"; exit 9; }
    if [ "$(scalar "$dsn" "SELECT count(*) FROM pg_roles WHERE rolname='$APP_ROLE'")" = "0" ]; then
      sql "$dsn" "CREATE ROLE $APP_ROLE LOGIN" || exit 4
      info "роль $APP_ROLE создана на '$s'"
    fi
    sql "$dsn" "$grant" || { err "не удалось выдать гранты на '$s'"; exit 4; }
    info "GRANT USAGE на схемы — $APP_ROLE"
  fi
done

# ── 6) Регистрация в etcd ────────────────────────────────────────────────────
step "6) Регистрация в etcd"
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
echo "  routing: $BUCKETS ключей $(cluster_root)/buckets/routing/bucket_0..$((BUCKETS - 1))"

echo
echo "Готово: кластер '$CLUSTER_NAME' инициализирован ($BUCKETS бакетов, БД '$DBNAME')."
echo "  Подключить шард:        ./add-shard.sh --cluster $CLUSTER_NAME <шард> --dsn '<...>'"
echo "  Мигрировать бакет:      ./move-bucket.sh --cluster $CLUSTER_NAME move <бакет> --to <шард>"
echo "  Снять пустой шард:      ./remove-shard.sh --cluster $CLUSTER_NAME <шард>"
