#!/usr/bin/env bash
# scripts/create-bucket.sh
#
# Создать бакет (схему) на шарде и зарегистрировать его в etcd-контрол-плейне
# (/clusters/<C>/buckets/routing/<bucket> → шард).
#
# ⚠️ Штатно ВСЕ бакеты кластера (bucket_0..bucket_<N-1>, N — константа)
# создаёт init-cluster.sh при инициализации, распределяя их по шардам поровну.
# Этот скрипт — утилита точечного восстановления/регистрации: утерянный бакет
# из диапазона 0..N-1, бакет с DDL из файла/шаблона, кластер без init (legacy).
# В инициализированном кластере бакет ВНЕ диапазона будет отклонён.
#
# Использование:
#   ./scripts/create-bucket.sh [--cluster <C>] <bucket> --shard <shard>
#   ./scripts/create-bucket.sh [--cluster <C>] <bucket> --shard <shard> --ddl <file.sql>
#   ./scripts/create-bucket.sh [--cluster <C>] <bucket> --shard <shard> --template <другой bucket на этом же шарде>
#
#   --cluster   кластер (дефолт CLUSTER_NAME из buckets.env)
#   --shard     шард, на котором создать бакет (имя из etcd-реестра или SHARDS)
#   --ddl       файл SQL: применяется после CREATE SCHEMA; объекты должны быть
#               квалифицированы именем схемы (или задай search_path в самом файле)
#   --template  взять структуру существующего бакета на ТОМ ЖЕ шарде
#               (pg_dump --schema-only + замена имени схемы)
#
# Если задан APP_ROLE — выдаёт базовые гранты (USAGE/DML/sequences, §4):
# заморозка P1 на cutover отнимает их, значит до переезда они обязаны быть.
#
# Конфиг: configs/buckets/buckets.env (см. buckets.env.example).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
. "$SCRIPT_DIR/buckets-common.sh"

usage() {
  cat >&2 <<EOF
Usage: $0 [--cluster <C>] <bucket> --shard <shard> [--ddl <file.sql> | --template <bucket>]
  --cluster  cluster in etcd (default CLUSTER_NAME from buckets.env)
  bucket     bucket name = schema name, e.g. bucket_42
  --shard    owner shard (etcd registry or SHARDS in buckets.env)
EOF
  exit 2
}

CLUSTER="" BUCKET="" SHARD="" DDL_FILE="" TEMPLATE=""
while [ $# -gt 0 ]; do
  case "$1" in
    --cluster)  CLUSTER="${2:-}"; shift 2 ;;
    --shard)    SHARD="${2:-}"; shift 2 ;;
    --ddl)      DDL_FILE="${2:-}"; shift 2 ;;
    --template) TEMPLATE="${2:-}"; shift 2 ;;
    -h|--help)  usage ;;
    *)          [ -z "$BUCKET" ] && BUCKET="$1" || usage; shift ;;
  esac
done
[ -n "$CLUSTER" ] && cluster_set "$CLUSTER"
[ -n "$BUCKET" ] && [ -n "$SHARD" ] || usage
[ -z "$DDL_FILE" ] || [ -z "$TEMPLATE" ] || { echo "❌ --ddl and --template are mutually exclusive" >&2; exit 2; }

require_bins psql pg_dump perl etcdctl
valid_bucket "$BUCKET" || { echo "❌ invalid bucket name '$BUCKET' (pattern: ^[a-z][a-z0-9_]*$)" >&2; exit 2; }
valid_shard "$SHARD"   || { echo "❌ unknown shard '$SHARD' (cluster '$CLUSTER_NAME' etcd registry or SHARDS in buckets.env: ${SHARDS})" >&2; exit 2; }
if [ -n "$TEMPLATE" ]; then
  valid_bucket "$TEMPLATE" || { echo "❌ invalid template name '$TEMPLATE'" >&2; exit 2; }
fi
if [ -n "$DDL_FILE" ]; then
  [ -r "$DDL_FILE" ] || { echo "❌ DDL file is not readable: $DDL_FILE" >&2; exit 2; }
fi

etcd_alive

# Guard инициализированного кластера: набор бакетов фиксирован (N — константа,
# P18); штатно все схемы создаёт init-cluster.sh.
cfg="$(cluster_config)"
if [ -n "$cfg" ]; then
  N="$(cfg_field buckets)"
  if [[ "$BUCKET" =~ ^bucket_([0-9]+)$ ]]; then
    idx="${BASH_REMATCH[1]}"
    if [ -z "$N" ] || [ "$idx" -ge "$N" ]; then
      echo "❌ bucket '$BUCKET' is outside cluster '$CLUSTER_NAME' range (N=$N: bucket_0..bucket_$((N - 1)))" >&2
      echo "   Normally all buckets are created by init-cluster.sh; N cannot be extended (P18)." >&2
      exit 3
    fi
  else
    echo "❌ in an initialized cluster bucket names are fixed: bucket_0..bucket_$(( ${N:-?} - 1 ))" >&2
    exit 3
  fi
fi

DSN="$(shard_dsn "$SHARD")"

# ─────────────────────────────────────────────────────────────────────────────
# 1) Проверки: бакета ещё нет ни в etcd, ни на шарде
# ─────────────────────────────────────────────────────────────────────────────
step "Checks"
if [ -n "$(routing_get "$BUCKET")" ]; then
  err "bucket '$BUCKET' is already registered: $(routing_key "$BUCKET") → $(routing_get "$BUCKET")"
  exit 3
fi
[ "$(poll_scalar "$DSN" 'SELECT 1' 3)" = "1" ] || { err "shard '$SHARD' is unreachable"; exit 3; }
schema_exists "$DSN" "$BUCKET" && { err "schema '$BUCKET' already exists on '$SHARD'"; exit 3; }
info "etcd: no bucket; shard '$SHARD' is reachable; no schema"

# ─────────────────────────────────────────────────────────────────────────────
# 2) CREATE SCHEMA + структура
# ─────────────────────────────────────────────────────────────────────────────
step "Creating schema '$BUCKET' on '$SHARD'"
sql "$DSN" "CREATE SCHEMA $BUCKET"

if [ -n "$TEMPLATE" ]; then
  schema_exists "$DSN" "$TEMPLATE" || { err "template '$TEMPLATE' not found on '$SHARD'"; exit 3; }
  echo "  Copying structure from '$TEMPLATE' (pg_dump --schema-only) ..."
  # Замена имени схемы по границе слова. CREATE SCHEMA из дампа выкидываем —
  # схема уже создана выше. Ограничение: если имя шаблона встречается внутри
  # строковых литералов/тел функций — оно тоже будет заменено (проверь DDL сам).
  pg_dump --schema-only --schema="$TEMPLATE" --no-owner --no-privileges "$DSN" \
    | grep -v "^CREATE SCHEMA $TEMPLATE;" \
    | perl -pe "s/\b\Q$TEMPLATE\E\b/$BUCKET/g" \
    | psql "$DSN" -X -q -v ON_ERROR_STOP=1 >/dev/null \
    || { err "failed to apply template DDL; schema '$BUCKET' remains — complete it manually or drop it"; exit 4; }
  info "structure copied from '$TEMPLATE'"
elif [ -n "$DDL_FILE" ]; then
  psql "$DSN" -X -q -v ON_ERROR_STOP=1 -f "$DDL_FILE" \
    || { err "failed to apply $DDL_FILE; schema '$BUCKET' remains — fix the file and apply it again"; exit 4; }
  info "DDL applied from $DDL_FILE"
else
  echo "  --ddl/--template not set: created an empty schema (the app will apply DDL via migrations)"
fi

TABLES="$(scalar "$DSN" "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                         WHERE n.nspname='$BUCKET' AND c.relkind='r'")"
info "tables in '$BUCKET': $TABLES"

# ─────────────────────────────────────────────────────────────────────────────
# 3) Базовые гранты app-роли (§4): до первого переезда права обязаны быть —
#    заморозка P1 на cutover их ОТНИМАЕТ и возвращает при abort/rollback
# ─────────────────────────────────────────────────────────────────────────────
if [ -n "${APP_ROLE:-}" ]; then
  valid_bucket "$APP_ROLE" || { err "APP_ROLE='$APP_ROLE' does not look like a role name (^[a-z][a-z0-9_]*$)"; exit 9; }
  if [ "$(scalar "$DSN" "SELECT count(*) FROM pg_roles WHERE rolname='$APP_ROLE'")" = "0" ]; then
    sql "$DSN" "CREATE ROLE $APP_ROLE LOGIN"
    info "role $APP_ROLE created on '$SHARD'"
  fi
  sql "$DSN" "GRANT USAGE ON SCHEMA $BUCKET TO $APP_ROLE;
              GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA $BUCKET TO $APP_ROLE;
              GRANT USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA $BUCKET TO $APP_ROLE;"
  [ "${APP_GRANT_CREATE:-0}" = 1 ] && sql "$DSN" "GRANT CREATE ON SCHEMA $BUCKET TO $APP_ROLE"
  info "base grants for $APP_ROLE issued"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 4) Регистрация в etcd: routing-ключ = владелец; нет статус-ключа = ACTIVE
# ─────────────────────────────────────────────────────────────────────────────
step "Registering in etcd"
ect put "$(routing_key "$BUCKET")" "$SHARD" >/dev/null
info "done: $(routing_key "$BUCKET") → $SHARD (ACTIVE)"

echo
echo "Next: the app router reads routing from etcd (watch) — and watch the DDL migrations:"
echo "  ./scripts/move-bucket.sh status $BUCKET"
