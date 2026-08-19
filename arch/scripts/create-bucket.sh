#!/usr/bin/env bash
# scripts/create-bucket.sh
#
# Создать бакет (схему) на шарде и зарегистрировать его в каталоге.
# См. arch/11-bucket-sharding.md §2, §4.
#
# Использование:
#   ./scripts/create-bucket.sh <bucket> --shard <shard>                    # пустая схема
#   ./scripts/create-bucket.sh <bucket> --shard <shard> --ddl <file.sql>   # схема + DDL из файла
#   ./scripts/create-bucket.sh <bucket> --shard <shard> --template <другой bucket на этом же шарде>
#
#   --shard     шард, на котором создать бакет (имя из SHARDS в buckets.env)
#   --ddl       файл SQL: применяется после CREATE SCHEMA; объекты должны быть
#               квалифицированы именем схемы (или задай search_path в самом файле)
#   --template  взять структуру существующего бакета на ТОМ ЖЕ шарде
#               (pg_dump --schema-only + замена имени схемы)
#
# Конфиг: configs/buckets/buckets.env (см. buckets.env.example).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
. "$SCRIPT_DIR/buckets-common.sh"

usage() {
  cat >&2 <<EOF
Usage: $0 <bucket> --shard <shard> [--ddl <file.sql> | --template <bucket>]
  bucket   имя нового бакета = имя схемы, напр. bucket_42
  --shard  шард-владелец (из SHARDS в buckets.env)
EOF
  exit 2
}

BUCKET="" SHARD="" DDL_FILE="" TEMPLATE=""
while [ $# -gt 0 ]; do
  case "$1" in
    --shard)     SHARD="${2:-}"; shift 2 ;;
    --ddl)       DDL_FILE="${2:-}"; shift 2 ;;
    --template)  TEMPLATE="${2:-}"; shift 2 ;;
    -h|--help)   usage ;;
    *)           [ -z "$BUCKET" ] && BUCKET="$1" || usage; shift ;;
  esac
done
[ -n "$BUCKET" ] && [ -n "$SHARD" ] || usage
[ -z "$DDL_FILE" ] || [ -z "$TEMPLATE" ] || { echo "❌ --ddl и --template взаимоисключимы" >&2; exit 2; }

require_bins
valid_bucket "$BUCKET" || { echo "❌ неверное имя бакета '$BUCKET' (шаблон: ^[a-z][a-z0-9_]*$)" >&2; exit 2; }
valid_shard "$SHARD"   || { echo "❌ неизвестный шард '$SHARD' (SHARDS в buckets.env: ${SHARDS})" >&2; exit 2; }
if [ -n "$TEMPLATE" ]; then
  valid_bucket "$TEMPLATE" || { echo "❌ неверное имя шаблона '$TEMPLATE'" >&2; exit 2; }
fi
if [ -n "$DDL_FILE" ]; then
  [ -r "$DDL_FILE" ] || { echo "❌ файл DDL не читается: $DDL_FILE" >&2; exit 2; }
fi

catalog_check_table
DSN="$(shard_dsn "$SHARD")"

# ─────────────────────────────────────────────────────────────────────────────
# 1) Проверки: бакета ещё нет ни в каталоге, ни на шарде
# ─────────────────────────────────────────────────────────────────────────────
step "Проверки"
[ -z "$(catalog_row "$BUCKET")" ] || { err "бакет '$BUCKET' уже есть в каталоге: $(catalog_row "$BUCKET")"; exit 3; }
schema_exists "$DSN" "$BUCKET" && { err "схема '$BUCKET' уже существует на '$SHARD'"; exit 3; }
[ "$(scalar "$DSN" 'SELECT 1')" = "1" ] || { err "шард '$SHARD' недоступен"; exit 3; }
info "каталог: бакета нет; шард '$SHARD' доступен; схемы нет"

# ─────────────────────────────────────────────────────────────────────────────
# 2) CREATE SCHEMA + структура
# ─────────────────────────────────────────────────────────────────────────────
step "Создаю схему '$BUCKET' на '$SHARD'"
sql "$DSN" "CREATE SCHEMA $BUCKET"

if [ -n "$TEMPLATE" ]; then
  schema_exists "$DSN" "$TEMPLATE" || { err "шаблон '$TEMPLATE' не найден на '$SHARD'"; exit 3; }
  echo "  Копирую структуру из '$TEMPLATE' (pg_dump --schema-only) ..."
  # Замена имени схемы по границе слова. CREATE SCHEMA из дампа выкидываем —
  # схема уже создана выше. Ограничение: если имя шаблона встречается внутри
  # строковых литералов/тел функций — оно тоже будет заменено (проверь DDL сам).
  pg_dump --schema-only --schema="$TEMPLATE" --no-owner --no-privileges "$DSN" \
    | grep -v "^CREATE SCHEMA $TEMPLATE;" \
    | perl -pe "s/\b\Q$TEMPLATE\E\b/$BUCKET/g" \
    | psql "$DSN" -X -q -v ON_ERROR_STOP=1 >/dev/null \
    || { err "не удалось применить DDL из шаблона; схема '$BUCKET' осталась — дозаполни вручную или удали"; exit 4; }
  info "структура скопирована из '$TEMPLATE'"
elif [ -n "$DDL_FILE" ]; then
  psql "$DSN" -X -q -v ON_ERROR_STOP=1 -f "$DDL_FILE" \
    || { err "не удалось применить $DDL_FILE; схема '$BUCKET' осталась — исправь файл и накати повторно"; exit 4; }
  info "DDL применён из $DDL_FILE"
else
  echo "  --ddl/--template не заданы: создана пустая схема (DDL накатит приложение-миграциями)"
fi

TABLES="$(scalar "$DSN" "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                         WHERE n.nspname='$BUCKET' AND c.relkind='r'")"
info "таблиц в '$BUCKET': $TABLES"

# ─────────────────────────────────────────────────────────────────────────────
# 3) Регистрация в каталоге
# ─────────────────────────────────────────────────────────────────────────────
step "Регистрирую в каталоге"
catalog_sql "INSERT INTO buckets(bucket_id, shard_id, state) VALUES ('$BUCKET', '$SHARD', 'ACTIVE')"
info "готово: $BUCKET → $SHARD (ACTIVE)"

echo
echo "Дальше: маршрутизируй в приложение (кэш каталога) — и следи за DDL-миграциями:"
echo "  ./scripts/move-bucket.sh status $BUCKET"
