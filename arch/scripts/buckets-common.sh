#!/usr/bin/env bash
# scripts/buckets-common.sh
#
# Общие функции для скриптов бакетного шардирования (create-bucket.sh,
# move-bucket.sh). Сам по себе не запускается.
#
# Конфиг: configs/buckets/buckets.env (переопределяется через BUCKETS_ENV).
# См. arch/11-bucket-sharding.md.

BUCKETS_ENV="${BUCKETS_ENV:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../configs/buckets" && pwd)/buckets.env}"
if [ ! -r "$BUCKETS_ENV" ]; then
  echo "❌ ОШИБКА: не найден конфиг $BUCKETS_ENV" >&2
  echo "  Скопируй configs/buckets/buckets.env.example → buckets.env и заполни адреса." >&2
  exit 9
fi
# shellcheck disable=SC1090
. "$BUCKETS_ENV"

FREEZE_WAIT_SEC="${FREEZE_WAIT_SEC:-5}"
CUTOVER_TIMEOUT_SEC="${CUTOVER_TIMEOUT_SEC:-90}"
POLL_INTERVAL_SEC="${POLL_INTERVAL_SEC:-2}"
SEQ_MARGIN="${SEQ_MARGIN:-1000}"

info() { echo "✓ $*"; }
step() { echo; echo ">>> $*"; }
err()  { echo "❌ ОШИБКА: $*" >&2; }

# Бинарки, нужные на машине запуска
require_bins() {
  local b
  for b in psql pg_dump perl; do
    command -v "$b" >/dev/null 2>&1 || { err "не найден '$b' (нужен на машине запуска)"; exit 9; }
  done
}

# Имя шарда валидно и известно в конфиге
valid_shard() {
  [[ "$1" =~ ^[a-z][a-z0-9_]*$ ]] || return 1
  local s
  for s in $SHARDS; do [ "$s" = "$1" ] && return 0; done
  return 1
}

shard_dsn() { # $1 = имя шарда → DSN для psql/pg_dump с ops-хоста
  local v="SHARD_${1}_DSN"
  local d="${!v:-}"
  [ -n "$d" ] || { err "для шарда '$1' не задан ${v} в buckets.env"; exit 9; }
  printf '%s' "$d"
}

mover_conninfo() { # $1 = имя шарда → conninfo, по которому ДРУГИЕ шарды подписываются на него
  local v="MOVER_CONNINFO_${1}"
  local d="${!v:-}"
  [ -n "$d" ] || { err "для шарда '$1' не задан ${v} в buckets.env (нужен для CREATE SUBSCRIPTION)"; exit 9; }
  printf '%s' "$d"
}

valid_bucket() { [[ "$1" =~ ^[a-z][a-z0-9_]*$ ]]; }

# SQL-хелперы. Пароли лежат в DSN — не выводим DSN в логи сами, psql их не печатает.
sql()    { psql "$1" -X -q -v ON_ERROR_STOP=1 -c "$2"; }   # выполнить (тихо)
scalar() { psql "$1" -X -qAt -v ON_ERROR_STOP=1 -c "$2"; } # одно значение

# ── Каталог (meta) ───────────────────────────────────────────────────────────

catalog_check_table() {
  [ "$(scalar "$BUCKET_CATALOG_DSN" "SELECT to_regclass('buckets') IS NOT NULL")" = "t" ] || {
    err "в каталоге ($BUCKET_CATALOG_DSN) нет таблицы buckets"
    echo "  Создай её (arch/11-bucket-sharding.md §2):" >&2
    cat >&2 <<'SQL'
CREATE TABLE buckets (
    bucket_id       text PRIMARY KEY,
    shard_id        text NOT NULL,
    target_shard_id text,
    state           text NOT NULL DEFAULT 'ACTIVE'
                    CHECK (state IN ('ACTIVE', 'SYNCING', 'FROZEN')),
    updated_at      timestamptz NOT NULL DEFAULT now()
);
SQL
    exit 9
  }
}

# catalog_row <bucket> → "shard_id|state|target_shard_id" (пусто, если бакета нет)
catalog_row() {
  scalar "$BUCKET_CATALOG_DSN" \
    "SELECT shard_id||'|'||state||'|'||coalesce(target_shard_id,'')
     FROM buckets WHERE bucket_id='$1'"
}

catalog_sql() { sql "$BUCKET_CATALOG_DSN" "$1"; }

# ── Полезные запросы по шардам ──────────────────────────────────────────────

schema_exists() { # <dsn> <schema> → t/f
  [ "$(scalar "$1" "SELECT to_regnamespace('$2') IS NOT NULL")" = "t" ]
}

pub_exists() { [ "$(scalar "$1" "SELECT count(*) FROM pg_publication WHERE pubname='$2'")" -gt 0 ]; }

sub_exists() { [ "$(scalar "$1" "SELECT count(*) FROM pg_subscription WHERE subname='$2'")" -gt 0 ]; }

# sub_sync <dsn> <subname> → "готово/всего" по состояниям таблиц подписки
sub_sync() {
  scalar "$1" "SELECT coalesce(sum((srsubstate='r')::int),0)||'/'||count(*)
               FROM pg_subscription_rel
               WHERE srsubid=(SELECT oid FROM pg_subscription WHERE subname='$2')"
}

# slot_lag <dsn> <slot> → отставание слота в байтах (0, если слота нет)
slot_lag() {
  scalar "$1" "SELECT coalesce(max(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)),0)::bigint
               FROM pg_replication_slots WHERE slot_name='$2'"
}

# slot_caught_up <dsn> <slot> <lsn> → t, если слот активен и подтвердил <lsn>
slot_caught_up() {
  [ "$(scalar "$1" "SELECT coalesce(bool_and(active AND confirmed_flush_lsn >= '$3'::pg_lsn), false)
                    FROM pg_replication_slots WHERE slot_name='$2'")" = "t" ]
}

# Синхронизация sequences на шард-приёмнике: печатает и выполняет setval для всех
# последовательностей схемы (serial deptype='a' и IDENTITY deptype='i').
sync_sequences() { # <dsn> <schema>
  local n
  n="$(scalar "$1" "SELECT count(*) FROM pg_class s
                    JOIN pg_namespace n ON n.oid=s.relnamespace
                    WHERE s.relkind='S' AND n.nspname='$2'")"
  if [ "$n" -eq 0 ]; then info "sequences в '$2' нет — пропускаю"; return 0; fi
  scalar "$1" "SELECT format('SELECT setval(%L, (SELECT coalesce(max(%I),0) FROM %I.%I) + $SEQ_MARGIN);',
                    format('%I.%I', n.nspname, s.relname), a.attname, n.nspname, t.relname)
               FROM pg_class s
               JOIN pg_depend d ON d.objid=s.oid AND d.deptype IN ('a','i')
                  AND d.classid='pg_class'::regclass AND d.refclassid='pg_class'::regclass
               JOIN pg_class t ON t.oid=d.refobjid
               JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=d.refobjsubid
               JOIN pg_namespace n ON n.oid=t.relnamespace
               WHERE s.relkind='S' AND n.nspname='$2'" \
  | psql "$1" -X -qAt -v ON_ERROR_STOP=1
  info "sequences синхронизированы ($n шт., запас +$SEQ_MARGIN)"
}
