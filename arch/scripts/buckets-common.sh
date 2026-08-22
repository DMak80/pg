#!/usr/bin/env bash
# scripts/buckets-common.sh
#
# Общие функции для скриптов бакетного шардирования (init-cluster.sh,
# add-shard.sh, remove-shard.sh, create-bucket.sh, move-bucket.sh,
# abort-move.sh). Сам по себе не запускается.
#
# Один etcd обслуживает НЕСКОЛЬКО независимых шардированных кластеров:
# всё состояние системы живёт под её префиксом (12-bucket-pitfalls.md,
# «Референс топологии»):
#   /clusters/<C>/config          → {"buckets":N,"dbname":"app"}  — константы init
#   /clusters/<C>/shards/X/dsn    → "host=... dbname=..."  — вход шарда, БЕЗ пароля
#   /clusters/<C>/shards/X/replicas → число реплик (декларативно)
#   /clusters/<C>/shards/X/master → "host:6432"  — lease/TTL, Patroni-callback
#   /clusters/<C>/buckets/routing/<bucket> → "shard1"  — владелец (авторитет)
#   /clusters/<C>/buckets/status/<bucket>  → {"state":...} — только при
#     переезде; нет ключа = ACTIVE.
# Кластер (система) выбирается --cluster у каждого скрипта, дефолт —
# CLUSTER_NAME из buckets.env. Patroni живёт отдельно в /service/<scope>/.
#
# Конфиг: configs/buckets/buckets.env (переопределяется через BUCKETS_ENV):
# адреса etcd, CLUSTER_NAME, роли и ПАРОЛИ (в etcd паролей нет — только DSN).
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
FREEZE_LOCK_TIMEOUT="${FREEZE_LOCK_TIMEOUT:-5s}"   # lock_timeout барьера P1
FREEZE_LOCK_TRIES="${FREEZE_LOCK_TRIES:-3}"
CUTOVER_TIMEOUT_SEC="${CUTOVER_TIMEOUT_SEC:-90}"
POLL_INTERVAL_SEC="${POLL_INTERVAL_SEC:-2}"
CONN_FAIL_BUDGET_SEC="${CONN_FAIL_BUDGET_SEC:-120}" # бюджет недоступности шарда в циклах ожидания (P8: failover приёмника)
SUB_SYNCCOMMIT="${SUB_SYNCCOMMIT:-remote_apply}"    # P8: synccommit подписок переезда
ETCD_ENDPOINTS="${ETCD_ENDPOINTS:-http://127.0.0.1:2379}"
ABORT_MIN_AGE_SEC="${ABORT_MIN_AGE_SEC:-120}"

# Имя кластера (шардированной системы): дефолт из buckets.env, перекрывается
# опцией --cluster каждого скрипта (cluster_set).
CLUSTER_NAME="${CLUSTER_NAME:-}"

info() { echo "✓ $*"; }
step() { echo; echo ">>> $*"; }
err()  { echo "❌ ОШИБКА: $*" >&2; }

# Бинарки, нужные на машине запуска (ops-боксе)
require_bins() { # require_bins psql pg_dump ... — выход 9, если чего-то нет
  local b
  for b in "$@"; do
    command -v "$b" >/dev/null 2>&1 || { err "не найден '$b' (нужен на машине запуска)"; exit 9; }
  done
}

# ── Кластер = шардированная система, префикс в etcd ───────────────────────────

valid_cluster() { [[ "$1" =~ ^[a-z][a-z0-9_-]*$ ]]; }

cluster_set() { # $1 = значение --cluster: валидация + перекрытие env-дефолта
  CLUSTER_NAME="$1"
  valid_cluster "$CLUSTER_NAME" \
    || { err "неверное имя кластера '$CLUSTER_NAME' (шаблон: ^[a-z][a-z0-9_-]*$)"; exit 2; }
}

cluster_root() { # префикс кластера в etcd; отказ, если кластер не выбран
  [ -n "${CLUSTER_NAME:-}" ] \
    || { err "кластер не задан: передай --cluster или CLUSTER_NAME в buckets.env"; exit 9; }
  printf '/clusters/%s' "$CLUSTER_NAME"
}

config_key()  { printf '%s/config' "$(cluster_root)"; }
shard_key()   { printf '%s/shards/%s' "$(cluster_root)" "$1"; }
routing_key() { printf '%s/buckets/routing/%s' "$(cluster_root)" "$1"; }
status_key()  { printf '%s/buckets/status/%s' "$(cluster_root)" "$1"; }

cluster_config() { # JSON конфига кластера (пусто = кластер не инициализирован)
  etcd_value "$(config_key)"
}

cfg_field() { # $1 = поле config (buckets|dbname) → значение (пусто = нет)
  jstr ".$1" "$(cluster_config)"
}

# Имя шарда валидно и известно: зарегистрировано в etcd кластера (dsn-ключ)
# или описано в buckets.env (legacy-режим до init-cluster.sh)
valid_shard() {
  [[ "$1" =~ ^[a-z][a-z0-9_]*$ ]] || return 1
  [ -n "$(shard_dsn_base "$1")" ] && return 0
  local s
  for s in ${SHARDS:-}; do [ "$s" = "$1" ] && return 0; done
  return 1
}

shards_list() { # шарды кластера из etcd (по dsn-ключам), построчно
  etcd_prefix_keys "$(cluster_root)/shards/" 2>/dev/null \
    | sed -nE 's|^.*/shards/([^/]+)/dsn$|\1|p' | sort -u
}

cluster_shards() { # имена шардов для обхода: etcd-реестр, fallback — SHARDS env
  local l
  l="$(shards_list)"
  if [ -n "$l" ]; then printf '%s\n' "$l"; return; fi
  [ -n "${SHARDS:-}" ] && printf '%s\n' $SHARDS
}

# DSN шарда: канонический вход в etcd (БЕЗ пароля — пароли в etcd не хранятся,
# P12/P17), пароль подставляется из SHARD_<X>_PASSWORD в buckets.env.
# Для подписок переездов роль меняется на MOVER_USER_<X> (дефолт bucket_mover).
# Legacy: пока шарда нет в etcd — берём SHARD_<X>_DSN / MOVER_CONNINFO_<X> из
# buckets.env (кластеры, созданные до введения /clusters/<C>/).
shard_dsn_base() { # $1 = шард → dsn без пароля (etcd → env), пусто = неизвестен
  local d v
  d="$(etcd_value "$(shard_key "$1")/dsn" 2>/dev/null)"
  if [ -z "$d" ]; then
    v="SHARD_${1}_DSN"; d="${!v:-}"
    [ -n "$d" ] && d="$(dsn_strip_password "$d")"
  fi
  printf '%s' "$d"
}

dsn_strip_password() { # убрать password= из DSN (для записи в etcd/логи)
  sed -E 's/(^| )password=[^ ]*/\1/g' <<<"$1" | sed -E 's/ +$//'
}

dsn_with_password() { # $1 = dsn без пароля, $2 = шард → + SHARD_<X>_PASSWORD
  local v pw
  v="SHARD_${2}_PASSWORD"; pw="${!v:-}"
  [ -n "$pw" ] && printf '%s password=%s' "$1" "$pw" || printf '%s' "$1"
}

shard_dsn() { # $1 = имя шарда → полная DSN для psql/pg_dump (write-эндпоинт)
  local d
  d="$(shard_dsn_base "$1")"
  [ -n "$d" ] \
    || { err "для шарда '$1' нет DSN: ни в etcd ($(shard_key "$1")/dsn), ни SHARD_${1}_DSN в buckets.env"; exit 9; }
  dsn_with_password "$d" "$1"
}

mover_conninfo() { # $1 = имя шарда → conninfo, по которому ДРУГИЕ шарды подписываются на него
  local d v u
  d="$(etcd_value "$(shard_key "$1")/dsn" 2>/dev/null)"
  if [ -n "$d" ]; then
    v="MOVER_USER_${1}"; u="${!v:-bucket_mover}"
    d="$(sed -E "s/(^| )user=[^ ]*/\\1user=$u/" <<<"$d")"
    v="MOVER_PASSWORD_${1}"
    if [ -n "${!v:-}" ]; then d="$d password=${!v}"; fi
    printf '%s' "$d"
    return
  fi
  v="MOVER_CONNINFO_${1}"
  local d2="${!v:-}"
  [ -n "$d2" ] || { err "для шарда '$1' не задан ${v} в buckets.env (нужен для CREATE SUBSCRIPTION)"; exit 9; }
  printf '%s' "$d2"
}

valid_bucket() { [[ "$1" =~ ^[a-z][a-z0-9_]*$ ]]; }

valid_dbname() { [[ "$1" =~ ^[a-z_][a-z0-9_]*$ ]]; }

# SQL-хелперы. Пароли лежат в DSN — не выводим DSN в логи сами, psql их не печатает.
sql()    { psql "$1" -X -q -v ON_ERROR_STOP=1 -c "$2"; }   # выполнить (тихо)
scalar() { psql "$1" -X -qAt -v ON_ERROR_STOP=1 -c "$2"; } # одно значение

# Транзиенто-толерантный опрос (P8: у приёмника может идти failover — обрывы
# соединения в циклах ожидания не должны убивать mover). Возвращает значение
# или код 1 после max_fail подряд неудач (каждая с паузой POLL_INTERVAL_SEC).
poll_scalar() { # <dsn> <sql> [max_fail]
  local tries=0 max="${3:-$(( CONN_FAIL_BUDGET_SEC / POLL_INTERVAL_SEC ))}"
  local out
  while :; do
    if out="$(scalar "$1" "$2" 2>/dev/null)"; then printf '%s' "$out"; return 0; fi
    tries=$((tries + 1))
    if [ "$tries" -ge "$max" ]; then return 1; fi
    sleep "$POLL_INTERVAL_SEC"
  done
}

# ── etcd-контрол-плейн ────────────────────────────────────────────────────────
# Чтение — через -w json + jq: значения в json-выводе лежат в base64, зато
# разбор не зависит от версии etcdctl (у --print-value-only история сложнее).
ect() { ETCDCTL_API=3 etcdctl --endpoints="$ETCD_ENDPOINTS" --command-timeout="${ETCD_TIMEOUT_SEC:-5}s" "$@"; }

etcd_value() { # <ключ> → значение (пусто = ключа нет)
  ect get "$1" -w json | jq -r '.kvs[0].value // "" | @base64d'
}

etcd_key_exists() { [ "$(ect get "$1" -w json | jq '.kvs | length')" -gt 0 ]; }

etcd_prefix_keys() { # <префикс> → ключи построчно
  ect get "$1" --prefix -w json | jq -r '.kvs[]?.key | @base64d'
}

etcd_alive() {
  ect get / --prefix --limit=1 -w json >/dev/null 2>&1 \
    || { err "etcd недоступен: $ETCD_ENDPOINTS"; exit 9; }
}

# ── P12: снапшоты контрол-плейна ──────────────────────────────────────────────
# etcdctl snapshot save пишет файл НА КЛИЕНТЕ (ops-боксе): каталог SNAPSHOT_DIR
# обязан быть persistence (volume) — иначе снапшоты умрут вместе с боксом.
# Снапшот физический, покрывает ВЕСЬ etcd (все кластеры): кластер в имени
# файла — только метка для удобства поиска. Восстановление/сверка —
# restore-cluster.sh.
etcd_snapshot() { # <label> → имя файла в выводе; код 1 = не снялся
  local dir="${SNAPSHOT_DIR:-/var/lib/etcd-snapshots}" f sz
  if [ ! -d "$dir" ]; then
    echo "⚠️ P12: нет каталога снапшотов '$dir' (создай / укажи SNAPSHOT_DIR)"
    return 1
  fi
  # .snapshot — суффикс ставим сами: etcdctl пишет ровно по заданному пути
  f="$dir/snap-${CLUSTER_NAME:-default}-$(date -u +%Y%m%dT%H%M%SZ)-$1.snapshot"
  if ! ect snapshot save "$f" >/dev/null 2>&1; then
    echo "⚠️ P12: etcdctl snapshot save НЕ удался (etcd недоступен?)"
    return 1
  fi
  sz="$(ect snapshot status -w json "$f" 2>/dev/null | jq -r '.totalSize // "?"')"
  info "P12: снапшот контрол-плейна → $f (${sz} байт)"
}

jstr() { [ -n "${2:-}" ] && jq -r "$1 // empty" <<<"$2" 2>/dev/null || true; }

routing_get() { # <bucket> → шард-владелец (пусто = ключа нет, P12)
  etcd_value "$(routing_key "$1")"
}

status_get() { # <bucket> → JSON статуса (пусто = ACTIVE)
  etcd_value "$(status_key "$1")"
}

# ── Полезные запросы по шардам ────────────────────────────────────────────────

schema_exists() { # <dsn> <schema> → t/f
  [ "$(scalar "$1" "SELECT to_regnamespace('$2') IS NOT NULL")" = "t" ]
}

pub_exists() { [ "$(scalar "$1" "SELECT count(*) FROM pg_publication WHERE pubname='$2'")" -gt 0 ]; }

sub_exists() { [ "$(scalar "$1" "SELECT count(*) FROM pg_subscription WHERE subname='$2'")" -gt 0 ]; }

slot_exists() { [ "$(scalar "$1" "SELECT count(*) FROM pg_replication_slots WHERE slot_name='$2'")" -gt 0 ]; }

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

# ── P8: предусловие remote_apply ──────────────────────────────────────────────
# Подписки переезда создаются с synchronous_commit=$SUB_SYNCCOMMIT (remote_apply):
# коммит применённой транзакции ждёт, пока реплика приёмника её проиграет, —
# тогда feedback (confirmed_flush) не убегает вперёд физической репликации
# приёмника, и failover приёмника не может молча пропустить срез изменений.
# remote_apply работает только при живом sync-standby у мастера приёмника
# (без него синхронная фиксация тихо вырождается в асинхронную).
check_sync_standby() { # <dsn> <shard> — отказ (exit 3), если sync-standby нет
  local names n
  names="$(poll_scalar "$1" 'SHOW synchronous_standby_names' 3)" || true
  if [ -z "$names" ]; then
    err "на приёмнике '$2' пусто synchronous_standby_names — remote_apply подписки (P8) будет вырожден в асинхронность."
    echo "  Настрой кластер приёмника: synchronous_standby_names + живая реплика (11-bucket-sharding.md §4)." >&2
    return 1
  fi
  n="$(poll_scalar "$1" "SELECT count(*) FROM pg_stat_replication WHERE sync_state IN ('sync','quorum')" 3)"
  if [ "$n" -lt 1 ]; then
    err "у мастера приёмника '$2' нет подключённого sync-standby (pg_stat_replication пуст) — remote_apply (P8) не защищает."
    echo "  Переезд требует здоровых ОБЕИХ кластеров: верни реплику приёмника и повтори." >&2
    return 1
  fi
  info "P8: у приёмника '$2' есть sync-standby — подписка будет с synchronous_commit=$SUB_SYNCCOMMIT"
}

# ── P5/P8: сверка инвентаря и строк ───────────────────────────────────────────

# Инвентарь схемы (таблицы/sequence/views) одним списком — для сверки DDL (P5)
schema_inventory() { # <dsn> <schema> → "relkind|relname" построчно
  scalar "$1" "SELECT c.relkind::text||'|'||c.relname
               FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
               WHERE n.nspname='$2' AND c.relkind IN ('r','S','v','m','p')
               ORDER BY c.relname, 1"
}

# П8-барьер перед flip: построчные count(*) всех таблиц схемы на источнике
# и приёмнике. Ловит «тихий пропуск» изменений после failover приёмника
# (лаг слота при этом выглядит нулевым). Вывод — строки расхождений
# "таблица: src=N dst=M"; код 0 = расхождений нет.
verify_row_counts() { # <src_dsn> <dst_dsn> <schema>
  local src="$1" dst="$2" sch="$3" t c1 c2 bad=0
  while IFS= read -r t; do
    [ -n "$t" ] || continue
    c1="$(poll_scalar "$src" "SELECT count(*) FROM $sch.$t" 5)" || { err "не посчитать $sch.$t на источнике"; return 1; }
    c2="$(poll_scalar "$dst" "SELECT count(*) FROM $sch.$t" 5)" || { err "не посчитать $sch.$t на приёмнике"; return 1; }
    if [ "$c1" != "$c2" ]; then
      echo "  $sch.$t: источник=$c1 приёмник=$c2"
      bad=1
    fi
  done <<<"$(scalar "$src" "SELECT format('%I', c.relname)
                            FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                            WHERE c.relkind IN ('r','p') AND n.nspname='$sch' ORDER BY 1")"
  return "$bad"
}

# ── P6: sequence→sequence, только вперёд ──────────────────────────────────────
# Sequences не реплицируются. dst доводится до последнего ВЫДАННОГО на src
# (setval только вперёд: пост-flip записи уже расходовали значения dst).
# issued/next считаем НА СТОРОНЕ SQL: конкатенация boolean с текстом даёт
# 'true'/'false' (не 't'/'f' дисплея psql) — парсить его в bash нельзя.
sync_sequences_forward() { # <src_dsn> <dst_dsn> <schema> <label-src> — код 1 при сбое
  local src="$1" dst="$2" sch="$3" srcl="$4"
  local seqs seq qseq issued next fixed=0
  seqs="$(scalar "$src" "SELECT s.relname FROM pg_class s
                         JOIN pg_namespace ns ON ns.oid=s.relnamespace
                         WHERE s.relkind='S' AND ns.nspname='$sch' ORDER BY 1")" || return 1
  if [ -z "$seqs" ]; then info "sequences в '$sch' нет — пропускаю"; return 0; fi
  for seq in $seqs; do
    qseq="$(scalar "$src" "SELECT format('%I.%I', '$sch', '$seq')")" || return 1
    # последнее ВЫДАННОЕ на источнике: is_called→last_value, иначе last_value-1
    issued="$(scalar "$src" "SELECT CASE WHEN is_called THEN last_value ELSE last_value-1 END
                            FROM $qseq" 2>/dev/null)" \
      || { echo "  ⚠️ не прочитался sequence $sch.$seq на '$srcl' — пропускаю"; continue; }
    if [ "$(scalar "$dst" "SELECT to_regclass('$sch.$seq') IS NOT NULL")" != "t" ]; then
      err "у приёмника нет sequence $sch.$seq (дрейф схем, P5) — проверь DDL!"
      return 1
    fi
    # следующее, которое выдаст sequence приёмника: is_called→last_value+1, иначе last_value
    next="$(scalar "$dst" "SELECT CASE WHEN is_called THEN last_value+1 ELSE last_value END
                          FROM $qseq")" || return 1
    if [ "$next" -le "$issued" ]; then
      scalar "$dst" "SELECT setval('$qseq', $issued, true)" >/dev/null || return 1
      info "P6: $sch.$seq → setval($issued) на приёмнике (следующее $((issued + 1)) > выданного на '$srcl')"
      fixed=1
    fi
  done
  [ "$fixed" = 1 ] || info "P6: инвариант уже соблюдён — sequences не трогаю"
}
