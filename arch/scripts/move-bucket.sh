#!/usr/bin/env bash
# scripts/move-bucket.sh
#
# Онлайн-переезд бакета (схемы) между шардами — обёртка над runbook'ом
# из arch/11-bucket-sharding.md: §5 (переезд), §6 (откат), §5-шаг-5 (уборка).
#
# Использование:
#   ./scripts/move-bucket.sh move     <bucket> --to <shard> [--yes] [--skip-reverse] [--resume]
#   ./scripts/move-bucket.sh status   <bucket>
#   ./scripts/move-bucket.sh rollback <bucket> [--yes]
#   ./scripts/move-bucket.sh finalize <bucket> --old-shard <shard> [--yes]
#
# Команды:
#   move     полный переезд: предполётные проверки → каталог SYNCING → перенос DDL →
#            PUBLICATION/SUBSCRIPTION (copy_data) → ожидание догоняния → cutover
#            (фриз записи на секунды: FROZEN → лаг 0 → sequences → атомарный flip
#            каталога) → срез прямой подписки → обратная подписка для отката.
#            Бакет доступен на запись всё время, кроме FROZEN. Прервать можно на любом
#            шаге: повторный запуск move с теми же аргументами продолжит с места сбоя.
#   status   каталог + артефакты переезда (схемы, публикации, подписки, лаги слотов).
#   rollback вернуть бакет на прежний шард через живую обратную подписку.
#   finalize уборка после переезда: удалить схему и артефакты на шарде-НЕ-владельце.
#
# ⚠️ С начала move и до finalize действует DDL-мораторий на бакет (§8):
#    логическая репликация не переносит DDL.
#
# Конфиг: configs/buckets/buckets.env (см. buckets.env.example).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
. "$SCRIPT_DIR/buckets-common.sh"

usage() {
  cat >&2 <<EOF
Usage:
  $0 move     <bucket> --to <shard> [--yes] [--skip-reverse] [--resume]
  $0 status   <bucket>
  $0 rollback <bucket> [--yes]
  $0 finalize <bucket> --old-shard <shard> [--yes]
EOF
  exit 2
}

CMD="${1:-}"
[ -n "$CMD" ] && shift || usage
BUCKET="" TO="" OLD_SHARD="" ASSUME_YES=0 SKIP_REVERSE=0 RESUME=0
while [ $# -gt 0 ]; do
  case "$1" in
    --to)           TO="${2:-}"; shift 2 ;;
    --old-shard)    OLD_SHARD="${2:-}"; shift 2 ;;
    --yes|-y)       ASSUME_YES=1; shift ;;
    --skip-reverse) SKIP_REVERSE=1; shift ;;
    --resume)       RESUME=1; shift ;;
    -h|--help)      usage ;;
    *)              if [ -z "$BUCKET" ]; then BUCKET="$1"; else usage; fi; shift ;;
  esac
done

require_bins
[ -n "$BUCKET" ] || usage
valid_bucket "$BUCKET" || { err "неверное имя бакета '$BUCKET' (шаблон: ^[a-z][a-z0-9_]*$)"; exit 2; }

# Имена артефактов переезда (§5–§6)
PUB="pub_${BUCKET}"        # прямая публикация: на старом шарде, пока идёт переезд
SUB="sub_${BUCKET}"        # прямая подписка: на новом шарде, пока идёт переезд
PUB_RB="pub_${BUCKET}_rb"  # обратная публикация: на новом владельце после flip
SUB_RB="sub_${BUCKET}_rb"  # обратная подписка: на старом шарде после flip

confirm() {
  [ "$ASSUME_YES" = 1 ] && return 0
  local a=""
  read -rp "$1 [введите YES]: " a
  [ "${a:-}" = "YES" ] || { echo "Отменено."; exit 1; }
}

# Cutover, общий для move и rollback:
#   $1 = текущий владелец (источник изменений), $2 = новый владелец,
#   $3 = имя слота на текущем владельце, $4 = состояние каталога при неудаче.
# Шаги: FROZEN → выждать TTL кэша роутера → последний LSN → дождаться слота →
# sequences → атомарный flip.
cutover_flip() {
  local cur="$1" new="$2" slot="$3" fail_state="$4"
  local cur_dsn new_dsn lsn waited=0 flipped
  cur_dsn="$(shard_dsn "$cur")"
  new_dsn="$(shard_dsn "$new")"

  catalog_sql "UPDATE buckets SET state='FROZEN', updated_at=now()
               WHERE bucket_id='$BUCKET' AND state IN ('ACTIVE','SYNCING')"
  echo "  FROZEN: роутер отклоняет запись (чтения работают)"
  echo "  жду ${FREEZE_WAIT_SEC}с (TTL кэша роутера — клиенты перестают писать) ..."
  sleep "$FREEZE_WAIT_SEC"

  lsn="$(scalar "$cur_dsn" 'SELECT pg_current_wal_lsn()')"
  echo "  последний LSN записи на '$cur': $lsn"

  until slot_caught_up "$cur_dsn" "$slot" "$lsn"; do
    if [ "$waited" -ge "$CUTOVER_TIMEOUT_SEC" ]; then
      catalog_sql "UPDATE buckets SET state='$fail_state', updated_at=now()
                   WHERE bucket_id='$BUCKET' AND state='FROZEN'"
      err "слот не подтвердил LSN за ${CUTOVER_TIMEOUT_SEC}с — разморозил (state=$fail_state), репликация продолжает догонять. Перезапусти команду позже."
      return 4
    fi
    sleep "$POLL_INTERVAL_SEC"; waited=$((waited + POLL_INTERVAL_SEC))
    echo "  жду подтверждения слота $slot ... ${waited}с, лаг: $(slot_lag "$cur_dsn" "$slot") байт"
  done
  info "репликация догнала (лаг 0 на момент $lsn)"

  sync_sequences "$new_dsn" "$BUCKET"

  flipped="$(scalar "$BUCKET_CATALOG_DSN" "UPDATE buckets
               SET shard_id='$new', target_shard_id=NULL, state='ACTIVE', updated_at=now()
               WHERE bucket_id='$BUCKET' AND state='FROZEN' RETURNING shard_id")"
  [ "$flipped" = "$new" ] || { err "flip каталога не прошёл (обновлено строк: ${flipped:-0}) — разберись вручную!"; exit 5; }
  info "владелец '$BUCKET' → '$new' (ACTIVE)"
}

# ── move ─────────────────────────────────────────────────────────────────────
cmd_move() {
  local row src_dsn dst_dsn state target sub_on_dst schema_on_dst mover_src
  valid_shard "$TO" || { err "неизвестный шард '$TO' (SHARDS в buckets.env: ${SHARDS})"; exit 2; }

  row="$(catalog_row "$BUCKET")"
  [ -n "$row" ] || { err "бакета '$BUCKET' нет в каталоге"; exit 3; }
  IFS='|' read -r SRC STATE TARGET <<< "$row"
  valid_shard "$SRC" || { err "текущий владелец '$SRC' не описан в buckets.env"; exit 9; }
  [ "$SRC" != "$TO" ] || { err "бакет уже на '$TO'"; exit 3; }
  src_dsn="$(shard_dsn "$SRC")"
  dst_dsn="$(shard_dsn "$TO")"

  # ── 0) Предполётные проверки
  step "0) Предполётные проверки"
  case "$STATE" in
    ACTIVE)
      [ -z "$TARGET" ] || { err "каталог: ACTIVE с target='$TARGET' — почин вручную"; exit 3; } ;;
    SYNCING|FROZEN)
      [ "$TARGET" = "$TO" ] || { err "переезд уже идёт на '$TARGET', а запрошен '$TO'"; exit 3; }
      info "незавершённый переезд на '$TO' — продолжаю" ;;
    *) err "неожиданное состояние каталога: $STATE"; exit 3 ;;
  esac

  [ "$(scalar "$src_dsn" 'SELECT 1')" = "1" ] || { err "шард-источник '$SRC' недоступен"; exit 3; }
  [ "$(scalar "$dst_dsn" 'SELECT 1')" = "1" ] || { err "шард-приёмник '$TO' недоступен"; exit 3; }
  schema_exists "$src_dsn" "$BUCKET" || { err "схемы '$BUCKET' нет на '$SRC'?!"; exit 3; }

  local wl
  wl="$(scalar "$src_dsn" 'SHOW wal_level')"
  [ "$wl" = "logical" ] || { err "wal_level='$wl' на '$SRC', нужно 'logical' (§4, рестарт кластера)"; exit 3; }

  local max_slots used_slots max_senders used_senders
  max_slots="$(scalar "$src_dsn" "SELECT setting::int FROM pg_settings WHERE name='max_replication_slots'")"
  used_slots="$(scalar "$src_dsn" 'SELECT count(*) FROM pg_replication_slots')"
  [ "$used_slots" -lt "$max_slots" ] || { err "слоты на '$SRC' кончились ($used_slots/$max_slots)"; exit 3; }
  max_senders="$(scalar "$src_dsn" "SELECT setting::int FROM pg_settings WHERE name='max_wal_senders'")"
  used_senders="$(scalar "$src_dsn" 'SELECT count(*) FROM pg_stat_replication')"
  [ "$used_senders" -lt "$max_senders" ] || { err "walsender'ы на '$SRC' кончились ($used_senders/$max_senders)"; exit 3; }

  mover_src="$(mover_conninfo "$SRC")"
  [ "$(scalar "$mover_src" 'SELECT 1')" = "1" ] \
    || { err "mover-роль недоступна на '$SRC' (MOVER_CONNINFO_${SRC})"; exit 3; }
  [ "$(scalar "$mover_src" 'SELECT rolsuper OR rolreplication FROM pg_roles WHERE rolname=current_user')" = "t" ] \
    || { err "mover-роль на '$SRC' без атрибута REPLICATION (§4)"; exit 3; }

  sub_on_dst="f"; sub_exists "$dst_dsn" "$SUB" && sub_on_dst="t"
  schema_on_dst="f"; schema_exists "$dst_dsn" "$BUCKET" && schema_on_dst="t"
  if [ "$sub_on_dst" = "f" ] && [ "$schema_on_dst" = "t" ]; then
    if [ "$RESUME" = 0 ]; then
      err "схема '$BUCKET' уже есть на '$TO' без подписки (остаток сорванного запуска?)"
      echo "  Либо удали её и запусти снова:  psql <dsn-$TO> -c 'DROP SCHEMA $BUCKET CASCADE'" >&2
      echo "  Либо продолжи с --resume, если это ДОПИСАННЫЙ DDL без данных." >&2
      exit 3
    fi
    # --resume допустим только для ПУСТОЙ схемы: copy_data=true в непустую даст дубликаты
    local cnt_q cnt
    cnt_q="$(scalar "$dst_dsn" "SELECT 'SELECT '||coalesce(string_agg('(SELECT count(*) FROM '||quote_ident(n.nspname)||'.'||quote_ident(c.relname)||')', '+'), '0')
                               FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                               WHERE n.nspname='$BUCKET' AND c.relkind='r'")"
    cnt="$(scalar "$dst_dsn" "$cnt_q")"
    [ "$cnt" = "0" ] || {
      err "схема на '$TO' не пустая ($cnt строк) — это остатки данных, а не сорванный DDL."
      echo "  Данные живут у владельца '$SRC'. Удали схему на '$TO' и запускай без --resume:" >&2
      echo "    psql <dsn-$TO> -c 'DROP SCHEMA $BUCKET CASCADE'" >&2
      exit 3
    }
    info "--resume: схема на '$TO' пустая — продолжаю"
  fi
  pub_exists "$dst_dsn" "$PUB_RB" && { err "на '$TO' осталась $PUB_RB — сначала разберись (finalize?)"; exit 3; }
  sub_exists "$src_dsn" "$SUB_RB" && { err "на '$SRC' осталась $SUB_RB — сначала finalize прошлого переезда"; exit 3; }

  if [ "$STATE" = "ACTIVE" ]; then
    catalog_sql "UPDATE buckets SET state='SYNCING', target_shard_id='$TO', updated_at=now()
                 WHERE bucket_id='$BUCKET' AND state='ACTIVE'"
    info "каталог: SYNCING (бакет продолжает работать на запись)"
  fi
  echo "  ⚠️ Напоминаю: до конца переезда — DDL-мораторий на '$BUCKET'."

  # ── 1) Перенос DDL
  if [ "$sub_on_dst" = "t" ] || [ "$schema_on_dst" = "t" ]; then
    step "1) DDL пропущен (возобновление: схема на '$TO' уже есть)"
  else
    step "1) Перенос DDL: pg_dump --schema-only $SRC → $TO"
    pg_dump --schema-only --schema="$BUCKET" --no-owner --no-privileges "$src_dsn" \
      | psql "$dst_dsn" -X -q -v ON_ERROR_STOP=1 >/dev/null \
      || { err "DDL не применился на '$TO'; перезапусти (схему на '$TO' удали или используй --resume)"; exit 4; }
    info "DDL перенесён"
  fi

  # ── 2) Публикация на источнике, подписка на приёмнике
  step "2) PUBLICATION '$PUB' на '$SRC' → SUBSCRIPTION '$SUB' на '$TO'"
  if pub_exists "$src_dsn" "$PUB"; then
    info "публикация уже есть (возобновление)"
  else
    sql "$src_dsn" "CREATE PUBLICATION $PUB FOR TABLES IN SCHEMA $BUCKET"
    info "публикация создана"
  fi
  if [ "$sub_on_dst" = "f" ]; then
    psql "$dst_dsn" -X -q -v ON_ERROR_STOP=1 -v conn="$mover_src" <<SQL
CREATE SUBSCRIPTION $SUB CONNECTION :'conn' PUBLICATION $PUB WITH (copy_data = true);
SQL
    info "подписка создана (copy_data=true), начался initial copy"
  else
    info "подписка уже есть (возобновление)"
  fi

  # ── 3) Ожидание initial copy (без таймаута: большой бакет копируется часами)
  step "3) Ожидаю initial copy (бакет доступен на запись; Ctrl+C безопасен — потом перезапусти)"
  local s lag last="" ready total
  while :; do
    s="$(sub_sync "$dst_dsn" "$SUB")"
    ready="${s%%/*}"; total="${s##*/}"
    if [ "$s" != "$last" ]; then
      lag="$(slot_lag "$src_dsn" "$SUB")"
      echo "  таблицы готовы: $ready/$total, отставание слота: ${lag} байт"
      last="$s"
    fi
    [ "$ready" = "$total" ] && break
    sleep "$POLL_INTERVAL_SEC"
  done
  info "initial copy завершён, подписка стримит изменения"

  # ── 4) Cutover
  step "4) Cutover: фриз записи → лаг 0 → sequences → flip каталога"
  confirm "Переключить '$BUCKET': $SRC → $TO? (запись будет недоступна ~$((FREEZE_WAIT_SEC + 5))с)"
  cutover_flip "$SRC" "$TO" "$SUB" "SYNCING" || exit 4

  # ── 5) Прямую подписку срезать, поставить обратную (для отката)
  step "5) Срезаю прямую подписку, ставлю обратную"
  local rev_ok=1
  sql "$dst_dsn" "DROP SUBSCRIPTION $SUB" || {
    echo "⚠️ не удалось удалить $SUB на '$TO' (источник недоступен?) — слот '$SUB' на '$SRC' держит WAL. Удали вручную позже."
    rev_ok=0
  }
  if [ "$SKIP_REVERSE" = 1 ]; then
    echo "  --skip-reverse: обратной подписки нет — откат только полным re-copy (§6)"
  elif [ "$rev_ok" = 1 ]; then
    # Прямая подписка обязана быть срезана ДО создания обратной — иначе петля репликации.
    sql "$dst_dsn" "CREATE PUBLICATION $PUB_RB FOR TABLES IN SCHEMA $BUCKET"
    psql "$src_dsn" -X -q -v ON_ERROR_STOP=1 -v conn="$(mover_conninfo "$TO")" <<SQL
CREATE SUBSCRIPTION $SUB_RB CONNECTION :'conn' PUBLICATION $PUB_RB WITH (copy_data = false);
SQL
    info "обратная подписка: $PUB_RB на '$TO' → $SUB_RB на '$SRC' (без re-copy)"
  else
    echo "⚠️ обратную подписку НЕ ставлю, пока не удалена прямая (риск петли). После удаления $SUB поставь вручную (§6)."
  fi

  echo
  echo "Готово: '$BUCKET' переехал $SRC → $TO."
  echo "  Откат (пока жива обратная подписка): $0 rollback $BUCKET"
  echo "  Уборка старого шарда позже:          $0 finalize $BUCKET --old-shard $SRC"
  echo "  Состояние:                           $0 status $BUCKET"
}

# ── status ───────────────────────────────────────────────────────────────────
cmd_status() {
  local row owner state target s dsn line
  row="$(catalog_row "$BUCKET")"
  [ -n "$row" ] || { err "бакета '$BUCKET' нет в каталоге"; exit 3; }
  IFS='|' read -r OWNER STATE TARGET <<< "$row"
  echo "$BUCKET: владелец=$OWNER  state=$STATE  target=${TARGET:--}"
  for s in $SHARDS; do
    dsn="$(shard_dsn "$s")"
    line="  $s:"
    schema_exists "$dsn" "$BUCKET" && line="$line схема=да" || line="$line схема=нет"
    pub_exists "$dsn" "$PUB" && line="$line $PUB"
    sub_exists "$dsn" "$SUB" && line="$line $SUB(готово: $(sub_sync "$dsn" "$SUB"))"
    pub_exists "$dsn" "$PUB_RB" && line="$line $PUB_RB"
    sub_exists "$dsn" "$SUB_RB" && line="$line $SUB_RB"
    local lag_f lag_r
    lag_f="$(slot_lag "$dsn" "$SUB")"
    lag_r="$(slot_lag "$dsn" "$SUB_RB")"
    [ "$lag_f" = "0" ] || line="$line лаг($SUB)=${lag_f}B"
    [ "$lag_r" = "0" ] || line="$line лаг($SUB_RB)=${lag_r}B"
    echo "$line"
  done
}

# ── rollback ─────────────────────────────────────────────────────────────────
cmd_rollback() {
  local row owner state target old=""
  row="$(catalog_row "$BUCKET")"
  [ -n "$row" ] || { err "бакета нет в каталоге"; exit 3; }
  IFS='|' read -r OWNER STATE TARGET <<< "$row"
  [ "$STATE" = "ACTIVE" ] || { err "откат возможен только из ACTIVE (сейчас $STATE)"; exit 3; }

  local s
  for s in $SHARDS; do
    sub_exists "$(shard_dsn "$s")" "$SUB_RB" && { old="$s"; break; }
  done
  [ -n "$old" ] || { err "обратная подписка $SUB_RB не найдена ни на одном шарде — откат только полным re-copy (§6)"; exit 3; }
  [ "$old" != "$OWNER" ] || { err "странно: $SUB_RB найдена на текущем владельце — разберись вручную"; exit 3; }

  step "Откат '$BUCKET': $OWNER → $old (через $SUB_RB)"
  confirm "Вернуть бакет на '$old'? (запись недоступна ~$((FREEZE_WAIT_SEC + 5))с)"
  cutover_flip "$OWNER" "$old" "$SUB_RB" "ACTIVE" || exit 4

  step "Убираю артефакты обратной репликации"
  sql "$(shard_dsn "$old")" "DROP SUBSCRIPTION $SUB_RB" \
    || echo "⚠️ не удалось удалить $SUB_RB на '$old' — удали вручную (слот на '$OWNER' держит WAL)"
  sql "$(shard_dsn "$OWNER")" "DROP PUBLICATION $PUB_RB" \
    || echo "⚠️ не удалось удалить $PUB_RB на '$OWNER' — удали вручную"

  echo
  echo "Готово: '$BUCKET' вернулся на '$old'."
  echo "  На '$OWNER' осталась схема '$BUCKET' (данные на момент отката) — убери:"
  echo "    $0 finalize $BUCKET --old-shard $OWNER"
}

# ── finalize ─────────────────────────────────────────────────────────────────
cmd_finalize() {
  local row owner state
  valid_shard "$OLD_SHARD" || { err "неизвестный шард '$OLD_SHARD'"; exit 2; }
  row="$(catalog_row "$BUCKET")"
  [ -n "$row" ] || { err "бакета нет в каталоге"; exit 3; }
  IFS='|' read -r OWNER STATE TARGET <<< "$row"
  [ "$STATE" = "ACTIVE" ] || { err "finalize возможен только из ACTIVE (сейчас $STATE)"; exit 3; }
  [ "$OLD_SHARD" != "$OWNER" ] || { err "--old-shard ($OLD_SHARD) совпадает с текущим владельцем!"; exit 3; }
  local old_dsn
  old_dsn="$(shard_dsn "$OLD_SHARD")"

  step "Уборка '$BUCKET' на '$OLD_SHARD' (владелец '$OWNER' не трогается)"
  echo "  ⚠️ Схема '$BUCKET' будет удалена на '$OLD_SHARD' СО ВСЕМИ ДАННЫМИ."
  confirm "Удалить?"

  # 1) подписки срезаем первыми — они держат слоты (и WAL) на владельце
  sub_exists "$old_dsn" "$SUB_RB" && sql "$old_dsn" "DROP SUBSCRIPTION $SUB_RB"
  sub_exists "$(shard_dsn "$OWNER")" "$SUB" && sql "$(shard_dsn "$OWNER")" "DROP SUBSCRIPTION $SUB"
  # 2) публикации и схема
  pub_exists "$old_dsn" "$PUB" && sql "$old_dsn" "DROP PUBLICATION $PUB"
  pub_exists "$(shard_dsn "$OWNER")" "$PUB_RB" && sql "$(shard_dsn "$OWNER")" "DROP PUBLICATION $PUB_RB"
  if schema_exists "$old_dsn" "$BUCKET"; then
    sql "$old_dsn" "DROP SCHEMA $BUCKET CASCADE"
    info "схема '$BUCKET' удалена на '$OLD_SHARD'"
  else
    info "схемы на '$OLD_SHARD' уже не было"
  fi
  echo "Уборка завершена. Проверь: $0 status $BUCKET"
}

# ── запуск ───────────────────────────────────────────────────────────────────
case "$CMD" in
  move)     [ -n "$TO" ] || usage; catalog_check_table; cmd_move ;;
  status)   catalog_check_table; cmd_status ;;
  rollback) catalog_check_table; cmd_rollback ;;
  finalize) [ -n "$OLD_SHARD" ] || usage; catalog_check_table; cmd_finalize ;;
  *)        usage ;;
esac
