#!/usr/bin/env bash
# scripts/move-bucket.sh
#
# Онлайн-переезд бакета (схемы) между шардами: состояние переезда живёт
# в etcd-контрол-плейне. Обёртка над runbook'ом из arch/11-bucket-sharding.md:
# §5 (переезд), §6 (откат), §5-шаг-5 (уборка).
# Решения P1/P4/P5/P6/P7/P8 из 12-bucket-pitfalls.md внесены.
#
# Использование:
#   ./scripts/move-bucket.sh [--cluster <C>] move     <bucket> --to <shard> [--yes] [--skip-reverse] [--resume]
#   ./scripts/move-bucket.sh [--cluster <C>] status   <bucket>
#   ./scripts/move-bucket.sh [--cluster <C>] rollback <bucket> [--yes]
#   ./scripts/move-bucket.sh [--cluster <C>] finalize <bucket> --old-shard <shard> [--yes]
#
#   --cluster  кластер в etcd (дефолт CLUSTER_NAME из buckets.env)
#
# Модель (etcd-контрол-плейн, всё под префиксом кластера /clusters/<C>/):
#   .../buckets/routing/<bucket> → владелец (авторитет); нет статус-ключа = ACTIVE.
#   .../buckets/status/<bucket>  → {"state":"SYNCING|FROZEN", "owner":…, "target":…,
#                                "phase":…, started/updated_unix} — только при переезде.
#   Cutover = атомарная etcd-транзакция: routing → новый владелец + delete status
#   («flip применился, но etcd не знает» невозможно по построению).
#
#   move     предполётные проверки → SYNCING в etcd → перенос DDL + гранты app-роли
#            + двойная сверка инвентаря схем (P5) → PUBLICATION/SUBSCRIPTION
#            (copy_data, failover=true, synchronous_commit=remote_apply — P8)
#            → ожидание догоняния (транзиенто-толерантно: failover приёмника не
#            убивает mover) → cutover: заморозка REVOKE+LOCK (P1) + мораторий
#            CREATE (P5) → FROZEN → лаг 0 → sequence→sequence (P6) → сверка строк
#            (P8) → атомарный flip → срез прямой подписки → обратная подписка.
#            Прервать можно на любом шаге: повторный запуск move с теми же
#            аргументами продолжит с места сбоя. После flip старый шард остаётся
#            замороженным (P1: призраки не пишут) до rollback/finalize.
#   status   etcd-состояние + артефакты на шардах (схемы, публикации, подписки, лаги).
#   rollback вернуть бакет на прежний шард через живую обратную подписку
#            (заморозка/сверки/flip — те же; после отката GRANT-разморозка).
#   finalize уборка после переезда: артефакты и схема (с данными) на НЕ-владельце,
#            включая осиротевшие tablesync-слоты (P8: copy рестартует новым слотом).
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
  $0 [--cluster <C>] move     <bucket> --to <shard> [--yes] [--skip-reverse] [--resume]
  $0 [--cluster <C>] status   <bucket>
  $0 [--cluster <C>] rollback <bucket> [--yes]
  $0 [--cluster <C>] finalize <bucket> --old-shard <shard> [--yes]
EOF
  exit 2
}

confirm() {
  [ "$ASSUME_YES" = 1 ] && return 0
  local a=""
  read -rp "$1 [введите YES]: " a
  [ "${a:-}" = "YES" ] || { echo "Отменено."; exit 1; }
}

# ── статусы в etcd ────────────────────────────────────────────────────────────
MOVE_STARTED="$(date +%s)"
status_put() { # $1=state $2=phase $3=target (по умолчанию TARGET)
  local payload
  payload="$(jq -n \
    --arg bucket "$BUCKET" --arg state "$1" --arg owner "$OWNER" --arg target "${3:-$TARGET}" \
    --argjson started "$MOVE_STARTED" --argjson updated "$(date +%s)" --arg phase "$2" \
    '{bucket:$bucket, state:$state, owner:$owner, target:$target,
      started_unix:$started, updated_unix:$updated, phase:$phase}')"
  ect put "$(status_key "$BUCKET")" "$payload" >/dev/null
}

# Атомарный flip: routing → new + delete status одной etcd-транзакцией.
# Успех только если routing всё ещё указывает на cur (никто не перехватил).
# Формат etcdctl txn: compare-секция, success-ops, failure-ops — секции
# разделяются пустыми строками. Подавать строго пайпом с хвостовыми \n:
# command substitution $(...) срезает trailing newlines, а herestring (<<<)
# добавляет ровно один — txn получает две секции вместо трёх и молча
# умирает с rc=3 (найдено на стенде).
etcd_flip() { # <cur> <new>
  local input out
  input="$(printf 'val("%s") = "%s"\n\nput %s %s\ndel %s\n' \
    "$(routing_key "$BUCKET")" "$1" "$(routing_key "$BUCKET")" "$2" "$(status_key "$BUCKET")")"
  out="$(printf '%s\n\n\n' "$input" | ect txn 2>/dev/null | head -1)"
  [ "$out" = "SUCCESS" ]
}

# ── P1/P5: заморозка источника и разморозка ───────────────────────────────────
# REVOKE — лёгкая блокировка и писателей НЕ ждёт: барьер — LOCK TABLE ACCESS
# EXCLUSIVE в той же транзакции (упирается в lock_timeout → ретрай).
freeze_source() { # <dsn> <schema>
  local dsn="$1" sch="$2" tables try lock=""
  for try in $(seq 1 "$FREEZE_LOCK_TRIES"); do
    tables="$(scalar "$dsn" "SELECT coalesce(string_agg(format('%I.%I','$sch',c.relname),', '), '')
                             FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                             WHERE c.relkind IN ('r','p') AND n.nspname='$sch'")" || return 1
    [ -n "$tables" ] && lock="LOCK TABLE $tables IN ACCESS EXCLUSIVE MODE;"
    if sql "$dsn" "BEGIN;
                   SET lock_timeout='$FREEZE_LOCK_TIMEOUT';
                   REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA $sch FROM $APP_ROLE;
                   REVOKE USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA $sch FROM $APP_ROLE;
                   REVOKE CREATE ON SCHEMA $sch FROM $APP_ROLE;
                   $lock
                   COMMIT;"; then
      return 0
    fi
    echo "  заморозка упёрлась в живого писателя (lock_timeout), попытка $try/$FREEZE_LOCK_TRIES"
    sleep "$POLL_INTERVAL_SEC"
  done
  return 1
}

unfreeze_shard() { # <dsn> <schema> — симметричный GRANT (P1/P5)
  sql "$1" "GRANT INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA $2 TO $APP_ROLE;
            GRANT USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA $2 TO $APP_ROLE;" || return 1
  if [ "${APP_GRANT_CREATE:-0}" = 1 ]; then
    sql "$1" "GRANT CREATE ON SCHEMA $2 TO $APP_ROLE" || return 1
  fi
}

grant_app_role() { # <dsn> <schema> — базовые гранты app-роли на приёмнике (§4)
  if [ "$(scalar "$1" "SELECT count(*) FROM pg_roles WHERE rolname='$APP_ROLE'")" = "0" ]; then
    sql "$1" "CREATE ROLE $APP_ROLE LOGIN" || return 1
  fi
  sql "$1" "GRANT USAGE ON SCHEMA $2 TO $APP_ROLE;
            GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA $2 TO $APP_ROLE;
            GRANT USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA $2 TO $APP_ROLE;" || return 1
  [ "${APP_GRANT_CREATE:-0}" = 1 ] && sql "$1" "GRANT CREATE ON SCHEMA $2 TO $APP_ROLE"
}

# ── cutover (общий для move и rollback) ───────────────────────────────────────
# $1 = текущий владелец (источник), $2 = новый владелец, $3 = слот на текущем,
# $4 = состояние статуса при неудаче. Заморозка → FROZEN → последний LSN →
# слот догнал → sequence→sequence (P6) → сверка строк (P8) → атомарный flip.
cutover_flip() {
  local cur="$1" new="$2" slot="$3" fail_state="$4"
  local cur_dsn new_dsn lsn waited=0 ok
  cur_dsn="$(shard_dsn "$cur")"
  new_dsn="$(shard_dsn "$new")"

  step "Заморозка источника '$cur' (P1: REVOKE + барьер LOCK; P5: мораторий CREATE)"
  freeze_source "$cur_dsn" "$BUCKET" || { status_put "$fail_state" "freeze-failed"; return 4; }
  status_put FROZEN frozen
  info "FROZEN: роутер отклоняет запись (чтения работают)"
  echo "  жду ${FREEZE_WAIT_SEC}с (TTL кэша роутера — клиенты перестают писать) ..."
  sleep "$FREEZE_WAIT_SEC"

  lsn="$(poll_scalar "$cur_dsn" 'SELECT pg_current_wal_lsn()' 5)" || { status_put "$fail_state" "lsn-failed"; return 4; }
  echo "  последний LSN записи на '$cur': $lsn"

  echo "  жду подтверждения слота $slot (лаг байт / сек):"
  while :; do
    ok="$(scalar "$cur_dsn" "SELECT coalesce(bool_and(active AND confirmed_flush_lsn >= '$lsn'::pg_lsn), false)
                             FROM pg_replication_slots WHERE slot_name='$slot'" 2>/dev/null || true)"
    [ "$ok" = "t" ] && break
    if [ "$waited" -ge "$CUTOVER_TIMEOUT_SEC" ]; then
      unfreeze_shard "$cur_dsn" "$BUCKET" || true
      status_put "$fail_state" "catchup-timeout"
      err "слот не подтвердил LSN за ${CUTOVER_TIMEOUT_SEC}с — разморозил (state=$fail_state), репликация продолжает догонять. Перезапусти команду позже."
      return 4
    fi
    sleep "$POLL_INTERVAL_SEC"; waited=$((waited + POLL_INTERVAL_SEC))
    echo "    $(slot_lag "$cur_dsn" "$slot" 2>/dev/null || echo '?') / ${waited}с"
  done
  info "репликация догнала (лаг 0 на момент $lsn)"

  sync_sequences_forward "$cur_dsn" "$new_dsn" "$BUCKET" "$cur" \
    || { unfreeze_shard "$cur_dsn" "$BUCKET" || true; status_put "$fail_state" "sequences-failed"; return 4; }

  # P8-барьер: lag 0 не гарантирует полноту копии (после failover приёмника
  # срез изменений может быть молча пропущен при нулевом лаге) — сверяем строки.
  status_put FROZEN verify
  if ! verify_row_counts "$cur_dsn" "$new_dsn" "$BUCKET"; then
    unfreeze_shard "$cur_dsn" "$BUCKET" || true
    status_put "$fail_state" "verify-failed"
    err "сверка строк источник/приёмник НЕ сошлась — копия дефектна (P8: failover приёмника?)."
    echo "  Отмени переезд (abort-move.sh abort $BUCKET) и запусти заново — свежий initial copy переприготовит копию." >&2
    return 4
  fi
  info "P8: сверка строк сошлась (count по всем таблицам совпадает)"

  status_put FROZEN flip
  if ! etcd_flip "$cur" "$new"; then
    err "etcd-транзакция flip не прошла (routing изменился под руками?) — заморозка ОСТАВЛЕНА, разберись вручную!"
    return 5
  fi
  info "атомарный flip: routing '$BUCKET' → '$new', статус-ключ удалён (нет ключа = ACTIVE)"
  # P12: снапшот точки «переключил на нового владельца»: flip атомарен и
  # статус-ключа уже нет — restore из него даёт сразу согласованную карту.
  # Flip уже случился — сбоем снапшота move не роняем, только громко просим.
  etcd_snapshot "flip-${BUCKET}-${new}" \
    || echo "⚠️ P12: сними снапшот вручную: restore-cluster.sh [--cluster $CLUSTER_NAME] snapshot flip-${BUCKET}"
}

# ── move ──────────────────────────────────────────────────────────────────────
cmd_move() {
  local state target sub_on_dst schema_on_dst mover_src max_fails fail_streak s ready total last
  valid_shard "$TO" || { err "неизвестный шард '$TO' (etcd-реестр кластера '$CLUSTER_NAME' или SHARDS в buckets.env)"; exit 2; }

  etcd_alive
  OWNER="$(routing_get "$BUCKET")"
  [ -n "$OWNER" ] || { err "нет $(routing_key "$BUCKET") — владелец неизвестен, переезд невозможен (восстанови контрол-плейн, P12)"; exit 3; }
  valid_shard "$OWNER" || { err "владелец '$OWNER' не описан в buckets.env (SHARDS)"; exit 9; }
  [ "$OWNER" != "$TO" ] || { err "бакет уже на '$TO'"; exit 3; }
  TARGET="$TO"
  local src_dsn dst_dsn
  src_dsn="$(shard_dsn "$OWNER")"
  dst_dsn="$(shard_dsn "$TO")"

  # ── 0) Предполётные проверки
  step "0) Предполётные проверки"
  STATUS_JSON="$(status_get "$BUCKET")"
  state=""; target=""
  if [ -n "$STATUS_JSON" ]; then
    state="$(jstr .state "$STATUS_JSON")"
    target="$(jstr .target "$STATUS_JSON")"
    MOVE_STARTED="$(jstr .started_unix "$STATUS_JSON")"
    MOVE_STARTED="${MOVE_STARTED:-$(date +%s)}"
    case "$state" in
      SYNCING|FROZEN)
        [ "$target" = "$TO" ] || { err "переезд уже идёт на '${target:-?}', а запрошен '$TO'"; exit 3; }
        info "незавершённый переезд на '$TO' (state=$state) — продолжаю" ;;
      ABORTING) err "уборка прерванного переезда не закончена (state=ABORTING) — сначала заверши: abort-move.sh abort $BUCKET"; exit 3 ;;
      *) err "неожиданное состояние статуса: $state"; exit 3 ;;
    esac
  else
    info "статус-ключа нет — бакет ACTIVE, начинаю новый переезд"
  fi

  [ "$(poll_scalar "$src_dsn" 'SELECT 1' 3)" = "1" ] || { err "шард-источник '$OWNER' недоступен"; exit 3; }
  [ "$(poll_scalar "$dst_dsn" 'SELECT 1' 3)" = "1" ] || { err "шард-приёмник '$TO' недоступен"; exit 3; }
  schema_exists "$src_dsn" "$BUCKET" || { err "схемы '$BUCKET' нет на '$OWNER'?!"; exit 3; }

  [ -n "${APP_ROLE:-}" ] || { err "APP_ROLE не задан в buckets.env — некого замораживать на cutover (P1)"; exit 9; }
  valid_bucket "$APP_ROLE" || { err "APP_ROLE='$APP_ROLE' не похоже на имя роли (^[a-z][a-z0-9_]*$)"; exit 9; }

  local wl
  wl="$(scalar "$src_dsn" 'SHOW wal_level')"
  [ "$wl" = "logical" ] || { err "wal_level='$wl' на '$OWNER', нужно 'logical' (§4, рестарт кластера)"; exit 3; }

  local max_slots used_slots max_senders used_senders
  max_slots="$(scalar "$src_dsn" "SELECT setting::int FROM pg_settings WHERE name='max_replication_slots'")"
  used_slots="$(scalar "$src_dsn" 'SELECT count(*) FROM pg_replication_slots')"
  [ "$used_slots" -lt "$max_slots" ] || { err "слоты на '$OWNER' кончились ($used_slots/$max_slots)"; exit 3; }
  max_senders="$(scalar "$src_dsn" "SELECT setting::int FROM pg_settings WHERE name='max_wal_senders'")"
  used_senders="$(scalar "$src_dsn" 'SELECT count(*) FROM pg_stat_replication')"
  [ "$used_senders" -lt "$max_senders" ] || { err "walsender'ы на '$OWNER' кончились ($used_senders/$max_senders)"; exit 3; }

  # P4-префлайт: инвалидированные слоты на источнике — прошлый переезд умер от WAL-лимита
  local lost
  lost="$(scalar "$src_dsn" "SELECT count(*) FROM pg_replication_slots WHERE wal_status='lost'")"
  [ "$lost" = "0" ] || echo "  ⚠️ на '$OWNER' $lost слотов(а) с wal_status='lost' (P4) — прошлая подписка умерла; прибери: abort/finalize"

  mover_src="$(mover_conninfo "$OWNER")"
  [ "$(scalar "$mover_src" 'SELECT 1')" = "1" ] \
    || { err "mover-роль недоступна на '$OWNER' (MOVER_CONNINFO_${OWNER})"; exit 3; }
  [ "$(scalar "$mover_src" 'SELECT rolsuper OR rolreplication FROM pg_roles WHERE rolname=current_user')" = "t" ] \
    || { err "mover-роль на '$OWNER' без атрибута REPLICATION (§4)"; exit 3; }

  # P8: remote_apply подписки работает только при живом sync-standby у приёмника
  check_sync_standby "$dst_dsn" "$TO" || exit 3

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
      echo "  Данные живут у владельца '$OWNER'. Удали схему на '$TO' и запускай без --resume:" >&2
      echo "    psql <dsn-$TO> -c 'DROP SCHEMA $BUCKET CASCADE'" >&2
      exit 3
    }
    info "--resume: схема на '$TO' пустая — продолжаю"
  fi
  pub_exists "$dst_dsn" "$PUB_RB" && { err "на '$TO' осталась $PUB_RB — сначала разберись (finalize?)"; exit 3; }
  sub_exists "$src_dsn" "$SUB_RB" && { err "на '$OWNER' осталась $SUB_RB — сначала finalize прошлого переезда"; exit 3; }

  status_put SYNCING ddl
  # P12: снапшот обязателен в точках переезда — «после начала»: restore при
  # потере etcd получит карту с зафиксированным SYNCING (сужает сверку)
  etcd_snapshot "move-${BUCKET}-start" \
    || { err "без стартового снапшота переезд не начинаю (P12: restore-cluster.sh)"; exit 4; }
  info "etcd: SYNCING $OWNER → $TO (бакет продолжает работать на запись)"
  echo "  ⚠️ Напоминаю: до конца переезда — DDL-мораторий на '$BUCKET'."

  # ── 1) Перенос DDL + гранты app-роли
  if [ "$sub_on_dst" = "t" ] || [ "$schema_on_dst" = "t" ]; then
    step "1) DDL пропущен (возобновление: схема на '$TO' уже есть)"
  else
    step "1) Перенос DDL: pg_dump --schema-only $OWNER → $TO"
    pg_dump --schema-only --schema="$BUCKET" --no-owner --no-privileges "$src_dsn" \
      | psql "$dst_dsn" -X -q -v ON_ERROR_STOP=1 >/dev/null \
      || { err "DDL не применился на '$TO'; перезапусти (схему на '$TO' удали или используй --resume)"; exit 4; }
    info "DDL перенесён"
  fi
  grant_app_role "$dst_dsn" "$BUCKET" || { err "не удалось выдать гранты $APP_ROLE на '$TO' (§4)"; exit 4; }
  info "базовые гранты $APP_ROLE на '$TO' в порядке"

  # P5: двойная сверка инвентаря схем (мораторий могли нарушить до переезда)
  local inv_src inv_dst
  inv_src="$(schema_inventory "$src_dsn" "$BUCKET")"
  inv_dst="$(schema_inventory "$dst_dsn" "$BUCKET")"
  if [ "$inv_src" != "$inv_dst" ]; then
    err "инвентарь '$BUCKET' на '$OWNER' и '$TO' расходится — DDL-мораторий (P5) нарушен?"
    diff <(echo "$inv_src") <(echo "$inv_dst") || true
    exit 4
  fi
  info "P5: инвентарь схем идентичен (таблицы/sequences/views)"

  # ── 2) Публикация на источнике, подписка на приёмнике
  step "2) PUBLICATION '$PUB' на '$OWNER' → SUBSCRIPTION '$SUB' на '$TO'"
  if pub_exists "$src_dsn" "$PUB"; then
    info "публикация уже есть (возобновление)"
  else
    sql "$src_dsn" "CREATE PUBLICATION $PUB FOR TABLES IN SCHEMA $BUCKET"
    info "публикация создана"
  fi
  status_put SYNCING pubsub
  if [ "$sub_on_dst" = "f" ]; then
    # failover=true — слот переживёт failover источника (P3);
    # synchronous_commit=remote_apply — P8: коммит применённых транзакций ждёт
    # реплику приёмника, feedback не убегает вперёд её физической репликации.
    psql "$dst_dsn" -X -q -v ON_ERROR_STOP=1 -v conn="$mover_src" -v sync="$SUB_SYNCCOMMIT" <<SQL
CREATE SUBSCRIPTION $SUB CONNECTION :'conn' PUBLICATION $PUB
  WITH (copy_data = true, failover = true, synchronous_commit = :'sync');
SQL
    info "подписка создана (copy_data=true, failover=true, synchronous_commit=$SUB_SYNCCOMMIT), начался initial copy"
  else
    info "подписка уже есть (возобновление)"
  fi

  # ── 3) Ожидание initial copy (без общего таймаута: большой бакет копируется
  #       часами; обрывы соединения tolerated — у приёмника может идти failover)
  step "3) Ожидаю initial copy (бакет доступен на запись; Ctrl+C безопасен — потом перезапусти)"
  status_put SYNCING copy-wait
  max_fails=$(( CONN_FAIL_BUDGET_SEC / POLL_INTERVAL_SEC )); fail_streak=0; last=""
  while :; do
    s="$(sub_sync "$dst_dsn" "$SUB" 2>/dev/null || true)"
    if [ -z "$s" ]; then
      fail_streak=$((fail_streak + 1))
      if [ "$fail_streak" -ge "$max_fails" ]; then
        err "приёмник '$TO' недоступен дольше ${CONN_FAIL_BUDGET_SEC}с (failover затянулся?) — перезапуши позже (продолжу с этого места)"
        exit 4
      fi
      sleep "$POLL_INTERVAL_SEC"; continue
    fi
    fail_streak=0
    if [ "$s" != "$last" ]; then
      echo "  таблицы готовы: ${s%%/*}/${s##*/}, отставание слота: $(slot_lag "$src_dsn" "$SUB" 2>/dev/null || echo '?') байт"
      last="$s"
    fi
    [ "${s%%/*}" = "${s##*/}" ] && break
    sleep "$POLL_INTERVAL_SEC"
  done
  info "initial copy завершён, подписка стримит изменения"

  # ── 4) Cutover
  step "4) Cutover: фриз записи → лаг 0 → sequences → сверка строк → атомарный flip"
  confirm "Переключить '$BUCKET': $OWNER → $TO? (запись будет недоступна ~$((FREEZE_WAIT_SEC + 5))с)"
  cutover_flip "$OWNER" "$TO" "$SUB" SYNCING || exit $?

  # ── 5) Прямую подписку срезать, поставить обратную (для отката)
  step "5) Срезаю прямую подписку, ставлю обратную"
  local rev_ok=1
  sql "$dst_dsn" "DROP SUBSCRIPTION $SUB" || {
    echo "⚠️ не удалось удалить $SUB на '$TO' (источник недоступен?) — слот '$SUB' на '$OWNER' держит WAL. Удали вручную позже."
    rev_ok=0
  }
  if [ "$SKIP_REVERSE" = 1 ]; then
    echo "  --skip-reverse: обратной подписки нет — откат только полным re-copy (§6)"
  elif [ "$rev_ok" = 1 ]; then
    # Прямая подписка обязана быть срезана ДО создания обратной — иначе петля репликации.
    sql "$dst_dsn" "CREATE PUBLICATION $PUB_RB FOR TABLES IN SCHEMA $BUCKET"
    psql "$src_dsn" -X -q -v ON_ERROR_STOP=1 -v conn="$(mover_conninfo "$TO")" -v sync="$SUB_SYNCCOMMIT" <<SQL
CREATE SUBSCRIPTION $SUB_RB CONNECTION :'conn' PUBLICATION $PUB_RB
  WITH (copy_data = false, failover = true, synchronous_commit = :'sync');
SQL
    info "обратная подписка: $PUB_RB на '$TO' → $SUB_RB на '$OWNER' (без re-copy)"
  else
    echo "⚠️ обратную подписку НЕ ставлю, пока не удалена прямая (риск петли). После удаления $SUB поставь вручную (§6)."
  fi

  echo
  echo "Готово: '$BUCKET' переехал $OWNER → $TO (etcd: routing=$TO, ACTIVE)."
  echo "  Старый шард '$OWNER' остался замороженным (P1) с копией на момент flip."
  echo "  Откат (пока жива обратная подписка): $0 rollback $BUCKET"
  echo "  Уборка старого шарда позже:          $0 finalize $BUCKET --old-shard $OWNER"
  echo "  Состояние:                           $0 status $BUCKET"
}

# ── status ────────────────────────────────────────────────────────────────────
cmd_status() {
  local s dsn line state target lag_f lag_r
  etcd_alive
  OWNER="$(routing_get "$BUCKET")"
  [ -n "$OWNER" ] || { err "нет $(routing_key "$BUCKET") — бакет не зарегистрирован (P12?)"; exit 3; }
  state="-"; target="-"
  STATUS_JSON="$(status_get "$BUCKET")"
  if [ -n "$STATUS_JSON" ]; then
    state="$(jstr .state "$STATUS_JSON")"
    target="$(jstr .target "$STATUS_JSON")"
    echo "$BUCKET: владелец=$OWNER  state=$state  target=$target  phase=$(jstr .phase "$STATUS_JSON")"
  else
    echo "$BUCKET: владелец=$OWNER  state=ACTIVE (статус-ключа нет)"
  fi
  for s in $(cluster_shards); do
    dsn="$(shard_dsn "$s")"
    line="  $s:"
    schema_exists "$dsn" "$BUCKET" && line="$line схема=да" || line="$line схема=нет"
    pub_exists "$dsn" "$PUB" && line="$line $PUB"
    sub_exists "$dsn" "$SUB" && line="$line $SUB(готово: $(sub_sync "$dsn" "$SUB"))"
    pub_exists "$dsn" "$PUB_RB" && line="$line $PUB_RB"
    sub_exists "$dsn" "$SUB_RB" && line="$line $SUB_RB"
    lag_f="$(slot_lag "$dsn" "$SUB" 2>/dev/null || echo 0)"
    lag_r="$(slot_lag "$dsn" "$SUB_RB" 2>/dev/null || echo 0)"
    [ "$lag_f" = "0" ] || line="$line лаг($SUB)=${lag_f}B"
    [ "$lag_r" = "0" ] || line="$line лаг($SUB_RB)=${lag_r}B"
    echo "$line"
  done
}

# ── rollback ──────────────────────────────────────────────────────────────────
cmd_rollback() {
  local s old="" old_dsn
  etcd_alive
  OWNER="$(routing_get "$BUCKET")"
  [ -n "$OWNER" ] || { err "нет $(routing_key "$BUCKET") — владелец неизвестен (P12)"; exit 3; }
  STATUS_JSON="$(status_get "$BUCKET")"
  [ -z "$STATUS_JSON" ] || { err "откат возможен только из ACTIVE (сейчас state=$(jstr .state "$STATUS_JSON"))"; exit 3; }

  for s in $(cluster_shards); do
    sub_exists "$(shard_dsn "$s")" "$SUB_RB" && { old="$s"; break; }
  done
  [ -n "$old" ] || { err "обратная подписка $SUB_RB не найдена ни на одном шарде — откат только полным re-copy (§6)"; exit 3; }
  [ "$old" != "$OWNER" ] || { err "странно: $SUB_RB найдена на текущем владельце — разберись вручную"; exit 3; }
  old_dsn="$(shard_dsn "$old")"
  TARGET="$old"

  step "Откат '$BUCKET': $OWNER → $old (через $SUB_RB)"
  confirm "Вернуть бакет на '$old'? (запись недоступна ~$((FREEZE_WAIT_SEC + 5))с)"
  cutover_flip "$OWNER" "$old" "$SUB_RB" ACTIVE || exit $?

  step "Убираю артефакты обратной репликации, размораживаю владельца"
  sql "$old_dsn" "DROP SUBSCRIPTION $SUB_RB" \
    || echo "⚠️ не удалось удалить $SUB_RB на '$old' — удали вручную (слот на '$OWNER' держит WAL)"
  sql "$(shard_dsn "$OWNER")" "DROP PUBLICATION $PUB_RB" \
    || echo "⚠️ не удалось удалить $PUB_RB на '$OWNER' — удали вручную"
  unfreeze_shard "$old_dsn" "$BUCKET" || { err "не удалось разморозить '$old' — верни GRANT вручную (P1)"; exit 4; }
  info "владелец '$old' разморожен (P1/P5 сняты)"

  echo
  echo "Готово: '$BUCKET' вернулся на '$old' (etcd: routing=$old, ACTIVE)."
  echo "  На '$OWNER' осталась схема '$BUCKET' (данные на момент отката) — убери:"
  echo "    $0 finalize $BUCKET --old-shard $OWNER"
}

# ── finalize ──────────────────────────────────────────────────────────────────
cmd_finalize() {
  local owner_dsn old_dsn slot
  valid_shard "$OLD_SHARD" || { err "неизвестный шард '$OLD_SHARD'"; exit 2; }
  etcd_alive
  OWNER="$(routing_get "$BUCKET")"
  [ -n "$OWNER" ] || { err "нет $(routing_key "$BUCKET") — владелец неизвестен (P12)"; exit 3; }
  STATUS_JSON="$(status_get "$BUCKET")"
  [ -z "$STATUS_JSON" ] || { err "finalize возможен только из ACTIVE (сейчас state=$(jstr .state "$STATUS_JSON"))"; exit 3; }
  [ "$OLD_SHARD" != "$OWNER" ] || { err "--old-shard ($OLD_SHARD) совпадает с текущим владельцем!"; exit 3; }
  owner_dsn="$(shard_dsn "$OWNER")"
  old_dsn="$(shard_dsn "$OLD_SHARD")"

  step "Уборка '$BUCKET' на '$OLD_SHARD' (владелец '$OWNER' не трогается)"
  echo "  ⚠️ Схема '$BUCKET' будет удалена на '$OLD_SHARD' СО ВСЕМИ ДАННЫМИ."
  confirm "Удалить?"

  # 1) подписки срезаем первыми — они держат слоты (и WAL) на владельце
  sub_exists "$old_dsn" "$SUB_RB" && sql "$old_dsn" "DROP SUBSCRIPTION $SUB_RB"
  sub_exists "$owner_dsn" "$SUB" && sql "$owner_dsn" "DROP SUBSCRIPTION $SUB"
  # 2) публикации и схема
  pub_exists "$old_dsn" "$PUB" && sql "$old_dsn" "DROP PUBLICATION $PUB"
  pub_exists "$owner_dsn" "$PUB_RB" && sql "$owner_dsn" "DROP PUBLICATION $PUB_RB"
  # 3) P8: осиротевшие tablesync-слоты (failover приёмника посреди copy
  #    рестартует синхронизацию таблицы НОВЫМ слотом, старый остаётся сиротой)
  while IFS= read -r slot; do
    [ -n "$slot" ] || continue
    if [ "$(scalar "$old_dsn" "SELECT active FROM pg_replication_slots WHERE slot_name='$slot'")" = "t" ]; then
      echo "  ⚠️ sync-слот $slot ещё активен — пропускаю"
      continue
    fi
    scalar "$old_dsn" "SELECT pg_drop_replication_slot('$slot')" >/dev/null && info "осиротевший sync-слот $slot удалён (P8)"
  done <<<"$(scalar "$old_dsn" "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE '${SUB}_sync_%'" 2>/dev/null || true)"
  if schema_exists "$old_dsn" "$BUCKET"; then
    sql "$old_dsn" "DROP SCHEMA $BUCKET CASCADE"
    info "схема '$BUCKET' удалена на '$OLD_SHARD'"
  else
    info "схемы на '$OLD_SHARD' уже не было"
  fi
  echo "Уборка завершена. Проверь: $0 status $BUCKET"
}

# ── запуск ────────────────────────────────────────────────────────────────────
CMD="${1:-}"
CLUSTER=""
# ведущий --cluster допустим до команды (usage: [--cluster <C>] move ...)
if [ "$CMD" = "--cluster" ]; then CLUSTER="${2:-}"; shift 2; CMD="${1:-}"; fi
[ -n "$CMD" ] && shift || usage
BUCKET="" TO="" OLD_SHARD="" ASSUME_YES=0 SKIP_REVERSE=0 RESUME=0
while [ $# -gt 0 ]; do
  case "$1" in
    --cluster)      CLUSTER="${2:-}"; shift 2 ;;
    --to)           TO="${2:-}"; shift 2 ;;
    --old-shard)    OLD_SHARD="${2:-}"; shift 2 ;;
    --yes|-y)       ASSUME_YES=1; shift ;;
    --skip-reverse) SKIP_REVERSE=1; shift ;;
    --resume)       RESUME=1; shift ;;
    -h|--help)      usage ;;
    *)              if [ -z "$BUCKET" ]; then BUCKET="$1"; else usage; fi; shift ;;
  esac
done
[ -n "$CLUSTER" ] && cluster_set "$CLUSTER"

require_bins psql pg_dump jq etcdctl
[ -n "$BUCKET" ] || usage
valid_bucket "$BUCKET" || { err "неверное имя бакета '$BUCKET' (шаблон: ^[a-z][a-z0-9_]*$)"; exit 2; }

# Имена артефактов переезда (§5–§6)
PUB="pub_${BUCKET}"        # прямая публикация: на старом шарде, пока идёт переезд
SUB="sub_${BUCKET}"        # прямая подписка: на новом шарде, пока идёт переезд
PUB_RB="pub_${BUCKET}_rb"  # обратная публикация: на новом владельце после flip
SUB_RB="sub_${BUCKET}_rb"  # обратная подписка: на старом шарде после flip

case "$CMD" in
  move)     [ -n "$TO" ] || usage; cmd_move ;;
  status)   cmd_status ;;
  rollback) cmd_rollback ;;
  finalize) [ -n "$OLD_SHARD" ] || usage; cmd_finalize ;;
  *)        usage ;;
esac
