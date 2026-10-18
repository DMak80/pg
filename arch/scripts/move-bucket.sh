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
  read -rp "$1 [type YES]: " a
  [ "${a:-}" = "YES" ] || { echo "Cancelled."; exit 1; }
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
    echo "  freeze hit a live writer (lock_timeout), attempt $try/$FREEZE_LOCK_TRIES"
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

  step "Freezing source '$cur' (P1: REVOKE + LOCK barrier; P5: CREATE moratorium)"
  freeze_source "$cur_dsn" "$BUCKET" || { status_put "$fail_state" "freeze-failed"; return 4; }
  status_put FROZEN frozen
  info "FROZEN: the router rejects writes (reads still work)"
  echo "  waiting ${FREEZE_WAIT_SEC}s (router cache TTL — clients stop writing) ..."
  sleep "$FREEZE_WAIT_SEC"

  lsn="$(poll_scalar "$cur_dsn" 'SELECT pg_current_wal_lsn()' 5)" || { status_put "$fail_state" "lsn-failed"; return 4; }
  echo "  last write LSN on '$cur': $lsn"

  echo "  waiting for slot $slot confirmation (lag bytes / sec):"
  while :; do
    ok="$(scalar "$cur_dsn" "SELECT coalesce(bool_and(active AND confirmed_flush_lsn >= '$lsn'::pg_lsn), false)
                             FROM pg_replication_slots WHERE slot_name='$slot'" 2>/dev/null || true)"
    [ "$ok" = "t" ] && break
    if [ "$waited" -ge "$CUTOVER_TIMEOUT_SEC" ]; then
      unfreeze_shard "$cur_dsn" "$BUCKET" || true
      status_put "$fail_state" "catchup-timeout"
      err "the slot did not confirm the LSN within ${CUTOVER_TIMEOUT_SEC}s — unfrozen (state=$fail_state), replication keeps catching up. Rerun the command later."
      return 4
    fi
    sleep "$POLL_INTERVAL_SEC"; waited=$((waited + POLL_INTERVAL_SEC))
    echo "    $(slot_lag "$cur_dsn" "$slot" 2>/dev/null || echo '?') / ${waited}s"
  done
  info "replication caught up (lag 0 as of $lsn)"

  sync_sequences_forward "$cur_dsn" "$new_dsn" "$BUCKET" "$cur" \
    || { unfreeze_shard "$cur_dsn" "$BUCKET" || true; status_put "$fail_state" "sequences-failed"; return 4; }

  # P8-барьер: lag 0 не гарантирует полноту копии (после failover приёмника
  # срез изменений может быть молча пропущен при нулевом лаге) — сверяем строки.
  status_put FROZEN verify
  if ! verify_row_counts "$cur_dsn" "$new_dsn" "$BUCKET"; then
    unfreeze_shard "$cur_dsn" "$BUCKET" || true
    status_put "$fail_state" "verify-failed"
    err "source/target row count check DID NOT match — the copy is defective (P8: target failover?)."
    echo "  Abort the move (abort-move.sh abort $BUCKET) and start over — a fresh initial copy will rebuild the data." >&2
    return 4
  fi
  info "P8: row counts match (count over all tables)"

  status_put FROZEN flip
  if ! etcd_flip "$cur" "$new"; then
    err "etcd flip transaction failed (routing changed underneath?) — the freeze is LEFT IN PLACE, resolve manually!"
    return 5
  fi
  info "atomic flip: routing '$BUCKET' → '$new', status key deleted (no key = ACTIVE)"
  # P12: снапшот точки «переключил на нового владельца»: flip атомарен и
  # статус-ключа уже нет — restore из него даёт сразу согласованную карту.
  # Flip уже случился — сбоем снапшота move не роняем, только громко просим.
  etcd_snapshot "flip-${BUCKET}-${new}" \
    || echo "⚠️ P12: take a snapshot manually: restore-cluster.sh [--cluster $CLUSTER_NAME] snapshot flip-${BUCKET}"
}

# ── move ──────────────────────────────────────────────────────────────────────
cmd_move() {
  local state target sub_on_dst schema_on_dst mover_src max_fails fail_streak s ready total last
  valid_shard "$TO" || { err "unknown shard '$TO' (cluster '$CLUSTER_NAME' etcd registry or SHARDS in buckets.env)"; exit 2; }

  etcd_alive
  OWNER="$(routing_get "$BUCKET")"
  [ -n "$OWNER" ] || { err "$(routing_key "$BUCKET") missing — owner unknown, move impossible (restore the control plane, P12)"; exit 3; }
  valid_shard "$OWNER" || { err "owner '$OWNER' is not described in buckets.env (SHARDS)"; exit 9; }
  [ "$OWNER" != "$TO" ] || { err "the bucket is already on '$TO'"; exit 3; }
  TARGET="$TO"
  local src_dsn dst_dsn
  src_dsn="$(shard_dsn "$OWNER")"
  dst_dsn="$(shard_dsn "$TO")"

  # ── 0) Предполётные проверки
  step "0) Preflight checks"
  STATUS_JSON="$(status_get "$BUCKET")"
  state=""; target=""
  if [ -n "$STATUS_JSON" ]; then
    state="$(jstr .state "$STATUS_JSON")"
    target="$(jstr .target "$STATUS_JSON")"
    MOVE_STARTED="$(jstr .started_unix "$STATUS_JSON")"
    MOVE_STARTED="${MOVE_STARTED:-$(date +%s)}"
    case "$state" in
      SYNCING|FROZEN)
        [ "$target" = "$TO" ] || { err "a move to '${target:-?}' is already in progress, but '$TO' was requested"; exit 3; }
        info "unfinished move to '$TO' (state=$state) — continuing" ;;
      ABORTING) err "cleanup of the aborted move is not finished (state=ABORTING) — finish it first: abort-move.sh abort $BUCKET"; exit 3 ;;
      *) err "unexpected status state: $state"; exit 3 ;;
    esac
  else
    info "no status key — the bucket is ACTIVE, starting a new move"
  fi

  [ "$(poll_scalar "$src_dsn" 'SELECT 1' 3)" = "1" ] || { err "source shard '$OWNER' is unreachable"; exit 3; }
  [ "$(poll_scalar "$dst_dsn" 'SELECT 1' 3)" = "1" ] || { err "target shard '$TO' is unreachable"; exit 3; }
  schema_exists "$src_dsn" "$BUCKET" || { err "schema '$BUCKET' is missing on '$OWNER'?!"; exit 3; }

  [ -n "${APP_ROLE:-}" ] || { err "APP_ROLE is not set in buckets.env — nothing to freeze at cutover (P1)"; exit 9; }
  valid_bucket "$APP_ROLE" || { err "APP_ROLE='$APP_ROLE' does not look like a role name (^[a-z][a-z0-9_]*$)"; exit 9; }

  local wl
  wl="$(scalar "$src_dsn" 'SHOW wal_level')"
  [ "$wl" = "logical" ] || { err "wal_level='$wl' on '$OWNER', need 'logical' (§4, cluster restart)"; exit 3; }

  local max_slots used_slots max_senders used_senders
  max_slots="$(scalar "$src_dsn" "SELECT setting::int FROM pg_settings WHERE name='max_replication_slots'")"
  used_slots="$(scalar "$src_dsn" 'SELECT count(*) FROM pg_replication_slots')"
  [ "$used_slots" -lt "$max_slots" ] || { err "slots on '$OWNER' are exhausted ($used_slots/$max_slots)"; exit 3; }
  max_senders="$(scalar "$src_dsn" "SELECT setting::int FROM pg_settings WHERE name='max_wal_senders'")"
  used_senders="$(scalar "$src_dsn" 'SELECT count(*) FROM pg_stat_replication')"
  [ "$used_senders" -lt "$max_senders" ] || { err "walsenders on '$OWNER' are exhausted ($used_senders/$max_senders)"; exit 3; }

  # P4-префлайт: инвалидированные слоты на источнике — прошлый переезд умер от WAL-лимита
  local lost
  lost="$(scalar "$src_dsn" "SELECT count(*) FROM pg_replication_slots WHERE wal_status='lost'")"
  [ "$lost" = "0" ] || echo "  ⚠️ '$OWNER' has $lost slot(s) with wal_status='lost' (P4) — a previous subscription died; clean up: abort/finalize"

  mover_src="$(mover_conninfo "$OWNER")"
  [ "$(scalar "$mover_src" 'SELECT 1')" = "1" ] \
    || { err "the mover role is unreachable on '$OWNER' (MOVER_CONNINFO_${OWNER})"; exit 3; }
  [ "$(scalar "$mover_src" 'SELECT rolsuper OR rolreplication FROM pg_roles WHERE rolname=current_user')" = "t" ] \
    || { err "the mover role on '$OWNER' lacks the REPLICATION attribute (§4)"; exit 3; }

  # P8: remote_apply подписки работает только при живом sync-standby у приёмника
  check_sync_standby "$dst_dsn" "$TO" || exit 3

  sub_on_dst="f"; sub_exists "$dst_dsn" "$SUB" && sub_on_dst="t"
  schema_on_dst="f"; schema_exists "$dst_dsn" "$BUCKET" && schema_on_dst="t"
  if [ "$sub_on_dst" = "f" ] && [ "$schema_on_dst" = "t" ]; then
    if [ "$RESUME" = 0 ]; then
      err "schema '$BUCKET' already exists on '$TO' without a subscription (leftover of a failed run?)"
      echo "  Either drop it and start again:  psql <dsn-$TO> -c 'DROP SCHEMA $BUCKET CASCADE'" >&2
      echo "  Or continue with --resume if this is fully applied DDL without data." >&2
      exit 3
    fi
    # --resume допустим только для ПУСТОЙ схемы: copy_data=true в непустую даст дубликаты
    local cnt_q cnt
    cnt_q="$(scalar "$dst_dsn" "SELECT 'SELECT '||coalesce(string_agg('(SELECT count(*) FROM '||quote_ident(n.nspname)||'.'||quote_ident(c.relname)||')', '+'), '0')
                               FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                               WHERE n.nspname='$BUCKET' AND c.relkind='r'")"
    cnt="$(scalar "$dst_dsn" "$cnt_q")"
    [ "$cnt" = "0" ] || {
      err "the schema on '$TO' is not empty ($cnt rows) — these are data leftovers, not a failed DDL."
      echo "  The data lives at owner '$OWNER'. Drop the schema on '$TO' and run without --resume:" >&2
      echo "    psql <dsn-$TO> -c 'DROP SCHEMA $BUCKET CASCADE'" >&2
      exit 3
    }
    info "--resume: the schema on '$TO' is empty — continuing"
  fi
  pub_exists "$dst_dsn" "$PUB_RB" && { err "$PUB_RB remains on '$TO' — resolve it first (finalize?)"; exit 3; }
  sub_exists "$src_dsn" "$SUB_RB" && { err "$SUB_RB remains on '$OWNER' — finalize the previous move first"; exit 3; }

  status_put SYNCING ddl
  # P12: снапшот обязателен в точках переезда — «после начала»: restore при
  # потере etcd получит карту с зафиксированным SYNCING (сужает сверку)
  etcd_snapshot "move-${BUCKET}-start" \
    || { err "no start snapshot — not starting the move (P12: restore-cluster.sh)"; exit 4; }
  info "etcd: SYNCING $OWNER → $TO (the bucket keeps serving writes)"
  echo "  ⚠️ Reminder: until the move completes — DDL moratorium on '$BUCKET'."

  # ── 1) Перенос DDL + гранты app-роли
  if [ "$sub_on_dst" = "t" ] || [ "$schema_on_dst" = "t" ]; then
    step "1) DDL skipped (resuming: the schema on '$TO' already exists)"
  else
    step "1) DDL transfer: pg_dump --schema-only $OWNER → $TO"
    pg_dump --schema-only --schema="$BUCKET" --no-owner --no-privileges "$src_dsn" \
      | psql "$dst_dsn" -X -q -v ON_ERROR_STOP=1 >/dev/null \
      || { err "DDL failed to apply on '$TO'; rerun (drop the schema on '$TO' or use --resume)"; exit 4; }
    info "DDL transferred"
  fi
  grant_app_role "$dst_dsn" "$BUCKET" || { err "failed to grant $APP_ROLE on '$TO' (§4)"; exit 4; }
  info "base grants for $APP_ROLE on '$TO' are in place"

  # P5: двойная сверка инвентаря схем (мораторий могли нарушить до переезда)
  local inv_src inv_dst
  inv_src="$(schema_inventory "$src_dsn" "$BUCKET")"
  inv_dst="$(schema_inventory "$dst_dsn" "$BUCKET")"
  if [ "$inv_src" != "$inv_dst" ]; then
    err "inventory of '$BUCKET' differs between '$OWNER' and '$TO' — was the DDL moratorium (P5) violated?"
    diff <(echo "$inv_src") <(echo "$inv_dst") || true
    exit 4
  fi
  info "P5: schema inventories are identical (tables/sequences/views)"

  # ── 2) Публикация на источнике, подписка на приёмнике
  step "2) PUBLICATION '$PUB' on '$OWNER' → SUBSCRIPTION '$SUB' on '$TO'"
  if pub_exists "$src_dsn" "$PUB"; then
    info "publication already exists (resuming)"
  else
    sql "$src_dsn" "CREATE PUBLICATION $PUB FOR TABLES IN SCHEMA $BUCKET"
    info "publication created"
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
    info "subscription created (copy_data=true, failover=true, synchronous_commit=$SUB_SYNCCOMMIT), initial copy started"
  else
    info "subscription already exists (resuming)"
  fi

  # ── 3) Ожидание initial copy (без общего таймаута: большой бакет копируется
  #       часами; обрывы соединения tolerated — у приёмника может идти failover)
  step "3) Waiting for initial copy (the bucket stays writable; Ctrl+C is safe — rerun afterwards)"
  status_put SYNCING copy-wait
  max_fails=$(( CONN_FAIL_BUDGET_SEC / POLL_INTERVAL_SEC )); fail_streak=0; last=""
  while :; do
    s="$(sub_sync "$dst_dsn" "$SUB" 2>/dev/null || true)"
    if [ -z "$s" ]; then
      fail_streak=$((fail_streak + 1))
      if [ "$fail_streak" -ge "$max_fails" ]; then
        err "target '$TO' has been unreachable for over ${CONN_FAIL_BUDGET_SEC}s (failover taking too long?) — rerun later (it will resume from here)"
        exit 4
      fi
      sleep "$POLL_INTERVAL_SEC"; continue
    fi
    fail_streak=0
    if [ "$s" != "$last" ]; then
      echo "  tables ready: ${s%%/*}/${s##*/}, slot lag: $(slot_lag "$src_dsn" "$SUB" 2>/dev/null || echo '?') bytes"
      last="$s"
    fi
    [ "${s%%/*}" = "${s##*/}" ] && break
    sleep "$POLL_INTERVAL_SEC"
  done
  info "initial copy finished, the subscription is streaming changes"

  # ── 4) Cutover
  step "4) Cutover: write freeze → lag 0 → sequences → row count check → atomic flip"
  confirm "Switch '$BUCKET': $OWNER → $TO? (writes will be unavailable for ~$((FREEZE_WAIT_SEC + 5))s)"
  cutover_flip "$OWNER" "$TO" "$SUB" SYNCING || exit $?

  # ── 5) Прямую подписку срезать, поставить обратную (для отката)
  step "5) Dropping the forward subscription, creating the reverse one"
  local rev_ok=1
  sql "$dst_dsn" "DROP SUBSCRIPTION $SUB" || {
    echo "⚠️ failed to drop $SUB on '$TO' (source unreachable?) — the '$SUB' slot on '$OWNER' holds WAL. Drop it manually later."
    rev_ok=0
  }
  if [ "$SKIP_REVERSE" = 1 ]; then
    echo "  --skip-reverse: no reverse subscription — rollback only via a full re-copy (§6)"
  elif [ "$rev_ok" = 1 ]; then
    # Прямая подписка обязана быть срезана ДО создания обратной — иначе петля репликации.
    sql "$dst_dsn" "CREATE PUBLICATION $PUB_RB FOR TABLES IN SCHEMA $BUCKET"
    psql "$src_dsn" -X -q -v ON_ERROR_STOP=1 -v conn="$(mover_conninfo "$TO")" -v sync="$SUB_SYNCCOMMIT" <<SQL
CREATE SUBSCRIPTION $SUB_RB CONNECTION :'conn' PUBLICATION $PUB_RB
  WITH (copy_data = false, failover = true, synchronous_commit = :'sync');
SQL
    info "reverse subscription: $PUB_RB on '$TO' → $SUB_RB on '$OWNER' (no re-copy)"
  else
    echo "⚠️ NOT creating the reverse subscription while the forward one remains (loop risk). After dropping $SUB, create it manually (§6)."
  fi

  echo
  echo "Done: '$BUCKET' moved $OWNER → $TO (etcd: routing=$TO, ACTIVE)."
  echo "  The old shard '$OWNER' remains frozen (P1) with a copy as of the flip."
  echo "  Rollback (while the reverse subscription lives): $0 rollback $BUCKET"
  echo "  Old shard cleanup later:                         $0 finalize $BUCKET --old-shard $OWNER"
  echo "  Status:                                          $0 status $BUCKET"
}

# ── status ────────────────────────────────────────────────────────────────────
cmd_status() {
  local s dsn line state target lag_f lag_r
  etcd_alive
  OWNER="$(routing_get "$BUCKET")"
  [ -n "$OWNER" ] || { err "$(routing_key "$BUCKET") missing — the bucket is not registered (P12?)"; exit 3; }
  state="-"; target="-"
  STATUS_JSON="$(status_get "$BUCKET")"
  if [ -n "$STATUS_JSON" ]; then
    state="$(jstr .state "$STATUS_JSON")"
    target="$(jstr .target "$STATUS_JSON")"
    echo "$BUCKET: owner=$OWNER  state=$state  target=$target  phase=$(jstr .phase "$STATUS_JSON")"
  else
    echo "$BUCKET: owner=$OWNER  state=ACTIVE (no status key)"
  fi
  for s in $(cluster_shards); do
    dsn="$(shard_dsn "$s")"
    line="  $s:"
    schema_exists "$dsn" "$BUCKET" && line="$line schema=yes" || line="$line schema=no"
    pub_exists "$dsn" "$PUB" && line="$line $PUB"
    sub_exists "$dsn" "$SUB" && line="$line $SUB(done: $(sub_sync "$dsn" "$SUB"))"
    pub_exists "$dsn" "$PUB_RB" && line="$line $PUB_RB"
    sub_exists "$dsn" "$SUB_RB" && line="$line $SUB_RB"
    lag_f="$(slot_lag "$dsn" "$SUB" 2>/dev/null || echo 0)"
    lag_r="$(slot_lag "$dsn" "$SUB_RB" 2>/dev/null || echo 0)"
    [ "$lag_f" = "0" ] || line="$line lag($SUB)=${lag_f}B"
    [ "$lag_r" = "0" ] || line="$line lag($SUB_RB)=${lag_r}B"
    echo "$line"
  done
}

# ── rollback ──────────────────────────────────────────────────────────────────
cmd_rollback() {
  local s old="" old_dsn
  etcd_alive
  OWNER="$(routing_get "$BUCKET")"
  [ -n "$OWNER" ] || { err "$(routing_key "$BUCKET") missing — owner unknown (P12)"; exit 3; }
  STATUS_JSON="$(status_get "$BUCKET")"
  [ -z "$STATUS_JSON" ] || { err "rollback is possible only from ACTIVE (current state=$(jstr .state "$STATUS_JSON"))"; exit 3; }

  for s in $(cluster_shards); do
    sub_exists "$(shard_dsn "$s")" "$SUB_RB" && { old="$s"; break; }
  done
  [ -n "$old" ] || { err "reverse subscription $SUB_RB not found on any shard — rollback only via a full re-copy (§6)"; exit 3; }
  [ "$old" != "$OWNER" ] || { err "odd: $SUB_RB found on the current owner — resolve manually"; exit 3; }
  old_dsn="$(shard_dsn "$old")"
  TARGET="$old"

  step "Rollback of '$BUCKET': $OWNER → $old (via $SUB_RB)"
  confirm "Move the bucket back to '$old'? (writes unavailable for ~$((FREEZE_WAIT_SEC + 5))s)"
  cutover_flip "$OWNER" "$old" "$SUB_RB" ACTIVE || exit $?

  step "Removing reverse replication artifacts, unfreezing the owner"
  sql "$old_dsn" "DROP SUBSCRIPTION $SUB_RB" \
    || echo "⚠️ failed to drop $SUB_RB on '$old' — drop it manually (the slot on '$OWNER' holds WAL)"
  sql "$(shard_dsn "$OWNER")" "DROP PUBLICATION $PUB_RB" \
    || echo "⚠️ failed to drop $PUB_RB on '$OWNER' — drop it manually"
  unfreeze_shard "$old_dsn" "$BUCKET" || { err "failed to unfreeze '$old' — restore the GRANT manually (P1)"; exit 4; }
  info "owner '$old' unfrozen (P1/P5 lifted)"

  echo
  echo "Done: '$BUCKET' is back on '$old' (etcd: routing=$old, ACTIVE)."
  echo "  A schema '$BUCKET' remains on '$OWNER' (data as of the rollback) — clean it up:"
  echo "    $0 finalize $BUCKET --old-shard $OWNER"
}

# ── finalize ──────────────────────────────────────────────────────────────────
cmd_finalize() {
  local owner_dsn old_dsn slot
  valid_shard "$OLD_SHARD" || { err "unknown shard '$OLD_SHARD'"; exit 2; }
  etcd_alive
  OWNER="$(routing_get "$BUCKET")"
  [ -n "$OWNER" ] || { err "$(routing_key "$BUCKET") missing — owner unknown (P12)"; exit 3; }
  STATUS_JSON="$(status_get "$BUCKET")"
  [ -z "$STATUS_JSON" ] || { err "finalize is possible only from ACTIVE (current state=$(jstr .state "$STATUS_JSON"))"; exit 3; }
  [ "$OLD_SHARD" != "$OWNER" ] || { err "--old-shard ($OLD_SHARD) equals the current owner!"; exit 3; }
  owner_dsn="$(shard_dsn "$OWNER")"
  old_dsn="$(shard_dsn "$OLD_SHARD")"

  step "Cleaning up '$BUCKET' on '$OLD_SHARD' (owner '$OWNER' is not touched)"
  echo "  ⚠️ Schema '$BUCKET' will be dropped on '$OLD_SHARD' WITH ALL ITS DATA."
  confirm "Drop it?"

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
      echo "  ⚠️ sync slot $slot is still active — skipping"
      continue
    fi
    scalar "$old_dsn" "SELECT pg_drop_replication_slot('$slot')" >/dev/null && info "orphaned sync slot $slot dropped (P8)"
  done <<<"$(scalar "$old_dsn" "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE '${SUB}_sync_%'" 2>/dev/null || true)"
  if schema_exists "$old_dsn" "$BUCKET"; then
    sql "$old_dsn" "DROP SCHEMA $BUCKET CASCADE"
    info "schema '$BUCKET' dropped on '$OLD_SHARD'"
  else
    info "the schema on '$OLD_SHARD' was already gone"
  fi
  echo "Cleanup finished. Verify: $0 status $BUCKET"
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
valid_bucket "$BUCKET" || { err "invalid bucket name '$BUCKET' (pattern: ^[a-z][a-z0-9_]*$)"; exit 2; }

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
