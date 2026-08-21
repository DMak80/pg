#!/usr/bin/env bash
# scripts/abort-move.sh
#
# P7 (arch/12-bucket-pitfalls.md): отмена незавершённого переезда и уборка его
# артефактов. Состояние переезда живёт в etcd-контрол-плейне.
#
# Модель («Референс топологии» в 12-bucket-pitfalls.md; всё под префиксом
# кластера /clusters/<C>/):
#   .../buckets/routing/<bucket> → "shard1"                                 — владелец (авторитет)
#   .../buckets/status/<bucket>  → {"state":"SYNCING","target":"shard2",...} — только при переезде
#   нет статус-ключа = бакет ACTIVE.
#
# Использование:
#   ./scripts/abort-move.sh list                     # кто застрял: статус-ключи etcd
#   ./scripts/abort-move.sh artifacts <bucket>       # инвентаризация артефактов (read-only)
#   ./scripts/abort-move.sh abort <bucket> [--yes] [--force]
#
#   без --cluster (и без CLUSTER_NAME в buckets.env) list показывает все
#   кластеры etcd; artifacts/abort требуют конкретный кластер.
#
# Порядок abort (журнал в etcd СТРОГО до манипуляций с БД):
#   1) etcd: владелец (routing, обязателен) + статус переезда (обязателен: нет
#      статуса = ACTIVE, откатывать нечего — пост-flip артефакты убирает
#      move-bucket.sh finalize);
#   2) инвентаризация артефактов на ВСЕХ шардах (подписки/слоты/публикации/
#      схемы). Шард недоступен → журнал с phase=blocked и выход: с неполной
#      картиной уборку не начинаем;
#   3) ★ журнал уборки в ТОТ ЖЕ статус-ключ: state=ABORTING + план (что и где
#      будет удалено) + phase. Крах скрипта посреди уборки оставляет в etcd
#      самодокументирующийся след; повторный запуск abort продолжает;
#   4) уборка БД идемпотентно (каждый шаг перепроверяет существование объекта),
#      в порядке: подписки (везде) → слоты → публикации → re-GRANT на владельце
#      (снятие заморозки P1/P5) → DROP SCHEMA на НЕ-владельцах (последним,
#      с данными). Схема владельца не трогается никогда;
#   5) контрольная инвентаризация: не осталось ничего, кроме схемы владельца;
#   6) etcd: удалить статус-ключ (= бакет снова ACTIVE у владельца). routing
#      не меняется: abort ≠ переезд, бакет остаётся у текущего владельца.
#
# DROP SUBSCRIPTION требует доступного источника (срезает слот удалённо).
# Если источник недоступен: DISABLE → SET (slot_name = NONE) → DROP — слот на
# источнике остаётся сиротой и добивается отдельным шагом (terminate walsender'а
# + pg_drop_replication_slot).
#
# Защита от убийства живого переезда: если статус свежее ABORT_MIN_AGE_SEC
# секунд — отказ (mover, возможно, ещё работает); --force ломает защиту.
# Два abort параллельно не запускать (журнал один, эксклюзивности нет).
#
# routing == target (flip прошёл, статус-ключ завис — вырожденный случай
# неатомарного cutover; только с --force) — abort работает как ДОВЕДЕНИЕ
# перевода (finalize): владелец-приёмник не трогается, старый шард вычищается,
# sequences владельца доводятся до последних выданных на старом шарде
# (P6: последовательности не реплицируются; если cutover прошёл без шага 4.5,
# счётчик у владельца отстаёт → гарантированные duplicate key; setval только
# ВПЕРЁД — пост-flip записи уже расходовали значения владельца).
#
# Конфиг: configs/buckets/buckets.env — ETCD_ENDPOINTS, APP_ROLE,
# APP_GRANT_CREATE, ABORT_MIN_AGE_SEC (см. buckets.env.example).
#
# Коды выхода: 2 — использование; 3 — отказ до записи журнала (состояние/
# guards); 4 — сбой/блокировка уборки (журнал в etcd уже есть); 9 — конфиг
# или недоступен etcd.

set -euo pipefail
export PGCONNECT_TIMEOUT="${PGCONNECT_TIMEOUT:-5}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
. "$SCRIPT_DIR/buckets-common.sh"

usage() {
  cat >&2 <<EOF
Usage:
  $0 [--cluster <C>] list
  $0 --cluster <C> artifacts <bucket>
  $0 --cluster <C> abort <bucket> [--yes] [--force]
EOF
  exit 2
}

confirm() {
  [ "$ASSUME_YES" = 1 ] && return 0
  local a=""
  read -rp "$1 [введите YES]: " a
  [ "${a:-}" = "YES" ] || { echo "Отменено."; exit 1; }
}

# ── etcd (контрол-плейн) ──────────────────────────────────────────────────────
# Хелперы ect/etcd_value/etcd_key_exists/etcd_prefix_keys/etcd_alive/jstr —
# общие, в buckets-common.sh.

# ── Инвентаризация артефактов на всех шардах ──────────────────────────────────
# ARTIFACTS — строки "шард|тип|имя" (типы: sub/slot/pub/schema);
# UNREACHABLE — шарды, куда не достучались (инвентаризация неполна).
ARTIFACTS=""
UNREACHABLE=""
scan_artifacts() {
  ARTIFACTS=""; UNREACHABLE=""
  local s dsn
  for s in $(cluster_shards); do
    dsn="$(shard_dsn "$s")"
    if ! scalar "$dsn" 'SELECT 1' >/dev/null 2>&1; then
      UNREACHABLE+="$s"$'\n'
      continue
    fi
    if sub_exists "$dsn" "$SUB";    then ARTIFACTS+="$s|sub|$SUB"$'\n'; fi
    if sub_exists "$dsn" "$SUB_RB"; then ARTIFACTS+="$s|sub|$SUB_RB"$'\n'; fi
    if slot_exists "$dsn" "$SUB";    then ARTIFACTS+="$s|slot|$SUB"$'\n'; fi
    if slot_exists "$dsn" "$SUB_RB"; then ARTIFACTS+="$s|slot|$SUB_RB"$'\n'; fi
    if pub_exists "$dsn" "$PUB";    then ARTIFACTS+="$s|pub|$PUB"$'\n'; fi
    if pub_exists "$dsn" "$PUB_RB"; then ARTIFACTS+="$s|pub|$PUB_RB"$'\n'; fi
    if schema_exists "$dsn" "$BUCKET"; then ARTIFACTS+="$s|schema|$BUCKET"$'\n'; fi
  done
  return 0
}

print_plan() {
  echo "владелец (routing): $OWNER"
  if [ -n "${PREV_STATE:-}" ]; then
    echo "прерванный переезд: state=$PREV_STATE target=${TARGET:--}"
  fi
  echo "артефакты:"
  if [ -z "$ARTIFACTS" ]; then
    echo "  (в БД ничего не найдено — уборка сведётся к etcd)"
  fi
  local s t n
  while IFS='|' read -r s t n; do
    [ -n "$s" ] || continue
    case "$t" in
      schema) if [ "$s" = "$OWNER" ]; then
                echo "  $s: схема $n (владелец — НЕ трогается)"
              else
                echo "  $s: схема $n  ★ удалится С ДАННЫМИ (копия переезда)"
              fi ;;
      sub)    echo "  $s: подписка $n (срезается; её слот на источнике уйдёт с ней)" ;;
      slot)   echo "  $s: слот $n" ;;
      pub)    echo "  $s: публикация $n" ;;
    esac
  done <<<"$ARTIFACTS"
}

# ── Журнал уборки: тот же статус-ключ, state=ABORTING ─────────────────────────
PLAN_JSON="[]"
JOURNAL_STARTED="$(date +%s)"
journal_set() { # $1 = phase, $2 = last_error (опц.)
  local payload
  payload="$(jq -n \
    --arg bucket "$BUCKET" --arg prev "${PREV_STATE:-}" --arg owner "$OWNER" --arg target "${TARGET:-}" \
    --argjson started "$JOURNAL_STARTED" --argjson updated "$(date +%s)" \
    --arg phase "$1" --arg err "${2:-}" --argjson plan "$PLAN_JSON" \
    --argjson unreach "$(jq -Rn '[inputs | select(length > 0)]' <<<"$UNREACHABLE")" \
    '{bucket:$bucket, state:"ABORTING", prev_state:$prev, owner:$owner, target:$target,
      started_unix:$started, updated_unix:$updated, phase:$phase,
      last_error:(if $err == "" then null else $err end),
      plan:$plan, unreachable_shards:$unreach}')"
  ect put "$STATUS_KEY" "$payload" >/dev/null
}

fail_abort() { # $1 = причина; журнал уже получает phase=failed
  journal_set "failed" "$1"
  err "$1"
  exit 4
}

# ── Шаги уборки (идемпотентны: перепроверяют существование объекта) ────────────
# Вызываются из if ! ... — set -e внутри подавлен, поэтому каждый деструктивный
# вызов обязан сам вернуть 1 при сбое.

drop_sub() { # <dsn> <sub>
  if ! sub_exists "$1" "$2"; then info "подписка $2 уже отсутствует"; return 0; fi
  if sql "$1" "DROP SUBSCRIPTION $2" >/dev/null 2>&1; then
    info "подписка $2 удалена (слот на источнике срезан удалённо)"
    return 0
  fi
  echo "  DROP SUBSCRIPTION $2 не прошёл (источник недоступен?) — срезаю локально, слот останется сиротой"
  sql "$1" "ALTER SUBSCRIPTION $2 DISABLE" || return 1
  sql "$1" "ALTER SUBSCRIPTION $2 SET (slot_name = NONE)" || return 1
  sql "$1" "DROP SUBSCRIPTION $2" || return 1
  info "подписка $2 удалена локально (slot_name=NONE) — осиротевший слот добью шагом слотов"
}

cleanup_subs() {
  local s t n
  while IFS='|' read -r s t n; do
    [ "$t" = "sub" ] || continue
    drop_sub "$(shard_dsn "$s")" "$n" || return 1
  done <<<"$ARTIFACTS"
}

cleanup_slots() {
  local s t n dsn act i
  while IFS='|' read -r s t n; do
    [ "$t" = "slot" ] || continue
    dsn="$(shard_dsn "$s")"
    if ! slot_exists "$dsn" "$n"; then info "слот $n уже отсутствует на '$s'"; continue; fi
    act="$(scalar "$dsn" "SELECT active FROM pg_replication_slots WHERE slot_name='$n'" 2>/dev/null || true)"
    if [ "$act" = "t" ]; then
      # walsender ещё держит слот (подписку только что срезали) — глушим и ждём
      scalar "$dsn" "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name='$n' AND active" >/dev/null || return 1
      for i in 1 2 3 4 5; do
        sleep 1
        act="$(scalar "$dsn" "SELECT active FROM pg_replication_slots WHERE slot_name='$n'" 2>/dev/null || true)"
        if [ "$act" = "f" ]; then break; fi
      done
    fi
    if [ "$act" != "f" ]; then
      err "слот $n на '$s' всё ещё активен — кто-то читает; разберись вручную"
      return 1
    fi
    scalar "$dsn" "SELECT pg_drop_replication_slot('$n')" >/dev/null || return 1
    info "слот $n удалён на '$s'"
  done <<<"$ARTIFACTS"
}

cleanup_pubs() {
  local s t n dsn
  while IFS='|' read -r s t n; do
    [ "$t" = "pub" ] || continue
    dsn="$(shard_dsn "$s")"
    if ! pub_exists "$dsn" "$n"; then info "публикация $n уже отсутствует на '$s'"; continue; fi
    sql "$dsn" "DROP PUBLICATION $n" || return 1
    info "публикация $n удалена на '$s'"
  done <<<"$ARTIFACTS"
}

unfreeze_owner() { # снятие заморозки P1 (DML/sequences) и моратория P5 (CREATE)
  local dsn
  dsn="$(shard_dsn "$OWNER")"
  if ! schema_exists "$dsn" "$BUCKET"; then
    info "схемы '$BUCKET' на владельце '$OWNER' нет — размораживать нечего"
    return 0
  fi
  if [ -z "${APP_ROLE:-}" ]; then
    echo "  ⚠️ APP_ROLE не задан: если переезд успел заморозить бакет (P1), верни GRANT вручную"
    return 0
  fi
  # app-роль попадает в SQL как идентификатор — валидируем как имя бакета
  valid_bucket "$APP_ROLE" || { err "APP_ROLE='$APP_ROLE' не похоже на имя роли (^[a-z][a-z0-9_]*$)"; return 1; }
  sql "$dsn" "GRANT INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA $BUCKET TO $APP_ROLE;
              GRANT USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA $BUCKET TO $APP_ROLE;" || return 1
  info "заморозка P1 снята: DML и sequences возвращены роли $APP_ROLE на '$OWNER'"
  if [ "${APP_GRANT_CREATE:-0}" = 1 ]; then
    sql "$dsn" "GRANT CREATE ON SCHEMA $BUCKET TO $APP_ROLE" || return 1
    info "мораторий P5 снят: CREATE ON SCHEMA возвращён роли $APP_ROLE"
  fi
}

cleanup_schemas() { # схемы НЕ-владельцев, последним (схема владельца не трогается)
  local s t n dsn
  while IFS='|' read -r s t n; do
    [ "$t" = "schema" ] || continue
    if [ "$s" = "$OWNER" ]; then continue; fi
    dsn="$(shard_dsn "$s")"
    if ! schema_exists "$dsn" "$n"; then info "схема $n уже отсутствует на '$s'"; continue; fi
    sql "$dsn" "DROP SCHEMA $n CASCADE" || return 1
    info "схема $n удалена на '$s' (с данными — копия переезда)"
  done <<<"$ARTIFACTS"
}

# Доведение P6 (routing==target): sequence не реплицируются — если cutover
# прошёл без шага 4.5, счётчики владельца отстают от выданных на старом шарде.
# sync_sequences_forward (общая, buckets-common.sh): setval только ВПЕРЁД —
# пост-flip записи уже расходовали значения владельца; issued/next считаются
# на стороне SQL (boolean||text даёт 'true'/'false', парсить в bash нельзя).
sync_sequences_postflip() {
  local s t n
  while IFS='|' read -r s t n; do
    [ "$t" = "schema" ] || continue
    [ "$s" != "$OWNER" ] || continue
    sync_sequences_forward "$(shard_dsn "$s")" "$(shard_dsn "$OWNER")" "$BUCKET" "$s" || return 1
  done <<<"$ARTIFACTS"
}

# ── Команды ────────────────────────────────────────────────────────────────────

cmd_list() {
  local keys k b c v st tg up owner age note
  etcd_alive
  if [ -n "${CLUSTER_NAME:-}" ]; then
    keys="$(etcd_prefix_keys "$(cluster_root)/buckets/status/")"
  else
    # кластер не выбран — показываем незавершённые переезды ВСЕХ кластеров etcd
    keys="$(etcd_prefix_keys /clusters/ | grep '/buckets/status/' || true)"
  fi
  if [ -z "$keys" ]; then
    echo "незавершённых переездов нет (статус-ключей .../buckets/status/ нет; нет ключа = ACTIVE)"
    return 0
  fi
  printf '%-10s %-16s %-9s %-8s %9s  %s\n' CLUSTER BUCKET STATE TARGET 'AGE,с' ЗАМЕТКА
  while IFS= read -r k; do
    [ -n "$k" ] || continue
    c="$(sed -nE 's|^/clusters/([^/]+)/buckets/status/.*$|\1|p' <<<"$k")"
    b="${k##*/}"
    v="$(etcd_value "$k")"
    st="$(jstr .state "$v")"; tg="$(jstr .target "$v")"; up="$(jstr .updated_unix "$v")"
    owner="$(etcd_value "/clusters/$c/buckets/routing/$b")"
    age="-"
    if [ -n "$up" ]; then age=$(( $(date +%s) - up )); fi
    note=""
    if [ "$st" = "ABORTING" ]; then note="уборка не закончена → $0 --cluster $c abort $b"; fi
    if [ -z "$owner" ]; then note="${note:+$note; }нет routing-ключа!"; fi
    printf '%-10s %-16s %-9s %-8s %9s  %s\n' "$c" "$b" "${st:-?}" "${tg:--}" "$age" "$note"
  done <<<"$keys"
}

cmd_artifacts() {
  etcd_alive
  OWNER="$(etcd_value "$ROUTING_KEY")"
  echo "$BUCKET: владелец(routing)=${OWNER:-<нет ключа>}"
  if ! valid_shard "$OWNER"; then
    err "владелец '${OWNER:-}' не зарегистрирован (etcd-реестр кластера '$CLUSTER_NAME' или SHARDS в buckets.env) — DSN неизвестны"
    exit 3
  fi
  scan_artifacts
  if [ -n "$UNREACHABLE" ]; then
    err "недоступны шарды: $(tr '\n' ' ' <<<"$UNREACHABLE")— инвентаризация неполна"
    exit 3
  fi
  PREV_STATE=""; TARGET=""
  print_plan
  if etcd_key_exists "$STATUS_KEY"; then
    echo "статус-ключ: $(etcd_value "$STATUS_KEY")"
  else
    echo "статус-ключа нет (ACTIVE): артефакты без переезда — пост-flip остатки убирает finalize"
  fi
}

cmd_abort() {
  local updated age leftover
  etcd_alive

  step "1) etcd: владелец и статус переезда"
  OWNER="$(etcd_value "$ROUTING_KEY")"
  if [ -z "$OWNER" ]; then
    err "нет $ROUTING_KEY — владелец неизвестен, уборка небезопасна (восстанови контрол-плейн, P12)"
    exit 3
  fi
  if ! valid_shard "$OWNER"; then
    err "владелец '$OWNER' не зарегистрирован (etcd-реестр кластера '$CLUSTER_NAME' или SHARDS в buckets.env)"
    exit 3
  fi
  if ! etcd_key_exists "$STATUS_KEY"; then
    err "статус-ключа $STATUS_KEY нет — бакет ACTIVE, откатывать нечего."
    echo "  Артефакты ПОСЛЕ удачного flip (sub_rb/pub_rb/схема на старом шарде) убирает:" >&2
    echo "    ./move-bucket.sh finalize $BUCKET --old-shard <шард>" >&2
    exit 3
  fi
  STATUS_JSON="$(etcd_value "$STATUS_KEY")"
  if ! jq -e . >/dev/null 2>&1 <<<"$STATUS_JSON"; then
    err "статус-ключ не является JSON: $STATUS_JSON"
    exit 3
  fi
  PREV_STATE="$(jstr .state "$STATUS_JSON")"
  TARGET="$(jstr .target "$STATUS_JSON")"
  updated="$(jstr .updated_unix "$STATUS_JSON")"
  echo "  владелец='$OWNER'  state=$PREV_STATE  target=${TARGET:--}"

  if [ "$PREV_STATE" = "ABORTING" ]; then
    info "найден журнал незавершённой уборки (ABORTING, phase=$(jstr .phase "$STATUS_JSON")) — продолжаю"
    JOURNAL_STARTED="$(jstr .started_unix "$STATUS_JSON")"
    JOURNAL_STARTED="${JOURNAL_STARTED:-$(date +%s)}"
  else
    JOURNAL_STARTED="$(date +%s)"
    if [ -n "$updated" ] && [ "$FORCE" != 1 ]; then
      age=$(( $(date +%s) - updated ))
      if [ "$age" -lt "$ABORT_MIN_AGE_SEC" ]; then
        err "статус обновлён ${age}с назад (< ABORT_MIN_AGE_SEC=$ABORT_MIN_AGE_SEC) — переезд, возможно, ещё жив."
        echo "  Если mover точно мёртв — повтори с --force." >&2
        exit 3
      fi
    fi
  fi
  if [ -n "$TARGET" ] && [ "$TARGET" = "$OWNER" ] && [ "$FORCE" != 1 ]; then
    err "routing уже указывает на target '$TARGET' — похоже, flip прошёл, а статус-ключ остался."
    echo "  Такой abort превратится в уборку СТАРОГО шарда (как finalize). Осознанно: --force." >&2
    exit 3
  fi

  step "2) Инвентаризация артефактов на всех шардах"
  scan_artifacts
  if [ -n "$UNREACHABLE" ]; then
    PLAN_JSON="$(jq -Rn '[inputs | select(length > 0)]' <<<"$ARTIFACTS")"
    journal_set "blocked" "недоступны шарды: $(tr '\n' ' ' <<<"$UNREACHABLE")— инвентаризация неполна, уборка не начиналась"
    info "журнал записан в etcd: state=ABORTING, phase=blocked"
    err "недоступны шарды: $(tr '\n' ' ' <<<"$UNREACHABLE")— с неполной картиной уборку не начинаю."
    echo "  Верни шард и повтори: $0 --cluster $CLUSTER_NAME abort $BUCKET" >&2
    exit 4
  fi
  echo "  схема владельца '$OWNER' остаётся на месте; остальное — в план уборки"

  step "3) План уборки"
  print_plan
  echo "  журнал уборки: $STATUS_KEY (state=ABORTING, план выше)"
  if [ -n "$TARGET" ] && [ "$TARGET" = "$OWNER" ]; then
    echo "  режим ДОВЕДЕНИЯ (routing==target): sequences владельца доводятся до выданных на старом шарде (P6, только вперёд)"
  fi
  echo "  ⚠️ Схемы на НЕ-владельцах удалятся С ДАННЫМИ; etcd-routing не меняется."
  confirm "Отменить переезд '$BUCKET' и убрать артефакты?"

  # ★ журнал ДО любых манипуляций с БД
  PLAN_JSON="$(jq -Rn '[inputs | select(length > 0)]' <<<"$ARTIFACTS")"
  journal_set "db-cleanup"
  info "журнал записан в etcd: state=ABORTING, phase=db-cleanup"

  step "4) Уборка БД"
  journal_set "drop-subscriptions"
  if ! cleanup_subs;   then fail_abort "не удалось срезать подписки (см. вывод выше)"; fi
  journal_set "drop-slots"
  if ! cleanup_slots;  then fail_abort "не удалось удалить слоты (см. вывод выше)"; fi
  journal_set "drop-publications"
  if ! cleanup_pubs;   then fail_abort "не удалось удалить публикации (см. вывод выше)"; fi
  journal_set "unfreeze-owner"
  if ! unfreeze_owner; then fail_abort "не удалось снять заморозку на владельце (см. вывод выше)"; fi
  if [ -n "$TARGET" ] && [ "$TARGET" = "$OWNER" ]; then
    # доведение (routing==target): sequences — ДО удаления старой схемы,
    # иначе последние выданные значения читать будет неоткуда
    journal_set "sync-sequences"
    if ! sync_sequences_postflip; then fail_abort "не удалось довести sequences владельца (P6, см. вывод выше)"; fi
  fi
  journal_set "drop-schema"
  if ! cleanup_schemas; then fail_abort "не удалось удалить схемы на не-владельцах (см. вывод выше)"; fi

  step "5) Контрольная инвентаризация"
  scan_artifacts
  if [ -n "$UNREACHABLE" ]; then
    fail_abort "при контроле недоступны шарды: $(tr '\n' ' ' <<<"$UNREACHABLE")— повтори запуск позже"
  fi
  leftover="$(while IFS='|' read -r s t n; do
    [ -n "$s" ] || continue
    if [ "$s" != "$OWNER" ] || [ "$t" != "schema" ]; then echo "  $s: $t $n"; fi
  done <<<"$ARTIFACTS")"
  if [ -n "$leftover" ]; then
    fail_abort "остались артефакты:$leftover"
  fi
  info "артефактов нет (кроме схемы владельца — так и должно быть)"

  step "6) etcd: возврат в ACTIVE"
  journal_set "done"
  ect del "$STATUS_KEY" >/dev/null
  info "статус-ключ удалён — '$BUCKET' снова ACTIVE у владельца '$OWNER' (нет ключа = ACTIVE)"
}

# ── Разбор аргументов ──────────────────────────────────────────────────────────
CMD="${1:-}"
CLUSTER=""
# ведущий --cluster допустим до команды (usage: [--cluster <C>] list)
if [ "$CMD" = "--cluster" ]; then CLUSTER="${2:-}"; shift 2; CMD="${1:-}"; fi
[ -n "$CMD" ] && shift || usage
case "$CMD" in
  list|artifacts|abort) ;;
  *) usage ;;
esac
BUCKET="" ASSUME_YES=0 FORCE=0
while [ $# -gt 0 ]; do
  case "$1" in
    --cluster) CLUSTER="${2:-}"; shift 2 ;;
    --yes|-y)  ASSUME_YES=1; shift ;;
    --force)   FORCE=1; shift ;;
    -h|--help) usage ;;
    *)         if [ -z "$BUCKET" ]; then BUCKET="$1"; else usage; fi; shift ;;
  esac
done
[ -n "$CLUSTER" ] && cluster_set "$CLUSTER"

for b in psql jq etcdctl; do
  command -v "$b" >/dev/null 2>&1 || { echo "❌ ОШИБКА: не найден '$b' (нужен на машине запуска)" >&2; exit 9; }
done

case "$CMD" in
  list)      [ -z "$BUCKET" ] || usage ;;
  artifacts|abort)
             [ -n "$BUCKET" ] || usage
             [ -n "${CLUSTER_NAME:-}" ] || { echo "❌ ОШИБКА: artifacts/abort требуют кластер: --cluster <C> или CLUSTER_NAME в buckets.env" >&2; exit 2; }
             valid_bucket "$BUCKET" || { err "неверное имя бакета '$BUCKET' (шаблон: ^[a-z][a-z0-9_]*$)"; exit 2; } ;;
esac

PUB="pub_${BUCKET}"        # прямая публикация: на источнике, пока идёт переезд
SUB="sub_${BUCKET}"        # прямая подписка: на приёмнике, пока идёт переезд
PUB_RB="pub_${BUCKET}_rb"  # обратная публикация: на новом владельце после flip
SUB_RB="sub_${BUCKET}_rb"  # обратная подписка: на старом шарде после flip
ROUTING_KEY="$(routing_key "$BUCKET")"
STATUS_KEY="$(status_key "$BUCKET")"

case "$CMD" in
  list)      cmd_list ;;
  artifacts) cmd_artifacts ;;
  abort)     cmd_abort ;;
esac
