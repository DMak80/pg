#!/usr/bin/env bash
# scripts/restore-cluster.sh
#
# P12 (12-bucket-pitfalls.md): потеря/restore etcd-контрол-плейна.
# Данные живут на шардах — список схем bucket_* на шардах восстанавливает
# истину; задача скрипта — сверить восстановленную из снапшота карту с фактом
# и привести её в согласованное состояние (однозначные случаи — автоматически,
# с журналом ДО манипуляций, по образцу abort-move.sh).
#
# Использование:
#   ./scripts/restore-cluster.sh [--cluster <C>] snapshot [label]
#       снапшот ВСЕГО etcd (etcdctl snapshot save пишет файл на клиенте —
#       каталог SNAPSHOT_DIR обязан быть persistence/volume). Тот же вызов —
#       в cron'е для регулярных снапшотов (P12). mover снимает снапшоты сам
#       в точках переезда (после SYNCING и после flip — move-bucket.sh).
#   ./scripts/restore-cluster.sh [--cluster <C>] verify
#       сверка карты etcd с фактом (схемы на шардах): диагноз по каждому
#       расхождению. Код 0 = согласовано, 1 = есть расхождения, 3 = ошибка.
#   ./scripts/restore-cluster.sh [--cluster <C>] heal [--yes]
#       автоприведение ОДНОЗНАЧНЫХ расхождений: routing без схемы (схема
#       ровно на одном другом шарде) и схема без routing. Неоднозначное —
#       окна переездов (схема на двух шардах, зависшие статусы) — НЕ чинится:
#       verify подскажет команду (move --resume / abort / finalize).
#   ./scripts/restore-cluster.sh restore <file.snapshot> [--stand-dir <D>]
#       физическое восстановление etcd из снапшота. Запускать с хоста, где
#       живёт etcd: на docker-стенде — автоматика (compose-проект pgstand),
#       иначе печатает процедуру для прод-хоста (те же шаги руками).
#       ⚠️ Снапшот вернёт ВСЕ кластеры этого etcd к моменту снятия.
#
# После restore ОБЯЗАТЕЛЬНО: verify → heal → verify (карта могла устареть
# относительно последнего flip — снапшоты точек переезда сужают это окно).
#
# Конфиг: configs/buckets/buckets.env (см. buckets.env.example).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
. "$SCRIPT_DIR/buckets-common.sh"

usage() {
  cat >&2 <<EOF
Usage:
  $0 [--cluster <C>] snapshot [label]
  $0 [--cluster <C>] verify
  $0 [--cluster <C>] heal [--yes]
  $0 restore <file.snapshot> [--yes] [--stand-dir <dir>]
EOF
  exit 2
}

confirm() {
  [ "$ASSUME_YES" = 1 ] && return 0
  local a=""
  read -rp "$1 [type YES]: " a
  [ "${a:-}" = "YES" ] || { echo "Cancelled."; exit 1; }
}

# ── сбор фактов: схемы bucket_* на шардах кластера ────────────────────────────
# FACTS — строки "shard bucket" (по одной на схему); недоступный шард = отказ:
# с неполной картиной карту не чиним (принцип P7).
gather_facts() {
  local s dsn sch
  FACTS=""
  for s in $(cluster_shards); do
    dsn="$(shard_dsn "$s")"
    [ "$(poll_scalar "$dsn" 'SELECT 1' 3)" = "1" ] \
      || { err "shard '$s' is unreachable — cannot reconcile with an incomplete picture"; exit 3; }
    while IFS= read -r sch; do
      [ -n "$sch" ] || continue
      FACTS="${FACTS}${s} ${sch}
"
    done <<<"$(scalar "$dsn" "SELECT nspname FROM pg_namespace WHERE nspname LIKE 'bucket_%' ORDER BY 1")"
  done
}

# Классификация одного бакета: CLASS|owner|target|state|hosts
#   OK              — routing → шард со схемой, статуса нет
#   HEAL_CREATE     — routing НЕТ, схема ровно на одном шарде → heal создаст
#   HEAL_REDIRECT   — routing → шард БЕЗ схемы, схема ровно на одном другом → heal переведёт
#   NO_DATA         — схемы нет нигде (данных нет — restore не лечит)
#   AMBIG           — схема на 2+ шардах при неоднозначном routing (окно переезда)
#   MOVE_WIN        — живой статус переезда (SYNCING/FROZEN): move --resume / abort
#   COPY_LEFT       — копия на не-владельцах без статуса: пост-flip, finalize
#   STUCK_STATUS    — routing уже = target, статус завис: abort --force (доведение)
scan_bucket() { # <bucket>
  local b="$1" owner st state target hs nh
  owner="$(routing_get "$b")"
  st="$(status_get "$b")"; state=""; target=""
  [ -n "$st" ] && { state="$(jstr .state "$st")"; target="$(jstr .target "$st")"; }
  hs="$(grep -E "^[a-z0-9_]+ ${b}\$" <<<"$FACTS" | awk '{print $1}' | sort -u | paste -sd, -)"
  nh="$(grep -cE "^[a-z0-9_]+ ${b}\$" <<<"$FACTS" || true)"
  if [ -z "$owner" ]; then
    case "$nh" in
      0) echo "NO_DATA|||$hs" ;;
      1) echo "HEAL_CREATE|||$hs" ;;
      *) echo "AMBIG|||$hs" ;;
    esac
  elif grep -qx "$owner" <<<"$hs"; then
    if [ -n "$state" ] && [ "$target" = "$owner" ]; then echo "STUCK_STATUS|$owner|$target|$state|$hs"
    elif [ -n "$state" ]; then echo "MOVE_WIN|$owner|$target|$state|$hs"
    elif [ "$nh" -gt 1 ]; then echo "COPY_LEFT|$owner|||$hs"
    else echo "OK|$owner|||$hs"; fi
  else
    case "$nh" in
      0) echo "NO_DATA|$owner|||$hs" ;;
      1) echo "HEAL_REDIRECT|$owner|||$hs" ;;
      *) echo "AMBIG|$owner|||$hs" ;;
    esac
  fi
}

# ── verify ────────────────────────────────────────────────────────────────────
cmd_verify() {
  local n i b line cls owner target state hs bad=0 ok=0 copies=0
  etcd_alive
  [ -n "$(cluster_config)" ] \
    || { err "cluster '$CLUSTER_NAME' is not initialized ($(config_key) missing)"; exit 3; }
  n="$(cfg_field buckets)"
  step "P12: reconciling the etcd map with reality — cluster '$CLUSTER_NAME' (N=$n, shards: $(cluster_shards | paste -sd, -))"
  gather_facts

  for i in $(seq 0 $((n - 1))); do
    b="bucket_$i"
    line="$(scan_bucket "$b")"
    IFS='|' read -r cls owner target state hs <<<"$line"
    case "$cls" in
      OK) ok=$((ok + 1)) ;;
      HEAL_CREATE)
        echo "  ❌ $b: no routing — the schema is on '$hs'. Fixed automatically: $0 --cluster $CLUSTER_NAME heal"
        bad=$((bad + 1)) ;;
      HEAL_REDIRECT)
        echo "  ❌ $b: routing='$owner', no schema there — the schema is on '$hs'. Fixed automatically: heal (routing → $hs)"
        bad=$((bad + 1)) ;;
      NO_DATA)
        echo "  ❌ $b: no schema anywhere (routing='${owner:-none}') — no data: restore the shard from a backup (08-operations.md), then heal"
        bad=$((bad + 1)) ;;
      AMBIG)
        echo "  ❌ $b: schema on: $hs (routing='${owner:-none}') — move window (restored from a PRE-flip snapshot?). Manually: move --resume / abort-move abort (P7)"
        bad=$((bad + 1)) ;;
      MOVE_WIN)
        echo "  ⚠️ $b: interrupted move ($state, → '$target', owner '$owner'): move --to $target --resume or abort-move abort"
        copies=$((copies + 1)) ;;
      COPY_LEFT)
        echo "  ⚠️ $b: copy on: $hs — post-flip leftover: finalize --old-shard <a shard from the list>"
        copies=$((copies + 1)) ;;
      STUCK_STATUS)
        echo "  ⚠️ $b: status $state stuck, routing already = target '$target': abort-move abort --force (catch-up completion, P7)"
        copies=$((copies + 1)) ;;
    esac
  done

  # схемы вне диапазона 0..N-1 — завести мимо init нельзя (P23), но факты не лгут
  while IFS= read -r b; do
    [ -n "$b" ] || continue
    case "$b" in
      bucket_[0-9]*)
        [[ "${b#bucket_}" =~ ^[0-9]+$ ]] && [ "${b#bucket_}" -ge "$n" ] \
          && echo "  ⚠️ schema '$b' is outside the 0..$((n - 1)) range (N changed? P18) — check manually" ;;
    esac
  done <<<"$(awk '{print $2}' <<<"$FACTS" | sort -u)"
  echo
  echo "Summary: consistent $ok/$n; mismatches: $bad; move situations: $copies"
  [ "$bad" = 0 ] || return 1
  info "the cluster map is consistent with reality"
}

# ── heal ──────────────────────────────────────────────────────────────────────
# Только HEAL_CREATE / HEAL_REDIRECT. Журнал ДО манипуляции — ключ
# /clusters/<C>/heals/<bucket>: even если heal умрёт посреди, след останется.
heal_tx() { # <bucket> <old|-> <new>
  local b="$1" old="$2" new="$3" out cmp
  ect put "$(cluster_root)/heals/$b" \
    "$(jq -n --arg b "$b" --arg was "$old" --arg now "$new" --argjson ts "$(date +%s)" \
      '{bucket:$b, was:(if $was=="-" then null else $was end), now:$now, reason:"restore-heal", ts:$ts}')" >/dev/null
  if [ "$old" = "-" ]; then
    cmp="$(printf 'version("%s") = "0"\n\nput %s %s\n' "$(routing_key "$b")" "$(routing_key "$b")" "$new")"
  else
    cmp="$(printf 'val("%s") = "%s"\n\nput %s %s\n' "$(routing_key "$b")" "$old" "$(routing_key "$b")" "$new")"
  fi
  out="$(printf '%s\n\n\n' "$cmp" | ect txn 2>/dev/null | head -1)"
  [ "$out" = "SUCCESS" ]
}

cmd_heal() {
  local n i b line cls owner hs plan="" cnt=0
  etcd_alive
  [ -n "$(cluster_config)" ] \
    || { err "cluster '$CLUSTER_NAME' is not initialized ($(config_key) missing)"; exit 3; }
  n="$(cfg_field buckets)"
  step "P12: auto-healing the map — cluster '$CLUSTER_NAME' (unambiguous cases)"
  gather_facts

  for i in $(seq 0 $((n - 1))); do
    b="bucket_$i"
    line="$(scan_bucket "$b")"
    IFS='|' read -r cls owner _ _ hs <<<"$line"
    case "$cls" in
      HEAL_CREATE)
        plan="${plan}  $b: no routing → $hs
"; cnt=$((cnt + 1)) ;;
      HEAL_REDIRECT)
        plan="${plan}  $b: $owner → $hs (no schema on '$owner')
"; cnt=$((cnt + 1)) ;;
    esac
  done

  [ "$cnt" -gt 0 ] || { info "no unambiguous mismatches — nothing to fix (see verify)"; return 0; }
  echo "$plan"
  confirm "Rewrite routing for $cnt bucket(s)? (journal — $(cluster_root)/heals/*)"
  for i in $(seq 0 $((n - 1))); do
    b="bucket_$i"
    line="$(scan_bucket "$b")"
    IFS='|' read -r cls owner _ _ hs <<<"$line"
    case "$cls" in
      HEAL_CREATE)
        heal_tx "$b" "-" "$hs" && info "$b: routing → $hs (created)" \
          || err "$b: routing appeared underneath — skipped, rerun verify" ;;
      HEAL_REDIRECT)
        heal_tx "$b" "$owner" "$hs" && info "$b: routing $owner → $hs" \
          || err "$b: routing changed underneath — skipped, rerun verify" ;;
    esac
  done
  echo "Re-run the check: $0 --cluster $CLUSTER_NAME verify"
}

# ── snapshot ──────────────────────────────────────────────────────────────────
cmd_snapshot() {
  etcd_alive
  etcd_snapshot "${1:-manual}" || exit 1
}

# ── restore (запуск с хоста etcd: стенд — docker-автоматика) ──────────────────
cmd_restore() {
  local f="$SNAPFILE" sd="${STAND_DIR:-$(cd "$SCRIPT_DIR/../stand" 2>/dev/null && pwd)}"
  [ -n "$f" ] || usage
  [ -r "$f" ] || { err "file '$f' is not readable"; exit 2; }

  if command -v docker >/dev/null 2>&1 && [ -n "${sd:-}" ] && [ -f "$sd/docker-compose.yml" ]; then
    local img vname
    img="$(awk '/image: .*etcd/ {print $2; exit}' "$sd/docker-compose.yml")"
    vname="$(awk '/^name:/ {print $2; exit}' "$sd/docker-compose.yml")_etcd-data"
    [ -n "$img" ] || { err "cannot find the etcd image in $sd/docker-compose.yml"; exit 9; }
    step "P12: restoring the stand's etcd from $(basename "$f")"
    echo "  snapshot status:"
    docker run --rm -v "$(cd "$(dirname "$f")" && pwd)":/snap:ro "$img" \
      etcdctl snapshot status "/snap/$(basename "$f")" || { err "the snapshot is corrupt"; exit 3; }
    echo "  ⚠️ The whole etcd (ALL clusters of the stand) will revert to the snapshot moment."
    confirm "Stop etcd and recreate the data-dir from the snapshot?"
    # контейнер именно УДАЛЯЕМ (rm -sf), не stop: volume занят даже
    # остановленным контейнером и не удалится
    docker compose --project-directory "$sd" rm -sf etcd >/dev/null
    docker volume rm "$vname" >/dev/null
    docker run --rm -v "$vname":/var/etcd/data -v "$(cd "$(dirname "$f")" && pwd)":/snap:ro "$img" \
      etcdctl snapshot restore "/snap/$(basename "$f")" --data-dir /var/etcd/data
    docker compose --project-directory "$sd" up -d etcd >/dev/null 2>&1
    local ok=""
    for i in $(seq 1 30); do
      docker exec etcd etcdctl --endpoints=http://localhost:2379 endpoint health >/dev/null 2>&1 && { ok=1; break; }
      sleep 1
    done
    [ -n "$ok" ] || { err "etcd did not come up after restore"; exit 3; }
    info "etcd is up from the snapshot; next, from the ops box: restore-cluster.sh verify → heal"
  else
    step "P12: etcd restore procedure (prod host; performed by the etcd admin)"
    cat <<EOF
  1) check the snapshot:      etcdctl snapshot status $f
  2) restore into a NEW directory (never over the live data-dir!):
       etcdctl snapshot restore $f --data-dir /var/lib/etcd-restored
  3) stop etcd, swap the data-dir (keep the old one), start
     (04-deploy-etcd.md; for a 3+ node cluster — restore on ONE node, the others
     are added as new members via etcdctl member add)
  ⚠️ The snapshot reverts ALL clusters of this etcd to the snapshot moment.
  4) reconcile the map with reality and fix the unambiguous cases:
       $0 --cluster $CLUSTER_NAME verify
       $0 --cluster $CLUSTER_NAME heal
EOF
  fi
}

# ── запуск ────────────────────────────────────────────────────────────────────
CMD="${1:-}"
CLUSTER=""
if [ "$CMD" = "--cluster" ]; then CLUSTER="${2:-}"; shift 2; CMD="${1:-}"; fi
[ -n "$CMD" ] && shift || usage
ASSUME_YES=0 SNAPFILE="" STAND_DIR="" LABEL=""
while [ $# -gt 0 ]; do
  case "$1" in
    --cluster)   CLUSTER="${2:-}"; shift 2 ;;
    --yes|-y)    ASSUME_YES=1; shift ;;
    --stand-dir) STAND_DIR="${2:-}"; shift 2 ;;
    -h|--help)   usage ;;
    *)
      case "$CMD" in
        snapshot) [ -z "$LABEL" ] && LABEL="$1" || usage ;;
        restore)  [ -z "$SNAPFILE" ] && SNAPFILE="$1" || usage ;;
        *)        usage ;;
      esac
      shift ;;
  esac
done
# restore не привязан к кластеру (физический уровень etcd); остальные — да
if [ "$CMD" != "restore" ]; then
  [ -n "$CLUSTER" ] && cluster_set "$CLUSTER"
  [ -n "${CLUSTER_NAME:-}" ] || { err "cluster not set: --cluster or CLUSTER_NAME in buckets.env"; exit 9; }
  require_bins psql jq etcdctl
fi

case "$CMD" in
  snapshot) cmd_snapshot "$LABEL" ;;
  verify)   cmd_verify ;;
  heal)     cmd_heal ;;
  restore)  cmd_restore ;;
  *)        usage ;;
esac
