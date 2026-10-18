#!/usr/bin/env bash
# scripts/restore-system.sh
#
# P22 (12-bucket-pitfalls.md): восстановление шардированной системы в ПРАВИЛЬНОМ
# порядке — etcd-контрол-плейн → шарды → сверка/починка карты. Данные живут на
# шардах и бэкапятся независимо (08-operations.md); контрол-плейн — снапшоты
# etcd (P12, restore-cluster.sh). Этот скрипт — оркестратор порядка: PG-ноды
# он не восстанавливает сам, а проверяет готовность каждого слоя и ведёт по
# шагам; карту при недоступном шарде не чинит (принцип P7/P12: с неполной
# картиной не чиним).
#
# Использование:
#   ./scripts/restore-system.sh [--cluster <C>] plan
#       порядок восстановления + текущее состояние слоёв (read-only).
#   ./scripts/restore-system.sh [--cluster <C>] run [--snapshot <file>] [--yes]
#       исполнение:
#       (1) etcd жив? нет → restore из --snapshot (делегирует
#           restore-cluster.sh restore; ⚠️ снапшот вернёт ВСЕ кластеры этого
#           etcd к моменту снятия);
#       (2) все шарды кластера доступны (dsn из etcd); недоступный →
#           восстанови ноду по 08-operations.md (rebuild-node) и повтори run;
#       (3) verify → heal → verify (restore-cluster.sh, P12).
#
# Восстановление отдельного бакета = восстановление схемы на шарде
# (08-operations.md) + сверка `restore-cluster.sh verify/heal` (routing
# подтянется к факту, однозначные случаи — автоматически).
#
# Конфиг: configs/buckets/buckets.env (см. buckets.env.example).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
. "$SCRIPT_DIR/buckets-common.sh"

usage() {
  cat >&2 <<EOF
Usage:
  $0 [--cluster <C>] plan
  $0 [--cluster <C>] run [--snapshot <file>] [--yes]
EOF
  exit 2
}

confirm() {
  [ "$ASSUME_YES" = 1 ] && return 0
  local a=""
  read -rp "$1 [type YES]: " a
  [ "${a:-}" = "YES" ] || { echo "Cancelled."; exit 1; }
}

# Мягкая проверка etcd (etcd_alive из common завершает процесс — здесь нужно
# решение ветвлением, а не выход).
etcd_up() { ect get / --prefix --limit=1 -w json >/dev/null 2>&1; }

# Шаг (2): доступность каждого шарда кластера (dsn из etcd — как раз
# проверка, что восстановленный контрол-плейн осмыслен). 0 = все доступны.
shards_ready() {
  local s dsn rc=0
  for s in $(cluster_shards); do
    dsn="$(shard_dsn "$s")"
    if [ "$(poll_scalar "$dsn" 'SELECT 1' 3)" = "1" ]; then
      info "shard '$s': reachable"
    else
      err "shard '$s': UNREACHABLE — restore the nodes per 08-operations.md (rebuild-node), then rerun run"
      rc=1
    fi
  done
  return "$rc"
}

print_order() {
  cat <<EOF
  Sharded system restore order (P22):
    1. etcd layer: nodes are up (04-deploy-etcd.md) → data from a snapshot
       (restore-cluster.sh restore <file>; the snapshot covers ALL etcd clusters)
    2. shards: each Patroni cluster restored from its own backups
       (08-operations.md, rebuild-node; shards are managed by Patroni, not us)
    3. map: verify → heal → verify (restore-cluster.sh; routing × the actual
       bucket_* schemas on the shards; unambiguous mismatches are fixed
       automatically with a journal under /clusters/<C>/heals/*)
    4. application: do nothing — the routers keep a watch on etcd and pick up
       the map themselves (P9); when in doubt — a canary on a single bucket
EOF
}

# ── plan: порядок + состояние слоёв (read-only) ───────────────────────────────
cmd_plan() {
  step "P22: system '$CLUSTER_NAME' restore order"
  print_order
  echo
  step "current layer state"
  if etcd_up; then
    info "etcd: reachable ($ETCD_ENDPOINTS)"
    if [ -n "$(cluster_config)" ]; then
      info "cluster '$CLUSTER_NAME': initialized (N=$(cfg_field buckets), shards: $(cluster_shards | paste -sd, -))"
      shards_ready || true
      echo "  map: reconcile separately — $SCRIPT_DIR/restore-cluster.sh --cluster $CLUSTER_NAME verify"
    else
      err "cluster '$CLUSTER_NAME' is NOT initialized in etcd ($(config_key) missing) — step 1: restore from a snapshot"
    fi
  else
    err "etcd is UNREACHABLE ($ETCD_ENDPOINTS) — step 1: bring the layer up (04-deploy-etcd.md) + restore from a snapshot"
  fi
}

# ── run: исполнение по порядку ────────────────────────────────────────────────
cmd_run() {
  # (1) etcd-контрол-плейн
  step "P22 (1/3): etcd control plane"
  if etcd_up; then
    info "etcd: reachable ($ETCD_ENDPOINTS) — no restore needed"
  else
    [ -n "$SNAPFILE" ] || {
      err "etcd is unreachable: bring the layer up (04-deploy-etcd.md) and pass --snapshot <file> (latest: ls ${SNAPSHOT_DIR:-/var/lib/etcd-snapshots})"
      exit 3
    }
    confirm "Restore etcd from $(basename "$SNAPFILE")? (the snapshot reverts ALL clusters of this etcd to the snapshot moment)"
    local yesflag=""
    [ "$ASSUME_YES" = 1 ] && yesflag="--yes"
    "$SCRIPT_DIR/restore-cluster.sh" restore "$SNAPFILE" $yesflag
    etcd_up || { err "etcd is still unreachable after restore"; exit 3; }
    info "etcd: restored from the snapshot"
  fi
  [ -n "$(cluster_config)" ] \
    || { err "cluster '$CLUSTER_NAME' is NOT initialized in etcd after restore (wrong snapshot? see plan)"; exit 3; }
  info "cluster '$CLUSTER_NAME': initialized (N=$(cfg_field buckets))"

  # (2) шарды
  step "P22 (2/3): shards (Patroni clusters; restored from their own backups)"
  shards_ready || exit 3

  # (3) карта: verify → heal → verify
  step "P22 (3/3): map reconcile/repair"
  local rc=0
  "$SCRIPT_DIR/restore-cluster.sh" --cluster "$CLUSTER_NAME" verify || rc=$?
  if [ "$rc" = 0 ]; then
    info "the map is consistent — restore finished"
  elif [ "$rc" = 1 ]; then
    local yesflag=""
    [ "$ASSUME_YES" = 1 ] && yesflag="--yes"
    "$SCRIPT_DIR/restore-cluster.sh" --cluster "$CLUSTER_NAME" heal $yesflag
    "$SCRIPT_DIR/restore-cluster.sh" --cluster "$CLUSTER_NAME" verify
    info "the map is consistent — restore finished"
  else
    err "verify exited with an error (code $rc) — not repairing the map, resolve it (see output above)"
    exit "$rc"
  fi

  cat <<EOF

  Next: the application does nothing — the routers keep a watch and pick up the
  map themselves (P9). If verify reported move situations (⚠️ MOVE_WIN / COPY_LEFT /
  STUCK_STATUS) — finish them per the verify hints (move --resume / abort-move /
  finalize). Canary:
    etcdctl get $(routing_key bucket_0)
    psql <owner shard from routing> -c 'SELECT count(*) FROM bucket_0.<table>'
EOF
}

# ── запуск ────────────────────────────────────────────────────────────────────
CMD="${1:-}"
CLUSTER=""
if [ "$CMD" = "--cluster" ]; then CLUSTER="${2:-}"; shift 2; CMD="${1:-}"; fi
[ -n "$CMD" ] && shift || usage
ASSUME_YES=0 SNAPFILE=""
while [ $# -gt 0 ]; do
  case "$1" in
    --cluster)   CLUSTER="${2:-}"; shift 2 ;;
    --yes|-y)    ASSUME_YES=1; shift ;;
    --snapshot)  SNAPFILE="${2:-}"; shift 2 ;;
    -h|--help)   usage ;;
    *)           usage ;;
  esac
done
[ -n "$CLUSTER" ] && cluster_set "$CLUSTER"
[ -n "${CLUSTER_NAME:-}" ] || { err "cluster not set: pass --cluster or set CLUSTER_NAME in buckets.env"; exit 9; }
require_bins psql jq etcdctl

case "$CMD" in
  plan) cmd_plan ;;
  run)  cmd_run ;;
  *)    usage ;;
esac
