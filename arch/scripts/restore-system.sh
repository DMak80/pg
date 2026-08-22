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
  read -rp "$1 [введите YES]: " a
  [ "${a:-}" = "YES" ] || { echo "Отменено."; exit 1; }
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
      info "шард '$s': доступен"
    else
      err "шард '$s': НЕдоступен — восстанови ноды по 08-operations.md (rebuild-node), затем повтори run"
      rc=1
    fi
  done
  return "$rc"
}

print_order() {
  cat <<EOF
  Порядок восстановления шардированной системы (P22):
    1. etcd-слой: ноды подняты (04-deploy-etcd.md) → данные из снапшота
       (restore-cluster.sh restore <file>; снапшот покрывает ВСЕ кластеры etcd)
    2. шарды: каждый Patroni-кластер восстановлен из своих бэкапов
       (08-operations.md, rebuild-node; шардами управляет Patroni, не мы)
    3. карта: verify → heal → verify (restore-cluster.sh; сверка routing ×
       фактические схемы bucket_* на шардах, однозначные расхождения чинятся
       автоматически с журналом /clusters/<C>/heals/*)
    4. приложение: ничего не делать — роутеры держат watch etcd и подхватят
       карту сами (P9); при сомнениях — канарейка на одном бакете
EOF
}

# ── plan: порядок + состояние слоёв (read-only) ───────────────────────────────
cmd_plan() {
  step "P22: порядок восстановления системы '$CLUSTER_NAME'"
  print_order
  echo
  step "состояние слоёв сейчас"
  if etcd_up; then
    info "etcd: доступен ($ETCD_ENDPOINTS)"
    if [ -n "$(cluster_config)" ]; then
      info "кластер '$CLUSTER_NAME': инициализирован (N=$(cfg_field buckets), шарды: $(cluster_shards | paste -sd, -))"
      shards_ready || true
      echo "  карта: сверь отдельно — $SCRIPT_DIR/restore-cluster.sh --cluster $CLUSTER_NAME verify"
    else
      err "кластер '$CLUSTER_NAME' в etcd НЕ инициализирован (нет $(config_key)) — шаг 1: restore из снапшота"
    fi
  else
    err "etcd НЕдоступен ($ETCD_ENDPOINTS) — шаг 1: поднять слой (04-deploy-etcd.md) + restore из снапшота"
  fi
}

# ── run: исполнение по порядку ────────────────────────────────────────────────
cmd_run() {
  # (1) etcd-контрол-плейн
  step "P22 (1/3): etcd-контрол-плейн"
  if etcd_up; then
    info "etcd: доступен ($ETCD_ENDPOINTS) — restore не требуется"
  else
    [ -n "$SNAPFILE" ] || {
      err "etcd недоступен: подними слой (04-deploy-etcd.md) и передай --snapshot <file> (последние: ls ${SNAPSHOT_DIR:-/var/lib/etcd-snapshots})"
      exit 3
    }
    confirm "Восстановить etcd из $(basename "$SNAPFILE")? (снапшот вернёт ВСЕ кластеры этого etcd к моменту снятия)"
    local yesflag=""
    [ "$ASSUME_YES" = 1 ] && yesflag="--yes"
    "$SCRIPT_DIR/restore-cluster.sh" restore "$SNAPFILE" $yesflag
    etcd_up || { err "etcd всё ещё недоступен после restore"; exit 3; }
    info "etcd: восстановлен из снапшота"
  fi
  [ -n "$(cluster_config)" ] \
    || { err "кластер '$CLUSTER_NAME' НЕ инициализирован в etcd после restore (не тот снапшот? см. plan)"; exit 3; }
  info "кластер '$CLUSTER_NAME': инициализирован (N=$(cfg_field buckets))"

  # (2) шарды
  step "P22 (2/3): шарды (Patroni-кластеры; восстановлены из своих бэкапов)"
  shards_ready || exit 3

  # (3) карта: verify → heal → verify
  step "P22 (3/3): сверка/починка карты"
  local rc=0
  "$SCRIPT_DIR/restore-cluster.sh" --cluster "$CLUSTER_NAME" verify || rc=$?
  if [ "$rc" = 0 ]; then
    info "карта согласована — восстановление завершено"
  elif [ "$rc" = 1 ]; then
    local yesflag=""
    [ "$ASSUME_YES" = 1 ] && yesflag="--yes"
    "$SCRIPT_DIR/restore-cluster.sh" --cluster "$CLUSTER_NAME" heal $yesflag
    "$SCRIPT_DIR/restore-cluster.sh" --cluster "$CLUSTER_NAME" verify
    info "карта согласована — восстановление завершено"
  else
    err "verify завершился с ошибкой (код $rc) — карту не чиним, разберись (см. вывод выше)"
    exit "$rc"
  fi

  cat <<EOF

  Дальше: приложению ничего не делать — роутеры держат watch и подхватят карту
  сами (P9). Если verify показывал ситуации переездов (⚠️ MOVE_WIN / COPY_LEFT /
  STUCK_STATUS) — доведи по подсказкам verify (move --resume / abort-move /
  finalize). Канарейка:
    etcdctl get $(routing_key bucket_0)
    psql <шард-владелец из routing> -c 'SELECT count(*) FROM bucket_0.<таблица>'
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
[ -n "${CLUSTER_NAME:-}" ] || { err "кластер не задан: передай --cluster или CLUSTER_NAME в buckets.env"; exit 9; }
require_bins psql jq etcdctl

case "$CMD" in
  plan) cmd_plan ;;
  run)  cmd_run ;;
  *)    usage ;;
esac
