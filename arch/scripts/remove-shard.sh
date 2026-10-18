#!/usr/bin/env bash
# scripts/remove-shard.sh
#
# Отключить шард из кластера: удаляет его ключи из etcd-контрол-плейна.
# Инвариант (P23): удаляться могут ТОЛЬКО шарды без бакетов — если хоть один
# routing-ключ указывает на шард или есть статус переезда с target=этот шард,
# отказ (сначала мигрируй бакеты move-bucket.sh и заверши переезды).
#
# Ключ /shards/<X>/master (lease/TTL от Patroni-callback) гаснет сам по
# истечении lease; здесь удаляется, если ещё жив.
#
# Физический демонтаж кластера-шарда (контейнеры, диски) — вручную по докам;
# скрипт убирает только регистрацию в контрол-плейне.
#
# Использование:
#   ./scripts/remove-shard.sh --cluster shop shard3 [--yes]
#
# Коды выхода: 2 — использование; 3 — guard'ы; 9 — конфиг/окружение.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
. "$SCRIPT_DIR/buckets-common.sh"

usage() {
  cat >&2 <<EOF
Usage:
  $0 --cluster <C> <shard> [--yes]
EOF
  exit 2
}

confirm() {
  [ "$ASSUME_YES" = 1 ] && return 0
  local a=""
  read -rp "$1 [type YES]: " a
  [ "${a:-}" = "YES" ] || { echo "Cancelled."; exit 1; }
}

CLUSTER="" SHARD="" ASSUME_YES=0
while [ $# -gt 0 ]; do
  case "$1" in
    --cluster) CLUSTER="${2:-}"; shift 2 ;;
    --yes|-y)  ASSUME_YES=1; shift ;;
    -h|--help) usage ;;
    *)         [ -z "$SHARD" ] && SHARD="$1" || usage; shift ;;
  esac
done

require_bins jq etcdctl
[ -n "$CLUSTER" ] && [ -n "$SHARD" ] || usage
cluster_set "$CLUSTER"
[[ "$SHARD" =~ ^[a-z][a-z0-9_]*$ ]] || { err "invalid shard name '$SHARD'"; exit 2; }

step "1) Guards"
etcd_alive
[ -n "$(cluster_config)" ] \
  || { err "cluster '$CLUSTER_NAME' is not initialized: $(config_key) missing"; exit 3; }
[ -n "$(etcd_value "$(shard_key "$SHARD")/dsn")" ] \
  || { err "shard '$SHARD' is not registered in cluster '$CLUSTER_NAME'"; exit 3; }
info "shard '$SHARD' is registered"

step "2) P23 invariant: the shard must be EMPTY"
# (обход через if: "[ ... ] && echo" последней итерацией вернул бы 1 и уронил
#  пайп целиком под set -o pipefail)
owners=""
while IFS= read -r k; do
  [ -n "$k" ] || continue
  v="$(etcd_value "$k")"
  if [ "$v" = "$SHARD" ]; then owners+="${k##*/} "; fi
done <<<"$(etcd_prefix_keys "$(cluster_root)/buckets/routing/")"
if [ -n "$owners" ]; then
  err "shard '$SHARD' still has buckets: $owners"
  echo "  Migrate them first:  ./move-bucket.sh --cluster $CLUSTER_NAME move <bucket> --to <another shard>" >&2
  exit 3
fi
info "routing: no buckets on the shard"
moves=""
while IFS= read -r k; do
  [ -n "$k" ] || continue
  t="$(jstr .target "$(etcd_value "$k")")"
  if [ "$t" = "$SHARD" ]; then moves+="${k##*/} "; fi
done <<<"$(etcd_prefix_keys "$(cluster_root)/buckets/status/")"
if [ -n "$moves" ]; then
  err "unfinished moves with target='$SHARD' exist: $moves"
  echo "  Finish or abort them (move-bucket.sh / abort-move.sh) and retry." >&2
  exit 3
fi
info "status: no unfinished moves to the shard"

step "3) Removing registration"
confirm "Detach shard '$SHARD' from cluster '$CLUSTER_NAME'? (control plane only)"
ect del "$(shard_key "$SHARD")/dsn" >/dev/null
ect del "$(shard_key "$SHARD")/replicas" >/dev/null 2>&1 || true
ect del "$(shard_key "$SHARD")/master" >/dev/null 2>&1 || true
info "keys $(shard_key "$SHARD")/{dsn,replicas,master} deleted"

echo
echo "Done: shard '$SHARD' removed from the control plane of cluster '$CLUSTER_NAME'."
echo "  Physical teardown of the shard cluster (containers/disks) is manual; the Patroni"
echo "  scope of the shard under /service/<scope>/ was not touched."
