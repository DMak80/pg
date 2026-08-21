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
  read -rp "$1 [введите YES]: " a
  [ "${a:-}" = "YES" ] || { echo "Отменено."; exit 1; }
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
[[ "$SHARD" =~ ^[a-z][a-z0-9_]*$ ]] || { err "неверное имя шарда '$SHARD'"; exit 2; }

step "1) Guards"
etcd_alive
[ -n "$(cluster_config)" ] \
  || { err "кластер '$CLUSTER_NAME' не инициализирован: нет $(config_key)"; exit 3; }
[ -n "$(etcd_value "$(shard_key "$SHARD")/dsn")" ] \
  || { err "шард '$SHARD' не зарегистрирован в кластере '$CLUSTER_NAME'"; exit 3; }
info "шард '$SHARD' зарегистрирован"

step "2) Инвариант P23: шард обязан быть ПУСТЫМ"
# (обход через if: "[ ... ] && echo" последней итерацией вернул бы 1 и уронил
#  пайп целиком под set -o pipefail)
owners=""
while IFS= read -r k; do
  [ -n "$k" ] || continue
  v="$(etcd_value "$k")"
  if [ "$v" = "$SHARD" ]; then owners+="${k##*/} "; fi
done <<<"$(etcd_prefix_keys "$(cluster_root)/buckets/routing/")"
if [ -n "$owners" ]; then
  err "на шарде '$SHARD' есть бакеты: $owners"
  echo "  Сначала мигрируй их:  ./move-bucket.sh --cluster $CLUSTER_NAME move <бакет> --to <другой шард>" >&2
  exit 3
fi
info "routing: бакетов на шарде нет"
moves=""
while IFS= read -r k; do
  [ -n "$k" ] || continue
  t="$(jstr .target "$(etcd_value "$k")")"
  if [ "$t" = "$SHARD" ]; then moves+="${k##*/} "; fi
done <<<"$(etcd_prefix_keys "$(cluster_root)/buckets/status/")"
if [ -n "$moves" ]; then
  err "есть незавершённые переезды с target='$SHARD': $moves"
  echo "  Заверши или отмени их (move-bucket.sh / abort-move.sh) и повтори." >&2
  exit 3
fi
info "status: незавершённых переездов на шард нет"

step "3) Удаление регистрации"
confirm "Отключить шард '$SHARD' от кластера '$CLUSTER_NAME'? (контрол-плейн only)"
ect del "$(shard_key "$SHARD")/dsn" >/dev/null
ect del "$(shard_key "$SHARD")/replicas" >/dev/null 2>&1 || true
ect del "$(shard_key "$SHARD")/master" >/dev/null 2>&1 || true
info "ключи $(shard_key "$SHARD")/{dsn,replicas,master} удалены"

echo
echo "Готово: шард '$SHARD' исключён из контрол-плейна кластера '$CLUSTER_NAME'."
echo "  Физический демонтаж кластера (контейнеры/диски) — вручную; Patroni-scope"
echo "  шарда в /service/<scope>/ затронут не был."
