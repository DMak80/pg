#!/usr/bin/env bash
# scripts/rebuild-node.sh
#
# Полностью пересоздать ноду «с пустого места»: остановить контейнер, очистить PGDATA,
# запустить. Patroni увидит пустой каталог → сделает pg_basebackup с лидера → нода
# станет репликой и догонит мастер.
#
# Использование:
#   ./scripts/rebuild-node.sh pg1                  # по ssh на pg1 (контейнер там)
#   ./scripts/rebuild-node.sh pg1 --local          # выполнять на текущей машине (без ssh)
#   ./scripts/rebuild-node.sh pg1 --yes            # без подтверждения
#
# Когда нужно:
#   - диск ноды был стёрт/заменён/пересоздан (пустой PGDATA);
#   - данные повреждены или нода «застряла» и не поднимается как реплика;
#   - нужно гарантированно чистое клонирование с лидера.
#
# ⚠️ ОПАСНО: удаляет все данные PostgreSQL на целевой ноде.
#    Скрипт ОТКАЖЕТСЯ работать, если:
#      - целевая нода сейчас ЛИДЕР (нельзя сносить мастера!);
#      - в кластере осталось < 2 живых нод (потеряем избыточность);
#      - путь PG_DATA_DIR не похож на каталог postgres.
#
# См. 08-operations.md → «Пересоздание ноды с пустого/повреждённого диска».

set -euo pipefail

NODE="${1:-}"
MODE="ssh"
ASSUME_YES=0
for arg in "${@:2}"; do
  case "$arg" in
    --local) MODE="local" ;;
    --yes|-y) ASSUME_YES=1 ;;
    *) echo "Неизвестный флаг: $arg" >&2; exit 2 ;;
  esac
done

if [ -z "$NODE" ]; then
  echo "Usage: $0 <node> [--local] [--yes]" >&2
  echo "  --local  выполнять на этой машине (без ssh; контейнер 'postgres' должен быть тут)" >&2
  echo "  --yes    без подтверждения" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ALL_NODES="${ALL_NODES:-pg1 pg2 pg3}"
PG_DATA_DIR="${PG_DATA_DIR:-/data/pg}"
COMPOSE_DIR="${COMPOSE_DIR:-/opt/postgres}"
PG_API_PORT="${PG_API_PORT:-8008}"

# ─────────────────────────────────────────────────────────────────────────────
# 1) Кто сейчас лидер? (нельзя пересоздавать лидера)
# ─────────────────────────────────────────────────────────────────────────────
LEADER="$("$SCRIPT_DIR/find-leader.sh" 2>/dev/null | sed -n 's/^Leader = \([^ ]*\).*/\1/p' || true)"
if [ -z "${LEADER:-}" ]; then
  echo "ОШИБКА: не удалось определить лидера кластера." >&2
  echo "  Сначала восстанови кластер (см. 09-troubleshooting.md)." >&2
  exit 3
fi
if [ "$LEADER" = "$NODE" ]; then
  echo "ОШИБКА: '$NODE' сейчас ЛИДЕР. Очистка её диска = потеря мастера." >&2
  echo "  Сначала смени лидера, например:" >&2
  echo "    ./scripts/switchover.sh <другая_нода>" >&2
  exit 3
fi
echo "✓ Текущий лидер: $LEADER (целевая '$NODE' — НЕ лидер)."

# ─────────────────────────────────────────────────────────────────────────────
# 2) Достаточно ли живых нод? (после удаления останется >= 2)
# ─────────────────────────────────────────────────────────────────────────────
alive=0
for h in $ALL_NODES; do
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "http://${h}:${PG_API_PORT}/" 2>/dev/null || echo 000)"
  [ "$code" = "200" ] && alive=$((alive+1))
done
if [ "$alive" -lt 2 ]; then
  echo "ОШИБКА: живых нод слишком мало ($alive). Пересоздание '$NODE' приведёт к потере избыточности." >&2
  exit 3
fi
echo "✓ Живых нод: $alive (избыточность сохранится после пересоздания)."

# ─────────────────────────────────────────────────────────────────────────────
# 3) Подтверждение
# ─────────────────────────────────────────────────────────────────────────────
echo
echo "ВНИМАНИЕ: будут УДАЛЕНЫ все данные PostgreSQL на '$NODE' ($PG_DATA_DIR)"
echo "          и нода пересоздана с нуля (клонирование с лидера '$LEADER')."
if [ "$ASSUME_YES" = 0 ]; then
  read -rp "Продолжить? [введите YES]: " confirm
  [ "${confirm:-}" = "YES" ] || { echo "Отменено."; exit 1; }
fi

# ─────────────────────────────────────────────────────────────────────────────
# 4) Выполнение на целевой ноде
# ─────────────────────────────────────────────────────────────────────────────
run() {
  if [ "$MODE" = "local" ]; then "$@"; else ssh "$NODE" "$@"; fi
}

echo
echo ">>> Останавливаю контейнер postgres на '$NODE' ..."
run bash -lc "cd '$COMPOSE_DIR' && docker compose down"

echo ">>> Очищаю PGDATA ($PG_DATA_DIR) ..."
# защита: путь должен содержать pg/postgres/pgdata — иначе rm -rf слишком опасен
run bash -lc '
  set -e
  case "$1" in
    *pg*|*postgres*|*pgdata*) ;;
    *) echo "ОШИБКА: PG_DATA_DIR не похож на postgres-каталог ($1)" >&2; exit 4 ;;
  esac
  # PGROOT Spilo = pgroot/pgdata/pg16 — чистим всё содержимое тома
  rm -rf "$1"/* "$1"/.[!.]* 2>/dev/null || true
  echo "  PGDATA очищен."
' _ "$PG_DATA_DIR"

echo ">>> Запускаю контейнер — Patroni сделает pg_basebackup с лидера ..."
run bash -lc "cd '$COMPOSE_DIR' && docker compose up -d"

echo
echo ">>> Логи (Ctrl+C — выйти; ждём 'replica'/'streaming'):"
run bash -lc "cd '$COMPOSE_DIR' && timeout 120 docker compose logs -f --tail=50 postgres" || true

echo
echo "Готово. Проверь состояние:"
echo "  ./scripts/patronictl.sh list"
echo "  ./scripts/get-role.sh $NODE"
