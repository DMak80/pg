#!/usr/bin/env bash
# scripts/patronictl.sh
#
# Обёртка над patronictl, запускающая его внутри контейнера Spilo (postgres),
# где он уже установлен и сконфигурирован (DCS-подключение читается из env контейнера).
#
# Примеры:
#   ./scripts/patronictl.sh list
#   ./scripts/patronictl.sh list --format json
#   ./scripts/patronictl.sh topology
#   ./scripts/patronictl.sh switchover --candidate pg2
#   ./scripts/patronictl.sh reinit pg1
#   ./scripts/patronictl.sh edit-config
#   ./scripts/patronictl.sh pause
#   ./scripts/patronictl.sh resume
#
# Можно явно указать, на какой ноде выполнять (где живёт контейнер 'postgres'):
#   REMOTE_HOST=pg2 ./scripts/patronictl.sh list
#
# Подробности: 08-operations.md.

set -euo pipefail

REMOTE_HOST="${REMOTE_HOST:-}"          # пусто = локальный docker
SCOPE="${SCOPE:-pgcluster}"

# общие флаги patronictl: имя scope берётся из DCS автоматически, но можно зафиксировать
run_patronictl() {
  local host_prefix=""
  if [ -n "$REMOTE_HOST" ]; then
    host_prefix="ssh $REMOTE_HOST"
  fi

  # patronictl внутри образа Spilo лежит в PATH.
  # --format/table настройки передаём как есть.
  $host_prefix docker exec -i postgres patronictl "$@"
}

# Если вызвано без аргументов — вывести помощь
if [ $# -eq 0 ]; then
  echo "Usage: $0 <patronictl args...>" >&2
  echo "Examples:" >&2
  echo "  $0 list" >&2
  echo "  $0 switchover --candidate pg2" >&2
  echo "  $0 reinit pg1" >&2
  exit 2
fi

run_patronictl "$@"
