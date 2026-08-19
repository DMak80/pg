#!/usr/bin/env bash
# scripts/switchover.sh
#
# Плановая смена лидера (switchover) через patronictl.
# Без даунтайма: Patroni штатно передаёт роль, HAProxy автоматически переключит трафик.
#
# Запуск:
#   ./scripts/switchover.sh                    # интерактивно (patronictl спросит кандидата)
#   ./scripts/switchover.sh pg2                # сделать pg2 новым лидером
#   ./scripts/switchover.sh pg2 --master pg1   # явно указать текущего мастера
#
# Подробности: 08-operations.md (раздел 1)

set -euo pipefail

CANDIDATE="${1:-}"
shift || true

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PATRONICTL="$SCRIPT_DIR/patronictl.sh"

if [ -n "$CANDIDATE" ]; then
  # определяется текущий мастер автоматически, явно задаём только кандидата
  exec "$PATRONICTL" switchover --candidate "$CANDIDATE" "$@"
else
  # интерактивный режим — patronictl сам спросит
  exec "$PATRONICTL" switchover "$@"
fi
