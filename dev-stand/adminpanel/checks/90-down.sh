#!/usr/bin/env bash
# Разбор стенда; -v — стереть и данные (вкл. etcd-data; spec t10 §7.6).
set -euo pipefail
cd "$(dirname "$0")/.."
if [ "${1:-}" = "-v" ]; then
  # Профили как в 00-up (full + kafka): kafkaworker не должен переживать
  # teardown со стёртым etcd (adopt-repair: полный прогон детерминирован).
  docker compose --profile full --profile kafka down -v --remove-orphans
  echo "✓ стенд разобран (данные стёрты)"
else
  docker compose --profile full --profile kafka down --remove-orphans
  echo "✓ стенд разобран (etcd-data сохранён)"
fi
