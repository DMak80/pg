#!/usr/bin/env bash
# Разбор стенда; -v — стереть и данные (вкл. etcd-data; spec t10 §7.6).
# Профиль kafka ОБЯЗАТЕЛЕН: kafkaworker (restart: unless-stopped) иначе
# переживает down с закешированным negative-DNS умершего etcd — свежий up
# не пересоздаёт контейнер, и keepalive не может резолвить «etcd».
set -euo pipefail
cd "$(dirname "$0")/.."
if [ "${1:-}" = "-v" ]; then
  docker compose --profile full --profile kafka down -v --remove-orphans
  echo "✓ стенд разобран (данные стёрты)"
else
  docker compose --profile full --profile kafka down --remove-orphans
  echo "✓ стенд разобран (etcd-data сохранён)"
fi
