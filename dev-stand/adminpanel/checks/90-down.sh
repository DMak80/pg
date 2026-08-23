#!/usr/bin/env bash
# Разбор стенда; -v — стереть и данные (вкл. etcd-data; spec t10 §7.6).
set -euo pipefail
cd "$(dirname "$0")/.."
if [ "${1:-}" = "-v" ]; then
  docker compose --profile full down -v --remove-orphans
  echo "✓ стенд разобран (данные стёрты)"
else
  docker compose --profile full down --remove-orphans
  echo "✓ стенд разобран (etcd-data сохранён)"
fi
