#!/usr/bin/env bash
# Разбор стенда; -v — стереть и данные (вкл. etcd-data; spec t10 §7.6).
# Профили kafka+valkey ОБЯЗАТЕЛЬНЫ в down: воркеры (restart: unless-stopped)
# иначе переживают down с закешированным negative-DNS умершего etcd (t03:
# valkeyworker + демо-контейнер vwk-demo-node1 — контейнер воркера удаляем
# руками: он создан на docker-хосте, compose его не знает).
set -euo pipefail
cd "$(dirname "$0")/.."
# демо-контейнер сида valkey (arch/04 §2.4): создан воркером вне compose.
docker rm -f vwk-demo-node1 >/dev/null 2>&1 || true
if [ "${1:-}" = "-v" ]; then
  # Профили как в 00-up (full + kafka + valkey): воркеры не должны переживать
  # teardown со стёртым etcd (adopt-repair: полный прогон детерминирован).
  docker compose --profile full --profile kafka --profile valkey down -v --remove-orphans
  echo "✓ стенд разобран (данные стёрты)"
else
  docker compose --profile full --profile kafka --profile valkey down --remove-orphans
  echo "✓ стенд разобран (etcd-data сохранён)"
fi

# t15 (arch/18 §5.4): deploy-контур разбирается ПОСЛЕ стендового — compose down
# deploy-проекта сносит сеть pgw-metrics (её владелец), осиротевшей сети не
# остаётся. Воркеры deploy больше НЕ переживают 90-down — подъём 00-up.sh.
ROOT="$(cd ../.. && pwd)"
if [ -f "$ROOT/deploy/.env" ]; then
  (cd "$ROOT/deploy" && docker compose --env-file .env down ${1:+-v} --remove-orphans) \
    || echo "⚠ deploy down не удался (docker network ls | grep pgw-metrics)"
fi
