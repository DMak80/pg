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
  # Профили как в 00-up (full + kafka + valkey + metrics): воркеры не должны
  # переживать teardown со стёртым etcd (adopt-repair: полный прогон
  # детерминирован). metrics обязателен: живой as-prometheus держит
  # external-сеть pgw-metrics — deploy-down не снесёт её (t15, arch/18 §5.4).
  docker compose --profile full --profile kafka --profile valkey --profile metrics down -v --remove-orphans
  echo "✓ stand taken down (data erased)"
else
  docker compose --profile full --profile kafka --profile valkey --profile metrics down --remove-orphans
  echo "✓ stand taken down (etcd-data kept)"
fi

# t15/t41: deploy-контур разбирается ПОСЛЕ стендового — теми же файлами, что
# поднимался (контур-override), compose down deploy-проекта сносит свои сети;
# стендовую сеть adminpanel-stand-net сносит стендовый down выше (владелец).
# Воркеры deploy больше НЕ переживают 90-down — подъём 00-up.sh.
ROOT="$(cd ../.. && pwd)"
if [ -f "$ROOT/deploy/.env" ]; then
  (cd "$ROOT/deploy" && docker compose -f docker-compose.yml -f docker-compose.contour.yml \
    --env-file .env down ${1:+-v} --remove-orphans) \
    || echo "⚠ deploy down failed (docker network ls | grep -E 'pgw-metrics|adminpanel-stand-net')"
fi

# Канонические контейнеры движка (t41): pgw-demo-* ноды, wal-агенты, джобы —
# созданы Engine API (не compose), полный down их не трогает. Снос явно +
# страховочный prune сетей (README-cleanup).
docker ps -aq --filter name='^pgw-' --format '{{.ID}}' | xargs -r docker rm -f
docker volume ls -q --filter name='^pgw-' --format '{{.Name}}' | xargs -r docker volume rm -f
docker network ls --filter name='^pgw-net-' --format '{{.Name}}' | xargs -r docker network rm 2>/dev/null || true
# s3w-* — ownS3-ноды пулов, созданные S3Worker'ом (Engine API, не compose;
# t36): полный down их не трогает — снос own-only по префиксу (контейнеры +
# их тома данных).
docker ps -aq --filter name='^s3w-' --format '{{.ID}}' | xargs -r docker rm -f
docker volume ls -q --filter name='^s3w-' --format '{{.Name}}' | xargs -r docker volume rm -f
docker network prune -f
