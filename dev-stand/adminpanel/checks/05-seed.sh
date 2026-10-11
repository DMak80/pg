#!/usr/bin/env bash
# Идемпотентная наливка демо-сидов ЧЕРЕЗ API воркеров (spec §3.5; прямая
# запись etcdctl'ом упразднена). Режимы: kafka | valkey | all (default all =
# kafka+valkey; t41: pg-ветка не наливается — кластер demo создаёт 00-up.sh
# через POST /api/clusters, эндпоинт /api/seed/demo в коде — no-op на живом
# кластере).
# Скрипт НЕ управляет жизнью воркера ПОСЛЕ наливки (решение пользователя по
# ревью Фазы 4): потребитель сида решает сам — чек 50 после наливки гоняет
# мутации через живой API и останавливает kafkaworker финальным шагом
# (end-state полного прогона «после сида воркер остановлен»).
set -euo pipefail
cd "$(dirname "$0")/.."
MODE="${1:-all}"
ROOT="$(cd ../.. && pwd)"

seed_kafka() {
  docker compose --profile kafka up -d kafkaworker >/dev/null 2>&1
  # mTLS (t03): /healthz и сид — только с клиентской парой healthcheck + ca.pem
  MTLS="curl -fsS -m 3 --cacert $ROOT/deploy/tls/ca.pem --cert $ROOT/deploy/tls/healthcheck.crt --key $ROOT/deploy/tls/healthcheck.key"
  for i in $(seq 1 60); do $MTLS https://localhost:8082/healthz >/dev/null 2>&1 && break; sleep 1; done
  $MTLS https://localhost:8082/healthz >/dev/null || { echo "❌ kafkaworker не ожил (:8082/healthz по mTLS)"; exit 1; }
  echo "  kafka-сид: $($MTLS -X POST https://localhost:8082/api/seed/demo)"
  # живой ключ доступа — его ждут и панель (WorkerEndpoints), и последующие
  # мутации чека 50; воркер остаётся Поднятым (безопасно: контейнеров брокеров
  # нет → пробы слепые → сидовые заявки не исполняются, arch/16 §5 C).
  for i in $(seq 1 30); do
    docker compose exec -T etcd1 etcdctl get /kafkaworker/api/ --prefix --keys-only 2>/dev/null | grep -q . && break
    sleep 1
  done
  docker compose exec -T etcd1 etcdctl get /kafkaworker/api/ --prefix --keys-only 2>/dev/null | grep -q . \
    || { echo "❌ /kafkaworker/api/ пуст за 30 c (AdvertiseUrl/keepalive?)"; exit 1; }
}
seed_valkey() {
  docker compose --profile valkey up -d valkeyworker >/dev/null 2>&1
  # mTLS-курл внутри образа воркера (curl установлен HEALTHCHECK'ом; серты /tls);
  # API-порт на хост не публикуется — ходим изнутри контейнера на localhost:8080.
  vwk_curl() {
    docker compose exec -T valkeyworker curl -fsS -m 3 \
      --cacert /tls/ca.pem --cert /tls/healthcheck.crt --key /tls/healthcheck.key "$@"
  }
  for i in $(seq 1 60); do vwk_curl https://localhost:8080/healthz >/dev/null 2>&1 && break; sleep 1; done
  vwk_curl https://localhost:8080/healthz >/dev/null \
    || { echo "❌ valkeyworker не ожил (:8080/healthz по mTLS из контейнера)"; exit 1; }
  echo "  valkey-сид: $(vwk_curl -X POST https://localhost:8080/api/seed/demo)"
  # Заявка доигрывается Reconcile-циклом воркера: ждём Active-конфиг demo
  # (config без state) ≤ бюджета тиков (NodeBootSec-граница 120 c + запас).
  for i in $(seq 1 150); do
    cfg="$(docker compose exec -T etcd1 etcdctl get /valkey/clusters/demo/config --print-value-only </dev/null 2>/dev/null)"
    [ -n "$cfg" ] && ! echo "$cfg" | grep -q '"state"' && break
    sleep 1
  done
  cfg="$(docker compose exec -T etcd1 etcdctl get /valkey/clusters/demo/config --print-value-only </dev/null 2>/dev/null)"
  [ -n "$cfg" ] && ! echo "$cfg" | grep -q '"state"' \
    || { echo "❌ демо-кластер demo не стал Active за 150 c (docker compose logs valkeyworker; контейнер vwk-demo-node1?)"; exit 1; }
  echo "  демо-кластер demo Active (воркер доиграл заявку; vwk-demo-node1 жив)"
}
[ "$MODE" = kafka ] || [ "$MODE" = valkey ] || [ "$MODE" = all ] \
  || { echo "usage: 05-seed.sh [kafka|valkey|all]"; exit 1; }
[ "$MODE" = valkey ] || seed_kafka
[ "$MODE" = kafka ] || seed_valkey
echo "✓ 05-seed ($MODE): сиды налиты через API воркеров (воркеры подняты — жизнью управляет потребитель)"
