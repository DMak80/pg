#!/usr/bin/env bash
# Идемпотентная наливка демо-сидов ЧЕРЕЗ API воркеров (spec §3.5; прямая
# запись etcdctl'ом упразднена). Режимы: pg | kafka | all (default all).
# Скрипт НЕ управляет жизнью воркера ПОСЛЕ наливки (решение пользователя по
# ревью Фазы 4): потребитель сида решает сам — чек 50 после наливки гоняет
# мутации через живой API и останавливает kafkaworker финальным шагом
# (end-state полного прогона «после сида воркер остановлен»).
set -euo pipefail
cd "$(dirname "$0")/.."
MODE="${1:-all}"
ROOT="$(cd ../.. && pwd)"

seed_pg() {
  [ -f "$ROOT/deploy/.env" ] || cp "$ROOT/deploy/.env.example" "$ROOT/deploy/.env"
  # Хост-порт публикации pgworker: env-оверрайд → .env (пишет 00-up.sh) → 8080;
  # force-recreate интерполирует bind из .env — пересоздание обязан попасть
  # на тот же порт (коллизии хоста: 8080 может быть занят посторонним).
  PGW_API_HOST_PORT="${PGW_API_HOST_PORT:-$(awk -F= '$1=="PGW_API_HOST_PORT"{print $2}' "$ROOT/deploy/.env" 2>/dev/null)}"
  PGW_API_HOST_PORT="${PGW_API_HOST_PORT:-8080}"
  # force-recreate: контейнер deploy-проекта может пережить пересоздание etcd
  # (другой compose-проект) с закешированным negative-DNS — свежий процесс
  # надёжнее (прецедент: 00-up.sh шаг 1b).
  ( cd "$ROOT/deploy" && docker compose --env-file .env up -d --force-recreate pgworker >/dev/null 2>&1 )
  # t03: ожидание и сид — клиентским сертом seed (отдельные креды сида, spec §1.4).
  SEED_TLS="curl -fsS -m 3 --cacert $ROOT/deploy/tls/ca.pem --cert $ROOT/deploy/tls/seed.crt --key $ROOT/deploy/tls/seed.key"
  for i in $(seq 1 60); do $SEED_TLS https://localhost:${PGW_API_HOST_PORT:-8080}/healthz >/dev/null 2>&1 && break; sleep 1; done
  $SEED_TLS https://localhost:${PGW_API_HOST_PORT:-8080}/healthz >/dev/null || { echo "❌ pgworker не ожил (https :${PGW_API_HOST_PORT:-8080}/healthz, seed-серт)"; exit 1; }
  echo "  pg-сид: $($SEED_TLS -X POST https://localhost:${PGW_API_HOST_PORT:-8080}/api/seed/demo)"
  # живость ключа доступа (arch/14 §1.1)
  docker compose exec -T etcd etcdctl get /pgworker/api/ --prefix --keys-only | grep -q . \
    || { echo "❌ /pgworker/api/ пуст"; exit 1; }
}
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
    docker compose exec -T etcd etcdctl get /kafkaworker/api/ --prefix --keys-only 2>/dev/null | grep -q . && break
    sleep 1
  done
  docker compose exec -T etcd etcdctl get /kafkaworker/api/ --prefix --keys-only 2>/dev/null | grep -q . \
    || { echo "❌ /kafkaworker/api/ пуст за 30 c (AdvertiseUrl/keepalive?)"; exit 1; }
}
[ "$MODE" = pg ] || [ "$MODE" = kafka ] || [ "$MODE" = all ] || { echo "usage: 05-seed.sh [pg|kafka|all]"; exit 1; }
[ "$MODE" = kafka ] || seed_pg
[ "$MODE" = pg ] || seed_kafka
echo "✓ 05-seed ($MODE): сиды налиты через API воркеров (воркеры подняты — жизнью управляет потребитель)"
