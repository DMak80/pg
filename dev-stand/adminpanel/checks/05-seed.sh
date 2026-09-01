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

seed_pg() {
  ROOT="$(cd ../.. && pwd)"
  [ -f "$ROOT/deploy/.env" ] || cp "$ROOT/deploy/.env.example" "$ROOT/deploy/.env"
  # force-recreate: контейнер deploy-проекта может пережить пересоздание etcd
  # (другой compose-проект) с закешированным negative-DNS — свежий процесс
  # надёжнее (прецедент: 00-up.sh шаг 1b).
  ( cd "$ROOT/deploy" && docker compose --env-file .env up -d --force-recreate pgworker >/dev/null 2>&1 )
  for i in $(seq 1 60); do curl -fsS -m 3 http://localhost:8080/healthz >/dev/null 2>&1 && break; sleep 1; done
  curl -fsS -m 3 http://localhost:8080/healthz >/dev/null || { echo "❌ pgworker не ожил (:8080/healthz)"; exit 1; }
  echo "  pg-сид: $(curl -fsS -X POST http://localhost:8080/api/seed/demo)"
  # живость ключа доступа (arch/14 §1.1)
  docker compose exec -T etcd etcdctl get /pgworker/api/ --prefix --keys-only | grep -q . \
    || { echo "❌ /pgworker/api/ пуст"; exit 1; }
}
seed_kafka() {
  docker compose --profile kafka up -d kafkaworker >/dev/null 2>&1
  for i in $(seq 1 60); do curl -fsS -m 3 http://localhost:8082/healthz >/dev/null 2>&1 && break; sleep 1; done
  curl -fsS -m 3 http://localhost:8082/healthz >/dev/null || { echo "❌ kafkaworker не ожил (:8082/healthz)"; exit 1; }
  echo "  kafka-сид: $(curl -fsS -X POST http://localhost:8082/api/seed/demo)"
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
