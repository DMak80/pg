#!/usr/bin/env bash
# E2E-чек мониторинга (t04, spec §6.3–6.5): /metrics трёх сервисов, все scrape-джобы
# up, rules зарегистрированы, дашборды загружены, Alertmanager жив, алерт-симуляция
# ServiceDown (down kafkaworker → up==0 ≤2мин → ServiceDown firing → восстановление).
set -euo pipefail
cd "$(dirname "$0")/.."
PROM="http://localhost:${METRICS_PROMETHEUS_PORT:-9090}"
GRAFANA="http://localhost:${METRICS_GRAFANA_PORT:-3000}"
AM="http://localhost:${METRICS_ALERTMANAGER_PORT:-9093}"

echo ">>> чек 65: мониторинг (профиль metrics)"
# 0) гарантия живости воркеров (ревью Ф4-3): 50-kafka-api.sh штатно останавливает
#    as-kafkaworker финальным шагом; deploy-pgworker переживает серию чеков, но
#    проверяем оба — чек обязан проходить после ЛЮБОЙ предыстории серии.
docker start as-kafkaworker >/dev/null 2>&1 || true
for i in $(seq 1 60); do curl -fsS -m 3 http://localhost:8082/healthz >/dev/null 2>&1 && break; sleep 1; done
curl -fsS -m 3 http://localhost:8082/healthz >/dev/null \
  || { echo "❌ kafkaworker не ожил за 60 c на :8082 (docker compose logs kafkaworker)"; exit 1; }
for i in $(seq 1 60); do curl -fsS -m 3 http://localhost:8080/healthz >/dev/null 2>&1 && break; sleep 1; done
curl -fsS -m 3 http://localhost:8080/healthz >/dev/null \
  || { echo "❌ pgworker не жив на :8080 — поднимите стенд: checks/00-up.sh (deploy-pgworker-1, docker logs deploy-pgworker-1)"; exit 1; }
echo "  воркеры живы (pgworker :8080, kafkaworker :8082)"

# 1) /metrics трёх сервисов (хост-публикации: deploy 8080, стенд 8082/5050).
#    Подстрока вместо «echo | grep -q»: под pipefail большой экспорт панели
#    (>64КБ буфера pipe) убивает echo SIGPIPE'ом раньше выхода grep — ложный ❌.
for u in http://localhost:8080/metrics http://localhost:8082/metrics http://localhost:5050/metrics; do
  body="$(curl -fsS -m 10 "$u")" || { echo "  ❌ $u недоступен"; exit 1; }
  [[ "$body" == *"# HELP"* ]] || { echo "  ❌ $u не отдал text-format"; exit 1; }
done
echo "  /metrics трёх сервисов: 200 text-format"

# 2) все scrape-джобы up (ждать прогрева: 15с scrape + оценка rules)
up_count=""
for i in $(seq 1 30); do
  up_count=$(curl -fsS "$PROM/api/v1/targets" | jq '[.data.activeTargets[] | select(.health=="up")] | length')
  total=$(curl -fsS "$PROM/api/v1/targets" | jq '.data.activeTargets | length')
  [ "${total:-0}" -gt 0 ] && [ "$up_count" -eq "$total" ] && break
  sleep 2
done
bad=$(curl -fsS "$PROM/api/v1/targets" | jq -r '[.data.activeTargets[] | select(.health!="up")] | .[] | .labels.job+"/"+.labels.instance' | tr '\n' ' ')
[ -z "$bad" ] || { echo "  ❌ таргеты не up: $bad"; exit 1; }
patroni_up=$(curl -fsS "$PROM/api/v1/targets" | jq '[.data.activeTargets[] | select(.labels.job=="patroni" and .health=="up")] | length')
[ "$patroni_up" -ge 2 ] || { echo "  ❌ patroni-эмуляторы: up только $patroni_up (<2)"; exit 1; }
echo "  scrape-джобы up (включая patroni: $patroni_up)"

# 3) серии словаря у живых таргетов (канонические имена arch/18 §2).
#    worker/pg — гарантированы живыми циклами воркера и эмуляторами.
#    kafka-серия — ТОЛЬКО при живых брокерах: сидовой кластер стенда «слепой»
#    (endpoints слушает только в e2e 55-го), сбор коллектора консервативно
#    неуспешен и kafka_collector_last_success честно НЕ обновляется
#    (алерт KafkaCollectorStalled в этот период горит — это корректно, arch/18 §4).
for s in worker_loop_ticks_total pg_replica_lag_seconds; do
  curl -fsS --data-urlencode "query=$s" "$PROM/api/v1/query" | jq -e '.data.result | length > 0' >/dev/null \
    || { echo "  ❌ серия $s не найдена в TSDB"; exit 1; }
done
if timeout 2 bash -c '</dev/tcp/localhost/16001' 2>/dev/null; then
  curl -fsS --data-urlencode "query=kafka_collector_last_success_timestamp_seconds" "$PROM/api/v1/query" \
    | jq -e '.data.result | length > 0' >/dev/null \
    || { echo "  ❌ серия kafka_collector_last_success_timestamp_seconds не найдена в TSDB"; exit 1; }
  echo "  серии словаря arch/18 §2 в TSDB (включая kafka: брокеры живы)"
else
  echo "  серии словаря arch/18 §2 в TSDB (kafka-серия пропущена: брокеров нет — консервативная свежесть, arch/18 §4)"
fi

# 4) rules зарегистрированы (8 алертов §3.7)
rules=$(curl -fsS "$PROM/api/v1/rules" | jq '[.data.groups[].rules[] | select(.type=="alerting")] | length')
[ "$rules" -ge 8 ] || { echo "  ❌ алерт-рулы: $rules < 8"; exit 1; }
echo "  rules: $rules алертов зарегистрировано"

# 5) Grafana: дашборды провиженены (basic admin/admin — стенд)
ds=$(curl -fsS -u admin:admin "$GRAFANA/api/search?type=dash-db" | jq 'length')
[ "$ds" -ge 3 ] || { echo "  ❌ дашборды: $ds < 3"; exit 1; }
echo "  Grafana: $ds дашборда"

# 6) Alertmanager жив
curl -fsS "$AM/api/v2/status" | jq -e '.versionInfo.version' >/dev/null || { echo "  ❌ alertmanager /api/v2/status"; exit 1; }
echo "  Alertmanager жив"

# 7) алерт-симуляция (spec §6.5): stop kafkaworker → up==0 + for:2m → ServiceDown
#    firing → доставлен в Alertmanager → восстановление. Бюджет: scrape 15с +
#    for 2м + group_wait 30с — ранний выход циклами.
docker stop as-kafkaworker >/dev/null
firing=""
am_alert=""
for i in $(seq 1 30); do
  firing=$(curl -fsS "$PROM/api/v1/alerts" | jq -r '[.data.alerts[] | select(.labels.alertname=="ServiceDown" and .state=="firing")] | length')
  # Доставка (ревью Ф7-2): alerting-секция Prometheus → Alertmanager — алерт
  # обязан появиться и в его API, а не только в Prometheus.
  am_alert=$(curl -fsS -m 5 "$AM/api/v2/alerts" | jq -r '[.[] | select(.labels.alertname=="ServiceDown")] | length' 2>/dev/null || echo 0)
  [ "${firing:-0}" -gt 0 ] && [ "${am_alert:-0}" -gt 0 ] && break
  sleep 10
done
docker start as-kafkaworker >/dev/null
[ "${firing:-0}" -gt 0 ] || { echo "  ❌ ServiceDown не перешёл в firing ≤~2.5мин после остановки kafkaworker"; exit 1; }
[ "${am_alert:-0}" -gt 0 ] || { echo "  ❌ ServiceDown не доставлен в Alertmanager (alerting-секция/группа)"; exit 1; }
echo "  алерт-симуляция: ServiceDown firing + доставлен в Alertmanager (kafkaworker восстановлен)"

echo "✓ чек 65: мониторинг жив (prometheus/grafana/alertmanager/rules/серии)"
