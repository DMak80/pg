#!/usr/bin/env bash
# E2E-чек мониторинга (t04, spec §6.3–6.5): /metrics трёх сервисов, все scrape-джобы
# up, rules зарегистрированы, дашборды загружены, Alertmanager жив, алерт-симуляция
# ServiceDown (down kafkaworker → up==0 ≤2мин → ServiceDown firing → восстановление).
set -euo pipefail
cd "$(dirname "$0")/.."
# Воркеры: /healthz и /metrics — открытая https-грань без клиентского серта
# (arch/18 §5.2); панель — http.
ROOT="$(cd ../.. && pwd)"
# Хост-порт публикации pgworker: env-оверрайд → .env (пишет 00-up.sh) → 8080.
PGW_API_HOST_PORT="${PGW_API_HOST_PORT:-$(awk -F= '$1=="PGW_API_HOST_PORT"{print $2}' "$ROOT/deploy/.env" 2>/dev/null)}"
PGW_API_HOST_PORT="${PGW_API_HOST_PORT:-8080}"
HEALTH="curl -fsS -m 3 --cacert $ROOT/deploy/tls/ca.pem"
PROM="http://localhost:${METRICS_PROMETHEUS_PORT:-9090}"
GRAFANA="http://localhost:${METRICS_GRAFANA_PORT:-3000}"
AM="http://localhost:${METRICS_ALERTMANAGER_PORT:-9093}"

echo ">>> check 65: monitoring (metrics profile)"
# 0) гарантия живости воркеров (ревью Ф4-3): 50-kafka-api.sh штатно останавливает
#    as-kafkaworker финальным шагом; deploy-pgworker переживает серию чеков, но
#    проверяем оба — чек обязан проходить после ЛЮБОЙ предыстории серии.
# t07: оба инстанса kafkaworker (50-й останавливает обоих; таргет kafkaworker-2
# обязан быть жив к шагу «все scrape-джобы up»)
docker start as-kafkaworker as-kafkaworker-2 >/dev/null 2>&1 || true
for i in $(seq 1 60); do $HEALTH https://localhost:8082/healthz >/dev/null 2>&1 && break; sleep 1; done
$HEALTH https://localhost:8082/healthz >/dev/null \
  || { echo "❌ kafkaworker did not come alive within 60 s on :8082 (https; docker compose logs kafkaworker)"; exit 1; }
for i in $(seq 1 60); do $HEALTH https://localhost:${PGW_API_HOST_PORT:-8080}/healthz >/dev/null 2>&1 && break; sleep 1; done
$HEALTH https://localhost:${PGW_API_HOST_PORT:-8080}/healthz >/dev/null \
  || { echo "❌ pgworker not alive on :8080 (https) — bring up the stand: checks/00-up.sh (deploy-pgworker-1, docker logs deploy-pgworker-1)"; exit 1; }
# 0.1) гарантия живости valkeyworker (t05): up -d создаёт/стартует (частичная
#      предыстория), healthz — https без серта ИЗНУТРИ контейнера.
docker compose --profile valkey up -d valkeyworker valkeyworker-2 >/dev/null 2>&1 || true
for i in $(seq 1 60); do
  docker compose exec -T valkeyworker curl -fsS -m 3 \
    --cacert /tls/ca.pem \
    https://localhost:8080/healthz >/dev/null 2>&1 && break; sleep 1
done
docker compose exec -T valkeyworker curl -fsS -m 3 \
  --cacert /tls/ca.pem \
  https://localhost:8080/healthz >/dev/null \
  || { echo "❌ valkeyworker did not come alive within 60 s (:8080/healthz https from inside; docker compose logs valkeyworker)"; exit 1; }
for i in $(seq 1 60); do
  docker compose exec -T valkeyworker-2 curl -fsS -m 3 \
    --cacert /tls/ca.pem \
    https://localhost:8080/healthz >/dev/null 2>&1 && break; sleep 1
done
docker compose exec -T valkeyworker-2 curl -fsS -m 3 \
  --cacert /tls/ca.pem \
  https://localhost:8080/healthz >/dev/null \
  || { echo "❌ valkeyworker-2 did not come alive within 60 s (:8080/healthz https from inside; docker compose logs valkeyworker-2)"; exit 1; }
echo "  workers alive (pgworker :8080, kafkaworker :8082, valkeyworker in-compose)"

# 1) /metrics трёх сервисов (хост-публикации: deploy 8080, стенд 8082/5050).
#    Подстрока вместо «echo | grep -q»: под pipefail большой экспорт панели
#    (>64КБ буфера pipe) убивает echo SIGPIPE'ом раньше выхода grep — ложный ❌.
# /metrics воркеров — открытая https-грань без клиентского серта; панель — http.
for u in "$HEALTH https://localhost:${PGW_API_HOST_PORT:-8080}/metrics" "$HEALTH https://localhost:8082/metrics" "curl -fsS -m 10 http://localhost:5050/metrics"; do
  body="$($u)" || { echo "  ❌ $u unavailable"; exit 1; }
  [[ "$body" == *"# HELP"* ]] || { echo "  ❌ $u did not return text-format"; exit 1; }
done
echo "  /metrics of the three services: 200 text-format"

# 2) все scrape-джобы up, КРОМЕ patroni-nodes: down-таргеты этой джобы —
#    легитимное состояние (rebuild ноды ~90с — arch/18 §2.5; устаревший file_sd
#    при лежачем etcd) — её проверка условная, шаг 2.1; остальные джобы
#    (включая sd-generator) — строго все up.
up_count=""
for i in $(seq 1 30); do
  up_count=$(curl -fsS "$PROM/api/v1/targets" | jq '[.data.activeTargets[] | select(.labels.job!="patroni-nodes" and .health=="up")] | length')
  total=$(curl -fsS "$PROM/api/v1/targets" | jq '[.data.activeTargets[] | select(.labels.job!="patroni-nodes")] | length')
  [ "${total:-0}" -gt 0 ] && [ "$up_count" -eq "$total" ] && break
  sleep 2
done
bad=$(curl -fsS "$PROM/api/v1/targets" | jq -r '[.data.activeTargets[] | select(.labels.job!="patroni-nodes" and .health!="up")] | .[] | .labels.job+"/"+.labels.instance' | tr '\n' ' ')
[ -z "$bad" ] || { echo "  ❌ targets not up: $bad"; exit 1; }
echo "  scrape jobs up (patroni-nodes — step 2.1)"

# 2.1) patroni-nodes (t15): файл file_sd существует/валиден; таргеты — условно:
#      ≥1 up = канал скрейпа жив; единичные down не роняют чек (rebuild/остановка).
docker exec as-prometheus sh -c 'test -s /etc/prometheus/sd/patroni-nodes.json' \
  || { echo "  ❌ /etc/prometheus/sd/patroni-nodes.json missing/empty (is as-sd-generator alive? docker logs as-sd-generator)"; exit 1; }
docker exec as-prometheus cat /etc/prometheus/sd/patroni-nodes.json | jq -e 'type=="array"' >/dev/null \
  || { echo "  ❌ file_sd patroni-nodes.json is not a JSON array"; exit 1; }
pn_total=$(curl -fsS "$PROM/api/v1/targets" | jq '[.data.activeTargets[] | select(.labels.job=="patroni-nodes")] | length')
pn_up=$(curl -fsS "$PROM/api/v1/targets" | jq '[.data.activeTargets[] | select(.labels.job=="patroni-nodes" and .health=="up")] | length')
# классификация по scrapeUrl — alias-таргеты канонических кластеров
# (<alias>:8008, контейнерный порт) vs advertised host-публикации portalloc
# (local:<порт>).
pn_net=$(curl -fsS "$PROM/api/v1/targets" | jq '[.data.activeTargets[]
  | select(.labels.job=="patroni-nodes")
  | select(.scrapeUrl | test(":8008/metrics$"))] | length')
if [ "$pn_total" -eq 0 ]; then
  echo "  patroni-nodes: no PgWorker clusters on the stand — file_sd empty (correct)"
elif [ "$pn_net" -gt 0 ]; then
  # alias-таргеты есть — строгий режим: сетевые без host-форвардинга,
  # канонический контур не ослеп (все down — фейл).
  bad_net=$(curl -fsS "$PROM/api/v1/targets" | jq '[.data.activeTargets[]
    | select(.labels.job=="patroni-nodes")
    | select(.scrapeUrl | test(":8008/metrics$"))
    | select(.scrapeUrl | contains("host.docker.internal"))] | length')
  [ "$bad_net" -eq 0 ] || { echo "  ❌ patroni-nodes: $bad_net alias targets with host forwarding"; exit 1; }
  net_up=$(curl -fsS "$PROM/api/v1/targets" | jq '[.data.activeTargets[]
    | select(.labels.job=="patroni-nodes" and .health=="up")
    | select(.scrapeUrl | test(":8008/metrics$"))] | length')
  [ "$net_up" -ge 1 ] || { echo "  ❌ patroni-nodes: all $pn_net alias targets down (canonical contour blind: is file_sd stale? are Patroni-REST nodes alive?)"; exit 1; }
  adv_down=$(( (pn_total - pn_net) - (pn_up - net_up) ))
  echo "  patroni-nodes: $net_up/$pn_net alias targets up (network :8008)${adv_down:+; advertised down: $adv_down — degradation zone (spec §6 p.5)}"
fi

# 3) серии словаря у живых таргетов (канонические имена arch/18 §2).
#    worker/pg — воркеровые серии дают живые циклы воркера; pg-грань —
#    нативные patroni_* серии скрейпа patroni-nodes (канон-минимум arch/18
#    §2.5; эмуляторная pg_replica_lag_seconds демонтирована с hc-эмуляторами).
#    kafka-серия — ТОЛЬКО при живых брокерах: сидовой кластер стенда «слепой»
#    (endpoints слушает только в e2e 55-го), сбор коллектора консервативно
#    неуспешен и kafka_collector_last_success честно НЕ обновляется
#    (алерт KafkaCollectorStalled в этот период горит — это корректно, arch/18 §4).
for s in worker_loop_ticks_total patroni_postgres_running patroni_xlog_replayed_timestamp; do
  curl -fsS --data-urlencode "query=$s" "$PROM/api/v1/query" | jq -e '.data.result | length > 0' >/dev/null \
    || { echo "  ❌ series $s not found in TSDB"; exit 1; }
done
# valkey-серия (t05): пустой домен = консервативный успех тика — серия обязана
# быть в TSDB при живом воркере (отличие от условной kafka-серии).
valkey_found=""
for i in $(seq 1 30); do
  curl -fsS --data-urlencode "query=valkey_collector_last_success_timestamp_seconds" "$PROM/api/v1/query" \
    | jq -e '.data.result | length > 0' >/dev/null 2>&1 && { valkey_found=1; break; }; sleep 2
done
[ -n "$valkey_found" ] \
  || { echo "❌ series valkey_collector_last_success_timestamp_seconds not found in TSDB (is valkeyworker alive? step 0)"; exit 1; }
# самонаблюдение генератора (t15): живой профиль metrics обязан скрейпить
# sd-generator — серия last_success в TSDB (успех консервативен: пустой префикс = успех).
sd_found=""
for i in $(seq 1 30); do
  curl -fsS --data-urlencode "query=sd_generator_last_success_timestamp_seconds" "$PROM/api/v1/query" \
    | jq -e '.data.result | length > 0' >/dev/null 2>&1 && { sd_found=1; break; }; sleep 2
done
[ -n "$sd_found" ] \
  || { echo "❌ series sd_generator_last_success_timestamp_seconds not found in TSDB (is as-sd-generator alive? docker logs as-sd-generator)"; exit 1; }
if timeout 2 bash -c '</dev/tcp/localhost/16001' 2>/dev/null; then
  curl -fsS --data-urlencode "query=kafka_collector_last_success_timestamp_seconds" "$PROM/api/v1/query" \
    | jq -e '.data.result | length > 0' >/dev/null \
    || { echo "  ❌ series kafka_collector_last_success_timestamp_seconds not found in TSDB"; exit 1; }
  echo "  arch/18 §2 dictionary series in TSDB (including kafka: brokers alive)"
else
  echo "  arch/18 §2 dictionary series in TSDB (kafka series skipped: no brokers — conservative freshness, arch/18 §4)"
fi

# 4) rules зарегистрированы (22 алерта: 12 + 7 группы backups §2.7 + 3 t15 patroni-nodes)
rules=$(curl -fsS "$PROM/api/v1/rules" | jq '[.data.groups[].rules[] | select(.type=="alerting")] | length')
[ "$rules" -ge 21 ] || { echo "  ❌ alert rules: $rules < 21"; exit 1; }
echo "  rules: $rules alerts registered (19 + 3 t15: patroni-nodes)"

# 5) Grafana: дашборды провиженены (basic admin/admin — стенд)
ds=$(curl -fsS -u admin:admin "$GRAFANA/api/search?type=dash-db" | jq 'length')
[ "$ds" -ge 5 ] || { echo "  ❌ dashboards: $ds < 5"; exit 1; }
echo "  Grafana: $ds dashboards"

# 6) Alertmanager жив
curl -fsS "$AM/api/v2/status" | jq -e '.versionInfo.version' >/dev/null || { echo "  ❌ alertmanager /api/v2/status"; exit 1; }
echo "  Alertmanager alive"

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
[ "${firing:-0}" -gt 0 ] || { echo "  ❌ ServiceDown did not go firing within ~2.5min after stopping kafkaworker"; exit 1; }
[ "${am_alert:-0}" -gt 0 ] || { echo "  ❌ ServiceDown not delivered to Alertmanager (alerting section/group)"; exit 1; }
echo "  alert simulation: ServiceDown firing + delivered to Alertmanager (kafkaworker restored)"

echo "✓ check 65: monitoring alive (prometheus/grafana/alertmanager/rules/series)"
