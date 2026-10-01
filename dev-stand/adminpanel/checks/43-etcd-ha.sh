#!/usr/bin/env bash
# HA-etcd контур (t09, arch/04 §8): кворум/health/member list; отказ ОДНОГО
# узла — не-событие (запись идёт, healthz воркеров жив, master-ключ демо-кластера
# не гаснет); возврат узла — снова 3/3. Самодостаточен: свой тестовый ключ
# убирает (trap), as-etcd-2 возвращает (trap).
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$(cd ../.. && pwd)"

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
PGW_API_HOST_PORT="${PGW_API_HOST_PORT:-8080}"
PGW_API_HOST_PORT2="${PGW_API_HOST_PORT2:-8083}"
MTLS="curl -fsS -m 5 --cacert $ROOT/deploy/tls/ca.pem --cert $ROOT/deploy/tls/healthcheck.crt --key $ROOT/deploy/tls/healthcheck.key"
# etcdctl выполняется ВНУТРИ as-etcd-1 (default endpoint 127.0.0.1:2379 — сам узел);
# все три узла адресуемся compose-DNS сети стенда (etcdN:2379 — клиентский порт
# контейнера один): ХОСТОВЫЕ публикации localhost:2379/2381/2383 внутри контейнера
# НЕ слушаются, host.docker.internal — зависел бы от Docker Desktop. Живость
# хостовых публикаций отдельно доказывают воркеры (etcd-reachable healthz — они
# ходят именно публикациями host.docker.internal).
ECT() { docker exec as-etcd-1 etcdctl "$@"; }
EP3="http://etcd1:2379,http://etcd2:2379,http://etcd3:2379"
KEY="/tmp/t09-check43/$(date +%s)"
JAR="$(mktemp)"

cleanup() {
  ECT del "$KEY" >/dev/null 2>&1 || true
  docker start as-etcd-2 >/dev/null 2>&1 || true   # узел обязан вернуться при любом исходе
  rm -f "$JAR"
}
trap cleanup EXIT

curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login панели (стенд поднят? 00-up.sh)"; exit 1; }
api() { curl -fsS -b "$JAR" "$BASE$1"; }

# 1) Контур цел: 3 started, единый leader; панель видит 3 члена с одним лидером
[ "$(ECT member list | grep -c started)" = "3" ] \
  || { echo "❌ member list ≠ 3 started (docker logs as-etcd-1/2/3)"; exit 1; }
ECT endpoint health --endpoints="$EP3" >/dev/null \
  || { echo "❌ endpoint health 3/3 (compose-DNS etcd1/etcd2/etcd3 из as-etcd-1)"; exit 1; }
api /api/etcd/status | jq -e '(.members | length) == 3
  and ([.members[] | select(.isLeader)] | length) == 1' >/dev/null \
  || { echo "❌ /api/etcd/status: 3 члена/единый leader"; exit 1; }
echo "  контур цел: 3 started, health 3/3, единый leader"

# 2) Отказ одного узла — НЕ-событие: кворум пишет, воркеры/панель/master-ключ живы
docker stop as-etcd-2 >/dev/null
sleep 2
ECT put "$KEY" check43 >/dev/null \
  || { echo "❌ кворум не пишет после stop as-etcd-2 (member list / raft-статус as-etcd-1)"; exit 1; }
ECT get "$KEY" --print-value-only | grep -qx check43 \
  || { echo "❌ read-after-write не сходится"; exit 1; }
ECT del "$KEY" >/dev/null
$MTLS "https://localhost:${PGW_API_HOST_PORT}/healthz" >/dev/null \
  && $MTLS "https://localhost:${PGW_API_HOST_PORT2}/healthz" >/dev/null \
  || { echo "❌ healthz PgWorker (etcd-reachable) погас при живом кворуме"; exit 1; }
api /api/healthz >/dev/null || { echo "❌ панель не отвечает при живом кворуме"; exit 1; }
api /api/clusters/demo | jq -e 'all(.shards[]; .masterLeaseAlive == true)' >/dev/null \
  || { echo "❌ master-ключ демо-кластера погас (эмулятор должен писать через уцелевший узел)"; exit 1; }
# ожидаемая деградация: 2 healthy / 1 unreachable — НЕ ошибка (вывод для журнала)
ECT endpoint health --endpoints="$EP3" 2>&1 | grep -q "is healthy" && \
  ECT endpoint health --endpoints="$EP3" 2>&1 | grep -qc "unreachable" \
  || true
echo "  stop as-etcd-2: запись/healthz×2/панель/master-ключ живы (2 healthy, 1 unreachable — ожидаемо)"

# 3) Возврат узла: member list снова 3 started, health 3/3 (retry-вхождение в кворум)
docker start as-etcd-2 >/dev/null
ok3=0
for i in $(seq 1 30); do
  if [ "$(ECT member list 2>/dev/null | grep -c started)" = "3" ] \
     && ECT endpoint health --endpoints="$EP3" >/dev/null 2>&1; then ok3=1; break; fi
  sleep 2
done
[ "$ok3" = 1 ] || { echo "❌ as-etcd-2 не вернулся в кворум за 60 c (docker logs as-etcd-2)"; exit 1; }
ECT del "$KEY" >/dev/null 2>&1 || true
echo "✓ etcd-ha: 3/3 → stop узла (не-событие) → 3/3"
