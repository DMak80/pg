#!/usr/bin/env bash
# 44-etcdworker.sh: read-only чек etcdworker-грани панели на стенде
# (arch/adminpanel/04 §3): 1) /api/workers — ряд etcdworker ≥1 инстанса,
# health инстансов healthy (стенд поднимает 2); 2) /api/etcd/domain — 200 и
# стендовый инвариант внешнего контура as-etcd: declaredNodes null («только
# наблюдение»), managed-узлов нет, snapshots отсутствует или OK (отсутствие —
# не fail, значение выводим); 3) /api/etcd/status — members ≥ 3, все external
# (peer-URL без etcdw-node-паттерна). Без мутаций; чистка — только сессионный
# JAR (trap).
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
JAR="$(mktemp)"
cleanup() { rm -f "$JAR"; }
trap cleanup EXIT

curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ adminpanel login (is the stand up? 00-up.sh)"; exit 1; }
api() { curl -fsS -b "$JAR" "$BASE$1"; }

# 1) Воркеры: ряд etcdworker с ≥1 инстансом, health каждого — healthy.
row="$(api /api/workers | jq -e '.workers[] | select(.worker == "etcdworker")')" \
  || { echo "❌ /api/workers: no etcdworker row (discovery keys /etcdworker/api/)"; exit 1; }
echo "$row" | jq -e '(.instances | length) >= 1' >/dev/null \
  || { echo "❌ etcdworker row: no instances"; exit 1; }
echo "$row" | jq -e 'all(.instances[]; .health == "healthy")' >/dev/null \
  || { echo "❌ etcdworker instances not healthy (docker logs deploy-etcdworker-1/-2)"; exit 1; }
echo "  etcdworker: $(echo "$row" | jq '.instances | length') instance(s), all healthy"

# 2) Домен: внешний контур — «только наблюдение»; слепки отсутствуют или OK.
dom="$(api /api/etcd/domain)"
echo "$dom" | jq -e '.declaredNodes == null and (.nodes | length) == 0' >/dev/null \
  || { echo "❌ /api/etcd/domain: stand invariant failed (declaredNodes/nodes must be null/empty)"; exit 1; }
snap="$(echo "$dom" | jq -r '.snapshots.state // "absent"')"
[ "$snap" = "absent" ] || [ "$snap" = "OK" ] \
  || { echo "❌ /api/etcd/domain: snapshots.state=$snap (expected absent or OK)"; exit 1; }
echo "  etcd domain: observation only (declaredNodes null, no managed nodes), snapshots: $snap"

# 3) Контур: members ≥ 3, все external (никаких etcdw-node-<k> peer-URL).
api /api/etcd/status | jq -e '(.members | length) >= 3
  and all(.members[]; all(.peerUrls[]?; (startswith("http://etcdw-node-") | not)))' >/dev/null \
  || { echo "❌ /api/etcd/status: members < 3 or managed-looking member present"; exit 1; }
echo "✓ etcdworker: workers row healthy, domain = observation only, contour members external"
