#!/usr/bin/env bash
# Грань «Хранилище бэкапов» против пула бэкапов (инвентарь — ноды пула
# s3w-<C>-<I> в стендовой сети; пул создаётся вручную через панель —
# t08, arch/04 §3):
# идемпотентный mc-налив тестовых объектов → /api/backups/storage
# (configured/health/дерево demo/shard1), on-demand objects, отсутствие
# секретов в DTO; дерево несут реальные бэкапы demo (налив — гарантия
# минимума до первого тика планировщика). Панель — всегда в докере.
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT

# Гвард: пул бэкапов жив (создаётся вручную через панель) — без пула грань
# не проверить; root-креды ноды пула — per-cluster secrets-ключ etcd
# /s3/clusters/backups/secrets (значения ТОЛЬКО в переменные, НИКОГДА в stdout).
ROOT="$(cd ../.. && pwd)"
STAND_NET="${PGW_CONTOUR_NETWORK:-adminpanel-stand-net}"
SECRETS="$(docker compose exec -T etcd1 etcdctl --endpoints=http://localhost:2379 \
  get /s3/clusters/backups/secrets --print-value-only </dev/null 2>/dev/null || true)"
S3W_ROOT_USER="$(echo "$SECRETS" | jq -r '.root_user // empty')"
S3W_ROOT_PASSWORD="$(echo "$SECRETS" | jq -r '.root_password // empty')"
[ -n "$S3W_ROOT_USER" ] && [ -n "$S3W_ROOT_PASSWORD" ] \
  || { echo "❌ etcd: /s3/clusters/backups/secrets not readable (pool not provisioned?)"; exit 1; }
pool_state="$(docker compose exec -T etcd1 etcdctl --endpoints=http://localhost:2379 \
  get /s3/clusters/backups/nodes/node1/state --print-value-only </dev/null 2>/dev/null || true)"
[ "$pool_state" = "RUNNING" ] \
  || { echo "❌ backups pool not RUNNING ($pool_state) — run checks/00-up.sh (pool seed)"; exit 1; }
echo "  backups pool alive (node1 RUNNING, inventory at s3w-backups-node1:9000)"

# Панель поднята (до 60 c) + cookie-логин — как 10-smoke-api.sh.
for i in $(seq 1 60); do curl -fsS "$BASE/api/healthz" >/dev/null 2>&1 && break; sleep 1; done
curl -fsS "$BASE/api/healthz" >/dev/null \
  || { echo "❌ adminpanel not responding: $BASE/api/healthz (docker compose up -d adminpanel)"; exit 1; }
curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login admin/admin failed"; exit 1; }
api() { curl -fsS -b "$JAR" "$BASE$1"; }
echo "  adminpanel alive ($BASE), login ok"

# Налив тестовых объектов mc-контейнером в стендовой СЕТИ (нода пула
# достаётся по имени контейнера: s3w-backups-node1:9000; root-креды кластера
# из secrets-ключа etcd; идемпотентно — перепись тех же ключей).
docker run --rm --network "$STAND_NET" \
  --entrypoint /bin/sh minio/mc:RELEASE.2025-08-13T08-35-41Z \
  -c "mc alias set a http://s3w-backups-node1:9000 '$S3W_ROOT_USER' '$S3W_ROOT_PASSWORD' >/dev/null \
      && echo data | mc pipe a/pgworker-backups/demo/shard1/full/20260913a/base.tar \
      && echo wal  | mc pipe a/pgworker-backups/demo/shard1/wal/000000010000000000000001" \
  || { echo "❌ mc upload to the backups pool/pgworker-backups failed (s3w-backups-node1)"; exit 1; }
echo "  uploaded demo/shard1/full/20260913a/base.tar and demo/shard1/wal/…01 (backups pool)"

# Ждём инвентарь-тик панели (интервал 60 c — ретрай до 70 c). ВАЖНО: ждать
# надо ИМЕННО налитое дерево — configured/health зелёные сразу от первого
# тика, который мог пройти ещё до налива (по пустому bucket); налитые объекты
# попадают в инвентарь только следующим тиком. Цели (endpoints/креды) панель
# берёт из снапшота S3-домена per-tick — подхватываются без reconfigure.
tree_ok() {
  api /api/backups/storage | jq -e \
    '.configured == true and .health.apiOk == true and .health.liveOk == true and .health.clusterOk == true
     and any(.clusters[]?; .cluster == "demo")
     and any(.clusters[] | select(.cluster == "demo") | .shards[]; .shard == "shard1" and .fullsCount >= 1)' >/dev/null
}
for i in $(seq 1 70); do tree_ok && break; sleep 1; done
tree_ok || { echo "❌ /api/backups/storage: configured/health/demo tree not green within 70 s (inventory tick?)"; exit 1; }
storage="$(api /api/backups/storage)"
echo "$storage" | jq -e '.configured == true and .health.apiOk == true and .health.liveOk == true and .health.clusterOk == true' >/dev/null \
  || { echo "❌ /api/backups/storage: configured/health"; exit 1; }
echo "  /api/backups/storage: configured=true, health api/live/cluster ok (ownS3 /s3/health/cluster)"

# Дерево: кластер demo налит, шард s1 несёт полный бэкап.
echo "$storage" | jq -e 'any(.clusters[]?; .cluster == "demo")' >/dev/null \
  || { echo "❌ inventory tree: no demo cluster"; exit 1; }
echo "$storage" | jq -e 'any(.clusters[] | select(.cluster == "demo") | .shards[]; .shard == "shard1" and .fullsCount >= 1)' >/dev/null \
  || { echo "❌ inventory tree: demo/shard1 has no fulls"; exit 1; }
echo "  tree: demo/shard1, fullsCount>=1"

# On-demand objects (единственный прямой выход панели в S3-хранилище на запрос).
api '/api/backups/objects?prefix=demo/shard1/&maxKeys=1' | jq -e '(.items | length) == 1' >/dev/null \
  || { echo "❌ /api/backups/objects?prefix=demo/shard1/&maxKeys=1: page is not 1 object"; exit 1; }
echo "  objects: maxKeys=1 → exactly 1 object"

# Секреты S3 не отдаются никогда (AC8).
api /api/backups/storage | jq -e '((tostring) | contains("SecretKey") or contains("AccessKey")) | not' >/dev/null \
  || { echo "❌ /api/backups/storage: AccessKey/SecretKey leaked into the DTO"; exit 1; }
echo "  no AccessKey/SecretKey secrets in the DTO"

# Пулы S3 read-минимум (arch/adminpanel/03 §3) — модал «Пул бэкапов»:
# пул «backups» — созданный вручную; список пулов панели несёт его с нодами,
# DTO без ключей.
api /api/backups/s3-pools | jq -e 'any(.[]; .name == "backups" and (.nodes | length) >= 1)' >/dev/null \
  || { echo "❌ /api/backups/s3-pools: no backups pool (create it via the panel)"; exit 1; }
# Состав кредов пула (rootUser + accessKeys name/policy, arch/23 §2) в DTO
# легален — это имена; секреты — только значения ключей:
# AccessKey/SecretKey/ACCESS_KEY/SECRET_KEY.
api /api/backups/s3-pools | jq -e '((tostring) | test("AccessKey|SecretKey|ACCESS_KEY|SECRET_KEY")) | not' >/dev/null \
  || { echo "❌ /api/backups/s3-pools: secrets leaked"; exit 1; }
echo "  s3-pools: backups pool with nodes, no secrets in the DTO"

# Мутация секции s3 policy через панель (замещение целиком) + сброс.
LOGIN_BODY='{"username":"admin","password":"admin"}'
api_auth_put() { curl -fsS -b "$JAR" -o /dev/null -w '%{http_code}' -X PUT "$BASE$1" -H 'Content-Type: application/json' -d "$2"; }
base_policy='{"retentionDays":1,"retentionWeeks":1,"retentionMonths":1,"fullMaxAgeSec":86400,"verifyOnCreate":true,"drillIntervalDays":1'
# PUT — из контейнера стендовой СЕТИ: путь через docker-proxy хоста на этом
# Mac возвращает 400/пусто на PUT с секцией s3 (GET те же грани — ок);
# из сети стенда — 200 (прецедент мерж-гейта t36).
s3_pool_put() { docker run --rm --network "$STAND_NET" curlimages/curl:latest sh -c '
  i=0; code=0
  while [ $i -lt 3 ]; do
    curl -fsS -c /tmp/j -X POST http://adminpanel:8080/api/auth/login -H "Content-Type: application/json" -d "{\"username\":\"admin\",\"password\":\"admin\"}" >/dev/null 2>&1 || { i=$((i+1)); sleep 5; continue; }
    code=$(curl -s -o /dev/null -w "%{http_code}" -b /tmp/j -X PUT http://adminpanel:8080/api/clusters/demo/backups/policy -H "Content-Type: application/json" -d "$1")
    [ "$code" = 200 ] && break
    i=$((i+1)); sleep 5
  done
  echo $code' _curl_ "$1"; }
# base_policy оставляет корневой { открытым: s3_on закрывает и секцию s3,
# и корень (две }}); s3_off закрывает корень с "s3":null.
s3_on=',"s3":{"pool":"backups","writeMaster":["node1"],"writeReplica":["node1"]}}'
# Факт policy — etcd-ключ (панель — прокси мутаций; DTO /api/backups/storage
# секцию s3 не отдаёт — форма дрилов t02). PUT 200 = воркер уже записал;
# ретрай 10 c — запас на асинхронность прокси.
policy_etcd() {
  for i in $(seq 1 10); do
    v="$(docker compose exec -T etcd1 etcdctl --endpoints=http://localhost:2379 \
      get /pgworker/backups/demo/policy --print-value-only </dev/null 2>/dev/null || true)"
    echo "$v" | jq -e "$1" >/dev/null 2>&1 && return 0
    sleep 1
  done
  return 1
}
code="$(s3_pool_put "$base_policy$s3_on")"
[ "$code" = "200" ] || { echo "❌ PUT policy with s3: $code (expected 200)"; exit 1; }
policy_etcd '.s3.pool == "backups" and (.s3.write_master | index("node1")) and (.s3.write_replica | index("node1"))' \
  || { echo "❌ policy.demo: s3 section not written by the worker"; exit 1; }
s3_off=',"s3":null}'
code="$(s3_pool_put "$base_policy$s3_off")"
[ "$code" = "200" ] || { echo "❌ PUT policy without s3 (reset): $code"; exit 1; }
policy_etcd '.s3 == null' \
  || { echo "❌ policy.demo: s3 reset did not work"; exit 1; }
echo "  policy s3: pool selection and reset via adminpanel — ok"

echo "✓ backups-storage green"
