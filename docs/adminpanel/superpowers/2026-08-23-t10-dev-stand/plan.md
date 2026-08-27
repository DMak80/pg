# t10-dev-stand — план реализации

> **Для агентных исполнителей:** ОБЯЗАТЕЛЬНЫЙ САБ-СКИЛЛ: superpowers:subagent-driven-development (рекомендуется) или superpowers:executing-plans — выполнять по задаче за раз. Шаги отмечаются чекбоксами (`- [ ]`).

**Цель:** self-contained docker dev-стенд AdminPanel (`dev-stand/`: compose quick/full, идемпотентный сид, patroni-эмуляторы) + e2e-скрипты проверок, полный прогон `00→10→20→30→40` зелёный против работающей панели.

**Архитектура:** compose-проект `adminpanel-stand` (имена сервисов канонические `s1a`/`etcd`/…, `container_name` с префиксом `as-`); etcd один на оба профиля, сид — одноразовый контейнер (alpine + etcdctl 3.5.21, скопированный из официального distroless-образа); patroni-эмуляторы `hc*` — python+pg8000, шейвят master/leader/members-ключи с lease TTL 5 c; панель работает на хосте (`dotnet run`, :5000), адреса проб мапит `HostMap`.

**Стек:** docker compose v2, `quay.io/coreos/etcd:v3.5.21`, `postgres:18`, `python:3.12-alpine` + `pg8000`, `alpine:3.20`, bash+jq+curl.

**Спецификация:** `docs/superpowers/2026-08-23-t10-dev-stand/spec.md` (далее «spec»); канон стенда — `arch/04-local-stand.md`. План аргументируется от spec — исполнитель читает оба.

## Глобальные ограничения

- WORKTREE: `/Users/demakaev/ZCodeProject/worktrees/feat-t10-dev-stand` — вся работа здесь.
- Один Task = один коммит с сообщением вида `t10: <суть>`.
- Код панели (C#) НЕ меняется; единственная правка кода — `src/AdminPanel.Api/appsettings.Development.json` (HostMap).
- `appsettings.json` не трогать (прод без HostMap).
- Идентификаторы — английские, комментарии/сообщения — русские; скрипты: `#!/usr/bin/env bash`, `set -euo pipefail`, `cd "$(dirname "$0")/.."`, стиль `../pg/arch/stand/checks/`.
- Образы фиксировать тегами: `postgres:18`, `quay.io/coreos/etcd:v3.5.21`, `python:3.12-alpine`, `alpine:3.20`.
- Порты host: 2379, 5433–5436, 8011/8012/8021/8022; панель :5000 (`ADMINPANEL_URL` переопределяет).
- Директория `$WT` ниже = путь worktree.

## Карта файлов

| Файл | Задача | Ответственность |
|---|---|---|
| `dev-stand/docker-compose.yml` | 2 (etcd+seed) → 3 (+PG) → 4 (+hc*) | топология стенда |
| `dev-stand/seed/Dockerfile` | 2 | образ сида (alpine + etcdctl 3.5.21) |
| `dev-stand/seed.sh` | 2 | идемпотентный сид контроль-плейна (spec §4) |
| `dev-stand/checks/90-down.sh` | 2 | разбор стенда |
| `dev-stand/checks/10-smoke-api.sh` | 2 | дым API против панели (spec §7.2) |
| `dev-stand/sidecar/Dockerfile`, `dev-stand/sidecar/emulator.py` | 4 | patroni-эмулятор (spec §5) |
| `dev-stand/checks/00-up.sh` | 4 | подъём full + sync + инвентарь (spec §7.1) |
| `src/AdminPanel.Api/appsettings.Development.json` | 5 | HostMap стенда (spec §8) |
| `dev-stand/checks/20-alerts.sh` | 6 | алерты появления/гашения (spec §7.3) |
| `dev-stand/checks/30-failover.sh` | 6 | failover-цикл + rejoin (spec §7.4) |
| `dev-stand/checks/40-live-probes.sh` | 6 | live-пробы (spec §7.5) |
| `dev-stand/README.md` | 7 | быстрый старт + e2e |
| `arch/roadmap/stand.md`, `arch/roadmap/README.md`, `arch/roadmap/infra.md` | 8 | roadmap-деливерабл |

---

### Task 1: коммит документации (spec, план, arch-правки)

**Files:** уже существуют — `docs/superpowers/2026-08-23-t10-dev-stand/spec.md`, `docs/superpowers/2026-08-23-t10-dev-stand/plan.md`, `arch/04-local-stand.md` (правки фаз spec/plan).

- [ ] **Шаг 1: убедиться в составе изменений**

Run: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t10-dev-stand && git status --short`
Ожидание: `M arch/04-local-stand.md`, `?? docs/superpowers/2026-08-23-t10-dev-stand/` (spec.md, plan.md). Других изменений нет.

- [ ] **Шаг 2: коммит**

```bash
git add arch/04-local-stand.md docs/superpowers/2026-08-23-t10-dev-stand/
git commit -m "t10: docs+arch — spec, план, уточнения 04-local-stand (routing 10/6, lease-механика эмуляторов, инвентарь ACTIVE)"
```

---

### Task 2: каркас quick-профиля — compose (etcd+seed), seed.sh, чеки 90-down/10-smoke-api

**Files:**
- Create: `dev-stand/docker-compose.yml` (пока только etcd+seed)
- Create: `dev-stand/seed/Dockerfile`, `dev-stand/seed.sh`
- Create: `dev-stand/checks/90-down.sh`, `dev-stand/checks/10-smoke-api.sh`

**Interfaces:**
- Produces (для последующих задач): compose-проект `adminpanel-stand`; сервисы `etcd` (контейнер `as-etcd`, publish 2379) и `seed` (`as-seed`); сид-ключи по spec §4; конвенция чеков `ect()`/`api()`/`wait_alert()` (Task 6 переиспользует те же имена в своих файлах — код повторяется, не импортируется).

- [ ] **Шаг 1: `dev-stand/seed/Dockerfile`**

```dockerfile
# Образ сида: etcdctl 3.5.21 поверх alpine. Официальный образ etcd —
# distroless (без sh), поэтому бинарик копируем из него (arch/04 §2.2).
FROM alpine:3.20
COPY --from=quay.io/coreos/etcd:v3.5.21 /usr/local/bin/etcdctl /usr/local/bin/etcdctl
```

- [ ] **Шаг 2: `dev-stand/seed.sh` (полный)**

```sh
#!/bin/sh
# Идемпотентный сид контроль-плейна demo (spec t10 §4).
# Значения = EtcdSeed интеграционных тестов; времена статус-ключей —
# динамические (now-60/-900/-7200), чтобы seeded-аномалии были живыми.
# Запуск: сервис seed (docker compose up) или docker compose run --rm seed.
set -eu

: "${ETCDCTL_ENDPOINTS:=http://etcd:2379}"
ECT() { etcdctl --endpoints="$ETCDCTL_ENDPOINTS" "$@"; }

# Идемпотентность: существующий config => состояние уже засеяно (в т.ч.
# эмуляторами с lease) — не портим, выходим успешно (spec §4).
if [ -n "$(ECT get /clusters/demo/config --print-value-only 2>/dev/null)" ]; then
  echo "seed: /clusters/demo уже засеян — пропускаю"
  exit 0
fi

now=$(date +%s)
put() { ECT put "$1" "$2" >/dev/null; }

echo "seed: пишу контроль-плейн demo (unix=$now)"
put /clusters/demo/config \
  "{\"buckets\":16,\"dbname\":\"demo\",\"created_unix\":$now}"

# Шарды: dsn/replicas/master (master статично; в full эмулятор перепишет с lease)
put /clusters/demo/shards/s1/dsn 'host=s1a,s1b port=5432 dbname=demo user=postgres'
put /clusters/demo/shards/s1/replicas '1'
put /clusters/demo/shards/s1/master 's1a:5432'
put /clusters/demo/shards/s2/dsn 'host=s2a,s2b port=5432 dbname=demo user=postgres'
put /clusters/demo/shards/s2/replicas '1'
put /clusters/demo/shards/s2/master 's2a:5432'

# Routing 16 бакетов фикс-раскладкой EtcdSeed (s1=10, s2=6; spec §4)
for b in 0 2 3 4 6 8 10 11 12 14; do put "/clusters/demo/buckets/routing/bucket_$b" s1; done
for b in 1 5 7 9 13 15;           do put "/clusters/demo/buckets/routing/bucket_$b" s2; done

# Статусы переездов: bucket_3 свежий; 7/11 протухшие (порог StaleMoveSeconds=600)
put /clusters/demo/buckets/status/bucket_3 \
  "{\"bucket\":\"bucket_3\",\"state\":\"SYNCING\",\"owner\":\"s1\",\"target\":\"s2\",\"started_unix\":$((now-120)),\"updated_unix\":$((now-60)),\"phase\":\"copy\"}"
put /clusters/demo/buckets/status/bucket_7 \
  "{\"bucket\":\"bucket_7\",\"state\":\"ABORTING\",\"owner\":\"s2\",\"target\":\"s1\",\"started_unix\":$((now-1000)),\"updated_unix\":$((now-900)),\"phase\":\"cleanup\",\"last_error\":\"receiver went away\"}"
put /clusters/demo/buckets/status/bucket_11 \
  "{\"bucket\":\"bucket_11\",\"state\":\"FROZEN\",\"owner\":\"s1\",\"target\":\"s2\",\"started_unix\":$((now-7400)),\"updated_unix\":$((now-7200)),\"phase\":\"cutover-wait\"}"

put /clusters/demo/heals/bucket_5 \
  "{\"bucket\":\"bucket_5\",\"was\":\"s2\",\"now\":\"s1\",\"reason\":\"restore-heal\",\"ts\":$((now-86400))}"

# HA-DCS: два scope; статично (в full эмуляторы перепишут members/leader/optime с lease)
for s in s1 s2; do
  a="${s}a"; b="${s}b"
  put "/service/demo-$s/leader" "{\"name\":\"$a\"}"
  put "/service/demo-$s/members/$a" "{\"name\":\"$a\",\"conn_url\":\"$a:5432\",\"role\":\"master\",\"state\":\"running\",\"timeline\":1,\"lag\":0}"
  put "/service/demo-$s/members/$b" "{\"name\":\"$b\",\"conn_url\":\"$b:5432\",\"role\":\"replica\",\"state\":\"streaming\",\"timeline\":1,\"lag\":0}"
done
put /service/demo-s1/optime/leader '738273634528'
put /service/demo-s1/initialize '738273612345678'
put /service/demo-s1/config '{"ttl":5,"loop_wait":2,"retry_timeout":3}'
put /service/demo-s2/optime/leader '738273634001'
put /service/demo-s2/initialize '738273611234567'
put /service/demo-s2/config '{"ttl":5,"loop_wait":2,"retry_timeout":3}'

# Стендовая топология (в full перепишут эмуляторы реальными IP с lease)
put /cluster/nodes/s1a '172.28.0.11'
put /cluster/nodes/s1b '172.28.0.12'
put /cluster/nodes/s2a '172.28.0.21'
put /cluster/nodes/s2b '172.28.0.22'

# Самопроверка: ключи легли (spec §4)
[ -n "$(ECT get /clusters/demo/config --print-value-only)" ] || { echo "seed: ❌config не записан"; exit 1; }
[ -n "$(ECT get /clusters/demo/buckets/routing/bucket_0 --print-value-only)" ] || { echo "seed: ❌routing не записан"; exit 1; }
echo "seed: ✓ контроль-плейн demo засеян"
```

- [ ] **Шаг 3: `dev-stand/docker-compose.yml` (уровень quick)**

```yaml
# Dev-стенд AdminPanel (arch/04, spec t10): профили quick (etcd+seed) и
# full (+PG-шарды и patroni-эмуляторы; добавляются задачами 3-4).
# Имена сервисов канонические (DNS сети стенда: на них построены
# DSN/репликация), container_name — с префиксом as- (не конфликтуют со
# стендом ../pg, который порты на хост не публикует; arch/04 §1).
name: adminpanel-stand

services:
  etcd:
    image: quay.io/coreos/etcd:v3.5.21
    container_name: as-etcd
    ports:
      - "2379:2379"
    command:
      - etcd
      - --data-dir=/var/etcd/data
      - --listen-client-urls=http://0.0.0.0:2379
      - --advertise-client-urls=http://etcd:2379
    volumes:
      - etcd-data:/var/etcd/data

  seed:
    build: ./seed
    container_name: as-seed
    restart: "no"
    environment:
      ETCDCTL_ENDPOINTS: http://etcd:2379
    volumes:
      - ./seed.sh:/seed.sh:ro
    entrypoint: ["/bin/sh", "/seed.sh"]
    depends_on: [etcd]

volumes:
  etcd-data:
```

- [ ] **Шаг 4: `dev-stand/checks/90-down.sh`**

```bash
#!/usr/bin/env bash
# Разбор стенда; -v — стереть и данные (вкл. etcd-data; spec t10 §7.6).
set -euo pipefail
cd "$(dirname "$0")/.."
if [ "${1:-}" = "-v" ]; then
  docker compose --profile full down -v --remove-orphans
  echo "✓ стенд разобран (данные стёрты)"
else
  docker compose --profile full down --remove-orphans
  echo "✓ стенд разобран (etcd-data сохранён)"
fi
```

- [ ] **Шаг 5: `dev-stand/checks/10-smoke-api.sh`**

```bash
#!/usr/bin/env bash
# Дым API: панель против стенда — auth + все зоны инспекции (spec t10 §7.2).
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5000}"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT

# Arrange: панель поднята (до 60 c; запуск руками — arch/04 §5)
for i in $(seq 1 60); do curl -fsS "$BASE/api/healthz" >/dev/null 2>&1 && break; sleep 1; done
curl -fsS "$BASE/api/healthz" >/dev/null \
  || { echo "❌ панель не отвечает: $BASE/api/healthz (dotnet run --project src/AdminPanel.Api)"; exit 1; }
curl -fsS "$BASE/api/healthz" | jq -e '.status == "ok"' >/dev/null \
  || { echo "❌ /api/healthz: тело не {\"status\":\"ok\"}"; exit 1; }
echo "  панель жива ($BASE, status=ok)"

# Act/Assert: 401 без cookie, login -> cookie
code="$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/overview")"
[ "$code" = 401 ] || { echo "❌ /api/overview без cookie = $code, ожидался 401"; exit 1; }
echo "  без cookie /api/overview -> 401"
curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login admin/admin не прошёл"; exit 1; }
echo "  login admin -> cookie"

api() { curl -fsS -b "$JAR" "$BASE$1"; }

api /api/overview | jq -e \
  '.etcd.reachable == true and .alertsCritical >= 0
   and (.clusters | length) == 1 and .clusters[0].buckets == 16' >/dev/null \
  || { echo "❌ /api/overview: etcd.reachable/alertsCritical/clusters"; exit 1; }
echo "  /api/overview: etcd reachable, demo 16 бакетов, alertsCritical=$(api /api/overview | jq -r '.alertsCritical')"

api /api/etcd/status | jq -e \
  '.endpoints[0].reachable == true and (.endpoints[0].version | length > 0)' >/dev/null \
  || { echo "❌ /api/etcd/status: endpoint/version"; exit 1; }
echo "  /api/etcd/status: $(api /api/etcd/status | jq -r '.endpoints[0].version') reachable"

api /api/clusters/demo | jq -e '
  ([.shards[] | select(.name=="s1")][0].masterAddress == "s1a:5432")
  and (.buckets | length) == 16
  and .heals[0].bucket == "bucket_5"' >/dev/null \
  || { echo "❌ /api/clusters/demo: master/buckets/heals"; exit 1; }
echo "  /api/clusters/demo: master s1a:5432, 16 бакетов, heal bucket_5"

api /api/ha/demo-s1 | jq -e \
  '.leaderName == "s1a" and (.members | length) == 2
   and ([.members[] | select(.name=="s1a")][0].role == "master")' >/dev/null \
  || { echo "❌ /api/ha/demo-s1: leader/members"; exit 1; }
echo "  /api/ha/demo-s1: leader s1a, 2 члена"

# Сид-аномалии видны в алертах (тик панели 3 c — ждём до 15 c)
for i in $(seq 1 15); do
  api /api/alerts | jq -e 'any(.[]; .kind=="move-stale" and .target=="demo/bucket_11")' >/dev/null && break
  sleep 1
done
api /api/alerts | jq -e \
  'any(.[]; .kind=="move-stale" and .target=="demo/bucket_11")
   and any(.[]; .kind=="move-aborting" and .target=="demo/bucket_7")' >/dev/null \
  || { echo "❌ /api/alerts: seeded move-stale/move-aborting не видны"; exit 1; }
echo "  /api/alerts: move-stale bucket_11, move-aborting bucket_7"

echo "✓ smoke API зелёный"
```

- [ ] **Шаг 6: права + подъём quick-профиля**

```bash
chmod +x dev-stand/seed.sh dev-stand/checks/*.sh
cd dev-stand && docker compose up -d --build
```

Ожидание: подняты `as-etcd` и `as-seed` (seed завершится exit 0). `docker compose ps` — seed `Exited (0)`.

- [ ] **Шаг 7: проверить сид и идемпотентность (критерий spec §4)**

```bash
cd dev-stand
docker compose exec -T etcd etcdctl --endpoints=http://localhost:2379 get /clusters/demo/config
rev1="$(docker compose exec -T etcd etcdctl --endpoints=http://localhost:2379 get /clusters/demo/buckets/routing/bucket_0 -w json | jq -r '.kvs[0].mod_revision')"
docker compose run --rm seed
rev2="$(docker compose exec -T etcd etcdctl --endpoints=http://localhost:2379 get /clusters/demo/buckets/routing/bucket_0 -w json | jq -r '.kvs[0].mod_revision')"
echo "rev1=$rev1 rev2=$rev2"; test "$rev1" = "$rev2"
```

Ожидание: вывод config-JSON; повторный seed печатает `уже засеян — пропускаю`; `rev1 == rev2` (мод-revision статичного ключа не вырос — идемпотентность доказана).

- [ ] **Шаг 8: smoke против панели**

```bash
cd "$WT" && nohup dotnet run --project src/AdminPanel.Api >/tmp/adminpanel.log 2>&1 &
echo $! >/tmp/adminpanel.pid
dev-stand/checks/10-smoke-api.sh; rc=$?
kill "$(cat /tmp/adminpanel.pid)"; exit $rc
```

Ожидание: все строки-ассерты + финал `✓ smoke API зелёный`, код 0.

- [ ] **Шаг 9: коммит**

```bash
git add dev-stand/
git commit -m "t10: каркас dev-stand — compose quick (etcd+seed), идемпотентный seed.sh, чеки 90-down/10-smoke-api"
```

---

### Task 3: full-профиль PG — шарды s1a/s1b, s2a/s2b (реплики + self-healing)

**Files:**
- Modify: `dev-stand/docker-compose.yml` (добавить 4 PG-сервиса после `seed`)

**Interfaces:**
- Consumes: compose-проект Task 2.
- Produces: сервисы `s1a`,`s1b`,`s2a`,`s2b` (profile `full`), БД `demo` на *a-нодах, физрепликация `*b → *a`, слоты `<node>_phys`, `application_name` = имени реплики (Task 4 ждёт реплики в recovery; чеки используют `psql -U postgres`).

- [ ] **Шаг 1: дописать PG-сервисы в `dev-stand/docker-compose.yml`**

После сервиса `seed` (до `volumes:`) добавить:

```yaml
  # Шард 1: s1a (мастер; self-healing — при пустом PGDATA и живом мастере-пире
  # клонируется репликой: повторные прогоны после failover, spec t10 §3) + s1b
  # (физическая реплика через pg_basebackup -R; паттерн ../pg arch/stand).
  # Имена sync-standby НЕ запекаются во флаги -c: флаг сильнее ALTER SYSTEM и
  # после promote без реплики повесит коммиты в SyncRep (урок ../pg) — их
  # ставит 00-up.sh ALTER SYSTEM'ом.
  s1a:
    image: postgres:18
    container_name: as-s1a
    profiles: ["full"]
    ports: ["5433:5432"]
    user: postgres
    environment:
      POSTGRES_HOST_AUTH_METHOD: trust
      POSTGRES_DB: demo
    command:
      - bash
      - -c
      - |
        set -e
        if [ ! -s "$$PGDATA/PG_VERSION" ]; then
          cloned=0
          for i in $$(seq 1 15); do
            if pg_isready -h s1b -U postgres -q 2>/dev/null \
               && [ "$$(psql -h s1b -U postgres -d postgres -tAc 'select pg_is_in_recovery()' 2>/dev/null)" = "f" ]; then
              echo "s1a: клонируюсь от мастера s1b (self-healing)"
              rm -rf "$$PGDATA"
              psql -h s1b -U postgres -d postgres -tAc "select pg_drop_replication_slot('s1a_phys')" >/dev/null 2>&1 || true
              pg_basebackup -h s1b -U postgres -D "$$PGDATA" -X stream -R -C -S s1a_phys
              chmod 700 "$$PGDATA"
              sed -i "s|^primary_conninfo.*|primary_conninfo = 'host=s1b port=5432 user=postgres dbname=postgres application_name=s1a'|" "$$PGDATA/postgresql.auto.conf"
              exec postgres -c wal_level=logical -c max_wal_senders=16 -c max_replication_slots=16 \
                   -c sync_replication_slots=on -c hot_standby_feedback=on
            fi
            sleep 1
          done
          echo "s1a: пир s1b не мастер — первый старт, initdb-мастер"
          exec docker-entrypoint.sh postgres -c wal_level=logical -c max_wal_senders=16 -c max_replication_slots=16
        fi
        exec postgres -c wal_level=logical -c max_wal_senders=16 -c max_replication_slots=16

  s1b:
    image: postgres:18
    container_name: as-s1b
    profiles: ["full"]
    ports: ["5434:5432"]
    user: postgres
    environment:
      POSTGRES_HOST_AUTH_METHOD: trust
    command:
      - bash
      - -c
      - |
        # Реплика шарда 1: клон с retry (ждёт hba/готовность s1a — их патчит
        # 00-up.sh), failover-слоты + application_name для sync-standby.
        set -e
        if [ ! -s "$$PGDATA/PG_VERSION" ]; then
          while ! pg_basebackup -h s1a -U postgres -D "$$PGDATA" -X stream -R -C -S s1b_phys; do
            echo "s1b: жду s1a (инициализация/hba)..."; sleep 2; rm -rf "$$PGDATA"
          done
          chmod 700 "$$PGDATA"
          sed -i "s|^primary_conninfo.*|primary_conninfo = 'host=s1a port=5432 user=postgres dbname=postgres application_name=s1b'|" "$$PGDATA/postgresql.auto.conf"
        fi
        exec postgres -c wal_level=logical -c max_wal_senders=16 -c max_replication_slots=16 \
             -c sync_replication_slots=on -c hot_standby_feedback=on
    depends_on: [s1a]

  # Шард 2 — зеркально шарду 1 (s2a self-healing мастер, s2b реплика)
  s2a:
    image: postgres:18
    container_name: as-s2a
    profiles: ["full"]
    ports: ["5435:5432"]
    user: postgres
    environment:
      POSTGRES_HOST_AUTH_METHOD: trust
      POSTGRES_DB: demo
    command:
      - bash
      - -c
      - |
        set -e
        if [ ! -s "$$PGDATA/PG_VERSION" ]; then
          cloned=0
          for i in $$(seq 1 15); do
            if pg_isready -h s2b -U postgres -q 2>/dev/null \
               && [ "$$(psql -h s2b -U postgres -d postgres -tAc 'select pg_is_in_recovery()' 2>/dev/null)" = "f" ]; then
              echo "s2a: клонируюсь от мастера s2b (self-healing)"
              rm -rf "$$PGDATA"
              psql -h s2b -U postgres -d postgres -tAc "select pg_drop_replication_slot('s2a_phys')" >/dev/null 2>&1 || true
              pg_basebackup -h s2b -U postgres -D "$$PGDATA" -X stream -R -C -S s2a_phys
              chmod 700 "$$PGDATA"
              sed -i "s|^primary_conninfo.*|primary_conninfo = 'host=s2b port=5432 user=postgres dbname=postgres application_name=s2a'|" "$$PGDATA/postgresql.auto.conf"
              exec postgres -c wal_level=logical -c max_wal_senders=16 -c max_replication_slots=16 \
                   -c sync_replication_slots=on -c hot_standby_feedback=on
            fi
            sleep 1
          done
          echo "s2a: пир s2b не мастер — первый старт, initdb-мастер"
          exec docker-entrypoint.sh postgres -c wal_level=logical -c max_wal_senders=16 -c max_replication_slots=16
        fi
        exec postgres -c wal_level=logical -c max_wal_senders=16 -c max_replication_slots=16

  s2b:
    image: postgres:18
    container_name: as-s2b
    profiles: ["full"]
    ports: ["5436:5432"]
    user: postgres
    environment:
      POSTGRES_HOST_AUTH_METHOD: trust
    command:
      - bash
      - -c
      - |
        set -e
        if [ ! -s "$$PGDATA/PG_VERSION" ]; then
          while ! pg_basebackup -h s2a -U postgres -D "$$PGDATA" -X stream -R -C -S s2b_phys; do
            echo "s2b: жду s2a (инициализация/hba)..."; sleep 2; rm -rf "$$PGDATA"
          done
          chmod 700 "$$PGDATA"
          sed -i "s|^primary_conninfo.*|primary_conninfo = 'host=s2a port=5432 user=postgres dbname=postgres application_name=s2b'|" "$$PGDATA/postgresql.auto.conf"
        fi
        exec postgres -c wal_level=logical -c max_wal_senders=16 -c max_replication_slots=16 \
             -c sync_replication_slots=on -c hot_standby_feedback=on
    depends_on: [s2a]
```

- [ ] **Шаг 2: поднять full-часть и патч hba (репликация-trust — паттерн ../pg 00-up)**

```bash
cd dev-stand && docker compose --profile full up -d
for c in s1a s2a s1b s2b; do
  for i in $(seq 1 60); do
    docker compose exec -T "$c" pg_isready -U postgres -q 2>/dev/null && break; sleep 1
  done
  docker compose exec -T "$c" pg_isready -U postgres -q 2>/dev/null \
    || { echo "❌ $c не готов за 60 c (docker compose logs $c)"; exit 1; }
  echo "$c ready"
done
for c in s1a s2a s1b s2b; do
  docker compose exec -T "$c" bash -c \
    'grep -q "host replication all all trust" $PGDATA/pg_hba.conf || echo "host replication all all trust" >> $PGDATA/pg_hba.conf;
     psql -U postgres -d postgres -qtAc "select pg_reload_conf()" >/dev/null'
done
```

Ожидание: 4× «ready» (первый старт *a-нод занимает ~15 c — цикл ожидания пира); команды hba без ошибок.

- [ ] **Шаг 3: дождаться репликации и проверить**

```bash
for c in s1b s2b; do
  for i in $(seq 1 120); do
    [ "$(docker compose exec -T "$c" psql -U postgres -d postgres -tAc 'select pg_is_in_recovery()' 2>/dev/null)" = "t" ] && break
    sleep 2
  done
  [ "$(docker compose exec -T "$c" psql -U postgres -d postgres -tAc 'select pg_is_in_recovery()')" = "t" ] \
    || { echo "❌ $c не в recovery (репликация не поднялась)"; exit 1; }
  echo "$c in recovery"
done
docker compose exec -T s1a psql -U postgres -d postgres -tAc "select application_name, state, sync_state from pg_stat_replication"
docker compose exec -T s1a psql -U postgres -d demo -tAc "select 1"
```

Ожидание: обе реплики `t`; на s1a строка `s1b|streaming|async`; БД `demo` отвечает `1`.

- [ ] **Шаг 4: коммит**

```bash
git add dev-stand/docker-compose.yml
git commit -m "t10: full-профиль — PG-шарды s1a/s1b, s2a/s2b (физреплики, self-healing мастеров, trust+hba)"
```

---

### Task 4: patroni-эмуляторы hc* + чек 00-up

**Files:**
- Create: `dev-stand/sidecar/Dockerfile`, `dev-stand/sidecar/emulator.py`
- Modify: `dev-stand/docker-compose.yml` (4 сервиса `hc*`)
- Create: `dev-stand/checks/00-up.sh`

**Interfaces:**
- Consumes: PG-ноды Task 3 (compose-DNS `s1a`…), etcd Task 2 (gateway `/v3/*`).
- Produces: эмуляторы REST `:8008` (`/cluster`,`/primary`,`/replica`,`/`,`/read-only`), publish 8011/8012/8021/8022; lease-ключи `/clusters/demo/shards/<X>/master`, `/service/demo-s<X>/{leader,members/<n>,optime/leader}`, `/cluster/nodes/<n>` (TTL 5 c, цикл 1 c); `00-up.sh` — единая точка подъёма стенда для всех последующих чеков.

- [ ] **Шаг 1: `dev-stand/sidecar/Dockerfile`**

```dockerfile
# Patroni-эмулятор стенда: python + pg8000 (как сайдкар ../pg; arch/04 §2.3)
FROM python:3.12-alpine
RUN pip install --no-cache-dir pg8000
COPY emulator.py /emulator.py
EXPOSE 8008
CMD ["python", "/emulator.py"]
```

- [ ] **Шаг 2: `dev-stand/sidecar/emulator.py` (полный)**

```python
# Patroni-эмулятор dev-стенда AdminPanel (spec t10 §5): REST :8008 + etcd-lease.
# Развитие ../pg/arch/stand/sidecar/rolecheck.py (HTTP-основа, gateway-паттерн,
# lease-механика); отличия: /cluster в формате Patroni по составу MEMBERS,
# master-lease шардового ключа + leader/optime, регистрация только при живой PG.
import base64
import json
import os
import socket
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pg8000.native

PG_PORT = 5432
PG_USER = "postgres"
PG_DB = "postgres"
ETCD = os.getenv("ETCD_ENDPOINTS", "http://etcd:2379").rstrip("/")
NODE = os.getenv("NODE_NAME", "")
CLUSTER = os.getenv("CLUSTER", "demo")
SHARD = os.getenv("SHARD", "s1")
MEMBERS = [m.strip() for m in os.getenv("MEMBERS", NODE).split(",") if m.strip()]
LEASE_TTL = 5  # ключи гаснут <=5 c после смерти ноды (как Patroni TTL; arch/02 §2.1)
STEP_SEC = 1   # цикл опроса/продления в 5 раз чаще TTL (паттерн rolecheck)
SCOPE = f"{CLUSTER}-{SHARD}"

# Снимок членов scope: name -> {alive, role, state, timeline, lag}
state = {}
state_lock = threading.Lock()
last_role = {m: "replica" for m in MEMBERS}  # последняя известная роль (для stopped)


# ---------- опрос PG (spec §5.1) ----------
def probe_node(host):
    con = pg8000.native.Connection(host=host, port=PG_PORT, user=PG_USER, database=PG_DB)
    try:
        inrec, timeline, lag = con.run(
            "select pg_is_in_recovery(), (pg_control_checkpoint()).timeline,"
            " coalesce(pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn()), 0)"
        )[0]
        inrec = bool(inrec)
        return {
            "alive": True,
            "role": "replica" if inrec else "master",
            "state": "streaming" if inrec else "running",
            "timeline": int(timeline or 1),
            "lag": int(lag or 0),
        }
    finally:
        con.close()


def optime_lsn(host):
    # LSN мастера числом-строкой для optime/leader (формат как у EtcdSeed)
    con = pg8000.native.Connection(host=host, port=PG_PORT, user=PG_USER, database=PG_DB)
    try:
        return str(con.run("select (pg_current_wal_lsn() - '0/0')")[0][0])
    finally:
        con.close()


def node_ip(host):
    # IP своей PG-ноды в сети стенда: DNS-resolve (эмулятор — отдельный
    # контейнер, сокет-приём rolecheck дал бы адрес эмулятора; spec §5.3)
    return socket.gethostbyname(host)


# ---------- etcd gateway (паттерн rolecheck.py) ----------
def etcd_post(path, payload):
    req = urllib.request.Request(
        ETCD + path, data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.load(r)


def etcd_put_leased(key, value, lease_id):
    # gateway etcd 3.5 принимает lease как ДЕСЯТИЧНУЮ строку (не hex)
    etcd_post("/v3/kv/put", {
        "key": base64.b64encode(key.encode()).decode(),
        "value": base64.b64encode(value.encode()).decode(),
        "lease": str(lease_id),
    })


def lease_grant():
    return int(etcd_post("/v3/lease/grant", {"TTL": LEASE_TTL})["ID"])


def lease_keepalive(lease_id):
    etcd_post("/v3/lease/keepalive", {"ID": str(lease_id)})


# ---------- цикл: опрос членов + регистрация себя (spec §5.3) ----------
def poll_loop():
    lease_id = None
    prev_role = None
    while True:
        snap = {}
        for name in MEMBERS:
            try:
                snap[name] = probe_node(name)
                last_role[name] = snap[name]["role"]
            except Exception:
                snap[name] = {"alive": False, "role": last_role[name],
                              "state": "stopped", "timeline": None, "lag": None}
        with state_lock:
            state.clear()
            state.update(snap)

        own = snap.get(NODE)
        # Регистрация/продление — только пока своя PG отвечает: смерть ноды
        # убирает ключи через TTL <=5 c, как у Patroni (spec §5.3)
        if own is not None and own["alive"]:
            try:
                if lease_id is None:
                    lease_id = lease_grant()
                etcd_put_leased(f"/service/{SCOPE}/members/{NODE}", json.dumps({
                    "name": NODE, "conn_url": f"{NODE}:5432",
                    "role": own["role"], "state": own["state"],
                    "timeline": own["timeline"], "lag": own["lag"],
                }), lease_id)
                etcd_put_leased(f"/cluster/nodes/{NODE}", node_ip(NODE), lease_id)
                if own["role"] == "master":
                    etcd_put_leased(f"/clusters/{CLUSTER}/shards/{SHARD}/master",
                                    f"{NODE}:5432", lease_id)
                    etcd_put_leased(f"/service/{SCOPE}/leader",
                                    json.dumps({"name": NODE}), lease_id)
                    etcd_put_leased(f"/service/{SCOPE}/optime/leader",
                                    optime_lsn(NODE), lease_id)
                lease_keepalive(lease_id)
                if prev_role != own["role"]:
                    print(f"{NODE}: role {prev_role} -> {own['role']}", flush=True)
                    prev_role = own["role"]
            except Exception:
                lease_id = None  # lease истёк/etcd недоступен — пересоздать
        time.sleep(STEP_SEC)


# ---------- REST :8008 (spec §5.2) ----------
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/cluster":
            # Всегда 200, пока жив эмулятор: полный состав MEMBERS, мёртвые —
            # stopped (Patroni ведёт себя так; панели нужна запись по имени)
            with state_lock:
                members = [
                    {"name": n, "role": s["role"], "state": s["state"],
                     "timeline": s["timeline"], "lag": s["lag"],
                     "host": n, "port": PG_PORT}
                    for n, s in sorted(state.items())
                ]
            self._send(200, json.dumps({"members": members}).encode(),
                       content_type="application/json")
            return
        with state_lock:
            own = state.get(NODE)
        if own is None or not own["alive"]:
            self._send(503, b"pg unreachable\n")
            return
        if self.path in ("/", "/read-only"):
            ok = True
        elif self.path == "/primary":
            ok = own["role"] == "master"
        elif self.path == "/replica":
            ok = own["role"] == "replica"
        else:
            self._send(404, b"not found\n")
            return
        self._send(200 if ok else 503, b"OK\n")

    def _send(self, code, body, content_type="text/plain"):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass  # не шумим в логи контейнера


print(f"{NODE}: эмулятор scope {SCOPE}, members={MEMBERS}", flush=True)
threading.Thread(target=poll_loop, daemon=True).start()
ThreadingHTTPServer(("0.0.0.0", 8008), Handler).serve_forever()
```

- [ ] **Шаг 3: дописать `hc*` в `dev-stand/docker-compose.yml`**

После `s2b` (до `volumes:`) добавить:

```yaml
  # Patroni-эмуляторы (spec t10 §5): отдельные контейнеры в общей сети,
  # :8008 опубликован на хосте под стендовыми портами (HostMap панели,
  # arch/04 §1). Без restart-политики: рестарт эмулятора не должен
  # реанимировать остановленную ноду (урок ../pg).
  hc1a:
    build: ./sidecar
    container_name: as-hc1a
    profiles: ["full"]
    ports: ["8011:8008"]
    environment:
      NODE_NAME: s1a
      ETCD_ENDPOINTS: http://etcd:2379
      CLUSTER: demo
      SHARD: s1
      MEMBERS: s1a,s1b
    depends_on: [s1a]

  hc1b:
    build: ./sidecar
    container_name: as-hc1b
    profiles: ["full"]
    ports: ["8012:8008"]
    environment:
      NODE_NAME: s1b
      ETCD_ENDPOINTS: http://etcd:2379
      CLUSTER: demo
      SHARD: s1
      MEMBERS: s1a,s1b
    depends_on: [s1b]

  hc2a:
    build: ./sidecar
    container_name: as-hc2a
    profiles: ["full"]
    ports: ["8021:8008"]
    environment:
      NODE_NAME: s2a
      ETCD_ENDPOINTS: http://etcd:2379
      CLUSTER: demo
      SHARD: s2
      MEMBERS: s2a,s2b
    depends_on: [s2a]

  hc2b:
    build: ./sidecar
    container_name: as-hc2b
    profiles: ["full"]
    ports: ["8022:8008"]
    environment:
      NODE_NAME: s2b
      ETCD_ENDPOINTS: http://etcd:2379
      CLUSTER: demo
      SHARD: s2
      MEMBERS: s2a,s2b
    depends_on: [s2b]
```

Env-контракт эмулятора — `NODE_NAME`, `ETCD_ENDPOINTS`, `CLUSTER`, `SHARD`, `MEMBERS` (spec §5): адрес каждой опрашиваемой PG = её имя из `MEMBERS` (compose-DNS), отдельный PGHOST не нужен (решение — spec §13.13).

- [ ] **Шаг 4: `dev-stand/checks/00-up.sh` (полный)**

```bash
#!/usr/bin/env bash
# Подъём полного стенда (profile full) и приведение в рабочее состояние:
# реплики, sync-standby, инвентарь схем (spec t10 §7.1).
set -euo pipefail
cd "$(dirname "$0")/.."

# Arrange: инструменты хоста
for bin in docker jq curl; do
  command -v "$bin" >/dev/null || { echo "❌ нет $bin в PATH"; exit 1; }
done

echo ">>> поднимаю стенд (docker compose --profile full up -d --build)"
docker compose --profile full up -d --build 2>&1 | tail -5

ect() { docker compose exec -T etcd etcdctl --endpoints=http://localhost:2379 "$@"; }
sq()   { docker compose exec -T "$1" psql -U postgres -d postgres -qAt -v ON_ERROR_STOP=1 "$2"; }

# 1) etcd жив и сид на месте (спец: seed идемпотентен, ключи не портит)
for i in $(seq 1 60); do ect endpoint health >/dev/null 2>&1 && break; sleep 1; done
ect endpoint health >/dev/null 2>&1 \
  || { echo "  ❌ etcd не стал здоровым за 60 c (docker compose logs etcd)"; exit 1; }
echo "  etcd ready"
for i in $(seq 1 30); do
  [ -n "$(ect get /clusters/demo/config --print-value-only 2>/dev/null)" ] && break
  sleep 1
done
[ -n "$(ect get /clusters/demo/config --print-value-only 2>/dev/null)" ] \
  || { echo "❌ сид не появился за 30 c (сервис seed: docker compose logs seed)"; exit 1; }
echo "  сид контроль-плейна на месте"

# 2) PG-ноды готовы; hba-replication (нужен basebackup/rejoin — паттерн ../pg)
for c in s1a s2a s1b s2b; do
  for i in $(seq 1 60); do
    docker compose exec -T "$c" pg_isready -U postgres -q 2>/dev/null && break
    sleep 1
  done
  docker compose exec -T "$c" pg_isready -U postgres -q 2>/dev/null \
    || { echo "  ❌ $c не готов за 60 c (docker compose logs $c)"; exit 1; }
  echo "  $c ready"
done
patch_hba() {
  docker compose exec -T "$1" bash -c \
    'grep -q "host replication all all trust" $PGDATA/pg_hba.conf || echo "host replication all all trust" >> $PGDATA/pg_hba.conf;
     psql -U postgres -d postgres -qtAc "select pg_reload_conf()" >/dev/null'
}
for c in s1a s1b s2a s2b; do patch_hba "$c"; done
echo "  pg_hba: replication-trust добавлен всем нодам"

# 3) реплики в recovery (базовый basebackup идёт с retry в command-скриптах нод)
for c in s1b s2b; do
  for i in $(seq 1 120); do
    [ "$(sq "$c" 'select pg_is_in_recovery()' 2>/dev/null)" = "t" ] && break
    sleep 2
  done
  [ "$(sq "$c" 'select pg_is_in_recovery()' 2>/dev/null)" = "t" ] \
    || { echo "❌ $c не стала репликой за 240 c (docker compose logs $c)"; exit 1; }
  echo "  $c в recovery (реплика своего шарда)"
done

# 4) эмуляторы зарегистрировались: lease-ключи /cluster/nodes + master шардов
for c in s1a s1b s2a s2b; do
  for i in $(seq 1 30); do
    [ -n "$(ect get "/cluster/nodes/$c" --print-value-only 2>/dev/null)" ] && break
    sleep 1
  done
  [ -n "$(ect get "/cluster/nodes/$c" --print-value-only 2>/dev/null)" ] \
    || { echo "❌ $c не зарегистрирован в /cluster/nodes (эмулятор hc: docker compose logs hc*)"; exit 1; }
done
echo "  эмуляторы: /cluster/nodes/* живы (lease TTL 5 c)"
m1="$(ect get /clusters/demo/shards/s1/master --print-value-only)"
m2="$(ect get /clusters/demo/shards/s2/master --print-value-only)"
[ -n "$m1" ] && [ -n "$m2" ] \
  || { echo "  ❌ master-ключ шарда пуст (s1='$m1' s2='$m2' — эмулятор мастера не зашёл в цикл?)"; exit 1; }
echo "  master s1=$m1 s2=$m2"

# 5) sync-standby: имена ALTER SYSTEM'ом (НЕ флагами -c — ловушка SyncRep,
#    урок ../pg: после promote без реплики коммиты виснут)
set_sync() { # master replica
  docker compose exec -T "$1" psql -U postgres -d postgres -qAt \
    -c "ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 ($2)'" \
    -c "SELECT pg_reload_conf()" >/dev/null
  st=""
  for i in $(seq 1 30); do
    st="$(sq "$1" "select sync_state from pg_stat_replication where application_name='$2'")"
    [ "$st" = "sync" ] && break
    sleep 1
  done
  [ "$st" = "sync" ] || { echo "❌ $2 не sync-standby у $1 (было: ${st:-нет})"; exit 1; }
  echo "  $1: sync-standby $2 -> sync"
}
master1="${m1%:*}"; rep1=s1b; [ "$master1" = s1b ] && rep1=s1a
master2="${m2%:*}"; rep2=s2b; [ "$master2" = s2b ] && rep2=s2a
set_sync "$master1" "$rep1"
set_sync "$master2" "$rep2"

# 6) инвентарь: схемы bucket_% только ACTIVE-бакетов владельца (spec §6:
#    inventory-mismatch сверяет только ACTIVE; 8 на s1, 5 на s2)
schemas() { # master "список бакетов"
  for b in $2; do
    docker compose exec -T "$1" psql -U postgres -d demo -qAt \
      -c "CREATE SCHEMA IF NOT EXISTS bucket_$b" >/dev/null
  done
}
schemas "$master1" "0 2 4 6 8 10 12 14"
schemas "$master2" "1 5 9 13 15"
echo "  инвентарь: 8 схем на $master1, 5 на $master2"

echo "✓ стенд поднят"
```

- [ ] **Шаг 5: чистый прогон 00-up (стенд из Task 3 уже поднят — сначала пересборка)**

```bash
chmod +x dev-stand/checks/00-up.sh
cd dev-stand && checks/00-up.sh
```

Ожидание: все промежуточные строки, финал `✓ стенд поднят`, код 0. Общее время ≤ 3 мин.

- [ ] **Шаг 6: прямые проверки эмуляторов с хоста**

```bash
curl -fsS -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8011/primary   # 200
curl -fsS -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8012/replica   # 200
curl -fsS http://127.0.0.1:8011/cluster | jq -c '.members[] | {name,role,state}'
```

Ожидание: `200`, `200`; два члена: s1a master/running, s1b replica/streaming.

- [ ] **Шаг 7: коммит**

```bash
git add dev-stand/
git commit -m "t10: patroni-эмуляторы hc* (REST /cluster|/primary|/replica, master-lease TTL 5 c, members/nodes) + чек 00-up"
```

---

### Task 5: HostMap стенда в appsettings.Development.json

**Files:**
- Modify: `src/AdminPanel.Api/appsettings.Development.json`

**Interfaces:**
- Produces: `AdminPanel:Probes:HostMap` (8 записей, spec §8) — на них работают Patroni/SQL-пробы панели в чеках 40/10.

- [ ] **Шаг 1: заменить файл целиком**

```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Microsoft.AspNetCore": "Warning"
    }
  },
  "AllowedHosts": "*",
  "AdminPanel": {
    "Auth": {
      "Username": "admin",
      "Password": "admin",
      "AllowHttp": true
    },
    "Etcd": {
      "Endpoints": [ "http://localhost:2379" ]
    },
    "Probes": {
      "Password": "",
      "HostMap": {
        "s1a:5432": "127.0.0.1:5433",
        "s1b:5432": "127.0.0.1:5434",
        "s2a:5432": "127.0.0.1:5435",
        "s2b:5432": "127.0.0.1:5436",
        "s1a:8008": "127.0.0.1:8011",
        "s1b:8008": "127.0.0.1:8012",
        "s2a:8008": "127.0.0.1:8021",
        "s2b:8008": "127.0.0.1:8022"
      }
    }
  }
}
```

- [ ] **Шаг 2: сборка без регрессий**

Run: `cd "$WT" && dotnet build && dotnet test`
Ожидание: Build succeeded, 0 Error (Warnings как ошибки — их нет); все тесты PASS.

- [ ] **Шаг 3: коммит**

```bash
git add src/AdminPanel.Api/appsettings.Development.json
git commit -m "t10: HostMap стенда в appsettings.Development (пробы на 127.0.0.1:5433-5436/8011-8022)"
```

---

### Task 6: чеки 20-alerts / 30-failover / 40-live-probes

**Files:**
- Create: `dev-stand/checks/20-alerts.sh`, `dev-stand/checks/30-failover.sh`, `dev-stand/checks/40-live-probes.sh`

**Interfaces:**
- Consumes: поднятый full-стенд (Task 4, `00-up.sh`), панель на хосте (HostMap Task 5), kind-имена алертов (`shard-no-master`, `shard-no-leader`, `move-stale`, `move-aborting`, `probe-failed`, `inventory-mismatch`), target-формат `demo/s1`, `demo-s1`, `demo/bucket_11`.

- [ ] **Шаг 1: `dev-stand/checks/20-alerts.sh` (полный)**

```bash
#!/usr/bin/env bash
# Seeded-аномалии + shard-no-master: появление <=2 тиков и гашение (spec t10 §7.3).
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5000}"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT
ect() { docker compose exec -T etcd etcdctl --endpoints=http://localhost:2379 "$@"; }

curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login (панель на $BASE? запусти dotnet run)"; exit 1; }
api() { curl -fsS -b "$JAR" "$BASE$1"; }

has_alert() { # kind target [severity]
  api /api/alerts | jq -e --arg k "$1" --arg t "$2" --arg s "${3:-}" \
    'any(.[]; .kind==$k and .target==$t and ($s=="" or .severity==$s))' >/dev/null
}
wait_alert() {
  for i in $(seq 1 15); do has_alert "$1" "$2" "${3:-}" && return 0; sleep 1; done
  echo "❌ алерт $1 -> $2${3:+ ($3)} не появился за 15 c"; return 1
}
wait_no_alert() {
  for i in $(seq 1 15); do has_alert "$1" "$2" || return 0; sleep 1; done
  echo "❌ алерт $1 -> $2 не погас за 15 c"; return 1
}

# Assert 1: seeded-аномалии (тик панели 3 c)
wait_alert move-stale demo/bucket_11;   echo "  move-stale -> demo/bucket_11"
wait_alert move-aborting demo/bucket_7; echo "  move-aborting -> demo/bucket_7"

# Act 2: удалить master-ключ s2 (в full сначала стоп эмуляторов — keepalive
# каждые 1 c переписал бы ключ; spec §7.3)
full=0
if docker compose ps --services --filter status=running 2>/dev/null | grep -qx hc2a; then
  full=1
  echo "  (full) стоп эмуляторов s2: hc2a/hc2b"
  docker compose stop hc2a hc2b >/dev/null
fi
ect del /clusters/demo/shards/s2/master >/dev/null
echo "  master-ключ s2 удалён"

# Assert 3: critical-алерт <=2 тиков
wait_alert shard-no-master demo/s2 critical
echo "  shard-no-master -> demo/s2 (critical)"

# Act 4: восстановление
if [ "$full" = 1 ]; then
  docker compose start hc2a hc2b >/dev/null
  echo "  (full) эмуляторы s2 запущены — lease восстановится сам (<=3 c)"
else
  ect put /clusters/demo/shards/s2/master 's2a:5432' >/dev/null
  echo "  (quick) ключ возвращён статично"
fi

# Assert 5: алерт погас
wait_no_alert shard-no-master demo/s2
echo "  shard-no-master -> demo/s2 погас"

echo "✓ alerts-сценарий зелёный"
```

- [ ] **Шаг 2: `dev-stand/checks/30-failover.sh` (полный)**

```bash
#!/usr/bin/env bash
# Failover шарда 1: stop мастера -> алерты -> promote s1b -> гашение ->
# rejoin s1a репликой (spec t10 §7.4). Только full-профиль.
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5000}"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT
ect() { docker compose exec -T etcd etcdctl --endpoints=http://localhost:2379 "$@"; }
sq()   { docker compose exec -T "$1" psql -U postgres -d postgres -qAt -v ON_ERROR_STOP=1 "$2"; }

curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login (панель на $BASE?)"; exit 1; }
api() { curl -fsS -b "$JAR" "$BASE$1"; }
has_alert() { api /api/alerts | jq -e --arg k "$1" --arg t "$2" 'any(.[]; .kind==$k and .target==$t)' >/dev/null; }
wait_alert()    { for i in $(seq 1 15); do has_alert "$1" "$2" && return 0; sleep 1; done; echo "❌ алерт $1 -> $2 не появился за 15 c"; return 1; }
wait_no_alert() { for i in $(seq 1 15); do has_alert "$1" "$2" || return 0; sleep 1; done; echo "❌ алерт $1 -> $2 не погас за 15 c"; return 1; }

m1="$(ect get /clusters/demo/shards/s1/master --print-value-only)"
[ "$m1" = "s1a:5432" ] \
  || { echo "❌ мастер s1 сейчас $m1 — сценарий требует s1a (перезапусти стенд: checks/90-down.sh -v && checks/00-up.sh)"; exit 1; }

# Act 1: отказ мастера
echo ">>> docker stop s1a (мастер s1)"
docker compose stop -t 3 s1a >/dev/null

# Assert 2: lease-ключи гаснут (TTL 5 c — запас 10 c)
for i in $(seq 1 10); do [ -z "$(ect get /clusters/demo/shards/s1/master --print-value-only 2>/dev/null)" ] && break; sleep 1; done
[ -z "$(ect get /clusters/demo/shards/s1/master --print-value-only 2>/dev/null)" ] \
  || { echo "❌ master-ключ s1 не погас (lease эмулятора hc1a жив?)"; exit 1; }
for i in $(seq 1 10); do [ -z "$(ect get /service/demo-s1/leader --print-value-only 2>/dev/null)" ] && break; sleep 1; done
[ -z "$(ect get /service/demo-s1/leader --print-value-only 2>/dev/null)" ] \
  || { echo "  ❌ leader-ключ demo-s1 не погас"; exit 1; }
for i in $(seq 1 10); do [ -z "$(ect get /service/demo-s1/optime/leader --print-value-only 2>/dev/null)" ] && break; sleep 1; done
[ -z "$(ect get /service/demo-s1/optime/leader --print-value-only 2>/dev/null)" ] \
  || { echo "  ❌ optime/leader demo-s1 не погас"; exit 1; }
echo "  lease-ключи s1 погасли: master, leader, optime (<=10 c)"

# Assert 3: панель видит оба алерта
wait_alert shard-no-master demo/s1; echo "  shard-no-master -> demo/s1"
wait_alert shard-no-leader demo-s1; echo "  shard-no-leader -> demo-s1"

# Act 4: promote s1b (+ снятие sync-имён: без реплики коммиты виснут — урок ../pg)
PGD="$(docker compose exec -T -u postgres s1b psql -U postgres -d postgres -tAc 'show data_directory' | tr -d '[:space:]')"
docker compose exec -T -u postgres s1b pg_ctl promote -D "$PGD" >/dev/null 2>&1 || true
for i in $(seq 1 60); do [ "$(sq s1b 'select pg_is_in_recovery()' 2>/dev/null)" = "f" ] && break; sleep 1; done
[ "$(sq s1b 'select pg_is_in_recovery()')" = "f" ] \
  || { echo "  ❌ s1b не вышла из recovery за 60 c (promote?)"; exit 1; }
sq s1b "ALTER SYSTEM SET synchronous_standby_names = ''" >/dev/null
sq s1b "SELECT pg_reload_conf()" >/dev/null
echo "  s1b повышен до мастера (sync-имена сняты)"

# Assert 5: эмулятор s1b взял lease; алерты гаснут; REST показывает нового мастера
for i in $(seq 1 10); do [ "$(ect get /clusters/demo/shards/s1/master --print-value-only 2>/dev/null)" = "s1b:5432" ] && break; sleep 1; done
[ "$(ect get /clusters/demo/shards/s1/master --print-value-only)" = "s1b:5432" ] \
  || { echo "❌ master-ключ не перешёл к s1b"; exit 1; }
ect get /service/demo-s1/leader --print-value-only | jq -e '.name == "s1b"' >/dev/null \
  || { echo "❌ leader не s1b"; exit 1; }
echo "  master-ключ и leader у s1b"
curl -fsS -o /dev/null http://127.0.0.1:8012/primary \
  || { echo "❌ hc1b /primary != 200"; exit 1; }
curl -fsS http://127.0.0.1:8011/cluster | jq -e \
  'any(.members[]; .name=="s1b" and .role=="master")
   and any(.members[]; .name=="s1a" and .state=="stopped")' >/dev/null \
  || { echo "❌ /cluster не показывает s1b-мастера / s1a-stopped"; exit 1; }
echo "  Patroni-REST: s1b master, s1a stopped"
wait_no_alert shard-no-master demo/s1; echo "  shard-no-master погас"
wait_no_alert shard-no-leader demo-s1; echo "  shard-no-leader погас"
api /api/ha/demo-s1 | jq -e 'any(.members[]; .name=="s1b" and .role=="master")' >/dev/null \
  || { echo "❌ /api/ha/demo-s1 не видит s1b мастером"; exit 1; }
echo "  /api/ha/demo-s1: s1b master"

# Act 6: rejoin s1a репликой (self-healing: пустой PGDATA -> клон от s1b)
echo ">>> rejoin: пересоздаю s1a репликой s1b"
docker compose rm -sf s1a >/dev/null
sq s1b "select pg_drop_replication_slot('s1a_phys')" >/dev/null 2>&1 || true
docker compose up -d s1a >/dev/null
ok=""
for i in $(seq 1 120); do
  [ "$(docker compose exec -T s1a psql -U postgres -d postgres -tAc 'select pg_is_in_recovery()' 2>/dev/null)" = "t" ] && { ok=1; break; }
  sleep 2
done
[ -n "$ok" ] || { echo "❌ s1a не поднялась репликой за 240 c (docker compose logs s1a)"; exit 1; }
echo "  s1a в recovery (клон s1b)"
sq s1b "ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (s1a)'" >/dev/null
sq s1b "SELECT pg_reload_conf()" >/dev/null
st=""
for i in $(seq 1 30); do
  st="$(sq s1b "select sync_state from pg_stat_replication where application_name='s1a'")"
  [ "$st" = "sync" ] && break
  sleep 1
done
[ "$st" = "sync" ] || { echo "❌ s1a не sync-standby у s1b"; exit 1; }
echo "  s1b: sync-standby s1a -> sync"

echo "✓ failover-цикл зелёный: алерт -> promote -> гашение -> rejoin"
```

- [ ] **Шаг 3: `dev-stand/checks/40-live-probes.sh` (полный)**

```bash
#!/usr/bin/env bash
# Live-пробы панели: Patroni-REST через HostMap + SQL-пробы multi-host
# (spec t10 §7.5). Гоняется ПОСЛЕ 30-failover (мастер s1 = s1b, реплика
# s1a sync; шард 2 нетронут). Только full.
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5000}"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT
curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login (панель на $BASE?)"; exit 1; }
api() { curl -fsS -b "$JAR" "$BASE$1"; }

# 1) HA-скопы: все члены пробиты без ошибок; роли/состояния живые.
#    Тик проб 15 c — ждём до 40 c (spec §7.5).
ha_ok() {
  api /api/ha/"$1" | jq -e '
    all(.members[]; .probeError == null and .probeAtUtc != null)
    and any(.members[]; .state == "running")
    and any(.members[]; .state == "streaming" and .lagBytes != null and .timeline >= 1)' >/dev/null
}
for i in $(seq 1 40); do ha_ok demo-s1 && break; sleep 2; done
ha_ok demo-s1 || { echo "❌ /api/ha/demo-s1: пробы не обогащены (HostMap? эмуляторы?)"; exit 1; }
echo "  demo-s1: мастер running, реплика streaming+lag (Patroni-REST через HostMap)"
for i in $(seq 1 40); do ha_ok demo-s2 && break; sleep 2; done
ha_ok demo-s2 || { echo "❌ /api/ha/demo-s2: пробы не обогащены"; exit 1; }
echo "  demo-s2: то же"

# 2) SQL-пробы: runtime без ошибок, sync-standby, инвентарь 8+5, lease живы
cl_ok() {
  api /api/clusters/demo | jq -e '
    ([.shards[] | select(.name=="s1")][0] |
      .runtime.error == null and .runtime.standbiesSync >= 1
      and (.runtime.bucketSchemas | length) == 8
      and .masterAddress == "s1b:5432" and .masterLeaseAlive == true)
    and ([.shards[] | select(.name=="s2")][0] |
      .runtime.error == null and .runtime.standbiesSync >= 1
      and (.runtime.bucketSchemas | length) == 5
      and .masterLeaseAlive == true)' >/dev/null
}
for i in $(seq 1 40); do cl_ok && break; sleep 2; done
cl_ok || { echo "❌ /api/clusters/demo: runtime/инвентарь/lease (SQL-проба на 127.0.0.1:5433-5436?)"; exit 1; }
echo "  SQL-пробы: runtime шардов жив, sync-standby есть, инвентарь 8+5"

# 3) никаких ошибок проб и расхождений (spec §7.5 п.4)
api /api/alerts | jq -e \
  'all(.[]; .kind != "probe-failed" and .kind != "inventory-mismatch" and .kind != "shard-no-master")' >/dev/null \
  || { echo "❌ /api/alerts: есть probe-failed / inventory-mismatch / shard-no-master"; exit 1; }
echo "  алертов проб/инвентаря/без-мастера нет"

echo "✓ live-probes зелёный"
```

- [ ] **Шаг 4: прогнать сценарий (стенд и панель подняты; 00-up уже выполнен в Task 4)**

```bash
chmod +x dev-stand/checks/20-alerts.sh dev-stand/checks/30-failover.sh dev-stand/checks/40-live-probes.sh
dev-stand/checks/20-alerts.sh && dev-stand/checks/30-failover.sh && dev-stand/checks/40-live-probes.sh
```

Ожидание: три финальных `✓ … зелёный`, суммарный код 0. 30-й оставляет стенд в топологии «мастер s1b, реплика s1a (sync)» — 40-й именно её и проверяет.

- [ ] **Шаг 5: коммит**

```bash
git add dev-stand/checks/
git commit -m "t10: чеки 20-alerts (seeded + shard-no-master), 30-failover (stop->promote->rejoin), 40-live-probes"
```

---

### Task 7: README + полный e2e-прогон (quick и full, повторный прогон с чистого состояния)

**Files:**
- Create: `dev-stand/README.md`

- [ ] **Шаг 1: `dev-stand/README.md` (полный)**

```markdown
# dev-stand — локальный docker-стенд AdminPanel

Канон — `arch/04-local-stand.md`; спецификация —
`docs/superpowers/2026-08-23-t10-dev-stand/spec.md`.

## Быстрый старт

```bash
# терминал 1 — панель (localhost:5000, admin/admin)
dotnet run --project src/AdminPanel.Api

# терминал 2 — стенд
cd dev-stand && checks/00-up.sh        # full: etcd+seed+2 PG-шарда+эмуляторы
# или: docker compose up -d            # quick: только etcd+сид (без PG/проб)

open http://localhost:5000
```

Порт панели/логин переопределяются: `ADMINPANEL_URL`, `AdminPanel:Auth`.

## Профили

| Профиль | Состав | Для чего |
|---|---|---|
| quick (по умолчанию) | etcd + seed | цикл бэкенд-разработки: API/алерты на сиде; Patroni/SQL-пробы закономерно падают (нод нет) |
| full | + s1a/s1b, s2a/s2b, hc1a/hc1b, hc2a/hc2b | live-пробы, failover, e2e |

## E2E (полный прогон; с чистого состояния)

```bash
checks/90-down.sh -v        # если стенд уже поднимался
# панель: dotnet run --project src/AdminPanel.Api (отдельный терминал)
checks/00-up.sh && checks/10-smoke-api.sh && checks/20-alerts.sh \
  && checks/30-failover.sh && checks/40-live-probes.sh
```

Порядок важен: 30-й делает failover s1 (мастером остаётся s1b, s1a
rejoin'ится репликой) — 40-й рассчитан на эту топологию. Повторный
прогон — только с чистого состояния (`90-down.sh -v`).

Quick-режим: `checks/90-down.sh -v && docker compose up -d` → зелёные
`10-smoke-api.sh` и `20-alerts.sh` (quick-ветка); 30/40 требуют full.
После full-прогонов переход в quick — только с `-v` (lease-ключи
протухли, идемпотентный сид их не восстановит).

## Отладка

- контейнеры: `docker compose ps`, логи `docker compose logs <сервис>`;
  ноды — по имени сервиса (`s1a`…), контейнеры — `as-*` (не конфликтуют
  со стендом `../pg`);
- etcd: `docker compose exec etcd etcdctl --endpoints=http://localhost:2379 get / --prefix --keys-only`;
- эмуляторы: `curl 127.0.0.1:8011/cluster | jq .` (8011/8012/8021/8022);
- панель: логи запуска `/tmp/adminpanel.log` (если через nohup), API —
  `curl -b jar $BASE/api/overview`.
```

- [ ] **Шаг 2: полный e2e-прогон с чистого состояния**

```bash
cd "$WT"
nohup dotnet run --project src/AdminPanel.Api >/tmp/adminpanel.log 2>&1 &
echo $! >/tmp/adminpanel.pid
cd dev-stand
checks/90-down.sh -v
checks/00-up.sh && checks/10-smoke-api.sh && checks/20-alerts.sh \
  && checks/30-failover.sh && checks/40-live-probes.sh
```

Ожидание: 5 финальных `✓ …` подряд, общий код 0.

- [ ] **Шаг 3: повторный прогон (чистое состояние обязательно)**

```bash
checks/90-down.sh -v
checks/00-up.sh && checks/10-smoke-api.sh && checks/20-alerts.sh \
  && checks/30-failover.sh && checks/40-live-probes.sh
kill "$(cat /tmp/adminpanel.pid)"
```

Ожидание: снова все зелёные (доказательство повторной прогоняемости после failover-мутаций).

- [ ] **Шаг 4: quick-режим (только с чистого состояния: `-v` обязателен —
  после full-прогонов lease-ключи протухли, а идемпотентный сид их не
  восстановит; spec §13.12)**

```bash
checks/90-down.sh -v && docker compose up -d
checks/10-smoke-api.sh && checks/20-alerts.sh
checks/90-down.sh
```

Ожидание: оба чека зелёные (20-й печатает quick-ветку: «ключ возвращён статично»).

- [ ] **Шаг 5: коммит**

```bash
git add dev-stand/README.md
git commit -m "t10: README dev-stand + полный e2e-прогон зелёный (full x2 с чистого состояния, quick)"
```

---

### Task 8: roadmap-деливерабл — закрытие t10

**Files:**
- Modify: `arch/roadmap/infra.md:8` (убрать `← t10-dev-stand` у `t11-finalize`)
- Delete: `arch/roadmap/stand.md` (после удаления пункта остаётся пустым)
- Modify: `arch/roadmap/README.md` (убрать строку stand.md из таблицы треков)

- [ ] **Шаг 1: проверить ссылки на t10**

Run: `grep -rn "t10" arch/roadmap/`
Ожидание: `stand.md:7` (пункт t10), `infra.md:8` (`t11-finalize ← t10-dev-stand`), `README.md` — строка трека stand.md (без «t10»).

- [ ] **Шаг 2: применить правки**

```bash
# infra.md: снять зависимость t11 от t10 (t10 уходит в main этим же набором)
sed -i '' 's/`t11-finalize` ← `t10-dev-stand`/`t11-finalize`/' arch/roadmap/infra.md
# stand.md пуст после удаления пункта — файл и строку трека убираем
git rm arch/roadmap/stand.md
```

Затем в `arch/roadmap/README.md` удалить строку `| [stand.md](stand.md) | собственный dev-стенд и e2e, финализация проекта |` из таблицы треков.

- [ ] **Шаг 3: проверить**

Run: `grep -rn "t10\|stand.md" arch/roadmap/ || true`
Ожидание: пусто (ни t10, ни stand.md в roadmap).

- [ ] **Шаг 4: коммит**

```bash
git add arch/roadmap/
git commit -m "t10: roadmap — пункт t10-dev-stand закрыт (stand.md пуст и удалён, зависимость t11 снята)"
```

---

### Task 9: финальный контроль (без коммита)

- [ ] **Шаг 1: сборка и тесты решения**

Run: `cd "$WT" && dotnet build && dotnet test`
Ожидание: 0 ошибок/варнингов (TreatWarningsAsErrors), все тесты PASS (C# не менялся — регрессий быть не должно).

- [ ] **Шаг 2: статус ветки**

Run: `cd "$WT" && git status --short && git log --oneline -9`
Ожидание: рабочее дерево чистое; 8 коммитов `t10: …` (Tasks 1–8).

- [ ] **Шаг 3: сверка деливерабл по spec §11**

Run: `ls dev-stand dev-stand/checks dev-stand/sidecar dev-stand/seed`
Ожидание: compose/seed.sh/README + checks/{00,10,20,30,40,90}.sh + sidecar/{Dockerfile,emulator.py} + seed/Dockerfile; `test -x` на всех .sh.

---

## Сценарий отладки (что смотреть при падении)

| Симптом | Где смотреть |
|---|---|
| 00-up: «сид не появился» | `docker compose logs seed` (etcd ещё не готов → перезапустить seed: `docker compose run --rm seed`); `docker compose logs etcd` |
| 00-up: «не стала репликой» | `docker compose logs s1b`/`s2b` — цикл «жду …a» бесконечен, если hba-патч не применился: проверить `docker compose exec s1a cat $PGDATA/pg_hba.conf \| tail -2`; порт 2379/5433+ занят — `docker compose up` покажет конфликт |
| 00-up: «не зарегистрирован в /cluster/nodes» | `docker compose logs hc1a` (стартовая строка «эмулятор scope …»); эмулятор жив, но ключа нет — опечатка в MEMBERS/NODE_NAME: `docker compose exec hc1a env` |
| 10-smoke: панель не отвечает | `/tmp/adminpanel.log`; панель не в Development (нет admin/admin) — запускать `dotnet run` из каталога решения (launchSettings) |
| 10-smoke: алерты не видны | `docker compose exec etcd etcdctl --endpoints=http://localhost:2379 get /clusters/demo/buckets/status/bucket_11`; подождать 2 тика (3 c) |
| 20-alerts: master-ключ не гаснет | эмуляторы s2 живы (шаг «(full) стоп» пропущен — quick-детект сработал неверно): `docker compose ps` — hc2a/hc2b должны быть Exited |
| 30-failover: master-ключ не у s1b | `docker compose logs hc1b` — строка `role replica -> master`; s1b не промоутилась: `docker compose exec s1b psql -U postgres -tAc 'select pg_is_in_recovery()'` |
| 30-failover: rejoin висит | `docker compose logs s1a` (цикл basebackup); слот занят: `docker compose exec s1b psql -U postgres -tAc "select slot_name,active from pg_replication_slots"` |
| 40-live: probeError не null | HostMap не подхвачен (панель запущена до Task 5 — перезапустить); порт стенда занят: `curl 127.0.0.1:8011/cluster` |
| 40-live: bucketSchemas не та длина | инвентарь создавался на прежнем мастере (прогон без `90 -v`): перезапуск с чистого состояния |
| Всё сразу красное после паузы | lease TTL 5 c истёк у остановленных эмуляторов/нод — состояние стенда mutate'нуто: `90-down.sh -v && 00-up.sh` |

## Критерий идемпотентности сида (spec §4)

Повторный `docker compose run --rm seed` печатает `уже засеян — пропускаю` и
выходит 0; `mod_revision` статичного ключа `/clusters/demo/buckets/routing/bucket_0`
не меняется до/после (проверено в Task 2, шаг 7). Lease-ключи (master, members,
nodes) сидом не переписываются — ими владеют эмуляторы.
