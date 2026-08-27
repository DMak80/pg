# Спецификация t10-dev-stand — собственный docker dev-стенд AdminPanel + e2e-проверки

Дата: 2026-08-23. Фаза dev-flow: spec. Источники истины:
`arch/roadmap/stand.md` (пункт `t10-dev-stand` — объём),
`arch/04-local-stand.md` (ГЛАВНЫЙ документ задачи: профили §1, сервисы
§2, проверки §3, отличия от `../pg` §4, быстрый старт §5 — правки t10
внесены до написания spec, см. §15),
`arch/02-etcd-contract.md` §2 (ключи, которые пишет сид), §2.3
(`/cluster/nodes/`), §4 (тик 3 c / тик проб 15 c — тайминги ожиданий
чеков), §6 (live-пробы: Patroni REST `:8008/cluster`, SQL, HostMap),
`arch/03-panels.md` §1 (проверяемые эндпоинты), §4 (алерты, чей
появление/гашение проверяем), §5 (SQL-каталог пробы).
Референс-стенд `../pg/arch/stand/` (compose-паттерны PG-реплик,
`sidecar/rolecheck.py`, стиль `checks/*.sh` — копирование разрешено).
Фактическое состояние кода — t01–t09: панель полностью работает
(`SnapshotRefresher` 3 c, `ProbeOrchestrator` 15 c, `AlertEngine` 24
правила, API overview/etcd/clusters/ha/alerts, auth admin/admin в
`appsettings.Development.json`); `EtcdSeed` интеграционных тестов —
канон значений сида; `HostMapResolver` — точное совпадение `host:port`.

## 1. Цель

Self-contained docker-стенд в репозитории (`dev-stand/`, compose-проект
`adminpanel-stand`), поднимающий etcd + шардированную PG с контроль-плейном
и patroni-эмуляторами, плюс набор e2e-скриптов проверок против работающей
на хосте панели. Итог: полный e2e-прогон `00 → 10 → 20 → 30 → 40` зелёный
(панель показывает живые данные стенда, алерты возникают и гаснут,
failover-цикл воспроизводится).

Состав поставки:

1. `dev-stand/docker-compose.yml` — сервисы по arch/04 §1/§2 (профили
   quick/full, порты 2379/5433–5436/8011/8012/8021/8022).
2. `dev-stand/seed.sh` — идемпотентный сид контроль-плейна в etcd
   (контракт §5; значения = `EtcdSeed`, кроме динамических времён).
3. `dev-stand/sidecar/` (образ patroni-эмулятора `hc*`: python + pg8000,
   развитие `../pg/arch/stand/sidecar/rolecheck.py`).
4. `dev-stand/checks/00-up.sh`, `10-smoke-api.sh`, `20-alerts.sh`,
   `30-failover.sh`, `40-live-probes.sh`, `90-down.sh` — стиль
   `../pg/arch/stand/checks/` (bash + jq, `set -euo pipefail`,
   Arrange/Act/Assert-комментарии).
5. Правка `src/AdminPanel.Api/appsettings.Development.json` — добавить
   `AdminPanel:Probes:HostMap` стенда (+ явный `Password: ""`).
6. `dev-stand/README.md` — быстрый старт по arch/04 §5 (панель + стенд,
   e2e-прогон).

Кода панели (C#) задача не меняет: стенд проверяет существующее поведение
t01–t09 end-to-end. Единственная правка кода — appsettings.Development.json.

## 2. Принципы

- Источник истины — `arch/` (прежде всего 04-local-stand, правки t10 уже
  внесены); всё, что arch не оговаривает, решено минимальным способом и
  зафиксировано в §13 «Принятые решения».
- Стенд не зависит от репозитория `../pg`: файлы копируются/адаптируются
  в `dev-stand/` (arch/04 преамбула). Соседние проекты не монтируются.
- Идентификаторы (сервисы, ключи, kind-алертов) — английские; комментарии
  в скриптах — русские, по AAA где уместно (прецедент `../pg`-чеков).
- Скрипты детерминированы: все ожидания — ретраи с таймаутом ( паттерн
  `for i in $(seq 1 N)` из `../pg`), никаких sleep-«наугад»; каждый
  assert при провале печатает ❌ и `exit 1`; успех — `✓`-строка.
- Идемпотентность: сид не портит существующее состояние; e2e-прогон
  начинается с чистого состояния (`90-down.sh -v` → `00-up.sh`), чек 30
  возвращает стенд в консистентный вид (rejoin).
- Безопасность: trust-аутентификация PG только внутри стендовой сети;
  образы — только из зафиксированных репозиториев (postgres:18,
  quay.io/coreos/etcd:v3.5.21, python:3.12-alpine).

## 3. Состав стенда (compose `dev-stand/docker-compose.yml`)

`name: adminpanel-stand`; сеть `adminstand` (default-драйвер). Имена
**сервисов** — канонические (`etcd`, `seed`, `s1a`, `s1b`, `s2a`, `s2b`,
`hc1a`, `hc1b`, `hc2a`, `hc2b`) — на них резолвятся DSN/репликация внутри
сети и работают `docker compose exec/stop/rm` в чеках. `container_name` —
с префиксом `as-` (`as-etcd`, `as-s1a`, …) во избежание конфликта имён
контейнеров со стендом `../pg` (arch/04 §1).

| Сервис | Образ | Профиль | Публикация (host:container) | Назначение |
|---|---|---|---|---|
| `etcd` | `quay.io/coreos/etcd:v3.5.21` | — (всегда) | `2379:2379` | контроль-плейн; флаги как в `../pg` (`--listen-client-urls=http://0.0.0.0:2379`, `--advertise-client-urls=http://etcd:2379`); named volume `etcd-data` (переживает `down` без `-v`) |
| `seed` | `dev-stand/seed/Dockerfile`: `alpine:3.20` + `etcdctl` 3.5.21, скопированный `COPY --from=quay.io/coreos/etcd:v3.5.21 /usr/local/bin/etcdctl` (официальный образ distroless, без shell — скрипт не выполнить; решение §13.14) | — (всегда) | — | одноразовый: `sh /seed.sh`, `restart: "no"`, volume `./seed.sh:/seed.sh:ro`, env `ETCDCTL_ENDPOINTS=http://etcd:2379`, depends_on `etcd` |
| `s1a`, `s2a` | `postgres:18` | `full` | `5433:5432` (s1a), `5435:5432` (s2a) | мастера шард; **self-healing** (§7): при пустом `PGDATA` — клон от пира-мастера, иначе initdb-мастер; env `POSTGRES_HOST_AUTH_METHOD=trust`, `POSTGRES_DB=demo`; `wal_level=logical`, `max_wal_senders=16`, `max_replication_slots=16` |
| `s1b`, `s2b` | `postgres:18` | `full` | `5434:5432` (s1b), `5436:5432` (s2b) | физические реплики: `pg_basebackup -R -C -S <peer>_phys` от пира с retry (паттерн `../pg` s1b, дословно: ожидание мастера, `sed primary_conninfo` c `application_name`), `sync_replication_slots=on`, `hot_standby_feedback=on` |
| `hc1a`, `hc1b`, `hc2a`, `hc2b` | `dev-stand/sidecar` (build) | `full` | `8011:8008` (hc1a), `8012:8008` (hc1b), `8021:8008` (hc2a), `8022:8008` (hc2b) | patroni-эмуляторы (§7); отдельные контейнеры в общей сети; без restart-политики |

Топологические конвенции (фиксируются env эмуляторов и чеками):

- шард 1 = `s1a`+`s1b`, HA-scope `demo-s1`; шард 2 = `s2a`+`s2b`, scope
  `demo-s2`; кластер `demo`.
- Профиль `quick` = сервисы без поля `profiles` (`etcd`+`seed`);
  `docker compose up -d` поднимает только их, `--profile full` добавляет
  PG и эмуляторы (arch/04 §1).
- named volume только `etcd-data`; PGDATA живёт в контейнерах (без
  volume) — `docker compose rm -sf s1a && up -d s1a` даёт чистый rejoin.

## 4. Скрипт `seed.sh` — контракт

Запускается сервисом `seed` (и вручную `docker compose run --rm seed`).
Транспорт — `etcdctl` (образ уже содержит). Идемпотентность: если
`/clusters/demo/config` непуст — скрипт печатает «сид уже на месте» и
выходит 0, не трогая существующие ключи (после `down` без `-v` и после
того, как эмуляторы переписали `master`/`members` с lease, повторный
сид состояние не ломает).

Пишемые ключи — в точности `EtcdSeed` интеграционных тестов
(`src/tests/AdminPanel.IntegrationTests/EtcdSeed.cs`), кроме
`started_/updated_unix` статус-ключей и `created_unix` (динамические,
см. §13.2). Полная таблица (`now` = `date +%s` на момент запуска):

| Ключ | Значение |
|---|---|
| `/clusters/demo/config` | `{"buckets":16,"dbname":"demo","created_unix":<now>}` |
| `/clusters/demo/shards/s1/dsn` | `host=s1a,s1b port=5432 dbname=demo user=postgres` |
| `/clusters/demo/shards/s1/replicas` | `1` |
| `/clusters/demo/shards/s1/master` | `s1a:5432` (статично; в full эмулятор перепишет с lease) |
| `/clusters/demo/shards/s2/dsn` | `host=s2a,s2b port=5432 dbname=demo user=postgres` |
| `/clusters/demo/shards/s2/replicas` | `1` |
| `/clusters/demo/shards/s2/master` | `s2a:5432` (аналогично) |
| `/clusters/demo/buckets/routing/bucket_<N>` | s1: N ∈ {0,2,3,4,6,8,10,11,12,14}; s2: N ∈ {1,5,7,9,13,15} (итого 10/6 — раскладка `EtcdSeed`) |
| `/clusters/demo/buckets/status/bucket_3` | `{"bucket":"bucket_3","state":"SYNCING","owner":"s1","target":"s2","started_unix":<now-120>,"updated_unix":<now-60>,"phase":"copy"}` — свежий, `move-stale` НЕ горит |
| `/clusters/demo/buckets/status/bucket_7` | `{"bucket":"bucket_7","state":"ABORTING","owner":"s2","target":"s1","started_unix":<now-1000>,"updated_unix":<now-900>,"phase":"cleanup","last_error":"receiver went away"}` — горит `move-aborting` + `move-stale` |
| `/clusters/demo/buckets/status/bucket_11` | `{"bucket":"bucket_11","state":"FROZEN","owner":"s1","target":"s2","started_unix":<now-7400>,"updated_unix":<now-7200>,"phase":"cutover-wait"}` — протухший, горит `move-stale` (порог 600 c) |
| `/clusters/demo/heals/bucket_5` | `{"bucket":"bucket_5","was":"s2","now":"s1","reason":"restore-heal","ts":<now-86400>}` |
| `/service/demo-s1/leader` | `{"name":"s1a"}` (статично; в full мастер перепишет с lease) |
| `/service/demo-s1/members/s1a` | `{"name":"s1a","conn_url":"s1a:5432","role":"master","state":"running","timeline":1,"lag":0}` |
| `/service/demo-s1/members/s1b` | `{"name":"s1b","conn_url":"s1b:5432","role":"replica","state":"streaming","timeline":1,"lag":0}` |
| `/service/demo-s1/optime/leader` | `738273634528` |
| `/service/demo-s1/initialize` | `738273612345678` |
| `/service/demo-s1/config` | `{"ttl":5,"loop_wait":2,"retry_timeout":3}` |
| `/service/demo-s2/*` | зеркально (leader `s2a`, members s2a/s2b, optime `738273634001`, initialize `738273611234567`, тот же config) |
| `/cluster/nodes/s1a\|s1b\|s2a\|s2b` | в quick — фиктивные отображаемые адреса `172.28.0.11/12/21/22` (как `EtcdSeed`); в full эмуляторы перепишут реальными IP с lease |

Скрипт в конце самопроверяется: `etcdctl get /clusters/demo/config` и
`.../routing/bucket_0` непусты — иначе `exit 1`.

## 5. Архитектура patroni-эмулятора (`dev-stand/sidecar/`)

`Dockerfile`: `python:3.12-alpine` + `pip install pg8000` (минимальная
зависимость, та же связка, что в `../pg`-сайдкаре). Скрипт `emulator.py` —
развитие `rolecheck.py`: HTTP-сервер (`ThreadingHTTPServer`, порт 8008,
`0.0.0.0`) + фоновый цикл регистрации (поток, раз в 1 с).

Env-контракт контейнера `hc*` (задаётся compose):

| Env | Пример (hc1a) | Смысл |
|---|---|---|
| `NODE_NAME` | `s1a` | имя ноды (= member-ключ, = DSN-хост, = адрес её PG в compose-DNS) |
| `ETCD_ENDPOINTS` | `http://etcd:2379` | HTTP-gateway `/v3/*` (как rolecheck) |
| `CLUSTER` | `demo` | кластер контроль-плейна |
| `SHARD` | `s1` | шард → ключ `…/shards/s1/master`, scope `demo-s1` |
| `MEMBERS` | `s1a,s1b` | состав членов scope: по этим именам опрашиваются PG обеих нод (§13.13) |

### 5.1. Опрос PG (источник истины о роли)

Раз в 1 с: `SELECT pg_is_in_recovery()` (+ при успехе —
`pg_last_wal_receive_lsn()/pg_last_wal_replay_lsn()` для лага и
`pg_control_checkpoint()->timeline` для timeline; PG18 отдаёт;
фолбэк при ошибке поля — `timeline=1`, `lag=0`). Последняя известная
роль запоминается. PG недоступна → нода «мёртва» (см. 5.3/5.4).

### 5.2. REST `:8008` (подмножество Patroni)

| GET | Ответ |
|---|---|
| `/cluster` | **всегда 200**, пока жив контейнер: `{"members":[{name,role,state,timeline,lag,host,port},…]}` по каждому имени из `MEMBERS`: своя нода — из свежего опроса (§5.1); пир — опрос его PG напрямую тем же запросом (кэш цикла, 1 с); недоступная нода — `state:"stopped"`, последняя известная роль, `lag:null`. `host` = имя ноды, `port` = 5432. Панель ищет запись по `name` (PatroniRestProbe) — состав обязан быть полным |
| `/primary` | 200 только если своя PG жива и не в recovery; иначе 503 |
| `/replica` | 200 только если своя PG жива и в recovery; иначе 503 |
| `/`, `/read-only` | 200 если своя PG отвечает (как rolecheck) |

### 5.3. Цикл регистрации в etcd (раз в 1 с, поток)

Основа — паттерн `register_loop` rolecheck (lease grant TTL 5 с →
put с lease → keepalive; lease-ID десятичной строкой — урок `../pg`).
Свою PG опрашивает `NODE_NAME`-эмулятор; запись ведётся **только пока
своя PG отвечает** — смерть ноды убирает ключи через TTL ≤ 5 c:

| Ключ | Когда пишет | Значение |
|---|---|---|
| `/service/<CLUSTER>-<SHARD>/members/<NODE_NAME>` | PG жива | `{"name","conn_url":"<node>:5432","role":"master"\|"replica","state":"running"\|"streaming","timeline":<t>,"lag":<l>}` |
| `/cluster/nodes/<NODE_NAME>` | PG жива | IP своей PG-ноды (DNS-resolve `NODE_NAME` — эмулятор живёт в отдельном контейнере, сокет-приём `node_ip()` из rolecheck дал бы адрес эмулятора) |
| `/clusters/<CLUSTER>/shards/<SHARD>/master` | PG жива **и не в recovery** (мастер) | `<node>:5432` |
| `/service/<CLUSTER>-<SHARD>/leader` | там же | `{"name":"<node>"}` |
| `/service/<CLUSTER>-<SHARD>/optime/leader` | там же | `pg_current_wal_lsn()` числом-строкой |

После promote реплики её эмулятор сам видит «не в recovery» и берёт
набор мастер-ключей — внешние скрипты ничего в etcd не пишут.
Конкуренция двух живых мастеров (split-brain ≤ TTL) — последний put
выигрывает; для стенда допустимо (§14).

### 5.4. Отказоустойчивость эмулятора

etcd недоступен → пересоздать lease (как rolecheck), продолжить цикл;
любая оока опроса — просто «мёртвая нода» в этом тике. Логи — одна
строка на смену состояния роли (`master → replica` и т.п.), не на тик.

## 6. Инвентарь PG (создаёт `00-up.sh`, full)

На каждом мастере (первоначально s1a/s2a; реплики получают basebackup'ом;
после rejoin — сам-healing клон):

- БД `demo` (задаётся `POSTGRES_DB=demo` у *a-нод);
- схемы `bucket_%` — по одному на **ACTIVE**-бакет владельца: s1 —
  `{bucket_0,2,4,6,8,10,12,14}` (8 шт.; 3 и 11 в статусе переезда —
  исключены), s2 — `{bucket_1,5,9,13,15}` (5 шт.; 7 — ABORTING).
  Ровно этот состав не зажигает `inventory-mismatch` (правило сверяет
  только ACTIVE) — и даёт 40-му чеку «позитивную» картину (§9.5);
- `pg_hba`: строка `host replication all all trust` (добавляет 00-up на
  всех 4 нодах — паттерн `../pg`, нужен для basebackup/rejoin);
- `synchronous_standby_names = 'FIRST 1 (<replica>)'` — ALTER SYSTEM на
  действующих мастерах (НЕ флагом `-c` — флаг сильнее и после promote
  без реплики повесит коммиты в SyncRep; урок `../pg` 00-up/30-failover).

## 7. Скрипты проверок (`dev-stand/checks/`)

Общие конвенции: `#!/usr/bin/env bash`, `set -euo pipefail`,
`cd "$(dirname "$0")/.."`; работа с контейнерами — `docker compose`
по **имени сервиса**; панель — `BASE="${ADMINPANEL_URL:-http://localhost:5000}"`,
логин `admin/admin` (cookie-jar `mktemp`); etcd — `docker compose exec etcd
etcdctl`; ретраи `for i in $(seq 1 N)` с шагом 1 c; финал — `✓`/`❌`.

### 7.1. `00-up.sh` — подъём (full)

Arrange: `docker compose --profile full up -d --build`.
Assert/Arrange-цепочка (каждый шаг — ретрай ≤ 60 c, иначе ❌):

1. etcd жив: `etcdctl endpoint health`;
2. сид на месте: `get /clusters/demo/config` непуст (ждать exited-контейнер
   не нужно — ждём ключ);
3. `pg_isready` на s1a/s2a → patch_hba на всех 4 нодах → `pg_isready`
   на s1b/s2b → `pg_is_in_recovery()=t` на репликах;
4. эмуляторы зарегистрировались: `/cluster/nodes/s1a…s2b` непусты,
   `/clusters/demo/shards/s1/master` (с lease) есть;
5. sync-standby: ALTER SYSTEM на s1a/s2a (§6), ждать
   `pg_stat_replication.sync_state='sync'` у обоих;
6. инвентарь: `CREATE SCHEMA bucket_<N>` по спискам §6 (psql на
   s1a/s2a в БД demo; idempotent — `IF NOT EXISTS`).

### 7.2. `10-smoke-api.sh` — дым API (панель поднята заранее)

Arrange: ретрай `GET $BASE/api/healthz` ≤ 60 c (таймаут → ❌ с
подсказкой «запусти dotnet run --project src/AdminPanel.Api»); логин.
Act/Assert (jq):

1. без cookie `GET /api/overview` → 401; login → 204 + cookie;
2. `/api/healthz` → `{"status":"ok"}`;
3. `/api/overview` → `etcd.reachable=true`, `clusters[0].buckets=16`,
   `alertsCritical≥0` (поле присутствует);
4. `/api/etcd/status` → `endpoints[0].reachable=true`, `version`
   непуста (v3.5.21);
5. `/api/clusters/demo` → `shards[0].masterAddress="s1a:5432"`,
   `buckets` длина 16, `heals[0].bucket="bucket_5"`;
6. `/api/ha/demo-s1` → `leaderName="s1a"`, `members` длина 2,
   `members[?name=='s1a'].role=="master"`;
7. `/api/alerts` → есть `kind=="move-stale" && target=="demo/bucket_11"`
   (допустимы и другие сид-алерты: `move-stale/bucket_7`, `move-aborting`).

Тайминги: панельный тик 3 c — все ожидания ≤ 15 c.

### 7.3. `20-alerts.sh` — seeded-аномалии и shard-no-master

1. Assert: `/api/alerts` содержит `move-stale → demo/bucket_11` и
   `move-aborting → demo/bucket_7` (≤ 15 c);
2. Act: `shard-no-master` (critical): определить full (`docker compose ps
   --services --filter status=running | grep -q hc2a`):
   - full: `docker compose stop hc2a hc2b` (иначе keepalive перепишет),
     `etcdctl del /clusters/demo/shards/s2/master`;
   - quick: просто `del`;
3. Assert: ≤ 15 c в `/api/alerts` есть `shard-no-master → demo/s2` с
   `severity=critical` (2 тика панели);
4. Act (восстановление): full — `docker compose start hc2a hc2b` (эмулятор
   s2a перепишет master с lease ≤ 3 c); quick — `etcdctl put …/master
   "s2a:5432"`;
5. Assert: ≤ 15 c алерт `shard-no-master → demo/s2` исчез (в full —
   master-ключ снова есть).

### 7.4. `30-failover.sh` — цикл алерт→успокоение (только full)

1. Act: `docker compose stop -t 3 s1a` (мастер s1);
2. Assert (etcd, ≤ 10 c): `/clusters/demo/shards/s1/master` исчез,
   `/service/demo-s1/leader` исчез (обе под lease);
3. Assert (панель, ≤ 15 c): `shard-no-master → demo/s1` (critical) +
   `shard-no-leader → demo-s1` (warning); `/service/demo-s1/optime/leader`
   тоже гаснет;
4. Act: promote s1b — `docker compose exec -u postgres s1b pg_ctl promote
   -D <data_directory>` (путь — `show data_directory`, паттерн `../pg`);
   ждать `pg_is_in_recovery()=f`; снять sync-имена
   (`ALTER SYSTEM SET synchronous_standby_names=''`, паттерн `../pg`);
5. Assert: ≤ 10 c master-ключ = `s1b:5432`, leader = `{"name":"s1b"}`;
   ≤ 15 c оба алерта из шага 3 погасли; REST напрямую: 
   `curl 127.0.0.1:8012/primary` → 200, `curl 127.0.0.1:8011/cluster` →
   s1b `role=master`, s1a `state=stopped`; панель: `/api/ha/demo-s1`
   member s1b `role=master` (etcd-член переписан эмулятором);
6. Act (rejoin, возврат стенда в консистентный вид):
   `docker compose rm -sf s1a` → drop осиротевшего слота `s1a_phys` на
   s1b (`pg_drop_replication_slot`, ignore-error) →
   `docker compose up -d s1a` (self-healing: пустой PGDATA → клон от
   мастера s1b) → ждать `pg_is_in_recovery()=t` на s1a → sync-имена
   на s1b `FIRST 1 (s1a)` → ждать `sync_state=sync`.
7. Assert: `/api/ha/demo-s1` — 2 члена, оба без `probeError`; SQL-проба
   s1 жива (см. 40).

Допустимый остаточный шум: `ha-member-not-streaming → demo-s1/s1a` в
окне между stop и пропаданием member-ключа (TTL) — не ассертится.

### 7.5. `40-live-probes.sh` — live-пробы панели (HostMap, full)

Топология после 30: мастер s1b, реплика s1a (sync); шард 2 нетронут.
Assert (панель, ожидание ≤ 40 c — тик проб 15 c + запас):

1. `/api/ha/demo-s1` и `/api/ha/demo-s2`: у каждого member
   `probeAtUtc != null`, `probeError == null`; у реплик `state=="streaming"`
   и `lagBytes` — число; у мастеров `state=="running"`, `timeline >= 1`
   (доказательство: Patroni-пробы реально прошли через HostMap на
   `127.0.0.1:8011/8012/8021/8022`);
2. `/api/clusters/demo`: `shards[].runtime != null`, `runtime.error ==
   null`, `runtime.standbiesSync >= 1` у обоих (SQL-проба через
   `TargetSessionAttributes=ReadWrite` выбрала мастера из multi-host и
   видит sync-standby в `pg_stat_replication`),
   `runtime.bucketSchemas` длины 8 (s1) и 5 (s2) — инвентарь сверен;
3. `shards[0].masterAddress == "s1b:5432"` (после failover),
   `masterLeaseAlive == true` у обоих шардов;
4. `/api/alerts`: нет `kind=="probe-failed"`; нет
   `kind=="inventory-mismatch"`; нет `kind=="shard-no-master"`.

### 7.6. `90-down.sh` — разбор

`docker compose --profile full down --remove-orphans` (+ `--volumes`
при аргументе `-v` — стирает и `etcd-data`).

## 8. Конфигурация панели для стенда

`src/AdminPanel.Api/appsettings.Development.json` — дополнить секцию
`AdminPanel` (Endpoints/auth уже есть):

```json
"Probes": {
  "Password": "",
  "HostMap": {
    "s1a__5432": "127.0.0.1:5433",
    "s1b__5432": "127.0.0.1:5434",
    "s2a__5432": "127.0.0.1:5435",
    "s2b__5432": "127.0.0.1:5436",
    "s1a__8008": "127.0.0.1:8011",
    "s1b__8008": "127.0.0.1:8012",
    "s2a__8008": "127.0.0.1:8021",
    "s2b__8008": "127.0.0.1:8022"
  }
}
```

(значения — arch/04 §2.3 дословно; ключ в appsettings записывается как
`host__port` — ':' в ключах режется конфиг-провайдерами .NET, канонический
`host:port` действует в памяти/тестах и приоритетен при наличии обоих;
`appsettings.json` не трогаем —
прод-профиль без HostMap). Запуск панели:
`dotnet run --project src/AdminPanel.Api` (launchSettings `http` →
`http://localhost:5000`, Development-окружение). Скрипты чеков
переопределяются `ADMINPANEL_URL`.

## 9. E2E-сценарий (критерий задачи)

0. (опционально) `dev-stand/checks/90-down.sh -v` — чистое состояние;
1. терминал 1: `dotnet run --project src/AdminPanel.Api`;
2. терминал 2: `checks/00-up.sh` → ✓ стенд поднят;
3. `checks/10-smoke-api.sh` → ✓ API отвечает, сид виден;
4. `checks/20-alerts.sh` → ✓ seeded-аномалии + cycle удаления
   master-ключа s2;
5. `checks/30-failover.sh` → ✓ failover-цикл + rejoin;
6. `checks/40-live-probes.sh` → ✓ все live-пробы обогащены, инвентарь
   сверен, алертов-ошибок нет;
7. (опционально) `checks/90-down.sh`.

Зелёный прогон = каждый скрипт завершился 0 с выводом `✓`. Повторный
прогон — с шага 0 (после 30-го мастер s1 — s1b; 00-up самодостаточен
только на чистом состоянии — `90 -v` обязателен).

## 10. Ограничения (не делаем)

- **Реальный Patroni/etcd-кластер**: Patroni — эмулятор (§5), etcd —
  одиночный (панель только читает; arch/04 §2.1).
- **PG-реплики — делаем по-настоящему** (физическая репликация,
  pg_basebackup): roadmap говорит «реплики», SQL-пробе нужны живые
  `pg_stat_replication`/`pg_is_in_recovery` — упрощение тут съело бы
  половину ценности стенда. Не делаем: логические переезды бакетов
  (подписки/слоты переездов) — это стенд `../pg`, не панели.
- **Клиентский роутинг**: без HAProxy/hasync — панель ходит по multi-host
  DSN напрямую (arch/04 §4).
- **opsbox**: роль ops выполняют host-скрипты + `docker compose exec`
  (arch/04 §4).
- **CI**: чеки не в CI (интеграционные тесты уже на Testcontainers;
  arch/04 §3).
- **Полный REST Patroni**: только `/cluster`,`/primary`,`/replica`,
  `/`,`/read-only`; выборы лидера не эмулируются (мастер = факт
  `pg_is_in_recovery()`), `PATCH /config` и прочее — нет.
- **Фронтенд**: e2e проверяет только API; UI смотрим руками (браузер).
- **Сиды чужих кластеров**: один кластер `demo` (как arch/04 §2.2).

## 11. Критерии приёмки

1. `dev-stand/` содержит compose, seed.sh, sidecar/{Dockerfile,emulator.py},
   checks/{00,10,20,30,40,90}.sh, README; всё исполняемое — `+x`.
2. `docker compose up -d` (quick) поднимает etcd+seed; сид идемпотентен
   (повторный `run --rm seed` не меняет ключи и не падает).
3. Полный e2e §9 зелёный на чистом состоянии; вывод каждого шага
   содержит его asserts.
4. 30-failover после завершения оставляет стенд консистентным (40
   проходит следом без ручных действий).
5. `appsettings.Development.json` содержит HostMap из §8;
   `appsettings.json` не изменён; `dotnet build` и все тесты решения
   зелёные (C#-код не менялся, кроме json).
6. Ни один файл вне `dev-stand/`, `appsettings.Development.json` и
   `docs/superpowers/2026-08-23-t10-dev-stand/` не изменён (arch/04
   правится в рамках этой же ветки — правки уже внесены, §15).

## 12. Тестирование задачи

Стенд сам и есть «тест»; отдельных xunit-тестов задача не добавляет
(кода приложения почти нет). Проверки исполнения: ручной прогон §9
дважды (чистый и повторный после `90 -v`), плюс quick-режим:
`docker compose up -d` → сид → `10-smoke-api.sh` зелёный (шаги 20/30/40
требуют full — в README указано; 20-й совместим с quick по §7.3).

## 13. Принятые решения (апрув выдан заранее, вопросы не задавались)

1. **routing 10/6, не «8/8»**: раскладка `EtcdSeed` (s1 — 10, s2 — 6) —
   канон, зафиксированный t04 и интеграционными тестами; arch/04 §2.2
   исправлен (было «поровну round-robin 8/8» — противоречило коду).
2. **Времена статус-ключей — динамические** (`now−60/−900/−7200`), не
   фикс-числа `EtcdSeed` (те протухли бы относительно «сегодня»):
   bucket_3 обязан быть свежим, чтобы `move-stale` горел только на 7/11.
   Структура значений и owner/target — идентичны `EtcdSeed`.
3. **Эмулятор — python:3.12-alpine + pg8000** (не busybox-httpd/dotnet):
   развитие проверенного `rolecheck.py` из `../pg` — та же HTTP-основа,
   тот же etcd-gateway-паттерн; нулевые новые стеки.
4. **`/cluster` всегда 200 и содержит всех members** (мёртвые —
   `state:"stopped"`): PatroniRestProbe ищет запись по имени — пустой
   или 503-ответ был бы ошибкой пробы, а не «нок-состоянием».
5. **`leader`/`optime/leader` тоже под lease мастера**: смерть мастера
   даёт связку `shard-no-master` + `shard-no-leader`, promote — гашение
   обоих; arch/04 §2.3 дополнен.
6. **Self-healing `s*a`-нод** (клон от пира при пустом PGDATA; паттерн
   s2a из `../pg`): делает 30-й чек обратимым и повторный e2e — чистым.
7. **Инвентарь — только ACTIVE-бакеты владельца (8+5)**: так сверяет
   `inventory-mismatch`; полный набор 16 схем на шарде зажигал бы
   warning. arch/04 §3 («inventory 16/16») исправлен.
8. **`container_name` с префиксом `as-`** при канонических именах
   сервисов: сосуществование со стендом `../pg` (у него те же имена
   контейнеров, но без публикации портов — портового конфликта нет).
9. **Панель запускается руками до чеков** (arch/04 §5 «два терминала»);
   скрипты не стартуют `dotnet` сами — только ждут healthz с внятной
   подсказкой; базовый URL — `ADMINPANEL_URL` (дефолт :5000).
10. **`20-alerts` в full останавливает `hc2a/hc2b`** перед `del`
    master-ключа (keepalive-цикл 1 c переписал бы ключ мгновенно) и
    возвращает их в конце — шаг становится циклом «алерт→гашение».
11. **e2e = full-профиль**; quick — dev-цикл, где Patroni/SQL-пробы
    закономерно падают (нод нет) — это отражает реальность и не
    ассертится; в README quick описан без обещаний проб.
12. **Повторный прогон — только с чистого состояния** (`90 -v` → `00`):
    после 30-го мастер s1 — s1b; self-healing сохраняет корректность
    стенда, но asserts 00/40 завязаны на исходную топологию. То же
    касается перехода full → quick: идемпотентный сид не восстанавливает
    lease-ключи (master/members/nodes), протухшие после остановки
    эмуляторов, — quick-прогон после full только через `90 -v`.
13. **Env эмулятора без `PGHOST`**: адрес каждой опрашиваемой PG = имя
    члена из `MEMBERS` (compose-DNS сети стенда; собственная нода —
    `NODE_NAME` ∈ `MEMBERS`); отдельная переменная была бы дублём и
    рассинхроном с `/cluster`-опросом соседа. Исключена из compose и
    env-контракта §5.
14. **Seed-образ — alpine + скопированный etcdctl**: официальный
    `quay.io/coreos/etcd:v3.5.21` — distroless (без shell: `sh` нет,
    проверено `docker run`), выполнить в нём `seed.sh` нельзя. Образ
    `dev-stand/seed/Dockerfile`: `alpine:3.20` +
    `COPY --from=quay.io/coreos/etcd:v3.5.21 /usr/local/bin/etcdctl` —
    версия etcdctl та же (3.5.21), строка запуска из arch/04 §2.2
    («контейнер с etcdctl») сохранена.

## 14. Риски

| Риск | Митигция |
|---|---|
| Port 2379/5433+ занят на хосте (локальный etcd, другой стенд) | 00-up сразу проверяет `etcdctl endpoint health` и печатает конфликт портов docker'а; порты зафикрированы arch/04 — смена только через arch |
| Flaky: тик панели 3 c / тик проб 15 c против TTL 5 c | все ожидания чеков — ретраи с запасом (≤15 c API, ≤40 c проб); lease-гашение ассертится в etcd до панели |
| SyncRep-ловушка после promote (коммиты висят без реплики) | 30-й чек снимает `synchronous_standby_names` сразу после promote, ставит заново после rejoin (дословный урок `../pg` 30-failover) |
| `pg_basebackup` rejoin падает из-за осиротевшего слота | 30-й дропает `s1a_phys` перед `up -d s1a`; `-C -S` пересоздаёт |
| Гонка старта *a-нод (оба initdb-мастера) | s*b имеют `depends_on: [s*a]`; self-healing *a проверяет пира только при НЕпустом peer'е и готовности — таймаут 15 c с фолбэком initdb только на первом старте (паттерн s2a `../pg`) |
| etcd-gateway lease-ID hex/dec | десятичная строка в JSON — урок rolecheck, копируем |
| jq/curl отсутствуют на хосте | 00-up проверяет наличие `jq`, `curl`, `docker` в начале с ❌-подсказкой |
| Публичный образ python/pg8000 недоступен | зафиксировано один раз build'ом; повторные прогоны используют кэш |

## 15. Связанные arch-правки (внесены в этой же ветке до spec)

`arch/04-local-stand.md`: §1 — примечание о `container_name`-префиксе
`as-`; §2.2 — seed-образ как Dockerfile (alpine + etcdctl 3.5.21 из
официального distroless-образа), master-ключи в сиде статично +
lease-перезапись в full, routing 10/6 (было «8/8»), динамические
времена статусов, лидер/optime под lease; §2.3 — механика эмулятора (MEMBERS, `/cluster` 200/stopped,
leader+optime мастером, members-lease только при живой PG),
self-healing `s*a`, инвентарь ACTIVE-only 8+5; §3 — уточнения строк
00/20/30/40 (alter system sync, stop эмуляторов в 20, rejoin в 30,
инвентарь 8+5 в 40), порядок e2e-прогона. Остальные arch-файлы не
тронуты (контракт 02 уже описывал всё нужное).
