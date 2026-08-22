# Исследование проекта pg (шардированная HA БД на PostgreSQL)

Источник: Explore-агент, 2026-08-22. Это система, которую AdminPanel инспектирует.

## 1. Что за проект pg

Инфраструктурный проект: воспроизводимый рецепт (документация + bash-скрипты + docker-compose + python-сайдкары) развёртывания **HA-кластеров PostgreSQL с бакетным шардированием поверх нескольких таких кластеров**.

- **Стек** (`/Users/demakaev/ZCodeProject/pg/README.md`): PostgreSQL 16 (бакетный слой — PG 18.4, минимум 15) + **Patroni 4.x** + **Spilo 3.3** (образ Zalando = PG+Patroni, `ghcr.io/zalando/spilo-16:3.3-p3`) + **etcd 3.5.21** (DCS и контрол-плейн шардинга) + **HAProxy 2.8** + **pg_doorman** (пулер `:6432`).
- **Понятия**:
  - **Шард** = отдельный HA-кластер (Spilo+Patroni+etcd-DCS+HAProxy): 1 primary (RW) + 1–2 реплики (RO), streaming replication.
  - **Кластер (система) `<C>`** = шардированная БД: одна БД (имя = имя кластера), бакеты — схемы в ней.
  - **Бакет** = схема `bucket_<id>` — «виртуальный шард» поверх физического.
  - Один etcd обслуживает несколько независимых кластеров (префиксы `/clusters/<C>/`).

## 2. Схема ключей etcd (v3 API, значения — плоские строки/JSON)

Чтение: `etcdctl get <key> -w json` + base64 (`arch/scripts/buckets-common.sh`: `etcd_value`, `etcd_prefix_keys`). Python-сайдкары используют HTTP-gateway etcd (`/v3/kv/range`, `/v3/kv/put`, `/v3/lease/grant`, `/v3/lease/keepalive`; lease — десятичная строка).

### 2.1. Контрол-плейн бакетов — префикс `/clusters/<C>/`

| Ключ | Значение | Кто пишет |
|---|---|---|
| `/clusters/<C>/config` | JSON: `{"buckets":256,"dbname":"<C>","created_unix":...}` | `init-cluster.sh` (один раз) |
| `/clusters/<C>/shards/X/dsn` | libpq-строка: `host=n1,n2,n3 port=5432 dbname=<C> user=bucket_admin` (multi-host, БЕЗ пароля) | init/add-shard |
| `/clusters/<C>/shards/X/replicas` | целое число (декларативное) | init/add-shard |
| `/clusters/<C>/shards/X/master` | `"host:6432"` (адрес мастер-ноды) — **lease TTL 5с, продление раз в 1–2с** | Patroni-callback `on_role_change` (единственный писатель); смерть ноды гасит ключ сама |
| `/clusters/<C>/buckets/routing/bucket_N` | имя шарда-владельца, напр. `"shard1"` — единственный авторитет «где бакет» | init/move-bucket (flip — атомарная txn) |
| `/clusters/<C>/buckets/status/bucket_N` | JSON, **только при переезде; нет ключа = ACTIVE** | move-bucket / abort-move |
| `/clusters/<C>/heals/<bucket>` | журнал авто-починки: `{"bucket":...,"was":...,"now":...,"reason":"restore-heal","ts":...}` | restore-cluster.sh heal |

Структура status-ключа (`arch/scripts/move-bucket.sh`, `status_put()`):
```json
{"bucket":"bucket_42","state":"SYNCING|FROZEN|ABORTING","owner":"shard1","target":"shard2",
 "started_unix":...,"updated_unix":...,"phase":"..."}
```
Состояния: нет ключа = ACTIVE; SYNCING — фоновая репликация; FROZEN — cutover (запись отклоняется, секунды); ABORTING — уборка прерванного переезда (`phase`, `last_error`).

### 2.2. Patroni DCS — префикс `/service/<scope>/`

Scope шарда = **`<C>-<X>`** (напр. `shop-shard1`), глобально уникален. Ключи:
- `/service/<scope>/leader` — имя лидера, TTL-lease (`ttl:5, loop_wait:2, retry_timeout:3` в `SPILO_CONFIGURATION`, `arch/configs/postgres/pg.env`);
- `/service/<scope>/config` — конфиг кластера;
- `/service/<scope>/members/<name>` — conn_url и role каждой ноды;
- `/service/<scope>/initialize`, `/service/<scope>/optime/leader` — позиция репликации лидера.

### 2.3. Стендовый топологический реестр

`/cluster/nodes/<node>` → IP ноды с lease TTL 5с (`arch/stand/sidecar/rolecheck.py`) — стендовая инкарнация мастер-ключа.

## 3. Бакеты и маппинг

- Бакет = **схема** `bucket_0..bucket_N-1` в единой БД кластера (имя БД = имя кластера). `bucket_id = hash(tenant_id) % N`; N — константа.
- Распределение: `init-cluster.sh` создаёт все N схем сразу, поровну round-robin по шардам (`bucket_shard() { printf '%s' "${SHARD_NAMES[$(( $1 % S_COUNT ))]}"; }`).
- Маппинг в etcd: `/clusters/<C>/buckets/routing/bucket_N` → имя шарда. Физическая истина — сами схемы `bucket_%` на шардах (`restore-cluster.sh verify` сверяет).
- Онлайн-переезд — логическая репликация PG≥15 (PUBLICATION FOR TABLES IN SCHEMA + SUBSCRIPTION с copy_data/failover), runbook в `arch/11-bucket-sharding.md` §5, автоматизирован `move-bucket.sh`. Cutover: FROZEN → REVOKE+LOCK → лаг 0 → сверка → атомарный etcd-flip.
- **Паролей в etcd НЕТ** — DSN без `password=`; пароли в env (`buckets.env`).

## 4. HA-кластеры и состояние реплик

- Репликация: физическая streaming replication (WAL :5432), управляет Patroni (в Spilo). Failover/switchover через DCS-выборы, кворум etcd.
- Источники состояния реплик:
  1. **Patroni REST API `:8008`**: `GET /cluster` → JSON: `{"name":"pg2","host":"10.0.0.12","port":5432,"role":"replica","state":"streaming","timeline":1,"lag":0}`; эндпоинты `/primary`, `/replica`, `/read-only` (200/503). Использует `cluster-state.sh` (NODE/ROLE/STATE/LAG_MB) и HAProxy health-check.
  2. **etcd DCS** `/service/<scope>/leader` (кто мастер) — самый надёжный (`find-leader.sh`).
  3. Контрол-плейн `/clusters/<C>/shards/X/master` (lease) — для роутера приложений.
  4. **SQL**: `pg_stat_replication` на мастере (`sync_state IN ('sync','quorum')`), `pg_is_in_recovery()`.
- `shards/X/replicas` — лишь декларативное намерение; фактическое число реплик живёт в Patroni-кластере шарда.

## 5. Порты и строки подключения

| Порт | Что |
|---|---|
| 2379 / 2380 | etcd client API / peer |
| 5432 | PostgreSQL (+ HAProxy write-эндпоинт шарда) |
| 5433 | HAProxy read-эндпоинт (базовая топология) |
| 8008 | Patroni REST API |
| 6432 | pg_doorman (клиентский вход на мастер-ноде шарда) |
| 7000 | HAProxy stats UI (+ /metrics Prometheus) |

DSN роутера: host из `.../shards/X/master` (`host:6432`), dbname из `/clusters/<C>/config` → `host=<ip> port=6432 dbname=<C> user=app sslmode=require`. Шардовый DSN в etcd — multi-host keyword-строка; пароль добавляется из env.

## 6. Локальный docker-стенд

Готовый стенд: `/Users/demakaev/ZCodeProject/pg/arch/stand/docker-compose.yml` (проект `pgstand`, сеть `pgstand_pgnet` 172.28.0.0/24):
- 2 «шарда»: `s1a`+`s1b`, `s2a`+`s2b` (postgres:18, мастер+реплика, `wal_level=logical`, trust-аутентификация);
- сайдкары `hc1a/hc1b/hc2a/hc2b` — эмуляция Patroni REST `/primary` + регистрация IP ноды в etcd `/cluster/nodes/<node>` с lease (`stand/sidecar/`);
- `hap1`/`hap2` HAProxy :5432 → мастер; `hasync1`/`hasync2` — синкеры etcd→HAProxy;
- `etcd` (quay.io/coreos/etcd:v3.5.21, одиночный);
- `opsbox` (профиль `ops`: psql+pg_dump+etcdctl+jq+curl).

Запуск: `cd arch/stand && checks/00-up.sh`. Конфиг: `arch/stand/buckets.stand.env` (кластер `legacy`, ETCD_ENDPOINTS=`http://etcd:2379`, шарды s1/s2 через hap1/hap2).

Прод-топология: `arch/configs/etcd/docker-compose.yml`, `arch/configs/postgres/docker-compose.yml` (Spilo, network_mode: host), `arch/configs/haproxy/...`; инструкции — доки 04/05/06.

## 7. Системные запросы PostgreSQL (образцы из проекта)

Из `arch/scripts/buckets-common.sh`, `move-bucket.sh`, дока 11 §5/§9:
- `pg_stat_replication` — `sync_state IN ('sync','quorum')`;
- `pg_replication_slots` — `confirmed_flush_lsn`, `active`, `synced`, `wal_status='lost'`, `safe_wal_size`; лаг слота `pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)`;
- `pg_stat_subscription` — `received_lsn`, `latest_end_lsn`, `latest_end_time`;
- `pg_subscription_rel` — прогресс initial copy (`srsubstate='r'`);
- `pg_stat_activity` — долгие транзакции, idle in transaction;
- `pg_class`/`pg_namespace` (инвентарь схем), `pg_publication`/`pg_subscription`, `pg_is_in_recovery()`, sequences;
- мониторинг WAL: размер `$PGDATA/pg_wal`, лаги.
- Patroni REST `/cluster` JSON (role/state/lag/timeline) — готовый «API состояния реплик».

## 8. Существующая инспекция

Отдельной админ-панели нет. Read-only CLI: `cluster-state.sh`, `find-leader.sh`, `get-role.sh`, `health.sh`, `patronictl.sh`, `move-bucket.sh status`, `abort-move.sh list`, `restore-cluster.sh verify`, `restore-system.sh plan`.

**Важно:** риск **P21** (`arch/12-bucket-pitfalls.md` §7) прямо требует «сводный дашборд "бакеты × шарды × статусы × лаги слотов" + состояние lease-мастеров шардов (etcd watch контрол-плейна + пер-бакетные метрики PG) + алерты на протухший lease мастера» — AdminPanel — предусмотренный проектом компонент.

## 9. Ключевые документы

- `arch/01-architecture.md` (Patroni/etcd/HAProxy, ключи DCS, failover, порты), `arch/07-identify-master.md` (все способы «кто мастер»), `arch/11-bucket-sharding.md` (бакеты: контроль-плейн §2, runbook §5, мониторинг §9), `arch/12-bucket-pitfalls.md` (P1–P23 + референс топологии), `arch/13-network-security.md`, `arch/stand/README.md` (docker-стенд, карта проверок).

## Практический вывод для AdminPanel

Минимально достаточный источник — **одно подключение к etcd (:2379)**: префикс `/clusters/` даёт кластеры→константы→шарды(dsn, replicas, master+lease)→routing бакетов→статусы переездов; префикс `/service/` — Patroni-состояние каждого шарда (leader/members). Опциональные уточнения: Patroni REST `:8008/cluster` (role/state/lag реплик) и SQL (`pg_stat_replication`, `pg_replication_slots`, `pg_stat_subscription`) по DSN из etcd — образцы запросов в `arch/scripts/buckets-common.sh`.
