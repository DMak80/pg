# 05. Деплой PostgreSQL + Patroni (Spilo)

Поднимаем **3 ноды** PostgreSQL, по одной на хост. Каждая нода:
- работает в контейнере **Spilo** (= Postgres + Patroni),
- хранит PGDATA на **своём локальном диске**,
- регистрируется в etcd-кластере как член scope `pgcluster`.

> Конфиги: `configs/postgres/docker-compose.yml` + `configs/postgres/pg.env`.

---

## 1. Роли при первом старте

Patroni сам проведёт «выборы» первого лидера:

1. Три ноды стартуют, каждая читает DCS (`/service/pgcluster/`).
2. Ключа лидера нет → ноды одновременно пытаются записать `/initialize`.
3. Победитель (обычно — та, что быстрее стартовала) делает `initdb`, становится
   **primary**, публикует конфиг.
4. Две другие видят лидера → делают `pg_basebackup`, становятся **replica**, начинают
   streaming replication.

> Поэтому порядок запуска не критичен. Но на практике удобно стартовать pg1 первой —
> он почти всегда становится лидером первого поколения.

---

## 2. Переменные окружения (Spilo)

Spilo настраивается **только** через env. Полный список — в
[ENVIRONMENT.rst](https://github.com/zalando/spilo/blob/master/ENVIRONMENT.rst).
Минимально нужное:

```bash
# configs/postgres/pg.env — пример для pg1. На pg2/pg3 меняется SPILO_*, остальное одинаково.

# --- идентификация кластера ---
SCOPE=pgcluster                          # ИМЯ КЛАСТЕРА. Одинаково на всех нодах!
SPILO_CONFIGURATION='...'                # встроенный Patroni YAML (см. ниже)

# --- подключение к DCS (etcd3) ---
ETCD_HOSTS=pg1:2379,pg2:2379,pg3:2379    # список endpoints etcd
# либо: ETCD3_HOSTS='pg1:2379,pg2:2379,pg3:2379'  (явно протокол etcd3)

# --- учётные данные PostgreSQL (СЕКРЕТЫ) ---
PGUSER_SUPERUSER=postgres
PGPASSWORD_SUPERUSER=<CHANGE_ME_strong>     # пароль суперпользователя
PGUSER_STANDBY=standby                       # имя пользователя репликации
PGPASSWORD_STANDBY=<CHANGE_ME_strong>        # его пароль

# --- параметры PG ---
PGROOT=/home/postgres/pgdata                # внутри контейнера Spilo PGROOT должен быть подмонтирован томом
USE_DATA_DIR_FOR_WAL=true                    # держать WAL в подкаталоге data (удобнее для бэкапа)

# --- прочее ---
ALLOW_NOSSL=true                             # для тестового стенда (в прод включи SSL)
```

### `SPILO_CONFIGURATION` — YAML Patroni внутри строки
Это самый удобный способ тонко настроить Patroni, не трогая образ. В env-файле пишется
как **однострочная строка с `\n`** или как **блок** (docker compose умеет multiline).

Минимальный набор (полная версия — в `configs/postgres/pg.env`):

```yaml
SPILO_CONFIGURATION: |
  bootstrap:
    dcs:
      synchronous_mode: true              # fail-safe: лидер ждёт подтверждения реплики
      postgresql:
        parameters:
          max_connections: '200'
          shared_buffers: '2GB'
          wal_level: replica
          hot_standby: 'on'
          max_wal_senders: '10'
          max_replication_slots: '10'
          wal_keep_size: '2048MB'
  postgresql:
    bin_dir: /usr/lib/postgresql/16/bin
    use_unix_socket: true
```

> `synchronous_mode: true` = лидер не коммитит, пока хотя бы одна реплика не подтвердила
> получение WAL. Это **гарантирует** нулевую потерю данных при failover. Стоимость —
> небольшая задержка на запись.

> Если этот кластер будет **шардом бакетов** ([11](11-bucket-sharding.md) §4,
> P3/P4 из [12](12-bucket-pitfalls.md)): дополнительно в `postgresql.parameters` —
> `wal_level: logical` (на всех нодах, не только мастере), `sync_replication_slots: 'on'`
> + `hot_standby_feedback: 'on'` (failover slots на репликах) и
> `max_slot_wal_keep_size` (изоляция взрыва WAL). Тогда логические слоты переездов
> переживают failover источника, а переполнение WAL при зависшем переезде
> инвалидирует слот, а не диск всего шарда.
>
> Рецепт failover slots, проверенный на стенде PG 18.4 (`arch/stand/`) — помимо
> параметров выше обязательны все три пункта:
> - подписки переездов создаются сразу `WITH (failover = true)` (менять потом —
>   только вне транзакции: `DISABLE` → `SET (failover)` → `ENABLE` отдельными
>   командами);
> - у каждой реплики — физический слот от мастера: `primary_slot_name`
>   (`pg_basebackup -C -S ...`); без него `sync_replication_slots` молча не работает;
> - в `primary_conninfo` реплики обязателен `dbname` — синхронизации слота нужна
>   подключаемая БД.
> Нюанс: сразу после initial copy подписки слот на репликах может отставать
> (`could not synchronize slot ...` в логах); promote в этом окне теряет слот →
> перед cutover переезда проверять `pg_replication_slots.synced = true` на репликах.

---

## 3. docker-compose.yml (узел PostgreSQL)

Полная версия — `configs/postgres/docker-compose.yml`. С пояснениями:

```yaml
# /opt/postgres/docker-compose.yml — пример для pg1
services:
  postgres:
    image: ghcr.io/zalando/spilo-16:3.3-p3
    container_name: postgres
    restart: unless-stopped
    hostname: pg1                          # должно совпадать с именем ноды в DCS!
    env_file: pg.env
    network_mode: host                     # чтобы Patroni API (8008) и PG (5432) были видны с хоста
    volumes:
      - /data/pg:/home/postgres/pgdata     # PGROOT: данные на отдельном диске
    # Patroni API и PG-порт открываются через host-сеть, ports: не нужны
```

> ⚠️ **`hostname` критичен**: Patroni использует его как `PATRONI_NAME` (имя ноды в DCS)
> по умолчанию. Если забыть — три ноды будут пытаться зваться одинаково и конфликтовать.

### Порты, которые откроются на хосте
| Порт | Что |
|---|---|
| 5432 | PostgreSQL (master принимает writes, replica тоже слушает 5432 но read-only) |
| 8008 | Patroni REST API (используют HAProxy и админ-скрипты) |

---

## 4. Запуск по нодам

```bash
# На pg1
sudo mkdir -p /opt/postgres && cd /opt/postgres
# положить docker-compose.yml + pg.env (с postgres пользователь -> pg1)
docker compose up -d
docker compose logs -f postgres | head -60
#   ждём строку про "no action. I am the leader with the lock" -> pg1 стал primary

# На pg2
sudo mkdir -p /opt/postgres && cd /opt/postgres
# pg.env такой же, hostname=pg2
docker compose up -d
docker compose logs -f postgres | head -60
#   ждём "clone from leader" -> pg2 делает basebackup, становится replica

# На pg3 — аналогично с hostname=pg3
```

---

## 5. Проверка кластера

### 5.1 Состояние через Patroni REST API
```bash
# на каждой ноде:
curl -s http://pg1:8008/ | jq .role          # → "master" или "replica"
curl -s http://pg1:8008/primary >/dev/null && echo "pg1 — leader" || echo "pg1 — not leader"
```

### 5.2 Через patronictl (внутри контейнера)
```bash
./scripts/patronictl.sh list
# ожидаемый вывод:
# + Cluster: pgcluster (7xxx...) ----+---------+----+-----------+
# | Member | Host    | Role    | State    | TL | Lag in MB |
# |--------|---------|---------|----------|----|-----------|
# | pg1    | 10.0.0.11 | Leader | running  |  1 |           |
# | pg2    | 10.0.0.12 | Replica| streaming|  1 |         0 |
# | pg3    | 10.0.0.13 | Replica| streaming|  1 |         0 |
```

### 5.3 Проверка репликации в самом PostgreSQL
```bash
docker exec -it postgres psql -U postgres -c \
  "SELECT application_name, state, sync_state, write_lag, flush_lag
   FROM pg_stat_replication;"
# на лидере должны быть 2 строки (pg2, pg3), sync_state = sync/quasy
```

### 5.4 Smoke-тест: пишем в лидера, читаем с реплики
```bash
# найти лидера (см. 07-identify-master.md), затем:
LEADER=pg1
docker exec -it postgres psql -U postgres -c "CREATE TABLE t(id int); INSERT INTO t VALUES (1);"

# на реплике (например pg2) — данные должны появиться почти мгновенно:
ssh pg2 'docker exec -it postgres psql -U postgres -c "SELECT * FROM t;"'
# → id=1
```

---

## 6. Что важно не упустить

| Грабли | Решение |
|---|---|
| Три ноды с одинаковым `hostname` → конфликт в DCS | `hostname: pg1`/`pg2`/`pg3` уникальны. |
| `/data/pg` принадлежит не тому uid | `chown -R 999:999 /data/pg` (postgres в образе). |
| Забыли `USE_DATA_DIR_FOR_WAL` → WAL вместе с PGDATA в одном каталоге — нормально, но при резервном копировании «только данных» его проще вынести | включить флаг. |
| Пароль меняли после первого старта → replication сломалась | пароли задаются **только при bootstrap**. Сменить = пересоздать replication-слот + ALTER USER, см. `08-operations.md`. |
| `synchronous_mode: true` → запись «подвисает», если все реплики мертвы | это **фича** (защита от потери). Для работы при потере 2 нод можно временно `patronictl edit-config` → `synchronous_mode: false`. |
| Patroni не видит etcd | проверить `ETCD_HOSTS`, что `curl http://pg1:2379/version` отвечает. |

---

## 7. Чек-лист

```text
[ ] /opt/postgres/{docker-compose.yml, pg.env} на всех трёх нодах
[ ] hostname/pg.env уникальны на каждой ноде (pg1/pg2/pg3)
[ ] SCOPE=pgcluster одинаковый на всех
[ ] ETCD_HOSTS указывает на все 3 узла etcd
[ ] /data/pg смонтирован, chown 999:999
[ ] docker compose up -d на всех трёх
[ ] patronictl list → 1 Leader + 2 Replica, state=running/streaming, lag=0
[ ] pg_stat_replication показывает 2 реплики
[ ] smoke-test запись/чтение прошёл
```

Готово → [06-deploy-haproxy.md](06-deploy-haproxy.md): точка входа для приложений.
