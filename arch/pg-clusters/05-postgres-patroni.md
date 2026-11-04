# PostgreSQL и Patroni: конфигурация

Уровень 3. Версия и параметры PostgreSQL, конфигурация Patroni/Spilo,
синхронная репликация, failover slots, bootstrap и конвергенция DCS.
HA-механика — [01-ha-architecture.md](01-ha-architecture.md); требования
шарда к параметрам для переездов — [11-buckets.md](11-buckets.md) §4.

## 1. Версия и образ

PostgreSQL **18.x** (Spilo `spilo-18:4.1-p2`, Patroni 4.x). Минимально
допустимая версия — **15** (`CREATE PUBLICATION … FOR TABLES IN SCHEMA`);
начиная с **17** логические слоты синхронизируются на реплики — на 18 это
есть из коробки (failover slots, важно для переездов). Spilo — проверенный
в проде образ Zalando: PostgreSQL + Patroni + окружение; свой Dockerfile
поверх не поддерживается.

## 2. SPILO_CONFIGURATION

Конфигурация ноды — YAML `SPILO_CONFIGURATION`, который генерирует PgWorker
при provisioning/rebuild и доставляет агенту PgNode через bootstrap-ключ
etcd (секреты внутри, в docker-env не попадают —
[pgworker/04-security.md](../pgworker/04-security.md) §3). Структура:

- `restapi` — `connect_address: <полное имя ноды>:8008` (DNS per-cluster
  сети; api_url в DCS = DNS из SAN), TLS-серт/ключ ноды (per-install
  REST-CA), `authentication` — per-cluster пара `rest_password` для
  мутационных эндпоинтов;
- `bootstrap.dcs` — `synchronous_mode`, `synchronous_mode_strict`
  (per-cluster), тайминги Patroni, `postgresql.parameters` =
  merge(PGTune ∪ канон);
- `postgresql` — пути Spilo, `use_unix_socket`; PGROOT не переопределяется
  (данные — в дефолтном `/home/postgres/pgdata`).

Генерация, PGTune-расчёт и правила слияния параметров —
[pgworker/01-placement.md](../pgworker/01-placement.md) §2.

## 3. Параметры PostgreSQL

Канон поверх PGTune (перезапись по имени; значения обязательны для домена):

| Параметр | Значение | Зачем |
|---|---|---|
| `wal_level` | `logical` | логическое декодирование подписок переездов (на всех нодах) |
| `hot_standby` | `on` | чтение с реплик |
| `sync_replication_slots` | `on` | failover slots: логические слоты синхронизируются на реплики |
| `hot_standby_feedback` | `on` | реплики сообщают мастеру о своём xmin — слоты не срезаются |
| `max_slot_wal_keep_size` | меньше диска, с запасом на штатный переезд | изоляция взрыва WAL: при переполнении инвалидируется слот, а не диск шарда |
| `max_wal_senders` | `10` | walsender на каждую подписку (с PG 13 не занимают `max_connections`) |
| `max_replication_slots` | `10` | запас под одновременные переезды + HA-слоты |
| `wal_keep_size` | с запасом | удержание WAL для реплик |
| `checkpoint_timeout`, лог-блок | канон PgWorker | предсказуемость checkpoint и диагностика |

`max_connections` приходит из PGTune (вход `PgWorker:Pgtune:Connections`,
дефолт 60); бюджет doorman → PG считается от него —
[06-client-access.md](06-client-access.md) §4. Параметры
postmaster-класса применяются при ближайшем рестарте ноды: Patroni помечает
`pending_restart`, PgWorker рестарт PG не инициирует — это штатное
состояние, а не алерт.

## 4. Синхронная репликация и strict

- `synchronous_mode=true` — Patroni выставляет `synchronous_standby_names`
  и ждёт подтверждения хотя бы одной синхронной реплики.
- `synchronous_mode_strict` — **per-cluster** опция из
  `/clusters/<C>/config` (отсутствие поля = `true`, durability-first).
  `true` — без sync-standby запись блокируется; `false` — деградация в
  асинхронность. Поле пишут только API создания/мутации кластера; воркер
  его читает и никогда не меняет.
- **Следствия strict**: при `replicas < 2` новый шард блокировал бы запись
  без sync-standby — add-shard для strict-кластера отклоняется (400
  панели) ([pgworker/03-shard-scaling.md](../pgworker/03-shard-scaling.md) §2).
  Потеря sync-реплики останавливает запись до её возврата/промоушена —
  это фича, а не дефект.
- Для переездов `synchronous_commit=remote_apply` подписки требует живого
  sync-standby у приёмника; без него защита P8 тихо вырождается —
  preflight переезда проверяет и отказывает ([11](11-buckets.md) §5).

## 5. Failover slots (рецепт для переездов)

Логические слоты переездов должны переживать failover источника. Канон
(проверен на PG 18.4):

1. Подписки создаются сразу `WITH (failover = true)`; менять потом — только
   вне транзакции (`DISABLE` → `SET (failover)` → `ENABLE` отдельными
   командами).
2. У каждой реплики — физический слот от мастера: `primary_slot_name`
   (`pg_basebackup -C -S …`); без него `sync_replication_slots` молча не
   работает.
3. В `primary_conninfo` реплики обязателен `dbname` — синхронизации слота
   нужна подключаемая БД.
4. После initial copy слот на репликах может отставать (`could not
   synchronize slot …` в логах); promote в этом окне теряет слот →
   перед cutover проверять `pg_replication_slots.synced = true` на репликах.

Детали и отказы — [11-buckets.md](11-buckets.md) §5,
[12-bucket-pitfalls.md](12-bucket-pitfalls.md) P3/P8.

## 6. Bootstrap и reinit

- **Первый старт кластера**: Patroni-ноды видят отсутствие
  `/service/<scope>/initialize`, одна выигрывает гонку и делает `initdb`,
  остальные клонируются с лидера (`pg_basebackup`) и стартуют репликами.
  Порядок запуска не критичен; ключ `initialize` защищает от повторного
  bootstrap.
- **Пароли**: per-install пароли PG (`SuPassword`/`StandbyPassword`)
  доставляются агенту при старте ноды и живут в памяти/tmpfs контейнера;
  они не меняются ротацией per-cluster секретов. Per-cluster креды
  (`app`, `bucket_mover`, `bucket_admin`) — ротация
  ([pgworker/04-security.md](../pgworker/04-security.md) §6), а не ручной
  `ALTER` с пересозданием слота.
- **Reinit/возврат**: пустой PGDATA → basebackup с лидера; расхождение →
  WAL/`pg_rewind`; повреждение → rebuild с чистого места
  ([08-operations.md](08-operations.md) §3–§4,
  [processes/d-node-recovery.md](processes/d-node-recovery.md)).
- **DCS-конфиг ≠ bootstrap**: `bootstrap.dcs` применяется только при первой
  инициализации; живой конфиг — динамический `/service/<scope>/config`,
  который приводит к канону конвергенция PgWorker (S2
  [17-synchronization-principles.md](../17-synchronization-principles.md)).
  Свежеподнятая нода может стартовать не на нашем наборе — первый тик
  конвергенции исправляет.

## 7. Обновление версии

Minor-обновления — смена тега образа Spilo и rolling restart нод через
пересоздание (PgWorker rebuild/надзор), по одной ноде, с switchover с
лидера. Major-обновление — отдельная процедура (параллельный кластер +
логическая репликация), в текущий канон не входит.
