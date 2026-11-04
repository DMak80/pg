# Бакеты и шардирование

Уровень 3. Модель данных домена: бакеты-схемы поверх шардов, контрол-плейн
`/clusters/`, роутинг приложения, жизненный цикл шарда и онлайн-переезд
бакета. Оркестрация (provisioning, переезды, репарация) —
[pgworker/README.md](../pgworker/README.md); риски и принятые решения —
[12-bucket-pitfalls.md](12-bucket-pitfalls.md).

## 1. Общая картина

**Бакет** = схема PostgreSQL. Все бакеты имеют одинаковую структуру — это
«виртуальный шард» поверх физического. **Шард** = отдельный HA-кластер
([01](01-ha-architecture.md)–[10](10-replica-count.md)). На одном шарде
живёт несколько бакетов; любой бакет можно **онлайн** перевезти на другой
шард — запись доступна всё время, кроме финального cutover (пауза записи
на секунды).

```
 ┌────────────────────┐
 │ Приложение         │  по bucket_id выбирает шард и мастера (etcd watch)
 │ роутер бакетов     │
 └─────────┬──────────┘
           │ DSN = мастер шарда-владельца, :6432 (pg_doorman)
           ▼
 ┌────────────────────┐
 │ etcd-контрол-плейн │  /clusters/<C>/…: routing, status, master, dsn
 └─────────┬──────────┘
           │
 ┌─────────┴─────────┬──────────────────┐
 ▼                   ▼                  ▼
┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│ Шард 1      │ │ Шард 2      │ │ Шард N      │
│ bucket_41   │ │ bucket_43   │ │ bucket_…    │
│ bucket_42 ──┼─┼→ bucket_44  │ │             │
└─────────────┘ └─────────────┘ └─────────────┘
        переезд (логическая репликация)
```

Каждый шард — HA-кластер с failover Patroni. Отличие от «одного кластера»:
приложение не ходит через общий балансировщик — роутер берёт из etcd адрес
мастер-ноды (`shards/X/master`) и подключается к её `pg_doorman :6432`.
Репликационный трафик переездов идёт напрямую в ноды — multi-host conninfo.

Зафиксированные вводные:

- PostgreSQL 18.x; минимально допустимая версия — 15
  (`CREATE PUBLICATION … FOR TABLES IN SCHEMA`); на 17+ логические слоты
  синхронизируются на реплики (failover slots), на 18 — из коробки;
- секундный запрет записи на cutover допустим;
- DDL в бакете во время переезда гарантированно не будет — мораторий
  обеспечивается технически (`REVOKE CREATE` + сверки схем);
- приложение пишет отдельной app-ролью, **не владельцем схемы**; DDL
  выполняет миграционная роль-владелец;
- JOIN между бакетами не нужны — бакеты на разных шардах друг про друга не
  знают.

> Почему не Citus/pgEdge: Citus умеет schema-based шардирование и
> онлайн-ребалансинг, но требует топологии «координатор + воркеры» вместо
> N независимых Patroni-кластеров; pgEdge (Spock) решает DDL-репликацию,
> которая нам не актуальна (DDL-мораторий на время переезда). Остаёмся на
> своей топологии и нативной логической репликации PG ≥ 15
> (`FOR TABLES IN SCHEMA`).

## 2. Контрол-плейн: ключи одного кластера

```
/clusters/<C>/config → {"buckets": 256, "dbname": "<C>"}   # константы init, навсегда
/clusters/<C>/app_user → "app"                             # per-cluster креды приложения
/clusters/<C>/app_password → "<32 симв [A-Za-z0-9]>"        # txn put-if-absent, атомарно с app_user
/clusters/<C>/shards/X/dsn → "host=n1,n2 port=p1,p2 dbname=<C> user=bucket_admin password=…"
/clusters/<C>/shards/X/replicas → "2"                      # декларативное число нод
/clusters/<C>/shards/X/state → "TO_REMOVE"                 # маркер демонтажа (пишет панель)
/clusters/<C>/shards/X/master → "host:<doorman-port>"           # lease TTL 5с, Patroni-callback
/clusters/<C>/buckets/routing/bucket_42 → "shard1"          # владелец (авторитет)
/clusters/<C>/buckets/status/bucket_42 → {"state":"SYNCING",…} # только при переезде
```

- **config** — константы создания: `buckets` (N) и `dbname` (= `<C>`).
  Одна БД на кластер; N — навсегда (P18): смена ломает хеш-маппинг всех
  тенантов.
- **app_user/app_password** — per-cluster креды приложения (генерирует
  PgWorker, txn put-if-absent); читают воркер (роли/гранты) и приложение.
  В etcd без per-key ACL — класс экспозиции per-cluster секретов.
- **dsn** — статическая multi-host строка подключения к нодам шарда
  (`target_session_attrs=read-write` находит текущего мастера, P2); для
  подписок переездов, mover'а и админки. Несёт per-cluster креды
  `bucket_admin`; app-секрет в DSN не попадает никогда.
- **master** — динамический адрес мастера для роутера приложений
  (`host:<doorman-port>`, lease TTL 5с); единственный основной писатель —
  Patroni-callback `on_role_change`, сверка и коррекция — PgWorker
  ([07-identify-master.md](07-identify-master.md)).
- **app_params** — серверные параметры подключения к КОНКРЕТНОЙ ноде
  (libpq `keyword=value`, `sslmode=require`): клиент конкатенирует значение
  к своему DSN ([06-client-access.md](06-client-access.md) §2).
- **routing/status** — `buckets/routing/N` — единственный ответ «где бакет»
  (значение — имя шарда); нет статус-ключа = бакет **ACTIVE**;
  статус-ключ появляется только на время переезда и удаляется атомарно с
  flip.

Состояния бакета (`SYNCING`, `FROZEN`, `ABORTING`, `NOT_INITIALIZED`) и
переходы — [processes/enums.md](processes/enums.md) §3; фазы переезда
(`ddl`, `frozen`, `flip`, …) определяет процесс PgWorker
([pgworker/processes/f-move.md](../pgworker/processes/f-move.md)).

Переключение владельца — **одна атомарная etcd-транзакция** (compare
routing = старый владелец → put routing = новый + delete status): «flip
применился, а контрол-плейн об этом не знает» невозможно по построению.

Снапшоты контрол-плейна (P12) — локальные слепки PgWorker в точках
изменений; DR-выгрузка etcd — зона EtcdWorker
([etcdworker/04-snapshots.md](../etcdworker/04-snapshots.md)). Потеря карты —
не потеря данных (схемы на шардах восстанавливают истину), но требует
verify/heal ([09](09-troubleshooting.md) §5).

## 3. Роутер (в приложении)

Маршрутизация по схеме — обязанность кода приложения. На каждый
запрос/транзакцию приложение знает `bucket_id` и строит DSN:

1. **bootstrap**: статика — только адреса etcd и имя кластера `<C>`;
2. `N` и `dbname` — из `/clusters/<C>/config`;
3. `bucket_id = hash(tenant_id) % N`, схема `bucket_<id>`;
4. шард-владелец — `buckets/routing/bucket_id` (кэш TTL секунды,
   инвалидация watch'ем);
5. мастер — `shards/<shard>/master` (формат `host:<doorman-port>`);
6. DSN: `host=<master-host> port=<master-port> dbname=<C> user=<app_user>
   password=<app_password>` + конкатенация `app_params` мастер-ноды
   (`sslmode=require`); `master-host`/`master-port` — из значения
   master-ключа (`host:<doorman-port>`), не литерал;
7. пулы — на каждый шард, не на бакет.

Отказы и переходы:

- `FROZEN` — write отклоняется **retryable**-ошибкой; чтения идут на
  владельца;
- контракт на любую ошибку записи (FROZEN, `permission denied`
  «призрака», обрыв при failover): сброс кэша → refetch → ретрай;
- failover мастера — ключ обновляется callback'ом (окно — секунды);
  оборванные соединения переподключаются;
- смерть ноды — ключ гаснет по lease (≤5с), новые подключения к шарду
  отклоняются;
- недоступность etcd — работа по last-known-good кэшу (fail-open, P9):
  маршруты не протухают, переезды/выборы заморожены;
- переезд — flip routing; коннекты перетекают на нового владельца после
  разморозки.

## 4. Требования к шардам

Нода шарда и порты — [02-topology.md](02-topology.md). Параметры PG,
критичные для переездов (`wal_level=logical`, failover slots,
`max_slot_wal_keep_size`, слоты/walsender'ы), — канон
[05-postgres-patroni.md](05-postgres-patroni.md) §3. Ниже — требования
уровня ролей, подписок и pg_hba:

| Требование | Зачем |
|---|---|
| живой sync-standby у мастера приёмника | `synchronous_commit=remote_apply` подписки не вырождается в асинхронность (P8); preflight проверяет и отказывает |
| роль `bucket_mover` с `REPLICATION` и `SELECT` на таблицы схем | подключение подписки и initial COPY; при заморозке не трогается |
| app-роль ≠ owner; DML + `USAGE,UPDATE` на sequences | заморозка `REVOKE` отбирает запись; owner пишет вопреки REVOKE (P1) |
| `pg_hba` для `bucket_mover` с адресов нод приёмника | подписка подключается напрямую в pg-порт источника |

> ⚠️ **Строка подключения подписки — multi-host conninfo по всем нодам
> шарда-источника** (`host=node1,node2,… port=… target_session_attrs=read-write`),
> не в конкретную ноду. Иначе failover источника рвёт репликацию намертво.
> libpq перебирает адреса и выбирает текущего писателя; после failover
> apply-worker переподключается. Обратная подписка — multi-host по нодам
> нового владельца. Креды — `bucket_mover` (SCRAM), шифрование —
> `sslmode=require`; адреса — из dsn-ключа шарда.

## 4.5. Жизненный цикл системы

Кластер создаётся **один раз** с константами (N, dbname), дальше
масштабируется только по шардам. **Декларативный provisioning — единственный
путь живой системы**: панель AdminPanel заявляет кластер через HTTP API
PgWorker, воркер поднимает ноды, создаёт БД/роли/схемы бакетов и снимает
`state=NOT_INITIALIZED`; перевод в `TO_REMOVE` — демонтаж
([pgworker/02-cluster-lifecycle.md](../pgworker/02-cluster-lifecycle.md)).

- **init** — создание кластера: `buckets=N`, `dbname=<C>`, шарды с
  `replicas` и нодами, `request_*`; PgWorker создаёт все N бакетов сразу и
  раскладывает поровну round-robin, пишет routing; структуру таблиц
  накатывает миграционная роль журналом миграций (P5). N и dbname —
  навсегда: повторный init отказывает (клэйм/наличие config).
- **add-shard** — панель заявляет новый шард (replicas + nodes +
  `request_*`, без dsn); PgWorker поднимает ноды, создаёт БД/роли, пишет
  dsn. Шард стартует **пустым**: routing на него не указывает; бакеты
  переезжают отдельными заявками
  ([pgworker/03-shard-scaling.md](../pgworker/03-shard-scaling.md) §2).
- **remove-shard** — маркер `shards/<X>/state=TO_REMOVE`; guards G1–G7
  (нет бакетов, нет незавершённых переездов, не последний шард) —
  P23; демонтаж docker и очистка etcd
  ([pgworker/03-shard-scaling.md](../pgworker/03-shard-scaling.md) §3).
- **move** — заявка `/pgworker/moves/<C>/bucket_<i>` (`op=move|rollback|
  finalize|abort`); исполняет держатель клэйма `<C>`, одновременно —
  старейшая заявка кластера ([pgworker/06-bucket-moves.md](../pgworker/06-bucket-moves.md)).

Скриптового пути мутаций контрол-плейна в живой системе нет: единственный
писатель домена — PgWorker (панель и сиды ходят через его API); ручные
скрипты к кластерам под управлением PgWorker не применяются.

## 5. Процедура переезда (механика данных)

Переезд `bucket_42`: шард1 → шард2. Шаги идемпотентны; сбой лечится
повтором шага или откатом (раздел 6), источник не портится до finalize.
Оркестрация фаз M0–M6 — [pgworker/processes/f-move.md](../pgworker/processes/f-move.md);
здесь — SQL-механика, на которую опирается процесс.

### Шаг 0. Предполётные проверки

```sql
-- источник: логический WAL и запас слотов
SHOW wal_level;                                  -- = logical
SELECT count(*) FROM pg_replication_slots;       -- < max_replication_slots
-- приёмник: место под ещё одну подписку и отсутствие схемы
SELECT count(*) FROM pg_subscription;            -- +1 должна влезть
SELECT count(*) FROM pg_namespace WHERE nspname = 'bucket_42';
```

DDL-мораторий (P5) — на **обоих** шардах до сверки:
`REVOKE CREATE ON SCHEMA bucket_42 FROM app_role`. Новых таблиц не
появится — публикация не подхватит то, чего нет на приёмнике. `ALTER`/`DROP`
существующих закрыты тем, что app ≠ owner (P1); для миграционной роли
мораторий процедурный — страховка сверками.

Пре-сверка схем (P5): фактические схемы источника/приёмника против эталона
(шаблон бакета + журнал миграций). Расхождение → отложить переезд и
накатить миграции.

Прогноз места (P4): удержанный WAL ≈ скорость записи × оценка длительности
SYNCING × запас — против свободного места **на всех нодах шарда**
(synced-слоты P3 держат WAL и на репликах).

### Шаг 1. Перенести DDL (без данных)

`pg_dump --schema-only --schema=bucket_42 --no-owner --no-privileges` с
источника → `psql` на приёмник. Схемы, таблицы, индексы, sequences,
ограничения — всё кроме данных. В живой системе операцию исполняет агент
PgNode ноды-источника по команде PgWorker (`/api/node/pgdump`, локальный
socket-коннект, без секретов).

### Шаг 2. Публикация и подписка

```sql
-- источник
CREATE PUBLICATION pub_b42 FOR TABLES IN SCHEMA bucket_42;

-- приёмник
CREATE SUBSCRIPTION sub_b42
  CONNECTION 'host=<shard1_n1>,<shard1_n2>,<shard1_n3> port=… dbname=<C>
              user=bucket_mover password=… sslmode=require
              target_session_attrs=read-write'
  PUBLICATION pub_b42
  WITH (copy_data = true, failover = true,
        synchronous_commit = remote_apply);
```

Подписка создаёт слот на источнике и делает initial COPY, затем стримит
изменения. **Бакет всё время доступен на запись.**

> `synchronous_commit=remote_apply` (P8): коммит применённой транзакции ждёт
> replay на standby приёмника — иначе feedback убегает вперёд его
> физической репликации, и failover приёмника посреди SYNCING **молча
> пропускает** срез изменений. Работает только при живом sync-standby у
> мастера приёмника — preflight отказывает без него.

> Окно досинка слота после initial copy: `failover=true` синхронизируется на
> реплики асинхронно; в момент initial copy возможны
> `could not synchronize slot …`. Промо реплики в этом окне теряет слот →
> перед cutover проверять `pg_replication_slots.synced = true`.

### Шаг 3. Ждать догоняния

```sql
-- приёмник
SELECT subname, received_lsn, latest_end_lsn, latest_end_time
FROM pg_stat_subscription WHERE subname = 'sub_b42';

-- источник: отставание слота
SELECT slot_name, active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag
FROM pg_replication_slots WHERE slot_name LIKE 'sub_b42%';
```

Initial copy большого бакета — часы; lag после него — доли секунды. Слот
держит WAL на источнике — следить за диском (P4). Ожидание толерантно к
обрывам: failover посреди initial copy рестартует копирование таблицы новым
tablesync-слотом (осиротевшие доберёт finalize).

### Шаг 4. Cutover (секунды)

1. **Заморозка в контрол-плейне**: статус `FROZEN` — роутер отклоняет
   запись, чтение живёт.
2. **Железная заморозка на источнике — ДО capture LSN** (P1):

```sql
BEGIN;
SET lock_timeout = '5s';
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA bucket_42 FROM app_role;
REVOKE USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA bucket_42 FROM app_role;
-- REVOKE — не барьер: берёт AccessShareLock и проходит при живом писателе.
-- Барьер — явный LOCK в той же транзакции:
LOCK TABLE bucket_42.<t1>, bucket_42.<t2>, … IN ACCESS EXCLUSIVE MODE;
COMMIT;
```

`LOCK` физически ждёт завершения всех транзакций, державших блокировки;
после `COMMIT` писателей доказательно нет. Зависший `idle in transaction` —
таймаут по `lock_timeout` → terminate по `pg_stat_activity` + ретрай.
Порядок обязателен: `REVOKE` + барьер до capture LSN, иначе поздний
писатель закоммитится после захвата позиции. Список таблиц — из
`pg_class`/`pg_namespace` (все `relkind='r'` схемы), не хардкод.

3. **Зафиксировать LSN** (`SELECT pg_current_wal_lsn()`) и ждать
   `confirmed_flush_lsn` слота ≥ него.
4. **Контрольная сверка схем** перед flip (P5): initial copy долгий, DDL
   мог уехать; расхождение → отказ cutover (GRANT + разморозка).
5. **Синхронизация sequence→sequence** (P6) — поимённо для всех sequence
   схемы: `last_value`/`is_called` источника → `setval` на приёмнике;
   инвариант: следующее выдаваемое на приёмнике СТРОГО > последнего
   выданного на источнике. Источник уже заморожен — margin не нужен.
   `is_called` в конкатенации даёт `'true'/'false'` (короткое `'t'` — только
   дисплей psql); скриптам считать на стороне SQL, не парсить boolean в bash.
6. **Сверка строк** перед flip (P8): `count(*)` каждой таблицы на
   источнике и приёмнике; расхождение → отказ cutover с разморозкой +
   abort и повторный move (свежий initial copy).
7. **Flip** — одна атомарная etcd-транзакция:

```text
val(/clusters/<C>/buckets/routing/bucket_42) = "shard1"
→ put routing = "shard2" + del status
```

Роутер видит нового владельца; запись идёт на шард2. Опоздавший писатель
(«соединение-призрак», P1) получает **громкую** `permission denied` вместо
тихой потери: REVOKE на источнике держится до финализации, ретрай уводит
его на новый шард. Фриз длится столько, сколько ждать лаг 0 (обычно < 1–2 с).

Отказ/таймаут на любом подшаге до flip — **разморозка** на источнике
(`GRANT INSERT, UPDATE, DELETE, TRUNCATE` + `GRANT UPDATE` на sequences);
бакет возвращается в SYNCING, репликация догоняет, move повторяется.

### Шаг 5. Finalize (не спешить)

Окно DDL-запрета — до finalize, **не до flip** (P5): после flip схема
источника ещё нужна обратной подписке. Сначала — обратная подписка для
отката (раздел 6), пожить часы/дни, затем:

```sql
-- новый владелец: снять DDL-запрет (GRANT CREATE), миграционная роль снова работает
-- приёмник: DROP SUBSCRIPTION sub_b42;   -- отпускает слот на источнике
-- источник: DROP PUBLICATION pub_b42;
-- источник: DROP SCHEMA bucket_42 CASCADE; -- последним; REVOKE умирает со схемой
```

В живой системе finalize/abort/rollback исполняет PgWorker по заявкам;
`DROP SUBSCRIPTION` при недоступном источнике — fallback (`DISABLE` →
`SET (slot_name = NONE)` → `DROP`, слот-сирота добивается следом).

## 6. Откат

До удаления схемы на источнике откат — тот же cutover в обратную сторону
(заявка `rollback`):

1. на новом владельце — `CREATE PUBLICATION pub_b42_back`;
2. на источнике — `CREATE SUBSCRIPTION sub_b42_back … (copy_data = false,
   failover = true, synchronous_commit = remote_apply)`, conninfo —
   multi-host по нодам нового владельца (схема и данные на источнике ещё
   на месте — copy_data не нужен);
3. зеркальный cutover: FROZEN → REVOKE DML на новом владельце → лаг 0 →
   сверка схем/строк → sequences (setval только вперёд) → атомарный
   flip обратно → симметричный GRANT.

Откат работает и после `DROP SUBSCRIPTION` — но через полный re-copy
данных. Нет обратной подписки — перманентный отказ «откат только полным
re-copy».

## 7. Отказы во время переезда

| Что случилось | Что происходит | Что делать |
|---|---|---|
| Исполнитель умер посреди переезда | Артефакты pub/sub/slots/схемы в промежуточном состоянии, статус-ключ висит | Репарация PgWorker (K) или `abort`: журнал ABORTING до манипуляций, идемпотентная уборка (подписки → слоты → публикации → re-GRANT на владельце → DROP SCHEMA не-владельцев), удаление статус-ключа = ACTIVE |
| Failover источника в SYNCING | Подписка ходит по multi-host conninfo → переподключается к новому мастеру; слот переживает failover (failover slots, P3) | Обычно ничего; слот отстаёт на лаг репликации → повторная доставка, replication origin пропускает применённое. Fallback: DROP SUBSCRIPTION, DROP SCHEMA, повторить с шага 1 |
| Failover приёмника в SYNCING | Подписка — объект каталога PG, физическая репликация переносит её на нового мастера; `remote_apply` держит feedback за физрепликацией | Обычно ничего; initial copy рестартует новым tablesync-слотом (сирот добьёт finalize). Без sync-standby защита не действует — сверка строк перед flip поймает дефект: abort + повторный move |
| Слот инвалидирован (`wal_status = 'lost'`) | `max_slot_wal_keep_size` превышен — PG срезал слот: умирает переезд, а не шард; бакет на источнике ACTIVE и консистентен | Алерт по `safe_wal_size` ДО порога; лечение: rollback/abort (срезать артефакты, WAL освободится) → понять, почему просчитан префлайт → повторить |
| Переполнение диска WAL (слот жив) | Слот держит WAL, initial copy долгий | Алерт на lag/размер `pg_wal`; аварийно — DROP SUBSCRIPTION (срежет слот), переезд позже |
| Сбой роутера при FROZEN | Записи отбиваются retryable | Клиенты повторяют; фриз рассчитан на секунды |
| Писатель-«призрак» после flip | REVOKE держится до finalize → `permission denied` вместо тихой потери | Ретрай через роутер уводит на новый шард; постфактум — сверка counts |
| etcd недоступен посреди переезда | Cutover невозможен (нужна txn); бакет живёт на владельце: SYNCING догоняет без etcd, FROZEN отбивает запись дольше обычного; роутер fail-open | Ждать восстановления etcd; после — flip либо abort при затянувшемся FROZEN |
| Контрол-плейн восстановлен из устаревшего снапшота | Данные на местах, карта может расходиться с фактом | verify/heal: однозначные расхождения чинятся (routing × фактические схемы); окна переездов — resume/abort/finalize |

## 8. Подводные камни и ограничения

- **DDL не реплицируется.** Публикация `FOR TABLES IN SCHEMA` подхватит
  новые таблицы источника, но на приёмнике их нет → apply упадёт.
  Мораторий технический: `REVOKE CREATE` + app ≠ owner + двойная сверка;
  окно — шаг 0 → finalize. Смена DDL между переездами адресуется бакету и
  применяется на текущем владельце; журнал миграций обязателен.
- **Sequences не реплицируются** — синхронизация sequence→sequence на
  cutover; эвристика `setval(max(id)+margin)` ломается на кастомных
  `nextval`, дырах и нескольких sequence на таблицу — не использовать (P6).
- **Large objects (`pg_largeobject`) не переносятся** логической
  репликацией; если бакеты их используют — переносить отдельно
  (`pg_dump --blobs` до cutover).
- **`TRUNCATE` реплицируется** (входит в публикацию) — ожидаемо.
- **Кросс-бакетные JOIN** — только внутри одного шарда; между шардами —
  сборка в приложении.
- **Долгие транзакции источника** тормозят логическое декодирование → лаг
  растёт; мониторить `pg_stat_activity.xact_start` при переездах.
- **Число одновременных переездов** ограничено `max_replication_slots`
  (источник) и `max_logical_replication_workers`/`max_sync_workers_per_subscription`
  (приёмник) — параллельно можно, но по счёту.

## 9. Мониторинг

- Источник при активных слотах: `pg_wal_lsn_diff(pg_current_wal_lsn,
  confirmed_flush_lsn)`, размер `pg_wal`; алерт при лаге > N МБ / диске
  > 80%; `safe_wal_size` — алерт ДО инвалидации; `wal_status = 'lost'` —
  слот уже срезан (P4).
- Слоты на репликах (failover slots P3) держат WAL и там — контроль места
  на **всех** нодах шарда.
- Приёмник: `pg_stat_subscription.latest_end_time` не старше минуты.
- Sync-standby у мастеров обоих шардов (`pg_stat_replication.sync_state`):
  пропажа — алерт, а не сюрприз на preflight (P8).
- Контрол-плейн: бакеты со статус-ключом дольше порога (`SYNCING` —
  `RepairStaleSec`, `FROZEN` — `RepairFrozenSec`); `ABORTING` —
  незавершённая уборка (P7).
- Роутер: доля FROZEN-ошибок и длительность фризов.
- Постфактум: сверка `count(*)`/`sum(hashtext(t::text))` таблиц на обоих
  шардах.
- Runbook «где бакет сейчас»: `etcdctl get
  /clusters/<C>/buckets/routing/bucket_N` → шард → его `dsn`/`master`.

## 10. Что не входит

- Код роутера бакетов — приложение-потребитель, вне репозитория.
- Provisioning/переезды/репарация как процессы — PgWorker
  ([pgworker/README.md](../pgworker/README.md)).
- Бэкапы и restore шарда — [backups/README.md](../backups/README.md).
- Скриптовый путь мутаций — вне живой системы; к кластерам под управлением
  PgWorker не применяется.
