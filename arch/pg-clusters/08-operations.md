# Эксплуатация шарда

Уровень 3. Плановые и аварийные операции HA-кластера: switchover, failover,
возврат и rebuild ноды, обслуживание, мониторинг. Процессы —
[processes/c-switchover.md](processes/c-switchover.md),
[processes/b-failover.md](processes/b-failover.md),
[processes/d-node-recovery.md](processes/d-node-recovery.md); оркестрация
PgWorker — [pgworker/processes/c-node-supervisor.md](../pgworker/processes/c-node-supervisor.md).

## 1. Switchover (плановая смена лидера)

Когда: обслуживание ноды-лидера, обновление образа, ротация серта,
перебалансировка.

```bash
# в контейнере ноды-лидера или любой ноды кластера
patronictl -c <конфиг Spilo> switchover --master <C>-<X> --candidate <нода>
```

Или `POST https://<нода>:<patroni-port>/switchover` с basic-auth
per-cluster (`rest_password`; GET-пробы заголовок не несут). Что
происходит: Patroni проверяет, что кандидат догнал лидера, останавливает
запись на старом мастере, повышает кандидата; старый мастер становится
репликой. Клиенты переезжают после обновления мастер-ключа; открытые
сессии старого мастера завершаются.

Проверка: `GET /primary` у нового мастера = 200; `patronictl list` — один
Leader, остальные streaming, lag 0. Точные состояния —
[processes/c-switchover.md](processes/c-switchover.md).

## 2. Failover (неплановая смена)

В штатном режиме делать ничего не нужно: Patroni сам обнаруживает потерю
lease, выбирает самую свежую реплику и повышает её; PgWorker ускоряет
failover мёртвого лидера (удаление leader-ключа + `/service/<scope>/failover`
с кандидатом) — бюджет [01-ha-architecture.md](01-ha-architecture.md) §4.

Вмешательство нужно, если failover не произошёл за бюджет:

- проверить кворум etcd (`endpoint health --cluster`) и отсутствие
  `patronictl pause`;
- проверить, что есть живая реплика с нужной timeline;
- принудительный failover — `patronictl failover --candidate <самая свежая
  реплика>` или `POST /failover` (basic-auth); это последнее средство,
  после него — разбор причины.

## 3. Возврат упавшей ноды

Patroni делает всё сам по состоянию PGDATA:

| Состояние PGDATA | Действие Patroni |
|---|---|
| пусто/нет | `pg_basebackup` с лидера → реплика |
| небольшое расхождение | догоняет по WAL |
| большое расхождение | `pg_rewind` (`use_pg_rewind: true`) |
| повреждено/несогласовано | может застрять → rebuild |

Ключ `/service/<scope>/initialize` уже существует, поэтому пустая нода не
делает `initdb`, а клонируется с лидера. Перед возвратом убедиться, что
лидер здоров и репликация с него работает.

## 4. Rebuild ноды

Штатный путь — PgWorker: надзор пересоздаёт мёртвую ноду с чистого места
(удаление контейнера и тома, новый пустой диск, `pg_basebackup`), сохраняя
portalloc-закрепление. Правила:

- **не лидер**: лидера сначала выводят из строя штатным failover; для
  `start_failed`-лидера действует failover-first — снос только после факта
  смены лидера (иначе уничтожается авторитетная копия);
- **кворум etcd жив**: мертва максимум одна нода, живых ≥ max(1, R−1);
- **положительное свидетельство смерти**: контейнер отсутствует или не
  running; недоступность REST при живом контейнере — не повод сносить;
- **единственная нода (R=1)** — снос только через restore-контур из бэкапа,
  не «в пустоту».

Маркер оператора `POST /api/ha/{scope}/nodes/{node}/recreate` (`soft` —
graceful switchover живой ноды, `hard` — снос/пересоздание) исполняет
надзор. Усыновлённые (`object`) ноды не rebuild'ятся — разбор оператором
([pgworker/processes/c-node-supervisor.md](../pgworker/processes/c-node-supervisor.md)
§7). Точный автомат — [processes/d-node-recovery.md](processes/d-node-recovery.md).

Внимание: при `synchronous_mode_strict=true` потеря sync-реплики
блокирует запись до её возврата/промоушена — это durability-first, а не
дефект; отключать strict на время rebuild — осознанное ослабление
гарантий, требующее отдельного решения.

## 5. Состав нод и replicas

Число нод шарда `replicas` (1..26, дефолт 2) фиксируется декларацией при
создании кластера/шарда. Штатной операции «добавить ноду в существующий
шард» нет: состав нод — часть декларации; изменение числа реплик живого
шарда не предусмотрено API. Масштабирование системы — добавление **нового
шарда** (`POST /api/clusters/{c}/shards`) и переезд бакетов на него
([11-buckets.md](11-buckets.md) §4.5, [10-replica-count.md](10-replica-count.md)).
Пересоздание ноды (тот же состав) — маркер `recreate`/авто-rebuild.

## 6. Обслуживание и пауза Patroni

- **`patronictl pause`/`resume`** — отключает автоматический failover на
  время работ (например, обслуживание etcd). Помнить: пока пауза стоит,
  смерть лидера не лечится; снимать сразу после работ.
- **`pending_restart`** — Patroni пометил postmaster-параметр; применяется
  при ближайшем рестарте ноды (rebuild/пересоздание). PgWorker рестарт PG
  не инициирует; `pending_restart=true` — штатное состояние, не алерт.
- **DDL** — только миграционная роль-владелец; app-роль DDL не выполняет
  (P5). Ручные `ALTER`/`DROP` схем бакетов в обход журнала миграций
  ломают сверки переездов.

## 7. Мониторинг

- **Patroni REST `/metrics`** — нативные серии (`patroni_primary`,
  `patroni_replica`, `patroni_sync_standby`,
  `patroni_postgres_timeline`, `patroni_xlog_replayed_timestamp`,
  `patroni_postgres_running`, `patroni_pending_restart`, …); scrape —
  [metrics/README.md](../metrics/02-dictionary.md) §5, таргеты — file_sd из portalloc
  §5.4.
- **Репликация** — лаг (`pg_stat_replication`, `patroni_xlog_*`),
  `sync_state`; пропажа sync-standby — алерт (условие переездов P8).
- **Кворум DCS** — `etcd_server_has_leader`, `endpoint health`; потеря
  кворума — отдельный алерт.
- **Мастер-ключ** — протухший lease/расхождение с `/primary` (P11);
  коррекция и алерт.
- **`wal_status` слотов и место** — при активных переездах
  ([11-buckets.md](11-buckets.md) §9).
- **doorman** — `maxwait` (клиенты ждут серверное соединение), ошибки
  аутентификации; метрики/логи пулера.
- **HA-факты** — `last_failover`/`last_rebuild` в журнале PgWorker
  ([pgworker/00-etcd-contract.md](../pgworker/00-etcd-contract.md) §4).

## 8. Полезные SQL-снимки

```sql
-- роль текущей сессии (false = primary, true = replica)
SELECT pg_is_in_recovery();

-- на лидере: подключённые реплики и лаг
SELECT application_name, state, sync_state,
       pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) AS lag_bytes,
       write_lag, flush_lag, replay_lag
FROM pg_stat_replication;

-- позиция WAL
SELECT pg_current_wal_lsn();          -- на лидере
SELECT pg_last_wal_replay_lsn();      -- на реплике

-- слоты и удержание WAL
SELECT slot_name, active, wal_status, safe_wal_size,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn))
FROM pg_replication_slots;
```

## 9. Границы

- Не выполнять `pg_ctl promote` вручную при живом DCS — обход Patroni
  создаёт split-brain ([09-troubleshooting.md](09-troubleshooting.md) §4).
- Не переопределять PGROOT, порты и параметры PG вне канона PgWorker:
  конвергенция вернёт канон, а расхождение конфигов между нодами — риск.
- Не удалять контейнеры/тома руками: снос и пересоздание — только через
  PgWorker (декларации/надзор); ручная чистка оставляет сиротские
  portalloc-записи и дыры в надзоре ([09](09-troubleshooting.md) §10).
