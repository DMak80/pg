# Бэкапы шардов — домен

Уровень 2. Описание домена бэкапов: что и где хранится, кто и как
оркестрирует, какие контракты и политики действуют. Уровень 3 — документы
функций (`01`–`09`), уровень 4 — процессы оркестрации и перечисления в
`processes/`.

## 1. Назначение

Домен защищает данные шардированных PG-кластеров: полные `pg_basebackup`,
online-WAL (физическая репликация с выгрузкой в S3), проверка
восстановимости (verify, drill) и восстановление (PITR latest/target_time).
Исполнитель операций на ноде — агент **PgNode**
([pgnode/README.md](../pgnode/README.md)); оркестратор — **PgWorker**.

**PgWorker — хозяин бэкапов шардов**: он сам создаёт, ретраит/пересоздаёт,
чистит, сигнализирует в панель. Ноды (Spilo) бэкапами не управляют.
WAL-G/pgBackRest НЕ встраиваются (параллельное использование оператором не
поддерживается и не смешивается).

## 2. Обязанности и границы

- **Единый оркестратор** — PgWorker: ВСЕ операции бэкапов (полный, verify,
  restore/drill) исполняет ОДИН И ТОТ ЖЕ процесс PgNode ноды — тот же
  процесс стрима WAL. Отдельных процессов/контейнеров для операций нет;
  воркер общается с приёмником через etcd и его HTTP API
  ([pgnode/02-api.md](../pgnode/02-api.md)).
- **Контракт в etcd** — состояния пишутся под `/pgworker/backups/*`; префикс
  пишет PgWorker (держатель клэйма `<C>`), приёмники — только свои
  статус-ключи, панель читает ([05-etcd-contract.md](05-etcd-contract.md)).
- **Хранилище — объектное S3** — один bucket на установку, layout
  per-cluster/per-shard, файлы `pg_basebackup` 1:1
  ([06-s3-layout.md](06-s3-layout.md)); цели записи — per-cluster пул S3,
  креды — per-cluster secrets в etcd ([08-secrets.md](08-secrets.md)).
- **Паттерны arch/17** — идемпотентность каждого шага,
  journal-before-manipulations, transient vs permanent-отказы.
- **Не входит**: UI/алерты панели (вычисления над снапшотом — зона
  AdminPanel); управление нодами/HA-контуром при СОЗДАНИИ бэкапов (полные и
  WAL — сторонние клиенты нод; restore — операция пересоздания шарда,
  выполняется держателем клэйма `<C>`); файловое том-хранилище docker-хоста
  (решение — S3); данные Kafka (невосстановимы из бэкапов — RPO держится
  RF×minISR, [kafkaworker/08-operability.md](../kafkaworker/08-operability.md) §4).

## 3. Функции

| Функция | Содержание | Документ |
|---|---|---|
| Полные бэкапы | инструмент `pg_basebackup`, планировщик rolling, выбор источника, WAL набора, идентификатор, бэкофф, наблюдаемость verify | [01-full-backup.md](01-full-backup.md) |
| WAL-архивация | двойная архивация, слоты, контроль непрерывности, `WAL_CHAIN_ERROR`/`gap`, отставание, стоп-семантика | [02-wal-archive.md](02-wal-archive.md) |
| Восстановление (PITR) | заявка restore, RestoreProcess, демонтаж/подъём шарда, REJOINING, гвард исключения, отмена | [03-restore.md](03-restore.md) |
| Дрилл восстановимости | периодический неразрушающий прогон DR-пути, отбор кандидата, стоп-семантика | [04-drill.md](04-drill.md) |
| Контракт etcd | ключи `/pgworker/backups/*`, policy, правила transient/permanent, backwards-compat | [05-etcd-contract.md](05-etcd-contract.md) |
| S3 layout | префиксы, семантика пула, circuit-breaker, дублирование WAL, служебный `etcd/` | [06-s3-layout.md](06-s3-layout.md) |
| Ретенция и гигиена | GFS-отбор, guard последнего валидного, чистка WAL, wal-sweeper, гейты мусора, storage/quota, алерты | [07-retention.md](07-retention.md) |
| Секреты | `backup_exec`, `backup_password`, per-cluster secrets пула, CA, pg_hba-гвард | [08-secrets.md](08-secrets.md) |
| Эксплуатация | конфигурация `PgWorker:Backups`, бюджеты операций, риски, наблюдаемость | [09-operability.md](09-operability.md) |

Точные автоматы оркестрации — в процессах уровня 4
(`processes/`), механика исполнения на ноде — в
[pgnode/README.md](../pgnode/README.md) и его процессах.

## 4. Взаимодействие

- **AdminPanel → PgWorker**: мутации policy/restore/orphans — HTTP API
  воркера ([pgworker/07-api.md](../pgworker/07-api.md)); статусы бэкапов
  панель читает из `/pgworker/backups/*`
  ([adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md) §2.3.1).
- **PgWorker → PgNode**: команды операций (`/api/backup/full*`,
  `/api/ops/*`, `/api/node/*`) по `alias:8080` общей сети воркера и нод
  ([pgnode/02-api.md](../pgnode/02-api.md),
  [25-component-communications.md](../25-component-communications.md) №22).
- **PgNode → PostgreSQL**: физическая репликация, `pg_basebackup`,
  `pg_verifybackup`, локальный restore — только `127.0.0.1`
  ([pgnode/README.md](../pgnode/README.md)).
- **PgNode / PgWorker → S3**: приёмники нод пишут объекты напрямую; воркер
  читает/чистит (ретенция, супервизор) — [06-s3-layout.md](06-s3-layout.md).
- **PgNode → etcd**: статус-ключи приёмника и агента
  ([05-etcd-contract.md](05-etcd-contract.md)).
- **EtcdWorker → S3**: DR-слепки etcd — отдельная зона
  ([etcdworker/04-snapshots.md](../etcdworker/04-snapshots.md)).

## 5. Свойства, на которые опираются все функции

- **Истина прогресса — S3**: приёмник пишет объекты и свой статус-ключ;
  воркер контролирует цепочку по фактическим объектам, а не по статусу
  приёмника ([02-wal-archive.md](02-wal-archive.md) §3).
- **Подтверждение-по-доставке (RPO)**: слот подтверждается только после
  успешного put в S3; неподтверждённое удерживается слотом на источнике
  ([pgnode/03-wal-receiver.md](../pgnode/03-wal-receiver.md)).
- **Идемпотентность**: имена объектов детерминированы, put одного и того же
  сегмента идемпотентен; ретенция/супервизор идемпотентны по проходу.
- **Данные дороже места**: стоп-семантика и deprovisioning S3-объекты не
  удаляют; удаляющие — только ретенция по политике, wal-sweeper (мусор
  `wal/`), супервизор (мусор `full/<id>/` и сироты по TTL), явная заявка
  `orphan-deletes`.
- **Convergence, не команды**: ensure ролей/слотов/политики — идемпотентная
  сверка на каждом тике; смена policy подхватывается без пересоздания.
