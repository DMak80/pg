# PgNode — агент ноды PG-кластера

Уровень 2. Описание компонента, его обязанностей и перечень функций.
Уровень 3 — документы функций (`01`–`08`), уровень 4 — процессы и
перечисления в `processes/`.

## 1. Назначение

**PgNode** — один процесс Kestrel внутри контейнера PG-ноды (программа
`pgnode` supervisord'а образа `pgworker-node`), self-contained .NET-бинарь.
Он исполняет ВСЕ операции ноды для PgWorker: приём WAL (физическая
репликация из PG своей ноды с выгрузкой в S3), полные `pg_basebackup`,
verify/restore/drill, `pg_dump` схемы, проверку данных ноды, обеспечение
pg_hba и доставку секретов в компоненты ноды. Отдельных процессов/контейнеров
для операций нет — стрим, полный, verify, restore, drill живут в нём
(фоновые задачи одного приложения).

## 2. Обязанности и границы

- **Единственный исполнитель операций ноды**: PgWorker подаёт команды по
  HTTP API (`alias:8080` общей сети воркера и нод), PgNode исполняет и
  публикует статус-снимки в памяти; etcd-ключи операций (`full`, `wal`,
  `restore`, `drill`) пишет ТОЛЬКО воркер. PgNode пишет в etcd только
  собственные статус-ключи: `/pgworker/backups/<C>/<X>/agent/<N>` (lease
  TTL 15 с) и `/pgworker/nodes/<C>/<X>/<N>/status`.
- **Единственный, кто читает/пишет PG-данные ноды локально**:
  `127.0.0.1:5432` своей ноды, `pg_basebackup`, `pg_verifybackup`,
  `pg_dump`, restore-`pg_ctl`; сеть для операций не нужна.
- **Секреты**: env контейнера ноды секретов не несёт; per-cluster данные
  (SPILO_CONFIGURATION, серт/ключ ноды) агент забирает из bootstrap-ключа
  etcd, per-install пароли PG получает по mTLS-каналу от воркера; S3-креды
  и пароль `backup_exec` резолвит сам из etcd
  ([backups/08-secrets.md](../backups/08-secrets.md)).
- **Единственный, кто управляет локальными программами ноды**:
  `supervisorctl start/stop spilo|pg_doorman`, возврат Patroni после
  restore, рендер конфигов doorman, материализация сертов в tmpfs.
- **Не входит**: оркестрация кластера (создание/удаление нод, переезды,
  ротации — PgWorker), запись etcd-контрактов операций, управление docker,
  создание нод/контейнеров, клиентский SQL-трафик (pg_doorman), метрики
  PG-репликации прод-нод (scrape Patroni).

## 3. Функции

| Функция | Содержание | Документ |
|---|---|---|
| Размещение и жизненный цикл процесса | `pgnode` в supervisord, порты `:8080`/`:8081`, сеть, tmpfs, env без секретов, ресурсы, staging/scratch | [01-placement.md](01-placement.md) |
| HTTP API ноды | команды/статусы операций (`/api/backup/full*`, `/api/ops/*`, `/api/node/*`), mTLS-канал доставки, 409/503, cancel | [02-api.md](02-api.md) |
| Приёмник WAL | физическая репликация, сегменты/`.partial`, гвард инкарнации, подтверждение-по-доставке, возобновление, tli/`.history`, ре-доклад инвентаря | [03-wal-receiver.md](03-wal-receiver.md) |
| Полный бэкап | `pg_basebackup`, staging, upload в S3, WAL набора, cancel/ENOSPC | [04-full-backup.md](04-full-backup.md) |
| Verify | скачивание набора, `pg_verifybackup`, отмена | [05-verify.md](05-verify.md) |
| Restore и drill | восстановление PGDATA, fetch-wal, `auto.conf`, `standby.signal`, `pg_ctl`, promote, scratch-дрилл, стоп/возврат Patroni | [06-restore-drill.md](06-restore-drill.md) |
| Операции ноды | `pg_dump` схемы, проба `PG_VERSION`, pg_hba-ensure | [07-node-ops.md](07-node-ops.md) |
| Эксплуатация | статусная машина агента, надёжность, наблюдаемость, риски | [08-operability.md](08-operability.md) |

Точные автоматы — в процессах уровня 4; перечисления —
[processes/enums.md](processes/enums.md).

## 4. Взаимодействие

- **PgWorker → PgNode**: HTTP команды `/api/backup/full*`, `/api/ops/*`,
  `/api/node/*` по `alias:8080`; статусы — GET того же API; отдельно —
  mTLS-канал `:8081` (доставка секретов/применение материала)
  ([02-api.md](02-api.md)).
- **PgNode → etcd**: читает bootstrap-ключ ноды, policy/секреты/слоты;
  пишет только свои статус-ключи агента/приёмника.
- **PgNode → PostgreSQL**: `START_REPLICATION PHYSICAL` по слоту,
  `pg_basebackup`, `pg_verifybackup`, `pg_dump`, restore-`pg_ctl` — всё на
  `127.0.0.1`.
- **PgNode → S3**: put/delete объектов `wal/`, `full/<id>/` (напрямую,
  мимо воркера); read-список для verify/restore/drill.
- **PgNode → supervisord**: управление `spilo`/`pg_doorman`; сокет
  `chmod=0770 root:postgres`.
- **Prometheus → PgNode**: `/metrics` на `:8080` внутри контейнера ноды
  ([metrics/README.md](../metrics/02-dictionary.md) §8).

## 5. Свойства, на которые опираются все функции

- **Истина прогресса — S3**: приёмник подтверждает слот только после
  успешного put; статус-ключ агента — факт сессии, не источник прогресса.
- **Идемпотентность**: put сегмента идемпотентен для тех же байтов; имена
  детерминированы; повторные команды/пины не перезапускают операцию.
- **Изоляция операций**: общий ops-реестр — одна ops-операция на процесс
  (409 на чужую); полный и ops-операция не блокируют друг друга; staging и
  scratch чистятся при любом исходе.
- **Секреты не покидают память/tmpfs**: tmpfs `/run/pgw`, env процесса
  postgres, никаких секретов в argv/GET/логах.
- **Данные неприкосновенны**: дрилл — только scratch; restore — только по
  команде и на пустом PGDATA; снос/остановка — по стоп-семантике.
