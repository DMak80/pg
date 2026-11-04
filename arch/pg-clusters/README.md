# PG-шарды — домен шардированных HA-кластеров PostgreSQL

Уровень 2. Описание домена: назначение, обязанности и границы, перечень
функций. Уровень 3 — документы функций (`01`–`13`), уровень 4 — процессы и
перечисления в `processes/`.

## 1. Назначение

**PG-шард (HA-кластер)** — единица данных домена: кластер PostgreSQL из
нескольких нод с автоматическим выбором лидера. Приложение работает с
кластером `<C>` как с одной базой `<C>`: внутри неё фиксированное число
схем-бакетов `bucket_<i>`, разложенных по шардам. **Шард** `<X>` — отдельный
HA-кластер (Spilo + Patroni + pg_doorman + PgNode) на R нодах; бакеты
одного шарда живут в его базе `<C>`, бакеты разных шардов друг про друга не
знают.

Нода шарда — контейнер `pgw-<C>-<X>-<n>` образа `pgworker-node`:

- **PostgreSQL 18** под управлением **Patroni** (Spilo): один primary и
  реплики, streaming replication, автоматический failover;
- **pg_doorman** — пулер `:6432` на каждой ноде, единственный клиентский
  вход приложений; бэкенд — только локальный `127.0.0.1:5432`;
- **PgNode** — агент ноды (приёмник WAL бэкапов, операции ноды);
- **etcd** — внешний DCS и контрол-плейн: Patroni проводит выборы лидера,
  PgWorker хранит декларации и адресацию.

Управление — декларативное: панель AdminPanel заявляет кластер/шард/переезд
через HTTP API **PgWorker**, воркер пишет декларации в etcd, поднимает и
надзирает ноды, исполняет переезды. Ноды и приложения в etcd не пишут
декларации: домен `/clusters/` принадлежит воркеру.

## 2. Обязанности и границы

- **Модель данных домена**: одна база `<C>` на кластер, N схем-бакетов,
  раскладка бакетов по шардам (`routing`), онлайновый переезд бакета.
  Константы (`buckets`, `dbname`) фиксируются при создании и не меняются.
- **HA-механика**: primary/replica, synchronous replication с per-cluster
  `synchronous_mode_strict` (дефолт durability-first), выборы и failover
  через etcd-DCS, защита от split-brain, плановый switchover.
- **Клиентский вход**: `pg_doorman` на каждой ноде, дискавери мастера и
  параметров подключения из etcd, TLS и аутентификация единственной
  app-роли.
- **Сеть и безопасность**: зоны, firewall-матрица, TLS-каналы,
  аутентификация, модель угроз (компрометация ноды/хоста).
- **Эксплуатация**: определение мастера, switchover/failover, rebuild
  ноды, диагностика, мониторинг, число реплик.
- **Не входит**: оркестрация (provisioning, надзор, переезды, ротации,
  бэкапы) — [pgworker/README.md](../pgworker/README.md); агент ноды —
  [pgnode/README.md](../pgnode/README.md); подсистема бэкапов —
  [backups/README.md](../backups/README.md); etcd-контур установки —
  [etcdworker/README.md](../etcdworker/README.md); панель и её API —
  [adminpanel/README.md](../adminpanel/README.md); код роутера бакетов —
  вне репозитория (приложение-потребитель).

## 3. Функции

| Функция | Содержание | Документ |
|---|---|---|
| HA-архитектура | PostgreSQL/Patroni/Spilo/etcd DCS/pg_doorman, роли и репликация, failover и split-brain, switchover, отказоустойчивость | [01-ha-architecture.md](01-ha-architecture.md) |
| Топология и размещение | нода = контейнер, порты и публикации, тома и диски, сети, portalloc, именование и адресация | [02-topology.md](02-topology.md) |
| Подготовка хостов | требования к docker-хостам, время, диски, sysctl, образы и локальный registry | [03-prerequisites.md](03-prerequisites.md) |
| etcd как DCS | роль внешнего etcd, контур установки, ключи `/service/<scope>/`, адреса для нод, отказы кворума | [04-etcd-dcs.md](04-etcd-dcs.md) |
| PostgreSQL и Patroni | версия и параметры PG, SPILO_CONFIGURATION, sync-режим, failover slots, bootstrap и конвергенция DCS | [05-postgres-patroni.md](05-postgres-patroni.md) |
| Клиентский вход | pg_doorman: пул, режим transaction, TLS, auth_query, дискавери и DSN, ограничения пулинга | [06-client-access.md](06-client-access.md) |
| Кто мастер | источники истины и предикаты: Patroni REST, DCS, мастер-ключ; сверка и коррекция ключа | [07-identify-master.md](07-identify-master.md) |
| Эксплуатация | switchover, failover, возврат и rebuild ноды, обслуживание, мониторинг | [08-operations.md](08-operations.md) |
| Диагностика | лаг реплики, отсутствие failover, split-brain, отказ etcd, переполнение диска, doorman, сироты | [09-troubleshooting.md](09-troubleshooting.md) |
| Число нод шарда | `replicas` 1..26 (дефолт 2), отказоустойчивость, синхронные реплики, выбор числа нод | [10-replica-count.md](10-replica-count.md) |
| Бакеты и шардирование | константы N, routing/status, жизненный цикл шарда, механика переезда и отката, отказы | [11-buckets.md](11-buckets.md) |
| Реестр рисков домена | закрытые решения P1–P23: переезды, контрол-плейн, пулер, сеть, модель шардирования | [12-bucket-pitfalls.md](12-bucket-pitfalls.md) |
| Сеть и безопасность | зоны, firewall-матрица, TLS-каналы, аутентификация, модель угроз | [13-network-security.md](13-network-security.md) |

Точные автоматы процессов HA-контура — в `processes/` уровня 4;
перечисления — [processes/enums.md](processes/enums.md).

## 4. Взаимодействие

- **PgWorker → ноды**: docker Engine API (создание/надзор/rebuild
  контейнеров) и Patroni REST `:8008` (пробы, `/primary`, конвергенция
  конфига) — [pgworker/01-placement.md](../pgworker/01-placement.md).
- **PgWorker → PgNode**: HTTP API операций ноды по `alias:8080` общей сети
  воркера и нод; mTLS-канал доставки секретов `:8081`
  ([pgnode/02-api.md](../pgnode/02-api.md),
  [communications/02-channel-registry.md](../communications/02-channel-registry.md) №22–23).
- **Patroni ↔ etcd**: DCS-ключи `/service/<scope>/` (лидер, члены,
  инициализация, конфиг) — [04-etcd-dcs.md](04-etcd-dcs.md).
- **Patroni-callback → etcd**: мастер-ключ
  `/clusters/<C>/shards/<X>/master` (lease), PgWorker сверяет и корректирует
  его по факту — [07-identify-master.md](07-identify-master.md).
- **Приложение → pg_doorman** мастер-ноды (адрес `host:port` из master-ключа,
  опубликованный порт portalloc); дискавери — etcd (`config`, `routing`,
  `master`, `app_params`) — [06-client-access.md](06-client-access.md),
  [11-buckets.md](11-buckets.md) §3.
- **Ноды шардов ↔ ноды шардов** `:5432`: подписки переездов бакетов
  (multi-host conninfo, `target_session_attrs=read-write`,
  `bucket_mover`) — [11-buckets.md](11-buckets.md) §4.
- **Prometheus → Patroni** `:8008/metrics`, **→ PgNode** `:8080/metrics`;
  таргеты — file_sd из portalloc ([metrics/README.md](../metrics/02-dictionary.md) §5,
  §5.4).

## 5. Свойства, на которые опираются все функции

- **Один primary на scope.** Кворум etcd — единственный арбитр лидерства:
  без lease-продления нода обязана уйти в read-only/остановиться. Два
  primary невозможны, пока жив кворум DCS.
- **Мастер-ключ — маршрутизация и кэш, не истина о роли.** Предикаты «кто
  мастер» строятся на прямых пробах Patroni REST (`/primary`); значение
  ключа может отставать и корректируется сверкой
  ([synchronization/README.md](../synchronization/README.md) S4).
- **Данные — только на локальных дисках нод.** etcd хранит метаданные и
  маршрутизацию, но не данные; потеря etcd не теряет данные, потеря диска
  ноды восстанавливается репликацией/бэкапом.
- **Durability-first по умолчанию.** `synchronous_mode=true` и
  `synchronous_mode_strict=true` (отсутствие поля = true): strict-кластер
  без sync-standby блокирует запись, а не деградирует в асинхронность.
- **Единая БД, N — навсегда.** Все бакеты — схемы одной базы `<C>`;
  смена `buckets` ломает хеш-маппинг тенантов и запрещена.
- **app ≠ owner.** Приложение пишет только DML/`USAGE,UPDATE` на sequences;
  DDL — миграционная роль-владелец; заморозка переезда отбирает права у
  app-роли.
- **Идемпотентность и декларативность.** Любая мутация ноды/бакета
  перепроверяет факт и переживает повтор; состояние — в etcd, не в памяти
  процесса ([pgworker/08-operability.md](../pgworker/08-operability.md)).
