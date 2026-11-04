# PgWorker — оркестратор шардированных PG-кластеров

Уровень 2. Описание компонента, его обязанностей и перечень функций.
Уровень 3 — документы функций (`00`–`08`), уровень 4 — процессы и
перечисления в `processes/`.

## 1. Назначение

**PgWorker** — фоновый сервис (.NET 10), который по состоянию в etcd
управляет жизненным циклом шардированных HA-кластеров PostgreSQL через
docker (plain / docker swarm). Это исполнительная сторона декларативного
контракта: панель AdminPanel **заявляет** кластер и изменения через **HTTP
API воркера** ([07-api.md](07-api.md)) — воркер записывает декларацию
(`config.state=NOT_INITIALIZED`; контракт панели —
[adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md) §9, перенесён
из репозитория AdminPanel), **поднимает** ноды, инициализирует
БД/роли/схемы бакетов и переводит кластер в рабочее состояние; перевод в
`TO_REMOVE` — воркер аккуратно демонтирует кластер.

**Ответственность изменений etcd**: префиксы `/clusters/`, `/pgworker/` и
заявки `/service/<C>-<X>/request_*` пишет ТОЛЬКО PgWorker (панель и сиды
ходят через его API); панель etcd только читает.

## 2. Обязанности и границы

- **Единственный писатель** префиксов `/clusters/`, `/pgworker/` и заявок
  `/service/<C>-<X>/request_*`; исключение — `/workers/api_tls/pgworker`
  (серверный серт API пишет панель). Панель и сиды ходят только через API
  воркера.
- **Единственный**, кто создаёт/удаляет контейнеры нод, сеть `pgw-net-<C>`
  и тома; фактическое состояние docker сверяет инспекцией, состояние
  системы — в etcd.
- **Несколько инстансов работают одновременно**: координация — lease-клэймы
  в etcd (`/pgworker/`); смерть контролирующего инстанса не роняет процессы
  (takeover ≤ TTL 15 с + тик); операции идемпотентны, состояние переживает
  смерть контроллера (etcd + сами ноды).
- **Хозяин подсистемы бэкапов шардов**: оркестрация полных `pg_basebackup`,
  restore/drill, политика бэкапов; механика приёмника WAL, layout S3 и
  контракт `/pgworker/backups/*` — [19-backups.md](../19-backups.md).
- **Не входит**: переезды бакетов скриптовым путём (в живой системе путь
  мутаций один — панель → API воркера → заявки в etcd; скрипты
  [11-bucket-sharding.md](../11-bucket-sharding.md) §4.5/§5 — инструмент
  исследований и стендов, к кластерам под управлением PgWorker не
  применяется); балансировка по метрикам; управление etcd-слоем (зона
  [etcdworker/README.md](../etcdworker/README.md)); тонкий RBAC Engine API
  (authz-плагины Docker CE); метрики PG-репликации прод-нод (scrape Patroni —
  [18-metrics.md](../18-metrics.md) §5.4); enterprise-доработки.

## 3. Функции

| Функция | Содержание | Документ |
|---|---|---|
| Контракт etcd и координация | читаемые/пишемые ключи домена, префикс `/pgworker/` (клэймы, журнал, portalloc, заявки), модель опроса и инварианты мутаций | [00-etcd-contract.md](00-etcd-contract.md) |
| Модель размещения и узлы | образ `pgworker-node` (Spilo + Patroni + pg_doorman + PgNode), роли и порты, тайминги Patroni и PGTune, режимы Plain/Swarm, транспорт Engine API, placement и portalloc, сеть | [01-placement.md](01-placement.md) |
| Жизненный цикл кластера | создание, демонтаж, надзор нод, ускорение failover, конвергенция DCS, усыновление Active-кластера | [02-cluster-lifecycle.md](02-cluster-lifecycle.md) |
| Масштабирование шардов | добавление и снятие отдельного шарда живого кластера (guard'ы G1–G7) | [03-shard-scaling.md](03-shard-scaling.md) |
| Секреты и ротации | per-cluster и per-install креды, TLS-транспорт, доставка секретов в ноду каналом агента, ротация без остановки записи | [04-security.md](04-security.md) |
| Аварийная эвакуация бакетов | перевод бакетов полностью умершего шарда на живые, карантин вернувшегося | [05-bucket-evacuation.md](05-bucket-evacuation.md) |
| Плановые переезды бакетов | move/rollback/finalize/abort по заявкам, cutover, репарация брошенных статусов | [06-bucket-moves.md](06-bucket-moves.md) |
| Внешний HTTP API | приём мутаций панели и сида, mTLS, дискавери, restart, `GET /api/config` | [07-api.md](07-api.md) |
| Эксплуатация | классификация тика, клэймы и takeover, надёжность, наблюдаемость, конфигурация, риски | [08-operability.md](08-operability.md) |

Точные автоматы состояний — в процессах уровня 4, на которые ссылается
каждая функция; перечисления — [processes/enums.md](processes/enums.md).

## 4. Взаимодействие

- **AdminPanel → PgWorker**: только HTTP API (`/api`, mTLS) —
  [07-api.md](07-api.md), контракт мутаций —
  [adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md)
  (глава PgWorker, §9).
- **PgWorker → etcd**: единственный источник состояния домена
  (`/clusters/`, `/pgworker/`) — контракт ключей
  [00-etcd-contract.md](00-etcd-contract.md).
- **PgWorker → docker / PostgreSQL / Patroni**: управление контейнерами,
  SQL-операции и REST-пробы Patroni — только штатные протоколы, границы
  оркестрации [25-component-communications.md](../25-component-communications.md).
- **PgWorker → PgNode**: HTTP API приёмника WAL (`alias:8080`: операции
  бэкапов, `pgdump`, `pgdata`) и mTLS-канал доставки секретов (`:8081`) —
  [19-backups.md](../19-backups.md) §2/§3, [04-security.md](04-security.md) §4.
- **Приложения → etcd**: клиентский дискавери (`dsn`, `app_user`/
  `app_password`, `app_params`) —
  [11-bucket-sharding.md](../11-bucket-sharding.md) §3.
- **Prometheus → PgWorker / PgNode**: `/metrics` на порту Kestrel `:8080`
  и в контейнере ноды — [18-metrics.md](../18-metrics.md).

## 5. Свойства, на которые опираются все функции

- Идемпотентность каждого шага: повторная сверка факта (контейнер есть?
  БД/схема есть? routing уже переведён?) — [08-operability.md](08-operability.md) §2.
- Journal-before-manipulations, фиксированный порядок записей, клэймы и
  txn-сравнения — [17-synchronization-principles.md](../17-synchronization-principles.md)
  (S5–S7), [08-operability.md](08-operability.md) §2.
- Декларативная модель: отсутствие `config.state` = Active-кластер;
  `NOT_INITIALIZED`/`TO_REMOVE` — заявки панели; состояния нод и заявок —
  [processes/enums.md](processes/enums.md).
- «Воркер — хозяин»: каждый тик сверяет декларацию с фактом (docker,
  HA-контур) и чинит расхождения сам; оператор привлекается только там,
  где автоматика рисковала бы потерять СОХРАННЫЕ данные.
- Per-cluster сеть `pgw-net-<C>` и advertised-адреса, резолвимые клиентами,
  — [01-placement.md](01-placement.md) §5.
- Секреты: etcd — источник истины для per-cluster кредов, PG следует за
  etcd финальным ALTER'ом; env контейнера ноды секретов не несёт —
  [04-security.md](04-security.md).
