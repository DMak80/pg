# S3Worker — оркестратор S3-кластеров

Уровень 2. Описание компонента, его обязанностей и перечень функций.
Уровень 3 — документы функций (`01`–`08`), уровень 4 — процессы и
перечисления в `processes/`.

## 1. Назначение

**S3Worker** — фоновый сервис (.NET 10), исполнительная сторона
декларативного контракта [s3-clusters/README.md](../s3-clusters/README.md): панель
AdminPanel **заявляет** кластер через **HTTP API воркера**
([07-api.md](07-api.md); `state=NOT_INITIALIZED`) — воркер **поднимает**
инстансы (volume данных + контейнер ownS3 на хосте из placement-селектора,
репликационный PKI из etcd — [s3-clusters/06-secrets.md](../s3-clusters/06-secrets.md)), создаёт bucket
per-install (etcd-backup кластер — дополнительно фиксированный
etcd-слепковый bucket, [s3-clusters/05-platform.md](../s3-clusters/05-platform.md)), доводит граф
репликации (links → join) и deny-list до инстансов, пишет `endpoints` и
снимает `state`; перевод в `TO_REMOVE` — воркер демонтирует кластер
полностью.

Домен — пул S3-хранилища бэкапов: данные инстанса живут только на его
volume, копия — лишь у инстансов, связанных в `links`
([05-mesh-governance.md](05-mesh-governance.md)).

## 2. Обязанности и границы

- **Единственный писатель** префиксов `/s3/` и `/s3worker/` (исключение —
  `/workers/api_tls/s3worker`: серверный серт API пишет панель). Панель и
  сиды ходят только через API воркера.
- **Единственный**, кто создаёт/удаляет контейнеры инстансов
  `s3w-<C>-<I>` и volume данных, а также выпускает и хранит per-cluster
  секреты и репликационный PKI; фактическое состояние docker сверяет
  инспекцией, состояние системы — в etcd.
- **Несколько инстансов работают одновременно**: координация — lease-клэймы
  в etcd (`/s3worker/`), смерть контролирующего инстанса не роняет процессы
  (takeover ≤ TTL 15 с + тик); операции идемпотентны, состояние переживает
  смерть контроллера (etcd).
- **Не входит**: механика mesh-репликации ownS3 (канон
  [owns3/07-replication.md](../../../OwnS3/arch/owns3/07-replication.md)),
  панель домена S3 (`adminpanel/02` §12), клиент пула (PgWorker — читатель `endpoints`;
  участником контракта не является: в `/s3/`/`/s3worker/` не пишет и
  docker-объектов домена не создаёт), плановая ротация ключей
  репликации и CA ([s3-clusters/06-secrets.md](../s3-clusters/06-secrets.md); [roadmap/s3.md](../roadmap/s3.md)),
  enforcement квот диска (`resources.disk` — инфо-поле).

## 3. Функции

| Функция | Содержание | Документ |
|---|---|---|
| Модель размещения и инстансы | контейнер ownS3 и volume данных, S3-порт, placement-селекторы, advertised-адреса, сеть, режимы Plain/Swarm, два инстанса воркера | [01-placement.md](01-placement.md) |
| Жизненный цикл кластера | создание, демонтаж, надзор инстансов, bucket'ы инстансов | [02-cluster-lifecycle.md](02-cluster-lifecycle.md) |
| Масштабирование состава | добавление и вывод инстанса, применение ресурсов | [03-node-scaling.md](03-node-scaling.md) |
| Безопасность и ротации | креды клиентской грани, репликационный PKI, env-доставка, ротация кредов, deny-list | [04-security.md](04-security.md) |
| Доводка mesh | приведение графа репликации и deny-list инстансов к декларации | [05-mesh-governance.md](05-mesh-governance.md) |
| Зонд связности и синхронности | периодические и внеплановые пробы, статусы наблюдения | [06-sync-probes.md](06-sync-probes.md) |
| Внешний HTTP API | приём мутаций панели и сида, mTLS, дискавери, серверный серт, `GET /api/config` | [07-api.md](07-api.md) |
| Эксплуатация | классификация тика, надёжность, наблюдаемость, конфигурация, риски | [08-operability.md](08-operability.md) |

Точные автоматы состояний — в процессах уровня 4, на которые ссылается
каждая функция; классификация тика и перечисления —
[08-operability.md](08-operability.md) §1.

## 4. Взаимодействие

- **AdminPanel → S3Worker**: только HTTP API (`/api`, mTLS) —
  [07-api.md](07-api.md), контракт мутаций —
  [adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md) §12;
  панель etcd домена S3 только читает ([s3-clusters/09-panel.md](../s3-clusters/09-panel.md)).
- **S3Worker → etcd**: единственный источник состояния домена (`/s3/`,
  `/s3worker/`) — контракт ключей
  [s3-clusters/README.md](../s3-clusters/README.md).
- **S3Worker → docker / инстансы ownS3**: управление контейнерами и
  админ-грань инстансов (`/healthz`, bucket'ы, replication/join/targets/
  denylist/probe) — только штатные протоколы, границы оркестрации
  [25-component-communications.md](../25-component-communications.md).
- **PgWorker → etcd**: клиент пула (`endpoints`, `secrets`-ключ `backups`,
  `/s3worker/portalloc/<C>`) — [s3-clusters/07-client-pool.md](../s3-clusters/07-client-pool.md); bucket
  per-install и раскладка объектов бэкапов — [19-backups.md](../19-backups.md)
  §5/§7.
- **Prometheus → S3Worker**: `/metrics` на порту Kestrel `:8080` —
  [18-metrics.md](../18-metrics.md).

## 5. Свойства, на которые опираются все функции

- Идемпотентность каждого шага: повторная сверка факта (контейнер есть?
  health-грань отвечает? bucket существует? правило mesh стоит? `state` уже
  снят?) — [08-operability.md](08-operability.md) §2.
- Journal-before-manipulations, фиксированный порядок записей, клэймы и
  txn-сравнения — [17-synchronization-principles.md](../17-synchronization-principles.md)
  (S5–S7), [08-operability.md](08-operability.md) §2.
- Декларативная модель: отсутствие `config.state` = Active-кластер;
  `NOT_INITIALIZED`/`TO_REMOVE` — заявки панели; состояния инстанса и
  кластера — [08-operability.md](08-operability.md) §1,
  [processes/enums.md](processes/enums.md).
- Секреты и PKI — per-cluster в etcd и переживают пересоздание контейнеров
  ([04-security.md](04-security.md) §1–§2); env инстанса — транспорт
  доставки значений, не хранилище.
- Локальное состояние (volume инстанса) — единственная копия данных, пока
  у инстанса нет связей в `links`; гарантии строятся от реплицированных
  источников (etcd + mesh), не от локального диска —
  [08-operability.md](08-operability.md) §3.
