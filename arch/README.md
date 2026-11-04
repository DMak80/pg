# pg — архитектурный канон

`arch/` — единственный источник истины (spec-first): код обязан зеркалить
канон, расхождение проекта с `arch/` — блокирующее замечание ревью. Этот
файл — навигация по иерархии; содержание документов здесь не дублируется.

## 1. Уровни иерархии

- **Уровень 1** — обзор системы и каталоги: [00-system.md](00-system.md)
  (назначение, контуры, модель развёртывания, инварианты),
  [00-components.md](00-components.md) (каталог компонентов и доменов),
  [00-technologies.md](00-technologies.md) (сквозные технологии и общие
  технические правила).
- **Уровень 2** — `README.md` компонента/домена: назначение, обязанности и
  границы, перечень функций.
- **Уровень 3** — документы функций/разделов канона (`01`–`NN`).
- **Уровень 4** — процессы и перечисления в `processes/`.

Ссылки — только на родительский, соседний (тот же уровень) и
непосредственно дочерний документ; через уровень не перешагивать. Факт
хранится в одном месте: нужен из другого места — ссылка, не копия.

## 2. Компоненты и домены

| Компонент/домен | Документ |
|---|---|
| **PG-шарды** — шардированные HA-кластеры PostgreSQL, бакеты, сеть | [pg-clusters/README.md](pg-clusters/README.md) |
| **PgWorker** — оркестратор PG-кластеров | [pgworker/README.md](pgworker/README.md) |
| **PgNode** — агент ноды PG-кластера | [pgnode/README.md](pgnode/README.md) |
| **Бэкапы шардов** — полные, WAL, restore/PITR, drill | [backups/README.md](backups/README.md) |
| **Kafka-кластеры** — домен `/kafka/` и клиентский дискавери | [kafka-clusters/README.md](kafka-clusters/README.md) |
| **KafkaWorker** — оркестратор Kafka-кластеров | [kafkaworker/README.md](kafkaworker/README.md) |
| **Valkey-кластеры** — домен `/valkey/` | [valkey-clusters/README.md](valkey-clusters/README.md) |
| **ValkeyWorker** — оркестратор Valkey-кластеров | [valkeyworker/README.md](valkeyworker/README.md) |
| **S3-кластеры** — домен `/s3/` хранилища бэкапов | [s3-clusters/README.md](s3-clusters/README.md) |
| **S3Worker** — оркестратор S3-кластеров | [s3worker/README.md](s3worker/README.md) |
| **EtcdWorker** — оркестратор etcd-контура установки | [etcdworker/README.md](etcdworker/README.md) |
| **AdminPanel** — панель администрирования | [adminpanel/README.md](adminpanel/README.md) |

## 3. Сквозные документы

- [synchronization/README.md](synchronization/README.md) — принципы
  синхронизации распределённого состояния (единый etcd, конвергенция DCS,
  предикаты на первоисточнике, порядок мутаций, флап≠смерть, реестр ошибок).
- [metrics/README.md](metrics/README.md) — единая телеметрия: каркас
  `Shared.Metrics`, словарь имён, коллекторы, таргеты file_sd,
  Prometheus/Grafana/Alertmanager.
- [communications/README.md](communications/README.md) — реестр каналов и
  границ оркестрации: стандартные протоколы, владелец контейнеров,
  допустимые отклонения.
- [operations/README.md](operations/README.md) — общие операционные
  процедуры: вторые инстансы, registry, серт API, E2E; тематические
  runbook-разделы — по ссылкам компонентов.
- [testing/README.md](testing/README.md) — E2E-контур: правила запуска и
  телеметрии, принципы изоляции окружений, чек-лист сценария.
- [stand/README.md](stand/README.md) — docker-стенд проверки решений
  канона (проверочные сценарии, карта «риск → проверка»).

## 4. Структура каталога

```
arch/
├── README.md                  ← вы здесь (навигация)
├── 00-system.md               ← уровень 1: система, контуры, инварианты
├── 00-components.md           ← уровень 1: каталог компонентов и доменов
├── 00-technologies.md         ← уровень 1: общие технические правила
├── pg-clusters/               ← PG-шарды: HA-кластер, топология, etcd-DCS,
│                                  PG/Patroni, клиентский вход, бакеты,
│                                  риски, сеть; процессы и перечисления
├── pgworker/                  ← PgWorker: контракт etcd, размещение,
│                                  жизненный цикл, масштабирование шардов,
│                                  секреты, эвакуация, переезды, API,
│                                  эксплуатация; процессы и перечисления
├── pgnode/                    ← PgNode: размещение, API ноды, приёмник WAL,
│                                  полный, verify, restore/drill, операции
│                                  ноды, эксплуатация; процессы и перечисления
├── backups/                   ← домен бэкапов: полные, WAL, restore/PITR,
│                                  drill, контракт etcd, S3-layout, ретенция,
│                                  секреты, эксплуатация; процессы и перечисления
├── kafka-clusters/            ← домен Kafka: транспорт, ключи, топики,
│                                  координация, клиентский дискавери, отказоустойчивость
├── kafkaworker/               ← KafkaWorker: размещение, жизненный цикл,
│                                  масштабирование, безопасность, топики,
│                                  ребалансировка, API, эксплуатация; процессы
├── valkey-clusters/           ← домен Valkey: транспорт, ключи, координация,
│                                  дискавери, отказоустойчивость
├── valkeyworker/              ← ValkeyWorker: размещение, жизненный цикл,
│                                  конфигурация, безопасность, API, эксплуатация; процессы
├── s3-clusters/               ← домен S3: транспорт, ключи, статусы,
│                                  размещение, платформа, секреты, пул, координация, панель
├── s3worker/                  ← S3Worker: размещение, жизненный цикл,
│                                  масштабирование, безопасность, mesh,
│                                  зонды, API, эксплуатация; процессы
├── etcdworker/                ← EtcdWorker: контракт etcd, размещение,
│                                  членство, надзор, снапшоты, API,
│                                  эксплуатация; процессы и перечисления
├── adminpanel/                ← AdminPanel: архитектура, etcd-контракт,
│                                  панели/API, локальный стенд, roadmap
├── synchronization/           ← принципы синхронизации: контур истины,
│                                  конвергенция, предикаты, мутации, журналы,
│                                  отказоустойчивость, расследование, реестр ошибок
├── metrics/                   ← телеметрия: каркас, словарь, экспозиция,
│                                  коллекторы, хранение, тесты, риски
├── communications/            ← коммуникации: правила, граница оркестрации,
│                                  реестр каналов, отклонения
├── operations/                ← общие runbook-процедуры: вторые инстансы,
│                                  registry, серт API, E2E
├── testing/                   ← E2E-контур: запуск/телеметрия, изоляция,
│                                  чек-лист сценария
├── stand/                     ← docker-стенд проверки канона
├── roadmap/                   ← несделанные задачи и известные дефекты
└── 01-architecture.md, 02-topology.md, 03-prerequisites.md,
    04-deploy-etcd.md, 05-deploy-postgres.md, 07-identify-master.md,
    08-operations.md, 09-troubleshooting.md, 10-four-nodes.md,
    11-bucket-sharding.md, 12-bucket-pitfalls.md, 13-network-security.md,
    14-pgworker.md, kafka-clusters/README.md, kafkaworker/README.md,
    synchronization/README.md, metrics/README.md, backups/README.md,
    valkeyworker/README.md, s3-clusters/README.md, s3worker/README.md,
    communications/02-channel-registry.md, 25-etcdworker.md
                              ← указатели на переработанные документы
                                (сохранены для старых ссылок)
```

## 5. Что вне канона

- Каталоги `configs/` и `scripts/` — артефакты прежнего ручного рецепта
  развёртывания; в каноне живой системы не используются. Развёртывание
  стенда — [stand/README.md](stand/README.md), поставка — `deploy/`,
  `dev-stand/` (вне `arch/`).
- Код роутера бакетов и приложений-потребителей — вне репозитория.
- Отложенные задачи и дефекты — [roadmap/README.md](roadmap/README.md),
  не в этом файле.

## 6. Правила работы с каноном

1. Меняя контракт — сначала обнови `arch/`, затем код.
2. Описывая процесс — заведи отдельный документ со всеми состояниями и
   переходами; енумы — с таблицей всех значений.
3. Не дублируй факты: ссылка на источник, а не копия.
4. Документы описывают текущее и планируемое состояние; история задач
   живёт в git и `docs/superpowers/`.
