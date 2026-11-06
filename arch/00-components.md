# 00. Каталог компонентов

Уровень 1. Перечень компонентов системы, их роль в одном-двух предложениях и
ссылка на документ уровня 2 с полным описанием обязанностей, функций и
процессов. Содержание этих документов здесь не дублируется.

## 1. Управляющие сервисы

| Компонент | Роль | Документ |
|---|---|---|
| **AdminPanel** | Веб-панель (React SPA + ASP.NET Core API): read-only инспекция etcd, шардирования, HA, алертов и S3; мутации (кластеры, шарды, переезды, ротации, перезапуск воркеров) проксирует в HTTP API воркеров. В etcd пишет только серверные серты API воркеров (`/workers/api_tls/*`). | [adminpanel/README.md](adminpanel/README.md) |
| **PgWorker** | Оркестратор шардированных PG-кластеров: provisioning/deprovisioning, надзор нод, эвакуация и онлайн-переезды бакетов, добавление/снятие шарда, ротации per-cluster секретов, adoption, репарация переездов; хозяин подсистемы бэкапов шардов. Несколько инстансов координируются lease-клэймами в etcd. | [pgworker/README.md](pgworker/README.md) |
| **KafkaWorker** | Оркестратор Kafka-кластеров (KRaft, docker plain/swarm): подъём и демонтаж, per-cluster SASL-креды и CA, автосинк топиков, reassignment партиций. | [kafkaworker/README.md](kafkaworker/README.md) |
| **ValkeyWorker** | Оркестратор Valkey-кластеров (standalone, `nodes=1`): подъём и демонтаж, per-cluster ACL-креды, надзор и ротации без рестартов. | [valkeyworker/README.md](valkeyworker/README.md) |
| **S3Worker** | Оркестратор S3-кластеров (ownS3): инстансы по placement-селекторам, bucket per-install, mesh-репликация, deny-list, публикация `endpoints`. | [s3worker/README.md](s3worker/README.md) |
| **EtcdWorker** | Хозяин etcd-контура установки: поддержание состава managed-узлов и кворума (managed-узлы — образ агента EtcdNode). | [etcdworker/README.md](etcdworker/README.md) |

## 2. Агенты на нодах

| Компонент | Роль | Документ |
|---|---|---|
| **PgNode** | Агент внутри контейнера PG-ноды (один процесс Kestrel): приёмник WAL (физическая репликация из PG с выгрузкой в S3), полные бэкапы (стриминговый клиент `BASE_BACKUP` → единый tar-объект в S3), verify/restore/drill, hba-гвард, `pg_dump` схемы, проверка данных ноды; API ноды для PgWorker. | [pgnode/README.md](pgnode/README.md) |
| **EtcdNode** | Агент внутри контейнера каждого managed-узла etcd-контура (etcd + агент под supervisord): DR-слепки etcd (своего узла) с выгрузкой в etcd-backup S3-кластер — планово и по важным изменениям (пульсы сервисов не триггерят), обслуживание контура (compact/defrag). | [etcdnode/README.md](etcdnode/README.md) |

## 3. Кластеры данных и домены

| Компонент | Роль | Документ |
|---|---|---|
| **PG-шард (HA-кластер)** | Ноды `pgworker-node` (Spilo: PostgreSQL 18 + Patroni 4.x; pg_doorman — пул `:6432` на каждой ноде; PgNode — агент ноды); etcd — внешний DCS (выборы лидера, динамический конфиг); один primary и R−1 реплик, streaming replication, автоматический failover; управление — PgWorker. | [pg-clusters/README.md](pg-clusters/README.md) |
| **Бакеты и шардирование** | Фиксированное число N схем-бакетов, распределённых по шардам; онлайн-переезд бакета (логическая репликация и cutover), добавление пустых и снятие пустых шардов. | [pg-clusters/11-buckets.md](pg-clusters/11-buckets.md) |
| **Kafka-кластеры** | Контроль-плейн `/kafka/`: KRaft-брокеры, топики, per-cluster SASL и CA, клиентский дискавери endpoints и кредов. | [kafka-clusters/README.md](kafka-clusters/README.md) |
| **Valkey-кластеры** | Контроль-плейн `/valkey/`: standalone-нода, ACL-креды, клиентский дискавери endpoints. | [valkey-clusters/README.md](valkey-clusters/README.md) |
| **S3-кластеры** | Контроль-плейн `/s3/`: пул ownS3-инстансов с привязкой к хостам, mesh-репликацией и публикацией endpoints; хранилище бэкапов шардов и etcd-слепков. | [s3-clusters/README.md](s3-clusters/README.md) |

## 4. Внешние потребители

Приложения и роутер бакетов — внешние потребители системы: читают адреса и
креды из etcd (клиентский дискавери) и ходят в кластеры штатными протоколами.
Их код вне этого репозитория, в состав компонентов они не входят.
