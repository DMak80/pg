# Телеметрия: коллекторы доменных метрик

Уровень 3. Коллекторы Kafka и Valkey. Родитель — [README.md](README.md);
соседние документы — [01-framework.md](01-framework.md),
[02-dictionary.md](02-dictionary.md), [03-exposure.md](03-exposure.md),
[05-storage.md](05-storage.md), [06-testing.md](06-testing.md),
[07-risks.md](07-risks.md).

## 1. Kafka

Фоновый hosted-сервис `KafkaWorker.App` (тик `KafkaWorker:Metrics:
CollectIntervalSec`, default 30): по Active-кластерам (снапшот etcd) через
`IKafkaAdminClientFactory` — `ListConsumerGroups` + `ListConsumerGroupOffsets`
+ `ListOffsets` (watermarks) → `kafka_consumer_lag`; describe-метаданные
топиков → `kafka_under_replicated_partitions`. Результаты пишутся в
ObservableGauge-стейт; ошибка сбора не валит тик (обновляется только
`kafka_collector_last_success_timestamp_seconds`). Один AdminClient-коннект
на тик по кластеру, таймаут короткий (seam-фабрика уже 10 с); сбор — вне
клэймов (read-only, безопасен параллельно любым процессам).

## 2. Valkey

Фоновый hosted-сервис `ValkeyWorker.App` (тик `ValkeyWorker:Metrics:
CollectIntervalSec`, default 30; `<=0` → 30 + warning-лог): по Active-кластерам
(снапшот etcd `/valkey/clusters/`, парсер ValkeySnapshotParser; `Config.State == null`
— невыполненные заявки не трогаем) с полными дискавери-кредами (`admin_user`/
`admin_password`). Адреса нод — отдельный read-only Range `/valkeyworker/portalloc/`
(канон `NodeAddress`); нода кластера без portalloc-записи — пропуск (лестница E9 —
забота надзора C, коллектор только читает). Хост пробы —
`AdvertisedClientHost ?? portalloc.host` (правило advertised
[valkeyworker/01-placement.md](../valkeyworker/01-placement.md) §2).

Сбор ноды — ОДНА RESP-проба `INFO all` за тик (аналог «одного AdminClient-коннекта
за тик» §1): все секции одним bulk-кадром, короткоживущий TCP с таймаутом клиента
(5 с), admin-кред. Ошибка ноды → тик кластера неуспешен (warning-лог), остальные
кластеры тика собираются. `valkey_collector_last_success_timestamp_seconds`
обновляется ТОЛЬКО при полном успехе всех (cluster, node)-проб тика (консервативно;
пустой список кластеров = успех). Сбор — вне клэймов (read-only, безопасен
параллельно любым процессам).

Без backoff-механизма (отличие от Kafka): RESP-миниклиент — лёгкая короткоживущая
TCP-проба (не тяжёлый AdminClient); лежачая нода стоит один connect-таймаут за тик,
отдельный backoff-стейт не нужен (YAGNI). Результаты пишутся в ObservableGauge-стейт
[02-dictionary.md](02-dictionary.md) §6 (`ValkeyMetricsState`, материализация измерений под lock — паттерн §1);
`UpdateCluster` затирает предыдущие записи кластера — ушедшие ноды не копятся.
