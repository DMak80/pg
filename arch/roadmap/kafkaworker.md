# Roadmap: сервис KafkaWorker

Отложенные задачи KafkaWorker и kafka-домена панели (out of scope текущей
спецификации `docs/superpowers/2026-08-30-kafka-admin-worker/spec.md` §8;
канон — [../15-kafka-clusters.md](../15-kafka-clusters.md) (контракт etcd) и
[../16-kafkaworker.md](../16-kafkaworker.md) (воркер)).

## Задачи

- **`t03-kafka-security`** — TLS (SASL_SSL), ACL/authorization, разделение
  admin/app кредов (арх-канон 16 §2.1: сейчас один per-cluster SASL-кред
  для всех ролей, CONTROLLER-listener PLAINTEXT внутри закрытой сети);
  сюда же — транспортная безопасность HTTP API KafkaWorker (arch/16 §1.1):
  mTLS вместо голого `X-Api-Key` в закрытой сети.
- **`t04-kafka-metrics`** — Prometheus-метрики воркера и панели (лаги, USR,
  фазы процессов, клэймы).
- **`t06-kafka-node-regen`** — rolling-перегенерация существующих брокеров
  с новыми ресурсами (лимиты cpu/mem) и новыми server-props.
- **`t09-kafka-worker-health`** — честная наблюдаемость здоровья воркера:
  sticky-`StatusError` циклов, флейд активных проб, единая правда для панели.
  Три дефекта одного корня «/healthz лжёт о живом воркере» (диагностика
  живого стенда 2026-08-31: `as-kafkaworker` unhealthy 18+ мин при живом
  heartbeat-lease и успешных тиках «active test: ok»; `/healthz` → 503):
  1. **Sticky-`StatusError`**: `ReconcileLoop.ExecuteAsync`
     (`src/KafkaWorker.App/Loops/ReconcileLoop.cs:52`) записывает
     `StatusError` при ошибке тика, но никогда не сбрасывает при
     последующих успешных тиках; тот же паттерн в
     `SnapshotLoop.cs:60` (проверить и `KeepaliveLoop`). Один transient-сбой
     (например, пересоздание etcd-контейнера — DNS-имя пропало на секунды)
     => `/healthz` отдаёт 503 «service has error» бесконечно до рестарта
     воркера, docker-HEALTHCHECK копит `FailingStreak` — при фактически
     живом воркере. Фикс: успешный тик сбрасывает `StatusError` в
     `Success` (health отражает ПОСЛЕДНЕЕ состояние цикла, а не первый
     сбой); тест: transient-сбой → восстановление → чек Healthy без
     рестарта.
  2. **Флейд активных проб + исключения из чеков**: `ServiceProbes.
     EtcdReachableAsync` (`src/KafkaWorker.App/HealthChecks/ServiceProbes.cs`)
     ждёт `Result.Failed`, но сетевые исключения при открытии новых
     соединений (`HttpRequestException: Name or service not known
     (etcd:2379)` — .NET DNS-клиент флейпит, при этом `curl`/`getent` из
     того же контейнера резолвят стабильно 10/10) летят из `RangeAsync`
     наружу и роняют чек исключением (`DefaultHealthCheckService[103]`
     стектрейсы в логе) вместо `Degraded` с данными секции. Фикс: пробы
     оборачивать в catch → `Result.Failed` (чек всегда отдаёт структуру,
     не исключение) + разобраться с DNS-флейдом (точная диагностика в
     spec: PooledConnectionLifetime/`SocketsHttpHandler`-опции, поведение
     A/AAAA против Docker embedded DNS 127.0.0.11).
  3. **Единая правда для панели**: панель судит о живости воркера по
     heartbeat-ключу `/kafkaworker/instances/*` (lease жив) и молчит, когда
     docker-health unhealthy — docker красный, панель зелёная. Решить
     контракт (арх-канон 16, spec §8 «задача 24»): воркер публикует
     агрегированное самочувствие в etcd (heartbeat расширяется статусом
     циклов/проб) и панель алертит (`worker-degraded`), либо панель пробит
     `/healthz` напрямую по сети стенда — выбор способа в spec. Критерий:
     degraded/unhealthy воркер виден панели как алерт ≤ 2 тиков, здоровый
     после восстановления — гаснет; docker-health и панель больше не
     расходятся.
  Канон-контракт: [../16-kafkaworker.md](../16-kafkaworker.md) (health §8);
  паттерн сброса ошибки тика взять из PgWorker-циклов после adopt-repair
  (живой прогон 2026-08-31, `docs/superpowers/2026-08-31-pgworker-adopt-repair/`).
  Зависимостей нет; e2e — стенд `dev-stand/adminpanel/checks/` (failover
  etcd-контейнера как transient-стимул).
- **`t10-kafka-discovery-integration`** — интеграция Kafka-клиента Puzzle
  (`Infrastructure.App.Kafka`, Confluent) с библиотекой дискавери HA.Kafka
  (t05): BootstrapServers/SASL-креды из etcd-снапшота вместо
  `ConnectionStrings:Kafka`; реакция на событие Updated — переподключение
  продюсеров/консюмеров при смене endpoints/кредов (вкл. ротацию app_password,
  arch/16 §5 H); Aspire-ветка без etcd (локальная разработка — источник
  топологии из ConnectionStrings, по образцу переключателя `Database:Source`
  у HA.Db). Канон-контракт — [../15-kafka-clusters.md](../15-kafka-clusters.md)
  §5–§6.
- **`t11-kafka-probe-churn`** — kafka-пробы панели не должны жечь CPU на
  недоступных брокерах: churn Confluent-клиентов «один на вызов» +
  reconnect-шторм librdkafka + блокирующий `Dispose` (инцидент as-adminpanel
  2026-09-02: ~99% ядра, ~2600 строк лога/мин).
  **Инцидент** (диагностика живого стенда 2026-09-02, `as-adminpanel`): в etcd
  оставался Active kafka-кластер с endpoints `host.docker.internal:16003–16005`,
  брокеры порты не слушали (connection refused за 1 мс). Панель стабильно
  ~99% CPU (docker stats; per-thread `/proc`: managed-поток `.NET Long
  Running` — ~92% всего CPU процесса), после удаления кластера из etcd —
  ~0%. Доказательства: core-dump (`createdump` → `dotnet-dump`): TP-воркер
  блокирован в `AdminClient.Dispose` → `Task.InternalWait` (вызов из
  `ConfluentKafkaRuntimeProbeClient.ListGroupsAsync`),
  Long-Running-потоки `AdminClient.StartPollTask`/`Producer.StartPollTask`
  крутят `rd_kafka_poll`; лог `rdkafka#producer-N` (N дорос до 65 за 12 мин —
  инстансы плодились) — «Connection refused … after 1ms», «5/5 brokers are
  down» каждую секунду.
  **Корень — три слоя**:
  1. Клиент на вызов: `ConfluentKafkaProbeClient`/`ConfluentKafkaRuntimeProbeClient`
     (`src/AdminPanel.Probes/Kafka/ConfluentKafkaProbeClient.cs`) создают новый
     AdminClient/Consumer на КАЖДЫЙ вызов (DescribeCluster/Topics/Groups/List
     + по клиенту на каждую consumer-группу в `CommittedAsync` + `EndOffsets`),
     тик `KafkaProbeLoop` — раз в 15 с. Confluent.Kafka 2.14: AdminClient
     внутри — Producer (отсюда `rdkafka#producer-N`), `Init()` стартует
     LongRunning poll-поток + нативные rdk-потоки (main + broker на каждый
     endpoint), на каждый инстанс — SASL PLAIN handshake. Итого churn 5–7
     тяжёлых нативных клиентов (по ~4–8 потоков каждый) каждые 15 с.
  2. Мгновенный refusal: `retry.backoff.ms`/`reconnect.backoff.ms` по
     умолчанию 100 мс → каждый клиент непрерывно штурмует все endpoints,
     poll-потоки постоянно обрабатывают fail-события (это и съедало ядро),
     FAIL/ERROR-лог librdkafka раз в секунду в stdout.
  3. Блокирующий Dispose: `AdminClient.Dispose()` → `callbackCts.Cancel()` +
     `callbackTask.Wait()` — синхронное ожидание poll-потока на TP-воркере;
     каскадно блокирует пул (в дампе — разросшийся threadpool, десятки
     idle-воркеров).
  **Фиксы** (выбор в spec):
  1. Кэш kafka-клиентов per (bootstrap, user) с TTL/версией снапшота:
     переиспользовать один AdminClient на кластер (минимум — на тик) для всех
     операций пробы, пересоздание только при смене endpoints/кредов или
     фейле; Dispose — только при замене/выключении, не в горячем пути.
  2. Настройки librdkafka для проб: `reconnect.backoff.ms`/`retry.backoff.ms`
     ≥ 1000 (+ разумный `reconnect.backoff.max.ms`), приглушить лог
     librdkafka (`SetLogHandler` → Debug-уровень панели; в инциденте —
     FAIL/ERROR каждую секунду).
  3. Backoff недоступных кластеров в `KafkaProbeLoop`: после K подряд
     неудачных проб — пропуск тиков с экспонентой (15 с → 60 с → 300 с, сброс
     при успехе), чтобы мёртвый кластер не штурмовался каждые 15 с; состояние
     backoff видно в снапшоте/UI (кластер не «мерцает»).
  4. (заодно) `System.Net.Http` → Warning в appsettings панели: 4 строки
     лога на каждый HTTP-запрос — 2600 строк/мин шума в логе контейнера.
  **Репро и приёмка** (стенд `dev-stand/adminpanel`, e2e-чек):
  - Репро «как было»: put в etcd `/kafka/clusters/<C>/endpoints` = 3 адреса
    на закрытые порты + `app_user`/`app_password` (Active, без state) →
    до фикса за ~5 мин панель >50% CPU, rdkafka-лог >20 строк/мин.
  - После фикса: тот же расклад ≥15 мин — CPU панели ≤5% одного ядра в покое
    тика, rdkafka-лог ≤1 события/мин на кластер, число потоков процесса
    стабильно (не растёт), проба кластера честно failed в снапшоте/UI и
    гаснет при поднятии брокеров; юнит-тест кэша клиентов (смена кредов →
    пересоздание, сброс backoff при успехе) + интеграционный с закрытыми
    портами.
  Зависимостей нет.
