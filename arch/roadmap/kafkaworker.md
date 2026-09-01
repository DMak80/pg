# Roadmap: сервис KafkaWorker

Отложенные задачи KafkaWorker и kafka-домена панели (out of scope текущей
спецификации `docs/superpowers/2026-08-30-kafka-admin-worker/spec.md` §8;
канон — [../15-kafka-clusters.md](../15-kafka-clusters.md) (контракт etcd) и
[../16-kafkaworker.md](../16-kafkaworker.md) (воркер)).

## Задачи

- **`t03-kafka-security`** — TLS (SASL_SSL), ACL/authorization, разделение
  admin/app кредов (арх-канон 16 §2.1: сейчас один per-cluster SASL-кред
  для всех ролей, CONTROLLER-listener PLAINTEXT внутри закрытой сети).
- **`t04-kafka-metrics`** — Prometheus-метрики воркера и панели (лаги, USR,
  фазы процессов, клэймы).
- **`t05-kafka-discovery-lib`** — клиентская библиотека дискавери kafka из
  etcd (в репозиторий Puzzle, по образцу ha-db: watch-long-poll/poll, кэш,
  событие); контракт — 15 §5.
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
