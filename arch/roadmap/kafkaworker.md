# Roadmap: сервис KafkaWorker

Отложенные задачи KafkaWorker и kafka-домена панели (out of scope текущей
спецификации `docs/superpowers/2026-08-30-kafka-admin-worker/spec.md` §8;
канон — [../15-kafka-clusters.md](../15-kafka-clusters.md) (контракт etcd) и
[../16-kafkaworker.md](../16-kafkaworker.md) (воркер)).

## Задачи

- **`t07-kafka-ca-rotation`** — ротация per-cluster CA и серверных сертификатов
  (окно двойного доверия CA/серт-версий в env, rolling-пересоздание брокеров;
  отложено из t03-kafka: серты долгоживущие — 10 лет; зависит от канона
  безопасности arch/16 §2.3 и `BrokerCertificateCache`).
- **`t91-kafka-seed-api-flaky`** — флейм
  `KafkaWorker.IntegrationTests.Api.KafkaSeedApiTests.SeedDemo_AlreadySeeded_NoOp`
  в фильтре `Api|Etcd` (вскрыт мерж-гейтом 2026-09-14, ветка
  `feat-panel-worker-restart-cert`). Механика: Arrange кейса делает
  `POST /api/seed/demo` БЕЗ предварительной чистки префиксов; общий etcd
  коллекции `kafka-api` загрязняется соседними классами —
  `ClusterMutationsApiTests`/`TopicMutationsApiTests` через
  `KafkaApiTestSeed.SeedActiveClusterAsync("events")` наливают
  `/kafka/clusters/events/config` и в teardown убирают только
  `/kafka/clusters/events*/topics/`. Если такие классы выполнились раньше,
  сид идёт по идемпотентной ветке (`seeded:false`) и НЕ пишет
  `/kafkaworker/rotations/events`; Assert падает с NullReferenceException на
  `rotation.Value!.Value` (`Result<Kv?>` с `Value == null`). Порядок классов
  xUnit внутри коллекции недетерминирован — добавление в коллекцию новых
  классов (ветка сертов/рестарта API: `RestartApiTests`,
  `WorkerApiCertStartupTests`) сменило расклад и сделало падение устойчивым.
  Подтверждено прогонами 2026-09-14: класс изолированно — 3/3 зелёный;
  фильтр на `main` (без новых классов) — 41/41 зелёный; в ветке — 50/51.
  Фикс (на выбор): в Arrange гарантировать собственную наливку — чистить
  префиксы сида (`/kafka/clusters/events/`, `/kafkaworker/rotations/`,
  `/kafkaworker/rebalances/`, `/kafkaworker/reassignments/`) перед первым POST
  по прецеденту соседнего кейса `SeedDemo_EmptyEtcd_SeedsCanonicalKeySet`
  (чистка там уже есть); либо добавить классам-загрязнителям own-only teardown
  своего `events`-кластера. Прод-код не затронут — чисто тестовая
  инфраструктура.
