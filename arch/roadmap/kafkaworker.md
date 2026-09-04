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

- **`t05-kafka-client-churn`** — AdminClient не должен churn'иться и жечь CPU
  на недоступных кластерах (инцидент as-kafkaworker 2026-09-04: ~100% ядра).
  **Что нашли** (разбор на живом стенде): воркер создаёт новый Confluent
  AdminClient на каждый тик (`KafkaAdminClient.EnsureClient()` ленивый — но
  supervise/reconcile зовут операции каждый тик); при лежащем seed-кластере
  (брокеров нет вовсе, endpoints `host.docker.internal:16001–16003` refused,
  portalloc пуст при объявленных брокерах → supervise падает «broker broker1
  не закреплён в portalloc» каждый тик) это даёт: churn rd_kafka-инстансов
  ~4–6/мин (каждый — `rdk:main` + брокерные нативные потоки + 2 managed
  LongRunning), reconnect-шторм дефолтным `reconnect.backoff.ms=100`
  (468 «3/3 brokers are down» за 3 мин, ~1000 строк лога/мин) и — главное —
  **зависший LongRunning-поток Confluent.Kafka, крутящий 100% ядра весь
  аптайм** (cgroup ~52 core-мин за 50 мин, 2/3 system time; per-thread
  804/800 тиков за 8 с у одного `.NET Long Running`, состояние `Running`
  без syscalls). Тот же класс инцидента, что t11-kafka-probe-churn в панели
  (0e59744, 2026-09-02), — но защиты t11 на воркер не переносились; паттерн
  существует и на main (адаптер и циклы идентичны; коллектор t04 не при чём —
  добавляет лишь 1 клиент/30 с при `Metrics.Enabled=true`).
  **Что делать** (уроки t11 → воркер): (1) кэш AdminClient'ов per
  `(bootstrap,user,password)` в `KafkaAdminClientFactory` вместо «клиент на
  тик», инвалидация при смене кредов/endpoints; (2) пины librdkafka
  `reconnect.backoff.ms`/`retry.backoff.ms` ≥ 1000 мс в `AdminClientConfig`;
  (3) детерминированный `await using` во всех путях (supervise/provisioning/
  collector), а не финализаторы GC; (4) экспоненциальный backoff
  недоступного кластера в kafka-доменных шагах reconcile/supervise
  (15→60→300 с, сброс при успехе) — лежащий кластер не долбить каждые 15 с
  (arch/17: флап≠смерть, честность инспекции); (5) привести стенд в согласие
  (пересеять kafka `05-seed.sh` либо почистить конфиг кластера — portalloc
  пуст при объявленных брокерах) и добавить чек на CPU/threads в `checks/`
  по образцу 58-го. Приёмка: на стенде с лежащим кластером CPU ≤5% ядра
  ≥15 мин, ≤1 rdkafka-строки/мин, число потоков стабильно; юнит-тесты
  кэша/backoff и интеграционный на закрытых портах (образцы — t11).
  Строится на seam `IKafkaAdminClient` из t04 (мержится вместе с ним).
