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
- **`t91-kafka-portalloc-race`** — тот же класс гонки параллельного
  выделения портов, что t90 PgWorker: два кластера KafkaWorker на одном
  docker-хосте, засеянные одновременно (provisioning/add-broker), читают
  занятость (docker-биндинги ∪ `/kafkaworker/portalloc/*`) ДО первой записи
  друг друга → одинаковые порты, контейнеры второго кластера падают с
  «port is already allocated». Решение — глобальный клэйм-лока секции
  довыделения по паттерну t90 (txn `version==0` + put-with-lease,
  `/pgworker/locks/portalloc` в arch/14 §3.3); контракты arch/15/16
  обновить по образцу t90. ← `t90-portalloc-parallel-race`.
