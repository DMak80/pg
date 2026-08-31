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
- **`t05-kafka-discovery-lib`** — клиентская библиотека дискавери kafka из
  etcd (в репозиторий Puzzle, по образцу ha-db: watch-long-poll/poll, кэш,
  событие); контракт — 15 §5.
- **`t06-kafka-node-regen`** — rolling-перегенерация существующих брокеров
  с новыми ресурсами (лимиты cpu/mem) и новыми server-props.
