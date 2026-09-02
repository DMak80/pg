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
- **`t10-kafka-discovery-integration`** — интеграция Kafka-клиента Puzzle
  (`Infrastructure.App.Kafka`, Confluent) с библиотекой дискавери HA.Kafka
  (t05): BootstrapServers/SASL-креды из etcd-снапшота вместо
  `ConnectionStrings:Kafka`; реакция на событие Updated — переподключение
  продюсеров/консюмеров при смене endpoints/кредов (вкл. ротацию app_password,
  arch/16 §5 H); Aspire-ветка без etcd (локальная разработка — источник
  топологии из ConnectionStrings, по образцу переключателя `Database:Source`
  у HA.Db). Канон-контракт — [../15-kafka-clusters.md](../15-kafka-clusters.md)
  §5–§6.
