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
