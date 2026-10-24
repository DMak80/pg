# Roadmap: сервис KafkaWorker

Отложенные задачи KafkaWorker и kafka-домена панели (out of scope текущей
спецификации `docs/superpowers/2026-08-30-kafka-admin-worker/spec.md` §8;
канон — [../15-kafka-clusters.md](../15-kafka-clusters.md) (контракт etcd) и
[../16-kafkaworker.md](../16-kafkaworker.md) (воркер)).

## Задачи

- **`t01-reassign-exec-channel`** — переделать docker-взаимодействия reassign
  на единый механизм Kafka-протокола: `PartitionReassignerProcess` сейчас
  исполняет kafka-команды reassign через `ExecNodeAsync` внутрь
  kafka-контейнеров — отклонение от
  [../25-component-communications.md](../25-component-communications.md) №24.
  Решение: подавать reassign-план прямо из KafkaWorker через AdminClient —
  тот же Kafka-протокол, операция `AlterReassignments` (KIP-455,
  идемпотентность подачи сохраняется). Тогда exec исчезает полностью:
  и чтение метаданных, и подача плана — одним протоколом; docker-exec
  (`ExecNodeAsync` в kafka-контейнеры) из процесса убирается, etcd-прогресс
  `/kafkaworker/reassignments/<C>` не меняется.
