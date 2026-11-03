# Roadmap: Valkey-домен (канон, ValkeyWorker, панель, дискавери-библиотека)

Задачи добавления управления Valkey-кластерами по образцу Kafka-домена:
**доступ** (клиентский дискавери endpoints/кредов) и **контроль**
(декларативное управление) — через единый etcd-контур, как у PG и Kafka.
Канон-образцы — [kafka-clusters/README.md](../kafka-clusters/README.md)
(контракт etcd `/kafka/` + клиентский дискавери),
[../kafkaworker/README.md](../kafkaworker/README.md) (воркер),
[../17-synchronization-principles.md](../17-synchronization-principles.md).
Клиентская библиотека исполняется в репозитории Puzzle
(`PuzzleServer.Infrastructure.App.HA.Valkey` по образцу HA.Kafka —
`docs/01.19-ha-kafka.md` там). Канон домена —
[valkey-clusters/README.md](../valkey-clusters/README.md) (контракт etcd) и
[../valkeyworker/README.md](../valkeyworker/README.md) (оркестратор).

Порядок: канон → воркер → панель → библиотека Puzzle.

## Задачи
