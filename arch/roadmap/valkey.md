# Roadmap: Valkey-домен (канон, ValkeyWorker, панель, дискавери-библиотека)

Задачи добавления управления Valkey-кластерами по образцу Kafka-домена:
**доступ** (клиентский дискавери endpoints/кредов) и **контроль**
(декларативное управление) — через единый etcd-контур, как у PG и Kafka.
Канон-образцы — [../15-kafka-clusters.md](../15-kafka-clusters.md)
(контракт etcd `/kafka/` + клиентский дискавери),
[../16-kafkaworker.md](../16-kafkaworker.md) (воркер),
[../17-synchronization-principles.md](../17-synchronization-principles.md).
Клиентская библиотека исполняется в репозитории Puzzle
(`PuzzleServer.Infrastructure.App.HA.Valkey` по образцу HA.Kafka —
`docs/01.19-ha-kafka.md` там). Канон домена —
[../20-valkey-clusters.md](../20-valkey-clusters.md) (контракт etcd) и
[../21-valkeyworker.md](../21-valkeyworker.md) (оркестратор).

Порядок: канон → воркер → панель → библиотека Puzzle.

## Задачи

- **`t31-vwk-contour-network`** — контурный режим сетей ValkeyWorker:
  провижининг vwk-нод в контурную сеть по образцу PgWorker/S3Worker
  (`PgWorker__Docker__ContourNetwork` / `S3Worker__Docker__ContourNetwork` ←
  `PGW_CONTOUR_NETWORK` в deploy/docker-compose.yml; канон режима —
  arch/14 §2.1, arch/24). Без опции — текущее поведение (дефолтный
  docker-bridge). Цель: весь стенд в одной сети `adminpanel-stand-net`;
  vwk-ноды сейчас — единственные контейнеры стенда вне её, bridge не
  резолвит имена контейнеров (алиасы), адресация нод — только хост-публикации
  портов. Arch-first: сначала режим в [../21-valkeyworker.md](../21-valkeyworker.md),
  затем код воркера и deploy.
