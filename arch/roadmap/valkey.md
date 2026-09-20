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

Порядок: канон → воркер → панель → библиотека Puzzle → метрики.

## Задачи

- **`t05-valkey-metrics`** — телеметрия Valkey-домена
  по образцу [../18-metrics.md](../18-metrics.md): ValkeyWorker на каркасе
  `Shared.Metrics` (воркер-паттерн §2.2: фазы/HealthState), коллектор метрик
  Valkey-нод (аналог коллектора Kafka §4: INFO/репликация через redis-пробу,
  самонаблюдение коллектора), доменный словарь §2, дашборд Grafana
  `dashboards/valkey.json`, конфиг `ValkeyWorker:Metrics`.
- **`t07-valkey-ca-rotation`** — ротация per-cluster CA и серверных
  сертов Valkey (окно двойного доверия, образец CaRotator
  [../16-kafkaworker.md](../16-kafkaworker.md) §5 K; `ca_next_*`
  staging, bundle `ca_pem`, пересоздание нод с перевыпуском) —
  по потребности; канон после t06 —
  [../20-valkey-clusters.md](../20-valkey-clusters.md) §2,
  [../21-valkeyworker.md](../21-valkeyworker.md) §9 (R10-аналог:
  `ca_key` в etcd — зона доверия контроль-плейна).
