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
`docs/01.19-ha-kafka.md` там).

Порядок: канон → воркер → панель → библиотека Puzzle → метрики.

## Задачи

- **`t01-valkey-canon`** — канон Valkey-домена: `arch/20-valkey-clusters.md` —
  контракт etcd `/valkey/` (контроль-плейн: state/endpoints/креды кластеров,
  клиентские точки дискавери и толерантность читателей) + координация
  `/valkeyworker/`; `arch/21-valkeyworker.md` — оркестратор: декларативный
  жизненный цикл Valkey-кластеров (docker provisioning/deprovisioning,
  надзор, converge). Назначение — **разделяемый кеш набора инстансов
  приложения**: топология standalone (реплики/sentinel/cluster — вне скоупа:
  кеш восполним, шардирование не нужно); maxmemory + политика выселения
  и модель кред (requirepass/ACL) — решения канона.
- **`t02-valkey-worker`** `← t01-valkey-canon` — сервис ValkeyWorker
  (`src/ValkeyWorker.*`, аналог KafkaWorker): provisioning/deprovisioning
  Valkey-кластеров в docker, публикация факта в etcd `/valkey/`, надзор
  (health/converge); переиспользование `Shared.{Core,Etcd,Metrics,Tls}`.
- **`t03-valkey-panel`** `← t01-valkey-canon` — valkey-домен AdminPanel:
  etcd-инспекция `/valkey/` (AdminPanel.Etcd), API + React-панель —
  кластеры/ноды/состояния, live-пробы, алерты, операции (создание/удаление
  кластера, операции над нодами).
- **`t04-valkey-discovery-lib`** `← t01-valkey-canon` — клиентская
  дискавери-библиотека в Puzzle: `PuzzleServer.Infrastructure.App.HA.Valkey`
  — только читатель `/valkey/clusters/<C>/` (снапшот endpoints/креды/state,
  watch+poll актуализация, fail-open, `GetClientConfig()` для
  StackExchange.Redis); интеграция с клиентским модулем Puzzle — по образцу
  интеграции HA.Kafka в `Infrastructure.App.Kafka`.
- **`t05-valkey-metrics`** `← t02-valkey-worker` — телеметрия Valkey-домена
  по образцу [../18-metrics.md](../18-metrics.md): ValkeyWorker на каркасе
  `Shared.Metrics` (воркер-паттерн §2.2: фазы/HealthState), коллектор метрик
  Valkey-нод (аналог коллектора Kafka §4: INFO/репликация через redis-пробу,
  самонаблюдение коллектора), доменный словарь §2, дашборд Grafana
  `dashboards/valkey.json`, конфиг `ValkeyWorker:Metrics`.
