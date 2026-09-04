# 18. Единая телеметрия: Prometheus-метрики (каркас + хранение) ★

**Единое решение по сбору и хранению Prometheus-метрик** для всех .NET-сервисов
монорепо (PgWorker, KafkaWorker, AdminPanel) и будущих проектов с аналогичным
стеком. Канон-шаблон каркаса — модуль `Infrastructure.App.Metrics` проекта
`../Puzzle` (док `docs/01.20-metrics.md`); в монорепо живёт его порт — общая
сборка `src/Shared.Metrics` — с надстройкой воркер-паттерна. Стек хранения —
Prometheus + Grafana + Alertmanager (профиль `metrics` dev-стенда, конфиги в
репо).

Ключевые свойства:

- **Инструментация** — `System.Diagnostics.Metrics` (Meter API, встроен в
  .NET 10) + официальный `OpenTelemetry.Exporter.Prometheus.AspNetCore`
  (эндпоинт `/metrics`); имена/типы/лейблы — единый словарь §2.
- **Экспозиция** — тот же Kestrel-порт, что `/healthz` (воркеры `:8080`):
  `/metrics` не попадает под `X-Api-Key` (middleware защищает только `/api`) и
  под cookie-авторизацию панели — доверенная закрытая docker-сеть.
- **Метрики — пассивные наблюдатели**: инструменты пишут в Meter, экспорт —
  pull (scrape Prometheus); сбор метрик никогда не влияет на поведение
  воркеров/панели (никаких throw наружу, никакой блокировки циклов).
- **Канон имён** — финальные имена Prometheus-формата (после OTel-экспорта);
  в коде — dot-нотация Meter (§2). Словарь общий для обоих воркеров —
  дашборды и алерты пишутся против одного набора имён.

Границы (что НЕ входит): метрики самих Kafka-брокеров (JMX-exporter — вне
стенда; лаги/USR снимает коллектор воркера §4), push-модель (OTLP/remote
write), прод-развёртывание мониторинга за пределами docker-стенда (паттерн
документируется §5.4), визуализация метрик внутри AdminPanel (панель живёт на
etcd-снапшоте; arch/adminpanel/01: «P21 просит дашборд, не Prometheus»).

---

## 1. Структура решения

```
Puzzle (архитектурный шаблон)          монорепо (pg)
───────────────────────────            ──────────────────────────────
Infrastructure.App.Metrics   ──порт──► src/Shared.Metrics
  базовая OTel-обвязка:                 порт базы (копия, паттерн
  + AddAppMetrics()/MapAppMetrics()     AdminPanel.Infrastructure)
  + конвенции имён/лейблов            + WorkerMetrics (надстройка
  + docs/01.20-metrics.md               воркер-паттерна §2.2 — только
                                        в монорепо: в Puzzle воркеров нет)
```

- **База (Puzzle = канон)**: DI-расширение `AddAppMetrics` (регистрация
  OTel-MeterProvider + сервисных Meter), `MapAppMetrics` (эндпоинт `/metrics`),
  конвенции: имя Meter = имя системы (`PgWorker`/`KafkaWorker`/`AdminPanel`),
  dot-нотация инструментов, единицы — секунды/штуки (OTel-суффиксы при
  экспорте), включение/путь — `[Config]`-секция `<Service>:Metrics`.
- **Надстройка WorkerMetrics (монорепо)**: типизированные инструменты
  воркер-паттерна (циклы/клэймы/фазы/операции/снапшоты §2.2) — общий словарь
  для PgWorker и KafkaWorker (их `HealthState`-паттерн уже идентичен).
  Серии §2.2 питаются марк-методами `WorkerMetricsInstrumentation`,
  вызываемыми циклами и подпиской на фазовые записи журнала работы (рядом с
  существующими `health.Mark*`; единственный источник на серию);
  `HealthState` остаётся нетронутым источником только `/healthz`.
- **Подключение нового .NET-проекта** — по докам Puzzle `01.20-metrics.md`
  (копия модуля или ProjectReference на общую сборку монорепо) — см. §7.

## 2. Словарь метрик (канон)

Имена — финальные после OTel-экспорта (точки → подчёркивания, суффиксы
единиц/`_total` по стандарту экспортёра; фактические имена фиксирует
интеграционный тест §6). Лейблы конечны (никаких free-form строк в лейблах,
кроме `cluster` — доменное имя кластера).

### 2.1. Общие (все сервисы, из OTel-рантайма)

| Имя | Тип | Источник |
|---|---|---|
| `dotnet_*` (gc, threadpool, allocations, process) | разные | OTel `System.Runtime`-метры |
| `http_server_request_duration_seconds` | histogram | OTel ASP.NET-метр (панель; у воркеров — при наличии HTTP-грани). Факт пинов 1.16.0-beta.1 (фиксирует тест `Shared.Metrics.UnitTests`): на минимальном slim-хосте гистограмма не эмитится; фактически присутствуют `http_server_active_requests`, `kestrel_*`, `aspnetcore_memory_pool_*` — на реальных сервисах сверяют интеграционные тесты Ф3–Ф5 |

### 2.2. Воркер-паттерн (`Shared.Metrics` WorkerMetrics; PgWorker + KafkaWorker)

Имена инструментов глобальны (имя Meter в метрику не входит) — оба воркера
пишут в одни серии; различение сервисов/инстансов — лейблы `job`/`instance`,
которые назначает Prometheus по scrape-джобе (§5.2). Факт экспортёра
1.16.0-beta.1 (фиксирует интеграционный тест §6): к каждой серии добавляется
системный лейбл `otel_scope_name` = имя Meter (`PgWorker`/`KafkaWorker`) —
 PromQL-запросы словаря от него не зависят, но факт зафиксирован для инспекции.

| Имя | Тип | Лейблы | Смысл |
|---|---|---|---|
| `worker_loop_ticks_total` | counter | `loop`, `ok` | тики циклов (Reconcile/Keepalive/Snapshot); `ok` ∈ {true,false} |
| `worker_loop_last_success_timestamp_seconds` | gauge | `loop` | unix-время последнего успешного тика (алерт «цикл умер»: `time() − значение > порога`) |
| `worker_loop_duration_seconds` | gauge | `loop` | длительность последнего тика |
| `worker_claims_held` | gauge | — | сколько кластеров держим под клэймом |
| `worker_process_phase_duration_seconds` | gauge | `cluster`, `process`, `phase` | сколько секунд кластер в текущей фазе процесса (source: марк-методы фаз; смена фазы/завершение процесса сбрасывает серию) |
| `worker_operation_total` | counter | `operation`, `result` | завершённые операции (provision/deprovision/rotate/move/rollback/finalize/abort…; подавленные ops — supervise/evacuate — не считаются, см. ниже), `result` ∈ {ok,error} |
| `worker_snapshot_age_seconds` | gauge | — | возраст последнего снапшота P12 |

`process` — фактическое `op` журнала работы (канон = факт, фиксируется
интеграционным тестом): у PgWorker — `provision`, `deprovision`, `adopt`,
`add-shard`, `remove-shard`, `rotate-app-password`, `move`, `rollback`,
`finalize`, `repair`, `abort`; у KafkaWorker — `provision`, `deprovision`, `add-broker`,
`remove-broker`, `reassign`, `rotate`, `regen`, `topicsync`. `phase` —
фаза машины состояний (journal-фаза). Завершение фазовой серии —
терминальные фазы журнала по фактическому словарю: `done`, `failed`,
`crashed`, `rejected`, `cancelled` (сброс серии + счёт `worker_operation_total`;
`skipped` — промежуточная фаза усыновления, серию НЕ закрывает).
Стационарные ops без терминальной фазы — `supervise` (пишется и через
`WriteSupervisionAsync`, мимо фазового события) и `evacuate` (только
`waiting-*`/`blocked-moving`) — фазовых серий НЕ эмитят: их живость
закрывает `worker_loop_last_success_timestamp_seconds` (алерт
WorkerLoopStalled), вечные серии не копятся.

### 2.3. Kafka-домен (коллектор KafkaWorker §4)

| Имя | Тип | Лейблы | Смысл |
|---|---|---|---|
| `kafka_consumer_lag` | gauge | `cluster`, `group`, `topic` | суммарный consumer-lag (end-offsets − committed), по группе и топику |
| `kafka_under_replicated_partitions` | gauge | `cluster`, `topic` | число партиций топика с USR>0 (ISR ⊂ assignment) |
| `kafka_collector_last_success_timestamp_seconds` | gauge | — | unix-время последнего успешного сбора коллектора (самонаблюдение) |

### 2.4. Панель (AdminPanel)

| Имя | Тип | Лейблы | Смысл |
|---|---|---|---|
| `panel_refresher_last_success_timestamp_seconds` | gauge | — | успешный тик etcd-refresher (свежесть снапшота панели) |
| `http_server_request_duration_seconds` | histogram | OTel | см. §2.1 |

### 2.5. PG-репликация — scrape Patroni напрямую

Метрики репликации НЕ дублирует воркер: Prometheus скрейпит `:8008/metrics`
Patroni-нод (arch/08). Стенд: Patroni-эмуляторы (`hc*`) отдают минимальный
набор `pg_replica_lag_seconds{scope,node}` (расширение emulator.py); таргеты —
static (DNS-имена сети стенда). Узлы, создаваемые PgWorker в per-cluster
сетях, в контуре стендового Prometheus недостижимы — см. ограничение §5.4.

## 3. Экспозиция и безопасность

- Воркеры: `/metrics` на том же Kestrel `:8080`, что `/healthz`;
  `ApiKeyMiddleware` защищает только `/api` — метрики открыты в доверенной
  docker-сети (порт /healthz-канона, симметрия панели).
- Панель: `/metrics` без cookie-авторизации (guard — только `/api/*`);
  бандл SPA секретов не содержит.
- Формат — Prometheus text/OpenMetrics (content-negotiation экспортёра);
  scrape-интервал стенда 15 с.

## 4. Коллектор Kafka-метрик

Фоновый hosted-сервис `KafkaWorker.App` (тик `KafkaWorker:Metrics:
CollectIntervalSec`, default 30): по Active-кластерам (снапшот etcd) через
`IKafkaAdminClientFactory` — `ListConsumerGroups` + `ListConsumerGroupOffsets`
+ `ListOffsets` (watermarks) → `kafka_consumer_lag`; describe-метаданные
топиков → `kafka_under_replicated_partitions`. Результаты пишутся в
ObservableGauge-стейт; ошибка сбора не валит тик (обновляется только
`kafka_collector_last_success_timestamp_seconds`). Один AdminClient-коннект
на тик по кластеру, таймаут короткий (seam-фабрика уже 10 с); сбор — вне
клэймов (read-only, безопасен параллельно любым процессам).

## 5. Хранение: стек мониторинга dev-стенда

### 5.1. Сервисы (профиль `metrics` в `dev-stand/adminpanel/docker-compose.yml`)

| Сервис | Образ | Порт (хост) | Назначение |
|---|---|---|---|
| `prometheus` | `prom/prometheus` (пин версии) | 9090 | TSDB + rules; конфиг `metrics/prometheus/prometheus.yml` |
| `grafana` | `grafana/grafana` (пин версии) | 3000 | дашборды; provisioning `metrics/grafana/` (datasource + JSON-дашборды из репо) |
| `alertmanager` | `prom/alertmanager` (пин версии) | 9093 | доставка алертов; `metrics/alertmanager/alertmanager.yml` |

Профиль входит в полный подъём `dev-stand/adminpanel/checks/00-up.sh`
(«стенд = полная система»); quick-профиль стенда мониторинга не поднимает.
Порты 9090/3000/9093 публикуются на хост; коллизия с локальными сервисами —
через env-override compose (значения по умолчанию в `.env`-шаблоне).

### 5.2. Scrape-таргеты (prometheus.yml)

| Job | Таргеты | Что снимает |
|---|---|---|
| `pgworker` | `host.docker.internal:8080` (публикация deploy-compose; вне сети стенда) | §2.1–2.2 |
| `kafkaworker` | имя сети стенда `kafkaworker:8080` (хост-публикация 8082 — только для чеков, Prometheus её не использует; fallback не предусмотрен) | §2.1–2.3 |
| `adminpanel` | имя сети стенда `adminpanel:8080` (хост-публикация 5050 — только для браузера/чеков) | §2.4 |
| `patroni` | static: `hc1a:8008, hc1b:8008, hc2a:8008, hc2b:8008` | §2.5 |

### 5.3. Дашборды (Grafana provisioning, JSON в репо)

`dashboards/workers.json` (циклы/клэймы/фазы/операции/снапшоты обоих
воркеров), `dashboards/kafka.json` (USR, consumer-lag, коллектор),
`dashboards/pg.json` (репликация Patroni-нод, health-грань систем).

### 5.4. Прод-паттерн (документируется, вне кода)

Прод-мультихост: Prometheus рядом с docker-хостами, таргеты нод — из
advertise-адресов portalloc (file_sd из etcd-снапшота — опция будущих задач);
узлы в per-cluster сетях скрейпятся Prometheus'ом, прикреплённым к этим сетям
(или federate через воркер — roadmap). В скоуп t04 не входит.

## 6. Тестирование (канон уровня)

- **Unit**: WorkerMetrics — семантика инструментов (счёт тиков по `ok`,
  сброс серий фаз, возраст снапшота) через тестовый MeterListener/коллектор.
- **Integration**: `WebApplicationFactory` — `/metrics` отвечает 200
  text-format, содержит канонические имена §2 (фиксирует фактические
  экспортированные имена против словаря); `/metrics` не требует ApiKey.
- **E2E-чек стенда**: `checks/65-metrics.sh` — профиль `metrics` поднят,
  все scrape-джобы `up`, дашборды загружены, алерт-рулы зарегистрированы
  (`/api/v1/rules`), Alertmanager жив.

## 7. Подключение нового .NET-проекта

1. Скопировать модуль `Infrastructure.App.Metrics` из Puzzle (или
   ProjectReference на `Shared.Metrics` внутри монорепо) — паттерн
   `AdminPanel.Infrastructure`.
2. `[Config]`-секция `<Service>:Metrics { Enabled=true, Path="/metrics" }`.
3. `services.AddAppMetrics(...)`, `app.MapAppMetrics()`; доменные метрики —
   свои инструменты dot-нотацией по конвенциям `01.20-metrics.md`; для
   воркер-паттерна — переиспользовать §2.2, а не плодить синонимы.
4. Добавить scrape-джобу в `prometheus.yml` стенда.

## 8. Конфигурация

```
<Service>:Metrics { Enabled=true, Path="/metrics" }   # все сервисы
KafkaWorker:Metrics { CollectIntervalSec=30 }          # коллектор §4
# стенд (env-override compose):
METRICS_PROMETHEUS_PORT=9090, METRICS_GRAFANA_PORT=3000,
METRICS_ALERTMANAGER_PORT=9093, METRICS_ALERT_WEBHOOK_URL=   # пусто — только UI
```

## 9. Риски

| # | Риск | Митигация |
|---|---|---|
| M1 | Кардинальность лейблов (фазы/группы/топики) растёт | лейблы конечны (§2), фаза сбрасывается при завершении процесса; `group`/`topic` — реальные сущности kafka-домена |
| M2 | Коллектор лагов грузит AdminClient/брокеров | один проход за тик, интервал 30 с, короткий таймаут; ошибки сбора не ретраятся поллерами циклов |
| M3 | OTel-суффиксы имён отличаются от словаря (версии экспортёра) | интеграционный тест фиксирует фактические имена против словаря §2; версия пин в Directory.Packages.props |
| M4 | Порт 3000/9090/9093 занят на хосте стенда | env-override compose (§8) |
| M5 | Эмуляторы Patroni без /metrics | расширение emulator.py (§2.5) — часть задачи |

---

→ Возврат к [README.md](README.md). Воркеры — [14-pgworker.md](14-pgworker.md)
§7, [16-kafkaworker.md](16-kafkaworker.md) §7; панель —
[adminpanel/01-architecture.md](adminpanel/01-architecture.md); реализация —
`docs/superpowers/2026-09-04-t04-unified-metrics/` (spec/plan).
