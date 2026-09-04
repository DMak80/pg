# Спецификация: t04-unified-metrics — единое решение Prometheus-метрик (каркас + инструментация + хранение)

Дата: 2026-09-04. Фаза dev-flow: spec. Источники: `arch/roadmap/pgworker.md`
(тег `t04-metrics`), `arch/roadmap/kafkaworker.md` (тег `t04-kafka-metrics`),
канон-контракт **`arch/18-metrics.md`** (создан этой задачей, arch-first),
`arch/14-pgworker.md` §7 (наблюдаемость — обновлён), `arch/16-kafkaworker.md`
§7/границы (обновлён), `arch/README.md` (индекс — дополнен), Puzzle-канон
каркаса (`../Puzzle/docs/01-infrastructure.md` — паттерн «модуль = проект +
док»; модулей метрик там нет — добавляется этой задачей), прецедент копий
Puzzle-каркаса (`src/AdminPanel.Infrastructure`), живой код `HealthState`
обоих воркеров (`src/PgWorker.App/HealthState.cs`,
`src/KafkaWorker.App/HealthState.cs` — паттерны идентичны, готовые источники
данных), `dev-stand/adminpanel/` (стенд: Patroni-эмуляторы без `/metrics`,
compose-профили quick/full/kafka).

Задача объединяет два тега roadmap в одну работу: единое решение — общая
библиотека/каркас + инструментация всех сервисов монорепо + стек хранения,
к которому подключаются другие .NET-проекты.

## 0. Контекст (что сейчас и почему меняется)

1. **Метрик нет ни у кого**: воркеры и панель отдают только `/healthz`
   (Kestrel `:8080`); Prometheus/Grafana/Alertmanager в монорепо и стенде
   отсутствуют; пакеты OpenTelemetry в `Directory.Packages.props` отсутствуют.
2. **Источники данных уже есть**: `HealthState` обоих воркеров уже собирает
   тики циклов (Reconcile/Keepalive/Snapshot), число клэймов и свежесть
   снапшотов — но отдаёт их только в `/healthz`, без истории и алертов;
   journal-фазы процессов и прогресс-ключи etcd дают материал для
   «кластер в фазе X уже N секунд»; KafkaWorker держит `IKafkaAdminClientFactory`
   (describe топиков, оффсеты) — данных для USR/consumer-lag не хватает
   только коллектора.
3. **Паттерн Puzzle-каркаса**: каждый модуль = проект
   `PuzzleServer.Infrastructure.App[.X]` + документ `docs/01.NN-name.md` в
   индексе `01-infrastructure.md`. Модуля метрик в Puzzle нет — по правилу
   монорепо («архитектурный шаблон — все новые решения переносятся туда»)
   базовая OTel-обвязка добавляется в Puzzle, в монорепо заносится портом.
4. **Дубли HealthState** PgWorker/KafkaWorker почти идентичны (осознанные,
   унификация — t08): единый словарь воркер-метрик (arch/18 §2.2) кладёт
   «общую семантику» раньше унификации кода — оба воркера сразу пишут в
   одни и те же имена.

## 1. Цель

1. **Единый каркас** Prometheus-метрик на OpenTelemetry
   (`System.Diagnostics.Metrics` + `OpenTelemetry.Exporter.Prometheus.
   AspNetCore`): базовый модуль в Puzzle (канон) + общая сборка монорепо
   `src/Shared.Metrics` с надстройкой воркер-паттерна; новые .NET-проекты
   подключаются по документации Puzzle (arch/18 §7).
2. **Инструментация всех трёх сервисов**: PgWorker и KafkaWorker — циклы,
   клэймы, фазы процессов, операции, возраст снапшотов (единый словарь
   arch/18 §2.2) + Kafka-коллектор (consumer-lag, USR, arch/18 §2.3–§4);
   AdminPanel — HTTP-метрики + свежесть refresher (§2.4). Экспозиция
   `/metrics` на тех же гранях, что `/healthz`, без ApiKey/cookie.
3. **Хранение и наблюдение**: профиль `metrics` dev-стенда
   (Prometheus + Grafana + Alertmanager, конфиги и дашборды в репо,
   arch/18 §5), scrape Patroni-эмуляторов `:8008` (репликация PG),
   e2e-чек стенда.
4. **Алертинг**: Prometheus rules (застрявшая фаза, мёртвый цикл, несвежий
   снапшот, USR>0, высокий consumer-lag, down сервиса) + Alertmanager с
   generic webhook-ресивером (URL — env; пусто — алерты только в UI).
5. **Итог для оператора**: «общее состояние систем и отклонения» — дашборды
   против одного словаря имён, алерты с порогами, история в TSDB.

## 2. Принципы

- **arch-first**: канон — `arch/18-metrics.md` (словарь имён/лейблов,
  экспозиция, коллектор, стек хранения; создан в spec-фазе). Код ссылается
  на arch/18; изменения контракта метрик — только через правку arch/18.
- **Puzzle — архитектурный шаблон**: базовый модуль `Infrastructure.App.
  Metrics` + `docs/01.20-metrics.md` добавляются в `../Puzzle` первыми;
  в монорепо заносится портом (копия, паттерн `AdminPanel.Infrastructure`).
  Воркер-паттерн — надстройка только монорепо (в Puzzle воркеров нет,
  мёртвый код не тащим).
- **Метрики — пассивные наблюдатели**: сбор никогда не влияет на поведение
  сервисов (инструменты не бросают исключений, коллектор не роняет тики
  циклов, ошибка сбора — только в собственную метрику свежести коллектора).
- **Единый словарь имён**: оба воркера пишут в одни и те же имена
  (arch/18 §2.2); лейблы конечны; новые доменные метрики — через правку
  словаря arch/18, а не локальные синонимы.
- **Стек стандартный**: .NET 10 `Meter` API (BCL) + официальный
  prometheus-exporter OTel; версии пин в `Directory.Packages.props`
  (централизованно, оба репо).
- **Закрытая docker-сеть**: `/metrics` открыт без авторизации — симметрия
  `/healthz` (`ApiKeyMiddleware` защищает только `/api`; у панели guard —
  только `/api/*`); TLS/mTLS — вне скоупа (roadmap t03).
- **Стенд = полная система**: профиль `metrics` входит в полный подъём
  `00-up.sh`; quick-профиль — без мониторинга.
- **Тестовые порты — динамические** (AGENTS): интеграционные тесты
  `/metrics` через `WebApplicationFactory` (без хост-портов); стендовые
  порты Prometheus/Grafana/Alertmanager (9090/3000/9093) — фиксированные
  публикации стенда с env-override при коллизиях.

## 3. Структура / компоненты

### 3.1. Puzzle: базовый модуль каркаса (канон)

- Новый проект **`PuzzleServer.Infrastructure.App.Metrics`**: `[Config]`-опции
  `MetricsOptions { Enabled=true, Path="/metrics" }`; DI-расширение
  `AddAppMetrics(...)` (OTel `MeterProvider`: сервисный Meter по имени
  системы + `System.Runtime`-метры; для ASP.NET — http-метр) и
  `app.MapAppMetrics()` (эндпоинт-обёртка prometheus-exporter'а, учёт
  `Enabled/Path`); конвенции имён/лейблов/единиц (dot-нотация Meter,
  секунды/штуки, OTel-суффиксы при экспорте). Без зависимостей от доменов.
- Новый документ **`docs/01.20-metrics.md`** + строка в индекс
  `01-infrastructure.md`: назначение, API, конвенции, инструкция
  подключения нового проекта (арх/18 §7 — зеркало).
- Пакеты в `../Puzzle/src/Directory.Packages.props`
  (`OpenTelemetry`, `OpenTelemetry.Exporter.Prometheus.AspNetCore`,
  `OpenTelemetry.Instrumentation.AspNetCore` — http-метрики ASP.NET).
- Unit-тесты модуля в Puzzle-решении (по правилам Puzzle).

### 3.2. Монорепо: `src/Shared.Metrics` (порт базы + WorkerMetrics)

- **Порт базы** (копия Puzzle-модуля, namespace `Shared.Metrics`): те же
  `AddAppMetrics`/`MapAppMetrics`/`MetricsOptions`.
- **Надстройка `Shared.Metrics.Worker`** (только монорепо): типизированные
  инструменты воркер-паттерна по словарю arch/18 §2.2 —
  `WorkerMetricsInstrumentation` (объект с марк-методами
  `LoopTick(loop, ok)`, `LoopDuration(loop, sec)`, `ClaimsHeld(n)`,
  `ProcessPhase(cluster, process, phase, startedAt)`,
  `Operation(operation, result)`, `SnapshotTaken(at)`), экспортируемые как
  counter/gauge-серии. Источник серий §2.2 — марк-методы, вызываемые
  циклами и подпиской на фазовые записи журнала рядом с существующими
  `health.Mark*` (ровно один источник на серию; `HealthState` остаётся
  нетронутым источником только `/healthz`). Завершение фазовой серии —
  терминальные фазы журнала по фактическому словарю (`done`, `failed`,
  `crashed`, `rejected`, `cancelled`; arch/18 §2.2); стационарные ops без
  терминальной фазы (`supervise`, `evacuate`) фазовых серий не эмитят —
  живость надзора закрывает `worker_loop_last_success_timestamp_seconds`
  (алерт WorkerLoopStalled). Проект в solution-папке `/common/` слnx.
- Тесты: `src/tests/Shared.Metrics.UnitTests` (семантика инструментов,
  AAA-комментарии) — счёт тиков по `ok`, сброс серий фаз при завершении
  процесса, возраст снапшота от TimeProvider.

### 3.3. Инструментация PgWorker (`src/PgWorker.App`)

- `AddAppMetrics` + `MapAppMetrics` в `Program.cs` (Meter `PgWorker`);
  `/metrics` на `:8080` (ApiKeyMiddleware не трогает — уже совместимо).
- `WorkerMetricsInstrumentation` подключается к `HealthState` (циклы,
  клэймы, снапшоты) и расширяется марк-методами фаз/операций: процессы
  Provisioning/Deprovisioning/надзор/эвакуация/переезды/add-remove
  шардов/ротация/усыновление/репарация сообщают `(cluster, process, phase)`
  и результат операций (в точках journal-записей — единый seam).
- Репликация PG — НЕ через воркера: scrape Patroni `:8008` (arch/18 §2.5).

### 3.4. Инструментация KafkaWorker (`src/KafkaWorker.App`)

- То же, что §3.3 (Meter `KafkaWorker`, единый словарь §2.2; процессы
  provisioning/deprovisioning/reassign/rotation/regen/надзор).
- **Коллектор Kafka-метрик** (arch/18 §4): hosted-сервис, тик
  `KafkaWorker:Metrics:CollectIntervalSec` (default 30), по Active-кластерам
  через `IKafkaAdminClientFactory`: группы+оффсеты+watermarks →
  `kafka_consumer_lag`; describe топиков → `kafka_under_replicated_
  partitions`; самонаблюдение `kafka_collector_last_success_timestamp_
  seconds`. Сбор read-only вне клэймов, ошибки не ретраятся, тики циклов
  не трогает.

### 3.5. Инструментация AdminPanel (`src/AdminPanel.Api`)

- `AddAppMetrics` + `MapAppMetrics` (Meter `AdminPanel`; сборка панели
  получает ProjectReference на `Shared.Metrics` — прецедент
  `AdminPanel.Infrastructure`): OTel http-метрики ASP.NET (§2.1) +
  `panel_refresher_last_success_timestamp_seconds` из refresher-тика
  (марк-метод у места существующего обновления снапшота etcd).

### 3.6. Стек хранения: профиль `metrics` dev-станда

- `dev-stand/adminpanel/docker-compose.yml` — сервисы `prometheus`
  (`prom/prometheus`, :9090), `grafana` (`grafana/grafana`, :3000),
  `alertmanager` (`prom/alertmanager`, :9093), профиль `metrics`; версии
  пин; env-override портов (`METRICS_*_PORT`).
- Конфиги в репо, `dev-stand/adminpanel/metrics/`:
  - `prometheus/prometheus.yml` (scrape 15 с: pgworker —
    `host.docker.internal:8080` (публикация deploy-compose, вне сети стенда);
    kafkaworker, adminpanel — DNS сети стенда (`kafkaworker:8080`,
    `adminpanel:8080`; fallback на host-публикации НЕ предусмотрен — полный
    стенд поднимает все сервисы в одной сети, «стенд части» честно виден как
    down-таргеты); patroni — static `hc1a:8008,
    hc1b:8008, hc2a:8008, hc2b:8008`), `prometheus/rules.yml` (алерты §3.7);
  - `grafana/provisioning/` (datasource Prometheus + дашборды из репо):
    `dashboards/workers.json`, `dashboards/kafka.json`, `dashboards/pg.json`
    (арх/18 §5.3);
  - `alertmanager/alertmanager.yml` (webhook-ресивер, URL из env
    `METRICS_ALERT_WEBHOOK_URL`; пусто — маршрут null/только UI).
- **Расширение Patroni-эмуляторов** (`dev-stand/adminpanel/sidecar/
  emulator.py`): эндпоинт `GET /metrics` — `pg_replica_lag_seconds{scope,
  node}` из уже собираемого состояния (`state` эмулятора), Prometheus
  text format.
- Полный подъём `checks/00-up.sh` включает профиль `metrics`; новый чек
  **`checks/65-metrics.sh`**: `/metrics` трёх сервисов отвечают, все
  scrape-джобы `up` (`/api/v1/targets`), rules зарегистрированы
  (`/api/v1/rules`), Grafana отдает дашборды, Alertmanager жив
  (`/api/v2/status`).

### 3.7. Алертинг (prometheus rules, пороги — стартовые, тюнинг в Grafana)

| Алерт | Выражение (суть) | Порог |
|---|---|---|
| `ServiceDown` | `up{job=~"pgworker\|kafkaworker\|adminpanel"} == 0` | 2 мин |
| `WorkerLoopStalled` | `time() − worker_loop_last_success_timestamp_seconds > 60` | 60 с (циклы 5 с; запас на ErrorDelay-бэкофф) |
| `SnapshotStale` | `worker_snapshot_age_seconds > X` | 8 ч (снапшоты раз в 6 ч) |
| `ProcessPhaseStuck` | `worker_process_phase_duration_seconds > X` | 30 мин (provision-фазы — 1 ч) |
| `KafkaUnderReplicated` | `sum by (cluster) (kafka_under_replicated_partitions) > 0` | 5 мин |
| `KafkaConsumerLagHigh` | `kafka_consumer_lag > X` | 1e6, 10 мин |
| `KafkaCollectorStalled` | `time() − kafka_collector_last_success_timestamp_seconds > 300` | 5 мин (фиксированный, ≥ 3×CollectIntervalSec) |
| `PgReplicaLagHigh` | `pg_replica_lag_seconds > X` | 30 с, 5 мин |

Аннотации алертов — `summary`/`description` (runbook-ссылки на arch/18).
Доставка — Alertmanager webhook (внешние каналы подключаются URL'ом;
конкретный канал — ответственность установки).

## 4. Фазы (план исполнения; каждая — с тестами и ревью)

- **Ф0 — arch-контракт (эта spec-фаза, выполнено)**: создан
  `arch/18-metrics.md`, обновлены `arch/README.md` (индекс),
  `arch/14-pgworker.md` (§7, границы), `arch/16-kafkaworker.md` (§7,
  границы).
- **Ф1 — Puzzle-модуль**: `PuzzleServer.Infrastructure.App.Metrics` +
  `docs/01.20-metrics.md` + строка индекса + пакеты + unit-тесты; изменения
  в `../Puzzle` по правилам его репо (коммит — по правилам Puzzle).
- **Ф2 — Shared.Metrics**: порт базы + `WorkerMetrics`-надстройка + проект
  в слnx (`/common/`) + `Shared.Metrics.UnitTests`; интеграционный тест
  `/metrics` (WebApplicationFactory на минимальном хосте: 200, text-format,
  канонические имена словаря — фиксирует фактические OTel-имена против
  arch/18 §2, риск M3).
- **Ф3 — PgWorker**: `AddAppMetrics`/`MapAppMetrics`, `HealthState`-гейджи,
  марк-методы фаз/операций в процессах; интеграционные тесты `/metrics`
  (без ApiKey; имена словаря); appsettings `PgWorker:Metrics`.
- **Ф4 — KafkaWorker**: то же + коллектор лагов/USR (тесты коллектора на
  фейковой `IKafkaAdminClientFactory`: лаги/USR считаются верно, ошибка
  сбора не валит тик); `KafkaWorker:Metrics`.
- **Ф5 — AdminPanel**: подключение `Shared.Metrics`, http-метрики, refresher
  -гейдж; интеграционный тест `/metrics` без cookie-авторизации.
- **Ф6 — стенд хранения**: профиль `metrics` compose, prometheus/grafana/
  alertmanager-конфиги, дашборды, rules, `/metrics` эмуляторов, `00-up.sh`
  (профиль в полном подъёме), чек `65-metrics.sh`; README стенда.
- **Ф7 — закрытие**: roadmap-чистка мерж-коммитом (теги `t04-metrics` из
  `arch/roadmap/pgworker.md` и `t04-kafka-metrics` из
  `arch/roadmap/kafkaworker.md` удалить), E2E-гейт воркеров (AGENTS:
  `Scale_AddEmptyShard` на свежем Release — тянем `src/PgWorker.App`).

Зависимости: Ф1 → Ф2 → {Ф3, Ф4, Ф5} → Ф6 → Ф7; Ф3/Ф4/Ф5 независимы между
собой (после Ф2).

## 5. Ограничения и вне скоупа

- **Не входит**: метрики самих Kafka-брокеров (JMX-exporter), push-модель
  (OTLP/remote write), прод-развёртывание мониторинга за пределами
  docker-стенда (паттерн — arch/18 §5.4), federate/file_sd из etcd,
  визуализация метрик внутри AdminPanel (панель — на etcd-снапшоте),
  TLS/mTLS scrape-грани (roadmap t03), балансировка бакетов по метрикам.
- **Узлы, создаваемые PgWorker в per-cluster сетях**, стендовым Prometheus
  недостижимы (изоляция сетей) — репликация скрейпится у стендовых
  Patroni-эмуляторов; прод-паттерн — arch/18 §5.4 (документация).
- **Сквозной перенос в Puzzle** ограничен базой (воркер-паттерн монорепо —
  в Puzzle воркеров нет; пользовательское решение Д6).
- Поведение сервисов не меняется: `/healthz`, панели, циклы — как были;
  метрики — только наблюдение (M-принцип §2).
- `worker-*`/`kafka-*`/`panel-*`/`pg_replica_lag_seconds` — контракт
  arch/18 §2; имена стабильны в рамках версии, ломающие изменения — через
  правку arch/18 + упоминание в changelog задачи.

## 6. Критерии приёмки

1. `dotnet build src/PgWorker.slnx` — 0 warnings; `dotnet test` зелёный
   (unit+integration монорепо, включая `Shared.Metrics.UnitTests` и
   `/metrics`-тесты трёх сервисов).
2. Puzzle: модуль `Infrastructure.App.Metrics` + `docs/01.20-metrics.md`
   (+индекс) закоммичены по правилам Puzzle; юнит-тесты Puzzle зелёные.
3. Все три сервиса в докере отдают `/metrics` (200, Prometheus text
   format) с каноническими именами arch/18 §2 (проверено интеграционными
   тестами и чеком `65-metrics.sh`); без ApiKey/cookie.
4. Стенд: полный подъём `00-up.sh` поднимает профиль `metrics`;
   Prometheus: все джобы `up` (pgworker, kafkaworker, adminpanel, patroni);
   Grafana: три дашборда с данными; rules видны в `/api/v1/rules`;
   Alertmanager: webhook-ресивер настраивается env (пусто — только UI).
5. Алерт-симуляция чеком: останов kafkaworker-контейнера → `ServiceDown`
   (up==0) срабатывает ≤ 2 мин (проверка в `65-metrics.sh`).
6. E2E-гейт AGENTS: `PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx
   -c Release --filter FullyQualifiedName~Scale_AddEmptyShard` зелёный
   на свежем Release.
7. Roadmap-чистка мерж-коммитом: теги `t04-metrics`/`t04-kafka-metrics`
   удалены из `arch/roadmap/*.md` (списки и `←`-зависимости).
8. Документация: README стенда дополнен (профиль `metrics`, порты, env),
   arch/18 — канон (обновлён при отклонениях, найденных в имплементации).

## 7. Решения пользователя (зафиксированы, вопросы заданы по одному)

- **Д1 — стек**: OpenTelemetry (`System.Diagnostics.Metrics` + официальный
  `OpenTelemetry.Exporter.Prometheus.AspNetCore`).
- **Д2 — скоуп callers**: все три сервиса (оба воркера + AdminPanel).
- **Д3 — хранение**: Prometheus + Grafana в dev-стенде (compose-профиль,
  конфиги в репо); deploy-поставку внешнего мониторинга не трогаем.
- **Д4 — алертинг**: Prometheus alerting rules + Alertmanager c generic
  webhook-ресивером (URL — env установки).
- **Д5 — «лаги» PgWorker**: все три вида — репликация (scrape Patroni
  `:8008` напрямую, воркер не дублирует), живость циклов воркера,
  длительности операций (фазы процессов). Цель — «общее состояние и
  видеть отклонения».
- **Д6 — разложение**: база OTel-обвязки в Puzzle (канон-шаблон) + в
  монорепо порт с надстройкой воркер-паттерна только в монорепо;
  «все новые решения переносятся в Puzzle» (пользователь).

## 8. Риски (зеркало arch/18 §9 + специфика спеки)

| # | Риск | Митигация |
|---|---|---|
| S1 | Фактические экспортированные имена OTel отличаются от словаря (суффиксы единиц/`_total` по версиям экспортёра) | Ф2: интеграционный тест фиксирует имена против arch/18 §2; версии пин; расхождение — правка arch/18 тем же коммитом |
| S2 | Инструментация циклов/процессов рассыпается по коду процессов (сложно поддерживать) | единый seam `WorkerMetricsInstrumentation` + марк-методы в точках journal-записей (фазы уже журналируются — метрика рядом) |
| S3 | Кардинальность `worker_process_phase_duration_seconds` (cluster×process×phase) | фаза сбрасывается при завершении процесса; серии живут только у активных кластеров |
| S4 | Коллектор лагов грузит брокеров/AdminClient | один проход за тик 30 с, короткий таймаут, без ретраев; `KafkaConsumerLagHigh`-пороги стартовые |
| S5 | Порты 9090/3000/9093 коллизируют с хостом стенда | env-override compose (`METRICS_*_PORT`) |
| S6 | Эмулятор `/metrics` дрейфует от состояния эмулятора (lag из `state`) | состояние уже трекается с lock — эндпоинт читает тот же `state`; чек 65 сверяет серию у живой реплики |
| S7 | Puzzle-изменения требуют отдельного процесса/коммита в другом репо | фаза Ф1 изолирована; синхронизация версий пакетов через Directory.Packages.props обоих репо |

## 9. Тестирование (сводно)

- **Unit**: `Shared.Metrics.UnitTests` — семантика инструментов (тики по
  `ok`, фазы/сброс, возраст снапшота, claims); коллектор-логика на фейках
  (`KafkaWorker.UnitTests`): лаги = watermarks − committed, USR по ISR.
- **Integration** (WebApplicationFactory, без хост-портов): `/metrics`
  трёх сервисов — 200/text-format/имена словаря; `/metrics` минует
  ApiKey/cookie.
- **E2E-чек стенда**: `checks/65-metrics.sh` (§3.6) + симуляция алерта
  (down → `ServiceDown`).
- **E2E-гейт**: docker-E2E `Scale_AddEmptyShard` на свежем Release
  (AGENTS; меняется `src/PgWorker.App`).
