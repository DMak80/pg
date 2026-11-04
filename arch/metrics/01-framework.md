# Телеметрия: структура решения и подключение

Уровень 3. Каркас телеметрии: структура решения, подключение нового
.NET-проекта, конфигурация. Родитель — [README.md](README.md); соседние
документы — [02-dictionary.md](02-dictionary.md),
[03-exposure.md](03-exposure.md), [04-collectors.md](04-collectors.md),
[05-storage.md](05-storage.md), [06-testing.md](06-testing.md),
[07-risks.md](07-risks.md).

## 1. Структура решения

```
Puzzle (архитектурный шаблон)          монорепо (pg)
───────────────────────────            ──────────────────────────────
Infrastructure.App.Metrics   ──порт──► src/Shared.Metrics
  базовая OTel-обвязка:                 порт базы (копия, паттерн
  + AddAppMetrics()/MapAppMetrics()     Shared.Core (бывший AdminPanel.Infrastructure))
  + конвенции имён/лейблов            + WorkerMetrics (надстройка
  + docs/01.20-metrics.md               воркер-паттерна — только
                                        в монорепо: в Puzzle воркеров нет)
```

- **База (Puzzle = канон)**: DI-расширение `AddAppMetrics` (регистрация
  OTel-MeterProvider + сервисных Meter), `MapAppMetrics` (эндпоинт `/metrics`),
  конвенции: имя Meter = имя системы (`PgWorker`/`KafkaWorker`/`AdminPanel`),
  dot-нотация инструментов, единицы — секунды/штуки (OTel-суффиксы при
  экспорте), включение/путь — `[Config]`-секция `<Service>:Metrics`.
- **Надстройка WorkerMetrics (монорепо)**: типизированные инструменты
  воркер-паттерна (циклы/клэймы/фазы/операции/снапшоты) — общий словарь
  для воркеров (их `HealthState`-паттерн уже идентичен). Серии питаются
  марк-методами `WorkerMetricsInstrumentation`, вызываемыми циклами и
  подпиской на фазовые записи журнала работы (рядом с существующими
  `health.Mark*`; единственный источник на серию); `HealthState` остаётся
  нетронутым источником только `/healthz`.
- **Подключение нового .NET-проекта** — по докам Puzzle `01.20-metrics.md`
  (копия модуля или ProjectReference на общую сборку монорепо) — см. §2.

## 2. Подключение нового .NET-проекта

1. Скопировать модуль `Infrastructure.App.Metrics` из Puzzle (или
   ProjectReference на `Shared.Metrics` внутри монорепо) — паттерн
   `Shared.Core (бывший AdminPanel.Infrastructure)`.
2. `[Config]`-секция `<Service>:Metrics { Enabled=true, Path="/metrics" }`.
3. `services.AddAppMetrics(...)`, `app.MapAppMetrics()`; доменные метрики —
   свои инструменты dot-нотацией по конвенциям `01.20-metrics.md`; для
   воркер-паттерна — переиспользовать
   [02-dictionary.md](02-dictionary.md) §2.2, а не плодить синонимы.
4. Добавить scrape-джобу в `prometheus.yml` стенда.

## 3. Конфигурация

```
<Service>:Metrics { Enabled=true, Path="/metrics" }   # все сервисы
KafkaWorker:Metrics { CollectIntervalSec=30 }          # коллектор
ValkeyWorker:Metrics { CollectIntervalSec=30 }         # коллектор
SdGenerator:Etcd:Endpoints[]           # узлы HA-контура
SdGenerator:RefreshIntervalSec=15      # <=0 → 15 + warning
SdGenerator:OutputPath=/sd/patroni-nodes.json
SdGenerator:PgNodeOutputPath=/sd/pgnode-nodes.json   # те же ноды, порт 8080
SdGenerator:Metrics { Enabled=true, Path="/metrics" }
# стенд (env-override compose):
METRICS_PROMETHEUS_PORT=9090, METRICS_GRAFANA_PORT=3000,
METRICS_ALERTMANAGER_PORT=9093, METRICS_ALERT_WEBHOOK_URL=   # пусто — только UI
```
