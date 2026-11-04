# Единая телеметрия: Prometheus-метрики

Уровень 2. Единое решение по сбору и хранению Prometheus-метрик для всех
.NET-сервисов монорепо (PgWorker, KafkaWorker, AdminPanel, ValkeyWorker,
S3Worker, EtcdWorker) и будущих проектов с аналогичным стеком. Канон-шаблон
каркаса — модуль `Infrastructure.App.Metrics` проекта `../Puzzle`
(док `docs/01.20-metrics.md`); в монорепо живёт его порт — общая сборка
`src/Shared.Metrics` — с надстройкой воркер-паттерна. Стек хранения —
Prometheus + Grafana + Alertmanager (профиль `metrics` dev-стенда, конфиги
в репо).

Ключевые свойства:

- **Инструментация** — `System.Diagnostics.Metrics` (Meter API, встроен в
  .NET 10) + официальный `OpenTelemetry.Exporter.Prometheus.AspNetCore`
  (эндпоинт `/metrics`); имена/типы/лейблы — единый словарь
  [02-dictionary.md](02-dictionary.md).
- **Экспозиция** — тот же Kestrel-порт, что `/healthz` (воркеры `:8080`);
  `/metrics` и `/healthz` — открытая грань доверенной закрытой docker-сети
  И хост-публикаций портов API docker-хоста (граница доверия грани —
  периметр docker-хоста, а не только docker-сеть; HTTPS без клиентского
  серта; mTLS у воркеров обязателен только на `/api`). У панели `/metrics` —
  без cookie-авторизации (guard — только `/api/*`).
- **Метрики — пассивные наблюдатели**: инструменты пишут в Meter, экспорт —
  pull (scrape Prometheus); сбор метрик никогда не влияет на поведение
  воркеров/панели (никаких throw наружу, никакой блокировки циклов).
- **Канон имён** — финальные имена Prometheus-формата (после OTel-экспорта);
  в коде — dot-нотация Meter. Словарь общий для воркеров — дашборды и
  алерты пишутся против одного набора имён.

Границы (что НЕ входит): метрики самих Kafka-брокеров (JMX-exporter — вне
стенда; лаги/USR снимает коллектор воркера), push-модель (OTLP/remote write),
прод-развёртывание мониторинга за пределами docker-стенда (паттерн
документируется [05-storage.md](05-storage.md) §4), визуализация метрик
внутри AdminPanel (панель живёт на etcd-снапшоте;
[adminpanel/01-architecture.md](../adminpanel/01-architecture.md): «P21 просит
дашборд, не Prometheus»).

## Документы

| Документ | Содержание |
|---|---|
| [01-framework.md](01-framework.md) | структура решения, подключение нового .NET-проекта, конфигурация |
| [02-dictionary.md](02-dictionary.md) | словарь метрик (общие, воркер-паттерн, домены Kafka/Valkey/PG/бэкапы/приёмник) |
| [03-exposure.md](03-exposure.md) | экспозиция и безопасность |
| [04-collectors.md](04-collectors.md) | коллекторы доменных метрик Kafka и Valkey |
| [05-storage.md](05-storage.md) | стек хранения, scrape-таргеты, дашборды, file_sd |
| [06-testing.md](06-testing.md) | канон уровня тестирования |
| [07-risks.md](07-risks.md) | риски и митигации |
