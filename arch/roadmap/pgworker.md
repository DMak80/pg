# Roadmap: сервис PgWorker

Отложенные задачи оркестратора PgWorker (out of scope MVP — спецификация
`docs/superpowers/2026-08-23-pgworker-backend/spec.md` §2; канон сервиса —
[../14-pgworker.md](../14-pgworker.md)).

## Задачи

- **`t02-per-cluster-secrets`** — ротация секретов per-cluster (смена без
  остановки записи), генерация per-cluster `bucket_mover`, интеграция с
  secret-manager. Генерация per-cluster app-секрета в etcd сделана
  (2026-08-28, feat-etcd-password-field).
- **`t03-docker-tls-ssh`** — TLS к Docker Engine API и SSH-туннели к
  docker-хостам (сейчас plaintext TCP/unix-socket в доверенной сети),
  RBAC/docker-группы; сюда же — транспортная безопасность HTTP API
  PgWorker (arch/14 §1.1): mTLS/сертификаты вместо голого `X-Api-Key`
  в закрытой сети, отдельные креды панели и сида.
- **`t04-metrics`** — Prometheus-метрики PgWorker (фазы процессов, клэймы,
  лаги, возраст снапшотов) + алертинг во внешние системы.
- **`t05-quarantine-merge`** — слияние/восстановление данных карантинного
  шарда после его возврата (runbook-операция после аварийной эвакуации E0–E4:
  сверка записей «осиротевших» схем с новыми, разрешение конфликтов).
- **`t90-portalloc-parallel-race`** — гонка ПАРАЛЛЕЛЬНОГО provisioning: два
  свежих кластера на одном docker-хосте, засеянные одновременно, могут
  получить одинаковые порты — PortAllocator дедуплицирует по живым
  docker-биндингам и записям portalloc всех кластеров
  (`/pgworker/portalloc/*`, arch/14 §2.4), но два инстанса, прочитавшие
  префикс одновременно ДО первой записи друг друга, общей картины не имеют
  (воспроизведено на dev-станде 2026-08-25 — контейнеры вторых кластеров в
  Created с «port is already allocated»). Нужно: глобальный txn/курсор на
  общий диапазон хоста. Обход: сеять кластеры последовательно.
- **`t08-unify-adminpanel-duplicates`** — унификация дублей кода после переноса
  AdminPanel в монорепо (2026-08-27): etcd-клиент `AdminPanel.Etcd/Client/`
  (`EtcdGateway`/`IEtcdGateway`/`Kv` — урезанный аналог `PgWorker.Etcd/Client`,
  без Coordination) → перевод панели на `PgWorker.Etcd`; Puzzle-каркас
  `AdminPanel.Infrastructure` (attribute-DI, CQRS, `Result`, Traces) → перевод
  на `PgWorker.Core`. Механика: панель получает ProjectReference на общие
  сборки, дубли удаляются; поведение обеих систем не меняется (тесты зелёные).
- **`t09-e2e-release-regression`** — регрессия E2E-тестов PgWorker на свежем
  Release-бинаре (найдена 2026-09-02 при финальном прогоне
  t06-kafka-node-regen): стабильно падают 4 кейса E2eFixture —
  `Scale_TakeoverMidAdd`, `Scale_AddEmptyShard`, `Acceptance_Ac2_To_Ac7`,
  `Move_Lifecycle_Chain` — в т.ч. изолированно. A/B-эксперимент: Release от
  29.08 зелёный, после пересборки на чистом main — те же падения (ветка t06
  код PgWorker не трогала) → регрессия PgWorker.App между 29.08 и 02.09.
  Маскировка: E2eFixture запускает готовый `bin/Release` PgWorker.App и не
  пересобирает — устаревший зелёный бинарь скрывал проблему. Нужно: бисект до
  коммита-виновника, фикс логики (или фикстуры, если дефект теста) и правило
  пересборки Release в E2eFixture при устаревании вместо молчаливого запуска
  старого. Обход до фикса: прогон E2E только после явной пересборки Release.
