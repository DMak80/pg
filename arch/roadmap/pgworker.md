# Roadmap: сервис PgWorker

Отложенные задачи оркестратора PgWorker (out of scope MVP — спецификация
`docs/superpowers/2026-08-23-pgworker-backend/spec.md` §2; канон сервиса —
[../14-pgworker.md](../14-pgworker.md)).

## Задачи

- **`t02-per-cluster-secrets`** — ротация секретов per-cluster (смена без
  остановки записи), генерация per-cluster `bucket_mover`, интеграция с
  secret-manager. Генерация per-cluster app-секрета в etcd сделана
  (2026-08-28, feat-etcd-password-field).
- **`t05-quarantine-merge`** — слияние/восстановление данных карантинного
  шарда после его возврата (runbook-операция после аварийной эвакуации E0–E4:
  сверка записей «осиротевших» схем с новыми, разрешение конфликтов).
- **`t08-unify-adminpanel-duplicates`** — унификация дублей кода после переноса
  AdminPanel в монорепо (2026-08-27): etcd-клиент `AdminPanel.Etcd/Client/`
  (`EtcdGateway`/`IEtcdGateway`/`Kv` — урезанный аналог `PgWorker.Etcd/Client`,
  без Coordination) → перевод панели на `PgWorker.Etcd`; Puzzle-каркас
  `AdminPanel.Infrastructure` (attribute-DI, CQRS, `Result`, Traces) → перевод
  на `PgWorker.Core`. Механика: панель получает ProjectReference на общие
  сборки, дубли удаляются; поведение обеих систем не меняется (тесты зелёные).
  Третья группа (t03, 2026-09-05): TLS-инфраструктура mTLS-граней —
  `ApiTlsEndpoints` (PgWorker.App) ↔ `TlsEndpoints` (KafkaWorker.App) ↔
  TLS-хелперы (`DockerTlsMaterial.ValidateChain`, `WorkerTlsHandler`,
  env-биндинги/PEM-дуализм) — унифицировать тем же проходом.
