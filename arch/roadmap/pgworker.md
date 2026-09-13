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
- **`t11-pgtune-params-convergence`** — конвергенция pg-параметров работающих
  нод: выравнивание живого конфига PostgreSQL с рассчитанным PGTune
  (PATCH /config Patroni + pending_restart для postmaster-параметров —
  `max_connections`, `shared_buffers`, …). Сегодня PGTune-параметры
  (`PgTune.Calculate`, расчёт при provision по `request_*`) применяются только
  при bootstrap контейнера; смена `PgWorker:Pgtune`/`request_*` подхватывается
  лишь пересозданными нодами — живые продолжают работать на прежнем конфиге
  (осознанный дрейф, spec `docs/superpowers/2026-09-12-pgtune-provision/spec.md`
  §6). Задача: автоматическое выравнивание без пересоздания.
- **`t12-integration-red-debt`** — долг красных интеграций (4 падения,
  предсуществующие — падают идентично на базе до диффа t07; выявлены на
  мерж-гейте t07, 2026-09-13): `AdoptionContractTests` ×3 — сиды/фикстуры
  старых контрактов не обновлены под обязательный `request_mem`, появившийся
  в f6d4574; флейм клэйма «sc3» в `ShardScaleContractTests` в полной сборке
  (джоба соседнего сценария перехватывает клэйм). Задача: починить сиды и
  устранить флейм клэйма — полная интеграционная серия обязана быть зелёной,
  мерж-гейты не должны принимать красную базу как норму.
