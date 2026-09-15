# Roadmap: сервис PgWorker

Отложенные задачи оркестратора PgWorker (out of scope MVP — спецификация
`docs/superpowers/2026-08-23-pgworker-backend/spec.md` §2; канон сервиса —
[../14-pgworker.md](../14-pgworker.md)).

## Задачи

- **`t02-external-secret-manager`** — интеграция с внешним secret-manager:
  публикация/чтение per-install/per-cluster секретов внешним SM поверх
  etcd-канона (per-cluster app/bucket_admin/mover + backup — arch/14 §4/§5 I,
  arch/19 §7; слито 2026-09-06, merge 5bcd467). Решение пользователя
  (2026-09-06): etcd остаётся единственным хранилищем per-cluster секретов
  до этой интеграции.
- **`t05-quarantine-merge`** — слияние/восстановление данных карантинного
  шарда после его возврата (runbook-операция после аварийной эвакуации E0–E4:
  сверка записей «осиротевших» схем с новыми, разрешение конфликтов).
- **`t09-unify-worker-duplicates`** — Pg↔Kfw-дубли вне панельного контура (осознанно
  не тронуты t08): Coordination `ClaimStore`/`PortAllocLock`/`WorkJournal`,
  `SnapshotJob`, `Writing/{PlanPut,ValidationError}`, `Planning/{PlacementPlanner,
  PortAllocator}` — перенос с параметризацией префиксов ключей и моделей журнала
  (PgWorker несёт `RetrySeries`/`EvacuationJournal`).
- **`t10-rs-dr-master-readiness-gate`** — доработка E2E-сценария rs-dr
  (`Restore_NewCluster_FromSourcePrefix`, `E2eRestoreScenarios`): добавить гейт
  готовности мастера после restore перед финальным чтением — сейчас финальный
  `SELECT count(*)` ловит рестарт-окно Patroni/postgres (Npgsql 57P01
  «terminating connection due to administrator command»,
  `E2eRestoreScenarios.cs` ScalarAsync:693 / вызов :338). Флейк системный на
  текущей машине, воспроизводится на main-бейзлайне cf06ce0 без t09-изменений
  (логи `/tmp/t09-rerun/pg-main-baseline.log`).
