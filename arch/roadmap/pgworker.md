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
- **`t07-unify-docker-engine`** — унификация docker-движков: три копии
  DockerEngine/ClusterDriver (PgWorker.Docker — SSH-туннели/TLS-docker;
  KafkaWorker.Docker; ValkeyWorker.Docker — копия kfw, t02) разошлись
  (diff kfw/pg ~492 строк). Вынос в общую Shared-сборку с сохранением
  Pg-специфики (SSH/TLS) как опций. Мерж-гейт: полный docker-E2E
  Pg+Kfw+Valkey (все три домена).
