# Roadmap: сервис PgWorker

Отложенные задачи оркестратора PgWorker (out of scope MVP — спецификация
`docs/superpowers/2026-08-23-pgworker-backend/spec.md` §2; канон сервиса —
[../14-pgworker.md](../14-pgworker.md)).

## Задачи

- **`t01-move-bucket-csharp`** — полный C#-порт планового переезда бакета
  (механика P1–P8 из [../11-bucket-sharding.md](../11-bucket-sharding.md) §5:
  логическая репликация, заморозка, cutover, rollback, finalize — эквивалент
  `move-bucket.sh`/`abort-move.sh`). Эвакуация MVP закрывает только аварийный
  случай «шард умер целиком» (копировать не с чего); штатные переезды живых
  шардов остаются скриптами до этого порта. ← базовые процессы PgWorker
  (provisioning/deprovisioning/надзор — уже в `main`).
- **`t02-per-cluster-secrets`** — генерация и ротация секретов per-cluster
  (сейчас per-install из env, Д7): пароли app/bucket_admin/bucket_mover на
  кластер, смена без остановки записи, интеграция с secret-manager.
- **`t03-docker-tls-ssh`** — TLS к Docker Engine API и SSH-туннели к
  docker-хостам (сейчас plaintext TCP/unix-socket в доверенной сети),
  RBAC/docker-группы.
- **`t04-metrics`** — Prometheus-метрики PgWorker (фазы процессов, клэймы,
  лаги, возраст снапшотов) + алертинг во внешние системы.
- **`t05-quarantine-merge`** — слияние/восстановление данных карантинного
  шарда после его возврата (runbook-операция после аварийной эвакуации E0–E4:
  сверка записей «осиротевших» схем с новыми, разрешение конфликтов).
- **`t06-shard-autoscaling`** — add/remove-shard из панели AdminPanel с
  оркестрацией PgWorker (подъём/демонтаж шарда + перебалансировка бакетов
  поверх `t01-move-bucket-csharp`).
