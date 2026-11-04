# E. DrillTask (дрилл)

Уровень 4. Ops-операция дрилла: та же механика restore, но в scratch-каталог
и без REJOINING. Функция — [06-restore-drill.md](../06-restore-drill.md) §3;
оркестрация — [backups/processes/g-drill.md](../../backups/processes/g-drill.md);
состояния — [enums.md](enums.md).

## 1. Фазы

```
POST /api/ops/restore {id} (id дрилла; latest, БЕЗ target)
  202 — принято
  409 — общий ops-реестр занят
downloading
  GetObject-стрим full/<id>.tar → потоковая распаковка tar в
  /home/postgres/pgdata/backup-drill/<id>/pgdata (scratch, НЕ PGDATA ноды)
recovering
  локальный postgres (unix-socket, listen_addresses=''), fetch-wal,
  поллинг pg_is_in_recovery, снятие pg_current_wal_lsn, stop
succeeded | failed | cancelled
```

`GET /api/ops/restore` — снимок: фаза, `restored_to_lsn`, `started_unix`,
`error`; `system_id` дриллу не нужен (воркер дрилла игнорирует).

## 2. Отличия от restore

- PGDATA прод-ноды не трогается; Patroni ноды НЕ останавливается (дрилл
  живёт рядом).
- Scratch-каталог приёмник чистит при ЛЮБОМ исходе (вкл. отмену и failed) —
  терминальный ключ дрилла сразу чистый, docker-сноса нет.
- По завершении сразу терминальный исход (REJOINING-фаз нет).
- Отмена (`POST /api/ops/cancel`) → `cancelled` + чистка scratch; воркер
  удаляет ключ дрилла.
