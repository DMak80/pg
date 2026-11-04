# B. FullBackupTask (операция полного)

Уровень 4. Фоновая задача полного в процессе приёмника. Функция —
[04-full-backup.md](../04-full-backup.md); политика —
[backups/01-full-backup.md](../../backups/01-full-backup.md); состояния —
[enums.md](enums.md).

## 1. Фазы

```
POST /api/backup/full {"id"}
  503 — стоп-гейт (STOPPED / backups-disabled / Enabled=false)
  409 — операция уже идёт
  202 — принято
running/basebackup
  чистка staging → pg_basebackup -D <staging>/<id>/full (127.0.0.1:5432,
  -X stream --checkpoint=fast --manifest-checksums=SHA256)
uploading/uploading
  wal_start_segment из backup_label → загрузка файлов 1:1 в full/<id>/
  → дубль закрытых сегментов набора в wal/ (строчные имена)
succeeded | failed (+ error)
```

Статус — IMMUTABLE-снимок в памяти: фаза, `wal_start_segment`,
`size_bytes`, `error`; `wal_start_segment` заполняется с фазы UPLOADING
(для рано упавших может отсутствовать). Staging чистится после исхода и при
старте новой операции; ENOSPC — реактивный FAILED.

## 2. Исходы

- Транспортный отказ/исчерпание write-списка пула → `failed` с маркером
  `s3-pool-exhausted`; воркер фиксирует FAILED и переснятие по бэкоффу.
- Отмена (`POST /api/backup/full/cancel`) — гасит дерево child-процесса,
  `failed` с причиной `cancelled`, staging чистится; гейт 503 идущую
  операцию не рвёт.
- Рестарт процесса/ноды → GET 404 (vanished) → воркер FAILED
  `operation-vanished` → переснятие новым id (резюмов нет).
- Чужая инкарнация при дубле WAL набора — `failed` «foreign WAL chain».

etcd-контракт полного приёмник не знает; статусы в etcd пишет ТОЛЬКО воркер.
