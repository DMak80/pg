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
  соединение BASE_BACKUP (127.0.0.1:5432, TARGET=CLIENT, WAL STREAM,
  CHECKPOINT fast, MANIFEST_CHECKSUMS SHA256) → потоковая перепаковка
  tar-потоков в единый tar → multipart-заливка частями (16 МиБ,
  параллельно, Content-MD5) → CompleteMultipartUpload (объект
  full/<id>.tar появляется атомарно)
uploading/uploading
  дубль закрытых сегментов набора в wal/ (Range-GET из tar-объекта,
  строчные имена, гвард чужой цепочки)
succeeded | failed (+ error)
```

Статус — IMMUTABLE-снимок в памяти: фаза, `wal_start_segment`, `size_bytes`,
прогресс (`bytes_done` прочитано байт / `parts_done` залитых частей),
`error`; `wal_start_segment` парсится из `backup_label` tar-потока и
публикуется с фазы UPLOADING (для рано упавших может отсутствовать). Резюмов
нет; staging у операции нет — чистить на диске нечего, мусорные
multipart-сессии гасятся Abort'ом при отказе/отмене (упущенные —
супервизором по возрасту).

## 2. Исходы

- Транспортный отказ endpoint'а посреди попытки → AbortMultipartUpload →
  полный рестарт попытки на следующей ноде write-списка (каждая попытка
  целиком на одном endpoint); исчерпание списка → `failed` с маркером
  `s3-pool-exhausted`; воркер фиксирует FAILED и переснятие по бэкоффу.
- Отказ/несовместимость протокола BASE_BACKUP (auth, версия, multiple
  tablespaces, размер >160 ГиБ) → `failed` fail-fast с диагностикой.
- Отмена (`POST /api/backup/full/cancel` / бюджет воркера) — гасит сокет
  BASE_BACKUP + AbortMultipartUpload, `failed` с причиной `cancelled`;
  гейт 503 идущую операцию не рвёт.
- Рестарт процесса/ноды → GET 404 (vanished) → воркер FAILED
  `operation-vanished` → переснятие новым id (резюмов нет).
- Чужая инкарнация при дубле WAL набора — `failed` «foreign WAL chain».

etcd-контракт полного приёмник не знает; статусы в etcd пишет ТОЛЬКО воркер.
