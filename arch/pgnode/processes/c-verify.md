# C. VerifyTask (проверка полного)

Уровень 4. Ops-операция потоковой checksums-проверки полного. Функция —
[05-verify.md](../05-verify.md); оркестрация —
[backups/01-full-backup.md](../../backups/01-full-backup.md) §7; состояния —
[enums.md](enums.md).

## 1. Фазы

```
POST /api/ops/verify (S3-контекст read-списка в теле)
  202 — принято
  409 — общий ops-реестр занят (своя/чужая операция неразличима)
downloading
  фиксация endpoint чтения (HEAD) → Range-хвост объекта full/<id>.tar →
  разбор backup_manifest (валидация checksum заголовков tar)
verifying
  проход по объекту: sha256 каждого файла, сверка с manifest
  (перечень/размеры/хэши; пропускаются backup_manifest и WAL-сегменты
  pg_wal/<segment> — 05-verify.md §2), сквозной sha256 — против
  etcd-поля sha256 полного
succeeded | failed | cancelled
```

`GET /api/ops/verify` — снимок: фаза, `started_unix`, `error`. Распаковки на
диск нет; staging не используется, чистить нечего.

## 2. Исходы

- Транспортный сбой чтения (S3/сеть, manifest не найден в доступном
  диапазоне хвоста) — `failed` с фазой `downloading` (transient для
  воркера: PENDING сохраняется, повторный POST следующим тиком, операция
  с нуля).
- Расхождение с manifest — фаза `verifying` — permanent: данные плохие.
- `POST /api/ops/cancel` → `cancelled` (не failed): воркер возвращает
  кандидата в PENDING с `checked_unix=now`.
- Рестарт процесса → GET 404 (vanished) → воркер повторяет POST.
- Снимок с чужим `id` ИЛИ чужим `kind` (restore/drill того же полного),
  а также снимок без `kind` (общий ops-реестр отдал операцию другого вида
  или старого контракта) — не итог проверки: супервиз перезапускает
  операцию vanished-семантикой ([backups/01-full-backup.md](../../backups/01-full-backup.md)
  §7 — родительский уровень).

etcd-контракт verify приёмник не знает; статусы в etcd пишет воркер.
