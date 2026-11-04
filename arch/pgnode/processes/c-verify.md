# C. VerifyTask (проверка полного)

Уровень 4. Ops-операция checksums-проверки полного. Функция —
[05-verify.md](../05-verify.md); оркестрация —
[backups/01-full-backup.md](../../backups/01-full-backup.md) §7; состояния —
[enums.md](enums.md).

## 1. Фазы

```
POST /api/ops/verify (S3-контекст read-списка в теле)
  202 — принято
  409 — общий ops-реестр занят (своя/чужая операция неразличима)
downloading
  скачивание full/<id>/ 1:1 в <Staging:Dir>/full/<id>/
verifying
  pg_verifybackup (manifest SHA256, вкл. pg_wal/ набора)
succeeded | failed | cancelled
```

`GET /api/ops/verify` — снимок: фаза, `started_unix`, `error`. Staging
чистится при любом исходе.

## 2. Исходы

- Сбой скачивания (S3/сеть/staging) — `failed` с фазой `downloading`
  (transient для воркера: PENDING сохраняется, повторный POST следующим
  тиком, операция с нуля).
- `pg_verifybackup` ≠ 0 (фаза `verifying`) — permanent: данные плохие.
- `POST /api/ops/cancel` → `cancelled` (не failed): воркер возвращает
  кандидата в PENDING с `checked_unix=now`.
- Рестарт процесса → GET 404 (vanished) → воркер повторяет POST.

etcd-контракт verify приёмник не знает; статусы в etcd пишет воркер.
