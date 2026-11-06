# D. RestoreTask (восстановление PGDATA)

Уровень 4. Ops-операция restore шарда на первой ноде. Функция —
[06-restore-drill.md](../06-restore-drill.md) §2/§4; оркестрация —
[backups/processes/f-restore.md](../../backups/processes/f-restore.md);
состояния — [enums.md](enums.md).

## 1. Фазы

```
POST /api/ops/restore (backup_id, target_time?, recovery_timeout_sec,
                       chain_life?, sha256?, S3-контекст)
  202 — принято
  400 — id вне allowlist имени
  409 — общий ops-реестр занят (своя/чужая операция неразличима)
downloading
  фиксация endpoint чтения (HEAD) → GetObject-стрим full/<id>.tar →
  потоковая распаковка tar в PGDATA (без архива на диске) с валидацией
  имён/симлинков (06 §2); сквозной sha256 против поля команды — до старта
  postgres
recovering
  локальный postgres: pg_ctl start -w -t <recovery_timeout_sec>,
  fetch-wal, поллинг pg_is_in_recovery до promote, снятие
  pg_current_wal_lsn, pg_ctl stop -m fast, чистка recovery-остатков
succeeded | failed | cancelled
```

`GET /api/ops/restore` — снимок: фаза, `restored_to_lsn`, `system_id`,
`started_unix`, `error`.

Инвариант фазы `recovering`: старт выполняется в подготовленный каталог
(postmaster прошлой инкарнации остановлен, остаточный `postmaster.pid`
снят), при любом исходе операции postmaster остановлен до чистки и возврата
Patroni — механика [06-restore-drill.md](../06-restore-drill.md) §2.

## 2. Обязательные шаги

1. Останов Patroni ноды (`supervisorctl stop spilo`) с ПОДТВЕРЖДЕНИЕМ:
   stop не удался — операция не начинается; возврат `start spilo` в finally
   при любом исходе.
2. Скачивание набора, `restore_command` (fetch-wal), `auto.conf`
   override/pristine, `recovery.signal`, повторное удаление
   `standby.signal` перед `pg_ctl start`, `chmod 700`, uid/gid 101,
   `HOME=/tmp`.
3. `pg_ctl start -w -t` тем же бюджетом `recovery_timeout_sec`; поллинг
   `pg_is_in_recovery` с бюджетом `Restore:RecoveryTimeoutSec`; promote;
   stop fast; возврат pristine `auto.conf`; `system_id` из
   `pg_controldata`.

## 3. Исходы

- Бюджет recovery исчерпан/target не достигнут — `failed` с `error`
  (permanent для воркера: новая заявка оператора).
- `POST /api/ops/cancel` → `cancelled` (не failed); воркер возвращает
  шард к состоянию «как до старта» (снос ноды/volume, снятие щита).
- Рестарт процесса → GET 404 (vanished) → воркер по бюджетам: демонтаж/
  повтор заявки.
