# Roadmap: подсистема бэкапов шардов

Отложенные задачи подсистемы бэкапов PG-шардов. **PgWorker — хозяин
бэкапов**: он сам создаёт, ретраит/пересоздаёт при неудаче, чистит лишнее,
обрабатывает пограничные случаи и сигнализирует в алертинг панели — что
сломалось и какие действия предприняты. Канон подсистемы —
[../backups/README.md](../backups/README.md); контекст сервиса —
[../pgworker/README.md](../pgworker/README.md).

Порядок: канон → полный бэкап → online-архивация WAL → проверки →
восстановление → ретенция → супервизор-цикл над всем → панель управления
хранилищем (S3).

## Задачи

- **`t01-basebackup-streaming-client`** — свой стриминговый клиент
  `BASE_BACKUP`: физический полный бэкап ОДНИМ S3-объектом, tar-поток
  напрямую в S3 multipart, без второй копии PGDATA на диске; потоковые
  restore и verify.

  **Почему возникла.** Текущий полный бэкап — plain-режим `pg_basebackup`
  (`-D <staging>`), затем пофайловая заливка 1:1 в `full/<id>/`
  ([../pgnode/04-full-backup.md](../pgnode/04-full-backup.md),
  [../backups/06-s3-layout.md](../backups/06-s3-layout.md)). Факты:
  один полный E2E-набора — `files=1304` при 73 МБ; для 100 ГБ это десятки
  тысяч S3-объектов. `pg_basebackup` не имеет режима stdout:
  `-D` — каталог, tar (`-Ft`) — файл(ы) в каталоге, `-t/--target=server`
  — каталог на сервере; `-i` — инкремент, не стрим. Итог: на диске
  PGDATA + staging-копия (2×, для 100 ГБ — 200 ГБ), в S3 — тысячи
  объектов с per-object put/листингами. Логический бэкап (`pg_dump`) эту
  задачу не решает: он не даёт PITR/восстановления PGDATA/HA — репликация
  у нас физическая ([../pgnode/03-wal-receiver.md](../pgnode/03-wal-receiver.md)).
  Требование владельца: база 100 ГБ не должна занимать 200 ГБ на диске
  и не должна превращаться в десятки тысяч S3-объектов, при сохранении
  PITR/restore/verify.

  **Требования.**
  1. *Формат*: один объект `full/<id>.tar` (или эквивалент одним ключом)
     — tar-контейнер с PGDATA, `pg_wal` и `backup_manifest`; маркер
     формата в имени/ключе и/или манифесте. Полные создаются и читаются
     только в этом формате (прежнего пофайлового режима нет — система
     не развёрнута).
  2. *Источник*: протокол `BASE_BACKUP` физической репликации
     (`TARGET=client`, tar-формат, WAL внутри), поверх существующего
     стека физической репликации; без `pg_basebackup` и без
     staging-каталога PGDATA. Пик диска ≈ PGDATA; пик памяти —
     ограниченный буфер частей (N×part), не размер базы.
  3. *Загрузка*: поток tar → S3 multipart (части 8–16 МиБ, параллельно
     по частям), `CreateMultipartUpload`/`UploadPart`/`Complete` на
     ОДНОМ endpoint пула (перебор нод посреди multipart недопустим;
     при отказе — `AbortMultipartUpload` + retry, mesh-репликация после
     Complete); таймауты и breaker пула сохраняются. `COMPLETED` — только
     после Complete + проверки целостности (Content-MD5 частей/ETag,
     sha256); до этого — `UPLOADING`; при отказе — `FAILED` с одной
     строкой причины, пересъём новым id.
  4. *Restore*: S3 GetObject → потоковая распаковка в PGDATA (без файла
     архива на диске), совместимо с текущими `target_time`/latest,
     TLI-переходами, `restore_command`/`fetch-wal`.
  5. *Verify*: потоковый верификатор по `backup_manifest`
     (sha256/CRC каждого файла) без распаковки на диск; статус verify и
     G2-гейт — как сейчас ([../pgnode/processes/c-verify.md](../pgnode/processes/c-verify.md)).
  6. *WAL набора*: закрытые сегменты `pg_wal/` дублируются в `wal/` —
     либо из tar-потока, либо явно зафиксированный отказ с обоснованием
     в каноне (restore/verify не должны зависеть от дубля).
  7. *Наблюдаемость*: фазы `basebackup|uploading`, прогресс по
     байтам/частям, `[PHASE]`-метки в E2E, `last_error` — однострочный.
  8. *Совместимость протокола*: клиент `BASE_BACKUP` соответствует
     версии протокола репликации сервера; несовместимость — fail-fast
     с диагностикой (без тихой порчи набора).
  9. *Безопасность*: пароль `backup_exec` — только env, не argv; TLS к S3
     — как сейчас; без P/Invoke.

  **Критерии готовности.** E2E: полный одним объектом → verify OK →
  restore `latest` и `target_time` (PITR через TLI) COMPLETED → retention
  удаляет объект; профиль «большой набор» подтверждает отсутствие второй
  копии на диске.
  Юниты: tar-writer/reader, multipart-обёртка (abort/retry/части),
  потоковый manifest-verifier, разбор `BASE_BACKUP`. Канон обновлён
  первым: [../backups/01-full-backup.md](../backups/01-full-backup.md),
  [../backups/06-s3-layout.md](../backups/06-s3-layout.md),
  [../pgnode/04-full-backup.md](../pgnode/04-full-backup.md), процессы
  restore/verify/retention ([../pgnode/processes/d-restore.md](../pgnode/processes/d-restore.md),
  [../pgnode/processes/c-verify.md](../pgnode/processes/c-verify.md),
  [../backups/processes/c-retention.md](../backups/processes/c-retention.md)).

  **Этапы.** (1) прототип клиента `BASE_BACKUP` → tar-поток в файл,
  побайтовая сверка с `pg_basebackup -Ft`; (2) multipart-адаптер OwnS3 и
  потоковая загрузка; (3) потоковый restore; (4) потоковый verify;
  (5) retention/панель под новый формат; (6) E2E и нагрузочный прогон.

  **Риски.** `BASE_BACKUP` версионно-зависим, и Npgsql может не отдавать
  COPY OUT `BASE_BACKUP` — возможен собственный низкоуровневый слой
  протокола; multipart-объект большого объёма (лимиты OwnS3, таймауты,
  abort); потоковый verify должен точно соответствовать
  `backup_manifest`.

