# PgNode: проверка полного (verify)

Уровень 3. Механика checksums-проверки полного на ноде. Оркестрация verify
(две проверки, политика, статусы) —
[backups/01-full-backup.md](../backups/01-full-backup.md) §7; процесс —
[processes/c-verify.md](processes/c-verify.md); состояния —
[processes/enums.md](processes/enums.md).

## 1. Команда и реестр

`POST /api/ops/verify` от воркера на `alias:8080` ноды (`candidate.Node`;
S3-контекст read-списка — секреты ТОЛЬКО в POST, не в GET-статусах и не в
логах). Verify — ops-операция общего реестра: одна ops-операция на процесс;
полная операция (full) и ops-операция на одной ноде не блокируют друг друга.
Занятый реестр — 409 (воркер откладывает запуск; снимок 409 не различает
свою/чужую операцию).

## 2. Механика

1. Скачивание `full/<id>/` 1:1 .NET S3-клиентом в staging ноды
   (`<Staging:Dir>/full/<id>/`) — read-список пула.
2. `pg_verifybackup` (manifest SHA256, вкл. `pg_wal/` набора).
3. Статус `GET /api/ops/verify` (фазы `downloading|verifying`,
   `started_unix`); воркер супервизит поллингом до итога.
4. Staging чистит приёмник при любом исходе.

Сбой скачивания (S3/сеть/staging) — transient: статус операции `failed` с
фазой `downloading`; воркер держит PENDING и повторяет POST следующим тиком
(операция начнётся заново, резюмов нет). `pg_verifybackup` ≠ 0 (фаза
`verifying`) — permanent: данные плохие, `error` — причина.

## 3. Отмена и vanished

Операторская отмена идущей проверки (`POST /api/ops/cancel` — внешний канал
мимо воркера) — операция завершается снимком `state=cancelled` (НЕ failed):
отмена — не суждение о данных; воркер-супервиз по GET `cancelled` возвращает
кандидата в PENDING (`checked_unix=now` — перепроверка по расписанию
политики `verify.interval_sec`, лив-лок немедленных перезапусков исключён);
journal-факт `cancelled-by-operator`. GET 404 (операции нет — старт не
дошёл/рестарт приёмника) — transient: следующий тик повторяет POST; идущий
процесс приёмника жив под supervisord'ом ноды — «окна бута ephemeral-
контейнера» нет (стартовавшее приложение уже слушает API).
