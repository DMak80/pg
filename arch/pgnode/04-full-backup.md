# PgNode: исполнение полного бэкапа

Уровень 3. Механика операции полного на ноде: staging, `pg_basebackup`,
upload, WAL набора, статус, отмена, ENOSPC. Политика и планировщик —
[backups/01-full-backup.md](../backups/01-full-backup.md); процесс —
[processes/b-full-backup.md](processes/b-full-backup.md); состояния —
[processes/enums.md](processes/enums.md).

## 1. Запуск

PgWorker подаёт `POST /api/backup/full` `{"id"}` на `alias:8080` ноды;
команда несёт ТОЛЬКО id — секреты (пароль `backup_exec`, креды пула S3)
приёмник резолвит сам из etcd, как для стрима. 503 стоп-гейтов —
`STOPPED`, `/pgworker/backups-disabled`, `Backups:Enabled=false`.
`WAL_CHAIN_ERROR` — НЕ гейт операции полного: POST отвечает 202, операция
начинается (разрыв — в архиве S3, не в данных нод; его лечение — пересъём).

`pg_basebackup` исполняется ЛОКАЛЬНО в контейнере ноды — подключение
`127.0.0.1:5432` под ролью `backup_exec` (пароль — env `PGPASSWORD`
child-процесса, в argv НЕ передаётся); серверные бинари PG (нужны
restore/verify-операциям приёмника) несёт образ `pgworker-node`. Тем же
процессом исполняются node-операции `/api/node/*`
([07-node-ops.md](07-node-ops.md)).

Статус операции — IMMUTABLE-снимок в памяти приёмника (фазы
`basebackup|uploading`, `wal_start_segment`, `size_bytes`, однострочный
`error`); `wal_start_segment` — из `backup_label`, `size_bytes` — сумма
файлов набора. Staging — каталог `<Staging:Dir>/<id>/` в data-volume ноды
(дефолт `/home/postgres/pgdata/backup-staging`; ENOSPC — реактивный FAILED;
приёмник чистит staging после исхода и при старте новой операции).

## 2. Пайплайн

Фоновая Task процесса приёмника, параллельно стриму; резюмов нет — FAILED →
переснятие новым id:

1. чистка staging;
2. `pg_basebackup -D <staging>/<id>/full` (подключение `127.0.0.1:5432`,
   `-X stream --checkpoint=fast --manifest-checksums=SHA256`);
3. `wal_start_segment` из `backup_label`;
4. загрузка в S3 .NET-клиентом файлами 1:1 (включая `backup_manifest` и
   `pg_wal/`).

**Загрузка в пул S3**: write-список endpoints приёмник резолвит по СВОЕЙ
фактической роли (REST-проба Patroni loopback: реплика → `write_replica`,
мастер → `write_master`); перебор списка — per-попыточный таймаут,
circuit-breaker 45 с ([backups/06-s3-layout.md](../backups/06-s3-layout.md) §3);
«быстрый отказ/успех, не молчание»; переливка идемпотентна — имена объектов
детерминированы; исчерпание списка → FAILED с маркером `s3-pool-exhausted`
в `error` (алерт панели, обычный бэкофф переснятия). Фазы операции приёмник
отдаёт GET-статусом (`running/basebackup` → `uploading/uploading` →
`succeeded`/`failed` + однострочный `error`); воркер поллит GET тиками,
отражает фазы в etcd-статусе и по итогу фиксирует исход — etcd пишет ТОЛЬКО
воркер.

**WAL набора `-X stream`**: файлы бэкапа грузятся строго 1:1 в `full/<id>/`
ВКЛЮЧАЯ `pg_wal/` (база `pg_verifybackup`, manifest перечисляет и WAL-файлы);
ЗАТЕМ закрытые сегменты этого набора дублируются в общий WAL-префикс `wal/`
шарда ПОД НОРМАЛИЗОВАННЫМ (СТРОЧНЫМ) именем (pg_basebackup кладёт сегменты
набора ВЕРХНИМ hex — имя дубля нормализуется при заливке). Закрытость — по
источнику на момент заливки: сегмент набора, имя которого СТРОГО ниже
текущего активного wal-файла источника. Последний сегмент набора (несёт
end-of-backup запись, но в кластере ещё ЖИВ — PG допишет в него же до
ближайшего switch) в `wal/` НЕ дублируется: его байты остаются в
`full/<id>/pg_wal/` (самодостаточность полного — recovery дочитывает из
layout), финальную версию сегмента в `wal/` доставляет стрим при закрытии.
Дубль чужой инкарнации не перезаписывает цепочку — гвард
[pgnode/03-wal-receiver.md](03-wal-receiver.md) §2, полный завершается FAILED
с причиной.

## 3. Отмена и зависание

Отмена (cancel API) — приёмник гасит дерево текущего child-процесса,
операция завершается `failed` с причиной `cancelled`, staging чистится;
гейт-команда на СТАРТЕ (503) идущую операцию не рвёт — отменяют только
cancel и бюджет воркера. Рестарт приёмника/ноды посреди операции → GET 404 →
воркер FAILED `operation-vanished` → переснятие новым id (резюмов нет).
Отдельного контейнера полного нет — cleanup только cancel API (воркер чужой
процесс убить не может); etcd пишет ТОЛЬКО воркер, приёмник etcd-контракта
полного не знает.

ENOSPC на staging — реактивный FAILED с диагностикой; `Staging:QuotaBytes`
упразднён.
