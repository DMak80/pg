# Бэкапы: восстановление шарда (PITR)

Уровень 3. Функция «восстановление»: заявка оператора, RestoreProcess
(демонтаж/подъём шарда, REJOINING), гвард исключения, отмена и
стоп-семантика. Механика restore-операции на ноде —
[pgnode/06-restore-drill.md](../pgnode/06-restore-drill.md); ключи —
[05-etcd-contract.md](05-etcd-contract.md); состояния —
[processes/enums.md](processes/enums.md).

## 1. Операция

Восстановление ОДНОГО шарда `<X>` кластера `<C>` из `full/<id>` + накат
WAL-цепочки до цели: `latest` (конец цепочки — полная потеря шарда/DR) или
`target_time` (порча данных, откат к моменту до порчи). Запуск — команда
оператора через HTTP API воркера
(`POST /api/clusters/<C>/shards/<X>/restore`,
[pgworker/07-api.md](../pgworker/07-api.md) §1): тело задаёт `backup_id` (нет
— новейший COMPLETED/list-S3; при own-source и живых etcd-статусах текущей
инкарнации кандидат DR — новейший полный S3 БЕЗ etcd-статуса: такие
orphan-полные остаются от инкарнации, чьи статусы стёр deprovision (D2),
и потому DR-источник; свежий полный ПЕРЕСОЗДАННОГО кластера статус имеет и
в DR не участвует — восстановление пустого кластера из собственного свежего
снимка не является восстановлением погибших данных; нет orphan — обычный
новейший COMPLETED), цель, `source` (S3-префикс источника; default —
собственный `<C>/<X>`; DR: префикс погибшего кластера — восстанавливаем в
пересозданный кластер) и `confirm` = имя шарда (перезапись данных осознана,
мисматч → 400). Воркер валидирует и сам пишет заявку-статус в etcd; запуск
restore — команда оператора по runbook ([10-runbook-restore.md](10-runbook-restore.md)), из UI
панель restore не запускает. Одна активная заявка на шард (повтор при
активной → 409). Потеря ОДНОЙ ноды живого шарда restore'ом НЕ чинится
(Patroni rebuild быстрее — runbook отсылает к recreate-эндпоинту).

## 2. Restore-операция PgNode

Операцию исполняет агент PgNode ПЕРВОЙ ноды шарда
(`pgw-<C>-<X>-<n1>`) — тот же единственный процесс ноды, что стримит WAL и
снимает полные; отдельных процессов для операций нет. Ход: демонтаж нод
шарда → воркер поднимает первую ноду штатным EnsureNode на НОВОМ пустом
data-volume (щит `initialize="restore-in-progress"` удерживает Patroni от
bootstrap пустого PGDATA — постгрес Patroni не поднимает, PGDATA ноды
свободен; PgNode ноды не стримит — роли нет, live в `waiting_role`) →
`POST /api/ops/restore` на `http://<нода>:8080` (тело: `backup_id`,
`target_time`? — PG-совместимый формат `yyyy-MM-dd HH:mm:sszzz` (воркер
разбирает RFC3339 заявки и нормализует — parser postgres не принимает
«T»-сепаратор/суффикс «Z»; `target_time` отсутствует = latest),
`recovery_timeout_sec`, S3-контекст) и `GET /api/ops/restore` (фазы
`downloading|recovering`, `restored_to_lsn`?, `system_id`?,
`started_unix`?, `error`?). PGDATA операции — PGDATA САМОЙ ноды
(`/home/postgres/pgdata/pgroot/data`, Spilo-layout, uid:gid `101:101`);
volume ноды в этот момент пуст.

Механика восстановления (скачивание, `restore_command`/fetch-wal,
`postgresql.auto.conf`, `standby.signal`, `pg_ctl`, promote) —
[pgnode/06-restore-drill.md](../pgnode/06-restore-drill.md) §2.

## 3. RestoreProcess (машина тика)

Под клэймом `<C>`: заявки разных шардов прогрессируют независимо (по шагу на
шард за тик, `Task.WhenAll`), ноды внутри шага — параллельно. Автомат
`PLANNED → RUNNING → REJOINING → COMPLETED/FAILED`, демонтаж, параллельный
подъём rejoin-нод, SQL-гейт мастера, гвард исключения (свежая сверка
restore-ключей перед мутациями надзора), отмена оператором —
[F. RestoreProcess](processes/f-restore.md).

## 4. Стоп-семантика

Deprovisioning (D1) сносит ноды шарда вместе с приёмниками (restore-операция
уходит с нодой), D2 чистит префикс `/pgworker/backups/<C>/` (вкл.
restore-ключи); remove-shard с активным restore → заявка FAILED
`cancelled-by-remove`. S3-объекты restore не удаляет никогда (только читает).

## 5. Runbook

[10-runbook-restore.md](10-runbook-restore.md): полная потеря ноды (живой шард → Patroni
recreate; весь шард → restore latest), порча данных (target_time до порчи),
восстановление в новый кластер (DR: пересоздание декларации + restore с
source) — критерий приёмки: кластер уничтожается и поднимается из бэкапов с
проверкой данных (E2E-гейт + ручной прогон dev-стенда).
