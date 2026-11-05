# Перечисления домена бэкапов

Уровень 4. Таблицы всех значений ограниченных наборов домена. Форматы
ключей — [../05-etcd-contract.md](../05-etcd-contract.md); процессы —
[A](a-full-scheduler.md)…[G](g-drill.md).

## 1. Состояние полного (`full/<id>.state`)

| Значение | Что означает | Кто пишет | Выходы |
|---|---|---|---|
| `PLANNED` | Запланирован, API-запуск на приёмнике ещё не подтверждён | воркер | `RUNNING` — команда принята; `FAILED` — отказ/vanished |
| `RUNNING` | Идёт съём полного на приёмнике (BASE_BACKUP-поток + multipart-заливка) | воркер (по GET) | `UPLOADING` — tar-объект Complete; `FAILED` |
| `UPLOADING` | Дубль WAL набора в `wal/`, фиксация итога | воркер (по GET) | `COMPLETED` — итог успешен; `FAILED` |
| `COMPLETED` | Полный снят и загружен | воркер | `DELETING` — ретенция; verify-статус отдельно |
| `FAILED` | Операция не удалась; `error` — причина | воркер | переснятие новым id (бэкофф) |
| `DELETING` | Транзитная фаза ретенции | воркер | ключ удаляется после delete объекта |

## 2. Verify полного (`full/<id>.verify.state`)

| Значение | Что означает |
|---|---|
| `PENDING` | Проверка не завершена (в т.ч. «идёт») |
| `OK` | Потоковая сверка tar-объекта с `backup_manifest` зелёная; `checked_unix` |
| `FAILED` | Вердикт о данных: расхождение с манифестом (файл/хэш/перечень), манифест в объекте не найден/нечитаем (в т.ч. исчерпание попыток хвоста), несовпадение `Manifest-Checksum`/`WAL-Ranges`/сквозного sha256; `error` — причина. Транспортный сбой сюда НЕ входит (transient — PENDING, повтор тиком) |

## 3. Состояние WAL-потока (`wal.state`)

| Значение | Что означает | Выходы |
|---|---|---|
| `ACTIVE` | Цепочка непрерывна, поток идёт | `DEGRADED`/`WAL_CHAIN_ERROR` |
| `DEGRADED` | Transient-деградация (lag/тишина; маркер `s3-pool-exhausted` — исчерпание пула) | `ACTIVE` — восстановление; `WAL_CHAIN_ERROR` — дыра доказана |
| `STOPPED` | Воркер сознательно остановил поток (эвакуация/карантин) | поток стоит до снятия |
| `WAL_CHAIN_ERROR` | Разрыв цепочки в S3; приёмники замирают; лечение — новый полный | `ACTIVE` — после пересъёма и непрерывности |

## 4. Приёмник WAL (`agent/<N>.state`)

| Значение | Что означает |
|---|---|
| `starting` | Процесс стартовал, резолв условий |
| `waiting_role` | Нода не мастер и не sync-standby — не стримит |
| `waiting_password` | Нет `backup_password` (ensure не прошёл) |
| `waiting_policy` | Нет policy/пула (в т.ч. пустой bucket) |
| `waiting_slot` | Слот ещё не создан |
| `waiting_disabled` | Стоп-ключ `/pgworker/backups-disabled` |
| `streaming` | Стрим идёт |
| `paused` | Стоп-семантика: `STOPPED`/`WAL_CHAIN_ERROR` wal-ключа |
| `failed_permanent` | Несъёмное условие (segment-size mismatch, разрыв LSN, чужая инкарнация); `error` |

## 5. Роль источника (`agent/<N>.role`)

| Значение | Что означает |
|---|---|
| `master` | Primary ноды — write-список `write_master` |
| `sync` | Sync-standby — write-список `write_replica` |
| `none` | Async-реплика/переходное окно — не стримит |

## 6. Restore (`restore/<id>.state`)

| Значение | Что означает |
|---|---|
| `PLANNED` | Заявка создана, валидация не завершена |
| `RUNNING` | Демонтаж/подъём/восстановление |
| `REJOINING` | Восстановленная первичка возвращается в HA-контур |
| `COMPLETED` | Все ноды RUNNING, SQL-гейт пройден |
| `FAILED` | Permanent-отказ; `error` |

Фазы `restore/<id>.phase`: `demolished`, `launching` (тик запуска);
`downloading`, `recovering` (операция приёмника).

## 7. Drill (`drill.state`)

| Значение | Что означает |
|---|---|
| `RUNNING` | Дрилл идёт |
| `SUCCEEDED` | Восстановление до latest и выход из recovery доказаны |
| `FAILED` | Валидация/операция/бюджет; `error` |

Фазы `drill.phase`: `downloading`, `recovering`.

## 8. Сироты (`orphans[].state`)

| Значение | Что означает |
|---|---|
| `OBSERVED` | Префикс без живого владельца наблюдается |
| `DELETING` | Идёт доводка удаления (batch-delete) |
| `absent` | Запись отсутствует — владелец жив или префикс исчез |

## 9. Storage (`storage.state`)

| Значение | Что означает |
|---|---|
| `OK` | Квота не задана или занятость ниже `WarnPercent` |
| `WARN` | Занятость ≥ `WarnPercent` |
| `CRIT` | Занятость ≥ `CritPercent` |

## 10. Журналы

| `op` | Процесс |
|---|---|
| `backup-full` | [A](a-full-scheduler.md) |
| `backup-wal` | [B](b-wal-control.md) |
| `backup-retention` | [C](c-retention.md) |
| `backup-orphan-sweeper` | [E](e-supervisor.md) (сироты) |
| `backup-restore` | [F](f-restore.md) |
| `backup-drill` | [G](g-drill.md) |

Набор `phase` — свой у каждого процесса; waiting-фазы (`waiting-backup-pool`,
`waiting-snapshot`, `waiting-portalloc-lock`) пишутся при живом ожидании,
мутационные — маркер возобновляемости.
