# Перечисления PgNode

Уровень 4. Таблицы всех значений ограниченных наборов компонента. Ключи —
[../02-api.md](../02-api.md), [../03-wal-receiver.md](../03-wal-receiver.md);
процессы — [A](a-wal-receiver.md)…[F](f-node-agent.md).

## 1. Состояние агента (`/pgworker/nodes/<C>/<X>/<N>/status.state`)

| Значение | Что означает | Выходы |
|---|---|---|
| `not_initialized` | Агент стартовал, листенер `:8081` поднят, доставки секретов ещё не было | `initializing` — txn после проверки аргументов |
| `initializing` | Идёт старт компонентов или ожидание готовности (`SELECT 1` без бюджета) | `ready` — компоненты готовы / параметры подготовлены (tool-путь); `start_failed` — исчерпаны попытки старта/ошибка рендера |
| `ready` | Компоненты готовы; `applied_revision` = `mod_revision` применённого bootstrap-ключа | `initializing` — новый материал/рестарт агента; `start_failed` — ошибка применения |
| `start_failed` | Сбой старта/применения; `error` — диагностика | пересоздание ноды с новым диском (воркер) |

`applied_revision`: `null` в `not_initialized`/`initializing`; в `ready` —
ревизия применённого bootstrap-ключа; в `start_failed` — ревизия, если сбой
произошёл после начала применения.

## 2. Компоненты агента (`status.components.{pg,doorman}`)

Набор ключей фиксирован: только `pg` и `doorman`.

| Значение | Что означает |
|---|---|
| `starting` | Фаза старта/ожидания `SELECT 1` |
| `ready` | Проба `SELECT 1` успешна (в tool-пути не используется) |
| `failed` | Исчерпаны 3 попытки старта |
| `stopped` | Не запускался (рендер-ошибка/применение) |
| `prepared` | Параметры готовы, запуск — за tool-командой (`is_tool_start=true`) |

В `not_initialized` и сразу после txn-перехода в `initializing` — пустой
объект `{}`.

## 3. Состояние приёмника WAL (`agent/<N>.state`)

| Значение | Что означает |
|---|---|
| `starting` | Процесс стартовал, резолв условий |
| `waiting_role` | Нода не primary и не sync-standby — не стримит |
| `waiting_password` | Нет `/clusters/<C>/backup_password` |
| `waiting_policy` | Нет policy/пула/bucket |
| `waiting_slot` | Слот не создан/потерян (воркер ensure/recreate) |
| `waiting_disabled` | Жив `/pgworker/backups-disabled` |
| `streaming` | Стрим идёт |
| `paused` | Стоп-семантика: `STOPPED`/`WAL_CHAIN_ERROR` wal-ключа |
| `failed_permanent` | Несъёмное условие (segment-size mismatch, разрыв LSN, чужая инкарнация); `error` |

## 4. Роль источника (`agent/<N>.role`)

| Значение | Что означает |
|---|---|
| `master` | Primary ноды — write-список `write_master` |
| `sync` | Sync-standby — write-список `write_replica` |
| `none` | Async-реплика/переходное окно — не стримит |

## 5. Фазы операций

| Операция | Фазы | Исходы |
|---|---|---|
| Полный (`/api/backup/full`) | `running/basebackup` → `uploading/uploading` | `succeeded`, `failed` |
| Verify (`/api/ops/verify`) | `downloading` → `verifying` | `succeeded`, `failed`, `cancelled` |
| Restore (`/api/ops/restore`) | `downloading` → `recovering` | `succeeded`, `failed`, `cancelled` |
| Drill (то же API с `id`) | `downloading` → `recovering` | `succeeded`, `failed`, `cancelled` |

`failed` несёт однострочный `error`; `cancelled` — отмена оператором, НЕ
суждение о данных.

## 6. Проба данных ноды (`/api/node/pgdata`)

| Значение | Что означает |
|---|---|
| `Present` | `PG_VERSION` найден — данные на ноде есть |
| `Absent` | `PG_VERSION` нет — данных нет (доказанная утрата) |
| `Unknown` | Транспортный отказ — судить нельзя |

## 7. Режим запуска компонентов (`is_tool_start`)

| Значение | Что означает |
|---|---|
| `false` (умолчание) | Агент стартует компоненты и ждёт готовности |
| `true` | Tool-путь: агент только готовит параметры (tmpfs), компоненты не запускает |
