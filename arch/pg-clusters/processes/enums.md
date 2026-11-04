# Перечисления домена PG-шардов

Уровень 4. Закрытые наборы значений, которые использует домен: роли и
состояния членов Patroni, синхронность репликации, статусы бакета, форматы
мастер-ключа. Перечисления оркестратора и агента не дублируются —
[pgworker/processes/enums.md](../../pgworker/processes/enums.md),
[pgnode/processes/enums.md](../../pgnode/processes/enums.md).

## 1. Роль и состояние члена Patroni (`/service/<scope>/members/<name>`)

Значения — собственные значения Patroni (REST `/cluster`), домен их только
читает.

| `role` | Что означает |
|---|---|
| `leader` | текущий primary (держатель лидерского lease) |
| `replica` | реплика, следующая за лидером |
| `sync_standby` | реплика, выбранная синхронной (`synchronous_standby_names`) |
| `standby_leader` | лидер standby-кластера (каскадная репликация; в каноне не используется) |
| `quorum_standby` | реплика, участвующая в quorum-commit (в каноне не используется) |

| `state` | Что означает |
|---|---|
| `running` | PostgreSQL запущен и принимает роль |
| `streaming` | реплика получает WAL потоком |
| `starting` | процесс стартует/догоняет после старта |
| `stopped` | PostgreSQL остановлен |
| `crashed` | процесс упал |
| `unknown` | состояние не определено (нода недоступна/молчит) |

`leader` + `running` — рабочий primary; `replica` + `streaming` — здоровая
реплика. `unknown`/`stopped`/`crashed` — повод для проб и разбора
([09-troubleshooting.md](../09-troubleshooting.md) §9).

## 2. Синхронность репликации (`pg_stat_replication.sync_state`)

| Значение | Что означает |
|---|---|
| `sync` | синхронная реплика: лидер ждёт её подтверждения |
| `potential` | кандидат в синхронные (резерв при потере sync) |
| `async` | асинхронная реплика; подтверждения не ждём |
| `quorum` | участвует в quorum-commit (канон не использует) |

Пропажа `sync`-реплики при `synchronous_mode_strict=true` останавливает
запись — это штатная защита durability-first
([01-ha-architecture.md](../01-ha-architecture.md) §2).

## 3. Статус бакета (`/clusters/<C>/buckets/status/bucket_<i>`)

| Значение | Что означает | Кто пишет |
|---|---|---|
| *(ключа нет)* `ACTIVE` | бакет принадлежит владельцу из `routing`; обычная жизнь | — |
| `NOT_INITIALIZED` | начальное состояние бакетов создаваемого кластера; снимает provisioning | PgWorker |
| `SYNCING` | идёт перенос (DDL/copy), DDL-мораторий; `phase` — фаза процесса F | PgWorker (MoveProcess) |
| `FROZEN` | источник заморожен (cutover/rollback), запись приложению запрещена | PgWorker |
| `ABORTING` | журнал уборки незавершённого переезда (план + `phase`) | PgWorker |

`phase` (`ddl`, `frozen`, `flip`, `rollback-post-flip`, `blocked`) и
transient-поля (`fail_state`/`last_error`) определяет процесс
[F. MoveProcess](../../pgworker/processes/f-move.md); заявки
`/pgworker/moves/<C>/bucket_<i>.op` — `move|rollback|finalize|abort`
([pgworker/processes/enums.md](../../pgworker/processes/enums.md) §6–§7).

Переходы статуса: `ACTIVE → SYNCING → FROZEN → ACTIVE` (flip), с ветками
`FROZEN → SYNCING` (разморозка/ретрай) и `→ ABORTING → ACTIVE` (уборка);
`NOT_INITIALIZED → ACTIVE` (provisioning).

## 4. Формат мастер-ключа (`/clusters/<C>/shards/<X>/master`)

| Формат | Кто пишет | Что означает |
|---|---|---|
| `host:<doorman-port>` | Patroni-callback / MasterKeyReconciler | каноническая нода: адрес doorman мастер-ноды (дискриминант по порту — порт doorman уникален на ноду) |
| `node:<pg-port>` | внешний HA-контур (усыновлённая нода) | нода внешнего происхождения; резолв мастера понимает оба формата |

Вырожденные значения (`host:0`, пустая строка) не матчатся ни на одну ноду
и предикатом роли не являются: «кто мастер» — только проба Patroni REST
`/primary` ([07-identify-master.md](../07-identify-master.md) §1).

## 5. Ссылки на чужие перечисления

| Набор | Где |
|---|---|
| Состояния ноды (`NOT_INITIALIZED`, `PROVISIONING`, `RUNNING`, `REBUILDING`, `UNREACHABLE`, `QUARANTINED`, `REMOVING`, `TO_RECREATE`) | [pgworker/processes/enums.md](../../pgworker/processes/enums.md) §2 |
| Маркер пересоздания (`soft`/`hard`) | [pgworker/processes/enums.md](../../pgworker/processes/enums.md) §3 |
| Состояние шарда (`TO_REMOVE`) | [pgworker/processes/enums.md](../../pgworker/processes/enums.md) §4 |
| Состояния агента PgNode и компонент ноды | [pgnode/processes/enums.md](../../pgnode/processes/enums.md) §1–§2 |
| Заявки и фазы переезда | [pgworker/processes/enums.md](../../pgworker/processes/enums.md) §6–§7 |
