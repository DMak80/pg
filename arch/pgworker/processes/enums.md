# Перечисления PgWorker

Уровень 4. Таблицы всех значений ограниченных наборов, которые использует
воркер. Формат ключей и контракт — [../00-etcd-contract.md](../00-etcd-contract.md);
процессы, меняющие состояния, — [A](a-provisioning.md)…[K](k-move-repair.md).

## 1. Состояние кластера (`/clusters/<C>/config.state`)

| Значение | Что означает | Процесс |
|---|---|---|
| отсутствует | Active-кластер: декларация выполнена, воркер ведёт надзор и функции Active-ветки | [C](c-node-supervisor.md)… |
| `NOT_INITIALIZED` | Заявка панели на создание кластера | [A](a-provisioning.md) |
| `TO_REMOVE` | Заявка панели на полный демонтаж кластера | [B](b-deprovisioning.md) |
| незнакомое значение | Толерантно трактуется как Active-ветка с raw-строкой `state` (система развивается) | — |

## 2. Состояние ноды (`/clusters/<C>/shards/<X>/nodes/<n>/state`)

Пишет PgWorker (кроме `NOT_INITIALIZED` — панель, и `TO_RECREATE` — маркер
оператора); панель отображает как строку.

| Значение | Что означает | Кто пишет | Выходы |
|---|---|---|---|
| `NOT_INITIALIZED` | Панель заявила ноду; воркер ещё не начал | панель (заявка) | `PROVISIONING` — [A](a-provisioning.md)/[G](g-add-shard.md) |
| `PROVISIONING` | Контейнер/сервис создаётся, ждём Patroni | воркер | `RUNNING` — по факту HA-контура [A](a-provisioning.md) P2.2/[G](g-add-shard.md) A4 |
| `RUNNING` | Patroni-нода в кластере (role master/replica из `/service/<scope>/members`) | воркер | `UNREACHABLE` — молчание дольше `NodeDeadSec`; `REBUILDING` — rebuild; `REMOVING` — демонтаж; `QUARANTINED` — эвакуация |
| `REBUILDING` | Пересоздание с чистого места (rebuild) | воркер | `RUNNING` — возврат в кластер [C](c-node-supervisor.md) |
| `UNREACHABLE` | Периодически недоступна (Patroni REST молчит), ждём/разбираемся | воркер | `REBUILDING` — смерть подтверждена; `RUNNING` — возврат; `QUARANTINED` — эвакуация |
| `QUARANTINED` | Шард эвакуирован; нода при возврате не допускается к работе | воркер (эвакуатор) | только ручной runbook; демонтаж — [H](h-remove-shard.md) |
| `REMOVING` | В процессе deprovisioning | воркер | ключ удаляется в конце [B](b-deprovisioning.md)/[H](h-remove-shard.md) |
| `TO_RECREATE` | Маркер оператора: пересоздать ноду (soft — graceful switchover реплики, hard — снос/пересоздание) | оператор через API ([07-api.md](../07-api.md) §1) | исполняет [C. NodeSupervisor](c-node-supervisor.md); парный ключ `nodes/<n>/recreate` — §3 |

`RUNNING` ставится только по факту HA-контура: `/service/<C>-<X>/initialize` +
leader + Patroni REST отвечает И идентифицирует ноду (`GET /patroni` несёт
`scope`/`name`). Ноды `QUARANTINED`/`REMOVING` надзор не пробирует и state не
трогает.

## 3. Маркер пересоздания (`/clusters/<C>/shards/<X>/nodes/<n>/recreate`)

| Значение | Что означает |
|---|---|
| `soft` | Пересоздание живой ноды через graceful switchover (лидер сначала уступает лидерство, затем пересоздаётся репликой) |
| `hard` | Снос/пересоздание ноды без graceful-пути |

Ключ парный к `state=TO_RECREATE`; исполнение — [C](c-node-supervisor.md)
(RecreateMarked); в открытом окне ротации — гварды
[I](i-secret-rotator.md).

## 4. Состояние шарда (`/clusters/<C>/shards/<X>/state`)

| Значение | Что означает | Кто пишет |
|---|---|---|
| отсутствует | Обычный шард | — |
| `TO_REMOVE` | Маркер демонтажа шарда (guards G1–G7) | панель (через API воркера) |

## 5. Состояние агента PgNode

Состояния агента (`not_initialized`, `initializing`, `ready`,
`start_failed`), компоненты (`starting`, `ready`, `failed`, `stopped`,
`prepared`) и `applied_revision` — перечисление компонента:
[pgnode/processes/enums.md](../../pgnode/processes/enums.md) §1–§2; процесс —
[F. NodeAgent](../../pgnode/processes/f-node-agent.md).

В `not_initialized` и сразу после txn-перехода в `initializing` —
пустой объект `{}`.

## 6. Состояние бакета (`/clusters/<C>/buckets/status/bucket_<i>`)

Формат, семантика и таблица всех значений — перечисление домена
[pg-clusters/processes/enums.md](../../pg-clusters/processes/enums.md) §3;
отсутствие ключа = `ACTIVE`. Фазы `phase` определяет процесс
[F](f-move.md): `ddl`, `frozen`, `flip`, `rollback-post-flip`, `blocked`;
transient-отказ фиксируется в `fail_state`/`last_error` статус-ключа,
заявка при этом жива.

## 7. Заявка переезда (`/pgworker/moves/<C>/bucket_<i>.op`)

| Значение | Что означает | Процесс |
|---|---|---|
| `move` | Плановый переезд бакета на целевой шард | [F](f-move.md) |
| `rollback` | Откат ранее завершённого переезда по обратной подписке | [F](f-move.md) |
| `finalize` | Уборка старого шарда после переезда | [F](f-move.md) |
| `abort` | Отмена/уборка незавершённого переезда (в т.ч. `force`) | [F](f-move.md) |

## 8. Журнал работы (`/pgworker/work/<C>.op`)

| `op` | Процесс |
|---|---|
| `provision` | [A](a-provisioning.md) |
| `deprovision` | [B](b-deprovisioning.md) |
| `evacuate` | [D](d-bucket-evacuator.md) |
| `rebuild` | [C](c-node-supervisor.md) |
| `rotate` | [I](i-secret-rotator.md) (журнальная фаза `rotate-app-password` — имя op сохранено для совместимости) |
| `adopt` | [J](j-adoption.md) |
| `repair` | [K](k-move-repair.md) |

Набор `phase` — свой у каждого процесса и описан в его документе
(waiting-*, phase-*, done, lease-lost/claim-lost).

## 9. HA-факты журнала (`work.last_failover`, `work.last_rebuild`)

| Поле | Значения |
|---|---|
| `last_failover.cause` | `accelerated` — ускорение failover мёртвого лидера (docker-объект отсутствует/не running); `elections` — промоушен ждал выборов Patroni |
| `last_rebuild.cause` | `auto-dead` — rebuild-ветка надзора; `operator-recreate` — исполнение маркера `TO_RECREATE` |

## 10. Эвакуация (`/pgworker/evacuations/<C>/<X>.state`)

| Значение | Что означает |
|---|---|
| `DONE` | Эвакуация завершена: бакеты переведены, ноды `QUARANTINED` |
| `QUARANTINED` | Шард вернулся после эвакуации; ноды остановлены и удерживаются в карантине до ручного разбора |

## 11. Режимы и перечисления конфигурации

| Набор | Значения |
|---|---|
| `PgWorker:Docker:Mode` | `Plain` (docker на выделенных хостах), `Swarm` (docker swarm) |
| `PgWorker:Pgtune:DbType` | `web`, `oltp`, `dw`, `mixed`; `desktop` ЗАПРЕЩЁН (wal_level=minimal несовместим с P3) — fail-fast валидации старта |
| `PgWorker:Moves` (пороговые имена) | `PollIntervalSec`, `FreezeWaitSec`, `FreezeLockTimeoutSec`, `FreezeLockTries`, `AbortMinAgeSec`, `FailoverSlots`, `AdvertisedPublisherHost`, `RepairStaleSec`, `RepairFrozenSec` |
| `PgWorker:Thresholds` | `NodeDeadSec`, `ShardDeadSec`, `PatroniBootSec`, `CutoverTimeoutSec`, `ConnFailBudgetSec`, `ProvisionRetryBaseSec`, `ProvisionRetryMaxSec` |
