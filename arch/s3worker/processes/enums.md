# Перечисления S3Worker

Уровень 4. Таблицы всех значений ограниченных наборов, которые использует
воркер. Формат ключей и контракт — [s3-clusters/02-keys.md](../../s3-clusters/02-keys.md);
процессы — [A](a-provisioning.md)…[H](h-credentials-rotator.md).

## 1. Состояние инстанса (`/s3/clusters/<C>/nodes/<I>/state`)

| Значение | Что означает | Кто пишет | Выходы |
|---|---|---|---|
| `NOT_INITIALIZED` | Панель заявила создание/добавление инстанса; воркер ещё не начал | панель (заявка) | `PROVISIONING` — [A](a-provisioning.md)/[D](d-add-node.md) |
| `PROVISIONING` | Воркер создаёт или пересоздаёт контейнер; health-грань ещё не отвечает | воркер | `RUNNING` — по успешной health-пробе [C](c-node-supervisor.md)/[D](d-add-node.md)/[H](h-credentials-rotator.md) |
| `RUNNING` | Контейнер жив, health-грань отвечает по advertised-адресу | воркер | `UNREACHABLE` — молчание дольше `NodeDeadSec`; `TO_REMOVE` — маркер панели, затем `REMOVING` — демонтаж [E](e-remove-node.md) |
| `UNREACHABLE` | Инстанс молчит дольше `NodeDeadSec`; volume сохранён, деструктива нет | воркер | `RUNNING` — возврат пробы; `PROVISIONING` — пересоздание по docker-факту [C](c-node-supervisor.md) |
| `REMOVING` | Идёт демонтаж инстанса (гварды пройдены) | воркер | ключи `nodes/<I>/…` удаляются — [E](e-remove-node.md) |
| `TO_REMOVE` | Маркер панели на вывод инстанса; one-way | панель | `REMOVING` — [E](e-remove-node.md) |

`RUNNING` ставится только по зрячей health-пробе (docker-факт прочитан);
`PROVISIONING`/`TO_REMOVE`/`REMOVING`/`NOT_INITIALIZED` чужих процессов
надзор не пересоздаёт.

## 2. Состояние кластера (`/s3/clusters/<C>/config.state`)

| Значение | Что означает | Процесс |
|---|---|---|
| отсутствует | Active-кластер: декларация выполнена, воркер ведёт надзор и функции Active-ветки | [C](c-node-supervisor.md)… |
| `NOT_INITIALIZED` | Заявка панели на создание кластера | [A](a-provisioning.md) |
| `TO_REMOVE` | Заявка панели на полный демонтаж кластера | [B](b-deprovisioning.md) |
| незнакомое значение | Толерантно трактуется как Active-ветка с raw-строкой `state` (система развивается) | — |

Битый или отсутствующий `config` — тик кластера пропускается с логом
(классификация тика — [08-operability.md](../08-operability.md) §1).

## 3. Журнал фаз (`/s3worker/work/<C>.op`)

| `op` | Процесс |
|---|---|
| `provision` | [A](a-provisioning.md) |
| `deprovision` | [B](b-deprovisioning.md) |
| `supervise` | [C](c-node-supervisor.md) |
| `add-node` | [D](d-add-node.md) |
| `remove-node` | [E](e-remove-node.md) |
| `mesh-governance` | [G](g-mesh-governance.md) |
| `sync-probe` | [F](f-sync-probe.md) |
| `rotate-secrets` | [H](h-credentials-rotator.md) |

Набор `phase` — свой у каждого процесса и описан в его документе: waiting-*
(InProgress — следующий тик), phase-маркеры возобновляемости, `done`,
`rejected`, `aborted-state-changed`, `invalid-secrets`, `recreated-*`,
`rotated-*`. Фазы отказа несут `last_error` (диагностика; значения секретов
в неё не попадают). Waiting-фазы не затирают маркер начатой мутации —
журнал читается процессами как носитель возобновляемости
([H](h-credentials-rotator.md)).
