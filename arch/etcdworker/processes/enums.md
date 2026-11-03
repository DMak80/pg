# Перечисления EtcdWorker

Уровень 4. Таблицы всех значений ограниченных наборов, которые использует
воркер. Формат ключей и контракт — [00-etcd-contract.md](../00-etcd-contract.md);
процессы — [A](a-membership.md)…[C](c-snapshot-export.md).

## 1. Состояние managed-узла (`/etcd/nodes/<k>/state`)

| Значение | Что означает | Кто пишет | Выходы |
|---|---|---|---|
| `NOT_INITIALIZED` | Лист узла создан до member-add; узел ещё не заявлен членом | воркер (подготовка add) | `PROVISIONING` — [A](a-membership.md) |
| `PROVISIONING` | Член добавлен (learner), контейнер создаётся/пересоздаётся; promote ещё не прошёл | воркер | `RUNNING` — promote + подтверждённый подъём [A](a-membership.md)/[B](b-node-supervisor.md) |
| `RUNNING` | Член voting, health-проба клиентской грани отвечает | воркер | `UNREACHABLE` — молчание дольше `NodeDeadSec` или член unstarted [B](b-node-supervisor.md); вывод — [A](a-membership.md) |
| `UNREACHABLE` | Узел молчит/unstarted дольше `NodeDeadSec`; деструктива нет | воркер | `RUNNING` — возврат пробы [B](b-node-supervisor.md); вывод при авто-замене — [A](a-membership.md) |
| `REMOVING` | Идёт вывод узла (перебор/замена) | воркер | листы `nodes/<k>/*` удаляются — [A](a-membership.md) |

Состояния `TO_REMOVE` у домена НЕТ: вывод инициируется только декларацией
(перебор) или авто-заменой, маркера панели на узле не существует.
`UNREACHABLE` не влечёт пересоздания само по себе — материал авто-замены
собирается комбинацией свидетельств надзора и порога `NodeDeadSec`
([03-node-supervision.md](../03-node-supervision.md) §1).

## 2. Декларация состава (`/etcd/config`)

| Значение | Что означает | Процесс |
|---|---|---|
| отсутствует / битая | N=0: только наблюдение и слепки (external-контур, стенд) | [B](b-node-supervisor.md), [C](c-snapshot-export.md) |
| `nodes`: 0 | managed-узлов нет; состав контура — только external | [B](b-node-supervisor.md) |
| `nodes`: 1..7 | доводка managed-состава до N; итог external + N ≥ 3 (гвард приёма, [05-api.md](../05-api.md) §2) | [A](a-membership.md) |

## 3. Классификация членов контура

| Класс | Признак | Поведение воркера |
|---|---|---|
| `managed` | peer-URL совпадает с паттерном `http://etcdw-node-<k>:2380` | создаётся, пересоздаётся, выводится, заменяется — [A](a-membership.md)/[B](b-node-supervisor.md) |
| `external` | любая другая форма peer-URL (seed, стендовые `etcdN`) | только наблюдается; в доводке не участвует; панель показывает отдельно |

## 4. Журнал фаз (`/etcdworker/work`, поле `op`)

| `op` | Процесс |
|---|---|
| `membership` | [A](a-membership.md) (все структурные фазы) и [B](b-node-supervisor.md) (фаза `orphan-cleanup`) |

Набор `phase`: `add-learner` (секция portalloc + member add), `promoting`
(promote learner), `removing` (вывод перебора), `replacing` (авто-замена),
`cleanup-orphans` (чистка осиротевших ключей/portalloc), `orphan-cleanup`
(снос осиротевших docker-объектов), `done`. Фаза — маркер возобновляемости:
takeover продолжает её по фактам (member в списке? learner voting? контейнер
жив?), а не по журналу вслепую. Фазы отказа несут `last_error`.

## 5. Статус DR-выгрузки (`/etcdworker/snapshots`, поле `state`)

| Значение | Что означает |
|---|---|
| `OK` | последняя выгрузка успешна; `last_uploaded_unix` — метка покрываемого слепка |
| `FAILED` | выгрузка не удалась; `error` — причина (`etcd-backup-missing` — цель не резолвится: кластера с признаком нет, он в `TO_REMOVE` или у него нечитаемые secrets/endpoints); поля последнего успеха сохраняются |

Поля формата и семантика покрытия — [00-etcd-contract.md](../00-etcd-contract.md) §3;
поле `enabled` не пишется (выгрузка безусловна).
