# Перечисления KafkaWorker

Уровень 4. Таблицы всех значений ограниченных наборов, которые использует
воркер. Формат ключей и контракт — [kafka-clusters/README.md](../../kafka-clusters/README.md);
процессы, меняющие состояния, — [A](a-provisioning.md)…[M](m-security-migrator.md).

## 1. Состояние брокера (`/kafka/clusters/<C>/brokers/broker<k>/state`)

| Значение | Что означает | Кто пишет | Выходы |
|---|---|---|---|
| `NOT_INITIALIZED` | Панель заявила создание/добавление брокера; воркер ещё не начал | панель (заявка) | `PROVISIONING` — [A](a-provisioning.md)/[F](f-add-broker.md) |
| `PROVISIONING` | Воркер создаёт или пересоздаёт контейнер; брокер ещё не в кластере | воркер | `RUNNING` — по трём фактам надзора [C](c-node-supervisor.md) |
| `RUNNING` | Контейнер жив, зрячая проба видит брокера, advertised-адрес уже в `endpoints` | воркер | `UNREACHABLE` — молчание дольше `NodeDeadSec`; `TO_REMOVE` — маркер панели, затем `REMOVING` — демонтаж [G](g-remove-broker.md) |
| `UNREACHABLE` | Брокер молчит дольше `NodeDeadSec`; том сохранён, контейнер пересоздаётся | воркер | `PROVISIONING` — пересоздание [C](c-node-supervisor.md); `RUNNING` — возврат в кластер |
| `REMOVING` | Идёт демонтаж брокера (guards пройдены) | воркер | ключ `brokers/<b>/` удаляется — [G](g-remove-broker.md) |
| `TO_REMOVE` | Маркер панели на снятие брокера; one-way | панель | `REMOVING` — [G](g-remove-broker.md) |

`RUNNING` ставится только по совокупности трёх фактов (контейнер, проба,
`endpoints`) — см. [C](c-node-supervisor.md). Ноды `TO_REMOVE`/`REMOVING`/
`PROVISIONING` чужих процессов надзор не трогает.

## 2. Состояние кластера (`/kafka/clusters/<C>/config.state`)

| Значение | Что означает | Процесс |
|---|---|---|
| отсутствует | Active-кластер: декларация выполнена, воркер ведёт надзор и функции Active-ветки | [C](c-node-supervisor.md)… |
| `NOT_INITIALIZED` | Заявка панели на создание кластера | [A](a-provisioning.md) |
| `TO_REMOVE` | Заявка панели на полный демонтаж кластера | [B](b-deprovisioning.md) |
| незнакомое значение | Толерантно трактуется как Active-ветка с raw-строкой `state` (система развивается) | — |

## 3. Роль брокера (`brokers/broker<k>/role`)

| Значение | Что означает |
|---|---|
| `controller` | Combined-нода `broker,controller`: участник KRaft-кворума, `PROCESS_ROLES=broker,controller`; фиксируется при создании навсегда, не демонтируется |
| `broker` | Broker-only нода: `PROCESS_ROLES=broker`; так создаются все добавляемые позже брокеры (кворум не меняется) |

## 4. Исход заявки (`/kafkaworker/ticket_outcomes/<C>`)

| Поле | Значения |
|---|---|
| `kind` | `password-app` — ротация app-пароля ([H](h-password-rotator.md)); `password-admin` — ротация admin-пароля ([H](h-password-rotator.md)); `ca` — ротация CA/сертов ([K](k-ca-rotator.md)); `rebalance` — ребалансировка ([I](i-partition-reassigner.md)) |
| `outcome` | `expired` — не-начатая заявка снята по возрасту ([08-operability.md](../08-operability.md) §3); `done` — успешный финал операции |
| `reason` | Необязательная причина; примеры: `waiting-cluster`, `waiting-drain` — заявка ждала условий на момент экспирации |
| `requested_unix`, `requested_by`, `finished_unix` | Момент постановки/автор заявки и момент исхода |

Ключ перезаписывается каждым новым исходом любого вида; чистится демонтажом
кластера ([B](b-deprovisioning.md)); панель читает его для алерта
`kafka-ticket-expired`.

## 5. Режим reassignment (`/kafkaworker/reassignments/<C>.mode`)

| Значение | Что означает |
|---|---|
| `drain` | Опустошение брокера `TO_REMOVE` (разблокирует [G](g-remove-broker.md)); `drain_broker` — имя брокера |
| `balance` | Ребалансировка размещения по заявке панели ([I](i-partition-reassigner.md)) |

## 6. Журнал фаз (`/kafkaworker/work/<C>.op`)

| `op` | Процесс |
|---|---|
| `provision` | [A](a-provisioning.md) |
| `deprovision` | [B](b-deprovisioning.md) |
| `migrate-security` | [M](m-security-migrator.md) |
| `rotate` | [H](h-password-rotator.md) |
| `rotate-ca` | [K](k-ca-rotator.md) |

Набор `phase` — свой у каждого процесса и описан в его документе (waiting-*,
phase-*, done, expired). Waiting-фазы пишутся только при живой заявке;
мутационные фазы — маркер возобновляемости, их затирание waiting-фазой
запрещено (см. [H](h-password-rotator.md)).
