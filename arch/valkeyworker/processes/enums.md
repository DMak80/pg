# Перечисления ValkeyWorker

Уровень 4. Таблицы всех значений ограниченных наборов, которые использует
воркер. Формат ключей и контракт — [valkey-clusters/02-keys.md](../../valkey-clusters/02-keys.md);
процессы — [A](a-provisioning.md)…[K](k-ca-rotator.md).

## 1. Состояние ноды (`/valkey/clusters/<C>/nodes/node<k>/state`)

| Значение | Что означает | Кто пишет | Выходы |
|---|---|---|---|
| `NOT_INITIALIZED` | Панель заявила создание ноды; воркер ещё не начал | панель (заявка) | `PROVISIONING` — [A](a-provisioning.md) |
| `PROVISIONING` | Воркер создаёт или пересоздаёт контейнер; PING ещё не отвечает | воркер | `RUNNING` — по PING через TLS; `UNREACHABLE` — при молчании |
| `RUNNING` | Контейнер жив, PING с admin-кредом по TLS отвечает | воркер | `UNREACHABLE` — молчание дольше `NodeDeadSec`; `TO_REMOVE` — маркер панели, затем `REMOVING` |
| `UNREACHABLE` | Нода молчит дольше `NodeDeadSec` | воркер | `PROVISIONING` — пересоздание [C](c-node-supervisor.md); `RUNNING` — возврат |
| `REMOVING` | Идёт демонтаж ноды | воркер | ключ `nodes/node<k>/` удаляется — [B](b-deprovisioning.md) |
| `TO_REMOVE` | Маркер панели на снятие ноды; one-way | панель | `REMOVING` — демонтаж |

## 2. Состояние кластера (`/valkey/clusters/<C>/config.state`)

| Значение | Что означает | Процесс |
|---|---|---|
| отсутствует | Active-кластер: декларация выполнена, воркер ведёт надзор и функции Active-ветки | [C](c-node-supervisor.md)… |
| `NOT_INITIALIZED` | Заявка панели на создание кластера | [A](a-provisioning.md) |
| `TO_REMOVE` | Заявка панели на полный демонтаж кластера | [B](b-deprovisioning.md) |
| незнакомое значение | Толерантно трактуется как Active-ветка с raw-строкой `state` (система развивается) | — |

## 3. ACL-пользователи ноды

| Пользователь | Значение |
|---|---|
| `admin` | `on`, `~* +@all`; воркер (CONFIG/ACL/пробы) и панель (read-only пробы) |
| `app` | `on`, `~* +@read +@write +ping +echo`; приложения |
| `default` | `off`; анонимный вход запрещён |

## 4. Исход заявки (`/valkeyworker/ticket_outcomes/<C>`)

| Поле | Значения |
|---|---|
| `kind` | `password-app` — ротация app-пароля ([E](e-password-rotator.md)); `password-admin` — ротация admin-пароля ([E](e-password-rotator.md)); `ca` — ротация CA/сертов ([K](k-ca-rotator.md)) |
| `outcome` | `expired` — не-начатая заявка снята по возрасту ([06-operability.md](../06-operability.md) §2); `done` — успешный финал операции |
| `reason` | Необязательная причина; примеры: `waiting-cluster` — заявка ждала условий на момент экспирации |
| `requested_unix`, `requested_by`, `finished_unix` | Момент постановки/автор заявки и момент исхода |

Ключ перезаписывается каждым новым исходом; чистится демонтажом
([B](b-deprovisioning.md)); панель читает его для алерта
`valkey-ticket-expired`.

## 5. Журнал фаз (`/valkeyworker/work/<C>.op`)

| `op` | Процесс |
|---|---|
| `provision` | [A](a-provisioning.md) |
| `deprovision` | [B](b-deprovisioning.md) |
| `migrate-tls` | [T](t-tls-migrator.md) |
| `rotate` | [E](e-password-rotator.md) |
| `rotate-ca` | [K](k-ca-rotator.md) |

Набор `phase` — свой у каждого процесса и описан в его документе
(waiting-*, phase-*, done, expired, aborted-state-changed). Стейт доигрывания
ротации кредов живёт отдельным ключом `work/<C>/rotation` (фазы `e1-pending`/
`e1-added`/`e2-committed`), потому что сам `work/<C>` надзор перезаписывает
каждый тик ([E](e-password-rotator.md)).
