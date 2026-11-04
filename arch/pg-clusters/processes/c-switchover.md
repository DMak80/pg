# C. Switchover (плановая смена лидера)

Уровень 4. Автомат плановой передачи лидерства: заявка, проверка
кандидата, догоняние, демоушен, промоушен, возврат старого лидера.
Модель — [01-ha-architecture.md](../01-ha-architecture.md) §5; операции —
[08-operations.md](../08-operations.md) §1.

## 1. Участники и предусловия

- Инициатор — оператор (`patronictl switchover` в контейнере ноды или
  `POST /switchover` с basic-auth per-cluster) либо PgWorker (soft-recreate
  живой ноды — [pgworker/processes/c-node-supervisor.md](../../pgworker/processes/c-node-supervisor.md)).
- В кластере есть лидер и хотя бы одна живая реплика-кандидат.
- Нет открытого окна REST-ротации секретов (`rest_window_open`) — PgWorker
  подавляет switchover до закрытия окна
  ([pgworker/04-security.md](../../pgworker/04-security.md) §6).
- Переезды бакетов не блокируют switchover: подписки ходят по multi-host
  conninfo и переживают смену мастера (P2), но cutover-фаза конкретного
  переезда может быть перенесена на следующий тик.

## 2. Состояния

| Состояние | Что означает | Условие выхода |
|---|---|---|
| `REQUESTED` | заявка принята, выбран кандидат | кандидат найден → `CANDIDATE_CHECK` |
| `CANDIDATE_CHECK` | кандидат жив, это реплика, отставание допустимо | проверка пройдена → `CATCHUP` |
| `CATCHUP` | ждём, пока кандидат догонит лидера (lag 0) | lag 0 → `DEMOTE` |
| `DEMOTE` | лидер прекращает запись, дожидается подтверждений | PG read-only → `PROMOTE` |
| `PROMOTE` | кандидат делает `pg_ctl promote` | writable → `KEY_UPDATE` |
| `KEY_UPDATE` | callback обновляет мастер-ключ; новый лидер публикует `optime/leader` | ключ обновлён → `REJOIN_OLD` |
| `REJOIN_OLD` | старый лидер становится репликой, догоняет/`pg_rewind` | streaming → `DONE` |
| `DONE` | новый лидер стабилен, реплики streaming | — |
| `ABORTED` | заявка не выполнена (кандидат не догнал, ошибка, окно ротации) | повтор заявки |

Переходы: `REQUESTED → CANDIDATE_CHECK → CATCHUP → DEMOTE → PROMOTE →
KEY_UPDATE → REJOIN_OLD → DONE`; из `CANDIDATE_CHECK`/`CATCHUP` — `ABORTED`
(лидер остаётся лидером); из `DEMOTE`/`PROMOTE` отказ — `BLOCKED`
(требуется разбор: лидер мог уже стать read-only).

## 3. Ход процесса

1. Patroni проверяет кандидата: живая реплика, нужная timeline, отставание
   в допустимых пределах.
2. Лидер закрывает запись, делает последнюю контрольную точку, дожидается
   подтверждений; Patroni переводит его в read-only.
3. Кандидат получает `pg_ctl promote` и становится primary.
4. Callback `on_role_change` обновляет мастер-ключ (lease TTL 5 с);
   новый лидер публикует `optime/leader`.
5. Старый лидер переподключается к новому как streaming-реплика
   (при расхождении — `pg_rewind`).
6. Клиенты: новые соединения идут по обновлённому мастер-ключу; открытые
   сессии старого лидера завершаются (ошибка записи → ретрай через роутер).

## 4. Проверка результата

- `GET /primary` у нового лидера = 200, у старого — 503;
- `patronictl list`: один Leader, остальные `streaming`, lag 0;
- мастер-ключ указывает на `host:<doorman-port>` нового лидера;
- `work.last_failover` не фиксируется: graceful switchover — не событие
  failover ([pgworker/processes/c-node-supervisor.md](../../pgworker/processes/c-node-supervisor.md) §5).

## 5. Отказы

- **Кандидат отстал/не догнал** — `ABORTED`: лидер остаётся лидером,
  выбирается другой кандидат или переносится окно.
- **Ошибка на `DEMOTE`/`PROMOTE`** — `BLOCKED`: возможен период без
  записи; разбор по логам Patroni, при необходимости — failover/rebuild
  ([b-failover.md](b-failover.md), [d-node-recovery.md](d-node-recovery.md)).
- **Окно ротации секретов** — switchover подавлен; повтор после `done`
  процесса ротации.
- **Switchover лидера во время переезда** — допустим (подписки
  multi-host); при сомнении — дождаться завершения cutover-тика.
