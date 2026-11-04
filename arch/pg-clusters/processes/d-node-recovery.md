# D. Возврат и rebuild ноды

Уровень 4. Автомат восстановления ноды после отказа: классификация
состояния, догоняние, `pg_rewind`, rebuild с чистого места, специальные
случаи лидера и одноузлового шарда. Модель —
[01-ha-architecture.md](../01-ha-architecture.md) §6; операции —
[08-operations.md](../08-operations.md) §3–§4; оркестрация —
[pgworker/processes/c-node-supervisor.md](../../pgworker/processes/c-node-supervisor.md).

## 1. Участники

- PgWorker (надзор): пробы Patroni REST, docker-инспекция, решение о
  rebuild, пересоздание контейнера/тома, portalloc-закрепление;
- Patroni ноды: выбор способа восстановления по состоянию PGDATA;
- лидер шарда: источник `pg_basebackup`/WAL;
- агент PgNode: старт компонентов после доставки секретов.

## 2. Состояния

| Состояние | Что означает | Условие выхода |
|---|---|---|
| `RUNNING` | нода в кластере, пробы отвечают | недоступность REST дольше `NodeDeadSec` → `UNREACHABLE` |
| `UNREACHABLE` | REST молчит, но контейнер работает — деградация наблюдаемости | контейнер мёртв (положительное свидетельство) → `REBUILD`; ожил → `RUNNING` |
| `REBUILD` | контейнер/том удалены, создаётся новый с пустым диском | контейнер запущен → `BOOTSTRAP` |
| `BOOTSTRAP` | Patroni на пустом PGDATA делает `pg_basebackup` с лидера | клон завершён → `REJOIN` |
| `REJOIN` | нода догоняет WAL / делает `pg_rewind` | streaming → `RUNNING` |
| `RECREATE_SOFT` | маркер оператора: живая нода выводится graceful switchover'ом и пересоздаётся | switchover завершён → `REBUILD` |
| `RECREATE_HARD` | маркер оператора: снос/пересоздание без graceful-пути | `REBUILD` |
| `QUARANTINED` | шард эвакуирован; нода при возврате не допускается | только ручной runbook |
| `RESTORE_ONLY` | одноузловой шард: rebuild «в пустоту» запрещён | restore из бэкапа |

Переходы: `RUNNING ↔ UNREACHABLE`; `UNREACHABLE → REBUILD` (только при
положительном свидетельстве смерти); `REBUILD → BOOTSTRAP → REJOIN →
RUNNING`; `TO_RECREATE → RECREATE_SOFT/HARD → REBUILD`; `REMOVING`/
`QUARANTINED` — вне самовосстановления.

## 3. Классификация

Patroni выбирает способ по PGDATA:

| PGDATA | Действие |
|---|---|
| пусто/не существует | `pg_basebackup` с лидера → реплика |
| небольшое расхождение | догоняет по WAL |
| большое расхождение | `pg_rewind` (`use_pg_rewind: true`) |
| повреждено/несогласовано | может застрять → rebuild с чистого места |

Ключ `/service/<scope>/initialize` уже существует — пустая нода не делает
`initdb`, а клонируется с лидера.

## 4. Правила rebuild (PgWorker)

- **Не лидер**: лидер сначала выводится штатным failover; для
  `start_failed`-лидера — **failover-first**: снос только после факта смены
  лидера в DCS (иначе уничтожается авторитетная копия).
- **Кворум шарда жив**: мертва максимум одна нода, живых ≥ `max(1, R−1)`.
- **Положительное свидетельство смерти**: контейнер отсутствует или не
  running; недоступность REST при живом контейнере — не повод сносить.
- **Portalloc переиспользуется**: rebuild сохраняет закреплённые порты и
  адреса; dsn не меняется.
- **Секреты**: новый контейнер не несёт секретов; агент публикует
  `not_initialized`, PgWorker доставляет bootstrap-ключ и пароли, агент
  поднимает `spilo`/`pg_doorman`
  ([pgnode/processes/f-node-agent.md](../../pgnode/processes/f-node-agent.md)).
- **Одноузловой шард (R=1)**: rebuild «в пустоту» запрещён — только restore
  из бэкапа ([backups/03-restore.md](../../backups/03-restore.md)).
- **Усыновлённые (`object`) ноды**: self-healing отключён; мёртвая —
  `UNREACHABLE` + journal, разбор оператором (rebuild поднял бы дубль рядом
  с внешним orchestration-кругом).
- **`QUARANTINED`/`REMOVING`** — надзор не пробирует и state не трогает.

## 5. Ход rebuild

1. Надзор фиксирует смерть (docker-инспекция) и отсутствие лидерства.
2. Удаляет контейнер и том, создаёт контейнер заново с пустым диском
   (образ `pgworker-node`, тот же portalloc, те же заявки ресурсов).
3. Агент PgNode после доставки секретов стартует `spilo`; Patroni видит
   существующий scope и пустой PGDATA → `pg_basebackup` с лидера.
4. Нода стартует репликой, догоняет WAL, попадает в `/service/<scope>/members`.
5. Надзор ставит `RUNNING` по факту HA-контура и закрывает
   `work.last_rebuild` (cause `auto-dead`/`operator-recreate`).

## 6. Отказы

- **Нет живого лидера/кворума** — rebuild не начинается (`BLOCKED`):
  сначала failover/восстановление DCS.
- **`pg_basebackup` не завершается** — нода остаётся в provisioning;
  повтор/пересоздание по бюджетам Patroni, причина — в journal.
- **Смерть хоста** — вместе с нодой теряется локальный том; rebuild на
  другом/том же хосте по плану размещения; данные — репликацией/бэкапом.
- **Повторный rebuild** — идемпотентен: состояние в etcd/журнале, порты
  закреплены, новый диск создаётся заново.
