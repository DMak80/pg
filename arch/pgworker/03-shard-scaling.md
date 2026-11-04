# PgWorker: масштабирование шардов

Уровень 3. Функция «масштабирование шардов»: подъём отдельного пустого
шарда в Active-кластере и демонтаж шарда; без автоматической
перебалансировки бакетов. Полные автоматы — процессы
[G](processes/g-add-shard.md), [H](processes/h-remove-shard.md); состояния
нод — [processes/enums.md](processes/enums.md).

## 1. Порядок в тике

Active-ветка после надзора выполняет scale-проход `ScaleShardsAsync`:
сначала remove-кандидаты (`shards/<X>/state=TO_REMOVE`), затем
add-кандидаты (declared-ноды без `dsn`), по одному шарду за тик. Демонтаж
освобождает хосты/порты до подъёма (Д13).

## 2. Добавление шарда (G)

Подъём ОТДЕЛЬНОГО пустого шарда в Active-кластере (панель заявила
декларацию [adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md)
§9.5: replicas + nodes/NOT_INITIALIZED + request_*). Машина состояний
одного тика, идемпотентна — механика ProvisioningProcess в
scoped-to-shard виде: EnsureNode, WaitPatroni, portalloc-merge,
DatabaseProvisioner. Тюнинг — per-shard от заявок `request_*` (один
результат на все ноды прохода); doorman-бюджет — от рассчитанного
`max_connections`.

Guard'ы и границы:

- A1: кластер Active; полное объявление (replicas>0,
  nodes.Count==replicas, ноды NOT_INITIALIZED/PROVISIONING — иначе
  `waiting-keys`); `dsn` нет (есть → Done); scope
  `/service/<C>-<X>/initialize` отсутствует — либо есть, но лидер
  совпадает с именем нод НАШЕГО шарда (наш же поднимающийся Patroni после
  A3 — идемпотентность повторных тиков); коллизия имён — initialize с
  чужим лидером (перманентная ошибка); имя шарда
  `^[a-z][a-z0-9_]{0,30}$`; перечитывание config — NOT_INITIALIZED/
  TO_REMOVE → `aborted`.
- Strict-гвард: strict-кластер (`config.synchronous_mode_strict` = true,
  отсутствие поля = true) отклоняет add-shard с `replicas < 2` (400
  панели) — без sync-standby новый шард блокировал бы запись.
- Перед созданием ролей — ensure app-секрета кластера (образец P1.5: у
  живого кластера ключи уже есть — читаем; отсутствуют, кластер создан до
  app-секрета, — генерируем и кладём txn put-if-absent; роль app
  выравнивается `ALTER ROLE … PASSWORD`).
- A5: БД/роли — ТОЛЬКО они; СХЕМЫ БАКЕТОВ НЕ СОЗДАЮТСЯ (шард пустой,
  routing не указывает). Routing/status не пишутся ВООБЩЕ. В SQL-фазе A5
  — свежий re-read app-кредов (пока нода поднималась, ротация
  [04-security.md](04-security.md) §6 могла сменить `app_password`) и ensure
  app_params нод шарда.

Финальное состояние: у шарда есть `dsn`, все ноды `RUNNING`, `nodes`-ключи
= декларации, routing/status/schema-мир кластера не изменён НИКАК.

Шаги A0–A6 — [G. AddShardProcess](processes/g-add-shard.md).

## 3. Снятие шарда (H)

Демонтаж шарда по маркеру `shards/<X>/state=TO_REMOVE` (пишет панель).
Guard'ы G1–G7 в S1 перед любым разрушающим действием:

| Гвард | Условие |
|---|---|
| G1 | кластер Active |
| G2 | шард заявлен |
| G3 | ни один routing не указывает на шард (P23) |
| G4 | ни один status-ключ не ссылается (owner ИЛИ target) |
| G5 | нет заявок `/pgworker/moves/<C>/` с `to=X` или `old_shard=X` (саморазрешающийся) |
| G6 | нет нод `QUARANTINED` |
| G7 | в кластере есть другой шард |

Провал guard'а = journal `last_error` с причиной + InProgress
(маркерное состояние живёт; после уезда бакетов демонтаж продолжится сам).
Порядок «сначала docker, потом etcd» сохранён (мёртвые ключи при сбое
безвредны — повторный тик продолжает). S3: del prefix `shards/<X>/` +
точечные `request_*` + del prefix scope + portalloc-фильтрация `"<X>/<n>"`
(read-modify-write под клэймом) + del `/pgworker/evacuations/<C>/<X>`.

Финальное состояние: префиксы `shards/<X>/` и `/service/<C>-<X>/` пусты,
записей шарда в portalloc нет, остальные шарды кластера не затронуты.

Шаги S0–S4 — [H. RemoveShardProcess](processes/h-remove-shard.md).
