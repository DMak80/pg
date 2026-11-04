# PgWorker: жизненный цикл кластера

Уровень 3. Функция «жизненный цикл»: создание кластера, демонтаж, надзор
нод, конвергенция DCS и усыновление Active-кластера. Полные автоматы
состояний и переходов — процессы [A](processes/a-provisioning.md),
[B](processes/b-deprovisioning.md), [C](processes/c-node-supervisor.md),
[J](processes/j-adoption.md); перечисления —
[processes/enums.md](processes/enums.md).

## 1. Состояния кластера и нод

- `config.state=NOT_INITIALIZED` — заявка панели на создание; `TO_REMOVE`
  — заявка на полный демонтаж; отсутствие `state` — Active-кластер.
  Семантика полей `config` —
  [pg-clusters/11-buckets.md](../pg-clusters/11-buckets.md) §2.
- Состояния нод (`NOT_INITIALIZED`, `PROVISIONING`, `RUNNING`,
  `REBUILDING`, `UNREACHABLE`, `QUARANTINED`, `REMOVING`, `TO_RECREATE`)
  и их владельцы —
  [processes/enums.md](processes/enums.md); процессы не трогают ноды
  `TO_REMOVE`/`REMOVING`/`QUARANTINED` чужих процессов.
- Классификация в цикле: `config.state=NOT_INITIALIZED` → Provisioning;
  `TO_REMOVE` → Deprovisioning; иначе (инициализирован) → надзор.

## 2. Создание кластера (A)

От `NOT_INITIALIZED` до рабочего кластера: план размещения и портов
([01-placement.md](01-placement.md) §5), ensure per-cluster секретов
([04-security.md](04-security.md) §1), создание томов и контейнеров,
ожидание Patroni, создание БД/ролей/схем бакетов, публикация `dsn` и
снятие `state`. Guard входа — полный набор ключей панели (config,
shards/*/replicas, nodes/*, routing всех N), иначе `waiting-keys`:
полуфабрикат не provisioning'уем.

Идемпотентность, точные фазы P0–P5, условия ожиданий, таймауты и бэкофф —
[A. ProvisioningProcess](processes/a-provisioning.md).

## 3. Демонтаж кластера (B)

От `TO_REMOVE` до чистого etcd и удалённых docker-объектов; порядок —
«сначала docker, потом etcd» (мёртвые ключи при сбое безвредны, повторный
тик продолжает). Удаляются контейнеры/тома `pgw-<C>-*`, префикс
`/clusters/<C>/`, заявки `/pgworker/moves/<C>/`, `/pgworker/rotations/<C>`,
`/pgworker/{portalloc,work,claims}/<C>*`, журналы эвакуаций; клэйм
снимается явно (del + revoke lease), не ожидая TTL. Имя кластера
освобождается — повторное создание панели пройдёт.

Точные шаги D0–D3 — [B. DeprovisioningProcess](processes/b-deprovisioning.md).

## 4. Надзор нод (C)

Постоянная сверка декларации с фактом docker и живой пробой Patroni:

- **Снесённый контейнер** пересоздаётся (декларативное
  самовосстановление), `state=PROVISIONING→RUNNING`; тюнинг —
  от АКТУАЛЬНЫХ `request_*` на момент пересоздания.
- **Мёртвая нода** (Patroni-REST молчит дольше `NodeDeadSec` и есть
  положительное свидетельство смерти процесса) → rebuild с чистого места;
  Patroni делает `pg_basebackup` с лидера. Недоступность REST при работающем
  контейнере — деградация наблюдаемости, снос здорового контейнера
  недопустим: нода остаётся `UNREACHABLE`, rebuild — когда контейнер
  действительно мёртв.
- **Мёртвый лидер** → ускорение failover: удаление
  `/service/<scope>/leader` + `/service/<scope>/failover` с кандидатом;
  промоушен — в пределах `loop_wait` (полный бюджет и границы Swarm/Plain —
  [C. NodeSupervisor](processes/c-node-supervisor.md)).
- **Конвергенция DCS-конфига**: сверка динамического конфига Patroni
  (тайминги, `synchronous_mode`, `synchronous_mode_strict` per-cluster,
  `postgresql.parameters` = merge(PGTune ∪ канон)) и ОДИН PATCH /config при
  расхождении; лишние ключи удаляются, «старое не живёт параллельно
  канону». postmaster-параметры Patroni помечает `pending_restart` —
  применяются при ближайшем рестарте ноды; воркер никогда не инициирует
  рестарт PG.
- **Весь шард недоступен** дольше `ShardDeadSec` → эвакуация
  ([05-bucket-evacuation.md](05-bucket-evacuation.md)).
- **HA-факты** (`last_failover`/`last_rebuild`) — последний факт каждого
  вида в журнале работы; открытое событие переживает takeover, закрывается
  фактом смены лидера/работоспособности ноды.
- **MasterKeyReconciler (P11)**: сверка master-ключа с фактическим primary
  по `GET /primary`; коррекция `host:<doorman-port>` — только при
  рассинхроне (не второй регулярный писатель). Усыновлённые шарды сверки
  не проходят: их master-ключ пишет внешний HA-контур, резолв мастера
  понимает оба формата.
- **Усыновлённые ноды** (запись portalloc с `object`): живость —
  Patroni-REST, при `patroni=0` — SQL-проба; сверка декларации к ним не
  применяется, self-healing отключён (rebuild поднял бы дубль рядом с
  внешним orchestration-кругом). Мёртвая усыновлённая нода получает
  `UNREACHABLE` + journal, разбор оператором.
- **Доставка секретов**: статусный ключ ноды
  ([04-security.md](04-security.md) §4) — доставка по `not_initialized`,
  ожидание `initializing`, применение по расхождению `applied_revision`,
  пересоздание с новым диском по `start_failed` (с failover-first для
  лидера и неприкосновенностью scope живого кластера). Повтор команды
  применения — один пин на ревизию bootstrap-ключа; прогресс читается
  тиками по статусному ключу.

Границы: шард без `dsn` — домен AddShard; шард с маркером `TO_REMOVE` —
домен RemoveShard (самовосстановление отключено, пробы остаются); ноды
`QUARANTINED`/`REMOVING` не пробируются и их state не трогается. Полные
правила и автомат — [C. NodeSupervisor](processes/c-node-supervisor.md).

## 5. Усыновление Active-кластера (J)

«Внешних» кластеров для воркера не существует: Active-кластер с шардами
(`dsn` есть), у которых нет записей в `/pgworker/portalloc/<C>` (portalloc
потерян / кластер поднят вне провижининга — сид, стенд, восстановление
etcd), воркер **усыновляет**: восстанавливает адреса из HA-контура и
docker-инспекции, закрепляет их в portalloc, ставит недостающие
`nodes/<n>/state=RUNNING`, ensures app-секреты и роли, затем кластер
переходит в обычный надзор. Кандидаты определяются положительным
свидетельством в docker: ни одного контейнера кластера не найдено — тихий
skip (кластер живёт вне docker-хостов воркера).

Точные шаги AD0–AD4 и правила частичного усыновления —
[J. AdoptionProcess](processes/j-adoption.md).

## 6. Ensure-инвариант Active

Каждый тик Active-ветки для всех dsn-шардов выполняет ensure БД и ролей,
даже когда усыновлять нечего: подключение к `postgres` (паттерн P2.x) +
схемы бакетов-владельцев по routing (`CREATE SCHEMA IF NOT EXISTS`). После
утраты данных и re-bootstrap Patroni артефакты etcd (dsn/portalloc/nodes)
есть, а баз/схем нет (initdb создал только postgres) — целевое подключение
падало бы вечным 3D000, панели не сходился inventory (routing ↔ схемы).
Гварды идемпотентны — здоровый кластер платит несколько дешёвых SELECT за
тик. Инвариант адресов Active: `portalloc`/`dsn` = факт живых канонических
контейнеров кластера, репарация каждый тик
([processes/j-adoption.md](processes/j-adoption.md) AD2').
