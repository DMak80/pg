# Спецификация: PgWorker — backend-оркестратор кластеров PostgreSQL

Дата: 2026-08-23. Фаза dev-flow: spec. Режим автономный: решения приняты
исполнителем без опроса пользователя (пользователь запретил вопросы) — каждое
с обоснованием в §14 «Принятые решения». Источники: `../pg/arch/11-bucket-sharding.md`
(схема etcd, процессы), `../pg/arch/12-bucket-pitfalls.md` (реестр P1–P23,
все закрыты — обязателен к учёту), `../pg/arch/scripts/*` (эталоны процессов),
`../AdminPanel/arch/02-etcd-contract.md` (контракт панели, особенно §9),
референс `../Puzzle` (Result, retry, worker-циклы, health checks) и
`../AdminPanel/src/AdminPanel.Etcd` (HTTP JSON gateway etcd-клиент).

---

## 1. Цель

Спроектировать и построить **PgWorker** — фоновый сервис (.NET 10, C#),
который по состоянию в etcd управляет жизненным циклом шардированных
HA-кластеров PostgreSQL через docker (plain / docker swarm):

1. **Provisioning** — кластер в `config.state=NOT_INITIALIZED` (создан панелью
   AdminPanel) доводится до рабочего состояния: поднятие нод (контейнеры/сервисы:
   PG+Patroni+pg_doorman+HAProxy), инициализация Patroni-кластеров, создание
   БД/ролей/схем бакетов, запись `dsn`/`nodes/<n>/state`, перевод бакетов в
   ACTIVE, снятие `config.state`.
2. **Deprovisioning** — кластер в `config.state=TO_REMOVE` (переведён панелью):
   остановка и удаление всех нод и их volume, очистка ключей etcd.
3. **Контроль нод** — штатный контроль работающих кластеров: failover
   отслеживается (Patroni), умершая навсегда нода пересобирается (rebuild по
   образцу `arch/scripts/rebuild-node.sh`), декларативное самовосстановление
   (снесённый руками контейнер пересоздаётся).
4. **Эвакуация бакетов** — при полной недоступности шарда: перевод бакетов
   на живые шарды (аварийный, без логической репликации — источник недоступен),
   с журналом в etcd и карантином вернувшегося шарда.

Свойства (требования пользователя): несколько инстансов PgWorker работают
одновременно без конфликтов; смерть контролирующего инстанса не роняет
процессы — другой инстанс берёт роль на себя (takeover); все операции
идемпотентны; всё значимое состояние переживает смерть контроллера (живёт
в etcd + самих нодах).

## 2. Границы

### In scope (MVP этой задачи)

- Сервис `PgWorker`: циклы опроса etcd, координация инстансов, процессы
  provisioning / deprovisioning / node-supervision / эвакуация бакетов /
  сверка мастер-ключей (P11) / регулярные снапшоты etcd (P12).
- Управление docker в двух режимах: **plain** (контейнеры на перечисленных
  хостах, подключение per-host к Docker Engine API) и **swarm** (сервисы
  через manager endpoint, spread-антиаффинити).
- Кастомный образ узлы `pgworker-node` (Spilo + pg_doorman + HAProxy +
  supervisord + lease-скрипт мастер-ключа) — единая единица размещения.
- Контракт etcd: чтение/запись ключей кластеров + НОВЫЕ ключи координации
  воркеров `/pgworker/*` (leader election, пер-кластерные клэймы, журнал работ).
- Интеграция с панелью AdminPanel по её контракту (02 §9): PgWorker — тот
  самый «будущий provisioning», паньель только пишет состояния.
- arch/-документация (deliverables — §13), тесты (unit + integration),
  поставка (Dockerfile, appsettings).

### Out of scope (в `arch/roadmap/`, не в этой задаче)

- **Плановые переезды бакетов** (`move-bucket.sh`-эквивалент на C# с полной
  механикой P1–P8: логическая репликация, заморозка, cutover, rollback,
  finalize) — эвакуация MVP закрывает только аварийный случай «шард умер
  целиком» (копировать не с чего). Штатные переезды остаются скриптами.
- Балансировка бакетов по нагрузке/метрикам, автоскейлинг шардов
  (add-shard/remove-shard как команды панели).
- Per-cluster генерация и ротация секретов; secret-manager (vault); TLS-серты
  с автогенерацией и ротацией (P17: серверные сертификаты doorman не
  hot-reload — план ротации вручную).
- SSH-туннели к Docker-хостам, TLS к Docker Engine API, RBAC/docker-группы —
  enterprise-доработки.
- Prometheus-метрики, алертинг во внешние системы (P21-дашборд — панель).
- Управление самим etcd-слоем (разворачивание, restore — рецепты 04/скрипты).
- Возврат/слияние данных эвакуированного шарда после его восстановления
  (разбор — runbook-операция, PgWorker только карантинит и алертит).

## 3. Роль PgWorker в системе и разделение ответственности

```
AdminPanel (UI)          PgWorker (оркестратор)              docker-хосты
─────────────            ──────────────────────              ────────────
создание кластера  ──►   /clusters/<C>/config.state=          контейнеры/
(заявка структуры)       NOT_INITIALIZED          ──читает──► сервисы узлов
удаление кластера  ──►   config.state=TO_REMOVE   ──создаёт/  pgworker-node
                         ноды, БД, схемы бакетов    удаёт ──►  (Spilo+doorman
инспекция (read-   ◄──   dsn, nodes/<n>/state,                +haproxy)
only, всё видит)         снятие status-ключей,
                         снятие state                Patroni-ноды
                                                     пишут /service/<scope>/,
                                                     callback пишет
                                                     shards/X/master (P11)
```

- **Панель** — декларатор и наблюдатель: пишет ТОЛЬКО `state=NOT_INITIALIZED`
  (создание, claim-txn §9.2) и `state=TO_REMOVE` (удаление, §9.4); читает всё.
  Контракт unchanged: PgWorker не требует правок панели (её толерантность к
  значениям `nodes/<n>/state` и отсутствию status-ключей уже описана в 02 §2.1).
- **PgWorker** — исполнитель: единственный, кто поднимает/удаляет ноды,
  пишет `dsn`, меняет `nodes/<n>/state`, снимает `status/bucket_*`
  (→ ACTIVE) и поле `state` у `config`, чистит префикс кластера при
  TO_REMOVE.
- **Patroni** (внутри нод) — единственный писатель `shards/X/master`
  (callback + lease, P11); PgWorker только сверяет и корректирует по
  фактическому primary (P11 «сверяющий демон»).

## 4. Контракт etcd

Транспорт — HTTP JSON gateway `/v3/*` (как панель, 02 §1): клиент копируется
из `../AdminPanel/src/AdminPanel.Etcd` и расширяется lease-операциями
(`POST /v3/lease/grant`, `/v3/lease/keepalive`, `/v3/kv/put` с `lease`,
txn с compare по `value`/`mod_revision` и `delete` в success-ветке).
Poll, без watch (аргументация — 02 §5; тик 5 с покрывает динамику).

### 4.1. Читаемые ключи (существующая схема)

| Ключ | Зачем |
|---|---|
| `/clusters/<C>/config` | константы (buckets=N, dbname) + `state` (NOT_INITIALIZED/TO_REMOVE/отсутствует) |
| `/clusters/<C>/shards/<X>/replicas` | плановое число нод шарда |
| `/clusters/<C>/shards/<X>/nodes/<n>/state` | состояние нод (свои же записи — проверка идемпотентности) |
| `/clusters/<C>/shards/<X>/master` | живой мастер (lease TTL 5 с) — маршрутизация SQL-операций |
| `/clusters/<C>/buckets/routing/bucket_<i>` | раскладка бакетов (какие схемы создавать на каком шарде) |
| `/clusters/<C>/buckets/status/bucket_<i>` | `NOT_INITIALIZED` — снять после создания схемы; SYNCING/FROZEN/ABORTING — незавершённый переезд (эвакуация такого бакета запрещена) |
| `/service/<scope>/…` (scope=`<C>-<X>`) | Patroni DCS: `leader`, `members/<name>`, `initialize` — подтверждение поднятия HA-кластера |
| `/service/<scope>/request_{cpu,mem,disk}` | заявки ресурсов на ноду (лимиты контейнера/сервиса) |

### 4.2. Пишемые ключи (существующая схема)

| Ключ | Когда | Значение |
|---|---|---|
| `/clusters/<C>/shards/<X>/dsn` | после поднятия нод шарда | `host=h1,h2 port=15432,15433 dbname=<C> user=bucket_admin` (multi-host, **без пароля**, P12/P17; порты — выделенные аллокатором, §6.3) |
| `/clusters/<C>/shards/<X>/nodes/<n>/state` | весь жизненный цикл | таблица состояний §6.4 |
| `/clusters/<C>/buckets/status/bucket_<i>` | DELETE при завершении provisioning | снятие = бакет ACTIVE (семантика 11 §2, панели 02 §2.1) |
| `/clusters/<C>/config` | txn по завершении provisioning | пере-put канонического JSON **без поля `state`** (инициализирован = поле отсутствует, 02 §2.1; compare по `mod_revision`) |
| `/clusters/<C>/…` (весь префикс) | TO_REMOVE, финал | `del --prefix` |
| `/service/<C>-shard<k>/request_*` | TO_REMOVE, финал | точечные `del` (свои заявки; остальное пространство Patroni не трогаем) |
| `/service/<C>-<X>/` (весь scope) | TO_REMOVE, после удаления нод | `del --prefix` (guard: контейнеров/сервисов нет) |
| `/clusters/<C>/shards/<X>/master` | ТОЛЬКО при рассинхроне (P11-сверка) | lease-put `host:6432` по фактическому primary из Patroni REST |

**Финальное состояние кластера после provisioning** (решение Д1): `config`
без поля `state` (унификация с кластерами `init-cluster.sh` — 02 §2.1
«отсутствует/иное = обычный инициализированный кластер»), все
`status/bucket_<i>` удалены (бакеты ACTIVE), у каждого шарда есть `dsn`,
у каждой ноды `state=RUNNING`. Панель видит кластер «Active» без правок.

### 4.3. НОВЫЕ ключи координации воркеров (префикс `/pgworker/`)

Панель этот префикс не читает (её снапшот ограничен `/clusters/`, `/service/`,
`/cluster/nodes/`) — координация не видна UI и не мешает контракту.

| Ключ | Тип | Назначение |
|---|---|---|
| `/pgworker/leader` | lease TTL 15 с | глобальный лидер для singleton-задач (регулярные снапшоты P12). Value: `{"instance":"<id>","since_unix":…}`. Захват: txn `version==0` + put-with-lease; продление keepalive раз в 5 с. Умер лидер → lease истёк → любой другой захватывает. |
| `/pgworker/claims/<C>` | lease TTL 15 с | **пер-кластерный клэйм** работы (решение Д2): exclusivity обработки кластера одним инстансом. Value: `{"instance":"<id>","since_unix":…,"phase":…}`. Захват txn `version==0` + put-with-lease; держатель продлевает. Takeover: lease истёк → ключ исчез сам → txn другого инстанса succeeds. |
| `/pgworker/work/<C>` | обычный | журнал текущего процесса кластера (journal-before-manipulations, по образцу P7): `{"op":"provision\|deprovision\|evacuate\|rebuild","phase":"…","updated_unix":…,"instance":"<id>","last_error"?}`. Крах оставляет самодокументирующийся след; следующий инстанс продолжает с записанной фазы. |
| `/pgworker/evacuations/<C>/<X>` | обычный | журнал эвакуации шарда: `{"evacuated_unix","reason","buckets":{...старый→новый владелец...},"state":"DONE\|QUARANTINED"}` — истина для разбора после возврата шарда. |
| `/pgworker/portalloc/<C>` | обычный | закрепление выделенных портов за нодами (см. §6.3): `{"<shard>/<node>":{"host":"h1","pg":15432,"patroni":18008,"doorman":16432}}` — переживает смерть инстанса, переиспользуется при rebuild. |
| `/pgworker/instances/<id>` | lease TTL 15 с | живость инстансов (диагностика; необязательно для работы) |

Инварианты: любая мутация чужих данных (`/clusters/`, docker) выполняется
**только держателем клэйма** `<C>`; txn-записи в `/clusters/` сопровождаются
compare (routing=старое значение, config.mod_revision) — «применилось, а
контрол-плейн не знает» невозможно (как flip в 11 §5 шаг 4.7).

## 5. Модель развертывания

### 5.1. Образ узлы `pgworker-node` (единая единица размещения)

Нода кластера-шарда = **один контейнер/сервис** из кастомного образа
(решение Д4): `ghcr.io/zalando/spilo-16:3.3-p3` + `pg_doorman` + `haproxy`
+ `supervisord` + python-скрипт мастер-lease (эталон —
`arch/stand/sidecar/rolecheck.py`: `/v3/lease/grant` + keepalive цикл 1 с,
TTL 5 с). Внутри контейнера всё общается через localhost — работает и в
plain, и в swarm (у swarm нет host-network и «подов»; sidecar'ы отдельными
сервисами громоздки — см. Д4).

Роли внутри: PostgreSQL `:5432` (только localhost + HAProxy-вход),
Patroni REST `:8008`, pg_doorman `:6432` (бэкенд `127.0.0.1:5432`,
единственный пул `<dbname>`, `pool_mode=transaction`, TLS require — P13/P14/P17),
HAProxy `:5432` (health-check `GET /primary` всех Patroni-нод шарда —
репликационный вход переездов, P2). Конфиги генерирует PgWorker при создании
(шаблоны §6.5), наружу публикуются порты: `pg`(для межшард-подписок,
`5432`→выделенный), `patroni`(8008→выделенный), `doorman`(6432→выделенный).

Volume: `pgw-<C>-<X>-<n>-data` → `/home/postgres/pgroot` (PGROOT Spilo).

### 5.2. Режим Plain (docker на выделенных хостах)

- Конфиг `PgWorker:Docker:Hosts[]` — таблица хостов: `{Name, Endpoint}`
  (`tcp://10.0.1.11:2375` или `unix:///var/run/docker.sock` для локального).
  Каждый хост — свой клиент Engine API (DOCKER_HOST per-host, решение Д5).
- PgWorker сам вычисляет placement (§6.3) и создаёт контейнеры на выбранных
  хостах: `POST /containers/create?name=pgw-<C>-<X>-<n>` → `start`.
  Restart-политика `unless-stopped` (docker сам поднимает после ребута хоста).
- Анти-аффинити: ноды одного HA-кластера (шарда) размещаются на разных
  хостах, если `hosts >= replicas`; иначе — равномерно по least-loaded
  (число занятых слотов из `/pgworker/portalloc` + `GET /containers/json`).
- Ограничение: хост должен открывать Engine API по TCP в сети PgWorker
  (home-окружение, без enterprise-защит; TLS/RBAC — roadmap).

### 5.3. Режим Swarm

- Конфиг `PgWorker:Docker:SwarmManager` — endpoint любого manager-узла.
  Одна нода шарда = один сервис `pgw-<C>-<X>-<n>` с `replicas=1`
  (гранулярность сервиса = нода: это даёт точечный rebuild без
  `--force-update` всего шарда).
- Анти-аффинити: constraint `node.labels.pgworker.host==<host>` НЕ
  используется (хосты не обязаны маркироваться); вместо этого PgWorker
  назначает нодам placement через `placement.preferences
  spread=node.id` + constraint на конкретную ноду, вычисленную из
  `GET /nodes` (число работающих тасков на узле — least-loaded spread).
  PgWorker периодически сверяет фактические `tasks` сервисов со своим
  планом и пересоздаёт drifted-сервисы (декларативность).
- Порты: `publish mode=host` (без ingress-балансировщика) с выделенными
  аллокатором портами на машине таска; DSN строится по факту
  (`host=<машина> port=<порт>` из `GET /tasks/<service>` → `NodeId` → node).
- Overlay-сеть `pgw-net` для межконтейнерного трафика не обязательна
  (взаимодействие через host:port), создаётся только если образ требует.

### 5.4. Сам PgWorker

Контейнер (`docker-compose.yml` в поставке): монтируется
`/var/run/docker.sock` (plain на одном хосте / swarm manager), volume под
снапшоты etcd (`/snapshots`, P12), env-секреты (§10). Масштабирование —
просто N реплик сервиса (обычный `replicated`, не swarm-специфичный):
координация в etcd разбирает роли.

## 6. Архитектура сервиса

### 6.1. Проекты (решение `src/PgWorker.slnx`, CPM в `Directory.Packages.props`)

| Проект | Содержимое |
|---|---|
| `PgWorker.Core` | модель домена (records): `ClusterConfig`, `ShardSpec`, `NodeSpec`, `BucketMap`, `PlacementPlan`, enum'ы состояний; чистые функции планирования: `PlacementPlanner` (анти-аффинити), `EvacuationPlanner` (целевые шарды), `SpiloEnvBuilder`/`DoormanConfigBuilder`/`HaproxyConfigBuilder` (шаблоны конфигов ноды из параметров P11/P13/P15/P17 и request_*); `Result` (копия Puzzle) |
| `PgWorker.Etcd` | клиент gateway (адаптация `AdminPanel.Etcd`: Range/Put/Txn/Delete + `LeaseGrant`/`LeaseKeepalive`, compare по value/mod_revision, delete в txn); `ClusterSnapshotParser` (парсеры /clusters/ + /service/); `ClaimStore` (leader/клэймы, keepalive-фон) |
| `PgWorker.Docker` | тонкий клиент Docker Engine API (решение Д3): `IDockerEngine` (Containers CRUD, Services CRUD, Nodes, Tasks, Volumes) поверх `HttpClient` + unix-socket/TCP ConnectCallback; `DockerClusterDriver` (plain/swarm стратегии создания/удаления ноды); фикстурные модели только нужных полей |
| `PgWorker.Provisioning` | процессы (машины состояний, §6.4): `ProvisioningProcess`, `DeprovisioningProcess`, `NodeSupervisor`, `BucketEvacuator`, `MasterKeyReconciler` (P11), `SnapshotJob` (P12, `/v3/snapshot/save` → файл); SQL-слой (Npgsql): `DatabaseProvisioner` (БД/роли/схемы/гранты), `ShardProbe` (Patroni REST + SQL-пробы из `buckets-common.sh`) |
| `PgWorker.App` | host: `Program.cs` (host-builder, DI по образцу Puzzle attribute-DI), `BackgroundService`-циклы (§6.2), health checks, конфигурация appsettings |
| `tests/PgWorker.UnitTests` | парсеры, планировщики, машины состояний на etcd/docker-фикстурах |
| `tests/PgWorker.IntegrationTests` | Testcontainers: etcd (клэймы/txn/lease, provisioning-контракт), docker (если доступен — реальные create/rm, помечено trait'ом `DockerAvailable`) |

Зависимости: `Npgsql` (SQL), `Polly` + `Polly.Contrib.WaitAndRetry` (копия
`RetryPolicies` из Puzzle), `Microsoft.Extensions.*` (hosting/http/options),
тесты — `xunit.v3`, `FluentAssertions`, `Testcontainers` (версии — как в
AdminPanel CPM). Техсоглашения: `net10.0`, `LangVersion=latest`,
`Nullable=enable`, `TreatWarningsAsErrors=true`.

### 6.2. Циклы (по образцу `BusConsumerHostedService` из Puzzle)

Все — `BackgroundService` с бесконечным `while (!ct.IsCancellationRequested)`,
ошибка тика не роняет цикл (лог + задержка; следующий тик = ретрай):

1. **ReconcileLoop** (тик 5 с, конфиг): range `/clusters/` + `/service/` →
   снапшот; классификация кластеров: `NOT_INITIALIZED` → попытка клэйма →
   `ProvisioningProcess`; `TO_REMOVE` → клэйм → `DeprovisioningProcess`;
   остальные (инициализированные) → клэйм → лёгкий проход `NodeSupervisor`.
   Клэймленный кластер обрабатывается до смены состояния; процесс хранит
   прогресс в `/pgworker/work/<C>` — тик продолжает с записанной фазы.
2. **KeepaliveLoop** (тик 5 с): продление lease'ов всех удерживаемых клэймов,
   лидерского lease, instance-ключа. Умер процесс — lease истекают ≤15 с.
3. **SnapshotLoop** (только глобальный лидер; тик — раз в 6 ч, конфиг):
   `/v3/snapshot/save` → файл в volume; плюс внеочередные снапшоты в точках
   изменений (после provisioning/deprovisioning/эвакуации — P12).

Параллелизм: процессы разных кластеров — параллельно (`SemaphoreSlim`,
лимит конфигурируется); внутри кластера — строго последовательно.

### 6.3. Placement и порты (как PgWorker узнаёт хосты)

Таблица хостов — статический конфиг `PgWorker:Docker:Hosts[]` (plain) или
`GET /nodes` swarm-менеджера (swarm). При provisioning:

1. Для каждого шарда `X` с `replicas=R` и нодами `<X>a..<X><буква>`:
   `PlacementPlanner` выбирает R хостов — все разные (анти-аффинити), при
   `hosts < R` — равномерно с минимальным числом совпадений («если топология
   позволяет — на разных, иначе на одной», требование пользователя).
2. Порт-аллокатор: каждой ноде — тройка портов из диапазона конфига
   (напр. 15000–15999: pg=base+i, patroni=+3000, doorman=+1500), проверка
   занятости (`GET /containers/json` + свои записи). Закрепление —
   `/pgworker/portalloc/<C>` (переживает rebuild: та же нода = те же порты).
3. Итог — `PlacementPlan` (node → host + порты), он же вход для генерации
   конфигов (HAProxy-бэкенды = Patroni-адреса всех нод шарда, DSN multi-host).

### 6.4. Процессы (машины состояний)

**Состояния ноды** (`/clusters/<C>/shards/<X>/nodes/<n>/state`, пишет
PgWorker; панель отображает как строку — правок не требует):

| Значение | Смысл |
|---|---|
| `NOT_INITIALIZED` | панель заявила (исходное) |
| `PROVISIONING` | контейнер/сервис создаётся, ждём Patroni |
| `RUNNING` | Patroni-нода в кластере (role master/replica из `/service/<scope>/members`) |
| `REBUILDING` | пересоздание с чистого места (rebuild) |
| `UNREACHABLE` | периодически недоступна (Patroni REST молчит), ждём/разбираемся |
| `QUARANTINED` | шард эвакуирован; нода при возврате не допускается к работе |
| `REMOVING` | в процессе deprovisioning (ключ удаляется в конце) |

**A. ProvisioningProcess** (`config.state=NOT_INITIALIZED`; все шаги
идемпотентны — перепроверяют факт, эталон `init-cluster.sh`):

```
P0 claim + journal(/pgworker/work/<C>, op=provision)
P1 план: placement (§6.3) для всех шард/нод; порт-аллокация; journal phase=planned
P2 на каждого шарда X:
   P2.1 для каждой ноды n: создать volume + контейнер/сервис с конфигом
       (SpiloEnvBuilder: SCOPE=<C>-<X>, ETCD_HOSTS, ttl=5/loop_wait=2 (P11),
        wal_level=logical + sync_replication_slots + max_slot_wal_keep_size (P3/P4),
        max_connections=60 и бюджет P15, callback on_role_change → lease-скрипт
        мастер-ключа; DoormanConfig: пул <dbname>, TLS require; HaproxyConfig:
        бэкенды всех Patroni-нод шарда), env-секреты (§10);
        nodes/<n>/state=PROVISIONING; при существовании (re-run) — сверить
        конфиг и пропустить
   P2.2 ждать: /service/<C>-<X>/initialize есть + leader есть + у каждой
        ноды Patroni REST отвечает (бюджет: 10 мин, транзиент-толерантно);
        nodes/<n>/state=RUNNING
   P2.3 на мастере шарда (адрес из master-ключа/Patroni): создать БД <dbname>
        (если нет), роли app/bucket_admin/bucket_mover + GRANT'ы (§4 доки 11),
        если нет
   P2.4 создать схемы bucket_<i> по routing (только шардовы бакеты; CREATE
        SCHEMA IF NOT EXISTS, GRANT USAGE) — идемпотентно
   P2.5 записать shards/X/dsn (multi-host, без пароля)
P3 снять ВСЕ status/bucket_<i> (txn-пакетами ≤128 ops) — бакеты ACTIVE
P4 config: txn (compare mod_revision) → put канонического JSON без state
P5 снапшот P12; journal phase=done; клэйм освобождается (оставляем до
   истечения или release-put) — кластер переходит в обычный надзор
```

Отказ на шаге: journal `last_error` + фаза; ретрай следующим тиком
(бэкофф). Частично созданные контейнеры не страшны (P2.1 идемпотентен).
Кластер со «зависшим» NOT_INITIALIZED (панель умерла на записи) — PgWorker
видит частичные ключи (нет routing и т.п.) → journal + alert-состояние,
ожидание доустойчивости ключей (панель перепишет при повторе создания —
409 на клэйме имени); полуфабрикат NOT_INITIALIZED без полного набора не
provisioning'уем (guard: обязательны config, shards/*/replicas, nodes/*,
routing всех N).

**B. DeprovisioningProcess** (`config.state=TO_REMOVE`):

```
D0 claim + journal(op=deprovision)
D1 для каждого шарда/ноды: остановить и удалить контейнер/сервис (swarm:
   service rm), удалить volume pgw-…-data; nodes/<n>/state=REMOVING;
   идемпотентно (404 = ок)
D2 удалить префикс /clusters/<C>/ (del --prefix) + точечные
   /service/<C>-shard<k>/request_* + префикс /service/<C>-<X>/
   (guard: docker-объектов не осталось) + /pgworker/{portalloc,work,claims}/<C>*
D3 снапшот P12; journal удаляется вместе с префиксом — успех = пустой
   /clusters/<C>/; имя освобождается (повторное создание панели пройдёт)
```

Удаление нод до чистки etcd — порядок осознанный: «мертвые» ключи при
сбитом D1 безвредны (кластер в TO_REMOVE, панель показывает «к удалению»),
повторный тик продолжает.

**C. NodeSupervisor** (инициализированные кластеры, тик внутри ReconcileLoop):

- Сверка декларации с фактом: каждой плановой ноде соответствует
  контейнер/сервис (по имени) — снесённый руками пересоздаётся
  (декларативное самовосстановление), state=PROVISIONING→RUNNING.
- Patroni-REST каждой ноды (`GET /cluster`, timeout 3 с): state/role.
  Нода недоступна дольше `NodeDeadThreshold` (90 с, конфиг) и **не лидер**
  и кворум шарда жив (≥2 нод member'ов с REST 200) → **rebuild**: удалить
  контейнер + volume, создать заново (Patroni сделает pg_basebackup с
  лидера — эталон `rebuild-node.sh`), state=REBUILDING → RUNNING.
  Лидер недоступен → ничего не делаем: failover делает Patroni (P11,
  окно ~5–8 с); после failover лидер-призрак становится репликой/умершей
  и обрабатывается общим путём.
- Весь шард недоступен (все ноды UNREACHABLE, master-ключ протух) дольше
  `ShardDeadThreshold` (300 с, конфиг) → **BucketEvacuator** (D).
- `MasterKeyReconciler` (P11): у каждого шарда сверить master-ключ с
  фактом (`GET /primary` по нодам): расхождение или ключа нет при живом
  primary → lease-put коррекция (идемпотентно, пишет только при
  рассинхроне — не второй регулярный писатель).

**D. BucketEvacuator** (аварийная эвакуация, MVP-граница — решение Д6):

Guard'ы перед любым действием (journal-before-manipulations, по образцу P7):
шард недоступен целиком ≥ `ShardDeadThreshold`; ни один бакет шарда не в
SYNCING/FROZEN/ABORTING (незавершённый переезд — блокируем эвакуацию,
alert-журнал, разбор оператором); живые шарды есть; снапшот P12 «до».

```
E0 journal /pgworker/evacuations/<C>/<X> (план: bucket → целевой шард;
   цели — живые шарды, баланс по числу бакетов; при N живых=0 — ждать)
E1 на целевых шардах: CREATE SCHEMA IF NOT EXISTS bucket_<i> + GRANT'ы
   (пустые схемы — источник недоступен, копировать нечего: данные шарда
   остаются на его дисках и вернутся вместе с ним)
E2 по каждому бакету: txn (compare routing=<старый шард>) put routing=
   <новый> — владение переведено; статус-ключей нет → бакет сразу ACTIVE
E3 ноды шарда: state=QUARANTINED; контейнеры/сервисы НЕ удаляются
   (данные на месте!), но при возврате REST-доступности — остановить
   (docker stop), чтобы «призраки» не писали в осиротевшие схемы (P1-логика)
E4 journal state=DONE; снапшот P12 «после»
```

Возврат шарда (REST ожил после эвакуации): PgWorker видит journal
`state=DONE` + quarantine → останавливает ноды, держит QUARANTINED,
в journal — `state=QUARANTINED, returned_unix`. Слияние/восстановление
данных — ручной runbook (out of scope):PgWorker ничего не удаляет и не
запускает сам. Новые записи тенантов эвакуированных бакетов шли в пустые
схемы живых шардов с момента flip — принятая аварийная семантика
(RPO = момент смерти шарда; фиксируется в journal и arch/14).

## 7. Надёжность

- **Идемпотентность**: каждый шаг процессов перепроверяет факт (контейнер
  есть? БД есть? схема есть? routing уже переведён?) — повтор после сбоя
  безопасен; именование объектов детерминировано (`pgw-<C>-<X>-<n>`).
- **Takeover**: состояние процессов — в etcd (journal + фазы в
  `nodes/<n>/state`, `dsn`, portalloc); смерть инстанса гасит lease-клэймы
  ≤15 с, следующий инстанс продолжает с записанной фазы. Двойной контроллер
  невозможен: операции над кластером — только под живым lease-клэймом
  (проверяется перед каждой мутацией docker/etcd: lease ещё мой).
- **Атомарность etcd**: flip-подобные переходы — txn с compare (routing,
  config.mod_revision, version==0 для клэймов); «нет ключа = ACTIVE»
  сохраняется как инвариант (status-ключи снимаются пактами после создания
  схем — между схемой и снятием бакет числится NOT_INITIALIZED, это безвредно:
  панель показывает его, клиенты до инициализации кластера не допускаются).
- **Снапшоты P12**: регулярные (SnapshotLoop) + в точках изменений
  (provisioning/deprovisioning/эвакуация — до и после). Restore — внешний
  рецепт (`restore-cluster.sh`), PgWorker только снимает.
- **Ретраи**: короткие сетевые/SQL-операции — Polly jitter-политики
  (`RetryPolicies` из Puzzle); долгие ожидания (Patroni-подъём, догоняние) —
  транзиент-толерантные циклы с бюджетом (по образцу mover'а,
  `CONN_FAIL_BUDGET_SEC`); ошибка тика процесса → journal.last_error +
  продолжение со следующего тика (бэкофф).
- **Отказ etcd**: контрол-плейн заморожен (P9): PgWorker не делает
  разрушительных операций без свежего клэйма; живые ноды не зависят от
  него (Patroni-DCS — тот же etcd, но это их собственная траектория).
- **Отказ docker-хоста**: размещение не меняется (portalloc фиксирован);
  недоступный хост → нода UNREACHABLE → supervisor-сценарии; создание
  новых нод учитывает только живые хосты.

## 8. Наблюдаемость

- **Health checks** (Puzzle-паттерн `IHealthCheckService`, `/healthz`):
  `etcd-reachable` (все endpoints), `docker-hosts` (per-host ping `/_ping`),
  `loops-alive` (последний тик каждого цикла), `claims` (сколько держим),
  `snapshot-freshness` (возраст последнего снапшота).
- **Логи**: `ILogger`, JSON-console; ключевые события: claim/takeover
  кластера, фазы процессов (с journal-фазой), rebuild ноды, эвакуация
  (полный план), сверка мастер-ключа с коррекцией. Уровень — конфиг.
- **Diag-ключи etcd** уже несут наблюдаемость для панели/оператора:
  `/pgworker/work/<C>` (живая фаза), `nodes/<n>/state`, journal эвакуаций.
- Метрики Prometheus — roadmap; MVP — health + логи (панель показывает
  остальное через свой снапшот).

## 9. Тестирование

- **Unit** (фикстуры — фрагменты реальных значений etcd/docker, по контракту
  панели 02 §8): парсеры снапшота; `PlacementPlanner` (анти-аффинити:
  hosts≥R — все разные; hosts<R — равномерно; determinism);
  `EvacuationPlanner` (баланс, блокировка при статус-ключах переезда);
  машины состояний — таблицы переходов на мок-гейтвее/мок-docker (проверка
  порядка операций, идемпотентность повтором, journal-before-manipulations);
  шаблоны конфигов ноды (Spilo env: scope/etcd/ttl-параметры P11,
  wal_level=logical P3, бюджет P15; doorman: TLS/pool_mode; haproxy:
  бэкенды всех нод).
- **Integration** (Testcontainers):
  - etcd (`quay.io/coreos/etcd:v3.5.21`): клэймы (двое «инстансов» —
    взаимное исключение, истечение lease → takeover), txn-compare
    (конкурентный flip отклоняется), контракт provisioning end-to-end
    (сид в стиле панели 02 §9.1 → ключи-результаты §4.2).
  - docker (trait `DockerAvailable`): реальный create/start/rm контейнера
    (образ `alpine`), portalloc-занятость, идемпотентный re-create.
  - e2e на dev-стенде (`dev-stand/`, compose: etcd + 2 docker-хоста или
    локальный docker): полный сценарий приёмки §12.
- Тесты — AAA-комментарии (правило пользователя), русский текст.

## 10. Поставка и конфигурация

- `Dockerfile` (multi-stage, образ `pgworker`); `docker/` — Dockerfile
  образа узлы `pgworker-node` (Spilo + doorman + haproxy + supervisord +
  lease-скрипт) — собирается при поставке или задаётся готовым тегом.
- `appsettings.json` (секции): `PgWorker:Etcd:Endpoints[]`;
  `PgWorker:Docker { Mode: Plain|Swarm, Hosts[{Name,Endpoint}],
  SwarmManager, PortRange{From,To}, Images{Node,Spilo…}, VolumesDir }`;
  `PgWorker:Loops { ScanIntervalSec=5, KeepaliveIntervalSec=5,
  SnapshotIntervalMin=360 }`; `PgWorker:Thresholds { NodeDeadSec=90,
  ShardDeadSec=300, PatroniBootTimeoutSec=600 }`;
  `PgWorker:Parallelism { MaxClustersConcurrent=4 }`.
- **Секреты** (решение Д7): per-install, из env PgWorker (не в git, не в
  etcd — P12/P17): `PGW_PG_SUPERUSER_PASSWORD`, `PGW_PG_STANDBY_PASSWORD`,
  `PGW_APP_ROLE_PASSWORD`, `PGW_BUCKET_ADMIN_PASSWORD`,
  `PGW_BUCKET_MOVER_PASSWORD`. Прокидываются в ноды при создании
  (env контейнера/секцию сервиса). Per-cluster генерация — roadmap.
- `docker-compose.yml` для запуска PgWorker (volume docker.sock + snapshots).

## 11. Критерии приёмки (проверяемые)

1. `dotnet build src/PgWorker.slnx -c Release` — 0 warnings
   (`TreatWarningsAsErrors=true`); `dotnet test` — зелёные (unit всегда;
   integration-серия etcd — под Testcontainers; docker-серия — при
   доступном docker).
2. **Provisioning e2e** (integration на стенде): сид панели (02 §9.1:
   config NOT_INITIALIZED + 2 шарда + ноды + routing/status NOT_INITIALIZED
   + request_*) → запуск PgWorker → в etcd: у каждого шарда `dsn`
   (multi-host, без пароля), все `nodes/<n>/state=RUNNING`, все
   `status/bucket_*` удалены, `config` без поля `state` (панель в тесте
   читает сырой ключ); на мастерах шардов существуют БД, схемы всех
   бакетов шарда, роли; `docker ps` — контейнеры `pgw-*` с анти-аффинити
   (на стенде 2 хоста: ноды одного шарда на разных).
3. **Идемпотентность/takeover**: `docker kill` инстанса PgWorker посреди
   provisioning → второй инстанс доносит кластер до конца ≤ (lease TTL +
   2 тика), дублей контейнеров/ключей нет (сверка списком).
4. **Deprovisioning**: PUT `config.state=TO_REMOVE` → в пределах таймаута
   `docker ps -a | grep pgw-<C>` пусто, volume'ы удалены, range
   `/clusters/<C>/` пуст, `/service/<C>-*/` пуст; повторный PUT до
   завершения — безвреден.
5. **Failover/rebuild**: `docker stop` контейнера лидера шарда → Patroni
   промоутирует реплику (master-ключ обновлён ≤10 с, проверка etcd),
   остановленный контейнер пересоздан (state REBUILDING→RUNNING), реплика
   догоняет (SQL-проба: `pg_is_in_recovery()` на обеих нодах).
6. **Эвакуация**: остановка всех контейнеров шарда → после
   `ShardDeadSec` бакеты его routing-ключей переведены на живой шард,
   схемы созданы, journal `/pgworker/evacuations/<C>/<X>` заполнен,
   снапшоты до/после есть; возврат нод shard'а → контейнеры остановлены
   PgWorker'ом, state=QUARANTINED, данные не тронуты.
7. **Клэймы**: два инстанса одновременно: один кластер обрабатывает только
   один (журналы: instance id одного); снапшоты снимает только глобальный
   лидер.
8. Prettiest-проверка контракта: интеграционный тест повторяет утверждения
   §4.2/§4.3 (формат ключей клэймов, work-journal, portalloc) против
   реального etcd.

## 12. Риски и открытые вопросы

| # | Риск/вопрос | Митигация |
|---|---|---|
| R1 | Образ `pgworker-node` зависит от бинарников pg_doorman (молодой проект, P16) и их поставки в контейнер | Образ собирается из артефактов релиза doorman; версия — пин в конфиге; при недоступности — plain-режим может стартовать узел без doorman (фича-флаг `EnableDoorman`), DSN-точка 5432 — компромисс задокументирован в arch/14 |
| R2 | Swarm `publish mode=host` + уникальные порты — нестандартный, но поддерживаемый путь; риск коллизий при ручных контейнерах | portalloc-аллокатор проверяет фактическую занятость портов перед созданием; коллизия → сдвиг порта + перегенерация DSN |
| R3 | Spilo callback (master-lease) — сторонний python-скрипт в контейнере; его отказ = протухший master-ключ | P11-сверка PgWorker (MasterKeyReconciler) восстанавливает ключ по Patroni REST — двойной контур по доке 12 P11 |
| R4 | Эвакуация «пустыми схемами» теряет записи умершего шарда с момента смерти | Осознанное аварийное поведение (источник недоступен); всё фиксируется в journal; порог `ShardDeadSec` — конфиг; восстановление/слияние — runbook (roadmap) |
| R5 | Несколько нод кластера на одном хосте (hosts < replicas) — пониженная отказоустойчивость | Требование пользователя («если топология позволяет»); факт отражается в journal-плане (PlacementPlan) и виден оператору |
| R6 | Гонка «панель пишет TO_REMOVE посреди provisioning» | Клэймный инстанс перечитывает config перед каждой фазой; смена state → процесс перепланируется (provisioning абортируется безопасно — контейнеры подчищит deprovisioning) |
| R7 | Восстановление etcd из снапшота (P12) откатывает journal/клэймы | Клэймы — lease (не восстанавливаются из снапшота с живым lease — конфликт невозможен физически); journal может откатиться — процессы идемпотентны, повторная фаза безопасна |
| O1 | Нужно ли в MVP удалять `/service/<C>-<X>/` при deprovisioning или оставить «мёртвый scope»? | Принято: удалять (guard: docker-объектов нет); если всплывут противопоказания — правка arch/14 в фазе исполнения |
| O2 | Формат значения `dsn` при нестандартных портах (multi-host с разными портами) поддерживается libpq | Проверяется integration-тестом (реальное подключение Npgsql к multi-host DSN) |

## 13. Deliverables в arch/ (выполняются в фазе исполнения до кода)

По правилу arch/-first (изменение контракта — новый сервис и новые ключи
etcd). Файлы — в этом же worktree, репозиторий `pg`:

1. **Новый** `arch/14-pgworker.md` — дока PgWorker: роль и границы, контракт
   etcd (§4 этого spec'а: читаемое/пишемое/новые ключи `/pgworker/*`,
   финальное состояние кластера после provisioning), модель развертывания
   (plain/swarm, образ узлы, placement/порты), процессы (машины состояний,
   таблица `nodes/<n>/state`), надёжность (клэймы/takeover/идемпотентность,
   снапшоты), связь с панелью (её контракт 02 §9) и скриптами.
2. **Правка** `arch/11-bucket-sharding.md` — §2: указатель на 14-ю доку и
   префикс `/pgworker/` (координация воркеров, вне читаемых панелью);
   §4.5: абзац о декларативном provisioning (панель заявляет — PgWorker
   поднимает; скрипты остаются для ручного пути).
3. **Правка** `arch/README.md` — индекс: пункт 14.
4. **Создание** `arch/roadmap/README.md` (+ первые записи), т.к. папки
   roadmap в репо ещё нет, а отложенные задачи появились: плановый
   C#-переезд бакетов (move-порт P1–P8), per-cluster секреты и ротация,
   TLS к Docker API/SSH-туннели, Prometheus-метрики, слияние данных
   карантинного шарда, автоматизация add/remove-shard.
5. **Правка** `arch/02-topology.md` — не требуется (стендовая таблица
   подмен; PgWorker-хосты описываются в 14-й доке и appsettings).

Сами правки выполняются в фазе исполнения до кода; код ссылается на
arch/14 как на источник истины.

## 14. Принятые решения (автономный режим)

- **Д1. Финальное состояние кластера после provisioning: `config` БЕЗ поля
  `state`** (не `state="ACTIVE"`). Обоснование: контракт панели 02 §2.1 —
  «`state` пишется только панелью… отсутствует/иное = обычный
  инициализированный кластер»; значения `ACTIVE` в её enum'е нет (только
  NotInitialized/ToRemove/Active-как-отсутствие). Удаление поля унифицирует
  панель-созданные кластеры с `init-cluster.sh`-кластерами для всех
  читателей. Пишем txn-ом с compare `mod_revision` (не затереть чужое).
  Бакеты → ACTIVE снятием status-ключей (02 §9.1 явно: «переводит в ACTIVE
  (снятием status-ключа)»).
- **Д2. Координация: пер-кластерные lease-клэймы + глобальный lease-лидер
  для singleton-задач (снапшоты)**. Отказ от «одного глобального лидера на
  всё»: единственный лидер = простой всех кластеров на takeover и нулевое
  масштабирование; пер-кластерные клэймы дают естественное разделение N
  инстансов по кластерам и takeover per-cluster (≤ TTL 15 с + тик).
  Глобальный лидер оставлен только для снапшотов (P12) — регулярная
  singleton-работа. Механизм — etcd lease+txn (compare version==0), тот же
  примитив, что Patroni использует в DCS: без дополнительных зависимостей.
- **Д3. Docker: собственный тонкий клиент Docker Engine API** (HTTP поверх
  unix-socket/TCP, по прецеденту собственного etcd-gateway в AdminPanel),
  не Docker.DotNet и не CLI. Обоснование: Docker.DotNet давно не
  обновляется, swarm-часть не гарантируется; CLI = Process-спавн, парсинг
  текста, зависимость от бинаря в контейнере. Engine API стабилен и
  документирован; нужен узкий набор endpoint'ов (containers/services/
  nodes/tasks/volumes); свой клиент даёт структурные ошибки, тестируемость
  на фикстурах и нулевые новые зависимости.
- **Д4. Нода кластера = ОДИН контейнер/сервис из кастомного образа
  `pgworker-node`** (Spilo+doorman+haproxy+supervisord), а не sidecar'ы.
  Обоснование: swarm не поддерживает host-network и multi-container pod —
  три раздельных контейнера потребовали бы три сервиса с constraint'ами на
  одну машину и сеть между ними; единый образ унифицирует plain/swarm,
  решает localhost-топологию доки 11 §4 и упрощает volume/rebuild. Цена —
  сборка образа — принята (поставка).
- **Д5. Размещение и адреса: статическая таблица хостов (plain) / список
  swarm-нод + порт-аллокатор с закреплением в etcd**. Хосты — из конфига
  (`PgWorker:Docker:Hosts`) или `GET /nodes`; анти-аффинити вычисляет
  PgWorker (plain: выбор хостов; swarm: constraint+spread на вычисленную
  ноду). Порты всегда выделяются из диапазона конфига и закрепляются в
  `/pgworker/portalloc/<C>` (переживает всё), DSN строится по факту. Это
  закрывает «как узнать машины» и конфликты нескольких нод на одном хосте.
- **Д6. Эвакуация MVP = аварийный перевод владения с пустыми схемами**
  (без логической репликации). Обоснование: эвакуация инициируется только
  при полной недоступности шарда — копировать не с чего; полный C#-порт
  move-bucket (P1–P8) — большой отдельный механизм,needed только для
  плановых переездов живых шардов (они остаются за скриптами/roadmap).
  Guard'ы: порог недоступности, блокировка при незавершённых переездах,
  journal до манипуляций (P7-паттерн), снапшоты до/после (P12), карантин
  вернувшегося шарда (не удаляем данные, не пускаем «призраков» — P1-логика).
- **Д7. Секреты per-install из env PgWorker** (не per-cluster, не в etcd —
  P12/P17). Home-окружение: простота и переживаемость рестартов важнее
  гранулярности; per-cluster генерация/ротация — roadmap.
- **Д8. Etcd-клиент: копия подхода AdminPanel (HTTP JSON gateway, poll без
  watch) + расширение lease/txn-delete**. Единый транспорт с панелью и
  rolecheck-стендом; watch не нужен при тике 5 с (аргументация 02 §5);
  keepalive — единственное долгоживущее соединение (стрим), переживает
  обрывы автоматическим регрантом lease.
- **Д9. Ретраи и каркас — из Puzzle** (Result, attribute-DI, Polly
  jitter-retry, BackgroundService-циклы, IHealthCheckService): разрешённое
  копирование, проверенные паттерны; отказ от новых фреймворков.

## Дальше

Фаза plan: декомпозиция на задачи (проекты по слоям, arch/-правки первой
задачей, потом Etcd-клиент → Docker-клиент → процессы → e2e-стенд),
каждая — с тестами AAA.
