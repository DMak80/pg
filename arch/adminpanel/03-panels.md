# 03. Панели и REST API

Спецификация UI-панелей и HTTP-контракта. Всё read-only, кроме единственной
мутации `POST /api/clusters` (создание кластера, 02 §9): GET-эндпоинты и три
POST (login/logout — не мутируют инспектируемые системы; clusters — пишет
только ключи своего создания). JSON, camelCase, `ProblemDetails` для ошибок.
Все эндпоинты, кроме `login` и `healthz`, требуют cookie-сессию (401 без неё).

## 1. Список эндпоинтов

| Метод+путь | Назначение |
|---|---|
| `POST /api/auth/login` | тело `{username,password}` → 204+cookie \| 401 (rate-limit 5/мин) |
| `POST /api/auth/logout` | погасить сессию → 204 |
| `GET /api/auth/me` | `{username}` \| 401 |
| `GET /api/healthz` | живость самой панели (без auth): `{status:"ok"}` |
| `GET /api/overview` | дашборд: сводка etcd+кластеров+алертов, `snapshotAgeMs` |
| `GET /api/etcd/status` | endpoints, members, leader, alarms, reachable, версия |
| `GET /api/clusters` | список кластеров (сводный) |
| `POST /api/clusters` | создание кластера (единственная мутация, 02 §9): тело `CreateClusterRequestDto` → 201+`ClusterCreatedDto` \| 400 (валидация) \| 409 (имя занято) \| 503 (etcd/снапшот) |
| `GET /api/clusters/{cluster}` | детали: config, шарды, бакеты, heals (всё сразу; N ≤ тысяч — грид фильтруется на клиенте) |
| `GET /api/ha` | список HA-scope'ов (сводный) |
| `GET /api/ha/{scope}` | детали scope: leader, members+runtime, optime, raw config, request_* |
| `GET /api/alerts` | все алерты; query `?severity=critical|warning|info`, `?kind=` |

Дополнительно к квери-параметрам: `?owner=&state=` на `/api/clusters/{c}`
возвращают отфильтрованный `buckets` (удобно для детальной страницы; по
умолчанию — все). `state` принимает и `NOT_INITIALIZED` (02 §2.1).

### 1.1. Контракт `POST /api/clusters`

Тело `CreateClusterRequestDto` (валидация — 02 §9.3; все ограничения —
ProblemDetails 400 с деталями по полям):

```text
CreateClusterRequestDto: name, sharded (bool, опционально: отсутствует/null
                          = true — обратная совместимость), buckets, shards
                          (передаются ТОЛЬКО при sharded=true; при
                          sharded=false не требуются — сервер нормализует
                          в 1/1, 02 §9.3), replicas,
                          requestCpu (число ядер, десятичное),
                          requestMem (GiB, целое), requestDisk (GiB, целое)
```

Ответ 201 (кластер записан в etcd, состояние NOT_INITIALIZED; снапшот
подхватит на следующем тике):

```text
ClusterCreatedDto: name, dbname, sharded (bool), bucketsCount, shardsTotal,
                    replicas, requestCpu, requestMem, requestDisk (строки-каноны
                    02 §9.1), state:"NOT_INITIALIZED"
```

Нешардированная БД (`sharded=false`): в etcd пишется вырожденная структура
1 бакет × 1 шард (02 §9.1), ответ возвращает `bucketsCount=1`,
`shardsTotal=1`, `sharded=false`.

Отказы: 409 `Cluster already exists` (клэйм-txn не сошёлся — имя занято);
503 (нет снапшота/активного endpoint'а, etcd-ошибка записи). Компенсация
частичной записи — 02 §9.2.

## 2. DTO (ключевые поля)

```text
OverviewDto:  alertsCritical, alertsWarning, etcd{reachable, endpointsOk, endpointsTotal},
              clusters[{name, shards, buckets, activeMoves, masterlessShards,
              notInitialized(bool)}],
              activeMoves[{cluster,bucket,state,owner,target,updatedUnix}],
              snapshotAgeMs, stale(bool)
EtcdStatusDto: endpoints[{url, reachable, latencyMs, version, dbSizeBytes,
              leaderMemberId, raftTerm, errors[], active}], members[{id, name,
              peerUrls, clientUrls, isLeader}], alarms[{memberId, type}],
              quorumSuspected, lastRefreshUtc
ClusterDto:   name, dbname, bucketsCount, createdUnix, incomplete(bool),
              state(ACTIVE|NOT_INITIALIZED), sharded(bool), shards[ShardDto],
              buckets[BucketDto], heals[HealDto],
              standNodes[{name,address}] — стендовый топо-реестр снапшота
              (02 §2.3; поле глобально для всех кластеров, обычно пусто;
              UI-блок «Стендовая топология» рисуется при наличии)
ClusterSummaryDto: name, dbname, bucketsCount, incomplete(bool),
              notInitialized(bool), shardsTotal, shardsWithMaster, activeMoves
ShardDto:     name, dsn, hosts[], replicasDeclared, masterAddress,
              masterLeaseAlive(bool), nodes[{name, state}],
              requests{cpu, mem, disk}?(nullable) — заявка на ноду из
              HaScope `<C>-<X>` (02 §2.2 request_*), null у старых кластеров,
              runtime{standbiesSync, slotsLagMaxBytes,
              walStatusLost[], subscriptions[], bucketSchemas[], error}(nullable)
BucketDto:    id, owner, state(ACTIVE|SYNCING|FROZEN|ABORTING|NOT_INITIALIZED),
              move{owner,target,startedUnix,updatedUnix,phase,lastError}? ,
              ageSec (для не-ACTIVE)
HealDto:      bucket, was, now, reason, tsUnix
HaScopeDto:   scope, cluster?, shard?, matched(bool), leaderName, optimeLeader,
              members[{name, host, port, role, state, timeline, lagBytes,
              probeAtUtc, probeError}], rawConfig,
              requests{cpu, mem, disk}?(nullable) — заявка на ноду (02 §9.1)
AlertDto:     id, severity, kind, target, message, details{...}, sinceUnix
```

`sinceUnix` алерта: `AlertEngine` сравнивает с прошлым снапшотом по
стабильному `id` (`kind:target`) — «присутствует с»; живёт в снапшоте, без
хранения истории.

`sharded` в `ClusterDto` — вычисляемое поле отображения: `false` ⟺ ровно
1 бакет и не более 1 шарда (`bucketsCount==1 && shards ≤ 1`). Признак «тип
БД» в etcd не хранится (02 §9.1: нешардированная пишется вырожденной 1×1),
поэтому осознанно созданный шардированный кластер 1×1 отображается как
нешардированный — для UI различие несущественно (таблица из одного бакета
на единственном шарде не информативна). Единственный потребитель поля —
решение «показывать ли вкладку Бакеты» (§3).

`masterlessShards` кластера в NOT_INITIALIZED всегда 0: «без мастера» у ещё
не поднятого кластера — ожидаемое состояние, не деградация (кластер помечен
`notInitialized`, UI показывает серым).

`activeMoves` (сводка кластера и Overview) считает только
`SYNCING|FROZEN|ABORTING`: `NOT_INITIALIZED` — не переезд, а начальное
состояние бакета (02 §9).

## 3. Панели UI

| Панель | Что показывает |
|---|---|
| **Login** | форма логин/пароль; ошибка 401 |
| **Overview** | бейдж stale; карточки: etcd (reachable, endpoints ok/total; alarms — в ленте алертов и на панели etcd), кластеры (шарды/бакеты/переезды), активные переезды списком, лента алертов (critical/warning); сводка HA: скольки scope'ов без лидера (клиентская агрегация `GET /api/ha` — `OverviewDto` HA-полей не содержит) |
| **etcd** | таблица endpoints (reachable, latency, версия, raftTerm, ошибки, метка «активный»), members (+лидер), alarms; `lastRefreshUtc` |
| **Clusters** | список: имя, dbname, N, шард мастеровых/всего, активные переезды, пометки (incomplete, not-initialized); кнопка «Создать кластер» → модальная форма (§3.1) |
| **Cluster details** | вкладки: Шарды (dsn, replicas, master+leaseAlive, sync-standby, лаг слотов; ноды: имя+state; заявка ресурсов на ноду cpu/mem/disk), Бакеты (грид id×owner×state, фильтр по owner/state, подсветка не-ACTIVE, возраст; вкладка скрыта при `sharded=false` — нешардированная БД 1×1 без карты бакетов, 02 §9.1), Переезды (только не-ACTIVE, кроме NOT_INITIALIZED: phase, updated, last_error), Heals (журнал), «Стендовая топология» (блок по `standNodes` деталей — реестр `/cluster/nodes/`, скрыт при пустом) |
| **HA** | список scope'ов: scope, cluster/shard, лидер, члены (роль/состояние), лаг max, пометка unmatched |
| **HA details** | leader, optime, таблица members: name/role/state/timeline/lag/probe-статус; блок «Заявленные ресурсы нод» (request_*, при наличии); raw config (свернуто) |
| **Alerts** | таблица всех алертов: severity-цвет, kind, target, message, since; фильтр по severity |

### 3.1. Форма «Создать кластер» (единственная форма данных)

Модальный диалог (Mantine Modal + TextInput/NumberInput) с кнопки «Создать
кластер» на панели Clusters. Поля: имя; бакеты; шарды (≤ бакетов); реплики
(дефолт 2, минимум 1 — только мастер); группа «Ресурсы нод (заявка, на каждую
ноду)»: CPU (ядра, шаг 0.1), память (GiB), диск (GiB). Клиентская валидация —
зеркало 02 §9.3 (быстрая ошибка у поля); серверная — источник истины.
Отправка — POST `/api/clusters`; успех → закрыть форму, инвалидировать
`clusters`-запросы (список обновится, новый кластер — с бейджем
«не инициализирован»); ошибка — ProblemDetails в теле формы (409 — «имя
занято», 400 — по полям, 503 — «etcd недоступен»). Двойной клик защищён
блокировкой кнопки на время мутации. Никаких других форм ввода, кроме логина
и этой, — панель немая по отношению к данным.

Общие элементы: переключатель интервала polling (2/5/15 с/off, default 5 с,
выбор сохраняется в localStorage), тёмная тема, авто-logout при 401
(redirect на /login), stale-бейдж в шапке layout'а — по `snapshotAgeMs`/`stale`
ответа `/api/overview`, опрашиваемого с текущим polling-интервалом (при
недоступности данных — «нет данных»), счётчики critical/warning у пункта
«Алерты» в навигации (клиентский подсчёт по ответу `/api/alerts`, опрашиваемому
с тем же интервалом; скрыты при нуле/ошибке). Форм ввода две: логин и создание
кластера (§3.1) — всё остальное панель немая по отношению к данным.

## 4. Каталог алертов (`AlertEngine`)

Чистая функция `Snapshot → Alert[]`; severity: `critical` (прод горит),
`warning` (деградация/риск), `info` (заметка). Пороги — `AdminPanel:Alerts`.

| kind | severity | Условие | Источник |
|---|---|---|---|
| `etcd-unreachable` | critical | `consecutiveFailures ≥ 2` тиков | refresher |
| `etcd-no-quorum` | critical | raft-признаки отсутствия лидера / `status.errors` | `/v3/maintenance/status` |
| `etcd-endpoint-down` | warning | endpoint из настроек недоступен | status по endpoints |
| `etcd-alarm` | critical | есть alarms (NOSPACE и др.) | `/v3/maintenance/alarm` |
| `snapshot-stale` | warning | `BuiltAtUtc` старше `3×RefreshInterval` | refresher |
| `shard-no-master` | critical | `dsn` есть, `master`-ключа нет (P11: протухший lease) | `/clusters/…/master` |
| `shard-no-leader` | critical | HA-scope без `leader`-ключа, **кроме scope'ов кластера в NOT_INITIALIZED** (ноды ещё не подняты — 02 §9) | `/service/…/leader` |
| `cluster-not-initialized` | info | кластер в `NOT_INITIALIZED` (заявлен, ноды не подняты) — заметка, пока provisioning не переведёт в ACTIVE | config.state |
| `move-stale` | warning | status-ключ не-ACTIVE (кроме NOT_INITIALIZED) дольше `StaleMoveSeconds` (600 c) | `…/buckets/status/*` |
| `move-frozen-long` | critical | `FROZEN` дольше `FrozenSeconds` (60 c) — cutover обязан быть секундами | `…/buckets/status/*` |
| `move-aborting` | warning | `ABORTING` (незавершённая уборка, P7) | `…/buckets/status/*` |
| `move-flipped-status-stuck` | warning | status есть, routing уже = target (P7) | routing+status |
| `bucket-lost` | critical | routing → несуществующий шард (P23-а) | routing × shards |
| `bucket-no-routing` | warning | бакет из `0..N-1` без routing-ключа («дыра» карты) | routing × config |
| `bucket-out-of-range` | warning | routing-ключ с `N ≥ buckets` (P18) | routing × config |
| `cluster-incomplete` | warning | префикс `/clusters/<C>` без `config` | парсер |
| `key-malformed` | warning | ключ не разобран | парсер |
| `ha-member-not-streaming` | warning | Patroni-проба: member не `running/streaming` | Patroni REST |
| `replica-lag-high` | warning | лаг реплики > `ReplicaLagBytes` (16 МБ) | Patroni REST |
| `slot-lag-high` / `slot-wal-lost` | warning / critical | лаг слота > порога / `wal_status='lost'` (P4) | SQL-проба |
| `slot-invalidation-risk` | warning | `safe_wal_size` < порога (P4, ДО среза) | SQL-проба |
| `sync-standby-missing` | warning | у мастера нет `sync_state IN ('sync','quorum')` (P8 — предусловие переездов) | SQL-проба |
| `inventory-mismatch` | warning | фактические схемы `bucket_%` ≠ routing (P21/P23) | SQL-проба |
| `probe-failed` | info | Patroni/SQL-проба ошибки (детали в probe) | пробы |

SQL-алерты вычисляются только при включённых пробах; etcd-алерты — всегда.
`NOT_INITIALIZED`-бакеты — не переезды: `move-*` правила их не алертят
(`move-frozen-long`/`move-aborting` смотрят свои точные состояния,
`move-flipped-status-stuck` — требует `target`, у NOT_INITIALIZED его нет);
бейдж «не инициализирован» в UI + `cluster-not-initialized` (info) вместо
critical-шума от ещё не поднятого кластера.

## 5. SQL-каталог пробы (read-only, только `pg_catalog`/`pg_stat_*`)

Выполняются на мастере каждого шарда (DSN из etcd + пароль панели;
`default_transaction_read_only=on`):

```sql
-- sync-standby и лаги физических реплик (P8)
select application_name, client_addr, state, sync_state, pg_wal_lsn_diff(
         pg_current_wal_lsn(), replay_lsn) as lag_bytes
from pg_stat_replication;

-- слоты переездов: лаг/риск среза (P4)
select slot_name, slot_type, active, wal_status, safe_wal_size, confirmed_flush_lsn,
       pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) as lag_bytes
from pg_replication_slots;

-- прогресс подписок (переезды)
select subname, received_lsn, latest_end_lsn, latest_end_time
from pg_stat_subscription;

-- инвентарь бакетов (сверка с routing, P21/P23)
select nspname from pg_namespace where nspname like 'bucket\_%' escape '\';

-- роль ноды (мастер или реплика)
select pg_is_in_recovery();
```

Образцы и тонкости (например, `like`-экранирование `_`) — из
`../pg/arch/scripts/buckets-common.sh`; запросы не меняются в SQL-семантике
без правки этого документа.

## 6. Версионирование контракта

Контракт API не версонируется (панель и API развёртываются одним артефактом,
фронт и бэк всегда согласованы). Изменение DTO — правкой этого документа
тем же PR, что и код.
