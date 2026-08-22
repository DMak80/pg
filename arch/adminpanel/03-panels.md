# 03. Панели и REST API

Спецификация UI-панелей и HTTP-контракта. Всё read-only: GET-эндпоинты и два
POST (login/logout — не мутируют инспектируемые системы). JSON, camelCase,
`ProblemDetails` для ошибок. Все эндпоинты, кроме `login` и `healthz`,
требуют cookie-сессию (401 без неё).

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
| `GET /api/clusters/{cluster}` | детали: config, шарды, бакеты, heals (всё сразу; N ≤ тысяч — грид фильтруется на клиенте) |
| `GET /api/ha` | список HA-scope'ов (сводный) |
| `GET /api/ha/{scope}` | детали scope: leader, members+runtime, optime, raw config |
| `GET /api/alerts` | все алерты; query `?severity=critical|warning|info`, `?kind=` |

Дополнительно к квери-параметрам: `?owner=&state=` на `/api/clusters/{c}`
возвращают отфильтрованный `buckets` (удобно для детальной страницы; по
умолчанию — все).

## 2. DTO (ключевые поля)

```text
OverviewDto:  alertsCritical, alertsWarning, etcd{reachable, endpointsOk, endpointsTotal},
              clusters[{name, shards, buckets, activeMoves, masterlessShards}],
              activeMoves[{cluster,bucket,state,owner,target,updatedUnix}],
              snapshotAgeMs, stale(bool)
EtcdStatusDto: endpoints[{url, reachable, latencyMs, version, dbSizeBytes,
              leaderMemberId, raftTerm, errors[], active}], members[{id, name,
              peerUrls, clientUrls, isLeader}], alarms[{memberId, type}],
              quorumSuspected, lastRefreshUtc
ClusterDto:   name, dbname, bucketsCount, createdUnix, incomplete(bool),
              shards[ShardDto], buckets[BucketDto], heals[HealDto],
              standNodes[{name,address}] — стендовый топо-реестр снапшота
              (02 §2.3; поле глобально для всех кластеров, обычно пусто;
              UI-блок «Стендовая топология» рисуется при наличии)
ShardDto:     name, dsn, hosts[], replicasDeclared, masterAddress,
              masterLeaseAlive(bool), runtime{standbiesSync, slotsLagMaxBytes,
              walStatusLost[], subscriptions[], bucketSchemas[], error}(nullable)
BucketDto:    id, owner, state(ACTIVE|SYNCING|FROZEN|ABORTING),
              move{owner,target,startedUnix,updatedUnix,phase,lastError}? ,
              ageSec (для не-ACTIVE)
HealDto:      bucket, was, now, reason, tsUnix
HaScopeDto:   scope, cluster?, shard?, matched(bool), leaderName, optimeLeader,
              members[{name, host, port, role, state, timeline, lagBytes,
              probeAtUtc, probeError}], rawConfig
AlertDto:     id, severity, kind, target, message, details{...}, sinceUnix
```

`sinceUnix` алерта: `AlertEngine` сравнивает с прошлым снапшотом по
стабильному `id` (`kind:target`) — «присутствует с»; живёт в снапшоте, без
хранения истории.

## 3. Панели UI

| Панель | Что показывает |
|---|---|
| **Login** | форма логин/пароль; ошибка 401 |
| **Overview** | бейдж stale; карточки: etcd (reachable, endpoints ok/total; alarms — в ленте алертов и на панели etcd), кластеры (шарды/бакеты/переезды), активные переезды списком, лента алертов (critical/warning); сводка HA: скольки scope'ов без лидера (клиентская агрегация `GET /api/ha` — `OverviewDto` HA-полей не содержит) |
| **etcd** | таблица endpoints (reachable, latency, версия, raftTerm, ошибки, метка «активный»), members (+лидер), alarms; `lastRefreshUtc` |
| **Clusters** | список: имя, dbname, N, шард мастеровых/всего, активные переезды, пометки (incomplete) |
| **Cluster details** | вкладки: Шарды (dsn, replicas, master+leaseAlive, sync-standby, лаг слотов), Бакеты (грид id×owner×state, фильтр по owner/state, подсветка не-ACTIVE, возраст), Переезды (только не-ACTIVE: phase, updated, last_error), Heals (журнал), «Стендовая топология» (блок по `standNodes` деталей — реестр `/cluster/nodes/`, скрыт при пустом) |
| **HA** | список scope'ов: scope, cluster/shard, лидер, члены (роль/состояние), лаг max, пометка unmatched |
| **HA details** | leader, optime, таблица members: name/role/state/timeline/lag/probe-статус; raw config (свернуто) |
| **Alerts** | таблица всех алертов: severity-цвет, kind, target, message, since; фильтр по severity |

Общие элементы: переключатель интервала polling (2/5/15 с/off, default 5 с,
выбор сохраняется в localStorage), тёмная тема, авто-logout при 401
(redirect на /login), stale-бейдж в шапке layout'а — по `snapshotAgeMs`/`stale`
ответа `/api/overview`, опрашиваемого с текущим polling-интервалом (при
недоступности данных — «нет данных»), счётчики critical/warning у пункта
«Алерты» в навигации (клиентский подсчёт по ответу `/api/alerts`, опрашиваемому
с тем же интервалом; скрыты при нуле/ошибке). Никаких форм ввода, кроме
логина — панель немая по отношению к данным.

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
| `shard-no-leader` | critical | HA-scope без `leader`-ключа | `/service/…/leader` |
| `move-stale` | warning | status-ключ не-ACTIVE дольше `StaleMoveSeconds` (600 c) | `…/buckets/status/*` |
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
