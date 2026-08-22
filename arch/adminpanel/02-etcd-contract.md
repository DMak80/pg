# 02. Контракт чтения etcd

AdminPanel читает etcd **только**. Источник схемы — инспектируемая система
(`../pg`: `arch/11-bucket-sharding.md` §2, `arch/12-bucket-pitfalls.md` §3,
скрипты `arch/scripts/`). Панель не пишет ни один ключ (кроме собственной
cookie — это не etcd).

## 1. Транспорт: HTTP JSON gateway `/v3/*`

- Клиент — `HttpClient` против **gRPC-gateway etcd** (JSON+base64), не gRPC.
  Обоснование: (а) это стабильный API etcd 3.5 (`/v3/*`; включён по умолчанию
  при запуске флагами — наш стенд и прод `../pg` так и стартуют); (б) тот же
  транспорт уже использует сама система (`arch/stand/sidecar/rolecheck.py`:
  `/v3/kv/range`, `/v3/lease/*`) — т.е. паттерн проверен в инспектируемом
  окружении; (в) не тащим gRPC-стек и заброшенные .NET-клиенты etcd.
- Формат: `POST /v3/kv/range` с телом `{"key":"<b64>","range_end":"<b64>"}`
  (`range_end` = префикс с инкрементированным последним байтом); ответ
  `{"kvs":[{"key","value","mod_revision"}...]}`. Отдельные вызовы:
  `POST /v3/cluster/member/list`, `POST /v3/maintenance/alarm`,
  `POST /v3/maintenance/status` (последний — на **каждый** endpoint из
  настроек, персонально).
- Ограничение: gateway не отдаёт lease-ID у произвольного range-ответа,
  поэтому «живость» master-ключа определяется **семантикой ключа** (lease
  TTL 5 c: ключ есть = lease жив; ключа нет = протух/нет писателя), а не
  запросом lease. Watch (`/v3/watch`) не используем: refresher-тик покрывает
  потребности read-only панели (см. §5).

## 2. Читаемые ключи и префиксы

Три источника: контроль-плейн шардинга `/clusters/`, Patroni DCS `/service/`,
стендовый топо-реестр `/cluster/nodes/` (показываем как «стендовые данные»
отдельным блоком, если присутствуют).

### 2.1. `/clusters/<C>/…` — контроль-плейн шардинга

| Ключ | Формат значения | В модель | Примечания |
|---|---|---|---|
| `/clusters/<C>/config` | JSON `{"buckets":N,"dbname":"<C>","created_unix":…}` | `ClusterInfo` (константы) | N — константа навсегда (P18); `created_unix` может отсутствовать (старые init) |
| `/clusters/<C>/shards/<X>/dsn` | libpq-строка `host=n1,n2,n3 port=5432 dbname=<C> user=bucket_admin` | `ShardInfo.Dsn`, парсим хосты/порт/dbname/user | пароля нет (секреты в env); пароль для SQL-пробы приходит из настроек панели |
| `/clusters/<C>/shards/<X>/replicas` | целое-строка `"2"` | `ShardInfo.ReplicasDeclared` | декларативное намерение; факт — в HA (`/service/`) |
| `/clusters/<C>/shards/<X>/master` | `"host:6432"` | `ShardInfo.MasterAddress` (nullable) | lease TTL 5 c; отсутствие = нет живого мастера (P11) |
| `/clusters/<C>/buckets/routing/bucket_<N>` | имя шарда `"shard1"` | `BucketInfo.Owner` | единственный авторитет «где бакет»; отсутствие при N из config = «дыра» в карте |
| `/clusters/<C>/buckets/status/bucket_<N>` | JSON `{"bucket","state":"SYNCING"\|"FROZEN"\|"ABORTING","owner","target","started_unix","updated_unix","phase","last_error"?}` | `BucketInfo.State=ACTIVE`-иначе, `MoveInfo` | отсутствие ключа = ACTIVE; ключ удаляется атомарно с flip |
| `/clusters/<C>/heals/<bucket>` | JSON `{"bucket","was","now","reason","ts"}` | `HealRecord[]` | журнал авто-починки (restore-heal) |

Неизвестные ключи внутри `/clusters/` не ошибка: лог-строка + счётчик
`unknownKeys` в снапшоте (система развивается, панель не должна падать).

### 2.2. `/service/<scope>/…` — Patroni DCS (HA)

Scope = `<C>-<X>`, глобально уникален. Связь со шардом — эвристикой
`scope.startsWith("<C>" + "-")` по известным кластерам (имена шардов уникальны
внутри `<C>`, `<C>` знаем из `/clusters/`), плюс человекочитаемая проверка:
если suffix не совпал ни с одним именем шарда — показываем scope «как есть» с
пометкой `unmatched`.

| Ключ | Формат | В модель |
|---|---|---|
| `/service/<scope>/leader` | JSON `{"name":"pg1"}` (у Patroni; на стенде возможна строка-имя) | `HaScope.LeaderName` (nullable; нет = нет лидера) |
| `/service/<scope>/members/<name>` | JSON Patroni `Member` (`{"name","conn_url":"host:port","role":"replica"\|"master",...})` — парсим толерантно: `name`, хост/порт из `conn_url`, `role`, `state` если есть | `HaMember[]` |
| `/service/<scope>/config` | JSON (любой) | показываем как raw-JSON (детали страницы HA) |
| `/service/<scope>/optime/leader` | число-строка (LSN) | `HaScope.OptimeLeader` — позиция репликации лидера |
| `/service/<scope>/initialize` | строка (system_id) | `HaScope.Initialized` |

### 2.3. `/cluster/nodes/<node>` — стендовый топо-реестр

Только стенд `arch/stand`: IP ноды с lease TTL. Панель показывает блок
«Стендовая топология» (node → IP, наличие ключа) — полезно при разработке
против pg-стенда. В проде префикса нет — блок скрыт.

### 2.4. Кластерные метаданные etcd (не KV)

| Вызов gateway | На | В модель |
|---|---|---|
| `POST /v3/maintenance/status` | каждый endpoint из `AdminPanel:Etcd:Endpoints` | `EtcdEndpoint.Status`: reachable, latencyMs, version, dbSize, leader(memberId), raftIndex, raftTerm, errors[] |
| `POST /v3/cluster/member/list` | активный endpoint | `EtcdMember[]`: id, name, peerUrls, clientUrls; `IsLeader` по совпадению id со статусом leader |
| `POST /v3/maintenance/alarm` | активный endpoint | `Alarm[]`: memberId, type (NOSPACE, CORRUPT, NOSPACE-потомки) |

## 3. Модель снапшота (С#-типы, проект `Core`)

Immutable records; enum'ы — английские идентификаторы.

```csharp
sealed record EtcdSnapshot(
    DateTimeOffset BuiltAtUtc,
    EtcdStatus Etcd,                       // §2.4 + lastRefresh/failures
    IReadOnlyList<ClusterInfo> Clusters,   // §2.1
    IReadOnlyList<HaScope> HaScopes,       // §2.2 (+ Patroni-обогащение §6)
    IReadOnlyList<StandNode> StandNodes,   // §2.3, обычно пусто
    IReadOnlyList<ProbeResult> Probes,     // результаты live-проб §6
    IReadOnlyList<Alert> Alerts,           // вычислено AlertEngine (03-panels §4)
    int UnknownKeyCount);                  // диагностика «неизвестных» ключей

sealed record ClusterInfo(
    string Name, string DbName, int BucketsCount, long? CreatedUnix,
    IReadOnlyList<ShardInfo> Shards,
    IReadOnlyList<BucketInfo> Buckets,     // все N, включая ACTIVE
    IReadOnlyList<HealRecord> Heals);

sealed record ShardInfo(
    string Name, string Dsn, IReadOnlyList<string> DsnHosts, int? Port,
    string? DbName, string? User, int? ReplicasDeclared,
    string? MasterAddress,                 // null => нет lease-мастера
    ShardRuntime? Runtime);                // из SQL-пробы, nullable

sealed record BucketInfo(
    int Id, string? Owner,                 // owner null => нет routing-ключа
    BucketState State,                     // Active/Syncing/Frozen/Aborting
    MoveInfo? Move);                       // owner/target/started/updated/phase/lastError

sealed record HaScope(
    string Scope, string? Cluster, string? Shard, bool Matched,
    string? LeaderName, string? OptimeLeader, bool Initialized,
    IReadOnlyList<HaMember> Members, string? RawConfig);

sealed record HaMember(
    string Name, string Host, int? Port, string? Role, string? State,
    long? Timeline, long? LagBytes,        // из Patroni-пробы, если включена
    DateTimeOffset? ProbeAtUtc, string? ProbeError);

sealed record ShardRuntime(
    string Shard, IReadOnlyList<ReplicationSlotInfo> Slots,
    IReadOnlyList<StandbyInfo> Standbies,  // pg_stat_replication (sync_state!)
    IReadOnlyList<SubscriptionInfo> Subscriptions,
    IReadOnlyList<string> BucketSchemas,   // инвентарь bucket_% (для сверки)
    bool? IsInRecovery, ProbeError? Error);
```

## 4. Стратегия poll (SnapshotRefresher)

- Тик `RefreshInterval` (по умолчанию **3 с**): покрывает динамику master-lease
  (TTL 5 c) с запасом; полный объём KV контроль-плейна — сотни ключей, один
  range на префикс, нагрузка пренебрежима.
- Порядок тика:
  1. Параллельно: `status` по **всем** endpoints (timeout `RequestTimeout`);
     «активный» = первый живой (sticky с прошлого тика, при отказе —
     следующий живой).
  2. На активном: два range (`/clusters/`, `/service/`) + `member/list` +
     `alarm`. Плюс range `/cluster/nodes/` (терпим к отсутствию — но это
     лишний запрос в проде; проверяем `keys_only` один раз и кешируем
     «префикс существует» — упрощение: просто шлём range, пустой ответ
     дешев).
  3. Парсеры (чистые функции `IReadOnlyList<Kv> → модель`) → сборка нового
     `EtcdSnapshot` (пробы вносятся из их последнего результата).
  4. `AlertEngine(snapshot)` → `Alerts`; атомарная замена в `SnapshotStore`.
- Отдельный тик `Probes.Interval` (по умолчанию **15 с**) — Patroni REST и SQL
  (§6); результат обогащает следующий снапшот (пробы не блокируют тик KV).
- Ретраи: нет ретраев внутри тика — следующий тик и есть ретрай;
  `consecutiveFailures` кормит алерт «etcd unreachable» (порог 2).

## 5. Почему poll, а не watch

Watch (`/v3/watch` стримом) даёт мгновенность, но: (а) read-only панели с
refresh 3 с не нуждается в миллисекундах — протухание lease (5 c) и так
видео-частота; (б) watch-стрим по HTTP gateway — chunked-стрим, который нужно
держать, реваншировать и парсить — лишнее состояние ради экономии одного
range-запроса раз в 3 с; (в) инспектируемые данные малы (≤ нескольких тысяч
ключей). Отказ от watch — осознанное упрощение; пересмотр — только если
появится доказанная потребность (roadmap, не текущая версия).

## 6. Live-пробы (опциональные, `AdminPanel.Probes`)

Пробы не являются источником топологии — только обогащение. Каждая включается
отдельно (`PatroniEnabled` / `SqlEnabled`), ошибка пробы не роняет данные из
etcd.

### 6.1. Patroni REST `:8008` (по умолчанию включена)

- Для каждого HA-scope и каждого его member (host из `/service/…/members/`):
  `GET http://<host>:8008/cluster` (timeout 3 c) → JSON Patroni:
  `members[]{name,role,state,timeline,lag,host,port}`.
- Даёт: фактическое состояние нод (`running`/`streaming`/`stopped`), лаг
  реплик в байтах, timeline — то, чего в etcd-DCS нет «в реальном времени».
- Порт `:8008` и путь — стандарт Patroni (`../pg` `arch/01-architecture.md`,
  HAProxy health-check использует те же).

### 6.2. SQL через Npgsql (по умолчанию включена; в проде — на усмотрение)

- Подключение: DSN шарда из etcd **+ Password из настроек панели**
  (`AdminPanel:Probes:Password`; в DSN пароля нет никогда) +
  `TargetSessionAttributes=ReadWrite` (multi-host DSN ведёт на мастер),
  `Application Name=adminpanel`, `statement_timeout`.
- Только `SELECT` к `pg_catalog`/`pg_stat_*` (образцы запросов —
  `../pg/arch/scripts/buckets-common.sh`, `move-bucket.sh`; список в
  [03-panels.md](03-panels.md) §5). Двойная защита от записи: сама панель
  не генерирует DML, дополнительно connection string содержит
  `Options=-c default_transaction_read_only=on`.
- Даёт: `pg_stat_replication` (sync-standby жив? P8), `pg_replication_slots`
  (лаг слотов, `wal_status`, `safe_wal_size` — P4), `pg_stat_subscription`
  (прогресс переездов), инвентарь схем `bucket_%` (сверка с routing —
  детекция «тихих» расхождений P21/P23).

## 7. Обработка сбоев и вырожденные случаи

| Случай | Поведение |
|---|---|
| Все endpoints недоступны | снапшот прежний, `Etcd.Reachable=false`, алерт critical; UI-бейдж «данные от <t>» |
| Активный endpoint умер между тиками | failover на следующий живой по списку, без потери тика |
| Кворума нет (raft без лидера) | `status.errors`/raft-признаки → алерт critical «no quorum»; KV может отдаваться stale — панель маркирует `Etcd.QuorumSuspected=true` |
| Битый JSON в значении ключа | ключ пропускается, в снапшот попадает `ParseError`-запись (видна в UI-details), алерт warning «malformed key» |
| `/clusters/<C>` без `config` | кластер показывается с пометкой `incomplete` + warning-алерт |
| routing → несуществующий шард | critical-алерт «lost bucket» (P23-а) |
| status-ключ есть, routing уже = target | предупреждение «flip прошёл, статус завис» (P7-детект, `move-bucket.sh status` так же делает) |
| `bucket_<N>` вне `0..N-1` | warning «bucket out of range» (P18) |
| scope не сопоставился кластеру | отображается с пометкой `unmatched`, не алерт (чужой service в общем etcd — норма) |

## 8. Контракт тестирования парсеров

Unit-тесты парсеров прогоняют **реальные фрагменты** значений, взятые из
`../pg` (скрипты `init-cluster.sh`, `move-bucket.sh`, Patroni JSON) — включая
вырожденные: отсутствующие поля, строковые числа, пустой префикс, неизвестный
ключ. Фикстуры хранятся в `tests/AdminPanel.UnitTests/EtcdFixtures/` как
.json-файлы. Интеграционные тесты поднимают etcd (Testcontainers,
`quay.io/coreos/etcd:v3.5.21`), сеют ключи тем же сидом, что и dev-стенд
([04-local-stand.md](04-local-stand.md)), и проверяют refresher end-to-end.
