# 02. Контракт etcd (чтение панели + мутации через API воркеров)

AdminPanel читает etcd для инспекции. Источник схемы — инспектируемая система
(pg (этот монорепозиторий): `arch/11-bucket-sharding.md` §2, `arch/12-bucket-pitfalls.md` §3,
скрипты `arch/scripts/`). **Панель в etcd не пишет ничего**: все мутации
декларативного контракта отправляются в **HTTP API исполнителя** —
PgWorker (arch/14 §1.1) для pg-домена и KafkaWorker (arch/16 §1.1) для
kafka-домена — воркер сам валидирует и записывает ключи (протоколы записи
ниже — канон поведения ИСПОЛНИТЕЛЯ; `claim-txn`/`PUT`/компенсации выполняет
воркер, панель лишь передаёт команду). URL API каждого воркера панель
берёт из etcd — lease-ключи дискавери `/pgworker/api/<id>` и
`/kafkaworker/api/<id>` (§2.3.2), которые воркеры ставят сами. Прежняя
модель «панель пишет декларации напрямую в etcd» упразднена (ответственность
изменений — у воркеров).

Мутации pg-домена (исполняет PgWorker API, arch/14 §1.1): создание кластера
(§9), перевод кластера в TO_REMOVE (§9.4), добавление шарда (§9.5), маркер
демонтажа шарда (§9.6), заявки на переезды бакетов — move/rollback/finalize/abort
и отмена стоящих заявок (§9.7), заявка ротации app-пароля (§9.8). Существует
также мутация пересоздания ноды —
`POST /api/ha/{scope}/nodes/{node}/recreate` ставит маркеры
`nodes/<n>/state=TO_RECREATE` + `nodes/<n>/recreate=soft|hard` (исполняет
NodeSupervisor PgWorker); она зафиксирована кодом ранее и контрактно ведёт
себя как §9.6-подобный маркер. Отдельный домен **Kafka** (§10): чтение
`/kafka/clusters/` + `/kafkaworker/{rotations,admin_rotations,rebalances,reassignments}/` и
14 мутаций декларативной модели (исполняет KafkaWorker API, arch/16 §1.1) + мутация №15 ресурсов брокера (t06) + мутация №16 ротации admin-пароля (t03).

## 1. Транспорт: HTTP JSON gateway `/v3/*`

- Клиент — `HttpClient` против **gRPC-gateway etcd** (JSON+base64), не gRPC.
  Обоснование: (а) это стабильный API etcd 3.5 (`/v3/*`; включён по умолчанию
  при запуске флагами — наш стенд и прод pg (этот монорепозиторий) так и стартуют); (б) тот же
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

Четыре источника: контроль-плейн шардинга `/clusters/`, Patroni DCS `/service/`,
стендовый топо-реестр `/cluster/nodes/` (показываем как «стендовые данные»
отдельным блоком, если присутствуют), координация PgWorker `/pgworker/`
(избирательно — §2.3.1).

### 2.1. `/clusters/<C>/…` — контроль-плейн шардинга

| Ключ | Формат значения | В модель | Примечания |
|---|---|---|---|
| `/clusters/<C>/config` | JSON `{"buckets":N,"dbname":"<C>","created_unix":…,"state"?:"NOT_INITIALIZED"\|"TO_REMOVE"}` | `ClusterInfo` (константы) | N — константа навсегда (P18); `created_unix` может отсутствовать (старые init); `state` пишется только панелью: при создании (§9) и переводе в удаление (§9.4); отсутствует/иное = обычный инициализированный кластер |
| `/clusters/<C>/shards/<X>/dsn` | libpq-строка `host=n1,n2,n3 port=5432 dbname=<C> user=bucket_admin` | `ShardInfo.Dsn`, парсим хосты/порт/dbname/user | dsn PgWorker-кластеров несёт `password=` (per-cluster bucket_admin); панель разбирает его в `ShardInfo.Password`, SQL-проба использует `shard.Password ?? AdminPanel:Probes:Password`; у создаваемого панелью кластера ключа нет — ноды ещё не подняты (§9) |
| `/clusters/<C>/shards/<X>/replicas` | целое-строка `"2"` | `ShardInfo.ReplicasDeclared` | декларативное намерение; факт — в HA (`/service/`) |
| `/clusters/<C>/shards/<X>/master` | `"host:6432"` | `ShardInfo.MasterAddress` (nullable) | lease TTL 5 c; отсутствие = нет живого мастера (P11) |
| `/clusters/<C>/shards/<X>/nodes/<n>/state` | строка `"NOT_INITIALIZED"` | `NodeInfo.State` | плановые ноды создаваемого панелью кластера (§9); имена `<X><буква>` (`s1a`,`s1b`… — конвенция стенда pg (этот монорепозиторий)); нода поднята/инициализирована — ключ меняет будущий provisioning |
| `/clusters/<C>/shards/<X>/state` | строка `"TO_REMOVE"` | `ShardInfo.State` | маркер демонтажа шарда (§9.6): пишет ТОЛЬКО панель (one-way, обратного перехода нет); отсутствие = обычный шард (ACTIVE); читают панель (бейдж «к удалению») и PgWorker (RemoveShardProcess, t06) |
| `/clusters/<C>/buckets/routing/bucket_<N>` | имя шарда `"shard1"` | `BucketInfo.Owner` | единственный авторитет «где бакет»; отсутствие при N из config = «дыра» в карте |
| `/clusters/<C>/buckets/status/bucket_<N>` | JSON `{"bucket","state":"SYNCING"\|"FROZEN"\|"ABORTING"\|"NOT_INITIALIZED","owner","target"?,"started_unix"?,"updated_unix","phase"?,"last_error"?}` | `BucketInfo.State=ACTIVE`-иначе, `MoveInfo` | отсутствие ключа = ACTIVE; ключ удаляется атомарно с flip; `NOT_INITIALIZED` — начальное состояние бакетов создаваемого кластера (§9), `target/started_unix/phase` при нём отсутствуют |
| `/clusters/<C>/heals/<bucket>` | JSON `{"bucket","was","now","reason","ts"}` | `HealRecord[]` | журнал авто-починки (restore-heal) |

Неизвестные ключи внутри `/clusters/` не ошибка: лог-строка + счётчик
`unknownKeys` в снапшоте (система развивается, панель не должна падать).

Ожидаемые игнорируемые ключи: `/clusters/<C>/app_user` и
`/clusters/<C>/app_password` — per-cluster креды приложения (генерирует
PgWorker), а также `/clusters/<C>/shards/<X>/nodes/<n>/app_params` —
per-node серверные параметры подключения (libpq-строка, `sslmode=require`;
ведёт PgWorker, [11](../11-bucket-sharding.md) §2). Панель их НЕ читает и
НЕ отображает: парсер пропускает без `unknownKeys`-счётчика, значение не
попадает в модель/UI/API.

### 2.2. `/service/<scope>/…` — Patroni DCS (HA)

Scope = `<C>-<X>`, глобально уникален. Связь со шардом — эвристикой
`scope.startsWith("<C>" + "-")` по известным кластерам (имена шардов уникальны
внутри `<C>`, `<C>` знаем из `/clusters/`), плюс человекочитаемая проверка:
если suffix не совпал ни с одним именем шарда — показываем scope «как есть» с
пометкой `unmatched`.

| Ключ | Формат | В модель |
|---|---|---|
| `/service/<scope>/leader` | JSON `{"name":"pg1"}` (у Patroni; на стенде возможна строка-имя) | `HaScope.LeaderName` (nullable; нет = нет лидера) |
| `/service/<scope>/members/<name>` | JSON Patroni `Member` (`{"name","conn_url":"host:port","role":"replica"\|"master",...}`) — парсим толерантно: `name`, хост/порт из `conn_url`, `role`, `state` если есть | `HaMember[]` |
| `/service/<scope>/config` | JSON (любой) | показываем как raw-JSON (детали страницы HA) |
| `/service/<scope>/optime/leader` | число-строка (LSN) | `HaScope.OptimeLeader` — позиция репликации лидера |
| `/service/<scope>/initialize` | строка (system_id) | `HaScope.Initialized` |
| `/service/<scope>/request_cpu` | число-ядра строкой `"2"` / `"0.5"` (invariant) | `HaScope.RequestCpu` — заявка CPU **на каждую ноду** scope (§9) |
| `/service/<scope>/request_mem` | `"<целое>Gi"` `"8Gi"` | `HaScope.RequestMem` — заявка памяти на ноду (§9) |
| `/service/<scope>/request_disk` | `"<целое>Gi"` `"100Gi"` | `HaScope.RequestDisk` — заявка диска на ноду (§9) |

### 2.3. `/cluster/nodes/<node>` — стендовый топо-реестр

Только стенд `arch/stand`: IP ноды с lease TTL. Панель показывает блок
«Стендовая топология» (node → IP, наличие ключа) — полезно при разработке
против pg-стенда. В проде префикса нет — блок скрыт.

### 2.3.1. `/pgworker/…` — координация воркеров (читается избирательно)

Префикс координации PgWorker (`arch/14` §3.3) панель читает точечно —
четыре ключа-семейства, остальные ключи префикса (`leader`, `claims`,
`evacuations`, `instances`) панель НЕ читает и не пишет:

| Ключ | Формат значения | В модель | Примечания |
|---|---|---|---|
| `/pgworker/portalloc/<C>` | JSON `{"<shard>/<node>":{"host":"h1","pg":15432,"patroni":18008,"doorman":16432}}` | адреса Patroni-проб (`arch/14` §2.4) | канонический `host:patroni-порт` члена HA (источник — DSN шарда); в UI не отображается |
| `/pgworker/work/<C>` | JSON `{"op":"provision\|…","phase":"…","updated_unix":…,"instance":"…","last_error"?,"fail_count"?,"fail_first_unix"?,"retry_not_before_unix"?,"unreachable"?}` (канон — arch/14 §3.3) | `WorkJournalInfo` (§3) | журнал фаз процесса воркера; `last_error` + `fail_first_unix` кормят алерт `provision-stuck` (03 §4) — панель видит, ЧТО именно фейлится у неинициализирующегося кластера; битый JSON — parseError-запись, ключ не трогаем (домен воркера); в UI отображается через алерты |
| `/pgworker/moves/<C>/<bucket>` | JSON-заявка `{"op":"move"\|"rollback"\|"finalize"\|"abort","to"?,"old_shard"?,"skip_reverse"?,"resume"?,"force"?,"requested_unix":<unix>,"requested_by"?}` | `MoveTicket` (§3) | очередь заявок на переезды: панель читает (вкладка «Переезды»); **пишет PgWorker** по команде мутации §9.7 (пришла через API воркера); после успеха/перманентного отказа заявку УДАЛЯЕТ PgWorker — исчезновение из очереди без изменения routing/status = отвергнутая заявка |
| `/pgworker/api/<id>` | lease TTL 15 c, JSON `{"url":"https://<host>:<port>","instance":"<id>","since_unix":…}` | `WorkerEndpoint[]` (§3) | **дискавери API PgWorker** (arch/14 §1.1): ставит сам воркер; ключ жив = инстанс жив и URL валиден. URL — `https://` (t03): API PgWorker обслуживается только по mTLS — панель аутентифицируется клиентским сертификатом per-install API-CA (единая пакета с KafkaWorker, §2.3.2: `AdminPanel:Workers:WorkerTls`, env `WORKERS_PANEL_TLS_*`); `X-Api-Key`/`PGW_API_KEY` удалён (t03). Панель кеширует в снапшоте и зовёт любой живой при мутациях §9; по этим же URL отдельный тик опрашивает `/healthz` (результат — `WorkerHealth[]`, алерт `worker-unhealthy` 03 §4); в UI не отображается (только через алерт доступности `worker-api-unreachable`, 03 §4.1) |

### 2.3.2. `/kafkaworker/api/…` — дискавери API KafkaWorker

Симметрично §2.3.1: lease-ключи `/kafkaworker/api/<id>` (ставит сам воркер,
arch/16 §1.1) читаются kafka-refresher'ом в `KafkaSnapshot.WorkerEndpoints`
— источник URL для kafka-мутаций §10.2. URL — `https://` (t03): API
KafkaWorker обслуживается только по mTLS — панель аутентифицируется
клиентским сертификатом per-install API-CA (t03-pg: ЕДИНАЯ пакета на оба
воркера — один CA `kfw-install-ca` подписывает серверные серты обоих API и
клиентские серты клиентов; настройки
`AdminPanel:Workers:WorkerTls { ClientCertPem|ClientCertPath,
ClientKeyPem|ClientKeyPath, ServerCaPem|ServerCaPath }`; env
`WORKERS_PANEL_TLS_*` — переименованы из `KFW_PANEL_TLS_*` тем же релизом),
`X-Api-Key` удалён для ОБОИХ воркеров (t03-pg: PgWorker — mTLS, §2.3.1).
Отсутствие живых ключей → 503
мутаций + critical-алерт `worker-api-unreachable` (03 §4.1). По этим же
URL тик опроса `/healthz` (t09; тот же поллер и интервал, что у
PgWorker-инстансов §2.3.1, — `AdminPanel:Workers:HealthIntervalSec`)
пробит живые инстансы KafkaWorker (тем же клиентским сертом): результат —
`WorkerHealth[]` в
`KafkaSnapshot.WorkerHealth` (модель §3), warning-алерт `worker-unhealthy`
(03 §4). Degraded/unhealthy воркер виден
панели ≤ 2 тиков поллера, после восстановления алерт гаснет — панель и
docker-health больше не расходятся.

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
    IReadOnlyList<MoveTicket> MoveTickets, // §2.3.1: очередь заявок /pgworker/moves/
    IReadOnlyList<WorkerEndpoint> PgWorkerEndpoints, // §2.3.1: живые /pgworker/api/<id>
    IReadOnlyList<WorkJournalInfo> PgWorkerWork, // §2.3.1: журналы /pgworker/work/<C>
    IReadOnlyList<WorkerHealth> WorkerHealth, // §2.3.1: опрос /healthz живых инстансов
    IReadOnlyList<ProbeResult> Probes,     // результаты live-проб §6
    IReadOnlyList<Alert> Alerts,           // вычислено AlertEngine (03-panels §4)
    int UnknownKeyCount);                  // диагностика «неизвестных» ключей

// Живой инстанс API воркера (§2.3.1/§2.3.2; arch/14 §1.1, arch/16 §1.1):
// lease-ключ дискавери; URL — куда панель отправляет мутации.
sealed record WorkerEndpoint(string InstanceId, string Url, long SinceUnix);

// Запись журнала /pgworker/work/<C> (§2.3.1; формат — arch/14 §3.3):
// живая фаза процесса воркера + серия фейлов (fail_count/fail_first_unix/
// retry_not_before_unix — бэкофф ретраев provision, arch/14 §5 A).
sealed record WorkJournalInfo(
    string Cluster, string Op, string Phase, string Instance,
    long UpdatedUnix, string? LastError,
    int? FailCount, long? FailFirstUnix, long? RetryNotBeforeUnix);

// Результат опроса /healthz инстанса PgWorker (§2.3.1): 200 → Healthy,
// 503 → Degraded (детали секций health — у воркера), сетевой сбой/таймаут →
// Unreachable (lease-ключ при этом жив — воркер недавно подавал признаки).
sealed record WorkerHealth(
    string InstanceId, string Url, WorkerHealthStatus Status,
    DateTimeOffset CheckedAtUtc, string? Detail);

enum WorkerHealthStatus { Healthy, Degraded, Unreachable }

sealed record ClusterInfo(
    string Name, string? DbName, int BucketsCount, long? CreatedUnix,
    ClusterState State,                       // Active|NotInitialized|ToRemove (config.state, §9/§9.4)
    IReadOnlyList<ShardInfo> Shards,
    IReadOnlyList<BucketInfo> Buckets,     // все N, включая ACTIVE
    IReadOnlyList<HealRecord> Heals);

sealed record ShardInfo(
    string Name, string Dsn, IReadOnlyList<string> DsnHosts, int? Port,
    string? DbName, string? User, int? ReplicasDeclared,
    string? MasterAddress,                 // null => нет lease-мастера
    IReadOnlyList<NodeInfo> Nodes,         // плановые ноды (§9), у старых кластеров пусто
    ShardRuntime? Runtime);                // из SQL-пробы, nullable

// Плановая нода шарда: /clusters/<C>/shards/<X>/nodes/<n>/state (§9).
sealed record NodeInfo(string Name, string? State);

sealed record BucketInfo(
    int Id, string? Owner,                 // owner null => нет routing-ключа
    BucketState State,                     // Active/Syncing/Frozen/Aborting/NotInitialized
    MoveInfo? Move);                       // owner/target/started/updated/phase/lastError

sealed record HaScope(
    string Scope, string? Cluster, string? Shard, bool Matched,
    string? LeaderName, string? OptimeLeader, bool Initialized,
    string? RequestCpu, string? RequestMem, string? RequestDisk, // §9, заявка на ноду
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

// Заявка на переезд — значение /pgworker/moves/<C>/<bucket> (§2.3.1; формат
// PgWorker MoveRequest). Op — raw-строка канона op (move|rollback|finalize|abort);
// BucketId — id из leaf'а "bucket_<i>" (null у неканонического leaf'а).
sealed record MoveTicket(
    string Cluster, string Bucket, int? BucketId,
    string Op, string? To, long RequestedUnix, string? RequestedBy);
```

`ClusterState` — enum `Active | NotInitialized` (не инициализирован: ноды не
подняты, схемы не созданы; см. §9).

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
     дешев) и range `/pgworker/portalloc/` + `/pgworker/moves/` +
     `/pgworker/api/` + `/pgworker/work/` (§2.3.1; транспортный провал
     любого KV-чтения роняет тик — неполный снапшот хуже прежнего).
     `WorkerHealth` в снапшот вносит отдельный тик опроса /healthz
     (§2.3.1; интервал — `AdminPanel:Workers:HealthIntervalSec`, образец
     проб §6: состояние готовым, KV-тик не блокируется).
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

Разрешение адреса цели: адрес всегда берётся из etcd (`conn_url` member'а —
Patroni, DSN шарда — SQL), но подключается панель из своей сети. Перед
каждым подключением `host:port` прогоняется через маппинг
`AdminPanel:Probes:HostMap` — словарь «etcd-адрес ноды `host:port`» →
«реальный адрес, достижимый с хоста панели». Порядок: адрес из etcd →
override из `HostMap` при точном совпадении ключа → прямое подключение
к полученному адресу. По умолчанию словарь пуст (прод: панель видит ноды по
их настоящим адресам); маппинг нужен локальным стендам, где compose-имена
нод не резолвятся с хоста, а порты опубликованы под другими номерами —
значения для стенда в [04-local-stand.md](04-local-stand.md) §2.3.

### 6.1. Patroni REST `:8008` (по умолчанию включена)

- Для каждого HA-scope и каждого его member (host из `/service/…/members/`):
  `GET http://<host>:8008/cluster` (timeout 3 c) → JSON Patroni:
  `members[]{name,role,state,timeline,lag,host,port}`. Адрес `<host>:8008`
  прогоняется через `HostMap` (порядок разрешения — §6): на стенде `:8008`
  слушает patroni-эмулятор `hc*` — отдельный контейнер, опубликованный на
  хосте под другим портом.
- Даёт: фактическое состояние нод (`running`/`streaming`/`stopped`), лаг
  реплик в байтах, timeline — то, чего в etcd-DCS нет «в реальном времени».
- Порт `:8008` и путь — стандарт Patroni (pg (этот монорепозиторий) `arch/01-architecture.md`,
  HAProxy health-check использует те же).

### 6.2. SQL через Npgsql (по умолчанию включена; в проде — на усмотрение)

- Подключение: DSN шарда из etcd (пароль из DSN при наличии, иначе
  `AdminPanel:Probes:Password`; в DSN app-секрета не бывает никогда) +
  `TargetSessionAttributes=ReadWrite` (multi-host DSN ведёт на мастер),
  `Application Name=adminpanel`, `statement_timeout`. Каждый `host:port`
  из DSN перед построением connection string прогоняется через `HostMap`
  (порядок разрешения — §6): compose-имена стенда резолвятся только внутри
  compose-сети.
- Только `SELECT` к `pg_catalog`/`pg_stat_*` (образцы запросов —
  `arch/scripts/buckets-common.sh`, `move-bucket.sh`; список в
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
| заявка `/pgworker/moves/` с битым JSON/неизвестным `op` | ключ пропускается, `ParseError`-запись (алерт `key-malformed`); сам ключ не трогаем — его отвергнет и удалит процесс PgWorker |
| заявка кластера, которого нет в `/clusters/` | попадает в `MoveTickets` (UI-очередь рисует только по своим кластерам — заявка невидима, ожидаемо: мусор чистит runbook/депровижининг D2) |
| кластер `NOT_INITIALIZED` | показывается с бейджем «не инициализирован»; move-алерты и `shard-no-leader` для него подавлены (03 §4) |

## 8. Контракт тестирования парсеров

Unit-тесты парсеров прогоняют **реальные фрагменты** значений, взятые из
pg (этот монорепозиторий) (скрипты `init-cluster.sh`, `move-bucket.sh`, Patroni JSON) — включая
вырожденные: отсутствующие поля, строковые числа, пустой префикс, неизвестный
ключ. Фикстуры хранятся в `tests/AdminPanel.UnitTests/EtcdFixtures/` как
.json-файлы. Интеграционные тесты поднимают etcd (Testcontainers,
`quay.io/coreos/etcd:v3.5.21`), сеют ключи тем же сидом, что и dev-стенд
([04-local-stand.md](04-local-stand.md)), и проверяют refresher end-to-end.

## 9. Запись: создание кластера (декларативный provisioning)

Первая из двух мутаций pg-домена (вторая — перевод в удаление, §9.4). По
параметрам формы (POST `/api/clusters`,
03 §1) панель отправляет команду в **API PgWorker** (arch/14 §1.1; URL —
живой `/pgworker/api/<id>`, §2.3.1); **воркер** валидирует и пишет в etcd
**заявленную структуру** кластера; поднятие нод PG,
Patroni и инициализация схем — процессы самого PgWorker (arch/14 §5).
Паттерн — `arch/scripts/init-cluster.sh`
(регистрация в etcd последним пакетом), но без PG-шагов. Сигнатуры,
валидации и протоколы записи ниже — канон ИСПОЛНИТЕЛЯ (воркера); панель
передаёт тело команды и маппит коды ответов 1:1 (400/404/409/503; 503 —
в т.ч. когда живых ключей `/pgworker/api/` нет).

### 9.1. Набор ключей одного создания

`<C>` — имя кластера (уникально, §9.3), `N` — бакеты, `S` — шарды
(`shard1..shardS`), `R` — реплики на шард, `T` — unix-время создания:

```
/clusters/<C>/config                              {"buckets":N,"dbname":"<C>",
                                                    "created_unix":T,
                                                    "state":"NOT_INITIALIZED"}
/clusters/<C>/shards/shard<k>/replicas            "<R>"            (k=1..S)
/clusters/<C>/shards/shard<k>/nodes/<n>/state     "NOT_INITIALIZED" (n = <k>-я буква
                                                     a..z: shard1a..; R ключей на шард)
/clusters/<C>/buckets/routing/bucket_<i>          "shard<k>"       (i=0..N-1, непрерывными
                                                     блоками — §9.1.1)
/clusters/<C>/buckets/status/bucket_<i>           {"bucket":"bucket_<i>",
                                                    "state":"NOT_INITIALIZED",
                                                    "owner":"shard<k>","updated_unix":T}
/service/<C>-shard<k>/request_cpu                 "<cores>"  ("2", "0.5")  на КАЖДУЮ ноду
/service/<C>-shard<k>/request_mem                 "<Gi>Gi"   ("8Gi")       на КАЖДУЮ ноду
/service/<C>-shard<k>/request_disk                "<Gi>Gi"   ("100Gi")     на КАЖДУЮ ноду
```

Семантика:

- `config.state = "NOT_INITIALIZED"` — кластер заявлен, но ноды/схемы не
  созданы. Отсутствие поля (кластеры `init-cluster.sh`) = обычный
  инициализированный кластер.
- routing + status — начальное состояние всех N бакетов: владелец назначен
  непрерывными блоками по шардам (канон — §9.1.1), бакет `NOT_INITIALIZED`
  (не ACTIVE: схемы `bucket_<i>` на нодах ещё не существуют). Будущий
  provisioning переводит в ACTIVE (снятием status-ключа — семантика §2.1).
- `nodes/<n>/state` — плановые ноды шарда; имя = имя шарда + буква
  (`shard1a` — мастер, `shard1b…` — реплики), конвенция стенда pg (этот монорепозиторий)
  (`s1a`,`s1b`). `replicas = 1` — только мастер (`<X>a`), стандартно `2`.
- `request_*` в DCS-пространстве scope (`scope = <C>-shard<k>`): заявка
  ресурсов **на каждую ноду** scope — будущий provisioning создаёт ноды с
  этими лимитами. Формат: cpu — десятичные ядра invariant-строкой, mem/disk —
  `"<целое>Gi"`; значения проходят серверную валидацию (03 §1) до записи.
- Нешардированная БД (`sharded=false` в запросе, 03 §1.1) — вырожденный
  случай: сервер пишет структуру с N=1, S=1 (нормализация — §9.3). Формат
  ключей не меняется: `config.buckets=1`, один `shard1` со своими нодами и
  заявками, один `bucket_0 → shard1`. Отдельного признака «тип БД» в etcd
  нет: после записи нешардированная БД структурно совпадает с шардированной
  в конфигурации 1 бакет × 1 шард — читатели контракта правок не требуют.
- Что НЕ пишется: `dsn` (нод нет), `master` (нет лидера — lease появится при
  поднятии Patroni), `/service/<scope>/{leader,members,optime,initialize,config}`
  (пространство Patroni), heals.

### 9.1.1. Распределение бакетов по шардам (канон)

Владелец бакета `i` при создании — **непрерывными блоками**, «бакет к
ближайшему центру отрезка»: отрезок `0..N-1` делится на S отрезков равной
длины N/S, бакет идёт в тот шард, чей центр ближе:

```
шард(i) = floor( (2·i + 1) · S / (2·N) )      i = 0..N-1
```

(нулевой индекс шарда; в routing пишется `shard<индекс+1>`). Целочисленное
умножение/деление, без float: max `(2·8191 + 1)·128 = 2 097 024` — в int.

**Канонический пример** (критерий приёмки) — N=10, S=3 → расклад **3+4+3**:
`shard1` = бакеты 0,1,2; `shard2` = бакеты 3,4,5,6; `shard3` = бакеты 7,8,9.
Остаток (10 mod 3 = 1) достаётся **среднему** шарду, не первому.

Свойства (следствия формулы, выполняются при допустимых N ≥ S ≥ 1):

- блоки непрерывны: шард получает диапазон идущих подряд бакетов
  (floor монотонен по i; шаг S/N ≤ 1 — пропусков номеров шардов нет);
- размеры блоков отличаются не более чем на 1; каждый шард непуст
  (последний бакет всегда в `shardS`);
- N = S → по одному бакету на шард; вырожденный 1×1 → `bucket_0 → shard1`
  (нешардированная БД §9.1 — без специального случая).

Осознанное расхождение с `arch/scripts/init-cluster.sh` (round-robin
`i % S`, «первые rem шардов по +1»): панель назначает владельцев блоками —
канон задан этой секцией, скрипты pg (этот монорепозиторий) не меняются. На протокол записи
§9.2 и читателей контракта алгоритм не влияет: routing — те же ключи со
значениями `shard<k>`, меняется только распределение значений по новым
созданиям (существующие кластеры не перезаписываются — N константа, P18).

### 9.2. Протокол записи (атомарность и отказы)

Исполняет PgWorker (API arch/14 §1.1) через тот же HTTP JSON gateway
(`/v3/*`), endpoints из своего конфига `PgWorker:Etcd:Endpoints`:

1. **Клэйм имени** — `POST /v3/kv/txn`: compare `version(/clusters/<C>/config)
   == 0` + success `[put config]`. Compare не сошёлся → имя занято (409).
   Одна txn гарантирует уникальность без TOCTOU; config с `state` — первая же
   запись клэйма.
2. **Пакет PUT** остальных ключей (shards/nodes/routing/status/request_*).
   Без txn: etcd `max-txn-ops` (128 по умолчанию) не вмещает 2N+ ключей
   кластера — тот же паттерн, что `init-cluster.sh` («ключи кладутся последним
   пакетом»).
3. **Отказ посередине** → компенсация: `del --prefix /clusters/<C>/` + точечные
   `del` каждого `/service/<C>-shard<k>/request_*` (только своих ключей —
   пространство Patroni не трогаем). Компенсация не удалась → частично
   созданный кластер остаётся видимым (config + часть ключей): это безопасное
   состояние — повторное создание откажет на клэйме (409), добор ключей и
   очистка — ручная операция через etcdctl (runbook-уровень, как у
   `init-cluster.sh`).

Успешное создание не дёргает refresher принудительно: следующий тик (3 с)
подхватывает новые ключи; UI видит кластер через polling (≤5 с).

### 9.3. Валидация (сервер — источник истины, фронт дублирует для UX)

| Поле | Правило |
|---|---|
| `name` | `^[a-z][a-z0-9_]{0,62}$`; без дефиса — scope `<C>-<X>` и `ScopeMatcher` (§2.2) однозначны; уникальность — клэйм-txn (§9.2); `dbname = <C>` |
| `sharded` | bool, опционально: отсутствует/`null` = `true` (обратная совместимость — клиенты без поля ведут себя как раньше); `false` = нешардированная БД — `buckets`/`shards` не требуются и игнорируются, сервер нормализует их в 1/1 до валидации (вырожденный случай §9.1) |
| `buckets` | целое 1..8192; валидируется только при `sharded=true` (при `false` после нормализации = 1) |
| `shards` | целое 1..128 и ≤ `buckets`; валидируется только при `sharded=true` (при `false` после нормализации = 1) |
| `replicas` | целое 1..26 (буквы нод a..z), по умолчанию 2; 1 = только мастер |
| `requestCpu` | десятичные ядра, 0.01..64, каноническая invariant-строка (`"0.5"`, `"2"`) |
| `requestMem` / `requestDisk` | целые GiB 1..65536, в etcd — `"<n>Gi"` |

### 9.4. Удаление кластера (перевод в TO_REMOVE)

Вторая мутация панели: `DELETE /api/clusters/<C>` (03 §1.2) не удаляет ключи,
а переводит кластер в состояние удаления — `config.state = "TO_REMOVE"`.
Снятие нод PG и очистка ключей — задача внешнего оркестратора/будущего
provisioning'а (панель read-only к чужим ключам); до очистки кластер виден
в UI с пометкой «к удалению» (03 §3).

Протокол (одиночный PUT, без txn):

1. Имя проверяется паттерном §9.3: неканоническое → 404 (такое имя не могло
   быть создано панелью; чужие ключи панель не трогает).
2. Активный endpoint из снапшота (как §9.2); нет — 503.
3. Читается `config` напрямую у etcd (снапшот отстаёт до тика): ключа нет —
   404 «кластер не найден».
4. `state` уже `"TO_REMOVE"` → успех без записи (идемпотентность).
5. Иначе — единственный PUT `config` с сохранением `buckets`/`dbname`/
   `created_unix` и `state:"TO_REMOVE"` (перезапись канонического набора
   полей §2.1; прочие/будущие поля config не переносятся).

Без txn-клэйма: config уже существует, уникальность не участвует;
конкурентные удаления сходятся к одному значению `TO_REMOVE` (перестановка
одного и того же PUT — безопасна). Обратного перехода из `TO_REMOVE` нет.
Пока config занят, повторное создание имени невозможно (клэйм §9.2 → 409) —
имя освобождается только очисткой ключей кластера (runbook: `etcdctl del
--prefix /clusters/<C>/` + точечные `/service/<C>-shard<k>/request_*`).

### 9.5. Добавление шарда (add-shard)

Третья мутация панели: `POST /api/clusters/<C>/shards` (03 §1.3) дописывает к
живому (Active) кластеру ключи нового шарда `<X>` — переиспользование схемы §9.1:
`shards/<X>/replicas`, `shards/<X>/nodes/<X><буква>/state=NOT_INITIALIZED` × R,
`/service/<C>-<X>/request_{cpu,mem,disk}`. НЕ пишется: `dsn` (запишет PgWorker),
`master` (lease Patroni), routing/status (шард стартует ПУСТЫМ — никакого
перераспределения бакетов; явные переезды — §9.7), config кластера.

Имя `<X>` генерирует панель: `shard<max+1>` (max — по числовым суффиксам
существующих шардов, префикс `shard`; ≤128); свободного ввода нет.

Протокол (образец §9.2): (1) config напрямую у etcd — состояние проверяется до
записи (Active only: NOT_INITIALIZED → 409 «дождитесь инициализации», TO_REMOVE
→ 409 «кластер удаляется»); (2) клэйм-txn имени: compare
`version(/clusters/<C>/shards/<X>/replicas)==0` + put `replicas`; проигрыш →
409 (конкурентный POST занял имя); (3) пакет PUT остальных ключей (nodes × R +
request_*); (4) сбой посередине → компенсация best-effort: del prefix
`shards/<X>/` + точечные del `request_*`. Без ретраев: повтор = новый POST;
повтор вычисляет ТО ЖЕ имя (компенсация успешна → тот же клэйм проходит;
выжил `replicas` → 409, остатки разбираются etcdctl). Валидация полей — §9.3
(replicas 1..26 дефолт 2, cpu 0.01..64, mem/disk 1..65536 GiB).

### 9.6. Демонтаж шарда (маркер TO_REMOVE)

Четвёртая мутация панели: `DELETE /api/clusters/<C>/shards/<X>` (03 §1.4)
ставит маркер `/clusters/<C>/shards/<X>/state = "TO_REMOVE"`. Снятие контейнеров
и очистка ключей — PgWorker (guard'ы G1–G7, t06); до демонтажа шард виден в UI
с бейджем «к удалению». Маркер — состояние, а не заявка: не удаляется по
завершении (ключи шарда исчезают вместе с ним в финале S3).

Протокол (образец §9.4): (1) имена канонические, иначе 404; (2) config
напрямую: нет → 404, не Active → 409; (3) шард существует (replicas-ключ) иначе
404; (4) серверная пред-проверка guard'ов по данным снапшота: routing на шард
>0 → 409 «на шарде N бакетов — сначала явно перевезите (UI переездов — §9.7)»;
незавершённый переезд (status owner/target = шард) → 409; шард один в кластере
→ 409 «нельзя снять последний шард»; ноды QUARANTINED → 409 «сначала разбор
карантина» — PgWorker перепроверит авторитетно (гонки ловят G3/G4, маркер
останется, демонтаж подождёт); (5) PUT маркера; уже `TO_REMOVE` →
идемпотентный 204 без записи. Обратного перехода нет (one-way). Имя шарда
освобождается финалом демонтажа PgWorker (после него клэйм-txn §9.5 того же
имени пройдёт).

### 9.7. Заявки на переезды бакетов (moves)

Пятая мутация панели — заявки `move` (§9.7.1); t07 добавляет шестую–девятую:
`rollback`/`finalize`/`abort` и отмена стоящих заявок (§9.7.2–§9.7.5).
Семейство эндпоинтов `/api/clusters/{cluster}/moves*` (03 §1.5–§1.9) ставит
в etcd **очередь заявок** на переезды бакетов — ключи
`/pgworker/moves/<C>/bucket_<i>`
(формат значения — §2.3.1, канон PgWorker `MoveRequest`). Выполнение —
PgWorker (holder клэйма `<C>`): процесс берёт **старейшую заявку кластера**
по `requested_unix` (tie-break — лексикографика ключа) и обрабатывает
**одну за раз**; успех или перманентный валидационный отказ → процесс
удаляет заявку. Поэтому последовательность переездов (по источнику и по
приёмнику — кластер один) гарантирует сам PgWorker; обязанность панели —
корректно упорядочить `requested_unix`, не создавать дубликатов и
конфликтных заявок. Четыре операции (move/rollback/finalize/abort) —
**отдельными эндпоинтами per op** (тела и guard'ы различны: finalize требует
`old_shard`, abort — `force`); выбор «кто куда переезжает» — всегда явное
решение оператора, никакой автоперебалансировки.

Общий протокол постановки (все четыре ops; различия — в guard'ах п.2 и теле):

1. Имена канонические (кластер/шарды), иначе 404; config напрямую у etcd:
   нет → 404, не Active → 409 (образец §9.5/§9.6).
2. Guard'ы по прямым чтениям etcd (Д4, быстро оператору; авторитетно
   перепроверит PgWorker) — по op, см. подразделы.
3. Чтение префикса `/pgworker/moves/` напрямую у etcd, один range (снапшот
   отстаёт): на выбранный бакет нашего кластера уже стоит заявка —
   идентичная (тот же op и совпадающие параметры) → `skipped`
   (не перезаписываем); иная → 409 «на бакет уже есть заявка (op/to)».
   Попутно вычисляется база упорядочивания (глобальный max по префиксу).
4. `requested_unix` (секунды): `base = max(now, 1 + max(requested_unix по
   всему префиксу))`, заявка k-й по порядку бакета получает `base + k` —
   наши заявки строго возрастают и встают **в конец** существующей очереди.
   `requested_by` — username сессии панели (аудит).
5. Постановка — по одной txn на заявку: `compare version(moveKey) == 0` +
   `put` (клэйм-паттерн §9.5, защита от перезаписи чужой заявки между
   чтением и записью). Compare не сошёлся → 409 (конкурентная заявка).
   Сбой etcd посередине → 503 **без компенсации**: поставленные заявки
   валидны и безопасны (их выполнит PgWorker), повтор POST досдаст
   остаток — уже стоящие идентичные попадут в `skipped` (полная
   идемпотентность повтора). Осознанное отличие от §9.2/§9.5: заявка —
   очередь, а не декларация; частичная очередь не ломает кластер.

Панель НЕ перезаписывает чужие заявки — только клэймит пустые ключи
`bucket_<i>` своих бакетов (п.5) и отменяет стоящие заявки своего кластера
(§9.7.5). Правка заявок — вне панели (runbook: etcdctl).

#### 9.7.1. Заявки на переезд (move)

`POST /api/clusters/{cluster}/moves` (03 §1.5). Тело — `{from, to,
buckets[]}`: источник, приёмник, список бакетов (порядок обработки = по
возрастанию id, независимо от порядка в массиве). Ответ 201:
`{cluster, from, to, queued[], skipped[]}` — `queued` поставлены, `skipped` —
уже стоят идентичные заявки (`op=move`, `to` совпадает).

Guard'ы п.2 (поверх общих): кластер нешардированный (1 бакет и ≤1 шард —
03 §2) → 409 `NonShardedClusterException`; `from`/`to` существуют (replicas)
иначе 404; `from ≠ to` иначе 400; `to` в TO_REMOVE → 409 «приёмник
удаляется» (источник TO_REMOVE допустим — эвакуация перед демонтажем);
каждый бакет `id` ∈ `0..N-1`, `routing.owner == from` и state ACTIVE
(не SYNCING/FROZEN/ABORTING/NOT_INITIALIZED) иначе 409 с пояснением;
`buckets` непустой, без дубликатов, иначе 400.

#### 9.7.2. Заявки на откат (rollback)

`POST /api/clusters/{cluster}/moves/rollback` (03 §1.7). Тело —
`{buckets[]}`: список бакетов (порядок обработки = по возрастанию id).
Откат возвращает бакет на прежний шард по живой обратной подписке
`sub_<b>_rb` — **куда именно**, определяет PgWorker сам (SQL-факт, панель
направление не выбирает и не проверяет; подсказка в UI — из SQL-пробы,
когда включена). Ответ 201: `{cluster, queued[], skipped[]}`.

Guard'ы п.2: нешардированный → 409; каждый бакет `id` ∈ `0..N-1`,
routing есть и state ACTIVE — rollback возможен только из ACTIVE
(незавершённый переезд сначала заверши или отмени abort'ом) иначе 409;
`buckets` непустой, без дубликатов иначе 400. Если обратной подписки нет
(переезжал с `skip_reverse`, давно финализирован) — перманентный отказ
исполняет PgWorker («откат только полным re-copy» — заявка удаляется,
причина в `/pgworker/work/<C>`); панель это SQL-фактом не проверяет.

#### 9.7.3. Заявки на уборку (finalize)

`POST /api/clusters/{cluster}/moves/finalize` (03 §1.8). Тело —
`{bucket, oldShard}`: один бакет и шард, артефакты которого убирать
(выбирает оператор; подсказка в UI — шарды с живыми подписками бакета из
SQL-пробы, когда включена). Уборка необратимо DROP'ает схему бакета на
старом шарде СО ДАННЫМИ. Ответ 201: `{cluster, bucket, oldShard}`.

Guard'ы п.2: нешардированный → 409; бакет `id` ∈ `0..N-1`, routing есть,
state ACTIVE (как rollback) иначе 409; `oldShard` существует (replicas)
иначе 404; `oldShard ≠ routing.owner` иначе 409 «совпадает с текущим
владельцем — убирать нечего»; `oldShard` в TO_REMOVE допустим (финализация
перед демонтажем — типовой путь эвакуации).

#### 9.7.4. Заявки на отмену переезда (abort)

`POST /api/clusters/{cluster}/moves/abort` (03 §1.9). Тело —
`{bucket, force?}` (bool, по умолчанию `false`): один бакет с незавершённым
переездом. Отмена убирает артефакты переезда и возвращает бакет владельцу.
Ответ 201: `{cluster, bucket, force}`.

Guard'ы п.2: бакет `id` ∈ `0..N-1`, routing есть; статус-ключ жив и state
∈ {SYNCING, FROZEN, ABORTING} — иначе 409 (нет ключа/ACTIVE → «бакет ACTIVE,
отменять нечего; пост-flip артефакты убирает finalize»; NOT_INITIALIZED →
«начальное состояние создаваемого кластера, не переезд»). Быстрые
пред-проверки семантики force (порт текстов процесса, авторитетно перепроверит
PgWorker): возраст статус-ключа (`now − updated_unix`) < `AbortMinAgeSec`
и `!force` → 409 «статус свежий — переезд, возможно, ещё жив; если mover
точно мёртв — force» (отсутствие `updated_unix` у статус-ключа —
пред-проверка пропускается, авторитетно решит процесс); `routing.owner ==
status.target` и `!force` → 409
«flip прошёл, статус завис — такой abort станет уборкой старого шарда
(как finalize) — осознанно: force».

#### 9.7.5. Отмена стоящей заявки

`DELETE /api/clusters/{cluster}/moves/{bucket}` (03 §1.9): удаляет ключ
заявки `/pgworker/moves/<C>/bucket_<i>` — оператор снимает ещё не начатые
заявки из очереди. Протокол: имена канонические иначе 404; чтение ключа
напрямую у etcd — нет → 404 «заявки нет» (идемпотентность повтора
отсутствует осознанно, образец kafka-мутации §10.2-11); есть → del ключа →
204. Состояние кластера не проверяется (TO_REMOVE-кластер: заявки всё равно
чистит deprovisioning D2 — ручная отмена безвредна).

⚠️ Семантика: удаление **не останавливает** уже взятую в работу заявку —
процесс переездов ведёт фазы по статус-ключу бакета и доедет до конца;
остановка начатого переезда — только abort (§9.7.4); UI-подтверждение
это предупреждает. Guard «ещё не начата» не вводится: старейшая заявка
кластера становится «в работе» в пределах одного тика PgWorker —
детерминированно отличить её от стоящих по одним ключам etcd нельзя
(процесс не помечает взятую заявку).

### 9.8. Заявка ротации app-пароля кластера

Мутация смены per-cluster app-пароля: `POST /api/clusters/{cluster}/app-password/rotate`
(03 §1.6) ставит в etcd **заявку** `/pgworker/rotations/<C>` — ключ
координации PgWorker (арх-канон `arch/14` §3.3/§5 I). Панель сама НЕ ходит
в SQL нод (панель read-only к БД; SQL-мутации — только PgWorker) и НЕ пишет
`app_password`. Выполнение — AppPasswordRotator PgWorker под клэймом `<C>`:
ALTER ROLE на мастере каждого поднятого шарда, затем атомарный txn-коммит
(put `app_password` + del заявки одной транзакцией). Исчезновение заявки
после успеха — как у §9.7; фазы/ошибки — `/pgworker/work/<C>`.

Значение ключа (формат PgWorker, аудполя — как §9.7):

```
/pgworker/rotations/<C>  {"requested_unix":<unix>,"requested_by":"<username>"}
```

Протокол:

1. Имена канонические, иначе 404; config напрямую у etcd: нет → 404,
   не Active (NOT_INITIALIZED/TO_REMOVE) → 409 (образец §9.5).
2. Чтение `/pgworker/rotations/<C>` напрямую: ключ уже есть → 409 «ротация
   уже запрошена» (панель не перезаписывает чужие/свои живые заявки —
   отмена вне панели, runbook; после исполнения PgWorker ключ исчезает и
   повторный POST валиден).
3. `requested_by` — username сессии панели (аудит), `requested_unix` — now.
4. Постановка — ОДНА txn: `compare version(ключа)==0` + `put` (клэйм-паттерн
   §9.7 п.5). Compare не сошёлся → 409 (конкурентный POST). Сбой etcd → 503
   без компенсации: заявка либо стоит (выполнит PgWorker), либо не встала —
   повтор POST идемпотентен (живая заявка → шаг 2 → 409).
5. Ответ 201 `{cluster, requestedUnix, requestedBy}`; выполнение асинхронно
   (тики PgWorker, секунды при живых шардах). UI-модалка предупреждает:
   после применения подключения со старым паролем отвергаются до
   перечитывания `app_password` клиентами — выполнять в тихое окно.

## 10. Kafka (чтение + записи панели)

Третий домен панели. Канон ключей — [15-kafka-clusters.md](../15-kafka-clusters.md)
(контроль-плейн `/kafka/clusters/`, координация `/kafkaworker/`, формат
`topics/<T>` §3 — дословно источник истины); эта глава фиксирует панельную
проекцию: что панель читает и как мутирует. Декларатор — панель, исполнитель —
KafkaWorker ([16-kafkaworker.md](../16-kafkaworker.md)); панель никогда не
трогает контейнеры и Kafka напрямую (мутации — только ключи etcd, пробы —
read-only). Отдельный домен-снапшот `KafkaSnapshot` (не `EtcdSnapshot` pg) —
своя механика тика §4, тем же транспортом §1 и настройками endpoints.

### 10.1. Читаемые ключи

| Префикс/ключ | В модель | Примечание |
|---|---|---|
| `/kafka/clusters/<C>/config` | `KafkaClusterInfo` (config + state-маппинг arch/15 §2) | `state` отсутствует = Active; поля 2–5 — mutable-конфиги |
| `/kafka/clusters/<C>/brokers/broker<k>/{state,resources,role}` | `KafkaBrokerInfo` | `state` — raw-строка (толерантно к новым); `role` пишет воркер |
| `/kafka/clusters/<C>/endpoints` | `KafkaClusterInfo.Endpoints` | точка дискавери клиентов (arch/15 §2); отсутствие у Active — critical-алерт |
| `/kafka/clusters/<C>/app_user`, `app_password` | internal-словарь стора (не в `KafkaClusterInfo`, не в UI/API) | значение пароля наружу не отдаётся; панель не подключается к Kafka с app-кредом (роль приложений, ACL — arch/16 §2.3) |
| `/kafka/clusters/<C>/admin_user`, `admin_password`, `ca_pem` | internal-словарь стора (не в `KafkaClusterInfo`, не в UI/API) | читаются ТОЛЬКО для SASL_SSL-проб (t03: admin-кред + truststore из CA-серта кластера, arch/16 §2.3); значение пароля наружу не отдаётся |
| `/kafka/clusters/<C>/topics/<T>` | `KafkaTopicInfo` | гибрид автосинк+desired, формат arch/15 §3; `__`-топиков в etcd не бывает. + leaf-ключи заявок `topics/<T>/desired.{create,delete}` (arch/15 §3.1) → `KafkaTopicLifecycleTicket` |
| `/kafkaworker/rotations/<C>`, `/kafkaworker/admin_rotations/<C>` | `KafkaRotationTicket` | читаемые из `/kafkaworker/` — только `rotations/`, `admin_rotations/`, `rebalances/`, `reassignments/`, `regens/` (arch/15 §4); остальные ключи префикса панель не читает и не пишет |
| `/kafkaworker/rebalances/<C>` | `KafkaRebalanceTicket` | заявка ребалансировки партиций (жива = очередь в UI) |
| `/kafkaworker/reassignments/<C>` | `KafkaReassignmentProgress` | прогресс текущего reassignment воркера (drain/balance: остаток партиций, режим); отсутствие ключа = операции нет |
| `/kafkaworker/regens/<C>` | `KafkaRegenProgress` | прогресс rolling-регенерации брокеров воркером (t06, arch/15 §4): `brokers_total`/`brokers_remaining`/`current_broker`; отсутствие ключа = операции нет |

Неизвестные ключи внутри `/kafka/` — лог + счётчик `unknownKeys`; битый JSON —
parseError-запись + warning-алерт `kafka-key-malformed` (arch/15 §6).

### 10.2. Мутации панели (15; исполняет KafkaWorker API)

Все 16 мутаций панель отправляет в **API KafkaWorker** (arch/16 §1.1; URL —
живой `/kafkaworker/api/<id>`, §2.3.2); воркер сам валидирует и пишет в etcd.
Общие правила исполнителя: имена канонические
(`^[a-z][a-z0-9_]{0,62}$` — иначе 404), чтение config **напрямую** у etcd
(не из панельного снапшота — он отстаёт до тика), ProblemDetails; панель
маппит коды ответов 1:1 (503 — в т.ч. когда живых ключей
`/kafkaworker/api/` нет). Таблица — сигнатуры UI и канон записи исполнителя
(протоколы — порты §9).

| # | Мутация | Протокол записи | Отказы |
|---|---|---|---|
| 1 | **Создание кластера** `POST /api/kafka/clusters` | (1) клэйм-txn `version(config)==0` + put config `state=NOT_INITIALIZED` (§9.2-паттерн); (2) пакет PUT: `brokers/broker<k>/state=NOT_INITIALIZED` × B + `brokers/broker<k>/resources` × B (порядок k=1..B); (3) сбой → компенсация `del --prefix /kafka/clusters/<C>/`; повтор — 409 на клэйме | 400 (валидация §10.3), 409 (имя занято), 503 |
| 2 | **Удаление кластера** `DELETE /api/kafka/clusters/{c}` | PUT config RMW: `state=TO_REMOVE` с сохранением остальных полей (§9.4 один в один: уже TO_REMOVE → 204 без записи) | 404, 503 |
| 3 | **Изменение default-конфигов** `PUT /api/kafka/clusters/{c}/config` | RMW-txn по `mod_revision`: обновить `replication_factor`/`min_insync_replicas`/`default_partitions`/`default_retention_ms` (границы §10.3); применяет воркер как dynamic broker configs (converge, без рестартов); проигрыш compare → 503 (retry клиентом) | 400, 404, 409 (не Active), 503 |
| 4 | **Добавление брокера** `POST /api/kafka/clusters/{c}/brokers` | имя генерит сервер `broker<max+1>` (≤9); клэйм-txn `version(brokers/<b>/state)==0` + put `NOT_INITIALIZED` + put resources; сбой → компенсация точечными del `brokers/<b>/` (§9.5-паттерн); поднимает воркер (broker-only, кворум не меняется) | 400, 404, 409 (не Active / имя занято / предел 9), 503 |
| 5 | **Удаление брокера** `DELETE /api/kafka/clusters/{c}/brokers/{b}` | маркер `brokers/<b>/state=TO_REMOVE` (one-way, идемпотентно; §9.6-паттерн). Серверные пред-проверки (детерминированные, по снапшоту): не `controller` (role-ключ); не последний — иначе 409. Live-проверку размещения реплик панель НЕ делает (в etcd фактических реплик нет, live-проба асинхронна — серверный 409 был бы нестабильным): маркер ставится всегда (204); guard «на брокере есть партиции» авторитетно исполняет воркер (describe-all → drain процессом I арх/16 §5 → демонтаж продолжится сам) | 404, 409 (controller/последний), 503 |
| 6 | **Конфиг-заявка топика** `PUT /api/kafka/clusters/{c}/topics/{t}` | RMW-txn по §10.4: read `topics/<t>` → set `desired`/`desired_unix`/`desired_by` → txn compare `mod_revision` → put. Топик должен существовать в реестре и не быть missing (404 иначе); partitions — только больше фактического (400). Без компенсации: неудавшаяся запись не встала — повтор идемпотентен | 400, 404, 409 (не Active), 503 |
| 7 | **Отмена конфиг-заявки** `DELETE /api/kafka/clusters/{c}/topics/{t}/desired` | RMW-txn: `desired=null` (убрать `desired`/`desired_*`); 404 если заявки нет. Нужна для missing-топиков (после отмены автосинк удалит ключ) и для «передумали» | 404, 409, 503 |
| 8 | **Ротация app-пароля** `POST /api/kafka/clusters/{c}/app-password/rotate` | клэйм-txn `/kafkaworker/rotations/<C>` `version==0` + put `{"requested_unix","requested_by"}` — §9.8 один в один; исполнение — PasswordRotator KafkaWorker (фазы A/B/C, роль app); UI-модалка предупреждает о rolling-перезапуске брокеров | 404, 409 (не Active / уже запрошена), 503 |
| 9 | **Создание топика** `POST /api/kafka/clusters/{c}/topics` | тело `{name, partitions?, replicationFactor?, retentionMs?, minInSyncReplicas?}` (валидация §10.3). Проверки: кластер Active; имя — Kafka-паттерн без `__`; факт-ключ `topics/<t>` отсутствует или `missing=true` (пересоздание) — иначе 409 «топик существует»; нет живых `desired.create`/`desired.delete` — 409; нет живого `desired` у missing-ключа — 409 «сначала отмените конфиг-заявку». Развёртка дефолтов из config кластера (partitions = `default_partitions`, replicationFactor = `replication_factor` — значения пишутся в etcd полностью). Клэйм-txn `version(desired.create)==0` + put канонического JSON (arch/15 §2.1); 201 `{cluster, topic, partitions, replicationFactor}`. Без компенсаций: неудавшаяся клэйм-txn запись просто не встала (повтор безопасен) | 400 (§10.3), 404 (кластер), 409 (не Active / топик есть / заявка жива), 503 |
| 10 | **Удаление топика** `DELETE /api/kafka/clusters/{c}/topics/{t}` | клэйм-txn `version(desired.delete)==0` + put `{"requested_unix","requested_by"}`. Пред-проверки: Active; факт-ключ существует и не missing (иначе 404); нет живого `desired.create` (409 «сначала отмените заявку создания»); нет живого `desired` (409 «сначала отмените конфиг-заявку»). Идемпотентно: живая delete-заявка → 204 без записи (порт TO_REMOVE-семантики §9.4/9.6) | 404, 409, 503 |
| 11 | **Отмена заявки создания** `DELETE /api/kafka/clusters/{c}/topics/{t}/desired.create` | del ключа заявки; 404 если заявки нет | 404, 409 (не Active), 503 |
| 12 | **Отмена заявки удаления** `DELETE /api/kafka/clusters/{c}/topics/{t}/desired.delete` | del ключа заявки; 404 если заявки нет. **Окно деструктивности**: снимает удаление до тика воркера | 404, 409, 503 |
| 13 | **Ребалансировка партиций** `POST /api/kafka/clusters/{c}/rebalance` | клэйм-txn `/kafkaworker/rebalances/<C>` `version==0` + put `{"requested_unix","requested_by"}` — протокол ротаций §9.8 один в один; исполнение — PartitionReassigner KafkaWorker (арх/16 §5 I; converge RF к `config.replication_factor`, лидеры сохраняются); UI-модалка предупреждает о переносе данных между брокерами | 404, 409 (не Active / уже запрошена), 503 |
| 14 | **Отмена ребалансировки** `DELETE /api/kafka/clusters/{c}/rebalance` | del заявки: новые батчи не подаются, уже поданные reassignment-бакеты Kafka доигрывает сама (безопасно — данные не теряются, converge просто останавливается на полпути); 404 если заявки нет | 404, 503 |
| 15 | **Изменение ресурсов брокера** `PUT /api/kafka/clusters/{c}/brokers/{b}/resources` | тело `{cpu?, memGi?, diskGi?}` (null = не менять; хотя бы одно — обязательно; границы §10.3, уменьшение разрешено — риск OOM на операторе, arch/16 R7); put ключа `brokers/<b>/resources` каноническим JSON (перезапись целиком). Применение — автоматическое: NodeRegenerator воркера (arch/16 §5 J) сверяет лимиты живого контейнера и rolling-ит по одному за тик; `disk` меняет только декларацию (действий нет). Идемпотентен (повтор — та же запись) | 400 (§10.3 / пустое тело), 404 (кластер/брокер), 409 (не Active / state `TO_REMOVE`/`REMOVING`), 503 |
| 16 | **Ротация admin-пароля** `POST /api/kafka/clusters/{c}/admin-password/rotate` (t03) | клэйм-txn `/kafkaworker/admin_rotations/<C>` `version==0` + put `{"requested_unix","requested_by"}` — протокол ротаций §9.8 один в один; исполнение — PasswordRotator KafkaWorker (фазы A/B/C, роль admin, arch/16 §5 H); UI-модалка предупреждает о rolling-перезапуске брокеров | 404, 409 (не Active / уже запрошена), 503 |

### 10.3. Валидация создания/конфиг-мутации (сервер — источник истины)

| Поле | Правило |
|---|---|
| `name` | `^[a-z][a-z0-9_]{0,62}$`; уникальность — клэйм-txn §10.2 п.1 |
| `brokers` | целое 1..9, def 3 |
| `replicationFactor` | целое 1..9 и ≤ brokers, def 3 |
| `minInSyncReplicas` | целое 1..RF, def 2 |
| `defaultPartitions` | целое 1..1000, def 12 |
| `defaultRetentionMs` | целое 1..2147483647, def 604800000 (7 дней) |
| `cpu` | десятичные ядра 0.01..64, def 2 (на брокера; мутации 1/4/15) |
| `mem`/`disk` | целые GiB 1..65536, def 2/20 (на брокера; в etcd `"<n>Gi"`; мутации 1/4/15) |

Топик `<t>` (мутации 6–7): Kafka-паттерн `^[a-zA-Z0-9._-]{1,249}$`, без `__`-префикса.

Создание топика (мутация 9, t01):

| Поле | Правило |
|---|---|
| `name` | Kafka-паттерн `^[a-zA-Z0-9._-]{1,249}$`, без `__`-префикса (как мутации 6–7); неканоническое — 404 |
| `partitions` | целое 1..1000, def = config.default_partitions |
| `replicationFactor` | целое 1..9 и ≤ config.brokers, def = config.replication_factor |
| `retentionMs` | 1..2147483647, опц. (нет → брокерный default, не пишется в configs) |
| `minInSyncReplicas` | ≥ 1 и ≤ эффективного RF, опц. |

`configs` create-заявки — только управляемые ключи (`retention.ms`,
`min.insync.replicas`); сервер собирает их из retentionMs/minInSyncReplicas
(как `KafkaTopicDesiredPlan.Build`).

### 10.4. Интеракция desired/missing (заявки конфигов топиков)

Панель пишет **только** `desired`-часть значения `topics/<T>` (формат —
arch/15 §3): RMW read → set `desired{partitions?, configs{retention.ms?,
min.insync.replicas?}}` + `desired_unix`=now + `desired_by`=username → txn
`compare mod_revision == прочитанной` → put. Управляемые поля: `partitions` —
только увеличение (≤ фактического — 400); `configs.retention.ms`,
`configs.min.insync.replicas`; лишние поля — 400.

- Применение и снятие заявки — автосинк воркера (arch/15 §3): desired
  отличается от факта → применить к Kafka + снять desired; проигрыш compare →
  re-read (панель успела переписать — применится свежий).
- **missing-ветка**: топик исчез из Kafka при живой заявке → воркер ставит
  `missing=true`, ключ не удаляет; панель показывает «топик отсутствует,
  заявка не исполнена» + warning-алерт `kafka-topic-missing-desired`;
  отмена (мутация 7) → следующий автосинк удаляет ключ.
- Отмена несуществующей заявки → 404 («заявки нет» — desired уже снят/не
  ставился); идемпотентность повтора PUT — перезапись той же заявки
  свежим `desired_unix` (безопасно).
- **Lifecycle-заявки** (мутации 9–12, arch/15 §3.1): постановка — клэйм-txn
  `version==0` на leaf-ключ `desired.create`/`desired.delete` (гварды —
  таблица §10.2), отмена — del ключа заявки (404 если нет), исполнение/чистка —
  del воркером. DELETE удаления идемпотентен (живая заявка → 204 без записи —
  порт TO_REMOVE-семантики кластера/брокера). Конфиг-заявка `desired` и
  lifecycle-заявки не живут одновременно: create/delete требуют отмены живого
  `desired` (409), живой `desired` у удаляемого топика гасится вместе с
  факт-ключом одной txn воркера.
