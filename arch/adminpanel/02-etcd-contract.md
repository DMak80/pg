# 02. Контракт etcd (чтение + записи панели)

AdminPanel читает etcd для инспекции. Источник схемы — инспектируемая система
(`../pg`: `arch/11-bucket-sharding.md` §2, `arch/12-bucket-pitfalls.md` §3,
скрипты `arch/scripts/`). Единственная операция записи — **создание нового
кластера** (декларативный provisioning, §9): все остальные ключи панель не
пишет и не удаляет никогда.

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
| `/clusters/<C>/config` | JSON `{"buckets":N,"dbname":"<C>","created_unix":…,"state"?:"NOT_INITIALIZED"\|"DELETING"}` | `ClusterInfo` (константы) | N — константа навсегда (P18); `created_unix` может отсутствовать (старые init); `state` пишется только панелью: при создании (§9) и переводе в удаление (§9.4); отсутствует/иное = обычный инициализированный кластер |
| `/clusters/<C>/shards/<X>/dsn` | libpq-строка `host=n1,n2,n3 port=5432 dbname=<C> user=bucket_admin` | `ShardInfo.Dsn`, парсим хосты/порт/dbname/user | пароля нет (секреты в env); пароль для SQL-побы приходит из настроек панели; у создаваемого панелью кластера ключа нет — ноды ещё не подняты (§9) |
| `/clusters/<C>/shards/<X>/replicas` | целое-строка `"2"` | `ShardInfo.ReplicasDeclared` | декларативное намерение; факт — в HA (`/service/`) |
| `/clusters/<C>/shards/<X>/master` | `"host:6432"` | `ShardInfo.MasterAddress` (nullable) | lease TTL 5 c; отсутствие = нет живого мастера (P11) |
| `/clusters/<C>/shards/<X>/nodes/<n>/state` | строка `"NOT_INITIALIZED"` | `NodeInfo.State` | плановые ноды создаваемого панелью кластера (§9); имена `<X><буква>` (`s1a`,`s1b`… — конвенция стенда `../pg`); нода поднята/инициализирована — ключ меняет будущий provisioning |
| `/clusters/<C>/buckets/routing/bucket_<N>` | имя шарда `"shard1"` | `BucketInfo.Owner` | единственный авторитет «где бакет»; отсутствие при N из config = «дыра» в карте |
| `/clusters/<C>/buckets/status/bucket_<N>` | JSON `{"bucket","state":"SYNCING"\|"FROZEN"\|"ABORTING"\|"NOT_INITIALIZED","owner","target"?,"started_unix"?,"updated_unix","phase"?,"last_error"?}` | `BucketInfo.State=ACTIVE`-иначе, `MoveInfo` | отсутствие ключа = ACTIVE; ключ удаляется атомарно с flip; `NOT_INITIALIZED` — начальное состояние бакетов создаваемого кластера (§9), `target/started_unix/phase` при нём отсутствуют |
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
    string Name, string? DbName, int BucketsCount, long? CreatedUnix,
    ClusterState State,                       // Active|NotInitialized|Deleting (config.state, §9/§9.4)
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
- Порт `:8008` и путь — стандарт Patroni (`../pg` `arch/01-architecture.md`,
  HAProxy health-check использует те же).

### 6.2. SQL через Npgsql (по умолчанию включена; в проде — на усмотрение)

- Подключение: DSN шарда из etcd **+ Password из настроек панели**
  (`AdminPanel:Probes:Password`; в DSN пароля нет никогда) +
  `TargetSessionAttributes=ReadWrite` (multi-host DSN ведёт на мастер),
  `Application Name=adminpanel`, `statement_timeout`. Каждый `host:port`
  из DSN перед построением connection string прогоняется через `HostMap`
  (порядок разрешения — §6): compose-имена стенда резолвятся только внутри
  compose-сети.
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
| кластер `NOT_INITIALIZED` | показывается с бейджем «не инициализирован»; move-алерты и `shard-no-leader` для него подавлены (03 §4) |

## 8. Контракт тестирования парсеров

Unit-тесты парсеров прогоняют **реальные фрагменты** значений, взятые из
`../pg` (скрипты `init-cluster.sh`, `move-bucket.sh`, Patroni JSON) — включая
вырожденные: отсутствующие поля, строковые числа, пустой префикс, неизвестный
ключ. Фикстуры хранятся в `tests/AdminPanel.UnitTests/EtcdFixtures/` как
.json-файлы. Интеграционные тесты поднимают etcd (Testcontainers,
`quay.io/coreos/etcd:v3.5.21`), сеют ключи тем же сидом, что и dev-стенд
([04-local-stand.md](04-local-stand.md)), и проверяют refresher end-to-end.

## 9. Запись: создание кластера (декларативный provisioning)

Первая из двух мутаций панели (вторая — перевод в удаление, §9.4). По
параметрам формы (POST `/api/clusters`,
03 §1) панель пишет в etcd **заявленную структуру** кластера; поднятие нод PG,
Patroni и инициализация схем — отдельная задача вне панели (будущий
provisioning читает эти ключи). Паттерн — `../pg arch/scripts/init-cluster.sh`
(регистрация в etcd последним пакетом), но без PG-шагов.

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
  (`shard1a` — мастер, `shard1b…` — реплики), конвенция стенда `../pg`
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

Осознанное расхождение с `../pg arch/scripts/init-cluster.sh` (round-robin
`i % S`, «первые rem шардов по +1»): панель назначает владельцев блоками —
канон задан этой секцией, скрипты `../pg` не меняются. На протокол записи
§9.2 и читателей контракта алгоритм не влияет: routing — те же ключи со
значениями `shard<k>`, меняется только распределение значений по новым
созданиям (существующие кластеры не перезаписываются — N константа, P18).

### 9.2. Протокол записи (атомарность и отказы)

Тот же HTTP JSON gateway (`/v3/*`), активный endpoint из снапшота (§2.4):

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

### 9.4. Удаление кластера (перевод в DELETING)

Вторая мутация панели: `DELETE /api/clusters/<C>` (03 §1.2) не удаляет ключи,
а переводит кластер в состояние удаления — `config.state = "DELETING"`.
Снятие нод PG и очистка ключей — задача внешнего оркестратора/будущего
provisioning'а (панель read-only к чужим ключам); до очистки кластер виден
в UI с пометкой «удаляется» (03 §3).

Протокол (одиночный PUT, без txn):

1. Имя проверяется паттерном §9.3: неканоническое → 404 (такое имя не могло
   быть создано панелью; чужие ключи панель не трогает).
2. Активный endpoint из снапшота (как §9.2); нет — 503.
3. Читается `config` напрямую у etcd (снапшот отстаёт до тика): ключа нет —
   404 «кластер не найден».
4. `state` уже `"DELETING"` → успех без записи (идемпотентность).
5. Иначе — единственный PUT `config` с сохранением `buckets`/`dbname`/
   `created_unix` и `state:"DELETING"` (перезапись канонического набора
   полей §2.1; прочие/будущие поля config не переносятся).

Без txn-клэйма: config уже существует, уникальность не участвует;
конкурентные удаления сходятся к одному значению `DELETING` (перестановка
одного и того же PUT — безопасна). Обратного перехода из `DELETING` нет.
Пока config занят, повторное создание имени невозможно (клэйм §9.2 → 409) —
имя освобождается только очисткой ключей кластера (runbook: `etcdctl del
--prefix /clusters/<C>/` + точечные `/service/<C>-shard<k>/request_*`).
