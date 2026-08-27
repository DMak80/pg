# Спецификация t03-etcd-snapshot — etcd-клиент, парсеры контроль-плейна, снапшот и refresher

Дата: 2026-08-22. Фаза dev-flow: spec. Источники истины: `arch/roadmap/etcd.md`
(пункт `t03-etcd-snapshot`), `arch/02-etcd-contract.md` (ГЛАВНЫЙ документ
задачи: транспорт, ключи, модель снапшота §3, poll §4, сбои §7, тесты §8),
`arch/01-architecture.md` §1–2, §6 (настройки), §8. Реальная схема ключей и
форматы значений — `docs/superpowers/2026-08-22-arch-design/research/pg-report.md`
(раздел 2) и код инспектируемой системы `../pg/arch/stand/` (`sidecar/
rolecheck.py`, `hasync.py` — образцы HTTP JSON gateway-вызовов).

## 1. Цель

Модуль `AdminPanel.Etcd`: read-only клиент etcd через HTTP JSON gateway
`/v3/*` (без gRPC-стека), парсеры ключей контроль-плейна `/clusters/`,
`/service/`, `/cluster/nodes/` в иммутабельную модель `EtcdSnapshot` (типы —
в `AdminPanel.Core`, контракт 02 §3), `SnapshotStore` (атомарная замена ссылки)
и `SnapshotRefresher` (`BackgroundService`, тик `RefreshInterval` = 3 c,
sticky endpoint-failover, устойчивость к сбоям: при отказе etcd прежние данные
остаются и помечаются возрастом `BuiltAtUtc`, растёт `ConsecutiveFailures`).
Интеграция с health-checks по паттерну t01 (`HealthCheckAbstract`).
API-эндпоинты и `AlertEngine` — t04; live-пробы — t06 — в t03 не входят.

Новых пакетов runtime — два служебных (`Microsoft.Extensions.Http`,
`Microsoft.Extensions.Hosting.Abstractions`); тестовый — `Testcontainers`
(integration). Специализированного .NET-модуля Testcontainers для etcd на
NuGet нет (проверено 2026-08-22: поиск `Testcontainers.Etcd` и `Ryd3P` — 0
результатов; официальный список модулей dotnet.testcontainers.org etcd не
содержит) — используется generic-контейнер с образом, предписанным 02 §8
(`quay.io/coreos/etcd:v3.5.21`), и собственным builder'ом в тестовом проекте.

## 2. Принципы

- Источник истины — `arch/`; всё, что arch/ не оговаривает, решено минимальным
  способом и зафиксировано в §3. Расхождение с arch/ запрещено (SPEC_DEVIATION).
- Идентификаторы — английские; комментарии в коде — русские.
- Тесты — xunit v3 + FluentAssertions, комментарии по нотации AAA
  (`// Arrange` / `// Act` / `// Assert`), на русском.
- Панель читает etcd и никогда не пишет (02, преамбула). Единственные
  «записи» в etcd делает тестовый сид integration-тестов — от имени теста,
  не панели.
- Паттерны скелета t01/t02 обязательны: attribute-DI (`[InjectAs*]`),
  `[Config]`-POCO + `IOptions<T>`, `Result`-монада, CQRS не трогаем (модуль
  ниже API), `HealthCheckAbstract`/`IHealthCheckService`, модульная композиция
  `ModuleExtensions.AddEtcd()`.

## 3. Решения в рамках контракта arch/ (уточнения неоднозначностей)

1. **Retry-политики Puzzle НЕ переносятся.** Задание допускало «retry с
   backoff по паттерну Puzzle Retry если уместно» — не уместно: 02 §4 прямо
   фиксирует «нет ретраев внутри тика — следующий тик и есть ретрай», а
   `consecutiveFailures` кормит алерт с порогом 2. Polly в панель не тянем,
   `IRetryConfig`/`RetryPolicies` не копируем. Устойчивость обеспечивают:
   тик-цикл (естественный retry), sticky-failover endpoint'ов (§3.5) и
   сохранение прежнего снапшота при сбое (§3.6).
2. **`RequestTimeout` = 2 c** (arch/01 §6), а не «3 с» из текста задания
   координатора: там 3 с — это `RefreshInterval`. Буква arch главнее;
   таймаут меньше тика — тик не перекрывает сам себя (при `RefreshInterval`
   3 c и таймауте 3 c зависший запрос съедал бы весь цикл).
3. **Формат настроек — секунды `double`**: `RefreshIntervalSeconds = 3`,
   `RequestTimeoutSeconds = 2` (в `EtcdOptions`, ключи appsettings
   `AdminPanel:Etcd:RefreshIntervalSeconds` / `RequestTimeoutSeconds`).
   Семантика та же, что у `RefreshInterval`/`RequestTimeout` из arch/01 §6,
   единица измерения зашита в имя (как `SessionHours` в t02) — без
   TimeSpan-строк и культурных сюрпризов при биндинге.
4. **`EtcdSnapshot` расширяется полем `ParseErrors`**
   (`IReadOnlyList<KeyParseError>`): 02 §7 требует «в снапшот попадает
   `ParseError`-запись (видна в UI-details)», но в записи снапшота 02 §3
   такого поля нет — минимальное расширение модели, закрывающее дыру контракта.
   `KeyParseError(string Key, string Reason)` — immutable record в Core.
   Алерт `key-malformed` (t04) строится по этим записям.
5. **Типы `Alert`, `ProbeResult`, `ShardRuntime` и прочие runtime-типы
   заводятся в Core сейчас, наполняются позже**: модель снапшота по 02 §3 —
   единый immutable-слепок, поля `Alerts`/`Probes`/`Runtime` должны
   существовать уже в t03, иначе t04/t06 ломали бы контракт. В t03 снапшот
   собирается с `Alerts = []`, `Probes = []`, `Runtime = null`. `Alert` — по
   DTO 03 §2 (`Id`, `Severity`, `Kind`, `Target`, `Message`, `Details`,
   `SinceUnix` — nullable, заполняет AlertEngine в t04). `ProbeResult` —
   минимальный контракт 02 §6/arch/01 §3 (`Target`, `Kind`, `Ok`, `LatencyMs`,
   `Error`, `AtUtc`); t06 может расширить (это не ломает t03).
6. **Вычислимые пометки — computed-свойства records**: `ClusterInfo.Incomplete`
   (`DbName is null || BucketsCount <= 0` — реализация пометки «incomplete»
   из 02 §7 без расширения конструктора) и `ShardInfo.MasterLeaseAlive`
   (`MasterAddress is not null` — lease-семантика 02 §1: ключ есть = lease
   жив). Соответствуют `incomplete`/`masterLeaseAlive` DTO из 03 §2;
   сериализацию в DTO делает t04/t05.
7. **Бакеты incomplete-кластера**: если `/clusters/<C>/config` отсутствует
   (`BucketsCount = 0`), список бакетов строится из фактически найденных
   routing/status-ключей (у остальных `Owner = null`); при наличии config —
   все `N` бакетов `0..N-1`, как требует 02 §2.1 («все N, включая ACTIVE»).
8. **`EtcdStatus.LastRefreshUtc`** — время последнего тика, коснувшегося etcd
   (успешного или нет); `EtcdSnapshot.BuiltAtUtc` — время последней
   **успешной** сборки данных. При полном отказе тика (§3.6) `BuiltAtUtc`
   прежнего снапшота сохраняется — возраст данных растёт, `snapshot-stale`
   (t04) считается от `BuiltAtUtc`.
9. **Отказ тика = обновление только Etcd-части**: при недоступности всех
   endpoints (или падении всех KV-чтений) строится новый снапшот, где
   `Etcd` = свежий статус (`Reachable = false`, endpoints с ошибками,
   `ConsecutiveFailures + 1`, `LastRefreshUtc = now`), а `Clusters`/`HaScopes`/
   `StandNodes`/`BuiltAtUtc` — ссылки из прежнего снапшота. Если снапшота еще
   не было — пустые коллекции, `BuiltAtUtc = now` (возраст отсчитывается от
   старта). Это буквальная реализация «снапшот прежний, `Etcd.Reachable=false`»
   из 02 §7: данные не подменяются, статус_etcd свежий.
10. **Sticky + внутритиковый failover**: «активный» endpoint — sticky-индекс с
    прошлого тика; на тике активен он, если жив по свежему status, иначе
    первый живой по порядку списка (02 §4 п.1). Если вызов на активном падает
    транспортной ошибкой уже после статусов (умер между вызовами), чтения
    повторяются на следующем живом endpoint по кругу — один проход, без
    задержек и повторов на том же endpoint. Это failover (смена цели),
    а не retry — запрета 02 §4 не нарушает; «без потери тика» из 02 §7
    выполняется.
11. **`QuorumSuspected`-эвристика** (02 §7 «кворума нет»): `true`, если есть
    живые endpoints, но ни у одного из них нет валидного `leader != 0` при
    ненулевом `raftTerm`, либо непустой `errors[]` содержит raft-признаки
    (строки с `raft`/`no leader`). Одиночный стендовый etcd отдаёт
    `leader` = собственный memberId — не подозревается.
12. **Пустые/битые `Endpoints` в конфиге** — не роняют хост: refresher на
    каждом тике завершается `Result.Failed(EtcdNotConfigured)`, на старте
    один `LogWarning` «AdminPanel:Etcd:Endpoints не задан — etcd-данные
    недоступны». Health-check: `Degraded` (не `Unhealthy`) до первого тика,
    далее `Unhealthy` по факту неработающего refresher. Integration/unit-тесты
    задают endpoints явно.
13. **`SnapshotStore.Current` nullable** (`EtcdSnapshot?`): до первого тика
    снапшота нет; потребители (t04) показывают «загрузка». Атомарная замена —
    запись в `volatile`-поле.
14. **Модель Core — плоский namespace `AdminPanel.Core`**, транспорт —
    `AdminPanel.Etcd.Client` и `AdminPanel.Etcd.Parsing`. Домен не знает про
    HTTP (направление зависимостей arch/01 §1).
15. **Integration-тесты НЕ используют attribute-DI и `WebApplicationFactory`**:
    статический кеш `ServiceCollectionExtensions._assemblies` (грабля t02
    §14: второй хост в процессе не получает регистраций) означает, что ручной
    `UseDiBehaviours` + `AutoRegistration(Etcd-сборка)` в тестовом процессе
    «съел» бы регистрацию для будущих WAF-хостов t04. Gateway/refresher/store
    конструируются напрямую `new` + `Options.Create` — статический кеш не
    трогается вовсе. Хостовые сценарии (healthz с etcd-чеком, API-смоук) —
    t04.
16. **Сид integration-тестов = сид dev-станда** (02 §8: «сеют ключи тем же
    сидом, что и dev-стенд»): в t03 — C#-набор `EtcdSeed.Demo` со значениями
    из arch/04 §2.2 (кластер `demo`, 16 бакетов, 2 шарда, 3 статус-ключа
    переездов, heal, два `/service/`-scope'а, `/cluster/nodes/*`). Скрипт
    `seed.sh` появится в t10 и обязан использовать те же значения.
17. **Gateway-числа int64 приходят строками** (особенность etcd JSON gateway:
    `mod_revision`, `ID`, `dbSize`, `raftIndex`… сериализуются как decimal-
    строки): DTO клиента читаются `System.Text.Json` с
    `JsonNumberHandling.AllowReadingFromString`; имена полей берутся по
    фактическим proto-именам etcd 3.5 (`mod_revision`, `dbSize`, `peerURLs`,
    `clientURLs`, `memberID`, `ID`) — неконсистентный casing etcd отражается
    явными `[JsonPropertyName]`. Реальные имена полей подтверждает
    integration-тест против живого etcd (§12): расхождение правится в DTO,
    контракт модели не меняется.
18. **Живость master-ключа — семантика ключа** (02 §1): lease-ID из range не
    получить, поэтому `MasterAddress != null` ⇔ lease жив. Отдельных
    lease-запросов панель не делает.

## 4. Транспорт: `IEtcdGateway` (AdminPanel.Etcd.Client)

### 4.1. Интерфейс

```csharp
// Read-only клиент etcd через HTTP JSON gateway /v3/* (arch/02 §1).
public interface IEtcdGateway
{
    // Префиксный range: POST /v3/kv/range {key, range_end=prefix+1}.
    Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct);

    // POST /v3/maintenance/status — персонально на указанный endpoint.
    Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct);

    // POST /v3/cluster/member/list.
    Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct);

    // POST /v3/maintenance/alarm.
    Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct);
}

// Декодированная пара KV (base64 уже снят клиентом).
public sealed record Kv(string Key, string Value, ulong ModRevision);

// Данные status-ответа без контекста endpoint (url/latency добавляет refresher).
public sealed record EtcdStatusPayload(
    string? Version, long? DbSizeBytes, ulong? LeaderMemberId,
    ulong? RaftIndex, ulong? RaftTerm);
```

Методы принимают endpoint явно — выбор/ротация «активного» принадлежат
refresher (02 §4), клиент чисто транспортный. `/v3/kv/put`, `/v3/lease/*` в
интерфейсе отсутствуют принципиально: панель не пишет (02, преамбула).

### 4.2. Реализация `EtcdGateway`

- `[InjectAsSingleton(typeof(IEtcdGateway))]`; конструктор — именованный
  `HttpClient` (константа `HttpClientName = "etcd"`), никаких других
  зависимостей.
- `ModuleExtensions.AddEtcd()` (правка существующего файла):
  `services.AutoRegistration(Assembly)` (как сейчас) +
  `services.AddHttpClient(EtcdGateway.HttpClientName).ConfigureHttpClient(
  (sp, c) => c.Timeout = TimeSpan.FromSeconds(sp.GetRequiredService<
  IOptions<EtcdOptions>>().Value.RequestTimeoutSeconds))` — единственное
  место конфигурации таймаута; `RequestTimeoutSeconds <= 0` — fallback 2 c
  и `LogWarning` (защита от опечатки, по образцу t02 `SessionHours`).
- Запрос: `POST {endpoint}/v3/kv/range` (и др.), `Content-Type:
  application/json`, тело `{"key":"<b64>","range_end":"<b64>"}`.
  `range_end` — префикс с инкрементированным последним байтом
  (`PrefixEnd("/clusters/") == "/clusters0"`); переполнение байта `0xFF`
  переносится в предыдущий (для наших фиксированных префиксов не случается,
  helper обрабатывает корректно).
- Ответ не-2xx → `Result.Failed(EtcdHttpException(endpoint, status, body))`;
  сетевая ошибка/таймаут → `Result.Failed` исходной ошибкой; битый JSON →
  `Result.Failed` (десериализация через `Result.FromAsync`).
- `kvs` отсутствует в ответе (пустой префикс) → пустой список, успех.
- JSON-опции клиента: `PropertyNamingPolicy = SnakeCaseLower` не годится
  (casing etcd неконсистентен) — все `[JsonPropertyName]` явные;
  `NumberHandling = JsonNumberHandling.AllowReadingFromString` (§3.17).

### 4.3. Внутренние DTO ответов (EtcdGateway, private)

```csharp
// /v3/kv/range
{ "header": {…}, "kvs": [ { "key": b64, "value": b64, "mod_revision": "42" } ], "count": "16" }
// /v3/maintenance/status
{ "header": { "member_id": "…", "raft_term": "…" }, "version": "3.5.21",
  "dbSize": "…", "leader": "…", "raftIndex": "…", "raftTerm": "…" }
// /v3/cluster/member/list
{ "header": {…}, "members": [ { "ID": "…", "name": "…",
  "peerURLs": ["…"], "clientURLs": ["…"] } ] }
// /v3/maintenance/alarm
{ "header": {…}, "alarms": [ { "memberID": "…", "alarm": 1 } ] }  // 1 = NOSPACE, 2 = CORRUPT
```

`alarm` — enum-число: `EtcdAlarmType { None = 0, NoSpace = 1, Corrupt = 2 }`
(Core, §5).

## 5. Модель снапшота (AdminPanel.Core)

Точно по 02 §3 (immutable records, enum'ы английские) + решения §3.4–3.5.
Файлы и состав:

```csharp
// EtcdSnapshot.cs
sealed record EtcdSnapshot(
    DateTimeOffset BuiltAtUtc,
    EtcdStatus Etcd,
    IReadOnlyList<ClusterInfo> Clusters,
    IReadOnlyList<HaScope> HaScopes,
    IReadOnlyList<StandNode> StandNodes,
    IReadOnlyList<ProbeResult> Probes,     // t03: всегда []
    IReadOnlyList<Alert> Alerts,           // t03: всегда []
    IReadOnlyList<KeyParseError> ParseErrors, // расширение §3.4
    int UnknownKeyCount);

sealed record KeyParseError(string Key, string Reason);

// EtcdStatus.cs
sealed record EtcdStatus(
    bool Reachable,
    IReadOnlyList<EtcdEndpoint> Endpoints,
    IReadOnlyList<EtcdMember> Members,
    IReadOnlyList<EtcdAlarm> Alarms,
    string? ActiveEndpoint,
    bool QuorumSuspected,
    DateTimeOffset LastRefreshUtc,
    int ConsecutiveFailures);

sealed record EtcdEndpoint(
    string Url, bool Reachable, double? LatencyMs, string? Version,
    long? DbSizeBytes, ulong? LeaderMemberId, ulong? RaftIndex, ulong? RaftTerm,
    IReadOnlyList<string> Errors);

sealed record EtcdMember(ulong Id, string? Name,
    IReadOnlyList<string> PeerUrls, IReadOnlyList<string> ClientUrls);

sealed record EtcdAlarm(ulong MemberId, EtcdAlarmType Type);
enum EtcdAlarmType { None, NoSpace, Corrupt }

// ClusterInfo.cs
sealed record ClusterInfo(
    string Name, string? DbName, int BucketsCount, long? CreatedUnix,
    IReadOnlyList<ShardInfo> Shards,
    IReadOnlyList<BucketInfo> Buckets,
    IReadOnlyList<HealRecord> Heals)
{
    public bool Incomplete => DbName is null || BucketsCount <= 0; // §3.6
}

sealed record ShardInfo(
    string Name, string Dsn, IReadOnlyList<string> DsnHosts, int? Port,
    string? DbName, string? User, int? ReplicasDeclared,
    string? MasterAddress,                 // null => нет lease-мастера
    ShardRuntime? Runtime)                 // t03: null (пробы — t06)
{
    public bool MasterLeaseAlive => MasterAddress is not null; // §3.6
}

sealed record BucketInfo(
    int Id, string? Owner,                 // owner null => нет routing-ключа
    BucketState State, MoveInfo? Move);
enum BucketState { Active, Syncing, Frozen, Aborting }

sealed record MoveInfo(
    string? Owner, string? Target, long? StartedUnix, long? UpdatedUnix,
    string? Phase, string? LastError);

sealed record HealRecord(string Bucket, string? Was, string? Now,
    string? Reason, long? TsUnix);

// HaScope.cs
sealed record HaScope(
    string Scope, string? Cluster, string? Shard, bool Matched,
    string? LeaderName, string? OptimeLeader, bool Initialized,
    IReadOnlyList<HaMember> Members, string? RawConfig);

sealed record HaMember(
    string Name, string Host, int? Port, string? Role, string? State,
    long? Timeline, long? LagBytes,          // Patroni-проба (t06): null
    DateTimeOffset? ProbeAtUtc, string? ProbeError);

// StandNode.cs
sealed record StandNode(string Name, string? Address);

// Alert.cs (наполняет AlertEngine в t04)
enum AlertSeverity { Info, Warning, Critical }
sealed record Alert(
    string Id, AlertSeverity Severity, string Kind, string Target, string Message,
    IReadOnlyDictionary<string, string>? Details, long? SinceUnix);

// ProbeResult.cs (минимальный контракт 02 §6; наполняют пробы t06)
sealed record ProbeResult(
    string Target, string Kind, bool Ok, double? LatencyMs,
    string? Error, DateTimeOffset AtUtc);

// ShardRuntime.cs (наполняет SQL-проба t06)
sealed record ShardRuntime(
    string Shard, IReadOnlyList<ReplicationSlotInfo> Slots,
    IReadOnlyList<StandbyInfo> Standbies,
    IReadOnlyList<SubscriptionInfo> Subscriptions,
    IReadOnlyList<string> BucketSchemas,
    bool? IsInRecovery, string? Error);

sealed record ReplicationSlotInfo(string SlotName, string SlotType, bool Active,
    string? WalStatus, long? SafeWalSizeBytes, long? LagBytes);
sealed record StandbyInfo(string ApplicationName, string? ClientAddr,
    string State, string SyncState, long? LagBytes);
sealed record SubscriptionInfo(string Name, string? ReceivedLsn,
    string? LatestEndLsn, DateTimeOffset? LatestEndTime);

// ScopeMatcher.cs — связь scope → (cluster, shard), arch/01 §2
public static class ScopeMatcher
{
    // "<C>-<X>" по известным кластерам: scope.startsWith("<C>" + "-"),
    // suffix совпал с именем шарда кластера <C>. Несколько кандидатов <C> —
    // берётся тот, чей suffix точно равен имени шарда; иначе первый по
    // префиксу с Matched=false-подобной пометкой (см. ниже).
    public static (string? Cluster, string? Shard, bool Matched) Match(
        string scope, IReadOnlyList<ClusterInfo> clusters);
}
```

Правило мэтчинга (02 §2.2): `scope` сопоставляется по `startsWith("<C>-")`
по всем кластерам, известным из `/clusters/`; если suffix (часть после
`<C>-`) равна имени шарда этого кластера → `Matched = true, Cluster = <C>,
Shard = <X>`; если префикс совпал, но suffix — не имя шарда →
`Cluster = <C>, Shard = null, Matched = false`; если не совпал ни один
префикс → `Cluster = Shard = null, Matched = false` («чужой service в общем
etcd — норма», 02 §7).

## 6. Парсеры (AdminPanel.Etcd.Parsing)

Чистые статические функции `IReadOnlyList<Kv> → модель` (02 §4 п.3);
JSON — `System.Text.Json` c `AllowReadingFromString` (толерантность к
«строковым числам», 02 §8). Битые значения не бросают исключения наружу —
порождают `KeyParseError` и счётчик неизвестных ключей.

### 6.1. `ClustersParser.Parse(IReadOnlyList<Kv>) → ClustersParseResult`

`sealed record ClustersParseResult(IReadOnlyList<ClusterInfo> Clusters,
IReadOnlyList<KeyParseError> Errors, int UnknownKeyCount);`

Разбор ключей `/…`:

| Ключ | Разбор |
|---|---|
| `/clusters/<C>/config` | JSON `{"buckets","dbname","created_unix"?}`; поля опциональны и толерантны к строкам; битый JSON → `ParseError`, кластер остаётся с дефолтами (`DbName=null, BucketsCount=0` → `Incomplete`) |
| `/clusters/<C>/shards/<X>/dsn` | `DsnParser.Parse` (§6.4); исходная строка — в `ShardInfo.Dsn` |
| `/clusters/<C>/shards/<X>/replicas` | целое-строка `"2"`; не парсится → `ParseError`, `ReplicasDeclared = null` |
| `/clusters/<C>/shards/<X>/master` | `"host:6432"` как есть; пустая строка → `ParseError`, `MasterAddress = null` |
| `/clusters/<C>/buckets/routing/bucket_<N>` | значение — имя шарда; ключ с нечисловым `N` → `ParseError` |
| `/clusters/<C>/buckets/status/bucket_<N>` | JSON 02 §2.1: `{"bucket","state","owner","target","started_unix","updated_unix","phase","last_error"?}`; `state` не из множества → `ParseError`; отсутствие ключа = ACTIVE |
| `/clusters/<C>/heals/<bucket>` | JSON `{"bucket","was","now","reason","ts"}`; имя бакета — из значения `bucket` (fallback — суффикс ключа) |
| прочие `/clusters/…` | `UnknownKeyCount++` (парсер — чистая функция, без логирования; вывод счётчика в лог — задача refresher'а на debug-уровне) |

`BucketInfo`-сборка: для кластера с валидным `config` — все `0..N-1`;
`State = Active`, `Move = null` при отсутствии status-ключа; `Owner = null`
при отсутствии routing-ключа. Для incomplete-кластера — §3.7.

### 6.2. `ServiceParser.Parse(IReadOnlyList<Kv>, IReadOnlyList<ClusterInfo>) → ServiceParseResult`

`sealed record ServiceParseResult(IReadOnlyList<HaScope> Scopes,
IReadOnlyList<KeyParseError> Errors, int UnknownKeyCount);`

| Ключ | Разбор |
|---|---|
| `/service/<scope>/leader` | JSON `{"name":"pg1"}`; на стенде возможна plain-строка-имя (`"pg1"` без JSON) — парсим оба варианта: JSON с полем `name`, иначе сырая строка (обрезанный whitespace) → `LeaderName` |
| `/service/<scope>/members/<name>` | Patroni-JSON толерантно: `name`, `conn_url` (`host:port`, порт опционален → `Port = null`), `role`, `state`; отсутствие полей — не ошибка |
| `/service/<scope>/config` | raw-JSON как строка (`RawConfig`) |
| `/service/<scope>/optime/leader` | число-строка (LSN) → `OptimeLeader` |
| `/service/<scope>/initialize` | любая непустая строка → `Initialized = true` |
| прочие `/service/…` | `UnknownKeyCount++` |

Связь scope → кластер/шард — `ScopeMatcher.Match` (§5) по кластерам того же
снапшота.

### 6.3. `StandNodesParser.Parse(IReadOnlyList<Kv>) → IReadOnlyList<StandNode>`

`/cluster/nodes/<node>` → `StandNode(node, value)`; пустое значение →
`Address = null`. Прочие ключи под `/cluster/nodes/` игнорируются молча
(стендовый реестр однороден).

### 6.4. `DsnParser.Parse(string dsn) → DsnInfo`

`sealed record DsnInfo(IReadOnlyList<string> Hosts, int? Port,
string? DbName, string? User);`

libpq keyword-строка: сплит по whitespace, `key=value`; `host` — список по
запятой (`host=s1a,s1b` → два хоста); `port`, `dbname`, `user` — одиночные.
Нераспознанные/битые токены игнорируются (DSN пишут init-скрипты pg;
quoting-синтаксис libpq — YAGNI, в системе не используется). Парсеры
значений статус-ключей (`started_unix` и пр.) принимают и число, и
строку-число (fixture 02 §8 «строковые числа»).

### 6.5. `SnapshotBuilder`

Статическая сборка: `SnapshotBuilder.Build(TimeProvider time,
ClustersParseResult, ServiceParseResult, IReadOnlyList<StandNode>,
IReadOnlyList<EtcdMember>, IReadOnlyList<EtcdAlarm>, EtcdStatus etcd)
→ EtcdSnapshot` — склеивает части тика, проставляет `BuiltAtUtc = now`,
`Alerts = []`, `Probes = []`, суммирует `UnknownKeyCount` обоих префиксов и
конкатенирует `ParseErrors`.

## 7. `SnapshotStore` и `SnapshotRefresher`

### 7.1. `SnapshotStore`

```csharp
// Хранилище текущего снапшота: атомарная замена ссылки (arch/01 §1).
public interface ISnapshotStore
{
    EtcdSnapshot? Current { get; }
    void Replace(EtcdSnapshot snapshot);
}
```

`[InjectAsSingleton(typeof(ISnapshotStore))] class SnapshotStore` —
`private volatile EtcdSnapshot? _current`. Читатели никогда не блокируются.

### 7.2. `SnapshotRefresher`

```csharp
// Единственный писатель снапшота (arch/01 §1): тик RefreshIntervalSeconds.
[InjectAsSingleton(typeof(IHostedService))]
public sealed class SnapshotRefresher(
    IEtcdGateway gateway, ISnapshotStore store, IOptions<EtcdOptions> options,
    TimeProvider time, ILogger<SnapshotRefresher> logger) : BackgroundService,
    IHealthCheckService
{
    // Ядро одного тика — публично для тестов (unit интеграционный прогон без хоста).
    public async Task<Result> RefreshOnceAsync(CancellationToken ct);
    public bool Inited { get; }        // завершён хотя бы один тик
    public bool Working { get; }       // последний тик успешен
    public Result StatusError { get; } // ошибка последнего неуспешного тика
}
```

`ExecuteAsync`: `using var timer = new PeriodicTimer(interval)` (fallback 3 c
при `<= 0` + `LogWarning`); первый тик сразу (без ожидания периода — панель
набирает данные со старта), далее по тику. Отмена — через `ct`.

Алгоритм `RefreshOnceAsync` (02 §4, порядок обязательный):

1. `Endpoints` пуст/невалиден → `LogWarning` (один раз на серии) +
   `Result.Failed(EtcdNotConfiguredException)`; счётчик неудач тика растёт
   (§3.12 — ведёт себя как полный отказ: Etcd-часть обновляется, данные
   прежние).
2. Параллельно `Task.WhenAll` — `StatusAsync` на каждый endpoint; латентность
   каждого замеряется `Stopwatch.GetTimestamp()` вокруг вызова; исход
   `Result` → `EtcdEndpoint` (`Reachable`, `LatencyMs` при успехе, `Errors` —
   сообщение исключения при неудаче, поля статуса при успехе).
3. Живые endpoints = `Reachable`. Нет живых → сценарий §3.9
   (отказ тика): обновить Etcd-часть прежнего снапшота (или пустого),
   `ConsecutiveFailures++`, `Working = false`, `StatusError` — агрегат ошибок
   endpoints; `Result.Failed`.
4. Активный = sticky (по индексу, сохранённому с прошлого успешного тика),
   если жив; иначе первый живой по списку. Сохранить sticky.
5. На активном параллельно: `RangeAsync("/clusters/")`,
   `RangeAsync("/service/")`, `RangeAsync("/cluster/nodes/")`,
   `MemberListAsync`, `AlarmAsync`. Транспортный провал любого из них —
   внутритиковый failover (§3.10): повтор на следующем живом endpoint по
   кругу, один проход. Все KV-чтения упали → сценарий §3.9. Упал только
   `member/list`/`alarm` — тик успешен с `Members = []`/`Alarms = []`,
   ошибка фиксируется в `EtcdEndpoint.Errors` активного endpoint; данные
   снапшота остаются валидными (метаданные — не KV).
6. Парсеры (`ClustersParser`, `ServiceParser` с кластерами этого же тика,
   `StandNodesParser`).
7. `QuorumSuspected` — эвристика §3.11 по EtcdEndpoint'ам.
8. `SnapshotBuilder.Build(...)` → `store.Replace(snapshot)`;
   `ConsecutiveFailures = 0`, `Working = true`, `StatusError = Success`.
   `EtcdStatus.Reachable = true`, `ActiveEndpoint` — URL активного,
   `LastRefreshUtc = now` (ставится и в сценарии §3.9).

`IHealthCheckService`-состояние обновляется в конце каждого тика: `Inited`
после первого, `Working`/`StatusError` по исходу.

### 7.3. `EtcdHealthCheck`

`[InjectAsTransient] class EtcdHealthCheck : HealthCheckAbstract<
SnapshotRefresher>` — без собственной логики: `Unhealthy` при ошибке
последнего тика (etcd недоступен), `Degraded` при старте (первого тика не
было), `Healthy` при живом refresher. Регистрация в `Program.cs`:
`.AddCheck<EtcdHealthCheck>("etcd")` (после `self`-чека). Имя чека —
`"etcd"`. Чек регистрируется **без тега `live`**: `/api/healthz` — liveness
«живость самой панели» (arch/03 §1, `{status:"ok"}`), поэтому маппинг
healthz фильтрует чеки по тегу `live` (`Predicate = r => r.Tags.Contains(
"live")`) — статус etcd-чека не роняет healthz; его отдают эндпоинты t04+
и health-check напрямую (unit/integration покрыт).

## 8. Настройки и композиция

### 8.1. `EtcdOptions` (AdminPanel.Etcd)

```csharp
// [Config]-POCO etcd-подключения: секция AdminPanel:Etcd (arch/01 §6).
[Config("AdminPanel:Etcd")]
public class EtcdOptions
{
    // HTTP JSON gateway endpoints, напр. "http://etcd1:2379". Обязателен хотя бы один.
    public string[] Endpoints { get; set; } = [];

    // Тик снапшота (arch/02 §4). <= 0 — fallback 3 c с LogWarning.
    public double RefreshIntervalSeconds { get; set; } = 3;

    // Таймаут HTTP-запроса к одному endpoint (arch/01 §6). <= 0 — fallback 2 c.
    public double RequestTimeoutSeconds { get; set; } = 2;
}
```

Свойства `get; set;` — биндинг `AutoRegistrationConfigDiTypeBehaviour`
(паттерн t02 §6.1).

### 8.2. appsettings

`appsettings.json` (прод-базовая; секции etcd без secrets):

```json
"AdminPanel": {
  "Auth": { "Username": "admin" },
  "Etcd": {
    "Endpoints": [],
    "RefreshIntervalSeconds": 3,
    "RequestTimeoutSeconds": 2
  }
}
```

`appsettings.Development.json` — dev-стенд quick-профиля (arch/04 §1):

```json
"AdminPanel": {
  "Auth": { "Username": "admin", "Password": "admin", "AllowHttp": true },
  "Etcd": {
    "Endpoints": [ "http://localhost:2379" ]
  }
}
```

### 8.3. `Program.cs` (правка — две точки)

1) Цепочка health-checks (после `self`-чека):

```csharp
   .AddHealthChecks()
   .AddCheck("self", () => HealthCheckResult.Healthy(), ["live"])
   .AddCheck<EtcdHealthCheck>("etcd")   // [t03] чек refresher'а; без тега live — healthz не роняет (arch/03 §1)
```

2) Маппинг healthz — фильтр liveness-чеков по тегу (см. §7.3: healthz =
живость панели, статус etcd-чека туда не входит):

```csharp
app.MapHealthChecks(
    "/api/healthz",
    new HealthCheckOptions
    {
        Predicate = registration => registration.Tags.Contains("live"),
        ResponseWriter = HealthzWriter.WriteStatus,
    });
```

Больше Program.cs не меняется: `AddEtcd()` уже вызывается скелетом t01,
hosted-сервис и `ISnapshotStore` подхватятся attribute-DI.

## 9. Состав изменений (дерево файлов)

```
src/AdminPanel.Core/
├── ModuleExtensions.cs                 [без изменений]
├── EtcdSnapshot.cs                     [новый] EtcdSnapshot, KeyParseError
├── EtcdStatus.cs                       [новый] EtcdStatus, EtcdEndpoint, EtcdMember,
│                                               EtcdAlarm, EtcdAlarmType
├── ClusterInfo.cs                      [новый] ClusterInfo(+Incomplete), ShardInfo
│                                               (+MasterLeaseAlive), BucketInfo,
│                                               BucketState, MoveInfo, HealRecord
├── HaScope.cs                          [новый] HaScope, HaMember
├── StandNode.cs                        [новый] StandNode
├── Alert.cs                            [новый] Alert, AlertSeverity (наполнение — t04)
├── ProbeResult.cs                      [новый] ProbeResult (мин. контракт; t06)
├── ShardRuntime.cs                     [новый] ShardRuntime, ReplicationSlotInfo,
│                                               StandbyInfo, SubscriptionInfo (t06)
└── ScopeMatcher.cs                     [новый] scope "<C>-<X>" → (cluster, shard)
src/AdminPanel.Etcd/
├── ModuleExtensions.cs                 [правка] AddEtcd(): AutoRegistration +
│                                               AddHttpClient("etcd") c таймаутом
├── EtcdOptions.cs                      [новый] [Config("AdminPanel:Etcd")]-POCO
├── SnapshotStore.cs                    [новый] ISnapshotStore + реализация (volatile)
├── SnapshotRefresher.cs                [новый] BackgroundService + IHealthCheckService
│                                               + RefreshOnceAsync (ядро тика)
├── SnapshotBuilder.cs                  [новый] сборка EtcdSnapshot из частей тика
├── EtcdHealthCheck.cs                  [новый] HealthCheckAbstract<SnapshotRefresher>
├── Client/
│   ├── IEtcdGateway.cs                 [новый] интерфейс + Kv + EtcdStatusPayload
│   └── EtcdGateway.cs                  [новый] HTTP-реализация, DTO ответов,
│                                           base64, PrefixEnd
└── Parsing/
    ├── ClustersParser.cs               [новый] /clusters/* → ClusterInfo[]
    ├── ServiceParser.cs                [новый] /service/* → HaScope[]
    ├── StandNodesParser.cs             [новый] /cluster/nodes/* → StandNode[]
    └── DsnParser.cs                    [новый] libpq-keyword DSN → DsnInfo
src/AdminPanel.Api/
├── Program.cs                          [правка] + .AddCheck<EtcdHealthCheck>("etcd")
├── appsettings.json                    [правка] + секция AdminPanel:Etcd
└── appsettings.Development.json        [правка] + Endpoints localhost:2379
src/Directory.Packages.props            [правка] + Microsoft.Extensions.Http,
                                            Microsoft.Extensions.Hosting.Abstractions
                                            (10.0.x, синхронизировать с существующими
                                            строками), + Testcontainers 4.14.0
src/AdminPanel.Etcd/AdminPanel.Etcd.csproj [правка] + PackageReference Http,
                                            Hosting.Abstractions
src/tests/AdminPanel.UnitTests/
├── AdminPanel.UnitTests.csproj         [правка] + ProjectReference AdminPanel.Etcd,
│                                               AdminPanel.Core + None-копия EtcdFixtures
├── EtcdFixtures/
│   ├── clusters-full.json              [новый] полный demo-сид /clusters/ (arch/04 §2.2)
│   ├── clusters-degenerate.json        [новый] вырожденные случаи 02 §7–8
│   ├── service-full.json               [новый] demo-сид /service/ (2 scope)
│   ├── service-unmatched.json          [новый] чужой scope, plain-строка leader
│   ├── stand-nodes.json                [новый] /cluster/nodes/*
│   ├── gateway-range.json              [новый] сырой ответ /v3/kv/range (base64)
│   ├── gateway-status.json             [новый] сырой ответ /v3/maintenance/status
│   ├── gateway-member-list.json        [новый] сырой ответ /v3/cluster/member/list
│   └── gateway-alarm.json              [новый] сырой ответ /v3/maintenance/alarm
├── EtcdFixtures.cs                     [новый] загрузчик фикстур (LoadKv/LoadText)
├── ClustersParserTests.cs              [новый]
├── ServiceParserTests.cs               [новый]
├── StandNodesParserTests.cs            [новый]
├── DsnParserTests.cs                   [новый]
├── ScopeMatcherTests.cs                [новый]
├── EtcdGatewayTests.cs                 [новый] fake HttpMessageHandler: base64,
│                                           range_end, имена полей, ошибки
├── SnapshotBuilderTests.cs             [новый]
├── SnapshotStoreTests.cs               [новый]
└── SnapshotRefresherTests.cs           [новый] FakeEtcdGateway + FixedTimeProvider
src/tests/AdminPanel.IntegrationTests/
├── AdminPanel.IntegrationTests.csproj  [правка] + PackageReference Testcontainers,
│                                               ProjectReference AdminPanel.Etcd/Core
├── EtcdContainerFixture.cs             [новый] Testcontainers etcd + HTTP-wait + сид
├── EtcdSeed.cs                         [новый] C#-сид demo (значения arch/04 §2.2)
└── EtcdSnapshotIntegrationTests.cs     [новый] gateway/refresher против живого etcd
arch/roadmap/etcd.md                    [правка] удалить пункт t03-etcd-snapshot (§15)
```

`Directory.Build.props`, `.slnx`, Infrastructure/Probes/Api-сервисы — без
изменений (кроме перечисленных правок Program.cs и appsettings).

## 10. Unit-тесты (src/tests/AdminPanel.UnitTests/)

Сервисы конструируются напрямую (`new EtcdGateway(httpClient, …)`, `new
SnapshotRefresher(fakeGateway, store, Options.Create(…), time,
NullLogger<…>.Instance)`) — без TestHost и attribute-DI (грабля статического
кеша — §3.15). Фикстуры — читаемые JSON-массивы
`[{"key":"/clusters/demo/config","value":"{\"buckets\":16,…}","modRevision":42}, …]`
(plain-строки; base64 — зона ответственности gateway, парсеры получают
декодированные `Kv`). Загрузчик `EtcdFixtures.LoadKv(name)` читает из
выходного каталога сборки.

### 10.1. `ClustersParserTests`

- `Parse_FullDemoSeed_BuildsClustersShardsBuckets` — 1 кластер, 2 шарда,
  16 бакетов, владельцы round-robin, `MasterAddress`, `ReplicasDeclared`,
  `DsnHosts`/`Port`/`DbName`/`User` из DSN.
- `Parse_StatusKeys_MapToMoveInfo` — bucket_3 SYNCING / bucket_7 ABORTING
  (c `Phase`, `LastError`) / bucket_11 FROZEN; отсутствие ключа → `Active`.
- `Parse_HealJournal_Collected` — heal-запись с полями was/now/reason/ts.
- `Parse_MissingConfig_ClusterIncomplete` — кластер без config:
  `Incomplete = true`, `DbName = null`, `BucketsCount = 0`, бакеты из
  наличествующих ключей (§3.7).
- `Parse_ConfigWithoutCreatedUnix_NullCreatedUnix`.
- `Parse_StringyNumbers_Tolerated` — `"created_unix":"1755800000"`,
  `"buckets":"8"` как строки.
- `Parse_BrokenStatusJson_ParseErrorRecorded` — битый JSON значения:
  ключ пропущен, `ParseErrors` содержит ключ, бакет Active.
- `Parse_BrokenReplicas_NullAndParseError`.
- `Parse_UnknownKey_Counted` — `/clusters/demo/surprise` →
  `UnknownKeyCount = 1`, не падает.
- `Parse_RoutingBadBucketId_ParseError` — `bucket_abc`.
- `Parse_EmptyPrefix_EmptyResult` — пустой список → пустые кластеры, 0 ошибок.

### 10.2. `ServiceParserTests`

- `Parse_DemoScopes_MatchedToClusters` — `demo-s1`/`demo-s2` →
  `Cluster="demo"`, `Shard="s1"/"s2"`, `Matched=true`.
- `Parse_LeaderJson_NameExtracted`; `Parse_LeaderPlainString_Tolerated` —
  значение `"s1a"` без JSON-обёртки.
- `Parse_Members_ConnUrlHostPortRoleParsed` — `conn_url: "s1a:5432"`,
  `role: master|replica`, `state`.
- `Parse_OptimeAndInitialize_Filled` — число-строка LSN, `Initialized=true`.
- `Parse_RawConfig_KeptAsString`.
- `Parse_UnmatchedScope_Flagged` — `other-scope`: `Matched=false`,
  отображается «как есть» (02 §2.2), не ошибка.
- `Parse_PartialShardSuffix_Unmatched` — `demo-s9` (префикс совпал, шарда
  нет): `Cluster="demo"`, `Shard=null`, `Matched=false`.

### 10.3. `StandNodesParserTests`

- `Parse_Nodes_MappedToStandNode` — 4 ноды стенда.
- `Parse_EmptyValue_NullAddress`.

### 10.4. `DsnParserTests`

- `Parse_MultiHost_SplitByComma` — `host=s1a,s1b port=5432 dbname=demo
  user=postgres`.
- `Parse_MissingKeywords_Nulls`.
- `Parse_ExtraKeywords_Ignored` — `sslmode=require application_name=x`.
- `Parse_Empty_EmptyHosts`.

### 10.5. `ScopeMatcherTests` (Core)

- `Match_KnownClusterAndShard_True`; `Match_SuffixNotShard_False`;
  `Match_UnknownPrefix_AllNullFalse`; `Match_NoClusters_False`.

### 10.6. `EtcdGatewayTests`

Fake `HttpMessageHandler` (захардкоженные ответы из фикстур gateway-*.json):

- `Range_Prefix_RequestHasBase64KeyAndRangeEnd` — перехват тела запроса:
  base64 ключа `/clusters/` и `range_end` = base64 `/clusters0`.
- `Range_DecodesBase64Kvs` — ответ gateway-range.json → декодированные
  `Kv` (plain-ключи/значения, `ModRevision` из строки-числа).
- `Range_EmptyKvs_EmptyList` — ответ без `kvs` → `[]`, успех.
- `Status_ParsesFields` — gateway-status.json: version/dbSize/leader/
  raftIndex/raftTerm из строк-чисел.
- `MemberList_ParsesUrls` — `peerURLs`/`clientURLs`, `ID` строкой.
- `Alarm_MapsAlarmType` — `"alarm":1` → `NoSpace`.
- `HttpError_ReturnsFailed` — 503 → `Result.Failed` c `EtcdHttpException`.
- `NetworkError_ReturnsFailed` — `HttpRequestException` из handler'а →
  `Result.Failed`.

### 10.7. `SnapshotBuilderTests`

- `Build_FullParts_AssemblesSnapshot` — `BuiltAtUtc` из TimeProvider,
  `Alerts`/`Probes` пусты, `UnknownKeyCount` суммирован, `ParseErrors`
  конкатенированы.

### 10.8. `SnapshotStoreTests`

- `Replace_SetsCurrentAtomically` — первая запись видна.
- `Current_NullBeforeFirstReplace`.

### 10.9. `SnapshotRefresherTests`

`FakeEtcdGateway : IEtcdGateway` (управляемый: ответы по endpoint'ам,
счётчики вызовов) + `FixedTimeProvider` (независимая копия файла из
IntegrationTests — тесто-сборки не ссылаются друг на друга) + `NullLogger`:

- `Refresh_AllAlive_BuildsAndStoresSnapshot` — 2 endpoint'а живы:
  статус обоих, активный = sticky-первый, снапшот в store, `Working=true`,
  `ConsecutiveFailures=0`.
- `Refresh_AllDead_PreservesDataAndCountsFailure` — первый тик успешен,
  затем все endpoints падают: `BuiltAtUtc` не изменился, кластеры прежние,
  `Etcd.Reachable=false`, `ConsecutiveFailures` растёт по тикам.
- `Refresh_Recovery_ResetsFailures` — после отказа endpoints оживают:
  `ConsecutiveFailures=0`, свежий `BuiltAtUtc`.
- `Refresh_StickyFails_OverToNextAlive` — активный умирает между тиками:
  тик успешен, активный = второй endpoint.
- `Refresh_MidTickFailure_FailsOverWithoutLosingTick` — статус живой, но
  range на активном кидает транспортную ошибку, на втором — успех:
  снапшот собран (§3.10).
- `Refresh_EmptyEndpoints_FailedTick` — `EtcdNotConfigured`: `Result.Failed`,
  `Working=false`.
- `Refresh_NoEndpoints_HealthState` — `Inited=true` после первого тика.

## 11. Integration-тесты (src/tests/AdminPanel.IntegrationTests/)

Требуют Docker (Testcontainers) — фиксируется в критериях приёмки. Не
используют `WebApplicationFactory`/attribute-DI (§3.15): модуль
конструируется вручную. Не входят в коллекцию `"api"` — отдельная коллекция
`"etcd"` со своим fixture.

### 11.1. `EtcdContainerFixture` (collection fixture `"etcd"`)

- `Testcontainers` generic: образ `quay.io/coreos/etcd:v3.5.21` (02 §8),
  команда как у pg-стенда (прод-minimum): `etcd --name test --data-dir=/data
  --listen-client-urls=http://0.0.0.0:2379 --advertise-client-urls=
  http://127.0.0.1:2379` (gateway `/v3/*` включён по умолчанию в 3.5 —
  arch/04 §2.1).
- `WithPortBinding(2379, assignRandomHostPort: true)`; endpoint =
  `http://localhost:{hostPort}`.
- Готовность: цикл ретраев `POST /v3/maintenance/status` (до ~30×1 c) —
  встроенная HTTP-wait Testcontainers шлёт GET, gateway требует POST, поэтому
  свой цикл в `IAsyncLifetime.InitializeAsync`.
- Сид `EtcdSeed.Demo` (§3.16) — после готовности: `POST /v3/kv/put`
  `{"key": b64, "value": b64}` по каждому ключу (тот же транспорт, что и у
  панели; etcdctl не нужен). Мастер-ключи и `/cluster/nodes/*` — без lease
  (панель различия не видит: семантика ключа, 02 §1).
- `StopAsync`/`DisposeAsync` контейнера — используются тестом «отказ etcd»
  (остановка и повторный старт не нужны: тик просто не находит живой endpoint).
  Для сценария восстановления — второй fixture-контейнер не заводим: тест
  «отказ» проверяет деградацию на уже собранном снапшоте.

### 11.2. `EtcdSnapshotIntegrationTests` (коллекция `"etcd"`)

- `Gateway_Status_AgainstRealEtcd` — `Version = "3.5.21"`, `LeaderMemberId
  != 0`, `RaftTerm >= 0` — подтверждает фактические имена полей gateway
  (§3.17).
- `Gateway_MemberList_SingleMember` — 1 member, `ClientUrls` непусты.
- `Gateway_Alarm_Empty` — `[]`.
- `Gateway_Range_ClustersPrefix_ReturnsSeededKvs`.
- `Refresher_RefreshOnce_BuildsExpectedSnapshot` — refresher с
  `Endpoints = [fixtureEndpoint]`: после `RefreshOnceAsync` — кластер demo
  (16 бакетов, 8/8), шарды s1/s2 с master, статус-ключи (SYNCING/FROZEN/
  ABORTING), heal, HA-scope'ы demo-s1/demo-s2 matched, стендовые ноды,
  `ActiveEndpoint` = endpoint, `Reachable=true`, `Alerts`/`Probes` пусты.
- `Refresher_SecondTick_PicksUpChanges` — `kv/put` перевладения
  `routing/bucket_0` → `s2` → второй тик отражает нового владельца.
- `Refresher_EtcdStopped_KeepsPreviousSnapshot` — `await fixture.Stop()`:
  `RefreshOnceAsync` → `Result.Failed`; `BuiltAtUtc` прежний, кластеры
  прежние, `Etcd.Reachable=false`, `ConsecutiveFailures=1` → повторный тик
  → 2.
- `Refresher_Failover_DeadFirstEndpoint` — `Endpoints = ["http://localhost:1",
  fixtureEndpoint]`: тик успешен, `ActiveEndpoint` = второй.
- `HealthCheck_ReflectsRefresherState` — `EtcdHealthCheck` напрямую:
  `Healthy` после успешного тика; после остановки etcd и неуспешного тика —
  `Unhealthy`.

## 12. Ограничения (что НЕ делается)

- `AlertEngine` и API-эндпоинты — t04 (`Alerts = []` в снапшоте).
- Live-пробы Patroni/SQL — t06 (`Probes = []`, `Runtime = null`).
- Watch `/v3/watch` — никогда в этой версии (02 §5), пересмотр — roadmap.
- Retry/Polly/backoff внутри тика — нет (§3.1).
- gRPC-клиент etcd, сторонние .NET-обёртки etcd — нет (02 §1).
- Запись в etcd из панели (kv/put, lease) — нет; сидит только тест (§3.16).
- Кеширование «префикс `/cluster/nodes/` существует» — не делаем: 02 §4
  п.2 явно упрощает до «просто шлём range, пустой ответ дешев».
- Специализированный Testcontainers-модуль etcd — нет пакета (§1); свой
  builder только в тестовом проекте.
- Фронтенд, DTO-мапперы API, `appsettings` Probes — вне t03.
- Мутации `arch/01–04` запрещены; из `arch/roadmap/` меняется только файл
  `etcd.md` — удаление пункта по деливераблу §15.

## 13. Пакеты (Directory.Packages.props)

| Пакет | Версия | Куда |
|---|---|---|
| `Microsoft.Extensions.Http` | 10.0.x (синхронизировать с существующими `Microsoft.Extensions.*` при добавлении) | CPM + AdminPanel.Etcd |
| `Microsoft.Extensions.Hosting.Abstractions` | 10.0.x (аналогично) | CPM + AdminPanel.Etcd |
| `Testcontainers` | 4.14.0 | CPM + AdminPanel.IntegrationTests |

`System.Text.Json` — в составе shared framework net10.0, отдельной ссылки не
нужно. `ILogger` — транзитивно через Hosting.Abstractions. Версии 10.0.x
уточняются при добавлении по фактическим на nuget (в CPM уже есть
`Microsoft.Extensions.*` 10.0.9 / 10.0.0 — новые строки ставятся в тот же
ряд; точная минорная версия — на этапе plan по `dotnet list package`).

## 14. Настройки тестовых проектов

- `AdminPanel.UnitTests.csproj`: + `ProjectReference` на `AdminPanel.Etcd` и
  `AdminPanel.Core`; + `<None Include="EtcdFixtures\**\*.json"
  CopyToOutputDirectory="PreserveNewest" />`.
- `AdminPanel.IntegrationTests.csproj`: + `PackageReference Testcontainers`;
  + `ProjectReference` на `AdminPanel.Etcd` и `AdminPanel.Core` (транзитивно
  через Api уже есть, но явно — для читаемости зависимостей теста).
- Существующие тесты t01/t02 не меняются (кроме csproj-добавок выше).

## 15. Деливерабл roadmap

Тем же мерж-коммитом удалить пункт `t03-etcd-snapshot` (строку) из
`arch/roadmap/etcd.md`. Зависимость `t04-etcd-api ← t02-auth,
t03-etcd-snapshot` не трогается — по указанию координатора и прецеденту t02
(зависимости `← tNN` очищаются задачей-владельцем зависимости, не
зависимостью). Правка выполняется в ветке задачи до мержа — попадает в
мерж-коммит.

## 16. Критерии приёмки

1. `dotnet build src/AdminPanel.slnx` — успех, 0 warnings
   (`TreatWarningsAsErrors=true` не подавлен).
2. `dotnet test src/AdminPanel.slnx` — все тесты зелёные; **нужен Docker**
   (Testcontainers поднимает etcd; без Docker integration-тесты падают —
   фиксируется в CI-нотисе).
3. Unit-парсеры гоняют фикстуры реальных форматов `../pg` (02 §8): полный
   demo-сид + вырожденные случаи (§10.1–10.5).
4. Integration против живого etcd: refresher строит ожидаемый снапшот сида;
   отказ etcd → снапшот прежний, `ConsecutiveFailures` растёт; failover на
   второй endpoint работает (§11.2).
5. Ручной сценарий (Development + dev-стенд quick `docker compose up etcd
   seed`): `dotnet run` — в логах тики refresher'а без ошибок;
   `GET /api/healthz` → 200 `{"status":"ok"}` — healthz остаётся liveness
   «живости панели» (arch/03 §1) и статуса etcd-чека не отражает; сам чек
   `EtcdHealthCheck` — `Healthy` (WAF-смоук эндпоинтов — t04). Без стенда:
   `Endpoints: []` → warning на старте, чек `etcd` `Unhealthy`
   (напрямую/через t04), хост жив, healthz по-прежнему 200 ok.
6. `grep PackageReference` по csproj: только §13; версии — в CPM.
7. Панель не отправляет в etcd ничего, кроме `POST /v3/{kv/range,
   maintenance/status, cluster/member/list, maintenance/alarm}` (ревью;
   сид-puts живут только в тестовом проекте).
8. Пункт `t03-etcd-snapshot` отсутствует в `arch/roadmap/etcd.md`;
   `t04-etcd-api ← …, t03-etcd-snapshot` сохранён; других мутаций `arch/`
   нет.
9. Все решения §3 не противоречат arch/01 §1/§6/§8, arch/02, arch/03 §2
   (проверка на ревью).

## 17. Риски и заметки

- **Имена полей etcd gateway неконсистентны** (`mod_revision` vs `dbSize` vs
  `peerURLs`): DTO покрывают фактические proto-имена etcd 3.5.21; защита от
  регрессии — integration-тест `Gateway_Status_AgainstRealEtcd` и
  gateway-фикстуры из реальных ответов. Расхождение версий etcd (3.4 и
  старше) вне поддержки: стенд и прод — 3.5.21 (pg-report §1).
- **Числа int64 строками** (§3.17): любое новое поле gateway-DTO обязано
  читаться с `AllowReadingOfString`-опциями — зафиксировано в DTO-паттерне.
- **Один hosted-сервис на модуль**: refresher — singleton; `RefreshOnceAsync`
  публичен и идемпотентен по отношению к store (повторный вызов просто
  строит очередной снапшот) — тесты не нуждаются в `IHost`.
- **Статический кеш attribute-DI** (§3.15): ни unit, ни integration t03 не
  вызывают `UseDiBehaviours`/`AutoRegistration` — регистрации Program-хоста
  t04+ не пострадают.
- **PeriodicTimer и долгие тики**: тик короче таймаута 2 c; при системных
  задержках `PeriodicTimer` не накапливает пропуски (следующий тик — сразу
  после завершения просроченного) — наложений тиков нет (один цикл).
- **`localhost:1` как мёртвый endpoint** в тесте failover: connection
  refused наступает мгновенно, тест не флакает по таймауту.
- **Docker в CI обязателен** для integration-сборки (Testcontainers);
  unit-тесты Docker не требуют.
- **LEASE-ключи в сиде без lease** (§11.1): панель детерминирует живость по
  наличию ключа — тест эквивалентен прод-поведению; истекающие lease в
  тестах — YAGNI (поведение «ключа нет» покрывается отсутствием ключа).
- **`EtcdStatusPayload` vs `EtcdEndpoint`**: транспортная запись не содержит
  URL/латентности — их добавляет refresher, владеющий контекстом endpoint;
  смешивать транспорт и модель в одном типе нельзя (направление зависимостей
  arch/01 §1: Core не знает HTTP).
- **`header` ответов gateway игнорируется**: raft-данные статуса дублируются
  полями верхнего уровня; `member_id` из header не нужен — `leader` в
  status и `ID` в member/list достаточны.
