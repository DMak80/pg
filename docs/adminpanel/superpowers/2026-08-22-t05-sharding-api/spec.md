# Спецификация t05-sharding-api — API шардирования и алерты кластеров/бакетов

Дата: 2026-08-22. Фаза dev-flow: spec. Источники истины:
`arch/roadmap/sharding.md` (пункт `t05-sharding-api` — объём),
`arch/03-panels.md` (ГЛАВНЫЙ документ задачи: эндпоинты §1, DTO §2,
каталог алертов §4, панели §3), `arch/02-etcd-contract.md` §2.1 (ключи
`/clusters/`, семантика master-lease и status-ключей), §3 (модель
снапшота), `arch/01-architecture.md` §1 (направление зависимостей),
§6 (секция настроек `AdminPanel:Alerts`). Фактическое состояние кода —
t04 (`src/`): `InspectionModule` (overview/etcd-status/alerts),
`OverviewMapper` с пустыми заглушками кластерной части, каркас
`AlertEngine` + 7 etcd-правил, `SnapshotRefresher` с оценкой алертов на
обоих путях тика, auth-guard, integration-фабрика `"api"` с
`TestSnapshotStore`.

## 1. Цель

Кластерная часть инспекции поверх уже собранного снапшота (данные
`ClusterInfo`/`ShardInfo`/`BucketInfo`/`HealRecord` — t03):

1. **Эндпоинты** `GET /api/clusters` (сводный список) и
   `GET /api/clusters/{cluster}` (детали: config, шарды с dsn и
   master+leaseAlive, бакеты с фильтрами `?owner=&state=`, журнал heals).
2. **Наполнение кластерной части `GET /api/overview`** — заглушки t04
   (`clusters[]`, `activeMoves[]`) получают реальные значения из
   снапшота; сами DTO-типы (`OverviewClusterDto`, `OverviewMoveDto`) не
   меняются.
3. **8 правил алертов шардирования** (03 §4): `shard-no-master`,
   `move-stale`, `move-frozen-long`, `move-aborting`,
   `move-flipped-status-stuck`, `bucket-lost`, `bucket-no-routing`,
   `bucket-out-of-range` — новыми классами `IAlertRule` в каркасе t04
   **без правок `AlertEngine`/`IAlertEngine`/`SnapshotRefresher`**
   (правила подхватываются DI автоматически).
4. **Секция настроек `AdminPanel:Alerts`** — POCO `AlertsOptions` с
   порогами `StaleMoveSeconds` (600) и `FrozenSeconds` (60) из
   arch/01 §6 (завещано t04 §3.6).

Тесты: unit — правила на снапшот-фикстурах (протухший lease, зависший
FROZEN, routing в никуда, дыра карты), мапперы DTO, хендлеры (503/404);
integration — HTTP-контракт новых эндпоинтов (401/503/200/404/400/
фильтры) через фабрику `"api"` с `TestSnapshotStore`, путь данных
«живой Testcontainers-etcd с сидом аномалий → refresher → AlertEngine →
API отдаёт данные и алерты».

Новых NuGet-пакетов нет.

## 2. Принципы

- Источник истины — `arch/`; всё, что arch/ не оговаривает, решено
  минимальным способом и зафиксировано в §3. Расхождение с arch/
  запрещено (SPEC_DEVIATION).
- Идентификаторы — английские; комментарии в коде — русские. Тексты
  `message` алертов — русские (прецедент t04 §2).
- Тесты — xunit v3 + FluentAssertions, комментарии по AAA
  (`// Arrange` / `// Act` / `// Assert`), на русском.
- Паттерны t01–t04 обязательны: attribute-DI, query-ветка CQRS
  (`IQuery<T>`/`IQueryHandler`/`IHandler.HandleQuery`), `Result`-монада,
  файл запроса = query + DTO + статический mapper + handler,
  unit-тесты без хоста, integration — один Program-хост на процесс.
- API не ходит в etcd на запрос: только чтение `ISnapshotStore`.
- Мутации `arch/01–04` запрещены; из `arch/roadmap/` меняется только
  `sharding.md` — удаление пункта t05 (§14).

## 3. Решения в рамках контракта arch/ (уточнения неоднозначностей)

1. **Пагинации нет.** arch/03 §1: «всё сразу; N ≤ тысяч — грид
   фильтруется на клиенте». Серверная пагинация/bucket-поиск не
   заводятся; фильтры `?owner=&state=` — единственная серверная
   фильтрация (ниже §3.9).
2. **DTO сводного списка.** arch/03 §2 определяет только детальный
   `ClusterDto`; для `GET /api/clusters` (§1 «список кластеров
   (сводный)») заводится `ClusterSummaryDto` с полями по UI-таблице
   (03 §3): имя, dbname, N, шард мастеровых/всего, активные переезды,
   пометка incomplete. `createdUnix` в сводный список не входит (в
   UI-таблице его нет; YAGNI) — он в детальном `ClusterDto` (03 §2).
3. **Сортировки DTO** (arch не оговаривает; парсер t03 уже отдаёт
   кластеры по `Name` Ordinal, шарды по `Name`, бакеты по `Id`):
   мапперы **сохраняют порядок снапшота**; единственная новая
   сортировка — `heals` по `TsUnix` по убыванию (журнал: новые сверху;
   `null`-штампы — в конец, при равенстве — стабильный порядок), и
   `activeMoves` в Overview: кластеры в порядке снапшота, внутри —
   по `bucket Id` по возрастанию.
4. **`OverviewClusterDto.buckets` = `ClusterInfo.BucketsCount`** —
   константа N из config (0 у incomplete), а не `Buckets.Count`
   (фактическое число routing-ключей, которое может включать
   out-of-range и «дыры»). Сводка показывает декларированный размер
   карты; фактические расхождения видны в деталях и алертах
   `bucket-*`.
5. **`masterlessShards` = число шардов с `MasterAddress == null`**
   (computed `MasterLeaseAlive` = false). Семантика lease: ключа нет =
   lease протух (arch/02 §1) — шард «без мастера».
6. **`activeMoves`** (Overview) — все бакеты всех кластеров с
   `State != Active` (SYNCING/FROZEN/ABORTING): `{cluster, bucket,
   state, owner, target, updatedUnix}`, где `owner`/`target`/
   `updatedUnix` — из `BucketInfo.Move` (nullable-поля модели,
   переносятся как есть). Название «active» — устоявшийся термин
   каталога (`activeMoves` в 03 §2); в него входят и ABORTING (переезд
   не завершён).
7. **`ageSec` не-ACTIVE бакета** = `now − (Move.UpdatedUnix ??
   Move.StartedUnix)`, целые секунды (`long?`); `null` — если бакет
   ACTIVE или оба штампа отсутствуют (битые данные такого рода уже
   видны как `key-malformed`). База «updated, при отсутствии —
   started» зафиксирована roadmap'ом («возраста переездов из
   `updated_unix`»; `started` — толерантный fallback для старых
   записей). Формула живёт в одном месте — публичный хелпер Core
   `MoveAge.Seconds(BucketInfo, nowUnix)` (§4.4): его используют
   правила `move-stale`/`move-frozen-long`/`move-aborting` (details) и
   `ClusterDetailsMapper` — формула алертов и UI не разойдутся
   (прецедент: порог 3×interval в t04 оставлен двумя строками; здесь
   формула длиннее и имеет двух потребителей).
8. **Представление `state`** — строки верхнего регистра канона
   статус-ключей (02 §2.1): `"ACTIVE"|"SYNCING"|"FROZEN"|"ABORTING"`
   (продолжение решения t04 §3.11). Общий хелпер Api
   `BucketStates.Name(BucketState)` + `TryParse(string?)`.
9. **Фильтры `?owner=&state=` на `/api/clusters/{cluster}`** (03 §1):
   - `state` — строго `ACTIVE|SYNCING|FROZEN|ABORTING` (roadmap:
     «`?state=` принимает ACTIVE тоже»); иное значение → 400
     ProblemDetails (прецедент валидации `?severity=` t04 §3.13);
   - `owner` — свободная строка точного сравнения с `BucketInfo.Owner`;
     неизвестный владелец → 200 `[]` (не 400 — имена шардов эволюци-
     руют, прецедент `?kind=`);
   - фильтруется только массив `buckets` (arch: «возвращают
     отфильтрованный `buckets`»); `shards`/`heals` всегда полные;
   - оба фильтра сочетаются (AND); без фильтров — все бакеты.
10. **404 для неизвестного кластера.** `GET /api/clusters/{cluster}` с
    именем, которого нет в снапшоте → 404 ProblemDetails
    (`title: "Cluster not found"`). Arch поведения не задаёт; 404
    точнее семантики 503/200-пусто. Реализация: хендлер возвращает
    `Result.Failed(new ClusterNotFoundException(cluster))`; эндпоинт
    различает отказы: `ClusterNotFoundException` → 404, прочее → 503
    (как t04 §3.12). Имя кластера дополнительно не валидируется
    (обычный lookup по `Name`).
11. **Пороги `AdminPanel:Alerts` — только два ключа t05.**
    `AlertsOptions { StaleMoveSeconds = 600, FrozenSeconds = 60 }`
    (arch/01 §6). `ReplicaLagBytes` не заводится — он нужен t06
    (YAGNI, прецедент t04 §3.6). Пороги передаются правилам через
    конструктор `IOptions<AlertsOptions>` (DI): каркас `AlertEngine`,
    `AlertContext` и `SnapshotRefresher` не меняются — выполнено
    обещание t04 §3.2 «t05/t06 добавляют правила без правки
    двигателя». `IOptions`-снимок при старте достаточен: пороги меня-
    ются редко, панель — один процесс. Толерантность к опечаткам
    конфига: значение `<= 0` → каталогный дефолт (константы правил
    600/60), как `RefreshIntervalSeconds` в t03 §3.3.
12. **Условия правил — по букве каталога 03 §4, независимы.** В
    частности: `move-stale` — любой не-ACTIVE статус старше
    `StaleMoveSeconds` (включая FROZEN и ABORTING), `move-frozen-long`
    — FROZEN старше `FrozenSeconds`. Долгий FROZEN (60 c < возраст <
    ∞) законно даёт оба алерта: kind'ы несут разную семантику
    («cutover завис» — critical против «переезд без прогресса» —
    warning); каталог не задаёт исключений, искусственная маскировка
    была бы SPEC_DEVIATION.
13. **`bucket-no-routing` не срабатывает у incomplete-кластеров**:
    диапазон `0..N-1` при `BucketsCount = 0` пуст (сам кластер уже
    алертится `cluster-incomplete` t04). `bucket-out-of-range`,
    аналогично, только при `BucketsCount > 0` и только для бакетов с
    routing (`Owner != null`) — каталог: «routing-ключ с `N ≥ buckets`»;
    голый out-of-range status-ключ без routing редчайший выродок,
    отдельных kind'ов каталог для него не имеет.
14. **`runtime` в `ShardDto`** (03 §2 `runtime{…}(nullable)`): в t05
    всегда `null` (живой источник — SQL-пробы t06; в модели
    `ShardInfo.Runtime` тоже всегда null), но DTO-тип `ShardRuntimeDto`
    заводится сразу с полным составом полей по 03 §2 (фронтенд t08
    типизирует сразу — прецедент заглушек Overview t04 §3.15), и
    маппер `Runtime → ShardRuntimeDto` пишется сейчас по стабильной
    модели t03 (`standbiesSync` = standbies с `SyncState in
    ("sync","quorum")`, `slotsLagMaxBytes` = max `LagBytes` слотов,
    `walStatusLost` = имена слотов с `WalStatus == "lost"`,
    `subscriptions` = имена, `bucketSchemas`, `error`). Unit-тест
    маппера — на фикстуре модели; данных до t06 нет ни у кого.
15. **Новые правила ломают часть существующих ассертов «чистого сида»
    — это ожидаемо и правится в t05.** Сид demo (EtcdSeed/фикстуры
    t03) содержит три статус-ключа с фиксированными штампами
    2025-08; на любую дату прогона (≥ 2026) их возраст > порогов →
    сид даёт ровно 5 move-алертов: `move-stale` × 3 (bucket_3 SYNCING,
    bucket_7 ABORTING, bucket_11 FROZEN) + `move-frozen-long`
    (bucket_11) + `move-aborting` (bucket_7). Детерминировано (штампы
    фиксированы в прошлом) — правки ассертов перечислены в §9.4/§10.4.
    Сид НЕ меняется: те же значения обязаны совпадать с dev-стендом
    (t03 §3.16).
16. **Харнессы правил пополняются списком t05.** `AlertTestRules.All()`
    (unit) и `EtcdTestHarness.NewRefresher` (integration) включают
    новые 8 правил; двум пороговым передают
    `Options.Create(new AlertsOptions())` (дефолты каталога).
17. **`GET /api/alerts` и `GET /api/overview` не меняются**: новые
    kind'ы автоматически появляются в ленте и счётчиках — эндпоинт
    t04 отдаёт все алерты снапшота (t04 §3.1). Правок `AlertsQuery`/
    `AlertsMapper`/`EtcdStatusQuery` нет.

## 4. Правила алертов шардирования (AdminPanel.Core/Alerting/Rules/)

### 4.1. Общий вид правила t05

Stateless-класс, `[InjectAsSingleton(typeof(IAlertRule))]` —
регистрация автосканом `AddCore()` (t04 §4.1), `Kind` — константа
каталога, условие ложно → пустой перечиситель. Пороговые правила
(`MoveStaleRule`, `MoveFrozenLongRule`) принимают в конструкторе
`IOptions<AlertsOptions>`; DI отдаёт снимок секции
`AdminPanel:Alerts`. Формат алерта, стабильные id `kind:target`,
`sinceUnix`, сортировка — механика `AlertEngine` t04 (без правок).

### 4.2. Таблица правил (условия — по модели t03)

| Класс | Kind | Severity | Условие | Target | По одному на |
|---|---|---|---|---|---|
| `ShardNoMasterRule` | `shard-no-master` | Critical | `Dsn` непуст и `MasterAddress == null` (P11: lease протух) | `{cluster}/{shard}` | шард |
| `MoveStaleRule` | `move-stale` | Warning | `State != Active` и `MoveAge.Seconds > StaleMoveSeconds` | `{cluster}/bucket_{id}` | не-ACTIVE бакет |
| `MoveFrozenLongRule` | `move-frozen-long` | Critical | `State == Frozen` и `MoveAge.Seconds > FrozenSeconds` | `{cluster}/bucket_{id}` | FROZEN бакет |
| `MoveAbortingRule` | `move-aborting` | Warning | `State == Aborting` (безусловно, P7) | `{cluster}/bucket_{id}` | ABORTING бакет |
| `MoveFlippedStatusStuckRule` | `move-flipped-status-stuck` | Warning | `State != Active` и `Move?.Target != null` и `Owner == Move.Target` (P7) | `{cluster}/bucket_{id}` | бакет |
| `BucketLostRule` | `bucket-lost` | Critical | `Owner != null` и среди `cluster.Shards` нет `Name == Owner` (P23-а) | `{cluster}/bucket_{id}` | бакет |
| `BucketNoRoutingRule` | `bucket-no-routing` | Warning | `Owner == null` и `0 <= Id < BucketsCount` (дыра карты) | `{cluster}/bucket_{id}` | бакет |
| `BucketOutOfRangeRule` | `bucket-out-of-range` | Warning | `Owner != null` и `BucketsCount > 0` и `Id >= BucketsCount` (P18) | `{cluster}/bucket_{id}` | бакет |

`MoveAge.Seconds`-база (§3.7) null (оба штампа отсутствуют) →
`move-stale`/`move-frozen-long` пропускают бакет (нет меры возраста);
`move-aborting` безусловен и в details отдаёт доступный возраст.

### 4.3. Сообщения и details (фиксируются; ключи camelCase, инвариант)

| Kind | Message (рус.) | Details |
|---|---|---|
| `shard-no-master` | `шард {cluster}/{shard} без master-ключа (lease протух или писателя нет)` | `cluster`, `shard`, `dsn` |
| `move-stale` | `переезд bucket_{id} кластера {cluster} ({state}) без прогресса {age} c — порог {t} c` | `state`, `ageSeconds`, `thresholdSeconds`, `updatedUnix` |
| `move-frozen-long` | `бакет bucket_{id} кластера {cluster} в FROZEN {age} c — cutover обязан быть секундами` | `ageSeconds`, `thresholdSeconds`, `updatedUnix` |
| `move-aborting` | `бакет bucket_{id} кластера {cluster} в ABORTING — незавершённая уборка` | `phase`, `lastError`, `ageSeconds` (если вычислим), `updatedUnix` |
| `move-flipped-status-stuck` | `routing бакета bucket_{id} кластера {cluster} уже = target {target}, но статус {state} не снят` | `owner`, `target`, `state` |
| `bucket-lost` | `routing бакета bucket_{id} кластера {cluster} указывает на несуществующий шард {owner}` | `owner` |
| `bucket-no-routing` | `бакет {id} кластера {cluster} из диапазона 0..{N-1} без routing-ключа (дыра карты)` | `bucketId`, `bucketsCount` |
| `bucket-out-of-range` | `routing-ключ bucket_{id} кластера {cluster} вне диапазона 0..{N-1}` | `bucketId`, `bucketsCount` |

`updatedUnix` в details — фактическая база возраста (`UpdatedUnix ??
StartedUnix`, строка; отсутствует — ключ не пишется). `state` — строки
канона §3.8. Отсутствующие nullable-поля (например `phase`) в details
не попадают.

### 4.4. Хелпер возраста (AdminPanel.Core/MoveAge.cs)

```csharp
// Возраст не-ACTIVE статуса бакета: now − (updated_unix ?? started_unix).
// null — бакет ACTIVE или оба штампа отсутствуют (битые данные видит key-malformed).
public static class MoveAge
{
    public static long? Seconds(BucketInfo bucket, long nowUnix);
}
```

Используют `MoveStaleRule`/`MoveFrozenLongRule`/`MoveAbortingRule` и
`ClusterDetailsMapper` (§6.2) — единая формула алертов и UI.

### 4.5. Настройки (AdminPanel.Core/Alerting/AlertsOptions.cs)

```csharp
// [Config]-POCO порогов алертов: секция AdminPanel:Alerts (arch/01 §6; t04 §3.6).
[Config("AdminPanel:Alerts")]
public class AlertsOptions
{
    // move-stale: не-ACTIVE статус без прогресса дольше N секунд (каталог 03 §4).
    public int StaleMoveSeconds { get; set; } = 600;

    // move-frozen-long: FROZEN дольше N секунд (каталог 03 §4).
    public int FrozenSeconds { get; set; } = 60;
}
```

Регистрация — автоскан `AddCore()` (`[Config]` → `Configure<AlertsOptions>`).
`appsettings.json`: секция `AdminPanel:Alerts` добавляется с дефолтами
600/60 — самодокументирование контракта (прецедент явной секции
`AdminPanel:Etcd`). Фолбэк `<= 0` → дефолт каталога (§3.11) —
константы `MoveStaleRule.DefaultSeconds = 600`,
`MoveFrozenLongRule.DefaultSeconds = 60`.

## 5. Что НЕ меняется в движке

- `IAlertEngine`, `AlertEngine`, `IAlertRule`, `AlertContext` — без
  правок (t04): новые правила подхватываются `IEnumerable<IAlertRule>`
  автоматически.
- `SnapshotRefresher` — без правок: оценка алертов на обоих путях тика
  уже на месте (t04 §5); правила t05 начинают работать после
  пересборки DI-графа без единой строки в refresher'е.
- `AlertsQuery`/`AlertsMapper`/`EtcdStatusQuery` — без правок (§3.17).
- `ClustersParser`/`DsnParser` — без правок: multi-host DSN уже
  разобран в `ShardInfo.DsnHosts` (t03), out-of-range бакеты уже в
  `Buckets` (t03 §6.6).

## 6. API-эндпоинты (AdminPanel.Api/Inspection/)

### 6.1. InspectionModule [правка]

Новые маршруты (auth-guard уже закрывает всё `/api/*`):

```csharp
// GET /api/clusters — сводный список (arch/03 §1).
endpoints.MapGet("/api/clusters", …HandleQuery<ClustersQuery, IReadOnlyList<ClusterSummaryDto>>);

// GET /api/clusters/{cluster}?owner=&state= — детали (arch/03 §1);
// state строго ACTIVE|SYNCING|FROZEN|ABORTING, иначе 400 (spec §3.9);
// отказ ClusterNotFoundException → 404, прочий → 503 (spec §3.10).
endpoints.MapGet("/api/clusters/{cluster}", (string cluster, string? owner, string? state, …) =>
{
    if (state is not null && !BucketStates.TryParse(state, out var parsed))
        return Results.Problem(statusCode: 400, title: "Invalid state",
            detail: $"state должен быть ACTIVE|SYNCING|FROZEN|ABORTING, получено: {state}");
    // … HandleQuery<ClusterDetailsQuery, ClusterDto>(new(cluster, owner, parsed)) →
    // успех 200; ClusterNotFoundException → Problem 404 "Cluster not found"; прочее → 503.
});
```

`ClusterNotFoundException` и хелпер `BucketStates` — публичные типы
модуля (используются тестами и Overview-маппером). Маппинг результата
деталей — локальный в лямбде (отличие 404/503 от общего `ResultToHttp`).
`Program.cs` не меняется: `MapInspectionApi()` уже вызывается.

### 6.2. DTO и мапперы (camelCase JSON; §3.8, t04 §3.11)

```csharp
// ClustersQuery.cs — сводный список.
public sealed record ClustersQuery : IQuery<IReadOnlyList<ClusterSummaryDto>>;

// Поля — UI-таблица Clusters (arch/03 §3); dbname null у incomplete (spec §3.2).
public sealed record ClusterSummaryDto(
    string Name, string? DbName, int BucketsCount, bool Incomplete,
    int ShardsTotal, int ShardsWithMaster, int ActiveMoves);

public static class ClustersMapper
{
    // Чистая функция: ShardsTotal/WithMaster/ActiveMoves — счётчики по модели;
    // порядок кластеров — как в снапшоте (spec §3.3).
    public static IReadOnlyList<ClusterSummaryDto> Map(IReadOnlyList<ClusterInfo> clusters);
}

[InjectAsScoped]
public sealed class ClustersQueryHandler(ISnapshotStore store)
    : IQueryHandler<ClustersQuery, IReadOnlyList<ClusterSummaryDto>>;
// store.Current == null → Failed(SnapshotNotReadyException) → 503 (t04 §3.12).
```

```csharp
// ClusterDetailsQuery.cs — детализация кластера.
public sealed record ClusterDetailsQuery(string Cluster, string? Owner, BucketState? State)
    : IQuery<ClusterDto>;

public sealed record ClusterDto(
    string Name, string? DbName, int BucketsCount, long? CreatedUnix, bool Incomplete,
    IReadOnlyList<ShardDto> Shards, IReadOnlyList<BucketDto> Buckets, IReadOnlyList<HealDto> Heals);

// arch/03 §2; masterLeaseAlive — computed t03; runtime — nullable, в t05 всегда
// null (данные — SQL-пробы t06), маппинг по стабильной модели — spec §3.14.
public sealed record ShardDto(
    string Name, string Dsn, IReadOnlyList<string> Hosts, int? ReplicasDeclared,
    string? MasterAddress, bool MasterLeaseAlive, ShardRuntimeDto? Runtime);

public sealed record ShardRuntimeDto(
    int? StandbiesSync, long? SlotsLagMaxBytes, IReadOnlyList<string> WalStatusLost,
    IReadOnlyList<string> Subscriptions, IReadOnlyList<string> BucketSchemas, string? Error);

// state — строка канона (ACTIVE|SYNCING|FROZEN|ABORTING); move/ageSec — null у ACTIVE.
public sealed record BucketDto(int Id, string? Owner, string State, MoveDto? Move, long? AgeSec);

public sealed record MoveDto(
    string? Owner, string? Target, long? StartedUnix, long? UpdatedUnix,
    string? Phase, string? LastError);

public sealed record HealDto(string Bucket, string? Was, string? Now, string? Reason, long? TsUnix);

// state-строки канона (spec §3.8) — общий источник для мапперов и валидации query.
public static class BucketStates
{
    public static string Name(BucketState state);          // Active → "ACTIVE", …
    public static bool TryParse(string? text, out BucketState state);
}

public static class ClusterDetailsMapper
{
    // Чистая функция: DTO кластера + фильтр buckets (owner точное совпадение,
    // state по enum; §3.9) + ageSec через MoveAge (§3.7) + heals по TsUnix desc (§3.3).
    public static ClusterDto Map(ClusterInfo cluster, long nowUnix, string? owner, BucketState? state);

    // Маппинг runtime — по стабильной модели t03 (spec §3.14); вызывается при Runtime != null.
    public static ShardRuntimeDto MapRuntime(ShardRuntime runtime);
}

[InjectAsScoped]
public sealed class ClusterDetailsQueryHandler(ISnapshotStore store, TimeProvider time)
    : IQueryHandler<ClusterDetailsQuery, ClusterDto>;
// null-снапшот → 503; кластер не найден → Failed(ClusterNotFoundException) → 404 (§3.10).
```

```csharp
// OverviewQuery.cs [правка — только тело OverviewMapper.Map]:
clusters:   snapshot.Clusters → OverviewClusterDto(Name, Shards.Count, BucketsCount,
            Count(State != Active), Count(MasterAddress == null))            (§3.4–3.5)
activeMoves: все не-ACTIVE бакеты в порядке кластеров снапшота, внутри по Id  (§3.6):
            OverviewMoveDto(cluster.Name, b.Id, BucketStates.Name(b.State),
            b.Move?.Owner, b.Move?.Target, b.Move?.UpdatedUnix)
```

DTO-типы `OverviewClusterDto`/`OverviewMoveDto` и хендлер — без правок.

### 6.3. Сводка контракта HTTP (сверка с arch/03 §1)

| Метод+путь | Auth | Успех | Отказ |
|---|---|---|---|
| `GET /api/clusters` | cookie | 200 `ClusterSummaryDto[]` | 401 без cookie; 503 ProblemDetails до первого тика |
| `GET /api/clusters/{cluster}` | cookie | 200 `ClusterDto` (`?owner=&state=` фильтруют `buckets`, §3.9) | 401; 503; **404** ProblemDetails `Cluster not found` для неизвестного имени; **400** ProblemDetails при невалидном `state` |

Проблемные ответы — `application/problem+json` (паттерн t02/t04).

## 7. Состав изменений (дерево файлов)

```
src/AdminPanel.Core/
├── MoveAge.cs                                [новый] возраст не-ACTIVE статуса (§4.4)
├── Alerting/
│   ├── AlertsOptions.cs                      [новый] [Config("AdminPanel:Alerts")]: 600/60 (§4.5)
│   └── Rules/
│       ├── ShardNoMasterRule.cs              [новый] critical, нет master-ключа при dsn
│       ├── MoveStaleRule.cs                  [новый] warning, не-ACTIVE старше StaleMoveSeconds
│       ├── MoveFrozenLongRule.cs             [новый] critical, FROZEN старше FrozenSeconds
│       ├── MoveAbortingRule.cs               [новый] warning, ABORTING безусловно
│       ├── MoveFlippedStatusStuckRule.cs     [новый] warning, routing == target при статусе
│       ├── BucketLostRule.cs                 [новый] critical, routing в несуществующий шард
│       ├── BucketNoRoutingRule.cs            [новый] warning, дыра карты 0..N-1
│       └── BucketOutOfRangeRule.cs           [новый] warning, routing с id >= N
src/AdminPanel.Api/
├── appsettings.json                          [правка] + AdminPanel:Alerts { 600, 60 }
└── Inspection/
    ├── InspectionModule.cs                   [правка] + 2 маршрута, state-валидация,
    │                                                 ClusterNotFoundException, 404/503-маппинг
    ├── OverviewQuery.cs                      [правка] OverviewMapper: кластеры/переезды (§6.2)
    ├── ClustersQuery.cs                      [новый] список: query+dto+mapper+handler
    └── ClusterDetailsQuery.cs                [новый] детали: query+dto+BucketStates+mapper+handler
src/tests/AdminPanel.UnitTests/
├── AlertTestRules.cs                         [правка] + 8 правил t05 (пороговые — с Options)
├── ShardingAlertRulesTests.cs               [новый] 8 правил на снапшот-фикстурах (§10.1)
├── ClustersMappersTests.cs                   [новый] Clusters/ClusterDetails/фильтры/ageSec/
│                                             runtime-маппинг (§10.2)
├── InspectionMappersTests.cs                 [правка] OverviewMapper: наполнение кластерной
│                                             части (stub-тест t04 заменяется, §10.3)
├── InspectionQueryHandlerTests.cs            [правка] + кластерные хендлеры: 503/404 (§10.3)
├── SnapshotRefresherTests.cs                 [правка] ассерты Alerts demo-фикстуры (§10.4)
└── TestSnapshots.cs                          [правка] + кластер с переездами/аномалиями (§10.5)
src/tests/AdminPanel.IntegrationTests/
├── InspectionApiTests.cs                     [правка] кластерная фикстура + ассерты Overview
│                                             (§9.1); LiveEtcd-ассерты алертов (§9.3, §3.15)
├── ClustersApiTests.cs                       [новый] HTTP-контракт кластерных эндпоинтов (§9.2)
└── EtcdSnapshotIntegrationTests.cs           [правка] NewRefresher: + правила t05; ассерты
│                                             Alerts сида demo (§9.3, §3.15)
arch/roadmap/sharding.md                      [правка] удалить пункт t05-sharding-api (§14)
```

`Program.cs`, `SnapshotRefresher.cs`, `AlertEngine.cs`, парсеры,
`AdminPanel.Etcd`/`Infrastructure`/`Probes`, `Directory.Packages.props`,
`.slnx` — без изменений.

## 8. Интеграция и настройки

- DI: правила и `AlertsOptions` регистрирует автоскан `AddCore()`
  (уже вызван); хендлеры — `AddApi()`; никаких ручных регистраций.
- `appsettings.json`: + `"Alerts": { "StaleMoveSeconds": 600,
  "FrozenSeconds": 60 }` внутри `AdminPanel` (§4.5). Integration-
  фабрика настроек не меняет (дефолты POCO).
- OpenAPI: новые GET попадают в схему автоматически (t04 §11).

## 9. Integration-тесты (src/tests/AdminPanel.IntegrationTests/)

Коллекция `"api"` (`AuthWebFactory` + `TestSnapshotStore` — без правок
фабрики). Кластерная HTTP-фикстура — расширение `InspectionSnapshots`:
кластер `demo` (2 шарда: s1 с master, s2 без master; бакеты: routing
s1/s2, bucket_1 SYNCING owner s1→s2 (updated = now−30 c), bucket_2
FROZEN (now−10 c), bucket_3 ABORTING (now−5 c, last_error), bucket_4
без routing — дыра; heal-запись), плюс отдельная фикстура без
кластеров для 404/пустых кейсов.

### 9.1. Правки `InspectionApiTests`

- `Overview_WithSnapshot_ReturnsDto` — фикстура с кластером:
  `clusters[0] = {name:"demo", shards:2, buckets:16, activeMoves:3,
  masterlessShards:1}`; `activeMoves` — 3 записи с state/owner/target/
  updatedUnix (порядок по bucket id). Существующие etcd-ассерты
  остаются.
- `Overview_StaleSnapshot_StaleTrue`, 401/503-тесты — без правок
  (503-тест дополняется `/api/clusters` и `/api/clusters/demo`).

### 9.2. Новые `ClustersApiTests` [Collection("api")]

- `Clusters_WithoutCookie_Return401` — оба пути без cookie → 401.
- `Clusters_NoSnapshot_Return503ProblemDetails` — оба → 503
  `Snapshot not ready`.
- `Clusters_WithSnapshot_ReturnSummaries` — сводный список: счётчики
  кластера фикстуры, `incomplete=false`.
- `ClusterDetails_ReturnsConfigShardsBucketsHeals` — dbname,
  bucketsCount, createdUnix, shards (dsn, hosts[2], replicasDeclared,
  masterAddress, masterLeaseAlive true/false, runtime null), buckets
  все, heals журнал.
- `ClusterDetails_AgeSec_ForNonActiveBuckets` — SYNCING 30/FROZEN 10/
  ABORTING 5; ACTIVE-бакеты `move=null`, `ageSec=null`.
- `ClusterDetails_OwnerFilter_ReturnsOnlyMatching` — `?owner=s1`.
- `ClusterDetails_StateFilter_Active Included` — `?state=ACTIVE` →
  только ACTIVE; `?state=SYNCING` → 1; `?state=FROZEN&owner=s2` →
  пересечение.
- `ClusterDetails_UnknownOwner_ReturnsEmptyBuckets` — 200, `buckets:[]`
  (остальные блоки полные).
- `ClusterDetails_UnknownCluster_Returns404ProblemDetails`.
- `ClusterDetails_InvalidState_Returns400ProblemDetails` — `?state=bogus`.
- `Clusters_IncompleteCluster_Flagged` — фикстура с ghost-кластером:
  `incomplete=true`, dbname null.

### 9.3. Путь данных «живой etcd → API» (правки)

`EtcdTestHarness.NewRefresher` — список правил пополняется 8 правилами
t05 (§3.16). Затем:

- `EtcdSnapshotIntegrationTests.Refresher_RefreshOnce_BuildsExpectedSnapshot`
  — ассерт `Alerts` меняется с `BeEmpty` на точный состав сида demo
  (§3.15): ровно 5 move-алертов — `move-stale:demo/bucket_3`,
  `move-stale:demo/bucket_7`, `move-stale:demo/bucket_11`,
  `move-frozen-long:demo/bucket_11` (Critical), `move-aborting:demo/
  bucket_7`; сортировка severity→kind→target проверяется составом и
  первым элементом (`move-frozen-long`).
- `InspectionApiTests.InspectionEtcdApiTests.LiveEtcd_…` — ассерт
  `/api/alerts` с `0` на 5 kind'ов move-* (состав §3.15); добавляется
  смоук `GET /api/clusters` (1 кластер, `shardsWithMaster=2`) и
  `GET /api/clusters/demo` (16 бакетов, bucket_3 `SYNCING` с `move`).
- `InspectionSeededAnomaliesApiTests.LiveEtcd_SeededAnomalies_…` —
  ожидание уточняется: 8 алертов = 5 move-* (сид demo) +
  `shard-no-master:ghost/g1` (dsn-шард ghost без master — живое
  покрытие P11-правила; сид не сужается) + `cluster-incomplete:ghost`
  + `key-malformed:/clusters/demo/buckets/status/bucket_1`; порядок
  critical→warning kind Ordinal (`move-frozen-long` <
  `shard-no-master`, затем `cluster-incomplete` < `key-malformed` <
  `move-aborting` < `move-stale`).

### 9.4. Контейнерные фикстуры

`EtcdContainerFixture` — без правок. Мутирующий сценарий сид аномалий —
как и в t04, в классе со своим контейнером (порядок тестов не
гарантирован).

## 10. Unit-тесты (src/tests/AdminPanel.UnitTests/)

Время — `FixedTimeProvider` (unix-эквивалент `now`); пороговые правила
конструируются с `Options.Create(new AlertsOptions { … })` для
проверки границы.

### 10.1. `ShardingAlertRulesTests` [новый]

- `ShardNoMaster_MissingMasterWithDsn_Critical` — `MasterAddress=null`
  при непустом dsn → Critical, target `demo/s1`, details `dsn`;
  мастер есть → нет алерта.
- `ShardNoMaster_IgnoredWhenNoDsn` — шард с пустым dsn без мастера не
  алертится (нет писателя — нет ожидания lease).
- `MoveStale_OlderThanThreshold_Warning` — SYNCING c
  `updated = now−601` → Warning (порог 600), details ageSeconds/
  thresholdSeconds; `now−599` → нет.
- `MoveStale_CustomThreshold_FromOptions` — `StaleMoveSeconds=5`,
  `now−6` → есть (порог реально из настроек).
- `MoveStale_FallsBackToStartedUnix` — `updated` отсутствует,
  `started = now−700` → есть.
- `MoveStale_NoTimestamps_Skipped` — оба штампа null → алерта нет
  (нет меры; §4.2).
- `MoveFrozenLong_FrozenOlderThan60s_Critical` — `now−61` → Critical;
  `now−59` → нет.
- `MoveAborting_AnyAborting_Warning` — свежий ABORTING → Warning,
  details phase/lastError.
- `MoveFlipped_RoutingEqualsTarget_Warning` — SYNCING, `owner == target`
  → Warning; `owner != target` → нет.
- `BucketLost_OwnerUnknownShard_Critical` — routing `s9` → Critical,
  details owner; routing на существующий шард → нет.
- `BucketNoRouting_HoleInRange_Warning` — бакет 5 из 0..15 без routing →
  Warning; вне диапазона без routing → нет; incomplete (N=0) → нет.
- `BucketOutOfRange_RoutingBeyondN_Warning` — routing `bucket_99` при
  N=16 → Warning, details bucketsCount; в диапазоне → нет; N=0 → нет.
- `MoveRules_OnAllClusters` — аномалии в двух кластерах → по алерту
  на каждый (target содержит имя кластера).
- Сквозной сценарий roadmap «протухший lease + зависший FROZEN +
  routing в никуда + дыра карты» на одном снапшоте через полный
  `AlertEngine` с правилами t04+t05: 3 Critical (`shard-no-master`,
  `move-frozen-long`, `bucket-lost`) + 1 Warning (`bucket-no-routing`)
  в детерминированной сортировке, `sinceUnix` переносится от previous.

### 10.2. `ClustersMappersTests` [новый]

- `ClustersMapper_CountsShardsMastersMoves` — 2 шарда (1 с master),
  3 не-ACTIVE → `ShardsTotal=2, ShardsWithMaster=1, ActiveMoves=3`.
- `ClustersMapper_IncompleteFlagAndNullDbName`.
- `ClusterDetailsMapper_FullDto` — config/shards/buckets/heals;
  ACTIVE-бакет: `move=null, ageSec=null`; не-ACTIVE: move-поля
  переносятся, `ageSec` из MoveAge; heals по `tsUnix` desc.
- `ClusterDetailsMapper_Filters` — owner/state/оба/null (все бакеты).
- `ClusterDetailsMapper_RuntimeMapped_WhenPresent` — фикстура
  `ShardRuntime` (слоты c lag/`lost`, standby sync/quorum, подписки,
  схемы) → `ShardRuntimeDto` поля; `Runtime=null` → `runtime=null`.
- `BucketStates_RoundTrip` — enum↔строки канона; `TryParse("bogus")`
  → false.

### 10.3. Правки `InspectionMappersTests`/`InspectionQueryHandlerTests`

- `OverviewMapper_ClusterStubs_Empty` (t04) заменяется на
  `OverviewMapper_ClustersAndMovesFilled` — healthy-снапшот:
  `clusters=[{demo, shards=1, buckets=16, activeMoves=0,
  masterlessShards=0}]`, `activeMoves=[]`; отдельный кейс с
  переездами (state-строки канона, порядок по Id).
- `OverviewMapper_EtcdParts_Unchanged` — счётчики etcd/алертов те же
  (регресс-защита правки маппера).
- Хендлеры напрямую (без DI): `ClustersQueryHandler` — 503 без
  снапшота / сводки с снапшотом; `ClusterDetailsQueryHandler` — 503
  без снапшота; `Failed` с `ClusterNotFoundException` для неизвестного
  имени; успех с фильтрами.

### 10.4. Правки `SnapshotRefresherTests`

- `Refresh_AlertsStoredOnSuccessTick` — ассерт «единственный алерт —
  key-malformed» расширяется: `key-malformed` на месте **плюс** 5
  move-алертов фикстуры demo (§3.15); формулировка через
  `Contain`-цепочку.
- `Refresh_AlertsComputedOnFailTick` — ассерты не меняются: фикстура
  сеет ghost с dsn-шардом (`/clusters/ghost/shards/g1/dsn` без
  master) → в списке появляется и `shard-no-master:ghost/g1`, но
  ассерты `Contain`/`Single(a => a.Kind == "cluster-incomplete")`
  инвариантны к составу соседних kind'ов (выборка по kind, не по
  индексу/счётчику); перенос sinceUnix проверяется как раньше.
- Прочие кейсы refresher'а — без правок (харнесс берёт правила из
  пополняемого `AlertTestRules.All()`).

### 10.5. `TestSnapshots` [правка]

- `MovingCluster(DateTimeOffset now)` — кластер demo: 2 шарда (s1 c
  master, s2 без), бакеты 0..15 (routing s1/s2, у 4 — дыра), 3
  статус-ключа с `updated_unix` относительно `now` (SYNCING −30 c,
  FROZEN −10 c, ABORTING −5 c с lastError), heal-запись; базис для
  правил/мапперов/HTTP-фикстур (модификации через `with`).
- `Healthy` не меняется (совместимость t04-тестов).

## 11. Ограничения (что НЕ делается)

- HA-часть (`/api/ha*`, `shard-no-leader`, `ha-member-not-streaming`,
  `replica-lag-high`, `slot-*`, `sync-standby-missing`,
  `inventory-mismatch`, `probe-failed`, live-пробы, `ReplicaLagBytes`)
  — t06. Правила t06 — так же новые классы `IAlertRule`.
- Наполнение `ShardRuntime` реальными данными (SQL-пробы) — t06; t05
  фиксирует только DTO-контракт и маппинг (§3.14).
- HA-сводка Overview («скольки scope'ов без лидера») — вне контракта
  `OverviewDto` (03 §2 поля не содержит) — не заводится.
- Серверная пагинация/поиск/сортировка бакетов — нет (§3.1);
  кластеризация массовых алертов — нет (t04 §11).
- Фронтенд-панели — t07/t08; «Стендовая топология» на странице
  кластера — блок фронта над `StandNodes` снапшота (в t05 API не
  расширяется — 03 §2 не требует).
- История алертов, mute/ack, watch/push — нет (03 §2, 02 §5).
- Мутации `arch/01–04` запрещены; roadmap — только удаление пункта
  t05 (§14).

## 12. Пакеты

Новых пакетов нет. CPM не меняется; используемое уже в решении:
`Microsoft.Extensions.*` (Options/DI), ASP.NET Core Minimal API, xunit
v3 + FluentAssertions, Testcontainers, Mvc.Testing.

## 13. Настройки тестовых проектов

Без изменений: ссылки проектов расставлены (t04); новых JSON-фикстур
нет (кластерные фикстуры — код `TestSnapshots`/`InspectionSnapshots`).

## 14. Деливерабл roadmap

Тем же мерж-коммитом удалить пункт `t05-sharding-api` из
`arch/roadmap/sharding.md` (файл останется с шапкой трека — других
пунктов в нём нет; удаляется только пункт списка). Зависимости
`← t05-sharding-api` (`arch/roadmap/frontend.md`, t08) НЕ трогаются —
указание координатора, прецедент t04 §14: зависимость чистится
задачей-владельцем. Правка выполняется в ветке задачи до мержа.

## 15. Критерии приёмки

1. `dotnet build src/AdminPanel.slnx` — успех, 0 warnings
   (`TreatWarningsAsErrors=true` не подавлен).
2. `dotnet test src/AdminPanel.slnx` — все тесты зелёные (нужен
   Docker: Testcontainers-etcd; unit — без Docker).
3. Unit: 8 правил t05 (включая границы порогов, fallback started,
   пропуск без штампов, incomplete-исключения), мапперы (список,
   детали, фильтры, ageSec, runtime), Overview-наполнение, хендлеры
   503/404 — покрыты (§10).
4. Integration: 401/503/200/404/400/фильтры кластерных эндпоинтов;
   Overview-кластерная часть; путь данных живой etcd → refresher →
   AlertEngine → `/api/clusters*` + `/api/alerts` отдаёт 5 move-
   алертов сида demo и посеянные аномалии (§9).
5. `AlertEngine`/`IAlertEngine`/`AlertContext`/`SnapshotRefresher`
   не изменялись (diff пуст) — правило t04 §3.2 выполнено.
6. Auth не ослаблен: новые эндпоинты под guard (401-смоук §9.2).
7. `grep PackageReference` по csproj: изменений нет (§12).
8. Панель по-прежнему не пишет в etcd (`kv/put` — только тесты).
9. Пункт `t05-sharding-api` отсутствует в
   `arch/roadmap/sharding.md`; `← t05-sharding-api` в frontend.md
   сохранён; других мутаций `arch/` нет.
10. Все решения §3 не противоречат arch/01 §1/§6, arch/02 §2.1/§3,
    arch/03 §1–§4 (проверка на ревью).

## 16. Риски и заметки

- **Ломка ассертов «чистого сида»** (§3.15) — осознанная: 4
  существующих ассерта правятся в t05 тем же коммитом; альтернатива
  (чистить статусы из сида) нарушила бы совпадение с dev-стендом
  (t03 §3.16) и лишила бы integration живой проверки move-правил.
- **Пороговые правила зависят от `IOptions`-снимка**: изменение
  `AdminPanel:Alerts` без рестарта не подхватится — фиксируется здесь;
  живой `IOptionsMonitor` — если понадобится (t06+), отдельным
  решением.
- **`bucket-no-routing` vs статус без routing**: бакет с status-ключом
  и дырой в routing алертится `bucket-no-routing` — корректно
  (авторитет расположения — routing, 02 §2.1); move-правила при этом
  продолжают работать по своим условиям.
- **`activeMoves` в ABORTING-бакете** — термин каталога, не «здоровый
  переезд»: панель показывает именно незавершённые процессы, включая
  уборку (§3.6).
- **Сортировка `heals` на маппере**, а не в парсере: снапшот хранит
  порядок etcd (range-лексикографический) — целостнее не менять
  снапшот, сортировать проекцию (§3.3).
- **decimal-строки** здесь не нужны (unix-секунды/id бакетов малы) —
  числа отдаются number (t04 §3.11 применяется только к uint64
  member-id).
- **`IntegrationTests` коллизия по `factory.Snapshot`** — коллекция
  сериализована; каждый тест ставит снапшот в Arrange (прецедент
  t04 §16).
- **Правила-конструкторы с `IOptions`** удлиняют харнессы
  (`new MoveStaleRule(Options.Create(new AlertsOptions()))`) — цена
  проверяемости порогов; безпороговые правила остаются
  беспараметрическими.
