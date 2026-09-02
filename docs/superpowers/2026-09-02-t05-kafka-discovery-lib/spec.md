# Спецификация: t05 — HA.Kafka, клиентская библиотека дискавери kafka-кластеров из etcd (+ общий etcd-слой HA.Etcd)

> Dev-flow, фаза спецификации. Worktree: `feat-t05-kafka-discovery-lib`.
> Дата: 2026-09-02. Задача roadmap:
> [`arch/roadmap/kafkaworker.md`](../../../arch/roadmap/kafkaworker.md)
> `t05-kafka-discovery-lib`.

## 1. Цель

Две части одной задачи в репозитории **Puzzle** (не в pg):

1. **`PuzzleServer.Infrastructure.App.HA.Etcd`** — рефакторинг: etcd-слой
   (HTTP JSON gateway `/v3/*`), сегодня живущий внутри
   `PuzzleServer.Infrastructure.App.HA.Db` и прибитый к `HaDbOptions`,
   выносится в отдельный общий проект. Оба HA-модуля (HA.Db и новый HA.Kafka)
   ссылаются на него; дублирования транспорта нет.
2. **`PuzzleServer.Infrastructure.App.HA.Kafka`** — новая библиотека
   дискавери kafka-кластеров **по образцу ha-db**
   (`PuzzleServer.Infrastructure.App.HA.Db`, спека
   `docs/superpowers/2026-08-27-ha-db-etcd-clusters/spec.md`): watch-long-poll /
   poll, кэш снапшотов в памяти, событие изменения, форс-рефреш, fail-open,
   health. Библиотека:

   - получает при регистрации (флуент-заявки в коде) **имена kafka-кластеров**
     и из конфигурации **seed-адреса etcd** (HTTP JSON gateway `/v3/*`);
   - читает для каждого заявленного кластера контракт дискавери —
     `pg/arch/15-kafka-clusters.md` §5: `endpoints` (bootstrap-адреса),
     `app_user`/`app_password` (SASL/PLAIN-креды), реестр топиков
     (`topics/<T>`) и `config` (state кластера);
   - непрерывно актуализирует **иммутабельный снапшот** в фоне — режим
     обновления настраивается при запуске (`WatchLongPoll` по умолчанию /
     `Poll`, все интервалы — настройки);
   - отдаёт потребителю мгновенно из кэша (`Get`), стреляет событием
     `Updated` только при фактическом изменении содержимого, даёт
     `RefreshAsync` — форс-рефетч за один RTT;
   - выдаёт клиентские параметры kafka **plain-полями** (без зависимости от
     Confluent.Kafka): `bootstrap.servers`-строка, креды, строки
     `security.protocol`/`sasl.mechanisms` из контракта.

Потребитель снапшота (будущее, вне скоупа) — `PuzzleServer.Infrastructure.App.Kafka`
(Confluent-клиент): наполнение `KafkaOptions.BootstrapServers`/кредов и реакция
`IKafkaConfigChangeSource` на событие. Это выделено в roadmap-задачу
`t10-kafka-discovery-integration` (добавлена в
`arch/roadmap/kafkaworker.md` этой же правкой).

Источник истины по контракту — репозиторий PgWorker:
`pg/arch/15-kafka-clusters.md` §5 (точки дискавери), §6 (толерантность
читателей), §2/§2.1 (ключи и канонические примеры значений), §1 (транспорт).
Библиотека — **читатель**; KafkaWorker — писатель. Контракт §5–§6 признан
достаточным (решение user-review), правки arch/15 не требуются.

## 2. Принципы

1. **Только чтение.** Библиотека читает `/kafka/clusters/<C>/` одним
   префиксным range. Никаких put/txn/lease; префиксы `/kafkaworker/` и
   ключи `brokers/*` не читаются и не интерпретируются (попадая в range —
   пропускаются парсером молча, без unknownKeys-шума: это известные ключи
   контракта, просто не входящие в клиентское подмножество §5). Единственная
   «активность» — короткоживущие watch-стримы `/v3/watch` (режим WatchLongPoll),
   etcd-сторонних изменений не производящие.
2. **Параллельная структура с ha-db.** Режимы актуализации, шина
   (bootstrap → сигналы → рефетч), кэш-стор с событием, ротация endpoints,
   fail-open, health, толерантный парсинг — механика 1:1 из HA.Db; отличия
   только в домене (префикс, модель снапшота, парсер) и в источнике списка
   кластеров.
3. **Режим обновления — настройка, а не приговор.** `HaKafka:Mode` выбирается
   при запуске: `WatchLongPoll` (по умолчанию) или `Poll`; все параметры
   режимов — настройки. Общая шина едина для обоих режимов; режим определяет
   только источник сигнала «пора рефрешить».
4. **Событие → полный рефетч.** И watch-событие, и poll-тик заканчиваются
   одним и тем же: полный range по префиксу кластера → парсинг → атомарная
   замена снапшота. Инкрементальное применение событий сознательно не строим.
5. **Короткоживущие итерации = тривиальное восстановление.** Watch-стрим живёт
   ровно окно фиксированной длины (по умолчанию 1 с), потом переоткрывается;
   пропуски между окнами исключены `start_revision` (ревизия последнего
   снапшота); compaction → форс-рефетч и сброс ревизии.
6. **Устойчивость и fail-open.** etcd недоступен → последний снапшот живёт в
   кэше без ограничения времени (пока etcd лежит, контрол-плейн заморожен,
   адреса не протухают). Ретрай — следующая итерация окна/тика; ротация
   endpoints — по кругу.
7. **Pull + push.** `Get(cluster)` — мгновенно из кэша; `Updated` — только при
   фактическом изменении содержимого (value-equality, без
   `FetchedAtUtc`/`Revision`), отдельно на каждый кластер.
8. **Креды читаем, но не светим.** В отличие от админ-полей ha-db,
   `app_user`/`app_password` — рабочие креды приложения и читаются
   контрактом §5 п.2. В снапшот попадают **только полным набором обоих
   ключей** (неполный → `App = null`); пароль редацирован (`Password = ***`)
   в `ToString()` моделей и сообщениях ошибок — фиксируется тестами.
9. **Стиль Puzzle.** .NET 10, `Nullable=enable`, `TreatWarningsAsErrors=true`;
   `Result`-монада (никаких `throw` через границы модуля); attribute-driven DI;
   file-scoped namespaces; primary constructors; идентификаторы
   по-английски, комментарии/документация по-русски; версии пакетов —
   централизованно в `src/Directory.Packages.props` (новых внешних пакетов нет);
   тесты — AAA-комментарии.
10. **Толерантный парсинг** (§6): битый JSON значения → ключ пропускается,
    parseError-запись (лог warning), не исключение; неизвестные ключи внутри
    `/kafka/` — лог + счётчик `unknownKeys`; незнакомое `config.state` —
    raw-строкой; топик без части факт-полей — читается с null-полями.
11. **Один etcd-слой на решение.** Транспорт/ротация/node-discovery живут в
    общем `HA.Etcd` и не зависят от опций потребителей: таймауты и настройки
    передаются при регистрации каждым модулем из его собственных опций
    (конфиг-секция `HaDb` не меняется — обратная совместимость конфигурации
    HA.Db полная).

## 3. Контракт etcd (что читаем и как разбираем)

Транспорт — HTTP JSON gateway etcd v3 (`/v3/kv/range`, `/v3/watch`,
`/v3/cluster/member/list` для node discovery): base64-ключи, int64
decimal-строки, proto-имена без camelCase. Всё это — общий слой HA.Etcd
(§4.3), портированный из HA.Db.

Читаемый префикс — **один на кластер**: `/kafka/clusters/<C>/` (и range, и
watch-стрим). В отличие от HA.Db (два префикса: кластер + Patroni DCS), здесь
все точки дискавери лежат внутри одного префикса — один range даёт
консистентный срез ревизии, один watch ловит всё.

### 3.1. Ключи (в снапшот)

Сегменты пути — по `key.Split('/')` с ведущим пустым элементом
(`/kafka/clusters/<C>/config` → 5 сегментов; конвенция эталонного парсера
`AdminPanel.Etcd/Parsing/KafkaParser.cs`).

| Ключ (относительно `/kafka/clusters/<C>/`) | Значение | В снапшот |
|---|---|---|
| `config` | JSON `{"brokers":B,...,"state"?:"NOT_INITIALIZED"\|"TO_REMOVE"\|<незнакомое>}` (§2.1) | `State` — **raw-строка** `state` (отсутствие поля = Active); остальные поля конфига клиенту не нужны — не читаются |
| `endpoints` | plain `"h1:p1,h2:p2,..."` | `BootstrapServers` (строка как есть; ключа нет → `null`) |
| `app_user` | plain, напр. `"app"` | `App.User` — только вместе с `app_password` |
| `app_password` | plain, 32 симв | `App.Password` — только вместе с `app_user`; редакция в `ToString` |
| `topics/<T>` (6 сегментов) | JSON факт+desired (§3 arch/15) | `Topics`: имя, `partitions`, `replication_factor`, `configs` (строковый словарь) — только факт-часть; `desired*`-поля игнорируются |

Правила разбора:

- **Фильтрация leaf-ключей заявок**: `topics/<T>/desired.create` /
  `desired.delete` (7 сегментов) — не факт-ключи, пропускаются (§5 п.3).
- **`missing:true`** — топик фактически не существует (исчез из Kafka при
  живой заявке): в клиентский реестр **не входит** (реестр для клиента =
  реально существующие топики; desired/missing — панельная семантика).
- **`__`-префикс имени топика** — internal-топики в реестр не попадают вовсе
  (воркер их не кладёт); если всё же попал — пропуск без ошибки.
- `brokers/...` — известные ключи контракта вне клиентского подмножества:
  пропуск молча (не unknownKeys).
- Битый JSON `config`/`topics/<T>` — parseError + пропуск ключа; `config`
  битый → `State = null` (трактовка Active-ветки), кластер в снапшоте жив.
- `app_user`/`app_password` — plain-значения; пустое значение = валидное
  (пустой пароль), отсутствие ключа = секрета нет. Неполный набор → `App = null`.

### 3.2. Watch-стрим (режим WatchLongPoll)

Один watch — один префикс, **один стрим на кластер**: `/kafka/clusters/<C>/`.
Механика (окна, `start_revision`, `progress_notify`, compact-сброс, ошибки →
ротация) — 1:1 из HA.Db (спека ha-db §3.3, реализация `WatchLongPollSignaler`).

### 3.3. Node discovery (состав кластера etcd)

`EtcdMembersMonitor` из HA.Db переносится в HA.Etcd как есть: цикл
`POST /v3/cluster/member/list`, пул = union(seed ∪ clientURLs), sticky-ротация
отказов. Настройки (`MembersMode`, интервалы) остаются в опциях каждого
модуля (`HaDbOptions` — без изменений; `HaKafkaOptions` — свои поля тех же
имён/дефолтов); монитор инстанцируется per-module при регистрации.

## 4. Структура / компоненты

### 4.1. Проекты и зависимости

Два новых csproj в `src/` (оба в `src/PuzzleServer.Api.slnx`, папка
`/Infrastructure/`):

**`PuzzleServer.Infrastructure.App.HA.Etcd`** (рефакторинг-вынос из HA.Db):

- ProjectReference: `PuzzleServer.Infrastructure.App` (`Result`).
- PackageReference (версии уже в `Directory.Packages.props`):
  `Microsoft.Extensions.DependencyInjection.Abstractions`,
  `Microsoft.Extensions.Hosting.Abstractions` (монитор — `BackgroundService`),
  `Microsoft.Extensions.Logging.Abstractions`,
  `Microsoft.Extensions.Options`.
- `InternalsVisibleTo PuzzleServer.UnitTests`.
- Переносится из `HA.Db/Etcd/`: `IEtcdClient`, `EtcdHttpClient`,
  `EtcdKv`, `EtcdWatchEvent`+`WatchEventType`, `EtcdHttpException`,
  `EtcdMember`, `StreamJsonObjectsReader` (internal),
  `EtcdEndpointRotation`, `EtcdMembersMonitor`.
- Namespace: `PuzzleServer.Infrastructure.App.HA.Etcd`.
- **Отвязка от опций потребителей**: `EtcdHttpClient` больше не принимает
  `IOptions<HaDbOptions>` — таймаут range/member-list передаётся при
  конструкции (значение из опций регистрирующего модуля, через typed-client
  фабрику); watch — без таймаута (окно сигнальщика). `EtcdMembersMonitor`
  принимает настройки (режим, интервалы, endpoints) параметрами, а не опциями.

**`PuzzleServer.Infrastructure.App.HA.Kafka`**:

- ProjectReference: `PuzzleServer.Infrastructure.App` (Result, DI-атрибуты,
  `HealthCheckAbstract`/`IHealthCheckService`),
  `PuzzleServer.Infrastructure.App.HA.Etcd` (транспорт).
- PackageReference — тот же набор, что у HA.Db (Configuration(.Abstractions),
  DependencyInjection.Abstractions, Diagnostics.HealthChecks(.Abstractions),
  Hosting.Abstractions, Http, Logging.Abstractions, Options,
  Options.ConfigurationExtensions).
- `InternalsVisibleTo PuzzleServer.UnitTests`.
- Внешних пакетов kafka **нет** (принцип «plain, без Confluent»).

HA.Db после рефакторинга: ProjectReference на HA.Etcd, собственный `Etcd/`
каталог удаляется, регистрации в `AddHaDb` переключаются на общие типы;
конфиг-секция `HaDb` и публичный API (ITopologyStore, снапшоты, пресеты) не
меняются (перенос типов etcd-слоя в другую сборку — допустимое изменение
внутренних деталей: потребителей etcd-типов вне HA.Db нет).

### 4.2. Структура папок HA.Kafka

```
PuzzleServer.Infrastructure.App.HA.Kafka/
├─ Parsing/
│   └─ KafkaClusterParser.cs      // чистые функции IReadOnlyList<EtcdKv> → модель
├─ Model/
│   ├─ KafkaClusterSnapshot.cs    // иммутабельный снапшот кластера
│   ├─ KafkaTopicInfo.cs          // факт-топик: имя/partitions/RF/configs
│   ├─ KafkaAppSecret.cs          // app_user+app_password (редакция ToString)
│   └─ KafkaClientConfig.cs       // plain-параметры клиента (редакция ToString)
├─ KafkaDiscoveryStore.cs         // кэш + Updated + RefreshAsync
├─ KafkaDiscoveryRefresher.cs     // шина: bootstrap + сигналы → рефетч + health
├─ Refresh/                       // IRefreshSignaler + PollRefreshSignaler +
│                                 // WatchLongPollSignaler — паттерн 1:1 из HA.Db
├─ HaKafkaClusterRegistry.cs      // флуент-заявки кластеров (паттерн
│                                 // ConfigurationTopologyRegistry)
├─ HaKafkaOptions.cs              // конфигурация (Mode + параметры режимов)
├─ HaKafkaException.cs            // ошибки библиотеки для Result.Failed
├─ HaKafkaHealthCheck.cs          // HealthCheckAbstract<KafkaDiscoveryRefresher>
└─ ModuleExtensions.cs            // AddHaKafka(configuration).AddKafkaCluster(...)
```

Сигнальщики `Refresh/` не копируются файл-в-файл из HA.Db механически, а
следуют их паттерну (интерфейс `IRefreshSignaler`, канал сигналов, окна
watch): реализация на общем `IEtcdClient` из HA.Etcd; при дословном
совпадении логики — осознанное дублирование, допустимое ради независимости
модулей (в HA.Etcd выносится только транспорт, не логика актуализации).

### 4.3. Парсер

`KafkaClusterParser` — чистие статические функции (тестируются без сети),
по образцу `TopologyParser` из HA.Db и с семантикой эталонного
`AdminPanel.Etcd/Parsing/KafkaParser.cs`:

- `Parse(string cluster, IReadOnlyList<EtcdKv> kvs, out IReadOnlyList<string> parseErrors)`
  — один префикс `/kafka/clusters/<C>/`: config (State raw-строкой), endpoints,
  app_user+app_password, topics (факт-поля; фильтры §3.1).
- Толерантность: битый JSON → parseError + пропуск; `configs` топика —
  отсутствует → `null`; числа `partitions`/`replication_factor` —
  `int?` (строки-числа допускаются, `AllowReadingFromString`); топики
  сортируются по имени (детерминизм value-equality).
- `parseErrors` — в лог warning агрегатом за проход (как HA.Db), в снапшот
  не попадают.

### 4.4. Модель снапшота

Все типы — `sealed record` с value-equality (условие корректности события
`Updated`: переписывание ключей теми же значениями событие не даёт).

```csharp
public sealed record KafkaClusterSnapshot(
    string Cluster,
    string? State,                     // raw config.state; null = Active (§6)
    string? BootstrapServers,          // endpoints как есть ("h1:p1,h2:p2"); null = ключа нет
    KafkaAppSecret? App,               // полный набор app_user+app_password; иначе null
    IReadOnlyList<KafkaTopicInfo> Topics, // по возрастанию имени
    DateTimeOffset FetchedAtUtc,
    long Revision);                    // max(mod_revision) — start_revision для watch

public sealed record KafkaTopicInfo(
    string Name,
    int? Partitions,                   // факт; null = поля нет (битый/неполный)
    int? ReplicationFactor,
    IReadOnlyDictionary<string, string>? Configs); // фактические управляемые конфиги

public sealed record KafkaAppSecret(string User, string Password)
{
    public override string ToString(); // Password = *** (редакция)
}

public sealed record KafkaClientConfig(
    string BootstrapServers,
    string SecurityProtocol,           // "SASL_PLAINTEXT" (контракт §5 п.2)
    string SaslMechanism,              // "PLAIN"
    string SaslUsername,
    string SaslPassword)
{
    public override string ToString(); // SaslPassword = *** (редакция)
}
```

Вычислители на снапшоте (без похода в etcd):

```csharp
public KafkaClientConfig? GetClientConfig();  // null: нет endpoints ИЛИ нет App
public KafkaTopicInfo? FindTopic(string name);
public IReadOnlyList<string> TopicNames { get; } // имена по возрастанию
public bool HasAppSecret { get; }              // App != null
```

Клиентские строки `security.protocol`/`sasl.mechanisms` — константы
контракта (SASL_PLAINTEXT/PLAIN), значения полей — строки: потребитель
передаёт их в свой клиент без привязки к типам Confluent.

### 4.5. `KafkaDiscoveryStore` — кэш + событие + форс-рефреш

`[InjectAsSingleton]`, потокобезопасен; контракт зеркален `ITopologyStore`
HA.Db:

```csharp
public interface IKafkaDiscoveryStore
{
    // Мгновенно из кэша, НИКАКОГО etcd. Failed: кластер не заявлен | снапшот ещё не собран.
    Result<KafkaClusterSnapshot> Get(string cluster);

    // Стреляет ТОЛЬКО при фактическом изменении содержимого (value-equality без
    // FetchedAtUtc/Revision), на каждый кластер; исключение подписчика гасится логом.
    event Action<KafkaClusterSnapshot>? Updated;

    // Немедленный рефетч (один range по префиксу → парсинг → атомарная замена);
    // SemaphoreSlim(1,1); провал etcd → Failed, кэш не трогаем (fail-open).
    Task<Result<KafkaClusterSnapshot>> RefreshAsync(string cluster, CancellationToken ct);
}
```

Один проход = **один range на одном endpoint** (консистентность ревизии),
ротация при отказе; `Revision` нового снапшота — `start_revision` следующего
watch-окна.

### 4.6. Актуализация: шина + сигнальщики

Общая шина `KafkaDiscoveryRefresher` (`[InjectAsSingleton]`,
`BackgroundService` + `IHealthCheckService`) — 1:1 из HA.Db
(`TopologyRefresher`):

- **Bootstrap** (`StartAsync`): немедленный рефетч всех заявленных кластеров,
  бюджет `BootstrapTimeoutSec` (15 с); провал не роняет старт приложения.
- **Цикл** (`ExecuteAsync`): сигналы `IRefreshSignaler` → рефетч всех
  кластеров; коалесценция пачки сигналов за время прохода — один проход;
  ретраев внутри прохода нет (следующий сигнал/тик — ретрай).
- **Health**: `Inited` (хотя бы один успешный рефетч), `Working` (успех за
  последние 3 интервала режима), `StatusError` (Failed при ≥2 отказах подряд;
  рефетч- и watch-сбои суммируются).

Сигнальщики — паттерн HA.Db:

- `PollRefreshSignaler` — `PeriodicTimer(PollIntervalMs)` → сигнал.
- `WatchLongPollSignaler` — цикл коротких окон: на каждый заявленный кластер
  один стрим `/kafka/clusters/<C>/`; первое содержательное событие → сигнал →
  досрочное закрытие окна; `Compacted` → сигнал + сброс ревизии;
  истечение окна → пауза `WatchReopenDelayMs` → новое окно; транспортная
  ошибка → `ReportFailure` + пауза `WatchErrorDelayMs` → другой endpoint.

### 4.7. Конфигурация

Секция `"HaKafka"` (Bind + валидация при старте: непустые endpoints,
положительные интервалы; **кластеры в секции НЕ задаются** — только заявками
в коде):

```csharp
public enum HaKafkaRefreshMode { WatchLongPoll, Poll }

public class HaKafkaOptions
{
    public HaKafkaRefreshMode Mode { get; set; } = HaKafkaRefreshMode.WatchLongPoll;
    public string[] EtcdEndpoints { get; set; } = [];   // seed "http://host:2379"
    public int RequestTimeoutMs { get; set; } = 2000;
    // --- WatchLongPoll ---
    public int WatchWindowMs { get; set; } = 1000;
    public int WatchReopenDelayMs { get; set; } = 100;
    public int WatchErrorDelayMs { get; set; } = 1000;
    // --- Poll ---
    public int PollIntervalMs { get; set; } = 1000;
    // --- Members (node discovery) ---
    public HaKafkaMembersMode MembersMode { get; set; } = HaKafkaMembersMode.Poll;
    public int MembersPollIntervalMs { get; set; } = 30_000;
    public int MembersMinIntervalMs { get; set; } = 1_000;
    // --- Общее ---
    public int BootstrapTimeoutSec { get; set; } = 15;
    // Наполняется PostConfigure из реестра заявок (AddKafkaCluster):
    public string[] Clusters { get; set; } = [];
}
```

`HaKafkaMembersMode` — свой enum (Poll/OnFailure/Off), значения и дефолты как
у `HaDbMembersMode`.

### 4.8. `ModuleExtensions` — флуент-заявки

Паттерн `AddConfigurationTopology` (реестр заявок, наполнение `Clusters`
через PostConfigure):

```csharp
// Первый вызов регистрирует модуль: опции, typed HttpClient /v3/* (на общем
// EtcdHttpClient из HA.Etcd, таймаут из HaKafka:RequestTimeoutMs), ротация,
// сигнальщик по Mode, store+refresher (AutoRegistration сборки), health-check
// "HaKafkaCheck", members-монитор (Off → нет в контейнере).
services.AddHaKafka(configuration);

// КАЖДЫЙ вызов добавляет заявку-кластер. Fail-fast: модуль не зарегистрирован,
// пустое имя, невалидный формат (^[a-z][a-z0-9_]{0,62}$ — arch/15), дубликат.
// PostConfigure HaKafkaOptions наполняет Clusters из реестра; пустой набор —
// fail-fast при старте («ни один kafka-кластер не заявлен»).
services.AddKafkaCluster("events");
```

Использование:

```csharp
services.AddHaKafka(configuration)
        .AddKafkaCluster("events");
```

Модуль **не подключается** в `Api/Program.cs` на этой фазе (как HA.Db на фазе
1): в Aspire-стенде Puzzle нет etcd — hosted-сервис ронял бы старт;
подключение — задача `t10-kafka-discovery-integration`. Миграций нет.

### 4.9. Health check

`HaKafkaHealthCheck(KafkaDiscoveryRefresher) : HealthCheckAbstract<...>` —
пустой класс-наследник (паттерн HaDbCheck); регистрация
`AddCheck<HaKafkaHealthCheck>("HaKafkaCheck")`.

## 5. Фазы реализации

Каждая фаза — самостоятельный коммит; тесты пишутся вместе с кодом (TDD, AAA).

1. **Ф1 — HA.Etcd: вынос общего etcd-слоя.** csproj `HA.Etcd` (+ в
   `.slnx`), перенос `Etcd/*` из HA.Db (namespace `...HA.Etcd`), отвязка
   `EtcdHttpClient` от `IOptions<HaDbOptions>` (таймаут при конструкции),
   параметры монитора, HA.Db → ProjectReference + удаления локального `Etcd/`.
   Unit-тесты etcd-слоя переезжают `UnitTests/HA/Db/` →
   `UnitTests/HA/Etcd/`; сборка и все существующие тесты зелёные; поведение
   HA.Db не меняется (интеграционные тесты HA/Db — контроль).
2. **Ф2 — каркас и модель HA.Kafka.** csproj, `.slnx`, модель (снапшот,
   KafkaTopicInfo, KafkaAppSecret, KafkaClientConfig + редакция ToString,
   вычислители), HaKafkaException. Unit-тесты модели/редакции пароля.
3. **Ф3 — парсер.** `KafkaClusterParser`. Unit-тесты: канонические примеры
   arch/15 §2.1 (`topics/orders` с desired, `topics/ghost` missing,
   Active-config без state), фильтрация desired.create/delete, `__`-топики,
   битые JSON, brokers-ключи молча, unknownKeys, State raw.
4. **Ф4 — шина актуализации.** `HaKafkaClusterRegistry` + `AddHaKafka`/
   `AddKafkaCluster` (fail-fast-ветки), `HaKafkaOptions` (+PostConfigure из
   реестра), `KafkaDiscoveryStore`, `KafkaDiscoveryRefresher`,
   `PollRefreshSignaler`, health, members-режимы. Unit-тесты на fake
   `IEtcdClient`: bootstrap, fail-open, коалесценция, событие только при
   изменении, RefreshAsync, выбор сигнальщика по Mode, валидация заявок.
5. **Ф5 — режим WatchLongPoll.** `WatchLongPollSignaler` (один стрим на
   кластер, окна/`start_revision`/compact/ротация — на общем watch-клиенте).
   Unit-тесты сигнальщика на fake: событие → сигнал, таймаут окна →
   переоткрытие с `start_revision`, compact → сброс ревизии, обрыв →
   ротация + пауза.
6. **Ф6 — интеграционные тесты.** Testcontainers etcd
   (`quay.io/coreos/etcd:v3.5`, fixture по образцу
   `IntegrationTests/HA/Db/EtcdClusterFixture`), засев `/kafka/...` через
   HTTP gateway. Сценарии в обоих режимах (матрица): сборка снапшота;
   изменение endpoints → снапшот + событие (WatchLongPoll ≤ ~2 с,
   Poll ≤ PollInterval+RTT); ротация app_password (put NEW) → событие →
   `GetClientConfig()` с новым паролем; смерть etcd → кэш живёт; возврат →
   восстановление; lease-подобный put тем же значением — событие НЕ стреляет.
7. **Ф7 — документация.** `docs/01.18-ha-etcd.md` (общий etcd-слой:
   транспорт, ротация, node discovery, использование модулями),
   `docs/01.19-ha-kafka.md` (библиотека: режимы, настройка, снапшот),
   строки в таблице `docs/01-infrastructure.md`, правка `docs/01.17-ha-db.md`
   (etcd-слой вынесен → ссылка на 01.18).

## 6. Ограничения и принятые решения

- **Скоуп = библиотека + общий etcd-слой.** Интеграция с
  `Infrastructure.App.Kafka` (bootstrap/креды из снапшота, Aspire-ветка) —
  roadmap `t10-kafka-discovery-integration` (arch/roadmap/kafkaworker.md,
  добавлена этой задачей); подключение в `Api/Program.cs` — там же.
- **Общий проект HA.Etcd** — решение user-review (вместо дубля или ссылки на
  HA.Db): транспорт один, логика актуализации остаётся у модулей. Перенос
  публичных etcd-типов в другую сборку — внутреннее изменение HA.Db
  (внешних потребителей нет); конфиг-секция `HaDb` не меняется.
- **Кластеры — заявки в коде** (решение user-review: «как в последней ревизии
  с pg — набор кластеров указывается при регистрации»), не конфиг-секция;
  пережиток `HaKafka:Clusters` в конфиге игнорируется.
- **Plain-модель без Confluent.Kafka** (решение user-review): параллель
  «ha-db не тянет Npgsql».
- **Контракт arch/15 §5–§6 достаточен** (решение user-review): неполный набор
  кредов → `App = null`, State raw, фильтрация заявок — трактуются по образцу
  pg и фиксируются этой спекой, arch/15 не правится.
- **`desired`/`missing` не входят в клиентский реестр**: реестр = факт
  (клиент выбирает реально существующий топик); `missing:true` → топик вне
  реестра. `brokers/*` клиенту не нужны — пропуск молча.
- **Креды приложения читаются** (в отличие от админ-полей ha-db): это
  контрактная точка дискавери §5 п.2; неполный набор → `App = null`;
  `KafkaClientConfig.ToString()`/`KafkaAppSecret.ToString()` редацируют
  пароль — тестами зафиксировано.
- **Мониторы members per-module**: при сосуществовании HA.Db и HA.Kafka в
  одном процессе каждый держит свой `EtcdMembersMonitor` (read-only
  member/list раз в 30 с — дёшево); объединение в один — YAGNI.
- **Out of scope:** AdminClient-операции (создание топиков и т.п.);
  consumer-группы/лаги; TLS/SCRAM (появятся в контракте — t03-kafka-security
  в pg — тогда же поля в модели); метрики (только health + логи); gRPC-транспорт;
  вычисление партиционирования продюсера (домен приложения).
- Эталон толерантности парсинга — `AdminPanel.Etcd/Parsing/KafkaParser.cs`
  (pg); канонические значения — arch/15 §2.1 (критерий приёмки парсеров).

## 7. Тестирование

- **Unit** (`PuzzleServer.UnitTests/HA/Kafka/`, Docker не нужен): модель и
  редакция пароля; парсер (канон §2.1, фильтры, битые данные, AAA);
  `KafkaDiscoveryStore` на fake `IEtcdClient` (событие, fail-open,
  RefreshAsync, коалесценция); `KafkaDiscoveryRefresher` (bootstrap-таймаут,
  health); `PollRefreshSignaler`; `WatchLongPollSignaler` на fake; заявки
  `AddKafkaCluster` (fail-fast-ветки, PostConfigure-наполнение); выбор
  сигнальщика по Mode.
- **Unit HA.Etcd** (`UnitTests/HA/Etcd/` — переезд существующих):
  `EtcdHttpClient` range/watch/members (против fake HttpMessageHandler),
  `StreamJsonObjectsReader`, `EtcdEndpointRotation`, `EtcdMembersMonitor`.
- **Integration** (`PuzzleServer.IntegrationTests/HA/Kafka/`, нужен Docker):
  Testcontainers etcd — сценарии Ф6.
- Сборка/тесты: `dotnet build src/PuzzleServer.Api.slnx` (0 warnings),
  `dotnet test src/PuzzleServer.Api.slnx`; после Ф1 — полная зелёность
  существующих тестов HA/Db (контроль не-регрессии рефакторинга).

## 8. Критерии приёмки

1. `dotnet build` решения без warnings; `dotnet test` зелёный (unit — без
   Docker, integration — с Docker); поведение HA.Db после Ф1 не изменилось
   (интеграционные тесты HA/Db зелёные, конфиг-секция HaDb работает).
2. Проекты `HA.Etcd` и `HA.Kafka` в `src/` и `.slnx` (папка `/Infrastructure/`);
   HA.Kafka не ссылается на HA.Db и не содержит копии etcd-транспорта.
3. `services.AddHaKafka(configuration).AddKafkaCluster("events")` регистрирует
   модуль и заявку; fail-fast: пустое имя, невалидный формат имени, дубликат,
   заявка без `AddHaKafka`, старт без единой заявки.
4. `Get(cluster)` возвращает снапшот **без сетевых вызовов** (тест с
   fake-клиентом: после bootstrap `Get` не дёргает клиент); незаявленный
   кластер → Failed.
5. Снапшот содержит: `State` raw (нет поля = null/Active),
   `BootstrapServers` из endpoints, `App` только при полном наборе
   `app_user`+`app_password`, `Topics` — факт-поля по возрастанию имён;
   `GetClientConfig()` → `bootstrap.servers` + `SASL_PLAINTEXT`/`PLAIN` +
   креды; null при отсутствии endpoints или секрета.
6. Парсер: канонические значения arch/15 §2.1 разбираются (критерий приёмки
   парсеров); `desired.create`/`desired.delete` leaf-ключи и `missing:true`-
   топики в реестр не попадают; `brokers/*` пропускаются молча; битый JSON →
   parseError-запись, парсер не падает; незнакомое `state` — raw-строкой.
7. Режимы — настройка: `HaKafka:Mode=WatchLongPoll|Poll` выбирает сигнальщик
   (unit-тест регистрации); все интервалы читаются из конфигурации.
8. Актуальность WatchLongPoll: изменение `endpoints` в etcd доставляется
   ≤ ~2 с (интеграционный порог); пропуски исключены (`start_revision`);
   compact → полный рефетч; put тем же значением событие `Updated` НЕ стреляет.
9. Актуальность Poll: то же изменение ≤ `PollIntervalMs` + RTT; событие
   стреляет. Ротация `app_password` (put NEW) → событие → новый пароль в
   `GetClientConfig()` (сценарий §16-H фазы B — интеграционный тест).
10. Fail-open (оба режима): при недоступности всех endpoints `Get` отдаёт
    последний снапшот; `RefreshAsync` → Failed; health деградирует; возврат
    etcd — восстановление первым окном/тиком.
11. Библиотека использует только чтение: `/v3/kv/range` и `/v3/watch` по
    префиксу `/kafka/clusters/<C>/` (+ `/v3/cluster/member/list` в
    members-режимах); интеграционный тест фиксирует список запросов прохода;
    ничего не пишет; `app_password` не светится в логах/`ToString`
    (редакция `***` — тестами).
12. Документация: `docs/01.18-ha-etcd.md`, `docs/01.19-ha-kafka.md` созданы,
    `docs/01-infrastructure.md` содержит строки-ссылки, `docs/01.17-ha-db.md`
    ссылается на общий etcd-слой.

## 9. Открытые вопросы

Нет — ключевые решения приняты user-review: общий проект HA.Etcd (вместо
дубля/ссылки), имя `Infrastructure.App.HA.Kafka`, скоуп «только библиотека»
(интеграция — roadmap `t10-kafka-discovery-integration`, добавлена),
plain-модель без Confluent, кластеры — заявки при регистрации
(`AddHaKafka(...).AddKafkaCluster(...)`), контракт arch/15 §5–§6 достаточен
(без правок).
