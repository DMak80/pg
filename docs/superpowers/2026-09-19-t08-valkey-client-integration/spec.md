# t08-valkey-client-integration — спецификация

> Дата: 2026-09-19. Репозиторий кода: **Puzzle** (`/Users/demakaev/ZCodeProject/Puzzle`,
> ветка `feat-t08-valkey-client-integration` в рабочей копии). Спека — pg-worktree
> `feat-t08-valkey-client-integration`. Задача roadmap pg:
> `arch/roadmap/valkey.md` → `t08-valkey-client-integration`.

## 1. Цель

Создать в Puzzle клиентский модуль Valkey **`PuzzleServer.Infrastructure.App.Valkey`** —
доменные кеш-абстракции поверх **StackExchange.Redis** с интеграцией дискавери
**HA.Valkey** (t04, слита): в HaDb-режиме модуль регистрирует
`AddHaValkey(...).AddValkeyCluster(<Valkey:Cluster>)`, соединительные параметры
(endpoints + ACL-креды app) берёт из снапшота `GetClientConfig()` HA.Valkey, ротация
`app_password`/смена endpoints доставляется событием `Updated` (hot-reload без
рестарта приложения), без параметров — fail-open.

Решения пользователя (зафиксированы вопросами фазы 1):

| Вопрос | Решение |
|---|---|
| Скоуп (клиентского модуля Valkey в Puzzle не существовало) | **Полный модуль + интеграция одной задачей t08** — модуль с кеш-абстракциями для доменов создаётся здесь |
| Доменная поверхность | **Свой `IValkeyCache`** (Result-монада, инкапсуляция StackExchange.Redis — единственная ссылка на пакет в кодовой базе; канон Puzzle по образцу `Infrastructure.App.Kafka`) |
| Состав операций v1 | **Расширенный набор**: базовые Get/Set/Remove/Exists + атомарные GetSet/Add-if-absent + инкременты + массовые MGet/MSet + TTL-управление |
| Per-модульные префиксы ключей | **Да**: generic-фабрика `IValkeyCacheBuilder<TConfig>` по образцу Kafka (`TConfig : ValkeyConfig` c `KeyPrefix`) |
| Git-режим Puzzle | **Ветка в рабочей копии** `feat-t08-valkey-client-integration`; коммиты свободно; мерж в main — по отдельному явному запросу (как t04) |
| Staged-файл `src/global.json` в индексе Puzzle | **Не трогать** — не включать в коммиты t08 |

## 2. Контекст и предпосылки

- **t04 слита** (Puzzle `ace1cf8`): `PuzzleServer.Infrastructure.App.HA.Valkey` —
  дискавери-библиотека (канон `docs/01.21-ha-valkey.md` в Puzzle): стор
  `IValkeyDiscoveryStore` (`Get` мгновенно из кэша / `RefreshAsync` / событие
  `Updated` по SameContent), `ValkeyClusterSnapshot.GetClientConfig()` →
  `ValkeyClientConfig(Endpoints, Username, Password, Ssl=false)` — plain-поля без
  зависимости от StackExchange.Redis; null при отсутствии endpoints ИЛИ неполном
  секрете. Требование roadmap «когда клиентский модуль Valkey появится в Puzzle»
  закрыто решением пользователя: модуль создаётся самой t08.
- **Образец интеграции — t10 (Kafka, слита)**: шов `IKafkaConnectionProvider`
  (`Current` + `OnChange`), Configuration-провайдер (Aspire) /
  Discovery-провайдер (HaDb), один переключатель `Database:Source`
  (`Aspire` \| `HaDb`, дефолт HaDb; `DatabaseSourceReader` в
  `Infrastructure.App/DB`), `Kafka:Cluster` fail-fast в HaDb, fail-open
  (`Current == null` — операции `Result.Failed`, старт не роняется),
  hot-reload ротации. Канон — `docs/01.16-kafka.md` §1a в Puzzle.
- **Контракт pg/arch НЕ меняется** (arch-first соблюдён): клиентский дискавери
  зафиксирован `arch/20-valkey-clusters.md` §4 (t04 его уже реализовала);
  t08 — только потребитель. Ключи: `/valkey/clusters/<C>/endpoints`,
  `app_user`, `app_password`; `ssl=false` в v1 (TLS — t06; поле `Ssl`
  маппится в `ConfigurationOptions.Ssl` — переделка при t06 не потребуется).
- Никаких потребителей кеша в Puzzle ещё нет — модуль создаёт инфраструктурную
  поверхность для будущих доменов (решение пользователя: полный модуль сейчас).

## 3. Принципы

1. **Образец 1:1 — Kafka t10**: архитектура шва, ветвление, fail-open,
   тест-стратегия копируют `Infrastructure.App.Kafka` / docs/01.16 §1a; отличия
   только в природе клиента (Valkey-кеш vs Kafka producer/consumer).
2. **HA.Valkey — 0 правок**: клиентский модуль потребляет публичный API t04;
   публичный контракт HA.Valkey не меняется (как в t10 к HA.Kafka).
3. **Инкапсуляция клиента**: ссылка на пакет `StackExchange.Redis` — только в
  `Infrastructure.App.Valkey`; домены видят `IValkeyCache` + `Result`-монаду.
4. **Fail-open**: отсутствие валидных параметров (снапшот не собран, ключи
   неполны, etcd недоступен, ConnectionStrings пуст) НЕ роняет старт приложения;
   все операции кеша возвращают `Result.Failed`; появление параметров —
   первый вызов строит соединение, без рестарта.
5. **Надёжность прежде всего** (AGENTS.base п.8): `AbortOnConnectFail=false` —
   multiplexer переживает недоступность сервера и сам реконнектится; смена
   параметров — управляемое пересоздание multiplexer'а без потери входящих вызовов.
6. **Изоляция доменов**: ключи каждого модля-потребителя изолированы
   префиксом `"<KeyPrefix>:<key>"` из per-модульного конфига.
7. **State кластера не интерпретируется** (как у Kafka-провайдера t10):
   клиенту достаточно наличия точек дискавери; raw `State` — дело наблюдения.

## 4. Структура и компоненты

Новый проект `src/PuzzleServer.Infrastructure.App.Valkey` (namespace
`PuzzleServer.Infrastructure.App.Valkey`, подключение в
`src/PuzzleServer.Api.slnx`; образец — App.Kafka).

### 4.1. Конфигурация

| Тип | Источник | Содержимое |
|---|---|---|
| `ValkeyOptions` | `ConnectionStrings:Valkey` (Aspire; строка вида `host:port`) + секция `Valkey` (переопределения `Endpoints`, `Username`, `Password`) | `Endpoints` (только Aspire-ветка; в HaDb игнорируется), `Username`, `Password` (опциональные — локальный контур может жить без ACL; HaDb-ветка их не читает) |
| `ValkeyConfig` (abstract) + наследник per-модуль | секция appsettings per-модуля через `[Config("<Section>")]` | `KeyPrefix` (обязателен, непустой) |
| `Valkey:Cluster` | конфиг приложения | имя кластера для заявки `AddValkeyCluster` и чтения снапшота; **обязателен в HaDb-режиме, пусто → fail-fast при старте** (зеркало `Kafka:Cluster`) |

Существующий AppHost (Aspire) дополняется ресурсом `builder.AddValkey("valkey")`
(пакет `Aspire.Hosting.Valkey`, пин версии в `Directory.Packages.props`, как
`Aspire.Hosting.Kafka`) → кладёт `ConnectionStrings:Valkey`. Если пакет
окажется недоступен в NuGet-источниках Puzzle — фоллэк: строка в
`appsettings.Development.json` (фиксируется в плане, не спекой).

### 4.2. Шов соединительных параметров (зеркало §1a Kafka)

```csharp
public sealed record ValkeyConnectionParams(string Endpoints, string Username, string Password, bool Ssl);

public interface IValkeyConnectionProvider
{
    ValkeyConnectionParams? Current { get; }   // null = параметров нет (fail-open)
    IDisposable OnChange(Action handler);      // только при фактическом изменении параметров
}
```

- **`ConfigurationValkeyConnectionProvider`** (Aspire): из
  `IOptionsMonitor<ValkeyOptions>`; пустой/пробельный `Endpoints` → `Current = null`;
  `OnChange` — нотификации options-монитора с value-equality фильтром.
- **`DiscoveryValkeyConnectionProvider`** (HaDb): 1:1
  `DiscoveryKafkaConnectionProvider` (t10): `Current` — вычисление из
  `IValkeyDiscoveryStore.Get(cluster).GetClientConfig()` (null при Failed /
  null-конфиге); `OnChange` — подписка на `store.Updated` своего кластера,
  фильтр по value-equality вычисленных параметров (шум чужих изменений не
  проходит); **baseline фиксируется ДО подписки** (инвариант t10: значение на
  момент первой подписки, обновляется только событиями). Hosted-сервисов нет —
  актуализацией владеет HA.Valkey.

### 4.3. Держатель соединения `ValkeyConnectionHolder` (singleton)

Владеет `ConnectionMultiplexer` — единственным на процесс (StackExchange.Redis
сам мультиплексирует; пул не нужен):

- **Ленивое построение**: multiplexer строится при первом обращении к
  операциям, когда `provider.Current != null`; до того операции →
  `Result.Failed` (fail-open; зеркало «consumer откладывает построение клиента»).
- **Маппинг** `ValkeyConnectionParams` → `ConfigurationOptions`:
  `Endpoints` (строка `h1:p1,h2:p2`) → `EndPoints` (сплит по запятой);
  `Username`/`Password` → `User`/`Password` (только если оба непустые);
  `Ssl` → `Ssl`; всегда `AbortOnConnectFail=false` (самореконнект, надёжностный
  дефолт). Таймауты — дефолты StackExchange.Redis (не настраиваем в v1).
- **Hot-reload**: подписка `provider.OnChange` → построить НОВЫЙ multiplexer по
  новым параметрам → атомарно подменить ссылку → старый dispose. Ротация
  `app_password` (окно двух паролей arch/21 §5 E: OLD ещё валиден на ноде) —
  подмена без разрыва обслуживания: идущие операции завершаются на старом
  экземпляре (dispose после рассрочки обращений, деталь плана). `Current`
  стал `null` (параметры исчезли) → dispose текущего, операции снова
  `Result.Failed` (fail-open симметричен).
- Экспонирует тонкий доступ `IDatabase` (через внутренний шов, §4.4).

### 4.4. Test-seam `IValkeyClient` (паттерн t10 §7.3 D7)

`IDatabase` StackExchange.Redis тяжело подменять (~десятки членов) — модуль
зависит от тонкого **internal**-шва:

```csharp
internal interface IValkeyClient : IAsyncDisposable
{
    // StringSet/Get/GetSet/Remove(SetRemove-семантика ключа: KeyDelete)/KeyExists/
    // StringSet(KeyNotFound)/StringIncrement/StringGetMultiple/StringSetMultiple/
    // KeyExpire/KeyTimeToLive — по одному методу на операцию IValkeyCache
}
```

`ValkeyClientAdapter` (единственное место, знающее `IDatabase`) создаётся
holder'ом от текущего multiplexer'а; юнит-тесты подменяют fake-клиентом
(`InternalsVisibleTo`, как `FakeKafkaTopicClient`).

### 4.5. Доменная поверхность `IValkeyCache` (расширенный набор v1)

```csharp
public interface IValkeyCache
{
    // базовые
    ValueTask<Result<T?>> GetAsync<T>(string key, CancellationToken ct = default);
    ValueTask<Result> SetAsync<T>(string key, T value, TimeSpan? ttl = null, CancellationToken ct = default);
    ValueTask<Result> RemoveAsync(string key, CancellationToken ct = default);
    ValueTask<Result<bool>> ExistsAsync(string key, CancellationToken ct = default);
    // атомарные
    ValueTask<Result<T?>> GetSetAsync<T>(string key, T value, TimeSpan? ttl = null, CancellationToken ct = default);      // GETSET (+ TTL ключа после SET)
    ValueTask<Result<bool>> AddIfAbsentAsync<T>(string key, T value, TimeSpan? ttl = null, CancellationToken ct = default); // SET NX
    // каунтеры
    ValueTask<Result<long>> IncrementAsync(string key, long delta = 1, CancellationToken ct = default);                    // INCRBY
    // массовые
    ValueTask<Result<IReadOnlyDictionary<string, T?>>> GetManyAsync<T>(IReadOnlyCollection<string> keys, CancellationToken ct = default); // MGET
    ValueTask<Result> SetManyAsync<T>(IReadOnlyCollection<KeyValuePair<string, T>> pairs, TimeSpan? ttl = null, CancellationToken ct = default); // батч SET (MSET не несёт TTL)
    // TTL-управление
    ValueTask<Result> KeyExpireAsync(string key, TimeSpan ttl, CancellationToken ct = default);
    ValueTask<Result<TimeSpan?>> KeyTimeToLiveAsync(string key, CancellationToken ct = default);
}
```

- **Сериализация**: примитивы (`string`, числовые, `bool`, `byte[]`) — напрямую
  через конвертеры `RedisValue`; остальные типы — `System.Text.Json`
  (встроенный, без новых пакетов). Фиксируется юнит-тестами.
- **Префикс**: реализация подставляет `"<KeyPrefix>:<key>"` ко всем операциям
  (в т.ч. элементам массовых); домен никогда не видит префикс.
- **Ошибки**: исключения StackExchange.Redis → `Result.Failed` (implicit
  conversion); ошибок соединения при fail-open — тоже (никаких бросков наружу).
- `IValkeyCache` лёгкий (без собственного соединения) — не `IAsyncDisposable`;
  соединением владеет holder (dispose при shutdown приложения).

### 4.6. Фабрика `IValkeyCacheBuilder<TConfig>` (open-generic)

```csharp
public abstract class ValkeyConfig { public string KeyPrefix { get; set; } = string.Empty; }

[Config("SessionCache")]            // пример домена-потребителя
public class SessionCacheConfig : ValkeyConfig { }

public interface IValkeyCacheBuilder<TConfig> where TConfig : ValkeyConfig
{
    IValkeyCache Build();           // KeyPrefix из config.CurrentValue.KeyPrefix
}
```

`[InjectAsSingleton] internal sealed class ValkeyCacheBuilder<TConfig>` —
регистрация open-generic через `AutoRegistration` (паттерн Kafka §6:
`IOptionsMonitor<TConfig>` обязан быть зарегистрирован через `[Config]`).
Пустой `KeyPrefix` при `Build()` → `InvalidOperationException` (fail-fast,
зеркало пустого Topic у Kafka).

### 4.7. Регистрация `AddValkey`

`builder.Services.AddValkey(builder.Configuration)` — рядом с `AddKafka` в
`src/PuzzleServer.Api/Program.cs` (после `UseDiBehaviours`, тот же порядок):
1. `Configure<ValkeyOptions>` (ConnectionStrings + секция `Valkey`);
2. **Ветвление по `Database:Source`**: `Aspire` → `ConfigurationValkeyConnectionProvider`;
   `HaDb` → fail-fast без `Valkey:Cluster`; `AddHaValkey(configuration).AddValkeyCluster(<Valkey:Cluster>)`
   + `DiscoveryValkeyConnectionProvider`;
3. `ValkeyConnectionHolder` (singleton);
4. `AutoRegistration(typeof(ModuleExtensions).Assembly)` — open-generic builder.

Пакет `StackExchange.Redis` — пин последней стабильной 2.x в
`Directory.Packages.props` (централизованное версионирование Puzzle).

## 5. Обработка сбоев

| Случай | Поведение |
|---|---|
| Параметров нет (снапшот не собран / ключи неполны / etcd умер / ConnectionStrings пуст) | `Current == null` → все операции `Result.Failed`; старт приложения не роняется; появление параметров — первый вызов строит соединение |
| Ротация `app_password` (arch/21 §5 E) | `Updated` → `OnChange` → пересоздание multiplexer с NEW; окно двух паролей на ноде (OLD валиден в E1–E2) гарантирует отсутствие разрыва |
| Смена endpoints | тот же механизм (value-equality по всему `ValkeyConnectionParams`) |
| Шум снапшота (изменения не-соединительных полей: State, admin-креды, unknown-ключи) | `OnChange` не стреляет — фильтр value-equality вычисленных параметров |
| Смерть etcd при живом Valkey | HA.Valkey fail-open отдаёт последний снапшот → кеш продолжает работать |
| Недоступность Valkey-ноды | `AbortOnConnectFail=false`: multiplexer жив, сам реконнектится; операции в период недоступности → `Result.Failed` |
| Исключения библиотеки в операции | инкапсуляция в `Result.Failed` — наружу не бросаются |

## 6. Тесты (канон Puzzle t04/t10 + AGENTS.base п.11)

### 6.1. Юниты (`src/PuzzleServer.UnitTests/Valkey/`)

- Регистрация `AddValkey`: обе ветки `Database:Source`; fail-fast (пустой
  `Valkey:Cluster` в HaDb; пустой `KeyPrefix` в `Build`); порядок
  зависимостей.
- Провайдеры: Configuration (Current из options; null при пустом endpoints;
  OnChange через монитор); Discovery (Current из стора; **baseline до
  подписки**; value-equality фильтр шума; ротация стреляет) — по образцу
  юнитов t10.
- Маппинг `ValkeyClientConfig` → `ConfigurationOptions` (endpoints-сплит,
  креды, Ssl, AbortOnConnectFail).
- `IValkeyCache` через fake `IValkeyClient`: все операции, префикс ключей,
  сериализация (примитивы/JSON), TTL-параметры, fail-open (`Result.Failed`
  без исключений).

### 6.2. Интеграционные (`src/PuzzleServer.IntegrationTests/Valkey/`)

Контур `ValkeyClientFixture`: переиспользуемая `ValkeyEtcdFixture` (etcd,
дефолт 32496) + контейнер `valkey/valkey:<пин версии>` с ACL-аргументами
канона arch/21 §2 (`--user default off --user app on >OLD >NEW ~* +@read +@write`)
— **окно двух паролей** (аналог JAAS-окна KafkaSaslFixture). Host-порт
valkey — динамический (`GetMappedPublicPort`), без литералов; endpoints/креды
фикстура сеет в etcd ПОСЛЕ старта по фактическому порту. Полный teardown
при любом исходе; чистка префикса `/valkey/clusters/` перед сценарием;
ассерт чистоты (контейнер удалён).

Сценарии (по образцу t10):
1. **fail-open → восстановление**: ключей нет → старт жив, операции Failed;
   появление ключей в etcd → операции работают без рестарта;
2. **roundtrip всех операций** по кредам из etcd (get/set/атомарные/каунтеры/
   массовые/TTL, с префиксами);
3. **ротация `app_password`** OLD→NEW: put NEW в etcd → клиент переподключается
   с NEW, операции продолжаются без потерь;
4. **шум**: изменение не-соединительных ключей → клиент не перестраивается;
5. **смерть etcd**: stop etcd → кеш продолжает работать по последнему снапшоту
   (valkey жив), рестарт etcd — восстановление актуализации.

После серии — зачистка контейнеров (0 остатков), серии не накладываются.

## 7. Документация и отражение в репозиториях

- **Puzzle**: новый канон модуля `docs/01.22-valkey.md` (структура и стиль —
  docs/01.16) + строка в индексе `docs/01-infrastructure.md`; актуализация
  `docs/01.21-ha-valkey.md` — раздел «Интеграция в клиентский модуль (t08 —
  плановая)» переписывается на ссылку 01.22 (убрать «модуля пока нет»).
- **pg**: контракты `arch/` НЕ меняются (проверено: arch/20 §4 уже описывает
  дискавери; t08 — потребитель). Мерж-гейт (тем же коммитом мержа, правило
  roadmap): удалить пункт `t08-valkey-client-integration` из
  `arch/roadmap/valkey.md` (+ зачистить `←`-упоминания t08, если появятся).
- Состояние задачи в persistent memory не пишется.

## 8. Фазы (плана)

1. **Ф1 Каркас**: проект `App.Valkey` (csproj, slnx, `ValkeyOptions`,
   `ValkeyConfig`, PackageVersion `StackExchange.Redis`) + юниты регистрации.
2. **Ф2 Шов**: `ValkeyConnectionParams`, `IValkeyConnectionProvider`,
   оба провайдера + юниты.
3. **Ф3 Соединение**: `ValkeyConnectionHolder` (лениво, hot-reload, dispose),
   маппинг `ConfigurationOptions`, seam `IValkeyClient` + адаптер + юниты.
4. **Ф4 Поверхность**: `IValkeyCache` (все операции, сериализация, префиксы),
   `IValkeyCacheBuilder<TConfig>` (open-generic, AutoRegistration) +
   `Program.cs` (`AddValkey`) + AppHost-ресурс + юниты.
5. **Ф5 Интеграция**: `ValkeyClientFixture` (etcd + valkey ACL), 5 сценариев
   §6.2.
6. **Ф6 Доки и финал**: 01.22 + 01-infrastructure + 01.21-актуализация;
   полный прогон (юниты + интеграция всей серии, зачистка серий), 0 warnings
   прод-кода.

## 9. Ограничения и вне-скоуп

- TLS — вне v1 (`ssl=false`; поле `Ssl` маппится, CA появится с t06-valkey-tls);
- pub/sub, SCAN, скрипты (EVAL), транзакции/батчи-команды — вне v1 (по
  потребности — отдельная задача);
- реплики/sentinel/cluster-топологии Valkey — вне канона домена (arch/20);
- admin-поверхность (аналог `IKafkaTopicAdmin`) — нет: контроль-плейн домена
  у ValkeyWorker, доменам не нужен;
- собственный health-check модуля — не добавляется (HaValkeyCheck уже есть в
  HaDb-режиме; зеркально Kafka-модулю t10);
- OpenTelemetry-спаны и метрики модуля — вне v1 (логирование ILogger; спаны —
  отдельной задачей по образцу Kafka §7.7);
- `HaValkey:MembersMode` при сосуществовании с HA.Db — рекомендации как у
  Kafka (§1a: один etcd на стенд) — фиксируются в доке 01.22;
- правки HA.Valkey — запрещены (0 правок);
- коммит/пуш/мерж в main любого репозитория — только по отдельному явному
  запросу пользователя.

## 10. Критерии приёмки

1. В HaDb-режиме: `AddValkey` регистрирует `AddHaValkey(configuration)
   .AddValkeyCluster(<Valkey:Cluster>)`; пустой `Valkey:Cluster` — fail-fast
   при старте с понятным сообщением.
2. Соединительные параметры — из `GetClientConfig()` снапшота HA.Valkey
   (endpoints + app-креды); `ConfigurationOptions` корректны (юнит-тест
   маппинга).
3. Fail-open: без валидных параметров приложение стартует, все операции кеша
   `Result.Failed`; появление параметров — работоспособность без рестарта
   (интеграционный сценарий 1).
4. Ротация `app_password` через etcd — клиент продолжает работать без
   рестарта приложения и потерь операций (интеграционный сценарий 3).
5. Шум снапшота не перестраивает соединение (сценарий 4); смерть etcd не
   роняет кеш (сценарий 5).
6. Все операции расширенного набора работают roundtrip против живого valkey
   с ACL по кредам из etcd, ключи доменов изолированы префиксами (сценарий 2).
7. `StackExchange.Redis` ссылается только `Infrastructure.App.Valkey`;
   HA.Valkey — 0 правок; прод-код — 0 warnings (TreatWarningsAsErrors).
8. Юниты и интеграционные — зелёные; полный тестовый набор Puzzle серии —
   зелёный, зачистка контейнеров после серии (0 остатков).
9. Доки Puzzle обновлены (01.22, 01-infrastructure, 01.21); в pg — тег t08
   удалён из `arch/roadmap/valkey.md` тем же коммитом мержа.
