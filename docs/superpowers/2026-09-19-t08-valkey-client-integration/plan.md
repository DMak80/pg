# t08-valkey-client-integration — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Клиентский модуль Valkey `PuzzleServer.Infrastructure.App.Valkey` в репозитории Puzzle — доменные кеш-абстракции (`IValkeyCache` + Result-монада) поверх StackExchange.Redis с интеграцией дискавери HA.Valkey (t04): HaDb-режим берёт endpoints/ACL-креды из `GetClientConfig()` снапшота, ротация `app_password`/смена endpoints доставляется событием `Updated` (hot-reload без рестарта), fail-open без параметров.

**Architecture:** Зеркало Kafka t10 (docs/01.16 §1a) 1:1: шов `IValkeyConnectionProvider` (Configuration/Discovery-провайдеры, переключатель `Database:Source`), singleton-держатель `ValkeyConnectionHolder` с единственным на процесс `ConnectionMultiplexer` (лениво, hot-reload, `AbortOnConnectFail=false`), тонкий internal-seam `IValkeyClient` для юнит-тестов, public-поверхность `IValkeyCache` + open-generic `IValkeyCacheBuilder<TConfig>` с per-модульными `KeyPrefix`. HA.Valkey — 0 правок.

**Tech Stack:** .NET 10, C# (`LangVersion=latest`, `Nullable=enable`), StackExchange.Redis **2.13.17** (последняя стабильная 2.x, проверено на nuget.org 2026-09-19), Aspire.Hosting.Valkey **13.4.3** (совпадает с пином Aspire 13.4.3 в `Directory.Packages.props`), System.Text.Json (встроенный), xunit.v3 + FluentAssertions, Testcontainers (etcd `quay.io/coreos/etcd:v3.5.21` — переиспользуемая `ValkeyEtcdFixture`; valkey `valkey/valkey:9.1.2` — пин из `dev-stand/images/images.txt` pg).

**Spec:** `docs/superpowers/2026-09-19-t08-valkey-client-integration/spec.md` (этот же каталог в pg-worktree `feat-t08-valkey-client-integration`). Канон-образец — Puzzle `src/PuzzleServer.Infrastructure.App.Kafka` (t10) и `docs/01.16-kafka.md` §1a; дискавери — Puzzle `src/PuzzleServer.Infrastructure.App.HA.Valkey` (t04).

## Куда пишется код и артефакты (важно)

- **Код — репозиторий Puzzle** `/Users/demakaev/ZCodeProject/Puzzle`. Ветка `feat-t08-valkey-client-integration` создаётся от `main` (HEAD `ace1cf8`) в Шаге 1. Все команды ниже — из корня Puzzle (если не указано иное).
- **Spec/plan** — pg-worktree `docs/superpowers/2026-09-19-t08-valkey-client-integration/` (этот файл); при исполнении НЕ трогать. Доки модуля Puzzle (`docs/01.22-valkey.md` и др.) — в репозиторий Puzzle (Шаг 10).
- **Staged-файл `src/global.json`** в индексе Puzzle существует ДО начала работы: НЕ трогать и НЕ включать в коммиты. Все `git add` — ТОЧЕЧНЫЕ (только файлы шага); `git add -A` / `git add .` / `git commit -a` — ЗАПРЕЩЕНЫ на всей задаче.
- Коммиты в ветке Puzzle — свободно. Мерж в `main` и пуш — только по отдельному явному запросу пользователя. Правка roadmap pg — тем же коммитом мержа (Шаг 10, гейт 10.6).

## Global Constraints (из spec, обязательны в каждом шаге)

- Ссылка на пакет `StackExchange.Redis` — только в проекте `PuzzleServer.Infrastructure.App.Valkey` (единственная PackageReference в решении; тест-проекты получают пакет транзитивно через ProjectReference, новых PackageReference в тест-проекты НЕ добавлять).
- `PuzzleServer.Infrastructure.App.HA.Valkey` — 0 правок (потребляется только публичный API t04).
- Версии пакетов централизованно: `src/Directory.Packages.props` (+ `StackExchange.Redis` 2.13.17, `Aspire.Hosting.Valkey` 13.4.3); `EnablePackageVersionOverride=false` — никаких `Version` в csproj.
- Прод-код — 0 warnings: `TreatWarningsAsErrors` в Puzzle глобально НЕ установлен — гейт «0 warnings» = вывод `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PuzzleServer.Api.slnx` не содержит строк `warning` (критерий приёмки №7). `#pragma`-подавления не использовать.
- Fail-open: отсутствие параметров НЕ роняет старт; все операции кеша — `Result.Failed`; исключения библиотеки наружу не бросаются.
- `AbortOnConnectFail=false` всегда; таймауты — дефолты StackExchange.Redis (в v1 не настраиваем).
- Порты docker-контейнеров в тестах: host-порт valkey — динамический (`WithPortBinding(6379, assignRandomHostPort: true)` + `GetMappedPublicPort`); etcd — фиксированный **32497** (зоны: 32490 kafka t05, 32495 kafka t10, 32496 valkey t04 default-коллекция — параллельная с нашей; фиксированный порт гарантирует рестарты etcd сценария 5 на том же endpoint). Никаких литералов ожидаемого host-порта valkey в assertions.
- Полный teardown интеграционной фикстуры при любом исходе (`IAsyncLifetime.DisposeAsync`); чистка префикса `/valkey/clusters/` перед каждым сценарием; после каждой серии — гейт «0 остатков» (`docker ps -a`).
- Бюджеты ожиданий в тестах — короткие (3–15 с, шаг поллинга 200 мс); никаких sleep/ожиданий > 30 с.
- Язык: комментарии/доки — русский; идентификаторы — английские. Тесты — AAA-комментарии (`// Arrange`, `// Act`, `// Assert`).
- Состояние задачи в persistent memory НЕ пишется.

## File Structure (карта файлов)

```
Puzzle (ветка feat-t08-valkey-client-integration)
├── src/Directory.Packages.props                        # +2 PackageVersion (Шаг 1, Шаг 8)
├── src/PuzzleServer.Api.slnx                           # +1 строка проекта (Шаг 1)
├── src/PuzzleServer.Infrastructure.App.Valkey/         # НОВЫЙ проект
│   ├── PuzzleServer.Infrastructure.App.Valkey.csproj   # Шаг 1
│   ├── ValkeyOptions.cs                                # Шаг 1  ([Config("Valkey")])
│   ├── ValkeyConfig.cs                                 # Шаг 1  (abstract, KeyPrefix)
│   ├── ValkeyConnectionParams.cs                       # Шаг 2  (record + редакция секрета)
│   ├── IValkeyConnectionProvider.cs                    # Шаг 2  (+ ConfigurationValkeyConnectionProvider)
│   ├── DiscoveryValkeyConnectionProvider.cs            # Шаг 3
│   ├── ValkeyConnectionOptions.cs                      # Шаг 4  (маппинг → ConfigurationOptions)
│   ├── IValkeyClient.cs                                # Шаг 5  (internal test-seam)
│   ├── ValkeyClientAdapter.cs                          # Шаг 5  (единственное место IDatabase)
│   ├── ValkeyClientFactory.cs                          # Шаг 5  (ConnectAsync + адаптер)
│   ├── ValkeyConnectionHolder.cs                       # Шаг 5  (singleton, лениво, hot-reload)
│   ├── ValkeySerializer.cs                             # Шаг 6  (T ↔ RedisValue)
│   ├── IValkeyCache.cs                                 # Шаг 7  (public-поверхность)
│   ├── ValkeyCache.cs                                  # Шаг 7  (префикс, Result-обёртки)
│   ├── IValkeyCacheBuilder.cs                          # Шаг 8  (public интерфейс)
│   ├── ValkeyCacheBuilder.cs                           # Шаг 8  ([InjectAsSingleton] open-generic)
│   └── ModuleExtensions.cs                             # Шаг 8  (AddValkey)
├── src/PuzzleServer.Api/Program.cs                     # Шаг 8  (+ .AddValkey(builder.Configuration))
├── src/PuzzleServer.Api.AppHost/                      # Шаг 8
│   ├── AppHost.cs                                      # (+ AddValkey("valkey") + WithReference)
│   └── PuzzleServer.Api.AppHost.csproj                 # (+ Aspire.Hosting.Valkey)
├── src/PuzzleServer.UnitTests/Valkey/                  # НОВЫЕ юниты
│   ├── Fakes/FakeOptionsMonitor.cs                     # Шаг 2
│   ├── Fakes/FakeValkeyDiscoveryStore.cs               # Шаг 3
│   ├── Fakes/FakeValkeyConnectionProvider.cs           # Шаг 3
│   ├── Fakes/FakeValkeyClient.cs                       # Шаг 5
│   ├── ValkeyOptionsTests.cs                           # Шаг 1
│   ├── ConfigurationValkeyConnectionProviderTests.cs   # Шаг 2
│   ├── DiscoveryValkeyConnectionProviderTests.cs       # Шаг 3
│   ├── ValkeyConnectionOptionsTests.cs                 # Шаг 4
│   ├── ValkeyConnectionHolderTests.cs                  # Шаг 5
│   ├── ValkeySerializerTests.cs                        # Шаг 6
│   ├── ValkeyCacheTests.cs                             # Шаг 7
│   ├── ValkeyCacheBuilderTests.cs                      # Шаг 8
│   ├── AddValkeySourceBranchingTests.cs                # Шаг 8
│   └── (PuzzleServer.UnitTests.csproj: + ProjectReference App.Valkey)
├── src/PuzzleServer.IntegrationTests/Valkey/           # НОВЫЕ интеграционные
│   ├── ValkeyClientFixture.cs                          # Шаг 9  (etcd 32497 + valkey/valkey:9.1.2 ACL)
│   ├── TestCacheConfigs.cs                             # Шаг 9  (тестовые TConfig)
│   ├── ValkeyClientIntegrationTests.cs                 # Шаг 9  (5 сценариев §6.2)
│   └── (PuzzleServer.IntegrationTests.csproj: + ProjectReference App.Valkey)
└── docs/
    ├── 01.22-valkey.md                                 # Шаг 10 (канон модуля)
    ├── 01-infrastructure.md                             # Шаг 10 (+строка индекса)
    └── 01.21-ha-valkey.md                               # Шаг 10 (актуализация раздела t08)
```

---

## Шаг 1: Каркас проекта App.Valkey (Ф1 spec §8.1)

**Вход:** Puzzle в `main` (HEAD `ace1cf8`); в индексе только staged `src/global.json` (не трогаем).

**Действие:** создать ветку `feat-t08-valkey-client-integration`; проект `src/PuzzleServer.Infrastructure.App.Valkey` (csproj по образцу App.Kafka), строка в slnx, пин `StackExchange.Redis` в `Directory.Packages.props`; `ValkeyOptions` (`[Config("Valkey")]`), `ValkeyConfig` (abstract); юнит биндинга опций; ProjectReference юнит-проекта.

**Выход:** проект собирается в решении, пакет восстанавливается, первый юнит зелёный; ветка с первым коммитом.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PuzzleServer.Api.slnx` — успех, 0 warnings; `dotnet test src/PuzzleServer.UnitTests --filter "FullyQualifiedName~UnitTests.Valkey"` — PASS.

**Связь со spec:** §4 (структура), §4.1 (конфигурация), §8 Ф1.

**Interfaces (Produces, используют Шаги 2–8):**

```csharp
// ValkeyOptions.cs
using PuzzleServer.Infrastructure.App.DI;

namespace PuzzleServer.Infrastructure.App.Valkey;

// [Config] -> IOptionsMonitor<ValkeyOptions>; секция "Valkey".
// Endpoints — Aspire-ветка: строка "h1:p1,h2:p2" из ConnectionStrings:Valkey
// (переопределяется секцией Valkey:Endpoints). HaDb-ветка опции игнорирует —
// параметры приходят из etcd-дискавери (IValkeyConnectionProvider).
// Username/Password опциональны: локальный контур может жить без ACL.
[Config("Valkey")]
public class ValkeyOptions
{
    public string Endpoints { get; set; } = string.Empty;
    public string Username { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
}
```

```csharp
// ValkeyConfig.cs
namespace PuzzleServer.Infrastructure.App.Valkey;

// Per-модульный конфиг кеша. Наследник объявляется доменом-потребителем с
// [Config("<Section>")] (пример: [Config("SessionCache")]); KeyPrefix обязателен
// (изоляция ключей доменов, спека §3 п.6): реализация подставляет "<KeyPrefix>:<key>".
public abstract class ValkeyConfig
{
    public string KeyPrefix { get; set; } = string.Empty;
}
```

- [ ] **1.1. Создать ветку в Puzzle (от main)**

```bash
cd /Users/demakaev/ZCodeProject/Puzzle
git status --short        # ожидание: только "A  src/global.json" (staged) — НЕ трогаем
git checkout -b feat-t08-valkey-client-integration
git branch --show-current # feat-t08-valkey-client-integration
```

- [ ] **1.2. Пин пакета** — в `src/Directory.Packages.props` добавить (после `System.IO.Hashing`, по алфавиту соседей):

```xml
    <PackageVersion Include="StackExchange.Redis" Version="2.13.17" />
```

- [ ] **1.3. csproj проекта** — `src/PuzzleServer.Infrastructure.App.Valkey/PuzzleServer.Infrastructure.App.Valkey.csproj` (образец — App.Kafka; единственная в решении PackageReference на StackExchange.Redis):

```xml
<Project Sdk="Microsoft.NET.Sdk">

    <PropertyGroup>
        <TargetFramework>net10.0</TargetFramework>
        <ImplicitUsings>enable</ImplicitUsings>
        <Nullable>enable</Nullable>
    </PropertyGroup>

    <ItemGroup>
      <PackageReference Include="StackExchange.Redis"/>
      <PackageReference Include="Microsoft.Extensions.Configuration"/>
      <PackageReference Include="Microsoft.Extensions.Configuration.Abstractions"/>
      <PackageReference Include="Microsoft.Extensions.DependencyInjection.Abstractions"/>
      <PackageReference Include="Microsoft.Extensions.Logging.Abstractions"/>
      <PackageReference Include="Microsoft.Extensions.Options"/>
    </ItemGroup>
    <ItemGroup>
      <ProjectReference Include="..\PuzzleServer.Infrastructure.App\PuzzleServer.Infrastructure.App.csproj"/>
      <ProjectReference Include="..\PuzzleServer.Infrastructure.App.HA.Valkey\PuzzleServer.Infrastructure.App.HA.Valkey.csproj"/>
    </ItemGroup>
    <ItemGroup>
      <InternalsVisibleTo Include="PuzzleServer.UnitTests"/>
    </ItemGroup>

</Project>
```

- [ ] **1.4. Подключить в slnx** — в `src/PuzzleServer.Api.slnx`, папка `/Infrastructure/`, строка сразу после `PuzzleServer.Infrastructure.App/PuzzleServer.Infrastructure.App.csproj`:

```xml
        <Project Path="PuzzleServer.Infrastructure.App.Valkey/PuzzleServer.Infrastructure.App.Valkey.csproj" />
```

- [ ] **1.5. ProjectReference юнит-проекта** — в `src/PuzzleServer.UnitTests/PuzzleServer.UnitTests.csproj` (ItemGroup ProjectReference):

```xml
        <ProjectReference Include="..\PuzzleServer.Infrastructure.App.Valkey\PuzzleServer.Infrastructure.App.Valkey.csproj"/>
```

- [ ] **1.6. Юнит-тест** — `src/PuzzleServer.UnitTests/Valkey/ValkeyOptionsTests.cs`:

```csharp
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using PuzzleServer.Infrastructure.App.DI;
using PuzzleServer.Infrastructure.App.Valkey;
using Xunit;

namespace PuzzleServer.UnitTests.Valkey;

// Опции модуля (спека §4.1): [Config("Valkey")] биндит секцию в IOptionsMonitor
public class ValkeyOptionsTests
{
    [Fact]
    public void ValkeyOptions_SectionBindsThroughConfigAttribute()
    {
        // Arrange — секция Valkey через AutoRegistrationConfigDiTypeBehaviour (канон Puzzle)
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Valkey:Endpoints"] = "localhost:6379",
            ["Valkey:Username"] = "app",
            ["Valkey:Password"] = "secret",
        }).Build();
        var services = new ServiceCollection();
        new AutoRegistrationConfigDiTypeBehaviour(services, config).Handle(typeof(ValkeyOptions).Assembly.GetTypes());
        using var sp = services.BuildServiceProvider();

        // Act
        var options = sp.GetRequiredService<Microsoft.Extensions.Options.IOptionsMonitor<ValkeyOptions>>().CurrentValue;

        // Assert
        options.Endpoints.Should().Be("localhost:6379");
        options.Username.Should().Be("app");
        options.Password.Should().Be("secret");
    }

    [Fact]
    public void ValkeyConfig_DefaultKeyPrefixEmpty()
    {
        // Arrange / Act
        var config = new TestCacheConfig();

        // Assert — пустой по умолчанию: Build() обязан fail-fast (проверяется в Шаге 8)
        config.KeyPrefix.Should().BeEmpty();
    }

    private sealed class TestCacheConfig : ValkeyConfig;
}
```

- [ ] **1.7. Проверка и коммит**

```bash
cd /Users/demakaev/ZCodeProject/Puzzle
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PuzzleServer.Api.slnx        # успех, вывод без "warning"
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PuzzleServer.UnitTests --filter "FullyQualifiedName~UnitTests.Valkey"   # PASS 2/2
git add src/Directory.Packages.props src/PuzzleServer.Api.slnx \
        src/PuzzleServer.Infrastructure.App.Valkey/PuzzleServer.Infrastructure.App.Valkey.csproj \
        src/PuzzleServer.Infrastructure.App.Valkey/ValkeyOptions.cs \
        src/PuzzleServer.Infrastructure.App.Valkey/ValkeyConfig.cs \
        src/PuzzleServer.UnitTests/PuzzleServer.UnitTests.csproj \
        src/PuzzleServer.UnitTests/Valkey/ValkeyOptionsTests.cs
git commit -m "feat(valkey): каркас PuzzleServer.Infrastructure.App.Valkey — csproj (StackExchange.Redis 2.13.17), slnx, ValkeyOptions/ValkeyConfig, юнит биндинга секции"
git status --short   # staged src/global.json остался нетронутым, в коммит не попал
```

---

## Шаг 2: Шов — ValkeyConnectionParams + ConfigurationValkeyConnectionProvider (Ф2 §8.2)

**Вход:** Шаг 1 закоммичен (проект собирается, `ValkeyOptions` доступен).

**Действие:** record `ValkeyConnectionParams` (редакция секрета в `ToString`), интерфейс `IValkeyConnectionProvider`, Aspire-провайдер над `IOptionsMonitor<ValkeyOptions>` с value-equality фильтром; fake `FakeOptionsMonitor`; юниты.

**Выход:** Aspire-ветка шова параметров готова и покрыта юнитами.

**Проверка:** `dotnet test src/PuzzleServer.UnitTests --filter "FullyQualifiedName~UnitTests.Valkey"` — PASS; сборка 0 warnings.

**Связь со spec:** §4.2 (шов, зеркало §1a Kafka), §5 (строки 1/4 таблицы сбоев), §8 Ф2.

**Interfaces (Produces, используют Шаги 3–5, 8):**

```csharp
// ValkeyConnectionParams.cs
namespace PuzzleServer.Infrastructure.App.Valkey;

// Соединительные параметры valkey-клиента — всё, что нужно ConfigurationOptions.
// Username/Password опциональны (пустые = без ACL, локальный контур); Ssl=false в v1
// (TLS — t06; поле маппится в ConfigurationOptions.Ssl, переделка при t06 не потребуется).
public sealed record ValkeyConnectionParams(
    string Endpoints,
    string Username,
    string Password,
    bool Ssl)
{
    // Редакция секрета (зеркало KafkaConnectionParams): пароль не попадает в логи/дампы
    public override string ToString()
        => $"{nameof(ValkeyConnectionParams)} {{ Endpoints = {Endpoints}, "
           + $"Username = {Username}, Password = ***, Ssl = {Ssl} }}";
}
```

```csharp
// IValkeyConnectionProvider.cs
namespace PuzzleServer.Infrastructure.App.Valkey;

// Шов «источник соединительных параметров valkey-клиента» (зеркало IKafkaConnectionProvider
// t10, спека §4.2). Реализации: Configuration (Aspire) и Discovery (HaDb, Шаг 3).
// Current == null — валидных параметров нет: fail-open (операции кеша Result.Failed,
// старт приложения не роняется). OnChange — только при фактическом изменении
// Current (value-equality), включая переходы null→params / params→null.
public interface IValkeyConnectionProvider
{
    ValkeyConnectionParams? Current { get; }

    IDisposable OnChange(Action handler);
}
```

- [ ] **2.1. Fake** — `src/PuzzleServer.UnitTests/Valkey/Fakes/FakeOptionsMonitor.cs` (копия generic-фейка из `UnitTests/Kafka/Fakes/FakeOptionsMonitor.cs`):

```csharp
using Microsoft.Extensions.Options;

namespace PuzzleServer.UnitTests.Valkey.Fakes;

// Ручной IOptionsMonitor: Set заменяет значение и триггерит слушателей.
public sealed class FakeOptionsMonitor<T> : IOptionsMonitor<T> where T : class, new()
{
    private readonly List<Action<T?, string?>> _listeners = [];
    private readonly object _gate = new();

    public T CurrentValue { get; private set; } = new();

    public T Get(string? name) => CurrentValue;

    public IDisposable OnChange(Action<T?, string?> listener)
    {
        lock (_gate) _listeners.Add(listener);
        return new Unsubscription(() => { lock (_gate) _listeners.Remove(listener); });
    }

    public void Set(T value)
    {
        CurrentValue = value;
        Action<T?, string?>[] snapshot;
        lock (_gate) snapshot = [.. _listeners];
        foreach (var listener in snapshot)
            listener(value, null);
    }

    private sealed class Unsubscription(Action unsubscribe) : IDisposable
    {
        public void Dispose() => unsubscribe();
    }
}
```

- [ ] **2.2. Aspire-провайдер** — `src/PuzzleServer.Infrastructure.App.Valkey/IValkeyConnectionProvider.cs` (под интерфейсом):

```csharp
// Aspire-ветка (спека §4.2): адаптер над IOptionsMonitor<ValkeyOptions>.
// Endpoints — из ConnectionStrings:Valkey / секции Valkey; пустой/пробельный → Current = null.
// OnChange — нотификации options-монитора с value-equality фильтром (шум не проходит).
internal sealed class ConfigurationValkeyConnectionProvider : IValkeyConnectionProvider
{
    private readonly Microsoft.Extensions.Options.IOptionsMonitor<ValkeyOptions> _options;
    private readonly object _gate = new();
    private ValkeyConnectionParams? _last;

    public ConfigurationValkeyConnectionProvider(Microsoft.Extensions.Options.IOptionsMonitor<ValkeyOptions> options)
    {
        _options = options;
        // baseline дедупа на момент создания (первое сравнение в OnChange)
        _last = Current;
    }

    public ValkeyConnectionParams? Current
        => string.IsNullOrWhiteSpace(_options.CurrentValue.Endpoints)
            ? null
            : new ValkeyConnectionParams(
                _options.CurrentValue.Endpoints,
                _options.CurrentValue.Username,
                _options.CurrentValue.Password,
                Ssl: false);

    public IDisposable OnChange(Action handler)
        => _options.OnChange((_, _) =>
        {
            lock (_gate)
            {
                var latest = Current;
                if (Equals(_last, latest))
                    return; // значение не изменилось — шум options-монитора (спека §4.2)
                _last = latest;
            }
            handler();
        });
}
```

- [ ] **2.3. Юниты** — `src/PuzzleServer.UnitTests/Valkey/ConfigurationValkeyConnectionProviderTests.cs`:

```csharp
using FluentAssertions;
using PuzzleServer.Infrastructure.App.Valkey;
using PuzzleServer.UnitTests.Valkey.Fakes;
using Xunit;

namespace PuzzleServer.UnitTests.Valkey;

// Aspire-провайдер шова параметров (спека §4.2)
public class ConfigurationValkeyConnectionProviderTests
{
    private static ValkeyOptions Options(string endpoints, string user = "", string password = "")
        => new() { Endpoints = endpoints, Username = user, Password = password };

    [Fact]
    public void Current_FromOptions_WithCreds()
    {
        // Arrange
        var monitor = new FakeOptionsMonitor<ValkeyOptions>();
        var provider = new ConfigurationValkeyConnectionProvider(monitor);

        // Act
        monitor.Set(Options("localhost:6379,localhost:6380", "app", "secret"));

        // Assert
        provider.Current.Should().Be(new ValkeyConnectionParams(
            "localhost:6379,localhost:6380", "app", "secret", Ssl: false));
    }

    [Fact]
    public void Current_Null_WhenEndpointsEmpty()
    {
        // Arrange
        var monitor = new FakeOptionsMonitor<ValkeyOptions>();
        var provider = new ConfigurationValkeyConnectionProvider(monitor);

        // Act
        monitor.Set(Options("   "));

        // Assert — fail-open: параметров нет
        provider.Current.Should().BeNull();
    }

    [Fact]
    public void OnChange_Fires_OnlyOnActualChange()
    {
        // Arrange
        var monitor = new FakeOptionsMonitor<ValkeyOptions>();
        monitor.Set(Options("localhost:6379", "app", "old"));
        var provider = new ConfigurationValkeyConnectionProvider(monitor);
        var fired = 0;
        provider.OnChange(() => fired++);

        // Act — «перезагрузка» без изменения значения, затем реальная ротация
        monitor.Set(Options("localhost:6379", "app", "old"));
        monitor.Set(Options("localhost:6379", "app", "new"));

        // Assert — шум поглощён, ротация стрельнула
        fired.Should().Be(1);
    }

    [Fact]
    public void OnChange_Fires_OnTransitionToNull()
    {
        // Arrange
        var monitor = new FakeOptionsMonitor<ValkeyOptions>();
        monitor.Set(Options("localhost:6379"));
        var provider = new ConfigurationValkeyConnectionProvider(monitor);
        var fired = 0;
        provider.OnChange(() => fired++);

        // Act — параметры исчезли
        monitor.Set(Options(""));

        // Assert
        fired.Should().Be(1);
        provider.Current.Should().BeNull();
    }

    [Fact]
    public void ToString_RedactsPassword()
    {
        // Arrange
        var p = new ValkeyConnectionParams("localhost:6379", "app", "secret", false);

        // Act
        var text = p.ToString();

        // Assert — секрет не светится
        text.Should().NotContain("secret").And.Contain("***");
    }
}
```

- [ ] **2.4. Проверка и коммит**

```bash
cd /Users/demakaev/ZCodeProject/Puzzle
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PuzzleServer.Api.slnx        # 0 warnings
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PuzzleServer.UnitTests --filter "FullyQualifiedName~UnitTests.Valkey"   # PASS
git add src/PuzzleServer.Infrastructure.App.Valkey/ValkeyConnectionParams.cs \
        src/PuzzleServer.Infrastructure.App.Valkey/IValkeyConnectionProvider.cs \
        src/PuzzleServer.UnitTests/Valkey/Fakes/FakeOptionsMonitor.cs \
        src/PuzzleServer.UnitTests/Valkey/ConfigurationValkeyConnectionProviderTests.cs
git commit -m "feat(valkey): шов IValkeyConnectionProvider + ValkeyConnectionParams + Aspire-провайдер (value-equality OnChange), юниты"
```

---

## Шаг 3: Шов — DiscoveryValkeyConnectionProvider (Ф2 §8.2)

**Вход:** Шаг 2 закоммичен; доступен public API t04: `IValkeyDiscoveryStore` (`Get`/`Updated`), `ValkeyClusterSnapshot.GetClientConfig()` → `ValkeyClientConfig(Endpoints, Username, Password, Ssl)`, `ValkeyAppSecret(Username, Password)`, `HaValkeyException`.

**Действие:** HaDb-провайдер 1:1 `DiscoveryKafkaConnectionProvider` (t10): `Current` из стора, подписка `Updated` своего кластера, baseline ДО подписки, value-equality фильтр; fakes `FakeValkeyDiscoveryStore`/`FakeValkeyConnectionProvider`; юниты.

**Выход:** обе ветки шова готовы; fake-провайдер для юнитов holder'а (Шаг 5).

**Проверка:** `dotnet test src/PuzzleServer.UnitTests --filter "FullyQualifiedName~UnitTests.Valkey"` — PASS.

**Связь со spec:** §4.2 (Discovery-провайдер, инвариант baseline), §5 (шум/ротация), §6.1, §8 Ф2.

**Interfaces (Consumes:** t04 `IValkeyDiscoveryStore`, `ValkeyClusterSnapshot`, `ValkeyClientConfig`. **Produces:** `DiscoveryValkeyConnectionProvider` для Шага 8; fakes для Шага 5.)

- [ ] **3.1. Fake стора** — `src/PuzzleServer.UnitTests/Valkey/Fakes/FakeValkeyDiscoveryStore.cs` (зеркало `Kafka/Fakes/FakeDiscoveryStore.cs`):

```csharp
using PuzzleServer.Infrastructure.App;
using PuzzleServer.Infrastructure.App.HA.Valkey;
using PuzzleServer.Infrastructure.App.HA.Valkey.Model;

namespace PuzzleServer.UnitTests.Valkey.Fakes;

// Ручной IValkeyDiscoveryStore: Publish заменяет снапшот и стреляет Updated.
public sealed class FakeValkeyDiscoveryStore : IValkeyDiscoveryStore
{
    private readonly Dictionary<string, ValkeyClusterSnapshot> _snapshots = new();
    private readonly List<Action<ValkeyClusterSnapshot>> _handlers = [];
    private readonly object _gate = new();

    public event Action<ValkeyClusterSnapshot>? Updated
    {
        add { lock (_gate) _handlers.Add(value); }
        remove { lock (_gate) _handlers.Remove(value); }
    }

    public Result<ValkeyClusterSnapshot> Get(string cluster)
    {
        lock (_gate)
            return _snapshots.TryGetValue(cluster, out var snapshot)
                ? Result<ValkeyClusterSnapshot>.Success(snapshot)
                : Result<ValkeyClusterSnapshot>.Failed(new HaValkeyException($"valkey-кластер {cluster} не заявлен"));
    }

    public Task<Result<ValkeyClusterSnapshot>> RefreshAsync(string cluster, CancellationToken ct)
        => Task.FromResult(Get(cluster));

    public void Publish(ValkeyClusterSnapshot snapshot)
    {
        Action<ValkeyClusterSnapshot>[] handlers;
        lock (_gate)
        {
            _snapshots[snapshot.Cluster] = snapshot;
            handlers = [.. _handlers];
        }
        foreach (var handler in handlers)
            handler(snapshot);
    }
}
```

- [ ] **3.2. Fake провайдера** — `src/PuzzleServer.UnitTests/Valkey/Fakes/FakeValkeyConnectionProvider.cs` (зеркало `Kafka/Fakes/FakeConnectionProvider.cs`):

```csharp
using PuzzleServer.Infrastructure.App.Valkey;

namespace PuzzleServer.UnitTests.Valkey.Fakes;

// Ручной IValkeyConnectionProvider: Set заменяет Current и оповещает подписчиков.
public sealed class FakeValkeyConnectionProvider : IValkeyConnectionProvider
{
    private readonly List<Action> _handlers = [];
    private readonly object _gate = new();

    public ValkeyConnectionParams? Current { get; private set; }

    public IDisposable OnChange(Action handler)
    {
        lock (_gate) _handlers.Add(handler);
        return new Unsubscription(() => { lock (_gate) _handlers.Remove(handler); });
    }

    public int HandlerCount { get { lock (_gate) return _handlers.Count; } }

    public void Set(ValkeyConnectionParams? p)
    {
        Current = p;
        Action[] snapshot;
        lock (_gate) snapshot = [.. _handlers];
        foreach (var handler in snapshot)
            handler();
    }

    private sealed class Unsubscription(Action unsubscribe) : IDisposable
    {
        public void Dispose() => unsubscribe();
    }
}
```

- [ ] **3.3. Discovery-провайдер** — `src/PuzzleServer.Infrastructure.App.Valkey/DiscoveryValkeyConnectionProvider.cs` (адаптация `DiscoveryKafkaConnectionProvider.cs`):

```csharp
using Microsoft.Extensions.Logging;
using PuzzleServer.Infrastructure.App.HA.Valkey;
using PuzzleServer.Infrastructure.App.HA.Valkey.Model;

namespace PuzzleServer.Infrastructure.App.Valkey;

// HaDb-ветка (спека §4.2): параметры из etcd-снапшота HA.Valkey. Current — вычисление
// из Get(cluster).GetClientConfig() (null при Failed/null-конфиге — fail-open); State
// кластера НЕ интерпретируется (спека §3 п.7). OnChange — подписка на store.Updated
// своего кластера с фильтром по value-equality вычисленных параметров (шум не-соединительных
// изменений не проходит); baseline дедупа — значение на момент ПЕРВОЙ ПОДПИСКИ
// (инвариант t10), обновляется только событиями. Hosted-сервисов нет: актуализацией
// владеет HA.Valkey (ValkeyDiscoveryRefresher).
internal sealed class DiscoveryValkeyConnectionProvider(
    IValkeyDiscoveryStore store,
    string cluster,
    ILogger<DiscoveryValkeyConnectionProvider> logger) : IValkeyConnectionProvider
{
    private readonly object _gate = new();
    private Action[] _handlers = [];
    private bool _subscribed;
    // Baseline дедупа: параметры на момент первой подписки (Current его не трогает)
    private ValkeyConnectionParams? _last;

    // Всегда свежо и без сети: Get — мгновенно из кэша стора (t04)
    public ValkeyConnectionParams? Current => Compute(store.Get(cluster));

    public IDisposable OnChange(Action handler)
    {
        lock (_gate)
        {
            _handlers = [.. _handlers, handler];
            if (_subscribed)
                return new Subscription(this, handler);
            _subscribed = true;
            // Baseline фиксируем ДО подписки на событие: ValkeyDiscoveryStore.Publish
            // сначала ЗАПИСЫВАЕТ снапшот в кэш стора и только потом зовёт handlers —
            // вычисление baseline внутри OnStoreUpdated сравнивало бы новое значение
            // с самим собой и ПОГЛОЩАЛО первое изменение после подписки (баг-ревью t10).
            _last = Compute(store.Get(cluster));
            store.Updated += OnStoreUpdated;
        }
        return new Subscription(this, handler);
    }

    private void OnStoreUpdated(ValkeyClusterSnapshot snapshot)
    {
        if (snapshot.Cluster != cluster)
            return;

        var latest = ComputeFromSnapshot(snapshot);
        Action[] handlers;
        lock (_gate)
        {
            if (Equals(_last, latest))
                return; // соединительные параметры не изменились — шум (спека §5)
            _last = latest;
            handlers = [.. _handlers];
        }
        foreach (var handler in handlers)
        {
            try
            {
                handler();
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "IValkeyConnectionProvider: подписчик OnChange бросил исключение (погашено)");
            }
        }
    }

    private static ValkeyConnectionParams? Compute(Result<ValkeyClusterSnapshot> result)
        => result.IsSuccess ? ComputeFromSnapshot(result.Value) : null;

    private static ValkeyConnectionParams? ComputeFromSnapshot(ValkeyClusterSnapshot snapshot)
        => snapshot.GetClientConfig() is { } cc
            ? new ValkeyConnectionParams(cc.Endpoints, cc.Username, cc.Password, cc.Ssl)
            : null;

    private void Unsubscribe(Action handler)
    {
        lock (_gate)
            _handlers = _handlers.Where(h => !ReferenceEquals(h, handler)).ToArray();
    }

    private sealed class Subscription(DiscoveryValkeyConnectionProvider owner, Action handler) : IDisposable
    {
        public void Dispose() => owner.Unsubscribe(handler);
    }
}
```

- [ ] **3.4. Юниты** — `src/PuzzleServer.UnitTests/Valkey/DiscoveryValkeyConnectionProviderTests.cs`:

```csharp
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using PuzzleServer.Infrastructure.App.HA.Valkey.Model;
using PuzzleServer.Infrastructure.App.Valkey;
using PuzzleServer.UnitTests.Valkey.Fakes;
using Xunit;

namespace PuzzleServer.UnitTests.Valkey;

// HaDb-провайдер шова (спека §4.2): Current из стора, baseline до подписки,
// value-equality фильтр шума, ротация стреляет
public class DiscoveryValkeyConnectionProviderTests
{
    private const string Cluster = "cache";

    // Позиционные параметры ValkeyClusterSnapshot (t04):
    // (Cluster, State, Endpoints, App, FetchedAtUtc, Revision)
    private static ValkeyClusterSnapshot Snapshot(
        string cluster = Cluster,
        string? endpoints = "localhost:6379",
        string? user = "app",
        string? password = "old",
        string? state = null)
        => new(cluster, state, endpoints,
            user is null || password is null ? null : new ValkeyAppSecret(user, password),
            DateTimeOffset.UtcNow, revision: 1);

    private static DiscoveryValkeyConnectionProvider Provider(FakeValkeyDiscoveryStore store)
        => new(store, Cluster, NullLogger<DiscoveryValkeyConnectionProvider>.Instance);

    [Fact]
    public void Current_FromStoreSnapshot()
    {
        // Arrange
        var store = new FakeValkeyDiscoveryStore();
        store.Publish(Snapshot());
        var provider = Provider(store);

        // Act / Assert
        provider.Current.Should().Be(new ValkeyConnectionParams("localhost:6379", "app", "old", false));
    }

    [Fact]
    public void Current_Null_WhenNoEndpointsOrSecret()
    {
        // Arrange — снапшот без endpoints → GetClientConfig() == null
        var store = new FakeValkeyDiscoveryStore();
        store.Publish(Snapshot(endpoints: null));
        var provider = Provider(store);

        // Act / Assert — fail-open
        provider.Current.Should().BeNull();
    }

    [Fact]
    public void Current_Null_WhenClusterNotInStore()
    {
        // Arrange — стор пуст (etcd ещё не отдал снапшот / кластер не заявлен)
        var provider = Provider(new FakeValkeyDiscoveryStore());

        // Act / Assert
        provider.Current.Should().BeNull();
    }

    [Fact]
    public void OnChange_BaselineFixedBeforeSubscription_NoFireOnSameValue()
    {
        // Arrange — снапшот уже в сторе ДО первой подписки (инвариант baseline)
        var store = new FakeValkeyDiscoveryStore();
        store.Publish(Snapshot(password: "old"));
        var provider = Provider(store);
        var fired = 0;
        provider.OnChange(() => fired++);

        // Act — публикация того же содержимого (параметры равны baseline)
        store.Publish(Snapshot(password: "old"));

        // Assert
        fired.Should().Be(0);
    }

    [Fact]
    public void OnChange_FiresOnPasswordRotation()
    {
        // Arrange
        var store = new FakeValkeyDiscoveryStore();
        store.Publish(Snapshot(password: "old"));
        var provider = Provider(store);
        var fired = 0;
        provider.OnChange(() => fired++);

        // Act — ротация app_password (arch/21 §5 E)
        store.Publish(Snapshot(password: "new"));

        // Assert
        fired.Should().Be(1);
        provider.Current.Should().Be(new ValkeyConnectionParams("localhost:6379", "app", "new", false));
    }

    [Fact]
    public void OnChange_FiresOnEndpointsChange()
    {
        // Arrange
        var store = new FakeValkeyDiscoveryStore();
        store.Publish(Snapshot(endpoints: "localhost:6379"));
        var provider = Provider(store);
        var fired = 0;
        provider.OnChange(() => fired++);

        // Act — смена endpoints
        store.Publish(Snapshot(endpoints: "localhost:6380,localhost:6381"));

        // Assert — value-equality по ВСЕМУ ValkeyConnectionParams (спека §5)
        fired.Should().Be(1);
    }

    [Fact]
    public void OnChange_SilentOnNonConnectionNoise()
    {
        // Arrange — шум: State (не-соединительное поле, Updated стреляет от SameContent)
        var store = new FakeValkeyDiscoveryStore();
        store.Publish(Snapshot(state: null));
        var provider = Provider(store);
        var fired = 0;
        provider.OnChange(() => fired++);

        // Act — изменение State
        store.Publish(Snapshot(state: "DRAINING"));

        // Assert — фильтр поглотил (спека §5)
        fired.Should().Be(0);
    }

    [Fact]
    public void OnChange_IgnoresOtherClusters()
    {
        // Arrange
        var store = new FakeValkeyDiscoveryStore();
        store.Publish(Snapshot());
        var provider = Provider(store);
        var fired = 0;
        provider.OnChange(() => fired++);

        // Act — событие ЧУЖОГО кластера
        store.Publish(Snapshot(cluster: "other", endpoints: "localhost:7000"));

        // Assert
        fired.Should().Be(0);
    }

    [Fact]
    public void OnChange_TransitionToNull_Fires()
    {
        // Arrange — полный снапшот; потом неполный (секрет исчез)
        var store = new FakeValkeyDiscoveryStore();
        store.Publish(Snapshot());
        var provider = Provider(store);
        var fired = 0;
        provider.OnChange(() => fired++);

        // Act — креды удалены → GetClientConfig() == null
        store.Publish(Snapshot(user: null));

        // Assert — params→null стреляет (holder обязан dispose текущее соединение)
        fired.Should().Be(1);
        provider.Current.Should().BeNull();
    }

    [Fact]
    public void OnChange_HandlerException_Swallowed()
    {
        // Arrange
        var store = new FakeValkeyDiscoveryStore();
        store.Publish(Snapshot());
        var provider = Provider(store);
        provider.OnChange(() => throw new InvalidOperationException("подписчик сломан"));
        var fired = 0;
        provider.OnChange(() => fired++);

        // Act — ротация после падения первого подписчика
        store.Publish(Snapshot(password: "new"));

        // Assert — исключение первого не роняет нотификацию второму
        fired.Should().Be(1);
    }
}
```

- [ ] **3.5. Проверка и коммит**

```bash
cd /Users/demakaev/ZCodeProject/Puzzle
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PuzzleServer.Api.slnx        # 0 warnings
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PuzzleServer.UnitTests --filter "FullyQualifiedName~UnitTests.Valkey"   # PASS
git add src/PuzzleServer.Infrastructure.App.Valkey/DiscoveryValkeyConnectionProvider.cs \
        src/PuzzleServer.UnitTests/Valkey/Fakes/FakeValkeyDiscoveryStore.cs \
        src/PuzzleServer.UnitTests/Valkey/Fakes/FakeValkeyConnectionProvider.cs \
        src/PuzzleServer.UnitTests/Valkey/DiscoveryValkeyConnectionProviderTests.cs
git commit -m "feat(valkey): DiscoveryValkeyConnectionProvider — HaDb-ветка шова из IValkeyDiscoveryStore (baseline до подписки, value-equality фильтр), fakes + юниты"
```

---

## Шаг 4: Маппинг ValkeyConnectionParams → ConfigurationOptions (Ф3 §8.3)

**Вход:** Шаг 3 закоммичен.

**Действие:** чистая функция `ValkeyConnectionOptions.Map`; юниты.

**Выход:** маппинг для holder'а (Шаг 5); критерий приёмки №2 (юнит-тест маппинга).

**Проверка:** `dotnet test src/PuzzleServer.UnitTests --filter "FullyQualifiedName~UnitTests.Valkey"` — PASS.

**Связь со spec:** §4.3 (маппинг), §10 п.2, §8 Ф3.

**Interfaces (Produces:** `ValkeyConnectionOptions.Map(ValkeyConnectionParams) : ConfigurationOptions` — использует Шаг 5.)

- [ ] **4.1. Реализация** — `src/PuzzleServer.Infrastructure.App.Valkey/ValkeyConnectionOptions.cs`:

```csharp
using StackExchange.Redis;

namespace PuzzleServer.Infrastructure.App.Valkey;

// Маппинг ValkeyConnectionParams -> ConfigurationOptions (спека §4.3). Надёжностный
// дефолт: AbortOnConnectFail=false — multiplexer переживает недоступность сервера и
// сам реконнектится; таймауты — дефолты StackExchange.Redis (v1 не настраиваем).
// Ssl маппится напрямую (t06 включит без переделок).
internal static class ValkeyConnectionOptions
{
    public static ConfigurationOptions Map(ValkeyConnectionParams p)
    {
        var options = new ConfigurationOptions
        {
            AbortOnConnectFail = false,
            Ssl = p.Ssl,
        };
        // Endpoints — строка "h1:p1,h2:p2" (контракт arch/20 §4): сплит по запятой с тримом
        foreach (var endpoint in p.Endpoints.Split(
                     ',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            options.EndPoints.Add(endpoint);
        // Креды — только парой: частичный набор (только user или только password) не валиден
        if (!string.IsNullOrWhiteSpace(p.Username) && !string.IsNullOrWhiteSpace(p.Password))
        {
            options.User = p.Username;
            options.Password = p.Password;
        }
        return options;
    }
}
```

- [ ] **4.2. Юниты** — `src/PuzzleServer.UnitTests/Valkey/ValkeyConnectionOptionsTests.cs`:

```csharp
using FluentAssertions;
using PuzzleServer.Infrastructure.App.Valkey;
using Xunit;

namespace PuzzleServer.UnitTests.Valkey;

// Маппинг в ConfigurationOptions (спека §4.3, критерий приёмки №2)
public class ValkeyConnectionOptionsTests
{
    [Fact]
    public void Map_SplitsEndpoints()
    {
        // Arrange
        var p = new ValkeyConnectionParams("h1:6379, h2:6380 ,h3:6381", "app", "secret", false);

        // Act
        var options = ValkeyConnectionOptions.Map(p);

        // Assert — сплит по запятой, пробелы срезаются
        options.EndPoints.Should().BeEquivalentTo(["h1:6379", "h2:6380", "h3:6381"]);
    }

    [Fact]
    public void Map_SetsCreds_WhenBothPresent()
    {
        // Arrange
        var p = new ValkeyConnectionParams("h1:6379", "app", "secret", false);

        // Act
        var options = ValkeyConnectionOptions.Map(p);

        // Assert
        options.User.Should().Be("app");
        options.Password.Should().Be("secret");
    }

    [Theory]
    [InlineData("", "secret")]   // пустой username
    [InlineData("app", "")]      // пустой password
    [InlineData(" ", "secret")]  // пробельный username
    public void Map_SkipsCreds_WhenIncomplete(string user, string password)
    {
        // Arrange — частичный набор кредов не валиден (спека §4.3)
        var p = new ValkeyConnectionParams("h1:6379", user, password, false);

        // Act
        var options = ValkeyConnectionOptions.Map(p);

        // Assert — подключение без ACL (локальный контур)
        options.User.Should().BeNull();
        options.Password.Should().BeNull();
    }

    [Fact]
    public void Map_AlwaysAbortOnConnectFailFalse()
    {
        // Arrange
        var p = new ValkeyConnectionParams("h1:6379", "app", "secret", false);

        // Act
        var options = ValkeyConnectionOptions.Map(p);

        // Assert — надёжностный дефолт (спека §3 п.5)
        options.AbortOnConnectFail.Should().BeFalse();
    }

    [Fact]
    public void Map_Ssl_MappedDirectly()
    {
        // Arrange / Act
        var options = ValkeyConnectionOptions.Map(new ValkeyConnectionParams("h1:6379", "", "", Ssl: true));

        // Assert — поле готово к t06 (TLS) без переделки маппинга
        options.Ssl.Should().BeTrue();
    }
}
```

- [ ] **4.3. Проверка и коммит**

```bash
cd /Users/demakaev/ZCodeProject/Puzzle
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PuzzleServer.Api.slnx        # 0 warnings
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PuzzleServer.UnitTests --filter "FullyQualifiedName~UnitTests.Valkey"   # PASS
git add src/PuzzleServer.Infrastructure.App.Valkey/ValkeyConnectionOptions.cs \
        src/PuzzleServer.UnitTests/Valkey/ValkeyConnectionOptionsTests.cs
git commit -m "feat(valkey): маппинг ValkeyConnectionParams -> ConfigurationOptions (endpoints-сплит, креды парой, AbortOnConnectFail=false), юниты"
```

---

## Шаг 5: Test-seam IValkeyClient + адаптер + ValkeyConnectionHolder (Ф3 §8.3)

**Вход:** Шаги 2–4 закоммичены.

**Действие:** internal-seam `IValkeyClient` (по одному методу на операцию `IValkeyCache`), `ValkeyClientAdapter` (единственное место `IDatabase`), `ValkeyClientFactory`, `ValkeyConnectionHolder` (лениво, hot-reload пересозданием, orphaned-диспос); `FakeValkeyClient`; юниты holder'а.

**Выход:** держатель соединения для `ValkeyCache` (Шаг 7) и builder'а (Шаг 8); механика критериев №2/№3 покрыта юнитами.

**Проверка:** `dotnet test src/PuzzleServer.UnitTests --filter "FullyQualifiedName~UnitTests.Valkey"` — PASS.

**Связь со spec:** §4.3 (holder), §4.4 (seam), §5 (ротация/недоступность/params→null), §6.1, §8 Ф3.

**Interfaces (Produces:** `ValkeyConnectionHolder.GetClientAsync(ct) : ValueTask<IValkeyClient?>` — использует Шаг 7; `ValkeyClientFactory.CreateAsync(ConfigurationOptions) : ValueTask<IValkeyClient>` — использует Шаг 8.)

- [ ] **5.1. Seam** — `src/PuzzleServer.Infrastructure.App.Valkey/IValkeyClient.cs`:

```csharp
using StackExchange.Redis;

namespace PuzzleServer.Infrastructure.App.Valkey;

// Тонкий internal-seam над IDatabase (паттерн t10 §7.3 D7): IDatabase тяжело подменять
// (~десятки членов), модуль зависит от этого интерфейса; юнит-тесты подменяют fake-клиентом
// (InternalsVisibleTo). По одному методу на операцию IValkeyCache (спека §4.4).
// ct — best effort: SE.Redis 2.x API без CancellationToken, адаптер проверяет ct до команды.
internal interface IValkeyClient : IAsyncDisposable
{
    ValueTask<RedisValue> StringGetAsync(string key, CancellationToken ct);

    ValueTask<bool> StringSetAsync(string key, RedisValue value, TimeSpan? ttl, CancellationToken ct);

    // SET NX (AddIfAbsent)
    ValueTask<bool> StringSetNotExistsAsync(string key, RedisValue value, TimeSpan? ttl, CancellationToken ct);

    // GETSET — возвращает СТАРОЕ значение (RedisValue.Null если ключа не было)
    ValueTask<RedisValue> StringGetSetAsync(string key, RedisValue value, CancellationToken ct);

    ValueTask<bool> KeyDeleteAsync(string key, CancellationToken ct);

    ValueTask<bool> KeyExistsAsync(string key, CancellationToken ct);

    // INCRBY
    ValueTask<long> StringIncrementAsync(string key, long delta, CancellationToken ct);

    // MGET
    ValueTask<RedisValue[]> StringGetMultipleAsync(IReadOnlyCollection<string> keys, CancellationToken ct);

    // батч SET с TTL (MSET TTL не несёт — спека §4.5): true, если все SET прошли
    ValueTask<bool> StringSetMultipleAsync(
        IReadOnlyCollection<KeyValuePair<string, RedisValue>> pairs, TimeSpan? ttl, CancellationToken ct);

    ValueTask<bool> KeyExpireAsync(string key, TimeSpan ttl, CancellationToken ct);

    // TTL ключа; null — ключа нет или TTL не задан
    ValueTask<TimeSpan?> KeyTimeToLiveAsync(string key, CancellationToken ct);
}
```

- [ ] **5.2. Адаптер** — `src/PuzzleServer.Infrastructure.App.Valkey/ValkeyClientAdapter.cs`:

```csharp
using StackExchange.Redis;

namespace PuzzleServer.Infrastructure.App.Valkey;

// Единственное место, знающее IDatabase (спека §4.4). Создаётся holder'ом от текущего
// multiplexer'а. Исключения библиотеки НЕ глотаются — их инкапсулирует ValkeyCache в Result.
internal sealed class ValkeyClientAdapter : IValkeyClient
{
    private readonly ConnectionMultiplexer _multiplexer;
    private readonly IDatabase _db;

    public ValkeyClientAdapter(ConnectionMultiplexer multiplexer)
    {
        _multiplexer = multiplexer;
        _db = multiplexer.GetDatabase();
    }

    public ValueTask<RedisValue> StringGetAsync(string key, CancellationToken ct)
    { ct.ThrowIfCancellationRequested(); return _db.StringGetAsync(key); }

    public ValueTask<bool> StringSetAsync(string key, RedisValue value, TimeSpan? ttl, CancellationToken ct)
    { ct.ThrowIfCancellationRequested(); return _db.StringSetAsync(key, value, ttl); }

    public ValueTask<bool> StringSetNotExistsAsync(string key, RedisValue value, TimeSpan? ttl, CancellationToken ct)
    { ct.ThrowIfCancellationRequested(); return _db.StringSetAsync(key, value, ttl, When.NotExists); }

    public ValueTask<RedisValue> StringGetSetAsync(string key, RedisValue value, CancellationToken ct)
    { ct.ThrowIfCancellationRequested(); return _db.StringGetSetAsync(key, value); }

    public ValueTask<bool> KeyDeleteAsync(string key, CancellationToken ct)
    { ct.ThrowIfCancellationRequested(); return _db.KeyDeleteAsync(key); }

    public ValueTask<bool> KeyExistsAsync(string key, CancellationToken ct)
    { ct.ThrowIfCancellationRequested(); return _db.KeyExistsAsync(key); }

    public ValueTask<long> StringIncrementAsync(string key, long delta, CancellationToken ct)
    { ct.ThrowIfCancellationRequested(); return _db.StringIncrementAsync(key, delta); }

    public ValueTask<RedisValue[]> StringGetMultipleAsync(IReadOnlyCollection<string> keys, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        return _db.StringGetAsync([.. keys]);
    }

    public async ValueTask<bool> StringSetMultipleAsync(
        IReadOnlyCollection<KeyValuePair<string, RedisValue>> pairs, TimeSpan? ttl, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        // Батч: один round-trip на набор, TTL у каждого SET (спека §4.5)
        using var batch = _db.CreateBatch();
        var tasks = pairs.Select(p => batch.StringSetAsync(p.Key, p.Value, ttl)).ToArray();
        batch.Execute();
        await Task.WhenAll(tasks);
        return tasks.All(t => t.Result);
    }

    public ValueTask<bool> KeyExpireAsync(string key, TimeSpan ttl, CancellationToken ct)
    { ct.ThrowIfCancellationRequested(); return _db.KeyExpireAsync(key, ttl); }

    public ValueTask<TimeSpan?> KeyTimeToLiveAsync(string key, CancellationToken ct)
    { ct.ThrowIfCancellationRequested(); return _db.KeyTimeToLiveAsync(key); }

    public ValueTask DisposeAsync() => _multiplexer.DisposeAsync();
}
```

ПРИМЕЧАНИЕ: точные приведения `string` → `RedisKey` (неявные конвертеры SE.Redis) и арность `StringGetAsync(string[])` сверить по сборке на шаге 5.6 — при необходимости добавить `(RedisKey)key` / `keys.Select(k => (RedisKey)k).ToArray()`. Семантика фиксирована этим кодом.

- [ ] **5.3. Фабрика** — `src/PuzzleServer.Infrastructure.App.Valkey/ValkeyClientFactory.cs`:

```csharp
using StackExchange.Redis;

namespace PuzzleServer.Infrastructure.App.Valkey;

// Единственное место построения ConnectionMultiplexer. Один multiplexer на процесс —
// SE.Redis сам мультиплексирует, пул не нужен (спека §4.3).
internal static class ValkeyClientFactory
{
    public static async ValueTask<IValkeyClient> CreateAsync(ConfigurationOptions options)
    {
        // AbortOnConnectFail=false (ValkeyConnectionOptions.Map): при недоступности
        // сервера ConnectAsync возвращает disconnected multiplexer — реконнект в фоне
        var multiplexer = await ConnectionMultiplexer.ConnectAsync(options);
        return new ValkeyClientAdapter(multiplexer);
    }
}
```

- [ ] **5.4. Holder** — `src/PuzzleServer.Infrastructure.App.Valkey/ValkeyConnectionHolder.cs`:

```csharp
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace PuzzleServer.Infrastructure.App.Valkey;

// Держатель единственного на процесс ConnectionMultiplexer (спека §4.3). Singleton.
// - Лениво: клиент строится при первом обращении операций, когда provider.Current != null;
//   до того GetClientAsync == null → операции кеша Result.Failed (fail-open).
// - Hot-reload: подписка provider.OnChange (лениво, при первом обращении) → перестроение
//   НОВЫМ клиентом по новым параметрам; старый уходит в orphaned и диспозится только в
//   DisposeAsync holder'а — идущие операции дорабатывают на старом экземпляре без разрыва
//   (ротация app_password: окно двух паролей arch/21 §5 E держит OLD валидным; паттерн
//   orphaned-кэша KafkaProducerBuilder t10).
// - Current стал null (параметры исчезли): текущий клиент в orphaned, операции снова
//   fail-open (симметрия).
// - Ошибки построения: лог + клиент остаётся прежним (или null) — наружу не бросаются
//   из RebuildAsync; из GetClientAsync бросаются (ValkeyCache инкапсулирует в Result).
internal sealed class ValkeyConnectionHolder : IAsyncDisposable
{
    private readonly IValkeyConnectionProvider _provider;
    private readonly Func<ConfigurationOptions, ValueTask<IValkeyClient>> _clientFactory;
    private readonly ILogger<ValkeyConnectionHolder> _logger;
    private readonly SemaphoreSlim _buildGate = new(1, 1);
    private readonly object _sync = new();
    private readonly List<IValkeyClient> _orphaned = [];
    private ValkeyConnectionParams? _builtFor;
    private IValkeyClient? _client;
    private IDisposable? _subscription;
    private bool _changeSubscribed;

    public ValkeyConnectionHolder(
        IValkeyConnectionProvider provider,
        Func<ConfigurationOptions, ValueTask<IValkeyClient>> clientFactory,
        ILogger<ValkeyConnectionHolder> logger)
    {
        _provider = provider;
        _clientFactory = clientFactory;
        _logger = logger;
    }

    // Срез клиента для операции: null = параметров нет (fail-open, спека §3 п.4)
    public async ValueTask<IValkeyClient?> GetClientAsync(CancellationToken ct)
    {
        SubscribeToChanges();
        var parameters = _provider.Current;
        if (parameters is null)
            return null;

        lock (_sync)
        {
            if (_client is not null && _builtFor == parameters)
                return _client; // актуальный клиент уже построен
        }

        await _buildGate.WaitAsync(ct);
        try
        {
            parameters = _provider.Current; // параметры могли измениться, пока ждали семафор
            if (parameters is null)
                return null;
            lock (_sync)
            {
                if (_client is not null && _builtFor == parameters)
                    return _client; // конкурент уже построил по тем же параметрам
            }

            var client = await _clientFactory(ValkeyConnectionOptions.Map(parameters));
            lock (_sync)
            {
                if (_client is not null && _builtFor == parameters)
                {
                    _orphaned.Add(client); // параллельное построение выиграно другим — этот лишний
                    return _client;
                }
                if (_client is not null)
                    _orphaned.Add(_client); // старый дорабатывает in-flight операции, диспоз в DisposeAsync
                _client = client;
                _builtFor = parameters;
                return client;
            }
        }
        finally
        {
            _buildGate.Release();
        }
    }

    // Ленивая подписка (паттерн KafkaProducerBuilder): holder, к которому ни разу не
    // обращались, не слушает события — юниты и невыключенные модули не тянут rebuild-логику.
    private void SubscribeToChanges()
    {
        lock (_sync)
        {
            if (_changeSubscribed)
                return;
            _changeSubscribed = true;
        }
        _subscription = _provider.OnChange(() => _ = RebuildAsync());
    }

    private async Task RebuildAsync()
    {
        var parameters = _provider.Current;
        try
        {
            await _buildGate.WaitAsync(CancellationToken.None);
            try
            {
                lock (_sync)
                {
                    if (parameters is null)
                    {
                        // параметры исчезли: dispose текущего (в orphaned), fail-open
                        if (_client is null)
                            return;
                        _orphaned.Add(_client);
                        _client = null;
                        _builtFor = null;
                        return;
                    }
                    if (_client is not null && _builtFor == parameters)
                        return; // уже актуален (шум отфильтрован провайдером — страховка)
                }
                var client = await _clientFactory(ValkeyConnectionOptions.Map(parameters));
                lock (_sync)
                {
                    if (_client is not null)
                        _orphaned.Add(_client); // старый дорабатывает, диспоз в DisposeAsync
                    _client = client;
                    _builtFor = parameters;
                }
                _logger.LogInformation("ValkeyConnectionHolder: соединение перестроено (hot-reload параметров)");
            }
            finally
            {
                _buildGate.Release();
            }
        }
        catch (Exception ex)
        {
            // fail-open: прежний клиент (или его отсутствие) сохраняется
            _logger.LogWarning(ex, "ValkeyConnectionHolder: перестроение соединения не удалось");
        }
    }

    public async ValueTask DisposeAsync()
    {
        _subscription?.Dispose();
        List<IValkeyClient> toDispose;
        lock (_sync)
        {
            toDispose = [.. _orphaned];
            _orphaned.Clear();
            if (_client is not null)
                toDispose.Add(_client);
            _client = null;
            _builtFor = null;
        }
        foreach (var client in toDispose)
            await client.DisposeAsync();
    }
}
```

- [ ] **5.5. Fake клиента** — `src/PuzzleServer.UnitTests/Valkey/Fakes/FakeValkeyClient.cs`:

```csharp
using PuzzleServer.Infrastructure.App.Valkey;
using StackExchange.Redis;

namespace PuzzleServer.UnitTests.Valkey.Fakes;

// Словарный fake IValkeyClient для юнитов ValkeyCache/holder'а: значения, TTL,
// порядок обращений (проверка префиксов), флаг dispose (orphaned-механика),
// ThrowNext — симуляция сбоя библиотеки.
public sealed class FakeValkeyClient : IValkeyClient
{
    public Dictionary<string, RedisValue> Store { get; } = new();
    public Dictionary<string, TimeSpan?> Ttls { get; } = new();
    public List<string> Calls { get; } = [];
    public bool Disposed { get; private set; }
    public Exception? ThrowNext { get; set; }

    private void MaybeThrow()
    {
        if (ThrowNext is { } ex)
        {
            ThrowNext = null;
            throw ex;
        }
    }

    public ValueTask<RedisValue> StringGetAsync(string key, CancellationToken ct)
    {
        MaybeThrow();
        Calls.Add(key);
        return ValueTask.FromResult(Store.TryGetValue(key, out var value) ? value : RedisValue.Null);
    }

    public ValueTask<bool> StringSetAsync(string key, RedisValue value, TimeSpan? ttl, CancellationToken ct)
    {
        MaybeThrow();
        Calls.Add(key);
        Store[key] = value;
        Ttls[key] = ttl;
        return ValueTask.FromResult(true);
    }

    public ValueTask<bool> StringSetNotExistsAsync(string key, RedisValue value, TimeSpan? ttl, CancellationToken ct)
    {
        MaybeThrow();
        Calls.Add(key);
        if (Store.ContainsKey(key))
            return ValueTask.FromResult(false);
        Store[key] = value;
        Ttls[key] = ttl;
        return ValueTask.FromResult(true);
    }

    public ValueTask<RedisValue> StringGetSetAsync(string key, RedisValue value, CancellationToken ct)
    {
        MaybeThrow();
        Calls.Add(key);
        var old = Store.TryGetValue(key, out var v) ? v : RedisValue.Null;
        Store[key] = value;
        Ttls.Remove(key); // GETSET сбрасывает TTL (семантика Redis)
        return ValueTask.FromResult(old);
    }

    public ValueTask<bool> KeyDeleteAsync(string key, CancellationToken ct)
    {
        MaybeThrow();
        Calls.Add(key);
        Store.Remove(key);
        Ttls.Remove(key);
        return ValueTask.FromResult(true);
    }

    public ValueTask<bool> KeyExistsAsync(string key, CancellationToken ct)
    {
        MaybeThrow();
        Calls.Add(key);
        return ValueTask.FromResult(Store.ContainsKey(key));
    }

    public ValueTask<long> StringIncrementAsync(string key, long delta, CancellationToken ct)
    {
        MaybeThrow();
        Calls.Add(key);
        var current = Store.TryGetValue(key, out var v) ? (long)v : 0L;
        var result = current + delta;
        Store[key] = result;
        return ValueTask.FromResult(result);
    }

    public ValueTask<RedisValue[]> StringGetMultipleAsync(IReadOnlyCollection<string> keys, CancellationToken ct)
    {
        MaybeThrow();
        Calls.AddRange(keys);
        return ValueTask.FromResult(
            keys.Select(k => Store.TryGetValue(k, out var v) ? v : RedisValue.Null).ToArray());
    }

    public ValueTask<bool> StringSetMultipleAsync(
        IReadOnlyCollection<KeyValuePair<string, RedisValue>> pairs, TimeSpan? ttl, CancellationToken ct)
    {
        MaybeThrow();
        foreach (var pair in pairs)
        {
            Calls.Add(pair.Key);
            Store[pair.Key] = pair.Value;
            Ttls[pair.Key] = ttl;
        }
        return ValueTask.FromResult(true);
    }

    public ValueTask<bool> KeyExpireAsync(string key, TimeSpan ttl, CancellationToken ct)
    {
        MaybeThrow();
        Calls.Add(key);
        Ttls[key] = ttl;
        return ValueTask.FromResult(Store.ContainsKey(key));
    }

    public ValueTask<TimeSpan?> KeyTimeToLiveAsync(string key, CancellationToken ct)
    {
        MaybeThrow();
        Calls.Add(key);
        if (!Store.TryGetValue(key, out _))
            return ValueTask.FromResult<TimeSpan?>(null);
        return ValueTask.FromResult(Ttls.TryGetValue(key, out var ttl) ? ttl : null);
    }

    public ValueTask DisposeAsync()
    {
        Disposed = true;
        return ValueTask.CompletedTask;
    }
}
```

- [ ] **5.6. Юниты holder'а** — `src/PuzzleServer.UnitTests/Valkey/ValkeyConnectionHolderTests.cs`:

```csharp
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using PuzzleServer.Infrastructure.App.Valkey;
using PuzzleServer.UnitTests.Valkey.Fakes;
using StackExchange.Redis;
using Xunit;

namespace PuzzleServer.UnitTests.Valkey;

// Держатель соединения (спека §4.3): лениво, кэш по параметрам, hot-reload
// пересозданием, orphaned-диспос, fail-open
public class ValkeyConnectionHolderTests : IAsyncLifetime
{
    private readonly FakeValkeyConnectionProvider _provider = new();
    private readonly List<FakeValkeyClient> _built = [];
    private ValkeyConnectionHolder _holder = null!;

    public ValueTask InitializeAsync()
    {
        _holder = new ValkeyConnectionHolder(
            _provider,
            _ =>
            {
                var client = new FakeValkeyClient();
                _built.Add(client);
                return ValueTask.FromResult<IValkeyClient>(client);
            },
            NullLogger<ValkeyConnectionHolder>.Instance);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync() => await _holder.DisposeAsync();

    private static ValkeyConnectionParams Params(string password = "old")
        => new("localhost:6379", "app", password, false);

    [Fact]
    public async Task GetClient_WithoutParams_ReturnsNull_NoBuild()
    {
        // Arrange — параметров нет (fail-open)

        // Act
        var client = await _holder.GetClientAsync(CancellationToken.None);

        // Assert — операции отклоняются, фабрика не звалась
        client.Should().BeNull();
        _built.Should().BeEmpty();
    }

    [Fact]
    public async Task GetClient_WithParams_BuildsLazily_AndCachesByParams()
    {
        // Arrange
        _provider.Set(Params());

        // Act — два обращения с теми же параметрами
        var first = await _holder.GetClientAsync(CancellationToken.None);
        var second = await _holder.GetClientAsync(CancellationToken.None);

        // Assert — один multiplexer, тот же экземпляр
        first.Should().Be(second);
        _built.Should().HaveCount(1);
    }

    [Fact]
    public async Task OnChange_Rotation_Rebuilds_OldClientSurvivesUntilDispose()
    {
        // Arrange
        _provider.Set(Params("old"));
        var oldClient = (FakeValkeyClient)await _holder.GetClientAsync(CancellationToken.None);

        // Act — ротация app_password → OnChange → перестроение
        _provider.Set(Params("new"));
        var newClient = await _holder.GetClientAsync(CancellationToken.None);

        // Assert — новый экземпляр по новым параметрам; старый ещё жив (orphaned,
        // дорабатывает in-flight операции), dispose — только в DisposeAsync holder'а
        newClient.Should().NotBeSameAs(oldClient);
        oldClient.Disposed.Should().BeFalse();
        _built.Should().HaveCount(2);
    }

    [Fact]
    public async Task OnChange_ParamsToNull_ClientDropped_OperationsFailOpen()
    {
        // Arrange
        _provider.Set(Params());
        _ = await _holder.GetClientAsync(CancellationToken.None);

        // Act — параметры исчезли (ключи удалены из etcd)
        _provider.Set(null);
        var client = await _holder.GetClientAsync(CancellationToken.None);

        // Assert — fail-open симметричен (спека §4.3)
        client.Should().BeNull();
    }

    [Fact]
    public async Task DisposeAsync_DisposesCurrentAndOrphaned()
    {
        // Arrange — текущий + orphaned после ротации
        _provider.Set(Params("old"));
        _ = await _holder.GetClientAsync(CancellationToken.None);
        _provider.Set(Params("new"));
        _ = await _holder.GetClientAsync(CancellationToken.None);
        _built.Should().HaveCount(2);

        // Act
        await _holder.DisposeAsync();

        // Assert — все экземпляры диспознуты
        _built.Should().OnlyContain(c => c.Disposed);
    }

    [Fact]
    public async Task RepeatedAccess_SameParams_NoRebuild()
    {
        // Arrange — провайдер стреляет только при изменении; даже при повторном
        // обращении с теми же параметрами клиент не перестраивается
        _provider.Set(Params());
        var before = await _holder.GetClientAsync(CancellationToken.None);

        // Act
        var after = await _holder.GetClientAsync(CancellationToken.None);

        // Assert — страховка holder'а: сравнение параметров
        after.Should().BeSameAs(before);
        _built.Should().HaveCount(1);
    }

    [Fact]
    public async Task GetClient_BuildFailure_ThrowsOutward_ForCacheEncapsulation()
    {
        // Arrange — фабрика падает (сервер недоступен и т.п.)
        var failing = new ValkeyConnectionHolder(
            _provider,
            _ => ValueTask.FromException<IValkeyClient>(new RedisConnectionException(
                ConnectionFailureType.SocketFailure, "недоступен")),
            NullLogger<ValkeyConnectionHolder>.Instance);
        _provider.Set(Params());

        // Act / Assert — исключение наружу: ValkeyCache обязан инкапсулировать в Result.Failed
        var act = () => failing.GetClientAsync(CancellationToken.None);
        await act.Should().ThrowAsync<RedisConnectionException>();
        await failing.DisposeAsync();
    }
}
```

- [ ] **5.7. Проверка и коммит**

```bash
cd /Users/demakaev/ZCodeProject/Puzzle
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PuzzleServer.Api.slnx        # 0 warnings
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PuzzleServer.UnitTests --filter "FullyQualifiedName~UnitTests.Valkey"   # PASS
git add src/PuzzleServer.Infrastructure.App.Valkey/IValkeyClient.cs \
        src/PuzzleServer.Infrastructure.App.Valkey/ValkeyClientAdapter.cs \
        src/PuzzleServer.Infrastructure.App.Valkey/ValkeyClientFactory.cs \
        src/PuzzleServer.Infrastructure.App.Valkey/ValkeyConnectionHolder.cs \
        src/PuzzleServer.UnitTests/Valkey/Fakes/FakeValkeyClient.cs \
        src/PuzzleServer.UnitTests/Valkey/ValkeyConnectionHolderTests.cs
git commit -m "feat(valkey): IValkeyClient seam + адаптер IDatabase + фабрика multiplexer + ValkeyConnectionHolder (лениво/hot-reload/orphaned-dispose), fake + юниты"
```

---

## Шаг 6: Сериализация T ↔ RedisValue (Ф4 §8.4)

**Вход:** Шаг 5 закоммичен (`IValkeyClient` с `RedisValue` в сигнатурах).

**Действие:** `ValkeySerializer` — примитивы (`string`, `byte[]`, `bool`, `int`, `long`, `double`) нативно через конвертеры `RedisValue`, остальные типы — `System.Text.Json`; юниты фиксации.

**Выход:** сериализация для `ValkeyCache` (Шаг 7); критерий §4.5 «фиксируется юнит-тестами» закрыт.

**Проверка:** `dotnet test src/PuzzleServer.UnitTests --filter "FullyQualifiedName~UnitTests.Valkey"` — PASS.

**Связь со spec:** §4.5 (сериализация), §8 Ф4.

**Interfaces (Produces:** `ValkeySerializer.Serialize<T>(T) : RedisValue`; `ValkeySerializer.Deserialize<T>(RedisValue) : T?`.)

- [ ] **6.1. Реализация** — `src/PuzzleServer.Infrastructure.App.Valkey/ValkeySerializer.cs`:

```csharp
using System.Text.Json;
using StackExchange.Redis;

namespace PuzzleServer.Infrastructure.App.Valkey;

// Сериализация значений кеша (спека §4.5): примитивы — нативные конвертеры RedisValue
// (без JSON-обёрток), byte[] — бинарно, остальные типы — System.Text.Json (встроенный,
// без новых пакетов). Набор нативных типов фиксирован юнит-тестами; всё прочее
// (decimal, enum, Guid, DTO) — JSON.
internal static class ValkeySerializer
{
    public static RedisValue Serialize<T>(T value)
    {
        if (value is null)
            return RedisValue.Null;
        return value switch
        {
            string s => (RedisValue)s,
            byte[] b => (RedisValue)b,
            bool v => (RedisValue)v,
            int v => (RedisValue)v,
            long v => (RedisValue)v,
            double v => (RedisValue)v,
            _ => (RedisValue)JsonSerializer.Serialize(value, typeof(T)),
        };
    }

    public static T? Deserialize<T>(RedisValue value)
    {
        if (value.IsNull)
            return default;
        var type = typeof(T);
        if (type == typeof(string)) return (T)(object)value.ToString();
        if (type == typeof(byte[])) return (T)(object)(byte[])value;
        if (type == typeof(bool)) return (T)(object)(bool)value;
        if (type == typeof(int)) return (T)(object)(int)value;
        if (type == typeof(long)) return (T)(object)(long)value;
        if (type == typeof(double)) return (T)(object)(double)value;
        return JsonSerializer.Deserialize<T>(value.ToString());
    }
}
```

- [ ] **6.2. Юниты** — `src/PuzzleServer.UnitTests/Valkey/ValkeySerializerTests.cs`:

```csharp
using FluentAssertions;
using PuzzleServer.Infrastructure.App.Valkey;
using Xunit;

namespace PuzzleServer.UnitTests.Valkey;

// Сериализация значений (спека §4.5): примитивы нативно, остальное JSON
public class ValkeySerializerTests
{
    [Theory]
    [InlineData("привет")]
    [InlineData("")]
    public void String_RoundTrip_Raw(string value)
    {
        // Arrange / Act
        var stored = ValkeySerializer.Serialize(value);
        var restored = ValkeySerializer.Deserialize<string>(stored);

        // Assert — строка хранится как есть (не JSON-quoted)
        stored.ToString().Should().Be(value);
        restored.Should().Be(value);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [InlineData(42)]
    [InlineData(-7)]
    [InlineData(long.MaxValue)]
    [InlineData(3.5)]
    public void Primitives_RoundTrip<T>(T value)
    {
        // Arrange / Act
        var restored = ValkeySerializer.Deserialize<T>(ValkeySerializer.Serialize(value));

        // Assert
        restored.Should().Be(value);
    }

    [Fact]
    public void ByteArray_RoundTrip_Binary()
    {
        // Arrange
        byte[] value = [0, 1, 2, 255, 128];

        // Act
        var stored = ValkeySerializer.Serialize(value);
        var restored = ValkeySerializer.Deserialize<byte[]>(stored);

        // Assert — бинарное представление
        restored.Should().Equal(value);
    }

    [Fact]
    public void Dto_RoundTrip_Json()
    {
        // Arrange
        var value = new TestDto { Name = "session-1", Count = 7 };

        // Act
        var stored = ValkeySerializer.Serialize(value);
        var restored = ValkeySerializer.Deserialize<TestDto>(stored);

        // Assert — JSON для сложных типов
        restored.Should().BeEquivalentTo(value);
    }

    [Fact]
    public void Null_SerializesToRedisNull_DeserializesToDefault()
    {
        // Arrange / Act / Assert
        ValkeySerializer.Serialize<string>(null).IsNull.Should().BeTrue();
        ValkeySerializer.Deserialize<string>(RedisValue.Null).Should().BeNull();
    }

    public sealed record TestDto
    {
        public string Name { get; init; } = "";
        public int Count { get; init; }
    }
}
```

- [ ] **6.3. Проверка и коммит**

```bash
cd /Users/demakaev/ZCodeProject/Puzzle
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PuzzleServer.Api.slnx        # 0 warnings
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PuzzleServer.UnitTests --filter "FullyQualifiedName~UnitTests.Valkey"   # PASS
git add src/PuzzleServer.Infrastructure.App.Valkey/ValkeySerializer.cs \
        src/PuzzleServer.UnitTests/Valkey/ValkeySerializerTests.cs
git commit -m "feat(valkey): ValkeySerializer — примитивы нативно в RedisValue, DTO через System.Text.Json, юниты фиксации"
```

---

## Шаг 7: Доменная поверхность IValkeyCache (Ф4 §8.4)

**Вход:** Шаги 5–6 закоммичены (`ValkeyConnectionHolder`, `ValkeySerializer`, `IValkeyClient`).

**Действие:** public-интерфейс `IValkeyCache` (расширенный набор v1), internal-реализация `ValkeyCache` (префикс ко всем операциям, Result-обёртки, полный catch); юниты всех операций через `FakeValkeyClient`.

**Выход:** кеш-поверхность готова; §6.1 (операции, префикс, сериализация, TTL, fail-open) закрыты юнитами.

**Проверка:** `dotnet test src/PuzzleServer.UnitTests --filter "FullyQualifiedName~UnitTests.Valkey"` — PASS.

**Связь со spec:** §4.5, §6.1, §8 Ф4.

**Interfaces (Produces:** `IValkeyCache` — публичная поверхность для доменов и Шага 8; сигнатуры — точный канон spec §4.5.)

- [ ] **7.1. Интерфейс** — `src/PuzzleServer.Infrastructure.App.Valkey/IValkeyCache.cs`:

```csharp
using PuzzleServer.Infrastructure.App;

namespace PuzzleServer.Infrastructure.App.Valkey;

// Доменная кеш-поверхность модуля (спека §4.5): единственное, что видят домены —
// StackExchange.Redis инкапсулирован (спека §3 п.3). Все операции — Result-монада:
// исключения библиотеки и отсутствие соединения инкапсулируются в Result.Failed.
// Лёгкий (без собственного соединения) — не IAsyncDisposable; соединением владеет
// ValkeyConnectionHolder (dispose при shutdown приложения). Ключи изолируются
// префиксом "<KeyPrefix>:<key>" — домен никогда не видит префикс.
public interface IValkeyCache
{
    // базовые
    ValueTask<Result<T?>> GetAsync<T>(string key, CancellationToken ct = default);

    ValueTask<Result> SetAsync<T>(string key, T value, TimeSpan? ttl = null, CancellationToken ct = default);

    ValueTask<Result> RemoveAsync(string key, CancellationToken ct = default);

    ValueTask<Result<bool>> ExistsAsync(string key, CancellationToken ct = default);

    // атомарные
    // GETSET: старое значение + TTL ключа после SET (две команды, неатомарно относительно TTL)
    ValueTask<Result<T?>> GetSetAsync<T>(string key, T value, TimeSpan? ttl = null, CancellationToken ct = default);

    // SET NX: true, если ключа не было и значение записано
    ValueTask<Result<bool>> AddIfAbsentAsync<T>(string key, T value, TimeSpan? ttl = null, CancellationToken ct = default);

    // каунтеры
    ValueTask<Result<long>> IncrementAsync(string key, long delta = 1, CancellationToken ct = default);

    // массовые: словарь по ВСЕМ запрошенным ключам, отсутствующие — default(T?)
    ValueTask<Result<IReadOnlyDictionary<string, T?>>> GetManyAsync<T>(
        IReadOnlyCollection<string> keys, CancellationToken ct = default);

    ValueTask<Result> SetManyAsync<T>(
        IReadOnlyCollection<KeyValuePair<string, T>> pairs, TimeSpan? ttl = null, CancellationToken ct = default);

    // TTL-управление
    ValueTask<Result> KeyExpireAsync(string key, TimeSpan ttl, CancellationToken ct = default);

    ValueTask<Result<TimeSpan?>> KeyTimeToLiveAsync(string key, CancellationToken ct = default);
}
```

- [ ] **7.2. Реализация** — `src/PuzzleServer.Infrastructure.App.Valkey/ValkeyCache.cs`:

```csharp
using PuzzleServer.Infrastructure.App;
using StackExchange.Redis;

namespace PuzzleServer.Infrastructure.App.Valkey;

// Реализация IValkeyCache (спека §4.5): подставляет "<KeyPrefix>:<key>" ко всем операциям
// (в т.ч. элементам массовых), сериализует ValkeySerializer'ом, инкапсулирует ЛЮБЫЕ
// исключения (включая построение соединения и сбои библиотеки) в Result.Failed —
// наружу не бросается ничего (fail-open, спека §5).
internal sealed class ValkeyCache(string keyPrefix, ValkeyConnectionHolder holder) : IValkeyCache
{
    public async ValueTask<Result<T?>> GetAsync<T>(string key, CancellationToken ct = default)
    {
        try
        {
            var client = await holder.GetClientAsync(ct);
            if (client is null)
                return Failed<T?>();
            var value = await client.StringGetAsync(Full(key), ct);
            return Result<T?>.Success(value.IsNull ? default : ValkeySerializer.Deserialize<T>(value));
        }
        catch (Exception ex)
        {
            return Result<T?>.Failed(ex);
        }
    }

    public async ValueTask<Result> SetAsync<T>(string key, T value, TimeSpan? ttl = null, CancellationToken ct = default)
    {
        try
        {
            var client = await holder.GetClientAsync(ct);
            if (client is null)
                return Failed();
            await client.StringSetAsync(Full(key), ValkeySerializer.Serialize(value), ttl, ct);
            return Result.Success();
        }
        catch (Exception ex)
        {
            return Result.Failed(ex);
        }
    }

    public async ValueTask<Result> RemoveAsync(string key, CancellationToken ct = default)
    {
        try
        {
            var client = await holder.GetClientAsync(ct);
            if (client is null)
                return Failed();
            await client.KeyDeleteAsync(Full(key), ct);
            return Result.Success();
        }
        catch (Exception ex)
        {
            return Result.Failed(ex);
        }
    }

    public async ValueTask<Result<bool>> ExistsAsync(string key, CancellationToken ct = default)
    {
        try
        {
            var client = await holder.GetClientAsync(ct);
            if (client is null)
                return Failed<bool>();
            return Result<bool>.Success(await client.KeyExistsAsync(Full(key), ct));
        }
        catch (Exception ex)
        {
            return Result<bool>.Failed(ex);
        }
    }

    public async ValueTask<Result<T?>> GetSetAsync<T>(string key, T value, TimeSpan? ttl = null, CancellationToken ct = default)
    {
        try
        {
            var client = await holder.GetClientAsync(ct);
            if (client is null)
                return Failed<T?>();
            var fullKey = Full(key);
            var old = await client.StringGetSetAsync(fullKey, ValkeySerializer.Serialize(value), ct);
            // TTL ключа после SET (спека §4.5): GETSET сбрасывает TTL — восстанавливаем, если задан
            if (ttl is { } span)
                await client.KeyExpireAsync(fullKey, span, ct);
            return Result<T?>.Success(old.IsNull ? default : ValkeySerializer.Deserialize<T>(old));
        }
        catch (Exception ex)
        {
            return Result<T?>.Failed(ex);
        }
    }

    public async ValueTask<Result<bool>> AddIfAbsentAsync<T>(string key, T value, TimeSpan? ttl = null, CancellationToken ct = default)
    {
        try
        {
            var client = await holder.GetClientAsync(ct);
            if (client is null)
                return Failed<bool>();
            return Result<bool>.Success(
                await client.StringSetNotExistsAsync(Full(key), ValkeySerializer.Serialize(value), ttl, ct));
        }
        catch (Exception ex)
        {
            return Result<bool>.Failed(ex);
        }
    }

    public async ValueTask<Result<long>> IncrementAsync(string key, long delta = 1, CancellationToken ct = default)
    {
        try
        {
            var client = await holder.GetClientAsync(ct);
            if (client is null)
                return Failed<long>();
            return Result<long>.Success(await client.StringIncrementAsync(Full(key), delta, ct));
        }
        catch (Exception ex)
        {
            return Result<long>.Failed(ex);
        }
    }

    public async ValueTask<Result<IReadOnlyDictionary<string, T?>>> GetManyAsync<T>(
        IReadOnlyCollection<string> keys, CancellationToken ct = default)
    {
        try
        {
            var client = await holder.GetClientAsync(ct);
            if (client is null)
                return Failed<IReadOnlyDictionary<string, T?>>();
            var keyList = keys.ToList();
            var values = await client.StringGetMultipleAsync(keyList.Select(Full).ToList(), ct);
            // Словарь по ВСЕМ запрошенным ключам (в именовании домена), отсутствующие — default
            var result = new Dictionary<string, T?>(keyList.Count);
            for (var i = 0; i < keyList.Count; i++)
                result[keyList[i]] = values[i].IsNull ? default : ValkeySerializer.Deserialize<T>(values[i]);
            return Result<IReadOnlyDictionary<string, T?>>.Success(result);
        }
        catch (Exception ex)
        {
            return Result<IReadOnlyDictionary<string, T?>>.Failed(ex);
        }
    }

    public async ValueTask<Result> SetManyAsync<T>(
        IReadOnlyCollection<KeyValuePair<string, T>> pairs, TimeSpan? ttl = null, CancellationToken ct = default)
    {
        try
        {
            var client = await holder.GetClientAsync(ct);
            if (client is null)
                return Failed();
            await client.StringSetMultipleAsync(
                pairs.Select(p => new KeyValuePair<string, RedisValue>(Full(p.Key), ValkeySerializer.Serialize(p.Value))).ToList(),
                ttl, ct);
            return Result.Success();
        }
        catch (Exception ex)
        {
            return Result.Failed(ex);
        }
    }

    public async ValueTask<Result> KeyExpireAsync(string key, TimeSpan ttl, CancellationToken ct = default)
    {
        try
        {
            var client = await holder.GetClientAsync(ct);
            if (client is null)
                return Failed();
            await client.KeyExpireAsync(Full(key), ttl, ct);
            return Result.Success();
        }
        catch (Exception ex)
        {
            return Result.Failed(ex);
        }
    }

    public async ValueTask<Result<TimeSpan?>> KeyTimeToLiveAsync(string key, CancellationToken ct = default)
    {
        try
        {
            var client = await holder.GetClientAsync(ct);
            if (client is null)
                return Failed<TimeSpan?>();
            return Result<TimeSpan?>.Success(await client.KeyTimeToLiveAsync(Full(key), ct));
        }
        catch (Exception ex)
        {
            return Result<TimeSpan?>.Failed(ex);
        }
    }

    private string Full(string key) => $"{keyPrefix}:{key}";

    private static Exception Unavailable()
        => new InvalidOperationException(
            "Valkey-параметры подключения недоступны — операция кеша отклонена (fail-open, спека t08 §3 п.4)");

    private static Result Failed() => Result.Failed(Unavailable());

    private static Result<T> Failed<T>() => Result<T>.Failed(Unavailable());
}
```

ПРИМЕЧАНИЕ: сигнатуры `Result<T>.Success/Failed` сверить по `src/PuzzleServer.Infrastructure.App/Result.cs` (используются как в t04/t10-коде); при необходимости заменить на implicit-конверсию `Result<T>.Failed(exc)` == `exc`.

- [ ] **7.3. Юниты** — `src/PuzzleServer.UnitTests/Valkey/ValkeyCacheTests.cs`:

```csharp
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using PuzzleServer.Infrastructure.App.Valkey;
using PuzzleServer.UnitTests.Valkey.Fakes;
using Xunit;

namespace PuzzleServer.UnitTests.Valkey;

// IValkeyCache через fake-клиента (спека §6.1): операции, префикс, сериализация,
// TTL-параметры, fail-open без исключений
public class ValkeyCacheTests : IAsyncLifetime
{
    private const string Prefix = "sess";
    private readonly FakeValkeyConnectionProvider _provider = new();
    private readonly FakeValkeyClient _client = new();
    private ValkeyConnectionHolder _holder = null!;
    private ValkeyCache _cache = null!;

    public ValueTask InitializeAsync()
    {
        _holder = new ValkeyConnectionHolder(
            _provider,
            _ => ValueTask.FromResult<IValkeyClient>(_client),
            NullLogger<ValkeyConnectionHolder>.Instance);
        _provider.Set(new ValkeyConnectionParams("localhost:6379", "app", "secret", false));
        _cache = new ValkeyCache(Prefix, _holder);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync() => await _holder.DisposeAsync();

    [Fact]
    public async Task SetGet_Roundtrip_WithPrefix()
    {
        // Arrange / Act
        var set = await _cache.SetAsync("user:1", "alice");
        var get = await _cache.GetAsync<string>("user:1");

        // Assert — домен видит свой ключ, стор — префиксованный
        set.IsSuccess.Should().BeTrue();
        get.IsSuccess.Should().BeTrue();
        get.Value.Should().Be("alice");
        _client.Store.Keys.Should().Contain($"{Prefix}:user:1");
        _client.Calls.Should().OnlyContain(k => k.StartsWith($"{Prefix}:"));
    }

    [Fact]
    public async Task Get_MissingKey_SuccessWithNull()
    {
        // Arrange / Act
        var get = await _cache.GetAsync<string>("missing");

        // Assert — отсутствие ключа = успех с default (не ошибка)
        get.IsSuccess.Should().BeTrue();
        get.Value.Should().BeNull();
    }

    [Fact]
    public async Task Set_WithTtl_PassesTtlToClient()
    {
        // Arrange
        var ttl = TimeSpan.FromMinutes(5);

        // Act
        await _cache.SetAsync("k", "v", ttl);

        // Assert
        _client.Ttls[$"{Prefix}:k"].Should().Be(ttl);
    }

    [Fact]
    public async Task Set_WithoutTtl_PassesNull()
    {
        // Arrange / Act
        await _cache.SetAsync("k", "v");

        // Assert
        _client.Ttls[$"{Prefix}:k"].Should().BeNull();
    }

    [Fact]
    public async Task Remove_ExistingKey_Succeeds()
    {
        // Arrange
        await _cache.SetAsync("k", "v");

        // Act
        var remove = await _cache.RemoveAsync("k");
        var exists = await _cache.ExistsAsync("k");

        // Assert
        remove.IsSuccess.Should().BeTrue();
        exists.IsSuccess.Should().BeTrue();
        exists.Value.Should().BeFalse();
    }

    [Fact]
    public async Task Exists_ReportsCorrectly()
    {
        // Arrange
        await _cache.SetAsync("k", "v");

        // Act / Assert
        (await _cache.ExistsAsync("k")).Value.Should().BeTrue();
        (await _cache.ExistsAsync("other")).Value.Should().BeFalse();
    }

    [Fact]
    public async Task GetSet_ReturnsOldValue_AndAppliesTtl()
    {
        // Arrange
        await _cache.SetAsync("k", "old");
        var ttl = TimeSpan.FromMinutes(1);

        // Act
        var result = await _cache.GetSetAsync("k", "new", ttl);

        // Assert — старое значение + TTL ключа восстановлен после GETSET (спека §4.5)
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be("old");
        _client.Store[$"{Prefix}:k"].ToString().Should().Be("new");
        _client.Ttls[$"{Prefix}:k"].Should().Be(ttl);
    }

    [Fact]
    public async Task AddIfAbsent_SecondCallFails()
    {
        // Arrange
        var first = await _cache.AddIfAbsentAsync("lock", "owner-1", TimeSpan.FromSeconds(30));

        // Act
        var second = await _cache.AddIfAbsentAsync("lock", "owner-2", TimeSpan.FromSeconds(30));

        // Assert — SET NX семантика
        first.Value.Should().BeTrue();
        second.IsSuccess.Should().BeTrue();
        second.Value.Should().BeFalse();
    }

    [Fact]
    public async Task Increment_Delta()
    {
        // Arrange / Act
        var one = await _cache.IncrementAsync("counter");
        var five = await _cache.IncrementAsync("counter", 4);

        // Assert — INCRBY: 1, затем 5
        one.Value.Should().Be(1);
        five.Value.Should().Be(5);
    }

    [Fact]
    public async Task GetMany_AllRequestedKeys_MissingDefaulted()
    {
        // Arrange
        await _cache.SetAsync("a", "1");

        // Act
        var result = await _cache.GetManyAsync<string>(["a", "b"]);

        // Assert — словарь по всем ключам, отсутствующие — default
        result.IsSuccess.Should().BeTrue();
        result.Value["a"].Should().Be("1");
        result.Value["b"].Should().BeNull();
        _client.Calls.Should().Contain($"{Prefix}:a").And.Contain($"{Prefix}:b");
    }

    [Fact]
    public async Task SetMany_PrefixedBatch_WithTtl()
    {
        // Arrange
        var ttl = TimeSpan.FromMinutes(2);

        // Act
        var result = await _cache.SetManyAsync(
            new[] { new KeyValuePair<string, string>("a", "1"), new KeyValuePair<string, string>("b", "2") }, ttl);

        // Assert — каждый элемент префиксован, TTL у каждого
        result.IsSuccess.Should().BeTrue();
        _client.Store[$"{Prefix}:a"].ToString().Should().Be("1");
        _client.Store[$"{Prefix}:b"].ToString().Should().Be("2");
        _client.Ttls[$"{Prefix}:a"].Should().Be(ttl);
    }

    [Fact]
    public async Task KeyExpire_KeyTimeToLive_Roundtrip()
    {
        // Arrange
        await _cache.SetAsync("k", "v");

        // Act
        var expire = await _cache.KeyExpireAsync("k", TimeSpan.FromMinutes(3));
        var ttl = await _cache.KeyTimeToLiveAsync("k");

        // Assert
        expire.IsSuccess.Should().BeTrue();
        ttl.Value.Should().Be(TimeSpan.FromMinutes(3));
    }

    [Fact]
    public async Task KeyTimeToLive_MissingKey_ReturnsNull()
    {
        // Arrange / Act
        var ttl = await _cache.KeyTimeToLiveAsync("nope");

        // Assert
        ttl.IsSuccess.Should().BeTrue();
        ttl.Value.Should().BeNull();
    }

    [Fact]
    public async Task Dto_Serialization_Roundtrip()
    {
        // Arrange
        var dto = new ValkeySerializerTests.TestDto { Name = "n", Count = 3 };

        // Act
        await _cache.SetAsync("dto", dto);
        var restored = await _cache.GetAsync<ValkeySerializerTests.TestDto>("dto");

        // Assert — JSON-путь для сложных типов
        restored.IsSuccess.Should().BeTrue();
        restored.Value.Should().BeEquivalentTo(dto);
    }

    [Fact]
    public async Task FailOpen_WithoutParams_AllOperationsFailed_NoThrow()
    {
        // Arrange — отдельный holder: провайдер без параметров
        var holder = new ValkeyConnectionHolder(
            new FakeValkeyConnectionProvider(),
            _ => ValueTask.FromResult<IValkeyClient>(_client),
            NullLogger<ValkeyConnectionHolder>.Instance);
        var cache = new ValkeyCache(Prefix, holder);

        // Act / Assert — весь набор операций: ни одна не бросает, все Result.Failed
        (await cache.GetAsync<string>("k")).IsSuccess.Should().BeFalse();
        (await cache.SetAsync("k", "v")).IsSuccess.Should().BeFalse();
        (await cache.RemoveAsync("k")).IsSuccess.Should().BeFalse();
        (await cache.ExistsAsync("k")).IsSuccess.Should().BeFalse();
        (await cache.GetSetAsync<string>("k", "v")).IsSuccess.Should().BeFalse();
        (await cache.AddIfAbsentAsync("k", "v")).IsSuccess.Should().BeFalse();
        (await cache.IncrementAsync("k")).IsSuccess.Should().BeFalse();
        (await cache.GetManyAsync<string>(["k"])).IsSuccess.Should().BeFalse();
        (await cache.SetManyAsync(new[] { new KeyValuePair<string, string>("k", "v") })).IsSuccess.Should().BeFalse();
        (await cache.KeyExpireAsync("k", TimeSpan.FromMinutes(1))).IsSuccess.Should().BeFalse();
        (await cache.KeyTimeToLiveAsync("k")).IsSuccess.Should().BeFalse();

        // хранилище не тронуто
        _client.Store.Should().BeEmpty();
        await holder.DisposeAsync();
    }

    [Fact]
    public async Task LibraryException_EncapsulatedInFailed()
    {
        // Arrange — клиент бросает (сбой сети и т.п.)
        _client.ThrowNext = new InvalidOperationException("сбой библиотеки");

        // Act
        var result = await _cache.GetAsync<string>("k");

        // Assert — наружу не бросается (спека §5)
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<InvalidOperationException>();
    }
}
```

- [ ] **7.4. Проверка и коммит**

```bash
cd /Users/demakaev/ZCodeProject/Puzzle
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PuzzleServer.Api.slnx        # 0 warnings
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PuzzleServer.UnitTests --filter "FullyQualifiedName~UnitTests.Valkey"   # PASS
git add src/PuzzleServer.Infrastructure.App.Valkey/IValkeyCache.cs \
        src/PuzzleServer.Infrastructure.App.Valkey/ValkeyCache.cs \
        src/PuzzleServer.UnitTests/Valkey/ValkeyCacheTests.cs
git commit -m "feat(valkey): IValkeyCache — расширенный набор v1 (базовые/атомарные/каунтеры/массовые/TTL), префиксы доменов, Result-инкапсуляция, юниты"
```

---

## Шаг 8: Builder + AddValkey + Program.cs + AppHost (Ф4 §8.4)

**Вход:** Шаги 1–7 закоммичены.

**Действие:** `IValkeyCacheBuilder<TConfig>` + `[InjectAsSingleton] ValkeyCacheBuilder<TConfig>` (fail-fast пустого KeyPrefix), `ModuleExtensions.AddValkey` (ветвление `Database:Source`, fail-fast `Valkey:Cluster`, holder-registration), строка в `Program.cs`, Aspire-ресурс AppHost (+`Aspire.Hosting.Valkey`); юниты.

**Выход:** модуль подключён к приложению; критерии приёмки №1 и №7 (структурная часть) закрыты.

**Проверка:** `dotnet test src/PuzzleServer.UnitTests --filter "FullyQualifiedName~UnitTests.Valkey"` — PASS; сборка решения 0 warnings.

**Связь со spec:** §4.1 (`Valkey:Cluster`, Aspire-ресурс), §4.6, §4.7, §8 Ф4.

**Interfaces (Consumes:** Шаги 1–7. **Produces:** `AddValkey(IServiceCollection, IConfiguration)` — точка входа приложения; `IValkeyCacheBuilder<TConfig>.Build() : IValkeyCache` — точка входа доменов.)

- [ ] **8.1. Builder** — `src/PuzzleServer.Infrastructure.App.Valkey/IValkeyCacheBuilder.cs`:

```csharp
namespace PuzzleServer.Infrastructure.App.Valkey;

// Фабрика кешей per-модуль (спека §4.6, паттерн Kafka §6): TConfig : ValkeyConfig с [Config]
// регистрируется через AutoRegistration; Build() создаёт IValkeyCache с KeyPrefix из
// config.CurrentValue.KeyPrefix. Пустой KeyPrefix → InvalidOperationException (fail-fast,
// зеркало пустого Topic у Kafka).
public interface IValkeyCacheBuilder<TConfig> where TConfig : ValkeyConfig
{
    IValkeyCache Build();
}
```

`src/PuzzleServer.Infrastructure.App.Valkey/ValkeyCacheBuilder.cs`:

```csharp
using Microsoft.Extensions.Options;
using PuzzleServer.Infrastructure.App.DI;

namespace PuzzleServer.Infrastructure.App.Valkey;

// Open-generic singleton (AutoRegistration, паттерн KafkaProducerBuilder): IOptionsMonitor<TConfig>
// обязан быть зарегистрирован через [Config] у наследника TConfig. Лёгкий: IValkeyCache
// создаётся на вызов, соединением владеет ValkeyConnectionHolder.
[InjectAsSingleton]
internal sealed class ValkeyCacheBuilder<TConfig>(
    IOptionsMonitor<TConfig> config,
    ValkeyConnectionHolder holder) : IValkeyCacheBuilder<TConfig> where TConfig : ValkeyConfig
{
    public IValkeyCache Build()
    {
        var prefix = config.CurrentValue.KeyPrefix;
        if (string.IsNullOrWhiteSpace(prefix))
            throw new InvalidOperationException(
                $"Valkey: KeyPrefix пуст у конфига {typeof(TConfig).Name} — префикс ключей домена обязателен (изоляция, спека t08 §3 п.6)");
        return new ValkeyCache(prefix, holder);
    }
}
```

- [ ] **8.2. AddValkey** — `src/PuzzleServer.Infrastructure.App.Valkey/ModuleExtensions.cs`:

```csharp
using System.Reflection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PuzzleServer.Infrastructure.App.DI;

namespace PuzzleServer.Infrastructure.App.Valkey;

public static class ModuleExtensions
{
    private static Assembly Assembly => typeof(ModuleExtensions).Assembly;
    private const string ConnectionKey = "Valkey";

    /// <summary>
    /// Регистрирует Valkey-модуль: опции (ValkeyOptions из ConnectionStrings:Valkey + секции
    /// Valkey), шов IValkeyConnectionProvider (ветка по Database:Source — общий переключатель
    /// с HA.Db/Kafka), держатель ValkeyConnectionHolder (единственный multiplexer на процесс)
    /// и AutoRegistration сборки (open-generic IValkeyCacheBuilder с [InjectAsSingleton]).
    /// Aspire — параметры из конфигурации; HaDb — HA.Valkey-дискавери из etcd
    /// (AddHaValkey(...).AddValkeyCluster(Valkey:Cluster)), пустой Valkey:Cluster — fail-fast.
    /// </summary>
    public static IServiceCollection AddValkey(this IServiceCollection services, IConfiguration configuration)
    {
        var connectionString = configuration.GetConnectionString(ConnectionKey) ?? string.Empty;
        var sectionEndpoints = configuration["Valkey:Endpoints"];

        services.Configure<ValkeyOptions>(opt =>
        {
            opt.Endpoints = !string.IsNullOrWhiteSpace(sectionEndpoints) ? sectionEndpoints! : connectionString;
            opt.Username = configuration["Valkey:Username"] ?? string.Empty;
            opt.Password = configuration["Valkey:Password"] ?? string.Empty;
        });

        // Ветвление источника соединительных параметров (зеркало AddKafka t10): один
        // переключатель Database:Source. Aspire — конфигурация; HaDb — HA.Valkey-дискавери.
        var source = PuzzleServer.Infrastructure.App.DB.DatabaseSourceReader.Read(configuration);
        if (source == PuzzleServer.Infrastructure.App.DB.DatabaseSource.Aspire)
        {
            services.AddSingleton<IValkeyConnectionProvider>(sp => new ConfigurationValkeyConnectionProvider(
                sp.GetRequiredService<Microsoft.Extensions.Options.IOptionsMonitor<ValkeyOptions>>()));
        }
        else
        {
            var cluster = configuration["Valkey:Cluster"];
            if (string.IsNullOrWhiteSpace(cluster))
                throw new InvalidOperationException(
                    "Valkey:Cluster не задан (Database:Source=HaDb) — имя valkey-кластера для дискавери обязательно (спека t08 §4.1)");
            services.AddHaValkey(configuration).AddValkeyCluster(cluster);
            services.AddSingleton<IValkeyConnectionProvider>(sp => new DiscoveryValkeyConnectionProvider(
                sp.GetRequiredService<PuzzleServer.Infrastructure.App.HA.Valkey.IValkeyDiscoveryStore>(),
                cluster,
                sp.GetRequiredService<ILogger<DiscoveryValkeyConnectionProvider>>()));
        }

        // Единственный держатель соединения на процесс (dispose при shutdown — IAsyncDisposable)
        services.AddSingleton(sp => new ValkeyConnectionHolder(
            sp.GetRequiredService<IValkeyConnectionProvider>(),
            ValkeyClientFactory.CreateAsync,
            sp.GetRequiredService<ILogger<ValkeyConnectionHolder>>()));

        return services.AutoRegistration(Assembly);
    }
}
```

- [ ] **8.3. Юниты регистрации** — `src/PuzzleServer.UnitTests/Valkey/AddValkeySourceBranchingTests.cs` (зеркало `Kafka/AddKafkaSourceBranchingTests.cs`):

```csharp
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PuzzleServer.Infrastructure.App.DI;
using PuzzleServer.Infrastructure.App.Valkey;
using Xunit;

namespace PuzzleServer.UnitTests.Valkey;

// Ветвление AddValkey по Database:Source (спека §4.7)
public class AddValkeySourceBranchingTests
{
    private static IConfiguration Config(params KeyValuePair<string, string?>[] extra)
    {
        var dict = new Dictionary<string, string?>
        {
            ["ConnectionStrings:Valkey"] = "localhost:6379",
        };
        foreach (var kv in extra)
            dict[kv.Key] = kv.Value;
        return new ConfigurationBuilder().AddInMemoryCollection(dict).Build();
    }

    [Fact]
    public void AddValkey_AspireMode_RegistersConfigurationProvider()
    {
        // Arrange — Aspire-ветка: etcd-стек не регистрируется вовсе
        var config = Config(new KeyValuePair<string, string?>("Database:Source", "Aspire"));
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));

        // Act
        services.AddValkey(config);
        using var sp = services.BuildServiceProvider();

        // Assert
        sp.GetService<PuzzleServer.Infrastructure.App.HA.Valkey.HaValkeyClusterRegistry>().Should().BeNull();
        sp.GetRequiredService<IValkeyConnectionProvider>().Should().BeOfType<ConfigurationValkeyConnectionProvider>();
        sp.GetRequiredService<ValkeyConnectionHolder>().Should().NotBeNull();
    }

    [Fact]
    public void AddValkey_HaDbMode_RegistersHaValkeyWithClusterClaim()
    {
        // Arrange — реальный стор HA.Valkey безопасен в unit-контейнере: ctor без сети
        // (паттерн t10, review Ф4-3: фейк стора был бы мёртвым грузом — AutoRegistration
        // добавляет реальный ПОСЛЕ него)
        var config = Config(
            new KeyValuePair<string, string?>("Valkey:Cluster", "cache"),
            new KeyValuePair<string, string?>("HaValkey:EtcdEndpoints:0", "http://localhost:2379"));
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));

        // Act
        services.AddValkey(config); // HaDb-ветка: AddHaValkey + AddValkeyCluster("cache")
        // Атрибутные регистрации HA.Valkey — Handle со своей коллекцией (паттерн t10:
        // глобальный UseBehaviour в тестах не настроен — параллельные тест-классы)
        new AutoRegistrationDiTypeBehaviour(services).Handle(
            typeof(PuzzleServer.Infrastructure.App.HA.Valkey.ModuleExtensions).Assembly.GetTypes());
        using var sp = services.BuildServiceProvider();

        // Assert — заявка кластера зарегистрирована, провайдер — Discovery
        var registry = sp.GetRequiredService<PuzzleServer.Infrastructure.App.HA.Valkey.HaValkeyClusterRegistry>();
        registry.Clusters.Should().Contain("cache");
        sp.GetRequiredService<IValkeyConnectionProvider>().Should().BeOfType<DiscoveryValkeyConnectionProvider>();
    }

    [Fact]
    public void AddValkey_HaDbModeWithoutCluster_FailFast()
    {
        // Arrange
        var config = Config(new KeyValuePair<string, string?>("HaValkey:EtcdEndpoints:0", "http://localhost:2379"));
        var services = new ServiceCollection();

        // Act / Assert — пустой Valkey:Cluster: ошибка при регистрации (критерий №1)
        var act = () => services.AddValkey(config);
        act.Should().Throw<InvalidOperationException>().WithMessage("*Valkey:Cluster*");
    }

    [Fact]
    public void AddValkey_UnknownSource_FailFast()
    {
        // Arrange
        var config = Config(new KeyValuePair<string, string?>("Database:Source", "Somewhere"));
        var services = new ServiceCollection();

        // Act / Assert — нераспознанный Source падает в DatabaseSourceReader
        var act = () => services.AddValkey(config);
        act.Should().Throw<InvalidOperationException>();
    }
}
```

- [ ] **8.4. Юниты builder'а** — `src/PuzzleServer.UnitTests/Valkey/ValkeyCacheBuilderTests.cs`:

```csharp
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using PuzzleServer.Infrastructure.App.Valkey;
using PuzzleServer.UnitTests.Valkey.Fakes;
using Xunit;

namespace PuzzleServer.UnitTests.Valkey;

// Фабрика кешей per-модуль (спека §4.6): префикс из TConfig, fail-fast пустого KeyPrefix
public class ValkeyCacheBuilderTests : IAsyncLifetime
{
    private readonly FakeValkeyConnectionProvider _provider = new();
    private ValkeyConnectionHolder _holder = null!;

    public ValueTask InitializeAsync()
    {
        _holder = new ValkeyConnectionHolder(
            _provider,
            _ => ValueTask.FromResult<IValkeyClient>(new FakeValkeyClient()),
            NullLogger<ValkeyConnectionHolder>.Instance);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync() => await _holder.DisposeAsync();

    private sealed class TestCacheConfig : ValkeyConfig;

    [Fact]
    public void Build_EmptyKeyPrefix_FailFast()
    {
        // Arrange — KeyPrefix пуст (дефолт)
        var monitor = new FakeOptionsMonitor<TestCacheConfig>();
        var builder = new ValkeyCacheBuilder<TestCacheConfig>(monitor, _holder);

        // Act / Assert — зеркало пустого Topic у Kafka (спека §4.6)
        var act = () => builder.Build();
        act.Should().Throw<InvalidOperationException>().WithMessage("*KeyPrefix*");
    }

    [Fact]
    public void Build_UsesKeyPrefixFromConfig_ReturnsCache()
    {
        // Arrange
        var monitor = new FakeOptionsMonitor<TestCacheConfig>();
        monitor.Set(new TestCacheConfig { KeyPrefix = "game" });
        var builder = new ValkeyCacheBuilder<TestCacheConfig>(monitor, _holder);

        // Act
        var cache = builder.Build();

        // Assert — домен получает готовый IValkeyCache (префикс-изоляция покрыта ValkeyCacheTests)
        cache.Should().NotBeNull();
    }
}
```

- [ ] **8.5. Program.cs** — в `src/PuzzleServer.Api/Program.cs`: using `PuzzleServer.Infrastructure.App.Valkey;` (в блок using по алфавиту) и строка СРАЗУ ПОСЛЕ `.AddKafka(builder.Configuration)` (порядок spec §4.7):

```csharp
   .AddKafka(builder.Configuration)
   .AddValkey(builder.Configuration)     // + новая строка
   .AddBus(builder.Configuration)
```

- [ ] **8.6. AppHost** — пин и ресурс:

`src/Directory.Packages.props` (после `Aspire.Hosting.Testing`):

```xml
    <PackageVersion Include="Aspire.Hosting.Valkey" Version="13.4.3" />
```

`src/PuzzleServer.Api.AppHost/PuzzleServer.Api.AppHost.csproj` (ItemGroup PackageReference, рядом с `Aspire.Hosting.Kafka`):

```xml
        <PackageReference Include="Aspire.Hosting.Valkey"/>
```

`src/PuzzleServer.Api.AppHost/AppHost.cs` — после `var kafka = builder.AddKafka("kafka");`:

```csharp
var valkey = builder.AddValkey("valkey");
```

и в цепочке `puzzleserver-api` (рядом с kafka-ссылкой):

```csharp
   .WithReference(valkey, "Valkey")
   .WaitForStart(valkey)
```

(Второй аргумент `"Valkey"` — alias: Aspire кладёт строку в `ConnectionStrings:Valkey`, откуда её читает `ValkeyOptions`.)

Если `Aspire.Hosting.Valkey` не восстановится из nuget.org — фоллэк spec §4.1: ресурс в AppHost НЕ добавлять, `ConnectionStrings:Valkey` задать строкой в `src/PuzzleServer.Api.AppHost/appsettings.Development.json` (`"ConnectionStrings": { "Valkey": "localhost:6379" }`) и зафиксировать фоллэк в доке 01.22 (Шаг 10). Основной путь — пакет (13.4.3 существует на nuget.org, проверено 2026-09-19).

- [ ] **8.7. Проверка и коммит**

```bash
cd /Users/demakaev/ZCodeProject/Puzzle
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PuzzleServer.Api.slnx        # 0 warnings (AppHost + Api собираются)
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PuzzleServer.UnitTests --filter "FullyQualifiedName~UnitTests.Valkey"   # PASS
git add src/PuzzleServer.Infrastructure.App.Valkey/IValkeyCacheBuilder.cs \
        src/PuzzleServer.Infrastructure.App.Valkey/ValkeyCacheBuilder.cs \
        src/PuzzleServer.Infrastructure.App.Valkey/ModuleExtensions.cs \
        src/PuzzleServer.UnitTests/Valkey/AddValkeySourceBranchingTests.cs \
        src/PuzzleServer.UnitTests/Valkey/ValkeyCacheBuilderTests.cs \
        src/PuzzleServer.Api/Program.cs \
        src/PuzzleServer.Api.AppHost/AppHost.cs \
        src/PuzzleServer.Api.AppHost/PuzzleServer.Api.AppHost.csproj \
        src/Directory.Packages.props
git commit -m "feat(valkey): IValkeyCacheBuilder<TConfig> open-generic + AddValkey (ветвление Database:Source, fail-fast Valkey:Cluster) + подключение в Api и AppHost (Aspire.Hosting.Valkey 13.4.3), юниты"
```

---

## Шаг 9: Интеграционные тесты — ValkeyClientFixture + 5 сценариев (Ф5 §8.5)

**Вход:** Шаг 8 закоммичен; модуль полностью работает в HaDb-ветке; docker доступен.

**Действие:** `ValkeyClientFixture` (переиспользуемая `ValkeyEtcdFixture` на порту 32497 + контейнер `valkey/valkey:9.1.2` с ACL-окном двух паролей канона arch/21 §2, динамический host-порт, admin-соединение для снятия OLD при ротации, полный teardown + ассерт чистоты); тестовые TConfig; 5 сценариев §6.2; ProjectReference.

**Выход:** критерии приёмки №3–№6, №8 закрыты живым контуром.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PuzzleServer.IntegrationTests --filter "FullyQualifiedName~IntegrationTests.Valkey"` — PASS 5/5; после серии гейт зачистки (docker ps — пусто).

**Связь со spec:** §6.2 (контур + 5 сценариев), §5, §8 Ф5, AGENTS.base п.11.

**Interfaces (Consumes:** `AddValkey`, `IValkeyCacheBuilder<TConfig>` + тестовые `TestCacheConfig`/`AnotherCacheConfig`, t04 `ValkeyEtcdFixture` (порт-параметр конструктора).)

- [ ] **9.1. ProjectReference** — в `src/PuzzleServer.IntegrationTests/PuzzleServer.IntegrationTests.csproj` (ItemGroup ProjectReference):

```xml
        <ProjectReference Include="..\PuzzleServer.Infrastructure.App.Valkey\PuzzleServer.Infrastructure.App.Valkey.csproj"/>
```

- [ ] **9.2. Тестовые конфиги** — `src/PuzzleServer.IntegrationTests/Valkey/TestCacheConfigs.cs` (верхний уровень — AutoRegistration сканирует assembly):

```csharp
using PuzzleServer.Infrastructure.App.DI;
using PuzzleServer.Infrastructure.App.Valkey;

namespace PuzzleServer.IntegrationTests.Valkey;

// Тестовые домены-потребители: ДВА конфига с разными префиксами — сценарий изоляции
[Config("TestValkeyCache")]
public class TestCacheConfig : ValkeyConfig;

[Config("AnotherValkeyCache")]
public class AnotherCacheConfig : ValkeyConfig;
```

- [ ] **9.3. Фикстура** — `src/PuzzleServer.IntegrationTests/Valkey/ValkeyClientFixture.cs`:

```csharp
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using FluentAssertions;
using PuzzleServer.IntegrationTests.HA.Valkey;
using StackExchange.Redis;
using Xunit;

namespace PuzzleServer.IntegrationTests.Valkey;

// Полный контур t08 (спека §6.2): переиспользуемая ValkeyEtcdFixture (etcd) + контейнер
// valkey/valkey:9.1.2 с ACL-аргументами канона arch/21 §2. ОКНО ДВУХ ПАРОЛЕЙ у ОДНОГО
// пользователя app (>OLD >NEW — аналог JAAS-окна KafkaSaslFixture t10): сценарий ротации
// после публикации NEW в etcd СНЯЛ OLD с ноды (ACL SETUSER app <OLD через admin) и требует,
// чтобы клиент продолжил работать — доказательство пересоздания multiplexer'а на NEW.
// Etcd — СВОЙ фиксированный порт 32497 (не дефолт 32496: t04-класс ValkeyDiscoveryIntegrationTests
// живёт в default-коллекции и ПАРАЛЛЕЛИТСЯ с этой коллекцией — общий порт дал бы
// «port is already allocated» при полном прогоне; фиксированный порт гарантирует рестарты
// etcd сценария 5 на том же endpoint). Host-порт valkey — динамический, литералов нет.
public sealed class ValkeyClientFixture : IAsyncLifetime
{
    public const string OldPassword = "abcdefghijklmnopqrstuvwxyz012345";
    public const string NewPassword = "0123456789abcdefghijklmnopqrstuv";
    private const string AdminPassword = "ZYXWVUTSRQPONMLKJIHGFEDCBA987654";
    private const int EtcdHostPort = 32497;

    private readonly ValkeyEtcdFixture _etcd = new(EtcdHostPort);

    private readonly IContainer _valkey = new ContainerBuilder("valkey/valkey:9.1.2")
        .WithCommand(
            "--user", "default", "off",
            "--user", "admin", "on", $">{AdminPassword}", "~*", "+@all",
            "--user", "app", "on", $">{OldPassword}", $">{NewPassword}", "~*", "+@read", "+@write",
            "--save", "",
            "--appendonly", "no")
        .WithPortBinding(6379, assignRandomHostPort: true)
        .Build();

    public string EtcdEndpoint => _etcd.Endpoint;
    public string Endpoint => $"localhost:{_valkey.GetMappedPublicPort(6379)}";

    public Task PutAsync(string key, string value) => _etcd.PutAsync(key, value);

    // Чистка дискавери-ключей: каждый сценарий начинается с ПУСТОГО префикса кластера
    // независимо от порядка исполнения (fail-open требует «ключей нет», ротация
    // оставляет app_password=NEW)
    public Task CleanClusterKeysAsync() => _etcd.DeletePrefixAsync("/valkey/clusters/");

    // Дискавери-ключи кластера (канон arch/20 §2.1/§4): config Active без state,
    // endpoints по ФАКТИЧЕСКОМУ динамическому порту, креды app
    public Task SeedDiscoveryKeysAsync(string password)
        => Task.WhenAll(
            _etcd.PutAsync("/valkey/clusters/cache/config",
                """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000}"""),
            _etcd.PutAsync("/valkey/clusters/cache/endpoints", Endpoint),
            _etcd.PutAsync("/valkey/clusters/cache/app_user", "app"),
            _etcd.PutAsync("/valkey/clusters/cache/app_password", password));

    // Ротация (arch/21 §5 E): публикация NEW в etcd; OLD на ноде снимается отдельно
    public Task RotateToNewPasswordAsync()
        => _etcd.PutAsync("/valkey/clusters/cache/app_password", NewPassword);

    // Закрытие окна (процесс E): OLD снят с ноды — клиент со старым multiplexer'ом
    // начинает получать NOAUTH, вынуждая перестроение на NEW
    public async Task RevokeOldPasswordAsync()
    {
        using var multiplexer = await ConnectionMultiplexer.ConnectAsync(AdminOptions());
        await multiplexer.GetDatabase().ExecuteAsync("ACL", "SETUSER", "app", "<" + OldPassword);
    }

    public Task StopEtcdAsync() => _etcd.StopEtcdAsync();
    public Task StartEtcdAsync() => _etcd.StartEtcdAsync();

    public async ValueTask InitializeAsync()
    {
        await _etcd.InitializeAsync();
        await _valkey.StartAsync(TestContext.Current.CancellationToken);
        await WaitValkeyReadyAsync(TestContext.Current.CancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        // Полный teardown ПРИ ЛЮБОМ ИСХОДЕ (IAsyncLifetime): контейнер + etcd-фикстура
        await _valkey.DisposeAsync();
        await _etcd.DisposeAsync();
        // Ассерт чистоты (спека §6.2): контейнер остановлен/удалён
        _valkey.State.Should().NotBe(TestcontainersStates.Running, "teardown обязан остановить valkey-контейнер");
    }

    // Готовность: команда по кредам app/OLD (ACL-пользователи активны при старте);
    // AbortOnConnectFail=false — ConnectAsync не бросает, проверяем IsConnected
    private async Task WaitValkeyReadyAsync(CancellationToken ct)
    {
        for (var i = 0; i < 30; i++)
        {
            try
            {
                using var multiplexer = await ConnectionMultiplexer.ConnectAsync(AppOptions(OldPassword));
                if (multiplexer.IsConnected)
                    return;
            }
            catch (RedisConnectionException)
            {
                // контейнер ещё поднимается — следующая попытка
            }
            await Task.Delay(1000, ct);
        }
        throw new InvalidOperationException($"valkey в {Endpoint} не поднялся за 30 с");
    }

    private ConfigurationOptions AppOptions(string password) => new()
    {
        EndPoints = { Endpoint },
        User = "app",
        Password = password,
        AbortOnConnectFail = false,
    };

    private ConfigurationOptions AdminOptions() => new()
    {
        EndPoints = { Endpoint },
        User = "admin",
        Password = AdminPassword,
        AbortOnConnectFail = false,
    };
}

// Одна фикстура на все сценарии (etcd+valkey стартуют один раз)
[CollectionDefinition(Name)]
public sealed class T08ValkeyClientCollection : ICollectionFixture<ValkeyClientFixture>
{
    public const string Name = "t08-valkey-client";
}
```

ПРИМЕЧАНИЕ: `WithPortBinding(6379, assignRandomHostPort: true)` — сигнатура Testcontainers 4.x; при иной арности использовать `WithPortBinding(6379, true)`. Сверить сборкой.

- [ ] **9.4. Сценарии** — `src/PuzzleServer.IntegrationTests/Valkey/ValkeyClientIntegrationTests.cs`:

```csharp
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PuzzleServer.Infrastructure.App.DI;
using PuzzleServer.Infrastructure.App.Valkey;
using Xunit;

namespace PuzzleServer.IntegrationTests.Valkey;

// Полный контур t08 (спека §6.2): реальные etcd + valkey-ACL; хост собирается как в t10
// (ServiceCollection + ручной старт hosted-сервисов). Пороги: WatchWindowMs=300 →
// бюджеты ожидания 5-15 с с запасом.
[Collection(T08ValkeyClientCollection.Name)]
public class ValkeyClientIntegrationTests(ValkeyClientFixture fixture) : IAsyncLifetime
{
    // Перед КАЖДЫМ тестом — чистый префикс /valkey/clusters/ (предпосылки конфликтуют)
    public async ValueTask InitializeAsync() => await fixture.CleanClusterKeysAsync();

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private IConfiguration Config()
    {
        var data = new Dictionary<string, string?>
        {
            // Database:Source пуст → HaDb-ветка (дефолт ридера)
            ["Valkey:Cluster"] = "cache",
            ["HaValkey:EtcdEndpoints:0"] = fixture.EtcdEndpoint,
            ["HaValkey:Mode"] = "WatchLongPoll",
            ["HaValkey:WatchWindowMs"] = "300",
            ["HaValkey:MembersMode"] = "Off",
            ["HaValkey:BootstrapTimeoutSec"] = "5",
            ["TestValkeyCache:KeyPrefix"] = "t08a",
            ["AnotherValkeyCache:KeyPrefix"] = "t08b",
            // ConnectionStrings:Valkey НЕ задаём: параметры обязаны прийти только из etcd
        };
        return new ConfigurationBuilder().AddInMemoryCollection(data).Build();
    }

    private async Task<ServiceProvider> StartHostAsync(IConfiguration config)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddValkey(config); // HaDb-ветка: AddHaValkey + AddValkeyCluster("cache")
        // Атрибутные регистрации — Handle со своей коллекцией (паттерн t10): глобальный
        // UseBehaviour трогал бы параллельные тест-коллекции
        new AutoRegistrationDiTypeBehaviour(services).Handle(typeof(ModuleExtensions).Assembly.GetTypes());
        new AutoRegistrationDiTypeBehaviour(services).Handle(
            typeof(PuzzleServer.Infrastructure.App.HA.Valkey.ModuleExtensions).Assembly.GetTypes());
        new AutoRegistrationConfigDiTypeBehaviour(services, config).Handle(typeof(TestCacheConfig).Assembly.GetTypes());
        var provider = services.BuildServiceProvider();
        // Стартуем ВСЕ hosted-сервисы (ValkeyDiscoveryRefresher — bootstrap)
        foreach (var hosted in provider.GetServices<IHostedService>())
            await hosted.StartAsync(CancellationToken.None);
        return provider;
    }

    private static async Task<bool> WaitUntilAsync(Func<Task<bool>> probe, TimeSpan budget)
    {
        var deadline = DateTimeOffset.UtcNow + budget;
        while (DateTimeOffset.UtcNow < deadline)
        {
            if (await probe())
                return true;
            await Task.Delay(200);
        }
        return await probe();
    }

    // Зонд: true = операция прошла (построение соединения + реальная команда)
    private static async Task<bool> PingCacheAsync(IServiceProvider host)
    {
        var cache = host.GetRequiredService<IValkeyCacheBuilder<TestCacheConfig>>().Build();
        var result = await cache.SetAsync($"ping-{Guid.NewGuid():N}", "v");
        return result.IsSuccess;
    }

    [Fact]
    public async Task FailOpen_WithoutEtcdKeys_ThenSeed_Connects()
    {
        // Arrange — ключей дискавери НЕТ: провайдер null, хост жив (критерий №3)
        await using var host = await StartHostAsync(Config());
        var cache = host.GetRequiredService<IValkeyCacheBuilder<TestCacheConfig>>().Build();

        // Act — операция отклонена (fail-open), не исключение
        var before = await cache.GetAsync<string>("k");

        // Assert
        before.IsSuccess.Should().BeFalse("без ключей дискавери параметров нет — Result.Failed, старт не роняется");

        // Act — воркер дописал ключи: watch доставляет без рестарта хоста
        await fixture.SeedDiscoveryKeysAsync(ValkeyClientFixture.OldPassword);
        var connected = await WaitUntilAsync(() => PingCacheAsync(host), TimeSpan.FromSeconds(5));

        // Assert — появление параметров строит соединение первым вызовом
        connected.Should().BeTrue("параметры должны прийти по watch (спека §6.2 п.1)");
        (await cache.SetAsync("k", "v")).IsSuccess.Should().BeTrue();
    }

    [Fact]
    public async Task Roundtrip_AllOperations_WithEtcdCredentials_AndPrefixIsolation()
    {
        // Arrange (критерий №6)
        await fixture.SeedDiscoveryKeysAsync(ValkeyClientFixture.OldPassword);
        await using var host = await StartHostAsync(Config());
        (await WaitUntilAsync(() => PingCacheAsync(host), TimeSpan.FromSeconds(5))).Should().BeTrue();
        var cache = host.GetRequiredService<IValkeyCacheBuilder<TestCacheConfig>>().Build();
        var other = host.GetRequiredService<IValkeyCacheBuilder<AnotherCacheConfig>>().Build();

        // Act / Assert — базовые
        (await cache.SetAsync("user:1", "alice", TimeSpan.FromMinutes(5))).IsSuccess.Should().BeTrue();
        var got = await cache.GetAsync<string>("user:1");
        got.Value.Should().Be("alice");
        (await cache.ExistsAsync("user:1")).Value.Should().BeTrue();

        // атомарные: GETSET (старое значение + TTL) и SET NX
        var old = await cache.GetSetAsync<string>("user:1", "bob", TimeSpan.FromMinutes(2));
        old.Value.Should().Be("alice");
        (await cache.KeyTimeToLiveAsync("user:1")).Value.Should()
            .BeCloseTo(TimeSpan.FromMinutes(2), TimeSpan.FromSeconds(30));
        (await cache.AddIfAbsentAsync("user:1", "carol")).Value.Should().BeFalse("ключ занят — SET NX false");
        (await cache.AddIfAbsentAsync("lock:1", "me", TimeSpan.FromSeconds(30))).Value.Should().BeTrue();

        // каунтеры
        (await cache.IncrementAsync("counter", 2)).Value.Should().Be(2);
        (await cache.IncrementAsync("counter", 3)).Value.Should().Be(5);

        // массовые
        (await cache.SetManyAsync(
            new[] { new KeyValuePair<string, string>("m:a", "1"), new KeyValuePair<string, string>("m:b", "2") },
            TimeSpan.FromMinutes(1))).IsSuccess.Should().BeTrue();
        var many = await cache.GetManyAsync<string>(["m:a", "m:b", "m:c"]);
        many.Value["m:a"].Should().Be("1");
        many.Value["m:b"].Should().Be("2");
        many.Value["m:c"].Should().BeNull("отсутствующий ключ — default в словаре по всем ключам");

        // TTL-управление
        (await cache.KeyExpireAsync("m:a", TimeSpan.FromMinutes(3))).IsSuccess.Should().BeTrue();
        (await cache.KeyTimeToLiveAsync("m:a")).Value.Should()
            .BeCloseTo(TimeSpan.FromMinutes(3), TimeSpan.FromSeconds(30));

        // изоляция доменов: ключ t08a невидим через t08b (критерий №6)
        (await other.GetAsync<string>("user:1")).Value.Should().BeNull("ключи изолированы префиксами доменов");
        (await other.ExistsAsync("user:1")).Value.Should().BeFalse();

        // удаление
        (await cache.RemoveAsync("user:1")).IsSuccess.Should().BeTrue();
        (await cache.ExistsAsync("user:1")).Value.Should().BeFalse();
    }

    [Fact]
    public async Task PasswordRotation_ClientRebuilds_NoLoss()
    {
        // Arrange (критерий №4): работаем на OLD
        await fixture.SeedDiscoveryKeysAsync(ValkeyClientFixture.OldPassword);
        await using var host = await StartHostAsync(Config());
        var cache = host.GetRequiredService<IValkeyCacheBuilder<TestCacheConfig>>().Build();
        (await WaitUntilAsync(() => PingCacheAsync(host), TimeSpan.FromSeconds(5))).Should().BeTrue();

        // Act — ротация: put NEW в etcd, окно закрыто (OLD снят с ноды)
        await fixture.RotateToNewPasswordAsync();
        await fixture.RevokeOldPasswordAsync();

        // Assert — клиент обязан перестроиться на NEW по OnChange (без рестарта хоста);
        // старый multiplexer с OLD получает NOAUTH — операции Failed до перестроения
        var recovered = await WaitUntilAsync(() => PingCacheAsync(host), TimeSpan.FromSeconds(15));
        recovered.Should().BeTrue("ротация app_password доставляется событием Updated — hot-reload без рестарта (спека §6.2 п.3)");
        (await cache.SetAsync("after-rotation", "v")).IsSuccess.Should().BeTrue();
    }

    [Fact]
    public async Task SnapshotNoise_DoesNotRebuildConnection()
    {
        // Arrange (критерий №5): параметры есть, хост работает
        await fixture.SeedDiscoveryKeysAsync(ValkeyClientFixture.OldPassword);
        await using var host = await StartHostAsync(Config());
        (await WaitUntilAsync(() => PingCacheAsync(host), TimeSpan.FromSeconds(5))).Should().BeTrue();
        var provider = host.GetRequiredService<IValkeyConnectionProvider>();
        var fired = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        provider.OnChange(() => fired.TrySetResult());

        // Act — шум снапшота: не-соединительные изменения (State в config + unknown-ключ)
        await fixture.PutAsync("/valkey/clusters/cache/config",
            """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000,"state":"DRAINING"}""");
        await fixture.PutAsync("/valkey/clusters/cache/whatever", "noise");

        // Assert — пара watch-окон (WatchWindowMs=300): OnChange НЕ стреляет
        var winner = await Task.WhenAny(fired.Task, Task.Delay(TimeSpan.FromSeconds(2)));
        winner.Should().NotBe(fired.Task, "шум снапшота не перестраивает соединение (спека §5)");
        (await PingCacheAsync(host)).Should().BeTrue("кеш продолжает работать");
    }

    [Fact]
    public async Task EtcdDown_CacheSurvives_RecoveryOnReturn()
    {
        // Arrange (критерий №5): снапшот собран, кеш работает
        await fixture.SeedDiscoveryKeysAsync(ValkeyClientFixture.OldPassword);
        await using var host = await StartHostAsync(Config());
        (await WaitUntilAsync(() => PingCacheAsync(host), TimeSpan.FromSeconds(5))).Should().BeTrue();

        // Act — смерть etcd: HA.Valkey fail-open отдаёт последний снапшот, valkey жив
        await fixture.StopEtcdAsync();
        var survived = await PingCacheAsync(host);

        // Assert — кеш продолжает работать по последнему снапшоту (спека §5)
        survived.Should().BeTrue("смерть etcd не роняет кеш");

        // Act — etcd вернулся + ротация: актуализация восстановлена
        await fixture.StartEtcdAsync();
        await fixture.CleanClusterKeysAsync();
        await fixture.SeedDiscoveryKeysAsync(ValkeyClientFixture.NewPassword);
        await fixture.RevokeOldPasswordAsync();
        var recovered = await WaitUntilAsync(() => PingCacheAsync(host), TimeSpan.FromSeconds(15));

        // Assert — после рестарта etcd изменения доставляются (спека §6.2 п.5)
        recovered.Should().BeTrue("после рестарта etcd смена кредов доставляется");
    }
}
```

ПРИМЕЧАНИЯ (обязательные для исполнителя):
1. В сценарии 5 после `StartEtcdAsync` ранее засеянные ключи могли пережить рестарт (том etcd жив) — поэтому явная `CleanClusterKeysAsync()` + повторный засев с NEW: изменение app_password гарантированно стреляет `Updated` после восстановления watch.
2. `RevokeOldPasswordAsync` в сценарии 5 требует, чтобы OLD был ещё на ноде — он и есть (окно фикстуры держит оба пароля до снятия).
3. Если ACL-снятие OLD (`<OLD`) у образа 9.1.2 потребует иного синтаксиса (`ACL SETUSER app resetpass >NEW`) — сверить по `docker exec ... valkey-cli acl list` в ручной проверке; план фиксирует семантику «после вызова OLD невалиден, NEW валиден».

- [ ] **9.5. Прогон серии и зачистка**

```bash
cd /Users/demakaev/ZCodeProject/Puzzle
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PuzzleServer.Api.slnx        # 0 warnings
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PuzzleServer.IntegrationTests --filter "FullyQualifiedName~IntegrationTests.Valkey" --logger "console;verbosity=normal"
# → PASS 5/5 (дождаться финальной строки прогона)

# Гейт зачистки после серии (фикстура чистит сама — это страховка; 0 остатков):
docker ps -a --filter ancestor=valkey/valkey:9.1.2 --format '{{.Names}}'            # пусто
docker ps -a --filter ancestor=quay.io/coreos/etcd:v3.5.21 --format '{{.Names}}'    # пусто
```

При падении — разбор по логам прогона и docker-логам контейнеров (`docker logs <id>`); перезапуск упавших тестов для выяснения «что было» — только после полного анализа логов.

- [ ] **9.6. Коммит**

```bash
git add src/PuzzleServer.IntegrationTests/PuzzleServer.IntegrationTests.csproj \
        src/PuzzleServer.IntegrationTests/Valkey/ValkeyClientFixture.cs \
        src/PuzzleServer.IntegrationTests/Valkey/TestCacheConfigs.cs \
        src/PuzzleServer.IntegrationTests/Valkey/ValkeyClientIntegrationTests.cs
git commit -m "test(valkey): интеграционный контур t08 — ValkeyClientFixture (etcd 32497 + valkey/valkey:9.1.2, ACL-окно двух паролей, динамический порт) и 5 сценариев (fail-open/roundtrip+изоляция/ротация/шум/смерть etcd), полный teardown"
```

---

## Шаг 10: Доки Puzzle + финальные гейты (Ф6 §8.6)

**Вход:** Шаги 1–9 закоммичены; все тесты зелёные.

**Действие:** канон `docs/01.22-valkey.md` (структура/стиль — docs/01.16), строка в `docs/01-infrastructure.md`, актуализация `docs/01.21-ha-valkey.md`; финальный полный прогон и структурные инварианты.

**Выход:** критерии приёмки №8–№9 закрыты (roadmap — на мерже, гейт 10.6).

**Проверка:** полный прогон юнитов → интеграции (серии по очереди, зачистка) + сборка 0 warnings + 0 остатков docker + инварианты ссылок.

**Связь со spec:** §7, §8 Ф6, §10 п.8–9.

- [ ] **10.1. docs/01.22-valkey.md** — новый канон модуля в Puzzle. Каркас (зеркало структуры 01.16; наполнение — по фактическому коду Шагов 1–8, все имена/поведение — фактические):

```markdown
# 01.22 — Valkey (кеш-абстракции)

> Проект **`PuzzleServer.Infrastructure.App.Valkey`**
> Namespace: `PuzzleServer.Infrastructure.App.Valkey`
> Назад: [01 — Инфраструктура](01-infrastructure.md) · Смежно: [01.21 HA.Valkey](01.21-ha-valkey.md) (дискавери), [01.16 Kafka](01.16-kafka.md) (образец архитектуры)

Доменные кеш-абстракции над StackExchange.Redis (единственная ссылка на пакет в решении):
`IValkeyCache` (Result-семантика, префикс-изоляция доменов), шов `IValkeyConnectionProvider`
(Aspire/HaDb — зеркало Kafka §1a), hot-reload ротации кредов, fail-open.

## 1. Конфиг: `ValkeyConfig` + `ValkeyOptions`
   (per-модульные наследники с `[Config]` и KeyPrefix; ValkeyOptions из
   ConnectionStrings:Valkey + секции Valkey; HaDb-ветка опции игнорирует)

## 1a. Источник соединительных параметров (Aspire / HaDb)
   (таблица-зеркало 01.16 §1a: провайдеры, Database:Source, Valkey:Cluster fail-fast,
   fail-open, шум/ротация; РЕКОМЕНДАЦИИ: HaValkey:MembersMode=Off и один etcd на стенд
   при сосуществовании с HA.Db — спека §9)

## 2. Держатель соединения: `ValkeyConnectionHolder`
   (единственный ConnectionMultiplexer на процесс; лениво; hot-reload пересозданием,
   orphaned-диспос; маппинг ConfigurationOptions: сплит endpoints, креды парой, Ssl,
   AbortOnConnectFail=false)

## 3. Кеш: `IValkeyCache` + `IValkeyCacheBuilder<TConfig>`
   (таблица операций расширенного набора с семантикой: GetSet двухкомандный + TTL,
   SetMany-батч с TTL, GetMany по всем ключам с default; сериализация — нативные
   примитивы string/byte[]/bool/int/long/double, остальное System.Text.Json;
   префикс "<KeyPrefix>:"; Result-семантика)

## 4. Регистрация: `AddValkey`
   (Program.cs; AppHost-ресурс builder.AddValkey("valkey") → ConnectionStrings:Valkey
   через WithReference(valkey, "Valkey"); фоллэк appsettings.Development.json, если
   Aspire.Hosting.Valkey недоступен)

## 5. Обработка сбоев (таблица §5 спеки)
   (fail-open; ротация app_password через окно двух паролей; шум; смерть etcd;
   недоступность ноды — AbortOnConnectFail=false; исключения в Result.Failed)

## 6. Границы v1 (§9 спеки)
   (TLS — t06; pub/sub/SCAN/EVAL/транзакции; admin-поверхность; health-check;
   OTel-спаны — вне v1)

## 7. Пример домена-потребителя
   ([Config("SessionCache")] class SessionCacheConfig : ValkeyConfig {}
   + инъекция IValkeyCacheBuilder<SessionCacheConfig>)
```

- [ ] **10.2. docs/01-infrastructure.md** — строка индекса после 01.21 (формат соседних строк):

```markdown
| [01.22 — Valkey](01.22-valkey.md) | `Infrastructure.App.Valkey` | Кеш-абстракции над StackExchange.Redis (единственная ссылка на пакет): `IValkeyCache` (Result-семантика, префикс-изоляция доменов), шов `IValkeyConnectionProvider` (Aspire/HaDb — зеркало Kafka §1a), hot-reload ротации кредов, fail-open. |
```

- [ ] **10.3. docs/01.21-ha-valkey.md** — актуализация:

- В шапке (фраза строк ~14–16 «Интеграция в клиентский модуль Valkey (StackExchange.Redis) — отдельная задача `t08-valkey-client-integration` (roadmap pg); сейчас клиентского модуля Valkey в Puzzle нет») — заменить на: «Клиентский модуль Valkey — [01.22](01.22-valkey.md)».
- Раздел `## Интеграция в клиентский модуль (t08 — плановая)` — переписать: заголовок `## Интеграция в клиентский модуль`, тело: «Модуль [01.22 Valkey](01.22-valkey.md) в HaDb-режиме регистрирует `AddHaValkey(...).AddValkeyCluster(<Valkey:Cluster>)` и строит `ConfigurationOptions` из `GetClientConfig()` (реализовано в t08)».

- [ ] **10.4. Финальные гейты (полный прогон с зачисткой серий)**

```bash
cd /Users/demakaev/ZCodeProject/Puzzle

# 1. Полная сборка решения — 0 errors, 0 warnings (вывод без строк "warning")
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PuzzleServer.Api.slnx

# 2. ВСЯ серия юнитов → дождаться финальной строки
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PuzzleServer.UnitTests --logger "console;verbosity=normal"

# 3. ВСЯ серия интеграционных (только после финальной строки юнитов — серии не накладываются)
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PuzzleServer.IntegrationTests --logger "console;verbosity=normal"

# 4. Зачистка: 0 остатков контейнеров фикстур (страховочный гейт после серии)
docker ps -a --filter ancestor=valkey/valkey:9.1.2 --format '{{.Names}}'
docker ps -a --filter ancestor=quay.io/coreos/etcd:v3.5.21 --format '{{.Names}}'
docker ps -a --filter ancestor=apache/kafka:4.0.0 --format '{{.Names}}'
# все три — пусто

# 5. Структурные инварианты:
#    - единственная PackageReference StackExchange.Redis:
grep -rn "StackExchange.Redis" src --include="*.csproj" | grep -v obj
#    → только PuzzleServer.Infrastructure.App.Valkey.csproj
#    - HA.Valkey не менялась:
git diff main...HEAD --stat -- src/PuzzleServer.Infrastructure.App.HA.Valkey/   # пусто
#    - staged src/global.json не попал ни в один коммит ветки:
git diff main...HEAD --stat -- src/global.json   # пусто
```

- [ ] **10.5. Коммит доков**

```bash
git add docs/01.22-valkey.md docs/01-infrastructure.md docs/01.21-ha-valkey.md
git commit -m "docs(valkey): канон 01.22-valkey (IValkeyCache, шов, holder, AddValkey) + индекс + актуализация 01.21 (интеграция t08 реализована)"
```

- [ ] **10.6. Гейты ПОСЛЕ одобрения пользователя (НЕ выполнять самостоятельно)**

- **Мерж-гейт (только по отдельному явному запросу пользователя):** мерж ветки `feat-t08-valkey-client-integration` в `main` Puzzle; тем же коммитом мержа — удалить пункт `t08-valkey-client-integration` из `arch/roadmap/valkey.md` pg-worktree (+ зачистить `←`-упоминания t08, если появятся), правило roadmap.
- Спека/план остаются в pg-worktree `docs/superpowers/2026-09-19-t08-valkey-client-integration/`.

---

## Самопроверка плана (выполнена автором)

1. **Покрытие spec по разделам:** §4.1 → Шаги 1, 8 (ValkeyOptions/ValkeyConfig/Valkey:Cluster/AppHost+фоллэк); §4.2 → Шаги 2–3 (шов, оба провайдера, baseline-до-подписки, value-equality); §4.3 → Шаги 4–5 (маппинг, holder: лениво/hot-reload/params→null/orphaned-диспос); §4.4 → Шаг 5 (seam + адаптер + fake); §4.5 → Шаги 6–7 (сериализация, все операции, префикс, Result); §4.6 → Шаг 8 (builder, fail-fast KeyPrefix); §4.7 → Шаг 8 (AddValkey, Program.cs, пины); §5 → юниты Шагов 2/3/5/7 + сценарии Шага 9 (все строки таблицы); §6.1 → юниты Шагов 1–8; §6.2 → Шаг 9 (фикстура: etcd 32497 + valkey ACL-окно, динамический порт, чистка префикса, teardown, ассерт чистоты; 5 сценариев); §7 → Шаг 10 (01.22, 01-infrastructure, 01.21; roadmap — гейт 10.6 тем же коммитом мержа); §8 Ф1–Ф6 ↔ Шаги 1–10; §9 вне-скоуп — не реализуется; §10 критерии: №1→8.3, №2→4.2, №3→7.3+9.4(1), №4→9.4(3), №5→9.4(4,5), №6→9.4(2), №7→10.4, №8→10.4, №9→10.1–10.3+10.6.
2. **Placeholder-scan:** каждый шаг содержит конкретный код и команды; «ПРИМЕЧАНИЕ»-блоки фиксируют точечные корректировки сигнатур по фактической сборке при уже зафиксированной семантике (не TODO).
3. **Консистентность типов:** `ValkeyConnectionParams(Endpoints, Username, Password, Ssl)` одинаков во всех шагах; методы `IValkeyClient` (Шаг 5) ↔ операции `ValkeyCache` (Шаг 7) — по одному на операцию; `ValkeyCacheBuilder<TConfig>(IOptionsMonitor<TConfig>, ValkeyConnectionHolder)` ↔ регистрация в `AddValkey` (Шаг 8); фейки Шагов 2/3/5 используются тестами Шагов 5/7/8; тестовые TConfig (Шаг 9) совпадают с секциями конфига сценария.
4. **Правила проекта:** динамический host-порт valkey; фиксированный etcd 32497 обоснован параллелью коллекций (зеркало решения KafkaSaslFixture 32495/32496); бюджеты ожиданий 2–15 с; teardown при любом исходе + ассерт чистоты + гейт зачистки серии; точечные `git add` (staged `src/global.json` не тронут); комментарии русские; тесты AAA.
