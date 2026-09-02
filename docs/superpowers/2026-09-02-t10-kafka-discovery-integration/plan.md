# t10 — Интеграция App.Kafka с HA.Kafka (дискавери из etcd): план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** В репозитории Puzzle — Confluent-клиент `PuzzleServer.Infrastructure.App.Kafka` получает параметры подключения (bootstrap + SASL/PLAIN-креды) из etcd-снапшота `HA.Kafka` в HaDb-режиме (`Database:Source=HaDb`, кластер из `Kafka:Cluster`), реагирует на изменения (ротация `app_password`, смена endpoints) переподключением без потери сообщений; Aspire-режим (`Database:Source=Aspire`) — прежний источник `ConnectionStrings:Kafka` без изменения поведения.

**Architecture:** Шов `IKafkaConnectionProvider` (Current + OnChange) в App.Kafka с двумя реализациями: Configuration-провайдер (адаптер `IOptionsMonitor<KafkaOptions>`, Aspire) и Discovery-провайдер (`IKafkaDiscoveryStore.Updated`, HaDb; App.Kafka → ProjectReference HA.Kafka). Существующие механики hot-reload переиспользуются: producer — инвалидация кэша, consumer — self-restart; построение Confluent-клиентов откладывается до появления валидных параметров (fail-open).

**Tech Stack:** .NET 10, C# (`LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`), xunit v3 + FluentAssertions, Testcontainers (`quay.io/coreos/etcd:v3.5.21`, `apache/kafka:4.0.0` с SASL_PLAINTEXT). Централизованные версии пакетов, новых внешних пакетов нет.

**Spec:** `docs/superpowers/2026-09-02-t10-kafka-discovery-integration/spec.md` (в worktree pg `/Users/demakaev/ZCodeProject/worktrees/feat-t10-kafka-discovery-integration`; далее «спека §N»).

## Global Constraints

- **Два репозитория**: код — в `/Users/demakaev/ZCodeProject/Puzzle` (git-репозиторий, ветка `feat-t10-kafka-discovery-integration`, коммиты по его AGENTS.md: feature-ветки — свободно); spec/plan/roadmap-артефакты — в worktree pg `/Users/demakaev/ZCodeProject/worktrees/feat-t10-kafka-discovery-integration`. Каждый таск помечен `[repo: …]`; пути файлов в задачах относительны корня соответствующего репо.
- Идентификаторы — английские; комментарии/документация — русские. Тесты — с комментариями `// Arrange` / `// Act` / `// Assert`.
- `TreatWarningsAsErrors`-режим: сборка без warnings (0 warnings как критерий).
- Никаких `throw` через границы модулей — `Result`/`Result<T>` из `PuzzleServer.Infrastructure.App` (исключения инкапсулируются в `Result.Failed`, implicit conversion из `Exception`).
- Никаких новых внешних пакетов; версии — только centrally в `src/Directory.Packages.props`.
- Публичный API `Infrastructure.App.Kafka` не меняется (спека §8 п.9); `HA.Kafka` не изменяется и не получает Confluent.Kafka (спека §8 п.9).
- `Database:Source` — единый переключатель БД и Kafka (спека §2 п.1); имя кластера — `Kafka:Cluster` (спека §2 п.6); HaKafka:MembersMode=Off в appsettings при сосуществовании с HA.Db (решение user-review Фазы 3, в spec не внесено — фиксируется как осознанное отклонение, см. конец плана).
- Команды сборки/тестов — из корня `/Users/demakaev/ZCodeProject/Puzzle`:
  - `dotnet build src/PuzzleServer.Api.slnx` (0 warnings);
  - unit без Docker: `dotnet test src/PuzzleServer.Api.slnx --filter "FullyQualifiedName~PuzzleServer.UnitTests"`;
  - integration (нужен Docker): `dotnet test src/PuzzleServer.Api.slnx --filter "FullyQualifiedName~PuzzleServer.IntegrationTests"`;
  - одиночный тест: `dotnet test src/PuzzleServer.Api.slnx --filter "FullyQualifiedName~<Full.Name>"`.

---

### Task 0: Подготовка веток и коммит артефактов фаз

**[repo: Puzzle + repo: pg-worktree]** Спека §6 «двухрепозиторная организация».

**Files:**
- pg-worktree: без новых (spec уже есть; этот plan.md).

- [ ] **Step 1: В Puzzle создать feature-ветку от main**

Вход: чистый main репозитория Puzzle.

```bash
cd /Users/demakaev/ZCodeProject/Puzzle
git switch main && git pull --ff-only || true
git switch -c feat-t10-kafka-discovery-integration
```

Выход: ветка создана, `git status --short` пуст. Проверка: `git branch --show-current` → `feat-t10-kafka-discovery-integration`.

- [ ] **Step 2: В pg-worktree закоммитить артефакты фаз (feature-ветка — коммитить свободно)**

Вход: файлы `docs/superpowers/2026-09-02-t10-kafka-discovery-integration/{spec.md,plan.md}` в worktree pg.

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t10-kafka-discovery-integration
git add docs/superpowers/2026-09-02-t10-kafka-discovery-integration/
git commit -m "t10: spec + plan (интеграция App.Kafka с HA.Kafka discovery)"
```

Выход: коммит создан. Проверка: `git log --oneline -1`.

---

### Task 1: Модель `KafkaConnectionParams` (+редакция пароля)

**[repo: Puzzle]** Спека §3.1 (шов, редакция — принцип 8).

**Files:**
- Create: `src/PuzzleServer.Infrastructure.App.Kafka/KafkaConnectionParams.cs`
- Test: `src/PuzzleServer.UnitTests/Kafka/KafkaConnectionParamsTests.cs`

**Interfaces (produces, используется всеми последующими задачами):**
```csharp
namespace PuzzleServer.Infrastructure.App.Kafka;

public sealed record KafkaConnectionParams(
    string BootstrapServers,
    string? SecurityProtocol,
    string? SaslMechanism,
    string? SaslUsername,
    string? SaslPassword);
```

- [ ] **Step 1: Написать падающий тест**

Вход: пустой проект UnitTests/Kafka (рядом с KafkaTopicAdminTests). Действие: создать тест-класс.

```csharp
using FluentAssertions;
using PuzzleServer.Infrastructure.App.Kafka;
using Xunit;

namespace PuzzleServer.UnitTests.Kafka;

// Редакция секрета в ToString (спека §2 п.8): пароль не светится в логах/дампах.
public class KafkaConnectionParamsTests
{
    [Fact]
    public void ToString_RedactsSaslPassword()
    {
        // Arrange
        var p = new KafkaConnectionParams("h:9092", "SASL_PLAINTEXT", "PLAIN", "app", "secret-password-32chars");

        // Act
        var text = p.ToString();

        // Assert
        text.Should().NotContain("secret-password-32chars");
        text.Should().Contain("***");
    }

    [Fact]
    public void ToString_KeepsNonSecretFields()
    {
        // Arrange
        var p = new KafkaConnectionParams("h:9092", "SASL_PLAINTEXT", "PLAIN", "app", "secret-password-32chars");

        // Act
        var text = p.ToString();

        // Assert
        text.Should().Contain("h:9092").And.Contain("SASL_PLAINTEXT").And.Contain("PLAIN").And.Contain("app");
    }
}
```

Выход: файл теста. Проверка: `dotnet test src/PuzzleServer.Api.slnx --filter "FullyQualifiedName~PuzzleServer.UnitTests.Kafka.KafkaConnectionParamsTests"` → FAIL (тип не существует).

- [ ] **Step 2: Реализовать модель**

Действие: создать `KafkaConnectionParams.cs`.

```csharp
namespace PuzzleServer.Infrastructure.App.Kafka;

/// <summary>
/// Соединительные параметры Kafka-клиента — всё, что нужно Confluent-конфигам.
/// SASL-поля опциональны: null → в Confluent-конфиг не задаются (дефолт PLAINTEXT,
/// Aspire-ветка); полный набор — HaDb-ветка из контракта pg/arch/15 §5.
/// </summary>
public sealed record KafkaConnectionParams(
    string BootstrapServers,
    string? SecurityProtocol,
    string? SaslMechanism,
    string? SaslUsername,
    string? SaslPassword)
{
    // Редакция секрета (спека §2 п.8): пароль не попадает в логи/дампы.
    public override string ToString()
        => $"KafkaConnectionParams {{ BootstrapServers = {BootstrapServers}, "
           + $"SecurityProtocol = {SecurityProtocol}, SaslMechanism = {SaslMechanism}, "
           + $"SaslUsername = {SaslUsername}, SaslPassword = *** }}";
}
```

Выход: тип `KafkaConnectionParams`. Проверка: тот же фильтр → PASS.

- [ ] **Step 3: Коммит**

```bash
cd /Users/demakaev/ZCodeProject/Puzzle
git add src/PuzzleServer.Infrastructure.App.Kafka/KafkaConnectionParams.cs src/PuzzleServer.UnitTests/Kafka/KafkaConnectionParamsTests.cs
git commit -m "feat: KafkaConnectionParams model with SaslPassword redaction (t10 Ф1)"
```

---

### Task 2: Маппер `KafkaConfluentConfigs` (params → Confluent-конфиги)

**[repo: Puzzle]** Спека §3.1 (маппинг; чистые функции).

**Files:**
- Create: `src/PuzzleServer.Infrastructure.App.Kafka/KafkaConfluentConfigs.cs`
- Test: `src/PuzzleServer.UnitTests/Kafka/KafkaConfluentConfigsTests.cs`

**Interfaces (produces):**
```csharp
namespace PuzzleServer.Infrastructure.App.Kafka;

internal static class KafkaConfluentConfigs
{
    public static ProducerConfig Producer(KafkaConnectionParams p);      // + EnableIdempotence/Acks (как сегодня)
    public static ConsumerConfig Consumer(KafkaConnectionParams p, string groupId); // + EnableAutoCommit=false, Earliest (как сегодня)
    public static AdminClientConfig Admin(KafkaConnectionParams p);
}
```
Consumes: `KafkaConnectionParams` (Task 1).

- [ ] **Step 1: Написать падающие тесты**

```csharp
using Confluent.Kafka;
using FluentAssertions;
using PuzzleServer.Infrastructure.App.Kafka;
using Xunit;

namespace PuzzleServer.UnitTests.Kafka;

// Маппинг KafkaConnectionParams → Confluent-конфиги (спека §3.1): полный SASL-набор →
// SaslPlaintext/Plain/креды; неполный/пустой → PLAINTEXT-дефолт Confluent (Aspire).
// Регистрация через [InternalsVisibleTo PuzzleServer.UnitTests].
public class KafkaConfluentConfigsTests
{
    private static readonly KafkaConnectionParams Sasl =
        new("h1:9092,h2:9092", "SASL_PLAINTEXT", "PLAIN", "app", "secret-password-32chars");

    private static readonly KafkaConnectionParams Plain = new("h1:9092", null, null, null, null);

    [Fact]
    public void Producer_FullSaslSet_AppliesSasl()
    {
        // Act
        var c = KafkaConfluentConfigs.Producer(Sasl);

        // Assert
        c.BootstrapServers.Should().Be("h1:9092,h2:9092");
        c.SecurityProtocol.Should().Be(SecurityProtocol.SaslPlaintext);
        c.SaslMechanism.Should().Be(SaslMechanism.Plain);
        c.SaslUsername.Should().Be("app");
        c.SaslPassword.Should().Be("secret-password-32chars");
        c.EnableIdempotence.Should().BeTrue();
        c.Acks.Should().Be(Acks.All);
    }

    [Fact]
    public void Consumer_FullSaslSet_AppliesSaslAndConsumerDefaults()
    {
        // Act
        var c = KafkaConfluentConfigs.Consumer(Sasl, "grp");

        // Assert
        c.GroupId.Should().Be("grp");
        c.SecurityProtocol.Should().Be(SecurityProtocol.SaslPlaintext);
        c.SaslUsername.Should().Be("app");
        c.EnableAutoCommit.Should().BeFalse();
        c.AutoOffsetReset.Should().Be(AutoOffsetReset.Earliest);
    }

    [Fact]
    public void Admin_FullSaslSet_AppliesSasl()
    {
        // Act
        var c = KafkaConfluentConfigs.Admin(Sasl);

        // Assert
        c.BootstrapServers.Should().Be("h1:9092,h2:9092");
        c.SecurityProtocol.Should().Be(SecurityProtocol.SaslPlaintext);
        c.SaslPassword.Should().Be("secret-password-32chars");
    }

    [Fact]
    public void All_NoSaslFields_LeaveConfluentDefaults()
    {
        // Act
        var producer = KafkaConfluentConfigs.Producer(Plain);
        var consumer = KafkaConfluentConfigs.Consumer(Plain, "grp");
        var admin = KafkaConfluentConfigs.Admin(Plain);

        // Assert — null → не задано: Confluent-дефолт PLAINTEXT (Aspire-ветка)
        producer.SecurityProtocol.Should().BeNull();
        producer.SaslUsername.Should().BeNull();
        consumer.SecurityProtocol.Should().BeNull();
        admin.SecurityProtocol.Should().BeNull();
        admin.BootstrapServers.Should().Be("h1:9092");
    }

    [Fact]
    public void All_PartialSaslSet_LeavesPlaintext()
    {
        // Arrange — неполный набор (username есть, password нет): SASL не применяется
        var partial = new KafkaConnectionParams("h1:9092", null, null, "app", null);

        // Act
        var c = KafkaConfluentConfigs.Producer(partial);

        // Assert
        c.SecurityProtocol.Should().BeNull();
        c.SaslUsername.Should().BeNull();
    }
}
```

Выход: тест-файл. Проверка: фильтр `~PuzzleServer.UnitTests.Kafka.KafkaConfluentConfigsTests` → FAIL (тип не существует).

- [ ] **Step 2: Реализовать маппер**

```csharp
using Confluent.Kafka;

namespace PuzzleServer.Infrastructure.App.Kafka;

/// <summary>
/// Чистые функции маппинга KafkaConnectionParams → Confluent-конфиги (спека §3.1).
/// SASL применяется только при ПОЛНОМ наборе полей (protocol+mechanism+username+password);
/// неполный → Confluent-дефолт PLAINTEXT. Строки протокола/механизма — константы
/// контракта pg/arch/15 §5; незнакомое значение — ошибка программирования (fail-fast).
/// </summary>
internal static class KafkaConfluentConfigs
{
    public static ProducerConfig Producer(KafkaConnectionParams p)
    {
        var config = new ProducerConfig
        {
            // Idempotent producer: Confluent сам выставляет acks=all/retries (как до t10).
            EnableIdempotence = true,
            Acks = Acks.All,
        };
        return Apply(config, p);
    }

    public static ConsumerConfig Consumer(KafkaConnectionParams p, string groupId)
    {
        var config = new ConsumerConfig
        {
            GroupId = groupId,
            EnableAutoCommit = false, // коммит вручную после обработки (как до t10)
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };
        return Apply(config, p);
    }

    public static AdminClientConfig Admin(KafkaConnectionParams p)
        => Apply(new AdminClientConfig(), p);

    private static T Apply<T>(T config, KafkaConnectionParams p) where T : ClientConfig
    {
        config.BootstrapServers = p.BootstrapServers;
        if (SaslSetComplete(p))
        {
            config.SecurityProtocol = ParseProtocol(p.SecurityProtocol!);
            config.SaslMechanism = ParseMechanism(p.SaslMechanism!);
            config.SaslUsername = p.SaslUsername;
            config.SaslPassword = p.SaslPassword;
        }
        return config;
    }

    private static bool SaslSetComplete(KafkaConnectionParams p)
        => p.SecurityProtocol is not null && p.SaslMechanism is not null
           && p.SaslUsername is not null && p.SaslPassword is not null;

    private static SecurityProtocol ParseProtocol(string s)
        => s switch
        {
            "SASL_PLAINTEXT" => SecurityProtocol.SaslPlaintext,
            _ => throw new InvalidOperationException($"Неизвестный security.protocol: '{s}' (контракт pg/arch/15 §5)"),
        };

    private static SaslMechanism ParseMechanism(string s)
        => s switch
        {
            "PLAIN" => SaslMechanism.Plain,
            _ => throw new InvalidOperationException($"Неизвестный sasl.mechanisms: '{s}' (контракт pg/arch/15 §5)"),
        };
}
```

Выход: маппер. Проверка: фильтр → PASS.

- [ ] **Step 3: Коммит**

```bash
git add src/PuzzleServer.Infrastructure.App.Kafka/KafkaConfluentConfigs.cs src/PuzzleServer.UnitTests/Kafka/KafkaConfluentConfigsTests.cs
git commit -m "feat: KafkaConfluentConfigs mapper — params to Confluent configs with SASL (t10 Ф1)"
```

---

### Task 3: Шов `IKafkaConnectionProvider` + `ConfigurationKafkaConnectionProvider`

**[repo: Puzzle]** Спека §3.1 (интерфейс), §3.2 (Aspire-реализация).

**Files:**
- Create: `src/PuzzleServer.Infrastructure.App.Kafka/IKafkaConnectionProvider.cs`
- Create: `src/PuzzleServer.UnitTests/Kafka/Fakes/FakeOptionsMonitor.cs`
- Create: `src/PuzzleServer.UnitTests/Kafka/Fakes/FakeConnectionProvider.cs`
- Test: `src/PuzzleServer.UnitTests/Kafka/ConfigurationKafkaConnectionProviderTests.cs`

**Interfaces (produces):**
```csharp
namespace PuzzleServer.Infrastructure.App.Kafka;

public interface IKafkaConnectionProvider
{
    KafkaConnectionParams? Current { get; }   // null = валидного конфига нет (fail-open)
    IDisposable OnChange(Action handler);      // только при фактическом изменении Current
}

// internal ConfigurationKafkaConnectionProvider(IOptionsMonitor<KafkaOptions>) : IKafkaConnectionProvider
```
Consumes: `KafkaConnectionParams` (Task 1).

- [ ] **Step 1: Тестовые фейки (FakeOptionsMonitor, FakeConnectionProvider)**

Вход: фейкам предстоит переиспользование в Task 4–7 — отдельные файлы в `Fakes/`.

`src/PuzzleServer.UnitTests/Kafka/Fakes/FakeOptionsMonitor.cs`:

```csharp
using Microsoft.Extensions.Options;

namespace PuzzleServer.UnitTests.Kafka.Fakes;

/// <summary>Ручной IOptionsMonitor: Set заменяет значение и триггерит слушателей.</summary>
public sealed class FakeOptionsMonitor<T> : IOptionsMonitor<T> where T : class, new()
{
    private readonly List<Action<T?, string?>> _listeners = [];
    private readonly object _gate = new();

    public T CurrentValue { get; private set; } = new();

    public T Get(string? name) => CurrentValue;

    public IDisposable OnChange(Action<T?, string?> listener)
    {
        lock (_gate)
            _listeners.Add(listener);
        return new Unsubscription(() => { lock (_gate) _listeners.Remove(listener); });
    }

    public void Set(T value)
    {
        CurrentValue = value;
        Action<T?, string?>[] snapshot;
        lock (_gate)
            snapshot = [.. _listeners];
        foreach (var listener in snapshot)
            listener(value, null);
    }

    private sealed class Unsubscription(Action unsubscribe) : IDisposable
    {
        public void Dispose() => unsubscribe();
    }
}
```

`src/PuzzleServer.UnitTests/Kafka/Fakes/FakeConnectionProvider.cs`:

```csharp
using PuzzleServer.Infrastructure.App.Kafka;

namespace PuzzleServer.UnitTests.Kafka.Fakes;

/// <summary>Ручной IKafkaConnectionProvider: Set заменяет Current и оповещает подписчиков.</summary>
public sealed class FakeConnectionProvider : IKafkaConnectionProvider
{
    private readonly List<Action> _handlers = [];
    private readonly object _gate = new();

    public KafkaConnectionParams? Current { get; private set; }

    public IDisposable OnChange(Action handler)
    {
        lock (_gate)
            _handlers.Add(handler);
        return new Unsubscription(() => { lock (_gate) _handlers.Remove(handler); });
    }

    public int HandlerCount { get { lock (_gate) return _handlers.Count; } }

    public void Set(KafkaConnectionParams? p)
    {
        Current = p;
        Action[] snapshot;
        lock (_gate)
            snapshot = [.. _handlers];
        foreach (var handler in snapshot)
            handler();
    }

    private sealed class Unsubscription(Action unsubscribe) : IDisposable
    {
        public void Dispose() => unsubscribe();
    }
}
```

Выход: фейки. Проверка: `dotnet build src/PuzzleServer.Api.slnx` — собирается (фейки не используются до теста; если фейк ссылается на несуществующий тип — сборка упадёт, это и есть красный шаг).

- [ ] **Step 2: Падающий тест Configuration-провайдера**

```csharp
using FluentAssertions;
using PuzzleServer.Infrastructure.App.Kafka;
using PuzzleServer.UnitTests.Kafka.Fakes;
using Xunit;

namespace PuzzleServer.UnitTests.Kafka;

// Aspire-реализация шва (спека §3.2): Current из KafkaOptions.BootstrapServers
// (пустой → null — fail-open), OnChange — по options-нотификации.
public class ConfigurationKafkaConnectionProviderTests
{
    [Fact]
    public void Current_EmptyBootstrap_ReturnsNull()
    {
        // Arrange
        var options = new FakeOptionsMonitor<KafkaOptions>();
        var provider = new ConfigurationKafkaConnectionProvider(options);

        // Act
        var current = provider.Current;

        // Assert
        current.Should().BeNull();
    }

    [Fact]
    public void Current_NonEmptyBootstrap_ReturnsParamsWithoutSasl()
    {
        // Arrange
        var options = new FakeOptionsMonitor<KafkaOptions>();
        options.Set(new KafkaOptions { BootstrapServers = "broker:9092" });
        var provider = new ConfigurationKafkaConnectionProvider(options);

        // Act
        var current = provider.Current;

        // Assert
        current.Should().NotBeNull();
        current!.BootstrapServers.Should().Be("broker:9092");
        current.SecurityProtocol.Should().BeNull();
        current.SaslUsername.Should().BeNull();
    }

    [Fact]
    public void OnChange_FiresOnOptionsChange()
    {
        // Arrange
        var options = new FakeOptionsMonitor<KafkaOptions>();
        var provider = new ConfigurationKafkaConnectionProvider(options);
        var fired = 0;
        provider.OnChange(() => fired++);

        // Act
        options.Set(new KafkaOptions { BootstrapServers = "broker:9094" });

        // Assert
        fired.Should().Be(1);
    }
}
```

Проверка: фильтр `~PuzzleServer.UnitTests.Kafka.ConfigurationKafkaConnectionProviderTests` → FAIL.

- [ ] **Step 3: Реализовать интерфейс и Configuration-провайдер**

`src/PuzzleServer.Infrastructure.App.Kafka/IKafkaConnectionProvider.cs`:

```csharp
namespace PuzzleServer.Infrastructure.App.Kafka;

/// <summary>
/// Шов «источник соединительных параметров Kafka-клиента» (спека §3.1). Реализации:
/// Configuration (Aspire — из IOptionsMonitor&lt;KafkaOptions&gt;) и Discovery (HaDb — из
/// IKafkaDiscoveryStore, HA.Kafka). Current == null — валидного конфига нет: fail-open
/// (producer отвечает Failed, consumer откладывает построение). OnChange — только при
/// фактическом изменении Current (value-equality), включая переходы null→params/params→null.
/// </summary>
public interface IKafkaConnectionProvider
{
    KafkaConnectionParams? Current { get; }

    IDisposable OnChange(Action handler);
}

/// <summary>
/// Aspire-ветка (спека §3.2): адаптер над IOptionsMonitor&lt;KafkaOptions&gt;.
/// BootstrapServers — из ConnectionStrings:Kafka/секции Kafka; SASL-полей нет (PLAINTEXT).
/// </summary>
internal sealed class ConfigurationKafkaConnectionProvider(
    Microsoft.Extensions.Options.IOptionsMonitor<KafkaOptions> options) : IKafkaConnectionProvider
{
    public KafkaConnectionParams? Current
        => string.IsNullOrWhiteSpace(options.CurrentValue.BootstrapServers)
            ? null
            : new KafkaConnectionParams(options.CurrentValue.BootstrapServers, null, null, null, null);

    public IDisposable OnChange(Action handler)
        => options.OnChange(_ => handler());
}
```

Выход: шов. Проверка: фильтр → PASS.

- [ ] **Step 4: Коммит**

```bash
git add src/PuzzleServer.Infrastructure.App.Kafka/IKafkaConnectionProvider.cs src/PuzzleServer.UnitTests/Kafka/Fakes/ src/PuzzleServer.UnitTests/Kafka/ConfigurationKafkaConnectionProviderTests.cs
git commit -m "feat: IKafkaConnectionProvider seam + Configuration provider (Aspire) (t10 Ф1)"
```

---

### Task 4: Producer-сторона на шве + ленивая admin-фабрика

**[repo: Puzzle]** Спека §3.4 (producer: Build от Current, пустой producer, подписки; admin-фабрика). Ветвление AddKafka — Task 7; здесь провайдер регистрируется безусловно (Configuration), Aspire-поведение сохранено.

**Files:**
- Modify: `src/PuzzleServer.Infrastructure.App.Kafka/IKafkaProducerBuilder.cs` (KafkaProducerBuilder: ctor и построение)
- Modify: `src/PuzzleServer.Infrastructure.App.Kafka/KafkaTopicClientAdapter.cs` (ленивый клиент)
- Modify: `src/PuzzleServer.Infrastructure.App.Kafka/ModuleExtensions.cs` (регистрация провайдера + фабрика)
- Test: `src/PuzzleServer.UnitTests/Kafka/KafkaProducerBuilderTests.cs` (new)

**Interfaces:**
- Consumes: `IKafkaConnectionProvider`/`ConfigurationKafkaConnectionProvider` (Task 3), `KafkaConfluentConfigs` (Task 2).
- Produces: ctor `KafkaProducerBuilder<TConfig, TKey, TValue>(IOptionsMonitor<TConfig>, IKafkaConnectionProvider, ILogger<KafkaProducer<TKey, TValue>>)`; internal `UnavailableKafkaProducer<TKey, TValue>(ILogger)`; ctor `KafkaTopicClientAdapter(Func<IAdminClient>)`.

- [ ] **Step 1: Падающие тесты producer-билдера**

```csharp
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using PuzzleServer.Infrastructure.App.Kafka;
using PuzzleServer.UnitTests.Kafka.Fakes;
using Xunit;

namespace PuzzleServer.UnitTests.Kafka;

// Producer на шве (спека §3.4): Build от provider.Current; null → SendAsync Failed
// без исключений; OnChange провайдера → кэш замещён (следующий Build — от новых параметров).
// KafkaProducer/KafkaProducerBuilder — internal: видны через InternalsVisibleTo.
public class KafkaProducerBuilderTests
{
    private sealed class TestConfig : KafkaConfig;

    private static (FakeOptionsMonitor<TestConfig> Config, FakeConnectionProvider Provider) Make()
        => (new(), new());

    [Fact]
    public async Task Build_NullParams_SendAsyncFailsWithoutException()
    {
        // Arrange
        var (config, provider) = Make();
        provider.Set(null); // валидного конфига нет
        var builder = new KafkaProducerBuilder<TestConfig, string, string>(
            config, provider, NullLogger<KafkaProducer<string, string>>.Instance);

        // Act
        var producer = builder.Build();
        var result = await producer.SendAsync("k", "v", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
    }

    [Fact]
    public async Task Build_ParamsToNullTransition_CacheInvalidated()
    {
        // Arrange
        var (config, provider) = Make();
        config.Set(new TestConfig { Topic = "t10-topic" });
        provider.Set(new KafkaConnectionParams("localhost:1", null, null, null, null));
        var builder = new KafkaProducerBuilder<TestConfig, string, string>(
            config, provider, NullLogger<KafkaProducer<string, string>>.Instance);
        _ = builder.Build(); // кэш заполнен валидным producer'ом

        // Act — параметры исчезли (params→null): кэш замещается, следующий Build — Unavailable
        provider.Set(null);

        // Assert
        var rebuilt = builder.Build();
        var result = await rebuilt.SendAsync("k", "v", CancellationToken.None);
        result.IsSuccess.Should().BeFalse("кэш замещён: producer без параметров отклоняет SendAsync");
    }

    [Fact]
    public void Build_WithParams_CreatesProducerWithoutNetwork()
    {
        // Arrange
        var (config, provider) = Make();
        config.Set(new TestConfig { Topic = "t10-topic" });
        provider.Set(new KafkaConnectionParams("localhost:1", "SASL_PLAINTEXT", "PLAIN", "app", "p".PadRight(32, 'x')));
        var builder = new KafkaProducerBuilder<TestConfig, string, string>(
            config, provider, NullLogger<KafkaProducer<string, string>>.Instance);

        // Act — Confluent producer строится лениво: подключений нет
        var producer = builder.Build();

        // Assert
        producer.Should().NotBeNull();
    }
}
```

Проверка: фильтр `~PuzzleServer.UnitTests.Kafka.KafkaProducerBuilderTests` → FAIL (нет ctor с провайдером).

- [ ] **Step 2: Перевести KafkaProducerBuilder на шов + UnavailableKafkaProducer**

В `IKafkaProducerBuilder.cs` заменить реализацию builder'а (файл содержит и `KafkaConfig`, и интерфейсы — их НЕ трогать):

```csharp
// Было:
// internal sealed class KafkaProducerBuilder<TConfig, TKey, TValue>(
//     IOptionsMonitor<TConfig> config, IOptionsMonitor<KafkaOptions> kafkaOptions) : ...

// Стало:
[InjectAsSingleton]
internal sealed class KafkaProducerBuilder<TConfig, TKey, TValue>(
    IOptionsMonitor<TConfig> config,
    IKafkaConnectionProvider connectionProvider,
    ILogger<KafkaProducer<TKey, TValue>> logger) : IKafkaProducerBuilder<TConfig, TKey, TValue>, IAsyncDisposable
    where TConfig : KafkaConfig
{
    private ConcurrentDictionary<string, IKafkaProducer<TKey, TValue>> _producers = new();
    private readonly ConcurrentQueue<ConcurrentDictionary<string, IKafkaProducer<TKey, TValue>>> _orphaned = new();
    private bool _changeSubscribed;

    public IKafkaProducer<TKey, TValue> Build()
    {
        SubscribeToChanges();
        return _producers.GetOrAdd(config.CurrentValue.Topic, _ => CreateProducer());
    }

    // Fail-open (спека §3.4): параметров нет → Unavailable-обёртка (SendAsync → Failed),
    // есть → Confluent producer от текущего среза параметров.
    private IKafkaProducer<TKey, TValue> CreateProducer()
    {
        var p = connectionProvider.Current;
        if (p is null)
            return new UnavailableKafkaProducer<TKey, TValue>(logger);
        return new KafkaProducer<TKey, TValue>(
            config.CurrentValue.Topic,
            new ProducerBuilder<TKey, TValue>(KafkaConfluentConfigs.Producer(p)).Build());
    }

    private void SubscribeToChanges()
    {
        if (_changeSubscribed)
            return;
        _changeSubscribed = true;
        config.OnChange(_ => InvalidateCache());
        connectionProvider.OnChange(() => InvalidateCache());
    }

    // Замещаем кэш пустым: следующий Build() создаст producer'ов с новыми параметрами
    // (спека §3.4 — прежняя механика orphaned-диспоза сохранена).
    private void InvalidateCache()
    {
        var old = Interlocked.Exchange(ref _producers, new ConcurrentDictionary<string, IKafkaProducer<TKey, TValue>>());
        if (!old.IsEmpty)
            _orphaned.Enqueue(old);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var cache in _orphaned)
            foreach (var producer in cache.Values)
                await producer.DisposeAsync();
        foreach (var producer in _producers.Values)
            await producer.DisposeAsync();
        _producers.Clear();
        _orphaned.Clear();
    }
}

// Producer «конфиг недоступен» (fail-open, спека §3.4): SendAsync → Result.Failed,
// лог с троттлингом (не спамить на каждый отказ).
internal sealed class UnavailableKafkaProducer<TKey, TValue>(ILogger logger) : IKafkaProducer<TKey, TValue>
{
    private readonly object _logGate = new();
    private DateTimeOffset _lastLogUtc = DateTimeOffset.MinValue;

    public ValueTask<Result> SendAsync(TKey key, TValue value, CancellationToken cancellationToken)
    {
        ThrottledLog();
        return ValueTask.FromResult(Result.Failed(new InvalidOperationException(
            "Kafka-конфиг недоступен (нет параметров подключения) — SendAsync отклонён (fail-open, спека §3.4)")));
    }

    private void ThrottledLog()
    {
        lock (_logGate)
        {
            if (DateTimeOffset.UtcNow - _lastLogUtc < TimeSpan.FromSeconds(30))
                return;
            _lastLogUtc = DateTimeOffset.UtcNow;
            logger.LogWarning("Kafka SendAsync отклонён: параметры подключения недоступны (ждём дискавери/конфигурацию)");
        }
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
```

Также обновить xml-doc класса (`KafkaOptions` → `IKafkaConnectionProvider`) и заголовочный комментарий файла. Удалить `using Microsoft.Extensions.Options;`-зависимость от `KafkaOptions` (IOptionsMonitor<TConfig> остаётся).

- [ ] **Step 3: Ленивая admin-фабрика**

`KafkaTopicClientAdapter.cs`: ctor принимает фабрику клиента, строит при первом обращении (спека §3.4 — при null-Current резолв AdminClient не должен ронять старт; операции возвращают Result.Failed через ретраи):

```csharp
// Было: internal sealed class KafkaTopicClientAdapter(IAdminClient client) : IKafkaTopicClient
// Стало:
internal sealed class KafkaTopicClientAdapter(Func<IAdminClient> clientFactory) : IKafkaTopicClient
{
    private readonly object _gate = new();
    private IAdminClient? _client;

    // Лениво: первый вызов метода строит клиент от актуального среза параметров
    // (спека §6 — AdminClient не перестраивается при смене параметров).
    private IAdminClient Client
    {
        get { lock (_gate) return _client ??= clientFactory(); }
    }

    public void Dispose()
    {
        lock (_gate)
            _client?.Dispose();
    }
    // ... все методы: client.X → Client.X (остальная логика без изменений)
}
```

`ModuleExtensions.AddKafka`: зарегистрировать провайдер и обновить фабрику (вставка до `services.AddSingleton<Func<IKafkaTopicClient>>`):

```csharp
// Шов соединительных параметров (спека §3.1–3.2). На этом шаге — Aspire-реализация
// безусловно; ветвление по Database:Source — следующим шагом (t10 Ф3).
services.AddSingleton<IKafkaConnectionProvider>(sp => new ConfigurationKafkaConnectionProvider(
    sp.GetRequiredService<IOptionsMonitor<KafkaOptions>>()));

services.AddSingleton<Func<IKafkaTopicClient>>(sp =>
{
    var provider = sp.GetRequiredService<IKafkaConnectionProvider>();
    return () => new KafkaTopicClientAdapter(() => BuildAdminClient(provider));
});
```

и private-метод в `ModuleExtensions`:

```csharp
// AdminClient от текущего среза параметров; null-Current → пустой bootstrap
// (спека §3.4: операции вернут Result.Failed через jitter-ретраи, не роняя резолв).
private static Confluent.Kafka.IAdminClient BuildAdminClient(IKafkaConnectionProvider provider)
    => new Confluent.Kafka.AdminClientBuilder(
        KafkaConfluentConfigs.Admin(
            provider.Current ?? new KafkaConnectionParams(string.Empty, null, null, null, null)))
        .Build();
```

Убрать прежнюю фабрику с `IOptionsMonitor<KafkaOptions>`. Прочее в `AddKafka` (KafkaOptions.Configure, AutoRegistration) — без изменений.

Выход: producer/admin на шве. Проверка: фильтр `~PuzzleServer.UnitTests.Kafka` → PASS; `dotnet build src/PuzzleServer.Api.slnx` → 0 warnings; весь существующий unit-набор зелёный (`--filter "FullyQualifiedName~PuzzleServer.UnitTests"`).

- [ ] **Step 4: Коммит**

```bash
git add src/PuzzleServer.Infrastructure.App.Kafka/ src/PuzzleServer.UnitTests/Kafka/
git commit -m "feat: producer builder + admin factory on IKafkaConnectionProvider seam, unavailable-producer fail-open (t10 Ф1)"
```

---

### Task 5: Consumer-сторона: ожидание параметров + change source от провайдера

**[repo: Puzzle]** Спека §3.4 (consumer: отложенное построение; change source = TConfig + провайдер). Механика едина для обеих веток провайдера — поэтому исполняется здесь, а не в Ф2 спеки (детализация плана).

**Files:**
- Create: `src/PuzzleServer.Infrastructure.App.Kafka/ConnectionParamsWaiter.cs`
- Modify: `src/PuzzleServer.Infrastructure.App.Kafka/IKafkaConfigChangeSource.cs` (KafkaConfigChangeSource)
- Modify: `src/PuzzleServer.Infrastructure.App.Kafka/IKafkaConsumerBuilder.cs` (builder)
- Modify: `src/PuzzleServer.Infrastructure.App.Kafka/IKafkaConsumer.cs` (KafkaConsumer: provider вместо строки)
- Test: `src/PuzzleServer.UnitTests/Kafka/ConnectionParamsWaiterTests.cs`, `src/PuzzleServer.UnitTests/Kafka/KafkaConfigChangeSourceTests.cs`, `src/PuzzleServer.UnitTests/Kafka/KafkaConsumerLazyStartTests.cs`

**Interfaces:**
- Consumes: `IKafkaConnectionProvider` (Task 3), `KafkaConfluentConfigs` (Task 2).
- Produces: `internal sealed class ConnectionParamsWaiter(IKafkaConnectionProvider provider)` c `Task<KafkaConnectionParams> WaitAsync(CancellationToken ct)`; ctor `KafkaConfigChangeSource<TConfig>(IOptionsMonitor<TConfig>, IKafkaConnectionProvider)`; ctor `KafkaConsumer<TKey, TValue>(IKafkaConnectionProvider, string groupId, string topic, IKafkaConfigChangeSource, ILogger<...>)` + `internal bool KafkaConsumer<,>.ClientBuilt` (тест-наблюдаемость построения Confluent-клиента); ctor `KafkaConsumerBuilder<TConfig, TKey, TValue>(IOptionsMonitor<TConfig>, IKafkaConnectionProvider, ILogger<KafkaConsumer<TKey, TValue>>)`.

- [ ] **Step 1: Падающие тесты ConnectionParamsWaiter**

```csharp
using FluentAssertions;
using PuzzleServer.Infrastructure.App.Kafka;
using PuzzleServer.UnitTests.Kafka.Fakes;
using Xunit;

namespace PuzzleServer.UnitTests.Kafka;

// Ожидание валидных параметров (спека §3.4): null-Current → ожидание (пробуждение по
// OnChange), появление → результат, отмена → OperationCanceledException.
public class ConnectionParamsWaiterTests
{
    private static readonly KafkaConnectionParams Params =
        new("localhost:1", "SASL_PLAINTEXT", "PLAIN", "app", "p".PadRight(32, 'x'));

    [Fact]
    public async Task WaitAsync_NoParams_WaitsUntilOnChange()
    {
        // Arrange
        var provider = new FakeConnectionProvider(); // Current = null
        var waiter = new ConnectionParamsWaiter(provider);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        // Act
        var waiting = waiter.WaitAsync(cts.Token);
        var premature = await Task.WhenAny(waiting, Task.Delay(TimeSpan.FromMilliseconds(300)));
        provider.Set(Params); // параметры появились
        var result = await waiting;

        // Assert
        premature.Should().NotBeSameAs(waiting, "ожидание не должно завершаться до появления параметров");
        result.Should().Be(Params);
    }

    [Fact]
    public async Task WaitAsync_ParamsAlreadyPresent_ReturnsImmediately()
    {
        // Arrange
        var provider = new FakeConnectionProvider();
        provider.Set(Params);
        var waiter = new ConnectionParamsWaiter(provider);

        // Act
        var result = await waiter.WaitAsync(CancellationToken.None);

        // Assert
        result.Should().Be(Params);
    }

    [Fact]
    public async Task WaitAsync_Cancellation_Throws()
    {
        // Arrange
        var provider = new FakeConnectionProvider(); // Current = null навсегда
        var waiter = new ConnectionParamsWaiter(provider);
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

        // Act / Assert
        await FluentActions.Awaiting(() => waiter.WaitAsync(cts.Token))
            .Should().ThrowAsync<OperationCanceledException>();
    }
}
```

Проверка: фильтр `~PuzzleServer.UnitTests.Kafka.ConnectionParamsWaiterTests` → FAIL.

- [ ] **Step 2: Реализовать ConnectionParamsWaiter**

```csharp
namespace PuzzleServer.Infrastructure.App.Kafka;

/// <summary>
/// Ожидание валидных соединительных параметров (спека §3.4, fail-open): периодическая
/// перепроверка Current (1 с) + немедленное пробуждение по OnChange. Перепроверка после
/// подписки исключает гонку «параметры появились между чтением и подпиской».
/// </summary>
internal sealed class ConnectionParamsWaiter(IKafkaConnectionProvider provider)
{
    private static readonly TimeSpan RepollInterval = TimeSpan.FromSeconds(1);

    public async Task<KafkaConnectionParams> WaitAsync(CancellationToken ct)
    {
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            if (provider.Current is { } immediate)
                return immediate;

            var signal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            using var subscription = provider.OnChange(() => signal.TrySetResult());
            if (provider.Current is { } afterSubscribe)
                return afterSubscribe;

            var delay = Task.Delay(RepollInterval, ct);
            await Task.WhenAny(signal.Task, delay);
            // Цикл перепроверит Current: сигнал или таймаут — оба ведут к новой итерации.
        }
    }
}
```

Проверка: фильтр → PASS.

- [ ] **Step 3: Падающие тесты change source**

`src/PuzzleServer.UnitTests/Kafka/KafkaConfigChangeSourceTests.cs`:

```csharp
using FluentAssertions;
using PuzzleServer.Infrastructure.App.Kafka;
using PuzzleServer.UnitTests.Kafka.Fakes;
using Xunit;

namespace PuzzleServer.UnitTests.Kafka;

// Change source consumer'а (спека §3.4): срабатывает от TConfig И от провайдера
// (провайдер — единственный путь соединительных параметров, подписка на
// IOptionsMonitor<KafkaOptions> устранена).
public class KafkaConfigChangeSourceTests
{
    private sealed class TestConfig : KafkaConfig;

    [Fact]
    public void OnChange_FiresOnTConfigChange()
    {
        // Arrange
        var config = new FakeOptionsMonitor<TestConfig>();
        var provider = new FakeConnectionProvider();
        var source = new KafkaConfigChangeSource<TestConfig>(config, provider);
        var fired = 0;
        source.OnChange(() => fired++);

        // Act
        config.Set(new TestConfig { Topic = "other" });

        // Assert
        fired.Should().Be(1);
    }

    [Fact]
    public void OnChange_FiresOnProviderChange()
    {
        // Arrange
        var config = new FakeOptionsMonitor<TestConfig>();
        var provider = new FakeConnectionProvider();
        var source = new KafkaConfigChangeSource<TestConfig>(config, provider);
        var fired = 0;
        source.OnChange(() => fired++);

        // Act
        provider.Set(new KafkaConnectionParams("h:9092", null, null, null, null));

        // Assert
        fired.Should().Be(1);
    }
}
```

Проверка: фильтр → FAIL.

- [ ] **Step 4: Обновить KafkaConfigChangeSource**

В `IKafkaConfigChangeSource.cs` заменить generic-реализацию (интерфейс и CompositeDisposable не менять; xml-doc обновить):

```csharp
// Было:
// internal sealed class KafkaConfigChangeSource<TConfig>(
//     IOptionsMonitor<TConfig> config, IOptionsMonitor<KafkaOptions> kafkaOptions) : ...

// Стало:
// Соединительные параметры (bootstrap/SASL) приходят через IKafkaConnectionProvider;
// подписка на IOptionsMonitor<KafkaOptions> устранена — провайдер единственный путь
// (спека §3.4): в Aspire-ветке Configuration-провайдер сам слушает KafkaOptions.
internal sealed class KafkaConfigChangeSource<TConfig>(
    IOptionsMonitor<TConfig> config,
    IKafkaConnectionProvider connectionProvider) : IKafkaConfigChangeSource
    where TConfig : KafkaConfig
{
    public IDisposable OnChange(Action handler)
    {
        var s1 = config.OnChange(_ => handler());
        var s2 = connectionProvider.OnChange(handler);
        return new CompositeDisposable(s1, s2);
    }
}
```

Проверка: фильтр → PASS.

- [ ] **Step 5: Падающие тесты ленивого старта consumer'а**

`src/PuzzleServer.UnitTests/Kafka/KafkaConsumerLazyStartTests.cs`:

```csharp
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using PuzzleServer.Infrastructure.App.Kafka;
using PuzzleServer.UnitTests.Kafka.Fakes;
using Xunit;

namespace PuzzleServer.UnitTests.Kafka;

// Fail-open consumer (спека §3.4): при null-Current построение Confluent-клиента
// отложено — StartAsync жив без faulted, отменяемо; появление параметров → построение.
public class KafkaConsumerLazyStartTests
{
    private sealed class TestConfig : KafkaConfig;

    private static KafkaConsumer<string, string> MakeConsumer(FakeConnectionProvider provider)
    {
        var config = new FakeOptionsMonitor<TestConfig>();
        config.Set(new TestConfig { Topic = "t10-topic", GroupId = "t10-group" });
        var source = new KafkaConfigChangeSource<TestConfig>(config, provider);
        return new KafkaConsumer<string, string>(
            provider, "t10-group", "t10-topic", source,
            NullLogger<KafkaConsumer<string, string>>.Instance);
    }

    [Fact]
    public async Task StartAsync_WithoutParams_StaysAliveAndCancellable()
    {
        // Arrange
        var provider = new FakeConnectionProvider(); // Current = null
        var consumer = MakeConsumer(provider);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var messages = 0;

        // Act
        var supervisor = consumer.StartAsync(_ => { messages++; return ValueTask.FromResult(Result.Success()); }, ct: cts.Token);
        await Task.Delay(TimeSpan.FromMilliseconds(300));
        var stillRunning = !supervisor.IsCompleted;

        // Assert
        stillRunning.Should().BeTrue("consumer ждёт параметры, а не падает (fail-open)");
        messages.Should().Be(0);
        cts.Cancel();
        await supervisor; // завершается по отмене без исключений
    }

    [Fact]
    public async Task StopAsync_WhileWaitingParameters_Completes()
    {
        // Arrange
        var provider = new FakeConnectionProvider();
        var consumer = MakeConsumer(provider);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        _ = consumer.StartAsync(_ => ValueTask.FromResult(Result.Success()), ct: cts.Token);
        await Task.Delay(TimeSpan.FromMilliseconds(200));

        // Act / Assert — явная остановка завершает supervisor
        await consumer.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task StartAsync_ParamsAppearWhileWaiting_BuildsClient()
    {
        // Arrange — старт без параметров: ожидание, клиент не построен
        var provider = new FakeConnectionProvider();
        var consumer = MakeConsumer(provider);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        _ = consumer.StartAsync(_ => ValueTask.FromResult(Result.Success()), ct: cts.Token);
        await Task.Delay(TimeSpan.FromMilliseconds(200));
        consumer.ClientBuilt.Should().BeFalse("до появления параметров Confluent-клиент не строится (спека §3.4)");

        // Act — параметры появились: OnChange будит ожидание → построение и подписка
        // (ConsumerBuilder.Build()/Subscribe сети не требуют — брокер не нужен)
        provider.Set(new KafkaConnectionParams("localhost:1", "SASL_PLAINTEXT", "PLAIN", "app", "p".PadRight(32, 'x')));

        // Assert — клиент построен; onMessage не зовётся (брокера нет) — достаточно факта построения
        var built = await WaitUntilAsync(() => consumer.ClientBuilt, TimeSpan.FromSeconds(5));
        built.Should().BeTrue("появление параметров должно построить Confluent-клиент (спека §7 unit)");
        await consumer.StopAsync(CancellationToken.None);
    }

    private static async Task<bool> WaitUntilAsync(Func<bool> probe, TimeSpan budget)
    {
        var deadline = DateTimeOffset.UtcNow + budget;
        while (DateTimeOffset.UtcNow < deadline)
        {
            if (probe())
                return true;
            await Task.Delay(50);
        }
        return probe();
    }
}
```

Примечание: `KafkaConsumer`/`KafkaConfigChangeSource` internal — видны через `InternalsVisibleTo PuzzleServer.UnitTests`.

Проверка: фильтр `~PuzzleServer.UnitTests.Kafka.KafkaConsumerLazyStartTests` → FAIL.

- [ ] **Step 6: Перевести KafkaConsumer и builder на провайдер**

`IKafkaConsumer.cs` — изменения в `KafkaConsumer<TKey, TValue>`:

```csharp
// ctor: string bootstrapServers → IKafkaConnectionProvider
internal sealed class KafkaConsumer<TKey, TValue>(
    IKafkaConnectionProvider connectionProvider,
    string groupId,
    string topic,
    IKafkaConfigChangeSource changeSource,
    ILogger<KafkaConsumer<TKey, TValue>> logger) : IKafkaConsumer<TKey, TValue>
{
    private readonly ConnectionParamsWaiter _waiter = new(connectionProvider);
    private IConsumer<TKey, TValue>? _consumer;

    // Тест-наблюдаемость (InternalsVisibleTo): построен ли Confluent-клиент
    // (юзнит-проверка ленивой ветки «параметры появились → построение», спека §7).
    internal bool ClientBuilt => _consumer is not null;
    // ... остальные поля прежние (_restartCts, _cts, _loop, _supervisor, _changeSub,
    //     _restartRequested, _disposed, _gate) ...

    public Task StartAsync(
        Func<TValue, CancellationToken, ValueTask<Result>> onMessage,
        Func<ValueTask>? onRestartRequested = null,
        CancellationToken ct = default)
    {
        _restartCts = new CancellationTokenSource();
        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct, _restartCts.Token);
        _changeSub = changeSource.OnChange(TriggerRestart);
        // Confluent-клиент НЕ строится здесь: построение отложено до валидных
        // параметров внутри ConsumeLoop (fail-open, спека §3.4).
        _loop = Task.Run(() => ConsumeLoop(onMessage));
        _supervisor = SuperviseAsync(onRestartRequested);
        return _supervisor;
    }

    private async Task ConsumeLoop(Func<TValue, CancellationToken, ValueTask<Result>> onMessage)
    {
        try
        {
            // Построение внутри try: отмена во время ожидания параметров — штатный выход
            var consumer = await GetConsumerAsync();
            while (!_cts!.Token.IsCancellationRequested)
            {
                // ... тело прежнего while: Consume/ConsumeException/onMessage/Commit — без изменений ...
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    // Отложенное построение: ждём валидные параметры (отменяемо), строим, подписываемся.
    private async Task<IConsumer<TKey, TValue>> GetConsumerAsync()
    {
        if (_consumer is { } existing)
            return existing;
        if (connectionProvider.Current is null)
            logger.LogInformation("Kafka consumer ждёт параметры подключения: topic={Topic} groupId={GroupId}", topic, groupId);
        var p = await _waiter.WaitAsync(_cts!.Token);
        var built = new ConsumerBuilder<TKey, TValue>(KafkaConfluentConfigs.Consumer(p, groupId)).Build();
        built.Subscribe(topic);
        return _consumer = built;
    }

    // TearDownAsync: прежняя логика (null-безопасна: _consumer может быть null,
    // если ожидание параметров прервано отменой); GetConsumer() (старый синхронный)
    // удалить. StopAsync/SuperviseAsync/TriggerRestart/DisposeAsync — без изменений.
}
```

`IKafkaConsumerBuilder.cs` — builder:

```csharp
// Было: KafkaConsumerBuilder<TConfig, TKey, TValue>(IOptionsMonitor<TConfig>, IOptionsMonitor<KafkaOptions>, ILogger<...>)
// Стало:
[InjectAsSingleton]
internal sealed class KafkaConsumerBuilder<TConfig, TKey, TValue>(
    IOptionsMonitor<TConfig> config,
    IKafkaConnectionProvider connectionProvider,
    ILogger<KafkaConsumer<TKey, TValue>> logger) : IKafkaConsumerBuilder<TConfig, TKey, TValue>
    where TConfig : KafkaConfig
{
    public IKafkaConsumer<TKey, TValue> Build()
    {
        var groupId = config.CurrentValue.GroupId;
        var topic = config.CurrentValue.Topic;
        IKafkaConfigChangeSource changeSource = new KafkaConfigChangeSource<TConfig>(config, connectionProvider);
        return new KafkaConsumer<TKey, TValue>(connectionProvider, groupId, topic, changeSource, logger);
    }
}
```

Xml-doc обоих файлов обновить (`KafkaOptions` → шов; комментарий «BootstrapServers берётся из KafkaOptions» → «из IKafkaConnectionProvider»).

Выход: consumer на шве. Проверка: фильтр `~PuzzleServer.UnitTests.Kafka` → PASS; `dotnet build src/PuzzleServer.Api.slnx` → 0 warnings; весь unit-набор зелёный.

- [ ] **Step 7: Коммит**

```bash
git add src/PuzzleServer.Infrastructure.App.Kafka/ src/PuzzleServer.UnitTests/Kafka/
git commit -m "feat: consumer waits for connection params (fail-open), change source on provider seam (t10 Ф1/Ф2)"
```

---

### Task 6: `DiscoveryKafkaConnectionProvider` (HaDb) + ссылка на HA.Kafka

**[repo: Puzzle]** Спека §3.3 (принцип 3: зависимость App.Kafka → HA.Kafka).

**Files:**
- Modify: `src/PuzzleServer.Infrastructure.App.Kafka/PuzzleServer.Infrastructure.App.Kafka.csproj` (+ProjectReference HA.Kafka)
- Create: `src/PuzzleServer.Infrastructure.App.Kafka/DiscoveryKafkaConnectionProvider.cs`
- Create: `src/PuzzleServer.UnitTests/Kafka/Fakes/FakeDiscoveryStore.cs`
- Create: `src/PuzzleServer.UnitTests/Kafka/Fakes/SpyLogger.cs`
- Test: `src/PuzzleServer.UnitTests/Kafka/DiscoveryKafkaConnectionProviderTests.cs`

**Interfaces:**
- Consumes: `IKafkaConnectionProvider` (Task 3); из HA.Kafka: `IKafkaDiscoveryStore` (`Result<KafkaClusterSnapshot> Get(string cluster)`; `event Action<KafkaClusterSnapshot>? Updated`), `KafkaClusterSnapshot.GetClientConfig() → KafkaClientConfig?` (record `KafkaClientConfig(BootstrapServers, SecurityProtocol, SaslMechanism, SaslUsername, SaslPassword)`), `HaKafkaException`.
- Produces: `internal sealed class DiscoveryKafkaConnectionProvider(IKafkaDiscoveryStore store, string cluster, ILogger<DiscoveryKafkaConnectionProvider>) : IKafkaConnectionProvider`.

- [ ] **Step 1: Фейк стора**

`src/PuzzleServer.UnitTests/Kafka/Fakes/FakeDiscoveryStore.cs`:

```csharp
using PuzzleServer.Infrastructure.App;
using PuzzleServer.Infrastructure.App.HA.Kafka;
using PuzzleServer.Infrastructure.App.HA.Kafka.Model;

namespace PuzzleServer.UnitTests.Kafka.Fakes;

/// <summary>Ручной IKafkaDiscoveryStore: Publish заменяет снапшот и стреляет Updated.</summary>
public sealed class FakeDiscoveryStore : IKafkaDiscoveryStore
{
    private readonly Dictionary<string, KafkaClusterSnapshot> _snapshots = new();
    private readonly List<Action<KafkaClusterSnapshot>> _handlers = [];
    private readonly object _gate = new();

    public event Action<KafkaClusterSnapshot>? Updated
    {
        add { lock (_gate) _handlers.Add(value); }
        remove { lock (_gate) _handlers.Remove(value); }
    }

    public Result<KafkaClusterSnapshot> Get(string cluster)
    {
        lock (_gate)
            return _snapshots.TryGetValue(cluster, out var snapshot)
                ? Result<KafkaClusterSnapshot>.Success(snapshot)
                : Result<KafkaClusterSnapshot>.Failed(new HaKafkaException($"kafka-кластер {cluster} не заявлен"));
    }

    public Task<Result<KafkaClusterSnapshot>> RefreshAsync(string cluster, CancellationToken ct)
        => Task.FromResult(Get(cluster));

    public void Publish(KafkaClusterSnapshot snapshot)
    {
        Action<KafkaClusterSnapshot>[] snapshotHandlers;
        lock (_gate)
        {
            _snapshots[snapshot.Cluster] = snapshot;
            snapshotHandlers = [.. _handlers];
        }
        foreach (var handler in snapshotHandlers)
            handler(snapshot);
    }
}
```

Примечание: `Publish` зеркалит реальный `KafkaDiscoveryStore.Publish` (t05): снапшот записывается в кэш ДО вызова handlers — это существенно для baseline-дедупа провайдера (см. код Step 3 и регрессионный комментарий тест-класса Step 2).

`src/PuzzleServer.UnitTests/Kafka/Fakes/SpyLogger.cs`:

```csharp
using Microsoft.Extensions.Logging;

namespace PuzzleServer.UnitTests.Kafka.Fakes;

/// <summary>
/// Собирает записи лога для ассертов (спека §8 п.8: SaslPassword не светится
/// в логах провайдера/builder'ов).
/// </summary>
public sealed class SpyLogger<T> : ILogger<T>
{
    private readonly object _gate = new();

    public List<(LogLevel Level, string Message)> Entries { get; } = [];

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        lock (_gate)
            Entries.Add((logLevel, formatter(state, exception)));
    }
}
```

- [ ] **Step 2: Падающие тесты Discovery-провайдера**

`src/PuzzleServer.UnitTests/Kafka/DiscoveryKafkaConnectionProviderTests.cs`:

```csharp
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using PuzzleServer.Infrastructure.App.HA.Kafka.Model;
using PuzzleServer.Infrastructure.App.Kafka;
using PuzzleServer.UnitTests.Kafka.Fakes;
using Xunit;

namespace PuzzleServer.UnitTests.Kafka;

// Discovery-провайдер (спека §3.3): Current из GetClientConfig() (null-семантика),
// OnChange — по Updated, фильтр по соединительным параметрам (шум топиков не роняет).
// РЕГРЕССИЯ baseline (review Фазы 4): KafkaDiscoveryStore.Publish пишет снапшот в кэш
// ДО вызова handlers — baseline дедупа обязан фиксироваться на момент ПОДПИСКИ, иначе
// первое изменение после подписки поглощается. Инвариант тестов ниже: первое событие
// после подписки, изменившее параметры, ОБЯЗАНО стрелять (fired=1), а не гаситься.
public class DiscoveryKafkaConnectionProviderTests
{
    private const string OldPassword = "abcdefghijklmnopqrstuvwxyz012345";

    private static KafkaClusterSnapshot Snapshot(
        string? bootstrap = "h1:9092,h2:9092", string? user = "app", string? password = OldPassword,
        string[]? topics = null)
        => new("events", null, bootstrap,
            user is null || password is null ? null : new KafkaAppSecret(user, password),
            (topics ?? ["orders"]).Select(t => new KafkaTopicInfo(t, 1, 1, null)).ToList(),
            DateTimeOffset.UtcNow, 42);

    private static DiscoveryKafkaConnectionProvider Make(FakeDiscoveryStore store)
        => new(store, "events", NullLogger<DiscoveryKafkaConnectionProvider>.Instance);

    [Fact]
    public void Current_ClusterNotInStore_ReturnsNull()
    {
        // Arrange — снапшот ещё не собран (Get → Failed)
        var store = new FakeDiscoveryStore();
        var provider = Make(store);

        // Act / Assert
        provider.Current.Should().BeNull();
    }

    [Fact]
    public void Current_NoEndpointsOrIncompleteSecret_ReturnsNull()
    {
        // Arrange
        var storeNoEndpoints = new FakeDiscoveryStore();
        storeNoEndpoints.Publish(Snapshot(bootstrap: null));  // нет endpoints
        var storeNoSecret = new FakeDiscoveryStore();
        storeNoSecret.Publish(Snapshot(password: null));      // неполный секрет

        // Act / Assert
        Make(storeNoEndpoints).Current.Should().BeNull("нет endpoints → GetClientConfig() == null");
        Make(storeNoSecret).Current.Should().BeNull("неполный набор кредов → GetClientConfig() == null");
    }

    [Fact]
    public void Current_FullDiscoverySet_MapsClientConfig()
    {
        // Arrange
        var store = new FakeDiscoveryStore();
        store.Publish(Snapshot());
        var provider = Make(store);

        // Act
        var current = provider.Current;

        // Assert — константы контракта pg/arch/15 §5 п.2
        current.Should().NotBeNull();
        current!.BootstrapServers.Should().Be("h1:9092,h2:9092");
        current.SecurityProtocol.Should().Be("SASL_PLAINTEXT");
        current.SaslMechanism.Should().Be("PLAIN");
        current.SaslUsername.Should().Be("app");
        current.SaslPassword.Should().Be(OldPassword);
    }

    [Fact]
    public void OnChange_BootstrapChanged_Fires()
    {
        // Arrange
        var store = new FakeDiscoveryStore();
        store.Publish(Snapshot());
        var provider = Make(store);
        var fired = 0;
        provider.OnChange(() => fired++);

        // Act
        store.Publish(Snapshot(bootstrap: "h3:9094"));

        // Assert
        fired.Should().Be(1);
    }

    [Fact]
    public void OnChange_PasswordRotated_Fires()
    {
        // Arrange
        var store = new FakeDiscoveryStore();
        store.Publish(Snapshot());
        var provider = Make(store);
        var fired = 0;
        provider.OnChange(() => fired++);

        // Act — сценарий ротации app_password (arch/16 §5 H, фаза B)
        store.Publish(Snapshot(password: "0123456789abcdefghijklmnopqrstuv"));

        // Assert
        fired.Should().Be(1);
    }

    [Fact]
    public void OnChange_TopicsOnlyChanged_DoesNotFire()
    {
        // Arrange
        var store = new FakeDiscoveryStore();
        store.Publish(Snapshot(topics: ["orders"]));
        var provider = Make(store);
        var fired = 0;
        provider.OnChange(() => fired++);

        // Act — изменился только реестр топиков (автосинк, arch/15 §3)
        store.Publish(Snapshot(topics: ["orders", "payments"]));

        // Assert
        fired.Should().Be(0, "шум реестра топиков не должен ронять клиентские соединения (спека §2 п.5)");
    }

    [Fact]
    public void OnChange_NullToParams_Fires()
    {
        // Arrange — снапшота не было (провайдер ещё не вычислял baseline)
        var store = new FakeDiscoveryStore();
        var provider = Make(store);
        var fired = 0;
        provider.OnChange(() => fired++);

        // Act — кластер поднялся: endpoints+креды появились
        store.Publish(Snapshot());

        // Assert
        fired.Should().Be(1, "переход null→params — событие (спека §3.1)");
    }

    [Fact]
    public void OnChange_ForeignClusterSnapshot_Ignored()
    {
        // Arrange
        var store = new FakeDiscoveryStore();
        store.Publish(Snapshot());
        var provider = Make(store);
        var fired = 0;
        provider.OnChange(() => fired++);

        // Act — чужой кластер (несколько заявок в одном процессе)
        var foreign = Snapshot(bootstrap: "h9:9999");
        foreign = foreign with { Cluster = "pending" };
        store.Publish(foreign);

        // Assert
        fired.Should().Be(0);
    }

    [Fact]
    public void OnChange_HandlerThrows_DoesNotBreakProvider()
    {
        // Arrange
        var store = new FakeDiscoveryStore();
        store.Publish(Snapshot());
        var provider = Make(store);
        provider.OnChange(() => throw new InvalidOperationException("subscriber bug"));
        var fired = 0;
        provider.OnChange(() => fired++);

        // Act
        store.Publish(Snapshot(bootstrap: "h4:9096"));

        // Assert — исключение подписчика гасится логом (спека §3.1)
        fired.Should().Be(1);
    }

    [Fact]
    public void OnChange_SubscriberFailure_LogsNoPassword()
    {
        // Arrange — лог-шпион + «падающий» подписчик: warning-лог гарантированно пишется
        var store = new FakeDiscoveryStore();
        store.Publish(Snapshot());
        var logger = new SpyLogger<DiscoveryKafkaConnectionProvider>();
        var provider = new DiscoveryKafkaConnectionProvider(store, "events", logger);
        provider.OnChange(() => throw new InvalidOperationException("subscriber bug"));
        var fired = 0;
        provider.OnChange(() => fired++);

        // Act — ротация пароля: провайдер стреляет, warning гасит исключение подписчика
        store.Publish(Snapshot(password: "0123456789abcdefghijklmnopqrstuv"));

        // Assert — записи есть, но ни OLD-, ни NEW-пароля в них нет (спека §8 п.8)
        fired.Should().Be(1);
        logger.Entries.Should().NotBeEmpty("warning о падении подписчика должен попадать в лог");
        logger.Entries.Should().OnlyContain(e =>
            !e.Message.Contains(OldPassword) && !e.Message.Contains("0123456789abcdefghijklmnopqrstuv"),
            "SaslPassword не должен светиться в логах провайдера (редакция — только ***)");
    }
}
```

Проверка: фильтр `~PuzzleServer.UnitTests.Kafka.DiscoveryKafkaConnectionProviderTests` → FAIL.

- [ ] **Step 3: Ссылка на HA.Kafka + реализация**

`PuzzleServer.Infrastructure.App.Kafka.csproj`, ItemGroup с ProjectReference:

```xml
<ItemGroup>
  <ProjectReference Include="..\PuzzleServer.Infrastructure.App.HA.Kafka\PuzzleServer.Infrastructure.App.HA.Kafka.csproj"/>
</ItemGroup>
```

(добавить в существующий ItemGroup с ProjectReference на `PuzzleServer.Infrastructure.App`; новый ItemGroup не нужен).

`src/PuzzleServer.Infrastructure.App.Kafka/DiscoveryKafkaConnectionProvider.cs`:

```csharp
using Microsoft.Extensions.Logging;
using PuzzleServer.Infrastructure.App.HA.Kafka;
using PuzzleServer.Infrastructure.App.HA.Kafka.Model;

namespace PuzzleServer.Infrastructure.App.Kafka;

/// <summary>
/// HaDb-ветка (спека §3.3): параметры из etcd-снапшота HA.Kafka. Current — вычисление
/// из Get(cluster).GetClientConfig() (null при Failed/отсутствии endpoints/неполном
/// секрете); State кластера НЕ интерпретируется — клиенту достаточно наличия точек
/// дискавери. OnChange — подписка на store.Updated своего кластера с фильтром по
/// value-equality вычисленных параметров (шум реестра топиков не проходит); baseline
/// дедупа — значение на момент ПЕРВОЙ ПОДПИСКИ (инвариант спеки §3.3 «последнее
/// отданное значение»), обновляется только событиями. Hosted-сервисов нет:
/// актуализацией владеет HA.Kafka (KafkaDiscoveryRefresher).
/// </summary>
internal sealed class DiscoveryKafkaConnectionProvider(
    IKafkaDiscoveryStore store,
    string cluster,
    ILogger<DiscoveryKafkaConnectionProvider> logger) : IKafkaConnectionProvider
{
    private readonly object _gate = new();
    private Action[] _handlers = [];
    private bool _subscribed;
    // Baseline дедупа: параметры на момент первой подписки (Current его не трогает)
    private KafkaConnectionParams? _last;

    // Всегда свежо и без сети: Get — мгновенно из кэша стора (t05)
    public KafkaConnectionParams? Current => Compute(store.Get(cluster));

    public IDisposable OnChange(Action handler)
    {
        lock (_gate)
        {
            _handlers = [.. _handlers, handler];
            if (_subscribed)
                return new Subscription(this, handler);
            _subscribed = true;
            // Baseline фиксируем ДО подписки на событие. KafkaDiscoveryStore.Publish
            // (t05) сначала ЗАПИСЫВАЕТ снапшот в кэш стора и только потом зовёт
            // handlers — вычисление baseline внутри OnStoreUpdated сравнивало бы
            // новое значение с самим собой (store.Get уже отдаёт НОВЫЙ снапшот) и
            // ПОГЛОЩАЛО первое изменение после подписки (баг review Фазы 4).
            // Compute → store.Get берёт лок стора и не коллбечит обратно — вложенная
            // блокировка _gate → store безопасна. Крошечное окно «событие между
            // baseline и +=» не догоняется (Current всегда отдаёт актуальное,
            // следующее событие стрельнёт от разницы с baseline).
            _last = Compute(store.Get(cluster));
            store.Updated += OnStoreUpdated;
        }
        return new Subscription(this, handler);
    }

    private void OnStoreUpdated(KafkaClusterSnapshot snapshot)
    {
        if (snapshot.Cluster != cluster)
            return;

        KafkaConnectionParams? latest = ComputeFromSnapshot(snapshot);
        Action[] handlers;
        lock (_gate)
        {
            if (Equals(_last, latest))
                return; // соединительные параметры не изменились — шум (спека §2 п.5)
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
                logger.LogWarning(ex, "IKafkaConnectionProvider: подписчик OnChange бросил исключение (погашено)");
            }
        }
    }

    private static KafkaConnectionParams? Compute(Result<KafkaClusterSnapshot> result)
        => result.IsSuccess ? ComputeFromSnapshot(result.Value) : null;

    private static KafkaConnectionParams? ComputeFromSnapshot(KafkaClusterSnapshot snapshot)
        => snapshot.GetClientConfig() is { } cc
            ? new KafkaConnectionParams(cc.BootstrapServers, cc.SecurityProtocol, cc.SaslMechanism, cc.SaslUsername, cc.SaslPassword)
            : null;

    private void Unsubscribe(Action handler)
    {
        lock (_gate)
            _handlers = _handlers.Where(h => !ReferenceEquals(h, handler)).ToArray();
    }

    private sealed class Subscription(DiscoveryKafkaConnectionProvider owner, Action handler) : IDisposable
    {
        public void Dispose() => owner.Unsubscribe(handler);
    }
}
```

Примечание: `Result.IsSuccess`/`Value` — сверить с фактическим API `Result<T>` Puzzle (используется как в `ConfigurationTopologyStore`: `built.Apply(...)`, `Result<T>.Success(...)`); при иной форме (например `TryGet`) — зеркалить существующие вызовы из `KafkaDiscoveryStore`.

Выход: провайдер. Проверка: фильтр → PASS; build 0 warnings; весь unit-набор зелёный.

- [ ] **Step 4: Коммит**

```bash
git add src/PuzzleServer.Infrastructure.App.Kafka/ src/PuzzleServer.UnitTests/Kafka/
git commit -m "feat: DiscoveryKafkaConnectionProvider over HA.Kafka store — params from etcd snapshot, Updated filtering (t10 Ф2)"
```

---

### Task 7: Ветвление `AddKafka` по `Database:Source` + конфигурация

**[repo: Puzzle]** Спека §3.5–3.6; `HaKafka:MembersMode=Off` — решение user-review Фазы 3 (в spec отсутствует, зафиксировано как осознанное отклонение в конце плана: общие IEtcdClient/ротация консистентны при совпадающих секциях, один etcd на стенд; ноль правок HA.Kafka).

**Files:**
- Modify: `src/PuzzleServer.Infrastructure.App.Kafka/ModuleExtensions.cs`
- Modify: `src/PuzzleServer.Api/appsettings.json`
- Modify: `src/PuzzleServer.UnitTests/Kafka/KafkaTopicAdminRegistrationTests.cs` (добавить `Database:Source=Aspire` — тесты описывают Aspire-ветку)
- Test: `src/PuzzleServer.UnitTests/Kafka/AddKafkaSourceBranchingTests.cs` (new)

**Interfaces:**
- Consumes: `DatabaseSourceReader`/`DatabaseSource` (`PuzzleServer.Infrastructure.App/DB/DatabaseSource.cs`), `AddHaKafka`/`AddKafkaCluster` (HA.Kafka), `DiscoveryKafkaConnectionProvider` (Task 6), `ConfigurationKafkaConnectionProvider` (Task 3).
- Produces: семантика `AddKafka(services, configuration)`: Aspire → Configuration-провайдер; HaDb → fail-fast без `Kafka:Cluster`, `AddHaKafka(...).AddKafkaCluster(<имя>)` + Discovery-провайдер.

- [ ] **Step 1: Падающие тесты ветвления**

```csharp
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PuzzleServer.Infrastructure.App.Kafka;
using Xunit;

namespace PuzzleServer.UnitTests.Kafka;

// Ветвление AddKafka по Database:Source (спека §3.5): один переключатель на БД и Kafka.
public class AddKafkaSourceBranchingTests
{
    private static IConfiguration Config(params KeyValuePair<string, string?>[] extra)
    {
        var dict = new Dictionary<string, string?>
        {
            ["ConnectionStrings:Kafka"] = "broker:9092",
        };
        foreach (var kv in extra)
            dict[kv.Key] = kv.Value;
        return new ConfigurationBuilder().AddInMemoryCollection(dict).Build();
    }

    private sealed class TestConfig : KafkaConfig;

    [Fact]
    public void AddKafka_AspireMode_RegistersConfigurationProvider()
    {
        // Arrange — фейк стора не нужен: Aspire-ветка etcd-стек не регистрирует вовсе
        var config = Config(new KeyValuePair<string, string?>("Database:Source", "Aspire"));
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));

        // Act
        services.AddKafka(config);
        using var sp = services.BuildServiceProvider();

        // Assert — etcd-стека в контейнере нет вовсе
        sp.GetService<PuzzleServer.Infrastructure.App.HA.Kafka.HaKafkaClusterRegistry>().Should().BeNull();
        sp.GetRequiredService<IKafkaConnectionProvider>().Should().BeOfType<ConfigurationKafkaConnectionProvider>();
    }

    [Fact]
    public void AddKafka_HaDbMode_RegistersHaKafkaWithClusterClaim()
    {
        // Arrange — фейк стора НЕ регистрируем (review Ф4-3): AutoRegistration HA.Kafka
        // из AddKafka добавила бы реальный KafkaDiscoveryStore ПОСЛЕ нашего фейка и
        // выиграла бы у него (Add, не TryAdd) — фейк стал бы мёртвым грузом. Реальный
        // стор резолвится в unit-контейнере безопасно: ctor без сетевых вызовов, а
        // валидация HaKafkaOptions проходит при заданных EtcdEndpoints + заявке кластера.
        var config = Config(
            new KeyValuePair<string, string?>("Kafka:Cluster", "events"),
            new KeyValuePair<string, string?>("HaKafka:EtcdEndpoints:0", "http://localhost:2379"));
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));

        // Act
        services.AddKafka(config);
        using var sp = services.BuildServiceProvider();

        // Assert — заявка кластера зарегистрирована, провайдер — Discovery
        var registry = sp.GetRequiredService<PuzzleServer.Infrastructure.App.HA.Kafka.HaKafkaClusterRegistry>();
        registry.Clusters.Should().Contain("events");
        sp.GetRequiredService<IKafkaConnectionProvider>().Should().BeOfType<DiscoveryKafkaConnectionProvider>();
    }

    [Fact]
    public void AddKafka_HaDbModeWithoutCluster_FailFast()
    {
        // Arrange
        var config = Config(new KeyValuePair<string, string?>("HaKafka:EtcdEndpoints:0", "http://localhost:2379"));
        var services = new ServiceCollection();

        // Act / Assert — пустой Kafka:Cluster: ошибка при регистрации (спека §2 п.6)
        var act = () => services.AddKafka(config);
        act.Should().Throw<InvalidOperationException>().WithMessage("*Kafka:Cluster*");
    }

    [Fact]
    public void AddKafka_UnknownSource_FailFast()
    {
        // Arrange
        var config = Config(new KeyValuePair<string, string?>("Database:Source", "Somewhere"));
        var services = new ServiceCollection();

        // Act / Assert — нераспознанный Source падает в DatabaseSourceReader (спека §2 п.1)
        var act = () => services.AddKafka(config);
        act.Should().Throw<InvalidOperationException>();
    }
}
```

Проверка: фильтр `~PuzzleServer.UnitTests.Kafka.AddKafkaSourceBranchingTests` → FAIL.

- [ ] **Step 2: Реализовать ветвление**

`ModuleExtensions.AddKafka` — в начале метода (после чтения секции `Kafka`, до `services.Configure<KafkaOptions>`):

```csharp
// Ветвление источника соединительных параметров (спека §3.5): один переключатель
// Database:Source на БД и Kafka (решение user-review). Aspire — конфигурация;
// HaDb — HA.Kafka-дискавери + флуент-заявка кластера из Kafka:Cluster.
var source = PuzzleServer.Infrastructure.App.DB.DatabaseSourceReader.Read(configuration);
if (source == PuzzleServer.Infrastructure.App.DB.DatabaseSource.Aspire)
{
    services.AddSingleton<IKafkaConnectionProvider>(sp => new ConfigurationKafkaConnectionProvider(
        sp.GetRequiredService<IOptionsMonitor<KafkaOptions>>()));
}
else
{
    var cluster = configuration["Kafka:Cluster"];
    if (string.IsNullOrWhiteSpace(cluster))
        throw new InvalidOperationException(
            "Kafka:Cluster не задан (Database:Source=HaDb) — имя kafka-кластера для дискавери обязательно (спека t10 §2 п.6)");
    services.AddHaKafka(configuration).AddKafkaCluster(cluster);
    services.AddSingleton<IKafkaConnectionProvider>(sp => new DiscoveryKafkaConnectionProvider(
        sp.GetRequiredService<IKafkaDiscoveryStore>(),
        cluster,
        sp.GetRequiredService<ILogger<DiscoveryKafkaConnectionProvider>>()));
}
```

Заменить этим блоком регистрацию из Task 4 (безусловную Configuration). Прочее (`KafkaOptions`-Configure, admin-фабрика, AutoRegistration) — без изменений. Xml-doc метода обновить.

- [ ] **Step 3: Обновить appsettings.json**

`src/PuzzleServer.Api/appsettings.json` — добавить после секции `DdMap`:

```json
"Kafka": {
  "Cluster": "events"
},
"HaKafka": {
  "EtcdEndpoints": [ "http://localhost:2379" ],
  "MembersMode": "Off"
}
```

Пояснение (в commit message и docs Task 10): HaDb-режим запускает HA.Db и HA.Kafka в одном процессе; MembersMode=Off оставляет один members-монитор (HaDb), общие IEtcdClient/ротация консистентны при совпадающих `HaDb:EtcdEndpoints`/`HaKafka:EtcdEndpoints` (один etcd на стенд — AGENTS pg).

- [ ] **Step 4: Обновить существующие registration-тесты**

`KafkaTopicAdminRegistrationTests.cs`: в `BuildProvider` и в локальном конфиге `AddKafka_SeamFactoryOverridableByFake` добавить `["Database:Source"] = "Aspire"` (тесты описывают Aspire-ветку; без этого новый fail-fast HaDb-ветки уронит их).

- [ ] **Step 5: Проверка и коммит**

Проверка: фильтр `~PuzzleServer.UnitTests.Kafka` → PASS; весь unit-набор зелёный; `dotnet build src/PuzzleServer.Api.slnx` → 0 warnings.

```bash
git add src/PuzzleServer.Infrastructure.App.Kafka/ModuleExtensions.cs src/PuzzleServer.Api/appsettings.json src/PuzzleServer.UnitTests/Kafka/
git commit -m "feat: AddKafka branches on Database:Source — HaDb registers HA.Kafka discovery with Kafka:Cluster claim (t10 Ф3)"
```

---

### Task 8: Интеграционная фикстура etcd + Kafka-SASL

**[repo: Puzzle]** Спека §5 Ф4, §7 (полный контур). Порты — фиксированные значения из диапазона интеграционных фикстур Puzzle, вне зоны dev-станда pg (16xxx), БЕЗ пересечений между параллельными xunit-коллекциями: 32379–32381 (HA/Db cluster), 32490 (etcd-фикстура t05), **32495 (etcd t10 — отдельный порт: t05-класс KafkaDiscoveryIntegrationTests живёт в default-коллекции и параллелится с t10-коллекцией, общий 32490 дал бы «port is already allocated»)**, kafka — зонд 32500–32509.

**Files:**
- Modify: `src/PuzzleServer.IntegrationTests/HA/Kafka/KafkaEtcdFixture.cs` (параметризация host-порта; дефолт 32490 — поведение t05 не меняется)
- Create: `src/PuzzleServer.IntegrationTests/Kafka/KafkaSaslFixture.cs`

**Interfaces (produces, для Task 9):**
```csharp
public sealed class KafkaSaslFixture : IAsyncLifetime
{
    public string EtcdEndpoint { get; }          // http://localhost:32495 (внутренний KafkaEtcdFixture(32495))
    public string Bootstrap { get; }             // localhost:<hostPort> — advertised CLIENT
    public const string OldPassword = "abcdefghijklmnopqrstuvwxyz012345";
    public const string NewPassword = "0123456789abcdefghijklmnopqrstuv";
    public const string TopicName = "t10-roundtrip";
    public Task PutAsync(string key, string value);            // засев в etcd
    public Task SeedDiscoveryKeysAsync(string password);       // config/endpoints/app_user/app_password
    public Task StopEtcdAsync(); public Task StartEtcdAsync(); // fail-open-сценарии
}
```
Consumes: `KafkaEtcdFixture` (t05, `IntegrationTests/HA/Kafka/`, после параметризации — `new KafkaEtcdFixture(hostPort)`), Testcontainers `ContainerBuilder`.

- [ ] **Step 1a: Параметризовать KafkaEtcdFixture host-портом**

Вход: `KafkaEtcdFixture` хардкодит `private const int HostPort = 32490` в `WithPortBinding(HostPort, 2379)`; t05-тесты используют `new KafkaEtcdFixture()` (без конструктора). Действие: заменить константу на optional-параметр первичного конструктора (t05 не трогаем — дефолт сохраняет их поведение 1:1):

```csharp
// Было:
// public sealed class KafkaEtcdFixture : IAsyncLifetime
// {
//     // Свободный диапазон интеграционных фикстур (HA/Db cluster: 32379–32381)
//     private const int HostPort = 32490;
//     private readonly IContainer _container = new ContainerBuilder("quay.io/coreos/etcd:v3.5.21")
//         ... .WithPortBinding(HostPort, 2379).Build();

// Стало:
// Порт — параметр (t10-фикстура поднимает СВОЙ etcd на 32495: xunit параллелит
// коллекции, общий 32490 с t05-классом дал бы «port is already allocated»).
// Дефолт 32490 — все существующие t05-тесты без изменений.
public sealed class KafkaEtcdFixture(int hostPort = 32490) : IAsyncLifetime
{
    private readonly IContainer _container = new ContainerBuilder("quay.io/coreos/etcd:v3.5.21")
        .WithCommand(
            "etcd",
            "--name=test",
            "--data-dir=/etcd-data",
            "--listen-client-urls=http://0.0.0.0:2379",
            "--advertise-client-urls=http://127.0.0.1:2379")
        .WithPortBinding(hostPort, 2379)
        .Build();
    // ... остальное без изменений (Endpoint, InitializeAsync, StopEtcdAsync,
    //     StartEtcdAsync, WaitReadyAsync, PutAsync, GetAsync, DisposeAsync)
}
```

Выход: фикстура принимает порт. Проверка: `dotnet build src/PuzzleServer.Api.slnx` → 0 warnings; существующие t05-тесты не менялись (`git diff --stat src/PuzzleServer.IntegrationTests/HA/Kafka/` — только KafkaEtcdFixture.cs).

- [ ] **Step 1b: Написать фикстуру**

```csharp
using System.Net;
using System.Net.Sockets;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using PuzzleServer.IntegrationTests.HA.Kafka;
using Xunit;

namespace PuzzleServer.IntegrationTests.Kafka;

// Полный контур t10 (спека §7): etcd (KafkaEtcdFixture на СОБСТВЕННОМ фиксированном
// host-порту 32495 — отдельная зона: t05-класс KafkaDiscoveryIntegrationTests живёт
// в default-коллекции и ПАРАЛЛЕЛИТСЯ с t10-коллекцией, общий с ним 32490 дал бы
// «port is already allocated» при полном прогоне; фиксированный порт гарантирует
// рестарты fail-open-сценариев на том же endpoint) + apache/kafka:4.0.0, единственный
// combined KRaft-брокер, CLIENT-listener SASL_PLAINTEXT на host-порту из зонда
// 32500–32509 (advertised должен быть известен ДО старта контейнера — env фиксируется
// при создании; диапазон вне зоны dev-станда pg 16xxx и занятых фикстур).
// JAAS держит ОКНО двух пользователей (app=OLD, app2=NEW) — сценарий ротации
// app_password (arch/16 §5 H): после put NEW клиент обязан переподключиться с NEW.
public sealed class KafkaSaslFixture : IAsyncLifetime
{
    public const string OldPassword = "abcdefghijklmnopqrstuvwxyz012345";
    public const string NewPassword = "0123456789abcdefghijklmnopqrstuv";
    public const string TopicName = "t10-roundtrip";
    private const string InterPassword = "interpass1234567890abcdefgh";

    // Отдельная зона etcd-порта t10 (см. комментарий класса): 32490 занят t05
    private const int EtcdHostPort = 32495;

    private readonly KafkaEtcdFixture _etcd = new(EtcdHostPort);

    private readonly IContainer _kafka = new ContainerBuilder("apache/kafka:4.0.0")
        .WithEnvironment("CLUSTER_ID", "ak0CLUSTERIDt10EXAMPLE1")   // 22 симв base64url
        .WithEnvironment("KAFKA_NODE_ID", "1")
        .WithEnvironment("KAFKA_PROCESS_ROLES", "broker,controller")
        .WithEnvironment("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
        .WithEnvironment("KAFKA_LISTENERS", "CONTROLLER://:9093,INTERNAL://:9092,CLIENT://:9094")
        .WithEnvironment("KAFKA_ADVERTISED_LISTENERS", $"INTERNAL://localhost:9092,CLIENT://localhost:{HostPort()}")
        .WithEnvironment("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "CONTROLLER:PLAINTEXT,INTERNAL:SASL_PLAINTEXT,CLIENT:SASL_PLAINTEXT")
        .WithEnvironment("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .WithEnvironment("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL")
        .WithEnvironment("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
        .WithEnvironment("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAIN")
        .WithEnvironment("KAFKA_LISTENER_NAME_INTERNAL_PLAIN_SASL_JAAS_CONFIG",
            $"org.apache.kafka.common.security.plain.PlainLoginModule required "
            + $"username=\"inter\" password=\"{InterPassword}\" user_inter=\"{InterPassword}\" "
            + $"user_app=\"{OldPassword}\" user_app2=\"{NewPassword}\";")
        .WithEnvironment("KAFKA_LISTENER_NAME_CLIENT_PLAIN_SASL_JAAS_CONFIG",
            $"org.apache.kafka.common.security.plain.PlainLoginModule required "
            + $"user_app=\"{OldPassword}\" user_app2=\"{NewPassword}\";")
        .WithEnvironment("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .WithEnvironment("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .WithEnvironment("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .WithEnvironment("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
        .WithEnvironment("KAFKA_HEAP_OPTS", "-Xmx512m")
        .WithPortBinding(HostPort(), 9094)
        .Build();

    private static int? _hostPort;

    // Зонд свободного host-порта 32500–32509 (один на фикстуру; env advertised фиксируется до старта)
    private static int HostPort()
    {
        if (_hostPort is { } cached)
            return cached;
        for (var port = 32500; port <= 32509; port++)
        {
            try
            {
                var listener = new TcpListener(IPAddress.Loopback, port);
                listener.Start();
                listener.Stop();
                return _hostPort = port;
            }
            catch (SocketException)
            {
                // порт занят — следующий
            }
        }
        throw new InvalidOperationException("нет свободного host-порта в диапазоне 32500–32509 (интеграционные фикстуры)");
    }

    public string EtcdEndpoint => _etcd.Endpoint;
    public string Bootstrap => $"localhost:{HostPort()}";

    public Task PutAsync(string key, string value) => _etcd.PutAsync(key, value);

    // Дискавери-ключи кластера events (канон arch/15 §2.1/§5)
    public Task SeedDiscoveryKeysAsync(string password)
        => Task.WhenAll(
            _etcd.PutAsync("/kafka/clusters/events/config",
                """{"brokers":1,"replication_factor":1,"min_insync_replicas":1,"default_partitions":3,"default_retention_ms":604800000,"created_unix":1756500000}"""),
            _etcd.PutAsync("/kafka/clusters/events/endpoints", Bootstrap),
            _etcd.PutAsync("/kafka/clusters/events/app_user", "app"),
            _etcd.PutAsync("/kafka/clusters/events/app_password", password));

    public Task StopEtcdAsync() => _etcd.StopEtcdAsync();
    public Task StartEtcdAsync() => _etcd.StartEtcdAsync();

    public async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await _etcd.InitializeAsync();
        await _kafka.StartAsync(ct);
        await WaitKafkaReadyAsync(ct);
    }

    // Готовность: DescribeCluster OLD-кредами + идемпотентное создание топика roundtrip
    private async Task WaitKafkaReadyAsync(CancellationToken ct)
    {
        using var admin = new AdminClientBuilder(SaslAdminConfig(OldPassword)).Build();
        for (var i = 0; i < 90; i++)
        {
            try
            {
                _ = admin.GetMetadata(TimeSpan.FromSeconds(3));
                try
                {
                    await admin.CreateTopicsAsync(new TopicSpecification[]
                        { new() { Name = TopicName, NumPartitions = 1, ReplicationFactor = 1 } });
                }
                catch (CreateTopicsException ex) when (ex.Results.Any(r =>
                    r.Error.Code == ErrorCode.TopicAlreadyExists))
                {
                    // идемпотентно
                }
                return;
            }
            catch
            {
                // брокер поднимается — следующая попытка
            }
            await Task.Delay(1000, ct);
        }
        throw new InvalidOperationException("kafka-брокер фикстуры не поднялся за 90 с");
    }

    private AdminClientConfig SaslAdminConfig(string password) => new()
    {
        BootstrapServers = Bootstrap,
        SecurityProtocol = SecurityProtocol.SaslPlaintext,
        SaslMechanism = SaslMechanism.Plain,
        SaslUsername = "app",
        SaslPassword = password,
    };

    public async ValueTask DisposeAsync()
    {
        await _kafka.DisposeAsync();
        await _etcd.DisposeAsync();
    }
}

// Одна фикстура на все тесты полного контура (etcd+kafka стартуют один раз)
[CollectionDefinition(Name)]
public sealed class T10KafkaCollection : ICollectionFixture<KafkaSaslFixture>
{
    public const string Name = "t10-kafka-e2e";
}
```

Выход: фикстура. Проверка: `dotnet build src/PuzzleServer.Api.slnx` → 0 warnings.

- [ ] **Step 2: Smoke-запуск фикстуры (временный тест → удалить перед коммитом)**

Действие: временно добавить тест-класс с одним тестом `SeedDiscoveryKeysAsync` + `GetMetadata` — убедиться контейнеры поднимаются (etcd на 32495, kafka на зондируемом порту). Локальная команда: `dotnet test src/PuzzleServer.Api.slnx --filter "FullyQualifiedName~PuzzleServer.IntegrationTests.Kafka.KafkaSaslFixtureSmoke"`. Перед коммитом smoke удалить (его роль исполнят тесты Task 9).

Выход: фикстура поднимается, топик создаётся. Проверка: тест PASS локально (Docker запущен).

- [ ] **Step 3: Коммит**

```bash
git add src/PuzzleServer.IntegrationTests/HA/Kafka/KafkaEtcdFixture.cs src/PuzzleServer.IntegrationTests/Kafka/KafkaSaslFixture.cs
git commit -m "test: kafka+etcd SASL fixture for t10 full-loop integration (apache/kafka 4.0.0, dual-user JAAS window, dynamic host port); parameterize KafkaEtcdFixture host port — t10 etcd on 32495 (parallel with t05 collection)"
```

---

### Task 9: Интеграционные тесты полного контура

**[repo: Puzzle]** Спека §7 (сценарии 1–5), §4 (ротация), критерии приёмки 4–8.

**Files:**
- Create: `src/PuzzleServer.IntegrationTests/Kafka/KafkaDiscoveryIntegrationTests.cs`

**Interfaces:**
- Consumes: `KafkaSaslFixture`/`T10KafkaCollection` (Task 8), `AddKafka` (Task 7), `IKafkaProducerBuilder`/`IKafkaConsumerBuilder`/`BusKafkaConfig` (существующие), `IKafkaConnectionProvider`/`IKafkaDiscoveryStore`.
- Produces: интеграционный набор `PuzzleServer.IntegrationTests.Kafka` (пять сценариев).

- [ ] **Step 1: Написать базовую обвязку тест-класса**

```csharp
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PuzzleServer.Infrastructure.App;
using PuzzleServer.Infrastructure.App.Bus.Consumer.BusKafka;
using PuzzleServer.Infrastructure.App.DI;
using PuzzleServer.Infrastructure.App.Kafka;
using Xunit;

namespace PuzzleServer.IntegrationTests.Kafka;

// Полный контур t10 (спека §7): реальные etcd+Kafka-SASL; хост собирается как в t05
// (ServiceCollection + ручной старт hosted-сервисов). BusKafkaConfig — реальный TConfig
// ([Config("BusKafka")]): его Topic/GroupId использует builder. Пороги ожидания —
// WatchWindowMs=300 → порог 5 с с запасом.
[Collection(T10KafkaCollection.Name)]
public class KafkaDiscoveryIntegrationTests(KafkaSaslFixture fixture) : IAsyncLifetime
{
    public ValueTask InitializeAsync() => ValueTask.CompletedTask;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private IConfiguration Config()
    {
        var data = new Dictionary<string, string?>
        {
            // Database:Source пуст → HaDb-ветка (дефолт ридера)
            ["Kafka:Cluster"] = "events",
            ["HaKafka:EtcdEndpoints:0"] = fixture.EtcdEndpoint,
            ["HaKafka:Mode"] = "WatchLongPoll",
            ["HaKafka:WatchWindowMs"] = "300",
            ["HaKafka:MembersMode"] = "Off",
            ["HaKafka:BootstrapTimeoutSec"] = "5",
            ["BusKafka:Topic"] = KafkaSaslFixture.TopicName,
            ["BusKafka:GroupId"] = $"t10-grp-{Guid.NewGuid():N}",
            // ConnectionStrings:Kafka НЕ задаём: параметры обязаны прийти только из etcd
        };
        return new ConfigurationBuilder().AddInMemoryCollection(data).Build();
    }

    private async Task<ServiceProvider> StartHostAsync(IConfiguration config)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(Microsoft.Extensions.Logging.Abstractions.NullLogger<>));
        new AutoRegistrationDiTypeBehaviour(services).UseBehaviour();
        new AutoRegistrationConfigDiTypeBehaviour(services, config).UseBehaviour();
        services.AddKafka(config); // HaDb-ветка: AddHaKafka + AddKafkaCluster("events")
        // Обход статического дедупа сборок в повторных тестах (паттерн KafkaTopicAdminRegistrationTests):
        services.AutoRegistration(typeof(KafkaTopicAdmin).Assembly.GetTypes());
        new AutoRegistrationDiTypeBehaviour(services).Handle(
            typeof(PuzzleServer.Infrastructure.App.HA.Kafka.ModuleExtensions).Assembly.GetTypes());
        var provider = services.BuildServiceProvider();
        foreach (var hosted in provider.GetServices<IHostedService>())
            await hosted.StartAsync(CancellationToken.None);
        return provider;
    }

    private static async Task<bool> WaitUntilAsync(Func<bool> probe, TimeSpan budget)
    {
        var deadline = DateTimeOffset.UtcNow + budget;
        while (DateTimeOffset.UtcNow < deadline)
        {
            if (probe())
                return true;
            await Task.Delay(200);
        }
        return probe();
    }

    // Rebuild-цикл как KafkaBusConsumerHostedService.ExecuteAsync (спека §3.4): при
    // изменении параметров consumer сам останавливает loop (provider.OnChange →
    // TriggerRestart), StartAsync возвращается — цикл пересоздаёт consumer с новыми
    // параметрами. Интеграционная верификация self-restart без рестарта процесса.
    private static async Task RunConsumerLoopAsync(
        IServiceProvider host,
        Func<string, ValueTask<Result>> onMessage,
        CancellationToken ct)
    {
        var builder = host.GetRequiredService<IKafkaConsumerBuilder<BusKafkaConfig, string, string>>();
        while (!ct.IsCancellationRequested)
        {
            var consumer = builder.Build();
            await consumer.StartAsync(
                (value, _) => onMessage(value),
                ct: ct);
        }
    }
    // ... тесты ниже ...
}
```

- [ ] **Step 2: Сценарий 1 — fail-open и самовосстановление (спека §7 п.1, критерий 7)**

```csharp
[Fact]
public async Task FailOpen_WithoutEtcdKeys_ThenSeed_Connects()
{
    // Arrange — ключей дискавери НЕТ: провайдер null, приложение живо
    using var host = await StartHostAsync(Config());
    var builder = host.GetRequiredService<IKafkaProducerBuilder<BusKafkaConfig, string, string>>();

    // Act — SendAsync отклонён (fail-open), процесс жив
    var before = await builder.Build().SendAsync("k", "v", CancellationToken.None);

    // Assert
    before.IsSuccess.Should().BeFalse("без ключей дискавери параметров нет — SendAsync Failed, не исключение");

    // Act — воркер дописал ключи (endpoints+креды): watch доставляет без рестарта хоста
    await fixture.SeedDiscoveryKeysAsync(KafkaSaslFixture.OldPassword);
    var connected = await WaitUntilAsync(
        () => host.GetRequiredService<IKafkaConnectionProvider>().Current is not null,
        TimeSpan.FromSeconds(5));

    // Assert — новый Build: кэш замещён по OnChange, запись проходит реальному брокеру
    connected.Should().BeTrue("параметры должны появиться по watch (спека §7 п.1)");
    var after = await builder.Build().SendAsync("k", "v", CancellationToken.None);
    after.IsSuccess.Should().BeTrue("после появления параметров запись должна проходить (реальный SASL-брокер)");
}
```

- [ ] **Step 3: Сценарий 2 — сквозной roundtrip по кредам из etcd (критерии 4, 6)**

```csharp
[Fact]
public async Task Roundtrip_ProducesAndConsumes_WithEtcdCredentials()
{
    // Arrange
    await fixture.SeedDiscoveryKeysAsync(KafkaSaslFixture.OldPassword);
    using var host = await StartHostAsync(Config());
    (await WaitUntilAsync(
        () => host.GetRequiredService<IKafkaConnectionProvider>().Current is not null,
        TimeSpan.FromSeconds(5))).Should().BeTrue();

    var marker = $"t10-{Guid.NewGuid():N}";
    var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
    var consumer = host.GetRequiredService<IKafkaConsumerBuilder<BusKafkaConfig, string, string>>().Build();
    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
    var supervisor = consumer.StartAsync(
        (value, _) => { if (value.Contains(marker)) received.TrySetResult(value); return ValueTask.FromResult(PuzzleServer.Infrastructure.App.Result.Success()); },
        ct: cts.Token);

    // Act
    var send = await host.GetRequiredService<IKafkaProducerBuilder<BusKafkaConfig, string, string>>()
        .Build().SendAsync(marker, marker, cts.Token);

    // Assert — креды/endpoints ТОЛЬКО из etcd-ключей (ConnectionStrings не задан)
    send.IsSuccess.Should().BeTrue();
    var winner = await Task.WhenAny(received.Task, Task.Delay(TimeSpan.FromSeconds(10), cts.Token));
    winner.Should().Be(received.Task, "consumer должен получить сообщение реального SASL-брокера");
    (await received.Task).Should().Contain(marker);
    cts.Cancel();
    await supervisor;
}
```

- [ ] **Step 4: Сценарий 3 — ротация app_password без потерь, producer И consumer (спека §1 п.2, §4, §7 п.3, критерий 6)**

```csharp
[Fact]
public async Task PasswordRotation_ProducersAndConsumersReconnect_NoMessageLoss()
{
    // Arrange — работающий контур на OLD: producer пишет, consumer (rebuild-цикл) читает
    await fixture.SeedDiscoveryKeysAsync(KafkaSaslFixture.OldPassword);
    using var host = await StartHostAsync(Config());
    (await WaitUntilAsync(
        () => host.GetRequiredService<IKafkaConnectionProvider>().Current is not null,
        TimeSpan.FromSeconds(5))).Should().BeTrue();
    var producerBuilder = host.GetRequiredService<IKafkaProducerBuilder<BusKafkaConfig, string, string>>();

    var beforeMarker = $"t10-rot-before-{Guid.NewGuid():N}";
    var afterMarker = $"t10-rot-after-{Guid.NewGuid():N}";
    var delivered = new System.Collections.Concurrent.ConcurrentQueue<string>();
    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
    var consumerLoop = Task.Run(() => RunConsumerLoopAsync(host, value =>
    {
        delivered.Enqueue(value);
        return ValueTask.FromResult(Result.Success());
    }, cts.Token), cts.Token);

    // Act / Assert — 1) поток работает на OLD: consumer получает маркер ДО ротации
    (await producerBuilder.Build().SendAsync(beforeMarker, beforeMarker, cts.Token)).IsSuccess.Should().BeTrue();
    (await WaitUntilAsync(() => delivered.Any(m => m.Contains(beforeMarker)), TimeSpan.FromSeconds(15)))
        .Should().BeTrue("consumer обязан читать на OLD-кредах до ротации (спека §1 п.2)");

    // Act — 2) фаза B ротации (arch/16 §5 H): txn etcd кладёт NEW; брокер в окне двух кредов
    await fixture.PutAsync("/kafka/clusters/events/app_password", KafkaSaslFixture.NewPassword);
    var rotated = await WaitUntilAsync(
        () => host.GetRequiredService<IKafkaConnectionProvider>().Current?.SaslPassword == KafkaSaslFixture.NewPassword,
        TimeSpan.FromSeconds(5));

    // Assert — провайдер отдаёт NEW (спека §4: «перечитывают etcd и переподключаются с NEW»)
    rotated.Should().BeTrue("ротация пароля должна дойти по watch ≤ ~2 с (спека §4)");

    // Act — 3) ПОСЛЕ ротации: новый Build пишет NEW-кредами; consumer жив — self-restart по
    // provider.OnChange + пересоздание rebuild-циклом, БЕЗ рестарта процесса (спека §3.4);
    // доставка after-маркера = поток сообщений не прервался (дубликаты допустимы — at-least-once)
    (await producerBuilder.Build().SendAsync(afterMarker, afterMarker, cts.Token)).IsSuccess
        .Should().BeTrue("переподключение с NEW не должно ломать запись (окно двух кредов)");
    (await WaitUntilAsync(() => delivered.Any(m => m.Contains(afterMarker)), TimeSpan.FromSeconds(15)))
        .Should().BeTrue("consumer должен получить after-маркер после ротации — поток непрерывен (спека §7 п.3, критерий 6)");

    cts.Cancel();
    try
    {
        await consumerLoop;
    }
    catch (OperationCanceledException)
    {
        // штатная остановка rebuild-цикла
    }
}
```

- [ ] **Step 5: Сценарий 4 — шум реестра топиков не роняет соединения (спека §2 п.5, критерий 5)**

```csharp
[Fact]
public async Task TopicRegistryNoise_DoesNotFireProviderChange()
{
    // Arrange
    await fixture.SeedDiscoveryKeysAsync(KafkaSaslFixture.OldPassword);
    using var host = await StartHostAsync(Config());
    (await WaitUntilAsync(
        () => host.GetRequiredService<IKafkaConnectionProvider>().Current is not null,
        TimeSpan.FromSeconds(5))).Should().BeTrue();
    var provider = host.GetRequiredService<IKafkaConnectionProvider>();
    var fired = 0;
    provider.OnChange(() => fired++);

    // Act — автосинк реестра: новый факт-топик (соединительные параметры не меняются)
    await fixture.PutAsync("/kafka/clusters/events/topics/registry-noise",
        """{"partitions":3,"replication_factor":1,"configs":{},"synced_unix":1750000100,"missing":false}""");
    var storeUpdated = await WaitUntilAsync(
        () => host.GetRequiredService<IKafkaDiscoveryStore>().Get("events").Value.TopicNames.Contains("registry-noise"),
        TimeSpan.FromSeconds(5));

    // Assert — событие стора пришло, провайдер молчит, запись жива
    storeUpdated.Should().BeTrue("снапшот должен обновиться (watch)");
    fired.Should().Be(0, "изменение только реестра топиков не роняет соединения (спека §2 п.5)");
    var send = await host.GetRequiredService<IKafkaProducerBuilder<BusKafkaConfig, string, string>>()
        .Build().SendAsync("noise", "noise", CancellationToken.None);
    send.IsSuccess.Should().BeTrue("старый producer продолжает работать");
}
```

- [ ] **Step 6: Сценарий 5 — смерть etcd: fail-open кэша (спека §7 п.5, критерий 7)**

```csharp
[Fact]
public async Task EtcdDown_CacheSurvives_RecoveryOnReturn()
{
    // Arrange
    await fixture.SeedDiscoveryKeysAsync(KafkaSaslFixture.OldPassword);
    using var host = await StartHostAsync(Config());
    (await WaitUntilAsync(
        () => host.GetRequiredService<IKafkaConnectionProvider>().Current is not null,
        TimeSpan.FromSeconds(5))).Should().BeTrue();
    var producerBuilder = host.GetRequiredService<IKafkaProducerBuilder<BusKafkaConfig, string, string>>();

    // Act — etcd умер: снапшот в кэше, клиент работает (etcd-контрол-плейн ≠ данные)
    await fixture.StopEtcdAsync();
    var duringOutage = await producerBuilder.Build().SendAsync("down", "down", CancellationToken.None);

    // Assert
    duringOutage.IsSuccess.Should().BeTrue("отказ etcd не должен ронять работающего клиента (fail-open)");
    host.GetRequiredService<IKafkaConnectionProvider>().Current.Should().NotBeNull("кэш снапшота живёт без etcd");

    // Act — возврат + новое изменение: обновления продолжаются
    await fixture.StartEtcdAsync();
    await fixture.PutAsync("/kafka/clusters/events/app_password", KafkaSaslFixture.NewPassword);
    var recovered = await WaitUntilAsync(
        () => host.GetRequiredService<IKafkaConnectionProvider>().Current?.SaslPassword == KafkaSaslFixture.NewPassword,
        TimeSpan.FromSeconds(10));

    // Assert
    recovered.Should().BeTrue("после возврата etcd watch восстанавливается (спека §7 п.5)");
}
```

- [ ] **Step 7: Прогон и коммит**

Проверка: `dotnet test src/PuzzleServer.Api.slnx --filter "FullyQualifiedName~PuzzleServer.IntegrationTests.Kafka"` → все PASS (Docker запущен); затем полный набор: `dotnet test src/PuzzleServer.Api.slnx` → зелёный (вкл. существующие HA/Kafka t05 — не-регрессия).

```bash
git add src/PuzzleServer.IntegrationTests/Kafka/KafkaDiscoveryIntegrationTests.cs
git commit -m "test: t10 full-loop integration — fail-open, etcd-credentials roundtrip, password rotation, topic-registry noise, etcd outage (t10 Ф4)"
```

---

### Task 10: Документация + финальная проверка

**[repo: Puzzle]** Спека §3.7, критерий 10.

**Files:**
- Modify: `docs/01.16-kafka.md`
- Modify: `docs/01.19-ha-kafka.md`

- [ ] **Step 1: Раздел в 01.16-kafka.md**

Вход: файл существует (структура §1 Конфиг, §2 Producer, …). Действие: после §1 вставить раздел «1a. Источник соединительных параметров (Aspire / HaDb)»:

```markdown
## 1a. Источник соединительных параметров (Aspire / HaDb)

`BootstrapServers` и SASL-креды приходят из шва **`IKafkaConnectionProvider`**; ветка
выбирается существующим переключателем `Database:Source` (общим с HA.Db):

| Режим | Источник | SASL | Hot-reload |
|---|---|---|---|
| `Aspire` (локальная разработка) | `ConnectionStrings:Kafka` → `KafkaOptions.BootstrapServers` (адаптер `ConfigurationKafkaConnectionProvider`) | нет — PLAINTEXT-дефолт Confluent | `IOptionsMonitor`-нотификации (как раньше) |
| `HaDb` (HA-стенд) | etcd-снапшот HA.Kafka: `endpoints` + `app_user`/`app_password` (контракт pg/arch/15 §5) через `DiscoveryKafkaConnectionProvider` | SASL_PLAINTEXT/PLAIN | событие `Updated` стора → `OnChange` провайдера |

Правила:

- `Kafka:Cluster` (обязателен в HaDb-режиме, напр. `"events"`) — имя кластера для заявки
  `AddKafkaCluster` и чтения снапшота; пусто → fail-fast при старте.
- **Fail-open**: валидного конфига нет (снапшот не собран, ключи неполны, etcd лежит) —
  `Current == null`: producer отклоняет `SendAsync` (`Result.Failed`), consumer откладывает
  построение Confluent-клиента до появления параметров. Старт приложения не роняется.
- `OnChange` провайдера стреляет только при изменении соединительных параметров
  (bootstrap/креды) — шум реестра топиков соединения не роняет. Ротация `app_password`
  (pg/arch/16 §5 H): producer-кэш замещается, consumer перезапускается с NEW-кредами.
- В HaDb-режиме `KafkaOptions.BootstrapServers` игнорируется (провайдер — единственный
  источник). При сосуществовании с HA.Db держите `HaKafka:MembersMode=Off` и одинаковые
  `HaDb:EtcdEndpoints`/`HaKafka:EtcdEndpoints` (один etcd на стенд).
- AdminClient строится от среза параметров на момент первого обращения и не
  перестраивается при их смене (документированное ограничение).
```

Выход: раздел в доке. Проверка: markdown-линк на 01.19 валиден.

- [ ] **Step 1b: Актуализировать существующие разделы 01.16 про BootstrapServers/KafkaOptions (спека §3.7)**

Вход: существующие разделы 01.16-kafka.md описывают `KafkaOptions.BootstrapServers` как единственный источник (§1 таблица «KafkaOptions | общий, Aspire», текст «`BootstrapServers` живёт только в `KafkaOptions`», упоминания в §2 Producer/§3 Consumer «BootstrapServers берётся из KafkaOptions»).

Действие: найти в файле все упоминания `BootstrapServers`/`KafkaOptions` и уточнить источник — искать grep'ом и править:

```bash
grep -n "BootstrapServers\|KafkaOptions" docs/01.16-kafka.md
```

Конкретные правки (формулировки — по фактическому контексту найденных мест):

1. §1, таблица слоя конфигурации, строка `KafkaOptions`: колонку «Откуда» изложить как
   «`[Config("Kafka")]` + `AddKafka`; **BootstrapServers — только Aspire-ветка** (`ConnectionStrings:Kafka`; в HaDb-режиме поле игнорируется — параметры из дискавери, см. §1a); `GroupId` (дефолт) — в обеих ветках».
2. §1, абзац «`BootstrapServers` **живёт только в `KafkaOptions`** — он общий...»: дополнить
   «...в Aspire-ветке; в HaDb-ветке источник — `IKafkaConnectionProvider` (§1a), а `KafkaOptions.BootstrapServers` игнорируется».
3. §2 (Producer) и §3 (Consumer), места вида «BootstrapServers берётся из `KafkaOptions`» /
   «кэш замещается ... bootstrap»: уточнить «из `IKafkaConnectionProvider` (Aspire — из `KafkaOptions`, HaDb — из etcd-дискавери; §1a)».
4. Места про hot-reload «при изменении TConfig или KafkaOptions» → «при изменении TConfig
   или соединительных параметров провайдера (Aspire: KafkaOptions; HaDb: снапшот дискавери — §1a)».

Выход: все упоминания источника параметров отражают двухветочную модель. Проверка: повторный `grep -n "BootstrapServers" docs/01.16-kafka.md` — каждое упоминание либо уточняет «только Aspire-ветка», либо ссылается на §1a; смысловых противоречий с новым разделом 1a нет.

- [ ] **Step 2: Раздел в 01.19-ha-kafka.md**

В конец файла добавить:

```markdown
## Интеграция с Infrastructure.App.Kafka (t10)

Клиент (`Infrastructure.App.Kafka`, Confluent) потребляет библиотеку напрямую:
`AddKafka` в HaDb-режиме (`Database:Source=HaDb`) регистрирует `AddHaKafka(...).
AddKafkaCluster(<Kafka:Cluster>)` и `DiscoveryKafkaConnectionProvider` —
`bootstrap.servers`/SASL-креды из снапшота, ротация `app_password` и смена
endpoints доставляются событием `Updated`. Aspire-режим — источник
`ConnectionStrings:Kafka` (PLAINTEXT). Детали и fail-open-семантика —
[01.16 Kafka §1a](01.16-kafka.md).
```

- [ ] **Step 3: Финальная проверка всего**

Проверка: `dotnet build src/PuzzleServer.Api.slnx` → 0 warnings; `dotnet test src/PuzzleServer.Api.slnx` → все зелёные (unit + integration, Docker запущен); `git status --short` чист после коммита.

- [ ] **Step 4: Коммит**

```bash
git add docs/01.16-kafka.md docs/01.19-ha-kafka.md
git commit -m "docs: kafka connection source (Aspire/HaDb seam, fail-open, rotation) + HA.Kafka integration note (t10 Ф5)"
```

---

## Соответствие задач спеке (self-review плана)

| Спека | Задачи |
|---|---|
| §3.1 шов + маппинг | Task 1 (модель), Task 2 (маппер), Task 3 (интерфейс) |
| §3.2 Configuration-провайдер (Aspire) | Task 3 |
| §3.3 Discovery-провайдер (HaDb) | Task 6 |
| §3.4 producer/consumer/change source/admin на шве, fail-open | Task 4 (producer+admin), Task 5 (consumer+waiter+change source) |
| §3.5 ветвление AddKafka | Task 7 |
| §3.6 appsettings | Task 7 (Kafka:Cluster, HaKafka-секция; MembersMode=Off — отклонение №2 в конце плана) |
| §3.7 документация | Task 10 |
| §4 ротация app_password (клиентская механика) | Task 6 (unit: OnChange), Task 8 (окно двух кредов), Task 9 (сценарий 3) |
| §5 Ф1–Ф5 | Task 1–5 (Ф1, с объединением «отложенного построения consumer» в Task 5), Task 6–7 (Ф2–Ф3), Task 8–9 (Ф4), Task 10 (Ф5) |
| §6 ограничения | Global Constraints + Task 4 (AdminClient не перестраивается), Task 7 (MembersMode=Off) |
| §7 тестирование | Task 1–7 (unit), Task 8–9 (интеграционные сценарии 1–5) |
| §8 критерии 1–11 | 1/2 — каждый таск (build+тесты), 3–5 — Task 7/9, 6 — Task 9 сценарий 3, 7 — Task 9 сценарии 1/5, 8 — Task 1/6, 9 — Tasks 4–7 (API не меняется; HA.Kafka не тронута), 10 — Task 10, 11 — Task 9 (читает только через HA.Kafka) |

Известные осознанные отклонения от буквы спеки:

1. **«Отложенное построение Confluent consumer'а» из Ф2 реализовано в Task 5 (Ф1)** —
   механика едина для обеих веток провайдера.
2. **`HaKafka:MembersMode=Off` при сосуществовании с HA.Db** (Task 7) — решение
   user-review Фазы 3, в spec не внесено намеренно (правка spec после его одобрения
   гоняла бы лишний каскад гейтов dev-flow): HaDb-режим впервые запускает HA.Db и
   HA.Kafka в одном процессе, оба регистрируют `IEtcdClient` (одно имя) и
   `EtcdEndpointRotation`, а `EtcdMembersMonitor` — дважды как hosted-сервис
   (двойной цикл member/list). Конфигурационное решение: монитор один (от HaDb),
   `HaKafka:MembersMode=Off`; общие `IEtcdClient`/ротация консистентны при совпадающих
   `HaDb:EtcdEndpoints`/`HaKafka:EtcdEndpoints` (один etcd на стенд — AGENTS pg).
   Ноль правок HA.Kafka — spec-критерий 9 соблюдён. Требование зафиксировано в
   документации (Task 10, раздел 1a).
3. **Фиксированные host-порты/зонды вместо `assignRandomHostPort`** (Task 8, спека
   §7 «динамические порты — assignRandomHostPort»): advertised CLIENT-listener обязан
   быть известен ДО старта kafka-контейнера (env фиксируется при создании), а
   фактический random-порт Testcontainers доступен только после — потому kafka-порт
   берётся рантайм-зондом 32500–32509 (допустимая альтернатива по AGENTS pg), а
   etcd-порт фиксирован 32495. Отдельная зона etcd от t05-фикстуры (32490) обязательна:
   xunit параллелит коллекции (t05-класс — default-коллекция, t10 — своя именованная),
   общий порт дал бы «port is already allocated» при полном прогоне (review Ф4-3);
   потому `KafkaEtcdFixture` параметризована портом (дефолт 32490 — t05 не меняется).
   Итоговое распределение зон: 32379–32381 (HA/Db cluster), 32490 (t05-etcd), 32495
   (t10-etcd), 32500–32509 (kafka) — всё вне зоны dev-станда pg (16xxx) и без
   пересечений между параллельными коллекциями. Смысл требования спеки (отсутствие
   коллизий со стендом и параллельными прогонами) соблюдён.
4. Smoke-тест фикстуры (Task 8 Step 2) удаляется перед коммитом.
