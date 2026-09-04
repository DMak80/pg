# t05-kafka-client-churn — план реализации (Фаза 3 dev-flow)

> **Для агентных исполнителей:** ОБЯЗАТЕЛЬНЫЙ SUB-SKILL:
> superpowers:subagent-driven-development (рекомендуется) или
> superpowers:executing-plans — выполнять по задачам; шаги помечены
> чекбоксами (`- [ ]`).

**Цель:** недоступный кластер Kafka не жжёт CPU/лог/потоки KafkaWorker
(кэш AdminClient + пины librdkafka + backoff 15→60→300 c), а утерянный
portalloc самолечится лестницей E9 (arch/17) вместо вечного тупика.

**Архитектура:** кэш per `(bootstrap,user,password)` встраивается в
`KafkaAdminClientFactory` (sharable, Dispose адаптера — no-op, владение у
кэша; процессы не меняются); трекер `KafkaClusterBackoff` гейтит kafka-пробы
надзора и kafka-шаги Active-ветки; `PortAllocHealer` — три ветки источников
адреса (portalloc → инспекция контейнера → новая аллокация + RMW endpoints)
на новой грани драйвера `InspectNodeEndpointAsync`.

**Стек:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`),
Confluent.Kafka (только в адаптере, seam `IKafkaAdminClient`), xunit.v3 +
FluentAssertions, Testcontainers (интеграция).

**Спека:** `docs/superpowers/2026-09-04-t05-kafka-client-churn/spec.md`
(исполнитель читает её вместе с планом; аргументация — оттуда).

## Глобальные ограничения (действуют на каждую задачу)

- Язык: комментарии/документация — русские, идентификаторы — английские.
- `TreatWarningsAsErrors=true` — ни одного warning.
- Порты в тестах — только динамические: зонд свободных портов рантаймом
  (паттерн `FreePortWindow`, `KafkaProbeClosedPortsTests` t11); литералов
  вида `:16000` в тестах и expects НЕТ; диапазоны не пересекаются с
  dev-стендом (15000–17000).
- `BrokerBootSec` в тестовых фикстурах ≤ 100 с.
- Мерж-гейт задачи: E2E-маркер `Scale_AddEmptyShard` на свежем Release +
  полный E2eFixture (трогаем provisioning/portalloc) — Task 8.
- arch/17 не переопределяем (E9 уже канон), arch/15 не меняем (Task 8
  фиксирует это явно — правки только arch/16).
- Коммит после каждой задачи; roadmap-тег `t05-kafka-client-churn` удаляется
  из `arch/roadmap/kafkaworker.md` тем же коммитом, что и слade кода (Task 8).
- Worktree: `/Users/demakaev/ZCodeProject/worktrees/feat-t05-kafka-client-churn`
  (все команды — из его корня).

Соглашения имён (используются между задачами):

- `KafkaAdminClientFactory(TimeSpan requestTimeout, ILogger<...>? logger, TimeProvider? clock)`
  — кэширующая, `IKafkaAdminClientFactory`, `IDisposable`;
  `public int CreatedClients { get; }`; `internal static AdminClientConfig BaseAdminConfig(string bootstrap, string user, string password)`.
- `KafkaClusterBackoff(TimeProvider clock)`: `bool IsBlocked(string cluster)`,
  `void RecordFailure(string cluster, string error)`, `void RecordSuccess(string cluster)`,
  `void ForgetMissing(IReadOnlySet<string> liveClusters)`,
  `internal static TimeSpan BackoffAfter(int consecutiveFailures)`.
- `IClusterDriver.InspectNodeEndpointAsync(string cluster, string nodeName, CancellationToken ct)`
  → `Task<Result<NodeEndpointInspection?>>`;
  `public sealed record NodeEndpointInspection(string Host, int ClientHostPort, string? AdvertisedClient)`.
- `PortAllocHealer` (полный ctor — в Task 5).

---

### Task 1: Кэширующая фабрика `KafkaAdminClientFactory` (пины librdkafka, unhealthy-инвалидация, вытеснение, sharable-контракт)

Закрывает spec §3.1 (пункты roadmap 1–3). Процессы и фейки НЕ меняются.

**Files:**
- Modify: `src/KafkaWorker.Provisioning/Kafka/KafkaAdminClient.cs` (фабрика —
  переписать; адаптер — колбэк `onFailed` + `internal void DisposeNative()`)
- Modify: `src/KafkaWorker.Provisioning/Kafka/IKafkaAdminClient.cs` (док
  контракта фабрики — sharable/no-op dispose)
- Modify: `src/KafkaWorker.App/Program.cs:171-172` (DI: логгер+TimeProvider)
- Create: `src/tests/KafkaWorker.UnitTests/Kafka/KafkaAdminClientFactoryTests.cs`

**Interfaces:**
- Consumes: существующий `IKafkaAdminClient` (не меняется), `KafkaAdminClient`
  (адаптер, почти как сейчас).
- Produces: `KafkaAdminClientFactory` sharable-семантика;
  `BaseAdminConfig` (internal, InternalsVisibleTo юнитам уже есть в
  `KafkaWorker.Provisioning.csproj`); `public int CreatedClients`;
  `internal const int BackoffMs = 1000`, `BackoffMaxMs = 10000`;
  `internal static readonly TimeSpan IdleEvictAfter = TimeSpan.FromMinutes(10)`.

- [ ] **Шаг 1: падающий тест кэша (без натива — клиент ленивый)**

`src/tests/KafkaWorker.UnitTests/Kafka/KafkaAdminClientFactoryTests.cs`:

```csharp
using Confluent.Kafka;
using KafkaWorker.Provisioning.Kafka;
using KafkaWorker.UnitTests.Provisioning;

namespace KafkaWorker.UnitTests.Kafka;

// Кэш AdminClient'ов (t05, spec §3.1): sharable-фабрика per
// (bootstrap,user,password) — «клиент на тик» давал churn rd_kafka-инстансов
// и 100% ядра на лежащем кластере (инцидент as-kafkaworker 2026-09-04).
// Адаптер строит нативный клиент лениво (первая операция) — юниты кэша
// работают без сети.
public class KafkaAdminClientFactoryTests
{
    // AAA: reuse по ключу — тот же адаптер, счётчик не растёт.
    [Fact]
    public void Create_SameKey_ReturnsSameClient()
    {
        var factory = new KafkaAdminClientFactory(TimeSpan.FromSeconds(3));
        var first = factory.Create("h:9092", "app", "pw");
        var second = factory.Create("h:9092", "app", "pw");
        second.Should().BeSameAs(first);
        factory.CreatedClients.Should().Be(1);
    }

    // AAA: смена endpoints/кредов = другой ключ = другой клиент.
    [Fact]
    public void Create_DifferentCredentials_DifferentClient()
    {
        var factory = new KafkaAdminClientFactory(TimeSpan.FromSeconds(3));
        var a = factory.Create("h:9092", "app", "pw1");
        var b = factory.Create("h:9092", "app", "pw2");
        var c = factory.Create("h2:9092", "app", "pw1");
        b.Should().NotBeSameAs(a);
        c.Should().NotBeSameAs(a);
        factory.CreatedClients.Should().Be(3);
    }

    // AAA: пины librdkafka в конфиге (reconnect-шторм дефолтных 100 мс).
    [Fact]
    public void BaseAdminConfig_PinsLibrdkafkaBackoffs()
    {
        var config = KafkaAdminClientFactory.BaseAdminConfig("h:9092", "app", "pw");
        config.BootstrapServers.Should().Be("h:9092");
        config.RetryBackoffMs.Should().Be(1000);
        config.ReconnectBackoffMs.Should().Be(1000);
        config.ReconnectBackoffMaxMs.Should().Be(10000);
    }

    // AAA: вытеснение по неактивности (FixedTimeProvider — сдвиг на 11 мин).
    [Fact]
    public void Create_EvictsIdleEntries()
    {
        var clock = new FixedTimeProvider();
        var factory = new KafkaAdminClientFactory(TimeSpan.FromSeconds(3), clock: clock);
        var first = factory.Create("h:9092", "app", "pw");

        clock.Utc += KafkaAdminClientFactory.IdleEvictAfter + TimeSpan.FromMinutes(1);
        var second = factory.Create("h:9092", "app", "pw");

        second.Should().NotBeSameAs(first);
        factory.CreatedClients.Should().Be(2);
    }

    // AAA: активный ключ не вытесняется (LastUsed обновляется на Create).
    [Fact]
    public void Create_ActiveKey_NotEvicted()
    {
        var clock = new FixedTimeProvider();
        var factory = new KafkaAdminClientFactory(TimeSpan.FromSeconds(3), clock: clock);
        var first = factory.Create("h:9092", "app", "pw");

        clock.Utc += TimeSpan.FromMinutes(5);
        factory.Create("h:9092", "app", "pw");
        clock.Utc += TimeSpan.FromMinutes(5);
        var second = factory.Create("h:9092", "app", "pw");

        second.Should().BeSameAs(first);
        factory.CreatedClients.Should().Be(1);
    }

    // AAA: Failed операции помечает запись Unhealthy — следующий Create
    // отдаёт свежий клиент (internal NotifyFailed — тот же путь, что зовёт
    // RunAsync при исключении; сети/натива нет — клиент ленивый).
    [Fact]
    public void Create_AfterFailure_CreatesFreshClient()
    {
        var factory = new KafkaAdminClientFactory(TimeSpan.FromSeconds(3));
        var first = factory.Create("h:9092", "app", "pw");

        ((KafkaAdminClient)first).NotifyFailed();
        var second = factory.Create("h:9092", "app", "pw");

        second.Should().NotBeSameAs(first);
        factory.CreatedClients.Should().Be(2);
    }

    // AAA: отмена host'а не инвалидирует (IsHostCancellation — условие
    // пометки в RunAsync): OCE при отменённом токене — да; OCE без отмены
    // (чужой cancellation) и обычные исключения — нет.
    [Theory]
    [InlineData(true, true)]   // OCE + ct.IsCancellationRequested → не фейл-пометка
    [InlineData(false, false)] // OCE без отмены → фейл-пометка
    public void IsHostCancellation_Classifies(bool cancelled, bool expected)
    {
        using var cts = new CancellationTokenSource();
        if (cancelled)
            cts.Cancel();

        KafkaAdminClient.IsHostCancellation(new OperationCanceledException(), cts.Token)
            .Should().Be(expected);
        KafkaAdminClient.IsHostCancellation(new ApplicationException("down"), cts.Token)
            .Should().BeFalse();
    }

    // AAA: Dispose (shutdown) детерминированно вычищает кэш — повторный
    // Create строит новый клиент (CreatedClients +1), старый Disposeится.
    [Fact]
    public void Dispose_ThenCreate_BuildsFreshClient()
    {
        var factory = new KafkaAdminClientFactory(TimeSpan.FromSeconds(3));
        var first = factory.Create("h:9092", "app", "pw");

        factory.Dispose();
        var second = factory.Create("h:9092", "app", "pw");

        second.Should().NotBeSameAs(first);
        factory.CreatedClients.Should().Be(2);
    }
}
```

Примечание: `FixedTimeProvider` — проектный управляемый TimeProvider
(`src/tests/KafkaWorker.UnitTests/Provisioning/FixedTimeProvider.cs`,
`Utc { get; set; }`); новых пакетов не добавлять.

- [ ] **Шаг 2: прогнать тест — должен упасть (компиляция: нет ctor/членов)**

```bash
dotnet test src/tests/KafkaWorker.UnitTests -c Debug --filter 'FullyQualifiedName~KafkaAdminClientFactoryTests'
```

Ожидание: FAIL (ошибка компиляции: фабрика без clock/CreatedClients/BaseAdminConfig).

- [ ] **Шаг 3: реализация — кэширующая фабрика**

В `src/KafkaWorker.Provisioning/Kafka/KafkaAdminClient.cs` заменить фабрику
(адаптер ниже — двумя правками: `onFailed`-колбэк и `DisposeNative`):

```csharp
/// <summary>
/// Кэширующая фабрика AdminClient-адаптеров (t05, spec §3.1): один адаптер
/// per (bootstrap, user, password) вместо «клиент на тик» — churn
/// rd_kafka-инстансов и LongRunning-потоков на недоступном кластере съедал
/// ~100% ядра (инцидент as-kafkaworker 2026-09-04). Sharable: DisposeAsync
/// адаптера — no-op, владение у кэша; смена endpoints/кредов — другой ключ
/// (инвалидация по построению); Failed операции помечает запись Unhealthy —
/// следующий Create пересоздаёт, заменяемый Disposeится в фоне (Dispose
/// недоступного клиента может ждать poll-поток — не в горячем пути тика);
/// неактивные > IdleEvictAfter вытесняются при Create; остановка host'а —
/// детерминированный Dispose всех (IDisposable через DI).
/// </summary>
public sealed class KafkaAdminClientFactory(
    TimeSpan requestTimeout,
    ILogger<KafkaAdminClientFactory>? logger = null,
    TimeProvider? clock = null) : IKafkaAdminClientFactory, IDisposable
{
    // Профиль librdkafka (t05, паттерн t11): дефолтные 100 мс backoff при
    // мгновенном connection-refusal дают reconnect-шторм («3/3 brokers are
    // down» каждую секунду) — затыкаем до ≥1 c.
    internal const int BackoffMs = 1000;
    internal const int BackoffMaxMs = 10000;

    // Вытеснение неактивных ключей (кластер удалён / креды сменены) —
    // нативные потоки не копятся; без таймеров: чистка с каждым Create.
    internal static readonly TimeSpan IdleEvictAfter = TimeSpan.FromMinutes(10);

    private readonly TimeProvider _clock = clock ?? TimeProvider.System;
    private readonly object _gate = new();
    private readonly Dictionary<(string Bootstrap, string User, string Password), Entry> _entries = [];

    // Сколько адаптеров создано за жизнь фабрики — метрика churn'а
    // (public: интеграционные тесты строят на ней границы).
    public int CreatedClients { get; private set; }

    public IKafkaAdminClient Create(string bootstrap, string user, string password)
    {
        Entry entry;
        lock (_gate)
        {
            var now = _clock.GetUtcNow();
            EvictIdle(now);
            var key = (bootstrap, user, password);
            if (_entries.TryGetValue(key, out var current) && !current.Unhealthy)
            {
                current.LastUsedUtc = now;
                return current.Client;
            }

            CreatedClients++;
            Entry? marked = null;
            entry = new Entry(new KafkaAdminClient(
                bootstrap, user, password, requestTimeout, logger,
                onFailed: () => { if (marked is { } m) lock (_gate) m.Unhealthy = true; }))
            {
                LastUsedUtc = now,
            };
            marked = entry;
            _entries[key] = entry;
        }

        return entry.Client;
    }

    // Вытеснение неактивных: заменяемые Disposeимся в фоне (Dispose ждёт
    // poll-поток; не блокирует тик). Вызывается под _gate.
    private void EvictIdle(DateTimeOffset now)
    {
        List<Entry>? evicted = null;
        foreach (var (key, entry) in _entries)
        {
            if (now - entry.LastUsedUtc <= IdleEvictAfter)
                continue;
            evicted ??= [];
            evicted.Add(entry);
            _entries.Remove(key);
        }

        if (evicted is null)
            return;
        foreach (var entry in evicted)
            Task.Run(entry.Client.DisposeNative);
    }

    // Остановка host'а — детерминированно: клиенты с backoff-пинами не
    // штормуют, poll-потоки выходят быстро (паттерн t11).
    public void Dispose()
    {
        List<Entry> removed;
        lock (_gate)
        {
            removed = [.. _entries.Values];
            _entries.Clear();
        }

        foreach (var entry in removed)
            entry.Client.DisposeNative();
    }

    // Профиль конфига всех клиентов фабрики (internal — юнит-проверки пинов).
    internal static AdminClientConfig BaseAdminConfig(string bootstrap, string user, string password) => new()
    {
        BootstrapServers = bootstrap,
        SecurityProtocol = SecurityProtocol.SaslPlaintext,
        SaslMechanism = SaslMechanism.Plain,
        SaslUsername = user,
        SaslPassword = password,
        RetryBackoffMs = BackoffMs,
        ReconnectBackoffMs = BackoffMs,
        ReconnectBackoffMaxMs = BackoffMaxMs,
    };

    private sealed class Entry(KafkaAdminClient client)
    {
        public readonly KafkaAdminClient Client = client;
        public DateTimeOffset LastUsedUtc;
        public bool Unhealthy;
    }
}
```

(Инициализация `LastUsedUtc` — object initializer при создании Entry, как
в `Create` выше; отдельных заглушек нет.)

Адаптер `KafkaAdminClient` — правки:

1) ctor: `public sealed class KafkaAdminClient(
    string bootstrap, string user, string password, TimeSpan requestTimeout,
    ILogger? log = null, Action? onFailed = null)` — добавить два опциональных
   параметра (существующий вызов из тестов фикстуры
   `new KafkaAdminClient(bootstrap, user, password, requestTimeout)` остаётся валиден);
2) `EnsureClient()`: строить через `AdminClientBuilder(BaseAdminConfig(bootstrap, user, password))
   .SetLogHandler((_, m) => log?.LogDebug("rdkafka: {Message}", m.Message)).Build()`
   (BaseAdminConfig — static фабрики; дублирование конфига убрать);
3) `RunAsync`/`RunAsync<T>`: в `catch` помечать фейл:
   `catch (Exception e) { if (!IsHostCancellation(e, ct)) NotifyFailed(); return Result...Failed(...); }`
   плюс два internal-члена (путь пометки один — юниты зовут без сети):
   `internal void NotifyFailed() => onFailed?.Invoke();` и
   `internal static bool IsHostCancellation(Exception e, CancellationToken ct)
   => e is OperationCanceledException && ct.IsCancellationRequested;`
4) `DisposeAsync()` → `public ValueTask DisposeAsync() => ValueTask.CompletedTask;`
   (no-op: владение у кэша, spec §3.1) + `internal void DisposeNative() {
   _client?.Dispose(); _client = null; }`.

В `IKafkaAdminClient.cs` обновить doc `IKafkaAdminClientFactory` текстом из
spec §3.1 (sharable-контракт: Create — «получить клиент ключа», DisposeAsync
адаптера — no-op, реальный Dispose — вытеснение/остановка).

В `src/KafkaWorker.App/Program.cs` (регистрация фабрики):

```csharp
builder.Services.AddSingleton<IKafkaAdminClientFactory>(sp =>
    new KafkaAdminClientFactory(
        TimeSpan.FromSeconds(10),
        sp.GetRequiredService<ILogger<KafkaAdminClientFactory>>(),
        TimeProvider.System));
```

- [ ] **Шаг 4: прогнать новые тесты + все юниты процессов (контракт не изменился)**

```bash
dotnet test src/tests/KafkaWorker.UnitTests -c Debug --filter 'FullyQualifiedName~KafkaAdminClientFactoryTests'
dotnet test src/tests/KafkaWorker.UnitTests -c Debug
```

Ожидание: PASS (новые — зелёные; существующие — без правок зелёные:
`FakeKafkaAdminClient.DisposeAsync` уже no-op, `await using` в процессах
компилируется как раньше).

- [ ] **Шаг 5: коммит**

```bash
git add -A src/KafkaWorker.Provisioning/Kafka src/KafkaWorker.App/Program.cs src/tests/KafkaWorker.UnitTests/Kafka src/Directory.Packages.props
git commit -m 't05: кэширующая KafkaAdminClientFactory — sharable-кэш per (bootstrap,user,password), пины librdkafka >=1000 мс, rdkafka-лог на Debug, unhealthy-инвалидация, вытеснение неактивных, Dispose заменяемых в фоне (spec §3.1)'
```

---

### Task 2: Трекер `KafkaClusterBackoff` (шкала 15→60→300, сброс, чистка)

Закрывает spec §3.2 (компонент; гейты — Task 3). roadmap-пункт 4.

**Files:**
- Create: `src/KafkaWorker.Provisioning/Kafka/KafkaClusterBackoff.cs`
- Create: `src/tests/KafkaWorker.UnitTests/Kafka/KafkaClusterBackoffTests.cs`

**Interfaces:**
- Produces (для Task 3): `KafkaClusterBackoff(TimeProvider clock)` с
  `IsBlocked / RecordFailure / RecordSuccess / ForgetMissing /
  internal static TimeSpan BackoffAfter(int)` — сигнатуры в соглашениях.

- [ ] **Шаг 1: падающий тест трекера**

`src/tests/KafkaWorker.UnitTests/Kafka/KafkaClusterBackoffTests.cs`:

```csharp
using KafkaWorker.Provisioning.Kafka;
using KafkaWorker.UnitTests.Provisioning;

namespace KafkaWorker.UnitTests.Kafka;

// Backoff недоступного кластера (t05, spec §3.2): 15 → 60 → 300 с, сброс при
// успехе, чистка исчезнувших; лежащий кластер не долбится каждый тик
// (порт BackoffAfter KafkaProbeLoop t11).
public class KafkaClusterBackoffTests
{
    // AAA: шкала после N-й подряд неудачи.
    [Theory]
    [InlineData(1, 15)]
    [InlineData(2, 60)]
    [InlineData(3, 300)]
    [InlineData(7, 300)]
    public void BackoffAfter_Scale(int failures, int expectedSec)
        => KafkaClusterBackoff.BackoffAfter(failures).Should().Be(TimeSpan.FromSeconds(expectedSec));

    // AAA: окно блокирует IsBlocked, истечение — разблокирует.
    [Fact]
    public void RecordFailure_BlocksUntilWindowExpires()
    {
        var clock = new FixedTimeProvider();
        var backoff = new KafkaClusterBackoff(clock);

        backoff.RecordFailure("events", "down");
        backoff.IsBlocked("events").Should().BeTrue();

        clock.Utc += TimeSpan.FromSeconds(15);
        backoff.IsBlocked("events").Should().BeFalse();
    }

    // AAA: рост окна со 2-й неудачи (60 с) и 3-й (300 с).
    [Fact]
    public void RecordFailure_WindowGrows()
    {
        var clock = new FixedTimeProvider();
        var backoff = new KafkaClusterBackoff(clock);

        backoff.RecordFailure("events", "down");       // окно 15 c
        clock.Utc += TimeSpan.FromSeconds(15);
        backoff.RecordFailure("events", "down");       // окно 60 c
        clock.Utc += TimeSpan.FromSeconds(59);
        backoff.IsBlocked("events").Should().BeTrue();
        clock.Utc += TimeSpan.FromSeconds(1);
        backoff.IsBlocked("events").Should().BeFalse();

        backoff.RecordFailure("events", "down");       // окно 300 c
        clock.Utc += TimeSpan.FromSeconds(299);
        backoff.IsBlocked("events").Should().BeTrue();
    }

    // AAA: успех сбрасывает счётчик (следующая неудача — снова 15 c).
    [Fact]
    public void RecordSuccess_Resets()
    {
        var clock = new FixedTimeProvider();
        var backoff = new KafkaClusterBackoff(clock);

        backoff.RecordFailure("events", "down");
        backoff.RecordFailure("events", "down");
        clock.Utc += TimeSpan.FromSeconds(60);
        backoff.RecordSuccess("events");

        backoff.IsBlocked("events").Should().BeFalse();
        backoff.RecordFailure("events", "down");
        clock.Utc += TimeSpan.FromSeconds(15);
        backoff.IsBlocked("events").Should().BeFalse(); // снова первая ступень
    }

    // AAA: чистка исчезнувших И сброс счётчика: после ForgetMissing новая
    // неудача gone-кластера даёт окно 15 с (не 60/300 — счётчик не пережил
    // исчезновение из снапшота).
    [Fact]
    public void ForgetMissing_RemovesAndResetsGoneClusters()
    {
        var clock = new FixedTimeProvider();
        var backoff = new KafkaClusterBackoff(clock);
        backoff.RecordFailure("gone", "down");
        backoff.RecordFailure("gone", "down"); // счётчик 2 — окно 60 c
        backoff.RecordFailure("events", "down");

        backoff.ForgetMissing(new HashSet<string> { "events" });

        // gone исчез из живых и «вернулся»: первая неудача заново — 15 c.
        clock.Utc += TimeSpan.FromHours(1);
        backoff.IsBlocked("gone").Should().BeFalse("запись стёрта чисткой");
        backoff.RecordFailure("gone", "down");
        backoff.IsBlocked("gone").Should().BeTrue();
        clock.Utc += TimeSpan.FromSeconds(15);
        backoff.IsBlocked("gone").Should().BeFalse("окно первой ступени, не 60/300 — счётчик сброшен");

        // events (живой) untouched: окно от своей записи; истекло к этому времени.
        backoff.IsBlocked("events").Should().BeFalse();
    }
}
```

- [ ] **Шаг 2: прогнать — упасть (нет типа)**

```bash
dotnet test src/tests/KafkaWorker.UnitTests -c Debug --filter 'FullyQualifiedName~KafkaClusterBackoffTests'
```

Ожидание: FAIL (компиляция: `KafkaClusterBackoff` не существует).

- [ ] **Шаг 3: реализация**

`src/KafkaWorker.Provisioning/Kafka/KafkaClusterBackoff.cs`:

```csharp
namespace KafkaWorker.Provisioning.Kafka;

/// <summary>
/// Backoff недоступного кластера (t05, spec §3.2; паттерн KafkaProbeLoop t11):
/// сколько kafka-проб подряд упало и когда разрешена следующая. Писатели —
/// supervise-проба и коллектор метрик (первые kafka-контакты конвейера);
/// успех сбрасывает. Чистая политика частоты: НЕ состояние кластера — в etcd
/// ничего не пишет, brokers/&lt;b&gt;/state не трогает (флап ≠ смерть, arch/17 S7).
/// </summary>
public sealed class KafkaClusterBackoff(TimeProvider clock)
{
    private readonly object _gate = new();
    private readonly Dictionary<string, State> _clusters = [];

    public bool IsBlocked(string cluster)
    {
        lock (_gate)
            return _clusters.TryGetValue(cluster, out var s) && clock.GetUtcNow() < s.NextAttemptUtc;
    }

    public void RecordFailure(string cluster, string error)
    {
        lock (_gate)
        {
            var failures = (_clusters.TryGetValue(cluster, out var s) ? s.ConsecutiveFailures : 0) + 1;
            _clusters[cluster] = new State(failures, clock.GetUtcNow() + BackoffAfter(failures), error);
        }
    }

    public void RecordSuccess(string cluster)
    {
        lock (_gate)
        {
            _clusters.Remove(cluster);
        }
    }

    // Кластеры исчезли из снапшота — запись удаляется ЦЕЛИКОМ: и окно, и
    // счётчик (возвращение кластера начинает с первой ступени; t11).
    public void ForgetMissing(IReadOnlySet<string> liveClusters)
    {
        lock (_gate)
        {
            foreach (var gone in _clusters.Keys.Where(c => !liveClusters.Contains(c)).ToList())
                _clusters.Remove(gone);
        }
    }

    // Окно после N-й подряд неудачи: 1-я → 15 с (база kafka-циклов), 2-я →
    // 60 с, дальше 300 с (t11: 15 → 60 → 300, сброс при успехе).
    internal static TimeSpan BackoffAfter(int consecutiveFailures)
        => consecutiveFailures switch
        {
            <= 1 => TimeSpan.FromSeconds(15),
            2 => TimeSpan.FromSeconds(60),
            _ => TimeSpan.FromSeconds(300),
        };

    private sealed record State(int ConsecutiveFailures, DateTimeOffset NextAttemptUtc, string LastError);
}
```

- [ ] **Шаг 4: прогнать — зелёный**

```bash
dotnet test src/tests/KafkaWorker.UnitTests -c Debug --filter 'FullyQualifiedName~KafkaClusterBackoffTests'
```

Ожидание: PASS.

- [ ] **Шаг 5: коммит**

```bash
git add src/KafkaWorker.Provisioning/Kafka/KafkaClusterBackoff.cs src/tests/KafkaWorker.UnitTests/Kafka/KafkaClusterBackoffTests.cs
git commit -m 't05: KafkaClusterBackoff — экспоненциальный backoff недоступного кластера 15→60→300 с, сброс при успехе, чистка исчезнувших (spec §3.2)'
```

---

### Task 3: Гейты backoff — supervise-проба, Active-ветка, коллектор, ForgetMissing

Закрывает spec §3.2/§3.5 (интеграционные точки трекера). roadmap-пункт 4.

**Files:**
- Modify: `src/KafkaWorker.Provisioning/Processes/NodeSupervisor.cs` (ctor:
  +`KafkaClusterBackoff backoff`; `DescribeAliveAsync` — гейт)
- Modify: `src/KafkaWorker.App/Loops/KafkaClusterProcesses.cs` (ctor:
  +`KafkaClusterBackoff backoff`; гейт kafka-шагов в `ActiveAsync`)
- Modify: `src/KafkaWorker.App/KafkaMetricsCollector.cs` (ctor: +backoff;
  skip в `CollectOnceAsync`)
- Modify: `src/KafkaWorker.App/Loops/ReconcileLoop.cs` (ctor: +backoff;
  `ForgetMissing` в `TickAsync`)
- Modify: `src/KafkaWorker.App/Program.cs` (DI-регистрация
  `KafkaClusterBackoff` + прокидывание в 4 компонента)
- Modify: `src/tests/KafkaWorker.UnitTests/Provisioning/NodeSupervisorTests.cs`
  (FakeAdminFactory — счётчик Create; тест гейта)
- Modify: `src/tests/KafkaWorker.UnitTests/App/KafkaMetricsCollectorTests.cs`
  (файл УЖЕ существует — 6 тестов t04; в существующий sealed-класс добавить
  новый `[Fact]` + локальный `CountingAdminFactory`; класс НЕ дублировать)

**Interfaces:**
- Consumes: `KafkaClusterBackoff` (Task 2), `IKafkaAdminClientFactory`.
- Produces: поведение — «окно активно → kafka-контакт кластера не
  выполняется»; гейты supervise/коллектора — юниты здесь, гейт
  ActiveAsync — интеграционно в Task 6 Шаг 1б (`KafkaActiveGateTests`:
  шов не разводим — у `KafkaClusterProcesses` из 10 зависимостей
  интерфейс только `IClusterConfigConverger`; 9 интерфейсов ради одного
  if — отказ, ревью F1).

- [ ] **Шаг 1: падающие юнит-тесты гейтов**

В `NodeSupervisorTests.cs` расширить локальный `FakeAdminFactory` счётчиком
(класс уже есть в файле, строки ~75–78):

```csharp
private sealed class FakeAdminFactory(FakeKafkaAdminClient client) : IKafkaAdminClientFactory
{
    public int CreateCalls { get; private set; }

    public IKafkaAdminClient Create(string bootstrap, string user, string password)
    {
        CreateCalls++;
        return client;
    }
}
```

Хелпер `NewRig` (строки ~62–66) расширить параметром `KafkaClusterBackoff? backoff = null`
и передать в `new NodeSupervisor(...)` ИМЕНОВАННЫМ аргументом
`backoff: backoff` (опциональные параметры — только ПОСЛЕ обязательного
`options`, иначе CS1737: сигнатура — `NodeSupervisor(IEtcdGateway etcd,
string[] endpoints, IClusterDriver driver, ClaimStore claims, WorkJournal
journal, IKafkaAdminClientFactory adminFactory, ProvisioningOptions options,
KafkaClusterBackoff? backoff = null, PortAllocHealer? healer = null)`;
существующие позиционные вызовы `... adminFactory, options)` не меняются;
существующие параметры `NewRig` (admin и пр.) — по факту файла, backoff
добавляется следующим опциональным);
все существующие вызовы `NewRig()` — без правок (default null → внутри
`backoff ?? new KafkaClusterBackoff(TimeProvider.System)`; строгость не
теряется: гейт-тест передаёт явный экземпляр).

Новый тест (рядом с существующими):

```csharp
// AAA: backoff-окно активно → проба не ходит в сеть (0 Create), кластер
// трактуется как слепая проба (probeBlind) — надзор не падает, docker-часть
// работает (spec §3.2).
[Fact]
public async Task Run_BackoffWindow_ProbeSkippedWithoutClient()
{
    var admin = new FakeKafkaAdminClient { ClusterView = new KafkaClusterView([], null) };
    var backoff = new KafkaClusterBackoff(FixedTimeProviderUtc());
    backoff.RecordFailure("events", "connection refused");
    var rig = await NewRig(admin, backoff);

    var result = await rig.Supervisor.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

    result.IsSuccess.Should().BeTrue();
    rig.AdminFactory.CreateCalls.Should().Be(0);
}
```

`FixedTimeProviderUtc()` — просто `new FixedTimeProvider()`
(`src/tests/KafkaWorker.UnitTests/Provisioning/FixedTimeProvider.cs`):
неудача записана «сейчас» фиксированного времени → окно 15 c активно при
неизменном `Utc` — гейт срабатывает без продвижения времени.

Тест коллектора — ДОБАВИТЬ в существующий класс
`src/tests/KafkaWorker.UnitTests/App/KafkaMetricsCollectorTests.cs`
(ctor коллектора: `(int collectIntervalSec, Func<CancellationToken,
Task<Result<IReadOnlyList<KafkaClusterSnapshot>>>> clustersSnapshot,
IKafkaAdminClientFactory adminFactory, KafkaMetricsState state, TimeProvider
clock, ILogger<KafkaMetricsCollector> logger, KafkaClusterBackoff backoff)` —
backoff добавить ПОСЛЕДНИМ опциональным: `KafkaClusterBackoff? backoff = null`;
`ActiveCluster`-хелпер уже есть в файле у тестов t04 — переиспользовать или
взять ниже; `KafkaMetricsState` конструируется с Meter — паттерн t04-тестов,
строки ~144/170/196: `new KafkaMetricsState(new Meter("TestKafkaWorker"))`;
свежесть — `state.DebugSnapshot().LastSuccess`, internal через
InternalsVisibleTo):

```csharp
// AAA: skip кластера в backoff — фабрика не зовётся, тик успешен
// (MarkSuccess жив: skip ≠ фейл).
[Fact]
public async Task CollectOnce_BackoffCluster_SkippedWithoutClient()
{
    var factory = new CountingAdminFactory();
    var backoff = new KafkaClusterBackoff(new KafkaWorker.UnitTests.Provisioning.FixedTimeProvider());
    backoff.RecordFailure("events", "down");
    var state = new KafkaMetricsState(new System.Diagnostics.Metrics.Meter("TestKafkaWorker"));
    var collector = new KafkaMetricsCollector(
        30, _ => Task.FromResult(Result<IReadOnlyList<KafkaClusterSnapshot>>.Success([ActiveCluster("events")])),
        factory, state, TimeProvider.System, NullLogger<KafkaMetricsCollector>.Instance, backoff);

    await collector.CollectOnceAsync(CancellationToken.None);

    factory.CreateCalls.Should().Be(0);
    state.DebugSnapshot().LastSuccess.Should().NotBeNull("skip — не фейл тика");
}

private sealed class CountingAdminFactory : IKafkaAdminClientFactory
{
    public int CreateCalls { get; private set; }

    public IKafkaAdminClient Create(string bootstrap, string user, string password)
    {
        CreateCalls++;
        throw new ApplicationException("клиент не должен создаваться: кластер в backoff");
    }
}
```

(`ActiveCluster(name)` — существующий хелпер t04-тестов в том же файле:
Active-кластер с endpoints `h:9092` и кредами; если сигнатура хелпера
отличается — сверить по файлу и подставить фактическую; других «сверить по
месту» точек в задаче нет.)

- [ ] **Шаг 2: прогнать — упасть (нет ctor-параметров/гейтов)**

```bash
dotnet test src/tests/KafkaWorker.UnitTests -c Debug --filter 'FullyQualifiedName~NodeSupervisorTests|FullyQualifiedName~KafkaMetricsCollectorTests'
```

Ожидание: FAIL (компиляция).

- [ ] **Шаг 3: реализация гейтов**

1. `NodeSupervisor`: ctor-параметр `KafkaClusterBackoff? backoff = null`
   ПОСЛЕ обязательного `ProvisioningOptions options` (опциональные перед
   обязательным — CS1737; healer добавится в Task 5 тем же хвостом):
   `NodeSupervisor(..., IKafkaAdminClientFactory adminFactory,
   ProvisioningOptions options, KafkaClusterBackoff? backoff = null)` →
   поле `_backoff = backoff ?? new KafkaClusterBackoff(TimeProvider.System)`;
   `DescribeAliveAsync` — в начало (после null-гейта дискавери-полей):

```csharp
// Backoff недоступного кластера (t05, spec §3.2): окно активно — проба не
// ходит в сеть (слепая проба без клиента; бюджет молчания не стартует,
// unreachable-трек заморожен — флап ≠ смерть). Фейл — растит окно, успех —
// сбрасывает (надзор — первый kafka-контакт конвейера).
if (_backoff.IsBlocked(snap.Cluster))
    return Result<HashSet<int>?>.Success(null);

await using var admin = adminFactory.Create(snap.Endpoints, snap.AppUser, snap.AppPassword);
var view = await admin.DescribeClusterAsync(ct);
if (!view.IsSuccess)
{
    _backoff.RecordFailure(snap.Cluster, view.Error!.Message);
    return Result<HashSet<int>?>.Success(null); // кластер целиком недоступен
}

_backoff.RecordSuccess(snap.Cluster);
return Result<HashSet<int>?>.Success(view.Value.Brokers.Select(b => b.Id).ToHashSet());
```

2. `KafkaClusterProcesses`: ctor-параметр `KafkaClusterBackoff backoff`;
   в `ActiveAsync` после `supervisor.RunAsync` (успех) и перед converger:

```csharp
// Backoff недоступного кластера (t05, spec §3.2): docker-часть надзора
// отработала; kafka-шаги E–J/D пропускаются до истечения окна — лежащий
// кластер не долбится каждые 5–15 с. Тик — успех (не ошибка).
if (backoff.IsBlocked(snap.Cluster))
    return Result.Success();
```

3. `KafkaMetricsCollector`: ctor-параметр `KafkaClusterBackoff? backoff = null`
   (после logger), поле; в `CollectOnceAsync` цикл по кластерам — первым
   делом после дискавери-гейта:

```csharp
// Кластер в backoff-окне — сбор пропускается без kafka-контакта (skip ≠
// фейл тика); окно пишет supervise-проба, сюда приходит уже готовым.
if (_backoff.IsBlocked(cluster))
    continue;
```

   и в `TryCollectClusterAsync`: фейл сбора (каждый `return false` после
   неудачной kafka-операции — обернуть: в конце catch/финальные возвраты)
   → `_backoff.RecordFailure(cluster, ...)`; полный успех перед `return
   true` → `_backoff.RecordSuccess(cluster)`. Минимальная правка: в начале
   метода локальный `bool failed = false`; присваивать true в каждой ветке
   `return false` (или refactor: обёртка `var ok = await CollectCore...`).
   Исполнителю: рефакторить метод на `CollectClusterCoreAsync` +
   обёртку, фиксирующую RecordFailure/RecordSuccess — диф читаемый.

4. `ReconcileLoop`: ctor-параметр `KafkaClusterBackoff backoff`; в
   `TickAsync` после `parsed` (успешный снапшот):

```csharp
// Кластеры исчезли из снапшота — backoff-состояние не копится (t11).
_backoff.ForgetMissing(parsed.Value.Select(c => c.Cluster).ToHashSet());
```

5. `Program.cs`: `builder.Services.AddSingleton(sp => new
   KafkaClusterBackoff(sp.GetRequiredService<TimeProvider>()));` (рядом с
   фабрикой; `TimeProvider` уже в DI — используется reassigner'ом, сверить
   регистрацию `sp.GetRequiredService<TimeProvider>()` в Program.cs и
   использовать тот же источник) + прокинуть в `NodeSupervisor(...)`,
   `KafkaClusterProcesses`, `KafkaMetricsCollector`, `ReconcileLoop` в их
   construction-сайтах Program.cs (строки ~211–218, ~280+, где
   регистрируются KafkaClusterProcesses/ReconcileLoop/коллектор — найти по
   `AddHostedService`/`AddSingleton<IKafkaClusterProcesses>`).

- [ ] **Шаг 4: прогнать юниты целиком**

```bash
dotnet test src/tests/KafkaWorker.UnitTests -c Debug
```

Ожидание: PASS (новые гейт-тесты + все существующие; Active-ветка
проверяется интеграционно в Task 6).

- [ ] **Шаг 5: коммит**

```bash
git add -A src/KafkaWorker.Provisioning src/KafkaWorker.App src/tests/KafkaWorker.UnitTests
git commit -m 't05: гейты KafkaClusterBackoff — supervise-проба/Active-ветка/коллектор пропускают kafka-контакт в окне, ForgetMissing в ReconcileLoop (spec §3.2, §3.5)'
```

---

### Task 4: Грань инспекции `InspectNodeEndpointAsync` (engine + драйверы)

Закрывает spec §3.4. Полезна только с Task 5; тестируется интеграционно в
Task 6 (юнитов на DockerEngine нет — прецедент: движок покрыт только
интеграционными прогонами).

**Files:**
- Modify: `src/KafkaWorker.Docker/Engine/IDockerEngine.cs` (метод + record)
- Modify: `src/KafkaWorker.Docker/Engine/DockerEngine.cs` (plain + swarm)
- Modify: `src/KafkaWorker.Docker/Drivers/ClusterDriver.cs` (интерфейс
  `IClusterDriver` + оба драйвера)
- Modify: `src/tests/KafkaWorker.UnitTests/Provisioning/Fakes.cs`
  (`FakeKafkaDriver` — реализация нового метода + конфигурируемая инспекция)

**Interfaces:**
- Produces (для Task 5):

```csharp
// IClusterDriver
Task<Result<NodeEndpointInspection?>> InspectNodeEndpointAsync(
    string cluster, string nodeName, CancellationToken ct);

// Model (в ClusterDriver.cs, рядом с KafkaNodeSpec)
// Инспекция размещения брокера (E9-реконструкция portalloc): Host — хост
// размещения, ClientHostPort — published host-порт CLIENT (9094),
// AdvertisedClient — клиентская пара из env KAFKA_ADVERTISED_LISTENERS
// (контрольная сверка; null — источник недоступен, swarm).
public sealed record NodeEndpointInspection(string Host, int ClientHostPort, string? AdvertisedClient);

// IDockerEngine (тонкая грань движка; null = объекта нет)
Task<Result<DockerNodeEndpoint?>> InspectNodeEndpointAsync(string name, CancellationToken ct);
public sealed record DockerNodeEndpoint(int ClientHostPort, string? AdvertisedClient);
```

- [ ] **Шаг 1: `IDockerEngine` + `DockerEngine`**

`DockerEngine.InspectNodeEndpointAsync` (plain; по образцу
`InspectContainerResourcesAsync`, строки ~182–200 — тот же GET):

```csharp
// Инспекция endpoint'а контейнера (t05 E9): published host-порт CLIENT-порта
// 9094 из HostConfig.PortBindings + клиентская пара из env
// KAFKA_ADVERTISED_LISTENERS (контроль). 404 → null (объекта нет).
public async Task<Result<DockerNodeEndpoint?>> InspectNodeEndpointAsync(string name, CancellationToken ct)
    => await Result<DockerNodeEndpoint?>.FromAsync(async () =>
    {
        try
        {
            var body = await GetAsync<JsonElement>(
                $"/containers/{Uri.EscapeDataString(name)}/json", ct);
            if (body.ValueKind == JsonValueKind.Undefined)
                return null;

            var clientPort = ReadClientHostPort(body.GetProperty("HostConfig"));
            var advertised = ReadAdvertisedClient(body.GetProperty("Config"));
            return clientPort is { } port
                ? new DockerNodeEndpoint(port, advertised)
                : null; // привязки 9094 нет — endpoint-факта нет
        }
        catch (DockerHttpException e) when (e.StatusCode == 404)
        {
            return null;
        }
    });

// "9094/tcp" → первый HostPort (int).
private static int? ReadClientHostPort(JsonElement hostConfig)
{
    if (!hostConfig.TryGetProperty("PortBindings", out var bindings)
        || bindings.ValueKind != JsonValueKind.Object
        || !bindings.TryGetProperty("9094/tcp", out var slot)
        || slot.ValueKind != JsonValueKind.Array
        || slot.GetArrayLength() == 0)
        return null;

    var first = slot[0];
    return first.TryGetProperty("HostPort", out var hp)
        && int.TryParse(hp.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var port)
            ? port
            : null;
}

// env KAFKA_ADVERTISED_LISTENERS=... → сегмент CLIENT://host:port.
private static string? ReadAdvertisedClient(JsonElement config)
{
    if (!config.TryGetProperty("Env", out var env) || env.ValueKind != JsonValueKind.Array)
        return null;
    var line = env.EnumerateArray()
        .Select(e => e.GetString())
        .FirstOrDefault(s => s?.StartsWith("KAFKA_ADVERTISED_LISTENERS=", StringComparison.Ordinal) == true);
    if (line is null)
        return null;
    var value = line["KAFKA_ADVERTISED_LISTENERS=".Length..];
    var segment = value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
        .FirstOrDefault(p => p.StartsWith("CLIENT://", StringComparison.Ordinal));
    return segment; // "CLIENT://host:port" либо null
}
```

Swarm-ветка того же метода (после plain-инспекции, если контейнер не
найден): running-таск сервиса по `ListTasksAsync(name)` (record `DockerTask`
уже несёт `Host` и `PublishedPort`, IDockerEngine.cs:80–82) — порт из таска,
env — null (шаблон сервиса не читаем, сверка plain-only):

```csharp
// swarm: контейнер инспектируется на ноде таска — берём published-порт
// running-таска (env шаблона не читаем: сверка advertised — plain-only).
var tasks = await ListTasksAsync(name, ct);
if (!tasks.IsSuccess)
    throw tasks.Error!;
var running = tasks.Value.FirstOrDefault(t => t.State == "running" && t.PublishedPort > 0);
if (running is null)
    return null; // сервиса/таска нет — факта нет
return new DockerNodeEndpoint(running.PublishedPort!.Value, null);
```

Исполнителю: сверить фактическую структуру `DockerEngine` (метод `GetAsync<T>`,
`DockerHttpException`, `ListTasksAsync` — сигнатуры по соседним методам
~182–207 и использованию в ClusterDriver.cs:401) и вписать код в те же
паттерны; plain и swarm объединить в один метод: сначала plain-GET (404 —
не ошибка), затем swarm-фолбэк (движок один на endpoint: swarm-метод
выполняется только если plain не нашёл контейнер).

- [ ] **Шаг 2: `IClusterDriver` + драйверы**

Интерфейс — метод из «Interfaces» (doc-комментарий: «инспекция размещения
брокера для E9-реконструкции portalloc (t05): null = docker-объекта нет —
положительное свидетельство смерти, arch/17 S7; ошибка инспекта → Failed —
надзор не решает вслепую»).

`PlainClusterDriver` (перебор хостов — симметрия `NodeResourcesAsync`,
~260–273):

```csharp
// E9-реконструкция (t05): перебор хостов — первый, где контейнер есть,
// отдаёт host-порт + host-аллиас этого движка.
public async Task<Result<NodeEndpointInspection?>> InspectNodeEndpointAsync(
    string cluster, string nodeName, CancellationToken ct)
{
    var name = NodeName(cluster, nodeName);
    foreach (var (host, engine) in _engines)
    {
        var endpoint = await engine.InspectNodeEndpointAsync(name, ct);
        if (!endpoint.IsSuccess)
            return Result<NodeEndpointInspection?>.Failed(endpoint.Error!);
        if (endpoint.Value is { } found)
            return Result<NodeEndpointInspection?>.Success(
                new NodeEndpointInspection(host, found.ClientHostPort, found.AdvertisedClient));
    }

    return Result<NodeEndpointInspection?>.Success(null);
}
```

`SwarmClusterDriver` (manager-движок; Host — хост running-таска):

```csharp
public async Task<Result<NodeEndpointInspection?>> InspectNodeEndpointAsync(
    string cluster, string nodeName, CancellationToken ct)
{
    var endpoint = await _engine.InspectNodeEndpointAsync(
        PlainClusterDriver.NodeName(cluster, nodeName), ct);
    if (!endpoint.IsSuccess)
        return Result<NodeEndpointInspection?>.Failed(endpoint.Error!);
    if (endpoint.Value is null)
        return Result<NodeEndpointInspection?>.Success(null);

    var tasks = await _engine.ListTasksAsync(PlainClusterDriver.NodeName(cluster, nodeName), ct);
    if (!tasks.IsSuccess)
        return Result<NodeEndpointInspection?>.Failed(tasks.Error!);
    var host = tasks.Value.FirstOrDefault(t => t.State == "running")?.Host;
    return host is null
        ? Result<NodeEndpointInspection?>.Success(null)
        : Result<NodeEndpointInspection?>.Success(
            new NodeEndpointInspection(host, endpoint.Value.ClientHostPort, endpoint.Value.AdvertisedClient));
}
```

(Если в Шаге 1 swarm-ветка движка уже вернула порт из таска — здесь Host
из того же таска; согласовать пары так, чтобы не было двойного
list-tasks-запроса: допустимо оставить только один источник истины —
движок; тогда драйвер просто мапит host. Исполнителю: выбрать один вариант
и выдержать его в обоих драйверах.)

- [ ] **Шаг 3: `FakeKafkaDriver` — реализация + конфигурация**

В `src/tests/KafkaWorker.UnitTests/Provisioning/Fakes.cs` (класс
`FakeKafkaDriver`, строки ~174+):

```csharp
// E9-инспекция (t05): сеется тестами; null по умолчанию — «контейнера нет»
// (положительное свидетельство смерти, S7). Инспекция broker<k> с контейнером
// отдаёт host "h1" и порт — сеёд siда portalloc-реконструкции.
public readonly Dictionary<string, NodeEndpointInspection> Endpoints = [];
public Func<string, Result<NodeEndpointInspection?>>? EndpointFaultByNode { get; set; }

public Task<Result<NodeEndpointInspection?>> InspectNodeEndpointAsync(
    string cluster, string nodeName, CancellationToken ct)
{
    if (EndpointFaultByNode is { } fault)
    {
        var failed = fault(nodeName);
        if (!failed.IsSuccess)
            return Task.FromResult(failed);
    }

    return Task.FromResult(Result<NodeEndpointInspection?>.Success(
        Endpoints.TryGetValue(nodeName, out var endpoint) ? endpoint : null));
}
```

- [ ] **Шаг 4: сборка + все юниты**

```bash
dotnet build src/PgWorker.slnx -c Debug
dotnet test src/tests/KafkaWorker.UnitTests -c Debug
```

Ожидание: PASS (грань компилируется, поведение покрыто фейком; сетевые
интеграции — Task 6).

- [ ] **Шаг 5: коммит**

```bash
git add -A src/KafkaWorker.Docker src/tests/KafkaWorker.UnitTests/Provisioning/Fakes.cs
git commit -m 't05: InspectNodeEndpointAsync — грань инспекции published-порта CLIENT (9094) + advertised env для E9-реконструкции portalloc (spec §3.4)'
```

---

### Task 5: `PortAllocHealer` — лестница E9 + вход в `NodeSupervisor`

Закрывает spec §3.3 (roadmap-пункт 5; arch/17 E9 — ссылка, не копия канона).

**Files:**
- Create: `src/KafkaWorker.Provisioning/Processes/PortAllocHealer.cs`
- Modify: `src/KafkaWorker.Provisioning/Processes/NodeSupervisor.cs`
  (вход лестницы после `ReadPortAllocAsync`; ctor +healer)
- Modify: `src/KafkaWorker.App/Program.cs` (DI healer → supervisor)
- Create: `src/tests/KafkaWorker.UnitTests/Provisioning/PortAllocHealerTests.cs`
- Modify: `src/tests/KafkaWorker.UnitTests/Provisioning/NodeSupervisorTests.cs`
  (интеграция лестницы в надзор — 1 сценарий: тупик исчез)

**Interfaces:**
- Consumes: `InspectNodeEndpointAsync` (Task 4), `PortAllocLock`,
  `PortAllocIndex`, `PlacementPlanner.Plan`, `PortAllocator.Allocate`,
  `WorkJournal.WriteAsync`, `IEtcdGateway` (Get/Txn/Put), `ProvisioningOptions`.
- Produces:

```csharp
// PortAllocHealer: ResolveAsync возвращает адрес брокера, восстанавливая
// portalloc по лестнице E9. Побочно (по веткам): put-if-absent/RMW
// /kafkaworker/portalloc/<C>, RMW /kafka/clusters/<C>/endpoints,
// пересоздание контейнера (только ветка 3), journal-фазы healing-portalloc /
// waiting-portalloc-lock / reconstructed / reallocated.
//
// Recreated=true — ветка 3 пересоздала контейнер (вызывающий supervise
// переводит брокера в PROVISIONING).
public sealed record HealedAddress(NodeAddress Address, bool Recreated);

public sealed class PortAllocHealer(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    PortAllocLock portLock,
    PortAllocIndex portAlloc,
    ProvisioningOptions options)
{
    // Лестница для ОДНОГО безадресного брокера (spec §3.3).
    // Успех: HealedAddress (адрес + признак пересоздания); PortLockBusyException
    // → вызывающий делает waiting-portalloc-lock, следующий тик.
    public Task<Result<HealedAddress>> ResolveAsync(
        KafkaClusterSnapshot snap, string broker,
        IReadOnlyDictionary<string, NodeAddress> addresses, CancellationToken ct);
}
```

- [ ] **Шаг 1: падающие юнит-тесты лестницы (3 ветки + S7 + клэйм-занят)**

`src/tests/KafkaWorker.UnitTests/Provisioning/PortAllocHealerTests.cs`.
Паттерн рига — по `NodeSupervisorTests.NewRig` (`FakeEtcd`, `FakeKafkaDriver`,
`ClaimStore`, `WorkJournal`, `PortAllocLock` на FakeEtcd — прецедент
`PortAllocLockTests.cs`); seeded-раскладка Active-кластера `events` с
broker1/broker2, portalloc ключ ОТСУТСТВУЕТ (утерян).

```csharp
// Лестница E9 (t05, spec §3.3): portalloc пуст при объявленных брокерах —
// тупик «не закреплён в portalloc» (инцидент as-kafkaworker 2026-09-04)
// заменён самолечением: инспекция живого контейнера либо новая аллокация
// под клэймом locks/portalloc + RMW endpoints.
public class PortAllocHealerTests
{
    // AAA: ветка 2 — контейнер жив: portalloc восстановлен инспекцией
    // (put-if-absent version==0), контейнер НЕ трогаем, адрес = inspected.
    [Fact]
    public async Task Resolve_ContainerAlive_ReconstructsPortAlloc()
    {
        var rig = await NewRig(); // portalloc ключа нет; инспекция засеяна:
        rig.Driver.Endpoints["broker1"] = new NodeEndpointInspection("h1", 21037, "CLIENT://host.docker.internal:21037");

        var resolved = await rig.Healer.ResolveAsync(rig.Snapshot, "broker1", rig.Addresses, CancellationToken.None);

        resolved.IsSuccess.Should().BeTrue();
        resolved.Value.Address.Should().Be(new NodeAddress("h1", 21037));
        resolved.Value.Recreated.Should().BeFalse("живой контейнер не трогаем");
        (await rig.PortAllocJson()).Should().Contain("21037");
        rig.Driver.Removed.Should().BeEmpty("живой контейнер не трогаем");
        rig.Driver.Ensured.Should().BeEmpty("пересоздания не было");
    }

    // AAA: ветка 3 — контейнера нет (S7): новая аллокация под клэймом,
    // контейнер пересоздан по новому адресу, endpoints RMW-обновлён.
    [Fact]
    public async Task Resolve_ContainerGone_ReallocatesAndRecreates()
    {
        var rig = await NewRig(); // инспекция null (нет записи), контейнеров нет

        var resolved = await rig.Healer.ResolveAsync(rig.Snapshot, "broker1", rig.Addresses, CancellationToken.None);

        resolved.IsSuccess.Should().BeTrue();
        resolved.Value.Recreated.Should().BeTrue("S7: контейнер пересоздан");
        var allocatedPort = resolved.Value.Address.ClientPort;
        allocatedPort.Should().BeInRange(rig.Options.PortFrom, rig.Options.PortTo);
        (await rig.PortAllocJson()).Should().Contain(allocatedPort.ToString());
        rig.Driver.Ensured.Should().ContainSingle(s => s.NodeName == "broker1" && s.ClientHostPort == allocatedPort);
        var endpoints = await rig.GetAsync("/kafka/clusters/events/endpoints");
        endpoints.Should().Contain(allocatedPort.ToString(), "endpoints RMW-обновлён (клиенты перечитают тиком)");
    }

    // AAA: ветка 1 — адрес уже в portalloc → без записей (ранний выход
    // гарантирует вызывающий; healer это тоже уважает).
    [Fact]
    public async Task Resolve_PinnedAddress_ReturnsWithoutWrites()
    {
        var rig = await NewRig(pinned: new NodeAddress("h1", 21010));

        var resolved = await rig.Healer.ResolveAsync(rig.Snapshot, "broker1", rig.Addresses, CancellationToken.None);

        resolved.Value.Address.Should().Be(new NodeAddress("h1", 21010));
        resolved.Value.Recreated.Should().BeFalse();
        rig.Driver.Ensured.Should().BeEmpty();
    }

    // AAA: ветка 2, проигрыш version==0 — сосед записал portalloc между
    // чтением и txn (гонка S5): OnTxnBeforeCompare сеет чужой ключ ДО
    // compare → txn NotExists проигрывает → re-read, адрес соседа = истина.
    [Fact]
    public async Task Resolve_ContainerAlive_TxnLostTakesForeignTruth()
    {
        var rig = await NewRig();
        rig.Driver.Endpoints["broker1"] = new NodeEndpointInspection("h1", 21037, "CLIENT://host.docker.internal:21037");
        rig.Etcd.OnTxnBeforeCompare = _ =>
        {
            // Сеём «соседа» до compare: ключ появился после нашего чтения.
            rig.Etcd.PutAsync(Ep, "/kafkaworker/portalloc/events",
                """{"broker1":{"host":"h1","client":21050}}""", null, CancellationToken.None).GetAwaiter().GetResult();
        };

        var resolved = await rig.Healer.ResolveAsync(rig.Snapshot, "broker1", rig.Addresses, CancellationToken.None);

        resolved.IsSuccess.Should().BeTrue();
        resolved.Value.Address.Should().Be(new NodeAddress("h1", 21050),
            "первый записавший — истина (S5): re-read перекрывает нашу инспекцию 21037");
        rig.Driver.Ensured.Should().BeEmpty();
    }

    // AAA: клэйм занят — без мутаций, PortLockBusyException наружу
    // (supervise → waiting-portalloc-lock, следующий тик).
    [Fact]
    public async Task Resolve_PortLockBusy_NoMutations()
    {
        var rig = await NewRig();
        await rig.HoldPortLockAsync(); // держим locks/portalloc чужим держателем

        var resolved = await rig.Healer.ResolveAsync(rig.Snapshot, "broker1", rig.Addresses, CancellationToken.None);

        resolved.IsSuccess.Should().BeFalse();
        resolved.Error.Should().BeOfType<PortLockBusyException>();
        (await rig.PortAllocJson()).Should().BeNull("никаких записей под чужим клэймом");
        rig.Driver.Ensured.Should().BeEmpty();
    }

    // AAA: ошибка инспекции (docker молчит) — никаких действий (S7:
    // «не можем проверить» ≠ «мёртв»), фейл тика.
    [Fact]
    public async Task Resolve_InspectionFails_NoActions()
    {
        var rig = await NewRig();
        rig.Driver.EndpointFaultByNode = _ => Result<NodeEndpointInspection?>.Failed(
            new ApplicationException("docker host unreachable"));

        var resolved = await rig.Healer.ResolveAsync(rig.Snapshot, "broker1", rig.Addresses, CancellationToken.None);

        resolved.IsSuccess.Should().BeFalse();
        (await rig.PortAllocJson()).Should().BeNull();
        rig.Driver.Ensured.Should().BeEmpty();
    }
}
```

Риг (`NewRig(pinned: NodeAddress? = null)`): FakeEtcd с сидом
`/kafka/clusters/events/config` (Active, brokers=1) + `brokers/broker1/state=RUNNING`
(+ `role=controller`) + `app_user`/`app_password`; при `pinned` —
`/kafkaworker/portalloc/events` seeded; `Addresses` = прочитанный portalloc
(пуст или pinned). `Options` = `new ProvisioningOptions(21000, 21100, 100,
90, "host.docker.internal", "apache/kafka:4.0.0")` — диапазон ВНЕ зоны
стенда (динамические порты юнитам не нужны — числа только в данных фейков).
`HoldPortLockAsync` — `new PortAllocLock([Ep], rig.Etcd, TimeProvider.System,
"other-instance").TryAcquireAsync(...)`.

- [ ] **Шаг 2: прогнать — упасть (нет PortAllocHealer)**

```bash
dotnet test src/tests/KafkaWorker.UnitTests -c Debug --filter 'FullyQualifiedName~PortAllocHealerTests'
```

Ожидание: FAIL (компиляция).

- [ ] **Шаг 3: реализация `PortAllocHealer`**

Скелет (полные алгоритмы — по тексту ниже; etcd-обёртки
Get/Put/Txn/WithFailover — скопировать паттерн из `ProvisioningProcess.cs`
строки 411–500 — `ReadPortAllocAsync`/`SerializePortAlloc` вынести в общий
helper или продублировать локально; выбор: продублировать локально
(процессы самодостаточны — прецедент EnsureNodeAsync в supervise, строка 209):

```csharp
// Лестница источников адреса при утере portalloc (E9, arch/17; t05 spec §3.3):
// 1) portalloc есть → адрес из журнала (advertise стабилен);
// 2) журнала нет, контейнер есть (положительная инспекция) → реконструкция
//    из docker inspect (published-порт + host) клэйм-txn version==0;
//    контейнер не трогаем — данные неприкосновенны;
// 3) нет ни журнала, ни контейнера → брокер мёртв по S7-свидетельству →
//    новая аллокация под клэймом locks/portalloc (S5/t90) + пересоздание +
//    RMW endpoints — клиенты перечитают дискавери тиком.
public sealed class PortAllocHealer(/* см. Interfaces */) : IAsyncDisposable
{
    private const string Op = "healing-portalloc";

    public async Task<Result<HealedAddress>> ResolveAsync(
        KafkaClusterSnapshot snap, string broker,
        IReadOnlyDictionary<string, NodeAddress> addresses, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        // Ветка 1: закрепление есть — advertise стабилен (rebuild по журналу).
        if (addresses.TryGetValue(broker, out var pinned))
            return Result<HealedAddress>.Success(new HealedAddress(pinned, Recreated: false));

        // Контейнер — до клэйма: положительная инспекция решает ветку.
        var inspection = await driver.InspectNodeEndpointAsync(cluster, broker, ct);
        if (!inspection.IsSuccess)
            return Result<HealedAddress>.Failed(inspection.Error!); // слепота — не лечим

        // journal-before-manipulations (spec §3.3 / arch/16 §5): фаза
        // ДО первого txn/EnsureNode — ветка известна после инспекции.
        var started = await journal.WriteAsync(cluster, Op, "started", claims.InstanceId, null, ct);
        if (!started.IsSuccess)
            return Result<HealedAddress>.Failed(started.Error!);

        if (inspection.Value is { } found)
            return await ReconstructAsync(snap, broker, found, ct);   // ветка 2

        return await ReallocateAsync(snap, broker, addresses, ct);    // ветка 3 (S7)
    }

    // Ветка 2: запись восстановленного закрепления put-if-absent (version==0)
    // под глобальным клэймом; проигрыш txn → re-read (первый записавший —
    // истина, S5). Контейнер НЕ трогаем. Контроль: клиентский порт advertised
    // env == published — расхождение journal-warning (канон — PortBindings).
    private async Task<Result<HealedAddress>> ReconstructAsync(
        KafkaClusterSnapshot snap, string broker, NodeEndpointInspection found, CancellationToken ct)
    {
        var cluster = snap.Cluster;
        var address = new NodeAddress(found.Host, found.ClientHostPort);

        var acquired = await portLock.TryAcquireAsync(ct);
        if (!acquired.IsSuccess)
            return Result<HealedAddress>.Failed(acquired.Error!);
        if (!acquired.Value)
            return Result<HealedAddress>.Failed(new PortLockBusyException());
        try
        {
            var key = PortAllocKey(cluster);
            var merged = new Dictionary<string, NodeAddress>(await ReadPortAllocAsync(cluster, ct));
            merged[broker] = address;
            var txn = await TxnAsync(
                TxnRequest.Of([TxnCompare.NotExists(key)], [new TxnOp.Put(key, SerializePortAlloc(merged), null)]), ct);
            if (!txn.IsSuccess)
                return Result<HealedAddress>.Failed(txn.Error!);
            if (!txn.Value.Succeeded)
            {
                // уже записал сосед — читаем его истину (S5) и дальше по ней
                var reread = await ReadPortAllocAsync(cluster, ct);
                if (!reread.IsSuccess)
                    return Result<HealedAddress>.Failed(reread.Error!);
                if (reread.Value.TryGetValue(broker, out var foreign))
                    address = foreign;
            }
        }
        finally
        {
            await portLock.ReleaseAsync();
        }

        if (found.AdvertisedClient is { } advertised
            && !advertised.EndsWith($":{found.ClientHostPort}", StringComparison.Ordinal))
            await journal.WriteAsync(cluster, Op, "reconstructed", claims.InstanceId,
                $"advertised {advertised} != published :{found.ClientHostPort} — канон PortBindings", ct);
        else
            await journal.WriteAsync(cluster, Op, "reconstructed", claims.InstanceId, null, ct);

        return Result<HealedAddress>.Success(new HealedAddress(address, Recreated: false));
    }

    // Ветка 3: новая аллокация (паттерн AddBrokerProcess.EnsurePortsAsync):
    // под клэймом busy = docker ∪ portalloc чужих ∪ свои закрепления →
    // PlacementPlanner+PortAllocator → RMW portalloc (mod_revision) →
    // EnsureNode (state=PROVISIONING пишет вызывающий supervise) →
    // RMW endpoints (mod_revision; put если ключа нет).
    private async Task<Result<HealedAddress>> ReallocateAsync(
        KafkaClusterSnapshot snap, string broker,
        IReadOnlyDictionary<string, NodeAddress> addresses, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        var acquired = await portLock.TryAcquireAsync(ct);
        if (!acquired.IsSuccess)
            return Result<HealedAddress>.Failed(acquired.Error!);
        if (!acquired.Value)
            return Result<HealedAddress>.Failed(new PortLockBusyException());
        try
        {
            var (revision, current) = await ReadPortAllocWithRevisionAsync(cluster, ct);
            var merged = new Dictionary<string, NodeAddress>(current);
            if (merged.ContainsKey(broker))
                return Result<HealedAddress>.Success(new HealedAddress(merged[broker], Recreated: false)); // сосед успел

            var hosts = await driver.GetHostsAsync(ct);
            if (!hosts.IsSuccess)
                return Result<HealedAddress>.Failed(hosts.Error!);
            var dockerBusy = await driver.GetBusyPortsAsync(ct);
            if (!dockerBusy.IsSuccess)
                return Result<HealedAddress>.Failed(dockerBusy.Error!);
            var foreign = await portAlloc.ReadBusyAsync(cluster, ct);
            if (!foreign.IsSuccess)
                return Result<HealedAddress>.Failed(foreign.Error!);

            var taken = new HashSet<(string Host, int Port)>(dockerBusy.Value);
            foreach (var p in foreign.Value)
                taken.Add(p);
            foreach (var addr in merged.Values)
                taken.Add((addr.Host, addr.ClientPort));

            var plan = PlacementPlanner.Plan([broker], hosts.Value);
            var allocated = PortAllocator.Allocate(plan, merged, taken, options.PortFrom, options.PortTo);
            if (!allocated.IsSuccess)
                return Result<HealedAddress>.Failed(allocated.Error!);
            foreach (var (node, addr) in allocated.Value)
                merged[node] = addr;

            // RMW portalloc под клэймом (compare mod_revision; проигрыш —
            // следующий тик перечитает и увидит чужую истину).
            var portTxn = await TxnAsync(TxnRequest.Of(
                [TxnCompare.ModRevisionEqual(PortAllocKey(cluster), revision)],
                [new TxnOp.Put(PortAllocKey(cluster), SerializePortAlloc(merged), null)]), ct);
            if (!portTxn.IsSuccess)
                return Result<HealedAddress>.Failed(portTxn.Error!);
            if (!portTxn.Value.Succeeded)
                return Result<HealedAddress>.Failed(new ApplicationException(
                    $"portalloc {cluster} изменился под клэймом — ретрай тиком"));
            var address = merged[broker];

            // Контейнер по новому адресу (env — как EnsureNodeAsync надзора).
            var ensured = await EnsureNodeAsync(snap, broker, address, ct);
            if (!ensured.IsSuccess)
                return ensured;

            // RMW endpoints: пересборка advertise-адресов всех брокеров из
            // восстановленного portalloc (AdvertisedClientHost ?? host:port).
            var endpointsError = await UpdateEndpointsAsync(snap, merged, ct);
            if (endpointsError is not null)
                return Result<HealedAddress>.Failed(endpointsError);

            await journal.WriteAsync(cluster, Op, "reallocated", claims.InstanceId, null, ct);
            return Result<HealedAddress>.Success(new HealedAddress(address, Recreated: true));
        }
        finally
        {
            await portLock.ReleaseAsync();
        }
    }

    // IAsyncDisposable → portLock.ReleaseAsync (страховка).
}
```

Вспомогательные (`EnsureNodeAsync` — копия `NodeSupervisor.EnsureNodeAsync`
строки 210–247: env NodeEnvBuilder, KafkaNodeSpec; `UpdateEndpointsAsync`:
read endpoints kv → строка `string.Join(",", merged.OrderBy(...).Select(b =>
$"{options.AdvertisedClientHost ?? addr.Host}:{addr.ClientPort}"))` →
ключа нет: Put; есть: txn ModRevisionEqual → Put (проигрыш — ApplicationException
ретрай тиком); `ReadPortAllocAsync/ReadPortAllocWithRevisionAsync/
SerializePortAlloc/TxnAsync/WithFailoverAsync` — по паттерну
ProvisioningProcess 411–500). `ModRevisionEqual(key, 0)` для отсутствующего
ключа — сверить семантику `TxnCompare.ModRevisionEqual` по использованию в
`AddBrokerProcess.EnsurePortsAsync` (там `current.Value.Revision ?? 0`,
строки ~155–160) и повторить её.

`PortLockBusyException` — существующий тип (ProvisioningProcess.cs:80).
Журнальный протокол лестницы (spec §3.3 journal-before-manipulations):
`op=healing-portalloc, phase=started` — в `ResolveAsync` ДО первого
txn/EnsureNode; итоговые фазы `reconstructed`/`reallocated` — после мутаций
(код веток выше); `waiting-portalloc-lock` — пишет вызывающий supervise при
`PortLockBusyException` (Шаг 4).

- [ ] **Шаг 4: вход лестницы в `NodeSupervisor.RunAsync`**

В `NodeSupervisor.RunAsync` сразу после `ReadPortAllocAsync` (строка ~49–52)
и до `ListNodeObjectsAsync`:

```csharp
// Лестница E9 (t05, spec §3.3): безадресные Supervisable-брокеры — до любых
// деструктивных действий (RecreateAsync сносит контейнер ДО EnsureNode —
// ветка «контейнер жив» из EnsureNode недостижима). Клэйм занят —
// waiting-portalloc-lock (InProgress), следующий тик.
var addresses = new Dictionary<string, NodeAddress>(pinned.Value);
foreach (var broker in Supervisable(snap).Where(b => !addresses.ContainsKey(b.Name)))
{
    var healed = await healer.ResolveAsync(snap, broker.Name, addresses, ct);
    if (!healed.IsSuccess)
    {
        if (healed.Error is PortLockBusyException)
            return await FinishWaitingPortLockAsync(cluster, ct);
        return Fail(cluster, healed.Error!, "healing-portalloc");
    }

    addresses[broker.Name] = healed.Value.Address;
    if (healed.Value.Recreated) // пересоздан в ветке 3 — state=PROVISIONING
    {
        var marked = await PutAsync(BrokerStateKey(cluster, broker.Name), "PROVISIONING", ct);
        if (!marked.IsSuccess)
            return Fail(cluster, marked.Error!, "mark-provisioning");
    }
}
```

`FinishWaitingPortLockAsync` —
`journal.WriteAsync(cluster, Op, "waiting-portalloc-lock", ...) →
Result.Success()` (InProgress-семантика supervise: тик без ошибки —
как в ProvisioningProcess K1).

Ниже по методу использовать `addresses` (локальную слитую копию) вместо
`addresses.Value` (3 точки: секция 1 `RecreateAsync(..., addresses, ...)` —
строки ~107–116; секция 2 — то же, строки ~126–164). `EnsureNodeAsync`-Fail
«не закреплён» (строки 215–217) остаётся страховкой.

Ctor (ревью F3: опциональные параметры — только ПОСЛЕ обязательного
`options`, иначе CS1737; та же сигнатура зафиксирована в Task 3):
`NodeSupervisor(IEtcdGateway etcd, string[] endpoints, IClusterDriver
driver, ClaimStore claims, WorkJournal journal, IKafkaAdminClientFactory
adminFactory, ProvisioningOptions options, KafkaClusterBackoff? backoff =
null, PortAllocHealer? healer = null)` — существующие позиционные вызовы
`... adminFactory, options)` не меняются; новые аргументы передаются
ИМЕНОВАННЫМИ (`backoff:`, `healer:`); при `healer is null` лестница
пропускается (строгий режим «без healer — прежний Fail» НЕ возвращаем:
healer null только в легаси-вызовах тестов без лестницы; Program.cs всегда
передаёт). Юнит-тесты надзора, где portalloc пуст — проверить актуальность:
если такой тест существует и ожидает Fail — обновить под лестницу (см.
Шаг 5).

Program.cs:

```csharp
builder.Services.AddSingleton(sp => new PortAllocHealer(
    sp.GetRequiredService<IEtcdGateway>(), opts.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<PortAllocLock>(),
    sp.GetRequiredService<PortAllocIndex>(),
    ToProvisioningOptions(opts)));
// NodeSupervisor: прокинуть ИМЕНОВАННЫМИ аргументами после options
// (опциональные после обязательного — CS1737):
//   new NodeSupervisor(etcd, endpoints, driver, claims, journal,
//       adminFactory, ToProvisioningOptions(opts),
//       backoff: sp.GetRequiredService<KafkaClusterBackoff>(),
//       healer: sp.GetRequiredService<PortAllocHealer>());
```

- [ ] **Шаг 5: тест надзора «тупик исчез» + прогон юнитов**

Добавить в `NodeSupervisorTests.cs`:

```csharp
// AAA: клэйм занят — тик надзора УСПЕШЕН (InProgress), journal-фаза
// waiting-portalloc-lock, никаких мутаций — следующий тик повторит.
[Fact]
public async Task Run_PortLockBusy_WaitsWithoutError()
{
    var rig = await NewRig(healer: true);
    rig.Driver.NodeObjects.Remove("kfw-events-broker1"); // контейнер снесён
    await rig.HoldPortLockAsync();                        // клэйм держит сосед

    var result = await rig.Supervisor.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

    result.IsSuccess.Should().BeTrue("waiting-portalloc-lock — InProgress, не ошибка тика");
    (await rig.JournalPhaseAsync()).Should().Be("waiting-portalloc-lock");
    rig.Driver.Ensured.Should().BeEmpty("под чужим клэймом — никаких действий");
}

// AAA: portalloc утерян при объявленных брокерах (инцидент 2026-09-04) —
// надзор самолечится (E9): контейнер снесен + portalloc пуст → новая
// аллокация + пересоздание + PROVISIONING (вечного «не закреплён» нет).
[Fact]
public async Task Run_LostPortAlloc_HealsInsteadOfDeadlock()
{
    var rig = await NewRig(healer: true); // риг с healer'ом; портalloc ключ не сидится
    rig.Driver.NodeObjects.Remove("kfw-events-broker1"); // контейнер снесён
    rig.Driver.Endpoints.Clear();                         // инспекции нет → S7-ветка

    var result = await rig.Supervisor.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

    result.IsSuccess.Should().BeTrue();
    rig.Driver.Ensured.Should().ContainSingle(s => s.NodeName == "broker1");
    (await rig.Etcd.GetAsync(Ep, "/kafka/clusters/events/brokers/broker1/state", CancellationToken.None))
        .Value!.Value.Should().Be("PROVISIONING");
}
```

(`NewRig` расширить параметром `bool healer = false`; `JournalPhaseAsync()` —
чтение `/kafkaworker/work/events` через `rig.Etcd.GetAsync` + JsonDocument →
`phase` (или фактический хелпер чтения журнала из соседних тестов, если уже
есть); сид рига не сидирует
portalloc-ключ — сверить по фактическому сиду `NodeSupervisorTests` и
убрать/оставить сидирование соответственно.)

```bash
dotnet test src/tests/KafkaWorker.UnitTests -c Debug
```

Ожидание: PASS (лестница + все процессы; упавшие от смены семантики
portalloc-пустые сценарии — обновить asserts под самолечение).

- [ ] **Шаг 6: коммит**

```bash
git add -A src/KafkaWorker.Provisioning src/KafkaWorker.App src/tests/KafkaWorker.UnitTests
git commit -m 't05: PortAllocHealer — лестница E9 самолечения утерянного portalloc (portalloc → инспекция контейнера version==0 → S7-аллокация+RMW endpoints) вместо тупика «не закреплён» (spec §3.3, arch/17 E9)'
```

---

### Task 6: Интеграционные тесты — churn на закрытых портах + подъём после утери portalloc

Закрывает spec §7.5–7.6 (приёмка интеграционная). Порты — только
динамические/зондовые.

**Files:**
- Create: `src/tests/KafkaWorker.IntegrationTests/Kafka/KafkaClientChurnTests.cs`
- Create: `src/tests/KafkaWorker.IntegrationTests/Kafka/KafkaActiveGateTests.cs`
  (гейт Active-ветки — ревью F1: интеграционная проверка `ActiveAsync`
  skip E–J, `CreatedClients` не растёт)
- Modify: `src/KafkaWorker.App/KafkaWorker.App.csproj` — добавить
  `<InternalsVisibleTo Include="KafkaWorker.IntegrationTests"/>`
  (`KafkaClusterProcesses`/`IKafkaClusterProcesses` — internal; для юнитов
  `<InternalsVisibleTo Include="KafkaWorker.UnitTests"/>` уже объявлен
  в csproj — строка 4)
- Create: `src/tests/KafkaWorker.IntegrationTests/Kafka/PortAllocHealingTests.cs`
- Modify: `src/tests/KafkaWorker.IntegrationTests/Kafka/KafkaClusterFixture.cs`
  — ТОЛЬКО новые хелперы для healing-теста (`DelAsync(key)`,
  `RemoveBrokerContainersAsync(cluster)`, `SupervisorRigAsync(cluster)`);
  существующее свойство `AdminFactory` не трогать (тип менять не нужно:
  `CreatedClients` используется лишь локальной фабрикой churn-теста).

**Interfaces:**
- Consumes: кэш-фабрика (Task 1: `CreatedClients`), backoff (Task 2),
  healer (Task 5), `KafkaClusterFixture` (etcd + docker + AdminFactory +
  Options + FreePortWindow).

- [ ] **Шаг 1: churn-тест (закрытые порты)**

`KafkaClientChurnTests.cs` (НЕ в KafkaCollection — контейнеров kafka нет,
только etcd-фикстура; паттерн t11 `KafkaProbeClosedPortsTests`):

```csharp
// Churn-интеграция (t05, spec §7.5): Active-кластер с endpoints на ЗАКРЫТЫЕ
// порты (зонд свободных — рантайм) — воркер-циклы (supervise-проба +
// коллектор) не churn'ят клиентов: кэш + backoff держат CreatedClients ≤ 2
// за окно, потоки процесса стабильны. Литералов :16000 нет.
[Collection(EtcdCollection.Name)] // etcd-only фикстура (сверить имя по Etcd/-тестам)
public class KafkaClientChurnTests(EtcdFixture etcd)
{
    private static int FreeClosedPort()
    {
        using var probe = new System.Net.Sockets.TcpListener(
            System.Net.IPAddress.Loopback, 0);
        probe.Start();
        var port = ((System.Net.IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();
        return port; // вероятно-свободный закрытый порт (никто не слушает)
    }

    [Fact]
    public async Task UnreachableCluster_DoesNotChurnClients()
    {
        // Arrange: Active-кластер, endpoints на закрытые порты + креды.
        var port = FreeClosedPort();
        var cluster = "churn1";
        await etcd.PutAsync($"/kafka/clusters/{cluster}/endpoints", $"127.0.0.1:{port}");
        await etcd.PutAsync($"/kafka/clusters/{cluster}/app_user", "app");
        await etcd.PutAsync($"/kafka/clusters/{cluster}/app_password", "pw");
        await etcd.PutAsync($"/kafka/clusters/{cluster}/brokers/broker1/state", "RUNNING");
        await etcd.PutAsync($"/kafka/clusters/{cluster}/config",
            """{"brokers":1,"replication_factor":1,"min_insync_replicas":1,"default_partitions":1,"default_retention_ms":3600000}""");

        var factory = new KafkaAdminClientFactory(TimeSpan.FromSeconds(3));
        var backoff = new KafkaClusterBackoff(TimeProvider.System);
        var clock = TimeProvider.System;
        var state = new KafkaMetricsState(new System.Diagnostics.Metrics.Meter("TestKafkaWorker"));
        var collector = new KafkaMetricsCollector(
            30, etcd.SnapshotClusters, factory, state, clock,
            NullLogger<KafkaMetricsCollector>.Instance, backoff);

        var threadsBefore = System.Diagnostics.Process.GetCurrentProcess().Threads.Count;

        // Act: 3 «виртуальных тика» коллектора + supervise-проба подряд
        // (без задержек: backoff-окно 15 c гасит 2-й и 3-й контакты).
        for (var i = 0; i < 3; i++)
        {
            await collector.CollectOnceAsync(TestContext.Current.CancellationToken);
            await collector.CollectOnceAsync(TestContext.Current.CancellationToken);
        }

        // Assert: ≤ 2 нативных клиентов (первый + не более одного
        // unhealthy-пересоздания), потоки не растут (churn погашен).
        factory.CreatedClients.Should().BeInRange(1, 2,
            $"кэш+backoff: 6 тиков = 1 клиент (+≤1 пересоздание), не 5–7 на тик; фактически {factory.CreatedClients}");
        var threadsAfter = System.Diagnostics.Process.GetCurrentProcess().Threads.Count;
        (threadsAfter - threadsBefore).Should().BeLessThanOrEqualTo(10);
    }
}
```

Исполнителю: сверить фактические имена etcd-фикстуры интеграционного
проекта (`src/tests/KafkaWorker.IntegrationTests/Etcd/` — имя коллекции,
хелперы Put/снапшот кластеров; если готового `SnapshotClusters`-делегата
нет — построить по паттерну `KafkaClusterFixture.SnapshotAsync`:
RangeAsync `/kafka/clusters/` → `KafkaSnapshotParser.Parse`). Двух вызовов
`CollectOnceAsync` в итерации достаточно: первый — реальный контакт
(клиент #1, фейл, RecordFailure), второй — skip по backoff; итого 6 тиков
коллектора — граница ≤2 проверяет и кэш, и гейт. Supervise-проба покрывается
тем же трекером — отдельного цикла не требуется.

- [ ] **Шаг 1б: гейт Active-ветки (ревью F1: `ActiveAsync` skip E–J — интеграция)**

`src/tests/KafkaWorker.IntegrationTests/Kafka/KafkaActiveGateTests.cs` —
KafkaCollection (нужен `fixture.Driver`/`Gateway`); kafka-брокеры НЕ
поднимаются (драйвер — только для ListAsync-пустоты). `KafkaClusterProcesses`
internal → csproj-правка выше. Сид БЕЗ brokers-декларации: supervise
`Supervisable = []` — лестница/docker-пересоздания не сработают, гейт
проверяется изолированно:

```csharp
// Гейт Active-ветки (t05, spec §7.5/§3.2, ревью F1): кластер в активном
// backoff-окне — ActiveAsync (надзор-гейт + skip E–J/D) исполняется
// хост-процессом без kafka-контакта: тик Success, фабрика не зовётся.
[Collection(KafkaCollection.Name)]
public class KafkaActiveGateTests(KafkaClusterFixture fixture)
{
    [Fact]
    public async Task ActiveAsync_BackoffWindow_SkipsKafkaSteps()
    {
        var ct = TestContext.Current.CancellationToken;
        var cluster = fixture.Cluster("gate1");

        // Arrange: Active-кластер с endpoints на закрытый порт (зонд), БЕЗ
        // brokers-ключей; backoff-окно активно заранее.
        using var probe = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        probe.Start();
        var port = ((System.Net.IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();
        await fixture.PutAsync($"/kafka/clusters/{cluster}/config",
            """{"brokers":1,"replication_factor":1,"min_insync_replicas":1,"default_partitions":1,"default_retention_ms":3600000}""");
        await fixture.PutAsync($"/kafka/clusters/{cluster}/endpoints", $"127.0.0.1:{port}");
        await fixture.PutAsync($"/kafka/clusters/{cluster}/app_user", "app");
        await fixture.PutAsync($"/kafka/clusters/{cluster}/app_password", "pw");

        var claims = new ClaimStore([fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        await claims.TryClaimClusterAsync(cluster, ct);
        var journal = new WorkJournal(fixture.Gateway, [fixture.Endpoint]);
        var factory = new KafkaAdminClientFactory(TimeSpan.FromSeconds(3));
        var backoff = new KafkaClusterBackoff(TimeProvider.System);
        backoff.RecordFailure(cluster, "connection refused"); // окно 15 c активно

        var ep = new[] { fixture.Endpoint };
        var processes = new KafkaClusterProcesses(
            new ProvisioningProcess(fixture.Gateway, ep, fixture.Driver, claims, journal,
                new PortAllocLock(ep, fixture.Gateway, TimeProvider.System, claims.InstanceId),
                new PortAllocIndex(fixture.Gateway, ep, NullLogger<PortAllocIndex>.Instance),
                new AppSecretEnsurer(fixture.Gateway, ep),
                factory, new ClusterConfigConverger(factory), fixture.Options, snapshot: null),
            new DeprovisioningProcess(fixture.Gateway, ep, fixture.Driver, claims, journal, snapshot: null),
            new NodeSupervisor(fixture.Gateway, ep, fixture.Driver, claims, journal,
                factory, fixture.Options, backoff),
            new ClusterConfigConverger(factory),
            new PartitionReassignerProcess(fixture.Gateway, ep, fixture.Driver, claims, journal,
                factory, new ReassignOptions(15, 10, 180, 120), TimeProvider.System),
            new RemoveBrokerProcess(fixture.Gateway, ep, fixture.Driver, claims, journal,
                factory, fixture.Options),
            new AddBrokerProcess(fixture.Gateway, ep, fixture.Driver, claims, journal,
                new PortAllocLock(ep, fixture.Gateway, TimeProvider.System, claims.InstanceId),
                new PortAllocIndex(fixture.Gateway, ep, NullLogger<PortAllocIndex>.Instance),
                factory, fixture.Options),
            new AppPasswordRotator(fixture.Gateway, ep, fixture.Driver, claims, journal,
                factory, fixture.Options, snapshot: null),
            new NodeRegenerator(fixture.Gateway, ep, fixture.Driver, claims, journal, fixture.Options),
            new TopicSyncProcess(fixture.Gateway, ep, claims, journal,
                factory, TimeProvider.System, intervalSec: 15));

        // Act: один полный тик Active-ветки в активном backoff-окне.
        var snap = await fixture.SnapshotAsync(cluster);
        var result = await processes.ActiveAsync(snap!, ct);

        // Assert: тик успех; kafka-контакт не выполнялся (гейт до converger'а
        // и TopicSync; supervise-проба тоже гейтится — без brokers это
        // единственный путь к фабрике).
        result.IsSuccess.Should().BeTrue("skip по backoff — не ошибка тика");
        factory.CreatedClients.Should().Be(0,
            $"ActiveAsync в окне не должен создавать клиентов (E–J/D skip, проба надзора гейтится); фактически {factory.CreatedClients}");
    }
}
```

(`PutAsync`-хелпер фикстуры — при отсутствии добавить рядом с
`GetAsync`/`SeedClusterAsync`; ctor-порядок процессов — 1:1 по
construction-сайтам `Program.cs:183–280`; `NodeSupervisor` здесь без
healer'а — brokers-декларации нет, лестница недостижима.)

- [ ] **Шаг 2: healing-тест (обе ветки)**

`PortAllocHealingTests.cs` (в `KafkaCollection`; паттерн
`ProvisioningTests` — процессы руками, поллинг-тики с потолком 200 с):

```csharp
// Лестница E9 интеграционно (t05, spec §7.6): утеря portalloc живого
// кластера → реконструкция из inspect (ветка 2, без пересозданий); утеря
// portalloc + снос контейнеров → новая аллокация + RMW endpoints + подъём
// (ветка 3). Порты — окно фикстуры (FreePortWindow, 21000+).
[Collection(KafkaCollection.Name)]
public class PortAllocHealingTests(KafkaClusterFixture fixture)
{
    [Fact]
    public async Task LostPortAlloc_Heals_ClusterStaysAlive()
    {
        // Arrange: кластер поднимается provisioning'ом (паттерн
        // ProvisioningTests.FullLifecycle_ProvisionDiscoveryDeprovision —
        // сид 1-брокерного кластера, тики до Config.State == null),
        // endpoints/portalloc записаны.
        var cluster = fixture.Cluster("heal1");
        var ct = TestContext.Current.CancellationToken;
        await fixture.SeedClusterAsync(cluster, brokers: 1);
        var (supervisor, snapshot) = await fixture.SupervisorRigAsync(cluster); // хелпер: claims+journal+healer+backoff
        var endpointsBefore = await fixture.GetAsync($"/kafka/clusters/{cluster}/endpoints");

        // Act 1 (ветка 2): утеря журнала при живых контейнерах.
        await fixture.DelAsync($"/kafkaworker/portalloc/{cluster}");
        var snap1 = await fixture.SnapshotAsync(cluster);
        (await supervisor.RunAsync(snap1!, ct)).IsSuccess.Should().BeTrue();

        // Assert 1: portalloc восстановлен, порты прежние, пересозданий нет.
        var portAlloc = await fixture.GetAsync($"/kafkaworker/portalloc/{cluster}");
        portAlloc.Should().NotBeNull("реконструкция из inspect (ветка 2)");
        (await fixture.GetAsync($"/kafka/clusters/{cluster}/endpoints")).Should().Be(endpointsBefore);
        var admin = fixture.AdminFactory.Create(endpointsBefore!, "app",
            (await fixture.GetAsync($"/kafka/clusters/{cluster}/app_password"))!);
        (await admin.DescribeClusterAsync(ct)).IsSuccess.Should().BeTrue("кластер жив");

        // Act 2 (ветка 3): снос контейнеров + повторная утеря журнала.
        await fixture.DelAsync($"/kafkaworker/portalloc/{cluster}");
        await fixture.RemoveBrokerContainersAsync(cluster); // docker rm kfw-<C>-broker1 (хелпер фикстуры)
        var snap2 = await fixture.SnapshotAsync(cluster);
        (await supervisor.RunAsync(snap2!, ct)).IsSuccess.Should().BeTrue();

        // Assert 2: новая аллокация + пересоздание + endpoints обновлён +
        // готовность (поллинг DescribeCluster до успеха, потолок 120 с —
        // BrokerBootSec-пол гейта ≤ 100 c + запас).
        var endpointsAfter = await fixture.GetAsync($"/kafka/clusters/{cluster}/endpoints");
        endpointsAfter.Should().NotBe(endpointsBefore, "адрес перевыделин (S7)");
        // Готовность — по ДВУМ фактам (ревью F9): DescribeCluster отвечает И
        // воркер довёл state до RUNNING (поллинг обоих, потолок 120 с).
        var deadline = DateTimeOffset.UtcNow.AddSeconds(120);
        var alive = false;
        while (DateTimeOffset.UtcNow < deadline)
        {
            var view = await fixture.AdminFactory.Create(endpointsAfter!, "app",
                (await fixture.GetAsync($"/kafka/clusters/{cluster}/app_password"))!).DescribeClusterAsync(ct);
            var state = await fixture.GetAsync($"/kafka/clusters/{cluster}/brokers/broker1/state");
            if (view.IsSuccess && state == "RUNNING") { alive = true; break; }
            await Task.Delay(3000, ct);
        }
        alive.Should().BeTrue("кластер поднялся воркером после утери portalloc: DescribeCluster жив + state=RUNNING (ветка 3)");
    }
}
```

Хелперы для фикстуры (`KafkaClusterFixture`): `SupervisorRigAsync(cluster)`
(клэйм + `NodeSupervisor` со всеми зависимостями — по construction-сайту
Program.cs/ProvisioningTests), `DelAsync(key)`, `RemoveBrokerContainersAsync`
(`Driver.RemoveNodeAsync(cluster, "broker1", removeVolume: false)` — том
жив, данные неприкосновенны). Assert ветки 2 «пересозданий нет» —
предикат `Removed`-списка реального драйвера не годится (нет счётчика),
поэтому минимальный честный предикат: до/после тика сравнить
`Driver.ListNodeObjectsAsync(cluster)` + `ExecNodeAsync(cluster, "broker1",
["true"])` — контейнер остался living-объектом с тем же именем и отвечает
на exec (пересоздание дало бы паузу Stop/Remove и новый ContainerId; для
строгости допускается сверка `State.StartedAt` через `docker inspect
kfw-<C>-broker1` до/после — ExecNode-предиката достаточно).

- [ ] **Шаг 3: прогнать интеграционные kafka-серии**

```bash
dotnet test src/tests/KafkaWorker.IntegrationTests -c Debug --filter 'FullyQualifiedName~KafkaClientChurnTests|FullyQualifiedName~KafkaActiveGateTests|FullyQualifiedName~PortAllocHealingTests'
dotnet test src/tests/KafkaWorker.IntegrationTests -c Debug
```

Ожидание: PASS (первые — десятки секунд, healing — до ~4 мин: подъём
брокера; весь интеграционный прогон — зелёный).

- [ ] **Шаг 4: коммит**

```bash
git add -A src/tests/KafkaWorker.IntegrationTests
git commit -m 't05: интеграционные — churn на закрытых портах (CreatedClients<=2, потоки стабильны) и подъём кластера после утери portalloc (ветки 2/3 лестницы E9) (spec §7.5-7.6)'
```

---

### Task 7: Чек 66 + операционная мера стенда

Закрывает spec §6 (roadmap-пункт 6).

**Files:**
- Create: `dev-stand/adminpanel/checks/66-kafka-worker-churn.sh`

**Interfaces:**
- Consumes: живой стенд (`00-up.sh` full+kafka, образ kafkaworker с t05),
  `as-etcd`/`as-kafkaworker` контейнеры, `as-adminpanel` API (login) —
  образец `58-kafka-probe-churn.sh`.

- [ ] **Шаг 1: чек 66 (полный скрипт; паттерн наблюдения — 58-й)**

`dev-stand/adminpanel/checks/66-kafka-worker-churn.sh`:

```bash
#!/usr/bin/env bash
# 66-kafka-worker-churn.sh (t05): воркер не жжёт CPU на недоступном kafka-
# кластере. Репро инцидента as-kafkaworker 2026-09-04: Active-кластер,
# endpoints на закрытые порты, brokers declared, portalloc ПУСТ (тупик
# «не закреплён» + churn AdminClient ~100% ядра). Фикс: кэш клиентов +
# пины librdkafka >=1 c + backoff 15→60→300 c + лестница E9 portalloc.
# Проверки за окно CHURN_MINUTES (default 5; приёмка t05 — 15):
# (1) тупик лечится: /kafkaworker/portalloc/<C> появляется (лестница E9);
# (2) CPU as-kafkaworker (docker stats) <= 10% (приёмка <=5% ядра);
# (3) rdkafka-строк в логе <= 1/мин (после фикса — 0: Debug);
# (4) число потоков процесса стабильно (+<=10).
# Профиль: full+kafka (as-kafkaworker); образ kafkaworker — с фиксом t05
# (00-up.sh или docker compose build kafkaworker && up -d kafkaworker).
set -euo pipefail
cd "$(dirname "$0")/.."

CHURN_MINUTES="${CHURN_MINUTES:-5}"
# Закрытые порты — вне зон стенда (15000-151xx pg, 16000-161xx kafka,
# 18xxx patroni) и тестовых окон (21xxx+): 24997-24999, как 58-й.
CHURN_PORTS="${CHURN_PORTS:-24997 24998 24999}"
CLUSTER="churnkw"
WORKER="as-kafkaworker"

ect() { docker exec as-etcd etcdctl --endpoints=http://localhost:2379 "$@"; }
cleanup() {
  ect del "/kafka/clusters/$CLUSTER" --prefix >/dev/null 2>&1 || true
  ect del "/kafkaworker/portalloc/$CLUSTER" >/dev/null 2>&1 || true
  ect del "/kafkaworker/claims/$CLUSTER" >/dev/null 2>&1 || true
  ect del "/kafkaworker/work/$CLUSTER" >/dev/null 2>&1 || true
  # Лестница E9 (ветка 3) реально поднимает контейнер чек-кластера —
  # демонтаж без сирот: контейнер + том + per-cluster сеть.
  docker rm -f "kfw-$CLUSTER-broker1" >/dev/null 2>&1 || true
  docker volume rm "kfw-$CLUSTER-broker1-data" >/dev/null 2>&1 || true
  docker network rm "kfw-net-$CLUSTER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker inspect "$WORKER" >/dev/null 2>&1 \
  || { echo "❌ контейнер $WORKER не найден — поднимите стенд (00-up.sh)"; exit 1; }

# Предусловие: порты действительно закрыты (иначе «репро» ничего не репает).
for port in $CHURN_PORTS; do
  if (echo > "/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    echo "❌ порт $port занят — выберите свободные: CHURN_PORTS=\"...\" ./66-kafka-worker-churn.sh"; exit 1
  fi
done

# Сид репро-раскладки: Active-кластер (config без state) + broker1 RUNNING
# + endpoints на закрытые порты + креды; portalloc НЕ сидится (утерян).
bootstrap="$(printf 'host.docker.internal:%s,' $CHURN_PORTS)"; bootstrap="${bootstrap%,}"
ect put "/kafka/clusters/$CLUSTER/config" \
  '{"brokers":1,"replication_factor":1,"min_insync_replicas":1,"default_partitions":1,"default_retention_ms":3600000}' >/dev/null
ect put "/kafka/clusters/$CLUSTER/brokers/broker1/state" "RUNNING" >/dev/null
ect put "/kafka/clusters/$CLUSTER/endpoints" "$bootstrap" >/dev/null
ect put "/kafka/clusters/$CLUSTER/app_user" "app" >/dev/null
ect put "/kafka/clusters/$CLUSTER/app_password" "deadbeef" >/dev/null
echo ">>> сид: /kafka/clusters/$CLUSTER → $bootstrap (порты закрыты), portalloc нет; окно ${CHURN_MINUTES} мин"

# Assert 1: тупик лечится — portalloc появился (лестница E9: контейнера нет
# → S7-ветка → новая аллокация + контейнер; endpoints чек-кластера при этом
# обновится — это часть самолечения, cleanup гасит всё).
healed=""
for i in $(seq 1 36); do
  healed="$(ect get "/kafkaworker/portalloc/$CLUSTER" 2>/dev/null || true)"
  [ -n "$healed" ] && break
  sleep 5
done
[ -n "$healed" ] || { echo "❌ portalloc/$CLUSTER не появился за 180 с — лестница E9 не работает"; exit 1; }
echo "  лестница E9: portalloc восстановлен (${healed:0:60}…)"

# Наблюдение: CPU каждые 30 c + стартовые потоки (окно — после сида, чтобы
# тики воркера уже прошли по лежащему кластеру минимум раз).
threads_start="$(docker exec "$WORKER" sh -c 'ls /proc/1/task | wc -l')"
cpu_max=0
for i in $(seq 1 "$((CHURN_MINUTES * 2))"); do
  cpu="$(docker stats --no-stream --format '{{.CPUPerc}}' "$WORKER" | tr -d '%\n' | cut -d. -f1)"
  [ "${cpu:-0}" -gt "$cpu_max" ] && cpu_max="$cpu"
  sleep 30
done
threads_end="$(docker exec "$WORKER" sh -c 'ls /proc/1/task | wc -l')"
rdkafka_lines="$(docker logs --since "${CHURN_MINUTES}m" "$WORKER" 2>&1 | grep -ci rdkafka || true)"

# Assert 2: CPU <= 10% (инцидент ~99%, приёмка <=5% ядра).
[ "$cpu_max" -le 10 ] || { echo "❌ CPU воркера до ${cpu_max}% (бюджет 10%)"; exit 1; }
echo "  CPU воркера ≤ ${cpu_max}% за окно"

# Assert 3: rdkafka-лог <= 1 события/мин (после фикса — 0: Debug).
allowed="$CHURN_MINUTES"
[ "$rdkafka_lines" -le "$allowed" ] || { echo "❌ rdkafka-строк $rdkafka_lines за ${CHURN_MINUTES} мин (бюджет $allowed)"; exit 1; }
echo "  rdkafka-строк в логе: $rdkafka_lines (бюджет ≤$allowed)"

# Assert 4: потоки процесса не растут (churn poll-потоков погашен).
[ "$threads_end" -le "$((threads_start + 10))" ] || { echo "❌ потоки: $threads_start → $threads_end (растут)"; exit 1; }
echo "  потоки процесса стабильны: $threads_start → $threads_end"

echo "✅ 66-kafka-worker-churn: лежащий кластер не жжёт CPU (≤${cpu_max}%), лог тих (${rdkafka_lines}), потоки стабильны, portalloc самолечится"
```

(`chmod +x` после создания. Слет-хелпер 58-го (`ect`) здесь — прямой
`docker exec as-etcd` без compose-конфигурации каталога: чек выполняется из
`dev-stand/adminpanel/`, где compose-проект стенда и так резолвится; если
запускать вне каталога — `cd` в шапке уже приводит.)

- [ ] **Шаг 2: операционная мера — согласие стенда (runbook-действие, не код)**

На живом стенде (до/во время проверки чека 66): рассинхрон инцидентного
кластера устранить — `dev-stand/adminpanel/checks/05-seed.sh` (пересев
kafka-части) ЛИБО `docker exec as-etcd etcdctl del /kafka/clusters/<C> --prefix`
для кластера с пустым portalloc. Зафиксировать факт выполнения в тексте
коммита Task 8 (что именно сделано на стенде).

- [ ] **Шаг 3: подъём стенда + прогон чека 66 (приёмка — 15 мин)**

```bash
cd dev-stand/adminpanel && ./checks/00-up.sh   # или docker compose build kafkaworker && up -d
CHURN_MINUTES=15 ./checks/66-kafka-worker-churn.sh
```

Ожидание: PASS — CPU ≤10% за окно (приёмка spec: ≤5% ядра ≥15 мин;
если пиковый замер 6–10% — разобрать (docker stats granularity), но чек
зелёный только при ≤10); rdkafka ≤ CHURN_MINUTES строк; потоки +≤10;
portalloc `<CLUSTER>` появился. Профиль quick достаточен для churn-части,
но чек требует as-kafkaworker — сверить профиль по 57-му (full+kafka).

- [ ] **Шаг 4: коммит**

```bash
git add dev-stand/adminpanel/checks/66-kafka-worker-churn.sh
git commit -m 't05: чек 66 — репро инцидента as-kafkaworker (CPU/rdkafka/threads/лестница E9) по образцу 58-го (spec §6)'
```

---

### Task 8: arch/16-правки, roadmap-гейт, финальные прогоны (мерж-гейт)

Закрывает spec §4 (контракт) и §7.7–7.8 (приёмка/гейт).

**Files:**
- Modify: `arch/16-kafkaworker.md` (§5 C; §5 введение Active-ветки; §6)
- Modify: `arch/roadmap/kafkaworker.md` (удалить запись t05 — мерж-гейт)
- Modify: `arch/17-synchronization-principles.md` (только строка реестра
  E9: «планируется t05» → «закрыта t05 (2026-09-…)» — канон не меняется)

**Interfaces:**
- Consumes: всё из Task 1–7.

- [ ] **Шаг 1: arch/16 — три правки (текст из spec §4)**

1. §5 C (надзор), после абзаца «Слепая проба…»: дополнить — «**Backoff
   недоступного кластера** (t05): окно 15→60→300 с (сброс при успехе;
   писатели — проба надзора и коллектор) гейтит kafka-пробу (окно активно →
   слепая проба без сети) и kafka-шаги Active-ветки; docker-часть надзора и
   provisioning не гейтятся. **Лестница E9** (arch/17): безадресные
   Supervisable-брокеры — portalloc → реконструкция из inspect живого
   контейнера (put-if-absent под locks/portalloc) → новая аллокация (S7) +
   RMW endpoints; тупик «не закреплён в portalloc» устранён».
2. §5 (абзац классификации тика, перед «### A.»): дополнить — «kafka-шаги
   Active (E–J, D) пропускаются на время backoff недоступного кластера
   (15→60→300 с, сброс при успехе) — тик успех; docker-надзор продолжает».
3. §6 (Надёжность), новый пункт: «**AdminClient-кэш (t05)**: фабрика —
   sharable-кэш per (bootstrap, user, password); Create — «получить клиент
   ключа», DisposeAsync адаптера — no-op (владение у кэша); пины librdkafka
   `reconnect/retry.backoff.ms ≥ 1000`; Failed операции → пересоздание
   (фон-Destroy); неактивные ключи вытесняются; остановка — детерминированный
   Dispose (кэш = IDisposable DI-синглтон). Инцидент-класс: t11 (панель)».

- [ ] **Шаг 2: roadmap + arch/17 реестр**

- `arch/roadmap/kafkaworker.md`: удалить блок `t05-kafka-client-churn`
  целиком (правила `arch/roadmap/README.md`: никаких пометок «сделано»).
- `arch/17-synchronization-principles.md` реестр E9: колонка «Закрыта» →
  `t05-kafka-client-churn (2026-09-…)` — дата фактического мержа.

- [ ] **Шаг 3: полная тестовая серия**

```bash
dotnet test src/tests/KafkaWorker.UnitTests -c Release
dotnet test src/tests/KafkaWorker.IntegrationTests -c Release
dotnet test src/tests/PgWorker.UnitTests -c Release
dotnet test src/tests/AdminPanel.UnitTests -c Release
```

Ожидание: все зелёные (юниты KafkaWorker выросли: кэш 5, backoff 5,
healer 5+1, supervise 2; интеграция +2).

- [ ] **Шаг 4: E2E-гейт AGENTS.md (воркер-код + provisioning/portalloc)**

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release \
  --filter FullyQualifiedName~Scale_AddEmptyShard
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release \
  --filter FullyQualifiedName~E2e
```

Ожидание: маркер и полный E2eFixture — зелёные на свежем Release
(E2eFixture собирает сама; `PGW_TEST_E2E_NOBUILD=1` не использовать).

- [ ] **Шаг 5: финальный коммит задачи**

```bash
git add arch/16-kafkaworker.md arch/17-synchronization-principles.md arch/roadmap/kafkaworker.md
git commit -m 't05: arch/16 — backoff кластера + лестница E9 в надзоре, кэш AdminClient в надёжности; arch/17 E9 закрыта; roadmap-тег t05 удалён (мерж-гейт); E2E и серии зелёные'
```

После мержа в main: удалить тег из roadmap выполнено этим же коммитом;
стенд-мера (Task 7 Шаг 2) отражена в сообщении.

---

---

### Task 9: фиксы code-review Фазы 7 (5 findings — точечные правки поверх реализованного кода)

Код Task 1–8 уже в worktree (коммиты 591248f..04c88b2); шаги ниже —
минимальные фиксы найденного ревью + юниты. Спец-уточнения контракта
(третий факт перевода RUNNING, сходимость endpoints) уже внесены в spec
§3.3/§7 тем же изменением.

**Files (сводно):**
- Modify: `src/KafkaWorker.Provisioning/Processes/NodeSupervisor.cs`
  (Ф7-1 третий факт; Ф7-4 сходимость endpoints)
- Modify: `src/KafkaWorker.Provisioning/Kafka/KafkaAdminClient.cs` (Ф7-2)
- Modify: `src/KafkaWorker.Docker/Engine/IDockerEngine.cs`,
  `src/KafkaWorker.Docker/Engine/DockerEngine.cs`,
  `src/KafkaWorker.Docker/Drivers/ClusterDriver.cs` (Ф7-3 один ListTasks)
- Modify: `src/tests/KafkaWorker.UnitTests/Provisioning/NodeSupervisorTests.cs`
  (Ф7-1 гонка; Ф7-4 сходимость)
- Modify: `src/tests/KafkaWorker.UnitTests/App/KafkaMetricsCollectorTests.cs`
  (Ф7-5 writer-путь)
- Modify: `arch/16-kafkaworker.md` §5 C (ревью Ф4-3-4: два новых контракта
  надзора — перевод по трём фактам + сходимость endpoints — шаг 9.6)

#### 9.1. Ф7-1 [major]: перевод PROVISIONING→RUNNING — третий факт (адрес в endpoints)

**Вход:** `NodeSupervisor.RunAsync`, блок перевода ~строки 140–155
(фактический код): контейнер жив + зрячая проба → RUNNING — гонка с
`AddBrokerProcess` (pending фильтруется по `NOT_INITIALIZED|PROVISIONING`,
`AddBrokerProcess.cs:46`; его `AddEndpointsAsync` — после WaitReady,
`:87`): supervise переводит RUNNING до endpoints-RMW → add-broker no-op →
адрес навсегда вне endpoints.

**Действие:** в блок перевода добавить третий факт — advertised-адрес
брокера из слитого `addresses` уже в `snap.Endpoints`:

```csharp
// Перевод PROVISIONING → RUNNING — по ТРЁМ фактам (ревью Ф7-1): контейнер
// жив, зрячая проба видит брокера, И advertised-адрес уже в endpoints —
// владелец процесса (add-broker F) пишет endpoints ДО RUNNING; без адреса
// чужой процесс не закончен, RUNNING не наш (иначе add-broker-брокер
// выпадает из bootstrap-списка навсегда — pending пуст, RMW не исполнится).
foreach (var broker in snap.Brokers.Where(b => b.State == "PROVISIONING"))
{
    if (!alive.Contains($"kfw-{cluster}-{broker.Name}")
        || !inCluster.Contains(NodeId(broker.Name)))
        continue; // ещё грузится либо контейнера нет — не готов

    if (!addresses.TryGetValue(broker.Name, out var addr)
        || !AdvertisedInEndpoints(snap.Endpoints,
            $"{options.AdvertisedClientHost ?? addr.Host}:{addr.ClientPort}"))
        continue; // endpoints-RMW владельца не дошёл — не переводим

    var running = await PutAsync(BrokerStateKey(cluster, broker.Name), "RUNNING", ct);
    if (!running.IsSuccess)
        return Fail(cluster, running.Error!, "mark-running");
}
```

Хелпер (рядом с `Supervisable`): `private static bool
AdvertisedInEndpoints(string? endpoints, string advertised) =>
endpoints?.Split(',', StringSplitOptions.TrimEntries)
    .Contains(advertised) == true;` (формат канона — запятая без пробелов,
`BuildEndpoints` ProvisioningProcess / `UpdateEndpointsAsync` healer'а).

**Выход:** supervise не забирает у add-broker финализацию; RUNNING — только
после endpoints (инвариант arch/16 §5 F сохранён для всех писателей).

**Проверка:** юнит-тест гонки в `NodeSupervisorTests.cs` (риг существующий):

```csharp
// AAA (Ф7-1): PROVISIONING-брокер add-broker'а — контейнер жив, проба
// видит, но endpoints ещё БЕЗ его адреса → остаётся PROVISIONING; после
// endpoints-RMW владельца → следующий тик переводит RUNNING.
[Fact]
public async Task Run_ProvisioningWithoutEndpoints_KeepsProvisioning()
{
    // Риг дефолтный (фактический NewRig, NodeSupervisorTests.cs:33–38:
    // int nodeDeadSec, int rf, Action setup, KafkaClusterBackoff?, bool
    // healer — параметра-admin НЕТ): 3 брокера RUNNING/controller, portalloc
    // 16000/16001/16002 (полный — healer не нужен), endpoints
    // «h1:16000,h1:16001,h1:16002», NodeObjects — все три контейнера.
    var rig = await NewRig();

    // Зрячая проба видит broker1+broker2 (паттерн rig.Admin.ClusterView,
    // :135): broker3 «молчит» — трек стартует, NodeDeadSec=90 — пересозданий
    // в тесте нет. Admin меняется через риг, не через параметр NewRig.
    rig.Admin.ClusterView = new KafkaClusterView(
        [new KafkaBrokerView(1, "b1"), new KafkaBrokerView(2, "b2")], ControllerId: 1);

    // Брокер add-broker'а: state=PROVISIONING; portalloc рига НЕ трогаем
    // (закрепления всех трёх на месте — healer-гейт не срабатывает).
    await rig.Etcd.PutAsync(Ep, "/kafka/clusters/events/brokers/broker2/state",
        "PROVISIONING", null, CancellationToken.None);

    // Фаза 1: endpoints БЕЗ адреса broker2 (RMW add-broker'а «ещё не дошёл»;
    // дефолтные endpoints содержат 16001 — перезаписываем).
    await rig.Etcd.PutAsync(Ep, "/kafka/clusters/events/endpoints",
        "h1:16000", null, CancellationToken.None);

    var result = await rig.Supervisor.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

    result.IsSuccess.Should().BeTrue();
    (await rig.Etcd.GetAsync(Ep, "/kafka/clusters/events/brokers/broker2/state",
        CancellationToken.None)).Value!.Value.Should().Be("PROVISIONING",
        "адреса broker2 (h1:16001) нет в endpoints — add-broker не дошёл до RMW, RUNNING не наш");

    // Фаза 2: владелец доделал endpoints-RMW (адрес broker2 появился — дефолт
    // рига) → следующий тик переводит (перевод читает снапшот начала тика).
    await rig.Etcd.PutAsync(Ep, "/kafka/clusters/events/endpoints",
        "h1:16000,h1:16001,h1:16002", null, CancellationToken.None);

    (await rig.Supervisor.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None))
        .IsSuccess.Should().BeTrue();
    (await rig.Etcd.GetAsync(Ep, "/kafka/clusters/events/brokers/broker2/state",
        CancellationToken.None)).Value!.Value.Should().Be("RUNNING");
}
```

(значения 16000–16002 — константы ДЕФОЛТНОГО сида фактического `NewRig`, не
хост-порты теста: FakeEtcd/FakeKafkaDriver — чистая память, сетевых
литералов нет; two-фазный дизайн корректен: перевод читает immutable
`snap.Endpoints` начала тика).

**Связь со spec:** §3.3 инвариант «перевод по трём фактам», §7.3 юнит гонки.

#### 9.2. Ф7-2 [minor]: EnsureClient — потокобезопасная ленивая инициализация

**Вход:** `KafkaAdminClient.EnsureClient()` (~строки 391–402): `if (_client
is not null) return _client; _client = Build();` — два параллельных первых
вызова (supervise-тик + коллектор на одном ключе кэша) оба видят null →
два нативных AdminClient, проигравший — сирота до финализатора.

**Действие:** double-checked lock (гонка редкая, lock в горячем пути только
при null — стоимость нулевая после инициализации):

```csharp
private readonly object _clientGate = new();

private IAdminClient EnsureClient()
{
    if (_client is not null)
        return _client;

    lock (_clientGate)
    {
        if (_client is not null)
            return _client;

        // Пины backoff + rdkafka-лог на Debug (профиль фабрики, t05 spec §3.1):
        // дефолтные 100 мс давали reconnect-шторм на лежащем кластере.
        _client = new AdminClientBuilder(KafkaAdminClientFactory.BaseAdminConfig(bootstrap, user, password))
            .SetLogHandler((_, m) => log?.LogDebug("rdkafka: {Message}", m.Message))
            .Build();
        return _client;
    }
}
```

`DisposeNative()` — под тем же `_clientGate` (взаимоисключение с
инициализацией: Dispose кэша при shutdown vs параллельный первый вызов).

**Выход:** максимум один нативный клиент на адаптер при любом числе
параллельных первых вызовов; Dispose/инициализация взаимоисключены.

**Проверка:** юнит не добавляем — нативный инстанс не наблюдаем без сети,
механика double-checked lock тривиальна; регрессия закрыта существующими
`KafkaAdminClientFactoryTests` (шаринг по ключу) + чеком 66 (потоки
стабильны). Прогон: `dotnet test src/tests/KafkaWorker.UnitTests -c Debug`.

**Связь со spec:** §3.1 «один адаптер per ключ, владение у кэша».

#### 9.3. Ф7-3 [minor]: swarm-инспекция — один ListTasksAsync (источник истины — движок)

**Вход:** `DockerEngine.InspectNodeEndpointAsync` при plain-404 на
swarm-движке зовёт `InspectSwarmTaskEndpointAsync` → `ListTasksAsync` (№1,
отдаёт порт); затем `SwarmClusterDriver.InspectNodeEndpointAsync`
(ClusterDriver.cs ~422–440) листает таски ЕЩЁ РАЗ для хоста — план Task 4
Шаг 2 требовал один источник.

**Действие:** host таска возвращает движок (тот же вызов):

1. `IDockerEngine.cs` — record: `public sealed record DockerNodeEndpoint(
    int ClientHostPort, string? AdvertisedClient, string? TaskHost = null);`
2. `DockerEngine.InspectSwarmTaskEndpointAsync`:
   `new DockerNodeEndpoint(running.PublishedPort!.Value, null, running.Host)`.
3. `SwarmClusterDriver.InspectNodeEndpointAsync` — ВТОРОЙ `ListTasksAsync`
   удалить, host из `endpoint.Value.TaskHost`:

```csharp
public async Task<Result<NodeEndpointInspection?>> InspectNodeEndpointAsync(
    string cluster, string nodeName, CancellationToken ct)
{
    var name = PlainClusterDriver.NodeName(cluster, nodeName);
    var endpoint = await _engine.InspectNodeEndpointAsync(name, ct);
    if (!endpoint.IsSuccess)
        return Result<NodeEndpointInspection?>.Failed(endpoint.Error!);
    if (endpoint.Value is not { } found)
        return Result<NodeEndpointInspection?>.Success(null);

    return found.TaskHost is { } host
        ? Result<NodeEndpointInspection?>.Success(
            new NodeEndpointInspection(host, found.ClientHostPort, found.AdvertisedClient))
        : Result<NodeEndpointInspection?>.Success(null); // хоста таска нет — факта нет
}
```

PlainClusterDriver не меняется (host — из перебора движков, TaskHost там
null и не читается).

**Выход:** один HTTP-раунд ListTasks на инспекцию; host/порт всегда из
одного снимка таска (нет расхождения при редкой смене ноды между вызовами).

**Проверка:** сборка + юниты (`FakeKafkaDriver` не трогаем — он выше
драйверов); swarm-покрытие — компиляцией (как до фикса, swarm-интеграции в
проекте нет): `dotnet build src/PgWorker.slnx -c Debug && dotnet test
src/tests/KafkaWorker.UnitTests -c Debug`.

**Связь со spec:** §3.4 «published-порт и хост — running-таск» (один
источник истины).

#### 9.4. Ф7-4 [minor]: сходимость endpoints в надзоре (недоехавший RMW ветки 3)

**Вход:** `PortAllocHealer` ветка 3: portalloc-txn закоммичен → сбой
`EnsureNodeAsync`/`UpdateEndpointsAsync` → следующий тик `ResolveAsync`
возвращает pinned (ветка 1, ранний выход) — «ретрай тиком» не исполняется:
endpoints навсегда без адреса.

**Действие:** шаг сходимости в `NodeSupervisor.RunAsync` — ПОСЛЕ лестницы и
ДО блока перевода PROVISIONING→RUNNING (ревью Ф4-3-3: перевод читает
immutable `snap.Endpoints` НАЧАЛА тика — адрес, дописанный сходимостью в
этом же тике, перевод увидит только СЛЕДУЮЩИМ тиком; порядок
«сходимость до перевода» сохраняет etcd-видимый инвариант
endpoints-до-RUNNING в рамках тика: к моменту, когда следующий тик
переведёт RUNNING, адрес уже в endpoints — поведение корректно и
консервативно, именно его ассертит тест §7.3):

```csharp
// Сходимость endpoints (ревью Ф7-4): закоммиченный portalloc — истина
// адресов; фактический endpoints сверяется с каноном «адреса всех брокеров
// с закреплением и state не TO_REMOVE/REMOVING»; расхождение → RMW. Закрывает
// недоехавший RMW ветки 3 healer'а (следующий тик — ветка 1, без сходимости
// ретрай не случился бы); add-broker в полёте безопасен: адрес каноничен,
// RMW владельца идемпотентен, «endpoints до RUNNING» сохраняется.
var converged = await EnsureEndpointsConvergedAsync(snap, addresses, ct);
if (!converged.IsSuccess)
    return Fail(cluster, converged.Error!, "endpoints-converge");
```

Реализация (private, ниже по классу):

```csharp
// Канон: адреса брокеров с закреплением (PROVISIONING включён — ветка 3
// healer'а и add-broker F пишут портalloc до контейнера), кроме чужих
// демонтажей (TO_REMOVE/REMOVING — их адрес убирает G). Порядок адресов —
// по имени брокера (детерминизм, порт BuildEndpoints).
private async Task<Result> EnsureEndpointsConvergedAsync(
    KafkaClusterSnapshot snap, IReadOnlyDictionary<string, NodeAddress> addresses, CancellationToken ct)
{
    var cluster = snap.Cluster;
    var canonical = string.Join(",", snap.Brokers
        .OrderBy(b => b.Name, StringComparer.Ordinal)
        .Where(b => b.State is not ("TO_REMOVE" or "REMOVING"))
        .Where(b => addresses.TryGetValue(b.Name, out _))
        .Select(b =>
        {
            var addr = addresses[b.Name];
            return $"{options.AdvertisedClientHost ?? addr.Host}:{addr.ClientPort}";
        }));

    var key = EndpointsKey(cluster);
    var current = await GetAsync(key, ct);
    if (!current.IsSuccess)
        return current;
    if (current.Value is { } kv && kv.Value == canonical)
        return Result.Success(); // сходимость — норма: ноль записей

    if (current.Value is null && canonical.Length == 0)
        return Result.Success(); // ключа нет и писать нечего

    // RMW: нет ключа → put; есть → txn compare mod_revision (проигрыш —
    // ретрай тиком, канон тот же у всех писателей).
    if (current.Value is null)
    {
        var put = await PutAsync(key, canonical, ct);
        return put.IsSuccess ? Result.Success() : put;
    }

    var txn = await TxnAsync(TxnRequest.Of(
        [TxnCompare.ModRevisionEqual(key, (long)current.Value.ModRevision)],
        [new TxnOp.Put(key, canonical, null)]), ct);
    if (!txn.IsSuccess)
        return txn;
    if (!txn.Value.Succeeded)
        return Result.Failed(new ApplicationException(
            $"endpoints {cluster} изменился с момента чтения — ретрай тиком"));
    return Result.Success();
}

private static string EndpointsKey(string cluster) => $"/kafka/clusters/{cluster}/endpoints";
```

(`GetAsync`/`PutAsync`/`TxnAsync`/failover-обёртки уже есть в
NodeSupervisor — PutAsync/GetAsync есть; `TxnAsync` добавить по паттерну
ProvisioningProcess.TxnAsync, `TxnCompare.ModRevisionEqual` — как в
AddBrokerProcess.EnsurePortsAsync. При расхождении — journal-фаза не
пишется: запись редкая сходимость, диагностика — само значение endpoints;
по решению исполнителя допустим `journal.WriteAsync(cluster, Op,
"endpoints-converged", ...)` единожды при расхождении.)

**Выход:** endpoints монотонно сходится к portalloc-истине любым тиком
надзора; сбой ветки 3 после portalloc-txn самолечится следующим тиком.

**Проверка:** юнит в `NodeSupervisorTests.cs`:

```csharp
// AAA (Ф7-4): portalloc закреплён, endpoints отстал (сбой RMW ветки 3) —
// тик надзора сходит endpoints к канону; повторный тик — без записей
// (значение стабильно; проверяем по итоговому значению ключа).
[Fact]
public async Task Run_EndpointsLagPortalloc_Converges()
{
    var rig = await NewRig(); // portalloc: broker1 h1:16000, broker2 h1:16001, broker3 h1:16002 (дефолт сида)
    await rig.Etcd.PutAsync(Ep, "/kafka/clusters/events/endpoints",
        "h1:21099", null, CancellationToken.None); // «недоехавший» RMW

    var result = await rig.Supervisor.RunAsync(await Snapshot(rig.Etcd), CancellationToken.None);

    result.IsSuccess.Should().BeTrue();
    var endpoints = (await rig.Etcd.GetAsync(Ep, "/kafka/clusters/events/endpoints",
        CancellationToken.None)).Value!.Value;
    endpoints.Should().Be(string.Join(",", rig.PortAllocAddresses()
        .OrderBy(a => a.Key, StringComparer.Ordinal)
        .Select(a => $"h1:{a.Value.ClientPort}")),
        "канон = advertise-адреса всех закреплённых брокеров (AdvertisedClientHost=null в риге)");
}
```

(хелпер `rig.PortAllocAddresses()` — чтение siда portalloc рига; при
отсутствии — прямой parse ключа `/kafkaworker/portalloc/events`. Если в
риге AdvertisedClientHost задан — подставить его в ожидание.)

**Связь со spec:** §3.3 инвариант «сходимость endpoints (шаг надзора)».

#### 9.5. Ф7-5 [plan/minor]: юнит writer-пути коллектора (RecordFailure/RecordSuccess)

**Вход:** после замены vehicle churn-теста (коллектор в churn-интеграции —
read-side) прямой тест writer-пути `TryCollectClusterAsync` отсутствует.

**Действие:** добавить в существующий класс
`src/tests/KafkaWorker.UnitTests/App/KafkaMetricsCollectorTests.cs`
(переиспользуя `FakeFactory { Next }`, `FakeAdmin { FailDescribe }`,
`Snapshot(name)` — все уже в файле):

```csharp
// AAA (Ф7-5): writer-путь коллектора — фейл сбора растит backoff-окно
// (IsBlocked true), успех сбрасывает (false). Clock — SETTABLE
// FixedTimeProvider (TestTime — immutable FakeClock: окно 15 c после фейла
// не истекло бы никогда, гейт CollectOnceAsync:76–77 съел бы второй тик):
// один инстанс и для backoff, и для коллектора; между тиками двигаем
// clock.Utc += 15 c (ревью Ф4-3-1).
[Fact]
public async Task Collect_FailThenSuccess_BackoffWindowFollows()
{
    var clock = new Provisioning.FixedTimeProvider(); // settable (уже юзается :285)
    var backoff = new KafkaClusterBackoff(clock);
    var factory = new FakeFactory
    {
        Next = new FakeAdmin { FailDescribe = new ApplicationException("down") },
    };
    var state = new KafkaMetricsState(new Meter("TestKafkaWorker"));
    var collector = new KafkaMetricsCollector(30,
        ct => Task.FromResult(Result<IReadOnlyList<KafkaClusterSnapshot>>.Success([Snapshot("c1")])),
        factory, state, clock, NullLogger<KafkaMetricsCollector>.Instance, backoff);

    await collector.CollectOnceAsync(TestContext.Current.CancellationToken);
    backoff.IsBlocked("c1").Should().BeTrue("фейл сбора — RecordFailure, окно 15 c активно");

    // Окно должно истечь, иначе гейт (IsBlocked → continue) съест второй тик
    // ДО writer-пути — RecordSuccess недостижим.
    clock.Utc += TimeSpan.FromSeconds(15);

    factory.Next = new FakeAdmin(); // DescribeCluster ок (пустой кластер), Groups/Topics пустые
    await collector.CollectOnceAsync(TestContext.Current.CancellationToken);
    backoff.IsBlocked("c1").Should().BeFalse("успех сбора — RecordSuccess, окно снято");
}
```

(`FakeAdmin.DescribeClusterAsync` при `FailDescribe == null` уже отдаёт
успех с пустым `KafkaClusterView` — фактический код тестов, строки ~27–34;
`Provisioning.FixedTimeProvider` — settable `Utc { get; set; }`, уже
используется в `CollectOnce_BackoffCluster_SkippedWithoutClient`; время
коллектора и backoff — один инстанс: `MarkSuccess` и окна читают общую
ось.)

**Выход:** writer-путь коллектора покрыт прямым юнитом (гейт Ф7-теста
`CollectOnce_BackoffCluster_SkippedWithoutClient` — read-side).

**Проверка:** `dotnet test src/tests/KafkaWorker.UnitTests -c Debug
--filter 'FullyQualifiedName~KafkaMetricsCollectorTests'`.

**Связь со spec:** §7.2 «writer-путь коллектора — фейл → IsBlocked true,
успех → false».

#### 9.6. arch/16 §5 C, прогоны и коммит

- [ ] **arch/16 §5 C (ревью Ф4-3-4 — Task 8 уже закрыл backoff/лестницу, но
  два контракта фиксов 9.1/9.4 в каноне отсутствуют):** дополнить раздел
  надзора одной строкой (после абзаца о слепой пробе, рядом с правками t05
  из Task 8):

> Перевод `PROVISIONING`→`RUNNING` — по трём фактам: контейнер жив, зрячая
> проба видит брокера, advertised-адрес уже в `endpoints` (владелец
> процесса — add-broker F — пишет endpoints ДО RUNNING; иначе чужой процесс
> «догоняется» и адрес выпадает из bootstrap-списка). `endpoints` сходится
> к portalloc-канону тиком надзора (расхождение → RMW; закрывает недоехавший
> RMW лестницы E9).

- [ ] Прогнать серии: `dotnet test src/tests/KafkaWorker.UnitTests -c Debug` →
  PASS (новые: гонка 9.1, сходимость 9.4, writer 9.5);
  `dotnet test src/tests/KafkaWorker.IntegrationTests -c Debug` → PASS
  (фиксы 9.1/9.4 меняют supervise — churn/healing/ActiveGate не должны
  регресснуть: ActiveGate-сид без brokers — блоки не исполняются).
- [ ] Release-серия + мерж-гейт не меняются (Task 8 Шаг 3–4 уже исполнены;
  после фиксов — повторить минимум: юниты+интеграция Release и E2E-маркер
  `Scale_AddEmptyShard`, полный E2eFixture — по гейту AGENTS.md, т.к.
  provisioning-код менялся).
- [ ] Коммит:

```bash
git add -A src/KafkaWorker.Provisioning src/KafkaWorker.Docker src/tests/KafkaWorker.UnitTests arch/16-kafkaworker.md
git commit -m 't05-ревью Ф7: третий факт перевода PROVISIONING→RUNNING (адрес в endpoints — гонка с add-broker), сходимость endpoints в надзоре (недоехавший RMW ветки 3), EnsureClient double-checked lock, swarm-инспекция — один ListTasks (TaskHost из движка), юнит writer-пути коллектора; arch/16 §5 C — оба контракта надзора (spec §3.3, §4, §7)'
```

---

## Соответствие план ↔ спека (самопроверка)

| Спека | Задача |
|---|---|
| §3.1 кэш/пины/no-op dispose/unhealthy/вытеснение/shutdown (roadmap 1–3) | Task 1 |
| §3.2 трекер (roadmap 4) | Task 2 |
| §3.2/§3.5 гейты supervise/Active/коллектор/ForgetMissing | Task 3 |
| §3.4 грань инспекции | Task 4 |
| §3.3 лестница E9 (roadmap 5) | Task 5 |
| §7.1–7.4 юниты | Task 1 (кэш, 8: reuse/пины/вытеснение×2/unhealthy/OCE/Dispose), Task 2 (backoff, 5), Task 3 (гейты supervise+коллектор), Task 5 (лестница, 7 + supervise 2) |
| §7.5–7.6 интеграционные | Task 6 (churn + ActiveGate-§7.5 + healing) |
| §6 стенд + чек по образцу 58 (roadmap 6) | Task 7 |
| §4 arch-правки, §7.7–7.8 приёмка/мерж-гейт | Task 8 |
| ревью Ф7-1..Ф7-5 (третий факт RUNNING, сходимость endpoints, lock, один ListTasks, writer-юнит) | Task 9 |

Границы по назначению: §8 спеки (вне scope) в план не попали сознательно.
