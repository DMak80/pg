# t37-owns3-replication-ping — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Активная проба живости репликационных линков ownS3 mesh: грань `GET /replication/ping` (приёмник) + админ-триггер `POST /admin/replication/probe` (источник) с per-target таймаутом и синхронным JSON-ответом.

**Architecture:** Тот же конвейер, что у notifier (креды/маршрут/SigV4), но мимо outbox: probe не читает журнал, не двигает курсоры, не порождает событий. `ReplicationProber` (новый класс ядра, тестируем без хоста) параллельно пингует снапшот targets исходящим `IReplicationTransport.PingAsync`; приёмная грань отвечает 200 без тела, не трогая ни одного состояния. Arch-first: контракт сначала в `arch/owns3/`.

**Tech Stack:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`), ASP.NET Core (Kestrel, WAF-тесты), xUnit + FluentAssertions, Testcontainers (docker-E2E `owns3:e2e`).

**Spec:** [`docs/superpowers/2026-10-13-t37-owns3-replication-ping/spec.md`](spec.md) — план аргументируется от spec; исполнители читают оба документа.

**Worktree:** `/Users/demakaev/ZCodeProject/worktrees/feat-t37-owns3-replication-ping` (ветка `feat-t37-owns3-replication-ping`). Все пути ниже — от корня worktree; все команды — из корня worktree.

## Global Constraints

- **Arch-first (спека §2.1):** Task 0 (правки `arch/`) коммитится РАНЬШЕ любого кодового коммита — критерий приёмки 1.
- **Язык:** комментарии и документация — по-русски; идентификаторы — на английском. Тесты — с AAA-комментариями (`// Arrange`, `// Act`, `// Assert`).
- **`TreatWarningsAsErrors=true`** — код обязан собираться без warnings.
- **Метрики/логи probe НЕ вводятся** (спека §3): результат — только JSON ответа; ping-трафик автоматически считается существующей `ownS3_requests_total{operation=replication}` (operationLabel middleware уже «replication»). Никаких правок `ReplicationMetrics`.
- **`link_online`, курсоры, outbox, fetch-очереди, счётчики probe НЕ меняет** (спека §1/§4.2) — ни в коде, ни в тестах.
- **Existing-регресс неприкосновенен** (спека §2.6): без соседей каталог `replication/` не создаётся, грань `/replication/*` — прежние 403/404, probe — 200 `{"active":false,"targets":[]}`.
- **Таймауты:** per-target дефолт **2000 мс**, env `OWNS3_REPLICATION_PROBE_TIMEOUT_MS` → `OwnS3:Replication:ProbeTimeoutMs`; linked CTS поверх общего таймаута HttpClient 10 с.
- **reachable строго при 200** ping-грани; `rttMs` (long, мс) при любом HTTP-ответе, отсутствует при таймауте/обрыве.
- **E2E-каноны:** уникальный guid-контур на сценарий (`RunScenarioAsync`), полный teardown при любом исходе + ассерт чистоты, динамические порты, `[PHASE]`-телеметрия — уже в `OwnS3MeshE2eEnvironment`; после КАЖДОЙ тестовой серии — зачистка контейнеров/сетей (правила AGENTS.md).
- **Запуск команд:** все dotnet-прогоны — с `DOTNET_CLI_UI_LANGUAGE=en`; docker-E2E — `PGW_TEST_DOCKER=1`, конфигурация Release, E2eFixture собирает образ сам (инкрементально).

---

## Task 0: Arch-first — контракт probe в `arch/owns3/` (фаза 0 spec)

**Files:**
- Modify: `arch/owns3/07-replication.md` (§2 — перечень админ-грани; §6 — таблица операций; §11 — env-таблица)
- Modify: `arch/owns3/05-access-config.md` (§4 — env-таблица; §7 — перечень эндпоинтов Mesh-репликации)

**Interfaces:**
- Consumes: ничего (контракт описывается до кода).
- Produces: канонические описания `POST /admin/replication/probe`, `GET /replication/ping`, env `OWNS3_REPLICATION_PROBE_TIMEOUT_MS` — код Tasks 1–6 зеркалит их дословно.

- [ ] **Step 0.1: §2 главы 07 — строка probe в перечень админ-грани**

В `arch/owns3/07-replication.md` §2, в список «Изменение состава без даунтайма S3-грани — админ-грань», ПОСЛЕ строки `POST /admin/replication/reconcile` (и до строки про оркестратор трека s3) вставить:

```markdown
  - `POST /admin/replication/probe` — активная проба репликационных линков
    (детектор живости для владельца/зонда; `online` статуса — пассивный кэш
    исходов notifier, §10). Синхронный ответ (не 202/фон — в отличие от
    reconcile): пингует всех соседей снапшота targets (или одного
    `?target=<name>`) ПАРАЛЛЕЛЬНО исходящим репликационным транспортом
    (креды/маршрут notifier'а, мимо outbox) — `GET /replication/ping` (§6);
    per-target таймаут `OWNS3_REPLICATION_PROBE_TIMEOUT_MS` (§11); ответ 200
    `{active, targets:[{name, reachable, rttMs?}]}`: `reachable=true` строго
    при валидном 200 ping-грани, `rttMs` (мс) — при любом полученном
    HTTP-ответе (отсутствует при таймауте/обрыве транспорта); неизвестное имя
    `?target` — 400; без соседей — `{active: false, targets: []}` (не ошибка).
    Probe не меняет `link_online`, курсоры, outbox, fetch-очереди и счётчики
    (только по требованию — периодический автопинг/фон-цикл НЕ вводится;
    retry-логики нет: один запрос — один исход). Задержка ответа — max
    per-target, не sum (параллельность).
```

- [ ] **Step 0.2: §6 главы 07 — строка ping в таблицу операций replication-грани**

В таблицу «| Операция | Запрос/ответ | Контракт |» §6 (после строки «листинг») добавить строку:

```markdown
| ping | `GET /replication/ping` | Активная проба линка (источник — админ-trigger `POST /admin/replication/probe`, §2): **200, тело отсутствует**. Полный конвейер грани без изъятий: SigV4-подпись, политика строго `replication` (глава 05 §3, прочие — 403 `AccessDenied`), accessKey подписи обязан числиться соседом приёмника (резолв `sourceName`, §2). Приёмник ничего не пишет и не применяет: дедуп, fetch-очереди, курсоры, tombstones, счётчики применения ping-ом не затрагиваются (состояние байт-в-байт то же). Прочие методы — **400 `InvalidArgument`** («Unknown replication resource») |
```

- [ ] **Step 0.3: §11 главы 07 — env-строка**

В env-таблицу §11 (после строки `OWNS3_REPLICATION_FETCH_QUEUE_MAX`) добавить:

```markdown
| `OWNS3_REPLICATION_PROBE_TIMEOUT_MS` | per-target таймаут ping-пробы линков (§2) | **2000** |
```

- [ ] **Step 0.4: §4 главы 05 — зеркало env-строки**

В env-таблицу `arch/owns3/05-access-config.md` §4 (после строки `OWNS3_REPLICATION_FETCH_QUEUE_MAX`) добавить:

```markdown
| `OWNS3_REPLICATION_PROBE_TIMEOUT_MS` | `OwnS3:Replication:ProbeTimeoutMs` | per-target таймаут ping-пробы репликационных линков (глава 07 §2) | **2000** |
```

- [ ] **Step 0.5: §7 главы 05 — эндпоинт в перечень Mesh-репликации**

В `arch/owns3/05-access-config.md` §7, в блок «Mesh-репликация» (после строки `GET /admin/replication/sync-report?bucket=&prefix=&marker=` — последней строки блока, конец блока «Mesh-репликация») добавить:

```markdown
- **`POST /admin/replication/probe`** — активная проба репликационных линков:
  пинг всех (или `?target=`) соседей параллельно, per-target таймаут,
  синхронный ответ `{active, targets:[{name, reachable, rttMs?}]}` (глава
  07 §2). JSON.
```

- [ ] **Step 0.6: Проверка правок**

Run: `git diff --stat arch/`
Expected: ровно 2 изменённых файла (`arch/owns3/05-access-config.md`, `arch/owns3/07-replication.md`).

Run: `grep -c "PROBE_TIMEOUT_MS" arch/owns3/05-access-config.md arch/owns3/07-replication.md`
Expected: `05-access-config.md:1` (только env-таблица §4) и `07-replication.md:2` (упоминание в §2 probe-строке + env-таблица §11).

- [ ] **Step 0.7: Commit (arch-first — до любого кода)**

```bash
git add arch/owns3/05-access-config.md arch/owns3/07-replication.md
git commit -m "t37: arch-first — активная проба репликационных линков ownS3: owns3/07 §2 (POST /admin/replication/probe — параллельный ping соседей мимо outbox, синхронный {active, targets:[{name, reachable, rttMs?}]}, reachable строго по 200) + §6 (GET /replication/ping — 200 без тела, полный конвейер грани, приёмник состояние не меняет, прочие методы 400) + §11 (OWNS3_REPLICATION_PROBE_TIMEOUT_MS, дефолт 2000); owns3/05 §4 (зеркало env) + §7 (эндпоинт в перечень админ-грани)"
```

---

## Task 1: Контракт `PingAsync` в `IReplicationTransport` + реализация `ReplicationHttpClient` (фаза 1.1 spec)

**Files:**
- Modify: `src/OwnS3.Replication/Transport/IReplicationTransport.cs`
- Modify: `src/OwnS3.Replication/Transport/ReplicationHttpClient.cs`
- Modify (заглушки фейков — интерфейс расширился): `src/tests/OwnS3.UnitTests/Replication/NotificationDispatcherTests.cs`, `src/tests/OwnS3.UnitTests/Replication/ReconcileRunnerTests.cs`, `src/tests/OwnS3.UnitTests/Replication/ObjectFetcherTests.cs`

**Interfaces:**
- Consumes: существующий `BuildRequest(...)` (GET с пустым payload-hash уже умеет: `payloadHash ?? SigV4Core.EmptySha256`).
- Produces (для Tasks 3, 5): в namespace `OwnS3.Replication.Transport`:
  - `public sealed record PingResult(bool Reachable, long? RttMs);`
  - `Task<PingResult> PingAsync(TargetRecord target, TimeSpan timeout, CancellationToken ct)` — член `IReplicationTransport`.

- [ ] **Step 1.1: `PingResult` + метод интерфейса**

В `src/OwnS3.Replication/Transport/IReplicationTransport.cs` добавить в конец интерфейса (после `ListingBucketsAsync`) и record перед интерфейсом:

```csharp
// Исход ping-пробы линка (глава 07 §2/§6): reachable — строго по 200 ping-грани;
// rttMs — при любом полученном HTTP-ответе (null при таймауте/обрыве транспорта).
public sealed record PingResult(bool Reachable, long? RttMs);

public interface IReplicationTransport
{
    // ... существующие 4 метода без изменений ...

    // GET /replication/ping → 200 без тела: лёгкая проба линка (глава 07 §2/§6).
    // timeout — per-target бюджет probe (глава 07 §11); внешняя отмена ct — проброс.
    Task<PingResult> PingAsync(TargetRecord target, TimeSpan timeout, CancellationToken ct);
}
```

- [ ] **Step 1.2: Заглушки во всех трёх фейках тестов**

В каждый фейк (реализации `IReplicationTransport` в юнит-тестах) добавить метод с заглушкой — по прецеденту существующих `NotSupportedException`-заглушек:

`src/tests/OwnS3.UnitTests/Replication/NotificationDispatcherTests.cs`, класс `FakeTransport`:

```csharp
    public Task<PingResult> PingAsync(TargetRecord target, TimeSpan timeout, CancellationToken ct) =>
        throw new NotSupportedException("notifier не пингует");
```

`src/tests/OwnS3.UnitTests/Replication/ReconcileRunnerTests.cs`, класс `FakeReconcileTransport`:

```csharp
    public Task<PingResult> PingAsync(TargetRecord target, TimeSpan timeout, CancellationToken ct) =>
        throw new NotSupportedException("reconcile не пингует");
```

`src/tests/OwnS3.UnitTests/Replication/ObjectFetcherTests.cs`, класс `FakeFetchTransport`:

```csharp
    public Task<PingResult> PingAsync(TargetRecord target, TimeSpan timeout, CancellationToken ct) =>
        throw new NotSupportedException("fetcher не пингует");
```

- [ ] **Step 1.3: Реализация `ReplicationHttpClient.PingAsync`**

В `src/OwnS3.Replication/Transport/ReplicationHttpClient.cs`: добавить `using System.Diagnostics;` в шапку и метод в класс (после `ListingBucketsAsync`, до секции «внутренняя механика»):

```csharp
    // GET /replication/ping (глава 07 §2/§6): лёгкая подписанная проба линка.
    // rtt — от старта запроса до статус-строки (ResponseHeadersRead); per-target
    // таймаут — linked CTS поверх общего таймаута HttpClient 10 с (глава 07 §2).
    public async Task<PingResult> PingAsync(TargetRecord target, TimeSpan timeout, CancellationToken ct)
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(timeout);
        var sw = Stopwatch.StartNew();
        try
        {
            using var request = BuildRequest(target, HttpMethod.Get, "/replication/ping", "",
                payload: null, payloadHash: null);
            using var response = await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead,
                timeoutCts.Token);
            sw.Stop();
            // reachable строго при валидном 200 ping-грани (глава 07 §2);
            // rttMs фиксируется при любом HTTP-ответе (включая не-200)
            return new PingResult(response.StatusCode == HttpStatusCode.OK,
                (long)sw.Elapsed.TotalMilliseconds);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            // Per-target таймаут probe — HTTP-ответа нет, rttMs отсутствует
            return new PingResult(false, null);
        }
        catch (HttpRequestException)
        {
            // Транспортный обрыв (connection refused/DNS) — rttMs отсутствует
            return new PingResult(false, null);
        }
        // Отмена внешнего ct — OperationCanceledException пробрасывается вызывающему
    }
```

- [ ] **Step 1.4: Проверка — сборка и существующие юниты репликации зелёные**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug`
Expected: BUILD SUCCEEDED (0 warnings — `TreatWarningsAsErrors`).

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~OwnS3.UnitTests.Replication"`
Expected: все тесты PASS (интерфейсная правка не сломала фейки/существующее поведение).

- [ ] **Step 1.5: Commit**

```bash
git add src/OwnS3.Replication/Transport/IReplicationTransport.cs src/OwnS3.Replication/Transport/ReplicationHttpClient.cs src/tests/OwnS3.UnitTests/Replication/NotificationDispatcherTests.cs src/tests/OwnS3.UnitTests/Replication/ReconcileRunnerTests.cs src/tests/OwnS3.UnitTests/Replication/ObjectFetcherTests.cs
git commit -m "t37: контракт PingAsync в IReplicationTransport (record PingResult(reachable, rttMs?)) + реализация ReplicationHttpClient: GET /replication/ping SigV4 с пустым payload-hash, per-target таймаут linked CTS поверх HttpClient 10с, rtt до статус-строки (ResponseHeadersRead), 200→true / не-200→false+rtt / обрыв-таймаут→false без rtt / внешняя отмена — проброс; заглушки PingAsync в трёх тестовых фейках"
```

---

## Task 2: `ProbeTimeoutMs` в `ReplicationOptions` (фаза 1.2 spec)

**Files:**
- Modify: `src/OwnS3.Replication/ReplicationOptions.cs`
- Test: `src/tests/OwnS3.UnitTests/Replication/ReplicationOptionsTests.cs`

**Interfaces:**
- Produces (для Tasks 3, 5): `public int ProbeTimeoutMs { get; set; } = 2000` в `ReplicationOptions` (секция `OwnS3:Replication`).

- [ ] **Step 2.1: Пишем падающие тесты**

В `src/tests/OwnS3.UnitTests/Replication/ReplicationOptionsTests.cs` добавить два кейса (по образцу существующих `ReconcileInterval_*`):

```csharp
    [Fact]
    public void ProbeTimeoutMs_Default_Is2000()
    {
        // Arrange/Act — дефолтный экземпляр (вне конфигурации)
        var options = new ReplicationOptions();

        // Assert — per-target таймаут ping-пробы 2000 мс (глава 07 §11)
        options.ProbeTimeoutMs.Should().Be(2000);
    }

    [Fact]
    public void ProbeTimeoutMs_BindsFromCanonicalConfigKey()
    {
        // Arrange — канонический ключ секции (тот, куда пишет env-трансляция
        // OWNS3_REPLICATION_PROBE_TIMEOUT_MS → OwnS3:Replication:ProbeTimeoutMs)
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection([new("OwnS3:Replication:ProbeTimeoutMs", "500")])
            .Build();
        var options = new ReplicationOptions();

        // Act
        config.Bind(ReplicationOptions.SectionName, options);

        // Assert — int-биндинг ключа работает, значение переопределено
        options.ProbeTimeoutMs.Should().Be(500);
    }
```

- [ ] **Step 2.2: Прогон — тесты падают**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~ReplicationOptionsTests"`
Expected: FAIL — «Property ProbeTimeoutMs does not exist» (ошибка компиляции теста).

- [ ] **Step 2.3: Реализация**

В `src/OwnS3.Replication/ReplicationOptions.cs` добавить свойство (после `FetchQueueMax`):

```csharp
    public int ProbeTimeoutMs { get; set; } = 2000;                        // per-target таймаут ping-пробы (§2)
```

- [ ] **Step 2.4: Прогон — тесты зелёные**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~ReplicationOptionsTests"`
Expected: PASS (все кейсы класса, включая 2 новых).

- [ ] **Step 2.5: Commit**

```bash
git add src/OwnS3.Replication/ReplicationOptions.cs src/tests/OwnS3.UnitTests/Replication/ReplicationOptionsTests.cs
git commit -m "t37: ReplicationOptions.ProbeTimeoutMs — per-target таймаут ping-пробы (OwnS3:Replication:ProbeTimeoutMs, дефолт 2000 по owns3/07 §11) + тесты дефолта и биндинга канонического ключа"
```

---

## Task 3: `ReplicationProber` — владелец логики probe (фаза 1.3 spec)

**Files:**
- Create: `src/OwnS3.Replication/ReplicationProber.cs`
- Test: `src/tests/OwnS3.UnitTests/Replication/ReplicationProberTests.cs` (новый файл)

**Interfaces:**
- Consumes: `ReplicationTargets` (`Snapshot`, `Active`), `IReplicationTransport.PingAsync(TargetRecord, TimeSpan, CancellationToken)` → `PingResult` (Task 1), `ReplicationOptions.ProbeTimeoutMs` (Task 2).
- Produces (для Task 5): в namespace `OwnS3.Replication`:
  - `public sealed class ReplicationProber(ReplicationTargets targets, IReplicationTransport transport, ReplicationOptions options)`
  - `Task<ProbeReport?> ProbeAsync(string? targetName, CancellationToken ct)` — **null = имя не из состава** (маркер 400 для хендлера); иначе отчёт.
  - `public sealed record ProbeReport(bool Active, IReadOnlyList<ProbeOutcome> Targets);`
  - `public sealed record ProbeOutcome(string Name, bool Reachable, long? RttMs);`

Решение по маркеру «не найден» (детализация spec §4.3 «фильтр по targetName (не найден — маркер для 400 хендлера)»): nullable-возврат — по прецеденту `SendNotificationsAsync → long?` (null = 429-случай). Успешный отчёт всегда объект, в т.ч. «соседей нет» → `ProbeReport(false, [])`.

- [ ] **Step 3.1: Пишем файл тестов целиком (фейк + 7 кейсов)**

Создать `src/tests/OwnS3.UnitTests/Replication/ReplicationProberTests.cs`:

```csharp
using System.Diagnostics;
using System.Net.Http;
using Microsoft.Extensions.Logging.Abstractions;
using OwnS3.Replication;
using OwnS3.Replication.Transport;

namespace OwnS3.UnitTests.Replication;

// Фейк-транспорт probe: per-имя исходы ping, управляемые исключения и задержка
// (кейсы параллельности); остальные операции грани — вне контекста probe.
public sealed class FakeProbeTransport : IReplicationTransport
{
    public Dictionary<string, PingResult> Results { get; } = new();
    public HashSet<string> Throwers { get; } = new();
    public TimeSpan Delay { get; set; } = TimeSpan.Zero;

    public Task<PingResult> PingAsync(TargetRecord target, TimeSpan timeout, CancellationToken ct)
    {
        if (Throwers.Contains(target.Name))
            throw new HttpRequestException("connection refused");
        if (Delay <= TimeSpan.Zero)
            return Task.FromResult(Results.GetValueOrDefault(target.Name, new PingResult(true, 1)));
        return Task.Run(async () =>
        {
            await Task.Delay(Delay, ct);
            return Results.GetValueOrDefault(target.Name, new PingResult(true, 1));
        }, ct);
    }

    public Task<long?> SendNotificationsAsync(TargetRecord t, NotificationBatchRequest b, CancellationToken ct) =>
        throw new NotSupportedException("probe не рассылает");

    public Task<FetchResult> FetchObjectAsync(TargetRecord t, string bucket, string key, CancellationToken ct) =>
        throw new NotSupportedException("probe не фетчит");

    public Task<ReplicationListingResponse> ListAsync(TargetRecord t, string bucket, string? marker,
        int maxKeys, CancellationToken ct) => throw new NotSupportedException("probe не листит");

    public Task<IReadOnlyList<(string Name, long CreationUnixMs)>> ListingBucketsAsync(TargetRecord t,
        CancellationToken ct) => throw new NotSupportedException("probe не листит");
}

// ReplicationProber (глава 07 §2): параллельный ping снапшота targets, фильтр
// одного соседа, гашение per-target исключений, маркер 400 неизвестного имени.
// Dispose чистит temp-каталоги targets.json (правило полной самоочистки тестов).
public class ReplicationProberTests : IDisposable
{
    private readonly List<string> _tempDirs = [];

    private (ReplicationProber Prober, FakeProbeTransport Transport) Prober(
        ReplicationOptions? options = null, params string[] names)
    {
        var sysDir = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));
        _tempDirs.Add(sysDir);
        var targets = OwnS3.Replication.ReplicationTargets.Load(sysDir,
            names.Select(n => new TargetOptions(n, $"http://{n}:9000", "ak-" + n, "sk-" + n)),
            NullLogger<OwnS3.Replication.ReplicationTargets>.Instance);
        var transport = new FakeProbeTransport();
        return (new ReplicationProber(targets, transport, options ?? new ReplicationOptions()), transport);
    }

    public void Dispose()
    {
        // Teardown: temp-каталоги targets.json удаляются при любом исходе
        foreach (var dir in _tempDirs)
            if (Directory.Exists(dir))
                Directory.Delete(dir, recursive: true);
    }

    [Fact]
    public async Task AllNeighborsReachable_200_RttPropagated()
    {
        // Arrange — два соседа, оба отвечают 200
        var (prober, transport) = Prober(names: ["peer-a", "peer-b"]);
        transport.Results["peer-a"] = new PingResult(true, 4);
        transport.Results["peer-b"] = new PingResult(true, 7);

        // Act
        var report = await prober.ProbeAsync(null, TestContext.Current.CancellationToken);

        // Assert — active=true, оба reachable, rttMs прокинуты из транспорта
        report.Should().NotBeNull();
        report!.Active.Should().BeTrue();
        report.Targets.Should().HaveCount(2);
        report.Targets.Should().Contain(o => o.Name == "peer-a" && o.Reachable && o.RttMs == 4);
        report.Targets.Should().Contain(o => o.Name == "peer-b" && o.Reachable && o.RttMs == 7);
    }

    [Fact]
    public async Task Non200Neighbor_ReachableFalseWithRtt()
    {
        // Arrange — сосед отвечает HTTP, но не 200 (403/400 — линк не работает)
        var (prober, transport) = Prober(names: ["peer-a"]);
        transport.Results["peer-a"] = new PingResult(false, 12);

        // Act
        var report = await prober.ProbeAsync(null, TestContext.Current.CancellationToken);

        // Assert — reachable=false, rttMs присутствует («отвечает, но не пускает»)
        report!.Targets.Single().Reachable.Should().BeFalse();
        report.Targets.Single().RttMs.Should().Be(12);
    }

    [Fact]
    public async Task TransportError_ReachableFalseWithoutRtt()
    {
        // Arrange — транспортное исключение (обрыв/refused)
        var (prober, transport) = Prober(names: ["peer-a"]);
        transport.Throwers.Add("peer-a");

        // Act
        var report = await prober.ProbeAsync(null, TestContext.Current.CancellationToken);

        // Assert — исключение погашено веткой: reachable=false, rttMs отсутствует
        report!.Targets.Single().Reachable.Should().BeFalse();
        report.Targets.Single().RttMs.Should().BeNull();
    }

    [Fact]
    public async Task TargetFilter_ExactlyOneOutcome()
    {
        // Arrange — три соседа; фильтр по имени peer-b
        var (prober, _) = Prober(names: ["peer-a", "peer-b", "peer-c"]);

        // Act
        var report = await prober.ProbeAsync("peer-b", TestContext.Current.CancellationToken);

        // Assert — ровно один исход, ровно запрошенный сосед
        report!.Targets.Should().ContainSingle(o => o.Name == "peer-b");
        report.Active.Should().BeTrue();
    }

    [Fact]
    public async Task UnknownTargetName_NullMarkerFor400()
    {
        // Arrange — имя вне состава
        var (prober, _) = Prober(names: ["peer-a"]);

        // Act
        var report = await prober.ProbeAsync("ghost", TestContext.Current.CancellationToken);

        // Assert — null-маркер: хендлер отвечает 400 (глава 07 §2)
        report.Should().BeNull();
    }

    [Fact]
    public async Task EmptySnapshot_InactiveEmptyTargets()
    {
        // Arrange — соседей нет (пустой seed)
        var (prober, _) = Prober();

        // Act
        var report = await prober.ProbeAsync(null, TestContext.Current.CancellationToken);

        // Assert — осмысленный «соседей нет»: active=false, пустой список
        report!.Active.Should().BeFalse();
        report.Targets.Should().BeEmpty();
    }

    [Fact]
    public async Task ParallelPings_DurationIsMaxNotSum()
    {
        // Arrange — три соседа, каждый пинг 200 мс (через задержку фейка)
        var (prober, transport) = Prober(names: ["peer-a", "peer-b", "peer-c"]);
        transport.Delay = TimeSpan.FromMilliseconds(200);
        var sw = Stopwatch.StartNew();

        // Act
        var report = await prober.ProbeAsync(null, TestContext.Current.CancellationToken);
        sw.Stop();

        // Assert — параллельность: ответ probe ≈ max per-target, не sum
        // (3×200 мс последовательно = 600 мс; бюджет с запасом на планировщик)
        report!.Targets.Should().HaveCount(3);
        sw.ElapsedMilliseconds.Should().BeLessThan(500,
            "пинги идут параллельно: задержка ответа = max, не sum (глава 07 §2)");
    }
}
```

- [ ] **Step 3.2: Прогон — падают (класс не существует)**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~ReplicationProberTests"`
Expected: FAIL — ошибка компиляции: `ReplicationProber` не найден.

- [ ] **Step 3.3: Реализация `ReplicationProber`**

Создать `src/OwnS3.Replication/ReplicationProber.cs`:

```csharp
using OwnS3.Replication.Transport;

namespace OwnS3.Replication;

// Активная проба репликационных линков (глава 07 §2): админ-trigger probe —
// параллельный ping всех (или одного) соседей снапшота targets исходящим
// транспортом (креды/маршрут notifier'а) МИМО outbox: журнал, курсоры,
// fetch-очереди, счётчики и link_online probe не трогает. Отдельный класс —
// тестируем без хоста (по прецеденту ReplicationMaintenance).
public sealed class ReplicationProber(ReplicationTargets targets,
    IReplicationTransport transport, ReplicationOptions options)
{
    // Отчёт probe: active — ≥1 сосед в составе. null-возврат — targetName не из
    // состава: маркер 400 для хендлера (глава 07 §2).
    public async Task<ProbeReport?> ProbeAsync(string? targetName, CancellationToken ct)
    {
        var snapshot = targets.Snapshot;
        if (targetName is not null && snapshot.All(t => t.Name != targetName))
            return null;
        IReadOnlyList<TargetRecord> selected = targetName is null
            ? snapshot
            : snapshot.Where(t => t.Name == targetName).ToList();
        var outcomes = await Task.WhenAll(selected.Select(t => PingQuietAsync(t, ct)));
        return new ProbeReport(selected.Count > 0, outcomes);
    }

    // Ветка ping одного соседа: гасит свои транспортные исключения (probe
    // отвечает за все цели); внешняя отмена ct — проброс наверх.
    private async Task<ProbeOutcome> PingQuietAsync(TargetRecord target, CancellationToken ct)
    {
        try
        {
            var result = await transport.PingAsync(target,
                TimeSpan.FromMilliseconds(options.ProbeTimeoutMs), ct);
            return new ProbeOutcome(target.Name, result.Reachable, result.RttMs);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch
        {
            // Обрыв/таймаут пер-цели — исход false без rttMs (глава 07 §2)
            return new ProbeOutcome(target.Name, false, null);
        }
    }
}

// Отчёт probe (глава 07 §2): active = «есть соседи», targets — per-target исходы.
public sealed record ProbeReport(bool Active, IReadOnlyList<ProbeOutcome> Targets);

// Исход ping одного соседа: reachable строго по 200; rttMs — при любом
// HTTP-ответе (null при таймауте/обрыве).
public sealed record ProbeOutcome(string Name, bool Reachable, long? RttMs);
```

- [ ] **Step 3.4: Прогон — зелёные**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~ReplicationProberTests"`
Expected: PASS 7/7.

- [ ] **Step 3.5: Commit**

```bash
git add src/OwnS3.Replication/ReplicationProber.cs src/tests/OwnS3.UnitTests/Replication/ReplicationProberTests.cs
git commit -m "t37: ReplicationProber — параллельный ping снапшота targets (Task.WhenAll, задержка = max не sum), фильтр ?target=, null-маркер неизвестного имени (400 хендлера), гашение per-target исключений транспорта, ProbeReport(active, targets)/ProbeOutcome(name, reachable, rttMs?); юнит-тесты 7/7 (200/не-200/обрыв/фильтр/неизвестное имя/пустой состав/параллельность)"
```

---

## Task 4: Грань `GET /replication/ping` приёмника (фаза 2.1 spec)

**Files:**
- Modify: `src/OwnS3.App/Handlers/ReplicationHandlers.cs` (новый `HandlePingAsync`)
- Modify: `src/OwnS3.App/Pipeline/S3Middleware.cs` (маршрут replication-ветки)
- Test: `src/tests/OwnS3.IntegrationTests/Api/ReplicationScenarios.cs` (3 новых кейса + purity-кейс)

**Interfaces:**
- Consumes: конвейер replication-грани middleware (auth SigV4 → политика строго Replication → резолв `sourceName` по accessKey) — ЕДИНЫЙ, без изъятий.
- Produces: `public Task HandlePingAsync(HttpContext context)` в `ReplicationHandlers` — 200, пустое тело.

- [ ] **Step 4.1: Пишем падающие интеграционные кейсы**

В `src/tests/OwnS3.IntegrationTests/Api/ReplicationScenarios.cs` добавить в класс `ReplicationScenarios` четыре кейса (используют существующие поля `Client`, `ReplicationClient`, `ReplicationCreds`, `OrphanReplicationCreds`, хелпер `PayloadHash`):

```csharp
    [Fact]
    public async Task Ping_ValidNeighborSignature_200EmptyBody()
    {
        // Act — GET /replication/ping подписью соседа (accessKey peer-a = ReplicationCreds)
        var response = await ReplicationClient.SendSignedAsync("GET", "/replication/ping",
            credentials: ReplicationCreds);

        // Assert — 200, пустое тело (глава 07 §6)
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        (await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .Should().BeEmpty();
    }

    [Fact]
    public async Task Ping_PolicyIsolation_403()
    {
        // Act / Assert — read-only/read-write/admin на ping-грани → 403 (A13)
        foreach (var creds in new[] { OwnS3TestClient.Reader(), OwnS3TestClient.Writer(), OwnS3TestClient.Admin() })
            (await Client.SendSignedAsync("GET", "/replication/ping", credentials: creds))
                .StatusCode.Should().Be(HttpStatusCode.Forbidden);

        // Act / Assert — replication-ключ, НЕ числящийся соседом → 403 (глава 07 §6)
        (await ReplicationClient.SendSignedAsync("GET", "/replication/ping",
            credentials: OrphanReplicationCreds)).StatusCode.Should().Be(HttpStatusCode.Forbidden);
    }

    [Fact]
    public async Task Ping_NonGetMethods_400_UnknownPath_400()
    {
        // Act — POST/PUT ping + GET неизвестного пути грани (регресс маршрутизации)
        var post = await ReplicationClient.SendSignedAsync("POST", "/replication/ping",
            payloadString: PayloadHash(string.Empty), credentials: ReplicationCreds);
        var put = await ReplicationClient.SendSignedAsync("PUT", "/replication/ping",
            payloadString: PayloadHash(string.Empty), credentials: ReplicationCreds);
        var unknown = await ReplicationClient.SendSignedAsync("GET", "/replication/no-such",
            credentials: ReplicationCreds);

        // Assert — 400 InvalidArgument по существующему правилу грани (глава 07 §6)
        post.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        put.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        unknown.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Ping_ReceiverPurity_StatusCountersUnchanged()
    {
        // Arrange — стабильный срез статуса: счётчики/курсоры/fetch-очереди
        // (время-зависимые deliveryLagSeconds/fetchLagSeconds исключены)
        async Task<string> StableNeighborsAsync()
        {
            var response = await Client.SendSignedAsync("GET", "/admin/replication/status",
                credentials: OwnS3TestClient.Admin());
            var json = JsonDocument.Parse(
                await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement;
            return System.Text.Json.JsonSerializer.Serialize(json.GetProperty("neighbors")
                .EnumerateArray().Select(n => new
                {
                    name = n.GetProperty("name").GetString(),
                    acceptedSeqNo = n.GetProperty("acceptedSeqNo").GetInt64(),
                    queueDepth = n.GetProperty("queueDepth").GetInt64(),
                    fetchQueueDepth = n.GetProperty("fetchQueueDepth").GetInt64(),
                    fetchPaused = n.GetProperty("fetchPaused").GetBoolean(),
                    reconcileRunning = n.GetProperty("reconcileRunning").GetBoolean(),
                    counters = n.GetProperty("counters").EnumerateObject()
                        .ToDictionary(p => p.Name, p => p.Value.GetRawText()),
                }));
        }

        // Act — серия ping-запросов к узлу-приёмнику (принцип 3: ping бесплатен)
        var before = await StableNeighborsAsync();
        for (var i = 0; i < 10; i++)
        {
            var ping = await ReplicationClient.SendSignedAsync("GET", "/replication/ping",
                credentials: ReplicationCreds);
            ping.StatusCode.Should().Be(HttpStatusCode.OK);
        }

        // Assert — состояние приёмника байт-в-байт то же: срезы до/после идентичны
        var after = await StableNeighborsAsync();
        after.Should().Be(before);
    }
```

- [ ] **Step 4.2: Прогон — кейсы падают (грань ещё не маршрутизируется)**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~ReplicationScenarios"`
Expected: FAIL — `Ping_ValidNeighborSignature_200EmptyBody` и `Ping_ReceiverPurity_StatusCountersUnchanged` получают 400 «Unknown replication resource» (маршрут ещё не добавлен); `Ping_PolicyIsolation_403` и `Ping_NonGetMethods_400_UnknownPath_400` фиксируют СУЩЕСТВУЮЩЕЕ поведение конвейера (403 политики/400 маршрутизации работают до правки) и могут быть зелёными — их ценность в регресс-фиксации. Существующие кейсы класса PASS.

- [ ] **Step 4.3: `HandlePingAsync` в `ReplicationHandlers`**

В `src/OwnS3.App/Handlers/ReplicationHandlers.cs` добавить метод (после `HandleListingAsync`):

```csharp
    // GET /replication/ping (глава 07 §6): 200 без тела — активная проба линка;
    // приёмник ничего не пишет и не применяет (auth/политика/резолв соседа —
    // конвейер S3Middleware, источник имени не используется).
    public Task HandlePingAsync(HttpContext context)
    {
        context.Response.StatusCode = StatusCodes.Status200OK;
        return Task.CompletedTask;
    }
```

- [ ] **Step 4.4: Маршрут в `S3Middleware` (replication-ветка)**

В `src/OwnS3.App/Pipeline/S3Middleware.cs`, в replication-ветке (шаг 2.4), добавить ping-ветку ПОСЛЕ ветки `/replication/listing` и ДО финального `else` («Unknown replication resource»):

```csharp
                    else if (HttpMethods.IsGet(method) && context.Request.Path.Value == "/replication/ping")
                        await replicationHandlers.HandlePingAsync(context);
```

Конвейер (auth → политика строго Replication → резолв sourceName) остаётся единым — ping-ветка встраивается в существующую цепочку диспетчеризации, ничего не вынося до проверок.

- [ ] **Step 4.5: Прогон — кейсы зелёные**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~ReplicationScenarios"`
Expected: PASS — весь класс, включая 4 новых кейса.

- [ ] **Step 4.6: Commit**

```bash
git add src/OwnS3.App/Handlers/ReplicationHandlers.cs src/OwnS3.App/Pipeline/S3Middleware.cs src/tests/OwnS3.IntegrationTests/Api/ReplicationScenarios.cs
git commit -m "t37: грань GET /replication/ping — HandlePingAsync (200 без тела, приёмник состояние не меняет) + маршрут replication-ветки S3Middleware (единый конвейер auth/политика/резолв соседа); интеграционные кейсы: подпись соседа 200-пустое, 403 read-write/admin и не-соседа, POST/PUT/неизвестный путь 400, приёмник-чистота (status до/после серии ping — счётчики/курсоры/fetch-очереди неизменны)"
```

---

## Task 5: Админ-триггер `POST /admin/replication/probe` (фаза 2.2 spec)

**Files:**
- Modify: `src/OwnS3.App/Handlers/ReplicationAdminHandlers.cs` (конструктор + `HandleProbeAsync` + DTO)
- Modify: `src/OwnS3.App/Pipeline/S3Middleware.cs` (admin-ветка: список ресурсов + диспетчеризация)
- Modify: `src/OwnS3.App/Program.cs` (env-скаляр + DI `ReplicationProber`)
- Modify: `src/tests/OwnS3.IntegrationTests/Api/ReplicationScenarios.cs` (под-фабрика заниженного таймаута — рядом с родителем `ReplicationAppFactory`, по прецеденту `ReplicationSmallQueueAppFactory`)
- Test: `src/tests/OwnS3.IntegrationTests/Api/ReplicationAdminScenarios.cs` (4 новых кейса)

**Interfaces:**
- Consumes: `ReplicationProber.ProbeAsync(string?, CancellationToken)` → `ProbeReport?` (Task 3), `ProbeOutcome(string Name, bool Reachable, long? RttMs)`.
- Produces: `public async Task HandleProbeAsync(HttpContext context)` в `ReplicationAdminHandlers`; конструктор класса расширяется параметром `OwnS3.Replication.ReplicationProber prober`.

- [ ] **Step 5.1: Пишем падающие интеграционные кейсы**

Сначала в `src/tests/OwnS3.IntegrationTests/Api/ReplicationScenarios.cs`, рядом с `ReplicationSmallQueueAppFactory` (под-фабрика рядом с родителем, тот же namespace `OwnS3.IntegrationTests.Api`), добавить фабрику заниженного probe-таймаута:

```csharp
// Фабрика с заниженным probe-таймаутом: недостижимые соседи 127.0.0.1:9
// закрываются за 200 мс — быстрый тест-таймаут (глава 07 §2/§11).
public sealed class ReplicationProbeFastAppFactory : ReplicationAppFactory
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        base.ConfigureWebHost(builder);
        builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["OwnS3:Replication:ProbeTimeoutMs"] = "200",
        }));
    }
}
```

Затем в `src/tests/OwnS3.IntegrationTests/Api/ReplicationAdminScenarios.cs` добавить в класс `ReplicationAdminScenarios` четыре кейса (кейсы с зависимостью от состава targets поднимают СОБСТВЕННЫЙ хост — кейсы класса `PutTargets_*` мутируют состав общей фикстуры, порядок тестов xUnit не детерминирован):

```csharp
    [Fact]
    public async Task Probe_NoTargets_Inactive200()
    {
        // Arrange — чистый хост без соседей (базовая фабрика: targets пуст)
        using var factory = new OwnS3AppFactory();
        var client = new OwnS3TestClient(factory.CreateClient());

        // Act
        var response = await client.SendSignedAsync("POST", "/admin/replication/probe",
            payloadString: PayloadHash(string.Empty), credentials: OwnS3TestClient.Admin());

        // Assert — осмысленный «соседей нет», не ошибка (глава 07 §2)
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var json = JsonDocument.Parse(
            await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement;
        json.GetProperty("active").GetBoolean().Should().BeFalse();
        json.GetProperty("targets").GetArrayLength().Should().Be(0);
    }

    [Fact]
    public async Task Probe_UnreachableNeighbors_FalseWithoutRtt()
    {
        // Arrange — собственный хост: все соседи фабрики недостижимы (127.0.0.1:9),
        // per-target таймаут занижен до 200 мс
        using var factory = new ReplicationProbeFastAppFactory();
        var client = new OwnS3TestClient(factory.CreateClient());

        // Act
        var response = await client.SendSignedAsync("POST", "/admin/replication/probe",
            payloadString: PayloadHash(string.Empty), credentials: OwnS3TestClient.Admin());

        // Assert — 200; active=true (соседи в составе); все reachable=false БЕЗ rttMs
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var json = JsonDocument.Parse(
            await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement;
        json.GetProperty("active").GetBoolean().Should().BeTrue();
        var targets = json.GetProperty("targets").EnumerateArray().ToList();
        targets.Should().HaveCount(3);
        foreach (var target in targets)
        {
            target.GetProperty("name").GetString().Should().NotBeNullOrEmpty();
            target.GetProperty("reachable").GetBoolean().Should().BeFalse();
            target.TryGetProperty("rttMs", out _).Should().BeFalse(
                "таймаут/обрыв транспорта — rttMs отсутствует (глава 07 §2)");
        }
    }

    [Fact]
    public async Task Probe_TargetFilter_Single_UnknownTarget_400()
    {
        // Arrange — собственный хост с сид-составом фабрики (peer-a/b/c)
        using var factory = new ReplicationAppFactory();
        var client = new OwnS3TestClient(factory.CreateClient());

        // Act — фильтр ровно одного соседа
        var single = await client.SendSignedAsync("POST", "/admin/replication/probe?target=peer-b",
            payloadString: PayloadHash(string.Empty), credentials: OwnS3TestClient.Admin());

        // Assert — один исход, имя запрошенного соседа (сосед недостижим — не важно:
        // кейс про форму фильтра, не про достижимость)
        single.StatusCode.Should().Be(HttpStatusCode.OK);
        var json = JsonDocument.Parse(
            await single.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement;
        json.GetProperty("active").GetBoolean().Should().BeTrue();
        json.GetProperty("targets").EnumerateArray().Should().ContainSingle()
            .Which.GetProperty("name").GetString().Should().Be("peer-b");

        // Act — имя не из состава
        var unknown = await client.SendSignedAsync("POST", "/admin/replication/probe?target=ghost",
            payloadString: PayloadHash(string.Empty), credentials: OwnS3TestClient.Admin());

        // Assert — 400 с пояснением в JSON (глава 07 §2)
        unknown.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await unknown.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .Should().Contain("ghost");
    }

    [Fact]
    public async Task Probe_NonAdmin_403()
    {
        // Act / Assert — replication-ключ и writer на probe → 403 (A13-зеркало;
        // 403 даёт конвейер admin-ветки до хендлера — состав роли не играет)
        foreach (var creds in new[]
                 {
                     new OwnS3TestClient.Credentials(OwnS3AppFactory.ReplicationAccessKey,
                         OwnS3AppFactory.ReplicationSecretKey),
                     OwnS3TestClient.Writer(),
                 })
            (await Client.SendSignedAsync("POST", "/admin/replication/probe",
                payloadString: PayloadHash(string.Empty), credentials: creds))
                .StatusCode.Should().Be(HttpStatusCode.Forbidden);
    }
```

- [ ] **Step 5.2: Прогон — кейсы падают**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~ReplicationAdminScenarios"`
Expected: FAIL — все 4 новых кейса получают 400 «Unknown admin resource» (ресурс ещё не в списке admin-ветки; `Probe_NonAdmin_403` ждёт 403, получает 400); существующие кейсы класса PASS.

- [ ] **Step 5.3: `HandleProbeAsync` + конструктор + DTO в `ReplicationAdminHandlers`**

В `src/OwnS3.App/Handlers/ReplicationAdminHandlers.cs`:

1. В шапку добавить `using System.Text.Json.Serialization;`.
2. Primary-конструктор класса расширить последним параметром:

```csharp
public sealed class ReplicationAdminHandlers(OwnS3.Replication.ReplicationTargets targets,
    OwnS3.Replication.Cursors.TargetCursors cursors, OutboxJournal journal,
    OwnS3.Replication.ReplicationMetrics metrics, FetchQueueRegistry fetchQueues,
    ReconcileRunner reconcile, IReplicaStore store, OwnS3.Replication.ReplicationOptions options,
    OwnS3.Replication.ReplicationProber prober)
```

3. Добавить метод (после `HandleReconcileAsync`) и приватные DTO (рядом с `PutTargetsRequest`):

```csharp
    // POST /admin/replication/probe?target=<name> (глава 07 §2): активная проба
    // линков — СИНХРОННЫЙ ответ (не 202/фон — в отличие от reconcile); без
    // target — все соседи, неизвестное имя — 400; неактивная репликация —
    // осмысленный {active:false, targets:[]} (не ошибка).
    public async Task HandleProbeAsync(HttpContext context)
    {
        var targetName = context.Request.Query["target"].FirstOrDefault();
        var report = await prober.ProbeAsync(string.IsNullOrEmpty(targetName) ? null : targetName,
            context.RequestAborted);
        if (report is null)
        {
            // Имя не из состава — 400 с пояснением (глава 07 §2)
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context.Response.WriteAsJsonAsync(
                new { error = $"unknown replication target: {targetName}" }, context.RequestAborted);
            return;
        }
        context.Response.StatusCode = StatusCodes.Status200OK;
        await context.Response.WriteAsJsonAsync(
            new ProbeResponse(report.Active,
                report.Targets.Select(o => new ProbeTargetDto(o.Name, o.Reachable, o.RttMs)).ToList()),
            Json, context.RequestAborted);
    }
```

```csharp
    // rttMs присутствует только при полученном HTTP-ответе (WhenWritingNull).
    private sealed record ProbeResponse(bool Active, IReadOnlyList<ProbeTargetDto> Targets);
    private sealed record ProbeTargetDto(string Name, bool Reachable,
        [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] long? RttMs);
```

- [ ] **Step 5.4: Маршрут в `S3Middleware` (admin-ветка)**

В `src/OwnS3.App/Pipeline/S3Middleware.cs`, шаг 2.5:

1. Список допустимых ресурсов — расширить:

```csharp
                        var isAdminRepl = replAdminPath is "/admin/replication/targets"
                            or "/admin/replication/status"
                            or "/admin/replication/reconcile"
                            or "/admin/replication/sync-report"
                            or "/admin/replication/probe";
```

2. Диспетчеризация — добавить ветку после `HandleSyncReportAsync` (до финального `else`):

```csharp
                        else if (HttpMethods.IsPost(method) && replAdminPath == "/admin/replication/probe")
                            await replicationAdminHandlers.HandleProbeAsync(context);
```

- [ ] **Step 5.5: env-скаляр + DI в `Program.cs`**

В `src/OwnS3.App/Program.cs`:

1. В массив env-трансляции `OWNS3_REPLICATION_*` добавить строку (после `FETCH_QUEUE_MAX`):

```csharp
             ("OWNS3_REPLICATION_PROBE_TIMEOUT_MS", "OwnS3:Replication:ProbeTimeoutMs"),
```

2. Перед регистрацией `ReplicationAdminHandlers` (блок «Админ-грань mesh-репликации») добавить DI prober'а и дополнить регистрацию хендлеров параметром:

```csharp
// Активная проба линков mesh (глава 07 §2): ping соседей исходящим транспортом
// (креды/маршрут notifier'а) мимо outbox; прав админ-грани достаточно.
builder.Services.AddSingleton(sp => new OwnS3.Replication.ReplicationProber(
    sp.GetRequiredService<OwnS3.Replication.ReplicationTargets>(),
    ReplicationTransportFactory(sp),
    sp.GetRequiredService<IOptions<OwnS3.Replication.ReplicationOptions>>().Value));
// Админ-грань mesh-репликации: targets/status/reconcile/sync-report/probe
builder.Services.AddSingleton(sp => new OwnS3.App.Handlers.ReplicationAdminHandlers(
    sp.GetRequiredService<OwnS3.Replication.ReplicationTargets>(),
    sp.GetRequiredService<OwnS3.Replication.Cursors.TargetCursors>(),
    sp.GetRequiredService<OwnS3.Replication.Outbox.OutboxJournal>(),
    sp.GetRequiredService<OwnS3.Replication.ReplicationMetrics>(),
    sp.GetRequiredService<OwnS3.Replication.Inbound.FetchQueueRegistry>(),
    sp.GetRequiredService<OwnS3.Replication.Reconcile.ReconcileRunner>(),
    sp.GetRequiredService<OwnS3.Storage.IReplicaStore>(),
    sp.GetRequiredService<IOptions<OwnS3.Replication.ReplicationOptions>>().Value,
    sp.GetRequiredService<OwnS3.Replication.ReplicationProber>()));
```

- [ ] **Step 5.6: Прогон — кейсы зелёные**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~ReplicationAdminScenarios"`
Expected: PASS — весь класс, включая 4 новых кейса.

- [ ] **Step 5.7: Регресс — соседние Api-сценарии не задеты**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~OwnS3.IntegrationTests.Api"`
Expected: PASS — вся серия (включая `AccessScenarios`, `RoutingScenarios`, `MetricsHealthScenarios`, `MetricsReplicationScenarios`, `AdminScenarios` — Existing-регресс, критерий приёмки 6).

- [ ] **Step 5.8: Commit**

```bash
git add src/OwnS3.App/Handlers/ReplicationAdminHandlers.cs src/OwnS3.App/Pipeline/S3Middleware.cs src/OwnS3.App/Program.cs src/tests/OwnS3.IntegrationTests/Api/ReplicationScenarios.cs src/tests/OwnS3.IntegrationTests/Api/ReplicationAdminScenarios.cs
git commit -m "t37: админ-триггер POST /admin/replication/probe — HandleProbeAsync (?target= фильтр одного, неизвестное имя 400 с пояснением, неактивная репликация {active:false,targets:[]}, синхронный JSON {active, targets:[{name, reachable, rttMs?}]} — rttMs опускается при null), маршрут admin-ветки S3Middleware, env OWNS3_REPLICATION_PROBE_TIMEOUT_MS → OwnS3:Replication:ProbeTimeoutMs, DI ReplicationProber (singleton, транспорт фабрикой координатора); интеграционные кейсы: без соседей 200, недостижимые соседи reachable=false без rttMs за заниженный таймаут (под-фабрика ProbeFast 200 мс), фильтр одного/400 неизвестного имени, не-admin 403; кейсы с зависимостью от состава — на собственных хостах (изоляция от мутирующих PutTargets-кейсов)"
```

---

## Task 6: E2E mesh — `ProbeScenarios` (фаза 3 spec)

**Files:**
- Modify: `src/tests/OwnS3.IntegrationTests/E2e/Replication/MeshClients.cs` (хелпер `AdminProbeAsync`)
- Test/Create: `src/tests/OwnS3.IntegrationTests/E2e/Replication/ProbeScenarios.cs`

**Interfaces:**
- Consumes: `OwnS3MeshE2eEnvironment.RunScenarioAsync(slug, nodes, body, fullMeshTargets, ct)` (свой guid-контур, teardown, телеметрия — каноны `docs/e2e-isolation.md`/`docs/e2e-launch.md` уже внутри), `MeshClients.AdminClient/AdminPutTargetsAsync/PollUntilAsync`, `DockerTrait.SkipIfUnavailable`, коллекция `OwnS3MeshE2eCollection` (DisableParallelization).
- Produces: `MeshClients.AdminProbeAsync(env, node, target?, ct)` → `(HttpStatusCode Status, string Body)`.

- [ ] **Step 6.1: Хелпер probe в `MeshClients`**

В `src/tests/OwnS3.IntegrationTests/E2e/Replication/MeshClients.cs` добавить (рядом с `AdminGetStatusAsync`):

```csharp
    /// <summary>POST /admin/replication/probe узла (статус + JSON-строка;
    /// target — фильтр одного соседа, null — все).</summary>
    public static async Task<(HttpStatusCode Status, string Body)> AdminProbeAsync(
        OwnS3MeshE2eEnvironment env, int node, string? target, CancellationToken ct)
    {
        // query — ОТДЕЛЬНЫМ параметром (участвует в canonical query подписи)
        var response = await AdminClient(env, node).SendAsync("POST", "/admin/replication/probe",
            target is null ? "" : $"target={Uri.EscapeDataString(target)}", null, ct);
        return (response.StatusCode, await response.Content.ReadAsStringAsync(ct));
    }
```

В шапку файла при необходимости добавить `using System.Net;` (уже есть — строка 1).

- [ ] **Step 6.2: Сценарный класс `ProbeScenarios` — 4 кейса**

Создать `src/tests/OwnS3.IntegrationTests/E2e/Replication/ProbeScenarios.cs`:

```csharp
using System.Diagnostics;
using System.Net;
using System.Text.Json;
using FluentAssertions;
using Xunit;

namespace OwnS3.IntegrationTests.E2e.Replication;

/// <summary>
/// Активная проба репликационных линков (t37, глава 07 §2/§6): живой mesh —
/// все reachable + rttMs; остановка соседа — false без rttMs, возврат — true;
/// закрытый порт — false в пределах таймаута; без соседей — {active:false}.
/// Каждый кейс — собственный guid-контур RunScenarioAsync (изоляция/teardown/
/// телеметрия — каноны docs/e2e-isolation.md, docs/e2e-launch.md).
/// </summary>
[Collection(OwnS3MeshE2eCollection.Name)]
public sealed class ProbeScenarios
{
    [Fact]
    public async Task Probe_LiveMesh_AllReachable_TargetFilter_Matrix()
    {
        DockerTrait.SkipIfUnavailable();
        // Контур 3 узла, полный mesh (env-seed): probe всех нод — матрица в обе стороны
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("probe-live", 3, async env =>
        {
            var ct = TestContext.Current.CancellationToken;
            // Act / Assert — probe КАЖДОЙ ноды: все соседи reachable=true с rttMs
            // (покрытие пар в обе стороны — модель потребления зонда t33)
            for (var i = 0; i < env.Nodes; i++)
            {
                var (status, body) = await MeshClients.AdminProbeAsync(env, i, null, ct);
                status.Should().Be(HttpStatusCode.OK);
                var json = JsonDocument.Parse(body).RootElement;
                json.GetProperty("active").GetBoolean().Should().BeTrue();
                var targets = json.GetProperty("targets").EnumerateArray().ToList();
                targets.Should().HaveCount(env.Nodes - 1);
                foreach (var target in targets)
                {
                    target.GetProperty("reachable").GetBoolean().Should().BeTrue(
                        $"линк {env.NodeName(i)}→{target.GetProperty("name").GetString()} жив");
                    target.GetProperty("rttMs").GetInt64().Should().BeGreaterThanOrEqualTo(0);
                }
            }

            // Act — фильтр одного соседа на ноде 0
            var (singleStatus, singleBody) = await MeshClients.AdminProbeAsync(env, 0, env.NodeName(1), ct);

            // Assert — ровно один исход, ровно запрошенный сосед
            singleStatus.Should().Be(HttpStatusCode.OK);
            var single = JsonDocument.Parse(singleBody).RootElement;
            single.GetProperty("targets").EnumerateArray().Should().ContainSingle()
                .Which.GetProperty("name").GetString().Should().Be(env.NodeName(1));
        }, ct: TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Probe_StoppedNeighbor_False_RecoversOnStart()
    {
        DockerTrait.SkipIfUnavailable();
        // Контур 2 узла, полный mesh: остановка контейнера соседа → reachable=false
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("probe-stop", 2, async env =>
        {
            var ct = TestContext.Current.CancellationToken;

            // Arrange — до остановки линк жив
            var before = JsonDocument.Parse((await MeshClients.AdminProbeAsync(env, 0, null, ct)).Body)
                .RootElement;
            before.GetProperty("targets").EnumerateArray().Single()
                .GetProperty("reachable").GetBoolean().Should().BeTrue();

            // Act — docker stop соседа
            await env.StopNodeAsync(1, ct: ct);
            var stopped = JsonDocument.Parse((await MeshClients.AdminProbeAsync(env, 0, null, ct)).Body)
                .RootElement;

            // Assert — сосед unreachable, rttMs отсутствует (обрыв — канал не отвечает)
            var outcome = stopped.GetProperty("targets").EnumerateArray().Single();
            outcome.GetProperty("reachable").GetBoolean().Should().BeFalse();
            outcome.TryGetProperty("rttMs", out _).Should().BeFalse(
                "транспортный обрыв — rttMs отсутствует (глава 07 §2)");

            // Act — возврат контейнера (поллинг healthz ≤1 с на попытку)
            await env.StartNodeAsync(1, ct);
            await MeshClients.PollUntilAsync(async () =>
            {
                using var http = new HttpClient
                {
                    BaseAddress = new Uri(env.NodeHostUrl(1)),
                    Timeout = TimeSpan.FromSeconds(1),
                };
                try
                {
                    return (await http.GetAsync("/healthz", ct)).StatusCode == HttpStatusCode.OK;
                }
                catch
                {
                    return false;
                }
            }, "n1 поднялся после остановки");

            // Assert — канал жив снова (доставка не оценивается — только линк)
            var after = JsonDocument.Parse((await MeshClients.AdminProbeAsync(env, 0, null, ct)).Body)
                .RootElement;
            after.GetProperty("targets").EnumerateArray().Single()
                .GetProperty("reachable").GetBoolean().Should().BeTrue();
        }, ct: TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Probe_ClosedPortNeighbor_Fast_UnknownTarget_400()
    {
        DockerTrait.SkipIfUnavailable();
        // Контур 2 узла, полный mesh; состав n0 заменяется соседом с закрытым портом
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("probe-closed", 2, async env =>
        {
            var ct = TestContext.Current.CancellationToken;

            // Arrange — PUT targets на n0: сосед «closed-port» на закрытый порт живого
            // контейнера (порт 1 ничего не слушает — connection refused). Тело собираем
            // вручную: AdminPutTargetsAsync парсит индекс узла из имени соседа.
            var body = JsonSerializer.Serialize(new
            {
                targets = new[]
                {
                    new
                    {
                        name = "closed-port",
                        endpoint = $"http://{env.NodeName(1)}:1",
                        accessKey = OwnS3MeshE2eEnvironment.PeerPairAccessKeyOf(0, 1),
                        secretKey = OwnS3MeshE2eEnvironment.PeerPairSecretKeyOf(0, 1),
                    },
                },
            });
            var put = await MeshClients.AdminClient(env, 0).SendAsync(
                "PUT", "/admin/replication/targets", "", body, ct);
            put.StatusCode.Should().Be(HttpStatusCode.Accepted);

            // Act — probe n0 (дефолтный per-target таймаут 2000 мс)
            var sw = Stopwatch.StartNew();
            var (status, probeBody) = await MeshClients.AdminProbeAsync(env, 0, null, ct);
            sw.Stop();

            // Assert — reachable=false за per-target таймаут (отказ быстрый: refused;
            // бюджет — дефолт 2000 мс с запасом на DNS/ретраи контейнера)
            status.Should().Be(HttpStatusCode.OK);
            var outcome = JsonDocument.Parse(probeBody).RootElement.GetProperty("targets")
                .EnumerateArray().Single();
            outcome.GetProperty("name").GetString().Should().Be("closed-port");
            outcome.GetProperty("reachable").GetBoolean().Should().BeFalse();
            outcome.TryGetProperty("rttMs", out _).Should().BeFalse();
            sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(5),
                "probe закрывается per-target таймаутом, не висит");

            // Act — неизвестное имя фильтра
            var (unknownStatus, unknownBody) = await MeshClients.AdminProbeAsync(env, 0, "ghost", ct);

            // Assert — 400 с пояснением (глава 07 §2)
            unknownStatus.Should().Be(HttpStatusCode.BadRequest);
            unknownBody.Should().Contain("ghost");
        }, ct: TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Probe_NoTargets_Inactive()
    {
        DockerTrait.SkipIfUnavailable();
        // Контур 2 узла БЕЗ targets (env-seed пуст): probe — «соседей нет»
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("probe-inactive", 2, async env =>
        {
            // Act
            var (status, body) = await MeshClients.AdminProbeAsync(env, 0, null,
                TestContext.Current.CancellationToken);

            // Assert — осмысленный ответ, не ошибка (глава 07 §2)
            status.Should().Be(HttpStatusCode.OK);
            var json = JsonDocument.Parse(body).RootElement;
            json.GetProperty("active").GetBoolean().Should().BeFalse();
            json.GetProperty("targets").GetArrayLength().Should().Be(0);
        }, fullMeshTargets: false, ct: TestContext.Current.CancellationToken);
    }
}
```

- [ ] **Step 6.3: Прогон E2E-серии probe на свежем Release**

Run (E2eFixture соберёт Release-образ сам, инкрементально):

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~OwnS3.IntegrationTests.E2e.Replication.ProbeScenarios"
```

Expected: PASS 4/4; в журнале — `[PHASE] start/teardown` каждого контура, teardown-ассерты чистоты зелёные.

- [ ] **Step 6.4: Зачистка после серии (канон AGENTS.md)**

Run: `docker ps -a --format '{{.Names}}' | grep -c os3m; docker network ls --format '{{.Name}}' | grep -c os3m`
Expected: оба счёта `0` (сценарии снесли свои контуры; сироты — вручную own-only по README-cleanup.txt из артефактов прогона, затем повторить счёт).

- [ ] **Step 6.5: Commit**

```bash
git add src/tests/OwnS3.IntegrationTests/E2e/Replication/MeshClients.cs src/tests/OwnS3.IntegrationTests/E2e/Replication/ProbeScenarios.cs
git commit -m "t37: E2E ProbeScenarios — живой mesh 3 ноды (probe всех нод: reachable=true + rttMs, симметрия пар в обе стороны — модель зонда t33; ?target= — ровно один исход), остановка соседа (reachable=false без rttMs, возврат контейнера — снова true), закрытый порт в составе (false в пределах per-target таймаута) + неизвестный ?target 400, без соседей {active:false,targets:[]}; хелпер MeshClients.AdminProbeAsync; каждый кейс — собственный guid-контур RunScenarioAsync (изоляция/teardown/телеметрия канонов e2e-isolation/e2e-launch)"
```

---

## Task 7: Полные прогоны и зачистка серий (фаза 4 spec, мерж-гейт)

**Files:** изменений нет — только прогоны и зачистка. Если прогон вскрывает дефект — правка отдельным коммитом с повтором задетой серии.

**Interfaces:**
- Consumes: всё, что построили Tasks 0–6.
- Produces: зелёные серии — критерии приёмки 5–7 spec.

- [ ] **Step 7.1: Юнит-серии OwnS3**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~OwnS3.UnitTests"`
Expected: PASS — вся сборка (включая новые `ReplicationProberTests` 7/7 и `ReplicationOptionsTests` +2).

Зачистка: юниты docker не поднимают — контрольный `docker ps -a --format '{{.Names}}' | grep -c os3m` → `0`.

- [ ] **Step 7.2: Интеграционная серия OwnS3 (Api, включая Existing-регресс)**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~OwnS3.IntegrationTests"`
Expected: PASS — вся серия; отдельно убедиться по выводу: `ReplicationScenarios` (+4), `ReplicationAdminScenarios` (+4), `AccessScenarios`/`RoutingScenarios`/`MetricsHealthScenarios` unchanged (Existing-регресс, критерий 6). E2e-классы в этой серии без `PGW_TEST_DOCKER=1` — Skip (норма).

Зачистка: `docker ps -a --format '{{.Names}}' | grep -c os3m` → `0`; `docker network ls --format '{{.Name}}' | grep -c os3m` → `0`.

- [ ] **Step 7.3: Полная docker-E2E серия Replication на свежем Release (мерж-гейт)**

Run:

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~OwnS3.IntegrationTests.E2e.Replication"
```

Expected: PASS — вся серия E2e.Replication (существующие сценарии + новые `ProbeScenarios` 4/4); телеметрия `[PHASE]`/артефакты по канонам `docs/e2e-launch.md`. Это мерж-гейт задачи: код трогает `src/OwnS3.*` — docker-E2E на свежем Release обязателен (правило AGENTS.md).

Зачистка после финальной строки прогона: `docker ps -a --format '{{.Names}}' | grep -c os3m` → `0`; `docker network ls --format '{{.Name}}' | grep -c os3m` → `0` (сироты `os3m-*` — `docker network rm` own-only по именам, при массовом скоплении от прерванных прогонов — `docker network prune -f`).

- [ ] **Step 7.4: Сводка соответствия критериям приёмки spec §7**

Проверить по итогам Tasks 0–7 (самопроверка перед ревью):

1. Arch-коммит (Task 0) раньше кодовых (Tasks 1–6) — `git log --oneline` показывает порядок; тексты arch соответствуют реализации дословно.
2. `GET /replication/ping`: 200-пустое / 403 / 400 / приёмник-чистота — Task 4.
3. `POST /admin/replication/probe`: форма ответа / reachable-200 / rttMs / ?target / 400 / без соседей / права — Task 5.
4. Таймаут env + дефолт + параллельность — Tasks 2, 3, 5 (юнит ParallelPings + интеграционный бюджет).
5. Серии зелёные на свежем Release — Task 7.
6. Existing-регресс — Step 7.2.
7. E2E на свежем Release — Step 7.3.

---

## Соответствие задач фазам и требованиям spec

| Spec | Task |
|---|---|
| §4.4 arch-first (фаза 0) | Task 0 |
| §4.3 транспорт: `PingResult`/`PingAsync`/`ReplicationHttpClient` | Task 1 |
| §4.3 `ReplicationOptions.ProbeTimeoutMs` | Task 2 |
| §4.3 `ReplicationProber` (параллельность, фильтр, гашение) | Task 3 |
| §4.1 грань ping приёмника; §5 фаза 2 (ping-кейсы, приёмник-чистота) | Task 4 |
| §4.2 админ-триггер probe; §5 фаза 2 (probe-кейсы, права, 400, таймаут) | Task 5 |
| §5 фаза 3 E2E `ProbeScenarios` (5 пунктов spec — кейсы объединены в 4 сценария по контурам) | Task 6 |
| §5 фаза 4 прогоны; §7 критерии 5–7 | Task 7 |

Вне задачи (spec §6 — НЕ реализовывать): зонд t33, периодический автопинг, retry/бэкофф probe, метрики/лог-события probe, обновление `link_online` по итогам probe, изменения матрицы прав и полей `GET status`, правки `deploy/`.
