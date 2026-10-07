# t12-loop-watchdog — план реализации: watchdog зависших циклов воркера

> **Для agentic-воркеров:** ОБЯЗАТЕЛЬНЫЙ SUB-SKILL: superpowers:subagent-driven-development
> (рекомендуется) или superpowers:executing-plans — выполнять план задача-за-задачей.
> Шаги используют синтаксис чекбоксов (`- [ ]`) для отслеживания.

**Цель:** зависший без исключения фоновый цикл воркера самолечется — внутренний
watchdog замечает staleness тиков, делает graceful `StopApplication` (путь
`POST /api/restart`), docker поднимает контейнер, клэймы мигрируют ≤15 с.

**Архитектура:** общий компонент `LoopWatchdog` (Shared.Core, `BackgroundService`)
читает абстракцию `ILoopsVitality` (реализация per-app поверх `HealthState`) и
сравнивает возраст тиков с порогом = `Multiplier` × порог healthz (формулы —
общий хелпер `LoopStaleness`, туда же переезжают healthz). Firing → LogCritical +
counter `worker_watchdog_restarts_total{loop}` (колбэк из app) + `StopApplication`.
Никаких сетевых вызовов, etcd не трогается. Панель — НЕ-цель; healthz-семантика
не меняется (только новая data-секция `watchdog`).

**Стек:** .NET 10, C# `LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`,
CPM (`src/Directory.Packages.props`), xunit.v3 + FluentAssertions, WAF-интеграционные
тесты (Microsoft.AspNetCore.Mvc.Testing).

**Спека:** [`spec.md`](spec.md) — план аргументирует от спеки; исполнители читают обе.
Решения Q1–Q5 зафиксированы в её шапке и здесь не пересматриваются.

## Глобальные ограничения

- Вся работа — ТОЛЬКО в worktree `/Users/demakaev/ZCodeProject/worktrees/t12-loop-watchdog`
  (ветка t12-loop-watchdog); коммиты в feature-ветке свободны, мерж в main — по
  отдельному приказу пользователя.
- `TreatWarningsAsErrors=true` — новый код без предупреждений; каждый таск
  завершается зелёной сборкой (`dotnet build`/`dotnet test` 0 warnings).
- Комментарии и документация — на русском; идентификаторы — на английском.
- Тесты — комментарии по нотации AAA (Arrange/Act/Assert).
- Тесты: никаких хардкодов хост-портов; каждое отдельное ожидание/поллинг ≤30 с
  (AGENTS.base.md §12); тесты этой задачи НЕ поднимают docker-контейнеров
  (кроме финального E2E-маркера фикстурой — каноны `docs/e2e-isolation.md`).
- Watchdog не делает сетевых вызовов и не пишет в etcd — нарушение блокирует ревью.
- Поведение healthz бит-в-бит, кроме: новая data-секция `watchdog`, порог
  orphan-sweep добавлен в loops-alive PgWorker, формулы переезжают в хелпер
  без изменения значений.
- Метрики — пассивные наблюдатели: марк-методы никогда не бросают исключений.
- Локально собираемые образы в registry `192.168.0.1:5000` не кладём (E2E здесь
  образы не собирает — фикстура сама).
- В документах (arch/, docs/, runbook) — только текущее/планируемое состояние,
  без пометок задачи (t12) — истории не пишем.

## Карта файлов (декомпозиция)

| Файл | Ответственность |
|---|---|
| `src/Shared.Core/HealthChecks/LoopStaleness.cs` (create) | формулы порогов: единый источник healthz + watchdog |
| `src/Shared.Core/Hosting/LoopWatchdog.cs` (create) | `LoopHeartbeat`, `ILoopsVitality`, `WatchdogOptions`, `LoopWatchdog` |
| `src/Shared.Core/Hosting/LoopWatchdogServiceCollectionExtensions.cs` (create) | условная DI-регистрация (`Enabled=false` → ничего) |
| `src/Shared.Core/Shared.Core.csproj` (modify) | + `Microsoft.Extensions.Hosting.Abstractions` (версия в CPM 10.0.9) |
| `src/Shared.Metrics/Worker/WorkerMetricsInstrumentation.cs` (modify) | маркер `WatchdogRestart` + counter |
| `src/PgWorker.App/HealthState.cs` (modify) | `MarkOrphanSweepTick` + поле снимка |
| `src/PgWorker.App/Loops/BackupOrphanSweeperLoop.cs` (modify) | тики живости (итерации + чанки лидерного сна) |
| `src/PgWorker.App/HealthChecks/PgWorkerHealth.cs` (modify) | хелпер формул, orphan-sweep в loops-alive, секция `watchdog` |
| `src/PgWorker.App/Options.cs` (modify) | `LoopsOptions.Watchdog` |
| `src/PgWorker.App/LoopsVitality.cs` (create) | `PgWorkerLoopsVitality`: 4 цикла |
| `src/PgWorker.App/appsettings.json`, `Program.cs` (modify) | секция конфига + подключение |
| `src/KafkaWorker.App/{Options.cs, HealthChecks/KafkaWorkerHealth.cs, LoopsVitality.cs, appsettings.json, Program.cs}` | симметрия (3 цикла) |
| `src/ValkeyWorker.App/{...те же 5 файлов}` | симметрия (3 цикла) |
| `src/tests/Shared.Core.UnitTests/Hosting/LoopWatchdogTests.cs` (create) | формулы + механика + регистрация |
| `src/tests/Shared.Metrics.UnitTests/WorkerMetricsInstrumentationTests.cs` (modify) | кейс `WatchdogRestart` |
| `src/tests/PgWorker.UnitTests/App/{HealthTests.cs (modify), BackupOrphanSweeperLoopTests.cs (create), LoopsVitalityTests.cs (create)}` | per-app юниты |
| `src/tests/KafkaWorker.UnitTests/App/HealthTests.cs`, `src/tests/ValkeyWorker.UnitTests/App/HealthTests.cs` (modify) | конструктор health + кейсы |
| `src/tests/PgWorker.IntegrationTests/Hosting/LoopWatchdogTests.cs` (create) | интеграционные кейсы самолечения |
| `src/tests/PgWorker.IntegrationTests/Api/MetricsTests.cs` (modify) | экспозиция серии watchdog |
| `arch/14-pgworker.md`, `arch/16-kafkaworker.md`, `arch/21-valkeyworker.md`, `arch/18-metrics.md` (modify) | arch-first канон |
| `docs/runbook.md` (modify) | раздел эксплуатации watchdog |
| `arch/roadmap/reliability.md`, `arch/roadmap/reliability-report.md` (modify) | roadmap-гейт (мерж-коммит закрытия) |

Порядок фаз — §5 спеки: arch-first → Shared.Core+Metrics → воркеры →
интеграционный тест → мерж-гейт. Фазы 2→3→4→5 жёстко последовательны.

---

## Фаза 1 — arch-first (только документация)

### Task 1: канон watchdog в arch/ (§4.1 спеки)

**Файлы:**
- Modify: `arch/14-pgworker.md` (§6 «Надёжность», §7 «Наблюдаемость», §8 «Конфигурация»)
- Modify: `arch/16-kafkaworker.md` (§6, §7, §8)
- Modify: `arch/21-valkeyworker.md` (§6, §7, §8)
- Modify: `arch/18-metrics.md` (§2.2, таблица воркер-паттерна)

**Interfaces:** потребляет формулировки спеки §4.1; производит канон, на который
ссылаются дальнейшие задачи (AC7).

- [ ] **Шаг 1: arch/14 §6 — пункт «Watchdog зависших циклов»**

Вставить после пункта «Отказ docker-хоста» (перед `---` §6):

```markdown
- **Watchdog зависших циклов**: внутренний компонент `LoopWatchdog`
  (Shared.Core, `BackgroundService`) следит за возрастом тиков всех фоновых
  циклов (reconcile/keepalive/snapshot/orphan-sweep) по отметкам `HealthState`;
  порог — множитель `Loops:Watchdog:Multiplier` (дефолт 2) к порогу healthz
  loops-alive (формулы — общий хелпер `LoopStaleness` Shared.Core: healthz и
  watchdog читают одну формулу и не разъезжаются). Staleness сверх порога →
  запись в журнал (critical) + метрика `worker_watchdog_restarts_total{loop}` +
  graceful `StopApplication` (тот же путь, что `POST /api/restart`): контейнер
  поднимает docker-политика, lease гаснут ≤15 с, клэймы мигрируют второму
  инстансу (takeover). Отметка `null` («цикл ещё не тикал») не firing, пока
  возраст watchdog с его запуска меньше 2×порога цикла; дальше — рестарт
  (цикл не стартовал или завис при старте). Компонент без сетевых вызовов и
  без etcd; за собой не следит. Граница: «завис весь процесс» (healthz не
  отвечает) — зона docker HEALTHCHECK, watchdog закрывает только «цикл завис,
  процесс жив». Анти-луп рестартов не вводится: сдерживание — docker
  restart-backoff, видимость — метрика/журнал.
```

- [ ] **Шаг 2: arch/14 §7 — секция watchdog в healthz**

В §7 в строку «Health checks (`/healthz`)» после `snapshot-freshness` добавить
`watchdog` (armed/stale — состояние компонента). Строку про Prometheus дополнить
после «снапшоты» словом «рестарты watchdog» (словарь §2.2).

- [ ] **Шаг 3: arch/14 §8 — секция Loops:Watchdog**

В конфиг-блоке `PgWorker:Loops` дополнить:

```text
PgWorker:Loops { ScanIntervalSec=5, KeepaliveSec=5, SnapshotIntervalMin=360,
                 ErrorDelayMs=2000,
                 Watchdog { Enabled=true, Multiplier=2, CheckIntervalSec=15, StopDelaySec=1 } }
                 # watchdog зависших циклов: порог = Multiplier × порог healthz
                 # loops-alive (60 c на быстрых циклах при дефолтах); Enabled=false
                 # — компонент не регистрируется; явного StaleAfterSec-оверрайда
                 # нет — пороги следуют за интервалами циклов
```

- [ ] **Шаг 4: arch/16 §6/§7/§8 — симметрия KafkaWorker**

В §6 после «Переживание данных» — тот же пункт «Watchdog зависших циклов», но
перечень циклов «reconcile/keepalive/snapshot» и ссылка на `arch/14 §6` для
механики (текст mechanics не дублировать дословно, дать краткую версию + ссылку).
В §7 — секция `watchdog` в health-строку. В §8 в блок `KafkaWorker:Loops` добавить
`Watchdog { Enabled=true, Multiplier=2, CheckIntervalSec=15, StopDelaySec=1 }`
с той же ремаркой.

- [ ] **Шаг 5: arch/21 §6/§7/§8 — симметрия ValkeyWorker**

Аналогично шагу 4 (§6 — после «Отказ etcd», §7 — health-строка, §8 — блок
`ValkeyWorker:Loops`).

- [ ] **Шаг 6: arch/18 §2.2 — серия**

В таблицу §2.2 после строки `worker_snapshot_age_seconds` добавить:

```markdown
| `worker_watchdog_restarts_total` | counter | `loop` | инициированные watchdog-остановки цикла по staleness (внутренний watchdog воркера: arch/14 §6, arch/16 §6, arch/21 §6); источник — марк-метод `WatchdogRestart` |
```

- [ ] **Шаг 7: проверка и коммит**

Run: `grep -c "Watchdog" arch/14-pgworker.md arch/16-kafkaworker.md arch/21-valkeyworker.md arch/18-metrics.md`
Ожидание: в каждом файле ≥2 вхождения; тексты не содержат пометок задачи (t12).

```bash
git add arch/14-pgworker.md arch/16-kafkaworker.md arch/21-valkeyworker.md arch/18-metrics.md
git commit -m "docs(t12): arch-канон watchdog зависших циклов — arch/14/16/21 §6-8, arch/18 §2.2 серия worker_watchdog_restarts_total (spec §4.1)"
```

---

## Фаза 2 — Shared.Core + Shared.Metrics

### Task 2: хелпер формул LoopStaleness (§4.2 спеки)

**Файлы:**
- Create: `src/Shared.Core/HealthChecks/LoopStaleness.cs`
- Test: `src/tests/Shared.Core.UnitTests/Hosting/LoopWatchdogTests.cs` (новый файл — тесты формул первыми; механика watchdog дописывается в Task 3 в тот же файл)

**Interfaces:**
- Produces: `static TimeSpan LoopStaleness.FastLoops(int scanIntervalSec, int keepaliveSec)` —
  `3 × max(scan, keepalive) + 15` секунд; `static TimeSpan LoopStaleness.SnapshotLoop(int scanIntervalSec, int snapshotIntervalMin)` —
  `3 × max(scan, 60 × snapshotIntervalMin) + 15` секунд. Потребители: healthz
  трёх воркеров (Task 6/8/9), per-app vitality (Task 7/8/9), интеграционный тест
  (Task 10).

- [ ] **Шаг 1: пишем failing-тесты формул**

Создать `src/tests/Shared.Core.UnitTests/Hosting/LoopWatchdogTests.cs`:

```csharp
using Shared.Core.HealthChecks;

namespace Shared.Core.UnitTests.Hosting;

// Юнит-тесты watchdog зависших циклов: формулы порогов LoopStaleness — единый
// источник healthz + watchdog (симметрия: порог healthz ×1, watchdog ×2).
public sealed class LoopStalenessTests
{
    [Theory]
    [InlineData(5, 5, 30)]    // дефолты воркеров: 3×max(5,5)+15
    [InlineData(10, 3, 45)]   // скан доминирует
    [InlineData(2, 8, 39)]    // keepalive доминирует
    public void FastLoops_Formula(int scan, int keepalive, int expectedSec)
    {
        // Act
        var staleAfter = LoopStaleness.FastLoops(scan, keepalive);

        // Assert
        staleAfter.Should().Be(TimeSpan.FromSeconds(expectedSec));
    }

    [Theory]
    [InlineData(5, 360, 64815)] // дефолт: 3×max(5, 21600)+15 — часы между тиками лидера
    [InlineData(10, 10, 1815)]  // интервал в минутах доминирует над сканом
    public void SnapshotLoop_Formula(int scan, int intervalMin, int expectedSec)
    {
        // Act
        var staleAfter = LoopStaleness.SnapshotLoop(scan, intervalMin);

        // Assert
        staleAfter.Should().Be(TimeSpan.FromSeconds(expectedSec));
    }

    [Fact]
    public void Symmetry_WatchdogThreshold_IsHealthzTimesMultiplier()
    {
        // Arrange: дефолтные интервалы воркера и множитель 2
        const int multiplier = 2;

        // Act: порог healthz и порог watchdog — одна формула, разные множители
        var healthz = LoopStaleness.FastLoops(5, 5);
        var watchdog = TimeSpan.FromTicks(healthz.Ticks * multiplier);

        // Assert: окно Degraded→рестарт = 30 c (алертам оператора и длинным тикам)
        healthz.Should().Be(TimeSpan.FromSeconds(30));
        watchdog.Should().Be(TimeSpan.FromSeconds(60));
    }
}
```

- [ ] **Шаг 2: запуск — убедиться, что падает**

Run: `dotnet test src/tests/Shared.Core.UnitTests -c Debug --filter "FullyQualifiedName~LoopStalenessTests"`
Ожидание: FAIL — `LoopStaleness` не существует (CS0103).

- [ ] **Шаг 3: реализация**

Создать `src/Shared.Core/HealthChecks/LoopStaleness.cs`:

```csharp
namespace Shared.Core.HealthChecks;

/// <summary>Пороги staleness циклов воркера — единый источник для healthz
/// loops-alive и watchdog (healthz — формулы как есть, watchdog — ×Multiplier):
/// вынесены из *WorkerHealth без изменения значений.</summary>
public static class LoopStaleness
{
    /// <summary>Быстрые циклы (reconcile/keepalive/orphan-sweep):
    /// 3 × max(scan, keepalive) + 15 секунд.</summary>
    public static TimeSpan FastLoops(int scanIntervalSec, int keepaliveSec)
        => TimeSpan.FromSeconds(3 * Math.Max(scanIntervalSec, keepaliveSec) + 15);

    /// <summary>Snapshot-цикл: между тиками лидер спит SnapshotIntervalMin —
    /// 3 × max(scan, 60 × interval) + 15 секунд.</summary>
    public static TimeSpan SnapshotLoop(int scanIntervalSec, int snapshotIntervalMin)
        => TimeSpan.FromSeconds(3 * Math.Max(scanIntervalSec, 60 * snapshotIntervalMin) + 15);
}
```

- [ ] **Шаг 4: запуск — зелёный**

Run: `dotnet test src/tests/Shared.Core.UnitTests -c Debug --filter "FullyQualifiedName~LoopStalenessTests"`
Ожидание: PASS (все кейсы).

- [ ] **Шаг 5: коммит**

```bash
git add src/Shared.Core/HealthChecks/LoopStaleness.cs src/tests/Shared.Core.UnitTests/Hosting/LoopWatchdogTests.cs
git commit -m "feat(t12): хелпер формул порогов LoopStaleness (Shared.Core) + юнит-тесты формул и симметрии healthz×2 (spec §4.2)"
```

### Task 3: LoopWatchdog + ILoopsVitality + DI-регистрация (§4.2, §4.4 спеки)

**Файлы:**
- Modify: `src/Shared.Core/Shared.Core.csproj` (+`Microsoft.Extensions.Hosting.Abstractions`, CPM-версия 10.0.9 уже в `src/Directory.Packages.props`)
- Create: `src/Shared.Core/Hosting/LoopWatchdog.cs`
- Create: `src/Shared.Core/Hosting/LoopWatchdogServiceCollectionExtensions.cs`
- Test: `src/tests/Shared.Core.UnitTests/Hosting/LoopWatchdogTests.cs` (дописать класс `LoopWatchdogTests`)

**Interfaces:**
- Consumes: `TimeProvider`, `Microsoft.Extensions.Hosting` (`BackgroundService`, `IHostApplicationLifetime`), `ILogger<T>`.
- Produces (соседние задачи зависят от этих имён — соблюдать точно):
  - `Shared.Core.Hosting.LoopHeartbeat(string Name, DateTimeOffset? LastTickAt, TimeSpan StaleAfter)` — `StaleAfter` уже watchdog-порог (множитель применён в per-app vitality);
  - `Shared.Core.Hosting.ILoopsVitality { IReadOnlyList<LoopHeartbeat> Snapshot(); }`;
  - `Shared.Core.Hosting.WatchdogOptions { bool Enabled = true; int Multiplier = 2; int CheckIntervalSec = 15; int StopDelaySec = 1; }`;
  - `Shared.Core.Hosting.LoopWatchdog : BackgroundService` — конструктор `(ILoopsVitality vitality, IHostApplicationLifetime lifetime, ILogger<LoopWatchdog> logger, TimeProvider clock, WatchdogOptions options, Action<string>? restartMark = null)`; наблюдаемое состояние `bool Armed { get; }`, `string? StaleLoop { get; }` (для healthz-секции);
  - `LoopWatchdogServiceCollectionExtensions.AddLoopWatchdog(this IServiceCollection, WatchdogOptions options, Action<IServiceProvider, string>? restartMark = null)` — требует регистрации `ILoopsVitality` в DI; `Enabled=false` не регистрирует ничего.

- [ ] **Шаг 1: csproj — пакет**

В `src/Shared.Core/Shared.Core.csproj` в первую ItemGroup добавить
`<PackageReference Include="Microsoft.Extensions.Hosting.Abstractions"/>`
(даёт `BackgroundService`, `IHostApplicationLifetime`; `ILogger<T>` — транзитивно).

- [ ] **Шаг 2: failing-тесты механики**

Дописать в `src/tests/Shared.Core.UnitTests/Hosting/LoopWatchdogTests.cs`:

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Shared.Core.Hosting;

namespace Shared.Core.UnitTests.Hosting;

// Механика watchdog: grace старта, firing (ровно один StopApplication + лог +
// маркер метрики), живые циклы не трогают, Enabled=false — не регистрируется.
public sealed class LoopWatchdogTests
{
    // Управляемая витальность: снимок фиксирован в конструкторе.
    private sealed class FakeVitality(params Shared.Core.Hosting.LoopHeartbeat[] beats)
        : ILoopsVitality
    {
        public IReadOnlyList<Shared.Core.Hosting.LoopHeartbeat> Snapshot() => beats;
    }

    // Счётчик вызовов StopApplication (IHostApplicationLifetime подменять в DI
    // нельзя, но watchdog принимает его конструктором — считаем напрямую).
    private sealed class FakeLifetime : IHostApplicationLifetime
    {
        public int StopCalls;
        public CancellationToken ApplicationStarted => CancellationToken.None;
        public CancellationToken ApplicationStopping => CancellationToken.None;
        public CancellationToken ApplicationStopped => CancellationToken.None;
        public void StopApplication() => StopCalls++;
    }

    // Собирающий логгер: проверка критического события перед остановкой.
    private sealed class CollectingLogger : ILogger<LoopWatchdog>
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel level, EventId eventId, TState state, Exception? e,
            Func<TState, Exception?, string> formatter) => Entries.Add((level, formatter(state, e)));
    }

    private static WatchdogOptions QuickOptions() => new()
    {
        Multiplier = 2, CheckIntervalSec = 1, StopDelaySec = 0
    };

    private static async Task<int> PollUntilAsync(Func<bool> done, TimeSpan budget)
    {
        var waited = TimeSpan.Zero;
        var step = TimeSpan.FromMilliseconds(250);
        while (!done() && waited < budget) // каждое ожидание ≤250 мс ≪ 30 c
        {
            await Task.Delay(step);
            waited += step;
        }
        return 0;
    }

    [Fact]
    public async Task StaleTick_FiresOnce_LogsCritical_AndMarksMetric()
    {
        // Arrange: цикл тикал 60 c назад при пороге 10 c — за пределами grace
        var lifetime = new FakeLifetime();
        var logger = new CollectingLogger();
        var marks = new List<string>();
        var sut = new LoopWatchdog(
            new FakeVitality(new LoopHeartbeat("reconcile",
                DateTimeOffset.UtcNow - TimeSpan.FromSeconds(60), TimeSpan.FromSeconds(10))),
            lifetime, logger, TimeProvider.System, QuickOptions(), marks.Add);
        using var cts = new CancellationTokenSource();

        // Act: одно наблюдение превышения достаточно — ждём firing
        await sut.StartAsync(cts.Token);
        await PollUntilAsync(() => lifetime.StopCalls > 0, TimeSpan.FromSeconds(10));

        // Assert: ровно один StopApplication, критический журнал, маркер метрики
        lifetime.StopCalls.Should().Be(1);
        sut.StaleLoop.Should().Be("reconcile");
        sut.Armed.Should().BeTrue();
        marks.Should().ContainSingle().Which.Should().Be("reconcile");
        logger.Entries.Should().Contain(e =>
            e.Level == LogLevel.Critical && e.Message.Contains("reconcile")
            && e.Message.Contains("self-restart"));
        cts.Cancel();
    }

    [Fact]
    public async Task FreshTicks_DoNotStop()
    {
        // Arrange: все циклы тикали только что
        var lifetime = new FakeLifetime();
        var sut = new LoopWatchdog(
            new FakeVitality(new LoopHeartbeat("reconcile",
                DateTimeOffset.UtcNow - TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(10))),
            lifetime, NullLogger<LoopWatchdog>.Instance, TimeProvider.System, QuickOptions());
        using var cts = new CancellationTokenSource();

        // Act: наблюдаем 4 с (4 проверки при CheckIntervalSec=1) и останавливаем сами
        await sut.StartAsync(cts.Token);
        await Task.Delay(TimeSpan.FromSeconds(4));
        cts.Cancel();

        // Assert: хост не останавливался
        lifetime.StopCalls.Should().Be(0);
        sut.StaleLoop.Should().BeNull();
    }

    [Fact]
    public async Task NullTick_InGraceWindow_DoesNotFire()
    {
        // Arrange: цикл ещё не тикал, grace = 2×порог = 4 c от запуска watchdog
        var lifetime = new FakeLifetime();
        var sut = new LoopWatchdog(
            new FakeVitality(new LoopHeartbeat("snapshot", null, TimeSpan.FromSeconds(2))),
            lifetime, NullLogger<LoopWatchdog>.Instance, TimeProvider.System, QuickOptions());
        using var cts = new CancellationTokenSource();

        // Act: наблюдаем МЕНЬШЕ grace-окна
        await sut.StartAsync(cts.Token);
        await Task.Delay(TimeSpan.FromSeconds(2));
        var inGrace = lifetime.StopCalls;
        cts.Cancel();

        // Assert: null-отметка в grace — не firing (цикл имеет право стартовать)
        inGrace.Should().Be(0);
    }

    [Fact]
    public async Task NullTick_AfterGrace_Fires()
    {
        // Arrange: null-отметка и порог 1 c → grace = 2 c
        var lifetime = new FakeLifetime();
        var sut = new LoopWatchdog(
            new FakeVitality(new LoopHeartbeat("snapshot", null, TimeSpan.FromSeconds(1))),
            lifetime, NullLogger<LoopWatchdog>.Instance, TimeProvider.System, QuickOptions());
        using var cts = new CancellationTokenSource();

        // Act: ждём исчерпания grace (~2 c) + первой проверки
        await sut.StartAsync(cts.Token);
        await PollUntilAsync(() => lifetime.StopCalls > 0, TimeSpan.FromSeconds(10));

        // Assert: цикл не тикнул за grace — рестарт
        lifetime.StopCalls.Should().Be(1);
        cts.Cancel();
    }

    [Fact]
    public void AddLoopWatchdog_Disabled_RegistersNothing()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddLoopWatchdog(new WatchdogOptions { Enabled = false });

        // Assert: ни синглтона, ни hosted-сервиса (поведение как до задачи)
        services.Should().NotContain(d => d.ServiceType == typeof(LoopWatchdog));
        services.Should().NotContain(d => d.ServiceType == typeof(IHostedService));
    }

    [Fact]
    public void AddLoopWatchdog_Enabled_RegistersSingletonAndHosted()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddLoopWatchdog(new WatchdogOptions());

        // Assert: синглтон + hosted-обёртка над ним (паттерн циклов Program.cs)
        services.Should().Contain(d => d.ServiceType == typeof(LoopWatchdog));
        services.Should().Contain(d => d.ServiceType == typeof(IHostedService));
    }
}
```

- [ ] **Шаг 3: запуск — падает**

Run: `dotnet test src/tests/Shared.Core.UnitTests -c Debug --filter "FullyQualifiedName~LoopWatchdogTests"`
Ожидание: FAIL — типов `Shared.Core.Hosting.*` нет.

- [ ] **Шаг 4: реализация LoopWatchdog**

Создать `src/Shared.Core/Hosting/LoopWatchdog.cs`:

```csharp
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Shared.Core.Hosting;

/// <summary>Сердцебиение одного цикла: имя, последний тик, порог staleness
/// watchdog (множитель уже применён реализацией ILoopsVitality).</summary>
public sealed record LoopHeartbeat(string Name, DateTimeOffset? LastTickAt, TimeSpan StaleAfter);

/// <summary>Источник живости циклов воркера: реализация per-app поверх своего
/// HealthState + LoopsOptions; чтение lock-free (миллисекунды).</summary>
public interface ILoopsVitality
{
    /// <summary>Снимок сердцебиений всех наблюдаемых циклов.</summary>
    IReadOnlyList<LoopHeartbeat> Snapshot();
}

/// <summary>Секция Loops:Watchdog: Enabled=false — компонент не регистрируется;
/// порог = Multiplier × порог healthz loops-alive.</summary>
public sealed class WatchdogOptions
{
    public bool Enabled { get; set; } = true;

    /// <summary>Множитель порога healthz (окно Degraded→рестарт).</summary>
    public int Multiplier { get; set; } = 2;

    /// <summary>Период проверки возрастов тиков.</summary>
    public int CheckIntervalSec { get; set; } = 15;

    /// <summary>Пауза перед StopApplication (лог/событие доезжают в выхлоп).</summary>
    public int StopDelaySec { get; set; } = 1;
}

/// <summary>
/// Watchdog зависших циклов: staleness тика сверх порога → журнал (critical) +
/// маркер метрики (колбэк) + graceful StopApplication (путь POST /api/restart).
/// Без сетевых вызовов и etcd; за собой не следит (его собственное зависание —
/// деградация всего процесса, зона docker HEALTHCHECK).
/// </summary>
public sealed class LoopWatchdog(
    ILoopsVitality vitality,
    IHostApplicationLifetime lifetime,
    ILogger<LoopWatchdog> logger,
    TimeProvider clock,
    WatchdogOptions options,
    Action<string>? restartMark = null) : BackgroundService
{
    /// <summary>Запущен и следит (секция healthz watchdog).</summary>
    public bool Armed { get; private set; }

    /// <summary>Цикл в staleness по последней проверке (null — все живы).</summary>
    public string? StaleLoop { get; private set; }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var startedAt = clock.GetUtcNow();
        Armed = true;
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                foreach (var heartbeat in vitality.Snapshot())
                {
                    var now = clock.GetUtcNow();
                    if (heartbeat.LastTickAt is { } at)
                    {
                        // Firing: одного наблюдения превышения достаточно — порог
                        // сам по себе защита от ложных срабатываний (гистерезис не вводим).
                        var age = now - at;
                        if (age > heartbeat.StaleAfter)
                        {
                            await FireAndStopAsync(heartbeat, age, stoppingToken);
                            return;
                        }
                    }
                    else if (now - startedAt > TimeSpan.FromTicks(heartbeat.StaleAfter.Ticks * 2))
                    {
                        // Grace старта исчерпан: цикл не тикнул вовсе.
                        await FireAndStopAsync(heartbeat, now - startedAt, stoppingToken);
                        return;
                    }
                }

                await Task.Delay(TimeSpan.FromSeconds(Math.Max(1, options.CheckIntervalSec)), stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // штатная остановка host'а
        }
    }

    // Firing: журнал → метрика → пауза StopDelaySec → StopApplication. Дальше —
    // существующее: shutdown (зависший цикл не реагирует на токен — закроет
    // ShutdownTimeout 30 с) → docker restart: unless-stopped.
    private async Task FireAndStopAsync(LoopHeartbeat heartbeat, TimeSpan age, CancellationToken ct)
    {
        StaleLoop = heartbeat.Name;
        logger.LogCritical(
            "watchdog: цикл {Loop} не тикал {Age:F0} c (порог {Threshold:F0} c) — инициирован self-restart",
            heartbeat.Name, age.TotalSeconds, heartbeat.StaleAfter.TotalSeconds);
        try
        {
            restartMark?.Invoke(heartbeat.Name); // метрика — пассивный наблюдатель
        }
        catch
        {
            // ошибка инструментария не отменяет остановку
        }

        if (options.StopDelaySec > 0)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(options.StopDelaySec), CancellationToken.None);
            }
            catch (OperationCanceledException)
            {
                // CancellationToken.None — недостижимо, страховка
            }
        }

        lifetime.StopApplication();
    }
}
```

- [ ] **Шаг 5: реализация DI-расширения**

Создать `src/Shared.Core/Hosting/LoopWatchdogServiceCollectionExtensions.cs`:

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Shared.Core.Hosting;

public static class LoopWatchdogServiceCollectionExtensions
{
    /// <summary>Регистрация watchdog: синглтон + hosted-обёртка (паттерн циклов
    /// Program.cs воркеров). Требует ILoopsVitality и TimeProvider в DI.
    /// Enabled=false — не регистрирует ничего (поведение как до задачи).</summary>
    public static IServiceCollection AddLoopWatchdog(
        this IServiceCollection services,
        WatchdogOptions options,
        Action<IServiceProvider, string>? restartMark = null)
    {
        if (!options.Enabled)
            return services;

        services.AddSingleton(sp => new LoopWatchdog(
            sp.GetRequiredService<ILoopsVitality>(),
            sp.GetRequiredService<IHostApplicationLifetime>(),
            sp.GetRequiredService<ILogger<LoopWatchdog>>(),
            sp.GetRequiredService<TimeProvider>(),
            options,
            restartMark is null ? null : loop => restartMark(sp, loop)));
        services.AddHostedService(sp => sp.GetRequiredService<LoopWatchdog>());
        return services;
    }
}
```

- [ ] **Шаг 6: запуск — зелёный + вся сборка Shared.Core**

Run: `dotnet test src/tests/Shared.Core.UnitTests -c Debug`
Ожидание: PASS — все тесты (включая LoopStalenessTests из Task 2), 0 warnings.

- [ ] **Шаг 7: коммит**

```bash
git add src/Shared.Core/Shared.Core.csproj src/Shared.Core/Hosting/LoopWatchdog.cs src/Shared.Core/Hosting/LoopWatchdogServiceCollectionExtensions.cs src/tests/Shared.Core.UnitTests/Hosting/LoopWatchdogTests.cs
git commit -m "feat(t12): LoopWatchdog + ILoopsVitality + WatchdogOptions (Shared.Core.Hosting) — grace/firing/ровно один StopApplication; AddLoopWatchdog c Enabled=false no-op (spec §4.2, §4.4)"
```

### Task 4: маркер метрики WatchdogRestart (§4.5 спеки, arch/18 §2.2)

**Файлы:**
- Modify: `src/Shared.Metrics/Worker/WorkerMetricsInstrumentation.cs`
- Test: `src/tests/Shared.Metrics.UnitTests/WorkerMetricsInstrumentationTests.cs`

**Interfaces:**
- Produces: `void WorkerMetricsInstrumentation.WatchdogRestart(string loop)` — counter
  `worker_watchdog_restarts_total{loop}`; internal-снимок `DebugState.WatchdogRestarts`
  (`IReadOnlyDictionary<string, long>`). Потребитель: колбэк регистрации watchdog
  в Program.cs трёх воркеров (Task 7/8/9), интеграционный кейс метрики (Task 10).

- [ ] **Шаг 1: failing-тест**

Дописать в `WorkerMetricsInstrumentationTests.cs` (внутрь существующего класса):

```csharp
[Fact]
public void WatchdogRestart_CountsPerLoop()
{
    // Arrange
    using var meter = new Meter("TestWorker");
    using var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);

    // Act: watchdog инициировал две остановки reconcile и одну keepalive
    sut.WatchdogRestart("reconcile");
    sut.WatchdogRestart("reconcile");
    sut.WatchdogRestart("keepalive");

    // Assert: counter per-loop (лейбл цикла), пассивность — без исключений
    sut.DebugSnapshot().WatchdogRestarts["reconcile"].Should().Be(2);
    sut.DebugSnapshot().WatchdogRestarts["keepalive"].Should().Be(1);
}
```

- [ ] **Шаг 2: запуск — падает**

Run: `dotnet test src/tests/Shared.Metrics.UnitTests -c Debug --filter "FullyQualifiedName~WatchdogRestart_CountsPerLoop"`
Ожидание: FAIL — метода `WatchdogRestart` нет (CS1061).

- [ ] **Шаг 3: реализация**

В `WorkerMetricsInstrumentation.cs`:

1) поле стейта рядом с `_backupVerify`:
```csharp
private readonly Dictionary<string, long> _watchdogRestarts = new();
```
2) в конструкторе рядом с `backupVerify` — инструмент и делегат:
```csharp
var watchdogRestarts = meter.CreateCounter<long>(
    "worker_watchdog_restarts_total",
    description: "Инициированные watchdog-остановки по staleness (arch/18 §2.2)");
```
и в блоке присваивания делегатов:
```csharp
WatchdogRestartMark = loop =>
{
    try
    {
        watchdogRestarts.Add(1, new KeyValuePair<string, object?>("loop", loop));
    }
    catch
    {
        // Пассивный наблюдатель.
    }
};
```
3) поле делегата рядом с `BackupVerifyMark`:
```csharp
private readonly Action<string> WatchdogRestartMark;
```
4) публичный марк-метод (по образцу `BackupVerify`):
```csharp
/// <summary>Counter worker_watchdog_restarts_total{loop}: watchdog инициировал
/// self-restart по staleness цикла (колбэк LoopWatchdog из Program.cs app).</summary>
public void WatchdogRestart(string loop)
{
    try
    {
        WatchdogRestartMark(loop);
        lock (_lock)
        {
            _watchdogRestarts[loop] = _watchdogRestarts.TryGetValue(loop, out var n) ? n + 1 : 1;
        }
    }
    catch
    {
        // Пассивный наблюдатель: ошибка инструментария не влияет на остановку.
    }
}
```
5) `DebugSnapshot()` — в конструктор `DebugState` добавить `WatchdogRestarts: _watchdogRestarts.ToFrozenDictionary(),` и в record `DebugState` — поле
```csharp
IReadOnlyDictionary<string, long> WatchdogRestarts,
```
(позиционно: после `BackupVerifyTotals`, до `SnapshotAgeSeconds`).

- [ ] **Шаг 4: запуск — зелёный + проект целиком**

Run: `dotnet test src/tests/Shared.Metrics.UnitTests -c Debug`
Ожидание: PASS, 0 warnings.

- [ ] **Шаг 5: коммит**

```bash
git add src/Shared.Metrics/Worker/WorkerMetricsInstrumentation.cs src/tests/Shared.Metrics.UnitTests/WorkerMetricsInstrumentationTests.cs
git commit -m "feat(t12): маркер WorkerMetricsInstrumentation.WatchdogRestart — counter worker_watchdog_restarts_total{loop} (arch/18 §2.2, spec §4.5)"
```

---

## Фаза 3 — воркеры

### Task 5: PgWorker — тики живости BackupOrphanSweeperLoop (§4.3 спеки, AC5)

**Файлы:**
- Modify: `src/PgWorker.App/HealthState.cs`
- Modify: `src/PgWorker.App/Loops/BackupOrphanSweeperLoop.cs`
- Test (create): `src/tests/PgWorker.UnitTests/App/BackupOrphanSweeperLoopTests.cs`

**Interfaces:**
- Produces: `HealthState.MarkOrphanSweepTick()`; `HealthSnapshot.LastOrphanSweepTick`
  (`DateTimeOffset?`, добавлен последним полем record — конструктор снимка вызывается
  только внутри `HealthState.Snapshot()`, внешних правок не требуется).
  Потребители: `PgWorkerHealth` (Task 6), `PgWorkerLoopsVitality` (Task 7).

**Важное конструктивное решение:** порог orphan-sweep — как у быстрых циклов
(`3×max(scan,keepalive)+15`, watchdog ×2 = 60 c при дефолтах), но лидерная ветка
спит `Supervisor:IntervalSec` (600 c) — сон без тиков дал бы честный ложный
рестарт watchdog'а. Поэтому: тик живости в начале каждой итерации + лидерный сон
чанками `ScanIntervalSec` с тиком в каждом чанке (суммарная длительность сна не
меняется). Длительный сам `SweepAsync` (>порога) — редкость для домашних
объёмов установки; защита от ресторма — docker restart-backoff (решение Q5 спеки).

- [ ] **Шаг 1: failing-тесты**

Создать `src/tests/PgWorker.UnitTests/App/BackupOrphanSweeperLoopTests.cs`:

```csharp
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using PgWorker.App;
using PgWorker.App.Loops;
using PgWorker.Backups.Supervisor;
using PgWorker.UnitTests.Provisioning;

namespace PgWorker.UnitTests.App;

// Тики живости BackupOrphanSweeperLoop: каждая итерация отмечается в HealthState
// (порог watchdog у sweeper'а — как у быстрых циклов: сон Supervisor:IntervalSec
// тикает чанками, спящий лидер не «stale»).
public sealed class BackupOrphanSweeperLoopTests
{
    private static (BackupOrphanSweeperLoop Loop, HealthState Health, CancellationTokenSource Cts) Create(
        int scanIntervalSec, int supervisorIntervalSec)
    {
        var etcd = new Fakes.FakeEtcd();
        var options = new FixedOptionsMonitor(new PgWorkerOptions
        {
            Etcd = new EtcdOptions { Endpoints = ["http://etcd:2379"] },
            Loops = new LoopsOptions { ScanIntervalSec = scanIntervalSec },
            Backups = new BackupsOptions { Enabled = false }, // не-лидер/выключено — тихий цикл
        });
        options.CurrentValue.Backups.Supervisor.IntervalSec = supervisorIntervalSec;
        var health = new HealthState(TimeProvider.System);
        var claims = new ClaimStore(
            "/pgworker", options.CurrentValue.Etcd.Endpoints, etcd, TimeProvider.System);
        var sweeper = new BackupOrphanSweeper(
            etcd, options.CurrentValue.Etcd.Endpoints,
            new Fakes.DisabledBackupS3Stub(), claims,
            new WorkJournal("/pgworker", etcd, options.CurrentValue.Etcd.Endpoints),
            () => null, TimeProvider.System, NullLogger<BackupOrphanSweeper>.Instance);
        var loop = new BackupOrphanSweeperLoop(options, claims, sweeper, health,
            NullLogger<BackupOrphanSweeperLoop>.Instance);
        return (loop, health, new CancellationTokenSource());
    }

    [Fact]
    public async Task ExecuteAsync_MarksOrphanSweepTick()
    {
        // Arrange: цикл с нулевым интервалом скана (тик каждой итерацией)
        var (loop, health, cts) = Create(scanIntervalSec: 0, supervisorIntervalSec: 600);

        // Act: запускаем и ждём первой отметки (поллинг ≤5 с, шаг 50 мс)
        await loop.StartAsync(cts.Token);
        for (var i = 0; i < 100 && health.Snapshot().LastOrphanSweepTick is null; i++)
            await Task.Delay(50, TestContext.Current.CancellationToken);
        cts.Cancel();

        // Assert: sweeper отмечает тики живости (невидим до задачи — AC5)
        health.Snapshot().LastOrphanSweepTick.Should().NotBeNull();
    }

    [Fact]
    public async Task DelayTickingAsync_TicksInChunks_DoesNotSleepWholeInterval()
    {
        // Arrange: лидерный сон 600 c чанками по 1 c — тики без 600-с паузы
        var (loop, health, cts) = Create(scanIntervalSec: 1, supervisorIntervalSec: 600);
        var counting = ((ITickingSleeper)loop); // см. шаг 3: интерфейс для теста
        // Act: 2,5 c сна чанками → ≥2 отметки, затем отмена
        var task = loop.DelayTickingAsync(TimeSpan.FromSeconds(600), cts.Token);
        for (var i = 0; i < 50; i++)
            await Task.Delay(50, TestContext.Current.CancellationToken);
        var ticksInWindow = health.Snapshot().LastOrphanSweepTick;
        cts.Cancel();
        await Task.WhenAny(task, Task.Delay(5000, TestContext.Current.CancellationToken));

        // Assert: за окно ≪ 600 c отметки уже есть — сон не глушит живость
        ticksInWindow.Should().NotBeNull();
    }
}
```

Примечание к тесту: строка `var counting = ((ITickingSleeper)loop);` не нужна —
убрать при написании (остаток черновика не оставлять); достаточно прямого вызова
`loop.DelayTickingAsync(...)` (метод internal, `InternalsVisibleTo PgWorker.UnitTests`
уже настроен в `src/PgWorker.App/PgWorker.App.csproj`).

- [ ] **Шаг 2: запуск — падает**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~BackupOrphanSweeperLoopTests"`
Ожидание: FAIL — `MarkOrphanSweepTick`/`LastOrphanSweepTick`/`DelayTickingAsync` и
конструктор цикла с `HealthState` отсутствуют; `DisabledBackupS3Stub` — см. шаг 3 п.4.

- [ ] **Шаг 3: реализация**

1) `src/PgWorker.App/HealthState.cs`: поле `private DateTimeOffset? _lastOrphanSweepTick;`
рядом с `_lastSnapshotTick`; метод:
```csharp
/// <summary>Тик BackupOrphanSweeperLoop (итерация цикла: лидерная/холостая).</summary>
public void MarkOrphanSweepTick()
{
    lock (_sync)
    {
        _lastOrphanSweepTick = clock.GetUtcNow();
    }
}
```
`Snapshot()` — передать `_lastOrphanSweepTick` последним аргументом; record
`HealthSnapshot` — добавить `DateTimeOffset? LastOrphanSweepTick` последним полем.

2) `src/PgWorker.App/Loops/BackupOrphanSweeperLoop.cs`: добавить параметр
`HealthState health` в конструктор (после `claims`); в начале тела `while`
первой строкой `health.MarkOrphanSweepTick();` (после `try {`); лидерный сон
`await Task.Delay(TimeSpan.FromSeconds(...Supervisor.IntervalSec), stoppingToken);`
заменить на `await DelayTickingAsync(TimeSpan.FromSeconds(options.CurrentValue.Backups.Supervisor.IntervalSec), stoppingToken);`;
добавить метод:
```csharp
// Сон лидера чанками ScanIntervalSec с тиком живости в каждом чанке:
// Supervisor:IntervalSec (600 c) ≫ порога быстрых циклов — непрерывный сон без
// тиков дал бы ложный self-restart watchdog'ом (порог sweeper'а — как у быстрых).
internal async Task DelayTickingAsync(TimeSpan total, CancellationToken ct)
{
    var remaining = total;
    var chunk = TimeSpan.FromSeconds(Math.Max(1, options.CurrentValue.Loops.ScanIntervalSec));
    while (remaining > TimeSpan.Zero && !ct.IsCancellationRequested)
    {
        var step = chunk < remaining ? chunk : remaining;
        await Task.Delay(step, ct);
        remaining -= step;
        health.MarkOrphanSweepTick();
    }
}
```
XML-комментарий класса дополнить: убрать упоминание «без health-обёртки», указать
«тики живости — HealthState.MarkOrphanSweepTick».

3) `src/PgWorker.App/Program.cs`: регистрацию `BackupOrphanSweeperLoop` дополнить
параметром `sp.GetRequiredService<HealthState>()`:
```csharp
builder.Services.AddSingleton(sp => new PgWorker.App.Loops.BackupOrphanSweeperLoop(
    sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<PgWorker.Backups.Supervisor.BackupOrphanSweeper>(),
    sp.GetRequiredService<HealthState>(),
    sp.GetRequiredService<ILogger<PgWorker.App.Loops.BackupOrphanSweeperLoop>>()));
```
(текущая регистрация `AddSingleton<PgWorker.App.Loops.BackupOrphanSweeperLoop>()` —
заменить на фабричную, как выше; hosted-регистрация не меняется).

4) Заглушка S3 для юнит-теста: если публичный тип-заглушка `DisabledBackupS3` в
`PgWorker.Backups` отсутствует (проверить: она сейчас private в Program.cs), в
`src/PgWorker.UnitTests/Provisioning/Fakes.cs` добавить минимальную заглушку
`internal sealed class DisabledBackupS3Stub : IBackupS3 { ... }` — каждый метод
возвращает `Result.Failed(new ApplicationException("Backups:Enabled=false"))`
(скопировать сигнатуры с `IBackupS3`; sweeper при `Enabled=false` в S3 не ходит).
Если заглушка уже доступна — использовать её и в тесте, и здесь имя не вводить.

- [ ] **Шаг 4: запуск — зелёный + юниты PgWorker.App целиком**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~PgWorker.UnitTests.App"`
Ожидание: PASS (включая прежние HealthTests — снимок расширён, старые вызовы
`new HealthSnapshot(...)` вне HealthState отсутствуют).

- [ ] **Шаг 5: коммит**

```bash
git add src/PgWorker.App/HealthState.cs src/PgWorker.App/Loops/BackupOrphanSweeperLoop.cs src/PgWorker.App/Program.cs src/tests/PgWorker.UnitTests/App/BackupOrphanSweeperLoopTests.cs src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs
git commit -m "feat(t12): BackupOrphanSweeperLoop — тики живости HealthState (итерации + лидерный сон чанками), порог как у быстрых циклов (spec §4.3, AC5)"
```

### Task 6: PgWorker — healthz на общий хелпер, orphan-sweep, секция watchdog, опции (§4.3–4.5)

**Файлы:**
- Modify: `src/PgWorker.App/HealthChecks/PgWorkerHealth.cs`
- Modify: `src/PgWorker.App/Options.cs` (LoopsOptions)
- Modify: `src/PgWorker.App/appsettings.json`
- Test (modify): `src/tests/PgWorker.UnitTests/App/HealthTests.cs`

**Interfaces:**
- Consumes: `LoopStaleness` (Task 2), `LoopWatchdog` (Task 3), `LastOrphanSweepTick` (Task 5).
- Produces: конструктор `PgWorkerHealth(ServiceProbes, HealthState, ClaimStore, IOptionsMonitor<PgWorkerOptions>, TimeProvider, IServiceProvider)`;
  `LoopsOptions.Watchdog` (`Shared.Core.Hosting.WatchdogOptions`); data-секция
  healthz `watchdog`: `"disabled"` | `"starting"` | `"armed; stale=<loop|нет>"`.

- [ ] **Шаг 1: failing-тесты**

В `src/tests/PgWorker.UnitTests/App/HealthTests.cs`:

1) поле-хелпер класса:
```csharp
// Пустой провайдер: watchdog не зарегистрирован → секция "disabled".
private static readonly IServiceProvider EmptyServices =
    new ServiceCollection().BuildServiceProvider();
```
2) все четыре `new PgWorkerHealth(...)` дополнить шестым аргументом `EmptyServices`;
3) в `Check_AllSectionsPresentInData` после существующих ассертов:
```csharp
// Assert — секция watchdog (не зарегистрирован → disabled; AC4)
result.Data.Keys.Should().Contain("watchdog");
result.Data["watchdog"].ToString().Should().Be("disabled");
```
и в Arrange того же теста добавить `health.MarkOrphanSweepTick();`;
4) новый кейс в конец класса:
```csharp
[Fact]
public async Task Check_OrphanSweepInLoopsAlive()
{
    // Arrange — все циклы тикали, включая sweeper
    var etcd = new Fakes.FakeEtcd();
    var health = new HealthState(TimeProvider.System);
    health.MarkEtcdOk();
    health.MarkReconcileTick(ok: true, claimsHeld: 0);
    health.MarkKeepaliveTick();
    health.MarkSnapshotTick();
    health.MarkOrphanSweepTick();
    var claims = new ClaimStore("/pgworker", ["http://etcd:2379"], etcd, TimeProvider.System);
    var check = new PgWorkerHealth(Probes(etcd), health, claims, Options, TimeProvider.System, EmptyServices);

    // Act
    var result = await check.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);

    // Assert — sweeper в loops-alive (AC5)
    result.Data["loops"].ToString().Should().Contain("orphan-sweep=");
}
```

- [ ] **Шаг 2: запуск — падает**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~HealthTests"`
Ожидание: FAIL — у `PgWorkerHealth` нет 6-параметрического конструктора/`MarkOrphanSweepTick`/секции.

- [ ] **Шаг 3: реализация**

1) `src/PgWorker.App/HealthChecks/PgWorkerHealth.cs`:
   - конструктору добавить последний параметр `IServiceProvider services`;
   - формулы заменить на хелпер (значения бит-в-бит):
```csharp
var loops = options.CurrentValue.Loops;
var staleAfter = Shared.Core.HealthChecks.LoopStaleness.FastLoops(loops.ScanIntervalSec, loops.KeepaliveSec);
var snapshotStaleAfter = Shared.Core.HealthChecks.LoopStaleness.SnapshotLoop(loops.ScanIntervalSec, loops.SnapshotIntervalMin);
```
   - в `data["loops"]`-массив добавить `LoopEntry("orphan-sweep", snapshot.LastOrphanSweepTick),`;
   - в пороговый массив добавить кортеж `("orphan-sweep", snapshot.LastOrphanSweepTick, staleAfter),`;
   - новую data-секцию (после `data["loops"]`-блока):
```csharp
// watchdog: состояние компонента (Enabled=false — не зарегистрирован);
// контракт статусов HTTP не меняется — только наблюдаемость.
var watchdog = services.GetService<Shared.Core.Hosting.LoopWatchdog>();
data["watchdog"] = watchdog is null
    ? "disabled"
    : watchdog.Armed ? $"armed; stale={watchdog.StaleLoop ?? "нет"}" : "starting";
```
2) `src/PgWorker.App/Options.cs`, `LoopsOptions`:
```csharp
/// <summary>Watchdog зависших циклов (arch/14 §6/§8): Enabled=false — компонент
/// не регистрируется; порог = Multiplier × порог healthz loops-alive.</summary>
public Shared.Core.Hosting.WatchdogOptions Watchdog { get; set; } = new();
```
3) `src/PgWorker.App/appsettings.json` — в `"Loops"` добавить:
```json
"Watchdog": { "Enabled": true, "Multiplier": 2, "CheckIntervalSec": 15, "StopDelaySec": 1 }
```

- [ ] **Шаг 4: запуск — зелёный + сборка App**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~HealthTests"`
затем `dotnet build src/PgWorker.App -c Debug`
Ожидание: PASS / build 0 warnings (регистрация `AddCheck<PgWorkerHealth>` не
меняется — DI резолвит `IServiceProvider` сам).

- [ ] **Шаг 5: коммит**

```bash
git add src/PgWorker.App/HealthChecks/PgWorkerHealth.cs src/PgWorker.App/Options.cs src/PgWorker.App/appsettings.json src/tests/PgWorker.UnitTests/App/HealthTests.cs
git commit -m "feat(t12): PgWorkerHealth — хелпер LoopStaleness, orphan-sweep в loops-alive, секция watchdog; Loops:Watchdog-опции + appsettings (spec §4.3–4.5)"
```

### Task 7: PgWorker — LoopsVitality + подключение в Program.cs (§4.6 спеки, AC6)

**Файлы:**
- Create: `src/PgWorker.App/LoopsVitality.cs`
- Modify: `src/PgWorker.App/Program.cs`
- Test (create): `src/tests/PgWorker.UnitTests/App/LoopsVitalityTests.cs`

**Interfaces:**
- Consumes: `ILoopsVitality`/`LoopHeartbeat` (Task 3), `LoopStaleness` (Task 2), `HealthSnapshot` (Task 5).
- Produces: `PgWorker.App.PgWorkerLoopsVitality(IOptionsMonitor<PgWorkerOptions>, HealthState) : Shared.Core.Hosting.ILoopsVitality` —
  снимок четырёх циклов: `reconcile`/`keepalive` (порог `FastLoops×Multiplier`),
  `snapshot` (`SnapshotLoop×Multiplier`), `orphan-sweep` (`FastLoops×Multiplier`);
  имена циклов совпадают со строками loops-alive healthz.

- [ ] **Шаг 1: failing-тест**

Создать `src/tests/PgWorker.UnitTests/App/LoopsVitalityTests.cs`:

```csharp
using Microsoft.Extensions.Options;
using PgWorker.App;

namespace PgWorker.UnitTests.App;

// Перечень циклов PgWorker для watchdog: все 4 цикла, пороги = формулы
// LoopStaleness × Watchdog:Multiplier (дефолт 2), null-отметки до старта циклов.
public sealed class LoopsVitalityTests
{
    private static readonly FixedOptionsMonitor Options = new(new PgWorkerOptions
    {
        Loops = new LoopsOptions { ScanIntervalSec = 5, KeepaliveSec = 5, SnapshotIntervalMin = 360 },
    });

    [Fact]
    public void Snapshot_AllFourLoops_WithThresholds()
    {
        // Arrange
        var health = new HealthState(TimeProvider.System);
        var sut = new PgWorkerLoopsVitality(Options, health);

        // Act
        var beats = sut.Snapshot();

        // Assert: перечень §4.6 — 4 цикла, пороги ×2 от порогов healthz
        beats.Select(b => b.Name).Should().Equal("reconcile", "keepalive", "snapshot", "orphan-sweep");
        beats.First(b => b.Name == "reconcile").StaleAfter.Should().Be(TimeSpan.FromSeconds(60));
        beats.First(b => b.Name == "orphan-sweep").StaleAfter.Should().Be(TimeSpan.FromSeconds(60));
        beats.First(b => b.Name == "snapshot").StaleAfter
            .Should().Be(TimeSpan.FromSeconds((3 * Math.Max(5, 60 * 360) + 15) * 2));
        beats.Should().OnlyContain(b => b.LastTickAt is null); // циклы ещё не тикали
    }

    [Fact]
    public void Snapshot_PassesHealthStateTicks()
    {
        // Arrange
        var clock = DateTimeOffset.UtcNow;
        var health = new HealthState(TimeProvider.System);
        health.MarkReconcileTick(ok: true, claimsHeld: 0);
        var sut = new PgWorkerLoopsVitality(Options, health);

        // Act
        var beats = sut.Snapshot();

        // Assert: отметки HealthState — единственный источник живости
        beats.First(b => b.Name == "reconcile").LastTickAt.Should().NotBeNull();
        beats.First(b => b.Name == "keepalive").LastTickAt.Should().BeNull();
    }
}
```

- [ ] **Шаг 2: запуск — падает**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~LoopsVitalityTests"`
Ожидание: FAIL — типа нет.

- [ ] **Шаг 3: реализация**

Создать `src/PgWorker.App/LoopsVitality.cs`:

```csharp
using Microsoft.Extensions.Options;
using Shared.Core.HealthChecks;
using Shared.Core.Hosting;

namespace PgWorker.App;

/// <summary>Живость циклов PgWorker для watchdog: снимок HealthState + пороги
/// LoopStaleness × Loops:Watchdog:Multiplier; перечень — все четыре цикла
/// (reconcile/keepalive/snapshot/orphan-sweep), имена = loops-alive healthz.</summary>
public sealed class PgWorkerLoopsVitality(
    IOptionsMonitor<PgWorkerOptions> options,
    HealthState health) : ILoopsVitality
{
    public IReadOnlyList<LoopHeartbeat> Snapshot()
    {
        var loops = options.CurrentValue.Loops;
        var multiplier = Math.Max(1, loops.Watchdog.Multiplier);
        var fast = TimeSpan.FromTicks(LoopStaleness.FastLoops(loops.ScanIntervalSec, loops.KeepaliveSec).Ticks * multiplier);
        var snapshotLoop = TimeSpan.FromTicks(LoopStaleness.SnapshotLoop(loops.ScanIntervalSec, loops.SnapshotIntervalMin).Ticks * multiplier);
        var snap = health.Snapshot();
        return
        [
            new LoopHeartbeat("reconcile", snap.LastReconcileTick, fast),
            new LoopHeartbeat("keepalive", snap.LastKeepaliveTick, fast),
            new LoopHeartbeat("snapshot", snap.LastSnapshotTick, snapshotLoop),
            new LoopHeartbeat("orphan-sweep", snap.LastOrphanSweepTick, fast),
        ];
    }
}
```

В `src/PgWorker.App/Program.cs` после регистраций health-checks (после блока
`.AddCheck<HealthCheckAbstract<SnapshotLoop>>("snapshot-loop")`):

```csharp
// Watchdog зависших циклов (arch/14 §6): staleness тиков всех циклов → журнал +
// метрика + graceful self-stop (путь POST /api/restart); Enabled=false (секция
// PgWorker:Loops:Watchdog) — компонент не регистрируется.
builder.Services.AddSingleton<PgWorkerLoopsVitality>();
builder.Services.AddSingleton<Shared.Core.Hosting.ILoopsVitality>(
    sp => sp.GetRequiredService<PgWorkerLoopsVitality>());
var loopsWatchdog = new Shared.Core.Hosting.WatchdogOptions();
builder.Configuration.GetSection("PgWorker:Loops:Watchdog").Bind(loopsWatchdog);
builder.Services.AddLoopWatchdog(
    loopsWatchdog,
    (sp, loop) => sp.GetRequiredService<Shared.Metrics.Worker.WorkerMetricsInstrumentation>()
        .WatchdogRestart(loop));
```

- [ ] **Шаг 4: запуск — зелёный + юниты PgWorker целиком**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug`
Ожидание: PASS, 0 warnings.

- [ ] **Шаг 5: коммит**

```bash
git add src/PgWorker.App/LoopsVitality.cs src/PgWorker.App/Program.cs src/tests/PgWorker.UnitTests/App/LoopsVitalityTests.cs
git commit -m "feat(t12): PgWorkerLoopsVitality (4 цикла) + подключение LoopWatchdog в Program.cs с колбэком метрики (spec §4.6)"
```

### Task 8: KafkaWorker — симметрия (§4.4–4.6 спеки, AC6)

**Файлы:**
- Modify: `src/KafkaWorker.App/Options.cs` (LoopsOptions + Watchdog)
- Modify: `src/KafkaWorker.App/appsettings.json`
- Modify: `src/KafkaWorker.App/HealthChecks/KafkaWorkerHealth.cs`
- Create: `src/KafkaWorker.App/LoopsVitality.cs`
- Modify: `src/KafkaWorker.App/Program.cs`
- Test (modify): `src/tests/KafkaWorker.UnitTests/App/HealthTests.cs`
- Test (create): `src/tests/KafkaWorker.UnitTests/App/LoopsVitalityTests.cs`

**Interfaces:**
- Consumes: те же, что Task 6/7 (с типами KafkaWorker: `KafkaWorkerOptions`, `HealthState` пространства `KafkaWorker.App`).
- Produces: `KafkaWorker.App.KafkaWorkerLoopsVitality(IOptionsMonitor<KafkaWorkerOptions>, HealthState) : ILoopsVitality` —
  3 цикла (`reconcile`/`keepalive` fast, `snapshot` свой); конструктор
  `KafkaWorkerHealth(..., IServiceProvider services)`.

- [ ] **Шаг 1: failing-тесты**

1) В `src/tests/KafkaWorker.UnitTests/App/HealthTests.cs`: добавить
`EmptyServices`-поле (как в Task 6), все `new KafkaWorkerHealth(...)` дополнить
аргументом `IServiceProvider`, кейс-ассерт секции:
```csharp
result.Data.Keys.Should().Contain("watchdog");
result.Data["watchdog"].ToString().Should().Be("disabled");
```
2) Создать `src/tests/KafkaWorker.UnitTests/App/LoopsVitalityTests.cs` — копия
теста Task 7 с точной правкой: тип `KafkaWorkerOptions`, `KafkaWorkerLoopsVitality`,
перечень из ТРЁХ циклов:
```csharp
beats.Select(b => b.Name).Should().Equal("reconcile", "keepalive", "snapshot");
```
(namespace `KafkaWorker.UnitTests.App`; `FixedOptionsMonitor` — тот же хелпер,
что используется существующими тестами проекта — проверить его расположение и
переиспользовать).

- [ ] **Шаг 2: запуск — падает**

Run: `dotnet test src/tests/KafkaWorker.UnitTests -c Debug --filter "FullyQualifiedName~HealthTests|FullyQualifiedName~LoopsVitalityTests"`
Ожидание: FAIL.

- [ ] **Шаг 3: реализация (зеркало Task 6/7)**

1) `Options.cs` — `LoopsOptions.Watchdog` (код из Task 6 п.2);
2) `appsettings.json` — секция `"Watchdog"` внутри `"Loops"` (JSON из Task 6 п.3);
3) `KafkaWorkerHealth.cs` — параметр `IServiceProvider services`; формулы →
   `LoopStaleness.FastLoops/SnapshotLoop`; data-секция `watchdog` (код из Task 6 п.1);
4) `LoopsVitality.cs`:

```csharp
using Microsoft.Extensions.Options;
using KafkaWorker.App;          // HealthState, KafkaWorkerOptions (namespace фактический)
using Shared.Core.HealthChecks;
using Shared.Core.Hosting;

namespace KafkaWorker.App;

/// <summary>Живость циклов KafkaWorker для watchdog: снимок HealthState + пороги
/// LoopStaleness × Loops:Watchdog:Multiplier; перечень — reconcile/keepalive/snapshot.</summary>
public sealed class KafkaWorkerLoopsVitality(
    IOptionsMonitor<KafkaWorkerOptions> options,
    HealthState health) : ILoopsVitality
{
    public IReadOnlyList<LoopHeartbeat> Snapshot()
    {
        var loops = options.CurrentValue.Loops;
        var multiplier = Math.Max(1, loops.Watchdog.Multiplier);
        var fast = TimeSpan.FromTicks(LoopStaleness.FastLoops(loops.ScanIntervalSec, loops.KeepaliveSec).Ticks * multiplier);
        var snapshotLoop = TimeSpan.FromTicks(LoopStaleness.SnapshotLoop(loops.ScanIntervalSec, loops.SnapshotIntervalMin).Ticks * multiplier);
        var snap = health.Snapshot();
        return
        [
            new LoopHeartbeat("reconcile", snap.LastReconcileTick, fast),
            new LoopHeartbeat("keepalive", snap.LastKeepaliveTick, fast),
            new LoopHeartbeat("snapshot", snap.LastSnapshotTick, snapshotLoop),
        ];
    }
}
```
(using привести к фактическим namespace файла `HealthState.cs` воркера —
проверить при написании; дублирующий using одного namespace не оставлять);
5) `Program.cs` — блок подключения после `.AddCheck<HealthCheckAbstract<SnapshotLoop>>("snapshot-loop")`
(зеркало Task 7: `KafkaWorkerLoopsVitality`, секция `"KafkaWorker:Loops:Watchdog"`,
`WorkerMetricsInstrumentation.WatchdogRestart`).

- [ ] **Шаг 4: запуск — зелёный + юниты KafkaWorker целиком**

Run: `dotnet test src/tests/KafkaWorker.UnitTests -c Debug`
Ожидание: PASS, 0 warnings.

- [ ] **Шаг 5: коммит**

```bash
git add src/KafkaWorker.App/Options.cs src/KafkaWorker.App/appsettings.json src/KafkaWorker.App/HealthChecks/KafkaWorkerHealth.cs src/KafkaWorker.App/LoopsVitality.cs src/KafkaWorker.App/Program.cs src/tests/KafkaWorker.UnitTests/App/HealthTests.cs src/tests/KafkaWorker.UnitTests/App/LoopsVitalityTests.cs
git commit -m "feat(t12): KafkaWorker — LoopWatchdog подключение: опции, healthz-хелпер + секция watchdog, KafkaWorkerLoopsVitality (3 цикла) (spec §4.4–4.6)"
```

### Task 9: ValkeyWorker — симметрия (§4.4–4.6 спеки, AC6)

**Файлы:**
- Modify: `src/ValkeyWorker.App/Options.cs`, `src/ValkeyWorker.App/appsettings.json`,
  `src/ValkeyWorker.App/HealthChecks/ValkeyWorkerHealth.cs`, `src/ValkeyWorker.App/Program.cs`
- Create: `src/ValkeyWorker.App/LoopsVitality.cs`
- Test (modify): `src/tests/ValkeyWorker.UnitTests/App/HealthTests.cs`
- Test (create): `src/tests/ValkeyWorker.UnitTests/App/LoopsVitalityTests.cs`

**Interfaces:** зеркало Task 8 с типами ValkeyWorker:
`ValkeyWorker.App.ValkeyWorkerLoopsVitality(IOptionsMonitor<ValkeyWorkerOptions>, HealthState) : ILoopsVitality` —
3 цикла; конструктор `ValkeyWorkerHealth(..., IServiceProvider)`; секция конфига
`"ValkeyWorker:Loops:Watchdog"`.

- [ ] **Шаги 1–5: в точности зеркало Task 8**

Шаг 1 — failing-тесты (`HealthTests.cs`: `EmptyServices` + аргумент + ассерт
`watchdog`-секции; `LoopsVitalityTests.cs` — 3 цикла, тип `ValkeyWorkerOptions`).
Шаг 2 — Run: `dotnet test src/tests/ValkeyWorker.UnitTests -c Debug --filter "FullyQualifiedName~HealthTests|FullyQualifiedName~LoopsVitalityTests"` → FAIL.
Шаг 3 — реализация: опции `Watchdog` в `LoopsOptions`, `appsettings.json`-секция,
`ValkeyWorkerHealth` (параметр + хелпер + data-секция), `ValkeyWorkerLoopsVitality`
(код Task 8 п.4 с типами Valkey), блок `AddLoopWatchdog` в Program.cs с секцией
`"ValkeyWorker:Loops:Watchdog"`.
Шаг 4 — Run: `dotnet test src/tests/ValkeyWorker.UnitTests -c Debug` → PASS, 0 warnings.
Шаг 5 — коммит:

```bash
git add src/ValkeyWorker.App/Options.cs src/ValkeyWorker.App/appsettings.json src/ValkeyWorker.App/HealthChecks/ValkeyWorkerHealth.cs src/ValkeyWorker.App/LoopsVitality.cs src/ValkeyWorker.App/Program.cs src/tests/ValkeyWorker.UnitTests/App/HealthTests.cs src/tests/ValkeyWorker.UnitTests/App/LoopsVitalityTests.cs
git commit -m "feat(t12): ValkeyWorker — LoopWatchdog подключение: опции, healthz-хелпер + секция watchdog, ValkeyWorkerLoopsVitality (3 цикла) (spec §4.4–4.6)"
```

- [ ] **Шаг 6 (фаза 3 целиком): контрольная сборка решения**

Run: `dotnet build src/PgWorker.slnx -c Debug`
Ожидание: 0 errors / 0 warnings. AC6 закрыт: один компонент Shared.Core, состав
циклов по §4.6, без копий per-app (кроме тонкой реализации ILoopsVitality).

---

## Фаза 4 — интеграционный тест (не docker)

### Task 10: самолечение хоста по staleness + экспозиция метрики (§4.7 спеки, AC2/AC4)

**Файлы:**
- Create: `src/tests/PgWorker.IntegrationTests/Hosting/LoopWatchdogTests.cs`
- Modify: `src/tests/PgWorker.IntegrationTests/Api/MetricsTests.cs`

**Interfaces:**
- Consumes: `ILoopsVitality`/`LoopWatchdog`/`LoopStaleness` (фаза 2), WAF-паттерны
  `RestartApiTests` (реальный `IHostApplicationLifetime`), `MetricsTests` (живой
  /metrics), `EtcdFixture`, `NonE2eCollection`, env-секреты Д7.

- [ ] **Шаг 1: интеграционный тест**

Создать `src/tests/PgWorker.IntegrationTests/Hosting/LoopWatchdogTests.cs`:

```csharp
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using PgWorker.App;
using Shared.Core.HealthChecks;
using Shared.Core.Hosting;
using Xunit;

namespace PgWorker.IntegrationTests.Hosting;

// Интеграционные кейсы watchdog (не docker): хост воркера с подменённой
// ILoopsVitality — устаревший тик инициирует graceful остановку в бюджет;
// свежие тики — хост живёт. Циклы не поднимаются (RemoveAll<IHostedService> +
// возврат hosted-обёртки LoopWatchdog), витальность — фейк с порогом по
// формулам от уменьшенных тестовых интервалов Loops (2/2 → healthz 21 c,
// watchdog ×2 = 42 c; тик устарел на 5 мин → firing первой проверкой).
[Collection(NonE2eCollection.Name)]
public sealed class LoopWatchdogTests
{
    private sealed class FakeVitality(bool stale) : ILoopsVitality
    {
        public IReadOnlyList<LoopHeartbeat> Snapshot()
        {
            var threshold = TimeSpan.FromSeconds(21) * 2; // FastLoops(2,2)=21 c ×2
            var at = stale ? DateTimeOffset.UtcNow - TimeSpan.FromMinutes(5)
                           : DateTimeOffset.UtcNow;
            return [new LoopHeartbeat("reconcile", at, threshold)];
        }
    }

    // Своя фабрика: циклы сняты, watchdog оставлен, витальность подменена.
    private sealed class WatchdogFactory(Etcd.EtcdFixture etcd, bool stale)
        : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Development");
            builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(
                new Dictionary<string, string?>
                {
                    ["PgWorker:Etcd:Endpoints:0"] = etcd.Endpoint,
                    ["PgWorker:Docker:Hosts:0:Name"] = "local",
                    ["PgWorker:Docker:Hosts:0:Endpoint"] = "unix:///var/run/does-not-exist.sock",
                    ["PgWorker:Api:Tls:AllowInsecureHttp"] = "true",
                    ["PgWorker:Api:AdvertiseUrl"] = "https://localhost:9996",
                    ["PgWorker:Api:EnableSeedEndpoint"] = "false",
                    // Уменьшенные интервалы Loops (пороги порядка десятков секунд).
                    ["PgWorker:Loops:ScanIntervalSec"] = "2",
                    ["PgWorker:Loops:KeepaliveSec"] = "2",
                }));
            builder.ConfigureServices(services =>
            {
                services.RemoveAll<IHostedService>(); // циклы и MeterProvider не нужны
                services.RemoveAll<ILoopsVitality>();
                services.AddSingleton<ILoopsVitality>(_ => new FakeVitality(stale));
                // LoopWatchdog-синглтон уже зарегистрирован Program.cs (Enabled=true
                // из appsettings.json) — возвращаем hosted-обёртку над ним.
                services.AddHostedService(sp => sp.GetRequiredService<LoopWatchdog>());
            });
        }
    }

    // Свой etcd + своя фабрика на тест-класс (teardown при любом исходе).
    private sealed class WatchdogHost : IAsyncLifetime
    {
        public Etcd.EtcdFixture Etcd { get; } = new();
        private WebApplicationFactory<Program>? _factory;

        public WatchdogHost()
        {
            Environment.SetEnvironmentVariable("PGW_PG_SUPERUSER_PASSWORD", "x");
            Environment.SetEnvironmentVariable("PGW_PG_STANDBY_PASSWORD", "x");
            Environment.SetEnvironmentVariable("PGW_BUCKET_ADMIN_PASSWORD", "x");
            Environment.SetEnvironmentVariable("PGW_BUCKET_MOVER_PASSWORD", "x");
        }

        public WebApplicationFactory<Program> CreateFactory(bool stale)
        {
            _factory = new WatchdogFactory(Etcd, stale);
            return _factory;
        }

        public async ValueTask InitializeAsync() => await Etcd.InitializeAsync();

        public async ValueTask DisposeAsync()
        {
            if (_factory is { } factory)
                await factory.DisposeAsync();
            await Etcd.DisposeAsync();
            Environment.SetEnvironmentVariable("PGW_PG_SUPERUSER_PASSWORD", null);
            Environment.SetEnvironmentVariable("PGW_PG_STANDBY_PASSWORD", null);
            Environment.SetEnvironmentVariable("PGW_BUCKET_ADMIN_PASSWORD", null);
            Environment.SetEnvironmentVariable("PGW_BUCKET_MOVER_PASSWORD", null);
        }
    }

    [Fact]
    public async Task StaleHeartbeat_HostStopsWithinBudget()
    {
        // Arrange: хост с устаревшим тиком; ApplicationStopping — маркер стопа
        await using var host = new WatchdogHost();
        await host.InitializeAsync();
        using var client = host.CreateFactory(stale: true).CreateClient();
        var lifetime = host.Etcd is { } _ ? null : null; // (не используется — см. ниже)
        var factoryServices = ((WatchdogFactory?)null, false); // (не используется)
        // Реальный lifetime построенного хоста берём через последнюю фабрику:
        // CreateFactory сохранил её — доступ через field ниже в шаге 3 при
        // написании теста заменить на прямой: сохранить фабрику в локальную
        // переменную и взять factory.Services.GetRequiredService<IHostApplicationLifetime>().

        // Act: ждём StopApplication (бюджет 30 c: первая проверка CheckIntervalSec=15
        // + StopDelaySec=1 + старт; каждое ожидание поллинга ≤1 c)
        // ... (полный код — см. шаг 3)

        // Assert: graceful stop инициирован watchdog'ом в бюджет
    }
}
```

ВЫШЕ — эскиз структуры; при написании файла оформить кейсы финально (без
остаточных «не используется»-строк) так:

**Кейс 1 `StaleHeartbeat_HostStopsWithinBudget`:**
```csharp
await using var host = new WatchdogHost();
await host.InitializeAsync();
var factory = host.CreateFactory(stale: true);
using var client = factory.CreateClient(); // хост собран и запущен
var lifetime = factory.Services.GetRequiredService<IHostApplicationLifetime>();
var stopping = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
lifetime.ApplicationStopping.Register(() => stopping.TrySetResult());

// Act: поллинг остановки — общий бюджет 30 c, шаг 1 c (AGENTS.base.md §12)
var winner = await Task.WhenAny(stopping.Task, Task.Delay(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken));

// Assert: watchdog уложил хост в бюджет самовосстановления (AC2)
winner.Should().Be(stopping.Task);
```

**Кейс 2 `FreshHeartbeats_HostAlive_AndHealthzWatchdogSection`:**
```csharp
await using var host = new WatchdogHost();
await host.InitializeAsync();
var factory = host.CreateFactory(stale: false);
using var client = factory.CreateClient();
var lifetime = factory.Services.GetRequiredService<IHostApplicationLifetime>();
var stopping = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
lifetime.ApplicationStopping.Register(() => stopping.TrySetResult());

// Act: 5 с наблюдения + GET /healthz (хост с живыми тиками не останавливается)
await Task.Delay(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
using var response = await client.GetAsync("/healthz", TestContext.Current.CancellationToken);
var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

// Assert: хост жив; секция watchdog в data (AC4)
stopping.Task.IsCompleted.Should().BeFalse();
response.StatusCode.Should().Be(HttpStatusCode.OK);
body.Should().Contain("watchdog");
body.Should().Contain("armed");
```

- [ ] **Шаг 2: кейс экспозиции серии в /metrics (AC4)**

В `src/tests/PgWorker.IntegrationTests/Api/MetricsTests.cs` добавить кейс:

```csharp
[Fact]
public async Task Metrics_WatchdogRestartSeries_ExposedAfterMark()
{
    // Arrange: живой хост метрик-фабрики; маркер вызывается колбэком watchdog
    var instrumentation = fx.Factory.Services.GetRequiredService<
        Shared.Metrics.Worker.WorkerMetricsInstrumentation>();

    // Act: watchdog-остановка цикла (как это сделает LoopWatchdog перед stop)
    instrumentation.WatchdogRestart("reconcile");
    using var client = fx.Factory.CreateClient();
    var body = await client.GetStringAsync("/metrics", TestContext.Current.CancellationToken);

    // Assert: серия counter с лейблом цикла в экспорте (AC4)
    body.Should().Contain("""worker_watchdog_restarts_total{otel_scope_name="PgWorker",loop="reconcile"}""");
}
```

- [ ] **Шаг 3: запуск интеграционных кейсов**

Run: `dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~LoopWatchdogTests|FullyQualifiedName~MetricsTests"`
Ожидание: PASS (etcd-фикстуры поднимаются сами, контейнеров нет; кейс 1 укладывается
в ~16–20 с — бюджет 30 с).

- [ ] **Шаг 4: полная не-E2E серия (контроль фаз 2–4)**

Перед серией — гейт чистоты ТОЛЬКО диагностикой, без удаления: серия ещё не
запущена — своего guid нет, ВСЕ контуры на демоне чужие (параллельные сессии,
dev-стенд): проверить `docker ps -a` и `docker network ls`; найденные остатки
(контейнеры/осиротевшие сети) НЕ чистить — rm/prune по общим фильтрам запрещены,
СТОП и вопрос пользователю.
Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName!~E2e"`
Ожидание: PASS — вся не-E2E серия зелёная (AC1, AC2-юнитная часть, AC4, AC5, AC6).
После серии дождаться финальной строки; контуры фикстур убирает их teardown
(testcontainers/ryuk) — руками ничего не удалять; остатки в `docker ps -a`/
`docker network ls` — СТОП и вопрос пользователю (чужое не трогать).

- [ ] **Шаг 5: коммит**

```bash
git add src/tests/PgWorker.IntegrationTests/Hosting/LoopWatchdogTests.cs src/tests/PgWorker.IntegrationTests/Api/MetricsTests.cs
git commit -m "test(t12): интеграционные кейсы watchdog — staleness останавливает хост в бюджет (30 c), живые тики + секция healthz, серия /metrics (spec §4.7, AC2/AC4)"
```

---

## Фаза 5 — мерж-гейт

### Task 11: runbook — раздел watchdog (§4.8 спеки, AC7)

**Файлы:**
- Modify: `docs/runbook.md`

- [ ] **Шаг 1: раздел**

Вставить после раздела «Вторые инстансы воркеров (t07)» (перед «PGTune-параметры»):

```markdown
## Watchdog зависших циклов воркера

Внутренний watchdog каждого воркера (PgWorker/KafkaWorker/ValkeyWorker) следит
за возрастом тиков фоновых циклов (reconcile/keepalive/snapshot; у PgWorker ещё
orphan-sweep) и самолечит зависание «без исключения»: staleness сверх порога →
graceful self-stop (тот же механизм, что `POST /api/restart`) → контейнер
поднимает docker-политика `restart: unless-stopped` → lease гаснут ≤15 с →
клэймы мигрируют второму инстансу. RTO самовосстановления ~1,5–2 мин (порог 60 с
+ shutdown ≤30 с + старт контейнера).

- **Порог** = `Loops:Watchdog:Multiplier` (дефолт 2) × порог healthz loops-alive
  (формула от интервалов циклов: быстрые 3×max(scan,keepalive)+15; snapshot — от
  `SnapshotIntervalMin`). Порог ×1 (Degraded healthz) срабатывает раньше порога
  ×2 (рестарт): между ними — окно алертов оператора (`WorkerLoopStalled`
  critical 60 с, панельный `worker-unhealthy`) и легитимно-длинных тиков.
- **Метрика**: `worker_watchdog_restarts_total{loop=…}` — инициированные
  watchdog-остановки. Растёт — цикл стабильно зависает: смотреть журналы
  воркера и причины зависания тика (не увеличивать порог вслепую).
- **Журнал**: последняя запись перед остановкой — `watchdog: цикл <loop> не
  тикал N c (порог M c) — инициирован self-restart` (critical). Docker сохраняет
  логи контейнера через рестарт — событие видно постфактум (`docker logs`).
- **Легитимное выключение** (только диагностика/временно): `Loops:Watchdog:
  Enabled=false` — компонент не регистрируется, поведение как без watchdog;
  после разбора вернуть `true`.
- **Связь с чеком 35** (`dev-stand/adminpanel/checks/35-worker-second-instance.sh`,
  kill→takeover): watchdog — та же точка самовосстановления, но по staleness
  тиков, а не по смерти контейнера; второй инстанс подхватывает клэймы в обоих
  случаях (идемпотентность + takeover).
- Граница: «завис весь процесс» (healthz не отвечает) — watchdog не лечит, это
  зона docker HEALTHCHECK; лимита частоты рестартов нет — ресторм сдерживает
  docker restart-backoff, видимость дают метрика и журнал.
```

- [ ] **Шаг 2: проверка и коммит**

Run: `grep -c "Watchdog зависших циклов" docs/runbook.md` → 1.

```bash
git add docs/runbook.md
git commit -m "docs(t12): runbook — раздел watchdog: метрика, журнал, выключение, связь с чеком 35 и WorkerLoopStalled (spec §4.8)"
```

### Task 12: мерж-гейт — E2E-маркер + roadmap-гейт (§4.8 спеки, AC3/AC7)

**Файлы:**
- Modify: `arch/roadmap/reliability.md` (снять пункт t12)
- Modify: `arch/roadmap/reliability-report.md` (строка «Осталось» → «Сделано», сводка R)

- [ ] **Шаг 1: полная сборка Release**

Run: `dotnet build src/PgWorker.slnx -c Release`
Ожидание: 0 errors / 0 warnings.

- [ ] **Шаг 2: E2E-маркер на свежем Release (AC3)**

Перед серией — гейт чистоты ТОЛЬКО диагностикой, без удаления: серия ещё не
запущена — своего guid нет, любые контуры `pgw-*`/`kfw-net-*` и стенд (`as-*`,
`adminpanel`) — чужие и могут быть живыми (параллельные сессии). Проверить
`docker ps -a` и `docker network ls`; при остатках НЕ чистить (`docker rm`/
`network prune` по общему фильтру запрещены — так сносится живой контур
соседней сессии) — СТОП и вопрос пользователю.
Run (в фоне, следить за прогрессом; фикстура собирает Release сама — no-op):

```
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard
```

Ожидание: PASS 1/1 — инстансы воркеров проживают полный provisioning без
остановки (нет ложных рестартов watchdog на живом docker-контуре).

- [ ] **Шаг 3: AC3 — нет ложных срабатываний за серию**

По артефактам упавшего/зелёного прогона (teardown сохраняет логи в
`/tmp/pgw-e2e-artifacts-<guid>/` — найти последний каталог) и docker-логам
инстансов серии проверить:
Run: `grep -r "инициирован self-restart" /tmp/pgw-e2e-artifacts-*/ 2>/dev/null | wc -l` → `0`
(счётчик `worker_watchdog_restarts_total` инкрементируется той же точкой кода,
что пишет журнал — отсутствие записи = отсутствие инкремента).
После проверки — страховочный own-only гейт СВОЕЙ серии (основная чистка —
teardown фикстуры, docs/e2e-isolation.md): взять guid своей серии из прогона
(E2eEnvironment: runId в именах `pgw-en-{runId}`/`pgw-ee*-{runId}`/
`pgw-em-{runId}`; движковые объекты — тег кластера: `pgw-{C}-*`, `pgw-net-{C}`);
перечислить кандидатов ДО удаления (`docker ps -aq --filter name={runId}` и
`--filter name={C}`, `docker network ls | grep -e {runId} -e {C}`); убедиться,
что контуры не живые (серия завершена, процесса-владельца нет); удалить только
своих и поимённо (`docker rm -f <имя>`, `docker network rm <имя>`); контроль —
`docker ps -aq --filter name={runId}` → 0. Кандидат без своего guid/тега или с
признаками живого владельца — СТОП и вопрос пользователю; `prune` не
использовать; контейнеры, оставленные телеметрией упавшего сценария
(`MarkFailed`), не удалять до окончания разбора.

- [ ] **Шаг 4: roadmap-гейт (мерж-коммит закрытия)**

1) `arch/roadmap/reliability.md` — удалить пункт `t12-loop-watchdog` (строки
«- **`t12-loop-watchdog`** — watchdog зависших циклов воркера: …»); `←`-зависимостей
у t12 нет (проверить grep `t12-loop-watchdog` по `arch/roadmap/*.md` — других
вхождений быть не должно).
2) `arch/roadmap/reliability-report.md`:
   - таблица «Осталось» — удалить строку `| `t12-loop-watchdog` | watchdog зависших циклов воркера | P2 | R |`;
   - сводка R «Открытые разрывы» — убрать фрагмент «нет watchdog зависших циклов
     (`t12`);» (строка станет: «Открытые разрывы: зависшие ротационные заявки
     без таймаутов (`t10`); Swarm без ускоренного failover (`t11`);
     сценарии отказа хоста/DC не отработаны (`t20`, `t21`).»);
   - таблица «Сделано в рамках трека» — добавить строку сверху:

```markdown
| `t12-loop-watchdog` | — (мерж-коммит t12-loop-watchdog) | зависший без исключения цикл самолечется self-restart'ом за ~минуту (внутренний LoopWatchdog Shared.Core у всех трёх воркеров, порог ×2 от healthz, graceful StopApplication — путь POST /api/restart), lease гаснут ≤15 с, takeover вторым инстансом не блокируется; BackupOrphanSweeperLoop получил тики живости (loops-alive + watchdog), метрика worker_watchdog_restarts_total{loop}, runbook-раздел |
```

- [ ] **Шаг 5: контроль гейта и финальный коммит ветки**

Run: `grep -rn "t12-loop-watchdog" arch/roadmap/ | grep -v "reliability-report.md.*Сделано\|мерж-коммит t12"` → пусто (пункт снят, `←`-ссылок нет);
`DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName!~E2e"` → PASS (контрольная не-E2E серия).

```bash
git add arch/roadmap/reliability.md arch/roadmap/reliability-report.md
git commit -m "merge-gate(t12): пункт t12-loop-watchdog снят из roadmap; reliability-report — перенос в «Сделано» + сводка R (мерж-гейт трека reliability)"
```

Дальше — ревью и мерж в main по отдельному приказу пользователя (этот коммит
становится частью мерж-коммита закрытия).

---

## Соответствие задач критериям приёмки спеки

| AC | Чем закрыт |
|---|---|
| AC1 (юнит — механика) | Task 2 (формулы/симметрия), Task 3 (grace, firing once, живые, Enabled=false), Task 4 |
| AC2 (интеграция — самолечение) | Task 10 кейс 1/2 |
| AC3 (нет ложных срабатываний) | Task 12 шаги 2–3 (E2E-маркер + grep артефактов) |
| AC4 (наблюдаемость) | Task 4 (counter), Task 6 (healthz-секция), Task 10 шаг 2 (/metrics), Task 3 (критический журнал) |
| AC5 (полнота перечня) | Task 5 (тики sweeper), Task 6 (loops-alive), Task 7 (vitality) |
| AC6 (симметрия трёх воркеров) | Task 3 (один компонент), Task 7/8/9 (подключение, состав §4.6) |
| AC7 (канон и гейт) | Task 1 (arch), Task 11 (runbook), Task 12 (roadmap-гейт) |

## Самопроверка плана (выполнена при написании)

- Покрытие спеки: все пункты §4.1–§4.8 и §5 имеют задачи; открытые решения
  Q1–Q5 шапки спеки не пересматриваются (watchdog внутренний, 3 воркера,
  graceful StopApplication, порог ×2 конфигурируемый, без анти-лупа).
- Плейсхолдеров нет; код шагов полный (эскиз Task 10 шаг 1 явно помечен и
  заменяется финальными кейсами в шаге описания — при исполнении писать кейсы 1/2
  финальным кодом без остаточных строк).
- Консистентность имён: `LoopStaleness.FastLoops/SnapshotLoop`,
  `LoopHeartbeat(Name, LastTickAt, StaleAfter)`, `ILoopsVitality.Snapshot()`,
  `WatchdogOptions{Enabled,Multiplier,CheckIntervalSec,StopDelaySec}`,
  `LoopWatchdog(..., restartMark)`, `AddLoopWatchdog(options, restartMark)`,
  `WatchdogRestart(string loop)`, `MarkOrphanSweepTick()`,
  `HealthSnapshot.LastOrphanSweepTick`, `PgWorkerLoopsVitality`/
  `KafkaWorkerLoopsVitality`/`ValkeyWorkerLoopsVitality` — едины по всему плану.
