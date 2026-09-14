# t11 — конвергенция pg-параметров работающих нод: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Живой конфиг PostgreSQL нод работающих шардов автоматически выравнивается с merge(PGTune ∪ канон) от актуальных заявок через PATCH /config Patroni-REST (динамика — сразу, postmaster — `pending_restart`), без пересоздания нод и без инициирования рестартов воркером.

**Architecture:** Расширяется существующий контур конвергенции t09 (`NodeSupervisor.ConvergeDcsConfigAsync`, шаг 4 тика надзора): GET /config → построение минимального патча → один PATCH на тик. Единый источник желаемого набора — новый `PgParametersCanon.Desired` (Core/Tuning), на который переключается и `SpiloEnvBuilder` (bootstrap SPILO_CONFIGURATION, байт-в-байт инвариант). Построение патча — новый `DcsConfigConvergence` (Core/Templates), поглощающий `PatroniTimings.DivergencePatch`. Новых процессов, циклов и etcd-ключей нет; etcd-контракт не меняется.

**Tech Stack:** .NET 10, C# `LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`; новые компоненты Core — только BCL, без NuGet. Тесты: xUnit + FluentAssertions, AAA-комментарии; docker-тесты — Testcontainers (`WithPortBinding(..., assignRandomHostPort: true)`), изоляция по `docs/e2e-isolation.md`.

**Spec:** `docs/superpowers/2026-09-14-t11-pgtune-params-convergence/spec.md` (рядом с этим планом; исполнитель читает spec и план вместе).

**Решения пользователя, зашитые в план** (spec, шапка «Решения пользователя»):
- 2026-09-14, рестарт-политика — вариант 1: только PATCH /config + `pending_restart`, БЕЗ авто-рестарта; воркер НИКОГДА не инициирует рестарт PG.
- 2026-09-14, неполная заявка — SKIP + warning, не фейл тика: skip pg-части конвергенции при ЛЮБОЙ неполной заявке (ресурсов нет вовсе ИЛИ любой из `MemoryBytes`/`CpuCores` null); тайминги конвергируются как обычно; «исключение = фейл» остаётся только для прочих исключений расчёта.

## Global Constraints

- .NET 10, `Nullable=enable`, `TreatWarningsAsErrors=true`; централизованное версионирование `Directory.Packages.props`; новые компоненты Core — только BCL, без NuGet (spec §6).
- Воркер НИКОГДА не инициирует рестарт PG: `POST /restart` и `POST /switchover` в контексте конвергенции не вызываются; `pending_restart` воркером не читается и решений на нём не строится (spec §2 п.5, §6).
- etcd-контракт не меняется вообще: новых ключей нет, панель не затрагивается (spec §3).
- Инвариант P3 (`arch/12`): `wal_level=logical`, `max_wal_senders=10`, `max_replication_slots=10`, `sync_replication_slots`, `max_slot_wal_keep_size`, `wal_keep_size` — канон поверх PGTune всегда (spec §1).
- Инвариант P15 (`arch/12`): doorman-бюджет = `max(10, max_connections − 5)`, обновляется только при создании контейнера; воркер живые контейнеры не трогает (spec §1).
- `SPILO_CONFIGURATION` генерируется байт-в-байт как сегодня: существующие `NodeConfigBuildersTests` зелёные без правки expects (spec §4.1, §7 п.4).
- Один PATCH на тик на шард: тайминги и параметры в одном документе `{"ttl":20,…,"postgresql":{"parameters":{…}}}` (spec §2 п.6).
- Комментарии/доки — по-русски, идентификаторы — английские; тесты с AAA-комментариями (`// Arrange`, `// Act`, `// Assert`).
- Docker-тесты: только динамические хост-порты, полный teardown при любом исходе, guid-теги во всех именах, own-only чистка, ассерт чистоты (`docs/e2e-isolation.md`); `BrokerBootSec` ≤ 100 с.
- Arch-first: правки `arch/14-pgworker.md` пишутся первыми, коммитятся тем же коммитом, что и код (spec §5 фаза 1).
- Все пути ниже — от корня worktree `/Users/demakaev/ZCodeProject/worktrees/feat-t11-pgtune-params-convergence`.

---

### Task 1: Канон arch/ + единый желаемый набор `PgParametersCanon` + рефакторинг `SpiloEnvBuilder`

Фазы spec §5: 1 (канон arch) + 2 (желаемый набор). Arch-first: сначала правки `arch/14`, затем TDD кода; коммит один — arch + код + тесты.

**Files:**
- Modify: `arch/14-pgworker.md` (§2.1, строки ~236–243; §5 C, строки ~759–765)
- Create: `src/PgWorker.Core/Tuning/PgParametersCanon.cs`
- Modify: `src/PgWorker.Core/Templates/NodeConfigBuilders.cs` (`SpiloEnvBuilder`: удалить `CanonParameters`, `TunedParametersBlock` переключить на `PgParametersCanon.Desired`)
- Test: `src/tests/PgWorker.UnitTests/Tuning/PgParametersCanonTests.cs` (новое)
- Test (гарант инварианта, без правок): `src/tests/PgWorker.UnitTests/Templates/NodeConfigBuildersTests.cs`

**Interfaces:**
- Consumes: `PgTuneResult` (`PgWorker.Core.Tuning.PgTune`, `IReadOnlyList<PgTuneParameter> Parameters` — порядок §5.2).
- Produces (используют Task 2, 3):
  - `PgParametersCanon.Desired(PgTuneResult tuning, IReadOnlySet<string>? excludeParams)` → `IReadOnlyList<(string Name, string RawValue)>` — сырые строки без кавычек ("60", "2047MB", "on", "logical");
  - `PgParametersCanon.CanonParameters` — `IReadOnlyList<(string Name, string RawValue)>`, канон P3 + лог-блок со значениями БЕЗ кавычек (переехал из `SpiloEnvBuilder`).

- [ ] **Step 1: Правка канона arch/14 §5 C — пункт «Конвергенция DCS-конфига (t09)»**

Заменить в `arch/14-pgworker.md` (§5 C) абзац, начинающийся с `- **Конвергенция DCS-конфига (t09)**: раз в тик надзора...`, на:

```markdown
- **Конвергенция DCS-конфига (t09; pg-параметры — t11)**: раз в тик надзора
  по одному узлу шарда — GET /config; расхождение с каноном (§2.1:
  ttl/loop_wait/retry_timeout/synchronous_mode + `postgresql.parameters` =
  merge(PGTune ∪ канон) от актуальных заявок `request_{cpu,mem}` и опций
  `PgWorker:Pgtune`, пересчёт на каждый тик конвергенции, БЕЗ фиксации в
  etcd) → ОДИН PATCH /config: обновляет расходящиеся значения, добавляет
  отсутствующие и удаляет лишние (null-патч — параметр исчез из PGTune-вывода
  при смене заявок или добавлен в `ExcludeParams`; «старое не живёт параллельно
  канону», включая вручную записанные оператором ключи — канал переопределения
  один: опции `PgWorker:Pgtune` + `ExcludeParams`). Динамические параметры
  Patroni применяет сам (reload в пределах loop_wait); postmaster-параметры
  (`max_connections`, `shared_buffers`, …) Patroni помечает `pending_restart`
  (GET /patroni) — применяются при ближайшем рестарте ноды (rebuild/эвакуация/
  оператор; решение 2026-09-14: воркер НИКОГДА не инициирует рестарт PG —
  `POST /restart`/`POST /switchover` в контексте конвергенции не используются;
  `pending_restart=true` — штатное состояние, не алерт). Заявка
  `request_{cpu,mem}` отсутствует или неполна (любое из полей ресурсов null) —
  pg-часть патча пропускается с warning-логом воркера (в конвергенции мутация
  опциональна: пропуск = конфиг остаётся прежним, безопасно; выдумывать размер
  ноды по остаточному ресурсу нельзя), тайминги конвергируются как раньше.
  Нода, начавшая работу на чужом/дефолтном/мусорном конфиге, приводится к
  канону без пересоздания; патч недоступен (шард мёртв) — лечат общие контуры
  (rebuild/эвакуация).
```

- [ ] **Step 2: Правка канона arch/14 §2.1 — абзац «Параметры PG в bootstrap.dcs — PGTune»**

В `arch/14-pgworker.md` §2.1 в конце абзаца (после «...консистентность на операторе).»):

Удалить:
```markdown
Конвергенция pg-параметров
работающих нод — out of scope (отдельная задача roadmap; в отличие от
таймингов Patroni, конвергенция DCS на pg-параметры не распространяется —
`max_connections`/`shared_buffers` требуют рестарта PG).
```

Вставить на это место:
```markdown
Конвергенция pg-параметров
работающих нод реализована (t11): желаемый набор — единый источник и для
bootstrap (SPILO_CONFIGURATION), и для живого конфига (DCS
`postgresql.parameters` через PATCH /config, механизм §5 C); bootstrap и
конвергенция не могут разойтись по определению. Дрейф конфига между нодами
шарда при изменении заявок устранён на уровне DCS: динамика применяется
сразу (reload в пределах loop_wait), postmaster-параметры — через
`pending_restart` до ближайшего рестарта ноды (решение 2026-09-14: воркер
никогда не инициирует рестарт PG). Doorman-бюджет обновляется при создании
контейнера (P15 согласован: фактическое применение postmaster идёт через
rebuild — пересоздание контейнера пишет и новый `DOORMAN_CONFIG`).
```

Проверка правок канона (Step 1 + Step 2):

Run: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t11-pgtune-params-convergence && ! rg -q "конвергенция DCS на pg-параметры не распространяется" arch/14-pgworker.md && rg -c "t11" arch/14-pgworker.md`
Expected: код возврата 0; вывод счётчика ≥ 2 (t11 упомянут и в §5 C из Step 1, и в §2.1 из Step 2; фраза out-of-scope удалена). Если `rg` недоступен — эквивалент: `! grep -q "конвергенция DCS на pg-параметры не распространяется" arch/14-pgworker.md && grep -c "t11" arch/14-pgworker.md`.

- [ ] **Step 3: Написать failing-тесты `PgParametersCanonTests`**

Создать `src/tests/PgWorker.UnitTests/Tuning/PgParametersCanonTests.cs`:

```csharp
using PgWorker.Core.Tuning;

namespace PgWorker.UnitTests.Tuning;

// PgParametersCanon (t11, spec §4.1): единый желаемый набор merge(PGTune ∪
// канон) — один источник для bootstrap (SpiloEnvBuilder) и конвергенции
// (DcsConfigConvergence). Значения — СЫРЫЕ строки без YAML/JSON-обвязки.
public class PgParametersCanonTests
{
    // Вход-эталон: oltp/Linux/8GiB/4cpu/60/ssd/mid_ram (как NodeConfigBuildersTests).
    private static PgTuneResult Tuning() => PgTune.Calculate(new PgTuneInput(
        18, PgTuneOsType.Linux, PgTuneDbType.Oltp, 8388608, PgTuneMemoryUnit.KB,
        4, 60, PgTuneHdType.Ssd, PgTuneDbSize.MidRam));

    // AAA: merge — сначала PGTune-вывод в порядке §5.2, затем канон поверх с
    // перезаписью по имени без дубликатов (позиция первого вхождения
    // сохраняется, новые ключи канона — в конец).
    [Fact]
    public void Desired_MergesPgTuneOrderWithCanonOverlay()
    {
        // Arrange — tuning эталонного входа, exclude пуст.
        // Act
        var desired = PgParametersCanon.Desired(Tuning(), null);
        var names = desired.Select(p => p.Name).ToList();

        // Assert: PGTune-порядок §5.2 сохранён для PGTune-имён...
        int Index(string name) => names.FindIndex(n => n == name);
        Index("max_connections").Should().BeLessThan(Index("shared_buffers"));
        Index("shared_buffers").Should().BeLessThan(Index("effective_cache_size"));
        Index("effective_cache_size").Should().BeLessThan(Index("work_mem"));
        // ...канон-ключи, которых нет в PGTune-выводе, — в конце.
        Index("wal_keep_size").Should().BeGreaterThan(Index("work_mem"));
        Index("logging_collector").Should().BeGreaterThan(Index("work_mem"));
        // Дубликатов нет.
        names.Should().OnlyHaveUniqueItems();
        // Имена канона — фиксированный состав (P3 + лог-блок).
        PgParametersCanon.CanonParameters.Select(p => p.Name).Should().Equal(
            "wal_level", "hot_standby", "sync_replication_slots",
            "max_slot_wal_keep_size", "max_wal_senders", "max_replication_slots",
            "wal_keep_size", "checkpoint_timeout", "logging_collector",
            "log_directory", "log_filename", "log_rotation_age", "log_rotation_size");
    }

    // AAA: канон перезаписывает PGTune-значение по имени (desktop-вывод несёт
    // wal_level=minimal/max_wal_senders=0 — PgTune выводит их ТОЛЬКО для
    // desktop, §4.14 алгоритма; канон даёт logical/10 — P3 поверх PGTune всегда).
    [Fact]
    public void Desired_CanonOverwritesPgTuneByName()
    {
        // Arrange — desktop-вход: только эта ветвь выводит имена, общие с
        // каноном (wal_level/max_wal_senders), — проверяется именно ПЕРЕЗАПИСЬ.
        var desktop = PgTune.Calculate(new PgTuneInput(
            18, PgTuneOsType.Linux, PgTuneDbType.Desktop, 8388608, PgTuneMemoryUnit.KB,
            4, 60, PgTuneHdType.Ssd, PgTuneDbSize.MidRam));
        desktop["wal_level"].Should().Be("minimal", "вход обязан нести PGTune-ветвь wal_level");
        desktop["max_wal_senders"].Should().Be("0", "вход обязан нести PGTune-ветвь max_wal_senders");

        // Act
        var desired = PgParametersCanon.Desired(desktop, null);

        // Assert: wal_level один и он канонический (PGTune-значение ПЕРЕЗАПИСАНО).
        desired.Count(p => p.Name == "wal_level").Should().Be(1);
        desired.Single(p => p.Name == "wal_level").RawValue.Should().Be("logical");
        // Канон-значения P3 фиксированы (перезапись по имени, не дубль).
        desired.Count(p => p.Name == "max_wal_senders").Should().Be(1);
        desired.Single(p => p.Name == "max_wal_senders").RawValue.Should().Be("10");
        desired.Single(p => p.Name == "max_replication_slots").RawValue.Should().Be("10");
    }

    // AAA: exclude вырезает параметр из PGTune-вывода ВООБЩЕ (никаких пустых
    // значений); канон exclude не касается.
    [Fact]
    public void Desired_ExcludeCutsParameter()
    {
        // Arrange — вход, выводящий io_method/io_workers (Windows, 72cpu).
        var tuning = PgTune.Calculate(new PgTuneInput(
            18, PgTuneOsType.Windows, PgTuneDbType.Oltp, 8388608, PgTuneMemoryUnit.KB,
            72, 60, PgTuneHdType.Ssd, PgTuneDbSize.MidRam));
        var exclude = new HashSet<string>(["io_method", "io_workers"], StringComparer.Ordinal);

        // Act
        var desired = PgParametersCanon.Desired(tuning, exclude);

        // Assert
        desired.Select(p => p.Name).Should().NotContain("io_method").And.NotContain("io_workers");
        desired.Should().Contain(p => p.Name == "max_connections");
    }

    // AAA: значения — СЫРЫЕ строки без кавычек (цитирование — деталь
    // сериализатора SpiloEnvBuilder; JSON-патч тоже пишет их как строки).
    [Fact]
    public void Desired_ValuesAreRawWithoutQuotes()
    {
        // Arrange/Act
        var desired = PgParametersCanon.Desired(Tuning(), null);

        // Assert
        desired.Single(p => p.Name == "max_connections").RawValue.Should().Be("60");
        desired.Single(p => p.Name == "shared_buffers").RawValue.Should().Be("2GB");
        desired.Single(p => p.Name == "wal_level").RawValue.Should().Be("logical");
        PgParametersCanon.CanonParameters.Single(p => p.Name == "hot_standby").RawValue
            .Should().Be("on");
        PgParametersCanon.CanonParameters.Single(p => p.Name == "wal_keep_size").RawValue
            .Should().Be("2048MB");
        desired.Should().OnlyContain(p => !p.RawValue.Contains('"'));
    }

    // AAA: детерминизм — одинаковый вход даёт эквивалентный набор.
    [Fact]
    public void Desired_Deterministic()
    {
        // Arrange/Act
        var first = PgParametersCanon.Desired(Tuning(), null);
        var second = PgParametersCanon.Desired(Tuning(), null);

        // Assert
        first.Should().Equal(second);
    }
}
```

- [ ] **Step 4: Прогнать новые тесты — обязаны падать (нет типа)**

Run: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t11-pgtune-params-convergence && dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~PgParametersCanonTests"`
Expected: ошибка компиляции `CS0103`/`CS0246` — `PgParametersCanon` не найден.

- [ ] **Step 5: Реализовать `PgParametersCanon`**

Создать `src/PgWorker.Core/Tuning/PgParametersCanon.cs`:

```csharp
namespace PgWorker.Core.Tuning;

/// <summary>
/// Единый желаемый набор pg-параметров (t11, arch/14 §2.1/§5 C):
/// merge(PGTune ∪ канон) — ОДИН источник и для bootstrap (SpiloEnvBuilder,
/// SPILO_CONFIGURATION), и для конвергенции живого DCS-конфига
/// (DcsConfigConvergence, PATCH /config). Bootstrap и конвергенция не могут
/// разойтись по определению. Значения — СЫРЫЕ строки ("60", "2047MB", "on",
/// "logical") без YAML/JSON-обвязки: цитирование — деталь сериализатора.
/// </summary>
public static class PgParametersCanon
{
    /// <summary>
    /// Канон PgWorker поверх PGTune: P3 (логическое декодирование + failover
    /// slots, wal_level=logical всегда) и лог-блок. Константы
    /// max_connections/shared_buffers/effective_cache_size/
    /// checkpoint_completion_target/random_page_cost из канона УБРАТЫ — их
    /// несёт PGTune. Значения БЕЗ кавычек (переехали из SpiloEnvBuilder, t11).
    /// </summary>
    public static readonly IReadOnlyList<(string Name, string RawValue)> CanonParameters =
    [
        ("wal_level", "logical"), // P3: логическое декодирование + failover slots
        ("hot_standby", "on"),
        ("sync_replication_slots", "on"),
        ("max_slot_wal_keep_size", "16GB"),
        ("max_wal_senders", "10"),
        ("max_replication_slots", "10"),
        ("wal_keep_size", "2048MB"),
        ("checkpoint_timeout", "15min"),
        ("logging_collector", "on"),
        ("log_directory", "log"),
        ("log_filename", "postgresql-%Y-%m-%d.log"),
        ("log_rotation_age", "1d"),
        ("log_rotation_size", "100MB"),
    ];

    /// <summary>
    /// Merge(PGTune ∪ канон): сначала вывод PgTune.Calculate в порядке §5.2
    /// спецификации алгоритма (минус excludeParams — параметр отсутствует
    /// вовсе, никаких пустых значений; exclude применяется ЗДЕСЬ, ядро всегда
    /// даёт полный вывод), затем канон P3/лог-блок поверх с перезаписью по
    /// имени без дубликатов: позиция первого вхождения сохраняется, новые
    /// ключи канона — в конец.
    /// </summary>
    public static IReadOnlyList<(string Name, string RawValue)> Desired(
        PgTuneResult tuning, IReadOnlySet<string>? excludeParams)
    {
        var exclude = excludeParams ?? new HashSet<string>(StringComparer.Ordinal);
        var lines = new List<(string Name, string RawValue)>();
        var positions = new Dictionary<string, int>(StringComparer.Ordinal);

        void Upsert(string name, string rawValue)
        {
            if (positions.TryGetValue(name, out var position))
            {
                lines[position] = (name, rawValue);
                return;
            }

            positions[name] = lines.Count;
            lines.Add((name, rawValue));
        }

        foreach (var parameter in tuning.Parameters)
            if (!exclude.Contains(parameter.Name))
                Upsert(parameter.Name, parameter.Value);
        foreach (var (name, rawValue) in CanonParameters)
            Upsert(name, rawValue);

        return lines;
    }
}
```

- [ ] **Step 6: Прогнать тесты PgParametersCanonTests — зелёные**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~PgParametersCanonTests"`
Expected: PASS 5/5.

- [ ] **Step 7: Переключить `SpiloEnvBuilder.TunedParametersBlock` на `PgParametersCanon.Desired`**

В `src/PgWorker.Core/Templates/NodeConfigBuilders.cs`:
1. Удалить приватный массив `CanonParameters` (строки ~141–156) — он переехал в `PgParametersCanon` со значениями без кавычек.
2. Заменить тело `TunedParametersBlock` (строки ~164–189) на:

```csharp
    // Merge PGTune ∪ канон — единый источник PgParametersCanon.Desired (t11);
    // YAML-цитирование — деталь ЭТОГО сериализатора: все значения в двойных
    // кавычках, КРОМЕ wal_level (исторический стиль raw-string, инвариант
    // байт-в-байт SPILO_CONFIGURATION).
    private static string TunedParametersBlock(PgTuneResult tuning, IReadOnlySet<string>? excludeParams)
    {
        var desired = PgParametersCanon.Desired(tuning, excludeParams);
        return string.Join("\n", desired.Select(p =>
            $"        {p.Name}: {(p.Name == "wal_level" ? p.RawValue : $"\"{p.RawValue}\"")}"));
    }
```

3. Обновить doc-комментарий `SpiloEnvBuilder.Build` (строки ~31–35): merge строится в `PgParametersCanon.Desired` (единый источник с конвергенцией t11), `SpiloEnvBuilder` только цитирует значения для YAML.

- [ ] **Step 8: Прогнать инвариант — NodeConfigBuildersTests зелёные БЕЗ правки expects**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~NodeConfigBuildersTests"`
Expected: PASS — SPILO_CONFIGURATION байт-в-байт не изменился (expects вида `max_connections: "60"`, `wal_level: logical` без кавычек остаются верными; `wal_level` исключение из цитирования сохранено).

Run: `dotnet build src/PgWorker.slnx -c Debug`
Expected: BUILD SUCCEEDED (0 errors, 0 warnings — `TreatWarningsAsErrors`).

- [ ] **Step 9: Полный юнит-прогон PgWorker + коммит**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug`
Expected: PASS.

```bash
git add arch/14-pgworker.md src/PgWorker.Core/Tuning/PgParametersCanon.cs \
  src/PgWorker.Core/Templates/NodeConfigBuilders.cs \
  src/tests/PgWorker.UnitTests/Tuning/PgParametersCanonTests.cs
git commit -m "feat(t11): единый желаемый набор PgParametersCanon.Desired — merge(PGTune ∪ канон) как один источник bootstrap и конвергенции; SpiloEnvBuilder переключён на него (инвариант SPILO_CONFIGURATION байт-в-байт); канон arch/14 §2.1/§5 C дополнен конвергенцией pg-параметров (тем же коммитом, arch-first)"
```

---

### Task 2: Патч-билдер `DcsConfigConvergence` — поглощение `PatroniTimings.DivergencePatch`

Фаза spec §5: 3. TDD: сначала миграция кейсов t09 на новый API (падение — типа нет), затем реализация, затем зелёный прогон.

**Files:**
- Create: `src/PgWorker.Core/Templates/DcsConfigConvergence.cs`
- Modify: `src/PgWorker.Core/Templates/PatroniTimings.cs` (удалить `DivergencePatch` + оба `AddIfDivergent`; константы `Ttl/LoopWait/RetryTimeout/SynchronousMode` остаются)
- Modify: `src/PgWorker.Provisioning/Processes/NodeSupervisor.cs:308` (временная склейка на новый API — см. Step 6)
- Test: `src/tests/PgWorker.UnitTests/Templates/DcsConfigConvergenceTests.cs` (новое — миграция кейсов t09 + новые)
- Modify: `src/tests/PgWorker.UnitTests/Templates/PatroniTimingsTests.cs` (кейсы Divergence мигрируют; self-check обновляется на новый API)

**Interfaces:**
- Consumes: `PgParametersCanon.Desired` (Task 1); константы `PatroniTimings.Ttl/LoopWait/RetryTimeout/SynchronousMode`.
- Produces (использует Task 3):
  - `DcsConfigConvergence.DivergencePatch(string? configJson, IReadOnlyList<(string Name, string RawValue)>? desiredParameters)` → `string?` (null = конвергентно);
  - `DcsConfigConvergence.Analyze(string? configJson, IReadOnlyList<(string Name, string RawValue)>? desiredParameters)` → `ConvergenceDivergence(string? Patch, int Updated, int Added, int Removed, bool PostmasterTouched)` — счётчики и пометка для журнала dcs-converge;
  - `DcsConfigConvergence.PostmasterParameters` — `IReadOnlySet<string>` (имена только для ТЕКСТА журнала).

- [ ] **Step 1: Создать `DcsConfigConvergenceTests` — миграция кейсов t09 + новые кейсы**

Создать `src/tests/PgWorker.UnitTests/Templates/DcsConfigConvergenceTests.cs`:

```csharp
using System.Text.Json;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Core.Tuning;

namespace PgWorker.UnitTests.Templates;

// DcsConfigConvergence (t11, spec §4.2): минимальный патч-документ для PATCH
// /config — тайминги Patroni (PatroniTimings, единый канон t09) +
// postgresql.parameters от PgParametersCanon.Desired. Поглощает
// PatroniTimings.DivergencePatch (кейсы Regression_T09_* мигрированы, смысл и
// имена сохранены). Конвергентно → null («не второй регулярный писатель»).
public class DcsConfigConvergenceTests
{
    // Desired-набор эталонного входа (oltp/8GiB/4cpu/60/ssd/mid_ram, exclude пуст).
    private static IReadOnlyList<(string Name, string RawValue)> Desired() =>
        PgParametersCanon.Desired(PgTune.Calculate(new PgTuneInput(
            18, PgTuneOsType.Linux, PgTuneDbType.Oltp, 8388608, PgTuneMemoryUnit.KB,
            4, 60, PgTuneHdType.Ssd, PgTuneDbSize.MidRam)), null);

    // ---------- Миграция кейсов t09 (desired == null → патч только таймингов) ----------

    // AAA (t09): дефолтный конфиг Patroni — патч несёт ВСЕ канонические поля.
    [Fact]
    public void Regression_T09_Divergence_DefaultConfig_PatchCarriesCanonical()
    {
        // Arrange — динамический конфиг на Patroni-дефолтах.
        const string config = """{"ttl":30,"loop_wait":10,"retry_timeout":10,"postgresql":{"use_pg_rewind":true}}""";

        // Act
        var patch = DcsConfigConvergence.DivergencePatch(config, null);

        // Assert: все канонические тайминги в патче; postgresql не тронут
        // (desired == null — параметры вне игры).
        patch.Should().NotBeNull();
        patch.Should().Contain("\"ttl\":20").And.Contain("\"loop_wait\":1")
            .And.Contain("\"retry_timeout\":3").And.Contain("\"synchronous_mode\":true");
        patch.Should().NotContain("postgresql");
    }

    // AAA (t09): конфиг, молча скорректированный Patroni 4.1 — патч минимальный.
    [Fact]
    public void Regression_T09_Divergence_PatroniAdjustedConfig_MinimalPatch()
    {
        // Arrange — фактический /config из диагностики t09.
        const string config = """{"ttl":20,"loop_wait":2,"retry_timeout":3,"synchronous_mode":true}""";

        // Act
        var patch = DcsConfigConvergence.DivergencePatch(config, null);

        // Assert
        patch.Should().Be("""{"loop_wait":1}""");
    }

    // AAA (t09): канонический конфиг — null, мутаций нет.
    [Fact]
    public void Regression_T09_Divergence_CanonicalConfig_NoPatch()
    {
        // Arrange
        const string config = """{"ttl":20,"loop_wait":1,"retry_timeout":3,"synchronous_mode":true}""";

        // Act
        var patch = DcsConfigConvergence.DivergencePatch(config, null);

        // Assert
        patch.Should().BeNull();
    }

    // AAA (t09): пустой/битый/чужой ответ — полный патч (тайминги + весь desired).
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("not-json{")]
    [InlineData("""{"foreign":"document"}""")]
    public void Regression_T09_Divergence_GarbageConfig_PatchAllCanonical(string? config)
    {
        // Arrange — непонятный конфиг приводится к канону.

        // Act
        var patch = DcsConfigConvergence.DivergencePatch(config, Desired());

        // Assert: тайминги + все параметры желаемого набора в патче.
        patch.Should().NotBeNull();
        patch.Should().Contain("\"ttl\":20").And.Contain("\"loop_wait\":1")
            .And.Contain("\"retry_timeout\":3").And.Contain("\"synchronous_mode\":true");
        patch.Should().Contain("\"postgresql\":{\"parameters\":{")
            .And.Contain("\"max_connections\":\"60\"")
            .And.Contain("\"wal_level\":\"logical\"");
    }

    // ---------- Новые кейсы t11 ----------

    // AAA: расходящееся значение параметра → обновление в патче; счётчик Updated.
    [Fact]
    public void Divergence_ParameterValueDiffers_UpdatedInPatch()
    {
        // Arrange — живой конфиг каноничен по таймингам; shared_buffers от
        // прежней заявки (1GB вместо 2GB), лишних ключей нет.
        var desired = new List<(string Name, string RawValue)>
        {
            ("max_connections", "60"), ("shared_buffers", "2GB"),
        };
        const string config =
            """{"ttl":20,"loop_wait":1,"retry_timeout":3,"synchronous_mode":true,"postgresql":{"parameters":{"max_connections":"60","shared_buffers":"1GB"}}}""";

        // Act
        var divergence = DcsConfigConvergence.Analyze(config, desired);

        // Assert: только расходящийся параметр; updated=1; патч без таймингов.
        divergence.Patch.Should().Be(
            """{"postgresql":{"parameters":{"shared_buffers":"2GB"}}}""");
        divergence.Updated.Should().Be(1);
        divergence.Added.Should().Be(0);
        divergence.Removed.Should().Be(0);
        divergence.PostmasterTouched.Should().BeTrue("shared_buffers — postmaster");
    }

    // AAA: параметр отсутствует в живом конфиге → добавление; счётчик Added.
    [Fact]
    public void Divergence_ParameterMissingInLive_AddedInPatch()
    {
        // Arrange — живой конфиг без блока parameters вовсе.
        var desired = new List<(string Name, string RawValue)> { ("max_connections", "60") };
        const string config = """{"ttl":20,"loop_wait":1,"retry_timeout":3,"synchronous_mode":true}""";

        // Act
        var divergence = DcsConfigConvergence.Analyze(config, desired);

        // Assert: desired добавлен; postmaster-имя затронуто.
        divergence.Patch.Should().Be(
            """{"postgresql":{"parameters":{"max_connections":"60"}}}""");
        divergence.Added.Should().Be(1);
        divergence.Updated.Should().Be(0);
        divergence.Removed.Should().Be(0);
        divergence.PostmasterTouched.Should().BeTrue();
    }

    // AAA: лишний ключ в живом конфиге (нет в desired) → удаление null-значением;
    // удаления — в КОНЦЕ патча; счётчик Removed.
    [Fact]
    public void Divergence_ExtraLiveParameter_RemovedWithNullAtEnd()
    {
        // Arrange — операторский ключ в DCS + один расходящийся desired-ключ.
        var desired = new List<(string Name, string RawValue)>
        {
            ("max_connections", "60"), ("shared_buffers", "2GB"),
        };
        const string config =
            """{"ttl":20,"loop_wait":1,"retry_timeout":3,"synchronous_mode":true,"postgresql":{"parameters":{"max_connections":"60","shared_buffers":"1GB","my_custom":"42"}}}""";

        // Act
        var divergence = DcsConfigConvergence.Analyze(config, desired);

        // Assert: обновление раньше, удаление null — последним ключом.
        divergence.Patch.Should().Be(
            """{"postgresql":{"parameters":{"shared_buffers":"2GB","my_custom":null}}}""");
        divergence.Updated.Should().Be(1);
        divergence.Removed.Should().Be(1);
        divergence.Added.Should().Be(0);
    }

    // AAA: число в живом конфиге нормализуется (GetRawText): 2048 ≡ "2048" —
    // конвергентно, в патч не попадает.
    [Fact]
    public void Divergence_NumberInLive_NormalizedConvergent()
    {
        // Arrange — shared_buffers числом; desired несёт строку "2048".
        var desired = new List<(string Name, string RawValue)> { ("shared_buffers", "2048") };
        const string config =
            """{"ttl":20,"loop_wait":1,"retry_timeout":3,"synchronous_mode":true,"postgresql":{"parameters":{"shared_buffers":2048}}}""";

        // Act
        var patch = DcsConfigConvergence.DivergencePatch(config, desired);

        // Assert: нормализация числа к строке — совпадение, патч пуст.
        patch.Should().BeNull();
    }

    // AAA: bool в живом конфиге нормализуется к "true"/"false" (НЕ семантически:
    // "on" ≠ "true" — посторонний формат конвергируется первым патчем; spec §6).
    [Fact]
    public void Divergence_BoolInLive_NotSemanticOn()
    {
        // Arrange — hot_standby: true (JSON-bool), канон несёт "on".
        var desired = new List<(string Name, string RawValue)> { ("hot_standby", "on") };
        const string config =
            """{"ttl":20,"loop_wait":1,"retry_timeout":3,"synchronous_mode":true,"postgresql":{"parameters":{"hot_standby":true}}}""";

        // Act
        var patch = DcsConfigConvergence.DivergencePatch(config, desired);

        // Assert: "true" ≠ "on" → расхождение, в патч каноническим значением.
        patch.Should().Be("""{"postgresql":{"parameters":{"hot_standby":"on"}}}""");
    }

    // AAA: тайминги и параметры — в ОДНОМ документе; порядок: тайминги, затем
    // параметры в порядке desired, удаления в конце.
    [Fact]
    public void Divergence_TimingsAndParametersSingleDocument()
    {
        // Arrange — расходится loop_wait + два параметра (один удаляется).
        var desired = new List<(string Name, string RawValue)>
        {
            ("max_connections", "60"), ("work_mem", "16MB"),
        };
        const string config =
            """{"ttl":20,"loop_wait":5,"retry_timeout":3,"synchronous_mode":true,"postgresql":{"parameters":{"max_connections":"20","work_mem":"16MB","old_key":"1"}}}""";

        // Act
        var patch = DcsConfigConvergence.DivergencePatch(config, desired);

        // Assert
        patch.Should().Be(
            """{"loop_wait":1,"postgresql":{"parameters":{"max_connections":"60","old_key":null}}}""");
    }

    // AAA: desired == null → патч ТОЛЬКО таймингов (нет/неполна заявка — домен
    // конвергенции параметров не расширяется).
    [Fact]
    public void Divergence_DesiredNull_TimingsOnly()
    {
        // Arrange — живой конфиг с чужими параметрами, desired нет.
        const string config =
            """{"ttl":30,"loop_wait":10,"retry_timeout":10,"postgresql":{"parameters":{"max_connections":"20"}}}""";

        // Act
        var patch = DcsConfigConvergence.DivergencePatch(config, null);

        // Assert: только тайминги; параметры не тронуты.
        patch.Should().NotContain("postgresql");
    }

    // AAA: конвергентный живой конфиг (тайминги + весь desired как строки) → null.
    [Fact]
    public void Divergence_FullyConvergent_NoPatch()
    {
        // Arrange — конфиг собран из desired + канонические тайминги.
        var parameters = string.Join(",", Desired()
            .Select(p => $"{JsonSerializer.Serialize(p.Name)}:{JsonSerializer.Serialize(p.RawValue)}"));
        var config =
            $$"""{"ttl":20,"loop_wait":1,"retry_timeout":3,"synchronous_mode":true,"postgresql":{"parameters":{{{parameters}}}}}""";

        // Act
        var divergence = DcsConfigConvergence.Analyze(config, Desired());

        // Assert: расхождений нет, postmaster не тронут.
        divergence.Patch.Should().BeNull();
        divergence.PostmasterTouched.Should().BeFalse();
    }

    // AAA (self-check SingleCanonicalSource, t11 spec §4.4): JSON, собранный из
    // SPILO_CONFIGURATION текущего билдера, конвергентен с desired (null-патч)
    // — bootstrap и конвергенция из одного источника, молодой кластер не патчится.
    [Fact]
    public void Regression_T09_SpiloEnv_AndConvergence_SingleCanonicalSource()
    {
        // Arrange: топология шарда из одной ноды; tuning эталонного входа.
        var topology = new ShardTopology("shop", "shard1", "shop-shard1",
            new Dictionary<string, NodeAddress> { ["shard1a"] = new("h1", new NodePorts(15432, 18008, 16432)) });
        var tuning = PgTune.Calculate(new PgTuneInput(
            18, PgTuneOsType.Linux, PgTuneDbType.Oltp, 8388608, PgTuneMemoryUnit.KB,
            4, 60, PgTuneHdType.Ssd, PgTuneDbSize.MidRam));

        // Act: env → вырезаем YAML-блок parameters → JSON живого конфига.
        var spilo = SpiloEnvBuilder.Build(
            topology, new EtcdEndpoints(["http://e1:2379"]),
            new InstallSecrets("su", "sb", "adm", "mov"), tuning, null)["SPILO_CONFIGURATION"];
        var lines = spilo.Split('\n')
            .SkipWhile(l => !l.TrimEnd().EndsWith("parameters:", StringComparison.Ordinal))
            .Skip(1)
            .TakeWhile(l => l.StartsWith("        ", StringComparison.Ordinal))
            .Select(l => l.Trim())
            .Where(l => l.Length > 0 && !l.StartsWith('#'))
            .Select(l =>
            {
                var parts = l.Split(':', 2);
                return (Name: parts[0].Trim(), Value: parts[1].Trim().Trim('"')); // YAML-цитирование снимается
            })
            .ToList();
        var parameters = string.Join(",", lines.Select(p =>
            $"{JsonSerializer.Serialize(p.Name)}:{JsonSerializer.Serialize(p.Value)}"));
        var config =
            $$"""{"ttl":{{PatroniTimings.Ttl}},"loop_wait":{{PatroniTimings.LoopWait}},"retry_timeout":{{PatroniTimings.RetryTimeout}},"synchronous_mode":true,"postgresql":{"parameters":{{{parameters}}}}}""";

        // Assert: bootstrap-YAML и desired дают нулевой патч (один источник);
        // состав блока совпадает с desired поимённо.
        var desired = PgParametersCanon.Desired(tuning, null);
        lines.Select(p => p.Name).Should().Equal(desired.Select(p => p.Name));
        DcsConfigConvergence.DivergencePatch(config, desired).Should()
            .BeNull("bootstrap и конвергенция строят параметры из одного набора");
    }
}
```

- [ ] **Step 2: Прогнать — падение (типа нет)**

Run: `dotnet build src/tests/PgWorker.UnitTests -c Debug`
Expected: ошибка компиляции — `DcsConfigConvergence` не найден.

- [ ] **Step 3: Реализовать `DcsConfigConvergence`**

Создать `src/PgWorker.Core/Templates/DcsConfigConvergence.cs`:

```csharp
using System.Text.Json;

namespace PgWorker.Core.Templates;

/// <summary>Итог сверки живого конфига с каноном: патч + счётчики для журнала.</summary>
/// <param name="Patch">Минимальный патч-документ; null — конвергентно, мутаций нет.</param>
/// <param name="Updated">Расходящиеся значения обновлены.</param>
/// <param name="Added">Отсутствующие в живом конфиге добавлены.</param>
/// <param name="Removed">Лишние ключи живого конфига удаляются null-патчем.</param>
/// <param name="PostmasterTouched">Патч затрагивает хотя бы одно postmaster-имя
/// (только для текста журнала — решения на списке НЕ строятся, spec §4.3 п.5).</param>
public sealed record ConvergenceDivergence(
    string? Patch, int Updated, int Added, int Removed, bool PostmasterTouched);

/// <summary>
/// Патч-билдер конвергенции DCS-конфига (t11, arch/14 §5 C; поглощает
/// PatroniTimings.DivergencePatch t09 — тайминги по-прежнему из PatroniTimings):
/// расхождение фактического динамического конфига (GET /config, JSON) с каноном
/// → минимальный патч-документ для PATCH /config. Конвергентно → null —
/// мутаций нет («не второй регулярный писатель», t09). desiredParameters ==
/// null → патч только таймингов (нет/неполна заявка — домен не наш). Битый/
/// чужой JSON → полный патч (безопасный исход t09: непонятный конфиг
/// приводится к канону). Нормализация НЕ семантическая: "2048MB" ≠ "2GB",
/// "on" ≠ "true" — источники нашего формата едины (bootstrap из того же
/// набора), посторонние форматы конвергируются первым патчем и далее стабильны.
/// Порядок ключей патча: тайминги, затем параметры в порядке желаемого набора,
/// удаления — в конце (стабильный детерминированный документ).
/// </summary>
public static class DcsConfigConvergence
{
    /// <summary>
    /// Имена pg_settings.context=postmaster, реально встречающиеся в
    /// PGTune-выводе/каноне — ТОЛЬКО для текста журнала (пометка
    /// pending_restart); решений на этом списке не строятся (spec §4.3 п.5).
    /// </summary>
    public static readonly IReadOnlySet<string> PostmasterParameters = new HashSet<string>(
        StringComparer.Ordinal)
    {
        "max_connections", "shared_buffers", "huge_pages", "wal_buffers",
        "max_worker_processes", "max_parallel_workers", "autovacuum_max_workers",
        "autovacuum_work_mem", "wal_level", "max_wal_senders", "max_replication_slots",
    };

    /// <summary>Минимальный патч-документ для PATCH /config; null — конвергентно.</summary>
    public static string? DivergencePatch(
        string? configJson, IReadOnlyList<(string Name, string RawValue)>? desiredParameters)
        => Analyze(configJson, desiredParameters).Patch;

    /// <summary>Сверка с итогом для журнала (счётчики updated/added/removed).</summary>
    public static ConvergenceDivergence Analyze(
        string? configJson, IReadOnlyList<(string Name, string RawValue)>? desiredParameters)
    {
        JsonElement root;
        try
        {
            root = JsonDocument.Parse(configJson ?? "null").RootElement;
        }
        catch (JsonException)
        {
            root = default; // битый/чужой документ — конвергируем все поля
        }

        var timingPatch = new List<string>();
        AddIfDivergent(timingPatch, root, "ttl", PatroniTimings.Ttl);
        AddIfDivergent(timingPatch, root, "loop_wait", PatroniTimings.LoopWait);
        AddIfDivergent(timingPatch, root, "retry_timeout", PatroniTimings.RetryTimeout);
        AddIfDivergent(timingPatch, root, "synchronous_mode", PatroniTimings.SynchronousMode);

        var updated = 0;
        var added = 0;
        var removed = 0;
        var postmaster = false;
        var parameterPatch = new List<string>();

        if (desiredParameters is not null)
        {
            // Живой блок postgresql.parameters: отсутствует/не объект (в т.ч.
            // битый документ) — все desired считаются добавляемыми.
            var hasLive = root.ValueKind == JsonValueKind.Object
                          && root.TryGetProperty("postgresql", out var postgresql)
                          && postgresql.ValueKind == JsonValueKind.Object
                          && postgresql.TryGetProperty("parameters", out var parameters)
                          && parameters.ValueKind == JsonValueKind.Object;

            var desiredNames = new HashSet<string>(StringComparer.Ordinal);
            foreach (var (name, rawValue) in desiredParameters)
            {
                desiredNames.Add(name);

                // Нормализация живого значения к строке: строка — как есть;
                // число — инвариантный GetRawText() (60 → "60"); bool —
                // "true"/"false"; null/объект/массив — расхождение (в патч).
                string? live = null;
                var keyExists = false;
                if (hasLive && parameters.TryGetProperty(name, out var value))
                {
                    keyExists = true;
                    live = value.ValueKind switch
                    {
                        JsonValueKind.String => value.GetString(),
                        JsonValueKind.Number => value.GetRawText(),
                        JsonValueKind.True => "true",
                        JsonValueKind.False => "false",
                        _ => null,
                    };
                }

                if (live is not null && string.Equals(live, rawValue, StringComparison.Ordinal))
                    continue; // конвергентно

                if (keyExists)
                    updated++; // ключ есть, значение разошлось/не нормализуемо
                else
                    added++; // ключа в живом конфиге нет

                parameterPatch.Add($"{JsonSerializer.Serialize(name)}:{JsonSerializer.Serialize(rawValue)}");
                if (PostmasterParameters.Contains(name))
                    postmaster = true;
            }

            // Лишние ключи живого конфига (нет в desired) → удаление null-значением
            // (Patroni-семантика PATCH /config: null удаляет ключ). Удаления — в конце.
            if (hasLive)
                foreach (var property in parameters.EnumerateObject())
                    if (!desiredNames.Contains(property.Name))
                    {
                        removed++;
                        parameterPatch.Add($"{JsonSerializer.Serialize(property.Name)}:null");
                    }
        }

        // Сборка ОДНОГО документа: тайминги, затем вложенный postgresql.parameters
        // (если есть параметрическая часть); пустой — null (конвергентно).
        var parts = new List<string>(timingPatch);
        if (parameterPatch.Count > 0)
            parts.Add($"\"postgresql\":{{\"parameters\":{{{string.Join(",", parameterPatch)}}}}}");
        var patch = parts.Count == 0 ? null : $"{{{string.Join(",", parts)}}}";

        return new ConvergenceDivergence(patch, updated, added, removed, postmaster);
    }

    // Поле отсутствует (в т.ч. в битом/чужом документе) или расходится — в патч.
    private static void AddIfDivergent(List<string> patch, JsonElement root, string name, int expected)
    {
        var diverges = root.ValueKind != JsonValueKind.Object
                       || !root.TryGetProperty(name, out var actual)
                       || !actual.TryGetInt32(out var value)
                       || value != expected;
        if (diverges)
            patch.Add($"\"{name}\":{expected}");
    }

    private static void AddIfDivergent(List<string> patch, JsonElement root, string name, bool expected)
    {
        var diverges = root.ValueKind != JsonValueKind.Object
                       || !root.TryGetProperty(name, out var actual)
                       || (actual.ValueKind is not (JsonValueKind.True or JsonValueKind.False))
                       || actual.GetBoolean() != expected;
        if (diverges)
            patch.Add($"\"{name}\":{(expected ? "true" : "false")}");
    }
}
```

- [ ] **Step 4: Прогнать DcsConfigConvergenceTests — зелёные**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~DcsConfigConvergenceTests"`
Expected: PASS 16/16 (13 методов: 12 Fact + Theory×4). Если кейс с точной строкой падает — сверять реализацию с ожидаемой строкой теста (порядок: тайминги → параметры desired-порядком → удаления последними), НЕ править тест.

- [ ] **Step 5: Удалить `PatroniTimings.DivergencePatch`, мигрировать `PatroniTimingsTests`, склеить NodeSupervisor**

1. В `src/PgWorker.Core/Templates/PatroniTimings.cs`: удалить метод `DivergencePatch` и оба приватных `AddIfDivergent`; константы `Ttl/LoopWait/RetryTimeout/SynchronousMode` и doc-комментарий класса остаются (единый канон таймингов); doc-комментарий дополнить строкой «построение патча конвергенции — DcsConfigConvergence (t11)».
2. В `src/PgWorker.Provisioning/Processes/NodeSupervisor.cs:308` временная склейка (сохраняет поведение t09 до Task 3): заменить
   `var patch = PatroniTimings.DivergencePatch(config.Value);`
   на
   `var patch = DcsConfigConvergence.DivergencePatch(config.Value, null);`
3. В `src/tests/PgWorker.UnitTests/Templates/PatroniTimingsTests.cs`: удалить 4 мигрированных кейса (`Regression_T09_Divergence_*`); кейс `Regression_T09_SpiloEnv_AndConvergence_SingleCanonicalSource` заменить на компактный (полы таймингов + self-check на новом API; расширенный self-check с параметрами — в `DcsConfigConvergenceTests`):

```csharp
    // AAA (t09, дополнен t11): канон таймингов удовлетворяет полам Patroni 4.x;
    // канонический документ конвергентен сам себе на новом API (расширенный
    // self-check с параметрами — DcsConfigConvergenceTests).
    [Fact]
    public void Regression_T09_SpiloEnv_AndConvergence_SingleCanonicalSource()
    {
        // Arrange: топология шарда из одной ноды.
        var topology = new ShardTopology("shop", "shard1", "shop-shard1",
            new Dictionary<string, NodeAddress> { ["shard1a"] = new("h1", new NodePorts(15432, 18008, 16432)) });

        // Act: генерируем env и сверяем канон через DcsConfigConvergence.
        var spilo = SpiloEnvBuilder.Build(
            topology, new EtcdEndpoints(["http://e1:2379"]),
            new InstallSecrets("su", "sb", "adm", "mov"))["SPILO_CONFIGURATION"];
        var selfPatch = DcsConfigConvergence.DivergencePatch(
            $$"""{"ttl":{{PatroniTimings.Ttl}},"loop_wait":{{PatroniTimings.LoopWait}},"retry_timeout":{{PatroniTimings.RetryTimeout}},"synchronous_mode":true}""",
            null);

        // Assert: env несёт канон; канон конвергентен сам себе (null-патч);
        // полы и правило Patroni 4.x соблюдены.
        spilo.Should().Contain($"ttl: {PatroniTimings.Ttl}")
            .And.Contain($"loop_wait: {PatroniTimings.LoopWait}")
            .And.Contain($"retry_timeout: {PatroniTimings.RetryTimeout}");
        selfPatch.Should().BeNull("канон конвергентен сам с собой");
        PatroniTimings.Ttl.Should().BeGreaterThanOrEqualTo(20, "пол Patroni 4.x: ttl≥20");
        PatroniTimings.LoopWait.Should().BeGreaterThanOrEqualTo(1);
        PatroniTimings.RetryTimeout.Should().BeGreaterThanOrEqualTo(3);
        (PatroniTimings.LoopWait + 2 * PatroniTimings.RetryTimeout)
            .Should().BeLessThanOrEqualTo(PatroniTimings.Ttl, "правило loop_wait+2*retry_timeout≤ttl");
    }
```

Заголовочный комментарий файла обновить: патч-логика — в `DcsConfigConvergenceTests`, здесь — канон таймингов.

- [ ] **Step 6: Прогнать Templates + весь юнит-проект**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~Templates"`
Expected: PASS (PatroniTimingsTests 1 кейс + DcsConfigConvergenceTests 16 (13 методов, Theory×4) + NodeConfigBuildersTests все).

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug`
Expected: PASS (включая NodeSupervisorTests — склейка из Step 5.2 сохраняет поведение t09: желаемый набор ещё не передаётся).

- [ ] **Step 7: Коммит**

```bash
git add src/PgWorker.Core/Templates/DcsConfigConvergence.cs \
  src/PgWorker.Core/Templates/PatroniTimings.cs \
  src/PgWorker.Core/Templates/NodeConfigBuilders.cs \
  src/PgWorker.Provisioning/Processes/NodeSupervisor.cs \
  src/tests/PgWorker.UnitTests/Templates/DcsConfigConvergenceTests.cs \
  src/tests/PgWorker.UnitTests/Templates/PatroniTimingsTests.cs
git commit -m "feat(t11): DcsConfigConvergence — патч-билдер конвергенции (тайминги PatroniTimings + postgresql.parameters от PgParametersCanon.Desired: обновление/добавление/удаление null-патчем, нормализация числа/bool, Analyze со счётчиками для журнала); поглощает PatroniTimings.DivergencePatch, кейсы Regression_T09_* мигрированы + self-check SingleCanonicalSource SPILO↔desired"
```

---

### Task 3: Проводка — расширение `NodeSupervisor.ConvergeDcsConfigAsync`

Фаза spec §5: 4. Конструктор получает `PgtuneSettings` + `ILogger<NodeSupervisor>`; шаг конвергенции: расчёт desired от заявок, skip pg-части при отсутствии/неполноте заявки (warning, решение пользователя 2026-09-14), один PATCH, журнал со счётчиками.

**Files:**
- Modify: `src/PgWorker.Provisioning/Processes/NodeSupervisor.cs` (конструктор + `ConvergeDcsConfigAsync`)
- Modify: `src/PgWorker.App/Program.cs` (DI: регистрация `PgtuneSettings`, вызов `NodeSupervisor`)
- Modify: `src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs` (хелпер `PgtuneSettings`, записывающий логгер)
- Test: `src/tests/PgWorker.UnitTests/Provisioning/NodeSupervisorTests.cs` (расширение + обновление кейсов t09)

**Interfaces:**
- Consumes: `PgParametersCanon.Desired` (Task 1), `DcsConfigConvergence.Analyze` + `ConvergenceDivergence` (Task 2), `PgtuneSettings.ExcludeParams`, `ReadShardResourcesAsync` (существующий), `probe.GetConfigAsync`/`PatchConfigAsync` (существующие; `ShardProbe` НЕ расширяется).
- Produces: поведение тика надзора (потребляет Task 4). Новая сигнатура конструктора:

```csharp
public sealed class NodeSupervisor(
    IEtcdGateway etcd, string[] endpoints, IClusterDriver driver, ShardProbe probe,
    ISqlExecutor sql, ClaimStore claims, WorkJournal journal, ThresholdsOptions thresholds,
    TimeProvider clock, InstallSecrets secrets, IAppParamsEnsurer appParams,
    PgtuneInputsFactory pgtune, PgtuneSettings pgtuneSettings, ILogger<NodeSupervisor> log,
    MasterKeyReconciler? masterKeys = null, EtcdEndpoints? etcdForNodes = null)
```

- [ ] **Step 1: Хелперы в Fakes.cs**

В `src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs` добавить (рядом с `PgtuneFactory`):

```csharp
    // Настройки PgWorker:Pgtune для конструкторов процессов (дефолты опций);
    // отдельная сущность от PgtuneFactory — NodeSupervisor получает её
    // конструкторно для ExcludeParams (t11 spec §4.3 п.3).
    internal static PgtuneSettings PgtuneSettings() => new(
        18, "oltp", "ssd", "mid_ram", 60,
        new HashSet<string>(StringComparer.Ordinal));
```

И записывающий логгер (в конец класса `Fakes`):

```csharp
    // Записывающий ILogger: фиксирует отформатированные сообщения — проверка
    // warning-логов процессов (t11: skip pgtune-конвергенции по заявке).
    internal sealed class RecordingLogger<T> : ILogger<T>
    {
        public readonly List<(LogLevel Level, string Message)> Entries = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : not null => null;
        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state,
            Exception? exception, Func<TState, Exception?, string> formatter)
            => Entries.Add((logLevel, formatter(state, exception)));
    }
```

(`Fakes.cs` уже имеет `using Microsoft.Extensions.Logging.Abstractions;` — для `NullLogger`.)

- [ ] **Step 2: Failing-тесты проводки в NodeSupervisorTests (новые кейсы + обновление кейсов t09)**

В `src/tests/PgWorker.UnitTests/Provisioning/NodeSupervisorTests.cs`:

1. Обновить `NewRig` (строка ~74): добавить параметры `RecordingLogger<NodeSupervisor>? log = null` в сигнатуру и два аргумента в конструктор супервизора:

```csharp
    private static async Task<Rig> NewRig(
        Func<int, HttpResponseMessage> respond,
        IReadOnlyList<string>? nodeObjects = null,
        long? staleUnreachableForShard1A = null,
        long? staleUnreachableAll = null,
        Func<HttpRequestMessage, HttpResponseMessage>? respondRaw = null,
        IReadOnlyDictionary<string, NodeAddress>? addresses = null,
        Fakes.FakeSql? sql = null,
        Fakes.RecordingLogger<NodeSupervisor>? log = null)
    {
        // ... (тело до создания supervisor — без изменений) ...
        ILogger<NodeSupervisor> supervisorLog = log is not null ? log : NullLogger<NodeSupervisor>.Instance;
        var supervisor = new NodeSupervisor(
            etcd, [Ep], driver, probe, sql ?? new Fakes.FakeSql(), claims, journal,
            Thresholds, TimeProvider.System, Secrets,
            new AppParamsEnsurer(etcd, [Ep], "sslmode=require"),
            Fakes.PgtuneFactory(), Fakes.PgtuneSettings(), supervisorLog,
            new MasterKeyReconciler(etcd, [Ep], probe));
        return new Rig(etcd, driver, claims, journal, supervisor);
    }
```

(в файл добавить `using Microsoft.Extensions.Logging;` и `using Microsoft.Extensions.Logging.Abstractions;`, если их ещё нет).

2. В `Tick_TwoClustersParallel_OneSupervisorSingleton_DeadShardsDoNotCross` (строка ~1023) — прямой `new NodeSupervisor(...)`: добавить после `Fakes.PgtuneFactory()` два аргумента `Fakes.PgtuneSettings(), NullLogger<NodeSupervisor>.Instance,`.

3. Обновить кейсы t09 (живой конфиг без канонических параметров теперь патчится и параметрами — сид несёт полную заявку cpu=2/mem=8Gi):

```csharp
    // AAA (t09→t11): DCS-конфиг на дефолтах Patroni — патч несёт ВСЕ канонические
    // тайминги И полный желаемый набор параметров (живой конфиг без блока
    // parameters → все desired добавляются; заявка в сиде полная).
    [Fact]
    public async Task Regression_T09_DcsConfigConvergence_DefaultConfig_PatchedToCanonical()
    {
        // Arrange — GET /config отдаёт Patroni-дефолты (ttl=30/loop_wait=10/
        // retry_timeout=10, без synchronous_mode), PATCH ловим в список
        var patches = new List<string>();
        var rig = await NewRig(_ => Ok(), respondRaw: r =>
        {
            if (r.Method.Method == "PATCH" && r.RequestUri!.AbsolutePath == "/config")
            {
                patches.Add(new StreamReader(r.Content!.ReadAsStream()).ReadToEnd());
                return new HttpResponseMessage(HttpStatusCode.OK);
            }

            if (r.Method.Method == "GET" && r.RequestUri!.AbsolutePath == "/config")
                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent(
                        """{"ttl":30,"loop_wait":10,"retry_timeout":10}""", Encoding.UTF8, "application/json"),
                };

            return Ok();
        });

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), null, CancellationToken.None);

        // Assert — один PATCH: тайминги каноном + параметры от заявки 8Gi
        // (shared_buffers 2GB, wal_level канонический) единым документом.
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        patches.Should().ContainSingle()
            .Which.Should().Contain("\"ttl\":20")
            .And.Contain("\"loop_wait\":1").And.Contain("\"retry_timeout\":3")
            .And.Contain("\"synchronous_mode\":true")
            .And.Contain("\"postgresql\":{\"parameters\":{")
            .And.Contain("\"shared_buffers\":\"2GB\"")
            .And.Contain("\"wal_level\":\"logical\"");
    }

    // AAA (t09→t11): конвергентный конфиг (тайминги + полный желаемый набор
    // от заявки сида) — ноль мутаций (не второй регулярный писатель).
    [Fact]
    public async Task Regression_T09_DcsConfigConvergence_CanonicalConfig_NoPatch()
    {
        // Arrange — GET /config уже канонический: тайминги + параметры desired
        // от сида (cpu=2/mem=8Gi, опции Fakes.PgtuneSettings), собираем программно.
        var desired = PgWorker.Core.Tuning.PgParametersCanon.Desired(PgTune.Calculate(
            new PgTuneInput(18, PgTuneOsType.Linux, PgTuneDbType.Oltp, 8388608,
                PgTuneMemoryUnit.KB, 2, 60, PgTuneHdType.Ssd, PgTuneDbSize.MidRam)), null);
        var parameters = string.Join(",", desired.Select(p =>
            $"{JsonSerializer.Serialize(p.Name)}:{JsonSerializer.Serialize(p.RawValue)}"));
        var canonicalConfig =
            $$"""{"ttl":20,"loop_wait":1,"retry_timeout":3,"synchronous_mode":true,"postgresql":{"parameters":{{{parameters}}}}}""";
        var patches = new List<string>();
        var rig = await NewRig(_ => Ok(), respondRaw: r =>
        {
            if (r.Method.Method == "PATCH" && r.RequestUri!.AbsolutePath == "/config")
            {
                patches.Add(new StreamReader(r.Content!.ReadAsStream()).ReadToEnd());
                return new HttpResponseMessage(HttpStatusCode.OK);
            }

            if (r.Method.Method == "GET" && r.RequestUri!.AbsolutePath == "/config")
                return new HttpResponseMessage(HttpStatusCode.OK)
                    { Content = new StringContent(canonicalConfig, Encoding.UTF8, "application/json") };

            return Ok();
        });

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), null, CancellationToken.None);

        // Assert — PATCH не звался вовсе
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        patches.Should().BeEmpty("конфиг уже канонический — мутаций нет");
    }
```

(в файл добавить `using System.Text.Json;` и `using PgWorker.Core.Tuning;`, если их ещё нет.)

4. Новые кейсы (после обновлённых t09):

```csharp
    // AAA (t11, spec §4.4): живой /config расходится по ПАРАМЕТРАМ (заявка 8Gi,
    // живой конфиг от прежней 4Gi) → ровно ОДИН PATCH за тик, документ несёт
    // пересчитанные значения (shared_buffers 2GB/effective_cache_size 6GB от 8Gi)
    // и добавляет отсутствующие desired-ключи; фаза dcs-converge в журнале.
    [Fact]
    public async Task Tick_DcsConfigConvergence_ParametersDiverged_SinglePatchWithRecalculated()
    {
        // Arrange — тайминги канонические; max_connections совпадает (60),
        // shared_buffers/effective_cache_size от прежней заявки 4Gi (1GB/3GB).
        var patches = new List<string>();
        var rig = await NewRig(_ => Ok(), respondRaw: r =>
        {
            if (r.Method.Method == "PATCH" && r.RequestUri!.AbsolutePath == "/config")
            {
                patches.Add(new StreamReader(r.Content!.ReadAsStream()).ReadToEnd());
                return new HttpResponseMessage(HttpStatusCode.OK);
            }

            if (r.Method.Method == "GET" && r.RequestUri!.AbsolutePath == "/config")
                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent(
                        """{"ttl":20,"loop_wait":1,"retry_timeout":3,"synchronous_mode":true,"postgresql":{"parameters":{"max_connections":"60","shared_buffers":"1GB","effective_cache_size":"3GB"}}}""",
                        Encoding.UTF8, "application/json"),
                };

            return Ok();
        });

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), null, CancellationToken.None);

        // Assert: один PATCH; расходящиеся обновлены к 8Gi-расчёту (2GB/6GB);
        // журнал несёт фазу dcs-converge со счётчиками.
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        patches.Should().ContainSingle()
            .Which.Should().Contain("\"postgresql\":{\"parameters\":{")
            .And.Contain("\"shared_buffers\":\"2GB\"")
            .And.Contain("\"effective_cache_size\":\"6GB\"");
        rig.Etcd.Store["/pgworker/work/shop"].Value
            .Should().Contain("dcs-converge").And.Contain("updated:").And.Contain("added:");
    }

    // AAA (t11, решение 2026-09-14 «неполная заявка — SKIP»): заявка НЕПОЛНАЯ —
    // request_mem удалён при живом request_cpu (ReadShardResourcesAsync вернёт
    // NodeResources(CpuCores:2, MemoryBytes:null), НЕ null) → патч ТОЛЬКО
    // таймингов + warning-лог; тик надзора НЕ фейлится.
    [Fact]
    public async Task Tick_DcsConfigConvergence_PartialResourceRequest_TimingsOnlyWithWarning()
    {
        // Arrange — request_mem снести (request_cpu жив); /config на дефолтных
        // таймингах, PATCH ловим.
        var log = new Fakes.RecordingLogger<NodeSupervisor>();
        var patches = new List<string>();
        var rig = await NewRig(_ => Ok(), log: log, respondRaw: r =>
        {
            if (r.Method.Method == "PATCH" && r.RequestUri!.AbsolutePath == "/config")
            {
                patches.Add(new StreamReader(r.Content!.ReadAsStream()).ReadToEnd());
                return new HttpResponseMessage(HttpStatusCode.OK);
            }

            if (r.Method.Method == "GET" && r.RequestUri!.AbsolutePath == "/config")
                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent(
                        """{"ttl":30,"loop_wait":10,"retry_timeout":10}""", Encoding.UTF8, "application/json"),
                };

            return Ok();
        });
        rig.Etcd.Store.Remove("/service/shop-shard1/request_mem");

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), null, CancellationToken.None);

        // Assert: патч только таймингов, pg-параметров нет; warning записан; тик Done.
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        patches.Should().ContainSingle().Which.Should().NotContain("postgresql");
        log.Entries.Should().Contain(e => e.Level == LogLevel.Warning
            && e.Message.Contains("request_", StringComparison.Ordinal));
    }

    // AAA (t11, симметричный кейс полного отсутствия): ОБЕ заявки отсутствуют
    // (ReadShardResourcesAsync == null) — тот же исход: тайминги + warning, тик Done.
    [Fact]
    public async Task Tick_DcsConfigConvergence_NoResourceRequests_TimingsOnlyWithWarning()
    {
        // Arrange — обе заявки снести; /config на дефолтных таймингах.
        var log = new Fakes.RecordingLogger<NodeSupervisor>();
        var patches = new List<string>();
        var rig = await NewRig(_ => Ok(), log: log, respondRaw: r =>
        {
            if (r.Method.Method == "PATCH" && r.RequestUri!.AbsolutePath == "/config")
            {
                patches.Add(new StreamReader(r.Content!.ReadAsStream()).ReadToEnd());
                return new HttpResponseMessage(HttpStatusCode.OK);
            }

            if (r.Method.Method == "GET" && r.RequestUri!.AbsolutePath == "/config")
                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent(
                        """{"ttl":30,"loop_wait":10,"retry_timeout":10}""", Encoding.UTF8, "application/json"),
                };

            return Ok();
        });
        rig.Etcd.Store.Remove("/service/shop-shard1/request_mem");
        rig.Etcd.Store.Remove("/service/shop-shard1/request_cpu");

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), null, CancellationToken.None);

        // Assert: патч только таймингов; warning записан; тик Done.
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        patches.Should().ContainSingle().Which.Should().NotContain("postgresql");
        log.Entries.Should().Contain(e => e.Level == LogLevel.Warning
            && e.Message.Contains("request_", StringComparison.Ordinal));
    }

    // AAA (t11): /config недоступен (транспорт/5xx) → мутаций нет, тик не
    // фейлится (транзиент — конвергенция повторится следующим тиком).
    [Fact]
    public async Task Tick_DcsConfigConvergence_ConfigUnavailable_NoMutation()
    {
        // Arrange — GET /config → 503 у probeNode (первый канонический узел,
        // порт 18000), PATCH ловим (не должен зваться).
        var patches = new List<string>();
        var rig = await NewRig(port => port == 18000 ? Down() : Ok(), respondRaw: r =>
        {
            if (r.Method.Method == "PATCH" && r.RequestUri!.AbsolutePath == "/config")
            {
                patches.Add(new StreamReader(r.Content!.ReadAsStream()).ReadToEnd());
                return new HttpResponseMessage(HttpStatusCode.OK);
            }

            if (r.Method.Method == "GET" && r.RequestUri!.AbsolutePath == "/config")
                return Down();

            return Ok();
        });

        // Act
        var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), null, CancellationToken.None);

        // Assert
        outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
        patches.Should().BeEmpty("конфиг недоступен — транзиент, патча нет");
    }
```

- [ ] **Step 3: Прогнать — падение (конструктор не компилируется)**

Run: `dotnet build src/tests/PgWorker.UnitTests -c Debug`
Expected: ошибка компиляции — у `NodeSupervisor` нет параметров `pgtuneSettings`/`log`.

- [ ] **Step 4: Реализовать проводку в NodeSupervisor + DI**

1. `src/PgWorker.Provisioning/Processes/NodeSupervisor.cs`:
   - сигнатура конструктора — по Interfaces выше (два новых параметра после `pgtune`, до опциональных; в файл добавить `using Microsoft.Extensions.Logging;` и `using PgWorker.Core.Tuning;`, если второго ещё нет — он уже есть);
   - заменить `ConvergeDcsConfigAsync` (строки ~285–319) на:

```csharp
    // Конвергенция динамического DCS-конфига (arch/14 §5 C; t09 — тайминги,
    // t11 — pg-параметры): GET /config первого канонического Patroni-узла
    // шарда → сверка с каноном (PatroniTimings + желаемый набор параметров
    // merge(PGTune ∪ канон) от АКТУАЛЬНЫХ заявок, пересчёт на каждый тик, БЕЗ
    // фиксации в etcd) → ОДИН PATCH /config на тик: обновляет расходящиеся,
    // добавляет отсутствующие, удаляет лишние null-патчем. Postmaster-параметры
    // Patroni помечает pending_restart — применяются при ближайшем рестарте
    // ноды; воркер НИКОГДА не инициирует рестарт PG (решение 2026-09-14).
    // Заявка request_{cpu,mem} отсутствует ИЛИ неполна (любое из полей
    // ресурсов null — NodeResourcesParser.Parse("2", null) возвращает частичный
    // объект, НЕ null) → pg-часть пропускается с warning (в конвергенции
    // мутация опциональна: пропуск = конфиг прежний, безопасно; выдумывать
    // размер ноды по остаточному ресурсу нельзя — решение 2026-09-14), тайминги
    // конвергируются как раньше. Прочие исключения pgtune.Create (неполнота уже
    // отсечена) — фейл фазы тика, транзиент-ретрай (как в EnsureNode-путях).
    // Транзиент-толерантно (t09): недоступность GET/PATCH — skip этого тика,
    // патч повторится следующим. Фазовая запись журнала при патче несёт трек
    // недоступности текущего тика.
    private async Task<Result> ConvergeDcsConfigAsync(
        string cluster, ShardSpec shard, IReadOnlyDictionary<string, NodeAddress> addresses,
        Dictionary<string, long> track, CancellationToken ct)
    {
        var probeNode = addresses
            .Where(p => p.Key.StartsWith($"{shard.Name}/", StringComparison.Ordinal))
            .OrderBy(p => p.Key, StringComparer.Ordinal)
            .Select(p => p.Value)
            .FirstOrDefault(a => a.Ports.Patroni != 0 && a.Object is null);
        if (probeNode is null)
            return Result.Success(); // канонического Patroni-узла нет — не наш домен

        var config = await probe.GetConfigAsync(probeNode, ct);
        if (!config.IsSuccess)
            return Result.Success(); // транзиент — сверка следующим тиком

        // Желаемый набор: от ОБЯЗАТЕЛЬНЫХ заявок (arch/14 §2.1 п.4). Отсутствие
        // ИЛИ неполнота (любое поле null) — skip pg-части с warning, НЕ фейл
        // тика (в отличие от EnsureNode-путей, где размер ноды обязателен).
        var resources = await ReadShardResourcesAsync(cluster, shard.Name, ct);
        IReadOnlyList<(string Name, string RawValue)>? desired = null;
        if (resources is null || resources.MemoryBytes is null || resources.CpuCores is null)
        {
            log.LogWarning(
                "supervise {Cluster}/{Shard}: pgtune-конвергенция пропущена — заявка request_{{cpu,mem}} отсутствует/неполна " +
                "(аномалия данных; тайминги DCS-конфига конвергируются без параметров)",
                cluster, shard.Name);
        }
        else
        {
            // Неполнота отсечена выше — прочие исключения расчёта = фейл фазы
            // тика, транзиент-ретрай (как в EnsureNode-путях).
            var tuning = pgtune.Create(resources);
            desired = PgParametersCanon.Desired(tuning, pgtuneSettings.ExcludeParams);
        }

        var divergence = DcsConfigConvergence.Analyze(config.Value, desired);
        if (divergence.Patch is null)
            return Result.Success(); // конвергентно — мутаций нет

        var applied = await probe.PatchConfigAsync(probeNode, divergence.Patch, ct);
        if (!applied.IsSuccess)
            return Result.Success(); // транзиент — патч следующим тиком

        // Postmaster-параметры Patroni пометит pending_restart — штатно до
        // ближайшего рестарта ноды (воркер рестарт не инициирует).
        var note = $"{shard.Name}: DCS-конфиг сконвергирован к канону " +
                   $"(updated:{divergence.Updated} added:{divergence.Added} removed:{divergence.Removed}" +
                   (divergence.PostmasterTouched
                       ? "; postmaster-параметры → pending_restart (применение при ближайшем рестарте ноды)"
                       : string.Empty) +
                   $") ({divergence.Patch})";
        await journal.WritePhaseAsync(cluster, "supervise", "dcs-converge", claims.InstanceId,
            note, ct, unreachable: track);
        return Result.Success();
    }
```

   - doc-комментарий шага 4 в `TickAsync` (строки ~172–178) дополнить: «+ pg-параметры merge(PGTune ∪ канон) от актуальных заявок (t11)».

2. `src/PgWorker.App/Program.cs`:
   - после регистрации `PgtuneInputsFactory` (строка ~202) добавить:

```csharp
// PgtuneSettings (runtime-склейка PgWorker:Pgtune) — единый ExcludeParams для
// bootstrap и конвергенции DCS (t11, spec §4.3 п.3).
builder.Services.AddSingleton(sp =>
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Pgtune.ToRuntime());
```

   - фабрику `PgtuneInputsFactory` (строка ~202) переключить на `sp.GetRequiredService<PgtuneSettings>()` (вместо inline `ToRuntime()`);
   - в создании `NodeSupervisor` (строка ~267) после `sp.GetRequiredService<PgtuneInputsFactory>()` добавить `sp.GetRequiredService<PgtuneSettings>(), sp.GetRequiredService<ILogger<NodeSupervisor>>(),`.

- [ ] **Step 5: Прогнать юниты надзора + весь проект**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~NodeSupervisorTests"`
Expected: PASS — включая обновлённые кейсы t09 и 4 новых.

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug`
Expected: PASS (весь проект).

- [ ] **Step 6: Сборка решения + коммит**

Run: `dotnet build src/PgWorker.slnx -c Debug`
Expected: BUILD SUCCEEDED.

```bash
git add src/PgWorker.Provisioning/Processes/NodeSupervisor.cs src/PgWorker.App/Program.cs \
  src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs \
  src/tests/PgWorker.UnitTests/Provisioning/NodeSupervisorTests.cs
git commit -m "feat(t11): проводка конвергенции pg-параметров в NodeSupervisor — desired от актуальных заявок (пересчёт на тик), skip pg-части с warning при отсутствии/неполноте заявки (решение 2026-09-14), один PATCH /config (тайминги+параметры), журнал dcs-converge со счётчиками updated/added/removed и пометкой postmaster→pending_restart; воркер не инициирует рестарт PG"
```

---

### Task 4: Интеграционный docker-сценарий конвергенции (E2ePgtuneScenarios)

Фаза spec §5: 5. Живой контур: смена заявки `request_mem` вверх/вниз → патч DCS, динамика применена живым PG, postmaster → `pending_restart`, идемпотентность.

Размещение: spec §4.4 именует каталог `Docker/` собирательно для docker-интеграционных тестов; фактическое размещение — `E2e/E2ePgtuneScenarios.cs` (расширение существующего сценария той же задачи pgtune): полный живой контур воркера (`E2eEnvironment`: своя сеть/etcd на Fact, guid-тег `ClusterTag`, динамические порты, own-only teardown + ассерт чистоты) даёт все требования изоляции spec §4.4; в `Docker/` живут только драйверные тесты без контура воркера.

**Files:**
- Test: `src/tests/PgWorker.IntegrationTests/E2e/E2ePgtuneScenarios.cs` (новый Fact + хелперы)

**Interfaces:**
- Consumes: `E2eEnvironment.StartAsync/StartHostAsync/RunDockerAsync` (существующие), `E2eFixture.WaitForAsync`, `EtcdGateway`, паттерны `E2eScaleScenarios` (portalloc JSON `{"host","pg","patroni","doorman"}`, master-резолв по `/primary`), Npgsql.

- [ ] **Step 1: Написать сценарий + хелперы**

Добавить в `src/tests/PgWorker.IntegrationTests/E2e/E2ePgtuneScenarios.cs` (в шапку файла добавить `using Npgsql;`; using `System.Text.Json` уже есть):

```csharp
    // E2E-сценарий конвергенции pg-параметров (t11 spec §4.4, AC §7 п.1–3):
    // живой кластер на заявке 8Gi → перезапись request_mem 16Gi → тик надзора
    // патчит DCS: (а) GET /config несёт пересчитанные параметры (динамика +
    // postmaster); (б) динамический параметр фактически применён живым PG
    // (reload в пределах loop_wait); (в) нода с изменённым postmaster —
    // pending_restart=true, рестарта/пересоздания нет; (г) идемпотентность —
    // mod_revision /service/<scope>/config стабилен после конвергенции
    // (не второй регулярный писатель); (д) возврат заявки 8Gi — патч вниз.
    [Fact]
    public async Task Pgtune_Convergence_RequestMemChanged_DcsPatchedLivePgReloaded()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        await using var fx = await E2eEnvironment.StartAsync("pgtune-converge", ct: ct);
        Fx = fx;
        var cluster = $"cshop{Fx.ClusterTag}";
        var scope = $"{cluster}-shard1";

        // Arrange: живой кластер на заявке 8Gi (расчёт: shared_buffers 2GB,
        // effective_cache_size 6GB = 8Gi×3/4).
        await SeedClusterAsync(cluster, requestMem: "8Gi", ct);
        await using var host = await Fx.StartHostAsync("s1", ct: ct);
        var provisioned = await E2eFixture.WaitForAsync(
            () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(360), ct);
        provisioned.Should().BeTrue("provisioning кластера должен дойти до Active; " +
                                    $"work={await WorkDumpAsync(cluster, ct)}");

        var master = await MasterInfoAsync(cluster, "shard1", ct);
        (await SqlScalarAsync(master.Dsn,
                "SELECT current_setting('effective_cache_size')", ct))
            .Should().Be("6GB", "bootstrap-расчёт от заявки 8Gi применён при инициализации");

        // Act: перезаписать заявку 8Gi → 16Gi (расчёт: effective_cache_size 12GB —
        // динамика; shared_buffers 4GB — postmaster) — ЖИВОЙ шард, ноды не трогаем.
        (await G.PutAsync(Endpoint, $"/service/{scope}/request_mem", "16Gi", null, ct))
            .IsSuccess.Should().BeTrue("заявка request_mem должна перезаписаться");

        // Assert (а): GET /config несёт пересчитанные параметры (динамика +
        // postmaster) — тик надзора патчит DCS одним документом.
        var configUpdated = await E2eFixture.WaitForAsync(async () =>
        {
            var config = await GetPatroniConfigValueAsync(master.PatroniPort, ct);
            return config.Contains("\"effective_cache_size\":\"12GB\"", StringComparison.Ordinal)
                   && config.Contains("\"shared_buffers\":\"4GB\"", StringComparison.Ordinal);
        }, TimeSpan.FromSeconds(120), ct);
        configUpdated.Should().BeTrue("тик надзора обязан патчить DCS-конфиг от новой заявки; " +
                                      $"work={await WorkDumpAsync(cluster, ct)}");

        // Assert (б): динамический параметр фактически применён живым PG без
        // рестарта (Patroni reload в пределах loop_wait; поллинг).
        var reloaded = await E2eFixture.WaitForAsync(async () =>
            await SqlScalarAsync(master.Dsn,
                "SELECT current_setting('effective_cache_size')", ct) == "12GB",
            TimeSpan.FromSeconds(60), ct);
        reloaded.Should().BeTrue("динамический параметр применяется Patroni без рестарта ноды");

        // Assert (в): postmaster-параметр помечен pending_restart=true (GET
        // /patroni), рестарт воркером НЕ инициируется: контейнеры живы;
        // применённое значение ещё 2GB (bootstrap-расчёт).
        var pending = await E2eFixture.WaitForAsync(async () =>
            (await GetPatroniFieldAsync(master.PatroniPort, "pending_restart", ct)) == "true",
            TimeSpan.FromSeconds(60), ct);
        pending.Should().BeTrue("Patroni обязан пометить postmaster-расхождение pending_restart");
        (await SqlScalarAsync(master.Dsn, "SELECT current_setting('shared_buffers')", ct))
            .Should().Be("2GB", "postmaster-параметр НЕ применён до рестарта (решение 2026-09-14)");
        (await ListContainerNamesAsync($"pgw-{cluster}-shard1-", all: true))
            .Should().HaveCount(2, "воркер не пересоздаёт и не рестартует ноды конвергенцией");

        // Assert (г): идемпотентность — mod_revision etcd-ключа
        // /service/<scope>/config стабилен окно в несколько тиков надзора.
        var stable = await ConfigModRevisionStableAsync(scope, TimeSpan.FromSeconds(15), ct);
        stable.Should().BeTrue("повторные тики не патчат конвергентный конфиг");

        // Act (д): возврат заявки 16Gi → 8Gi — патч ВНИЗ отрабатывает.
        (await G.PutAsync(Endpoint, $"/service/{scope}/request_mem", "8Gi", null, ct))
            .IsSuccess.Should().BeTrue("заявка request_mem должна вернуться к 8Gi");
        var rolledBack = await E2eFixture.WaitForAsync(async () =>
        {
            var config = await GetPatroniConfigValueAsync(master.PatroniPort, ct);
            return config.Contains("\"effective_cache_size\":\"6GB\"", StringComparison.Ordinal)
                   && config.Contains("\"shared_buffers\":\"2GB\"", StringComparison.Ordinal);
        }, TimeSpan.FromSeconds(120), ct);
        rolledBack.Should().BeTrue("уменьшение значений тоже конвергируется (патч вниз); " +
                                   $"work={await WorkDumpAsync(cluster, ct)}");
    }
```

Хелперы — в секцию «Хелперы» файла:

```csharp
    private static readonly HttpClient PatroniHttp = new() { Timeout = TimeSpan.FromSeconds(3) };

    // Тело GET /config Patroni-ноды (сырой JSON).
    private static async Task<string> GetPatroniConfigValueAsync(int patroniPort, CancellationToken ct)
    {
        using var response = await PatroniHttp.GetAsync($"http://localhost:{patroniPort}/config", ct);
        response.IsSuccessStatusCode.Should().BeTrue($"GET /config → HTTP {(int)response.StatusCode}");
        return await response.Content.ReadAsStringAsync(ct);
    }

    // Поле корня GET /patroni (например, pending_restart) raw-текстом ("true").
    private static async Task<string?> GetPatroniFieldAsync(int patroniPort, string field, CancellationToken ct)
    {
        using var response = await PatroniHttp.GetAsync($"http://localhost:{patroniPort}/patroni", ct);
        if (!response.IsSuccessStatusCode)
            return null;
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct));
        return doc.RootElement.TryGetProperty(field, out var value) ? value.GetRawText() : null;
    }

    // mod_revision etcd-ключа /service/<scope>/config стабилен окно window
    // (перечитываем каждые 2 с; Patroni пишет конфиг только при изменении).
    private async Task<bool> ConfigModRevisionStableAsync(string scope, TimeSpan window, CancellationToken ct)
    {
        var revision = (await GetOrNullAsync($"/service/{scope}/config"))?.ModRevision;
        if (revision is null)
            return false;
        var deadline = DateTimeOffset.UtcNow + window;
        while (DateTimeOffset.UtcNow < deadline)
        {
            await Task.Delay(2000, ct);
            if ((await GetOrNullAsync($"/service/{scope}/config"))?.ModRevision != revision)
                return false;
        }

        return true;
    }

    // SQL-скаляр мастера шарда (паттерн E2eScaleScenarios.SqlScalarAsync).
    private static async Task<string> SqlScalarAsync(string dsn, string sql, CancellationToken ct)
    {
        await using var con = new NpgsqlConnection(
            $"{dsn};Timeout=10;SSL Mode=Require;Trust Server Certificate=true");
        await con.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(sql, con);
        return (await cmd.ExecuteScalarAsync(ct))?.ToString() ?? "";
    }

    private sealed record NodeAddr(string Host, int Pg, int Patroni, int Doorman);

    private async Task<Dictionary<string, NodeAddr>> PortallocAsync(string cluster)
    {
        var kv = await GetOrNullAsync($"/pgworker/portalloc/{cluster}");
        if (kv is null)
            return [];
        return JsonSerializer.Deserialize<Dictionary<string, NodeAddr>>(kv.Value, Json) ?? [];
    }

    private sealed record MasterInfo(string Node, int Port, int PatroniPort, string Dsn);

    // Мастер шарда: резолв по контракту arch/14 §5 C — проба /primary по
    // patroni-портам portalloc (приём E2eScaleScenarios.MasterInfoAsync:
    // матч master-ключа по doorman-порту недискриминантен при EnableDoorman=false).
    private async Task<MasterInfo> MasterInfoAsync(string cluster, string shard, CancellationToken ct)
    {
        for (var i = 0; i < 60; i++)
        {
            var key = await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/master");
            if (key is { Value.Length: > 0 })
            {
                var addresses = await PortallocAsync(cluster);
                foreach (var (nodeKey, addr) in addresses
                             .Where(p => p.Key.StartsWith($"{shard}/", StringComparison.Ordinal))
                             .OrderBy(p => p.Key, StringComparer.Ordinal))
                {
                    try
                    {
                        using var response = await PatroniHttp.GetAsync(
                            $"http://localhost:{addr.Patroni}/primary", ct);
                        if (!response.IsSuccessStatusCode)
                            continue;
                        var node = nodeKey.Split('/')[1];
                        return new MasterInfo(node, addr.Pg, addr.Patroni,
                            $"Host=localhost;Port={addr.Pg};Database={cluster};Username=postgres;Password={E2eFixture.SuPassword}");
                    }
                    catch (Exception)
                    {
                        // сетевой сбой пробы (рестарт/ещё не готова) — не primary
                    }
                }
            }

            await Task.Delay(1000, ct);
        }

        throw new ApplicationException($"мастер {cluster}/{shard} не найден за 60 с");
    }

    private async Task<List<string>> ListContainerNamesAsync(string prefix, bool all = false)
    {
        var ct = TestContext.Current.CancellationToken;
        var args = new List<string> { "ps", "--format", "{{.Names}}" };
        if (all)
            args.Add("-a");
        args.AddRange(["--filter", $"name={prefix}"]);
        var output = await Fx.RunDockerAsync([.. args], ct);
        return output.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(n => n.StartsWith(prefix, StringComparison.Ordinal))
            .ToList();
    }
```

Замечания к сценарию (обязательны):
- изоляция — окружение `E2eEnvironment.StartAsync("pgtune-converge")` на Fact (своя сеть/etcd, guid-тег `ClusterTag` во всех именах, own-only teardown + ассерт чистоты уже в `E2eEnvironment`; ничего хардкодного не добавлять, порты только из portalloc);
- ожидания — только поллинг `E2eFixture.WaitForAsync` (никаких `Thread.Sleep` у агента длиннее 30 с);
- `EnableDoorman=false` в E2e-хосте не влияет на конвергенцию (doorman не участвует).

- [ ] **Step 2: Прогнать сценарий на Debug (docker доступен)**

Run: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~Pgtune_Convergence"`
Expected: PASS. При падении — разбор по канону телеметрии (артефакты `/tmp/pgw-e2e-artifacts-<guid>/`, `host.log`, `docker logs`); перезапуск упавших «чтобы посмотреть» — ЗАПРЕЩЁН (правила AGENTS.md «Телеметрия E2E»).

- [ ] **Step 3: Зачистка + коммит**

После серии: `docker rm -f $(docker ps -aq)` (контейнеры dev-стенда `as-*`/`adminpanel`, если стенд поднят, — не трогать), проверить `docker ps -aq | wc -l` (0 либо только стенд), затем `docker network prune -f`.

```bash
git add src/tests/PgWorker.IntegrationTests/E2e/E2ePgtuneScenarios.cs
git commit -m "test(t11): E2E-сценарий конвергенции pg-параметров — request_mem 8→16Gi→8Gi: PATCH DCS, живой reload динамики (current_setting), postmaster→pending_restart без рестарта, идемпотентность по mod_revision /service/<scope>/config"
```

---

### Task 5: E2E на свежем Release + финальный гейт

Фаза spec §5: 6. Мерж-гейт задачи, трогающей `PgWorker.Provisioning`: кейс-маркер на свежем Release; зачистка контейнеров/сетей после каждой серии.

**Files:**
- Без изменений кода (проверочный таск); правки — только если гейт упал и разбор логов дал конкретный фикс.

- [ ] **Step 1: Полный юнит-прогон в двух конфигурациях**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug && dotnet test src/tests/PgWorker.UnitTests -c Release`
Expected: PASS ×2.

- [ ] **Step 2: Docker-E2E кейс-маркер на свежем Release (E2eFixture собирает Release сам)**

Run: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard`
Expected: PASS (инкрементальная сборка Release; `PGW_TEST_E2E_NOBUILD=1` НЕ использовать).

- [ ] **Step 3: Полный E2ePgtuneScenarios на Release (свежая сборка)**

Run: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter "FullyQualifiedName~E2ePgtuneScenarios"`
Expected: PASS все 4 Fact (3 существующих + новый конвергенции).

- [ ] **Step 4: Зачистка после серий**

Run: `docker rm -f $(docker ps -aq) 2>/dev/null; docker ps -aq | wc -l; docker network prune -f`
Expected: контейнеров нет (кроме поднятого dev-стенда `as-*`/`adminpanel` — не трогать); осиротевших сетей нет.

- [ ] **Step 5: Сверка критериев приёмки spec §7 (чек-лист по результатам прогонов)**

1. Изменение заявки → патч DCS; повторный тик мутаций не делает — Task 4 (а), (г); Task 3 `Regression_T09_DcsConfigConvergence_CanonicalConfig_NoPatch`. ✔
2. Динамика применена живым PG; postmaster → `pending_restart=true`, рестарт не инициируется — Task 4 (б), (в). ✔
3. Исчезнувшие/ExcludeParams параметры удаляются null-патчем — Task 2 `Divergence_ExtraLiveParameter_RemovedWithNullAtEnd` + `Desired_ExcludeCutsParameter`. ✔
4. SPILO_CONFIGURATION байт-в-байт; единый источник + self-check — Task 1 Step 8, Task 2 `Regression_T09_SpiloEnv_AndConvergence_SingleCanonicalSource`. ✔
5. Отсутствие ИЛИ неполнота заявки (частичная и полная) у шарда: pg-часть пропущена (warning-лог), тайминги конвергируются, тик НЕ фейлится — Task 3 `Tick_DcsConfigConvergence_PartialResourceRequest_TimingsOnlyWithWarning` + `Tick_DcsConfigConvergence_NoResourceRequests_TimingsOnlyWithWarning`. ✔
6. Юнит/интеграционные зелёные; docker-E2E Release зелёный; teardown чист — Tasks 1–5. ✔
7. arch/14 §2.1/§5 C синхронизирован тем же коммитом — Task 1 Steps 1–2 + коммит Task 1. ✔

- [ ] **Step 6: Финальный коммит (если были правки по итогам гейта) и подготовка к мержу**

Мерж-гейт (при слиянии в main по явной просьбе пользователя, тем же мерж-коммитом): удалить тег `t11-pgtune-params-convergence` из `arch/roadmap/pgworker.md` (пункт задачи целиком) — правило AGENTS.md «Roadmap — только несделанные задачи». В feature-ветке roadmap-файл НЕ трогать до мержа.

---

## Правила исполнения

- Порядок задач строгий: Task 1 → 2 → 3 → 4 → 5 (Task 2 зависит от `PgParametersCanon`, Task 3 — от `DcsConfigConvergence`, Task 4 — от проводки).
- Коммиты — в feature-ветке `feat-t11-pgtune-params-convergence`, свободно (AGENTS.base §6); мерж в main — только по явной просьбе.
- Каждый прогон docker-серий завершается зачисткой (Task 4 Step 3, Task 5 Step 4): контейнеры + сети; никогда не запускать следующую серию поверх незачищенной предыдущей.
- Упавший docker-сценарий: разбор по артефактам телеметрии (`/tmp/pgw-e2e-artifacts-<guid>/`), повторный прогон — только после полного анализа причин (правила AGENTS.md «Телеметрия E2E»).
- Любая правка плана после апрува — только с явного согласия пользователя (апрув аннулируется правкой).
