# t03-backup-wal-stream — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Непрерывная online-доставка WAL-сегментов каждого шарда Active-кластера в S3 (агент `pg_receivewal` в docker-контейнере, супервизируемый PgWorker) с контролем непрерывности цепочки, lag-зондом, etcd-статусом `/pgworker/backups/<C>/<X>/wal` и панельными алертами.

**Architecture:** Новый проект `src/PgWorker.Backups` (по образцу `PgWorker.Moves`): чистые модели имён WAL/gap-детектора + билдер inline-команды агента + S3-листер (AWSSDK.S3, истина прогресса — объекты S3) + тиковый процесс `WalStreamProcess` (ensure слота → супервиз контейнера → контроль цепочки по расписанию → lag-зонд → статус в etcd). Вызов — из Active-ветки `ReconcileLoop` (после `rotate-app-password`, до `repair`/`moves`). Стоп-семантика — в `WalStreamProcess` + правки `DeprovisioningProcess` (D1/D2) и `RemoveShardProcess` (S1.5 + чистка префикса бэкапов шарда). Панель AdminPanel — чтение префикса + 3 правила алертов. E2E-маркер на живом Release-стенде с MinIO.

**Tech Stack:** .NET 10, C# (`LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`), AWSSDK.S3 (path-style), Npgsql, Testcontainers (etcd/MinIO/postgres, порты динамические), xunit.v3 + FluentAssertions.

**Spec:** [`docs/superpowers/2026-09-10-t03-backup-wal-stream/spec.md`](spec.md) — план аргументируется от spec; канон — [`arch/19-backups.md`](../../../arch/19-backups.md) §3/§4/§5/§6/§7 (правки Ф0 уже в ветке).

**Worktree:** `/Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream` — весь код и все команды выполняются отсюда (пути в задачах — относительно корня worktree).

**Ревью plan↔spec (Фаза 4, раунд 1 — 6 замечаний) учтено** и **повторное ревью (раунд 2 — 5 замечаний) учтено**: сводки — в Self-Review в конце.

## Global Constraints

- .NET 10, `Nullable=enable`, `TreatWarningsAsErrors=true`, `LangVersion=latest`; версии пакетов — ТОЛЬКО `src/Directory.Packages.props` (`ManagePackageVersionsCentrally=true`, `EnablePackageVersionOverride=false`).
- Порты docker-контейнеров в тестах — только динамические (`WithPortBinding(...)` + `GetMappedPublicPort`, зонд свободных портов); никаких литералов `:16000` в expects. Бюджеты фикстур ≤ 100 с (упавший прогон падает быстро).
- Зачистка после КАЖДОЙ тестовой серии: `docker rm -f $(docker ps -aq)` (не трогая контейнеры dev-стенда `as-*`/`adminpanel`, если стенд поднят) + проверка `docker ps -aq | wc -l` + `docker network prune -f`. Никогда не запускать серию поверх незачищенной предыдущей.
- E2E — на свежем Release: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter ...` (E2eFixture собирает сам; `PGW_TEST_E2E_NOBUILD=1` — только бисект).
- Язык: документация/комментарии — русский, идентификаторы — английские. Тесты — с AAA-комментариями (`// Arrange` / `// Act` / `// Assert`).
- Не трогать: HA-контур нод, конфиги Patroni, префиксы чужих писателей. S3-объекты никогда не удаляются автоматически (R4-симметрия).
- Формат etcd-ключа wal — 1:1 с каркасной моделью t01 (`WalStreamState`/`BackupsParser` из `PgWorker.Etcd.Parsing`): формат не меняется.
- PgWorker — единственный писатель `/pgworker/backups/*` (клэйм `<C>`); агент etcd не касается; истина прогресса — объекты S3; «факт над записью» — last_uploaded пишется только из наблюдаемых фактов (S3-объекты или прошлое наблюдение), никогда не фабрикуется от now().
- Имена: контейнер агента `pgw-backup-wal-<C>-<X>`, volume `pgw-backup-wal-<C>-<X>-staging`, слот `pgw_bkp_<C>_<X>` (длиннее 63 → `pgw_bkp_` + sha1(`<C>/<X>`)[:16]).
- Секреты агента — ТОЛЬКО env контейнера (arch/19 §7): PG-параметры — по-переменно, S3-креды — одной `MC_HOST_pgwbkp`-строкой (mc резолвит alias из env); никакого `mc alias set` с секретами в argv.
- Сборка всего решения обязана оставаться зелёной после каждой задачи: `dotnet build src/PgWorker.slnx` (0 warnings as errors).
- Все коммиты — conventional-стиль проекта с русским описанием (`feat(pgworker-backups): ...`); в конце каждой задачи — коммит.

---

### Task 1: Каркас проекта PgWorker.Backups + опции

**Вход (предусловие):** каркас t01 в ветке (`BackupsOptions`/`BackupsParser`), ветка t03 от main.
**Выход (что готово после):** проект `PgWorker.Backups` в решении, пакет `AWSSDK.S3`, runtime-опции `BackupsRuntimeOptions` + `BackupsOptions.ToRuntime()`, env-комментарий в deploy.
**Проверка:** `dotnet build src/PgWorker.slnx` зелёный; юниты `BackupsOptions` PASS.
**Связь со spec:** §3.1 (новый проект), §3.4 (конфиг AgentImage/Wal), §5 (ограничения пакетов).

**Files:**
- Create: `src/PgWorker.Backups/PgWorker.Backups.csproj`
- Create: `src/PgWorker.Backups/BackupsRuntimeOptions.cs`
- Modify: `src/Directory.Packages.props` (+`AWSSDK.S3`)
- Modify: `src/PgWorker.slnx` (папка `/backups/`)
- Modify: `src/PgWorker.App/PgWorker.App.csproj` (+ProjectReference)
- Modify: `src/PgWorker.App/Options.cs` (`BackupsOptions`: `AgentImage`, `Wal`, `S3.AdvertisedEndpoint`, `ToRuntime()`)
- Modify: `src/PgWorker.App/appsettings.json` (секция Backups)
- Modify: `deploy/.env.example` (+заккомментированный `PGW_BACKUP_S3_ADVERTISED_ENDPOINT`)
- Test: `src/tests/PgWorker.UnitTests/App/BackupsOptionsTests.cs` (дополнить)

**Interfaces:**
- Consumes: `BackupsOptions`/`BackupsS3Options`/`BackupsStagingOptions`/`BackupsAgentOptions` из `src/PgWorker.App/Options.cs` (каркас t01).
- Produces: `PgWorker.Backups.BackupsRuntimeOptions` — плоский record runtime-опций (образец `MovesRuntimeOptions`); `BackupsOptions.ToRuntime()` в App. Поля и порядок — ниже, используют задачи 5, 9–12.

- [ ] **Step 1: Добавить пакет AWSSDK.S3**

Взять актуальную стабильную версию (ветка 3.7.x) и вписать в `src/Directory.Packages.props` (секция ItemGroup, по алфавиту):

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && dotnet package search AWSSDK.S3 --take 1 --format json | grep '"version"'
```

```xml
    <PackageVersion Include="AWSSDK.S3" Version="<версия из команды выше>" />
```

- [ ] **Step 2: Создать проект**

`src/PgWorker.Backups/PgWorker.Backups.csproj` (копия зависимостей `PgWorker.Moves.csproj` + AWSSDK.S3):

```xml
<Project Sdk="Microsoft.NET.Sdk">

    <!-- Подсистема бэкапов (arch/19): t03 — WAL-агент pg_receivewal + контроль цепочки.
         Зависимость от Provisioning — ShardEndpoints (резолв мастера) и ProcessOutcome. -->
    <ItemGroup>
        <ProjectReference Include="..\PgWorker.Core\PgWorker.Core.csproj"/>
        <ProjectReference Include="..\PgWorker.Etcd\PgWorker.Etcd.csproj"/>
        <ProjectReference Include="..\PgWorker.Docker\PgWorker.Docker.csproj"/>
        <ProjectReference Include="..\PgWorker.Provisioning\PgWorker.Provisioning.csproj"/>
    </ItemGroup>

    <ItemGroup>
        <PackageReference Include="AWSSDK.S3"/>
        <PackageReference Include="Npgsql"/>
    </ItemGroup>

</Project>
```

В `src/PgWorker.slnx` после папки `/moves/`:

```xml
    <Folder Name="/backups/">
        <Project Path="PgWorker.Backups/PgWorker.Backups.csproj" />
    </Folder>
```

В `src/PgWorker.App/PgWorker.App.csproj` добавить:

```xml
        <ProjectReference Include="..\PgWorker.Backups\PgWorker.Backups.csproj"/>
```

- [ ] **Step 3: Runtime-опции**

`src/PgWorker.Backups/BackupsRuntimeOptions.cs`:

```csharp
namespace PgWorker.Backups;

/// <summary>Runtime-опции подсистемы бэкапов (arch/19 §9): склейка секции
/// PgWorker:Backups для процесса WalStreamProcess (образец MovesRuntimeOptions).
/// Достаётся из App через BackupsOptions.ToRuntime().</summary>
/// <param name="AgentImage">Образ агента/джоба (контракт arch/19 §2, сборка t02).</param>
/// <param name="S3Endpoint">S3 endpoint (MinIO/облако) для КЛИЕНТОВ ВОРОКЕРА.</param>
/// <param name="S3AdvertisedEndpoint">Endpoint, как S3 виден ИЗ контейнеров агентов
/// (single-host стенды: host.docker.internal; null → S3Endpoint — паттерн
/// Etcd:AdvertisedEndpoints). Реализация-деталь t03 (адресация env агента, §7) —
/// канон §9 её не перечисляет сознательно; стенд-включение подсистемы — t02.</param>
/// <param name="S3Bucket">Bucket per-install (arch/19 §5).</param>
/// <param name="WalVerifyIntervalSec">Период list S3 + контроля цепочки (§3).</param>
/// <param name="WalLagMaxSegments">Порог отставания в сегментах → DEGRADED.</param>
/// <param name="WalStaleSec">Порог тишины загрузок → DEGRADED.</param>
public sealed record BackupsRuntimeOptions(
    string AgentImage,
    string S3Endpoint,
    string? S3AdvertisedEndpoint,
    string? S3Region,
    string S3Bucket,
    string S3AccessKey,
    string S3SecretKey,
    bool S3PathStyle,
    string StagingDir,
    long? StagingQuotaBytes,
    double? AgentCpu,
    long? AgentMem,
    int WalVerifyIntervalSec,
    int WalLagMaxSegments,
    int WalStaleSec)
{
    /// <summary>Endpoint для env контейнера агента (§7: передача — env, не строка команды).</summary>
    public string AgentS3Endpoint => S3AdvertisedEndpoint is { Length: > 0 } ? S3AdvertisedEndpoint : S3Endpoint;
}
```

- [ ] **Step 4: Дополнить BackupsOptions в App**

В `src/PgWorker.App/Options.cs`:

В `BackupsS3Options` добавить поле (после `PathStyle`):

```csharp
    /// <summary>Endpoint, как S3 виден ИЗ контейнеров агентов/джобов (single-host:
    /// host.docker.internal; null → Endpoint как есть — паттерн Etcd:AdvertisedEndpoints,
    /// Moves:AdvertisedPublisherHost). Реализация-деталь t03 (env-адресация агента,
    /// arch/19 §7); стенд-включение подсистемы — t02.</summary>
    public string? AdvertisedEndpoint { get; set; }
```

В `BackupsOptions` добавить (после `Agent`):

```csharp
    /// <summary>Образ джобов/агентов бэкапов (контракт arch/19 §2: только инструменты,
    /// команда inline от воркера; сборка образа — t02).</summary>
    public string AgentImage { get; set; } = "pgworker-backup:latest";

    /// <summary>WAL-поток (t03, arch/19 §3/§9): расписание контроля, пороги.</summary>
    public BackupsWalOptions Wal { get; set; } = new();

    /// <summary>Runtime-склейка для WalStreamProcess (t03).</summary>
    public BackupsRuntimeOptions ToRuntime() => new(
        AgentImage,
        S3.Endpoint, S3.AdvertisedEndpoint, S3.Region, S3.Bucket,
        S3.AccessKey, S3.SecretKey, S3.PathStyle,
        Staging.Dir, Staging.QuotaBytes,
        Agent.Cpu, Agent.Mem,
        Wal.VerifyIntervalSec, Wal.LagMaxSegments, Wal.StaleSec);
```

В конец файла — новая секция:

```csharp
/// <summary>WAL-поток шарда (t03, arch/19 §3/§9): период list/контроля цепочки,
/// порог отставания в сегментах, порог тишины загрузок.</summary>
public sealed class BackupsWalOptions
{
    public int VerifyIntervalSec { get; set; } = 30;

    public int LagMaxSegments { get; set; } = 1024;

    public int StaleSec { get; set; } = 300;
}
```

В `src/PgWorker.App/appsettings.json` секцию `"Backups": { "Enabled": false }` заменить на:

```json
    "Backups": {
      "Enabled": false,
      "AgentImage": "pgworker-backup:latest",
      "Wal": {
        "VerifyIntervalSec": 30,
        "LagMaxSegments": 1024,
        "StaleSec": 300
      }
    }
```

- [ ] **Step 5: deploy/.env.example — env-комментарий AdvertisedEndpoint**

В `deploy/.env.example` в блок `PGW_BACKUP_S3_*` (после `PGW_BACKUP_S3_SECRET_KEY`) добавить закомментированную строку — знание о env-адресации живёт в deploy-шаблоне сразу, а не в голове:

```bash
# Endpoint S3, как он виден ИЗ контейнеров агентов/джобов бэкапов (single-host:
# host.docker.internal:9000; пусто → PGW_BACKUP_S3_ENDPOINT как есть). Реализация
# t03; стенд-включение подсистемы (вместе с полными бэкапами) — t02.
#PGW_BACKUP_S3_ADVERTISED_ENDPOINT=
```

(Примечание: включение подсистемы в dev-стенд — вне scope t03 по spec §3.4 — закроет t02.)

- [ ] **Step 6: Тест дефолтов и ToRuntime (AAA)**

Дополнить `src/tests/PgWorker.UnitTests/App/BackupsOptionsTests.cs` (файл уже есть — каркас t01):

```csharp
[Fact]
public void ToRuntime_склеивает_все_секции_и_advertised_fallback()
{
    // Arrange
    var options = new BackupsOptions
    {
        AgentImage = "pgworker-backup:test",
        S3 = new BackupsS3Options
        {
            Endpoint = "http://localhost:9000",
            AdvertisedEndpoint = "http://host.docker.internal:9000",
            Bucket = "b", AccessKey = "a", SecretKey = "s",
        },
        Staging = new BackupsStagingOptions { Dir = "/st", QuotaBytes = 1024 },
        Agent = new BackupsAgentOptions { Cpu = 0.5, Mem = 512 },
        Wal = new BackupsWalOptions { VerifyIntervalSec = 5, LagMaxSegments = 10, StaleSec = 60 },
    };

    // Act
    var runtime = options.ToRuntime();

    // Assert
    runtime.AgentImage.Should().Be("pgworker-backup:test");
    runtime.AgentS3Endpoint.Should().Be("http://host.docker.internal:9000");
    runtime.WalVerifyIntervalSec.Should().Be(5);
    runtime.WalLagMaxSegments.Should().Be(10);
    runtime.WalStaleSec.Should().Be(60);
    runtime.StagingQuotaBytes.Should().Be(1024);
}

[Fact]
public void ToRuntime_без_advertised_берет_endpoint_как_есть()
{
    // Arrange
    var options = new BackupsOptions
    {
        S3 = new BackupsS3Options { Endpoint = "http://minio:9000", Bucket = "b", AccessKey = "a", SecretKey = "s" },
    };

    // Act
    var runtime = options.ToRuntime();

    // Assert
    runtime.AgentS3Endpoint.Should().Be("http://minio:9000");
    runtime.WalVerifyIntervalSec.Should().Be(30); // дефолты канона §9
    runtime.WalLagMaxSegments.Should().Be(1024);
    runtime.WalStaleSec.Should().Be(300);
}
```

Если тесты не видят `PgWorker.Backups.BackupsRuntimeOptions` — добавить в `GlobalUsings.cs` юнит-тестов `global using PgWorker.Backups;`.

- [ ] **Step 7: Прогон**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && dotnet build src/PgWorker.slnx && dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~BackupsOptions"
```

Ожидание: build 0 ошибок/0 warnings, тесты PASS.

- [ ] **Step 8: Commit**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && git add -A && git commit -m "feat(pgworker-backups): каркас проекта PgWorker.Backups + runtime-опции Wal/AgentImage (t03, arch/19 §9)"
```

---

### Task 2: WalFileName — имена WAL-файлов

**Вход:** Task 1 (проект существует).
**Выход:** чистые функции разбора/сравнения имён WAL-сегментов с полным юнит-покрытием.
**Проверка:** `dotnet test --filter WalFileName` PASS.
**Связь со spec:** §3.1 (`Model/WalFileName.cs`: 24 hex, `.partial`, history, `Next()`, `Distance`), §4 Ф1.

**Files:**
- Create: `src/PgWorker.Backups/Model/WalFileName.cs`
- Test: `src/tests/PgWorker.UnitTests/Backups/WalFileNameTests.cs`

**Interfaces:**
- Produces: `PgWorker.Backups.WalFileName` (readonly record struct `Tli/Log/Seg`) со статиками `TryParse(string)`, `TryParseHistory(string)`, `IsPartial(string)`, `FromLsn(uint tli, string lsn)`; инстанс-методы `Next()`, `DistanceTo(WalFileName)`, свойство `Name`. Используют задачи 3, 9, 10.

- [ ] **Step 1: Тесты (AAA)**

`src/tests/PgWorker.UnitTests/Backups/WalFileNameTests.cs`:

```csharp
using FluentAssertions;
using PgWorker.Backups;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Разбор/сравнение имён WAL-файлов (arch/19 §3: TLI(8)+log(8)+seg(8) hex).
public class WalFileNameTests
{
    [Theory]
    [InlineData("000000010000000000000001", 1u, 0u, 1u)]
    [InlineData("0000000200000001000000FE", 2u, 1u, 0xFEu)]
    public void TryParse_корректные_сегменты(string name, uint tli, uint log, uint seg)
    {
        // Arrange / Act
        var parsed = WalFileName.TryParse(name);

        // Assert
        parsed.Should().NotBeNull();
        parsed!.Value.Tli.Should().Be(tli);
        parsed.Value.Log.Should().Be(log);
        parsed.Value.Seg.Should().Be(seg);
        parsed.Value.Name.Should().Be(name.ToLowerInvariant());
    }

    [Theory]
    [InlineData("000000010000000000000001.partial")]  // незакрытый сегмент
    [InlineData("00000002.history")]                  // history-файл
    [InlineData("0000000100000000000000")]            // 23 символа
    [InlineData("0000000100000000000000ZZ")]          // не hex
    [InlineData("backup_manifest")]
    [InlineData("")]
    public void TryParse_не_сегменты_дает_null(string name)
    {
        // Arrange / Act / Assert
        WalFileName.TryParse(name).Should().BeNull();
    }

    [Fact]
    public void TryParse_регистронезависим()
    {
        // Arrange / Act
        var parsed = WalFileName.TryParse("0000000100000000000000AB");

        // Assert — pg_receivewal пишет lowercase, S3 может вернуть иначе
        parsed!.Value.Seg.Should().Be(0xAB);
        parsed.Value.Name.Should().Be("00000001000000000000ab");
    }

    [Theory]
    [InlineData("000000010000000000000001.partial", true)]
    [InlineData("000000010000000000000001", false)]
    [InlineData("00000002.history", false)]
    public void IsPartial_только_partial_суффикс(string name, bool expected)
    {
        // Arrange / Act / Assert
        WalFileName.IsPartial(name).Should().Be(expected);
    }

    [Theory]
    [InlineData("00000002.history", 2u)]
    [InlineData("00000010.history", 16u)]
    public void TryParseHistory_валидные(string name, uint tli)
    {
        // Arrange / Act
        var parsed = WalFileName.TryParseHistory(name);

        // Assert
        parsed.Should().Be(tli);
    }

    [Theory]
    [InlineData("000000010000000000000001")]
    [InlineData("00000002.histories")]
    [InlineData("2.history")]
    public void TryParseHistory_невалидные_дает_null(string name)
    {
        // Arrange / Act / Assert
        WalFileName.TryParseHistory(name).Should().BeNull();
    }

    [Fact]
    public void Next_инкремент_сегмента()
    {
        // Arrange
        var segment = WalFileName.TryParse("000000010000000000000001")!.Value;

        // Act / Assert
        segment.Next().Name.Should().Be("000000010000000000000002");
    }

    [Fact]
    public void Next_переход_0xFF_переносит_log()
    {
        // Arrange — seg=0xFF: следующий = log+1, seg=0 (arch/19 §3)
        var segment = WalFileName.TryParse("0000000100000000000000FF")!.Value;

        // Act / Assert
        segment.Next().Name.Should().Be("000000010000000100000000");
    }

    [Fact]
    public void DistanceTo_разница_в_сегментах_через_log()
    {
        // Arrange — (log=0,seg=254) → (log=2,seg=1): 2*256+1-254 = 259
        var from = WalFileName.TryParse("0000000100000000000000FE")!.Value;
        var to = WalFileName.TryParse("000000010000000200000001")!.Value;

        // Act / Assert
        from.DistanceTo(to).Should().Be(259);
        to.DistanceTo(from).Should().Be(-259);
    }

    [Fact]
    public void FromLsn_сегмент_16MB()
    {
        // Arrange — LSN 0/3000000 при wal_segment_size=16MB: segId=3 → log=0, seg=3
        // Act
        var segment = WalFileName.FromLsn(1, "0/3000000");

        // Assert
        segment.Name.Should().Be("000000010000000000000003");
    }
}
```

- [ ] **Step 2: Прогон — падают**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~WalFileName"
```

Ожидание: FAIL — тип `WalFileName` не существует (CS0103).

- [ ] **Step 3: Реализация**

`src/PgWorker.Backups/Model/WalFileName.cs`:

```csharp
using System.Globalization;

namespace PgWorker.Backups;

/// <summary>Имя WAL-сегмента — 24 hex-символа TLI(8)+log(8)+seg(8) (arch/19 §3).
/// `.partial` — незакрытый сегмент (в цепочку не входит); `NNNNNNNN.history` —
/// timeline-история (загружается обязательно). Чистые функции, без состояния.</summary>
public readonly record struct WalFileName(uint Tli, uint Log, uint Seg)
{
    /// <summary>Стандартный размер WAL-сегмента (16 MiB) — ноды Spilo канона arch/14.</summary>
    public const long SegmentBytes = 16L * 1024 * 1024;

    /// <summary>Сегментов в одном log-файле (seg 0x00..0xFF).</summary>
    public const uint SegsPerLog = 0x100;

    /// <summary>Каноническое имя (lowercase hex).</summary>
    public string Name => $"{Tli:x8}{Log:x8}{Seg:x8}";

    /// <summary>Разобрать имя объекта: строго 24 hex-символа (без суффиксов);
    /// `.partial`/`.history`/мусор → null.</summary>
    public static WalFileName? TryParse(string name)
        => name.Length == 24 && uint.TryParse(
               name.AsSpan(0, 8), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var tli)
           && uint.TryParse(
               name.AsSpan(8, 8), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var log)
           && uint.TryParse(
               name.AsSpan(16, 8), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var seg)
            ? new WalFileName(tli, log, seg)
            : null;

    /// <summary>Незакрытый сегмент (pg_receivewal дописывает) — не грузится агентом.</summary>
    public static bool IsPartial(string name)
        => name.EndsWith(".partial", StringComparison.Ordinal);

    /// <summary>Timeline-история `NNNNNNNN.history` → TLI; иначе null.</summary>
    public static uint? TryParseHistory(string name)
        => name.Length == 8 + ".history".Length
           && name.EndsWith(".history", StringComparison.Ordinal)
           && uint.TryParse(
               name.AsSpan(0, 8), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var tli)
            ? tli
            : null;

    /// <summary>LSN 'X/Y' (pg_current_wal_lsn) → имя сегмента позиции записи мастера
    /// (lag-зонд arch/19 §3): segId = lsn / 16MB; log = segId / 256; seg = segId % 256.</summary>
    public static WalFileName FromLsn(uint tli, string lsn)
    {
        var parts = lsn.Split('/');
        var value = (ulong.Parse(parts[0], NumberStyles.HexNumber, CultureInfo.InvariantCulture) << 32)
            | ulong.Parse(parts[1], NumberStyles.HexNumber, CultureInfo.InvariantCulture);
        var segId = value / (ulong)SegmentBytes;
        return new WalFileName(tli, (uint)(segId / SegsPerLog), (uint)(segId % SegsPerLog));
    }

    /// <summary>Следующий сегмент: seg+1; при seg=0xFF → log+1, seg=0 (arch/19 §3).</summary>
    public WalFileName Next()
        => Seg < 0xFF
            ? this with { Seg = Seg + 1 }
            : this with { Log = Log + 1, Seg = 0 };

    /// <summary>Расстояние в сегментах (other − this по log*256+seg); TLI не участвует
    /// (лаг — про позицию записи, timeline-скачки учитывает контроль цепочки).</summary>
    public long DistanceTo(WalFileName other)
        => (long)(other.Log * SegsPerLog + other.Seg) - (long)(Log * SegsPerLog + Seg);
}
```

- [ ] **Step 4: Прогон — зелёные**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~WalFileName"
```

Ожидание: PASS (все тесты).

- [ ] **Step 5: Commit**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && git add -A && git commit -m "feat(pgworker-backups): WalFileName — разбор/Next/Distance имён WAL-сегментов (t03, arch/19 §3)"
```

---

### Task 3: WalChain — gap-детектор непрерывности

**Вход:** Task 2 (`WalFileName`).
**Выход:** чистый gap-детектор цепочки с правилами TLI-переходов.
**Проверка:** `dotnet test --filter WalChain` PASS (AC3-юниты).
**Связь со spec:** §3.1 (`Model/WalChain.cs`), §3.2 шаг 6 (правила непрерывности), AC3.

**Files:**
- Create: `src/PgWorker.Backups/Model/WalChain.cs`
- Test: `src/tests/PgWorker.UnitTests/Backups/WalChainTests.cs`

**Interfaces:**
- Consumes: `WalFileName` (Task 2).
- Produces: `PgWorker.Backups.ChainResult(bool IsContinuous, string? GapError, WalFileName? LastSegment)`; `PgWorker.Backups.WalChain.Check(WalFileName chainStart, IEnumerable<string> objectNames)` — статический. Использует задача 10.

- [ ] **Step 1: Тесты (AAA)**

`src/tests/PgWorker.UnitTests/Backups/WalChainTests.cs`:

```csharp
using FluentAssertions;
using PgWorker.Backups;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Gap-детектор непрерывности WAL-цепочки (arch/19 §3): последовательность Next()
// без пропусков; TLI-переход валиден при history + первом сегменте нового TLI
// ∈ {последний старого, Next(последний)}; .partial игнорируется.
public class WalChainTests
{
    private static WalFileName Seg(uint tli, uint log, uint seg) => new(tli, log, seg);

    [Fact]
    public void Сплошная_цепочка_непрерывна()
    {
        // Arrange
        var objects = new[]
        {
            "000000010000000000000001", "000000010000000000000002", "000000010000000000000003",
        };

        // Act
        var result = WalChain.Check(Seg(1, 0, 1), objects);

        // Assert
        result.IsContinuous.Should().BeTrue();
        result.GapError.Should().BeNull();
        result.LastSegment!.Value.Name.Should().Be("000000010000000000000003");
    }

    [Fact]
    public void Дыра_внутри_цепочки_дает_разрыв_с_границами()
    {
        // Arrange — нет 000000010000000000000002
        var objects = new[] { "000000010000000000000001", "000000010000000000000003" };

        // Act
        var result = WalChain.Check(Seg(1, 0, 1), objects);

        // Assert
        result.IsContinuous.Should().BeFalse();
        result.GapError.Should().Contain("000000010000000000000002")
            .And.Contain("000000010000000000000003");
    }

    [Fact]
    public void Первый_объект_выше_старта_дает_дыру_от_стартовой_точки()
    {
        // Arrange — цепочка начинается с chainStart, а первый объект позже
        var objects = new[] { "000000010000000000000005" };

        // Act
        var result = WalChain.Check(Seg(1, 0, 1), objects);

        // Assert
        result.IsContinuous.Should().BeFalse();
        result.GapError.Should().Contain("000000010000000000000001");
    }

    [Fact]
    public void Сегменты_раньше_старта_игнорируются()
    {
        // Arrange — хвост до chain_start (ретенция/сдвиг старта полным бэкапом)
        var objects = new[]
        {
            "000000010000000000000001", "000000010000000000000005", "000000010000000000000006",
        };

        // Act
        var result = WalChain.Check(Seg(1, 0, 5), objects);

        // Assert
        result.IsContinuous.Should().BeTrue();
    }

    [Fact]
    public void Partial_сегменты_игнорируются()
    {
        // Arrange
        var objects = new[]
        {
            "000000010000000000000001", "000000010000000000000002.partial",
            "000000010000000000000002",
        };

        // Act
        var result = WalChain.Check(Seg(1, 0, 1), objects);

        // Assert
        result.IsContinuous.Should().BeTrue();
    }

    [Fact]
    public void TLI_переход_с_history_и_повтором_последнего_сегмента_валиден()
    {
        // Arrange — failover в середине сегмента: PG перезаписывает сегмент с нового TLI
        var objects = new[]
        {
            "000000010000000000000001", "000000010000000000000002",
            "00000002.history",
            "000000020000000000000002", // == последнему старого TLI
        };

        // Act
        var result = WalChain.Check(Seg(1, 0, 1), objects);

        // Assert
        result.IsContinuous.Should().BeTrue();
        result.LastSegment!.Value.Tli.Should().Be(2);
    }

    [Fact]
    public void TLI_переход_с_history_и_следующим_сегментом_валиден()
    {
        // Arrange — переключение на границе сегмента: первый новый = Next(последнего)
        var objects = new[]
        {
            "000000010000000000000001",
            "00000002.history",
            "000000020000000000000002",
        };

        // Act
        var result = WalChain.Check(Seg(1, 0, 1), objects);

        // Assert
        result.IsContinuous.Should().BeTrue();
    }

    [Fact]
    public void TLI_переход_без_history_разрыв()
    {
        // Arrange
        var objects = new[]
        {
            "000000010000000000000001", "000000020000000000000002",
        };

        // Act
        var result = WalChain.Check(Seg(1, 0, 1), objects);

        // Assert
        result.IsContinuous.Should().BeFalse();
        result.GapError.Should().Contain("00000002.history");
    }

    [Fact]
    public void TLI_переход_со_скачком_мимо_точки_переключения_разрыв()
    {
        // Arrange — первый сегмент нового TLI ≠ последнему/next (skip позиции)
        var objects = new[]
        {
            "000000010000000000000001", "00000002.history", "000000020000000000000005",
        };

        // Act
        var result = WalChain.Check(Seg(1, 0, 1), objects);

        // Assert
        result.IsContinuous.Should().BeFalse();
    }

    [Fact]
    public void History_без_перехода_не_влияет()
    {
        // Arrange — лишний history (файл от старого failover) — не ошибка
        var objects = new[] { "00000002.history", "000000010000000000000001", "000000010000000000000002" };

        // Act
        var result = WalChain.Check(Seg(1, 0, 1), objects);

        // Assert
        result.IsContinuous.Should().BeTrue();
    }

    [Fact]
    public void Дыра_после_перехода_на_новый_TLI_ловится()
    {
        // Arrange
        var objects = new[]
        {
            "000000010000000000000001", "00000002.history", "000000020000000000000002",
            "000000020000000000000004", // пропущен 3-й
        };

        // Act
        var result = WalChain.Check(Seg(1, 0, 1), objects);

        // Assert
        result.IsContinuous.Should().BeFalse();
        result.GapError.Should().Contain("000000020000000000000003");
    }
}
```

- [ ] **Step 2: Прогон — падают**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~WalChain"
```

Ожидание: FAIL — `WalChain` не существует.

- [ ] **Step 3: Реализация**

`src/PgWorker.Backups/Model/WalChain.cs`:

```csharp
namespace PgWorker.Backups;

/// <summary>Результат контроля цепочки: дыра с границами (для error-статуса) и
/// последний валидный сегмент (для last_uploaded).</summary>
public sealed record ChainResult(bool IsContinuous, string? GapError, WalFileName? LastSegment);

/// <summary>Gap-детектор непрерывности WAL-цепочки от chain_start (arch/19 §3,
/// правила t03 spec §3.1): последовательность Next() без пропусков; TLI-переход
/// валиден при наличии загруженного `&lt;newTLI&gt;.history` и первом сегменте нового
/// TLI ∈ {последний сегмент старого TLI, Next(последний)}; `.partial` игнорируется.
/// Полный LSN-разбор history — t04 (verify).</summary>
public static class WalChain
{
    public static ChainResult Check(WalFileName chainStart, IEnumerable<string> objectNames)
    {
        // Разбор объектов: сегменты (сортировка имени == сортировка (Tli,Log,Seg) —
        // фиксированная ширина hex) + множество загруженных history-TLI.
        var segments = new List<WalFileName>();
        var histories = new HashSet<uint>();
        foreach (var name in objectNames)
        {
            if (WalFileName.IsPartial(name))
                continue; // незакрытый сегмент — не грузится агентом и не входит в цепочку
            if (WalFileName.TryParseHistory(name) is { } tli)
            {
                histories.Add(tli);
                continue;
            }
            if (WalFileName.TryParse(name) is { } segment)
                segments.Add(segment);
        }

        segments.Sort((a, b) => a.Tli != b.Tli ? a.Tli.CompareTo(b.Tli)
            : a.Log != b.Log ? a.Log.CompareTo(b.Log) : a.Seg.CompareTo(b.Seg));

        var expected = chainStart;
        WalFileName? last = null;
        foreach (var segment in segments)
        {
            if (segment.Tli == expected.Tli)
            {
                if (segment.Log == expected.Log && segment.Seg == expected.Seg)
                {
                    // ожидаемый сегмент — цепочка продолжается
                    last = segment;
                    expected = segment.Next();
                }
                else if ((long)(segment.Log * WalFileName.SegsPerLog + segment.Seg)
                         > (long)(expected.Log * WalFileName.SegsPerLog + expected.Seg))
                {
                    // пропуск внутри TLI: границы дыры — ожидали/найдено
                    return new ChainResult(false,
                        $"дыра WAL-цепочки: ожидался {expected.Name}, найден {segment.Name}", last);
                }
                // сегмент меньше expected (дубли/хвост старого потока) — игнор
            }
            else if (segment.Tli > expected.Tli)
            {
                // TLI-переход: обязательна history нового TLI; первый сегмент нового
                // TLI — точка переключения (== last, PG перезаписывает сегмент) или
                // следующий за ней (переход на границе).
                if (!histories.Contains(segment.Tli))
                    return new ChainResult(false,
                        $"TLI-переход {expected.Tli}→{segment.Tli} без {segment.Tli:x8}.history", last);
                var lastOrNext = last is { } prev
                    ? (segment.Log == prev.Log && segment.Seg == prev.Seg)
                      || segment.Equals(prev.Next())
                    : false;
                if (!lastOrNext)
                    return new ChainResult(false,
                        $"TLI-переход {expected.Tli}→{segment.Tli}: первый сегмент {segment.Name} " +
                        $"не совпадает с точкой переключения ({(last is { } l ? l.Name : "нет предыдущего")})", last);
                last = segment;
                expected = segment.Next();
            }
            // сегмент старого TLI после перехода — вне цепочки (сортировка даёт их
            // раньше; сюда попадают только дубли ниже expected) — игнор
        }

        return new ChainResult(true, null, last);
    }
}
```

- [ ] **Step 4: Прогон — зелёные**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~WalChain"
```

Ожидание: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && git add -A && git commit -m "feat(pgworker-backups): WalChain — gap-детектор непрерывности с TLI-переходами (t03, arch/19 §3)"
```

---

### Task 4: WalAgentCommand — inline bash агента

**Вход:** Task 1.
**Выход:** билдер inline-команды контейнера агента — единственное место механики шиппера; S3-креды — через `MC_HOST_pgwbkp` env (mc резолвит alias из env, секреты не попадают в argv/`ps`), PG-секреты — по-переменно env.
**Проверка:** `dotnet test --filter WalAgentCommand` PASS (AC3: фильтр `.partial`, обязательность history; MC_HOST-escape).
**Связь со spec:** §3.1 (`WalAgentCommand.cs`, «секреты — через env контейнера (не интерполяция в строку команды)»), §2 принцип 4, §7 (передача секретов — env).

**Files:**
- Create: `src/PgWorker.Backups/WalAgentCommand.cs`
- Test: `src/tests/PgWorker.UnitTests/Backups/WalAgentCommandTests.cs`

**Interfaces:**
- Produces: `PgWorker.Backups.WalAgentCommand`:
  - `Build()` → `["bash","-c",<script>]` (Cmd контейнера агента; скрипт НЕ содержит S3-кредов и `mc alias set` — alias `pgwbkp` резолвится mc из env `MC_HOST_pgwbkp`);
  - `static string McHost(string endpoint, string accessKey, string secretKey)` — сборка значения env `MC_HOST_pgwbkp` с URL-escape кредов (зовёт `AgentEnv` задачи 9);
  - константы env-имён: `MC_HOST_PGWBKP` («MC_HOST_pgwbkp»), `S3_BUCKET/CLUSTER/SHARD/SLOT/PG_HOST/PG_PORT/PG_USER/PG_PASSWORD/PG_DBNAME/STAGING_DIR/STAGING_QUOTA_BYTES/POLL_SEC`.

- [ ] **Step 1: Тесты (AAA)**

`src/tests/PgWorker.UnitTests/Backups/WalAgentCommandTests.cs`:

```csharp
using FluentAssertions;
using PgWorker.Backups;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Inline-команда контейнера агента (arch/19 §2/§3): механика шиппера версионируется
// кодом воркера; секреты — env контейнера, не строка команды и не argv процессов.
public class WalAgentCommandTests
{
    [Fact]
    public void Build_bash_с_скриптом()
    {
        // Arrange / Act
        var cmd = WalAgentCommand.Build();

        // Assert
        cmd.Should().HaveCount(3);
        cmd[0].Should().Be("bash");
        cmd[1].Should().Be("-c");
        cmd[2].Should().ContainAll(
            "pg_receivewal", "--slot=", "mc cp", "rm -f",
            "*.partial" /* AC3: фильтр .partial */,
            "pgwbkp/$S3_BUCKET" /* alias из env MC_HOST_pgwbkp, без alias set */,
            "PG_HOST", "PG_PASSWORD", "SLOT");
    }

    [Fact]
    public void Build_скрипт_без_S3_кредов_и_alias_set_в_команде()
    {
        // Arrange / Act — креды уходят ТОЛЬКО env-строкой MC_HOST (ревью Ф4-2 №5),
        // mc alias set не вызывается: секреты не попадают в argv процессов (ps)
        var script = WalAgentCommand.Build()[2];

        // Assert
        script.Should().NotContain("mc alias set");
        script.Should().NotContain("$S3_ACCESS_KEY");
        script.Should().NotContain("$S3_SECRET_KEY");
        script.Should().NotContain("minioadmin"); // никаких литеральных секретов
    }

    [Fact]
    public void Build_скрипт_не_содержит_литеральных_PG_секретов()
    {
        // Arrange / Act
        var script = string.Join(" ", WalAgentCommand.Build());

        // Assert — env-имена, но никакой интерполяции значений в Cmd
        script.Should().NotContainAny(["password=", "$PG_PASSWORD\""]);
        script.Should().NotContain("host=");
        // conninfo собирается ВНУТРИ контейнера из env, не в строке команды
        script.Should().Contain("\"host=$PG_HOST");
    }

    [Fact]
    public void Build_грузит_history_обязательно()
    {
        // Arrange / Act — history не матчится *.partial-фильтром → попадает в mc cp
        var script = WalAgentCommand.Build()[2];

        // Assert — AC3: фильтр строго по .partial, расширения .history не исключаются
        script.Should().Contain("! -name '*.partial'");
        script.Should().NotContain("! -name '*.history'");
    }

    [Fact]
    public void Build_умирает_при_смерти_pg_receivewal()
    {
        // Arrange / Act — смерть приёмника обязана гасить контейнер (restart-контур
        // супервиза: exited/restarting → пересоздание воркером)
        var script = WalAgentCommand.Build()[2];

        // Assert
        script.Should().Contain("kill -0").And.Contain("wait");
    }

    [Fact]
    public void Build_квота_staging_при_заданной()
    {
        // Arrange / Act
        var script = WalAgentCommand.Build()[2];

        // Assert — guard «нет места» (arch/19 §6): du-проверка → exit
        script.Should().Contain("STAGING_QUOTA_BYTES").And.Contain("du -sb");
    }

    [Fact]
    public void Build_пишет_в_wal_префикс_шарда()
    {
        // Arrange / Act
        var script = WalAgentCommand.Build()[2];

        // Assert — layout arch/19 §5: <C>/<X>/wal/<name>
        script.Should().Contain("/$CLUSTER/$SHARD/wal/");
    }

    [Fact]
    public void McHost_собирает_URL_с_URL_escape_кредов()
    {
        // Arrange / Act — mc читает MC_HOST_<alias>: scheme://access:secret@authority;
        // escape обязателен: секреты per-install могут содержать спецсимволы URL
        var host = WalAgentCommand.McHost("http://localhost:9000", "ak", "sk/p@ss");

        // Assert
        host.Should().Be("http://ak:sk%2Fp%40ss@localhost:9000");
    }

    [Fact]
    public void McHost_именованный_переменной_агента()
    {
        // Arrange / Act / Assert — имя env-переменной = alias шиппера (контракт with AgentEnv Task 9)
        WalAgentCommand.EnvMcHostVariable.Should().Be("MC_HOST_pgwbkp");
    }
}
```

- [ ] **Step 2: Прогон — падают**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~WalAgentCommand"
```

Ожидание: FAIL.

- [ ] **Step 3: Реализация**

`src/PgWorker.Backups/WalAgentCommand.cs`:

```csharp
namespace PgWorker.Backups;

/// <summary>Билдер inline bash-команды контейнера WAL-агента (arch/19 §2/§3) —
/// ЕДИНСТВЕННОЕ место механики шиппера: запуск pg_receivewal в staging + цикл
/// доставки закрытых сегментов/history в S3 через mc с удалением после успешного
/// cp; `.partial` не грузится; смерть pg_receivewal гасит контейнер (restart-луп
/// подхватывает unless-stopped, воркер пересоздаёт со свежими env). Секреты —
/// ТОЛЬКО env контейнера (§7): S3 — одной строкой MC_HOST_pgwbkp (mc резолвит
/// alias из env: секреты не попадают в argv процессов и в ps), PG — по-переменно;
/// скрипт оперирует именами переменных.</summary>
public static class WalAgentCommand
{
    /// <summary>Имя env-переменной mc-alias (формат mc: MC_HOST_&lt;alias&gt;).</summary>
    public const string EnvMcHostVariable = "MC_HOST_pgwbkp";

    public const string EnvS3Bucket = "S3_BUCKET";
    public const string EnvCluster = "CLUSTER";
    public const string EnvShard = "SHARD";
    public const string EnvSlot = "SLOT";
    public const string EnvPgHost = "PG_HOST";
    public const string EnvPgPort = "PG_PORT";
    public const string EnvPgUser = "PG_USER";
    public const string EnvPgPassword = "PG_PASSWORD";
    public const string EnvPgDbName = "PG_DBNAME";
    public const string EnvStagingDir = "STAGING_DIR";
    public const string EnvStagingQuotaBytes = "STAGING_QUOTA_BYTES";
    public const string EnvPollSec = "POLL_SEC";

    /// <summary>Значение env MC_HOST_pgwbkp: scheme://access:secret@authority.
    /// URL-escape кредов — секреты per-install могут содержать спецсимволы URL.</summary>
    public static string McHost(string endpoint, string accessKey, string secretKey)
    {
        var uri = new Uri(endpoint);
        return $"{uri.Scheme}://{Uri.EscapeDataString(accessKey)}:{Uri.EscapeDataString(secretKey)}@{uri.Authority}";
    }

    /// <summary>Cmd контейнера: ["bash","-c",script]. S3-креды скрипту не нужны —
    /// alias pgwbkp приходит env-строкой MC_HOST_pgwbkp.</summary>
    public static IReadOnlyList<string> Build()
    {
        const string script = """
set -euo pipefail
# S3: alias pgwbkp резолвится mc из env MC_HOST_pgwbkp — креды не в argv (не видны в ps)
pg_receivewal --slot="$SLOT" -D "$STAGING_DIR" \
  -d "host=$PG_HOST port=$PG_PORT user=$PG_USER password=$PG_PASSWORD dbname=$PG_DBNAME sslmode=require" &
RECEIVE_PID=$!
while kill -0 "$RECEIVE_PID" 2>/dev/null; do
  if [ -n "${STAGING_QUOTA_BYTES:-}" ]; then
    USED=$(du -sb "$STAGING_DIR" | cut -f1)
    [ "$USED" -gt "$STAGING_QUOTA_BYTES" ] && exit 4
  fi
  find "$STAGING_DIR" -maxdepth 1 -type f ! -name '*.partial' -print0 |
    while IFS= read -r -d '' f; do
      if mc cp -- "$f" "pgwbkp/$S3_BUCKET/$CLUSTER/$SHARD/wal/$(basename -- "$f")"; then
        rm -f -- "$f"
      fi
    done
  sleep "$POLL_SEC"
done
wait "$RECEIVE_PID"
""";
        return ["bash", "-c", script];
    }
}
```

- [ ] **Step 4: Прогон — зелёные**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~WalAgentCommand"
```

Ожидание: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && git add -A && git commit -m "feat(pgworker-backups): WalAgentCommand — inline bash pg_receivewal+mc, S3-креды через MC_HOST env (t03, arch/19 §2/§3/§7)"
```

---

### Task 5: BackupS3 — S3-листер (MinIO testcontainers)

**Вход:** Tasks 1 (runtime-опции), 2 (имена сегментов — для тест-данных).
**Выход:** тонкий S3-листер wal-префикса с пагинацией; продакшен-поверхность интерфейса — строго по spec (list + диагностика bucket), создание bucket — тестовая утилита фикстур (прямой AWSSDK-клиент), НЕ интерфейс подсистемы.
**Проверка:** интеграции `BackupS3Tests` PASS против testcontainers-MinIO (динамический порт).
**Связь со spec:** §3.1 (`BackupS3.cs`: ListWalAsync/BucketExistsAsync — «никаких других операций t03 не требует»), Ф2, §5 (динамические порты).

**Files:**
- Create: `src/PgWorker.Backups/BackupS3.cs`
- Create: `src/tests/PgWorker.IntegrationTests/Backups/MinioFixture.cs`
- Test: `src/tests/PgWorker.IntegrationTests/Backups/BackupS3Tests.cs`

**Interfaces:**
- Consumes: `BackupsRuntimeOptions` (Task 1).
- Produces: `PgWorker.Backups.IBackupS3` / `BackupS3`:
  - `Task<Result<bool>> BucketExistsAsync(CancellationToken ct)` (диагностика старта)
  - `Task<Result<IReadOnlyList<WalObject>>> ListWalAsync(string cluster, string shard, int? maxKeysPerTest, CancellationToken ct)` — list-objects-v2 с пагинацией по префиксу `<C>/<X>/wal/`; `record WalObject(string Name, DateTimeOffset LastModified)`.
  Используют задачи 9–10. `maxKeysPerTest` — инъекция размера страницы для теста пагинации (null → библиотечный максимум). Создание bucket в тестах/E2E — прямой `AmazonS3Client.PutBucketAsync` в фикстурах (вне `IBackupS3` — поверхность интерфейса по spec).

- [ ] **Step 1: Реализация BackupS3 (поверхность строго по spec)**

`src/PgWorker.Backups/BackupS3.cs`:

```csharp
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using PgWorker.Core;

namespace PgWorker.Backups;

/// <summary>Объект WAL-префикса: имя (последний компонент ключа) + факт времени
/// последней модификации (для last_uploaded_unix — S3 истина, arch/19 §3).</summary>
public sealed record WalObject(string Name, DateTimeOffset LastModified);

/// <summary>Тонкая обёртка S3-клиента (t03): list-objects-v2 с пагинацией по
/// префиксу `<C>/<X>/wal/` + диагностика старта (BucketExists). Загрузку делает
/// mc внутри агента — других операций t03 не требует (spec §3.1). PathStyle —
/// MinIO и облако одним клиентом (ForcePathStyle). Создание bucket — забота
/// стенда/фикстур (прямой AWSSDK-клиент), НЕ интерфейс подсистемы.</summary>
public interface IBackupS3
{
    Task<Result<bool>> BucketExistsAsync(CancellationToken ct);

    /// <summary>maxKeysPerTest — инъекция размера страницы (тест пагинации); null — максимум.</summary>
    Task<Result<IReadOnlyList<WalObject>>> ListWalAsync(
        string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default);
}

public sealed class BackupS3 : IBackupS3, IAsyncDisposable
{
    private readonly AmazonS3Client _client;
    private readonly string _bucket;

    public BackupS3(BackupsRuntimeOptions options)
    {
        _bucket = options.S3Bucket;
        var config = new AmazonS3Config
        {
            ServiceURL = options.S3Endpoint,
            ForcePathStyle = options.S3PathStyle,
            AuthenticationRegion = options.S3Region,
        };
        _client = new AmazonS3Client(
            new BasicAWSCredentials(options.S3AccessKey, options.S3SecretKey), config);
    }

    public async Task<Result<bool>> BucketExistsAsync(CancellationToken ct)
    {
        try
        {
            await _client.ListBucketsAsync(ct);
            return Result<bool>.Success(true);
        }
        catch (AmazonS3Exception e) when (e.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return Result<bool>.Success(false);
        }
        catch (Exception e)
        {
            return Result<bool>.Failed(new ApplicationException($"S3 list-buckets: {e.Message}", e));
        }
    }

    public async Task<Result<IReadOnlyList<WalObject>>> ListWalAsync(
        string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
    {
        try
        {
            var result = new List<WalObject>();
            string? token = null;
            do
            {
                var request = new ListObjectsV2Request
                {
                    BucketName = _bucket,
                    Prefix = $"{cluster}/{shard}/wal/",
                    ContinuationToken = token,
                    MaxKeys = maxKeysPerTest,
                };
                var page = await _client.ListObjectsV2Async(request, ct);
                foreach (var obj in page.S3Objects)
                {
                    var name = obj.Key[(obj.Key.LastIndexOf('/') + 1)..];
                    if (name.Length > 0)
                        result.Add(new WalObject(name, obj.LastModified));
                }

                token = page.IsTruncated is true ? page.NextContinuationToken : null;
            }
            while (token is not null);

            return Result<IReadOnlyList<WalObject>>.Success(result);
        }
        catch (Exception e)
        {
            return Result<IReadOnlyList<WalObject>>.Failed(new ApplicationException(
                $"S3 list {cluster}/{shard}/wal/: {e.Message}", e));
        }
    }

    public ValueTask DisposeAsync()
    {
        _client.Dispose();
        return ValueTask.CompletedTask;
    }
}
```

- [ ] **Step 2: MinIO-фикстура (динамический порт!)**

`src/tests/PgWorker.IntegrationTests/Backups/MinioFixture.cs` — bucket создаётся ПРЯМЫМ AWSSDK-клиентом (не через `IBackupS3`):

```csharp
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using PgWorker.Backups;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

// MinIO testcontainers для S3-интеграций бэкапов (t03): ДИНАМИЧЕСКИЙ хост-порт
// (никаких литералов), bucket создаётся фикстурой прямым AWSSDK-клиентом.
public sealed class MinioFixture : IAsyncLifetime
{
    public const string AccessKey = "minioadmin";
    public const string SecretKey = "minioadmin";
    public const string Bucket = "pgw-backups-test";

    private IContainer? _minio;

    // Endpoint для ХОСТА-клиента (тест): localhost:<динамический порт>.
    public string HostEndpoint { get; private set; } = "";

    // Endpoint для КОНТЕЙНЕРОВ (агент): host.docker.internal:<тот же порт>.
    public string ContainerEndpoint { get; private set; } = "";

    public async ValueTask InitializeAsync()
    {
        // Порт публикуется на свободный хост-порт (GetMappedPublicPort ниже).
        _minio = new ContainerBuilder("minio/minio:RELEASE.2025-09-07T16-13-09Z")
            .WithCommand("server", "/data", "--console-address", ":9001")
            .WithEnvironment("MINIO_ROOT_USER", AccessKey)
            .WithEnvironment("MINIO_ROOT_PASSWORD", SecretKey)
            .WithPortBinding(9000, assignRandomHostPort: true)
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilCommandIsCompleted("mc", "ready", "local")
                .WithTimeout(TimeSpan.FromSeconds(45)))
            .Build();
        await _minio.StartAsync(TestContext.Current.CancellationToken);

        var port = _minio.GetMappedPublicPort(9000);
        HostEndpoint = $"http://localhost:{port}";
        ContainerEndpoint = $"http://host.docker.internal:{port}";

        // bucket per-install — прямой AWSSDK-клиент (создание bucket — НЕ операция
        // подсистемы: spec §3.1; тестовая утилита фикстуры).
        var client = new AmazonS3Client(
            new BasicAWSCredentials(AccessKey, SecretKey),
            new AmazonS3Config { ServiceURL = HostEndpoint, ForcePathStyle = true });
        await client.PutBucketAsync(new PutBucketRequest { BucketName = Bucket },
            TestContext.Current.CancellationToken);
        client.Dispose();
    }

    public BackupsRuntimeOptions Runtime() => new(
        "pgworker-backup:test", HostEndpoint, ContainerEndpoint, null,
        Bucket, AccessKey, SecretKey, PathStyle: true,
        "/backup-staging", null, null, null,
        WalVerifyIntervalSec: 30, WalLagMaxSegments: 1024, WalStaleSec: 300);

    public async ValueTask DisposeAsync()
    {
        if (_minio is not null)
            await _minio.DisposeAsync();
    }
}

[CollectionDefinition(Name)]
public sealed class MinioCollection : ICollectionFixture<MinioFixture>
{
    public const string Name = "minio";
}
```

- [ ] **Step 3: Интеграционные тесты (AAA)**

`src/tests/PgWorker.IntegrationTests/Backups/BackupS3Tests.cs`:

```csharp
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using FluentAssertions;
using PgWorker.Backups;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

// S3-листер против живого MinIO (testcontainers, динамический порт): list/
// пагинация/отсутствие префикса — t03 spec Ф2.
[Collection(MinioCollection.Name)]
public class BackupS3Tests(MinioFixture fixture) : IAsyncLifetime
{
    // Прямой клиент-помощник для сида объектов (AWSSDK, не тестируемый код).
    private AmazonS3Client SeedClient(MinioFixture f) => new(
        new BasicAWSCredentials(MinioFixture.AccessKey, MinioFixture.SecretKey),
        new AmazonS3Config { ServiceURL = f.HostEndpoint, ForcePathStyle = true });

    public Task InitializeAsync() => Task.CompletedTask;

    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task BucketExists_живой_bucket()
    {
        // Arrange
        await using var s3 = new BackupS3(fixture.Runtime());

        // Act
        var exists = await s3.BucketExistsAsync(TestContext.Current.CancellationToken);

        // Assert
        exists.IsSuccess.Should().BeTrue();
        exists.Value.Should().BeTrue();
    }

    [Fact]
    public async Task ListWal_пустой_префикс_пустой_список()
    {
        // Arrange
        await using var s3 = new BackupS3(fixture.Runtime());

        // Act
        var listed = await s3.ListWalAsync("c1", "shard1", ct: TestContext.Current.CancellationToken);

        // Assert — отсутствие объектов — валидный пустой результат (не ошибка)
        listed.IsSuccess.Should().BeTrue();
        listed.Value.Should().BeEmpty();
    }

    [Fact]
    public async Task ListWal_возвращает_только_wal_префикс_шарда()
    {
        // Arrange
        await using var client = SeedClient(fixture);
        await client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = MinioFixture.Bucket,
            Key = "c1/shard1/wal/000000010000000000000001",
            ContentBody = "x",
        });
        await client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = MinioFixture.Bucket,
            Key = "c1/shard2/wal/000000010000000000000001", // чужой шард
            ContentBody = "x",
        });
        await client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = MinioFixture.Bucket,
            Key = "c1/shard1/full/20260910120000Z/base.tar", // не-wal
            ContentBody = "x",
        });
        await using var s3 = new BackupS3(fixture.Runtime());

        // Act
        var listed = await s3.ListWalAsync("c1", "shard1", ct: TestContext.Current.CancellationToken);

        // Assert
        listed.Value.Should().ContainSingle(o => o.Name == "000000010000000000000001");
    }

    [Fact]
    public async Task ListWal_пагинация_maxKeys_2_собирает_все()
    {
        // Arrange — 5 объектов, страница по 2 → 3 запроса
        await using var client = SeedClient(fixture);
        for (var i = 1; i <= 5; i++)
            await client.PutObjectAsync(new PutObjectRequest
            {
                BucketName = MinioFixture.Bucket,
                Key = $"c2/shard1/wal/0000000100000000000000{i:x2}",
                ContentBody = "x",
            });
        await using var s3 = new BackupS3(fixture.Runtime());

        // Act
        var listed = await s3.ListWalAsync("c2", "shard1", maxKeysPerTest: 2,
            ct: TestContext.Current.CancellationToken);

        // Assert
        listed.Value.Should().HaveCount(5);
    }

    [Fact]
    public async Task ListWal_отдает_LastModified_объекта()
    {
        // Arrange
        await using var client = SeedClient(fixture);
        var put = await client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = MinioFixture.Bucket,
            Key = "c3/shard1/wal/000000010000000000000001",
            ContentBody = "x",
        });
        await using var s3 = new BackupS3(fixture.Runtime());

        // Act
        var listed = await s3.ListWalAsync("c3", "shard1", ct: TestContext.Current.CancellationToken);

        // Assert — время модификации = время загрузки (для last_uploaded_unix)
        listed.Value.Should().ContainSingle();
        listed.Value[0].LastModified.Should().BeWithin(TimeSpan.FromMinutes(1)).After(DateTimeOffset.UtcNow.AddMinutes(-1));
    }
}
```

В `src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj` добавить `ProjectReference` на `src/PgWorker.Backups/PgWorker.Backups.csproj`.

- [ ] **Step 4: Прогон**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~BackupS3Tests"
```

Ожидание: PASS (5 тестов). Если тесты скипаются — проверить `PGW_TEST_DOCKER=1`.

После серии — зачистка:

```bash
docker rm -f $(docker ps -aq) 2>/dev/null; docker network prune -f; docker ps -aq | wc -l
```

(при поднятом dev-стенде не трогать `as-*`/`adminpanel`).

- [ ] **Step 5: Commit**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && git add -A && git commit -m "feat(pgworker-backups): BackupS3 — list-v2 пагинация wal-префикса + MinIO-интеграции (t03, arch/19 §5)"
```

---

### Task 6: WalStatusWriter — статус в etcd (put при изменении)

**Вход:** Task 1; каркасная модель `WalStreamState` (t01).
**Выход:** статус-райтер ключа `/pgworker/backups/<C>/<X>/wal` в формате t01 1:1, put только при изменении, честный failover по endpoints (continue-on-failure).
**Проверка:** юниты `WalStatusWriter` PASS (включая read-парсинг каркасным форматом и failover-кейс).
**Связь со spec:** §3.1 (`WalStatusWriter.cs`), §3.2 шаг 8, ограничение «совместимость 1:1»; паттерны arch/17 (failover-обёртки ShardEndpoints.GetAsync).

**Files:**
- Create: `src/PgWorker.Backups/WalStatusWriter.cs`
- Test: `src/tests/PgWorker.UnitTests/Backups/WalStatusWriterTests.cs`

**Interfaces:**
- Consumes: `WalStreamState` из `PgWorker.Etcd.Parsing/BackupsModel.cs` (каркас t01), `FakeEtcdGateway` (существует в `src/tests/PgWorker.UnitTests/Api/FakeEtcdGateway.cs`).
- Produces: `PgWorker.Backups.WalStatusWriter(IEtcdGateway, string[] endpoints)`:
  - `Task<Result<WalStreamState?>> ReadAsync(string cluster, string shard, CancellationToken ct)`
  - `Task<Result> WriteIfChangedAsync(string cluster, string shard, WalStreamState state, CancellationToken ct)`
  - `static string ToJson(WalStreamState)` — snake_case JSON канона §4.
  Оба метода — failover по списку endpoints (continue-on-failure, последний отказ — итог; ревью Ф4-2 №4). Используют задачи 9–11.

- [ ] **Step 1: Тесты (AAA)**

`src/tests/PgWorker.UnitTests/Backups/WalStatusWriterTests.cs`:

```csharp
using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Etcd.Client;
using PgWorker.Core;
using PgWorker.Etcd.Parsing;
using PgWorker.UnitTests.Api;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Статус-райтер WAL-потока: JSON 1:1 с каркасным форматом t01 (парсер читает без
// parseErrors), идемпотентность — безделье не пишет; failover по endpoints.
public class WalStatusWriterTests
{
    private static readonly WalStatusWriter Writer = new(
        new FakeEtcdGateway(), ["http://test"]);

    private static WalStreamState Sample(WalStreamStatus state = WalStreamStatus.Active) => new(
        state, "pgw_bkp_c1_shard1", "shard1a",
        "000000010000000000000001", "000000010000000000000005",
        "000000010000000000000005", 1757500000, 3, null);

    [Fact]
    public void ToJson_формат_канона_snake_case()
    {
        // Arrange / Act
        var json = WalStatusWriter.ToJson(Sample());

        // Assert — поля ключа arch/19 §4, значения каноническими строками
        json.Should().Contain("\"state\":\"ACTIVE\"")
            .And.Contain("\"slot\":\"pgw_bkp_c1_shard1\"")
            .And.Contain("\"master_node\":\"shard1a\"")
            .And.Contain("\"chain_start_segment\":\"000000010000000000000001\"")
            .And.Contain("\"last_received_segment\":\"000000010000000000000005\"")
            .And.Contain("\"last_uploaded_segment\":\"000000010000000000000005\"")
            .And.Contain("\"last_uploaded_unix\":1757500000")
            .And.Contain("\"lag_segments\":3");
        json.Should().NotContain("\"Error\""); // camelCase нет
    }

    [Fact]
    public void ToJson_error_null_опускается_degraded_несет_error()
    {
        // Arrange
        var degraded = Sample(WalStreamStatus.Degraded) with { Error = "дыра WAL-цепочки: ожидался X" };

        // Act
        var active = WalStatusWriter.ToJson(Sample());
        var degradedJson = WalStatusWriter.ToJson(degraded);

        // Assert
        active.Should().NotContain("\"error\"");
        degradedJson.Should().Contain("\"error\":\"дыра WAL-цепочки: ожидался X\"");
    }

    [Fact]
    public async Task WriteIfChanged_новый_ключ_пишется_и_читается_парсером_t01()
    {
        // Arrange
        var gateway = new FakeEtcdGateway();
        var writer = new WalStatusWriter(gateway, ["http://test"]);

        // Act
        var written = await writer.WriteIfChangedAsync("c1", "shard1", Sample(), ct: TestContext.Current.CancellationToken);
        var read = await writer.ReadAsync("c1", "shard1", ct: TestContext.Current.CancellationToken);

        // Assert
        written.IsSuccess.Should().BeTrue();
        read.Value.Should().BeEquivalentTo(Sample());
    }

    [Fact]
    public async Task WriteIfChanged_повтор_без_изменений_не_пишет()
    {
        // Arrange
        var gateway = new FakeEtcdGateway();
        var writer = new WalStatusWriter(gateway, ["http://test"]);
        await writer.WriteIfChangedAsync("c1", "shard1", Sample(), ct: TestContext.Current.CancellationToken);

        // Act — то же значение
        var second = await writer.WriteIfChangedAsync("c1", "shard1", Sample(), ct: TestContext.Current.CancellationToken);

        // Assert — идемпотентность: успех без второй Put-мутации
        second.IsSuccess.Should().BeTrue();
        gateway.Store["/pgworker/backups/c1/shard1/wal"].Should().Be(WalStatusWriter.ToJson(Sample()));
    }

    [Fact]
    public async Task WriteIfChanged_изменение_state_перезаписывает()
    {
        // Arrange
        var gateway = new FakeEtcdGateway();
        var writer = new WalStatusWriter(gateway, ["http://test"]);
        await writer.WriteIfChangedAsync("c1", "shard1", Sample(), ct: TestContext.Current.CancellationToken);

        // Act
        var stopped = Sample(WalStreamStatus.Stopped);
        await writer.WriteIfChangedAsync("c1", "shard1", stopped, ct: TestContext.Current.CancellationToken);

        // Assert
        (await writer.ReadAsync("c1", "shard1", ct: TestContext.Current.CancellationToken))
            .Value!.State.Should().Be(WalStreamStatus.Stopped);
    }

    [Fact]
    public async Task ReadAsync_нет_ключа_null()
    {
        // Arrange / Act
        var read = await Writer.ReadAsync("nope", "shardX", ct: TestContext.Current.CancellationToken);

        // Assert — нет ключа = агент не поднимался, НЕ ошибка
        read.IsSuccess.Should().BeTrue();
        read.Value.Should().BeNull();
    }

    [Fact]
    public async Task WriteIfChanged_отказ_первого_endpoint_failover_на_второй()
    {
        // Arrange — «плохой» endpoint отвечает отказом, «хороший» — живой
        // (ревью Ф4-2 №4: отказ endpoints[0] не должен ронять запись при живых остальных)
        var good = new FakeEtcdGateway();
        var writer = new WalStatusWriter(
            new FirstEndpointFailsGateway(good, "http://bad"),
            ["http://bad", "http://good"]);

        // Act
        var written = await writer.WriteIfChangedAsync("c1", "shard1", Sample(), ct: TestContext.Current.CancellationToken);
        var read = await new WalStatusWriter(good, ["http://good"])
            .ReadAsync("c1", "shard1", ct: TestContext.Current.CancellationToken);

        // Assert — запись прошла через живой endpoint
        written.IsSuccess.Should().BeTrue();
        read.Value.Should().BeEquivalentTo(Sample());
    }

    // Фейк с отказом одного endpoint: все операции на badEndpoint → Failed,
    // остальные — делегируются inner (паттерн failover-тестов).
    private sealed class FirstEndpointFailsGateway(IEtcdGateway inner, string badEndpoint) : IEtcdGateway
    {
        private Result<T> Fail<T>(string endpoint) =>
            Result<T>.Failed(new ApplicationException($"endpoint {endpoint} недоступен"));

        public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
            => endpoint == badEndpoint
                ? Task.FromResult(Fail<IReadOnlyList<Kv>>(endpoint))
                : inner.RangeAsync(endpoint, prefix, ct);

        public Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
            => endpoint == badEndpoint
                ? Task.FromResult(Fail<Kv?>(endpoint))
                : inner.GetAsync(endpoint, key, ct);

        public Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
            => endpoint == badEndpoint
                ? Task.FromResult(Result.Failed(new ApplicationException($"endpoint {endpoint} недоступен")))
                : inner.PutAsync(endpoint, key, value, lease, ct);

        public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
            => endpoint == badEndpoint
                ? Task.FromResult(Result.Failed(new ApplicationException($"endpoint {endpoint} недоступен")))
                : inner.DeleteAsync(endpoint, keyOrPrefix, prefix, ct);

        public Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct)
            => endpoint == badEndpoint
                ? Task.FromResult(Fail<TxnResult>(endpoint))
                : inner.TxnAsync(endpoint, req, ct);
    }
}
```

(Сверить фактический состав интерфейса `IEtcdGateway` с `src/PgWorker.Etcd/Client/IEtcdGateway.cs` — при дополнительных методах пробросить делегированием.)

- [ ] **Step 2: Прогон — падают**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~WalStatusWriter"
```

- [ ] **Step 3: Реализация**

`src/PgWorker.Backups/WalStatusWriter.cs`:

```csharp
using System.Text.Json;
using System.Text.Json.Serialization;
using PgWorker.Core;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Parsing;

namespace PgWorker.Backups;

/// <summary>Сборка JSON статуса WAL-потока (формат arch/19 §4 — 1:1 с каркасной
/// моделью WalStreamState t01) + put etcd-ключа /pgworker/backups/<C>/<X>/wal
/// ТОЛЬКО при изменении (идемпотентность: безделье не пишет). Оба метода —
/// failover по endpoints (continue-on-failure, итог — последний отказ; образец
/// WorkJournal.WithFailoverAsync). Пишет PgWorker под клэймом <C>; панель читает.</summary>
public sealed class WalStatusWriter(IEtcdGateway etcd, string[] endpoints)
{
    private static readonly JsonSerializerOptions Json = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false,
    };

    public static string ToJson(WalStreamState state) => JsonSerializer.Serialize(
        new WalStatusPayload(
            StateName(state.State), state.Slot, state.MasterNode,
            state.ChainStartSegment, state.LastReceivedSegment,
            state.LastUploadedSegment, state.LastUploadedUnix,
            state.LagSegments, state.Error), Json);

    public async Task<Result<WalStreamState?>> ReadAsync(string cluster, string shard, CancellationToken ct)
    {
        Result<Kv?>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.GetAsync(endpoint, Key(cluster, shard), ct);
            if (!result.IsSuccess)
            {
                last = result; // отказ — пробуем следующий endpoint (failover)
                continue;
            }

            if (result.Value is not { } kv)
                return Result<WalStreamState?>.Success(null);
            return Parse(cluster, shard, kv.Value);
        }

        return Result<WalStreamState?>.Failed(last?.Error
            ?? new ApplicationException("нет живых endpoints etcd"));
    }

    public async Task<Result> WriteIfChangedAsync(
        string cluster, string shard, WalStreamState state, CancellationToken ct)
    {
        var payload = ToJson(state);
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var current = await etcd.GetAsync(endpoint, Key(cluster, shard), ct);
            if (!current.IsSuccess)
            {
                last = current; // отказ — failover на следующий endpoint
                continue;
            }

            if (current.Value is { } kv && kv.Value == payload)
                return Result.Success(); // без изменений — не пишем (частые тики)

            var put = await etcd.PutAsync(endpoint, Key(cluster, shard), payload, lease: null, ct);
            if (!put.IsSuccess)
            {
                last = put; // отказ — failover на следующий endpoint
                continue;
            }

            return Result.Success();
        }

        return Result.Failed(last?.Error ?? new ApplicationException("нет живых endpoints etcd"));
    }

    private static Result<WalStreamState?> Parse(string cluster, string shard, string raw)
    {
        try
        {
            var payload = JsonSerializer.Deserialize<WalStatusPayload>(raw, Json);
            if (payload is null || payload.State is null || payload.LastUploadedUnix is null)
                return Result<WalStreamState?>.Failed(new ApplicationException(
                    $"битый ключ /pgworker/backups/{cluster}/{shard}/wal"));
            return Result<WalStreamState?>.Success(new WalStreamState(
                StateOf(payload.State), payload.Slot ?? "", payload.MasterNode ?? "",
                payload.ChainStartSegment ?? "", payload.LastReceivedSegment ?? "",
                payload.LastUploadedSegment ?? "", payload.LastUploadedUnix,
                payload.LagSegments, payload.Error));
        }
        catch (JsonException e)
        {
            return Result<WalStreamState?>.Failed(new ApplicationException(
                $"битый JSON /pgworker/backups/{cluster}/{shard}/wal: {e.Message}", e));
        }
    }

    private static string Key(string cluster, string shard) => $"/pgworker/backups/{cluster}/{shard}/wal";

    private static string StateName(WalStreamStatus state) => state switch
    {
        WalStreamStatus.Active => "ACTIVE",
        WalStreamStatus.Degraded => "DEGRADED",
        WalStreamStatus.Stopped => "STOPPED",
        _ => "ACTIVE",
    };

    private static WalStreamStatus StateOf(string name) => name switch
    {
        "DEGRADED" => WalStreamStatus.Degraded,
        "STOPPED" => WalStreamStatus.Stopped,
        _ => WalStreamStatus.Active,
    };

    // snake_case-поля ключа (архивный формат §4; писать/читать — только через ToJson/Parse)
    private sealed record WalStatusPayload(
        [property: JsonPropertyName("state")] string? State,
        [property: JsonPropertyName("slot")] string? Slot,
        [property: JsonPropertyName("master_node")] string? MasterNode,
        [property: JsonPropertyName("chain_start_segment")] string? ChainStartSegment,
        [property: JsonPropertyName("last_received_segment")] string? LastReceivedSegment,
        [property: JsonPropertyName("last_uploaded_segment")] string? LastUploadedSegment,
        [property: JsonPropertyName("last_uploaded_unix")] long? LastUploadedUnix,
        [property: JsonPropertyName("lag_segments")] long? LagSegments,
        [property: JsonPropertyName("error")] string? Error);
}
```

- [ ] **Step 4: Прогон — зелёные**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~WalStatusWriter"
```

Ожидание: PASS (включая failover-тест).

- [ ] **Step 5: Commit**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && git add -A && git commit -m "feat(pgworker-backups): WalStatusWriter — ключ wal в формате t01, put при изменении, failover (t03, arch/19 §4)"
```

---

### Task 7: Слот-SQL слой (ensure слота + LSN-зонд)

**Вход:** Task 1.
**Выход:** SQL-слой: идемпотентный ensure слота (immediate+reserved) + LSN/TLI-зонд мастера.
**Проверка:** интеграция `WalSqlTests` PASS против testcontainers-postgres (динамический порт).
**Связь со spec:** §3.2 шаги 3/7 (слот, лаг-зонд), arch/19 §3 (слот создаёт воркер).

**Files:**
- Create: `src/PgWorker.Backups/Sql/IWalSqlExecutor.cs`
- Create: `src/PgWorker.Backups/Sql/NpgsqlWalSqlExecutor.cs`
- Test: `src/tests/PgWorker.IntegrationTests/Backups/WalSqlTests.cs`

**Interfaces:**
- Produces: `PgWorker.Backups.IWalSqlExecutor`:
  - `Task<Result<bool>> SlotExistsAsync(string adminDsn, string slot, CancellationToken ct)`
  - `Task<Result> EnsureSlotAsync(string adminDsn, string slot, CancellationToken ct)` — `pg_create_physical_replication_slot(slot, true)`, идемпотентно.
  - `Task<Result<(string Lsn, int Tli)>> CurrentWalAsync(string adminDsn, CancellationToken ct)`.
  Реализация `NpgsqlWalSqlExecutor` — Npgsql без ретраев (ретраи тиками процесса). Использует задача 9.

- [ ] **Step 1: Интерфейс и реализация**

`src/PgWorker.Backups/Sql/IWalSqlExecutor.cs`:

```csharp
using PgWorker.Core;

namespace PgWorker.Backups.Sql;

/// <summary>SQL-слой WAL-подсистемы к мастеру шарда (Npgsql, admin-DSN — билдер
/// ShardEndpoints.AdminDsn): ensure слота + LSN-зонд лага. Ретраи — тиками
/// процесса (transient), здесь их нет (образец IMoveSqlExecutor).</summary>
public interface IWalSqlExecutor
{
    Task<Result<bool>> SlotExistsAsync(string adminDsn, string slot, CancellationToken ct);

    /// <summary>Идемпотентно: слота нет → pg_create_physical_replication_slot(slot, true)
    /// (immediate reserved, arch/19 §3).</summary>
    Task<Result> EnsureSlotAsync(string adminDsn, string slot, CancellationToken ct);

    /// <summary>Текущая позиция записи мастера: (pg_current_wal_lsn()::text, timeline_id).</summary>
    Task<Result<(string Lsn, int Tli)>> CurrentWalAsync(string adminDsn, CancellationToken ct);
}
```

`src/PgWorker.Backups/Sql/NpgsqlWalSqlExecutor.cs`:

```csharp
using Npgsql;
using PgWorker.Core;

namespace PgWorker.Backups.Sql;

/// <summary>Npgsql-исполнение слот/LSN-SQL (t03): без ретраев — процесс ретраит
/// тиками; ошибка → Result.Failed (transient).</summary>
public sealed class NpgsqlWalSqlExecutor : IWalSqlExecutor
{
    public async Task<Result<bool>> SlotExistsAsync(string adminDsn, string slot, CancellationToken ct)
    {
        try
        {
            await using var connection = new NpgsqlConnection(adminDsn);
            await connection.OpenAsync(ct);
            await using var command = new NpgsqlCommand(
                "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
                connection) { Parameters = { new() { Value = slot } } };
            var exists = (bool)(await command.ExecuteScalarAsync(ct))!;
            return Result<bool>.Success(exists);
        }
        catch (Exception e)
        {
            return Result<bool>.Failed(new ApplicationException($"слот-зонд {slot}: {e.Message}", e));
        }
    }

    public async Task<Result> EnsureSlotAsync(string adminDsn, string slot, CancellationToken ct)
    {
        try
        {
            await using var connection = new NpgsqlConnection(adminDsn);
            await connection.OpenAsync(ct);
            // immediate+reserved: WAL копится на мастере до подтверждения приёма (§3);
            // повтор при живом слоте падает duplicate_object — идемпотентность проверкой выше.
            await using var command = new NpgsqlCommand(
                "SELECT pg_create_physical_replication_slot($1, true)", connection)
            {
                Parameters = { new() { Value = slot } },
            };
            await command.ExecuteNonQueryAsync(ct);
            return Result.Success();
        }
        catch (PostgresException e) when (e.SqlState == "42710") // duplicate_object
        {
            return Result.Success(); // слот уже есть — идемпотентность
        }
        catch (Exception e)
        {
            return Result.Failed(new ApplicationException($"ensure слота {slot}: {e.Message}", e));
        }
    }

    public async Task<Result<(string Lsn, int Tli)>> CurrentWalAsync(string adminDsn, CancellationToken ct)
    {
        try
        {
            await using var connection = new NpgsqlConnection(adminDsn);
            await connection.OpenAsync(ct);
            await using var command = new NpgsqlCommand(
                "SELECT pg_current_wal_lsn()::text, (pg_control_checkpoint()).timeline_id", connection);
            await using var reader = await command.ExecuteReaderAsync(ct);
            await reader.ReadAsync(ct);
            return Result<(string, int)>.Success((reader.GetString(0), reader.GetInt32(1)));
        }
        catch (Exception e)
        {
            return Result<(string, int)>.Failed(new ApplicationException($"LSN-зонд: {e.Message}", e));
        }
    }
}
```

- [ ] **Step 2: Интеграционный тест (postgres testcontainers, AAA)**

`src/tests/PgWorker.IntegrationTests/Backups/WalSqlTests.cs`:

```csharp
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using FluentAssertions;
using PgWorker.Backups.Sql;
using PgWorker.IntegrationTests.Docker;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

// Слот-SQL против живого postgres (testcontainers, динамический порт): идемпотентный
// ensure + LSN-зонд — t03 spec Ф3 (шаги 3/7).
public class WalSqlTests
{
    private const string Password = "pgw-test-su";

    [Fact]
    public async Task EnsureSlot_идемпотентен_CurrentWal_возвращает_lsn_и_tli()
    {
        // Arrange
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = new ContainerBuilder("postgres:17-alpine")
            .WithEnvironment("POSTGRES_PASSWORD", Password)
            .WithPortBinding(5432, assignRandomHostPort: true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilCommandIsCompleted(
                "pg_isready", "-U", "postgres").WithTimeout(TimeSpan.FromSeconds(45)))
            .Build();
        await postgres.StartAsync(ct);
        var dsn = $"Host=localhost;Port={postgres.GetMappedPublicPort(5432)};" +
                  $"Username=postgres;Password={Password};Database=postgres;SSL Mode=Disable";
        var sql = new NpgsqlWalSqlExecutor();

        // Act
        var before = await sql.SlotExistsAsync(dsn, "pgw_bkp_test", ct);
        var created = await sql.EnsureSlotAsync(dsn, "pgw_bkp_test", ct);
        var again = await sql.EnsureSlotAsync(dsn, "pgw_bkp_test", ct);
        var exists = await sql.SlotExistsAsync(dsn, "pgw_bkp_test", ct);
        var wal = await sql.CurrentWalAsync(dsn, ct);

        // Assert
        before.Value.Should().BeFalse();
        created.IsSuccess.Should().BeTrue();
        again.IsSuccess.Should().BeTrue("повтор create при живом слоте — идемпотентность (duplicate_object)");
        exists.Value.Should().BeTrue();
        wal.IsSuccess.Should().BeTrue();
        wal.Value.Lsn.Should().MatchRegex("^[0-9A-F]+/[0-9A-F]+$");
        wal.Value.Tli.Should().BeGreaterThanOrEqualTo(1);
    }
}
```

- [ ] **Step 3: Прогон + зачистка**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~WalSqlTests" && docker rm -f $(docker ps -aq) 2>/dev/null; docker network prune -f
```

Ожидание: PASS.

- [ ] **Step 4: Commit**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && git add -A && git commit -m "feat(pgworker-backups): IWalSqlExecutor — ensure слота immediate+reserved и LSN-зонд (t03, arch/19 §3)"
```

---

### Task 8: Docker-грань агентов (IClusterDriver + имена)

**Вход:** Task 1; существующий `IClusterDriver`/`PlainClusterDriver`/`SwarmClusterDriver`.
**Выход:** 3 метода драйвера для контейнеров агентов (с advertised-fallback как в `EnsureNodeAsync`) + фильтр агентов из `ListNodeObjectsAsync`; счётчик `GetHostsAsync` осознанно НЕ фильтрует агентов (консервативный учёт ресурсов).
**Проверка:** build решения зелёный (все реализации/стабы дополнены); юниты имён PASS; юнит advertised-fallback PASS.
**Связь со spec:** §3.2 шаг 4 (контейнер агента по образцу EnsureNode, без портов), стоп-семантика (арх/19 §3), ограничение «не трогать HA-контур».

**Files:**
- Create: `src/PgWorker.Docker/Drivers/BackupAgentNames.cs`
- Modify: `src/PgWorker.Docker/Drivers/ClusterDriver.cs` (интерфейс + Plain + Swarm + фильтр ListNodeObjects)
- Modify: `src/tests/PgWorker.IntegrationTests/Etcd/StubScaleDriver.cs` (+3 метода-заглушки)
- Modify: `src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs` (+3 метода-заглушки)
- Test: `src/tests/PgWorker.UnitTests/Docker/BackupAgentNamesTests.cs`
- Test: `src/tests/PgWorker.UnitTests/Docker/ClusterDriverTests.cs` (дополнить: advertised-fallback EnsureBackupAgentAsync)

**Interfaces:**
- Produces:
  - `PgWorker.Docker.Drivers.BackupAgentNames` (static): `Container(cluster, shard)` = `pgw-backup-wal-<C>-<X>`, `Volume(cluster, shard)` = `pgw-backup-wal-<C>-<X>-staging`, `Prefix(cluster)` = `pgw-backup-wal-<C>-`.
  - `IClusterDriver.EnsureBackupAgentAsync(string cluster, string shard, ContainerSpec spec, string host, CancellationToken ct)` — resolve engine по хосту с advertised-fallback (как `EnsureNodeAsync`), сеть `pgw-net` + create + start (идемпотентно; 409/304 = успех).
  - `IClusterDriver.RemoveBackupAgentsAsync(string cluster, string? shard, CancellationToken ct)` — stop+rm+volume (404 = успех; shard=null → все агенты кластера).
  - `IClusterDriver.ListBackupAgentsAsync(string cluster, CancellationToken ct)` → `IReadOnlyList<DockerContainer>`.
  Используют задачи 9–11. Побочный инвариант: `ListNodeObjectsAsync` больше НЕ возвращает контейнеры агентов (они не pgw-ноды).

- [ ] **Step 1: Имена + юнит-тест (AAA)**

`src/PgWorker.Docker/Drivers/BackupAgentNames.cs`:

```csharp
namespace PgWorker.Docker.Drivers;

/// <summary>Имена docker-объектов WAL-агентов бэкапов (arch/19 §3): контейнер
/// pgw-backup-wal-&lt;C&gt;-&lt;X&gt; + staging volume. ЕДИНСТВЕННЫЙ источник имён для
/// драйвера (ensure/remove/list) и WalStreamProcess (ContainerSpec).</summary>
public static class BackupAgentNames
{
    public static string Prefix(string cluster) => $"pgw-backup-wal-{cluster}-";

    public static string Container(string cluster, string shard) => $"pgw-backup-wal-{cluster}-{shard}";

    public static string Volume(string cluster, string shard) => $"pgw-backup-wal-{cluster}-{shard}-staging";
}
```

`src/tests/PgWorker.UnitTests/Docker/BackupAgentNamesTests.cs`:

```csharp
using FluentAssertions;
using PgWorker.Docker.Drivers;
using Xunit;

namespace PgWorker.UnitTests.Docker;

// Имена docker-объектов агентов (arch/19 §3) — контракт between драйвера и процесса.
public class BackupAgentNamesTests
{
    [Fact]
    public void Имена_канонические()
    {
        // Arrange / Act / Assert
        BackupAgentNames.Container("shop", "shard1").Should().Be("pgw-backup-wal-shop-shard1");
        BackupAgentNames.Volume("shop", "shard1").Should().Be("pgw-backup-wal-shop-shard1-staging");
        BackupAgentNames.Prefix("shop").Should().Be("pgw-backup-wal-shop-");
    }

    [Fact]
    public void Имена_начинаются_с_pgw_и_отличимы_от_нод()
    {
        // Arrange / Act
        var container = BackupAgentNames.Container("shop", "shard1");

        // Assert — ListNodeObjectsAsync(“pgw-shop-”) фильтрует их (не ноды кластера)
        container.StartsWith("pgw-", StringComparison.Ordinal).Should().BeTrue();
        container.StartsWith("pgw-shop-", StringComparison.Ordinal).Should().BeFalse();
    }
}
```

- [ ] **Step 2: Расширение IClusterDriver**

В `src/PgWorker.Docker/Drivers/ClusterDriver.cs` интерфейс `IClusterDriver` — добавить перед `bool SupportsRunningInspection`:

```csharp
    // ── WAL-агенты бэкапов (arch/19 §3, t03) ──

    // Идемпотентно поднять контейнер агента pgw-backup-wal-<C>-<X> на docker-хосте
    // host: resolve движка с advertised-fallback (как EnsureNodeAsync — single-host
    // advertised-стенды), сеть нод pgw-net (адрес мастера по alias :5432),
    // create + start. spec (ContainerSpec) — от WalStreamProcess (механика агента —
    // код воркера).
    Task<Result> EnsureBackupAgentAsync(
        string cluster, string shard, ContainerSpec spec, string host, CancellationToken ct);

    // Остановить и удалить контейнеры агентов кластера (shard=null → все) + staging
    // volume (404 = успех). Стоп-семантика: Enabled=false/QUARANTINED/remove-shard/D1.
    Task<Result> RemoveBackupAgentsAsync(string cluster, string? shard, CancellationToken ct);

    // Живые контейнеры агентов кластера (состояние running/exited — супервиз).
    Task<Result<IReadOnlyList<DockerContainer>>> ListBackupAgentsAsync(string cluster, CancellationToken ct);
```

- [ ] **Step 3: Реализация в PlainClusterDriver (с advertised-fallback)**

В `PlainClusterDriver` (после `StopNodeAsync`):

```csharp
    // ── WAL-агенты бэкапов (t03, arch/19 §3): те же engine-инстансы, сеть нод ──

    public async Task<Result> EnsureBackupAgentAsync(
        string cluster, string shard, ContainerSpec spec, string host, CancellationToken ct)
    {
        if (!_engines.TryGetValue(host, out var engine))
        {
            // advertised-режим (ревью Ф4-2 №3, образец EnsureNodeAsync): адрес мастера
            // из portalloc несёт advertised-имя, а не ключ движка; валидация старта
            // гарантирует единственный хост — fallback на него.
            if (advertisedHost is not { Length: > 0 } || host != advertisedHost || _engines.Count != 1)
                return Result.Failed(new ApplicationException(
                    $"хост {host} не в таблице Docker:Hosts (агент {cluster}/{shard})"));
            engine = _engines.Values.Single();
        }

        if (!string.Equals(spec.VolumeName, BackupAgentNames.Volume(cluster, shard), StringComparison.Ordinal))
            return Result.Failed(new ApplicationException(
                $"VolumeName спеки агента {cluster}/{shard} обязан быть {BackupAgentNames.Volume(cluster, shard)}"));

        return await Result.FromAsync(async () =>
        {
            // Сеть нод кластера (как EnsureNode): агент видит мастера по alias :5432.
            var network = await engine.EnsureNetworkAsync(NodesNetwork, ct);
            if (!network.IsSuccess)
                throw network.Error!;

            var name = BackupAgentNames.Container(cluster, shard);
            var existing = await engine.ListContainersAsync(name, all: true, ct);
            if (!existing.IsSuccess)
                throw existing.Error!;
            if (existing.Value.FirstOrDefault(c => c.Names.Contains(name)) is not null)
            {
                var started = await engine.StartContainerAsync(name, ct); // 304 = успех
                if (!started.IsSuccess)
                    throw started.Error!;
                return; // контейнер есть — идемпотентность (супервиз процесса решает про пересоздание)
            }

            var created = await engine.CreateContainerAsync(spec, name, ct);
            if (!created.IsSuccess)
                throw created.Error!;
            var up = await engine.StartContainerAsync(name, ct);
            if (!up.IsSuccess)
                throw up.Error!;
        });
    }

    public async Task<Result> RemoveBackupAgentsAsync(string cluster, string? shard, CancellationToken ct)
    {
        return await Result.FromAsync(async () =>
        {
            foreach (var engine in _engines.Values)
            {
                var agents = await ListAgentsOfEngineAsync(engine, cluster, ct);
                foreach (var agent in agents)
                {
                    var agentShard = AgentShardOf(cluster, agent);
                    if (shard is not null && agentShard != shard)
                        continue;
                    var stopped = await engine.StopContainerAsync(agent, timeoutSec: 10, ct);
                    if (!stopped.IsSuccess)
                        throw stopped.Error!;
                    var removed = await engine.RemoveContainerAsync(agent, force: true, ct);
                    if (!removed.IsSuccess)
                        throw removed.Error!;
                    var volume = await engine.RemoveVolumeAsync($"{agent}-staging", ct);
                    if (!volume.IsSuccess)
                        throw volume.Error!;
                }
            }
        });
    }

    public async Task<Result<IReadOnlyList<DockerContainer>>> ListBackupAgentsAsync(
        string cluster, CancellationToken ct)
    {
        var result = new List<DockerContainer>();
        foreach (var engine in _engines.Values)
        foreach (var agent in await ListAgentsOfEngineAsync(engine, cluster, ct))
        {
            var listed = await engine.ListContainersAsync(agent, all: true, ct);
            if (!listed.IsSuccess)
                return Result<IReadOnlyList<DockerContainer>>.Failed(listed.Error!);
            var match = listed.Value.FirstOrDefault(c => c.Names.Contains(agent));
            if (match is not null)
                result.Add(match);
        }

        return Result<IReadOnlyList<DockerContainer>>.Success(result);
    }

    // Имена контейнеров-агентов кластера (Names — с ведущим "/").
    private static async Task<IReadOnlyList<string>> ListAgentsOfEngineAsync(
        IDockerEngine engine, string cluster, CancellationToken ct)
    {
        var listed = await engine.ListContainersAsync(BackupAgentNames.Prefix(cluster), all: true, ct);
        if (!listed.IsSuccess)
            throw listed.Error!;
        return listed.Value
            .SelectMany(c => c.Names)
            .Where(n => n.TrimStart('/').StartsWith(BackupAgentNames.Prefix(cluster), StringComparison.Ordinal))
            .Select(n => n.TrimStart('/'))
            .Distinct()
            .ToList();
    }

    // pgw-backup-wal-<C>-<X>(-staging) → <X>: хвост после префикса без volume-суффикса.
    private static string AgentShardOf(string cluster, string containerName)
    {
        var tail = containerName[BackupAgentNames.Prefix(cluster).Length..];
        return tail.EndsWith("-staging", StringComparison.Ordinal)
            ? tail[..^"-staging".Length]
            : tail;
    }
```

- [ ] **Step 4: Фильтр ListNodeObjectsAsync (Plain) + осознанный НЕ-фильтр GetHostsAsync**

В `PlainClusterDriver.ListNodeObjectsAsync` строку с `names.AddRange(...)` заменить (агенты — НЕ ноды, их жизнью управляет WalStreamProcess/D1):

```csharp
            names.AddRange(containers.Value.SelectMany(c => c.Names)
                .Where(n => n.StartsWith(prefix, StringComparison.Ordinal))
                .Where(n => !n.TrimStart('/').StartsWith("pgw-backup-wal-", StringComparison.Ordinal)));
```

`PlainClusterDriver.GetHostsAsync` (счётчик UsedSlots по `pgw-*`) — ОСОЗНАННО НЕ трогаем: WAL-агент реально потребляет ресурсы хоста (лимиты `Backups:Agent { Cpu, Mem }` → HostConfig), консервативный учёт слота корректен для PlacementPlanner; при переезде мастера агент пересоздаётся на новом хосте и счётчик следует за фактом. Комментарий в `GetHostsAsync` (рядом со строкой `ListContainersAsync("pgw-", ...)`):

```csharp
                // t03: pgw-backup-wal-* (агенты бэкапов) попадают в счётчик СОЗНАТЕЛЬНО —
                // они потребляют ресурсы хоста (лимиты Backups:Agent); фильтруются только
                // из ListNodeObjectsAsync (там семантика «объекты НОД кластера»).
```

- [ ] **Step 5: SwarmClusterDriver + тестовые заглушки**

В `SwarmClusterDriver` (файл тот же `ClusterDriver.cs`) добавить:

```csharp
    // t03: агенты бэкапов — plain-контейнеры; swarm-режим подсистема не поднимает
    // (деплой/стенд/E2E — plain): явный Failed, процесс переведёт шард в journal-заметку.
    public Task<Result> EnsureBackupAgentAsync(
        string cluster, string shard, ContainerSpec spec, string host, CancellationToken ct)
        => Task.FromResult(Result.Failed(new ApplicationException(
            $"WAL-агенты бэкапов в Mode=Swarm не поддерживаются (t03, arch/19 — plain-деплой)")));

    public async Task<Result> RemoveBackupAgentsAsync(string cluster, string? shard, CancellationToken ct)
    {
        return await Result.FromAsync(async () =>
        {
            var services = await _engine.ListServicesAsync(BackupAgentNames.Prefix(cluster), ct);
            if (!services.IsSuccess)
                throw services.Error!;
            foreach (var service in services.Value)
            {
                if (shard is not null && AgentShardOfSwarm(cluster, service) != shard)
                    continue;
                var removed = await _engine.RemoveServiceAsync(service, ct);
                if (!removed.IsSuccess)
                    throw removed.Error!;
            }
        });
    }

    private static string AgentShardOfSwarm(string cluster, string service)
        => service[BackupAgentNames.Prefix(cluster).Length..];

    public Task<Result<IReadOnlyList<DockerContainer>>> ListBackupAgentsAsync(
        string cluster, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<DockerContainer>>.Success(
            (IReadOnlyList<DockerContainer>)[]));
```

В `src/tests/PgWorker.IntegrationTests/Etcd/StubScaleDriver.cs` и `src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs` (фигура фейка — по образцу `StopNodeAsync`) добавить заглушки: список `public readonly List<string> EnsuredBackupAgents = [];` / `RemovedBackupAgents`, методы возвращают `Result.Success()` и фиксируют вызовы; `ListBackupAgentsAsync` возвращает `DockerContainer[]` из мутабельного поля `public List<DockerContainer> BackupAgentObjects = [];` (нужно тестам задач 9–10).

- [ ] **Step 6: Юнит-тест advertised-fallback (AAA) — дополнить ClusterDriverTests**

По образцу существующих тестов файла (там уже есть `FakeEngine : IDockerEngine`, `FakeFactory : DockerEngineFactory` и хелпер `NewPlainDriver(engine, advertisedHost)` — использовать их):

```csharp
[Fact]
public async Task EnsureBackupAgent_advertised_хост_резолвится_в_единственный_движок()
{
    // Arrange — advertised-режим dev-стенда: portalloc несёт advertised-имя,
    // движок один (ревью Ф4-2 №3: без fallback подъём агента в стенде падал бы
    // «хост не в таблице»)
    var engine = new FakeEngine();
    var driver = NewPlainDriver(engine, advertisedHost: "host.docker.internal");
    var spec = new ContainerSpec(
        "pgworker-backup:test", new Dictionary<string, string>(),
        BackupAgentNames.Volume("shop", "shard1"), "/backup-staging",
        [], "pgw-backup-wal-shop-shard1", null, null, "shop");

    // Act
    var ensured = await driver.EnsureBackupAgentAsync(
        "shop", "shard1", spec, "host.docker.internal", ct: TestContext.Current.CancellationToken);

    // Assert
    ensured.IsSuccess.Should().BeTrue();
    engine.CreatedName.Should().Be("pgw-backup-wal-shop-shard1");
    engine.Calls.Should().Contain(c => c.Call == "create").And.Contain(c => c.Call == "start");
}
```

(Сигнатуру `NewPlainDriver`/поля `FakeEngine` сверить с фактическим кодом `ClusterDriverTests`; при отсутствии хелпера с advertisedHost — расширить по месту.)

- [ ] **Step 7: Прогон + Commit**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && dotnet build src/PgWorker.slnx && dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~BackupAgentNames|FullyQualifiedName~ClusterDriverTests" && git add -A && git commit -m "feat(pgworker-docker): методы драйвера WAL-агентов с advertised-fallback + фильтр ListNodeObjects (t03, arch/19 §3)"
```

Ожидание: build зелёный (все реализации интерфейса дополнены), тесты PASS.

---

### Task 9: WalStreamProcess — машина тика (ensure/супервиз агента)

**Вход:** Tasks 2–8 (модели, команда, S3-интерфейс, статус-райтер, SQL-слой, docker-грань).
**Выход:** тиковый процесс: guard'ы → креды → мастер → слот → агент → супервиз (exited/смена мастера); стоп-семантика Enabled=false/QUARANTINED; `DegradeAsync` null-толерантный, маркирует `_chainBroken` и возвращает состояние.
**Проверка:** интеграции `WalStreamProcessTests` (подъём/идемпотентность) PASS на реальном etcd + фейках.
**Связь со spec:** §3.2 (структура/компоненты, шаги 1–5, стоп-семантика), Ф3.

**Files:**
- Create: `src/PgWorker.Backups/WalStreamProcess.cs`
- Create: `src/PgWorker.Backups/BackupNames.cs`
- Test: `src/tests/PgWorker.IntegrationTests/Backups/WalStreamProcessTests.cs`
- Test: `src/tests/PgWorker.IntegrationTests/Backups/FakeBackupDeps.cs`

**Interfaces:**
- Consumes: `WalFileName`/`WalAgentCommand` (Tasks 2, 4), `IBackupS3` (5), `WalStatusWriter` (6), `IWalSqlExecutor` (7), `IClusterDriver.*BackupAgent*` (8), `ShardEndpoints.ResolveMasterAsync`/`ReadPortAllocAsync`, `ClusterBackups` из `PgWorker.Etcd.Parsing`, `ClaimStore.IsMine`, `WorkJournal.WritePhaseAsync`, `BackupsRuntimeOptions` (1).
- Produces (использует Task 12 интеграцией в цикл):
  - `BackupNames.Slot(cluster, shard)` — имя слота с sha1-усечением.
  - `WalStreamProcess` ctor: `(IEtcdGateway etcd, string[] endpoints, IClusterDriver driver, ShardEndpoints shards, IWalSqlExecutor sql, IBackupS3 s3, WalStatusWriter status, ClaimStore claims, WorkJournal journal, Func<BackupsRuntimeOptions?> runtime, InstallSecrets secrets, TimeProvider clock, Action<string, string, long?>? lagObserver = null, ILogger? logger = null)` — `runtime() == null` = подсистема выключена (стоп-семантика).
  - `Task<Result<ProcessOutcome>> TickAsync(ClusterSnapshot snap, ClusterBackups? backups, CancellationToken ct)`.
  - `sealed record ControlOutcome(WalStreamState? State, bool ChainBroken)` — контракт контроля (тело — Task 10): `State` — СВЕЖЕЕ состояние ПОСЛЕ контроля; `ChainBroken=true` — деградация разрыва цепочки/слота (агента НЕ поднимать); transient-деградация (lag/тишина) имеет `ChainBroken=false` — exited-агент ОБЯЗАН пересоздаваться (spec §3.2 п.5/п.7, ревью Ф4-2 №1).
  - op журнала: `"backup-wal"`.

- [ ] **Step 1: BackupNames (слот)**

`src/PgWorker.Backups/BackupNames.cs`:

```csharp
using System.Security.Cryptography;
using System.Text;

namespace PgWorker.Backups;

/// <summary>Имена подсистемы WAL-бэкапов (arch/19 §3): слот pgw_bkp_&lt;C&gt;_&lt;X&gt;;
/// длиннее NAMEDATALEN(63) → pgw_bkp_ + sha1("&lt;C&gt;/&lt;X&gt;")[:16] (усечение без коллизий
/// на практике — канон §3; имя контейнера/volume — PgWorker.Docker.BackupAgentNames).</summary>
public static class BackupNames
{
    public static string Slot(string cluster, string shard)
    {
        var full = $"pgw_bkp_{cluster}_{shard}";
        if (Encoding.ASCII.GetByteCount(full) <= 63)
            return full;
        var hash = Convert.ToHexStringLower(
            SHA1.HashData(Encoding.UTF8.GetBytes($"{cluster}/{shard}")))[..16];
        return $"pgw_bkp_{hash}";
    }
}
```

- [ ] **Step 2: Фейки для интеграционных тестов**

`src/tests/PgWorker.IntegrationTests/Backups/FakeBackupDeps.cs` (фейковый SQL + S3; docker — `StubScaleDriver` из Task 8):

```csharp
using System.Collections.Concurrent;
using PgWorker.Backups;
using PgWorker.Backups.Sql;
using PgWorker.Core;

namespace PgWorker.IntegrationTests.Backups;

// Фейковый SQL-слой: слоты в памяти, LSN управляется тестом (AAA-Act).
public sealed class FakeWalSqlExecutor : IWalSqlExecutor
{
    public ConcurrentDictionary<string, bool> Slots { get; } = new();

    public (string Lsn, int Tli) Current { get; set; } = ("0/1000000", 1);

    public Task<Result<bool>> SlotExistsAsync(string adminDsn, string slot, CancellationToken ct)
        => Task.FromResult(Result<bool>.Success(Slots.TryGetValue(slot, out var alive) && alive));

    public Task<Result> EnsureSlotAsync(string adminDsn, string slot, CancellationToken ct)
    {
        Slots[slot] = true;
        return Task.FromResult(Result.Success());
    }

    public Task<Result<(string Lsn, int Tli)>> CurrentWalAsync(string adminDsn, CancellationToken ct)
        => Task.FromResult(Result<(string, int)>.Success(Current));
}

// Фейковый S3 (поверхность IBackupS3 по spec — только list/exists): объекты в памяти,
// стартовое наполнение — тестом.
public sealed class FakeBackupS3 : IBackupS3
{
    public List<(string Cluster, string Shard, string Name)> Objects { get; } = [];

    public DateTimeOffset LastModified { get; set; } = DateTimeOffset.UtcNow;

    public Task<Result<bool>> BucketExistsAsync(CancellationToken ct)
        => Task.FromResult(Result<bool>.Success(true));

    public Task<Result<IReadOnlyList<WalObject>>> ListWalAsync(
        string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
        => Task.FromResult(Result<IReadOnlyList<WalObject>>.Success(
            (IReadOnlyList<WalObject>)Objects
                .Where(o => o.Cluster == cluster && o.Shard == shard)
                .Select(o => new WalObject(o.Name, LastModified))
                .ToList()));
}
```

- [ ] **Step 3: Тесты скелета (AAA) — подъём/идемпотентность**

`src/tests/PgWorker.IntegrationTests/Backups/WalStreamProcessTests.cs` — фикстура etcd по образцу существующей `src/tests/PgWorker.IntegrationTests/Etcd/EtcdFixture.cs` (тесты в коллекции etcd-фикстуры). Два теста этой задачи:

```csharp
using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Backups.Sql;
using PgWorker.Core.Model;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;
using PgWorker.IntegrationTests.Etcd;
using PgWorker.Provisioning.Endpoints;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

// Интеграции WalStreamProcess (t03 spec Ф3): реальный etcd (статусы/журнал) +
// фейки docker-агентов/SQL/S3. Снапшот кластера строится руками.
[Collection(EtcdCollection.Name)]
public class WalStreamProcessTests(EtcdFixture fixture)
{
    // ... общие хелперы: BuildSnap (Active-кластер c1, shard1 c нодой shard1a,
    // portalloc localhost:16001/16002/16003), BuildProcess(runtime, sql, s3, driver).
    // Полный код хелперов — в шаге 5 реализации (сигнатуры ниже).

    [Fact]
    public async Task Тик_поднимает_агента_ensure_слот_и_ждет_первый_объект()
    {
        // Arrange — полных нет, S3 пуст, ключ wal нет
        // Act — первый тик: слот создан, контейнер агента создан+запущен,
        //       ключ wal НЕ пишется (нет объектов — spec §3.2 шаг 8)
        // Assert — driver.EnsuredBackupAgents содержит pgw-backup-wal-c1-shard1;
        //          sql.Slots содержит pgw_bkp_c1_shard1;
        //          Get(/pgworker/backups/c1/shard1/wal) == null
    }

    [Fact]
    public async Task Тик_идемпотентен_агент_не_пересоздается()
    {
        // Arrange — агент уже создан (первый тик)
        // Act — второй тик
        // Assert — EnsuredBackupAgents без дублей; ключ wal не пишется
    }
}
```

ВНИМАНИЕ: тесты выше — скелет смыслов; при реализации расписать полностью по образцу задач 2–8 (Arrange: сид etcd `/clusters/c1/config` = `{"buckets":2,"dbname":"c1","state":"ACTIVE"}`, `/clusters/c1/shards/shard1/nodes/shard1a/state` = `RUNNING`, `/clusters/c1/backup_password` = `pw`, `/pgworker/portalloc/c1` — JSON c `shard1/shard1a` → `{host:"localhost",pg:16001,patroni:18001,doorman:17001}` (структуру ключа сверить с `Portalloc.Parse` в `src/PgWorker.Provisioning/Endpoints/PortAllocIndex.cs` и fixture-файлом `clusters-full.json`); клэйм через `ClaimStore.TryClaimClusterAsync`). BuildProcess собирает `WalStreamProcess` с `StubScaleDriver` + `FakeWalSqlExecutor` + `FakeBackupS3` + `runtime: () => options`.

- [ ] **Step 4: Реализация WalStreamProcess (каркас + шаги 1–5 spec)**

`src/PgWorker.Backups/WalStreamProcess.cs`:

```csharp
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using PgWorker.Backups.Sql;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Docker.Drivers;
using PgWorker.Docker.Engine;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Processes;

namespace PgWorker.Backups;

/// <summary>Итог контроля одного прохода (Task 10): State — СВЕЖЕЕ состояние шарда
/// ПОСЛЕ контроля (записанное контролем или прежнее при «не время»); ChainBroken —
/// деградация РАЗРЫВА (дыра цепочки/слот): агента не поднимать. Transient-деградация
/// (lag/тишина) держит ChainBroken=false — exited-агент пересоздаётся со свежими
/// env (spec §3.2 п.5/п.7; ревью Ф4-2 №1: guard по «любому DEGRADED» запирал
/// restart-контур навсегда).</summary>
public sealed record ControlOutcome(WalStreamState? State, bool ChainBroken);

/// <summary>WalStreamProcess — машина одного тика WAL-архивации шардов кластера
/// под клэймом <C> (t03, arch/19 §3): (1) креды/ensure-зависимости, (2) резолв
/// мастера, (3) ensure слота, (4) контейнер агента, (5) супервиз (exited/смена
/// мастера → пересоздание), (6–7) контроль цепочки+lag по расписанию
/// Wal:VerifyIntervalSec, (8) статус etcd. runtime() == null (Enabled=false) —
/// стоп-семантика. Ошибка шарда не роняет остальные; guard'ы: Active-кластер,
/// шард с dsn. Идемпотентность каждого шага (arch/17).</summary>
public sealed class WalStreamProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ShardEndpoints shards,
    IWalSqlExecutor sql,
    IBackupS3 s3,
    WalStatusWriter status,
    ClaimStore claims,
    WorkJournal journal,
    Func<BackupsRuntimeOptions?> runtime,
    InstallSecrets secrets,
    TimeProvider clock,
    Action<string, string, long?>? lagObserver = null,
    ILogger? logger = null)
{
    private const string Op = "backup-wal";

    // Расписание контроля (list S3 — не каждый тик): ключ cluster/shard → unix последнего прохода.
    private readonly ConcurrentDictionary<string, long> _lastVerifyUnix = [];

    // Маркер «цепочка разорвана» per-shard (живёт между тиками до успешного контроля
    // — агент не поднимается повторными тиками, пока полный бэкап не сдвинет старт).
    private readonly ConcurrentDictionary<string, bool> _chainBroken = [];

    public async Task<Result<ProcessOutcome>> TickAsync(
        ClusterSnapshot snap, ClusterBackups? backups, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Мутации — только держателем живого клэйма (инвариант arch/14 §4.3).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"backup-wal {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        var options = runtime();

        // Стоп-семантика Enabled=false: агенты вниз + финальный STOPPED при живом
        // ключе; неактивная подсистема дальше не идёт.
        if (options is null)
            return await StopAllAsync(cluster, snap, backups, ct);

        // Guard: только Active-кластер (spec §3.2).
        if (snap.Config.State != ClusterState.Active)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        var shardBackups = backups?.Shards ?? (IReadOnlyDictionary<string, ShardBackups>)new Dictionary<string, ShardBackups>();
        foreach (var shard in snap.Shards)
        {
            if (shard.Dsn is null)
                continue; // не поднят — домен AddShard

            try
            {
                await TickShardAsync(cluster, snap, shard, shardBackups.GetValueOrDefault(shard.Name), options, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                // ошибка шарда не роняет остальные (spec §3.2): журнал + следующий шард
                logger?.LogError(ex, "backup-wal {Cluster}/{Shard}: {Message}", cluster, shard.Name, ex.Message);
                await journal.WritePhaseAsync(cluster, Op, "shard-error", claims.InstanceId,
                    $"{shard.Name}: {ex.Message}", ct);
            }
        }

        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    // ── Шаги одного шарда (spec §3.2 п.1–5) ──

    private async Task TickShardAsync(
        string cluster, ClusterSnapshot snap, ShardSpec shard, ShardBackups? shardBackups,
        BackupsRuntimeOptions options, CancellationToken ct)
    {
        // (1) Креды: /clusters/<C>/backup_password (t02): отсутствует → transient-пропуск
        //     шарда с journal-заметкой — агент не поднимается, ретраи тиками.
        var passwordKv = await GetAsync($"/clusters/{cluster}/backup_password", ct);
        if (passwordKv is not { Value: { Length: > 0 } password })
        {
            await journal.WritePhaseAsync(cluster, Op, "waiting-backup-password", claims.InstanceId,
                $"{shard.Name}: нет /clusters/{cluster}/backup_password (t02 не смержена/ensure не прошёл)", ct);
            return;
        }

        // (2) Резолв мастера: недоступен → transient-пропуск (failover-окно, статус не деградирует).
        var addresses = await shards.ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            throw new ApplicationException($"portalloc: {addresses.Error!.Message}");
        var master = await shards.ResolveMasterAsync(cluster, shard, addresses.Value, ct);
        if (!master.IsSuccess)
            throw new ApplicationException($"резолв мастера: {master.Error!.Message}");
        if (master.Value is not { } masterAddr)
        {
            await journal.WritePhaseAsync(cluster, Op, "waiting-master", claims.InstanceId,
                $"{shard.Name}: мастер не резолвится (failover-окно)", ct);
            return;
        }

        // conninfo-пара для агента + masterRef для сверки смены (arch/19 §3 «Подключение»):
        // каноническая нода — alias сети pgw-net :5432; усыновлённая (object) — host:pg-port.
        var (masterRef, pgHost, pgPort) = ResolveMasterRef(shard, addresses.Value, masterAddr);

        var slot = BackupNames.Slot(cluster, shard.Name);
        var wal = shardBackups?.Wal;
        var chainKnown = wal is not null;
        var adminDsn = ShardEndpoints.AdminDsn(masterAddr, snap.Config.DbName, secrets);

        // Стоп-семантика QUARANTINED (все ноды шарда): агент вниз + STOPPED.
        if (shard.Nodes is { Count: > 0 } && shard.Nodes.All(n => n.State == NodeState.Quarantined))
        {
            await StopShardAsync(cluster, shard.Name, wal, ct);
            return;
        }

        // (3) Ensure слота (spec §3.2 п.3): есть → пропуск; нет при живой записи о
        //     цепочке → инвалидация (DEGRADED + стоп агента — chain-broken); нет и
        //     цепочки нет (первый старт) → create immediate+reserved.
        var slotExists = await sql.SlotExistsAsync(adminDsn, slot, ct);
        if (!slotExists.IsSuccess)
            throw new ApplicationException($"слот-зонд {slot}: {slotExists.Error!.Message}");
        if (!slotExists.Value)
        {
            if (chainKnown && wal!.State != WalStreamStatus.Stopped)
            {
                await DegradeAsync(cluster, shard.Name, wal, slot, masterRef, ct,
                    baseStart: wal.ChainStartSegment, baseLast: wal.LastUploadedSegment,
                    baseUnix: wal.LastUploadedUnix!.Value,
                    error: $"слот {slot} исчез при живой цепочке (инвалидация max_slot_wal_keep_size?) — пересними полный бэкап");
                return;
            }

            var created = await sql.EnsureSlotAsync(adminDsn, slot, ct);
            if (!created.IsSuccess)
                throw new ApplicationException($"ensure слота {slot}: {created.Error!.Message}");
        }

        // (6–7) Контроль по расписанию — ДО ensure агента. ControlOutcome:
        // State — свежее состояние ПОСЛЕ контроля (восстановление ACTIVE новым
        // полным + подъём агента ОДНИМ тиком, AC4c); ChainBroken=true — агента
        // НЕ поднимаем (дыра/слот); transient-DEGRADED (lag/тишина) — НЕ блокирует
        // супервиз (exited-агент пересоздаётся, spec §3.2 п.5; ревью Ф4-2 №1).
        // Тело — Task 10; в этой задаче — временная заглушка.
        var controlled = await ControlDueAsync(
            cluster, shard.Name, wal, shardBackups, options, masterRef, slot, adminDsn, ct);

        // (4–5) Агент + супервиз.
        if (!controlled.ChainBroken)
            await EnsureAgentAsync(cluster, shard.Name, options, slot, masterRef, pgHost, pgPort,
                password, masterAddr.Host, controlled.State, ct);
    }

    // (4–5) Контейнер агента: создание идемпотентно (по образцу EnsureNode, без
    // портов); супервиз: running → пропуск; exited/restarting → пересоздание
    // (restart-луп: устаревшие креды/staging переполнен — включая transient-DEGRADED
    // статуса: деградация тишины НЕ запирает restart-контур, ревью Ф4-2 №1);
    // смена мастера (резолв ≠ master_node статуса) → пересоздание с нового мастера.
    private async Task EnsureAgentAsync(
        string cluster, string shard, BackupsRuntimeOptions options, string slot,
        string masterRef, string pgHost, int pgPort, string password, string agentHost,
        WalStreamState? wal, CancellationToken ct)
    {
        var listed = await driver.ListBackupAgentsAsync(cluster, ct);
        if (!listed.IsSuccess)
            throw new ApplicationException($"лист агентов: {listed.Error!.Message}");
        var agentName = PgWorker.Docker.Drivers.BackupAgentNames.Container(cluster, shard);
        var existing = listed.Value.FirstOrDefault(c => c.Names.Contains("/" + agentName));

        // Смена мастера: резолв разошёлся со статусом → пересоздание с нового мастера.
        var masterChanged = wal is not null && wal.MasterNode != masterRef;
        if (existing is { State: "running" } && !masterChanged)
            return; // жив и на месте — docker unless-stopped держит процесс

        if (existing is not null)
        {
            logger?.LogInformation("backup-wal: агент {Agent} {Reason} — пересоздание",
                agentName, existing.State != "running" ? $"exited({existing.State})" : "смена мастера");
            var removed = await driver.RemoveBackupAgentsAsync(cluster, shard, ct);
            if (!removed.IsSuccess)
                throw new ApplicationException($"демонтаж агента: {removed.Error!.Message}");
        }

        var spec = new ContainerSpec(
            Image: options.AgentImage,
            Env: AgentEnv(options, cluster, shard, slot, pgHost, pgPort, password),
            VolumeName: PgWorker.Docker.Drivers.BackupAgentNames.Volume(cluster, shard),
            VolumeDest: options.StagingDir,
            Ports: [],
            Hostname: agentName,
            CpuCores: options.AgentCpu,
            MemoryBytes: options.AgentMem,
            Label: cluster,
            Cmd: WalAgentCommand.Build(),
            Network: null, // сеть назначает драйвер (pgw-net)
            NetworkAliases: null);

        // Хост агента = docker-хост мастера (per-cluster сеть живёт на нём).
        var ensured = await driver.EnsureBackupAgentAsync(cluster, shard, spec, agentHost, ct);
        if (!ensured.IsSuccess)
            throw new ApplicationException($"подъём агента: {ensured.Error!.Message}");
        logger?.LogInformation("backup-wal: агент {Agent} поднят (слот {Slot}, мастер {Master})",
            agentName, slot, masterRef);
    }

    // env контейнера агента: СЕКРЕТЫ — env, не строка команды (arch/19 §7):
    // PG-параметры по-переменно; S3-креды — ОДНОЙ строкой MC_HOST (mc резолвит
    // alias из env; секреты не попадают в argv процессов — ревью Ф4-2 №5).
    private static IReadOnlyDictionary<string, string> AgentEnv(
        BackupsRuntimeOptions options, string cluster, string shard, string slot,
        string pgHost, int pgPort, string password) => new Dictionary<string, string>
    {
        [WalAgentCommand.EnvMcHostVariable] = WalAgentCommand.McHost(
            options.AgentS3Endpoint, options.S3AccessKey, options.S3SecretKey),
        [WalAgentCommand.EnvS3Bucket] = options.S3Bucket,
        [WalAgentCommand.EnvCluster] = cluster,
        [WalAgentCommand.EnvShard] = shard,
        [WalAgentCommand.EnvSlot] = slot,
        [WalAgentCommand.EnvPgHost] = pgHost,
        [WalAgentCommand.EnvPgPort] = pgPort.ToString(),
        [WalAgentCommand.EnvPgUser] = "backup_exec",
        [WalAgentCommand.EnvPgPassword] = password,
        [WalAgentCommand.EnvPgDbName] = "postgres",
        [WalAgentCommand.EnvStagingDir] = options.StagingDir,
        [WalAgentCommand.EnvStagingQuotaBytes] = options.StagingQuotaBytes?.ToString() ?? "",
        [WalAgentCommand.EnvPollSec] = "5",
    };

    // masterRef + conninfo-пара: каноническая нода → alias :5432 в сети нод;
    // усыновлённая (object) → host:pg-port из portalloc (arch/19 §3 «Подключение»).
    private (string MasterRef, string PgHost, int PgPort) ResolveMasterRef(
        ShardSpec shard, IReadOnlyDictionary<string, NodeAddress> addresses, NodeAddress master)
    {
        foreach (var node in shard.Nodes)
            if (addresses.TryGetValue($"{shard.Name}/{node.Name}", out var addr)
                && addr.Host == master.Host
                && addr.Ports == master.Ports)
                return (node.Name, node.Name, 5432); // alias сети нод
        return (master.Object ?? $"{master.Host}:{master.Ports.Pg}", master.Host, master.Ports.Pg);
    }

    private async Task<WalStreamState?> ReadWalAsync(string cluster, string shard, CancellationToken ct)
    {
        var read = await status.ReadAsync(cluster, shard, ct);
        if (!read.IsSuccess)
            throw new ApplicationException($"чтение ключа wal: {read.Error!.Message}");
        return read.Value;
    }

    // Точечный GET с failover (паттерн ShardEndpoints.GetAsync); ошибка транспорта —
    // исключение (поймается пер-шардовой обёрткой → журнал), null = ключа нет.
    private async Task<Kv?> GetAsync(string key, CancellationToken ct)
    {
        Result<Kv?>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.GetAsync(endpoint, key, ct);
            if (!result.IsSuccess)
            {
                last = result;
                continue;
            }

            return result.Value;
        }

        if (last is not null)
            throw new ApplicationException($"get {key}: {last.Error!.Message}");
        return null;
    }

    // (6–7) Контроль по расписанию — ТЕЛО В TASK 10 (здесь временная заглушка,
    // тесты этой задачи контроль не требуют): возврат — ControlOutcome
    // (заглушка консервативна: состояния нет, цепочка не рвётся — агент работает).
    private Task<ControlOutcome> ControlDueAsync(
        string cluster, string shard, WalStreamState? wal, ShardBackups? shardBackups,
        BackupsRuntimeOptions options, string masterRef, string slot, string adminDsn,
        CancellationToken ct)
        => Task.FromResult(new ControlOutcome(wal, ChainBroken: false));

    // Стоп-семантика Enabled=false (spec §3.2 «Стоп-семантика»): агенты кластера
    // вниз (идемпотентно); при живом ключе шарда — финальный STOPPED (последнее
    // касание, иначе застывший ACTIVE кормит ложный wal-stream-lag).
    private async Task<Result<ProcessOutcome>> StopAllAsync(
        string cluster, ClusterSnapshot snap, ClusterBackups? backups, CancellationToken ct)
    {
        var removed = await driver.RemoveBackupAgentsAsync(cluster, shard: null, ct);
        if (!removed.IsSuccess)
            return Result<ProcessOutcome>.Failed(removed.Error!);

        foreach (var shard in snap.Shards)
        {
            var wal = backups?.Shards.GetValueOrDefault(shard.Name)?.Wal
                      ?? await ReadWalAsync(cluster, shard.Name, ct);
            if (wal is not null && wal.State != WalStreamStatus.Stopped)
                await status.WriteIfChangedAsync(cluster, shard.Name,
                    wal with { State = WalStreamStatus.Stopped }, ct);
        }

        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    // Стоп одного шарда: QUARANTINED (эвакуация) — STOPPED, ключ жив (демонтаж удалит).
    private async Task StopShardAsync(string cluster, string shard, WalStreamState? wal, CancellationToken ct)
    {
        var removed = await driver.RemoveBackupAgentsAsync(cluster, shard, ct);
        if (!removed.IsSuccess)
            throw new ApplicationException($"стоп агента {shard}: {removed.Error!.Message}");
        wal ??= await ReadWalAsync(cluster, shard, ct);
        if (wal is not null && wal.State != WalStreamStatus.Stopped)
            await status.WriteIfChangedAsync(cluster, shard,
                wal with { State = WalStreamStatus.Stopped }, ct);
    }

    // Chain-broken деградация: DEGRADED + error + ОСТАНОВ агента (слот не
    // пересоздаётся — продолжение с дырой бессмысленно; лечение — новый полный
    // t02/t07). Маркирует _chainBroken (повторные тики агента не поднимают).
    // wal == null — запись создаётся с наблюдаемыми фактами контроля (AC4-
    // тотальность: дыра найдена при первом наблюдении цепочки). Возвращает
    // записанное состояние (свежий вердикт для вызова — AC4c одним тиком).
    private async Task<WalStreamState> DegradeAsync(
        string cluster, string shard, WalStreamState? wal, string slot, string masterRef,
        CancellationToken ct,
        string baseStart, string baseLast, long baseUnix, string error)
    {
        logger?.LogError("backup-wal {Cluster}/{Shard}: {Error}", cluster, shard, error);
        _chainBroken[$"{cluster}/{shard}"] = true;
        var removed = await driver.RemoveBackupAgentsAsync(cluster, shard, ct);
        if (!removed.IsSuccess)
            throw new ApplicationException($"стоп агента {shard}: {removed.Error!.Message}");
        var current = wal ?? new WalStreamState(
            WalStreamStatus.Active, slot, masterRef, baseStart, baseLast, baseLast, baseUnix, null, null);
        var degraded = current with { State = WalStreamStatus.Degraded, Error = error };
        await status.WriteIfChangedAsync(cluster, shard, degraded, ct);
        return degraded;
    }
}
```

- [ ] **Step 5: Дописать тесты Task 9 полностью и прогнать**

Дописать оба теста из Step 3 кодом по образцу тестов задач 2–7. Прогон:

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~WalStreamProcessTests" && docker rm -f $(docker ps -aq) 2>/dev/null; docker network prune -f
```

Ожидание: PASS (etcd-фикстура, без docker-контейнеров PG).

- [ ] **Step 6: Commit**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && git add -A && git commit -m "feat(pgworker-backups): WalStreamProcess — тик: креды/мастер/слот/агент/супервиз + стоп-семантика (t03, arch/19 §3)"
```

---

### Task 10: WalStreamProcess — контроль цепочки + lag + статусы

**Вход:** Task 9 (каркас с заглушкой `ControlDueAsync`, `ControlOutcome`, `_chainBroken`, null-толерантный `DegradeAsync`), Tasks 2–3 (имена/цепочка), 5 (list), 7 (LSN).
**Выход:** полное тело контроля: list → chain_start (полный ?? ключ ?? min-объект) → CheckChain (дыра → chain-broken DEGRADED+стоп, даже без прошлого ключа) → факты прогресса только из наблюдений (никакого now()-фабрикования last_uploaded) → lag/тишина (transient-DEGRADED, НЕ блокирует супервиз) → статус; `ControlDueAsync` возвращает `ControlOutcome`.
**Проверка:** интеграции `WalStreamProcessTests` (все контрольные кейсы, AC4/AC5 + transient-пересоздание + факт-над-записью) PASS.
**Связь со spec:** §3.2 шаги 6–8, AC3/AC4/AC5; §2 п.5 «факт над записью»; риски §7 (инвалидация слота, lag, агент-луп).

**Files:**
- Modify: `src/PgWorker.Backups/WalStreamProcess.cs` (реализовать `ControlDueAsync`)
- Test: `src/tests/PgWorker.IntegrationTests/Backups/WalStreamProcessTests.cs` (дополнить)

**Interfaces:**
- Consumes: `WalChain.Check` (Task 3), `BackupS3.ListWalAsync` (5), `CurrentWalAsync`/`FromLsn` (2, 7), `WalStatusWriter` (6), `ShardBackups.Full` (`FullBackupState.WalStartSegment`, статус `Completed`) из `ClusterBackups.Shards` (каркас t01), `DegradeAsync`/`_chainBroken`/`ControlOutcome` из Task 9.
- Produces: `ControlDueAsync` сигнатурой `Task<ControlOutcome> ControlDueAsync(string cluster, string shard, WalStreamState? wal, ShardBackups? shardBackups, BackupsRuntimeOptions options, string masterRef, string slot, string adminDsn, CancellationToken ct)`:
  - «не время» → `ControlOutcome(wal, _chainBroken[key])`;
  - дыра/слот → `ControlOutcome(degraded, ChainBroken: true)` (через `DegradeAsync`);
  - успех → `ControlOutcome(next, ChainBroken: false)` (сбрасывает `_chainBroken`);
  - прогресс не наблюдается и прошлого ключа нет → `ControlOutcome(null, false)` — ключ НЕ пишется;
  - прогресс не наблюдается, прошлое есть → факты от прошлого наблюдения (staleness может выстрелить; now() не подставляется).
  lag-observer вызывается с (cluster, shard, lag).

- [ ] **Step 1: Тесты контроля (AAA) — дописать в WalStreamProcessTests**

```csharp
[Fact]
public async Task Контроль_сплошная_цепочка_пишет_ACTIVE_и_chain_start_от_полного()
{
    // Arrange — в backups-snapshot: full id=20260910120000Z COMPLETED с
    // wal_start_segment=000000010000000000000001; в FakeS3 объекты 1..3
    // (lastModified=now); контрольный проход форсируется (VerifyIntervalSec прошёл —
    // второй тик после первичного, или WalVerifyIntervalSec=0 в тест-опциях)
    // Act — тик
    // Assert — ключ wal: state=ACTIVE, chain_start_segment=<полного>,
    //          last_uploaded=000000010000000000000003, lag_segments посчитан,
    //          lagObserver получил (c1, shard1, lag)
}

[Fact]
public async Task Контроль_дыра_DEGRADED_границы_стоп_и_блокировка_подъема_AC4a()
{
    // Arrange — full COMPLETED wal_start=..01; S3-объекты 1,3 (дыра на 2);
    // агент поднят (первый тик); контроль due
    // Act — тик контроля
    // Assert — ключ wal: state=DEGRADED, error содержит границы дыры
    //          (ожидался ..02, найден ..03); driver.RemovedBackupAgents содержит
    //          шардовый агент; слот НЕ пересоздаётся (sql.Slots жив);
    //          ПОВТОРНЫЙ тик агента не поднимает (ChainBroken=true — ревью Ф4-2 №1)
}

[Fact]
public async Task Контроль_инвалидация_слота_DEGRADED_стоп_агента_AC4b()
{
    // Arrange — ключ wal жив (ACTIVE от прошлого прохода); FakeSql.Slots очищен
    // (слот исчез — max_slot_wal_keep_size исчерпан)
    // Act — тик
    // Assert — DEGRADED с error про слот; агент остановлен
}

[Fact]
public async Task Контроль_новый_полный_выше_дыры_восстанавливает_ACTIVE_тем_же_тиком_AC4c()
{
    // Arrange — DEGRADED (дыра на ..02); появляется full COMPLETED
    // wal_start_segment=000000010000000000000005; S3-объекты 5,6
    // Act — ОДИН тик (контроль due)
    // Assert — state=ACTIVE, chain_start=..05 (свежезаписанное состояние);
    //          агент снова поднят ТЕМ ЖЕ тиком (ControlOutcome.State=ACTIVE,
    //          ChainBroken=false — EnsureAgentAsync увидел свежее, не устаревший
    //          DEGRADED)
}

[Fact]
public async Task Transient_stale_DEGRADED_не_блокирует_пересоздание_агента()
{
    // Arrange — агент exited (StubScaleDriver.BackupAgentObjects: контейнер с
    // State="exited"); цепочка сплошная, но FakeS3.LastModified = now-3600
    // (StaleSec=60) → transient DEGRADED «тишина загрузок»
    // Act — тик контроля
    // Assert — state=DEGRADED (тишина); агент ПЕРЕСОЗДАН (RemovedBackupAgents за
    //          ним следом EnsuredBackupAgents — ревью Ф4-2 №1: transient-деградация
    //          не запирает restart-контур супервиза spec §3.2 п.5)
}

[Fact]
public async Task Контроль_дыра_без_прошлого_ключа_пишет_DEGRADED_AC4_тотальность()
{
    // Arrange — ключа wal нет; full COMPLETED wal_start=..01; S3-объекты 1,3
    // (дыра на 2 при первом наблюдении цепочки)
    // Act — тик контроля
    // Assert — ключ создан: state=DEGRADED, error с границами дыры; агент
    //          не поднимается (ChainBroken)
}

[Fact]
public async Task Контроль_без_сегментных_объектов_ключ_не_пишется_до_первого_наблюдения()
{
    // Arrange — full COMPLETED wal_start=..01 (chain anchored полным); S3 ПУСТ;
    // ключа wal нет
    // Act — тик контроля
    // Assert — ключ НЕ пишется (spec п.8: «ключ не пишется до первого наблюдения»;
    //          ревью Ф4-2 №2: никакого last_uploaded=chain_start, которого не было);
    //          агент при этом работает (поднимается/жив)
}

[Fact]
public async Task Контроль_пропажа_объектов_staleness_от_прошлого_факта_не_от_now()
{
    // Arrange — ключ wal ACTIVE с last_uploaded_unix = now-3600 (StaleSec=60);
    // FakeS3.Objects очищен (объекты пропали: сбой/ретенция)
    // Act — тик контроля
    // Assert — DEGRADED «тишина загрузок»; last_uploaded_unix в ключе ОСТАЛСЯ
    //          прежним (now-3600), не подменён на текущее время (ревью Ф4-2 №2:
    //          «факт над записью», фабрика now() прятала бы мёртвый поток)
}

[Fact]
public async Task Контроль_lag_выше_порога_DEGRADED_AC5a()
{
    // Arrange — цепочка сплошная; FakeSql.Current = ("0/20000000", 1) — сильно
    // выше last_uploaded (порог теста LagMaxSegments=2)
    // Act — тик контроля
    // Assert — state=DEGRADED, error про отставание; агент жив (transient-деградация
    //          не останавливает контейнер — ретраи тиками, восстановление → ACTIVE)
}

[Fact]
public async Task Первый_объект_закрепляет_chain_start_без_полных()
{
    // Arrange — полных нет, ключа нет; S3-объекты 000000010000000000000002..3
    // Act — тик контроля
    // Assert — ключ появился: chain_start=000000010000000000000002 (min-объект,
    //          spec §3.2 п.8), state=ACTIVE (цепочка от min непрерывна)
}
```

Каждый тест дописывается реальным кодом при выполнении (Arrange-хелперы из Task 9).

- [ ] **Step 2: Прогон — новые падают**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~WalStreamProcessTests"
```

- [ ] **Step 3: Реализация ControlDueAsync (полное тело, заменить заглушку Task 9)**

```csharp
    // (6–7) Контроль по расписанию VerifyIntervalSec (spec §3.2 п.6–8): list S3 →
    // chain_start (min COMPLETED-полного ?? закреплённый ?? min-объект) → CheckChain →
    // дыра: chain-broken DEGRADED + ОСТАНОВ агента (в т.ч. без прошлого ключа —
    // AC4-тотальность); факты прогресса — ТОЛЬКО из наблюдений: last_uploaded из
    // S3-объектов либо прошлого ключа, никогда от now() («факт над записью»,
    // ревью Ф4-2 №2); lag-зонд; transient-деградации (lag/тишина) агент не
    // останавливают и супервиз не блокируют (ChainBroken=false).
    private async Task<ControlOutcome> ControlDueAsync(
        string cluster, string shard, WalStreamState? wal, ShardBackups? shardBackups,
        BackupsRuntimeOptions options, string masterRef, string slot, string adminDsn,
        CancellationToken ct)
    {
        var key = $"{cluster}/{shard}";
        var now = clock.GetUtcNow().ToUnixTimeSeconds();
        if (wal is not null && _lastVerifyUnix.TryGetValue(key, out var last)
            && now - last < options.WalVerifyIntervalSec)
            return new ControlOutcome(wal, _chainBroken.TryGetValue(key, out var broken) && broken);
        // (листим S3 и до создания ключа каждый тик — скорость первого наблюдения,
        // пустой префикс дёшев; подтверждено ревью Ф4-2 как допустимое)

        _lastVerifyUnix[key] = now;

        // Истина прогресса — объекты S3 (arch/19 §3): list префикса wal/.
        var listed = await s3.ListWalAsync(cluster, shard, ct: ct);
        if (!listed.IsSuccess)
            throw new ApplicationException($"list S3 {cluster}/{shard}/wal: {listed.Error!.Message}");
        var objects = listed.Value;

        // chain_start (п.8): min wal_start_segment COMPLETED-полных ?? закреплённое
        // из текущего ключа ?? min-объект (первый сегмент потока агента).
        var fromFull = shardBackups?.Full
            .Where(f => f.State == FullBackupStatus.Completed && !string.IsNullOrEmpty(f.WalStartSegment))
            .Select(f => WalFileName.TryParse(f.WalStartSegment))
            .Where(s => s is not null)
            .Select(s => s!.Value)
            .OrderBy(s => s.Name, StringComparer.Ordinal)
            .FirstOrDefault();
        var minObject = objects
            .Select(o => WalFileName.TryParse(o.Name))
            .Where(s => s is not null)
            .Select(s => s!.Value)
            .OrderBy(s => s.Name, StringComparer.Ordinal)
            .FirstOrDefault();
        var chainStart = fromFull
                         ?? (wal is not null ? WalFileName.TryParse(wal.ChainStartSegment) : null)
                         ?? minObject;

        // Нет полных, нет объектов, нет прошлого ключа — писать нечего (п.8):
        // ключ не пишется до первого наблюдения; агент работает (объекты появятся).
        if (chainStart is not { } start)
            return new ControlOutcome(wal, ChainBrokenOf(key));

        var chain = WalChain.Check(start, objects.Select(o => o.Name));
        if (!chain.IsContinuous)
        {
            // Дыра: DEGRADED даже без прошлого ключа (AC4-тотальность) — DegradeAsync
            // строит запись с наблюдаемыми фактами и маркирует _chainBroken.
            var lastForBase = chain.LastSegment?.Name ?? start.Name;
            var lastModifiedForBase = objects
                .Where(o => o.Name == lastForBase)
                .Select(o => o.LastModified)
                .Select(m => (DateTimeOffset?)m)
                .FirstOrDefault() ?? DateTimeOffset.FromUnixTimeSeconds(wal?.LastUploadedUnix ?? now);
            var degraded = await DegradeAsync(cluster, shard, wal, slot, masterRef, ct,
                baseStart: start.Name, baseLast: lastForBase,
                baseUnix: lastModifiedForBase.ToUnixTimeSeconds(),
                error: chain.GapError!);
            return new ControlOutcome(degraded, ChainBroken: true);
        }

        // Факты прогресса — только из наблюдений: наблюдаемый последний сегмент
        // цепочки; если цепочка не наблюдалась вовсе (объектов нет / все ниже
        // chain_start) — прошлый ключ, НИКОГДА не now() (ревью Ф4-2 №2).
        var observedLast = chain.LastSegment;
        var lastUploadedName = observedLast?.Name ?? wal?.LastUploadedSegment;
        var lastUploadedUnix = observedLast is { } seen
            ? objects.Where(o => o.Name == seen.Name).Select(o => o.LastModified).Max().ToUnixTimeSeconds()
            : wal?.LastUploadedUnix;

        // Прогресс не наблюдался и прошлого наблюдения нет — ключ не пишем
        // (spec п.8: «ключ не пишется до первого наблюдения»; писать
        // last_uploaded = chain_start, который не загружался, — подмена факта).
        if (lastUploadedName is null || lastUploadedUnix is null)
            return new ControlOutcome(null, ChainBrokenOf(key));

        // (7) Lag-зонд: pg_current_wal_lsn() мастера → сегмент → дистанция.
        long? lag = null;
        var current = await sql.CurrentWalAsync(adminDsn, ct);
        if (current.IsSuccess && WalFileName.TryParse(lastUploadedName) is { } lastSegment)
        {
            var masterSegment = WalFileName.FromLsn((uint)current.Value.Tli, current.Value.Lsn);
            lag = Math.Max(0, lastSegment.DistanceTo(masterSegment));
        }

        lagObserver?.Invoke(cluster, shard, lag);

        // Деградации transient-природы (lag/тишина): агент НЕ останавливаем и
        // супервиз НЕ блокируем (ChainBroken=false — ретраи тиками; exited-агент
        // пересоздаётся со свежими env, spec §3.2 п.5/п.7; ревью Ф4-2 №1).
        string? error = null;
        var state = WalStreamStatus.Active;
        if (lag is > options.WalLagMaxSegments)
        {
            state = WalStreamStatus.Degraded;
            error = $"отставание WAL-потока {lag} сегментов (порог {options.WalLagMaxSegments})";
        }
        else if (now - lastUploadedUnix > options.WalStaleSec)
        {
            state = WalStreamStatus.Degraded;
            error = $"тишина загрузок {now - lastUploadedUnix} c (порог {options.WalStaleSec})";
        }

        var next = new WalStreamState(
            state, slot, masterRef, start.Name, lastUploadedName, lastUploadedName,
            lastUploadedUnix, lag, error);
        await status.WriteIfChangedAsync(cluster, shard, next, ct);
        _chainBroken[key] = false; // цепочка цела — супервиз разрешён
        return new ControlOutcome(next, ChainBroken: false);
    }

    // Текущий вердикт разрыва для «не время»-веток (грязное чтение словаря — ок:
    // пишет только этот же процесс под клэймом).
    private bool ChainBrokenOf(string key) => _chainBroken.TryGetValue(key, out var broken) && broken;
```

Замечания исполнителю:
1. Отказ LSN-зонда (transient) — не деградация: `lag=null`, статус без поля.
2. `lastUploadedUnix` при наблюдаемом сегменте — `Max()` без `DefaultIfEmpty`: сегмент пришёл из list-результатов, объект существует.
3. Ветка дыры: `lastModifiedForBase` — от объекта `lastForBase`, если он есть; иначе от прошлого ключа; `?? now` — только вырожденный случай «ни объекта, ни ключа» (DegradeAsync при `wal == null` требует хоть какого-то unix; дыра без единого объекта практически недостижима — chainStart тогда не из объектов).

- [ ] **Step 4: Прогон — все зелёные**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~WalStreamProcessTests" && docker rm -f $(docker ps -aq) 2>/dev/null; docker network prune -f
```

Ожидание: PASS (все тесты задач 9–10: AC4a-блокировка повторным тиком, AC4c «тем же тиком», AC4-тотальность, transient-пересоздание, «ключ не пишется без наблюдения», «staleness от прошлого факта»).

- [ ] **Step 5: Commit**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && git add -A && git commit -m "feat(pgworker-backups): контроль цепочки + lag: chain-broken vs transient, факты без now()-фабрики (t03, arch/19 §3)"
```

---

### Task 11: Стоп-семантика демонтажа (S-процесс, D1/D2)

**Вход:** Task 8 (методы драйвера); существующие `RemoveShardProcess`/`DeprovisioningProcess`.
**Выход:** remove-shard и deprovisioning останавливают/удаляют контейнеры агентов И чистят etcd-ключи бэкапов шарда/кластера (S-CleanKeys + D2) — без сирот.
**Проверка:** контрактные интеграции (`ShardScaleContractTests` новый кейс + `EtcdContractTests` не сломаны) PASS.
**Связь со spec:** §3.2 «Стоп-семантика» (remove-shard S-процессы, deprovisioning D1, D2 `del --prefix`), AC6.

**Files:**
- Modify: `src/PgWorker.Provisioning/Processes/RemoveShardProcess.cs` (S1.5-агент + чистка ключа wal шарда в CleanKeysAsync)
- Modify: `src/PgWorker.Provisioning/Processes/DeprovisioningProcess.cs` (D1: агенты кластера; D2: префикс `/pgworker/backups/<C>/`)
- Test: `src/tests/PgWorker.IntegrationTests/Etcd/ShardScaleContractTests.cs` (дополнить)

**Interfaces:**
- Consumes: `IClusterDriver.RemoveBackupAgentsAsync` (Task 8).
- Produces: AC6 — deprovisioning/remove-shard останавливают и удаляют контейнеры агентов без сирот; S-чистка удаляет `/pgworker/backups/<C>/<X>/` (ключ wal шарда не переживает демонтаж); D2 чистит `/pgworker/backups/<C>/`.

- [ ] **Step 1: Тест (AAA) — дополнить ShardScaleContractTests**

```csharp
[Fact]
public async Task RemoveShard_останавливает_агента_WAL_и_чистит_ключ_бэкапов_AC6()
{
    // Arrange — кластер Active, shard1 с TO_REMOVE-маркером; StubScaleDriver
    // видит объекты нод + агент (поле BackupAgentObjects — имитация живого агента);
    // etcd-ключ /pgworker/backups/<C>/shard1/wal жив (посади напрямую)
    // Act — тик RemoveShardProcess
    // Assert — драйвер зафиксировал RemovedBackupAgents("shard1"); docker-объектов
    //          шарда нет; ключ /pgworker/backups/<C>/shard1/wal УДАЛЁН (чистка
    //          CleanKeysAsync шарда — без неё ключ пережил бы демонтаж)
}
```

(Код дописывается исполнителем по существующим паттернам файла.)

- [ ] **Step 2: Прогоны — падает (агент не снесён, ключ жив)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~ShardScaleContractTests"
```

- [ ] **Step 3: RemoveShardProcess — стоп агента (S1.5)**

В `RemoveShardProcess.cs` в `TickAsync` между G5-guard'ами и S2 (`// S2: REMOVING → RemoveNode...`) вставить:

```csharp
        // S1.5 (t03, arch/19 §3 «Стоп-семантика»): агент WAL шарда — вниз ДО нод
        // (идемпотентно по имени); ключ wal /pgworker/backups/<C>/<X>/ удаляет
        // CleanKeysAsync (шаг 4) вместе с демонтажом etcd-ключей шарда.
        var agent = await driver.RemoveBackupAgentsAsync(cluster, shardName, ct);
        if (!agent.IsSuccess)
            return await FailAsync(cluster, agent.Error!, "removing-agent", ct);
```

- [ ] **Step 4: RemoveShardProcess — чистка ключа wal шарда (CleanKeysAsync)**

В `RemoveShardProcess.CleanKeysAsync` (метод `// S3: чистка etcd — всё про шард`) — сразу после `delScope` (удаление `/service/{scope}/`) добавить:

```csharp
        // t03 (arch/19 §4): ключи бэкапов шарда (wal-статус; full-ключи — t02)
        // не переживают демонтаж — S-аналог D2-чистки deprovisioning.
        var delBackups = await DeleteAsync($"/pgworker/backups/{cluster}/{shardName}/", prefix: true, ct);
        if (!delBackups.IsSuccess)
            return delBackups;
```

- [ ] **Step 5: DeprovisioningProcess — D1-агенты + D2-префикс**

В `DeprovisioningProcess.cs` в `TickAsync` сразу после D0 (`started`) и перед `RemoveNodesAsync`:

```csharp
        // D1' (t03, arch/19 §3): WAL-агенты бэкапов кластера — вниз ДО нод (idempotent
        // по префиксу pgw-backup-wal-<C>-); etcd-ключи чистит D2 ниже.
        var agents = await driver.RemoveBackupAgentsAsync(cluster, shard: null, ct);
        if (!agents.IsSuccess)
            return await FailAsync(cluster, agents.Error!, "removing-agents", ct);
```

В `DeprovisioningProcess.CleanKeysAsync` (D2-список префиксов — рядом с `/pgworker/portalloc` и `/pgworker/work`) добавить (сверить фактический хелпер удаления префикса в файле и использовать его стиль):

```csharp
        // t03 (arch/19 §4): статусы бэкапов кластера (S3-объекты НЕ трогаем — R4).
        var backups = await DeleteAsync($"/pgworker/backups/{cluster}/", prefix: true, ct);
        if (!backups.IsSuccess)
            return backups;
```

- [ ] **Step 6: Прогоны — зелёные + существующие контракты не сломаны**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~ShardScaleContractTests|FullyQualifiedName~EtcdContractTests" && docker rm -f $(docker ps -aq) 2>/dev/null; docker network prune -f
```

- [ ] **Step 7: Commit**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && git add -A && git commit -m "feat(pgworker): стоп-семантика демонтажа — WAL-агенты и ключи бэкапов в remove-shard (S) и deprovisioning (D1/D2) (t03, arch/19 §3/§4)"
```

---

### Task 12: Интеграция в ReconcileLoop + DI + метрика

**Вход:** Tasks 9–10 (`WalStreamProcess` готов).
**Выход:** вызов в Active-ветке `ReconcileLoop` (после `rotate-app-password`, до `repair`), чтение префикса `/pgworker/backups/` каркасным `BackupsParser`, DI-регистрации, метрика `pgworker_backup_wal_lag_segments`.
**Проверка:** build зелёный; юниты ReconcileLoop (порядок вызовов) и метрик PASS.
**Связь со spec:** §3.2 («Вызов — из ClusterProcesses/ReconcileLoop в Active-ветке, после rotate-app-password, до repair/moves»), §3.5 (метрика), ограничение «BackupsParser подключается к потреблению в воркере».

**Files:**
- Modify: `src/PgWorker.App/Loops/ClusterProcesses.cs` (+`WalStreamAsync`)
- Modify: `src/PgWorker.App/Loops/ReconcileLoop.cs` (чтение `/pgworker/backups/` + вызов)
- Modify: `src/PgWorker.App/Program.cs` (DI)
- Modify: `src/Shared.Metrics/Worker/WorkerMetricsInstrumentation.cs` (+gauge)
- Test: `src/tests/PgWorker.UnitTests/App/ReconcileLoopTests.cs` (дополнить FakeProcesses)
- Test: `src/tests/Shared.Metrics.UnitTests/` (дополнить по образцу существующих)

**Interfaces:**
- Consumes: `WalStreamProcess` (9–10), `BackupsParser.Parse` (каркас t01, `PgWorker.Etcd.Parsing`).
- Produces: `IClusterProcesses.WalStreamAsync(ClusterSnapshot snap, ClusterBackups? backups, CancellationToken ct)`; метрика gauge `pgworker_backup_wal_lag_segments{cluster,shard}` + публичный Mark-метод `BackupWalLag(cluster, shard, long? segments)`.

- [ ] **Step 1: Метрика**

В `WorkerMetricsInstrumentation` по образцу `worker.claims_held` (ObservableGauge + Mark-метод):

```csharp
        meter.CreateObservableGauge(
            "pgworker_backup_wal_lag_segments",
            () => Measure(() => _walLag.Select(kv =>
                new Measurement<long>(kv.Value,
                    new KeyValuePair<string, object?>("cluster", kv.Key.Cluster),
                    new KeyValuePair<string, object?>("shard", kv.Key.Shard)))),
            unit: "{segment}", description: "Отставание WAL-потока бэкапов, сегментов (arch/19 §3)");
```

Стейт: `private readonly Dictionary<(string Cluster, string Shard), long> _walLag = new();` + публичный метод (внутри `lock (_lock)`, try/catch-пассивность как у соседей):

```csharp
    // Gauge pgworker_backup_wal_lag_segments{cluster,shard}: лаг WAL-потока шарда
    // (наблюдение контрольного прохода WalStreamProcess; null — серия исчезает).
    public void BackupWalLag(string cluster, string shard, long? segments)
    {
        try
        {
            lock (_lock)
            {
                if (segments is { } value)
                    _walLag[(cluster, shard)] = value;
                else
                    _walLag.Remove((cluster, shard));
            }
        }
        catch
        {
            // Пассивный наблюдатель.
        }
    }
```

- [ ] **Step 2: ClusterProcesses + ReconcileLoop**

В `IClusterProcesses` после `RotateAppPasswordAsync`:

```csharp
    /// <summary>WAL-архивация шардов (t03, arch/19 §3): ensure слота/агента,
    /// контроль цепочки по расписанию, статус /pgworker/backups/<C>/<X>/wal.
    /// backups — парс префикса /pgworker/backups/ этим же тиком.</summary>
    Task<Result<ProcessOutcome>> WalStreamAsync(ClusterSnapshot snap, ClusterBackups? backups, CancellationToken ct);
```

Реализация в `ClusterProcesses` (ctor: +`WalStreamProcess walStream`): `=> walStream.TickAsync(snap, backups, ct);`

В `ReconcileLoop.TickAsync` — третье чтение после `/service/`:

```csharp
        // t03 (arch/19 §4): статусы бэкапов — префикс читаем всегда (стоп-семантика
        // Enabled=false обязана видеть живые ключи); пустой префикс — дёшево.
        var backupsKvs = await RangeWithFailoverAsync(endpoints, "/pgworker/backups/", ct);
        if (!backupsKvs.IsSuccess)
            return Result.Failed(backupsKvs.Error!);
        var backupsParsed = BackupsParser.Parse(backupsKvs.Value, out var backupsErrors);
        foreach (var error in backupsErrors)
            logger.LogWarning("пропущен битый ключ бэкапов: {Error}", error);
```

Передать `backupsParsed.Value` в `ProcessClusterAsync` (параметр `IReadOnlyList<ClusterBackups> backups`) → в Active-ветке между `rotate-app-password` и `repair`:

```csharp
                    // WAL-архивация (t03, arch/19 §3): после ротации (креды агента —
                    // свежий пароль в пересоздании) и до repair/moves (короткая).
                    var clusterBackups = backups.FirstOrDefault(b => b.Cluster == cluster);
                    await RunClusterOpAsync(cluster, "backup-wal",
                        () => processes.WalStreamAsync(snap, clusterBackups, ct), ct);
```

- [ ] **Step 3: DI в Program.cs**

По образцу `MoveProcess` (после `ClusterSecretRotator`):

```csharp
// WAL-архивация (t03, arch/19 §3): слот/агент/контроль цепочки; runtime()==null
// (Backups:Enabled=false) — процесс выполняет стоп-семантику и не активен.
builder.Services.AddSingleton(sp => new WalStreamProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    opts.Etcd.Endpoints, /* см. примечание */
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ShardEndpoints>(),
    sp.GetRequiredService<IWalSqlExecutor>(),
    sp.GetRequiredService<IBackupS3>(),
    new WalStatusWriter(sp.GetRequiredService<IEtcdGateway>(), opts.Etcd.Endpoints),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    () => sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>().CurrentValue.Backups.Enabled
        ? sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>().CurrentValue.Backups.ToRuntime()
        : null,
    sp.GetRequiredService<InstallSecrets>(),
    sp.GetRequiredService<TimeProvider>(),
    m.BackupWalLag,
    sp.GetRequiredService<ILoggerFactory>().CreateLogger("WalStreamProcess")));
builder.Services.AddSingleton<IWalSqlExecutor, NpgsqlWalSqlExecutor>();
builder.Services.AddSingleton<IBackupS3>(sp =>
{
    var backups = sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>().CurrentValue.Backups;
    return backups.Enabled ? new BackupS3(backups.ToRuntime()) : NullBackupS3.Instance;
});
```

Примечания исполнителю: (а) `opts` — внутри лямбды из `IOptionsMonitor` (сверить фактический паттерн соседних registrations в `Program.cs`); (б) `m` — инстанс `WorkerMetricsInstrumentation` из блока метрик выше (сохранить в переменную при создании, как уже сделано для `PhaseWritten`); (в) `NullBackupS3` — приватный класс-заглушка в `Program.cs` (нижний колонтитул файла), реализует `IBackupS3` методами `Result.Failed(ApplicationException("Backups:Enabled=false"))` — подсистема выключена, list не зовётся; (г) порядок аргументов ctor сверить с реализацией Task 9.

- [ ] **Step 4: Тесты цикла (AAA)**

Дополнить `ReconcileLoopTests.FakeProcesses` заглушкой `WalStreamAsync` → `Result<ProcessOutcome>.Success(ProcessOutcome.Done)` (компиляция); в существующих тестах тика — проверить порядок вызовов (если FakeProcesses фиксирует последовательность ops — добавить `"backup-wal"` после `rotate-app-password`, до `repair`). Юнит-метрики: дополнить тест-класс `WorkerMetricsInstrumentation`-тестов (Shared.Metrics.UnitTests) кейсом: два наблюдения `BackupWalLag("c1","s1", 5)` / `("c1","s2", 0)` → gauge-серии 2 с тегами cluster/shard; `null` — серия удаляется.

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && dotnet build src/PgWorker.slnx && dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~ReconcileLoop" && dotnet test src/tests/Shared.Metrics.UnitTests
```

Ожидание: build зелёный, тесты PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && git add -A && git commit -m "feat(pgworker): WalStreamProcess в Active-ветке ReconcileLoop + DI + метрика wal_lag (t03, arch/19 §3)"
```

---

### Task 13: AdminPanel — чтение префикса + 3 правила алертов

**Вход:** Tasks 1–12 (воркер пишет ключи wal); панельный каркас (SnapshotRefresher/SnapshotBuilder/IAlertRule).
**Выход:** панель читает `/pgworker/backups/` в снапшот (толерантный парсер) + 3 правила алертов (wal-chain-broken/wal-stream-lag/wal-stream-stopped) + пороги AlertsOptions.
**Проверка:** build зелёный; юниты панели (парсер + правила) PASS, существующие тесты не сломаны.
**Связь со spec:** §3.3 (панельные правила, парсер по образцу воркерного), AC5 (панельная часть).

**Files:**
- Create: `src/AdminPanel.Core/BackupsInfo.cs` (панельная модель)
- Create: `src/AdminPanel.Etcd/Parsing/BackupsParser.cs`
- Modify: `src/AdminPanel.Core/EtcdSnapshot.cs` (+поле Backups)
- Modify: `src/AdminPanel.Etcd/SnapshotRefresher.cs` (префикс+чтение)
- Modify: `src/AdminPanel.Etcd/SnapshotBuilder.cs` (параметр)
- Modify: `src/AdminPanel.Core/Alerting/AlertsOptions.cs` (+2 порога)
- Create: `src/AdminPanel.Core/Alerting/Rules/WalChainBrokenRule.cs`
- Create: `src/AdminPanel.Core/Alerting/Rules/WalStreamLagRule.cs`
- Create: `src/AdminPanel.Core/Alerting/Rules/WalStreamStoppedRule.cs`
- Test: `src/tests/AdminPanel.UnitTests/` — парсер + правила (по образцу существующих тестов правил)

**Interfaces:**
- Consumes: префикс `/pgworker/backups/` (воркер t01/t03 пишет), паттерн `IAlertRule` + `[InjectAsSingleton(typeof(IAlertRule))]` (автоскан правил), `AlertsOptions`-биндинг.
- Produces:
  - `AdminPanel.Core.BackupsInfo`: `record WalStreamInfo(string Cluster, string Shard, WalStreamInfoState State, string Slot, string MasterNode, long LastUploadedUnix, long? LagSegments, string? Error)` + enum `WalStreamInfoState { Active, Degraded, Stopped }`; `record ClusterBackupsInfo(string Cluster, IReadOnlyDictionary<string, WalStreamInfo?> Shards)`.
  - `EtcdSnapshot` новое поле `IReadOnlyList<ClusterBackupsInfo> Backups` (позиционно после `PgWorkerWork`).
  - AlertsOptions: `WalLagMaxSegments` (default 1024), `WalStaleSec` (default 300) — синхронизированы с воркерными дефолтами.
  - Правила: `wal-chain-broken` (critical), `wal-stream-lag` (warning), `wal-stream-stopped` (warning).

- [ ] **Step 1: Модель + парсер панели**

`src/AdminPanel.Core/BackupsInfo.cs`:

```csharp
namespace AdminPanel.Core;

// Панельная модель подсистемы бэкапов (чтение /pgworker/backups/, arch/19 §4;
// adminpanel/02 §2.3.1): только поля статусов WAL — политика/полные панели в t03
// не нужны (UI бэкапов — t08). Дубли с воркерной моделью — осознанные (roadmap t08).
public enum WalStreamInfoState { Active, Degraded, Stopped }

/// <summary>WAL-поток шарда из ключа /pgworker/backups/<C>/<X>/wal.</summary>
public sealed record WalStreamInfo(
    string Cluster, string Shard, WalStreamInfoState State,
    string Slot, string MasterNode, long LastUploadedUnix,
    long? LagSegments, string? Error);

public sealed record ClusterBackupsInfo(
    string Cluster, IReadOnlyDictionary<string, WalStreamInfo?> Shards);
```

`src/AdminPanel.Etcd/Parsing/BackupsParser.cs` — панельный парсер по образцу воркерного (`PgWorker.Etcd/Parsing/BackupsParser.cs`), толерантный (битые → errors, не исключение): статический `Parse(IReadOnlyList<Kv> kvs)` → `BackupsParseResult(IReadOnlyList<ClusterBackupsInfo> Clusters, IReadOnlyList<string> Errors)`; разбирает ТОЛЬКО ключи `case 6 … segments[5] == "wal"` (формат полей — как у воркерного TryParseWal: state/slot/master_node/last_uploaded_unix обязательны, lag_segments/error опциональны; незнакомое state → пропуск записи с ошибкой).

- [ ] **Step 2: Снапшот**

`EtcdSnapshot` — добавить параметр `IReadOnlyList<ClusterBackupsInfo> Backups` после `PgWorkerWork` (правка затронет все конструкторы: `SnapshotBuilder.Build` — новый параметр `BackupsParseResult backups` и позиция; `SnapshotRefresher.FailTick` — `previous?.Backups ?? []` в той же позиции; `SnapshotRefresher.RefreshOnceAsync` — чтение префикса):

```csharp
        var backupsTask = WithFailoverAsync(alive, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.Backups, t), ct);
        // … в общий провал-чек: || !backupsKv.IsSuccess
        var backupsParsed = BackupsParser.Parse(backupsKv.Value);
```

`Prefixes`: `public const string Backups = "/pgworker/backups/";`

- [ ] **Step 3: AlertsOptions**

```csharp
    // wal-stream-lag: порог лага в сегментах и порог тишины загрузок (arch/19 §3;
    // синхронизированы с воркерными Wal:LagMaxSegments/StaleSec — дефолты 1024/300).
    public int WalLagMaxSegments { get; set; } = 1024;

    public int WalStaleSec { get; set; } = 300;
```

- [ ] **Step 4: Три правила (по образцу MoveStaleRule — AAA)**

`WalChainBrokenRule.cs`:

```csharp
using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Core.Alerting.Rules;

// wal-chain-broken (critical, t03, arch/19 §3/§8): DEGRADED WAL-поток шарда живого
// Active-кластера — дыра цепочки/инвалидация слота; текст — error статуса.
// transient-деградации (lag/тишина) тоже DEGRADED у воркера — различает текст
// error статуса (дыра/слот vs отставание/тишина); правило едино для DEGRADED,
// критичность оправдана: восстановление только новым полным (t02/t07) либо
// самооздоровлением потока (transient) — оператор смотрит error.
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class WalChainBrokenRule : IAlertRule
{
    public const string KindName = "wal-chain-broken";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var backups in snapshot.Backups)
        foreach (var (shard, wal) in backups.Shards)
        {
            if (wal is not { State: WalStreamInfoState.Degraded })
                continue;

            // только живой Active-кластер: демонтаж сам останавливает поток
            var cluster = snapshot.Clusters.FirstOrDefault(
                c => c.Name == backups.Cluster && c.State == ClusterState.Active
                     && c.Shards.Any(s => s.Name == shard));
            if (cluster is null)
                continue;

            yield return new Alert(
                $"{KindName}:{backups.Cluster}/{shard}",
                AlertSeverity.Critical,
                KindName,
                $"{backups.Cluster}/{shard}",
                $"WAL-цепочка шарда {shard} кластера {backups.Cluster} в DEGRADED: {wal.Error ?? "без причины"}",
                new Dictionary<string, string>
                {
                    ["state"] = "DEGRADED",
                    ["slot"] = wal.Slot,
                    ["error"] = wal.Error ?? "",
                },
                null,
                "разбери error статуса: дыра цепочки/слот — переснять полный бэкап (PgWorker, runbook arch/19 §3: нужен полный с wal_start_segment выше дыры); отставание/тишина — проверь контейнер pgw-backup-wal-<C>-<X> и S3-доступность (transient, воркер ретраит)",
                AlertRemedy.WorkerAuto,
                "дыра/слот: WAL-агент остановлен воркером, восстановление — новый полный (t02)/reconcile (t07); lag/тишина: self-healing тиками");
        }
    }
}
```

`WalStreamLagRule.cs` — warning, `state == Active` и (`LagSegments >= options.Value.WalLagMaxSegments` ИЛИ `now - LastUploadedUnix > options.Value.WalStaleSec`), тот же guard живого Active-кластера, kind `wal-stream-lag`, remedy: «проверь контейнер pgw-backup-wal-<C>-<X> и S3-доступность; воркер ретраит тиками (transient)».

`WalStreamStoppedRule.cs` — warning, `state == Stopped` при живом шарде Active-кластера (кластер в снапшоте со стейтом Active и шард есть), kind `wal-stream-stopped`, remedy: «если остановка не операционная (Enabled=false/QUARANTINED) — перезапусти подсистему/разбери журналы /pgworker/work/<C>».

Сигнатуру `Alert`-record и enum `AlertRemedy`/`AlertSeverity` сверить с `src/AdminPanel.Core/Alert.cs` и соседними правилами (копировать структуру аргументов 1:1 из `MoveStaleRule`; если `AlertRemedy.Manual` не существует — взять фактическое значение из `Alert.cs` для ручного ремеди).

- [ ] **Step 5: Тесты (AAA) — юниты правил и парсера**

В `src/tests/AdminPanel.UnitTests/` по образцу существующих тестов правил (найти `MoveStaleRule`-тесты): снапшоты с `Backups = [ClusterBackupsInfo("c1", {"shard1": WalStreamInfo(...)})]`:

- `wal-chain-broken`: DEGRADED у живого Active-кластера → critical-алерт с error; DEGRADED у ToRemove-кластера → нет алерта.
- `wal-stream-lag`: ACTIVE + LagSegments=5000 (порог 1024) → warning; ACTIVE + свежий + малый лаг → нет; ACTIVE + LastUploadedUnix = now-3600 (StaleSec=300) → warning.
- `wal-stream-stopped`: STOPPED у живого шарда → warning; STOPPED у несуществующего шарда → нет.
- Парсер: валидный ключ wal → модель; битый JSON → errors без исключения; ключ full → игнор.

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && dotnet build src/PgWorker.slnx && dotnet test src/tests/AdminPanel.UnitTests
```

Ожидание: build зелёный, все тесты PASS (включая существующие — конструктор EtcdSnapshot изменён, тесты панели могли строить снапшот позиционно: дополнить `[]` в нужной позиции).

- [ ] **Step 6: Commit**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && git add -A && git commit -m "feat(adminpanel): чтение /pgworker/backups/ + правила wal-chain-broken/wal-stream-lag/wal-stream-stopped (t03, arch/19 §8)"
```

---

### Task 14: E2E-маркер WalStream_UploadsSegmentsContinuously

**Вход:** Tasks 1–13 (подсистема в цикле воркера); E2eFixture; образ `pgworker-backup` из t02 (`docker/pgworker-backup.Dockerfile` — до мержа t02 отсутствует → явный skip).
**Выход:** живой docker-E2E: кластер + MinIO + INSERT/`pg_switch_wal`-нагрузка → сегменты в MinIO, ключ wal ACTIVE, цепочка непрерывна, агент running, слот создан.
**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~WalStream_UploadsSegmentsContinuously` — PASS (или легитимный skip до t02) + зачистка.
**Связь со spec:** Ф5, AC1/AC2/AC7; §5 (короткие WAL-бюджеты: нагрузка, не таймауты).

**Files:**
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eFixture.cs` (+MinIO-контейнер, +AgentImage-сборка, +env Backups)
- Create: `src/tests/PgWorker.IntegrationTests/E2e/E2eBackupScenarios.cs`

**Interfaces:**
- Consumes: `E2eFixture` (etcd/hosts/`WaitForAsync`/`RunDockerAsync`/`FreePort`), `DockerTrait.SkipIfUnavailable`, `BackupS3.ListWalAsync` (проверка объектов из теста), `WalChain.Check` (проверка непрерывности в тесте), `BackupsParser` (AC2: без parseErrors).
- Produces: `E2eFixture.MinioHostEndpoint`/`MinioAgentEndpoint`/`BackupAgentImage` (string?, null до t02), параметр `StartHostAsync(..., bool backups = false, ...)`; сценарий-маркер мерж-гейта.

- [ ] **Step 1: Фикстура — MinIO + образ агента**

В `E2eFixture.InitializeAsync` (после старта etcd):

```csharp
        // t03: MinIO (S3 бэкапов) — динамический хост-порт; bucket создаёт сценарий
        // прямым AWSSDK-клиентом (создание bucket — не операция подсистемы, spec §3.1).
        var minioPort = FreePort();
        _minio = new ContainerBuilder("minio/minio:RELEASE.2025-09-07T16-13-09Z")
            .WithCommand("server", "/data", "--console-address", ":9001")
            .WithEnvironment("MINIO_ROOT_USER", "minioadmin")
            .WithEnvironment("MINIO_ROOT_PASSWORD", "minioadmin")
            .WithPortBinding(minioPort, 9000)
            .Build();
        await _minio.StartAsync(ct);
        MinioHostEndpoint = $"http://localhost:{_minio.GetMappedPublicPort(9000)}";
        MinioAgentEndpoint = MinioHostEndpoint.Replace("localhost:", "host.docker.internal:", StringComparison.Ordinal);

        // t03: образ агента из docker/pgworker-backup.Dockerfile (файл t02): до мержа
        // t02 файла нет — сценарий скипается с внятным сообщением (мерж-порядок t03←t02).
        var agentDockerfile = Path.Combine(Root, "docker", "pgworker-backup.Dockerfile");
        BackupAgentImage = File.Exists(agentDockerfile) ? "pgworker-backup:e2e" : null;
        if (BackupAgentImage is not null)
            await RunProcessAsync("docker",
                ["build", "-q", "-f", agentDockerfile, "-t", BackupAgentImage, Root]);
```

Публичные свойства: `MinioHostEndpoint`, `MinioAgentEndpoint`, `BackupAgentImage` (string?), поле `_minio` (IContainer) + DisposeAsync. В `StartHostAsync` — параметр `bool backups = false`, при true добавить env:

```csharp
            // Подсистема бэкапов (t03): S3 для воркера — localhost, для агентов —
            // advertised (паттерн Etcd:AdvertisedEndpoints); короткие пороги контроля.
            ["PgWorker__Backups__Enabled"] = "true",
            ["PgWorker__Backups__AgentImage"] = BackupAgentImage ?? "pgworker-backup:e2e",
            ["PgWorker__Backups__S3__Endpoint"] = MinioHostEndpoint,
            ["PgWorker__Backups__S3__AdvertisedEndpoint"] = MinioAgentEndpoint,
            ["PgWorker__Backups__S3__Bucket"] = "pgworker-backups-e2e",
            ["PgWorker__Backups__S3__AccessKey"] = "minioadmin",
            ["PgWorker__Backups__S3__SecretKey"] = "minioadmin",
            ["PgWorker__Backups__Wal__VerifyIntervalSec"] = "2",
            ["PgWorker__Backups__Wal__StaleSec"] = "600",
            ["PgWorker__Backups__Wal__LagMaxSegments"] = "100000",
```

- [ ] **Step 2: Сценарий (AAA)**

`src/tests/PgWorker.IntegrationTests/E2e/E2eBackupScenarios.cs` — каркас (исполнитель дописывает хелперы по образцу `E2eScenarios` — сид кластера, `WaitForAsync`, Npgsql по host-порту мастера из portalloc):

```csharp
using FluentAssertions;
using Npgsql;
using PgWorker.Backups;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.IntegrationTests.Docker;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E t03 (AC1/AC2/AC7): кластер provisioned + MinIO → INSERT-нагрузка с
// pg_switch_wal → сегменты в MinIO, ключ wal ACTIVE, цепочка непрерывна.
[Collection(E2eCollection.Name)]
public class E2eBackupScenarios(E2eFixture fixture)
{
    [Fact]
    public async Task WalStream_UploadsSegmentsContinuously()
    {
        // Arrange — гейты: docker + образ t02 (до мержа t02 — явный skip)
        DockerTrait.SkipIfUnavailable();
        if (fixture.BackupAgentImage is null)
            Assert.Skip("docker/pgworker-backup.Dockerfile отсутствует — образ агента приходит из t02 (мерж-порядок t03 ← t02)");
        var ct = TestContext.Current.CancellationToken;

        // bucket — ПРЯМЫМ AWSSDK-клиентом (AmazonS3Client.PutBucketAsync на
        // fixture.MinioHostEndpoint; создание bucket — не операция подсистемы)
        // сид кластера "shopb" (как SeedClusterAsync E2eScenarios) → StartHostAsync(backups: true)
        // ждём provisioned (WaitForAsync по /clusters/shopb/…, бюджет ≤ 360 с)

        // Act — INSERT-нагрузка в мастер (Npgsql по host-порту из portalloc):
        //   CREATE TABLE wal_load(id bigserial, payload text);
        //   цикл 30 итераций: INSERT больших строк + SELECT pg_switch_wal()
        //   (форсируем закрытие сегментов — без таймаутов, spec §5)

        // Assert — бюджет 120 с (WaitForAsync):
        //   1) MinIO (BackupS3.ListWalAsync("shopb","shard1")) непуст и растёт;
        //      все имена без .partial (AC1)
        //   2) etcd-ключ /pgworker/backups/shopb/shard1/wal: state=ACTIVE,
        //      slot=pgw_bkp_shopb_shard1, master_node непуст,
        //      last_uploaded_segment == последнему объекту, last_uploaded_unix свежий (AC2)
        //   3) WalChain.Check(chain_start из ключа, объекты) непрерывна (AC3-факт)
        //   4) контейнер pgw-backup-wal-shopb-shard1 running (docker ps через
        //      fixture.RunDockerAsync) и слот pgw_bkp_shopb_shard1 на мастере:
        //      SELECT EXISTS(... pg_replication_slots ...) (AC1)
        //   5) BackupsParser.Parse(...) ключа — без parseErrors (AC2)
    }
}
```

Исполнитель ДОПИСЫВАЕТ код полностью (Arrange-хелперы сид-кластера — копия `SeedClusterAsync`/`ProvisionedAsync` из `E2eScenarios`; assertions — реальными вызовами). Сверить имя коллекции E2e (`E2eCollection.Name`) и сигнатуру `StartHostAsync`.

- [ ] **Step 3: Прогон маркера (Release!) + зачистка**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~WalStream_UploadsSegmentsContinuously && docker rm -f $(docker ps -aq) 2>/dev/null; docker network prune -f; docker ps -aq | wc -l
```

Ожидание: до мержа t02 — SKIP с сообщением про Dockerfile; после мержа t02 — PASS. Если маркер падает — диагностика: `docker logs pgw-backup-wal-…`, журнал `/pgworker/work/<C>` (op=backup-wal), ключ wal (error).

- [ ] **Step 4: Commit**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && git add -A && git commit -m "test(pgworker-e2e): маркер WalStream_UploadsSegmentsContinuously — MinIO-стенд, INSERT+pg_switch_wal, цепочка непрерывна (t03, arch/19 §3)"
```

---

### Task 15: Мерж-гейт — полный прогон, зачистка, roadmap

**Вход:** Tasks 1–14 (весь код написан и зелёный локально).
**Выход:** полный зелёный прогон серий (юниты → интеграции → E2E Release) с зачисткой между сериями; roadmap-гейт по мерж-порядку t03←t02.
**Проверка:** все команды серий зелёные; `docker ps -aq | wc -l` → 0 и нет осиротевших сетей после каждой серии.
**Связь со spec:** Ф6, AC7 (мерж-гейт: полный прогон на свежем Release, зачистка контейнеров/сетей, roadmap-гейт).

**Files:**
- Modify: `arch/roadmap/backup.md` (снятие тега t03 — ТОЛЬКО если t02 уже в main; иначе пункт остаётся)
- Без правок кода (если всё зелёное)

- [ ] **Step 1: Юниты (обе панели + воркеры + метрики)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests src/tests/AdminPanel.UnitTests src/tests/Shared.Metrics.UnitTests src/tests/KafkaWorker.UnitTests
```

Ожидание: PASS. Зачистка не нужна (юниты без docker), но проверить `docker ps -aq | wc -l` → 0 перед следующей серией.

- [ ] **Step 2: Интеграция PgWorker (docker-серия)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests -c Debug && docker rm -f $(docker ps -aq) 2>/dev/null; docker network prune -f; docker ps -aq | wc -l
```

Ожидание: PASS; после — контейнеров 0 (кроме поднятого dev-стенда), сетей kfw-net-*/pgw-* нет: `docker network ls | grep -c 'pgw-\|kfw-net'` → 0.

- [ ] **Step 3: Интеграция AdminPanel**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/AdminPanel.IntegrationTests && docker rm -f $(docker ps -aq) 2>/dev/null; docker network prune -f
```

- [ ] **Step 4: Полный E2E на свежем Release**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-backup-wal-stream && DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~E2e" && docker rm -f $(docker ps -aq) 2>/dev/null; docker network prune -f; docker ps -aq | wc -l
```

Ожидание: PASS (маркер E2E — skip только по легитимной причине отсутствия Dockerfile t02 — фиксируется в отчёте гейта).

- [ ] **Step 5: Roadmap-гейт**

Проверить, что t02 смержен в main (`git log origin/main --oneline | grep t02` — по факту репозитория):
- t02 ЕЩЁ не в main → пункт `t03-backup-wal-stream` в `arch/roadmap/backup.md` ОСТАЕТСЯ (мерж-порядок; тег снимет мерж-коммит t03 после t02).
- t02 в main → в `arch/roadmap/backup.md` удалить пункт `t03-backup-wal-stream` и все `← t03-backup-wal-stream`-зависимости у соседних пунктов ТЕМ ЖЕ коммитом мержа t03.

- [ ] **Step 6: Финальный коммит/отчёт**

Собрать сводку гейта: версии сборки, результаты серий (юниты/интеграция/E2E), факт зачисток (`docker ps -aq | wc -l` → 0, `docker network ls` без осиротевших), состояние roadmap. Коммит — только если были правки файлов.

---

## Self-Review (выполнен при написании плана)

**Правки по повторному ревью Фазы 4 (раунд 2, verdict CHANGES_REQUESTED) — все 5 замечаний внесены:**

1. [HIGH] Guard `State == Degraded` больше НЕ блокирует супервиз: введён `ControlOutcome(WalStreamState? State, bool ChainBroken)` — `TickShardAsync` пропускает `EnsureAgentAsync` ТОЛЬКО при `ChainBroken=true` (дыра цепочки/слот, путь `DegradeAsync` + `_chainBroken`-маркер между тиками); transient-деградация (lag/тишина) — `ChainBroken=false`, exited-агент пересоздаётся со свежими env (spec §3.2 п.5/п.7, митигация риска §7 «агент-луп после ротации»). Guard из `EnsureAgentAsync` удалён; добавлен интеграционный тест `Transient_stale_DEGRADED_не_блокирует_пересоздание_агента`; AC4a дополнен ассертом «повторный тик не поднимает агент».
2. [MEDIUM] «Фабрика свежести» устранена: при отсутствии наблюдаемого прогресса (`chain.LastSegment == null`: сегментных объектов нет / все ниже chain_start) ключ НЕ пишется, если прошлого наблюдения не было (spec п.8); если прошлое есть — `last_uploaded_segment/_unix` реюзятся из ключа (staleness/lag выстреливают от последнего факта); `DefaultIfEmpty(now)` удалён из основного пути. Добавлены тесты `Контроль_без_сегментных_объектов_ключ_не_пишется_до_первого_наблюдения` и `Контроль_пропажа_объектов_staleness_от_прошлого_факта_не_от_now`.
3. [MEDIUM] `EnsureBackupAgentAsync` получил advertised-fallback как `EnsureNodeAsync` (advertisedHost == host && ровно один движок → `Single()`); добавлен юнит-тест `EnsureBackupAgent_advertised_хост_резолвится_в_единственный_движок` по образцу существующих `ClusterDriverTests` (`FakeFactory`/`NewPlainDriver(advertisedHost)`).
4. [LOW] `WalStatusWriter.ReadAsync`/`WriteIfChangedAsync` — честный failover: continue-on-failure по endpoints, итог — последний отказ (образец `WorkJournal.WithFailoverAsync`); добавлен тест `WriteIfChanged_отказ_первого_endpoint_failover_на_второй` с обёрткой-фейком.
5. [LOW] `mc alias set` убран: S3-креды — одной env-строкой `MC_HOST_pgwbkp` (константа `EnvMcHostVariable`), собирает `WalAgentCommand.McHost(endpoint, access, secret)` с URL-escape (секреты не попадают в argv/`ps`); `AgentEnv` (Task 9) кладёт `MC_HOST_pgwbkp` и больше не раздаёт S3-креды по-переменно; тесты Task 4 обновлены + `McHost_собирает_URL_с_URL_escape_кредов`.

Мелкие заметки ревьюера учтены: Task 10 Interfaces исправлен на `ShardBackups.Full` (фактическое имя модели); list S3 каждый тик до создания ключа — оставлен (подтверждено допустимым, комментарий в теле); формула min(wal_start_segment по COMPLETED) — не тронута.

**Правки по ревью Фазы 4 (раунд 1) — остаются в силе:** (1) S-чистка `/pgworker/backups/<C>/<X>/` в `RemoveShardProcess.CleanKeysAsync` (Task 11 Step 4); (2) свежесть статуса для ensure-агента одним тиком (AC4c — `ControlOutcome.State`); (3) `EnsureBucketAsync` вне `IBackupS3` (bucket — прямой AWSSDK-клиент фикстур/E2E); (4) `#PGW_BACKUP_S3_ADVERTISED_ENDPOINT=` в `deploy/.env.example` (Task 1 Step 5); (5) консервативный учёт агентов в `GetHostsAsync` (Task 8 Step 4); (6) DEGRADED при дыре без прошлого ключа (AC4-тотальность).

**Покрытие spec → задачи:**

| Требование spec | Задача |
|---|---|
| §3.1 WalFileName/WalChain/WalAgentCommand/BackupS3/WalStreamProcess/WalStatusWriter | 2, 3, 4, 5, 9–10, 6 |
| §3.2 шаг 1 (креды t02-transient) | 9 (TickShardAsync п.1) |
| §3.2 шаг 2 (резолв мастера) | 9 (п.2 + ResolveMasterRef) |
| §3.2 шаг 3 (ensure слота/инвалидация) | 7, 9 (п.3), 10 (AC4b) |
| §3.2 шаг 4 (контейнер агента, env, volume, лимиты, сеть, unless-stopped) | 8 (драйвер + advertised-fallback), 9 (EnsureAgentAsync; restart-политика — уже в BuildContainerBody) |
| §3.2 шаг 5 (супервиз exited/смена мастера — включая transient-DEGRADED) | 9 (ControlOutcome), 10 (ChainBroken vs transient) |
| §3.2 шаг 6 (контроль по расписанию, DEGRADED+стоп, восстановление полным — одним тиком) | 10 |
| §3.2 шаг 7 (lag-зонд, восстановление → ACTIVE) | 10 (+метрика 12) |
| §3.2 шаг 8 (chain_start приоритеты, ключ не пишется до наблюдения, факт над записью) | 10 |
| §3.2 стоп-семантика (Enabled=false/QUARANTINED/S/D1/D2) | 9 (StopAll/StopShard), 11 (S+D1+D2+S-CleanKeys) |
| §3.3 панель (парсер, 3 алерта) | 13 |
| §3.4 конфигурация | 1 (+deploy env-комментарий) |
| §3.5 наблюдаемость (логи, метрика) | 9–10 (логи), 12 (gauge) |
| Ф5 E2E-маркер | 14 |
| Ф6 мерж-гейт/зачистка/roadmap | 15 |
| AC1–AC8 | 14 (AC1/AC2/AC7-маркер), 3 (AC3-юниты), 4 (AC4-юниты, вкл. AC4c «тем же тиком», тотальность и блокировку повторных подъёмов), 10+13 (AC5), 11 (AC6), 15 (AC7/AC8) |

**Осознанные решения плана** (зафиксированы, чтобы исполнитель не блуждал):
1. Docker-операции агентов — через `IClusterDriver` (3 новых метода, advertised-fallback как `EnsureNodeAsync`), а не отдельный engine-стек в Backups: драйвер уже инкапсулирует hosts/engines/сети; `ListNodeObjectsAsync` фильтрует `pgw-backup-wal-*` (D1-гварды нод не ломаются); `GetHostsAsync` счётчик UsedSlots агентов НЕ фильтрует — консервативный учёт ресурсов (агент жёстко сидит на хосте мастера, счётчик следует за фактом при пересоздании). Swarm — `EnsureBackupAgentAsync` = Failed (деплой/стенд/E2E — plain; канон t03 swarm не требует).
2. `S3:AdvertisedEndpoint` — реализация-деталь t03 (паттерн `Etcd:AdvertisedEndpoints`): воркер-хост и контейнеры агентов видят MinIO по разным адресам; env-комментарий добавлен в `deploy/.env.example` (Task 1 Step 5); стенд-включение — t02.
3. Деградации двух классов: chain-broken (дыра/слот — `DegradeAsync`: стоп агента + `_chainBroken`-маркер, подъём только после сдвига chain_start новым полным) и transient (lag/тишина — статус DEGRADED, агент/супервиз живут: exited пересоздаётся, running не трогается; self-healing тиками → ACTIVE). Смешивание классов в одном guard'е — тупик restart-контура (ревью Ф4-2 №1), разведены контрактно через `ControlOutcome.ChainBroken`.
4. Факты прогресса — только из наблюдений: last_uploaded/unix из S3-объектов либо прошлого ключа; now() не подставляется никогда; без наблюдений и прошлого ключа ключ wal не пишется (spec п.8).
5. `last_uploaded_unix` = S3 `LastModified` последнего сегмента (истина — объекты, не часы воркера).
6. Секреты агента: PG-параметры — по-переменно env; S3-креды — одна строка `MC_HOST_pgwbkp` с URL-escape (mc резолвит alias из env; ничего в argv/`ps`, ничего в Cmd). Битые S3-креды → тишина загрузок → transient-DEGRADED (сигнал оператору); битые PG-креды → смерть pg_receivewal → restart-луп → пересоздание со свежими env (spec п.5).
7. Интеграционные тесты процесса — реальный etcd (EtcdFixture) + фейки docker/SQL/S3: гонять живой PG+docker в каждом кейсе = хрупкость; живое — в E2E.
8. Тестовые задачи 9–14 содержат опорные каркасы тестов: исполнитель ДОПИСЫВАЕТ Arrange/Assert-код по образцу задач 2–8 (существующие фикстуры/хелперы), не выдумывая новых паттернов — все требуемые ассерты перечислены в комментариях.
