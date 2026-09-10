# План: t02-backup-full-daily — полные суточные бэкапы шардов (pg_basebackup)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Цель:** полные бэкапы БД каждого шарда Active-кластера раз в сутки (`pg_basebackup`): rolling-планировщик в PgWorker, ephemeral джоб-контейнеры образа `pgworker-backup` (mc-загрузка в S3), жизненный цикл PLANNED→RUNNING→UPLOADING→COMPLETED/FAILED в etcd, бэкофф повторов, роль `backup_exec`, суточный алерт `backup-full-stale` в AdminPanel.

**Архитектура:** новый процесс `BackupProcess` (проект `src/PgWorker.Backups`, по образцу `PgWorker.Moves`) в Active-ветке ReconcileLoop под клэймом `<C>`: тик проверяет возраст последнего COMPLETED, резолвит sync-standby (fallback мастер), создаёт PLANNED-запись (journal-before-manipulations) и джоб-контейнер, супервизирует его по stdout-маркерам и exit-коду, фиксирует итог в etcd, удаляет контейнер/staging-volume. Джоб — чистый shell-скрипт в образе `pgworker-backup` (postgres:18 + mc), etcd не знает. Панель читает префикс `/pgworker/backups/` и вычисляет алерт по снапшоту.

**Стек:** .NET 10 (`LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`), централизованные пакеты (`src/Directory.Packages.props`), xUnit v3 + FluentAssertions, Testcontainers (etcd/MinIO), docker Engine API v1.44.

**Spec:** [`spec.md`](spec.md) — план аргументируется от спека; канон — [`arch/19-backups.md`](../../../arch/19-backups.md) (обновлён в этой ветке).

## Global Constraints (действуют в каждой задаче)

- .NET 10, C# `LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true` — код не компилируется с warnings.
- Пакеты только через `src/Directory.Packages.props` (новых пакетов в t02 НЕТ — всё на BCL и существующих рефах).
- Docker-тесты: порты ТОЛЬКО динамические (`WithPortBinding(..., assignRandomHostPort: true)` + `GetMappedPublicPort`); никаких хардкодов хост-портов в тестах.
- После КАЖДОЙ docker-серии: `docker rm -f $(docker ps -aq --filter name=pgw-) 2>/dev/null` + обязательный `docker network prune -f` (сети не трогают живые — чистятся только неприкаянные). Фильтр `pgw-` не затрагивает контейнеры dev-стенда (`as-*`/`adminpanel*`/`deploy-*`) — стенд и тесты живут на одном хосте; следующая серия — только после зачистки (ревью Ф4-2 finding 2: буква `docker ps -aq` без фильтра снесла бы живой стенд).
- Таймауты фикстур ≤ 100 с (`BrokerBootSec`-аналог); ожидания E2E-сценариев — по существующим паттернам E2eFixture.
- Тесты пишутся с AAA-комментариями (`// Arrange`, `// Act`, `// Assert`).
- Документация/комментарии — русский; идентификаторы — английский.
- Коммиты — в ветке worktree `feat-t02-backup-full-daily`; в `main` не переключаться, не пушить без команды.
- Прогон тестов из корня worktree: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-backup-full-daily`.
- Отправка docker-E2E — только на свежем Release (`-c Release`, сборку делает E2eFixture; `PGW_TEST_E2E_NOBUILD=1` запрещён в гейте).

## Соглашения имён (используются всеми задачами)

| Имя | Где | Значение |
|---|---|---|
| `BackupExecRole` | `DatabaseProvisioner` (const) | `"backup_exec"` |
| `BackupNames` | `PgWorker.Backups` | ключи etcd + имена контейнера/volume |
| `BackupsRuntimeOptions` | `PgWorker.Backups/Options.cs` | склейка `PgWorker:Backups` (образец `MovesRuntimeOptions`) |
| `BackupProcess.TickAsync(snap, backups, ct)` | `PgWorker.Backups/Process` | тик G0–G3/S |
| `IClusterProcesses.BackupsAsync(snap, backups, ct)` | `PgWorker.App/Loops` | грань цикла |
| `ClusterBackupsInfo` | `AdminPanel.Core` | панельная модель префикса |

---

### Task 1: Коммит канона arch (уже внесён spec-фазой)

**Files:**
- Commit: `arch/19-backups.md`, `arch/adminpanel/02-etcd-contract.md`, `docs/superpowers/2026-09-10-t02-backup-full-daily/` (spec + этот план)

**Interfaces:** — нет кода.

- [ ] **Step 1: Проверить, что правки канона на месте**

Run: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-backup-full-daily && git status --short`
Expected: ` M arch/19-backups.md`, ` M arch/adminpanel/02-etcd-contract.md`, `?? docs/superpowers/2026-09-10-t02-backup-full-daily/`.

- [ ] **Step 2: Закоммитить (arch-first — первым коммитом ветки, spec §5 фаза 1)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-backup-full-daily
git add arch/19-backups.md arch/adminpanel/02-etcd-contract.md docs/superpowers/2026-09-10-t02-backup-full-daily
git commit -m "docs(t02): канон arch/19 §2/§4-§6/§9-§10 + spec/plan полных суточных бэкапов"
```

**Выход:** канон закоммичен; связь со spec — §2 п.1 (arch-first), §5 фаза 1.

---

### Task 2: Опции `Job`/`Retry` + валидация Enabled→Image

**Files:**
- Modify: `src/PgWorker.App/Options.cs` (классы `BackupsOptions` и новые `BackupsJobOptions`/`BackupsRetryOptions`)
- Modify: `src/PgWorker.App/Program.cs` (текст Validate — уже зовёт `Backups.IsValid()`, расширить сообщение)
- Test: `src/tests/PgWorker.UnitTests/App/BackupsOptionsTests.cs`

**Interfaces:**
- Produces: `BackupsOptions.Job.Image` (дефолт `"pgworker-backup:dev"`), `BackupsOptions.Retry.BaseSec=300`/`MaxSec=3600`; `BackupsOptions.IsValid()` требует непустой `Job.Image` при `Enabled=true` (используют Task 6 `ToRuntime()`, Task 13 wiring).

- [ ] **Step 1: Тесты (падают)** — дописать в `BackupsOptionsTests.cs`:

```csharp
[Fact]
public void Defaults_JobRetry_CanonValues()
{
    // Arrange / Act — дефолты t02 (arch/19 §9): образ джоба + бэкофф 300/3600.
    var options = new BackupsOptions();

    // Assert
    options.Job.Image.Should().Be("pgworker-backup:dev");
    options.Retry.BaseSec.Should().Be(300);
    options.Retry.MaxSec.Should().Be(3600);
}

[Fact]
public void Enabled_EmptyJobImage_Invalid()
{
    // Arrange — включили с S3-комплектом, но образ джоба не задан.
    var options = new BackupsOptions
    {
        Enabled = true,
        S3 = new BackupsS3Options
        {
            Endpoint = "http://host.docker.internal:9000",
            Bucket = "pgworker-backups",
            AccessKey = "minioadmin",
            SecretKey = "minioadmin",
        },
        Job = new BackupsJobOptions { Image = "" },
    };

    // Act / Assert — fail-fast старта (Program.cs ValidateOnStart).
    options.IsValid().Should().BeFalse();
}
```

- [ ] **Step 2: Прогнать — упасть**

Run: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-backup-full-daily && dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~BackupsOptionsTests"`
Expected: FAIL — нет `BackupsJobOptions`/`Job`/`Retry`.

- [ ] **Step 3: Реализация** — в `Options.cs` после `BackupsAgentOptions` добавить и в `BackupsOptions` — два свойства:

```csharp
/// <summary>Образ джоба полного бэкапа (arch/19 §2/§9, t02): собирается из
/// docker/PgWorker.Backup.Dockerfile; запуск — воркер, содержимое — без .NET.</summary>
public sealed class BackupsJobOptions
{
    public string Image { get; set; } = "pgworker-backup:dev";
}

/// <summary>Бэкофф переснятия FAILED-полного (arch/19 §2, t02):
/// задержка n-й попытки после последнего COMPLETED =
/// min(BaseSec·2^(n−1), MaxSec), без лимита попыток.</summary>
public sealed class BackupsRetryOptions
{
    public int BaseSec { get; set; } = 300;

    public int MaxSec { get; set; } = 3600;
}
```

В `BackupsOptions` (после `Agent`):

```csharp
    public BackupsJobOptions Job { get; set; } = new();

    public BackupsRetryOptions Retry { get; set; } = new();
```

`IsValid()` дополнить условием `&& !string.IsNullOrWhiteSpace(Job.Image)`. В `Program.cs` расширить сообщение валидации (строка с `Validate(o => o.Backups.IsValid(), ...)`):

```csharp
    .Validate(o => o.Backups.IsValid(),
        "PgWorker:Backups: Enabled=true требует непустые PgWorker:Backups:S3:Endpoint/Bucket/AccessKey/SecretKey (env PGW_BACKUP_S3_*) и Backups:Job:Image (arch/19 §7/§9)")
```

- [ ] **Step 4: Прогнать весь юнит-проект** — зелёно.

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug`
Expected: PASS (весь проект).

- [ ] **Step 5: Коммит**

```bash
git add src/PgWorker.App/Options.cs src/PgWorker.App/Program.cs src/tests/PgWorker.UnitTests/App/BackupsOptionsTests.cs
git commit -m "feat(t02): опции Backups Job/Retry + валидация Enabled→Job.Image"
```

**Выход:** конфиг-каркас t02; spec §3.6/§4 (Options.cs: BackupsOptions + Job{Image}, Retry{BaseSec=300,MaxSec=3600} + валидация), arch/19 §9.

---

### Task 3: `BackupsParser`: `wal_start_segment` опционален

**Files:**
- Modify: `src/PgWorker.Etcd/Parsing/BackupsModel.cs:60-70` (`FullBackupState.WalStartSegment: string` → `string?`)
- Modify: `src/PgWorker.Etcd/Parsing/BackupsParser.cs:151-208` (`TryParseFull` — поле опционально)
- Test: `src/tests/PgWorker.UnitTests/EtcdFixtures/backups-full.json`, `src/tests/PgWorker.UnitTests/Etcd/BackupsParserTests.cs`

**Interfaces:**
- Consumes: модель t01.
- Produces: `FullBackupState.WalStartSegment` — `string?` (null до фазы UPLOADING; Task 7 сериализатор и Task 11-12 процесс рассчитывают на null).

- [ ] **Step 1: Фикстура** — в `backups-full.json` добавить запись PLANNED без `wal_start_segment` (шард s2) и запись RUNNING без него:

```json
  {"key":"/pgworker/backups/demo/s2/full/20260910030000Z","value":"{\"state\":\"PLANNED\",\"node\":\"pgw-demo-s2-2\",\"role\":\"replica\",\"started_unix\":1757463600}","modRevision":65},
  {"key":"/pgworker/backups/shop/s2/full/20260910030000Z","value":"{\"state\":\"RUNNING\",\"node\":\"pgw-shop-s2-1\",\"role\":\"master\",\"started_unix\":1757463600}","modRevision":140},
```

- [ ] **Step 2: Тест (падает)** — в `BackupsParserTests.cs`:

```csharp
[Fact]
public void Parse_FullWithoutWalStart_ParsedWithNull()
{
    // Arrange — записи до фазы UPLOADING не знают wal_start_segment
    // (arch/19 §4: заполняется с UPLOADING, у рано упавших может отсутствовать).
    var kvs = EtcdFixtures.LoadKv("backups-full.json");

    // Act
    var result = BackupsParser.Parse(kvs, out var errors);

    // Assert — null без ошибки; обязательны только state/node/role/started_unix.
    result.IsSuccess.Should().BeTrue();
    errors.Should().NotContain(e => e.Contains("20260910030000Z"));
    var planned = result.Value.First(c => c.Cluster == "demo").Shards["s2"].Full
        .Single(f => f.Id == "20260910030000Z");
    planned.State.Should().Be(FullBackupStatus.Planned);
    planned.WalStartSegment.Should().BeNull();
    var running = result.Value.First(c => c.Cluster == "shop").Shards["s2"].Full
        .Single(f => f.Id == "20260910030000Z");
    running.State.Should().Be(FullBackupStatus.Running);
    running.WalStartSegment.Should().BeNull();
}
```

- [ ] **Step 3: Прогнать — упасть**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~BackupsParserTests"`
Expected: FAIL (текущий парсер требует `wal_start_segment`).

- [ ] **Step 4: Реализация**

`BackupsModel.cs` — тип поля (и обновить doc-комментарий записи: опционален до UPLOADING):

```csharp
public sealed record FullBackupState(
    string Id,
    FullBackupStatus State,
    string Node,
    BackupSourceRole Role,
    long StartedUnix,
    long? FinishedUnix,
    string? WalStartSegment,
    long? SizeBytes,
    string? Error,
    BackupVerify? Verify);
```

`BackupsParser.TryParseFull` — убрать `walStart` из проверки обязательных:

```csharp
            var startedUnix = ReadLong(root, "started_unix");
            var node = ReadString(root, "node");
            if (state is null || role is null || startedUnix is null || string.IsNullOrEmpty(node))
            {
                errors.Add($"{key}: битый JSON или неизвестное state/role, обязательное поле отсутствует");
                return null;
            }
```

и в конструкторе — `ReadString(root, "wal_start_segment")` как есть (null допустим). Комментарий метода скорректировать: «обязательны state/node/role/started_unix; `wal_start_segment` — с фазы UPLOADING (§4)».

- [ ] **Step 5: Прогнать юниты Etcd + весь проект** — существующие тесты (FAILED с `wal_start` и COMPLETED с ним) остаются зелёными.

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug`
Expected: PASS.

- [ ] **Step 6: Коммит**

```bash
git add src/PgWorker.Etcd/Parsing src/tests/PgWorker.UnitTests/Etcd src/tests/PgWorker.UnitTests/EtcdFixtures/backups-full.json
git commit -m "feat(t02): BackupsParser — wal_start_segment опционален (arch/19 §4)"
```

**Выход:** контракт допускает статусы до UPLOADING; spec §3.3 п.1.

---

### Task 4: Docker-грань джоба: логи, runtime-инспект, tmpfs/extra_hosts

**Files:**
- Modify: `src/PgWorker.Docker/Engine/IDockerEngine.cs` (`GetContainerLogsAsync`; `DockerContainerInspect` + `Running`/`ExitCode`; `ContainerSpec` + `Tmpfs`/`ExtraHosts`)
- Modify: `src/PgWorker.Docker/Engine/DockerEngine.cs` (реализация; `ContainerInspectDto.State`; `BuildContainerBody`)
- Test: `src/tests/PgWorker.IntegrationTests/Docker/BackupEngineTests.cs` (новый)

**Interfaces:**
- Produces (используют Task 7, 10, 11):
  - `Task<Result<string>> GetContainerLogsAsync(string idOrName, int tail, CancellationToken ct)`
  - `DockerContainerInspect(..., bool? Running = null, int? ExitCode = null)`
  - `ContainerSpec(..., IReadOnlyDictionary<string, string>? Tmpfs = null, IReadOnlyList<string>? ExtraHosts = null)`

- [ ] **Step 1: Интеграционный тест (падает)** — `src/tests/PgWorker.IntegrationTests/Docker/BackupEngineTests.cs` (по образцу `ExecDriverTests` — docker-гейт, порты не публикуем):

```csharp
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using PgWorker.Docker.Engine;
using Xunit;

namespace PgWorker.IntegrationTests.Docker;

// Docker-грань джоба бэкапа (t02): логи контейнера + runtime-инспект
// (exit-код) + HostConfig-поля tmpfs/extra_hosts (arch/19 §2/§6).
public class BackupEngineTests
{
    [Fact]
    public async Task Logs_And_RuntimeInspect_OfExitedContainer()
    {
        // Arrange — контейнер без портов: печатает маркер и падает с кодом 7.
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var container = new ContainerBuilder("alpine:3.20")
            .WithCommand("sh", "-c", "echo pgw-marker; exit 7")
            .Build();
        await container.StartAsync(ct);

        // Act — ждём exited, тянем логи и инспект напрямую движком сокета.
        var engine = new DockerEngineFactory().Create("unix:///var/run/docker.sock");
        string? name = null;
        for (var i = 0; i < 30; i++)
        {
            var list = await engine.ListContainersAsync(container.Name, all: true, ct);
            if (list.IsSuccess && list.Value.FirstOrDefault(c => c.Names.Contains(container.Name)) is { State: "exited" } found)
            {
                name = found.Names[0];
                break;
            }
            await Task.Delay(500, ct);
        }
        name.Should().NotBeNull("контейнер обязан выйти за 15 c");

        var logs = await engine.GetContainerLogsAsync(name!, tail: 100, ct);
        var inspect = await engine.InspectContainerAsync(name!, ct);

        // Assert — stdout размультиплексирован; инспект несёт факт/код выхода.
        logs.IsSuccess.Should().BeTrue(logs.Error?.ToString());
        logs.Value.Should().Contain("pgw-marker");
        inspect.Value.Running.Should().BeFalse();
        inspect.Value.ExitCode.Should().Be(7);
    }

    [Fact]
    public async Task Create_WithTmpfsAndExtraHosts_Applied()
    {
        // Arrange / Act — контейнер с tmpfs-квотой и host-gateway-записью.
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        const string name = "pgw-backup-engine-test";
        var spec = new ContainerSpec(
            Image: "alpine:3.20",
            Env: new Dictionary<string, string>(),
            VolumeName: "",
            VolumeDest: "",
            Ports: [],
            Hostname: name,
            CpuCores: null,
            MemoryBytes: null,
            Label: "enginetest",
            Cmd: ["sh", "-c", "mount | grep backup-staging; getent hosts host.docker.internal; exit 0"],
            Tmpfs: new Dictionary<string, string> { ["/backup-staging"] = "size=10485760" },
            ExtraHosts: ["host.docker.internal:host-gateway"]);

        var engine = new DockerEngineFactory().Create("unix:///var/run/docker.sock");
        var created = await engine.CreateContainerAsync(spec, name, ct);
        var started = await engine.StartContainerAsync(name, ct);
        string? exited = null;
        for (var i = 0; i < 30; i++)
        {
            var list = await engine.ListContainersAsync(name, all: true, ct);
            if (list.IsSuccess && list.Value.Any(c => c.Names.Contains(name) && c.State == "exited"))
            {
                exited = name;
                break;
            }
            await Task.Delay(500, ct);
        }
        var logs = exited is null ? null : await engine.GetContainerLogsAsync(name, tail: 50, ct);
        var removed = await engine.RemoveContainerAsync(name, force: true, ct);

        // Assert — tmpfs смонтирован, extra_hosts зарезолвился, удаление идемпотентно.
        created.IsSuccess.Should().BeTrue(created.Error?.ToString());
        started.IsSuccess.Should().BeTrue(started.Error?.ToString());
        exited.Should().NotBeNull("контейнер обязан выйти");
        logs!.Value.Should().Contain("tmpfs on /backup-staging");
        logs.Value.Should().Contain("host.docker.internal");
        removed.IsSuccess.Should().BeTrue();
    }
}
```

- [ ] **Step 2: Прогнать — упасть (нет методов/полей)**

Run: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-backup-full-daily && PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~BackupEngineTests"`
Expected: FAIL компиляция.

- [ ] **Step 3: Реализация**

`IDockerEngine.cs` — после `InspectContainerAsync`:

```csharp
    // GET /containers/<id>/logs?stdout=1&stderr=1&tail=N — супервизия джобов
    // бэкапов (t02): stdout-маркеры фаз/result; ответ — raw-stream (demux).
    Task<Result<string>> GetContainerLogsAsync(string idOrName, int tail, CancellationToken ct);
```

`DockerContainerInspect` — расширить (опциональные в конце, обратная совместимость):

```csharp
public sealed record DockerContainerInspect(
    string Id, string Hostname, string[] Aliases, string[] Env, PortMap[] Ports,
    bool? Running = null, int? ExitCode = null);
```

`ContainerSpec` — в конец:

```csharp
    IReadOnlyList<string>? Cmd = null,
    string? Network = null,
    IReadOnlyList<string>? NetworkAliases = null,
    IReadOnlyDictionary<string, string>? Tmpfs = null,
    IReadOnlyList<string>? ExtraHosts = null);
```

`DockerEngine.cs`:

```csharp
    // GET /containers/<id>/logs — тело raw-stream (мультиплексировано), demux
    // как у exec; не-TTY контейнеры docker всегда шлют фреймами.
    public async Task<Result<string>> GetContainerLogsAsync(string idOrName, int tail, CancellationToken ct)
        => await Result<string>.FromAsync(async () =>
        {
            using var request = new HttpRequestMessage(HttpMethod.Get,
                Api + $"/containers/{Uri.EscapeDataString(idOrName)}/logs?stdout=1&stderr=1&tail={tail}");
            using var response = await httpClient.SendAsync(request, ct);
            if (!response.IsSuccessStatusCode)
            {
                var errorBody = response.Content is null
                    ? string.Empty
                    : await response.Content.ReadAsStringAsync(ct);
                throw new DockerHttpException("GET", $"/containers/{idOrName}/logs", (int)response.StatusCode, errorBody);
            }

            var payload = response.Content is null ? [] : await response.Content.ReadAsByteArrayAsync(ct);
            var (stdout, stderr) = Demux(payload);
            return stderr.Length == 0 ? stdout : stdout + "\n" + stderr;
        });
```

`InspectContainerAsync` — прочитать `State` и прокинуть:

```csharp
            return new DockerContainerInspect(dto.Id, dto.Config?.Hostname ?? "", aliases, dto.Config?.Env ?? [],
                ports.Distinct().ToArray(),
                dto.State?.Running, dto.State?.ExitCode);
```

DTO (`ContainerInspectDto` + новый класс):

```csharp
    private sealed class ContainerInspectDto
    {
        [JsonPropertyName("Id")] public string Id { get; set; } = "";

        [JsonPropertyName("Config")] public ContainerConfigDto? Config { get; set; }

        [JsonPropertyName("State")] public ContainerStateDto? State { get; set; }

        [JsonPropertyName("NetworkSettings")] public NetworkSettingsDto? NetworkSettings { get; set; }
    }

    private sealed class ContainerStateDto
    {
        [JsonPropertyName("Running")] public bool? Running { get; set; }

        [JsonPropertyName("ExitCode")] public int? ExitCode { get; set; }
    }
```

`BuildContainerBody` — после лимитов:

```csharp
        if (spec.Tmpfs is { Count: > 0 })
            hostConfig["Tmpfs"] = spec.Tmpfs;
        if (spec.ExtraHosts is { Count: > 0 })
            hostConfig["ExtraHosts"] = spec.ExtraHosts;
```

- [ ] **Step 4: Прогнать — зелёно; зачистка серии**

Run: `PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~BackupEngineTests"`
Expected: PASS (2 теста). Затем зачистка (Global Constraints, фильтр pgw- — контейнеры живого стенда не трогаем): `docker rm -f $(docker ps -aq --filter name=pgw-) 2>/dev/null; docker network prune -f`.

- [ ] **Step 5: Коммит**

```bash
git add src/PgWorker.Docker/Engine src/tests/PgWorker.IntegrationTests/Docker/BackupEngineTests.cs
git commit -m "feat(t02): DockerEngine — логи контейнера, runtime-инспект, tmpfs/extra_hosts"
```

**Выход:** транспорт супервизии джоба; spec §3.2 (поллинг логов, квота tmpfs, extra_hosts), §4 (GetContainerLogsAsync).

---

### Task 5: Секреты: `backup_password` в Ensurer + `backup_exec` в Rotator

**Files:**
- Modify: `src/PgWorker.Provisioning/Sql/DatabaseProvisioner.cs` (const `BackupExecRole` + `BuildBackupExecRoleGuardSql`)
- Modify: `src/PgWorker.Provisioning/Processes/ClusterSecretEnsurer.cs` (четвёртый пароль-ключ)
- Modify: `src/PgWorker.Provisioning/Processes/ClusterSecretRotator.cs` (R2/R3 + backup_exec)
- Modify: `src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs` (`FakeSql.ScalarResultBySql`)
- Test: `src/tests/PgWorker.UnitTests/Provisioning/ClusterSecretEnsurerTests.cs`, `ClusterSecretRotatorTests.cs`

**Interfaces:**
- Produces:
  - `ClusterCredentials(AppCredentials App, string MoverPassword, AppCredentials BucketAdmin, string BackupPassword)` — конструирует только Ensurer, потребители читают поля.
  - ключ `/clusters/<C>/backup_password` (кладёт Ensurer put-if-absent, Rotator — txn; удаляет D2 общим `del --prefix /clusters/<C>/` — уже есть).
  - `DatabaseProvisioner.BackupExecRole` == `"backup_exec"` и `BuildBackupExecRoleGuardSql(password)` — gexec-SELECT: потребляют Task 11 (G2 каждый тик) и РОТАТОР R2 при отсутствии роли (гвард создаёт роль с NEW-паролем — существующая ротация не ломается при `Enabled=false`, ревью Ф4 finding 1).
  - `InstallSecrets` НЕ меняется (пароль per-cluster, не per-install).

- [ ] **Step 1: Тест Ensurer (падает)** — в `ClusterSecretEnsurerTests.cs` (механика `Fakes.FakeEtcd`: `.Seed(key, value)`, чтение `etcd.Store[key].Value` — как в соседних кейсах):

```csharp
[Fact]
public async Task Ensure_BackupPasswordAbsent_GeneratedPutIfAbsent()
{
    // Arrange — пятёрка t02 есть, backup_password отсутствует (t02-секрет, arch/19 §7).
    var etcd = new Fakes.FakeEtcd();
    etcd.Seed("/clusters/shop/app_user", "app");
    etcd.Seed("/clusters/shop/app_password", "pa0000000000000000000000000000A");
    etcd.Seed("/clusters/shop/mover_password", "pm0000000000000000000000000000A");
    etcd.Seed("/clusters/shop/bucket_admin_user", "bucket_admin");
    etcd.Seed("/clusters/shop/bucket_admin_password", "pba00000000000000000000000000A");

    // Act
    var result = await Sut(etcd).EnsureAsync("shop", Config, CancellationToken.None);

    // Assert — ключ создан (32 [A-Za-z0-9], AppSecretGenerator), в кредлах.
    result.IsSuccess.Should().BeTrue();
    etcd.Store["/clusters/shop/backup_password"].Value
        .Should().MatchRegex("^[A-Za-z0-9]{32}$");
    result.Value!.BackupPassword.Should().Be(etcd.Store["/clusters/shop/backup_password"].Value);
}

[Fact]
public async Task Ensure_BackupPasswordExists_NotOverwritten()
{
    // Arrange — внешний etcdctl-пароль уже записан.
    var etcd = new Fakes.FakeEtcd();
    etcd.Seed("/clusters/shop/app_user", "app");
    etcd.Seed("/clusters/shop/app_password", "pa0000000000000000000000000000A");
    etcd.Seed("/clusters/shop/mover_password", "pm0000000000000000000000000000A");
    etcd.Seed("/clusters/shop/bucket_admin_user", "bucket_admin");
    etcd.Seed("/clusters/shop/bucket_admin_password", "pba00000000000000000000000000A");
    etcd.Seed("/clusters/shop/backup_password", "OldBackupPass0000000000000000000A");

    // Act
    var result = await Sut(etcd).EnsureAsync("shop", Config, CancellationToken.None);

    // Assert — put-if-absent: существующее значение не тронуто.
    result.Value!.BackupPassword.Should().Be("OldBackupPass0000000000000000000A");
    etcd.Store["/clusters/shop/backup_password"].Value
        .Should().Be("OldBackupPass0000000000000000000A");
}
```

- [ ] **Step 2: Тесты Rotator (падают)** — в `ClusterSecretRotatorTests.cs` доработать под четвёртую роль. ВАЖНО (ревью Ф4 finding 1): R2 для `backup_exec` исполняет gexec-гвард — голый `ALTER ROLE` упал бы на отсутствующей роли при дефолтном `Enabled=false` (G2 процесса бэкапов не выполнялся) и заявка ротации не закрылась бы никогда.
  - в `SeedCluster` добавить `etcd.Seed("/clusters/shop/backup_password", "OldBackupPass0000000000000000000A");`
  - в `Fakes.cs` (`FakeSql`) добавить per-SQL хук скаляра (по образцу `ScalarResultByDsn`):

```csharp
        // t02: ответ гварда роли зависит от SQL (роль есть → null, нет → CREATE-текст)
        public Func<string, string, Result<object?>>? ScalarResultBySql { get; set; }
```

и в `ExecuteScalarAsync` — `ScalarResultBySql is { } bySql ? bySql(dsn, sql) : …` перед `ScalarResultByDsn`.
  - в `Tick_Ticket_AltersAllShardsAndCommitsAtomically` (роль СУЩЕСТВУЕТ — дефолт FakeSql scalar → null) обновить ассерты:

```csharp
        // Assert — ALTER трёх ролей + гвард→ALTER backup_exec на мастерах ОБЕИХ
        // шардов (8 SQL + 2 скаляра-гварда); одна txn: compare OLD (4 креда + 2 dsn)
        // + put новых кредов + перезапись dsn + del заявки
        outcome.IsSuccess.Should().BeTrue();
        var sqlTexts = rig.Sql.Executed.Select(e => e.Sql).ToList();
        sqlTexts.Should().HaveCount(8);
        sqlTexts.Count(s => s.Contains("ALTER ROLE \"app\" PASSWORD")).Should().Be(2);
        sqlTexts.Count(s => s.Contains("ALTER ROLE \"bucket_admin\" PASSWORD")).Should().Be(2);
        sqlTexts.Count(s => s.Contains("ALTER ROLE \"bucket_mover\" PASSWORD")).Should().Be(2);
        sqlTexts.Count(s => s.Contains("ALTER ROLE \"backup_exec\" PASSWORD")).Should().Be(2);
        rig.Sql.Scalars.Count(s => s.Sql.Contains("backup_exec")).Should().Be(2); // гварды

        var newBackup = rig.Etcd.Store["/clusters/shop/backup_password"].Value;
        newBackup.Should().MatchRegex("^[A-Za-z0-9]{32}$").And.NotBe("OldBackupPass0000000000000000000A");
        // ... существующие ассерты app/mover/admin/dsn остаются ...

        commit.Compare.Should().Contain(c =>
            c.Key == "/clusters/shop/backup_password" && c.Arg == "OldBackupPass0000000000000000000A");
```

  - НОВЫЙ кейс (роль ОТСУТСТВУЕТ — подсистема бэкапов выключена):

```csharp
    // AAA: R2 backup_exec при отсутствующей роли — гвард создаёт её с NEW-паролем,
    // ротация НЕ падает (регресс-гвард finding 1: Enabled=false, G2 не выполнялся)
    [Fact]
    public async Task Tick_Ticket_BackupExecRoleAbsent_CreatedByGuard()
    {
        // Arrange — заявка; скаляр-гвард backup_exec возвращает CREATE-текст (роли нет)
        var rig = await NewRig();
        SeedTicket(rig.Etcd);
        rig.Sql.ScalarResultBySql = (_, sql) =>
            sql.Contains("\"backup_exec\"")
                ? Result<object?>.Success("SELECT 'CREATE ROLE \"backup_exec\" LOGIN REPLICATION PASSWORD ''x'''")
                : Result<object?>.Success(null);

        // Act
        var outcome = await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — роль создана (CREATE ×2 шарда), ALTER backup_exec не было;
        // ротация успешна: заявка закрыта, backup_password перезаписан.
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        var executed = rig.Sql.Executed.Select(e => e.Sql).ToList();
        executed.Count(s => s.Contains("CREATE ROLE \"backup_exec\"")).Should().Be(2);
        executed.Should().NotContain(s => s.Contains("ALTER ROLE \"backup_exec\""));
        rig.Etcd.Store.Should().NotContainKey("/pgworker/rotations/shop");
        rig.Etcd.Store["/clusters/shop/backup_password"].Value
            .Should().MatchRegex("^[A-Za-z0-9]{32}$");
    }
```

- [ ] **Step 3: Прогнать — упасть**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~ClusterSecret"`
Expected: FAIL (нет BackupPassword).

- [ ] **Step 4: Реализация**

`DatabaseProvisioner.cs` — рядом с ролью mover (const и guard по образцу `Role(...)`):

```csharp
    /// <summary>Роль полного бэкапа (arch/19 §7): LOGIN + REPLICATION,
    /// per-cluster пароль /clusters/&lt;C&gt;/backup_password (ensure t02).</summary>
    public const string BackupExecRole = "backup_exec";

    // gexec-guard роли backup_exec (t02 G2): SELECT возвращает CREATE ROLE,
    /// если её нет; пароль выравнивается при создании/ротации.
    public static string BuildBackupExecRoleGuardSql(string password)
        => Role(BackupExecRole, password, replication: true);
```

`ClusterSecretEnsurer.cs`: `RawSecrets` + `string? BackupPassword`; чтение шестого ключа в `ReadAsync` (по образцу цепочки); `IsComplete` + проверка; `AddIfAbsent(BackupKey(cluster), current.BackupPassword ?? AppSecretGenerator.Generate(), ...)`; `ToCredentials` + `s.BackupPassword!`; ключ:

```csharp
    private static string BackupKey(string cluster) => $"/clusters/{cluster}/backup_password";
```

`ClusterCredentials` — четвёртое поле `string BackupPassword` (doc-комментарий: тройка t02 + backup_exec t02-backups, arch/14 §4 / arch/19 §7). Диагностическое сообщение `EnsureAsync` дополнить `backup_password: {final.Value.BackupPassword is not null}`.

`ClusterSecretRotator.cs`:
- R2: `var newBackupPassword = AppSecretGenerator.Generate();` рядом с остальными NEW; тройной ALTER-цикл не меняется. ВНУТРИ цикла по шардам ПОСЛЕ тройного ALTER — backup_exec через гвард (finding 1: роли может не быть при `Enabled=false` — голый ALTER упал бы, заявка не закрылась бы никогда):

```csharp
            // backup_exec (t02, arch/19 §7): роли может не быть (подсистема
            // бэкапов выключена — G2 не выполнялся) → gexec-гвард вместо голого
            // ALTER: нет роли — CREATE с NEW-паролем; есть — ALTER (идемпотентная
            // перезапись, семантика «четвёртого ALTER» сохранена, spec §3.4).
            var backupGuard = await db.ExecuteScalarAsync(
                dsn, DatabaseProvisioner.BuildBackupExecRoleGuardSql(newBackupPassword), ct);
            if (!backupGuard.IsSuccess)
                return await FailAsync(cluster, backupGuard.Error!, $"alter/{shard.Name}", ct);
            if (backupGuard.Value is string createBackupRole)
            {
                var created = await db.ExecuteAsync(dsn, createBackupRole, ct);
                if (!created.IsSuccess)
                    return await FailAsync(cluster, created.Error!, $"alter/{shard.Name}", ct);
            }
            else
            {
                var alteredBackup = await db.ExecuteAsync(
                    dsn,
                    DatabaseProvisioner.BuildAlterRolePasswordSql(DatabaseProvisioner.BackupExecRole, newBackupPassword),
                    ct);
                if (!alteredBackup.IsSuccess)
                    return await FailAsync(cluster, alteredBackup.Error!, $"alter/{shard.Name}", ct);
            }
```

- R3: `compares.Add(TxnCompare.ValueEqual(BackupKey(cluster), creds.BackupPassword));` и `ops.Add(new TxnOp.Put(BackupKey(cluster), newBackupPassword, null));` (+ приватный `BackupKey` как у Ensurer).

- [ ] **Step 5: Прогнать юниты** — весь `PgWorker.UnitTests` зелёный (Consumers `ClusterCredentials` читают поля — конструирует только Ensurer).

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug`
Expected: PASS.

- [ ] **Step 6: Коммит**

```bash
git add src/PgWorker.Provisioning src/tests/PgWorker.UnitTests/Provisioning
git commit -m "feat(t02): секреты backup_password/backup_exec — ensure + ротация (arch/19 §7)"
```

**Выход:** per-cluster пароль бэкап-роли с ensure/ротацией; spec §3.4, arch/19 §7.

---

### Task 6: Проект `PgWorker.Backups` + `BackupPlanner` (чистые решения)

**Files:**
- Create: `src/PgWorker.Backups/PgWorker.Backups.csproj`
- Create: `src/PgWorker.Backups/Options.cs` (`BackupsRuntimeOptions`)
- Create: `src/PgWorker.Backups/Process/BackupPlanner.cs`
- Modify: `src/PgWorker.slnx` (папка `/backups/`)
- Modify: `src/PgWorker.App/Options.cs` (`BackupsOptions.ToRuntime()`)
- Test: `src/tests/PgWorker.UnitTests/Backups/BackupPlannerTests.cs` (новый каталог)

**Interfaces:**
- Consumes: `FullBackupState`/`BackupPolicy` из `PgWorker.Etcd.Parsing` (Task 3).
- Produces (используют Task 11-12):
  - `BackupsRuntimeOptions(Enabled, FullMaxAgeSec, VerifyOnCreate, S3Endpoint, S3Region, S3Bucket, S3AccessKey, S3SecretKey, JobImage, RetryBaseSec, RetryMaxSec, StagingDir, StagingQuotaBytes, AgentCpu, AgentMem)`
  - `BackupPlanner.HasActive(fulls)`, `IsDue(fulls, fullMaxAgeSec, nowUnix)`, `BackoffPassed(fulls, baseSec, maxSec, nowUnix)`, `NextId(existingIds, nowUtc)`

- [ ] **Step 1: Каркас проекта** — `PgWorker.Backups.csproj` (копия структуры `PgWorker.Moves.csproj`, без Npgsql):

```xml
<Project Sdk="Microsoft.NET.Sdk">

    <!-- Зависимости как у PgWorker.Moves: ProcessOutcome/ISqlExecutor — в
         Provisioning; etcd-модель бэкапов — в Etcd; docker-спека — в Docker. -->
    <ItemGroup>
        <ProjectReference Include="..\PgWorker.Core\PgWorker.Core.csproj"/>
        <ProjectReference Include="..\PgWorker.Etcd\PgWorker.Etcd.csproj"/>
        <ProjectReference Include="..\PgWorker.Docker\PgWorker.Docker.csproj"/>
        <ProjectReference Include="..\PgWorker.Provisioning\PgWorker.Provisioning.csproj"/>
    </ItemGroup>

</Project>
```

`slnx` — после папки `/moves/`:

```xml
    <Folder Name="/backups/">
        <Project Path="PgWorker.Backups/PgWorker.Backups.csproj" />
    </Folder>
```

Сборка проекта подхватывает `Directory.Build.props`/`Directory.Packages.props` из `src/` автоматически.

- [ ] **Step 2: Тесты планировщика (падают)** — `src/tests/PgWorker.UnitTests/Backups/BackupPlannerTests.cs`:

```csharp
using System.Globalization;
using PgWorker.Backups;
using PgWorker.Etcd.Parsing;

namespace PgWorker.UnitTests.Backups;

// Чистые решения планировщика полных бэкапов (arch/19 §2, t02): due по
// возрасту последнего COMPLETED, бэкофф min(Base·2^(n−1), Max), инвариант
// одного активного, суффикс коллизии id.
public class BackupPlannerTests
{
    private static readonly DateTime Now = new(2026, 9, 10, 3, 0, 0, DateTimeKind.Utc);

    private static long Unix(DateTime t)
        => new DateTimeOffset(t).ToUnixTimeSeconds();

    private static FullBackupState Full(string id, FullBackupStatus state, long started, long? finished = null)
        => new(id, state, "n1", BackupSourceRole.Replica, started, finished, null, null, null, null);

    // AAA: due — нет COMPLETED (первое включение/все FAILED)
    [Fact]
    public void IsDue_NoCompleted_True()
    {
        // Arrange — только FAILED-история
        var fulls = new[] { Full("20260909030000Z", FullBackupStatus.Failed, Unix(Now.AddDays(-1))) };

        // Act / Assert
        BackupPlanner.IsDue(fulls, 86400, Unix(Now)).Should().BeTrue();
    }

    // AAA: due — последний COMPLETED старше full_max_age_sec
    [Fact]
    public void IsDue_LastCompletedOlderThanMaxAge_True()
    {
        // Arrange — COMPLETED сутки+минуту назад при пороге 86400
        var fulls = new[] { Full("20260909025500Z", FullBackupStatus.Completed, Unix(Now.AddDays(-1).AddMinutes(-5)), Unix(Now.AddDays(-1).AddMinutes(-1))) };

        // Act / Assert
        BackupPlanner.IsDue(fulls, 86400, Unix(Now)).Should().BeTrue();
    }

    // AAA: не due — свежий COMPLETED (finished_unix внутри окна)
    [Fact]
    public void IsDue_FreshCompleted_False()
    {
        // Arrange — завершён час назад
        var fulls = new[] { Full("20260910020000Z", FullBackupStatus.Completed, Unix(Now.AddHours(-1).AddMinutes(-5)), Unix(Now.AddHours(-1))) };

        // Act / Assert
        BackupPlanner.IsDue(fulls, 86400, Unix(Now)).Should().BeFalse();
    }

    // AAA: бэкофф — первая неудача ждёт BaseSec с последней попытки
    [Fact]
    public void Backoff_FirstFailure_WaitsBaseSec()
    {
        // Arrange — один FAILED 100 c назад, Base=300
        var fulls = new[]
        {
            Full("20260908030000Z", FullBackupStatus.Completed, Unix(Now.AddDays(-2)), Unix(Now.AddDays(-2).AddMinutes(5))),
            Full("20260910025820Z", FullBackupStatus.Failed, Unix(Now) - 100),
        };

        // Act / Assert — 100 < 300: гвард держит; сквозь 300 c — отпускает.
        BackupPlanner.BackoffPassed(fulls, 300, 3600, Unix(Now)).Should().BeFalse();
        BackupPlanner.BackoffPassed(fulls, 300, 3600, Unix(Now) + 201).Should().BeTrue();
    }

    // AAA: бэкофф — экспонента с капом MaxSec
    [Fact]
    public void Backoff_ExponentialCappedAtMax()
    {
        // Arrange — 5 FAILED после последнего COMPLETED, последняя 100 c назад:
        // ожидание min(300·2^4, 3600) = 3600.
        var fulls = new[]
        {
            Full("20260905030000Z", FullBackupStatus.Completed, Unix(Now.AddDays(-5)), Unix(Now.AddDays(-5).AddMinutes(5))),
        };
        for (var i = 1; i <= 5; i++)
            fulls = fulls.Append(Full($"2026090{i}030000Z", FullBackupStatus.Failed, Unix(Now.AddDays(-5).AddHours(i)))).ToArray();
        fulls = fulls.Append(Full("20260910025820Z", FullBackupStatus.Failed, Unix(Now) - 100)).ToArray();

        // Act / Assert — 100 < 3600: держит; через 3501 c — отпускает.
        BackupPlanner.BackoffPassed(fulls, 300, 3600, Unix(Now)).Should().BeFalse();
        BackupPlanner.BackoffPassed(fulls, 300, 3600, Unix(Now) + 3501).Should().BeTrue();
    }

    // AAA: бэкофф — нет FAILED после COMPLETED: попытка не откладывается
    [Fact]
    public void Backoff_NoFailures_PassesImmediately()
    {
        // Arrange — свежих FAILED нет
        var fulls = new[] { Full("20260908030000Z", FullBackupStatus.Completed, Unix(Now.AddDays(-2)), Unix(Now.AddDays(-2).AddMinutes(5))) };

        // Act / Assert
        BackupPlanner.BackoffPassed(fulls, 300, 3600, Unix(Now)).Should().BeTrue();
    }

    // AAA: инвариант — максимум один активный (PLANNED/RUNNING/UPLOADING)
    [Theory]
    [InlineData(FullBackupStatus.Planned, true)]
    [InlineData(FullBackupStatus.Running, true)]
    [InlineData(FullBackupStatus.Uploading, true)]
    [InlineData(FullBackupStatus.Completed, false)]
    [InlineData(FullBackupStatus.Failed, false)]
    [InlineData(FullBackupStatus.Deleting, false)]
    public void HasActive_ByState(FullBackupStatus state, bool expected)
    {
        // Arrange / Act / Assert
        BackupPlanner.HasActive([Full("id", state, Unix(Now))]).Should().Be(expected);
    }

    // AAA: id — YYYYMMDDHHMMSSZ UTC; коллизия в пределах шарда → суффикс
    [Fact]
    public void NextId_Collision_SuffixIncrement()
    {
        // Arrange — id этой секунды уже занят (и -2 тоже)
        var baseId = Now.ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture) + "Z";
        var existing = new[] { baseId, baseId + "-2", "20260908030000Z" };

        // Act
        var next = BackupPlanner.NextId(existing, Now);

        // Assert
        next.Should().Be(baseId + "-3");
    }

    [Fact]
    public void NextId_NoCollision_PlainTimestamp()
    {
        // Arrange — секунда свободна
        var existing = new[] { "20260908030000Z" };

        // Act
        var next = BackupPlanner.NextId(existing, Now);

        // Assert
        next.Should().Be("20260910030000Z");
    }
}
```

- [ ] **Step 3: Прогнать — упасть**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~BackupPlannerTests"`
Expected: FAIL (нет проекта/типа).

- [ ] **Step 4: Реализация** — `src/PgWorker.Backups/Process/BackupPlanner.cs`:

```csharp
using System.Globalization;
using PgWorker.Etcd.Parsing;

namespace PgWorker.Backups;

// Чистые решения планировщика полных бэкапов (arch/19 §2, t02): без I/O,
// юнит-тестируемы. Записи шарда отсортированы по Id (= время, парсер t01).
public static class BackupPlanner
{
    public static bool HasActive(IReadOnlyList<FullBackupState> fulls)
        => fulls.Any(f => f.State
            is FullBackupStatus.Planned or FullBackupStatus.Running or FullBackupStatus.Uploading);

    // Rolling-правило (t02): нет COMPLETED ИЛИ возраст последнего COMPLETED
    // (finished_unix; толерантно started_unix) больше full_max_age_sec.
    public static bool IsDue(IReadOnlyList<FullBackupState> fulls, long fullMaxAgeSec, long nowUnix)
    {
        var lastCompleted = fulls
            .Where(f => f.State == FullBackupStatus.Completed)
            .OrderByDescending(f => f.StartedUnix)
            .FirstOrDefault();
        if (lastCompleted is null)
            return true;

        var finished = lastCompleted.FinishedUnix ?? lastCompleted.StartedUnix;
        return nowUnix - finished > fullMaxAgeSec;
    }

    // Бэкофф переснятия FAILED: n = FAILED с последнего COMPLETED; окно =
    // min(BaseSec·2^(n−1), MaxSec) от последней попытки (max started_unix
    // после последнего COMPLETED); n = 0 → без задержки.
    public static bool BackoffPassed(
        IReadOnlyList<FullBackupState> fulls, int baseSec, int maxSec, long nowUnix)
    {
        var lastCompletedStarted = fulls
            .Where(f => f.State == FullBackupStatus.Completed)
            .Select(f => (long?)f.StartedUnix)
            .Max() ?? long.MinValue;
        var tail = fulls.Where(f => f.StartedUnix > lastCompletedStarted).ToList();
        var failures = tail.Count(f => f.State == FullBackupStatus.Failed);
        if (failures == 0)
            return true;

        var lastAttempt = tail.Max(f => f.StartedUnix);
        var delaySec = Math.Min((long)baseSec << Math.Min(failures - 1, 30), maxSec);
        return nowUnix >= lastAttempt + delaySec;
    }

    // id = YYYYMMDDHHMMSSZ UTC (сортируемый); коллизия в пределах шарда → -2/-3…
    public static string NextId(IEnumerable<string> existingIds, DateTime nowUtc)
    {
        var baseId = nowUtc.ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture) + "Z";
        if (!existingIds.Contains(baseId))
            return baseId;

        for (var suffix = 2; ; suffix++)
        {
            var candidate = $"{baseId}-{suffix.ToString(CultureInfo.InvariantCulture)}";
            if (!existingIds.Contains(candidate))
                return candidate;
        }
    }
}
```

`src/PgWorker.Backups/Options.cs`:

```csharp
namespace PgWorker.Backups;

/// <summary>Runtime-опции подсистемы бэкапов (arch/19 §9, t02): склейка секции
/// PgWorker:Backups (маппер BackupsOptions.ToRuntime() в App; образец
/// MovesRuntimeOptions). S3-креды — per-install, env воркера, дальше — env
/// джоба (в etcd/логи не попадают).</summary>
public sealed record BackupsRuntimeOptions(
    bool Enabled = false,
    long FullMaxAgeSec = 86400,
    bool VerifyOnCreate = true,
    string S3Endpoint = "",
    string? S3Region = null,
    string S3Bucket = "",
    string S3AccessKey = "",
    string S3SecretKey = "",
    string JobImage = "pgworker-backup:dev",
    int RetryBaseSec = 300,
    int RetryMaxSec = 3600,
    string StagingDir = "/backup-staging",
    long? StagingQuotaBytes = null,
    double? AgentCpu = null,
    long? AgentMem = null);
```

`App/Options.cs` — метод в `BackupsOptions`:

```csharp
    /// <summary>Runtime-опции подсистемы бэкапов: склейка Backups-секции (t02).</summary>
    public BackupsRuntimeOptions ToRuntime() => new(
        Enabled, Policy.FullMaxAgeSec, Policy.VerifyOnCreate,
        S3.Endpoint, S3.Region, S3.Bucket, S3.AccessKey, S3.SecretKey,
        Job.Image, Retry.BaseSec, Retry.MaxSec,
        Staging.Dir, Staging.QuotaBytes, Agent.Cpu, Agent.Mem);
```

(+ `using PgWorker.Backups;` в шапке `App/Options.cs`.)

- [ ] **Step 5: Прогнать — зелёно**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~BackupPlannerTests"`
Expected: PASS (9 тестов).

- [ ] **Step 6: Коммит**

```bash
git add src/PgWorker.Backups src/PgWorker.slnx src/PgWorker.App/Options.cs src/tests/PgWorker.UnitTests/Backups
git commit -m "feat(t02): проект PgWorker.Backups + BackupPlanner (due/бэкофф/id)"
```

**Выход:** чистое ядро решений планировщика; spec §3.1 (rolling-правило, бэкофф, инвариант, id-коллизии), §4.

---

### Task 7: `BackupNames` + `BackupJobSpec` + `BackupJobLog` + сериализатор статусов

**Files:**
- Create: `src/PgWorker.Backups/BackupNames.cs`
- Create: `src/PgWorker.Backups/Job/BackupJobSpec.cs`
- Create: `src/PgWorker.Backups/Job/BackupJobLog.cs`
- Create: `src/PgWorker.Backups/Job/BackupStatusJson.cs`
- Test: `src/tests/PgWorker.UnitTests/Backups/BackupJobSpecTests.cs`, `BackupJobLogTests.cs`, `BackupStatusJsonTests.cs`

**Interfaces:**
- Consumes: `ContainerSpec` (Task 4), `BackupsRuntimeOptions` (Task 6), `FullBackupState` (Task 3).
- Produces (используют Task 11-12, 10):
  - `BackupNames.ClusterPrefix/FullKey/ContainerName/VolumeName/JobContainerPrefix/JobVolumePrefix`
  - `BackupJobSpec.Build(opts, cluster, shard, id, source, backupPassword)` → `ContainerSpec`
  - `BackupJobLog.Parse(logs)` → `BackupJobMarkers(Phase, WalStartSegment, Result)`, `BackupJobResult(Ok, WalStartSegment, SizeBytes, Error)`
  - `BackupStatusJson.Serialize(FullBackupState)` → JSON канона (round-trip через `BackupsParser`)

- [ ] **Step 1: Тесты (падают)** — три файла в `src/tests/PgWorker.UnitTests/Backups/`:

`BackupJobSpecTests.cs`:

```csharp
using PgWorker.Backups;
using PgWorker.Backups.Job;
using PgWorker.Core.Model;

namespace PgWorker.UnitTests.Backups;

// Спецификация джоба полного бэкапа (arch/19 §2, t02): детерминированные
// имена, env-контракт образа, tmpfs-квота против named volume.
public class BackupJobSpecTests
{
    private readonly BackupsRuntimeOptions _opts = new()
    {
        Enabled = true,
        S3Endpoint = "http://host.docker.internal:9000",
        S3Bucket = "pgworker-backups",
        S3AccessKey = "ak",
        S3SecretKey = "sk",
        JobImage = "pgworker-backup:dev",
        StagingDir = "/backup-staging",
    };

    // AAA: имена контейнера/volume детерминированы (takeover-супервизия)
    [Fact]
    public void Names_Deterministic()
    {
        // Arrange / Act / Assert
        BackupNames.ContainerName("demo", "s1", "20260910030000Z")
            .Should().Be("pgw-backup-full-demo-s1-20260910030000Z");
        BackupNames.VolumeName("demo", "s1", "20260910030000Z")
            .Should().Be("pgw-backup-demo-s1-20260910030000Z");
        BackupNames.FullKey("demo", "s1", "20260910030000Z")
            .Should().Be("/pgworker/backups/demo/s1/full/20260910030000Z");
    }

    // AAA: env джоба — полный контракт образа (DSN от backup_exec, S3, prefix)
    [Fact]
    public void Build_Env_DsnS3Prefix()
    {
        // Arrange — источник: advertised-хост реплики из portalloc.
        var source = new NodeAddress("host.docker.internal", new NodePorts(15432, 18432, 16932));

        // Act
        var spec = BackupJobSpec.Build(_opts, "demo", "s1", "20260910030000Z", source, "pw");

        // Assert
        spec.Image.Should().Be("pgworker-backup:dev");
        spec.Hostname.Should().Be("pgw-backup-full-demo-s1-20260910030000Z");
        spec.Ports.Should().BeEmpty("джоб — клиент без публикации портов");
        spec.Env["PGW_BK_DSN"].Should().Be(
            "host=host.docker.internal port=15432 user=backup_exec password=pw sslmode=require");
        spec.Env["PGW_BK_ID"].Should().Be("20260910030000Z");
        spec.Env["PGW_BK_S3_ENDPOINT"].Should().Be("http://host.docker.internal:9000");
        spec.Env["PGW_BK_S3_BUCKET"].Should().Be("pgworker-backups");
        spec.Env["PGW_BK_S3_ACCESS_KEY"].Should().Be("ak");
        spec.Env["PGW_BK_S3_SECRET_KEY"].Should().Be("sk");
        spec.Env["PGW_BK_PREFIX"].Should().Be("demo/s1");
        spec.Env["PGW_BK_STAGING_DIR"].Should().Be("/backup-staging");
        spec.ExtraHosts.Should().Contain("host.docker.internal:host-gateway");
        spec.Label.Should().Be("demo");
    }

    // AAA: квота задана → tmpfs size= (жёсткий ENOSPC), без named volume
    [Fact]
    public void Build_QuotaSet_TmpfsWithoutVolume()
    {
        // Arrange
        var opts = _opts with { StagingQuotaBytes = 1024 * 1024 * 1024 };

        // Act
        var spec = BackupJobSpec.Build(opts, "demo", "s1", "id1",
            new NodeAddress("h", new NodePorts(5432, 8008, 6432)), "pw");

        // Assert
        spec.Tmpfs.Should().ContainKey("/backup-staging")
            .WhoseValue.Should().Be("size=1073741824");
        spec.VolumeName.Should().BeEmpty();
    }

    // AAA: квота null → named volume на диске (реактивный ENOSPC)
    [Fact]
    public void Build_NoQuota_NamedVolume()
    {
        // Arrange / Act
        var spec = BackupJobSpec.Build(_opts, "demo", "s1", "id1",
            new NodeAddress("h", new NodePorts(5432, 8008, 6432)), "pw");

        // Assert
        spec.Tmpfs.Should().BeNull();
        spec.VolumeName.Should().Be("pgw-backup-demo-s1-id1");
        spec.VolumeDest.Should().Be("/backup-staging");
    }
}
```

`BackupJobLogTests.cs`:

```csharp
using PgWorker.Backups.Job;

namespace PgWorker.UnitTests.Backups;

// Парсер stdout джоба (arch/19 §2): маркеры фаз/result JSON, незнакомые
// строки лога игнорируются (толерантность протокола).
public class BackupJobLogTests
{
    // AAA: полный лог — фаза uploading + result ok
    [Fact]
    public void Parse_PhasesAndResult()
    {
        // Arrange — реальный шум pg_basebackup + маркеры
        var logs = """
            {"phase":"basebackup"}
            12345678/99999999 kB (100%), tablespace 0
            {"phase":"uploading","wal_start_segment":"000000010000000000000042"}
            mc: copied 12 objects
            {"ok":true,"wal_start_segment":"000000010000000000000042","size_bytes":1048576}
            """;

        // Act
        var markers = BackupJobLog.Parse(logs);

        // Assert
        markers.Phase.Should().Be("uploading");
        markers.WalStartSegment.Should().Be("000000010000000000000042");
        markers.Result.Should().NotBeNull();
        markers.Result!.Ok.Should().BeTrue();
        markers.Result.WalStartSegment.Should().Be("000000010000000000000042");
        markers.Result.SizeBytes.Should().Be(1048576);
        markers.Result.Error.Should().BeNull();
    }

    // AAA: провал — result ok=false с ошибкой
    [Fact]
    public void Parse_FailureResult()
    {
        // Arrange / Act
        var markers = BackupJobLog.Parse("{\"ok\":false,\"error\":\"staging: no space left on device\"}\n");

        // Assert
        markers.Result.Should().NotBeNull();
        markers.Result!.Ok.Should().BeFalse();
        markers.Result.Error.Should().Be("staging: no space left on device");
    }

    // AAA: мусорные/незнакомые строки — не ошибка, маркеров нет
    [Fact]
    public void Parse_OnlyNoise_NoMarkers()
    {
        // Arrange / Act
        var markers = BackupJobLog.Parse("not json\n{\"unknown\":1}\nplain text");

        // Assert
        markers.Phase.Should().BeNull();
        markers.Result.Should().BeNull();
        markers.WalStartSegment.Should().BeNull();
    }
}
```

`BackupStatusJsonTests.cs`:

```csharp
using PgWorker.Backups.Job;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Parsing;

namespace PgWorker.UnitTests.Backups;

// Сериализация статуса полного в JSON канона (arch/19 §4): воркер —
// единственный писатель; round-trip через BackupsParser.
public class BackupStatusJsonTests
{
    // AAA: PLANNED без wal_start — опциональные поля отсутствуют
    [Fact]
    public void Serialize_Planned_RoundTrip()
    {
        // Arrange
        var state = new FullBackupState(
            "20260910030000Z", FullBackupStatus.Planned, "pgw-c-s1-b", BackupSourceRole.Replica,
            1757463600, null, null, null, null, null);

        // Act
        var json = BackupStatusJson.Serialize(state);
        var parsed = BackupsParser.Parse(
            (IReadOnlyList<Kv>)[new Kv(BackupNames.FullKey("c", "s1", state.Id), json, 1)], out var errors);

        // Assert — поля канона на месте, лишних нет.
        errors.Should().BeEmpty();
        var full = parsed.Value[0].Shards["s1"].Full.Should().ContainSingle().Subject;
        full.State.Should().Be(FullBackupStatus.Planned);
        full.Node.Should().Be("pgw-c-s1-b");
        full.Role.Should().Be(BackupSourceRole.Replica);
        full.StartedUnix.Should().Be(1757463600);
        full.WalStartSegment.Should().BeNull();
        full.Verify.Should().BeNull();
        json.Should().NotContain("wal_start_segment").And.NotContain("finished_unix");
    }

    // AAA: COMPLETED — все итоговые поля + verify PENDING
    [Fact]
    public void Serialize_CompletedWithVerify_RoundTrip()
    {
        // Arrange
        var state = new FullBackupState(
            "20260910030000Z", FullBackupStatus.Completed, "pgw-c-s1-a", BackupSourceRole.Master,
            1757463600, 1757464000, "000000010000000000000042", 1048576, null,
            new BackupVerify(BackupVerifyStatus.Pending, null));

        // Act
        var json = BackupStatusJson.Serialize(state);
        var parsed = BackupsParser.Parse(
            (IReadOnlyList<Kv>)[new Kv(BackupNames.FullKey("c", "s1", state.Id), json, 1)], out var errors);

        // Assert
        errors.Should().BeEmpty();
        var full = parsed.Value[0].Shards["s1"].Single();
        full.State.Should().Be(FullBackupStatus.Completed);
        full.FinishedUnix.Should().Be(1757464000);
        full.WalStartSegment.Should().Be("000000010000000000000042");
        full.SizeBytes.Should().Be(1048576);
        full.Verify!.State.Should().Be(BackupVerifyStatus.Pending);
        json.Should().Contain("\"state\":\"COMPLETED\"").And.Contain("\"role\":\"master\"");
    }

    // AAA: FAILED — error, без wal/size
    [Fact]
    public void Serialize_FailedWithError_RoundTrip()
    {
        // Arrange
        var state = new FullBackupState(
            "20260910030000Z", FullBackupStatus.Failed, "pgw-c-s1-b", BackupSourceRole.Replica,
            1757463600, 1757463700, null, null, "pg_basebackup failed: connection reset", null);

        // Act
        var json = BackupStatusJson.Serialize(state);
        var parsed = BackupsParser.Parse(
            (IReadOnlyList<Kv>)[new Kv(BackupNames.FullKey("c", "s1", state.Id), json, 1)], out var errors);

        // Assert
        errors.Should().BeEmpty();
        parsed.Value[0].Shards["s1"].Single().Error.Should().Be("pg_basebackup failed: connection reset");
    }
}
```

- [ ] **Step 2: Прогнать — упасть**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~BackupJobSpecTests|FullyQualifiedName~BackupJobLogTests|FullyQualifiedName~BackupStatusJsonTests"`
Expected: FAIL компиляция.

- [ ] **Step 3: Реализация**

`BackupNames.cs`:

```csharp
namespace PgWorker.Backups;

// Канонические имена подсистемы бэкапов (arch/19 §2/§4): ключи etcd пишет
// ТОЛЬКО воркер под клэймом <C>; имена docker-объектов детерминированы —
// takeover-инстанс находит джоб по имени.
public static class BackupNames
{
    public static string ClusterPrefix(string cluster) => $"/pgworker/backups/{cluster}/";

    public static string FullKey(string cluster, string shard, string id)
        => $"/pgworker/backups/{cluster}/{shard}/full/{id}";

    public static string ContainerName(string cluster, string shard, string id)
        => $"pgw-backup-full-{cluster}-{shard}-{id}";

    public static string VolumeName(string cluster, string shard, string id)
        => $"pgw-backup-{cluster}-{shard}-{id}";

    // Префиксы чистки Deprovisioning (D2, t02).
    public static string JobContainerPrefix(string cluster) => $"pgw-backup-full-{cluster}-";

    public static string JobVolumePrefix(string cluster) => $"pgw-backup-{cluster}-";
}
```

`Job/BackupJobSpec.cs`:

```csharp
using PgWorker.Backups;
using PgWorker.Core.Model;
using PgWorker.Docker.Engine;

namespace PgWorker.Backups.Job;

// Спецификация ephemeral-джоба полного бэкапа (arch/19 §2, t02): env-контракт
// образа pgworker-backup; пароль per-cluster — ТОЛЬКО env контейнера (не в
// статусы/логи). Квота staging задана → tmpfs size= (жёсткий ENOSPC); null →
// named volume (docker создаст при первом mount).
public static class BackupJobSpec
{
    public static ContainerSpec Build(
        BackupsRuntimeOptions opts, string cluster, string shard, string id,
        NodeAddress source, string backupPassword)
    {
        var env = new Dictionary<string, string>
        {
            ["PGW_BK_DSN"] =
                $"host={source.Host} port={source.Ports.Pg} user=backup_exec password={backupPassword} sslmode=require",
            ["PGW_BK_ID"] = id,
            ["PGW_BK_S3_ENDPOINT"] = opts.S3Endpoint,
            ["PGW_BK_S3_REGION"] = opts.S3Region ?? string.Empty,
            ["PGW_BK_S3_BUCKET"] = opts.S3Bucket,
            ["PGW_BK_S3_ACCESS_KEY"] = opts.S3AccessKey,
            ["PGW_BK_S3_SECRET_KEY"] = opts.S3SecretKey,
            ["PGW_BK_PREFIX"] = $"{cluster}/{shard}",
            ["PGW_BK_STAGING_DIR"] = opts.StagingDir,
        };
        return new ContainerSpec(
            Image: opts.JobImage,
            Env: env,
            VolumeName: opts.StagingQuotaBytes is null ? BackupNames.VolumeName(cluster, shard, id) : "",
            VolumeDest: opts.StagingDir,
            Ports: [],
            Hostname: BackupNames.ContainerName(cluster, shard, id),
            CpuCores: opts.AgentCpu,
            MemoryBytes: opts.AgentMem,
            Label: cluster,
            Cmd: null,
            Network: null,
            NetworkAliases: null,
            Tmpfs: opts.StagingQuotaBytes is { } quota
                ? new Dictionary<string, string> { [opts.StagingDir] = $"size={quota}" }
                : null,
            ExtraHosts: ["host.docker.internal:host-gateway"]);
    }
}
```

`Job/BackupJobLog.cs`:

```csharp
using System.Text.Json;

namespace PgWorker.Backups.Job;

/// <summary>Result-JSON джоба: {"ok":true,"wal_start_segment":…,"size_bytes":…}
/// либо {"ok":false,"error":…} (arch/19 §2).</summary>
public sealed record BackupJobResult(bool Ok, string? WalStartSegment, long? SizeBytes, string? Error);

/// <summary>Маркеры stdout джоба на момент поллинга: последняя фаза, её
/// wal_start_segment и финальный result (null — ещё не напечатан).</summary>
public sealed record BackupJobMarkers(string? Phase, string? WalStartSegment, BackupJobResult? Result);

// Парсер stdout-протокола джоба: воркер разбирает ТОЛЬКО свои JSON-строки,
// незнакомые строки (шум pg_basebackup/mc) игнорирует (arch/19 §2/§10).
public static class BackupJobLog
{
    public static BackupJobMarkers Parse(string logs)
    {
        string? phase = null;
        string? walStart = null;
        BackupJobResult? result = null;
        foreach (var line in logs.Split('\n'))
        {
            var trimmed = line.Trim();
            if (!trimmed.StartsWith('{') || !trimmed.EndsWith('}'))
                continue;
            try
            {
                using var doc = JsonDocument.Parse(trimmed);
                var root = doc.RootElement;
                if (root.ValueKind != JsonValueKind.Object)
                    continue;

                if (root.TryGetProperty("phase", out var phaseEl)
                    && phaseEl.ValueKind == JsonValueKind.String)
                {
                    phase = phaseEl.GetString();
                    if (root.TryGetProperty("wal_start_segment", out var walEl)
                        && walEl.ValueKind == JsonValueKind.String)
                        walStart = walEl.GetString();
                }

                if (root.TryGetProperty("ok", out var okEl) && okEl.ValueKind is JsonValueKind.True or JsonValueKind.False)
                    result = new BackupJobResult(
                        okEl.ValueKind == JsonValueKind.True,
                        GetString(root, "wal_start_segment"),
                        GetLong(root, "size_bytes"),
                        GetString(root, "error"));
            }
            catch (JsonException)
            {
                // чужая JSON-подобная строка — не наш контракт
            }
        }

        return new BackupJobMarkers(phase, walStart, result);
    }

    private static string? GetString(JsonElement root, string name)
        => root.TryGetProperty(name, out var e) && e.ValueKind == JsonValueKind.String ? e.GetString() : null;

    private static long? GetLong(JsonElement root, string name)
        => root.TryGetProperty(name, out var e) && e.ValueKind == JsonValueKind.Number && e.TryGetInt64(out var v) ? v : null;
}
```

`Job/BackupStatusJson.cs`:

```csharp
using System.Text.Json;
using PgWorker.Etcd.Parsing;

namespace PgWorker.Backups.Job;

// Сериализация статуса полного в JSON канона /pgworker/backups/<C>/<X>/full/<id>
// (arch/19 §4): пишет ТОЛЬКО воркер (держатель клэйма); поля состояния —
/// по факту (null/опциональные не сериализуем).
public static class BackupStatusJson
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);

    public static string Serialize(FullBackupState state)
    {
        var o = new Dictionary<string, object?>
        {
            ["state"] = StateName(state.State),
            ["node"] = state.Node,
            ["role"] = state.Role == BackupSourceRole.Replica ? "replica" : "master",
            ["started_unix"] = state.StartedUnix,
        };
        if (state.FinishedUnix is { } finished)
            o["finished_unix"] = finished;
        if (state.WalStartSegment is { } wal)
            o["wal_start_segment"] = wal;
        if (state.SizeBytes is { } size)
            o["size_bytes"] = size;
        if (state.Error is { } error)
            o["error"] = error;
        if (state.Verify is { } verify)
        {
            var v = new Dictionary<string, object?> { ["state"] = VerifyName(verify.State) };
            if (verify.CheckedUnix is { } checkedUnix)
                v["checked_unix"] = checkedUnix;
            o["verify"] = v;
        }

        return JsonSerializer.Serialize(o, Options);
    }

    private static string StateName(FullBackupStatus state) => state switch
    {
        FullBackupStatus.Planned => "PLANNED",
        FullBackupStatus.Running => "RUNNING",
        FullBackupStatus.Uploading => "UPLOADING",
        FullBackupStatus.Completed => "COMPLETED",
        FullBackupStatus.Failed => "FAILED",
        _ => "DELETING",
    };

    private static string VerifyName(BackupVerifyStatus state) => state switch
    {
        BackupVerifyStatus.Pending => "PENDING",
        BackupVerifyStatus.Ok => "OK",
        _ => "FAILED",
    };
}
```

- [ ] **Step 4: Прогнать — зелёно**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~Backup"`
Expected: PASS.

- [ ] **Step 5: Коммит**

```bash
git add src/PgWorker.Backups src/tests/PgWorker.UnitTests/Backups
git commit -m "feat(t02): BackupJobSpec/BackupJobLog/сериализатор статусов — контракт воркер↔джоб"
```

**Выход:** контракты джоба и записи статусов; spec §3.2 (env, контейнер), §3.3 (воркер производит записи).

---

### Task 8: Образ `pgworker-backup` (Dockerfile + entrypoint)

**Files:**
- Create: `docker/PgWorker.Backup.Dockerfile`
- Create: `docker/backup/entrypoint.sh`

**Interfaces:**
- Produces: образ `pgworker-backup:dev` (env-контракт Task 7; потребляют Task 14 E2E как `pgworker-backup:e2e`, стенд Task 16).

- [ ] **Step 1: Запинить версию mc** — свежий стабильный релиз minio/mc:

Run: `curl -fsS https://api.github.com/repos/minio/mc/releases/latest | /usr/bin/python3 -c "import sys,json;print(json.load(sys.stdin)['tag_name'])"`
Expected: строка вида `RELEASE.YYYY-MM-DDTHH-MM-SSZ` — её подставить в `MC_VERSION` ниже (обновление mc — отдельной правкой, arch/19 §10).

- [ ] **Step 2: `docker/backup/entrypoint.sh`** (исполняемым не коммитим — права даёт Dockerfile):

```sh
#!/bin/sh
# Джоб полного бэкапа (arch/19 §2, t02): pg_basebackup → mc в S3. Протокол —
# stdout-маркеры JSON (воркер парсирует свои строки), итог — result-JSON и
# exit-код; etcd контейнеру неизвестен. Все параметры — env (PGW_BK_*).
set -u
STAGING="${PGW_BK_STAGING_DIR:-/backup-staging}"

LOG() { printf '%s\n' "$1"; }
FAIL() {
  # однострочная причина без кавычек (JSON-безопасность)
  LOG "{\"ok\":false,\"error\":\"$(printf '%s' "$1" | tr '\n' ' ' | tr -d '"')\"}"
  exit 1
}

LOG '{"phase":"basebackup"}'
rm -rf "$STAGING/full"
pg_basebackup -d "$PGW_BK_DSN" -D "$STAGING/full" -X stream --checkpoint=spread --manifest-checksums=SHA256 \
  || FAIL "pg_basebackup failed"

# wal_start_segment из backup_label: "START WAL LOCATION: ... (file <seg>)"
WAL_START="$(sed -n 's/^START WAL LOCATION: .*(file \(.*\)).*/\1/p' "$STAGING/full/backup_label" | head -n 1)"
[ -n "$WAL_START" ] || FAIL "backup_label: START WAL LOCATION not found"
SIZE_BYTES="$(du -sb "$STAGING/full" | cut -f1)"
[ -n "$SIZE_BYTES" ] || FAIL "du staging failed"

LOG "{\"phase\":\"uploading\",\"wal_start_segment\":\"$WAL_START\"}"
export MC_HOST_pgw="$PGW_BK_S3_ACCESS_KEY:$PGW_BK_S3_SECRET_KEY@$PGW_BK_S3_ENDPOINT"

# файлы pg_basebackup 1:1 (вкл. backup_manifest и pg_wal/) — arch/19 §5
mc cp --recursive "$STAGING/full/." "pgw/$PGW_BK_S3_BUCKET/$PGW_BK_PREFIX/full/$PGW_BK_ID/" \
  || FAIL "upload full failed"

# закрытые сегменты набора -X stream → общий wal/-префикс (дублирование
# канона §5; идемпотентная перезапись; .partial/.history — домен t03)
for seg in "$STAGING"/full/pg_wal/*; do
  [ -f "$seg" ] || continue
  case "$(basename "$seg")" in *.partial|*.history) continue ;; esac
  mc cp "$seg" "pgw/$PGW_BK_S3_BUCKET/$PGW_BK_PREFIX/wal/$(basename "$seg")" \
    || FAIL "upload wal failed"
done

LOG "{\"ok\":true,\"wal_start_segment\":\"$WAL_START\",\"size_bytes\":$SIZE_BYTES}"
```

- [ ] **Step 3: `docker/PgWorker.Backup.Dockerfile`**

```dockerfile
# Образ джоба полного бэкапа (arch/19 §2, t02): postgres-клиенты (pg_basebackup)
# + mc (MinIO/S3-клиент; версия запинена — supply-chain, arch/19 §10).
# Никаких .NET-компонентов: джоб — sh-скрипт, параметры только env.
# Сборка (контекст — корень репо): docker build -f docker/PgWorker.Backup.Dockerfile -t pgworker-backup:dev .
FROM postgres:18

ARG MC_VERSION=RELEASE.2025-08-13T09-26-51Z

RUN curl -fsSL -o /usr/local/bin/mc \
      "https://dl.min.io/client/mc/release/archive/mc.${MC_VERSION}" \
 && chmod +x /usr/local/bin/mc \
 && mc --version

COPY docker/backup/entrypoint.sh /usr/local/bin/pgw-backup-entrypoint
RUN chmod +x /usr/local/bin/pgw-backup-entrypoint

ENTRYPOINT ["/usr/local/bin/pgw-backup-entrypoint"]
```

(`MC_VERSION` — значение из Step 1; путь архива `release/archive/mc.<tag>` отдаёт запиненный бинарник linux-amd64.)

- [ ] **Step 4: Собрать и проверить утилиты**

Run:
```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-backup-full-daily
docker build -f docker/PgWorker.Backup.Dockerfile -t pgworker-backup:dev . \
 && docker run --rm --entrypoint sh pgworker-backup:dev -c 'pg_basebackup --version && mc --version | head -1 && du --version | head -1'
```
Expected: версии утилит печатаются, exit 0.

- [ ] **Step 5: Smoke-прогон скрипта против локального MinIO (динамический порт, без хардкодов)**

Run:
```bash
MINIO_PORT=$(docker run --rm -d -p 0:9000 --name pgw-bk-smoke-minio \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio:RELEASE.2025-09-07T16-13-09Z server /data \
  && sleep 2 && docker port pgw-bk-smoke-minio 9000 | head -1 | sed 's/.*://')
docker run --rm --name pgw-bk-smoke-mc --network host minio/mc:RELEASE.2025-08-13T09-26-51Z \
  sh -c "mc alias set s http://127.0.0.1:$MINIO_PORT minioadmin minioadmin && mc mb --ignore-existing s/pgworker-backups"
# фиктивный «бэкап» из двух файлов + сегмент pg_wal
docker run --rm --network host \
  -e PGW_BK_DSN= -e PGW_BK_ID=smoke -e PGW_BK_S3_ENDPOINT=http://127.0.0.1:$MINIO_PORT \
  -e PGW_BK_S3_BUCKET=pgworker-backups -e PGW_BK_S3_ACCESS_KEY=minioadmin -e PGW_BK_S3_SECRET_KEY=minioadmin \
  -e PGW_BK_PREFIX=smoke/s1 --entrypoint sh pgworker-backup:dev \
  -c 'mkdir -p /tmp/staging/full/pg_wal; echo data > /tmp/staging/full/PG_VERSION; \
      echo manifest > /tmp/staging/full/backup_manifest; echo seg > /tmp/staging/full/pg_wal/000000010000000000000001; \
      export PGW_BK_STAGING_DIR=/tmp/staging; export MC_HOST_pgw=minioadmin:minioadmin@http://127.0.0.1:'"$MINIO_PORT"'; \
      export PGW_BK_S3_BUCKET=pgworker-backups PGW_BK_PREFIX=smoke/s1 PGW_BK_ID=smoke; \
      wal=000000010000000000000001; mc cp --recursive /tmp/staging/full/. pgw/pgworker-backups/smoke/s1/full/smoke/ \
      && mc cp /tmp/staging/full/pg_wal/$wal pgw/pgworker-backups/smoke/s1/wal/$wal && echo SMOKE-OK'
docker run --rm --network host minio/mc:RELEASE.2025-08-13T09-26-51Z \
  sh -c "mc alias set s http://127.0.0.1:$MINIO_PORT minioadmin minioadmin >/dev/null && mc ls --recursive s/pgworker-backups/smoke/"
docker rm -f pgw-bk-smoke-minio; docker network prune -f
```
Expected: объекты `full/smoke/...` и `wal/000000010000000000000001` в листинге; после прогона контейнер MinIO удалён. (Smoke проверяет mc-часть скрипта; полный путь с pg_basebackup — в E2E Task 14.)

- [ ] **Step 6: Коммит**

```bash
git add docker/PgWorker.Backup.Dockerfile docker/backup/entrypoint.sh
git commit -m "feat(t02): образ pgworker-backup — postgres:18 + mc (запинен) + entrypoint-пайплайн"
```

**Выход:** образ джоба; spec §3.2 (пайплайн/stdout-маркеры), §8 (риск mc-версии).

---

### Task 9: Резолв источника: `PatroniMember.Sync` + `ResolveBackupSourceAsync`

**Files:**
- Modify: `src/PgWorker.Provisioning/Probes/ShardProbe.cs` (`PatroniMember` + `Sync`; парсинг)
- Modify: `src/PgWorker.Provisioning/Endpoints/ShardEndpoints.cs` (`ResolveBackupSourceAsync`)
- Test: `src/tests/PgWorker.UnitTests/Provisioning/ShardProbeTests.cs` (+кейс sync-поля)

**Interfaces:**
- Consumes: `ResolveMasterAsync`, `ShardProbe.GetClusterAsync`.
- Produces: `ShardEndpoints.ResolveBackupSourceAsync(cluster, shard, addresses, ct)` → `Result<NodeAddress?>` (sync-реплика → fallback мастер; потребляет Task 11 G3); `PatroniMember.Sync: bool?`.

- [ ] **Step 1: Тест (падает)** — в `ShardProbeTests.cs` (механика файла: `FakeHandler` + `Json(status, body)`; фикстуры — файлы в `ProbesFixtures/`). Добавить файл `src/tests/PgWorker.UnitTests/ProbesFixtures/patroni-cluster-sync.json`:

```json
{"members":[
  {"name":"s1a","role":"master","state":"running","timeline":1},
  {"name":"s1b","role":"replica","state":"running","timeline":1,"lag":0,"sync":true},
  {"name":"s1c","role":"replica","state":"streaming","timeline":1,"lag":10,"sync":false}
]}
```

и кейс:

```csharp
    private static string PatroniClusterSyncJson()
        => File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "ProbesFixtures", "patroni-cluster-sync.json"));

    // AAA: sync-статус члена (t02: выбор реплики-источника полного бэкапа)
    [Fact]
    public async Task GetCluster_SyncStandby_Parsed()
    {
        // Arrange — Patroni 3.x+ помечает sync-standby "sync": true
        var probe = new ShardProbe(new HttpClient(new FakeHandler(_ => Json(200, PatroniClusterSyncJson()))));

        // Act
        var result = await probe.GetClusterAsync(Node, CancellationToken.None);

        // Assert — bool true/false различены; отсутствие поля (старый Patroni) — null
        result.IsSuccess.Should().BeTrue();
        var sync = result.Value.Should().ContainSingle(m => m.Name == "s1b").Subject;
        sync.Sync.Should().BeTrue();
        result.Value.Single(m => m.Name == "s1c").Sync.Should().BeFalse();
        result.Value.Single(m => m.Name == "s1a").Sync.Should().BeNull();
    }
```

Существующий кейс `GetCluster_PatroniFixture_ParsesMembers` остаётся зелёным (его фикстура без `sync` → `Sync == null`, 3-аргументные ожидания эквивалентны — параметр опционален).

- [ ] **Step 2: Прогнать — упасть**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~ShardProbeTests"`
Expected: FAIL (нет поля Sync).

- [ ] **Step 3: Реализация**

`ShardProbe.cs`:

```csharp
/// <summary>
/// Член Patroni-кластера из GET /cluster: role — master|replica, state —
/// running|streaming|stopped|creating (P2.2 ожидание поднятия, надзор C);
/// Sync — sync-статус члена (t02: выбор реплики-источника полного бэкапа,
/// arch/19 §2): true/false от Patroni 3.x+, null — поле отсутствует.
/// </summary>
public sealed record PatroniMember(string Name, string Role, string State, bool? Sync = null);
```

В `GetClusterAsync` при сборке члена:

```csharp
                .Select(m => new PatroniMember(
                    m.GetProperty("name").GetString() ?? string.Empty,
                    m.GetProperty("role").GetString() ?? string.Empty,
                    m.GetProperty("state").GetString() ?? string.Empty,
                    ReadSync(m)))
```

и приватный хелпер (толерантно к строковой форме `"sync"`/`"quorum"`):

```csharp
    // sync-статус члена: bool (Patroni 3.x+) либо строка sync/quorum; нет поля — null.
    private static bool? ReadSync(JsonElement member)
    {
        if (!member.TryGetProperty("sync", out var sync))
            return null;
        return sync.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.String when sync.GetString() is "sync" or "quorum" => true,
            _ => null,
        };
    }
```

`ShardEndpoints.cs` — после `ResolveMasterAsync`:

```csharp
    // Источник полного бэкапа (t02, arch/19 §2/§6): штатно sync-standby
    // (Patroni GET /cluster: role=replica, state=running, sync-статус) —
    // мастер не трогаем; нет sync-реплики/недоступна → fallback мастер
    // (ResolveMasterAsync, журнал-факт role=master).
    public async Task<Result<NodeAddress?>> ResolveBackupSourceAsync(
        string cluster, ShardSpec shard, IReadOnlyDictionary<string, NodeAddress> addresses, CancellationToken ct)
    {
        var shardNodes = addresses
            .Where(p => p.Key.StartsWith($"{shard.Name}/", StringComparison.Ordinal))
            .ToDictionary(p => p.Key.Split('/')[1], p => p.Value);

        foreach (var node in shardNodes)
        {
            var members = await probe.GetClusterAsync(node.Value, ct);
            if (!members.IsSuccess)
                continue; // нода недоступна — пробуем следующую
            var sync = members.Value.FirstOrDefault(m =>
                m.Role == "replica" && m.State == "running" && m.Sync == true
                && shardNodes.ContainsKey(m.Name));
            if (sync is not null)
                return Result<NodeAddress?>.Success(shardNodes[sync.Name]);
        }

        return await ResolveMasterAsync(cluster, shard, addresses, ct);
    }
```

- [ ] **Step 4: Прогнать юниты Provisioning — зелёно**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug`
Expected: PASS (новый record-параметр опционален — существующие конструирования PatroniMember не ломаются).

- [ ] **Step 5: Коммит**

```bash
git add src/PgWorker.Provisioning src/tests/PgWorker.UnitTests/Provisioning
git commit -m "feat(t02): резолв источника бэкапа — sync-standby с fallback на мастер"
```

**Выход:** источник бэкапа резолвится; spec §3.1 G3, arch/19 §2/§6.

---

### Task 10: Docker-драйвер хоста (`EngineFor`/`RemoveBackupJobsAsync`) + Deprovisioning D2

**Files:**
- Modify: `src/PgWorker.Docker/Drivers/ClusterDriver.cs` (`IClusterDriver.EngineFor` + `RemoveBackupJobsAsync`; реализация Plain/Swarm)
- Modify: `src/PgWorker.Provisioning/Processes/DeprovisioningProcess.cs` (D1: удаление джобов; D2: `del --prefix /pgworker/backups/<C>/`)
- Test: `src/tests/PgWorker.UnitTests/Provisioning/DeprovisioningProcessTests.cs` (+кейс)

**Interfaces:**
- Consumes: `BackupNames.JobContainerPrefix` (Task 7).
- Produces: `IDockerEngine? IClusterDriver.EngineFor(string host)` (потребляет Task 11-12 `BackupProcess`), `Task<Result> IClusterDriver.RemoveBackupJobsAsync(string cluster, CancellationToken ct)` (потребляет Deprovisioning этого же таска).

- [ ] **Step 1: Тест (падает)** — в `DeprovisioningProcessTests.cs` по образцу существующих (там есть фейковый драйвер — расширить записью вызовов; чтение — `etcd.Store`, как в соседних кейсах):

```csharp
// AAA: D2 t02 — deprovisioning убивает джобы бэкапов и чистит их префикс etcd
[Fact]
public async Task Tick_RemovesBackupJobsAndPrefix()
{
    // Arrange — кластер в TO_REMOVE + живые ключи бэкапов; фейк-драйвер помнит
    // вызов RemoveBackupJobsAsync; фейк-etcd содержит /pgworker/backups/shop/s1/full/x.
    // (сид кластера — по образцу соседних кейсов DeprovisioningProcessTests)
    etcd.Seed("/pgworker/backups/shop/s1/full/20260910030000Z",
        "{\"state\":\"RUNNING\",\"node\":\"n\",\"role\":\"replica\",\"started_unix\":1,\"wal_start_segment\":\"s\"}");

    // Act
    var tick = await process.TickAsync(snap, TestContext.Current.CancellationToken);

    // Assert
    tick.IsSuccess.Should().BeTrue();
    fakeDriver.RemoveBackupJobsCalled.Should().BeTrue();
    etcd.Store.Should().NotContainKey("/pgworker/backups/shop/s1/full/20260910030000Z",
        "префикс бэкапов не переживает кластер");
}
```

- [ ] **Step 2: Прогнать — упасть**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~DeprovisioningProcessTests"`
Expected: FAIL.

- [ ] **Step 3: Реализация**

`IClusterDriver` — новые члены:

```csharp
    // Docker-движок хоста по имени (plain-таблица/advertised; swarm — manager):
    // джобы бэкапов создаются/супервизируются на docker-хосте источника (t02).
    // null — хост не известен (вызывающий трактует как transient).
    IDockerEngine? EngineFor(string host);

    // Чистка джоб-контейнеров pgw-backup-full-<C>-* и их staging volumes
    // (Deprovisioning D2, t02): идемпотентно, 404 = успех; объекты S3 НЕ трогаем
    // (arch/19 §4 — orphan t07).
    Task<Result> RemoveBackupJobsAsync(string cluster, CancellationToken ct);
```

`PlainClusterDriver`:

```csharp
    // advertised-режим: адрес из portalloc несёт advertised-имя — валидация
    // старта гарантирует единственный хост (как EnsureNodeAsync).
    public IDockerEngine? EngineFor(string host)
    {
        if (_engines.TryGetValue(host, out var engine))
            return engine;
        return advertisedHost is { Length: > 0 } && host == advertisedHost && _engines.Count == 1
            ? _engines.Values.Single()
            : null;
    }

    public Task<Result> RemoveBackupJobsAsync(string cluster, CancellationToken ct)
        => RemoveBackupJobsAsync(_engines.Values, cluster, ct);

    // Общая чистка: контейнеры по префиксу + volume, выводимый из имени контейнера
    // (pgw-backup-full-<C>-<X>-<id> → pgw-backup-<C>-<X>-<id>; tmpfs-джобы без volume — 404=ок).
    private static async Task<Result> RemoveBackupJobsAsync(
        IEnumerable<IDockerEngine> engines, string cluster, CancellationToken ct)
    {
        var prefix = BackupNames.JobContainerPrefix(cluster);
        foreach (var engine in engines)
        {
            var list = await engine.ListContainersAsync(prefix, all: true, ct);
            if (!list.IsSuccess)
                return list;
            foreach (var container in list.Value.Where(c => c.Names.Any(n => n.StartsWith(prefix, StringComparison.Ordinal))))
            {
                var name = container.Names.First(n => n.StartsWith(prefix, StringComparison.Ordinal));
                var removed = await engine.RemoveContainerAsync(name, force: true, ct);
                if (!removed.IsSuccess)
                    return removed;
                var volume = name.Replace(
                    BackupNames.JobContainerPrefix(cluster), BackupNames.JobVolumePrefix(cluster), StringComparison.Ordinal);
                var volumeRemoved = await engine.RemoveVolumeAsync(volume, ct);
                if (!volumeRemoved.IsSuccess)
                    return volumeRemoved;
            }
        }

        return Result.Success();
    }
```

`SwarmClusterDriver` — `EngineFor(string host)` → `_engine` (manager: джобы создаются движком manager'а); `RemoveBackupJobsAsync` → `RemoveBackupJobsAsync([_engine], cluster, ct)` (общий хелпер сделать доступным обоим классам — private static в файле). `PgWorker.Docker` ссылается на `PgWorker.Backups`? НЕТ — обратная зависимость (Backups→Docker). Префиксы `pgw-backup-full-`/`pgw-backup-` продублировать локальными константами в `ClusterDriver.cs` (паттерн локального дубля — прецедент `MoverRole` в `ShardEndpoints.cs:22-24`), комментарий: «имена канона BackupNames (PgWorker.Backups) — дубль без ссылки (цикл зависимостей)».

`DeprovisioningProcess.cs`:
- В `RemoveNodesAsync`, после сирот (перед `return Result.Success()`):

```csharp
        // Джобы бэкапов кластера (t02, arch/19 §4): убиваем до чистки etcd —
        // «мёртвые» ключи при сбое безвредны (кластер в TO_REMOVE, тик продолжит).
        var jobs = await driver.RemoveBackupJobsAsync(cluster, ct);
        if (!jobs.IsSuccess)
            return jobs;
```

- В `CleanKeysAsync`, перед `delWork`:

```csharp
        // Статусы бэкапов не переживают кластер (t02 D2; объекты S3 остаются — orphan t07).
        var delBackups = await DeleteAsync($"/pgworker/backups/{cluster}/", prefix: true, ct);
        if (!delBackups.IsSuccess)
            return delBackups;
```

- [ ] **Step 4: Прогнать юниты** — весь проект зелёный (фейки драйверов в других тестах дополняются двумя методами — заглушки: `EngineFor → null`, `RemoveBackupJobsAsync → Result.Success()`).

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug`
Expected: PASS.

- [ ] **Step 5: Коммит**

```bash
git add src/PgWorker.Docker/Drivers/ClusterDriver.cs src/PgWorker.Provisioning/Processes/DeprovisioningProcess.cs src/tests/PgWorker.UnitTests/Provisioning
git commit -m "feat(t02): Deprovisioning D2 — чистка джобов и префикса /pgworker/backups/<C>/"
```

**Выход:** бэкапы не переживают кластер; spec §3.3 п.4, arch/19 §4.

---

### Task 11: `BackupProcess` — планирование и запуск джоба (G0–G3)

**Files:**
- Create: `src/PgWorker.Backups/Process/BackupProcess.cs`
- Test: `src/tests/PgWorker.UnitTests/Backups/BackupProcessTests.cs` (кейсы G-веток)

**Interfaces:**
- Consumes: `BackupPlanner`/`BackupJobSpec`/`BackupStatusJson`/`BackupNames` (Task 6-7), `ResolveBackupSourceAsync` (Task 9), `IClusterSecretEnsurer.BackupPassword` (Task 5), `DatabaseProvisioner.BuildBackupExecRoleGuardSql` (Task 5), `BackupsParser` (t01).
- Produces: `BackupProcess.TickAsync(ClusterSnapshot snap, IReadOnlyList<ClusterBackups> backups, CancellationToken ct)` → `Task<Result<ProcessOutcome>>` (потребляет Task 13 wiring; S-ветка — Task 12).

- [ ] **Step 1: Скелет процесса** — `src/PgWorker.Backups/Process/BackupProcess.cs` (S-супервизия — `SuperviseActiveAsync`-заглушка, наполняется в Task 12):

```csharp
using Microsoft.Extensions.Logging;
using PgWorker.Backups.Job;
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
using PgWorker.Provisioning.Sql;

namespace PgWorker.Backups;

/// <summary>
/// Планировщик полных бэкапов (t02, arch/19 §2): тик под клэймом &lt;C&gt; для
/// каждого шарда Active-кластера с dsn. G0 выключен → no-op; G1 ensure
/// backup_password; на каждый шард: S-супервизия активного → G2 ensure роли
/// backup_exec на мастере (КАЖДЫЙ тик — spec §3.1, до due-гвардов: роль обязана
/// существовать до любого запуска джоба; transient-skip шарда при недоступном
/// мастере) → G3 при due: PLANNED (journal-before-manipulations) → джоб-контейнер
/// на docker-хосте источника → RUNNING. Инвариант: максимум один активный
/// (PLANNED/RUNNING/UPLOADING) на шард. Тик не блокируется на длинные операции:
/// бэкап живёт в контейнере.
/// </summary>
public sealed class BackupProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ShardEndpoints shardEndpoints,
    ISqlExecutor db,
    IClusterSecretEnsurer secrets,
    ClaimStore claims,
    WorkJournal journal,
    InstallSecrets installSecrets,
    BackupsRuntimeOptions options,
    TimeProvider time,
    ILogger<BackupProcess> logger,
    Func<CancellationToken, Task<Result>>? snapshot = null)
{
    private const string Op = "backups";

    public async Task<Result<ProcessOutcome>> TickAsync(
        ClusterSnapshot snap, IReadOnlyList<ClusterBackups> backups, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Мутации префикса /pgworker/backups/* — только держатель клэйма (arch/19 §4).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"{Op} {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        // G0: подсистема выключена — поведение воркера не меняется (no-op).
        if (!options.Enabled)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        var mine = backups.FirstOrDefault(b => b.Cluster == cluster)
                   ?? new ClusterBackups(cluster, null, new Dictionary<string, ShardBackups>());
        var fullMaxAgeSec = mine.Policy?.FullMaxAgeSec ?? options.FullMaxAgeSec;
        var verifyOnCreate = mine.Policy?.VerifyOnCreate ?? options.VerifyOnCreate;
        var nowUnix = time.GetUtcNow().ToUnixTimeSeconds();

        // G1: ensure per-cluster пароля backup_exec (P1.5-образец, t02).
        var creds = await secrets.EnsureAsync(cluster, snap.Config, ct);
        if (!creds.IsSuccess)
            return Result<ProcessOutcome>.Failed(creds.Error!);

        // Адреса нод один раз на тик (portalloc).
        var addresses = await shardEndpoints.ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            return Result<ProcessOutcome>.Failed(addresses.Error!);

        foreach (var shard in snap.Shards.Where(s => s.Dsn is not null && !s.ToRemove))
        {
            var fulls = mine.Shards.TryGetValue(shard.Name, out var shardBackups)
                ? shardBackups.Full
                : (IReadOnlyList<FullBackupState>)[];

            // S: супервизия активного (Task 12: PLANNED — запуск/достарт;
            // Running/Uploading — поллинг UPLOADING/итог/vanished).
            var supervised = await SuperviseActiveAsync(
                cluster, shard, fulls, addresses.Value, creds.Value.BackupPassword, verifyOnCreate, ct);
            if (!supervised.IsSuccess)
                return Result<ProcessOutcome>.Failed(supervised.Error!);

            // G2: ensure роли backup_exec на мастере шарда — КАЖДЫЙ тик, до
            // due-гвардов (spec §3.1); идемпотентный gexec-гвард. Мастер
            // недоступен → transient: шард в этом тике skip (супервизия выше
            // уже прошла), следующий тик дообеспечит.
            var master = await shardEndpoints.ResolveMasterAsync(cluster, shard, addresses.Value, ct);
            if (!master.IsSuccess || master.Value is null)
                continue;
            var adminDsn = ShardEndpoints.AdminDsn(master.Value, snap.Config.DbName, installSecrets);
            var guard = await db.ExecuteScalarAsync(
                adminDsn, DatabaseProvisioner.BuildBackupExecRoleGuardSql(creds.Value.BackupPassword), ct);
            if (!guard.IsSuccess)
                continue; // transient (сеть/мастер ушёл) — следующий тик дообеспечит
            if (guard.Value is string createRole)
            {
                var createdRole = await db.ExecuteAsync(adminDsn, createRole, ct);
                if (!createdRole.IsSuccess)
                    continue;
            }

            // G3: новый полный — только без активного, при due и после бэкоффа.
            if (BackupPlanner.HasActive(fulls))
                continue; // инвариант одного активного — новый не создаём
            if (!BackupPlanner.IsDue(fulls, fullMaxAgeSec, nowUnix))
                continue;
            if (!BackupPlanner.BackoffPassed(fulls, options.RetryBaseSec, options.RetryMaxSec, nowUnix))
                continue; // бэкофф переснятия FAILED — следующий тик

            // источник — sync-standby, fallback мастер; резолв не удался →
            // transient: journal НЕ пишем, следующий тик повторит.
            var source = await shardEndpoints.ResolveBackupSourceAsync(cluster, shard, addresses.Value, ct);
            if (!source.IsSuccess || source.Value is null)
                continue;
            var role = source.Value.Ports == master.Value.Ports && source.Value.Host == master.Value.Host
                ? BackupSourceRole.Master
                : BackupSourceRole.Replica;

            var engine = driver.EngineFor(source.Value.Host);
            if (engine is null)
                continue; // хост источника не в таблице Docker:Hosts — transient

            var id = BackupPlanner.NextId(fulls.Select(f => f.Id), time.GetUtcNow().UtcDateTime);
            var node = addresses.Value.FirstOrDefault(p =>
                    p.Key.StartsWith($"{shard.Name}/", StringComparison.Ordinal)
                    && p.Value.Host == source.Value.Host && p.Value.Ports == source.Value.Ports)
                .Key.Split('/')[1];

            // journal-before-manipulations: PLANNED до создания контейнера.
            var planned = new FullBackupState(
                id, FullBackupStatus.Planned, node, role, nowUnix, null, null, null, null, null);
            var put = await PutAsync(BackupNames.FullKey(cluster, shard.Name, id), BackupStatusJson.Serialize(planned), ct);
            if (!put.IsSuccess)
                return Result<ProcessOutcome>.Failed(put.Error!);

            // Джоб-контейнер на docker-хосте источника (extra_hosts, без портов);
            // сбой create/start — PLANNED остаётся, S-супервизия (Task 12)
            // идемпотентно запустит следующим тиком (spec §2.4).
            var spec = BackupJobSpec.Build(options, cluster, shard.Name, id, source.Value, creds.Value.BackupPassword);
            var name = BackupNames.ContainerName(cluster, shard.Name, id);
            var createdContainer = await engine.CreateContainerAsync(spec, name, ct);
            if (!createdContainer.IsSuccess)
                continue;
            var started = await engine.StartContainerAsync(name, ct);
            if (!started.IsSuccess)
                continue;

            var running = planned with { State = FullBackupStatus.Running };
            var putRunning = await PutAsync(BackupNames.FullKey(cluster, shard.Name, id), BackupStatusJson.Serialize(running), ct);
            if (!putRunning.IsSuccess)
                return Result<ProcessOutcome>.Failed(putRunning.Error!);
            await journal.WritePhaseAsync(cluster, Op, $"started/{shard.Name}/{id}", claims.InstanceId, null, ct);
        }

        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    // S-ветка супервизии — реализуется Task 12 этого плана.
    private Task<Result> SuperviseActiveAsync(
        string cluster, ShardSpec shard, IReadOnlyList<FullBackupState> fulls,
        IReadOnlyDictionary<string, NodeAddress> addresses, string backupPassword,
        bool verifyOnCreate, CancellationToken ct)
        => Task.FromResult(Result.Success());

    // Failover-обёртка put: первый успешный endpoint выигрывает (образец DeprovisioningProcess).
    private async Task<Result> PutAsync(string key, string value, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.PutAsync(endpoint, key, value, null, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }
}
```

ПРИМЕЧАНИЕ: в теле использовать `time.GetUtcNow()` из параметра конструктора (не создавать заново); при резолве `node`, если ключ `"<X>/<node>"` не найден в portalloc — использовать `source.Value.Host` как журнал-факт и не валить тик.

- [ ] **Step 2: Тесты G-веток (падают)** — `src/tests/PgWorker.UnitTests/Backups/BackupProcessTests.cs`. Переиспользовать существующие двойники: `Fakes.FakeEtcd` / `Fakes.FakeSql` (файл `src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs`), `ClaimStore` + `WorkJournal` на FakeEtcd и Rig-паттерн сборки — как `ClusterSecretRotatorTests.NewRig` (`src/tests/PgWorker.UnitTests/Provisioning/ClusterSecretRotatorTests.cs:75-90`). Новые двойники в файле теста:
  - `FakeBackupEngine : IDockerEngine` — словарь контейнеров по имени (тест управляет `State` и `Logs`), записи `Created specs`, `Removed`, `RemovedVolumes`, флаг `ListFails` (transport-отказ);
  - `FakeBackupDriver : IClusterDriver` — `EngineFor(host)` → FakeBackupEngine, `RemoveBackupJobsAsync` → Success (остальные члены — `throw new NotSupportedException()`);
  - `FakeSecretEnsurer : IClusterSecretEnsurer` — `ClusterCredentials` с `BackupPassword = "pw0000000000000000000000000000A"`;
  - `FakeSql : ISqlExecutor` — здесь свой (нужен `ExecuteScalarAsync` → `null` «роль уже есть»): `Executed`-список;
  - `ShardProbe` — `new ShardProbe(new HttpClient(new DeadHandler()))` (Patroni недоступен → резолв мастера по master-ключу — как в ротатор-тестах);
  - сид: Active-кластер `shop`, шард `shard1` с dsn/master-ключом + `Portalloc.Serialize` (формат — `ClusterSecretRotatorTests.SeedCluster:53-60`).

Кейсы (все AAA, имена тестов):

```csharp
[Fact] Disabled_Noop:  // G0 — FakeSecretEnsurer не вызван; /pgworker/backups/ пуст после тика
[Fact] DueNoCompleted_PlansRunsAndStartsContainer:
    // G3 — в FakeEtcd появилась запись /pgworker/backups/shop/shard1/full/<id>
    // с state RUNNING, node/role от источника; FakeBackupEngine.Created содержит
    // ContainerName с env PGW_BK_DSN "user=backup_exec password=pw…" и
    // ExtraHosts host-gateway; WorkJournal phase started/shard1/<id>
[Fact] ActiveExists_DoesNotPlanSecond:  // сид RUNNING-записи → ни нового ключа, ни контейнера
[Fact] FreshCompleted_NotDue_RoleStillEnsured:
    // сид COMPLETED finished час назад (порог 86400) → новых записей НЕТ, но
    // гвард backup_exec исполнен (FakeSql.Scalars не пуст — G2 каждый тик,
    // spec §3.1, ревью Ф4 finding 1), тик Done
[Fact] FailedRecent_BackoffBlocks:  // сид FAILED started 100 c назад, Base=300 → нет нового PLANNED
[Fact] MasterUnresolved_SkipsShard:
    // пустой portalloc → гвард НЕ исполнен (FakeSql пуст), ключей нет, тик Done
    // (супервизия при этом прошла без мутаций — активных нет)
[Fact] NotClaimed_Refuses:  // ClaimStore без клэйма → Failed, FakeEtcd.Txns пуст (после отсечения claim-txn)
```

- [ ] **Step 3: Прогнать — упасть, затем зелёно по мере реализации**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~BackupProcessTests"`
Expected: после реализации — PASS (7 кейсов).

- [ ] **Step 4: Коммит**

```bash
git add src/PgWorker.Backups/Process/BackupProcess.cs src/tests/PgWorker.UnitTests/Backups/BackupProcessTests.cs
git commit -m "feat(t02): BackupProcess G0-G3 — планирование и запуск джоба полного бэкапа"
```

**Выход:** тик создаёт бэкапы по due; spec §3.1 (G0–G3, инварианты), §2 п.4 (journal-before-manipulations, тик non-blocking).

---

### Task 12: `BackupProcess` — супервизия: перезапуск PLANNED, поллинг, фиксация итога (S)

**Files:**
- Modify: `src/PgWorker.Backups/Process/BackupProcess.cs` (реализация `SuperviseActiveAsync` + чистки)
- Test: `src/tests/PgWorker.UnitTests/Backups/BackupProcessTests.cs` (+кейсы S)

**Interfaces:**
- Consumes: `GetContainerLogsAsync`/`InspectContainerAsync`/`ListContainersAsync`/`RemoveContainerAsync`/`RemoveVolumeAsync`/`CreateContainerAsync`/`StartContainerAsync` (Task 4), `BackupJobLog`/`BackupJobSpec` (Task 7).
- Produces: полный жизненный цикл PLANNED→RUNNING→UPLOADING→COMPLETED/FAILED + удаление контейнера/staging-volume; идемпотентный (пере)запуск PLANNED (spec §2.4: «PLANNED без контейнера → запуск», ревью Ф4 finding 2) — потребляет E2E Task 14, панель Task 15 читает итоги.

- [ ] **Step 1: Тесты S-веток (падают)** — добавить в `BackupProcessTests.cs` (механика: сид etcd с активной записью + `FakeBackupEngine` с управляемым состоянием контейнера; кейсы перезапуска седят PLANNED-запись БЕЗ контейнера/с контейнером в `created` — как после сбоя create/start «прошлого тика»):

```csharp
[Fact] PlannedWithoutContainer_Relaunches:
    // сид PLANNED без контейнера (сбой create в прошлом тике) → FakeBackupEngine
    // получил CreateContainerAsync (env от backup_exec) + StartContainerAsync;
    // etcd state=RUNNING (та же запись/тот же id — «PLANNED без контейнера →
    // запуск», spec §2.4); бэкофф-штрафа нет (новых ключей не появилось)
[Fact] PlannedWithCreatedContainer_GetsStarted:
    // сид PLANNED + контейнер в created (сбой start в прошлом тике) →
    // StartContainerAsync вызван, etcd=RUNNING (не «жив» — wedge исключён)
[Fact] RunningJob_UploadingMarker_MovesToUploading:
    // контейнер running, логи содержат {"phase":"uploading","wal_start_segment":"...42"}
    // → в etcd state=UPLOADING и wal_start_segment=...42 (node/role/started сохранены)
[Fact] ExitedZero_OkResult_CompletesAndCleans:
    // контейнер exited, exit 0, result {"ok":true,...} → COMPLETED с finished_unix,
    // wal_start_segment, size_bytes, verify PENDING; FakeBackupEngine зафиксировал
    // RemoveContainerAsync + RemoveVolumeAsync (volume pgw-backup-<C>-<X>-<id>)
[Fact] ExitedNonZero_FailsWithError:
    // exit 1 + {"ok":false,"error":"..."} → FAILED с error/finished_unix; контейнер/volume удалены
[Fact] ExitedWithoutResult_FailsWithExitCode:
    // exit 2 без result-JSON → FAILED с error "exit 2" (exit-код — истина итога)
[Fact] RunningWithoutContainer_MarksFailedContainerVanished:
    // статус RUNNING, ListContainers пуст (all=true) → FAILED error=container-vanished;
    // RemoveVolumeAsync вызван (осиротевший staging), 404-семантика — на движке
[Fact] TransportError_KeepsStatus:
    // ListContainers возвращает Failed → статус в etcd НЕ изменён (следующий тик повторит)
[Fact] Exited_InspectOrLogsTransportError_KeepsStatus:
    // статус RUNNING, контейнер exited (list ok), но GetContainerLogsAsync ИЛИ
    // InspectContainerAsync возвращает Failed (transport) → статус в etcd НЕ
    // изменён (остался RUNNING), контейнер/volume НЕ удалены (CleanupJobAsync
    // не зван) — попытка не теряется, следующий тик повторит супервизию
    // (spec §3.1 S «inspect/logs недоступен → transient», ревью Ф4-3)
```

- [ ] **Step 2: Реализация `SuperviseActiveAsync`** (замена заглушки Task 11; сигнатура уже с `backupPassword` из Task 11):

```csharp
    // S: супервизия активного по детерминированному имени контейнера на
    // docker-хосте его источника (арх/19 §2). Ветвление по active.State
    // (spec §2.4, ревью Ф4 finding 2):
    //   PLANNED + нет контейнера → идемпотентный запуск (сбой create/start
    //     прошлого тика НЕ превращается в FAILED/vanished и НЕ карается бэкоффом);
    //   PLANNED/другой + created → довыгоняем StartContainerAsync (304=успех);
    //   running → поллинг логов → UPLOADING;
    //   exited + result → COMPLETED/FAILED; RUNNING/UPLOADING без контейнера →
    //     FAILED container-vanished; transport-отказ docker (list/logs/inspect)
    //     — transient: статус не меняем, следующий тик повторит (spec §3.1 S).
    private async Task<Result> SuperviseActiveAsync(
        string cluster, ShardSpec shard, IReadOnlyList<FullBackupState> fulls,
        IReadOnlyDictionary<string, NodeAddress> addresses, string backupPassword,
        bool verifyOnCreate, CancellationToken ct)
    {
        foreach (var active in fulls.Where(f => f.State
                     is FullBackupStatus.Planned or FullBackupStatus.Running or FullBackupStatus.Uploading))
        {
            // хост джоба — хост ноды-источника из portalloc (node-факт статуса);
            // нода исчезла из portalloc → transient: следующий тик.
            var source = addresses.FirstOrDefault(p => p.Key == $"{shard.Name}/{active.Node}").Value;
            if (source is null)
                continue;
            var engine = driver.EngineFor(source.Host);
            if (engine is null)
                continue;

            var name = BackupNames.ContainerName(cluster, shard.Name, active.Id);
            var list = await engine.ListContainersAsync(name, all: true, ct);
            if (!list.IsSuccess)
                continue; // transient transport-отказ: статус не меняем (арх/19 §2)

            var found = list.Value.FirstOrDefault(c => c.Names.Contains(name));

            // PLANNED: джоб ещё не стартовал — идемпотентный запуск (spec §2.4).
            if (active.State == FullBackupStatus.Planned && found is not { State: "running" or "exited" })
            {
                if (found is null)
                {
                    var spec = BackupJobSpec.Build(options, cluster, shard.Name, active.Id, source, backupPassword);
                    var created = await engine.CreateContainerAsync(spec, name, ct);
                    if (!created.IsSuccess)
                        continue; // transient — следующий тик повторит запуск
                }
                else
                {
                    // контейнер есть, но не running (created/paused и т.п.) — стартуем;
                    // 304 already-started = успех (движок).
                    var started = await engine.StartContainerAsync(name, ct);
                    if (!started.IsSuccess)
                        continue;
                }

                var running = active with { State = FullBackupStatus.Running };
                var putRunning = await PutAsync(
                    BackupNames.FullKey(cluster, shard.Name, active.Id), BackupStatusJson.Serialize(running), ct);
                if (!putRunning.IsSuccess)
                    return putRunning;
                await journal.WritePhaseAsync(cluster, Op, $"started/{shard.Name}/{active.Id}", claims.InstanceId, null, ct);
                continue;
            }

            // RUNNING/UPLOADING + created — аномалия (start потерялся между тиками):
            // довыгоняем (304 = успех), статус не трогаем — следующий тик увидит running.
            if (found is { State: "created" })
            {
                await engine.StartContainerAsync(name, ct);
                continue;
            }

            if (found is null)
            {
                // контейнера нет вовсе (включая exited) — сюда попадают только
                // RUNNING/UPLOADING (PLANNED разобран выше): рестарт docker-хоста
                // и пр.; осиротевший staging volume удаляем (404 = ок), переснятие по G3.
                var vanished = active with
                {
                    State = FullBackupStatus.Failed,
                    FinishedUnix = time.GetUtcNow().ToUnixTimeSeconds(),
                    Error = "container-vanished",
                };
                var putVanished = await PutAsync(
                    BackupNames.FullKey(cluster, shard.Name, active.Id), BackupStatusJson.Serialize(vanished), ct);
                if (!putVanished.IsSuccess)
                    return putVanished;
                await CleanupJobAsync(engine, cluster, shard.Name, active.Id, ct);
                await journal.WritePhaseAsync(cluster, Op, $"vanished/{shard.Name}/{active.Id}",
                    claims.InstanceId, "container-vanished", ct);
                continue;
            }

            // Логи — транспорт guarded (spec §3.1 S: logs недоступны → transient,
            // статус не меняем, следующий тик повторит супервизию; ревью Ф4-3).
            var logs = await engine.GetContainerLogsAsync(name, tail: 200, ct);
            if (!logs.IsSuccess)
                continue;
            var markers = BackupJobLog.Parse(logs.Value);

            if (found is { State: "running" or "restarting" })
            {
                if (markers is { Phase: "uploading", WalStartSegment: { } wal }
                    && active.State != FullBackupStatus.Uploading)
                {
                    var uploading = active with { State = FullBackupStatus.Uploading, WalStartSegment = wal };
                    var put = await PutAsync(
                        BackupNames.FullKey(cluster, shard.Name, active.Id), BackupStatusJson.Serialize(uploading), ct);
                    if (!put.IsSuccess)
                        return put;
                }

                continue; // жив — ждём следующие тики
            }

            if (found is { State: "exited" })
            {
                // exit-код — только при успешном инспекте (spec §3.1 S: inspect
                // недоступен → transient, статус не меняем). Без гварда успешный
                // бэкап (exit 0 + ok:true, но логи/инспект не прочитаны из-за
                // transport-отказа) ушёл бы в ЛОЖНЫЙ FAILED («exit 0»/«exit -1»),
                // CleanupJobAsync удалил бы контейнер — попытка потеряна, воркер
                // переснимает полный лишний раз (ложный критичный алерт панели).
                var inspect = await engine.InspectContainerAsync(found.Id, ct);
                if (!inspect.IsSuccess)
                    continue;
                var exitCode = inspect.Value.ExitCode ?? -1;
                FullBackupState outcome;
                if (exitCode == 0 && markers.Result is { Ok: true } result)
                {
                    outcome = active with
                    {
                        State = FullBackupStatus.Completed,
                        FinishedUnix = time.GetUtcNow().ToUnixTimeSeconds(),
                        WalStartSegment = result.WalStartSegment ?? active.WalStartSegment,
                        SizeBytes = result.SizeBytes,
                        Verify = verifyOnCreate ? new BackupVerify(BackupVerifyStatus.Pending, null) : null,
                    };
                }
                else
                {
                    // exit-код — истина итога; result-JSON — метаданные (арх/19 §10).
                    var error = markers.Result is { Ok: false, Error: { } reason }
                        ? reason
                        : $"exit {exitCode}";
                    outcome = active with
                    {
                        State = FullBackupStatus.Failed,
                        FinishedUnix = time.GetUtcNow().ToUnixTimeSeconds(),
                        Error = error,
                    };
                }

                var putOutcome = await PutAsync(
                    BackupNames.FullKey(cluster, shard.Name, active.Id), BackupStatusJson.Serialize(outcome), ct);
                if (!putOutcome.IsSuccess)
                    return putOutcome;
                await CleanupJobAsync(engine, cluster, shard.Name, active.Id, ct);
                await journal.WritePhaseAsync(cluster, Op,
                    $"{(outcome.State == FullBackupStatus.Completed ? "completed" : "failed")}/{shard.Name}/{active.Id}",
                    claims.InstanceId, outcome.Error, ct);
            }
        }

        return Result.Success();
    }

    // Итог зафиксирован — контейнер и staging volume джоба не нужны (арх/19 §2);
    // квота-tmpfs-джоб volume не имеет — RemoveVolumeAsync 404 = успех.
    private async Task CleanupJobAsync(IDockerEngine engine, string cluster, string shard, string id, CancellationToken ct)
    {
        await engine.RemoveContainerAsync(BackupNames.ContainerName(cluster, shard, id), force: true, ct);
        await engine.RemoveVolumeAsync(BackupNames.VolumeName(cluster, shard, id), ct);
    }
```

- [ ] **Step 3: Прогнать — зелёно (вся BackupProcessTests: 16 кейсов — 7 G + 9 S)**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~BackupProcessTests"`
Expected: PASS.

- [ ] **Step 4: Полный юнит-прогон + коммит**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug`
Expected: PASS.

```bash
git add src/PgWorker.Backups/Process/BackupProcess.cs src/tests/PgWorker.UnitTests/Backups/BackupProcessTests.cs
git commit -m "feat(t02): супервизия джоба — UPLOADING/COMPLETED/FAILED/vanished + чистка"
```

**Выход:** жизненный цикл статусов и takeover-устойчивость; spec §3.1 S, §9 п.1/3/4.

---

### Task 13: Wiring в ReconcileLoop + DI

**Files:**
- Modify: `src/PgWorker.App/Loops/ClusterProcesses.cs` (`IClusterProcesses.BackupsAsync` + реализация)
- Modify: `src/PgWorker.App/Loops/ReconcileLoop.cs` (range `/pgworker/backups/` + вызов после rotate-app-password, до repair)
- Modify: `src/PgWorker.App/Program.cs` (DI `BackupProcess`, `ClusterProcesses`)
- Test: `src/tests/PgWorker.UnitTests/App/ReconcileLoopTests.cs` (FakeProcesses + 2 кейса)

**Interfaces:**
- Consumes: `BackupProcess.TickAsync` (Task 11-12), `IClusterDriver.EngineFor/RemoveBackupJobsAsync` (Task 10), `BackupsOptions.Enabled` (Task 2).
- Produces: `IClusterProcesses.BackupsAsync(ClusterSnapshot snap, IReadOnlyList<ClusterBackups> backups, CancellationToken ct)`.

- [ ] **Step 1: Тесты (падают)** — расширить `FakeProcesses` (в `ReconcileLoopTests.cs`) методом `BackupsAsync` (по образцу `ProcessMovesAsync`: трек `BackupsUp`, callName `"backups"`) и два кейса:

```csharp
// AAA: бэкапы — в Active-ветке после rotate-app-password и до repair
// (короткие плановые операции раньше; планировщик non-blocking, spec §3.1)
[Fact]
public async Task Tick_ActiveClusterWithBackupsEnabled_CalledBetweenRotateAndRepair()
{
    // Arrange — кластер + Backups.Enabled=true; FakeProcesses пишет порядок Calls.
    SeedCluster("shop", null);
    _options.CurrentValue.Backups.Enabled = true;

    // Act
    await CreateLoop(processes).TickSafelyAsync(TestContext.Current.CancellationToken);

    // Assert — порядок вызовов: ...rotate-app-password → backups → repair...
    var calls = processes.Calls;
    calls.Should().Contain("rotate-app-password/shop");
    calls.Should().Contain("backups/shop");
    calls.Should().Contain("repair/shop");
    calls.IndexOf("rotate-app-password/shop").Should().BeLessThan(calls.IndexOf("backups/shop"));
    calls.IndexOf("backups/shop").Should().BeLessThan(calls.IndexOf("repair/shop"));
}

// AAA: Enabled=false (дефолт) — процесс не вызывается, поведение не меняется
[Fact]
public async Task Tick_BackupsDisabled_ProcessNotCalled()
{
    // Arrange — дефолтный конфиг (Backups.Enabled=false).
    SeedCluster("shop", null);

    // Act
    await CreateLoop(new FakeProcesses()).TickSafelyAsync(TestContext.Current.CancellationToken);

    // Assert
    processes.Calls.Should().NotContain(c => c.StartsWith("backups/"));
}
```

(`FixedOptionsMonitor` из `TestSupport.cs` — если мутабельный, установить `Enabled` до создания loop; иначе задать в конструкторе опций.)

- [ ] **Step 2: Прогнать — упасть**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~ReconcileLoopTests"`
Expected: FAIL (нет метода у FakeProcesses/интерфейса).

- [ ] **Step 3: Реализация**

`ClusterProcesses.cs` — в интерфейс (после `RotateAppPasswordAsync`):

```csharp
    /// <summary>Планировщик/супервизор полных бэкапов (t02, arch/19 §2):
    /// префикс /pgworker/backups/ читается циклом при Enabled; тик non-blocking.</summary>
    Task<Result<ProcessOutcome>> BackupsAsync(
        ClusterSnapshot snap, IReadOnlyList<ClusterBackups> backups, CancellationToken ct);
```

реализация — в `ClusterProcesses` добавить зависимость `BackupProcess backups` и:

```csharp
    public Task<Result<ProcessOutcome>> BackupsAsync(
        ClusterSnapshot snap, IReadOnlyList<ClusterBackups> backups, CancellationToken ct)
        => backupsProcess.TickAsync(snap, backups, ct);
```

`ReconcileLoop.cs` — в `TickAsync` после `serviceKvs`:

```csharp
        // Бэкапы (t02, arch/19): префикс читаем только при Enabled — выключенная
        // подсистема не меняет поведение (лишних чтений/алертов нет).
        IReadOnlyList<ClusterBackups> backups = [];
        if (options.CurrentValue.Backups.Enabled)
        {
            var backupsKvs = await RangeWithFailoverAsync(endpoints, "/pgworker/backups/", ct);
            if (!backupsKvs.IsSuccess)
                return Result.Failed(backupsKvs.Error!);
            var parsedBackups = BackupsParser.Parse(backupsKvs.Value, out var backupsParseErrors);
            foreach (var error in backupsParseErrors)
                logger.LogWarning("пропущен битый ключ: {Error}", error);
            backups = parsedBackups.Value;
        }
```

прокинуть `backups` в `ProcessClusterAsync` (параметр) и в Active-ветке — после `rotate-app-password`, до `repair`:

```csharp
                    // Полные бэкапы (t02, arch/19 §2): после коротких плановых
                    // операций, до репарации; тик запускает/поллит джобы, не ждёт их.
                    await RunClusterOpAsync(cluster, "backups",
                        () => processes.BackupsAsync(snap, backups, ct), ct);
```

`Program.cs` — DI (по образцу MoveProcess; секция после ClusterSecretRotator):

```csharp
// Полные бэкапы шардов (t02, arch/19): планировщик + супервизия джобов;
// runtime-опции — склейка секции PgWorker:Backups; процесс — no-op при Enabled=false.
builder.Services.AddSingleton(sp => new PgWorker.Backups.BackupProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ShardEndpoints>(),
    sp.GetRequiredService<ISqlExecutor>(),
    sp.GetRequiredService<IClusterSecretEnsurer>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<InstallSecrets>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Backups.ToRuntime(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ILoggerFactory>().CreateLogger<PgWorker.Backups.BackupProcess>(),
    SnapshotDelegate(sp.GetRequiredService<SnapshotJob>())));
```

и параметр в `ClusterProcesses`-регистрации (`AddSingleton<IClusterProcesses, ClusterProcesses>` — тип резолвится DI; убедиться, что конструктор `ClusterProcesses` получил `BackupProcess`).

- [ ] **Step 4: Прогнать юниты + сборку решения**

Run: `dotnet test src/tests/PgWorker.UnitTests -c Debug && dotnet build src/PgWorker.slnx -c Debug`
Expected: PASS + сборка без warnings.

- [ ] **Step 5: Коммит**

```bash
git add src/PgWorker.App src/tests/PgWorker.UnitTests/App/ReconcileLoopTests.cs
git commit -m "feat(t02): wiring BackupProcess в ReconcileLoop (после rotate, до repair)"
```

**Выход:** подсистема включается в цикл; spec §3.1 (вызов через IClusterProcesses.BackupsAsync), §4.

---

### Task 14: E2E-сценарии бэкапов (MinIO testcontainer + живой кластер)

**Files:**
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eFixture.cs` (`StartHostAsync` + `extraEnv`)
- Create: `src/tests/PgWorker.IntegrationTests/E2e/E2eBackupScenarios.cs`

**Interfaces:**
- Consumes: всё ранее (процесс, образ, секреты, deprovisioning).
- Produces: кейсы приёмки `Backup_FullDaily_Completes`, `Backup_FailsOnBadS3_RetriesWithNewId`, `Backup_Deprovision_CleansPrefix`, `Backup_Rotator_IncludesBackupExec` (мерж-гейт Task 17).

- [ ] **Step 1: `E2eFixture.StartHostAsync`** — сигнатура + слияние (строки ~172-179):

```csharp
    public async Task<HostInstance> StartHostAsync(
        string name, int snapshotIntervalMin = 360,
        IReadOnlyDictionary<string, string>? extraEnv = null, CancellationToken ct = default)
    {
        // ...
        var env = new Dictionary<string, string> { /* существующие пары без изменений */ };
        foreach (var (key, value) in extraEnv ?? [])
            env[key] = value;
        // ...
```

- [ ] **Step 2: Сценарии** — `src/tests/PgWorker.IntegrationTests/E2e/E2eBackupScenarios.cs`. Класс в коллекции `E2eCollection` + собственный `IAsyncLifetime` (MinIO + сеть + bucket + сборка образа `pgworker-backup:e2e`); сид/ожидания — компактные локальные хелперы по образцу `E2eScenarios` (`SeedClusterAsync`, `WaitForAsync`, `RunProcessAsync("docker", …)`):

```csharp
[Collection(E2eCollection.Name)]
public class E2eBackupScenarios(E2eFixture fixture) : IAsyncLifetime
{
    // MinIO testcontainer: порт ДИНАМИЧЕСКИЙ (AGENTS.md), network с alias
    // e2e-minio; bucket — mc-контейнером в той же сети. Образ джоба — сборка
    // из docker/PgWorker.Backup.Dockerfile (тег pgworker-backup:e2e).
    // Extra-env воркера: PgWorker__Backups__Enabled=true, S3-комплект
    // (endpoint http://host.docker.internal:<mapped>), Job__Image=pgworker-backup:e2e,
    // Retry__BaseSec=2/MaxSec=4 (ускоренный бэкофф), ScanIntervalSec=1 — уже в базе.

    public async ValueTask InitializeAsync() /* MinIO+сеть+bucket+build */;
    public async ValueTask DisposeAsync() /* контейнеры/сеть — ryuk */;

    [Fact]
    public async Task Backup_FullDaily_Completes()
    {
        // Arrange — кластер bkshop (seed по образцу E2eScenarios.SeedClusterAsync) +
        // policy-ключ /pgworker/backups/bkshop/policy {"full_max_age_sec":3600,
        // "verify":{"on_create":true}}; воркер с extra-env бэкапов.
        // Act — ждём (WaitForAsync ≤ 300 c) ключ full/<id> state=COMPLETED.
        // Assert — поля: node/role (replica|master), started/finished_unix,
        // wal_start_segment, size_bytes, verify PENDING; mc ls: full/<id>/
        // содержит backup_manifest и pg_wal/; wal/<wal_start_segment> есть;
        // контейнеров pgw-backup-full-bkshop-* и volume pgw-backup-bkshop-* нет.
    }

    [Fact]
    public async Task Backup_FailsOnBadS3_RetriesWithNewId()
    {
        // Arrange — кластер bkbads3, S3-endpoint http://host.docker.internal:1
        // (закрытый порт; порт 1 допустим как «заведомо закрытый» — это НЕ
        // хардкод тестового порта, а постоянный порт протокола tcpmux).
        // Act/Assert — (1) full/<id1> FAILED с error; (2) в течение ≤ 120 c
        // появился PLANNED/RUNNING с ДРУГИМ id (Retry BaseSec=2) — минимум
        // две разные записи-попытки в префиксе шарда.
    }

    [Fact]
    public async Task Backup_Deprovision_CleansPrefix()
    {
        // Arrange — кластер bkclean, ждём первый RUNNING/PLANNED (джоб жив).
        // Act — state=TO_REMOVE; ждём DeprovisionedAsync (по образцу E2eScenarios).
        // Assert — range /pgworker/backups/bkclean/ пуст; docker ps -a не имеет
        // pgw-backup-full-bkclean-*; volume pgw-backup-bkclean-* отсутствует.
    }

    [Fact]
    public async Task Backup_Rotator_IncludesBackupExec()
    {
        // Arrange — кластер bkrot с COMPLETED-бэкапом; читаем OLD =
        // /clusters/bkrot/backup_password; заявка /pgworker/rotations/bkrot
        // {"requested_unix": <now>} (по образцу E2eRotateScenarios).
        // Act — ждём смены backup_password (≠ OLD).
        // Assert — Npgsql-подключение к мастеру шарда (host/port из portalloc,
        // user=backup_exec password=NEW, sslmodeDisable? — как в существующих
        // SQL-пробах E2e) выполняет SELECT 1 (роль принимает новый пароль).
    }
}
```

Детали реализации хелперов брать из `E2eScenarios.cs`: `SeedClusterAsync` (копия с другим именем кластера), `ProvisionedAsync`/`WaitForAsync` (переиспользовать по смыслу — они private, продублировать компактно), `RunProcessAsync` для docker CLI. MinIO-контейнер:

```csharp
_net = new NetworkBuilder().Build();
_minio = new ContainerBuilder("minio/minio:RELEASE.2025-09-07T16-13-09Z")
    .WithCommand("server", "/data")
    .WithEnvironment("MINIO_ROOT_USER", "minioadmin")
    .WithEnvironment("MINIO_ROOT_PASSWORD", "minioadmin")
    .WithNetwork(_net)
    .WithNetworkAliases("e2e-minio")
    .WithPortBinding(9000, assignRandomHostPort: true)
    .Build();
// готовность: GET http://localhost:<mapped>/minio/health/live (ретрай ≤ 60 c)
// bucket: docker run --rm --network <net> --entrypoint /bin/sh minio/mc:<pin> -c \
//   "mc alias set t http://e2e-minio:9000 minioadmin minioadmin && mc mb --ignore-existing t/pgworker-backups"
// сборка: docker build -q -f <Root>/docker/PgWorker.Backup.Dockerfile -t pgworker-backup:e2e <Root>
```

- [ ] **Step 3: Прогон (Debug-контур с гейтом), зачистка**

Run:
```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-backup-full-daily
PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~E2eBackup"
docker rm -f $(docker ps -aq --filter name=pgw-) 2>/dev/null; docker network prune -f
```
Expected: 4 кейса PASS. Если падает — систематический дебаг (логи воркера в логе теста, `docker logs` джоба, etcd-ключи) до зелёного; время фикстур ≤ 100 с на подъём, ожидания кластера ≤ 360 с.

- [ ] **Step 4: Коммит**

```bash
git add src/tests/PgWorker.IntegrationTests/E2e
git commit -m "test(t02): E2E-сценарии полных бэкапов — MinIO testcontainer, 4 кейса приёмки"
```

**Выход:** приёмочные кейсы §7 (интеграция 1-4 + E2E Backup_FullDaily); spec §7, §9 п.1-5.

---

### Task 15: Панель — чтение префикса + правило `backup-full-stale`

**Files:**
- Create: `src/AdminPanel.Core/BackupInfo.cs` (модель)
- Create: `src/AdminPanel.Etcd/Parsing/BackupsParser.cs` (панельный)
- Modify: `src/AdminPanel.Core/EtcdSnapshot.cs` (+`Backups`), `src/AdminPanel.Etcd/SnapshotBuilder.cs`, `src/AdminPanel.Etcd/SnapshotRefresher.cs` (префикс + FailTick)
- Create: `src/AdminPanel.Core/Alerting/Rules/BackupFullStaleRule.cs`
- Modify: `src/AdminPanel.Core/Alerting/AlertsOptions.cs` (+`BackupFullMaxAgeSec`)
- Test: `src/tests/AdminPanel.UnitTests/BackupsParserTests.cs`, `BackupFullStaleRuleTests.cs`; правки `SnapshotRefresherTests.cs`/`SnapshotBuilderTests.cs` под новое поле

**Interfaces:**
- Consumes: префикс `/pgworker/backups/` (канон §4).
- Produces: `EtcdSnapshot.Backups: IReadOnlyList<ClusterBackupsInfo>`; модель:

```csharp
// AdminPanel.Core/BackupInfo.cs
namespace AdminPanel.Core;

// Бэкапы кластера из /pgworker/backups/ (arch/19 §4, t02; дубль воркерной модели
// осознанный — унификация t08-unify-adminpanel-duplicates). FullMaxAgeSec — null,
// если policy-ключа нет (правило берёт панельный дефолт). В словаре — шарды,
// у которых есть ХОТЯ БЫ ОДИН ключ полных: значение null = COMPLETED не было
// («полного никогда не было»); «шарда нет в словаре» = подсистема не включена
// для него → правило молчит.
public sealed record ClusterBackupsInfo(
    string Cluster,
    long? FullMaxAgeSec,
    IReadOnlyDictionary<string, long?> ShardLastCompletedUnix);
```

- [ ] **Step 1: Тесты (падают)**

`src/tests/AdminPanel.UnitTests/BackupsParserTests.cs` — по образцу `MovesQueueParserTests` (Kv-список руками; данные — из фикстуры `src/tests/PgWorker.UnitTests/EtcdFixtures/backups-full.json` как референс формата): кластер demo → `FullMaxAgeSec=43200`, s1 → `ShardLastCompletedUnix=1757294400`; кластер без policy → null; битый JSON → KeyParseError. Ключевой кейс never-семантики (ревью Ф4-2 finding 1 — null-значение словаря обязано возникать):

```csharp
using AdminPanel.Core;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Parsing;

namespace AdminPanel.UnitTests;

// Панельный парсер /pgworker/backups/ (t02): never-семантика — шард с ключами
// полных, но без COMPLETED, попадает в словарь со значением null (не молчит:
// молчание правила — только для ПУСТОГО префикса кластера).
public class BackupsParserTests
{
    // AAA: ключи есть, COMPLETED нет → ShardLastCompletedUnix[s1] = null
    [Fact]
    public void Parse_ShardWithKeysButNoCompleted_NullLastCompleted()
    {
        // Arrange — RUNNING + FAILED (первое включение подсистемы), COMPLETED нет
        var kvs = new List<Kv>
        {
            new("/pgworker/backups/demo/s1/full/20260910030000Z",
                "{\"state\":\"RUNNING\",\"node\":\"n\",\"role\":\"replica\",\"started_unix\":1757463600}", 1),
            new("/pgworker/backups/demo/s1/full/20260910030100Z",
                "{\"state\":\"FAILED\",\"node\":\"n\",\"role\":\"replica\",\"started_unix\":1757463660}", 2),
        };

        // Act
        var result = BackupsParser.Parse(kvs);

        // Assert — шард в словаре (never-алерт реализуем), значение null.
        result.Errors.Should().BeEmpty();
        var cluster = result.Clusters.Should().ContainSingle().Subject;
        cluster.Cluster.Should().Be("demo");
        cluster.FullMaxAgeSec.Should().BeNull(); // policy-ключа нет — панельный дефолт в правиле
        var pair = cluster.ShardLastCompletedUnix.Should().ContainKey("s1").WhoseValue;
        pair.Should().BeNull();
    }

    // AAA: несколько COMPLETED — в словаре максимум finished_unix
    [Fact]
    public void Parse_MultipleCompleted_LatestFinishedUnix()
    {
        // Arrange — два COMPLETED с разными finished_unix
        var kvs = new List<Kv>
        {
            new("/pgworker/backups/demo/s1/full/20260908030000Z",
                "{\"state\":\"COMPLETED\",\"node\":\"n\",\"role\":\"replica\",\"started_unix\":1757290800,\"finished_unix\":1757294400}", 1),
            new("/pgworker/backups/demo/s1/full/20260909030000Z",
                "{\"state\":\"COMPLETED\",\"node\":\"n\",\"role\":\"replica\",\"started_unix\":1757377200,\"finished_unix\":1757380800}", 2),
        };

        // Act
        var result = BackupsParser.Parse(kvs);

        // Assert
        result.Clusters.Single().ShardLastCompletedUnix["s1"].Should().Be(1757380800);
    }

    // AAA: COMPLETED без finished_unix (битый) — шард регистрируется, значение не обновляется
    [Fact]
    public void Parse_CompletedWithoutFinishedUnix_ShardKeptValueNull()
    {
        // Arrange
        var kvs = new List<Kv>
        {
            new("/pgworker/backups/demo/s1/full/20260908030000Z",
                "{\"state\":\"COMPLETED\",\"node\":\"n\",\"role\":\"replica\",\"started_unix\":1757290800}", 1),
        };

        // Act
        var result = BackupsParser.Parse(kvs);

        // Assert — шард в словаре со значением null (never-ветка правила).
        result.Clusters.Single().ShardLastCompletedUnix.Should().ContainKey("s1")
            .WhoseValue.Should().BeNull();
    }
}
```

(Плюс кейс «пустой список KV → `Clusters` пуст» — правило молчит.)

`src/tests/AdminPanel.UnitTests/BackupFullStaleRuleTests.cs` — по образцу `MoveStaleRule`-тестов (`AlertTestRules`/`TestSnapshots`-механика; снапшот собрать руками `new EtcdSnapshot(...)` с Backups):

```csharp
// 4 случая §3.5:
[Fact] FreshCompleted_NoAlert:        // finished 3600 c назад, порог 86400 — пусто
[Fact] StaleCompleted_CriticalAlert:  // finished 90000 c назад — kind backup-full-stale,
                                      // Severity Critical, target "demo/s1", details с порогом
[Fact] NoCompleted_NeverCompleted:    // ключи есть, COMPLETED нет — алерт «никогда не завершался»
[Fact] EmptyPrefix_ClusterSilent:     // кластера нет в snapshot.Backups — правилу нечего сказать
// + policy кластера переопределяет дефолт панели:
[Fact] ClusterPolicy_OverridesPanelDefault: // policy full_max_age_sec=60, finished 100 c назад → алерт
```

- [ ] **Step 2: Прогнать — упасть**

Run: `dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~Backup"`
Expected: FAIL компиляция.

- [ ] **Step 3: Реализация**

Панельный `BackupsParser` (чистая функция по образцу воркерного t01 — дубль осознанный):

```csharp
using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

public sealed record BackupsParseResult(
    IReadOnlyList<ClusterBackupsInfo> Clusters,
    IReadOnlyList<KeyParseError> Errors);

// Чистая функция: KV префикса /pgworker/backups/ (arch/19 §4, t02): policy
// full_max_age_sec + per-shard последний COMPLETED finished_unix. Битые
// значения — KeyParseError + пропуск записи (паттерн панели). Шард с любыми
// ключами полных попадает в словарь: null = COMPLETED не было («полного
// никогда не было», spec §3.5); шарды без ключей ВООБЩЕ в словарь не
// попадают (правило молчит — подсистема не включена).
public static class BackupsParser
{
    public const string Prefix = "/pgworker/backups/";

    public static BackupsParseResult Parse(IReadOnlyList<Kv> kvs)
    {
        // "/pgworker/backups/<C>/policy" | "/pgworker/backups/<C>/<X>/full/<id>"
        var policies = new Dictionary<string, long?>();
        var shards = new Dictionary<string, Dictionary<string, long?>>();
        var errors = new List<KeyParseError>();
        foreach (var kv in kvs)
        {
            var segments = kv.Key.Split('/');
            if (segments.Length < 5 || segments[1] != "pgworker" || segments[2] != "backups")
                continue; // чужой префикс

            var cluster = segments[3];
            if (cluster.Length == 0)
                continue;

            if (segments.Length == 5 && segments[4] == "policy")
            {
                try
                {
                    using var doc = JsonDocument.Parse(kv.Value);
                    policies[cluster] = doc.RootElement.ValueKind == JsonValueKind.Object
                        && doc.RootElement.TryGetProperty("full_max_age_sec", out var age)
                        && age.ValueKind == JsonValueKind.Number
                        && age.TryGetInt64(out var value)
                        ? value
                        : null;
                }
                catch (JsonException e)
                {
                    errors.Add(new(kv.Key, $"битый JSON policy: {e.Message}"));
                }

                continue;
            }

            if (segments.Length == 7 && segments[5] == "full" && segments[4].Length > 0 && segments[6].Length > 0)
            {
                try
                {
                    using var doc = JsonDocument.Parse(kv.Value);
                    var root = doc.RootElement;
                    if (root.ValueKind == JsonValueKind.Object
                        && root.TryGetProperty("state", out var state)
                        && state.ValueKind == JsonValueKind.String)
                    {
                        // Ключи бэкапов есть → шард В СЛОВАРЕ даже без COMPLETED:
                        // null = «полного никогда не было» (spec §3.5/§9.6, ревью
                        // Ф4-2 finding 1); молчание правила — только для ПУСТОГО
                        // префикса (шарда нет в словаре вовсе).
                        var perShard = GetOrAdd(shards, cluster);
                        perShard.TryAdd(segments[4], null);

                        if (state.GetString() == "COMPLETED"
                            && root.TryGetProperty("finished_unix", out var finished)
                            && finished.ValueKind == JsonValueKind.Number
                            && finished.TryGetInt64(out var value))
                        {
                            var current = perShard[segments[4]];
                            perShard[segments[4]] = current is null || value > current ? value : current;
                        }
                    }
                }
                catch (JsonException e)
                {
                    errors.Add(new(kv.Key, $"битый JSON full: {e.Message}"));
                }
            }
        }

        var clusters = shards.Keys
            .Concat(policies.Keys.Where(p => !shards.ContainsKey(p)))
            .Distinct()
            .OrderBy(c => c, StringComparer.Ordinal)
            .Select(c => new ClusterBackupsInfo(
                c,
                policies.TryGetValue(c, out var age) ? age : null,
                (shards.TryGetValue(c, out var perShard)
                    ? perShard
                    : new Dictionary<string, long?>())
                .ToDictionary(p => p.Key, p => p.Value)))
            .ToList();
        return new(clusters, errors);
    }
}
```

(Кластер с policy, но без шардов, остаётся в списке с пустым словарём — правило по пустому словарю молчит. `GetOrAdd`-хелпер — локальный, как в воркерном парсере; `TryAdd(shard, null)` гарантирует never-семантику «ключи есть, COMPLETED нет» — ревью Ф4-2 finding 1.)

`EtcdSnapshot` — поле после `MoveTickets`:

```csharp
    IReadOnlyList<ClusterBackupsInfo> Backups,        // префикс /pgworker/backups/ (arch/19 §4, t02)
```

`SnapshotBuilder.Build(...)` — параметр `BackupsParseResult backups` (после `moves`) и в конструктор снапшота `backups.Clusters`; `ParseErrors` дополнить `.. backups.Errors`.

`SnapshotRefresher`:
- `Prefixes`: `public const string Backups = "/pgworker/backups/";`
- параллельное чтение: `var backupsTask = WithFailoverAsync(alive, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.Backups, t), ct);`
- добавить `backupsKv` в проверку KV-провала и `BackupsParser.Parse(backupsKv.Value)` в блок парсинга → `SnapshotBuilder.Build(..., backupsParsed, ...)`.
- `FailTick`: `previous?.Backups ?? []` на позиции нового поля.

`AlertsOptions`:

```csharp
    // backup-full-stale (t02): панельный дефолт окна суточного алерта при
    // отсутствии policy-ключа кластера (arch/19 §4). <= 0 — дефолт каталога 86400.
    public long BackupFullMaxAgeSec { get; set; } = 86400;
```

`BackupFullStaleRule` (по образцу `MoveStaleRule`; severity критичный — данные без свежего полного = риск потери):

```csharp
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Core.Alerting.Rules;

// backup-full-stale (critical, t02): возраст последнего COMPLETED полного
// шарда > full_max_age_sec политики кластера (нет policy — панельный дефолт);
// COMPLETED нет вовсе — «полного никогда не было»; пустой префикс кластера
// (подсистема не включена) — молчим (arch/19 §4). Воркер алерты не пишет.
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class BackupFullStaleRule(IOptions<AlertsOptions> options) : IAlertRule
{
    public const string KindName = "backup-full-stale";
    public const long DefaultMaxAgeSec = 86400;

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        var nowUnix = context.NowUtc.ToUnixTimeSeconds();
        foreach (var cluster in snapshot.Backups)
        {
            var maxAge = cluster.FullMaxAgeSec is > 0 ? cluster.FullMaxAgeSec.Value : EffectiveDefault();
            foreach (var (shard, lastCompleted) in cluster.ShardLastCompletedUnix)
            {
                if (lastCompleted is { } finished && nowUnix - finished <= maxAge)
                    continue; // свежий полный — ок

                var description = lastCompleted is { } stale
                    ? $"последний полный бэкап шарда {shard} кластера {cluster.Cluster} старше {nowUnix - stale} c — порог {maxAge} c"
                    : $"полный бэкап шарда {shard} кластера {cluster.Cluster} никогда не завершался (нет COMPLETED)";
                yield return new Alert(
                    $"{KindName}:{cluster.Cluster}/{shard}",
                    AlertSeverity.Critical,
                    KindName,
                    $"{cluster.Cluster}/{shard}",
                    description,
                    new Dictionary<string, string>
                    {
                        ["maxAgeSeconds"] = maxAge.ToString(),
                        ["lastCompletedUnix"] = lastCompleted?.ToString() ?? string.Empty,
                    },
                    null,
                    "суточное окно без валидного полного бэкапа: восстановимость кластера под угрозой — проверь статусы /pgworker/backups/<C>/ (FAILED-ошибки джобов) и S3-хранилище",
                    AlertRemedy.Operator,
                    "воркер сам переснимает с бэкоффом; висит — S3/сеть/роль backup_exec");
            }
        }
    }

    private long EffectiveDefault()
    {
        var configured = options.Value.BackupFullMaxAgeSec;
        return configured > 0 ? configured : DefaultMaxAgeSec;
    }
}
```

(Поля `Alert`/`AlertRemedy`/`AlertSeverity` — сверить с реальным кортежем конструктора `Alert` в `AlertEngine`-правилах, при расхождении — подогнать под фактическую сигнатуру; смысл сохранён.)

- [ ] **Step 4: Правки существующих тестов** — `SnapshotRefresherTests`/`SnapshotBuilderTests`/`TestSnapshots` (новый параметр/поле `Backups` — компилятор укажет все точки; подставлять `[]`/парсер по образцу соседних префиксов).

- [ ] **Step 5: Прогнать панель-юниты + интеграцию панели**

Run: `dotnet test src/tests/AdminPanel.UnitTests -c Debug && dotnet build src/PgWorker.slnx -c Debug`
Expected: PASS + сборка чистая.

- [ ] **Step 6: Коммит**

```bash
git add src/AdminPanel.Core src/AdminPanel.Etcd src/tests/AdminPanel.UnitTests
git commit -m "feat(t02): панель — парсер /pgworker/backups/ + алерт backup-full-stale"
```

**Выход:** суточный алерт панели; spec §3.5, §9 п.6.

---

### Task 16: Стенд/deploy — сборка образа, env стенда

**Files:**
- Modify: `deploy/.env.example` (раскомментировать блок PGW_BACKUPS_*)
- Modify: `deploy/docker-compose.yml` (build-сервис `pgworker-backup` + env `PgWorker__Backups__Job__Image`)
- Modify: `dev-stand/adminpanel/checks/00-up.sh` (сборка `pgworker-backup:dev`)

**Interfaces:** — конфигурация стенда.

- [ ] **Step 1: `deploy/.env.example`** — заменить закомментированный блок (строки 54-59) на стендовые значения:

```bash
# Подсистема бэкапов шардов (arch/19, t02): полный контур на стенде —
# S3 = as-minio стенда (00-up.sh создаёт bucket pgworker-backups), воркер
# с Enabled=true; креды — стендовые дефолты MinIO (прод — per-install S3).
PGW_BACKUPS_ENABLED=true
PGW_BACKUP_S3_ENDPOINT=http://host.docker.internal:9000
PGW_BACKUP_S3_REGION=
PGW_BACKUP_S3_BUCKET=pgworker-backups
PGW_BACKUP_S3_ACCESS_KEY=minioadmin
PGW_BACKUP_S3_SECRET_KEY=minioadmin
```

- [ ] **Step 2: `deploy/docker-compose.yml`** — два изменения (spec §3.6 «compose build + 00-up.sh», ревью Ф4 finding 4):
  1. в `environment` сервиса `pgworker` (после `PgWorker__Backups__SecretKey`):

```yaml
      # Образ джоба полного бэкапа (t02): тег = дефолт Job.Image; сборка —
      # build-сервис pgworker-backup ниже (compose build) или 00-up.sh.
      PgWorker__Backups__Job__Image: ${PGW_BACKUP_JOB_IMAGE:-pgworker-backup:dev}
```

  2. после сервиса `kafkaworker` — build-only сервис (профиль `build` исключает его из `up`; `docker compose build pgworker-backup` собирает образ и без 00-up.sh):

```yaml
  # Образ джоба полного бэкапа (t02, arch/19 §2): только сборка — сервиса нет,
  # джобы запускает PgWorker как ephemeral-контейнеры. Профиль build не даёт
  # подняться в up; entrypoint-заглушка — защита от случайного run.
  pgworker-backup:
    build:
      context: ..
      dockerfile: docker/PgWorker.Backup.Dockerfile
    image: pgworker-backup:dev
    profiles: ["build"]
    entrypoint: ["true"]
```

- [ ] **Step 3: `dev-stand/adminpanel/checks/00-up.sh`** — перед блоком 1b (подъём pgworker), после блока MinIO:

```bash
# 1a-2) Образ джоба бэкапов (t02): собирается рядом с pgworker:dev — тег
#       PgWorker:Backups:Job:Image (дефолт pgworker-backup:dev). Прямой docker
#       build (а не compose build) — без env-зависимостей deploy/.env.
docker build -q -f "$ROOT/docker/PgWorker.Backup.Dockerfile" -t pgworker-backup:dev "$ROOT" \
  || { echo "❌ образ pgworker-backup не собрался (docker/PgWorker.Backup.Dockerfile)"; exit 1; }
echo "  образ pgworker-backup:dev готов"
```

(Ручная проверка compose-пути: `( cd deploy && docker compose --profile build build pgworker-backup )` — образ собирается и без 00-up.sh.)

- [ ] **Step 4: Ручная проверка стенда (полный профиль)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-backup-full-daily/dev-stand/adminpanel
./checks/00-up.sh          # ожидание: все блоки зелёные, включая сборку pgworker-backup:dev
# ускорить окно алерта для проверки (малый full_max_age_sec — только стенд):
docker exec as-etcd etcdctl --endpoints=http://localhost:2379 put \
  /pgworker/backups/demo/policy '{"retention":{"days":7,"weeks":4,"months":6},"full_max_age_sec":120,"verify":{"on_create":true}}'
# ждать появления бэкапов (полминуты-минуты):
docker exec as-etcd etcdctl --endpoints=http://localhost:2379 get /pgworker/backups/demo/ --prefix --keys-only
# объекты в MinIO:
docker run --rm --entrypoint /bin/sh --network "$(docker inspect as-minio -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}')" minio/mc:latest \
  -c "mc alias set s http://as-minio:9000 minioadmin minioadmin >/dev/null && mc ls --recursive s/pgworker-backups/ | head -20"
# алерт при сломанном S3: остановить MinIO (docker stop as-minio), подождать
# FAILED-переснятий и прогара окна (policy 120 c) — панель:
curl -fsS http://localhost:5050/api/... # сверить алерт backup-full-stale в API алертов (эндпоинт — как в checks/20-alerts.sh)
docker start as-minio    # вернуть стенд в рабочее состояние
```

Expected: статусы проходят до COMPLETED; объекты `full/<id>/...` и `wal/...` видны в MinIO; при остановленном MinIO записи FAILED с error, панель зажигает `backup-full-stale`; после старта MinIO воркер переснимает и алерт гаснет. (Если чек упирается в долгий полный бэкап pg-эмуляторов — дождаться по `--keys-only`-поллингу; pg-данные стенда малы.)

- [ ] **Step 5: Зачистка проверочного мусора + коммит**

```bash
docker exec as-etcd etcdctl --endpoints=http://localhost:2379 del /pgworker/backups/demo/policy  # вернуть дефолт (по желанию)
git add deploy/.env.example deploy/docker-compose.yml dev-stand/adminpanel/checks/00-up.sh
git commit -m "feat(t02): стенд — сборка pgworker-backup:dev + Backups-Enabled из коробки"
```

**Выход:** dev-стенд с работающими суточными бэкапами «из коробки»; spec §3.6, §9 п.8.

---

### Task 17: Мерж-гейт — серии тестов с зачисткой + roadmap

**Files:**
- Modify: `arch/roadmap/backup.md` (удалить `t02-backup-full-daily` и `←`-зависимости) — тем же коммитом, что и финальный мерж.

**Interfaces:** — гейт.

- [ ] **Step 1: Юнит-серии (без docker)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-backup-full-daily
dotnet test src/tests/PgWorker.UnitTests -c Debug
dotnet test src/tests/AdminPanel.UnitTests -c Debug
dotnet test src/tests/KafkaWorker.UnitTests -c Debug   # не задет, но прогон дешёвый
```
Expected: PASS везде.

- [ ] **Step 2: Полная docker-интеграционная серия (Debug) + зачистка** — БЕЗ фильтров: t02 трогает общие компоненты (`IClusterDriver`/`ContainerSpec`, ротатор, `EtcdSnapshot`/`SnapshotRefresher`, реестр правил, ReconcileLoop), spec §9.7 требует «существующие юниты/интеграция PgWorker/AdminPanel зелёные» (ревью Ф4 finding 3):

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-backup-full-daily
PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests -c Debug   # Etcd/Docker/Api-контракты + E2e-сценарии Debug-гейтом
docker rm -f $(docker ps -aq --filter name=pgw-) 2>/dev/null || true
docker ps -aq | wc -l   # убедиться: прочих тест-контейнеров нет (стендовые as-*/deploy-* не трогаем)
docker network prune -f
PGW_TEST_DOCKER=1 dotnet test src/tests/AdminPanel.IntegrationTests -c Debug # панель: снапшот/алерты/API
docker rm -f $(docker ps -aq --filter name=pgw-) 2>/dev/null || true
docker network prune -f
```
Expected: PASS обе серии; сети/контейнеры чисты между сериями. Упавшие E2eRotate/E2eAppSecret/RecreateRotate — сигнал регрессии R2-ротатора (finding 1) — чинить до продолжения.

- [ ] **Step 3: E2E на свежем Release — полный прогон (обязательный гейт AGENTS.md; тронуты App/Provisioning/Etcd/Docker; changed provisioning-процессы → полный E2eFixture)**

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release
docker rm -f $(docker ps -aq --filter name=pgw-) 2>/dev/null || true
docker network prune -f
```
Expected: PASS — весь solution на Release: юниты обеих панелей/воркеров + интеграция + ВСЕ E2E-сценарии (маркер `Scale_AddEmptyShard` входит в `E2eScaleScenarios`, новые `E2eBackup*` — рядом; узкий фильтр запрещён finding 3). E2eFixture сам собрал Release (инкрементально — секунды). Прогон долгий (полный E2E-контур) — это осознанный мерж-гейт; после — зачистка. Если время критично, допустим двухсерийный сплит `--filter "FullyQualifiedName~E2e"` затем `--filter "FullyQualifiedName!=E2e&..."` — с зачисткой между сериями, но НЕ сужением состава.

- [ ] **Step 4: Roadmap-гейт (тем же мерж-коммитом)** — в `arch/roadmap/backup.md`:
- удалить пункт `t02-backup-full-daily` (строки «- **`t02-backup-full-daily`** — …» целиком);
- в `t04-backup-verify` удалить хвост «← `t02-backup-full-daily`.» (остаётся без зависимостей);
- в `t05-backup-restore`: «← `t02-backup-full-daily`, `t03-backup-wal-stream`.» → «← `t03-backup-wal-stream`.»;
- в `t06-backup-retention`: «← `t02-backup-full-daily`,\n  `t03-backup-wal-stream`.» → «← `t03-backup-wal-stream`.»;
- в `t07-backup-supervisor`: убрать `t02-backup-full-daily` из списка `←`;
- в `t08-backup-minio-panel`: фраза «Осмысленна после `t02-backup-full-daily`, `t03-backup-wal-stream`» → «Осмысленна после `t03-backup-wal-stream`».

Никаких пометок «сделано» — история в git (правила `arch/roadmap/README.md`).

```bash
git add arch/roadmap/backup.md
git commit -m "merge: t02-backup-full-daily — суточные полные бэкапы (roadmap-гейт)"
```

- [ ] **Step 5: Итоговая сверка критериев приёмки** — прогнать чек-лист spec §9 (1-9) по фактическому состоянию: каждое «да» подтвердить артефактом (ключ etcd/объект S3/зелёный прогон/алерт). При провале пункта — вернуться к соответствующей задаче.

**Выход:** все серии зелёные с зачисткой, roadmap чист; spec §5 фазы 8-9, §9 п.7/9.

---

## Self-Review (выполнен при написании; дополнен по ревью Фазы 4)

1. **Покрытие spec:** §1.1 п.1 (планировщик) → Task 6,11-13; п.2 (джоб/образ) → Task 4,7,8; п.3 (статусы/бэкофф) → Task 3,6,11-12; п.4 (роль/секреты) → Task 5,10; п.5 (алерт) → Task 15. §3.3 — Task 3 (парсер), 7 (сериализация), 13 (range), 10 (D2). §3.6 — Task 16. §5 фазы 1-9 → Task 1,2-4,5-13,14,15,16,17. §7 — Task 11-12 (юниты), 14 (docker/E2E), 16 (стенд). §9 — Task 17 Step 5. Ограничения §6 — вне задач (не реализуется).
2. **Плейсхолдеры:** отсутствуют — каждый шаг содержит команды/код/критерий; в Task 11 Step 2 и Task 15 Step 3 оставлены оговорки «сверить с фактической сигнатурой» только там, где план опирается на внутренние детали тестовых двойников (точка проверки — компиляция).
3. **Консистентность типов:** `BackupNames`/`BackupJobSpec.Build`/`BackupJobLog.Parse`/`BackupStatusJson.Serialize`/`BackupProcess.TickAsync(snap, backups, ct)`/`SuperviseActiveAsync(…, backupPassword, verifyOnCreate, ct)`/`IClusterProcesses.BackupsAsync`/`ClusterBackupsInfo` едины между задачами; `FullBackupState.WalStartSegment: string?` после Task 3 используется всюду.
4. **Ревью Фазы 4 (закрытие findings):**
   - finding 1 → Task 5 (R2 backup_exec — gexec-гвард + юнит-кейс `Tick_Ticket_BackupExecRoleAbsent_CreatedByGuard`; ротация не ломается при `Enabled=false`) и Task 11 (G2 исполняется КАЖДЫЙ тик для каждого шарда с dsn, до due-гвардов; кейс `FreshCompleted_NotDue_RoleStillEnsured`);
   - finding 2 → Task 12 (ветвление супервизии по `active.State`: PLANNED без контейнера → идемпотентный запуск, `created` → довыгоняющий start; `container-vanished` — только для RUNNING/UPLOADING; кейсы `PlannedWithoutContainer_Relaunches`/`PlannedWithCreatedContainer_GetsStarted`);
   - finding 3 → Task 17 (полные серии: Debug-интеграция PgWorker+AdminPanel без фильтров + полный Release-прогон slnx; существующие E2eRotate/E2eAppSecret/RecreateRotate в гейте);
   - finding 4 → Task 16 (build-сервис `pgworker-backup` с профилем `build` в compose + 00-up.sh);
   - finding 5 (env `PGW_BK_STAGING_DIR`) — уже был в плане (Task 7 `BackupJobSpec`, Task 8 entrypoint), spec-правка учтена.
5. **Повторное ревью Фазы 4 (закрытие findings):**
   - finding 1 → Task 15 Step 3: full-ветка панельного парсера регистрирует шард (`GetOrAdd(shards, cluster)` + `perShard.TryAdd(shard, null)`) для ЛЮБОЙ записи со `state`, значение обновляет только в COMPLETED-подветке — «ключи есть, COMPLETED нет» даёт `null` → never-алерт (spec §3.5/§9.6); Step 1 дополнен кейсами `Parse_ShardWithKeysButNoCompleted_NullLastCompleted` / `Parse_MultipleCompleted_LatestFinishedUnix` / `Parse_CompletedWithoutFinishedUnix_ShardKeptValueNull` (согласованы с кодом Step 3);
   - finding 2 → Global Constraints: зачистка приведена к фильтрованному варианту `docker rm -f $(docker ps -aq --filter name=pgw-) 2>/dev/null` + `docker network prune -f` (контейнеры dev-стенда `as-*`/`adminpanel*`/`deploy-*` не затрагиваются); Task 4 Step 4 синхронизирован.
6. **Контрольное ревью (третий проход, один low-finding):**
   - finding → Task 12 Step 2: exited-ветка супервизии гвардит транспорт по spec §3.1 S — `GetContainerLogsAsync` при `!IsSuccess` → `continue` (markers больше не «молча null»), `InspectContainerAsync` при `!IsSuccess` → `continue` (exit-код читается только при успехе): успешный бэкап с непрочитанными из-за transport-отказа логами/инспектом больше не уходит в ложный FAILED с удалением контейнера; юнит-кейс `Exited_InspectOrLogsTransportError_KeepsStatus` (статус неизменен, контейнер/volume не тронуты); счётчик кейсов — 16 (7 G + 9 S). Кейс `ExitedWithoutResult_FailsWithExitCode` совместим (там логи/инспект успешны).

## Примечание об исполнении

Задачи строго последовательны (Task N использует артефакты N-1); исключение — Task 15 (панель) можно исполнять параллельно с Task 14 после Task 12. Коммиты — в ветке `feat-t02-backup-full-daily` worktree; пуш/мерж в `main` — только по команде пользователя (dev-flow гейт ревью).
