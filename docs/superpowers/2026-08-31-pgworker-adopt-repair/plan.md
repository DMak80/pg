# pgworker-adopt-repair — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** PgWorker становится хозяином любого Active-кластера в etcd: усыновляет кластеры без portalloc (восстановление адресов из HA-контура + docker, запись в portalloc), резолвит мастера по цепочке master-ключ → HA-leader → Patroni-REST, репарирует брошенные статус-ключи переездов синтетическими заявками; e2e на стенде доказывает гашение алертов панели реальным ремонтом.

**Architecture:** Три новых компонента — docker-инспекция нод (`NodeMatcher` + `IClusterDriver.InspectNodesAsync`), тиковый `AdoptionProcess` (AD0–AD4: merge portalloc с `object`-полем, nodes-ключи, ensure секретов/ролей) и тиковый `MoveRepairProcess` (MR0–MR3: классификация брошенных статусов → синтетические заявки put-if-absent в существующий MoveProcess). Резолв мастера и advertised-правило — точечные усиления `ShardEndpoints`/`MoveProcess`; границы надзора — точечные правки `NodeSupervisor`/`MasterKeyReconciler`.

**Tech Stack:** .NET 10, C# (`LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`), xUnit (AAA-комментарии), Testcontainers (integration), bash+jq (стенд-чеки).

**Spec:** `docs/superpowers/2026-08-31-pgworker-adopt-repair/spec.md` (план аргументируется от spec; исполнители читают оба). Канон: `arch/14-pgworker.md` §5 C/F/J/K.

**Ревизии:** v4 — правки по третьему ревью Фазы 4: `InspectNodesAsync` собирает все пары хоста и зовёт `NodeMatcher.Match` один раз на хост (merge patroni-порта сайдкара + skip-on-ambiguity работают как в юнит-тестах; зам. 1), `AdoptionProcess.cs` (создан T3 со старой сигнатурой) добавлен в вызовы/git add T4 (зам. 2). v3 — AbortSequence.cs в вызовах T4, полный e2e-прогон 00→10→15→20→30→40 (Step 8.2, T9.1, T9.2 п.5/6), `TxnCompare.NotExists` вместо несуществующего `VersionEqual`. v2 — блок shard-no-master в чеке 20, jq-проверка 4 нод, unit-тест формата `node:pg-port`, journal пропусков в AdoptionProcess.

## Global Constraints

- Работа в worktree `/Users/demakaev/ZCodeProject/worktrees/feat-pgworker-adopt-repair` (ветка `feat-pgworker-adopt-repair`); коммит после каждой задачи.
- Стиль коммитов — как в `git log`: `feat(pgworker): …` / `test(stand): …`, по-русски, кратко.
- `TreatWarningsAsErrors=true` — новый код без ворнингов; `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1` для integration-тестов.
- Тесты — AAA-комментарии (`// Arrange / // Act / // Assert`) — обязательное правило репозитория.
- Документация/комментарии — русские; идентификаторы — английские.
- Формат статус-ключа и etcd-контракт не менять (совместимость 1:1 со скриптами, панель не читает `/pgworker/`).
- Сборка/тесты из корня worktree: `dotnet build src/PgWorker.App/PgWorker.App.csproj` (подтягивает весь граф), `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj`, integration — `dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj` (Testcontainers: нужен живой Docker).

## File Structure (карта изменений)

| Файл | Задача | Ответственность |
|---|---|---|
| `src/PgWorker.Core/Model/Domain.cs` | T1, T7 | `NodeAddress.Object`, `BucketRoute.MovePhase/MoveUpdatedUnix` |
| `src/PgWorker.Core/Model/Portalloc.cs` | T1 | `PortallocEntry.Object`, перенос через From/ToAddress |
| `src/PgWorker.Docker/Engine/IDockerEngine.cs` | T2 | `DockerContainerInspect`, `InspectContainerAsync` |
| `src/PgWorker.Docker/Engine/DockerEngine.cs` | T2 | GET `/containers/<id>/json`, DTO Hostname/Env/Aliases/Ports |
| `src/PgWorker.Docker/Drivers/ClusterDriver.cs` | T2, T5 | `InspectNodesAsync`, `ExecContainerAsync` (интерфейс + plain + swarm) |
| `src/PgWorker.Docker/Drivers/NodeMatcher.cs` | T2 | НОВЫЙ: чистый матчинг контейнер↔нода |
| `src/PgWorker.Provisioning/Processes/AdoptionProcess.cs` | T3 (создание), T4 (вызов новой сигнатуры резолва) | НОВЫЙ: AD0–AD4 (+journal skipped при неоднозначности) |
| `src/PgWorker.Provisioning/Endpoints/ShardEndpoints.cs` | T4 | шаг HA-leader в резолве |
| `src/PgWorker.Moves/Ddl/MoveDdl.cs` | T5 | `containerOverride` для pg_dump |
| `src/PgWorker.Moves/Process/MoveProcess.cs` | T5 | advertised по исполнителю, exec-fallback M1 |
| `src/PgWorker.Moves/Process/AbortSequence.cs` | T4 | вызов `ResolveMasterAsync` (новая сигнатура) |
| `src/PgWorker.Provisioning/Processes/NodeSupervisor.cs` | T6 | object-матч, SQL-живость, self-healing off |
| `src/PgWorker.Provisioning/Processes/MasterKeyReconciler.cs` | T6 | skip усыновлённых шардов |
| `src/PgWorker.Etcd/Parsing/ClusterSnapshotParser.cs` | T7 | phase/updated_unix статусов |
| `src/PgWorker.Moves/Options.cs` | T7 | `RepairStaleSec`/`RepairFrozenSec` |
| `src/PgWorker.Moves/Requests/MoveRequestsStore.cs` | T7 | `PutIfAbsentAsync` (txn NotExists) |
| `src/PgWorker.Moves/Process/MoveRepairProcess.cs` | T7 | НОВЫЙ: MR0–MR3 + `RepairClassifier` |
| `src/PgWorker.App/Options.cs` | T7 | `MovesOptions` + `ToRuntime` |
| `src/PgWorker.App/Loops/ClusterProcesses.cs` | T3, T7 | `AdoptAsync`/`RepairAsync` |
| `src/PgWorker.App/Loops/ReconcileLoop.cs` | T3, T7 | порядок Active-ветки |
| `src/PgWorker.App/Program.cs` | T3, T6, T7 | DI новых процессов |
| `src/PgWorker.App/appsettings.json` | T7 | явные пороги репарации |
| `dev-stand/adminpanel/seed.sh` | T8 | чистка аномалий |
| `dev-stand/adminpanel/checks/20-alerts.sh` | T8 | перепись: нахерачивание→гашение→move-цикл (+сохранённый блок shard-no-master) |
| тесты (см. задачи) | все | unit + integration |

Порядок задач = порядок фаз spec §4: T1→T2→T3 (фаза 2), T4→T5→T6 (фаза 3), T7 (фаза 4), T8 (фаза 5), T9 (фаза 6/финал).

---

### Task 1: Модель — `object`-поле усыновлённой ноды (portalloc)

**Files:**
- Modify: `src/PgWorker.Core/Model/Domain.cs` (запись `NodeAddress`, ≈стр. 72)
- Modify: `src/PgWorker.Core/Model/Portalloc.cs` (запись `PortallocEntry`)
- Test: `src/tests/PgWorker.UnitTests/Model/PortallocTests.cs` (дополнить; если файла нет — создать рядом с существующими тестами модели в `src/tests/PgWorker.UnitTests/Model/`)

**Interfaces (Produces):**
- `public sealed record NodeAddress(string Host, NodePorts Ports, string? Object = null)` — `Object` = имя docker-контейнера усыновлённой ноды; `null` = каноническая `pgw-`-нода.
- `PortallocEntry` с `[JsonPropertyName("object")] string? Object = null`; `ToAddress()/From()` переносят `Object`; JSON: `object` отсутствует при null (`WhenWritingNull` — уже включён).

- [ ] **Step 1.1: Пишет failing-тест сериализации с object/без**

Вход: `Portalloc.Serialize/Parse` сегодня теряют `Object` (его нет в модели).
Действие: дополнить тест-файл кейсами (AAA):

```csharp
[Fact]
public void Serialize_WithObject_WritesObjectField()
{
    // Arrange: адрес усыновлённой ноды — object-контейнер вместо pgw-имени.
    var addresses = new Dictionary<string, NodeAddress>
    {
        ["s2/s2a"] = new("local", new NodePorts(5435, 8021, 0), "as-s2a"),
    };

    // Act
    var json = Portalloc.Serialize(addresses);

    // Assert: object сериализуется, doorman=0 пишется (int, не nullable).
    Assert.Contains("\"object\":\"as-s2a\"", json);
    Assert.Contains("\"doorman\":0", json);
}

[Fact]
public void RoundTrip_WithAndWithoutObject_PreservesEntries()
{
    // Arrange
    var raw = """
        {"s1/s1a":{"host":"local","pg":5433,"patroni":8011,"doorman":0,"object":"as-s1a"},
         "s1/s1b":{"host":"local","pg":5434,"patroni":8012,"doorman":16434}}
        """;

    // Act
    var parsed = Portalloc.Parse("demo", raw);
    var back = Portalloc.Serialize(parsed.Value);

    // Assert: object пережил roundtrip; у канонической ноды поле не пишется.
    Assert.True(parsed.IsSuccess);
    Assert.Equal("as-s1a", parsed.Value["s1/s1a"].Object);
    Assert.Null(parsed.Value["s1/s1b"].Object);
    Assert.DoesNotContain("\"object\"", back.Replace("\"object\":\"as-s1a\"", ""));
}

[Fact]
public void Parse_LegacyJsonWithoutObject_StillWorks()
{
    // Arrange: существующие кластеры — JSON без object (обратная совместимость).
    var raw = "{\"s1/s1a\":{\"host\":\"h1\",\"pg\":15432,\"patroni\":18008,\"doorman\":16432}}";

    // Act
    var parsed = Portalloc.Parse("shop", raw);

    // Assert
    Assert.True(parsed.IsSuccess);
    Assert.Null(parsed.Value["s1/s1a"].Object);
}
```

Выход: тесты не компилируются (`NodeAddress` не принимает третий аргумент).
Проверка: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~Portalloc"` → Compile ERROR.
Spec: §3.2 AD2 (`object`-поле portalloc), arch/14 §2.4.

- [ ] **Step 1.2: Реализация модели**

Вход: failing-тесты Step 1.1.
Действие: в `Domain.cs`:

```csharp
/// <summary>Адрес ноды: docker-хост + выделенные host-порты; Object — имя
/// фактического docker-контейнера усыновлённой ноды (arch/14 §2.4/§5 J),
/// null = каноническая pgw-нода нашего провижининга.</summary>
public sealed record NodeAddress(string Host, NodePorts Ports, string? Object = null);
```

В `Portalloc.cs`:

```csharp
public sealed record PortallocEntry(
    [property: JsonPropertyName("host")] string Host,
    [property: JsonPropertyName("pg")] int Pg,
    [property: JsonPropertyName("patroni")] int Patroni,
    [property: JsonPropertyName("doorman")] int Doorman,
    [property: JsonPropertyName("object")] string? Object = null)
{
    public NodeAddress ToAddress() => new(Host, new NodePorts(Pg, Patroni, Doorman), Object);

    public static PortallocEntry From(NodeAddress address)
        => new(address.Host, address.Ports.Pg, address.Ports.Patroni, address.Ports.Doorman, address.Object);
}
```

Выход: модель несёт `Object` через весь порт-аллокатор.
Проверка: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~Portalloc"` → PASS; `dotnet build src/PgWorker.App/PgWorker.App.csproj` → 0 errors/warnings.
Spec: §3.2.

- [ ] **Step 1.3: Коммит**

Вход: зелёные тесты Step 1.2.
Действие:

```bash
git add src/PgWorker.Core/Model/Domain.cs src/PgWorker.Core/Model/Portalloc.cs src/tests/PgWorker.UnitTests/Model/
git commit -m "feat(pgworker): object-поле portalloc — имя docker-контейнера усыновлённой ноды (adopt-repair T1)"
```

Выход: коммит в ветке worktree.
Проверка: `git log --oneline -1` показывает коммит; `git status --short` чист по этим файлам.
Spec: §3.2.

---

### Task 2: Docker-инспекция нод — `InspectContainerAsync` + `NodeMatcher` + `InspectNodesAsync`

**Files:**
- Modify: `src/PgWorker.Docker/Engine/IDockerEngine.cs` (новый record + метод)
- Modify: `src/PgWorker.Docker/Engine/DockerEngine.cs` (реализация + DTO)
- Create: `src/PgWorker.Docker/Drivers/NodeMatcher.cs`
- Modify: `src/PgWorker.Docker/Drivers/ClusterDriver.cs` (интерфейс + plain + swarm)
- Test: `src/tests/PgWorker.UnitTests/Docker/NodeMatcherTests.cs` (новый)
- Modify: стабы драйвера по компиляции: `src/tests/PgWorker.IntegrationTests/Etcd/StubScaleDriver.cs`, `src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs`

**Interfaces:**
- Consumes: `DockerContainer`, `PortMap` (существующие), `NodeAddress.Object` (T1).
- Produces (для T3):
  - `public sealed record DockerContainerInspect(string Id, string Hostname, string[] Aliases, string[] Env, PortMap[] Ports)` в `IDockerEngine.cs`;
  - `Task<Result<DockerContainerInspect>> InspectContainerAsync(string id, CancellationToken ct)` у `IDockerEngine`/`DockerEngine`;
  - `public sealed record DiscoveredNode(string NodeName, string Host, string Object, int Pg, int Patroni, int Doorman)` + `public NodeAddress ToAddress() => new(Host, new NodePorts(Pg, Patroni, Doorman), Object);` в `NodeMatcher.cs`;
  - `public static IReadOnlyDictionary<string, DiscoveredNode> Match(string dockerHost, IEnumerable<(DockerContainer Container, DockerContainerInspect Inspect)> containers, IReadOnlyCollection<string> nodeNames)` — чистая функция;
  - `Task<Result<IReadOnlyDictionary<string, DiscoveredNode>>> InspectNodesAsync(IReadOnlyCollection<string> nodeNames, CancellationToken ct)` у `IClusterDriver` (plain — полный; swarm — осознанно пустой результат с комментарием: усыновление swarm-кластеров вне текущего стенда, spec §3.1 покрывает plain);
  - `Task<Result<string>> ExecContainerAsync(string containerName, IReadOnlyList<string> cmd, CancellationToken ct)` у `IClusterDriver` (реализация в этом же таске — нужна T5; plain: поиск по имени по хостам → `engine.ExecAsync(id, cmd, ct)`; swarm: `ListTasksAsync` по сервису → `ContainerId`).

- [ ] **Step 2.1: Failing-тесты матчинга (чистая функция)**

Вход: правила матча spec §3.1: контейнер = нода при `hostname == nodeName` ИЛИ alias содержит nodeName; сайдкар Patroni при `env NODE_NAME == nodeName` (даёт patroni-порт); порты — public-биндинги 5432→pg / 8008→patroni / 6432→doorman, отсутствие → 0; неоднозначность (два контейнера-ноды на имя) → имя пропускается (безопасный отказ; журналирование пропуска — в AdoptionProcess, T3).
Действие: создать `NodeMatcherTests.cs` (xUnit, AAA; конструируем кортежи контейнер+инспект без docker):

```csharp
using PgWorker.Docker.Drivers;
using PgWorker.Docker.Engine;
using PgWorker.Core;

namespace PgWorker.UnitTests.Docker;

public class NodeMatcherTests
{
    private static (DockerContainer, DockerContainerInspect) Container(
        string name, string hostname, string[]? aliases = null, string[]? env = null, PortMap[]? ports = null)
        => (new DockerContainer("id-" + name, [name], "running", "img"),
            new DockerContainerInspect("id-" + name, hostname, aliases ?? [], env ?? [], ports ?? []));

    [Fact]
    public void Match_ByHostname_FillsPgPortAndObject()
    {
        // Arrange: стендовый as-s2a (hostname s2a) публикует 5432→5435.
        var containers = new[] { Container("as-s2a", "s2a", ports: [new PortMap(5432, 5435)]) };

        // Act
        var found = NodeMatcher.Match("local", containers, ["s2a"]);

        // Assert: нода найдена, object=имя контейнера, patroni/doorman=0 (нет биндингов).
        var node = found["s2a"];
        Assert.Equal("local", node.Host);
        Assert.Equal("as-s2a", node.Object);
        Assert.Equal(5435, node.Pg);
        Assert.Equal(0, node.Patroni);
        Assert.Equal(0, node.Doorman);
    }

    [Fact]
    public void Match_ByNetworkAlias_FillsNode()
    {
        // Arrange: Names отличается, но alias в сети равен имени ноды.
        var containers = new[] { Container("as-s1b", "stand-s1b-1", aliases: ["s1b"], ports: [new PortMap(5432, 5434)]) };

        // Act
        var found = NodeMatcher.Match("local", containers, ["s1b"]);

        // Assert
        Assert.Equal("as-s1b", found["s1b"].Object);
        Assert.Equal(5434, found["s1b"].Pg);
    }

    [Fact]
    public void Match_PatroniSidecarByNodeNameEnv_MergesPatroniPort()
    {
        // Arrange: PG-контейнер ноды + отдельный эмулятор hc2a (env NODE_NAME=s2a, 8008→8021).
        var containers = new[]
        {
            Container("as-s2a", "s2a", ports: [new PortMap(5432, 5435)]),
            Container("as-hc2a", "hc2a", env: ["NODE_NAME=s2a"], ports: [new PortMap(8008, 8021)]),
        };

        // Act
        var found = NodeMatcher.Match("local", containers, ["s2a"]);

        // Assert: pg из контейнера ноды, patroni из сайдкара.
        Assert.Equal(5435, found["s2a"].Pg);
        Assert.Equal(8021, found["s2a"].Patroni);
        Assert.Equal("as-s2a", found["s2a"].Object);
    }

    [Fact]
    public void Match_CanonicalPgwContainer_MatchesByHostname()
    {
        // Arrange: наша нода pgw-demo-s2-s2a (hostname s2a) со всеми тремя портами.
        var containers = new[]
        {
            Container("pgw-demo-s2-s2a", "s2a",
                ports: [new PortMap(5432, 15432), new PortMap(8008, 18008), new PortMap(6432, 16432)]),
        };

        // Act
        var found = NodeMatcher.Match("local", containers, ["s2a"]);

        // Assert
        Assert.Equal((15432, 18008, 16432), (found["s2a"].Pg, found["s2a"].Patroni, found["s2a"].Doorman));
    }

    [Fact]
    public void Match_AmbiguousNodeContainer_SkipsName()
    {
        // Arrange: два живых контейнера претендуют на имя ноды.
        var containers = new[]
        {
            Container("a1", "s2a", ports: [new PortMap(5432, 5435)]),
            Container("a2", "s2a", ports: [new PortMap(5432, 5436)]),
        };

        // Act
        var found = NodeMatcher.Match("local", containers, ["s2a"]);

        // Assert: неоднозначность → безопасный пропуск (журнал — в AdoptionProcess, spec §3.1).
        Assert.False(found.ContainsKey("s2a"));
    }

    [Fact]
    public void Match_UnknownName_NotPresent()
    {
        // Arrange
        var containers = new[] { Container("as-s1a", "s1a") };

        // Act
        var found = NodeMatcher.Match("local", containers, ["s2a"]);

        // Assert
        Assert.Empty(found);
    }
}
```

Выход: тесты не компилируются (`NodeMatcher` нет).
Проверка: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~NodeMatcher"` → Compile ERROR.
Spec: §3.1.

- [ ] **Step 2.2: Реализация NodeMatcher**

Вход: failing-тесты.
Действие: создать `src/PgWorker.Docker/Drivers/NodeMatcher.cs`:

```csharp
using PgWorker.Core.Model;
using PgWorker.Docker.Engine;

namespace PgWorker.Docker.Drivers;

/// <summary>Нода, опознанная docker-инспекцией (spec §3.1, arch/14 §5 J AD1):
/// Host — docker-хост находки, Object — имя контейнера ноды; Patroni может
/// прийти из сайдкара (env NODE_NAME), Doorman=0 при отсутствии биндинга.</summary>
public sealed record DiscoveredNode(string NodeName, string Host, string Object, int Pg, int Patroni, int Doorman)
{
    public NodeAddress ToAddress() => new(Host, new NodePorts(Pg, Patroni, Doorman), Object);
}

/// <summary>Чистый матчинг контейнер↔нода (spec §3.1): контейнер = нода при
/// hostname==имя ИЛИ alias содержит имя; сайдкар Patroni — env NODE_NAME==имя;
/// порты — public-биндинги 5432/8008/6432; неоднозначность → имя пропускается
/// (безопасный отказ; журналирование пропуска — задача AdoptionProcess).</summary>
public static class NodeMatcher
{
    public static IReadOnlyDictionary<string, DiscoveredNode> Match(
        string dockerHost,
        IEnumerable<(DockerContainer Container, DockerContainerInspect Inspect)> containers,
        IReadOnlyCollection<string> nodeNames)
    {
        var names = new HashSet<string>(nodeNames, StringComparer.Ordinal);
        var candidates = new Dictionary<string, List<(DockerContainer C, DockerContainerInspect I)>>();
        foreach (var item in containers)
        {
            foreach (var name in NamesOf(item.I))
            {
                if (!names.Contains(name))
                    continue;
                if (!candidates.TryGetValue(name, out var list))
                    candidates[name] = list = [];
                list.Add(item);
            }
        }

        var result = new Dictionary<string, DiscoveredNode>();
        foreach (var (name, list) in candidates)
        {
            var nodeContainers = list.Where(IsNode).ToList();
            if (nodeContainers.Count != 1)
                continue; // 0 = только сайдкар, >1 = неоднозначность → пропуск (spec §3.1)

            var (c, i) = nodeContainers[0];
            var patroni = PublicPort(i, 8008);
            if (patroni == 0)
            {
                // Patroni-сайдкар этой ноды (стендовые эмуляторы hc*, env NODE_NAME).
                patroni = list.Where(s => !IsNode(s) && HasEnv(s.I, "NODE_NAME", name))
                    .Select(s => PublicPort(s.I, 8008)).FirstOrDefault(p => p > 0);
            }

            result[name] = new DiscoveredNode(name, dockerHost, c.Names[0],
                PublicPort(i, 5432), patroni, PublicPort(i, 6432));
        }

        return result;

        static IEnumerable<string> NamesOf(DockerContainerInspect i)
        {
            yield return i.Hostname;
            foreach (var alias in i.Aliases)
                yield return alias;
        }

        static bool IsNode((DockerContainer C, DockerContainerInspect I) item)
            => item.I.Env.All(e => !e.StartsWith("NODE_NAME=", StringComparison.Ordinal));

        static bool HasEnv(DockerContainerInspect i, string key, string value)
            => i.Env.Any(e => e == $"{key}={value}");

        static int PublicPort(DockerContainerInspect i, int containerPort)
            => i.Ports.FirstOrDefault(p => p.ContainerPort == containerPort) is { } map ? map.HostPort : 0;
    }
}
```

Выход: матчинг как чистая функция (юнит-тестируется без docker).
Проверка: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~NodeMatcher"` → PASS.
Spec: §3.1.

- [ ] **Step 2.3: `DockerContainerInspect` + `InspectContainerAsync` в движке**

Вход: матчеру нужен инспект из Engine API.
Действие: в `IDockerEngine.cs` добавить record и метод (рядом с `DockerContainer`):

```csharp
// Инспект контейнера GET /containers/<id>/json: hostname, сетевые алиасы, env
// и host-биндинги — вход матчинга усыновления (spec §3.1).
public sealed record DockerContainerInspect(
    string Id, string Hostname, string[] Aliases, string[] Env, PortMap[] Ports);

// GET /containers/<id>/json — инспект для матчинга нод усыновления.
Task<Result<DockerContainerInspect>> InspectContainerAsync(string id, CancellationToken ct);
```

В `DockerEngine.cs` — реализация по образцу `ListContainersAsync`:

```csharp
public async Task<Result<DockerContainerInspect>> InspectContainerAsync(string id, CancellationToken ct)
    => await Result<DockerContainerInspect>.FromAsync(async () =>
    {
        var dto = await GetAsync<ContainerInspectDto>($"/containers/{Uri.EscapeDataString(id)}/json", ct)
                  ?? throw new ApplicationException($"инспект контейнера {id} пуст");
        var ports = (dto.NetworkSettings?.Ports ?? [])
            .SelectMany(kv => (kv.Value ?? [])
                .Where(b => int.TryParse(b.HostPort, out _))
                .Select(b => new PortMap(int.Parse(kv.Key.Split('/')[0]), int.Parse(b.HostPort))))
            .Distinct().ToArray();
        var aliases = (dto.NetworkSettings?.Networks ?? new Dictionary<string, NetworkDto>())
            .Values.SelectMany(n => n.Aliases ?? []).Distinct().ToArray();
        return new DockerContainerInspect(dto.Id, dto.Config?.Hostname ?? "", aliases, dto.Config?.Env ?? [], ports);
    });
```

И приватные DTO (рядом с `ContainerDto`, ≈стр. 631):

```csharp
private sealed class ContainerInspectDto
{
    [JsonPropertyName("Id")] public string? Id { get; set; }
    [JsonPropertyName("Config")] public ContainerConfigDto? Config { get; set; }
    [JsonPropertyName("NetworkSettings")] public NetworkSettingsDto? NetworkSettings { get; set; }
}

private sealed class ContainerConfigDto
{
    [JsonPropertyName("Hostname")] public string? Hostname { get; set; }
    [JsonPropertyName("Env")] public string[]? Env { get; set; }
}

private sealed class NetworkSettingsDto
{
    [JsonPropertyName("Ports")] public Dictionary<string, List<PortBindingDto>?>? Ports { get; set; }
    [JsonPropertyName("Networks")] public Dictionary<string, NetworkDto>? Networks { get; set; }
}

private sealed class NetworkDto
{
    [JsonPropertyName("Aliases")] public string[]? Aliases { get; set; }
}

private sealed class PortBindingDto
{
    [JsonPropertyName("HostIp")] public string? HostIp { get; set; }
    [JsonPropertyName("HostPort")] public string? HostPort { get; set; }
}
```

Выход: движок отдаёт инспект.
Проверка: `dotnet build src/PgWorker.Docker/PgWorker.Docker.csproj` → 0 warnings/errors.
Spec: §3.1.

- [ ] **Step 2.4: `IClusterDriver.InspectNodesAsync` + `ExecContainerAsync`**

Вход: движок и матчер готовы.
Действие: в `ClusterDriver.cs` — интерфейс (после `ExecNodeAsync`):

```csharp
// Docker-инспекция нод усыновления (spec §3.1, arch/14 §5 J AD1): по именам
// нод вернуть DiscoveredNode (host/object/порты). 0 находок — пустой словарь.
Task<Result<IReadOnlyDictionary<string, DiscoveredNode>>> InspectNodesAsync(
    IReadOnlyCollection<string> nodeNames, CancellationToken ct);

// Exec в контейнер по имени (docker-exec fallback для pg_dump усыновлённых
// нод, spec §3.3): 404/не найден — Failed.
Task<Result<string>> ExecContainerAsync(string containerName, IReadOnlyList<string> cmd, CancellationToken ct);
```

`PlainClusterDriver` — реализации. Важно: `NodeMatcher.Match` вызывается ОДИН раз на хост со ВСЕМИ парами (контейнер, инспект) этого хоста — только так работают merge patroni-порта из сайдкара (env NODE_NAME) и skip-on-ambiguity «два контейнера на имя → пропуск» (вызов Match по одному контейнеру молча взял бы первого и потерял сайдкар — поведение, покрываемое юнит-тестами `Match_PatroniSidecarByNodeNameEnv_MergesPatroniPort`/`Match_AmbiguousNodeContainer_SkipsName`, обязано сохраниться и в драйвере):

```csharp
public async Task<Result<IReadOnlyDictionary<string, DiscoveredNode>>> InspectNodesAsync(
    IReadOnlyCollection<string> nodeNames, CancellationToken ct)
{
    return await Result<IReadOnlyDictionary<string, DiscoveredNode>>.FromAsync(async () =>
    {
        var found = new Dictionary<string, DiscoveredNode>();
        foreach (var (host, engine) in _engines)
        {
            var list = await engine.ListContainersAsync("", all: false, ct);
            if (!list.IsSuccess)
                throw list.Error!; // хост недоступен — не тихий список (паттерн GetHostsAsync)

            // Собираем ВСЕ пары хоста: Match должен видеть и ноду, и её
            // patroni-сайдкар (env NODE_NAME), и пары конкурирующих контейнеров
            // (неоднозначность → пропуск имени) — один вызов на хост.
            var pairs = new List<(DockerContainer, DockerContainerInspect)>();
            foreach (var c in list.Value)
            {
                var inspect = await engine.InspectContainerAsync(c.Id, ct);
                if (inspect.IsSuccess)
                    pairs.Add((c, inspect.Value)); // контейнер исчез между list и inspect — не наша находка
            }

            foreach (var (name, node) in NodeMatcher.Match(host, pairs, nodeNames))
                if (!found.ContainsKey(name))
                    found[name] = node;
        }

        return (IReadOnlyDictionary<string, DiscoveredNode>)found;
    });
}

public async Task<Result<string>> ExecContainerAsync(string containerName, IReadOnlyList<string> cmd, CancellationToken ct)
{
    return await Result<string>.FromAsync(async () =>
    {
        foreach (var engine in _engines.Values)
        {
            var list = await engine.ListContainersAsync(containerName, all: false, ct);
            if (!list.IsSuccess)
                throw list.Error!;
            if (list.Value.FirstOrDefault(c => c.Names.Contains(containerName)) is not { } hit)
                continue;
            var exec = await engine.ExecAsync(hit.Id, cmd, ct);
            if (!exec.IsSuccess)
                throw exec.Error!;
            return exec.Value;
        }

        throw new ApplicationException($"контейнер '{containerName}' не найден на хостах драйвера");
    });
}
```

`SwarmClusterDriver` (≈стр. 337) — реализация-заглушка с осознанным комментарием и рабочим `ExecContainerAsync` через таски:

```csharp
// Усыновление swarm-кластеров: за пределами текущей задачи (стенд plain,
// spec §3.1); при необходимости — инспект тасков сервисов. Exec — по таску.
public Task<Result<IReadOnlyDictionary<string, DiscoveredNode>>> InspectNodesAsync(
    IReadOnlyCollection<string> nodeNames, CancellationToken ct)
    => Task.FromResult(Result<IReadOnlyDictionary<string, DiscoveredNode>>.Success(
        (IReadOnlyDictionary<string, DiscoveredNode>)new Dictionary<string, DiscoveredNode>()));
```

`ExecContainerAsync` в swarm — по образцу существующего `ExecNodeAsync` (стр. 337): имя сервиса = containerName → `ListTasksAsync` → `ContainerId` → `engine.ExecAsync`.

Действие (стабы): дополнить `StubScaleDriver` и unit-фейк `Provisioning/Fakes.cs` двумя методами (по компиляции): стаб возвращает пустой словарь / `Result<string>.Success("")` (или recorded-список для тестов T3).

Выход: интерфейс драйвера расширен, всё собирается.
Проверка: `dotnet build src/PgWorker.App/PgWorker.App.csproj` → 0 errors (включая тестовые проекты: `dotnet build src/tests/PgWorker.UnitTests` …). Все прежние тесты зелёные: `dotnet test src/tests/PgWorker.UnitTests`.
Spec: §3.1, §3.3.

- [ ] **Step 2.5: Коммит**

Вход: сборка и тесты зелёные.
Действие:

```bash
git add src/PgWorker.Docker src/tests
git commit -m "feat(pgworker): docker-инспекция нод — InspectContainerAsync, NodeMatcher, InspectNodesAsync/ExecContainerAsync драйвера (adopt-repair T2)"
```

Выход: коммит.
Проверка: `git log --oneline -1`.
Spec: §3.1, §3.3.

---

### Task 3: AdoptionProcess (AD0–AD4) + интеграция в цикл

**Files:**
- Create: `src/PgWorker.Provisioning/Processes/AdoptionProcess.cs`
- Modify: `src/PgWorker.App/Loops/ClusterProcesses.cs` (интерфейс + реализация + ctor)
- Modify: `src/PgWorker.App/Loops/ReconcileLoop.cs` (Active-ветка, ≈стр. 168–191)
- Modify: `src/PgWorker.App/Program.cs` (DI, после NodeSupervisor ≈стр. 135)
- Test: `src/tests/PgWorker.UnitTests/Provisioning/AdoptionProcessTests.cs` (новый; фейки `Fakes.cs`)
- Test: `src/tests/PgWorker.IntegrationTests/Etcd/AdoptionContractTests.cs` (новый; `EtcdFixture`, `StubScaleDriver`)

**Interfaces:**
- Consumes: `IClusterDriver.InspectNodesAsync` (T2), `IAppSecretEnsurer.EnsureAsync(cluster, ct) → Result<AppCredentials>`, `IAppParamsEnsurer.EnsureShardAsync(cluster, shard, IEnumerable<string> nodes, ct)`, `ShardEndpoints.ReadPortAllocAsync/ResolveMasterAsync/AdminDsn` (в T3 резолв — СУЩЕСТВУЮЩАЯ 3-аргументная сигнатура; T4 добавит параметр `cluster` и поправит вызов здесь же), `ISqlExecutor.{EnsureDatabaseAsync, ExecuteScalarAsync, ExecuteAsync}`, `DatabaseProvisioner.BuildRoleGuardsSql/BuildRoleExecSql/BuildAlterAppPasswordSql`, `WorkJournal.WritePhaseAsync`, `ClaimStore.IsMine`, `Portalloc.Serialize/Parse` (T1).
- Produces: `Task<Result<ProcessOutcome>> TickAsync(ClusterSnapshot snap, CancellationToken ct)` у `AdoptionProcess`; `Task<Result<ProcessOutcome>> AdoptAsync(ClusterSnapshot snap, CancellationToken ct)` у `IClusterProcesses`.

- [ ] **Step 3.1: Failing unit-тесты процесса**

Вход: семантика AD0–AD4 (spec §3.2): no-op при полном portalloc; merge отсутствующих записей; nodes-ключи put-if-absent; тихий skip при 0 находок; ensure-фазы вызываются; **journal-запись пропущенных имён при частичном/неоднозначном матчинге (spec §3.1 — безопасный отказ с журналом)**.
Действие: `AdoptionProcessTests.cs` на фейках (etcd-фейк и фейк-драйвер из `Fakes.cs`; драйвер-фейк расширить записываемым `Dictionary<string, DiscoveredNode> InspectResult`; фейк `WorkJournal` — записывает фазы в список). Четыре ключевых теста:

```csharp
[Fact]
public async Task TickAsync_FullPortalloc_NoOpWithoutEnsures()
{
    // Arrange: кластер Active, у обоих шардов dsn, portalloc полон — усыновлять нечего.
    var snap = SnapshotActive(shards: ["s1", "s2"]);
    var etcd = new FakeEtcd();
    await etcd.PutPortallocAsync("demo", """{"s1/s1a":{"host":"local","pg":15432,"patroni":18008,"doorman":16432}}""");
    // (FakeEtcd-хелперы по образцу ShardEndpointsTests.Fakes)
    var adoption = NewAdoption(etcd, inspect: []);

    // Act
    var outcome = await adoption.TickAsync(snap, CancellationToken.None);

    // Assert: Done, portalloc не перезаписан, nodes-ключи не тронуты.
    Assert.Equal(ProcessOutcome.Done, outcome.Value);
    Assert.Null(await etcd.GetAsync("/clusters/demo/shards/s1/nodes/s1a/state"));
}

[Fact]
public async Task TickAsync_ExternalShard_MergesPortallocWithObject()
{
    // Arrange: members /service/demo-s1/members/{s1a,s1b} живы, portalloc пуст,
    // драйвер нашёл as-контейнеры (5433/8011 и 5434/8012).
    var snap = SnapshotActive(shards: ["s1"]);
    var etcd = new FakeEtcd();
    await etcd.PutServiceMembersAsync("demo", "s1", ["s1a", "s1b"]);
    var adoption = NewAdoption(etcd, inspect: new Dictionary<string, DiscoveredNode>
    {
        ["s1a"] = new("s1a", "local", "as-s1a", 5433, 8011, 0),
        ["s1b"] = new("s1b", "local", "as-s1b", 5434, 8012, 0),
    });

    // Act
    var outcome = await adoption.TickAsync(snap, CancellationToken.None);

    // Assert: portalloc дополнен записями с object; nodes-ключи = RUNNING.
    Assert.Equal(ProcessOutcome.Done, outcome.Value);
    var raw = await etcd.GetAsync("/pgworker/portalloc/demo");
    Assert.Contains("\"object\":\"as-s1a\"", raw);
    Assert.Equal("RUNNING", await etcd.GetAsync("/clusters/demo/shards/s1/nodes/s1a/state"));
}

[Fact]
public async Task TickAsync_NoContainersFound_SilentSkip()
{
    // Arrange: members есть, docker находок 0 — не наш docker-домен (spec §2.5).
    var snap = SnapshotActive(shards: ["s1"]);
    var etcd = new FakeEtcd();
    await etcd.PutServiceMembersAsync("demo", "s1", ["s1a"]);
    var adoption = NewAdoption(etcd, inspect: []);

    // Act
    var outcome = await adoption.TickAsync(snap, CancellationToken.None);

    // Assert: Done (тихий skip) — portalloc/nodes не тронуты, журнала adopt нет.
    Assert.Equal(ProcessOutcome.Done, outcome.Value);
    Assert.Null(await etcd.GetAsync("/pgworker/portalloc/demo"));
}

[Fact]
public async Task TickAsync_PartialDiscovery_JournalsSkippedNodes()
{
    // Arrange: members s1a+s1b, инспекция опознала только s1a (s1b —
    // неоднозначный матчинг двух контейнеров, безопасный пропуск spec §3.1).
    var snap = SnapshotActive(shards: ["s1"]);
    var etcd = new FakeEtcd();
    await etcd.PutServiceMembersAsync("demo", "s1", ["s1a", "s1b"]);
    var journal = new Fakes.RecordingJournal();
    var adoption = NewAdoption(etcd, journal, inspect: new Dictionary<string, DiscoveredNode>
    {
        ["s1a"] = new("s1a", "local", "as-s1a", 5433, 8011, 0),
    });

    // Act
    var outcome = await adoption.TickAsync(snap, CancellationToken.None);

    // Assert: s1a усыновлена; в журнале adopt/skipped с именем s1b (оператор
    // видит, какая нода не усыновлена и почему — spec §3.1 «journal-запись»).
    Assert.Equal(ProcessOutcome.Done, outcome.Value);
    var skipped = journal.Entries.Single(e => e.Phase == "skipped");
    Assert.Contains("s1b", skipped.Message);
}
```

Хелперы `SnapshotActive/FakeEtcd/NewAdoption` — по образцам `AddShardProcessTests`/`ShardEndpointsTests` (в Fakes.cs уже есть FakeEtcd-паттерн); `RecordingJournal` — фейк WorkJournal по существующим фейкам.
Выход: не компилируется (`AdoptionProcess` нет).
Проверка: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~AdoptionProcess"` → Compile ERROR.
Spec: §3.2, §3.1 (журнал пропуска).

- [ ] **Step 3.2: Реализация AdoptionProcess**

Вход: failing-тесты; паттерн тикового процесса — `AppPasswordRotator` (клэйм-гвард, journal, failover-обёртки).
Действие: создать `AdoptionProcess.cs`:

```csharp
using System.Text.Json;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Docker.Drivers;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Sql;

namespace PgWorker.Provisioning.Processes;

/// <summary>
/// Усыновление кластера (spec §3.2, arch/14 §5 J AD0–AD4): Active-кластер с
/// dsn-шардами без записей portalloc получает адреса из HA-контура + docker
/// (InspectNodesAsync) и переходит в обычный домен воркера. «Не наших»
/// объектов не существует; 0 docker-находок — тихий skip (кластер вне
/// docker-хостов воркера). Идемпотентно: только отсутствующие записи.
/// </summary>
public sealed class AdoptionProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ShardEndpoints shards,
    ISqlExecutor sql,
    IAppSecretEnsurer appSecret,
    IAppParamsEnsurer appParams,
    InstallSecrets secrets,
    ClaimStore claims,
    WorkJournal journal,
    TimeProvider clock,
    Func<CancellationToken, Task<Result>>? snapshot = null)
{
    public async Task<Result<ProcessOutcome>> TickAsync(ClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Мутации — только держателем живого клэйма (инвариант arch/14 §3.3).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"adopt {cluster}: клэйм не наш (или потерян) — мутации запрещены"));
        if (snap.Config.State != ClusterState.Active)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done); // провижининг/демонтаж — свои процессы

        // AD1: кандидаты — шарды с dsn; недостающие ноды = HA-members − portalloc.
        var existing = await shards.ReadPortAllocAsync(cluster, ct);
        if (!existing.IsSuccess)
            return Result<ProcessOutcome>.Failed(existing.Error!);

        var missingByShard = new Dictionary<string, List<string>>();
        foreach (var shard in snap.Shards.Where(s => s.Dsn is not null && !s.ToRemove))
        {
            var members = await ReadMemberNamesAsync(cluster, shard.Name, ct);
            if (!members.IsSuccess)
                return Result<ProcessOutcome>.Failed(members.Error!);
            var missing = members.Value
                .Where(n => !existing.Value.ContainsKey($"{shard.Name}/{n}"))
                .ToList();
            if (missing.Count > 0)
                missingByShard[shard.Name] = missing;
        }

        if (missingByShard.Count == 0)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done); // всё на месте — no-op

        var wanted = missingByShard.Values.SelectMany(v => v).Distinct().ToList();
        var discovered = await driver.InspectNodesAsync(wanted, ct);
        if (!discovered.IsSuccess)
            return Result<ProcessOutcome>.Failed(discovered.Error!);
        if (discovered.Value.Count == 0)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done); // тихий skip (spec §2.5)

        await journal.WritePhaseAsync(cluster, "adopt", "started", claims.InstanceId, null, ct);

        // Spec §3.1: неопознанные ноды (неоднозначный матчинг/нет контейнера) —
        // безопасный пропуск С журнальной записью: оператор видит, кто не
        // усыновлен; усыновление частично (остальные ноды — следующим тиком
        // после разбора, идемпотентность merge это допускает).
        var skipped = wanted.Where(n => !discovered.Value.ContainsKey(n)).ToList();
        if (skipped.Count > 0)
            await journal.WritePhaseAsync(cluster, "adopt", "skipped", claims.InstanceId,
                $"контейнеры не опознаны (неоднозначность/отсутствие): {string.Join(", ", skipped)}", ct);

        // AD2: merge portalloc — только отсутствующие записи, под клэймом.
        var merged = new Dictionary<string, NodeAddress>(existing.Value);
        foreach (var (name, node) in discovered.Value)
        {
            var shard = missingByShard.First(kv => kv.Value.Contains(name)).Key;
            merged[$"{shard}/{name}"] = node.ToAddress();
        }

        var put = await PutAsync($"/pgworker/portalloc/{cluster}", Portalloc.Serialize(merged), ct);
        if (!put.IsSuccess)
            return await FailAsync(cluster, put.Error!, ct);

        // AD3: nodes-ключи put-if-absent (декларация следует за фактом).
        foreach (var (shard, nodes) in missingByShard)
        {
            var ensuredNodes = nodes.Where(n => discovered.Value.ContainsKey(n)).ToList();
            foreach (var node in ensuredNodes)
            {
                var key = $"/clusters/{cluster}/shards/{shard}/nodes/{node}/state";
                var txn = await TxnPutIfAbsentAsync(key, "RUNNING", ct);
                if (!txn.IsSuccess)
                    return await FailAsync(cluster, txn.Error!, ct);
            }

            var appParamsDone = await appParams.EnsureShardAsync(cluster, shard, ensuredNodes, ct);
            if (!appParamsDone.IsSuccess)
                return await FailAsync(cluster, appParamsDone.Error!, ct);
        }

        // AD3: app-секрет кластера + роли БД на мастерах (P1.5/P2.3-паттерн).
        var creds = await appSecret.EnsureAsync(cluster, ct);
        if (!creds.IsSuccess)
            return await FailAsync(cluster, creds.Error!, ct);

        foreach (var shard in snap.Shards.Where(s => missingByShard.ContainsKey(s.Name)))
        {
            var master = await shards.ResolveMasterAsync(shard, merged, ct);
            if (!master.IsSuccess)
                return await FailAsync(cluster, master.Error!, ct);
            if (master.Value is null)
                return await FailAsync(cluster, new ApplicationException(
                    $"adopt {cluster}: мастер шарда '{shard.Name}' не определён — повтор следующим тиком"), ct);

            var dsn = ShardEndpoints.AdminDsn(master.Value, snap.Config.DbName, secrets);
            var provisioned = await EnsureShardDatabaseAsync(dsn, snap, creds.Value, ct);
            if (!provisioned.IsSuccess)
                return await FailAsync(cluster, provisioned.Error!, ct);
        }

        // AD4: снапшот P12 (точка изменения, best-effort) + journal done.
        if (snapshot is not null)
            await snapshot(ct); // неудача — не повод откатывать усыновление (журналируется SnapshotJob)

        await journal.WritePhaseAsync(cluster, "adopt", "done", claims.InstanceId,
            $"усыновлено нод: {discovered.Value.Count} ({string.Join(", ", discovered.Value.Keys.OrderBy(n => n))})"
            + (skipped.Count > 0 ? $"; пропущено: {skipped.Count}" : ""), ct);
        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    // Имена членов HA-контура = последние сегменты ключей /service/<scope>/members/*.
    private async Task<Result<IReadOnlyList<string>>> ReadMemberNamesAsync(
        string cluster, string shard, CancellationToken ct)
    {
        var range = await RangeAsync($"/service/{cluster}-{shard}/members/", ct);
        if (!range.IsSuccess)
            return Result<IReadOnlyList<string>>.Failed(range.Error!);
        return Result<IReadOnlyList<string>>.Success(
            (IReadOnlyList<string>)range.Value
                .Select(kv => kv.Key.Split('/')[^1])
                .Where(n => n.Length > 0)
                .Distinct().OrderBy(n => n).ToList());
    }

    // Ensure БД + ролей бакетного слоя на мастере усыновляемого шарда —
    // идемпотентные тексты P2.3 (gexec-гварды → exec, ALTER app-пароля).
    private async Task<Result> EnsureShardDatabaseAsync(
        string dsn, ClusterSnapshot snap, AppCredentials app, CancellationToken ct)
    {
        var db = await sql.EnsureDatabaseAsync(dsn, snap.Config.DbName, ct);
        if (!db.IsSuccess)
            return db;

        foreach (var guard in DatabaseProvisioner.BuildRoleGuardsSql(
                     secrets, app, snap.Config.BucketAdminUser, snap.Config.BucketAdminPassword))
        {
            var role = await sql.ExecuteScalarAsync(dsn, guard, ct);
            if (!role.IsSuccess)
                return role;
            if (role.Value is string { Length: > 0 } create)
            {
                var exec = await sql.ExecuteAsync(dsn, create, ct);
                if (!exec.IsSuccess)
                    return exec;
            }
        }

        foreach (var execSql in DatabaseProvisioner.BuildRoleExecSql(snap.Config.BucketAdminUser))
        {
            var exec = await sql.ExecuteAsync(dsn, execSql, ct);
            if (!exec.IsSuccess)
                return exec;
        }

        var alter = await sql.ExecuteAsync(dsn, DatabaseProvisioner.BuildAlterAppPasswordSql(app), ct);
        return alter;
    }

    private async Task<Result> FailAsync(string cluster, Exception error, CancellationToken ct)
    {
        await journal.WritePhaseAsync(cluster, "adopt", "failed", claims.InstanceId, error.Message, ct);
        return Result<ProcessOutcome>.Failed(error);
    }

    // Failover-обёртки (паттерн AppPasswordRotator).
    private async Task<Result> PutAsync(string key, string value, CancellationToken ct) { /* цикл по endpoints: etcd.PutAsync */ }
    private async Task<Result> TxnPutIfAbsentAsync(string key, string value, CancellationToken ct) { /* txn TxnCompare.NotExists(key) → Put (эталон ClaimStore.TryPutLeasedKeyAsync) */ }
    private async Task<Result<IReadOnlyList<Kv>>> RangeAsync(string prefix, CancellationToken ct) { /* цикл по endpoints: etcd.RangeAsync */ }
}
```

(`PutAsync/TxnPutIfAbsentAsync/RangeAsync` — тела по failover-паттерну `ShardEndpoints.WithFailoverAsync`, включая `TxnRequest.Of([TxnCompare.NotExists(key)], [new TxnOp.Put(key, value, null)])` через `etcd.TxnAsync(endpoint, …)`; put-if-absent в кодовой базе — именно `TxnCompare.NotExists` (ключа нет ↔ version==0), фабрики `VersionEqual` у `TxnCompare` нет. Вызов `shards.ResolveMasterAsync(shard, merged, ct)` здесь — текущая 3-аргументная сигнатура; задача T4 добавит параметр `cluster` и поправит этот вызов вместе со всеми остальными.)

Выход: процесс AD0–AD4 с журнальной записью пропусков.
Проверка: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~AdoptionProcess"` → PASS.
Spec: §3.2, §3.1 (journal-запись при неоднозначности).

- [ ] **Step 3.3: Интеграция в цикл (ClusterProcesses + ReconcileLoop + DI)**

Вход: процесс готов.
Действие:
1. `ClusterProcesses.cs`: в `IClusterProcesses` добавить `Task<Result<ProcessOutcome>> AdoptAsync(ClusterSnapshot snap, CancellationToken ct);` — комментарий «Усыновление: адреса внешних нод в portalloc (spec §3.2, arch/14 §5 J)»; в `ClusterProcesses` — ctor-параметр `AdoptionProcess adopt` и метод `=> adopt.TickAsync(snap, ct);`.
2. `ReconcileLoop.cs`, Active-ветка (default-кейс ≈стр. 167): сразу после `RunSuperviseAsync`-блока и ДО `scale-shards`:

```csharp
// Усыновление (spec §3.2, arch/14 §5 J): адреса dsn-шард без portalloc —
// до scale (add смотрит pinned portalloc) и до repair/moves (SQL нужен адрес).
await RunClusterOpAsync(cluster, "adopt",
    () => processes.AdoptAsync(snap, ct), ct);
```

3. `Program.cs`: DI после `ShardEndpoints` (≈стр. 140), паттерн соседних процессов:

```csharp
// Усыновление кластеров (spec §3.2): адреса из HA-контура+docker → portalloc;
// ensure секретов/ролей — общие ensurer'ы выше.
builder.Services.AddSingleton(sp => new AdoptionProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ShardEndpoints>(),
    sp.GetRequiredService<ISqlExecutor>(),
    sp.GetRequiredService<IAppSecretEnsurer>(),
    sp.GetRequiredService<IAppParamsEnsurer>(),
    sp.GetRequiredService<InstallSecrets>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<TimeProvider>(),
    SnapshotDelegate(sp.GetRequiredService<SnapshotJob>())));
```

Выход: Active-ветка = supervise → **adopt** → scale → rotate → repair (T7) → evacuate → moves.
Проверка: `dotnet build src/PgWorker.App/PgWorker.App.csproj` → 0 warnings; `dotnet test src/tests/PgWorker.UnitTests` → все зелёные (включая тесты ReconcileLoop — при изменении порядка обновить моки `IClusterProcesses`, они есть в unit `App/`).
Spec: §3.2 (порядок), §4 фаза 2.

- [ ] **Step 3.4: Integration-тест контракта усыновления (etcd реален)**

Вход: паттерн `ShardScaleContractTests` (EtcdFixture + StubScaleDriver).
Действие: `AdoptionContractTests.cs`:

```csharp
[Fact]
public async Task Adopt_ExternalCluster_WritesPortallocAndNodeStates()
{
    // Arrange: живой etcd (EtcdFixture); сид «внешнего» кластера: config Active
    // (без state), shards/s1/{dsn,replicas}, members scope; portalloc нет.
    // StubScaleDriver.InspectResult: s1a/s1b → as-контейнеры.
    // (хелперы put-ключей — как в ShardScaleContractTests)

    // Act: два тика (идемпотентность: второй — no-op).
    await adoption.TickAsync(snap, ct);
    await adoption.TickAsync(snap, ct);

    // Assert: portalloc содержит object-записи обеих нод; nodes-ключи RUNNING;
    // journal /pgworker/work/demo содержит op adopt phase done.
}

[Fact]
public async Task Adopt_PartialDiscovery_JournalContainsSkipped()
{
    // Arrange: members s1a+s1b, InspectResult — только s1a (s1b не опознана).
    // Act: тик.
    // Assert: journal содержит фазу skipped с именем s1b; s1a в portalloc.
}
```

SQL/секретные части в тестах — на моках `ISqlExecutor`/`IAppSecretEnsurer` (etcd-контракт проверяем, SQL-механика уже покрыта unit-тестами P2.3).
Выход: контракт закреплён (включая skipped-журнал).
Проверка: `dotnet test src/tests/PgWorker.IntegrationTests --filter "FullyQualifiedName~AdoptionContract"` (`DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1`) → PASS.
Spec: §6 integration (adoption-контракт), §3.1.

- [ ] **Step 3.5: Коммит**

Вход: всё зелёное.
Действие:

```bash
git add src/PgWorker.Provisioning/Processes/AdoptionProcess.cs src/PgWorker.App src/tests
git commit -m "feat(pgworker): AdoptionProcess — усыновление кластеров без portalloc: docker+HA адреса, nodes-ключи, ensure секретов/ролей, журнал пропусков (adopt-repair T3)"
```

Выход: коммит.
Проверка: `git log --oneline -1`.
Spec: §3.2, §4 фаза 2.

---

### Task 4: Резолв мастера — шаг HA-leader в `ResolveMasterAsync`

**Files:**
- Modify: `src/PgWorker.Provisioning/Endpoints/ShardEndpoints.cs` (метод `ResolveMasterAsync`, ≈стр. 59–97; сигнатура + вызовы)
- Modify: вызовы: `src/PgWorker.Moves/Process/MoveProcess.cs` (≈стр. 363, 975, 1007, 1014), `src/PgWorker.Moves/Process/CutoverSequence.cs` (≈стр. 269, 276), `src/PgWorker.Moves/Process/AbortSequence.cs` (≈стр. 402 — приватный `ResolveAllDsnAsync`), `src/PgWorker.Provisioning/Processes/BucketEvacuator.cs` (≈стр. 109), **`src/PgWorker.Provisioning/Processes/AdoptionProcess.cs` (AD3-блок — вызов со старой 3-аргументной сигнатурой, создан этим планом в T3)**
- Test: `src/tests/PgWorker.UnitTests/Moves/ShardEndpointsTests.cs` (дополнить)

**Interfaces:**
- Produces: `Task<Result<NodeAddress?>> ResolveMasterAsync(string cluster, ShardSpec shard, IReadOnlyDictionary<string, NodeAddress> addresses, CancellationToken ct)` — добавлен первый параметр `cluster` (нужен для ключа `/service/<cluster>-<shard>/leader`). Цепочка: master-ключ (byName → byHostPort) → **HA-leader** → Patroni-REST.

- [ ] **Step 4.1: Failing-тесты HA-leader-шага и усыновлённого формата**

Вход: два сценария цепочки — (а) мастер-ключ протух/отсутствует, `/service/<C>-<X>/leader = {"name":"s2a"}` жив; (б) master-ключ усыновлённого формата `node:pg-port` (внешний HA-контур), portalloc с object-нодами (spec §6: «усыновлённый формат node:pg-port»).
Действие: в `ShardEndpointsTests.cs` (FakeEtcd уже сеет portalloc; добавить хелпер сида object-нод `SeedPortallocWithObjects` — тот же JSON с `"object":"as-…"`):

```csharp
[Fact]
public async Task ResolveMasterAsync_NoMasterKey_HaLeaderNameResolves()
{
    // Arrange: master-ключа нет; HA-контур называет лидера по имени ноды.
    var etcd = new Fakes.FakeEtcd();
    etcd.SeedPortalloc("shop"); // хелпер-сид из соседних тестов (s1/s1a,…)
    await etcd.PutAsync("/service/shop-s1/leader", """{"name":"s1a"}""");
    var endpoints = EndpointsOf(etcd);

    // Act: шард s1 без мастера, Patroni недоступен (фakes-проба молчит).
    var master = await endpoints.ResolveMasterAsync("shop", Shard1(null), addresses, CancellationToken.None);

    // Assert: адрес ноды s1a из portalloc — REST не понадобился.
    Assert.NotNull(master.Value);
    Assert.Equal(15432, master.Value.Ports.Pg);
}

[Fact]
public async Task ResolveMasterAsync_AdoptedMasterKeyNodePort_ResolvesByNodeName()
{
    // Arrange: усыновлённый кластер — master-ключ внешнего формата node:pg-port
    // (пишет эмулятор/Patroni-callback стендового контура), portalloc с
    // object-нодами; Patroni-REST недоступен.
    var etcd = new Fakes.FakeEtcd();
    etcd.SeedPortallocWithObjects("shop"); // s1/s1a: {"host":"local","pg":5433,"patroni":0,"doorman":0,"object":"as-s1a"}
    var endpoints = EndpointsOf(etcd);

    // Act: шард s1 с master-ключом "s1a:5433" (имя ноды:pg-порт).
    var master = await endpoints.ResolveMasterAsync("shop", Shard1("s1a:5433"), addresses, CancellationToken.None);

    // Assert: byName-резолв по части имени ноды — адрес object-ноды, REST не нужен.
    Assert.NotNull(master.Value);
    Assert.Equal(5433, master.Value.Ports.Pg);
    Assert.Equal("as-s1a", master.Value.Object);
}

[Fact]
public async Task ResolveMasterAsync_MasterKeyWinsOverHaLeader()
{
    // Arrange: master-ключ валиден (имя ноды) + есть HA-leader с ДРУГИМ именем.
    // Act / Assert: приоритет master-ключа — резолв по нему (цепочка spec §3.3).
}
```

(Плюс механическая правка всех существующих тестов `ResolveMasterAsync_*` на новую сигнатуру — добавляется аргумент `"shop"`.)
Выход: не компилируется (нет параметра cluster / нового шага).
Проверка: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~ShardEndpoints"` → Compile ERROR.
Spec: §3.3 (цепочка), §6 (unit: усыновлённый формат node:pg-port), arch/14 §5 F.

- [ ] **Step 4.2: Реализация шага + обновление вызовов**

Вход: failing-тесты.
Действие: в `ShardEndpoints.cs` — сигнатура и вставка шага между master-ключом и Patroni-перебором:

```csharp
public async Task<Result<NodeAddress?>> ResolveMasterAsync(
    string cluster, ShardSpec shard, IReadOnlyDictionary<string, NodeAddress> addresses, CancellationToken ct)
{
    // … существующий блок master-ключа (byName → byHostPort) без изменений …

    // Шаг 2 (spec §3.3): HA-лидер контура — имя из /service/<C>-<X>/leader,
    // адрес ноды из portalloc; работает без Patroni-REST (усыновлённые шарды,
    // окно failover с протухшим master-ключом).
    var leader = await GetAsync($"/service/{cluster}-{shard.Name}/leader", ct);
    if (leader.IsSuccess && leader.Value is { } leaderKv)
    {
        try
        {
            using var doc = JsonDocument.Parse(leaderKv.Value);
            if (doc.RootElement.TryGetProperty("name", out var name)
                && shardNodes.TryGetValue(name.GetString() ?? "", out var leaderAddr))
                return Result<NodeAddress?>.Success(leaderAddr);
        }
        catch (JsonException)
        {
            // битый leader-ключ — просто идём дальше по цепочке
        }
    }

    // … существующий Patroni /cluster-перебор без изменений …
}
```

Приватный `GetAsync(key, ct)` — failover-обёртка по `endpoints` (скопировать паттерн из `ReadPortAllocAsync`). Все вызовы `ResolveMasterAsync` обновить аргументом `cluster`/`snap.Config.Cluster`: `MoveProcess.cs` (4 места, ≈стр. 363, 975, 1007, 1014), `CutoverSequence.cs` (2 места, ≈стр. 269, 276), **`AbortSequence.cs` (1 место, ≈стр. 402 — приватный `ResolveAllDsnAsync`; пропуск сломает сборку)**, `BucketEvacuator.cs` (≈стр. 109), **`AdoptionProcess.cs` (AD3-блок, `shards.ResolveMasterAsync(shard, merged, ct)` → добавить аргумент `cluster`; файл создан в T3 с этим вызовом — пропуск сломает сборку)**.
Выход: цепочка (1)→(2)→(3); усыновлённый формат `node:pg-port` резолвится существующим byName-шагом (первый сегмент ключа = имя ноды).
Проверка: `dotnet test src/tests/PgWorker.UnitTests` → PASS; `dotnet build src/PgWorker.App/PgWorker.App.csproj` → 0 warnings (сборка всего графа подтверждает, что НИ ОДИН вызов не пропущен — включая AbortSequence и AdoptionProcess).
Spec: §3.3.

- [ ] **Step 4.3: Коммит**

Вход: зелёное.
Действие:

```bash
git add src/PgWorker.Provisioning/Endpoints/ShardEndpoints.cs src/PgWorker.Moves src/PgWorker.Provisioning/Processes src/tests
git commit -m "feat(pgworker): резолв мастера — HA-leader фоллбэк между master-ключом и Patroni-REST (adopt-repair T4)"
```

(каталог `src/PgWorker.Provisioning/Processes` вместо отдельного файла — стейджит и `BucketEvacuator.cs`, и `AdoptionProcess.cs`; NodeSupervisor/MasterKeyReconciler правятся только в T6, в коммит T4 не попадут.)
Выход: коммит.
Проверка: `git log --oneline -1`; `git status --short` — без остатков по `src/PgWorker.Provisioning`.
Spec: §3.3.

---

### Task 5: Advertised-правило + docker-exec fallback для pg_dump

**Files:**
- Modify: `src/PgWorker.Provisioning/Endpoints/ShardEndpoints.cs` (хелпер `HasAdoptedNodes`)
- Modify: `src/PgWorker.Moves/Ddl/MoveDdl.cs` (`DumpAsync` + `containerOverride`)
- Modify: `src/PgWorker.Moves/Process/MoveProcess.cs` (M1 exec-fallback ≈стр. 353–380; M2 advertised ≈стр. 413; M5 advertised ≈стр. 506)
- Test: `src/tests/PgWorker.UnitTests/Moves/MoveDdlTests.cs` (дополнить), `src/tests/PgWorker.UnitTests/Moves/MoveProcessPhasesTests.cs` (дополнить)

**Interfaces:**
- Consumes: `NodeAddress.Object` (T1), `IClusterDriver.ExecContainerAsync` (T2).
- Produces:
  - `public static bool HasAdoptedNodes(string shard, IReadOnlyDictionary<string, NodeAddress> addresses)` в `ShardEndpoints` — `true` если хоть одна нода шарда имеет `Object != null` (исполнитель подписок — внешняя сеть);
  - `public Task<Result<string>> DumpAsync(string cluster, string shard, string node, string dbname, string bucket, CancellationToken ct, string? containerOverride = null)` у `MoveDdl`.

- [ ] **Step 5.1: Failing-тесты**

Вход: правила — (а) `DumpAsync` с `containerOverride` шлёт exec в object-контейнер; (б) M2/M5 подменяют advertised только для канонических исполнителей.
Действие: юнит-тесты (фейк драйвера записывает вызовы — паттерн `MoveDdlTests`):

```csharp
[Fact]
public async Task DumpAsync_WithOverride_ExecsInObjectContainer()
{
    // Arrange: драйвер-фейк записывает ExecContainerAsync-вызовы.
    var driver = new Fakes.FakeDriver(); // дополнить фейк методом (см. Fakes.cs)
    var ddl = new MoveDdl(driver, sqlFake);

    // Act
    var dump = await ddl.DumpAsync("demo", "s2", "s2a", "demo", "bucket_13", CancellationToken.None,
        containerOverride: "as-s2a");

    // Assert: exec ушёл в object-контейнер, а не в pgw-имя.
    Assert.Contains(("as-s2a", "--schema=bucket_13"), driver.ContainerExecs);
}

[Fact]
public void HasAdoptedNodes_MixedShard_True()
{
    // Arrange: в шарде есть object-нода.
    var addresses = new Dictionary<string, NodeAddress>
    {
        ["s2/s2a"] = new("local", new NodePorts(5435, 8021, 0), "as-s2a"),
        ["s2/s2b"] = new("local", new NodePorts(5436, 8022, 0)),
    };

    // Act / Assert
    Assert.True(ShardEndpoints.HasAdoptedNodes("s2", addresses));
    Assert.False(ShardEndpoints.HasAdoptedNodes("s1", addresses));
}
```

Плюс в `MoveProcessPhasesTests` — кейс: у приёмника object-ноды → `CREATE SUBSCRIPTION`-SQL содержит хост dsn-ключа без `host.docker.internal` (фейк `IMoveSqlExecutor` перехватывает conninfo).
Выход: не компилируется.
Проверка: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~MoveDdl|FullyQualifiedName~MoveProcessPhases"` → Compile ERROR.
Spec: §3.3.

- [ ] **Step 5.2: Реализация**

Вход: failing-тесты.
Действие:
1. `ShardEndpoints.cs` — хелпер:

```csharp
// Внешний ли шард-исполнитель подписок (spec §3.3): object-ноды живут вне
// pgw-net и видят адреса dsn-ключа напрямую — подмена advertised ломает подключение.
public static bool HasAdoptedNodes(string shard, IReadOnlyDictionary<string, NodeAddress> addresses)
    => addresses.Any(p => p.Key.StartsWith($"{shard}/", StringComparison.Ordinal) && p.Value.Object is not null);
```

2. `MoveDdl.DumpAsync`:

```csharp
public Task<Result<string>> DumpAsync(
    string cluster, string shard, string node, string dbname, string bucket,
    CancellationToken ct, string? containerOverride = null)
{
    if (!MoveNames.ValidateIdentifier(bucket))
        throw new ArgumentException($"недопустимое имя бакета: '{bucket}' (шаблон ^[a-z][a-z0-9_]*)");

    var cmd = new[]
    {
        "su", "postgres", "-c",
        $"pg_dump --schema-only --no-owner --no-privileges --schema={bucket} {dbname}"
    };

    // Усыновлённая нода (spec §3.3): pg_dump в её фактический контейнер
    // (postgres-образ несёт утилиты), а не в каноническое pgw-имя.
    return containerOverride is { Length: > 0 }
        ? driver.ExecContainerAsync(containerOverride, cmd, ct)
        : driver.ExecNodeAsync(cluster, shard, node, cmd, ct);
}
```

3. `MoveProcess.cs`:
   - `RunMovePhasesAsync` (M1-блок ≈стр. 360): после резолва `srcMaster` → `var dump = await ddl.DumpAsync(cluster, owner, entry.Split('/')[1], snap.Config.DbName, bucket, ct, containerOverride: master.Object);` (`master.Object` — `null` для канонических: поведение не меняется).
   - M2 (≈стр. 413) — advertised по исполнителю (приёмник move = `to`):

```csharp
// Advertised-правило (spec §3.3): подмена только для канонических
// pgw-исполнителей — внешний приёмник видит адреса dsn напрямую.
var advertised = await AdvertisedForShardAsync(cluster, to, ct);
… ShardEndpoints.MoverConninfo(srcShard.Dsn!, secrets, advertised) …
```

   - M5 (≈стр. 506) — исполнитель `sub_rb` = бывший владелец (`owner`): `var advertised = await AdvertisedForShardAsync(cluster, owner, ct);` и передать в `MoverConninfo(dstShard.Dsn!, secrets, advertised)`.
   - Приватный хелпер (в `MoveProcess`, чтение адресов уже кэшировано `shards.ReadPortAllocAsync` в M1/M5-контексте — читать один раз и передавать):

```csharp
// Подмена advertised-хоста только для канонических исполнителей подписок
// (spec §3.3): усыновлённый (object) исполнитель в compose-сети резолвит
// адреса dsn-ключа сам — host.docker.internal сломал бы подключение.
private async Task<string?> AdvertisedForShardAsync(string cluster, string shard, CancellationToken ct)
{
    var addresses = await shards.ReadPortAllocAsync(cluster, ct);
    return !addresses.IsSuccess || ShardEndpoints.HasAdoptedNodes(shard, addresses.Value)
        ? null
        : options.AdvertisedPublisherHost;
}
```

Выход: M1/M2/M5 работают на усыновлённых кластерах.
Проверка: `dotnet test src/tests/PgWorker.UnitTests` → PASS; `dotnet build src/PgWorker.App` → 0 warnings.
Spec: §3.3.

- [ ] **Step 5.3: Коммит**

Вход: зелёное.
Действие:

```bash
git add src/PgWorker.Provisioning/Endpoints/ShardEndpoints.cs src/PgWorker.Moves src/tests
git commit -m "feat(pgworker): advertised-правило для подписок (только pgw-исполнители) + pg_dump через object-контейнер (adopt-repair T5)"
```

Выход: коммит.
Проверка: `git log --oneline -1`.
Spec: §3.3.

---

### Task 6: Границы надзора для усыновлённых нод

**Files:**
- Modify: `src/PgWorker.Provisioning/Processes/NodeSupervisor.cs` (ctor + `EnsureDeclaredNodesAsync` + `RecreateMarkedNodesAsync` + `SuperviseShardAsync`)
- Modify: `src/PgWorker.Provisioning/Processes/MasterKeyReconciler.cs` (skip-гвард)
- Modify: `src/PgWorker.App/Program.cs` (DI NodeSupervisor + `ISqlExecutor`)
- Test: `src/tests/PgWorker.UnitTests/Provisioning/NodeSupervisorTests.cs` (дополнить), `src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs` (фейк ISqlExecutor при необходимости)

**Interfaces:**
- Consumes: `NodeAddress.Object` (T1), `DatabaseProvisioner.BuildAdminDsn(host, pg, dbname, secrets)` (существующий статик).
- Produces: поведение надзора (без новых публичных контрактов): object-матчинг декларации; SQL-живость при `Ports.Patroni == 0`; rebuild/recreate только для канонических; reconciler пропускает шарды с object-нодами.

- [ ] **Step 6.1: Failing-тесты границ**

Вход: четыре правила spec §3.4.
Действие: в `NodeSupervisorTests.cs` (существующие фейки):

```csharp
[Fact]
public async Task EnsureDeclared_ExternalObjectContainerAlive_NodeNotRecreated()
{
    // Arrange: у s2/s2a адрес с Object="as-s2a"; docker-список содержит as-s2a
    // и НЕ содержит pgw-demo-s2-s2a. Живая нода не должна пересоздаваться.
    // Act: TickAsync.
    // Assert: driver.EnsuredNodes пуст (дубль не создан), состояние нод не PROVISIONING.
}

[Fact]
public async Task Supervise_AdoptedNodeWithoutPatroni_SqlProbeKeepsRunning()
{
    // Arrange: у ноды Ports.Patroni=0; патрони-пробы нет; ISqlExecutor-фейк
    // на admin-DSN возвращает успех.
    // Act / Assert: state остаётся RUNNING (не UNREACHABLE), трек недоступности пуст.
}

[Fact]
public async Task Supervise_DeadAdoptedNode_NoRebuildOnlyUnreachable()
{
    // Arrange: object-нода мертва (SQL тоже падает) дольше NodeDeadSec, кворум жив.
    // Act / Assert: state=UNREACHABLE, driver.RemoveNodeAsync/EnsureNodeAsync НЕ звались.
}

[Fact]
public async Task Recreate_AdoptedNodeMarker_IgnoredWithJournal()
{
    // Arrange: nodes/s2a/recreate=soft (панель), нода — object.
    // Act / Assert: контейнер не пересоздаётся (self-healing off, spec §3.4/R9).
}
```

И для reconciler — тест в файле тестов reconciler'а (если файла нет — `MasterKeyReconcilerTests.cs` новый, фейки по образцу `ShardProbeTests`):

```csharp
[Fact]
public async Task Reconcile_ShardWithObjectNodes_SkippedNoWrite()
{
    // Arrange: у шарда адрес с Object; живой primary (probe 200).
    // Act / Assert: etcd-put мастера НЕ было (внешний писатель не воюет, R8).
}
```

Выход: тесты падают (текущее поведение их нарушает: дубль/UNREACHABLE/rebuild/перезапись).
Проверка: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~NodeSupervisor|FullyQualifiedName~MasterKeyReconciler"` → FAIL.
Spec: §3.4, arch/14 §5 C.

- [ ] **Step 6.2: Реализация границ**

Вход: failing-тесты.
Действие в `NodeSupervisor.cs`:
1. ctor: добавить `ISqlExecutor sql` (после `ShardProbe probe`).
2. `EnsureDeclaredNodesAsync` — матчинг объекта (в цикле нод, ≈стр. 187):

```csharp
// Матчинг декларации (spec §3.4): каноническое имя ИЛИ object-контейнер
// усыновлённой ноды — живой внешний контейнер = «нода на месте», дубль не создаём.
var canonical = $"pgw-{cluster}-{shard.Name}-{node.Name}";
var adoptedObject = topology.Nodes.TryGetValue(node.Name, out var declaredAddr) ? declaredAddr.Object : null;
if (existing.Contains(canonical) || (adoptedObject is { } && existing.Contains(adoptedObject)))
    continue; // объект на месте
```

(заменяет текущую строку `if (existing.Contains($"pgw-{cluster}-{shard.Name}-{node.Name}")) continue;`.)
3. `RecreateMarkedNodesAsync` — в цикле `marked` первым делом:

```csharp
// Self-healing off для усыновлённых (spec §3.4, R9): rebuild поднял бы
// канонический pgw-контейнер рядом с внешним orchestration — только журнал.
if (addresses.TryGetValue($"{shard.Name}/{node.Name}", out var markedAddr) && markedAddr.Object is not null)
{
    await journal.WritePhaseAsync(cluster, "supervise", "recreate-external", claims.InstanceId,
        $"{shard.Name}/{node.Name}: усыновлённая нода — пересоздание вручную (object={markedAddr.Object})", ct);
    continue;
}
```

4. `SuperviseShardAsync` — живость (в цикле проб, ≈стр. 424):

```csharp
// Живость усыновлённой ноды без Patroni-REST (spec §3.4): SQL-проба мастера —
// положительное свидетельство живости PG (сайдкар мёртв ≠ PG мёртв).
bool nodeAlive;
if (addr.Ports.Patroni == 0)
{
    var dsn = DatabaseProvisioner.BuildAdminDsn(addr.Host, addr.Ports.Pg, snap.Config.DbName, secrets);
    var probeResult = await sql.ExecuteScalarAsync(dsn, "SELECT 1", ct);
    nodeAlive = probeResult.IsSuccess;
}
else
    nodeAlive = await probe.IsAliveAsync(addr, ct);
if (nodeAlive) alive.Add(node.Name); else dead.Add(node.Name);
```

5. rebuild-гвард (≈стр. 447): условие `if (!isLeader && quorum && expired)` → добавить `&& addresses.TryGetValue(trackKey, out var deadAddr) && deadAddr.Object is null` (усыновлённые не пересоздаются — только UNREACHABLE ниже по коду, что остаётся).

В `MasterKeyReconciler.ReconcileAsync` — в начале цикла по шардам:

```csharp
// Усыновлённые шарды не сверяем (spec §3.4, arch/14 §5 C/R8): их master-ключ
// пишет внешний HA-контур своим форматом node:port — коррекция порождает
// войну писателей; резолв мастера понимает оба формата (§5 F).
var adopted = shard.Nodes.Any(n =>
    addresses.TryGetValue($"{shard.Name}/{n.Name}", out var a) && a.Object is not null);
if (adopted)
    continue;
```

В `Program.cs` — DI `NodeSupervisor` дополнить `sp.GetRequiredService<ISqlExecutor>()` в ctor-вызов.
Выход: границы надзора.
Проверка: `dotnet test src/tests/PgWorker.UnitTests` → PASS; `dotnet build src/PgWorker.App` → 0 warnings.
Spec: §3.4.

- [ ] **Step 6.3: Коммит**

Вход: зелёное.
Действие:

```bash
git add src/PgWorker.Provisioning src/PgWorker.App src/tests
git commit -m "feat(pgworker): границы надзора усыновлённых нод — object-матчинг, SQL-живость, self-healing off, reconciler skip (adopt-repair T6)"
```

Выход: коммит.
Проверка: `git log --oneline -1`.
Spec: §3.4.

---

### Task 7: MoveRepairProcess (MR0–MR3) + модель фазы/возраста + PutIfAbsent

**Files:**
- Modify: `src/PgWorker.Core/Model/Domain.cs` (`BucketRoute`)
- Modify: `src/PgWorker.Etcd/Parsing/ClusterSnapshotParser.cs` (`TryParseStatus` + `BuildRouting`)
- Modify: `src/PgWorker.Moves/Options.cs` (`MovesRuntimeOptions`)
- Modify: `src/PgWorker.App/Options.cs` (`MovesOptions` + `ToRuntime`)
- Modify: `src/PgWorker.App/appsettings.json` (явные пороги)
- Modify: `src/PgWorker.Moves/Requests/MoveRequestsStore.cs` (`PutIfAbsentAsync`)
- Create: `src/PgWorker.Moves/Process/MoveRepairProcess.cs` (+ классификатор внутри)
- Modify: `src/PgWorker.App/Loops/ClusterProcesses.cs`, `src/PgWorker.App/Loops/ReconcileLoop.cs`, `src/PgWorker.App/Program.cs`
- Test: `src/tests/PgWorker.UnitTests/Moves/MoveRepairClassifierTests.cs` (новый), `src/tests/PgWorker.UnitTests/Moves/MoveRequestsStoreTests.cs` (дополнить)
- Test: `src/tests/PgWorker.IntegrationTests/Etcd/RepairContractTests.cs` (новый)

**Interfaces:**
- Consumes: `MoveRequestsStore.ListAsync` (существующий), `TxnCompare.NotExists`/`TxnOp.Put` (существующие; `NotExists(key)` — примитив put-if-absent «ключа нет ↔ version==0», эталон `ClaimStore.TryPutLeasedKeyAsync`), `MovePhases.RollbackPostFlip` (существующий), `BucketRoute` (расширенный здесь).
- Produces:
  - `public sealed record BucketRoute(int Id, string? Owner, BucketMoveState? Status, string? MoveTarget = null, string? MoveSource = null, string? MovePhase = null, long? MoveUpdatedUnix = null)`;
  - `MovesRuntimeOptions` + `int RepairStaleSec = 600, int RepairFrozenSec = 120`; `MovesOptions` + два свойства; `ToRuntime` прокидывает;
  - `MoveRequestsStore.PutIfAbsentAsync(string cluster, string bucket, MoveRequest request, CancellationToken ct) → Task<Result<bool>>` (false = txn проигран, заявка уже есть);
  - `MoveRepairProcess.TickAsync(ClusterSnapshot snap, CancellationToken ct) → Task<Result<ProcessOutcome>>`; внутренний статический классификатор `internal static MoveRequest? Classify(BucketRoute route, string? routingOwner, long nowUnix, MovesRuntimeOptions o)`;
  - `IClusterProcesses.RepairAsync(snap, ct)`.

- [ ] **Step 7.1: Модель — фаза/возраст в снапшоте (failing → реализация)**

Вход: репаратору нужны phase (rollback-post-flip) и updated_unix (возраст) — сегодня парсер их теряет.
Действие: тест (в `MoveRepairClassifierTests.cs` или рядом с тестами парсера):

```csharp
[Fact]
public void ParseClusters_StatusCarriesPhaseAndUpdatedUnix()
{
    // Arrange: снапшот-парсер: routing + статус SYNCING/copy с updated_unix.
    // Act: ClusterSnapshotParser.ParseClusters(kvs, out _).
    // Assert: route.MovePhase == "copy"; route.MoveUpdatedUnix == 1755850000.
}
```

Реализация: `Domain.cs` — расширить `BucketRoute` (см. Interfaces; дефолты сохраняют все существующие вызовы). `ClusterSnapshotParser`: `TryParseStatus` получает два дополнительных out (`phase`, `updatedUnix`; `root.TryGetProperty("phase"…)`, `root.TryGetProperty("updated_unix") && TryGetInt64`), `BuildRouting` прокидывает в конструктор. XML-комментарий `BucketRoute` дополнить: «MovePhase/MoveUpdatedUnix — из статус-ключа (репарация §3.5); null без статуса».
Выход: снапшот несёт данные репарации.
Проверка: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~ParseClusters"` → PASS; вся сборка зелёная.
Spec: §3.5 (модель).

- [ ] **Step 7.2: Опции порогов**

Вход: пороги spec §2.3: RepairStaleSec=600 (= StaleMoveSeconds панели), RepairFrozenSec=120 (= AbortMinAgeSec).
Действие: `Moves/Options.cs` — два поля в конец `MovesRuntimeOptions`; `App/Options.cs` — в `MovesOptions`:

```csharp
/// <summary>Репарация брошенных статусов: возраст без заявки для
/// SYNCING/ABORTING (600 = StaleMoveSeconds панели, spec §2.3).</summary>
public int RepairStaleSec { get; set; } = 600;

/// <summary>Репарация FROZEN (заморозка режет запись — чиним быстрее;
/// 120 = AbortMinAgeSec, spec §2.3).</summary>
public int RepairFrozenSec { get; set; } = 120;
```

`ToRuntime` — дописать два аргумента. `appsettings.json` — в секцию `Moves` добавить `"RepairStaleSec": 600, "RepairFrozenSec": 120`.
Выход: конфигурируемые пороги.
Проверка: `dotnet build src/PgWorker.App` → 0 warnings; unit-тест опций (если есть тесты ToRuntime — дополнить).
Spec: §2.3, arch/14 §8.

- [ ] **Step 7.3: `PutIfAbsentAsync` (failing → реализация)**

Вход: синтетическая заявка не должна затирать операторскую.
Действие: тест в `MoveRequestsStoreTests.cs` (FakeEtcd из `FakesMove.cs`):

```csharp
[Fact]
public async Task PutIfAbsentAsync_ExistingOperatorRequest_NotReplaced()
{
    // Arrange: операторская заявка move уже стоит.
    // Act: PutIfAbsentAsync синтетической abort-заявки того же бакета.
    // Assert: вернула false; значение ключа — исходное (операторское).
}

[Fact]
public async Task PutIfAbsentAsync_FreeKey_WritesAndTrue()
{
    // Arrange: ключа нет. Act: PutIfAbsentAsync. Assert: true, заявка читается.
}
```

Реализация в `MoveRequestsStore.cs`:

```csharp
/// <summary>Заявка put-if-absent (txn NotExists = version==0, spec §3.5 MR1):
/// синтетическая заявка репарации НЕ затирает операторскую (гонка с
/// оператором безопасна). Эталон txn — ClaimStore.TryPutLeasedKeyAsync.</summary>
public async Task<Result<bool>> PutIfAbsentAsync(
    string cluster, string bucket, MoveRequest request, CancellationToken ct)
{
    var key = MoveNames.MoveKey(cluster, bucket);
    var txn = await WithFailoverAsync(endpoint => gateway.TxnAsync(endpoint, TxnRequest.Of(
        [TxnCompare.NotExists(key)],
        [new TxnOp.Put(key, request.Serialize(), null)]), ct));
    return txn.IsSuccess
        ? Result<bool>.Success(txn.Value.Succeeded)
        : Result<bool>.Failed(txn.Error!);
}
```

(тип возврата `WithFailoverAsync` для Txn — привести по существующему использованию txn в `MoveStatusStore.FlipAsync`.)
Выход: атомарная защита.
Проверка: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~MoveRequestsStore"` → PASS.
Spec: §3.5 MR1.

- [ ] **Step 7.4: Классификатор (failing → реализация)**

Вход: MR1-таблица spec §3.5.
Действие: тесты в `MoveRepairClassifierTests.cs` — полный набор исходов (по строке таблицы + негативы):

```csharp
[Fact]
public void Classify_StaleAborting_AbortNoForce() { /* ABORTING, age 900 → op abort, force false */ }

[Fact]
public void Classify_SyncingOwnerStale_AbortNoForce() { /* SYNCING, routing==owner, age 700 → abort */ }

[Fact]
public void Classify_SyncingRoutingTargetStale_AbortForce() { /* routing==target → force true */ }

[Fact]
public void Classify_FrozenOwnerFrozenSec_AbortNoForce() { /* FROZEN, routing==owner, age 200 (>120) → abort */ }

[Fact]
public void Classify_FrozenRoutingTargetFrozenSec_AbortForce() { /* → force true */ }

[Fact]
public void Classify_RollbackPostFlipPhase_RollbackOp() { /* MovePhase==rollback-post-flip → op rollback */ }

[Fact]
public void Classify_FreshStatus_Null() { /* age < порога → нет действия */ }

[Fact]
public void Classify_NotInitialized_Null() { /* NOT_INITIALIZED — домен P3 */ }
```

Реализация — статический метод внутри `MoveRepairProcess.cs`:

```csharp
/// <summary>MR1-классификация (spec §3.5): брошенный статус → синтетическая
/// заявка; null = не трогаем (свежий/чужой домен). routingOwner — текущий
/// владелец из ROUTING (единственный авторитет «где бакет»).</summary>
internal static MoveRequest? Classify(
    BucketRoute route, string? routingOwner, long nowUnix, MovesRuntimeOptions o)
{
    if (route.Status is not ({ } state and not BucketMoveState.NotInitialized))
        return null;

    var age = nowUnix - (route.MoveUpdatedUnix ?? 0);

    // Фаза доведения отката: заявка rollback — MoveProcess продолжит по фазе.
    if (route.MovePhase == MovePhases.RollbackPostFlip)
        return age <= o.RepairFrozenSec ? null : RepairRequest(route, MoveOp.Rollback, force: false, nowUnix);

    return state switch
    {
        BucketMoveState.Aborting => age > o.RepairStaleSec
            ? RepairRequest(route, MoveOp.Abort, force: false, nowUnix) : null,

        // routing==target: flip прошёл, статус завис — доведение перевода;
        // без force AbortSequence даёт permanent-отказ (цикл), spec §3.5.
        BucketMoveState.Syncing when route.MoveTarget == routingOwner => age > o.RepairStaleSec
            ? RepairRequest(route, MoveOp.Abort, force: true, nowUnix) : null,
        BucketMoveState.Frozen when route.MoveTarget == routingOwner => age > o.RepairFrozenSec
            ? RepairRequest(route, MoveOp.Abort, force: true, nowUnix) : null,

        // routing==owner: откат на владельца — уборка артефактов + re-GRANT
        // (разморозка). Свежесть пройдёт сама: порог ≥ AbortMinAgeSec (Д12).
        BucketMoveState.Syncing => age > o.RepairStaleSec
            ? RepairRequest(route, MoveOp.Abort, force: false, nowUnix) : null,
        BucketMoveState.Frozen => age > o.RepairFrozenSec
            ? RepairRequest(route, MoveOp.Abort, force: false, nowUnix) : null,

        _ => null,
    };

    static MoveRequest RepairRequest(BucketRoute r, MoveOp op, bool force, long now)
        => new($"bucket_{r.Id}", op, To: null, OldShard: null, SkipReverse: false,
               Resume: false, Force: force, RequestedUnix: now, RequestedBy: "pgworker-repair");
}
```

Выход: чистая классификация.
Проверка: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~MoveRepairClassifier"` → PASS.
Spec: §3.5 MR1 (таблица).

- [ ] **Step 7.5: Процесс + интеграция в цикл + DI**

Вход: классификатор, PutIfAbsent, модель готовы.
Действие: `MoveRepairProcess.cs`:

```csharp
using Microsoft.Extensions.Logging;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;

namespace PgWorker.Moves;

/// <summary>
/// Репарация брошенных переездов (spec §3.5, arch/14 §5 K MR0–MR3): статус-ключ
/// без живого владельца (нет заявки, updated_unix постарел) закрывается
/// синтетической заявкой put-if-absent в существующий MoveProcess — механика
/// доведения/журналов/идемпотентности переиспользуется 1:1. Живой владелец
/// (свежий статус или заявка) неприкосновенен (spec §2.4).
/// </summary>
public sealed class MoveRepairProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    ClaimStore claims,
    WorkJournal journal,
    MovesRuntimeOptions options,
    TimeProvider clock,
    ILogger<MoveRepairProcess>? logger = null)
{
    private readonly MoveRequestsStore requests = new(etcd, endpoints);

    public async Task<Result<ProcessOutcome>> TickAsync(ClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Мутации — только под живым клэймом (MR0).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"repair {cluster}: клэйм не наш (или потерян) — мутации запрещены"));
        if (snap.Config.State != ClusterState.Active)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        var stale = snap.Routing.Where(r => r.Status is not null).ToList();
        if (stale.Count == 0)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        var listing = await requests.ListAsync(cluster, ct);
        if (!listing.IsSuccess)
            return Result<ProcessOutcome>.Failed(listing.Error!);
        var claimed = listing.Value.Requests.Select(r => r.Bucket).ToHashSet(StringComparer.Ordinal);

        var now = clock.GetUtcNow().ToUnixTimeSeconds();
        var dispatched = new List<string>();
        foreach (var route in stale)
        {
            var bucket = $"bucket_{route.Id}";
            if (claimed.Contains(bucket))
                continue; // живая заявка — домен MoveProcess (MR1-гвард)

            var repair = Classify(route, route.Owner, now, options);
            if (repair is null)
                continue;

            // put-if-absent: оператор успел раньше — его заявка живёт (spec §3.5).
            var put = await requests.PutIfAbsentAsync(cluster, bucket, repair, ct);
            if (!put.IsSuccess)
                return Result<ProcessOutcome>.Failed(put.Error!);
            if (put.Value)
            {
                dispatched.Add(bucket);
                logger?.LogInformation(
                    "repair {cluster}/{bucket}: синтетическая заявка {op} (force={force}) — доведёт MoveProcess",
                    cluster, bucket, repair.Op, repair.Force);
            }
        }

        if (dispatched.Count > 0)
            await journal.WritePhaseAsync(cluster, "repair", "dispatched", claims.InstanceId,
                $"статусы: {string.Join(", ", dispatched)}", ct);

        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }
}
```

Интеграция: `IClusterProcesses.RepairAsync` + `ClusterProcesses` (ctor `MoveRepairProcess repair`) + `ReconcileLoop` после `rotate-app-password`, до эвакуаций:

```csharp
// Репарация брошенных переездов (spec §3.5, arch/14 §5 K): синтетические
// заявки до moves — этот же тик начнёт их обработку (старейшая заявка).
await RunClusterOpAsync(cluster, "repair",
    () => processes.RepairAsync(snap, ct), ct);
```

DI в `Program.cs` (после MoveProcess, паттерн `opts.Moves.ToRuntime(opts.Thresholds)`):

```csharp
// Репарация брошенных переездов (spec §3.5): синтетические заявки в MoveProcess.
builder.Services.AddSingleton(sp => new MoveRepairProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Moves.ToRuntime(
        sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Thresholds),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ILoggerFactory>().CreateLogger<MoveRepairProcess>()));
```

Выход: Active-ветка полная: supervise → adopt → scale → rotate → **repair** → evacuate → moves.
Проверка: `dotnet build src/PgWorker.App` → 0 warnings; unit-цикла тесты зелёные (моки `IClusterProcesses` дополнить).
Spec: §3.5, §3.2 (порядок).

- [ ] **Step 7.6: Integration-тест контракта репарации**

Вход: паттерн `MoveContractTests` (EtcdFixture).
Действие: `RepairContractTests.cs` — три сценария (AAA):

```csharp
[Fact]
public async Task Repair_StaleStatusesWithoutRequests_SyntheticRequestsAppear()
{
    // Arrange: сид трёх брошенных статусов (updated_unix = now-3600):
    // bucket_3 SYNCING/copy owner=s1; bucket_7 ABORTING/cleanup;
    // bucket_11 FROZEN/cutover-wait owner=s1. Заявок нет.
    // Act: первый тик repair; второй тик (идемпотентный no-op — заявки уже стоят).
    // Assert: /pgworker/moves/demo/{bucket_3,bucket_7} = op abort (force false),
    // bucket_11 = op abort; requested_by=pgworker-repair.
}

[Fact]
public async Task Repair_OperatorRequestPresent_NotReplaced()
{
    // Arrange: статус bucket_3 SYNCING протухший + операторская заявка move.
    // Act / Assert: заявка move не перезаписана (txn проигран), значение ключа неизменно.
}

[Fact]
public async Task Repair_FreshStatus_NoRequest()
{
    // Arrange: bucket_3 SYNCING updated_unix = now-30 (< 600).
    // Act / Assert: заявок не появилось.
}
```

(ClaimStore для теста — реальный с захваченным клэймом, паттерн `MoveContractTests`.)
Выход: контракт закреплён.
Проверка: `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test src/tests/PgWorker.IntegrationTests --filter "FullyQualifiedName~RepairContract"` → PASS.
Spec: §6 integration (repair-контракт).

- [ ] **Step 7.7: Коммит**

Вход: зелёное.
Действие:

```bash
git add src/PgWorker.Core src/PgWorker.Etcd src/PgWorker.Moves src/PgWorker.App src/tests
git commit -m "feat(pgworker): MoveRepairProcess — репарация брошенных статусов синтетическими заявками put-if-absent; пороги RepairStaleSec/RepairFrozenSec (adopt-repair T7)"
```

Выход: коммит.
Проверка: `git log --oneline -1`.
Spec: §3.5, §2.3.

---

### Task 8: Стенд — чистый сид + перепись чека 20

**Files:**
- Modify: `dev-stand/adminpanel/seed.sh` (удалить аномалии, ≈стр. 40–49)
- Modify: `dev-stand/adminpanel/checks/20-alerts.sh` (полная перепись)

**Interfaces:**
- Consumes: критерии spec §3.6 (5 частей сценария + сохранение блока shard-no-master из действующего чека, стр. 32–58), живой PgWorker на общем etcd (00-up.sh шаг 9), бюджет гашения 120 c.
- Produces: стенд-контракт: чистый подъём = согласованный кластер; e2e-доказательство «нахерачил → воркер починил → алерты погасли» + прежний сценарий shard-no-master сохранён.

- [ ] **Step 8.1: Чистка seed.sh**

Вход: сид сегодня сеет заявку bucket_13 (стр. 40–41) и три статуса (стр. 43–49) — вечные аномалии.
Действие: удалить блок заявки и блок статусов (строки 40–49); вместо них — комментарий:

```sh
# Аномалии переездов НЕ сеются (adopt-repair spec §3.6): стенд поднимается
# согласованным; брошенные статусы/заявки нахерачивает чек checks/20-alerts.sh —
# так e2e доказывает гашение алертов реальным ремонтом живого PgWorker.
```

Сид-ключ `/clusters/demo/heals/bucket_5` и HA-DCS/`/cluster/nodes` не трогать. Заголовочный комментарий файла (стр. 2–4: «времена статус-ключей — динамические…») обновить: упоминание аномалий убрать.
Выход: `docker compose run --rm seed` на чистом стенде сеет согласованный контроль-плейн (без status-ключей, без заявок).
Проверка: `sh -n dev-stand/adminpanel/seed.sh` → 0; ручная (опционально, если Docker доступен): поднять etcd+seed, `etcdctl get /clusters/demo/buckets/status/ --prefix --keys-only` → пусто; `/pgworker/moves/demo/` → пусто.
Spec: §3.6 (seed), критерий 6.

- [ ] **Step 8.2: Перепись checks/20-alerts.sh (сохраняя блок shard-no-master)**

Вход: сценарий §3.6 (быстрая проверка без PG — только появление; полный — весь цикл); spec §3.6 требует сохранить существующие shard-no-master Assert/Act 2–5 (действующий чек, стр. 32–58) — блок переносится финальным сценарием ПОСЛЕ move-цикла: после усыновления мастер-ключ s2 пишет внешний HA-контур (эмуляторы), поэтому стоп эмуляторов + удаление ключа по-прежнему корректно проверяет critical-алерт и его гашение. Существующие helpers чека (login/api/has_alert/wait_alert/wait_no_alert, `ect`) сохраняются.
Действие: новая версия файла целиком:

```bash
#!/usr/bin/env bash
# Репарация + усыновление воркером: нахераченные извне ключи → алерты →
# гашение РЕАЛЬНЫМ ремонтом живого PgWorker + полный move на усыновлённом
# кластере + прежний сценарий shard-no-master (adopt-repair spec §3.6).
# Quick (без PgWorker/PG) — только появление алертов.
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT
ect() { docker compose exec -T etcd etcdctl --endpoints=http://localhost:2379 "$@"; }

curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login (панель на $BASE? запусти docker compose up -d adminpanel)"; exit 1; }
api() { curl -fsS -b "$JAR" "$BASE$1"; }

has_alert() { # kind target [severity]
  api /api/alerts | jq -e --arg k "$1" --arg t "$2" --arg s "${3:-}" \
    'any(.[]; .kind==$k and .target==$t and ($s=="" or .severity==$s))' >/dev/null
}
wait_alert() {
  for i in $(seq 1 15); do has_alert "$1" "$2" "${3:-}" && return 0; sleep 1; done
  echo "❌ алерт $1 -> $2${3:+ ($3)} не появился за 15 c"; return 1
}
wait_no_alert() { # kind target [timeout_sec]
  local t="${3:-120}"
  for i in $(seq 1 "$t"); do has_alert "$1" "$2" || return 0; sleep 1; done
  echo "❌ алерт $1 -> $2 не погас за ${t} c"; return 1
}
wait_routing() { # bucket owner [timeout_sec]
  local t="${3:-180}" want
  for i in $(seq 1 "$t"); do
    want="$(ect get "/clusters/demo/buckets/routing/$1" --print-value-only 2>/dev/null || true)"
    [ "$want" = "$2" ] && return 0
    sleep 2
  done
  echo "❌ routing $1 не стал '$2' за ${t} c"; return 1
}

# Act 1: нахерачиваем брошенные статусы извне (протухшие: updated_unix=now-3600,
# пороги репарации RepairStaleSec=600/RepairFrozenSec=120 истекли заранее —
# гашение пойдёт в первые же тики воркера, spec §3.6/§9).
now=$(date +%s); past=$((now - 3600))
ect put /clusters/demo/buckets/status/bucket_3 \
  "{\"bucket\":\"bucket_3\",\"state\":\"SYNCING\",\"owner\":\"s1\",\"target\":\"s2\",\"started_unix\":$past,\"updated_unix\":$past,\"phase\":\"copy\"}" >/dev/null
ect put /clusters/demo/buckets/status/bucket_7 \
  "{\"bucket\":\"bucket_7\",\"state\":\"ABORTING\",\"owner\":\"s2\",\"target\":\"s1\",\"started_unix\":$past,\"updated_unix\":$past,\"phase\":\"cleanup\",\"last_error\":\"receiver went away\"}" >/dev/null
ect put /clusters/demo/buckets/status/bucket_11 \
  "{\"bucket\":\"bucket_11\",\"state\":\"FROZEN\",\"owner\":\"s1\",\"target\":\"s2\",\"started_unix\":$past,\"updated_unix\":$past,\"phase\":\"cutover-wait\"}" >/dev/null
echo "  статусы bucket_3/7/11 нахерачены (протухшие)"

# Assert 2: алерты появились (тик панели 3 c).
wait_alert move-stale demo/bucket_11;   echo "  move-stale -> demo/bucket_11"
wait_alert move-stale demo/bucket_3;    echo "  move-stale -> demo/bucket_3"
wait_alert move-stale demo/bucket_7;    echo "  move-stale -> demo/bucket_7"
wait_alert move-aborting demo/bucket_7; echo "  move-aborting -> demo/bucket_7"
wait_alert move-frozen-long demo/bucket_11 critical; echo "  move-frozen-long -> demo/bucket_11 (critical)"

# Full-ветка (живой PgWorker, 00-up.sh шаг 9): гашение ремонтом + move-цикл
# + сохранённый сценарий shard-no-master.
if ! curl -fsS -m 3 http://localhost:8080/healthz >/dev/null 2>&1; then
  echo "  (quick) PgWorker не поднят — проверка появления алертов пройдена, выход"
  exit 0
fi

# Assert 3: гашение = статус-ключи сняты воркером (репарация), routing не тронут.
wait_no_alert move-stale demo/bucket_11;  echo "  move-stale -> demo/bucket_11 погашен"
wait_no_alert move-aborting demo/bucket_7; echo "  move-aborting -> demo/bucket_7 погашен"
wait_no_alert move-frozen-long demo/bucket_11; echo "  move-frozen-long -> demo/bucket_11 погашен"
[ -z "$(ect get /clusters/demo/buckets/status/bucket_3 --print-value-only 2>/dev/null)" ] \
  || { echo "❌ статус bucket_3 не снят"; exit 1; }
[ "$(ect get /clusters/demo/buckets/routing/bucket_3 --print-value-only)" = "s1" ] \
  || { echo "❌ routing bucket_3 изменился (ожидался владелец s1)"; exit 1; }
echo "  статусы сняты репарацией, routing нетронут"

# Assert 3.5: усыновление — portalloc внешнего кластера восстановлен: ВСЕ 4 ноды
# (s1a/s1b/s2a/s2b) с object-контейнерами as-* (spec §3.6 Assert 5 / §7.1).
alloc="$(ect get /pgworker/portalloc/demo --print-value-only 2>/dev/null || true)"
echo "$alloc" | jq -e 'has("s1/s1a") and has("s1/s1b") and has("s2/s2a") and has("s2/s2b")
  and ([.[].object] | all(. != null and startswith("as-")))' >/dev/null \
  || { echo "❌ portalloc/demo: ожидались все 4 ноды с object: as-* — получено: $alloc"; exit 1; }
echo "  portalloc/demo: все 4 ноды усыновлены (object: as-*)"

# Act/Assert 4: полный move на усыновлённом кластере (bucket_5: s2→s1→s2;
# возврат раскладки — чек 40 ждёт инвентарь 8+5, spec §3.6).
ect put /pgworker/moves/demo/bucket_5 \
  "{\"op\":\"move\",\"to\":\"s1\",\"requested_unix\":$now,\"requested_by\":\"check-20\"}" >/dev/null
wait_routing bucket_5 s1; echo "  bucket_5 переехал s2 → s1 (полный move на усыновлённом)"
wait_no_alert move-stale demo/bucket_5 60
ect put /pgworker/moves/demo/bucket_5 \
  "{\"op\":\"move\",\"to\":\"s2\",\"requested_unix\":$(date +%s),\"requested_by\":\"check-20\"}" >/dev/null
wait_routing bucket_5 s2; echo "  bucket_5 вернулся s1 → s2 (раскладка исходная)"

# Assert 5: ни одного move-* алерта перед финальным сценарием.
api /api/alerts | jq -e 'all(.[]; (.kind | startswith("move-")) | not)' >/dev/null \
  || { echo "❌ остались move-* алерты"; exit 1; }
echo "  move-* алертов нет"

# Act 6 / Assert 7: прежний сценарий shard-no-master (сохранён из действующего
# чека, t10 §7.3): после усыновления мастер-ключ s2 пишет внешний HA-контур
# (эмуляторы) — стоп эмуляторов + удаление ключа по-прежнему корректны.
full=0
if docker compose ps --services --filter status=running 2>/dev/null | grep -qx hc2a; then
  full=1
  echo "  (full) стоп эмуляторов s2: hc2a/hc2b"
  docker compose stop hc2a hc2b >/dev/null
fi
ect del /clusters/demo/shards/s2/master >/dev/null
echo "  master-ключ s2 удалён"
wait_alert shard-no-master demo/s2 critical
echo "  shard-no-master -> demo/s2 (critical)"
if [ "$full" = 1 ]; then
  docker compose start hc2a hc2b >/dev/null
  echo "  (full) эмуляторы s2 запущены — lease восстановится сам (<=3 c)"
else
  ect put /clusters/demo/shards/s2/master 's2a:5432' >/dev/null
  echo "  (quick) ключ возвращён статично"
fi
wait_no_alert shard-no-master demo/s2
echo "  shard-no-master -> demo/s2 погас"

echo "✓ alerts/repair-сценарий зелёный (появление → ремонт → гашение; move туда-обратно; shard-no-master)"
```

Выход: чек реализует полный цикл spec §3.6 (включая сохранённый shard-no-master) с quick/full-ветвлением.
Проверка: `bash -n dev-stand/adminpanel/checks/20-alerts.sh` → 0. Полный e2e-прогон цепочкой spec §7.6 (при живом стенде; порядок важен — 30-й делает failover s1, 40-й рассчитан на итоговую топологию, а 20-й возвращает раскладку):

```bash
cd dev-stand/adminpanel
checks/90-down.sh -v && checks/00-up.sh && checks/10-smoke-api.sh \
  && checks/15-cluster-create.sh && checks/20-alerts.sh \
  && checks/30-failover.sh && checks/40-live-probes.sh
```

(ожидание: вся цепочка зелёная, включая 30/40 — состояние после 20-го исходное; при недоступности стенда отметить в отчёте задачи, прогон не блокирует мерж-гейт кода, но блокирует критерии приёмки 1–3/5/6 по spec §7 — сообщить main-агенту).
Spec: §3.6 (все 5 частей + сохранение shard-no-master), §6 (главный полный прогон: 20 → затем 30/40 зелёные), §7 (критерии 1–3, 5, 6).

- [ ] **Step 8.3: Коммит**

Вход: скрипты синтаксически валидны.
Действие:

```bash
git add dev-stand/adminpanel/seed.sh dev-stand/adminpanel/checks/20-alerts.sh
git commit -m "test(stand): сид без вечных аномалий; чек 20 — нахерачивание → гашение репарацией, полный move на усыновлённом кластере, сохранён shard-no-master (adopt-repair T8)"
```

Выход: коммит.
Проверка: `git log --oneline -1`.
Spec: §3.6.

---

### Task 9: Финальная сборка, полный прогон, соответствие критериям

**Files:**
- Read-only: весь граф изменений T1–T8.

**Interfaces:**
- Consumes: всё из T1–T8.

- [ ] **Step 9.1: Полная сборка и тесты**

Вход: все задачи слиты в ветке.
Действие:

```bash
dotnet build /Users/demakaev/ZCodeProject/worktrees/feat-pgworker-adopt-repair/src/PgWorker.App/PgWorker.App.csproj
dotnet test /Users/demakaev/ZCodeProject/worktrees/feat-pgworker-adopt-repair/src/tests/PgWorker.UnitTests
DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test /Users/demakaev/ZCodeProject/worktrees/feat-pgworker-adopt-repair/src/tests/PgWorker.IntegrationTests
```

Дополнительно (живой Docker-стенд; обязательная часть критериев приёмки 1–3/5/6, если недоступна — зафиксировать в отчёте и передать main-агенту): полный e2e-прогон цепочкой spec §7.6 — `checks/90-down.sh -v && checks/00-up.sh && checks/10-smoke-api.sh && checks/15-cluster-create.sh && checks/20-alerts.sh && checks/30-failover.sh && checks/40-live-probes.sh` (30-й и 40-й — обязательное продолжение 20-го: failover-сценарий и live-пробы на возвращённой раскладке).
Выход: 0 warnings/errors; все тесты зелёные; стенд-цепочка 00→10→15→20→30→40 зелёная (или явная пометка «требует стенда»).
Проверка: вывод команд (expected: `Build succeeded` без warnings; `Passed!` в обоих dotnet-прогонах; каждый чек заканчивается своим «✓ … зелёный»; KafkaWorker/AdminPanel тесты не трогались — прогнать для уверенности: `dotnet test src/tests/AdminPanel.UnitTests` остаётся зелёным, т.к. панель не менялась).
Spec: §7 (критерии 1–3, 5–7), §6 (e2e), Global Constraints.

- [ ] **Step 9.2: Чек-лист критериев приёмки (сверка с spec §7)**

Вход: зелёная сборка.
Действие (прогнать по списку, зафиксировать в отчёте задачи; пункты 1–3/5/6 требуют живого стенда — если Docker-стенд недоступен исполнителю, пометить «требует прогона стенда» и передать main-агенту):
1. portalloc/demo со всеми 4 object-записями после подъёма PgWorker (чек 20 Assert 3.5, jq проверяет 4 ключа).
2. Полный move bucket_5 s2→s1→s2, routing возвращён, **чек 40 зелёный** (прогоняется после 20-го в общей цепочке T9.1).
3. Нахераченные статусы гаснут за ≤120 c, routing нетронут, алерты исчезли.
4. Свежий статус/живая заявка не диспатчатся (unit+integration T7).
5. Стоп патрони-эмуляторов не порождает rebuild-циклов усыновлённых нод — **чек 30 зелёный** (основное условие; финальный блок shard-no-master чека 20 и unit-тесты T6 — дополнительное покрытие).
6. После полного прогона 00→10→15→20→30→40 в панели нет вечных move-* алертов; сид не сеет аномалии (T8).
7. Сборка/тесты зелёные (Step 9.1).
Выход: протокол соответствия.
Проверка: каждый пункт — зелёная галка или явная пометка «требует стенда».
Spec: §7 целиком (§7.2 чек 40, §7.5 чек 30, §7.6 цепочка прогонов).

- [ ] **Step 9.3: Финальный коммит (если остались незакоммиченные правки) и сводка**

Вход: Step 9.1–9.2.
Действие: `git status --short` — при пустоте коммит не нужен; иначе закоммитить остатки `chore(pgworker): adopt-repair — финальные правки прогона`. Сводка исполнителя: список задач, результаты прогонов (включая чеки 30/40 при доступном стенде), незакрытые пункты.
Выход: ветка готова к ревью Фазы 4 (повторное).
Проверка: `git log --oneline` — 8+ коммитов задач; `git status --short` чист.
Spec: §4 фаза 6.

---

## Self-Review (выполнен при написании; обновлён в v4)

1. **Spec coverage:** §3.1→T2 (+T3: journal пропуска при неоднозначности); §3.2→T3; §3.3→T4+T5 (T4 включает unit усыновлённого формата `node:pg-port`); §3.4→T6; §3.5→T7; §3.6→T8 (сохранённый блок shard-no-master; jq-проверка 4 нод); §6 (тесты) распределены по задачам, полный e2e-прогон = цепочка 00→10→15→20→30→40 (Step 8.2 + T9.1); §7→T9.2 (п.5 «чек 30 зелёный» как основное условие, п.6 — цепочка прогонов). Пороги §2.3→T7.2. Порядок ветки §3.2→T3.3/T7.5. Пробелов нет.
2. **Placeholder scan:** развёрнутые тела `PutAsync/TxnPutIfAbsentAsync/RangeAsync` в T3.2 даны ссылкой на failover-паттерн с точным исходником-образцом (`ShardEndpoints.WithFailoverAsync`) — это указание на конкретный существующий код, не TBD. Иных placeholder-паттернов нет.
3. **Type consistency:** `NodeAddress.Object` (T1) → `NodeMatcher/DiscoveredNode.ToAddress()` (T2) → `AdoptionProcess` merge + journal skipped (T3) → `HasAdoptedNodes` (T5) → надзор/reconciler (T6). Драйвер T2 зовёт `NodeMatcher.Match` ОДИН раз на хост со всеми парами — merge сайдкара и skip-on-ambiguity в рантайме эквивалентны юнит-тестам Step 2.1 (зам. 1). `ResolveMasterAsync(cluster, …)` в T4 — полный список вызовов для дерева ПОСЛЕ T3: MoveProcess (4), CutoverSequence (2), **AbortSequence (1, ≈стр. 402)**, BucketEvacuator (1), **AdoptionProcess (1, AD3-блок — файл создан планом в T3 с этим вызовом)** — зам. 2; git add T4.3 стейджит каталог `src/PgWorker.Provisioning/Processes`. Put-if-absent везде — `TxnCompare.NotExists(key)` (эталон `ClaimStore.TryPutLeasedKeyAsync`). `PutIfAbsentAsync → Result<bool>` согласован T7.3/T7.5. `RepairStaleSec/RepairFrozenSec` — MovesOptions→ToRuntime→MovesRuntimeOptions. Чек T8: helpers согласованы со всеми вызовами; полный прогон в Step 8.2/T9.1 включает 30/40 в правильном порядке.

## Примечание об исполнении

Зависимости: T1→T2→T3 строго по порядку; T4 и T6 после T1 (T6 использует `Object`, но не T2/T3 — допустимо параллельно с T3 при конфликте мержа в `Program.cs`/`ReconcileLoop.cs` — правки в разных местах); T5 после T2/T4; T7 независимо от T3–T6 (кроме порядка в ReconcileLoop — правка T7.5 затрагивает тот же файл, что T3.3: исполнять последовательно); T8/T9 последними. Рекомендуемый режим — subagent-driven (по задаче на субагента, ревью между задачами).
