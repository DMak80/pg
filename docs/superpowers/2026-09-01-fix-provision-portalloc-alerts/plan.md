# fix-provision-portalloc-alerts — план исполнения (остаток: Ф2-А автономный reconcile Д1–Д3 + финал)

> **Для агентов-исполнителей:** ОБЯЗАТЕЛЬНЫЙ SUB-SKILL: superpowers:subagent-driven-development (рекомендуется) или superpowers:executing-plans — исполнять задачу-за-задачей. Шаги отмечаются чекбоксами (`- [ ]`).

**Цель:** добить расширение «воркер — хозяин» (spec §3.7 Д1/Д1б/Д2/Д3): перепланирование занятых чужим портов в provision, probe-идентификация своей ноды, инвариант адресов Active в AdoptionProcess, лечение HA-scope при доказанной утрате данных; затем финальная верификация ветки.

**Архитектура:** занятость «чужим» = docker-факт минус свои контейнеры ∪ portalloc соседей; чистая функция `PortPlanConvergence.DetachColliding` снимает коллизионные закрепления и переиспользуется provision (Д1) и adopt (Д2); Patroni-проба идентифицирует ноду парой (scope, name) из GET `/patroni` (Д1б); бюджет-ветка WaitPatroni превращается в трёхуровневую пробу данных (Present/Absent/Unknown) с чисткой HA-scope только при доказанной утрате (Д3).

**Стек:** .NET 10, C# latest, `Nullable=enable`, `TreatWarningsAsErrors=true`, xUnit v3 + FluentAssertions, centralized packages (`Directory.Packages.props`).

**Spec:** `docs/superpowers/2026-09-01-fix-provision-portalloc-alerts/spec.md` (план аргументируется от spec'а; исполнитель читает оба). Контракты arch/ уже обновлены в этом ветвлении (arch/14 §2.4/§3.3/§5 A P1/P2.1/P2.2/§5 J AD2'/§8/R11/R12, arch/adminpanel/02-03, arch/09 §11, roadmap t90) — код ниже реализует их, новых контрактов не пишем.

## Глобальные ограничения

- Рабочая директория ВСЕХ команд: `/Users/demakaev/ZCodeProject/worktrees/fix-provision-portalloc-alerts` (worktree-ветка `fix-provision-portalloc-alerts`).
- Сборка: `dotnet build src/PgWorker.slnx` — обязана проходить БЕЗ warnings (`TreatWarningsAsErrors=true`).
- Тесты: `dotnet test src/tests/PgWorker.UnitTests` и `dotnet test src/tests/AdminPanel.UnitTests`; интеграционные — с `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1`. Каждый тест — с AAA-комментариями (`// Arrange`, `// Act`, `// Assert`).
- Документация/комментарии — русский; идентификаторы — английские.
- Стенд НЕ трогать: никаких docker/etcd-мутаций из плана; e2e/деплой — вне плана (живая верификация — read-only документ `stand-verification.md`, дополняется Task 5).
- Коммит после каждой задачи: конвейер `feat(pgworker): …` / `fix(review): …` / `docs(stand): …` с русским описанием (стиль `git log`).
- Тест-инфраструктура готова (новую не создавать): PgWorker — `Fakes.FakeEtcd/FakeDriver/FakeSql`, `Rig/NewRig` в `ProvisioningProcessTests`, `FakeEngine`/`NewPlainDriver` в `ClusterDriverTests`, `RecordingJournal`/`SnapshotActive`/`NewAdoption` в `AdoptionProcessTests`; AdminPanel — без изменений в этом остатке.
- Границы spec §5, действующие на остаток: `AddShardProcess` не получает Д1б-identity и Д3-лечения (только сброс трекера уже сделан); `NodeSupervisor` не меняется; object-записи нигде не перезаписываются; `request_*`-ключи Patroni не трогаем; swarm получает `NodeDataPresenceAsync` через свой `ExecNodeAsync`, `InspectNodesAsync` swarm остаётся заглушкой.

---

## Часть A — Реализовано (коммиты fdd6db9…2182372; НЕ перепланировать)

Базовая часть задачи (spec §3.1–§3.6, фазы Ф1/Ф2/Ф3/Ф4/Ф5) уже в ветке и зелёная. Соответствие:

| Spec | Что | Коммит | Где в коде |
|---|---|---|---|
| §3.1 (A) | Усыновление фактических портов в `PlanPortsAsync` (инспекция каждый тик, guard'ы канон/порты/object-less, put/txn-коммит) | a9cd178 | `ProvisioningProcess.PlanPortsAsync`/`AdoptRunningContainersAsync` + 5 тестов `Tick_*Portalloc*` в `ProvisioningProcessTests` |
| — | Фильтр ЧУЖИХ pgw-кластеров в `InspectNodesAsync` (живая верификация Ф7) | 2182372 | `PlainClusterDriver.InspectNodesAsync`/`IsForeignPgw` + 3 теста `InspectNodes_*` |
| §3.2 (B) | Сверка public-биндингов в `EnsureNodeAsync` (расхождение → stop+rm+create; object не трогаем; swarm-комментарий) | 58a2495 | `PlainClusterDriver.EnsureNodeAsync`/`PortsMatchPlan` + тесты `EnsureNode_*` |
| §3.3 (C) | `PortAllocIndex` (busy из etcd, битые соседи skip+лог) | c632aa6 | `PgWorker.Provisioning/Endpoints/PortAllocIndex.cs` + `PortAllocIndexTests` |
| §3.3 (C) | busy-union в `PlanShardPortsAsync` (AddShard) | 17a8b14 | `AddShardProcess.PlanShardPortsAsync` |
| §3.5 (E1) | Поля серии `fail_count`/`fail_first_unix`/`retry_not_before_unix` в WorkState + round-trip | fdd6db9 | `WorkJournal.cs` + CoordinationTests + EtcdContractTests |
| §3.5 (E2/E4) | Бэкофф: skip тика до `retry_not_before`, счётчик Base·2^n кап 60, перенос фазами, опции | 4ca780f | `ProvisioningProcess.TickAsync`/`FailAsync`, `PlacementOptions`, appsettings |
| §3.5 (E3) | Сброс `_patroniWaitSince` при бюджет-фейле (оба процесса) | 9bf0fab | `ProvisioningProcess.WaitPatroniAsync`, `AddShardProcess.WaitPatroniAsync` |
| §3.4 (D1) | Снапшот панели читает `/pgworker/work/` (WorkJournalParser → PgWorkerWork; ParseError; FailTick) | 54aac24 | `AdminPanel.Etcd/Parsing/WorkJournalParser.cs` и др. + интеграционный |
| §3.4 (D2/D3) | Эскалация `cluster-not-initialized` (900 c) + правило `provision-stuck` | 25cad21 | `ClusterNotInitializedRule`, `ProvisionStuckRule`, `AlertsOptions` |
| §3.4 (D4) | `WorkerHealthStore`/`WorkerHealthPoller` + правило `worker-unhealthy` | 69c320b | `AdminPanel.Etcd/Workers/*`, `WorkerUnhealthyRule` |
| §3.6 (F) | Чек 15 чистит `/pgworker/portalloc/smoke`; инструкция верификации стенда | 18d7eb3 | `dev-stand/adminpanel/checks/15-cluster-create.sh`, `stand-verification.md` |
| Ф1 | Контракты arch/ (§2.4/§5 A/§3.3/§8, adminpanel/02-03, arch/09 §11, roadmap t90) | 579dd5f (+ рабочий diff §3.7) | `arch/14-pgworker.md` и др. — уже включают Д1–Д3 |

### Task A0: сверка «реализовано» прогонами (входной гейт остатка)

**Files:** без изменений кода — только прогоны.

**Interfaces:**
- Consumes: ветка как есть.
- Produces: подтверждение зелёной базы перед Д1–Д3 (базовые числа тестов для контроля прироста).

- [ ] **Шаг 1: сборка**

Run: `dotnet build src/PgWorker.slnx`
Expected: Build succeeded, 0 warnings.

- [ ] **Шаг 2: юнит-прогоны**

Run: `dotnet test src/tests/PgWorker.UnitTests && dotnet test src/tests/AdminPanel.UnitTests`
Expected: PASS (у воркера — 496 тестов на момент составления плана; у панели — 375). Зафиксировать фактические числа — Task 5 сверит прирост.

- [ ] **Шаг 3: интеграционные прогоны**

Run: `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test src/tests/PgWorker.IntegrationTests && DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test src/tests/AdminPanel.IntegrationTests`
Expected: PASS (4 + 108 тестов; Testcontainers поднимет etcd).

Коммита нет (нет изменений). Если что-то красное — стоп, разбор с координатором (не чинить молча).

---

## Часть B — остаток: автономный reconcile (Ф2-А) + финал

### Task 1: Д1 — перепланирование занятых чужим портов в `PlanPortsAsync` (spec §3.7 Д1, arch/14 §5 A P1)

**Files:**
- Create: `src/PgWorker.Core/Planning/PortPlanConvergence.cs`
- Modify: `src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs` (record `Adoption` += `SelfFact`; `AdoptRunningContainersAsync` собирает selfFact; `PlanPortsAsync` — detach-шаг и перенос чтений busy до проверки полноты)
- Test: Create: `src/tests/PgWorker.UnitTests/Planning/PortPlanConvergenceTests.cs`
- Test: Modify: `src/tests/PgWorker.UnitTests/Provisioning/ProvisioningProcessTests.cs` (+2 теста)

**Interfaces (Вход/Выход):**
- Consumes: `NodeAddress`/`NodePorts` (record, value-Equals); `PortAllocIndex.ReadBusyAsync(cluster, ct)`; `driver.GetBusyPortsAsync`; существующий `Adoption(IReadOnlyList<string> Skipped, bool Changed)`; `CommitPortAllocAsync`.
- Produces: `public static class PortPlanConvergence { public static bool DetachColliding(Dictionary<string, NodeAddress> existing, IReadOnlySet<(string Host, int Port)> selfFact, IReadOnlySet<(string Host, int Port)> foreign); }` в `PgWorker.Core.Planning` — снимает записи без `Object`, НЕ подтверждённые selfFact (хотя бы один порт записи не в selfFact) и имеющие ЛЮБОЙ порт в foreign; возврат — были ли изменения. `Adoption` расширяется до `Adoption(IReadOnlyList<string> Skipped, bool Changed, IReadOnlySet<(string Host, int Port)> SelfFact)`.

- [ ] **Шаг 1: падающие тесты чистой функции — `PortPlanConvergenceTests.cs` (новый файл)**

```csharp
using PgWorker.Core.Model;
using PgWorker.Core.Planning;

namespace PgWorker.UnitTests.Planning;

// PortPlanConvergence (spec §3.7 Д1, arch/14 §5 A P1): закрепление, не
// подтверждённое фактом своего живого контейнера и занятое чужим
// (docker-биндинг соседа минус свои ∪ portalloc соседей), снимается —
// PortAllocator выделит ноде свободные порты, EnsureNode создаст контейнер
// в том же тике. object-записи (усыновлённые) не трогаются (R9).

public class PortPlanConvergenceTests
{
    private static NodeAddress Addr(string host, int pg) => new(host, new NodePorts(pg, pg + 3000, pg + 1500));

    [Fact]
    public void DetachColliding_ForeignPgPort_RemovesRecord()
    {
        // Arrange: запись без контейнера; её pg-порт занят чужим docker-фактом.
        var existing = new Dictionary<string, NodeAddress> { ["s1/n1"] = Addr("h1", 15000) };
        var foreign = new HashSet<(string, int)> { ("h1", 15000) };

        // Act
        var changed = PortPlanConvergence.DetachColliding(existing, [], foreign);

        // Assert: коллизионное закрепление снято (недобор → аллокация заново).
        changed.Should().BeTrue();
        existing.Should().NotContainKey("s1/n1");
    }

    [Fact]
    public void DetachColliding_SelfFactRecord_Survives()
    {
        // Arrange: запись подтверждена фактом своего живого контейнера — её порты
        // есть в docker-busy (живая публикация), но это НЕ чужая занятость
        // (spec §8.10: без вычитания selfFact перепланирование сносило бы
        // здоровые закрепления).
        var existing = new Dictionary<string, NodeAddress> { ["s1/n1"] = Addr("h1", 15000) };
        var selfFact = new HashSet<(string, int)> { ("h1", 15000), ("h1", 18000), ("h1", 16500) };
        var foreign = new HashSet<(string, int)> { ("h1", 15000) };

        // Act
        var changed = PortPlanConvergence.DetachColliding(existing, selfFact, foreign);

        // Assert: своя живая нода не перепланируется.
        changed.Should().BeFalse();
        existing.Should().ContainKey("s1/n1");
    }

    [Fact]
    public void DetachColliding_ObjectRecord_Untouched()
    {
        // Arrange: object-запись (усыновлённая) с портом в foreign.
        var existing = new Dictionary<string, NodeAddress>
        {
            ["s1/n1"] = new("h1", new NodePorts(15000, 18000, 16500), Object: "external-1"),
        };

        // Act
        var changed = PortPlanConvergence.DetachColliding(existing, [], new HashSet<(string, int)> { ("h1", 15000) });

        // Assert: чужие контейнеры не трогаем (R9).
        changed.Should().BeFalse();
        existing.Should().ContainKey("s1/n1");
    }

    [Fact]
    public void DetachColliding_PatroniPortCollision_RemovesRecord()
    {
        // Arrange: занят PATRONI-порт (18000) — коллизия по любому из трёх портов.
        var existing = new Dictionary<string, NodeAddress> { ["s1/n1"] = Addr("h1", 15000) };

        // Act
        var changed = PortPlanConvergence.DetachColliding(existing, [], new HashSet<(string, int)> { ("h1", 18000) });

        // Assert
        changed.Should().BeTrue();
        existing.Should().BeEmpty();
    }

    [Fact]
    public void DetachColliding_NoCollisions_NoChanges()
    {
        // Arrange: все записи чисты; занятость host-специфична (чужой хост не мешает).
        var existing = new Dictionary<string, NodeAddress> { ["s1/n1"] = Addr("h1", 15000) };

        // Act
        var changed = PortPlanConvergence.DetachColliding(existing, [], new HashSet<(string, int)> { ("h2", 15000) });

        // Assert
        changed.Should().BeFalse();
        existing.Should().ContainKey("s1/n1");
    }
}
```

- [ ] **Шаг 2: прогон — падают**

Run: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~PortPlanConvergenceTests"`
Expected: FAIL — тип `PortPlanConvergence` не существует (ошибка компиляции).

- [ ] **Шаг 3: реализация `PortPlanConvergence.cs`**

```csharp
using PgWorker.Core.Model;

namespace PgWorker.Core.Planning;

/// <summary>
/// Сходимость плана портов с фактом занятости (spec §3.7 Д1, arch/14 §5 A P1):
/// закрепление, не подтверждённое фактом своего живого контейнера и занятое
/// чужим (docker-биндинг соседа минус свои ∪ portalloc-записи соседей),
/// снимается — PortAllocator выделит ноде свободные порты, EnsureNode создаст
/// контейнер в том же тике. object-записи (усыновлённые) не трогаются (R9).
/// Переиспользуется provision (P1) и adopt (AD2').
/// </summary>
public static class PortPlanConvergence
{
    public static bool DetachColliding(
        Dictionary<string, NodeAddress> existing,
        IReadOnlySet<(string Host, int Port)> selfFact,
        IReadOnlySet<(string Host, int Port)> foreign)
    {
        var colliding = new List<string>();
        foreach (var (key, addr) in existing)
        {
            if (addr.Object is not null)
                continue; // усыновлённая (object) — чужой контейнер, не трогаем (R9)
            var ports = new[] { addr.Ports.Pg, addr.Ports.Patroni, addr.Ports.Doorman };
            if (ports.All(p => selfFact.Contains((addr.Host, p))))
                continue; // подтверждено фактом своего живого контейнера (spec §8.10)
            if (ports.Any(p => foreign.Contains((addr.Host, p))))
                colliding.Add(key);
        }

        foreach (var key in colliding)
            existing.Remove(key);
        return colliding.Count > 0;
    }
}
```

- [ ] **Шаг 4: прогон — зелёные**

Run: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~PortPlanConvergenceTests"`
Expected: PASS (5 тестов).

- [ ] **Шаг 5: падающие процессные тесты (дописать в `ProvisioningProcessTests`)**

```csharp
// AAA: Д1 — порт плана занят чужим docker-фактом: нода перепланирована и
// контейнер создаётся В ТОТ ЖЕ тик; остальные записи не тронуты
[Fact]
public async Task Tick_ForeignDockerBusyPort_ReplansNodeInSameTick()
{
    // Arrange: ПОЛНЫЙ portalloc, контейнеров нет (InspectResult пуст — наследие
    // инцидента); docker-факт: порт (h1, 15000) — нода shard1a — занял чужой.
    var rig = await NewRig(_ => DeadPatroni());
    rig.Etcd.Seed("/pgworker/portalloc/shop",
        """
        {"shard1/shard1a":{"host":"h1","pg":15000,"patroni":18000,"doorman":16500},
        "shard1/shard1b":{"host":"h2","pg":15001,"patroni":18001,"doorman":16501},
        "shard2/shard2a":{"host":"h1","pg":15002,"patroni":18002,"doorman":16502},
        "shard2/shard2b":{"host":"h2","pg":15003,"patroni":18003,"doorman":16503}}
        """);
    rig.Driver.BusyPorts = new HashSet<(string, int)> { ("h1", 15000) };

    // Act
    var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

    // Assert: ТОЛЬКО shard1a перепланирована (свободная тройка h1 = 15001/18001/16501),
    // остальные записи переиспользованы; EnsureNode выполнен в том же тике
    // (контейнер создаётся сразу — Д1 «сам починил» без оператора).
    outcome.IsSuccess.Should().BeTrue();
    var raw = rig.Etcd.Store["/pgworker/portalloc/shop"].Value;
    raw.Should().Contain("\"shard1/shard1a\":{\"host\":\"h1\",\"pg\":15001");
    raw.Should().Contain("\"shard1/shard1b\":{\"host\":\"h2\",\"pg\":15001");
    raw.Should().Contain("\"shard2/shard2a\":{\"host\":\"h1\",\"pg\":15002");
    rig.Driver.EnsuredNodes.Should().Contain("shard1/shard1a");
    // Ключ существовал → перезапись put-ом (не txn), version вырос.
    rig.Etcd.Store["/pgworker/portalloc/shop"].Version.Should().Be(2);
}

// AAA: Д1 — docker-busy СВОИХ живых контейнеров не «занимает» их же закрепления:
// записи подтверждены фактом (adoption-находки) → ничего не перепланируется
[Fact]
public async Task Tick_SelfContainerDockerBusy_NotReplanned()
{
    // Arrange: полный portalloc == факт контейнеров (все 4 ноды с явными doorman!);
    // docker-busy содержит ИХ ЖЕ порты (живые публикации своих контейнеров).
    var rig = await NewRig(_ => DeadPatroni());
    rig.Etcd.Seed("/pgworker/portalloc/shop",
        """
        {"shard1/shard1a":{"host":"h1","pg":15000,"patroni":18000,"doorman":16500},
        "shard1/shard1b":{"host":"h2","pg":15001,"patroni":18001,"doorman":16501},
        "shard2/shard2a":{"host":"h1","pg":15002,"patroni":18002,"doorman":16502},
        "shard2/shard2b":{"host":"h2","pg":15003,"patroni":18003,"doorman":16503}}
        """);
    rig.Driver.InspectResult = new Dictionary<string, DiscoveredNode>
    {
        ["shard1a"] = Node("h1", "pgw-shop-shard1-shard1a", 15000, 18000, 16500),
        ["shard1b"] = Node("h2", "pgw-shop-shard1-shard1b", 15001, 18001, 16501),
        ["shard2a"] = Node("h1", "pgw-shop-shard2-shard2a", 15002, 18002, 16502),
        ["shard2b"] = Node("h2", "pgw-shop-shard2-shard2b", 15003, 18003, 16503),
    };
    rig.Driver.BusyPorts = new HashSet<(string, int)> { ("h1", 15000), ("h1", 18000), ("h2", 15001) };

    // Act
    var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

    // Assert: свои живые публикации — НЕ чужая занятость: записи не тронуты
    // (version ключа не выросла — ранний выход без записи), перепланирования нет.
    outcome.IsSuccess.Should().BeTrue();
    rig.Etcd.Store["/pgworker/portalloc/shop"].Version.Should().Be(1);
}
```

Внимание: в `Tick_SelfContainerDockerBusy_NotReplanned` находки `Node(...)` обязаны передавать doorman ПЯТЫМ аргументом (совпадение записи и факта — по value-Equals всех полей; дефолт 16500 разошёлся бы с сидом 16501+ и дал ложный Changed).

- [ ] **Шаг 6: прогон — падают**

Run: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~ProvisioningProcessTests"`
Expected: FAIL — коллизионная запись не снимается (`pg":15000` остаётся / version не растёт).

- [ ] **Шаг 7: реализация в `ProvisioningProcess.cs`**

7а. Record `Adoption` и сбор selfFact в `AdoptRunningContainersAsync` (после guard'ов, при валидной находке; return-строка дополнена):

```csharp
// Результат усыновления: имена пропущенных находок (journal-заметка), признак
// «merge изменил existing» (ранний выход без записи) и порты ФАКТА своих
// контейнеров (Д1: docker-занятость минус selfFact = чужая).
private sealed record Adoption(
    IReadOnlyList<string> Skipped, bool Changed, IReadOnlySet<(string Host, int Port)> SelfFact);

// внутри метода, до цикла:
var selfFact = new HashSet<(string, int)>();
// в цикле, сразу после guard'а канонического имени/портов (перед работой с existing):
foreach (var p in new[] { node.Pg, node.Patroni, node.Doorman })
    if (p > 0)
        selfFact.Add((node.Host, p));
// в конце:
return Result<Adoption>.Success(new Adoption(skipped, changed, selfFact));
```

7б. `PlanPortsAsync` — после блока adoption вставить detach-шаг, чтения busy поднять ДО раннего выхода, недобор аллокирует по `foreign` (полный новый порядок середины метода — заменить блок от `var skipped = …` до конца недобора):

```csharp
var skipped = adopted.Value.Skipped;

// Д1 (spec §3.7): занятость ЧУЖИМ = docker-факт минус свои контейнеры ∪ portalloc
// соседей. Читается до проверки полноты: полный portalloc может нести коллизию
// (наследие инцидента canon10/smoke) — «закреплено и переиспользуется» не должно
// давать вечный фейл-цикл «port is already allocated».
var dockerBusy = await driver.GetBusyPortsAsync(ct);
if (!dockerBusy.IsSuccess)
    return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(dockerBusy.Error!);
var foreignAlloc = await portAlloc.ReadBusyAsync(cluster, ct);
if (!foreignAlloc.IsSuccess)
    return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(foreignAlloc.Error!);
var foreign = new HashSet<(string, int)>(foreignAlloc.Value);
foreach (var p in dockerBusy.Value)
    if (!adopted.Value.SelfFact.Contains(p))
        foreign.Add(p);
var detached = PortPlanConvergence.DetachColliding(existing, adopted.Value.SelfFact, foreign);

// Ранний выход (идемпотентность, spec §3.1 шаг 4 + §3.7 Д1): всё закреплено,
// merge и detach ничего не изменили — записи portalloc нет.
if (wanted.All(existing.ContainsKey) && !adopted.Value.Changed && !detached)
    return await PlannedAsync(existing, cluster, ct, series, skipped);

if (wanted.All(existing.ContainsKey))
{
    var commit = await CommitPortAllocAsync(cluster, existing, pinned.Value.Count > 0, ct);
    if (!commit.IsSuccess)
        return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(commit.Error!);
    return await PlannedAsync(existing, cluster, ct, series, skipped);
}

var hosts = await driver.GetHostsAsync(ct);
if (!hosts.IsSuccess)
    return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(hosts.Error!);
// Занятость для аллокации — foreign (docker-чужие ∪ соседи): docker-публикации
// СВОИХ живых контейнеров — это закрепления (existing), а не запреты; попади
// selfFact в busy, allocator не переиспользовал бы валидные записи → EnsureNode
// пересоздавал бы живые контейнеры (spec §8.10 — вычитание selfFact).
var plan = PlacementPlanner.Plan(snap.Shards, hosts.Value);
var allocated = PortAllocator.Allocate(plan, existing, foreign, placementOpts.PortFrom, placementOpts.PortTo);
if (!allocated.IsSuccess)
    return allocated;

foreach (var (merged, addr) in allocated.Value)
    existing[merged] = addr;

var commitAll = await CommitPortAllocAsync(cluster, existing, pinned.Value.Count > 0, ct);
if (!commitAll.IsSuccess)
    return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(commitAll.Error!);

return await PlannedAsync(existing, cluster, ct, series, skipped);
```

(прежний блок чтения `dockerBusy`/`foreignBusy` из недобора удаляется — значения уже прочитаны выше; `busy`-переменная недобора = `foreign`.)

- [ ] **Шаг 8: прогон — зелёные**

Run: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~ProvisioningProcessTests"`
Expected: PASS (7 новых + все существующие: ранний выход при чистом portalloc сохранён, `Tick_FreshCluster_AvoidsForeignPortallocRecords` зелёный — соседская тройка попадает в foreign через portalloc-индекс).

- [ ] **Шаг 9: коммит**

```bash
git add src/PgWorker.Core/Planning/PortPlanConvergence.cs src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs src/tests/PgWorker.UnitTests/Planning/PortPlanConvergenceTests.cs src/tests/PgWorker.UnitTests/Provisioning/ProvisioningProcessTests.cs
git commit -m "feat(pgworker): Д1 — перепланирование занятых портов: закрепление, не подтверждённое фактом своего контейнера и занятое чужим (docker минус свои ∪ portalloc соседей), снимается — контейнер создаётся в том же тике; вечный фейл-цикл «port is already allocated» самолечится ≤2 тиками (spec §3.7 Д1, arch/14 §5 A P1)"
```

---

### Task 2: Д1б — probe-идентификация своей ноды (spec §3.7 Д1б, arch/14 §5 A P2.2)

**Files:**
- Modify: `src/PgWorker.Provisioning/Probes/ShardProbe.cs` (+`NodeIdentity`, +`IdentifyAsync`)
- Modify: `src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs` (`WaitPatroniAsync` — identity вместо `IsAliveAsync`)
- Test: Modify: `src/tests/PgWorker.UnitTests/Provisioning/ShardProbeTests.cs` (+2 теста)
- Test: Modify: `src/tests/PgWorker.UnitTests/Provisioning/ProvisioningProcessTests.cs` (харнесс `Probe` переключается на путь `/patroni`; +1 новый тест)

**Interfaces (Вход/Выход):**
- Consumes: `HttpClient` пробы (таймаут `ProbeTimeout` 3 c уже есть); JSON GET `/patroni` (поля `name`, `scope`).
- Produces: `public sealed record NodeIdentity(string Name, string Scope);` и `Task<Result<NodeIdentity?>> IdentifyAsync(NodeAddress node, CancellationToken ct)` в `ShardProbe` — сетевой сбой/не-2xx/битый JSON/отсутствующие поля → `Success(null)` («не опознана», НЕ Failed). Поведение `WaitPatroniAsync`: нода жива ⟺ `identity.Scope == scope && identity.Name == node`.

Граница (spec §5): `AddShardProcess.WaitPatroniAsync` и `NodeSupervisor` остаются на `IsAliveAsync` — их тесты и код НЕ трогаем.

- [ ] **Шаг 1: падающие тесты пробы (дописать в `ShardProbeTests`; `Node`-фикстура и `FakeHandler`/`Json`-хелперы уже есть в файле)**

```csharp
[Fact]
public async Task IdentifyAsync_PatroniJson_ParsesNameAndScope()
{
    // Arrange: /patroni отвечает scope+name (Patroni 3.x REST).
    var probe = new ShardProbe(new HttpClient(new FakeHandler(_ => Json(200,
        """{"state":"running","role":"replica","scope":"shop-shard1","name":"shard1a"}"""))));

    // Act
    var identity = await probe.IdentifyAsync(Node, CancellationToken.None);

    // Assert: пара (name, scope) — глобально уникальна (scope <C>-<X>).
    identity.IsSuccess.Should().BeTrue();
    identity.Value.Should().Be(new NodeIdentity("shard1a", "shop-shard1"));
}

[Fact]
public async Task IdentifyAsync_BrokenOrForeignOrMissing_Null()
{
    // Arrange: битый JSON, не-2xx и JSON без полей — «не опознана» (не ошибка).
    var broken = new ShardProbe(new HttpClient(new FakeHandler(_ => Json(200, "not-json"))));
    var notFound = new ShardProbe(new HttpClient(new FakeHandler(_ => Json(404, ""))));
    var noFields = new ShardProbe(new HttpClient(new FakeHandler(_ => Json(200, """{"members":[]}"""))));

    // Act
    var a = await broken.IdentifyAsync(Node, CancellationToken.None);
    var b = await notFound.IdentifyAsync(Node, CancellationToken.None);
    var c = await noFields.IdentifyAsync(Node, CancellationToken.None);

    // Assert: null — чужой ответ по коллизионному порту ≠ наша нода (фальш-RUNNING исключён).
    a.Value.Should().BeNull();
    b.Value.Should().BeNull();
    c.Value.Should().BeNull();
}
```

- [ ] **Шаг 2: прогон — падают**

Run: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~ShardProbeTests"`
Expected: FAIL — `NodeIdentity`/`IdentifyAsync` не существуют.

- [ ] **Шаг 3: реализация в `ShardProbe.cs`**

```csharp
/// <summary>Идентичность Patroni-ноды из GET /patroni (spec §3.7 Д1б): scope
/// глобально уникален (&lt;C&gt;-&lt;X&gt;), name — имя ноды; пары достаточно для вывода
/// «наша/чужая» (у /cluster поля scope нет — имена нод шаблонные между кластерами).</summary>
public sealed record NodeIdentity(string Name, string Scope);

// Идентификация ноды: GET /patroni несёт scope+name; транспорт/битый JSON/
// не-2xx/отсутствующие поля → Success(null) — «не опознана» (чужой ответ по
// коллизионному порту не является успехом ожидания — Д1б).
public async Task<Result<NodeIdentity?>> IdentifyAsync(NodeAddress node, CancellationToken ct)
{
    try
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(ProbeTimeout);
        using var response = await http.GetAsync(BuildUri(node, "patroni"), timeout.Token);
        if (!response.IsSuccessStatusCode)
            return Result<NodeIdentity?>.Success(null);

        await using var stream = await response.Content.ReadAsStreamAsync(timeout.Token);
        using var doc = await JsonDocument.ParseAsync(stream, cancellationToken: timeout.Token);
        var name = doc.RootElement.TryGetProperty("name", out var n) ? n.GetString() : null;
        var scope = doc.RootElement.TryGetProperty("scope", out var s) ? s.GetString() : null;
        return Result<NodeIdentity?>.Success(
            name is { Length: > 0 } && scope is { Length: > 0 } ? new NodeIdentity(name, scope) : null);
    }
    catch (Exception e) when (e is HttpRequestException or TaskCanceledException or JsonException)
    {
        return Result<NodeIdentity?>.Success(null);
    }
}
```

- [ ] **Шаг 4: прогон — зелёные**

Run: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~ShardProbeTests"`
Expected: PASS (2 новых + существующие).

- [ ] **Шаг 5: правка харнесса `Probe` в `ProvisioningProcessTests` (респондер начинает различать путь; без этого ВСЕ живые Patroni-тесты сломаются — members-JSON не парсится как identity)**

Заменить метод `Probe` и добавить дефолтную карту identity (раскладка портов стандартного сида: shard1 → 15000/18000 на h1+h2, shard2 → 15001/18001):

```csharp
// Дефолтная карта идентичностей стандартного сида (Д1б): patroni-порты нод
// после аллокации placement (shard1a h1:18000, shard1b h2:18000,
// shard2a h1:18001, shard2b h2:18001).
private static readonly IReadOnlyDictionary<(string Host, int Port), (string Scope, string Name)>
    DefaultIdentity = new Dictionary<(string, int), (string, string)>
    {
        [("h1", 18000)] = ("shop-shard1", "shard1a"),
        [("h2", 18000)] = ("shop-shard1", "shard1b"),
        [("h1", 18001)] = ("shop-shard2", "shard2a"),
        [("h2", 18001)] = ("shop-shard2", "shard2b"),
    };

private static ShardProbe Probe(
    Func<int, HttpResponseMessage> respondByPort,
    List<int>? trace = null,
    IReadOnlyDictionary<(string Host, int Port), (string Scope, string Name)>? identityByEndpoint = null)
    => new(new HttpClient(new FakeHandler(r =>
    {
        var host = r.RequestUri!.Host;
        var port = r.RequestUri!.Port;
        lock (trace ?? new object())
        {
            trace?.Add(port);
        }

        // Д1б: /patroni — только карта (host, port)→(scope, name); порта в карте
        // нет → 404 (чужой Patroni по коллизионному порту). Прочие пути — прежний
        // respondByPort (живые /cluster-тесты не меняются).
        if (r.RequestUri.AbsolutePath == "/patroni")
        {
            var map = identityByEndpoint ?? DefaultIdentity;
            return map.TryGetValue((host, port), out var id)
                ? new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent(
                        $$"""{"state":"running","role":"replica","scope":"{{id.Scope}}","name":"{{id.Name}}"}""",
                        Encoding.UTF8, "application/json"),
                }
                : new HttpResponseMessage(HttpStatusCode.NotFound);
        }

        return respondByPort(port);
    })));
```

Там же `NewRig` получает опциональный параметр и прокидывает в Probe (иначе тесты Шага 6 не смогут подменять карту):

```csharp
private static async Task<Rig> NewRig(
    Func<int, HttpResponseMessage> patroniResponse, List<int>? trace = null, PlacementOptions? opts = null,
    IReadOnlyDictionary<(string Host, int Port), (string Scope, string Name)>? identityByEndpoint = null)
{
    ...
    var process = new ProvisioningProcess(
        etcd, [Ep], driver, sql, Probe(patroniResponse, trace, identityByEndpoint), ...);
}
```

ВАЖНО (ревью): дефолтная карта НЕ безобидна для DeadPatroni-тестов — `SeedCluster` сеёт `initialize`+`leader` (scopeReady=true), а `DefaultIdentity` ответила бы валидной identity на каждый `/patroni` стандартного сида → probesOurs=true → `WaitPatroni` вернул бы Ready даже при «мёртвом» `/cluster`-респондере (identity больше не зовёт `/cluster`). Поэтому ВСЕ риги с DeadPatroni получают явную пустую карту `identityByEndpoint: []` (каждый `/patroni` → 404 → probesOurs=false → бюджет-ветка сохраняет прежнюю семантику):

- существующие тесты: `Tick_FreshCluster_EnsureNodesThenInProgressWaitingPatroni`, `Tick_RequestResources_PassedToEnsureNodePerShard`, `Tick_PartialPortalloc_MergesFactKeepsObjectRecord_WritesByPut`, `Tick_PatroniBudgetFail_WritesRetrySeriesAndBacksOff`, `Tick_AfterRetryDeadline_FailsAgainWithIncrementedSeries`, `Tick_InProgressPhasesAfterFail_CarrySeriesUntilNextFail`, `Tick_PatroniBudgetFail_ResetsWaitTrackerForNextAttempt`, `Tick_ForeignObjectMatch_SkippedAndAllocatedNormally`;
- из Task 1 (пишутся раньше, карта добавляется этим же шагом): `Tick_ForeignDockerBusyPort_ReplansNodeInSameTick`, `Tick_SelfContainerDockerBusy_NotReplanned`;
- из Task 4 (пишутся уже с картой — см. Task 4): `Tick_BudgetDeadAllAbsent_ScopeResetInProgress`, `Tick_BudgetDeadAnyNodePresent_NoResetOperatorFail`, `Tick_BudgetDeadUnknown_NoResetWait`.

Живые PatroniAlive-тесты остаются на дефолтной карте: `Tick_PatroniAlive_DoesEverythingToDone`, `Tick_AfterDone_NoNewEnsureNodes`, `Tick_CreatesAppSecretKeysAndAlignsRole`, `Tick_SqlPhase_WritesNodeAppParamsForAllShardNodes`, `Tick_SqlFailure_ErrorAndJournalHaveNoAppPassword`; `Tick_NoRoutingKeys_…` и `Tick_ConfigSwitchedToRemove_…` фазы P2.2 не достигают.

Прогон: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~ProvisioningProcessTests"` — Expected: PASS (перечисленные ассерты не меняются; единственная правка текстового ассерта понадобится только в Task 4).

- [ ] **Шаг 6: падающий процессный тест (дописать)**

```csharp
// AAA: Д1б — по план-порту отвечает ЧУЖОЙ Patroni: идентификация не прошла —
// RUNNING не выставляется, InProgress-ожидание (фальш-RUNNING исключён)
[Fact]
public async Task Tick_ForeignPatroniOnPlannedPort_NotRunning()
{
    // Arrange: /cluster жив (respondByPort отвечает members), но /patroni по
    // план-портам — пустая карта → все 404 = чужой scope (коллизия портов).
    var rig = await NewRig(port => Patroni(port == 18000 ? "shard1a" : "shard2a"), identityByEndpoint: []);

    // Act
    var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

    // Assert: ожидание, ноды не RUNNING (бюджет 600 c ещё не истёк).
    outcome.IsSuccess.Should().BeTrue();
    outcome.Value.Should().Be(ProcessOutcome.InProgress);
    rig.Etcd.Store["/clusters/shop/shards/shard1/nodes/shard1a/state"].Value.Should().Be("PROVISIONING");
}
```

- [ ] **Шаг 7: прогон — падает (нода становится RUNNING по чужому ответу)**

Run: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~Tick_ForeignPatroniOnPlannedPort"`
Expected: FAIL — state=RUNNING (нынешняя `IsAliveAsync` считает 200 от /cluster достаточным).

- [ ] **Шаг 8: реализация — `WaitPatroniAsync` заменяет блок probesAlive**

```csharp
// Д1б (spec §3.7): проба обязана подтвердить ИМЕННО нашу ноду — GET /patroni
// несёт scope+name; чужой ответ по коллизионному порту ≠ наша нода
// (фальш-RUNNING/фальш-dsn на чужие данные исключены).
var probesOurs = true;
foreach (var node in topology.Nodes.Keys)
{
    var identity = await probe.IdentifyAsync(topology.Nodes[node], ct);
    if (!identity.IsSuccess
        || identity.Value is not { } id
        || id.Scope != scope
        || id.Name != node)
    {
        probesOurs = false;
        break;
    }
}

if (!scopeReady || !probesOurs)
{
    ... // прежняя бюджет-ветка без изменений (до Task 4)
}
```

(переменная `scope` уже есть в методе: `$"{cluster}-{shard.Name}"`.)

- [ ] **Шаг 9: прогон — зелёные**

Run: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~ProvisioningProcessTests|FullyQualifiedName~AddShardProcessTests"`
Expected: PASS (AddShard не менялся; его харнесс `Probe(respondByPort)` остаётся без identity — компиляция не зависит, т.к. новый параметр опциональный).

- [ ] **Шаг 10: коммит**

```bash
git add src/PgWorker.Provisioning/Probes/ShardProbe.cs src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs src/tests/PgWorker.UnitTests/Provisioning/ShardProbeTests.cs src/tests/PgWorker.UnitTests/Provisioning/ProvisioningProcessTests.cs
git commit -m "feat(pgworker): Д1б — WaitPatroni идентифицирует ноду (GET /patroni: scope+name; чужой ответ по коллизионному порту ≠ success) — фальш-RUNNING/фальш-dsn на чужие данные исключены; AddShard/надзор без изменений (spec §3.7 Д1б, arch/14 §5 A P2.2)"
```

---

### Task 3: Д2 — инвариант адресов Active в `AdoptionProcess` (spec §3.7 Д2, arch/14 §5 J AD2')

**Files:**
- Modify: `src/PgWorker.Provisioning/Processes/AdoptionProcess.cs` (ctor += `PortAllocIndex portAlloc, PlacementOptions placementOpts`; `ReconcileAddressesAsync` + вызов после AD1-чтения)
- Modify: `src/PgWorker.App/Program.cs` (проброс в фабрику AdoptionProcess)
- Test: Modify: `src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs` (`FakeDriver` += `InspectFault`)
- Test: Modify: `src/tests/PgWorker.UnitTests/Provisioning/AdoptionProcessTests.cs` (харнесс `NewAdoption` + 3 теста)

**Interfaces (Вход/Выход):**
- Consumes: `PortPlanConvergence.DetachColliding` (Task 1); `PortAllocIndex.ReadBusyAsync`; `driver.InspectNodesAsync(cluster, names, ct)` (фильтр кластера); `PlacementPlanner.Plan`; `PortAllocator.Allocate`; `Portalloc.Serialize`; `secrets`/`claims`/`journal`/`PutAsync`/`ReadMemberNamesAsync` (уже в AdoptionProcess).
- Produces: journal-фазы `repaired-portalloc` / `repaired-dsn` (op=adopt); `private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> ReconcileAddressesAsync(ClusterSnapshot snap, IReadOnlyDictionary<string, NodeAddress> existing, CancellationToken ct)` — кандидаты адресов = **nodes-ключи снапшота ∪ HA-members** (`ReadMemberNamesAsync`, как AD1 — сценарий «Active + dsn, nodes-ключей нет» тоже репарируется, spec §3.7 Д2); ctor `AdoptionProcess(…, WorkJournal journal, PortAllocIndex portAlloc, PlacementOptions placementOpts, Func<CancellationToken, Task<Result>>? snapshot = null)`.

- [ ] **Шаг 1: `FakeDriver` += сбой-инъекция инспекции (Fakes.cs; нужна тесту «transport-провал не роняет тик»)**

```csharp
// сбой-инъекция: docker-хост недоступен (Д2: transport-провал инспекции — transient)
public Exception? InspectFault { get; set; }

public Task<Result<IReadOnlyDictionary<string, DiscoveredNode>>> InspectNodesAsync(
    string cluster, IReadOnlyCollection<string> nodeNames, CancellationToken ct)
    => InspectFault is { } fault
        ? Task.FromResult(Result<IReadOnlyDictionary<string, DiscoveredNode>>.Failed(fault))
        : Task.FromResult(Result<IReadOnlyDictionary<string, DiscoveredNode>>.Success(
            (IReadOnlyDictionary<string, DiscoveredNode>)InspectResult
                .Where(p => nodeNames.Contains(p.Key))
                .ToDictionary(p => p.Key, p => p.Value)));
```

- [ ] **Шаг 2: падающие тесты (дописать в `AdoptionProcessTests`; харнесс `NewAdoption` дополнить двумя аргументами перед `snapshot`)**

```csharp
// в NewAdoption — два новых аргумента (using Microsoft.Extensions.Logging.Abstractions;
// using PgWorker.Core.Planning; уже есть/добавить):
//     new PortAllocIndex(etcd, [Ep], NullLogger<PortAllocIndex>.Instance),
//     new PlacementOptions(15000, 15100, PatroniBootSec: 600),
//     snapshot: null);

// AAA: Д2 — фальш-Active (portalloc/dsn на чужие порты — наследие коллизии):
// первый тик с живой docker-картиной репарирует адреса фактом и пересобирает dsn
[Fact]
public async Task TickAsync_DivergedPortalloc_FactRepairsAddressesAndDsn()
{
    // Arrange: Active demo, шард s1 с dsn; HA-members /service/demo-s1/members/{s1a,s1b}
    // сеются SnapshotActive ДО парсинга — кандидаты репарации непусты БЕЗ nodes-ключей
    // (Nodes строятся только из nodes/<n>/state, сид AFTER-парсинга парсер бы не увидел —
    // ревью). Запись s1/s1a РАСХОДИТСЯ с фактом (15014 — наследие коллизии).
    var etcd = new Fakes.FakeEtcd();
    var snap = await SnapshotActive(etcd, ["s1"], ["s1"]);
    etcd.Seed("/pgworker/portalloc/demo",
        """
        {"s1/s1a":{"host":"h1","pg":15014,"patroni":18014,"doorman":16514},
        "s1/s1b":{"host":"h2","pg":15005,"patroni":18005,"doorman":16505}}
        """);
    var journal = new RecordingJournal();
    journal.Attach(etcd);
    var (process, _) = await NewAdoption(etcd, new Dictionary<string, DiscoveredNode>
    {
        ["s1a"] = new("s1a", "h1", "pgw-demo-s1-s1a", 15004, 18004, 16504),
        ["s1b"] = new("s1b", "h2", "pgw-demo-s1-s1b", 15005, 18005, 16505),
    });

    // Act
    var outcome = await process.TickAsync(snap, CancellationToken.None);

    // Assert: запись перезаписана фактом; dsn пересобран из фактического portalloc
    // (по кандидатам nodes ∪ members); обе репарации в журнале (Д2, AD2').
    outcome.IsSuccess.Should().BeTrue();
    (await GetValueAsync(etcd, "/pgworker/portalloc/demo")).Should().Contain("\"pg\":15004");
    (await GetValueAsync(etcd, "/pgworker/portalloc/demo")).Should().NotContain("\"pg\":15014");
    (await GetValueAsync(etcd, "/clusters/demo/shards/s1/dsn"))
        .Should().Contain("port=15004,15005");
    journal.Entries.Should().Contain(e => e.Phase == "repaired-portalloc");
    journal.Entries.Should().Contain(e => e.Phase == "repaired-dsn");
}

// AAA: Д2 — сходящийся кластер: адреса/dsn соответствуют факту — мутаций нет
[Fact]
public async Task TickAsync_AddressesMatchFact_NoRepairMutations()
{
    // Arrange: portalloc == факт контейнеров; dsn уже равен пересобранному из
    // факта по кандидатам members (креды P2.5: bucket_admin + глобальный password).
    var etcd = new Fakes.FakeEtcd();
    var snap = await SnapshotActive(etcd, ["s1"], ["s1"]);
    etcd.Seed("/clusters/demo/shards/s1/dsn",
        "host=h1,h2 port=15004,15005 dbname=demo user=bucket_admin password=adm-pw");
    etcd.Seed("/pgworker/portalloc/demo",
        """
        {"s1/s1a":{"host":"h1","pg":15004,"patroni":18004,"doorman":16504},
        "s1/s1b":{"host":"h2","pg":15005,"patroni":18005,"doorman":16505}}
        """);
    var journal = new RecordingJournal();
    journal.Attach(etcd);
    var (process, _) = await NewAdoption(etcd, new Dictionary<string, DiscoveredNode>
    {
        ["s1a"] = new("s1a", "h1", "pgw-demo-s1-s1a", 15004, 18004, 16504),
        ["s1b"] = new("s1b", "h2", "pgw-demo-s1-s1b", 15005, 18005, 16505),
    });

    // Act
    var outcome = await process.TickAsync(snap, CancellationToken.None);

    // Assert: никаких repaired-фаз, version portalloc-ключа не выросла (идемпотентность).
    outcome.IsSuccess.Should().BeTrue();
    journal.Entries.Should().NotContain(e => e.Phase.StartsWith("repaired"));
    etcd.Store["/pgworker/portalloc/demo"].Version.Should().Be(1);
}

// AAA: Д2 — transport-провал инспекции = transient: тик не роняется, мутаций адресов нет
[Fact]
public async Task TickAsync_InspectTransportFails_TickSurvives()
{
    // Arrange: portalloc полный по обоим HA-members → missing пуст (основной AD1-путь
    // инспекцию НЕ зовёт) — docker-сбой ловит ТОЛЬКО AD2'-инспекция по кандидатам.
    var etcd = new Fakes.FakeEtcd();
    var snap = await SnapshotActive(etcd, ["s1"], ["s1"]);
    etcd.Seed("/pgworker/portalloc/demo",
        """
        {"s1/s1a":{"host":"h1","pg":15004,"patroni":18004,"doorman":16504},
        "s1/s1b":{"host":"h2","pg":15005,"patroni":18005,"doorman":16505}}
        """);
    var journal = new RecordingJournal();
    journal.Attach(etcd);
    var (process, _, driver) = await NewAdoption(etcd, new Dictionary<string, DiscoveredNode>());
    driver.InspectFault = new ApplicationException("docker: connection refused");

    // Act
    var outcome = await process.TickAsync(snap, CancellationToken.None);

    // Assert: тик жив (Done по инварианту ролей), адреса не тронуты (version та же),
    // repaired-фаз нет — следующий тик повторит сверку.
    outcome.IsSuccess.Should().BeTrue();
    journal.Entries.Should().NotContain(e => e.Phase.StartsWith("repaired"));
    etcd.Store["/pgworker/portalloc/demo"].Version.Should().Be(1);
}
```

(Харнесс дорабатывается: `NewAdoption` возвращает кортеж `(AdoptionProcess Process, Fakes.FakeSql Sql, Fakes.FakeDriver Driver)` — третий тест ставит `InspectFault` на драйвере; прочие тесты файла деконструируют первые два элемента — правка локальная.)

Замечание по покрытию существующих тестов (проверено по харнессу): `TickAsync_FullPortalloc_NoOpButRolesEnsured` сидируется без members (`SnapshotActive(etcd, ["s1","s2"], [])`) → кандидаты пусты → репарация тихо пропускает; `TickAsync_ExternalShard_MergesPortallocWithObject` — находки `as-*` отсечены фильтром канонического имени, dsn-петля спотыкается о «адресов не хватает» (merged до AD1 пуст); `TickAsync_NoContainersFound_SilentSkip` — 0 находок инспекции → тихий skip; `TickAsync_PartialDiscovery_JournalsSkippedNodes` — единственная находка `as-*` отсечена фильтром → репарация без мутаций, AD1-путь работает как раньше.

- [ ] **Шаг 3: прогон — падают**

Run: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~AdoptionProcessTests"`
Expected: FAIL — репарации нет (`pg":15014` остаётся / repaired-фаз нет).

- [ ] **Шаг 4: реализация**

4а. Ctor: после `WorkJournal journal` добавить `PortAllocIndex portAlloc, PlacementOptions placementOpts` (перед optional `snapshot`); using `PgWorker.Core.Planning`.

4б. Вызов в `TickAsync` — сразу после AD1-чтения `existing` (ДО подсчёта `missingByShard`, чтобы недостающие считались по репарированному словарю):

```csharp
// AD2' (Д2, arch/14 §5 J): инвариант адресов Active — portalloc/dsn = факт
// живых канонических контейнеров; расхождение репарируется под клэймом с
// журналом. Transport-провал инспекции — transient: тик продолжается
// без репарации (следующий тик повторит).
var reconciled = await ReconcileAddressesAsync(snap, existing.Value, ct);
if (!reconciled.IsSuccess)
    return await FailAsync(cluster, reconciled.Error!, ct);
existing = Result<IReadOnlyDictionary<string, NodeAddress>>.Success(reconciled.Value);
```

4в. Метод `ReconcileAddressesAsync` (полный код):

```csharp
// AD2' (Д2, spec §3.7): кандидаты — nodes-ключи снапшота ∪ HA-members (как AD1:
// сценарий «Active + dsn, nodes-ключей нет» тоже репарируется); merge факта
// канонических контейнеров (тот же фильтр, что P1) + перепланирование занятых
// чужим (PortPlanConvergence) + пересборка dsn из фактического portalloc.
// 0 находок — тихий skip (кластер вне docker-хостов); transport-провал
// инспекции — transient (не роняем тик).
private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> ReconcileAddressesAsync(
    ClusterSnapshot snap, IReadOnlyDictionary<string, NodeAddress> existing, CancellationToken ct)
{
    var cluster = snap.Config.Cluster;
    var dsnShards = snap.Shards.Where(s => s.Dsn is not null && !s.ToRemove).ToList();

    // Кандидаты адресов по каждому dsn-шарду: nodes-ключи ∪ HA-members
    // (members читаются и ниже в AD1 — дешёвый range, дублирование осознанное).
    var candidatesByShard = new Dictionary<string, List<string>>();
    foreach (var shard in dsnShards)
    {
        var members = await ReadMemberNamesAsync(cluster, shard.Name, ct);
        if (!members.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(members.Error!); // etcd-транспорт — как AD1
        var names = shard.Nodes.Select(n => n.Name).Concat(members.Value).Distinct().ToList();
        if (names.Count > 0)
            candidatesByShard[shard.Name] = names;
    }

    if (candidatesByShard.Count == 0)
        return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(existing); // кандидатов нет — репарировать нечего

    var discovered = await driver.InspectNodesAsync(
        cluster, candidatesByShard.Values.SelectMany(v => v).Distinct().ToList(), ct);
    if (!discovered.IsSuccess)
        return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(existing); // transient
    if (discovered.Value.Count == 0)
        return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(existing); // тихий skip (вне docker-хостов)

    var merged = new Dictionary<string, NodeAddress>(existing);
    var selfFact = new HashSet<(string, int)>();
    var changed = false;
    foreach (var (shardName, names) in candidatesByShard)
        foreach (var nodeName in names)
        {
            var key = $"{shardName}/{nodeName}";
            if (!discovered.Value.TryGetValue(nodeName, out var node))
                continue;
            var canonicalObject = $"pgw-{cluster}-{key.Replace('/', '-')}";
            if (node.Object != canonicalObject || node.Pg <= 0 || node.Patroni <= 0)
                continue; // не наша находка — фильтр канонического имени (как P1)
            var fact = node.ToAddress() with { Object = null };
            foreach (var p in new[] { fact.Ports.Pg, fact.Ports.Patroni, fact.Ports.Doorman })
                if (p > 0)
                    selfFact.Add((fact.Host, p));
            if (merged.TryGetValue(key, out var current) && current.Object is not null)
                continue; // object-записи не перезаписываем (R9)
            if (!merged.TryGetValue(key, out var same) || !same.Equals(fact))
            {
                merged[key] = fact;
                changed = true;
            }
        }

    // Перепланирование занятых чужим (Д1-механика для Active; busy = docker минус
    // свои ∪ portalloc соседей — как в P1, spec §8.10). Placement строится по
    // nodes-ключам снапшота: у шарда без nodes-ключей detach-нутая нода
    // переаллоцируется следующим тиком (после того как AD3 доведёт nodes).
    var dockerBusy = await driver.GetBusyPortsAsync(ct);
    if (!dockerBusy.IsSuccess)
        return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(dockerBusy.Error!);
    var foreignAlloc = await portAlloc.ReadBusyAsync(cluster, ct);
    if (!foreignAlloc.IsSuccess)
        return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(foreignAlloc.Error!);
    var foreign = new HashSet<(string, int)>(foreignAlloc.Value);
    foreach (var p in dockerBusy.Value)
        if (!selfFact.Contains(p))
            foreign.Add(p);
    if (PortPlanConvergence.DetachColliding(merged, selfFact, foreign))
    {
        // Недобор адресов снятых нод: переаллокация (паттерн P1-недобора).
        var hosts = await driver.GetHostsAsync(ct);
        if (!hosts.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(hosts.Error!);
        var plan = PlacementPlanner.Plan(dsnShards, hosts.Value);
        var allocated = PortAllocator.Allocate(plan, merged, foreign, placementOpts.PortFrom, placementOpts.PortTo);
        if (!allocated.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(allocated.Error!);
        foreach (var (k, addr) in allocated.Value)
            merged[k] = addr;
        changed = true;
    }

    if (changed)
    {
        var put = await PutAsync($"/pgworker/portalloc/{cluster}", Portalloc.Serialize(merged), ct);
        if (!put.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(put.Error!);
        await journal.WritePhaseAsync(cluster, Op, "repaired-portalloc", claims.InstanceId, null, ct);
    }

    // dsn-инвариант: пересборка multi-host dsn по кандидатам (nodes ∪ members) из
    // фактического portalloc (креды как P2.5: per-cluster override → глобальные).
    foreach (var shard in dsnShards)
    {
        if (!candidatesByShard.TryGetValue(shard.Name, out var names) || names.Count == 0)
            continue;
        var ordered = names.OrderBy(n => n, StringComparer.Ordinal).ToList();
        if (ordered.Any(n => !merged.ContainsKey($"{shard.Name}/{n}")))
            continue; // адресов не хватает — усыновление/следующий тик доведут
        var hosts = string.Join(",", ordered.Select(n => merged[$"{shard.Name}/{n}"].Host));
        var ports = string.Join(",", ordered.Select(n => merged[$"{shard.Name}/{n}"].Ports.Pg));
        var user = snap.Config.BucketAdminUser ?? "bucket_admin";
        var password = snap.Config.BucketAdminPassword ?? secrets.BucketAdminPassword;
        var dsn = $"host={hosts} port={ports} dbname={snap.Config.DbName} user={user} password={password}";
        if (shard.Dsn != dsn)
        {
            var put = await PutAsync($"/clusters/{cluster}/shards/{shard.Name}/dsn", dsn, ct);
            if (!put.IsSuccess)
                return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(put.Error!);
            await journal.WritePhaseAsync(cluster, Op, "repaired-dsn", claims.InstanceId, null, ct);
        }
    }

    return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(merged);
}
```

4г. `Program.cs` (фабрика AdoptionProcess — обернуть в блок с `opts`, по образцу ProvisioningProcess):

```csharp
builder.Services.AddSingleton(sp =>
{
    var opts = sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value;
    return new AdoptionProcess(
        sp.GetRequiredService<IEtcdGateway>(),
        opts.Etcd.Endpoints,
        sp.GetRequiredService<IClusterDriver>(),
        sp.GetRequiredService<ShardEndpoints>(),
        sp.GetRequiredService<ISqlExecutor>(),
        sp.GetRequiredService<IAppSecretEnsurer>(),
        sp.GetRequiredService<IAppParamsEnsurer>(),
        sp.GetRequiredService<InstallSecrets>(),
        sp.GetRequiredService<ClaimStore>(),
        sp.GetRequiredService<WorkJournal>(),
        sp.GetRequiredService<PortAllocIndex>(),
        new PlacementOptions(opts.Docker.PortRange.From, opts.Docker.PortRange.To, opts.Thresholds.PatroniBootSec,
            opts.Thresholds.ProvisionRetryBaseSec, opts.Thresholds.ProvisionRetryMaxSec),
        SnapshotDelegate(sp.GetRequiredService<SnapshotJob>()));
});
```

- [ ] **Шаг 5: прогон — зелёные**

Run: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~AdoptionProcessTests" && dotnet build src/PgWorker.slnx`
Expected: PASS (3 новых + 4 существующих — поведение существующих проверено в примечании Шага 2: кандидаты-фильтры не дают лишних мутаций) и сборка без warnings.

- [ ] **Шаг 6: коммит**

```bash
git add src/PgWorker.Provisioning/Processes/AdoptionProcess.cs src/PgWorker.App/Program.cs src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs src/tests/PgWorker.UnitTests/Provisioning/AdoptionProcessTests.cs
git commit -m "feat(pgworker): Д2 — инвариант адресов Active в AdoptionProcess (AD2'): portalloc/dsn = факт живых канонических контейнеров, занятое чужим перепланируется; фальш-Active/вечные UNREACHABLE самолечатся с журналом repaired-portalloc/repaired-dsn; transport-провал инспекции — transient (spec §3.7 Д2, arch/14 §5 J)"
```

---

### Task 4: Д3 — лечение HA-scope при доказанной утрате данных (spec §3.7 Д3, arch/14 §5 A P2.2, R11)

**Files:**
- Modify: `src/PgWorker.Docker/Drivers/ClusterDriver.cs` (enum `DataPresence`; `IClusterDriver` += `NodeDataPresenceAsync`; реализации Plain/Swarm)
- Modify: `src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs` (enum `WaitPatroniOutcome`; бюджет-ветка Д3; `ResetScopeAsync`)
- Modify: `src/tests/PgWorker.IntegrationTests/Etcd/StubScaleDriver.cs` (тривиальная реализация нового метода интерфейса — иначе ломается компиляция интеграционного проекта, он в `PgWorker.slnx`)
- Test: Modify: `src/tests/PgWorker.UnitTests/Docker/ClusterDriverTests.cs` (`FakeEngine` += `ExecStdout`; +3 теста)
- Test: Modify: `src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs` (`FakeDriver` += `DataPresenceByNode` + реализация интерфейса)
- Test: Modify: `src/tests/PgWorker.UnitTests/Provisioning/ProvisioningProcessTests.cs` (+3 теста; 1 правка существующего ассерта)

**Interfaces (Вход/Выход):**
- Consumes: `ExecNodeAsync(cluster, shard, node, cmd, ct)` (перебор хостов, running-контейнер); PGDATA-путь Spilo `/home/postgres/pgdata/pgroot/data/PG_VERSION` (arch/14 §2.1); `DeleteAsync(key, prefix, ct)` (уже в ProvisioningProcess); `_patroniWaitSince`-трекер.
- Produces: `public enum DataPresence { Present, Absent, Unknown }` (файл ClusterDriver.cs); `Task<Result<DataPresence>> NodeDataPresenceAsync(string cluster, string shard, string node, CancellationToken ct)` в `IClusterDriver`; поведение WaitPatroni — все Absent → чистка scope + фаза `reset-scope`; хоть одна Present → фейл «разбор оператора»; Unknown → ожидание нового бюджета.

- [ ] **Шаг 1: падающие тесты драйвера (`ClusterDriverTests`; `FakeEngine.ExecAsync` заменить на `ExecStdout`-заготовку: `public string ExecStdout { get; set; } = "";` → `Success(ExecStdout)`)**

```csharp
// AAA: Д3 — проба данных ноды: docker-exec test -f PG_VERSION → Present/Absent;
// контейнера нет → Unknown (транспорт ≠ доказательство утраты, arch/14 R11)
[Fact]
public async Task NodeDataPresence_StdoutPresent_Present()
{
    // Arrange: running-контейнер ноды; exec вернул "present" (PG_VERSION есть).
    var engine = new FakeEngine
    {
        Containers = [new DockerContainer("id1", ["pgw-shop-shard1-shard1a"], "running", "img")],
        ExecStdout = "present",
    };
    var driver = NewPlainDriver(engine);

    // Act
    var result = await driver.NodeDataPresenceAsync("shop", "shard1", "shard1a", CancellationToken.None);

    // Assert: данные доказанно есть.
    result.IsSuccess.Should().BeTrue();
    result.Value.Should().Be(DataPresence.Present);
}

[Fact]
public async Task NodeDataPresence_StdoutAbsent_Absent()
{
    // Arrange: контейнер жив, PG_VERSION нет (volume пуст — доказанная утрата).
    var engine = new FakeEngine
    {
        Containers = [new DockerContainer("id1", ["pgw-shop-shard1-shard1a"], "running", "img")],
        ExecStdout = "absent",
    };
    var driver = NewPlainDriver(engine);

    // Act
    var result = await driver.NodeDataPresenceAsync("shop", "shard1", "shard1a", CancellationToken.None);

    // Assert: данных доказанно нет.
    result.Value.Should().Be(DataPresence.Absent);
}

[Fact]
public async Task NodeDataPresence_NoRunningContainer_Unknown()
{
    // Arrange: контейнера нет — утрата НЕ доказана.
    var engine = new FakeEngine { Containers = [] };
    var driver = NewPlainDriver(engine);

    // Act
    var result = await driver.NodeDataPresenceAsync("shop", "shard1", "shard1a", CancellationToken.None);

    // Assert: Unknown — чистка scope запрещена.
    result.Value.Should().Be(DataPresence.Unknown);
}
```

- [ ] **Шаг 2: прогон — падают**

Run: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~NodeDataPresence"`
Expected: FAIL — enum/метод не существуют.

- [ ] **Шаг 3: реализация драйвера (ClusterDriver.cs)**

```csharp
/// <summary>Наличие данных PG у ноды (Д3, arch/14 R11): Present/Absent — доказано
/// exec-пробой PG_VERSION; Unknown — транспорт недоступен (НЕ доказательство утраты).</summary>
public enum DataPresence { Present, Absent, Unknown }
```

В `IClusterDriver` (после `StopNodeAsync`):

```csharp
// Данные ноды (Д3, spec §3.7): docker-exec test -f PG_VERSION; контейнера
// нет/exec-сбой/нечитаемый stdout → Unknown (утрата не доказана).
Task<Result<DataPresence>> NodeDataPresenceAsync(string cluster, string shard, string node, CancellationToken ct);
```

Реализация (одинаковая в `PlainClusterDriver` и `SwarmClusterDriver` — каждый через свой `ExecNodeAsync`):

```csharp
public async Task<Result<DataPresence>> NodeDataPresenceAsync(string cluster, string shard, string node, CancellationToken ct)
{
    // PGDATA Spilo (arch/14 §2.1): volume-корень /home/postgres/pgdata,
    // данные — pgroot/data/PG_VERSION.
    const string marker = "/home/postgres/pgdata/pgroot/data/PG_VERSION";
    var exec = await ExecNodeAsync(cluster, shard, node,
        ["sh", "-c", $"test -f {marker} && echo present || echo absent"], ct);
    if (!exec.IsSuccess)
        return Result<DataPresence>.Success(DataPresence.Unknown);
    return Result<DataPresence>.Success(exec.Value.Trim() switch
    {
        "present" => DataPresence.Present,
        "absent" => DataPresence.Absent,
        _ => DataPresence.Unknown,
    });
}
```

`Fakes.FakeDriver` (Fakes.cs) — реализация с картой (default Present — безопасный: без явного доказательства утраты данные считаем живыми):

```csharp
// Д3: карта присутствия данных по имени ноды (default Present — чистка запрещена).
public Func<string, DataPresence> DataPresenceByNode { get; set; } = _ => DataPresence.Present;

public Task<Result<DataPresence>> NodeDataPresenceAsync(string cluster, string shard, string node, CancellationToken ct)
    => Task.FromResult(Result<DataPresence>.Success(DataPresenceByNode(node)));
```

`src/tests/PgWorker.IntegrationTests/Etcd/StubScaleDriver.cs` — тривиальная реализация (ревью: проект в `PgWorker.slnx`, без неё ломается компиляция):

```csharp
// Д3 вне контрактных сценариев scale (t06 §8): утрата не доказана — не лечим.
public Task<Result<DataPresence>> NodeDataPresenceAsync(string cluster, string shard, string node, CancellationToken ct)
    => Task.FromResult(Result<DataPresence>.Success(DataPresence.Unknown));
```

Прогнать драйверные: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~ClusterDriverTests"` — PASS. Процессные пока не затронуты (вызова `NodeDataPresenceAsync` из ProvisioningProcess ещё нет — он появится в Шаге 6).

- [ ] **Шаг 4: падающие процессные тесты Д3 (дописать в `ProvisioningProcessTests`)**

```csharp
// AAA: Д3 — бюджет лидера исчерпан + ВСЕ ноды scope без данных (доказанная
// утрата): HA-scope чистится, request_* живы, фаза reset-scope, InProgress
[Fact]
public async Task Tick_BudgetDeadAllAbsent_ScopeResetInProgress()
{
    // Arrange: лидеров нет (ключи удалены), Patroni-REST молчит, бюджет -1;
    // все ноды обоих scope БЕЗ данных; заявка request_cpu — декларация панели.
    // identityByEndpoint: [] — пустая карта /patroni (Task 2): probesOurs=false.
    var rig = await NewRig(_ => DeadPatroni(), opts: new PlacementOptions(15000, 15100, PatroniBootSec: -1),
        identityByEndpoint: []);
    rig.Etcd.Store.Remove("/service/shop-shard1/leader");
    rig.Etcd.Store.Remove("/service/shop-shard2/leader");
    rig.Etcd.Seed("/service/shop-shard1/request_cpu", "2");
    rig.Driver.DataPresenceByNode = _ => DataPresence.Absent;

    // Act
    var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

    // Assert: HA-scope очищен (initialize/optime/members), request_* живы,
    // reset в журнале, исход InProgress (Patroni бутстрапится заново, бюджет новый).
    outcome.IsSuccess.Should().BeTrue();
    outcome.Value.Should().Be(ProcessOutcome.InProgress);
    rig.Etcd.Store.Keys.Should().NotContain("/service/shop-shard1/initialize");
    rig.Etcd.Store.Keys.Should().NotContain("/service/shop-shard2/initialize");
    rig.Etcd.Store.Keys.Should().NotContain(k => k.StartsWith("/service/shop-shard1/members/", StringComparison.Ordinal));
    rig.Etcd.Store.Keys.Should().Contain("/service/shop-shard1/request_cpu");
    var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
    work.Value!.Phase.Should().Be("reset-scope");
}

// AAA: Д3 — данные есть хотя бы у одной ноды scope: чистки НЕТ, фейл оператору
// (чистка scope уничтожила бы данные, arch/14 R11)
[Fact]
public async Task Tick_BudgetDeadAnyNodePresent_NoResetOperatorFail()
{
    // Arrange: как выше, но у shard1a данные ЕСТЬ (разбор оператора). Ключи
    // scope shop-shard1 обязаны уцелеть; про shop-shard2 ассертов нет — его
    // ноды Absent, чистка этого scope допустима по контракту.
    var rig = await NewRig(_ => DeadPatroni(), opts: new PlacementOptions(15000, 15100, PatroniBootSec: -1),
        identityByEndpoint: []);
    rig.Etcd.Store.Remove("/service/shop-shard1/leader");
    rig.Etcd.Store.Remove("/service/shop-shard2/leader");
    rig.Driver.DataPresenceByNode = node => node == "shard1a" ? DataPresence.Present : DataPresence.Absent;

    // Act
    var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

    // Assert: фейл с текстом оператора; scope shard1 НЕ тронут (initialize цел).
    outcome.IsSuccess.Should().BeFalse();
    var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
    work.Value!.LastError.Should().Contain("разбор оператора");
    rig.Etcd.Store.Keys.Should().Contain("/service/shop-shard1/initialize");
}

// AAA: Д3 — docker недоступен (Unknown): утрата НЕ доказана — ждём, ключи целы
[Fact]
public async Task Tick_BudgetDeadUnknown_NoResetWait()
{
    // Arrange: все пробы Unknown (транспорт docker).
    var rig = await NewRig(_ => DeadPatroni(), opts: new PlacementOptions(15000, 15100, PatroniBootSec: -1),
        identityByEndpoint: []);
    rig.Etcd.Store.Remove("/service/shop-shard1/leader");
    rig.Etcd.Store.Remove("/service/shop-shard2/leader");
    rig.Driver.DataPresenceByNode = _ => DataPresence.Unknown;

    // Act
    var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

    // Assert: InProgress-ожидание, scope цел.
    outcome.IsSuccess.Should().BeTrue();
    outcome.Value.Should().Be(ProcessOutcome.InProgress);
    rig.Etcd.Store.Keys.Should().Contain("/service/shop-shard1/initialize");
}
```

Плюс правка существующего ассерта `Tick_PatroniBudgetFail_WritesRetrySeriesAndBacksOff` (ветка бюджет-фейла теперь доходит до оператора — default Present):

```csharp
// было: work.Value!.LastError.Should().Contain("не поднялся");
work.Value!.LastError.Should().Contain("разбор оператора");
```

- [ ] **Шаг 5: прогон — падают (Д3-тесты: без чистки/фазы reset-scope/текста оператора)**

Run: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~BudgetDead|FullyQualifiedName~Tick_PatroniBudgetFail"`
Expected: FAIL у трёх новых BudgetDead-тестов и у правленого ассерта `Tick_PatroniBudgetFail_WritesRetrySeriesAndBacksOff` (текст ещё старый — «не поднялся»; ветвление появится в Шаге 6).

- [ ] **Шаг 6: реализация в `ProvisioningProcess.cs`**

6а. Тип-исход ожидания (рядом с полями класса) + смена сигнатуры `WaitPatroniAsync` → `Result<WaitPatroniOutcome>`:

```csharp
// Исход P2.2-ожидания: ждём / готово / починили HA-scope (Д3 — тик завершается
// фазой reset-scope, Patroni бутстрапится заново).
private enum WaitPatroniOutcome { Waiting, Ready, ResetScope }
```

6б. Бюджет-ветка (заменить блок `if (now - since > placementOpts.PatroniBootSec) { TryRemove…; return Failed…; }`):

```csharp
if (now - since > placementOpts.PatroniBootSec)
{
    // Бюджет исчерпан: сброс трекера — новая попытка получает полный бюджет
    // заново (E3); далее — лечение HA-scope при доказанной утрате (Д3).
    _patroniWaitSince.TryRemove(scope, out _);

    // Д3 (spec §3.7, arch/14 R11): трёхуровневая проба данных нод scope.
    var presences = new List<DataPresence>();
    foreach (var node in shard.Nodes)
    {
        var presence = await driver.NodeDataPresenceAsync(cluster, shard.Name, node.Name, ct);
        presences.Add(presence.IsSuccess ? presence.Value : DataPresence.Unknown);
    }

    if (presences.All(p => p == DataPresence.Absent))
    {
        var reset = await ResetScopeAsync(scope, ct);
        if (!reset.IsSuccess)
            return Result<WaitPatroniOutcome>.Failed(reset.Error!);
        return Result<WaitPatroniOutcome>.Success(WaitPatroniOutcome.ResetScope);
    }

    var alive = string.Join(",", shard.Nodes
        .Where((n, i) => presences[i] == DataPresence.Present).Select(n => n.Name));
    if (alive.Length > 0)
        return Result<WaitPatroniOutcome>.Failed(new ApplicationException(
            $"{scope}: данные есть (ноды {alive}), лидера нет {placementOpts.PatroniBootSec} с — разбор оператора: чистка scope уничтожила бы данные"));

    return Result<WaitPatroniOutcome>.Success(WaitPatroniOutcome.Waiting); // Unknown: утрата не доказана — новый бюджет
}
```

Остальные return'ы метода: `Success(false)` → `Success(WaitPatroniOutcome.Waiting)`, `Success(true)` → `Success(WaitPatroniOutcome.Ready)`; в конце — `Success(WaitPatroniOutcome.Ready)`.

6в. Вызов в `TickAsync` (Parallel-блок шардов) + финализация:

```csharp
var booted = await WaitPatroniAsync(cluster, shard, topology, token);
if (!booted.IsSuccess)
{
    shardErrors.Enqueue(booted.Error!);
    return;
}
if (booted.Value == WaitPatroniOutcome.ResetScope)
{
    resetScopes.Enqueue(shard.Name); // Д3: тик завершится фазой reset-scope
    return;
}
if (booted.Value == WaitPatroniOutcome.Waiting)
    return; // InProgress — не ошибка, следующий тик
```

(перед Parallel-блоком: `var resetScopes = new ConcurrentQueue<string>();`; после `if (shardErrors.TryDequeue(out var firstError)) … return FailAsync…` вставить:)

```csharp
// Д3: чистка HA-scope выполнена — тик завершаем журналом reset-scope (одна
// фаза на тик; серию переносим — прогресс, не фейл).
if (resetScopes.TryDequeue(out _))
    return await Finish(cluster, "reset-scope", ProcessOutcome.InProgress, ct, series);
```

6г. Метод `ResetScopeAsync` (рядом с `WaitPatroniAsync`):

```csharp
// Д3: чистка HA-scope (Patroni бутстрапится заново): точечные initialize/leader/
// sync + префиксы optime//members/; request_* — декларации панели — НЕ трогаем
// (spec §3.7 Д3, arch/14 §5 A P2.2/R11). Одна чистка на scope за бюджет —
// трекер сброшен, следующая не раньше нового бюджета.
private async Task<Result> ResetScopeAsync(string scope, CancellationToken ct)
{
    foreach (var key in new[] { "initialize", "leader", "sync" })
    {
        var del = await DeleteAsync($"/service/{scope}/{key}", prefix: false, ct);
        if (!del.IsSuccess)
            return del;
    }

    foreach (var prefix in new[] { $"/service/{scope}/optime/", $"/service/{scope}/members/" })
    {
        var del = await DeleteAsync(prefix, prefix: true, ct);
        if (!del.IsSuccess)
            return del;
    }

    return Result.Success();
}
```

- [ ] **Шаг 7: прогон — зелёные**

Run: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~ClusterDriverTests|FullyQualifiedName~ProvisioningProcessTests"`
Expected: PASS (3 драйверных + 3 Д3 + все существующие: `Tick_AfterRetryDeadline_…`/`Tick_InProgressPhases_…`/`Tick_PatroniBudgetFail_ResetsWaitTracker…` не ассертят текст ошибки — фейл-ветка Present сохраняет серию/трекер).

- [ ] **Шаг 8: коммит**

```bash
git add src/PgWorker.Docker/Drivers/ClusterDriver.cs src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs src/tests/PgWorker.IntegrationTests/Etcd/StubScaleDriver.cs src/tests/PgWorker.UnitTests/Docker/ClusterDriverTests.cs src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs src/tests/PgWorker.UnitTests/Provisioning/ProvisioningProcessTests.cs
git commit -m "feat(pgworker): Д3 — лечение HA-scope при доказанной утрате данных (NodeDataPresenceAsync PG_VERSION-проба Present/Absent/Unknown; все Absent → чистка initialize/leader/sync/optime/members с фазой reset-scope, request_* живы; Present → фейл «разбор оператора»; Unknown → ждать) — мёртвые «waiting for leader to bootstrap» самолечатся (spec §3.7 Д3, arch/14 R11)"
```

---

### Task 5: Финал — сборка, полные прогоны, self-review, stand-verification (spec Ф6 + Ф7-документ)

**Files:**
- Modify: `docs/superpowers/2026-09-01-fix-provision-portalloc-alerts/stand-verification.md` (+разделы наблюдения Д1/Д2/Д3)
- Modify: (по результату self-review — точечные правки кода/тестов)

**Interfaces (Вход/Выход):**
- Consumes: всё выше (Tasks 1–4 + реализованная база).
- Produces: зелёная сборка и все прогоны обоих решений; сверка диффа с контрактами arch/; документ верификации, покрывающий критерии приёмки §7.8–7.10.

- [ ] **Шаг 1: сборка без warnings**

Run: `dotnet build src/PgWorker.slnx`
Expected: Build succeeded, 0 warnings.

- [ ] **Шаг 2: полные прогоны**

Run: `dotnet test src/tests/PgWorker.UnitTests && dotnet test src/tests/AdminPanel.UnitTests && DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test src/tests/PgWorker.IntegrationTests && DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test src/tests/AdminPanel.IntegrationTests`
Expected: PASS; прирост юнит-тестов воркера против чисел Task A0 = +19 (Task 1: 5+2, Task 2: 2+1, Task 3: 3, Task 4: 3+3). Панель/интеграционные — без регрессий (панель в остатке не менялась).

- [ ] **Шаг 3: self-review против контрактов (чеклист по диффу `git diff 2182372..HEAD`)**

- arch/14 §5 A P1 (Д1): detach — только записи «не подтверждены фактом своего контейнера И заняты чужим»; selfFact вычитается в обоих потребителях (`PlanPortsAsync`, `ReconcileAddressesAsync`); busy для `PortAllocator.Allocate` = foreign (свои публикации — закрепления, не запреты).
- arch/14 §5 A P2.2 (Д1б): WaitPatroni идентифицирует scope+name; чужой ответ ≠ success; `AddShardProcess.WaitPatroniAsync` и `NodeSupervisor` НЕ перешли на identity (граница §5).
- arch/14 §5 J AD2' (Д2): репарация portalloc/dsn с журналом `repaired-portalloc`/`repaired-dsn`; object-записи не перезаписаны нигде; 0 находок — тихий skip; transport-провал — transient.
- arch/14 R11 (Д3): чистка ТОЛЬКО при Absent у ВСЕХ нод scope; request_* живы; одна чистка на бюджет; Present → фейл оператору; Unknown → ждать.
- arch/14 R12: dsn меняется только при расхождении с фактом (стабильный кластер не трогается); журналирование repaired-dsn на месте.
- spec §5: swarm получил `NodeDataPresenceAsync` через свой `ExecNodeAsync`; `SwarmClusterDriver.InspectNodesAsync` остаётся заглушкой; roadmap-файлы не тронуты (t90 уже сужен базовой частью).

- [ ] **Шаг 4: дополнить `stand-verification.md` (раздел наблюдения после деплоя — read-only; существующие разделы заняты № 1–4, новый — «## 5.»)**

Добавить раздел «Д1–Д3 (автономный reconcile)» с проверками:

```markdown
## 5. Автономный reconcile Д1–Д3 (после деплоя, read-only)

- Д1 (коллизия портов): пересекающиеся portalloc canon10/smoke сходятся — каждый
  кластер на своих свободных портах; в docker logs воркера исчезает цикл
  «port is already allocated» за ≤ 2 тика; etcdctl get /pgworker/portalloc/<C> —
  без чужих пересечений (сверка с docker ps).
- Д2 (фальш-Active): журнал /pgworker/work/<C> (docker logs) показывает фазы
  repaired-portalloc / repaired-dsn; unreachable-ноды Active уходят вместе с
  репарацией адресов; dsn в etcd указывает на фактические порты контейнеров.
- Д3 (мёртвый HA-scope): scope без лидера при пустых volume — фаза reset-scope
  в журнале, ключи /service/<scope>/{initialize,leader,sync} исчезают, request_*
  живы, Patroni бутстрапится (initialize появляется заново); при живых данных —
  last_error «разбор оператора» (панель: provision-stuck с текстом).
```

- [ ] **Шаг 5: итоговый коммит**

```bash
git add -A
git commit -m "docs(stand): верификация автономного reconcile Д1–Д3 на живом стенде (repaired-*/reset-scope, сходимость portalloc) + правки self-review фазы Ф6"
```

(если правок self-review не было — коммит только stand-verification.md; ветка готова к ревью перед main.)

---

## Порядок зависимостей

```
Task A0 (гейт: база зелёная)
Task 1 (Д1: PortPlanConvergence + P1) ──► Task 3 (Д2: AD2' переиспользует DetachColliding)
Task 2 (Д1б: identity) — после Task 1 (общие тестовые харнессы ProvisioningProcessTests);
      не зависит по коду, но правит тот же WaitPatroniAsync-регион, что и Task 4
Task 4 (Д3: бюджет-ветка WaitPatroni) — после Task 2 (правит смежный блок того же метода)
Task 5 (финал) — после ВСЕХ
```

Рекомендуемая последовательность: **A0 → 1 → 2 → 4 → 3 → 5** (Д3 раньше Д2: Task 4 изолирован драйвером+процессом provision, Task 3 зависит от Task 1 и не пересекается по файлам с Task 4; допустим и порядок 1 → 2 → 3 → 4).

## Границы остатка (не делать)

- НЕ перепланировать реализованное (Часть A): WorkJournal/бэкофф/PortAllocIndex/adoption/EnsureNode-сверка/панель/чек 15 — всё уже в ветке.
- НЕ трогать стенд (docker/etcd-мутации), e2e-мутации, деплой — вне плана.
- НЕ расширять identity/бэкофф/Д3 на AddShardProcess и NodeSupervisor (spec §5; механика переносится отдельной задачей при необходимости).
- НЕ заводить новые roadmap-задачи и НЕ править контракты arch/ (уже обновлены в ветке; при расхождении кода и контракта — стоп и разбор с координатором).
