# t06 — rolling-перегенерация брокеров Kafka: план реализации

> **Для агентных исполнителей:** ОБЯЗАТЕЛЬНЫЙ SUB-SKILL: superpowers:subagent-driven-development
> (рекомендуется) или superpowers:executing-plans — исполнение по задачам; шаги
> отмечаются чекбоксами (`- [ ]`).

**Цель:** автоконверге лимитов контейнеров брокеров (cpu/mem) к декларации
`brokers/<b>/resources` — rolling-пересоздание по одному за тик без потери
данных + мутация №15 (PUT resources) + прогресс в панели.

**Архитектура:** воркер получает docker-inspect лимитов (новый метод драйвера),
чистая функция сверяет их с декларацией; новый процесс NodeRegenerator (J) в
Active-ветке конвейера делает максимум одно пересоздание за тик (Remove без
тома → Ensure с новыми лимитами/env → PROVISIONING; возврат в RUNNING —
штатный AddBrokerProcess). Прогресс — live-ключ `/kafkaworker/regens/<C>` (по
образцу reassignments): ставится ТОЛЬКО при живой операции (расхождения есть
или хвост недоведённого пересоздания) — чужие PROVISIONING-ноды (add-broker/
надзор) фантомного прогресса не рисуют. Панель: чтение ключа в
kafka-снапшот, прокси PUT-мутации в API воркера, модалка ресурсов + строка
прогресса в UI.

**Стек:** .NET 10 (LangVersion=latest, Nullable=enable, TreatWarningsAsErrors),
xUnit + FluentAssertions, Testcontainers; React+Mantine (фронт панели);
bash-чеки стенда.

**Spec:** `docs/superpowers/2026-09-02-t06-kafka-node-regen/spec.md` — план
аргументируется от спеки; исполнители читают оба файла. Ревью Фазы 4
(раунд 1, CHANGES_REQUESTED) учтено: формулы сверки = арифметика записи
(decimal→double→NanoCPUs), интеграционные кейсы доводят кластер циклом
Provision до Active перед дискавери, X2 покрыт тестом, фантом-правки убраны.
Ревью Фазы 4 (раунд 2) учтено: прогресс-ключ только при живой операции
(замечание 1), паттерн инициализации FixedTimeProvider (замечание 2).

## Глобальные ограничения

- Всё — ТОЛЬКО в worktree `/Users/demakaev/ZCodeProject/worktrees/feat-t06-kafka-node-regen`;
  абсолютные пути ниже даны от его корня.
- `dotnet build src/PgWorker.slnx` — 0 warnings (TreatWarningsAsErrors=true):
  прогон сборки обязателен в проверках шагов.
- Интеграционные тесты: порты docker-контейнеров динамические
  (`WithPortBinding(..., assignRandomHostPort: true)` / `FreePortWindow`),
  никаких хардкодов хост-портов в assertions; `BrokerBootSec` фикстур ≤ 100 с.
- Комментарии в тестах — по нотации AAA (Arrange/Act/Assert).
- Язык комментариев/документации — русский; идентификаторы — английские.
- Контракт уже обновлён spec-фазой: `arch/15-kafka-clusters.md` (§2/§4/§6),
  `arch/16-kafkaworker.md` (§3.2/§5 J/§6/§9), `arch/adminpanel/02-etcd-contract.md`
  (§10.1–10.3). План НЕ меняет arch/ (кроме случая расхождения с фактом — тогда
  сначала правка arch, потом код).
- Фронт: `frontend/package.json` — проверка `npm run typecheck` и `npm run build`
  (в каталоге `frontend/`).
- Порядок задач = порядок зависимостей; коммит после каждой задачи
  (`feat(kafka): t06 — <шаг>`; работа в ветке worktree, без push).
- **До Task 1** — закоммитить артефакты фаз 0–4 (контракты + spec + план):

```bash
git add arch/15-kafka-clusters.md arch/16-kafkaworker.md arch/adminpanel/02-etcd-contract.md docs/superpowers/2026-09-02-t06-kafka-node-regen/
git commit -m "docs(kafka): t06 — контракты arch (15/16/02), spec и план (rolling-перегенерация брокеров)"
```

## Карта файлов

| Файл | Ответственность | Задача |
|---|---|---|
| `src/KafkaWorker.Core/Planning/NodeRegenPlanner.cs` | `NodeLimits` + чистая сверка лимитов | 1 |
| `src/tests/KafkaWorker.UnitTests/Planning/NodeRegenPlannerTests.cs` | AAA-тесты сверки | 1 |
| `src/KafkaWorker.Docker/Engine/IDockerEngine.cs` | + 2 inspect-метода | 2 |
| `src/KafkaWorker.Docker/Engine/DockerEngine.cs` | + реализации inspect (plain/swarm) | 2 |
| `src/KafkaWorker.Docker/Drivers/ClusterDriver.cs` | + `NodeResourcesAsync` (интерфейс, plain, swarm) | 2 |
| `src/tests/KafkaWorker.UnitTests/Provisioning/Fakes.cs` | фейк: лимиты + отказы инспекта | 2 |
| `src/KafkaWorker.Core/Writing/KafkaWriting.cs` | request/валидатор/план мутации №15 | 3 |
| `src/KafkaWorker.App/Api/Operations/KafkaExceptions.cs` | + KafkaBrokerRemovalInProgressException | 3 |
| `src/KafkaWorker.App/Api/Operations/UpdateBrokerResourcesHandler.cs` | хендлер мутации №15 | 3 |
| `src/KafkaWorker.App/Api/ApiModule.cs` | + маршрут PUT resources (№15) | 3 |
| `src/tests/KafkaWorker.UnitTests/Writing/KafkaResourcesUpdateValidatorTests.cs` | AAA-тесты валидатора | 3 |
| `src/tests/KafkaWorker.IntegrationTests/Api/UpdateBrokerResourcesApiTests.cs` | API-кейсы №15 (etcd-only) | 3 |
| `src/KafkaWorker.Provisioning/Processes/NodeRegenerator.cs` | процесс J (spec §5.2) | 4 |
| `src/KafkaWorker.App/Loops/KafkaClusterProcesses.cs` | J в конвейере Active | 4 |
| `src/KafkaWorker.Provisioning/Processes/DeprovisioningProcess.cs` | X2: del regens-ключа | 4 |
| `src/KafkaWorker.App/Program.cs` | DI NodeRegenerator | 4 |
| `src/tests/KafkaWorker.UnitTests/Provisioning/NodeRegeneratorTests.cs` | AAA-тесты процесса J | 4 |
| `src/tests/KafkaWorker.UnitTests/Provisioning/DeprovisioningProcessTests.cs` | + regens-ключ в X2-кейс | 4 |
| `src/tests/KafkaWorker.IntegrationTests/Kafka/NodeRegenTests.cs` | полный цикл (docker) | 5 |
| `src/AdminPanel.Core/Kafka/KafkaSnapshot.cs` | + KafkaRegenProgress + поле снапшота | 6 |
| `src/AdminPanel.Etcd/Parsing/KafkaParser.cs` | + ParseRegens | 6 |
| `src/AdminPanel.Etcd/KafkaSnapshotRefresher.cs` | + префикс/чтение regens | 6 |
| `src/AdminPanel.Api/Inspection/KafkaQuery.cs` | + KafkaRegenDto + MapDetails | 6 |
| `src/tests/AdminPanel.UnitTests/KafkaParserTests.cs` | + тесты ParseRegens | 6 |
| `src/AdminPanel.Api/Operations/Kafka/BrokerResourcesCommands.cs` | прокси-команда №15 (new; образец — `RebalanceCommands.cs`) | 7 |
| `src/AdminPanel.Api/Operations/Kafka/KafkaOperationsModule.cs` | + прокси-маршрут PUT | 7 |
| `src/tests/AdminPanel.UnitTests/Operations/WorkerProxyCommandTests.cs` | + кейс прокси №15 | 7 |
| `frontend/src/api/dto.ts`, `frontend/src/api/queries.ts` | DTO + api-функция | 8 |
| `frontend/src/pages/kafka-cluster/EditBrokerResourcesModal.tsx` | модалка (new) | 8 |
| `frontend/src/pages/kafka-cluster/BrokersTab.tsx` | кнопка + подпись реген-ноды | 8 |
| `frontend/src/pages/kafka-cluster/KafkaClusterDetailsPage.tsx` | строка прогресса | 8 |
| `dev-stand/adminpanel/checks/59-kafka-regen.sh` | e2e-чек (new) | 9 |

---

## Волна A — KafkaWorker (задачи 1–5)

### Task 1: Чистая сверка лимитов — NodeRegenPlanner

**Вход:** собранный worktree, spec §5.2 (J2), §5.3 (формулы).
**Выход:** `NodeLimits` + `NodeRegenPlanner` в Core с зелёными unit-тестами.
**Spec:** §5.2 J2 (сверка), §5.3 (формулы идентичны записи), §3.2 (только cpu/mem).

**Files:**
- Create: `src/KafkaWorker.Core/Planning/NodeRegenPlanner.cs`
- Test: `src/tests/KafkaWorker.UnitTests/Planning/NodeRegenPlannerTests.cs`

**Interfaces (produces):**
- `public sealed record NodeLimits(long NanoCpus, long MemoryBytes)` —
  пространство имён `KafkaWorker.Core.Planning` (0 = без лимита).
- `public static class NodeRegenPlanner` с:
  - `static long ExpectedNanoCpus(decimal cpu)`
  - `static long ExpectedMemoryBytes(int memGi)`
  - `static bool NeedsRegen(BrokerResources decl, NodeLimits actual)`

- [x] **Шаг 1. Пишет failing-тест** (файл
  `src/tests/KafkaWorker.UnitTests/Planning/NodeRegenPlannerTests.cs`):

```csharp
using FluentAssertions;
using KafkaWorker.Core.Model;
using KafkaWorker.Core.Planning;
using Xunit;

namespace KafkaWorker.UnitTests.Planning;

// Сверка лимитов контейнера с декларацией resources (t06, spec §5.2 J2 / §5.3).
// ВАЖНО: формула cpu повторяет арифметику ЗАПИСИ DockerEngine — decimal →
// double → (long)(cores * 1e9) — иначе значения, непредставимые точно в
// double (0.01, 1.15), дают вечное «расхождение» и цикл рестартов
// (ревью Фазы 4, замечание 4).
public class NodeRegenPlannerTests
{
    [Theory]
    [InlineData("2", 2_000_000_000L)]
    [InlineData("0.5", 500_000_000L)]
    [InlineData("0.01", 10_000_000L)]   // double(0.01)*1e9 == double-арифметика записи
    [InlineData("1.15", 1149999999L)]   // непредставимо в double точно —
                                        // (long)(1.1499999999…*1e9) == 1149999999
    public void ExpectedNanoCpus_MatchesDockerEngineWriteArithmetic(string cpu, long nano)
    {
        // Act
        var actual = NodeRegenPlanner.ExpectedNanoCpus(decimal.Parse(cpu, System.Globalization.CultureInfo.InvariantCulture));

        // Assert
        actual.Should().Be(nano);
    }

    [Fact]
    public void ExpectedMemoryBytes_IsGiBShifted()
    {
        // Act
        var actual = NodeRegenPlanner.ExpectedMemoryBytes(4);

        // Assert
        actual.Should().Be(4L * 1024 * 1024 * 1024);
    }

    [Fact]
    public void NeedsRegen_EqualLimits_False()
    {
        // Arrange — лимиты контейнера получены той же арифметикой (запись)
        var decl = new BrokerResources(2m, 4, 40);

        // Act
        var needs = NodeRegenPlanner.NeedsRegen(decl, new NodeLimits(2_000_000_000L, 4L << 30));

        // Assert
        needs.Should().BeFalse();
    }

    [Fact]
    public void NeedsRegen_DecimalUnfriendlyCpuEqualByWriteArithmetic_False()
    {
        // Arrange — 1.15 ядер: запись даёт 1149999999 нано; сверка обязана
        // сойтись с фактом инспекта (тот же расчёт), а не с decimal-идеалом
        var decl = new BrokerResources(1.15m, 4, 40);

        // Act
        var needs = NodeRegenPlanner.NeedsRegen(decl, new NodeLimits(
            NodeRegenPlanner.ExpectedNanoCpus(1.15m), 4L << 30));

        // Assert
        needs.Should().BeFalse();
    }

    [Theory]
    [InlineData(1_000_000_000L, 4L << 30)] // cpu расходится
    [InlineData(2_000_000_000L, 2L << 30)] // mem расходится
    [InlineData(0L, 0L)]                    // контейнер без лимитов
    public void NeedsRegen_AnyDivergence_True(long nano, long mem)
    {
        // Arrange
        var decl = new BrokerResources(2m, 4, 40);

        // Act
        var needs = NodeRegenPlanner.NeedsRegen(decl, new NodeLimits(nano, mem));

        // Assert
        needs.Should().BeTrue();
    }
}
```

- [x] **Шаг 2. Прогон — убедиться, что падает**

Run: `dotnet test src/tests/KafkaWorker.UnitTests --filter NodeRegenPlanner`
Ожидание: FAIL — тип `NodeRegenPlanner` не существует (CS0103).

- [x] **Шаг 3. Минимальная реализация**
  (`src/KafkaWorker.Core/Planning/NodeRegenPlanner.cs`):

```csharp
using KafkaWorker.Core.Model;

namespace KafkaWorker.Core.Planning;

/// <summary>
/// Фактические лимиты контейнера/сервиса брокера из docker inspect
/// (t06, spec §5.3): 0 = лимит не задан.
/// </summary>
public sealed record NodeLimits(long NanoCpus, long MemoryBytes);

/// <summary>
/// Сверка лимитов контейнера с декларацией brokers/&lt;b&gt;/resources
/// (t06, spec §5.2 J2 / §5.3). Формула cpu ПОКАЗАТЕЛЬНО повторяет
/// арифметику ЗАПИСИ DockerEngine.BuildContainerBody: spec.CpuCores
/// (decimal) → KafkaNodeSpec.CpuCores (double) →
/// (long)(cores * 1_000_000_000). Каст в double ДО умножения обязателен:
/// decimal-арифметика (long)(cpu * 1e9m) для значений, непредставимых
/// точно в double (0.01, 1.15), расходится с фактом инспекта → вечный
/// цикл регенерации (ревью Фазы 4, замечание 4). mem — целые GiB, сдвиг
/// точен в обеих арифметиках. disk не сверяется (инфо-поле, квот нет).
/// </summary>
public static class NodeRegenPlanner
{
    public static long ExpectedNanoCpus(decimal cpu)
        => (long)((double)cpu * 1_000_000_000);

    public static long ExpectedMemoryBytes(int memGi)
        => (long)memGi * 1024 * 1024 * 1024;

    public static bool NeedsRegen(BrokerResources decl, NodeLimits actual)
        => actual.NanoCpus != ExpectedNanoCpus(decl.Cpu)
           || actual.MemoryBytes != ExpectedMemoryBytes(decl.MemGi);
}
```

- [x] **Шаг 4. Прогон — зелёный**

Run: `dotnet test src/tests/KafkaWorker.UnitTests --filter NodeRegenPlanner`
Ожидание: PASS (все кейсы, включая 1.15).

- [x] **Шаг 5. Коммит**

```bash
git add src/KafkaWorker.Core/Planning/NodeRegenPlanner.cs src/tests/KafkaWorker.UnitTests/Planning/NodeRegenPlannerTests.cs
git commit -m "feat(kafka): t06 — чистая сверка лимитов контейнера с декларацией (NodeRegenPlanner, арифметика записи)"
```

---

### Task 2: Docker-инспект лимитов (engine + драйверы + фейк)

**Вход:** Task 1 (`NodeLimits` в `KafkaWorker.Core.Planning`).
**Выход:** `IClusterDriver.NodeResourcesAsync` в обоих режимах + фейк для
unit-тестов; сборка 0 warnings.
**Spec:** §5.3 (inspect plain/swarm; null = объекта нет), §5.2 J2 (ошибка
инспекта → ошибка тика).

**Files:**
- Modify: `src/KafkaWorker.Docker/Engine/IDockerEngine.cs` (в конец интерфейса)
- Modify: `src/KafkaWorker.Docker/Engine/DockerEngine.cs` (реализации; рядом с
  `VolumeExistsAsync` ~строка 163)
- Modify: `src/KafkaWorker.Docker/Drivers/ClusterDriver.cs` (интерфейс
  `IClusterDriver` ~строка 31; `PlainClusterDriver`; `SwarmClusterDriver`)
- Modify: `src/tests/KafkaWorker.UnitTests/Provisioning/Fakes.cs`
  (`FakeKafkaDriver`)

**Interfaces (produces):**
- `IDockerEngine.InspectContainerResourcesAsync(string name, CancellationToken ct)`
  → `Task<Result<NodeLimits?>>` (404 → null).
- `IDockerEngine.InspectServiceResourcesAsync(string name, CancellationToken ct)`
  → `Task<Result<NodeLimits?>>` (404 → null).
- `IClusterDriver.NodeResourcesAsync(string cluster, string nodeName, CancellationToken ct)`
  → `Task<Result<NodeLimits?>>` (объекта нет ни на одном хосте → null).
- Фейк: `FakeKafkaDriver.Limits` (`Dictionary<string, NodeLimits>` — ключ
  `kfw-<C>-<b>`; EnsureNodeAsync поддерживает актуальность),
  `FakeKafkaDriver.ResourcesFaultByNode` (`Func<string, Result<NodeLimits?>>?`).

- [x] **Шаг 1. Реализация в `IDockerEngine.cs`** — добавить в интерфейс после
  `BusyPortsAsync`:

```csharp
    // Лимиты контейнера (HostConfig.NanoCPUs/Memory; 0 = без лимита); 404 → null.
    Task<Result<NodeLimits?>> InspectContainerResourcesAsync(string name, CancellationToken ct);

    // Лимиты swarm-сервиса (TaskTemplate.Resources.Limits); 404 → null.
    Task<Result<NodeLimits?>> InspectServiceResourcesAsync(string name, CancellationToken ct);
```

Добавить `using KafkaWorker.Core.Planning;` в шапку файла.

- [x] **Шаг 2. Реализации в `DockerEngine.cs`** — после `VolumeExistsAsync`
  (~строка 175); паттерн 404 → null — как в `VolumeExistsAsync`; чтение JSON —
  через существующий `GetAsync<T>`, который возвращает `Task<T?>`
  (`private async Task<T?> GetAsync<T>(...)`, DockerEngine.cs:616) — поэтому
  тело разворачивается из nullable с guard (ревью Фазы 4, замечание 6):

```csharp
    public async Task<Result<NodeLimits?>> InspectContainerResourcesAsync(string name, CancellationToken ct)
        => await Result<NodeLimits?>.FromAsync(async () =>
        {
            try
            {
                var body = await GetAsync<System.Text.Json.JsonElement>(
                    $"/containers/{Uri.EscapeDataString(name)}/json", ct);
                if (body is not { } json)
                    return null; // пустое тело — факта для сверки нет
                var host = json.GetProperty("HostConfig");
                return new NodeLimits(
                    host.TryGetProperty("NanoCPUs", out var nano) && nano.ValueKind == JsonValueKind.Number
                        ? nano.GetInt64()
                        : 0,
                    host.TryGetProperty("Memory", out var mem) && mem.ValueKind == JsonValueKind.Number
                        ? mem.GetInt64()
                        : 0);
            }
            catch (DockerHttpException e) when (e.StatusCode == 404)
            {
                return null; // контейнера нет — факта для сверки нет
            }
        });

    public async Task<Result<NodeLimits?>> InspectServiceResourcesAsync(string name, CancellationToken ct)
        => await Result<NodeLimits?>.FromAsync(async () =>
        {
            try
            {
                var body = await GetAsync<System.Text.Json.JsonElement>(
                    $"/services/{Uri.EscapeDataString(name)}", ct);
                if (body is not { } json)
                    return null; // пустое тело — факта для сверки нет
                var limits = json.GetProperty("Spec").GetProperty("TaskTemplate")
                    .GetProperty("Resources").GetProperty("Limits");
                return new NodeLimits(
                    limits.ValueKind == JsonValueKind.Object
                        && limits.TryGetProperty("NanoCPUs", out var nano)
                        && nano.ValueKind == JsonValueKind.Number
                        ? nano.GetInt64()
                        : 0,
                    limits.ValueKind == JsonValueKind.Object
                        && limits.TryGetProperty("MemoryBytes", out var mem)
                        && mem.ValueKind == JsonValueKind.Number
                        ? mem.GetInt64()
                        : 0);
            }
            catch (DockerHttpException e) when (e.StatusCode == 404)
            {
                return null; // сервиса нет — факта для сверки нет
            }
        });
```

(если `JsonValueKind` не в scope — полностью квалифицировать
`System.Text.Json.JsonValueKind`; стиль файла — минимальные using).

- [x] **Шаг 3. Метод интерфейса `IClusterDriver`** (`ClusterDriver.cs`, после
  `NodeVolumeExistsAsync`):

```csharp
    // Фактические лимиты контейнера/сервиса брокера (t06, spec §5.3): null =
    // объекта нет; ошибка инспекта → Failed (регенератор не решает вслепую).
    Task<Result<NodeLimits?>> NodeResourcesAsync(string cluster, string nodeName, CancellationToken ct);
```

- [x] **Шаг 4. `PlainClusterDriver.NodeResourcesAsync`** (после
  `NodeVolumeExistsAsync`):

```csharp
    // Перебор хостов: первый хост с контейнером отдаёт факт (симметрия Exec).
    public async Task<Result<NodeLimits?>> NodeResourcesAsync(string cluster, string nodeName, CancellationToken ct)
    {
        var name = NodeName(cluster, nodeName);
        foreach (var engine in _engines.Values)
        {
            var limits = await engine.InspectContainerResourcesAsync(name, ct);
            if (!limits.IsSuccess)
                return limits;
            if (limits.Value is not null)
                return limits;
        }

        return Result<NodeLimits?>.Success(null);
    }
```

- [x] **Шаг 5. `SwarmClusterDriver.NodeResourcesAsync`** (после его
  `NodeVolumeExistsAsync`; `NodeName` — internal-статик ПЛЕЙН-драйвера, в
  swarm-драйвере всегда вызывается с квалификацией `PlainClusterDriver.`
  — ревью Фазы 4, замечание 7):

```csharp
    public Task<Result<NodeLimits?>> NodeResourcesAsync(string cluster, string nodeName, CancellationToken ct)
        => _engine.InspectServiceResourcesAsync(PlainClusterDriver.NodeName(cluster, nodeName), ct);
```

- [x] **Шаг 6. Фейк** (`Fakes.cs`): в `FakeKafkaDriver` добавить поля и метод;

```csharp
        // Фактические лимиты kfw-<C>-<b> (t06): EnsureNodeAsync обновляет,
        // тесты сеют расхождение вручную (регенератор обязан сходиться).
        // Арифметика — как в записи: decimal→double→(long)(cores*1e9).
        public readonly Dictionary<string, NodeLimits> Limits = [];

        // Отказ инспекта конкретной ноды (ошибка тика — никаких действий).
        public Func<string, Result<NodeLimits?>>? ResourcesFaultByNode { get; set; }

        public Task<Result<NodeLimits?>> NodeResourcesAsync(string cluster, string nodeName, CancellationToken ct)
        {
            if (ResourcesFaultByNode is { } fault)
            {
                var result = fault(nodeName);
                if (!result.IsSuccess)
                    return Task.FromResult(result);
            }

            var name = $"kfw-{cluster}-{nodeName}";
            return Task.FromResult(NodeObjects.Contains(name)
                ? Result<NodeLimits?>.Success(Limits.GetValueOrDefault(name, new NodeLimits(0, 0)))
                : Result<NodeLimits?>.Success(null));
        }
```

и в `EnsureNodeAsync` (внутрь `lock (_gate)`, после `NodeObjects.Add(name)`)
добавить поддержку факта:

```csharp
                Limits[name] = new NodeLimits(
                    (long?)((double?)spec.CpuCores * 1_000_000_000) ?? 0,
                    spec.MemoryBytes ?? 0);
```

и в `RemoveNodeAsync` (внутрь `lock`, после `NodeObjects.Remove`) — чистка факта:

```csharp
                Limits.Remove($"kfw-{cluster}-{nodeName}");
```

Добавить `using KafkaWorker.Core.Planning;` в шапку `Fakes.cs`.

- [x] **Шаг 7. Сборка**

Run: `dotnet build src/PgWorker.slnx`
Ожидание: 0 errors, 0 warnings (реализации интерфейса во всех
реализациях/фейках: `PlainClusterDriver`, `SwarmClusterDriver`,
`FakeKafkaDriver`).

- [x] **Шаг 8. Прогон существующих unit-тестов (регресс фейка)**

Run: `dotnet test src/tests/KafkaWorker.UnitTests`
Ожидание: PASS (все; фейк обратно совместим).

- [x] **Шаг 9. Коммит**

```bash
git add src/KafkaWorker.Docker src/tests/KafkaWorker.UnitTests/Provisioning/Fakes.cs
git commit -m "feat(kafka): t06 — docker-инспект лимитов контейнера/сервиса (NodeResourcesAsync, оба режима, фейк)"
```

---

### Task 3: Мутация №15 — PUT resources (воркер)

**Вход:** Task 1; существующие хендлеры (`UpdateConfigHandler`,
`AddBrokerHandler`, `DeleteBrokerHandler`) как образцы.
**Выход:** `PUT /api/kafka/clusters/{c}/brokers/{b}/resources` — 200/400/404/
409/503, канонический JSON в etcd; unit + integration зелёные.
**Spec:** §4.2 (мутация №15: guard'ы, валидация, канонизация, идемпотентность),
§10.2-15 контракта adminpanel/02.

**Files:**
- Modify: `src/KafkaWorker.Core/Writing/KafkaWriting.cs` (в конец файла)
- Modify: `src/KafkaWorker.App/Api/Operations/KafkaExceptions.cs`
- Create: `src/KafkaWorker.App/Api/Operations/UpdateBrokerResourcesHandler.cs`
- Modify: `src/KafkaWorker.App/Api/ApiModule.cs` (маршрут после
  DELETE-брокера ~строки 149–172)
- Test: `src/tests/KafkaWorker.UnitTests/Writing/KafkaResourcesUpdateValidatorTests.cs`
- Test: `src/tests/KafkaWorker.IntegrationTests/Api/UpdateBrokerResourcesApiTests.cs`

**Interfaces (produces):**
- `public sealed record KafkaResourcesUpdateRequest(decimal? Cpu, int? MemGi, int? DiskGi)`
- `public sealed record KafkaResourcesUpdatePlan(decimal Cpu, int MemGi, int DiskGi)` со
  свойством `string CanonicalJson`
- `public static class KafkaResourcesUpdateValidator` со
  `static IReadOnlyList<ValidationError> Validate(KafkaResourcesUpdateRequest request, BrokerResources current)`
- `public sealed class KafkaBrokerRemovalInProgressException(string cluster, string broker)`
- `public sealed record KafkaBrokerResourcesDto(string Cluster, string Broker, string Cpu, string MemGi, string DiskGi)`
- `UpdateBrokerResourcesHandler(IEtcdGateway gateway, string[] endpoints)` со
  `Task<Result<KafkaBrokerResourcesDto>> HandleAsync(string cluster, string broker, KafkaResourcesUpdateRequest request, CancellationToken ct)`

Note: канонизация decimal — СУЩЕСТВУЮЩИЙ метод `KafkaClusterCreatePlan.Canonical`
(KafkaWriting.cs:199; публичного `KafkaLimits.Canonical` нет — ревью Фазы 4,
замечание 8).

- [x] **Шаг 1. Failing-тест валидатора**
  (`src/tests/KafkaWorker.UnitTests/Writing/KafkaResourcesUpdateValidatorTests.cs`):

```csharp
using FluentAssertions;
using KafkaWorker.Core.Model;
using KafkaWorker.Core.Writing;
using Xunit;

namespace KafkaWorker.UnitTests.Writing;

// Валидация мутации №15 (t06, spec §4.2): границы §10.3, null = не менять,
// хотя бы одно поле обязательно; эффективные значения = new ?? current.
public class KafkaResourcesUpdateValidatorTests
{
    private static readonly BrokerResources Current = new(2m, 4, 40);

    [Fact]
    public void Validate_AllFieldsNull_SingleError()
    {
        // Arrange
        var request = new KafkaResourcesUpdateRequest(null, null, null);

        // Act
        var errors = KafkaResourcesUpdateValidator.Validate(request, Current);

        // Assert
        errors.Should().ContainSingle().Which.Field.Should().Be("");
    }

    [Fact]
    public void Validate_PartialUpdate_EffectiveValuesInBounds_NoErrors()
    {
        // Arrange — меняется только cpu, mem/disk наследуются
        var request = new KafkaResourcesUpdateRequest(4m, null, null);

        // Act
        var errors = KafkaResourcesUpdateValidator.Validate(request, Current);

        // Assert
        errors.Should().BeEmpty();
    }

    [Fact]
    public void Validate_NewCpuOutOfBounds_Error()
    {
        // Arrange
        var request = new KafkaResourcesUpdateRequest(100m, null, null);

        // Act
        var errors = KafkaResourcesUpdateValidator.Validate(request, Current);

        // Assert
        errors.Should().Contain(e => e.Field == "cpu");
    }

    [Fact]
    public void Validate_NewMemInvalid_Error()
    {
        // Arrange — новый memGi вне границ
        var request = new KafkaResourcesUpdateRequest(null, 0, null);

        // Act
        var errors = KafkaResourcesUpdateValidator.Validate(request, Current);

        // Assert
        errors.Should().Contain(e => e.Field == "memGi");
    }

    [Fact]
    public void Validate_DiskDecreaseAllowed_NoErrors()
    {
        // Arrange — уменьшение разрешено (spec §3.5: риск OOM — оператор)
        var request = new KafkaResourcesUpdateRequest(1m, 2, 10);

        // Act
        var errors = KafkaResourcesUpdateValidator.Validate(request, Current);

        // Assert
        errors.Should().BeEmpty();
    }
}
```

- [x] **Шаг 2. Прогон — failing**

Run: `dotnet test src/tests/KafkaWorker.UnitTests --filter KafkaResourcesUpdateValidator`
Ожидание: FAIL (типы не существуют).

- [x] **Шаг 3. Реализация в `KafkaWriting.cs`** (в конец файла; рядом уже есть
  `ResourcesJson` и `KafkaClusterCreatePlan.Canonical`):

```csharp
// Запрос мутации №15 — изменение ресурсов существующего брокера (t06,
// adminpanel/02 §10.2-15): null = не менять; хотя бы одно поле обязательно.
public sealed record KafkaResourcesUpdateRequest(decimal? Cpu, int? MemGi, int? DiskGi);

// Эффективные ресурсы после применения частичного обновления + канонический
// JSON ключа brokers/<b>/resources (формат 1:1 — arch/15 §2). Канонизация —
// существующий KafkaClusterCreatePlan.Canonical (decimal → "0.#########").
public sealed record KafkaResourcesUpdatePlan(decimal Cpu, int MemGi, int DiskGi)
{
    public string CanonicalJson
        => $$"""{"cpu":"{{KafkaClusterCreatePlan.Canonical(Cpu)}}","mem":"{{MemGi}}Gi","disk":"{{DiskGi}}Gi"}""";
}

// Чистая функция валидации мутации №15: границы §10.3 на ЭФФЕКТИВНЫХ
// значениях (new ?? current) — уменьшение разрешено (spec §3.5).
public static class KafkaResourcesUpdateValidator
{
    public static IReadOnlyList<ValidationError> Validate(
        KafkaResourcesUpdateRequest request, BrokerResources current)
    {
        var errors = new List<ValidationError>();
        if (request.Cpu is null && request.MemGi is null && request.DiskGi is null)
            errors.Add(new("", "хотя бы одно поле обновления обязательно"));

        var cpu = request.Cpu ?? current.Cpu;
        if (cpu < KafkaLimits.MinCpu || cpu > KafkaLimits.MaxCpu)
            errors.Add(new("cpu", $"cpu: {KafkaLimits.MinCpu}..{KafkaLimits.MaxCpu} ядер"));
        var memGi = request.MemGi ?? current.MemGi;
        if (memGi is < KafkaLimits.MinGiB or > KafkaLimits.MaxGiB)
            errors.Add(new("memGi", $"memGi: целое {KafkaLimits.MinGiB}..{KafkaLimits.MaxGiB} GiB"));
        var diskGi = request.DiskGi ?? current.DiskGi;
        if (diskGi is < KafkaLimits.MinGiB or > KafkaLimits.MaxGiB)
            errors.Add(new("diskGi", $"diskGi: целое {KafkaLimits.MinGiB}..{KafkaLimits.MaxGiB} GiB"));
        return errors;
    }
}

// Чтение ключа resources (мутация №15): {"cpu":"2","mem":"4Gi","disk":"40Gi"}.
public static class BrokerResourcesJson
{
    public static BrokerResources? TryParse(string json)
    {
        try
        {
            using var doc = System.Text.Json.JsonDocument.Parse(json);
            var root = doc.RootElement;
            var cpu = root.GetProperty("cpu").GetString()!;
            var mem = root.GetProperty("mem").GetString()!;
            var disk = root.GetProperty("disk").GetString()!;
            return new BrokerResources(
                decimal.Parse(cpu.TrimEnd('G', 'i'), System.Globalization.CultureInfo.InvariantCulture),
                int.Parse(mem.TrimEnd('G', 'i'), System.Globalization.CultureInfo.InvariantCulture),
                int.Parse(disk.TrimEnd('G', 'i'), System.Globalization.CultureInfo.InvariantCulture));
        }
        catch (Exception e) when (e is System.Text.Json.JsonException or FormatException or KeyNotFoundException)
        {
            return null; // битый JSON — мутация невозможна, 503 (InvalidKafkaConfigException-ветка)
        }
    }
}
```

- [x] **Шаг 4. Прогон валидатора — зелёный**

Run: `dotnet test src/tests/KafkaWorker.UnitTests --filter KafkaResourcesUpdateValidator`
Ожидание: PASS.

- [x] **Шаг 5. Исключение** (`KafkaExceptions.cs`, рядом с
   `KafkaBrokerIsControllerException`):

```csharp
/// <summary>Мутация №15: брокер в демонтаже — ресурсы менять незачем (t06, 02 §10.2-15).</summary>
public sealed class KafkaBrokerRemovalInProgressException(string cluster, string broker)
    : Exception($"брокер {cluster}/{broker} заявлен к удалению (TO_REMOVE/REMOVING) — изменение ресурсов отклонено");
```

- [x] **Шаг 6. Хендлер** (`src/KafkaWorker.App/Api/Operations/UpdateBrokerResourcesHandler.cs`):

```csharp
using System.Text.RegularExpressions;
using KafkaWorker.Core;
using KafkaWorker.Core.Writing;
using KafkaWorker.Etcd.Client;

namespace KafkaWorker.App.Api.Operations;

// Ответ 200 PUT /api/kafka/clusters/{c}/brokers/{b}/resources (t06, spec §4.2).
public sealed record KafkaBrokerResourcesDto(
    string Cluster, string Broker, string Cpu, string MemGi, string DiskGi);

// Изменение ресурсов существующего брокера — мутация №15 (t06, adminpanel/02
// §10.2-15): guard'ы по прямым чтениям etcd → канонизация → put ключа целиком.
// Применение — автоматическое: NodeRegenerator воркера (arch/16 §5 J) сверяет
// лимиты живого контейнера и rolling-ит по одному за тик; disk — инфо-поле.
// Порт DeleteBrokerHandler (guard'ы) + UpdateConfigHandler (DTO-ответ).
public sealed partial class UpdateBrokerResourcesHandler(IEtcdGateway gateway, string[] endpoints)
{
    // Имя брокера каноническое (иначе 404 — воркер такие не создаёт).
    [GeneratedRegex("^broker[1-9]$")]
    private static partial Regex BrokerPattern();

    public async Task<Result<KafkaBrokerResourcesDto>> HandleAsync(
        string cluster, string broker, KafkaResourcesUpdateRequest request, CancellationToken ct)
    {
        if (!KafkaLimits.ClusterPattern().IsMatch(cluster))
            return Result<KafkaBrokerResourcesDto>.Failed(new KafkaClusterNotFoundException(cluster));
        if (!BrokerPattern().IsMatch(broker))
            return Result<KafkaBrokerResourcesDto>.Failed(new KafkaBrokerNotFoundException(cluster, broker));

        var config = await KafkaApiHelpers.ReadConfigAsync(gateway, endpoints, cluster, ct);
        if (config.Error is not null)
            return Result<KafkaBrokerResourcesDto>.Failed(config.Error);
        if (config.Value is null)
            return Result<KafkaBrokerResourcesDto>.Failed(new KafkaClusterNotFoundException(cluster));
        if (config.Value.State is not null)
            return Result<KafkaBrokerResourcesDto>.Failed(
                new KafkaClusterNotActiveException(cluster, config.Value.State));

        // Текущая декларация ресурсов (эффективные значения = new ?? current).
        var resourcesKey = KafkaApiHelpers.BrokerKey(cluster, broker, "resources");
        var currentJson = await KafkaApiHelpers.ReadKeyAsync(gateway, endpoints, resourcesKey, ct);
        if (!currentJson.IsSuccess)
            return Result<KafkaBrokerResourcesDto>.Failed(currentJson.Error!);
        if (currentJson.Value is null)
            return Result<KafkaBrokerResourcesDto>.Failed(new KafkaBrokerNotFoundException(cluster, broker));
        var current = BrokerResourcesJson.TryParse(currentJson.Value);
        if (current is null)
            return Result<KafkaBrokerResourcesDto>.Failed(new InvalidKafkaConfigException(cluster));

        // Брокер в демонтаже — ресурсы менять незачем (409).
        var state = await KafkaApiHelpers.ReadKeyAsync(
            gateway, endpoints, KafkaApiHelpers.BrokerKey(cluster, broker, "state"), ct);
        if (!state.IsSuccess)
            return Result<KafkaBrokerResourcesDto>.Failed(state.Error!);
        if (state.Value is "TO_REMOVE" or "REMOVING")
            return Result<KafkaBrokerResourcesDto>.Failed(
                new KafkaBrokerRemovalInProgressException(cluster, broker));

        // Валидация §10.3 на эффективных значениях.
        var errors = KafkaResourcesUpdateValidator.Validate(request, current);
        if (errors.Count > 0)
            return Result<KafkaBrokerResourcesDto>.Failed(new KafkaValidationException(errors));

        // Каноническая перезапись целиком (ключ плоский — RMW не нужен);
        // идемпотентность: повтор — та же запись.
        var plan = new KafkaResourcesUpdatePlan(
            request.Cpu ?? current.Cpu, request.MemGi ?? current.MemGi, request.DiskGi ?? current.DiskGi);
        var put = await EtcdFailover.CallAsync(endpoints, endpoint => gateway.PutAsync(
            endpoint, resourcesKey, plan.CanonicalJson, null, ct));
        if (!put.IsSuccess)
            return Result<KafkaBrokerResourcesDto>.Failed(put.Error!);

        return Result<KafkaBrokerResourcesDto>.Success(new KafkaBrokerResourcesDto(
            cluster, broker,
            KafkaClusterCreatePlan.Canonical(plan.Cpu), $"{plan.MemGi}Gi", $"{plan.DiskGi}Gi"));
    }
}
```

- [x] **Шаг 7. Маршрут** (`ApiModule.cs`, после DELETE-брокера; маппинг —
  порт существующих веток; `UpdateBrokerResourcesHandler` добавить в параметры
  lambda как остальные хендлеры):

```csharp
        // PUT /api/kafka/clusters/{cluster}/brokers/{broker}/resources — мутация
        // №15 (t06, 02 §10.2-15): декларация ресурсов; применяет NodeRegenerator
        // воркера rolling-пересозданием (автоконверге, без заявки). Идемпотентен.
        endpoints.MapPut("/api/kafka/clusters/{cluster}/brokers/{broker}/resources", async (
            string cluster, string broker, KafkaResourcesUpdateRequest request,
            UpdateBrokerResourcesHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(cluster, broker, request, ct);
            if (result.IsSuccess)
                return Results.Ok(result.Value);

            return result.Error switch
            {
                KafkaValidationException validation => Results.Problem(
                    statusCode: StatusCodes.Status400BadRequest,
                    title: "Validation failed",
                    detail: result.Error.Message,
                    extensions: new Dictionary<string, object?>
                    {
                        ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }),
                    }),
                KafkaClusterNotFoundException or KafkaBrokerNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Not found",
                    detail: result.Error.Message),
                KafkaClusterNotActiveException or KafkaBrokerRemovalInProgressException => Results.Problem(
                    statusCode: StatusCodes.Status409Conflict, title: "Broker resources update rejected",
                    detail: result.Error.Message),
                EtcdWriteUnavailableException or InvalidKafkaConfigException => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable",
                    detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                    detail: result.Error!.Message),
            };
        });
```

Плюс: DI-регистрация в `Program.cs` рядом с `UpdateConfigHandler`
(~строка 65):

```csharp
builder.Services.AddSingleton(sp => new UpdateBrokerResourcesHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints));
```

- [x] **Шаг 8. Failing-интеграционные тесты**
  (`src/tests/KafkaWorker.IntegrationTests/Api/UpdateBrokerResourcesApiTests.cs`;
  фикстура `KafkaApiCollection`/`KafkaApiTestSeed.SeedActiveClusterAsync`):

```csharp
using System.Net;
using System.Net.Http.Json;
using KafkaWorker.Core.Writing;
using Xunit;

namespace KafkaWorker.IntegrationTests.Api;

// Мутация №15 (t06, spec §4.2): guard'ы/канонизация/идемпотентность на
// настоящем etcd (loops выключены — применяется NodeRegenerator'ом отдельно).
[Collection(KafkaApiCollection.Name)]
public class UpdateBrokerResourcesApiTests(KafkaApiFixture fixture)
{
    private static async Task<string?> GetValueAsync(KafkaApiFixture fixture, string key)
    {
        var ct = TestContext.Current.CancellationToken;
        var kv = await fixture.Etcd.Gateway.GetAsync(fixture.Etcd.Endpoint, key, ct);
        return kv.Value?.Value;
    }

    private static string Unique() => $"res{Guid.NewGuid():N}"[..8];

    [Fact]
    public async Task Update_PartialCpu_WritesCanonicalAndReturnsEffective()
    {
        // Arrange — сид: broker1 {"cpu":"2","mem":"4Gi","disk":"40Gi"}
        var cluster = Unique();
        await KafkaApiTestSeed.SeedActiveClusterAsync(fixture.Etcd, cluster);
        var client = fixture.Factory.CreateClient();

        // Act — меняем только cpu
        var response = await client.PutAsJsonAsync(
            $"/api/kafka/clusters/{cluster}/brokers/broker1/resources",
            new KafkaResourcesUpdateRequest(4m, null, null));

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var dto = await response.Content.ReadFromJsonAsync<KafkaBrokerResourcesDto>();
        dto!.Cpu.Should().Be("4");
        dto.MemGi.Should().Be("4Gi"); // унаследовано
        (await GetValueAsync(fixture, $"/kafka/clusters/{cluster}/brokers/broker1/resources"))
            .Should().Be("""{"cpu":"4","mem":"4Gi","disk":"40Gi"}""");
    }

    [Fact]
    public async Task Update_IdempotentRepeat_SameKeyAnd200()
    {
        // Arrange
        var cluster = Unique();
        await KafkaApiTestSeed.SeedActiveClusterAsync(fixture.Etcd, cluster);
        var client = fixture.Factory.CreateClient();

        // Act
        await client.PutAsJsonAsync(
            $"/api/kafka/clusters/{cluster}/brokers/broker1/resources",
            new KafkaResourcesUpdateRequest(null, 8, null));
        var second = await client.PutAsJsonAsync(
            $"/api/kafka/clusters/{cluster}/brokers/broker1/resources",
            new KafkaResourcesUpdateRequest(null, 8, null));

        // Assert
        second.StatusCode.Should().Be(HttpStatusCode.OK);
        (await GetValueAsync(fixture, $"/kafka/clusters/{cluster}/brokers/broker1/resources"))
            .Should().Be("""{"cpu":"2","mem":"8Gi","disk":"40Gi"}""");
    }

    [Fact]
    public async Task Update_OutOfBoundsCpu_400WithErrors()
    {
        // Arrange
        var cluster = Unique();
        await KafkaApiTestSeed.SeedActiveClusterAsync(fixture.Etcd, cluster);
        var client = fixture.Factory.CreateClient();

        // Act
        var response = await client.PutAsJsonAsync(
            $"/api/kafka/clusters/{cluster}/brokers/broker1/resources",
            new KafkaResourcesUpdateRequest(100m, null, null));

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await response.Content.ReadAsStringAsync()).Should().Contain("cpu");
    }

    [Fact]
    public async Task Update_EmptyBody_400()
    {
        // Arrange
        var cluster = Unique();
        await KafkaApiTestSeed.SeedActiveClusterAsync(fixture.Etcd, cluster);
        var client = fixture.Factory.CreateClient();

        // Act
        var response = await client.PutAsJsonAsync(
            $"/api/kafka/clusters/{cluster}/brokers/broker1/resources",
            new KafkaResourcesUpdateRequest(null, null, null));

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Update_UnknownCluster_404()
    {
        // Arrange
        var client = fixture.Factory.CreateClient();

        // Act
        var response = await client.PutAsJsonAsync(
            "/api/kafka/clusters/ghost/brokers/broker1/resources",
            new KafkaResourcesUpdateRequest(4m, null, null));

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Update_UnknownBroker_404()
    {
        // Arrange — broker9 отсутствует в сиде (кластер из 3 брокеров)
        var cluster = Unique();
        await KafkaApiTestSeed.SeedActiveClusterAsync(fixture.Etcd, cluster);
        var client = fixture.Factory.CreateClient();

        // Act
        var response = await client.PutAsJsonAsync(
            $"/api/kafka/clusters/{cluster}/brokers/broker9/resources",
            new KafkaResourcesUpdateRequest(4m, null, null));

        // Assert — 404 «брокер»: нет ключа brokers/broker9/resources
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Update_BrokerInRemoval_409()
    {
        // Arrange — брокер заявлен к демонтажу
        var cluster = Unique();
        await KafkaApiTestSeed.SeedActiveClusterAsync(fixture.Etcd, cluster);
        var ct = TestContext.Current.CancellationToken;
        await fixture.Etcd.Gateway.PutAsync(fixture.Etcd.Endpoint,
            $"/kafka/clusters/{cluster}/brokers/broker3/state", "TO_REMOVE", null, ct);
        var client = fixture.Factory.CreateClient();

        // Act
        var response = await client.PutAsJsonAsync(
            $"/api/kafka/clusters/{cluster}/brokers/broker3/resources",
            new KafkaResourcesUpdateRequest(4m, null, null));

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task Update_NotActiveCluster_409()
    {
        // Arrange — config с state=TO_REMOVE
        var cluster = Unique();
        await KafkaApiTestSeed.SeedActiveClusterAsync(fixture.Etcd, cluster);
        var ct = TestContext.Current.CancellationToken;
        await fixture.Etcd.Gateway.PutAsync(fixture.Etcd.Endpoint,
            $"/kafka/clusters/{cluster}/config",
            """{"brokers":3,"replication_factor":3,"min_insync_replicas":2,"default_partitions":12,"default_retention_ms":604800000,"created_unix":1756500000,"state":"TO_REMOVE"}""",
            null, ct);
        var client = fixture.Factory.CreateClient();

        // Act
        var response = await client.PutAsJsonAsync(
            $"/api/kafka/clusters/{cluster}/brokers/broker1/resources",
            new KafkaResourcesUpdateRequest(4m, null, null));

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }
}
```

- [x] **Шаг 9. Прогон — сначала failing, затем реализация уже из шагов выше**

Run: `dotnet test src/tests/KafkaWorker.IntegrationTests --filter UpdateBrokerResources`
Ожидание: до шагов 5–7 — FAIL (404 маршрута); после — PASS (8 кейсов).
Требуется Docker (только etcd-контейнер фикстуры).

- [x] **Шаг 10. Полная сборка + unit-прогон**

Run: `dotnet build src/PgWorker.slnx && dotnet test src/tests/KafkaWorker.UnitTests`
Ожидание: 0 warnings; unit PASS.

- [x] **Шаг 11. Коммит**

```bash
git add src/KafkaWorker.Core/Writing/KafkaWriting.cs src/KafkaWorker.App/Api src/tests/KafkaWorker.UnitTests/Writing src/tests/KafkaWorker.IntegrationTests/Api/UpdateBrokerResourcesApiTests.cs src/KafkaWorker.App/Program.cs
git commit -m "feat(kafka): t06 — мутация №15 PUT resources брокера (валидатор, хендлер, маршрут, тесты)"
```

---

### Task 4: Процесс J — NodeRegenerator (конвейер, X2, DI)

**Вход:** Tasks 1–2 (`NodeLimits`, `NodeResourcesAsync`, фейк), Task 3
(декларация меняется мутацией №15).
**Выход:** процесс J в Active-ветке (после ротации, перед TopicSync),
X2-очистка regens-ключа (с тестом), DI; unit-тесты фаз зелёные.
**Spec:** §5.2 (J0–J5: guard'ы, один за тик, прогресс-ключ ТОЛЬКО при живой
операции), §4.1/§4.3 (формат ключа; put при старте первого пересоздания —
отсутствие = операции нет), §5.4 (без снапшотов), §5.5 (конвейер/X2),
arch/16 §5 J; §10.5 (X2).

**Files:**
- Create: `src/KafkaWorker.Provisioning/Processes/NodeRegenerator.cs`
- Modify: `src/KafkaWorker.App/Loops/KafkaClusterProcesses.cs` (интерфейс +
  реализация + конструктор)
- Modify: `src/KafkaWorker.Provisioning/Processes/DeprovisioningProcess.cs`
  (X2: del `/kafkaworker/regens/<C>`)
- Modify: `src/KafkaWorker.App/Program.cs` (DI)
- Test: `src/tests/KafkaWorker.UnitTests/Provisioning/NodeRegeneratorTests.cs`
- Test: `src/tests/KafkaWorker.UnitTests/Provisioning/DeprovisioningProcessTests.cs`
  (расширить `Run_RemovesRebalanceKeys`)

**Interfaces (produces):**
- `public sealed class NodeRegenerator(IEtcdGateway etcd, string[] endpoints, IClusterDriver driver, ClaimStore claims, WorkJournal journal, ProvisioningOptions options)`
  со `Task<Result> RunAsync(KafkaClusterSnapshot snap, CancellationToken ct)`.
- `IKafkaClusterProcesses.ActiveAsync` дополнен вызовом `regenerator.RunAsync`
  (между ротацией и TopicSync) — реализация `KafkaClusterProcesses` принимает
  `NodeRegenerator regenerator` в конструкторе.

- [x] **Шаг 1. Failing-тесты процесса**
  (`src/tests/KafkaWorker.UnitTests/Provisioning/NodeRegeneratorTests.cs`;
  фейки `Fakes.FakeEtcd`/`Fakes.FakeKafkaDriver`, снапшот-сид как в
  `NodeSupervisorTests`; `FixedTimeProvider` инициализируется свойством
  `Utc` — конструктора с аргументом нет, ревью Фазы 4 раунд 2, замечание 2):

```csharp
using System.Text.Json;
using FluentAssertions;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Core.Planning;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Etcd.Parsing;
using KafkaWorker.Provisioning;
using KafkaWorker.Provisioning.Processes;
using Xunit;
using static KafkaWorker.UnitTests.Provisioning.Fakes;

namespace KafkaWorker.UnitTests.Provisioning;

// Процесс J (t06, spec §5.2): автоконверге лимитов — один брокер за тик,
// guard'ы передержки, прогресс-ключ /kafkaworker/regens/<C> ТОЛЬКО при живой
// операции (чужие недоведённые ноды фантома не рисуют), del по сходимости.
public class NodeRegeneratorTests : IAsyncLifetime
{
    private readonly FakeEtcd _etcd = new();
    private readonly FakeKafkaDriver _driver = new();
    private readonly FixedTimeProvider _time = new()
    {
        Utc = new DateTimeOffset(2026, 9, 2, 12, 0, 0, TimeSpan.Zero),
    };
    private readonly ClaimStore _claims;
    private readonly WorkJournal _journal;
    private readonly NodeRegenerator _regen;
    private const string Cluster = "events";

    public NodeRegeneratorTests()
    {
        _etcd.Seed($"/kafka/clusters/{Cluster}/config",
            """{"brokers":2,"replication_factor":2,"min_insync_replicas":1,"default_partitions":3,"default_retention_ms":604800000,"created_unix":1756500000}""");
        for (var k = 1; k <= 2; k++)
        {
            _etcd.Seed($"/kafka/clusters/{Cluster}/brokers/broker{k}/state", "RUNNING");
            _etcd.Seed($"/kafka/clusters/{Cluster}/brokers/broker{k}/role", k == 1 ? "controller" : "broker");
            _etcd.Seed($"/kafka/clusters/{Cluster}/brokers/broker{k}/resources", """{"cpu":"2","mem":"4Gi","disk":"40Gi"}""");
        }

        _etcd.Seed($"/kafka/clusters/{Cluster}/app_user", "app");
        _etcd.Seed($"/kafka/clusters/{Cluster}/app_password", "p1");
        _etcd.Seed($"/kafkaworker/portalloc/{Cluster}",
            """{"broker1":{"host":"h1","client":16001},"broker2":{"host":"h1","client":16002}}""");
        _driver.NodeObjects.AddRange(["kfw-events-broker1", "kfw-events-broker2"]);
        // Факт: broker1 разошёлся (cpu 1 против 2), broker2 сходится.
        // Арифметика — как в записи/сверке: (long)((double)1 * 1e9).
        _driver.Limits["kfw-events-broker1"] = new(1_000_000_000L, 4L << 30);
        _driver.Limits["kfw-events-broker2"] = new(2_000_000_000L, 4L << 30);

        var endpoints = new[] { "http://etcd" };
        _claims = new ClaimStore(endpoints, _etcd, _time);
        _journal = new WorkJournal(_etcd, endpoints);
        _regen = new NodeRegenerator(_etcd, endpoints, _driver, _claims, _journal,
            new ProvisioningOptions(16000, 16999, BrokerBootSec: 100, NodeDeadSec: 90, null, "apache/kafka:4.0.0"));
    }

    // Клэйм держит _claims (владелец _regen); «чужие» инстансы строятся
    // отдельным ClaimStore (см. RunAsync_NoClaim_Fails).
    public ValueTask InitializeAsync()
    {
        TestContext.Current.CancellationToken.ThrowIfCancellationRequested();
        return _claims.TryClaimClusterAsync(Cluster, TestContext.Current.CancellationToken);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private async Task<KafkaClusterSnapshot> SnapshotAsync()
    {
        var range = await _etcd.RangeAsync("http://etcd", "/kafka/clusters/", CancellationToken.None);
        return KafkaSnapshotParser.Parse(range.Value).Value.Single(c => c.Cluster == Cluster);
    }

    private Task<string?> KeyAsync(string key)
        => _etcd.GetAsync("http://etcd", key, CancellationToken.None)
            .ContinueWith(t => t.Result.Value?.Value);

    private NodeRegenerator Stranger()
        => new(_etcd, ["http://etcd"], _driver,
            new ClaimStore(["http://etcd"], _etcd, _time), _journal,
            new ProvisioningOptions(16000, 16999, BrokerBootSec: 100, NodeDeadSec: 90, null, "apache/kafka:4.0.0"));

    [Fact]
    public async Task RunAsync_LimitsDiverged_RecreatesOneBrokerPerTick()
    {
        // Arrange
        var snap = await SnapshotAsync();

        // Act
        var result = await _regen.RunAsync(snap, CancellationToken.None);

        // Assert — ровно один рестарт за тик (брокер1 — первый по имени),
        // том сохранён, state=PROVISIONING, прогресс-ключ поставлен.
        result.IsSuccess.Should().BeTrue();
        _driver.Removed.Should().ContainSingle().Which.Should().Be(("broker1", false));
        _driver.AllEnsured.Should().ContainSingle(e => e.NodeName == "broker1");
        (await KeyAsync($"/kafka/clusters/{Cluster}/brokers/broker1/state")).Should().Be("PROVISIONING");
        var progress = await KeyAsync($"/kafkaworker/regens/{Cluster}");
        progress.Should().NotBeNull();
        using var doc = JsonDocument.Parse(progress!);
        doc.RootElement.GetProperty("brokers_total").GetInt32().Should().Be(1);
        doc.RootElement.GetProperty("brokers_remaining").GetInt32().Should().Be(1);
        doc.RootElement.GetProperty("current_broker").GetString().Should().Be("broker1");
    }

    [Fact]
    public async Task RunAsync_NotRunningBrokerExists_WaitsWithoutRecreate()
    {
        // Arrange — broker2 ещё PROVISIONING (возврат — зона AddBrokerProcess)
        // при ЖИВОЙ операции (broker1 разошёлся) — передержка с прогрессом
        _etcd.Seed($"/kafka/clusters/{Cluster}/brokers/broker2/state", "PROVISIONING");
        var snap = await SnapshotAsync();

        // Act
        var result = await _regen.RunAsync(snap, CancellationToken.None);

        // Assert — передержка: никаких пересозданий, журнал waiting-return,
        // прогресс-ключ жив (операция идёт — remaining пересчитан)
        result.IsSuccess.Should().BeTrue();
        _driver.Removed.Should().BeEmpty();
        var state = await _journal.ReadAsync(Cluster, CancellationToken.None);
        state.Value!.Phase.Should().Be("waiting-return");
        (await KeyAsync($"/kafkaworker/regens/{Cluster}")).Should().NotBeNull();
    }

    [Fact]
    public async Task RunAsync_ForeignNotRunningBroker_NoPhantomProgressKey()
    {
        // Arrange — broker2 PROVISIONING (чужой add-broker/надзор),
        // расхождений НЕТ и операции не было — ключа тоже нет
        _driver.Limits["kfw-events-broker1"] = new(2_000_000_000L, 4L << 30); // сходится
        _etcd.Seed($"/kafka/clusters/{Cluster}/brokers/broker2/state", "PROVISIONING");
        var snap = await SnapshotAsync();

        // Act
        var result = await _regen.RunAsync(snap, CancellationToken.None);

        // Assert — фантомный прогресс запрещён (spec §4.1: put при старте
        // первого пересоздания; отсутствие ключа = операции нет — ревью
        // Фазы 4 раунд 2, замечание 1): no-op, ключ не появляется
        result.IsSuccess.Should().BeTrue();
        _driver.Removed.Should().BeEmpty();
        (await KeyAsync($"/kafkaworker/regens/{Cluster}")).Should().BeNull();
    }

    [Fact]
    public async Task RunAsync_LiveRotation_WaitsWithoutRecreate()
    {
        // Arrange — живая заявка ротации (фазы A–B)
        _etcd.Seed($"/kafkaworker/rotations/{Cluster}", """{"requested_unix":1756500000,"requested_by":"admin"}""");
        var snap = await SnapshotAsync();

        // Act
        var result = await _regen.RunAsync(snap, CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        _driver.Removed.Should().BeEmpty();
        var state = await _journal.ReadAsync(Cluster, CancellationToken.None);
        state.Value!.Phase.Should().Be("waiting-rotation");
    }

    [Fact]
    public async Task RunAsync_LiveReassignment_WaitsWithoutRecreate()
    {
        // Arrange — живой прогресс reassignment
        _etcd.Seed($"/kafkaworker/reassignments/{Cluster}",
            """{"mode":"drain","drain_broker":"broker2","partitions_total":3,"partitions_remaining":2,"submitted_unix":1756500000,"updated_unix":1756500000,"instance":"i1"}""");
        var snap = await SnapshotAsync();

        // Act
        var result = await _regen.RunAsync(snap, CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        _driver.Removed.Should().BeEmpty();
        var state = await _journal.ReadAsync(Cluster, CancellationToken.None);
        state.Value!.Phase.Should().Be("waiting-reassign");
    }

    [Fact]
    public async Task RunAsync_InspectFails_FailsTickWithoutRecreate()
    {
        // Arrange — слепой docker: сверка невозможна → никаких действий
        _driver.ResourcesFaultByNode = _ => Result<NodeLimits?>.Failed(
            new ApplicationException("docker: connection refused"));
        var snap = await SnapshotAsync();

        // Act
        var result = await _regen.RunAsync(snap, CancellationToken.None);

        // Assert — ошибка тика (следующий тик повторит), брокеры не тронуты
        result.IsSuccess.Should().BeFalse();
        _driver.Removed.Should().BeEmpty();
    }

    [Fact]
    public async Task RunAsync_EnsureFailsOnce_NextTickRecoversWithoutHarm()
    {
        // Arrange — сбой между Remove и Ensure (spec §5.4: идемпотентность)
        var snap = await SnapshotAsync();
        _driver.EnsureResultByNode = _ => Result.Failed(new ApplicationException("docker: create failed"));

        // Act — первый тик падает на ensure; второй (docker «ожил») проходит
        var first = await _regen.RunAsync(snap, CancellationToken.None);
        _driver.EnsureResultByNode = null;
        var second = await _regen.RunAsync(await SnapshotAsync(), CancellationToken.None);

        // Assert — первый Failed; второй сходится безопасно: контейнера broker1
        // нет (Remove сработал) → инспект null → пропуск, никаких повторных
        // Remove/Ensure (контейнер восстановит надзор; state сойдётся по факту)
        first.IsSuccess.Should().BeFalse();
        second.IsSuccess.Should().BeTrue();
        _driver.Removed.Should().ContainSingle(); // ровно один Remove за два тика
    }

    [Fact]
    public async Task RunAsync_AllConverged_DropsProgressKey()
    {
        // Arrange — расхождений нет, но прогресс-ключ висит (последний рестарт)
        _etcd.Seed($"/kafkaworker/regens/{Cluster}",
            """{"brokers_total":1,"brokers_remaining":1,"current_broker":"broker1","updated_unix":1756500000,"instance":"i1"}""");
        var snap = await SnapshotAsync();

        // Act
        var result = await _regen.RunAsync(snap, CancellationToken.None);

        // Assert — сходимость: ключ удалён, журнал done
        result.IsSuccess.Should().BeTrue();
        (await KeyAsync($"/kafkaworker/regens/{Cluster}")).Should().BeNull();
        var state = await _journal.ReadAsync(Cluster, CancellationToken.None);
        state.Value!.Phase.Should().Be("done");
    }

    [Fact]
    public async Task RunAsync_MissingContainer_SkipsNode()
    {
        // Arrange — контейнера broker1 нет (надзор восстановит) — не кандидат
        _driver.NodeObjects.Remove("kfw-events-broker1");
        var snap = await SnapshotAsync();

        // Act
        var result = await _regen.RunAsync(snap, CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        _driver.Removed.Should().BeEmpty();
    }

    [Fact]
    public async Task RunAsync_NoClaim_Fails()
    {
        // Arrange — «чужой» инстанс: свой ClaimStore НЕ держит клэйм кластера
        // (клэйм захвачен _claims в InitializeAsync и FakeEtcd-txn не отдаст
        // его повторным TryClaim — поэтому чужой строится без захвата;
        // ревью Фазы 4, замечание 3)
        var stranger = Stranger();
        var snap = await SnapshotAsync();

        // Act
        var result = await stranger.RunAsync(snap, CancellationToken.None);

        // Assert — клэйм не наш: мутации запрещены, брокеры не тронуты
        result.IsSuccess.Should().BeFalse();
        _driver.Removed.Should().BeEmpty();
    }
}
```

- [x] **Шаг 2. Прогон — failing**

Run: `dotnet test src/tests/KafkaWorker.UnitTests --filter NodeRegenerator`
Ожидание: FAIL — класс `NodeRegenerator` не существует.

- [x] **Шаг 3. Реализация процесса**
  (`src/KafkaWorker.Provisioning/Processes/NodeRegenerator.cs`):

```csharp
using System.Text.Json;
using System.Text.Json.Serialization;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Core.Planning;
using KafkaWorker.Docker.Drivers;
using KafkaWorker.Etcd.Client;
using KafkaWorker.Etcd.Coordination;

namespace KafkaWorker.Provisioning.Processes;

// Прогресс /kafkaworker/regens/<C> (t06, arch/15 §4): live-ключ — живёт
// только во время операции (put при старте первого пересоздания, del по
// сходимости; отсутствие ключа = операции нет — фантомы запрещены).
internal sealed record KafkaRegenProgressJson(
    [property: JsonPropertyName("brokers_total")] int BrokersTotal,
    [property: JsonPropertyName("brokers_remaining")] int BrokersRemaining,
    [property: JsonPropertyName("current_broker")] string? CurrentBroker,
    [property: JsonPropertyName("updated_unix")] long UpdatedUnix,
    [property: JsonPropertyName("instance")] string Instance,
    [property: JsonPropertyName("last_error")] string? LastError = null);

/// <summary>
/// NodeRegenerator (arch/16 §5 J, t06): автоконверге лимитов контейнера к
/// декларации brokers/&lt;b&gt;/resources. Триггер — ТОЛЬКО расхождение cpu/mem
/// (inspect vs декларация — NodeRegenPlanner, арифметика записи; env
/// пересобирается попутно, disk не сверяется). Один брокер за тик:
/// Remove(том жив) → Ensure(лимиты из декларации) → state=PROVISIONING;
/// возврат в RUNNING — AddBrokerProcess (F) следующих тиков; следующий
/// брокер — только когда все ноды RUNNING. Прогресс-ключ ставится/держится
/// ТОЛЬКО при живой операции (расхождения есть ИЛИ хвост: ключ жив, а
/// последний пересозданный ещё не RUNNING) — чужие недоведённые ноды
/// (add-broker F, надзор C) фантомного прогресса не создают (spec §4.1;
/// ревью Фазы 4 раунд 2, замечание 1). Guard'ы: живая ротация/reassignment —
/// передержка; ошибка инспекта — ошибка тика без действий (порт слепоты
/// надзора). Без снапшотов P12 (spec §5.4). Вызывается только держателем
/// клэйма &lt;C&gt;.
/// </summary>
public sealed class NodeRegenerator(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    ProvisioningOptions options)
{
    private const string Op = "regen";

    public async Task<Result> RunAsync(KafkaClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;
        if (!claims.IsMine(cluster))
            return Result.Failed(new ApplicationException(
                $"regen {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        // J0a: живая заявка ротации (ключ фаз A–B) ИЛИ journal-фаза ротации
        // (фаза C живёт после del заявки; надзор мог перезаписать журнал —
        // тогда guard просто не сработает, пересечение идемпотентно).
        var rotation = await GetAsync($"/kafkaworker/rotations/{cluster}", ct);
        if (!rotation.IsSuccess)
            return Fail(cluster, rotation.Error!, "reading-rotation");
        var rotateJournal = await journal.ReadAsync(cluster, ct);
        if (!rotateJournal.IsSuccess)
            return Fail(cluster, rotateJournal.Error!, "reading-journal");
        if (rotation.Value is not null
            || rotateJournal.Value is { Op: "rotate" } r && r.Phase != "done")
            return await journal.WriteAsync(cluster, Op, "waiting-rotation", claims.InstanceId, null, ct);

        // J0b: живой reassignment — пересоздания не смешиваются с переездами реплик.
        var reassignment = await GetAsync($"/kafkaworker/reassignments/{cluster}", ct);
        if (!reassignment.IsSuccess)
            return Fail(cluster, reassignment.Error!, "reading-reassignment");
        if (reassignment.Value is not null)
            return await journal.WriteAsync(cluster, Op, "waiting-reassign", claims.InstanceId, null, ct);

        // J1: кандидаты — стабильные ноды с декларацией ресурсов (TO_REMOVE/
        // REMOVING/PROVISIONING/NOT_INITIALIZED/UNREACHABLE — чужие процессы).
        var candidates = snap.Brokers
            .Where(b => b.State == "RUNNING" && b.Resources is not null)
            .OrderBy(b => b.Name, StringComparer.Ordinal)
            .ToList();

        // J2: сверка лимитов (ошибка инспекта → ошибка тика; контейнера нет →
        // пропуск — надзор восстановит).
        var diverged = new List<KafkaBrokerDecl>();
        foreach (var broker in candidates)
        {
            var limits = await driver.NodeResourcesAsync(cluster, broker.Name, ct);
            if (!limits.IsSuccess)
                return Fail(cluster, limits.Error!, "inspecting-limits");
            if (limits.Value is null)
                continue;
            if (NodeRegenPlanner.NeedsRegen(broker.Resources!, limits.Value))
                diverged.Add(broker);
        }

        // Прогресс-ключ читаем ДО ветвлений: «операция жива» = расхождения
        // есть ИЛИ ключ уже стоит (хвост — последний пересозданный брокер
        // ещё не вернулся в RUNNING). Ключ без операции не создаём (§4.1).
        var progress = await GetAsync(RegenKey(cluster), ct);
        if (!progress.IsSuccess)
            return Fail(cluster, progress.Error!, "reading-progress");
        var operationLive = diverged.Count > 0 || progress.Value is not null;
        if (!operationLive)
            return Result.Success(); // операции нет и не было — no-op: чужие
                                     // недоведённые ноды прогресс не рисуют

        // Недоведённые ноды кластера (не все RUNNING) — доводит F; при живой
        // операции их счёт входит в remaining (операциональная оценка, §4.3).
        var pending = diverged.Count
            + snap.Brokers.Count(b => b.State != "RUNNING");

        // J3: сходимость — прогресс-ключ гасим (operationLive ⇒ ключ мог
        // стоять; если не стоит — просто успех без записи).
        if (pending == 0)
        {
            if (progress.Value is not null)
            {
                var deleted = await DeleteAsync(RegenKey(cluster), ct);
                if (!deleted.IsSuccess)
                    return Fail(cluster, deleted.Error!, "dropping-progress");
                return await journal.WriteAsync(cluster, Op, "done", claims.InstanceId, null, ct);
            }

            return Result.Success();
        }

        // J4: операция жива, но в кластере есть недоведённые ноды — ждём
        // возврата (без новых пересозданий); remaining пересчитываем фактом.
        if (snap.Brokers.Any(b => b.State != "RUNNING"))
        {
            var current = snap.Brokers
                .FirstOrDefault(b => b.State != "RUNNING")?.Name;
            var written = await WriteProgressAsync(cluster, pending, current, null, ct);
            if (!written.IsSuccess)
                return written;
            return await journal.WriteAsync(cluster, Op, "waiting-return", claims.InstanceId, null, ct);
        }

        // J5: пересоздание первой расходящейся ноды (одна за тик;
        // diverged гарантированно непуст — иначе мы выше в no-op/J3/J4).
        var target = diverged[0];
        var marked = await journal.WriteAsync(
            cluster, Op, $"regenerating:{target.Name}", claims.InstanceId, null, ct);
        if (!marked.IsSuccess)
            return marked;

        if (snap.AppUser is null || snap.AppPassword is null)
            return Fail(cluster, new ApplicationException(
                $"regen {cluster}: нет app-кредов (ensure не выполнен)"), "no-creds");

        var addresses = await ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            return Fail(cluster, addresses.Error!, "reading-portalloc");
        if (!addresses.Value.TryGetValue(target.Name, out var addr))
            return Fail(cluster, new ApplicationException(
                $"regen {cluster}: broker {target.Name} не закреплён в portalloc"), "reading-portalloc");

        var removed = await driver.RemoveNodeAsync(cluster, target.Name, removeVolume: false, ct);
        if (!removed.IsSuccess)
            return Fail(cluster, removed.Error!, "removing-container");

        // Env пересобирается из текущей декларации (новые server-props
        // применяются тем же рестартом — детерминизм NodeEnvBuilder, R3).
        var env = BrokerEnvBuilder.Build(snap, target.Name, addr, [snap.AppPassword], options);
        var ensured = await driver.EnsureNodeAsync(new KafkaNodeSpec(
            cluster, target.Name, addr.Host, addr.ClientPort, options.NodeImage, env,
            target.Resources!.Cpu,
            target.Resources.MemGi * 1024L * 1024 * 1024), ct);
        if (!ensured.IsSuccess)
            return Fail(cluster, ensured.Error!, "ensuring-container");

        var state = await PutAsync(BrokerStateKey(cluster, target.Name), "PROVISIONING", ct);
        if (!state.IsSuccess)
            return Fail(cluster, state.Error!, "mark-provisioning");

        var progressWritten = await WriteProgressAsync(cluster, pending, target.Name, null, ct);
        if (!progressWritten.IsSuccess)
            return progressWritten;

        return Result.Success(); // следующий брокер — после RUNNING этого (J4)
    }

    // total — монотонный пик (PUT ресурсов посреди операции растит total),
    // remaining — текущий недоведённый счёт: UI видит «2 из 3».
    private async Task<Result> WriteProgressAsync(
        string cluster, int pending, string? currentBroker, string? lastError, CancellationToken ct)
    {
        var key = RegenKey(cluster);
        var existing = await GetAsync(key, ct);
        if (!existing.IsSuccess)
            return existing;
        var total = pending;
        if (existing.Value is { } kv)
        {
            try
            {
                using var doc = JsonDocument.Parse(kv.Value);
                if (doc.RootElement.TryGetProperty("brokers_total", out var prev)
                    && prev.GetInt32() > total)
                    total = prev.GetInt32();
            }
            catch (JsonException)
            {
                // Битый прогресс — мусор (arch/15 §6): перезаписываем фактом.
            }
        }

        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        return await PutAsync(key, JsonSerializer.Serialize(new KafkaRegenProgressJson(
            total, pending, currentBroker, now, claims.InstanceId, lastError)), ct);
    }

    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> ReadPortAllocAsync(
        string cluster, CancellationToken ct)
    {
        var result = await GetAsync($"/kafkaworker/portalloc/{cluster}", ct);
        if (!result.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(result.Error!);
        var addresses = new Dictionary<string, NodeAddress>();
        if (result.Value is { } kv)
        {
            using var doc = JsonDocument.Parse(kv.Value);
            foreach (var node in doc.RootElement.EnumerateObject())
                addresses[node.Name] = new NodeAddress(
                    node.Value.GetProperty("host").GetString()!,
                    node.Value.GetProperty("client").GetInt32());
        }

        return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(addresses);
    }

    private Result Fail(string cluster, Exception error, string phase)
    {
        journal.WriteAsync(cluster, Op, phase, claims.InstanceId, error.Message, CancellationToken.None)
            .GetAwaiter().GetResult();
        return Result.Failed(error);
    }

    private static string RegenKey(string cluster) => $"/kafkaworker/regens/{cluster}";

    private static string BrokerStateKey(string cluster, string broker)
        => $"/kafka/clusters/{cluster}/brokers/{broker}/state";

    private async Task<Result<Kv?>> GetAsync(string key, CancellationToken ct)
    {
        Result<Kv?>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.GetAsync(endpoint, key, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

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

    private async Task<Result> DeleteAsync(string key, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.DeleteAsync(endpoint, key, prefix: false, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }
}
```

- [x] **Шаг 4. Прогон unit — зелёный**

Run: `dotnet test src/tests/KafkaWorker.UnitTests --filter NodeRegenerator`
Ожидание: PASS (10 кейсов, включая запрет фантомного прогресса). Если
конструктор `ProvisioningOptions` не совпадает с записанным — сверить с
`KafkaClusterFixture.Options` (позиционные параметры `From, To,
BrokerBootSec, NodeDeadSec, AdvertisedClientHost, NodeImage`).

- [x] **Шаг 5. Конвейер** (`src/KafkaWorker.App/Loops/KafkaClusterProcesses.cs`):
  в интерфейс `IKafkaClusterProcesses` (doc-коммент ActiveAsync дополнить
  «→ регенерация (J)»), в класс `KafkaClusterProcesses` — параметр
  `NodeRegenerator regenerator` (после `AppPasswordRotator rotator`) и вызов
  между ротацией и TopicSync:

```csharp
        // Регенерация (J, t06): автоконверге лимитов — после ротации (не
        // смешиваем rolling-ы) и перед TopicSync (реестр — к итогу).
        var regenerated = await regenerator.RunAsync(snap, ct);
        if (!regenerated.IsSuccess)
            return regenerated;
```

- [x] **Шаг 6. X2-очистка + тест** (ревью Фазы 4, замечание 5):

  6a. `DeprovisioningProcess.cs`: найти фазу X2 (del координационных ключей —
  рядом del `/kafkaworker/rotations/...` и rebalances/reassignments) и
  добавить `del /kafkaworker/regens/<C>` тем же блоком (порт строки соседей;
  комментарий: «regens — live-прогресс регенерации, не переживает демонтаж
  (t06, spec §10.5)»).

  6b. Расширить существующий тест `DeprovisioningProcessTests.Run_RemovesRebalanceKeys`
  (`src/tests/KafkaWorker.UnitTests/Provisioning/DeprovisioningProcessTests.cs:80–99`)
  — в setup-сид добавить ключ регенерации и в asserts — его отсутствие:

```csharp
        // В setup-ламбду NewRig (после seed reassignments) добавить:
        etcd.Seed("/kafkaworker/regens/events",
            """{"brokers_total":1,"brokers_remaining":1,"current_broker":"broker1","updated_unix":1756500140,"instance":"x"}""");

        // В asserts (после NotContainKey reassignments) добавить:
        // t06: live-прогресс регенерации тоже не переживает демонтаж (X2)
        rig.Etcd.Store.Should().NotContainKey("/kafkaworker/regens/events");
```

- [x] **Шаг 7. DI** (`Program.cs`, после регистрации `AppPasswordRotator`):

```csharp
builder.Services.AddSingleton(sp => new NodeRegenerator(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    ToProvisioningOptions(sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value)));
```

- [x] **Шаг 8. Сборка + весь unit-прогон**

Run: `dotnet build src/PgWorker.slnx && dotnet test src/tests/KafkaWorker.UnitTests`
Ожидание: 0 warnings; PASS (включая расширенный X2-кейс и анти-фантом).

- [x] **Шаг 9. Коммит**

```bash
git add src/KafkaWorker.Provisioning src/KafkaWorker.App src/tests/KafkaWorker.UnitTests/Provisioning
git commit -m "feat(kafka): t06 — NodeRegenerator (J): автоконверге лимитов, один брокер за тик, прогресс только при живой операции; X2+тест"
```

---

### Task 5: Интеграционный полный цикл регенерации (docker)

**Вход:** Tasks 1–4 (процесс J, инспект, мутация); фикстура
`KafkaClusterFixture` (динамические порты, BrokerBootSec=100).
**Выход:** e2e-кейс «PUT ресурсов → пересоздание → RUNNING → ключ исчез →
лимиты == декларации; данные пережили»; совпадающие ресурсы не рестартуют.
**Spec:** §7 (интеграционные), §10.3/10.6 (критерии приёмки 3, 6).

**Доказательства без container-Id (осознанное упрощение, ревью Фазы 4,
замечание 10):** driver API не отдаёт Id контейнера, поэтому
(а) «пересоздание произошло» доказывается сменой лимитов инспекта (лимиты
docker меняются ТОЛЬКО пересозданием) + наблюдаемым циклом
PROVISIONING→RUNNING; (б) «рестарта нет» — отсутствием прогресс-ключа
regens и стабильностью state=RUNNING на серии Regen-тиков (любой рестарт
обязан поставить PROVISIONING и живой ключ — наблюдаемо в снапшоте).
Produce/consume-цикл сообщения не добавляем: сохранность тома подтверждается
выживанием топика в метаданных кластера (spec §7 согласован).

**Files:**
- Test: `src/tests/KafkaWorker.IntegrationTests/Kafka/NodeRegenTests.cs`

**Interfaces (consumes):** `KafkaClusterFixture` (`Cluster()`,
`SeedClusterAsync`, `SnapshotAsync`, `GetAsync`, `Options`, `Driver`,
`Gateway`, `Endpoint`, `AdminFactory`, `DiscoveryAdminBuilderAsync`),
`ProvisioningProcess`/`AddBrokerProcess`/`NodeRegenerator` (риг — порт
`ReassignmentTests.NewRigAsync`; доведение — порт `UpAsync`
`ReassignmentTests.cs:76–93`).

- [x] **Шаг 1. Тест** (`src/tests/KafkaWorker.IntegrationTests/Kafka/NodeRegenTests.cs`):

```csharp
using Confluent.Kafka.Admin;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Provisioning;
using KafkaWorker.Provisioning.Processes;
using Xunit;

namespace KafkaWorker.IntegrationTests.Kafka;

// Полный цикл регенерации (t06, spec §7): 1-брокерный кластер (рестарт
// единственного брокера — бюджет бут-времени фикстуры), сходимость лимитов,
// сохранность данных, отсутствие лишних рестартов. Кластер доводится до
// Active ЦИКЛОМ Provision-тиков (порт UpAsync ReassignmentTests) — один тик
// не поднимает кластер, а дискавери-креды требуют endpoints из K5
// (ревью Фазы 4, замечания 1–2).
[Collection(KafkaCollection.Name)]
public class NodeRegenTests(KafkaClusterFixture fixture)
{
    private sealed record Rig(
        ClaimStore Claims, WorkJournal Journal,
        ProvisioningProcess Provision,
        AddBrokerProcess Add, NodeRegenerator Regen);

    private Rig BuildRig()
    {
        // Порт NewRigAsync ReassignmentTests (реальные зависимости, без null!):
        var claims = new ClaimStore([fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        var journal = new WorkJournal(fixture.Gateway, [fixture.Endpoint]);
        return new Rig(
            claims, journal,
            new ProvisioningProcess(
                fixture.Gateway, [fixture.Endpoint], fixture.Driver, claims, journal,
                new AppSecretEnsurer(fixture.Gateway, [fixture.Endpoint]),
                fixture.AdminFactory, new ClusterConfigConverger(fixture.AdminFactory),
                fixture.Options, snapshot: null),
            new AddBrokerProcess(
                fixture.Gateway, [fixture.Endpoint], fixture.Driver, claims, journal,
                fixture.AdminFactory, fixture.Options),
            new NodeRegenerator(
                fixture.Gateway, [fixture.Endpoint], fixture.Driver, claims, journal, fixture.Options));
    }

    // Порт UpAsync (ReassignmentTests.cs:76–93): цикл Provision-тиков до
    // Active (config без state) — K4 транзиентно waiting-brokers = успех.
    private async Task UpAsync(Rig rig, string cluster, int budgetSec)
    {
        var ct = TestContext.Current.CancellationToken;
        var deadline = DateTimeOffset.UtcNow.AddSeconds(budgetSec);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var snap = await fixture.SnapshotAsync(cluster);
            if (snap!.Config.State is null)
                break;

            var tick = await rig.Provision.RunAsync(snap, ct);
            tick.IsSuccess.Should().BeTrue(
                $"тик provisioning не должен падать (waiting-brokers — успех): {tick.Error?.Message}");
            await Task.Delay(3000, ct);
        }

        (await fixture.SnapshotAsync(cluster))!.Config.State.Should().BeNull(
            $"кластер {cluster} не поднялся за {budgetSec} с");
    }

    // Доведение до RUNNING (после UpAsync брокеры уже RUNNING по K4, но
    // цикл гарантирует; Add идемпотентен на RUNNING — no-op).
    private async Task BringToRunningAsync(Rig rig, string cluster, int budgetSec)
    {
        var ct = TestContext.Current.CancellationToken;
        var deadline = DateTimeOffset.UtcNow.AddSeconds(budgetSec);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var snap = await fixture.SnapshotAsync(cluster);
            if (snap!.Brokers.All(b => b.State == "RUNNING"))
                return;

            var tick = await rig.Add.RunAsync(snap, ct);
            tick.IsSuccess.Should().BeTrue($"тик add не должен падать: {tick.Error?.Message}");
            await Task.Delay(3000, ct);
        }

        Assert.Fail("брокеры не достигли RUNNING в бюджет");
    }

    [Fact]
    public async Task PutResources_LimitsDiverge_RollingRegenConvergesWithSameVolume()
    {
        // Arrange — 1-брокерный кластер: ЦИКЛ Provision до Active (endpoints
        // появятся в K5 — только тогда доступен дискавери-клиент), затем
        // топик «keep» (том должен пережить регенерацию)
        var cluster = fixture.Cluster("rg1");
        await fixture.SeedClusterAsync(cluster, 1);
        var rig = BuildRig();
        var ct = TestContext.Current.CancellationToken;
        await rig.Claims.TryClaimClusterAsync(cluster, ct);
        await UpAsync(rig, cluster, budgetSec: 120);
        await BringToRunningAsync(rig, cluster, budgetSec: 60);

        var topicBuilder = await fixture.DiscoveryAdminBuilderAsync(cluster);
        using (var admin = topicBuilder.Build())
            await admin.CreateTopicsAsync([new TopicSpecification { Name = "keep", NumPartitions = 1 }]);

        // Act — декларация меняется (cpu 1→2, mem 1Gi→2Gi): как мутация №15
        await fixture.Gateway.PutAsync(fixture.Endpoint,
            $"/kafka/clusters/{cluster}/brokers/broker1/resources",
            """{"cpu":"2","mem":"2Gi","disk":"10Gi"}""", null, ct);

        // Assert — сходимость: RUNNING + новые лимиты + прогресс-ключ исчез.
        // Пересоздание доказывается сменой лимитов (docker меняет лимиты
        // только пересозданием) и наблюдаемым PROVISIONING-циклом.
        var inspected = await fixture.Driver.NodeResourcesAsync(cluster, "broker1", ct);
        var deadline = DateTimeOffset.UtcNow.AddSeconds(180);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var snap = await fixture.SnapshotAsync(cluster);
            await rig.Regen.RunAsync(snap!, ct);
            if (snap!.Brokers.Single(b => b.Name == "broker1").State != "RUNNING")
            {
                await rig.Add.RunAsync(snap, ct); // доводка PROVISIONING → RUNNING (F)
                continue;
            }

            inspected = await fixture.Driver.NodeResourcesAsync(cluster, "broker1", ct);
            if (inspected.IsSuccess
                && inspected.Value == new Core.Planning.NodeLimits(2_000_000_000L, 2L << 30)
                && await fixture.GetAsync($"/kafkaworker/regens/{cluster}") is null)
                break;
            await Task.Delay(2000, ct);
        }

        inspected.Value.Should().Be(new Core.Planning.NodeLimits(2_000_000_000L, 2L << 30),
            "лимиты контейнера обязаны сойтись к декларации");
        (await fixture.GetAsync($"/kafkaworker/regens/{cluster}")).Should().BeNull(
            "прогресс-ключ удаляется по сходимости");

        // Том пережил пересоздание: топик жив в метаданных кластера
        // (производственный produce/consume-цикл — вне объёма; spec §7).
        var metaBuilder = await fixture.DiscoveryAdminBuilderAsync(cluster);
        using (var admin = metaBuilder.Build())
        {
            var metadata = admin.GetMetadata(TimeSpan.FromSeconds(10));
            metadata.Topics.Should().Contain(t => t.Topic == "keep");
        }
    }

    [Fact]
    public async Task PutSameResources_NoRecreate()
    {
        // Arrange — сошедшийся кластер: ЦИКЛ Provision до Active, затем
        // доводка до RUNNING (ревью Фазы 4, замечание 2: в цикле обязаны
        // тикать процессы — одного чтения снапшота недостаточно)
        var cluster = fixture.Cluster("rg2");
        await fixture.SeedClusterAsync(cluster, 1);
        var rig = BuildRig();
        var ct = TestContext.Current.CancellationToken;
        await rig.Claims.TryClaimClusterAsync(cluster, ct);
        await UpAsync(rig, cluster, budgetSec: 120);
        await BringToRunningAsync(rig, cluster, budgetSec: 60);

        // Act — серия Regen-тиков при сошедшихся ресурсах (JSON сида не менялся)
        for (var i = 0; i < 3; i++)
        {
            var tick = await rig.Regen.RunAsync(await fixture.SnapshotAsync(cluster), ct);
            tick.IsSuccess.Should().BeTrue();
            await Task.Delay(1000, ct);
        }

        // Assert — рестарта нет: прогресс-ключ не ставился, брокер остался
        // RUNNING (любое пересоздание обязано поставить PROVISIONING и
        // живой regens-ключ — наблюдаемо; spec §10.3 «совпадающие — без
        // рестарта», доказательство без container-Id — см. шапку задачи)
        (await fixture.GetAsync($"/kafkaworker/regens/{cluster}")).Should().BeNull();
        (await fixture.SnapshotAsync(cluster))!.Brokers.Single(b => b.Name == "broker1")
            .State.Should().Be("RUNNING");
    }
}
```

Примечание: rig скопирован с `ReassignmentTests.NewRigAsync` (реальные
`AppSecretEnsurer`/`ClusterConfigConverger`/`fixture.AdminFactory`) — фаза K2
provisioning'а требует живого ensurer'а. Порядок тиков теста:
`Provision` (цикл до Active) → `Add` (доводка до RUNNING) → `Regen`
(сверка/шаг) — как конвейер Active-ветки без надзора (контейнер жив — надзор
no-op).

- [x] **Шаг 2. Прогон (Docker требуется; ~2–5 мин)**

Run: `dotnet test src/tests/KafkaWorker.IntegrationTests --filter NodeRegenTests`
Ожидание: PASS (2 кейса). Падение с бюджетом — проверить руками
`docker ps | grep kfw-rg` (осиротевшие контейнеры — teardown фикстуры чистит).

- [x] **Шаг 3. Полный прогон интеграционных воркера (регресс конвейера)**

Run: `dotnet test src/tests/KafkaWorker.IntegrationTests`
Ожидание: PASS (существующие + новые; J не ломает чужие процессы).

- [x] **Шаг 4. Коммит**

```bash
git add src/tests/KafkaWorker.IntegrationTests/Kafka/NodeRegenTests.cs
git commit -m "test(kafka): t06 — интеграционный цикл регенерации (доведение Provision-циклом, сходимость лимитов, том жив, без лишних рестартов)"
```

---

## Волна B — панель и стенд (задачи 6–9)

### Task 6: Панель — чтение regens (парсер, снапшот, DTO)

**Вход:** Tasks 1–5 (воркер пишет `/kafkaworker/regens/<C>`).
**Выход:** `KafkaSnapshot.Regens` → `KafkaClusterDto.Regen` в inspection API;
битый JSON толерантен.
**Spec:** §6.1, §4.1/§4.3, arch/adminpanel/02 §10.1.

**Files:**
- Modify: `src/AdminPanel.Core/Kafka/KafkaSnapshot.cs` (+record, +поле)
- Modify: `src/AdminPanel.Etcd/Parsing/KafkaParser.cs` (+ParseRegens)
- Modify: `src/AdminPanel.Etcd/KafkaSnapshotRefresher.cs` (префикс+чтение+поле)
- Modify: `src/AdminPanel.Api/Inspection/KafkaQuery.cs` (+DTO+MapDetails)
- Test: `src/tests/AdminPanel.UnitTests/KafkaParserTests.cs` (+кейсы)

**Interfaces (produces):**
- `public sealed record KafkaRegenProgress(string Cluster, int BrokersTotal, int BrokersRemaining, string? CurrentBroker, long UpdatedUnix, string? LastError)` — `AdminPanel.Core.Kafka`.
- `KafkaSnapshot(..., IReadOnlyList<KafkaRegenProgress> Regens, ...)` — новое
  поле после `Reassignments` (обновить все конструкторы: refresher и
  FailTick-заготовку).
- `KafkaParser.ParseRegens(IReadOnlyList<Kv> kvs)` →
  `KafkaRegensParseResult(IReadOnlyList<KafkaRegenProgress> Progress, IReadOnlyList<KeyParseError> Errors)`.
- `public sealed record KafkaRegenDto(int BrokersTotal, int BrokersRemaining, string? CurrentBroker, long UpdatedUnix)`
  + поле `KafkaRegenDto? Regen = null` в `KafkaClusterDto` (последним);
  `KafkaMappers.MapDetails(..., IReadOnlyList<KafkaRegenProgress> regens, ...)`.

- [x] **Шаг 1. Failing-тесты парсера** (в `KafkaParserTests.cs`, по образцу
  существующих ParseRebalances-кейсов; AAA; тип `Kv` — `AdminPanel.Etcd.Client`):

```csharp
    [Fact]
    public void ParseRegens_CanonicalKey_ParsesProgress()
    {
        // Arrange
        var kvs = new List<Kv> { new("/kafkaworker/regens/events",
            """{"brokers_total":3,"brokers_remaining":2,"current_broker":"broker2","updated_unix":1750000000,"instance":"kfw-1"}""") };

        // Act
        var result = KafkaParser.ParseRegens(kvs);

        // Assert
        result.Errors.Should().BeEmpty();
        result.Progress.Should().ContainSingle().Which.Should().Match<KafkaRegenProgress>(p =>
            p.Cluster == "events" && p.BrokersTotal == 3 && p.BrokersRemaining == 2
            && p.CurrentBroker == "broker2" && p.UpdatedUnix == 1750000000);
    }

    [Fact]
    public void ParseRegens_BrokenJson_ErrorWithoutThrow()
    {
        // Arrange
        var kvs = new List<Kv> { new("/kafkaworker/regens/events", "{oops") };

        // Act
        var result = KafkaParser.ParseRegens(kvs);

        // Assert — толерантность arch/15 §6: parseError, не исключение
        result.Progress.Should().BeEmpty();
        result.Errors.Should().ContainSingle();
    }

    [Fact]
    public void ParseRegens_WrongShape_Error()
    {
        // Arrange — ключ без кластера
        var kvs = new List<Kv> { new("/kafkaworker/regens/", "{}") };

        // Act
        var result = KafkaParser.ParseRegens(kvs);

        // Assert
        result.Progress.Should().BeEmpty();
        result.Errors.Should().ContainSingle();
    }
```

- [x] **Шаг 2. Прогон — failing**

Run: `dotnet test src/tests/AdminPanel.UnitTests --filter ParseRegens`
Ожидание: FAIL — метод не существует.

- [x] **Шаг 3. Реализация парсера** (`KafkaParser.cs`, после
  `ParseReassignments`; `KafkaRegensParseResult` — рядом с
  `KafkaRebalancesParseResult`):

```csharp
public sealed record KafkaRegensParseResult(
    IReadOnlyList<KafkaRegenProgress> Progress,
    IReadOnlyList<KeyParseError> Errors);
```

```csharp
    // /kafkaworker/regens/<C> (t06, arch/15 §4): live-прогресс регенерации.
    public static KafkaRegensParseResult ParseRegens(IReadOnlyList<Kv> kvs)
    {
        var progress = new List<KafkaRegenProgress>();
        var errors = new List<KeyParseError>();
        foreach (var kv in kvs)
        {
            // "/kafkaworker/regens/<C>" → ["", "kafkaworker", "regens", <C>]
            var segments = kv.Key.Split('/');
            if (segments.Length != 4 || segments[3].Length == 0)
            {
                errors.Add(new(kv.Key, "ожидается /kafkaworker/regens/<cluster>"));
                continue;
            }

            try
            {
                using var doc = JsonDocument.Parse(kv.Value);
                var root = doc.RootElement;
                var total = JsonValues.ReadInt(root, "brokers_total");
                var remaining = JsonValues.ReadInt(root, "brokers_remaining");
                if (total is null || remaining is null)
                {
                    errors.Add(new(kv.Key, "нет полей brokers_total/brokers_remaining"));
                    continue;
                }

                progress.Add(new KafkaRegenProgress(
                    segments[3], total.Value, remaining.Value,
                    JsonValues.ReadString(root, "current_broker"),
                    JsonValues.ReadLong(root, "updated_unix") ?? 0,
                    JsonValues.ReadString(root, "last_error")));
            }
            catch (JsonException e)
            {
                errors.Add(new(kv.Key, $"битый JSON: {e.Message}"));
            }
        }

        return new(progress, errors);
    }
```

(если `JsonValues.ReadInt` нет — добавить по образцу `ReadLong` рядом.)

- [x] **Шаг 4. Модель+refresher:** `KafkaSnapshot.cs` — record
  `KafkaRegenProgress` (после `KafkaReassignmentProgress`) + поле
  `IReadOnlyList<KafkaRegenProgress> Regens` в `KafkaSnapshot` (после
  `Reassignments`); `KafkaSnapshotRefresher.cs` — префикс
  `public const string Regens = "/kafkaworker/regens/";`, чтение
  `var regensKv = await RangeWithFailoverAsync(endpoints, active, Prefixes.Regens, ct);`
  рядом с reassignments (добавить в проверку `!regensKv.IsSuccess`), парсинг
  `var regens = KafkaParser.ParseRegens(regensKv.Value);`, поле
  `regens.Progress` в конструктор `KafkaSnapshot`, ошибки
  `.. regens.Errors` в общий список; в `FailTick`-заготовке — `[]` для нового
  поля (позиционно после Reassignments-`[]`).

- [x] **Шаг 5. DTO** (`KafkaQuery.cs`): record `KafkaRegenDto` (после
  `KafkaReassignmentDto`), поле `KafkaRegenDto? Regen = null` последним в
  `KafkaClusterDto`; в `KafkaMappers.MapDetails` — параметр
  `IReadOnlyList<KafkaRegenProgress> regens` (после `reassignments`),
  вычисление `var regen = regens.FirstOrDefault(r => r.Cluster == cluster.Name);`
  и в возвращаемый `KafkaClusterDto` —
  `Regen: regen is null ? null : new KafkaRegenDto(regen.BrokersTotal, regen.BrokersRemaining, regen.CurrentBroker, regen.UpdatedUnix)`;
  обновить вызов `MapDetails` (передать `snapshot.Regens`).

- [x] **Шаг 6. Сборка + тесты панели**

Run: `dotnet build src/PgWorker.slnx && dotnet test src/tests/AdminPanel.UnitTests`
Ожидание: 0 warnings; PASS.

- [x] **Шаг 7. Коммит**

```bash
git add src/AdminPanel.Core src/AdminPanel.Etcd src/AdminPanel.Api src/tests/AdminPanel.UnitTests
git commit -m "feat(adminpanel): t06 — чтение прогресса регенерации (парсер regens, снапшот, DTO)"
```

---

### Task 7: Панель — прокси PUT resources (мутация №15)

**Вход:** Task 3 (воркер-маршрут), Task 6.
**Выход:** `PUT /api/kafka/clusters/{cluster}/brokers/{broker}/resources`
панели — прокси 1:1 в API воркера.
**Spec:** §6.2, arch/adminpanel/02 §10.2-15.

**Files:**
- Create: `src/AdminPanel.Api/Operations/Kafka/BrokerResourcesCommands.cs`
- Modify: `src/AdminPanel.Api/Operations/Kafka/KafkaOperationsModule.cs`
  (+маршрут после MapDelete-брокера)
- Test: `src/tests/AdminPanel.UnitTests/Operations/WorkerProxyCommandTests.cs` (+кейсы)

**Interfaces (produces):**
- `public sealed record UpdateKafkaBrokerResourcesCommand(string Cluster, string Broker, KafkaBrokerResourcesRequestDto Request) : ICommand<KafkaBrokerResourcesUpdatedDto>`
- `public sealed record KafkaBrokerResourcesRequestDto(decimal? Cpu, int? MemGi, int? DiskGi)`
- `public sealed record KafkaBrokerResourcesUpdatedDto(string Cluster, string Broker, string Cpu, string MemGi, string DiskGi)`

- [x] **Шаг 1. Failing-тест** (в `WorkerProxyCommandTests.cs`):

```csharp
    [Fact]
    public async Task UpdateKafkaBrokerResources_PutsBodyToWorkerApi()
    {
        // Arrange — мутация №15 не заявка: оператор не шлётся (нет requested_by)
        var api = new StubWorkerApi
        {
            Respond = _ => new WorkerApiResult(200,
                """{"cluster":"events","broker":"broker1","cpu":"4","memGi":"4Gi","diskGi":"40Gi"}"""),
        };
        var handler = new UpdateKafkaBrokerResourcesCommandHandler(api);
        var command = new UpdateKafkaBrokerResourcesCommand(
            "events", "broker1", new KafkaBrokerResourcesRequestDto(4m, null, null));

        // Act
        var result = await handler.Handle(command, CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Cpu.Should().Be("4");
        api.Calls.Should().ContainSingle().Which.Should().Match<StubWorkerApi.Call>(c =>
            c.Worker == "kafkaworker" && c.Method == HttpMethod.Put
            && c.Path == "/api/kafka/clusters/events/brokers/broker1/resources"
            && c.RequestedBy == null);
    }

    [Fact]
    public async Task UpdateKafkaBrokerResources_409ProblemDetails_Failed()
    {
        // Arrange — гварды воркера проксируются 1:1
        var api = new StubWorkerApi
        {
            Respond = _ => new WorkerApiResult(409,
                """{"title":"Broker resources update rejected","status":409,"detail":"брокер заявлен к удалению"}"""),
        };
        var handler = new UpdateKafkaBrokerResourcesCommandHandler(api);

        // Act
        var result = await handler.Handle(
            new UpdateKafkaBrokerResourcesCommand("events", "broker3",
                new KafkaBrokerResourcesRequestDto(4m, null, null)),
            CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<WorkerProblemDetails>()
            .Which.StatusCode.Should().Be(409);
    }
```

- [x] **Шаг 2. Прогон — failing**

Run: `dotnet test src/tests/AdminPanel.UnitTests --filter UpdateKafkaBrokerResources`
Ожидание: FAIL.

- [x] **Шаг 3. Реализация**
  (`src/AdminPanel.Api/Operations/Kafka/BrokerResourcesCommands.cs`):

```csharp
using AdminPanel.Etcd.Workers;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Operations.Kafka;

// ===== 15. Изменение ресурсов брокера (t06, 02 §10.2-15): декларация в etcd
// через API воркера; применяет NodeRegenerator (rolling, автоконверге) =====

public sealed record UpdateKafkaBrokerResourcesCommand(
    string Cluster, string Broker, KafkaBrokerResourcesRequestDto Request)
    : ICommand<KafkaBrokerResourcesUpdatedDto>;

public sealed record KafkaBrokerResourcesRequestDto(decimal? Cpu, int? MemGi, int? DiskGi);

public sealed record KafkaBrokerResourcesUpdatedDto(
    string Cluster, string Broker, string Cpu, string MemGi, string DiskGi);

[InjectAsScoped]
public sealed class UpdateKafkaBrokerResourcesCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<UpdateKafkaBrokerResourcesCommand, KafkaBrokerResourcesUpdatedDto>
{
    public async ValueTask<Result<KafkaBrokerResourcesUpdatedDto>> Handle(
        UpdateKafkaBrokerResourcesCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<KafkaBrokerResourcesUpdatedDto>(
            api, "kafkaworker", HttpMethod.Put,
            $"/api/kafka/clusters/{command.Cluster}/brokers/{command.Broker}/resources",
            body: command.Request, requestedBy: null, ct);
}
```

- [x] **Шаг 4. Маршрут** (`KafkaOperationsModule.cs`, после MapDelete
  брокеров):

```csharp
        // PUT /api/kafka/clusters/{cluster}/brokers/{broker}/resources — мутация
        // №15 (t06, 02 §10.2-15): прокси в API воркера; применяет NodeRegenerator.
        endpoints.MapPut("/api/kafka/clusters/{cluster}/brokers/{broker}/resources", async (
            string cluster, string broker, KafkaBrokerResourcesRequestDto request,
            IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<UpdateKafkaBrokerResourcesCommand, KafkaBrokerResourcesUpdatedDto>(
                new UpdateKafkaBrokerResourcesCommand(cluster, broker, request), ct);
            if (result.IsSuccess)
                return Results.Ok(result.Value);

            return Error(result);
        });
```

- [x] **Шаг 5. Прогон + сборка**

Run: `dotnet build src/PgWorker.slnx && dotnet test src/tests/AdminPanel.UnitTests --filter WorkerProxyCommandTests`
Ожидание: 0 warnings; PASS (включая 2 новых кейса).

- [x] **Шаг 6. Коммит**

```bash
git add src/AdminPanel.Api/Operations/Kafka src/tests/AdminPanel.UnitTests/Operations
git commit -m "feat(adminpanel): t06 — прокси мутации №15 (PUT resources брокера) в API воркера"
```

---

### Task 8: Фронт — модалка ресурсов + прогресс регенерации

**Вход:** Tasks 6–7 (DTO `regen` в деталях кластера, PUT-мутация).
**Выход:** редактирование ресурсов брокера из UI, видимый прогресс операции.
**Spec:** §6.3 (модалка с предупреждением об уменьшении; строка прогресса;
подпись у текущего брокера).

**Files:**
- Modify: `frontend/src/api/dto.ts` (+`KafkaRegenDto`, поле `regen`)
- Modify: `frontend/src/api/queries.ts` (+`updateKafkaBrokerResources`)
- Create: `frontend/src/pages/kafka-cluster/EditBrokerResourcesModal.tsx`
- Modify: `frontend/src/pages/kafka-cluster/BrokersTab.tsx`
- Modify: `frontend/src/pages/kafka-cluster/KafkaClusterDetailsPage.tsx`

**Interfaces (consumes):** `KafkaClusterDto.regen: KafkaRegenDto | null`,
`PUT /api/kafka/clusters/{c}/brokers/{b}/resources` (body
`{cpu?, memGi?, diskGi?}`); образцы: `AddBrokerModal.tsx` (форма+валидация),
`EditClusterConfigModal.tsx` (частичное обновление), `RemoveBrokerButton.tsx`.

- [x] **Шаг 1. DTO** (`dto.ts`): после `KafkaReassignmentDto` —

```ts
// Прогресс rolling-регенерации брокеров (t06, arch/15 §4); null = операции нет.
export interface KafkaRegenDto {
  brokersTotal: number;
  brokersRemaining: number;
  currentBroker: string | null;
  updatedUnix: number;
}
```

и в `KafkaClusterDto` — поле `regen: KafkaRegenDto | null;` после
`reassignment`.

- [x] **Шаг 2. API-функция** (`queries.ts`, после `removeKafkaBroker`):

```ts
// PUT /api/kafka/clusters/{cluster}/brokers/{broker}/resources — мутация №15
// (t06, arch/02 §10.2-15): применяется автоматически rolling-регенерацией.
export function updateKafkaBrokerResources(
  cluster: string,
  broker: string,
  request: { cpu?: number; memGi?: number; diskGi?: number },
): Promise<KafkaBrokerResourcesUpdatedDto> {
  return apiFetch<KafkaBrokerResourcesUpdatedDto>(
    `/api/kafka/clusters/${encodeURIComponent(cluster)}/brokers/${encodeURIComponent(broker)}/resources`,
    { method: 'PUT', body: request },
  );
}
```

(+ `KafkaBrokerResourcesUpdatedDto` interface в `dto.ts`:
`{ cluster: string; broker: string; cpu: string; memGi: string; diskGi: string }`.)

- [x] **Шаг 3. Модалка** (`EditBrokerResourcesModal.tsx` — по каркасу
  `AddBrokerModal.tsx`: Mantine `Modal`+`NumberInput` cpu/memGi/diskGi с
  предзаполнением текущих значений брокера; submit →
  `updateKafkaBrokerResources` → инвалидация квери кластера
  (`queryClient.invalidateQueries({ queryKey: kafkaQueryKeys.cluster(cluster) })`)
  → `notifications`; текст-предупреждение в теле модалки:
  «Уменьшение CPU/памяти может привести к OOM или деградации брокера (риск —
  на операторе). Применяется автоматически: rolling-пересоздание брокеров,
  по одному за тик; данные сохраняются.»; кнопка дизейблена, если все поля
  пусты/не менялись). Props: `{ cluster: string; broker: KafkaBrokerDto; opened: boolean; onClose: () => void }`.

- [x] **Шаг 4. BrokersTab**: в `BrokerRow` — иконка-кнопка «Изменить ресурсы»
  (`<Button size="compact-xs" variant="light">Ресурсы</Button>`) рядом с
  «Убрать»; дизейбл: `!canScale || broker.state === 'TO_REMOVE' ||
  broker.state === 'REMOVING' || broker.state === 'NOT_INITIALIZED'`;
  tooltip причины. Подпись текущего реген-брокера (порт drain-подписи):

```tsx
  {regen !== null && regen.currentBroker === broker.name ? (
    <Text size="xs" c="indigo" display="block">
      регенерация (осталось {regen!.brokersRemaining})
    </Text>
  ) : null}
```

(прокинуть `regen: KafkaRegenDto | null` пропсом из страницы, как
`reassignment`).

- [x] **Шаг 5. Прогресс в деталях** (`KafkaClusterDetailsPage.tsx`): рядом с
  местом вывода `reassignment`-информации — блок при `cluster.regen !== null`:

```tsx
{cluster.regen !== null ? (
  <Text size="sm" c="indigo">
    Регенерация брокеров: осталось {cluster.regen.brokersRemaining} из{' '}
    {cluster.regen.brokersTotal}
    {cluster.regen.currentBroker !== null ? ` (текущий ${cluster.regen.currentBroker})` : ''}
  </Text>
) : null}
```

- [x] **Шаг 6. Проверки фронтенда**

Run: `cd frontend && npm run typecheck && npm run build`
Ожидание: обе команды — 0 errors.

- [x] **Шаг 7. Коммит**

```bash
git add frontend/src
git commit -m "feat(adminpanel): t06 — UI ресурсов брокера (модалка) и прогресс регенерации"
```

---

### Task 9: e2e-чек стенда 59 + финальный прогон

**Вход:** Tasks 1–8; поднятый стенд (`dev-stand/adminpanel/checks/00-up.sh`)
или чек поднимает своё (порт `ADMINPANEL_URL`, образец — `55-kafka-e2e.sh`).
**Выход:** зелёный `59-kafka-regen.sh` с чистого состояния; полная сборка и
все тесты зелёные.
**Spec:** §7 (e2e), §10 (критерии 1, 3, 4, 8).

**Files:**
- Create: `dev-stand/adminpanel/checks/59-kafka-regen.sh`

- [x] **Шаг 1. Чек-скрипт** (по каркасу `55-kafka-e2e.sh`: `set -euo pipefail`,
  `BASE="${ADMINPANEL_URL:-http://localhost:5050}"`, cookie-JAR; воркер —
  `docker compose --profile kafka` как в 55; кластер `e2e6`; шаги):
  1) чистый kafka-префикс стенда (`etcd_kafka_keys` пуст для `/kafka/`);
  2) создать кластер 3 брокера через API панели → ждать RUNNING+endpoints
     (поллинг GET деталей; таймаут ~150 с);
  3) `docker inspect kfw-e2e6-broker1` — зафиксировать `NanoCPUs` ДО;
  4) `PUT /api/kafka/clusters/e2e6/brokers/broker1/resources` body
     `{"cpu":3}` (cookie-сессия) → 200;
  5) поллинг: (а) GET деталей — `regen !== null` хотя бы раз (прогресс
     виден); (б) затем `regen === null` И broker1 `state === 'RUNNING'`
     И `docker inspect` `NanoCPUs == 3000000000` — сходимость (бюджет ~150 с);
  6) негативы: PUT `{"cpu":100}` → 400; PUT на `ghost`-кластер → 404; PUT
     `broker9` → 404;
  7) идемпотентность: повтор `{"cpu":3}` → 200, рестарта нет — контейнер
     жив без изменений (проверка: `docker inspect --format '{{.State.Running}}'`
     остаётся true, age контейнера (`{{.State.StartedAt}}`) не меняется
     за ~15 с наблюдения);
  8) TO_REMOVE кластера → `/kafka/` пуст (как в 55, финал).
  Каждое утверждение — `echo "✅/❌"` и `exit 1` при провале (стиль 55).

- [x] **Шаг 2. Прогон чека на стенде**

Run: `bash dev-stand/adminpanel/checks/59-kafka-regen.sh` (стенд поднят
`00-up.sh`; при недоступности — поднять по `55`-паттерну).
Ожидание: все шаги ✅, exit 0.

- [x] **Шаг 3. Финальный полный прогон**

Run: `dotnet build src/PgWorker.slnx && dotnet test` и
`cd frontend && npm run typecheck`
Ожидание: 0 warnings; все тесты зелёные (unit без Docker; integration с
Docker); typecheck 0 errors.

Факт (2026-09-03): build 0 warnings/0 errors; typecheck 0 errors; зелёные —
все unit (AdminPanel 402, KafkaWorker 209, PgWorker 531), AdminPanel и
KafkaWorker integration (110 и 47, в т.ч. Docker). 4 падения PgWorker E2e
(Scale_TakeoverMidAdd, Scale_AddEmptyShard, Acceptance_Ac2, Move_Chain) —
pre-existing на main: воспроизведены на чистом main со свежесобранным
Release-бинаром PgWorker.App (старый Release от 29 авг зелёный) — вне зоны
t06 (ветка PgWorker.*/PgWorker.IntegrationTests не трогала).

- [x] **Шаг 4. Коммит**

```bash
git add dev-stand/adminpanel/checks/59-kafka-regen.sh
git commit -m "test(stand): t06 — e2e-чек 59 rolling-регенерации брокеров (прогресс, лимиты, идемпотентность)"
```

---

## Самопроверка плана (выполнена; обновлена по ревью Фазы 4, раунды 1–2)

- **Покрытие spec:** §4.2/§4.3 → Task 3; §5.1 → Task 3 (шаги 5–7);
  §5.2 J0–J5 → Task 4; §5.3 → Tasks 1–2; §5.4–5.5 → Task 4 (шаги 5–7);
  §6.1 → Task 6; §6.2 → Task 7; §6.3 → Task 8; §7 → Tasks 3/4/5/9;
  §8 (волны A/B) → структура; §10.1–10.8 → Tasks 3/4/5/9 проверки.
  Пробелов нет.
- **Плейсхолдеры:** отсутствуют; каждый шаг с кодом или точной командой.
- **Типы:** `NodeLimits`/`NodeRegenPlanner` (Core) потребляются Docker-слоем и
  фейком (Task 2) и `NodeRegenerator` (Task 4); `KafkaResourcesUpdateRequest`
  (Core.Writing) — хендлером Task 3; панельные DTO (Task 6/7) и фронт
  (Task 8) имена согласованы (`Regen`, `KafkaRegenDto`,
  `updateKafkaBrokerResources`).
- **Ревью Фазы 4 раунд 1 (CHANGES_REQUESTED), все 10 замечаний учтены:**
  1/2 — интеграционные кейсы Task 5 доводят кластер циклом
  `UpAsync`(Provision до Active)+`BringToRunningAsync`(Add до RUNNING) до
  дискавери/Regen-тиков; 3 — `RunAsync_NoClaim_Fails` строит чужой
  NodeRegenerator со своим ClaimStore; 4 — `ExpectedNanoCpus` повторяет
  арифметику записи `(long)((double)cpu * 1e9)` + кейсы 0.01/1.15; 5 — X2
  покрыт расширением `DeprovisioningProcessTests.Run_RemovesRebalanceKeys`;
  6 — inspect-методы разворачивают nullable `GetAsync<T>`; 7 —
  `PlainClusterDriver.NodeName` с квалификацией в swarm-драйвере; 8 —
  канонизация `KafkaClusterCreatePlan.Canonical` (план + spec §4.2); 9 —
  фантом-правка комментария «14 мутаций» убрана из плана и spec §5.5;
  10 — кейс `Update_UnknownBroker_404`; доказательства пересоздания/отсутствия
  рестарта без container-Id зафиксированы в шапке Task 5 и согласованы в
  spec §7.
- **Ревью Фазы 4 раунд 2, оба замечания учтены:**
  1 — прогресс-ключ `/kafkaworker/regens/<C>` ставится/держится ТОЛЬКО при
  живой операции: в RunAsync прогресс-ключ читается до ветвлений,
  `operationLive = diverged.Count > 0 || ключ жив`; при живой операции и
  недоведённых нодах — J4 (прогресс + waiting-return), при чужих
  недоведённых нодах без операции — no-op БЕЗ put (нет фантома «Регенерация
  N из N»); зафиксировано unit-кейсом
  `RunAsync_ForeignNotRunningBroker_NoPhantomProgressKey`; кейс
  `RunAsync_NotRunningBrokerExists_WaitsWithoutRecreate` дополнен ассертом
  живого ключа; spec §5.2 (J3/J4) точечно уточнён без изменения требований;
  2 — `FixedTimeProvider` инициализируется свойством `Utc` через
  object-initializer (конструктора с аргументом нет — сверенo с
  FixedTimeProvider.cs); других вхождений паттерна в плане нет.
