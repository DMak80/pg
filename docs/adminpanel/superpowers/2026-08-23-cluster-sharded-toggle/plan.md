# cluster-sharded-toggle — план реализации

> **Для agentic-исполнителей:** ОБЯЗАТЕЛЬНЫЙ SUB-SKILL: superpowers:subagent-driven-development (рекомендуется) или superpowers:executing-plans — реализовывать по задачам. Шаги оформлены чекбоксами (`- [ ]`) для трекинга.

**Цель:** переключатель «Шардированная | Нешардированная» в модалке создания кластера; для нешардированной поля бакетов/шардов не запрашиваются, сервер нормализует в 1×1.

**Архитектура:** нешардированная БД = вырожденный случай контракта t12 (buckets=1, shards=1); формат etcd-ключей и протокол записи не меняются. `CreateClusterRequest` получает хвостовое `bool? Sharded = null` (отсутствует = true, backward-compat) и чистую `Normalize()` до валидации; `ClusterCreatedDto` — поле `sharded`. Фронт: `SegmentedControl` + условный блок «Шардирование», тело запроса при `sharded=false` без buckets/shards.

**Стек:** .NET 10 Minimal API (C# `Nullable=enable`, `TreatWarningsAsErrors=true`), xUnit + FluentAssertions, Testcontainers etcd, React 19 + Mantine 9.5 + TanStack Query, Vite.

**Spec:** `docs/superpowers/2026-08-23-cluster-sharded-toggle/spec.md` (в worktree; план аргументируется от spec — исполнители читают оба).

## Глобальные ограничения

- Рабочая директория всех команд: `/Users/demakaev/ZCodeProject/worktrees/feat-cluster-sharded-toggle` (далее `$WT`); ветка `feat-cluster-sharded-toggle`, коммитим в неё, НЕ пушим, в `main` не мерджим (гейт main-агента).
- `TreatWarningsAsErrors=true`: новые warning = ошибка сборки; не оставлять неиспользуемые using/переменные.
- Тесты — с комментариями по AAA (Arrange/Act/Assert) — правило AGENTS.md.
- Язык русский (комментарии, UI-тексты), идентификаторы английские.
- Integration-тесты требуют запущенный Docker (Testcontainers `quay.io/coreos/etcd:v3.5.21`).
- Frontend в worktree без `node_modules` — первый запуск начинается с `npm ci`.
- Формат etcd-ключей arch/02 §9.1 и протокол §9.2 НЕ меняем; validator/plan/OperationsModule не трогаем (кроме перечисленного).
- Mantine 9.5.2: у `SegmentedControl` нет корневого prop `label` (проверено по `SegmentedControl.d.ts`) — подпись «Тип базы данных» через `Text` над контролом (паттерн как «Ресурсы нод»); уточнение деталей spec §3.4, дизайн не меняет.

---

### Task 1: Зафиксировать arch-правки, spec и план (arch-first)

**Вход:** worktree `feat-cluster-sharded-toggle` содержит незакоммиченные правки Фазы 1: `arch/02-etcd-contract.md` (§9.1 семантика нешардированной, §9.3 поле `sharded`), `arch/03-panels.md` (§1.1 DTO), `docs/superpowers/2026-08-23-cluster-sharded-toggle/spec.md`, и этот `plan.md`.

**Действие:** закоммитить всё перечисленное одним коммитом в feature-ветку (код ещё не трогаем).

**Выход:** коммит в `feat-cluster-sharded-toggle`, чистый `git status`.

**Проверка:** `git log -1 --stat` показывает 4 файла (arch ×2, spec.md, plan.md).

**Связь со spec:** §2 «arch/-first»; требование main-агента.

- [ ] **Шаг 1.1. Проверить состав изменений**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-cluster-sharded-toggle
git status --short
```

Ожидание: ` M arch/02-etcd-contract.md`, ` M arch/03-panels.md`, `?? docs/superpowers/2026-08-23-cluster-sharded-toggle/`. Если состав отличается — STOP, вернуть main-агенту `{status: "NEEDS_CONTEXT", ...}`.

- [ ] **Шаг 1.2. Коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-cluster-sharded-toggle
git add arch/02-etcd-contract.md arch/03-panels.md docs/superpowers/2026-08-23-cluster-sharded-toggle
git commit -m "arch: sharded-переключатель создания кластера — 02 §9.1/§9.3 нешардированная БД (1x1), 03 §1.1 sharded + spec/plan"
```

- [ ] **Шаг 1.3. Проверка**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-cluster-sharded-toggle
git log -1 --stat && git status --short
```

Ожидание: коммит с 4 файлами; `git status --short` пуст.

---

### Task 2: `CreateClusterRequest.Sharded` + `Normalize()` (TDD, unit)

**Files:**
- Modify: `src/AdminPanel.Etcd/Writing/CreateClusterRequest.cs` (record + новый метод)
- Test: `src/tests/AdminPanel.UnitTests/CreateClusterPlanTests.cs` (новый класс `CreateClusterNormalizeTests` + новый тест в `ClusterCreatePlanTests`)

**Interfaces:**
- Consumes: существующий `CreateClusterRequest(string Name, int Buckets, int Shards, int Replicas, decimal RequestCpu, int RequestMem, int RequestDisk)`, `CreateClusterValidator.Validate`, `ClusterCreatePlan.Build`.
- Produces: `CreateClusterRequest(..., bool? Sharded = null)` — хвостовой опциональный параметр; метод `CreateClusterRequest Normalize()` (идемпотентный: `sharded ?? true`; при `false` → `this with { Sharded=false, Buckets=1, Shards=1 }`). Task 3 вызывает его в хендлере.

**Вход:** Task 1 закоммичен; тесты t12 зелёные.

**Действие:** RED — написать unit-тесты нормализации/валидации/вырожденного плана; GREEN — добавить поле и `Normalize()` в record.

**Выход:** `CreateClusterRequest` с `Sharded` и `Normalize()`; тесты нормализации зелёные; существующие тесты не тронуты (7-аргументные вызовы компилируются).

**Проверка:** `dotnet test src/tests/AdminPanel.UnitTests --filter "FullyQualifiedName~CreateCluster"` — все зелёные; `dotnet build src/AdminPanel.slnx` — 0 warnings.

**Связь со spec:** §3.2 (DTO+Normalize), §8.2 (единая точка нормализации), §8.3 (`bool? = null`).

- [ ] **Шаг 2.1. RED: тесты нормализации (новый класс в конец `CreateClusterPlanTests.cs`)**

Добавить в конец файла `src/tests/AdminPanel.UnitTests/CreateClusterPlanTests.cs`:

```csharp
// Нормализация запроса создания: arch/02 §9.3 — sharded=false → buckets/shards
// игнорируются и перезаписываются в 1/1; отсутствующий sharded = true.
public class CreateClusterNormalizeTests
{
    [Fact]
    public void Normalize_ShardedAbsent_TrueAndValuesKept()
    {
        // Arrange: легаси-запрос без sharded (null) — обратная совместимость
        var request = new CreateClusterRequest("shop", 4, 2, 2, 0.5m, 8, 100);

        // Act
        var normalized = request.Normalize();

        // Assert: sharded=true, buckets/shards без изменений
        normalized.Sharded.Should().BeTrue();
        normalized.Buckets.Should().Be(4);
        normalized.Shards.Should().Be(2);
    }

    [Fact]
    public void Normalize_ShardedFalse_OverwritesToOneAndOne()
    {
        // Arrange: мусорные buckets/shards при нешардированной — игнорируются
        var request = new CreateClusterRequest("solo", 9999, -5, 2, 1m, 8, 100, Sharded: false);

        // Act
        var normalized = request.Normalize();

        // Assert: вырожденный случай 1×1 (arch/02 §9.1)
        normalized.Sharded.Should().BeFalse();
        normalized.Buckets.Should().Be(1);
        normalized.Shards.Should().Be(1);
    }

    [Fact]
    public void Normalize_ShardedTrue_KeepsValues()
    {
        // Arrange
        var request = new CreateClusterRequest("shop", 8, 4, 2, 1m, 8, 100, Sharded: true);

        // Act
        var normalized = request.Normalize();

        // Assert
        normalized.Sharded.Should().BeTrue();
        normalized.Buckets.Should().Be(8);
        normalized.Shards.Should().Be(4);
    }

    [Fact]
    public void Normalize_Idempotent()
    {
        // Arrange
        var request = new CreateClusterRequest("solo", 7, 3, 2, 1m, 8, 100, Sharded: false);

        // Act/Assert: повторная нормализация ничего не меняет
        request.Normalize().Normalize().Should().Be(request.Normalize());
    }

    [Fact]
    public void Validate_AfterNormalizeSingleWithGarbage_NoErrors()
    {
        // Arrange: невалидные buckets/shards нормализованы ДО валидации
        var normalized = new CreateClusterRequest("solo", 0, 999, 2, 1m, 8, 100, Sharded: false).Normalize();

        // Act/Assert: ошибок по buckets/shards нет — сервер нормализовал (arch/02 §9.3)
        CreateClusterValidator.Validate(normalized).Should().BeEmpty();
    }
}
```

И новый тест в существующий класс `ClusterCreatePlanTests` (инвариант: план НЕ менялся, вырожденная структура строится сама):

```csharp
    [Fact]
    public void Build_NormalizedSingle_DegenerateStructure()
    {
        // Arrange: нешардированная — нормализованный запрос (мусор перезаписан в 1×1)
        var request = new CreateClusterRequest("solo", 999, 99, 2, 0.5m, 8, 100, Sharded: false).Normalize();

        // Act
        var plan = ClusterCreatePlan.Build(request, nowUnix: 1755900000);

        // Assert: config.buckets=1; единственный shard1 (ноды a/b); единственный
        // bucket_0 → shard1; заявки только /service/solo-shard1/* (arch/02 §9.1)
        plan.ConfigValue.Should().Contain("\"buckets\":1");
        var keys = plan.Puts.Select(p => p.Key).ToList();
        keys.Should().Contain(
        [
            "/clusters/solo/shards/shard1/replicas",
            "/clusters/solo/shards/shard1/nodes/shard1a/state",
            "/clusters/solo/shards/shard1/nodes/shard1b/state",
            "/clusters/solo/buckets/routing/bucket_0",
            "/clusters/solo/buckets/status/bucket_0",
        ]);
        keys.Where(k => k.Contains("shard2")).Should().BeEmpty();
        keys.Where(k => k.Contains("bucket_1")).Should().BeEmpty();
        plan.Puts.Single(p => p.Key == "/clusters/solo/buckets/routing/bucket_0").Value.Should().Be("shard1");
        plan.RequestKeys.Should().BeEquivalentTo(
        [
            "/service/solo-shard1/request_cpu",
            "/service/solo-shard1/request_mem",
            "/service/solo-shard1/request_disk",
        ]);
    }
```

- [ ] **Шаг 2.2. RED-проверка (не компилируется — это и есть RED)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-cluster-sharded-toggle
dotnet build src/AdminPanel.slnx 2>&1 | tail -5
```

Ожидание: ошибка компиляции — `CreateClusterRequest` не содержит `Sharded`/`Normalize` (CS1061/CS7036).

- [ ] **Шаг 2.3. GREEN: правка `src/AdminPanel.Etcd/Writing/CreateClusterRequest.cs`**

Record получить тело с методом (файл `CreateClusterLimits`/`CreateClusterValidator` ниже — не трогать):

```csharp
// Тело POST /api/clusters (arch/03 §1.1): биндится Minimal API как JSON.
// Sharded: отсутствует/null = true — обратная совместимость старых клиентов (arch/02 §9.3).
public sealed record CreateClusterRequest(
    string Name,
    int Buckets,
    int Shards,
    int Replicas,
    decimal RequestCpu,
    int RequestMem,
    int RequestDisk,
    bool? Sharded = null)
{
    // Нормализация (arch/02 §9.3): sharded=false → buckets/shards игнорируются и
    // перезаписываются в 1/1 (нешардированная БД — вырожденный случай §9.1);
    // отсутствующий sharded трактуется как true. Вызывается ДО Validate
    // (симметрично «Build — только после Validate»). Идемпотентна.
    public CreateClusterRequest Normalize()
    {
        var sharded = Sharded ?? true;
        return sharded
            ? this with { Sharded = sharded }
            : this with { Sharded = sharded, Buckets = 1, Shards = 1 };
    }
}
```

- [ ] **Шаг 2.4. GREEN-проверка**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-cluster-sharded-toggle
dotnet build src/AdminPanel.slnx && dotnet test src/tests/AdminPanel.UnitTests --filter "FullyQualifiedName~CreateClusterNormalize|FullyQualifiedName~ClusterCreatePlan|FullyQualifiedName~CreateClusterValidator"
```

Ожидание: сборка без warnings; все тесты PASS (включая старые t12).

- [ ] **Шаг 2.5. Коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-cluster-sharded-toggle
git add src/AdminPanel.Etcd/Writing/CreateClusterRequest.cs src/tests/AdminPanel.UnitTests/CreateClusterPlanTests.cs
git commit -m "feat(etcd): CreateClusterRequest.Normalize — sharded-флаг, нормализация нешардированной в 1x1 (arch/02 §9.3)"
```

---

### Task 3: `ClusterCreatedDto.Sharded` + `Normalize()` в хендлере (TDD, unit)

**Files:**
- Modify: `src/AdminPanel.Api/Operations/CreateClusterCommand.cs:14-23` (DTO), `:48-55` (Handle)
- Test: `src/tests/AdminPanel.UnitTests/CreateClusterCommandHandlerTests.cs` (+2 теста, +1 проверка в существующем)

**Interfaces:**
- Consumes: `CreateClusterRequest.Normalize()` из Task 2.
- Produces: `ClusterCreatedDto(string Name, string DbName, bool Sharded, int BucketsCount, int ShardsTotal, int Replicas, string RequestCpu, string RequestMem, string RequestDisk, string State)` — порядок важен, Task 4/5 читают JSON-поля `sharded`/`bucketsCount`/`shardsTotal`.

**Вход:** Task 2 влит (зелёный).

**Действие:** RED — тесты хендлера на DTO нешардированной/legacy; GREEN — `bool Sharded` в DTO (после `DbName`), `request = request.Normalize();` первой строкой `Handle`.

**Выход:** хендлер нормализует запрос до валидации; DTO ответа содержит `sharded`.

**Проверка:** фильтр по handler-тестам зелёный; сборка 0 warnings.

**Связь со spec:** §3.2 (конвейер `Normalize → Validate → Build → DTO`), §8.2.

- [ ] **Шаг 3.1. RED: тесты в `CreateClusterCommandHandlerTests.cs`**

В существующий `Handle_ValidRequest_ClaimsThenPutsAndReturnsDto` добавить после `result.Value.State.Should().Be("NOT_INITIALIZED");`:

```csharp
        result.Value.Sharded.Should().BeTrue(); // legacy-запрос без sharded = true
```

Новые тесты в конец класса:

```csharp
    [Fact]
    public async Task Handle_SingleCluster_ReturnsDegenerateDto()
    {
        // Arrange: нешардированная — buckets/shards не переданы (0) и не важны
        var (handler, gateway, _) = NewHandler();

        // Act
        var result = await handler.Handle(new CreateClusterCommand(
            new("solo", 0, 0, 2, 0.5m, 8, 100, Sharded: false)), CancellationToken.None);

        // Assert: DTO вырожденный — sharded=false, 1/1; ключи только solo-shard1
        result.IsSuccess.Should().BeTrue();
        result.Value.Sharded.Should().BeFalse();
        result.Value.BucketsCount.Should().Be(1);
        result.Value.ShardsTotal.Should().Be(1);
        gateway.Puts.Where(k => k.Contains("shard2")).Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_ShardedFalseWithGarbage_NormalizesAndSucceeds()
    {
        // Arrange: sharded=false + мусорные buckets/shards — игнорируются
        var (handler, _, _) = NewHandler();

        // Act
        var result = await handler.Handle(new CreateClusterCommand(
            new("solo2", 99999, -3, 2, 1m, 8, 100, Sharded: false)), CancellationToken.None);

        // Assert: не 400-валидация, а успешная вырожденная запись
        result.IsSuccess.Should().BeTrue();
        result.Value.BucketsCount.Should().Be(1);
        result.Value.ShardsTotal.Should().Be(1);
    }
```

- [ ] **Шаг 3.2. RED-проверка**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-cluster-sharded-toggle
dotnet build src/AdminPanel.slnx 2>&1 | tail -5
```

Ожидание: ошибка компиляции — у `ClusterCreatedDto` нет `Sharded` (CS1061).

- [ ] **Шаг 3.3. GREEN: правка `src/AdminPanel.Api/Operations/CreateClusterCommand.cs`**

DTO (новое поле — третьим, после `DbName`):

```csharp
// Ответ 201 POST /api/clusters (arch/03 §1.1).
public sealed record ClusterCreatedDto(
    string Name,
    string DbName,
    bool Sharded,
    int BucketsCount,
    int ShardsTotal,
    int Replicas,
    string RequestCpu,
    string RequestMem,
    string RequestDisk,
    string State);
```

В `Handle` — нормализация первой операцией (после `var request = command.Request;`) и DTO из нормализованного запроса:

```csharp
        var request = command.Request;

        // 0) Нормализация: sharded=false → 1/1; отсутствует = true (arch/02 §9.3).
        //    ДО Validate — валидатор и план работают с каноническим запросом.
        request = request.Normalize();
```

```csharp
        return Result<ClusterCreatedDto>.Success(new ClusterCreatedDto(
            request.Name, request.Name, request.Sharded!.Value, request.Buckets, request.Shards,
            request.Replicas, plan.CanonicalCpu, plan.CanonicalMem, plan.CanonicalDisk,
            ClusterCreatePlan.NotInitialized));
```

- [ ] **Шаг 3.4. GREEN-проверка**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-cluster-sharded-toggle
dotnet build src/AdminPanel.slnx && dotnet test src/tests/AdminPanel.UnitTests --filter "FullyQualifiedName~CreateClusterCommandHandler"
```

Ожидание: сборка без warnings; все PASS.

- [ ] **Шаг 3.5. Коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-cluster-sharded-toggle
git add src/AdminPanel.Api/Operations/CreateClusterCommand.cs src/tests/AdminPanel.UnitTests/CreateClusterCommandHandlerTests.cs
git commit -m "feat(api): ClusterCreatedDto.sharded + Normalize до валидации в CreateClusterCommandHandler"
```

---

### Task 4: Integration-тесты API против реального etcd

**Files:**
- Test: `src/tests/AdminPanel.IntegrationTests/CreateClusterApiTests.cs` (+2 факта, +1 проверка в существующем)

**Interfaces:**
- Consumes: HTTP-контракт `POST /api/clusters` с телом `{"name","sharded"?,"buckets"?,"shards"?,"replicas","requestCpu","requestMem","requestDisk"}`; `EtcdTestHarness.NewGateway()`, `fixture.Endpoint`, `ApiTestLogin.LoginAsync(_factory)`, `SetLiveSnapshot()`.

**Вход:** Tasks 2–3 влиты; Docker запущен.

**Действие:** обновить существующий кейс (явная backward-compat проверка), добавить кейс нешардированной (тело без buckets/shards) и кейс мусора.

**Выход:** API-уровень закрывает критерии приёмки spec §7.2–7.4 (кроме e2e-части).

**Проверка:** `dotnet test src/tests/AdminPanel.IntegrationTests --filter "FullyQualifiedName~CreateClusterApiTests"` — зелёные.

**Связь со spec:** §5 фаза 2; §7.2–7.4.

- [ ] **Шаг 4.1. Обновить существующий `Create_Valid_WritesContractKeysToEtcd`**

После `dto.GetProperty("state").GetString().Should().Be("NOT_INITIALIZED");` добавить:

```csharp
        // Тело без sharded — обратная совместимость: ответ трактует как sharded=true
        dto.GetProperty("sharded").GetBoolean().Should().BeTrue();
```

- [ ] **Шаг 4.2. Добавить кейс нешардированной (новый факт в класс `CreateClusterApiTests`)**

```csharp
    [Fact]
    public async Task Create_SingleWithoutBucketsShards_WritesDegenerateStructure()
    {
        // Arrange
        SetLiveSnapshot();
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act: нешардированная — buckets/shards в теле отсутствуют вовсе (arch/03 §1.1)
        using var response = await client.PostAsJsonAsync(
            "/api/clusters",
            new { name = "solo", sharded = false, replicas = 2, requestCpu = 0.5m, requestMem = 8, requestDisk = 100 },
            TestContext.Current.CancellationToken);

        // Assert: 201 + вырожденный DTO (sharded=false, 1/1)
        response.StatusCode.Should().Be(HttpStatusCode.Created);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        dto.GetProperty("sharded").GetBoolean().Should().BeFalse();
        dto.GetProperty("bucketsCount").GetInt32().Should().Be(1);
        dto.GetProperty("shardsTotal").GetInt32().Should().Be(1);

        // Ключи в etcd — ровно вырожденная структура arch/02 §9.1
        var gateway = EtcdTestHarness.NewGateway();
        var range = await gateway.RangeAsync(fixture.Endpoint, "/clusters/solo/", TestContext.Current.CancellationToken);
        range.Value.Select(kv => kv.Key).Should().BeEquivalentTo(
        [
            "/clusters/solo/config",
            "/clusters/solo/shards/shard1/replicas",
            "/clusters/solo/shards/shard1/nodes/shard1a/state",
            "/clusters/solo/shards/shard1/nodes/shard1b/state",
            "/clusters/solo/buckets/routing/bucket_0",
            "/clusters/solo/buckets/status/bucket_0",
        ]);
        range.Value.Single(kv => kv.Key == "/clusters/solo/config").Value.Should().Contain("\"buckets\":1");
        var requests = await gateway.RangeAsync(fixture.Endpoint, "/service/solo-", TestContext.Current.CancellationToken);
        requests.Value.Select(kv => kv.Key).Should().BeEquivalentTo(
        [
            "/service/solo-shard1/request_cpu",
            "/service/solo-shard1/request_mem",
            "/service/solo-shard1/request_disk",
        ]);
    }

    [Fact]
    public async Task Create_SingleWithGarbageBuckets_IgnoresAndNormalizes()
    {
        // Arrange
        SetLiveSnapshot();
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act: sharded=false + невалидные buckets/shards — сервер игнорирует
        using var response = await client.PostAsJsonAsync(
            "/api/clusters",
            new { name = "solo2", sharded = false, buckets = 99999, shards = -3, replicas = 2, requestCpu = 1m, requestMem = 8, requestDisk = 100 },
            TestContext.Current.CancellationToken);

        // Assert: 201 (не 400) и вырожденная структура — без bucket_1/shard2
        response.StatusCode.Should().Be(HttpStatusCode.Created);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        dto.GetProperty("bucketsCount").GetInt32().Should().Be(1);
        var gateway = EtcdTestHarness.NewGateway();
        var range = await gateway.RangeAsync(fixture.Endpoint, "/clusters/solo2/", TestContext.Current.CancellationToken);
        range.Value.Select(kv => kv.Key).Where(k => k.Contains("bucket_1") || k.Contains("shard2"))
            .Should().BeEmpty();
    }
```

- [ ] **Шаг 4.3. Проверка**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-cluster-sharded-toggle
dotnet test src/tests/AdminPanel.IntegrationTests --filter "FullyQualifiedName~CreateClusterApiTests"
```

Ожидание: все PASS, включая новые `Create_Single*` и обновлённый `Create_Valid*`. Если падает с ошибкой Docker/Testcontainers — поднять Docker и повторить.

- [ ] **Шаг 4.4. Коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-cluster-sharded-toggle
git add src/tests/AdminPanel.IntegrationTests/CreateClusterApiTests.cs
git commit -m "test(api): integration — нешардированная без buckets/shards пишет 1x1, мусор игнорируется, backward-compat sharded:true"
```

---

### Task 5: Фронтенд — DTO и модалка

**Files:**
- Modify: `frontend/src/api/dto.ts:11-31` (оба интерфейса)
- Modify: `frontend/src/pages/clusters/ClusterCreateModal.tsx` (целевой вид ниже)

**Interfaces:**
- Consumes: `createCluster(request: CreateClusterRequestDto)` из `api/queries.ts` (не меняется); HTTP-контракт Tasks 2–3.
- Produces: `CreateClusterRequestDto { name; sharded: boolean; buckets?: number; shards?: number; replicas; requestCpu; requestMem; requestDisk }`; `ClusterCreatedDto` c `sharded: boolean`.

**Вход:** Tasks 2–4 влиты (API реально отвечает `sharded`).

**Действие:** обновить типы; переписать модалку: `FormState` (значения переживают переключение), `SegmentedControl`, условный блок «Шардирование», «Реплики» отдельным рядом, зеркальная валидация (buckets/shards только при `form.sharded`), сборка body в `submit`.

**Выход:** модалка по spec §3.4; сборка фронтенда без ошибок TS.

**Проверка:** `npm ci` (первый раз) → `npm run typecheck` → `npm run build`.

**Связь со spec:** §3.3–3.4; решения §8.4–8.6, §8.8–8.9.

- [ ] **Шаг 5.1. `frontend/src/api/dto.ts` — заменить оба интерфейса**

```ts
// POST /api/clusters — тело и ответ (arch/03 §1.1).
// sharded: фронт передаёт всегда; buckets/shards — только при sharded=true
// (для нешардированной не запрашиваются вовсе, сервер нормализует в 1/1).
export interface CreateClusterRequestDto {
  name: string;
  sharded: boolean;
  buckets?: number;
  shards?: number;
  replicas: number;
  requestCpu: number;
  requestMem: number;
  requestDisk: number;
}

export interface ClusterCreatedDto {
  name: string;
  dbName: string;
  sharded: boolean;
  bucketsCount: number;
  shardsTotal: number;
  replicas: number;
  requestCpu: string;
  requestMem: string;
  requestDisk: string;
  state: ClusterStateName;
}
```

- [ ] **Шаг 5.2. `frontend/src/pages/clusters/ClusterCreateModal.tsx` — целевой вид файла**

```tsx
// Форма создания кластера — единственная мутация панели (spec t12 §3.8).
// Переключатель типа БД (spec cluster-sharded-toggle §3.4): нешардированная =
// вырожденный случай 1×1 — поля бакетов/шардов не запрашиваются вовсе.
// Клиентская валидация — зеркало серверной (arch/02 §9.3); сервер — истина.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import {
  Alert,
  Box,
  Button,
  Group,
  Modal,
  NumberInput,
  SegmentedControl,
  Stack,
  Text,
  TextInput,
} from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { createCluster } from '../../api/queries';
import type { CreateClusterRequestDto } from '../../api/dto';

// Границы — зеркало CreateClusterLimits (arch/02 §9.3).
const NAME_RE = /^[a-z][a-z0-9_]{0,62}$/;

// Состояние формы: buckets/shards живут всегда — переживают переключение
// типа туда-обратно (блок лишь скрывается, spec §3.4).
interface FormState {
  name: string;
  sharded: boolean;
  buckets: number;
  shards: number;
  replicas: number;
  requestCpu: number;
  requestMem: number;
  requestDisk: number;
}

const EMPTY: FormState = {
  name: '',
  sharded: true, // дефолт — текущее поведение модалки (spec §8.5)
  buckets: 16,
  shards: 2,
  replicas: 2,
  requestCpu: 2,
  requestMem: 8,
  requestDisk: 100,
};

export function ClusterCreateModal({ opened, onClose }: { opened: boolean; onClose: () => void }) {
  const queryClient = useQueryClient();
  const [form, setForm] = useState<FormState>(EMPTY);
  const [fieldErrors, setFieldErrors] = useState<Record<string, string>>({});
  const set = <K extends keyof FormState>(key: K, value: FormState[K]) =>
    setForm((f) => ({ ...f, [key]: value }));

  const mutation = useMutation({
    mutationFn: createCluster,
    onSuccess: async () => {
      // Список кластеров обновит следующий тик refresher'а — инвалидация ключа
      onClose();
      setForm(EMPTY);
      await queryClient.invalidateQueries({ queryKey: ['clusters'] });
    },
  });

  // Зеркало серверной валидации: по полям, до отправки (spec t12 §2).
  // buckets/shards проверяются только для шардированной (spec §3.4).
  function validate(): boolean {
    const errors: Record<string, string> = {};
    if (!NAME_RE.test(form.name)) errors.name = 'a-z, 0-9, _; начинается с буквы; без дефиса';
    if (form.sharded) {
      if (!Number.isInteger(form.buckets) || form.buckets < 1 || form.buckets > 8192)
        errors.buckets = 'целое 1..8192';
      if (!Number.isInteger(form.shards) || form.shards < 1 || form.shards > 128 || form.shards > form.buckets)
        errors.shards = 'целое 1..128 и не больше бакетов';
    }
    if (!Number.isInteger(form.replicas) || form.replicas < 1 || form.replicas > 26)
      errors.replicas = 'целое 1..26 (1 = только мастер)';
    if (form.requestCpu < 0.01 || form.requestCpu > 64) errors.requestCpu = '0.01..64';
    if (!Number.isInteger(form.requestMem) || form.requestMem < 1 || form.requestMem > 65536)
      errors.requestMem = 'целое 1..65536';
    if (!Number.isInteger(form.requestDisk) || form.requestDisk < 1 || form.requestDisk > 65536)
      errors.requestDisk = 'целое 1..65536';
    setFieldErrors(errors);
    return Object.keys(errors).length === 0;
  }

  function submit() {
    if (!validate()) return;
    // Тело запроса: при sharded=false поля бакетов/шардов не передаются вовсе —
    // сервер нормализует в 1/1 (arch/02 §9.3).
    const body: CreateClusterRequestDto = form.sharded
      ? { ...form }
      : {
          name: form.name,
          sharded: false,
          replicas: form.replicas,
          requestCpu: form.requestCpu,
          requestMem: form.requestMem,
          requestDisk: form.requestDisk,
        };
    mutation.mutate(body);
  }

  // Ошибка сервера: 409 «имя занято» / 400 по полям / 503 «etcd» (ProblemDetails).
  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <Modal opened={opened} onClose={onClose} title="Создать кластер" centered>
      <Stack gap="sm">
        <TextInput
          label="Имя кластера"
          description="уникально; dbname = имя"
          placeholder="shop"
          value={form.name}
          error={fieldErrors.name}
          onChange={(e) => set('name', e.currentTarget.value)}
        />
        {/* Тип БД: Mantine 9.5 SegmentedControl без label-prop — подпись Text
            над контролом (паттерн как «Ресурсы нод»), выбор — spec §8.4 */}
        <Stack gap={4}>
          <Text size="sm" fw={500}>Тип базы данных</Text>
          <SegmentedControl
            fullWidth
            data={[
              { value: 'sharded', label: 'Шардированная' },
              { value: 'single', label: 'Нешардированная' },
            ]}
            value={form.sharded ? 'sharded' : 'single'}
            onChange={(v) => set('sharded', v === 'sharded')}
          />
        </Stack>
        {form.sharded ? (
          <Box withBorder radius="md" p="sm">
            <Text size="sm" fw={500} mb="xs">Шардирование</Text>
            <Group grow gap="sm">
              <NumberInput label="Бакеты" min={1} max={8192} value={form.buckets}
                error={fieldErrors.buckets} onChange={(v) => set('buckets', Number(v ?? 0))} />
              <NumberInput label="Шарды" min={1} max={128} value={form.shards}
                error={fieldErrors.shards} onChange={(v) => set('shards', Number(v ?? 0))} />
            </Group>
          </Box>
        ) : null}
        {/* Реплики — общий ряд обоих типов; description больше не ломает
            выравнивание соседей (NBSP-хак d2549ba удалён, spec §8.8) */}
        <NumberInput label="Реплики" min={1} max={26} value={form.replicas}
          description="2 = мастер + реплика"
          error={fieldErrors.replicas} onChange={(v) => set('replicas', Number(v ?? 0))} />
        <Text size="sm" c="dimmed">Ресурсы нод (заявка, на каждую ноду)</Text>
        <Group grow gap="sm">
          <NumberInput label="CPU (ядра)" min={0.01} max={64} step={0.1} decimalScale={2}
            value={form.requestCpu} error={fieldErrors.requestCpu}
            onChange={(v) => set('requestCpu', Number(v ?? 0))} />
          <NumberInput label="Память (GiB)" min={1} max={65536} value={form.requestMem}
            error={fieldErrors.requestMem} onChange={(v) => set('requestMem', Number(v ?? 0))} />
          <NumberInput label="Диск (GiB)" min={1} max={65536} value={form.requestDisk}
            error={fieldErrors.requestDisk} onChange={(v) => set('requestDisk', Number(v ?? 0))} />
        </Group>
        {serverError !== null ? (
          <Alert color={serverError.status === 409 ? 'yellow' : 'red'} variant="light">
            {serverError.status === 409
              ? 'Имя уже занято — выберите другое'
              : serverError.status === 400
                ? (serverError.detail ?? 'Проверьте параметры')
                : 'etcd недоступен — повторите позже'}
          </Alert>
        ) : null}
        <Group justify="flex-end" mt="xs">
          <Button variant="default" onClick={onClose}>Отмена</Button>
          <Button loading={mutation.isPending} onClick={submit}>Создать</Button>
        </Group>
      </Stack>
    </Modal>
  );
}
```

- [ ] **Шаг 5.3. Установить зависимости (worktree без node_modules) и проверить типы**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-cluster-sharded-toggle/frontend
npm ci && npm run typecheck
```

Ожидание: `tsc --noEmit` без ошибок (в т.ч. `CreateClusterRequestDto` с опциональными `buckets?/shards?`).

- [ ] **Шаг 5.4. Прод-сборка**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-cluster-sharded-toggle/frontend
npm run build
```

Ожидание: `vite build` успешно.

- [ ] **Шаг 5.5. Коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-cluster-sharded-toggle
git add frontend/src/api/dto.ts frontend/src/pages/clusters/ClusterCreateModal.tsx
git commit -m "feat(frontend): переключатель типа БД в модалке создания — SegmentedControl, условный блок Шардирование, реплики отдельным рядом"
```

---

### Task 6: e2e-чек 15 — кейс нешардированной + явный backward-compat

**Files:**
- Modify: `dev-stand/checks/15-cluster-create.sh` (дополнение после smoke-блоков)

**Interfaces:**
- Consumes: работающий dev-стенд (`ADMINPANEL_URL`, default `http://localhost:5050`), etcd через `docker compose exec etcd etcdctl` (функция `ect` в чеке).

**Вход:** Tasks 2–5 влиты; стенд запущен (если стенд не поднимается в окружении исполнения — отметить шаг как отложенный и выполнить в Task 7 при прогоне; правка скрипта не требует стенда).

**Действие:** в smoke-сценарии добавить явную проверку `sharded:true` (backward-compat: тело без `sharded`); добавить кейс `solo` — нешардированная без buckets/shards, вырожденная структура, видимость панелью.

**Выход:** чек покрывает оба типа создания; существующие проверки smoke не изменены по смыслу.

**Проверка:** `bash -n` (синтаксис); при доступном стенде — полный прогон чека.

**Связь со spec:** §3.6; §7.2–7.3, 7.6.

- [ ] **Шаг 6.1. Дополнить smoke-сценарий backward-compat-проверкой**

В `dev-stand/checks/15-cluster-create.sh` после строки `echo "  создан: …"` (вывод jq) добавить:

```bash
# Assert: smoke-тело без sharded — заодно регрессия обратной совместимости:
# отсутствующее поле трактуется как sharded=true (arch/02 §9.3)
jq -e '.sharded == true' /tmp/t12-create.json >/dev/null \
  || { echo "❌ ответ без поля sharded не вернул sharded=true"; exit 1; }
```

- [ ] **Шаг 6.2. Добавить кейс solo перед финальной строкой `echo "✓ 15-cluster-create…"`**

```bash
# --- Кейс нешардированной (spec cluster-sharded-toggle §3.6): sharded=false, без buckets/shards ---
ect del --prefix /clusters/solo >/dev/null
for k in request_cpu request_mem request_disk; do
  ect del "/service/solo-shard1/$k" >/dev/null
done

code="$(curl -s -o /tmp/t12-create-solo.json -w '%{http_code}' -b "$JAR" -X POST "$BASE/api/clusters" \
  -H 'Content-Type: application/json' \
  -d '{"name":"solo","sharded":false,"replicas":2,"requestCpu":0.5,"requestMem":8,"requestDisk":100}')"
[ "$code" = 201 ] || { echo "❌ POST /api/clusters (solo, sharded=false) = $code: $(cat /tmp/t12-create-solo.json)"; exit 1; }
echo "  создан solo: $(jq -c '{name,sharded,bucketsCount,shardsTotal,state}' /tmp/t12-create-solo.json)"

# Вырожденная структура 1×1 (arch/02 §9.1): один бакет, один шард, заявки только shard1
[ "$(ect get /clusters/solo/config --print-value-only | jq -r '.buckets')" = "1" ] \
  || { echo "❌ solo config.buckets != 1"; exit 1; }
[ "$(ect get /clusters/solo/buckets/routing/bucket_0 --print-value-only)" = "shard1" ] \
  || { echo "❌ solo routing bucket_0 != shard1"; exit 1; }
[ -z "$(ect get /clusters/solo/buckets/routing/bucket_1 --print-value-only)" ] \
  || { echo "❌ solo: появился лишний bucket_1"; exit 1; }
[ -z "$(ect get /service/solo-shard2/request_cpu --print-value-only)" ] \
  || { echo "❌ solo: появились заявки shard2"; exit 1; }
jq -e '.sharded == false and .bucketsCount == 1 and .shardsTotal == 1' /tmp/t12-create-solo.json >/dev/null \
  || { echo "❌ solo-ответ не вырожденный (sharded/bucketsCount/shardsTotal)"; exit 1; }
echo "  etcd solo: вырожденная структура 1x1 — контракт §9.1 соблюдён"

# Панель видит solo (следующий тик ≤ 3 c + polling): 1 бакет, 1 шард
for i in $(seq 1 15); do
  curl -fsS -b "$JAR" "$BASE/api/clusters/solo" | jq -e \
    '.state=="NOT_INITIALIZED" and .bucketsCount==1 and (.shards|length)==1' >/dev/null && break
  sleep 1
done
curl -fsS -b "$JAR" "$BASE/api/clusters/solo" | jq -e \
  '.state=="NOT_INITIALIZED" and .bucketsCount==1 and (.shards|length)==1' >/dev/null \
  || { echo "❌ /api/clusters/solo: вырожденная структура не видна"; exit 1; }
echo "  /api/clusters/solo: 1 бакет × 1 шард, NOT_INITIALIZED"
```

- [ ] **Шаг 6.3. Синтаксис + (если стенд поднят) прогон**

```bash
bash -n /Users/demakaev/ZCodeProject/worktrees/feat-cluster-sharded-toggle/dev-stand/checks/15-cluster-create.sh && echo OK
# Прогон на работающем стенде (если поднят):
cd /Users/demakaev/ZCodeProject/worktrees/feat-cluster-sharded-toggle/dev-stand && ./checks/15-cluster-create.sh
```

Ожидание: `OK`; при прогоне — финальная строка `✓ 15-cluster-create: создание кластера e2e прошло`.

- [ ] **Шаг 6.4. Коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-cluster-sharded-toggle
git add dev-stand/checks/15-cluster-create.sh
git commit -m "chore(stand): e2e-чек 15 — кейс solo (нешардированная 1x1) + явная backward-compat проверка sharded:true"
```

---

### Task 7: Полная верификация ветки

**Вход:** Tasks 1–6 закоммичены.

**Действие:** прогнать все проверки целиком; убедиться в чистоте дерева.

**Выход:** зелёная ветка, готовая к ревью/мерж-гейту main-агента.

**Проверка:** команды ниже; все успешны.

**Связь со spec:** §5 фаза 5; §7.7.

- [ ] **Шаг 7.1. Сборка решения (0 warnings = 0 ошибок, TreatWarningsAsErrors)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-cluster-sharded-toggle
dotnet build src/AdminPanel.slnx
```

- [ ] **Шаг 7.2. Все бэкенд-тесты (unit + integration, нужен Docker)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-cluster-sharded-toggle
dotnet test src/AdminPanel.slnx
```

- [ ] **Шаг 7.3. Фронтенд: типы + прод-сборка**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-cluster-sharded-toggle/frontend
npm run typecheck && npm run build
```

- [ ] **Шаг 7.4. e2e-чек на стенде (если стенд доступен; иначе зафиксировать как TODO для main-агента)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-cluster-sharded-toggle/dev-stand && ./checks/15-cluster-create.sh
```

- [ ] **Шаг 7.5. Чистота дерева и состав ветки**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-cluster-sharded-toggle
git status --short && git log --oneline main..HEAD
```

Ожидание: статус пуст; 6 коммитов (arch+spec+plan, etcd-normalize, api-handler, integration, frontend, stand).

- [ ] **Шаг 7.6. Ручная UI-проверка модалки (spec §7.1/7.6; если браузер недоступен исполнителю — зафиксировать TODO для main-агента)**

На работающей панели (стенд): открыть «Кластеры» → «Создать» → переключить тип туда-обратно (значения бакетов/шардов сохраняются, блок «Шардирование» скрывается/показывается) → создать нешардированную `solo_manual` → убедиться: в списке кластеров 1 бакет/1 шард, вкладки Бакеты/Шарды/Ноды не пусты, overview без ошибок. После проверки удалить `solo_manual` через etcdctl (кластер неинициализирован, панель не удаляет).
