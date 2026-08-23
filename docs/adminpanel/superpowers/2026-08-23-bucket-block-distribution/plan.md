# bucket-block-distribution — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Два требования spec: (1) распределение бакетов по шардам при создании кластера непрерывными блоками по канону `floor((2i+1)·S/(2N))` вместо round-robin; (2) скрытие вкладки «Бакеты» в UI для нешардированных кластеров (`ClusterDto.sharded`).

**Architecture:** Канон — публичная чистая функция `ClusterCreatePlan.OwnerShard(i, N, S)` (целочисленная, без float), заменяющая `i % S` в `Build`; признак отображения — вычисляемое серверное поле `ClusterDto.Sharded` (`false ⟺ 1 бакет и ≤1 шард`), по которому фронт скрывает вкладку. Формат etcd-ключей, протокол записи §9.2 и читатели контракта не меняются.

**Tech Stack:** .NET 10 (`Nullable=enable`, `TreatWarningsAsErrors=true`, ImplicitUsings), xUnit + FluentAssertions, Testcontainers etcd, React 19 + Mantine 9.5 + TypeScript (tsc + vite), bash e2e-чек на docker compose стенде.

**Spec:** `docs/superpowers/2026-08-23-bucket-block-distribution/spec.md` — план аргументирует от spec; исполнители читают оба. Контракт уже поправлен в arch (коммит a6aebef): `arch/02-etcd-contract.md` §9.1 + §9.1.1, `arch/03-panels.md` §2/§3.

## Global Constraints

- Работать только в worktree `/Users/demakaev/ZCodeProject/worktrees/feat-bucket-block-distribution`, ветка `feat-bucket-block-distribution`; коммит после каждой задачи (в feature-ветках коммитить свободно).
- Канон распределения неизменяем: 10×3 → **3+4+3** (shard1: бакеты 0,1,2; shard2: 3,4,5,6; shard3: 7,8,9; остаток — среднему шарду) — spec §2.1, arch/02 §9.1.1.
- Формула: `шард(i) = (2·i + 1) · S / (2·N) + 1` (целочисленное деление, 1-based) — без float; max `(2·8191+1)·128 = 2 097 024` — в int.
- `ClusterDto.Sharded = !(BucketsCount == 1 && Shards.Count <= 1)` — arch/03 §2 (оговорка: шардированный 1×1 отображается как нешардированный; принято).
- Формат etdc-ключей §9.1 и протокол §9.2 не меняются; меняются только значения routing у **новых** созданий; существующие кластеры не перезаписываются.
- Скрывается только вкладка «Бакеты» (Tabs.Tab + Tabs.Panel); вкладки «Переезды»/«Heals», шапка «Бакеты: N», JSON-массив `buckets` в ответе API — без изменений.
- Язык: комментарии и тексты UI русские, идентификаторы английские; тесты — с AAA-комментариями (`// Arrange`, `// Act`, `// Assert`); стиль окружающего кода.
- Сборка: 0 warnings (`TreatWarningsAsErrors=true` — любой warning = ошибка).
- Команды выполнять из корня worktree (`/Users/demakaev/ZCodeProject/worktrees/feat-bucket-block-distribution`), если не указано иное.
- ⚠️ Решение — `src/AdminPanel.slnx`: в корне worktree нет .slnx/.csproj, голые `dotnet build` / `dotnet test` из корня падают (MSB1003). Всегда указывать путь: `dotnet build src/AdminPanel.slnx`, `dotnet test src/AdminPanel.slnx` либо путь конкретного тест-проекта.

---

### Task 1: Канон `OwnerShard` — блочное распределение в `ClusterCreatePlan`

**Files:**
- Modify: `src/AdminPanel.Etcd/Writing/ClusterCreatePlan.cs` (метод `Build`, строки ~52–60; новый метод)
- Test: `src/tests/AdminPanel.UnitTests/CreateClusterPlanTests.cs` (класс `ClusterCreatePlanTests`)

**Interfaces:**
- Consumes: `CreateClusterRequest` (поля `Buckets`, `Shards` — после Normalize это канонические N≥S≥1).
- Produces: `public static int OwnerShard(int bucket, int buckets, int shards)` — 1-based номер шарда (`1..shards`); используется `Build` и тестами Tasks 1; на неё же опираются интеграционные ожидания Task 3.

- [ ] **Step 1.1: Написать падающие тесты формулы (новые члены класса `ClusterCreatePlanTests`)**

Вход: файл `src/tests/AdminPanel.UnitTests/CreateClusterPlanTests.cs`, класс `ClusterCreatePlanTests` (после `Build_NormalizedSingle_DegenerateStructure`).

Действие — добавить:

```csharp
// Канон распределения (arch/02 §9.1.1): непрерывные блоки, «бакет к ближайшему
// центру отрезка» — floor((2i+1)·S/(2N)); таблица и свойства — spec §2.1.
[Fact]
public void OwnerShard_CanonicalTenByThree_BlocksThreeFourThree()
{
    // Arrange: канон пользователя — 10×3, остаток СРЕДНЕМУ шарду (spec §2.1)
    // Act
    var owners = Enumerable.Range(0, 10)
        .Select(i => ClusterCreatePlan.OwnerShard(i, 10, 3)).ToArray();

    // Assert: shard1={0,1,2}, shard2={3,4,5,6}, shard3={7,8,9} — расклад 3+4+3
    owners.Should().Equal(1, 1, 1, 2, 2, 2, 2, 3, 3, 3);
}

[Theory]
[InlineData(4, 2, new[] { 1, 1, 2, 2 })]
[InlineData(5, 2, new[] { 1, 1, 2, 2, 2 })]
[InlineData(7, 3, new[] { 1, 1, 2, 2, 2, 3, 3 })]
[InlineData(8, 3, new[] { 1, 1, 1, 2, 2, 3, 3, 3 })]
[InlineData(9, 4, new[] { 1, 1, 2, 2, 3, 3, 3, 4, 4 })]
[InlineData(3, 3, new[] { 1, 2, 3 })]
[InlineData(1, 1, new[] { 1 })]
public void OwnerShard_Table_MatchesSpec(int buckets, int shards, int[] expected)
{
    // Arrange: строки таблицы распределений spec §2.1
    // Act
    var owners = Enumerable.Range(0, buckets)
        .Select(i => ClusterCreatePlan.OwnerShard(i, buckets, shards));

    // Assert
    owners.Should().Equal(expected);
}

[Theory]
[InlineData(10, 3)]
[InlineData(4, 2)]
[InlineData(5, 2)]
[InlineData(7, 3)]
[InlineData(8, 3)]
[InlineData(9, 4)]
[InlineData(16, 3)]
[InlineData(100, 7)]
[InlineData(3, 3)]
[InlineData(1, 1)]
[InlineData(8192, 128)]
public void OwnerShard_Properties_ContinuousBalancedNonEmpty(int buckets, int shards)
{
    // Arrange: свойства формулы §9.1.1 при допустимых N ≥ S ≥ 1
    // Act
    var owners = Enumerable.Range(0, buckets)
        .Select(i => ClusterCreatePlan.OwnerShard(i, buckets, shards)).ToArray();

    // Assert: размеры шардов — сумма N, размах не более 1
    var sizes = Enumerable.Range(1, shards)
        .Select(k => owners.Count(o => o == k)).ToArray();
    sizes.Sum().Should().Be(buckets);
    (sizes.Max() - sizes.Min()).Should().BeLessThanOrEqualTo(1);

    // Assert: каждый шард непуст, его бакеты — непрерывный диапазон (блок)
    foreach (var k in Enumerable.Range(1, shards))
    {
        var ids = Enumerable.Range(0, buckets).Where(i => owners[i] == k).ToArray();
        ids.Should().NotBeEmpty();
        (ids.Last() - ids.First() + 1).Should().Be(ids.Length);
    }
}

[Fact]
public void OwnerShard_LargeSizes_ExactSplits()
{
    // Arrange/Act/Assert: точные расклады больших N×S из таблицы spec §2.1
    BlockSizes(16, 3).Should().Equal(5, 6, 5);
    BlockSizes(100, 7).Should().Equal(14, 15, 14, 14, 14, 15, 14);
    BlockSizes(8192, 128).Should().Match(l => l.Count == 128 && l.All(s => s == 64));

    static int[] BlockSizes(int buckets, int shards)
        => Enumerable.Range(1, shards)
            .Select(k => Enumerable.Range(0, buckets)
                .Count(i => ClusterCreatePlan.OwnerShard(i, buckets, shards) == k))
            .ToArray();
}
```

Выход: тесты ссылаются на несуществующий `ClusterCreatePlan.OwnerShard`.

Проверка: `dotnet test src/tests/AdminPanel.UnitTests --filter "FullyQualifiedName~ClusterCreatePlanTests"`
Ожидание: **ошибка компиляции** `CS0117: 'ClusterCreatePlan' does not contain a definition for 'OwnerShard'` (это и есть «падение» TDD-цикла для новой функции).

Связь со spec: §2/§2.1 (таблица), §4.2, критерии приёмки 1–2.

- [ ] **Step 1.2: Реализовать `OwnerShard` в `ClusterCreatePlan`**

Вход: `src/AdminPanel.Etcd/Writing/ClusterCreatePlan.cs`.

Действие — после закрывающей `}` метода `Build` (перед вложенным record `ConfigJson`) добавить:

```csharp
// Распределение бакетов непрерывными блоками — «бакет к ближайшему центру
// отрезка» (arch/02 §9.1.1): floor((2·i+1)·S/(2·N)); канон 10×3 → 3+4+3
// (остаток — среднему шарду). Целочисленно, без float:
// max (2·8191+1)·128 = 2 097 024 — переполнение int исключено.
public static int OwnerShard(int bucket, int buckets, int shards)
    => (2 * bucket + 1) * shards / (2 * buckets) + 1;
```

Выход: метод существует; `Build` ещё использует round-robin (меняется в Step 1.5).

Проверка: `dotnet test src/tests/AdminPanel.UnitTests --filter "FullyQualifiedName~ClusterCreatePlanTests"`
Ожидание: **PASS** (все тесты, включая старые round-robin-ассерты `Build_*`, — round-robin в `Build` пока не тронут).

Связь со spec: §4.2 (сигнатура и реализация — дословно).

- [ ] **Step 1.3: Обновить ожидания `Build`-тестов под блочный расклад**

Вход: `src/tests/AdminPanel.UnitTests/CreateClusterPlanTests.cs`.

Действие — три правки:

(a) в `Build_FullPlan_MatchesContract` заменить блок round-robin-ассертов (строки с комментарием `// round-robin: bucket_i → shard_(i % S + 1) — как init-cluster.sh`):

```csharp
// блочное распределение (arch/02 §9.1.1): 4×2 → бакеты 0,1=shard1; 2,3=shard2
plan.Puts.Single(p => p.Key == "/clusters/shop/buckets/routing/bucket_0").Value.Should().Be("shard1");
plan.Puts.Single(p => p.Key == "/clusters/shop/buckets/routing/bucket_1").Value.Should().Be("shard1");
plan.Puts.Single(p => p.Key == "/clusters/shop/buckets/routing/bucket_2").Value.Should().Be("shard2");
```

(ассерт `status/bucket_3` c `"owner":"shard2"` ниже по тексту теста остаётся верным: b3 → shard2 по формуле и по round-robin совпадает).

(b) заменить тест `Build_RoundRobinUneven_FirstShardsGetExtra` целиком на:

```csharp
[Fact]
public void Build_BlockUneven_RemainderToLaterShards()
{
    // Arrange: 5 бакетов, 2 шарда — блоки 2+3, остаток у ПОСЛЕДНЕГО (spec §2.1)
    var request = new CreateClusterRequest("u", 5, 2, 1, 1m, 1, 1);

    // Act
    var plan = ClusterCreatePlan.Build(request, 1);

    // Assert: floor((2i+1)·2/10): b0,b1→shard1; b2,b3,b4→shard2
    plan.Puts.Single(p => p.Key == "/clusters/u/buckets/routing/bucket_4").Value.Should().Be("shard2");
}
```

(c) комментарий над классом `ClusterCreatePlanTests` (строка `// План ключей одного создания: arch/02 §9.1 — конфиг, шарды, ноды, routing round-robin, request_*.`) заменить на `// План ключей одного создания: arch/02 §9.1 — конфиг, шарды, ноды, routing блоками (§9.1.1), request_*.`

Выход: тесты `Build_*` ожидают блочный расклад, которого `Build` ещё не строит.

Проверка: `dotnet test src/tests/AdminPanel.UnitTests --filter "FullyQualifiedName~ClusterCreatePlanTests"`
Ожидание: **FAIL** в `Build_FullPlan_MatchesContract` (bucket_1: ожидалось `shard1`, получено `shard2`) и `Build_BlockUneven_RemainderToLaterShards` (bucket_4: ожидалось `shard2`, получено `shard1`).

Связь со spec: §4.6 (обновление план-тестов), критерий 3.

- [ ] **Step 1.4: Переключить `Build` на `OwnerShard`**

Вход: `src/AdminPanel.Etcd/Writing/ClusterCreatePlan.cs`, цикл бакетов в `Build`.

Действие — заменить (строки ~52–55):

```csharp
for (var i = 0; i < request.Buckets; i++)
{
    // round-robin по шардам — как init-cluster.sh bucket_shard(): i % S
    var owner = $"shard{i % request.Shards + 1}";
```

на:

```csharp
for (var i = 0; i < request.Buckets; i++)
{
    // владелец — непрерывный блок: канон arch/02 §9.1.1
    var owner = $"shard{OwnerShard(i, request.Buckets, request.Shards)}";
```

Выход: round-robin (`i % request.Shards`) в кодовой базе плана отсутствует; вырожденный 1×1 проходит формулой без спецеветки (`OwnerShard(0,1,1) == 1` — тест `Build_NormalizedSingle_DegenerateStructure` не меняется).

Проверка: `dotnet test src/tests/AdminPanel.UnitTests --filter "FullyQualifiedName~ClusterCreatePlanTests" && dotnet build src/AdminPanel.slnx`
Ожидание: **PASS** всеми тестами; build решения — 0 warnings/0 errors.

Связь со spec: §4.2, критерии 1–3, 5.

- [ ] **Step 1.5: Коммит**

Вход: зелёные тесты Task 1.
Действие:

```bash
git add src/AdminPanel.Etcd/Writing/ClusterCreatePlan.cs src/tests/AdminPanel.UnitTests/CreateClusterPlanTests.cs
git commit -m "feat(etcd): OwnerShard — блочное распределение бакетов каноном §9.1.1 (10×3 → 3+4+3) вместо round-robin"
```

Выход: коммит в `feat-bucket-block-distribution`.
Проверка: `git log --oneline -1` — новый коммит; `git status --short` — чисто.
Связь со spec: фаза 2.

---

### Task 2: `ClusterDto.Sharded` — вычисляемый признак нешардированной

**Files:**
- Modify: `src/AdminPanel.Api/Inspection/ClusterDetailsQuery.cs` (record `ClusterDto` строки ~14–24; `ClusterDetailsMapper.Map` строки ~111–151)
- Test: `src/tests/AdminPanel.UnitTests/ClustersMappersTests.cs`

**Interfaces:**
- Consumes: `ClusterInfo` из `AdminPanel.Core` (поля `BucketsCount`, `Shards`).
- Produces: поле `bool Sharded` в `ClusterDto` (позиционно после `State`; JSON — `sharded` camelCase). Потребители: интеграционные тесты Task 3 (JSON `sharded`), фронт Task 4 (`ClusterDto.sharded: boolean`). Единственное место конструирования `new ClusterDto(` — маппер (проверено).

- [ ] **Step 2.1: Написать падающий тест маппера**

Вход: `src/tests/AdminPanel.UnitTests/ClustersMappersTests.cs`, после `ClusterDetailsMapper_Filters_OwnerStateBothNull`.

Действие — добавить (uses `AdminPanel.Core`, `AdminPanel.Etcd` и `AdminPanel.Api.Inspection` уже есть в файле — сверить по существующим тестам `ClusterDetailsMapper_*`):

```csharp
[Fact]
public void ClusterDetailsMapper_ShardedFlag_SingleVsMultiBucket()
{
    // Arrange: lone — нешардированная 1×1 (arch/02 §9.1); orphan — incomplete-обрывок
    // config.buckets=1 без шардов (spec §8.6); MovingCluster — 16 бакетов × 2 шарда
    var lone = new ClusterInfo("lone", "lone", 1, 1755900000, ClusterState.NotInitialized,
        [new ShardInfo("shard1", "", [], null, null, null, 2, null,
            [new NodeInfo("shard1a", "NOT_INITIALIZED"), new NodeInfo("shard1b", "NOT_INITIALIZED")], null)],
        [new BucketInfo(0, "shard1", BucketState.NotInitialized, null)], []);
    var orphan = new ClusterInfo("orphan", null, 1, null, ClusterState.Active, [], [], []);

    // Act/Assert: false ⟺ ровно 1 бакет и ≤1 шард (arch/03 §2)
    ClusterDetailsMapper.Map(lone, NowUnix, null, null, [], []).Sharded.Should().BeFalse();
    ClusterDetailsMapper.Map(orphan, NowUnix, null, null, [], []).Sharded.Should().BeFalse();
    ClusterDetailsMapper.Map(TestSnapshots.MovingCluster(Now), NowUnix, null, null, [], []).Sharded.Should().BeTrue();
}
```

Выход: тест ссылается на несуществующее поле `Sharded` у `ClusterDto`.
Проверка: `dotnet test src/tests/AdminPanel.UnitTests --filter "FullyQualifiedName~ClustersMappersTests"`
Ожидание: **ошибка компиляции** `CS1061: 'ClusterDto' does not contain a definition for 'Sharded'`.
Связь со spec: §4.3, §8.2, §8.6, критерий 4.

- [ ] **Step 2.2: Добавить поле и вычисление**

Вход: `src/AdminPanel.Api/Inspection/ClusterDetailsQuery.cs`.

Действие — две правки:

(a) record `ClusterDto` — вставить поле после `string State,`:

```csharp
public sealed record ClusterDto(
    string Name,
    string? DbName,
    int BucketsCount,
    long? CreatedUnix,
    bool Incomplete,
    string State,
    bool Sharded,
    IReadOnlyList<ShardDto> Shards,
    IReadOnlyList<BucketDto> Buckets,
    IReadOnlyList<HealDto> Heals,
    IReadOnlyList<StandNodeDto> StandNodes);
```

(b) в `ClusterDetailsMapper.Map` — вставить аргумент после `ClusterStates.Name(cluster.State),`:

```csharp
            ClusterStates.Name(cluster.State),
            // sharded — вычисляемое поле отображения (arch/03 §2): false ⟺ ровно 1
            // бакет и не более 1 шарда; признак «тип БД» в etcd не хранится (02 §9.1).
            !(cluster.BucketsCount == 1 && cluster.Shards.Count <= 1),
```

Выход: `ClusterDto.Sharded` вычисляется в единственном месте конструирования.
Проверка: `dotnet test src/tests/AdminPanel.UnitTests --filter "FullyQualifiedName~ClustersMappersTests" && dotnet build src/AdminPanel.slnx`
Ожидание: **PASS** (включая новый тест); build решения — 0 warnings (в т.ч. `InspectionQueryHandlerTests` и прочие юзеры маппера компилируются — поле позиционное, конструктор один).
Связь со spec: §4.3 (дословно), arch/03 §2.

- [ ] **Step 2.3: Прогнать весь юнит-проект (регресс соседей)**

Вход: правки Task 2.
Действие: `dotnet test src/tests/AdminPanel.UnitTests`
Выход: уверенность, что позиционное изменение record не сломало другие тесты (маппер вызывается только через `Map`).
Проверка: полный прогон — **все зелёные**; при падении `Inspection*`-тестов — сверить порядок аргументов `new ClusterDto(...)` (должно быть единственное место).
Связь со spec: критерий 4 (unit-уровень).

- [ ] **Step 2.4: Коммит**

Вход: зелёный юнит-проект.
Действие:

```bash
git add src/AdminPanel.Api/Inspection/ClusterDetailsQuery.cs src/tests/AdminPanel.UnitTests/ClustersMappersTests.cs
git commit -m "feat(api): ClusterDto.sharded — вычисляемый признак нешардированной (1 бакет × ≤1 шард), arch/03 §2"
```

Выход: коммит. Проверка: `git log --oneline -1`; `git status --short` — чисто.
Связь со spec: фаза 3.

---

### Task 3: Интеграционные тесты — значения routing (блоки) и `sharded` в деталях

**Files:**
- Modify: `src/tests/AdminPanel.IntegrationTests/CreateClusterApiTests.cs` (кейс `Create_Valid_WritesContractKeysToEtcd`; новый кейс)
- Modify: `src/tests/AdminPanel.IntegrationTests/ClustersApiTests.cs` (новый тест; фикстуру `InspectionSnapshots.Clustered` НЕ трогать — есть ассерт `GetArrayLength().Should().Be(1)`)

**Interfaces:**
- Consumes: `ClusterCreatePlan.OwnerShard` (Task 1) через реальный POST→etcd; `ClusterDto.Sharded` (Task 2) через GET-детали; `EtcdTestHarness.NewGateway()`, `fixture.Endpoint`, `InspectionSnapshots.Fixture` (существующие).
- Produces: ничего (только тесты); фиксируют контракт для e2e Task 5.

Требование к окружению: Docker (Testcontainers `quay.io/coreos/etcd:v3.5.21`).

- [ ] **Step 3.1: Routing-значения в существующем кейсе (4×2) + канонический кейс (10×3)**

Вход: `src/tests/AdminPanel.IntegrationTests/CreateClusterApiTests.cs`.

Действие — две правки:

(a) в `Create_Valid_WritesContractKeysToEtcd`, после блока проверки `/service/shop-` ключей, добавить:

```csharp
// routing — блочное распределение (arch/02 §9.1.1): 4×2 → 0,1=shard1; 2,3=shard2
// (порядок ключей bucket_0..3 лексикографичен — одна разрядность)
var routing = range.Value
    .Where(kv => kv.Key.StartsWith("/clusters/shop/buckets/routing/"))
    .OrderBy(kv => kv.Key)
    .Select(kv => kv.Value).ToArray();
routing.Should().Equal("shard1", "shard1", "shard2", "shard2");
```

(b) новый кейс после `Create_Duplicate_Returns409`:

```csharp
[Fact]
public async Task Create_CanonicalTenByThree_WritesBlockRouting()
{
    // Arrange: канон spec §2.1 — 10 бакетов × 3 шарда, остаток среднему шарду
    SetLiveSnapshot();
    using var client = await ApiTestLogin.LoginAsync(_factory);

    // Act
    using var response = await client.PostAsJsonAsync(
        "/api/clusters",
        new { name = "canon10", buckets = 10, shards = 3, replicas = 2, requestCpu = 0.5m, requestMem = 8, requestDisk = 100 },
        TestContext.Current.CancellationToken);

    // Assert: через реальный gateway — блоки 3+4+3 (arch/02 §9.1.1)
    response.StatusCode.Should().Be(HttpStatusCode.Created);
    var gateway = EtcdTestHarness.NewGateway();
    var range = await gateway.RangeAsync(
        fixture.Endpoint, "/clusters/canon10/buckets/routing/", TestContext.Current.CancellationToken);
    var routing = range.Value.OrderBy(kv => kv.Key).Select(kv => kv.Value).ToArray();
    routing.Should().Equal(
        "shard1", "shard1", "shard1",
        "shard2", "shard2", "shard2", "shard2",
        "shard3", "shard3", "shard3");
}
```

Выход: контракту блочного routing даны интеграционные ассерты.
Проверка: `dotnet test src/tests/AdminPanel.IntegrationTests --filter "FullyQualifiedName~CreateClusterApiTests"`
Ожидание: **PASS** (код Tasks 1–2 уже реализован; при FAIL — читать фактические значения routing из сообщения FluentAssertions).
Связь со spec: §4.6 (integration), критерии 1, 5.

- [ ] **Step 3.2: `sharded` в GET-деталях**

Вход: `src/tests/AdminPanel.IntegrationTests/ClustersApiTests.cs`, после `Clusters_NotInitializedCluster_FlaggedInSummaryAndDetails` (паттерн построения кластера поверх `Fixture` — оттуда же).

Действие — добавить тест:

```csharp
[Fact]
public async Task ClusterDetails_ShardedFlag_SingleFalse_MultiTrue()
{
    // Arrange: lone — нешардированная 1×1 (arch/02 §9.1); mini — 2×1; паттерн
    // Clusters_NotInitializedCluster_FlaggedInSummaryAndDetails (поверх Fixture)
    using var client = await LoginAsync();
    var lone = new ClusterInfo("lone", "lone", 1, 1755900000, ClusterState.NotInitialized,
        [new ShardInfo("shard1", "", [], null, null, null, 2, null,
            [new NodeInfo("shard1a", "NOT_INITIALIZED"), new NodeInfo("shard1b", "NOT_INITIALIZED")], null)],
        [new BucketInfo(0, "shard1", BucketState.NotInitialized, null)], []);
    var mini = new ClusterInfo("mini", "mini", 2, 1755900000, ClusterState.NotInitialized,
        [new ShardInfo("shard1", "", [], null, null, null, 2, null,
            [new NodeInfo("shard1a", "NOT_INITIALIZED"), new NodeInfo("shard1b", "NOT_INITIALIZED")], null)],
        [
            new BucketInfo(0, "shard1", BucketState.NotInitialized, null),
            new BucketInfo(1, "shard1", BucketState.NotInitialized, null),
        ], []);
    _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.Utc) with { Clusters = [lone, mini] };

    // Act
    var loneDto = await GetJsonAsync(client, "/api/clusters/lone");
    var miniDto = await GetJsonAsync(client, "/api/clusters/mini");

    // Assert: sharded=false ⟺ 1 бакет и ≤1 шард (arch/03 §2)
    loneDto.GetProperty("sharded").GetBoolean().Should().BeFalse();
    miniDto.GetProperty("sharded").GetBoolean().Should().BeTrue();
}
```

Выход: HTTP-контракт `sharded` зафиксирован.
Проверка: `dotnet test src/tests/AdminPanel.IntegrationTests --filter "FullyQualifiedName~ClustersApiTests"`
Ожидание: **PASS**; `Clusters_WithSnapshot_ReturnSummaries` (Be(1)) и прочие — не затронуты.
Связь со spec: §4.6, критерии 4–5.

- [ ] **Step 3.3: Прогон всего integration-проекта и коммит**

Вход: правки Task 3.
Действие:

```bash
dotnet test src/tests/AdminPanel.IntegrationTests
git add src/tests/AdminPanel.IntegrationTests/CreateClusterApiTests.cs src/tests/AdminPanel.IntegrationTests/ClustersApiTests.cs
git commit -m "test(api): integration — блочный routing 4×2 и канон 10×3 (§9.1.1) + sharded в деталях кластера"
```

Выход: коммит; regression-поверхность интеграционных тестов зелёная.
Проверка: полный прогон integration — **все зелёные**; `git status --short` — чисто.
Связь со spec: фаза 4.

---

### Task 4: Фронтенд — `sharded` в DTO и условная вкладка «Бакеты»

**Files:**
- Modify: `frontend/src/api/dto.ts` (интерфейс `ClusterDto`, строки ~127–138)
- Modify: `frontend/src/pages/ClusterDetailsPage.tsx` (Tabs, строки ~55–66; комментарий шапки файла)

**Interfaces:**
- Consumes: JSON-поле `sharded` (camelCase) ответа `GET /api/clusters/{cluster}` (Task 2).
- Produces: `ClusterDto.sharded: boolean` — единственный источник условия для вкладки; более ничего на фронте не меняется.

- [ ] **Step 4.1: Поле в TS-DTO**

Вход: `frontend/src/api/dto.ts`.

Действие — в `interface ClusterDto` вставить после `state: ClusterStateName;`:

```ts
  // Вычисляется сервером (arch/03 §2): false ⟺ 1 бакет и ≤1 шард —
  // нешардированная БД; скрывает вкладку «Бакеты» на странице деталей.
  sharded: boolean;
```

Выход: тип отражает контракт API.
Проверка: `cd frontend && npm run typecheck && cd ..` — без ошибок.
Связь со spec: §4.4.

- [ ] **Step 4.2: Условная вкладка на странице деталей**

Вход: `frontend/src/pages/ClusterDetailsPage.tsx`.

Действие — три правки:

(a) комментарий шапки файла дополнить упоминанием скрытия:

```tsx
// Детали кластера: шапка + вкладки Шарды/Бакеты/Переезды/Heals + стендовая топология (t08 spec §4.7–4.8).
// Вкладка «Бакеты» скрыта для нешардированных (sharded=false, arch/03 §3; spec
// bucket-block-distribution §4.4): у БД 1×1 нет карты бакетов.
```

(b) `Tabs.List` — обернуть вкладку «Бакеты» условием:

```tsx
        <Tabs.List>
          <Tabs.Tab value="shards">Шарды</Tabs.Tab>
          {data.sharded ? <Tabs.Tab value="buckets">Бакеты</Tabs.Tab> : null}
          <Tabs.Tab value="moves">Переезды</Tabs.Tab>
          <Tabs.Tab value="heals">Heals</Tabs.Tab>
        </Tabs.List>
```

(c) панель «Бакеты» — обернуть условием:

```tsx
        <Tabs.Panel value="shards" pt="sm"><ShardsTab shards={data.shards} /></Tabs.Panel>
        {data.sharded ? (
          <Tabs.Panel value="buckets" pt="sm"><BucketsTab buckets={data.buckets} /></Tabs.Panel>
        ) : null}
```

`defaultValue="shards"` не меняется: скрытие вкладки «Бакеты» на выбор не влияет. Импорт `BucketsTab` остаётся (используется в JSX-условии).

Выход: нешардированный кластер рендерится без вкладки «Бакеты» (ни таба, ни панели).
Проверка: `cd frontend && npm run build && cd ..` (tsc --noEmit двух конфигов + vite build) — без ошибок.
Связь со spec: §4.4, критерий 4; arch/03 §3.

- [ ] **Step 4.3: Коммит**

Вход: зелёный build фронта.
Действие:

```bash
git add frontend/src/api/dto.ts frontend/src/pages/ClusterDetailsPage.tsx
git commit -m "feat(frontend): вкладка Бакеты скрыта для нешардированных кластеров (ClusterDto.sharded)"
```

Выход: коммит. Проверка: `git log --oneline -1`; `git status --short` — чисто.
Связь со spec: фаза 5.

---

### Task 5: e2e-чек 15 — блочная раскладка, канон 10×3, `sharded` в деталях

**Files:**
- Modify: `dev-stand/checks/15-cluster-create.sh`

**Interfaces:**
- Consumes: всё выше (сервер пишет блоки, детали содержат `sharded`); переменные чека `BASE`, `JAR`, функция `ect` — существуют.
- Produces: зелёный e2e-чек — критерий приёмки 6; кейсы: smoke (4×2 блоки + `sharded:true`), solo (`sharded:false`), canon10 (3+4+3).

- [ ] **Step 5.1: Заменить round-robin-ассерт smoke на блочный**

Вход: `dev-stand/checks/15-cluster-create.sh`, строки 44–48.

Действие — заменить:

```bash
# etcdctl get --prefix отдаёт значения в порядке ключей (bucket_0..3) — БЕЗ sort,
# чтобы проверить именно round-robin-раскладку: shard1 shard2 shard1 shard2
routing="$(ect get --prefix /clusters/smoke/buckets/routing --print-value-only | tr '\n' ' ')"
[ "$routing" = "shard1 shard2 shard1 shard2 " ] || { echo "❌ routing round-robin: $routing"; exit 1; }
echo "  etcd: config/nodes/request_*/routing — контракт §9.1 соблюдён"
```

на:

```bash
# etcdctl get --prefix отдаёт значения в порядке ключей (bucket_0..3) — БЕЗ sort:
# блочное распределение (arch/02 §9.1.1) — 4×2: бакеты 0,1→shard1; 2,3→shard2
routing="$(ect get --prefix /clusters/smoke/buckets/routing --print-value-only | tr '\n' ' ')"
[ "$routing" = "shard1 shard1 shard2 shard2 " ] || { echo "❌ routing blocks 4×2: $routing"; exit 1; }
echo "  etcd: config/nodes/request_*/routing — контракт §9.1.1 (блоки) соблюдён"
```

Выход: smoke-кейс утверждает блочный расклад.
Проверка: `bash -n dev-stand/checks/15-cluster-create.sh` — синтаксис ок (полный прогон — Task 6).
Связь со spec: §4.5, §8.5.

- [ ] **Step 5.2: `.sharded` в проверках деталей smoke и solo**

Вход: тот же файл; smoke-блок (строки 64–66) и solo-блок (строки 94–103).

Действие:

(a) jq-проверку деталей smoke (единственную — строки 64–66; цикл выше проверяет только список `/api/clusters`) расширить полем `sharded`:

```bash
curl -fsS -b "$JAR" "$BASE/api/clusters/smoke" | jq -e \
  '.sharded == true and .state=="NOT_INITIALIZED" and .shards[0].requests.cpu=="0.5" and (.shards[0].nodes|length)==2' >/dev/null \
```

(комментарий об ошибке дополнить: `|| { echo "❌ /api/clusters/smoke: sharded/state/requests/nodes"; exit 1; }`)

(b) обе jq-проверки solo (в цикле и финальную) расширить полем `sharded`:

```bash
curl -fsS -b "$JAR" "$BASE/api/clusters/solo" | jq -e \
  '.sharded == false and .state=="NOT_INITIALIZED" and .bucketsCount==1 and (.shards|length)==1' >/dev/null \
```

(сообщение об ошибке: `|| { echo "❌ /api/clusters/solo: sharded=false / вырожденная структура не видна"; exit 1; }`)

Выход: e2e проверяет вычисляемый признак у обоих типов кластеров.
Проверка: `bash -n dev-stand/checks/15-cluster-create.sh`.
Связь со spec: §4.5, критерий 4.

- [ ] **Step 5.3: Кейс `canon10` — канон 10×3 → 3+4+3**

Вход: тот же файл; вставка после solo-блока (после строки `echo "  /api/clusters/solo: 1 бакет × 1 шард, NOT_INITIALIZED"`), до финального `echo "✓ 15-cluster-create: …"`.

Действие — добавить:

```bash
# --- Кейс канона (spec bucket-block-distribution §2.1): 10×3 → 3+4+3 ---
ect del --prefix /clusters/canon10 >/dev/null
for s in 1 2 3; do
  for k in request_cpu request_mem request_disk; do
    ect del "/service/canon10-shard$s/$k" >/dev/null
  done
done

code="$(curl -s -o /tmp/t15-canon10.json -w '%{http_code}' -b "$JAR" -X POST "$BASE/api/clusters" \
  -H 'Content-Type: application/json' \
  -d '{"name":"canon10","buckets":10,"shards":3,"replicas":2,"requestCpu":0.5,"requestMem":8,"requestDisk":100}')"
[ "$code" = 201 ] || { echo "❌ POST /api/clusters (canon10) = $code: $(cat /tmp/t15-canon10.json)"; exit 1; }

# Значения в порядке bucket_0..9: блоки 3+4+3 — остаток среднему шарду (§9.1.1)
routing="$(ect get --prefix /clusters/canon10/buckets/routing --print-value-only | tr '\n' ' ')"
[ "$routing" = "shard1 shard1 shard1 shard2 shard2 shard2 shard2 shard3 shard3 shard3 " ] \
  || { echo "❌ canon10 routing 3+4+3: $routing"; exit 1; }
echo "  canon10: 10×3 → 3+4+3 — канон §9.1.1 соблюдён"
```

Выход: канон — самостоятельный e2e-критерий (smoke остаётся нетронутым регрессом обратной совместимости: тело без `sharded`).
Проверка: `bash -n dev-stand/checks/15-cluster-create.sh`.
Связь со spec: §4.5, §8.5, критерии 1, 5.

- [ ] **Step 5.4: Коммит**

Вход: синтаксически валидный чек.
Действие:

```bash
git add dev-stand/checks/15-cluster-create.sh
git commit -m "chore(stand): e2e-чек 15 — блочная раскладка smoke, кейс canon10 (10×3 → 3+4+3), .sharded в деталях"
```

Выход: коммит. Проверка: `git log --oneline -1`; `git status --short` — чисто.
Связь со spec: фаза 6.

---

### Task 6: Полная верификация и сверка с контрактом

**Files:**
- Без правок кода (только проверки; если что-то красное — фикс в рамках соответствующего Task и перекоммит).

**Interfaces:**
- Consumes: все предыдущие Tasks.
- Produces: подтверждение критериев приёмки spec §7 (1–7) — отчёт исполнителя.

- [ ] **Step 6.1: Сервер — сборка и все тесты**

Вход: ветка с Tasks 1–5.
Действие:

```bash
dotnet build src/AdminPanel.slnx
dotnet test src/AdminPanel.slnx
```

Выход: сборка и полный прогон решения (unit + integration; integration требует запущенного Docker).
Проверка: build — **0 warnings, 0 errors**; `dotnet test src/AdminPanel.slnx` — **все зелёные**.
Связь со spec: критерий 6.

- [ ] **Step 6.2: Фронтенд — production-сборка**

Вход: правки Task 4.
Действие: `cd frontend && npm run build && cd ..`
Выход: tsc (app+node) + vite build.
Проверка: **без ошибок**.
Связь со spec: критерий 6.

- [ ] **Step 6.3: e2e на dev-стенде**

Вход: Docker-стенд.
Действие:

```bash
cd dev-stand && ./checks/00-up.sh && ./checks/15-cluster-create.sh; cd ..
```

Выход: полный прогон чека 15 (панель `http://localhost:5050`, admin/admin).
Проверка: вывод заканчивается `✓ 15-cluster-create: создание кластера e2e прошло`; в логе видны строки всех кейсов: smoke (routing blocks 4×2, `sharded==true`), solo (`sharded==false`), canon10 (3+4+3). Любая `❌`-строка = FAIL → фикс и повтор.
Связь со spec: критерии 1, 4, 5, 6.

- [ ] **Step 6.4: UI-проверка скрытия вкладки «Бакеты» (браузер или curl-фолбэк)**

Вход: стенд поднят (Step 6.3), панель `http://localhost:5050` отдаёт собранный бандл (SPA хостится панелью, arch/03 §6), кластеры `solo` (нешардированная) и `smoke`/`canon10` (шардированные) существуют.

Действие (вариант A — IAB-браузер доступен): открыть `http://localhost:5050` → логин admin/admin → Clusters → `solo`: на странице деталей есть вкладки Шарды/Переезды/Heals и **нет** вкладки «Бакеты» (ни таба, ни панели); `smoke`: вкладка «Бакеты» **есть**, грид бакетов (4 строки) открывается. При наличии — снять domSnapshot.

Действие (вариант B — браузер недоступен; основной в этой среде): curl-проверка, что панель отдаёт бандл из нового кода — в JS-бандле есть обращение к полю `sharded` (условие `data.sharded ? … : null` не минифицирует имя свойства):

```bash
bundle="$(curl -s http://localhost:5050/ | grep -o 'assets/index-[^"]*\.js' | head -1)"
curl -s "http://localhost:5050/$bundle" | grep -c 'sharded'
```

Выход: подтверждение, что UI-слой с условным рендером задеплоен; само решение «показывать ли вкладку» принимает фронт по полю `sharded`, корректность которого уже доказана e2e на API-уровне (`jq '.sharded == false'` у solo, `'.sharded == true'` у smoke — Step 6.3).
Проверка: вариант A — у solo вкладка «Бакеты» отсутствует, у smoke присутствует; вариант B — `grep -c` вернул ≥ 1 (обращение к `sharded` в бандле есть). При нуле — панель отдаёт старый бандл: пересобрать/переподнять стенд и повторить.
Связь со spec: §4.6 (UI покрывается e2e и ручной проверкой), критерий 4.

- [ ] **Step 6.5: Сверка соответствия arch ↔ код и итог ветки**

Вход: всё зелёное.
Действие:

```bash
git status --short   # чисто
git log --oneline main..HEAD
```

Плюс ручная сверка трёх точек: (1) `ClusterCreatePlan.OwnerShard` = формуле arch/02 §9.1.1; (2) `ClusterDetailsMapper` — `sharded` = `!(BucketsCount==1 && Shards.Count<=1)` = arch/03 §2; (3) условная вкладка «Бакеты» = arch/03 §3.

Выход: список коммитов задачи (docs a6aebef + 5 коммитов Tasks 1–5).
Проверка: расхождений с arch/spec нет; незакоммиченных изменений нет.
Связь со spec: критерий 7; после этого основной агент запускает review-субагента (план не запускать дальше самостоятельно).

---

## Сводка соответствия spec → задачи

| Spec | Задача |
|---|---|
| §4.2 алгоритм `OwnerShard`, замена round-robin | Task 1 |
| §4.3 `ClusterDto.Sharded` + маппер | Task 2 |
| §4.6 integration (routing 4×2/10×3, sharded) | Task 3 |
| §4.4 фронт (dto.ts + условная вкладка) | Task 4 |
| §4.5 e2e-чек 15 (блоки, canon10, .sharded) | Task 5 |
| §5 фаза 7 / §7 критерии 1–7 (полные проверки) | Task 6 (6.4 — UI-проверка критерия 4) |
| §4.1 контракт arch | уже сделано (коммит a6aebef, Фаза 1) |
| §2.1 таблица распределений | Task 1 Steps 1.1 (теория + свойства + точные расклады) |
