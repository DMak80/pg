# t12-integration-red-debt — план (починка красных интеграционных тестов)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Полная интеграционная серия `src/tests/PgWorker.IntegrationTests` зелёная дважды подряд: 3 красных `AdoptionContractTests` (нет обязательных заявок `request_{cpu,mem}` в сидах) + флейк клэйма «sc3» (пересечение имён кластеров `ShardScaleContractTests` × `BackupSupervisorProcessTests` на общем etcd коллекции).

**Architecture:** Prod-код не меняется — только тестовая инфраструктура. Дефект 1 закрывается посевом заявок в сидах (формат панели). Дефект 2 закрывается устранением общего контекста: per-class guid-тег в именах кластеров (механическая уникальность, канон `docs/e2e-isolation.md` §1), предочистка `/pgworker/claims/<C>` перед `TryClaim`, own-only teardown клэйма (`await using` / `IAsyncLifetime` → `ClaimStore.DisposeAsync` отзывает lease немедленно). Никаких sleep/TTL-ожиданий.

**Tech Stack:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`), xUnit v3 (collection-фикстура `EtcdCollection` — один testcontainers-etcd), FluentAssertions.

**Spec:** `docs/superpowers/2026-09-14-t12-integration-red-debt/spec.md` (в worktree `/Users/demakaev/ZCodeProject/worktrees/t12-integration-red-debt`).

**Worktree/ветка:** `/Users/demakaev/ZCodeProject/worktrees/t12-integration-red-debt`, ветка `t12-integration-red-debt` (уже создана, чистая — только spec/plan не в git). Все команды ниже выполняются от корня этого worktree.

## Global Constraints

- Prod-код (`src/PgWorker.*`) НЕ меняется; `arch/` НЕ меняется (spec §2.4, §6). Правятся только файлы `src/tests/PgWorker.IntegrationTests/`.
- Формат заявок — канонический строковый вид панели: `request_cpu="2"`, `request_mem="4Gi"` (spec §3 п.5, как `ShardScaleContractTests.SeedAddDeclarationAsync`).
- Никаких sleep / ожиданий TTL; флейк лечится изоляцией, не таймингами (spec §3 п.1).
- Порты: ничего не хардкодится — `EtcdFixture` уже на динамических портах, не трогается (spec §4.3).
- Комментарии — по-русски; идентификаторы — на английском; тесты — AAA (AGENTS.base.md §7, spec §6).
- `TreatWarningsAsErrors=true`: любая сборка (входит в каждый `dotnet test`) обязана проходить без ворнингов.
- Минимальный diff: латентные пересечения `c1` (Restore × WalStream) НЕ переливаются (spec §4.3); `AdoptionContractTests` не переводится на guid-теги (его имена `adoptc*`/`adoptadv` уникальны, spec ограничил скоуп двумя диагностированными дефектами).
- Каждый прогон — дожидаться финальной строки `dotnet test`; после КАЖДОЙ серии (точечной и полной) — зачистка docker (AGENTS.md): контейнеры тестов удалить, контейнеры dev-стенда `as-*`/`adminpanel` НЕ трогать, `docker network prune -f`.
- Перезапуск упавших тестов «для выяснения, что было» запрещён (spec §7 п.2): красное → анализ вывода прогона → фикс → счётчик «дважды подряд» обнуляется.
- Снятие roadmap-тега `t12` из `arch/roadmap/pgworker.md` — тем же МЕРЖ-коммитом в `main` (мерж-гейт), в задачах плана НЕ делается (spec §6).

---

### Task 1: AdoptionContractTests — посев обязательных заявок (spec §4.1, Фаза 1)

**Files:**
- Modify: `src/tests/PgWorker.IntegrationTests/Etcd/AdoptionContractTests.cs` (хелпер `SeedExternalClusterAsync`, строки ~32–47; сид теста `Adopt_LegacyDockerHostNameInPortalloc_RepairsToAdvertisedHost`, строки ~169–186)

**Interfaces:**
- Consumes: существующий контракт `AdoptionProcess` → `ReadShardResourcesAsync` читает `/service/<cluster>-<shard>/request_{cpu,mem}` (prod, не меняется); формат значений — как у панели.
- Produces: ничего для соседних задач — правка локальна для класса `AdoptionContractTests`.

Механика дефекта (подтверждена по коду): `AdoptionProcess.cs:400` — `ReadShardResourcesAsync` при отсутствии ключа возвращает `null` → `PgtuneInputsFactory.Create(null)` бросает `InvalidOperationException` → тик `Failed` → красный `IsSuccess.Should().BeTrue(...)`. Сид закрывает и `adoptc1-s1`, и `adoptc2-s1` (общий хелпер), отдельный сид — `adoptadv-shard1`.

- [ ] **Step 1: Подтвердить красноту базы (3/3 FAIL)**

Run:
```bash
cd /Users/demakaev/ZCodeProject/worktrees/t12-integration-red-debt && DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter FullyQualifiedName~AdoptionContractTests
```
Expected: `Failed! ... failed: 3, passed: 0` — все три теста падают на `IsSuccess.Should().BeTrue` (в сообщении — pgtune «обязательная заявка request_mem отсутствует/нечитаема» или Equivalent of it). Если падения ДРУГИЕ — остановиться и разобрать вывод, не продолжать (значит база отличается от spec).

Зачистка после серии:
```bash
for id in $(docker ps -aq); do name=$(docker inspect -f '{{.Name}}' "$id" | tr -d '/'); case "$name" in as-*|adminpanel*) ;; *) docker rm -f "$id" ;; esac; done
docker network prune -f
```

- [ ] **Step 2: Сид заявок в `SeedExternalClusterAsync` (закрывает adoptc1-s1, adoptc2-s1)**

В `AdoptionContractTests.cs` в конец хелпера `SeedExternalClusterAsync(string cluster)` (после PUT членов `s1b`, перед закрывающей скобкой) добавить:

```csharp
        // Заявки ресурсов ОБЯЗАТЕЛЬНЫ (arch/14 §2.1 п.4, канонизировано f6d4574):
        // сид зеркалит то, что панель пишет при создании шарда, — без них
        // PgtuneInputsFactory фейлит тик усыновления (fail-fast, дефолтов нет).
        await Gateway.PutAsync(Endpoint, $"/service/{cluster}-s1/request_cpu", "2", null, ct);
        await Gateway.PutAsync(Endpoint, $"/service/{cluster}-s1/request_mem", "4Gi", null, ct);
```

- [ ] **Step 3: Сид заявок в тесте `Adopt_LegacyDockerHostNameInPortalloc_RepairsToAdvertisedHost` (scope adoptadv-shard1)**

В том же файле, в Arrange теста — после PUT членов `shard1b` (строки с `/service/adoptadv-shard1/members/shard1b`), перед PUT `nodes-ключей` — добавить:

```csharp
        // Заявки обязательны (arch/14 §2.1 п.4): формат панели — как SeedExternalClusterAsync.
        await Gateway.PutAsync(Endpoint, "/service/adoptadv-shard1/request_cpu", "2", null, ct);
        await Gateway.PutAsync(Endpoint, "/service/adoptadv-shard1/request_mem", "4Gi", null, ct);
```

- [ ] **Step 4: Точечная проверка — 3/3 зелёные**

Run:
```bash
cd /Users/demakaev/ZCodeProject/worktrees/t12-integration-red-debt && DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter FullyQualifiedName~AdoptionContractTests
```
Expected: `Passed! ... passed: 3, failed: 0`. Ассерты каждого теста: `first/second/outcome.IsSuccess` = true; фазы журнала `done`/`done`/`repaired-dsn` — как и до правки (методы `TickAsync` и ассерты не менялись).

Зачистка docker — та же команда, что в Step 1.

- [ ] **Step 5: Commit**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/t12-integration-red-debt && git add src/tests/PgWorker.IntegrationTests/Etcd/AdoptionContractTests.cs && git commit -m "test(adoption): сиды обязательных заявок request_{cpu,mem} в AdoptionContractTests — 3 красных теста приведены к канону arch/14 §2.1 п.4 (формат панели, как SeedAddDeclarationAsync); t12 spec §4.1"
```

---

### Task 2: ShardScaleContractTests — guid-теги, предочистка клэйма, `await using` (spec §4.2 п.1–3, Фаза 2)

**Files:**
- Modify: `src/tests/PgWorker.IntegrationTests/Etcd/ShardScaleContractTests.cs` (весь класс: 6 тестов, имена `sc1`..`sc6`)

**Interfaces:**
- Consumes: `ClaimStore` (`src/PgWorker.Etcd/Coordination/ClaimStore.cs`) — `IAsyncDisposable`, `DisposeAsync` отзывает все lease (ключ `/pgworker/claims/<C>` исчезает немедленно); `EtcdGateway.DeleteAsync(endpoint, key, prefix, ct)`; матчинг контейнеров `pgw-{cluster}-{shard}-` в `RemoveShardProcess.cs:98/254/256`; `BackupAgentNames.Container(cluster, shard)` = `pgw-backup-wal-{cluster}-{shard}`.
- Produces: паттерн `private static readonly string Tag = Guid.NewGuid().ToString("N")[..8];` + `$"scN{Tag}"` — тот же механизм повторяет Task 3 в `BackupSupervisorProcessTests` (литеральная база «scN» у классов остаётся общей, но тег делает полные имена уникальными per-class-запуск).

Важно: тег статический per-class — два класса с базой `scN` получают разные теги, пересечение ключей `/clusters/<C>`, `/pgworker/claims/<C>`, S3-префиксов `<C>/...` механически невозможно.

- [ ] **Step 1: Зафиксировать зелёную базу класса (точечный прогон)**

Run:
```bash
cd /Users/demakaev/ZCodeProject/worktrees/t12-integration-red-debt && DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter FullyQualifiedName~ShardScaleContractTests
```
Expected: `passed: 6, failed: 0` (флейк проявляется только в полной сборке; здесь — санити-база). Зачистка docker (как в Task 1 Step 1).

- [ ] **Step 2: Поле Tag и тегирование имён кластеров**

В `ShardScaleContractTests.cs` сразу после `private string Endpoint => fixture.Endpoint;` добавить:

```csharp
    // Per-class guid-тег (канон docs/e2e-isolation.md §1: guid во всех именах):
    // имена кластеров несут уникальный суффикс — пересечение с соседними классами
    // EtcdCollection механически невозможно (инцидент t07: BackupSupervisor взял
    // имена sc1..sc3, клэйм sc3 жил 15с по TTL и ронял TryClaim этого класса).
    private static readonly string Tag = Guid.NewGuid().ToString("N")[..8];
```

Затем в каждом тесте заменить литерал имени кластера на локальную переменную и провести её через ВСЕ ключи/имена теста. Точные правки по тестам:

Тест `PanelAddDeclaration_RealRange_ParserDetectsAddCandidate` — начало Arrange:

```csharp
        var cluster = $"sc1{Tag}";
        await SeedActiveClusterAsync(cluster, 6);
        await SeedAddDeclarationAsync(cluster, "shard3");

        // Act — реальный range → парсер → детекция scale-кандидатов
        var snap = await SnapshotAsync(cluster);
```
(селектор `s.Name is "shard1" or "shard2"` — имена шардов, не трогать.)

Тест `Marker_RealRange_ParserDetectsRemoveCandidate`:

```csharp
        var cluster = $"sc2{Tag}";
        await SeedActiveClusterAsync(cluster, 6);
        var put = await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard1/state", "TO_REMOVE",
            null, TestContext.Current.CancellationToken);
```
и в Act: `Single(c => c.Config.Cluster == cluster)`.

Тест `RemoveShardProcess_OnRealEtcd_CleansKeysWithRealTxnAndDel` — полный новый Arrange (клэйм: предочистка + `await using`):

```csharp
        var ct = TestContext.Current.CancellationToken;
        var cluster = $"sc3{Tag}";
        await SeedActiveClusterAsync(cluster, 3);
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard1/state", "TO_REMOVE", null, ct);
        await Gateway.PutAsync(Endpoint, $"/pgworker/portalloc/{cluster}", Portalloc.Serialize(
            new Dictionary<string, NodeAddress>
            {
                ["shard1/shard1a"] = new("h1", new NodePorts(15000, 18000, 16500)),
                ["shard1/shard1b"] = new("h2", new NodePorts(15000, 18000, 16500)),
                ["shard2/shard2a"] = new("h1", new NodePorts(15001, 18001, 16501)),
                ["shard2/shard2b"] = new("h2", new NodePorts(15001, 18001, 16501)),
            }), null, ct);
        await Gateway.PutAsync(Endpoint, $"/pgworker/evacuations/{cluster}/shard1",
            """{"buckets":{"0":"shard2"},"reason":"shard-dead","evacuated_unix":1,"state":"DONE","returned_unix":null}""",
            null, ct);
        var driver = new StubScaleDriver
        {
            NodeObjects = [$"pgw-{cluster}-shard1-shard1a", $"pgw-{cluster}-shard1-shard1b", $"pgw-{cluster}-shard2-shard2a"],
        };
        // Own-only предочистка клэйма (паттерн SeedAsync Backups-классов): тест не
        // зависит от таймингов TTL соседей по коллекции.
        await Gateway.DeleteAsync(Endpoint, $"/pgworker/claims/{cluster}", prefix: false, ct);
        // Own-only teardown: DisposeAsync отзывает lease — ключ исчезает сразу, не по TTL 15с.
        await using var claims = new ClaimStore([Endpoint], Gateway, TimeProvider.System);
        (await claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var process = new RemoveShardProcess(
            Gateway, [Endpoint], driver, claims, new WorkJournal(Gateway, [Endpoint]), snapshot: null);

        // Act — демонтаж на реальном etcd
        var outcome = await process.TickAsync(await SnapshotAsync(cluster), "shard1", ct);
```

Assert того же теста — все ключи от `cluster`:

```csharp
        var shardPrefix = await Gateway.RangeAsync(Endpoint, $"/clusters/{cluster}/shards/shard1/", ct);
        shardPrefix.Value.Should().BeEmpty();
        var scopePrefix = await Gateway.RangeAsync(Endpoint, $"/service/{cluster}-shard1/", ct);
        scopePrefix.Value.Should().BeEmpty();
        var portalloc = await Gateway.GetAsync(Endpoint, $"/pgworker/portalloc/{cluster}", ct);
        portalloc.Value!.Value.Should().NotContain("shard1/");
        portalloc.Value.Value.Should().Contain("shard2/");
        var evacuation = await Gateway.GetAsync(Endpoint, $"/pgworker/evacuations/{cluster}/shard1", ct);
        evacuation.Value.Should().BeNull();
        var sibling = await Gateway.GetAsync(Endpoint, $"/clusters/{cluster}/shards/shard2/dsn", ct);
        sibling.Value.Should().NotBeNull();
```

Тест `RemoveShard_останавливает_агента_WAL_и_чистит_ключ_бэкапов_AC6` — Arrange:

```csharp
        var ct = TestContext.Current.CancellationToken;
        var cluster = $"sc6{Tag}";
        await SeedActiveClusterAsync(cluster, 3);
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard1/state", "TO_REMOVE", null, ct);
        await Gateway.PutAsync(Endpoint, $"/pgworker/portalloc/{cluster}", Portalloc.Serialize(
            new Dictionary<string, NodeAddress>
            {
                ["shard1/shard1a"] = new("h1", new NodePorts(15000, 18000, 16500)),
                ["shard1/shard1b"] = new("h2", new NodePorts(15000, 18000, 16500)),
                ["shard2/shard2a"] = new("h1", new NodePorts(15001, 18001, 16501)),
                ["shard2/shard2b"] = new("h2", new NodePorts(15001, 18001, 16501)),
            }), null, ct);
        await Gateway.PutAsync(Endpoint, $"/pgworker/backups/{cluster}/shard1/wal",
            """{"state":"ACTIVE","slot":"pgw_bkp_sc6_shard1","master_node":"shard1a","chain_start_segment":"000000010000000000000001","last_received_segment":"000000010000000000000002","last_uploaded_segment":"000000010000000000000002","last_uploaded_unix":1757500000,"lag_segments":1}""",
            null, ct);
        var driver = new StubScaleDriver
        {
            NodeObjects = [$"pgw-{cluster}-shard1-shard1a", $"pgw-{cluster}-shard1-shard1b", $"pgw-{cluster}-shard2-shard2a"],
            BackupAgentObjects =
            [
                new PgWorker.Docker.Engine.DockerContainer(
                    "id-agent", [$"/pgw-backup-wal-{cluster}-shard1"], "running", "bkp-img"),
            ],
        };
        // Own-only: предочистка клэйма + немедленный выпуск (await using).
        await Gateway.DeleteAsync(Endpoint, $"/pgworker/claims/{cluster}", prefix: false, ct);
        await using var claims = new ClaimStore([Endpoint], Gateway, TimeProvider.System);
        (await claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var process = new RemoveShardProcess(
            Gateway, [Endpoint], driver, claims, new WorkJournal(Gateway, [Endpoint]), snapshot: null);

        // Act — тик RemoveShardProcess
        var outcome = await process.TickAsync(await SnapshotAsync(cluster), "shard1", ct);
```

Assert того же теста:

```csharp
        driver.RemovedBackupAgents.Should().Contain($"pgw-backup-wal-{cluster}-shard1");
        driver.BackupAgentObjects.Should().NotContain(c => c.Names.Any(n => n.Contains(cluster)));
        var backupsPrefix = await Gateway.RangeAsync(Endpoint, $"/pgworker/backups/{cluster}/shard1/", ct);
        backupsPrefix.Value.Should().BeEmpty("ключ wal не переживает демонтаж (CleanKeysAsync)");
```
(JSON-значение слота `pgw_bkp_sc6_shard1` — данные внутри ключа, никем не матчится: литерал НЕ трогаем, минимальный diff.)

Тест `ConcurrentMarkerPuts_ConvergeToSameValue`:

```csharp
        var cluster = $"sc4{Tag}";
        await SeedActiveClusterAsync(cluster, 2);
        var ct = TestContext.Current.CancellationToken;

        // Act — два параллельных PUT
        var puts = await Task.WhenAll(
            Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard1/state", "TO_REMOVE", null, ct),
            Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard1/state", "TO_REMOVE", null, ct));

        // Assert — оба успеха; значение ровно "TO_REMOVE"
        puts.Should().OnlyContain(p => p.IsSuccess);
        var read = await Gateway.GetAsync(Endpoint, $"/clusters/{cluster}/shards/shard1/state", ct);
        read.Value!.Value.Should().Be("TO_REMOVE");
```

Тест `AddShardDeclaration_ThenMarker_UndeclaredShardDismantledOnRealEtcd` — Arrange:

```csharp
        var ct = TestContext.Current.CancellationToken;
        var cluster = $"sc5{Tag}";
        await SeedActiveClusterAsync(cluster, 2);
        await SeedAddDeclarationAsync(cluster, "shard3");
        await Gateway.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard3/state", "TO_REMOVE", null, ct);
        // Own-only: предочистка клэйма + немедленный выпуск (await using).
        await Gateway.DeleteAsync(Endpoint, $"/pgworker/claims/{cluster}", prefix: false, ct);
        await using var claims = new ClaimStore([Endpoint], Gateway, TimeProvider.System);
        (await claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var process = new RemoveShardProcess(
            Gateway, [Endpoint], new StubScaleDriver(), claims,
            new WorkJournal(Gateway, [Endpoint]), snapshot: null);

        // Act
        var outcome = await process.TickAsync(await SnapshotAsync(cluster), "shard3", ct);
```

Assert того же теста:

```csharp
        var shardPrefix = await Gateway.RangeAsync(Endpoint, $"/clusters/{cluster}/shards/shard3/", ct);
        shardPrefix.Value.Should().BeEmpty();
        var scopePrefix = await Gateway.RangeAsync(Endpoint, $"/service/{cluster}-shard3/", ct);
        scopePrefix.Value.Should().BeEmpty();
```

Хелперы `SeedActiveClusterAsync` / `SeedAddDeclarationAsync` / `SnapshotAsync` уже принимают имя кластера параметром — их код не меняется (ключи внутри уже интерполированы от `{cluster}`).

- [ ] **Step 3: Точечная проверка класса**

Run:
```bash
cd /Users/demakaev/ZCodeProject/worktrees/t12-integration-red-debt && DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter FullyQualifiedName~ShardScaleContractTests
```
Expected: `passed: 6, failed: 0`. Зачистка docker (команда из Task 1 Step 1).

- [ ] **Step 4: Commit**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/t12-integration-red-debt && git add src/tests/PgWorker.IntegrationTests/Etcd/ShardScaleContractTests.cs && git commit -m "test(scale): per-class guid-тег имён кластеров sc1..sc6 + own-only клэйм (предочистка /pgworker/claims/<C> перед TryClaim, await using ClaimStore — lease отзывается в teardown, не по TTL 15с) в ShardScaleContractTests; t12 spec §4.2"
```

---

### Task 3: BackupSupervisorProcessTests — guid-теги + teardown клэйма + инвариант фикстуры (spec §4.2 п.1/3/4, Фаза 2)

**Files:**
- Modify: `src/tests/PgWorker.IntegrationTests/Backups/BackupSupervisorProcessTests.cs` (класс + все 3 теста)
- Modify: `src/tests/PgWorker.IntegrationTests/Etcd/EtcdFixture.cs` (комментарий `EtcdCollection`, строка ~102)

**Interfaces:**
- Consumes: `ClaimStore.IAsyncDisposable` (те же семантики, что в Task 2); `FakeBackupS3` (`Backups/FakeBackupDeps.cs`) — S3-ключи строятся от `cluster` (`PrefixObjects`: полные ключи `<cluster>/shard1/full/...`; `Objects`: кортеж `(cluster, shard, name)`); xUnit v3 `IAsyncLifetime` (using `Xunit` уже есть в файле).
- Produces: инвариант, зафиксированный в комментарии `EtcdCollection` (правило для всех будущих классов коллекции); тот же Tag-паттерн, что в Task 2.

- [ ] **Step 1: Класс — Tag, IAsyncLifetime, teardown поля `_claims`**

Заголовок класса и новые члены (заменить текущий заголовок `public class BackupSupervisorProcessTests(EtcdFixture fixture)` и строку поля `_claims`):

```csharp
[Collection(EtcdCollection.Name)]
public class BackupSupervisorProcessTests(EtcdFixture fixture) : IAsyncLifetime
{
    // Per-class guid-тег (канон docs/e2e-isolation.md §1): имена кластеров уникальны
    // per-class-запуск — пересечение с ShardScaleContractTests (sc1..sc6) и любым
    // будущим классом EtcdCollection механически невозможно (инцидент t07: клэйм
    // sc3 этого класса жил 15с по TTL и ронял TryClaim жертвы).
    private static readonly string Tag = Guid.NewGuid().ToString("N")[..8];

    private readonly ClaimStore _claims = new([fixture.Endpoint], fixture.Gateway, TimeProvider.System);

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    // Own-only teardown клэйма (t12 spec §4.2 п.3): DisposeAsync отзывает lease —
    // живых /pgworker/claims/<C> после тестов класса не остаётся, без TTL-ожидания.
    public ValueTask DisposeAsync() => _claims.DisposeAsync();
```

- [ ] **Step 2: Тегирование имён в трёх тестах (все ключи/префиксы/снапшоты — от `cluster`)**

Тест `Проход_мусор_живого_шарда_удаляется_идемпотентно` (замены литералов `"sc1"` → `cluster`, дефолт `BuildSnap` не используется — все вызовы с явным аргументом):

```csharp
        var ct = TestContext.Current.CancellationToken;
        var cluster = $"sc1{Tag}";
        await SeedAsync(cluster);
        var s3 = new FakeBackupS3();
        s3.PrefixObjects.Add(($"{cluster}/shard1/full/20260911110000Z/backup_manifest", 100));
        s3.PrefixObjects.Add(($"{cluster}/shard1/full/20260911110000Z/backup_label", 50));
        s3.PrefixObjects.Add(($"{cluster}/shard1/full/20260911120000Z/backup_manifest", 100));
        s3.Objects.Add((cluster, "shard1", "000000010000000000000001"));
        var owned = new FullBackupState("20260911110000Z", FullBackupStatus.Completed, "shard1a",
            BackupSourceRole.Replica, 1757500000, 1757500300, "000000010000000000000001", 150, null, null);
        var process = BuildProcess(Options(), s3);
        var backups = new ClusterBackups(cluster, null,
            new Dictionary<string, ShardBackups> { ["shard1"] = FullsOf(owned) });

        // Act 1 — проход супервизора
        (await process.TickAsync(BuildSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert 1 — объекты B удалены, A и wal/ живы; journal-факт swept-full
        s3.DeletedKeys.Should().Contain($"{cluster}/shard1/full/20260911120000Z/backup_manifest");
        s3.DeletedKeys.Should().HaveCount(1, "владеемый префикс и wal/ не трогаются");
        s3.PrefixObjects.Should().Contain(o => o.Key == $"{cluster}/shard1/full/20260911110000Z/backup_manifest");
        var journal = await fixture.Gateway.GetAsync(fixture.Endpoint, $"/pgworker/work/{cluster}", ct);
        journal.Value!.Value.Should().Contain("swept-full/shard1/20260911120000Z");

        // Act 2 — повторный проход
        (await process.TickAsync(BuildSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert 2 — новых удалений нет (list подтверждает пустоту — no-op)
        s3.DeletedKeys.Should().HaveCount(1, "идемпотентность: повторный проход ничего не удаляет");
```

Тест `Проход_скипает_шард_в_активном_restore`:

```csharp
        var ct = TestContext.Current.CancellationToken;
        var cluster = $"sc2{Tag}";
        await SeedAsync(cluster);
        var s3 = new FakeBackupS3();
        s3.PrefixObjects.Add(($"{cluster}/shard1/full/20260911120000Z/backup_manifest", 100));
        var process = BuildProcess(Options(), s3);
        var restoring = new ShardBackups([], null,
            [new RestoreOperationState("20260911130000Z", RestoreStatus.Planned,
                "", $"{cluster}/shard1", "latest", "shard1a", 1760000000, "operator")]);
        var backups = new ClusterBackups(cluster, null,
            new Dictionary<string, ShardBackups> { ["shard1"] = restoring });

        // Act
        (await process.TickAsync(BuildSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — объекты живы (гвард владельца), журнал-фактов нет
        s3.DeletedKeys.Should().BeEmpty("restore владеет шардом — сверка не выполняется");
        s3.PrefixObjects.Should().ContainSingle();
```

Тест `Гварды_Enabled_и_клэйм` (локальный `claims2` — тоже `await using`, spec §4.2 п.3 «локальные ClaimStore»):

```csharp
        // Arrange 1 — Enabled=false: проход не выполняется
        var ct = TestContext.Current.CancellationToken;
        var cluster = $"sc3{Tag}";
        await SeedAsync(cluster);
        var s3 = new FakeBackupS3();
        s3.PrefixObjects.Add(($"{cluster}/shard1/full/20260911120000Z/backup_manifest", 100));
        var disabled = BuildProcess(null, s3);
        (await disabled.TickAsync(BuildSnap(cluster), null, ct)).IsSuccess.Should().BeTrue();
        s3.DeletedKeys.Should().BeEmpty("Enabled=false — no-op");

        // Arrange 2 — не-Active кластер: Done без list
        var s3b = new FakeBackupS3();
        s3b.PrefixObjects.Add(($"{cluster}/shard1/full/20260911120000Z/backup_manifest", 100));
        var process = BuildProcess(Options(), s3b);
        var notActive = new ClusterSnapshot(
            new ClusterConfig(cluster, 2, cluster, null, ClusterState.NotInitialized),
            [new ShardSpec("shard1", 1, $"host=shard1a dbname={cluster}", "shard1a:17001",
                [new NodeSpec("shard1", "shard1a", NodeState.Running)])],
            []);
        (await process.TickAsync(notActive, null, ct)).IsSuccess.Should().BeTrue();
        s3b.DeletedKeys.Should().BeEmpty("не-Active — no-op");

        // Arrange 3 — чужой клэйм: отказ до любых мутаций
        var s3c = new FakeBackupS3();
        s3c.PrefixObjects.Add(($"{cluster}/shard1/full/20260911120000Z/backup_manifest", 100));
        await using var claims2 = new ClaimStore([fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        var other = new BackupSupervisorProcess(
            fixture.Gateway, [fixture.Endpoint], s3c, claims2,
            new WorkJournal(fixture.Gateway, [fixture.Endpoint]),
            () => Options(), TimeProvider.System);
        (await other.TickAsync(BuildSnap(cluster), null, ct)).IsSuccess.Should().BeFalse(
            "мутации без клэйма запрещены");
        s3c.DeletedKeys.Should().BeEmpty();
```

Хелперы `SeedAsync`/`BuildProcess`/`Options`/`FullsOf` не меняются (уже параметризованы именем кластера); дефолт `BuildSnap(string cluster = "c1")` НЕ трогается (латентное пересечение `c1` — осознанное ограничение скоупа, spec §4.3).

- [ ] **Step 3: Фиксация инварианта в `EtcdFixture.cs`**

Заменить комментарий над `EtcdCollection` (строка ~102):

```csharp
// Один etcd-контейнер на все contract/coordination-классы. Инвариант непересечения
// ключей держат сами классы: имена кластеров ОБЯЗАНЫ нести per-class guid-тег
// (канон docs/e2e-isolation.md §1), клэймящие тесты чистят /pgworker/claims/<C>
// перед TryClaim и отпускают клэйм в teardown (await using / IAsyncLifetime) —
// литеральная коллизия имён (инцидент t07: sc3 BackupSupervisor × ShardScale)
// даёт флейк полной сборки из-за lease-TTL 15с.
```

- [ ] **Step 4: Точечная проверка класса**

Run:
```bash
cd /Users/demakaev/ZCodeProject/worktrees/t12-integration-red-debt && DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter FullyQualifiedName~BackupSupervisorProcessTests
```
Expected: `passed: 3, failed: 0`. Зачистка docker (команда из Task 1 Step 1).

- [ ] **Step 5: Commit**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/t12-integration-red-debt && git add src/tests/PgWorker.IntegrationTests/Backups/BackupSupervisorProcessTests.cs src/tests/PgWorker.IntegrationTests/Etcd/EtcdFixture.cs && git commit -m "test(backups): per-class guid-тег имён кластеров + IAsyncLifetime teardown клэйма в BackupSupervisorProcessTests, инвариант EtcdCollection зафиксирован в EtcdFixture (guid-тег + предочистка + выпуск клэйма); t12 spec §4.2"
```

---

### Task 4: Полная приёмка — юниты + интеграционная серия ×2 (spec Фаза 3, критерии §7)

**Files:**
- Create: ничего. Modify: ничего. (Проверочный task — код уже из Tasks 1–3.)

**Interfaces:**
- Consumes: зелёные точечные прогоны Tasks 1–3.
- Produces: доказательство критериев приёмки spec §7 п.1–7 (Adoption 3/3; полная серия зелёная дважды подряд; guid-теги; предочистка/teardown клэйма; без sleep; юниты зелёные; docker чист).

Правила прогона: каждую серию — до финальной строки итога; между сериями — зачистка docker; красный прогон → полный разбор вывода прогона (ассерты, стектрейсы), фикс, счётчик «дважды подряд» обнуляется. Перезапуск «посмотреть, что было» запрещён.

- [ ] **Step 1: Юнит-серия (код тестов компилируется, санити `TreatWarningsAsErrors`)**

Run:
```bash
cd /Users/demakaev/ZCodeProject/worktrees/t12-integration-red-debt && DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release
```
Expected: `Passed!` (failed: 0). Правок юнитов не было — регрессии исключены, прогон безусловный по spec Ф3.

- [ ] **Step 2: Полная интеграционная серия — прогон 1**

Run:
```bash
cd /Users/demakaev/ZCodeProject/worktrees/t12-integration-red-debt && DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release
```
Expected: `Passed! ... failed: 0` (полная серия; E2e-сценарии скипнуты гейтом `PGW_TEST_DOCKER` — их включение в этой задаче не требуется: prod-код не менялся). Записать итоговую строку (passed/skipped).

- [ ] **Step 3: Зачистка docker после серии 1 + контроль чистоты**

```bash
for id in $(docker ps -aq); do name=$(docker inspect -f '{{.Name}}' "$id" | tr -d '/'); case "$name" in as-*|adminpanel*) ;; *) docker rm -f "$id" ;; esac; done
docker network prune -f
docker ps -a --format '{{.Names}}' | grep -Ev '^(as-|adminpanel)' | wc -l   # 0 (или пусто)
docker network ls --format '{{.Name}}' | grep -Ec 'kfw-net|pgw-'            # 0
```
Expected: обе контрольные команды печатают `0` (при поднятом dev-стенде из списка контейнеров исключены только `as-*`/`adminpanel`).

- [ ] **Step 4: Полная интеграционная серия — прогон 2 (доказательство нефлейковости)**

Run:
```bash
cd /Users/demakaev/ZCodeProject/worktrees/t12-integration-red-debt && DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release
```
Expected: `Passed! ... failed: 0` — оба прогона подряд зелёные (spec §7 п.2). Любое красное → разбор вывода → фикс → оба прогона повторяются заново.

- [ ] **Step 5: Финальная зачистка docker (после серии 2)**

Та же пара команд зачистки + контроля, что в Step 3. Expected: `0` и `0`.

- [ ] **Step 6: Self-check по критериям приёмки spec §7**

Проверить по git diff и итогам прогонов (все пункты обязаны выполняться):
1. `git diff main...HEAD --stat` — изменены ТОЛЬКО файлы `src/tests/PgWorker.IntegrationTests/` (никакого prod, никакого `arch/`).
2. Adoption 3/3 + сиды `request_cpu`/`request_mem` в формате панели (Task 1).
3. Оба класса несут `Tag = Guid.NewGuid().ToString("N")[..8]`, имена `scN{Tag}` (Tasks 2–3).
4. Клэймящие тесты ShardScale (sc3/sc5/sc6) делают `DeleteAsync /pgworker/claims/{cluster}` перед `TryClaim`; все локальные `ClaimStore` — `await using`; `_claims` BackupSupervisor выпускается в `IAsyncLifetime.DisposeAsync`.
5. В диффе нет `Task.Delay`/`Thread.Sleep` (греп: `git diff main...HEAD | grep -nE 'Task\.Delay|Thread\.Sleep'` — пусто).
6. Юниты зелёные, обе серии зелёные, docker чист.

Коммита нет (код не менялся). Слияние — отдельный этап (finishing-a-development-branch): мерж-коммит в `main` обязан тем же коммитом снять тег `t12-integration-red-debt` из `arch/roadmap/pgworker.md` (строка ~36) — мерж-гейт по AGENTS.md; мерж/пуш — только по явной просьбе пользователя.

---

## Самопроверка плана (выполнена автором)

- **Покрытие spec:** §4.1 → Task 1; §4.2 п.1 (теги обоих классов) → Tasks 2–3; п.2 (предочистка sc3/sc5/sc6) → Task 2; п.3 (`await using`/`IAsyncLifetime`) → Tasks 2–3; п.4 (инвариант фикстуры) → Task 3 Step 3; Ф3/§7 (серия ×2, юниты, зачистка, self-check) → Task 4; §4.3 (не трогаем c1/Adoption-теги/фикстуру портов) — Global Constraints. Пробелов нет.
- **Плейсхолдеры:** код правок приведён полностью; «и т.д.»-шагов нет.
- **Консистентность имён:** `Tag` (static readonly, `Guid.NewGuid().ToString("N")[..8]`), локальная `cluster`, сигнатура `DeleteAsync(Endpoint, key, prefix: false, ct)` сверена с `SeedAsync` Backups-классов; `ClaimStore.DisposeAsync` существует (`IAsyncDisposable`, `ClaimStore.cs:196`); матчинг `pgw-{cluster}-{shardName}-` (`RemoveShardProcess.cs:98`) и `BackupAgentNames.Container` подтверждены по коду.
