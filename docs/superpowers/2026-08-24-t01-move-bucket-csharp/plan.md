# t01-move-bucket-csharp — план реализации

> **Для агентных исполнителей:** ОБЯЗАТЕЛЬНЫЙ SUB-SKILL: `superpowers:subagent-driven-development` (рекомендуется) или `superpowers:executing-plans` — выполнять по задачам. Шаги — чекбоксы (`- [ ]`).

**Цель:** перенести плановый онлайн-переезд бакета между шардами (механика P1–P8, эквивалент `arch/scripts/move-bucket.sh` + `abort-move.sh`) в PgWorker как управляемый тиковый процесс по заявкам в etcd.

**Архитектура:** новый проект `src/PgWorker.Moves` (машина состояний `MoveProcess` M0–M6, `CutoverSequence`, `AbortSequence`, SQL-билдеры `MoveSql`, заявки/статус-сторы поверх существующего `IEtcdGateway`) + расширения `IDockerEngine`/`IClusterDriver` (exec для pg_dump) + интеграция в `ReconcileLoop`/`IClusterProcesses`/`DeprovisioningProcess`. Формат статус-ключа и имена артефактов — 1:1 со скриптами.

**Технологии:** .NET 10 (`LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`), Npgsql 10.0.3 (уже в CPM), Polly 8 (`RetryPolicies.SqlRetry`), xunit.v3 + FluentAssertions, Testcontainers (integration/e2e). Новых NuGet-пакетов НЕ добавлять.

**Спецификация:** `docs/superpowers/2026-08-24-t01-move-bucket-csharp/spec.md` (аргументация — там; план следует спеку, исполнитель читает оба документа).

**Worktree:** `/Users/demakaev/ZCodeProject/worktrees/feat-t01-move-bucket-csharp` (все пути ниже — относительно него, если не указано абсолютное).

## Глобальные ограничения

- Сборка: `dotnet build src/PgWorker.slnx -c Release` — 0 warnings (`TreatWarningsAsErrors=true`); каждый шаг проверки — `-warnaserror`.
- Тесты: комментарии по AAA (`// Arrange / // Act / // Assert`), русский текст сообщений `Should()...Be(..., "пояснение")`. Xunit — implicit using (csproj), FluentAssertions — global using.
- Версии пакетов — только из `src/Directory.Packages.props` (CPM, `EnablePackageVersionOverride=false`).
- Идентификаторы в коде — английские; комментарии/XML-doc — русские.
- Пароли не попадают в тексты ошибок: переиспользовать паттерн `DatabaseProvisioner.Redact` (продублировать локально; см. Task 7).
- Коммиты: `t01: <краткое описание по-русски>` (тег задачи по правилам `arch/roadmap/README.md`).
- Формат статус-ключа `/clusters/<C>/buckets/status/bucket_<i>` и имена `pub_<bucket>` / `sub_<bucket>` / `pub_<bucket>_rb` / `sub_<bucket>_rb` — точно как в скриптах (совместимость, spec Д6).
- App-роль — константа `"app"` (её создаёт provisioning; грант CREATE app-роли никогда не выдавался — unfreeze без `GRANT CREATE`, как дефолт скриптов `APP_GRANT_CREATE=0`).
- Имя бакета/шарда валидируется `^[a-z][a-z0-9_]*$` перед подстановкой в SQL (защита от инъекций, паттерн `DatabaseProvisioner.ValidateIdentifier`).
- Семантика отказов cutover (spec §6.2): **transient** (заявка жива, ретраи тиками; разморозка при отказе после успешной заморозки) — freeze-failed / lsn-failed / catchup-timeout / sequences-failed; **permanent** (del заявки + work.last_error с подсказкой; заморозка при flip-conflict ОСТАВЛЕНА) — verify-failed (дефектная копия → «abort + повторный move») и flip-conflict. Классификация — по типу исключения `CutoverPermanentException` (Task 12).

---

### Task 1: arch-правки (arch-first, ДО кода)

**Files:**
- Modify: `arch/14-pgworker.md`
- Modify: `arch/11-bucket-sharding.md`

**Interfaces:** Consumes: spec §4, §5, §6, §12. Produces: канон, на который ссылаются XML-doc новых классов (`arch/14 §5 F`, `arch/14 §3.3`).

- [ ] **Step 1: arch/14-pgworker.md — ключи заявок**

В §3.3 (таблица «НОВЫЕ ключи координации воркеров») добавить строку:

```markdown
| `/pgworker/moves/<C>/bucket_<i>` | обычный | заявка на плановый переезд/откат/уборку/отмену (t01): `{"op":"move\|rollback\|finalize\|abort","to":…,"old_shard":…,"skip_reverse":…,"resume":…,"force":…,"requested_unix":…,"requested_by":…}`. Успех или перманентный валидационный отказ → ключ удаляется; transient-сбой → остаётся, фазы — в статус-ключе бакета. Обрабатывается только держателем клэйма `<C>`; одновременно — старейшая заявка кластера. Deprovisioning D2 чистит `/pgworker/moves/<C>/` (префикс). |
```

- [ ] **Step 2: arch/14-pgworker.md — процесс F. MoveProcess**

В §5 после «D. BucketEvacuator» добавить подраздел (краткая выжимка spec §6: M0–M6, cutover подшаги 1–7 с классификацией отказов transient/permanent, abort-журнал ABORTING, rollback/finalize; заявки; снапшот-точки move-start обязателен / flip best-effort; подписки `copy_data=true` при move, `copy_data=false` при обратной, `failover` (PG17+, конфиг `FailoverSlots`), `synchronous_commit=remote_apply`; статус-ключ в формате скриптов — скрипты и PgWorker взаимозаменяемы на разборе, но не смешивать в одном окне переезда — у скрипта нет клэйма). В разделе «Границы» (вступление доуна) убрать «плановые переезды бакетов (move P1–P8 остаются скриптами … — roadmap)» и заменить на: «CLI-обёртки заявок и панельные кнопки переездов — roadmap (t06); ручной скриптовый путь остаётся для стендов без PgWorker — не смешивать с заявками в одном окне переезда». В §3.2/§5 B (deprovisioning D2) дописать `/pgworker/moves/<C>/` в список чистки. В §8 (конфигурация) добавить секцию:

```
PgWorker:Moves { PollIntervalSec=2, FreezeWaitSec=5, FreezeLockTimeoutSec=5,
                 FreezeLockTries=3, AbortMinAgeSec=120, FailoverSlots=true }
PgWorker:Thresholds { … CutoverTimeoutSec=90, ConnFailBudgetSec=120 }
```

- [ ] **Step 3: arch/11-bucket-sharding.md — указатель C#-пути**

В §5 «Автоматизация» после списка команд добавить абзац:

```markdown
> **C#-путь (t01):** в кластерах под управлением PgWorker эти же операции
> выполняет оркестратор по заявкам в etcd (`/pgworker/moves/<C>/bucket_<i>`,
> формат статуса и имена артефактов pub_/sub_ — идентичны скриптам; контракт —
> [14-pgworker.md](14-pgworker.md) §3.3/§5 F). Скрипты остаются ручным путём
> для стендов/кластеров вне PgWorker; одновременно скриптом и заявкой один
> бакет не переезжать.
```

- [ ] **Step 4: Проверка текстовая**

Run: `grep -c 'pgworker/moves' arch/14-pgworker.md` → ≥ 3; `grep -c 'MoveProcess' arch/14-pgworker.md` → ≥ 1; `grep -c 'C#-путь' arch/11-bucket-sharding.md` → 1.

- [ ] **Step 5: Commit**

```bash
git add arch/14-pgworker.md arch/11-bucket-sharding.md
git commit -m "t01: arch — процесс MoveProcess, заявки /pgworker/moves/, чистка при deprovisioning"
```

---

### Task 2: Проект PgWorker.Moves + модель (MoveNames, MoveRequest, MoveStatus, AbortJournal)

**Files:**
- Create: `src/PgWorker.Moves/PgWorker.Moves.csproj`
- Modify: `src/PgWorker.slnx` (папка `/moves/`)
- Create: `src/PgWorker.Moves/Model/MoveNames.cs`
- Create: `src/PgWorker.Moves/Model/MoveRequest.cs`
- Create: `src/PgWorker.Moves/Model/MoveStatus.cs`
- Test: `src/tests/PgWorker.UnitTests/Moves/ModelTests.cs`

**Interfaces:**
- Consumes: `PgWorker.Core.Result`, `System.Text.Json`.
- Produces (используют Tasks 3–17): `MoveNames` (статические `Pub/Sub/PubRb/SubRb/RoutingKey/StatusKey/MoveKey/MovesPrefix/ValidateIdentifier`), `MoveOp { Move, Rollback, Finalize, Abort }`, `MoveRequest(Bucket, Op, To, OldShard, SkipReverse, Resume, Force, RequestedUnix, RequestedBy)` + `MoveRequest.Parse(bucket, json): Result<MoveRequest>`, `MoveStatus(Bucket, State, Owner, Target, StartedUnix, UpdatedUnix, Phase)` + `Serialize()/Parse`, `MoveStates { Syncing, Frozen, Aborting }` (константы строк), `AbortJournal(...)` + `Serialize()/Parse`.

- [ ] **Step 1: csproj + slnx**

`src/PgWorker.Moves/PgWorker.Moves.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">

    <ItemGroup>
        <ProjectReference Include="..\PgWorker.Core\PgWorker.Core.csproj"/>
        <ProjectReference Include="..\PgWorker.Etcd\PgWorker.Etcd.csproj"/>
        <ProjectReference Include="..\PgWorker.Docker\PgWorker.Docker.csproj"/>
        <ProjectReference Include="..\PgWorker.Provisioning\PgWorker.Provisioning.csproj"/>
    </ItemGroup>

</Project>
```

Обоснование (фиксирую в комментарии XML-doc проекта): зависимость от Provisioning нужна для `ProcessOutcome`, `ShardProbe`, `SnapshotJob`-делегата и `ISqlExecutor`-паттернов — переносить их в Core вне скоупа t01 (спек §5.1 допускал Core/Etcd/Docker; расширение обосновано, отметить в spec не нужно — план уточняет реализацию).

В `src/PgWorker.slnx` внутрь `<Solution>` после папки `/provisioning/`:

```xml
    <Folder Name="/moves/">
        <Project Path="PgWorker.Moves/PgWorker.Moves.csproj" />
    </Folder>
```

В `src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj` добавить `<ProjectReference Include="..\..\PgWorker.Moves\PgWorker.Moves.csproj"/>`.

- [ ] **Step 2: Пишем failing-тесты модели**

`src/tests/PgWorker.UnitTests/Moves/ModelTests.cs`:

```csharp
using System.Text.Json;
using PgWorker.Moves;

namespace PgWorker.UnitTests.Moves;

public class ModelTests
{
    // AAA: сериализация статуса — формат 1:1 со скриптами (spec §4.2, Д6)
    [Fact]
    public void MoveStatus_Serialize_MatchesScriptFormat()
    {
        // Arrange
        var s = new MoveStatus("bucket_42", MoveStates.Syncing, "shard1", "shard2",
            1770000000, 1770000100, "copy-wait");

        // Act
        var json = JsonDocument.Parse(s.Serialize()).RootElement;

        // Assert
        json.GetProperty("bucket").GetString().Should().Be("bucket_42");
        json.GetProperty("state").GetString().Should().Be("SYNCING");
        json.GetProperty("owner").GetString().Should().Be("shard1");
        json.GetProperty("target").GetString().Should().Be("shard2");
        json.GetProperty("started_unix").GetInt64().Should().Be(1770000000);
        json.GetProperty("updated_unix").GetInt64().Should().Be(1770000100);
        json.GetProperty("phase").GetString().Should().Be("copy-wait");
    }

    // AAA: парсинг заявки — все поля, дефолты опциональных
    [Fact]
    public void MoveRequest_Parse_FullAndMinimal()
    {
        // Arrange
        var full = """{"op":"move","to":"shard2","skip_reverse":true,"resume":true,"force":true,"requested_unix":1770000000,"requested_by":"op"}""";

        // Act
        var parsed = MoveRequest.Parse("bucket_42", full);

        // Assert
        parsed.Value!.Op.Should().Be(MoveOp.Move);
        parsed.Value.To.Should().Be("shard2");
        parsed.Value.SkipReverse.Should().BeTrue();
        parsed.Value.Resume.Should().BeTrue();
        parsed.Value.Force.Should().BeTrue();
        parsed.Value.RequestedBy.Should().Be("op");

        // Arrange
        var minimal = """{"op":"rollback","requested_unix":5}""";

        // Act
        var min = MoveRequest.Parse("bucket_7", minimal);

        // Assert
        min.Value!.Op.Should().Be(MoveOp.Rollback);
        min.Value.To.Should().BeNull();
        min.Value.Force.Should().BeFalse();
    }

    // AAA: битый/чужой JSON и неизвестный op — Result.Failed (заявка будет отвергнута)
    [Theory]
    [InlineData("not json")]
    [InlineData("""{"op":"nonsense","requested_unix":1}""")]
    public void MoveRequest_Parse_RejectsGarbage(string raw)
    {
        // Act
        var parsed = MoveRequest.Parse("bucket_42", raw);

        // Assert
        parsed.IsSuccess.Should().BeFalse("битая заявка не должна молча съедаться");
    }

    // AAA: имена ключей/артефактов — конвенция скриптов
    [Fact]
    public void MoveNames_KeysAndArtifacts_MatchScripts()
    {
        // Assert
        MoveNames.Pub("bucket_42").Should().Be("pub_bucket_42");
        MoveNames.Sub("bucket_42").Should().Be("sub_bucket_42");
        MoveNames.PubRb("bucket_42").Should().Be("pub_bucket_42_rb");
        MoveNames.SubRb("bucket_42").Should().Be("sub_bucket_42_rb");
        MoveNames.RoutingKey("shop", "bucket_42").Should().Be("/clusters/shop/buckets/routing/bucket_42");
        MoveNames.StatusKey("shop", "bucket_42").Should().Be("/clusters/shop/buckets/status/bucket_42");
        MoveNames.MoveKey("shop", "bucket_42").Should().Be("/pgworker/moves/shop/bucket_42");
        MoveNames.MovesPrefix("shop").Should().Be("/pgworker/moves/shop/");
        MoveNames.ValidateIdentifier("bucket_42").Should().BeTrue();
        MoveNames.ValidateIdentifier("B;DROP").Should().BeFalse();
    }
}
```

- [ ] **Step 3: Запуск — ожидаем FAIL (нет проекта/типов)**

Run: `dotnet test src/tests/PgWorker.UnitTests -f net10.0` → ошибка компиляции (`PgWorker.Moves` не найден).

- [ ] **Step 4: Реализация модели**

`src/PgWorker.Moves/Model/MoveNames.cs`:

```csharp
using System.Text.RegularExpressions;

namespace PgWorker.Moves;

/// <summary>Строки state статус-ключа (формат скриптов, spec §4.2; нет ключа = ACTIVE).</summary>
public static class MoveStates
{
    public const string Syncing = "SYNCING";
    public const string Frozen = "FROZEN";
    public const string Aborting = "ABORTING";
}

/// <summary>App-роль, чей write-доступ срезается заморозкой P1 (создаёт provisioning).</summary>
public static class MoveNames
{
    public const string AppRole = "app";
    public const string MoverRole = "bucket_mover";

    [GeneratedRegex("^[a-z][a-z0-9_]*$", RegexOptions.CultureInvariant)]
    private static partial Regex IdentifierRegex();

    public static bool ValidateIdentifier(string name) => IdentifierRegex().IsMatch(name);

    public static string Pub(string bucket) => $"pub_{bucket}";
    public static string Sub(string bucket) => $"sub_{bucket}";
    public static string PubRb(string bucket) => $"pub_{bucket}_rb";
    public static string SubRb(string bucket) => $"sub_{bucket}_rb";

    public static string RoutingKey(string cluster, string bucket) => $"/clusters/{cluster}/buckets/routing/{bucket}";
    public static string StatusKey(string cluster, string bucket) => $"/clusters/{cluster}/buckets/status/{bucket}";
    public static string MoveKey(string cluster, string bucket) => $"/pgworker/moves/{cluster}/{bucket}";
    public static string MovesPrefix(string cluster) => $"/pgworker/moves/{cluster}/";
}
```

`MoveRequest.cs` и `MoveStatus.cs` — records с `[JsonPropertyName]`-атрибутами (`bucket/state/owner/target/started_unix/updated_unix/phase`; заявка: `op/to/old_shard/skip_reverse/resume/force/requested_unix/requested_by`, `DefaultIgnoreCondition = WhenWritingNull`), `Parse` через `JsonSerializer.Deserialize` с catch `JsonException` → `Result.Failed` (образец толерантного парсинга — `WorkJournal.ReadAsync`); `op` маппится строкой (`"move"→MoveOp.Move` и т.д., неизвестное → Failed). `AbortJournal.cs` (в `Model/MoveStatus.cs` тем же файлом или рядом): record `AbortJournal(Bucket, PrevState, Owner, Target, StartedUnix, UpdatedUnix, Phase, LastError, Plan: IReadOnlyList<AbortPlanItem>, UnreachableShards: IReadOnlyList<string>)`, `AbortPlanItem(Shard, Kind, Name)` — kind `sub|slot|pub|schema`; сериализация с `state:"ABORTING"` (в Serialize пишет константу, как `journal_set` скрипта).

- [ ] **Step 5: Запуск — PASS + сборка решения**

Run: `dotnet test src/tests/PgWorker.UnitTests -f net10.0 --filter ModelTests` → PASS; `dotnet build src/PgWorker.slnx -c Release -warnaserror` → 0 warnings.

- [ ] **Step 6: Commit**

```bash
git add src/PgWorker.Moves src/PgWorker.slnx src/tests/PgWorker.UnitTests
git commit -m "t01: проект PgWorker.Moves + модель заявок/статуса (формат скриптов 1:1)"
```

---

### Task 3: MoveRequestsStore (заявки в etcd)

**Files:**
- Create: `src/PgWorker.Moves/Requests/MoveRequestsStore.cs`
- Test: `src/tests/PgWorker.UnitTests/Moves/MoveRequestsStoreTests.cs`

**Interfaces:**
- Consumes: `IEtcdGateway` (`RangeAsync/GetAsync/DeleteAsync`), `FakeEtcd` из `tests/PgWorker.UnitTests/Provisioning/Fakes.cs`.
- Produces: `MoveRequestsStore(IEtcdGateway gateway, string[] endpoints)`:
  - `Task<Result<IReadOnlyList<(string Bucket, MoveRequest Request)>>> ListAsync(string cluster, CancellationToken ct)`
  - `Task<Result<(string Bucket, MoveRequest Request)?>> OldestAsync(string cluster, CancellationToken ct)` — минимальный `RequestedUnix`, tie-break — лексикографика ключа (детерминизм).
  - `Task<Result> DeleteAsync(string cluster, string bucket, CancellationToken ct)`

- [ ] **Step 1: Failing-тесты**

```csharp
using PgWorker.Etcd.Client;
using PgWorker.Moves;
using PgWorker.UnitTests.Provisioning;

namespace PgWorker.UnitTests.Moves;

public class MoveRequestsStoreTests
{
    private static MoveRequestsStore StoreOf(FakeEtcd etcd) => new(etcd, ["http://x"]);

    // AAA: range по префиксу кластера возвращает только его заявки
    [Fact]
    public async Task ListAsync_ReturnsOnlyClusterRequests()
    {
        // Arrange
        var etcd = new FakeEtcd();
        etcd.Seed(MoveNames.MoveKey("shop", "bucket_1"), """{"op":"move","to":"shard2","requested_unix":20}""");
        etcd.Seed(MoveNames.MoveKey("shop", "bucket_2"), """{"op":"abort","force":true,"requested_unix":10}""");
        etcd.Seed(MoveNames.MoveKey("other", "bucket_1"), """{"op":"move","to":"shard2","requested_unix":5}""");

        // Act
        var list = await StoreOf(etcd).ListAsync("shop", CancellationToken.None);

        // Assert
        list.Value.Should().HaveCount(2, "чужой кластер не попадает в выборку");
    }

    // AAA: старейшая заявка — по requested_unix (Д2: одна активная заявка на кластер)
    [Fact]
    public async Task OldestAsync_PicksMinRequestedUnix()
    {
        // Arrange
        var etcd = new FakeEtcd();
        etcd.Seed(MoveNames.MoveKey("shop", "bucket_1"), """{"op":"move","to":"shard2","requested_unix":20}""");
        etcd.Seed(MoveNames.MoveKey("shop", "bucket_2"), """{"op":"abort","requested_unix":10}""");

        // Act
        var oldest = await StoreOf(etcd).OldestAsync("shop", CancellationToken.None);

        // Assert
        oldest.Value!.Value.Bucket.Should().Be("bucket_2");
    }

    // AAA: удаление заявки по завершении (успех/перманентный отказ, spec §4.1)
    [Fact]
    public async Task DeleteAsync_RemovesKey()
    {
        // Arrange
        var etcd = new FakeEtcd();
        etcd.Seed(MoveNames.MoveKey("shop", "bucket_1"), """{"op":"abort","requested_unix":1}""");

        // Act
        var deleted = await StoreOf(etcd).DeleteAsync("shop", "bucket_1", CancellationToken.None);

        // Assert
        deleted.IsSuccess.Should().BeTrue();
        etcd.Store.ContainsKey(MoveNames.MoveKey("shop", "bucket_1")).Should().BeFalse();
    }

    // AAA: битая заявка не роняет список — исключается из выборки
    [Fact]
    public async Task ListAsync_SkipsBrokenJson()
    {
        // Arrange
        var etcd = new FakeEtcd();
        etcd.Seed(MoveNames.MoveKey("shop", "bucket_9"), "not-json");

        // Act
        var list = await StoreOf(etcd).ListAsync("shop", CancellationToken.None);

        // Assert
        list.IsSuccess.Should().BeTrue("битая заявка — не ошибка тика, её увидит оператор в логе");
        list.Value.Should().BeEmpty();
    }
}
```

- [ ] **Step 2: Run → FAIL** (нет `MoveRequestsStore`).

- [ ] **Step 3: Реализация**

Failover-обёртка по `endpoints` — локальная копия паттерна `WorkJournal.WithFailoverAsync`; `RangeAsync(MovesPrefix(cluster))` → парсинг leaf-имени (`key.Split('/')[^1]` → bucket) + `MoveRequest.Parse`; битые — пропускать (добавить `out IReadOnlyList<string> errors` по образцу `ClusterSnapshotParser.ParseClusters` — залогируются процессом). `DeleteAsync` → `DeleteAsync(endpoint, MoveKey, prefix:false)`.

- [ ] **Step 4: Run → PASS** (`--filter MoveRequestsStoreTests`), build решение `-warnaserror`.

- [ ] **Step 5: Commit** `t01: MoveRequestsStore — заявки /pgworker/moves/`

---

### Task 4: MoveStatusStore (статус-ключ + атомарный flip)

**Files:**
- Create: `src/PgWorker.Moves/Requests/MoveStatusStore.cs`
- Test: `src/tests/PgWorker.UnitTests/Moves/MoveStatusStoreTests.cs`

**Interfaces:**
- Consumes: `IEtcdGateway` (`GetAsync/PutAsync/TxnAsync`), `TxnCompare.ValueEqual`, `TxnOp.Put/Delete` (уже есть).
- Produces: `MoveStatusStore(IEtcdGateway gateway, string[] endpoints)` (без TimeProvider — тест сам строит `MoveStatus` с нужными unix-значениями):
  - `Task<Result<MoveStatus?>> GetAsync(string cluster, string bucket, CancellationToken ct)` — null = ACTIVE.
  - `Task<Result> PutAsync(string cluster, MoveStatus status, CancellationToken ct)`.
  - `Task<Result<bool>> FlipAsync(string cluster, string bucket, string current, string next, CancellationToken ct)` — txn `[ValueEqual(routing, current)] → [Put(routing, next), Delete(status)]`; `false` = compare не сошёлся.
  - `Task<Result> DeleteAsync(string cluster, string bucket, CancellationToken ct)` (rollback-семантика «нет ключа = ACTIVE»).

- [ ] **Step 1: Failing-тесты** (каждый — отдельный `[Fact]` с AAA-комментариями):

```csharp
using PgWorker.Moves;
using PgWorker.UnitTests.Provisioning;

namespace PgWorker.UnitTests.Moves;

public class MoveStatusStoreTests
{
    // AAA: put/get round-trip статус-ключа
    [Fact]
    public async Task Get_AfterPut_ReturnsStatus()
    {
        // Arrange
        var etcd = new FakeEtcd();
        var store = new MoveStatusStore(etcd, ["http://x"]);
        var put = new MoveStatus("bucket_42", MoveStates.Syncing, "shard1", "shard2", 1, 2, "ddl");

        // Act
        await store.PutAsync("shop", put, CancellationToken.None);
        var got = await store.GetAsync("shop", "bucket_42", CancellationToken.None);

        // Assert
        got.Value!.State.Should().Be(MoveStates.Syncing);
        got.Value.Phase.Should().Be("ddl");
    }

    // AAA: нет ключа = ACTIVE (null)
    [Fact]
    public async Task Get_MissingKey_ReturnsNull()
    {
        // Arrange
        var store = new MoveStatusStore(new FakeEtcd(), ["http://x"]);

        // Act
        var got = await store.GetAsync("shop", "bucket_42", CancellationToken.None);

        // Assert
        got.Value.Should().BeNull("нет статус-ключа = бакет ACTIVE");
    }

    // AAA: flip — атомарная txn: routing → новый + delete status (скрипт etcd_flip)
    [Fact]
    public async Task FlipAsync_ReplacesRoutingAndDropsStatus()
    {
        // Arrange
        var etcd = new FakeEtcd();
        etcd.Seed(MoveNames.RoutingKey("shop", "bucket_42"), "shard1");
        var store = new MoveStatusStore(etcd, ["http://x"]);
        await store.PutAsync("shop", new MoveStatus("bucket_42", MoveStates.Frozen, "shard1", "shard2", 1, 2, "flip"), CancellationToken.None);

        // Act
        var flipped = await store.FlipAsync("shop", "bucket_42", "shard1", "shard2", CancellationToken.None);

        // Assert
        flipped.Value.Should().BeTrue("routing соответствовал cur");
        etcd.Store[MoveNames.RoutingKey("shop", "bucket_42")].Value.Should().Be("shard2");
        etcd.Store.ContainsKey(MoveNames.StatusKey("shop", "bucket_42")).Should().BeFalse("статус-ключ удалён той же txn");
    }

    // AAA: конкурентный flip (routing изменился под руками) — Succeeded=false, всё нетронуто
    [Fact]
    public async Task FlipAsync_CompetingChange_FailsCleanly()
    {
        // Arrange
        var etcd = new FakeEtcd();
        etcd.Seed(MoveNames.RoutingKey("shop", "bucket_42"), "shard9"); // конкурент уже перевёл
        var store = new MoveStatusStore(etcd, ["http://x"]);

        // Act
        var flipped = await store.FlipAsync("shop", "bucket_42", "shard1", "shard2", CancellationToken.None);

        // Assert
        flipped.Value.Should().BeFalse("compare по routing=cur обязан не сойтись");
        etcd.Store[MoveNames.RoutingKey("shop", "bucket_42")].Value.Should().Be("shard9");
    }
}
```

- [ ] **Step 2: Run → FAIL.** **Step 3: Реализация** (failover-обёртка как в Task 3; Serialize из Task 2). **Step 4: Run → PASS + build.** **Step 5: Commit** `t01: MoveStatusStore — статус-ключ и атомарный flip-txn`.

---

### Task 5: MoveSql — билдеры SQL, часть 1 (проверки/префлайт/инвентарь)

**Files:**
- Create: `src/PgWorker.Moves/Sql/MoveSql.cs`
- Test: `src/tests/PgWorker.UnitTests/Moves/MoveSqlTests.cs`

**Interfaces:**
- Produces (строковые билдеры; валидация идентификатора — `ArgumentException` на невалидное, как `DatabaseProvisioner.ValidateIdentifier`): `SchemaExists(schema)`, `PubExists(pub)`, `SubExists(sub)`, `SlotExists(slot)`, `WalLevel()`, `MaxSlots()`, `UsedSlots()`, `MaxWalSenders()`, `UsedWalSenders()`, `LostSlots()`, `MoverRoleOk()`, `SyncStandbyNames()`, `SyncStandbyCount()`, `TableNames(schema)` (string_agg `%I.%I` relkind r/p), `SchemaInventory(schema)` (relkind|relname, r/S/v/m/p, ORDER BY), `SequenceNames(schema)`, `EmptySchemaCheckSqlGen(schema)` (генератор второго SQL: сумма count всех r-таблиц), `OrphanTablesyncSlots(sub)`.

- [ ] **Step 1: Failing-тесты** — снапшот-строки (перенос из `buckets-common.sh`/`move-bucket.sh`):

```csharp
using PgWorker.Moves;

namespace PgWorker.UnitTests.Moves;

public class MoveSqlTests
{
    // AAA: префлайт wal_level — через pg_settings (Npgsql-надёжнее SHOW)
    [Fact]
    public void WalLevel_SelectsFromPgSettings()
    {
        // Act
        var sql = MoveSql.WalLevel();

        // Assert
        sql.Should().Be("SELECT setting FROM pg_settings WHERE name = 'wal_level'");
    }

    // AAA: sync-standby приёмника — имена непусты + живой sync/quorum (P8)
    [Fact]
    public void SyncStandbyCount_UsesSyncState()
    {
        // Act
        var sql = MoveSql.SyncStandbyCount();

        // Assert
        sql.Should().Be("SELECT count(*) FROM pg_stat_replication WHERE sync_state IN ('sync','quorum')");
    }

    // AAA: список таблиц для LOCK-барьера и сверки — из каталога, не хардкод (дока 11 §5 4.2)
    [Fact]
    public void TableNames_AggregatesQuoted()
    {
        // Act
        var sql = MoveSql.TableNames("bucket_42");

        // Assert
        sql.Should().Be("SELECT coalesce(string_agg(format('%I.%I', 'bucket_42', c.relname), ', '), '') " +
                        "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace " +
                        "WHERE c.relkind IN ('r','p') AND n.nspname = 'bucket_42'");
    }

    // AAA: инвентарь P5 — relkind|relname построчно, сортировка стабильна
    [Fact]
    public void SchemaInventory_KindsAndOrder()
    {
        // Act
        var sql = MoveSql.SchemaInventory("bucket_42");

        // Assert
        sql.Should().Contain("c.relkind IN ('r','S','v','m','p')");
        sql.Should().Contain("ORDER BY c.relname, 1");
    }

    // AAA: невалидное имя схемы — исключение (SQL-инъекция)
    [Theory]
    [InlineData("B;DROP TABLE x")]
    [InlineData("bucket-42")]
    public void Builders_RejectInvalidIdentifiers(string bad)
    {
        // Act
        var act = () => MoveSql.TableNames(bad);

        // Assert
        act.Should().Throw<ArgumentException>("идентификаторы обязаны проходить ^[a-z][a-z0-9_]*$");
    }
}
```

Плюс однострочные assert-тесты (каждый — `[Fact]` с AAA, точные ожидания): `SchemaExists` → `SELECT to_regnamespace('bucket_42') IS NOT NULL`; `PubExists` → `SELECT count(*) FROM pg_publication WHERE pubname = 'pub_b42'`; `SubExists`/`SlotExists` — аналогично по `pg_subscription`/`pg_replication_slots`; `MaxSlots` → `SELECT setting::int FROM pg_settings WHERE name = 'max_replication_slots'`; `UsedSlots` → `SELECT count(*) FROM pg_replication_slots`; `MaxWalSenders`/`UsedWalSenders` (pg_settings / `SELECT count(*) FROM pg_stat_replication`); `LostSlots` → `WHERE wal_status = 'lost'`; `MoverRoleOk` → `SELECT rolsuper OR rolreplication FROM pg_roles WHERE rolname = current_user`; `SyncStandbyNames` → `SELECT setting FROM pg_settings WHERE name = 'synchronous_standby_names'`; `SequenceNames`; `OrphanTablesyncSlots` → `SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'sub_b42_sync_%'`; `EmptySchemaCheckSqlGen`.

- [ ] **Step 2: Run → FAIL.** **Step 3: Реализация** — статический класс, каждый билдер валидирует идентификаторы (`MoveNames.ValidateIdentifier` → `ArgumentException`), имена pub/sub/slot — тоже валидировать (они производные от бакета, но проверять). **Step 4: Run → PASS + build.** **Step 5: Commit** `t01: MoveSql — SQL-билдеры префлайта/инвентаря (перенос buckets-common.sh)`.

---

### Task 6: MoveSql — часть 2 (freeze/pub/sub/слоты/sequences/сверки)

**Files:**
- Modify: `src/PgWorker.Moves/Sql/MoveSql.cs`
- Test: `src/tests/PgWorker.UnitTests/Moves/MoveSqlPart2Tests.cs`

**Interfaces:** Produces: `Freeze(schema, appRole, tables)` (REVOKE DML + REVOKE sequences + REVOKE CREATE + `LOCK TABLE <tables> IN ACCESS EXCLUSIVE MODE;` — БЕЗ BEGIN/COMMIT/lock_timeout: их ставит executor), `Unfreeze(schema, appRole)`, `GrantAppOnSchema(schema, appRole)`, `CreatePublication(pub, schema)`, `CreateSubscription(sub, conninfo, pub, copyData, failover)` (`WITH (copy_data = …, failover = …, synchronous_commit = remote_apply)` — conninfo экранировать одинарные кавычки удвоением), `DisableSubscription(sub)`, `SetSlotNone(sub)`, `DropSubscription(sub)`, `DropPublication(pub)`, `DropSchemaCascade(schema)`, `CurrentWalLsn()` (`SELECT pg_current_wal_lsn()::text`), `SlotCaughtUp(slot, lsn)` (`bool_and(active AND confirmed_flush_lsn >= '<lsn>'::pg_lsn)`), `SlotLag(slot)`, `SlotActive(slot)`, `TerminateSlotBackend(slot)`, `DropSlot(slot)`, `SubSyncReady(sub)` (`'<ready>'||'/'||count` по `pg_subscription_rel`), `SequenceIssued(schema, seq)`, `SequenceNext(schema, seq)`, `SetvalForward(schema, seq, issued)` (`SELECT setval('bucket_42."seq"', <issued>, true)` — схема/seq всегда в кавычках), `RowCount(schema, table)`.

- [ ] **Step 1: Failing-тесты** — ключевые:

```csharp
// AAA: заморозка P1/P5 — три REVOKE + барьер LOCK в одном батче (REVOKE не барьер!)
[Fact]
public void Freeze_ThreeRevokesAndLockBarrier()
{
    // Act
    var sql = MoveSql.Freeze("bucket_42", "app", "bucket_42.\"t1\", bucket_42.\"t2\"");

    // Assert
    sql.Should().Contain("REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA bucket_42 FROM app");
    sql.Should().Contain("REVOKE USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA bucket_42 FROM app");
    sql.Should().Contain("REVOKE CREATE ON SCHEMA bucket_42 FROM app");
    sql.Should().Contain("LOCK TABLE bucket_42.\"t1\", bucket_42.\"t2\" IN ACCESS EXCLUSIVE MODE;");
}

// AAA: разморозка — симметричные GRANT, без CREATE (его app-роли не выдавалось)
[Fact]
public void Unfreeze_SymmetricGrants()
{
    // Act
    var sql = MoveSql.Unfreeze("bucket_42", "app");

    // Assert
    sql.Should().Contain("GRANT INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA bucket_42 TO app");
    sql.Should().Contain("GRANT USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA bucket_42 TO app");
    sql.Should().NotContain("GRANT CREATE");
}

// AAA: подписка — failover-флаг конфигурируем (PG17+), remote_apply всегда (P3/P8)
[Theory]
[InlineData(true)]
[InlineData(false)]
public void CreateSubscription_Flags(bool failover)
{
    // Act
    var sql = MoveSql.CreateSubscription("sub_b42", "host=h1,h2 port=1,2 dbname=shop user=bucket_mover password=p'x",
        "pub_b42", copyData: true, failover: failover);

    // Assert
    sql.Should().StartWith("CREATE SUBSCRIPTION sub_b42 CONNECTION '");
    sql.Should().Contain("password=p''x'"); // кавычка conninfo экранирована
    sql.Should().Contain($"WITH (copy_data = true, failover = {failover.ToString().ToLowerInvariant()}, synchronous_commit = remote_apply)");
}

// AAA: sequence-issued — is_called учитывается на стороне SQL (баш-нюанс стенда, P6)
[Fact]
public void SequenceIssued_CaseWhenOnSqlSide()
{
    // Act
    var sql = MoveSql.SequenceIssued("bucket_42", "seq1");

    // Assert
    sql.Should().Be("SELECT CASE WHEN is_called THEN last_value ELSE last_value - 1 END FROM bucket_42.\"seq1\"");
}

// AAA: слот догнал — активен и подтвердил LSN
[Fact]
public void SlotCaughtUp_ActiveAndConfirmed()
{
    // Act
    var sql = MoveSql.SlotCaughtUp("sub_b42", "0/A000123");

    // Assert
    sql.Should().Contain("confirmed_flush_lsn >= '0/A000123'::pg_lsn");
    sql.Should().Contain("bool_and(active");
}
```

Плюс точечные тесты `CurrentWalLsn`, `SetvalForward` (`SELECT setval('bucket_42."seq1"', 100, true)`), `SubSyncReady` (`srsubstate='r'`), `DropSchemaCascade`, `RowCount` (`SELECT count(*) FROM bucket_42."items"`), `TerminateSlotBackend`/`DropSlot`/`SlotActive`/`SlotLag`/`DisableSubscription`/`SetSlotNone`.

- [ ] **Step 2: Run → FAIL.** **Step 3: Реализация** (конкатенация строк; conninfo-экранирование `Replace("'", "''")`). **Step 4: Run → PASS + build.** **Step 5: Commit** `t01: MoveSql — freeze/pubsub/слоты/sequences/сверки`.

---

### Task 7: IMoveSqlExecutor + Npgsql-реализация

**Files:**
- Create: `src/PgWorker.Moves/Sql/IMoveSqlExecutor.cs`
- Create: `src/PgWorker.Moves/Sql/NpgsqlMoveSqlExecutor.cs`
- Create: `src/PgWorker.Moves/Properties/AssemblyInfo.cs` (`[assembly: InternalsVisibleTo("PgWorker.UnitTests")]`)
- Test: `src/tests/PgWorker.UnitTests/Moves/MoveSqlExecutorTests.cs`

**Interfaces:**
- Produces:
```csharp
public interface IMoveSqlExecutor
{
    Task<Result<object?>> ScalarAsync(string dsn, string sql, CancellationToken ct);
    Task<Result<IReadOnlyList<string>>> ListAsync(string dsn, string sql, CancellationToken ct); // пустые → []
    Task<Result> ExecuteAsync(string dsn, string sql, CancellationToken ct);
    // Freeze: ОДНА транзакция: SET LOCAL lock_timeout → батч; longrun-транзакция без Polly-обёртки (LOCK сам ждёт lock_timeout)
    Task<Result> ExecuteTransactionalAsync(string dsn, string sql, int lockTimeoutSec, CancellationToken ct);
}
```
`NpgsqlMoveSqlExecutor` — Polly `RetryPolicies.SqlRetry(3, 1s)` на Scalar/List/Execute (образец `DatabaseProvisioner.ExecuteAsync`); `ExecuteTransactionalAsync`: `await using var tx = await conn.BeginTransactionAsync` → cmd `SET LOCAL lock_timeout = '<n>s'` → cmd тела → `tx.CommitAsync`; ошибки → `internal static Result WrapError(string dsn, Exception e)` (сообщение `SQL не выполнен [<ред-DSN>]: …`; приватный `Redact` — regex `password=(?:'[^']*'|[^; ]*)`, IgnoreCase, дубль паттерна `DatabaseProvisioner`).

- [ ] **Step 1: Failing-тест** — редакция пароля (Npgsql-путь покрыт e2e):

```csharp
// AAA: пароль не утекает в текст ошибки (P12/P17)
[Fact]
public void WrapError_MasksPassword()
{
    // Arrange
    var dsn = "Host=h;Port=1;Database=d;Username=postgres;Password=secret";

    // Act
    var failed = NpgsqlMoveSqlExecutor.WrapError(dsn, new ApplicationException("boom"));

    // Assert
    failed.Error!.Message.Should().NotContain("secret");
    failed.Error!.Message.Should().Contain("password=***");
}
```

- [ ] **Step 2: Run → FAIL.** **Step 3: Реализация** (интерфейс + Npgsql + AssemblyInfo). **Step 4: Run → PASS + build.** **Step 5: Commit** `t01: IMoveSqlExecutor — scalar/list/батч/транзакция с lock_timeout`.

---

### Task 8: Docker exec (Engine + драйверы)

**Files:**
- Modify: `src/PgWorker.Docker/Engine/IDockerEngine.cs` (+`ExecAsync`)
- Modify: `src/PgWorker.Docker/Engine/DockerEngine.cs`
- Modify: `src/PgWorker.Docker/Drivers/ClusterDriver.cs` (интерфейс + Plain/Swarm)
- Modify: `src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs` (FakeDriver + exec)
- Test: `src/tests/PgWorker.UnitTests/Docker/DockerEngineExecTests.cs`

**Interfaces:**
- Consumes: `DockerEngine(HttpClient)`, `FakeDriver`.
- Produces: `IDockerEngine.ExecAsync(string containerId, IReadOnlyList<string> cmd, CancellationToken ct): Task<Result<string>>` (stdout, exit≠0 → Failed); `IClusterDriver.ExecNodeAsync(string cluster, string shard, string node, IReadOnlyList<string> cmd, CancellationToken ct): Task<Result<string>>`; FakeDriver: `public Func<string, IReadOnlyList<string>, Result<string>>? ExecResult { get; set; }` + запись в `public readonly List<(string Node, string Cmd)> Executed`.

- [ ] **Step 1: Failing-тесты**

`DockerEngineExecTests`: handler отвечает `POST /v1.44/containers/{id}/exec` → 201 `{Id:"e1"}`, `POST /v1.44/exec/e1/start` → 200 raw-стрим `application/vnd.docker.raw-stream` с фреймом stdout (8-байт заголовок: `[stream-type,0,0,0, size BE32]` — собрать байтами: `0x01,0,0,0, 0,0,0,5` + `"hello"`), затем `GET /v1.44/exec/e1/json` → `{ExitCode:0}`:

```csharp
// AAA: exec возвращает demultiplexed stdout
[Fact] ExecAsync_ReturnsStdout — "hello".

// AAA: ненулевой exit — Result.Failed со stderr в сообщении
[Fact] ExecAsync_NonZeroExit_Fails — ExitCode:1 + stderr-фрейм → IsSuccess=false, Message содержит stderr.

// AAA: имя контейнера драйвером — pgw-<C>-<X>-<n>, поиск по хостам plain
[Fact] PlainDriver_ExecNode_ResolvesContainerByPattern (через handler, контейнер list на h2).
```

- [ ] **Step 2: Run → FAIL** (метода нет — не компилируется).

- [ ] **Step 3: Реализация**

`DockerEngine.ExecAsync`: POST JSON `{"AttachStdout":true,"AttachStderr":true,"Cmd":[...]}` → id; POST `/exec/{id}/start` `{"Detach":false,"Tty":false}` — читать весь ответ как байты (raw-stream мультиплексирован: парсить фреймы 8-байтных заголовков, тип 1=stdout, 2=stderr); `GET /exec/{id}/json` → `ExitCode != 0` → `Result.Failed(new ApplicationException($"exec {string.Join(' ', cmd)} → exit {code}: {stderr}"))`. Plain-`ExecNodeAsync(cluster, shard, node, cmd, ct)`: имя `pgw-<C>-<X>-<n>`; перебор хостов, на первом, где `ListContainersAsync(name)` нашёл running-контейнер → `ExecAsync(id, …)` (аналог цикла `StopNodeAsync`); нигде нет → Failed «контейнер не найден». Swarm: `ListTasksAsync(serviceName)` → running task → расширить record `DockerTask` полем `ContainerId` (из `Tasks[].Status.ContainerStatus.ContainerID` — данные уже в ответе API) → `ExecAsync(task.ContainerId, …)`.

- [ ] **Step 4: Run → PASS + build + существующие DockerEngineTests/ClusterDriverTests зелёные** (`dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~Docker"`).

- [ ] **Step 5: Commit** `t01: docker exec — ExecAsync в Engine + ExecNodeAsync в драйверах (pg_dump-транспорт)`.

---

### Task 9: ShardEndpoints (адресация мастеров + DSN-билдеры, включая mover-Npgsql-DSN)

**Files:**
- Create: `src/PgWorker.Moves/Endpoints/ShardEndpoints.cs`
- Modify: `src/PgWorker.Provisioning/Processes/BucketEvacuator.cs` (переключить на общий сервис)
- Test: `src/tests/PgWorker.UnitTests/Moves/ShardEndpointsTests.cs`
- Test: существующий `src/tests/PgWorker.UnitTests/Provisioning/BucketEvacuatorTests.cs` должен остаться зелёным.

**Interfaces:**
- Consumes: `IEtcdGateway`, `ShardProbe` (Provisioning.Probes), `Portalloc.Parse` (Core), `InstallSecrets`, `DatabaseProvisioner.BuildAdminDsn`.
- Produces: `ShardEndpoints(IEtcdGateway etcd, string[] endpoints, ShardProbe probe)`:
  - `Task<Result<IReadOnlyDictionary<string, NodeAddress>>> ReadPortAllocAsync(string cluster, CancellationToken ct)`
  - `Task<Result<NodeAddress?>> ResolveMasterAsync(ShardSpec shard, IReadOnlyDictionary<string, NodeAddress> addresses, CancellationToken ct)` — перенос тела `BucketEvacuator.ResolveMasterAsync` (master-ключ → поиск среди нод шарда → Patroni `/cluster` fallback).
  - `static string AdminDsn(NodeAddress master, string dbname, InstallSecrets secrets)` → `DatabaseProvisioner.BuildAdminDsn(master.Host, master.Ports.Pg, dbname, secrets)`.
  - `static string MoverConninfo(string shardDsn, InstallSecrets secrets)` — libpq-строка для `CREATE SUBSCRIPTION … CONNECTION`: заменить `user=<x>` → `user=bucket_mover`, добавить ` password=<MoverPassword>`; если `user=` нет — добавить (regex `(^| )user=[^ ]*`).
  - `static string MoverNpgsqlDsn(string shardDsn, InstallSecrets secrets)` — конвертация той же libpq-строки в Npgsql-DSN для SQL-проб роли mover (замечание ревью №2): сплит по пробелам → пары key=value → маппинг `host→Host, port→Port, dbname→Database, user→Username` (user заменить на `bucket_mover`), join `";"`, добавить `Password=<MoverPassword>`. Пример: `host=n1,n2 port=15432,15433 dbname=shop user=bucket_admin` → `Host=n1,n2;Port=15432,15433;Database=shop;Username=bucket_mover;Password=moverpw`.

`BucketEvacuator`: удалить private `ResolveMasterAsync`/`ReadPortAllocAsync`, принимать `ShardEndpoints` в конструктор (DI-место в Program.cs правится в Task 17).

- [ ] **Step 1: Failing-тесты**

```csharp
// AAA: mover-conninfo — user подменён, пароль добавлен, host-часть сохранена (P2/P17)
[Fact]
public void MoverConninfo_SwapsUserAddsPassword()
{
    // Arrange
    var dsnKey = "host=n1,n2,n3 port=15432,15433,15434 dbname=shop user=bucket_admin";
    var secrets = new InstallSecrets("su", "sb", "app", "adm", "moverpw");

    // Act
    var conninfo = ShardEndpoints.MoverConninfo(dsnKey, secrets);

    // Assert
    conninfo.Should().Be("host=n1,n2,n3 port=15432,15433,15434 dbname=shop user=bucket_mover password=moverpw");
}

// AAA: dsn без user= — user добавляется (не теряем вход)
[Fact]
public void MoverConninfo_AppendsUserIfMissing()
{
    // Act
    var conninfo = ShardEndpoints.MoverConninfo("host=n1 dbname=shop", new InstallSecrets("s","s","s","s","moverpw"));

    // Assert
    conninfo.Should().Be("host=n1 dbname=shop user=bucket_mover password=moverpw");
}

// AAA: mover-Npgsql-DSN — libpq→Npgsql конвертация для SQL-проб роли (spec §6.1 M0)
[Fact]
public void MoverNpgsqlDsn_ConvertsLibpqToNpgsql()
{
    // Arrange
    var dsnKey = "host=n1,n2,n3 port=15432,15433,15434 dbname=shop user=bucket_admin";
    var secrets = new InstallSecrets("su", "sb", "app", "adm", "moverpw");

    // Act
    var dsn = ShardEndpoints.MoverNpgsqlDsn(dsnKey, secrets);

    // Assert
    dsn.Should().Be("Host=n1,n2,n3;Port=15432,15433,15434;Database=shop;Username=bucket_mover;Password=moverpw");
}

// AAA: Npgsql-DSN без user= — Username добавляется, пароль всегда
[Fact]
public void MoverNpgsqlDsn_MissingUser_AddsUsername()
{
    // Act
    var dsn = ShardEndpoints.MoverNpgsqlDsn("host=n1 port=1 dbname=d", new InstallSecrets("s","s","s","s","pw"));

    // Assert
    dsn.Should().Be("Host=n1;Port=1;Database=d;Username=bucket_mover;Password=pw");
}
```

Плюс тест `ResolveMasterAsync` на FakeEtcd-сиде (portalloc + master-ключ → адрес; master-ключа нет — Patroni-фолбэк через `ShardProbe` не мокается (sealed) — эта ветка покрыта e2e; unit — только master-ключ-ветка).

- [ ] **Step 2: Run → FAIL.** **Step 3: Реализация + рефакторинг эвакуатора.** **Step 4: Run → PASS, `--filter BucketEvacuator` зелёный, build.** **Step 5: Commit** `t01: ShardEndpoints — адресация мастеров, admin-DSN/mover-conninfo/mover-Npgsql-DSN; эвакуатор на общий сервис`.

---

### Task 10: MoveDdl (pg_dump через exec + применение + гранты + инвентарь)

**Files:**
- Create: `src/PgWorker.Moves/Ddl/MoveDdl.cs`
- Test: `src/tests/PgWorker.UnitTests/Moves/MoveDdlTests.cs`

**Interfaces:**
- Consumes: `IClusterDriver.ExecNodeAsync`, `IMoveSqlExecutor`, `MoveSql`.
- Produces: `MoveDdl(IClusterDriver driver, IMoveSqlExecutor sql)`:
  - `Task<Result<string>> DumpAsync(string cluster, string shard, string node, string dbname, string bucket, CancellationToken ct)` — exec `["su","postgres","-c","pg_dump --schema-only --no-owner --no-privileges --schema=<bucket> <dbname>"]` (Spilo: утилиты под postgres; exec от root).
  - `Task<Result> ApplyAsync(string dsn, string ddl, CancellationToken ct)` — `sql.ExecuteAsync(dsn, ddl)`.
  - `Task<Result> GrantAppOnSchemaAsync(string dsn, string bucket, CancellationToken ct)` — `MoveSql.GrantAppOnSchema` (USAGE + DML + sequences для app).
  - `Task<Result<bool>> InventoryMatchesAsync(string srcDsn, string dstDsn, string bucket, CancellationToken ct)` — `MoveSql.SchemaInventory` с обоих, построчное сравнение списков.

- [ ] **Step 1: Failing-тесты** (FakeDriver.ExecResult + `StubSql : IMoveSqlExecutor` — записывающий мок, локальный в тест-файле; в Task 11 превратится в общий `FakeMoveSql`):

```csharp
// AAA: команда pg_dump — флаги как в скрипте шага 1 (schema-only, no-owner, no-privileges)
[Fact]
public async Task DumpAsync_ExecsPgDumpInNodeContainer()
{
    // Arrange
    var driver = new FakeDriver { ExecResult = (_, cmd) => Result<string>.Success("-- ddl") };
    var ddl = new MoveDdl(driver, new StubSql());

    // Act
    var dump = await ddl.DumpAsync("shop", "shard1", "shard1a", "shop", "bucket_42", CancellationToken.None);

    // Assert
    dump.Value.Should().Be("-- ddl");
    driver.Executed.Should().ContainSingle().Which.Cmd.Should().BeEquivalentTo(
        ["su", "postgres", "-c", "pg_dump --schema-only --no-owner --no-privileges --schema=bucket_42 shop"]);
}
```

- [ ] **Step 2: Run → FAIL.** **Step 3: Реализация.** **Step 4: Run → PASS + build.** **Step 5: Commit** `t01: MoveDdl — pg_dump через docker exec, применение, гранты, сверка инвентаря P5`.

---

### Task 11: MovesRuntimeOptions + MoveProcess M0 (каркас, выбор заявки, валидация/prefлайт)

**Files:**
- Create: `src/PgWorker.Moves/Options.cs`
- Create: `src/PgWorker.Moves/Process/MoveProcess.cs`
- Create: `src/tests/PgWorker.UnitTests/Moves/FakesMove.cs`
- Test: `src/tests/PgWorker.UnitTests/Moves/MoveProcessPreflightTests.cs`

**Interfaces:**
- Produces:
```csharp
namespace PgWorker.Moves;
public sealed record MovesRuntimeOptions(
    int PollIntervalSec = 2, int FreezeWaitSec = 5, int FreezeLockTimeoutSec = 5,
    int FreezeLockTries = 3, int AbortMinAgeSec = 120, bool FailoverSlots = true,
    int CutoverTimeoutSec = 90, int ConnFailBudgetSec = 120);
```
`MoveProcess` конструктор (DI-готовый; `TimeProvider` — источник `updated_unix`/`started_unix` и проверки AbortMinAgeSec):
```csharp
public sealed class MoveProcess(
    IEtcdGateway etcd, string[] etcdEndpoints,
    IMoveSqlExecutor sql, MoveDdl ddl, IClusterDriver driver, ShardEndpoints shards,
    ClaimStore claims, WorkJournal journal, InstallSecrets secrets,
    MovesRuntimeOptions options, TimeProvider clock,
    Microsoft.Extensions.Logging.ILogger<MoveProcess>? logger = null,
    Func<CancellationToken, Task<Result>>? snapshot = null)
{
    public async Task<Result<ProcessOutcome>> TickAsync(ClusterSnapshot snap, CancellationToken ct);
}
```
`TickAsync`: `claims.IsMine` guard → `requests.OldestAsync` → нет заявок → `Done`; по `Op` — `RunMoveAsync/RunRollbackAsync/RunFinalizeAsync/RunAbortAsync` (последние три — заглушки `throw new NotSupportedException`, реализуют Tasks 15–16; тесты только move-ветки).

`FakesMove.cs` — `FakeMoveSql : IMoveSqlExecutor` со словарём ответов:
```csharp
internal sealed class FakeMoveSql : IMoveSqlExecutor
{
    public readonly List<(string Dsn, string Sql)> Calls = [];
    public Func<string, object?> ScalarResolver { get; set; } = _ => null; // по тексту SQL
    public Func<string, Result>? ExecuteResult { get; set; }
    public Func<string, IReadOnlyList<string>> ListResolver { get; set; } = _ => [];
}
```
Resolver-подстроки: `"pg_settings WHERE name = 'wal_level'" → "logical"`, `max_replication_slots` (setting) → "10", `FROM pg_replication_slots` count → "0", `pg_stat_replication` count → "0"/"1", `to_regnamespace` → true/false, `pg_publication`/`pg_subscription` count → 0/1 и т.д. — тесты конфигурируют. Различение DSN: resolver при необходимости смотрит `Calls`-контекст — для проб mover-роли резолвер ключуется по DSN (mover-DSN отличается от admin-DSN).

M0-логика (`RunMoveAsync` — первая часть): валидации по порядку (spec §6.1 M0), каждая — либо `PermanentReject(reason)` (del заявки + `journal.WritePhaseAsync(cluster,"move","rejected",instance,reason)` + `Result<ProcessOutcome>.Failed`), либо `TransientFail(reason)` (журнал last_error, заявка жива, `Failed`), либо продолжение. Проверки: config.State == Active; `MoveNames.ValidateIdentifier`; To задан/валиден/зарегистрирован (dsn-ключ в snap.Shards); To != Owner; **схема бакета есть на источнике** (`SchemaExists` по admin-DSN владельца — нет → Permanent «схемы нет на владельце?!», ревью №5); статус-ключ (нет → новый; SYNCING/FROZEN + target==To → resume c наследованием started_unix; SYNCING/FROZEN + target!=To → Permanent; ABORTING → Permanent); SQL-префлайт источника/приёмника по admin-DSN (wal_level, слоты, walsenders, lost-warning — только журнал) — недоступность шарда → Transient, факт-несоответствие (wal_level≠logical, слоты кончились) → Permanent; **пробы mover-роли через `ShardEndpoints.MoverNpgsqlDsn(dsnKey источника)`** (ревью №2): `ScalarAsync(moverDsn, "SELECT 1")` (fail → Transient), `ScalarAsync(moverDsn, MoveSql.MoverRoleOk())` (false → Permanent «mover-роль без REPLICATION»); sync-standby приёмника (Names непусто + Count ≥ 1; недоступность → Transient, факт-отсутствие → Permanent); остатки pub_rb/sub_rb → Permanent. Схема на приёмнике без подписки: `resume=false` → Permanent; `resume=true` + непустая (`EmptySchemaCheckSqlGen` → второй scalar ≠ 0) → Permanent. Затем: `status.PutAsync(SYNCING/ddl, updated_unix=clock)` + снапшот `move-<bucket>-start` (fail → Transient + phase `waiting-snapshot`, повтор тика). M0-конец = `Result<ProcessOutcome>.Success(ProcessOutcome.InProgress)`.

- [ ] **Step 1: Failing-тесты** (каждый кейс — отдельный `[Fact]` с AAA-комментариями; формат тела — как в `MoveRequestsStoreTests`, полный разбор входа/действия/ожидания):

1. `NoRequests_Done` — пустой префикс → `ProcessOutcome.Done`.
2. `ClaimNotMine_RefusesMutation` — `claims` не держит → Failed «клэйм не наш».
3. `Move_BasicPreflightOk_PutsSyncingAndTakesSnapshot` — резолверы зелёные (включая mover-DSN пробы) → после тика: статус-ключ `SYNCING/ddl`, снапшот-делегат вызван, `InProgress`.
4. `Move_WrongTargetPermanent_RejectsAndDeletesRequest` — target=owner → заявка удалена, журнал `rejected`.
5. `Move_AbortingStatus_Rejects`.
6. `Move_OtherTargetInProgress_Rejects`.
7. `Move_ResumeSameTarget_ContinuesAndKeepsStartedUnix`.
8. `Move_WalLevelNotLogical_RejectsPermanent`.
9. `Move_NoSyncStandby_RejectsPermanent`.
10. `Move_ShardUnreachable_TransientKeepsRequest` (Scalar по admin-DSN падает → Failed, заявка на месте).
11. `Move_SnapshotFails_WaitsWithoutPhases` (делегат снапшота Failed → статус SYNCING остался, повтор тика снова пробует).
12. `Move_NonEmptySchemaWithoutResume_Rejects`.
13. `Move_SchemaMissingOnSource_RejectsPermanent` (ревью №5: `to_regnamespace` источника → false → заявка удалена, журнал rejected).
14. `Move_MoverRoleProbeUsesMoverDsn_RejectsWithoutReplication` (ревью №2: SELECT 1 по mover-DSN ок, MoverRoleOk → false → Permanent; и `Calls` фиксирует, что обе пробы шли по DSN с `Username=bucket_mover`, а не по admin-DSN).

Сид-хелперы: `Snap()` — `ClusterSnapshot` c `ClusterConfig("shop", 6, "shop", null, Active)`, `ShardSpec("shard1",2,dsn,master,nodes RUNNING)`/`shard2`, `BucketRoute(42,"shard1",null)`; `etcd.Seed(RoutingKey,"shard1")`; заявка `etcd.Seed(MoveKey, json)`; ClaimStore реальный поверх FakeEtcd (`TryClaim` в arrange теста).

- [ ] **Step 2: Run → FAIL.** **Step 3: Реализация Options + MoveProcess.M0 + FakesMove.** **Step 4: Run → PASS + build.** **Step 5: Commit** `t01: MoveProcess M0 — заявки, префлайт (permanent/transient), пробы mover-роли, SYNCING+снапшот`.

---

### Task 12: CutoverSequence (с классификацией отказов transient/permanent)

**Files:**
- Create: `src/PgWorker.Moves/Process/CutoverSequence.cs`
- Test: `src/tests/PgWorker.UnitTests/Moves/CutoverSequenceTests.cs`

**Interfaces:**
- Consumes: `IMoveSqlExecutor`, `MoveStatusStore`, `ShardEndpoints`, `MoveSql`, `MovesRuntimeOptions`.
- Produces:
```csharp
// Перманентный отказ cutover (ревью №1, spec §6.2 п.6/п.7): MoveProcess по нему
// удаляет заявку и пишет подсказку в журнал; заморозка при flip-conflict ОСТАВЛЕНА.
public sealed class CutoverPermanentException(string reason) : Exception(reason);

public sealed record CutoverContext(string Cluster, string Bucket, string Cur, string New,
    string Slot, string FailState /* "SYNCING" для move; rollback — DropStatusOnFail */,
    bool DropStatusOnFail = false);

public sealed class CutoverSequence(IMoveSqlExecutor sql, MoveStatusStore status)
{
    // true = flip прошёл. Failed-исходы:
    //  transient (обычное исключение; заморозка снята, статус=FailState/<фаза>):
    //    freeze-failed / lsn-failed / catchup-timeout / sequences-failed
    //  permanent (CutoverPermanentException):
    //    verify-failed (дефектная копия P8; разморозка сделана, статус FailState/verify-failed)
    //    flip-conflict (заморозка ОСТАВЛЕНА — P1-призраки до разбора)
    public async Task<Result<bool>> RunAsync(ShardEndpoints shards, ClusterSnapshot snap,
        CutoverContext c, MovesRuntimeOptions o, CancellationToken ct,
        Func<CancellationToken, Task<Result>>? snapshot = null);
}
```
Шаги (spec §6.2; «отказ на подшагах 1–6 до flip → разморозка + возврат FailState»): freeze (до `FreezeLockTries`: TableNames → пустой список → без LOCK; `ExecuteTransactionalAsync(Freeze(...), FreezeLockTimeoutSec)`; fail → пауза `PollIntervalSec`, повтор; исчерпаны → статус `FailState/freeze-failed` + transient-Failed) → `PutAsync(Frozen/frozen)` → delay `FreezeWaitSec` → `CurrentWalLsn` (fail → **Unfreeze** + `FailState/lsn-failed` + transient) → цикл ожидания `SlotCaughtUp` до `CutoverTimeoutSec` (пауза `PollIntervalSec`; таймаут → Unfreeze + `FailState/catchup-timeout` + transient) → sequences (`SequenceNames` → по каждой `SequenceIssued(src)`/`SequenceNext(dst)`; next ≤ issued → `SetvalForward(dst, issued)`; сбой чтения/отсутствие seq на dst → Unfreeze + `FailState/sequences-failed` + transient) → `PutAsync(Frozen/verify)` → row counts (`TableNames` → по каждой `RowCount` src/dst; расхождение → Unfreeze + `PutAsync(FailState/verify-failed)` + **`Result.Failed(new CutoverPermanentException("сверка строк не сошлась — копия дефектна (P8): abort + повторный move"))`**) → `PutAsync(Frozen/flip)` → `status.FlipAsync(Cur→New)`: false → **`Result.Failed(new CutoverPermanentException("flip-conflict: routing изменился под руками — заморозка оставлена, разбор вручную"))`** (заморозка НЕ снимается) → true → best-effort snapshot (fail → журнал процесса; снапшот-колбэк обёрнут процессом). При `DropStatusOnFail=true` (rollback) все fail-пути вместо `PutAsync(FailState/…)` делают `DeleteAsync` статус-ключа (нет ключа = ACTIVE).

- [ ] **Step 1: Failing-тесты** (FakeMoveSql с full-resolver + FakeEtcd-статусы):

1. `HappyPath_FreezesFrozenFlipsUnfreezesNothing` — транзакционный вызов Freeze на cur-DSN, статус FROZEN/frozen, LSN, caught-up, sequences (issued=100, next=101 → setval НЕ вызван), counts равны → flip txn succeeded → true.
2. `LockTimeoutRetries_ThenGivesUp_Transient` — `ExecuteTransactionalAsync` fail ×3 → статус FailState/freeze-failed, `Result.Failed`, ошибка НЕ `CutoverPermanentException`.
3. `SlotNeverCatchesUp_Unfreezes_Transient` — caught-up всегда false (`CutoverTimeoutSec=1, PollIntervalSec=1`) → Unfreeze вызван, статус FailState/catchup-timeout, не permanent.
4. `LsnReadFails_Unfreezes_Transient` — CurrentWalLsn resolver fail → Unfreeze + FailState/lsn-failed.
5. `SequenceMissingOnDst_Unfreezes_Transient` — dst SequenceNext fail → Unfreeze + sequences-failed, не permanent.
6. `RowCountsMismatch_Unfreezes_Permanent` (ревью №1) — расхождение → Unfreeze + статус `FailState/verify-failed`, ошибка — именно `CutoverPermanentException`, сообщение содержит «abort».
7. `FlipCompareFails_Permanent_FreezeLeft` (ревью №1) — routing в etcd уже другой → `CutoverPermanentException`, Unfreeze НЕ вызывался (заморозка оставлена).
8. `SequenceBackward_SetvalForward` — issued=100, next=5 → Setval на dst со значением 100.
9. `DropStatusOnFail_DeletesStatusInsteadOfPut` (для rollback) — sequences-fail при `DropStatusOnFail=true` → статус-ключ удалён.

- [ ] **Step 2: Run → FAIL.** **Step 3: Реализация.** **Step 4: Run → PASS + build.** **Step 5: Commit** `t01: CutoverSequence — заморозка P1, лаг 0, sequences P6, сверка P8, flip; классификация transient/permanent`.

---

### Task 13: MoveProcess M1–M3 (DDL, pub/sub, copy-wait с обновлением updated_unix и логом)

**Files:**
- Modify: `src/PgWorker.Moves/Process/MoveProcess.cs`
- Test: `src/tests/PgWorker.UnitTests/Moves/MoveProcessPhasesTests.cs`

**Interfaces:** Consumes: `MoveDdl`, `MoveSql`, `IMoveSqlExecutor`, `ShardEndpoints`, `ILogger<MoveProcess>` (лог готовности, ревью №7), `TimeProvider` (updated_unix, ревью №4). Produces: продолжение `RunMoveAsync` после M0:

- M1: схема на dst есть? (`SchemaExists`) → skip; иначе `ddl.DumpAsync(cluster, Cur, master-node-of-Cur, dbname, bucket)` → `ddl.ApplyAsync(dstDsn)` → `ddl.GrantAppOnSchemaAsync` → `ddl.InventoryMatchesAsync` false → Permanent `inventory-mismatch` (мораторий P5 нарушен).
- M2: `PubExists(cur)` нет → `ExecuteAsync(curDsn, CreatePublication)`; `SubExists(dst)` нет → `ExecuteAsync(dstDsn, CreateSubscription(conninfo=MoverConninfo(dsnKey Cur), copyData:true, failover:options.FailoverSlots))`; `PutAsync(SYNCING/pubsub)`.
- M3: **каждый тик M3 перезаписывает статус-ключ** `PutAsync(SYNCING/copy-wait, updated_unix=clock.GetUtcNow())` — фундамент защиты abort (AbortMinAgeSec по updated_unix, Д12; ревью №4); один poll: `SubSyncReady(dstDsn)` = `"N/N"` → фаза `cutover-wait` + `InProgress` (M4 — Task 14); иначе `InProgress`. Лог (ревью №7): при изменении готовности (`"2/5" → "3/5"`) — `logger.LogInformation("move {bucket}: таблицы готовы {ready}, лаг слота {lag} байт", …)` c `SlotLag(srcDsn)` (образец — move-bucket.sh шаг 3; лог-высказывание — не контракт, unit-тест на него не пишем, фиксируется ручной/e2e-трассировкой). Недоступность приёмника (`ScalarAsync` fail) → Transient-fail тика с last_error «приёмник недоступен» (тики повторяются; ConnFailBudgetSec — контекст сообщения).

- [ ] **Step 1: Failing-тесты** (по образцу Task 11):

1. `M1_SchemaMissing_DumpsAppliesGrants` — FakeDriver.ExecResult → DDL; после тика: Calls содержат Apply (dsn=dst) и Grant; статус продвинулся (M2-резолверы готовы) либо (M2-fail) — Apply зафиксирован.
2. `M1_InventoryMismatch_RejectsPermanent` — InventoryMatches false → заявка удалена, журнал `inventory-mismatch`.
3. `M2_PubMissing_Created_SubMissing_CreatedWithFailoverOption` — в Calls есть `CREATE PUBLICATION pub_bucket_42 FOR TABLES IN SCHEMA bucket_42` и `CREATE SUBSCRIPTION sub_bucket_42 CONNECTION '…user=bucket_mover password=…' PUBLICATION pub_bucket_42 WITH (copy_data = true, failover = <конфиг>, synchronous_commit = remote_apply)` (параметризовать `FailoverSlots`).
4. `M2_Resume_SubExists_SkipsCreate` — не вызывается CREATE SUBSCRIPTION.
5. `M3_NotReady_InProgress` — SubSyncReady → `"1/3"` → InProgress, статус `SYNCING/copy-wait`.
6. `M3_Ready_SetsCutoverWaitPhase` — `"3/3"` → статус `SYNCING/cutover-wait`.
7. `M3_EachTickRewritesStatus_UpdatedUnixAdvances` (ревью №4) — два последовательных тика copy-wait (`"1/3"`): `updated_unix` второго put строго больше первого (между тиками `await Task.Delay(20)`; `TimeProvider.System` монотонен) — статус-ключ переписан, значения полей state/phase неизменны.

- [ ] **Step 2: Run → FAIL.** **Step 3: Реализация.** **Step 4: Run → PASS + build.** **Step 5: Commit** `t01: MoveProcess M1–M3 — DDL, pub/sub (P3/P8), copy-wait с обновлением updated_unix и логом лага`.

---

### Task 14: MoveProcess M4–M6 (cutover с классификацией отказов, post-flip, done)

**Files:**
- Modify: `src/PgWorker.Moves/Process/MoveProcess.cs`
- Test: `src/tests/PgWorker.UnitTests/Moves/MoveProcessCutoverTests.cs`

**Interfaces:** Consumes: `CutoverSequence` (включая `CutoverPermanentException`, ревью №1). Produces: M4: статус `SYNCING/cutover-wait` (или `FROZEN` от прошлого сбоя — resume: повтор cutover безопасен, spec §6.2) → `cutover.RunAsync(...)` с `FailState="SYNCING"`, `Slot=Sub(bucket)`:
- `true` → M5;
- `Failed` **с `CutoverPermanentException`** → `PermanentReject` с подсказкой из исключения: verify-failed → «abort + повторный move» (статус-ключ остаётся `SYNCING/verify-failed` — переезд живёт до abort), flip-conflict → «routing изменился — заморозка оставлена, разбор вручную»;
- `Failed` с обычной ошибкой (freeze-failed/lsn-failed/catchup-timeout/sequences-failed) → Transient (статус уже записан cutover'ом; заявка жива; ретраи тиками).

M5: `DropSubscription(dst)` fail → журнал `last_error` («срезается finalize») и ВСЁ РАВНО продолжение; `!SkipReverse` и drop-ok → `CreatePublication(PubRb, dst)` + `CreateSubscription(SubRb, srcDsn, PubRb, copyData:false, failover)` (conninfo = MoverConninfo(dsnKey dst)). M6: снапшот `flip-<bucket>-<to>` best-effort (fail → журнал) → `requests.DeleteAsync` → журнал `done` → `ProcessOutcome.Done`.

- [ ] **Step 1: Failing-тесты**:

1. `M4_CutoverFlipSuccess_DropsSubCreatesReverse_Done` — полный happy-resolver → после тика: routing=shard2 (FakeEtcd), статус-ключ удалён, заявка удалена, Calls содержат `DROP SUBSCRIPTION sub_bucket_42`, `CREATE PUBLICATION pub_bucket_42_rb`, `CREATE SUBSCRIPTION sub_bucket_42_rb` с `copy_data = false`; outcome Done.
2. `M4_CutoverTransientFail_RequestSurvives` (ревью №1) — catchup-timeout-резолвер → заявка НА МЕСТЕ, статус `SYNCING/catchup-timeout`, outcome Failed (ретраи тиками).
3. `M4_VerifyFailed_RequestRejectedWithHint` (ревью №1) — counts mismatch → заявка УДАЛЕНА, журнал `rejected` с «abort», статус-ключ остался `SYNCING/verify-failed`, outcome Failed.
4. `M4_FlipConflict_RequestRejected_FreezeLeft` (ревью №1) — routing чужой → заявка удалена, журнал rejected; Unfreeze НЕ вызывался (заморозка оставлена).
5. `M5_DropSubFails_StillDoneWithError` — DropSubscription ExecuteResult fail → Done, журнал содержит last_error, reverse НЕ создан (анти-петля, spec M5).
6. `M5_SkipReverse_NoReverseArtifacts` — заявка `skip_reverse:true` → нет pub_rb/sub_rb.
7. `M4_ResumeFromFrozen_RepeatsCutoverSafely` — сид статуса `FROZEN/flip` + routing=shard1 → freeze-вызов снова, успешный flip.

- [ ] **Step 2: Run → FAIL.** **Step 3: Реализация.** **Step 4: Run → PASS + build.** **Step 5: Commit** `t01: MoveProcess M4–M6 — cutover (transient/permanent), обратная подписка, done/снапшот`.

---

### Task 15: Rollback + Finalize (finalize с fallback DROP SUBSCRIPTION)

**Files:**
- Create: `src/PgWorker.Moves/Process/SubscriptionDrop.cs`
- Modify: `src/PgWorker.Moves/Process/MoveProcess.cs`
- Test: `src/tests/PgWorker.UnitTests/Moves/MoveProcessRollbackFinalizeTests.cs`

**Interfaces:**
- Produces (ревью №3): общий хелпер среза подписки (используют finalize здесь и abort в Task 16):
```csharp
// DROP SUBSCRIPTION с fallback при недоступном источнике (abort-move.sh drop_sub):
// DROP → при сбое: DISABLE → SET (slot_name = NONE) → DROP; слот-сирота (имя = имя
// подписки, PG-конвенция) добивается вызывающим кодом отдельно.
public static class SubscriptionDrop
{
    public static Task<Result> DropAsync(IMoveSqlExecutor sql, string dsn, string sub, CancellationToken ct);
}
```
`RunRollbackAsync` (spec §6.3): статус-ключ есть → Permanent «только из ACTIVE»; routing владелец; поиск `SubExists(SubRb)` по всем шардам snap: найден на владельце → Permanent «странно»; нигде → Permanent «re-copy»; ровно один не-владелец → `cutover.RunAsync(Cur=owner, New=that, Slot=SubRb, FailState:"SYNCING", DropStatusOnFail:true)` — при сбое до flip cutover сам разморозит и удалит статус-ключ (нет ключа = ACTIVE; скриптовый эквивалент `state=ACTIVE`); permanent-исходы (verify/flip-conflict) обрабатываются как в M4 → PermanentReject. После flip: `SubscriptionDrop.DropAsync(newDsn, SubRb)` best-effort-last_error, `DropPublication(PubRb, oldOwnerDsn)` best-effort, `Unfreeze(new)` (обязателен: fail → Transient), снапшот best-effort, del заявки, Done.

`RunFinalizeAsync` (spec §6.4): статус-ключ есть → Permanent; OldShard невалиден/== owner → Permanent; порядок: `SubscriptionDrop.DropAsync(oldDsn, SubRb)` → `SubscriptionDrop.DropAsync(ownerDsn, Sub)` → `DropPublication(Pub, old)` → `DropPublication(PubRb, owner)` → слоты на old: основной слот `Sub(bucket)` (если остался после fallback — `SlotExists`→`SlotActive`? terminate+wait→`DropSlot`, по скриптовому cleanup_slots) + осиротевшие tablesync `OrphanTablesyncSlots` (неактивные — DropSlot; активные — журнал-пропуск) → `DropSchemaCascade(bucket, old)` → снапшот → del заявки → Done. Каждый шаг идемпотентен через exists-проверки; fallback-случай оставляет слот-сироту на источнике — добивается этим же шагом слотов, если источник доступен (см. тест 3).

- [ ] **Step 1: Failing-тесты**:

1. `Rollback_ActiveWithReverse_FlipsBackAndUnfreezes` — сид: routing=bucket→shard2, sub_rb существует на shard1, cutover-happy → routing=shard1, статус удалён, Calls: `DROP SUBSCRIPTION sub_bucket_42_rb`, `DROP PUBLICATION pub_bucket_42_rb`, GRANT (unfreeze) на shard1-DSN.
2. `Rollback_NoReverseAnywhere_RejectsPermanent`.
3. `Finalize_SourceUnavailable_SubDroppedLocally_OrphanSlotKilled` (ревью №3) — резолвер: `DROP SUBSCRIPTION` (по owner-DSN) падает (имитация недоступного источника подписки), `ALTER SUBSCRIPTION` проходит; слот `sub_bucket_42` «существует» на old-DSN и неактивен → Calls содержат: `ALTER SUBSCRIPTION sub_bucket_42 DISABLE`, `ALTER SUBSCRIPTION sub_bucket_42 SET (slot_name = NONE)`, повторный `DROP SUBSCRIPTION sub_bucket_42`, `pg_drop_replication_slot('sub_bucket_42')`; Done.
4. `Finalize_OrderAndOrphans` — сид: pub на old, sub на owner, схема на old, orphan-слот `sub_b42_sync_1234` (неактивен) → порядок Calls: DROP SUBSCRIPTION sub_rb → DROP SUBSCRIPTION sub → DROP PUBLICATION pub → DROP PUBLICATION pub_rb → pg_drop_replication_slot('sub_bucket_42_sync_1234') → DROP SCHEMA bucket_42 CASCADE; заявка удалена; Done.
5. `Finalize_ActiveOrphanSlot_Skipped` — slot active=true → DROP SLOT отсутствует, схема всё равно дропнута (поведение скрипта).

- [ ] **Step 2: Run → FAIL.** **Step 3: Реализация (+ CutoverContext.DropStatusOnFail из Task 12).** **Step 4: Run → PASS + build.** **Step 5: Commit** `t01: rollback (зеркальный cutover) + finalize с fallback DROP SUBSCRIPTION и сиротами-слотами`.

---

### Task 16: AbortSequence (порт abort-move.sh)

**Files:**
- Create: `src/PgWorker.Moves/Process/AbortSequence.cs`
- Modify: `src/PgWorker.Moves/Process/MoveProcess.cs` (`RunAbortAsync` — делегирует; контракт «одна заявка на бакет» (spec §4.1): ключ `/pgworker/moves/<C>/bucket_<i>` один — abort-заявка оператора физически ПЕРЕЗАПИСЫВАЕТ move-заявку того же бакета, отдельного «удаления move-заявки» не существует и не требуется — ревью №6)
- Test: `src/tests/PgWorker.UnitTests/Moves/AbortSequenceTests.cs`

**Interfaces:**
- Consumes: `IMoveSqlExecutor`, `MoveStatusStore`, `MoveRequestsStore`, `WorkJournal`, `SubscriptionDrop.DropAsync` (Task 15), `TimeProvider` (AbortMinAgeSec, Д12).
- Produces: `AbortSequence(IMoveSqlExecutor sql, MoveStatusStore status, MoveRequestsStore requests, WorkJournal journal)`:
`Task<Result<ProcessOutcome>> RunAsync(ClusterSnapshot snap, string bucket, MoveRequest request, ClaimStore claims, TimeProvider clock, MovesRuntimeOptions o, CancellationToken ct)`.

Шаги (spec §6.5): (1) routing есть/шард валиден — иначе Permanent; статус-ключ есть — иначе Permanent («ACTIVE, откатывать нечего»); state==ABORTING → продолжение с inherited started_unix; защита свежести: `clock.Now - UpdatedUnix < AbortMinAgeSec && !Force` → Transient «mover возможно жив»; routing==target && !Force → Permanent «flip прошёл — доведение только с force». (2) инвентаризация на ВСЕХ шардах: pub/sub/slot по 4 конвенциям + schema — Scalar-доступность каждого шарда: недоступен → журнал `ABORTING/blocked` (Serialize AbortJournal в статус-ключ, unreachable_shards) + Transient. (3) журнал `ABORTING/db-cleanup` + план (★ ДО манипуляций). (4) фазы: `drop-subscriptions` (`SubscriptionDrop.DropAsync` для каждой exists-подписки: основной слот подписки остаётся сиротой при fallback → добивается фазой слотов), `drop-slots` (exists → active? terminate+wait 5×1s → не дезактивировался → failed; pg_drop), `drop-publications`, `unfreeze-owner` (схема есть → GRANT-симметрия), routing==target → `sync-sequences` (SequenceNames на НЕ-владельце со схемой → issued там / next у владельца → SetvalForward — только вперёд, ДО drop schema), `drop-schema` (только не-владельцы, CASCADE). (5) контрольная инвентаризация → остатки → journal `failed` + Transient. (6) `DeleteAsync` статус-ключа + del СВОЕЙ заявки (op=abort) + снапшот best-effort → Done.

- [ ] **Step 1: Failing-тесты**:

1. `Abort_JournalBeforeManipulations` — инъекция: `ExecuteResult` fail на первом DROP → после тика статус-ключ = `ABORTING/db-cleanup` (журнал записан ДО попытки SQL, включая план).
2. `Abort_UnreachableShard_BlockedJournal` — scalar недоступен → статус `ABORTING/blocked` + unreachable_shards содержит шард, Transient.
3. `Abort_FreshMoveWithoutForce_Waits` — UpdatedUnix=now → Transient, статус не ABORTING.
4. `Abort_Force_CleansEverything_ActiveAgain` — полный happy: все exists → порядок Calls: DROP SUB (sub, sub_rb) → slot terminate/drop → DROP PUBLICATION (pub, pub_rb) → GRANT на владельце → DROP SCHEMA на не-владельце; статус-ключ удалён; Done; своя заявка (op=abort) удалена.
5. `Abort_RoutingEqualsTarget_NoForce_Rejects` — Permanent.
6. `Abort_RoutingEqualsTarget_Force_SyncsSequencesBeforeDrop` — force: Calls содержат Setval(владелец) ДО DROP SCHEMA.
7. `Abort_OwnerSchemaNeverDropped` — DROP SCHEMA вызывался только с dsn не-владельца.
8. `Abort_LeftoverFailsControl` — контрольная инвентаризация находит остаток → `ABORTING/failed`, Transient.

- [ ] **Step 2: Run → FAIL.** **Step 3: Реализация.** **Step 4: Run → PASS + build.** **Step 5: Commit** `t01: AbortSequence — P7-порт: журнал ABORTING, идемпотентная уборка, доведение`.

---

### Task 17: Интеграция в App (цикл, конфиг, DI, deprovisioning-чистка)

**Files:**
- Modify: `src/PgWorker.App/Options.cs` (`MovesOptions` класс + поля `ThresholdsOptions.CutoverTimeoutSec`, `ConnFailBudgetSec`)
- Modify: `src/PgWorker.App/Loops/ClusterProcesses.cs` (+`ProcessMovesAsync`)
- Modify: `src/PgWorker.App/Loops/ReconcileLoop.cs` (вызов в default-ветке)
- Modify: `src/PgWorker.App/Program.cs` (DI: `IMoveSqlExecutor`, `MoveDdl`, `ShardEndpoints`, `MoveProcess` — включая передачу `TimeProvider.System`, `ILogger<MoveProcess>` и снапшот-делегат; `BucketEvacuator` на `ShardEndpoints`)
- Modify: `src/PgWorker.Provisioning/Processes/DeprovisioningProcess.cs` (D2: + `/pgworker/moves/{cluster}` prefix-delete после `/pgworker/work`)
- Modify: `src/PgWorker.App/appsettings.json` (+`Moves`-секция, +2 порога)
- Test: `src/tests/PgWorker.UnitTests/App/ReconcileLoopTests.cs` (новый кейс), `src/tests/PgWorker.UnitTests/Provisioning/DeprovisioningProcessTests.cs` (новый кейс)

**Interfaces:**
- Produces: `IClusterProcesses.ProcessMovesAsync(ClusterSnapshot, CancellationToken): Task<Result<ProcessOutcome>>`; `MovesOptions { PollIntervalSec=2, FreezeWaitSec=5, FreezeLockTimeoutSec=5, FreezeLockTries=3, AbortMinAgeSec=120, FailoverSlots=true }`; маппер `MovesRuntimeOptions` из `(MovesOptions, ThresholdsOptions)`.

- [ ] **Step 1: Failing-тесты**

`ReconcileLoopTests`-новый-кейс (по образцу существующих — мок `IClusterProcesses`):

```csharp
// AAA: кластер Active после надзора обрабатывает заявки переездов (spec §5.3)
[Fact] Tick_ActiveCluster_CallsProcessMoves — после тика мок-процессов записал вызов ProcessMovesAsync (после SuperviseAsync).
```

`DeprovisioningProcessTests`:

```csharp
// AAA: D2 чистит и заявки переездов — /pgworker/moves/<C>/ не переживает кластер
[Fact] Deprovision_RemovesMovesPrefix — сид заявки + полный цикл → FakeEtcd не содержит MovesPrefix.
```

- [ ] **Step 2: Run → FAIL.** **Step 3: Реализация правок (в ReconcileLoop — в default-ветке после foreach evacuate: `await RunClusterOpAsync(cluster, "moves", () => processes.ProcessMovesAsync(snap, ct), ct);`; Program.cs — сборка MovesRuntimeOptions из двух секций + `ILogger<MoveProcess>` из `sp.GetRequiredService<ILoggerFactory>()`).** **Step 4: Run → PASS (весь unit-проект) + build.** **Step 5: Commit** `t01: интеграция — ReconcileLoop/IClusterProcesses, DI, MovesOptions, чистка заявок при deprovisioning`.

---

### Task 18: Integration-тесты (etcd-контракт + docker exec)

**Files:**
- Test: `src/tests/PgWorker.IntegrationTests/Etcd/MoveContractTests.cs`
- Test: `src/tests/PgWorker.IntegrationTests/Docker/ExecDriverTests.cs`

**Interfaces:** Consumes: `EtcdFixture` (Testcontainers etcd), `MoveRequestsStore`, `MoveStatusStore`, `DockerTrait`/`DockerDriverTests`-паттерн, `PlainClusterDriver`, `DockerEngineFactory`.

- [ ] **Step 1: Тесты etcd** (по образцу `EtcdCoordinationTests`):

```csharp
// AAA: AC7 заявок — старейшая по requested_unix выбирается на реальном etcd
[Fact] Requests_OldestWins — две Put-заявки → OldestAsync → минимальная.

// AAA: AC7 flip — конкурентная txn на реальном etcd: второй flip не проходит
[Fact] Flip_CompetingTxn_Fails — put routing=shard1 → FlipAsync(shard1→shard2) ok → FlipAsync(shard1→shardX) (тот же cur) → false, значение не изменилось.

// AAA: успешный flip удаляет статус-ключ той же транзакцией (нет ключа = ACTIVE)
[Fact] Flip_DropsStatusAtomically.
```

- [ ] **Step 2: Тест docker exec** (trait `DockerAvailable`, ревью №8 — spec §10 «exec-механика драйвера»): по образцу `DockerDriverTests` — создать alpine-контейнер с именем `pgw-execit-shard1-n1` (Cmd `sleep 30`) на unix-сокете → `PlainClusterDriver.ExecNodeAsync("execit","shard1","n1", ["echo","hello"])` → `"hello"`; после `rm -f` контейнера → `Result.Failed` «контейнер не найден». Кейс ненулевого exit (`["sh","-c","exit 3"]`) → Failed с кодом в сообщении.

- [ ] **Step 3: Run** `dotnet test src/tests/PgWorker.IntegrationTests --filter "FullyQualifiedName~MoveContract|FullyQualifiedName~ExecDriver"` (etcd — Testcontainers; docker-серия — при доступном docker) → PASS. **Step 4: build.** **Step 5: Commit** `t01: integration — контракт заявок/flip на etcd + exec-механика драйвера`.

---

### Task 19: E2E-сценарий переезда (стенд E2eFixture)

**Files:**
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eFixture.cs` (`StartHostAsync` env: `PgWorker__Moves__FailoverSlots=false` (spilo-16, R1/Д11), `PgWorker__Moves__FreezeWaitSec=1`, `PgWorker__Moves__PollIntervalSec=1`, `PgWorker__Moves__AbortMinAgeSec=3`, `PgWorker__Thresholds__CutoverTimeoutSec=60`, `PgWorker__Thresholds__ConnFailBudgetSec=15`)
- Test: `src/tests/PgWorker.IntegrationTests/E2e/E2eMoveScenarios.cs`

**Interfaces:** Consumes: `E2eFixture` (etcd + образ `pgworker-node:e2e` + `StartHostAsync` + `WaitForAsync`), Npgsql напрямую (пробы/DDL-сид).

- [ ] **Step 1: Сценарий** (один `[Fact]`, последователен; кластер `mshop`, `buckets=6`, сид = копия `SeedClusterAsync("mshop")` из `E2eScenarios`):

1. Запуск host → `WaitForAsync(Provisioned("mshop"), 360s)`.
2. DDL-подготовка на мастере shard1 (порт из `/pgworker/portalloc/mshop` `shard1/<master-node>`; master-узел — из `/clusters/mshop/shards/shard1/master` или Patroni; Npgsql DSN `Host=localhost;Port=<pg>;Database=mshop;Username=postgres;Password=pgw-e2e-su`): `CREATE TABLE bucket_0.items(id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, note text NOT NULL);` + то же для `bucket_1`; INSERT 50 строк в `bucket_0.items` и 10 в `bucket_1.items`; гранты: `GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA bucket_0 TO app; GRANT USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA bucket_0 TO app; GRANT SELECT ON ALL TABLES IN SCHEMA bucket_0 TO bucket_mover;` (и для bucket_1).
3. Фоновая нагрузка: Task-цикл `INSERT INTO bucket_0.items(note) VALUES (...)` под `app`/`pgw-e2e-app` на мастер shard1 c ретраями при исключениях; нагрузка-хелпер пишет timeline-события `(timestamp, Ok | FrozenDenied)` — по ним измеряется окно FROZEN (шаг 6, ревью №7).
4. Заявка move: `PutAsync(MoveKey("mshop","bucket_0"), {"op":"move","to":"shard2","requested_unix":<now>})`.
5. `WaitForAsync(routing[bucket_0]=="shard2" && status[bucket_0]==null, 120s)` — AC3-move.
6. Проверки: **окно FROZEN** (AC3, ревью №7): `firstDeniedAt` = timestamp первого события `FrozenDenied` (SqlState 42501), `firstOkAfter` = timestamp первого `Ok` после него; assert `firstOkAfter - firstDeniedAt <= 15s` (FreezeWaitSec=1 + буфер на sequences/counts/flip мелких таблиц; спек: «FreezeWaitSec + несколько секунд»). counts равны (Npgsql по обоим шардам); sequence-инвариант: `SELECT CASE WHEN is_called THEN last_value ELSE last_value-1 END FROM bucket_0.items_id_seq` на shard1 < next (last_value(+1/is_called)) на shard2 (AC4); нагрузка жива (последний успешный INSERT после flip — через мастер shard2).
7. Призрак P1: Npgsql-сессия `app` на мастер shard1 (открыта ДО flip) → `INSERT INTO bucket_0.items...` → ожидание `PostgresException` с `SqlState == "42501"` (permission denied) — AC3-призрак.
8. Rollback: заявка `{"op":"rollback"}` → `WaitForAsync(routing=="shard1" && status==null, 120s)` → INSERT под app на мастер shard1 успешен (заморозка снята).
9. Повторный move → снова shard2 (шаги 4–5).
10. Finalize: заявка `{"op":"finalize","old_shard":"shard1"}` → `WaitForAsync`: на shard1 `to_regnamespace('bucket_0') IS NULL` AND нет `pg_subscription`/`pg_publication` по конвенциям AND нет `pg_replication_slots LIKE '%bucket_0%'` (AC3-finalize, AC6-артефакты).
11. Abort-сценарий (bucket_1, находящийся на shard1): заявка move bucket_1→shard2; `WaitForAsync(status[bucket_1] содержит SYNCING, 60s)`; **перезаписать ту же заявку** на `{"op":"abort","force":true}` (ключ один — spec §4.1, ревью №6); `host.Kill()` (смерть контроллера посреди уборки); новый `StartHostAsync`; `WaitForAsync(status[bucket_1]==null, 180s)`; проверки: артефактов bucket_1 нет нигде (набор п.10 по обоим шардам), INSERT под app на мастер shard1 в `bucket_1.items` работает (AC6-abort).
12. Deprovisioning с заявкой: put мусорной заявки на bucket_2; `TO_REMOVE` (хелпер `SetToRemoveAsync`-копия); `WaitForAsync(Deprovisioned("mshop"), 180s)` → range `/pgworker/moves/mshop/` пуст (AC8).
13. Нагрузка-остановка, dispose.

Оформить хелперы: `MasterDsn(shard)`, `Scalar(dsn, sql)`, `PutMoveRequestAsync`, `RoutingAsync(bucket)`, `StatusAsync(bucket)`, `ArtifactsClean(shard, bucket)`.

- [ ] **Step 2: Run** `dotnet test src/tests/PgWorker.IntegrationTests --filter "FullyQualifiedName~E2eMove"` (docker доступен; длительность ~5–10 мин). Ожидание: PASS. Возможные проблемы и правки в этой же задаче: (а) `su postgres -c` не работает в образе → заменить на прямой `pg_dump` от root с `--no-password` (env PGHOST/PGUSER внутри контейнера уже настроены Spilo) — подобрать фактически; (б) identity-sequence видна в SequenceNames (`GENERATED BY DEFAULT AS IDENTITY` создаёт sequence в той же схеме, relkind='S'); (в) master-ключ формат `host:port` — парсер уже есть в ShardEndpoints.

- [ ] **Step 3: build решения.** **Step 4: Commit** `t01: e2e — move (окно FROZEN, призрак, rollback, finalize) + abort-takeover + deprovisioning-чистка`.

---

### Task 20: Финальная проверка + roadmap-чистка t01 (мерж-гейт)

**Files:**
- Modify: `arch/roadmap/pgworker.md`
- Verify: всё решение.

- [ ] **Step 1: Полная сборка и тесты**

```bash
dotnet build src/PgWorker.slnx -c Release -warnaserror
dotnet test src/PgWorker.slnx -c Release
```

Ожидание: build 0 warnings/errors; unit — зелёные; integration (Testcontainers) — зелёные при доступном docker/e2e (AC1).

- [ ] **Step 2: Сверка критериев приёмки spec §11** — чеклистом по AC1–AC9 (AC2 — unit-фазовые таблицы Tasks 11–16; AC3–AC6 — Task 19; AC7 — Tasks 11/18; AC8 — Tasks 17/19; AC9 — Tasks 2/6 — форматы как в скриптах). Пробелы — устранить в этой задаче.

- [ ] **Step 3: Roadmap-чистка (правило мерж-гейта)**

В `arch/roadmap/pgworker.md`: удалить пункт `t01-move-bucket-csharp` целиком; в пункте `t06-shard-autoscaling` убрать `поверх t01-move-bucket-csharp`-фразу зависимости (переписать «поверх C#-переездов (реализованы)» → просто «с оркестрацией PgWorker»). Проверка: `grep -c 't01-move' arch/roadmap/*.md` → 0.

- [ ] **Step 4: Commit (попадает в merge-коммит)**

```bash
git add arch/roadmap/pgworker.md
git commit -m "t01: roadmap — задача исполнена, тег удалён (мерж-гейт)"
```

---

## Порядок и зависимости

`1 arch → 2 модель → 3 заявки → 4 статус → 5/6 SQL → 7 executor → 8 exec → 9 endpoints → 10 ddl → 11 M0 → 12 cutover → 13 M1–M3 → 14 M4–M6 → 15 rollback/finalize (+SubscriptionDrop) → 16 abort → 17 App-интеграция → 18 integration (etcd + exec) → 19 e2e → 20 финал/roadmap`.

Зависимости: 11+ требуют 3–10; 13+ требует 12; 14 классифицирует исходы 12; 16 требует 15 (`SubscriptionDrop`); 17 требует 11–16; 19 требует 17. Задачи 5–7 и 8–10 можно исполнять параллельно после 4.

## Журнал ревью план↔spec (Фаза 4, CHANGES_REQUESTED — устранено)

Все девять замечаний ревью внесены: №1 — классификация transient/permanent в Task 12/14 (+`CutoverPermanentException`, тесты verify-failed/flip-conflict → del заявки); №2 — `MoverNpgsqlDsn` в Task 9 (+тесты), пробы mover-роли по нему в Task 11 (+кейс 14); №3 — `SubscriptionDrop` с fallback в Task 15 (+тест локального среза и добивания слота), переиспользован в Task 16; №4 — перезапись статус-ключа каждый тик M3 (Task 13, тест 7, `TimeProvider` в конструкторе MoveProcess с Task 11); №5 — `SchemaExists(src)` в M0 (Task 11, кейс 13); №6 — убрано «del move-заявки того же бакета» (Task 16), e2e-п.11 переформулирован как перезапись ключа; №7 — измерение окна FROZEN в e2e (Task 19 п.3/6) + лог готовности/лага в M3 (Task 13, `ILogger<MoveProcess>`); №8 — integration-тест `ExecDriverTests` (Task 18 Step 2); №9 — ссылка «Task 18» → «Task 17» (Task 9).
