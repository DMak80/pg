# t06-shard-autoscaling — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Цель:** add/remove-shard живого кластера из панели AdminPanel с оркестрацией PgWorker (два новых процесса A0–A6/S0–S4, guard'ы G1–G7, две мутации API, UI), БЕЗ автоматической перебалансировки бакетов.

**Архитектура:** декларативная поверх существующего контракта etcd: add-shard — переиспользование ключей декларации кластера (replicas + nodes/NOT_INITIALIZED + request_*), remove-shard — новый one-way маркер `shards/<X>/state=TO_REMOVE`. PgWorker в Active-ветке ReconcileLoop получает scale-проход (remove → add, по одному шард-за-тик); панель пишет декларацию/маркер по образцу §9.2/§9.4 своего контракта. Прогресс виден панели из её существующих префиксов, `/pgworker/` панели не читает.

**Стек:** .NET 10 (C# latest, Nullable, TreatWarningsAsErrors), xUnit + FluentAssertions, Testcontainers (integration/e2e), React+Mantine+react-query (frontend), HTTP JSON etcd gateway `/v3/*`.

**Spec:** `docs/superpowers/2026-08-24-t06-shard-autoscaling/spec.md` (worktree pg) — план аргументируется от spec'а; исполнители читают оба.

**Worktree:**
- репо `pg`: `/Users/demakaev/ZCodeProject/worktrees/feat-t06-shard-autoscaling` (ветка `feat-t06-shard-autoscaling`) — Tasks 1–11, 17.
- репо `AdminPanel`: `/Users/demakaev/ZCodeProject/worktrees/ap-t06-shard-autoscaling` (ветка `feat-t06-shard-autoscaling`) — Tasks 12–16, 17.

## Global Constraints

- Язык документации/комментариев — русский; идентификаторы — английские (AGENTS.md).
- .NET 10, `TreatWarningsAsErrors=true` — код обязан собираться без warning'ов; новых NuGet-пакетов и секций конфигурации НЕТ (spec §5.7).
- Тесты — xUnit, комментарии по нотации AAA, русские (spec §8).
- КЛЮЧЕВАЯ ГРАНИЦА (spec §2.1): add-shard не двигает ни один бакет (routing/status/схемы не пишутся); remove шарда с бакетами блокируется guard'ом G3.
- Мутации `/clusters/` и docker — только держателем пер-кластерного клэйма (spec §3.5).
- Коммит-стратегия: каждая Task — отдельный коммит в feature-ветку СВОЕГО репозитория, префикс `t06` в сообщении. Работаем только в своих worktree (никаких правок `main` напрямую).
- Сборка/тесты pg: `dotnet build src/PgWorker.slnx -c Release` (из корня worktree pg); unit — `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release`; integration — `dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release` (нужен Docker; e2e требует предварительного Release-билда — E2eFixture ищет `src/PgWorker.App/bin/Release/net10.0/PgWorker.App.dll`).
- Сборка/тесты AdminPanel: `dotnet build src/AdminPanel.slnx -c Release` (0 warnings); unit — `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj -c Release`; integration — `dotnet test src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj -c Release` (Docker); frontend — `cd frontend && npm run typecheck && npm run build`.
- Roadmap-гейт: пункт `t06-shard-autoscaling` удаляется из `arch/roadmap/pgworker.md` тем же набором коммитов, что и реализация (Task 1); никаких пометок «закрыта» — история в git.

Сокращения путей: `pg/…` = `/Users/demakaev/ZCodeProject/worktrees/feat-t06-shard-autoscaling/…`; `ap/…` = `/Users/demakaev/ZCodeProject/worktrees/ap-t06-shard-autoscaling/…`.

---

### Task 1: arch-правки pg — канон контракта и roadmap

**Files:**
- Modify: `pg/arch/14-pgworker.md` (§1, §3.1, §3.2, §3.3, §5: шапка классификации + новые G/H, §5 C/F/B)
- Modify: `pg/arch/11-bucket-sharding.md` (§2 таблица ключей, §4.5)
- Modify: `pg/arch/roadmap/pgworker.md` (удалить t06, добавить t07)

**Interfaces:**
- Consumes: spec §4 (контракт), §4.3 (таблица ключей), §4.4 (guard'ы), §5 (процессы), §10.1/§10.3 (deliverables).
- Produces: канон в arch/, на который ссылаются коммиты задач 2–11; roadmap без t06, с t07.

- [ ] **Step 1: arch/14-pgworker.md — общие правки**

В §1 (роль/разделение ответственности) и в перечне процессов добавить процессы **№6 add/remove шарда**. В §1 «Границы» добавить строку: «панельные кнопки ЯВНЫХ переездов бакетов — roadmap `t07-move-bucket-ui`; в t06 переезды инициируются только etcdctl'ом (заявки `/pgworker/moves/`, t01)».

В §3.1 (читаемые) добавить строку таблицы:

```
| `/clusters/<C>/shards/<X>/state` | маркер демонтажа шарда `TO_REMOVE` (пишет ТОЛЬКО панель; отсутствие = обычный шард; t06) |
```

В §3.2 (пишемые/удаляемые) добавить таблицу из spec §4.3 (перенести дословно — 7 строк: nodes state PROVISIONING→RUNNING при add A3/A4; dsn при A5; REMOVING при S2; del prefix `shards/<X>/` при S3; del prefix+точечные `/service/<C>-<X>/`; portalloc-фильтрация `"<X>/<n>"`; del `/pgworker/evacuations/<C>/<X>`). Добавить абзац о контрактных финальных состояниях после add/remove (spec §4.3, последний абзац).

В §5 после описания классификации дополнить: «Active-ветка после надзора выполняет scale-проход `ScaleShardsAsync` (t06): remove-кандидаты (`shards/<X>/state=TO_REMOVE`) → затем add-кандидаты (declared-ноды без `dsn`), по одному шард-за-тик; демонтаж освобождает хосты/порты до подъёма (Д13)».

- [ ] **Step 2: arch/14-pgworker.md — новые разделы процессов G и H**

После раздела «F. MoveProcess» добавить два раздела (тексты фаз — дословно из spec §5.2/§5.3, guard'ы — таблица §4.4):

```markdown
### G. AddShardProcess (A0–A6; t06)
Подъём ОТДЕЛЬНОГО пустого шарда в Active-кластере (панель заявила декларацию
§9.5 контракта панели: replicas + nodes/NOT_INITIALIZED + request_*).
Машина состояний одного тика, идемпотентна (механика ProvisioningProcess
в scoped-to-shard виде: EnsureNode, WaitPatroni, portalloc-merge,
DatabaseProvisioner). Guard A1: кластер Active; полное объявление (replicas>0,
nodes.Count==replicas, ноды NOT_INITIALIZED/PROVISIONING — иначе
phase=waiting-keys); `dsn` нет (есть → Done); scope `/service/<C>-<X>/initialize`
отсутствует (живой чужой Patroni-кластер = коллизия имён — перманентная ошибка);
имя шарда `^[a-z][a-z0-9_]{0,30}$`; перечитывание config (R6) — NOT_INITIALIZED/
TO_REMOVE → phase=aborted. A5: БД/роли — ТОЛЬКО они; СХЕМЫ БАКЕТОВ НЕ
СОЗДАЮТСЯ (шард пустой, routing не указывает). Routing/status не пишутся ВООБЩЕ.

### H. RemoveShardProcess (S0–S4; t06)
Демонтаж шарда по маркеру `shards/<X>/state=TO_REMOVE` (пишет панель).
Guard'ы G1–G7 в S1 перед любым разрушающим действием (таблица §4.4):
G1 кластер Active; G2 шард заявлен; G3 ни один routing не указывает на шард
(P23); G4 ни один status-ключ не ссылается (owner ИЛИ target); G5 нет заявок
`/pgworker/moves/<C>/` с to=X или old_shard=X (саморазрешающийся); G6 нет нод
QUARANTINED; G7 в кластере есть другой шард. Провал guard'а = journal
last_error с причиной + InProgress (маркер-состояние живёт; после уезда
бакетов демонтаж продолжится сам). Порядок «сначала docker, потом etcd»
сохранён (мёртвые ключи при сбое безвредны — повторный тик продолжает).
S3: del prefix shards/<X>/ + точечные request_* + del prefix scope +
portalloc-фильтрация "<X>/<n>" (read-modify-write под клэймом) +
del /pgworker/evacuations/<C>/<X>.
```

В §5 C (NodeSupervisor) дополнить границы (spec §5.4): «EnsureDeclaredNodes пропускает шарды без `dsn` (домен AddShardProcess) и с маркером TO_REMOVE (домен RemoveShardProcess — не пересоздавать демонтируемое). Кандидат эвакуации требует `dsn` и ≥1 бакета на шарде по routing (эвакуация пустого/незарегистрированного шарда бессмысленна и блокировала бы G6 карантином); шард с TO_REMOVE-маркером кандидатом МОЖЕТ быть — эвакуация умирающего помеченного шарда освобождает бакеты, после чего G3 пропускает демонтаж (Д6)».

В §5 F (MoveProcess) добавить отказы M0 (spec §5.5): move `to` = шард в TO_REMOVE → перманентный отказ «шард помечен к удалению — выберите другую цель»; `to` без dsn → «шард ещё не поднят (add-shard не завершён)»; finalize с `old_shard` без dsn → «шард удалён — убирать нечего». Переезды ИЗ TO_REMOVE-шарда разрешены.

В §5 B (Deprovisioning D2) дополнить: «+ `del --prefix /pgworker/evacuations/<C>/` — журналы эвакуаций не переживают удаление кластера (t06, симметрия с S3)».

- [ ] **Step 3: arch/11-bucket-sharding.md**

В §2 (таблица ключей кластера) добавить строку `shards/<X>/state` → `строка "TO_REMOVE" | маркер демонтажа (t06: пишет панель, читают PgWorker и панель; отсутствие = обычный шард)`.

В §4.5 после цитаты «Декларативный provisioning» добавить абзац: «**Декларативный add/remove-shard (t06).** Для кластеров под управлением PgWorker панель AdminPanel заявляет новый шард переиспользованием ключей декларации (replicas + nodes/NOT_INITIALIZED + request_*, без dsn — его запишет PgWorker) и помечает демонтаж маркером `shards/<X>/state=TO_REMOVE`; PgWorker поднимает/демонтирует шард (инвариант P23 воспроизведён guard'ом G3). Скрипты этого раздела остаются ручным путём для внешних кластеров (без PgWorker)».

- [ ] **Step 4: arch/roadmap/pgworker.md — мерж-гейт t06 + новая t07**

Удалить пункт `t06-shard-autoscaling` (задача исполняется этим набором коммитов — мерж в `main` понесёт удаление). Добавить пункт (следующий свободный номер — t07; тегов t07 ещё нет):

```markdown
- **`t07-move-bucket-ui`** — UI явных переездов бакетов из панели AdminPanel
  (кнопки «перевезти/откатить/finalize/abort» → заявки
  `/pgworker/moves/<C>/bucket_<i>`, чтение очереди заявок и их результатов;
  выбор «кто куда переезжает» — только оператор, никакой автоперебалансировки).
  Выделена из t06 по решению пользователя; зависимостей нет (контракт заявок —
  t01, в main).
```

- [ ] **Step 5: Проверка**

Run: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t06-shard-autoscaling && git diff --stat`
Expected: 3 файла arch/ изменены; в `arch/roadmap/pgworker.md` нет строки `t06-shard-autoscaling`, есть `t07-move-bucket-ui`.

- [ ] **Step 6: Commit**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t06-shard-autoscaling
git add arch/14-pgworker.md arch/11-bucket-sharding.md arch/roadmap/pgworker.md
git commit -m "t06: arch — канон add/remove-shard (14 G/H, 11 §2/§4.5), roadmap t07 + мерж-гейт t06"
```

---

### Task 2: pg — модель и парсер: `ShardSpec.ToRemove`, `BucketRoute.MoveSource/MoveTarget`

**Files:**
- Modify: `pg/src/PgWorker.Core/Model/Domain.cs:44-53` (ShardSpec, BucketRoute)
- Modify: `pg/src/PgWorker.Etcd/Parsing/ClusterSnapshotParser.cs` (ShardAcc, ветка `shards/<X>/state`, owner/target статуса)
- Test: `pg/src/tests/PgWorker.UnitTests/Etcd/ClusterSnapshotParserTests.cs` (новые кейсы)

**Interfaces:**
- Consumes: существующие `ShardSpec(string Name, int Replicas, string? Dsn, string? Master, IReadOnlyList<NodeSpec> Nodes)`, `BucketRoute(int Id, string? Owner, BucketMoveState? Status)`.
- Produces: `ShardSpec(…, bool ToRemove = false)`; `BucketRoute(…, string? MoveTarget = null, string? MoveSource = null)`; парсер понимает `shards/<X>/state=="TO_REMOVE"` и поля `"owner"`/`"target"` из status-JSON. `Owner` BucketRoute — владелец по ROUTING; `MoveSource`/`MoveTarget` — owner/target СТАТУС-ключа (могут отличаться от routing после flip — guard G4, spec §4.4 «owner ИЛИ target»). Используют Tasks 3–8.

- [ ] **Step 1: Написать failing-тесты парсера**

Добавить в `ClusterSnapshotParserTests` (сид — инлайн-списки `Kv`, по образцу существующих тестов; `Kv = record(string Key, string Value, ulong ModRevision)`):

```csharp
[Fact]
public void ParseClusters_ShardStateToRemove_SetsToRemoveTrue()
{
    // Arrange — Active-кластер с маркером демонтажа шарда (t06 §4.2)
    var kvs = new List<Kv>
    {
        new("/clusters/shop/config", """{"buckets":2,"dbname":"shop"}""", 1),
        new("/clusters/shop/shards/shard1/replicas", "2", 2),
        new("/clusters/shop/shards/shard1/state", "TO_REMOVE", 3),
        new("/clusters/shop/buckets/routing/bucket_0", "shard1", 4),
        new("/clusters/shop/buckets/routing/bucket_1", "shard1", 5),
    };

    // Act
    var result = ClusterSnapshotParser.ParseClusters(kvs, out var errors);

    // Assert — маркер прочитан; parseError нет (значение одно — толерантность)
    errors.Should().BeEmpty();
    result.Value.Single().Shards.Single().ToRemove.Should().BeTrue();
}

[Theory]
[InlineData(null)] [InlineData("ACTIVE")] [InlineData("")]
public void ParseClusters_ShardStateAbsentOrOther_ToRemoveFalse(string? raw)
{
    // Arrange — ключа нет / иное значение = обычный шард (толерантность как у config.state)
    var kvs = new List<Kv> { new("/clusters/shop/config", """{"buckets":1,"dbname":"shop"}""", 1) };
    if (raw is not null) kvs.Add(new("/clusters/shop/shards/shard1/state", raw, 2));

    // Act
    var result = ClusterSnapshotParser.ParseClusters(kvs, out _);

    // Assert
    result.Value.Single().Shards.Single().ToRemove.Should().BeFalse();
}

[Fact]
public void ParseClusters_StatusOwnerAndTarget_ProduceMoveSourceAndMoveTarget()
{
    // Arrange — «flip прошёл, статус завис» (P7/G4): routing уже на shard2,
    // статус-ключ ещё жив с owner=shard1 (статус-owner ≠ routing-owner)
    var kvs = new List<Kv>
    {
        new("/clusters/shop/config", """{"buckets":1,"dbname":"shop"}""", 1),
        new("/clusters/shop/shards/shard1/replicas", "1", 2),
        new("/clusters/shop/shards/shard2/replicas", "1", 3),
        new("/clusters/shop/buckets/routing/bucket_0", "shard2", 4),
        new("/clusters/shop/buckets/status/bucket_0",
            """{"state":"FROZEN","owner":"shard1","target":"shard2","phase":"flip"}""", 5),
    };

    // Act
    var result = ClusterSnapshotParser.ParseClusters(kvs, out _);

    // Assert — owner И target статус-ключа попадают в маршрут; routing-owner —
    // отдельно (guard G4 сравнивает X со статус-owner/target, не с routing)
    var route = result.Value.Single().Routing.Single();
    route.Owner.Should().Be("shard2");
    route.Status.Should().Be(BucketMoveState.Frozen);
    route.MoveSource.Should().Be("shard1");
    route.MoveTarget.Should().Be("shard2");
}

[Fact]
public void ParseClusters_StatusNotInitialized_OwnerOnlyNoTarget()
{
    // Arrange — начальный статус создаваемого кластера: owner есть, target нет (02 §9)
    var kvs = new List<Kv>
    {
        new("/clusters/shop/config", """{"buckets":1,"dbname":"shop"}""", 1),
        new("/clusters/shop/buckets/routing/bucket_0", "shard1", 2),
        new("/clusters/shop/buckets/status/bucket_0",
            """{"state":"NOT_INITIALIZED","owner":"shard1","updated_unix":1}""", 3),
    };

    // Act
    var result = ClusterSnapshotParser.ParseClusters(kvs, out _);

    // Assert — NOT_INITIALIZED: MoveSource = owner статуса, MoveTarget = null
    var route = result.Value.Single().Routing.Single();
    route.Status.Should().Be(BucketMoveState.NotInitialized);
    route.MoveSource.Should().Be("shard1");
    route.MoveTarget.Should().BeNull();
}
```

- [ ] **Step 2: Прогнать тесты — убедиться в отказе компиляции**

Run: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t06-shard-autoscaling && dotnet build src/PgWorker.slnx -c Release`
Expected: FAIL — у `ShardSpec` нет `ToRemove`, у `BucketRoute` нет `MoveTarget`/`MoveSource` (CS1061).

- [ ] **Step 3: Реализовать модель**

В `pg/src/PgWorker.Core/Model/Domain.cs`:

```csharp
/// <summary>Шард кластера: replicas — плановое число нод, Dsn/Master — runtime;
/// ToRemove — маркер демонтажа shards/<X>/state=TO_REMOVE (t06; пишет панель).</summary>
public sealed record ShardSpec(string Name, int Replicas, string? Dsn, string? Master,
    IReadOnlyList<NodeSpec> Nodes, bool ToRemove = false);

/// <summary>Маршрут бакета: Owner — владелец по ROUTING (единственный авторитет
/// «где бакет»); Status — статус переезда (null → ACTIVE); MoveSource/MoveTarget —
/// owner/target из СТАТУС-ключа (guard G4 t06: после flip статус-owner ≠ routing-owner;
/// null без статуса; у NOT_INITIALIZED — owner без target).</summary>
public sealed record BucketRoute(int Id, string? Owner, BucketMoveState? Status,
    string? MoveTarget = null, string? MoveSource = null);
```

- [ ] **Step 4: Реализовать парсер**

В `pg/src/PgWorker.Etcd/Parsing/ClusterSnapshotParser.cs`:

1) `ShardAcc` — добавить поле `public string? StateRaw;`.
2) Новый case ПЕРЕД существующим `case "shards" when segments.Length == 6 && … is "dsn" or "replicas" or "master"`:

```csharp
case "shards" when segments.Length == 6
    && segments[4].Length > 0
    && segments[5] == "state":
{
    // Маркер демонтажа шарда (t06 §4.2): единственное значение "TO_REMOVE";
    // иное/битое — не ошибка, ToRemove=false (значение одно — parseError не пишем).
    var shard = GetOrAdd(acc.Shards, segments[4], static _ => new ShardAcc());
    shard.StateRaw = kv.Value;
    break;
}
```

3) `BuildShard` — передать `ToRemove: shard.StateRaw?.Trim() == "TO_REMOVE"` в конструктор `ShardSpec`.
4) `TryParseStatus` — добавить два out-параметра `out string? source, out string? target`: после успешного парсинга состояния `source = ReadString(root, "owner")`, `target = ReadString(root, "target")`; в ветке `NotInitialized` — `source = ReadString(root, "owner")`, `target = null` (02 §9: начальный статус без target). Обновить вызов в `BuildRouting` и прокинуть `MoveTarget: target, MoveSource: source` в `new BucketRoute(…)`. ВАЖНО: `BucketRoute.Owner` остаётся значением ROUTING-ключа — owner статуса живёт только в `MoveSource` (после flip они различаются — целевой случай G4 «flip прошёл, статус завис»).

- [ ] **Step 5: Прогнать тесты**

Run: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t06-shard-autoscaling && dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release --filter "FullyQualifiedName~ClusterSnapshotParserTests"`
Expected: PASS (все, включая старые).

- [ ] **Step 6: Полная сборка (0 warnings) + все unit**

Run: `dotnet build src/PgWorker.slnx -c Release && dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release`
Expected: PASS — дефолтные значения параметров record сохранили все существующие call-sites.

- [ ] **Step 7: Commit**

```bash
git add src/PgWorker.Core/Model/Domain.cs src/PgWorker.Etcd/Parsing/ClusterSnapshotParser.cs src/tests/PgWorker.UnitTests/Etcd/ClusterSnapshotParserTests.cs
git commit -m "t06: модель/парсер — shards/<X>/state=TO_REMOVE + owner/target статуса (ShardSpec.ToRemove, BucketRoute.MoveSource/MoveTarget)"
```

---

### Task 3: pg — детекция кандидатов scale: `ShardScaleClassifier`

**Files:**
- Create: `pg/src/PgWorker.App/Loops/ShardScaleClassifier.cs`
- Test: `pg/src/tests/PgWorker.UnitTests/App/ShardScaleClassifierTests.cs`

**Interfaces:**
- Consumes: `ClusterSnapshot` (Task 2: `ShardSpec.ToRemove`, `Dsn`, `Nodes`).
- Produces: `record ShardScaleCandidates(IReadOnlyList<string> Remove, IReadOnlyList<string> Add)`; `static ShardScaleClassifier.Detect(ClusterSnapshot snap)`. Использует Task 6.

- [ ] **Step 1: Написать failing-тесты**

`pg/src/tests/PgWorker.UnitTests/App/ShardScaleClassifierTests.cs` (по образцу `ClassificationTests`; snapshot собирать вручную из record'ов):

```csharp
using PgWorker.App.Loops;
using PgWorker.Core.Model;

namespace PgWorker.UnitTests.App;

// Детекция кандидатов scale-прохода Active-ветки (t06 spec §5.1).
public class ShardScaleClassifierTests
{
    private static ShardSpec Shard(string name, bool toRemove = false, string? dsn = null, int nodes = 2)
        => new(name, nodes, dsn, null,
            Enumerable.Range(0, nodes)
                .Select(i => new NodeSpec(name, $"{name}{(char)('a' + i)}", NodeState.Running))
                .ToList(), toRemove);

    private static ClusterSnapshot Snap(params ShardSpec[] shards)
        => new(new ClusterConfig("shop", 6, "shop", null, ClusterState.Active), shards, []);

    [Fact]
    public void Detect_DeclaredWithoutDsn_IsAddCandidate()
    {
        // Arrange — панель заявила shard3 (ноды есть, dsn нет)
        var snap = Snap(Shard("shard1", dsn: "host=h1"), Shard("shard3", dsn: null));

        // Act
        var candidates = ShardScaleClassifier.Detect(snap);

        // Assert
        candidates.Add.Should().Equal("shard3");
        candidates.Remove.Should().BeEmpty();
    }

    [Fact]
    public void Detect_ToRemoveMarker_IsRemoveCandidate()
    {
        // Arrange — шард поднят и помечен к удалению
        var snap = Snap(Shard("shard1", toRemove: true, dsn: "host=h1"));

        // Act
        var candidates = ShardScaleClassifier.Detect(snap);

        // Assert
        candidates.Remove.Should().Equal("shard1");
        candidates.Add.Should().BeEmpty();
    }

    [Fact]
    public void Detect_RegisteredWithoutMarker_IsNeither()
    {
        // Arrange — обычный живой шард
        var snap = Snap(Shard("shard1", dsn: "host=h1"));

        // Act / Assert
        ShardScaleClassifier.Detect(snap).Should().BeEquivalentTo(
            new ShardScaleCandidates([], []));
    }

    [Fact]
    public void Detect_MarkedUndeclaredShard_IsInBothLists()
    {
        // Arrange — помечен к удалению и не поднят (declared без dsn): оба списка
        // (spec §8: «оба одновременно — оба списка»; порядок прохода remove→add
        // и guard ToRemove в A1 разбирают конфликт — Task 4/6)
        var snap = Snap(Shard("shard1", toRemove: true, dsn: null));

        // Act
        var candidates = ShardScaleClassifier.Detect(snap);

        // Assert
        candidates.Remove.Should().Equal("shard1");
        candidates.Add.Should().Equal("shard1");
    }

    [Fact]
    public void Detect_NoNodesNoDsnNotCandidate()
    {
        // Arrange — ключи шарда без declared-нод (внешние кластеры без nodes) — не add
        var snap = Snap(Shard("s1", dsn: null, nodes: 0));

        // Act / Assert
        ShardScaleClassifier.Detect(snap).Add.Should().BeEmpty();
    }
}
```

- [ ] **Step 2: Прогнать — отказ компиляции**

Run: `dotnet build src/PgWorker.slnx -c Release`
Expected: FAIL — `ShardScaleClassifier` не существует.

- [ ] **Step 3: Реализовать**

`pg/src/PgWorker.App/Loops/ShardScaleClassifier.cs`:

```csharp
using PgWorker.Core.Model;

namespace PgWorker.App.Loops;

/// <summary>Кандидаты scale-прохода Active-ветки (t06 spec §5.1): чистая функция над снапшотом.</summary>
public sealed record ShardScaleCandidates(IReadOnlyList<string> Remove, IReadOnlyList<string> Add);

/// <summary>
/// Детекция шардов для Add/RemoveShardProcess: remove — маркер
/// shards/<X>/state=TO_REMOVE; add — declared-ноды (nodes.Count > 0) без dsn.
/// Шард может быть в обоих списках (помечен и не поднят): remove-проход идёт
/// первым и демонтирует его (Д5), AddShardProcess дополнительноguarded ToRemove.
/// </summary>
public static class ShardScaleClassifier
{
    public static ShardScaleCandidates Detect(ClusterSnapshot snap)
    {
        var remove = snap.Shards.Where(s => s.ToRemove).Select(s => s.Name).ToList();
        var add = snap.Shards.Where(s => s.Nodes.Count > 0 && s.Dsn is null).Select(s => s.Name).ToList();
        return new ShardScaleCandidates(remove, add);
    }
}
```

- [ ] **Step 4: Тесты зелёные + сборка**

Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release --filter "FullyQualifiedName~ShardScaleClassifierTests" && dotnet build src/PgWorker.slnx -c Release`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/PgWorker.App/Loops/ShardScaleClassifier.cs src/tests/PgWorker.UnitTests/App/ShardScaleClassifierTests.cs
git commit -m "t06: детекция кандидатов scale-прохода — ShardScaleClassifier (§5.1)"
```

---

### Task 4: pg — AddShardProcess (A0–A6)

**Files:**
- Create: `pg/src/PgWorker.Provisioning/Processes/AddShardProcess.cs`
- Test: `pg/src/tests/PgWorker.UnitTests/Provisioning/AddShardProcessTests.cs`

**Interfaces:**
- Consumes: `IClusterDriver` (EnsureNodeAsync/GetHostsAsync/GetBusyPortsAsync/ListNodeObjectsAsync), `ISqlExecutor`, `ShardProbe`, `ClaimStore.IsMine`, `WorkJournal.WritePhaseAsync`, `PlacementPlanner.Plan`, `PortAllocator.Allocate`, `Portalloc.Serialize/Parse`, `DatabaseProvisioner.BuildAdminDsn/BuildRoleGuardsSql`, `NodeResourcesParser.Parse` — всё существует.
- Produces: `AddShardProcess.TickAsync(ClusterSnapshot snap, string shardName, CancellationToken ct) → Task<Result<ProcessOutcome>>`; op-метка журнала `"add-shard"`; фазы `started/planned/waiting-keys/waiting-patroni/waiting-master/aborted/blocked-removing/done`. Использует Task 6.

- [ ] **Step 1: Написать failing-тесты**

`pg/src/tests/PgWorker.UnitTests/Provisioning/AddShardProcessTests.cs`. Rig — по образцу `ProvisioningProcessTests.NewRig` (Fakes.FakeEtcd/FakeDriver/FakeSql, ClaimStore с захваченным клэймом, WorkJournal, ShardProbe через FakeHandler; `SeedActiveCluster` сидит Active-кластер `shop` — config БЕЗ state, 2 шарда с dsn, routing, portalloc существующих нод). Обязательные кейсы (все с AAA-комментариями, русскими):

```csharp
// Сид Active-кластера: config без state, shard1/shard2 подняты (dsn + service
// initialize/leader), routing 0..5; portalloc "/pgworker/portalloc/shop" с
// записями shard1/shard2 (формат Portalloc.Serialize). Add-декларация shard3:
// replicas=2, nodes shard3a/b NOT_INITIALIZED, /service/shop-shard3/request_*.

[Fact]
public async Task Tick_IncompleteDeclaration_WaitingKeys_NoMutations()
// Arrange: только replicas, без nodes-ключей
// Act: TickAsync(snap, "shard3")
// Assert: InProgress; journal phase=waiting-keys (op=add-shard);
//         Driver.EnsuredNodes пуст; в Store нет ключей */shard3/* (кроме сида)

[Fact]
public async Task Tick_ScopeTaken_PermanentError_DeclarationStays()
// Arrange: сид + /service/shop-shard3/initialize уже существует (чужой Patroni)
// Act / Assert: outcome.IsSuccess==false; journal last_error содержит "shop-shard3";
//         декларация (replicas/nodes) жива в Store; docker-мутаций нет

[Fact]
public async Task Tick_AlreadyMarkedToRemove_BlockedRemoving_NoMutations()
// Arrange: add-декларация + маркер /clusters/shop/shards/shard3/state=TO_REMOVE
// Act / Assert: InProgress; phase=blocked-removing; EnsuredNodes пуст

[Fact]
public async Task Tick_ClusterToRemoveMidFlight_Aborted_NoMutations()
// Arrange: add-декларация; сид config перезаписан с "state":"TO_REMOVE"
// Act / Assert: InProgress; phase=aborted; EnsuredNodes пуст

[Fact]
public async Task Tick_FullDeclaration_EnsureNodesThenInProgress()
// Arrange: полный сид; Patroni-пробы глухие (DeadPatroni)
// Act / Assert: InProgress; EnsuredNodes == ["shard3/shard3a","shard3/shard3b"]
//   (порядок!); nodes state = PROVISIONING; portalloc-ключ содержит "shard3/shard3a"
//   и СОХРАНЯЕТ "shard1/shard1a"; journal phase=waiting-patroni

[Fact]
public async Task Tick_PatroniAlive_BootStrapsEmptyShardAndRegistersDsn()
// Arrange: полный сид; Patroni жив (по портам нод shard3)
// Act / Assert: Done; SQL: EnsuredDatabases содержит ("…","shop"); среди
//   Sql.Executed есть CREATE ROLE-гварды (BuildRoleGuardsSql) и НЕТ строки,
//   содержащей "CREATE SCHEMA bucket_" (ГЛАВНЫЙ ассерт границы §2.1);
//   dsn-ключ /clusters/shop/shards/shard3/dsn = "host=… port=… dbname=shop user=bucket_admin";
//   nodes state = RUNNING; routing bucket_0..5 НЕ изменились (сравнить с сидом);
//   ни одного status-ключа не появилось

[Fact]
public async Task Tick_DsnAlreadyWritten_DoneIdempotent()
// Arrange: полный сид + dsn-ключ уже записан
// Act / Assert: Done; EnsuredNodes пуст; SQL-вызовов нет

[Fact]
public async Task Tick_RerunAfterPartial_ConvergesToSameState()
// Arrange: первый тик с глухим Patroni (EnsureNode вызван, ноды PROVISIONING);
//   Patroni оживил; второй тик по СВЕЖЕМУ снапшоту (Snapshot(rig.Etcd) заново)
// Act / Assert: Done; dsn записан тем же значением, что и при первом прохождении
//   до A5 (детерминизм multi-host по именам нод); nodes RUNNING; CREATE SCHEMA
//   по-прежнему нет; routing по-прежнему не тронут (идемпотентность повтором тика)

[Fact]
public async Task Tick_PortRangeExhausted_LastErrorMentionsPortRange()
// Arrange: полный сид; rig с УЗКИМ PlacementOptions (например PortFrom=15000,
//   PortTo=15001) и FakeDriver.BusyPorts, содержащим ("h1",15000) — тройку
//   портов взять негде (частный случай §5.2 «порт-диапазон исчерпан»)
// Act / Assert: outcome.IsSuccess==false; journal phase=planning, last_error
//   содержит «расширьте PortRange»; декларация шарда жива (панельной отмены
//   нет — Д8); docker-мутаций нет; повторный тик с теми же порогами — тот же
//   отказ (ретраи тиками, разбор оператором через конфиг)
```

- [ ] **Step 2: Прогнать — отказ**

Run: `dotnet build src/PgWorker.slnx -c Release`
Expected: FAIL — `AddShardProcess` не существует.

- [ ] **Step 3: Реализовать AddShardProcess**

`pg/src/PgWorker.Provisioning/Processes/AddShardProcess.cs`. Структура (конструктор — как у ProvisioningProcess; failover-обёртки `GetAsync/PutAsync/RangeAsync/WithFailoverAsync` и `ReadPortAllocAsync` — скопировать дословно из `pg/src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs:462-534`):

```csharp
using System.Collections.Concurrent;
using System.Text.RegularExpressions;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Planning;
using PgWorker.Core.Templates;
using PgWorker.Docker.Drivers;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Probes;
using PgWorker.Provisioning.Sql;

namespace PgWorker.Provisioning.Processes;

/// <summary>
/// AddShardProcess — подъём ОТДЕЛЬНОГО пустого шарда в Active-кластере
/// (t06 spec §5.2; arch/14 §5 G). Панель заявила декларацию (replicas +
/// nodes/NOT_INITIALIZED + request_*); процесс доводит шард до dsn/RUNNING,
/// НЕ трогая routing/status/схемы бакетов (граница §2.1). Механика —
/// ProvisioningProcess в scoped-to-shard виде; идемпотентность каждого шага,
/// R6-перечитывание config, фазы в /pgworker/work/<C>.
/// </summary>
public sealed class AddShardProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ISqlExecutor db,
    ShardProbe probe,
    ClaimStore claims,
    WorkJournal journal,
    PlacementOptions placementOpts,
    InstallSecrets secrets,
    EtcdEndpoints etcdEndpoints,
    Func<CancellationToken, Task<Result>>? snapshot = null)
{
    private const string Op = "add-shard";

    // Паттерн имени шарда (t06 §4.1): без дефиса — scope <C>-<X> и имена нод однозначны.
    [GeneratedRegex("^[a-z][a-z0-9_]{0,30}$")]
    private static partial Regex ShardNamePattern();

    // Время первого наблюдения «scope без живого Patroni» (бюджет PatroniBootSec).
    private readonly ConcurrentDictionary<string, long> _patroniWaitSince = new();

    public async Task<Result<ProcessOutcome>> TickAsync(ClusterSnapshot snap, string shardName, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Мутации — только держателем живого клэйма (инвариант §4.3).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"add-shard {cluster}/{shardName}: клэйм не наш (или потерян) — мутации запрещены"));

        var shard = snap.Shards.FirstOrDefault(s => s.Name == shardName);
        if (shard is null)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done); // шарда уже нет

        // A0: journal-before-manipulations (P7).
        var started = await journal.WritePhaseAsync(cluster, Op, "started", claims.InstanceId, null, ct);
        if (!started.IsSuccess)
            return Result<ProcessOutcome>.Failed(started.Error!);

        // A1: guard'ы add (§4.4). R6: свежий config — смена state прекращает add.
        if (await ClusterStateChangedAsync(cluster, ct))
            return await Finish(cluster, "aborted", ProcessOutcome.InProgress, ct);
        if (shard.Dsn is not null)
            return await Finish(cluster, "done", ProcessOutcome.Done, ct); // уже поднят
        if (shard.ToRemove)
            return await Finish(cluster, "blocked-removing", ProcessOutcome.InProgress, ct);
        if (!ShardNamePattern().IsMatch(shardName))
            return await FailAsync(cluster,
                new ApplicationException(
                    $"имя шарда '{shardName}' неканоническое (^[a-z][a-z0-9_]{{0,30}}$) — разбор оператором (etcdctl)"),
                "invalid-name", ct);
        var scope = $"{cluster}-{shardName}";
        var taken = await GetAsync($"/service/{scope}/initialize", ct);
        if (!taken.IsSuccess)
            return await FailAsync(cluster, taken.Error!, "scope-check", ct);
        if (taken.Value is not null)
            return await FailAsync(cluster,
                new ApplicationException(
                    $"scope {scope} занят живым Patroni-кластером (initialize существует) — коллизия имён, разбор оператором"),
                "scope-taken", ct);
        if (!IsFullyDeclared(shard))
            return await Finish(cluster, "waiting-keys", ProcessOutcome.InProgress, ct);

        // A2: план placement (только ноды нового шарда; UsedSlots хостов уже
        // учитывает контейнеры живых шардов — драйвер) + порт-аллокация;
        // merge в существующий /pgworker/portalloc/<C> (read-modify-write под клэймом).
        var planned = await PlanShardPortsAsync(cluster, shard, ct);
        if (!planned.IsSuccess)
            return await FailAsync(cluster, planned.Error!, "planning", ct);
        var topology = Topology(cluster, shardName, planned.Value);

        // A3: EnsureNode каждой ноды + state=PROVISIONING (идемпотентно).
        var resources = await ReadShardResourcesAsync(cluster, shardName, ct);
        var ensured = await EnsureNodesAsync(cluster, shard, topology, resources, ct);
        if (!ensured.IsSuccess)
            return await FailAsync(cluster, ensured.Error!, "ensure-nodes", ct);

        // R6 перед ожиданиями/SQL.
        if (await ClusterStateChangedAsync(cluster, ct))
            return await Finish(cluster, "aborted", ProcessOutcome.InProgress, ct);

        // A4: ждать Patroni (scope initialize+leader + REST всех нод) → RUNNING.
        var booted = await WaitPatroniAsync(cluster, shard, topology, ct);
        if (!booted.IsSuccess)
            return await FailAsync(cluster, booted.Error!, "waiting-patroni", ct);
        if (!booted.Value)
            return await Finish(cluster, "waiting-patroni", ProcessOutcome.InProgress, ct);

        var master = await ResolveMasterAsync(shard, topology, ct);
        if (master is null)
            return await Finish(cluster, "waiting-master", ProcessOutcome.InProgress, ct);

        // A5: БД/роли на мастере НОВОГО шарда; СХЕМЫ БАКЕТОВ НЕ СОЗДАЮТСЯ (§2.1);
        // dsn multi-host (порты portalloc, без пароля).
        var sqlDone = await ProvisionShardSqlAsync(snap, shard, topology, master, ct);
        if (!sqlDone.IsSuccess)
            return await FailAsync(cluster, sqlDone.Error!, "sql", ct);

        // A6: снапшот P12 (точка изменения) + journal done — шард в надзоре.
        if (snapshot is not null)
        {
            var shot = await snapshot(ct);
            if (!shot.IsSuccess)
                return await FailAsync(cluster, shot.Error!, "snapshot", ct);
        }

        return await Finish(cluster, "done", ProcessOutcome.Done, ct);
    }

    // A1: полное объявление шарда (панель доустанила ключи? ждём — waiting-keys).
    private static bool IsFullyDeclared(ShardSpec shard) =>
        shard.Replicas > 0
        && shard.Nodes.Count == shard.Replicas
        && shard.Nodes.All(n => n.State is NodeState.NotInitialized or NodeState.Provisioning);

    // R6: перечитывание config — NOT_INITIALIZED/TO_REMOVE безопасно прекращает add
    // (провиженининг поднимет декларацию как обычный шард / deprovisioning снесёт).
    private async Task<bool> ClusterStateChangedAsync(string cluster, CancellationToken ct)
    {
        var config = await GetAsync($"/clusters/{cluster}/config", ct);
        if (!config.IsSuccess || config.Value is not { } kv)
            return false;
        return kv.Value.Contains("\"NOT_INITIALIZED\"") || kv.Value.Contains("\"TO_REMOVE\"");
    }
}
```

Приватные методы (по образцу ProvisioningProcess, scoped до одного шарда):

- `PlanShardPortsAsync(cluster, shard, ct)` — прочитать `/pgworker/portalloc/<C>` (`ReadPortAllocAsync`); `wanted` = ключи `$"{shard.Name}/{n.Name}"`; если все закреплены — вернуть словарь; иначе `driver.GetHostsAsync` + `GetBusyPortsAsync` → `PlacementPlanner.Plan([shard], hosts)` (список из ОДНОГО шарда — анти-аффинити внутри нового шарда; занятость живыми шардами уже в `UsedSlots`) → `PortAllocator.Allocate(plan, existing, busy, placementOpts.PortFrom, placementOpts.PortTo)` → merge → `PutAsync(portallocKey, Portalloc.Serialize(existing))` — просто put, БЕЗ compare version==0 (read-modify-write под клэймом, Д10) → `journal.WritePhaseAsync(cluster, Op, "planned", …)` → вернуть словарь адресов. Частный случай §5.2 «порт-диапазон исчерпан»: провал `Allocate` (текст PortAllocator — «нет свободной тройки портов…») НЕ прокидывать как есть — вернуть `Result.Failed(new ApplicationException($"порт-диапазон исчерпан — расширьте PortRange (PgWorker:Docker:PortRange): {allocated.Error!.Message}"))` — last_error с подсказкой оператору; фаза `planning` (FailAsync в TickAsync), декларация остаётся, ретраи тиками.
- `Topology(cluster, shard, addresses)` — та же, что `ProvisioningProcess.Topology` (фильтр ключей с префиксом `<X>/`).
- `ReadShardResourcesAsync(cluster, shard, ct)` — копия `ProvisioningProcess.ReadShardResourcesAsync` (чтение `/service/<C>-<X>/request_{cpu,mem}` → `NodeResourcesParser.Parse`; request_disk docker-лимита не имеет — игнор).
- `EnsureNodesAsync(cluster, shard, topology, resources, ct)` — копия `ProvisioningProcess.EnsureNodesAsync` (state=PROVISIONING → `driver.EnsureNodeAsync`).
- `WaitPatroniAsync(cluster, shard, topology, ct)` — копия `ProvisioningProcess.WaitPatroniAsync` с бюджетом `_patroniWaitSince[scope]` (PatroniBootSec) и `nodes/<n>/state=RUNNING` в конце.
- `ResolveMasterAsync(shard, topology, ct)` — копия `ProvisioningProcess.ResolveMasterAsync` (master-ключ → fallback Patroni REST).
- `ProvisionShardSqlAsync(snap, shard, topology, master, ct)`:

```csharp
// A5: БД/роли — ТОЛЬКО (идемпотентные гварды); СХЕМЫ БАКЕТОВ НЕ СОЗДАЮТСЯ:
// шард стартует пустым (§2.1) — routing на него не указывает. dsn — multi-host.
private async Task<Result> ProvisionShardSqlAsync(
    ClusterSnapshot snap, ShardSpec shard, ShardTopology topology, NodeAddress master, CancellationToken ct)
{
    var cluster = snap.Config.Cluster;
    var dbname = snap.Config.DbName;

    var adminDsn = DatabaseProvisioner.BuildAdminDsn(master.Host, master.Ports.Pg, "postgres", secrets);
    var ensured = await db.EnsureDatabaseAsync(adminDsn, dbname, ct);
    if (!ensured.IsSuccess)
        return ensured;

    var dbDsn = DatabaseProvisioner.BuildAdminDsn(master.Host, master.Ports.Pg, dbname, secrets);
    foreach (var guard in DatabaseProvisioner.BuildRoleGuardsSql(secrets))
    {
        var probe = await db.ExecuteScalarAsync(dbDsn, guard, ct);
        if (!probe.IsSuccess)
            return probe;
        if (probe.Value is string create)
        {
            var created = await db.ExecuteAsync(dbDsn, create, ct);
            if (!created.IsSuccess)
                return created;
        }
    }

    // НИКАКИХ BuildSchemasSql/routing/status-записей (граница §2.1).

    var nodes = shard.Nodes.OrderBy(n => n.Name, StringComparer.Ordinal).ToList();
    var hosts = string.Join(",", nodes.Select(n => topology.Nodes[n.Name].Host));
    var ports = string.Join(",", nodes.Select(n => topology.Nodes[n.Name].Ports.Pg));
    var dsn = $"host={hosts} port={ports} dbname={dbname} user=bucket_admin";
    if (shard.Dsn == dsn)
        return Result.Success();
    return await PutAsync($"/clusters/{cluster}/shards/{shard.Name}/dsn", dsn, ct);
}
```

- `Finish`/`FailAsync` — копии из ProvisioningProcess (op = `Op`).

- [ ] **Step 4: Тесты зелёные**

Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release --filter "FullyQualifiedName~AddShardProcessTests"`
Expected: PASS — все 8 кейсов.

- [ ] **Step 5: Сборка без warnings + весь unit-набор**

Run: `dotnet build src/PgWorker.slnx -c Release && dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/PgWorker.Provisioning/Processes/AddShardProcess.cs src/tests/PgWorker.UnitTests/Provisioning/AddShardProcessTests.cs
git commit -m "t06: AddShardProcess A0–A6 — подъём пустого шарда в Active-кластере (§5.2)"
```

---

### Task 5: pg — RemoveShardProcess (S0–S4, guard'ы G1–G7)

**Files:**
- Create: `pg/src/PgWorker.Provisioning/Processes/RemoveShardProcess.cs`
- Test: `pg/src/tests/PgWorker.UnitTests/Provisioning/RemoveShardProcessTests.cs`

**Interfaces:**
- Consumes: `IClusterDriver.RemoveNodeAsync/ListNodeObjectsAsync`, `ClaimStore`, `WorkJournal`, `Portalloc.Parse/Serialize`, `BucketRoute.Owner/Status/MoveSource/MoveTarget` (Task 2).
- Produces: `RemoveShardProcess.TickAsync(ClusterSnapshot snap, string shardName, CancellationToken ct) → Task<Result<ProcessOutcome>>`; op-метка `"remove-shard"`; фазы `started/blocked-<guard>/removing-nodes/cleaning-keys/aborted/done`. Использует Task 6.

- [ ] **Step 1: Написать failing-тесты**

`pg/src/tests/PgWorker.UnitTests/Provisioning/RemoveShardProcessTests.cs`. Rig по образцу DeprovisioningProcessTests (FakeEtcd/FakeDriver/ClaimStore/Journal; сид Active-кластера shop, 2 шарда, routing). Обязательные кейсы:

```csharp
// Базовый сид: Active-кластер shop (config без state), shard1 (2 ноды RUNNING,
// dsn) + shard2 (dsn), routing все бакеты → shard2 (shard1 ПУСТ),
// portalloc с записями обоих шардов, /pgworker/evacuations/shop/shard1 (журнал),
// контейнеры pgw-shop-shard1-* в Driver.NodeObjects, маркер
// /clusters/shop/shards/shard1/state=TO_REMOVE.

[Theory]
[InlineData("G2")] [InlineData("G3")] [InlineData("G5")]
[InlineData("G6")] [InlineData("G7")]
public async Task Tick_GuardBlocked_MarkerStays_NoDockerMutations(string guardId)
// Arrange: базовый сид + мутация под конкретный guard:
//   G2: удалить replicas/nodes-ключи shard1 (шард не заявлен)
//   G3: routing/bucket_0 → shard1 (бакет на шарде)
//   G5: /pgworker/moves/shop/bucket_0 = {"op":"move","to":"shard1",...}
//   G6: nodes/shard1a/state=QUARANTINED
//   G7: удалить shard2 из сида (shard1 — единственный шард)
// Act: TickAsync(snap, "shard1")
// Assert: InProgress (не Failed); journal op=remove-shard, last_error
//   содержит человекочитаемую причину (тексты §4.4); маркер жив в Store;
//   Driver.RemovedNodes пуст; ключи шарда живы

[Theory]
[InlineData("owner")] [InlineData("target")]
public async Task Tick_G4_StatusKeyReferencesShard_Blocked(string side)
// Arrange — ОБА плеча G4 (§4.4 «owner ИЛИ target»):
//   owner: routing/bucket_0 → shard2 (flip прошёл!), статус-ключ жив
//          {"state":"FROZEN","owner":"shard1","target":"shard2",...} —
//          статус-owner = shard1 при routing-owner = shard2 (P7 «статус завис»)
//   target: routing/bucket_0 → shard2, статус {"state":"SYNCING",
//          "owner":"shard1","target":"shard1",...} — статус-target = shard1
//   (routing НЕ указывает на shard1 — G3 проходит; блокирует именно G4)
// Act: TickAsync(snap, "shard1")
// Assert: InProgress; phase=blocked-G4; last_error «незавершённый переезд
//   бакета — завершите/отмените»; маркер жив; docker-мутаций нет

[Fact]
public async Task Tick_G1_ClusterToRemoveMidFlight_Aborted()
// Arrange: базовый сид + config перезаписан с "state":"TO_REMOVE"
// Act / Assert: InProgress; phase=aborted; docker-мутаций нет (кластер снесёт deprovisioning)

[Fact]
public async Task Tick_HappyPath_RemovesDockerThenEtcdKeys()
// Act: TickAsync(snap, "shard1")
// Assert: Done; Driver.RemovedNodes == ["shard1/shard1a","shard1/shard1b"];
//   nodes state были REMOVING перед удалением (проверить через Store-трейс
//   или порядок FakeEtcd.OnPut); в Store НЕТ ключей с префиксом
//   /clusters/shop/shards/shard1/ и /service/shop-shard1/;
//   portalloc-JSON не содержит "shard1/" но СОДЕРЖИТ "shard2/";
//   /pgworker/evacuations/shop/shard1 удалён; shard2-ключи не тронуты

[Fact]
public async Task Tick_HappyPath_RemovesShardScopedOrphans()
// Arrange: базовый сид + Driver.NodeObjects содержит сироту pgw-shop-shard1-x
// Act / Assert: Done; RemovedNodes содержит "shard1/x"

[Fact]
public async Task Tick_DockerObjectAlive_RepeatsNextTick()
// Arrange: Driver.RemoveFailsOnce=true (первый RemoveNode падает)
// Act: первый тик → Fail; второй тик
// Assert: первый — не успех, маркер жив; второй — Done (идемпотентность)

[Fact]
public async Task Tick_AlreadyRemoved_Done()
// Arrange: ключей шарда нет (после чистки)
// Act / Assert: Done без мутаций

[Fact]
public async Task Tick_MarkedUndeclaredShard_DismantlesDeclaration()
// Arrange: add-декларация shard3 (nodes NOT_INITIALIZED, БЕЗ dsn) + маркер
// Act / Assert: Done (guard'ы проходят: бакетов нет — Д5, способ отменить зависший add);
//   декларация вычищена, контейнеров не было (RemoveNode 404=ок у драйвера)
```

- [ ] **Step 2: Прогнать — отказ**

Run: `dotnet build src/PgWorker.slnx -c Release`
Expected: FAIL — `RemoveShardProcess` не существует.

- [ ] **Step 3: Реализовать RemoveShardProcess**

`pg/src/PgWorker.Provisioning/Processes/RemoveShardProcess.cs` (конструктор: `IEtcdGateway etcd, string[] endpoints, IClusterDriver driver, ClaimStore claims, WorkJournal journal, Func<CancellationToken, Task<Result>>? snapshot = null`; failover-обёртки `GetAsync/PutAsync/DeleteAsync/RangeAsync/WithFailoverAsync` — копия из `DeprovisioningProcess.cs:179-219`):

```csharp
using System.Text.Json;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Docker.Drivers;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;

namespace PgWorker.Provisioning.Processes;

/// <summary>
/// RemoveShardProcess — демонтаж ОТДЕЛЬНОГО шарда Active-кластера по маркеру
/// shards/<X>/state=TO_REMOVE (t06 spec §5.3; arch/14 §5 H; эталон remove-shard.sh
/// + DeprovisioningProcess scoped-to-shard). Guard'ы G1–G7 в S1 перед любым
/// разрушающим действием; порядок «сначала docker, потом etcd» — мёртвые ключи
/// при сбое безвредны (маркер стоит, повторный тик продолжает). Кластер живёт.
/// </summary>
public sealed class RemoveShardProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    Func<CancellationToken, Task<Result>>? snapshot = null)
{
    private const string Op = "remove-shard";

    public async Task<Result<ProcessOutcome>> TickAsync(ClusterSnapshot snap, string shardName, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Мутации — только держателем живого клэйма (инвариант §4.3).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"remove-shard {cluster}/{shardName}: клэйм не наш (или потерян) — мутации запрещены"));

        var shard = snap.Shards.FirstOrDefault(s => s.Name == shardName);
        if (shard is null)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done); // уже демонтирован

        // S0: journal-before-manipulations (P7).
        var started = await journal.WritePhaseAsync(cluster, Op, "started", claims.InstanceId, null, ct);
        if (!started.IsSuccess)
            return Result<ProcessOutcome>.Failed(started.Error!);

        // G1 (R6): свежее чтение config ДО guard'ов — NOT_INITIALIZED/TO_REMOVE
        // безопасно прекращает демонтаж шарда (provisioning поднимет declared-шард
        // как обычный / deprovisioning кластера снесёт всё сам).
        if (await ClusterStateChangedAsync(cluster, ct))
            return await Finish(cluster, "aborted", ProcessOutcome.InProgress, ct);

        // S1: guard'ы G2–G7 (§4.4) — над снапшотом тика; провал = last_error
        // с причиной + InProgress (маркер-состояние живёт, повтор тиком).
        var blocked = CheckGuards(snap, shard);
        if (blocked is { } guardId)
        {
            await journal.WritePhaseAsync(cluster, Op, $"blocked-{guardId}", claims.InstanceId,
                GuardReason(snap, shard, guardId), ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.InProgress);
        }

        // G5 (заявки /pgworker/moves/<C>/ с to/old_shard == X) — единственный
        // guard с чтением вне снапшота; саморазрешающийся: MoveProcess отклонит
        // заявку перманентно (§5.5) и удалит её — следующий тик пройдёт guard.
        var movesRef = await MovesReferenceShardAsync(cluster, shardName, ct);
        if (!movesRef.IsSuccess)
            return await FailAsync(cluster, movesRef.Error!, "guards", ct);
        if (movesRef.Value)
        {
            await journal.WritePhaseAsync(cluster, Op, "blocked-G5", claims.InstanceId,
                "есть заявки переездов, ссылающиеся на шард — дождитесь их разбора", ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.InProgress);
        }

        // S2: REMOVING → RemoveNode каждой ноды (404 = ок) + сироты шарда.
        var removed = await RemoveNodesAsync(cluster, shard, ct);
        if (!removed.IsSuccess)
            return await FailAsync(cluster, removed.Error!, "removing-nodes", ct);

        // S3: guard docker-объектов нет → чистка etcd.
        var objects = await driver.ListNodeObjectsAsync(cluster, ct);
        if (!objects.IsSuccess)
            return await FailAsync(cluster, objects.Error!, "listing-objects", ct);
        if (objects.Value.Any(name => name.StartsWith($"pgw-{cluster}-{shardName}-", StringComparison.Ordinal)))
            return await Finish(cluster, "removing-nodes", ProcessOutcome.InProgress, ct);

        var cleaned = await CleanKeysAsync(cluster, shardName, ct);
        if (!cleaned.IsSuccess)
            return await FailAsync(cluster, cleaned.Error!, "cleaning-keys", ct);

        // S4: снапшот P12 (точка изменения) + done. Кластер продолжает жить.
        if (snapshot is not null)
        {
            var shot = await snapshot(ct);
            if (!shot.IsSuccess)
                return await FailAsync(cluster, shot.Error!, "snapshot", ct);
        }

        return await Finish(cluster, "done", ProcessOutcome.Done, ct);
    }
}
```

Guard'ы G2–G4/G6/G7 — чистые функции над снапшотом (unit-тестируемые отдельно): `static string? CheckGuards(ClusterSnapshot snap, ShardSpec shard)` → `null` = прошли, иначе id `"G2"`..`"G7"`; `static string GuardReason(ClusterSnapshot snap, ShardSpec shard, string guardId)` — человекочитаемые причины дословно из spec §4.4:

```csharp
// G2: shard.Replicas <= 0 && shard.Nodes.Count == 0 →
//     «шард не заявлен — нечего демонтировать»
// G3: snap.Routing.Any(r => r.Owner == shard.Name) →
//     $"на шарде {count} бакетов (routing) — сначала явно перевезите их (заявки /pgworker/moves/, UI переездов — t07)"
// G4: snap.Routing.Any(r => r.Status is not null
//         && (r.MoveSource == shard.Name || r.MoveTarget == shard.Name)) →
//     «незавершённый переезд бакета — завершите/отмените»
//     (owner И target СТАТУС-ключа — §4.4; после flip routing-owner уже новый
//     шард, а зависший статус ещё держит старого в MoveSource — P7; routing-owner
//     отдельно покрыт G3 и в G4 не дублируется)
// G6: shard.Nodes.Any(n => n.State == NodeState.Quarantined) →
//     «шард в карантине после эвакуации — сначала разбор данных (t05 runbook)»
//     (случай «эвакуирован и НЕ вернулся» — ноды UNREACHABLE — демонтаж разрешён, Д7)
// G7: snap.Shards.Count <= 1 →
//     «нельзя снять последний шард — для полного демонтажа удалите кластер»
```

`MovesReferenceShardAsync(cluster, shardName, ct) → Result<bool>`: `RangeAsync($"/pgworker/moves/{cluster}/")` → для каждого значения `JsonDocument.Parse` → поля `"to"`/`"old_shard"` (чтение как в `WorkJournal`-парсинге: `root.TryGetProperty`) равны shardName → true.

`RemoveNodesAsync(cluster, shard, ct)`: для каждой ноды — `state != Removing` → `PutAsync(nodeStateKey, "REMOVING")`; `driver.RemoveNodeAsync(cluster, shard.Name, node.Name, ct)`. Затем сироты: `driver.ListNodeObjectsAsync(cluster)` → имена с префиксом `pgw-<C>-<X>-` не из known → разбор `tail = name[("pgw-"+cluster+"-").Length..].Split('-')` (по образцу `DeprovisioningProcess.RemoveNodesAsync:104-125`) → `RemoveNodeAsync(cluster, shardName, tail[^1])`.

`ClusterStateChangedAsync(cluster, ct)` — как `ProvisioningProcess.IsRemovedAsync` + `Contains("\"NOT_INITIALIZED\"")` (оба значения → true).

`CleanKeysAsync(cluster, shardName, ct)`:

```csharp
// S3: чистка etcd — всё про шард; остальные шарды не затронуты.
private async Task<Result> CleanKeysAsync(string cluster, string shardName, CancellationToken ct)
{
    var scope = $"{cluster}-{shardName}";

    // Префикс шарда целиком (state/replicas/nodes/dsn/master — всё).
    var delShard = await DeleteAsync($"/clusters/{cluster}/shards/{shardName}/", prefix: true, ct);
    if (!delShard.IsSuccess)
        return delShard;

    // Точечные заявки ресурсов (даже если scope ещё жив) + префикс scope.
    foreach (var request in (string[])["request_cpu", "request_mem", "request_disk"])
    {
        var del = await DeleteAsync($"/service/{scope}/{request}", prefix: false, ct);
        if (!del.IsSuccess)
            return del;
    }

    var delScope = await DeleteAsync($"/service/{scope}/", prefix: true, ct);
    if (!delScope.IsSuccess)
        return delScope;

    // portalloc: точечная фильтрация записей "<X>/<n>" из JSON (Д10 — ключ общий
    // на кластер, read-modify-write под клэймом безопасен).
    var ports = await GetAsync($"/pgworker/portalloc/{cluster}", ct);
    if (ports is { IsSuccess: true, Value: not null })
    {
        var parsed = Portalloc.Parse(cluster, ports.Value.Value);
        if (parsed.IsSuccess)
        {
            var kept = parsed.Value
                .Where(p => !p.Key.StartsWith($"{shardName}/", StringComparison.Ordinal))
                .ToDictionary(p => p.Key, p => p.Value);
            var put = await PutAsync($"/pgworker/portalloc/{cluster}", Portalloc.Serialize(kept), ct);
            if (!put.IsSuccess)
                return put;
        }
    }

    // Журнал эвакуации не переживает демонтаж шарда.
    return await DeleteAsync($"/pgworker/evacuations/{cluster}/{shardName}", prefix: false, ct);
}
```

`Finish`/`FailAsync` — копии из DeprovisioningProcess/ProvisioningProcess (op = `Op`).

- [ ] **Step 4: Тесты зелёные**

Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release --filter "FullyQualifiedName~RemoveShardProcessTests"`
Expected: PASS — таблица guard'ов + счастливые пути.

- [ ] **Step 5: Сборка + весь unit**

Run: `dotnet build src/PgWorker.slnx -c Release && dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/PgWorker.Provisioning/Processes/RemoveShardProcess.cs src/tests/PgWorker.UnitTests/Provisioning/RemoveShardProcessTests.cs
git commit -m "t06: RemoveShardProcess S0–S4 — демонтаж шарда с guard'ами G1–G7 (§5.3)"
```

---

### Task 6: pg — интеграция scale-прохода: IClusterProcesses, ReconcileLoop, DI

**Files:**
- Modify: `pg/src/PgWorker.App/Loops/ClusterProcesses.cs` (интерфейс + реализация)
- Modify: `pg/src/PgWorker.App/Loops/ReconcileLoop.cs:167-181` (default-ветка)
- Modify: `pg/src/PgWorker.App/Program.cs:136-146` (DI: AddShardProcess/RemoveShardProcess, ClusterProcesses)
- Test: `pg/src/tests/PgWorker.UnitTests/App/ReconcileLoopTests.cs` (FakeProcesses += ScaleShards; новый тест порядка)

**Interfaces:**
- Consumes: `AddShardProcess.TickAsync`, `RemoveShardProcess.TickAsync` (Tasks 4–5), `ShardScaleClassifier.Detect` (Task 3).
- Produces: `IClusterProcesses.ScaleShardsAsync(ClusterSnapshot, CancellationToken) → Task<Result<ProcessOutcome>>` — remove-проход (первый кандидат) → add-проход (первый кандидат), по одному шард-за-тик.

- [ ] **Step 1: Написать failing-тесты**

В `ReconcileLoopTests`:

1) `FakeProcesses` (мок `IClusterProcesses` в тестах цикла — найти по месту определения в `src/tests/PgWorker.UnitTests/App/`) — добавить поле `public readonly List<string> Scaled = [];`, при необходимости общий `public readonly List<string> Calls = [];` (дописывать имя операции в каждом методе — для проверки порядка), и реализацию нового метода интерфейса `ScaleShardsAsync` → записывает `"shop"` в `Scaled` (и `"scale-shards"` в `Calls`), возвращает `Success(ProcessOutcome.Done)`. Добавить в существующие ассерты `processes.Scaled.Should().BeEmpty()` для Provision/Deprovision-кейсов.
2) Новый тест:

```csharp
[Fact]
public async Task Tick_ActiveCluster_ScaleShardsAfterSuperviseBeforeMoves()
{
    // Arrange — Active-кластер; надзор → scale-проход → moves (порядок §5.1)
    SeedCluster("shop", null);
    var processes = new FakeProcesses();
    var loop = CreateLoop(processes);

    // Act
    var tick = await loop.TickAsync(TestContext.Current.CancellationToken);

    // Assert — scale-проход вызван ровно один раз; порядок проверяет Calls-трейс
    // (FakeProcesses записывает порядок: supervise → scale-shards → moves)
    tick.IsSuccess.Should().BeTrue();
    processes.Scaled.Should().Equal("shop");
    // порядок: индекс "scale-shards" между "supervise" и "moves" в processes.Calls
}
```

Если `FakeProcesses` не ведёт общий `Calls`-трейс — добавить `public readonly List<string> Calls = [];` и дописывать в каждом методе (малая правка мока).

- [ ] **Step 2: Прогнать — отказ компиляции**

Run: `dotnet build src/PgWorker.slnx -c Release`
Expected: FAIL — `IClusterProcesses` не содержит `ScaleShardsAsync`.

- [ ] **Step 3: Реализовать**

`ClusterProcesses.cs` — в интерфейс добавить:

```csharp
/// <summary>Scale-проход Active-ветки (t06, spec §5.1): remove-кандидаты →
/// add-кандидаты, по одному шард-за-тик (Д13: демонтаж освобождает хосты/порты).</summary>
Task<Result<ProcessOutcome>> ScaleShardsAsync(ClusterSnapshot snap, CancellationToken ct);
```

Реализация `ClusterProcesses` — конструктор += `AddShardProcess addShards, RemoveShardProcess removeShards`:

```csharp
public async Task<Result<ProcessOutcome>> ScaleShardsAsync(ClusterSnapshot snap, CancellationToken ct)
{
    var candidates = ShardScaleClassifier.Detect(snap);

    // Remove-проход первым (Д13): помеченные демонтируются, недоднятый add
    // отменяется этим же путём (Д5).
    if (candidates.Remove.Count > 0)
    {
        var removed = await removeShards.TickAsync(snap, candidates.Remove[0], ct);
        if (!removed.IsSuccess)
            return removed;
    }

    // Add-проход: только НЕпомеченные кандидаты. Шард из обоих списков
    // (TO_REMOVE + declared без dsn) уже демонтирован remove-проходом выше —
    // снапшот тика ещё видит его declared-ноды, и без фильтра add поднял бы
    // шард заново; AddShardProcess (A1) также guard'ит ToRemove (blocked-removing).
    var addCandidate = candidates.Add.FirstOrDefault(name => !candidates.Remove.Contains(name));
    if (addCandidate is { } shard)
    {
        var added = await addShards.TickAsync(snap, shard, ct);
        if (!added.IsSuccess)
            return added;
    }

    return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
}
```

`ReconcileLoop.cs` default-ветка — после `RunSuperviseAsync`, ПЕРЕД эвакуациями:

```csharp
default:
    var supervised = await RunSuperviseAsync(cluster, snap, ct);
    if (supervised is null)
        break;

    // Scale-проход (t06 spec §5.1): remove → add, после надзора, до
    // эвакуаций/moves — демонтаж освобождает хосты/порты для подъёма (Д13).
    await RunClusterOpAsync(cluster, "scale-shards",
        () => processes.ScaleShardsAsync(snap, ct), ct);

    // События эвакуации: полностью мёртвые шарды (spec §6.4 D/E).
    foreach (var deadShard in supervised.DeadShards)
        await RunClusterOpAsync(cluster, $"evacuate/{deadShard}",
            () => processes.EvacuateAsync(snap, deadShard, ct), ct);

    // Существующий блок moves — без правок (заявки переездов после эвакуаций).
```

`Program.cs` — после регистрации BucketEvacuator добавить (зеркало ProvisioningProcess-регистрации):

```csharp
// Scale-процессы шардов (t06): подъём/демонтаж отдельного шарда Active-кластера.
builder.Services.AddSingleton(sp =>
{
    var opts = sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value;
    return new AddShardProcess(
        sp.GetRequiredService<IEtcdGateway>(), opts.Etcd.Endpoints,
        sp.GetRequiredService<IClusterDriver>(), sp.GetRequiredService<ISqlExecutor>(),
        sp.GetRequiredService<ShardProbe>(), sp.GetRequiredService<ClaimStore>(),
        sp.GetRequiredService<WorkJournal>(),
        new PlacementOptions(opts.Docker.PortRange.From, opts.Docker.PortRange.To, opts.Thresholds.PatroniBootSec),
        sp.GetRequiredService<InstallSecrets>(),
        sp.GetRequiredService<EtcdEndpoints>(),
        SnapshotDelegate(sp.GetRequiredService<SnapshotJob>()));
});
builder.Services.AddSingleton(sp => new RemoveShardProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    SnapshotDelegate(sp.GetRequiredService<SnapshotJob>())));
```

И `ClusterProcesses`-регистрация не меняется (конструктор резолвится DI сам).

- [ ] **Step 4: Тесты + сборка**

Run: `dotnet build src/PgWorker.slnx -c Release && dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release`
Expected: PASS (включая новый порядок-тест и все прежние).

- [ ] **Step 5: Commit**

```bash
git add src/PgWorker.App/Loops/ClusterProcesses.cs src/PgWorker.App/Loops/ReconcileLoop.cs src/PgWorker.App/Program.cs src/tests/PgWorker.UnitTests/App/ReconcileLoopTests.cs
git commit -m "t06: scale-проход ReconcileLoop — ScaleShardsAsync (remove→add) + DI процессов"
```

---

### Task 7: pg — NodeSupervisor: границы надзора

**Files:**
- Modify: `pg/src/PgWorker.Provisioning/Processes/NodeSupervisor.cs:120-172` (EnsureDeclaredNodesAsync), `:73-83` (DeadShards)
- Test: `pg/src/tests/PgWorker.UnitTests/Provisioning/NodeSupervisorTests.cs` (новые кейсы)

**Interfaces:**
- Consumes: `ShardSpec.ToRemove/Dsn` (Task 2).
- Produces: поведение — самовосстановление пропускает шарды без dsn и с TO_REMOVE; DeadShards требует dsn + ≥1 бакета по routing (TO_REMOVE с бакетами — кандидат, Д6).

- [ ] **Step 1: Написать failing-тесты**

В `NodeSupervisorTests` (rig по существующим образцам — сид Active-кластера, portalloc, Patroni-пробы глухие чтобы ноды уходили в unreachable-трек; для DeadShards-кейсов время прокрутить через пороги/FakeTimeProvider по образцу существующих тестов эвакуации):

```csharp
[Fact]
public async Task Tick_ShardWithoutDsn_DockerRm_NotRecreated()
// Arrange: шард shard3 declared (nodes), dsn НЕТ; контейнер pgw-shop-shard3-shard3a
//   отсутствует в NodeObjects (снесён руками)
// Act / Assert: Driver.EnsuredNodes НЕ содержит shard3/* (домен AddShardProcess)

[Fact]
public async Task Tick_MarkedShard_DockerRm_NotRecreated()
// Arrange: шард с dsn + state=TO_REMOVE; контейнер снесён
// Act / Assert: EnsuredNodes пуст для этого шарда (домен RemoveShardProcess)

[Fact]
public async Task Tick_DeadShardWithoutBuckets_NotEvacuationCandidate()
// Arrange: шард мёртв (все ноды в unreachable-треке дольше ShardDeadSec,
//   master-ключа нет), dsn есть, routing НЕ указывает на него (0 бакетов)
// Act / Assert: outcome.Value.DeadShards не содержит шард

[Fact]
public async Task Tick_DeadShardWithoutDsn_NotEvacuationCandidate()
// Arrange: мёртвый declared-шард без dsn, routing указывает (аномалия сида)
// Act / Assert: DeadShards пуст (add ещё идёт — эвакуировать нечего, §5.4)

[Fact]
public async Task Tick_DeadMarkedShardWithBuckets_IsEvacuationCandidate()
// Arrange: мёртвый шард с dsn + маркер TO_REMOVE + ≥1 бакет по routing
// Act / Assert: DeadShards содержит шард (Д6: эвакуация — способ освободить
//   бакеты умирающего помеченного шарда, после чего G3 пропустит демонтаж)
```

- [ ] **Step 2: Прогнать — падают новые кейсы**

Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release --filter "FullyQualifiedName~NodeSupervisorTests"`
Expected: FAIL новых кейсов (сейчас supervisor пересоздаёт всё declared и эвакуирует без условий).

- [ ] **Step 3: Реализовать**

В `EnsureDeclaredNodesAsync` — первой строкой цикла по шардам:

```csharp
foreach (var shard in snap.Shards)
{
    // Границы надзора (t06 §5.4): шард без dsn — домен AddShardProcess;
    // TO_REMOVE — домен RemoveShardProcess (не пересоздавать демонтируемое).
    if (shard.Dsn is null || shard.ToRemove)
        continue;
    …
```

В детекте DeadShards (блок `allDead && string.IsNullOrWhiteSpace(shard.Master)`) — добавить условия:

```csharp
// Кандидат эвакуации (t06 §5.4): только зарегистрированный шард (dsn есть —
// add завершён) И с бакетами по routing (эвакуация пустого шарда бессмысленна
// и карантинила бы ноды, блокируя демонтаж по G6). Шард с TO_REMOVE-маркером
// кандидатом БЫТЬ МОЖЕТ — эвакуация освобождает бакеты умирающего помеченного
// шарда, после чего G3 пропустит демонтаж (Д6).
var hasBuckets = snap.Routing.Any(r => r.Owner == shard.Name);
if (allDead && string.IsNullOrWhiteSpace(shard.Master) && shard.Dsn is not null && hasBuckets)
{
    // Тело условия (порог oldest/ShardDeadSec → deadShards.Add) — без правок.
}
```

- [ ] **Step 4: Тесты + сборка**

Run: `dotnet build src/PgWorker.slnx -c Release && dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release`
Expected: PASS (новые + все существующие сценарии надзора).

- [ ] **Step 5: Commit**

```bash
git add src/PgWorker.Provisioning/Processes/NodeSupervisor.cs src/tests/PgWorker.UnitTests/Provisioning/NodeSupervisorTests.cs
git commit -m "t06: NodeSupervisor — границы надзора: skip без dsn/TO_REMOVE; эвакуация требует dsn+бакеты (§5.4)"
```

---

### Task 8: pg — MoveProcess: новые перманентные отказы M0

**Files:**
- Modify: `pg/src/PgWorker.Moves/Process/MoveProcess.cs:132-137` (move-валидации цели), `:~758` (finalize old_shard)
- Test: `pg/src/tests/PgWorker.UnitTests/Moves/MoveProcessPreflightTests.cs` (новые кейсы)

**Interfaces:**
- Consumes: `ShardSpec.ToRemove/Dsn` (Task 2); `RejectAsync` (перманентный отказ + del заявки + last_error).
- Produces: три новых перманентных отказа с подсказками (spec §5.5).

- [ ] **Step 1: Написать failing-тесты**

В `MoveProcessPreflightTests` (rig по образцу существующих — FakeEtcd/FakeSql, сид заявки `{"op":"move","to":"shardX",…}`):

```csharp
[Fact]
public async Task Move_ToMarkedShard_RejectedPermanently()
// Arrange: кластер Active, владелец bucket_0=shard1 (dsn), цель shard2:
//   dsn есть + /clusters/shop/shards/shard2/state=TO_REMOVE; заявка записана
// Act: TickAsync
// Assert: Reject — заявка удалена из Store; work.last_error содержит
//   "помечен к удалению"; SQL-мутаций нет

[Fact]
public async Task Move_ToShardWithoutDsn_RejectedWithAddHint()
// Arrange: цель shard2 declared (nodes есть), dsn НЕТ, маркера нет
// Act / Assert: заявка удалена; last_error содержит "ещё не поднят (add-shard не завершён)"

[Fact]
public async Task Finalize_OldShardRemoved_RejectedNothingToClean()
// Arrange: заявка {"op":"finalize","old_shard":"shard2",…}; shard2 БЕЗ dsn-ключа
// Act / Assert: заявка удалена; last_error содержит "удалён — убирать нечего"
```

- [ ] **Step 2: Прогнать — FAIL (текущие тексты другие/нет проверок)**

Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release --filter "FullyQualifiedName~MoveProcessPreflightTests"`
Expected: FAIL трёх новых кейсов.

- [ ] **Step 3: Реализовать**

В `RunMoveAsync` заменить блок валидации цели (строки ~132–137):

```csharp
var srcShard = snap.Shards.FirstOrDefault(s => s.Name == owner);
var dstShard = snap.Shards.FirstOrDefault(s => s.Name == to);
// t06 §5.5: помеченный к удалению шард — не цель переезда (его демонтаж
// блокировал бы G3 до разбора заявки; переезды ИЗ него разрешены).
if (dstShard?.ToRemove == true)
    return await RejectAsync(cluster, bucket,
        $"шард-приёмник '{to}' помечен к удалению — выберите другую цель", ct);
if (dstShard?.Dsn is null)
    return await RejectAsync(cluster, bucket,
        $"шард-приёмник '{to}' ещё не поднят (add-shard не завершён)", ct);
if (srcShard?.Dsn is null)
    return await RejectAsync(cluster, bucket,
        $"шард-источник '{owner}' не зарегистрирован (нет dsn-ключа)", ct);
```

В `RunFinalizeAsync` (валидация `old_shard`, строка ~758) — уточнить текст отказа:

```csharp
// t06 §5.5: старый шард демонтирован (артефакты исчезли вместе с volume) —
// убирать нечего; отказ перманентный с подсказкой.
return await RejectAsync(cluster, bucket,
    $"шард '{old}' удалён — убирать нечего (артефакты исчезли вместе с volume)", ct, op);
```

- [ ] **Step 4: Тесты + сборка**

Run: `dotnet build src/PgWorker.slnx -c Release && dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release`
Expected: PASS (новые + прежние preflight-кейсы; существующий тест текста «не зарегистрирован» для ИСТОЧНИКА остаётся зелёным).

- [ ] **Step 5: Commit**

```bash
git add src/PgWorker.Moves/Process/MoveProcess.cs src/tests/PgWorker.UnitTests/Moves/MoveProcessPreflightTests.cs
git commit -m "t06: MoveProcess M0 — отказы: цель TO_REMOVE / цель без dsn / finalize удалённого шарда (§5.5)"
```

---

### Task 9: pg — DeprovisioningProcess: чистка журналов эвакуаций (мини-фикс D2)

**Files:**
- Modify: `pg/src/PgWorker.Provisioning/Processes/DeprovisioningProcess.cs:154-163` (CleanKeysAsync)
- Test: `pg/src/tests/PgWorker.UnitTests/Provisioning/DeprovisioningProcessTests.cs` (расширить)

**Interfaces:**
- Consumes: существующий `CleanKeysAsync`.
- Produces: D2 дополнительно удаляет `/pgworker/evacuations/<C>/` (spec §5.6, Д12).

- [ ] **Step 1: Failing-тест**

В `DeprovisioningProcessTests`:

```csharp
[Fact]
public async Task Tick_CleanKeys_RemovesEvacuationJournals()
{
    // Arrange — сид кластера TO_REMOVE + журнал эвакуации, переживший удаление
    // (Д12: пробел, найден при проектировании S3). Базовый сид — повторить
    // из существующего happy-path теста DeprovisioningProcessTests (кластер
    // TO_REMOVE, ноды в NodeObjects), добавив одну строку сида:
    etcd.Seed("/pgworker/evacuations/shop/shard1",
        """{"buckets":{"0":"shard2"},"reason":"shard-dead","evacuated_unix":1,"state":"DONE","returned_unix":null}""");

    // Act
    var outcome = await process.TickAsync(await Snapshot(etcd), CancellationToken.None);

    // Assert — журналы эвакуаций не переживают удаление кластера
    outcome.Value.Should().Be(ProcessOutcome.Done);
    etcd.Store.Keys.Should().NotContain(k => k.StartsWith("/pgworker/evacuations/shop/", StringComparison.Ordinal));
}
```

- [ ] **Step 2: Прогнать — FAIL**

Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release --filter "FullyQualifiedName~DeprovisioningProcessTests"`
Expected: FAIL нового кейса.

- [ ] **Step 3: Реализовать**

В `CleanKeysAsync` после `delWork` (перед return del moves):

```csharp
// Журналы эвакуаций не переживают удаление кластера (t06 §5.6, симметрия с S3).
var delEvacuations = await DeleteAsync($"/pgworker/evacuations/{cluster}/", prefix: true, ct);
if (!delEvacuations.IsSuccess)
    return delEvacuations;
```

- [ ] **Step 4: Тесты + сборка**

Run: `dotnet build src/PgWorker.slnx -c Release && dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/PgWorker.Provisioning/Processes/DeprovisioningProcess.cs src/tests/PgWorker.UnitTests/Provisioning/DeprovisioningProcessTests.cs
git commit -m "t06: Deprovisioning D2 — чистка /pgworker/evacuations/<C>/ (§5.6, Д12)"
```

---

### Task 10: pg — integration: контракт scale-ключей на реальном etcd

**Files:**
- Create: `pg/src/tests/PgWorker.IntegrationTests/Etcd/ShardScaleContractTests.cs`
- Create: `pg/src/tests/PgWorker.IntegrationTests/Etcd/StubScaleDriver.cs` (мок docker для процессов)

**Interfaces:**
- Consumes: `EtcdFixture` (коллекция `EtcdCollection`), `ClusterSnapshotParser`, `ShardScaleClassifier`, `AddShardProcess`, `RemoveShardProcess` (Tasks 3–5).
- Produces: контрактные тесты сида панели → детекции → процессов на реальном etcd (spec §8 integration).

- [ ] **Step 1: Написать тесты**

`StubScaleDriver` — реализация `IClusterDriver` в стиле `Fakes.FakeDriver` из unit-тестов (EnsuredNodes/RemovedNodes списки, `NodeObjects` список, Hosts h1/h2, BusyPorts пусто; остальные методы → `Result.Success()`).

`ShardScaleContractTests` (`[Collection(EtcdCollection.Name)]`, по образцу `EtcdContractTests`):

```csharp
[Fact]
public async Task PanelAddDeclaration_RealRange_ParserDetectsAddCandidate()
// Arrange: сид Active-кластера shop (как EtcdContractTests) + add-декларация
//   shard3: replicas=2, nodes shard3a/b NOT_INITIALIZED, /service/shop-shard3/request_*
// Act: Gateway.RangeAsync("/clusters/") → ParseClusters → ShardScaleClassifier.Detect
// Assert: Add == ["shard3"]; Remove пуст; ToRemove==false у shard1/2

[Fact]
public async Task Marker_RealRange_ParserDetectsRemoveCandidate()
// Arrange: сид + PUT /clusters/shop/shards/shard1/state=TO_REMOVE
// Act / Assert: Remove == ["shard1"]; парсер не пишет parseErrors

[Fact]
public async Task RemoveShardProcess_OnRealEtcd_CleansKeysWithRealTxnAndDel()
// Arrange: сид (маркер shard1, routing пуст от shard1, portalloc с записями
//   обоих шардов через реальные PutAsync, StubScaleDriver с объектами
//   pgw-shop-shard1-*); ClaimStore с захваченным клэймом (как в unit)
// Act: await new RemoveShardProcess(Gateway, [Endpoint], driver, claims,
//      new WorkJournal(Gateway, [Endpoint]), snapshot: null).TickAsync(snap, "shard1", ct)
// Assert: Done; реальный RangeAsync не находит /clusters/shop/shards/shard1/ и
//   /service/shop-shard1/; portalloc-ключ (реальное чтение) не содержит "shard1/",
//   содержит "shard2/"

[Fact]
public async Task ConcurrentMarkerPuts_ConvergeToSameValue()
// Arrange: два параллельных PutAsync state=TO_REMOVE (Task.WhenAll)
// Act / Assert: оба успеха; значение ровно "TO_REMOVE" (идемпотентность PUT §4.2)

[Fact]
public async Task AddShardDeclaration_ThenMarker_UndeclaredShardDismantledOnRealEtcd()
// Arrange: add-декларация shard3 (без dsn) + маркер TO_REMOVE на shard3
// Act: RemoveShardProcess.TickAsync(snap, "shard3")
// Assert: Done; декларация вычищена реальными del (Д5 — отмена зависшего add)
```

- [ ] **Step 2: Прогнать (Docker требуется)**

Run: `dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~ShardScaleContractTests"`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add src/tests/PgWorker.IntegrationTests/Etcd/ShardScaleContractTests.cs src/tests/PgWorker.IntegrationTests/Etcd/StubScaleDriver.cs
git commit -m "t06: integration — контракт scale-ключей на реальном etcd (сид→детекция→процессы, §8)"
```

---

### Task 11: pg — e2e: scale-сценарии на живом стенде

**Files:**
- Create: `pg/src/tests/PgWorker.IntegrationTests/E2e/E2eScaleScenarios.cs`

**Interfaces:**
- Consumes: `E2eFixture`/`E2eCollection` (`StartHostAsync`, `WaitForAsync`, `RunDockerAsync`), `EtcdGateway`; helpers-паттерны из `E2eScenarios`/`E2eMoveScenarios` (сид кластера, `SetToRemoveAsync`, SQL-хелперы по мастерам, заявки `/pgworker/moves/`).
- Produces: приёмочные сценарии §8-1..5 (критерии §9.2–9.4).

- [ ] **Step 1: Написать e2e-сценарии**

Класс `[Collection(E2eCollection.Name)] public class E2eScaleScenarios(E2eFixture fixture)`; кластер `sshop`. Хелперы сида скопировать из `E2eScenarios.SeedClusterAsync` (переименовав), SQL-хелперы мастер-подключений и заявку move — из `E2eMoveScenarios` (`MasterInfoAsync`, `PutMoveRequestAsync`, `EnableSyncModeAsync`).

Сценарий 1+2+3+5 — один последовательный `[Fact]` (общий стенд; таймауты как у E2eScenarios: provisioning 360 с, операции 120–180 с):

```csharp
[Fact]
public async Task Scale_AddEmptyShard_BlockedRemoveThenAutoDismantle_NameReused()
{
    // ---------- §8-1: add-shard в живой кластер — шард поднят и ПУСТ ----------
    // Arrange: сид sshop (NOT_INITIALIZED) → StartHostAsync("s1") → WaitForAsync
    //   ProvisionedAsync("sshop") (dsn/RUNNING/без status/state — по образцу E2eScenarios)
    // Снапшот routing ДО (RangeAsync /clusters/sshop/buckets/routing/ → отсортированный список).
    // Act: сид add-декларации shard3 В СТИЛЕ ПАНЕЛИ (§6.1): replicas=2,
    //   nodes shard3a/b=NOT_INITIALIZED, /service/sshop-shard3/request_{cpu,mem,disk}.
    // Assert: WaitForAsync — dsn(shard3) есть, nodes shard3a/b=RUNNING;
    //   docker ps содержит pgw-sshop-shard3-shard3a/-shard3b (RunDockerAsync ps);
    //   routing ПОСЛЕ == routing ДО (главный ассерт границы §2.1);
    //   статус-ключей нет; SQL по мастеру shard3: SELECT count(*) FROM pg_namespace
    //   WHERE nspname LIKE 'bucket_%' == 0 (схем бакетов нет);
    //   запись в bucket_0 (владелец shard1) до и после add успешна (Npgsql INSERT).

    // ---------- §8-2: remove шарда с бакетами заблокирован G3 ----------
    // Arrange: включить sync_mode у приёмников (PATCH /config Patroni — образец
    //   E2eMoveScenarios.EnableSyncModeAsync) для shard2/shard3 (P8-префлайт move)
    // Act: заявка move bucket_1 → shard3 (заявка t01, дождаться flip);
    //   PUT /clusters/sshop/shards/shard1/state=TO_REMOVE
    // Assert: WaitForAsync — /pgworker/work/sshop содержит last_error с "бакет"
    //   (G3; у shard1 остаются ещё 2 бакета); контейнеры pgw-sshop-shard1-* живы;
    //   маркер жив.

    // ---------- §8-3: явные переезды → демонтаж завершается сам ----------
    // Act: заявки move остальных бакетов shard1 → shard3/shard2 + finalize
    //   каждого (заявки {"op":"finalize",...}); УДАЛЕНИЕ маркера НЕ повторяем.
    // Assert: WaitForAsync — контейнеры/volumes pgw-sshop-shard1-* удалены
    //   (docker ps -a + volume ls пусты); RangeAsync: /clusters/sshop/shards/shard1/
    //   и /service/sshop-shard1/ пусты; portalloc-JSON без "shard1/";
    //   кластер жив: INSERT в бакет на shard3 и на shard2 успешен.

    // ---------- §8-5: имя освобождается демонтажом ----------
    // Act: сид add-декларации с именем shard1 НАПРЯМУЮ в etcd (в обход
    //   автогенерации панели; клэйм-инвариант §4.2)
    // Assert: WaitForAsync — dsn(shard1) записан, ноды RUNNING
    //   (AddShardProcess принял освобождённое имя).
}

[Fact]
public async Task Scale_TakeoverMidAdd_SecondInstanceFinishesNoDuplicates()
{
    // ---------- §8-4: takeover посреди A3 ----------
    // Arrange: живой кластер stshop (provisioned первым инстансом s2);
    //   сид add-декларации shard3; WaitForAsync появления первого контейнера
    //   pgw-stshop-shard3-* (A3 начался)
    // Act: p1.Kill() (docker-kill PgWorker посреди A3) → StartHostAsync("s3")
    // Assert: WaitForAsync — dsn(shard3) записан, все ноды RUNNING
    //   (второй инстанс донёс, клэйм истёк ≤15 с);
    //   контейнеров pgw-stshop-shard3-* ровно 2 (нет дублей).
}
```

- [ ] **Step 2: Сборка Release + прогон e2e (Docker)**

Run: `dotnet build src/PgWorker.slnx -c Release && dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~E2eScaleScenarios"`
Expected: PASS (длительный: 2 provisioning'а + add/remove; таймаут фикстуры/тестов — по образцу E2eMoveScenarios).

- [ ] **Step 3: Полный прогон integration-сборки pg**

Run: `dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release`
Expected: PASS — прежние сценарии не сломаны.

- [ ] **Step 4: Commit**

```bash
git add src/tests/PgWorker.IntegrationTests/E2e/E2eScaleScenarios.cs
git commit -m "t06: e2e — add пустого шарда, G3-блокировка, авто-демонтаж, takeover, освобождение имени (§8)"
```

---

### Task 12: AdminPanel — arch-правки (02, 03, 01)

**Files (все в worktree ap):**
- Modify: `ap/arch/02-etcd-contract.md` (шапка, §2.1, новые §9.5/§9.6)
- Modify: `ap/arch/03-panels.md` (§1 таблица, §1.3/§1.4, §2, §3, §3.1 оговорка)
- Modify: `ap/arch/01-architecture.md` (упоминания числа мутаций)

**Interfaces:**
- Consumes: spec §4.1/§4.2 (ключи), §6.1 (API), §10.2.
- Produces: канон контракта панели, на который ссылаются Tasks 13–16.

- [ ] **Step 1: arch/02-etcd-contract.md**

1) Шапка (строки 3–6): «Единственная операция записи — создание нового кластера…» заменить на: «Панель выполняет **четыре мутации**: создание кластера (§9), перевод кластера в TO_REMOVE (§9.4), добавление шарда (§9.5), маркер демонтажа шарда (§9.6). Все остальные ключи панель не пишет и не удаляет никогда».
2) §2.1 — добавить строку таблицы:

```
| `/clusters/<C>/shards/<X>/state` | строка `"TO_REMOVE"` | `ShardInfo.State` | маркер демонтажа шарда (§9.6): пишет ТОЛЬКО панель (one-way, обратного перехода нет); отсутствие = обычный шард (ACTIVE); читают панель (бейдж «к удалению») и PgWorker (RemoveShardProcess, t06) |
```

3) После §9.4 добавить два раздела (образец стиля §9.2/§9.4; содержание — spec §4.1/§6.1):

```markdown
### 9.5. Добавление шарда (add-shard)

Третья мутация панели: `POST /api/clusters/<C>/shards` (03 §1.3) дописывает к
живому (Active) кластеру ключи нового шарда `<X>` — переиспользование схемы §9.1:
`shards/<X>/replicas`, `shards/<X>/nodes/<X><буква>/state=NOT_INITIALIZED` × R,
`/service/<C>-<X>/request_{cpu,mem,disk}`. НЕ пишется: `dsn` (запишет PgWorker),
`master` (lease Patroni), routing/status (шард стартует ПУСТЫМ — никакого
перераспределения бакетов; явные переезды — t07), config кластера.

Имя `<X>` генерирует панель: `shard<max+1>` (max — по числовым суффиксам
существующих шардов, префикс `shard`; ≤128); свободного ввода нет.

Протокол (образец §9.2): (1) config напрямую у etcd — состояние проверяется до
записи (Active only: NOT_INITIALIZED → 409 «дождитесь инициализации», TO_REMOVE
→ 409 «кластер удаляется»); (2) клэйм-txn имени: compare
`version(/clusters/<C>/shards/<X>/replicas)==0` + put `replicas`; проигрыш →
409 (конкурентный POST занял имя); (3) пакет PUT остальных ключей (nodes × R +
request_*); (4) сбой посередине → компенсация best-effort: del prefix
`shards/<X>/` + точечные del `request_*`. Без ретраев: повтор = новый POST;
повтор вычисляет ТО ЖЕ имя (компенсация успешна → тот же клэйм проходит;
выжил `replicas` → 409, остатки разбираются etcdctl). Валидация полей — §9.3
(replicas 1..26 дефолт 2, cpu 0.01..64, mem/disk 1..65536 GiB).

### 9.6. Демонтаж шарда (маркер TO_REMOVE)

Четвёртая мутация панели: `DELETE /api/clusters/<C>/shards/<X>` (03 §1.4)
ставит маркер `/clusters/<C>/shards/<X>/state = "TO_REMOVE"`. Снятие контейнеров
и очистка ключей — PgWorker (guard'ы G1–G7, t06); до демонтажа шард виден в UI
с бейджем «к удалению». Маркер — состояние, а не заявка: не удаляется по
завершении (ключи шарда исчезают вместе с ним в финале S3).

Протокол (образец §9.4): (1) имена канонические, иначе 404; (2) config
напрямую: нет → 404, не Active → 409; (3) шард существует (replicas-ключ) иначе
404; (4) серверная пред-проверка guard'ов по данным снапшота: routing на шард
>0 → 409 «на шарде N бакетов — сначала явно перевезите (UI переездов — t07)»;
незавершённый переезд (status owner/target = шард) → 409; шард один в кластере
→ 409 «нельзя снять последний шард»; ноды QUARANTINED → 409 «сначала разбор
карантина» — PgWorker перепроверит авторитетно (гонки ловят G3/G4, маркер
останется, демонтаж подождёт); (5) PUT маркера; уже `TO_REMOVE` →
идемпотентный 204 без записи. Обратного перехода нет (one-way). Имя шарда
освобождается финалом демонтажа PgWorker (после него клэйм-txn §9.5 того же
имени пройдёт).
```

(Примечание к коммиту ниже: сообщение — `t06: arch — мутации шардов: 02 §2.1/§9.5/§9.6, 03 §1.3/§1.4/§3.2, 01`.)

- [ ] **Step 2: arch/03-panels.md**

1) §1 таблица — после строки `DELETE /api/clusters/{name}` добавить:

```
| `POST /api/clusters/{cluster}/shards` | добавить шард Active-кластеру (02 §9.5): тело `AddShardRequestDto` → 201+`ShardAddedDto` \| 400 \| 404 \| 409 \| 503 |
| `DELETE /api/clusters/{cluster}/shards/{shard}` | маркер демонтажа шарда `TO_REMOVE` (02 §9.6): 204 \| 404 \| 409 \| 503 |
```

2) Новые §1.3/§1.4 (после §1.2) — контракты из spec §6.1 (тело/ответ/коды — дословно; AddShardRequestDto: replicas (1..26, дефолт 2), requestCpu (0.01..64), requestMem/requestDisk (1..65536 GiB); ShardAddedDto: cluster, name (сгенерированное `shard<k>`), replicas, requestCpu/requestMem/requestDisk (канонические строки), state:"NOT_INITIALIZED").
3) §2 — `ShardDto` дополнить полем `state(ACTIVE|TO_REMOVE)`; добавить описания `AddShardRequestDto`/`ShardAddedDto`.
4) §3 «Cluster details» — дополнить строку вкладки Шарды: «колонка действий — кнопка "Убрать шард" (красная, per-row; диалог со счётчиком бакетов шарда, дизейбл при N>0 с пояснением "сначала перевезите бакеты", серверный 409 — текстом ProblemDetails); бейдж "к удалению" у шарда state=TO_REMOVE; кнопка "Добавить шард" (модальная форма §3.2: реплики/CPU/память/диск, без имени — генерируется; подпись "Шард стартует пустым — перераспределение бакетов выполняется отдельными явными переездами")». Отметить: «кнопки скрыты, когда кластер не Active (симметрия с "Удалить кластер")».
5) §3.1 заголовок «(единственная форма данных)» — добавить оговорку «+ форма добавления шарда на Cluster details (t06, §3.2)». Добавить короткий §3.2 «Форма "Добавить шард"» с полями.

- [ ] **Step 3: arch/01-architecture.md**

Строка ~6: «Единственная мутация инспектируемых систем — **создание кластера**» → «Мутации инспектируемых систем — **четыре**: создание кластера, перевод кластера в TO_REMOVE, добавление шарда, маркер демонтажа шарда (02 §9–§9.6)». Просмотреть файл на другие упоминания «единственная мутация»/«две мутации» и синхронизировать.

- [ ] **Step 4: Проверка + commit**

Run: `cd /Users/demakaev/ZCodeProject/worktrees/ap-t06-shard-autoscaling && git diff --stat`
Expected: 3 файла arch/.

```bash
git add arch/01-architecture.md arch/02-etcd-contract.md arch/03-panels.md
git commit -m "t06: arch — мутации шардов: 02 §2.1/§9.5/§9.6, 03 §1.3/§1.4/§3.2, 01"
```

---

### Task 13: AdminPanel — Core/Etcd: ShardState, парсер, ShardScalePlan, валидатор

**Files (worktree ap):**
- Modify: `ap/src/AdminPanel.Core/ClusterInfo.cs` (enum ShardState, ShardInfo.State)
- Modify: `ap/src/AdminPanel.Etcd/Parsing/ClustersParser.cs` (ShardAcc.StateRaw, ветка `shards/<X>/state`)
- Create: `ap/src/AdminPanel.Etcd/Writing/ShardScalePlan.cs` (AddShardRequest + limits + validator + план)
- Test: `ap/src/tests/AdminPanel.UnitTests/ClustersParserTests.cs` (новые кейсы), Create: `ap/src/tests/AdminPanel.UnitTests/ShardScalePlanTests.cs`

**Interfaces:**
- Consumes: `CreateClusterLimits`/`CreateClusterValidator.CanonicalCpu/CanonicalGiB` (`ap/src/AdminPanel.Etcd/Writing/CreateClusterRequest.cs`), `KvPut`/`TxnCompare` (`AdminPanel.Etcd.Client`).
- Produces: `enum ShardState { Active, ToRemove }`; `ShardInfo(…, ShardState State = ShardState.Active)`; `AddShardRequest(int Replicas, decimal RequestCpu, int RequestMem, int RequestDisk)`; `AddShardValidator.Validate(AddShardRequest) → IReadOnlyList<ValidationError>`; `ShardScalePlan.Build(string cluster, string shard, AddShardRequest request) → ShardScalePlan { ReplicasKey, ReplicasValue, Puts, RequestKeys, CanonicalCpu, CanonicalMem, CanonicalDisk }`. Используют Tasks 14–15.

- [ ] **Step 1: Failing-тесты парсера**

В `ClustersParserTests`:

```csharp
[Fact]
public void Parse_ShardStateToRemove_MapsToShardInfoState()
{
    // Arrange — маркер демонтажа шарда (t06 §9.6); сегодня ключ уходит в unknown
    var kvs = new List<Kv>
    {
        new("/clusters/shop/config", """{"buckets":1,"dbname":"shop"}""", 1),
        new("/clusters/shop/shards/shard1/replicas", "2", 2),
        new("/clusters/shop/shards/shard1/state", "TO_REMOVE", 3),
    };

    // Act
    var result = ClustersParser.Parse(kvs);

    // Assert — state в модели; unknown-счётчик не вырос
    result.Clusters.Single().Shards.Single().State.Should().Be(ShardState.ToRemove);
    result.UnknownKeyCount.Should().Be(0);
}

[Fact]
public void Parse_NoShardState_DefaultsActive()
{
    // Arrange — ключа state нет: обычный шард (02 §2.1-паттерн «отсутствие = Active»)
    var kvs = new List<Kv>
    {
        new("/clusters/shop/config", """{"buckets":1,"dbname":"shop"}""", 1),
        new("/clusters/shop/shards/shard1/replicas", "2", 2),
    };

    // Act
    var result = ClustersParser.Parse(kvs);

    // Assert
    result.Clusters.Single().Shards.Single().State.Should().Be(ShardState.Active);
}
```

- [ ] **Step 2: Реализовать модель+парсер**

`ClusterInfo.cs` — добавить enum и поле:

```csharp
// Состояние шарда: shards/<X>/state (t06 §9.6); отсутствие = Active.
public enum ShardState { Active, ToRemove }

public sealed record ShardInfo(…, ShardRuntime? Runtime, ShardState State = ShardState.Active)
```

`ClustersParser.cs`: `ShardAcc` += `public string? StateRaw;`; новый case ПЕРЕД существующим `dsn/replicas/master`:

```csharp
case "shards" when segments.Length == 6 && segments[5] == "state":
{
    // Маркер демонтажа шарда (t06 §9.6): уходит из unknown-счётчика в модель.
    var shard = GetOrAdd(acc.Shards, segments[4], static _ => new ShardAcc());
    shard.StateRaw = kv.Value;
    break;
}
```

`BuildShard` — последний аргумент `shard.StateRaw?.Trim() == "TO_REMOVE" ? ShardState.ToRemove : ShardState.Active`.

- [ ] **Step 3: Failing-тесты плана+валидатора**

`ShardScalePlanTests`:

```csharp
[Fact]
public void Build_FullKeySet_MatchesContractSection9_5()
{
    // Arrange — запрос на 2 реплики (валидный)
    var request = new AddShardRequest(2, 0.5m, 8, 100);

    // Act
    var plan = ShardScalePlan.Build("shop", "shard3", request);

    // Assert — ровно контракт 02 §9.5 (1:1 §4.1 spec): клэйм-ключ + пакет
    plan.ReplicasKey.Should().Be("/clusters/shop/shards/shard3/replicas");
    plan.ReplicasValue.Should().Be("2");
    plan.Puts.Select(p => p.Key).Should().BeEquivalentTo(
    [
        "/clusters/shop/shards/shard3/nodes/shard3a/state",
        "/clusters/shop/shards/shard3/nodes/shard3b/state",
        "/service/shop-shard3/request_cpu",
        "/service/shop-shard3/request_mem",
        "/service/shop-shard3/request_disk",
    ]);
    plan.Puts.Single(p => p.Key.EndsWith("shard3a/state")).Value.Should().Be("NOT_INITIALIZED");
    plan.RequestKeys.Should().BeEquivalentTo(
    [
        "/service/shop-shard3/request_cpu", "/service/shop-shard3/request_mem", "/service/shop-shard3/request_disk",
    ]);
    plan.CanonicalCpu.Should().Be("0.5");
    plan.CanonicalMem.Should().Be("8Gi");
    plan.CanonicalDisk.Should().Be("100Gi");
}

[Fact]
public void Validator_Boundaries_TableOf409And400()
{
    // Arrange / Act / Assert — границы §9.3: replicas 0/27 → ошибка; cpu
    // 0.009/64.1 → ошибка; mem 0/65537 → ошибка; валидный (2, 2, 8, 100) → пусто
    AddShardValidator.Validate(new(0, 2, 8, 100)).Should().Contain(e => e.Field == "replicas");
    AddShardValidator.Validate(new(27, 2, 8, 100)).Should().Contain(e => e.Field == "replicas");
    AddShardValidator.Validate(new(2, 0.009m, 8, 100)).Should().Contain(e => e.Field == "requestCpu");
    AddShardValidator.Validate(new(2, 2, 0, 100)).Should().Contain(e => e.Field == "requestMem");
    AddShardValidator.Validate(new(2, 2, 8, 65537)).Should().Contain(e => e.Field == "requestDisk");
    AddShardValidator.Validate(new(2, 2, 8, 100)).Should().BeEmpty();
}
```

- [ ] **Step 4: Реализовать ShardScalePlan**

`ap/src/AdminPanel.Etcd/Writing/ShardScalePlan.cs`:

```csharp
using System.Globalization;
using AdminPanel.Etcd.Client;

namespace AdminPanel.Etcd.Writing;

// Тело POST /api/clusters/{cluster}/shards (arch/02 §9.5): replicas с дефолтом 2
// обрабатывает handler (JSON-биндинг даёт 0 при отсутствии поля).
public sealed record AddShardRequest(int Replicas, decimal RequestCpu, int RequestMem, int RequestDisk);

// Границы — те же, что создания кластера (arch/02 §9.3; константы кода).
public static class AddShardValidator
{
    public static IReadOnlyList<ValidationError> Validate(AddShardRequest request)
    {
        var errors = new List<ValidationError>();
        if (request.Replicas is < CreateClusterLimits.MinReplicas or > CreateClusterLimits.MaxReplicas)
            errors.Add(new("replicas",
                $"реплики: целое {CreateClusterLimits.MinReplicas}..{CreateClusterLimits.MaxReplicas}"));
        if (request.RequestCpu < CreateClusterLimits.MinCpu || request.RequestCpu > CreateClusterLimits.MaxCpu)
            errors.Add(new("requestCpu",
                $"CPU (ядра): {CreateClusterLimits.MinCpu}..{CreateClusterLimits.MaxCpu}"));
        if (request.RequestMem is < CreateClusterLimits.MinGiB or > CreateClusterLimits.MaxGiB)
            errors.Add(new("requestMem",
                $"память (GiB): {CreateClusterLimits.MinGiB}..{CreateClusterLimits.MaxGiB}"));
        if (request.RequestDisk is < CreateClusterLimits.MinGiB or > CreateClusterLimits.MaxGiB)
            errors.Add(new("requestDisk",
                $"диск (GiB): {CreateClusterLimits.MinGiB}..{CreateClusterLimits.MaxGiB}"));
        return errors;
    }
}

// План ключей одного add-shard (arch/02 §9.5): чистая функция — вызывается
// ТОЛЬКО после валидатора (образец ClusterCreatePlan).
public sealed record ShardScalePlan(
    string ReplicasKey,
    string ReplicasValue,
    IReadOnlyList<KvPut> Puts,          // nodes state × R + request_* (пакет PUT после клэйма)
    IReadOnlyList<string> RequestKeys,  // компенсация: точечные del
    string CanonicalCpu,
    string CanonicalMem,
    string CanonicalDisk)
{
    public const string NotInitialized = "NOT_INITIALIZED";

    public static ShardScalePlan Build(string cluster, string shard, AddShardRequest request)
    {
        var cpu = CreateClusterValidator.CanonicalCpu(request.RequestCpu);
        var mem = CreateClusterValidator.CanonicalGiB(request.RequestMem);
        var disk = CreateClusterValidator.CanonicalGiB(request.RequestDisk);

        var puts = new List<KvPut>();
        for (var r = 0; r < request.Replicas; r++)
            puts.Add(new(
                $"/clusters/{cluster}/shards/{shard}/nodes/{shard}{(char)('a' + r)}/state",
                NotInitialized));

        var requestKeys = new List<string>();
        foreach (var (leaf, value) in new[] { ("request_cpu", cpu), ("request_mem", mem), ("request_disk", disk) })
        {
            var key = $"/service/{cluster}-{shard}/{leaf}";
            puts.Add(new(key, value));
            requestKeys.Add(key);
        }

        puts.Sort((a, b) => string.CompareOrdinal(a.Key, b.Key));
        return new ShardScalePlan(
            $"/clusters/{cluster}/shards/{shard}/replicas",
            request.Replicas.ToString(),
            puts,
            requestKeys,
            cpu, mem, disk);
    }
}
```

- [ ] **Step 5: Тесты + сборка**

Run: `cd /Users/demakaev/ZCodeProject/worktrees/ap-t06-shard-autoscaling && dotnet build src/AdminPanel.slnx -c Release && dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj -c Release`
Expected: PASS (новые + прежние; дефолт `State` в ShardInfo не сломал call-sites).

- [ ] **Step 6: Commit**

```bash
git add src/AdminPanel.Core/ClusterInfo.cs src/AdminPanel.Etcd/Parsing/ClustersParser.cs src/AdminPanel.Etcd/Writing/ShardScalePlan.cs src/tests/AdminPanel.UnitTests/ClustersParserTests.cs src/tests/AdminPanel.UnitTests/ShardScalePlanTests.cs
git commit -m "t06: Core/Etcd — ShardState в модели/парсере; ShardScalePlan + AddShardValidator (§9.5)"
```

---

### Task 14: AdminPanel — API: команды add/remove шарда, эндпоинты, ShardDto.state

**Files (worktree ap):**
- Create: `ap/src/AdminPanel.Api/Operations/AddShardCommand.cs`
- Create: `ap/src/AdminPanel.Api/Operations/DeleteShardCommand.cs`
- Modify: `ap/src/AdminPanel.Api/Operations/OperationsModule.cs` (два эндпоинта)
- Modify: `ap/src/AdminPanel.Api/Inspection/ClusterDetailsQuery.cs` (ShardDto.State + ShardStates + маппер)
- Test: Create `ap/src/tests/AdminPanel.UnitTests/AddShardCommandHandlerTests.cs`, `ap/src/tests/AdminPanel.UnitTests/DeleteShardCommandHandlerTests.cs`; Modify `ap/src/tests/AdminPanel.UnitTests/ClustersMappersTests.cs`

**Interfaces:**
- Consumes: Task 13 (`ShardScalePlan`, `AddShardValidator`, `ShardInfo.State`); `ISnapshotStore`, `IEtcdGateway.RangeAsync/TxnAsync/PutAsync/DeleteAsync`, `EtcdWriteUnavailableException`, `ClusterNotFoundException`, `CreateClusterLimits.NamePattern`, `TestSnapshots`/`InspectionSnapshots` (тесты).
- Produces: `POST /api/clusters/{cluster}/shards` → 201 `ShardAddedDto(cluster, name, replicas, requestCpu, requestMem, requestDisk, state:"NOT_INITIALIZED")`; `DELETE /api/clusters/{cluster}/shards/{shard}` → 204; `ShardDto.State: "ACTIVE"|"TO_REMOVE"`; исключения `AddShardValidationException`(400), `ClusterNotActiveException`(409), `ShardNameTakenException`(409), `ShardLimitReachedException`(409), `ShardNotFoundException`(404), `ShardRemoveBlockedException`(409), `ShardPrecheckUnavailableException`(503).

- [ ] **Step 1: Failing unit-тесты AddShardCommandHandler**

`AddShardCommandHandlerTests` — FakeGateway по образцу `DeleteClusterCommandHandlerTests` (Range/Put/Txn/Delete перехват; Txn с полем `SucceedTxn` для проигрыша клэйма; снапшот через `TestSnapshots.Healthy` с ActiveEndpoint; для сбоя чтения — инъекция `Func<string, bool>? FailRangeByPrefix`, возвращающая true для нужного префикса → `Result<...>.Failed(…)`, чтобы отличать 503 от 404). Кейсы (все AAA):

```csharp
[Fact] Handle_InvalidRequest_ReturnsValidationErrors       // replicas=27 → AddShardValidationException с errors
    // (replicas=0 НЕ годится: handler подставляет дефолт 2 ДО валидации — §6.1/§9.3;
    //  поведение 0→дефолт отдельно покрыто Handle_ReplicasZeroDefaultsToTwo)
[Fact] Handle_NoConfigKey_Returns404ClusterNotFound        // Range пуст по config-ключу
[Fact] Handle_ConfigReadFails_ReturnsEtcdError503Not404    // RangeAsync по config-ключу Failed →
    // результат Failed с той же ошибкой (эндпоинт даст 503 «Etcd write failed»,
    // НЕ ClusterNotFoundException/404 — контракт кодов §6.1)
[Fact] Handle_ClusterNotInitialized_ReturnsClusterNotActive // config state=NOT_INITIALIZED → ClusterNotActiveException
[Fact] Handle_ClusterToRemove_ReturnsClusterNotActive      // state=TO_REMOVE → ClusterNotActiveException
[Fact] Handle_ComputesShardNameMaxPlusOne                  // Range содержит shards/shard1,shard2/replicas →
    // имя shard3: Txn compare на /clusters/shop/shards/shard3/replicas; Puts — nodes+request_*
[Fact] Handle_ClaimTxnLost_ReturnsShardNameTaken           // SucceedTxn=false → ShardNameTakenException; Put пакетов НЕ было
[Fact] Handle_PutFailsMiddle_Compensates                   // Put падает на 2-м ключе → DeleteAsync вызван с prefix
    // "/clusters/shop/shards/shard3/" и точечными request_*; результат Failed
[Fact] Handle_Success_ReturnsCanonicalDto                  // name=shard3, cpu "0.5", mem "8Gi", disk "100Gi", state NOT_INITIALIZED
[Fact] Handle_ReplicasZeroDefaultsToTwo                     // replicas=0 в запросе → Txn/Puts с replicas "2"
    // (положительный кейс дефолта §6.1: 0 = поле отсутствовало; валидацию не ломает)
```

- [ ] **Step 2: Реализовать AddShardCommand**

`ap/src/AdminPanel.Api/Operations/AddShardCommand.cs` (протокол — spec §6.1 / 02 §9.5; образец `CreateClusterCommand`):

```csharp
using System.Text.Json;
using System.Text.RegularExpressions;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Writing;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Operations;

// Команда добавления шарда — третья мутация панели (arch/02 §9.5, t06).
public sealed record AddShardCommand(string Cluster, AddShardRequest Request) : ICommand<ShardAddedDto>;

// Ответ 201 POST /api/clusters/{cluster}/shards (arch/03 §1.3).
public sealed record ShardAddedDto(
    string Cluster, string Name, int Replicas,
    string RequestCpu, string RequestMem, string RequestDisk, string State);

public sealed class AddShardValidationException(IReadOnlyList<ValidationError> errors)
    : Exception("параметры добавления шарда некорректны")
{
    public IReadOnlyList<ValidationError> Errors { get; } = errors;
}

// Кластер не Active: NOT_INITIALIZED («дождитесь инициализации») или TO_REMOVE
// («кластер удаляется») — подсказка оператору по state (§9.5/§9.6).
public sealed class ClusterNotActiveException(string name, string state)
    : Exception(state == "NOT_INITIALIZED"
        ? $"кластер {name} ещё инициализируется (NOT_INITIALIZED) — дождитесь инициализации"
        : $"кластер {name} удаляется (TO_REMOVE) — операция запрещена");

// Клэйм-txn имени не сошёлся: конкурентный POST занял имя (arch/02 §9.5).
public sealed class ShardNameTakenException(string cluster, string shard)
    : Exception($"имя шарда {cluster}/{shard} занято (replicas-ключ присутствует)");

// shard<max+1> превысил предел числа шардов (§9.3: ≤128).
public sealed class ShardLimitReachedException(string cluster)
    : Exception($"кластер {cluster} достиг предела числа шардов (128)");

[InjectAsScoped]
public sealed class AddShardCommandHandler(ISnapshotStore store, IEtcdGateway gateway)
    : ICommandHandler<AddShardCommand, ShardAddedDto>
{
    private const int MaxShards = CreateClusterLimits.MaxShards;

    // Имя существующего шарда панели: shard<k> (§9.1).
    [GeneratedRegex("^shard(\\d+)$")]
    private static partial Regex PanelShardPattern();

    public async ValueTask<Result<ShardAddedDto>> Handle(AddShardCommand command, CancellationToken ct)
    {
        var cluster = command.Cluster;

        // 1) Валидация (replicas 0 = поле отсутствовало → дефолт 2, §9.3).
        var request = command.Request with { Replicas = command.Request.Replicas == 0 ? 2 : command.Request.Replicas };
        var errors = AddShardValidator.Validate(request);
        if (errors.Count > 0)
            return Result<ShardAddedDto>.Failed(new AddShardValidationException(errors));

        // 2) Активный endpoint из снапшота.
        var snapshot = store.Current;
        if (snapshot?.Etcd.ActiveEndpoint is not { } endpoint)
            return Result<ShardAddedDto>.Failed(new EtcdWriteUnavailableException());

        // 3) Config напрямую: имя каноническое (иначе 404), ключа нет → 404,
        //    сбой чтения → 503, state не Active → 409 (§9.5).
        if (!CreateClusterLimits.NamePattern().IsMatch(cluster ?? ""))
            return Result<ShardAddedDto>.Failed(new ClusterNotFoundException(cluster));
        var config = await ReadKeyAsync(endpoint, $"/clusters/{cluster}/config", ct);
        if (!config.IsSuccess)
            return Result<ShardAddedDto>.Failed(config.Error!); // 503 (etcd недоступен)
        if (config.Value is null)
            return Result<ShardAddedDto>.Failed(new ClusterNotFoundException(cluster));
        string? rawState;
        try { rawState = ReadStateField(config.Value); }
        catch (JsonException)
        {
            return Result<ShardAddedDto>.Failed(new InvalidClusterConfigException(cluster)); // 503
        }

        if (rawState is not null)
            return Result<ShardAddedDto>.Failed(new ClusterNotActiveException(cluster, rawState));

        // 4) Имя shard<max+1> по фактическому префиксу shards/ (range).
        var shardsRange = await gateway.RangeAsync(endpoint, $"/clusters/{cluster}/shards/", ct);
        if (!shardsRange.IsSuccess)
            return Result<ShardAddedDto>.Failed(shardsRange.Error!);
        var max = shardsRange.Value
            .Select(kv => kv.Key.Split('/')[..^1])
            .Where(segments => segments.Length == 5 && segments[3] == "shards")
            .Select(segments => PanelShardPattern().Match(segments[4]))
            .Where(m => m.Success)
            .Select(m => int.Parse(m.Groups[1].Value))
            .DefaultIfEmpty(0)
            .Max();
        if (max + 1 > MaxShards)
            return Result<ShardAddedDto>.Failed(new ShardLimitReachedException(cluster));
        var shard = $"shard{max + 1}";

        // 5) Клэйм-txn имени (§9.5): compare version(replicas)==0 + put replicas.
        var plan = ShardScalePlan.Build(cluster, shard, request);
        var claim = await gateway.TxnAsync(
            endpoint, [new TxnCompare(plan.ReplicasKey, 0)], [new KvPut(plan.ReplicasKey, plan.ReplicasValue)], ct);
        if (!claim.IsSuccess)
            return Result<ShardAddedDto>.Failed(claim.Error!);
        if (!claim.Value.Succeeded)
            return Result<ShardAddedDto>.Failed(new ShardNameTakenException(cluster, shard));

        // 6) Пакет PUT; сбой посередине → компенсация best-effort (§9.5).
        foreach (var put in plan.Puts)
        {
            var putResult = await gateway.PutAsync(endpoint, put.Key, put.Value, ct);
            if (putResult.IsSuccess)
                continue;

            await gateway.DeleteAsync(endpoint, $"/clusters/{cluster}/shards/{shard}/", prefix: true, ct);
            foreach (var key in plan.RequestKeys)
                await gateway.DeleteAsync(endpoint, key, prefix: false, ct);
            return Result<ShardAddedDto>.Failed(putResult.Error!);
        }

        return Result<ShardAddedDto>.Success(new ShardAddedDto(
            cluster, shard, request.Replicas,
            plan.CanonicalCpu, plan.CanonicalMem, plan.CanonicalDisk,
            ShardScalePlan.NotInitialized));
    }

    // Точечное чтение ключа через range (gateway без GetAsync — образец §9.4).
    // Различаем сбой и отсутствие (§6.1): Failed → эндпоинт ответит 503,
    // Success(null) — ровно «ключа нет» (404-путь вызывающего).
    private async Task<Result<string?>> ReadKeyAsync(string endpoint, string key, CancellationToken ct)
    {
        var range = await gateway.RangeAsync(endpoint, key, ct);
        if (!range.IsSuccess)
            return Result<string?>.Failed(range.Error!); // 503: etcd недоступен
        return Result<string?>.Success(range.Value.FirstOrDefault(kv => kv.Key == key)?.Value);
    }

    // state из config-JSON; битый JSON бросает JsonException наверх (→ 503).
    private static string? ReadStateField(string raw)
    {
        using var doc = JsonDocument.Parse(raw);
        return doc.RootElement.TryGetProperty("state", out var state)
            && state.ValueKind == JsonValueKind.String
            ? state.GetString()
            : null;
    }
}
```

(После этой правки примечание про «битый config уронит клэйм-этап ниже» устарело: `ReadStateField` больше не глотает JsonException — handler явно возвращает `InvalidClusterConfigException` (существующий тип из `DeleteClusterCommand.cs`) → 503.)

- [ ] **Step 3: Failing unit-тесты DeleteShardCommandHandler**

`DeleteShardCommandHandlerTests` — FakeGateway + снапшот через `TestSnapshots`/`InspectionSnapshots.Fixture` с кластером shop (2 шарда, routing 4+2, состояние шардов Active; в FakeGateway — инъекция сбоя Range по префиксу, как в тестах AddShard). Кейсы:

```csharp
[Fact] Handle_NoClusterConfig_Returns404
[Fact] Handle_ConfigReadFails_ReturnsEtcdError503Not404     // RangeAsync Failed → исходная
    // ошибка (503), НЕ ClusterNotFoundException
[Fact] Handle_ClusterNotActive_ReturnsClusterNotActive
[Fact] Handle_NoReplicasKey_ReturnsShardNotFound            // шарда нет
[Fact] Handle_ReplicasReadFails_ReturnsEtcdError503Not404   // сбой чтения replicas-ключа →
    // 503, НЕ ShardNotFoundException
[Fact] Handle_RoutingOnShard_Returns409WithBucketCount      // 3 бакета на shard1 →
    // ShardRemoveBlockedException, Message содержит "3" и "перевезите"; Put НЕ вызван
[Fact] Handle_UnfinishedMoveTargetShard_Returns409          // Buckets: SYNCING с
    // Move.Target=shard1 → 409 «незавершённый переезд»
[Fact] Handle_UnfinishedMoveFlippedOwnerShard_Returns409    // «flip прошёл, статус завис»:
    // бакет Owner=shard2 (routing переехал), Move.Owner=shard1 (статус жив) → 409 —
    // пред-проверка смотрит и статус-owner (зеркало G4, §4.4 «owner ИЛИ target»)
[Fact] Handle_SingleShardCluster_Returns409LastShard        // Shards.Count=1 → 409 «последний шард»
[Fact] Handle_QuarantinedNode_Returns409Quarantine          // Nodes state "QUARANTINED" → 409
[Fact] Handle_AlreadyToRemove_IdempotentSuccessNoWrite      // Range вернул state-ключ TO_REMOVE → успех без Put
[Fact] Handle_MarkerReadFails_ReturnsEtcdError_NoPut        // сбой чтения state-ключа → 503
    // (НЕ идемпотентный успех и НЕ PUT поверх нечитанного состояния)
[Fact] Handle_Success_PutsMarkerKey                         // Put("/clusters/shop/shards/shard1/state","TO_REMOVE")
[Fact] Handle_SnapshotLag_Returns503PrecheckUnavailable     // кластера нет в снапшоте → ShardPrecheckUnavailableException
```

- [ ] **Step 4: Реализовать DeleteShardCommand**

`ap/src/AdminPanel.Api/Operations/DeleteShardCommand.cs` (протокол — spec §6.1 / 02 §9.6; образец `DeleteClusterCommand`; пред-проверки — Д4):

```csharp
using System.Text.Json;
using System.Text.RegularExpressions;
using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Operations;

// Команда демонтажа шарда — четвёртая мутация панели (arch/02 §9.6, t06):
// one-way маркер shards/<X>/state=TO_REMOVE; очистку выполняет PgWorker.
public sealed record DeleteShardCommand(string Cluster, string Shard) : ICommand<ShardDeletedDto>;

public sealed record ShardDeletedDto(string Cluster, string Shard, string State);

public sealed class ShardNotFoundException(string cluster, string shard)
    : Exception($"шард {cluster}/{shard} не найден (replicas-ключ отсутствует)");

// Быстрая серверная пред-проверка guard'ов (Д4): PgWorker перепроверит авторитетно.
public sealed class ShardRemoveBlockedException(string reason) : Exception(reason)
{
    public static ShardRemoveBlockedException Buckets(int count)
        => new($"на шарде {count} бакетов — сначала явно перевезите (UI переездов — t07)");

    public static ShardRemoveBlockedException UnfinishedMove()
        => new("незавершённый переезд бакета — завершите/отмените");

    public static ShardRemoveBlockedException LastShard()
        => new("нельзя снять последний шард — для полного демонтажа удалите кластер");

    public static ShardRemoveBlockedException Quarantine()
        => new("шард в карантине после эвакуации — сначала разбор данных");
}

// Снапшот отстаёт (кластер в etcd есть, в снапшоте нет) — повтор запроса.
public sealed class ShardPrecheckUnavailableException()
    : Exception("снапшот панели отстаёт — повторите запрос");

[InjectAsScoped]
public sealed class DeleteShardCommandHandler(ISnapshotStore store, IEtcdGateway gateway)
    : ICommandHandler<DeleteShardCommand, ShardDeletedDto>
{
    public const string ToRemoveState = "TO_REMOVE"; // канон маркера (§9.6)

    // Паттерн имени шарда (§9.5/pg §4.1): без дефиса.
    [GeneratedRegex("^[a-z][a-z0-9_]{0,30}$")]
    private static partial Regex ShardNamePattern();

    public async ValueTask<Result<ShardDeletedDto>> Handle(DeleteShardCommand command, CancellationToken ct)
    {
        var (cluster, shard) = (command.Cluster, command.Shard);

        // 1) Имена канонические, иначе 404 (такие панель создать не могла).
        if (!CreateClusterLimits.NamePattern().IsMatch(cluster ?? "")
            || !ShardNamePattern().IsMatch(shard ?? ""))
            return Result<ShardDeletedDto>.Failed(new ClusterNotFoundException(cluster));

        // 2) Активный endpoint.
        var snapshot = store.Current;
        if (snapshot?.Etcd.ActiveEndpoint is not { } endpoint)
            return Result<ShardDeletedDto>.Failed(new EtcdWriteUnavailableException());

        // 3) Config напрямую: нет → 404; сбой чтения → 503; не Active → 409.
        var config = await ReadKeyAsync(endpoint, $"/clusters/{cluster}/config", ct);
        if (!config.IsSuccess)
            return Result<ShardDeletedDto>.Failed(config.Error!); // 503 (etcd недоступен)
        if (config.Value is null)
            return Result<ShardDeletedDto>.Failed(new ClusterNotFoundException(cluster));
        string? state = null;
        try { state = ReadState(config.Value); }
        catch (JsonException) { return Result<ShardDeletedDto>.Failed(new InvalidClusterConfigException(cluster)); }
        if (state is not null)
            return Result<ShardDeletedDto>.Failed(new ClusterNotActiveException(cluster, state));

        // 4) Шард существует (replicas-ключ) иначе 404; сбой чтения → 503.
        var replicas = await ReadKeyAsync(endpoint, $"/clusters/{cluster}/shards/{shard}/replicas", ct);
        if (!replicas.IsSuccess)
            return Result<ShardDeletedDto>.Failed(replicas.Error!); // 503
        if (replicas.Value is null)
            return Result<ShardDeletedDto>.Failed(new ShardNotFoundException(cluster, shard));

        // 5) Пред-проверки guard'ов по данным снапшота (Д4: быстро оператору;
        //    гонки ловят G3/G4 PgWorker — маркер-состояние ждёт бесконечно).
        //    Переезд: owner ИЛИ target СТАТУСА, плюс routing-owner (зеркало G4:
        //    после flip routing уже уехал, а зависший статус держит старый шард
        //    в Move.Owner — §4.4 «owner ИЛИ target»).
        var info = snapshot.Clusters.FirstOrDefault(c => c.Name == cluster)
            ?? return Result<ShardDeletedDto>.Failed(new ShardPrecheckUnavailableException());
        var shardInfo = info.Shards.FirstOrDefault(s => s.Name == shard);
        var owned = info.Buckets.Count(b => b.Owner == shard);
        if (owned > 0)
            return Result<ShardDeletedDto>.Failed(ShardRemoveBlockedException.Buckets(owned));
        if (info.Buckets.Any(b => b.State is BucketState.Syncing or BucketState.Frozen or BucketState.Aborting
                && (b.Owner == shard || b.Move?.Owner == shard || b.Move?.Target == shard)))
            return Result<ShardDeletedDto>.Failed(ShardRemoveBlockedException.UnfinishedMove());
        if (info.Shards.Count <= 1)
            return Result<ShardDeletedDto>.Failed(ShardRemoveBlockedException.LastShard());
        if (shardInfo?.Nodes.Any(n => n.State == "QUARANTINED") == true)
            return Result<ShardDeletedDto>.Failed(ShardRemoveBlockedException.Quarantine());

        // 6) PUT маркера; уже TO_REMOVE → идемпотентный успех без записи (§9.6);
        //    сбой чтения state-ключа → 503 (не пишем поверх нечитанного).
        var markerKey = $"/clusters/{cluster}/shards/{shard}/state";
        var marker = await ReadKeyAsync(endpoint, markerKey, ct);
        if (!marker.IsSuccess)
            return Result<ShardDeletedDto>.Failed(marker.Error!); // 503
        if (marker.Value == ToRemoveState)
            return Result<ShardDeletedDto>.Success(new ShardDeletedDto(cluster, shard, ToRemoveState));

        var put = await gateway.PutAsync(endpoint, markerKey, ToRemoveState, ct);
        if (!put.IsSuccess)
            return Result<ShardDeletedDto>.Failed(put.Error!);
        return Result<ShardDeletedDto>.Success(new ShardDeletedDto(cluster, shard, ToRemoveState));
    }

    // Точечное чтение ключа через range (образец §9.4). Failed → 503 у эндпоинта;
    // Success(null) — ровно «ключа нет» (404-путь вызывающего), §6.1.
    private async Task<Result<string?>> ReadKeyAsync(string endpoint, string key, CancellationToken ct)
    {
        var range = await gateway.RangeAsync(endpoint, key, ct);
        if (!range.IsSuccess)
            return Result<string?>.Failed(range.Error!); // 503: etcd недоступен
        return Result<string?>.Success(range.Value.FirstOrDefault(kv => kv.Key == key)?.Value);
    }

    private static string? ReadState(string raw)
    {
        using var doc = JsonDocument.Parse(raw);
        return doc.RootElement.TryGetProperty("state", out var s) && s.ValueKind == JsonValueKind.String
            ? s.GetString() : null;
    }
}
```

- [ ] **Step 5: Эндпоинты + ShardDto.state**

`OperationsModule.cs` — добавить (маппинг ошибок по образцу существующих):

```csharp
// POST /api/clusters/{cluster}/shards — добавить шард Active-кластеру (02 §9.5, t06).
endpoints.MapPost("/api/clusters/{cluster}/shards", async (
    string cluster, AddShardRequest request, IHandler handler, CancellationToken ct) =>
{
    var result = await handler.HandleCommand<AddShardCommand, ShardAddedDto>(
        new AddShardCommand(cluster, request), ct);
    if (result.IsSuccess)
        return Results.Created($"/api/clusters/{cluster}/shards/{result.Value.Name}", result.Value);

    return result.Error switch
    {
        AddShardValidationException validation => Results.Problem(
            statusCode: StatusCodes.Status400BadRequest,
            title: "Validation failed",
            detail: result.Error.Message,
            extensions: new Dictionary<string, object?>
            {
                ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }),
            }),
        ClusterNotFoundException => Results.Problem(
            statusCode: StatusCodes.Status404NotFound, title: "Cluster not found", detail: result.Error.Message),
        ClusterNotActiveException or ShardNameTakenException or ShardLimitReachedException => Results.Problem(
            statusCode: StatusCodes.Status409Conflict, title: "Shard add rejected", detail: result.Error.Message),
        EtcdWriteUnavailableException => Results.Problem(
            statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable", detail: result.Error.Message),
        _ => Results.Problem(
            statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed", detail: result.Error!.Message),
    };
});

// DELETE /api/clusters/{cluster}/shards/{shard} — маркер демонтажа (02 §9.6, t06);
// 204 идемпотентен; 404/409/503.
endpoints.MapDelete("/api/clusters/{cluster}/shards/{shard}", async (
    string cluster, string shard, IHandler handler, CancellationToken ct) =>
{
    var result = await handler.HandleCommand<DeleteShardCommand, ShardDeletedDto>(
        new DeleteShardCommand(cluster, shard), ct);
    if (result.IsSuccess)
        return Results.NoContent();

    return result.Error switch
    {
        ClusterNotFoundException or ShardNotFoundException => Results.Problem(
            statusCode: StatusCodes.Status404NotFound, title: "Not found", detail: result.Error.Message),
        ClusterNotActiveException or ShardRemoveBlockedException => Results.Problem(
            statusCode: StatusCodes.Status409Conflict, title: "Shard remove rejected", detail: result.Error.Message),
        EtcdWriteUnavailableException or ShardPrecheckUnavailableException => Results.Problem(
            statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable", detail: result.Error.Message),
        _ => Results.Problem(
            statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed", detail: result.Error!.Message),
    };
});
```

`ClusterDetailsQuery.cs`: `ShardDto` += поле `string State` (после `MasterLeaseAlive` или в конце — порядок полей несущественен для JSON); добавить хелпер:

```csharp
// Канон state шарда (arch/03 §2, t06): отсутствие ключа = ACTIVE.
public static class ShardStates
{
    public static string Name(ShardState state)
        => state == ShardState.ToRemove ? "TO_REMOVE" : "ACTIVE";
}
```

В `ClusterDetailsMapper.Map` — в `new ShardDto(…)` добавить `ShardStates.Name(s.State)`.

- [ ] **Step 6: Тесты маппера + все unit + сборка**

В `ClustersMappersTests` — кейс: ShardInfo с `State: ShardState.ToRemove` → `ShardDto.State == "TO_REMOVE"`; без — `"ACTIVE"`.

Run: `cd /Users/demakaev/ZCodeProject/worktrees/ap-t06-shard-autoscaling && dotnet build src/AdminPanel.slnx -c Release && dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj -c Release`
Expected: PASS, 0 warnings.

- [ ] **Step 7: Commit**

```bash
git add src/AdminPanel.Api/Operations/AddShardCommand.cs src/AdminPanel.Api/Operations/DeleteShardCommand.cs src/AdminPanel.Api/Operations/OperationsModule.cs src/AdminPanel.Api/Inspection/ClusterDetailsQuery.cs src/tests/AdminPanel.UnitTests/AddShardCommandHandlerTests.cs src/tests/AdminPanel.UnitTests/DeleteShardCommandHandlerTests.cs src/tests/AdminPanel.UnitTests/ClustersMappersTests.cs
git commit -m "t06: API — POST/DELETE шардов (клэйм/компенсация/маркер §9.5-§9.6), ShardDto.state"
```

---

### Task 15: AdminPanel — integration: протоколы мутаций шардов на реальном etcd

**Files (worktree ap):**
- Create: `ap/src/tests/AdminPanel.IntegrationTests/ShardsApiTests.cs`

**Interfaces:**
- Consumes: `AuthWebFactory`, `EtcdContainerFixture`, `EtcdSeed`, `EtcdTestHarness`, `InspectionSnapshots` (существуют); эндпоинты Task 14.
- Produces: приёмка §9.6 (клэйм-гонки, компенсация, идемпотентность маркера, 404/409-матрица).

- [ ] **Step 1: Написать тесты**

По образцу `CreateClusterApiTests`/`DeleteClusterApiTests` (`[Collection("api")]`, `IClassFixture<EtcdContainerFixture>`, `SetLiveSnapshot`, логин `ApiTestLogin.LoginAsync`; сид Active-кластера shop — config без state, 2 шарда с replicas/nodes, routing 4+2 — напрямую через gateway):

```csharp
[Fact] AddShard_WithoutCookie_Returns401
[Fact] AddShard_ActiveCluster_Returns201AndWritesContractKeys
    // POST { replicas=2, requestCpu=0.5, requestMem=8, requestDisk=100 } → 201,
    // name=="shard3"; реальный range: ключи §9.5 1:1 (replicas + 2 nodes + 3 request_*),
    // ЗНАЧЕНИЯ: nodes NOT_INITIALIZED, cpu "0.5", mem "8Gi", disk "100Gi";
    // routing НЕ дописан, config не изменён (сравнить до/после)
[Fact] AddShard_ConcurrentPosts_One201Other409
    // два ПАРАЛЛЕЛЬНЫХ POST (Task.WhenAll): клэйм-txn атомарен — один 201,
    // другой 409 Shard add rejected (spec §8: «конкурентные POST → один 201, другой 409»)
[Fact] AddShard_FailedCompensationLeftovers_RepeatGets409
    // сид «провалившейся компенсации»: replicas-ключ shard3 есть, nodes/request_*
    // НЕТ (частичная декларация §9.5) → повторный POST вычислит ТО ЖЕ имя (max+1)
    // и проиграет клэйм → 409 (молча создать «другой» шард повтор не может;
    // остатки — ручной разбор etcdctl)
[Fact] AddShard_ClusterNotInitialized_Returns409
[Fact] AddShard_ClusterNotFound_Returns404
[Fact] AddShard_InvalidBody_Returns400WithFieldErrors
[Fact] DeleteShard_EmptyShard_PutsMarkerAndReturns204
    // routing НЕ указывает на shard2 → 204; реальный get: state=="TO_REMOVE"
[Fact] DeleteShard_Idempotent_SecondCall204WithoutExtraPut
    // второй DELETE → 204 (значение то же; повторная запись не нужна — проверить
    // по mod_revision или просто 204)
[Fact] DeleteShard_ShardWithBuckets_Returns409WithCount
    // routing указывает на shard1 → 409 ProblemDetails detail содержит "4" и "перевезите";
    // маркер в etcd ОТСУТСТВУЕТ (Д4: быстрая проверка до записи)
[Fact] DeleteShard_LastShard_Returns409
    // сид кластера с 1 шардом → 409
[Fact] DeleteShard_UnknownShard_Returns404
[Fact] DeleteShard_ClusterNotActive_Returns409
[Fact] DeleteShard_WithoutCookie_Returns401
```

- [ ] **Step 2: Прогнать (Docker)**

Run: `cd /Users/demakaev/ZCodeProject/worktrees/ap-t06-shard-autoscaling && dotnet test src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~ShardsApiTests"`
Expected: PASS.

- [ ] **Step 3: Весь integration-набор AP**

Run: `dotnet test src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj -c Release`
Expected: PASS — прежние тесты не сломаны.

- [ ] **Step 4: Commit**

```bash
git add src/tests/AdminPanel.IntegrationTests/ShardsApiTests.cs
git commit -m "t06: integration — клэйм/компенсация/идемпотентность маркера/матрица 404-409 (§9.6)"
```

---

### Task 16: AdminPanel — frontend: кнопки, форма, бейдж

**Files (worktree ap):**
- Modify: `ap/frontend/src/api/dto.ts` (ShardStateName, ShardDto.state, AddShardRequestDto, ShardAddedDto)
- Modify: `ap/frontend/src/api/queries.ts` (addShard/removeShard)
- Create: `ap/frontend/src/pages/cluster-details/AddShardModal.tsx`
- Create: `ap/frontend/src/pages/cluster-details/RemoveShardButton.tsx`
- Modify: `ap/frontend/src/pages/cluster-details/ShardsTab.tsx` (бейдж + колонка действий)
- Modify: `ap/frontend/src/pages/ClusterDetailsPage.tsx` (кнопка «Добавить шард», скрытие при не-Active, передача пропсов)

**Interfaces:**
- Consumes: эндпоинты Task 14; `queryKeys.cluster(name)`/`clusters`; паттерны `ClusterCreateModal`/`DeleteClusterButton` (useMutation + блокировка `isPending`, ApiError → ProblemDetails).
- Produces: UI §6.3 spec — кнопки «Добавить шард»/«Убрать шард», бейдж «к удалению», форма без имени с подписью про пустой старт.

- [ ] **Step 1: Слои api**

`dto.ts`:

```ts
// Канон состояния шарда (t06, arch/03 §2): отсутствие ключа = ACTIVE.
export type ShardStateName = 'ACTIVE' | 'TO_REMOVE';

// ShardDto += state: ShardStateName;  (в interface ShardDto добавить поле)

// POST /api/clusters/{cluster}/shards — тело и ответ (arch/03 §1.3).
export interface AddShardRequestDto {
  replicas: number;
  requestCpu: number;
  requestMem: number;
  requestDisk: number;
}

export interface ShardAddedDto {
  cluster: string;
  name: string;
  replicas: number;
  requestCpu: string;
  requestMem: string;
  requestDisk: string;
  state: ClusterStateName;
}
```

`queries.ts` (валидация — `queryKeys.cluster(name)` и `clusters`):

```ts
// POST /api/clusters/{cluster}/shards — третья мутация панели (t06, 02 §9.5):
// шард стартует пустым; имя генерирует сервер (shard<max+1>).
export function addShard(cluster: string, request: AddShardRequestDto): Promise<ShardAddedDto> {
  return apiFetch<ShardAddedDto>(`/api/clusters/${encodeURIComponent(cluster)}/shards`,
    { method: 'POST', body: request });
}

// DELETE /api/clusters/{cluster}/shards/{shard} — маркер демонтажа TO_REMOVE
// (t06, 02 §9.6); 204 без тела; демонтаж выполняет PgWorker.
export function removeShard(cluster: string, shard: string): Promise<void> {
  return apiFetch<void>(
    `/api/clusters/${encodeURIComponent(cluster)}/shards/${encodeURIComponent(shard)}`,
    { method: 'DELETE' });
}
```

- [ ] **Step 2: AddShardModal**

`AddShardModal.tsx` — паттерн `ClusterCreateModal` (валидация-зеркало, ApiError-Alert, `loading={mutation.isPending}`), БЕЗ поля имени:

```tsx
// Форма добавления шарда (t06, arch/03 §3.2): имя генерирует сервер (shard<max+1>);
// шард стартует ПУСТЫМ — никакого перераспределения бакетов (граница t06 §2.1).
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, NumberInput, Stack, Text } from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { addShard, queryKeys } from '../../api/queries';

interface FormState { replicas: number; requestCpu: number; requestMem: number; requestDisk: number; }

const EMPTY: FormState = { replicas: 2, requestCpu: 2, requestMem: 8, requestDisk: 100 };

export function AddShardModal({ cluster, opened, onClose }: {
  cluster: string; opened: boolean; onClose: () => void;
}) {
  const queryClient = useQueryClient();
  const [form, setForm] = useState<FormState>(EMPTY);
  const [fieldErrors, setFieldErrors] = useState<Record<string, string>>({});
  const set = <K extends keyof FormState>(key: K, value: FormState[K]) =>
    setForm((f) => ({ ...f, [key]: value }));

  const mutation = useMutation({
    mutationFn: (body: FormState) => addShard(cluster, body),
    onSuccess: async () => {
      onClose();
      setForm(EMPTY);
      await queryClient.invalidateQueries({ queryKey: ['clusters'] });
      await queryClient.invalidateQueries({ queryKey: queryKeys.cluster(cluster) });
    },
  });

  // Зеркало серверной валидации §9.3 (replicas/cpu/mem/disk — те же границы).
  function validate(): boolean {
    const errors: Record<string, string> = {};
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

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <Modal opened={opened} onClose={onClose} title="Добавить шард" centered>
      <Stack gap="sm">
        <Text size="sm" c="dimmed">Имя генерируется автоматически (shard&lt;N+1&gt;).</Text>
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
        <Text size="sm" c="dimmed">
          Шард стартует пустым — перераспределение бакетов выполняется отдельными
          явными переездами (UI переездов — t07).
        </Text>
        {serverError !== null ? (
          <Alert color={serverError.status === 409 ? 'yellow' : 'red'} variant="light">
            {serverError.status === 409
              ? (serverError.detail ?? 'Кластер не Active или имя шарда занято')
              : serverError.status === 400
                ? (serverError.detail ?? 'Проверьте параметры')
                : 'etcd недоступен — повторите позже'}
          </Alert>
        ) : null}
        <Group justify="flex-end" mt="xs">
          <Button variant="default" onClick={onClose}>Отмена</Button>
          <Button loading={mutation.isPending}
            onClick={() => validate() && mutation.mutate(form)}>Добавить</Button>
        </Group>
      </Stack>
    </Modal>
  );
}
```

- [ ] **Step 3: RemoveShardButton**

`RemoveShardButton.tsx` — паттерн `DeleteClusterButton` + счётчик бакетов и серверный 409:

```tsx
// Кнопка «Убрать шард» (t06, arch/03 §3): диалог со счётчиком бакетов шарда;
// при N>0 кнопка подтверждения дизейблится («сначала перевезите бакеты»);
// серверный 409 (guard-пред-проверки Д4) показывается текстом ProblemDetails.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Badge, Button, Group, Modal, Stack, Text } from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { queryKeys, removeShard } from '../../api/queries';

export function RemoveShardButton({ cluster, shard, bucketCount }: {
  cluster: string; shard: string; bucketCount: number;
}) {
  const queryClient = useQueryClient();
  const [opened, setOpened] = useState(false);
  const mutation = useMutation({
    mutationFn: () => removeShard(cluster, shard),
    onSuccess: async () => {
      // Следующий тик refresher'а (≤3 с) подхватит маркер; бейдж перерисуется.
      setOpened(false);
      await queryClient.invalidateQueries({ queryKey: ['clusters'] });
      await queryClient.invalidateQueries({ queryKey: queryKeys.cluster(cluster) });
    },
  });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <>
      <Button color="red" variant="light" size="xs" onClick={() => setOpened(true)}>Убрать шард</Button>
      <Modal opened={opened} onClose={() => setOpened(false)} title="Убрать шард" centered>
        <Stack gap="sm">
          <Text>
            Шард <b>{shard}</b> будет помечен к удалению (<b>TO_REMOVE</b>). Демонтаж выполнит
            PgWorker — после того, как все бакеты уедут со шарда.
          </Text>
          {bucketCount > 0 ? (
            <Alert color="yellow" variant="light">
              На шарде {bucketCount} бакет(ов) — сначала явно перевезите их (UI переездов — t07)
            </Alert>
          ) : null}
          {serverError !== null ? (
            <Alert color="red" variant="light">{serverError.message}</Alert>
          ) : null}
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setOpened(false)}>Отмена</Button>
            <Button color="red" disabled={bucketCount > 0} loading={mutation.isPending}
              onClick={() => mutation.mutate()}>Убрать шард</Button>
          </Group>
        </Stack>
      </Modal>
    </>
  );
}
```

- [ ] **Step 4: ShardsTab + ClusterDetailsPage**

`ShardsTab.tsx` — props: `{ cluster: string; canScale: boolean; shards: ShardDto[]; bucketCounts: Record<string, number> }`. Изменения:
- шапка — `Group justify="space-between"`: `<Text fw={500}>Шарды</Text>` + `<AddShardModal>`-кнопка «Добавить шард» (рендерится при `canScale`);
- колонка `Действия` в таблице: `canScale ? <RemoveShardButton cluster={cluster} shard={s.name} bucketCount={bucketCounts[s.name] ?? 0} /> : null`;
- в колонке «Шард»: `{s.state === 'TO_REMOVE' ? <Badge color="red" variant="light">к удалению</Badge> : null}` рядом с именем.

`ClusterDetailsPage.tsx`:
- вычислить `const canScale = data.state === 'ACTIVE';`
- `const bucketCounts = Object.fromEntries(data.shards.map(s => [s.name, data.buckets.filter(b => b.owner === s.name).length]));`
- вкладка: `<Tabs.Panel value="shards" pt="sm"><ShardsTab cluster={data.name} canScale={canScale} shards={data.shards} bucketCounts={bucketCounts} /></Tabs.Panel>`.
Кнопки скрыты при `NOT_INITIALIZED`/`TO_REMOVE` — симметрия с «Удалить кластер» (spec §6.3).

- [ ] **Step 5: typecheck + build frontend**

Run: `cd /Users/demakaev/ZCodeProject/worktrees/ap-t06-shard-autoscaling/frontend && npm run typecheck && npm run build`
Expected: PASS — 0 ошибок TS, vite-сборка успешна.

- [ ] **Step 6: Commit**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/ap-t06-shard-autoscaling
git add frontend/src/api/dto.ts frontend/src/api/queries.ts frontend/src/pages/cluster-details/AddShardModal.tsx frontend/src/pages/cluster-details/RemoveShardButton.tsx frontend/src/pages/cluster-details/ShardsTab.tsx frontend/src/pages/ClusterDetailsPage.tsx
git commit -m "t06: frontend — кнопки add/remove шарда, форма, бейдж «к удалению» (§6.3)"
```

---

### Task 17: Финальная верификация обоих репозиториев + UI-чек-лист

**Files:** — (только проверки; при найденных дефектах — фикс в соответствующей задаче и `git commit --amend`/дополнительный `t06:`-коммит).

**Interfaces:**
- Consumes: всё выше.
- Produces: подтверждение критериев приёмки spec §9.1/§9.6.

- [ ] **Step 1: pg — полная сборка и все тесты**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t06-shard-autoscaling
dotnet build src/PgWorker.slnx -c Release   # 0 warnings
dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release
dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release   # Docker
```
Expected: все зелёные; сборка 0 warnings (§9.1).

- [ ] **Step 2: AdminPanel — полная сборка, все тесты, frontend**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/ap-t06-shard-autoscaling
dotnet build src/AdminPanel.slnx -c Release   # 0 warnings
dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj -c Release
dotnet test src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj -c Release   # Docker
cd frontend && npm run typecheck && npm run build
```
Expected: все зелёные (§9.1/§9.6).

- [ ] **Step 3: Ручной UI-чек-лист на dev-стенде (spec §8 UI)**

Поднять dev-стенд (pg: `dev-stand/compose.yaml` + `seed.sh` + PgWorker; ap: `dotnet run --project src/AdminPanel.Api`) и пройти чек-лист §6.3:

1. Cluster details Active-кластера: на вкладке «Шарды» есть кнопка «Добавить шард» и per-row «Убрать шард».
2. Модалка добавления: поля реплики/CPU/память/диск, подпись про пустой старт; отправка → через тик снапшота появляется новый шард (ноды NOT_INITIALIZED → PROVISIONING → RUNNING, появляется dsn).
3. Диалог удаления шарда с бакетами: показан счётчик, кнопка дизейблена; для пустого шарда — 204, бейдж «к удалению» появляется.
4. Серверный 409 (создать условие: бакеты на шарде при скрытой кнопке — проверить через прямой API-вызов `curl -X DELETE -b cookie …/shards/shard1`) отображается текстом ProblemDetails.
5. Кластер NOT_INITIALIZED/TO_REMOVE: кнопки add/remove скрыты.
6. Двойной клик: кнопки блокируются на время мутации (`isPending`).

Зафиксировать результат в сообщении финального ревью-запроса (файлов чек-листа не создавать).

- [ ] **Step 4: Проверка мерж-гейта roadmap**

Run: `grep -n "t06-shard-autoscaling\|t07-move-bucket-ui" /Users/demakaev/ZCodeProject/worktrees/feat-t06-shard-autoscaling/arch/roadmap/pgworker.md`
Expected: только строка `t07-move-bucket-ui`; `t06-shard-autoscaling` отсутствует (гейт §9.7 — удалён тем же набором коммитов).

- [ ] **Step 5: Итоговые коммиты (если были фиксы) и сводка веток**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t06-shard-autoscaling && git log --oneline main..feat-t06-shard-autoscaling
cd /Users/demakaev/ZCodeProject/worktrees/ap-t06-shard-autoscaling && git log --oneline main..feat-t06-shard-autoscaling
```
Expected: только `t06:`-коммиты задач 1–16; рабочие деревья чисты (`git status` пусто). Пуш/мерж — только по отдельному указанию пользователя (dev-flow: ревью → мерж).

---

## Приложение А: Сводка «spec-требование → задача»

| Spec | Задача |
|---|---|
| §4.1 декларация add (ключи) | T13 (план), T14 (протокол), T4 (guard A1 waiting-keys) |
| §4.2 маркер TO_REMOVE | T2 (парсер pg), T13 (парсер AP), T14 (PUT/идемпотентность) |
| §4.3 пишемые/удаляемые ключи PgWorker | T4 (dsn/state/dsn-put), T5 (REMOVING/del/portalloc/evacuations) |
| §4.4 guard'ы G1–G7 / guard'ы add | T5 (S1), T4 (A1) |
| §5.1 классификация scale-прохода | T3, T6 |
| §5.2 AddShardProcess A0–A6 | T4 |
| §5.3 RemoveShardProcess S0–S4 | T5 |
| §5.4 NodeSupervisor/эвакуации границы | T7 |
| §5.5 MoveProcess отказы M0 | T8 |
| §5.6 Deprovisioning чистка эвакуаций | T9 |
| §5.7 модель/парсер pg | T2 |
| §6.1 API add/delete | T14, T15 |
| §6.2 DTO/парсеры AP | T13, T14 |
| §6.3 UI | T16 (+T17 чек-лист) |
| §8 unit-таблицы | T2, T4, T5, T7, T8, T9, T13, T14 |
| §8 integration | T10 (pg), T15 (AP) |
| §8 e2e 1–5 | T11 |
| §10.1 arch pg | T1 |
| §10.2 arch AP | T12 |
| §10.3 roadmap-гейт (t06→out, t07→in) | T1 |
| §9 критерии | T17 |
