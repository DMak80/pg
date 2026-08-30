# t01-kafka-topic-lifecycle — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** создание/удаление топиков Kafka из AdminPanel через etcd-заявки `topics/<T>/desired.{create,delete}`, исполнение их KafkaWorker'ом (расширение TopicSyncProcess).

**Architecture:** два новых leaf-ключа рядом с факт-ключом топика (панель ставит клэйм-txn `version==0`, воркер после исполнения делает del); инвариант «факт-ключ `topics/<T>` целиком воркера» сохраняется. Порядок decide за тик: чистка create-заявок-мусора → delete-заявки → create-заявки → факт-синк. Arch-first: канон arch/15, arch/16, adminpanel/02, adminpanel/03 правится до кода.

**Tech Stack:** .NET 10 (`Nullable=enable`, `TreatWarningsAsErrors=true`), CPM (новых пакетов нет), Confluent.Kafka 2.14.2 (уже в CPM), React+Mantine+TanStack Query, xUnit+FluentAssertions, Testcontainers.

**Spec:** `docs/superpowers/2026-08-30-t01-kafka-topic-lifecycle/spec.md` (утверждена; план аргументируется от неё — исполнители читают оба файла).

**Ревью:** правки по итогам независимого ревью Фазы 4 (CHANGES_REQUESTED, 8 замечаний) внесены — см. секцию «Самопроверка плана» в конце.

## Global Constraints

- .NET 10, C# `LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true` — `dotnet build src/PgWorker.slnx` 0 warnings.
- Новых NuGet-пакетов нет; версии — `src/Directory.Packages.props` (CPM).
- Документация/комментарии — русский; идентификаторы — английский.
- Тесты — AAA-комментарии (`// Arrange / // Act / // Assert`).
- KafkaWorker — только docker (`deploy/docker-compose.yml`); AdminPanel — хост-процесс (`dotnet run --project src/AdminPanel.Api` + собранный SPA в `wwwroot`).
- Все корневые пути — от `/Users/demakaev/ZCodeProject/worktrees/feat-t01-kafka-topic-lifecycle/` (worktree ветки `feat-t01-kafka-topic-lifecycle`).
- Коммит на каждую задачу (в конце задачи); финальный мерж-коммит удаляет тег `t01-kafka-topic-lifecycle` из `arch/roadmap/kafkaworker.md` (roadmap-гейт).
- Английские тексты API-ошибок не используем: сообщения исключений — русские (как в существующих `KafkaCommands.cs`).

---

### Task 1: arch-канон (правки четырёх канонов, до кода)

**Вход:** spec утверждена; worktree чистый (кроме `docs/superpowers/2026-08-30-t01-kafka-topic-lifecycle/`).
**Действие (файлы):**
- Modify: `arch/15-kafka-clusters.md` (§2 таблица, §2.1 примеры, §3 протокол, §5 п.3, §6)
- Modify: `arch/16-kafkaworker.md` (вступление «Границы», §1, §2.1, §3.2, §5 D)
- Modify: `arch/adminpanel/02-etcd-contract.md` (§10.1, §10.2, §10.3, §10.4)
- Modify: `arch/adminpanel/03-panels.md` (§7: эндпоинты, DTO, UI-таблица, алерты)
**Выход:** канон описывает lifecycle-заявки; код следующих задач ссылается на него.
**Проверка:** просмотр diff; `grep -n "desired.create" arch/15-kafka-clusters.md arch/adminpanel/02-etcd-contract.md` находит строки.
**Spec:** §3 (весь), §4.1, §5.1–5.3 (канонные части), §1 п.3.

- [ ] **Step 1: arch/15 §2 — две новые строки таблицы ключей**

После строки `topics/<T>` добавить:

```markdown
| `topics/<T>/desired.create` | JSON `{"partitions":P,"replication_factor":R,"configs"?:{...},"requested_unix":T,"requested_by":"u"}` | панель (клэйм-txn `version==0`), воркер — только del | заявка создания (t01); `configs` — только управляемые (`retention.ms`, `min.insync.replicas`); отсутствие = брокерные дефолты |
| `topics/<T>/desired.delete` | JSON `{"requested_unix":T,"requested_by":"u"}` | панель (клэйм-txn `version==0`), воркер — только del | заявка удаления (деструктивная; t01) |
```

- [ ] **Step 2: arch/15 §2.1 — канонические примеры**

Добавить три примера (после примера `topics/ghost`):

```markdown
`topics/audit/desired.create`:

```json
{"partitions":12,"replication_factor":3,
 "configs":{"retention.ms":"86400000","min.insync.replicas":"2"},
 "requested_unix":1750000000,"requested_by":"admin"}
```

`topics/audit/desired.create` без начальных конфигов (брокерные дефолты):

```json
{"partitions":6,"replication_factor":3,
 "requested_unix":1750000050,"requested_by":"admin"}
```

`topics/orders/desired.delete`:

```json
{"requested_unix":1750000100,"requested_by":"admin"}
```
```

- [ ] **Step 3: arch/15 §3 — подраздел «lifecycle-заявки (create/delete)»**

В конец §3 добавить (таблица протокола + идемпотентность дословно из spec §3.2):

```markdown
### 3.1. Lifecycle-заявки создания/удаления (t01)

Ключи `topics/<T>/desired.create` / `topics/<T>/desired.delete` (§2): ставит панель
клэйм-txn `version==0` (повтор при живой заявке — 409; отмена — del ключа заявки),
воркер после исполнения/чистки делает del. Обе заявки на один `<T>` запрещены
панелью; etcd-мусор — delete авторитетен, create чистится ДО исполнения delete.

Исполнение — тем же тиком TopicSync (§16 5 D), порядок decide: чистка create
(коллизия) → delete → create → факт-синк; один топик — одно lifecycle-действие
за тик:

| Ситуация | Действие воркера |
|---|---|
| `desired.delete` + топик в факте | journal → DeleteTopics → одной txn: del `topics/<T>` (факт-ключ; живой `desired` гасится с ним) + del заявки |
| `desired.delete` + топика нет | del заявки (+ del факт-ключа, если висит missing-ключ) — «исполнено внешне» |
| `desired.create` + топика нет | journal → CreateTopics(partitions, RF, configs?) → del заявки; факт-ключ кладёт следующий автосинк-тик |
| `desired.create` + топик есть | del заявки + journal-note «уже существует, параметры не применены» |
| обе живы | del `desired.create` + journal-warning; исполняется delete |
| заявка на `__`-имя | del + журнал, не исполняя |

Идемпотентность: CreateTopics → AlreadyExists = исполнено; DeleteTopics →
TopicDoesNotExist = исполнено; отказ между мутацией и del заявки — следующий тик
сходится по факту. `missing`-семантика не меняется; create на missing-топике —
«пересоздание» (панель требует отменить живой `desired` раньше; обход etcd не
ломается: после create `missing=false`, `desired` применится штатно).
```

- [ ] **Step 4: arch/15 §5 и §6 — мелкие правки**

§5 п.3 после «реестр топиков…» дописать: «читатель реестра фильтрует leaf-ключи заявок `desired.{create,delete}` по числу сегментов (факт-ключи — 6 сегментов)». §6: в строке «Битый JSON в значении ключа» добавить `desired.create`/`desired.delete`; после строки «Неизвестный ключ» примечание: «в т.ч. неизвестный leaf под `topics/<T>/`».

- [ ] **Step 5: arch/16 — границы, §1, §2.1, §3.2, §5 D**

- Вступление «Границы (что НЕ входит)»: убрать «создание/удаление топиков из панели,» из перечисления.
- §1: в столбец панели диаграммы добавить `создание/удаление топика ──► topics/<T>/desired.{create,delete}`; в список «Панель — декларатор…» дополнить «lifecycle-заявки топиков».
- §2.1 «`auto.create.topics.enable=false` (создание топиков — явное, CLI/клиентами)» → «(создание — явное: панелью (lifecycle-заявки, 15 §3.1) или CLI/клиентами; автосоздание продюсером запрещено)».
- §3.2: строку `topics/<T>` дополнить: «+ del `topics/<T>/desired.{create,delete}` после исполнения lifecycle-заявок (одной txn с del факт-ключа при delete)».
- §5 D: дополнить абзацем «Lifecycle-заявки (15 §3.1): исполнение перед факт-синком (порядок: чистка create-коллизий → delete → create → sync), guards и идемпотентность — там же».

- [ ] **Step 6: adminpanel/02 §10 — читаемые ключи, мутации 9–12, валидация, интеракция**

- §10.1: строка `topics/<T>` → примечание «+ leaf-ключи заявок `topics/<T>/desired.{create,delete}` (arch/15 §3.1) → `KafkaTopicLifecycleTicket`».
- §10.2: 4 новые строки таблицы (9–12) с протоколами/отказами из spec §5.1 (create: клэйм-txn + развёртка дефолтов; delete: идемпотентный 204; отмены: del, 404).
- §10.3: строки валидации create из spec §5.2.
- §10.4: дополнить абзацем про lifecycle (постановка/отмена/гварды; DELETE идемпотентен — порт TO_REMOVE-семантики).

- [ ] **Step 7: adminpanel/03 §7 — эндпоинты, DTO, UI, алерты**

- Таблица эндпоинтов + 4 строки (`POST /api/kafka/clusters/{cluster}/topics` → 201|400|404|409|503; `DELETE .../topics/{topic}` → 204|404|409|503; `DELETE .../topics/{topic}/desired.create` и `.../desired.delete` → 204|404|409|503).
- DTO: `KafkaTopicDto` + поле `lifecycle` (`TopicLifecycleDto`); create-заявка без факт-ключа — «виртуальная» строка (факт-поля null/0, параметры — в lifecycle-части).
- UI-строка KafkaClusterDetails (вкладка Топики): кнопка «Создать топик» (модал, дефолты из config кластера), бейджи заявок + «Отменить заявку», красная «Удалить топик» (подтверждение с вводом имени).
- Таблица алертов + 3 строки: `kafka-topic-create-pending` (info), `kafka-topic-delete-pending` (warning), `kafka-lifecycle-stale` (warning, `StaleDesiredSec`).

- [ ] **Step 8: Commit**

```bash
git add arch/15-kafka-clusters.md arch/16-kafkaworker.md arch/adminpanel/02-etcd-contract.md arch/adminpanel/03-panels.md
git commit -m "docs(kafka): канон lifecycle-заявок топиков desired.create/delete (t01, arch-first)"
```

---

### Task 2: воркер — модель `TopicLifecycleTicket` + парсер leaf-ключей

**Вход:** Task 1 (канон arch/15 §2/§3.1).
**Действие (файлы):**
- Modify: `src/KafkaWorker.Core/Model/KafkaDomain.cs`
- Modify: `src/KafkaWorker.Etcd/Parsing/KafkaSnapshotParser.cs`
- Test: `src/tests/KafkaWorker.UnitTests/Etcd/KafkaSnapshotParserTests.cs`
**Выход:** `KafkaClusterSnapshot.LifecycleTickets` наполняется из `topics/<T>/desired.{create,delete}`; битый JSON или отсутствие `requested_unix` → parseErrors, прочие leaf → unknownKeys.
**Проверка:** `dotnet test src/tests/KafkaWorker.UnitTests --filter KafkaSnapshotParserTests` зелёный.
**Spec:** §4.2 (модель/парсер), §3.1 (формат ключей).

- [ ] **Step 1: Write the failing tests** — добавить в `KafkaSnapshotParserTests.cs`:

```csharp
[Fact]
public void Parse_LifecycleCreateTicket_FillsLifecycleTickets()
{
    // Arrange: leaf-ключ заявки создания рядом с факт-ключом топика.
    var kvs = new List<Kv>
    {
        Kv("/kafka/clusters/events/config", """{"brokers":3,"replication_factor":3,"min_insync_replicas":2,"default_partitions":12,"default_retention_ms":604800000,"created_unix":1}"""),
        Kv("/kafka/clusters/events/topics/audit/desired.create",
            """{"partitions":12,"replication_factor":3,"configs":{"retention.ms":"86400000"},"requested_unix":1750000000,"requested_by":"admin"}"""),
    };

    // Act
    var result = KafkaSnapshotParser.Parse(kvs);

    // Assert: один тикет create с полными полями; факт-топиков нет.
    var cluster = result.Value.Single(c => c.Cluster == "events");
    cluster.LifecycleTickets.Should().ContainSingle().Which.Should().BeEquivalentTo(new TopicLifecycleTicket(
        "audit", "create", 12, 3,
        new Dictionary<string, string> { ["retention.ms"] = "86400000" },
        1750000000L, "admin"));
    cluster.Topics.Should().BeEmpty();
}

[Fact]
public void Parse_LifecycleDeleteTicket_AndMalformedTicket()
{
    // Arrange: заявка удаления + битый JSON второй заявки.
    var kvs = new List<Kv>
    {
        Kv("/kafka/clusters/events/config", """{"brokers":3,"replication_factor":3,"min_insync_replicas":2,"default_partitions":12,"default_retention_ms":604800000}"""),
        Kv("/kafka/clusters/events/topics/orders/desired.delete",
            """{"requested_unix":1750000100,"requested_by":"admin"}"""),
        Kv("/kafka/clusters/events/topics/bad/desired.create", """{oops"""),
    };

    // Act
    var result = KafkaSnapshotParser.Parse(kvs);

    // Assert: валидный delete-тикет; битый — parseError, не исключение.
    var cluster = result.Value.Single(c => c.Cluster == "events");
    cluster.LifecycleTickets.Should().ContainSingle(t => t.Topic == "orders" && t.Op == "delete");
    cluster.ParseErrors.Should().Contain(e => e.Contains("topics/bad"));
}

[Fact]
public void Parse_TicketWithoutRequestedUnix_IsParseError()
{
    // Arrange: JSON валиден, но аудита requested_unix нет — заявка битая
    // (панель пишет аудит всегда; образец — ParseRotations панели).
    var kvs = new List<Kv>
    {
        Kv("/kafka/clusters/events/config", """{"brokers":1,"replication_factor":1,"min_insync_replicas":1,"default_partitions":1,"default_retention_ms":1}"""),
        Kv("/kafka/clusters/events/topics/x/desired.delete", """{"requested_by":"u"}"""),
    };

    // Act
    var result = KafkaSnapshotParser.Parse(kvs);

    // Assert: parseError, тикет не создан.
    var cluster = result.Value.Single();
    cluster.LifecycleTickets.Should().BeEmpty();
    cluster.ParseErrors.Should().Contain(e => e.Contains("topics/x"));
}

[Fact]
public void Parse_UnknownTopicsLeaf_CountsUnknownKey()
{
    // Arrange: неизвестный leaf под topics/<T>/ — не ошибка, счётчик.
    var kvs = new List<Kv>
    {
        Kv("/kafka/clusters/events/config", """{"brokers":1,"replication_factor":1,"min_insync_replicas":1,"default_partitions":1,"default_retention_ms":1}"""),
        Kv("/kafka/clusters/events/topics/x/desired.pause", "{}"),
    };

    // Act
    var result = KafkaSnapshotParser.Parse(kvs);

    // Assert
    result.Value.Single().UnknownKeys.Should().Be(1);
}
```

(Тип `Kv` — `KafkaWorker.Etcd.Client`; хелпер построения `Kv(key, value)` — по образцу существующих кейсов `KafkaSnapshotParserTests.cs`.)

- [ ] **Step 2: Run** `dotnet test src/tests/KafkaWorker.UnitTests --filter KafkaSnapshotParserTests` → FAIL (нет `TopicLifecycleTicket`/`LifecycleTickets`).

- [ ] **Step 3: Реализация**

`src/KafkaWorker.Core/Model/KafkaDomain.cs` — после `KafkaTopicReg`:

```csharp
/// <summary>Операции lifecycle-заявки топика (arch/15 §3.1).</summary>
public static class TopicLifecycleOps
{
    public const string Create = "create";
    public const string Delete = "delete";
}

/// <summary>
/// Lifecycle-заявка топика (leaf-ключ topics/&lt;T&gt;/desired.create|delete,
/// arch/15 §3.1): create — параметры создания (configs — начальные, управляемые),
/// delete — только аудит. RequestedUnix обязателен (панель пишет аудит всегда;
/// образец толерантности — KafkaRotationTicket панели).
/// </summary>
public sealed record TopicLifecycleTicket(
    string Topic,
    string Op,
    int Partitions,
    short? ReplicationFactor,
    IReadOnlyDictionary<string, string>? Configs,
    long RequestedUnix,
    string? RequestedBy);
```

`KafkaClusterSnapshot` — последним positional-параметром: `IReadOnlyList<TopicLifecycleTicket>? LifecycleTickets = null`.

`src/KafkaWorker.Etcd/Parsing/KafkaSnapshotParser.cs`:
- `ClusterAcc`: `public readonly List<(string Topic, string Op, string Raw)> LifecycleRaw = [];`
- switch — до существующего `case "topics" when segments.Length == 6`:

```csharp
case "topics" when segments.Length == 7
    && segments[5].Length > 0
    && segments[6] is "desired.create" or "desired.delete":
    acc.LifecycleRaw.Add((segments[5], segments[6] == "desired.create" ? TopicLifecycleOps.Create : TopicLifecycleOps.Delete, kv.Value));
    break;
```

- `BuildCluster`: передать `acc.LifecycleRaw.OrderBy(t => t.Topic, StringComparer.Ordinal).Select(t => BuildLifecycleTicket(acc.Name, t.Topic, t.Op, t.Raw, acc.Errors)).ToList()`; новый `BuildLifecycleTicket` — толерантный `JsonDocument.Parse` (битый JSON → `errors.Add(new(...$"/kafka/clusters/{cluster}/topics/{topic}/desired.{op}", "битый JSON заявки"))`; отсутствие числового `requested_unix` → parseError «нет поля requested_unix» — образец `ParseRotations` панели; вернуть null → фильтр OfType, как `BuildTopic`).

- [ ] **Step 4: Run** — тест зелёный; весь юнит-набор воркера не сломан: `dotnet test src/tests/KafkaWorker.UnitTests`.

- [ ] **Step 5: Commit** `git commit -am "feat(kafka): модель и парсер lifecycle-заявок топиков (воркер, arch/15 §3.1)"`

---

### Task 3: воркер — decide-ветки lifecycle (чистые функции)

**Вход:** Task 2 (`TopicLifecycleTicket` в снапшоте).
**Действие (файлы):**
- Modify: `src/KafkaWorker.Provisioning/Processes/TopicSyncDecision.cs`
- Test: `src/tests/KafkaWorker.UnitTests/Provisioning/TopicSyncDecisionTests.cs`
**Выход:** `DecideLifecycle(tickets, facts, registry)` возвращает `LifecycleDelete`/`LifecycleCreate`/`LifecycleCleanup` в порядке: чистка create-коллизий → delete → чистка delete → create.
**Проверка:** `dotnet test src/tests/KafkaWorker.UnitTests --filter TopicSyncDecisionTests` зелёный.
**Spec:** §3.2 (таблица протокола), §4.2 (TopicSyncDecision, включая missing+create).

- [ ] **Step 1: Write the failing tests** — добавить в `TopicSyncDecisionTests.cs` (AAA):

```csharp
[Fact]
public void DecideLifecycle_DeleteWithTopicInFacts_ProducesLifecycleDelete()
{
    // Arrange: delete-заявка на топик, который есть в факте Kafka.
    var tickets = new[] { new TopicLifecycleTicket("orders", TopicLifecycleOps.Delete, 0, null, null, 1, "u") };
    var facts = new[] { new TopicFact("orders", 3, 1, null) };
    var registry = new[] { new KafkaTopicReg("orders", 3, 1, null, null, null, null, 1, false) };

    // Act
    var actions = TopicSyncDecision.DecideLifecycle(tickets, facts, registry);

    // Assert
    actions.Should().ContainSingle().Which.Should().BeOfType<TopicSyncAction.LifecycleDelete>()
        .Which.Topic.Should().Be("orders");
}

[Fact]
public void DecideLifecycle_CreateWithoutTopic_ProducesLifecycleCreate()
{
    // Arrange: create-заявка, топика нет ни в факте, ни в реестре.
    var tickets = new[] { new TopicLifecycleTicket("audit", TopicLifecycleOps.Create, 12, 3,
        new Dictionary<string, string> { ["retention.ms"] = "86400000" }, 1, "u") };

    // Act
    var actions = TopicSyncDecision.DecideLifecycle(tickets, [], []);

    // Assert
    actions.Should().ContainSingle().Which.Should().BeOfType<TopicSyncAction.LifecycleCreate>()
        .Which.Should().BeEquivalentTo(new { Topic = "audit", Partitions = 12, ReplicationFactor = (short)3 });
}

[Fact]
public void DecideLifecycle_CreateWithMissingRegistryKey_ProducesLifecycleCreate()
{
    // Arrange: create-заявка при висящем missing-ключе реестра (топика нет в
    // факте — «пересоздание», arch/15 §3.1): desired у ключа уже отменён.
    var tickets = new[] { new TopicLifecycleTicket("ghost", TopicLifecycleOps.Create, 3, 1, null, 1, "u") };
    var registry = new[] { new KafkaTopicReg("ghost", 3, 1, null, null, null, null, 1, true) };

    // Act
    var actions = TopicSyncDecision.DecideLifecycle(tickets, [], registry);

    // Assert: топика нет в факте — создаём (missing-ветка автосинка снимет
    // missing следующим тиком по появившемуся факту).
    actions.Should().ContainSingle().Which.Should().BeOfType<TopicSyncAction.LifecycleCreate>()
        .Which.Topic.Should().Be("ghost");
}

[Fact]
public void DecideLifecycle_CreateWithTopicInFacts_CleanupAsAlreadyExists()
{
    // Arrange: create-заявка при живом топике (панель обошла проверку — мусор).
    var tickets = new[] { new TopicLifecycleTicket("orders", TopicLifecycleOps.Create, 6, 1, null, 1, "u") };
    var facts = new[] { new TopicFact("orders", 3, 1, null) };

    // Act
    var actions = TopicSyncDecision.DecideLifecycle(tickets, facts, []);

    // Assert: cleanup create-заявки («уже существует, параметры не применены»).
    actions.Should().ContainSingle().Which.Should().BeOfType<TopicSyncAction.LifecycleCleanup>()
        .Which.Should().BeEquivalentTo(new { Topic = "orders", Op = TopicLifecycleOps.Create });
}

[Fact]
public void DecideLifecycle_BothTickets_CleanupCreateThenDelete()
{
    // Arrange: обе заявки живы (etcd-мусор).
    var tickets = new[]
    {
        new TopicLifecycleTicket("orders", TopicLifecycleOps.Create, 6, 1, null, 1, "u"),
        new TopicLifecycleTicket("orders", TopicLifecycleOps.Delete, 0, null, null, 2, "u"),
    };
    var facts = new[] { new TopicFact("orders", 3, 1, null) };

    // Act
    var actions = TopicSyncDecision.DecideLifecycle(tickets, facts, []);

    // Assert: сначала чистка create (арх/15 §3.1 — ДО исполнения delete),
    // затем delete; create не исполняется.
    actions.Should().HaveCount(2);
    actions[0].Should().BeOfType<TopicSyncAction.LifecycleCleanup>().Which.Op.Should().Be(TopicLifecycleOps.Create);
    actions[1].Should().BeOfType<TopicSyncAction.LifecycleDelete>();
}

[Fact]
public void DecideLifecycle_DeleteWithoutTopic_CleanupExternal()
{
    // Arrange: delete-заявка, топика нет в факте (удалён CLI раньше).
    var tickets = new[] { new TopicLifecycleTicket("orders", TopicLifecycleOps.Delete, 0, null, null, 1, "u") };
    var registry = new[] { new KafkaTopicReg("orders", 3, 1, null, null, null, null, 1, true) }; // missing-ключ висит

    // Act
    var actions = TopicSyncDecision.DecideLifecycle(tickets, [], registry);

    // Assert: cleanup delete-заявки; act-ветка снесёт и missing-ключ.
    actions.Should().ContainSingle().Which.Should().BeOfType<TopicSyncAction.LifecycleCleanup>()
        .Which.Op.Should().Be(TopicLifecycleOps.Delete);
}

[Fact]
public void DecideLifecycle_InternalTopicTicket_CleanupWithoutExecution()
{
    // Arrange: заявка на __-имя — панель такие не ставит, мусор.
    var tickets = new[] { new TopicLifecycleTicket("__consumer_offsets", TopicLifecycleOps.Create, 1, 1, null, 1, "u") };

    // Act
    var actions = TopicSyncDecision.DecideLifecycle(tickets, [], []);

    // Assert
    actions.Should().ContainSingle().Which.Should().BeOfType<TopicSyncAction.LifecycleCleanup>();
}
```

- [ ] **Step 2: Run** `dotnet test src/tests/KafkaWorker.UnitTests --filter TopicSyncDecisionTests` → FAIL.

- [ ] **Step 3: Реализация в `TopicSyncDecision.cs`**

Новые action-типы (в `TopicSyncAction`):

```csharp
/// <summary>Исполнить delete-заявку: DeleteTopics → txn del факт-ключа + del заявки.</summary>
public sealed record LifecycleDelete(string Topic) : TopicSyncAction;

/// <summary>Исполнить create-заявку: CreateTopics(P, RF, configs?) → del заявки.</summary>
public sealed record LifecycleCreate(
    string Topic,
    int Partitions,
    short ReplicationFactor,
    IReadOnlyDictionary<string, string>? Configs) : TopicSyncAction;

/// <summary>
/// Чистка заявки без исполнения (arch/15 §3.1): create при живом топике /
/// delete при отсутствующем / коллизия / __-имя. Op определяет, сносит ли
/// act и факт-ключ (delete + топика нет → да).
/// </summary>
public sealed record LifecycleCleanup(string Topic, string Op, string Reason) : TopicSyncAction;
```

Статический метод (рядом с `Decide`):

```csharp
public static IReadOnlyList<TopicSyncAction> DecideLifecycle(
    IReadOnlyList<TopicLifecycleTicket> tickets,
    IReadOnlyList<TopicFact> facts,
    IReadOnlyList<KafkaTopicReg> registry)
{
    var actions = new List<TopicSyncAction>();
    var factsByTopic = facts.ToDictionary(f => f.Topic, StringComparer.Ordinal);
    var byTopic = tickets.GroupBy(t => t.Topic, StringComparer.Ordinal);

    foreach (var group in byTopic)
    {
        var delete = group.FirstOrDefault(t => t.Op == TopicLifecycleOps.Delete);
        var create = group.FirstOrDefault(t => t.Op == TopicLifecycleOps.Create);

        // Коллизия заявок (etcd-мусор): delete авторитетен — create чистится
        // ДО исполнения delete (arch/15 §3.1: «del desired.create; исполняется delete»).
        if (delete is not null && create is not null)
            actions.Add(new TopicSyncAction.LifecycleCleanup(
                group.Key, TopicLifecycleOps.Create, "коллизия с delete-заявкой — delete авторитетен"));

        if (delete is not null)
        {
            if (factsByTopic.ContainsKey(group.Key))
                actions.Add(new TopicSyncAction.LifecycleDelete(group.Key));
            else
                actions.Add(new TopicSyncAction.LifecycleCleanup(
                    group.Key, TopicLifecycleOps.Delete, "топика нет в Kafka — исполнено внешне"));
            continue;
        }

        if (create is not null)
        {
            if (IsInternal(group.Key))
                actions.Add(new TopicSyncAction.LifecycleCleanup(
                    group.Key, TopicLifecycleOps.Create, "internal-топик __* — не исполняется"));
            else if (factsByTopic.ContainsKey(group.Key))
                actions.Add(new TopicSyncAction.LifecycleCleanup(
                    group.Key, TopicLifecycleOps.Create, "топик уже существует — параметры заявки не применяются"));
            else
                actions.Add(new TopicSyncAction.LifecycleCreate(
                    group.Key, create.Partitions, create.ReplicationFactor ?? (short)1, create.Configs));
        }
    }

    // Порядок исполнения за тик (arch/15 §3.1): сначала чистка create-заявок
    // (в т.ч. коллизия — до delete), затем delete-действия и чистка delete-заявок,
    // затем создание. Факт-синк процесса идёт после всех lifecycle-действий.
    return actions
        .OrderBy(a => a switch
        {
            TopicSyncAction.LifecycleCleanup { Op: TopicLifecycleOps.Create } => 0,
            TopicSyncAction.LifecycleDelete => 1,
            TopicSyncAction.LifecycleCleanup { Op: TopicLifecycleOps.Delete } => 2,
            TopicSyncAction.LifecycleCreate => 3,
            _ => 4,
        })
        .ToList();
}
```

(`IsInternal` уже есть в `Decide` как local function — вынести в private static метод класса, использовать в обоих.)

- [ ] **Step 4: Run** — тесты зелёные; весь юнит-набор: `dotnet test src/tests/KafkaWorker.UnitTests`.

- [ ] **Step 5: Commit** `git commit -am "feat(kafka): decide-ветки lifecycle-заявок топиков (чистка create → delete → create, guards, arch/15 §3.1)"`

---

### Task 4: воркер — seam-методы + act-ветки TopicSyncProcess

**Вход:** Task 3 (action-типы); фейк `FakeKafkaAdminClient` существует.
**Действие (файлы):**
- Modify: `src/KafkaWorker.Provisioning/Kafka/IKafkaAdminClient.cs`
- Modify: `src/KafkaWorker.Provisioning/Kafka/KafkaAdminClient.cs`
- Modify: `src/KafkaWorker.Provisioning/Processes/TopicSyncProcess.cs`
- Modify: `src/tests/KafkaWorker.UnitTests/Provisioning/FakeKafkaAdminClient.cs`
- Test: `src/tests/KafkaWorker.UnitTests/Provisioning/TopicSyncProcessTests.cs`
**Выход:** процесс исполняет lifecycle-действия (journal → Kafka-мутация → txn del), транзиенты ретраятся тиком; AlreadyExists/NotFound — не ошибки.
**Проверка:** `dotnet test src/tests/KafkaWorker.UnitTests --filter TopicSyncProcessTests` зелёный; build 0 warnings.
**Spec:** §3.2, §4.2 (IKafkaAdminClient/TopicSyncProcess/адаптер).

- [ ] **Step 1: Write the failing tests** — добавить в `TopicSyncProcessTests.cs` (использовать существующий `Rig`/`NewRigAsync`/`SeedTopicFact`):

```csharp
[Fact]
public async Task LifecycleCreateTicket_CreatesTopicAndRemovesTicket()
{
    // Arrange: create-заявка на несуществующий топик; fact-ключа нет.
    var rig = await NewRigAsync();
    rig.Etcd.Seed("/kafka/clusters/events/topics/audit/desired.create",
        """{"partitions":12,"replication_factor":1,"configs":{"retention.ms":"86400000"},"requested_unix":1750000000,"requested_by":"admin"}""");

    // Act
    var result = await rig.Process.RunAsync(await rig.Snapshot(), CancellationToken.None);

    // Assert: CreateTopics вызван с параметрами заявки; заявка удалена;
    // факт-ключ появится следующим автосинк-тиком (топик теперь в fake).
    result.IsSuccess.Should().BeTrue();
    rig.Admin.CreatedTopics.Should().ContainSingle().Which.Should().BeEquivalentTo(
        new { Topic = "audit", Partitions = 12, ReplicationFactor = (short)1 });
    (await rig.Etcd.GetAsync(Ep, "/kafka/clusters/events/topics/audit/desired.create", CancellationToken.None))
        .Value.Should().BeNull();
}

[Fact]
public async Task LifecycleCreateTicket_TopicAlreadyExists_CleansTicketWithoutCreate()
{
    // Arrange: топик уже в факте Kafka (создан параллельно CLI) + живая create-заявка.
    var rig = await NewRigAsync();
    SeedTopicFact(rig.Admin, "orders");
    rig.Etcd.Seed("/kafka/clusters/events/topics/orders/desired.create",
        """{"partitions":6,"replication_factor":1,"requested_unix":1750000000,"requested_by":"admin"}""");

    // Act
    var result = await rig.Process.RunAsync(await rig.Snapshot(), CancellationToken.None);

    // Assert: повторный create НЕ вызван (идемпотентность AlreadyExists решена на нашей стороне).
    result.IsSuccess.Should().BeTrue();
    rig.Admin.CreatedTopics.Should().BeEmpty();
    (await rig.Etcd.GetAsync(Ep, "/kafka/clusters/events/topics/orders/desired.create", CancellationToken.None))
        .Value.Should().BeNull();
}

[Fact]
public async Task LifecycleDeleteTicket_DeletesTopicRegistryKeyAndTicket()
{
    // Arrange: факт-ключ + delete-заявка на живой топик.
    var rig = await NewRigAsync();
    SeedTopicFact(rig.Admin, "orders");
    SeedRegistry(rig.Etcd);
    rig.Etcd.Seed("/kafka/clusters/events/topics/orders/desired.delete",
        """{"requested_unix":1750000100,"requested_by":"admin"}""");

    // Act
    var result = await rig.Process.RunAsync(await rig.Snapshot(), CancellationToken.None);

    // Assert: DeleteTopics вызван; одной txn снесены факт-ключ и заявка.
    result.IsSuccess.Should().BeTrue();
    rig.Admin.DeletedTopics.Should().Contain("orders");
    (await rig.Etcd.GetAsync(Ep, "/kafka/clusters/events/topics/orders", CancellationToken.None)).Value.Should().BeNull();
    (await rig.Etcd.GetAsync(Ep, "/kafka/clusters/events/topics/orders/desired.delete", CancellationToken.None)).Value.Should().BeNull();
}

[Fact]
public async Task LifecycleDeleteTicket_TransientFailure_RetriedNextTick()
{
    // Arrange: DeleteTopics падает дольше jitter-ретраев одного тика
    // (3 попытки — образец AlterTopicFailCount в существующих тестах).
    var rig = await NewRigAsync();
    SeedTopicFact(rig.Admin, "orders");
    SeedRegistry(rig.Etcd);
    rig.Etcd.Seed("/kafka/clusters/events/topics/orders/desired.delete",
        """{"requested_unix":1,"requested_by":"u"}""");
    rig.Admin.DeleteTopicFailCount = 3;

    // Act
    var first = await rig.Process.RunAsync(await rig.Snapshot(), CancellationToken.None);
    var second = await rig.Process.RunAsync(await rig.Snapshot(), CancellationToken.None);

    // Assert: первый тик неуспешен (заявка жива), второй доводит.
    first.IsSuccess.Should().BeFalse();
    second.IsSuccess.Should().BeTrue();
    rig.Admin.DeletedTopics.Should().Contain("orders");
}
```

- [ ] **Step 2: Run** `dotnet test src/tests/KafkaWorker.UnitTests --filter TopicSyncProcessTests` → FAIL (нет методов у фейка/процесса).

- [ ] **Step 3: Seam — `IKafkaAdminClient.cs`**

```csharp
// Исход lifecycle-операции: адаптер классифицирует отчёты Confluent,
// процессы не парсят строки ошибок (arch/15 §3.1).
public enum TopicCreateOutcome { Created, AlreadyExists }
public enum TopicDeleteOutcome { Deleted, NotFound }

public interface IKafkaAdminClient : IAsyncDisposable
{
    // ... существующие методы без изменений ...

    // Создание топика с начальными управляемыми конфигами (lifecycle create, t01).
    Task<Result<TopicCreateOutcome>> CreateTopicAsync(
        string topic, int partitions, short replicationFactor,
        IReadOnlyDictionary<string, string>? configs, CancellationToken ct);

    // Удаление топика (lifecycle delete, t01).
    Task<Result<TopicDeleteOutcome>> DeleteTopicAsync(string topic, CancellationToken ct);
}
```

- [ ] **Step 4: Адаптер — `KafkaAdminClient.cs`**

```csharp
public Task<Result<TopicCreateOutcome>> CreateTopicAsync(
    string topic, int partitions, short replicationFactor,
    IReadOnlyDictionary<string, string>? configs, CancellationToken ct)
    => RunAsync<TopicCreateOutcome>(async client =>
    {
        var spec = new TopicsSpecification
        {
            Name = topic,
            NumPartitions = partitions,
            ReplicationFactor = replicationFactor,
        };
        if (configs is { Count: > 0 })
            spec.Configs = configs.Select(p => new ConfigEntry { Name = p.Key, Value = p.Value }).ToList();

        var reports = await client.CreateTopicsAsync(
            [spec], new CreateTopicsOptions { RequestTimeout = requestTimeout });
        var error = reports.FirstOrDefault(r => r.Error.IsError);
        if (error is not null && error.Error.Code == ErrorCode.TopicAlreadyExists)
            return TopicCreateOutcome.AlreadyExists; // идемпотентность: исполнено (§3.1)
        if (error is not null)
            throw new KafkaException(error.Error);

        return TopicCreateOutcome.Created;
    }, ct);

public Task<Result<TopicDeleteOutcome>> DeleteTopicAsync(string topic, CancellationToken ct)
    => RunAsync<TopicDeleteOutcome>(async client =>
    {
        var reports = await client.DeleteTopicsAsync(
            [topic], new DeleteTopicsOptions { RequestTimeout = requestTimeout });
        var error = reports.FirstOrDefault(r => r.Error.IsError);
        if (error is not null && error.Error.Code == ErrorCode.UnknownTopicOrPartition)
            return TopicDeleteOutcome.NotFound; // идемпотентность: исполнено (§3.1)
        if (error is not null)
            throw new KafkaException(error.Error);

        return TopicDeleteOutcome.Deleted;
    }, ct);
```

(Если версия Confluent бросает `KafkaException` вместо ошибочного report — обернуть в try/catch по `ErrorCode.TopicAlreadyExists`/`UnknownTopicOrPartition` с теми же исходами; канон исхода неизменен.)

- [ ] **Step 5: Фейк — `FakeKafkaAdminClient.cs`**

```csharp
// Lifecycle-журнал вызовов (t01): создание/удаление + транзиенты.
public List<(string Topic, int Partitions, short ReplicationFactor, IReadOnlyDictionary<string, string>? Configs)> CreatedTopics = [];
public List<string> DeletedTopics = [];
public int CreateTopicFailCount { get; set; }
public int DeleteTopicFailCount { get; set; }
private int _createFails, _deleteFails;

public Task<Result<TopicCreateOutcome>> CreateTopicAsync(
    string topic, int partitions, short replicationFactor,
    IReadOnlyDictionary<string, string>? configs, CancellationToken ct)
{
    CallLog.Add($"create-topic:{topic}");
    if (_createFails++ < CreateTopicFailCount)
        return Task.FromResult(Result<TopicCreateOutcome>.Failed(new ApplicationException("create transient")));

    if (Topics is not null && Topics.Any(t => t.Topic == topic))
        return Task.FromResult(Result<TopicCreateOutcome>.Success(TopicCreateOutcome.AlreadyExists));

    var views = (Topics ?? []).ToList();
    views.Add(new KafkaTopicView(topic, partitions,
        Enumerable.Repeat((IReadOnlyList<int>)[1], partitions).ToList()));
    Topics = views;
    CreatedTopics.Add((topic, partitions, replicationFactor, configs));
    return Task.FromResult(Result<TopicCreateOutcome>.Success(TopicCreateOutcome.Created));
}

public Task<Result<TopicDeleteOutcome>> DeleteTopicAsync(string topic, CancellationToken ct)
{
    CallLog.Add($"delete-topic:{topic}");
    if (_deleteFails++ < DeleteTopicFailCount)
        return Task.FromResult(Result<TopicDeleteOutcome>.Failed(new ApplicationException("delete transient")));

    if (Topics is not null && Topics.Any(t => t.Topic == topic))
    {
        Topics = Topics.Where(t => !string.Equals(t.Topic, topic, StringComparison.Ordinal)).ToList();
        DeletedTopics.Add(topic);
        return Task.FromResult(Result<TopicDeleteOutcome>.Success(TopicDeleteOutcome.Deleted));
    }

    return Task.FromResult(Result<TopicDeleteOutcome>.Success(TopicDeleteOutcome.NotFound));
}
```

- [ ] **Step 6: Процесс — `TopicSyncProcess.cs`**

В `RunAsync` после `DescribeFactsAsync` (до `TopicSyncDecision.Decide`):

```csharp
// Lifecycle-заявки — до факт-синка (порядок §3.1: чистка create → delete → create → sync).
var lifecycle = TopicSyncDecision.DecideLifecycle(
    snap.LifecycleTickets ?? [], facts.Value, snap.Topics);
foreach (var action in lifecycle)
{
    var applied = await ActLifecycleAsync(snap, action, ct);
    if (!applied.IsSuccess)
        return applied; // транзиент: заявка жива, следующий тик повторит
}
```

Новый private-метод `ActLifecycleAsync` (failover-обёртки/`WithJitterRetryAsync`/`TopicKey` уже есть):

```csharp
// Исполнение lifecycle-действий (arch/15 §3.1): journal → Kafka-мутация →
// txn-чистка (del заявки по mod_revision; при delete — вместе с факт-ключом).
private async Task<Result> ActLifecycleAsync(KafkaClusterSnapshot snap, TopicSyncAction action, CancellationToken ct)
{
    var cluster = snap.Cluster;
    switch (action)
    {
        case TopicSyncAction.LifecycleDelete del:
        {
            var ticketKey = LifecycleKey(cluster, del.Topic, TopicLifecycleOps.Delete);
            await using var admin = adminFactory.Create(snap.Endpoints!, snap.AppUser!, snap.AppPassword!);
            var journaled = await journal.WriteAsync(cluster, Op, $"deleting-topic:{del.Topic}", claims.InstanceId, null, ct);
            if (!journaled.IsSuccess)
                return journaled;

            var deleted = await WithJitterRetryAsync(() => admin.DeleteTopicAsync(del.Topic, ct));
            if (!deleted.IsSuccess)
                return deleted; // транзиент — заявка жива, тик повторит

            return await DeleteKeysAsync(
                [TopicKey(cluster, del.Topic), ticketKey], ticketKey, ct);
        }

        case TopicSyncAction.LifecycleCreate create:
        {
            var ticketKey = LifecycleKey(cluster, create.Topic, TopicLifecycleOps.Create);
            await using var admin = adminFactory.Create(snap.Endpoints!, snap.AppUser!, snap.AppPassword!);
            var journaled = await journal.WriteAsync(cluster, Op, $"creating-topic:{create.Topic}", claims.InstanceId, null, ct);
            if (!journaled.IsSuccess)
                return journaled;

            var created = await WithJitterRetryAsync(
                () => admin.CreateTopicAsync(create.Topic, create.Partitions, create.ReplicationFactor, create.Configs, ct));
            if (!created.IsSuccess)
                return created;

            // AlreadyExists = исполнено ранее; факт-ключ положит автосинк (§3.1).
            return await DeleteKeysAsync([ticketKey], ticketKey, ct);
        }

        case TopicSyncAction.LifecycleCleanup cleanup:
        {
            // Чистка без исполнения: журнал-примечание + del заявки (для
            // delete-ветки при отсутствующем топике — снести и missing-ключ).
            var journaled = await journal.WriteAsync(cluster, Op, $"ticket-cleanup:{cleanup.Topic}", claims.InstanceId, cleanup.Reason, ct);
            if (!journaled.IsSuccess)
                return journaled;

            var keys = new List<string> { LifecycleKey(cluster, cleanup.Topic, cleanup.Op) };
            if (cleanup.Op == TopicLifecycleOps.Delete)
                keys.Add(TopicKey(cluster, cleanup.Topic)); // missing-ключ висит без топика
            return await DeleteKeysAsync([.. keys], keys[0], ct);
        }

        default:
            return Result.Failed(new ApplicationException(
                $"topicsync {cluster}: неизвестное lifecycle-действие {action.GetType().Name}"));
    }
}

// txn-удаление группы ключей с compare по mod_revision первого (заявки);
// проигрыш compare — не ошибка (следующий тик).
private async Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, string compareKey, CancellationToken ct)
{
    var fresh = await GetAsync(compareKey, ct);
    if (!fresh.IsSuccess)
        return fresh;
    if (fresh.Value is null)
        return Result.Success(); // заявку уже снесли — идемпотентность

    var ops = keys.Select(k => new TxnOp.Delete(k, Prefix: false)).ToList();
    var txn = await TxnAsync(
        TxnRequest.Of([TxnCompare.ModRevisionEqual(compareKey, (long)fresh.Value.ModRevision)], ops), ct);
    if (!txn.IsSuccess)
        return txn;
    return Result.Success();
}

private static string LifecycleKey(string cluster, string topic, string op)
    => $"/kafka/clusters/{cluster}/topics/{topic}/desired.{op}";
```

(`journal`/`claims` — параметры конструктора процесса; в switch-ветках используются напрямую, без локалей с теми же именами.)

- [ ] **Step 7: Run** `dotnet test src/tests/KafkaWorker.UnitTests --filter TopicSyncProcessTests` → PASS; затем весь набор: `dotnet test src/tests/KafkaWorker.UnitTests` и `dotnet build src/PgWorker.slnx` (0 warnings).

- [ ] **Step 8: Commit** `git commit -am "feat(kafka): исполнение lifecycle-заявок топиков воркером (CreateTopics/DeleteTopics, txn-чистка, arch/15 §3.1)"`

---

### Task 5: воркер — интеграционные тесты (Testcontainers)

**Вход:** Task 4; интеграционный харнесс `src/tests/KafkaWorker.IntegrationTests/Kafka/` (фичи `KafkaClusterFixture`/`TopicSyncTests`) существует.
**Действие (файлы):**
- Test: `src/tests/KafkaWorker.IntegrationTests/Kafka/TopicLifecycleTests.cs`
**Выход:** lifecycle против реального Kafka+etcd: create/delete/идемпотентность/сходимость обеих веток после «отказа между мутацией и del».
**Проверка:** `dotnet test src/tests/KafkaWorker.IntegrationTests --filter TopicLifecycleTests` (требует Docker).
**Spec:** §4.2 (интеграционные кейсы), §9.2/9.3/9.5.

- [ ] **Step 1: Тесты (AAA; поднять кластер и процесс по образцу `TopicSyncTests`)**

```csharp
// Кейсы (все — Arrange из харнесса TopicSyncTests: 1-брокерный кластер, воркер-процесс):
// 1) CreateTicket_ExecutesAgainstRealKafka: сид desired.create (2 партиции,
//    RF 1, retention 1д) → RunAsync → топик в Kafka с теми же параметрами
//    (DescribeTopics/DescribeTopicConfigs), заявка снята; повторный RunAsync
//    кладёт факт-ключ topics/<T> (partitions 2, без заявок).
// 2) DeleteTicket_RemovesTopicAndKeys: сид факт-ключа + desired.delete →
//    RunAsync → топика нет в Kafka (метаданные), оба ключа etcd удалены.
// 3) CreateTicket_TopicCreatedByCliConcurrently_CleansTicket: создать топик
//    AdminClient напрямую, затем сид create-заявки с другими параметрами →
//    RunAsync → топик НЕ пересоздан (параметры исходные), заявка снята.
//    (Сходимость create-ветки после «отказа между мутацией и del».)
// 4) DeleteTicket_TopicDeletedExternally_CleansTicketWithoutError: сид
//    факт-ключа + desired.delete; удалить топик напрямую AdminClient'ом
//    (имитация «DeleteTopics прошёл, del заявки не успел») → RunAsync →
//    успех (NotFound = исполнено), заявка и факт-ключ удалены, ошибок нет.
//    (Сходимость delete-ветки — spec §4.2.)
```

Конкретные asserts — по образцу существующего `TopicSyncTests.cs` (его хелперы `seed`/`admin describe` переиспользовать; каждый тест — AAA-комментарии).

- [ ] **Step 2: Run** `dotnet test src/tests/KafkaWorker.IntegrationTests --filter TopicLifecycleTests` → PASS.

- [ ] **Step 3: Commit** `git commit -am "test(kafka): интеграция lifecycle-заявок против реального Kafka (create/delete/сходимость обеих веток)"`

---

### Task 6: панель — парсер и модель заявок

**Вход:** Task 1 (канон adminpanel/02 §10.1).
**Действие (файлы):**
- Modify: `src/AdminPanel.Core/Kafka/KafkaSnapshot.cs`
- Modify: `src/AdminPanel.Etcd/Parsing/KafkaParser.cs`
- Test: `src/tests/AdminPanel.UnitTests/KafkaParserTests.cs`
**Выход:** `KafkaClusterInfo.LifecycleTickets` наполняется; битый JSON → parseError.
**Проверка:** `dotnet test src/tests/AdminPanel.UnitTests --filter KafkaParserTests` зелёный.
**Spec:** §5.3 (парсер).

- [ ] **Step 1: Write the failing tests** — в `KafkaParserTests.cs` (AAA) по образцу Task 2: create-тикет с configs → поля `Partitions=12, ReplicationFactor=3, RetentionMs=86400000, MinInSyncReplicas=null`; delete-тикет; битый JSON → parseErrors; неизвестный leaf → unknownKeys.

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Реализация**

`KafkaSnapshot.cs` — после `KafkaRotationTicket`:

```csharp
// Lifecycle-заявка топика topics/<T>/desired.{create,delete} (arch/15 §3.1):
// create — параметры (configs развёрнуты в типизированные поля), delete — аудит.
public sealed record KafkaTopicLifecycleTicket(
    string Topic,
    string Op,                 // "create" | "delete" (raw-строка, толерантно)
    int? Partitions,
    short? ReplicationFactor,
    long? RetentionMs,
    short? MinInSyncReplicas,
    long RequestedUnix,
    string? RequestedBy);
```

`KafkaClusterInfo` — последним параметром: `IReadOnlyList<KafkaTopicLifecycleTicket>? LifecycleTickets = null`.

`KafkaParser.cs`: в `ClusterAcc` + `List<(string Topic, string Op, string Raw)> LifecycleRaw`; switch — ветка 7 сегментов (`desired.create`/`desired.delete`), прочие leaf под `topics/` → unknown; `BuildCluster` → `BuildLifecycleTicket` (толерантный разбор JSON: `partitions`/`replication_factor`/`configs.retention.ms`/`configs.min.insync.replicas`/`requested_unix` (обязателен — иначе parseError, образец `ParseRotations`)/`requested_by`).

- [ ] **Step 4: Run** → PASS; весь набор: `dotnet test src/tests/AdminPanel.UnitTests`.

- [ ] **Step 5: Commit** `git commit -am "feat(panel): парсер lifecycle-заявок топиков (KafkaSnapshot, arch/02 §10.1)"`

---

### Task 7: панель — writing: request/валидатор/план JSON заявок

**Вход:** Task 1; `KafkaWriting.cs` (существующие `KafkaLimits`/`KafkaTopicDesiredPlan`).
**Действие (файлы):**
- Modify: `src/AdminPanel.Etcd/Writing/KafkaWriting.cs`
- Test: `src/tests/AdminPanel.UnitTests/KafkaWritingPlanTests.cs`
**Выход:** `CreateTopicRequest` + `KafkaTopicCreateValidator.Validate(request, config)` (чистая валидация) + `KafkaTopicCreatePlan.Build(...)` (построение заявки) + `TopicLifecycleCreateJson`/`TopicLifecycleDeleteJson` (каноническая сериализация). Разделение имён — по прецеденту `KafkaCreateValidator`/`KafkaClusterCreatePlan`.
**Проверка:** `dotnet test src/tests/AdminPanel.UnitTests --filter KafkaWritingPlanTests` зелёный.
**Spec:** §5.1 (мутация 9: развёртка дефолтов), §5.2 (валидация), §3.1 (JSON).

- [ ] **Step 1: Write the failing tests** — в `KafkaWritingPlanTests.cs` (AAA):

```csharp
[Fact]
public void Validate_FullBodyWithDefaults_EffectiveFromClusterConfig()
{
    // Arrange: тело без partitions/RF — дефолты из config кластера (12/3);
    // minISR 5 > эффективного RF 3 — ошибка.
    var request = new CreateTopicRequest("audit", null, null, 86400000L, 5);
    var config = new KafkaConfigJson(3, 3, 2, 12, 604800000L, 1, null);

    // Act
    var errors = KafkaTopicCreateValidator.Validate(request, config);

    // Assert: единственная ошибка — minISR > RF.
    errors.Should().ContainSingle().Which.Field.Should().Be("minInSyncReplicas");
}

[Fact]
public void Validate_RfAboveClusterBrokers_Rejected()
{
    // Arrange: RF 4 при brokers 3.
    var request = new CreateTopicRequest("audit", 6, 4, null, null);
    var config = new KafkaConfigJson(3, 3, 2, 12, 604800000L, 1, null);

    // Act
    var errors = KafkaTopicCreateValidator.Validate(request, config);

    // Assert
    errors.Should().ContainSingle().Which.Field.Should().Be("replicationFactor");
}

[Fact]
public void Validate_BadNameOrInternal_Rejected()
{
    // Arrange: __-префикс и пустое имя.
    var config = new KafkaConfigJson(3, 3, 2, 12, 604800000L, 1, null);

    // Act / Assert
    KafkaTopicCreateValidator.Validate(new("__x", 1, 1, null, null), config)
        .Should().Contain(e => e.Field == "name");
    KafkaTopicCreateValidator.Validate(new("", 1, 1, null, null), config)
        .Should().Contain(e => e.Field == "name");
}

[Fact]
public void BuildCreateJson_CanonicalShape()
{
    // Arrange: полный запрос + config.
    var request = new CreateTopicRequest("audit", 6, 2, 86400000L, 2);
    var config = new KafkaConfigJson(3, 3, 2, 12, 604800000L, 1, null);

    // Act
    var json = KafkaTopicCreatePlan.Build(request, config, 1750000000L, "admin").Serialize();

    // Assert: канон arch/15 §2.1 (значения явные — дефолты не подставлялись).
    using var doc = JsonDocument.Parse(json);
    doc.RootElement.GetProperty("partitions").GetInt32().Should().Be(6);
    doc.RootElement.GetProperty("replication_factor").GetInt32().Should().Be(2);
    doc.RootElement.GetProperty("configs").GetProperty("retention.ms").GetString().Should().Be("86400000");
    doc.RootElement.GetProperty("requested_by").GetString().Should().Be("admin");
}

[Fact]
public void BuildCreateJson_NoConfigs_OmitsConfigsField()
{
    // Arrange: без retention/minISR → брокерные дефолты (поле отсутствует).
    var request = new CreateTopicRequest("audit", null, null, null, null);
    var config = new KafkaConfigJson(3, 3, 2, 12, 604800000L, 1, null);

    // Act
    var json = KafkaTopicCreatePlan.Build(request, config, 1L, "u").Serialize();

    // Assert
    using var doc = JsonDocument.Parse(json);
    doc.RootElement.TryGetProperty("configs", out _).Should().BeFalse();
    doc.RootElement.GetProperty("partitions").GetInt32().Should().Be(12); // дефолт config
    doc.RootElement.GetProperty("replication_factor").GetInt32().Should().Be(3);
}
```

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Реализация в `KafkaWriting.cs`**

```csharp
// Тело POST /api/kafka/clusters/{c}/topics (arch/02 §10.2-9): name обязателен;
// partitions/RF дефолтятся из config кластера; retention/minISR опциональны.
public sealed record CreateTopicRequest(
    string? Name,
    int? Partitions = null,
    short? ReplicationFactor = null,
    long? RetentionMs = null,
    short? MinInSyncReplicas = null);

// Значение заявки topics/<T>/desired.create (arch/15 §3.1).
public sealed record TopicLifecycleCreateJson(
    [property: JsonPropertyName("partitions")] int Partitions,
    [property: JsonPropertyName("replication_factor")] int ReplicationFactor,
    [property: JsonPropertyName("configs")] Dictionary<string, string>? Configs,
    [property: JsonPropertyName("requested_unix")] long RequestedUnix,
    [property: JsonPropertyName("requested_by")] string RequestedBy)
{
    public string Serialize() => JsonSerializer.Serialize(this, new JsonSerializerOptions
    { DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull });
}

// Значение заявки topics/<T>/desired.delete (arch/15 §3.1).
public sealed record TopicLifecycleDeleteJson(
    [property: JsonPropertyName("requested_unix")] long RequestedUnix,
    [property: JsonPropertyName("requested_by")] string RequestedBy)
{
    public string Serialize() => JsonSerializer.Serialize(this);
}

// Чистая валидация создания топика (arch/02 §10.2-9 / §10.3) на эффективных
// значениях (дефолты config кластера); отдельный класс — прецедент KafkaCreateValidator.
public static class KafkaTopicCreateValidator
{
    public static IReadOnlyList<ValidationError> Validate(CreateTopicRequest request, KafkaConfigJson config)
    {
        var errors = new List<ValidationError>();
        if (!KafkaLimits.TopicPattern().IsMatch(request.Name ?? "") || KafkaLimits.IsInternalTopic(request.Name ?? ""))
            errors.Add(new("name", "имя: ^[a-zA-Z0-9._-]{1,249}$ без __-префикса"));

        var partitions = request.Partitions ?? config.DefaultPartitions;
        if (partitions < KafkaLimits.MinPartitions || partitions > KafkaLimits.MaxPartitions)
            errors.Add(new("partitions", $"partitions: целое {KafkaLimits.MinPartitions}..{KafkaLimits.MaxPartitions}"));

        var rf = request.ReplicationFactor ?? (short)config.ReplicationFactor;
        if (rf < KafkaLimits.MinRf || rf > config.Brokers)
            errors.Add(new("replicationFactor", $"replicationFactor: целое {KafkaLimits.MinRf}..{KafkaLimits.MaxRf} и ≤ brokers ({config.Brokers})"));

        if (request.RetentionMs is { } r && (r < KafkaLimits.MinRetentionMs || r > KafkaLimits.MaxRetentionMs))
            errors.Add(new("retentionMs", $"retentionMs: {KafkaLimits.MinRetentionMs}..{KafkaLimits.MaxRetentionMs}"));

        if (request.MinInSyncReplicas is { } isr && (isr < 1 || isr > rf))
            errors.Add(new("minInSyncReplicas", $"minInSyncReplicas: целое 1..replicationFactor (={rf})"));

        return errors;
    }
}

// Построение канонической create-заявки (arch/02 §10.2-9): развёртка дефолтов
// config кластера; прецедент — KafkaClusterCreatePlan.Build.
public static class KafkaTopicCreatePlan
{
    public static TopicLifecycleCreateJson Build(
        CreateTopicRequest request, KafkaConfigJson config, long nowUnix, string by)
    {
        Dictionary<string, string>? configs = null;
        if (request.RetentionMs is not null || request.MinInSyncReplicas is not null)
        {
            configs = new Dictionary<string, string>();
            if (request.RetentionMs is { } r)
                configs["retention.ms"] = r.ToString(CultureInfo.InvariantCulture);
            if (request.MinInSyncReplicas is { } isr)
                configs["min.insync.replicas"] = isr.ToString(CultureInfo.InvariantCulture);
        }

        return new TopicLifecycleCreateJson(
            request.Partitions ?? config.DefaultPartitions,
            request.ReplicationFactor ?? (short)config.ReplicationFactor,
            configs, nowUnix, by);
    }
}
```

- [ ] **Step 4: Run** → PASS; весь набор `dotnet test src/tests/AdminPanel.UnitTests`.

- [ ] **Step 5: Commit** `git commit -am "feat(panel): валидатор и план lifecycle-заявок топиков (KafkaTopicCreateValidator/KafkaTopicCreatePlan, arch/02 §10.2-9)"`

---

### Task 8: панель — команды 9–12 и роуты

**Вход:** Task 7 (`KafkaTopicCreateValidator.Validate` / `KafkaTopicCreatePlan.Build`); харнесс `src/tests/AdminPanel.UnitTests/Kafka/KafkaCommandHarness.cs` существует.
**Действие (файлы):**
- Modify: `src/AdminPanel.Api/Operations/Kafka/KafkaCommands.cs`
- Modify: `src/AdminPanel.Api/Operations/Kafka/KafkaOperationsModule.cs`
- Test: `src/tests/AdminPanel.UnitTests/Kafka/TopicLifecycleCommandTests.cs` (Create)
**Выход:** POST create (201/400/404/409/503), DELETE delete (204 идемпотентно), две отмены (204/404).
**Проверка:** `dotnet test src/tests/AdminPanel.UnitTests --filter TopicLifecycleCommandTests` зелёный.
**Spec:** §5.1 (мутации 9–12), §8 (гварды).

- [ ] **Step 1: Write the failing tests** — новый файл `TopicLifecycleCommandTests.cs` по образцу `TopicDesiredCommandTests.cs` (харнесс `KafkaCommandHarness`; AAA):

```csharp
// Кейсы (все через харнесс: fake etcd с сидом config/топиков):
// 1) Create_NewTopic_ClaimPut201: нет ключей topics/x → 201, в etcd
//    desired.create с развёрнутыми дефолтами config (12/3), requested_by из команды.
// 2) Create_ExistingTopic_409: факт-ключ topics/orders есть и не missing → 409.
// 3) Create_MissingTopicAllowed: факт-ключ с missing=true и БЕЗ desired → 201.
// 4) Create_MissingTopicWithDesired_409: missing=true с живым desired → 409.
// 5) Create_LiveTicket_409: desired.create уже стоит → 409 (клэйм не проходит).
// 6) Create_NotActive_409 / Create_NoCluster_404 / Create_InvalidBody_400.
// 7) Delete_ExistingTopic_PutsTicket204: факт-ключ не missing → 204,
//    desired.delete в etcd; повторный DELETE при живой заявке → 204 без записи
//    (идемпотентность, тот же JSON — сверить value/mod_revision).
// 8) Delete_MissingTopic_404 / Delete_LiveCreateTicket_409 / Delete_LiveDesired_409.
// 9) CancelCreate_NoTicket_404 / CancelDelete_RemovesTicket_204.
```

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Реализация команд в `KafkaCommands.cs`**

Новые исключения (рядом с существующими):

```csharp
// Топик уже существует в реестре (не missing) — 409 (create, arch/02 §10.2-9).
public sealed class KafkaTopicExistsException(string cluster, string topic)
    : Exception($"топик {topic} kafka-кластера {cluster} уже существует");

// Живая lifecycle-заявка на топик — 409.
public sealed class KafkaLifecyclePendingException(string cluster, string topic, string op)
    : Exception($"заявка {op} топика {topic} kafka-кластера {cluster} уже жива — дождитесь исполнения или отмените");

// Живая конфиг-заявка desired у топика — 409 (create/delete требуют отмены).
public sealed class KafkaDesiredPendingException(string cluster, string topic)
    : Exception($"у топика {topic} кластера {cluster} живая конфиг-заявка desired — сначала отмените её");

// Lifecycle-заявка не найдена (отмена) — 404.
public sealed class KafkaLifecycleNotFoundException(string cluster, string topic, string op)
    : Exception($"заявка {op} топика {topic} kafka-кластера {cluster} не найдена");
```

Команды (по образцу `RotateKafkaPasswordCommandHandler`/`CancelTopicDesiredCommandHandler`; helpers `KafkaCommandHelpers` переиспользовать):

```csharp
// ===== 9. Создание топика — клэйм-txn desired.create (arch/02 §10.2-9) =====

public sealed record CreateKafkaTopicCommand(string Cluster, CreateTopicRequest Request, string RequestedBy)
    : ICommand<KafkaTopicCreatedDto>;

public sealed record KafkaTopicCreatedDto(string Cluster, string Topic, int Partitions, int ReplicationFactor);

[InjectAsScoped]
public sealed class CreateKafkaTopicCommandHandler(
    ISnapshotStore store, IEtcdGateway gateway, TimeProvider time)
    : ICommandHandler<CreateKafkaTopicCommand, KafkaTopicCreatedDto>
{
    public async ValueTask<Result<KafkaTopicCreatedDto>> Handle(CreateKafkaTopicCommand command, CancellationToken ct)
    {
        var (cluster, request) = (command.Cluster, command.Request);
        var topic = request.Name ?? "";

        // Имя каноническое (404 при мусоре — как мутации 6–7).
        if (!KafkaLimits.TopicPattern().IsMatch(topic) || KafkaLimits.IsInternalTopic(topic))
            return Result<KafkaTopicCreatedDto>.Failed(new KafkaTopicNotFoundException(cluster, topic));

        if (KafkaCommandHelpers.ActiveEndpoint(store) is not { } endpoint)
            return Result<KafkaTopicCreatedDto>.Failed(new EtcdWriteUnavailableException());

        var config = await KafkaCommandHelpers.ReadConfigAsync(gateway, endpoint, cluster, ct);
        if (config.Error is not null)
            return Result<KafkaTopicCreatedDto>.Failed(config.Error);
        if (config.Value is null)
            return Result<KafkaTopicCreatedDto>.Failed(new KafkaClusterNotFoundException(cluster));
        if (config.Value.State is not null)
            return Result<KafkaTopicCreatedDto>.Failed(new KafkaClusterNotActiveException(cluster, config.Value.State));

        // Guards по свежему ключу топика: есть и не missing → 409; missing с
        // живым desired → 409; обе lifecycle-заявки отсутствуют (§10.2-9).
        var key = KafkaCommandHelpers.TopicKey(cluster, topic);
        var read = await KafkaCommandHelpers.ReadTopicKeyAsync(gateway, endpoint, key, ct);
        if (read.Error is not null)
            return Result<KafkaTopicCreatedDto>.Failed(read.Error);
        if (read.Json is not null && !read.Json.Missing)
            return Result<KafkaTopicCreatedDto>.Failed(new KafkaTopicExistsException(cluster, topic));
        if (read.Json is { Missing: true, Desired: not null })
            return Result<KafkaTopicCreatedDto>.Failed(new KafkaDesiredPendingException(cluster, topic));

        foreach (var op in new[] { "create", "delete" })
        {
            var ticket = await gateway.RangeAsync(endpoint, KafkaCommandHelpers.LifecycleKey(cluster, topic, op), ct);
            if (!ticket.IsSuccess)
                return Result<KafkaTopicCreatedDto>.Failed(new EtcdWriteUnavailableException());
            if (ticket.Value.Count > 0)
                return Result<KafkaTopicCreatedDto>.Failed(new KafkaLifecyclePendingException(cluster, topic, op));
        }

        var errors = KafkaTopicCreateValidator.Validate(request, config.Value);
        if (errors.Count > 0)
            return Result<KafkaTopicCreatedDto>.Failed(new KafkaValidationException(errors));

        // Клэйм-txn: version(desired.create)==0 + put (порт §9.8).
        var ticketKey = KafkaCommandHelpers.LifecycleKey(cluster, topic, "create");
        var plan = KafkaTopicCreatePlan.Build(request, config.Value, time.GetUtcNow().ToUnixTimeSeconds(), command.RequestedBy);
        var txn = await gateway.TxnAsync(endpoint, [new TxnCompare(ticketKey, 0)], [new KvPut(ticketKey, plan.Serialize())], ct);
        if (!txn.IsSuccess)
            return Result<KafkaTopicCreatedDto>.Failed(new EtcdWriteUnavailableException());
        if (!txn.Value.Succeeded)
            return Result<KafkaTopicCreatedDto>.Failed(new KafkaLifecyclePendingException(cluster, topic, "create"));

        return Result<KafkaTopicCreatedDto>.Success(new KafkaTopicCreatedDto(
            cluster, topic, plan.Partitions, plan.ReplicationFactor));
    }
}

// ===== 10. Удаление топика — клэйм-txn desired.delete (arch/02 §10.2-10) =====

public sealed record DeleteKafkaTopicCommand(string Cluster, string Topic, string RequestedBy)
    : ICommand<KafkaTopicDeletedDto>;

public sealed record KafkaTopicDeletedDto(string Cluster, string Topic);

[InjectAsScoped]
public sealed class DeleteKafkaTopicCommandHandler(
    ISnapshotStore store, IEtcdGateway gateway, TimeProvider time)
    : ICommandHandler<DeleteKafkaTopicCommand, KafkaTopicDeletedDto>
{
    public async ValueTask<Result<KafkaTopicDeletedDto>> Handle(DeleteKafkaTopicCommand command, CancellationToken ct)
    {
        var (cluster, topic) = (command.Cluster, command.Topic);
        if (!KafkaLimits.TopicPattern().IsMatch(topic) || KafkaLimits.IsInternalTopic(topic))
            return Result<KafkaTopicDeletedDto>.Failed(new KafkaTopicNotFoundException(cluster, topic));

        if (KafkaCommandHelpers.ActiveEndpoint(store) is not { } endpoint)
            return Result<KafkaTopicDeletedDto>.Failed(new EtcdWriteUnavailableException());

        var config = await KafkaCommandHelpers.ReadConfigAsync(gateway, endpoint, cluster, ct);
        if (config.Error is not null)
            return Result<KafkaTopicDeletedDto>.Failed(config.Error);
        if (config.Value is null)
            return Result<KafkaTopicDeletedDto>.Failed(new KafkaClusterNotFoundException(cluster));
        if (config.Value.State is not null)
            return Result<KafkaTopicDeletedDto>.Failed(new KafkaClusterNotActiveException(cluster, config.Value.State));

        // Топик должен существовать и не быть missing (404), живые заявки — 409.
        var read = await KafkaCommandHelpers.ReadTopicKeyAsync(gateway, endpoint, KafkaCommandHelpers.TopicKey(cluster, topic), ct);
        if (read.Error is not null)
            return Result<KafkaTopicDeletedDto>.Failed(read.Error);
        if (read.Json is null || read.Json.Missing)
            return Result<KafkaTopicDeletedDto>.Failed(new KafkaTopicNotFoundException(cluster, topic, "топик отсутствует в кластере"));
        if (read.Json.Desired is not null)
            return Result<KafkaTopicDeletedDto>.Failed(new KafkaDesiredPendingException(cluster, topic));

        var createTicket = await gateway.RangeAsync(endpoint, KafkaCommandHelpers.LifecycleKey(cluster, topic, "create"), ct);
        if (!createTicket.IsSuccess)
            return Result<KafkaTopicDeletedDto>.Failed(new EtcdWriteUnavailableException());
        if (createTicket.Value.Count > 0)
            return Result<KafkaTopicDeletedDto>.Failed(new KafkaLifecyclePendingException(cluster, topic, "create"));

        // Клэйм-txn + идемпотентность: живая delete-заявка → 204 без записи.
        var ticketKey = KafkaCommandHelpers.LifecycleKey(cluster, topic, "delete");
        var existing = await gateway.RangeAsync(endpoint, ticketKey, ct);
        if (!existing.IsSuccess)
            return Result<KafkaTopicDeletedDto>.Failed(new EtcdWriteUnavailableException());
        if (existing.Value.Count > 0)
            return Result<KafkaTopicDeletedDto>.Success(new KafkaTopicDeletedDto(cluster, topic));

        var txn = await gateway.TxnAsync(
            endpoint, [new TxnCompare(ticketKey, 0)],
            [new KvPut(ticketKey, new TopicLifecycleDeleteJson(time.GetUtcNow().ToUnixTimeSeconds(), command.RequestedBy).Serialize())], ct);
        if (!txn.IsSuccess)
            return Result<KafkaTopicDeletedDto>.Failed(new EtcdWriteUnavailableException());
        if (!txn.Value.Succeeded)
            return Result<KafkaTopicDeletedDto>.Success(new KafkaTopicDeletedDto(cluster, topic)); // гонка постановки — уже стоит

        return Result<KafkaTopicDeletedDto>.Success(new KafkaTopicDeletedDto(cluster, topic));
    }
}

// ===== 11–12. Отмена lifecycle-заявок — del ключа (arch/02 §10.2-11/12) =====

public sealed record CancelTopicLifecycleCommand(string Cluster, string Topic, string Op)
    : ICommand<KafkaTopicLifecycleCancelledDto>;

public sealed record KafkaTopicLifecycleCancelledDto(string Cluster, string Topic, string Op);

[InjectAsScoped]
public sealed class CancelTopicLifecycleCommandHandler(
    ISnapshotStore store, IEtcdGateway gateway)
    : ICommandHandler<CancelTopicLifecycleCommand, KafkaTopicLifecycleCancelledDto>
{
    public async ValueTask<Result<KafkaTopicLifecycleCancelledDto>> Handle(CancelTopicLifecycleCommand command, CancellationToken ct)
    {
        var (cluster, topic, op) = (command.Cluster, command.Topic, command.Op);
        if (op is not ("create" or "delete"))
            return Result<KafkaTopicLifecycleCancelledDto>.Failed(new KafkaLifecycleNotFoundException(cluster, topic, op));

        if (KafkaCommandHelpers.ActiveEndpoint(store) is not { } endpoint)
            return Result<KafkaTopicLifecycleCancelledDto>.Failed(new EtcdWriteUnavailableException());

        var config = await KafkaCommandHelpers.ReadConfigAsync(gateway, endpoint, cluster, ct);
        if (config.Error is not null)
            return Result<KafkaTopicLifecycleCancelledDto>.Failed(config.Error);
        if (config.Value is null)
            return Result<KafkaTopicLifecycleCancelledDto>.Failed(new KafkaClusterNotFoundException(cluster));
        if (config.Value.State is not null)
            return Result<KafkaTopicLifecycleCancelledDto>.Failed(new KafkaClusterNotActiveException(cluster, config.Value.State));

        if (!KafkaLimits.TopicPattern().IsMatch(topic) || KafkaLimits.IsInternalTopic(topic))
            return Result<KafkaTopicLifecycleCancelledDto>.Failed(new KafkaTopicNotFoundException(cluster, topic));

        // 404 если заявки нет; del ключа заявки.
        var ticketKey = KafkaCommandHelpers.LifecycleKey(cluster, topic, op);
        var range = await gateway.RangeAsync(endpoint, ticketKey, ct);
        if (!range.IsSuccess)
            return Result<KafkaTopicLifecycleCancelledDto>.Failed(new EtcdWriteUnavailableException());
        if (range.Value.Count == 0)
            return Result<KafkaTopicLifecycleCancelledDto>.Failed(new KafkaLifecycleNotFoundException(cluster, topic, op));

        var deleted = await gateway.DeleteAsync(endpoint, ticketKey, prefix: false, ct);
        if (!deleted.IsSuccess)
            return Result<KafkaTopicLifecycleCancelledDto>.Failed(new EtcdWriteUnavailableException());

        return Result<KafkaTopicLifecycleCancelledDto>.Success(new KafkaTopicLifecycleCancelledDto(cluster, topic, op));
    }
}
```

`KafkaCommandHelpers` + helper:

```csharp
internal static string LifecycleKey(string cluster, string topic, string op)
    => $"/kafka/clusters/{cluster}/topics/{topic}/desired.{op}";
```

- [ ] **Step 4: Роуты в `KafkaOperationsModule.cs`** — по образцу существующих (switch по исключениям):

```csharp
// POST /api/kafka/clusters/{cluster}/topics — создание топика (02 §10.2-9).
endpoints.MapPost("/api/kafka/clusters/{cluster}/topics", async (
    string cluster, CreateTopicRequest request, ClaimsPrincipal user,
    IHandler handler, CancellationToken ct) =>
{
    var result = await handler.HandleCommand<CreateKafkaTopicCommand, KafkaTopicCreatedDto>(
        new CreateKafkaTopicCommand(cluster, request, user.Identity?.Name ?? "adminpanel"), ct);
    if (result.IsSuccess)
        return Results.Created($"/api/kafka/clusters/{cluster}/topics/{request.Name}", result.Value);

    return result.Error switch
    {
        KafkaValidationException validation => Results.Problem(
            statusCode: StatusCodes.Status400BadRequest, title: "Validation failed",
            detail: result.Error.Message,
            extensions: new Dictionary<string, object?>
            { ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }) }),
        KafkaClusterNotFoundException or KafkaTopicNotFoundException => Results.Problem(
            statusCode: StatusCodes.Status404NotFound, title: "Not found", detail: result.Error.Message),
        KafkaClusterNotActiveException or KafkaTopicExistsException
            or KafkaLifecyclePendingException or KafkaDesiredPendingException => Results.Problem(
            statusCode: StatusCodes.Status409Conflict, title: "Conflict", detail: result.Error.Message),
        EtcdWriteUnavailableException or InvalidKafkaConfigException or InvalidKafkaTopicKeyException
            => Results.Problem(statusCode: StatusCodes.Status503ServiceUnavailable,
            title: "Etcd write unavailable", detail: result.Error.Message),
        _ => Results.Problem(statusCode: StatusCodes.Status503ServiceUnavailable,
            title: "Etcd write failed", detail: result.Error!.Message),
    };
});

// DELETE /api/kafka/clusters/{cluster}/topics/{topic} — удаление топика (02 §10.2-10).
// switch: KafkaClusterNotFound/KafkaTopicNotFound → 404; NotActive/
// LifecyclePending/DesiredPending → 409; остальное → 503. Успех → 204.

// DELETE /api/kafka/clusters/{cluster}/topics/{topic}/desired.create — отмена (02 §10.2-11).
// DELETE /api/kafka/clusters/{cluster}/topics/{topic}/desired.delete — отмена (02 §10.2-12).
// Оба: CancelTopicLifecycleCommand(cluster, topic, op); switch: NotFound
// (кластер/топик/заявка) → 404; NotActive → 409; остальное → 503; успех → 204.
```

(Полные switch-тела скопировать из роута `DELETE .../topics/{topic}/desired` выше по файлу, заменив типы исключений; для DELETE-роутов клэйм-txn-исключений нет.)

- [ ] **Step 5: Run** `dotnet test src/tests/AdminPanel.UnitTests --filter TopicLifecycleCommandTests` → PASS; `dotnet build src/PgWorker.slnx` 0 warnings.

- [ ] **Step 6: Commit** `git commit -am "feat(panel): мутации создания/удаления топика и отмены lifecycle-заявок (arch/02 §10.2-9..12)"`

---

### Task 9: панель — DTO (виртуальные строки) и алерты

**Вход:** Task 6 (`KafkaTopicLifecycleTicket` в снапшоте).
**Действие (файлы):**
- Modify: `src/AdminPanel.Api/Inspection/KafkaQuery.cs`
- Modify: `src/AdminPanel.Core/Kafka/KafkaAlerting/KafkaAlertEngine.cs`
- Test: `src/tests/AdminPanel.UnitTests/KafkaAlertRulesTests.cs`, `src/tests/AdminPanel.UnitTests/InspectionMappersTests.cs` (или `KafkaModelTests.cs` — куда ближе по образцу)
**Выход:** `KafkaTopicDto.Lifecycle` + create-виртуальные строки (факт-поля null/0 — параметры только в `lifecycle`); алерты create-pending/delete-pending/lifecycle-stale.
**Проверка:** `dotnet test src/tests/AdminPanel.UnitTests --filter "KafkaAlert|Inspection|KafkaModel"` зелёный.
**Spec:** §5.3 (DTO/алерты).

- [ ] **Step 1: Write the failing tests** (AAA):
  - Алерты: create-заявка → info `kafka-topic-create-pending`; delete-заявка → warning `kafka-topic-delete-pending`; заявка старше `StaleDesiredSeconds` → warning `kafka-lifecycle-stale`; без заявок — алертов нет.
  - Маппинг: у кластера топики `[orders]` + тикеты `orders:delete`, `audit:create` → DTO: `orders.lifecycle.op == "delete"`; в списке появляется `audit` — виртуальная строка с факт-полями null/0 (`partitions == 0`, `replicationFactor/retentionMs/minInSyncReplicas == null`, `desired == null`, `missing == false`) и параметрами в `lifecycle` (`partitions == 12` и т.д.); delete-тикет на топике без факт-ключа — строка не создаётся.

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Реализация**

`KafkaQuery.cs`:

```csharp
// Lifecycle-часть строки топика (arch/03 §7.2): op + параметры + аудит.
public sealed record TopicLifecycleDto(
    string Op, int? Partitions, short? ReplicationFactor,
    long? RetentionMs, short? MinInSyncReplicas,
    long RequestedUnix, string? RequestedBy);

// KafkaTopicDto: + поле
public sealed record KafkaTopicDto(
    ...,
    TopicLifecycleDto? Lifecycle = null);
```

В маппинг кластера (после существующего `cluster.Topics.Select(...)`):

```csharp
// Мерж lifecycle-тикетов: delete — к существующей строке; create без
// топика — виртуальная строка: факт-поля null/0 (спека §5.3), параметры —
// только в lifecycle-части.
var lifecycleByTopic = (cluster.LifecycleTickets ?? [])
    .ToDictionary(t => t.Topic, StringComparer.Ordinal);
var topics = cluster.Topics
    .Select(t => ToDto(t, lifecycleByTopic.GetValueOrDefault(t.Name)))
    .ToList();
foreach (var ticket in cluster.LifecycleTickets ?? [])
    if (ticket.Op == "create" && topics.All(t => t.Name != ticket.Topic))
        topics.Add(new KafkaTopicDto(
            ticket.Topic, 0, null, null, null,
            Desired: null, Missing: false, SyncedUnix: null, UnderReplicatedPartitions: null,
            Lifecycle: new TopicLifecycleDto(
                ticket.Op, ticket.Partitions, ticket.ReplicationFactor,
                ticket.RetentionMs, ticket.MinInSyncReplicas, ticket.RequestedUnix, ticket.RequestedBy)));
```

`KafkaAlertEngine.TopicAlerts` — дополнить (по `cluster.LifecycleTickets`):

```csharp
// Lifecycle-алерты (t01, arch/03 §7.4): pending-заявки + буксование.
foreach (var ticket in cluster.LifecycleTickets ?? [])
{
    if (nowUnix - ticket.RequestedUnix > _options.StaleDesiredSeconds)
        yield return new Alert(
            $"kafka-lifecycle-stale:{cluster.Name}/{ticket.Topic}",
            AlertSeverity.Warning,
            "kafka-lifecycle-stale",
            $"{cluster.Name}/{ticket.Topic}",
            $"заявка {ticket.Op} топика {ticket.Topic} кластера {cluster.Name} не исполнена дольше {_options.StaleDesiredSeconds} c — воркер буксует или кластер недоступен",
            new Dictionary<string, string>
            {
                ["op"] = ticket.Op,
                ["requestedUnix"] = ticket.RequestedUnix.ToString(),
                ["requestedBy"] = ticket.RequestedBy ?? "unknown",
            },
            null);
    else
        yield return new Alert(
            $"kafka-topic-{ticket.Op}-pending:{cluster.Name}/{ticket.Topic}",
            ticket.Op == "delete" ? AlertSeverity.Warning : AlertSeverity.Info,
            ticket.Op == "delete" ? "kafka-topic-delete-pending" : "kafka-topic-create-pending",
            $"{cluster.Name}/{ticket.Topic}",
            ticket.Op == "delete"
                ? $"заявка удаления топика {ticket.Topic} кластера {cluster.Name} жива — топик и данные будут удалены (до тика можно отменить)"
                : $"заявка создания топика {ticket.Topic} кластера {cluster.Name} жива — ждёт тика воркера",
            null, null);
}
```

(`KafkaTopicLifecycleTicket.RequestedUnix` — non-nullable `long`, спека §4.2.)

- [ ] **Step 4: Run** → PASS; весь набор `dotnet test src/tests/AdminPanel.UnitTests`.

- [ ] **Step 5: Commit** `git commit -am "feat(panel): lifecycle-бейджи топиков (виртуальные строки create) и алерты pending/stale (arch/03 §7)"`

---

### Task 10: фронтенд — модалки и вкладка Топики

**Вход:** Task 8–9 (API и DTO готовы); `frontend/src/pages/kafka-cluster/TopicDesiredModal.tsx` — образец.
**Действие (файлы):**
- Modify: `frontend/src/api/dto.ts`
- Modify: `frontend/src/api/queries.ts`
- Create: `frontend/src/pages/kafka-cluster/TopicCreateModal.tsx`
- Create: `frontend/src/pages/kafka-cluster/DeleteTopicModal.tsx`
- Modify: `frontend/src/pages/kafka-cluster/TopicsTab.tsx`
**Выход:** кнопка «Создать топик», бейджи/отмены заявок, красное удаление с вводом имени.
**Проверка:** `cd frontend && npm run build` (tsc + vite) без ошибок.
**Spec:** §5.3 (UI).

- [ ] **Step 1: `dto.ts` + `queries.ts`**

```typescript
// dto.ts — рядом с KafkaTopicDesiredDto:
export interface KafkaTopicLifecycleDto {
  op: 'create' | 'delete';
  partitions: number | null;
  replicationFactor: number | null;
  retentionMs: number | null;
  minInSyncReplicas: number | null;
  requestedUnix: number;
  requestedBy: string | null;
}
// KafkaTopicDto: + lifecycle: KafkaTopicLifecycleDto | null;
// CreateTopicRequestDto: { name: string; partitions?: number;
//   replicationFactor?: number; retentionMs?: number; minInSyncReplicas?: number; }
// KafkaTopicCreatedDto: { cluster: string; topic: string;
//   partitions: number; replicationFactor: number; }

// queries.ts — по образцу upsertTopicDesired/cancelTopicDesired:
export function createKafkaTopic(cluster: string, request: CreateTopicRequestDto): Promise<KafkaTopicCreatedDto> {
  return apiFetch<KafkaTopicCreatedDto>(
    `/api/kafka/clusters/${encodeURIComponent(cluster)}/topics`,
    { method: 'POST', body: request });
}
export function deleteKafkaTopic(cluster: string, topic: string): Promise<void> {
  return apiFetch<void>(
    `/api/kafka/clusters/${encodeURIComponent(cluster)}/topics/${encodeURIComponent(topic)}`,
    { method: 'DELETE' });
}
export function cancelTopicLifecycle(cluster: string, topic: string, op: 'create' | 'delete'): Promise<void> {
  return apiFetch<void>(
    `/api/kafka/clusters/${encodeURIComponent(cluster)}/topics/${encodeURIComponent(topic)}/desired.${op}`,
    { method: 'DELETE' });
}
```

- [ ] **Step 2: `TopicCreateModal.tsx`** — Mantine-модал по образцу `TopicDesiredModal.tsx`: поля name/partitions/replicationFactor (дефолты — из props кластера: `defaultPartitions`, `replicationFactor`), retentionMs/minInSyncReplicas опционально; клиентская валидация-зеркало §5.2 (`^[a-zA-Z0-9._-]{1,249}$` без `__`; partitions 1..1000; RF 1..9 ≤ brokers; retention 1..2147483647; minISR 1..RF); onSuccess → `invalidateQueries(['kafka-clusters'])`; ошибки ProblemDetails — текстом (как образец).

- [ ] **Step 3: `DeleteTopicModal.tsx`** — модал подтверждения: текст «Топик `<t>` и все его данные будут удалены из Kafka безвозвратно. Заявка исполнится в течение ~15 с — до этого её можно отменить во вкладке.»; `TextInput` для ввода имени; кнопка «Удалить» красная `disabled={input !== topic}`; `deleteKafkaTopic`.

- [ ] **Step 4: `TopicsTab.tsx`** — правки:
  - шапка: кнопка `+ Создать топик` (открывает TopicCreateModal; видна при `canMutate`);
  - колонка «Заявка»: при `topic.lifecycle` бейдж (`blue` create: `создание: N партиций, RF R`; `red` delete: `удаление…`) с Tooltip `formatAge(requestedUnix) · автор` + кнопка «Отменить заявку» (`cancelTopicLifecycle`);
  - колонка действий: `canMutate && !topic.missing && topic.lifecycle?.op !== 'create'` → красная compact `Удалить топик` (DeleteTopicModal);
  - подпись: «Создание/удаление топиков — заявками панели; внешние изменения (CLI/клиенты) подхватываются автосинком»;
  - виртуальная строка создания (`partitions === 0 && lifecycle`): факт-поля отображаются `—` (RF/retention/minISR из DTO null; параметры видны в бейдже lifecycle).

- [ ] **Step 5: Run** `cd frontend && npm run build` → без ошибок (tsc strict).

- [ ] **Step 6: Commit** `git commit -am "feat(panel-ui): создание/удаление топиков и бейджи lifecycle-заявок во вкладке Топики"`

---

### Task 11: стенд — сид и чеки 50/55

**Вход:** Tasks 8–10; стенд `dev-stand/adminpanel/` (профиль quick = сид, `kafka` = живой воркер).
**Действие (файлы):**
- Modify: `dev-stand/adminpanel/kafka-seed.sh`
- Modify: `dev-stand/adminpanel/checks/50-kafka-api.sh`
- Modify: `dev-stand/adminpanel/checks/55-kafka-e2e.sh`
**Выход:** заявки в сиде; API/e2e-покрытие lifecycle (включая отмену delete до тика).
**Проверка:** `cd dev-stand/adminpanel && ./checks/50-kafka-api.sh` зелёный; `./checks/55-kafka-e2e.sh` зелёный (с чистого состояния).
**Spec:** §6 (стенд/e2e), §9.3 (отмена удаления до тика).

- [ ] **Step 1: Сид** — в `kafka-seed.sh` (после существующих topics-строк кластера `events`):

```bash
# lifecycle-заявки (t01, arch/15 §3.1): create без факт-ключа + delete на живой orders
put /kafka/clusters/events/topics/audit/desired.create \
  '{"partitions":12,"replication_factor":3,"configs":{"retention.ms":"86400000"},"requested_unix":1756501200,"requested_by":"seed"}'
put /kafka/clusters/events/topics/orders/desired.delete \
  '{"requested_unix":1756501300,"requested_by":"seed"}'
```

- [ ] **Step 2: `50-kafka-api.sh`** — новые шаги (после существующего блока topics):

```bash
# N) lifecycle: виртуальная строка audit (create) + бейдж delete у orders.
api /api/kafka/clusters/events | jq -e '
  ([.topics[] | select(.name=="audit")][0].lifecycle.op == "create")
  and ([.topics[] | select(.name=="audit")][0].lifecycle.partitions == 12)
  and ([.topics[] | select(.name=="orders")][0].lifecycle.op == "delete")' >/dev/null \
  || { echo "❌ lifecycle-бейджи (audit create / orders delete)"; exit 1; }

# N+1) негативы: повторный create audit → 409 (клэйм); create payments → 409 (есть);
#      delete ghost (missing) → 404; отмена несуществующей → 404; RF 10 → 400.
c="$(code -X POST "$BASE/api/kafka/clusters/events/topics" -H 'Content-Type: application/json' -d '{"name":"audit"}')"
[ "$c" = 409 ] || { echo "❌ повторный create audit = $c"; exit 1; }
c="$(code -X POST "$BASE/api/kafka/clusters/events/topics" -H 'Content-Type: application/json' -d '{"name":"payments"}')"
[ "$c" = 409 ] || { echo "❌ create payments = $c"; exit 1; }
c="$(code -X DELETE "$BASE/api/kafka/clusters/events/topics/ghost")"
[ "$c" = 404 ] || { echo "❌ delete ghost = $c"; exit 1; }
c="$(code -X DELETE "$BASE/api/kafka/clusters/events/topics/payments/desired.create")"
[ "$c" = 404 ] || { echo "❌ отмена несуществующей create = $c"; exit 1; }
c="$(code -X POST "$BASE/api/kafka/clusters/events/topics" -H 'Content-Type: application/json' -d '{"name":"x","replicationFactor":10}')"
[ "$c" = 400 ] || { echo "❌ RF 10 = $c"; exit 1; }

# N+2) отмена create audit → 204, ключ заявки исчез из etcd; DELETE orders идемпотентен (204 ×2).
c="$(code -X DELETE "$BASE/api/kafka/clusters/events/topics/audit/desired.create")"
[ "$c" = 204 ] || { echo "❌ отмена create = $c"; exit 1; }
docker compose exec -T etcd etcdctl get /kafka/clusters/events/topics/audit/desired.create \
  </dev/null 2>/dev/null | grep -q . && { echo "❌ заявка audit не удалена"; exit 1; }
```

- [ ] **Step 3: `55-kafka-e2e.sh`** — новые шаги 10–14 (после missing-ветки, перед демонтажём брокера; хелперы `wait_until`/`kafka_cli`/`etcd_key` уже есть; нумерацию следующих шагов сдвинуть):

```bash
# ===== 10) Создание топика из панели → воркер исполняет =====
echo ">>> (10/14) POST create e2e-panel (6 партиций, RF 3, retention 1д) → автосинк приносит факт"
c="$(code -X POST "$BASE/api/kafka/clusters/$CLUSTER/topics" -H 'Content-Type: application/json' \
  -d '{"name":"e2e-panel","partitions":6,"replicationFactor":3,"retentionMs":86400000}')"
[ "$c" = 201 ] || { echo "❌ POST create = $c"; exit 1; }
wait_until "факт-ключ e2e-panel (partitions 6 / RF 3 / retention 1д, без заявок)" 90 bash -c '
  docker compose exec -T etcd etcdctl get /kafka/clusters/e2e/topics/e2e-panel --print-value-only 2>/dev/null \
  | jq -e ".partitions == 6 and .replication_factor == 3 and .configs[\"retention.ms\"] == \"86400000\" and (has(\"desired\") | not)"'
kafka_cli kafka-topics --describe --topic e2e-panel </dev/null | grep -q "PartitionCount: 6" \
  || { echo "❌ kafka-topics --describe: не 6 партиций"; exit 1; }

# ===== 11) Негативы create =====
c="$(code -X POST "$BASE/api/kafka/clusters/$CLUSTER/topics" -H 'Content-Type: application/json' -d '{"name":"e2e-panel","partitions":1,"replicationFactor":1}')"
[ "$c" = 409 ] || { echo "❌ повторный create = $c, ожидался 409"; exit 1; }
c="$(code -X POST "$BASE/api/kafka/clusters/$CLUSTER/topics" -H 'Content-Type: application/json' -d '{"name":"x","replicationFactor":10}')"
[ "$c" = 400 ] || { echo "❌ RF 10 = $c, ожидался 400"; exit 1; }
c="$(code -X POST "$BASE/api/kafka/clusters/$CLUSTER/topics" -H 'Content-Type: application/json' -d '{"name":"x","partitions":0}')"
[ "$c" = 400 ] || { echo "❌ partitions 0 = $c, ожидался 400"; exit 1; }

# ===== 12) Удаление топика из панели → ключи исчезают =====
echo ">>> (12/14) DELETE e2e-panel → топик и ключи исчезли"
c="$(code -X DELETE "$BASE/api/kafka/clusters/$CLUSTER/topics/e2e-panel")"
[ "$c" = 204 ] || { echo "❌ DELETE = $c"; exit 1; }
wait_until "факт-ключ и заявка e2e-panel удалены" 60 bash -c '
  ! docker compose exec -T etcd etcdctl get /kafka/clusters/e2e/topics/e2e-panel --print-value-only 2>/dev/null | grep -q .'
kafka_cli kafka-topics --list </dev/null | grep -qv e2e-panel \
  || { echo "❌ топик e2e-panel всё ещё в Kafka"; exit 1; }

# ===== 13) Отмена создания: заявка снята до тика =====
echo ">>> (13/14) отмена create: e2e-cancel не создаётся"
c="$(code -X POST "$BASE/api/kafka/clusters/$CLUSTER/topics" -H 'Content-Type: application/json' -d '{"name":"e2e-cancel","partitions":1,"replicationFactor":1}')"
[ "$c" = 201 ] || { echo "❌ POST create e2e-cancel = $c"; exit 1; }
c="$(code -X DELETE "$BASE/api/kafka/clusters/$CLUSTER/topics/e2e-cancel/desired.create")"
[ "$c" = 204 ] || { echo "❌ отмена = $c"; exit 1; }
sleep 35  # 2 тика автосинка
if docker compose exec -T etcd etcdctl get /kafka/clusters/e2e/topics/e2e-cancel --print-value-only 2>/dev/null | grep -q .; then
  # Гонка задокументирована (спека §6): тик успел раньше отмены — доводим до удаления.
  c="$(code -X DELETE "$BASE/api/kafka/clusters/$CLUSTER/topics/e2e-cancel")"
  [ "$c" = 204 ] || { echo "❌ cleanup e2e-cancel = $c"; exit 1; }
  wait_until "e2e-cancel удалён (гонка тика)" 60 bash -c '
    ! docker compose exec -T etcd etcdctl get /kafka/clusters/e2e/topics/e2e-cancel --print-value-only 2>/dev/null | grep -q .'
fi

# ===== 14) Отмена удаления: топик остаётся (spec §9.3) =====
echo ">>> (14/14) отмена delete: e2e-undo остаётся жив"
c="$(code -X POST "$BASE/api/kafka/clusters/$CLUSTER/topics" -H 'Content-Type: application/json' \
  -d '{"name":"e2e-undo","partitions":1,"replicationFactor":1}')"
[ "$c" = 201 ] || { echo "❌ POST create e2e-undo = $c"; exit 1; }
wait_until "факт-ключ e2e-undo" 90 bash -c '
  docker compose exec -T etcd etcdctl get /kafka/clusters/e2e/topics/e2e-undo --print-value-only 2>/dev/null | grep -q .'
c="$(code -X DELETE "$BASE/api/kafka/clusters/$CLUSTER/topics/e2e-undo")"
[ "$c" = 204 ] || { echo "❌ DELETE e2e-undo = $c"; exit 1; }
c="$(code -X DELETE "$BASE/api/kafka/clusters/$CLUSTER/topics/e2e-undo/desired.delete")"
[ "$c" = 204 ] || { echo "❌ отмена delete = $c"; exit 1; }
sleep 35  # 2 тика: тик воркера не должен был исполнить удаление
if ! kafka_cli kafka-topics --list </dev/null | grep -q e2e-undo; then
  # Гонка задокументирована (спека §6, симметрично шагу 13): тик успел раньше
  # отмены — топик пересоздаём и шаг считается пройденным.
  echo "  гонка: тик исполнил удаление раньше отмены — пересоздаём e2e-undo"
  c="$(code -X POST "$BASE/api/kafka/clusters/$CLUSTER/topics" -H 'Content-Type: application/json' \
    -d '{"name":"e2e-undo","partitions":1,"replicationFactor":1}')"
  [ "$c" = 201 ] || { echo "❌ восстановление e2e-undo = $c"; exit 1; }
fi
docker compose exec -T etcd etcdctl get /kafka/clusters/e2e/topics/e2e-undo --print-value-only 2>/dev/null \
  | grep -q . || { echo "❌ факт-ключ e2e-undo пропал после отмены delete"; exit 1; }
```

(Обновить заголовочный комментарий чека: подшаги теперь до 14.)

- [ ] **Step 4: Run** — `cd dev-stand/adminpanel && ./checks/90-down.sh -v && ./checks/50-kafka-api.sh` (профиль quick/сид), затем `./checks/55-kafka-e2e.sh` (сам собирает профиль kafka с чистого состояния). Оба зелёные.

- [ ] **Step 5: Commit** `git commit -am "test(stand): lifecycle-топики в сиде и чеках 50/55 (создание/удаление/обе отмены, e2e)"`

---

### Task 12: финал — полный прогон и roadmap-гейт

**Вход:** Tasks 1–11.
**Действие (файлы):**
- Modify: `arch/roadmap/kafkaworker.md` (удалить пункт t01)
- Modify: `dev-stand/adminpanel/README.md` (если в нём перечислены чеки/подшаги 55 — синхронизировать)
**Выход:** всё зелёное; roadmap-гейт исполнен.
**Проверка:** команды ниже.
**Spec:** §9 (критерии приёмки), §4.1 (roadmap-гейт).

- [ ] **Step 1: Полная сборка/тесты**

```bash
dotnet build src/PgWorker.slnx          # 0 warnings
dotnet test                              # все юниты (без Docker)
dotnet test src/tests/KafkaWorker.IntegrationTests   # с Docker
cd frontend && npm run build             # tsc + vite
```

- [ ] **Step 2: Все чеки стенда** — `10-smoke-api`, `20-alerts`, `30-failover`, `40-live-probes`, `50-kafka-api`, `55-kafka-e2e` с чистого состояния (`90-down.sh -v` перед профилем) — зелёные.

- [ ] **Step 3: Roadmap-гейт** — удалить из `arch/roadmap/kafkaworker.md` блок `t01-kafka-topic-lifecycle` (строки 10–14); проверить отсутствие ссылок: `grep -rn "t01-kafka" arch/ docs/ dev-stand/` — пусто (кроме исторических `docs/superpowers/` — они остаются, это история).

- [ ] **Step 4: Критерии приёмки спеки §9** — пройтись по пунктам 1–9, каждый подтвердить фактом (команда/вывод); расхождений нет.

- [ ] **Step 5: Commit**

```bash
git add arch/roadmap/kafkaworker.md dev-stand/adminpanel/README.md
git commit -m "merge: lifecycle топиков Kafka из панели (t01) — desired.create/delete, воркер+панель+e2e; roadmap-гейт: тег t01 удалён"
```

---

## Самопроверка плана (выполнена при написании и после ревью)

- **Покрытие spec:** §3.1→Task 1 (Step 1–3) + 2; §3.2→Tasks 1/3/4; §3.3→Task 1 (Step 4); §4.1→Task 1 (Step 5) + Task 12; §4.2→Tasks 2/3/4/5 (включая missing+create — Task 3, сходимость обеих веток — Task 5 кейсы 3–4); §5.1→Tasks 7/8; §5.2→Task 7; §5.3→Tasks 6/9/10; §6→Task 11; §7 (фазы)→порядок задач 1→12; §8 (допущения)→учтены в Task 8 (гварды) и Task 7 (дефолты); §9→Task 12 (Step 4) + проверки задач; §9.3 (отмена delete до тика)→Task 11 шаг 14.
- **Плейсхолдеры:** отсутствуют; «по образцу <существующий файл>» — ссылки на реальный образец в репо с полным списком требуемых полей/кейсов.
- **Консистентность типов:** `TopicLifecycleTicket`(воркер, Task 2; `RequestedUnix` — non-nullable `long` по спеке §4.2) ↔ `DecideLifecycle`/`LifecycleKey` (Tasks 3–4); `KafkaTopicLifecycleTicket`(панель, Task 6; `RequestedUnix` — `long`) ↔ DTO/алерты (Task 9); `KafkaTopicCreateValidator.Validate` + `KafkaTopicCreatePlan.Build` (Task 7) ↔ команды (Task 8); `LifecycleKey` в `KafkaCommandHelpers` (Task 8) — тот же формат, что в воркере (Task 4).
- **Правки по ревью Фазы 4 (8 замечаний):** (1) сортировка DecideLifecycle — `Cleanup{Create}=>0, LifecycleDelete=>1, Cleanup{Delete}=>2, LifecycleCreate=>3`, тест коллизии и комментарий согласованы (чистка create ДО delete — по спеке §3.2); (2) `DeleteTopicFailCount = 3` (дольше jitter-ретраев тика — 3 попытки, образец `AlterTopicFailCount`); (3) имя валидатора зафиксировано — `KafkaTopicCreateValidator.Validate` (валидация) + `KafkaTopicCreatePlan.Build` (построение), синхронизированы Task 7 Step 1/Step 3/«Выход» и Task 8; (4) `TopicLifecycleTicket.RequestedUnix` — `long` (спека §4.2), отсутствие `requested_unix` в JSON — parseError (образец ParseRotations), добавлен тест `Parse_TicketWithoutRequestedUnix_IsParseError`; (5) виртуальная строка create — факт-поля null/0 по спеке §5.3 (Task 9 Step 1/Step 3, UI-примечание Task 10 Step 4); (6) интеграционный кейс сходимости delete-ветки добавлен (Task 5 кейс 4); (7) тест missing+create добавлен (Task 3 `DecideLifecycle_CreateWithMissingRegistryKey_ProducesLifecycleCreate`); (8) e2e-шаг 14 «отмена delete до тика → топик остаётся» добавлен (Task 11), нумерация подшагов 55-чека — до 14.
