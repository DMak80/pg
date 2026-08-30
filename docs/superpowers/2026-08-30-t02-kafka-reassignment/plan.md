# t02-kafka-reassignment: план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Процесс PartitionReassigner (I) в KafkaWorker — reassignment партиций (drain брокера `TO_REMOVE` с репликами + ребалансировка по заявке панели) через `kafka-reassign-partitions.sh` docker-exec'ом в контейнер брокера; панель получает заявки/отмена ребалансировки и видимость прогресса.

**Architecture:** Декларативная модель как в существующих процессах: панель пишет заявку `/kafkaworker/rebalances/<C>`, воркер (под lease-клэймом кластера) каждым тиком пересчитывает план из факта метаданных Kafka (включая `__`-топики), подаёт идемпотентные батчи через CLI в контейнере брокера (bootstrap — INTERNAL-listener сети `kfw-net`), пишет прогресс-ключ `/kafkaworker/reassignments/<C>`; завершение — по факту (drain-брокер вне Replicas + нет USR / факт == план). Порядок Active-ветки: надзор → converge → **reassign** → remove → add → ротация → TopicSync.

**Tech Stack:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`), Confluent.Kafka 2.14.2 (без reassignment-API — потому CLI), Docker Engine API (существующий `ExecAsync`), React/Mantine (панель), bash-чеки dev-стенда.

**Spec:** `docs/superpowers/2026-08-30-t02-kafka-reassignment/spec.md` (исполнитель читает spec и этот план; канон — `arch/16-kafkaworker.md` §5 I/§2.4, `arch/15-kafka-clusters.md` §4, `arch/adminpanel/02-etcd-contract.md` §10, `arch/adminpanel/03-panels.md` §7).

## Global Constraints

- Все команды `dotnet` — с `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1`; сборка: `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet build src/PgWorker.slnx` — 0 warnings (`TreatWarningsAsErrors=true`, warning = ошибка сборки).
- Integration-тесты KafkaWorker требуют запущенный Docker (Testcontainers: etcd + `apache/kafka:4.0.0`); прецедент — `src/tests/KafkaWorker.IntegrationTests/Kafka/KafkaClusterFixture.cs`.
- Комментарии/документация — русский; идентификаторы — английский; тесты — AAA-комментарии (`// Arrange / // Act / // Assert`).
- Новые пакеты НЕ добавляются (Confluent.Kafka уже в `src/Directory.Packages.props`).
- Коммиты — в feature-ветку `feat-t02-kafka-reassignment`, свободно, по каждому шагу-вехе.
- Roadmap (`arch/roadmap/kafkaworker.md`) НЕ трогаем: тег `t02` удаляется только мерж-коммитом в `main`.
- arch/ уже обновлён (фаза spec) — в плане код только зеркалит его; при расхождении править код, не arch (расхождение = баг плана/кода, зафиксировать в комментарии коммита).

## Файловая структура (карта изменений)

```
src/KafkaWorker.Provisioning/
  Kafka/IKafkaAdminClient.cs        — Modify: DescribeTopicsAsync(includeInternal), KafkaTopicView.IsrPerPartition (опц.)
  Kafka/KafkaAdminClient.cs         — Modify: метаданные без фильтра __ + ISR
  Processes/ProcessCommon.cs        — Modify: + ReassignOptions
  Processes/ReassignPlanner.cs      — Create: чистые функции планов drain/balance
  Processes/ReassignCli.cs          — Create: сборка CLI-команды (json/properties/sh - c)
  Processes/PartitionReassignerProcess.cs — Create: оркестрация D1–D6/B1–B3
  Processes/RemoveBrokerProcess.cs  — Modify: вызов DescribeTopicsAsync(includeInternal) + journal-текст
  Processes/TopicSyncProcess.cs     — Modify: вызов DescribeTopicsAsync(includeInternal: false)
src/KafkaWorker.Docker/Drivers/ClusterDriver.cs — Modify: ExecNodeAsync в IClusterDriver + оба драйвера
src/KafkaWorker.App/
  Options.cs                        — Modify: Loops.Reassign*, Thresholds.Reassign*
  Program.cs                        — Modify: DI PartitionReassignerProcess + опции
  appsettings.json                  — Modify: значения по умолчанию
  Loops/KafkaClusterProcesses.cs    — Modify: reassigner в ActiveAsync (после converge, до remove)
src/tests/KafkaWorker.UnitTests/Provisioning/
  Fakes.cs                          — Modify: FakeKafkaDriver.ExecNodeAsync + хук
  FakeKafkaAdminClient.cs           — Modify: includeInternal
  FixedTimeProvider.cs              — Create: управляемый TimeProvider (порт AdminPanel.UnitTests/FixedTimeProvider.cs)
  ReassignPlannerTests.cs           — Create
  ReassignCliTests.cs               — Create
  PartitionReassignerProcessTests.cs— Create
  RemoveBrokerProcessTests.cs       — Modify: регресс-кейс internal-реплик
src/tests/KafkaWorker.IntegrationTests/Kafka/ReassignmentTests.cs — Create
src/AdminPanel.Core/Kafka/KafkaSnapshot.cs  — Modify: + KafkaRebalanceTicket, KafkaReassignmentProgress
src/AdminPanel.Etcd/Parsing/KafkaParser.cs   — Modify: ParseRebalances/ParseReassignments
src/AdminPanel.Etcd/KafkaSnapshotRefresher.cs— Modify: чтение двух новых префиксов
src/AdminPanel.Api/Operations/Kafka/RebalanceCommands.cs — Create
src/AdminPanel.Api/Operations/Kafka/KafkaOperationsModule.cs — Modify: 2 эндпоинта
src/AdminPanel.Api/Inspection/KafkaQuery.cs  — Modify: DTO-поля
src/AdminPanel.Core/Kafka/KafkaAlerting/KafkaAlertEngine.cs (+Options) — Modify: 2 алерта
src/tests/AdminPanel.UnitTests/            (корень): KafkaParserTests.cs, KafkaRefresherTests.cs, KafkaModelTests.cs — Modify
src/tests/AdminPanel.UnitTests/Kafka/      (папка):  KafkaCommandTests.cs — Modify (rebalance-команды)
src/tests/AdminPanel.UnitTests/            (корень): KafkaAlertRulesTests.cs — Modify
frontend/src/api/dto.ts, queries.ts          — Modify
frontend/src/pages/kafka-cluster/RebalanceButton.tsx — Create
frontend/src/pages/kafka-cluster/KafkaClusterDetailsPage.tsx, BrokersTab.tsx — Modify
dev-stand/adminpanel/kafka-seed.sh           — Modify: сид заявки/прогресса
dev-stand/adminpanel/checks/50-kafka-api.sh, 55-kafka-e2e.sh — Modify
```

Волны: **A (Task 1–8)** — воркер; **B (Task 9–14)** — панель+стенд. Коммит в конце каждой задачи; после Task 8 и Task 14 — контрольные прогоны.

---

## Волна A — воркер

### Task 1: DescribeTopicsAsync(includeInternal) + IsrPerPartition

**Files:**
- Modify: `src/KafkaWorker.Provisioning/Kafka/IKafkaAdminClient.cs`
- Modify: `src/KafkaWorker.Provisioning/Kafka/KafkaAdminClient.cs`
- Modify: `src/KafkaWorker.Provisioning/Processes/TopicSyncProcess.cs` (вызов в `DescribeFactsAsync`)
- Modify: `src/KafkaWorker.Provisioning/Processes/RemoveBrokerProcess.cs` (~строка 110, вызов в `HasPartitionsAsync`; до Task 6 — `includeInternal: false`)
- Modify: `src/tests/KafkaWorker.UnitTests/Provisioning/FakeKafkaAdminClient.cs`

**Interfaces (produces, используют Task 5/6/7):**
```csharp
// KafkaTopicView — добавить ISR (USR-критерий завершения drain, spec §5.2 D4).
// IsrPerPartition — ОПЦИОНАЛЬНЫЙ параметр (= null): сохраняет 3-аргументные
// конструкторы существующих тестов (RemoveBrokerProcessTests.cs:85,151,
// TopicSyncProcessTests.cs:67) без их правки. Семантика null = «ISR не задан»:
// реальный адаптер заполняет всегда; ReassignPlanner.HasUnderReplicated
// трактует null как отсутствие данных о USR (не блокирует завершение).
public sealed record KafkaTopicView(
    string Topic,
    int Partitions,
    IReadOnlyList<IReadOnlyList<int>> ReplicasPerPartition,
    IReadOnlyList<IReadOnlyList<int>>? IsrPerPartition = null);

// Сигнатура: false — прежнее поведение (без __, TopicSync); true — все топики (I, G).
Task<Result<IReadOnlyList<KafkaTopicView>>> DescribeTopicsAsync(bool includeInternal, CancellationToken ct);
```

- [ ] **Шаг 1.1: Сигнатура + модель.** В `IKafkaAdminClient.cs` добавить опциональный `IsrPerPartition` (сигнатура выше — 3-аргументные `new KafkaTopicView(...)` в существующих тестах компилируются без правок) и параметр `bool includeInternal` в `DescribeTopicsAsync` (xml-комментарий: зачем — arch/16 §5 I/G). Починить вызовы: `TopicSyncProcess.DescribeFactsAsync` → `DescribeTopicsAsync(includeInternal: false, ct)`; `RemoveBrokerProcess.HasPartitionsAsync` (~строка 110) → временно `(includeInternal: false, ct)` (поменяет Task 6); `FakeKafkaAdminClient.DescribeTopicsAsync` → параметр игнорирует: фейк отдаёт `Topics` как есть при обоих значениях флага (фильтрация `__` живёт в адаптере, не в фейке), ISR — новое публичное поле `public IReadOnlyList<IReadOnlyList<int>>? Isr;` (null → в view `IsrPerPartition = null`).
- [ ] **Шаг 1.2: Адаптер.** В `KafkaAdminClient.DescribeTopicsViaMetadata` метод получает `includeInternal`, при `false` фильтрует `t.Topic.StartsWith("__")`; `IsrPerPartition` строится из `p.Isr` (PartitionMetadata Confluent-клиента, рядом с `p.Replicas`) — реальный адаптер заполняет всегда.
- [ ] **Шаг 1.3: Проверка.** `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet build src/PgWorker.slnx` — 0 warnings; `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test src/tests/KafkaWorker.UnitTests` — зелёные (все существующие: TopicSync/RemoveBroker/Supervisor на fake, без правок их данных).
- [ ] **Шаг 1.4: Коммит.** `git add -A && git commit -m "feat(kafkaworker): DescribeTopicsAsync(includeInternal) + ISR per partition, опц. параметр (t02 A1)"`.

**Spec:** §5.5 строка 1 (метаданные включая `__` + ISR).

---

### Task 2: IClusterDriver.ExecNodeAsync

**Files:**
- Modify: `src/KafkaWorker.Docker/Drivers/ClusterDriver.cs` (интерфейс + PlainClusterDriver + SwarmClusterDriver)
- Modify: `src/tests/KafkaWorker.UnitTests/Provisioning/Fakes.cs` (FakeKafkaDriver)

**Interfaces (produces, использует Task 5):**
```csharp
// IClusterDriver:
// Выполнить команду в контейнере живого брокера (arch/16 §2.4; порт PgWorker
// ExecNodeAsync): plain — running-контейнер по имени kfw-<C>-<b> перебором
// хостов; swarm — running-таск сервиса → ContainerId. stdout → строка.
Task<Result<string>> ExecNodeAsync(string cluster, string nodeName, IReadOnlyList<string> cmd, CancellationToken ct);
```

- [ ] **Шаг 2.1: Интерфейс + реализации.** Скопировать порт из `src/PgWorker.Docker/Drivers/ClusterDriver.cs:169-194` (plain) и `:337-357` (swarm) с заменой `NodeName(cluster, shard, node)` → `NodeName(cluster, nodeName)` (вариант KafkaWorker) — код уже в репо, адаптировать имена. В `FakeKafkaDriver` добавить:
```csharp
// Записывающий мок exec: команды видят тесты; опциональный хук симулирует
// Kafka (применение поданного reassignment тестом).
public readonly List<(string Node, IReadOnlyList<string> Cmd)> Execs = [];
public Func<string, IReadOnlyList<string>, Result<string>>? ExecHandler { get; set; }

public Task<Result<string>> ExecNodeAsync(string cluster, string nodeName, IReadOnlyList<string> cmd, CancellationToken ct)
{
    lock (_gate) { Execs.Add((nodeName, cmd)); }
    return Task.FromResult(ExecHandler is { } h ? h(nodeName, cmd) : Result<string>.Success(""));
}
```
- [ ] **Шаг 2.2: Проверка.** Сборка + `dotnet test src/tests/KafkaWorker.UnitTests` (fake реализует интерфейс — юниты собираются).
- [ ] **Шаг 2.3: Коммит.** `git commit -m "feat(kafkaworker): driver ExecNodeAsync (docker exec в контейнер брокера, t02 A2)"`.

**Spec:** §3.1-A, §5.5 строка 2, arch/16 §2.4.

---

### Task 3: ReassignPlanner — чистые функции планов (TDD)

**Files:**
- Create: `src/KafkaWorker.Provisioning/Processes/ReassignPlanner.cs`
- Create: `src/tests/KafkaWorker.UnitTests/Provisioning/ReassignPlannerTests.cs`

**Interfaces (produces, использует Task 5):**
```csharp
/// <summary>Целевой assignment одной партиции (элемент reassignment.json).</summary>
public sealed record ReassignMove(string Topic, int Partition, IReadOnlyList<int> Replicas);

public static class ReassignPlanner
{
    /// <summary>Drain: для каждой партиции с репликой drainBroker — переезд.
    /// newReplicas = старые без drain (порядок сохранён) + добор least-loaded
    /// из targets до min(len(old), targets.Count). Инвариант: newReplicas.Count
    /// >= minIsr(topic) — иначе Result.Failed с причиной (spec §5.2 D3).</summary>
    public static Result<IReadOnlyList<ReassignMove>> PlanDrain(
        IReadOnlyList<KafkaTopicView> topics,               // describe-all (включая __)
        int drainBrokerId,
        IReadOnlyList<int> targetBrokerIds,                 // только RUNNING-цели, отсортированы
        IReadOnlyDictionary<string, int> minIsrByTopic);    // юзер: реестр ?? config.minISR; internal: min(2,targets)

    /// <summary>Balance: converge к декларации (spec §3.4): RF юзер-топиков
    /// min(configRf, targets.Count), internal — min(3, targets.Count)
    /// (формулы от числа живых); лидер (первая реплика) сохраняется; добор
    /// least-loaded; детерминизм сортировкой (topic, partition, brokerId).</summary>
    public static IReadOnlyList<ReassignMove> PlanBalance(
        IReadOnlyList<KafkaTopicView> topics,
        IReadOnlyList<int> targetBrokerIds,
        int configRf);

    /// <summary>Партиции, чей факт != план (по множеству реплик, порядок не
    /// важен) — кандидаты батча; сортировка (Topic, Partition).</summary>
    public static IReadOnlyList<ReassignMove> Pending(
        IReadOnlyList<KafkaTopicView> topics, IReadOnlyList<ReassignMove> plan);

    /// <summary>Drain завершён: drainBrokerId отсутствует в Replicas всех
    /// партиций (ISR не учитывается — см. процесс).</summary>
    public static bool DrainComplete(IReadOnlyList<KafkaTopicView> topics, int drainBrokerId);

    /// <summary>Under-replicated есть: любая партиция Isr.Count < Replicas.Count.
    /// IsrPerPartition == null (ISR не задан — фейк без ISR) = данных о USR
    /// нет → false (не блокирует завершение; реальный адаптер заполняет ISR).</summary>
    public static bool HasUnderReplicated(IReadOnlyList<KafkaTopicView> topics);

    private static IReadOnlyList<int> PickLeastLoaded(HashSet<int> chosen, Dictionary<int,int> load, IReadOnlyList<int> targets);
    // greedy: минимальный load, tie-break — меньший brokerId; счётчик load инкрементится в плане.
}
```

- [ ] **Шаг 3.1: Failing-тесты.** `ReassignPlannerTests.cs` (AAA), кейсы (helper-фабрика `TopicView(string name, int[][] replicas, int[][]? isr = null)`):
  1. `PlanDrain_замещает_реплику_когда_целей_достаточно`: p0 `[1,2,4]`, drain=4, targets=[1,2,3] → `[1,2,3]` (replacement с хвоста — не лидер), p1 `[2,4,1]` → `[2,1,3]` (цель 3 в хвосте, порядок старых сохранён; load выровнен).
  2. `PlanDrain_снижение_RF_при_нехватке_целей`: p0 `[1,2,4]`, drain=4, targets=[1,2] → move `[1,2]` (RF 3→2, добор невозможен — min(len(old),targets)=2).
  3. `PlanDrain_отказ_когда_минISR_недостижим`: minIsrByTopic[t]=2, targets=[1], старые `[1,2,3]`, drain=3 → `Failed`, сообщение содержит «min.insync.replicas» и имя топика.
  4. `PlanDrain_партиции_без_drain_не_в_плане`: p2 `[1,2]` без drain → в плане нет.
  5. `PlanBalance_восстанавливает_RF_и_сохраняет_лидера`: факт p0 `[1,2]` (лидер 1), configRf=3, targets=[1,2,3] → первая реплика 1, множество {1,2,3}; p1 `[2,1]` → `[2,1,3]` (лидер 2 сохранён).
  6. `PlanBalance_детерминизм`: два вызова на одном входе → эквивалентные списки (последовательность move и реплик идентична).
  7. `PlanBalance_internal_формулы`: `__consumer_offsets` p0 `[1,2]`, configRf=3(юзер), targets=3 брокера → 3 реплики (min(3,B)); `Pending` по факту `[1,2]` != план → move в списке.
  8. `DrainComplete_и_HasUnderReplicated`: drain=3 вне всех Replicas → true; `Isr [1]` vs `Replicas [1,2]` → HasUnderReplicated true; `IsrPerPartition=null` → HasUnderReplicated false.
  9. `Pending_сортировка_по_топику_и_партиции`.
- [ ] **Шаг 3.2: Прогнать — красные.** `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test src/tests/KafkaWorker.UnitTests --filter ReassignPlanner` — FAIL (тип не существует).
- [ ] **Шаг 3.3: Реализация** `ReassignPlanner.cs` по сигнатурам выше; internal-топик — `Topic.StartsWith("__")`; RF-цель balance: `__` → `Math.Min(3, targets.Count)`, иначе `Math.Min(configRf, targets.Count)`.
- [ ] **Шаг 3.4: Прогон зелёный.** Тот же filter — PASS.
- [ ] **Шаг 3.5: Коммит.** `git commit -m "feat(kafkaworker): ReassignPlanner — чистые функции планов drain/balance (t02 A3)"`.

**Spec:** §3.3, §3.4, §5.2 D3, §5.3 B2.

---

### Task 4: ReassignCli — сборка CLI-вызова (TDD)

**Files:**
- Create: `src/KafkaWorker.Provisioning/Processes/ReassignCli.cs`
- Create: `src/tests/KafkaWorker.UnitTests/Provisioning/ReassignCliTests.cs`

**Interfaces (produces, использует Task 5):**
```csharp
public static class ReassignCli
{
    /// <summary>bootstrap INTERNAL-listener живых брокеров: "broker1:9092,broker2:9092".</summary>
    public static string Bootstrap(IReadOnlyList<string> brokerNames);

    /// <summary>reassignment.json: {"version":1,"partitions":[{"topic","partition","replicas","log_dirs":["any"…]}]}.</summary>
    public static string BuildAssignmentJson(IReadOnlyList<ReassignMove> moves);

    /// <summary>SASL/PLAIN properties для --command-config (креды app из etcd).</summary>
    public static string BuildAdminProperties(string user, string password);

    /// <summary>sh -c команда: printf файлов (префикс kfw-) + CLI c KAFKA_HEAP_OPTS=-Xmx256m.</summary>
    public static IReadOnlyList<string> BuildExecCommand(
        IReadOnlyList<ReassignMove> moves, string bootstrap, string user, string password);
}
```

- [ ] **Шаг 4.1: Failing-тесты** (`ReassignCliTests.cs`):
  1. `Bootstrap_внутренний_listener`: `["broker1","broker2"]` → `"broker1:9092,broker2:9092"`.
  2. `BuildAssignmentJson_канонический_формат`: один move `("t",0,[1,2,3])` → JSON содержит `"version":1`, `"topic":"t"`, `"partition":0`, `"replicas":[1,2,3]`, `"log_dirs":["any","any","any"]` (длина log_dirs == replicas).
  3. `BuildAdminProperties_sasl_plain`: содержит `security.protocol=SASL_PLAINTEXT`, `sasl.mechanism=PLAIN`, `sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="app" password="p";`.
  4. `BuildExecCommand_без_апострофов_в_данных`: полная команда — `["sh","-c", <одна строка>]`; строка содержит `/opt/kafka/bin/kafka-reassign-partitions.sh`, `--execute`, `--bootstrap-server broker1:9092`, `--reassignment-json-file /tmp/kfw-reassign.json`, `--command-config /tmp/kfw-cmd.properties`, `KAFKA_HEAP_OPTS=-Xmx256m`; JSON-подстрока не содержит `'` (данные топиков/креды апострофов не несут — printf-обёртка безопасна).
- [ ] **Шаг 4.2: Прогон красный.** `--filter ReassignCli` — FAIL.
- [ ] **Шаг 4.3: Реализация**: JSON через `System.Text.Json` (JsonSerializer + record `ReassignmentJson(int Version, List<ReassignmentPart> Partitions)` с `JsonPropertyName`); строка exec:
```text
printf %s '<props>' > /tmp/kfw-cmd.properties && printf %s '<json>' > /tmp/kfw-reassign.json && KAFKA_HEAP_OPTS=-Xmx256m /opt/kafka/bin/kafka-reassign-partitions.sh --bootstrap-server <bootstrap> --command-config /tmp/kfw-cmd.properties --execute --reassignment-json-file /tmp/kfw-reassign.json
```
(переносы строк внутри — недопустимы, одна строка; spec §6).
- [ ] **Шаг 4.4: Прогон зелёный** + весь юнит-набор.
- [ ] **Шаг 4.5: Коммит.** `git commit -m "feat(kafkaworker): ReassignCli — сборка kafka-reassign-partitions вызова (t02 A4)"`.

**Spec:** §6 (CLI-вызов), arch/16 §2.4.

---

### Task 5: PartitionReassignerProcess (TDD)

**Files:**
- Modify: `src/KafkaWorker.Provisioning/Processes/ProcessCommon.cs` (+ `ReassignOptions`)
- Create: `src/KafkaWorker.Provisioning/Processes/PartitionReassignerProcess.cs`
- Create: `src/tests/KafkaWorker.UnitTests/Provisioning/PartitionReassignerProcessTests.cs`
- Create: `src/tests/KafkaWorker.UnitTests/Provisioning/FixedTimeProvider.cs` (порт `src/tests/AdminPanel.UnitTests/FixedTimeProvider.cs` — 10 строк: `public sealed class FixedTimeProvider : TimeProvider { public DateTimeOffset Utc { get; set; } = new(2026,1,1,0,0,0,TimeSpan.Zero); public override DateTimeOffset GetUtcNow() => Utc; }`, namespace `KafkaWorker.UnitTests.Provisioning`)

**Interfaces:**
```csharp
// ProcessCommon.cs:
/// <summary>Параметры процесса I (arch/16 §8): интервал/батч/бюджеты exec и переподачи.</summary>
public sealed record ReassignOptions(
    int IntervalSec,          // ReassignIntervalSec=15
    int BatchPartitions,      // ReassignBatchPartitions=10
    int ExecSec,              // ReassignExecSec=180
    int RetrySubmitSec)       // ReassignRetrySubmitSec=120
{
    public static ReassignOptions Default { get; } = new(15, 10, 180, 120);
}

// PartitionReassignerProcess.cs — ctor-паттерн существующих процессов:
public sealed class PartitionReassignerProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    IKafkaAdminClientFactory adminFactory,
    ReassignOptions options,
    TimeProvider timeProvider)
{
    public const string Op = "reassign";
    public Task<Result> RunAsync(KafkaClusterSnapshot snap, CancellationToken ct);
}
```
Ключи: `RebalanceKey(cluster)` = `/kafkaworker/rebalances/<C>`; `ProgressKey(cluster)` = `/kafkaworker/reassignments/<C>`. Прогресс-JSON (arch/15 §4, camelCase как WorkState):
```csharp
public sealed record ReassignProgress(
    [property: JsonPropertyName("mode")] string Mode,             // "drain"|"balance"
    [property: JsonPropertyName("drain_broker")] string? DrainBroker,
    [property: JsonPropertyName("partitions_total")] int PartitionsTotal,
    [property: JsonPropertyName("partitions_remaining")] int PartitionsRemaining,
    [property: JsonPropertyName("submitted_unix")] long SubmittedUnix,
    [property: JsonPropertyName("updated_unix")] long UpdatedUnix,
    [property: JsonPropertyName("instance")] string Instance,
    [property: JsonPropertyName("last_error")] string? LastError = null);
```

Алгоритм `RunAsync` (spec §5.1–§5.3; все etcd-обращения — failover-обёртка по `endpoints`, как в RemoveBrokerProcess):
1. `claims.IsMine(cluster)` → иначе Failed («клэйм не наш»).
2. Троттл: `_lastOk[cluster]` + `options.IntervalSec` (как TopicSyncProcess; провал — без штрафа).
3. Кластер не поднят (Endpoints/AppUser/AppPassword null) → жива заявка balance → journal `waiting-cluster`, иначе `Success`.
4. `admin.DescribeTopicsAsync(includeInternal: true, ct)` → `all`; Failed → journal `waiting-cluster` (слепая проба: никаких подач, прогресс-ключ НЕ трогается), `Success`.
5. Чтение заявки: `GetAsync(RebalanceKey)`.
6. **drain-кандидаты = `snap.Brokers` state=`TO_REMOVE` — БЕЗ фильтра по факту реплик** (завершение drain проверяется ниже по свежим метаданным `all` п.4: фильтр по DrainComplete здесь делал бы ветки завершения п.7 недостижимыми — метаданные не меняются внутри тика). Берём первого по `Name` (Ordinal). Кандидат есть → режим drain (balance-заявка ждёт: journal `waiting-drain`, если жива).
7. **Режим drain** (завершение — внутри drain-ветки, spec §5.2 D4):
   - targets = NodeId брокеров state=`RUNNING` (не TO_REMOVE/REMOVING/PROVISIONING/UNREACHABLE, не drain), отсортированы; пусто → journal-отказ `no-targets`.
   - `DrainComplete(all, drainId)` И `!HasUnderReplicated(all)` → del ProgressKey + journal `done` + `Success` (брокер остаётся TO_REMOVE — G демонтирует; повторные тики до демонтажа G идемпотентно перезапишут done).
   - `DrainComplete` но `HasUnderReplicated` → journal `waiting-sync`, прогресс обновить, `Success`.
   - Иначе план/подача: minIsrByTopic — для `__`-топиков `Math.Min(2, targets.Count)`, для юзер — `snap.Topics` реестр `Configs?["min.insync.replicas"]` parse int ?? `snap.Config.MinInSyncReplicas`; `var plan = ReassignPlanner.PlanDrain(all, drainId, targets, minIsr)`; Failed → journal-отказ c message (перманентное ожидание — spec §5.2 D3) + прогресс-ключ с `last_error`, `Success`.
   - Подача батча: `var batch = Pending(all, plan).Take(options.BatchPartitions)`; дедуп: если прошло < `RetrySubmitSec` с последней подачи (`_lastSubmit[cluster]` в памяти) — только put прогресс (remaining), `Success`; иначе exec (шаг 8) и put прогресса `submitted_unix=now`.
   - `partitions_total`: если прогресс-ключа не было — `remaining_now`, иначе сохранить прежний total; `remaining` = `Pending(all, plan).Count` по полному плану.
8. **exec**: цель — контейнер drain-брокера (drain) или первый `RUNNING` по Name (balance):
   ```csharp
   using var execCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
   execCts.CancelAfter(TimeSpan.FromSeconds(options.ExecSec));
   var cmd = ReassignCli.BuildExecCommand(batch, ReassignCli.Bootstrap(liveBrokerNames), snap.AppUser!, snap.AppPassword!);
   var exec = await driver.ExecNodeAsync(cluster, execNode, cmd, execCts.Token);
   ```
   `liveBrokerNames` — имена живых (state=RUNNING + drain при drain-режиме) брокеров для bootstrap. Failed → `Fail(...)` (journal + Failed) — следующий тик ретрай.
9. **Режим balance** (заявка жива, drain-кандидатов по п.6 нет): `plan = PlanBalance(all, targets, snap.Config.ReplicationFactor)`; `Pending` пусто → `del RebalanceKey` + `del ProgressKey` + journal `done`; иначе батч/дедуп/exec как в drain (exec-цель — первый RUNNING). Заявки нет И drain-кандидатов нет вовсе → прогресс-ключ жив = мусор оборванного баланса/отмены: del ProgressKey, journal `cancelled` (spec §7.1 «Отмена»).
10. `journal.WriteAsync(cluster, Op, phase, ...)` на каждом переходе; `Fail(...)`-обёртка как в RemoveBrokerProcess.

> **Ремарка (review MAJOR/MINOR-2 первой итерации, фиксация решения): фактическое снижение `min.insync.replicas` internal-топиков через AlterTopicConfigs НЕ реализуется.** Шаг spec §5.2 D3 («для internal-топиков minISR владеет воркер — снижает сам до min(2,B')») вырожден при всех достижимых состояниях: controller-ноды не демонтируются (guard G + серверный 409 панели) ⇒ при B≥3 живы ≥3 controller ⇒ targets ≥ 3; при B<3 все ноды controller и демонтаж невозможен вовсе ⇒ `min(2,B') = min(2,B) = 2` всегда, пока кластер отвечает describe (при потере кворума — слепая проба, процесс стоит). Состояние B'=1 недостижимо. Guard-таблица `minIsrByTopic` для `__`-топиков остаётся (инвариант плана). Отражено в коммит-сообщении этого шага.

- [ ] **Шаг 5.1: Failing-тесты** `PartitionReassignerProcessTests.cs` (FakeEtcd + FakeKafkaDriver + FakeKafkaAdminClient; ClaimStore реальный поверх FakeEtcd — сетап по прецеденту RemoveBrokerProcessTests). **Риг**: `new ReassignOptions(IntervalSec: 0, BatchPartitions: 10, ExecSec: 180, RetrySubmitSec: 120)` + `FixedTimeProvider` — троттл выключен, последовательные RunAsync в одном «времени» не отсекаются (прецедент `TopicSyncProcessTests.NewRigAsync(intervalSec: 0)`; тесты изолируют именно дедуп `RetrySubmitSec`):
  1. `Drain_подаёт_батч_и_пишет_прогресс`: кластер Active (endpoints/креды в etcd-сиде, брокеры RUNNING + один TO_REMOVE), fake.Topics с репликами на drain; ExecHandler → Success. Act: RunAsync. Assert: `driver.Execs` один вызов (cmd содержит `--execute`); ключ `/kafkaworker/reassignments/<C>` существует, `mode=drain`, `partitions_remaining > 0`.
  2. `Drain_завершение_очищает_прогресс`: брокер TO_REMOVE в снапшоте, fake.Topics уже без его реплик, ISR==Replicas → RunAsync: exec НЕТ, ключ удалён, journal `done` (ReadAsync work → op=reassign) — завершение внутри drain-ветки (кандидат отобран по state, п.6).
  3. `Drain_minISR_отказ`: targets меньше minISR юзер-топика → RunAsync: exec НЕТ, прогресс-ключ с `last_error` содержащим `min.insync.replicas`, Result Success (ожидание, не ошибка тика).
  4. `Drain_USR_держит_завершение`: брокер TO_REMOVE, реплик его нет, но Isr < Replicas → ключ жив, journal waiting-sync, exec нет.
  5. `Drain_дедуп_переподачи`: реплики есть; два RunAsync подряд (факт не движется, время статично — троттл=0) → ровно один exec (второй — только put прогресса); `FixedTimeProvider.Utc += RetrySubmitSec` → третий RunAsync подаёт повторно.
  6. `Balance_исполняет_и_снимает_заявку`: TO_REMOVE-кандидатов нет; заявка rebalances в FakeEtcd; первый RunAsync подаёт батч; тест вручную выставляет Topics = факту == план → второй RunAsync: del заявки + del прогресса.
  7. `Balance_ждёт_drain`: заявка + TO_REMOVE-брокер (с репликами) → режим drain (exec только drain), journal waiting-drain для заявки.
  8. `Balance_отмена_заявки`: заявки нет, drain-кандидатов нет, прогресс-ключ есть → ключ удалён, exec нет, journal cancelled.
  9. `Слепая_проба`: fake.TopicsError → journal waiting-cluster, exec нет, Result Success; **прогресс-ключ не тронут**: сид ключа с известным значением перед RunAsync → значение после идентично (spec §11.7: «прошлый прогресс сохраняется»).
  10. `Клэйм_не_наш`: без TryClaimClusterAsync → Failed.
- [ ] **Шаг 5.2: Красный прогон.** `--filter PartitionReassigner` — FAIL.
- [ ] **Шаг 5.3: Реализация** процесса по алгоритму выше (журнал до мутаций — journal-before-manipulations; ремарка о minISR internal — в xml-комментарии шага 7).
- [ ] **Шаг 5.4: Зелёный прогон** + весь юнит-набор воркера.
- [ ] **Шаг 5.5: Коммит.** `git commit -m "feat(kafkaworker): PartitionReassignerProcess — drain/balance оркестрация (t02 A5); фактическое снижение minISR internal не требуется: min(2,B')=2 при всех достижимых B' (controller-ноды не демонтируются)"`.

**Spec:** §5.1–§5.4, §4 (ключи/формат), arch/16 §5 I; ремарка — §5.2 D3 (вырожденность шага зафиксирована).

---

### Task 6: Интеграция в конвейер + фикс G + DI + конфигурация

**Files:**
- Modify: `src/KafkaWorker.Provisioning/Processes/RemoveBrokerProcess.cs`
- Modify: `src/KafkaWorker.Provisioning/Processes/DeprovisioningProcess.cs` (~строки 119–125)
- Modify: `src/KafkaWorker.App/Loops/KafkaClusterProcesses.cs`
- Modify: `src/KafkaWorker.App/Options.cs`, `Program.cs`, `appsettings.json`
- Modify: `src/tests/KafkaWorker.UnitTests/Provisioning/RemoveBrokerProcessTests.cs`, `DeprovisioningProcessTests.cs`

- [ ] **Шаг 6.1: Регресс-тест guard'а G (фиксация контракта).** В `RemoveBrokerProcessTests.cs` добавить кейс `HasPartitions_учитывает_internal_топики`: fake.Topics = только `__consumer_offsets` с репликой `[1,2,3]`, кандидат broker3 TO_REMOVE → RunAsync: демонтажа НЕТ (`driver.Removed` пуст), journal-фаза `waiting-partitions`. Тест зелёный сразу (без TDD-красного): фейк не фильтрует `__`-топики (Task 1.1 — фильтрация живёт в адаптере), поэтому guard и до правки видит internal-реплики на фейке; тест фиксирует контракт «guard удерживает брокер с репликами `__consumer_offsets`» от будущих регрессов. Реальную проверку того, что адаптер с `includeInternal: true` видит `__`-топики, даёт интеграционный T7.1 (describe includeInternal: true — ни одна партиция, включая `__consumer_offsets`, не содержит nodeId=4).
- [ ] **Шаг 6.2: Фикс прод-кода.** `RemoveBrokerProcess.HasPartitionsAsync` (~строка 110) → `DescribeTopicsAsync(includeInternal: true, ct)` — реальный адаптер начнёт видеть internal-реплики (раньше фильтр `__` в `DescribeTopicsViaMetadata` прятал их от guard'а: брокер только с `__consumer_offsets` считался «пустым» и демонтировался); текст journal-ожидания: `$"на {broker.Name} есть реплики партиций — drain идёт (процесс reassign), демонтаж продолжится сам"`. Прогон: весь набор зелёный.
- [ ] **Шаг 6.3: X2-очистка координации.** `DeprovisioningProcess.cs` (массив `deletions`, ~строки 119–125): добавить два ключа рядом с ротацией (spec §11.9, arch/16 §3.2 — вечные заявки/прогресс не переживают кластер):
```csharp
($"/kafkaworker/rebalances/{cluster}", false),     // заявка ребалансировки
($"/kafkaworker/reassignments/{cluster}", false),  // прогресс reassignment
```
Плюс юнит-кейс в `DeprovisioningProcessTests.cs`: сид обоих ключей → RunAsync → оба удалены (по образцу существующего rotations-кейса).
- [ ] **Шаг 6.4: Конвейер.** `KafkaClusterProcesses`: ctor += `PartitionReassignerProcess reassigner`; в `ActiveAsync` после converge-блока и ДО `removeBroker.RunAsync`:
```csharp
// Reassignment (I) перед remove: к моменту G дренируемый брокер пуст
// (drain TO_REMOVE-кандидатов + заявка balance; arch/16 §5 классификация).
var reassigned = await reassigner.RunAsync(snap, ct);
if (!reassigned.IsSuccess)
    return reassigned;
```
- [ ] **Шаг 6.5: Опции и DI.** `Options.cs`: `LoopsOptions` += `public int ReassignIntervalSec { get; set; } = 15;` и `public int ReassignBatchPartitions { get; set; } = 10;`; `ThresholdsOptions` += `public int ReassignExecSec { get; set; } = 180;` и `public int ReassignRetrySubmitSec { get; set; } = 120;`. `Program.cs`: регистрация
```csharp
builder.Services.AddSingleton(sp => new PartitionReassignerProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<IKafkaAdminClientFactory>(),
    new ReassignOptions(
        opts.Loops.ReassignIntervalSec, opts.Loops.ReassignBatchPartitions,
        opts.Thresholds.ReassignExecSec, opts.Thresholds.ReassignRetrySubmitSec),
    sp.GetRequiredService<TimeProvider>()));
```
(по образцу TopicSyncProcess-регистрации; appsettings.json — добавить 4 ключа с дефолтами 15/10/180/120).
- [ ] **Шаг 6.6: Проверка.** Сборка 0 warnings; весь юнит-набор зелёный.
- [ ] **Шаг 6.7: Коммит.** `git commit -m "feat(kafkaworker): reassigner в Active-ветке + guard G по describe-all + опции + X2-очистка (t02 A6)"`.

**Spec:** §1 (дефект guard — фикс в адаптере, контракт — регресс-тестом, сквозная гарантия — T7.1), §5.1 (порядок), §5.5, §11.9.

---

### Task 7: Integration — drain непустого брокера со снижением RF (4→3)

**Files:**
- Create: `src/tests/KafkaWorker.IntegrationTests/Kafka/ReassignmentTests.cs`

Прецедент: `ProvisioningTests.cs` (процессы напрямую + поллинг-цикл ≤ N сек; `[Collection(KafkaCollection.Name)]`).

- [ ] **Шаг 7.1: Тест `Drain_RemovesNonEmptyBroker_Снижает_RF`.**
  - Arrange: `SeedClusterAsync("re1", brokers: 4)` (m=min(3,4)=3 controller — broker1..3; broker4 broker-only — демонтаж разрешён) → provisioning-цикл до готовности (как ProvisioningTests, ≤ 180 c); создать юзер-топик через `fixture.DiscoveryAdminBuilderAsync("re1")` + `CreateTopicsAsync` **RF=4, 6 партиций** (покрытие §11.3: при drain broker4 targets=3 < RF=4 → план снижает RF до 3); produce несколько сообщений (креды из etcd — приёмка §11.2 «данные читаются»).
  - Act: put `brokers/broker4/state=TO_REMOVE`; цикл `reassigner.RunAsync(snap)` + `removeBroker.RunAsync(snap)` + `topicSync.RunAsync(snap)` тиками (процессы собрать как в ProvisioningTests) до исчезновения `brokers/broker4/` (бюджет ≤ 300 c).
  - Assert: ключ `brokers/broker4/state` нет; `endpoints` без адреса broker4; `ListNodeObjects` не содержит `kfw-re1-broker4`; прогресс-ключ `/kafkaworker/reassignments/re1` удалён; describe (`includeInternal: true`) — ни одна партиция (включая `__consumer_offsets`) не содержит nodeId=4 (в т.ч. сквозная гарантия guard'а G из Task 6); **снижение RF: describe показывает ровно 3 реплики у каждой партиции юзер-топика; `topics/<T>` реестр `replication_factor == 3` после тика TopicSync (автосинк обновил факт)**; сообщения топика читаются (consume с кредами из etcd).
- [ ] **Шаг 7.2: Тест `Reassign_Повторная_Подача_Безопасна`** (§11.7 идемпотентность на уровне Kafka): после завершения drain — повторный `reassigner.RunAsync` на стабильном факте: describe до/после идентичен по assignment всех партиций; exec не падает (кластер консистентен).
- [ ] **Шаг 7.3: Прогон (Docker required).** `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test src/tests/KafkaWorker.IntegrationTests --filter Reassignment` — PASS (RF=4/6 партиций с короткими сообщениями — копирование быстрое; бюджет 300 c).
- [ ] **Шаг 7.4: Коммит.** `git commit -m "test(kafkaworker): integration — drain непустого брокера со снижением RF 4→3 (t02 A7)"`.

**Spec:** §11.2 (drain непустого, данные целы), §11.3 (снижение RF + обновление реестра автосинком — интеграционно), §11.5 (internal), §11.7 (идемпотентность повторной подачи).

---

### Task 8: Integration — повторный add + balance: восстановление RF после снижения + контрольный прогон волны A

**Files:**
- Modify: `src/tests/KafkaWorker.IntegrationTests/Kafka/ReassignmentTests.cs`

- [ ] **Шаг 8.1: Тест `Balance_Восстанавливает_RF_После_Повторного_Add`.** Кластер `re2` — старт как Task 7: 4 брокера, юзер-топик **RF=4**, drain broker4 (тик-цикл reassigner+removeBroker+topicSync до демонтажа) → assert снижения: describe RF==3, реплики {1,2,3}, `topics/<T>.replication_factor==3`, ключей `brokers/broker4/` нет (в кластере 3 RUNNING-брокера). **Повторный add четвёртого брокера** (без него восстановление RF=4 недостижимо — targets=3 ≤ RF_цели, заявка снялась бы сразу):
  - put `/kafka/clusters/re2/brokers/broker4/state` = `NOT_INITIALIZED` + put `/kafka/clusters/re2/brokers/broker4/resources` = `{"cpu":"1","mem":"1Gi","disk":"10Gi"}` (формат — как `SeedClusterAsync`);
  - тик-цикл `addBroker.RunAsync(snap)` (процесс собрать по образцу ProvisioningTests: gateway, driver, claims, journal, adminFactory, options) до `brokers/broker4/state == RUNNING` (бюджет ≤ 180 c; NodeId=4 сохранится — `BrokerEnvBuilder.NodeId("broker4")`; роль `broker` перепоставит AddBrokerProcess — `EnsurePortsAsync` пишет role при null, демонтаж удалил role-ключ; клиентский порт перевыделят portalloc — при демонтаже запись отфильтрована);
  - assert `endpoints` содержит адрес broker4, DescribeCluster видит 4 брокеров.
  Затем: put `/kafka/clusters/re2/config` канонический JSON c `"replication_factor":4` (без state, как сид); put `/kafkaworker/rebalances/re2` `{"requested_unix":<now>,"requested_by":"it"}`; цикл reassigner-тиков (+topicSync) ≤ 300 c до исчезновения заявки. Assert: заявка и прогресс-ключ удалены; describe — каждая партиция юзер-топика имеет **4 реплики, среди них nodeId=4** (targets=4 → план min(4,4)=4, добор 4-й реплики), первая реплика (лидер) unchanged относительно факта до баланса; `topics/<T>.replication_factor == 4` (автосинк-тик в цикле). Полное покрытие §11.3-цепочки «снижение → повторный add + rebalance → восстановление» и §11.6.
- [ ] **Шаг 8.2: Прогон.** `--filter Reassignment` — все PASS.
- [ ] **Шаг 8.3: Контрольный прогон волны A.** `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet build src/PgWorker.slnx` (0 warnings) + `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test src/tests/KafkaWorker.UnitTests` + `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test src/tests/KafkaWorker.IntegrationTests --filter Kafka` — всё зелёное.
- [ ] **Шаг 8.4: Коммит.** `git commit -m "test(kafkaworker): integration — повторный add + balance восстанавливает RF после снижения (t02 A8)"`.

**Spec:** §3.4, §11.3 (восстановление через повторный add + rebalance), §11.6.

---

## Волна B — панель и стенд

### Task 9: Чтение rebalances/reassignments (модель, парсеры, refresher)

**Files:**
- Modify: `src/AdminPanel.Core/Kafka/KafkaSnapshot.cs`
- Modify: `src/AdminPanel.Etcd/Parsing/KafkaParser.cs`
- Modify: `src/AdminPanel.Etcd/KafkaSnapshotRefresher.cs`
- Modify: `src/tests/AdminPanel.UnitTests/KafkaParserTests.cs`, `src/tests/AdminPanel.UnitTests/KafkaRefresherTests.cs` (оба — в корне тест-проекта)

**Interfaces (produces, используют Task 10/11):**
```csharp
// KafkaSnapshot.cs:
/// <summary>Заявка ребалансировки /kafkaworker/rebalances/<C> (arch/15 §4).</summary>
public sealed record KafkaRebalanceTicket(string Cluster, long RequestedUnix, string? RequestedBy);

/// <summary>Прогресс reassignment /kafkaworker/reassignments/<C> (arch/15 §4);
/// отсутствие ключа = операции нет.</summary>
public sealed record KafkaReassignmentProgress(
    string Cluster, string Mode, string? DrainBroker,
    int PartitionsTotal, int PartitionsRemaining, long UpdatedUnix, string? LastError);

// KafkaSnapshot: += IReadOnlyList<KafkaRebalanceTicket> Rebalances,
//                 IReadOnlyList<KafkaReassignmentProgress> Reassignments (после Rotations)

// KafkaParser:
public static KafkaRebalancesParseResult ParseRebalances(IReadOnlyList<Kv> kvs); // формат ротаций
public static KafkaReassignmentsParseResult ParseReassignments(IReadOnlyList<Kv> kvs);
// обязательные поля mode/partitions_remaining/updated_unix отсутствуют → KeyParseError
```
Refresher: `Prefixes` += `Rebalances = "/kafkaworker/rebalances/"`, `Reassignments = "/kafkaworker/reassignments/"`; два RangeWithFailover-чтения (провал любого роняет тик — как rotations); в конструктор `KafkaSnapshot` добавить списки; ParseErrors — конкатенация всех четырёх парсеров.

- [ ] **Шаг 9.1: Failing-тесты** (по прецеденту `ParseRotations`-тестов в `src/tests/AdminPanel.UnitTests/KafkaParserTests.cs`): валидный ключ заявки → ticket; валидный прогресс (mode=drain, drain_broker, totals) → progress; битый JSON / нет обязательных полей / мусорный префикс → KeyParseError; refresher-тест (`src/tests/AdminPanel.UnitTests/KafkaRefresherTests.cs`): сид FakeEtcd с ключами → snapshot.Rebalances/Reassignments заполнены, битый ключ → parseError (алерт `kafka-key-malformed` появится в Task 12 — пока ParseErrors).
- [ ] **Шаг 9.2: Красный, реализация, зелёный.** `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test src/tests/AdminPanel.UnitTests --filter FullyQualifiedName~Kafka`.
- [ ] **Шаг 9.3: Коммит.** `git commit -m "feat(adminpanel): чтение rebalances/reassignments в kafka-снапшот (t02 B1)"`.

**Spec:** §7.1, arch/15 §4.

---

### Task 10: Мутации 9/10 — заявка и отмена ребалансировки (TDD)

**Files:**
- Create: `src/AdminPanel.Api/Operations/Kafka/RebalanceCommands.cs`
- Modify: `src/AdminPanel.Api/Operations/Kafka/KafkaOperationsModule.cs`
- Modify: `src/tests/AdminPanel.UnitTests/Kafka/KafkaCommandTests.cs` (+ harness `KafkaCommandHarness.cs` — переиспользуется)

**Interfaces (produces, использует Task 14/e2e):** точный порт `RotateKafkaPasswordCommand` (KafkaCommands.cs:367-423) и эндпоинта ротации:
```csharp
public sealed record RequestKafkaRebalanceCommand(string Cluster, string RequestedBy)
    : ICommand<KafkaRebalanceRequestedDto>;
public sealed record KafkaRebalanceRequestedDto(string Cluster, long RequestedUnix, string RequestedBy);
public sealed class KafkaRebalanceAlreadyRequestedException(string cluster)
    : Exception($"ребалансировка партиций {cluster} уже запрошена — дождитесь исполнения или отмените");

public sealed record CancelKafkaRebalanceCommand(string Cluster) : ICommand<KafkaRebalanceCancelledDto>;
public sealed record KafkaRebalanceCancelledDto(string Cluster);
public sealed class KafkaRebalanceNotFoundException(string cluster)
    : Exception($"заявка ребалансировки {cluster} не найдена");
```
Протокол заявки (adminpanel/02 §10.2-9): имя каноническое (`KafkaLimits.ClusterPattern()`), config Active (404/409 как ротация), ReadKey живой → 409, клэйм-txn `[TxnCompare(key, 0)] [KvPut(key, ticket)]`. Отмена (§10.2-10): ReadKey → null → 404; `gateway.DeleteAsync(endpoint, key, prefix: false)` → 204.

- [ ] **Шаг 10.1: Failing-тесты** в `src/tests/AdminPanel.UnitTests/Kafka/KafkaCommandTests.cs` — точный порт `RotateKafkaPasswordCommandTests` (строки 377–436), harness `FakeKafkaEtcd`/`SeedActiveCluster`/`StoreWithEndpoint` уже в `KafkaCommandHarness.cs`:
  1. `RequestRebalance_ActiveCluster_ClaimsTicket` (порт `Handle_ActiveCluster_ClaimsRotationTicket`): Success + `etcd.Store["/kafkaworker/rebalances/events"]` содержит `"requested_by":"admin"` + `etcd.Txns` содержит compare `Key == "/kafkaworker/rebalances/events" && Version == 0`.
  2. `RequestRebalance_TicketAlreadyLive_409`: сид заявки `{"requested_unix":…,"requested_by":"ops"}` → `KafkaRebalanceAlreadyRequestedException`, значение ключа не перезаписано (`Contain("ops")`).
  3. `RequestRebalance_NotActive_409`: config с `"state":"TO_REMOVE"` → `KafkaClusterNotActiveException`.
  4. `CancelRebalance_RemovesTicket`: сид заявки → Handle `CancelKafkaRebalanceCommand("events")` → Success, ключа в Store нет.
  5. `CancelRebalance_NoTicket_404`: пустой etcd → `KafkaRebalanceNotFoundException`.
- [ ] **Шаг 10.2: Красный прогон.** `--filter Rebalance` — FAIL (команды не существуют).
- [ ] **Шаг 10.3: Реализация команд** (`RebalanceCommands.cs`; порт ротации: 409-чек до txn + клэйм-txn; RequestedBy = `user.Identity?.Name ?? "adminpanel"`); эндпоинты в модуль (по образцу rotate, comment «02 §10.2-9/10»): `POST /api/kafka/clusters/{cluster}/rebalance` → 201 `KafkaRebalanceRequestedDto`; 404 `KafkaClusterNotFoundException`; 409 `KafkaClusterNotActiveException or KafkaRebalanceAlreadyRequestedException`; 503 EtcdWrite*/InvalidConfig. `DELETE /api/kafka/clusters/{cluster}/rebalance` → 204; 404 `KafkaClusterNotFoundException or KafkaRebalanceNotFoundException`; 503.
- [ ] **Шаг 10.4: Зелёный прогон** + весь AdminPanel.UnitTests.
- [ ] **Шаг 10.5: Коммит.** `git commit -m "feat(adminpanel): POST/DELETE rebalance — заявка и отмена ребалансировки, TDD (t02 B2)"`.

**Spec:** §7.2, adminpanel/02 §10.2-9/10.

---

### Task 11: DTO деталей кластера (rebalance/reassignment)

**Files:**
- Modify: `src/AdminPanel.Api/Inspection/KafkaQuery.cs` (маппинг KafkaClusterDto — свериться с фактическим маппером в файле)
- Modify: `src/tests/AdminPanel.UnitTests/KafkaModelTests.cs` (корень тест-проекта)
- Modify: `frontend/src/api/dto.ts` (типы), `frontend/src/api/queries.ts` (requestKafkaRebalance/cancelKafkaRebalance — по образцу rotateKafkaPassword)

DTO (adminpanel/03 §7.2): `KafkaClusterDto` += `rebalance{requestedUnix, requestedBy}?`, `reassignment{mode, drainBroker?, partitionsTotal, partitionsRemaining, updatedUnix}?`; summary += `rebalancePending(bool)`. Источник: snapshot.Rebalances/Reassignments по имени кластера (join в маппере запроса). camelCase-JSON как существующее.

- [ ] **Шаг 11.1: Тест маппинга** (в `src/tests/AdminPanel.UnitTests/KafkaModelTests.cs` — по прецеденту существующих кейсов): snapshot с ticket+progress → DTO-поля заполнены; без — null.
- [ ] **Шаг 11.2: Реализация** (Api + dto.ts + queries.ts: `export async function requestKafkaRebalance(cluster: string)` POST без тела → `KafkaRebalanceRequestedDto`; `cancelKafkaRebalance(cluster)` DELETE → void).
- [ ] **Шаг 11.3: Проверка + коммит.** `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test src/tests/AdminPanel.UnitTests`; `cd frontend && npm run build`; `git commit -m "feat(adminpanel): rebalance/reassignment в DTO кластера + api-клиент (t02 B3)"`.

**Spec:** §7.3, adminpanel/03 §7.2.

---

### Task 12: Алерты

**Files:**
- Modify: `src/AdminPanel.Core/Kafka/KafkaAlerting/KafkaAlertEngine.cs`, `KafkaAlertsOptions.cs`
- Modify: `src/tests/AdminPanel.UnitTests/KafkaAlertRulesTests.cs` (корень тест-проекта)

- [ ] **Шаг 12.1: Failing-тесты**: (а) живая заявка живого кластера → info `kafka-rebalance-pending` (target=кластер; у мёртвого кластера — нет, как ротация); (б) прогресс жив, `partitions_remaining` не менялся дольше `ReassignStaleSec` (дефолт 900) → warning `kafka-reassignment-stale` (сравнение prev.Reassignments по cluster: same remaining && now−updatedUnix > threshold; prev нет → алерт по возрасту updatedUnix).
- [ ] **Шаг 12.2: Реализация**: `KafkaAlertsOptions` += `public int ReassignStaleSec { get; set; } = 900;`; в `Enumerate` (метод `KafkaAlertEngine`, строка ~37) после ротационных — два блока по образцу rotation-pending.
- [ ] **Шаг 12.3: Зелёный прогон + коммит.** `git commit -m "feat(adminpanel): алерты rebalance-pending/reassignment-stale (t02 B4)"`.

**Spec:** §7.3 (алерты), adminpanel/03 §7.4.

---

### Task 13: UI — кнопка, бейджи, подпись drain

**Files:**
- Create: `frontend/src/pages/kafka-cluster/RebalanceButton.tsx`
- Modify: `frontend/src/pages/kafka-cluster/KafkaClusterDetailsPage.tsx`, `BrokersTab.tsx`

- [ ] **Шаг 13.1: `RebalanceButton.tsx`** — точный порт `RotatePasswordButton.tsx` (модал + mutation + invalidate `['kafka-clusters']`): текст «Перебалансировать», предупреждение «перенос данных между брокерами; длительность зависит от объёма; доступность сохраняется (reassignment без даунтайма)»; кнопка дизейблится при `disabled` (не Active); если заявка уже жива (`cluster.rebalance !== null`) — вместо «Отправить» кнопка «Отменить ребалансировку» (mutation cancel, подтверждение «поданные батчи Kafka доиграет сама»); 409/404 — текстом в Alert модала.
- [ ] **Шаг 13.2: Шапка деталей.** В `KafkaClusterDetailsPage`: рядом с бейджем ротации — `c.reassignment !== null && <Tooltip…><Badge color="violet" variant="light">{c.reassignment.mode === 'drain' ? \`drain ${c.reassignment.drainBroker}\` : 'ребалансировка'}: осталось {c.reassignment.partitionsRemaining}/{c.reassignment.partitionsTotal} партиций</Badge></Tooltip>`; в группу кнопок — `<RebalanceButton cluster={c.name} rebalance={c.rebalance} disabled={!active} />`. Значения `mode` — нижний регистр `"drain"|"balance"` (канон: spec §4, arch/15 §4, `ReassignProgress.Mode` Task 5, сид Task 14.1, DTO Task 11) — нормализацию регистра в DTO не вводим.
- [ ] **Шаг 13.3: BrokersTab.** У строки брокера state=`TO_REMOVE` при живом reassignment с `drainBroker === broker.name` — подпись «drain: осталось N партиций» (prop `reassignment` прокинуть из страницы).
- [ ] **Шаг 13.4: Проверка + коммит.** `cd frontend && npm ci --prefer-offline >/dev/null 2>&1 || npm ci; npm run build` — без ошибок TS; `git commit -m "feat(adminpanel-ui): ребалансировка — кнопка/отмена, прогресс-бейджи, подпись drain (t02 B5)"`.

**Spec:** §7.3 (UI), adminpanel/03 §7.3.

---

### Task 14: Сид и e2e-чеки + контрольный прогон

**Files:**
- Modify: `dev-stand/adminpanel/kafka-seed.sh`, `checks/50-kafka-api.sh`, `checks/55-kafka-e2e.sh`

- [ ] **Шаг 14.1: Сид.** В `kafka-seed.sh` рядом с заявкой ротации (строки ~48-62) добавить: `put /kafkaworker/rebalances/events '{"requested_unix":1756500123,"requested_by":"seed"}'` и `put /kafkaworker/reassignments/events '{"mode":"drain","drain_broker":"broker2","partitions_total":6,"partitions_remaining":3,"submitted_unix":1756500130,"updated_unix":1756500135,"instance":"seed"}'` + проверки чтения (по образцу строк 62).
- [ ] **Шаг 14.2: `50-kafka-api.sh`** (сид-профиль): негативы и позитивы по образцу существующих kafka-блоков: `POST /api/kafka/clusters/events/rebalance` → 409 (заявка уже жива с сида); `DELETE /api/kafka/clusters/events/rebalance` → 204 (отмена); повторный `DELETE` → 404; `POST` на несуществующий `nope` → 404; детали `GET /api/kafka/clusters/events` содержат `"rebalance"` и `"reassignment"` (progress с сида).
- [ ] **Шаг 14.3: `55-kafka-e2e.sh`.** После шага «8) демонтаж broker-only» (текущий: демонтаж пустого) заменить/расширить порядок: (8a) `POST brokers` add → ждём RUNNING; (8b) kafka_cli create topic `e2e2` **RF=4, partitions=6** (кластер e2e к этому моменту 4-брокерный), produce сообщения; (8c) `DELETE brokers/broker<N>` — НЕПУСТОЙ broker-only: ждём прогресс-ключ `/kafkaworker/reassignments/e2e` (etcd_has, бюджет 120 c), ждём демонтажа: `! etcd_has /kafka/clusters/e2e/brokers/broker<N>/state` (бюджет 300 c); kafka_cli describe `e2e2`: брокер N в репликах отсутствует, у партиций 3 реплики (снижение RF 4→3), IsrCount>0 (упрощённый parse `--describe`); etcd-ключ `topics/e2e2` содержит `"replication_factor":3` (после тика автосинка — бюджет 60 c); (9) rebalance-заявочный протокол: `POST /api/kafka/clusters/e2e/rebalance` → 201; повтор → 409; кластер после демонтажа 3-брокерный — восстановление RF=4 требует повторного add, это покрыто integration-тестом Task 8.1 (в e2e сознательно НЕ повторяем: длительность); в e2e проверяем корректность заявочного цикла на 3-брокерном факте: ждём `! etcd_has /kafkaworker/rebalances/e2e` (бюджет 300 c; факт==план RF=3 → заявка снимется) + describe показывает RF=3 (план не ухудшил); отмена-негатив `DELETE` → 404; (10) TO_REMOVE кластера как раньше.
- [ ] **Шаг 14.4: Прогон чеков.** Стенд: `cd dev-stand/adminpanel && docker compose up -d etcd` (+ сид-профиль) → панель хост-процессом (`dotnet run --project src/AdminPanel.Api`, AdminPanel__Probes__Password) → `./checks/50-kafka-api.sh`; e2e: `./checks/55-kafka-e2e.sh` (сам собирает образ воркера и поднимает профиль kafka — прочитать шапку чека и следовать ей). Оба зелёные с чистого состояния (`./checks/00-up.sh`/`90-down.sh` по необходимости).
- [ ] **Шаг 14.5: Контрольный прогон всего.** `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet build src/PgWorker.slnx` (0 warnings); `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test` (все тест-проекты); `cd frontend && npm run build`.
- [ ] **Шаг 14.6: Коммит.** `git commit -m "test(stand): e2e — drain непустого брокера (снижение RF) и ребалансировка (t02 B6)"`.

**Spec:** §8, §11 (приёмки 2/3/6/8/10).

---

## Self-review плана (выполнен при составлении; обновлён после review Фазы 4 — итерации 1, 2 и 3)

1. **Покрытие spec**: §3.1→Task 2/4; §3.2→Task 3 (DrainComplete/HasUnderReplicated); §3.3→Task 3 (minIsr) + Task 5; §3.4→Task 3/8; §4→Task 5/9; §5.1→Task 6.4; §5.2→Task 5/3 (в т.ч. D4-завершение — внутри drain-ветки: кандидаты отбираются по state=TO_REMOVE без фильтра по факту, ветки done/waiting-sync достижимы — тесты 5.1 №2/№4); §5.3→Task 5; §5.4→Task 5 (тесты 5/9/10) + Task 7.2; §5.5→Task 1/2/6; §6→Task 4; §7.1→Task 9; §7.2→Task 10; §7.3→Task 11/12/13; §8→Task 14; §10 (минимизация)→ремарка Task 5 (minISR internal); §11 приёмки: 11.2→T7.1, 11.3→T7.1 (снижение RF=4→3 + автосинк реестра, интеграционно) и T8.1 (повторный add broker4 + rebalance → восстановление RF=4 на nodeId=4 — полная цепочка §11.3; шаг повторного add обязателен, без него targets=3 и заявка снялась бы без движения), 11.4→T5.3 (юнит; integration недостижим: при B=2 оба брокера controller — демонтаж запрещён сервером 409, зафиксировано в T7), 11.5→T7.1 (describe includeInternal), 11.6→T8.1, 11.7→T5 тест 5 (дедуп/идемпотентность) + T5 тест 9 (прогресс-ключ не тронут при слепой пробе) + T7.2 (повторная подача); **takeover ≤ TTL 15 с — свойство существующего ClaimStore/KeepaliveLoop (порт PgWorker, покрыто их тестами), отдельного теста в t02 нет**; 11.8→T9/T12/T13/T14; 11.9→Task 6 шаг 6.3 (X2 del-набор явно расширяется ключами rebalances/reassignments; факт по коду: DeprovisioningProcess чистит явным списком ключей); 11.10→T14.4. Согласованность T8.1 ↔ T14.3: полный сценарий восстановления RF (add + rebalance) — integration T8.1; e2e T14.3(9) сознательно проверяет только заявочный протокол на 3-брокерном факте с явной отсылкой к T8.1 — противоречия нет.
2. **Placeholders**: шаг «свериться с фактическим именем/паттерном» используется только там, где прецедент в репо гарантированно существует и назван файл-источник — не TBD.
3. **Консистентность типов**: `ReassignMove`, `ReassignOptions`, `ReassignProgress`, `KafkaRebalanceTicket`, `KafkaReassignmentProgress`, `ExecNodeAsync`, `DescribeTopicsAsync(includeInternal, ct)` — единые по всем задачам; `KafkaTopicView.IsrPerPartition` — опциональный (`= null`): 3-аргументные конструкторы существующих тестов (RemoveBrokerProcessTests.cs:85,151; TopicSyncProcessTests.cs:67) компилируются без правок, `null` = «ISR не задан» (адаптер заполняет всегда; HasUnderReplicated при null → false).
4. **TDD-исключения зафиксированы явно**: Шаг 6.1 — регресс-тест зелёный сразу (фейк не фильтрует `__`; реальный фильтр адаптера подтверждается интеграционно T7.1); Шаг 5.1 — риг с `ReassignOptions(IntervalSec: 0, …)` + `FixedTimeProvider` (прецедент `TopicSyncProcessTests.NewRigAsync(intervalSec: 0)`) — троттл не мешает последовательным RunAsync в тестах 5/6.

## Замечания для исполнителя

- Порядок задач строгий (A1→A8, B1→B14); каждая задача самодостаточна для ревью.
- Если integration-тест Task 7/8 выявит расхождение поведения `kafka-reassign-partitions.sh` (выходной формат/флаги 4.0.0) — фиксировать фактический формат в `ReassignCliTests` и коде; семантика (`--execute --reassignment-json-file`) канонична для 4.x.
- `NodeId(broker<k>)` = k (существующий `BrokerEnvBuilder.NodeId`); заявки etcd-тестов пишутся через `FakeEtcd.Seed` / `fixture.Gateway.PutAsync`.
- Снижение фактического minISR internal-топиков в Kafka не реализуется (ремарка Task 5): `min(2,B')=min(2,B)=2` при всех достижимых B' — контроллеры не демонтируются, при потере кворума процесс стоит по слепой пробе.
- Task 8.1: повторный add broker4 возвращает NodeId=4 (детерминирован именем), роль/порт AddBrokerProcess восстанавливает сам (role-ключ удалён демонтажом — `EnsurePortsAsync` пишет `broker` при null; portalloc-запись отфильтрована — порт перевыделяется). KRaft-повторное использование NodeId с чистым томом корректно: нода broker-only, метаданные живут у контроллеров.
- Task 5 п.6: drain-кандидаты — ТОЛЬКО по state=TO_REMOVE; проверка завершённости (DrainComplete/USR) — в п.7 по свежим метаданным тика (иначе done/waiting-sync — мёртвый код). Идемпотентность повторных тиков после done — G демонтирует брокер, повторный done-тик безопасен.
