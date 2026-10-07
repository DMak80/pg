# t17-rpo-rto-dashboard — план реализации (Фаза 3, dev-flow; правка 3 по ревью)

> **Для исполняющих агентов:** REQUIRED SUB-SKILL: `superpowers:subagent-driven-development` (рекомендуется) или `superpowers:executing-plans` — исполнять задачу за задачей. Шаги используют чекбоксы (`- [ ]`); каждый шаг несёт Вход/Действие/Выход/Проверка/Связь со spec.

**Цель:** RPO/RTO-числа оператору — возраст валидного полного, лаг WAL, сводный RPO-потенциал, фактические длительности последнего failover/rebuild/drill/restore per-shard в AdminPanel (страница `/reliability` + карточка Overview) + две Prometheus-серии длительностей failover/rebuild.

**Архитектура:** без новых etcd-ключей — work-ключ `/pgworker/work/<C>` расширяется аддитивными полями `last_failover`/`last_rebuild` (последний факт каждого вида, пишет `NodeSupervisor`, факты переносятся фазовыми записями); панель — чистая функция `ReliabilityCalculator` над существующим снапшотом + расширенный парсер; read-only API `GET /api/reliability`; RTO-длительности — ObservableGauge-серии тика надзора; RPO-числа в Prometheus не экспортируются (серии t14 покрывают).

**Стек:** .NET 10, C# `LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`; xUnit + FluentAssertions (AAA-комментарии в тестах); React+Mantine+TanStack Query.

**Spec:** `docs/superpowers/2026-10-07-t17-rpo-rto-dashboard/spec.md` — план аргументируется от спеки; исполнители читают обе. Канон контрактов — `arch/` (arch-first: arch-правки первыми, теми же коммитами, что и соответствующий код).

## Глобальные ограничения

- Работа ТОЛЬКО в worktree `/Users/demakaev/ZCodeProject/worktrees/feat-t17-rpo-rto-dashboard`; в feature-ветке коммитим свободно; в `main` — ничего без одобрения. Стенд для приёмки поднимается ИЗ WORKTREE (не из основного репо) — задача 11.
- `dotnet build` — 0 warnings 0 errors (`TreatWarningsAsErrors=true`); комментарии/документация — по-русски, идентификаторы — по-английски; тесты — с AAA-комментариями (`// Arrange`, `// Act`, `// Assert`).
- Механика надзора НЕ меняется (spec §2): пороги/бюджеты (`NodeDeadSec`, ускорение failover, rebuild) — бит-в-бит как сейчас; факты — наблюдатель рядом с переходами; условия существующих веток (включая условие ускорения failover) не расширять/не сужать — новые гварды (`!shard.ToRemove`) ставятся ТОЛЬКО вокруг вызовов фиксации фактов; сбой записи фактов — warning, тик не роняет.
- Никаких новых etcd-ключей, Prometheus-алертов, сервисов/портов/джоб; новых форм ввода в UI нет (грань read-only).
- Порты docker-контейнеров в тестах — только динамические; `BrokerBootSec`-таймауты интеграционных фикстур ≤ 100 с; любые ожидания агента ≤ 30 с (AGENTS.base.md §12).
- После КАЖДОЙ тестовой серии — зачистка контейнеров/сетей (AGENTS.md); следующая серия — только после финальной строки предыдущей.
- Запуск тестов — из корня worktree: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx ...`; команды длиннее 30 с — в фоне (`run_in_background: true`) с фиксацией task_id.
- Исторических атрибуций в arch/docs не оставляем: правки arch/ описывают состояние («как устроено»), без «в рамках t17»/«инцидент X».

---

### Задача 1: arch-правки — контракт work-ключа, словарь серий, контракт панели

**Files:**
- Modify: `arch/14-pgworker.md` (§3.3 таблица — строка `/pgworker/work/<C>`; §5 C — блок NodeSupervisor)
- Modify: `arch/18-metrics.md` (§2.7 — заголовок + таблица серий)
- Modify: `arch/adminpanel/02-etcd-contract.md` (§2.3.1 строка `/pgworker/work/<C>`; §3 — `WorkJournalInfo`)
- Modify: `arch/adminpanel/03-panels.md` (§1 — таблица эндпоинтов; §3 — таблица панелей UI)

**Interfaces:**
- Consumes: spec §3.1–§3.6 (семантика полей, серии, DTO, страница).
- Produces: канон контракта для всех последующих задач: имена полей `last_failover`/`last_rebuild`, cause-словарь `accelerated|elections` / `auto-dead|operator-recreate`, серии `pgworker_ha_failover_duration_seconds`/`pgworker_ha_rebuild_duration_seconds`, эндпоинт `GET /api/reliability`, страница `/reliability`.

- [ ] **Шаг 1.1: arch/14 §3.3 — расширить строку `/pgworker/work/<C>`**

  - **Вход:** файл `arch/14-pgworker.md` в актуальном состоянии ветки; spec §3.1 (семантика полей).
  - **Действие:** в JSON-схему значения ключа (таблица §3.3, строка с `/pgworker/work/<C>`, после `"unreachable"?`) добавить `"last_failover"?,"last_rebuild"?`; в конец описания ячейки (до закрывающей `|`) вставить абзац:

```
Поля HA-фактов надзора (последний факт каждого вида, без истории):
`last_failover` — недоступность лидера HA-scope, завершившаяся сменой лидера:
`{"shard":"<X>","node":"<n>","cause":"accelerated"|"elections","detected_unix":<unix>,"resolved_unix"?,"duration_sec"?}`;
`last_rebuild` — пересоздание ноды:
`{"shard":"<X>","node":"<n>","cause":"auto-dead"|"operator-recreate","detected_unix":<unix>,"resolved_unix"?,"duration_sec"?}`.
`detected_unix` — первый тик недоступности из трека `unreachable` (для
operator-recreate живой ноды — момент исполнения маркера); `resolved_unix`/
`duration_sec` отсутствуют — событие открыто (идёт сейчас; takeover продолжает
от сохранённого `detected_unix`). Длительность — от первого тика недоступности
до работоспособности (окно детекции входит — честный RTO). Новое событие того
же вида перезаписывает поле. Владелец полей — надзор (единственный писатель);
фазовые записи процессов переносят оба поля (как `unreachable`).
```

  - **Выход:** таблица §3.3 описывает расширенную схему work-ключа.
  - **Проверка:** визуальная — правка на месте, исторических атрибуций нет, перекрёстная ссылка на §5 C корректна.
  - **Связь со spec:** §3.1 (контракт etcd), §2 (arch-first).

- [ ] **Шаг 1.2: arch/14 §5 C — блок фиксации фактов надзором**

  - **Вход:** шаг 1.1 выполнен.
  - **Действие:** в §5 C после абзаца «Весь шард недоступен … (поле `unreachable`)» (перед bullet `MasterKeyReconciler`) вставить bullet:

```
- **HA-факты в work-ключе** (`last_failover`/`last_rebuild`, §3.3):
  надзор фиксирует последний факт каждого вида. Failover: недоступность
  лидера (трек `unreachable`) с применённым ускорением
  (`AccelerateDeadLeaderFailoverAsync` — docker-объект отсутствует/не
  running) → `cause=accelerated`; без применения ускорения — нет
  `SupportsRunningInspection`, живых по пробам, либо живой кандидат не
  найден (промоушен ждёт Patroni) → `cause=elections`. Закрытие — тик, где
  leader-ключ scope указывает на ДРУГУЮ ноду (`resolved_unix` = тик,
  `duration_sec` = resolved − detected); транзиентный флап (нода ожила,
  лидерство сохранила) — открытая запись удаляется без фиксации; graceful
  switchover живого лидера (soft-path recreate) — не событие. Rebuild:
  rebuild-ветка надзора → `cause=auto-dead`; исполнение маркера
  TO_RECREATE → `cause=operator-recreate`; закрытие — первый тик, где
  пересозданная нода жива пробой (переход state → RUNNING). Границы:
  усыновлённые (`object`) ноды фактов не порождают; QUARANTINED/REMOVING —
  вне проб; шард без dsn и шард с TO_REMOVE — фактов нет (самовосстановление
  и ускорение для TO_REMOVE работают как раньше — гвард только на записи
  фактов). Открытое событие живёт в ключе (переживает takeover); запись —
  финальный put тика надзора, сбой записи — warning (наблюдаемость ≠
  данные). Панель отображает факты только шардов живой декларации (ключ
  кластерный; чистки D2 достаточно).
```

  - **Выход:** §5 C описывает точки/cause-словарь/detected-resolved-семантику/гварды.
  - **Проверка:** визуальная.
  - **Связь со spec:** §3.1 (семантика), §3.2 (точки), R2/R3.

- [ ] **Шаг 1.3: arch/18 §2.7 — две RTO-серии**

  - **Вход:** шаги 1.1–1.2 выполнены.
  - **Действие:** заголовок секции (строка ~165) заменить на `### 2.7. Бэкап-домен и RTO-длительности PgWorker (серии питают тики процессов бэкапов и надзора, arch/19, arch/14 §5 C)`; в первый абзац после фразы про counters добавить `RTO-длительности пишутся тиком надзора (NodeSupervisor) из фактов work-ключа (arch/14 §3.3).`; в таблицу серий добавить две строки:

```
| `pgworker_ha_failover_duration_seconds` | gauge | cluster, shard | длительность последнего ЗАВЕРШЁННОГО failover (смена лидера после недоступности; resolved − detected, arch/14 §3.3); открытого события/факта нет — серия не эмитится |
| `pgworker_ha_rebuild_duration_seconds` | gauge | cluster, shard | длительность последнего ЗАВЕРШЁННОГО rebuild ноды; факта нет — серия не эмитится |
```

  - **Выход:** словарь §2.7 расширен (заголовок + таблица).
  - **Проверка:** визуальная; имена серий посимвольно совпадают с задачей 5.
  - **Связь со spec:** §3.3 (серии), §7.3.

- [ ] **Шаг 1.4: arch/adminpanel/02 §2.3.1 + §3**

  - **Вход:** шаг 1.1 выполнен (канон полей).
  - **Действие:** в строке `/pgworker/work/<C>` таблицы §2.3.1 (строка ~119) в JSON-схему значения добавить `"last_failover"?,"last_rebuild"?` (канон — arch/14 §3.3, ссылка уже в ячейке). Модель §3 (строка ~222) расширить:

```csharp
sealed record WorkJournalInfo(
    string Cluster, string Op, string Phase, string Instance,
    long UpdatedUnix, string? LastError,
    int? FailCount, long? FailFirstUnix, long? RetryNotBeforeUnix,
    HaSupervisionInfo? LastFailover, HaSupervisionInfo? LastRebuild);

// Последний HA-факт надзора из /pgworker/work/<C> (arch/14 §3.3; дубль
// воркерной модели — осознанный): null = факта нет/старый ключ.
sealed record HaSupervisionInfo(
    string Shard, string Node, string Cause,
    long DetectedUnix, long? ResolvedUnix, long? DurationSec);
```

  - **Выход:** контракт панели читает новые поля; модель панели синхронна.
  - **Проверка:** визуальная.
  - **Связь со spec:** §3.4 (парсер).

- [ ] **Шаг 1.5: arch/adminpanel/03 §1 + §3**

  - **Вход:** шаг 1.4 выполнен.
  - **Действие:** §1, после строки `GET /api/ha/{scope}` добавить:

```
| `GET /api/reliability` | грань «Надёжность» (read-only): per-cluster/per-shard RPO-блок (mode wal/full/off, возраст валидного полного + id, лаг/возраст WAL, сводный RPO-потенциал, порог) и RTO-блок (последний failover/rebuild с cause/duration/ongoing, длительности последнего drill/restore); всегда 200 с пустыми секциями при выключенных подсистемах, 503 — нет снапшота |
```

  §3, после строки **HA details** добавить:

```
| **Надёжность** | `/reliability` (read-only): таблица кластер×шард — RPO-колонки (возраст валидного полного с индикацией по порогу policy + id, лаг WAL сегм., возраст WAL-хвоста, «потеряем ≈ N» с индикацией источника wal/full/off; клик по id полного — переход к деталям шарда грани бэкапов) + RTO-колонки (последний failover: длительность/когда/cause/node; последний rebuild: аналогично; drill: итог+длительность; restore: итог+длительность); ongoing-значения — бейдж «идёт» с тикающей длительностью; polling — общий переключатель; форм ввода нет |
```

  Строку **Overview** дополнить после «карточка „Бэкапы“…»: `; карточка «Надёжность»: worst RPO-потенциал по Active-кластерам, число шардов mode=full («RPO держится только полным») и mode=off, длительность последнего failover установки — клиентская агрегация `GET /api/reliability``.

  - **Выход:** эндпоинт и страница в каноне панели.
  - **Проверка:** визуальная.
  - **Связь со spec:** §3.5, §3.6.

- [ ] **Шаг 1.6: коммит arch-правок**

  - **Вход:** шаги 1.1–1.5 выполнены.
  - **Действие:**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t17-rpo-rto-dashboard
git add arch/14-pgworker.md arch/18-metrics.md arch/adminpanel/02-etcd-contract.md arch/adminpanel/03-panels.md
git commit -m "arch(t17): work-ключ — HA-факты last_failover/last_rebuild, RTO-серии §2.7, панель надёжности"
```

  - **Выход:** коммит arch-правок в feature-ветке.
  - **Проверка:** `git log -1 --stat` — 4 файла, только arch/.
  - **Связь со spec:** §2 (arch-first, теми же коммитами, что и код задачи).

---

### Задача 2: WorkJournal — модель фактов, supervision-чтение/запись, carry-forward

**Files:**
- Modify: `src/Shared.Etcd/Coordination/WorkJournal.cs`
- Modify (тесты): `src/tests/Shared.Etcd.UnitTests/WorkJournalTests.cs`

**Interfaces:**
- Consumes: существующие `WorkJournal(string keyPrefix, IEtcdGateway gateway, string[] endpoints)`, `FakeCoordinationGateway` (тесты Shared.Etcd.UnitTests).
- Produces (для задач 4, 5):
  - `public sealed record HaSupervisionFact(string Shard, string Node, string Cause, long DetectedUnix, long? ResolvedUnix = null, long? DurationSec = null)` — JSON-имена `shard`/`node`/`cause`/`detected_unix`/`resolved_unix`/`duration_sec`;
  - `public sealed record HaSupervisionFacts(HaSupervisionFact? LastFailover, HaSupervisionFact? LastRebuild)`;
  - `public sealed record SupervisionState(IReadOnlyDictionary<string, long> Unreachable, HaSupervisionFact? LastFailover, HaSupervisionFact? LastRebuild)`;
  - `WorkState` + поля `LastFailover`/`LastRebuild` (`last_failover`/`last_rebuild`, null опускается);
  - `Task<Result> WriteSupervisionAsync(string cluster, string instance, IReadOnlyDictionary<string, long> unreachable, string? lastError, CancellationToken ct, HaSupervisionFacts? facts = null)`;
  - `Task<Result<SupervisionState>> ReadSupervisionStateAsync(string cluster, CancellationToken ct)`;
  - `WritePhaseAsync(..., RetrySeries? series = null, IReadOnlyDictionary<string, long>? unreachable = null, HaSupervisionFacts? facts = null)` — carry-forward фактов.

  **Решение по сигнатуре `WriteSupervisionAsync` (фиксация отступления от буквы spec §3.2 «обязательные, как unreachable»):** параметр `facts` — optional (`= null`), потому что обязательный параметр потребовал бы правок вызовов KafkaWorker/ValkeyWorker-надзоров (свои префиксы, фактов не пишут) — против spec §1.3/§3.2/R7 «KafkaWorker: общий класс, поля nullable — kfw-журналы меняются ТОЛЬКО переносом». Семантика: `facts = null` → поля опускаются (kfw/vwk-ключи фактов никогда не несут — их префиксы изолированы, стирание чужих фактов невозможно); PgWorker-надзор ВСЕГДА передаёт `facts` явно (в том числе с null-полями внутри = сброс/флап). `ReadUnreachableAsync` остаётся без изменений — вызовы KafkaWorker/ValkeyWorker/AdoptionProcess не трогаем.

- [ ] **Шаг 2.1: провальные тесты**

  - **Вход:** ветка с коммитом задачи 1; `WorkJournalTests.cs` (класс `WorkJournalTests`, helper `NewJournal(FakeCoordinationGateway)`).
  - **Действие:** добавить в конец класса секцию:

```csharp
// --- HA-факты надзора (arch/14 §3.3: last_failover/last_rebuild) ---

[Fact]
public async Task WriteSupervisionAsync_WithFacts_RoundTrip()
{
    // Arrange: закрытый failover-факт + открытый rebuild-факт
    var gateway = new FakeCoordinationGateway();
    var journal = NewJournal(gateway);
    var failover = new HaSupervisionFact("s1", "n1", "accelerated", 1000, 1075, 75);
    var rebuild = new HaSupervisionFact("s1", "n2", "auto-dead", 900);
    var facts = new HaSupervisionFacts(failover, rebuild);

    // Act: тик надзора с факторами
    var result = await journal.WriteSupervisionAsync("demo", "i1",
        new Dictionary<string, long>(), null, TestContext.Current.CancellationToken, facts);

    // Assert: JSON snake_case, null-поля опущены; чтение возвращает факты
    result.IsSuccess.Should().BeTrue();
    var raw = gateway.Store[$"{Prefix}/work/demo"];
    raw.Should().Contain("\"last_failover\":{\"shard\":\"s1\",\"node\":\"n1\",\"cause\":\"accelerated\"");
    raw.Should().Contain("\"detected_unix\":1000").And.Contain("\"resolved_unix\":1075").And.Contain("\"duration_sec\":75");
    raw.Should().Contain("\"last_rebuild\":{\"shard\":\"s1\",\"node\":\"n2\",\"cause\":\"auto-dead\",\"detected_unix\":900}");
    var state = await journal.ReadSupervisionStateAsync("demo", TestContext.Current.CancellationToken);
    state.Value!.LastFailover.Should().Be(failover);
    state.Value.LastRebuild.Should().Be(rebuild);
}

[Fact]
public async Task WriteSupervisionAsync_WithoutFacts_OmitsFields()
{
    // Arrange: запись надзора без фактов (KafkaWorker/ValkeyWorker — свой префикс,
    // фактов не пишут; поля опускаются сериализацией)
    var gateway = new FakeCoordinationGateway();
    var journal = NewJournal(gateway);

    // Act
    var result = await journal.WriteSupervisionAsync("demo", "i1",
        new Dictionary<string, long>(), null, TestContext.Current.CancellationToken);

    // Assert
    result.IsSuccess.Should().BeTrue();
    gateway.Store[$"{Prefix}/work/demo"].Should().NotContain("last_failover");
    gateway.Store[$"{Prefix}/work/demo"].Should().NotContain("last_rebuild");
}

[Fact]
public async Task ReadSupervisionStateAsync_LegacyKey_FactsNull()
{
    // Arrange: старый ключ без полей фактов
    var gateway = new FakeCoordinationGateway();
    gateway.Store[$"{Prefix}/work/old"] =
        """{"op":"supervise","phase":"supervising","instance":"i","updated_unix":1756000000,"unreachable":{"b1":100}}""";
    var journal = NewJournal(gateway);

    // Act
    var state = await journal.ReadSupervisionStateAsync("old", TestContext.Current.CancellationToken);

    // Assert: обратная совместимость — трек читается, факты null
    state.Value!.Unreachable.Should().ContainKey("b1").WhoseValue.Should().Be(100);
    state.Value.LastFailover.Should().BeNull();
    state.Value.LastRebuild.Should().BeNull();
}

[Fact]
public async Task WritePhaseAsync_WithoutFacts_PreservesExistingFacts()
{
    // Arrange: надзор записал факты
    var gateway = new FakeCoordinationGateway();
    var journal = NewJournal(gateway);
    await journal.WriteSupervisionAsync("demo", "i1", new Dictionary<string, long>(), null,
        TestContext.Current.CancellationToken,
        new HaSupervisionFacts(new HaSupervisionFact("s1", "n1", "elections", 1000, 1100, 100), null));

    // Act: фазовая запись процесса БЕЗ фактов (чтение ключа для трека — carry-forward)
    var result = await journal.WritePhaseAsync("demo", "wal-stream", "running", "i1", null,
        TestContext.Current.CancellationToken);

    // Assert: факт пережил фазовую запись (перенос, как unreachable)
    result.IsSuccess.Should().BeTrue();
    var state = await journal.ReadAsync("demo", TestContext.Current.CancellationToken);
    state.Value!.LastFailover.Should().NotBeNull();
    state.Value.LastFailover!.DurationSec.Should().Be(100);
}

[Fact]
public async Task WritePhaseAsync_WithExplicitTrackAndFacts_CarriesFactsExplicitly()
{
    // Arrange: dcs-converge-путь — явный трек + явные факты (чтения ключа нет)
    var gateway = new FakeCoordinationGateway();
    var journal = NewJournal(gateway);
    await journal.WriteSupervisionAsync("demo", "i1", new Dictionary<string, long>(), null,
        TestContext.Current.CancellationToken,
        new HaSupervisionFacts(null, new HaSupervisionFact("s1", "n2", "operator-recreate", 500)));

    // Act: фазовая запись с ЯВНЫМ треком и факторами
    var result = await journal.WritePhaseAsync("demo", "supervise", "dcs-converge", "i1", "note",
        TestContext.Current.CancellationToken,
        unreachable: new Dictionary<string, long>(),
        facts: new HaSupervisionFacts(null, new HaSupervisionFact("s1", "n2", "operator-recreate", 500)));

    // Assert: факт на месте (не стёрт фазовой записью)
    result.IsSuccess.Should().BeTrue();
    var state = await journal.ReadAsync("demo", TestContext.Current.CancellationToken);
    state.Value!.LastRebuild!.DetectedUnix.Should().Be(500);
}
```

  - **Выход:** тесты в файле; код не менялся.
  - **Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/Shared.Etcd.UnitTests -c Debug --filter "FullyQualifiedName~WorkJournalTests"` — FAIL компиляции (`HaSupervisionFact`/`ReadSupervisionStateAsync` не определены).
  - **Связь со spec:** §3.1 (схема), §3.2 (WritePhaseAsync-carry/ReadSupervisionStateAsync), §2 (обратная совместимость).

- [ ] **Шаг 2.2: реализация WorkJournal**

  - **Вход:** шаг 2.1 (тесты падают по отсутствию типов).
  - **Действие:** в `WorkJournal.cs`:

    (a) после record `RetrySeries` добавить:

```csharp
/// <summary>HA-факт надзора (arch/14 §3.3): последнее событие вида в
/// {prefix}/work/&lt;C&gt;; открытое событие — без ResolvedUnix/DurationSec.</summary>
public sealed record HaSupervisionFact(
    [property: JsonPropertyName("shard")] string Shard,
    [property: JsonPropertyName("node")] string Node,
    [property: JsonPropertyName("cause")] string Cause,
    [property: JsonPropertyName("detected_unix")] long DetectedUnix,
    [property: JsonPropertyName("resolved_unix")] long? ResolvedUnix = null,
    [property: JsonPropertyName("duration_sec")] long? DurationSec = null);

/// <summary>Пара фактов тика надзора: null внутри = факта вида нет/сброшен.</summary>
public sealed record HaSupervisionFacts(HaSupervisionFact? LastFailover, HaSupervisionFact? LastRebuild);

/// <summary>Состояние надзора из work-ключа одним чтением: трек + факты.</summary>
public sealed record SupervisionState(
    IReadOnlyDictionary<string, long> Unreachable,
    HaSupervisionFact? LastFailover,
    HaSupervisionFact? LastRebuild);
```

    (b) `WorkState` — два последних позиционных параметра:

```csharp
    [property: JsonPropertyName("retry_not_before_unix")] long? RetryNotBeforeUnix = null,
    [property: JsonPropertyName("last_failover")] HaSupervisionFact? LastFailover = null,
    [property: JsonPropertyName("last_rebuild")] HaSupervisionFact? LastRebuild = null);
```

    (c) `WritePhaseAsync` — расширить сигнатуру (`HaSupervisionFacts? facts = null`) и carry-forward тем же существующим чтением (0 дополнительных etcd-операций):

```csharp
        IReadOnlyDictionary<string, long>? track = unreachable;
        HaSupervisionFact? carryFailover = facts?.LastFailover;
        HaSupervisionFact? carryRebuild = facts?.LastRebuild;
        if (track is null)
        {
            var current = await ReadAsync(cluster, ct);
            if (!current.IsSuccess)
                return current;
            track = current.Value?.Unreachable;
            // carry-forward фактов (arch/14 §3.3): фазовая запись без явных
            // фактов сохраняет существующие из ключа (как unreachable).
            carryFailover ??= current.Value?.LastFailover;
            carryRebuild ??= current.Value?.LastRebuild;
        }

        var payload = new WorkState(op, phase, instance, DateTimeOffset.UtcNow.ToUnixTimeSeconds(), lastError,
            track, series?.FailCount, series?.FailFirstUnix, series?.RetryNotBeforeUnix,
            carryFailover, carryRebuild);
```

    Внимание: при ЯВНОМ `unreachable` и `facts = null` факты НЕ переносятся (чтения нет) — все точки явной передачи трека (задача 4: `ConvergeDcsConfigAsync`, AdoptionProcess) обязаны передавать `facts` явно.

    (d) `WriteSupervisionAsync` — расширить (optional-параметр, семантика — фиксация в Interfaces задачи):

```csharp
    public Task<Result> WriteSupervisionAsync(
        string cluster, string instance, IReadOnlyDictionary<string, long> unreachable,
        string? lastError, CancellationToken ct, HaSupervisionFacts? facts = null)
        => WithFailoverAsync(endpoint => gateway.PutAsync(
            endpoint, WorkKey(cluster),
            JsonSerializer.Serialize(new WorkState("supervise", "supervising", instance,
                DateTimeOffset.UtcNow.ToUnixTimeSeconds(), lastError, unreachable,
                LastFailover: facts?.LastFailover, LastRebuild: facts?.LastRebuild), Json),
            lease: null, ct));
```

    (e) `ReadSupervisionStateAsync` — новый метод рядом с `ReadUnreachableAsync` (который не трогаем):

```csharp
    // Состояние надзора одним чтением: трек недоступности + HA-факты
    // (takeover-продолжение открытых событий, arch/14 §5 C).
    public async Task<Result<SupervisionState>> ReadSupervisionStateAsync(string cluster, CancellationToken ct)
    {
        var state = await ReadAsync(cluster, ct);
        if (!state.IsSuccess)
            return Result<SupervisionState>.Failed(state.Error!);

        return Result<SupervisionState>.Success(new SupervisionState(
            state.Value?.Unreachable ?? (IReadOnlyDictionary<string, long>)new Dictionary<string, long>(),
            state.Value?.LastFailover, state.Value?.LastRebuild));
    }
```

  - **Выход:** модель и методы WorkJournal реализованы.
  - **Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/Shared.Etcd.UnitTests -c Debug --filter "FullyQualifiedName~WorkJournalTests"` — PASS (все, включая старые: сериализация старых ключей не сломана).
  - **Связь со spec:** §3.2 (WorkJournal), §2 (обратная совместимость, R1).

- [ ] **Шаг 2.3: сборка решения и коммит**

  - **Вход:** шаг 2.2 зелёный.
  - **Действие:**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug
git add src/Shared.Etcd/Coordination/WorkJournal.cs src/tests/Shared.Etcd.UnitTests/WorkJournalTests.cs
git commit -m "feat(t17): WorkJournal — HA-факты last_failover/last_rebuild (supervision-запись/чтение, carry-forward фаз)"
```

  - **Выход:** коммит; решение собирается.
  - **Проверка:** сборка 0 warnings 0 errors; `git log -1 --stat` — 2 файла.
  - **Связь со spec:** §4 фаза 2 (воркер: Shared.Etcd).

---

### Задача 3: HaFactState — чистый трансформер состояний фактов

**Files:**
- Create: `src/PgWorker.Provisioning/Processes/HaFactState.cs`
- Create: `src/tests/PgWorker.UnitTests/Provisioning/HaFactStateTests.cs`

**Interfaces:**
- Consumes: `HaSupervisionFact`/`HaSupervisionFacts` (задача 2).
- Produces (для задачи 4): `HaFactState` — `static HaFactState FromStored(HaSupervisionFact? failover, HaSupervisionFact? rebuild)`; `void FailoverDetected(string shard, string node, string cause, long detectedUnix)`; `void RebuildDetected(string shard, string node, string cause, long detectedUnix)`; `void LeaderChanged(string shard, string? leader, long nowUnix)`; `void LeaderRecovered(string shard, string node, string? leader)`; `void NodeAlive(string shard, string node, long nowUnix)`; `HaSupervisionFacts ToRecord()`.

Семантика (детерминированная, без etcd/часов — все времена приходят параметрами):
- `FailoverDetected`/`RebuildDetected`: уже открытое событие ТОГО ЖЕ (shard, node) — игнор (detected первого тика, cause первоисточника); событие другого шарда/ноды — перезапись (последний факт побеждает, без истории).
- `LeaderChanged(shard, leader, now)`: открытый failover этого шарда и `leader != null && leader != fact.Node` → закрыть (`ResolvedUnix=now`, `DurationSec=now−DetectedUnix`); иначе ничего.
- `LeaderRecovered(shard, node, leader)`: открытый failover `(shard, node)` и `leader == node` → сброс в null без фиксации (флап).
- `NodeAlive(shard, node, now)`: открытый rebuild `(shard, node)` → закрыть (`ResolvedUnix=now`, `DurationSec=now−DetectedUnix`); идемпотентно (закрытый не трогаем).

- [ ] **Шаг 3.1: провальные тесты**

  - **Вход:** коммит задачи 2.
  - **Действие:** создать `src/tests/PgWorker.UnitTests/Provisioning/HaFactStateTests.cs`:

```csharp
using PgWorker.Provisioning.Processes;
using Shared.Etcd.Coordination;

namespace PgWorker.UnitTests.Provisioning;

// HaFactState (arch/14 §5 C): чистые переходы HA-фактов — открытие/закрытие/
// флап-сброс/перезапись; без etcd и часов (времена приходят параметрами).
public class HaFactStateTests
{
    [Fact]
    public void FailoverDetected_ThenLeaderChanged_ClosesWithDuration()
    {
        // Arrange: открытие accelerated-факта
        var state = HaFactState.FromStored(null, null);

        // Act: открытие в тике детекции; лидер сменился на 75-й секунде
        state.FailoverDetected("s1", "n1", "accelerated", detectedUnix: 1000);
        state.LeaderChanged("s1", "n2", nowUnix: 1075);

        // Assert: закрытый факт с длительностью (окно детекции входит)
        var failover = state.ToRecord().LastFailover!;
        failover.Cause.Should().Be("accelerated");
        failover.DetectedUnix.Should().Be(1000);
        failover.ResolvedUnix.Should().Be(1075);
        failover.DurationSec.Should().Be(75);
    }

    [Fact]
    public void FailoverDetected_ElectionsCause_StaysOpen()
    {
        // Arrange/Act: ускорение не применено — промоушен ждёт Patroni
        var state = HaFactState.FromStored(null, null);
        state.FailoverDetected("s1", "n1", "elections", 1000);

        // Assert: открытое событие — без resolved/duration
        var failover = state.ToRecord().LastFailover!;
        failover.ResolvedUnix.Should().BeNull();
        failover.DurationSec.Should().BeNull();
    }

    [Fact]
    public void LeaderRecovered_SameLeader_DropsWithoutFixation()
    {
        // Arrange: открытый failover (транзиентный флап лидера)
        var state = HaFactState.FromStored(
            new HaSupervisionFact("s1", "n1", "accelerated", 1000), null);

        // Act: нода ожила, лидерство сохранила
        state.LeaderRecovered("s1", "n1", leader: "n1");

        // Assert: факта failover НЕ БЫЛО — запись удалена без фиксации
        state.ToRecord().LastFailover.Should().BeNull();
    }

    [Fact]
    public void LeaderChanged_SameLeader_DoesNotClose()
    {
        // Arrange: открытый failover, лидер всё ещё она (выборы идут)
        var state = HaFactState.FromStored(
            new HaSupervisionFact("s1", "n1", "elections", 1000), null);

        // Act: leader-ключ указывает на ту же ноду
        state.LeaderChanged("s1", "n1", nowUnix: 1100);

        // Assert: событие остаётся открытым
        state.ToRecord().LastFailover!.ResolvedUnix.Should().BeNull();
    }

    [Fact]
    public void FromStored_TakeoverContinues_FromDetected()
    {
        // Arrange: воркер упал в окне события — открытый факт в ключе
        var state = HaFactState.FromStored(
            new HaSupervisionFact("s2", "n9", "elections", 1000), null);

        // Act: takeover-инстанс видит смену лидера
        state.LeaderChanged("s2", "n3", nowUnix: 1300);

        // Assert: длительность от СОХРАНЁННОГО detected (не от рестарта)
        var failover = state.ToRecord().LastFailover!;
        failover.DurationSec.Should().Be(300);
    }

    [Fact]
    public void FailoverDetected_OtherShard_OverwritesLastFact()
    {
        // Arrange: закрытый факт шарда s1
        var state = HaFactState.FromStored(
            new HaSupervisionFact("s1", "n1", "elections", 1000, 1100, 100), null);

        // Act: новое событие на другом шарде (последний факт побеждает)
        state.FailoverDetected("s2", "n2", "accelerated", 2000);

        // Assert
        var failover = state.ToRecord().LastFailover!;
        failover.Shard.Should().Be("s2");
        failover.DetectedUnix.Should().Be(2000);
        failover.ResolvedUnix.Should().BeNull();
    }

    [Fact]
    public void FailoverDetected_SameShardNode_Ignored()
    {
        // Arrange/Act: повторная детекция того же события в следующем тике
        var state = HaFactState.FromStored(null, null);
        state.FailoverDetected("s1", "n1", "accelerated", 1000);
        state.FailoverDetected("s1", "n1", "elections", 1050);

        // Assert: detected первого тика, cause первоисточника
        var failover = state.ToRecord().LastFailover!;
        failover.DetectedUnix.Should().Be(1000);
        failover.Cause.Should().Be("accelerated");
    }

    [Fact]
    public void RebuildDetected_ThenNodeAlive_Closes()
    {
        // Arrange/Act: rebuild auto-dead открыт, нода поднялась
        var state = HaFactState.FromStored(null, null);
        state.RebuildDetected("s1", "n2", "auto-dead", 1000);
        state.NodeAlive("s1", "n2", nowUnix: 1150);

        // Assert
        var rebuild = state.ToRecord().LastRebuild!;
        rebuild.ResolvedUnix.Should().Be(1150);
        rebuild.DurationSec.Should().Be(150);
    }

    [Fact]
    public void RebuildDetected_OperatorRecreate_DetectedFromMarkerTime()
    {
        // Arrange/Act: маркер TO_RECREATE живой ноды — трека нет, detected = момент исполнения
        var state = HaFactState.FromStored(null, null);
        state.RebuildDetected("s1", "n3", "operator-recreate", 5000);

        // Assert
        state.ToRecord().LastRebuild!.DetectedUnix.Should().Be(5000);
    }

    [Fact]
    public void NodeAlive_OtherShardOrClosed_DoesNothing()
    {
        // Arrange: открытый rebuild s1/n2; закрытый s3/n3
        var state = HaFactState.FromStored(null, null);
        state.RebuildDetected("s1", "n2", "auto-dead", 1000);
        state.RebuildDetected("s3", "n3", "auto-dead", 2000);
        state.NodeAlive("s3", "n3", 2100);

        // Act: живость чужой ноды и повторная живость закрытой
        state.NodeAlive("s2", "nX", 2200);
        state.NodeAlive("s3", "n3", 2300);

        // Assert: s1/n2 открыт, s3/n3 не тронут (first-resolved)
        var r = state.ToRecord();
        r.LastRebuild!.Shard.Should().Be("s1");
        r.LastRebuild.ResolvedUnix.Should().BeNull();
    }

    [Fact]
    public void Kinds_Independent()
    {
        // Arrange: умерший лидер — возможны ОБА факта на одном шарде
        var state = HaFactState.FromStored(null, null);

        // Act: failover (смена лидера) + rebuild (пересоздание ноды)
        state.FailoverDetected("s1", "n1", "accelerated", 1000);
        state.RebuildDetected("s1", "n1", "auto-dead", 1000);

        // Assert: поля независимы
        var r = state.ToRecord();
        r.LastFailover.Should().NotBeNull();
        r.LastRebuild.Should().NotBeNull();
    }
}
```

  - **Выход:** файл тестов создан.
  - **Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~HaFactStateTests"` — FAIL компиляции (класс `HaFactState` не существует).
  - **Связь со spec:** §3.2 (HaFactState), AC2.

- [ ] **Шаг 3.2: реализация HaFactState**

  - **Вход:** шаг 3.1 (падение по отсутствию класса).
  - **Действие:** создать `src/PgWorker.Provisioning/Processes/HaFactState.cs`:

```csharp
using Shared.Etcd.Coordination;

namespace PgWorker.Provisioning.Processes;

/// <summary>
/// Чистые переходы HA-фактов надзора (arch/14 §5 C): решения «открыть/закрыть/
/// сбросить» без etcd и часов (времена приходят параметрами); NodeSupervisor
/// делегирует. Владеет последним фактом каждого вида (без истории):
/// новое событие перезаписывает, повторная детекция того же события — игнор
/// (detected первого тика).
/// </summary>
public sealed class HaFactState
{
    public HaSupervisionFact? Failover { get; private set; }
    public HaSupervisionFact? Rebuild { get; private set; }

    private HaFactState(HaSupervisionFact? failover, HaSupervisionFact? rebuild)
        => (Failover, Rebuild) = (failover, rebuild);

    // takeover-продолжение: открытые события из work-ключа
    public static HaFactState FromStored(HaSupervisionFact? failover, HaSupervisionFact? rebuild)
        => new(failover, rebuild);

    // Открытие failover (dead-ветка надзора/применённое ускорение):
    // уже открытое событие того же (shard, node) не перезаписываем.
    public void FailoverDetected(string shard, string node, string cause, long detectedUnix)
    {
        if (Failover is { } open && open.Shard == shard && open.Node == node)
            return;
        Failover = new HaSupervisionFact(shard, node, cause, detectedUnix);
    }

    // Открытие rebuild (rebuild-ветка/маркер TO_RECREATE).
    public void RebuildDetected(string shard, string node, string cause, long detectedUnix)
    {
        if (Rebuild is { } open && open.Shard == shard && open.Node == node)
            return;
        Rebuild = new HaSupervisionFact(shard, node, cause, detectedUnix);
    }

    // Тик с другим лидером scope: закрытие открытого failover этого шарда.
    public void LeaderChanged(string shard, string? leader, long nowUnix)
    {
        if (Failover is not { } open || open.Shard != shard || open.ResolvedUnix is not null)
            return;
        if (leader is null || leader == open.Node)
            return; // лидер тот же/неизвестен — событие продолжается
        Failover = open with { ResolvedUnix = nowUnix, DurationSec = nowUnix - open.DetectedUnix };
    }

    // Флап-оживание: нода жива и лидерство сохранила — факта НЕ БЫЛО.
    public void LeaderRecovered(string shard, string node, string? leader)
    {
        if (Failover is { } open && open.Shard == shard && open.Node == node && leader == node)
            Failover = null;
    }

    // Живость ноды пробой: закрытие открытого rebuild (первый живой тик).
    public void NodeAlive(string shard, string node, long nowUnix)
    {
        if (Rebuild is not { } open || open.Shard != shard || open.Node != node || open.ResolvedUnix is not null)
            return;
        Rebuild = open with { ResolvedUnix = nowUnix, DurationSec = nowUnix - open.DetectedUnix };
    }

    public HaSupervisionFacts ToRecord() => new(Failover, Rebuild);
}
```

  - **Выход:** класс реализован.
  - **Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~HaFactStateTests"` — PASS.
  - **Связь со spec:** §3.2, AC2 (юниты без фикстур).

- [ ] **Шаг 3.3: коммит**

  - **Вход:** шаг 3.2 зелёный.
  - **Действие:**

```bash
git add src/PgWorker.Provisioning/Processes/HaFactState.cs src/tests/PgWorker.UnitTests/Provisioning/HaFactStateTests.cs
git commit -m "feat(t17): HaFactState — чистые переходы HA-фактов надзора"
```

  - **Выход:** коммит.
  - **Проверка:** `git log -1 --stat` — 2 новых файла.
  - **Связь со spec:** §4 фаза 2.

---

### Задача 4: NodeSupervisor — встройка фиксации фактов

**Files:**
- Modify: `src/PgWorker.Provisioning/Processes/NodeSupervisor.cs`
- Modify: `src/PgWorker.Provisioning/Processes/AdoptionProcess.cs` (перенос фактов при явной передаче трека)
- Modify (тесты): `src/tests/PgWorker.UnitTests/Provisioning/NodeSupervisorTests.cs`

**Interfaces:**
- Consumes: `HaFactState` (задача 3); `ReadSupervisionStateAsync`/`WriteSupervisionAsync(..., facts)`/`WritePhaseAsync(..., facts)` (задача 2); фикстуры `Fakes.FakeEtcd`/`Fakes.FakeDriver` (`SupportsRunningInspection=true`, `InspectResult` — мутабельная карта, ПО УМОЛЧАНИЮ ПУСТАЯ → `InspectNodesAsync` не находит ноду → «не в running»), `Fakes.FakeSql` (`ScalarResultByDsn` — инжекция исхода SQL-проб усыновлённых), helper `NewRig(..., sql: ...)`; существующие кейсы `Tick_DeletedLeaderContainer_FailoverAccelerated_NodeRecreated` (EnsureDeclared-путь ускорения) и `Tick_NoRunningInspection_DeadLeaderProbe_NoAcceleration` (нет инспекции).
- Produces: `NodeSupervisor` с optional-параметром ctor `Action<string, IReadOnlyDictionary<string, (long? FailoverSec, long? RebuildSec)>>? haDurations = null` (для задачи 5); enum + поведение work-ключа — открытые/закрытые факты в финальном supervision-put:

```csharp
// Исход попытки ускорения failover (arch/14 §5 C): Applied — failover-маркер
// поставлен и leader-ключ снят; NotLeader — нода не лидер scope (ускорение
// не её, обычный путь); NoCandidate — лидер мёртв, живого кандидата нет
// (промоушен ждёт Patroni) — elections-факт по spec §3.1.
public enum FailoverAcceleration { Applied, NotLeader, NoCandidate }
```

Точки встройки (карта — всё уже существует в коде, добавляется только наблюдение; механика бит-в-бит):
1. Чтение в начале `TickAsync` (после клэйма, до `ReadPortAllocAsync`): `ReadSupervisionStateAsync` → `track` + `HaFactState.FromStored(...)`.
2. `EnsureDeclaredNodesAsync` — после попытки ускорения: `Applied` → `FailoverDetected("accelerated")`; `NoCandidate` → `FailoverDetected("elections")` (spec §3.1: мёртвый лидер без живого кандидата — elections); `NotLeader` → факта нет (нода не лидер — события failover нет). Под гвардом `!shard.ToRemove` (гвард ТОЛЬКО на факте; сам вызов ускорения и его условия не трогаем).
3. `SuperviseShardAsync` dead-ветка `isLeader`: условие ускорения — БИТ-В-БИТ исходное (`isLeader && !adopted && node.State != NodeState.ToRecreate && alive.Count > 0 && driver.SupportsRunningInspection`); внутри после попытки ускорения: `Applied` → `"accelerated"`, `NoCandidate` → `"elections"` (в этой ветке `NotLeader` невозможен — isLeader по свежему leader-ключу тика); под гвардом `!shard.ToRemove`. Else-ветка (ускорение НЕ применяется: нет инспекции/живых по пробам) — `FailoverDetected("elections")` под теми же гвардами наблюдения.
4. `SuperviseShardAsync` — после парсинга leader: `LeaderChanged(shard, leader, now)` (только закрытие существующего факта — R4); alive-ветка: `LeaderRecovered(...)` (флап — R3) и `NodeAlive(...)` (закрытие rebuild при живости: переход → RUNNING или уже RUNNING при takeover).
5. rebuild-ветка `SuperviseShardAsync` (после REBUILDING-put, до `track.Remove`): `RebuildDetected(shard, node, "auto-dead", track[trackKey])` (detected читается ДО Remove) под гвардом `!shard.ToRemove`.
6. `RecreateMarkedNodesAsync` (после REBUILDING-put): `RebuildDetected(shard, node, "operator-recreate", detected)` под гвардом `!shard.ToRemove`.
7. Финальный `WriteSupervisionAsync(..., facts: haFacts.ToRecord())`.
8. `ConvergeDcsConfigAsync` — `WritePhaseAsync(..., unreachable: track, facts: haFacts.ToRecord())`.
9. Гварды наблюдения (не механики): факты не пишутся для шарда `ToRemove`, усыновлённой ноды (`addr.Object is not null`), шарда без dsn (уже skip), `restoring`-шарда (уже skip), QUARANTINED/REMOVING (уже skip в пробах — гвард один: `node.State is NodeState.Quarantined or NodeState.Removing`).

`AccelerateDeadLeaderFailoverAsync` меняет возврат `Result` → `Result<FailoverAcceleration>`: `leader != missing` → `NotLeader`; `candidate is null` → `NoCandidate`; после успешного `DeleteAsync` → `Applied`; ошибки — Failed как раньше. Условия вызова метода из обеих точек НЕ меняются.

- [ ] **Шаг 4.1: провальные тесты — факты в work-ключе**

  - **Вход:** коммиты задач 2–3; `NodeSupervisorTests.cs` (helpers `NewRig`/`Ok()`/`Down()`/`Snapshot`; параметр `NewRig(..., sql:)` для инжекции SQL-исходов); сид ToRemove-шарда — ключ `/clusters/shop/shards/shard1/state` = `"TO_REMOVE"` (парсер: `shards/<X>/state` = TO_REMOVE → `ShardSpec.ToRemove`); фейк-инспект: `InspectResult` по умолчанию пуст → «ноды нет в running».
  - **Действие:** добавить в `NodeSupervisorTests.cs` (после существующих кейсов):

```csharp
// --- HA-факты (arch/14 §3.3/§5 C): last_failover/last_rebuild в work-ключе ---

private static async Task<WorkState?> WorkStateOf(Rig rig)
    => (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value;

[Fact]
public async Task Tick_DeadLeaderContainer_AccelerationApplied_OpensAcceleratedFailover()
{
    // Arrange: контейнер ЛИДЕРА shard1b снесён (docker-объекта нет — EnsureDeclared
    // ускоряет), реплики живы, SupportsRunningInspection=true
    var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
    var rig = await NewRig(_ => Ok(), nodeObjects:
    [
        "pgw-shop-shard1-shard1a", "pgw-shop-shard1-shard1c",
    ]);
    rig.Etcd.Seed("/service/shop-shard1/leader", """{"name":"shard1b"}""");
    rig.Etcd.Seed("/clusters/shop/shards/shard1/master", "h1:16500");

    // Act
    var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), null, CancellationToken.None);

    // Assert: открытый факт failover cause=accelerated; detected — первый тик
    // недоступности (трека не было — момент детекции этого тика)
    outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
    var state = await WorkStateOf(rig);
    state!.LastFailover.Should().NotBeNull();
    state.LastFailover!.Shard.Should().Be("shard1");
    state.LastFailover.Node.Should().Be("shard1b");
    state.LastFailover.Cause.Should().Be("accelerated");
    state.LastFailover.ResolvedUnix.Should().BeNull();
    state.LastFailover.DetectedUnix.Should().BeGreaterThan(now - 5);
}

[Fact]
public async Task Tick_DeadLeader_NoRunningInspection_OpensElectionsFailover()
{
    // Arrange: лидер shard1b мертва по пробе, все docker-объекты на месте
    // (EnsureDeclared не ускоряет), Swarm-подобный драйвер — ускорение НЕ применяется
    var rig = await NewRig(port => port == 18001 ? Down() : Ok());
    rig.Driver.SupportsRunningInspection = false;
    rig.Etcd.Seed("/service/shop-shard1/leader", """{"name":"shard1b"}""");

    // Act
    await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), null, CancellationToken.None);

    // Assert: открытый факт cause=elections (промоушен ждёт Patroni)
    var state = await WorkStateOf(rig);
    state!.LastFailover!.Cause.Should().Be("elections");
    state.LastFailover.Node.Should().Be("shard1b");
}

[Fact]
public async Task Tick_DeadLeaderContainer_NoLiveCandidate_OpensElectionsFailover()
{
    // Arrange: контейнер ЛИДЕРА shard1a снесён (EnsureDeclared пытается ускорить),
    // ВСЕ ноды шарда мертвы по Patroni-пробам (18000/18001/18002 Down) —
    // живого кандидата нет (NoCandidate), лидер мёртв → alive-ветка его не видит.
    // ВАЖНО: лидер обязан быть мёртвым И по пробе — иначе alive-ветка того же
    // тика сделает флап-сброс факта (LeaderRecovered, R3) и факт исчезнет.
    var rig = await NewRig(port => port == 18000 || port == 18001 || port == 18002 ? Down() : Ok(),
        nodeObjects:
        [
            "pgw-shop-shard1-shard1b", "pgw-shop-shard1-shard1c",
        ]);
    rig.Etcd.Seed("/service/shop-shard1/leader", """{"name":"shard1a"}""");

    // Act
    await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), null, CancellationToken.None);

    // Assert: ускорение НЕ применено (маркера нет), факт ОТКРЫТ cause=elections
    // и пережил тик (флап-сброс не сработал — нода мертва)
    rig.Etcd.Store.Should().NotContainKey("/service/shop-shard1/failover");
    var state = await WorkStateOf(rig);
    state!.LastFailover!.Cause.Should().Be("elections");
    state.LastFailover.Node.Should().Be("shard1a");
    state.LastFailover.ResolvedUnix.Should().BeNull();
}

// ПРЕДУПРЕЖДЕНИЕ (фиксация решения): если этот тест падает из-за того, что
// факт исчез (LastFailover == null) — это НЕ повод ослаблять флап-сброс
// (LeaderRecovered): сброс — канон spec §3.1/R3 («транзиентный флап — запись
// удаляется без фиксации»). Чинить СИД (лидер должен быть мёртв по пробе),
// не семантику флапа.

[Fact]
public async Task Tick_LeaderChanged_ClosesFailoverWithDuration()
{
    // Arrange: открытый факт от прошлого тика (detected=1000), лидер уже ДРУГАЯ нода
    var rig = await NewRig(_ => Ok());
    await rig.Journal.WriteSupervisionAsync("shop", "seed",
        new Dictionary<string, long>(), null, CancellationToken.None,
        new HaSupervisionFacts(
            new HaSupervisionFact("shard1", "shard1b", "accelerated", 1000), null));
    rig.Etcd.Seed("/service/shop-shard1/leader", """{"name":"shard1c"}""");

    // Act
    await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), null, CancellationToken.None);

    // Assert: закрыт — resolved/duration зафиксированы
    var state = await WorkStateOf(rig);
    state!.LastFailover!.ResolvedUnix.Should().NotBeNull();
    state.LastFailover.DurationSec.Should().Be(state.LastFailover.ResolvedUnix!.Value - 1000);
}

[Fact]
public async Task Tick_LeaderFlapRecovery_DropsOpenFailover()
{
    // Arrange: открытый факт, нода ожила и лидерство СОХРАНИЛА
    var rig = await NewRig(_ => Ok());
    await rig.Journal.WriteSupervisionAsync("shop", "seed",
        new Dictionary<string, long>(), null, CancellationToken.None,
        new HaSupervisionFacts(
            new HaSupervisionFact("shard1", "shard1b", "elections", 1000), null));
    rig.Etcd.Seed("/service/shop-shard1/leader", """{"name":"shard1b"}""");

    // Act
    await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), null, CancellationToken.None);

    // Assert: транзиентный флап — факта НЕ БЫЛО, запись удалена без фиксации
    var state = await WorkStateOf(rig);
    state!.LastFailover.Should().BeNull();
}

[Fact]
public async Task Tick_DeadNonLeaderRebuild_OpensAutoDeadFact()
{
    // Arrange: не-лидер shard1a мертва дольше NodeDeadSec, кворум жив
    var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
    var rig = await NewRig(
        port => port == 18000 ? Down() : Ok(),
        staleUnreachableForShard1A: now - 200);
    rig.Etcd.Seed("/service/shop-shard1/leader", """{"name":"shard1b"}""");

    // Act
    await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), null, CancellationToken.None);

    // Assert: открытый rebuild cause=auto-dead, detected из трека
    var state = await WorkStateOf(rig);
    state!.LastRebuild!.Shard.Should().Be("shard1");
    state.LastRebuild.Node.Should().Be("shard1a");
    state.LastRebuild.Cause.Should().Be("auto-dead");
    state.LastRebuild.DetectedUnix.Should().Be(now - 200);
    state.LastRebuild.ResolvedUnix.Should().BeNull();
}

[Fact]
public async Task Tick_RebuiltNodeAlive_ClosesRebuild()
{
    // Arrange: открытый rebuild, нода пересоздана и жива (state=REBUILDING → RUNNING)
    var rig = await NewRig(_ => Ok());
    rig.Etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1a/state", "REBUILDING");
    await rig.Journal.WriteSupervisionAsync("shop", "seed",
        new Dictionary<string, long>(), null, CancellationToken.None,
        new HaSupervisionFacts(null,
            new HaSupervisionFact("shard1", "shard1a", "auto-dead", 1000)));
    rig.Etcd.Seed("/service/shop-shard1/leader", """{"name":"shard1b"}""");

    // Act
    await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), null, CancellationToken.None);

    // Assert: закрытие — первый тик живости (переход → RUNNING)
    var state = await WorkStateOf(rig);
    state!.LastRebuild!.ResolvedUnix.Should().NotBeNull();
    state.LastRebuild.DurationSec.Should().Be(state.LastRebuild.ResolvedUnix!.Value - 1000);
    rig.Etcd.Store["/clusters/shop/shards/shard1/nodes/shard1a/state"].Value
        .Should().Be("RUNNING");
}

[Fact]
public async Task Tick_RecreateMarker_OpensOperatorRebuildFact()
{
    // Arrange: нода помечена TO_RECREATE (заявка оператора)
    var rig = await NewRig(_ => Ok());
    rig.Etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1a/state", "TO_RECREATE");
    rig.Etcd.Seed("/service/shop-shard1/leader", """{"name":"shard1b"}""");

    // Act
    await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), null, CancellationToken.None);

    // Assert: rebuild cause=operator-recreate, detected — момент исполнения
    // (живая нода — трека недоступности нет)
    var state = await WorkStateOf(rig);
    state!.LastRebuild!.Cause.Should().Be("operator-recreate");
    state.LastRebuild.Node.Should().Be("shard1a");
}

[Fact]
public async Task Tick_PhaseWrites_CarryFactsForward()
{
    // Arrange: факты записаны надзором; фазовая запись dcs-converge (патч DCS-конфига)
    // с явным unreachable перенесёт их (put всего ключа не стирает)
    var rig = await NewRig(_ => Ok(), respondRaw: r =>
    {
        if (r.Method.Method == "GET" && r.RequestUri!.AbsolutePath == "/config")
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(
                    """{"ttl":20,"loop_wait":1,"retry_timeout":3}""",
                    Encoding.UTF8, "application/json"),
            };
        return Ok();
    }, configOverride: """{"buckets":2,"dbname":"shop","created_unix":1755900000,"synchronous_mode_strict":true,"postgresql":{"parameters":{"shared_buffers":"1GB"}}}""");
    await rig.Journal.WriteSupervisionAsync("shop", "seed",
        new Dictionary<string, long>(), null, CancellationToken.None,
        new HaSupervisionFacts(new HaSupervisionFact("shard1", "shard1b", "elections", 1000, 1100, 100), null));

    // Act: тик надзора с патчем DCS-конфига (фазовая запись dcs-converge)
    await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), null, CancellationToken.None);

    // Assert: факт пережил фазовую запись (carry-forward)
    var state = await WorkStateOf(rig);
    state!.LastFailover!.DurationSec.Should().Be(100);
}

[Fact]
public async Task Tick_AdoptedNode_DeadBySqlProbe_NoRebuildFact()
{
    // Arrange: усыновлённая нода shard1a (object в portalloc, patroni=0) МЕРТВА
    // ПО SQL-ПРОБЕ (ScalarResultByDsn падает — без этого FakeSql молчит Success,
    // нода «жива», dead/rebuild-путь не выполняется и тест проходит вхолостую);
    // трек недоступности свежий; реплики живы по Patroni
    var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
    var adopted = new Dictionary<string, NodeAddress>
    {
        ["shard1/shard1a"] = new("ext-h", new NodePorts(15000, 0, 16500), Object: "external"),
        ["shard1/shard1b"] = new("h1", new NodePorts(15001, 18001, 16501)),
        ["shard1/shard1c"] = new("h2", new NodePorts(15002, 18002, 16502)),
    };
    var rig = await NewRig(
        _ => Ok(), // Patroni-пробы реплик живы; лидер — shard1b (не усыновлённая)
        addresses: adopted,
        staleUnreachableForShard1A: now - 200,
        sql: new Fakes.FakeSql
        {
            ScalarResultByDsn = _ => Result<object?>.Failed(new ApplicationException("down")),
        });
    rig.Etcd.Seed("/service/shop-shard1/leader", """{"name":"shard1b"}""");

    // Act
    await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), null, CancellationToken.None);

    // Assert: нода мертва по пробе, но rebuild усыновлённых не выполняется
    // (гвард adopted) — rebuild-факта нет; failover-факта тоже нет (не лидер)
    var state = await WorkStateOf(rig);
    state!.LastRebuild.Should().BeNull("усып. нода мертва по SQL, но rebuild-гвард её исключает");
    state.LastFailover.Should().BeNull();
}

[Fact]
public async Task Tick_ToRemoveShard_DeadLeader_AcceleratedButNoFacts()
{
    // Arrange: шард помечен TO_REMOVE (демонтаж); контейнеры всех нод на месте
    // (EnsureDeclared TO_REMOVE-шард скипает), лидер shard1a мёртв ПО ПРОБЕ
    // (18000 Down), реплики живы; фейк-инспект по умолчанию пуст → «нода не в
    // running» → dead-ветвь ускоряет КАК РАНЬШЕ
    var rig = await NewRig(port => port == 18000 ? Down() : Ok());
    rig.Etcd.Seed("/clusters/shop/shards/shard1/state", "TO_REMOVE");
    rig.Etcd.Seed("/service/shop-shard1/leader", """{"name":"shard1a"}""");

    // Act
    var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), null, CancellationToken.None);

    // Assert: механика не изменилась — failover-маркер поставлен; фактов нет
    outcome.Value.Outcome.Should().Be(ProcessOutcome.Done);
    rig.Etcd.Store.Should().ContainKey("/service/shop-shard1/failover",
        "TO_REMOVE не отключает ускорение — гвард только на записи фактов");
    var state = await WorkStateOf(rig);
    state!.LastFailover.Should().BeNull("TO_REMOVE — демонтируемое, фактов нет");
    state.LastRebuild.Should().BeNull();
}

[Fact]
public async Task Tick_QuarantinedAndRemovingDeadNodes_NoFacts()
{
    // Arrange: нода shard1a в QUARANTINED (домен эвакуатора), нода shard1c в
    // REMOVING (домен демонтажа) — обе мертвы по пробам; гвард проб один:
    // node.State is Quarantined or Removing; лидер shard1b жив
    var rig = await NewRig(port => port == 18000 || port == 18002 ? Down() : Ok());
    rig.Etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1a/state", "QUARANTINED");
    rig.Etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1c/state", "REMOVING");
    rig.Etcd.Seed("/service/shop-shard1/leader", """{"name":"shard1b"}""");

    // Act
    await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), null, CancellationToken.None);

    // Assert: обе ноды вне проб надзора; фактов нет, state обеих не перезаписан
    var state = await WorkStateOf(rig);
    state!.LastFailover.Should().BeNull();
    state.LastRebuild.Should().BeNull();
    rig.Etcd.Store["/clusters/shop/shards/shard1/nodes/shard1a/state"].Value
        .Should().Be("QUARANTINED");
    rig.Etcd.Store["/clusters/shop/shards/shard1/nodes/shard1c/state"].Value
        .Should().Be("REMOVING");
}
```

  ПРИМЕЧАНИЕ: конструктор `NodeAddress` (параметр `Object`), `Fakes.FakeSql.ScalarResultByDsn` и комбинации `NewRig` сверить с фактическим кодом (`Fakes.cs`, `Portalloc`); пути HTTP-проб `ShardProbe` (Patroni по портам 1800x) — с соседними кейсами; при несовпадении поправить сид минимально, смысл ассертов каноничен.
  - **Выход:** тесты в файле; реализация не менялась.
  - **Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~NodeSupervisorTests"` — новые кейсы FAIL (факты не пишутся), старые — PASS.
  - **Связь со spec:** §3.1 (семантика/гварды; NoCandidate → elections), §3.2 (точки), AC2 (включая TO_REMOVE/QUARANTINED/REMOVING; R3-флап).

- [ ] **Шаг 4.2: реализация встройки**

  - **Вход:** шаг 4.1 (новые кейсы падают).
  - **Действие:** в `NodeSupervisor.cs`:

    (a) ctor — optional-параметр последним:

```csharp
    MasterKeyReconciler? masterKeys = null,
    EtcdEndpoints? etcdForNodes = null,
    Action<string, IReadOnlyDictionary<string, (long? FailoverSec, long? RebuildSec)>>? haDurations = null)
```

    (b) enum `FailoverAcceleration` (в файле NodeSupervisor.cs, над классом):

```csharp
/// <summary>Исход попытки ускорения failover (arch/14 §5 C): Applied — маркер
/// поставлен и leader-ключ снят; NotLeader — нода не лидер scope; NoCandidate —
/// лидер мёртв, живого кандидата нет (промоушен ждёт Patroni — elections).</summary>
public enum FailoverAcceleration { Applied, NotLeader, NoCandidate }
```

    (c) `TickAsync` — существующее чтение трека (`ReadUnreachableAsync` + `track`, строки ~98-101) ПЕРЕНЕСТИ выше (сразу после проверки клэйма, до `ReadPortAllocAsync`) и заменить:

```csharp
        // Состояние надзора одним чтением: трек недоступности + HA-факты
        // (takeover продолжает открытые события от сохранённого detected).
        var supervision = await journal.ReadSupervisionStateAsync(cluster, ct);
        if (!supervision.IsSuccess)
            return Fail(supervision.Error!);
        var track = new Dictionary<string, long>(supervision.Value.Unreachable);
        var haFacts = HaFactState.FromStored(supervision.Value.LastFailover, supervision.Value.LastRebuild);
```

    Старые строки чтения на прежнем месте (шаг 2 тика) удалить.

    (d) `AccelerateDeadLeaderFailoverAsync` — возврат `Result` → `Result<FailoverAcceleration>`: точка выхода `leader != missing` → `Result<FailoverAcceleration>.Success(FailoverAcceleration.NotLeader)`; `candidate is null` → `Success(FailoverAcceleration.NoCandidate)`; после успешного `DeleteAsync` → `Success(FailoverAcceleration.Applied)`; фейлы `marked`/`DeleteAsync` — Failed как сейчас. Условия вызова метода из обеих точек НЕ менять.

    (e) `EnsureDeclaredNodesAsync` (сигнатура + `Dictionary<string, long> track`, `HaFactState haFacts`) — точка ускорения:

```csharp
                var accelerated = await AccelerateDeadLeaderFailoverAsync(
                    cluster, shard, node.Name, addresses, ct);
                if (!accelerated.IsSuccess)
                    return accelerated;
                // Наблюдатель: Applied — accelerated; NoCandidate — elections
                // (мёртвый лидер без живого кандидата, промоушен ждёт Patroni);
                // NotLeader — не событие (нода не лидер scope). TO_REMOVE —
                // демонтируемое, фактов нет (гвард только на факте).
                if (!shard.ToRemove && accelerated.Value is not FailoverAcceleration.NotLeader)
                    haFacts.FailoverDetected(shard.Name, node.Name,
                        accelerated.Value == FailoverAcceleration.Applied ? "accelerated" : "elections",
                        track.GetValueOrDefault($"{shard.Name}/{node.Name}", Now()));
```

    (f) `SuperviseShardAsync` (сигнатура + `HaFactState haFacts`):
    - после `var leader = ...ParseService...`:

```csharp
        // Закрытие открытого failover: лидер scope — ДРУГАЯ нода (R4: смена без
        // открытого факта событием не изобретается — только закрытие существующего).
        haFacts.LeaderChanged(shard.Name, leader, Now());
```

    - существующий if-блок ускорения dead-ветки: УСЛОВИЕ БИТ-В-БИТ исходное, внутри добавлен только наблюдатель + else-ветка наблюдения:

```csharp
            // Условие ускорения — без изменений (механика надзора не меняется);
            // новые гварды ToRemove — ТОЛЬКО вокруг записи фактов.
            if (isLeader && !adopted && node.State != NodeState.ToRecreate
                && alive.Count > 0 && driver.SupportsRunningInspection)
            {
                var running = await driver.InspectNodesAsync(cluster, [name], ct);
                if (!running.IsSuccess)
                    return running.Error!;
                if (!running.Value.ContainsKey(name))
                {
                    var accelerated = await AccelerateDeadLeaderFailoverAsync(
                        cluster, shard, name, addresses, ct);
                    if (!accelerated.IsSuccess)
                        return accelerated;
                    // В этой ветке NotLeader невозможен (isLeader по свежему
                    // leader-ключу тика): Applied → accelerated, NoCandidate →
                    // elections (spec §3.1).
                    if (!shard.ToRemove)
                        haFacts.FailoverDetected(shard.Name, name,
                            accelerated.Value == FailoverAcceleration.Applied ? "accelerated" : "elections",
                            track[trackKey]);
                }
            }
            else if (isLeader && !adopted && node.State != NodeState.ToRecreate && !shard.ToRemove)
            {
                // Ускорение не применяется (нет честной инспекции/живых по пробам):
                // промоушен ждёт Patroni — фиксируем elections-факт (наблюдатель).
                haFacts.FailoverDetected(shard.Name, name, "elections", track[trackKey]);
            }
```

    (`track[trackKey]` — `track.TryAdd(trackKey, Now())` выше по dead-ветке уже выполнен; else-ветка не выполняется для `ToRecreate`/adopted — их домен RecreateMarkedNodes/журнал, факта нет.)

    - rebuild-ветка — после успешного `rebuilding`-put, ДО `track.Remove(trackKey)`:

```csharp
                if (!shard.ToRemove)
                    haFacts.RebuildDetected(shard.Name, name, "auto-dead", track[trackKey]);
                track.Remove(trackKey); // пересоздана — счётчик с нуля
```

    - alive-ветка — после `track.Remove($"{shard.Name}/{name}")`:

```csharp
            // Флап-оживание лидера: недоступность БЕЗ смены лидера — факта не было.
            haFacts.LeaderRecovered(shard.Name, name, leader);
            // Живая нода пробой — закрытие открытого rebuild (переход → RUNNING
            // или уже RUNNING при takeover после подъёма).
            haFacts.NodeAlive(shard.Name, name, Now());
```

    (g) `RecreateMarkedNodesAsync` (сигнатура + `Dictionary<string, long> track`, `HaFactState haFacts`) — после успешного `rebuilding`-put (до удаления маркера):

```csharp
            if (!shard.ToRemove)
                haFacts.RebuildDetected(shard.Name, node.Name, "operator-recreate",
                    track.GetValueOrDefault($"{shard.Name}/{node.Name}", Now()));
```

    (для живой ноды трека нет — detected = момент исполнения маркера; object-ветка уже сделала `continue` раньше — усыновлённые фактов не порождают.)

    (h) вызовы в `TickAsync` дополнить аргументами `track`/`haFacts`; финальный supervision-put (строка ~164) — сохранить существующий стиль обработки результата, дополнив факты:

```csharp
        await journal.WriteSupervisionAsync(cluster, claims.InstanceId, track, null, ct,
            haFacts.ToRecord());
```

    (i) `ConvergeDcsConfigAsync` (сигнатура + `HaFactState haFacts`) — фазовая запись:

```csharp
        await journal.WritePhaseAsync(cluster, "supervise", "dcs-converge", claims.InstanceId,
            note, ct, unreachable: track, facts: haFacts.ToRecord());
```

    (j) после supervision-put, перед P11 (`masterKeys`) — наблюдатель серий (null-гварда; вызов пассивный, канон файла):

```csharp
        // RTO-серии (arch/18 §2.7): только завершённые длительности; наблюдатель
        // пассивный (try/catch внутри марк-метода).
        if (haDurations is not null)
        {
            var factsRecord = haFacts.ToRecord();
            haDurations(cluster, snap.Shards
                .Where(s => s.Dsn is not null)
                .ToDictionary(
                    s => s.Name,
                    s => ((factsRecord.LastFailover is { } f && f.Shard == s.Name) ? f.DurationSec : null,
                          (factsRecord.LastRebuild is { } r && r.Shard == s.Name) ? r.DurationSec : null)));
        }
```

  - **Выход:** встройка готова; механика надзора не изменена (условия ускорения бит-в-бит; NoCandidate → elections).
  - **Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~NodeSupervisorTests"` — PASS все (новые + старые).
  - **Связь со spec:** §3.1/§3.2 (точки; ускорение без кандидата = elections), §2 («механики надзора не меняются»), AC2.

- [ ] **Шаг 4.3: AdoptionProcess — перенос фактов при явной передаче трека**

  - **Вход:** шаг 4.2 (сборка решения падает/AdoptionProcess-тесты падают: `ReadUnreachableAsync`-путь и явная передача `unreachable` без фактов).
  - **Действие:** `AdoptionProcess.cs` (строки ~388 и ~428): чтение `ReadUnreachableAsync` заменить на `ReadSupervisionStateAsync` (одно чтение — трек + факты); в `WritePhaseAsync(..., unreachable: unreachableTrack.Value)` добавить `facts: new HaSupervisionFacts(state.LastFailover, state.LastRebuild)` от прочитанного состояния.
  - **Выход:** фазовые записи AdoptionProcess не стирают факты.
  - **Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~AdoptionProcessTests"` — PASS.
  - **Связь со spec:** §3.2 (carry-forward), R1.

- [ ] **Шаг 4.4: сборка решения и коммит**

  - **Вход:** шаги 4.2–4.3 зелёные.
  - **Действие:**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug
git add src/PgWorker.Provisioning/Processes/NodeSupervisor.cs src/PgWorker.Provisioning/Processes/AdoptionProcess.cs src/tests/PgWorker.UnitTests/Provisioning/NodeSupervisorTests.cs
git commit -m "feat(t17): NodeSupervisor фиксирует HA-факты в work-ключе (failover accelerated/elections, rebuild auto-dead/operator-recreate, carry-forward)"
```

  - **Выход:** коммит.
  - **Проверка:** сборка 0 warnings; `git log -1 --stat` — 3 файла.
  - **Связь со spec:** §4 фаза 2.

---

### Задача 5: метрики — RTO-серии + подключение + Grafana-панель

**Files:**
- Modify: `src/Shared.Metrics/Worker/WorkerMetricsInstrumentation.cs`
- Modify: `src/PgWorker.App/Program.cs` (регистрация NodeSupervisor, строки ~344-365)
- Modify: `dev-stand/adminpanel/metrics/grafana/dashboards/backups.json`
- Modify (тесты): `src/tests/Shared.Metrics.UnitTests/WorkerMetricsInstrumentationTests.cs`, `src/tests/Shared.Metrics.UnitTests/MetricsEndpointTests.cs`

**Interfaces:**
- Consumes: `WorkerMetricsInstrumentation(Meter, TimeProvider)`; делегат `haDurations` (задача 4).
- Produces: марк-метод `public void HaDurations(string cluster, IReadOnlyDictionary<string, (long? FailoverSec, long? RebuildSec)> shards)`; серии `pgworker_ha_failover_duration_seconds`/`pgworker_ha_rebuild_duration_seconds` (gauge, unit `s`, labels cluster/shard); `DebugState` + поля `HaFailoverDurations`/`HaRebuildDurations`.

- [ ] **Шаг 5.1: провальные юниты стейта**

  - **Вход:** коммит задачи 4.
  - **Действие:** добавить в `WorkerMetricsInstrumentationTests.cs` (по образцу `BackupFullAge_ЗамещениеНабора...`):

```csharp
[Fact]
public void HaDurations_ЗамещениеНабора_nullУбирает_СериюШардаИсчезает()
{
    // Arrange
    using var meter = new Meter("test");
    var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);

    // Act: тик 1 — оба факта; тик 2 — failover закрылся (55с), rebuild ушёл (null)
    sut.HaDurations("c1", new Dictionary<string, (long?, long?)>
    {
        ["s1"] = (null, 40),
        ["s2"] = (10, null),
    });
    sut.HaDurations("c1", new Dictionary<string, (long?, long?)>
    {
        ["s1"] = (55, null),
    });

    // Assert: набор кластера перезаписан целиком; s2 исчез; null-факт не эмитится
    var state = sut.DebugSnapshot();
    state.HaFailoverDurations.Should().ContainKey(("c1", "s1")).WhoseValue.Should().Be(55);
    state.HaFailoverDurations.Should().NotContainKey(("c1", "s2"));
    state.HaRebuildDurations.Should().BeEmpty();
}

[Fact]
public void HaDurations_РазныеКластерыНезависимы()
{
    // Arrange
    using var meter = new Meter("test");
    var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);

    // Act
    sut.HaDurations("c1", new Dictionary<string, (long?, long?)> { ["s1"] = (11, null) });
    sut.HaDurations("c2", new Dictionary<string, (long?, long?)> { ["s1"] = (null, 22) });

    // Assert
    var state = sut.DebugSnapshot();
    state.HaFailoverDurations[("c1", "s1")].Should().Be(11);
    state.HaRebuildDurations[("c2", "s1")].Should().Be(22);
}
```

  - **Выход:** тесты в файле.
  - **Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/Shared.Metrics.UnitTests -c Debug --filter "FullyQualifiedName~WorkerMetricsInstrumentationTests"` — FAIL компиляции (метод/поля отсутствуют).
  - **Связь со spec:** §3.3 (марк-метод/стейт), AC3.

- [ ] **Шаг 5.2: реализация марк-метода и серий**

  - **Вход:** шаг 5.1 (падение по отсутствию).
  - **Действие:** в `WorkerMetricsInstrumentation.cs`:

    (a) стейт-поля рядом с `_fullMaxAge`:

```csharp
    private readonly Dictionary<(string Cluster, string Shard), long> _haFailoverSec = new();
    private readonly Dictionary<(string Cluster, string Shard), long> _haRebuildSec = new();
```

    (b) два ObservableGauge рядом с `pgworker_backup_full_max_age_seconds`:

```csharp
        meter.CreateObservableGauge(
            "pgworker_ha_failover_duration_seconds",
            () => Measure(() => _haFailoverSec.Select(kv =>
                new Measurement<long>(kv.Value,
                    new KeyValuePair<string, object?>("cluster", kv.Key.Cluster),
                    new KeyValuePair<string, object?>("shard", kv.Key.Shard)))),
            unit: "s", description: "Длительность последнего завершённого failover, с (arch/14 §3.3)");

        meter.CreateObservableGauge(
            "pgworker_ha_rebuild_duration_seconds",
            () => Measure(() => _haRebuildSec.Select(kv =>
                new Measurement<long>(kv.Value,
                    new KeyValuePair<string, object?>("cluster", kv.Key.Cluster),
                    new KeyValuePair<string, object?>("shard", kv.Key.Shard)))),
            unit: "s", description: "Длительность последнего завершённого rebuild, с (arch/14 §3.3)");
```

    (c) марк-метод (по образцу `BackupFullAge`):

```csharp
    // Gauge pgworker_ha_{failover,rebuild}_duration_seconds (arch/18 §2.7):
    // ЕДИНЫЙ марк-метод на тик надзора — набор шардов тика замещает стейт
    // кластера ЦЕЛИКОМ (ушедшие шарды серии не копят; null-факт — серия шарда
    // исчезает до появления завершённого факта).
    public void HaDurations(string cluster, IReadOnlyDictionary<string, (long? FailoverSec, long? RebuildSec)> shards)
    {
        try
        {
            lock (_lock)
            {
                foreach (var gone in _haFailoverSec.Keys.Concat(_haRebuildSec.Keys)
                             .Where(k => k.Cluster == cluster && !shards.ContainsKey(k.Shard))
                             .Distinct().ToList())
                {
                    _haFailoverSec.Remove(gone);
                    _haRebuildSec.Remove(gone);
                }

                foreach (var (shard, value) in shards)
                {
                    if (value.FailoverSec is { } failover)
                        _haFailoverSec[(cluster, shard)] = failover;
                    else
                        _haFailoverSec.Remove((cluster, shard));
                    if (value.RebuildSec is { } rebuild)
                        _haRebuildSec[(cluster, shard)] = rebuild;
                    else
                        _haRebuildSec.Remove((cluster, shard));
                }
            }
        }
        catch
        {
            // Пассивный наблюдатель: ошибка инструментария не влияет на цикл.
        }
    }
```

    (d) `DebugSnapshot()` — добавить `_haFailoverSec.ToFrozenDictionary()`/`_haRebuildSec.ToFrozenDictionary()`; в record `DebugState` — поля `IReadOnlyDictionary<(string Cluster, string Shard), long> HaFailoverDurations`/`HaRebuildDurations` (в конец record-а).

  - **Выход:** серии и марк-метод реализованы.
  - **Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/Shared.Metrics.UnitTests -c Debug --filter "FullyQualifiedName~WorkerMetricsInstrumentationTests"` — PASS.
  - **Связь со spec:** §3.3, R9.

- [ ] **Шаг 5.3: канон-тест словаря /metrics**

  - **Вход:** шаг 5.2 зелёный.
  - **Действие:** в `MetricsEndpointTests.cs`, тест `MetricsEndpoint_ExportsDictionaryNames`: в Arrange после `sut.SnapshotTaken(now);` добавить `sut.HaDurations("demo", new Dictionary<string, (long?, long?)> { ["s1"] = (12, 34) });`; в Assert — две строки:

```csharp
            body.Should().Contain("pgworker_ha_failover_duration_seconds");
            body.Should().Contain("pgworker_ha_rebuild_duration_seconds");
```

  - **Выход:** канон-тест фиксирует имена серий в экспорте.
  - **Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/Shared.Metrics.UnitTests -c Debug` — PASS (весь проект).
  - **Связь со spec:** AC3 (канон-тест по образцу t14).

- [ ] **Шаг 5.4: подключение в Program.cs**

  - **Вход:** шаг 5.2 (марк-метод существует; сигнатуры делегата и метода совпадают).
  - **Действие:** в регистрации `NodeSupervisor` (`Program.cs`, строки ~344-365): последним аргументом УЖЕ является `sp.GetRequiredService<EtcdEndpoints>()` — добавить СЛЕДОМ ещё ОДИН аргумент-делегат (по образцу `fullAgeObserver` BackupProcess, Program.cs:518). Итоговый хвост регистрации:

```csharp
    sp.GetRequiredService<EtcdEndpoints>(),
    sp.GetRequiredService<Shared.Metrics.Worker.WorkerMetricsInstrumentation>().HaDurations));
```

    (дублирующих аргументов нет: `EtcdEndpoints` остаётся одним — предпоследним.)

  - **Выход:** тик надзора питает серии.
  - **Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug` — 0 errors (компиляция регистрации).
  - **Связь со spec:** §3.3 (подключение ctor-параметром, паттерн t14).

- [ ] **Шаг 5.5: Grafana-панель**

  - **Вход:** серии реализованы.
  - **Действие:** в `dev-stand/adminpanel/metrics/grafana/dashboards/backups.json` добавить панель (структуру скопировать из существующей «Drill outcomes…», тип `timeseries`, уникальный `id`, gridPos — следующая свободная позиция): title `HA RTO durations, s (by cluster/shard)`; два query: `pgworker_ha_failover_duration_seconds` (legend `failover {{cluster}}/{{shard}}`) и `pgworker_ha_rebuild_duration_seconds` (legend `rebuild {{cluster}}/{{shard}}`); datasource — тот же Prometheus, что у соседних панелей.
  - **Выход:** панель в дашборде.
  - **Проверка:** `python3 -c "import json; json.load(open('dev-stand/adminpanel/metrics/grafana/dashboards/backups.json'))"` — exit 0 (валидный JSON).
  - **Связь со spec:** §3.3 (Grafana).

- [ ] **Шаг 5.6: сборка Release и коммит**

  - **Вход:** шаги 5.2–5.5 выполнены.
  - **Действие:**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
git add src/Shared.Metrics/Worker/WorkerMetricsInstrumentation.cs src/PgWorker.App/Program.cs dev-stand/adminpanel/metrics/grafana/dashboards/backups.json src/tests/Shared.Metrics.UnitTests/
git commit -m "feat(t17): RTO-серии pgworker_ha_{failover,rebuild}_duration_seconds + Grafana-панель"
```

  - **Выход:** коммит.
  - **Проверка:** сборка Release 0 warnings 0 errors; `git log -1 --stat`.
  - **Связь со spec:** §4 фаза 3.

---

### Задача 6: панель — модель HaSupervisionInfo + парсер work-ключа

**Files:**
- Modify: `src/AdminPanel.Core/WorkJournalInfo.cs`
- Modify: `src/AdminPanel.Etcd/Parsing/WorkJournalParser.cs`
- Modify (тесты): `src/tests/AdminPanel.UnitTests/WorkJournalParserTests.cs`

**Interfaces:**
- Consumes: `KeyParseError(string Key, string Reason)`; формат work-ключа arch/14 §3.3 (задача 1).
- Produces (для задачи 7): `HaSupervisionInfo(string Shard, string Node, string Cause, long DetectedUnix, long? ResolvedUnix, long? DurationSec)`; `WorkJournalInfo` + `HaSupervisionInfo? LastFailover = null`, `HaSupervisionInfo? LastRebuild = null`.

- [ ] **Шаг 6.1: провальные тесты парсера**

  - **Вход:** коммиты задач 1–5; `WorkJournalParserTests.cs` (существующий стиль: сид `Kv`, вызов `Parse([...])`).
  - **Действие:** добавить в `WorkJournalParserTests.cs`:

```csharp
[Fact]
public void Parse_LastFailoverAndRebuild_Mapped()
{
    // Arrange: work-ключ с закрытым failover и открытым rebuild
    var kv = new Kv("/pgworker/work/shop", """
        {"op":"supervise","phase":"supervising","instance":"i1","updated_unix":1756000000,
         "unreachable":{"shard1/shard1b":1756000000},
         "last_failover":{"shard":"shard1","node":"shard1b","cause":"accelerated","detected_unix":1000,"resolved_unix":1075,"duration_sec":75},
         "last_rebuild":{"shard":"shard1","node":"shard1a","cause":"auto-dead","detected_unix":900}}
        """);

    // Act
    var result = WorkJournalParser.Parse([kv]);

    // Assert
    var item = result.Items.Should().ContainSingle().Subject;
    item.LastFailover.Should().NotBeNull();
    item.LastFailover!.Shard.Should().Be("shard1");
    item.LastFailover.Node.Should().Be("shard1b");
    item.LastFailover.Cause.Should().Be("accelerated");
    item.LastFailover.DetectedUnix.Should().Be(1000);
    item.LastFailover.ResolvedUnix.Should().Be(1075);
    item.LastFailover.DurationSec.Should().Be(75);
    item.LastRebuild!.Cause.Should().Be("auto-dead");
    item.LastRebuild.ResolvedUnix.Should().BeNull();
    result.Errors.Should().BeEmpty();
}

[Fact]
public void Parse_OldKeyWithoutFacts_Nulls()
{
    // Arrange: старый ключ (надзор прежней версии) — полей фактов нет
    var kv = new Kv("/pgworker/work/old",
        """{"op":"supervise","phase":"supervising","instance":"i","updated_unix":1756000000}""");

    // Act
    var result = WorkJournalParser.Parse([kv]);

    // Assert: толерантный читатель — факты null, не ошибка
    var item = result.Items.Should().ContainSingle().Subject;
    item.LastFailover.Should().BeNull();
    item.LastRebuild.Should().BeNull();
    result.Errors.Should().BeEmpty();
}

[Fact]
public void Parse_UnrelatedFields_Ignored()
{
    // Arrange: незнакомые поля значения — игнор
    var kv = new Kv("/pgworker/work/shop", """
        {"op":"supervise","phase":"supervising","instance":"i1","updated_unix":1756000000,
         "future_field":123,"last_failover":{"shard":"s","node":"n","cause":"elections","detected_unix":1,"extra":"x"}}
        """);

    // Act
    var result = WorkJournalParser.Parse([kv]);

    // Assert
    result.Items.Should().ContainSingle().Subject.LastFailover!.Cause.Should().Be("elections");
    result.Errors.Should().BeEmpty();
}

[Fact]
public void Parse_MalformedFactField_ParseError()
{
    // Arrange: поле last_failover не объект / без обязательных полей — битое значение ключа
    var kv = new Kv("/pgworker/work/shop",
        """{"op":"supervise","phase":"supervising","instance":"i1","updated_unix":1756000000,"last_failover":"oops"}""");

    // Act
    var result = WorkJournalParser.Parse([kv]);

    // Assert: parseError-запись (существующий паттерн толерантности)
    result.Items.Should().BeEmpty();
    result.Errors.Should().ContainSingle().Which.Key.Should().Be("/pgworker/work/shop");
}
```

  (конструктор `Kv` сверить с существующими тестами файла.)
  - **Выход:** тесты в файле.
  - **Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~WorkJournalParserTests"` — FAIL компиляции (`LastFailover`/`LastRebuild` отсутствуют в `WorkJournalInfo`).
  - **Связь со spec:** §3.4 (парсер, толерантность).

- [ ] **Шаг 6.2: реализация модели и парсера**

  - **Вход:** шаг 6.1 (падение).
  - **Действие:**

    (a) `WorkJournalInfo.cs` — модель и поля:

```csharp
/// <summary>Последний HA-факт надзора из /pgworker/work/&lt;C&gt; (arch/14 §3.3;
/// панельный дубль воркерной модели — осознанный): null = факта нет/старый ключ.</summary>
public sealed record HaSupervisionInfo(
    string Shard, string Node, string Cause,
    long DetectedUnix, long? ResolvedUnix, long? DurationSec);
```

    `WorkJournalInfo` — два nullable-поля в конец: `HaSupervisionInfo? LastFailover = null, HaSupervisionInfo? LastRebuild = null`.

    (b) `WorkJournalParser.Parse` — внутри существующего try после сбора основных полей (битое факт-поле → parseError + `continue`, item не добавляется — ожидание `Parse_MalformedFactField_ParseError`):

```csharp
                if (!TryFact(root, "last_failover", kv.Key, out var failover, errors)
                    || !TryFact(root, "last_rebuild", kv.Key, out var rebuild, errors))
                    continue;

                items.Add(new WorkJournalInfo(
                    cluster,
                    String(root, "op") ?? "",
                    String(root, "phase") ?? "",
                    String(root, "instance") ?? "",
                    Long(root, "updated_unix") ?? 0,
                    String(root, "last_error"),
                    (int?)Long(root, "fail_count"),
                    Long(root, "fail_first_unix"),
                    Long(root, "retry_not_before_unix"),
                    failover,
                    rebuild));
```

    helper рядом с `String`/`Long`:

```csharp
    // Факт-поле (last_failover/last_rebuild): отсутствует/null → факт null (старый
    // ключ); битое (не объект / нет обязательных shard/node/cause/detected_unix) —
    // false + parseError-запись (толерантность: тик не роняют, ключ не трогаем).
    private static bool TryFact(
        JsonElement root, string name, string key,
        out HaSupervisionInfo? fact, List<KeyParseError> errors)
    {
        fact = null;
        if (!root.TryGetProperty(name, out var el) || el.ValueKind == JsonValueKind.Null)
            return true;
        if (el.ValueKind != JsonValueKind.Object
            || !el.TryGetProperty("shard", out var shardEl) || shardEl.ValueKind != JsonValueKind.String
            || !el.TryGetProperty("node", out var nodeEl) || nodeEl.ValueKind != JsonValueKind.String
            || !el.TryGetProperty("cause", out var causeEl) || causeEl.ValueKind != JsonValueKind.String
            || !el.TryGetProperty("detected_unix", out var detectedEl) || detectedEl.ValueKind != JsonValueKind.Number)
        {
            errors.Add(new(key, $"поле {name} битое: ожидается объект shard/node/cause/detected_unix"));
            return false;
        }

        fact = new HaSupervisionInfo(
            shardEl.GetString()!, nodeEl.GetString()!, causeEl.GetString()!,
            detectedEl.GetInt64(),
            el.TryGetProperty("resolved_unix", out var resolved) && resolved.ValueKind == JsonValueKind.Number
                ? resolved.GetInt64() : null,
            el.TryGetProperty("duration_sec", out var duration) && duration.ValueKind == JsonValueKind.Number
                ? duration.GetInt64() : null);
        return true;
    }
```

  - **Выход:** парсер читает факты толерантно.
  - **Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~WorkJournalParserTests"` — PASS (все).
  - **Связь со spec:** §3.4, §2 (толерантный читатель).

- [ ] **Шаг 6.3: сборка и коммит**

  - **Вход:** шаг 6.2 зелёный.
  - **Действие:**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug
git add src/AdminPanel.Core/WorkJournalInfo.cs src/AdminPanel.Etcd/Parsing/WorkJournalParser.cs src/tests/AdminPanel.UnitTests/WorkJournalParserTests.cs
git commit -m "feat(t17): панель — парсер HA-фактов work-ключа (HaSupervisionInfo, толерантность)"
```

  - **Выход:** коммит.
  - **Проверка:** сборка 0 warnings; `git log -1 --stat` — 3 файла.
  - **Связь со spec:** §4 фаза 4.

---

### Задача 7: ReliabilityCalculator — вычислитель RPO/RTO

**Files:**
- Create: `src/AdminPanel.Core/ReliabilityCalculator.cs`
- Create: `src/tests/AdminPanel.UnitTests/ReliabilityCalculatorTests.cs`

**Interfaces:**
- Consumes: `EtcdSnapshot` (`Clusters`, `Backups`, `PgWorkerWork`), `ClusterBackupsInfo` (`ShardLastCompletedUnix`, `Shards`, `ShardsFulls`, `ShardsDrills`, `ShardsRestores`, `Policy`), `BackupsPolicyInfo`, `WalStreamInfo(WalStreamInfoState)`, `DrillInfo`, `RestoreOperationInfo`, `BackupFullInfo`, `BackupFullStaleRule.DefaultMaxAgeSec`, `HaSupervisionInfo` (задача 6).
- Produces (для задачи 8): `public static class ReliabilityCalculator { public static IReadOnlyList<ClusterReliability> Calculate(EtcdSnapshot snapshot, DateTimeOffset nowUtc); }` и модели:

```csharp
public enum RpoMode { Wal, Full, Off }
public sealed record ClusterReliability(string Cluster, IReadOnlyList<ShardReliability> Shards);
public sealed record ShardReliability(string Shard, bool Declared, RpoBlock? Rpo, RtoBlock? Rto);
public sealed record RpoBlock(
    RpoMode Mode, long? FullAgeSec, string? FullId, long? WalLagSegments, long? WalAgeSec,
    long? RpoPotentialSec, long ThresholdFullAgeSec);
public sealed record HaFactRto(
    string Shard, string Node, string Cause, long DetectedUnix, long? ResolvedUnix,
    long? DurationSec, bool Ongoing, long OngoingSec);
public sealed record OpRto(string State, long? DurationSec, long? OngoingSec, long? FinishedUnix, string? Error);
public sealed record RtoBlock(HaFactRto? LastFailover, HaFactRto? LastRebuild, OpRto? LastDrill, OpRto? LastRestore);
```

Правила вычисления (spec §3.4):
- Шарды таблицы — живая декларация кластера (`snapshot.Clusters`); факт/бэкап-данные шарда вне декларации не отображаются (R10).
- `Rpo`: кластера нет в `Backups` → блок `Mode=Off` без чисел (префикс пуст — подсистема не включена); шард не в `ShardLastCompletedUnix` → `Mode=Off`. `FullAgeSec = now − ShardLastCompletedUnix[X]` (значение null → FullAgeSec=null «никогда»); `FullId` = id полного с max `FinishedUnix` среди `State="COMPLETED"` и `VerifyState != "FAILED"` из `ShardsFulls`; `WalLagSegments`/`WalAgeSec` из `Shards[X]` (`now − LastUploadedUnix`); `Mode`/`RpoPotentialSec`: wal-статус `Active|Degraded` → `Mode=Wal`, `RpoPotentialSec=WalAgeSec`; `Broken|Stopped` или wal нет → `Mode=Full`, `RpoPotentialSec=FullAgeSec`.
- **`ThresholdFullAgeSec = Policy.FullMaxAgeSec ?? BackupFullStaleRule.DefaultMaxAgeSec`** — источник именно `ClusterBackupsInfo.Policy.FullMaxAgeSec` (полная policy кластера из policy-ключа), НЕ `ClusterBackupsInfo.FullMaxAgeSec` и не `AlertsOptions`; осознанное расхождение с правилом `BackupFullStaleRule` (то читает `cluster.FullMaxAgeSec is > 0` + панельный `AlertsOptions.BackupFullMaxAgeSec`-конфиг — у калькулятора конфиг-фолбэка нет, только каталожный дефолт 86400).
- `Rto`: из `PgWorkerWork` кластера: `LastFailover`/`LastRebuild` → `HaFactRto` (закрытый: `DurationSec`; открытый: `Ongoing=true`, `OngoingSec = now − DetectedUnix`); `LastDrill` из `ShardsDrills[X]` (State RUNNING → ongoing от `StartedUnix`, иначе `FinishedUnix − StartedUnix`); `LastRestore` — последняя заявка `ShardsRestores[X]` по max `RequestedUnix` (активная → ongoing от `RequestedUnix`, иначе `FinishedUnix − RequestedUnix` — от ЗАЯВКИ, включая очередь).

- [ ] **Шаг 7.1: провальные тесты**

  - **Вход:** коммит задачи 6; helpers `TestSnapshots.HealthyEtcd` (доступен: тот же проект AdminPanel.UnitTests).
  - **Действие:** создать `src/tests/AdminPanel.UnitTests/ReliabilityCalculatorTests.cs`:

```csharp
using AdminPanel.Core;
using AdminPanel.Core.Alerting.Rules;

namespace AdminPanel.UnitTests;

// ReliabilityCalculator (spec §3.4): чистая функция над снапшотом — RPO-блок
// (валидный полный, лаг WAL, сводный потенциал) и RTO-блок (факты/drill/restore).
public class ReliabilityCalculatorTests
{
    private static readonly DateTimeOffset Now = DateTimeOffset.FromUnixTimeSeconds(1_760_000_000);

    private static EtcdSnapshot Snapshot(
        IReadOnlyList<ClusterBackupsInfo>? backups = null,
        IReadOnlyList<WorkJournalInfo>? work = null,
        IReadOnlyList<ClusterInfo>? clusters = null)
        => new(
            Now,
            TestSnapshots.HealthyEtcd(Now),      // живой EtcdStatus (существующий helper)
            clusters ?? [], [], [], [],
            backups ?? [],                        // Backups
            [],                                   // PgWorkerEndpoints
            work ?? [],                           // PgWorkerWork
            [], [], [], [],                       // WorkerHealth, Probes, Alerts, ParseErrors
            0);

    [Fact]
    public void WalActive_RpoPotential_IsWalAge()
    {
        // Arrange: свежий полный (age 100с), wal ACTIVE с age 10с; policy-ключа нет
        var backups = new List<ClusterBackupsInfo>
        {
            new("shop", null,
                new Dictionary<string, long?> { ["s1"] = Now.ToUnixTimeSeconds() - 100 },
                Shards: new Dictionary<string, WalStreamInfo?>
                {
                    ["s1"] = new("shop", "s1", WalStreamInfoState.Active, "slot", "n1",
                        Now.ToUnixTimeSeconds() - 10, 3, null),
                }),
        };
        var clusters = new List<ClusterInfo> { Cluster("shop", "s1") };

        // Act
        var result = ReliabilityCalculator.Calculate(Snapshot(backups, clusters: clusters), Now);

        // Assert: RPO держится WAL-хвостом — потеряем возраст последней загрузки;
        // policy нет — порог = каталожный дефолт правила BackupFullStaleRule
        var rpo = result.Single().Shards.Single().Rpo!;
        rpo.Mode.Should().Be(RpoMode.Wal);
        rpo.RpoPotentialSec.Should().Be(10);
        rpo.FullAgeSec.Should().Be(100);
        rpo.WalLagSegments.Should().Be(3);
        rpo.ThresholdFullAgeSec.Should().Be(BackupFullStaleRule.DefaultMaxAgeSec);
    }

    [Fact]
    public void WalBroken_RpoPotential_FallsBackToFull_ThresholdFromPolicy()
    {
        // Arrange: цепочка WAL разорвана — потеряем весь хвост после полного;
        // порог — из ПОЛНОЙ policy кластера (Policy.FullMaxAgeSec), не из
        // ClusterBackupsInfo.FullMaxAgeSec и не дефолт
        var t = Now.ToUnixTimeSeconds();
        var backups = new List<ClusterBackupsInfo>
        {
            new("shop", null,
                new Dictionary<string, long?> { ["s1"] = t - 500 },
                Shards: new Dictionary<string, WalStreamInfo?>
                {
                    ["s1"] = new("shop", "s1", WalStreamInfoState.Broken, "slot", "n1", t - 5, 9, "err"),
                },
                Policy: new BackupsPolicyInfo(null, null, null, 86400, null, null)),
        };

        // Act
        var result = ReliabilityCalculator.Calculate(Snapshot(backups, clusters: [Cluster("shop", "s1")]), Now);

        // Assert
        var rpo = result.Single().Shards.Single().Rpo!;
        rpo.Mode.Should().Be(RpoMode.Full);
        rpo.RpoPotentialSec.Should().Be(500);
        rpo.ThresholdFullAgeSec.Should().Be(86400);
    }

    [Fact]
    public void NoBackupsPrefix_ModeOff()
    {
        // Arrange: префикс бэкапов пуст — подсистема не включена, секция молчит
        // Act
        var result = ReliabilityCalculator.Calculate(Snapshot(clusters: [Cluster("shop", "s1")]), Now);

        // Assert
        var rpo = result.Single().Shards.Single().Rpo!;
        rpo.Mode.Should().Be(RpoMode.Off);
        rpo.RpoPotentialSec.Should().BeNull();
    }

    [Fact]
    public void FullId_ValidCompletedNotFailedVerify()
    {
        // Arrange: FAILED-полный НОВЕЕ валидного — свежесть/источник только валидный
        var t = Now.ToUnixTimeSeconds();
        var backups = new List<ClusterBackupsInfo>
        {
            new("shop", null, new Dictionary<string, long?> { ["s1"] = t - 100 },
                ShardsFulls: new Dictionary<string, IReadOnlyList<BackupFullInfo>>
                {
                    ["s1"] = new List<BackupFullInfo>
                    {
                        new("b-old", "COMPLETED", null, t - 300, t - 100, 1, "OK", null, null),
                        new("b-new", "COMPLETED", null, t - 50, t - 40, 1, "FAILED", t - 30, "verify err"),
                    },
                }),
        };

        // Act
        var result = ReliabilityCalculator.Calculate(Snapshot(backups, clusters: [Cluster("shop", "s1")]), Now);

        // Assert: id валидного (verify ≠ FAILED); FAILED новее — не свежесть
        result.Single().Shards.Single().Rpo!.FullId.Should().Be("b-old");
    }

    [Fact]
    public void OpenFailoverFact_OngoingSecondsTick()
    {
        // Arrange: открытый failover (detected 200с назад) в work-ключе
        var work = new List<WorkJournalInfo>
        {
            new("shop", "supervise", "supervising", "i1", Now.ToUnixTimeSeconds(), null, null, null, null,
                new HaSupervisionInfo("s1", "n1", "elections", Now.ToUnixTimeSeconds() - 200, null, null), null),
        };

        // Act
        var result = ReliabilityCalculator.Calculate(Snapshot(work: work, clusters: [Cluster("shop", "s1")]), Now);

        // Assert: ongoing с тикающей длительностью now − detected
        var failover = result.Single().Shards.Single().Rto!.LastFailover!;
        failover.Ongoing.Should().BeTrue();
        failover.OngoingSec.Should().Be(200);
        failover.DurationSec.Should().BeNull();
    }

    [Fact]
    public void FactOfUndeclaredShard_NotShown()
    {
        // Arrange: факт шарда s9, живая декларация — только s1
        var work = new List<WorkJournalInfo>
        {
            new("shop", "supervise", "supervising", "i1", Now.ToUnixTimeSeconds(), null, null, null, null,
                new HaSupervisionInfo("s9", "n1", "elections", 1, 2, 1), null),
        };

        // Act
        var result = ReliabilityCalculator.Calculate(Snapshot(work: work, clusters: [Cluster("shop", "s1")]), Now);

        // Assert: факт пережил удаление шарда из декларации — не отображается
        result.Single().Shards.Should().ContainSingle().Which.Shard.Should().Be("s1");
        result.Single().Shards.Single().Rto!.LastFailover.Should().BeNull();
    }

    [Fact]
    public void OldKeyWithoutFacts_RtoEmpty()
    {
        // Arrange: work-ключ старого формата (без полей фактов)
        var work = new List<WorkJournalInfo>
        {
            new("shop", "supervise", "supervising", "i1", Now.ToUnixTimeSeconds(), null, null, null, null),
        };

        // Act/Assert: толерантность — RTO-блок пустой, не падает
        var rto = ReliabilityCalculator.Calculate(Snapshot(work: work, clusters: [Cluster("shop", "s1")]), Now)
            .Single().Shards.Single().Rto!;
        ((object?)rto.LastFailover).Should().BeNull();
        ((object?)rto.LastRebuild).Should().BeNull();
    }

    [Fact]
    public void ClosedRebuildFact_DurationFromFact()
    {
        // Arrange: закрытый rebuild — длительность фиксирует воркер
        var work = new List<WorkJournalInfo>
        {
            new("shop", "supervise", "supervising", "i1", Now.ToUnixTimeSeconds(), null, null, null, null,
                null, new HaSupervisionInfo("s1", "n2", "auto-dead", 1000, 1150, 150)),
        };

        // Act
        var rto = ReliabilityCalculator.Calculate(Snapshot(work: work, clusters: [Cluster("shop", "s1")]), Now)
            .Single().Shards.Single().Rto!;

        // Assert
        rto.LastRebuild!.Ongoing.Should().BeFalse();
        rto.LastRebuild.DurationSec.Should().Be(150);
    }

    [Fact]
    public void DrillAndRestore_DurationsFromSnapshot()
    {
        // Arrange: завершённый drill (400с), активный restore (от заявки 60с назад)
        var t = Now.ToUnixTimeSeconds();
        var backups = new List<ClusterBackupsInfo>
        {
            new("shop", null, new Dictionary<string, long?> { ["s1"] = t - 100 },
                ShardsDrills: new Dictionary<string, DrillInfo>
                {
                    ["s1"] = new("shop", "s1", "d1", "SUCCEEDED", "b1", t - 500, t - 100, null, null, null),
                },
                ShardsRestores: new Dictionary<string, IReadOnlyList<RestoreOperationInfo>>
                {
                    ["s1"] = new List<RestoreOperationInfo>
                    {
                        new("shop", "s1", "r1", "RUNNING", null, t - 60, t - 50, null, "recovering"),
                    },
                }),
        };

        // Act
        var rto = ReliabilityCalculator.Calculate(Snapshot(backups, clusters: [Cluster("shop", "s1")]), Now)
            .Single().Shards.Single().Rto!;

        // Assert: drill 400с; restore ongoing от RequestedUnix (включая очередь)
        rto.LastDrill!.DurationSec.Should().Be(400);
        rto.LastDrill.State.Should().Be("SUCCEEDED");
        rto.LastRestore!.OngoingSec.Should().Be(60);
        rto.LastRestore.DurationSec.Should().BeNull();
    }

    [Fact]
    public void RestoreFinished_DurationFromRequested()
    {
        // Arrange: терминальный restore: заявка 1000, завершён 1100
        var backups = new List<ClusterBackupsInfo>
        {
            new("shop", null, new Dictionary<string, long?> { ["s1"] = 900 },
                ShardsRestores: new Dictionary<string, IReadOnlyList<RestoreOperationInfo>>
                {
                    ["s1"] = new List<RestoreOperationInfo>
                    {
                        new("shop", "s1", "r1", "COMPLETED", null, 1000, 1005, 1100, null),
                    },
                }),
        };

        // Act
        var rto = ReliabilityCalculator.Calculate(Snapshot(backups, clusters: [Cluster("shop", "s1")]), Now)
            .Single().Shards.Single().Rto!;

        // Assert: честный операторский RTO — от ЗАЯВКИ (включая очередь)
        rto.LastRestore!.DurationSec.Should().Be(100);
    }

    private static ClusterInfo Cluster(string name, params string[] shards)
        => new(name, "db", 2, 1755900000, ClusterState.Active,
            [.. shards.Select(s => new ShardInfo(s, "host=1", ["host=1"], 1, "db", "u", null, null, [], null))],
            [], []);
}
```

  ПРИМЕЧАНИЕ: именованные параметры конструкторов (`Shards:`/`ShardsFulls:`/`Policy:`/…) сверить с фактическими record-определениями (`BackupInfo.cs` — `DrillInfo(Cluster, Shard, Id, State, BackupId, StartedUnix, FinishedUnix, Phase, RestoredToLsn, Error)`, `BackupsPolicyInfo(RetentionDays, RetentionWeeks, RetentionMonths, FullMaxAgeSec, VerifyOnCreate, DrillIntervalDays)`; `EtcdSnapshot.cs`, `ClusterInfo.cs`) и подогнать; смысл ассертов — канон.
  - **Выход:** файл тестов создан.
  - **Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~ReliabilityCalculatorTests"` — FAIL компиляции (`ReliabilityCalculator` не существует).
  - **Связь со spec:** §3.4, AC4.

- [ ] **Шаг 7.2: реализация калькулятора**

  - **Вход:** шаг 7.1 (падение).
  - **Действие:** создать `src/AdminPanel.Core/ReliabilityCalculator.cs` — полная реализация по правилам блока Interfaces: модели (как в Interfaces), один проход по `snapshot.Clusters`; бэкапы/work кластера — `FirstOrDefault`; декларация шардов — `ClusterInfo.Shards`; RPO/RTO-блоки — по правилам; порог — `backups?.Policy?.FullMaxAgeSec ?? BackupFullStaleRule.DefaultMaxAgeSec` (источник — Policy, см. Interfaces); конвертация `HaSupervisionInfo` → `HaFactRto` (закрытый → `DurationSec`, `Ongoing=false`; открытый → `Ongoing=true`, `OngoingSec = nowUnix − DetectedUnix`).
  - **Выход:** вычислитель готов (чистая функция, без etcd/IO).
  - **Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~ReliabilityCalculatorTests"` — PASS.
  - **Связь со spec:** §3.4 (вкл. `thresholdFullAgeSec = Policy.FullMaxAgeSec ?? 86400`), §2 («чистые функции над снапшотом»).

- [ ] **Шаг 7.3: сборка и коммит**

  - **Вход:** шаг 7.2 зелёный.
  - **Действие:**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug
git add src/AdminPanel.Core/ReliabilityCalculator.cs src/tests/AdminPanel.UnitTests/ReliabilityCalculatorTests.cs
git commit -m "feat(t17): ReliabilityCalculator — RPO/RTO-числа чистой функцией над снапшотом"
```

  - **Выход:** коммит.
  - **Проверка:** сборка 0 warnings; `git log -1 --stat` — 2 новых файла.
  - **Связь со spec:** §4 фаза 4.

---

### Задача 8: API — ReliabilityQuery + эндпоинт /api/reliability

**Files:**
- Create: `src/AdminPanel.Api/Inspection/ReliabilityQuery.cs`
- Modify: `src/AdminPanel.Api/Inspection/InspectionModule.cs`
- Create (тесты): `src/tests/AdminPanel.IntegrationTests/ReliabilityApiTests.cs`

**Interfaces:**
- Consumes: `ISnapshotStore` (+ `InspectionModule.SnapshotNotReadyException`), `ReliabilityCalculator` (задача 7), `IHandler.HandleQuery`-паттерн, `AuthWebFactory` (снапшот-инъекция, fixed-время `factory.Time`), `InspectionSnapshots.Clustered` (`InspectionApiTests.cs` — кластер demo, шарды s1/s2).
- Produces (для задачи 10): `GET /api/reliability` → `ReliabilityDto { clusters: [{ cluster, shards: [{ shard, declared, rpo { mode, fullAgeSec?, fullId?, walLagSegments?, walAgeSec?, rpoPotentialSec?, thresholdFullAgeSec }, rto { lastFailover?, lastRebuild?, lastDrill?, lastRestore? } }] }] }`.

- [ ] **Шаг 8.1: провочные смоук-тесты**

  - **Вход:** коммиты задач 1–7; коллекция `"api"` (`AuthWebFactory`: логин admin/adminpw, `AllowHttp`, fixed-время).
  - **Действие:** создать `src/tests/AdminPanel.IntegrationTests/ReliabilityApiTests.cs` (логин — самодостаточный локальный хелпер по телу существующего общего логин-хелпера сборки: сдвиг fixed-времени на 61с — свежее окно rate-limiter'а — и POST `/api/auth/login`):

```csharp
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Core;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// HTTP-контракт /api/reliability (spec §3.5): 401 без cookie; 503 без снапшота;
// 200 с числами из снапшота (пустые секции = подсистема выключена — толерантность).
[Collection("api")]
public class ReliabilityApiTests
{
    private readonly AuthWebFactory _factory;

    public ReliabilityApiTests(AuthWebFactory factory) => _factory = factory;

    // Логин: свежее окно rate-limiter'а (fixed-время фабрики) + cookie в клиенте
    // (самодостаточно — по телу общего логин-хелпера соседних api-тестов).
    private async Task<HttpClient> LoginAsync()
    {
        _factory.Time.Utc += TimeSpan.FromSeconds(61);
        var client = _factory.CreateClient();
        var login = await client.PostAsJsonAsync(
            "/api/auth/login",
            new { username = "admin", password = "adminpw" },
            TestContext.Current.CancellationToken);
        login.StatusCode.Should().Be(HttpStatusCode.NoContent);
        return client;
    }

    [Fact]
    public async Task Reliability_WithoutCookie_Returns401()
    {
        // Arrange
        using var client = _factory.CreateClient();

        // Act
        using var response = await client.GetAsync("/api/reliability", TestContext.Current.CancellationToken);

        // Assert: default-deny guard закрыл эндпоинт
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task Reliability_NoSnapshot_Returns503()
    {
        // Arrange
        _factory.Snapshot = null;
        using var client = await LoginAsync();

        // Act
        using var response = await client.GetAsync("/api/reliability", TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        body.GetProperty("title").GetString().Should().Be("Snapshot not ready");
    }

    [Fact]
    public async Task Reliability_WithSnapshot_ReturnsNumbers()
    {
        // Arrange: кластерный снапшот InspectionSnapshots.Clustered (кластер demo,
        // шарды s1/s2 — см. InspectionApiTests.cs) + work-факт failover + бэкапы s1
        var now = _factory.Time.Utc;
        var t = now.ToUnixTimeSeconds();
        _factory.Snapshot = InspectionSnapshots.Clustered(now, now) with
        {
            PgWorkerWork = new List<WorkJournalInfo>
            {
                new("demo", "supervise", "supervising", "i1", t, null, null, null, null,
                    new HaSupervisionInfo("s1", "s1a", "accelerated", t - 200, t - 125, 75), null),
            },
            Backups = new List<ClusterBackupsInfo>
            {
                new("demo", null, new Dictionary<string, long?> { ["s1"] = t - 100 }),
            },
        };
        using var client = await LoginAsync();

        // Act
        using var response = await client.GetAsync("/api/reliability", TestContext.Current.CancellationToken);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);

        // Assert: 200; кластер demo/шард s1; rpo.fullAgeSec≈100; rto.lastFailover.durationSec=75
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var shard = body.GetProperty("clusters")[0].GetProperty("shards")[0];
        shard.GetProperty("shard").GetString().Should().Be("s1");
        shard.GetProperty("rpo").GetProperty("fullAgeSec").GetInt64().Should().BeInRange(99, 101);
        var failover = shard.GetProperty("rto").GetProperty("lastFailover");
        failover.GetProperty("durationSec").GetInt64().Should().Be(75);
        failover.GetProperty("cause").GetString().Should().Be("accelerated");
    }
}
```

  - **Выход:** файл тестов создан.
  - **Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.IntegrationTests -c Debug --filter "FullyQualifiedName~ReliabilityApiTests"` — FAIL (маршрута нет: 404).
  - **Связь со spec:** §3.5, AC4.

- [ ] **Шаг 8.2: реализация query/handler/мапперов и эндпоинта**

  - **Вход:** шаг 8.1 (404).
  - **Действие:**

    (a) создать `src/AdminPanel.Api/Inspection/ReliabilityQuery.cs`:

```csharp
using AdminPanel.Core;
using Shared.Core.CQRS;
using Shared.Core.DI;

namespace AdminPanel.Api.Inspection;

// Запрос грани «Надёжность» (arch/03 §1): RPO/RTO-числа per-cluster/per-shard.
public sealed record ReliabilityQuery : IQuery<ReliabilityDto>;

public sealed record ReliabilityDto(IReadOnlyList<ReliabilityClusterDto> Clusters);
public sealed record ReliabilityClusterDto(string Cluster, IReadOnlyList<ReliabilityShardDto> Shards);
public sealed record ReliabilityShardDto(string Shard, bool Declared, RpoDto? Rpo, RtoDto? Rto);
public sealed record RpoDto(
    string Mode, long? FullAgeSec, string? FullId, long? WalLagSegments,
    long? WalAgeSec, long? RpoPotentialSec, long ThresholdFullAgeSec);
public sealed record HaFactRtoDto(
    string Shard, string Node, string Cause, long DetectedUnix, long? ResolvedUnix,
    long? DurationSec, bool Ongoing, long OngoingSec);
public sealed record OpRtoDto(string State, long? DurationSec, long? OngoingSec, long? FinishedUnix, string? Error);
public sealed record RtoDto(
    HaFactRtoDto? LastFailover, HaFactRtoDto? LastRebuild, OpRtoDto? LastDrill, OpRtoDto? LastRestore);

// Core → DTO: чистые функции.
public static class ReliabilityMappers
{
    public static ReliabilityDto Map(IReadOnlyList<ClusterReliability> clusters)
        => new([.. clusters.Select(c => new ReliabilityClusterDto(
            c.Cluster,
            [.. c.Shards.Select(s => new ReliabilityShardDto(
                s.Shard, s.Declared,
                s.Rpo is { } rpo ? new RpoDto(
                    rpo.Mode.ToString().ToLowerInvariant(), rpo.FullAgeSec, rpo.FullId,
                    rpo.WalLagSegments, rpo.WalAgeSec, rpo.RpoPotentialSec, rpo.ThresholdFullAgeSec) : null,
                s.Rto is { } rto ? new RtoDto(
                    MapFact(rto.LastFailover), MapFact(rto.LastRebuild),
                    rto.LastDrill is { } d ? new OpRtoDto(d.State, d.DurationSec, d.OngoingSec, d.FinishedUnix, d.Error) : null,
                    rto.LastRestore is { } r ? new OpRtoDto(r.State, r.DurationSec, r.OngoingSec, r.FinishedUnix, null) : null)
                : null))]));

    private static HaFactRtoDto? MapFact(HaFactRto? fact)
        => fact is null ? null
            : new HaFactRtoDto(fact.Shard, fact.Node, fact.Cause, fact.DetectedUnix,
                fact.ResolvedUnix, fact.DurationSec, fact.Ongoing, fact.OngoingSec);
}

// 503 «снапшота нет»; при наличии снапшота всегда 200 (пустые секции —
// подсистема выключена/надзор старой версии — толерантность, spec §3.5).
[InjectAsScoped]
public sealed class ReliabilityQueryHandler(ISnapshotStore store, TimeProvider clock)
    : IQueryHandler<ReliabilityQuery, ReliabilityDto>
{
    public ValueTask<Result<ReliabilityDto>> Handle(ReliabilityQuery query, CancellationToken ct)
    {
        var snapshot = store.Current;
        return ValueTask.FromResult(snapshot is null
            ? Result<ReliabilityDto>.Failed(new InspectionModule.SnapshotNotReadyException())
            : Result<ReliabilityDto>.Success(
                ReliabilityMappers.Map(ReliabilityCalculator.Calculate(snapshot, clock.GetUtcNow()))));
    }
}
```

    (`TimeProvider` — сверить с DI панели; если не зарегистрирован/соседние хендлеры берут `DateTimeOffset.UtcNow` — убрать параметр и использовать `DateTimeOffset.UtcNow`, как у соседей.)

    (b) `InspectionModule.cs` — рядом с `/api/ha` добавить (маппинг ошибки — по паттерну `/api/overview`):

```csharp
        // GET /api/reliability — грань «Надёжность» (arch/03 §1): RPO/RTO-числа.
        endpoints.MapGet("/api/reliability", async (IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleQuery<ReliabilityQuery, ReliabilityDto>(new ReliabilityQuery(), ct);
            return result.IsSuccess
                ? Results.Ok(result.Value)
                : Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable,
                    title: "Snapshot not ready",
                    detail: result.Error!.Message);
        });
```

  - **Выход:** эндпоинт работает по контракту.
  - **Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.IntegrationTests -c Debug --filter "FullyQualifiedName~ReliabilityApiTests"` — PASS (3/3).
  - **Связь со spec:** §3.5.

- [ ] **Шаг 8.3: сборка и коммит**

  - **Вход:** шаг 8.2 зелёный.
  - **Действие:**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug
git add src/AdminPanel.Api/Inspection/ReliabilityQuery.cs src/AdminPanel.Api/Inspection/InspectionModule.cs src/tests/AdminPanel.IntegrationTests/ReliabilityApiTests.cs
git commit -m "feat(t17): GET /api/reliability — RPO/RTO-числа граней надёжности"
```

  - **Выход:** коммит.
  - **Проверка:** сборка 0 warnings; `git log -1 --stat` — 3 файла.
  - **Связь со spec:** §4 фаза 5 (Api).

---

### Задача 9: интеграция refresher — расширенный work-ключ в реальном etcd

**Files:**
- Modify: `src/tests/AdminPanel.IntegrationTests/EtcdSnapshotIntegrationTests.cs` (новый кейс)

**Interfaces:**
- Consumes: `EtcdContainerFixture` (реальный etcd-контейнер; свойство `Endpoint` — единственное, сид `EtcdSeed.SeedAsync` наливается в InitializeAsync фикстуры), `EtcdSeed.PutAsync(endpoint, key, value, ct)` (точечный поверх сида), `EtcdTestHarness.NewRefresher(store, fixture.Endpoint)` + `refresher.RefreshOnceAsync(ct)` (по образцу кейса `Refresher_RefreshOnce_BuildsExpectedSnapshot`), `SnapshotStore`, `WorkJournalParser`.
- Produces: интеграционное доказательство AC4 (refresher подхватывает расширенный work-ключ; `/api/reliability`-смоук — задача 8).

- [ ] **Шаг 9.1: интеграционный кейс**

  - **Вход:** коммит задачи 8; фикстура `EtcdContainerFixture` (класс `EtcdSnapshotIntegrationTests(EtcdContainerFixture fixture) : IClassFixture<EtcdContainerFixture>` — существующая форма файла).
  - **Действие:** добавить в `EtcdSnapshotIntegrationTests.cs` кейс (по образцу `Refresher_RefreshOnce_BuildsExpectedSnapshot`; Demo-сид кластера уже налит фикстурой):

```csharp
[Fact]
public async Task Refresh_WorkKeyWithHaFacts_SnapshotCarriesThem()
{
    // Arrange: реальный etcd (Demo-сид фикстуры); поверх — расширенный work-ключ
    await EtcdSeed.PutAsync(fixture.Endpoint, "/pgworker/work/shop", """
        {"op":"supervise","phase":"supervising","instance":"i1","updated_unix":1756000000,
         "last_failover":{"shard":"s1","node":"n1","cause":"accelerated","detected_unix":1000,"resolved_unix":1075,"duration_sec":75},
         "last_rebuild":{"shard":"s1","node":"n2","cause":"auto-dead","detected_unix":900}}
        """, CancellationToken.None);
    var store = new SnapshotStore();
    var refresher = EtcdTestHarness.NewRefresher(store, fixture.Endpoint);

    // Act: один тик refresher'а
    var result = await refresher.RefreshOnceAsync(CancellationToken.None);

    // Assert: снапшот несёт факты; parseError пуст
    result.IsSuccess.Should().BeTrue();
    var work = store.Current!.PgWorkerWork.Should().ContainSingle(w => w.Cluster == "shop").Subject;
    work.LastFailover!.DurationSec.Should().Be(75);
    work.LastRebuild!.Cause.Should().Be("auto-dead");
    store.Current.ParseErrors.Should().NotContain(e => e.Key == "/pgworker/work/shop");
}
```

  (имя кластера Demo-сида (`shop`/`demo`) сверить с `EtcdSeed.Demo` и держать единым с сидом фикстуры.)
  - **Выход:** кейс в файле (docker-контейнер etcd — фикстура, зачистка — её).
  - **Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.IntegrationTests -c Debug --filter "FullyQualifiedName~EtcdSnapshotIntegrationTests"` — PASS (весь класс).
  - **Связь со spec:** AC4 (интеграция refresher).

- [ ] **Шаг 9.2: коммит**

  - **Вход:** шаг 9.1 зелёный.
  - **Действие:**

```bash
git add src/tests/AdminPanel.IntegrationTests/
git commit -m "test(t17): refresher подхватывает расширенный work-ключ с HA-фактами (реальный etcd)"
```

  - **Выход:** коммит.
  - **Проверка:** `git log -1 --stat`.
  - **Связь со spec:** §4 фаза 5.

---

### Задача 10: frontend — API-типы, страница «Надёжность», роут/навигация, карточка Overview

**Files:**
- Modify: `frontend/src/api/dto.ts` (типы Reliability)
- Modify: `frontend/src/api/queries.ts` (`reliabilityQueryKeys`, `fetchReliability`)
- Create: `frontend/src/pages/ReliabilityPage.tsx`
- Modify: `frontend/src/App.tsx` (роут `reliability`)
- Modify: `frontend/src/layout/AppLayout.tsx` (пункт навигации между HA и Алертами)
- Modify: `frontend/src/pages/OverviewPage.tsx` (карточка «Надёжность»)

**Interfaces:**
- Consumes: `GET /api/reliability` (задача 8), `usePollingIntervalMs` (`frontend/src/polling/PollingContext.tsx`), `apiFetch` (`frontend/src/api/client.ts`), паттерны `BackupsStoragePage.tsx`/`OverviewPage.tsx` (в т.ч. Link-переход на `/backups-storage/:cluster/:shard`).
- Produces: страница `/reliability` (таблица кластер×шард: RPO-колонки + RTO-колонки, ongoing-бейдж «идёт»; клик по id полного — переход к деталям шарда грани бэкапов), пункт навигации, карточка Overview.

- [ ] **Шаг 10.1: типы и fetch**

  - **Вход:** коммиты задач 1–9.
  - **Действие:** в `dto.ts` (в конец, стилем файла):

```typescript
// GET /api/reliability — грань «Надёжность» (RPO/RTO-числа).
export type RpoMode = 'wal' | 'full' | 'off';

export interface RpoDto {
  mode: RpoMode;
  fullAgeSec?: number | null;
  fullId?: string | null;
  walLagSegments?: number | null;
  walAgeSec?: number | null;
  rpoPotentialSec?: number | null;
  thresholdFullAgeSec: number;
}

export interface HaFactRtoDto {
  shard: string;
  node: string;
  cause: string;
  detectedUnix: number;
  resolvedUnix?: number | null;
  durationSec?: number | null;
  ongoing: boolean;
  ongoingSec: number;
}

export interface OpRtoDto {
  state: string;
  durationSec?: number | null;
  ongoingSec?: number | null;
  finishedUnix?: number | null;
  error?: string | null;
}

export interface RtoDto {
  lastFailover?: HaFactRtoDto | null;
  lastRebuild?: HaFactRtoDto | null;
  lastDrill?: OpRtoDto | null;
  lastRestore?: OpRtoDto | null;
}

export interface ReliabilityShardDto {
  shard: string;
  declared: boolean;
  rpo?: RpoDto | null;
  rto?: RtoDto | null;
}

export interface ReliabilityClusterDto {
  cluster: string;
  shards: ReliabilityShardDto[];
}

export interface ReliabilityDto {
  clusters: ReliabilityClusterDto[];
}
```

    в `queries.ts` (рядом с ha-запросами; импорт `ReliabilityDto` в шапку):

```typescript
export const reliabilityQueryKeys = {
  all: ['reliability'] as const,
};

export function fetchReliability(): Promise<ReliabilityDto> {
  return apiFetch<ReliabilityDto>('/api/reliability');
}
```

  - **Выход:** типы/fetch на месте.
  - **Проверка:** `cd frontend && npx tsc --noEmit -p tsconfig.app.json` — 0 ошибок.
  - **Связь со spec:** §3.6.

- [ ] **Шаг 10.2: страница ReliabilityPage**

  - **Вход:** шаг 10.1.
  - **Действие:** создать `frontend/src/pages/ReliabilityPage.tsx` (по образцу `BackupsStoragePage.tsx` — `useQuery` + `refetchInterval: intervalMs`, `Table`, `Badge`, `Link` (react-router) для перехода по id полного, цвета Mantine):

```tsx
import { useQuery } from '@tanstack/react-query';
import { Link } from 'react-router';
import { Badge, Card, Group, Table, Text } from '@mantine/core';
import { reliabilityQueryKeys, fetchReliability } from '../api/queries';
import { usePollingIntervalMs } from '../polling/PollingContext';
import type { HaFactRtoDto, OpRtoDto, ReliabilityClusterDto, RpoDto } from '../api/dto';

// Грань «Надёжность» (read-only): таблица кластер×шард — RPO-блок + RTO-блок;
// ongoing — бейдж «идёт» с тикающей длительностью (пересчёт каждым тиком
// снапшота); клик по id полного — детали шарда грани бэкапов; форм ввода нет.
export function ReliabilityPage() {
  const intervalMs = usePollingIntervalMs();
  const query = useQuery({
    queryKey: reliabilityQueryKeys.all,
    queryFn: fetchReliability,
    refetchInterval: intervalMs,
  });
  if (query.isPending) return <Text>Загрузка…</Text>;
  if (query.isError) return <Text c="red">Ошибка: {(query.error as Error).message}</Text>;
  return (
    <>
      {query.data.clusters.map((c) => (
        <ClusterCard key={c.cluster} cluster={c} />
      ))}
    </>
  );
}

function ClusterCard({ cluster }: { cluster: ReliabilityClusterDto }) {
  return (
    <Card withBorder padding="md" radius="md" mb="md">
      <Group justify="space-between" mb="xs">
        <Text fw={600}>Кластер {cluster.cluster}</Text>
      </Group>
      <Table striped highlightOnHover>
        <Table.Thead>
          <Table.Tr>
            <Table.Th>Шард</Table.Th>
            <Table.Th>Полный (возраст)</Table.Th>
            <Table.Th>WAL лаг</Table.Th>
            <Table.Th>WAL возраст</Table.Th>
            <Table.Th>Потеряем ≈</Table.Th>
            <Table.Th>Failover</Table.Th>
            <Table.Th>Rebuild</Table.Th>
            <Table.Th>Drill</Table.Th>
            <Table.Th>Restore</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {cluster.shards.map((s) => (
            <Table.Tr key={s.shard}>
              <Table.Td>{s.shard}</Table.Td>
              <RpoCells cluster={cluster.cluster} shard={s.shard} rpo={s.rpo} />
              <FactCell fact={s.rto?.lastFailover} />
              <FactCell fact={s.rto?.lastRebuild} />
              <OpCell op={s.rto?.lastDrill} />
              <OpCell op={s.rto?.lastRestore} />
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>
    </Card>
  );
}
```

    и локальные компоненты этого же файла:
    - `RpoCells({ cluster, shard, rpo })`: возраст полного `fullAgeSec` с индикацией (`> thresholdFullAgeSec` → красный, свежий → зелёный, `null` → «никогда»); **`fullId` — КЛИК-ссылка** на детали шарда грани бэкапов (решение по spec §3.4 «клик — переход к деталям грани бэкапов»; образец — Link-переходы BackupsStoragePage):

```tsx
              <Link to={`/backups-storage/${encodeURIComponent(cluster)}/${encodeURIComponent(shard)}`}>
                {rpo.fullId}
              </Link>
```

    («Потеряем ≈» — `rpoPotentialSec` + `Badge` источника (`wal`/`full`; `off` → «—»); `walLagSegments`/`walAgeSec` — числа или «—»;)
    - `FactCell({ fact }: { fact?: HaFactRtoDto | null })`: `ongoing` → `Badge «идёт Nс»` (N = `ongoingSec`); закрытый → `durationSec` + `cause` + ago от `resolvedUnix`; нет факта → «—»;
    - `OpCell({ op }: { op?: OpRtoDto | null })`: терминальный → `state` + `durationSec` (drill при `error` — красным); ongoing (`ongoingSec` есть) → бейдж «идёт Nс»; нет → «—»;
    - форматирование секунд — существующий utils `frontend/src/utils/` (сверить наличие форматтера длительностей; нет — локальная `formatSec`).

  - **Выход:** страница-компонент готова (fullId — переход на `/backups-storage/:cluster/:shard`).
  - **Проверка:** `npx tsc --noEmit -p tsconfig.app.json` — 0 ошибок.
  - **Связь со spec:** §3.6 (таблица, бейджи, пороговая индикация; fullId — клик к деталям бэкапов, §3.4).

- [ ] **Шаг 10.3: роут и навигация**

  - **Вход:** шаг 10.2.
  - **Действие:** `App.tsx` — импорт `ReliabilityPage` и строка в children между `ha/:scope` и `alerts`:

```tsx
      { path: 'reliability', element: <ReliabilityPage /> },
```

    `AppLayout.tsx` — в массиве пунктов после `{ to: '/ha', label: 'HA' }`:

```tsx
  { to: '/reliability', label: 'Надёжность' },
```

  - **Выход:** роут/навигация на месте (между HA и Алертами).
  - **Проверка:** `npx tsc --noEmit -p tsconfig.app.json` — 0 ошибок.
  - **Связь со spec:** §3.6 (роут+навигация между HA и Алертами).

- [ ] **Шаг 10.4: карточка Overview**

  - **Вход:** шаги 10.1–10.3.
  - **Действие:** в `OverviewPage.tsx` — запрос (по образцу соседних клиентских агрегаций):

```tsx
  const reliability = useQuery({
    queryKey: reliabilityQueryKeys.all,
    queryFn: fetchReliability,
    refetchInterval: intervalMs,
  });
```

    в сетку карточек после `<BackupsCard .../>` — `<ReliabilityCard data={reliability.data} activeClusters={...} loading={...} error={...} onRetry={...} />` (активные кластера — из сводки `OverviewDto.Clusters` страницы; фактическое поле имени сверить по `OverviewQuery.cs`/`dto.ts`).

    `ReliabilityCard` (в том же файле, стилем соседних карточек):
    - worst `rpoPotentialSec` = max по шардам `mode==='wal'|'full'` активных кластеров;
    - счётчик шардов `mode==='full'` («RPO держится только полным») и `mode==='off'`;
    - «последний failover установки»: среди всех `lastFailover` с `durationSec != null` — факт с МАКСИМАЛЬНЫМ `resolvedUnix` (fallback `detectedUnix`, если `resolvedUnix` нет) — самый НЕдавний, а не самый длинный; показываем его `durationSec` и ago от `resolvedUnix`;
    - загрузка/ошибка — заглушки как у соседних карточек.
  - **Выход:** карточка на Overview.
  - **Проверка:** `npx tsc --noEmit -p tsconfig.app.json` — 0 ошибок.
  - **Связь со spec:** §3.6 (карточка: worst RPO, счётчики full/off, последний failover).

- [ ] **Шаг 10.5: typecheck + build + коммит**

  - **Вход:** шаги 10.1–10.4.
  - **Действие:**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t17-rpo-rto-dashboard/frontend && npm run typecheck && npm run build
cd .. && git add frontend/src/api/dto.ts frontend/src/api/queries.ts frontend/src/pages/ReliabilityPage.tsx frontend/src/App.tsx frontend/src/layout/AppLayout.tsx frontend/src/pages/OverviewPage.tsx
git commit -m "feat(t17): страница «Надёжность» (/reliability) + карточка Overview"
```

  - **Выход:** коммит.
  - **Проверка:** `npm run build` (tsc + vite) — без ошибок; `git log -1 --stat` — 6 файлов.
  - **Связь со spec:** §4 фаза 6.

---

### Задача 11: приёмка — серии прогонов, E2E-маркер, стенд ИЗ WORKTREE

**Files:** (прогоны; фиксы по результатам — отдельными коммитами)

- [ ] **Шаг 11.1: Release-сборка всего решения**

  - **Вход:** все задачи 1–10 закоммичены.
  - **Действие:** `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release` (из корня worktree).
  - **Выход:** сборка прошла.
  - **Проверка:** вывод `0 Warning(s) / 0 Error(s)`.
  - **Связь со spec:** AC1.

- [ ] **Шаг 11.2: юнит-серии с зачисткой между сериями**

  - **Вход:** шаг 11.1.
  - **Действие:** прогонять серии ПО ОДНОЙ; после финальной строки каждой серии — зачистка остаточных контейнеров/сетей (`docker network prune -f`; осиротевшие `pgw-*`/`kfw-net-*` — по AGENTS.md); следующая серия — только после зачистки:

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/Shared.Etcd.UnitTests -c Debug
docker network prune -f
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/Shared.Metrics.UnitTests -c Debug
docker network prune -f
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug
docker network prune -f
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug
docker network prune -f
```

  - **Выход:** все юнит-серии прогнаны.
  - **Проверка:** каждая серия зелёная (0 failed); `docker network ls | grep -c 'kfw-net'` → 0 при нулевых контейнерах.
  - **Связь со spec:** AC1, AC2, AC3, AC4 (юнит-часть).

- [ ] **Шаг 11.3: интеграционная серия панели с зачисткой**

  - **Вход:** шаг 11.2 (юниты зелёные, зачищено).
  - **Действие:**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.IntegrationTests -c Debug
docker ps -a --format '{{.Names}}' | grep -E 'pgw-|adminpanel|etcd' | xargs -r docker rm -f
docker network prune -f
```

  - **Выход:** интеграционная серия панели прогнана (включая `ReliabilityApiTests`, `EtcdSnapshotIntegrationTests`).
  - **Проверка:** серия зелёная; после зачистки — остаточных контейнеров/сетей серии нет.
  - **Связь со spec:** AC4 (интеграция), AC1 (зачистка).

- [ ] **Шаг 11.4: E2E-маркер мерж-гейта на свежем Release**

  - **Вход:** шаги 11.1–11.3; зачищенное окружение.
  - **Действие:**

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard
```

    (из корня worktree; при долгом старте — в фоне с task_id; после ФИНАЛЬНОЙ строки серии — зачистка: `docker ps -a -q --filter name=pgw- | xargs -r docker rm -f; docker network prune -f`.)
  - **Выход:** E2E-маркер прогнан на свежем Release (E2eFixture собирает Release сам — инкрементальный no-op).
  - **Проверка:** PASS (кейс `Scale_AddEmptyShard_BlockedRemoveThenAutoDismantle_NameReused`, `src/tests/PgWorker.IntegrationTests/E2e/E2eScaleScenarios.cs`).
  - **Связь со spec:** AC6, R8.

- [ ] **Шаг 11.5: стенд full-профиль ИЗ WORKTREE — ручной чек UI**

  - **Вход:** шаги 11.1–11.4; код t17 существует ТОЛЬКО в worktree — стенд основного репо (`/Users/demakaev/ZCodeProject/pg`) его не содержит (скрипт `00-up.sh` собирает образы из СВОЕГО чекаута), поэтому чек ведётся стендом worktree.
  - **Действие:**
    1. Убедиться, что стенд основного репо НЕ запущен (общий порт 5050): `curl -s -o /dev/null -w '%{http_code}' http://localhost:5050` или `docker ps --format '{{.Names}} {{.Ports}}' | grep 5050`; если запущен — остановить его (средствами основного репо: скрипт остановки dev-stand / `docker compose down` контура adminpanel) и зафиксировать, что остановили (после чека НЕ поднимать обратно без запроса пользователя).
    2. Поднять стенд ИЗ WORKTREE: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t17-rpo-rto-dashboard && dev-stand/adminpanel/checks/00-up.sh` (full-профиль; образы `pgworker:dev`/`adminpanel:dev` соберутся из чекаута worktree — с кодом t17).
    3. В браузере `http://localhost:5050`: (a) навигация содержит «Надёжность» между HA и Алертами; (b) `/reliability` рендерится: таблицы кластеров стенда, RPO-колонки с числами (или «—» при выключенных подсистемах), пороговая индикация по `thresholdFullAgeSec`, клик по id полного ведёт на `/backups-storage/<cluster>/<shard>`; (c) карточка «Надёжность» на Overview (worst RPO-потенциал, счётчики full/off, последний failover); (d) ongoing-тик: снести контейнер ноды `docker rm -f pgw-<C>-<X>-<n>` → бейдж «идёт» растёт между опросами; после восстановления — закрытый факт с duration; (e) `/metrics` PgWorker: `docker exec <pgworker-контейнер> wget -qO- localhost:8080/metrics | grep pgworker_ha_` — обе серии после первого факта.
    4. После чека — ОСТАНОВИТЬ стенд worktree (скрипт остановки/`docker compose down` профилей 00-up), зачистить остаточные `pgw-*`-контейнеры и сети (`docker network prune -f`).
  - **Выход:** приёмочный чек UI выполнен на живом стенде с кодом t17.
  - **Проверка:** все пункты (a)–(e) подтверждены; стенд остановлен, остатков нет.
  - **Связь со spec:** AC5 (страница рендерится с числами живого стенда, ongoing тикает, карточка, форм ввода нет; fullId-переход), AC3 (/metrics).

- [ ] **Шаг 11.6: фиксы по результатам + финальный коммит (если были правки)**

  - **Вход:** шаги 11.1–11.5; зафиксированные проблемы.
  - **Действие:** точечные фиксы + `git add -A && git commit -m "fix(t17): приёмка — правки по итогам прогонов"`; повторить затронутые прогоны (принцип: правка → повтор серии шага, где найдено).
  - **Выход:** приёмка закрыта.
  - **Проверка:** `git status --short` — чисто; все серии зелёные.
  - **Связь со spec:** §6 (критерии приёмки).

---

### Задача 12: мерж-гейт roadmap (выполняется МЕРЖ-КОММИТОМ в main — не в feature-ветке)

**Files (правки мерж-коммитом, тем же коммитом, что и мерж):**
- Modify: `arch/roadmap/reliability.md`
- Modify: `arch/roadmap/reliability-report.md`
- Modify (при необходимости): `docs/adminpanel/02-etcd-snapshot.md`

- [ ] **Шаг 12.1: снять тег t17 из reliability.md**

  - **Вход:** решение о мерже (явная просьба пользователя); ветка прошла гейты.
  - **Действие:** удалить пункт `**\`t17-rpo-rto-dashboard\`** — RPO/RTO-числа…` из списка; удалить все `← t17-rpo-rto-dashboard` из зависимостей других пунктов.
  - **Выход:** тега t17 в файле нет.
  - **Проверка:** `grep -n "t17" arch/roadmap/reliability.md` — пусто.
  - **Связь со spec:** AC7.

- [ ] **Шаг 12.2: reliability-report.md — перенос в «Сделано», сводка N**

  - **Вход:** шаг 12.1.
  - **Действие:** (a) в §N убрать `RPO/RTO-чисел оператору нет (\`t17\`)` из открытых разрывов; (b) в таблице «Осталось (открытые задачи трека)» удалить строку `t17-rpo-rto-dashboard`; (c) в таблицу «Сделано в рамках трека» добавить строку:

```
| `t17-rpo-rto-dashboard` | — (мерж-коммит t17-rpo-rto-dashboard) | RPO/RTO-числа оператору (характеристика N): возраст валидного полного + лаг WAL + сводный RPO-потенциал и длительности последнего failover/rebuild/drill/restore per-shard на странице «Надёжность» + карточка Overview (API /api/reliability, чистая функция над etcd-снапшотом); факты HA-надзора в work-ключе (last_failover/last_rebuild — последний факт вида, переживают takeover); Prometheus-серии pgworker_ha_failover/rebuild_duration_seconds + Grafana-панель |
```

    (d) обновить сводку характеристики N (абзац после `### N`) с учётом появления чисел.
  - **Выход:** отчёт консистентен (открытых t17-упоминаний нет, «Сделано» пополнено).
  - **Проверка:** `grep -n "t17" arch/roadmap/reliability-report.md` — только строка «Сделано».
  - **Связь со spec:** AC7.

- [ ] **Шаг 12.3: docs-практики при необходимости**

  - **Вход:** шаги 12.1–12.2.
  - **Действие:** проверить `docs/adminpanel/02-etcd-snapshot.md`: если документ перечисляет поля work-ключа — добавить факт-поля (правила INDEX документа); не перечисляет — не трогать.
  - **Выход:** документация снапшота актуальна.
  - **Проверка:** визуальная.
  - **Связь со spec:** §4 фаза 7.

- [ ] **Шаг 12.4: проверка чистоты (без исторических атрибуций)**

  - **Вход:** шаги 12.1–12.3.
  - **Действие:** `grep -rn "t17" arch/ docs/ | grep -v superpowers` — разобрать каждое попадание: упоминаний слитой задачи быть не должно (кроме roadmap-«Сделано», где тег — идентификатор записи).
  - **Выход:** arch/docs описывают только текущее состояние.
  - **Проверка:** grep-вывод пуст (или только «Сделано»-строка report).
  - **Связь со spec:** AC8, правило AGENTS.md.

---

## Self-review (выполнен автором плана; обновлён по итогам 3-го внешнего ревью)

1. **Покрытие спеки:** §3.1 → задачи 1 (шаги 1.1–1.2) + 2 + 4; §3.2 → задачи 2–4; §3.3 → задача 1 (шаг 1.3) + задача 5; §3.4 → задачи 6–7 (вкл. клик по fullId — задача 10.2); §3.5 → задача 8; §3.6 → задача 10; §4 фазы 1–7 → задачи 1–12 (arch → воркер → метрики → панель Core/Etcd → Api → frontend → приёмка → roadmap-гейт); AC1→11.1–11.3; AC2→3–4 (включая TO_REMOVE/QUARANTINED/REMOVING/NoCandidate-кейсы 4.1); AC3→5; AC4→6–9; AC5→10 + 11.5; AC6→11.4; AC7→12; AC8→12.4. Риски: R1 (carry-forward 2/4), R2 (takeover 3/4), R3 (флап 3/4; предупреждение против ослабления — 4.1), R4 (только закрытие 3), R5 (0 доп. etcd-операций 2), R7 (nullable-поля + фиксация решения об optional-параметре в Interfaces задачи 2), R8 (E2E-маркер 11.4), R9 (перезапись набора 5), R10 (фильтр по декларации 7).
2. **Placeholder-scan:** «TBD»/«реализуй подобно» нет; пометки «сверить с фактическим кодом» стоят только у внешних сигнатур и снабжены точным источником для сверки.
3. **Консистентность типов:** `HaSupervisionFact` (воркер, snake_case) ↔ `HaSupervisionInfo` (панель) — осознанный дубль спеки §3.4; делегат `haDurations` (задача 4, Interfaces/(a)) сигнатурно совпадает с марк-методом `HaDurations` (задача 5); подключение в Program.cs — ДОБАВЛЕНИЕ одного аргумента после существующего `EtcdEndpoints` (без дубля); `FailoverAcceleration` используется единообразно в 4.2(d)/(e).
4. **Правки по 1-му ревью (сохранены):** стенд из worktree с гасанием чужого стенда на :5050; условие ускорения бит-в-бит, `!shard.ToRemove` только вокруг фактов; юниты TO_REMOVE/QUARANTINED; «последний failover» по `resolvedUnix`; Program.cs без дубля аргумента; optional `facts` с мотивировкой; болванка `EtcdSnapshot` — 14 аргументов.
5. **Правки по 2-му ревью (сохранены):** TO_REMOVE-тест сидирован мёртвым по пробе лидером; NoCandidate → elections через `FailoverAcceleration` (с мотивировкой отказа от ternary по bool); сид `DrillInfo` — 10 аргументов, `State="SUCCEEDED"`; порог из `Policy.FullMaxAgeSec` (тест с Policy-сидом); фактические имена хелперов (локальный LoginAsync, `fixture.Endpoint`, `EtcdSeed.PutAsync`, `RefreshOnceAsync`).
6. **Правки по 3-му ревью:** (1) тест `Tick_DeadLeaderContainer_NoLiveCandidate_OpensElectionsFailover` пересидирован — ВСЕ ноды шарда (включая лидера) мертвы по Patroni-пробам (`port == 18000 || 18001 || 18002 → Down`), иначе alive-ветка того же тика делала флап-сброс факта (LeaderRecovered, R3); добавлен ассерт `ResolvedUnix == null` и ЯВНОЕ ПРЕДУПРЕЖДЕНИЕ: падение этого теста НЕ чинить ослаблением флап-сброса — чинить сид, семантика флапа канонична (4.1); (2) тест `Tick_AdoptedNode_DeadBySqlProbe_NoRebuildFact` — инжекция падающей SQL-пробы `sql: new Fakes.FakeSql { ScalarResultByDsn = _ => Result<object?>.Failed(...) }` (иначе FakeSql молчит Success, нода «жива», тест проходил вхолостую) + ассерт LastFailover null (4.1); (3) fullId — РЕШЕНИЕ: КЛИК-переход (`Link` на `/backups-storage/:cluster/:shard`) по spec §3.4, `RpoCells` принимает cluster/shard, arch-текст шага 1.5 дополнен, чек-пункт 11.5(b) включает переход (10.2); (4) кейс карантина расширен до `Tick_QuarantinedAndRemovingDeadNodes_NoFacts` — вторая нода в REMOVING, оба state не перезаписаны (гвард проб один — `Quarantined or Removing` — покрыт обоими значениями, 4.1).
