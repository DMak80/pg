# t04-etcd-api — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** API инспекции etcd из снапшота (`/api/overview`, `/api/etcd/status`, `/api/alerts`) и каркас `AlertEngine` с 7 etcd-правилами каталога алертов.

**Architecture:** Каркас правил в `AdminPanel.Core/Alerting` (`IAlertRule` → `AlertEngine`: стабильные id `kind:target`, `sinceUnix` из прошлого снапшота, сортировка), вычисляется `SnapshotRefresher` на каждом тике (успешном и отказном). API — Minimal API-модуль `AdminPanel.Api/Inspection`: тонкие эндпоинты → `IHandler.HandleQuery` → `IQueryHandler` читает `ISnapshotStore` → статические мапперы снапшот→DTO. Тесты: unit без хоста, integration в существующей коллекции `"api"` (один Program-хост на процесс) с подменённым `ISnapshotStore`.

**Tech Stack:** .NET 10, C# latest, ASP.NET Core Minimal API, attribute-DI (`[InjectAs*]`), `Result`-монада, xunit v3 + FluentAssertions, Testcontainers (etcd `quay.io/coreos/etcd:v3.5.21`).

**Spec:** `docs/superpowers/2026-08-22-t04-etcd-api/spec.md` — план аргументируется от спеки; исполнители читают обе. Ссылки «spec §N» ниже — на её разделы.

## Global Constraints

- Все пути — от корня worktree `/Users/demakaev/ZCodeProject/worktrees/feat-t04-etcd-api`; команды `dotnet` — из него.
- .NET 10, `Nullable=enable`, `TreatWarningsAsErrors=true` — 0 warnings, иначе build падает: usings только реально используемые.
- Идентификаторы английские; комментарии в коде русские; тексты `message` алертов русские (spec §2).
- Тесты: xunit v3 + FluentAssertions, AAA-комментарии (`// Arrange` / `// Act` / `// Assert`), на русском.
- Новых NuGet-пакетов нет; `Directory.Packages.props` не менять (spec §12).
- Не вызывать `UseDiBehaviours`/`AutoRegistration` в тестах — статический кеш сборок ломает Program-хост фабрики (spec §3.16, прецедент t03 §3.15).
- В unit-тестах сервисы конструировать `new` + `Options.Create` (без хоста).
- `HandleQuery` — всегда с явными generic-аргументами: `handler.HandleQuery<Query, Dto>(...)` (прецедент `MeQuery`).
- В хендлерах `ValueTask` строить через `ValueTask.FromResult(...)` с тернарником двух веток `Result<T>` (прецедент §6.2 spec).
- Мутации `arch/01–04` запрещены; из roadmap меняется только `arch/roadmap/etcd.md` (Task 6).
- Каждый таск завершается коммитом (сообщения — по прецеденту t03, префикс `t04:`).
- Ожидаемые счётчики тестов: до начала — 83 unit / 19 integration кейсов; после всех задач — 120 unit / 32 integration / 152 total.

---

### Task 1: Каркас AlertEngine (Core) + тест-фикстуры снапшотов

**Files:**
- Create: `src/AdminPanel.Core/Alerting/IAlertEngine.cs`
- Create: `src/AdminPanel.Core/Alerting/AlertEngine.cs`
- Create: `src/tests/AdminPanel.UnitTests/TestSnapshots.cs`
- Create: `src/tests/AdminPanel.UnitTests/AlertEngineTests.cs`

**Interfaces:**
- Consumes: `EtcdSnapshot`, `Alert`, `AlertSeverity` из `AdminPanel.Core` (t03); `Result` не используется (чистая функция).
- Produces (для t05/t06 и следующих задач):
  - `namespace AdminPanel.Core.Alerting`: `interface IAlertRule { string Kind { get; } IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context); }`, `record AlertContext(EtcdSnapshot? Previous, DateTimeOffset NowUtc, double RefreshIntervalSeconds)`, `interface IAlertEngine { IReadOnlyList<Alert> Evaluate(EtcdSnapshot snapshot, EtcdSnapshot? previous, DateTimeOffset nowUtc, double refreshIntervalSeconds); }`, `class AlertEngine(IEnumerable<IAlertRule> rules) : IAlertEngine` (`[InjectAsSingleton(typeof(IAlertEngine))]`).
  - В тестах: `TestSnapshots.Healthy(DateTimeOffset builtAt)`, `TestSnapshots.HealthyEtcd(DateTimeOffset at, int alive = 3, int total = 3)`, `TestSnapshots.FullCluster()`, `TestSnapshots.GhostCluster()`.

- [ ] **Шаг 1: Фейковые правила в тесте каркаса (failing test)**

  Вход: `src/tests/AdminPanel.UnitTests/` — проект ссылается на Core (готово).

  Действие: создать `src/tests/AdminPanel.UnitTests/TestSnapshots.cs`:

  ```csharp
  using AdminPanel.Core;

  namespace AdminPanel.UnitTests;

  // Сборка EtcdSnapshot-фикстур для тестов алертов/мапперов/хендлеров (spec §10):
  // healthy-базис и модификации through with.
  internal static class TestSnapshots
  {
      // Здоровый снапшот: 3 живых endpoints, полный кластер demo, без алертов/ошибок.
      public static EtcdSnapshot Healthy(DateTimeOffset builtAt) => new(
          builtAt,
          HealthyEtcd(builtAt),
          [FullCluster()],
          [],
          [],
          [],
          [],
          [],
          0);

      // Все endpoints живые; alive < total — хвост мёртвый с ошибкой транспорта.
      public static EtcdStatus HealthyEtcd(DateTimeOffset at, int alive = 3, int total = 3) => new(
          alive > 0,
          [.. Enumerable.Range(0, total).Select(i => new EtcdEndpoint(
              $"http://etcd{i + 1}:2379",
              i < alive,
              i < alive ? 3 + i : null,
              i < alive ? "3.5.21" : null,
              i < alive ? 20480 : null,
              i < alive ? 42 : null,
              i < alive ? 17 : null,
              i < alive ? 3 : null,
              i < alive ? [] : ["connection refused"]))],
          [new EtcdMember(42, "etcd1", ["http://etcd1:2380"], ["http://etcd1:2379"])],
          [],
          "http://etcd1:2379",
          false,
          at,
          0);

      // Полный кластер (config есть): Incomplete = false.
      public static ClusterInfo FullCluster() => new(
          "demo", "demo", 16, 1755800000,
          [new ShardInfo(
              "s1", "host=s1a,s1b port=5432 dbname=demo user=postgres",
              ["s1a", "s1b"], 5432, "demo", "postgres", 1, "s1a:5432", null)],
          [.. Enumerable.Range(0, 16).Select(i => new BucketInfo(i, "s1", BucketState.Active, null))],
          []);

      // Кластер без config-ключа: Incomplete = true (t03 §3.6).
      public static ClusterInfo GhostCluster() => new("ghost", null, 0, null, [], [], []);
  }
  ```

  Создать `src/tests/AdminPanel.UnitTests/AlertEngineTests.cs` (пока только каркасные тесты; правила добавит Task 2 в этот же файл):

  ```csharp
  using AdminPanel.Core;
  using AdminPanel.Core.Alerting;
  using FluentAssertions;
  using Xunit;

  namespace AdminPanel.UnitTests;

  // Каркас AlertEngine: сбор правил, сортировка, механика sinceUnix (spec §4.1, §3.4).
  public class AlertEngineTests
  {
      private static readonly DateTimeOffset BuiltAt = new(2026, 8, 22, 12, 0, 0, TimeSpan.Zero);

      // Фейк-правило: всегда возвращает заданный алерт (каркас-тесты без реальных правил).
      private sealed class ConstRule(string kind, Alert alert) : IAlertRule
      {
          public string Kind => kind;

          public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context) => [alert];
      }

      private static Alert Make(
          string id, AlertSeverity severity, string kind, string target, long? sinceUnix = null)
          => new(id, severity, kind, target, "message", null, sinceUnix);

      [Fact]
      public void Evaluate_NoRules_EmptyList()
      {
          // Arrange
          var engine = new AlertEngine([]);

          // Act
          var alerts = engine.Evaluate(TestSnapshots.Healthy(BuiltAt), null, BuiltAt, 3);

          // Assert
          alerts.Should().BeEmpty();
      }

      [Fact]
      public void Evaluate_CollectsAllRules()
      {
          // Arrange
          var engine = new AlertEngine(
          [
              new ConstRule("kind-a", Make("kind-a:t", AlertSeverity.Warning, "kind-a", "t")),
              new ConstRule("kind-b", Make("kind-b:t", AlertSeverity.Critical, "kind-b", "t")),
          ]);

          // Act
          var alerts = engine.Evaluate(TestSnapshots.Healthy(BuiltAt), null, BuiltAt, 3);

          // Assert
          alerts.Should().HaveCount(2);
      }

      [Fact]
      public void Evaluate_Sorts_SeverityDescThenKindThenTarget()
      {
          // Arrange: critical всегда первый; внутри уровня — kind, затем target (Ordinal).
          var engine = new AlertEngine(
          [
              new ConstRule("k1", Make("k1:x", AlertSeverity.Warning, "k1", "x")),
              new ConstRule("k2", Make("k2:z", AlertSeverity.Critical, "k2", "z")),
              new ConstRule("k3", Make("k3:y", AlertSeverity.Warning, "k3", "y")),
              new ConstRule("k4", Make("k4:x", AlertSeverity.Warning, "k4", "x")),
          ]);

          // Act
          var alerts = engine.Evaluate(TestSnapshots.Healthy(BuiltAt), null, BuiltAt, 3);

          // Assert: critical первым; внутри уровня — по kind (Ordinal): k1 < k3 < k4.
          string.Join("|", alerts.Select(a => a.Id))
              .Should().Be("k2:z|k1:x|k3:y|k4:x");
      }

      [Fact]
      public void SinceUnix_CarriedFromPrevious()
      {
          // Arrange: id был в previous со since=1000 — переносится без изменений (spec §3.4).
          var engine = new AlertEngine([new ConstRule("k", Make("k:t", AlertSeverity.Warning, "k", "t"))]);
          var previous = TestSnapshots.Healthy(BuiltAt) with
          {
              Alerts = [Make("k:t", AlertSeverity.Warning, "k", "t", sinceUnix: 1000)],
          };

          // Act
          var alerts = engine.Evaluate(TestSnapshots.Healthy(BuiltAt + TimeSpan.FromSeconds(3)), previous, BuiltAt + TimeSpan.FromSeconds(3), 3);

          // Assert
          alerts.Single().SinceUnix.Should().Be(1000);
      }

      [Fact]
      public void SinceUnix_NewAlert_GetsCurrentUnix()
      {
          // Arrange: id в previous отсутствовал — since = unix времени оценки.
          var now = BuiltAt + TimeSpan.FromSeconds(5);
          var engine = new AlertEngine([new ConstRule("k", Make("k:t", AlertSeverity.Warning, "k", "t"))]);
          var previous = TestSnapshots.Healthy(BuiltAt) with { Alerts = [] };

          // Act
          var alerts = engine.Evaluate(TestSnapshots.Healthy(now), previous, now, 3);

          // Assert
          alerts.Single().SinceUnix.Should().Be(now.ToUnixTimeSeconds());
      }

      [Fact]
      public void SinceUnix_NullOnFirstTick()
      {
          // Arrange: previous нет (первый тик) — время появления неизвестно (spec §3.4).
          var engine = new AlertEngine([new ConstRule("k", Make("k:t", AlertSeverity.Warning, "k", "t"))]);

          // Act
          var alerts = engine.Evaluate(TestSnapshots.Healthy(BuiltAt), null, BuiltAt, 3);

          // Assert
          alerts.Single().SinceUnix.Should().BeNull();
      }
  }
  ```

  Выход: два новых тест-файла; прод не менялся.

  Проверка: `dotnet build src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj 2>&1 | tail -5` → **FAIL компиляции**: `CS0246: IAlertRule/AlertContext/AlertEngine not found` — ожидаемо (типов ещё нет). Это красная фаза TDD.

- [ ] **Шаг 2: Контракты каркаса**

  Вход: красная фаза шага 1.

  Действие: создать `src/AdminPanel.Core/Alerting/IAlertEngine.cs`:

  ```csharp
  namespace AdminPanel.Core.Alerting;

  // Правило каталога алертов (arch/03 §4): один kind, чистая оценка снапшота.
  // Каркас: t05/t06 добавляют правила новыми классами без правки AlertEngine (spec §3.2).
  public interface IAlertRule
  {
      // Kind каталога, напр. "etcd-unreachable" (arch/03 §4).
      string Kind { get; }

      // Алерты правила по текущему снапшоту (0..N; SinceUnix проставляет AlertEngine).
      IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context);
  }

  // Параметры оценки вне снапшота: прошлый снапшот (sinceUnix), текущее время и период тика
  // (порог snapshot-stale). Core не знает настроек — направление зависимостей arch/01 §1 (spec §3.3).
  public sealed record AlertContext(
      EtcdSnapshot? Previous,
      DateTimeOffset NowUtc,
      double RefreshIntervalSeconds);

  // Чистая функция Snapshot → Alert[] (arch/01 §2): правила + общая механика (spec §4.1).
  public interface IAlertEngine
  {
      IReadOnlyList<Alert> Evaluate(
          EtcdSnapshot snapshot,
          EtcdSnapshot? previous,
          DateTimeOffset nowUtc,
          double refreshIntervalSeconds);
  }
  ```

  Выход: контракты компилируются, тесты всё ещё падают (нет `AlertEngine`).

  Проверка: `dotnet build src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj 2>&1 | tail -5` → FAIL: только `AlertEngine` не найден (`ConstRule : IAlertRule` уже резолвится).

- [ ] **Шаг 3: Реализация AlertEngine**

  Вход: контракты шага 2.

  Действие: создать `src/AdminPanel.Core/Alerting/AlertEngine.cs`:

  ```csharp
  using AdminPanel.Infrastructure.DI;

  namespace AdminPanel.Core.Alerting;

  // Каркас: прогон правил → sinceUnix из прошлого снапшота → детерминированная сортировка (spec §4.1).
  // Id ("kind:target") формируют правила; движок не меняет id, только SinceUnix.
  [InjectAsSingleton(typeof(IAlertEngine))]
  public sealed class AlertEngine(IEnumerable<IAlertRule> rules) : IAlertEngine
  {
      // Severity по убыванию: Critical → Warning → Info (spec §3.10).
      private static readonly IComparer<AlertSeverity> SeverityDescending =
          Comparer<AlertSeverity>.Create((x, y) => y.CompareTo(x));

      public IReadOnlyList<Alert> Evaluate(
          EtcdSnapshot snapshot,
          EtcdSnapshot? previous,
          DateTimeOffset nowUtc,
          double refreshIntervalSeconds)
      {
          var context = new AlertContext(previous, nowUtc, refreshIntervalSeconds);
          var nowUnix = nowUtc.ToUnixTimeSeconds();
          return
          [
              .. rules
                 .SelectMany(r => r.Evaluate(snapshot, context))
                 .Select(a => a with { SinceUnix = ResolveSince(a, previous, nowUnix) })
                 .OrderBy(a => a.Severity, SeverityDescending)
                 .ThenBy(a => a.Kind, StringComparer.Ordinal)
                 .ThenBy(a => a.Target, StringComparer.Ordinal),
          ];
      }

      // sinceUnix: previous нет → null; id был в previous → перенос (в т.ч. null);
      // новый id → unix текущей оценки (spec §3.4).
      private static long? ResolveSince(Alert alert, EtcdSnapshot? previous, long nowUnix)
      {
          if (previous is null)
              return null;
          var before = previous.Alerts.FirstOrDefault(a => a.Id == alert.Id);
          return before is null ? nowUnix : before.SinceUnix;
      }
  }
  ```

  Выход: реализация каркаса; Core-сборка собирается.

  Проверка: `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj --filter "FullyQualifiedName~AlertEngineTests" 2>&1 | tail -5` → PASS: **Passed: 6, Failed: 0** (6 новых каркасных тестов).

- [ ] **Шаг 4: Полный unit-прогон (регрессия) и коммит**

  Вход: зелёные каркасные тесты.

  Действие: прогнать всё; закоммитить.

  Проверка: `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj 2>&1 | tail -3` → `Passed! - Failed: 0, Passed: 89, Skipped: 0, Total: 89` (83 существующих + 6 новых).

  Коммит:

  ```bash
  git add src/AdminPanel.Core/Alerting/ src/tests/AdminPanel.UnitTests/TestSnapshots.cs src/tests/AdminPanel.UnitTests/AlertEngineTests.cs
  git commit -m "t04: AlertEngine — каркас правил алертов: id/sinceUnix/сортировка (unit)"
  ```

---

### Task 2: Семь etcd-правил каталога (03 §4)

**Files:**
- Create: `src/AdminPanel.Core/Alerting/Rules/EtcdUnreachableRule.cs`
- Create: `src/AdminPanel.Core/Alerting/Rules/EtcdNoQuorumRule.cs`
- Create: `src/AdminPanel.Core/Alerting/Rules/EtcdEndpointDownRule.cs`
- Create: `src/AdminPanel.Core/Alerting/Rules/EtcdAlarmRule.cs`
- Create: `src/AdminPanel.Core/Alerting/Rules/SnapshotStaleRule.cs`
- Create: `src/AdminPanel.Core/Alerting/Rules/ClusterIncompleteRule.cs`
- Create: `src/AdminPanel.Core/Alerting/Rules/KeyMalformedRule.cs`
- Create: `src/tests/AdminPanel.UnitTests/AlertTestRules.cs`
- Modify: `src/tests/AdminPanel.UnitTests/AlertEngineTests.cs` (добавить 12 тестов)

**Interfaces:**
- Consumes: `IAlertRule`, `AlertContext` (Task 1); модель Core t03 (`EtcdStatus`, `ClusterInfo.Incomplete`, `ParseErrors`).
- Produces:
  - Классы правил `EtcdUnreachableRule` / `EtcdNoQuorumRule` / `EtcdEndpointDownRule` / `EtcdAlarmRule` / `SnapshotStaleRule` / `ClusterIncompleteRule` / `KeyMalformedRule`, каждый `[InjectAsSingleton(typeof(IAlertRule))]`, kind'ы: `etcd-unreachable`, `etcd-no-quorum`, `etcd-endpoint-down`, `etcd-alarm`, `snapshot-stale`, `cluster-incomplete`, `key-malformed` (spec §4.2–4.3).
  - `EtcdUnreachableRule.Threshold` (const int = 2), `SnapshotStaleRule.Multiplier` (const double = 3), `EtcdAlarmRule.AlarmTypeName(EtcdAlarmType) → string` (public static: `nospace`/`corrupt`/`unknown`) — используются Task 4 (OverviewMapper порог) и тестами.
  - Тестовый `AlertTestRules.All() → IReadOnlyList<IAlertRule>` (7 правил) — используют харнессы Task 3.

- [ ] **Шаг 1: Failing-тесты правил**

  Вход: Task 1 слит (каркас зелёный).

  Действие: создать `src/tests/AdminPanel.UnitTests/AlertTestRules.cs`:

  ```csharp
  using AdminPanel.Core.Alerting;
  using AdminPanel.Core.Alerting.Rules;

  namespace AdminPanel.UnitTests;

  // Все правила t04 одним списком: харнессы refresher'а и тест уникальности kind'ов (spec §10.1).
  internal static class AlertTestRules
  {
      public static IReadOnlyList<IAlertRule> All()
          =>
          [
              new EtcdUnreachableRule(),
              new EtcdNoQuorumRule(),
              new EtcdEndpointDownRule(),
              new EtcdAlarmRule(),
              new SnapshotStaleRule(),
              new ClusterIncompleteRule(),
              new KeyMalformedRule(),
          ];
  }
  ```

  Добавить в конец класса `AlertEngineTests` (файл Task 1; `using AdminPanel.Core.Alerting;` в начало файла, `using AdminPanel.Core.Alerting.Rules;` не нужен — правила только через `AlertTestRules`) 12 тестов:

  ```csharp
      private static IReadOnlyList<Alert> EvaluateAll(EtcdSnapshot snapshot, EtcdSnapshot? previous = null, DateTimeOffset? nowUtc = null)
          => new AlertEngine(AlertTestRules.All())
              .Evaluate(snapshot, previous, nowUtc ?? BuiltAt, 3);

      [Fact]
      public void Evaluate_HealthySnapshot_NoAlerts()
      {
          // Arrange / Act
          var alerts = EvaluateAll(TestSnapshots.Healthy(BuiltAt));

          // Assert: здоровая система — пустой список.
          alerts.Should().BeEmpty();
      }

      [Fact]
      public void RuleKinds_AllUnique()
      {
          // Arrange / Act
          var kinds = AlertTestRules.All().Select(r => r.Kind).ToList();

          // Assert: защита каркаса от copy-paste новых правил t05/t06 (spec §10.1).
          kinds.Should().HaveCount(7).And.OnlyHaveUniqueItems();
      }

      [Fact]
      public void Unreachable_AtThresholdTwo_Critical()
      {
          // Arrange
          var one = TestSnapshots.Healthy(BuiltAt) with
          {
              Etcd = TestSnapshots.HealthyEtcd(BuiltAt) with { ConsecutiveFailures = 1 },
          };
          var two = TestSnapshots.Healthy(BuiltAt) with
          {
              Etcd = TestSnapshots.HealthyEtcd(BuiltAt) with { ConsecutiveFailures = 2 },
          };

          // Act
          var below = EvaluateAll(one);
          var atThreshold = EvaluateAll(two);

          // Assert: порог каталога — 2 тика (arch/03 §4).
          below.Should().BeEmpty();
          var alert = atThreshold.Should().ContainSingle().Subject;
          alert.Severity.Should().Be(AlertSeverity.Critical);
          alert.Id.Should().Be("etcd-unreachable:etcd");
          alert.Details!["consecutiveFailures"].Should().Be("2");
      }

      [Fact]
      public void NoQuorum_WhenSuspected_CriticalWithErrors()
      {
          // Arrange: мёртвый endpoint даёт errors для details.
          var snapshot = TestSnapshots.Healthy(BuiltAt) with
          {
              Etcd = TestSnapshots.HealthyEtcd(BuiltAt, alive: 2, total: 3) with { QuorumSuspected = true },
          };

          // Act
          var alerts = EvaluateAll(snapshot);

          // Assert: quorum-алерт critical + один endpoint-down (мёртвый); ошибки склеены.
          var quorum = alerts.Single(a => a.Kind == "etcd-no-quorum");
          quorum.Severity.Should().Be(AlertSeverity.Critical);
          quorum.Id.Should().Be("etcd-no-quorum:etcd");
          quorum.Details!["errors"].Should().Contain("connection refused");
      }

      [Fact]
      public void EndpointDown_PerFailedEndpoint_Warning()
      {
          // Arrange: 2 из 3 упали.
          var snapshot = TestSnapshots.Healthy(BuiltAt) with
          {
              Etcd = TestSnapshots.HealthyEtcd(BuiltAt, alive: 1, total: 3),
          };

          // Act
          var alerts = EvaluateAll(snapshot);

          // Assert: по одному алерту на endpoint, target = URL.
          var down = alerts.Where(a => a.Kind == "etcd-endpoint-down").ToList();
          down.Should().HaveCount(2);
          down.Should().OnlyContain(a => a.Severity == AlertSeverity.Warning
              && a.Target.StartsWith("http://etcd", StringComparison.Ordinal)
              && a.Message.Contains(a.Target, StringComparison.Ordinal));
      }

      [Fact]
      public void EndpointDown_AllAlive_NoAlert()
      {
          // Arrange / Act
          var alerts = EvaluateAll(TestSnapshots.Healthy(BuiltAt));

          // Assert
          alerts.Should().NotContain(a => a.Kind == "etcd-endpoint-down");
      }

      [Fact]
      public void Alarm_PerAlarm_CriticalWithMemberIdType()
      {
          // Arrange: NOSPACE и CORRUPT — два alarm'а.
          var snapshot = TestSnapshots.Healthy(BuiltAt) with
          {
              Etcd = TestSnapshots.HealthyEtcd(BuiltAt) with
              {
                  Alarms = [new EtcdAlarm(42, EtcdAlarmType.NoSpace), new EtcdAlarm(43, EtcdAlarmType.Corrupt)],
              },
          };

          // Act
          var alerts = EvaluateAll(snapshot);

          // Assert: target "{memberId}:{type}" (spec §3.7).
          var alarms = alerts.Where(a => a.Kind == "etcd-alarm").ToList();
          alarms.Should().HaveCount(2).And.OnlyContain(a => a.Severity == AlertSeverity.Critical);
          alarms.Should().Contain(a => a.Target == "42:nospace" && a.Details!["alarmType"] == "nospace");
          alarms.Should().Contain(a => a.Target == "43:corrupt" && a.Details!["memberId"] == "43");
      }

      [Fact]
      public void SnapshotStale_AfterThreeIntervals_Warning()
      {
          // Arrange: порог 3×3 c = 9 c (arch/03 §4).
          var snapshot = TestSnapshots.Healthy(BuiltAt);

          // Act
          var fresh = EvaluateAll(snapshot, nowUtc: BuiltAt + TimeSpan.FromSeconds(6));
          var stale = EvaluateAll(snapshot, nowUtc: BuiltAt + TimeSpan.FromSeconds(10));

          // Assert
          fresh.Should().NotContain(a => a.Kind == "snapshot-stale");
          var alert = stale.Single(a => a.Kind == "snapshot-stale");
          alert.Severity.Should().Be(AlertSeverity.Warning);
          alert.Details!["ageSeconds"].Should().Be("10");
          alert.Details!["thresholdSeconds"].Should().Be("9");
      }

      [Fact]
      public void ClusterIncomplete_OnlyIncompleteClusters()
      {
          // Arrange: полный demo + ghost без config.
          var snapshot = TestSnapshots.Healthy(BuiltAt) with
          {
              Clusters = [TestSnapshots.FullCluster(), TestSnapshots.GhostCluster()],
          };

          // Act
          var alerts = EvaluateAll(snapshot);

          // Assert
          var alert = alerts.Single(a => a.Kind == "cluster-incomplete");
          alert.Target.Should().Be("ghost");
          alert.Details!["dbname"].Should().Be("missing");
      }

      [Fact]
      public void KeyMalformed_PerParseError()
      {
          // Arrange
          var snapshot = TestSnapshots.Healthy(BuiltAt) with
          {
              ParseErrors =
              [
                  new KeyParseError("/clusters/demo/config", "битый JSON"),
                  new KeyParseError("/clusters/demo/shards/s1/replicas", "не целое"),
              ],
          };

          // Act
          var alerts = EvaluateAll(snapshot);

          // Assert: по одному алерту на запись, target = ключ.
          var malformed = alerts.Where(a => a.Kind == "key-malformed").ToList();
          malformed.Should().HaveCount(2);
          malformed.Should().Contain(a => a.Target == "/clusters/demo/config" && a.Details!["reason"] == "битый JSON");
      }

      [Fact]
      public void SinceUnix_DisappearedAlert_NotResurrected()
      {
          // Arrange: previous содержал key-malformed, новый снапшот — без ошибок парсинга.
          var previous = TestSnapshots.Healthy(BuiltAt) with
          {
              ParseErrors = [new KeyParseError("/clusters/demo/config", "битый JSON")],
              Alerts = [new Alert("key-malformed:/clusters/demo/config", AlertSeverity.Warning,
                  "key-malformed", "/clusters/demo/config", "ключ не разобран", null, 1000)],
          };

          // Act
          var alerts = EvaluateAll(TestSnapshots.Healthy(BuiltAt + TimeSpan.FromSeconds(3)), previous);

          // Assert: истории нет — исчезнувший алерт не возвращается (spec §3.4).
          alerts.Should().NotContain(a => a.Kind == "key-malformed");
      }

      [Fact]
      public void Evaluate_Ids_AreKindColonTarget()
      {
          // Arrange: все семь проблем разом.
          var snapshot = TestSnapshots.Healthy(BuiltAt) with
          {
              Etcd = TestSnapshots.HealthyEtcd(BuiltAt, alive: 1, total: 2) with
              {
                  ConsecutiveFailures = 2,
                  QuorumSuspected = true,
                  Alarms = [new EtcdAlarm(42, EtcdAlarmType.NoSpace)],
              },
              Clusters = [TestSnapshots.FullCluster(), TestSnapshots.GhostCluster()],
              ParseErrors = [new KeyParseError("/clusters/demo/config", "битый JSON")],
          };

          // Act: 30 c с постройки → snapshot-stale тоже активен.
          var alerts = EvaluateAll(snapshot, nowUtc: BuiltAt + TimeSpan.FromSeconds(30));

          // Assert
          alerts.Should().HaveCount(7);
          alerts.Should().OnlyContain(a => a.Id == $"{a.Kind}:{a.Target}");
      }
  ```

  Выход: 12 новых failing-тестов (типы правил не существуют).

  Проверка: `dotnet build src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj 2>&1 | tail -5` → FAIL компиляции: `EtcdUnreachableRule` и остальные не найдены — красная фаза.

- [ ] **Шаг 2: Реализация семи правил**

  Вход: красная фаза шага 1.

  Действие: создать 7 файлов в `src/AdminPanel.Core/Alerting/Rules/`.

  `EtcdUnreachableRule.cs`:

  ```csharp
  using AdminPanel.Core.Alerting;
  using AdminPanel.Infrastructure.DI;

  namespace AdminPanel.Core.Alerting.Rules;

  // etcd-unreachable (critical): consecutiveFailures >= 2 тиков (arch/03 §4, spec §4.2).
  [InjectAsSingleton(typeof(IAlertRule))]
  public sealed class EtcdUnreachableRule : IAlertRule
  {
      public const string KindName = "etcd-unreachable";

      // Порог каталога «>= 2 тиков» — константа, не настройка (spec §3.6).
      public const int Threshold = 2;

      public string Kind => KindName;

      public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
      {
          var failures = snapshot.Etcd.ConsecutiveFailures;
          if (failures < Threshold)
              yield break;

          yield return new Alert(
              $"{KindName}:etcd",
              AlertSeverity.Critical,
              KindName,
              "etcd",
              $"etcd недоступен: {failures} подряд неудачных тика",
              new Dictionary<string, string> { ["consecutiveFailures"] = failures.ToString() },
              SinceUnix: null); // проставляет AlertEngine (spec §3.4)
      }
  }
  ```

  `EtcdNoQuorumRule.cs`:

  ```csharp
  using AdminPanel.Core.Alerting;
  using AdminPanel.Infrastructure.DI;

  namespace AdminPanel.Core.Alerting.Rules;

  // etcd-no-quorum (critical): raft-признаки отсутствия лидера — QuorumSuspected t03 §3.11 (spec §4.2).
  [InjectAsSingleton(typeof(IAlertRule))]
  public sealed class EtcdNoQuorumRule : IAlertRule
  {
      public const string KindName = "etcd-no-quorum";

      public string Kind => KindName;

      public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
      {
          if (!snapshot.Etcd.QuorumSuspected)
              yield break;

          yield return new Alert(
              $"{KindName}:etcd",
              AlertSeverity.Critical,
              KindName,
              "etcd",
              "подозрение на отсутствие кворума etcd (raft без лидера)",
              new Dictionary<string, string>
              {
                  ["errors"] = string.Join("; ", snapshot.Etcd.Endpoints.SelectMany(e => e.Errors)),
              },
              null);
      }
  }
  ```

  `EtcdEndpointDownRule.cs`:

  ```csharp
  using AdminPanel.Core.Alerting;
  using AdminPanel.Infrastructure.DI;

  namespace AdminPanel.Core.Alerting.Rules;

  // etcd-endpoint-down (warning): endpoint из настроек недоступен — по одному на endpoint (arch/03 §4).
  [InjectAsSingleton(typeof(IAlertRule))]
  public sealed class EtcdEndpointDownRule : IAlertRule
  {
      public const string KindName = "etcd-endpoint-down";

      public string Kind => KindName;

      public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
      {
          foreach (var endpoint in snapshot.Etcd.Endpoints.Where(e => !e.Reachable))
              yield return new Alert(
                  $"{KindName}:{endpoint.Url}",
                  AlertSeverity.Warning,
                  KindName,
                  endpoint.Url,
                  $"endpoint etcd недоступен: {endpoint.Url}",
                  new Dictionary<string, string> { ["errors"] = string.Join("; ", endpoint.Errors) },
                  null);
      }
  }
  ```

  `EtcdAlarmRule.cs`:

  ```csharp
  using AdminPanel.Core.Alerting;
  using AdminPanel.Infrastructure.DI;

  namespace AdminPanel.Core.Alerting.Rules;

  // etcd-alarm (critical): активные тревоги /v3/maintenance/alarm — по одной на alarm (arch/03 §4).
  [InjectAsSingleton(typeof(IAlertRule))]
  public sealed class EtcdAlarmRule : IAlertRule
  {
      public const string KindName = "etcd-alarm";

      public string Kind => KindName;

      // Строчное имя типа тревоги; толерантность к будущим типам etcd — "unknown" (spec §3.7).
      // Public: тот же маппинг использует EtcdStatusMapper (Task 4) — единый источник.
      public static string AlarmTypeName(EtcdAlarmType type)
          => type switch
          {
              EtcdAlarmType.NoSpace => "nospace",
              EtcdAlarmType.Corrupt => "corrupt",
              _ => "unknown",
          };

      public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
      {
          foreach (var alarm in snapshot.Etcd.Alarms)
          {
              var type = AlarmTypeName(alarm.Type);
              yield return new Alert(
                  $"{KindName}:{alarm.MemberId}:{type}",
                  AlertSeverity.Critical,
                  KindName,
                  $"{alarm.MemberId}:{type}",
                  $"тревога etcd {type.ToUpperInvariant()} на member {alarm.MemberId}",
                  new Dictionary<string, string>
                  {
                      ["memberId"] = alarm.MemberId.ToString(),
                      ["alarmType"] = type,
                  },
                  null);
          }
      }
  }
  ```

  `SnapshotStaleRule.cs`:

  ```csharp
  using AdminPanel.Core.Alerting;
  using AdminPanel.Infrastructure.DI;

  namespace AdminPanel.Core.Alerting.Rules;

  // snapshot-stale (warning): BuiltAtUtc старше 3×RefreshInterval (arch/03 §4).
  [InjectAsSingleton(typeof(IAlertRule))]
  public sealed class SnapshotStaleRule : IAlertRule
  {
      public const string KindName = "snapshot-stale";

      // «старше 3×RefreshInterval» — константа каталога, не настройка (spec §3.6).
      // Public: порог OverviewDto.stale использует ту же константу (Task 4).
      public const double Multiplier = 3;

      public string Kind => KindName;

      public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
      {
          var threshold = TimeSpan.FromSeconds(Multiplier * context.RefreshIntervalSeconds);
          var age = context.NowUtc - snapshot.BuiltAtUtc;
          if (age <= threshold)
              yield break;

          yield return new Alert(
              $"{KindName}:snapshot",
              AlertSeverity.Warning,
              KindName,
              "snapshot",
              $"снапшот устарел: возраст {(long)age.TotalSeconds} c при пороге {(long)threshold.TotalSeconds} c",
              new Dictionary<string, string>
              {
                  ["ageSeconds"] = ((long)age.TotalSeconds).ToString(),
                  ["thresholdSeconds"] = ((long)threshold.TotalSeconds).ToString(),
                  ["builtAtUnix"] = snapshot.BuiltAtUtc.ToUnixTimeSeconds().ToString(),
              },
              null);
      }
  }
  ```

  `ClusterIncompleteRule.cs`:

  ```csharp
  using AdminPanel.Core.Alerting;
  using AdminPanel.Infrastructure.DI;

  namespace AdminPanel.Core.Alerting.Rules;

  // cluster-incomplete (warning): префикс /clusters/<C> без config (arch/03 §4; Incomplete — t03 §3.6).
  [InjectAsSingleton(typeof(IAlertRule))]
  public sealed class ClusterIncompleteRule : IAlertRule
  {
      public const string KindName = "cluster-incomplete";

      public string Kind => KindName;

      public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
      {
          foreach (var cluster in snapshot.Clusters.Where(c => c.Incomplete))
              yield return new Alert(
                  $"{KindName}:{cluster.Name}",
                  AlertSeverity.Warning,
                  KindName,
                  cluster.Name,
                  $"кластер {cluster.Name} без config-ключа (incomplete)",
                  new Dictionary<string, string> { ["dbname"] = cluster.DbName ?? "missing" },
                  null);
      }
  }
  ```

  `KeyMalformedRule.cs`:

  ```csharp
  using AdminPanel.Core.Alerting;
  using AdminPanel.Infrastructure.DI;

  namespace AdminPanel.Core.Alerting.Rules;

  // key-malformed (warning): ключ не разобран — по одному на ParseError (arch/03 §4; t03 §3.4).
  [InjectAsSingleton(typeof(IAlertRule))]
  public sealed class KeyMalformedRule : IAlertRule
  {
      public const string KindName = "key-malformed";

      public string Kind => KindName;

      public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
      {
          foreach (var error in snapshot.ParseErrors)
              yield return new Alert(
                  $"{KindName}:{error.Key}",
                  AlertSeverity.Warning,
                  KindName,
                  error.Key,
                  $"ключ не разобран: {error.Key}",
                  new Dictionary<string, string> { ["reason"] = error.Reason },
                  null);
      }
  }
  ```

  Выход: 7 правил; kind'ы/targets/details по таблицам spec §4.2–4.3.

  Проверка: `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj --filter "FullyQualifiedName~AlertEngineTests" 2>&1 | tail -3` → PASS: **Passed: 18, Failed: 0** (6 каркасных + 12 правил).

- [ ] **Шаг 3: Регрессия и коммит**

  Действие: полный unit-прогон + коммит.

  Проверка: `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj 2>&1 | tail -3` → `Passed! - Failed: 0, Passed: 101, Skipped: 0, Total: 101` (89 + 12).

  Коммит:

  ```bash
  git add src/AdminPanel.Core/Alerting/Rules/ src/tests/AdminPanel.UnitTests/AlertTestRules.cs src/tests/AdminPanel.UnitTests/AlertEngineTests.cs
  git commit -m "t04: etcd-правила каталога алертов 03 §4 — 7 kind'ов (unit)"
  ```

---

### Task 3: Интеграция AlertEngine в SnapshotRefresher (оба пути тика)

**Files:**
- Modify: `src/AdminPanel.Etcd/SnapshotRefresher.cs`
- Modify: `src/tests/AdminPanel.UnitTests/SnapshotRefresherTests.cs` (харнесс + 2 новых теста)
- Modify: `src/tests/AdminPanel.IntegrationTests/EtcdSnapshotIntegrationTests.cs` (харнесс + ассерты в EtcdFailureTests)

**Interfaces:**
- Consumes: `IAlertEngine` (Task 1), `AlertTestRules.All()` (Task 2, unit), полный список правил (integration — своя сборка, хелпер недоступен).
- Produces: `SnapshotRefresher(IEtcdGateway gateway, IAlertEngine alertEngine, ISnapshotStore store, IOptions<EtcdOptions> options, TimeProvider time, ILogger<SnapshotRefresher> logger)` — новый порядок аргументов конструктора (вторым параметром); снапшоты в store всегда с вычисленными `Alerts` (spec §3.5, §5).

- [ ] **Шаг 1: Failing-тесты (alerts в снапшоте на обоих путях тика)**

  Вход: Task 2 слит.

  Действие: в `src/tests/AdminPanel.UnitTests/SnapshotRefresherTests.cs` добавить `using AdminPanel.Core.Alerting;` и `using AdminPanel.Etcd.Client;` (последний уже есть) — новый using только `AdminPanel.Core.Alerting` не нужен: тесты не ссылаются на типы Alerting напрямую (Kv уже импортирован). Добавить в конец класса `SnapshotRefresherTests` два теста:

  ```csharp
      [Fact]
      public async Task Refresh_AlertsStoredOnSuccessTick()
      {
          // Arrange: полный demo-сид + один битый статус-ключ → key-malformed (spec §10.2).
          var store = new SnapshotStore();
          var gateway = new FakeEtcdGateway
          {
              ClustersKv =
              [
                  .. EtcdFixtures.LoadKv("clusters-full.json"),
                  new Kv("/clusters/demo/buckets/status/bucket_9", "not json", 99),
              ],
              ServiceKv = EtcdFixtures.LoadKv("service-full.json"),
          };
          var refresher = RefresherTestHarness.New(gateway, store, "http://e1");

          // Act
          await refresher.RefreshOnceAsync(CancellationToken.None);

          // Assert: единственный алерт — битый ключ (кластер demo полный, endpoints живы).
          var alert = store.Current!.Alerts.Should().ContainSingle().Subject;
          alert.Kind.Should().Be("key-malformed");
          alert.Target.Should().Be("/clusters/demo/buckets/status/bucket_9");
      }

      [Fact]
      public async Task Refresh_AlertsComputedOnFailTick()
      {
          // Arrange: первый тик собирает снапшот с incomplete-кластером; затем endpoints умирают.
          var store = new SnapshotStore();
          var gateway = new FakeEtcdGateway
          {
              ClustersKv = [new Kv("/clusters/ghost/shards/g1/dsn", "host=g1 port=5432", 1)],
          };
          var refresher = RefresherTestHarness.New(gateway, store, "http://e1");
          await refresher.RefreshOnceAsync(CancellationToken.None);
          gateway.StatusFailEndpoints.Add("http://e1");

          // Act: два отказных тика — порог etcd-unreachable = 2 (spec §4.2).
          await refresher.RefreshOnceAsync(CancellationToken.None);
          await refresher.RefreshOnceAsync(CancellationToken.None);

          // Assert: unreachable вспыхнул; data-алерт из прежних данных сохранён,
          // sinceUnix не рвётся (перенос null с первого тика — §3.4).
          var alerts = store.Current!.Alerts;
          alerts.Should().Contain(a => a.Id == "etcd-unreachable:etcd"
              && a.Severity == AlertSeverity.Critical);
          var incomplete = alerts.Single(a => a.Kind == "cluster-incomplete");
          incomplete.Target.Should().Be("ghost");
          incomplete.SinceUnix.Should().BeNull();
      }
  ```

  Выход: 2 failing-теста (первый — `Alerts` пуст; второй — `etcd-unreachable` отсутствует: t03 кладёт `Alerts = []`).

  Проверка: `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj --filter "FullyQualifiedName~SnapshotRefresherTests" 2>&1 | tail -3` → FAIL: **Failed: 2, Passed: 6** — красная фаза.

- [ ] **Шаг 2: Правка SnapshotRefresher**

  Вход: красная фаза шага 1; текущий `SnapshotRefresher.cs` (t03).

  Действие: в `src/AdminPanel.Etcd/SnapshotRefresher.cs`:

  1) Добавить using: `using AdminPanel.Core.Alerting;`

  2) Конструктор — добавить вторым параметром движок (остальные параметры без изменений):

  ```csharp
  public sealed class SnapshotRefresher(
      IEtcdGateway gateway,
      IAlertEngine alertEngine,
      ISnapshotStore store,
      IOptions<EtcdOptions> options,
      TimeProvider time,
      ILogger<SnapshotRefresher> logger) : BackgroundService, IHealthCheckService
  ```

  3) Успешный тик — заменить блок шага 6 (комментарий «6. Сборка + атомарная замена …») на:

  ```csharp
          // 6. Сборка + алерты + атомарная замена (arch/02 §4 п.4–5; Alerts на обоих путях тика, spec §5).
          var built = SnapshotBuilder.Build(
              time, clustersParsed, serviceParsed, nodes,
              etcd.Members, etcd.Alarms, etcd);
          store.Replace(built with
          {
              Alerts = alertEngine.Evaluate(built, previous, now, EffectiveIntervalSeconds()),
          });
          return Finish(Result.Success(), working: true);
  ```

  4) `FailTick` — заменить хвостовую часть (после конструирования `new EtcdSnapshot(...)`, вместо текущего `store.Replace(new EtcdSnapshot(...))`):

  ```csharp
          var failed = new EtcdSnapshot(
              previous?.BuiltAtUtc ?? now,
              etcd,
              previous?.Clusters ?? [],
              previous?.HaScopes ?? [],
              previous?.StandNodes ?? [],
              [],
              [],
              previous?.ParseErrors ?? [],
              previous?.UnknownKeyCount ?? 0);

          // Алерты вычисляются и на отказном тике: etcd-unreachable/snapshot-stale
          // живут именно здесь (spec §3.5); data-алерты пересчитываются по прежним данным.
          store.Replace(failed with
          {
              Alerts = alertEngine.Evaluate(failed, previous, now, EffectiveIntervalSeconds()),
          });
          return Finish(error, working: false);
  ```

  5) Добавить приватный метод (рядом с `IsValidEndpoint`):

  ```csharp
      // Эффективный интервал тика: RefreshIntervalSeconds или fallback 3 c (t03 §3.3);
      // тот же порог ×3 кормит snapshot-stale (spec §3.3).
      private double EffectiveIntervalSeconds()
      {
          var seconds = options.Value.RefreshIntervalSeconds;
          return seconds > 0 ? seconds : 3;
      }
  ```

  6) Обновить unit-харнесс в `src/tests/AdminPanel.UnitTests/SnapshotRefresherTests.cs` (`RefresherTestHarness.New`) — добавить второй аргумент:

  ```csharp
      public static SnapshotRefresher New(FakeEtcdGateway gateway, ISnapshotStore store, params string[] endpoints)
          => new(
              gateway,
              new AlertEngine(AlertTestRules.All()),
              store,
              Options.Create(new EtcdOptions { Endpoints = endpoints }),
              new FixedTimeProvider(),
              NullLogger<SnapshotRefresher>.Instance);
  ```

  и using вверху файла: `using AdminPanel.Core.Alerting;`

  7) Обновить integration-харнесс в `src/tests/AdminPanel.IntegrationTests/EtcdSnapshotIntegrationTests.cs` (`EtcdTestHarness.NewRefresher`) — usings `using AdminPanel.Core.Alerting;` + `using AdminPanel.Core.Alerting.Rules;` вверху файла и тело:

  ```csharp
      public static SnapshotRefresher NewRefresher(ISnapshotStore store, params string[] endpoints)
          => new(
              NewGateway(),
              new AlertEngine(
              [
                  new EtcdUnreachableRule(),
                  new EtcdNoQuorumRule(),
                  new EtcdEndpointDownRule(),
                  new EtcdAlarmRule(),
                  new SnapshotStaleRule(),
                  new ClusterIncompleteRule(),
                  new KeyMalformedRule(),
              ]),
              store,
              Options.Create(new EtcdOptions { Endpoints = endpoints }),
              new RealTimeProvider(),
              NullLogger<SnapshotRefresher>.Instance);
  ```

  8) В том же файле, `EtcdFailureTests.Refresher_EtcdStopped_KeepsPreviousSnapshot`, после ассерта `store.Current.Etcd.ConsecutiveFailures.Should().Be(2);` добавить:

  ```csharp
          // t04: алерты вычислены и на отказном тике — unreachable на пороге 2 (spec §3.5).
          store.Current.Alerts.Should().Contain(a => a.Id == "etcd-unreachable:etcd");
  ```

  Выход: оба пути тика вычисляют Alerts; харнессы обновлены (существующие тесты `EtcdHealthCheckTests` компилируются без правок — они зовут `RefresherTestHarness.New`).

  Проверка: `dotnet build src/AdminPanel.slnx 2>&1 | tail -3` → успех, 0 warnings (если CS-ошибка об аргументах конструктора — проверить порядок параметров: `gateway, alertEngine, store, options, time, logger`). Затем `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj --filter "FullyQualifiedName~SnapshotRefresherTests" 2>&1 | tail -3` → PASS: **Passed: 8, Failed: 0** (6 прежних + 2 новых). Полный unit-прогон: `Passed! - Failed: 0, Passed: 103` (101 + 2).

- [ ] **Шаг 3: Integration против живого etcd (Docker) и коммит**

  Действие: прогнать integration-классы, затронутые правкой, затем закоммитить.

  Проверка (нужен Docker):
  `dotnet test src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj --filter "FullyQualifiedName~EtcdSnapshotIntegrationTests|FullyQualifiedName~EtcdFailureTests" 2>&1 | tail -3` → PASS: **Passed: 9, Failed: 0** (8 фактов класса EtcdSnapshotIntegrationTests — включая неизменный ассерт `snapshot.Alerts.Should().BeEmpty()` на чистом сиде demo — плюс 1 факт EtcdFailureTests с новым ассертом unreachable; оба класса в одном файле, grep-счётчик файла «9 [Fact]» = 8 + 1).

  Коммит:

  ```bash
  git add src/AdminPanel.Etcd/SnapshotRefresher.cs src/tests/AdminPanel.UnitTests/SnapshotRefresherTests.cs src/tests/AdminPanel.IntegrationTests/EtcdSnapshotIntegrationTests.cs
  git commit -m "t04: SnapshotRefresher — Alerts на обоих путях тика (unit+integration)"
  ```

---

### Task 4: API-эндпоинты инспекции (InspectionModule + 3 query)

**Files:**
- Create: `src/AdminPanel.Api/Inspection/InspectionModule.cs`
- Create: `src/AdminPanel.Api/Inspection/OverviewQuery.cs`
- Create: `src/AdminPanel.Api/Inspection/EtcdStatusQuery.cs`
- Create: `src/AdminPanel.Api/Inspection/AlertsQuery.cs`
- Modify: `src/AdminPanel.Api/Program.cs` (+1 строка маппинга, +1 using)
- Test: `src/tests/AdminPanel.UnitTests/InspectionMappersTests.cs` (новый)
- Test: `src/tests/AdminPanel.UnitTests/InspectionQueryHandlerTests.cs` (новый)

**Interfaces:**
- Consumes: `ISnapshotStore`/`EtcdOptions` (`AdminPanel.Etcd`), `IHandler`/`IQuery`/`IQueryHandler`/`Result` (Infrastructure), `EtcdAlarmRule.AlarmTypeName` и `SnapshotStaleRule.Multiplier` (Task 2), `TimeProvider` из DI (t02).
- Produces (полные DTO/мапперы — spec §6.2):
  - `InspectionModule.MapInspectionApi(this IEndpointRouteBuilder)`; `InspectionModule.SnapshotNotReadyException` (вложенный `sealed class`).
  - `OverviewQuery : IQuery<OverviewDto>`; `OverviewDto(AlertsCritical, AlertsWarning, Etcd, Clusters, ActiveMoves, SnapshotAgeMs, Stale)` + `OverviewEtcdDto(Reachable, EndpointsOk, EndpointsTotal)` + заглушки `OverviewClusterDto`/`OverviewMoveDto`; `OverviewMapper.Map(EtcdSnapshot, DateTimeOffset, double) → OverviewDto`; `OverviewQueryHandler(ISnapshotStore, TimeProvider, IOptions<EtcdOptions>)`.
  - `EtcdStatusQuery : IQuery<EtcdStatusDto>`; `EtcdStatusDto(Endpoints, Members, Alarms, QuorumSuspected, LastRefreshUtc)` + `EtcdEndpointDto(Url, Reachable, LatencyMs, Version, DbSizeBytes, LeaderMemberId, RaftTerm, Errors, Active)` + `EtcdMemberDto(Id, Name, PeerUrls, ClientUrls, IsLeader)` + `EtcdAlarmDto(MemberId, Type)`; `EtcdStatusMapper.Map(EtcdStatus) → EtcdStatusDto`; `EtcdStatusQueryHandler(ISnapshotStore)`.
  - `AlertsQuery(AlertSeverity? Severity, string? Kind) : IQuery<IReadOnlyList<AlertDto>>`; `AlertDto(Id, Severity, Kind, Target, Message, Details, SinceUnix)`; `AlertsMapper.Map(IReadOnlyList<Alert>) / .ApplyFilters(IReadOnlyList<Alert>, AlertSeverity?, string?)`; `AlertsQueryHandler(ISnapshotStore)`.

- [ ] **Шаг 1: Failing-тесты мапперов**

  Вход: Task 3 слит; `TestSnapshots` доступен в unit-сборке.

  Действие: создать `src/tests/AdminPanel.UnitTests/InspectionMappersTests.cs`:

  ```csharp
  using AdminPanel.Api.Inspection;
  using AdminPanel.Core;
  using FluentAssertions;
  using Xunit;

  namespace AdminPanel.UnitTests;

  // Мапперы снапшот → DTO: чистые функции, тестируются напрямую (spec §10.3).
  public class InspectionMappersTests
  {
      private static readonly DateTimeOffset BuiltAt = new(2026, 8, 22, 12, 0, 0, TimeSpan.Zero);

      [Fact]
      public void OverviewMapper_CountsEtcdAndAlerts()
      {
          // Arrange: 1 critical + 1 warning алерт, 3 живых endpoints из 3.
          var snapshot = TestSnapshots.Healthy(BuiltAt) with
          {
              Alerts =
              [
                  new Alert("a:etcd", AlertSeverity.Critical, "a", "etcd", "m", null, null),
                  new Alert("b:etcd", AlertSeverity.Warning, "b", "etcd", "m", null, null),
              ],
          };

          // Act
          var dto = OverviewMapper.Map(snapshot, BuiltAt + TimeSpan.FromSeconds(1), 3);

          // Assert
          dto.AlertsCritical.Should().Be(1);
          dto.AlertsWarning.Should().Be(1);
          dto.Etcd.Reachable.Should().BeTrue();
          dto.Etcd.EndpointsOk.Should().Be(3);
          dto.Etcd.EndpointsTotal.Should().Be(3);
          dto.SnapshotAgeMs.Should().Be(1000);
          dto.Stale.Should().BeFalse();
      }

      [Fact]
      public void OverviewMapper_StaleByTripleInterval_True()
      {
          // Arrange: возраст 12 c > порога 3×3 c (spec §3.15).
          var snapshot = TestSnapshots.Healthy(BuiltAt);

          // Act
          var dto = OverviewMapper.Map(snapshot, BuiltAt + TimeSpan.FromSeconds(12), 3);

          // Assert
          dto.Stale.Should().BeTrue();
          dto.SnapshotAgeMs.Should().Be(12000);
      }

      [Fact]
      public void OverviewMapper_NegativeAgeClampedToZero()
      {
          // Arrange: BuiltAtUtc в будущем (скачок часов) — возраст не отрицательный.
          var snapshot = TestSnapshots.Healthy(BuiltAt);

          // Act
          var dto = OverviewMapper.Map(snapshot, BuiltAt - TimeSpan.FromSeconds(5), 3);

          // Assert
          dto.SnapshotAgeMs.Should().Be(0);
      }

      [Fact]
      public void OverviewMapper_ClusterStubs_Empty()
      {
          // Arrange / Act
          var dto = OverviewMapper.Map(TestSnapshots.Healthy(BuiltAt), BuiltAt, 3);

          // Assert: кластерная часть — заглушки t05 (spec §3.15).
          dto.Clusters.Should().BeEmpty();
          dto.ActiveMoves.Should().BeEmpty();
      }

      [Fact]
      public void EtcdStatusMapper_ActiveFlag_OnlyForActiveEndpoint()
      {
          // Arrange: ActiveEndpoint = etcd1, endpoints etcd1..etcd3.
          var etcd = TestSnapshots.HealthyEtcd(BuiltAt);

          // Act
          var dto = EtcdStatusMapper.Map(etcd);

          // Assert
          dto.Endpoints.Should().HaveCount(3);
          dto.Endpoints.Should().OnlyContain(e => e.Active == (e.Url == "http://etcd1:2379"));
      }

      [Fact]
      public void EtcdStatusMapper_IsLeader_ByLeaderMemberIdOfAliveEndpoint()
      {
          // Arrange: лидер 42; member 42 и member 43.
          var etcd = TestSnapshots.HealthyEtcd(BuiltAt) with
          {
              Members =
              [
                  new EtcdMember(42, "etcd1", ["http://p1"], ["http://c1"]),
                  new EtcdMember(43, "etcd2", ["http://p2"], ["http://c2"]),
              ],
          };

          // Act
          var dto = EtcdStatusMapper.Map(etcd);

          // Assert: isLeader по совпадению id со статусом leader (arch/02 §2.4).
          dto.Members.Should().HaveCount(2);
          dto.Members.Single(m => m.Id == "42").IsLeader.Should().BeTrue();
          dto.Members.Single(m => m.Id == "43").IsLeader.Should().BeFalse();
      }

      [Fact]
      public void EtcdStatusMapper_IsLeader_FallsBackToDeadEndpointLeader()
      {
          // Arrange: живых нет; у первого (неживого) endpoint'а leader остался (spec §3.14).
          var etcd = TestSnapshots.HealthyEtcd(BuiltAt) with
          {
              Endpoints =
              [
                  new EtcdEndpoint("http://etcd1:2379", false, null, null, null, 42, null, null, ["timeout"]),
                  new EtcdEndpoint("http://etcd2:2379", false, null, null, null, null, null, null, ["timeout"]),
              ],
              Members =
              [
                  new EtcdMember(42, "etcd1", ["http://p1"], ["http://c1"]),
                  new EtcdMember(43, "etcd2", ["http://p2"], ["http://c2"]),
              ],
          };

          // Act
          var dto = EtcdStatusMapper.Map(etcd);

          // Assert: лидер определён по fallback — неживому endpoint'у с валидным leader.
          dto.Members.Single(m => m.Id == "42").IsLeader.Should().BeTrue();
          dto.Members.Single(m => m.Id == "43").IsLeader.Should().BeFalse();
      }

      [Fact]
      public void EtcdStatusMapper_IsLeader_NoLeaderAnywhere_AllFalse()
      {
          // Arrange: ни у одного endpoint'а нет leader (нет кворума — arch/01 §8).
          var etcd = TestSnapshots.HealthyEtcd(BuiltAt) with
          {
              Endpoints =
              [
                  new EtcdEndpoint("http://etcd1:2379", true, 3.0, "3.5.21", 20480, null, 17, 3, []),
                  new EtcdEndpoint("http://etcd2:2379", true, 4.0, "3.5.21", 20480, null, 17, 3, []),
              ],
              Members =
              [
                  new EtcdMember(42, "etcd1", ["http://p1"], ["http://c1"]),
                  new EtcdMember(43, "etcd2", ["http://p2"], ["http://c2"]),
              ],
          };

          // Act
          var dto = EtcdStatusMapper.Map(etcd);

          // Assert: лидер не определён — все IsLeader=false.
          dto.Members.Should().HaveCount(2).And.OnlyContain(m => !m.IsLeader);
      }

      [Fact]
      public void EtcdStatusMapper_MapsAlarmsQuorumLastRefresh()
      {
          // Arrange: мёртвый endpoint без leader → лидер не определён; alarm NOSPACE.
          var etcd = TestSnapshots.HealthyEtcd(BuiltAt) with
          {
              Alarms = [new EtcdAlarm(42, EtcdAlarmType.NoSpace)],
              QuorumSuspected = true,
          };

          // Act
          var dto = EtcdStatusMapper.Map(etcd);

          // Assert
          dto.Alarms.Should().ContainSingle().Which.Type.Should().Be("nospace");
          dto.QuorumSuspected.Should().BeTrue();
          dto.LastRefreshUtc.Should().Be(BuiltAt);
      }

      [Fact]
      public void AlertsMapper_SeverityLowercaseStrings()
      {
          // Arrange
          var alerts = new List<Alert>
          {
              new("a:1", AlertSeverity.Critical, "a", "1", "m", null, null),
              new("b:1", AlertSeverity.Warning, "b", "1", "m", null, null),
              new("c:1", AlertSeverity.Info, "c", "1", "m", null, null),
          };

          // Act
          var dto = AlertsMapper.Map(alerts);

          // Assert: строчный канон arch/03 §1 (spec §3.11).
          dto.Select(a => a.Severity).Should().Equal("critical", "warning", "info");
      }

      [Fact]
      public void AlertsMapper_PassesDetailsAndSinceUnix()
      {
          // Arrange
          var alert = new Alert(
              "k:t", AlertSeverity.Warning, "k", "t", "msg",
              new Dictionary<string, string> { ["reason"] = "битый JSON" }, 1755800000);

          // Act
          var dto = AlertsMapper.Map([alert]).Single();

          // Assert
          dto.Id.Should().Be("k:t");
          dto.Message.Should().Be("msg");
          dto.Details!["reason"].Should().Be("битый JSON");
          dto.SinceUnix.Should().Be(1755800000);
      }

      [Fact]
      public void AlertsMapper_Filters_SeverityKindBoth()
      {
          // Arrange
          var alerts = new List<Alert>
          {
              new("a:1", AlertSeverity.Critical, "a", "1", "m", null, null),
              new("b:1", AlertSeverity.Warning, "b", "1", "m", null, null),
              new("c:1", AlertSeverity.Warning, "c", "1", "m", null, null),
          };

          // Act / Assert
          AlertsMapper.ApplyFilters(alerts, AlertSeverity.Warning, null)
              .Should().HaveCount(2);
          AlertsMapper.ApplyFilters(alerts, null, "b")
              .Should().ContainSingle().Which.Kind.Should().Be("b");
          AlertsMapper.ApplyFilters(alerts, AlertSeverity.Warning, "c")
              .Should().ContainSingle().Which.Kind.Should().Be("c");
          AlertsMapper.ApplyFilters(alerts, null, null)
              .Should().HaveCount(3);
      }
  }
  ```

  Выход: 12 failing-тестов (мапперы не существуют).

  Проверка: `dotnet build src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj 2>&1 | tail -5` → FAIL компиляции: `OverviewMapper`/`EtcdStatusMapper`/`AlertsMapper` не найдены — красная фаза.

- [ ] **Шаг 2: Failing-тесты хендлеров**

  Действие: создать `src/tests/AdminPanel.UnitTests/InspectionQueryHandlerTests.cs`:

  ```csharp
  using AdminPanel.Api.Inspection;
  using AdminPanel.Core;
  using AdminPanel.Etcd;
  using FluentAssertions;
  using Microsoft.Extensions.Options;
  using Xunit;

  namespace AdminPanel.UnitTests;

  // Хендлеры инспекции: 503-отказ «снапшота нет» и сборка DTO (spec §10.4).
  public class InspectionQueryHandlerTests
  {
      private readonly FixedTimeProvider _time = new();

      [Fact]
      public async Task OverviewHandle_NoSnapshot_ReturnsFailedSnapshotNotReady()
      {
          // Arrange: до первого тика Current = null (t03 §3.13).
          var handler = new OverviewQueryHandler(new SnapshotStore(), _time, Options.Create(new EtcdOptions()));

          // Act
          var result = await handler.Handle(new OverviewQuery(), CancellationToken.None);

          // Assert
          result.IsSuccess.Should().BeFalse();
          result.Error.Should().BeOfType<InspectionModule.SnapshotNotReadyException>();
      }

      [Fact]
      public async Task EtcdStatusHandle_NoSnapshot_ReturnsFailedSnapshotNotReady()
      {
          // Arrange
          var handler = new EtcdStatusQueryHandler(new SnapshotStore());

          // Act
          var result = await handler.Handle(new EtcdStatusQuery(), CancellationToken.None);

          // Assert
          result.IsSuccess.Should().BeFalse();
          result.Error.Should().BeOfType<InspectionModule.SnapshotNotReadyException>();
      }

      [Fact]
      public async Task AlertsHandle_NoSnapshot_ReturnsFailedSnapshotNotReady()
      {
          // Arrange
          var handler = new AlertsQueryHandler(new SnapshotStore());

          // Act
          var result = await handler.Handle(new AlertsQuery(null, null), CancellationToken.None);

          // Assert
          result.IsSuccess.Should().BeFalse();
          result.Error.Should().BeOfType<InspectionModule.SnapshotNotReadyException>();
      }

      [Fact]
      public async Task OverviewHandle_WithSnapshot_ReturnsDto()
      {
          // Arrange: BuiltAtUtc = фиксированное время теста → возраст 0.
          var store = new SnapshotStore();
          store.Replace(TestSnapshots.Healthy(_time.Utc) with
          {
              Alerts = [new Alert("a:etcd", AlertSeverity.Critical, "a", "etcd", "m", null, null)],
          });
          var handler = new OverviewQueryHandler(store, _time, Options.Create(new EtcdOptions()));

          // Act
          var result = await handler.Handle(new OverviewQuery(), CancellationToken.None);

          // Assert
          result.IsSuccess.Should().BeTrue();
          result.Value.AlertsCritical.Should().Be(1);
          result.Value.SnapshotAgeMs.Should().Be(0);
          result.Value.Stale.Should().BeFalse();
      }

      [Fact]
      public async Task AlertsHandler_AppliesFilters()
      {
          // Arrange
          var store = new SnapshotStore();
          store.Replace(TestSnapshots.Healthy(_time.Utc) with
          {
              Alerts =
              [
                  new Alert("a:1", AlertSeverity.Critical, "a", "1", "m", null, null),
                  new Alert("b:1", AlertSeverity.Warning, "b", "1", "m", null, null),
              ],
          });
          var handler = new AlertsQueryHandler(store);

          // Act
          var critical = await handler.Handle(new AlertsQuery(AlertSeverity.Critical, null), CancellationToken.None);
          var both = await handler.Handle(new AlertsQuery(AlertSeverity.Warning, "b"), CancellationToken.None);
          var none = await handler.Handle(new AlertsQuery(null, null), CancellationToken.None);

          // Assert
          critical.Value.Should().ContainSingle().Which.Kind.Should().Be("a");
          both.Value.Should().ContainSingle().Which.Kind.Should().Be("b");
          none.Value.Should().HaveCount(2);
      }
  }
  ```

  Выход: ещё 5 failing-тестов (хендлеры/`InspectionModule` не существуют).

  Проверка: тот же build → FAIL компиляции (красная фаза; всего в двух файлах 17 тестов).

- [ ] **Шаг 3: Реализация DTO/мапперов/хендлеров/модуля + Program.cs**

  Вход: красная фаза шагов 1–2.

  Действие: создать `src/AdminPanel.Api/Inspection/OverviewQuery.cs`:

  ```csharp
  using AdminPanel.Core;
  using AdminPanel.Core.Alerting.Rules;
  using AdminPanel.Etcd;
  using AdminPanel.Infrastructure;
  using AdminPanel.Infrastructure.CQRS;
  using AdminPanel.Infrastructure.DI;
  using Microsoft.Extensions.Options;

  namespace AdminPanel.Api.Inspection;

  // Запрос сводки дашборда (arch/03 §1 GET /api/overview).
  public sealed record OverviewQuery : IQuery<OverviewDto>;

  // Ответ GET /api/overview: etcd-часть реальна, кластерная — заглушки t05 (spec §3.15).
  public sealed record OverviewDto(
      int AlertsCritical,
      int AlertsWarning,
      OverviewEtcdDto Etcd,
      IReadOnlyList<OverviewClusterDto> Clusters,
      IReadOnlyList<OverviewMoveDto> ActiveMoves,
      long SnapshotAgeMs,
      bool Stale);

  public sealed record OverviewEtcdDto(bool Reachable, int EndpointsOk, int EndpointsTotal);

  // Заглушки контракта t05 (arch/03 §2): поля полные, значения — всегда пусто в t04.
  public sealed record OverviewClusterDto(
      string Name, int Shards, int Buckets, int ActiveMoves, int MasterlessShards);

  public sealed record OverviewMoveDto(
      string Cluster, int Bucket, string State, string? Owner, string? Target, long? UpdatedUnix);

  // Снапшот → сводку: чистая функция (spec §6.2).
  public static class OverviewMapper
  {
      public static OverviewDto Map(EtcdSnapshot snapshot, DateTimeOffset nowUtc, double refreshIntervalSeconds)
      {
          var age = nowUtc - snapshot.BuiltAtUtc;
          return new OverviewDto(
              snapshot.Alerts.Count(a => a.Severity == AlertSeverity.Critical),
              snapshot.Alerts.Count(a => a.Severity == AlertSeverity.Warning),
              new OverviewEtcdDto(
                  snapshot.Etcd.Reachable,
                  snapshot.Etcd.Endpoints.Count(e => e.Reachable),
                  snapshot.Etcd.Endpoints.Count),
              [],
              [],
              Math.Max(0L, (long)Math.Round(age.TotalMilliseconds)),
              age > TimeSpan.FromSeconds(SnapshotStaleRule.Multiplier * refreshIntervalSeconds));
      }
  }

  // Хендлер: store → отказ «снапшота нет» или маппер (spec §3.12).
  [InjectAsScoped]
  public sealed class OverviewQueryHandler(
      ISnapshotStore store,
      TimeProvider time,
      IOptions<EtcdOptions> etcdOptions) : IQueryHandler<OverviewQuery, OverviewDto>
  {
      public ValueTask<Result<OverviewDto>> Handle(OverviewQuery query, CancellationToken ct)
      {
          var snapshot = store.Current;
          return ValueTask.FromResult(snapshot is null
              ? Result<OverviewDto>.Failed(new InspectionModule.SnapshotNotReadyException())
              : Result<OverviewDto>.Success(OverviewMapper.Map(
                  snapshot, time.GetUtcNow(), EffectiveInterval(etcdOptions))));
      }

      // Эффективный интервал: fallback 3 c при опечатке конфига — как в refresher (t03 §3.3).
      private static double EffectiveInterval(IOptions<EtcdOptions> options)
          => options.Value.RefreshIntervalSeconds > 0
              ? options.Value.RefreshIntervalSeconds
              : 3;
  }
  ```

  Создать `src/AdminPanel.Api/Inspection/EtcdStatusQuery.cs`:

  ```csharp
  using AdminPanel.Core;
  using AdminPanel.Core.Alerting.Rules;
  using AdminPanel.Infrastructure;
  using AdminPanel.Infrastructure.CQRS;
  using AdminPanel.Infrastructure.DI;

  namespace AdminPanel.Api.Inspection;

  // Запрос статуса кластера etcd (arch/03 §1 GET /api/etcd/status).
  public sealed record EtcdStatusQuery : IQuery<EtcdStatusDto>;

  // Ответ GET /api/etcd/status (arch/03 §2; id — decimal-строки, spec §3.11).
  public sealed record EtcdStatusDto(
      IReadOnlyList<EtcdEndpointDto> Endpoints,
      IReadOnlyList<EtcdMemberDto> Members,
      IReadOnlyList<EtcdAlarmDto> Alarms,
      bool QuorumSuspected,
      DateTimeOffset LastRefreshUtc);

  public sealed record EtcdEndpointDto(
      string Url,
      bool Reachable,
      double? LatencyMs,
      string? Version,
      long? DbSizeBytes,
      string? LeaderMemberId,
      ulong? RaftTerm,
      IReadOnlyList<string> Errors,
      bool Active);

  public sealed record EtcdMemberDto(
      string Id, string? Name, IReadOnlyList<string> PeerUrls, IReadOnlyList<string> ClientUrls, bool IsLeader);

  public sealed record EtcdAlarmDto(string MemberId, string Type);

  // EtcdStatus → DTO: чистая функция (spec §6.2, §3.14).
  public static class EtcdStatusMapper
  {
      public static EtcdStatusDto Map(EtcdStatus etcd)
      {
          // Лидер: первый живой endpoint с валидным leader > 0, иначе первый любой не-null.
          var leaderId = etcd.Endpoints
              .FirstOrDefault(e => e.Reachable && e.LeaderMemberId is > 0)?.LeaderMemberId
              ?? etcd.Endpoints.FirstOrDefault(e => e.LeaderMemberId is > 0)?.LeaderMemberId;
          return new EtcdStatusDto(
              [.. etcd.Endpoints.Select(e => new EtcdEndpointDto(
                  e.Url,
                  e.Reachable,
                  e.LatencyMs,
                  e.Version,
                  e.DbSizeBytes,
                  e.LeaderMemberId?.ToString(),
                  e.RaftTerm,
                  e.Errors,
                  e.Url == etcd.ActiveEndpoint))],
              [.. etcd.Members.Select(m => new EtcdMemberDto(
                  m.Id.ToString(),
                  m.Name,
                  m.PeerUrls,
                  m.ClientUrls,
                  leaderId is not null && m.Id == leaderId))],
              [.. etcd.Alarms.Select(a => new EtcdAlarmDto(
                  a.MemberId.ToString(),
                  EtcdAlarmRule.AlarmTypeName(a.Type)))],
              etcd.QuorumSuspected,
              etcd.LastRefreshUtc);
      }
  }

  // Хендлер: store → отказ «снапшота нет» или маппер (spec §3.12).
  [InjectAsScoped]
  public sealed class EtcdStatusQueryHandler(ISnapshotStore store)
      : IQueryHandler<EtcdStatusQuery, EtcdStatusDto>
  {
      public ValueTask<Result<EtcdStatusDto>> Handle(EtcdStatusQuery query, CancellationToken ct)
      {
          var snapshot = store.Current;
          return ValueTask.FromResult(snapshot is null
              ? Result<EtcdStatusDto>.Failed(new InspectionModule.SnapshotNotReadyException())
              : Result<EtcdStatusDto>.Success(EtcdStatusMapper.Map(snapshot.Etcd)));
      }
  }
  ```

  Создать `src/AdminPanel.Api/Inspection/AlertsQuery.cs`:

  ```csharp
  using AdminPanel.Core;
  using AdminPanel.Infrastructure;
  using AdminPanel.Infrastructure.CQRS;
  using AdminPanel.Infrastructure.DI;

  namespace AdminPanel.Api.Inspection;

  // Запрос ленты алертов с фильтрами (arch/03 §1; severity уже провалидирован эндпоинтом).
  public sealed record AlertsQuery(AlertSeverity? Severity, string? Kind) : IQuery<IReadOnlyList<AlertDto>>;

  // Ответ: один алерт (arch/03 §2; severity — строчная строка, spec §3.11).
  public sealed record AlertDto(
      string Id,
      string Severity,
      string Kind,
      string Target,
      string Message,
      IReadOnlyDictionary<string, string>? Details,
      long? SinceUnix);

  // Core → DTO + фильтры: чистые функции (spec §6.2).
  public static class AlertsMapper
  {
      public static IReadOnlyList<AlertDto> Map(IReadOnlyList<Alert> alerts)
          => [.. alerts.Select(ToDto)];

      public static AlertDto ToDto(Alert alert)
          => new(
              alert.Id,
              SeverityName(alert.Severity),
              alert.Kind,
              alert.Target,
              alert.Message,
              alert.Details,
              alert.SinceUnix);

      // Фильтры до маппинга: severity и kind — точные совпадения (spec §3.13).
      public static IReadOnlyList<Alert> ApplyFilters(
          IReadOnlyList<Alert> alerts, AlertSeverity? severity, string? kind)
          => [.. alerts
              .Where(a => severity is null || a.Severity == severity)
              .Where(a => kind is null || a.Kind == kind)];

      private static string SeverityName(AlertSeverity severity)
          => severity switch
          {
              AlertSeverity.Critical => "critical",
              AlertSeverity.Warning => "warning",
              _ => "info",
          };
  }

  // Хендлер: store → отказ «снапшота нет» или фильтры+маппер (spec §3.12).
  [InjectAsScoped]
  public sealed class AlertsQueryHandler(ISnapshotStore store)
      : IQueryHandler<AlertsQuery, IReadOnlyList<AlertDto>>
  {
      public ValueTask<Result<IReadOnlyList<AlertDto>>> Handle(AlertsQuery query, CancellationToken ct)
      {
          var snapshot = store.Current;
          return ValueTask.FromResult(snapshot is null
              ? Result<IReadOnlyList<AlertDto>>.Failed(new InspectionModule.SnapshotNotReadyException())
              : Result<IReadOnlyList<AlertDto>>.Success(
                  AlertsMapper.Map(AlertsMapper.ApplyFilters(snapshot.Alerts, query.Severity, query.Kind))));
      }
  }
  ```

  Создать `src/AdminPanel.Api/Inspection/InspectionModule.cs`:

  ```csharp
  using AdminPanel.Core;
  using AdminPanel.Infrastructure;
  using AdminPanel.Infrastructure.CQRS;
  using Microsoft.AspNetCore.Builder;
  using Microsoft.AspNetCore.Http;
  using Microsoft.AspNetCore.Routing;

  namespace AdminPanel.Api.Inspection;

  // Композиция эндпоинтов инспекции etcd из снапшота (arch/03 §1; auth-guard уже закрыл /api/*).
  public static class InspectionModule
  {
      // До первого тика снапшота нет (t03 §3.13): хендлеры возвращают этот отказ → 503 (spec §3.12).
      public sealed class SnapshotNotReadyException() : Exception("etcd-снапшот ещё не собран");

      // GET /api/overview | /api/etcd/status | /api/alerts (arch/03 §1, spec §6.1).
      public static IEndpointRouteBuilder MapInspectionApi(this IEndpointRouteBuilder endpoints)
      {
          endpoints.MapGet("/api/overview", async (IHandler handler, CancellationToken ct) =>
          {
              var result = await handler.HandleQuery<OverviewQuery, OverviewDto>(new OverviewQuery(), ct);
              return ResultToHttp(result);
          });

          endpoints.MapGet("/api/etcd/status", async (IHandler handler, CancellationToken ct) =>
          {
              var result = await handler.HandleQuery<EtcdStatusQuery, EtcdStatusDto>(new EtcdStatusQuery(), ct);
              return ResultToHttp(result);
          });

          endpoints.MapGet("/api/alerts", async (string? severity, string? kind, IHandler handler, CancellationToken ct) =>
          {
              // Валидация до query: строго critical|warning|info, иначе 400 (spec §3.13).
              AlertSeverity? parsed = null;
              if (severity is not null)
              {
                  if (!SeverityNames.TryGetValue(severity, out var value))
                      return Results.Problem(
                          statusCode: StatusCodes.Status400BadRequest,
                          title: "Invalid severity",
                          detail: $"severity должен быть critical|warning|info, получено: {severity}");
                  parsed = value;
              }

              var result = await handler.HandleQuery<AlertsQuery, IReadOnlyList<AlertDto>>(
                  new AlertsQuery(parsed, kind), ct);
              return ResultToHttp(result);
          });

          return endpoints;
      }

      // Допустимые значения ?severity= — строчный канон arch/03 §1.
      private static readonly Dictionary<string, AlertSeverity> SeverityNames = new()
      {
          ["critical"] = AlertSeverity.Critical,
          ["warning"] = AlertSeverity.Warning,
          ["info"] = AlertSeverity.Info,
      };

      // Общий маппинг Result → HTTP: успех 200; отказ хендлера — 503 ProblemDetails (spec §3.12).
      private static IResult ResultToHttp<T>(Result<T> result)
          => result.IsSuccess
              ? Results.Ok(result.Value)
              : Results.Problem(
                  statusCode: StatusCodes.Status503ServiceUnavailable,
                  title: "Snapshot not ready",
                  detail: result.Error!.Message);
  }
  ```

  В `src/AdminPanel.Api/Program.cs`: добавить using `using AdminPanel.Api.Inspection;` (после `using AdminPanel.Api.Auth;`) и строку после `app.MapAuthApi();`:

  ```csharp
  app.MapInspectionApi(); // [t04] эндпоинты инспекции etcd из снапшота (arch/03 §1)
  ```

  Выход: три query-файла + модуль + 2 строки Program.cs; хендлеры подхватятся существующим `AddApi()` (attribute-DI).

  Проверка: `dotnet build src/AdminPanel.slnx 2>&1 | tail -3` → успех, 0 warnings. Затем `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj --filter "FullyQualifiedName~InspectionMappersTests|FullyQualifiedName~InspectionQueryHandlerTests" 2>&1 | tail -3` → PASS: **Passed: 17, Failed: 0**.

- [ ] **Шаг 4: Полная unit-регрессия и коммит**

  Проверка: `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj 2>&1 | tail -3` → `Passed! - Failed: 0, Passed: 120, Skipped: 0, Total: 120` (83 базовых кейса, включая 4 InlineData Theory-кейса AdminAuthenticatorTests, + 18 AlertEngineTests + 2 SnapshotRefresherTests + 17 Inspection-тестов).

  Коммит:

  ```bash
  git add src/AdminPanel.Api/Inspection/ src/AdminPanel.Api/Program.cs src/tests/AdminPanel.UnitTests/InspectionMappersTests.cs src/tests/AdminPanel.UnitTests/InspectionQueryHandlerTests.cs
  git commit -m "t04: API инспекции — /api/overview, /api/etcd/status, /api/alerts (unit)"
  ```

- [ ] **Шаг 5: Контрольная сверка счётчика (при расхождении)**

  Действие: если итог Шага 4 отличается от 120 — сверить состав: `grep -c "\[Fact\]" src/tests/AdminPanel.UnitTests/AlertEngineTests.cs src/tests/AdminPanel.UnitTests/SnapshotRefresherTests.cs src/tests/AdminPanel.UnitTests/InspectionMappersTests.cs src/tests/AdminPanel.UnitTests/InspectionQueryHandlerTests.cs` → ожидание 18 / 8 / 12 / 5; найти пропущенный/лишний тест до старта Task 5.

---

### Task 5: Integration-тесты API (фабрика "api" + живой etcd)

**Files:**
- Modify: `src/tests/AdminPanel.IntegrationTests/AuthTests.cs` (правка `AuthWebFactory`)
- Create: `src/tests/AdminPanel.IntegrationTests/InspectionApiTests.cs` (внутри также `TestSnapshotStore`, `InspectionSnapshots`, `ApiTestLogin`, `InspectionSeededAnomaliesApiTests` — мутирующий сценарий со своим контейнером)

**Interfaces:**
- Consumes: `AuthWebFactory`/коллекция `"api"` (t02), `EtcdContainerFixture`/`EtcdSeed`/`EtcdTestHarness` (t03, Task 3), Program-хост t01–t04.
- Produces: `AuthWebFactory.Snapshot` (управляемый снапшот хоста, `EtcdSnapshot?`); `TestSnapshotStore : ISnapshotStore`; helper'ы для будущих API-тестов t05/t06: `ApiTestLogin.LoginAsync(AuthWebFactory) → Task<HttpClient>`, `InspectionSnapshots.Fixture(DateTimeOffset builtAt) → EtcdSnapshot`.

- [ ] **Шаг 1: Правка фабрики (hosted off, store подменён)**

  Вход: Task 4 слит; `AuthTests.cs` — текущая `AuthWebFactory`.

  Действие: в `src/tests/AdminPanel.IntegrationTests/AuthTests.cs`:

  1) Добавить usings (вверх файла, к существующим):

  ```csharp
  using AdminPanel.Core;
  using AdminPanel.Etcd;
  using Microsoft.Extensions.Hosting;
  ```

  2) В `ConfigureWebHost` заменить блок `ConfigureTestServices` и добавить акцессор — итоговый вид фабрики:

  ```csharp
  // Единая на сборку фабрика (collection fixture "api"): статический кеш сборок
  // attribute-DI не допускает второй хост в процессе (spec t02 §14, §10).
  public sealed class AuthWebFactory : WebApplicationFactory<Program>
  {
      public FixedTimeProvider Time { get; } = new();

      protected override void ConfigureWebHost(IWebHostBuilder builder)
      {
          // http-стенд: без AllowHttp Secure-cookie не вернётся по http (spec t02 §10, §14).
          builder.UseSetting("AdminPanel:Auth:Username", "admin");
          builder.UseSetting("AdminPanel:Auth:Password", "adminpw");
          builder.UseSetting("AdminPanel:Auth:AllowHttp", "true");

          // Подмена времени ПОСЛЕ композиции Program (ConfigureTestServices):
          // singleton-лимитер живёт на управляемом времени фабрики.
          builder.ConfigureTestServices(services =>
          {
              services.Replace(ServiceDescriptor.Singleton(typeof(TimeProvider), Time));

              // t04: hosted не стартуют — тики refresher'а перезатирали бы тестовые снапшоты (spec §3.16);
              // снапшот общего хоста под контролем тестов через TestSnapshotStore.
              services.RemoveAll<IHostedService>();
              services.Replace(typeof(ISnapshotStore), new TestSnapshotStore());
          });
      }

      // t04: снапшот хоста под контролем тестов инспекции (spec §3.16).
      public EtcdSnapshot? Snapshot
      {
          get => Store.Current;
          set => Store.Current = value;
      }

      private TestSnapshotStore Store => (TestSnapshotStore)Services.GetRequiredService<ISnapshotStore>();
  }
  ```

  Выход: фабрика с отключёнными hosted-сервисами и подменённым store; `AuthTests`/`HealthzTests` не затронуты (guard/cookie/self-чек не зависят от refresher'а).

  Проверка: `dotnet build src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj 2>&1 | tail -3` → FAIL: `TestSnapshotStore` не найден — исправляется на шаге 2 (красная фаза).

- [ ] **Шаг 2: HTTP-контрактные тесты (фailing → зелёные вместе с шагом 3)**

  Действие: создать `src/tests/AdminPanel.IntegrationTests/InspectionApiTests.cs`:

  ```csharp
  using System.Net;
  using System.Net.Http.Json;
  using System.Text.Json;
  using AdminPanel.Core;
  using AdminPanel.Etcd;
  using FluentAssertions;
  using Microsoft.AspNetCore.Mvc.Testing;
  using Xunit;

  namespace AdminPanel.IntegrationTests;

  // Управляемое хранилище фабрики "api": тест ставит снапшот сам (spec §3.16).
  public sealed class TestSnapshotStore : ISnapshotStore
  {
      public EtcdSnapshot? Current { get; set; }

      public void Replace(EtcdSnapshot snapshot) => Current = snapshot;
  }

  // Логин в общем хосте фабрики: свежее окно rate-limiter'а + cookie в клиенте.
  internal static class ApiTestLogin
  {
      public static async Task<HttpClient> LoginAsync(AuthWebFactory factory)
      {
          factory.Time.Utc += TimeSpan.FromSeconds(61);
          var client = factory.CreateClient();
          var login = await client.PostAsJsonAsync(
              "/api/auth/login",
              new { username = "admin", password = "adminpw" },
              TestContext.Current.CancellationToken);
          login.StatusCode.Should().Be(HttpStatusCode.NoContent);
          return client;
      }
  }

  // Фикстурный снапшот HTTP-тестов (spec §9.1): 1 живой + 1 мёртвый endpoint,
  // member-лидер, alarm NOSPACE, 1 critical + 2 warning алерта.
  internal static class InspectionSnapshots
  {
      public static EtcdSnapshot Fixture(DateTimeOffset builtAt)
      {
          var etcd = new EtcdStatus(
              true,
              [
                  new EtcdEndpoint("http://etcd1:2379", true, 4.2, "3.5.21", 20480, 42, 17, 3, []),
                  new EtcdEndpoint("http://etcd2:2379", false, null, null, null, null, null, null, ["connection refused"]),
              ],
              [new EtcdMember(42, "etcd1", ["http://etcd1:2380"], ["http://etcd1:2379"])],
              [new EtcdAlarm(42, EtcdAlarmType.NoSpace)],
              "http://etcd1:2379",
              false,
              builtAt,
              0);
          return new EtcdSnapshot(
              builtAt,
              etcd,
              [],
              [],
              [],
              [],
              [
                  new Alert("etcd-alarm:42:nospace", AlertSeverity.Critical, "etcd-alarm", "42:nospace",
                      "тревога etcd NOSPACE на member 42", new Dictionary<string, string> { ["memberId"] = "42" }, null),
                  new Alert("etcd-endpoint-down:http://etcd2:2379", AlertSeverity.Warning, "etcd-endpoint-down",
                      "http://etcd2:2379", "endpoint etcd недоступен", new Dictionary<string, string> { ["errors"] = "connection refused" }, null),
                  new Alert("key-malformed:/x", AlertSeverity.Warning, "key-malformed", "/x",
                      "ключ не разобран", null, null),
              ],
              [],
              0);
      }
  }

  // HTTP-контракт инспекционных эндпоинтов: 401/503/200/400/фильтры (spec §9.1).
  [Collection("api")]
  public class InspectionApiTests
  {
      private readonly AuthWebFactory _factory;

      public InspectionApiTests(AuthWebFactory factory) => _factory = factory;

      private Task<HttpClient> LoginAsync() => ApiTestLogin.LoginAsync(_factory);

      private async Task<JsonElement> GetJsonAsync(HttpClient client, string url)
      {
          using var response = await client.GetAsync(url, TestContext.Current.CancellationToken);
          response.StatusCode.Should().Be(HttpStatusCode.OK);
          return await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
      }

      [Fact]
      public async Task Endpoints_WithoutCookie_Return401()
      {
          // Arrange
          using var client = _factory.CreateClient(new WebApplicationFactoryClientOptions { AllowAutoRedirect = false });

          // Act
          var overview = await client.GetAsync("/api/overview", TestContext.Current.CancellationToken);
          var status = await client.GetAsync("/api/etcd/status", TestContext.Current.CancellationToken);
          var alerts = await client.GetAsync("/api/alerts", TestContext.Current.CancellationToken);

          // Assert: default-deny guard закрыл новые эндпоинты без правок auth.
          overview.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
          status.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
          alerts.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
      }

      [Fact]
      public async Task Endpoints_NoSnapshot_Return503ProblemDetails()
      {
          // Arrange: до первого тика снапшота нет (t03 §3.13).
          _factory.Snapshot = null;
          using var client = await LoginAsync();

          // Act
          var overview = await client.GetAsync("/api/overview", TestContext.Current.CancellationToken);
          var status = await client.GetAsync("/api/etcd/status", TestContext.Current.CancellationToken);
          var alerts = await client.GetAsync("/api/alerts", TestContext.Current.CancellationToken);

          // Assert: 503 ProblemDetails на всех трёх эндпоинтах (spec §9.1).
          overview.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
          overview.Content.Headers.ContentType!.MediaType.Should().Be("application/problem+json");
          var body = await overview.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
          body.GetProperty("title").GetString().Should().Be("Snapshot not ready");
          status.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
          alerts.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
      }

      [Fact]
      public async Task Overview_WithSnapshot_ReturnsDto()
      {
          // Arrange: BuiltAtUtc = фиксированное время фабрики → возраст 0.
          _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.Utc);
          using var client = await LoginAsync();

          // Act
          var dto = await GetJsonAsync(client, "/api/overview");

          // Assert
          dto.GetProperty("alertsCritical").GetInt32().Should().Be(1);
          dto.GetProperty("alertsWarning").GetInt32().Should().Be(2);
          dto.GetProperty("stale").GetBoolean().Should().BeFalse();
          dto.GetProperty("snapshotAgeMs").GetInt64().Should().Be(0);
          var etcd = dto.GetProperty("etcd");
          etcd.GetProperty("reachable").GetBoolean().Should().BeTrue();
          etcd.GetProperty("endpointsOk").GetInt32().Should().Be(1);
          etcd.GetProperty("endpointsTotal").GetInt32().Should().Be(2);
          dto.GetProperty("clusters").GetArrayLength().Should().Be(0);
          dto.GetProperty("activeMoves").GetArrayLength().Should().Be(0);
      }

      [Fact]
      public async Task Overview_StaleSnapshot_StaleTrue()
      {
          // Arrange: возраст 12 c > порога 3×3 c.
          _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.Utc - TimeSpan.FromSeconds(12));
          using var client = await LoginAsync();

          // Act
          var dto = await GetJsonAsync(client, "/api/overview");

          // Assert
          dto.GetProperty("stale").GetBoolean().Should().BeTrue();
          dto.GetProperty("snapshotAgeMs").GetInt64().Should().Be(12000);
      }

      [Fact]
      public async Task EtcdStatus_WithSnapshot_ReturnsEndpointsMembersAlarms()
      {
          // Arrange
          _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.Utc);
          using var client = await LoginAsync();

          // Act
          var dto = await GetJsonAsync(client, "/api/etcd/status");

          // Assert
          var endpoints = dto.GetProperty("endpoints");
          endpoints.GetArrayLength().Should().Be(2);
          var first = endpoints[0];
          first.GetProperty("url").GetString().Should().Be("http://etcd1:2379");
          first.GetProperty("reachable").GetBoolean().Should().BeTrue();
          first.GetProperty("active").GetBoolean().Should().BeTrue();
          first.GetProperty("version").GetString().Should().Be("3.5.21");
          first.GetProperty("leaderMemberId").GetString().Should().Be("42");
          first.GetProperty("raftTerm").GetInt64().Should().Be(3);
          endpoints[1].GetProperty("active").GetBoolean().Should().BeFalse();
          endpoints[1].GetProperty("errors")[0].GetString().Should().Be("connection refused");
          var member = dto.GetProperty("members")[0];
          member.GetProperty("id").GetString().Should().Be("42");
          member.GetProperty("name").GetString().Should().Be("etcd1");
          member.GetProperty("isLeader").GetBoolean().Should().BeTrue();
          var alarm = dto.GetProperty("alarms")[0];
          alarm.GetProperty("memberId").GetString().Should().Be("42");
          alarm.GetProperty("type").GetString().Should().Be("nospace");
          dto.GetProperty("quorumSuspected").GetBoolean().Should().BeFalse();
          dto.GetProperty("lastRefreshUtc").GetString().Should().NotBeNullOrEmpty();
      }

      [Fact]
      public async Task Alerts_WithSnapshot_ReturnAllSorted()
      {
          // Arrange
          _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.Utc);
          using var client = await LoginAsync();

          // Act
          var alerts = await GetJsonAsync(client, "/api/alerts");

          // Assert: severity desc, внутри уровня — kind (Ordinal); sinceUnix null виден как null.
          alerts.GetArrayLength().Should().Be(3);
          alerts[0].GetProperty("id").GetString().Should().Be("etcd-alarm:42:nospace");
          alerts[0].GetProperty("severity").GetString().Should().Be("critical");
          alerts[1].GetProperty("kind").GetString().Should().Be("etcd-endpoint-down");
          alerts[2].GetProperty("kind").GetString().Should().Be("key-malformed");
          alerts[0].GetProperty("sinceUnix").ValueKind.Should().Be(JsonValueKind.Null);
      }

      [Fact]
      public async Task Alerts_SeverityFilter_ReturnsOnlyMatching()
      {
          // Arrange
          _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.Utc);
          using var client = await LoginAsync();

          // Act
          var critical = await GetJsonAsync(client, "/api/alerts?severity=critical");
          var warning = await GetJsonAsync(client, "/api/alerts?severity=warning");

          // Assert
          critical.GetArrayLength().Should().Be(1);
          critical[0].GetProperty("severity").GetString().Should().Be("critical");
          warning.GetArrayLength().Should().Be(2);
      }

      [Fact]
      public async Task Alerts_KindFilter_ReturnsOnlyMatching()
      {
          // Arrange
          _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.Utc);
          using var client = await LoginAsync();

          // Act
          var alerts = await GetJsonAsync(client, "/api/alerts?kind=etcd-endpoint-down");

          // Assert
          alerts.GetArrayLength().Should().Be(1);
          alerts[0].GetProperty("kind").GetString().Should().Be("etcd-endpoint-down");
      }

      [Fact]
      public async Task Alerts_UnknownKind_ReturnsEmpty200()
      {
          // Arrange
          _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.Utc);
          using var client = await LoginAsync();

          // Act
          var alerts = await GetJsonAsync(client, "/api/alerts?kind=nope");

          // Assert: kind'ы эволюционируют между задачами — пустой список, не 400 (spec §3.13).
          alerts.GetArrayLength().Should().Be(0);
      }

      [Fact]
      public async Task Alerts_InvalidSeverity_Returns400ProblemDetails()
      {
          // Arrange
          _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.Utc);
          using var client = await LoginAsync();

          // Act
          var response = await client.GetAsync("/api/alerts?severity=bogus", TestContext.Current.CancellationToken);

          // Assert
          response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
          response.Content.Headers.ContentType!.MediaType.Should().Be("application/problem+json");
      }

      [Fact]
      public async Task Alerts_BothFilters_Combine()
      {
          // Arrange
          _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.Utc);
          using var client = await LoginAsync();

          // Act
          var alerts = await GetJsonAsync(client, "/api/alerts?severity=warning&kind=key-malformed");

          // Assert
          alerts.GetArrayLength().Should().Be(1);
          alerts[0].GetProperty("kind").GetString().Should().Be("key-malformed");
      }
  }
  ```

  Выход: 11 контрактных тестов + инфраструктура фабрики.

  Проверка: `dotnet build src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj 2>&1 | tail -3` → успех, 0 warnings.

- [ ] **Шаг 3: Прогон контрактных тестов (без Docker)**

  Действие: `dotnet test src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj --filter "FullyQualifiedName~InspectionApiTests|FullyQualifiedName~AuthTests|FullyQualifiedName~HealthzTests" 2>&1 | tail -3` → PASS: **Passed: 21, Failed: 0** (11 новых + 9 AuthTests + 1 HealthzTests; Docker не нужен — контейнерных фикстур в фильтре нет). При падении AuthTests — проверить, что правка фабрики не задела логин-настройки.

- [ ] **Шаг 4: Живой-etcd смок (два класса) и коммит**

  Действие: дописать в конец `src/tests/AdminPanel.IntegrationTests/InspectionApiTests.cs` два класса пути данных (spec §3.17, §9.2) — чистый и мутирующий сценарии в разных классах с собственными контейнерами:

  ```csharp
  // Путь данных «живой etcd → API» (spec §3.17): реальный refresher (EtcdTestHarness t03 + AlertEngine)
  // строит снапшот против контейнера, снапшот переносится в TestSnapshotStore хоста.
  // Только НЕмутирующие проверки чистого сида: мутирующий сценарий — отдельный класс ниже
  // (порядок выполнения тестов xunit не гарантирован).
  [Collection("api")]
  public class InspectionEtcdApiTests(AuthWebFactory factory, EtcdContainerFixture fixture)
      : IClassFixture<EtcdContainerFixture>
  {
      private readonly AuthWebFactory _factory = factory;

      [Fact]
      public async Task LiveEtcd_Endpoints_ReflectRealSnapshot()
      {
          // Arrange
          var store = new SnapshotStore();
          var refresher = EtcdTestHarness.NewRefresher(store, fixture.Endpoint);
          (await refresher.RefreshOnceAsync(CancellationToken.None)).IsSuccess.Should().BeTrue();
          _factory.Snapshot = store.Current;
          using var client = await ApiTestLogin.LoginAsync(_factory);

          // Act
          using var status = await client.GetAsync("/api/etcd/status", TestContext.Current.CancellationToken);
          var overview = await client.GetAsync("/api/overview", TestContext.Current.CancellationToken);
          var alerts = await client.GetAsync("/api/alerts", TestContext.Current.CancellationToken);

          // Assert: чистый сид demo — etcd жив, алертов нет.
          status.StatusCode.Should().Be(HttpStatusCode.OK);
          var etcd = await status.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
          etcd.GetProperty("endpoints")[0].GetProperty("version").GetString().Should().Be("3.5.21");
          etcd.GetProperty("members")[0].GetProperty("name").GetString().Should().Be("test");
          var overviewDto = await overview.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
          overviewDto.GetProperty("etcd").GetProperty("reachable").GetBoolean().Should().BeTrue();
          overviewDto.GetProperty("etcd").GetProperty("endpointsOk").GetInt32().Should().Be(1);
          (await alerts.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken))
              .GetArrayLength().Should().Be(0);
      }

  }

  // Сценарий с мутацией сида (kv/put без удаления — необратим для контейнера): отдельный класс
  // со СВОИМ контейнером (IClassFixture — экземпляр на класс), чтобы строгий «чистый» тест выше
  // не зависел от порядка выполнения (прецедент t03 — EtcdFailureTests с собственным fixture).
  [Collection("api")]
  public class InspectionSeededAnomaliesApiTests(AuthWebFactory factory, EtcdContainerFixture fixture)
      : IClassFixture<EtcdContainerFixture>
  {
      private readonly AuthWebFactory _factory = factory;

      [Fact]
      public async Task LiveEtcd_SeededAnomalies_ProduceAlerts()
      {
          // Arrange: аномалии засеяны ДО первого тика → previous = null → sinceUnix null (spec §3.4, §9.2).
          var store = new SnapshotStore();
          var refresher = EtcdTestHarness.NewRefresher(store, fixture.Endpoint);
          await EtcdSeed.PutAsync(
              fixture.Endpoint, "/clusters/demo/buckets/status/bucket_1", "not json", CancellationToken.None);
          await EtcdSeed.PutAsync(
              fixture.Endpoint, "/clusters/ghost/shards/g1/dsn", "host=g1 port=5432", CancellationToken.None);
          (await refresher.RefreshOnceAsync(CancellationToken.None)).IsSuccess.Should().BeTrue();
          _factory.Snapshot = store.Current;
          using var client = await ApiTestLogin.LoginAsync(_factory);

          // Act
          using var response = await client.GetAsync("/api/alerts", TestContext.Current.CancellationToken);

          // Assert: оба warning; "cluster-incomplete" < "key-malformed" по Ordinal.
          response.StatusCode.Should().Be(HttpStatusCode.OK);
          var alerts = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
          alerts.GetArrayLength().Should().Be(2);
          alerts[0].GetProperty("kind").GetString().Should().Be("cluster-incomplete");
          alerts[0].GetProperty("target").GetString().Should().Be("ghost");
          alerts[1].GetProperty("kind").GetString().Should().Be("key-malformed");
          alerts[1].GetProperty("sinceUnix").ValueKind.Should().Be(JsonValueKind.Null);
      }
  }
  ```

  Проверка (нужен Docker): `dotnet test src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj --filter "FullyQualifiedName~InspectionEtcdApiTests|FullyQualifiedName~InspectionSeededAnomaliesApiTests" 2>&1 | tail -3` → PASS: **Passed: 2, Failed: 0** (сценарии изолированы собственными контейнерами — порядок не важен). Полный прогон: `dotnet test src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj 2>&1 | tail -3` → `Passed! - Failed: 0, Passed: 32, Skipped: 0, Total: 32` (19 существующих + 11 + 2).

  Коммит:

  ```bash
  git add src/tests/AdminPanel.IntegrationTests/AuthTests.cs src/tests/AdminPanel.IntegrationTests/InspectionApiTests.cs
  git commit -m "t04: integration — 401/503/200/400 HTTP-контракт + живой etcd смок"
  ```

---

### Task 6: Roadmap-деливерабл + финальная верификация + финальный коммит

**Files:**
- Modify: `arch/roadmap/etcd.md` (удалить пункт `t04-etcd-api`)
- Modify: git (коммит spec + plan + roadmap)

**Interfaces:**
- Consumes: всё выше.
- Produces: roadmap без пункта t04; финальное состояние ветки `feat-t04-etcd-api`, готовое к ревью и мержу.

- [ ] **Шаг 1: Удалить пункт t04 из roadmap**

  Вход: `arch/roadmap/etcd.md` содержит единственный пункт `t04-etcd-api` (строки после заголовка «## Задачи»).

  Действие: удалить из `arch/roadmap/etcd.md` весь блок пункта — строки от `- \`t04-etcd-api\` ← \`t02-auth\`, \`t03-etcd-snapshot\` — API инспекции etcd и` до строки `смоук API против Testcontainers-etcd.` включительно (9 строк, spec §14). После удаления раздел «## Задачи» остаётся пустым — это корректно (трек закрыт; история — в git и docs/superpowers/, никаких пометок «закрыта» — правила `arch/roadmap/README.md`).

  Выход: `arch/roadmap/etcd.md` — шапка трека + контекст + пустой раздел задач.

  Проверка: `grep -n "t04-etcd-api" arch/roadmap/*.md` → ровно 3 строки-вхождения: `sharding.md` (t05 ← t04), `ha.md` (t06 ← t04), `frontend.md` (t07 ← t04); в `etcd.md` — **ни одного** (анкер: зависимости не трогаем, spec §14). `git diff --stat` — изменён только `etcd.md`.

- [ ] **Шаг 2: Финальная верификация (критерии приёмки spec §15)**

  Вход: все задачи слиты.

  Действие и проверка (каждая команда — зелёная):

  1. `dotnet build src/AdminPanel.slnx 2>&1 | tail -3` → `Build succeeded` / `0 Warning(s)` / `0 Error(s)` (критерий §15.1).
  2. `dotnet test src/AdminPanel.slnx 2>&1 | tail -3` (нужен Docker) → `Passed! - Failed: 0, Passed: 152, Skipped: 0, Total: 152` (120 unit + 32 integration; критерий §15.2).
  3. `grep -rn "PackageReference" src/ --include="*.csproj"` → состав пакетов идентичен t03 (Testcontainers/Mvc.Testing/FluentAssertions/xunit.v3/coverlet/TestSdk; критерий §15.7 — новых нет).
  4. `grep -rn "v3/kv/put\|v3/lease" src/AdminPanel.Api src/AdminPanel.Core src/AdminPanel.Etcd src/AdminPanel.Infrastructure src/AdminPanel.Probes --include="*.cs"` → пусто: панель не пишет в etcd — прод-сборки содержат только читающие пути `/v3/*` (критерий §15.8; `kv/put` легитимно живёт лишь в тестовом `EtcdSeed` — сборка tests/ в grep не входит).
  5. Ручная сверка каталога алертов: kind'ы из spec §4.2 присутствуют, лишних нет: `grep -rn "KindName = \"" src/AdminPanel.Core/Alerting/Rules/ | wc -l` → 7.

  Выход: все критерии приёмки подтверждены выводами команд.

- [ ] **Шаг 3: Финальный коммит (roadmap + spec + plan)**

  Вход: верификация шага 2 зелёная.

  Действие:

  ```bash
  git add arch/roadmap/etcd.md docs/superpowers/2026-08-22-t04-etcd-api/spec.md docs/superpowers/2026-08-22-t04-etcd-api/plan.md
  git commit -m "t04: spec/plan задачи + roadmap-деливерабл (удаление пункта t04-etcd-api)"
  ```

  Выход: ветка `feat-t04-etcd-api` готова к ревью dev-flow; дальнейшие действия (ревью, мерж, пуш) — вне плана, по команде координатора.

  Проверка: `git log --oneline -7` → 6 коммитов t04 (Tasks 1–6) поверх мержа t03; `git status --porcelain` → пусто.

---

## Сводка задач

| # | Задача | Тесты (новые) | Коммит |
|---|---|---|---|
| 1 | Каркас AlertEngine + TestSnapshots | 6 unit | `t04: AlertEngine — каркас правил алертов: id/sinceUnix/сортировка (unit)` |
| 2 | 7 etcd-правил + AlertTestRules | 12 unit | `t04: etcd-правила каталога алертов 03 §4 — 7 kind'ов (unit)` |
| 3 | SnapshotRefresher: Alerts на обоих путях тика | 2 unit + integration-ассерты | `t04: SnapshotRefresher — Alerts на обоих путях тика (unit+integration)` |
| 4 | InspectionModule + 3 query/DTO/маппера + Program.cs | 17 unit | `t04: API инспекции — /api/overview, /api/etcd/status, /api/alerts (unit)` |
| 5 | Фабрика "api": hosted-off + TestSnapshotStore; HTTP-контракт + живой etcd | 13 integration | `t04: integration — 401/503/200/400 HTTP-контракт + живой etcd смок` |
| 6 | Roadmap-деливерабл + финальная верификация | — | `t04: spec/plan задачи + roadmap-деливерабл (удаление пункта t04-etcd-api)` |

Контрольные счётчики прогонов (полные итоговые строки `dotnet test`):
- После Task 1: unit `Passed: 89`; после Task 2: `Passed: 101`; после Task 3: unit `Passed: 103` (по классу SnapshotRefresherTests `Passed: 8`), integration `~EtcdSnapshotIntegrationTests|~EtcdFailureTests` `Passed: 9`; после Task 4: unit `Passed: 120`; после Task 5: integration `Passed: 32`; финал: `Total: 152`.
