# t05-sharding-api — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** API шардирования (`GET /api/clusters`, `GET /api/clusters/{cluster}` с фильтрами `?owner=&state=`), наполнение кластерной части `GET /api/overview` и 8 правил алертов шардирования (`shard-no-master`, `move-stale`, `move-frozen-long`, `move-aborting`, `move-flipped-status-stuck`, `bucket-lost`, `bucket-no-routing`, `bucket-out-of-range`) с порогами `AdminPanel:Alerts` — без единой правки `AlertEngine`/`AlertContext`/`SnapshotRefresher`.

**Architecture:** Правила — новые классы `IAlertRule` в `AdminPanel.Core/Alerting/Rules` (пороговые принимают `IOptions<AlertsOptions>` в конструктор), подхватываются автосканом `AddCore()`; `AlertsOptions` — `[Config("AdminPanel:Alerts")]`-POCO в Core. Возраст не-ACTIVE статуса — единый хелпер `Core/MoveAge` (для правил и маппера DTO). API — файлы запросов в `AdminPanel.Api/Inspection` по паттерну t04 (query + DTO + статический mapper + `[InjectAsScoped]`-handler), маршруты добавляются в существующий `InspectionModule`. Тесты: unit без хоста (правила напрямую, мапперы/хендлеры `new`), integration в коллекции `"api"` (фабрика t04 с `TestSnapshotStore`) + путь данных живой Testcontainers-etcd.

**Tech Stack:** .NET 10, C# latest, ASP.NET Core Minimal API, attribute-DI (`[InjectAs*]`, `[Config]`), `Result`-монада, `IOptions`, xunit v3 + FluentAssertions, Testcontainers (etcd `quay.io/coreos/etcd:v3.5.21`).

**Spec:** `docs/superpowers/2026-08-22-t05-sharding-api/spec.md` — план аргументируется от спеки; исполнители читают обе. Ссылки «spec §N» ниже — на её разделы.

## Global Constraints

- Все пути — от корня worktree `/Users/demakaev/ZCodeProject/worktrees/feat-t05-sharding-api`; команды `dotnet` — из него.
- .NET 10, `Nullable=enable`, `TreatWarningsAsErrors=true` — 0 warnings, иначе build падает: usings только реально используемые (грабля CS0105/CS8019-как-error).
- Идентификаторы английские; комментарии в коде русские; тексты `message` алертов русские (spec §2).
- Тесты: xunit v3 + FluentAssertions, AAA-комментарии (`// Arrange` / `// Act` / `// Assert`), на русском.
- Новых NuGet-пакетов нет; `Directory.Packages.props` и csproj не менять (spec §12).
- `AlertEngine.cs`, `IAlertEngine.cs`, `SnapshotRefresher.cs`, парсеры etcd — НЕ трогать (spec §5, критерий приёмки §15.5); мутации `arch/01–04` запрещены.
- В unit-тестах правила/хендлеры конструировать `new` + `Options.Create` (без хоста/DI); статические классы (`MoveAge`, `BucketStates`, мапперы) не использовать как generic-аргументы (CS0718).
- `HandleQuery` — всегда с явными generic-аргументами; в хендлерах `ValueTask.FromResult(...)` с тернарником двух веток `Result<T>` (прецедент t04).
- Integration: порядок Arrange «сначала `LoginAsync` (сдвиг окна лимитера), затем `_factory.Snapshot = ...`» — `LoginAsync` двигает `factory.Time` на +61 c, а `ageSec`/`snapshotAgeMs` считаются от текущего времени фабрики (прецедент t04 §16).
- Каждый таск завершается коммитом (префикс `t05:`); roadmap-правка (`arch/roadmap/sharding.md`) уже в рабочем дереве с фазы spec — коммитится только в Task 9 (spec §14).
- Ожидаемые счётчики тестов: до начала — **120 unit / 32 integration**; после всех задач — **153 unit / 43 integration / 196 total**.

---

### Task 1: MoveAge + AlertsOptions + фикстура MovingCluster (Core)

**Files:**
- Create: `src/AdminPanel.Core/MoveAge.cs`
- Create: `src/AdminPanel.Core/Alerting/AlertsOptions.cs`
- Modify: `src/AdminPanel.Api/appsettings.json` (+ секция `AdminPanel:Alerts`)
- Modify: `src/tests/AdminPanel.UnitTests/TestSnapshots.cs` (+ `MovingCluster`)
- Test: `src/tests/AdminPanel.UnitTests/MoveAgeTests.cs` (новый)

**Interfaces:**
- Consumes: `BucketInfo`/`MoveInfo`/`BucketState` из `AdminPanel.Core` (t03); `[Config]` из Infrastructure.
- Produces (для Tasks 2–3, 5, 7):
  - `namespace AdminPanel.Core`: `static class MoveAge { long? Seconds(BucketInfo bucket, long nowUnix); long? Stamp(BucketInfo bucket); }` — возраст/штамп-база не-ACTIVE статуса (`null` — ACTIVE или оба штампа отсутствуют; spec §3.7, §4.4).
  - `namespace AdminPanel.Core.Alerting`: `class AlertsOptions { int StaleMoveSeconds = 600; int FrozenSeconds = 60; }` с `[Config("AdminPanel:Alerts")]`.
  - Тестовая фикстура `TestSnapshots.MovingCluster(DateTimeOffset now) → ClusterInfo`: 2 шарда (s2 без master), бакеты 0..15 (bucket_1 SYNCING −30 c, bucket_2 FROZEN −10 c, bucket_3 ABORTING −5 c с lastError, bucket_4 без routing — дыра), 2 heals (−3600 c, −7200 c).

- [ ] **Шаг 1: Failing-тесты MoveAge**

  Вход: unit-проект ссылается на Core (готово).

  Действие: создать `src/tests/AdminPanel.UnitTests/MoveAgeTests.cs`:

  ```csharp
  using AdminPanel.Core;
  using FluentAssertions;
  using Xunit;

  namespace AdminPanel.UnitTests;

  // Возраст не-ACTIVE статуса: единая формула правил move-* и ClusterDetailsMapper (spec §3.7, §4.4).
  public class MoveAgeTests
  {
      private static readonly long Now = 1_800_000_000;

      [Fact]
      public void Seconds_FromUpdatedUnix()
      {
          // Arrange: SYNCING с обоими штампами — база updated_unix (roadmap t05).
          var bucket = new BucketInfo(1, "s1", BucketState.Syncing,
              new MoveInfo("s1", "s2", Now - 700, Now - 30, "copy", null));

          // Act
          var age = MoveAge.Seconds(bucket, Now);

          // Assert
          age.Should().Be(30);
      }

      [Fact]
      public void Seconds_FallsBackToStartedUnix()
      {
          // Arrange: updated отсутствует — толерантный fallback на started (spec §3.7).
          var bucket = new BucketInfo(1, "s1", BucketState.Syncing,
              new MoveInfo("s1", "s2", Now - 700, null, "copy", null));

          // Act
          var age = MoveAge.Seconds(bucket, Now);

          // Assert
          age.Should().Be(700);
      }

      [Fact]
      public void Seconds_ActiveBucket_Null()
      {
          // Arrange / Act
          var age = MoveAge.Seconds(new BucketInfo(1, "s1", BucketState.Active, null), Now);

          // Assert: возраст только для не-ACTIVE (arch/03 §2).
          age.Should().BeNull();
      }

      [Fact]
      public void Seconds_NoTimestamps_Null()
      {
          // Arrange: оба штампа отсутствуют — меры возраста нет (битые данные видит key-malformed).
          var bucket = new BucketInfo(1, "s1", BucketState.Frozen, new MoveInfo("s1", "s2", null, null, null, null));

          // Act
          var age = MoveAge.Seconds(bucket, Now);

          // Assert
          age.Should().BeNull();
      }

      [Fact]
      public void Stamp_MatchesSecondsBase()
      {
          // Arrange: штамп-база — тот же fallback, что у Seconds (кормит details правил).
          var bucket = new BucketInfo(1, "s1", BucketState.Aborting,
              new MoveInfo("s2", "s1", Now - 45, null, "cleanup", "err"));

          // Act / Assert
          MoveAge.Stamp(bucket).Should().Be(Now - 45);
      }
  }
  ```

  Выход: 5 failing-тестов (тип `MoveAge` не существует).

  Проверка: `dotnet build src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj 2>&1 | tail -5` → **FAIL компиляции**: `CS0246: MoveAge not found` — красная фаза TDD.

- [ ] **Шаг 2: Реализация MoveAge**

  Вход: красная фаза шага 1.

  Действие: создать `src/AdminPanel.Core/MoveAge.cs`:

  ```csharp
  namespace AdminPanel.Core;

  // Возраст не-ACTIVE статуса бакета: now − (updated_unix ?? started_unix) (spec §3.7, §4.4).
  // Единая формула правил move-* и ClusterDetailsMapper — алерты и UI не расходятся.
  public static class MoveAge
  {
      // Штамп-база возраста: updated_unix, при отсутствии — started_unix.
      // null — бакет ACTIVE или оба штампа отсутствуют (битые данные видит key-malformed).
      public static long? Stamp(BucketInfo bucket)
          => bucket.State == BucketState.Active
              ? null
              : bucket.Move?.UpdatedUnix ?? bucket.Move?.StartedUnix;

      // Возраст в целых секундах от штампа-базы; null — базы нет (spec §3.7).
      public static long? Seconds(BucketInfo bucket, long nowUnix)
      {
          var stamp = Stamp(bucket);
          return stamp is null ? null : nowUnix - stamp.Value;
      }
  }
  ```

  Выход: хелпер возраста в Core.

  Проверка: `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj --filter "FullyQualifiedName~MoveAgeTests" 2>&1 | tail -3` → PASS: **Passed: 5, Failed: 0**.

- [ ] **Шаг 3: AlertsOptions + appsettings + фикстура MovingCluster**

  Вход: `MoveAge` зелёный.

  Действие:

  1) Создать `src/AdminPanel.Core/Alerting/AlertsOptions.cs`:

  ```csharp
  using AdminPanel.Infrastructure.DI;

  namespace AdminPanel.Core.Alerting;

  // [Config]-POCO порогов алертов: секция AdminPanel:Alerts (arch/01 §6; заводит t05 — t04 §3.6).
  // Регистрация — автоскан AddCore(); ReplicaLagBytes появится в t06 (YAGNI, spec §4.5).
  [Config("AdminPanel:Alerts")]
  public class AlertsOptions
  {
      // move-stale: не-ACTIVE статус без прогресса дольше N секунд (каталог 03 §4).
      public int StaleMoveSeconds { get; set; } = 600;

      // move-frozen-long: FROZEN дольше N секунд — cutover обязан быть секундами (каталог 03 §4).
      public int FrozenSeconds { get; set; } = 60;
  }
  ```

  2) В `src/AdminPanel.Api/appsettings.json` внутрь объекта `"AdminPanel"` после `"Etcd"` добавить (самодокументирование контракта — прецедент явной секции Etcd, spec §4.5):

  ```json
  ,
    "Alerts": {
      "StaleMoveSeconds": 600,
      "FrozenSeconds": 60
    }
  ```

  3) В `src/tests/AdminPanel.UnitTests/TestSnapshots.cs` добавить в конец класса (после `GhostCluster`):

  ```csharp
      // Кластер с динамикой переездов и аномалиями (spec §10.5): 2 шарда (s2 — без master),
      // бакеты 0..15 (routing s1/s2, у 4 — дыра), 3 статус-ключа относительно now, 2 heals.
      public static ClusterInfo MovingCluster(DateTimeOffset now)
      {
          var unix = now.ToUnixTimeSeconds();
          return new ClusterInfo(
              "demo", "demo", 16, 1755800000,
              [
                  new ShardInfo("s1", "host=s1a,s1b port=5432 dbname=demo user=postgres",
                      ["s1a", "s1b"], 5432, "demo", "postgres", 1, "s1a:5432", null),
                  new ShardInfo("s2", "host=s2a,s2b port=5432 dbname=demo user=postgres",
                      ["s2a", "s2b"], 5432, "demo", "postgres", 1, null, null),
              ],
              [.. Enumerable.Range(0, 16).Select(i => i switch
              {
                  1 => new BucketInfo(1, "s1", BucketState.Syncing,
                      new MoveInfo("s1", "s2", unix - 130, unix - 30, "copy", null)),
                  2 => new BucketInfo(2, "s1", BucketState.Frozen,
                      new MoveInfo("s1", "s2", unix - 70, unix - 10, "cutover-wait", null)),
                  3 => new BucketInfo(3, "s2", BucketState.Aborting,
                      new MoveInfo("s2", "s1", unix - 45, unix - 5, "cleanup", "receiver went away")),
                  4 => new BucketInfo(4, null, BucketState.Active, null),
                  _ => new BucketInfo(i, i % 2 == 0 ? "s1" : "s2", BucketState.Active, null),
              })],
              [
                  new HealRecord("bucket_5", "s2", "s1", "restore-heal", unix - 3600),
                  new HealRecord("bucket_9", "s1", "s2", "restore-heal", unix - 7200),
              ]);
      }
  ```

  Выход: POCO порогов (регистрация автосканом при следующем `AddCore()`-хосте), настройка в appsettings, кластерная фикстура.

  Проверка: `dotnet build src/AdminPanel.slnx 2>&1 | tail -3` → успех, 0 warnings.

- [ ] **Шаг 4: Полная unit-регрессия и коммит**

  Действие: прогнать всё; закоммитить.

  Проверка: `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj 2>&1 | tail -3` → `Passed! - Failed: 0, Passed: 125, Skipped: 0, Total: 125` (120 существующих + 5 новых).

  Коммит:

  ```bash
  git add src/AdminPanel.Core/MoveAge.cs src/AdminPanel.Core/Alerting/AlertsOptions.cs src/AdminPanel.Api/appsettings.json src/tests/AdminPanel.UnitTests/MoveAgeTests.cs src/tests/AdminPanel.UnitTests/TestSnapshots.cs
  git commit -m "t05: MoveAge + AlertsOptions (AdminPanel:Alerts) + фикстура MovingCluster (unit)"
  ```

---

### Task 2: Шесть безпороговых правил шардирования

**Files:**
- Create: `src/AdminPanel.Core/Alerting/Rules/ShardNoMasterRule.cs`
- Create: `src/AdminPanel.Core/Alerting/Rules/MoveAbortingRule.cs`
- Create: `src/AdminPanel.Core/Alerting/Rules/MoveFlippedStatusStuckRule.cs`
- Create: `src/AdminPanel.Core/Alerting/Rules/BucketLostRule.cs`
- Create: `src/AdminPanel.Core/Alerting/Rules/BucketNoRoutingRule.cs`
- Create: `src/AdminPanel.Core/Alerting/Rules/BucketOutOfRangeRule.cs`
- Test: `src/tests/AdminPanel.UnitTests/ShardingAlertRulesTests.cs` (новый)

**Interfaces:**
- Consumes: `IAlertRule`/`AlertContext` (t04), `MoveAge` (Task 1), модель Core t03.
- Produces: 6 правил `[InjectAsSingleton(typeof(IAlertRule))]` с kind'ами `shard-no-master`/`move-aborting`/`move-flipped-status-stuck`/`bucket-lost`/`bucket-no-routing`/`bucket-out-of-range` (spec §4.2–4.3); беспараметрические конструкторы. Target-формат: шард — `"{cluster}/{shard}"`, бакет — `"{cluster}/bucket_{id}"`.

- [ ] **Шаг 1: Failing-тесты шести правил**

  Вход: Task 1 слит.

  Действие: создать `src/tests/AdminPanel.UnitTests/ShardingAlertRulesTests.cs`:

  ```csharp
  using AdminPanel.Core;
  using AdminPanel.Core.Alerting;
  using AdminPanel.Core.Alerting.Rules;
  using FluentAssertions;
  using Microsoft.Extensions.Options;
  using Xunit;

  namespace AdminPanel.UnitTests;

  // Правила шардирования каталога 03 §4 (spec §4.2–4.3): напрямую на снапшот-фикстурах;
  // механику двигателя (id/sinceUnix/сортировка) проверяет AlertEngineTests.
  public class ShardingAlertRulesTests
  {
      private static readonly DateTimeOffset Now = new(2026, 8, 22, 12, 0, 0, TimeSpan.Zero);
      private static readonly long NowUnix = Now.ToUnixTimeSeconds();

      // Дефолты каталога (600/60) — общий аргумент пороговых правил (Task 3).
      private static readonly IOptions<AlertsOptions> DefaultOptions = Options.Create(new AlertsOptions());

      // Оценка одного правила на снапшоте (spec §10.1).
      private static IReadOnlyList<Alert> Evaluate(IAlertRule rule, EtcdSnapshot snapshot)
          => [.. rule.Evaluate(snapshot, new AlertContext(null, Now, 3))];

      // Снапшот с заданными кластерами поверх здорового etcd-базиса.
      private static EtcdSnapshot Snapshot(params ClusterInfo[] clusters)
          => TestSnapshots.Healthy(Now) with { Clusters = [.. clusters] };

      [Fact]
      public void ShardNoMaster_MissingMasterWithDsn_Critical()
      {
          // Arrange: MovingCluster — s2 без master-ключа при живом dsn (P11).
          var rule = new ShardNoMasterRule();

          // Act
          var alerts = Evaluate(rule, Snapshot(TestSnapshots.MovingCluster(Now)));
          var clean = Evaluate(rule, Snapshot(TestSnapshots.FullCluster()));

          // Assert
          clean.Should().BeEmpty();
          var alert = alerts.Should().ContainSingle().Subject;
          alert.Severity.Should().Be(AlertSeverity.Critical);
          alert.Id.Should().Be("shard-no-master:demo/s2");
          alert.Details!["dsn"].Should().Contain("host=s2a");
      }

      [Fact]
      public void ShardNoMaster_IgnoredWhenNoDsn()
      {
          // Arrange: шард без dsn-ключа — писателя нет, ожидание lease неуместно (spec §4.2).
          var cluster = TestSnapshots.FullCluster() with
          {
              Shards = [new ShardInfo("s1", "", [], null, null, null, null, null, null)],
          };

          // Act
          var alerts = Evaluate(new ShardNoMasterRule(), Snapshot(cluster));

          // Assert
          alerts.Should().BeEmpty();
      }

      [Fact]
      public void MoveAborting_AnyAborting_Warning()
      {
          // Arrange / Act: ABORTING безусловно, даже свежий (P7).
          var alerts = Evaluate(new MoveAbortingRule(), Snapshot(TestSnapshots.MovingCluster(Now)));

          // Assert
          var alert = alerts.Should().ContainSingle().Subject;
          alert.Severity.Should().Be(AlertSeverity.Warning);
          alert.Id.Should().Be("move-aborting:demo/bucket_3");
          alert.Details!["phase"].Should().Be("cleanup");
          alert.Details["lastError"].Should().Be("receiver went away");
          alert.Details["ageSeconds"].Should().Be("5");
      }

      [Fact]
      public void MoveFlipped_RoutingEqualsTarget_Warning()
      {
          // Arrange: routing уже указывает на target, но статус-ключ не снят (P7).
          var cluster = TestSnapshots.FullCluster() with
          {
              Buckets =
              [
                  new BucketInfo(5, "s2", BucketState.Syncing,
                      new MoveInfo("s1", "s2", NowUnix - 100, NowUnix - 100, "copy", null)),
                  new BucketInfo(6, "s1", BucketState.Syncing,
                      new MoveInfo("s1", "s2", NowUnix - 100, NowUnix - 100, "copy", null)),
              ],
          };

          // Act
          var alerts = Evaluate(new MoveFlippedStatusStuckRule(), Snapshot(cluster));

          // Assert: только бакет 5 — owner уже = target; бакет 6 — переезд ещё идёт.
          var alert = alerts.Should().ContainSingle().Subject;
          alert.Severity.Should().Be(AlertSeverity.Warning);
          alert.Id.Should().Be("move-flipped-status-stuck:demo/bucket_5");
          alert.Details!["owner"].Should().Be("s2");
          alert.Details["target"].Should().Be("s2");
      }

      [Fact]
      public void BucketLost_OwnerUnknownShard_Critical()
      {
          // Arrange: routing указывает на шард, которого нет (P23-а).
          var cluster = TestSnapshots.FullCluster() with
          {
              Buckets =
              [
                  new BucketInfo(0, "s9", BucketState.Active, null),
                  new BucketInfo(1, "s1", BucketState.Active, null),
              ],
          };

          // Act
          var alerts = Evaluate(new BucketLostRule(), Snapshot(cluster));

          // Assert
          var alert = alerts.Should().ContainSingle().Subject;
          alert.Severity.Should().Be(AlertSeverity.Critical);
          alert.Id.Should().Be("bucket-lost:demo/bucket_0");
          alert.Details!["owner"].Should().Be("s9");
      }

      [Fact]
      public void BucketNoRouting_HoleInRange_Warning()
      {
          // Arrange: бакет 5 из 0..15 без routing — дыра карты; вне диапазона и incomplete — не дыры.
          var holey = TestSnapshots.FullCluster() with
          {
              Buckets =
              [
                  .. Enumerable.Range(0, 16).Select(i => new BucketInfo(i, i == 5 ? null : "s1", BucketState.Active, null)),
                  new BucketInfo(99, null, BucketState.Active, null),
              ],
          };

          // Act
          var alerts = Evaluate(new BucketNoRoutingRule(), Snapshot(holey, TestSnapshots.GhostCluster()));

          // Assert: одна дыра 0..15; bucket_99 вне диапазона; у ghost (N=0) диапазон пуст (spec §3.13).
          var alert = alerts.Should().ContainSingle().Subject;
          alert.Severity.Should().Be(AlertSeverity.Warning);
          alert.Id.Should().Be("bucket-no-routing:demo/bucket_5");
          alert.Details!["bucketsCount"].Should().Be("16");
      }

      [Fact]
      public void BucketOutOfRange_RoutingBeyondN_Warning()
      {
          // Arrange: routing bucket_99 при N=16 (P18); в диапазоне и incomplete — чисто.
          var withExtra = TestSnapshots.FullCluster() with
          {
              Buckets =
              [
                  .. Enumerable.Range(0, 16).Select(i => new BucketInfo(i, "s1", BucketState.Active, null)),
                  new BucketInfo(99, "s1", BucketState.Active, null),
              ],
          };

          // Act
          var alerts = Evaluate(new BucketOutOfRangeRule(), Snapshot(withExtra, TestSnapshots.GhostCluster()));

          // Assert
          var alert = alerts.Should().ContainSingle().Subject;
          alert.Severity.Should().Be(AlertSeverity.Warning);
          alert.Id.Should().Be("bucket-out-of-range:demo/bucket_99");
          alert.Details!["bucketId"].Should().Be("99");
      }

      [Fact]
      public void Rules_TargetsContainClusterName()
      {
          // Arrange: одна и та же аномалия в двух кластерах — алерты различаются таргетом (spec §4.2).
          var a = TestSnapshots.FullCluster() with
          {
              Buckets = [new BucketInfo(0, "s9", BucketState.Active, null)],
          };
          var b = a with { Name = "other" };

          // Act
          var alerts = Evaluate(new BucketLostRule(), Snapshot(a, b));

          // Assert
          alerts.Should().HaveCount(2);
          alerts.Should().Contain(x => x.Target == "demo/bucket_0");
          alerts.Should().Contain(x => x.Target == "other/bucket_0");
      }
  }
  ```

  Выход: 8 failing-тестов (классы правил не существуют).

  Проверка: `dotnet build src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj 2>&1 | tail -5` → FAIL компиляции: `ShardNoMasterRule` и остальные не найдены — красная фаза.

- [ ] **Шаг 2: Реализация шести правил**

  Вход: красная фаза шага 1.

  Действие: создать 6 файлов в `src/AdminPanel.Core/Alerting/Rules/`.

  `ShardNoMasterRule.cs`:

  ```csharp
  using AdminPanel.Core.Alerting;
  using AdminPanel.Infrastructure.DI;

  namespace AdminPanel.Core.Alerting.Rules;

  // shard-no-master (critical): dsn есть, master-ключа нет — lease протух или писателя нет (P11, arch/03 §4).
  [InjectAsSingleton(typeof(IAlertRule))]
  public sealed class ShardNoMasterRule : IAlertRule
  {
      public const string KindName = "shard-no-master";

      public string Kind => KindName;

      public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
      {
          foreach (var cluster in snapshot.Clusters)
          foreach (var shard in cluster.Shards)
          {
              if (shard.MasterAddress is not null || string.IsNullOrEmpty(shard.Dsn))
                  continue;

              yield return new Alert(
                  $"{KindName}:{cluster.Name}/{shard.Name}",
                  AlertSeverity.Critical,
                  KindName,
                  $"{cluster.Name}/{shard.Name}",
                  $"шард {cluster.Name}/{shard.Name} без master-ключа (lease протух или писателя нет)",
                  new Dictionary<string, string>
                  {
                      ["cluster"] = cluster.Name,
                      ["shard"] = shard.Name,
                      ["dsn"] = shard.Dsn,
                  },
                  null);
          }
      }
  }
  ```

  `MoveAbortingRule.cs`:

  ```csharp
  using AdminPanel.Core.Alerting;
  using AdminPanel.Infrastructure.DI;

  namespace AdminPanel.Core.Alerting.Rules;

  // move-aborting (warning): ABORTING — незавершённая уборка, безусловно (P7, arch/03 §4).
  [InjectAsSingleton(typeof(IAlertRule))]
  public sealed class MoveAbortingRule : IAlertRule
  {
      public const string KindName = "move-aborting";

      public string Kind => KindName;

      public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
      {
          var nowUnix = context.NowUtc.ToUnixTimeSeconds();
          foreach (var cluster in snapshot.Clusters)
          foreach (var bucket in cluster.Buckets.Where(b => b.State == BucketState.Aborting))
          {
              var details = new Dictionary<string, string>();
              if (bucket.Move?.Phase is { } phase)
                  details["phase"] = phase;
              if (bucket.Move?.LastError is { } lastError)
                  details["lastError"] = lastError;
              var stamp = MoveAge.Stamp(bucket);
              if (stamp is not null)
              {
                  details["ageSeconds"] = (nowUnix - stamp.Value).ToString();
                  details["updatedUnix"] = stamp.Value.ToString();
              }

              yield return new Alert(
                  $"{KindName}:{cluster.Name}/bucket_{bucket.Id}",
                  AlertSeverity.Warning,
                  KindName,
                  $"{cluster.Name}/bucket_{bucket.Id}",
                  $"бакет bucket_{bucket.Id} кластера {cluster.Name} в ABORTING — незавершённая уборка",
                  details,
                  null);
          }
      }
  }
  ```

  `MoveFlippedStatusStuckRule.cs`:

  ```csharp
  using AdminPanel.Core.Alerting;
  using AdminPanel.Infrastructure.DI;

  namespace AdminPanel.Core.Alerting.Rules;

  // move-flipped-status-stuck (warning): status есть, routing уже = target (P7, arch/03 §4).
  [InjectAsSingleton(typeof(IAlertRule))]
  public sealed class MoveFlippedStatusStuckRule : IAlertRule
  {
      public const string KindName = "move-flipped-status-stuck";

      public string Kind => KindName;

      public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
      {
          foreach (var cluster in snapshot.Clusters)
          foreach (var bucket in cluster.Buckets)
          {
              if (bucket.State == BucketState.Active
                  || bucket.Move?.Target is not { } target
                  || bucket.Owner != target)
                  continue;

              // Строка канона статус-ключей — в message и details одинаково (spec §4.3).
              var state = bucket.State.ToString().ToUpperInvariant();
              yield return new Alert(
                  $"{KindName}:{cluster.Name}/bucket_{bucket.Id}",
                  AlertSeverity.Warning,
                  KindName,
                  $"{cluster.Name}/bucket_{bucket.Id}",
                  $"routing бакета bucket_{bucket.Id} кластера {cluster.Name} уже = target {target}, но статус {state} не снят",
                  new Dictionary<string, string>
                  {
                      ["owner"] = bucket.Owner!,
                      ["target"] = target,
                      ["state"] = state,
                  },
                  null);
          }
      }
  }
  ```

  `BucketLostRule.cs`:

  ```csharp
  using AdminPanel.Core.Alerting;
  using AdminPanel.Infrastructure.DI;

  namespace AdminPanel.Core.Alerting.Rules;

  // bucket-lost (critical): routing указывает на несуществующий шард (P23-а, arch/03 §4).
  [InjectAsSingleton(typeof(IAlertRule))]
  public sealed class BucketLostRule : IAlertRule
  {
      public const string KindName = "bucket-lost";

      public string Kind => KindName;

      public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
      {
          foreach (var cluster in snapshot.Clusters)
          foreach (var bucket in cluster.Buckets)
          {
              if (bucket.Owner is not { } owner || cluster.Shards.Any(s => s.Name == owner))
                  continue;

              yield return new Alert(
                  $"{KindName}:{cluster.Name}/bucket_{bucket.Id}",
                  AlertSeverity.Critical,
                  KindName,
                  $"{cluster.Name}/bucket_{bucket.Id}",
                  $"routing бакета bucket_{bucket.Id} кластера {cluster.Name} указывает на несуществующий шард {owner}",
                  new Dictionary<string, string> { ["owner"] = owner },
                  null);
          }
      }
  }
  ```

  `BucketNoRoutingRule.cs`:

  ```csharp
  using AdminPanel.Core.Alerting;
  using AdminPanel.Infrastructure.DI;

  namespace AdminPanel.Core.Alerting.Rules;

  // bucket-no-routing (warning): бакет из 0..N-1 без routing-ключа — дыра карты (arch/03 §4).
  // incomplete-кластер (N=0) не проверяется — он уже алертится cluster-incomplete (spec §3.13).
  [InjectAsSingleton(typeof(IAlertRule))]
  public sealed class BucketNoRoutingRule : IAlertRule
  {
      public const string KindName = "bucket-no-routing";

      public string Kind => KindName;

      public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
      {
          foreach (var cluster in snapshot.Clusters)
          foreach (var bucket in cluster.Buckets)
          {
              if (bucket.Owner is not null || bucket.Id < 0 || bucket.Id >= cluster.BucketsCount)
                  continue;

              yield return new Alert(
                  $"{KindName}:{cluster.Name}/bucket_{bucket.Id}",
                  AlertSeverity.Warning,
                  KindName,
                  $"{cluster.Name}/bucket_{bucket.Id}",
                  $"бакет {bucket.Id} кластера {cluster.Name} из диапазона 0..{cluster.BucketsCount - 1} без routing-ключа (дыра карты)",
                  new Dictionary<string, string>
                  {
                      ["bucketId"] = bucket.Id.ToString(),
                      ["bucketsCount"] = cluster.BucketsCount.ToString(),
                  },
                  null);
          }
      }
  }
  ```

  `BucketOutOfRangeRule.cs`:

  ```csharp
  using AdminPanel.Core.Alerting;
  using AdminPanel.Infrastructure.DI;

  namespace AdminPanel.Core.Alerting.Rules;

  // bucket-out-of-range (warning): routing-ключ с id >= N (P18, arch/03 §4);
  // incomplete (N=0) — мимо: без config нет и диапазона (spec §3.13).
  [InjectAsSingleton(typeof(IAlertRule))]
  public sealed class BucketOutOfRangeRule : IAlertRule
  {
      public const string KindName = "bucket-out-of-range";

      public string Kind => KindName;

      public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
      {
          foreach (var cluster in snapshot.Clusters.Where(c => c.BucketsCount > 0))
          foreach (var bucket in cluster.Buckets)
          {
              if (bucket.Owner is null || bucket.Id < cluster.BucketsCount)
                  continue;

              yield return new Alert(
                  $"{KindName}:{cluster.Name}/bucket_{bucket.Id}",
                  AlertSeverity.Warning,
                  KindName,
                  $"{cluster.Name}/bucket_{bucket.Id}",
                  $"routing-ключ bucket_{bucket.Id} кластера {cluster.Name} вне диапазона 0..{cluster.BucketsCount - 1}",
                  new Dictionary<string, string>
                  {
                      ["bucketId"] = bucket.Id.ToString(),
                      ["bucketsCount"] = cluster.BucketsCount.ToString(),
                  },
                  null);
          }
      }
  }
  ```

  Выход: 6 правил; kind'ы/targets/details по таблицам spec §4.2–4.3.

  Проверка: `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj --filter "FullyQualifiedName~ShardingAlertRulesTests" 2>&1 | tail -3` → PASS: **Passed: 8, Failed: 0**.

- [ ] **Шаг 3: Полная unit-регрессия и коммит**

  Действие: полный unit-прогон + коммит.

  Проверка: `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj 2>&1 | tail -3` → `Passed! - Failed: 0, Passed: 133, Skipped: 0, Total: 133` (125 + 8). Существующие `AlertEngineTests`/`SnapshotRefresherTests` не затронуты: `AlertTestRules.All()` ещё не пополнен (Task 3), etcd-тесты не касаются кластерных данных.

  Коммит:

  ```bash
  git add src/AdminPanel.Core/Alerting/Rules/ src/tests/AdminPanel.UnitTests/ShardingAlertRulesTests.cs
  git commit -m "t05: безпороговые правила шардирования — shard-no-master, move-aborting/flipped, bucket-* (unit)"
  ```

---

### Task 3: Пороговые move-stale / move-frozen-long + полный набор правил в харнессах

**Files:**
- Create: `src/AdminPanel.Core/Alerting/Rules/MoveStaleRule.cs`
- Create: `src/AdminPanel.Core/Alerting/Rules/MoveFrozenLongRule.cs`
- Modify: `src/tests/AdminPanel.UnitTests/AlertTestRules.cs` (+8 правил, + using)
- Modify: `src/tests/AdminPanel.UnitTests/ShardingAlertRulesTests.cs` (+6 тестов)
- Modify: `src/tests/AdminPanel.UnitTests/AlertEngineTests.cs` (счётчик в `RuleKinds_AllUnique`)
- Modify: `src/tests/AdminPanel.UnitTests/SnapshotRefresherTests.cs` (ассерт `Refresh_AlertsStoredOnSuccessTick`)

**Interfaces:**
- Consumes: `AlertsOptions`/`MoveAge` (Task 1); `AlertTestRules.All()` (t04) — харнесс `RefresherTestHarness.New` берёт список из него, сам харнесс не правится.
- Produces: `MoveStaleRule(IOptions<AlertsOptions>)` / `MoveFrozenLongRule(IOptions<AlertsOptions>)` с kind'ами `move-stale`/`move-frozen-long`, константами `DefaultSeconds = 600` / `DefaultSeconds = 60` (фолбэк `<= 0`, spec §3.11); `AlertTestRules.All()` → 15 правил (7 t04 + 8 t05).

- [ ] **Шаг 1: Failing-тесты пороговых правил**

  Вход: Task 2 слит.

  Действие: добавить в конец класса `ShardingAlertRulesTests` (файл уже имеет `using Microsoft.Extensions.Options;` из Task 2):

  ```csharp
      [Fact]
      public void MoveStale_OlderThanThreshold_Warning()
      {
          // Arrange: 601 c — есть; ровно 600 и 599 — нет: порог каталога 600 (03 §4).
          var stale = TestSnapshots.FullCluster() with
          {
              Buckets =
              [
                  new BucketInfo(3, "s1", BucketState.Syncing,
                      new MoveInfo("s1", "s2", NowUnix - 700, NowUnix - 601, "copy", null)),
                  new BucketInfo(4, "s1", BucketState.Syncing,
                      new MoveInfo("s1", "s2", NowUnix - 700, NowUnix - 600, "copy", null)),
                  new BucketInfo(5, "s1", BucketState.Syncing,
                      new MoveInfo("s1", "s2", NowUnix - 700, NowUnix - 599, "copy", null)),
              ],
          };

          // Act
          var alerts = Evaluate(new MoveStaleRule(DefaultOptions), Snapshot(stale));

          // Assert
          var alert = alerts.Should().ContainSingle().Subject;
          alert.Severity.Should().Be(AlertSeverity.Warning);
          alert.Id.Should().Be("move-stale:demo/bucket_3");
          alert.Details!["state"].Should().Be("SYNCING");
          alert.Details["ageSeconds"].Should().Be("601");
          alert.Details["thresholdSeconds"].Should().Be("600");
          alert.Details["updatedUnix"].Should().Be((NowUnix - 601).ToString());
      }

      [Fact]
      public void MoveStale_CustomThreshold_FromOptions()
      {
          // Arrange: порог реально читается из AdminPanel:Alerts (spec §3.11): 5 c вместо 600.
          var snapshot = Snapshot(TestSnapshots.FullCluster() with
          {
              Buckets =
              [
                  new BucketInfo(3, "s1", BucketState.Syncing,
                      new MoveInfo("s1", "s2", NowUnix - 10, NowUnix - 6, "copy", null)),
              ],
          });

          // Act
          var custom = Evaluate(
              new MoveStaleRule(Options.Create(new AlertsOptions { StaleMoveSeconds = 5 })), snapshot);

          // Assert: возраст 6 > порога 5 — алерт с фактическим порогом в details.
          var alert = custom.Should().ContainSingle().Subject;
          alert.Details!["thresholdSeconds"].Should().Be("5");
      }

      [Fact]
      public void MoveStale_FallsBackToStartedUnix()
      {
          // Arrange: updated отсутствует — база started (spec §3.7).
          var snapshot = Snapshot(TestSnapshots.FullCluster() with
          {
              Buckets =
              [
                  new BucketInfo(3, "s1", BucketState.Syncing,
                      new MoveInfo("s1", "s2", NowUnix - 700, null, "copy", null)),
              ],
          });

          // Act / Assert
          Evaluate(new MoveStaleRule(DefaultOptions), snapshot)
              .Should().ContainSingle().Which.Details!["updatedUnix"].Should().Be((NowUnix - 700).ToString());
      }

      [Fact]
      public void MoveStale_NoTimestamps_Skipped()
      {
          // Arrange: оба штампа отсутствуют — меры возраста нет, правило молчит (spec §4.2).
          var snapshot = Snapshot(TestSnapshots.FullCluster() with
          {
              Buckets = [new BucketInfo(3, "s1", BucketState.Syncing, new MoveInfo("s1", "s2", null, null, null, null))],
          });

          // Act / Assert
          Evaluate(new MoveStaleRule(DefaultOptions), snapshot).Should().BeEmpty();
      }

      [Fact]
      public void MoveFrozenLong_FrozenOlderThan60s_Critical()
      {
          // Arrange: FROZEN 61 c — порог 60 (cutover секундами, 03 §4); 59 c — чисто.
          var frozen = TestSnapshots.FullCluster() with
          {
              Buckets =
              [
                  new BucketInfo(2, "s1", BucketState.Frozen,
                      new MoveInfo("s1", "s2", NowUnix - 100, NowUnix - 61, "cutover-wait", null)),
                  new BucketInfo(8, "s1", BucketState.Frozen,
                      new MoveInfo("s1", "s2", NowUnix - 100, NowUnix - 59, "cutover-wait", null)),
              ],
          };

          // Act
          var alerts = Evaluate(new MoveFrozenLongRule(DefaultOptions), Snapshot(frozen));

          // Assert
          var alert = alerts.Should().ContainSingle().Subject;
          alert.Severity.Should().Be(AlertSeverity.Critical);
          alert.Id.Should().Be("move-frozen-long:demo/bucket_2");
          alert.Details!["ageSeconds"].Should().Be("61");
          alert.Details["thresholdSeconds"].Should().Be("60");
      }

      [Fact]
      public void ShardingScenario_AllFourAnomalies_ThroughFullEngine()
      {
          // Arrange: сценарий roadmap — протухший lease + зависший FROZEN + routing в никуда + дыра карты.
          var cluster = new ClusterInfo(
              "demo", "demo", 4, 1755800000,
              [new ShardInfo("s1", "host=s1a port=5432 dbname=demo user=postgres",
                  ["s1a"], 5432, "demo", "postgres", 1, null, null)],
              [
                  new BucketInfo(0, "s1", BucketState.Active, null),
                  new BucketInfo(1, null, BucketState.Active, null),
                  new BucketInfo(2, "s9", BucketState.Active, null),
                  new BucketInfo(3, "s1", BucketState.Frozen,
                      new MoveInfo("s1", "s2", NowUnix - 500, NowUnix - 100, "cutover-wait", null)),
              ],
              []);
          var snapshot = TestSnapshots.Healthy(Now) with { Clusters = [cluster] };
          var engine = new AlertEngine(AlertTestRules.All());

          // Act: previous без этого id → sinceUnix = unix оценки (механика t04 §3.4).
          var alerts = engine.Evaluate(snapshot, snapshot with { Alerts = [] }, Now, 3);

          // Assert: 3 critical (no-master, frozen-long, lost) + 1 warning (no-routing),
          // сортировка severity → kind (Ordinal); etcd-правила на здоровом базисе молчат.
          string.Join("|", alerts.Select(a => a.Kind))
              .Should().Be("bucket-lost|move-frozen-long|shard-no-master|bucket-no-routing");
          alerts.Should().OnlyContain(a => a.SinceUnix == NowUnix);
      }
  ```

  Выход: 6 failing-тестов (`MoveStaleRule`/`MoveFrozenLongRule` не существуют; `AlertTestRules.All()` ещё не содержит их — сквозной тест тоже падает компиляцией).

  Проверка: `dotnet build src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj 2>&1 | tail -5` → FAIL компиляции — красная фаза.

- [ ] **Шаг 2: Реализация пороговых правил + пополнение AlertTestRules**

  Вход: красная фаза шага 1.

  Действие:

  1) Создать `src/AdminPanel.Core/Alerting/Rules/MoveStaleRule.cs`:

  ```csharp
  using AdminPanel.Core.Alerting;
  using AdminPanel.Infrastructure.DI;
  using Microsoft.Extensions.Options;

  namespace AdminPanel.Core.Alerting.Rules;

  // move-stale (warning): status-ключ не-ACTIVE без прогресса дольше StaleMoveSeconds (arch/03 §4).
  // Условия каталога независимы: FROZEN/ABORTING старше порога тоже stale (spec §3.12).
  [InjectAsSingleton(typeof(IAlertRule))]
  public sealed class MoveStaleRule(IOptions<AlertsOptions> options) : IAlertRule
  {
      public const string KindName = "move-stale";

      // Каталожный дефолт 600 c — фолбэк при опечатке конфига AdminPanel:Alerts (spec §3.11).
      public const int DefaultSeconds = 600;

      public string Kind => KindName;

      private int ThresholdSeconds
          => options.Value.StaleMoveSeconds > 0 ? options.Value.StaleMoveSeconds : DefaultSeconds;

      public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
      {
          var nowUnix = context.NowUtc.ToUnixTimeSeconds();
          foreach (var cluster in snapshot.Clusters)
          foreach (var bucket in cluster.Buckets)
          {
              if (bucket.State == BucketState.Active)
                  continue;

              var stamp = MoveAge.Stamp(bucket);
              if (stamp is null || nowUnix - stamp.Value <= ThresholdSeconds)
                  continue; // нет меры возраста (spec §4.2) либо прогресс свежий

              var age = nowUnix - stamp.Value;
              yield return new Alert(
                  $"{KindName}:{cluster.Name}/bucket_{bucket.Id}",
                  AlertSeverity.Warning,
                  KindName,
                  $"{cluster.Name}/bucket_{bucket.Id}",
                  $"переезд bucket_{bucket.Id} кластера {cluster.Name} ({StateName(bucket.State)}) без прогресса {age} c — порог {ThresholdSeconds} c",
                  new Dictionary<string, string>
                  {
                      ["state"] = StateName(bucket.State),
                      ["ageSeconds"] = age.ToString(),
                      ["thresholdSeconds"] = ThresholdSeconds.ToString(),
                      ["updatedUnix"] = stamp.Value.ToString(),
                  },
                  null);
          }
      }

      // Строка канона статус-ключей для message/details (spec §3.8); Core не зависит от Api (arch/01 §1).
      private static string StateName(BucketState state)
          => state switch
          {
              BucketState.Syncing => "SYNCING",
              BucketState.Frozen => "FROZEN",
              _ => "ABORTING",
          };
  }
  ```

  2) Создать `src/AdminPanel.Core/Alerting/Rules/MoveFrozenLongRule.cs`:

  ```csharp
  using AdminPanel.Core.Alerting;
  using AdminPanel.Infrastructure.DI;
  using Microsoft.Extensions.Options;

  namespace AdminPanel.Core.Alerting.Rules;

  // move-frozen-long (critical): FROZEN дольше FrozenSeconds — cutover обязан быть секундами (arch/03 §4).
  [InjectAsSingleton(typeof(IAlertRule))]
  public sealed class MoveFrozenLongRule(IOptions<AlertsOptions> options) : IAlertRule
  {
      public const string KindName = "move-frozen-long";

      // Каталожный дефолт 60 c — фолбэк при опечатке конфига (spec §3.11).
      public const int DefaultSeconds = 60;

      public string Kind => KindName;

      private int ThresholdSeconds
          => options.Value.FrozenSeconds > 0 ? options.Value.FrozenSeconds : DefaultSeconds;

      public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
      {
          var nowUnix = context.NowUtc.ToUnixTimeSeconds();
          foreach (var cluster in snapshot.Clusters)
          foreach (var bucket in cluster.Buckets.Where(b => b.State == BucketState.Frozen))
          {
              var stamp = MoveAge.Stamp(bucket);
              if (stamp is null || nowUnix - stamp.Value <= ThresholdSeconds)
                  continue;

              var age = nowUnix - stamp.Value;
              yield return new Alert(
                  $"{KindName}:{cluster.Name}/bucket_{bucket.Id}",
                  AlertSeverity.Critical,
                  KindName,
                  $"{cluster.Name}/bucket_{bucket.Id}",
                  $"бакет bucket_{bucket.Id} кластера {cluster.Name} в FROZEN {age} c — cutover обязан быть секундами",
                  new Dictionary<string, string>
                  {
                      ["ageSeconds"] = age.ToString(),
                      ["thresholdSeconds"] = ThresholdSeconds.ToString(),
                      ["updatedUnix"] = stamp.Value.ToString(),
                  },
                  null);
          }
      }
  }
  ```

  3) В `src/tests/AdminPanel.UnitTests/AlertTestRules.cs` добавить using `using Microsoft.Extensions.Options;` и заменить тело `All()` (spec §3.16):

  ```csharp
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
              new ShardNoMasterRule(),
              new MoveStaleRule(Options.Create(new AlertsOptions())),
              new MoveFrozenLongRule(Options.Create(new AlertsOptions())),
              new MoveAbortingRule(),
              new MoveFlippedStatusStuckRule(),
              new BucketLostRule(),
              new BucketNoRoutingRule(),
              new BucketOutOfRangeRule(),
          ];
  ```

  4) В `src/tests/AdminPanel.UnitTests/AlertEngineTests.cs`, тест `RuleKinds_AllUnique`: заменить `kinds.Should().HaveCount(7)` на `kinds.Should().HaveCount(15)` (комментарий «защита каркаса от copy-paste» остаётся).

  Выход: 8 правил шардирования + 15 в харнессе; DI Program-хоста подхватит их автоматически при следующем старте (Task 7+).

  Проверка: `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj --filter "FullyQualifiedName~ShardingAlertRulesTests|FullyQualifiedName~AlertEngineTests" 2>&1 | tail -3` → PASS: **Passed: 32, Failed: 0** (14 sharding + 18 engine; `Evaluate_HealthySnapshot_NoAlerts` остаётся зелёным — FullCluster чист и для новых правил).

- [ ] **Шаг 3: Правка ассерта `Refresh_AlertsStoredOnSuccessTick` (ожидаемая ломка, spec §3.15)**

  Вход: правила в `AlertTestRules.All()` — refresher-тесты теперь видят move-алерты demo-фикстуры (FixedTimeProvider 2026-01-01, штампы сида — август 2025 → все три статуса протухли).

  Действие: в `src/tests/AdminPanel.UnitTests/SnapshotRefresherTests.cs` заменить в `Refresh_AlertsStoredOnSuccessTick` блок Assert (строки `var alert = store.Current!.Alerts.Should().ContainSingle().Subject;` … `alert.Target.Should().Be("/clusters/demo/buckets/status/bucket_9");`) на:

  ```csharp
          // Assert: key-malformed от битого ключа + 5 move-алертов сида demo (spec §3.15, §10.4).
          var alerts = store.Current!.Alerts;
          alerts.Should().HaveCount(6);
          alerts.Should().Contain(a => a.Id == "key-malformed:/clusters/demo/buckets/status/bucket_9");
          alerts.Should().Contain(a => a.Id == "move-stale:demo/bucket_3");
          alerts.Should().Contain(a => a.Id == "move-stale:demo/bucket_7");
          alerts.Should().Contain(a => a.Id == "move-stale:demo/bucket_11");
          alerts.Should().Contain(a => a.Id == "move-frozen-long:demo/bucket_11");
          alerts.Should().Contain(a => a.Id == "move-aborting:demo/bucket_7");
  ```

  `Refresh_AlertsComputedOnFailTick` не правится: его фикстура сеет ghost с dsn-шардом (`/clusters/ghost/shards/g1/dsn` без master) → в списке появляется и `shard-no-master:ghost/g1`, но ассерты инвариантны к составу соседних kind'ов: `Contain(a => a.Id == "etcd-unreachable:etcd")` и `Single(a => a.Kind == "cluster-incomplete")` (выборка по kind, не по индексу/счётчику) — правка не требуется.

  Выход: ассерты соответствуют 15-правильному набору.

  Проверка: `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj 2>&1 | tail -3` → `Passed! - Failed: 0, Passed: 139, Skipped: 0, Total: 139` (133 + 6 новых).

- [ ] **Шаг 4: Коммит**

  ```bash
  git add src/AdminPanel.Core/Alerting/Rules/MoveStaleRule.cs src/AdminPanel.Core/Alerting/Rules/MoveFrozenLongRule.cs src/tests/AdminPanel.UnitTests/AlertTestRules.cs src/tests/AdminPanel.UnitTests/ShardingAlertRulesTests.cs src/tests/AdminPanel.UnitTests/AlertEngineTests.cs src/tests/AdminPanel.UnitTests/SnapshotRefresherTests.cs
  git commit -m "t05: пороговые move-stale/move-frozen-long + 15 правил в харнессах (unit)"
  ```

---

### Task 4: `GET /api/clusters` — сводный список (ClustersQuery)

**Files:**
- Create: `src/AdminPanel.Api/Inspection/ClustersQuery.cs`
- Test: `src/tests/AdminPanel.UnitTests/ClustersMappersTests.cs` (новый)
- Modify: `src/tests/AdminPanel.UnitTests/InspectionQueryHandlerTests.cs` (+2 теста)

**Interfaces:**
- Consumes: `ISnapshotStore`/`SnapshotNotReadyException` (t04), `ClusterInfo` (t03), `TestSnapshots.MovingCluster` (Task 1).
- Produces (маршрут — Task 5): `ClustersQuery : IQuery<IReadOnlyList<ClusterSummaryDto>>`; `ClusterSummaryDto(string Name, string? DbName, int BucketsCount, bool Incomplete, int ShardsTotal, int ShardsWithMaster, int ActiveMoves)`; `ClustersMapper.Map(IReadOnlyList<ClusterInfo>) → IReadOnlyList<ClusterSummaryDto>`; `ClustersQueryHandler(ISnapshotStore) : IQueryHandler<ClustersQuery, IReadOnlyList<ClusterSummaryDto>>` (`[InjectAsScoped]`).

- [ ] **Шаг 1: Failing-тесты маппера и хендлера**

  Вход: Task 3 слит.

  Действие: создать `src/tests/AdminPanel.UnitTests/ClustersMappersTests.cs`:

  ```csharp
  using AdminPanel.Api.Inspection;
  using AdminPanel.Core;
  using FluentAssertions;
  using Xunit;

  namespace AdminPanel.UnitTests;

  // Мапперы кластерных DTO: чистые функции (spec §10.2).
  public class ClustersMappersTests
  {
      private static readonly DateTimeOffset Now = new(2026, 8, 22, 12, 0, 0, TimeSpan.Zero);
      private static readonly long NowUnix = Now.ToUnixTimeSeconds();

      [Fact]
      public void ClustersMapper_CountsShardsMastersMoves()
      {
          // Arrange: MovingCluster — 2 шарда (s2 без master), 3 не-ACTIVE бакета.

          // Act
          var summaries = ClustersMapper.Map([TestSnapshots.MovingCluster(Now)]);

          // Assert: счётчики UI-таблицы Clusters (arch/03 §3; spec §3.2).
          var summary = summaries.Should().ContainSingle().Subject;
          summary.Name.Should().Be("demo");
          summary.DbName.Should().Be("demo");
          summary.BucketsCount.Should().Be(16);
          summary.ShardsTotal.Should().Be(2);
          summary.ShardsWithMaster.Should().Be(1);
          summary.ActiveMoves.Should().Be(3);
          summary.Incomplete.Should().BeFalse();
      }

      [Fact]
      public void ClustersMapper_IncompleteFlagAndNullDbName()
      {
          // Arrange / Act
          var summaries = ClustersMapper.Map([TestSnapshots.GhostCluster()]);

          // Assert: incomplete-кластер в сводке — dbname null, флаг поднят (spec §3.2).
          var summary = summaries.Should().ContainSingle().Subject;
          summary.Incomplete.Should().BeTrue();
          summary.DbName.Should().BeNull();
      }
  }
  ```

  В `src/tests/AdminPanel.UnitTests/InspectionQueryHandlerTests.cs` добавить в конец класса (usings уже есть):

  ```csharp
      [Fact]
      public async Task ClustersHandle_NoSnapshot_ReturnsFailedSnapshotNotReady()
      {
          // Arrange
          var handler = new ClustersQueryHandler(new SnapshotStore());

          // Act
          var result = await handler.Handle(new ClustersQuery(), CancellationToken.None);

          // Assert
          result.IsSuccess.Should().BeFalse();
          result.Error.Should().BeOfType<InspectionModule.SnapshotNotReadyException>();
      }

      [Fact]
      public async Task ClustersHandle_WithSnapshot_ReturnsSummaries()
      {
          // Arrange
          var store = new SnapshotStore();
          store.Replace(TestSnapshots.Healthy(_time.Utc) with
          {
              Clusters = [TestSnapshots.MovingCluster(_time.Utc)],
          });
          var handler = new ClustersQueryHandler(store);

          // Act
          var result = await handler.Handle(new ClustersQuery(), CancellationToken.None);

          // Assert
          result.IsSuccess.Should().BeTrue();
          var summary = result.Value.Should().ContainSingle().Subject;
          summary.ShardsTotal.Should().Be(2);
          summary.ShardsWithMaster.Should().Be(1);
      }
  ```

  Выход: 4 failing-теста (типы `ClustersMapper`/`ClustersQueryHandler` не существуют).

  Проверка: `dotnet build src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj 2>&1 | tail -5` → FAIL компиляции — красная фаза.

- [ ] **Шаг 2: Реализация ClustersQuery**

  Вход: красная фаза шага 1.

  Действие: создать `src/AdminPanel.Api/Inspection/ClustersQuery.cs`:

  ```csharp
  using AdminPanel.Core;
  using AdminPanel.Etcd;
  using AdminPanel.Infrastructure;
  using AdminPanel.Infrastructure.CQRS;
  using AdminPanel.Infrastructure.DI;

  namespace AdminPanel.Api.Inspection;

  // Запрос сводного списка кластеров (arch/03 §1 GET /api/clusters).
  public sealed record ClustersQuery : IQuery<IReadOnlyList<ClusterSummaryDto>>;

  // Сводка кластера — UI-таблица Clusters (arch/03 §3; spec §3.2); dbname null у incomplete.
  public sealed record ClusterSummaryDto(
      string Name,
      string? DbName,
      int BucketsCount,
      bool Incomplete,
      int ShardsTotal,
      int ShardsWithMaster,
      int ActiveMoves);

  // Снапшот → сводки: чистая функция; порядок кластеров — как в снапшоте (spec §3.3).
  public static class ClustersMapper
  {
      public static IReadOnlyList<ClusterSummaryDto> Map(IReadOnlyList<ClusterInfo> clusters)
          => [.. clusters.Select(c => new ClusterSummaryDto(
              c.Name,
              c.DbName,
              c.BucketsCount,
              c.Incomplete,
              c.Shards.Count,
              c.Shards.Count(s => s.MasterAddress is not null),
              c.Buckets.Count(b => b.State != BucketState.Active)))];
  }

  // Хендлер: store → отказ «снапшота нет» или маппер (spec §3.12).
  [InjectAsScoped]
  public sealed class ClustersQueryHandler(ISnapshotStore store)
      : IQueryHandler<ClustersQuery, IReadOnlyList<ClusterSummaryDto>>
  {
      public ValueTask<Result<IReadOnlyList<ClusterSummaryDto>>> Handle(ClustersQuery query, CancellationToken ct)
      {
          var snapshot = store.Current;
          return ValueTask.FromResult(snapshot is null
              ? Result<IReadOnlyList<ClusterSummaryDto>>.Failed(new InspectionModule.SnapshotNotReadyException())
              : Result<IReadOnlyList<ClusterSummaryDto>>.Success(ClustersMapper.Map(snapshot.Clusters)));
      }
  }
  ```

  Выход: сводный список кластеров (маршрут подключит Task 5).

  Проверка: `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj --filter "FullyQualifiedName~ClustersMappersTests|FullyQualifiedName~InspectionQueryHandlerTests" 2>&1 | tail -3` → PASS: **Passed: 9, Failed: 0** (2 mapper + 7 handler: 5 t04 + 2 новых).

- [ ] **Шаг 3: Полная unit-регрессия и коммит**

  Проверка: `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj 2>&1 | tail -3` → `Passed! - Failed: 0, Passed: 143, Skipped: 0, Total: 143` (139 + 4).

  Коммит:

  ```bash
  git add src/AdminPanel.Api/Inspection/ClustersQuery.cs src/tests/AdminPanel.UnitTests/ClustersMappersTests.cs src/tests/AdminPanel.UnitTests/InspectionQueryHandlerTests.cs
  git commit -m "t05: GET /api/clusters — сводный список кластеров (unit)"
  ```

---

### Task 5: `GET /api/clusters/{cluster}` — детали, фильтры, 404 + маршруты InspectionModule

**Files:**
- Create: `src/AdminPanel.Api/Inspection/ClusterDetailsQuery.cs`
- Modify: `src/AdminPanel.Api/Inspection/InspectionModule.cs` (+ `ClusterNotFoundException`, +2 маршрута)
- Test: `src/tests/AdminPanel.UnitTests/ClustersMappersTests.cs` (+6 тестов)
- Modify: `src/tests/AdminPanel.UnitTests/InspectionQueryHandlerTests.cs` (+3 теста)

**Interfaces:**
- Consumes: `ISnapshotStore`/`Result`/`InspectionModule` (t04), `MoveAge` (Task 1), `ShardRuntime`-модель (t03).
- Produces (для Tasks 6–7):
  - `ClusterDetailsQuery(string Cluster, string? Owner, BucketState? State) : IQuery<ClusterDto>`;
  - `ClusterDto(Name, DbName, BucketsCount, CreatedUnix, Incomplete, Shards, Buckets, Heals)` + `ShardDto(Name, Dsn, Hosts, ReplicasDeclared, MasterAddress, MasterLeaseAlive, Runtime)` + `ShardRuntimeDto(StandbiesSync, SlotsLagMaxBytes, WalStatusLost, Subscriptions, BucketSchemas, Error)` + `BucketDto(Id, Owner, State, Move, AgeSec)` + `MoveDto(Owner, Target, StartedUnix, UpdatedUnix, Phase, LastError)` + `HealDto(Bucket, Was, Now, Reason, TsUnix)`;
  - `static class BucketStates { string Name(BucketState); bool TryParse(string?, out BucketState); }` — использует Task 6;
  - `ClusterDetailsMapper.Map(ClusterInfo, long nowUnix, string? owner, BucketState? state) → ClusterDto` + `MapRuntime(ShardRuntime) → ShardRuntimeDto`;
  - `ClusterDetailsQueryHandler(ISnapshotStore, TimeProvider)`;
  - `InspectionModule.ClusterNotFoundException` (вложенный `sealed class`); маршруты `/api/clusters` и `/api/clusters/{cluster}` с state-валидацией (400) и 404/503-различением.

- [ ] **Шаг 1: Failing-тесты маппера/BucketStates**

  Вход: Task 4 слит.

  Действие: добавить в конец класса `ClustersMappersTests` (using `AdminPanel.Etcd` не нужен — runtime-модель в Core):

  ```csharp
      [Fact]
      public void ClusterDetailsMapper_FullDto()
      {
          // Arrange
          var cluster = TestSnapshots.MovingCluster(Now);

          // Act
          var dto = ClusterDetailsMapper.Map(cluster, NowUnix, null, null);

          // Assert: config-константы + полные блоки (arch/03 §2).
          dto.Name.Should().Be("demo");
          dto.DbName.Should().Be("demo");
          dto.BucketsCount.Should().Be(16);
          dto.CreatedUnix.Should().Be(1755800000);
          dto.Incomplete.Should().BeFalse();
          dto.Shards.Should().HaveCount(2);
          var s1 = dto.Shards[0];
          s1.Dsn.Should().Contain("host=s1a,s1b");
          s1.Hosts.Should().Equal("s1a", "s1b");
          s1.ReplicasDeclared.Should().Be(1);
          s1.MasterAddress.Should().Be("s1a:5432");
          s1.MasterLeaseAlive.Should().BeTrue();
          s1.Runtime.Should().BeNull(); // t05: данных SQL-пробы нет (spec §3.14)
          dto.Shards[1].MasterLeaseAlive.Should().BeFalse();
          dto.Buckets.Should().HaveCount(16);
          dto.Heals.Should().HaveCount(2);
      }

      [Fact]
      public void ClusterDetailsMapper_AgeSec_FromMoveAge()
      {
          // Arrange: SYNCING −30 / FROZEN −10 / ABORTING −5; ACTIVE — null (spec §3.7).
          var dto = ClusterDetailsMapper.Map(TestSnapshots.MovingCluster(Now), NowUnix, null, null);

          // Act — возрасты по id из DTO.
          var ages = dto.Buckets.ToDictionary(b => b.Id, b => b.AgeSec);

          // Assert
          ages[1].Should().Be(30);
          ages[2].Should().Be(10);
          ages[3].Should().Be(5);
          ages[0].Should().BeNull();
          dto.Buckets[0].Move.Should().BeNull();
          dto.Buckets[1].Move!.Target.Should().Be("s2");
          dto.Buckets[1].State.Should().Be("SYNCING");
      }

      [Fact]
      public void ClusterDetailsMapper_Filters_OwnerStateBothNull()
      {
          // Arrange: routing s1 — 8 бакетов (6 базис + SYNCING/FROZEN), s2 — 7, дыра — 1.
          var cluster = TestSnapshots.MovingCluster(Now);

          // Act / Assert: owner — точное совпадение; state — по enum; оба — пересечение (spec §3.9).
          ClusterDetailsMapper.Map(cluster, NowUnix, "s1", null).Buckets.Should().HaveCount(8);
          ClusterDetailsMapper.Map(cluster, NowUnix, "s1", BucketState.Syncing).Buckets
              .Should().ContainSingle().Which.Id.Should().Be(1);
          ClusterDetailsMapper.Map(cluster, NowUnix, null, BucketState.Active).Buckets.Should().HaveCount(13);
          ClusterDetailsMapper.Map(cluster, NowUnix, "nope", null).Buckets.Should().BeEmpty();
          ClusterDetailsMapper.Map(cluster, NowUnix, null, null).Buckets.Should().HaveCount(16);
      }

      [Fact]
      public void ClusterDetailsMapper_Heals_NewestFirst()
      {
          // Arrange: журнал — новые сверху; null-штамп в конец (spec §3.3).
          var cluster = TestSnapshots.MovingCluster(Now) with
          {
              Heals =
              [
                  new HealRecord("bucket_9", "s1", "s2", "restore-heal", 100),
                  new HealRecord("bucket_5", "s2", "s1", "restore-heal", 200),
                  new HealRecord("bucket_7", "s1", "s1", "restore-heal", null),
              ],
          };

          // Act
          var dto = ClusterDetailsMapper.Map(cluster, NowUnix, null, null);

          // Assert
          dto.Heals.Select(h => h.Bucket).Should().Equal("bucket_5", "bucket_9", "bucket_7");
          dto.Heals[0].Was.Should().Be("s2");
          dto.Heals[2].TsUnix.Should().BeNull();
      }

      [Fact]
      public void ClusterDetailsMapper_RuntimeMapped_WhenPresent()
      {
          // Arrange: модель t03 → DTO arch/03 §2 — маппинг фиксируется до данных t06 (spec §3.14).
          var runtime = new ShardRuntime(
              "s1",
              [
                  new ReplicationSlotInfo("slot_a", "logical", true, "lost", 1024, 5000),
                  new ReplicationSlotInfo("slot_b", "logical", true, "reserved", 2048, 9000),
              ],
              [
                  new StandbyInfo("s1b", "10.0.0.2", "streaming", "sync", 100),
                  new StandbyInfo("s1c", "10.0.0.3", "streaming", "async", 200),
              ],
              [new SubscriptionInfo("sub_bucket_3", "0/100", "0/200", null)],
              ["bucket_0", "bucket_3"],
              false,
              null);
          var cluster = TestSnapshots.FullCluster() with
          {
              Shards =
              [
                  new ShardInfo("s1", "host=s1a port=5432 dbname=demo user=postgres",
                      ["s1a"], 5432, "demo", "postgres", 1, "s1a:5432", runtime),
              ],
          };

          // Act
          var dto = ClusterDetailsMapper.Map(cluster, NowUnix, null, null);

          // Assert: standbiesSync — только sync/quorum; лаг слотов — max; lost — имена слотов.
          var mapped = dto.Shards.Single().Runtime.Should().NotBeNull().And.Subject.As<ShardRuntimeDto>();
          mapped.StandbiesSync.Should().Be(1);
          mapped.SlotsLagMaxBytes.Should().Be(9000);
          mapped.WalStatusLost.Should().Equal("slot_a");
          mapped.Subscriptions.Should().Equal("sub_bucket_3");
          mapped.BucketSchemas.Should().Equal("bucket_0", "bucket_3");
          mapped.Error.Should().BeNull();
      }

      [Fact]
      public void BucketStates_RoundTrip()
      {
          // Arrange / Act / Assert: enum ↔ строки канона; мусор не парсится (spec §3.8).
          BucketStates.Name(BucketState.Active).Should().Be("ACTIVE");
          BucketStates.Name(BucketState.Syncing).Should().Be("SYNCING");
          BucketStates.Name(BucketState.Frozen).Should().Be("FROZEN");
          BucketStates.Name(BucketState.Aborting).Should().Be("ABORTING");
          foreach (var (text, expected) in new (string, BucketState)[]
          {
              ("ACTIVE", BucketState.Active),
              ("SYNCING", BucketState.Syncing),
              ("FROZEN", BucketState.Frozen),
              ("ABORTING", BucketState.Aborting),
          })
          {
              BucketStates.TryParse(text, out var parsed).Should().BeTrue();
              parsed.Should().Be(expected);
          }

          BucketStates.TryParse("bogus", out _).Should().BeFalse();
          BucketStates.TryParse(null, out _).Should().BeFalse();
      }
  ```

  Выход: 6 failing-тестов (типы не существуют).

  Проверка: `dotnet build src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj 2>&1 | tail -5` → FAIL компиляции — красная фаза.

- [ ] **Шаг 2: Failing-тесты хендлера (404-отказ)**

  Действие: добавить в конец класса `InspectionQueryHandlerTests`:

  ```csharp
      [Fact]
      public async Task ClusterDetailsHandle_NoSnapshot_ReturnsFailedSnapshotNotReady()
      {
          // Arrange
          var handler = new ClusterDetailsQueryHandler(new SnapshotStore(), _time);

          // Act
          var result = await handler.Handle(new ClusterDetailsQuery("demo", null, null), CancellationToken.None);

          // Assert
          result.IsSuccess.Should().BeFalse();
          result.Error.Should().BeOfType<InspectionModule.SnapshotNotReadyException>();
      }

      [Fact]
      public async Task ClusterDetailsHandle_UnknownCluster_ReturnsFailedClusterNotFound()
      {
          // Arrange
          var store = new SnapshotStore();
          store.Replace(TestSnapshots.Healthy(_time.Utc));
          var handler = new ClusterDetailsQueryHandler(store, _time);

          // Act
          var result = await handler.Handle(new ClusterDetailsQuery("ghost", null, null), CancellationToken.None);

          // Assert: 404-отказ отличается от 503 — различает эндпоинт (spec §3.10).
          result.IsSuccess.Should().BeFalse();
          result.Error.Should().BeOfType<InspectionModule.ClusterNotFoundException>();
      }

      [Fact]
      public async Task ClusterDetailsHandle_WithSnapshot_ReturnsDtoWithFilters()
      {
          // Arrange
          var store = new SnapshotStore();
          store.Replace(TestSnapshots.Healthy(_time.Utc) with
          {
              Clusters = [TestSnapshots.MovingCluster(_time.Utc)],
          });
          var handler = new ClusterDetailsQueryHandler(store, _time);

          // Act
          var result = await handler.Handle(
              new ClusterDetailsQuery("demo", "s1", BucketState.Syncing), CancellationToken.None);

          // Assert
          result.IsSuccess.Should().BeTrue();
          result.Value.Buckets.Should().ContainSingle().Which.Id.Should().Be(1);
          result.Value.Buckets[0].AgeSec.Should().Be(30);
      }
  ```

  Выход: ещё 3 failing-теста.

  Проверка: тот же build → FAIL компиляции (всего в двух шагах 9 тестов — красная фаза).

- [ ] **Шаг 3: Реализация ClusterDetailsQuery + маршруты**

  Вход: красная фаза шагов 1–2.

  Действие:

  1) Создать `src/AdminPanel.Api/Inspection/ClusterDetailsQuery.cs`:

  ```csharp
  using AdminPanel.Core;
  using AdminPanel.Etcd;
  using AdminPanel.Infrastructure;
  using AdminPanel.Infrastructure.CQRS;
  using AdminPanel.Infrastructure.DI;

  namespace AdminPanel.Api.Inspection;

  // Запрос детализации кластера (arch/03 §1; state уже провалидирован эндпоинтом — spec §3.9).
  public sealed record ClusterDetailsQuery(string Cluster, string? Owner, BucketState? State)
      : IQuery<ClusterDto>;

  // Ответ GET /api/clusters/{cluster} (arch/03 §2): всё сразу, N <= тысяч — грид на клиенте.
  public sealed record ClusterDto(
      string Name,
      string? DbName,
      int BucketsCount,
      long? CreatedUnix,
      bool Incomplete,
      IReadOnlyList<ShardDto> Shards,
      IReadOnlyList<BucketDto> Buckets,
      IReadOnlyList<HealDto> Heals);

  // arch/03 §2: masterLeaseAlive — семантика lease (arch/02 §1); hosts — multi-host из DsnParser t03.
  public sealed record ShardDto(
      string Name,
      string Dsn,
      IReadOnlyList<string> Hosts,
      int? ReplicasDeclared,
      string? MasterAddress,
      bool MasterLeaseAlive,
      ShardRuntimeDto? Runtime);

  // Контракт runtime фиксируется сейчас (фронтенд t08 типизирует сразу), данные — t06 (spec §3.14).
  public sealed record ShardRuntimeDto(
      int? StandbiesSync,
      long? SlotsLagMaxBytes,
      IReadOnlyList<string> WalStatusLost,
      IReadOnlyList<string> Subscriptions,
      IReadOnlyList<string> BucketSchemas,
      string? Error);

  // state — строка канона статус-ключей (spec §3.8); move/ageSec — null у ACTIVE (spec §3.7).
  public sealed record BucketDto(
      int Id,
      string? Owner,
      string State,
      MoveDto? Move,
      long? AgeSec);

  public sealed record MoveDto(
      string? Owner,
      string? Target,
      long? StartedUnix,
      long? UpdatedUnix,
      string? Phase,
      string? LastError);

  public sealed record HealDto(string Bucket, string? Was, string? Now, string? Reason, long? TsUnix);

  // Строки state — верхний регистр канона (arch/02 §2.1); общий источник мапперов и валидации query.
  public static class BucketStates
  {
      public static string Name(BucketState state)
          => state switch
          {
              BucketState.Syncing => "SYNCING",
              BucketState.Frozen => "FROZEN",
              BucketState.Aborting => "ABORTING",
              _ => "ACTIVE",
          };

      public static bool TryParse(string? text, out BucketState state)
      {
          switch (text)
          {
              case "ACTIVE": state = BucketState.Active; return true;
              case "SYNCING": state = BucketState.Syncing; return true;
              case "FROZEN": state = BucketState.Frozen; return true;
              case "ABORTING": state = BucketState.Aborting; return true;
              default: state = BucketState.Active; return false;
          }
      }
  }

  // Core → DTO: чистая функция; фильтры buckets, возраст MoveAge, heals по TsUnix desc (spec §3.3, §3.7, §3.9).
  public static class ClusterDetailsMapper
  {
      public static ClusterDto Map(ClusterInfo cluster, long nowUnix, string? owner, BucketState? state)
      {
          var buckets = cluster.Buckets
              .Where(b => owner is null || b.Owner == owner)
              .Where(b => state is null || b.State == state);
          return new ClusterDto(
              cluster.Name,
              cluster.DbName,
              cluster.BucketsCount,
              cluster.CreatedUnix,
              cluster.Incomplete,
              [.. cluster.Shards.Select(s => new ShardDto(
                  s.Name,
                  s.Dsn,
                  s.DsnHosts,
                  s.ReplicasDeclared,
                  s.MasterAddress,
                  s.MasterLeaseAlive,
                  s.Runtime is null ? null : MapRuntime(s.Runtime)))],
              [.. buckets.Select(b => new BucketDto(
                  b.Id,
                  b.Owner,
                  BucketStates.Name(b.State),
                  b.Move is null ? null : new MoveDto(
                      b.Move.Owner, b.Move.Target, b.Move.StartedUnix, b.Move.UpdatedUnix,
                      b.Move.Phase, b.Move.LastError),
                  MoveAge.Seconds(b, nowUnix)))],
              [.. cluster.Heals
                  .OrderByDescending(h => h.TsUnix) // журнал: новые сверху; null — в конец (spec §3.3)
                  .Select(h => new HealDto(h.Bucket, h.Was, h.Now, h.Reason, h.TsUnix))]);
      }

      // Маппинг runtime — по стабильной модели t03; поля arch/03 §2 (spec §3.14).
      public static ShardRuntimeDto MapRuntime(ShardRuntime runtime)
          => new(
              runtime.Standbies.Count(s => s.SyncState is "sync" or "quorum"),
              runtime.Slots.Count == 0 ? null : runtime.Slots.Max(s => s.LagBytes),
              [.. runtime.Slots.Where(s => s.WalStatus == "lost").Select(s => s.SlotName)],
              [.. runtime.Subscriptions.Select(s => s.Name)],
              runtime.BucketSchemas,
              runtime.Error);
  }

  // Хендлер: 503 «снапшота нет» / 404 «кластер не найден» / маппер (spec §3.10, §3.12).
  [InjectAsScoped]
  public sealed class ClusterDetailsQueryHandler(ISnapshotStore store, TimeProvider time)
      : IQueryHandler<ClusterDetailsQuery, ClusterDto>
  {
      public ValueTask<Result<ClusterDto>> Handle(ClusterDetailsQuery query, CancellationToken ct)
      {
          var snapshot = store.Current;
          if (snapshot is null)
              return ValueTask.FromResult(Result<ClusterDto>.Failed(
                  new InspectionModule.SnapshotNotReadyException()));

          var cluster = snapshot.Clusters.FirstOrDefault(c => c.Name == query.Cluster);
          return ValueTask.FromResult(cluster is null
              ? Result<ClusterDto>.Failed(new InspectionModule.ClusterNotFoundException(query.Cluster))
              : Result<ClusterDto>.Success(ClusterDetailsMapper.Map(
                  cluster, time.GetUtcNow().ToUnixTimeSeconds(), query.Owner, query.State)));
      }
  }
  ```

  2) В `src/AdminPanel.Api/Inspection/InspectionModule.cs`:

  а) Добавить после `SnapshotNotReadyException` (spec §3.10):

  ```csharp
      // Кластер отсутствует в снапшоте: 404 — отличается от 503 «снапшота нет» (spec §3.10).
      public sealed class ClusterNotFoundException(string cluster)
          : Exception($"кластер {cluster} не найден в снапшоте");
  ```

  б) В `MapInspectionApi` после маршрута `/api/etcd/status` добавить (spec §6.1):

  ```csharp
          endpoints.MapGet("/api/clusters", async (IHandler handler, CancellationToken ct) =>
          {
              var result = await handler.HandleQuery<ClustersQuery, IReadOnlyList<ClusterSummaryDto>>(
                  new ClustersQuery(), ct);
              return ResultToHttp(result);
          });

          endpoints.MapGet("/api/clusters/{cluster}", async (
              string cluster, string? owner, string? state, IHandler handler, CancellationToken ct) =>
          {
              // Валидация до query: строго канон статус-ключей, иначе 400 (spec §3.9).
              BucketState? parsed = null;
              if (state is not null)
              {
                  if (!BucketStates.TryParse(state, out var value))
                      return Results.Problem(
                          statusCode: StatusCodes.Status400BadRequest,
                          title: "Invalid state",
                          detail: $"state должен быть ACTIVE|SYNCING|FROZEN|ABORTING, получено: {state}");
                  parsed = value;
              }

              var result = await handler.HandleQuery<ClusterDetailsQuery, ClusterDto>(
                  new ClusterDetailsQuery(cluster, owner, parsed), ct);
              if (result.IsSuccess)
                  return Results.Ok(result.Value);
              return result.Error is ClusterNotFoundException
                  ? Results.Problem(
                      statusCode: StatusCodes.Status404NotFound,
                      title: "Cluster not found",
                      detail: result.Error.Message)
                  : Results.Problem(
                      statusCode: StatusCodes.Status503ServiceUnavailable,
                      title: "Snapshot not ready",
                      detail: result.Error!.Message);
          });
  ```

  `Program.cs` не меняется: `app.MapInspectionApi()` уже вызывается (t04).

  Выход: детали кластера + 2 маршрута; auth-guard уже закрывает `/api/*`.

  Проверка: `dotnet build src/AdminPanel.slnx 2>&1 | tail -3` → успех, 0 warnings. Затем `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj --filter "FullyQualifiedName~ClustersMappersTests|FullyQualifiedName~InspectionQueryHandlerTests" 2>&1 | tail -3` → PASS: **Passed: 18, Failed: 0** (8 mapper + 10 handler).

- [ ] **Шаг 4: Полная unit-регрессия и коммит**

  Проверка: `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj 2>&1 | tail -3` → `Passed! - Failed: 0, Passed: 152, Skipped: 0, Total: 152` (143 + 9).

  Коммит:

  ```bash
  git add src/AdminPanel.Api/Inspection/ClusterDetailsQuery.cs src/AdminPanel.Api/Inspection/InspectionModule.cs src/tests/AdminPanel.UnitTests/ClustersMappersTests.cs src/tests/AdminPanel.UnitTests/InspectionQueryHandlerTests.cs
  git commit -m "t05: GET /api/clusters/{cluster} — детали, фильтры owner/state, 404 (unit)"
  ```

---

### Task 6: Overview — наполнение кластерной части

**Files:**
- Modify: `src/AdminPanel.Api/Inspection/OverviewQuery.cs` (тело `OverviewMapper.Map`)
- Modify: `src/tests/AdminPanel.UnitTests/InspectionMappersTests.cs` (замена stub-теста t04 + 2 новых)

**Interfaces:**
- Consumes: `BucketStates.Name` (Task 5), `ClusterInfo` (t03), DTO-типы `OverviewClusterDto`/`OverviewMoveDto` (t04, не меняются).
- Produces: `OverviewMapper.Map` возвращает заполненные `Clusters` (`{Name, Shards.Count, BucketsCount, ActiveMoves, MasterlessShards}` — spec §3.4–3.5) и `ActiveMoves` (все не-ACTIVE бакеты, порядок кластеров снапшота → bucket id — spec §3.6).

- [ ] **Шаг 1: Failing-тесты наполнения**

  Вход: Task 5 слит (`BucketStates` доступен).

  Действие: в `src/tests/AdminPanel.UnitTests/InspectionMappersTests.cs` удалить тест `OverviewMapper_ClusterStubs_Empty` (заглушка t04) и добавить вместо него в тот же класс:

  ```csharp
      [Fact]
      public void OverviewMapper_ClustersAndMovesFilled()
      {
          // Arrange: MovingCluster — 2 шарда (1 без master), 3 переезда (spec §10.3).
          var snapshot = TestSnapshots.Healthy(BuiltAt) with
          {
              Clusters = [TestSnapshots.MovingCluster(BuiltAt)],
          };

          // Act
          var dto = OverviewMapper.Map(snapshot, BuiltAt + TimeSpan.FromSeconds(1), 3);

          // Assert: заглушки t04 наполнены (spec §3.15 t04 → §1 t05).
          var cluster = dto.Clusters.Should().ContainSingle().Subject;
          cluster.Name.Should().Be("demo");
          cluster.Shards.Should().Be(2);
          cluster.Buckets.Should().Be(16);
          cluster.ActiveMoves.Should().Be(3);
          cluster.MasterlessShards.Should().Be(1);
          dto.ActiveMoves.Should().HaveCount(3);
          dto.ActiveMoves.Should().Contain(m => m.Cluster == "demo" && m.Bucket == 1
              && m.State == "SYNCING" && m.Owner == "s1" && m.Target == "s2");
      }

      [Fact]
      public void OverviewMapper_MovesAcrossClusters_Ordered()
      {
          // Arrange: два кластера — порядок кластеров снапшота, внутри по bucket id (spec §3.6).
          var snapshot = TestSnapshots.Healthy(BuiltAt) with
          {
              Clusters =
              [
                  TestSnapshots.MovingCluster(BuiltAt),
                  TestSnapshots.FullCluster() with
                  {
                      Name = "other",
                      Buckets =
                      [
                          new BucketInfo(7, "s2", BucketState.Syncing,
                              new MoveInfo("s2", "s1", null, BuiltAt.ToUnixTimeSeconds() - 5, null, null)),
                          new BucketInfo(0, "s1", BucketState.Aborting,
                              new MoveInfo("s1", "s2", null, null, null, null)),
                      ],
                  },
              ],
          };

          // Act
          var dto = OverviewMapper.Map(snapshot, BuiltAt, 3);

          // Assert: id по возрастанию внутри кластера; state-строки канона; nullable-поля как есть.
          string.Join("|", dto.ActiveMoves.Select(m => $"{m.Cluster}/{m.Bucket}"))
              .Should().Be("demo/1|demo/2|demo/3|other/0|other/7");
          dto.ActiveMoves[4].State.Should().Be("SYNCING");
          dto.ActiveMoves[4].UpdatedUnix.Should().Be(BuiltAt.ToUnixTimeSeconds() - 5);
          dto.ActiveMoves[3].UpdatedUnix.Should().BeNull();
      }
  ```

  Примечание: файл уже использует `TestSnapshots`/`AdminPanel.Core` (тесты t04 в нём).

  Выход: stub-тест удалён, 2 новых падают (`clusters`/`activeMoves` всё ещё `[]`).

  Проверка: `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj --filter "FullyQualifiedName~InspectionMappersTests" 2>&1 | tail -3` → FAIL: **Failed: 2, Passed: 11** — красная фаза.

- [ ] **Шаг 2: Наполнение OverviewMapper**

  Вход: красная фаза шага 1.

  Действие: в `src/AdminPanel.Api/Inspection/OverviewQuery.cs` заменить в `OverviewMapper.Map` два аргумента-заглушки `[], [],` (четвёртый и пятый позиционные аргументы `OverviewDto`) на:

  ```csharp
              [.. snapshot.Clusters.Select(c => new OverviewClusterDto(
                  c.Name,
                  c.Shards.Count,
                  c.BucketsCount,
                  c.Buckets.Count(b => b.State != BucketState.Active),
                  c.Shards.Count(s => s.MasterAddress is null)))],
              [.. snapshot.Clusters
                  .SelectMany(c => c.Buckets
                      .Where(b => b.State != BucketState.Active)
                      .OrderBy(b => b.Id) // внутри кластера — по Id (spec §3.6): модель порядка Buckets не гарантирует
                      .Select(b => new OverviewMoveDto(
                          c.Name, b.Id, BucketStates.Name(b.State),
                          b.Move?.Owner, b.Move?.Target, b.Move?.UpdatedUnix)))],
  ```

  и комментарий над record `OverviewDto` «кластерная — заглушки t05» заменить на «кластерная часть — t05». Больше в файле изменений нет (DTO-типы и хендлер — без правок, spec §6.2).

  Выход: кластерная часть Overview наполнена.

  Проверка: `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj --filter "FullyQualifiedName~InspectionMappersTests" 2>&1 | tail -3` → PASS: **Passed: 13, Failed: 0** (12 t04 − 1 stub + 2 новых). Регресс etcd-части маппера (spec §10.3 «EtcdParts_Unchanged») — те же существующие тесты `OverviewMapper_CountsEtcdAndAlerts` / `_StaleByTripleInterval` / `_NegativeAgeClampedToZero`: остаются в прогоне фильтра и обязаны остаться зелёными при наполненном маппере.

- [ ] **Шаг 3: Полная unit-регрессия и коммит**

  Проверка: `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj 2>&1 | tail -3` → `Passed! - Failed: 0, Passed: 153, Skipped: 0, Total: 153` (152 − 1 + 2). Integration не запускался — `InspectionApiTests` с Fixture без кластеров (`clusters: []`) остаётся валидным, полный integration-прогон — Task 7.

  Коммит:

  ```bash
  git add src/AdminPanel.Api/Inspection/OverviewQuery.cs src/tests/AdminPanel.UnitTests/InspectionMappersTests.cs
  git commit -m "t05: Overview — наполнение clusters/activeMoves из снапшота (unit)"
  ```

---

### Task 7: Integration — кластерная фикстура + HTTP-контракт `ClustersApiTests`

**Files:**
- Modify: `src/tests/AdminPanel.IntegrationTests/InspectionApiTests.cs` (+ `InspectionSnapshots.Clustered`; правка `Overview_WithSnapshot_ReturnsDto`)
- Create: `src/tests/AdminPanel.IntegrationTests/ClustersApiTests.cs`

**Interfaces:**
- Consumes: фабрика `"api"`/`AuthWebFactory`/`ApiTestLogin`/`TestSnapshotStore` (t04, без правок фабрики), `InspectionSnapshots.Fixture` (t04), кластерные эндпоинты (Tasks 4–6).
- Produces: `InspectionSnapshots.Clustered(DateTimeOffset builtAt, DateTimeOffset now) → EtcdSnapshot` (Fixture + кластер demo: 2 шарда, дыра bucket_4, SYNCING −30/FROZEN −10/ABORTING −5, 2 heals) — переиспользуется Task 8 при необходимости и t06+.

- [ ] **Шаг 1: Кластерная фикстура + правка Overview-ассертов**

  Вход: Tasks 4–6 слиты.

  Действие: в `src/tests/AdminPanel.IntegrationTests/InspectionApiTests.cs`:

  1) Добавить в `InspectionSnapshots` после `Fixture`:

  ```csharp
      // Кластерный снапшот HTTP-тестов (spec §9): Fixture + кластер demo — 2 шарда (s2 без master),
      // бакеты 0..15 (у 4 — дыра), SYNCING −30 c / FROZEN −10 c / ABORTING −5 c, 2 heals.
      public static EtcdSnapshot Clustered(DateTimeOffset builtAt, DateTimeOffset now)
      {
          var unix = now.ToUnixTimeSeconds();
          var cluster = new ClusterInfo(
              "demo", "demo", 16, 1755800000,
              [
                  new ShardInfo("s1", "host=s1a,s1b port=5432 dbname=demo user=postgres",
                      ["s1a", "s1b"], 5432, "demo", "postgres", 1, "s1a:5432", null),
                  new ShardInfo("s2", "host=s2a,s2b port=5432 dbname=demo user=postgres",
                      ["s2a", "s2b"], 5432, "demo", "postgres", 1, null, null),
              ],
              [.. Enumerable.Range(0, 16).Select(i => i switch
              {
                  1 => new BucketInfo(1, "s1", BucketState.Syncing,
                      new MoveInfo("s1", "s2", unix - 130, unix - 30, "copy", null)),
                  2 => new BucketInfo(2, "s1", BucketState.Frozen,
                      new MoveInfo("s1", "s2", unix - 70, unix - 10, "cutover-wait", null)),
                  3 => new BucketInfo(3, "s2", BucketState.Aborting,
                      new MoveInfo("s2", "s1", unix - 45, unix - 5, "cleanup", "receiver went away")),
                  4 => new BucketInfo(4, null, BucketState.Active, null),
                  _ => new BucketInfo(i, i % 2 == 0 ? "s1" : "s2", BucketState.Active, null),
              })],
              [
                  new HealRecord("bucket_5", "s2", "s1", "restore-heal", unix - 3600),
                  new HealRecord("bucket_9", "s1", "s2", "restore-heal", unix - 7200),
              ]);
          return Fixture(builtAt) with { Clusters = [cluster] };
      }
  ```

  2) В `Overview_WithSnapshot_ReturnsDto` заменить строку Arrange-снапшота на `_factory.Snapshot = InspectionSnapshots.Clustered(_factory.Time.Utc, _factory.Time.Utc);` (порядок «логин → снапшот» уже соблюдён в тесте) и заменить два финальных ассерта `clusters`/`activeMoves` на:

  ```csharp
          var clusters = dto.GetProperty("clusters");
          clusters.GetArrayLength().Should().Be(1);
          clusters[0].GetProperty("name").GetString().Should().Be("demo");
          clusters[0].GetProperty("shards").GetInt32().Should().Be(2);
          clusters[0].GetProperty("buckets").GetInt32().Should().Be(16);
          clusters[0].GetProperty("activeMoves").GetInt32().Should().Be(3);
          clusters[0].GetProperty("masterlessShards").GetInt32().Should().Be(1);
          dto.GetProperty("activeMoves").GetArrayLength().Should().Be(3);
  ```

  3) В `Endpoints_NoSnapshot_Return503ProblemDetails` дополнить Act двумя запросами и Assert двумя строками:

  ```csharp
          var clustersList = await client.GetAsync("/api/clusters", TestContext.Current.CancellationToken);
          var clusterDetails = await client.GetAsync("/api/clusters/demo", TestContext.Current.CancellationToken);
          // … (в Assert)
          clustersList.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
          clusterDetails.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
  ```

  Выход: фикстура + наполненные Overview-ассерты.

  Проверка: `dotnet build src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj 2>&1 | tail -3` → успех, 0 warnings.

- [ ] **Шаг 2: HTTP-контрактные тесты (failing → зелёные вместе с фикстурой шага 1)**

  Действие: создать `src/tests/AdminPanel.IntegrationTests/ClustersApiTests.cs`:

  ```csharp
  using System.Net;
  using System.Net.Http.Json;
  using System.Text.Json;
  using AdminPanel.Core;
  using FluentAssertions;
  using Microsoft.AspNetCore.Mvc.Testing;
  using Xunit;

  namespace AdminPanel.IntegrationTests;

  // HTTP-контракт кластерных эндпоинтов: 401/503/200/404/400/фильтры (spec §9.2).
  [Collection("api")]
  public class ClustersApiTests
  {
      private readonly AuthWebFactory _factory;

      public ClustersApiTests(AuthWebFactory factory) => _factory = factory;

      private Task<HttpClient> LoginAsync() => ApiTestLogin.LoginAsync(_factory);

      private async Task<JsonElement> GetJsonAsync(HttpClient client, string url)
      {
          using var response = await client.GetAsync(url, TestContext.Current.CancellationToken);
          response.StatusCode.Should().Be(HttpStatusCode.OK);
          return await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
      }

      // Порядок Arrange: сначала логин (+61 c окна лимитера), затем снапшот по текущему времени
      // фабрики — ageSec/snapshotAgeMs считаются от factory.Time (прецедент t04).
      private void SetClusteredSnapshot()
          => _factory.Snapshot = InspectionSnapshots.Clustered(_factory.Time.Utc, _factory.Time.Utc);

      [Fact]
      public async Task Clusters_WithoutCookie_Return401()
      {
          // Arrange
          using var client = _factory.CreateClient(new WebApplicationFactoryClientOptions { AllowAutoRedirect = false });

          // Act
          var list = await client.GetAsync("/api/clusters", TestContext.Current.CancellationToken);
          var details = await client.GetAsync("/api/clusters/demo", TestContext.Current.CancellationToken);

          // Assert: default-deny guard закрыл новые эндпоинты без правок auth.
          list.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
          details.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
      }

      [Fact]
      public async Task Clusters_NoSnapshot_Return503ProblemDetails()
      {
          // Arrange
          _factory.Snapshot = null;
          using var client = await LoginAsync();

          // Act
          var list = await client.GetAsync("/api/clusters", TestContext.Current.CancellationToken);
          var details = await client.GetAsync("/api/clusters/demo", TestContext.Current.CancellationToken);

          // Assert
          list.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
          list.Content.Headers.ContentType!.MediaType.Should().Be("application/problem+json");
          var body = await list.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
          body.GetProperty("title").GetString().Should().Be("Snapshot not ready");
          details.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
      }

      [Fact]
      public async Task Clusters_WithSnapshot_ReturnSummaries()
      {
          // Arrange
          using var client = await LoginAsync();
          SetClusteredSnapshot();

          // Act
          var clusters = await GetJsonAsync(client, "/api/clusters");

          // Assert: сводка кластера фикстуры (spec §9.2).
          clusters.GetArrayLength().Should().Be(1);
          var summary = clusters[0];
          summary.GetProperty("name").GetString().Should().Be("demo");
          summary.GetProperty("dbname").GetString().Should().Be("demo");
          summary.GetProperty("bucketsCount").GetInt32().Should().Be(16);
          summary.GetProperty("shardsTotal").GetInt32().Should().Be(2);
          summary.GetProperty("shardsWithMaster").GetInt32().Should().Be(1);
          summary.GetProperty("activeMoves").GetInt32().Should().Be(3);
          summary.GetProperty("incomplete").GetBoolean().Should().BeFalse();
      }

      [Fact]
      public async Task ClusterDetails_ReturnsConfigShardsBucketsHeals()
      {
          // Arrange
          using var client = await LoginAsync();
          SetClusteredSnapshot();

          // Act
          var dto = await GetJsonAsync(client, "/api/clusters/demo");

          // Assert
          dto.GetProperty("name").GetString().Should().Be("demo");
          dto.GetProperty("createdUnix").GetInt64().Should().Be(1755800000);
          var shards = dto.GetProperty("shards");
          shards.GetArrayLength().Should().Be(2);
          shards[0].GetProperty("hosts").GetArrayLength().Should().Be(2);
          shards[0].GetProperty("masterLeaseAlive").GetBoolean().Should().BeTrue();
          shards[1].GetProperty("masterLeaseAlive").GetBoolean().Should().BeFalse();
          shards[1].GetProperty("masterAddress").ValueKind.Should().Be(JsonValueKind.Null);
          shards[0].GetProperty("runtime").ValueKind.Should().Be(JsonValueKind.Null); // данные — t06 (spec §3.14)
          dto.GetProperty("buckets").GetArrayLength().Should().Be(16);
          var heals = dto.GetProperty("heals");
          heals.GetArrayLength().Should().Be(2);
          heals[0].GetProperty("bucket").GetString().Should().Be("bucket_5"); // новые сверху (spec §3.3)
      }

      [Fact]
      public async Task ClusterDetails_AgeSec_ForNonActiveBuckets()
      {
          // Arrange
          using var client = await LoginAsync();
          SetClusteredSnapshot();

          // Act
          var dto = await GetJsonAsync(client, "/api/clusters/demo");

          // Assert: возраст не-ACTIVE от updated_unix; ACTIVE — move/ageSec null (spec §3.7).
          var buckets = dto.GetProperty("buckets");
          buckets[1].GetProperty("state").GetString().Should().Be("SYNCING");
          buckets[1].GetProperty("ageSec").GetInt64().Should().Be(30);
          buckets[1].GetProperty("move").GetProperty("target").GetString().Should().Be("s2");
          buckets[2].GetProperty("state").GetString().Should().Be("FROZEN");
          buckets[2].GetProperty("ageSec").GetInt64().Should().Be(10);
          buckets[3].GetProperty("state").GetString().Should().Be("ABORTING");
          buckets[3].GetProperty("move").GetProperty("lastError").GetString().Should().Be("receiver went away");
          buckets[0].GetProperty("state").GetString().Should().Be("ACTIVE");
          buckets[0].GetProperty("ageSec").ValueKind.Should().Be(JsonValueKind.Null);
          buckets[0].GetProperty("move").ValueKind.Should().Be(JsonValueKind.Null);
      }

      [Fact]
      public async Task ClusterDetails_OwnerFilter_ReturnsOnlyMatching()
      {
          // Arrange
          using var client = await LoginAsync();
          SetClusteredSnapshot();

          // Act
          var dto = await GetJsonAsync(client, "/api/clusters/demo?owner=s2");

          // Assert: 7 бакетов s2 (6 routing + ABORTING bucket_3); shards/heals не фильтруются (spec §3.9).
          dto.GetProperty("buckets").GetArrayLength().Should().Be(7);
          dto.GetProperty("shards").GetArrayLength().Should().Be(2);
          dto.GetProperty("heals").GetArrayLength().Should().Be(2);
      }

      [Fact]
      public async Task ClusterDetails_StateFilter_ActiveIncluded()
      {
          // Arrange
          using var client = await LoginAsync();
          SetClusteredSnapshot();

          // Act
          var active = await GetJsonAsync(client, "/api/clusters/demo?state=ACTIVE");
          var syncing = await GetJsonAsync(client, "/api/clusters/demo?state=SYNCING");
          var both = await GetJsonAsync(client, "/api/clusters/demo?owner=s2&state=ABORTING");

          // Assert: ACTIVE входит в фильтр (roadmap t05); фильтры сочетаются (AND).
          active.GetProperty("buckets").GetArrayLength().Should().Be(13);
          syncing.GetProperty("buckets").GetArrayLength().Should().Be(1);
          both.GetProperty("buckets").GetArrayLength().Should().Be(1);
      }

      [Fact]
      public async Task ClusterDetails_UnknownOwner_ReturnsEmptyBuckets()
      {
          // Arrange
          using var client = await LoginAsync();
          SetClusteredSnapshot();

          // Act
          var dto = await GetJsonAsync(client, "/api/clusters/demo?owner=nope");

          // Assert: пустой buckets и 200 — имена шардов эволюционируют (spec §3.9).
          dto.GetProperty("buckets").GetArrayLength().Should().Be(0);
      }

      [Fact]
      public async Task ClusterDetails_UnknownCluster_Returns404ProblemDetails()
      {
          // Arrange
          using var client = await LoginAsync();
          SetClusteredSnapshot();

          // Act
          using var response = await client.GetAsync("/api/clusters/ghost", TestContext.Current.CancellationToken);

          // Assert
          response.StatusCode.Should().Be(HttpStatusCode.NotFound);
          response.Content.Headers.ContentType!.MediaType.Should().Be("application/problem+json");
          var body = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
          body.GetProperty("title").GetString().Should().Be("Cluster not found");
      }

      [Fact]
      public async Task ClusterDetails_InvalidState_Returns400ProblemDetails()
      {
          // Arrange
          using var client = await LoginAsync();
          SetClusteredSnapshot();

          // Act
          using var response = await client.GetAsync(
              "/api/clusters/demo?state=bogus", TestContext.Current.CancellationToken);

          // Assert: опечатка фронта ловится сразу, а не пустым списком (spec §3.9).
          response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
          response.Content.Headers.ContentType!.MediaType.Should().Be("application/problem+json");
      }

      [Fact]
      public async Task Clusters_IncompleteCluster_Flagged()
      {
          // Arrange: ghost без config — incomplete=true, dbname null (spec §9.2).
          using var client = await LoginAsync();
          var clustered = InspectionSnapshots.Clustered(_factory.Time.Utc, _factory.Time.Utc);
          _factory.Snapshot = clustered with
          {
              Clusters =
              [
                  .. clustered.Clusters,
                  new ClusterInfo("ghost", null, 0, null, [], [], []),
              ],
          };

          // Act
          var clusters = await GetJsonAsync(client, "/api/clusters");

          // Assert
          clusters.GetArrayLength().Should().Be(2);
          clusters[1].GetProperty("name").GetString().Should().Be("ghost");
          clusters[1].GetProperty("incomplete").GetBoolean().Should().BeTrue();
          clusters[1].GetProperty("dbname").ValueKind.Should().Be(JsonValueKind.Null);
      }
  }
  ```

  Выход: 11 контрактных тестов.

  Проверка (Docker не нужен — фикстурных контейнеров в фильтре нет): `dotnet test src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj --filter "FullyQualifiedName~ClustersApiTests|FullyQualifiedName~InspectionApiTests|FullyQualifiedName~AuthTests|FullyQualifiedName~HealthzTests" 2>&1 | tail -3` → PASS: **Passed: 32, Failed: 0** (11 новых ClustersApiTests + 11 фактов класса InspectionApiTests [401/503/overview/etcd-status/alerts t04, с правками шага 1] + 9 AuthTests + 1 HealthzTests; подстрока «InspectionApiTests» не матчит классы `InspectionEtcdApiTests`/`InspectionSeededAnomaliesApiTests` — контейнеры не поднимаются).

- [ ] **Шаг 3: Полный integration-прогон (Docker) и коммит**

  Проверка (нужен Docker): `dotnet test src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj 2>&1 | tail -3` → `Passed! - Failed: 0, Passed: 43, Skipped: 0, Total: 43` (32 + 11). Живой-etcd классы (`InspectionEtcdApiTests`/`InspectionSeededAnomaliesApiTests`) и `EtcdSnapshotIntegrationTests` пока зелёные на старых ассертах: их `EtcdTestHarness.NewRefresher` всё ещё со списком 7 правил t04 (Task 8).

  Коммит:

  ```bash
  git add src/tests/AdminPanel.IntegrationTests/InspectionApiTests.cs src/tests/AdminPanel.IntegrationTests/ClustersApiTests.cs
  git commit -m "t05: integration — HTTP-контракт кластерных эндпоинтов (401/503/200/404/400/фильтры)"
  ```

---

### Task 8: Integration — правила t05 в живом-etcd харнессе + ассерты сида demo

**Files:**
- Modify: `src/tests/AdminPanel.IntegrationTests/EtcdSnapshotIntegrationTests.cs` (харнесс `NewRefresher` + ассерты двух тестов)
- Modify: `src/tests/AdminPanel.IntegrationTests/InspectionApiTests.cs` (`LiveEtcd_…`, `InspectionSeededAnomaliesApiTests`)

**Interfaces:**
- Consumes: 8 правил t05 (Tasks 2–3), `AlertsOptions` (Task 1), сид demo `EtcdSeed`/`clusters-full.json` (t03; штампы фиксированы в прошлом — spec §3.15).
- Produces: `EtcdTestHarness.NewRefresher` с полным набором 15 правил — путь данных «живой etcd → refresher → AlertEngine(15 правил) → API» (spec §9.3).

- [ ] **Шаг 1: Харнесс + ассерты `Refresher_RefreshOnce_BuildsExpectedSnapshot`**

  Вход: Task 7 слит; Docker доступен.

  Действие: в `src/tests/AdminPanel.IntegrationTests/EtcdSnapshotIntegrationTests.cs`:

  1) В `EtcdTestHarness.NewRefresher` заменить список правил в `new AlertEngine([...])` на 15 (spec §3.16) и добавить using `using Microsoft.Extensions.Options;`:

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
                  new ShardNoMasterRule(),
                  new MoveStaleRule(Options.Create(new AlertsOptions())),
                  new MoveFrozenLongRule(Options.Create(new AlertsOptions())),
                  new MoveAbortingRule(),
                  new MoveFlippedStatusStuckRule(),
                  new BucketLostRule(),
                  new BucketNoRoutingRule(),
                  new BucketOutOfRangeRule(),
              ]),
              store,
              Options.Create(new EtcdOptions { Endpoints = endpoints }),
              new RealTimeProvider(),
              NullLogger<SnapshotRefresher>.Instance);
  ```

  2) В `Refresher_RefreshOnce_BuildsExpectedSnapshot` заменить ассерт `snapshot.Alerts.Should().BeEmpty();` на (штампы сида — август 2025, реальное время прогона позже всегда):

  ```csharp
          // t05: сид demo несёт 3 статус-ключа с протухшими штампами → ровно 5 move-алертов (spec §3.15);
          // сортировка: critical (frozen-long) → warnings по kind/target (Ordinal).
          string.Join("|", snapshot.Alerts.Select(a => a.Id))
              .Should().Be("move-frozen-long:demo/bucket_11|move-aborting:demo/bucket_7|move-stale:demo/bucket_11|move-stale:demo/bucket_3|move-stale:demo/bucket_7");
  ```

  Выход: харнесс с 15 правилами; ассерт точного состава.

  Проверка (нужен Docker): `dotnet test src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj --filter "FullyQualifiedName~EtcdSnapshotIntegrationTests|FullyQualifiedName~EtcdFailureTests" 2>&1 | tail -3` → PASS: **Passed: 9, Failed: 0** (8 + 1; `EtcdFailureTests` не затронут: ghost-аномалий в его сценарии нет, unreachable-ассерт прежний).

- [ ] **Шаг 2: Живой-etcd API-смоук (кластеры + 5 move-алертов) и SeededAnomalies**

  Действие: в `src/tests/AdminPanel.IntegrationTests/InspectionApiTests.cs`:

  1) В `InspectionEtcdApiTests` переименовать тест `LiveEtcd_Endpoints_ReflectRealSnapshot` → `LiveEtcd_InspectionEndpoints_ReflectRealSnapshot` и привести к виду (полный листинг тела):

  ```csharp
      [Fact]
      public async Task LiveEtcd_InspectionEndpoints_ReflectRealSnapshot()
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
          using var clustersList = await client.GetAsync("/api/clusters", TestContext.Current.CancellationToken);
          using var details = await client.GetAsync("/api/clusters/demo", TestContext.Current.CancellationToken);

          // Assert: etcd жив; 5 move-алертов протухшего сида demo (spec §3.15); кластеры отдают данные.
          status.StatusCode.Should().Be(HttpStatusCode.OK);
          var etcd = await status.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
          etcd.GetProperty("endpoints")[0].GetProperty("version").GetString().Should().Be("3.5.21");
          etcd.GetProperty("members")[0].GetProperty("name").GetString().Should().Be("test");
          var overviewDto = await overview.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
          overviewDto.GetProperty("etcd").GetProperty("reachable").GetBoolean().Should().BeTrue();
          overviewDto.GetProperty("etcd").GetProperty("endpointsOk").GetInt32().Should().Be(1);
          var alertList = await alerts.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
          string.Join("|", alertList.EnumerateArray().Select(a => a.GetProperty("id").GetString()))
              .Should().Be("move-frozen-long:demo/bucket_11|move-aborting:demo/bucket_7|move-stale:demo/bucket_11|move-stale:demo/bucket_3|move-stale:demo/bucket_7");
          clustersList.StatusCode.Should().Be(HttpStatusCode.OK);
          var summaries = await clustersList.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
          summaries.GetArrayLength().Should().Be(1);
          summaries[0].GetProperty("shardsWithMaster").GetInt32().Should().Be(2); // оба master сида живы
          var detailsDto = await details.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
          detailsDto.GetProperty("buckets").GetArrayLength().Should().Be(16);
          detailsDto.GetProperty("buckets")[3].GetProperty("state").GetString().Should().Be("SYNCING");
          detailsDto.GetProperty("buckets")[3].GetProperty("move").GetProperty("target").GetString().Should().Be("s2");
      }
  ```

  2) В `InspectionSeededAnomaliesApiTests.LiveEtcd_SeededAnomalies_ProduceAlerts` заменить блок Assert (строки `alerts.GetArrayLength().Should().Be(2);` … `alerts[1].GetProperty("sinceUnix")...`) на:

  ```csharp
          // Assert: 5 move-алертов сида demo + shard-no-master:ghost/g1 (dsn-шард ghost без master —
          // живое покрытие P11-правила, сид не сужается; spec §9.3) + cluster-incomplete:ghost
          // + key-malformed битого ключа; порядок severity → kind → target (Ordinal);
          // sinceUnix null — первое наблюдение (spec §3.4).
          alerts.GetArrayLength().Should().Be(8);
          string.Join("|", alerts.EnumerateArray().Select(a => a.GetProperty("id").GetString()))
              .Should().Be("move-frozen-long:demo/bucket_11|shard-no-master:ghost/g1|cluster-incomplete:ghost|key-malformed:/clusters/demo/buckets/status/bucket_1|move-aborting:demo/bucket_7|move-stale:demo/bucket_11|move-stale:demo/bucket_3|move-stale:demo/bucket_7");
          alerts[1].GetProperty("kind").GetString().Should().Be("shard-no-master");
          alerts[1].GetProperty("target").GetString().Should().Be("ghost/g1");
          alerts[2].GetProperty("target").GetString().Should().Be("ghost");
          alerts[3].GetProperty("sinceUnix").ValueKind.Should().Be(JsonValueKind.Null);
  ```

  Выход: путь данных живой etcd → API проверяет и кластерные эндпоинты, и move-алерты.

  Проверка (нужен Docker): `dotnet test src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj --filter "FullyQualifiedName~InspectionEtcdApiTests|FullyQualifiedName~InspectionSeededAnomaliesApiTests" 2>&1 | tail -3` → PASS: **Passed: 2, Failed: 0**.

- [ ] **Шаг 3: Полный integration-прогон и коммит**

  Проверка (нужен Docker): `dotnet test src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj 2>&1 | tail -3` → `Passed! - Failed: 0, Passed: 43, Skipped: 0, Total: 43` (число тестов не менялось — правки ассертов).

  Коммит:

  ```bash
  git add src/tests/AdminPanel.IntegrationTests/EtcdSnapshotIntegrationTests.cs src/tests/AdminPanel.IntegrationTests/InspectionApiTests.cs
  git commit -m "t05: integration — живой etcd: 15 правил, move-алерты сида demo, clusters-смоук"
  ```

---

### Task 9: Финальная верификация + roadmap-деливерабл + финальный коммит

**Files:**
- Modify: git (финальный коммит: `docs/superpowers/2026-08-22-t05-sharding-api/` + `arch/roadmap/sharding.md` — правка уже в рабочем дереве с фазы spec)

**Interfaces:**
- Consumes: всё выше.
- Produces: ветка `feat-t05-sharding-api`, готовая к ревью dev-flow.

- [ ] **Шаг 1: Финальная верификация (критерии приёмки spec §15)**

  Вход: Tasks 1–8 слиты.

  Действие и проверка (каждая команда — зелёная):

  1. `dotnet build src/AdminPanel.slnx 2>&1 | tail -3` → `Build succeeded` / `0 Warning(s)` / `0 Error(s)` (критерий §15.1).
  2. `dotnet test src/AdminPanel.slnx 2>&1 | tail -3` (нужен Docker) → `Passed! - Failed: 0, Passed: 196, Skipped: 0, Total: 196` (153 unit + 43 integration; критерий §15.2).
  3. `git diff --name-only main -- src/AdminPanel.Core/Alerting/AlertEngine.cs src/AdminPanel.Core/Alerting/IAlertEngine.cs src/AdminPanel.Etcd/SnapshotRefresher.cs` → пусто: каркас и refresher не тронуты (критерий §15.5).
  4. `grep -rn "PackageReference" src/ --include="*.csproj"` → состав пакетов идентичен t04 (критерий §15.7 — новых нет).
  5. `grep -rn "v3/kv/put\|v3/lease" src/AdminPanel.Api src/AdminPanel.Core src/AdminPanel.Etcd src/AdminPanel.Infrastructure src/AdminPanel.Probes --include="*.cs"` → пусто: панель не пишет в etcd (критерий §15.8; `kv/put` живёт только в тестовом `EtcdSeed`).
  6. `grep -rn "KindName = \"" src/AdminPanel.Core/Alerting/Rules/ | wc -l` → **15** (7 t04 + 8 t05; ручная сверка каталога §15.3).
  7. Анкер roadmap: `grep -rn "t05-sharding-api" arch/roadmap/*.md` → ровно **1** строка — `frontend.md` (`t08-frontend-clusters ← t05-sharding-api`); в `sharding.md` — ни одной (пункт удалён фазой spec; зависимость t08 НЕ трогаем — spec §14). Ложный FAIL исключён: grep по всем файлам каталога, ожидаемое число выписано точно.
  8. `git diff --stat -- arch/` → единственный изменённый файл `arch/roadmap/sharding.md` (мутаций arch/01–04 нет — критерий §15.9/§15.10).

  Выход: все критерии приёмки подтверждены выводами команд.

- [ ] **Шаг 2: Финальный коммит (spec + plan + roadmap)**

  Вход: верификация шага 1 зелёная.

  Действие:

  ```bash
  git add docs/superpowers/2026-08-22-t05-sharding-api arch/roadmap/sharding.md
  git commit -m "t05: spec/plan задачи + roadmap-деливерабл (удаление пункта t05-sharding-api)"
  ```

  Выход: ветка `feat-t05-sharding-api` готова к ревью; дальнейшие действия (ревью, мерж, пуш) — вне плана, по команде координатора.

  Проверка: `git log --oneline -10` → 9 коммитов t05 (Tasks 1–9) поверх HEAD фазы spec; `git status --porcelain` → пусто.

---

## Сводка задач

| # | Задача | Тесты (новые) | Коммит |
|---|---|---|---|
| 1 | MoveAge + AlertsOptions + appsettings + MovingCluster | 5 unit | `t05: MoveAge + AlertsOptions (AdminPanel:Alerts) + фикстура MovingCluster (unit)` |
| 2 | 6 безпороговых правил (shard-no-master, move-aborting/flipped, bucket-*) | 8 unit | `t05: безпороговые правила шардирования — shard-no-master, move-aborting/flipped, bucket-* (unit)` |
| 3 | move-stale/move-frozen-long + 15 правил в харнессах + правки ассертов | 6 unit + 2 правки | `t05: пороговые move-stale/move-frozen-long + 15 правил в харнессах (unit)` |
| 4 | GET /api/clusters — сводный список | 4 unit | `t05: GET /api/clusters — сводный список кластеров (unit)` |
| 5 | GET /api/clusters/{cluster} — детали/фильтры/404 + маршруты | 9 unit | `t05: GET /api/clusters/{cluster} — детали, фильтры owner/state, 404 (unit)` |
| 6 | Overview — наполнение кластерной части | +1 unit (−1 stub +2) | `t05: Overview — наполнение clusters/activeMoves из снапшота (unit)` |
| 7 | Integration: фикстура Clustered + ClustersApiTests + Overview-ассерты | 11 integration | `t05: integration — HTTP-контракт кластерных эндпоинтов (401/503/200/404/400/фильтры)` |
| 8 | Integration: 15 правил в NewRefresher + ассерты сида demo (5/8 алертов) | 0 (правки) | `t05: integration — живой etcd: 15 правил, move-алерты сида demo, clusters-смоук` |
| 9 | Финальная верификация + spec/plan/roadmap-коммит | — | `t05: spec/plan задачи + roadmap-деливерабл (удаление пункта t05-sharding-api)` |

Контрольные счётчики прогонов (полные итоговые строки `dotnet test`):
- База: unit `Passed: 120`, integration `Passed: 32`.
- После Task 1: unit `125`; Task 2: `133`; Task 3: `139` (по классу ShardingAlertRulesTests `14`, AlertEngineTests `18`); Task 4: `143`; Task 5: `152`; Task 6: `153`; Task 7: integration `43`; Task 8: integration `43`; финал: `Total: 196` (153 + 43).
