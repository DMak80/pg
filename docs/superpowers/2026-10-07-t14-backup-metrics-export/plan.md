# t14-backup-metrics-export — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Бэкапные статусы PgWorker становятся рядами Prometheus (7 серий словаря arch/18 §2.7): возраст валидного полного + per-cluster порог, WAL-лаг и тишина загрузок, исходы verify/restore/drill; группа алертов `backups` и Grafana-дашборд `backups.json`.

**Architecture:** Отдельного коллектора нет — серии питаются тиками существующих процессов (`BackupProcess`, `WalStreamProcess`, `BackupProcess` verify, `RestoreProcess`, `RestoreDrillProcess`) через nullable observer-делегаты (паттерн уже заложен `lagObserver`/`verifyObserver`). Стейт серий — в `WorkerMetricsInstrumentation` (ObservableGauge-колбэки пересчитывают возраст на каждом scrape; набор шардов тика замещает стейт кластера; null-факт удаляет серию). Контракт etcd (arch/19 §4) не меняется.

**Tech Stack:** .NET 10, System.Diagnostics.Metrics (OTel Prometheus-экспортер уже подключён), xUnit + FluentAssertions; Prometheus rules.yml + Grafana provisioning JSON (bash-чек стенда).

**Spec:** `docs/superpowers/2026-10-07-t14-backup-metrics-export/spec.md` (в этом же каталоге; план спорит от spec — executors читают оба).

## Global Constraints

- Работа ТОЛЬКО в worktree `/Users/demakaev/ZCodeProject/worktrees/feat-t14-backup-metrics-export`; команды сборки/тестов — из его корня (абсолютные пути ниже опущены до относительных от корня worktree).
- .NET 10, `Nullable=enable`, **`TreatWarningsAsErrors=true`** — сборка обязана быть 0 warnings.
- Централизованное версионирование (CPM, `Directory.Packages.props`): НОВЫХ пакетов нет — всё на уже подключённых System.Diagnostics.Metrics/OTel.
- Метрики — пассивные наблюдатели (spec §2): марк-методы `WorkerMetricsInstrumentation` никогда не бросают исключений (try/catch «Пассивный наблюдатель»); делегаты процессов — nullable, null-вызов безопасен.
- Лейблы конечны (spec §2, M1): только `cluster`, `shard`, `result` из фиксированных словарей (`result` ∈ ok|failed|transient для verify, ok|failed для restore/drill); свободные строки в лейблы не попадают.
- Семантика «валидный полный» едина (spec §2): `BackupPlanner.IsValid` (COMPLETED и verify ≠ FAILED), толерантно `finished_unix ?? started_unix` — метрика третьего толкования не вводит.
- Никаких новых конфиг-опций (spec §5); пороги правил = дефолты конфига: `Wal:LagMaxSegments=1024`, `Wal:StaleSec=300` (`src/PgWorker.Backups/Options.cs`).
- Комментарии/доки — русский; идентификаторы — английский; тесты — с AAA-комментариями.
- Коммиты в стиле репо: `feat(t14): …` / `test(t14): …` / `docs(t14): …` (описание на русском).
- Интеграционные тесты не-E2E поднимают docker-etcd (`EtcdFixture`/`OwnEtcd`): после КАЖДОЙ серии — контроль зачистки (`docker ps --filter name=pgw-`, `docker network ls | grep kfw-net` → `docker network prune -f` при осиротевших) — канон AGENTS.md.
- E2E-прогон — только на финальном гейте (Task 11) с env `PGW_TEST_DOCKER=1`; перезапуски упавших тестов без анализа логов запрещены.

## Карта файлов

| Файл | Ответственность |
|---|---|
| `src/PgWorker.Backups/Process/BackupPlanner.cs` | + чистая `LastValidUnix` (источник age-серии; DRY в `IsDue`) |
| `src/Shared.Metrics/Worker/WorkerMetricsInstrumentation.cs` | Марк-методы/инструменты 7 серий §2.7: counters restore/drill/verify(лейблы), gauges full_age/full_max_age/wal_uploaded_age |
| `src/PgWorker.Backups/Process/BackupProcess.cs` | + `fullAgeObserver`: тик передаёт набор `(shard → lastValidUnix, maxAge)` |
| `src/PgWorker.Backups/WalStreamProcess.cs` | + `uploadedAgeObserver` рядом с `lagObserver`; null-семантика BROKEN/STOPPED/нет факта |
| `src/PgWorker.Backups/Process/RestoreProcess.cs` | + `restoreObserver`: терминальные COMPLETED/FAILED |
| `src/PgWorker.Backups/Process/RestoreDrillProcess.cs` | + `drillObserver`: чистый итог (снятие `cleaning`) + FAILED-валидация без джоба |
| `src/PgWorker.App/Program.cs` | wiring 4 делегатов на `WorkerMetricsInstrumentation` |
| `dev-stand/adminpanel/metrics/prometheus/rules.yml` | + группа `backups` (7 алертов) |
| `dev-stand/adminpanel/metrics/grafana/dashboards/backups.json` | новый дашборд (6 панелей) |
| `dev-stand/adminpanel/checks/65-metrics.sh` | счётчики: 18 алертов, 5 дашбордов |
| `src/tests/…` (см. задачи) | TDD: instrumentation-юниты, процессы, канон-тест /metrics |
| `arch/roadmap/reliability.md`, `arch/roadmap/reliability-report.md` | мерж-гейт: снятие тега t14 (Task 11) |

Контракт типов между задачами (единственное место определения — здесь; задачи ниже повторяют сигнатуры):

```csharp
// BackupPlanner (Task 2)
public static long? LastValidUnix(IReadOnlyList<FullBackupState> fulls)

// WorkerMetricsInstrumentation (Task 3, Task 4)
public void BackupFullAge(string cluster,
    IReadOnlyDictionary<string, (long? LastValidUnix, long MaxAgeSec)> shards)
public void BackupWalUploadedAge(string cluster, string shard, long? ageSec)
public void BackupRestore(string cluster, string shard, string result)
public void BackupDrill(string cluster, string shard, string result)
// public void BackupVerify(string cluster, string shard, string result) — сигнатура прежняя,
// counter получает лейблы cluster/shard (миграция серии)

// Делегаты процессов (Tasks 5–8) — все nullable, примитивы/кортежи без типов Shared.Metrics
Action<string, IReadOnlyDictionary<string, (long? LastValidUnix, long MaxAgeSec)>>? fullAgeObserver   // BackupProcess
Action<string, string, long?>? uploadedAgeObserver  // WalStreamProcess: (cluster, shard, ageSec|null)
Action<string, string, string>? restoreObserver     // RestoreProcess: (cluster, shard, "ok"|"failed")
Action<string, string, string>? drillObserver       // RestoreDrillProcess: (cluster, shard, "ok"|"failed")
```

Замечание о возрасте: делегаты передают возрастной ФАКТ (`LastValidUnix` / `ageSec`), а ObservableGauge-колбэк instrumentation хранит unix и пересчитывает `now − unix` на каждом scrape (паттерн `worker_snapshot_age_seconds`, spec §7 M3) — между тиками значение не замирает.

---

### Task 1: Зафиксировать spec + arch/18 (docs-коммит)

Спека одобрена на user-review, правки `arch/18-metrics.md` (§2.7, §5.3, §6) уже лежат в рабочем дереве незакоммиченными. До любого кода — зафиксировать контракт в истории (arch-first, spec §2/§3.5).

**Files:**
- Commit (без изменений): `arch/18-metrics.md` (уже изменён), `docs/superpowers/2026-10-07-t14-backup-metrics-export/spec.md` (уже создан).

**Interfaces:**
- Consumes: одобренный spec.
- Produces: коммит контракта §2.7 — точка отсчёта код-задач (revert-грань ревью).

- [ ] **Step 1: Проверить незакоммиченные файлы**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t14-backup-metrics-export && git status --short
```
Expected: ` M arch/18-metrics.md` и `?? docs/superpowers/2026-10-07-t14-backup-metrics-export/`. Если arch/18 не изменён — СТОП: сверить с задачей main-агента (spec-фаза не завершена).

- [ ] **Step 2: Коммит**

```bash
git add arch/18-metrics.md docs/superpowers/2026-10-07-t14-backup-metrics-export/
git commit -m "docs(t14): spec + arch/18 §2.7/§5.3/§6 — словарь бэкап-серий, дашборд backups.json, счётчик рулов в E2E-чеке"
```

- [ ] **Step 3: Проверка**

```bash
git status --short
```
Expected: пусто (рабочее дерево чистое).

---

### Task 2: `BackupPlanner.LastValidUnix` — чистый источник age-серии

Единая семантика «валидный полный» (spec §2): age-серия берёт unix последнего валидного из того же `IsValid`, что и планировщик. Чистая функция — TDD на существующем хелпере `Full(...)`.

**Files:**
- Modify: `src/PgWorker.Backups/Process/BackupPlanner.cs` (после `IsDue`)
- Test: `src/tests/PgWorker.UnitTests/Backups/BackupPlannerTests.cs`

**Interfaces:**
- Consumes: закрытый `BackupPlanner.IsValid` (уже есть).
- Produces: `public static long? LastValidUnix(IReadOnlyList<FullBackupState> fulls)` — unix последнего валидного (`FinishedUnix ?? StartedUnix`), null — валидного нет. Используется Task 5 (набор наблюдателя BackupProcess) и DRY внутри `IsDue`.

- [ ] **Step 1: Пишем failing-тесты (в конец `BackupPlannerTests.cs`)**

```csharp
    // AAA: t14 — unix последнего валидного: COMPLETED с verify FAILED не считается,
    // finished_unix приоритетен, толерантность started_unix как в IsDue
    [Fact]
    public void LastValidUnix_ПоследнийВалидный_finished_или_started()
    {
        // Arrange — старый валидный (без verify), новее — COMPLETED c verify FAILED
        var validFinished = Unix(Now.AddHours(-2));
        var fulls = new[]
        {
            Full("20260911010000Z", FullBackupStatus.Completed,
                Unix(Now.AddHours(-2).AddMinutes(-5)), validFinished),
            Full("20260911020000Z", FullBackupStatus.Completed, Unix(Now.AddHours(-1)), Unix(Now.AddHours(-1)),
                verify: new BackupVerify(BackupVerifyStatus.Failed, Unix(Now.AddHours(-1)), "bad")),
        };

        // Act / Assert — валиден только первый: его finished_unix
        BackupPlanner.LastValidUnix(fulls).Should().Be(validFinished);
    }

    // AAA: t14 — валидного нет вовсе (только FAILED-статусы/verify) → null
    [Fact]
    public void LastValidUnix_ВалидныхНет_null()
    {
        // Arrange
        var fulls = new[]
        {
            Full("20260911020000Z", FullBackupStatus.Failed, Unix(Now.AddHours(-1))),
            Full("20260911030000Z", FullBackupStatus.Completed, Unix(Now.AddHours(-1)), Unix(Now.AddHours(-1)),
                verify: new BackupVerify(BackupVerifyStatus.Failed, Unix(Now), "bad")),
        };

        // Act / Assert
        BackupPlanner.LastValidUnix(fulls).Should().BeNull();
    }

    // AAA: t14 — finished_unix отсутствует → толерантно started_unix (образец IsDue)
    [Fact]
    public void LastValidUnix_БезFinished_БерётStarted()
    {
        // Arrange — COMPLETED ещё без finished_unix (аномалия ключа)
        var started = Unix(Now.AddHours(-1));
        var fulls = new[] { Full("20260911030000Z", FullBackupStatus.Completed, started) };

        // Act / Assert
        BackupPlanner.LastValidUnix(fulls).Should().Be(started);
    }
```

- [ ] **Step 2: Прогон — убедиться в отказе компиляции**

```bash
dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release --filter "FullyQualifiedName~BackupPlannerTests"
```
Expected: FAIL (CS0117 — `BackupPlanner` не содержит `LastValidUnix`).

- [ ] **Step 3: Реализация**

В `BackupPlanner.cs` после `IsValid` добавить (и заменить в `IsDue` дублирующую выборку — DRY, поведение не меняется):

```csharp
    // Unix последнего ВАЛИДНОГО полного (finished_unix, толерантно started_unix —
    // образец IsDue); null — валидного нет. Источник возрастной серии
    // pgworker_backup_full_age_seconds (t14, arch/18 §2.7): метрика не вводит
    // третьего толкования «валидный».
    public static long? LastValidUnix(IReadOnlyList<FullBackupState> fulls)
    {
        var last = fulls
            .Where(IsValid)
            .OrderByDescending(f => f.FinishedUnix ?? f.StartedUnix)
            .FirstOrDefault();
        return last is null ? null : last.FinishedUnix ?? last.StartedUnix;
    }
```

В `IsDue` заменить блок вычисления `lastValid` (строки с `var lastValid = …` по `return nowUnix - finished > fullMaxAgeSec;`) на:

```csharp
        var lastValidUnix = LastValidUnix(fulls);
        if (lastValidUnix is null)
            return true;

        if (lastRestoreFinishedUnix is { } restored && lastValidUnix <= restored)
            return true;

        return nowUnix - lastValidUnix.Value > fullMaxAgeSec;
```

- [ ] **Step 4: Прогон — зелёный (включая все существующие IsDue/BackoffPassed)**

```bash
dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release --filter "FullyQualifiedName~BackupPlannerTests"
```
Expected: PASS (все Fact'ы класса).

- [ ] **Step 5: Commit**

```bash
git add src/PgWorker.Backups/Process/BackupPlanner.cs src/tests/PgWorker.UnitTests/Backups/BackupPlannerTests.cs
git commit -m "feat(t14): BackupPlanner.LastValidUnix — единый источник возраста валидного полного (arch/18 §2.7), DRY в IsDue"
```

---

### Task 3: Instrumentation — counters verify(лейблы)/restore/drill

Миграция `pgworker_backup_verify_total` (лейблы cluster/shard — spec §3.1) и два новых counter'а терминальных исходов. Стейт зеркалится в `DebugSnapshot` для юнит-проверок (паттерн файла).

**Files:**
- Modify: `src/Shared.Metrics/Worker/WorkerMetricsInstrumentation.cs`
- Test: `src/tests/Shared.Metrics.UnitTests/WorkerMetricsInstrumentationTests.cs`
- Test: `src/tests/PgWorker.IntegrationTests/Backups/BackupVerifyProcessTests.cs` (шаг 5 — характеристический тест вызовов `verifyObserver`: критерий приёмки 3 «verify ok/failed/transient»; сам процесс не меняется — observe-точки уже в коде)

**Interfaces:**
- Consumes: существующий `verifyObserver`-делегат `BackupVerifyProcess` (контракт не меняется).
- Produces (wiring Task 5–8, канон-тест Task 11):
  - `public void BackupVerify(string cluster, string shard, string result)` — counter `pgworker_backup_verify_total{cluster,shard,result}` (сигнатура прежняя, лейблы добавляются);
  - `public void BackupRestore(string cluster, string shard, string result)` — counter `pgworker_backup_restore_total{cluster,shard,result}`;
  - `public void BackupDrill(string cluster, string shard, string result)` — counter `pgworker_backup_drill_total{cluster,shard,result}`;
  - `DebugState.BackupVerifyTotals: IReadOnlyDictionary<(string Cluster, string Shard, string Result), long>` (ключ меняется со `string`), `DebugState.BackupRestoreTotals`, `DebugState.BackupDrillTotals` — те же типы.

- [ ] **Step 1: Обновить существующий тест verify + добавить restore/drill (failing)**

В `WorkerMetricsInstrumentationTests.cs` заменить тест `BackupVerify_ИнкрементыПоРезультату` целиком и добавить два новых:

```csharp
    // AAA: counter pgworker_backup_verify_total{cluster,shard,result} — лейблы
    // cluster/shard добавлены (миграция серии t14, arch/18 §2.7): инкременты по тройке
    [Fact]
    public void BackupVerify_ИнкрементыПоКластеруШардуРезультату()
    {
        // Arrange
        using var meter = new Meter("TestWorker");
        using var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);

        // Act — три ok + один failed, два шарда
        sut.BackupVerify("c1", "s1", "ok");
        sut.BackupVerify("c1", "s1", "ok");
        sut.BackupVerify("c1", "s2", "ok");
        sut.BackupVerify("c1", "s2", "failed");

        // Assert — тройка (cluster, shard, result): s2/ok не смешался с s1/ok
        sut.DebugSnapshot().BackupVerifyTotals[("c1", "s1", "ok")].Should().Be(2);
        sut.DebugSnapshot().BackupVerifyTotals[("c1", "s2", "ok")].Should().Be(1);
        sut.DebugSnapshot().BackupVerifyTotals[("c1", "s2", "failed")].Should().Be(1);
    }

    // AAA: counter pgworker_backup_restore_total{cluster,shard,result} — исходы restore
    [Fact]
    public void BackupRestore_ИнкрементыПоИсходам()
    {
        // Arrange
        using var meter = new Meter("TestWorker");
        using var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);

        // Act — ok и failed разных шардов
        sut.BackupRestore("c1", "s1", "ok");
        sut.BackupRestore("c1", "s1", "failed");
        sut.BackupRestore("c2", "s1", "ok");

        // Assert
        var d = sut.DebugSnapshot();
        d.BackupRestoreTotals[("c1", "s1", "ok")].Should().Be(1);
        d.BackupRestoreTotals[("c1", "s1", "failed")].Should().Be(1);
        d.BackupRestoreTotals[("c2", "s1", "ok")].Should().Be(1);
    }

    // AAA: counter pgworker_backup_drill_total{cluster,shard,result} — исходы дрилов
    [Fact]
    public void BackupDrill_ИнкрементыПоИсходам()
    {
        // Arrange
        using var meter = new Meter("TestWorker");
        using var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);

        // Act
        sut.BackupDrill("c1", "s1", "ok");
        sut.BackupDrill("c1", "s1", "ok");
        sut.BackupDrill("c1", "s2", "failed");

        // Assert
        var d = sut.DebugSnapshot();
        d.BackupDrillTotals[("c1", "s1", "ok")].Should().Be(2);
        d.BackupDrillTotals[("c1", "s2", "failed")].Should().Be(1);
    }
```

- [ ] **Step 2: Прогон — отказ (тип ключа BackupVerifyTotals не совпадает / методов нет)**

```bash
dotnet test src/tests/Shared.Metrics.UnitTests/Shared.Metrics.UnitTests.csproj -c Release --filter "FullyQualifiedName~WorkerMetricsInstrumentationTests"
```
Expected: FAIL компиляции (CS1061 `BackupRestore`/`BackupDrill` не найдены; ключ `BackupVerifyTotals` — неверный тип).

- [ ] **Step 3: Реализация в `WorkerMetricsInstrumentation.cs`**

3a. Стейт (заменить `private readonly Dictionary<string, long> _backupVerify = new();`):

```csharp
    private readonly Dictionary<(string Cluster, string Shard, string Result), long> _backupVerify = new();
    private readonly Dictionary<(string Cluster, string Shard, string Result), long> _backupRestore = new();
    private readonly Dictionary<(string Cluster, string Shard, string Result), long> _backupDrill = new();
```

3b. Конструктор: после создания `backupVerify` добавить два counter'а и делегаты; `BackupVerifyMark` теперь принимает три аргумента (лейблы cluster/shard — миграция серии t14):

```csharp
        var backupRestore = meter.CreateCounter<long>(
            "pgworker_backup_restore_total",
            description: "Терминальные исходы restore-заявок (arch/18 §2.7)");
        var backupDrill = meter.CreateCounter<long>(
            "pgworker_backup_drill_total",
            description: "Терминальные исходы дрилов восстановимости (arch/18 §2.7)");
```

```csharp
        BackupVerifyMark = (cluster, shard, result) =>
        {
            try
            {
                backupVerify.Add(1,
                    new KeyValuePair<string, object?>("cluster", cluster),
                    new KeyValuePair<string, object?>("shard", shard),
                    new KeyValuePair<string, object?>("result", result));
            }
            catch
            {
                // Пассивный наблюдатель.
            }
        };
        BackupRestoreMark = (cluster, shard, result) =>
        {
            try
            {
                backupRestore.Add(1,
                    new KeyValuePair<string, object?>("cluster", cluster),
                    new KeyValuePair<string, object?>("shard", shard),
                    new KeyValuePair<string, object?>("result", result));
            }
            catch
            {
                // Пассивный наблюдатель.
            }
        };
        BackupDrillMark = (cluster, shard, result) =>
        {
            try
            {
                backupDrill.Add(1,
                    new KeyValuePair<string, object?>("cluster", cluster),
                    new KeyValuePair<string, object?>("shard", shard),
                    new KeyValuePair<string, object?>("result", result));
            }
            catch
            {
                // Пассивный наблюдатель.
            }
        };
```

3c. Поля делегатов (заменить `private readonly Action<string> BackupVerifyMark;`):

```csharp
    private readonly Action<string, string, string> BackupVerifyMark;
    private readonly Action<string, string, string> BackupRestoreMark;
    private readonly Action<string, string, string> BackupDrillMark;
```

3d. `BackupVerify` — стейт по тройке (вызов метки не меняется); после него добавить два марк-метода:

```csharp
    /// <summary>Counter pgworker_backup_verify_total{cluster,shard,result}:
    /// итог проверки полного (BackupVerifyProcess), result ∈ ok|failed|transient.</summary>
    public void BackupVerify(string cluster, string shard, string result)
    {
        try
        {
            BackupVerifyMark(cluster, shard, result);
            lock (_lock)
            {
                var key = (cluster, shard, result);
                _backupVerify[key] = _backupVerify.TryGetValue(key, out var n) ? n + 1 : 1;
            }
        }
        catch
        {
            // Пассивный наблюдатель.
        }
    }

    /// <summary>Counter pgworker_backup_restore_total{cluster,shard,result}: терминальный
    /// исход restore-заявки (RestoreProcess), result ∈ ok|failed (arch/18 §2.7).</summary>
    public void BackupRestore(string cluster, string shard, string result)
    {
        try
        {
            BackupRestoreMark(cluster, shard, result);
            lock (_lock)
            {
                var key = (cluster, shard, result);
                _backupRestore[key] = _backupRestore.TryGetValue(key, out var n) ? n + 1 : 1;
            }
        }
        catch
        {
            // Пассивный наблюдатель.
        }
    }

    /// <summary>Counter pgworker_backup_drill_total{cluster,shard,result}: чистый терминальный
    /// итог дрилла (RestoreDrillProcess), result ∈ ok|failed (arch/18 §2.7).</summary>
    public void BackupDrill(string cluster, string shard, string result)
    {
        try
        {
            BackupDrillMark(cluster, shard, result);
            lock (_lock)
            {
                var key = (cluster, shard, result);
                _backupDrill[key] = _backupDrill.TryGetValue(key, out var n) ? n + 1 : 1;
            }
        }
        catch
        {
            // Пассивный наблюдатель.
        }
    }
```

3e. `DebugSnapshot()` — заменить аргумент `_backupVerify.ToFrozenDictionary()` и `DebugState`-record:

```csharp
                _backupVerify.ToFrozenDictionary(),
                _backupRestore.ToFrozenDictionary(),
                _backupDrill.ToFrozenDictionary(),
                age);
```

```csharp
    internal sealed record DebugState(
        IReadOnlyDictionary<string, long> LastSuccess,
        IReadOnlyDictionary<(string Loop, bool Ok), long> LoopTicks,
        IReadOnlyDictionary<(string Cluster, string Process), DebugPhase> Phases,
        IReadOnlyDictionary<(string Operation, string Result), long> Operations,
        int ClaimsHeld,
        IReadOnlyDictionary<(string Cluster, string Shard), long> WalLag,
        IReadOnlyDictionary<(string Cluster, string Shard, string Result), long> BackupVerifyTotals,
        IReadOnlyDictionary<(string Cluster, string Shard, string Result), long> BackupRestoreTotals,
        IReadOnlyDictionary<(string Cluster, string Shard, string Result), long> BackupDrillTotals,
        double? SnapshotAgeSeconds);
```

- [ ] **Step 4: Прогон — зелёный**

```bash
dotnet test src/tests/Shared.Metrics.UnitTests/Shared.Metrics.UnitTests.csproj -c Release --filter "FullyQualifiedName~WorkerMetricsInstrumentationTests"
```
Expected: PASS (все Fact'ы класса).

- [ ] **Step 5: Характеристический тест вызовов `verifyObserver` процессом (критерий приёмки 3: verify ok/failed/transient)**

Процесс не меняется — observe-точки уже существуют; тест фиксирует вызовы. В `BackupVerifyProcessTests.cs` расширить `BuildProcess` (прокинуть observer последним аргументом):

```csharp
    private BackupVerifyProcess BuildProcess(
        string cluster, FakeVerifyDriver driver, IBackupS3 s3, BackupsRuntimeOptions? options = null,
        Action<string, string, string>? verifyObserver = null)
        => new(
            Fx.Gateway, [Fx.Endpoint], driver,
            new ShardEndpoints(Fx.Gateway, [Fx.Endpoint], new ShardProbe(new HttpClient())),
            s3, Claims, new WorkJournal("/pgworker", Fx.Gateway, [Fx.Endpoint]),
            options ?? new BackupsRuntimeOptions(
                Enabled: true, S3Endpoint: "http://minio", S3Bucket: "bkt",
                S3AccessKey: "ak", S3SecretKey: "sk", JobImage: "pgworker-backup:test",
                StagingDir: "/backup-staging"),
            TimeProvider.System,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<BackupVerifyProcess>.Instance,
            verifyObserver);
```

Новый Fact (по образцу соседних: одно `OwnEtcd`-окружение на Fact, кейс на исход; все три observe-точки уже в коде — transient `BackupVerifyProcess.cs:171`, failed `:358`/`:415`, ok `:343`):

```csharp
    // AAA: t14 — observe-точки verify: S3 недоступен → transient; битый
    // wal_start → permanent FAILED → failed; exited(0) ok:true → ok
    // (процесс не меняется — тест фиксирует вызовы всех трёх исходов)
    [Fact]
    public async Task Verify_исходы_зовут_наблюдателя_transient_failed_ok()
    {
        // Arrange 1 — COMPLETED PENDING-кандидат; S3-транспорт лежит
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify-obs", ct);
        Fx = fx;
        const string cluster = "vo1";
        await SeedAsync(cluster);
        var s3 = new FakeBackupS3 { Fails = true };
        var outcomes = new List<(string C, string S, string Result)>();
        var process = BuildProcess(cluster, new FakeVerifyDriver(new FakeVerifyEngine()), s3,
            verifyObserver: (c, s, r) => outcomes.Add((c, s, r)));

        // Act 1
        (await process.TickAsync(BuildSnap(cluster), BackupsOf(cluster,
                Completed("20260911120000Z", "000000010000000000000001",
                    new BackupVerify(BackupVerifyStatus.Pending, null))), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 1 — list S3 упал: transient, статус кандидата не тронут
        outcomes.Should().ContainSingle().Which.Should().Be((cluster, "shard1", "transient"));

        // Arrange 2 — wal_start не разбирается: permanent FAILED сразу
        const string cluster2 = "vo2";
        await SeedAsync(cluster2);
        var outcomes2 = new List<(string C, string S, string Result)>();
        var process2 = BuildProcess(cluster2, new FakeVerifyDriver(new FakeVerifyEngine()), new FakeBackupS3(),
            verifyObserver: (c, s, r) => outcomes2.Add((c, s, r)));

        // Act 2
        (await process2.TickAsync(BuildSnap(cluster2), BackupsOf(cluster2,
                Completed("20260911120000Z", "garbage-wal-start",
                    new BackupVerify(BackupVerifyStatus.Pending, null))), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 2 — вердикт permanent: failed после успешной записи FAILED в etcd
        outcomes2.Should().ContainSingle().Which.Should().Be((cluster2, "shard1", "failed"));

        // Arrange 3 — джоб стартует первым тиком (StartJobAsync без наблюдателя:
        // старт-тик исходов не эмитит), контейнер завершается exit 0 + ok:true
        // (образец «Супервиз_Exit0_Ok_чисткаДжоба»)
        const string cluster3 = "vo3";
        await SeedAsync(cluster3);
        var s3ok = new FakeBackupS3();
        var engine = await StartJobAsync(cluster3, s3ok);
        var jobName = $"pgw-backup-verify-{cluster3}-shard1-20260911120000Z";
        engine.Containers[jobName] = engine.Containers[jobName] with
            { State = "exited", ExitCode = 0, Logs = "{\"ok\":true}" };
        var outcomes3 = new List<(string C, string S, string Result)>();
        var process3 = BuildProcess(cluster3, new FakeVerifyDriver(engine), s3ok,
            verifyObserver: (c, s, r) => outcomes3.Add((c, s, r)));

        // Act 3 — тик супервиза итога
        (await process3.TickAsync(BuildSnap(cluster3), BackupsOf(cluster3,
                Completed("20260911120000Z", "000000010000000000000001",
                    new BackupVerify(BackupVerifyStatus.Pending, null))), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 3 — exit 0 + ok:true → observe ok после успешной записи OK в etcd
        outcomes3.Should().ContainSingle().Which.Should().Be((cluster3, "shard1", "ok"));
    }
```

Прогон (seriya поднимет OwnEtcd-контейнеры; после — контроль зачистки):

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~BackupVerifyProcessTests"
```
Expected: PASS (17 существующих + 1 новый).

- [ ] **Step 6: Commit**

```bash
git add src/Shared.Metrics/Worker/WorkerMetricsInstrumentation.cs src/tests/Shared.Metrics.UnitTests/WorkerMetricsInstrumentationTests.cs src/tests/PgWorker.IntegrationTests/Backups/BackupVerifyProcessTests.cs
git commit -m "feat(t14): counters verify (лейблы cluster/shard — миграция серии)/restore/drill + тест вызовов verifyObserver ok/failed/transient (arch/18 §2.7)"
```

---

### Task 4: Instrumentation — gauge-серии full_age/full_max_age/wal_uploaded_age

Три ObservableGauge. Возрастные серии хранят unix-факт, колбэк пересчитывает `now − unix` на каждом scrape (spec §7 M3); `BackupFullAge` замещает набор шардов кластера целиком (ушедший шард серии не эмитит); `BackupWalUploadedAge` — null-удаление per-shard (симметрия `BackupWalLag`).

**Files:**
- Modify: `src/Shared.Metrics/Worker/WorkerMetricsInstrumentation.cs`
- Test: `src/tests/Shared.Metrics.UnitTests/WorkerMetricsInstrumentationTests.cs`

**Interfaces:**
- Consumes: `Measure<T>`-хелпер, `_clock` (есть в классе).
- Produces (wiring Tasks 5–6, канон-тест Task 11):
  - `public void BackupFullAge(string cluster, IReadOnlyDictionary<string, (long? LastValidUnix, long MaxAgeSec)> shards)`;
  - `public void BackupWalUploadedAge(string cluster, string shard, long? ageSec)`;
  - серии `pgworker_backup_full_age_seconds`, `pgworker_backup_full_max_age_seconds`, `pgworker_backup_wal_last_uploaded_age_seconds`;
  - `DebugState.FullAgeFinishedUnix: IReadOnlyDictionary<(string Cluster, string Shard), long>`, `DebugState.FullMaxAge` и `DebugState.WalUploadedUnix` — того же типа.

- [ ] **Step 1: Пишем failing-тесты (в конец `WorkerMetricsInstrumentationTests.cs`)**

```csharp
    // AAA: t14 — набор тика замещает стейт кластера ЦЕЛИКОМ: age null → age-серия
    // отсутствует, max_age пишется всегда, ушедший из набора шард не эмитится
    [Fact]
    public void BackupFullAge_ЗамещениеНабора_nullУбираетAge_maxAgeВсегда()
    {
        // Arrange — тик 1: s1 с валидным, s2 без
        using var meter = new Meter("TestWorker");
        using var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);
        sut.BackupFullAge("c1", new Dictionary<string, (long? LastValidUnix, long MaxAgeSec)>
        {
            ["s1"] = (9_000, 86_400),
            ["s2"] = (null, 86_400),
        });

        // Act — тик 2: s2 получил валидный, s1 ушёл из набора
        sut.BackupFullAge("c1", new Dictionary<string, (long? LastValidUnix, long MaxAgeSec)>
        {
            ["s2"] = (9_500, 43_200),
        });

        // Assert — стейт кластера = последний набор: s1 исчез, s2 обновлён
        var d = sut.DebugSnapshot();
        d.FullAgeFinishedUnix.Keys.Should().ContainSingle(k => k == ("c1", "s2"));
        d.FullAgeFinishedUnix[("c1", "s2")].Should().Be(9_500);
        d.FullMaxAge.Keys.Should().ContainSingle(k => k == ("c1", "s2"));
        d.FullMaxAge[("c1", "s2")].Should().Be(43_200);
    }

    // AAA: t14 — age-серия шарда исчезает при null-факте, max_age остаётся
    [Fact]
    public void BackupFullAge_nullУбираетТолькоAge_шардыРазныхКластеровНезависимы()
    {
        // Arrange — два кластера
        using var meter = new Meter("TestWorker");
        using var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);
        sut.BackupFullAge("c1", new Dictionary<string, (long? LastValidUnix, long MaxAgeSec)> { ["s1"] = (100, 10) });
        sut.BackupFullAge("c2", new Dictionary<string, (long? LastValidUnix, long MaxAgeSec)> { ["s1"] = (200, 20) });

        // Act — c1/s1 потерял валидный
        sut.BackupFullAge("c1", new Dictionary<string, (long? LastValidUnix, long MaxAgeSec)> { ["s1"] = (null, 10) });

        // Assert
        var d = sut.DebugSnapshot();
        d.FullAgeFinishedUnix.Keys.Should().ContainSingle(k => k == ("c2", "s1"));
        d.FullMaxAge.Should().HaveCount(2); // max_age обоих кластеров жив
    }

    // AAA: t14 — uploaded-age хранится как unix-факт (now − age на момент
    // наблюдения — колбэк гейджа пересчитывает на каждом scrape); null удаляет
    [Fact]
    public void BackupWalUploadedAge_unixФакт_nullУдаляет()
    {
        // Arrange — часы на T=10_000
        var clock = new FakeTimeProvider { Now = DateTimeOffset.UnixEpoch.AddSeconds(10_000) };
        using var meter = new Meter("TestWorker");
        using var sut = new WorkerMetricsInstrumentation(meter, clock);

        // Act — наблюдение «сегмент загружен 42 с назад» → unix 9_958; затем снятие
        sut.BackupWalUploadedAge("c1", "s1", 42);
        sut.DebugSnapshot().WalUploadedUnix[("c1", "s1")].Should().Be(9_958);
        sut.BackupWalUploadedAge("c1", "s1", null);

        // Assert — серия исчезла
        sut.DebugSnapshot().WalUploadedUnix.Should().BeEmpty();
    }
```

- [ ] **Step 2: Прогон — отказ компиляции**

```bash
dotnet test src/tests/Shared.Metrics.UnitTests/Shared.Metrics.UnitTests.csproj -c Release --filter "FullyQualifiedName~WorkerMetricsInstrumentationTests"
```
Expected: FAIL (CS1061 — `BackupFullAge`/`BackupWalUploadedAge` не найдены).

- [ ] **Step 3: Реализация**

3a. Стейт (рядом с `_walLag`):

```csharp
    private readonly Dictionary<(string Cluster, string Shard), long> _fullAgeFinishedUnix = new();
    private readonly Dictionary<(string Cluster, string Shard), long> _fullMaxAge = new();
    private readonly Dictionary<(string Cluster, string Shard), long> _walUploadedUnix = new();
```

3b. Конструктор — три ObservableGauge (после гейджа `pgworker_backup_wal_lag_segments`):

```csharp
        // t14 (arch/18 §2.7): возрастные серии хранят unix-факт, колбэк пересчитывает
        // now − unix на КАЖДОМ scrape — между тиками процессов значение не замирает
        // (паттерн worker_snapshot_age_seconds).
        meter.CreateObservableGauge(
            "pgworker_backup_full_age_seconds",
            () => Measure(() => _fullAgeFinishedUnix.Select(kv =>
                new Measurement<double>(
                    Math.Max(0, _clock.GetUtcNow().ToUnixTimeSeconds() - kv.Value),
                    new KeyValuePair<string, object?>("cluster", kv.Key.Cluster),
                    new KeyValuePair<string, object?>("shard", kv.Key.Shard)))),
            unit: "s", description: "Возраст последнего валидного полного бэкапа, с (arch/18 §2.7)");

        meter.CreateObservableGauge(
            "pgworker_backup_full_max_age_seconds",
            () => Measure(() => _fullMaxAge.Select(kv =>
                new Measurement<long>(kv.Value,
                    new KeyValuePair<string, object?>("cluster", kv.Key.Cluster),
                    new KeyValuePair<string, object?>("shard", kv.Key.Shard)))),
            unit: "s", description: "Порог full_max_age_sec эффективной политики кластера (arch/18 §2.7)");

        meter.CreateObservableGauge(
            "pgworker_backup_wal_last_uploaded_age_seconds",
            () => Measure(() => _walUploadedUnix.Select(kv =>
                new Measurement<double>(
                    Math.Max(0, _clock.GetUtcNow().ToUnixTimeSeconds() - kv.Value),
                    new KeyValuePair<string, object?>("cluster", kv.Key.Cluster),
                    new KeyValuePair<string, object?>("shard", kv.Key.Shard)))),
            unit: "s", description: "Возраст последней загрузки WAL-сегмента в S3, с (arch/18 §2.7)");
```

3c. Марк-методы (после `BackupWalLag`):

```csharp
    // Gauge pgworker_backup_full_age_seconds/full_max_age_seconds (t14, arch/18
    // §2.7): ЕДИНЫЙ марк-метод на тик кластера — набор шардов тика замещает
    // стейт кластера ЦЕЛИКОМ (паттерн UpdateCluster §4.2: шард, не пришедший в
    // наборе, серии больше не эмитит). Элемент набора: LastValidUnix — источник
    // возраста (null → age-серия шарда отсутствует), MaxAgeSec — порог (пишется
    // всегда; per-cluster порог алерта age > max_age без хардкода в правиле).
    public void BackupFullAge(
        string cluster, IReadOnlyDictionary<string, (long? LastValidUnix, long MaxAgeSec)> shards)
    {
        try
        {
            lock (_lock)
            {
                foreach (var gone in _fullMaxAge.Keys
                             .Where(k => k.Cluster == cluster && !shards.ContainsKey(k.Shard))
                             .ToList())
                {
                    _fullAgeFinishedUnix.Remove(gone);
                    _fullMaxAge.Remove(gone);
                }

                foreach (var (shard, value) in shards)
                {
                    _fullMaxAge[(cluster, shard)] = value.MaxAgeSec;
                    if (value.LastValidUnix is { } unix)
                        _fullAgeFinishedUnix[(cluster, shard)] = unix;
                    else
                        _fullAgeFinishedUnix.Remove((cluster, shard));
                }
            }
        }
        catch
        {
            // Пассивный наблюдатель.
        }
    }

    // Gauge pgworker_backup_wal_last_uploaded_age_seconds (t14, arch/18 §2.7):
    // возраст последней доставки закрытого сегмента — наблюдение контрольного
    // прохода WalStreamProcess; null — серия исчезает (симметрия BackupWalLag).
    // Хранится unix-факт (now − age наблюдения), колбэк гейджа пересчитывает
    // возраст на каждом scrape.
    public void BackupWalUploadedAge(string cluster, string shard, long? ageSec)
    {
        try
        {
            lock (_lock)
            {
                if (ageSec is { } age)
                    _walUploadedUnix[(cluster, shard)] = _clock.GetUtcNow().ToUnixTimeSeconds() - age;
                else
                    _walUploadedUnix.Remove((cluster, shard));
            }
        }
        catch
        {
            // Пассивный наблюдатель.
        }
    }
```

3d. `DebugSnapshot()` — добавить три аргумента перед `age` (и в конец `DebugState`-record после `BackupDrillTotals`):

```csharp
                _fullAgeFinishedUnix.ToFrozenDictionary(),
                _fullMaxAge.ToFrozenDictionary(),
                _walUploadedUnix.ToFrozenDictionary(),
                age);
```

```csharp
        IReadOnlyDictionary<(string Cluster, string Shard), long> FullAgeFinishedUnix,
        IReadOnlyDictionary<(string Cluster, string Shard), long> FullMaxAge,
        IReadOnlyDictionary<(string Cluster, string Shard), long> WalUploadedUnix,
        double? SnapshotAgeSeconds);
```

- [ ] **Step 4: Прогон — зелёный**

```bash
dotnet test src/tests/Shared.Metrics.UnitTests/Shared.Metrics.UnitTests.csproj -c Release --filter "FullyQualifiedName~WorkerMetricsInstrumentationTests"
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/Shared.Metrics/Worker/WorkerMetricsInstrumentation.cs src/tests/Shared.Metrics.UnitTests/WorkerMetricsInstrumentationTests.cs
git commit -m "feat(t14): gauge-серии full_age/full_max_age (замещение набора тика) и wal_last_uploaded_age (unix-факт, пересчёт на scrape) — arch/18 §2.7"
```

---

### Task 5: BackupProcess — `fullAgeObserver` + wiring

Тик планировщика отдаёт наблюдателю полный набор бэкапимых шардов (`Dsn is not null && !s.ToRemove`) с `BackupPlanner.LastValidUnix` и эффективным `fullMaxAgeSec`. Ранние Failed-выходы (клэйм/G0/creds/portalloc) делегат не зовут — стейт живёт прошлым тиком (риск M2 принят: свежесть контролируют ServiceDown/WorkerLoopStalled).

**Files:**
- Modify: `src/PgWorker.Backups/Process/BackupProcess.cs` (ctor + конец `TickAsync`)
- Modify: `src/PgWorker.App/Program.cs` (регистрация BackupProcess, ~строка 504)
- Test: `src/tests/PgWorker.UnitTests/Backups/BackupProcessTests.cs`

**Interfaces:**
- Consumes: `BackupPlanner.LastValidUnix` (Task 2); `WorkerMetricsInstrumentation.BackupFullAge` (Task 4).
- Produces: ctor-параметр `Action<string, IReadOnlyDictionary<string, (long? LastValidUnix, long MaxAgeSec)>>? fullAgeObserver = null` (после `snapshot`); Program.cs wiring.

- [ ] **Step 1: Пишем failing-тест**

В `BackupProcessTests.cs`: расширить `NewRig` параметром и добавить Fact. Правка `NewRig` (сигнатура и конструктор процесса):

```csharp
    private static async Task<Rig> NewRig(
        bool claim = true, bool seedPortalloc = true, BackupsRuntimeOptions? options = null,
        Fakes.FakeEtcd? etcdOverride = null,
        Action<string, IReadOnlyDictionary<string, (long? LastValidUnix, long MaxAgeSec)>>? observer = null)
```

```csharp
        var process = new BackupProcess(
            store, [Ep], driver,
            new ShardEndpoints(store, [Ep], probe), sql, ensurer,
            claims, journal, Secrets, options ?? new BackupsRuntimeOptions { Enabled = true },
            TimeProvider.System, NullLogger<BackupProcess>.Instance, snapshot: null,
            fullAgeObserver: observer);
```

Новый тест (после `Disabled_Noop`):

```csharp
    // AAA: t14 — тик передаёт наблюдателю набор бэкапимых шардов: валидный
    // COMPLETED → LastValidUnix его finished; verify-FAILED → null; max_age —
    // эффективная политика тика (дефолт конфига при отсутствии policy-ключа)
    [Fact]
    public async Task Tick_ПередаётНаблюдателюВозрастПолных()
    {
        // Arrange — шард с валидным COMPLETED (finished=T); дефолт FullMaxAgeSec=86400
        var seen = new List<(string Cluster,
            IReadOnlyDictionary<string, (long? LastValidUnix, long MaxAgeSec)> Shards)>();
        var rig = await NewRig(observer: (c, shards) => seen.Add((c, shards)));
        const long finished = 1_757_500_300;
        var valid = new FullBackupState("20260910120000Z", FullBackupStatus.Completed, "shard1a",
            BackupSourceRole.Replica, 1_757_500_000, finished, "000000010000000000000001", 1024, null, null);
        var broken = valid with
        {
            Id = "20260911090000Z", StartedUnix = 1_757_586_000, FinishedUnix = 1_757_586_300,
            Verify = new BackupVerify(BackupVerifyStatus.Failed, 1_757_586_300, "bad"),
        };

        // Act — тик с валидным, затем тик, где валидного нет (новейший COMPLETED verify-FAILED)
        (await rig.Process.TickAsync(await Snapshot(rig.Etcd), BackupsOf(valid), CancellationToken.None))
            .IsSuccess.Should().BeTrue();
        (await rig.Process.TickAsync(await Snapshot(rig.Etcd), BackupsOf(broken), CancellationToken.None))
            .IsSuccess.Should().BeTrue();

        // Assert — один вызов на тик; замещение семантики набора проверяет
        // instrumentation (WorkerMetricsInstrumentationTests), здесь — содержимое
        seen.Should().HaveCount(2);
        seen[0].Cluster.Should().Be("shop");
        seen[0].Shards.Should().ContainKey("shard1")
            .WhoseValue.LastValidUnix.Should().Be(finished);
        seen[0].Shards["shard1"].MaxAgeSec.Should().Be(86_400);
        seen[1].Shards["shard1"].LastValidUnix.Should().BeNull("verify FAILED — не валиден");
        seen[1].Shards["shard1"].MaxAgeSec.Should().Be(86_400, "порог пишется всегда");
    }
```

- [ ] **Step 2: Прогон — отказ компиляции**

```bash
dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release --filter "FullyQualifiedName~BackupProcessTests"
```
Expected: FAIL (CS1739/CS1061 — именованного параметра `fullAgeObserver` нет).

- [ ] **Step 3: Реализация в `BackupProcess.cs`**

3a. Ctor — последний параметр:

```csharp
    Func<CancellationToken, Task<Result>>? snapshot = null,
    Action<string, IReadOnlyDictionary<string, (long? LastValidUnix, long MaxAgeSec)>>? fullAgeObserver = null)
```

3b. Конец `TickAsync` — перед `_ = snapshot;` вставить:

```csharp
        // t14 (arch/18 §2.7): возрастные серии полных — наблюдение тика
        // планировщика: набор бэкапимых шардов (dsn, не ToRemove) замещает стейт
        // кластера в instrumentation целиком; возрастной факт — семантика
        // планировщика (BackupPlanner.LastValidUnix), порог — эффективная
        // политика тика (policy-ключ ?? дефолт конфига).
        fullAgeObserver?.Invoke(cluster, snap.Shards
            .Where(s => s.Dsn is not null && !s.ToRemove)
            .ToDictionary(
                s => s.Name,
                s => (BackupPlanner.LastValidUnix(mine.Shards.GetValueOrDefault(s.Name)?.Full ?? []),
                    fullMaxAgeSec)));
```

- [ ] **Step 4: Прогон — зелёный**

```bash
dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release --filter "FullyQualifiedName~BackupProcessTests"
```
Expected: PASS (новый + все существующие).

- [ ] **Step 5: Wiring в `Program.cs`**

В регистрации `BackupProcess` (блок после `SnapshotDelegate(...)` — последним аргументом добавить):

```csharp
    SnapshotDelegate(sp.GetRequiredService<SnapshotJob>()),
    fullAgeObserver: sp.GetRequiredService<Shared.Metrics.Worker.WorkerMetricsInstrumentation>().BackupFullAge));
```

(было: `SnapshotDelegate(...)` закрывал список аргументов — теперь он предпоследний).

- [ ] **Step 6: Сборка решения**

```bash
dotnet build src/PgWorker.slnx -c Release
```
Expected: 0 errors, 0 warnings.

- [ ] **Step 7: Commit**

```bash
git add src/PgWorker.Backups/Process/BackupProcess.cs src/PgWorker.App/Program.cs src/tests/PgWorker.UnitTests/Backups/BackupProcessTests.cs
git commit -m "feat(t14): BackupProcess — observer возраста полных (набор шардов тика, LastValidUnix планировщика) + wiring (arch/18 §2.7)"
```

---

### Task 6: WalStreamProcess — `uploadedAgeObserver` + null-семантика

Оба наблюдателя (`lagObserver` уже есть, `uploadedAgeObserver` новый) живут в контрольном проходе и вызываются из одних точек с одинаковой null-семантикой (spec §3.2): факт — значения; BROKEN/STOPPED/нет наблюдения — null (серии исчезают). Расписание «не время» — серии не трогаем.

**Files:**
- Modify: `src/PgWorker.Backups/WalStreamProcess.cs` (ctor, `ControlDueAsync`, `BreakAsync`, `StopShardAsync`, `StopAllAsync`)
- Modify: `src/PgWorker.App/Program.cs` (регистрация WalStreamProcess, ~строка 550)
- Test: `src/tests/PgWorker.IntegrationTests/Backups/WalStreamProcessTests.cs`

**Interfaces:**
- Consumes: `WorkerMetricsInstrumentation.BackupWalUploadedAge` (Task 4), `WorkerMetricsInstrumentation.BackupWalLag` (существует).
- Produces: ctor-параметр `Action<string, string, long?>? uploadedAgeObserver = null` (после `logger`); Program.cs wiring.

- [ ] **Step 1: Пишем failing-тесты**

В `WalStreamProcessTests.cs` расширить `BuildProcess` (два optional-параметра в конец) и прокинуть в конструктор:

```csharp
    private WalStreamProcess BuildProcess(
        BackupsRuntimeOptions? options,
        FakeWalSqlExecutor sql,
        FakeBackupS3 s3,
        StubScaleDriver driver,
        TimeProvider? clock = null,
        Action<string, string, long?>? uploadedAgeObserver = null,
        Action<string, string, long?>? lagObserver = null)
        => new(
            fixture.Gateway, [fixture.Endpoint], driver,
            new ShardEndpoints(fixture.Gateway, [fixture.Endpoint], new ShardProbe(new HttpClient())),
            sql, s3,
            new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]),
            _claims, new WorkJournal("/pgworker", fixture.Gateway, [fixture.Endpoint]),
            () => options,
            new InstallSecrets("su-pw", "sb-pw", "adm-pw", "mov-pw"),
            clock ?? TimeProvider.System,
            lagObserver,
            null,
            uploadedAgeObserver);
```

Два новых Fact'а (по образцу `Контроль_сплошная_цепочка_пишет_ACTIVE_и_chain_start_от_полного`; `SeedSegments`/`FullShard`/`ReadWal` — существующие хелперы класса):

```csharp
    // AAA: t14 — контрольный проход отдаёт ОБЕИМ наблюдателям факт одного
    // прохода: lag (сегменты) и uploaded-age (now − last_uploaded_unix ≥ 0)
    [Fact]
    public async Task Контроль_наблюдатели_получают_лаг_и_возраст_загрузки()
    {
        // Arrange — полный COMPLETED wal_start=..01; S3: сегменты 1..3; мастер 0/3000000
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cw1");
        (await _claims.TryClaimClusterAsync("cw1", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor { Current = ("0/3000000", 1) };
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cw1", 1, 3);
        var lags = new List<(string C, string S, long? Lag)>();
        var ages = new List<(string C, string S, long? Age)>();
        var process = BuildProcess(Options(), sql, s3, new StubScaleDriver(),
            uploadedAgeObserver: (c, s, a) => ages.Add((c, s, a)),
            lagObserver: (c, s, l) => lags.Add((c, s, l)));
        var backups = new ClusterBackups("cw1", null,
            new Dictionary<string, ShardBackups> { ["shard1"] = FullShard("000000010000000000000001") });

        // Act — контроль due (VerifyIntervalSec=0)
        (await process.TickAsync(BuildSnap("cw1"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — по одному вызову на наблюдателя: lag посчитан, возраст ≥ 0
        lags.Should().ContainSingle(t => t.C == "cw1" && t.S == "shard1" && t.Lag != null);
        ages.Should().ContainSingle(t => t.C == "cw1" && t.S == "shard1")
            .Which.Age.Should().BeGreaterThanOrEqualTo(0);
    }

    // AAA: t14 — BROKEN (дыра цепочки) снимает обе серии: наблюдатели получают null
    [Fact]
    public async Task Контроль_дыра_BROKEN_снимаетСерииОбоихНаблюдателей()
    {
        // Arrange — wal_start=..01; S3: 1 и 3 (дыра на ..02)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cw2");
        (await _claims.TryClaimClusterAsync("cw2", ct)).Value.Should().BeTrue();
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cw2", 1, 1);
        s3.Objects.Add(("cw2", "shard1", "000000010000000000000003"));
        var lags = new List<(string C, string S, long? Lag)>();
        var ages = new List<(string C, string S, long? Age)>();
        var process = BuildProcess(Options(), new FakeWalSqlExecutor(), s3, new StubScaleDriver(),
            uploadedAgeObserver: (c, s, a) => ages.Add((c, s, a)),
            lagObserver: (c, s, l) => lags.Add((c, s, l)));
        var backups = new ClusterBackups("cw2", null,
            new Dictionary<string, ShardBackups> { ["shard1"] = FullShard("000000010000000000000001") });

        // Act
        (await process.TickAsync(BuildSnap("cw2"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — BROKEN: ровно один вызов на наблюдателя, оба null (серии исчезают)
        (await ReadWal("cw2")).State.Should().Be(WalStreamStatus.Broken);
        lags.Should().ContainSingle(t => t.C == "cw2" && t.Lag is null);
        ages.Should().ContainSingle(t => t.C == "cw2" && t.Age is null);
    }
```

- [ ] **Step 2: Прогон — отказ компиляции**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~WalStreamProcessTests"
```
Expected: FAIL компиляции (параметр `uploadedAgeObserver` не существует). Серия поднимет docker-etcd фикстуру — после прогона убедиться в зачистке (`docker ps --filter name=pgw-`).

- [ ] **Step 3: Реализация в `WalStreamProcess.cs`**

3a. Ctor — два последних параметра:

```csharp
    Action<string, string, long?>? lagObserver = null,
    ILogger? logger = null,
    Action<string, string, long?>? uploadedAgeObserver = null) // t14: uploaded-age (arch/18 §2.7)
```

3b. Хелпер (рядом с `ControlDueAsync`):

```csharp
    // Наблюдатели контрольного прохода (t14, arch/18 §2.7): лаг и uploaded-age —
    // один факт одного прохода, одинаковая null-семантика (BROKEN/STOPPED/факта
    // нет — серии исчезают; null идемпотентен).
    private void Observe(string cluster, string shard, long? lagSegments, long? uploadedAgeSec)
    {
        lagObserver?.Invoke(cluster, shard, lagSegments);
        uploadedAgeObserver?.Invoke(cluster, shard, uploadedAgeSec);
    }
```

3c. `ControlDueAsync` — четыре правки:
- ветка `chainStart is not { } start` (перед `return new ControlOutcome(wal);`):

```csharp
        if (chainStart is not { } start)
        {
            Observe(cluster, shard, null, null); // t14: факта нет — серии исчезают
            return new ControlOutcome(wal);
        }
```

- ветка `lastUploadedName/lastUploadedUnix` null (перед `return new ControlOutcome(wal);`):

```csharp
        if (lastUploadedName is null || lastUploadedUnix is null)
        {
            Observe(cluster, shard, null, null); // t14: наблюдения нет — серии исчезают
            return new ControlOutcome(wal);
        }
```

- существующий вызов `lagObserver?.Invoke(cluster, shard, lag);` заменить на:

```csharp
        Observe(cluster, shard, lag, now - lastUploadedUnix.Value); // t14: факт прохода
```

- ветка дыры `!chain.IsContinuous` — вызов `BreakAsync` уже внутри покрывает снятие (3d).

3d. `BreakAsync` — перед `return broken;`:

```csharp
        Observe(cluster, shard, null, null); // t14: BROKEN — серии исчезают
```

3e. `StopShardAsync` — последней строкой метода:

```csharp
        Observe(cluster, shard, null, null); // t14: STOPPED — серии исчезают
```

3f. `StopAllAsync` — в цикле `foreach (var shard in snap.Shards)` последней инструкцией тела:

```csharp
            Observe(cluster, shard.Name, null, null); // t14: STOPPED — серии исчезают
```

- [ ] **Step 4: Прогон — зелёный (весь класс, включая существующие STOPPED/BROKEN-факты)**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~WalStreamProcessTests"
```
Expected: PASS (30 существующих + 2 новых).

- [ ] **Step 5: Wiring в `Program.cs`** — в регистрации `WalStreamProcess` после аргумента logger добавить:

```csharp
        sp.GetRequiredService<ILoggerFactory>().CreateLogger("WalStreamProcess"),
        sp.GetRequiredService<Shared.Metrics.Worker.WorkerMetricsInstrumentation>().BackupWalUploadedAge);
```

(и в существующей строке `sp.GetRequiredService<WorkerMetricsInstrumentation>().BackupWalLag` порядок аргументов уже соответствует: lagObserver, logger, uploadedAgeObserver).

- [ ] **Step 6: Сборка**

```bash
dotnet build src/PgWorker.slnx -c Release
```
Expected: 0 errors, 0 warnings.

- [ ] **Step 7: Commit**

```bash
git add src/PgWorker.Backups/WalStreamProcess.cs src/PgWorker.App/Program.cs src/tests/PgWorker.IntegrationTests/Backups/WalStreamProcessTests.cs
git commit -m "feat(t14): WalStreamProcess — uploadedAgeObserver рядом с lagObserver, null-семантика BROKEN/STOPPED/нет факта + wiring (arch/18 §2.7)"
```

---

### Task 7: RestoreProcess — `restoreObserver` (COMPLETED/FAILED)

Вызов строго в точке записи терминального etcd-статуса (spec §3.2): FAILED — `FailPermanentAsync` (после успешного put), COMPLETED — `RejoinAsync` (после успешного `putDone`). Put не прошёл → статус не сменился → исход НЕ зафиксирован (наблюдатель молчит — тик повторит).

**Files:**
- Modify: `src/PgWorker.Backups/Process/RestoreProcess.cs` (ctor, `FailPermanentAsync`, `RejoinAsync`)
- Modify: `src/PgWorker.App/Program.cs` (регистрация RestoreProcess, ~строка 522)
- Test: `src/tests/PgWorker.IntegrationTests/Backups/RestoreProcessTests.cs`

**Interfaces:**
- Consumes: `WorkerMetricsInstrumentation.BackupRestore` (Task 3).
- Produces: ctor-параметр `Action<string, string, string>? restoreObserver = null` (после `logger`); Program.cs wiring.

- [ ] **Step 1: Пишем failing-тесты**

`BuildProcess` в `RestoreProcessTests.cs` расширить параметром (в конец) и прокинуть последним аргументом конструктора:

```csharp
    private RestoreProcess BuildProcess(FakeBackupS3 s3, IClusterDriver driver,
        TimeProvider? clock = null, HttpMessageHandler? patroni = null,
        BackupsRuntimeOptions? options = null, ISqlExecutor? db = null,
        Action<string, string, string>? restoreObserver = null)
```

Новый Fact — ОБА терминальных исхода (кейс 1 — по образцу `Валидация_полных_нет_в_S3_permanent_FAILED`; кейс 2 — по образцу успешного rejoin-факта `Rejoin_лидер_избран_реплика_стартовала_basebackup_COMPLETED_wal_удалён`; сид-хелперы класса: `SeedAsync`/`SeedTwoNodeAllocAsync`/`SeedRestoreAsync`/`BackupsFromEtcdAsync`/`BuildTwoNodeSnap`/`PatroniHandler`/`TestDriver`):

```csharp
    // AAA: t14 — оба терминальных исхода restore: permanent-FAILED ("failed")
    // в точке записи статуса и COMPLETED ("ok") после успешного putDone
    // (симметрия drill-теста; обе точки вставки 3b/3c)
    [Fact]
    public async Task Терминальные_исходы_FAILED_и_COMPLETED_зовут_наблюдателя()
    {
        // Arrange 1 — заявка PLANNED, полных нет ни в etcd (ShardBackups.Full пуст),
        // ни в S3 (FakeBackupS3.Fulls пуст) → DR-резолв «полные не найдены»
        // → permanent-FAILED (FailPermanentAsync)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("rc1");
        (await _claims.TryClaimClusterAsync("rc1", ct)).Value.Should().BeTrue();
        var s3 = new FakeBackupS3();
        var driver = new StubScaleDriver();
        var outcomes = new List<(string C, string S, string Result)>();
        var process = BuildProcess(s3, driver, restoreObserver: (c, s, r) => outcomes.Add((c, s, r)));
        var op = new RestoreOperationState("20260910120000Z", RestoreStatus.Planned,
            "", "rc1/shard1", "latest", "shard1a", 1_757_500_000, "admin");

        // Act 1
        (await process.TickAsync(BuildSnap("rc1"),
            [new ClusterBackups("rc1", null,
                new Dictionary<string, ShardBackups> { ["shard1"] = new([], null, [op]) })], ct))
            .IsSuccess.Should().BeTrue();

        // Assert 1 — терминальный FAILED зафиксирован наблюдателем
        outcomes.Should().ContainSingle().Which.Should().Be(("rc1", "shard1", "failed"));

        // Arrange 2 — успешный rejoin одним тиком до COMPLETED: REJOINING-заявка,
        // Patroni Ready (лидер running + реплика creating replica), two-node
        // portalloc/снапшот (wal-ключ не сидируется: его удаление идемпотентно,
        // для наблюдателя не нужно)
        await SeedAsync("c1");
        await SeedTwoNodeAllocAsync();
        var inner = new StubScaleDriver();
        var driver2 = new TestDriver(inner, new FakeBackupEngine());
        var outcomes2 = new List<(string C, string S, string Result)>();
        var process2 = BuildProcess(new FakeBackupS3(), driver2,
            patroni: new PatroniHandler { Ready = true },
            restoreObserver: (c, s, r) => outcomes2.Add((c, s, r)));
        await SeedRestoreAsync("c1", "shard1", "20260911122009Z",
            backupId: "20260910120000Z", state: RestoreStatus.Rejoining);
        (await _claims.TryClaimClusterAsync("c1", ct)).Value.Should().BeTrue();

        // Act 2
        (await process2.TickAsync(BuildTwoNodeSnap(), await BackupsFromEtcdAsync("c1"), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 2 — COMPLETED зафиксирован наблюдателем ("ok" после putDone)
        outcomes2.Should().ContainSingle().Which.Should().Be(("c1", "shard1", "ok"));
    }
```

- [ ] **Step 2: Прогон — отказ компиляции**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~RestoreProcessTests"
```
Expected: FAIL компиляции (параметр `restoreObserver` не существует).

- [ ] **Step 3: Реализация в `RestoreProcess.cs`**

3a. Ctor — последний параметр:

```csharp
    ILogger<RestoreProcess>? logger = null,
    Action<string, string, string>? restoreObserver = null) // (cluster, shard, ok|failed) — t14, arch/18 §2.7
```

3b. `FailPermanentAsync` — после записи статуса (существующий блок `var put = await PutStatusAsync(...)`):

```csharp
        var put = await PutStatusAsync(cluster, shard, op with
        {
            State = RestoreStatus.Failed,
            FinishedUnix = NowUnix(),
            Error = error,
        }, ct);
        if (!put.IsSuccess)
            logger?.LogError("backup-restore {Cluster}/{Shard}/{Id}: статус FAILED не записан — {Error}",
                cluster, shard, op.Id, put.Error?.Message);
        else
            restoreObserver?.Invoke(cluster, shard, "failed"); // t14: терминальный исход зафиксирован в etcd
```

3c. `RejoinAsync` — после успешного `putDone` (существующий блок), перед `delWal`:

```csharp
        var putDone = await PutStatusAsync(cluster, shard.Name, completed, ct);
        if (!putDone.IsSuccess)
            return Result<ProcessOutcome>.Failed(putDone.Error!);
        restoreObserver?.Invoke(cluster, shard.Name, "ok"); // t14: COMPLETED зафиксирован в etcd
```

- [ ] **Step 4: Прогон — зелёный**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~RestoreProcessTests"
```
Expected: PASS (31 существующий + 1 новый).

- [ ] **Step 5: Wiring в `Program.cs`** — в регистрации `RestoreProcess` последним аргументом добавить:

```csharp
    sp.GetRequiredService<ILoggerFactory>().CreateLogger<PgWorker.Backups.Process.RestoreProcess>(),
    sp.GetRequiredService<Shared.Metrics.Worker.WorkerMetricsInstrumentation>().BackupRestore));
```

- [ ] **Step 6: Сборка**

```bash
dotnet build src/PgWorker.slnx -c Release
```
Expected: 0 errors, 0 warnings.

- [ ] **Step 7: Commit**

```bash
git add src/PgWorker.Backups/Process/RestoreProcess.cs src/PgWorker.App/Program.cs src/tests/PgWorker.IntegrationTests/Backups/RestoreProcessTests.cs
git commit -m "feat(t14): RestoreProcess — restoreObserver терминальных исходов COMPLETED/FAILED + wiring (arch/18 §2.7)"
```

---

### Task 8: RestoreDrillProcess — `drillObserver` (чистый итог + FAILED-валидация)

Две точки чистого терминального итога (spec §3.2): снятие `phase=cleaning` в `FinishCleanupAsync` (единственная точка после подтверждённого сноса контура — ровно один вызов на итог благодаря идемпотентной доводке: `activities`-фильтр больше не выбирает завершённый ключ) и `FailValidationAsync` (FAILED без запуска джоба — итог пишется сразу).

**Files:**
- Modify: `src/PgWorker.Backups/Process/RestoreDrillProcess.cs` (ctor, `FinishCleanupAsync`, `FailValidationAsync`)
- Modify: `src/PgWorker.App/Program.cs` (регистрация RestoreDrillProcess, ~строка 605)
- Test: `src/tests/PgWorker.IntegrationTests/Backups/RestoreDrillProcessTests.cs`

**Interfaces:**
- Consumes: `WorkerMetricsInstrumentation.BackupDrill` (Task 3).
- Produces: ctor-параметр `Action<string, string, string>? drillObserver = null` (после `logger`); Program.cs wiring.

- [ ] **Step 1: Пишем failing-тесты**

`BuildProcess` в `RestoreDrillProcessTests.cs` расширить (в конец) и прокинуть:

```csharp
    private RestoreDrillProcess BuildProcess(
        string cluster, FakeDrillDriver driver, IBackupS3 s3, BackupsRuntimeOptions? options = null,
        Action<string, string, string>? drillObserver = null)
        => new(
            Fx.Gateway, [Fx.Endpoint], driver,
            new ShardEndpoints(Fx.Gateway, [Fx.Endpoint], new ShardProbe(new HttpClient())),
            s3, Claims, new WorkJournal("/pgworker", Fx.Gateway, [Fx.Endpoint]),
            options ?? new BackupsRuntimeOptions(
                Enabled: true, S3Endpoint: "http://minio", S3Bucket: "bkt",
                S3AccessKey: "ak", S3SecretKey: "sk", JobImage: "pgworker-backup:test"),
            TimeProvider.System,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<RestoreDrillProcess>.Instance,
            drillObserver);
```

Новый Fact (по образцу `Tick_ManifestMissing_FailsWithoutJob`; хелперы `SeedAsync`/`SeedFullAsync`/`SnapshotBackupsAsync`/`ReadDrillAsync` класса):

```csharp
    // AAA: t14 — оба пути чистого терминального итога зовут наблюдателя:
    // FAILED-валидация без джоба ("failed") и доведённый SUCCEEDED ("ok")
    [Fact]
    public async Task ТерминальныеИсходы_зовут_наблюдателя()
    {
        // Arrange 1 — полный есть, манифеста нет: FAILED-валидация без джоба
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill-obs", ct);
        Fx = fx;
        const string cluster = "dro1";
        await SeedAsync(cluster);
        await SeedFullAsync(cluster);
        var s3 = new FakeBackupS3();
        s3.Objects.Add((cluster, Shard, "000000010000000000000001"));
        s3.Objects.Add((cluster, Shard, "000000010000000000000002"));
        s3.Objects.Add((cluster, Shard, "000000010000000000000003"));
        var outcomes = new List<(string C, string S, string Result)>();
        var process = BuildProcess(cluster, new FakeDrillDriver(new FakeDrillEngine()), s3,
            drillObserver: (c, s, r) => outcomes.Add((c, s, r)));

        // Act 1 — тик валидации
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 1 — чистый итог без джоба
        outcomes.Should().ContainSingle().Which.Should().Be((cluster, Shard, "failed"));

        // Arrange 2 — RUNNING-джоб exited(0) ok → чистый итог SUCCEEDED
        const string cluster2 = "dro2";
        await SeedAsync(cluster2);
        var engine = new FakeDrillEngine();
        var driver = new FakeDrillDriver(engine);
        var name = BackupNames.DrillContainerName(cluster2, Shard, DrillId);
        SeedContainer(engine, name, "exited", 0,
            "{\"phase\":\"recovering\"}\n{\"ok\":true,\"restored_to_lsn\":\"0/42\"}");
        await SeedDrillAsync(cluster2,
            new DrillState(DrillId, DrillStatus.Running, "20261001090000Z", NowUnix()));
        outcomes.Clear();
        var process2 = BuildProcess(cluster2, driver, new FakeBackupS3(),
            drillObserver: (c, s, r) => outcomes.Add((c, s, r)));

        // Act 2 — вердикт + доводка сноса одним тиком
        (await process2.TickAsync(BuildSnap(cluster2), await SnapshotBackupsAsync(cluster2), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 2 — ровно один вызов: ok после снятия cleaning
        outcomes.Should().ContainSingle().Which.Should().Be((cluster2, Shard, "ok"));
    }
```

(`SeedAsync`/`BuildSnap`/`SeedFullAsync`/`SeedDrillAsync`/`SnapshotBackupsAsync`/`SeedContainer` и константы `Shard`/`DrillId` — существующие хелперы класса; OwnEtcd-окружение переиспользуется обоими кейсами Fact'а.)

- [ ] **Step 2: Прогон — отказ компиляции**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~RestoreDrillProcessTests"
```
Expected: FAIL компиляции (параметр `drillObserver` не существует).

- [ ] **Step 3: Реализация в `RestoreDrillProcess.cs`**

3a. Ctor — последний параметр:

```csharp
    ILogger<RestoreDrillProcess> logger,
    Action<string, string, string>? drillObserver = null) // (cluster, shard, ok|failed) — t14, arch/18 §2.7
```

3b. `FinishCleanupAsync` — после успешного put чистого итога:

```csharp
        var clean = drill with { Phase = null };
        var put = await PutDrillAsync(cluster, shard, clean, ct);
        if (!put.IsSuccess)
            return; // cleaning-фаза осталась — следующий тик перепишет (идемпотентно)
        drillObserver?.Invoke(cluster, shard,
            drill.State == DrillStatus.Succeeded ? "ok" : "failed"); // t14: чистый терминальный итог
```

3c. `FailValidationAsync` — после успешного put:

```csharp
        var put = await PutDrillAsync(cluster, shard, failed, ct);
        if (!put.IsSuccess)
            return Result<ProcessOutcome>.Failed(put.Error!);
        drillObserver?.Invoke(cluster, shard, "failed"); // t14: FAILED-валидация — итог без джоба
```

- [ ] **Step 4: Прогон — зелёный**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~RestoreDrillProcessTests"
```
Expected: PASS (21 существующий + 1 новый).

- [ ] **Step 5: Wiring в `Program.cs`** — в регистрации `RestoreDrillProcess` последним аргументом добавить:

```csharp
    sp.GetRequiredService<ILoggerFactory>().CreateLogger<PgWorker.Backups.Process.RestoreDrillProcess>(),
    sp.GetRequiredService<Shared.Metrics.Worker.WorkerMetricsInstrumentation>().BackupDrill));
```

- [ ] **Step 6: Сборка**

```bash
dotnet build src/PgWorker.slnx -c Release
```
Expected: 0 errors, 0 warnings.

- [ ] **Step 7: Commit**

```bash
git add src/PgWorker.Backups/Process/RestoreDrillProcess.cs src/PgWorker.App/Program.cs src/tests/PgWorker.IntegrationTests/Backups/RestoreDrillProcessTests.cs
git commit -m "feat(t14): RestoreDrillProcess — drillObserver чистого терминального итога (снятие cleaning) и FAILED-валидации + wiring (arch/18 §2.7)"
```

---

### Task 9: Prometheus-группа `backups` + счётчики чека 65

7 алертов spec §3.3 (severity — зеркало панельных правил), счётчик чека `>= 18` (11+7), дашборды `>= 5`. Пороги WAL — статические дефолты конфига (1024/300) с комментарием в правиле; у полных — честный per-cluster порог из `max_age`-серии; счётчики — `increase[15m]` (scrape 15 с).

**Files:**
- Modify: `dev-stand/adminpanel/metrics/prometheus/rules.yml` (новая группа в конец)
- Modify: `dev-stand/adminpanel/checks/65-metrics.sh` (шаг 4: счётчик рулов; шаг 5: счётчик дашбордов)

**Interfaces:**
- Consumes: серии Task 3/4 (канон имён — spec §3.1).
- Produces: группа `backups` — вход дашборда Task 10 и канон-теста; обновлённый чек 65 — критерий приёмки 4/5.

- [ ] **Step 1: Добавить группу `backups` в конец `rules.yml`**

```yaml
  # t14 (arch/18 §2.7): бэкап-домен — зеркало подмножества панельных правил
  # (независимый от панели канал; etcd-источник истины статусов остаётся).
  - name: backups
    rules:
      - alert: BackupFullStale
        expr: pgworker_backup_full_age_seconds > pgworker_backup_full_max_age_seconds
        for: 0m
        labels: {severity: critical}
        annotations:
          summary: "кластер {{ $labels.cluster }}/{{ $labels.shard }}: возраст валидного полного выше порога"
          description: "per-cluster порог из серии max_age (политики бэкапов, arch/19 §2); runbook — arch/18 §2.7"
      - alert: BackupFullMissing
        expr: pgworker_backup_full_max_age_seconds unless pgworker_backup_full_age_seconds
        for: 0m
        labels: {severity: critical}
        annotations:
          summary: "кластер {{ $labels.cluster }}/{{ $labels.shard }}: валидного полного нет вовсе"
          description: "max_age эмитится, age нет — семантика IsValid планировщика; runbook — arch/18 §2.7"
      - alert: BackupWalLagHigh
        expr: pgworker_backup_wal_lag_segments > 1024
        for: 0m
        labels: {severity: warning}
        annotations:
          summary: "{{ $labels.cluster }}/{{ $labels.shard }}: отставание WAL-потока >1024 сегментов"
          description: "порог = дефолт PgWorker:Backups:Wal:LagMaxSegments (per-install конфиг; per-cluster WAL-политики в etcd нет, arch/19 §4); runbook — arch/18 §2.7"
      - alert: BackupWalStale
        expr: pgworker_backup_wal_last_uploaded_age_seconds > 300
        for: 0m
        labels: {severity: warning}
        annotations:
          summary: "{{ $labels.cluster }}/{{ $labels.shard }}: тишина WAL-загрузок >300с"
          description: "порог = дефолт PgWorker:Backups:Wal:StaleSec (per-install конфиг); runbook — arch/18 §2.7"
      - alert: BackupVerifyFailed
        expr: increase(pgworker_backup_verify_total{result="failed"}[15m]) > 0
        for: 0m
        labels: {severity: critical}
        annotations:
          summary: "{{ $labels.cluster }}/{{ $labels.shard }}: проверка полного бэкапа провалена"
          description: "increase[15m] при scrape 15с; runbook — arch/18 §2.7, arch/19 §5"
      - alert: BackupRestoreFailed
        expr: increase(pgworker_backup_restore_total{result="failed"}[15m]) > 0
        for: 0m
        labels: {severity: critical}
        annotations:
          summary: "{{ $labels.cluster }}/{{ $labels.shard }}: восстановление из бэкапа провалено"
          description: "терминальный FAILED restore-заявки; runbook — arch/18 §2.7, arch/19 §3.5"
      - alert: BackupDrillFailed
        expr: increase(pgworker_backup_drill_total{result="failed"}[15m]) > 0
        for: 0m
        labels: {severity: critical}
        annotations:
          summary: "{{ $labels.cluster }}/{{ $labels.shard }}: дрилл восстановимости провален"
          description: "восстановимость не доказана; runbook — arch/18 §2.7, arch/19 §3.6"
```

- [ ] **Step 2: Синтаксис YAML — локальная проверка**

```bash
python3 -c "import yaml,sys; d=yaml.safe_load(open('dev-stand/adminpanel/metrics/prometheus/rules.yml')); groups=[g for g in d['groups'] if g['name']=='backups']; alerts=[r for r in groups[0]['rules'] if 'alert' in r]; print(len(alerts), [a['alert'] for a in alerts])"
```
Expected: `7 ['BackupFullStale', 'BackupFullMissing', 'BackupWalLagHigh', 'BackupWalStale', 'BackupVerifyFailed', 'BackupRestoreFailed', 'BackupDrillFailed']`. (Окончательная валидация PromQL — загрузкой Prometheus на чеке 65, Task 12.)

- [ ] **Step 3: Обновить счётчики `checks/65-metrics.sh`**

Шаг 4 (rules): заменить

```bash
# 4) rules зарегистрированы (18 алертов: 11 + 7 группы backups §2.7)
rules=$(curl -fsS "$PROM/api/v1/rules" | jq '[.data.groups[].rules[] | select(.type=="alerting")] | length')
[ "$rules" -ge 18 ] || { echo "  ❌ алерт-рулы: $rules < 18"; exit 1; }
```

Шаг 5 (dashboards): заменить `[ "$ds" -ge 4 ]` → `[ "$ds" -ge 5 ]` (и сообщение `❌ дашборды: $ds < 5`).

- [ ] **Step 4: Commit**

```bash
git add dev-stand/adminpanel/metrics/prometheus/rules.yml dev-stand/adminpanel/checks/65-metrics.sh
git commit -m "feat(t14): группа алертов backups (7 правил: full stale/missing, wal lag/stale, verify/restore/drill failed) + счётчики чека 65 (18 рулов, 5 дашбордов)"
```

---

### Task 10: Grafana-дашборд `backups.json`

6 панелей spec §3.4 по образцу `pg.json`/`workers.json` (datasource-переменная по умолчанию, provisioning общий — отдельной правки не требует).

**Files:**
- Create: `dev-stand/adminpanel/metrics/grafana/dashboards/backups.json`

**Interfaces:**
- Consumes: серии Task 3/4, группа Task 9.
- Produces: дашборд uid `backups` — поднимает счётчик чека 65 до 5 (критерий приёмки 5).

- [ ] **Step 1: Создать файл**

```json
{
  "uid": "backups", "title": "Backups (PgWorker)", "schemaVersion": 41,
  "refresh": "30s", "time": {"from": "now-1h", "to": "now"},
  "panels": [
    {"type": "timeseries", "title": "Full backup age, s (by cluster/shard)", "gridPos": {"x":0,"y":0,"w":12,"h":8},
     "targets": [
        {"expr": "pgworker_backup_full_age_seconds", "legendFormat": "{{cluster}}/{{shard}}", "refId": "A"},
        {"expr": "pgworker_backup_full_max_age_seconds", "legendFormat": "max {{cluster}}/{{shard}}", "refId": "B"}]},
    {"type": "timeseries", "title": "WAL lag, segments", "gridPos": {"x":12,"y":0,"w":12,"h":8},
     "targets": [{"expr": "pgworker_backup_wal_lag_segments", "legendFormat": "{{cluster}}/{{shard}}", "refId": "A"}]},
    {"type": "timeseries", "title": "WAL uploaded age, s", "gridPos": {"x":0,"y":8,"w":12,"h":8},
     "targets": [{"expr": "pgworker_backup_wal_last_uploaded_age_seconds", "legendFormat": "{{cluster}}/{{shard}}", "refId": "A"}]},
    {"type": "timeseries", "title": "Verify outcomes (increase 15m, by result)", "gridPos": {"x":12,"y":8,"w":12,"h":8},
     "targets": [{"expr": "increase(pgworker_backup_verify_total[15m])", "legendFormat": "{{cluster}}/{{shard}} {{result}}", "refId": "A"}]},
    {"type": "timeseries", "title": "Restore outcomes (increase 15m, by result)", "gridPos": {"x":0,"y":16,"w":12,"h":8},
     "targets": [{"expr": "increase(pgworker_backup_restore_total[15m])", "legendFormat": "{{cluster}}/{{shard}} {{result}}", "refId": "A"}]},
    {"type": "timeseries", "title": "Drill outcomes (increase 15m, by result)", "gridPos": {"x":12,"y":16,"w":12,"h":8},
     "targets": [{"expr": "increase(pgworker_backup_drill_total[15m])", "legendFormat": "{{cluster}}/{{shard}} {{result}}", "refId": "A"}]}
  ]
}
```

- [ ] **Step 2: Валидация JSON**

```bash
python3 -m json.tool dev-stand/adminpanel/metrics/grafana/dashboards/backups.json >/dev/null && echo OK
```
Expected: `OK`.

- [ ] **Step 3: Commit**

```bash
git add dev-stand/adminpanel/metrics/grafana/dashboards/backups.json
git commit -m "feat(t14): Grafana-дашборд backups.json — возраст полных vs порог, WAL лаг/тишина, исходы verify/restore/drill (arch/18 §5.3)"
```

---

### Task 11: Интеграционная фиксация словаря — `MetricsTests`

Канон-тест arch/18 §6 (риск M3): фактические экспортированные имена §2.7 против словаря, включая лейблы verify/restore/drill. Серии процессов живут только под клэймом живого кластера — в WAF-хосте их нет, поэтому тест пинает singleton-`WorkerMetricsInstrumentation` из DI напрямую (проверяет ИМЕНА/лейблы экспозиции, не значения процессов) и скрейпит `/metrics`.

**Files:**
- Modify: `src/tests/PgWorker.IntegrationTests/Api/MetricsTests.cs` (новый Fact)

**Interfaces:**
- Consumes: `PgMetricsFixture.Factory.Services` (singleton instrumentation зарегистрирован Program.cs:85), марк-методы Task 3/4 + существующий `BackupWalLag`.
- Produces: критерий приёмки 1 (словарь зафиксирован тестом).

- [ ] **Step 1: Пишем failing-тест**

В `MetricsTests.cs` (коллекция `NonE2eCollection` — фикстура уже на классе):

```csharp
    // Канон-тест словаря §2.7 (arch/18 §6): все 7 бэкапных серий экспортируются
    // с каноническими именами и лейблами cluster/shard/result. Серии процессов
    // живут под клэймом живого кластера — instrumentation пинается напрямую:
    // тест фиксирует экспозицию имён/лейблов (маппинг марк-методов в серии).
    [Fact]
    public async Task Metrics_BackupSeries_CanonicalNamesAndLabels()
    {
        // Arrange — по одному факту каждой серии через singleton-инструментацию DI
        var m = fx.Factory.Services.GetRequiredService<Shared.Metrics.Worker.WorkerMetricsInstrumentation>();
        m.BackupFullAge("canon", new Dictionary<string, (long? LastValidUnix, long MaxAgeSec)>
        {
            ["s1"] = (1700000000, 86400),
            ["s2"] = (null, 43200),
        });
        m.BackupWalLag("canon", "s1", 3);
        m.BackupWalUploadedAge("canon", "s1", 42);
        m.BackupVerify("canon", "s1", "ok");
        m.BackupRestore("canon", "s1", "failed");
        m.BackupDrill("canon", "s1", "failed");

        // Act
        using var client = fx.Factory.CreateClient();
        var body = await client.GetStringAsync("/metrics", TestContext.Current.CancellationToken);

        // Assert — канонические имена §2.7 в фактическом экспорте
        foreach (var name in new[]
                 {
                     "pgworker_backup_full_age_seconds",
                     "pgworker_backup_full_max_age_seconds",
                     "pgworker_backup_wal_lag_segments",
                     "pgworker_backup_wal_last_uploaded_age_seconds",
                     "pgworker_backup_verify_total",
                     "pgworker_backup_restore_total",
                     "pgworker_backup_drill_total",
                 })
            body.Should().Contain(name, $"серия {name} словаря §2.7 обязана экспортироваться");

        // Лейблы (порядок cluster,shard,result = порядок добавления инструмента)
        body.Should().Contain("""cluster="canon",shard="s1"""");
        body.Should().Contain("""cluster="canon",shard="s1",result="ok"""");
        body.Should().Contain("""cluster="canon",shard="s1",result="failed"""");

        // Null-семантика age (spec §3.1): шард s2 без валидного — age-серия s2
        // НЕ эмитится, max_age s2 эмитится (порог пишется всегда)
        body.Should().Contain(
            """pgworker_backup_full_max_age_seconds{otel_scope_name="PgWorker",cluster="canon",shard="s2"} 43200""");
        body.Should().NotContain(
            """pgworker_backup_full_age_seconds{otel_scope_name="PgWorker",cluster="canon",shard="s2"}""");
    }
```

- [ ] **Step 2: Прогон (характеризационный: марк-методы Tasks 3–4 к этому моменту уже в ветке — тест фиксирует текущее поведение; задача выполняется строго после них)**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~MetricsTests"
```
Expected: PASS (3 Fact'а: 2 существующих + канон). Если падение — читать вывод: отсутствие серии = ошибка маппинга марк-метода (Tasks 3/4) или порядка лейблов.

- [ ] **Step 3: Commit**

```bash
git add src/tests/PgWorker.IntegrationTests/Api/MetricsTests.cs
git commit -m "test(t14): канон-тест словаря §2.7 — 7 бэкапных серий /metrics с лейблами cluster/shard/result, null-семантика age (arch/18 §6)"
```

---

### Task 12: Финальный гейт — сборка, полные прогоны, roadmap, E2E-маркер

Мерж-гейт задачи (spec §6.6–6.7): сборка 0 warnings, зелёные серии, E2E-кейс-маркер на свежем Release, roadmap-чистка тем же мерж-коммитом. Выполняется после всех задач; roadmap-правка и мерж — в ветке ДО мержа в main (тег снимается мерж-коммитом).

**Files:**
- Modify: `arch/roadmap/reliability.md` (снять пункт t14 и зависимость `← t14-backup-metrics-export` у t17)
- Modify: `arch/roadmap/reliability-report.md` (строка t14 → «Сделано»)
- Проверочные команды (без правок).

**Interfaces:**
- Consumes: всё выше.
- Produces: зелёная ветка, готовая к мержу в main.

- [ ] **Step 1: Сборка решения 0 warnings**

```bash
dotnet build src/PgWorker.slnx -c Release
```
Expected: `0 Warning(s)`, `0 Error(s)`.

- [ ] **Step 2: Юниты обоих затронутых проектов**

```bash
dotnet test src/tests/Shared.Metrics.UnitTests/Shared.Metrics.UnitTests.csproj -c Release
dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Release
```
Expected: все PASS. После серии — зачистка docker-остатков юнитов (юниты docker не поднимают, контроль штатный).

- [ ] **Step 3: Интеграционная серия PgWorker (не-E2E; docker-etcd фикстуры)**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release --filter "FullyQualifiedName!~E2e"
```
Expected: все PASS. ДО следующей серии дождаться финальной строки и зачистить остатки:

```bash
docker ps -a --filter "name=pgw-" --format '{{.Names}}' | head
docker network ls | grep -c kfw-net || true
# при осиротевших сетях (контейнеров 0, сетей > 0): docker network prune -f
```
Expected: пустой список контейнеров; сети kfw-net — только при живых контейнерах.

- [ ] **Step 4: Roadmap-правка (мерж-гейт трека reliability, spec §6.7)**

- `arch/roadmap/reliability.md`: удалить пункт `**t14-backup-metrics-export**` и снять зависимость `← t14-backup-metrics-export` из строки t17 (никаких пометок «сделана» — канон `arch/roadmap/README.md`).
- `arch/roadmap/reliability-report.md`: строку t14 перевести в «Сделано» (формат строки — как у соседних закрытых пунктов трека).
- Правка фиксируется коммитом Step 5 в ветке: весь код задачи попадает в main одним мержем — правило «тег снимается тем же коммитом, что и попадание задачи в main» (AGENTS.md) соблюдено; при squash/e2e-мерже main-агент включает правку в мерж-коммит (образец t28).

- [ ] **Step 5: Roadmap-коммит**

```bash
git add arch/roadmap/reliability.md arch/roadmap/reliability-report.md
git commit -m "docs(t14): мерж-гейт reliability — тег t14 снят (зависимость t17 освобождена), отчёт — Сделано"
```

- [ ] **Step 6: E2E-кейс-маркер на свежем Release (задача трогает src/PgWorker.App — канон AGENTS.md)**

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard
```
Expected: PASS. После прогона — зачистка контейнеров/сетей/томов серии (правила AGENTS.md; MarkFailed-окружение — по README-cleanup.txt без удаления).

- [ ] **Step 7: Чек 65 на стенде (ОБЯЗАТЕЛЬНЫЙ — критерии приёмки 4/5: счётчик 18 рулов + 5 дашбордов + загрузка группы backups Prometheus'ом)**

```bash
cd dev-stand/adminpanel && checks/00-up.sh && checks/65-metrics.sh
```
Expected: `✓ чек 65: мониторинг жив` — 18 рулов, 5 дашбордов; группа `backups` зарегистрирована (валидация PromQL загрузкой Prometheus). Стенд с бэкапами — ряды на дашборде `backups` (критерий 5, визуально). Сверить дашборд по `http://localhost:3000` (admin/admin).

- [ ] **Step 8: Итог ветки**

```bash
git log --oneline main..HEAD && git status --short
```
Expected: коммиты задач 1–12, рабочее дерево чистое. Ветка готова к мерж-ревью (main-агент).

---

## Self-Review (выполнен при написании плана)

**Покрытие spec:**
- §3.1 все 7 серий → Task 3 (verify/restore/drill counters), Task 4 (full_age/full_max_age/wal_uploaded_age; wal_lag уже существует — канонизирован), Task 11 (канон-тест имён).
- §3.2 кодовые компоненты → Tasks 2–8 (instrumentation, BackupProcess, WalStreamProcess, Restore/Drill, Program.cs wiring); rules/дашборд/чек → Tasks 9–10.
- §3.3 алерты (7, severity, пороги 1024/300, per-cluster max_age, increase[15m]) → Task 9.
- §3.4 дашборд (6 панелей, разрезы cluster/shard) → Task 10.
- §3.5 arch-first → Task 1 (правки arch/18 уже в дереве со spec-фазы — фиксируются коммитом до кода).
- §4 фазы 2–5 → Tasks 2–11; §6.6 мерж-гейт (сборка/юниты/интеграция/E2E-маркер) и §6.7 roadmap-гейт → Task 12.
- §5 ограничения: без новых конфиг-опций (пороги в правилах = дефолты Options.cs, комментарий в правиле); миграция verify-identity — Task 3; панельные правила не трогаем; RPO/RTO и Patroni-метрики вне скоупа — задач нет (осознанно).
- Критерии приёмки 1–7: 1→T11, 2→T3/T4/T2, 3→T3-Step5 (verify: все три исхода transient/failed/ok — по кейсу на observe-точку) + T7 (restore: failed и ok) + T8 (drill: failed-валидация и ok), 4→T9/T12-Step7, 5→T10/T12-Step7, 6→T12, 7→T12.

**Плейсхолдеры:** отсутствуют — каждый шаг содержит точный код/команду и ожидаемый результат.

**Консистентность типов:** сигнатура кортежа `(long? LastValidUnix, long MaxAgeSec)` едина в Task 2→5→11; `Action<string,string,long?>` (T4/T6) и `Action<string,string,string>` (T3/T7/T8) совпадают с марк-методами instrumentation (`BackupFullAge`, `BackupWalUploadedAge`, `BackupRestore`, `BackupDrill` — method-group совместимы с Action); `DebugState`-поля согласованы между Tasks 3/4.
