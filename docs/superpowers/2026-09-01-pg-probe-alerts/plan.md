# План: ошибки проб и подключений кластеров Pg — настоящими алертами

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ошибки live-проб Pg-кластеров становятся настоящими алертами: SQL-проба шарда — critical, Patroni-проба одного члена — warning, весь скоп молчит — один critical; lifecycle-цели (NOT_INITIALIZED/TO_REMOVE) подавлены.

**Architecture:** Переписывается единственное правило `ProbeFailedRule` (`AlertEngine` не трогается): правило идёт от целей текущего снапшота (Active-кластеры/шарды, matched-скопы), а не от результатов проб; severity назначается по каталогу arch/03 §4. Фронтенд, DTO, пробы и движок — без изменений. Контракт arch/ уже обновлён (spec-фаза).

**Tech Stack:** .NET 10, C# latest, `Nullable=enable`, `TreatWarningsAsErrors=true`; xUnit + FluentAssertions; Testcontainers (интеграционные); bash+jq (чеки стенда).

**Spec:** `docs/superpowers/2026-09-01-pg-probe-alerts/spec.md` (в этом же worktree; canonical arch — `arch/adminpanel/03-panels.md` §4, `arch/adminpanel/01-architecture.md` §8 — уже обновлены).

**Ревью Фазы 4 учтено (два раунда):** раунд 1 — Step 1.1 дополнен degenerate-сценариями «Active-шард без DSN» и «matched-скоп без членов»; Step 2.1 — скоп получает второго живего члена (per-member warning, не scope-critical). Раунд 2 (Critical) — в Step 2.1 скоп `moving-s1` привязан к существующему кластеру фикстуры: `Cluster = "demo"` (кластер `MovingCluster()` называется `"demo"`, TestSnapshots.cs:65; `Cluster = "moving"` не существует — скоп был бы пропущен фильтром правила, kind потерял бы покрытие).

## Global Constraints

- Все пути — в worktree `/Users/demakaev/ZCodeProject/worktrees/fix-pg-probe-alerts`; основной репо не трогаем.
- `TreatWarningsAsErrors=true` — код обязан собираться без ворнингов.
- Тесты — по AAA (комментарии `// Arrange / // Act / // Assert`), комментарии и документация — русские, идентификаторы — английские.
- Никаких изменений: `ProbeOrchestrator`, `SqlProbe`, `PatroniRestProbe`, `ProbeEnricher`, `AlertEngine`, `AlertsOptions`, DTO, фронтенд (`frontend/**`).
- Тексты Hint/Remedy непустые у каждого алерта (инвариант `AlertHintRemedyTests`).
- Id стабильны: `probe-failed:sql:<C>/<X>`, `probe-failed:patroni:<scope>/<member>` (как сегодня); новый шаблон — только `probe-failed:patroni-scope:<scope>` (spec §2 «Стабильность id»).
- Коммиты — в ветке `fix-pg-probe-alerts`, каждый таск завершается своим коммитом.

---

### Task 1: Правило `ProbeFailedRule` — новая семантика (unit, TDD)

**Files:**
- Modify: `src/AdminPanel.Core/Alerting/Rules/ProbeFailedRule.cs` (переписать целиком)
- Test: `src/tests/AdminPanel.UnitTests/HaAlertRulesTests.cs` (заменить тест `ProbeFailed_EachFailedResult_Info` на блок новых тестов, строки ~218–242)

**Interfaces:**
- Consumes: `EtcdSnapshot` (`Clusters: IReadOnlyList<ClusterInfo>`, `HaScopes: IReadOnlyList<HaScope>`, `Probes: IReadOnlyList<ProbeResult>`), `ClusterState` (`Active|NotInitialized|ToRemove`), `ShardState` (`Active|ToRemove`), `ShardInfo.DsnHosts`, `HaScope.Scope/Cluster/Shard/Matched/Members`, `ProbeResult.Kind/Target/Ok/Error`, `Alert`-record (10 позиционных полей, `src/AdminPanel.Core/Alert.cs`), `IAlertRule.Evaluate(EtcdSnapshot, AlertContext)`.
- Produces: алерты `probe-failed` severity Critical/Warning: id `probe-failed:sql:<C>/<X>` (details: `kind,target,error,dsnHosts`), `probe-failed:patroni:<scope>/<member>` (details: `kind,target,error`), `probe-failed:patroni-scope:<scope>` (details: `scope,cluster,shard,failed,total,error`). Сигнатура правила не меняется — DI-регистрация `[InjectAsSingleton(typeof(IAlertRule))]` остаётся.

- [ ] **Step 1.1: Переписать unit-тесты правила (красные)**

Вход: `src/tests/AdminPanel.UnitTests/HaAlertRulesTests.cs`; существующий тест `ProbeFailed_EachFailedResult_Info` (строки ~218–242), хелперы `TestSnapshots` (`Healthy`, `WithHaScopes`, `FullCluster`, `HaScopeDemo`), `Context()`, `Now` — уже в файле.

Действие: удалить тест `ProbeFailed_EachFailedResult_Info` целиком и вставить на его место пять тестов ниже (комментарий блока над ними — `// ==== probe-failed (spec 2026-09-01 §3.1) ====`):

```csharp
    // ==== probe-failed (spec 2026-09-01 §3.1) ====

    [Fact]
    public void ProbeFailed_SqlFailed_Critical()
    {
        // Arrange: Active-кластер, SQL-проба шарда упала (timeout).
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            Probes = [new ProbeResult("demo/s1", "sql", false, 4.0, "timeout", Now)],
        };

        // Act
        var alerts = new ProbeFailedRule().Evaluate(snapshot, Context()).ToList();

        // Assert: шард недоступен — critical; details несут ошибку и хосты DSN.
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Id.Should().Be("probe-failed:sql:demo/s1");
        alert.Severity.Should().Be(AlertSeverity.Critical);
        alert.Details!["error"].Should().Be("timeout");
        alert.Details["dsnHosts"].Should().Be("s1a,s1b");
    }

    [Fact]
    public void ProbeFailed_PatroniOneMemberFailed_Warning()
    {
        // Arrange: один из двух членов matched-скопа упал, второй жив.
        var snapshot = TestSnapshots.WithHaScopes(Now) with
        {
            Probes = [new ProbeResult("demo-s1/s1a", "patroni", false, 1.0, "connection refused", Now)],
        };

        // Act
        var alerts = new ProbeFailedRule().Evaluate(snapshot, Context()).ToList();

        // Assert: одиночный член — warning (одна нода ≠ весь кластер).
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Id.Should().Be("probe-failed:patroni:demo-s1/s1a");
        alert.Severity.Should().Be(AlertSeverity.Warning);
        alert.Details!["error"].Should().Be("connection refused");
    }

    [Fact]
    public void ProbeFailed_PatroniAllMembersFailed_SingleCriticalNoWarnings()
    {
        // Arrange: обе Patroni-пробы членов matched-скопа упали.
        var snapshot = TestSnapshots.WithHaScopes(Now) with
        {
            Probes =
            [
                new ProbeResult("demo-s1/s1a", "patroni", false, 1.0, "refused", Now),
                new ProbeResult("demo-s1/s1b", "patroni", false, 2.0, "timeout", Now),
            ],
        };

        // Act
        var alerts = new ProbeFailedRule().Evaluate(snapshot, Context()).ToList();

        // Assert: один critical на скоп; per-member warning не эмитятся —
        // один факт, один алерт (spec §1.3).
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Id.Should().Be("probe-failed:patroni-scope:demo-s1");
        alert.Severity.Should().Be(AlertSeverity.Critical);
        alert.Details!["failed"].Should().Be("2");
        alert.Details["total"].Should().Be("2");
        alert.Details["cluster"].Should().Be("demo");
    }

    [Fact]
    public void ProbeFailed_LifecycleTargets_Suppressed()
    {
        // Arrange: пробы падают по NOT_INITIALIZED/TO_REMOVE-кластерам,
        // TO_REMOVE-шарду Active-кластера и их HA-скопам.
        var fresh = TestSnapshots.FullCluster() with
        {
            Name = "fresh", DbName = "fresh", State = ClusterState.NotInitialized,
        };
        var dying = TestSnapshots.FullCluster() with
        {
            Name = "dying", DbName = "dying", State = ClusterState.ToRemove,
        };
        var shardRemoving = TestSnapshots.FullCluster() with
        {
            Shards = [TestSnapshots.FullCluster().Shards.Single() with { State = ShardState.ToRemove }],
        };
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            Clusters = [fresh, dying, shardRemoving],
            HaScopes =
            [
                TestSnapshots.HaScopeDemo(Now),
                TestSnapshots.HaScopeDemo(Now) with { Scope = "fresh-s1", Cluster = "fresh" },
                TestSnapshots.HaScopeDemo(Now) with { Scope = "dying-s1", Cluster = "dying" },
            ],
            Probes =
            [
                new ProbeResult("fresh/s1", "sql", false, 1.0, "refused", Now),
                new ProbeResult("dying/s1", "sql", false, 1.0, "refused", Now),
                new ProbeResult("demo/s1", "sql", false, 1.0, "refused", Now),
                new ProbeResult("fresh-s1/s1a", "patroni", false, 1.0, "refused", Now),
                new ProbeResult("fresh-s1/s1b", "patroni", false, 1.0, "refused", Now),
                new ProbeResult("dying-s1/s1a", "patroni", false, 1.0, "refused", Now),
                new ProbeResult("dying-s1/s1b", "patroni", false, 1.0, "refused", Now),
            ],
        };

        // Act
        var alerts = new ProbeFailedRule().Evaluate(snapshot, Context()).ToList();

        // Assert: подъём/демонтаж — не авария, lifecycle-цели не алертятся
        // (spec §1.4; прецедент — подавление shard-no-leader).
        alerts.Should().BeEmpty();
    }

    [Fact]
    public void ProbeFailed_NoProbesOrDegenerateTargets_Silent()
    {
        // Arrange: проб нет вовсе; orphan-результат по несуществующему скопу;
        // вырожденные цели: Active-шард без DSN с упавшей sql-пробой и
        // matched-скоп Active-кластера без членов (spec §3.3).
        var degenerate = TestSnapshots.Healthy(Now) with
        {
            Clusters =
            [
                TestSnapshots.FullCluster() with
                {
                    Shards = [TestSnapshots.FullCluster().Shards.Single() with { DsnHosts = [] }],
                },
            ],
            HaScopes = [TestSnapshots.HaScopeDemo(Now) with { Members = [] }],
            Probes =
            [
                new ProbeResult("demo/s1", "sql", false, 1.0, "refused", Now),
                new ProbeResult("demo-s1/s1a", "patroni", false, 1.0, "refused", Now),
            ],
        };

        // Act
        var empty = new ProbeFailedRule().Evaluate(TestSnapshots.WithHaScopes(Now), Context()).ToList();
        var orphan = new ProbeFailedRule().Evaluate(TestSnapshots.WithHaScopes(Now) with
        {
            Probes = [new ProbeResult("ghost-s1/s1a", "patroni", false, 1.0, "refused", Now)],
        }, Context()).ToList();
        var degenerateAlerts = new ProbeFailedRule().Evaluate(degenerate, Context()).ToList();

        // Assert: без результатов (пробы выключены), по исчезнувшей цели и по
        // вырожденным целям (шард без DSN, скоп без членов) — тишина
        // (spec §2, §3.3): правило идёт от целей снапшота.
        empty.Should().BeEmpty();
        orphan.Should().BeEmpty();
        degenerateAlerts.Should().BeEmpty();
    }
```

Выход: файл тестов с пятью новыми тестами вместо старого (включая сценарии «шард без DSN» и «скоп без членов» из spec §3.3).

Проверка: визуально — тесты компилируются по сигнатурам фикстур (см. Interfaces); старый `ProbeFailed_EachFailedResult_Info` удалён.

Spec: §3.3 (unit-сценарии — все перечисленные), §3.1 (логика правила).

- [ ] **Step 1.2: Прогнать новые тесты — убедиться в красных**

Вход: файл тестов из шага 1.1.

Действие:

```bash
cd /Users/demakaev/ZCodeProject/worktrees/fix-pg-probe-alerts
dotnet test src/tests/AdminPanel.UnitTests --filter "FullyQualifiedName~HaAlertRulesTests.ProbeFailed"
```

Выход: 5 тестов упали.

Проверка: ожидаемо FAIL всех пяти — старое правило даёт `Info`, не подавляет lifecycle-цели и алертит по любому результату без разбора целей (`Expected alerts to be empty, but found...` / `Expected alert.Severity to be Critical, but found Info`).

Spec: TDD-цикл §4 фаза 3.

- [ ] **Step 1.3: Реализовать новое правило**

Вход: `src/AdminPanel.Core/Alerting/Rules/ProbeFailedRule.cs` (текущая версия — info на каждый `!p.Ok`).

Действие: заменить содержимое файла целиком:

```csharp
using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Core.Alerting.Rules;

// probe-failed — severity по цели (arch/03 §4; spec 2026-09-01 §3.1): SQL-проба
// шарда Active-кластера упала = critical («шард недоступен» — ни один хост DSN
// не принял подключение или writable-мастер не найден); Patroni-проба одного
// члена matched-скопа упала = warning; Patroni-пробы всех членов скопа упали =
// один critical на скоп (per-member warning не эмитятся — один факт, один
// алерт). Lifecycle-цели (кластеры/шарды NOT_INITIALIZED/TO_REMOVE) не
// алертятся — подъём/демонтаж не авария (прецедент shard-no-leader); пробы по
// ним продолжают ходить, runtime-ошибки остаются в UI деталей. Правило идёт от
// целей текущего снапшота — исчезнувшая цель не алертится.
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class ProbeFailedRule : IAlertRule
{
    public const string KindName = "probe-failed";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        var activeClusters = snapshot.Clusters
            .Where(c => c.State == ClusterState.Active)
            .ToDictionary(c => c.Name);

        // SQL: шард с DSN и упавшей пробой — шард недоступен (critical).
        foreach (var cluster in activeClusters.Values)
        foreach (var shard in cluster.Shards.Where(s => s.DsnHosts.Count > 0 && s.State == ShardState.Active))
        {
            var failed = Find(snapshot.Probes, "sql", $"{cluster.Name}/{shard.Name}");
            if (failed is null)
                continue;

            yield return new Alert(
                $"{KindName}:sql:{cluster.Name}/{shard.Name}",
                AlertSeverity.Critical,
                KindName,
                $"sql:{cluster.Name}/{shard.Name}",
                $"SQL-проба шарда {cluster.Name}/{shard.Name} не удалась: {failed.Error}",
                new Dictionary<string, string>
                {
                    ["kind"] = "sql",
                    ["target"] = $"{cluster.Name}/{shard.Name}",
                    ["error"] = failed.Error ?? string.Empty,
                    ["dsnHosts"] = string.Join(",", shard.DsnHosts),
                },
                null,
                "панель не смогла подключиться ни к одному хосту DSN шарда либо writable-мастер не найден: шард недоступен целиком — либо кластер лежит, либо недостижим из сети панели; SQL-живость — предусловие live-данных (слоты/лаги/инвентарь)",
                AlertRemedy.OperatorRunbook,
                "проверьте контейнеры нод шарда и Patroni-скоп, достижимость хостов DSN из сети панели; панель ретраит пробу каждым тиком");
        }

        // Patroni: matched-скоп Active-кластера. Результат есть по каждому
        // члену и все упали → один critical на скоп; иначе per-member warning.
        foreach (var scope in snapshot.HaScopes.Where(s => s.Matched && s.Cluster is not null
                     && activeClusters.ContainsKey(s.Cluster)))
        {
            var results = scope.Members
                .Select(m => Find(snapshot.Probes, "patroni", $"{scope.Scope}/{m.Name}"))
                .ToList();

            if (results.All(r => r is null))
                continue; // тиков не было / проба выключена / членов нет — тишина

            if (results.All(r => r is { Ok: false }))
            {
                var first = results.OfType<ProbeResult>().First(r => !r.Ok);
                yield return new Alert(
                    $"{KindName}:patroni-scope:{scope.Scope}",
                    AlertSeverity.Critical,
                    KindName,
                    $"patroni-scope:{scope.Scope}",
                    $"Patroni-скоп {scope.Scope} недоступен целиком: {results.Count(r => r is { Ok: false })}/{scope.Members.Count} проб упали ({first.Error})",
                    new Dictionary<string, string>
                    {
                        ["scope"] = scope.Scope,
                        ["cluster"] = scope.Cluster!,
                        ["shard"] = scope.Shard ?? string.Empty,
                        ["failed"] = results.Count(r => r is { Ok: false }).ToString(),
                        ["total"] = scope.Members.Count.ToString(),
                        ["error"] = first.Error ?? string.Empty,
                    },
                    null,
                    "ни один член скопа не ответил на Patroni REST :8008 — HA-кластер Patroni невидим для панели: недоступен целиком либо изолирован от сети панели; REST-живость — предусловие live-данных HA",
                    AlertRemedy.OperatorRunbook,
                    "проверьте patroni-эмуляторы/ноды скопа (контейнеры, сеть, HostMap стенда) и живость Patroni; панель ретраит пробы каждым тиком");
                continue;
            }

            foreach (var member in scope.Members)
            {
                var failed = Find(snapshot.Probes, "patroni", $"{scope.Scope}/{member.Name}");
                if (failed is not { Ok: false })
                    continue;

                yield return new Alert(
                    $"{KindName}:patroni:{scope.Scope}/{member.Name}",
                    AlertSeverity.Warning,
                    KindName,
                    $"patroni:{scope.Scope}/{member.Name}",
                    $"проба patroni по {scope.Scope}/{member.Name} не удалась: {failed.Error}",
                    new Dictionary<string, string>
                    {
                        ["kind"] = "patroni",
                        ["target"] = $"{scope.Scope}/{member.Name}",
                        ["error"] = failed.Error ?? string.Empty,
                    },
                    null,
                    "проба панели не дошла до цели: пробы (Patroni REST/SQL) идут из контейнера панели, неудача означает сетевую недостижимость или нездоровье цели; успешная проба — предусловие live-данных",
                    AlertRemedy.OperatorRunbook,
                    "проверьте достижимость цели из сети панели (сервисы стенда) и живость самой цели; панель ретраит пробу следующим тиком");
            }
        }
    }

    // Lookup результата пробы тика по kind+target (строки — ordinal).
    private static ProbeResult? Find(IReadOnlyList<ProbeResult> probes, string kind, string target)
        => probes.FirstOrDefault(p => p.Kind == kind && p.Target == target);
}
```

Примечания к реализации:
- Условие «весь скоп молчит» — `results.All(r => r is { Ok: false })`: истинно только когда по **каждому** члену есть результат и все упали (null-результат делает условие ложным). Гонка «новый член появился в etcd между тиком проб и KV-тиком» не даёт ложного critical — такой член не алертится до следующего тика. Скоп с одним членом и упавшей пробой законно даёт scope-critical.
- `results.All(r => r is null)` на пустом списке (скоп без членов) = true → тишина.
- Фильтр скопов `activeClusters.ContainsKey(s.Cluster)` требует, чтобы `scope.Cluster` ссылался на реально существующий Active-кластер снапшота — включая тестовые фикстуры (см. Task 2).

Выход: новое правило; DI-регистрация прежняя.

Проверка: `dotnet build src/AdminPanel.Core` — без ворнингов (`TreatWarningsAsErrors=true`).

Spec: §3.1 (полная логика), §2 (цели-не-результаты, стабильность id).

- [ ] **Step 1.4: Прогнать новые тесты — зелёные**

Вход: реализация из шага 1.3.

Действие:

```bash
dotnet test src/tests/AdminPanel.UnitTests --filter "FullyQualifiedName~HaAlertRulesTests.ProbeFailed"
```

Выход: 5 тестов зелёные.

Проверка: PASS ×5; в выводе нет других упавших ProbeFailed-тестов (старый удалён).

Spec: критерий приёмки 6.

- [ ] **Step 1.5: Коммит**

Вход: зелёные тесты шага 1.4.

Действие:

```bash
git add src/AdminPanel.Core/Alerting/Rules/ProbeFailedRule.cs src/tests/AdminPanel.UnitTests/HaAlertRulesTests.cs
git commit -m "feat(alerts): probe-failed — честная severity по цели (sql=critical, patroni=warning, весь скоп=critical) + подавление lifecycle-целей (spec 2026-09-01-pg-probe-alerts §3.1; arch/03 §4)"
```

Выход: коммит в `fix-pg-probe-alerts`.

Проверка: `git log --oneline -1` — коммит на месте; `git status --short` — чисто по этим файлам.

Spec: фаза 2 §4.

---

### Task 2: Смежные unit-тесты — каталожная фикстура и порядок движка

**Files:**
- Test: `src/tests/AdminPanel.UnitTests/AlertHintRemedyTests.cs` (`CatalogSnapshot()`, HaScopes ~строки 120–130)
- Test: `src/tests/AdminPanel.UnitTests/HaAlertRulesTests.cs` (`HaRules_FullEngine_Scenario`, строки ~529–576)

**Interfaces:**
- Consumes: `ProbeFailedRule` из Task 1: идёт от целей снапшота — проба без скопа в `HaScopes` не даёт алерта; фильтр скопов требует существующий Active-кластер (`activeClusters.ContainsKey(scope.Cluster)`); семантика scope-critical: у скопа с единственным членом упавшая проба даёт critical, не warning. Важно: кластер фикстуры `TestSnapshots.MovingCluster(Now)` в `CatalogSnapshot()` носит имя `"demo"` (TestSnapshots.cs:65 — `MovingCluster` возвращает `Name = "demo"`, без переименования), кластера `"moving"` в снапшоте нет. `AlertEngine` сортирует severity ↓ → kind (Ordinal) → target (Ordinal).
- Produces: `CatalogSnapshot()` покрывает kind `probe-failed` per-member **warning** скопом `moving-s1` из двух членов (один упал, у второго результата нет), привязанным к существующему Active-кластеру `"demo"`; инвариант `EveryPgAlert_Kind_HasHintAndRemedy` зелёный.

- [ ] **Step 2.1: Дополнить `CatalogSnapshot` скопом `moving-s1` (двое членов, кластер `demo`)**

Вход: `AlertHintRemedyTests.CatalogSnapshot()`; в `Probes` уже есть упавшая проба `new ProbeResult("moving-s1/s1a", "patroni", false, 5.0, "connection refused", Now)`, но скопа `moving-s1` в `HaScopes` нет — после Task 1 kind `probe-failed` теряет покрытие (правило идёт от целей).

Действие: в `CatalogSnapshot()` в список `HaScopes` (после скопа `moving-s2`) добавить:

```csharp
                // matched-скоп Active-кластера: один член с упавшей patroni-пробой,
                // у второго результата нет → per-member warning probe-failed
                // (scope-critical не эмитится: не все члены упали). Cluster="demo" —
                // имя кластера фикстуры MovingCluster (TestSnapshots: Name="demo";
                // "moving" в снапшоте не существует, такой скоп был бы пропущен
                // фильтром activeClusters.ContainsKey — повторное ревью Фазы 4)
                new HaScope("moving-s1", "demo", "s1", true, "s1a", 738273634528L, true, null, null, null,
                [
                    new HaMember("s1a", "s1a", 5432, "master", "running", 1L, 0L, Now, null, null),
                    new HaMember("s1b", "s1b", 5432, "replica", "streaming", 1L, 0L, Now, null, null),
                ], null),
```

Выход: каталожный снапшот даёт warning `probe-failed:patroni:moving-s1/s1a`. Два обязательных условия (оба — замечания ревью Фазы 4): (а) второй член `s1b` — с единственным членом условие «результаты есть и все !Ok» истинно тривиально, был бы critical `probe-failed:patroni-scope:moving-s1`; (б) `Cluster = "demo"` — кластер `"moving"` в снапшоте не существует, скоп с такой ссылкой был бы пропущен фильтром `activeClusters.ContainsKey` и kind потерял бы покрытие.

Проверка: скоп компилируется по сигнатуре `HaScope` (12 позиционных полей); `Cluster = "demo"` указывает на Active-кластер фикстуры `MovingCluster` (шард `s1` — Active с DSN), фильтр правила его пропускает; новый скоп не порождает `shard-no-leader` (лидер `s1a` задан), `ha-member-not-streaming` (master → running, replica → streaming) и patroni-алерт по `s1b` (результата нет); существующий скоп `moving-s2` с `Cluster = "moving"` не трогаем — его kind (`shard-no-leader`) не зависит от существования кластера.

Spec: §3.3 (`AlertHintRemedyTests` — kind остаётся в списке), §3.1 п.2 (условие scope-critical), §2 (цели-не-результаты).

- [ ] **Step 2.2: Обновить порядок в `HaRules_FullEngine_Scenario`**

Вход: `HaAlertRulesTests.HaRules_FullEngine_Scenario` — `probe-failed:patroni:demo-s1/s1a` раньше был info и стоял последним; теперь warning.

Действие: в Assert заменить блок ожидаемого порядка и комментарий:

```csharp
        // Assert: сортировка severity → kind (Ordinal): critical (shard-no-leader,
        // slot-wal-lost) → warning (ha-member-not-streaming, probe-failed,
        // slot-invalidation-risk, sync-standby-missing). probe-failed теперь
        // warning (spec 2026-09-01 §3.1) — стоит между ha-member и slot-риском.
        // Слот фикстуры несёт safe_wal_size 512 МБ < 1 GiB — risk-алерт входит
        // в сценарий законно (6-й). t04/t05-правила на этой фикстуре молчат.
        alerts.Select(a => a.Id).Should().ContainInOrder(
            "shard-no-leader:demo-s1",
            "slot-wal-lost:demo/s1/move_bucket_3",
            "ha-member-not-streaming:demo-s1/s1b",
            "probe-failed:patroni:demo-s1/s1a",
            "slot-invalidation-risk:demo/s1/move_bucket_3",
            "sync-standby-missing:demo/s1");
        alerts.Select(a => a.Id).Should().HaveCount(6);
```

Выход: ожидание соответствует новой сортировке (Ordinal внутри warning: `ha-…` < `probe-…` < `slot-…` < `sync-…`).

Проверка: мысленно — порядок отсортирован по severity ↓, внутри kind по Ordinal.

Spec: §3.3 (unit-сценарии не содержат ожиданий info для Active-целей — критерий 6).

- [ ] **Step 2.3: Прогнать весь unit-набор панели — зелёный**

Вход: правки шагов 2.1–2.2.

Действие:

```bash
dotnet test src/tests/AdminPanel.UnitTests
```

Выход: весь `AdminPanel.UnitTests` зелёный.

Проверка: 0 failed; в частности `EveryPgAlert_Kind_HasHintAndRemedy` (полное покрытие kinds, включая `probe-failed`) и `HaRules_FullEngine_Scenario` — PASS. Если красные — только ожидания, связанные с probe-failed (другие правила не менялись).

Spec: критерии 5–6.

- [ ] **Step 2.4: Коммит**

Вход: зелёный набор шага 2.3.

Действие:

```bash
git add src/tests/AdminPanel.UnitTests/AlertHintRemedyTests.cs src/tests/AdminPanel.UnitTests/HaAlertRulesTests.cs
git commit -m "test(alerts): каталожная фикстура probe-failed (скоп moving-s1 → кластер demo фикстуры, двое членов — per-member warning) + новый порядок движка (spec 2026-09-01 §3.3)"
```

Выход: коммит.

Проверка: `git log --oneline -1`.

Spec: фаза 3 §4.

---

### Task 3: Интеграционные тесты API (`/api/alerts`)

**Files:**
- Test: `src/tests/AdminPanel.IntegrationTests/InspectionProbeApiTests.cs` (тест `LiveEtcd_FailedProbe_ProducesProbeFailedAlert` ~строки 79–106; новый тест рядом)

**Interfaces:**
- Consumes: `AuthWebFactory`, `EtcdContainerFixture`, `RefreshedAsync(ProbeState?)`, `ApiTestLogin.LoginAsync` — уже в файле; сид даёт Active-кластер `demo` с шардом `s1` (DSN `demo/s1`) и HA-скопом `demo-s1`.
- Produces: HTTP-контракт-проверки: `probe-failed:patroni:demo-s1/s1a` → `severity=warning`; `probe-failed:sql:demo/s1` → `severity=critical`.

- [ ] **Step 3.1: Правка ожидания severity + новый sql-сценарий**

Вход: `LiveEtcd_FailedProbe_ProducesProbeFailedAlert` — сейчас ждёт `"info"` (строка ~103).

Действие: в `LiveEtcd_FailedProbe_ProducesProbeFailedAlert`:
1) комментарий перед Assert заменить на: `// Assert: одиночная patroni-проба — warning (spec 2026-09-01 §3.1); ha-member-not-streaming по упавшей пробе не вычисляется (spec §3.13/§3.14).`
2) строку `probeAlert.GetProperty("severity").GetString().Should().Be("info");` заменить на `probeAlert.GetProperty("severity").GetString().Should().Be("warning");`

и добавить новый тест после него:

```csharp
    [Fact]
    public async Task LiveEtcd_FailedSqlProbe_CriticalAlert()
    {
        // Arrange: SQL-проба шарда Active-кластера demo упала — шард недоступен.
        var at = DateTimeOffset.UtcNow;
        var probes = new ProbeState(
            at,
            [new ProbeResult("demo/s1", "sql", false, 4.0, "timeout", at)],
            new Dictionary<string, HaMemberProbe>(),
            new Dictionary<string, ShardRuntime>());
        _factory.Snapshot = await RefreshedAsync(probes);
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act
        using var response = await client.GetAsync("/api/alerts", TestContext.Current.CancellationToken);

        // Assert: неработающий кластер — critical-алерт (spec 2026-09-01 §1.1);
        // id и details стабильны для фильтров ?kind=.
        var alerts = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        var alert = alerts.EnumerateArray().Single(a =>
            a.GetProperty("id").GetString() == "probe-failed:sql:demo/s1");
        alert.GetProperty("severity").GetString().Should().Be("critical");
        alert.GetProperty("details").GetProperty("error").GetString().Should().Be("timeout");
    }
```

Выход: два интеграционных сценария probe-failed (warning patroni + critical sql).

Проверка: код компилируется по образцу соседнего теста (те же using).

Spec: §3.3 (integration), критерий 1.

- [ ] **Step 3.2: Прогнать интеграционные тесты**

Вход: правки шага 3.1; в окружении нужен живой docker (Testcontainers поднимает etcd).

Действие:

```bash
dotnet test src/tests/AdminPanel.IntegrationTests --filter "FullyQualifiedName~InspectionProbeApiTests"
```

Выход: сценарии InspectionProbeApiTests зелёные, включая новый.

Проверка: 0 failed (в т.ч. `LiveEtcd_ProbeStateEnriches_HaAndClusterApi` — probe-failed по-прежнему пуст при живых пробах); если docker недоступен — зафиксировать и вернуться на финальной верификации (Task 5).

Spec: критерии 1, 6.

- [ ] **Step 3.3: Коммит**

Вход: зелёные тесты шага 3.2.

Действие:

```bash
git add src/tests/AdminPanel.IntegrationTests/InspectionProbeApiTests.cs
git commit -m "test(alerts-api): упавшая SQL-проба Active-шарда — critical probe-failed; одиночная patroni — warning (spec 2026-09-01 §3.3)"
```

Выход: коммит.

Проверка: `git log --oneline -1`.

Spec: фаза 3 §4.

---

### Task 4: Чек стенда + практики

**Files:**
- Modify: `dev-stand/adminpanel/checks/40-live-probes.sh` (п.3, строки ~47–54)
- Modify: `docs/adminpanel/03-probes-alerts.md` (таблица «AlertEngine — 25 правил» и «Грабли»)

**Interfaces:**
- Consumes: новая семантика из Task 1: неподнятые кластеры чека 15 (NOT_INITIALIZED) больше не дают `probe-failed` вовсе; фильтр чека можно усилить.
- Produces: чек 40 п.3 требует отсутствия `probe-failed` полностью; практики синхронны каталогу arch/03 §4.

- [ ] **Step 4.1: Усилить условие чека 40-live-probes**

Вход: `dev-stand/adminpanel/checks/40-live-probes.sh` п.3 — сейчас терпит `probe-failed` info от неподнятых кластеров.

Действие: заменить блок п.3 (строки ~47–54) на:

```bash
# 3) никаких ошибок проб и расхождений (spec §7.5 п.4): probe-failed быть не
#    должно вовсе — неподнятые кластеры чека 15 (canon10/smoke/solo) подавлены
#    lifecycle-правилом (NOT_INITIALIZED, arch/03 §4), живые цели — без ошибок.
api /api/alerts | jq -e \
  'all(.[]; .kind != "probe-failed"
     and .kind != "inventory-mismatch" and .kind != "shard-no-master")' >/dev/null \
  || { echo "❌ /api/alerts: есть probe-failed / inventory-mismatch / shard-no-master"; exit 1; }
echo "  алертов проб/инвентаря/без-мастера нет"
```

Выход: чек требует полного отсутствия `probe-failed`.

Проверка: `bash -n dev-stand/adminpanel/checks/40-live-probes.sh` — синтаксис ок.

Spec: §3.3 (стенд), критерий 7.

- [ ] **Step 4.2: Обновить практики `docs/adminpanel/03-probes-alerts.md`**

Вход: таблица «## AlertEngine — 25 правил» — строка `| пробы (1) | `probe-failed` |`; грабля «`probe-failed` ≠ пустые данные».

Действие: два точечных исправления:
1) строку таблицы заменить на:

```markdown
| пробы (1) | `probe-failed` (sql→critical, patroni→warning, весь скоп молчит→critical один на скоп; lifecycle-цели NOT_INITIALIZED/TO_REMOVE подавлены — arch/03 §4) |
```

2) граблю «**`probe-failed` ≠ пустые данные**: отказ пробы оставляет etcd-часть (поля null), SQL-поля в UI скрываются с пометкой (arch/01 §8)» дополнить вторым предложением:

```markdown
- **`probe-failed` — severity по цели (2026-09-01)**: SQL-проба Active-шарда
  упала = critical («кластер не работает»); Patroni одного члена — warning,
  все члены скопа — один critical; NOT_INITIALIZED/TO_REMOVE не алертятся
  (подъём/демонтаж — не авария), но пробы по ним ходят и runtime-ошибки
  остаются в деталях.
```

Выход: практики синхронны arch/03 §4.

Проверка: перечитать оба фрагмента — формулировки совпадают с каталогом.

Spec: §3.4 (практики), критерий 8.

- [ ] **Step 4.3: Коммит**

Вход: правки 4.1–4.2.

Действие:

```bash
git add dev-stand/adminpanel/checks/40-live-probes.sh docs/adminpanel/03-probes-alerts.md
git commit -m "chore(stand,docs): чек 40 требует отсутствия probe-failed вовсе (lifecycle-подавление); практики — severity по цели (spec 2026-09-01 §3.3–3.4)"
```

Выход: коммит.

Проверка: `git log --oneline -1`; `bash -n` уже пройден.

Spec: фаза 4 §4.

---

### Task 5: Финальная верификация

**Files:**
- Modify: ничего (только проверки; при найденных дефектах — возврат в соответствующий таск)

**Interfaces:**
- Consumes: все коммиты Task 1–4.
- Produces: доказательство зелёности — сборка + все тесты панели (+ опционально живой стенд).

- [ ] **Step 5.1: Полная сборка solution**

Вход: ветка `fix-pg-probe-alerts` со всеми коммитами.

Действие:

```bash
cd /Users/demakaev/ZCodeProject/worktrees/fix-pg-probe-alerts
dotnet build
```

Выход: сборка всего solution.

Проверка: 0 error, 0 warning (`TreatWarningsAsErrors=true` — любой warning = error).

Spec: ограничения §2.

- [ ] **Step 5.2: Все тесты панели**

Вход: сборка шага 5.1.

Действие:

```bash
dotnet test src/tests/AdminPanel.UnitTests
dotnet test src/tests/AdminPanel.IntegrationTests
```

Выход: оба набора зелёные (integration — при живом docker).

Проверка: 0 failed в обоих прогонах. Остальные наборы (PgWorker/KafkaWorker) не затронуты — правило панели локальное.

Spec: критерии 6–7.

- [ ] **Step 5.3: Живой стенд (опционально, при доступном docker-окружении)**

Вход: полный dev-стенд (`dev-stand/adminpanel/checks/00-up.sh`).

Действие:

```bash
cd /Users/demakaev/ZCodeProject/worktrees/fix-pg-probe-alerts/dev-stand/adminpanel/checks
./00-up.sh && ./20-alerts.sh && ./40-live-probes.sh
```

Выход: чеки зелёные в новой формулировке (40 — probe-failed нет вовсе).

Проверка: оба чека заканчиваются на `✓`; ручная доп-проверка критерия 1: `docker stop` одного PG-контейнера демо-шарда → в `/api/alerts` в течение ≤40 c появляется `probe-failed:sql:demo/s1` с `severity=critical`; `docker start` — алерт гаснет. Если стенд не поднимается по environmental-причинам — зафиксировать в отчёте, не блокировать мерж (unit+integration уже доказали поведение).

Spec: критерии 1–5, 7; §4 фаза 5.

- [ ] **Step 5.4: Итоговый статус**

Вход: результаты 5.1–5.3.

Действие: `git status --short` — рабочее дерево чистое; `git log --oneline -5` — 4 таск-коммита (+ spec/arch из фазы spec).

Выход: ветка готова к ревью и мерж-гейту dev-flow.

Проверка: список коммитов соответствует Task 1–4; незакоммиченных файлов нет.

Spec: §4 (фазы), §6 (критерии приёмки).
