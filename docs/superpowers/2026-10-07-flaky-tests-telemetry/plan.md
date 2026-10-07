# t29-flaky-tests: фазовая телеметрия и диагностика флакающих тестов — план реализации

> **Для исполнителей:** используйте superpowers:subagent-driven-development (рекомендуется) или superpowers:executing-plans — задача за задачей. Шаги отмечаются чекбоксами (`- [ ]`). Каждый исполнитель видит СВОЮ задачу; межзадачные контракты — в блоках «Interfaces».

**Цель:** по логам и артефактам любого прогона t29-класса видно место торможения/сбоя (фаза, elapsed, budget, прогресс-тики, последний успешный шаг) без перезапуска тестов и без чтения кода.

**Архитектура:** единый статический хелпер `E2ePhase.WaitAsync` (формат `[PHASE]`/`[PHASE-TICK]`, `phases.log`, порог 60 с, failed-phase-сбор) замещает голые ожидания t29-классов; `RunProcessAsync` сохраняет вывод процесса при kill по бюджету; не-E2E фикстуры etcd классифицируют транзиентные таймауты проб как «ещё не готов».

**Технологии:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`), xunit.v3, testcontainers, docker CLI. Только тестовый контур — прод-код воркеров не трогаем.

**Spec:** `docs/superpowers/2026-10-07-flaky-tests-telemetry/spec.md` (канон; план аргументируется от него).

## Глобальные ограничения (действуют на каждую задачу)

- ЗАПРЕЩЕНО менять любой таймаут/бюджет/окно/ассерт: числа `TimeSpan`/`Task.Delay`/`CancelAfter` до и после задачи идентичны (spec §2, AC8). Миграция ожидания = перенос того же бюджета в `E2ePhase.WaitAsync`.
- НЕ трогаем: прод-код (`src/PgWorker.App|Core|Provisioning|Etcd|Backups|Moves|WalReceiver` — только потребляются тестами), `arch/**`, копии `WaitPhaseAsync` в не-t29-классах (`E2eRestoreScenarios`, `E2eRestoreNewClusterScenarios`, `E2eSupervisor*`).
- Пробы HaEtcd (5×2 с) НЕ превращаются в ретрай — инвариант (spec §2, §3.5).
- Docker-CLI вызовы в прогресс-тиках ЗАПРЕЩЕНЫ (spec §4.1, §9); S3-листинг в тиках — не чаще 1 раза в 5 с (интервал тиков и есть 5 с).
- Комментарии/доки — по-русски, идентификаторы — по-английски. Попадать в тон окружающего кода.
- Сборка: `dotnet build` без ошибок И без warnings (`TreatWarningsAsErrors=true`).
- Все пути ниже — от корня worktree `/Users/demakaev/ZCodeProject/worktrees/feat-flaky-tests-telemetry`.
- Точечная сборка тестового проекта: `dotnet build src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release` (из корня worktree).
- Прогоны docker-серий — только в задачах 11 и 14 (AC7/AC10); после финальной строки каждой серии — страховочная проверка чистоты (см. задачу 11 п.6 и задачу 14 п.4).
- Коммит в feature-ветке `feat-flaky-tests-telemetry` — свободно, после каждой задачи; стиль сообщений: `test(t29): …` / `docs(t29): …`.

---

> **СТАТУС (дополнение по spec §3.9/§3.10).** Задачи 1–11 ИСПОЛНЕНЫ и
> закоммичены (коммиты `592a6f7`…`ca1524f`; в ветку смержен свежий main —
> `ce0bf7c`): `E2ePhase`, `RunProcessAsync`-logFile, миграция шести
> t29-классов, не-E2E-ретрай etcd, `docs/e2e-launch.md`, приёмка — пройдены.
> Исполнению подлежат ТОЛЬКО задачи 12–14 — новые улики прогона 43 фактов
> 2026-10-07 (WalStream_MasterDown 16 м 29 с; Strict×2 5–6 м). База
> AC8-диффа задач 12–14 — коммит `ce0bf7c` (текущий HEAD на момент старта).

---

### Задача 1: `E2ePhase` — единый фазовый телеметрический хелпер

**Вход (предусловие):** ветка `feat-flaky-tests-telemetry` чистая; spec одобрен; nothing из кода ещё не менялся.

**Действие (файлы):**
- Create: `src/tests/PgWorker.IntegrationTests/E2e/E2ePhase.cs`

**Interfaces (производит — на этот контракт опираются задачи 3–8):**

```csharp
namespace PgWorker.IntegrationTests.E2e;

/// <summary>
/// Единый фазовый телеметрический контур t29-классов (spec t29 §4.1): замер
/// фазы + строки [PHASE]/[PHASE-TICK] в журнал теста (Console — прецедент
/// StaticPhase) и в <see cref="E2eEnvironment.ArtifactsDir"/>/phases.log с
/// UTC-меткой; пересечение 60 с — немедленный сбор docker-диагностики;
/// провал окна (ok=False) — сбор failed-phase-* ДО возврата сценарию.
/// Копии WaitPhaseAsync в не-t29-классах не мигрируются (объём); канон для
/// новых сценариев — docs/e2e-launch.md.
/// </summary>
internal static class E2ePhase
{
    /// <summary>Порог автосбора диагностики (docs/e2e-launch.md §2).</summary>
    private static readonly TimeSpan SlowPhaseThreshold = TimeSpan.FromSeconds(60);

    /// <summary>Интервал прогресс-тиков: короткие окна 10–15 с дают 2–3 строки
    /// динамики (spec §4.1); docker-CLI/S3 в тиках — не чаще этого интервала.</summary>
    private static readonly TimeSpan TickInterval = TimeSpan.FromSeconds(5);

    /// <summary>Полл условия — 0.5 с, как E2eFixture.WaitForAsync (семантика
    /// ожиданий не меняется: только телеметрия вокруг).</summary>
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(500);

    public static async Task<bool> WaitAsync(
        E2eEnvironment fx,
        string phase,
        Func<Task<bool>> condition,
        TimeSpan budget,
        CancellationToken ct,
        Func<Task<string>>? progress = null)
}
```

Алгоритм `WaitAsync` (важно: полл, тики и пороги — в одном цикле, НЕ обёртка над `WaitForAsync`, которой не видно внутреннего времени):

1. `var sw = Stopwatch.StartNew();` — T0 фазы.
2. Цикл: `if (await condition()) → EmitFinal(ok: true); return true;`
3. `if (sw.Elapsed >= budget) → break;` (провал окна).
4. Немедленный slow-phase: при `sw.Elapsed >= SlowPhaseThreshold` и ещё не собиравшемся — однократно `await fx.CollectDiagnosticsAsync($"slow-phase-{phase}")` (сбор в цикле — «немедленно», не дожидаясь конца фазы; повторные вызовы подавляются флагом).
5. Тик: при `progress is not null && sw.Elapsed >= nextTick` — снять снапшот, строка `[PHASE-TICK]`, `nextTick += TickInterval`. Исключения прогресс-делегата глотаются («лучшими усилиями», снапшот = `<progress error: …>`): прогресс из SQL/etcd может кидать на рестартующих нодах и не должен ронять фазу.
6. `await Task.Delay(PollInterval, ct);` — отмена теста всплывает как из `WaitForAsync` (итоговой строки не пишем: фаза не завершена).
7. После break (бюджет исчерпан): `await fx.CollectDiagnosticsAsync($"failed-phase-{phase}")` ДО возврата управления (spec §4.4 — контейнеры ещё живые, teardown ещё не мутировал); `EmitFinal(ok: false); return false;`.

`EmitFinal(ok)`: строка строго формата spec §4.1
`[PHASE] {phase}: ok={ok}, elapsed={sw.Elapsed.TotalSeconds:F1}s, budget={budget.TotalSeconds:0.#}s[, progress={последний снапшот}]`
— прогресс-часть добавляется только при `progress != null`; если тиков не было (фаза короче 5 с), снапшот снимается однократно для итоговой строки.

`EmitTick`: `[PHASE-TICK] {phase} t={sw.Elapsed.TotalSeconds:F1}s: {снапшот}`.

Обе строки печатаются: (а) `Console.WriteLine` — журнал теста (xUnit буферизует до конца факта — прецедент `E2eEnvironment.StaticPhase`); (б) `File.AppendAllText(Path.Combine(fx.ArtifactsDir, "phases.log"), $"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} {line}\n")` — UTC-метка сопоставима с `docker logs --timestamps` (spec §4.2); файловая запись в try/catch — телеметрия не роняет тест.

`CollectDiagnosticsAsync` уже глотает свои ошибки (E2eEnvironment.cs:864–927) — дополнительных try/catch не нужно.

- [ ] **Шаг 1.1.** Создать `src/tests/PgWorker.IntegrationTests/E2e/E2ePhase.cs` с контрактом и алгоритмом выше (комментарии — по-русски, в тоне окружающего кода).
- [ ] **Шаг 1.2.** Проверка — точечная сборка: `dotnet build src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release`. Ожидание: Build succeeded, 0 Error(s), 0 Warning(s).
- [ ] **Шаг 1.3.** Коммит: `git add src/tests/PgWorker.IntegrationTests/E2e/E2ePhase.cs && git commit -m "test(t29): единый фазовый хелпер E2ePhase — [PHASE]/[PHASE-TICK], phases.log, пороги 60с/failed-phase (spec §4.1–4.4)"`.

**Выход:** хелпер `E2ePhase.WaitAsync` готов; задачи 3–8 мигрируют на него ожидания.

**Проверка:** сборка проекта без warnings (юнит-тест `E2ePhase` не пишем: хелпер завязан на живой `E2eEnvironment` — docker-контур; функциональная проверка — миграционные задачи и приёмка AC10).

**Связь со spec:** §4.1 (контракт, формат, тики, запрет docker-CLI в тиках), §4.2 (phases.log, UTC), §4.3 (порог 60 с), §4.4 (failed-phase-сбор); AC1, AC2, AC3, AC4.

---

### Задача 2: `RunProcessAsync` — вывод процесса переживает таймаут, параметр `logFile`

**Вход:** задача 1 закоммичена.

**Действие (файлы):**
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eFixture.cs` — перегрузка `RunProcessAsync(file, args, ct, timeout, env)` (строки 62–110) + подключение `logFile` у статических build/publish в `src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs` (`EnsureStaticAsync` — docker build node, строки ~1031–1034; `EnsureJobImageAsync` — build, строки ~1059–1063; `EnsureWalImageAsync` — publish и build, строки ~1091–1100).

**Interfaces (производит):** сигнатура основной перегрузки становится
```csharp
internal static async Task<string> RunProcessAsync(
    string file, string[] args, CancellationToken ct, TimeSpan? timeout,
    IReadOnlyDictionary<string, string>? env = null,
    string? logFile = null)
```
(использует задача 8 для docker build SecondInstance). Остальные вызовы без `logFile` не меняются.

Правки в `E2eFixture.RunProcessAsync`:

1. Чтение потоков — БЕЗ токена бюджета (токен остаётся только на `WaitForExitAsync`; spec §4.5):
```csharp
// Оба потока — ПАРАЛЛЕЛЬНО и БЕЗ токена бюджета (t29 §4.5): при kill по
// бюджету дерево умирает, пайпы закрываются, чтения завершаются сами —
// накопленный вывод доступен (ReadToEndAsync(ct) при отмене терял вывод:
// фейл build > 120 c не оставлял следов, на каком шаге зависло).
var outTask = process.StandardOutput.ReadToEndAsync();
var errTask = process.StandardError.ReadToEndAsync();
try
{
    await process.WaitForExitAsync(timeoutCts.Token);
}
catch (OperationCanceledException) when (!ct.IsCancellationRequested)
{
    // Бюджет исчерпан (не остановка host'а): убиваем дерево, вывод — в
    // диагностику: хвост в исключение, полный — в logFile (если задан).
    try { process.Kill(entireProcessTree: true); } catch { /* процесс мог уже выйти */ }
    var killed = await DrainAsync(outTask, errTask);
    await WriteLogAsync(logFile, killed);
    var tail = OutputTail(killed, lines: 40);
    throw new ApplicationException(
        $"{file} {string.Join(' ', args)} не завершился за {timeout!.Value.TotalSeconds:0} c — убит"
        + (tail.Length == 0 ? "" : $"; хвост вывода:\n{tail}"));
}
```
2. Хелпер `DrainAsync(Task<string> outTask, Task<string> errTask)`: после kill пайпы закрываются операционкой и оба чтения завершаются естественно; страховка от зависшего пайпа — `await Task.WhenAll(outTask, errTask).WaitAsync(TimeSpan.FromSeconds(5))` с `catch (TimeoutException) { return ""; }` (бюджет страховки — новый, относится к сбору диагностики уже убитого процесса, не к выполняемой команде; таймауты команд не меняются).
3. Хелпер `WriteLogAsync(string? logFile, string output)`: при заданном `logFile` пишет полный вывод (успех и таймаут — единый журнал build-вызовов); try/catch — файловая телеметрия не роняет вызов.
4. Существующий приватный `OutputTail(text, lines = 30)` переиспользуется с явным `lines: 40` (~40 строк по spec §4.5).
5. Штатный путь (exit code != 0 / возврат output) не меняется; при внешней отмене (`ct.IsCancellationRequested`) исключение всплывает как раньше — фильтр `when (!ct.IsCancellationRequested)` сохранён.

Подключение `logFile` у статических build/publish (ArtifactsDir на момент статических фаз ещё не существует — окружение создаётся позже; полный вывод — рядом с журналом статических фаз `/tmp/pgw-e2e-static-phase.log`, тот же префикс):

- `EnsureStaticAsync`, docker build node → `logFile: "/tmp/pgw-e2e-static-process-node-e2e.log"`.
- `EnsureJobImageAsync`, docker build job → `logFile: "/tmp/pgw-e2e-static-process-backup-e2e.log"`.
- `EnsureWalImageAsync`, dotnet publish → `logFile: "/tmp/pgw-e2e-static-process-wal-e2e-publish.log"`; docker build wal → `logFile: "/tmp/pgw-e2e-static-process-wal-e2e-build.log"`.

Таймауты всех вызовов НЕ меняются (10 мин — как в коде сейчас).

- [ ] **Шаг 2.1.** Внести правки в `E2eFixture.RunProcessAsync` (+ `DrainAsync`, `WriteLogAsync`, `OutputTail(..., lines: 40)` в таймаут-пути).
- [ ] **Шаг 2.2.** Добавить `logFile` к четырём статическим build/publish вызовам в `E2eEnvironment.cs`.
- [ ] **Шаг 2.3.** Проверка — точечная сборка: `dotnet build src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release` → 0 errors / 0 warnings.
- [ ] **Шаг 2.4.** Коммит: `git commit -m "test(t29): RunProcessAsync — вывод переживает таймаут (хвост ~40 строк в исключение, полный в logFile), logFile у статических build/publish (spec §4.5)"`.

**Выход:** таймаут любого процесса оставляет хвост вывода в исключении и полный вывод в файле; параметр `logFile` доступен build-вызовам.

**Проверка:** сборка; функциональная проверка — задача 8 (build SecondInstance) и приёмка (AC6).

**Связь со spec:** §3.6 (слепая зона build), §4.5; AC6.

---

### Задача 3: AC5 — `E2eScenarios`: замер stop-команды, тики окна, миграция на `E2ePhase`

**Вход:** задачи 1–2 закоммичены (`E2ePhase` доступен).

**Действие (файлы):**
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eScenarios.cs`

**Interfaces (потребляет):** `E2ePhase.WaitAsync(fx, phase, condition, budget, ct, progress)` из задачи 1.

1. **Миграция всех ожиданий класса** (приватная копия `WaitPhaseAsync`, строки 102–112, — удалить; класс t29): все 14 вызовов `WaitPhaseAsync(name, cond, timeout, ct)` → `E2ePhase.WaitAsync(Fx, name, cond, timeout, ct)` — имя фазы и бюджет переносятся ДОСЛОВНО (AC8). Фактические фазы класса (grep по файлу): `ac2-provisioning` (360 с), `ac2-nodes-sql-ready`, `ac3-takeover-started` (120 с), `ac3-takeover-done` (360 с), `ac4-deprovisioning` (180 с), `ac5-failover` (10 с), `ac5-rebuild` (300 с), `ac5-replica-catchup` (120 с), `ac6-evacuation` (120 с), `ac6-quarantine`, `ac7-snapshot-leader`, `ac7-snapshots`, `ac7-single-grower`, `ac7-single-instance` — у неперечисленных бюджет берётся из кода как есть. Формат строки меняется с `elapsed=… ms` на единый `elapsed=<N.N>s, budget=<N>s` — это формат телеметрии, не бюджета.

2. **AC5: отдельный замер stop-команды** (в `AssertFailoverRebuildAsync`, вокруг строки 297). T0 ассерта `≤ 5 с` остаётся ДО вызова `docker stop` — ассерт и его точка отсчёта НЕ меняются (spec §2, AC5/AC8):
```csharp
var sw = Stopwatch.StartNew(); // T0 жёсткого ассерта ≤5с — ДО docker stop (не меняем)
// Длительность самой stop-команды — отдельной строкой (t29 §3.1): замер окна
// failover неотделим от длительности SIGTERM-завершения PG, ассерт при этом
// остаётся от того же T0.
var stopSw = Stopwatch.StartNew();
await Fx.RunDockerAsync(["stop", container], ct);
stopSw.Stop();
output.WriteLine($"[PHASE] ac5-docker-stop: elapsed={stopSw.Elapsed.TotalSeconds:F1}s");
```
3. **AC5: прогресс-тики окна failover** — фаза `ac5-failover` получает progress-делегат (формат spec §3.1: `primary=<node|none>, master-key=<есть/нет>`):
```csharp
var flipped = await E2ePhase.WaitAsync(Fx, "ac5-failover", async () =>
{
    var key = await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/master");
    if (key is not { Value.Length: > 0 })
        return false;
    var primary = await PrimaryNodeAsync(cluster, shard, ct);
    return primary is not null && primary != masterBefore.Node;
}, TimeSpan.FromSeconds(10), ct,
    progress: async () =>
    {
        var key = await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/master");
        var primary = await PrimaryNodeAsync(cluster, shard, ct); // HTTP-проба Patroni — дёшево, тик 5 с
        return $"primary={primary ?? "none"}, master-key={(key is { Value.Length: > 0 } ? "есть" : "нет")}";
    });
```
(бюджет окна 10 с и условие — дословно как сейчас; `sw.Elapsed.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(5), …)` и строка `AC5 … leader failover took N ms` — без изменений).

- [ ] **Шаг 3.1.** Замер `ac5-docker-stop` (п.2) + progress-делегат фазы `ac5-failover` (п.3).
- [ ] **Шаг 3.2.** Миграция всех `WaitPhaseAsync` → `E2ePhase.WaitAsync`, удаление приватной копии (п.1). Пройтись по классу grep'ом: `grep -n "WaitPhaseAsync" src/tests/PgWorker.IntegrationTests/E2e/E2eScenarios.cs` — пусто.
- [ ] **Шаг 3.3.** Проверка — точечная сборка: 0 errors / 0 warnings.
- [ ] **Шаг 3.4.** Проверка AC8 по классу: `git diff -- src/tests/PgWorker.IntegrationTests/E2e/E2eScenarios.cs | grep -E '^[+-].*(FromSeconds|FromMinutes|Elapsed.Should)'` — только переносы, числа совпадают парами; ассерт ≤5 с на месте.
- [ ] **Шаг 3.5.** Коммит: `git commit -m "test(t29): AC5 — замер docker stop отдельной строкой, тики окна failover, миграция AC-фаз на E2ePhase (spec §3.1)"`.

**Выход:** AC-фазы класса печатают единый формат; AC5-разложение (stop отдельно, failover отдельно) в каждом прогоне.

**Проверка:** сборка + grep-гейт; функционально — AC5/AC10 в приёмке.

**Связь со spec:** §3.1 (слепые зоны 1–3), §4.1; AC2, AC5.

---

### Задача 4: `E2eMoveScenarios` — все ожидания через `E2ePhase`, catch→`MarkFailed`

**Вход:** задача 1 закоммичена.

**Действие (файлы):**
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eMoveScenarios.cs`

**Interfaces (потребляет):** `E2ePhase.WaitAsync` (задача 1).

1. **Миграция 12 ожиданий** `E2eFixture.WaitForAsync` → `E2ePhase.WaitAsync(Fx, …)` с короткими именами фаз (бюджеты дословно, AC8):

| Место (метод) | Имя фазы | Бюджет |
|---|---|---|
| `E1_Provisioning` | `move-provisioning` | 360 с |
| `E1` хелпер `SeedBucketAsync` | `move-seed-bucket` | 60 с |
| `E1` хелпер `EnableSyncModeAsync` | `move-sync-mode` | 60 с |
| `E2_MoveUnderLoad` routing | `move-routing-flip` | 120 с |
| `E2` заявка → finalize | `move-finalize-op` | 15 с |
| `E2` FROZEN denied | `move-frozen-denied` | 20 с |
| `E2` запись после flip | `move-frozen-resumed` | 20 с |
| `E2` sub_rb-догон | `move-sub-rb-catchup` | 15 с |
| `E3_AutoFinalize` | `move-auto-finalize` | 120 с |
| `E4_AbortTakeover` SYNCING | `move-abort-syncing` | 60 с |
| `E4` takeover-доводка | `move-abort-done` | 180 с |
| `E5_Deprovisioning` | `move-deprovisioning` | 180 с |

2. **Прогресс-снапшоты** (spec §3.2/§3.3):
   - `move-provisioning` — приватный прогресс-делегат: state-ключи нод + dsn-ключи из etcd:
     ```csharp
     progress: async () =>
     {
         var parts = new List<string>();
         foreach (var shard in new[] { "shard1", "shard2" })
         {
             var dsn = await GetOrNullAsync($"/clusters/{Cluster}/shards/{shard}/dsn") is null ? "нет" : "есть";
             var a = (await GetOrNullAsync($"/clusters/{Cluster}/shards/{shard}/nodes/{shard}a/state"))?.Value ?? "-";
             var b = (await GetOrNullAsync($"/clusters/{Cluster}/shards/{shard}/nodes/{shard}b/state"))?.Value ?? "-";
             parts.Add($"{shard}: {a}/{b}, dsn={dsn}");
         }
         return string.Join("; ", parts);
     }
     ```
   - `move-sub-rb-catchup` — counts обеих сторон в каждый тик (лёгкий SQL-скаляр малой таблицы):
     ```csharp
     progress: async () =>
     {
         var c1 = await SqlScalarAsync(shard1.Dsn, "SELECT count(*) FROM bucket_0.items", ct);
         var c2 = await SqlScalarAsync(shard2.Dsn, "SELECT count(*) FROM bucket_0.items", ct);
         return $"src={c1}, dst={c2}";
     }
     ```
     (снапшот может кидать `NpgsqlException` на рестартующих нодах — глотается хелпером, тик печатает `<progress error: …>`).
   - Docker-картина (число контейнеров префикса) в тики НЕ добавляется (запрет docker-CLI в тиках, spec §4.1) — её даёт failed/slow-phase-сбор `CollectDiagnosticsAsync` (docker ps всех своих).
3. **catch→MarkFailed** — тело `Move_Lifecycle_Chain` обернуть:
```csharp
Fx = fx;
try
{
    await E1_Provisioning();
    output.WriteLine("=== E1 done, calling E2 ===");
    // … E2–E5 дословно …
}
catch
{
    // docs/e2e-launch.md §3: упавший сценарий — окружение ОСТАНОВИТЬ, не
    // удалить (телеметрия в артефактах + живые объекты для разбора).
    Fx.MarkFailed();
    throw;
}
```

- [ ] **Шаг 4.1.** Миграция 12 ожиданий (таблица п.1) с прогрессами `move-provisioning`/`move-sub-rb-catchup` (п.2).
- [ ] **Шаг 4.2.** Обёртка catch→`MarkFailed` (п.3).
- [ ] **Шаг 4.3.** Проверка — точечная сборка: 0 errors / 0 warnings.
- [ ] **Шаг 4.4.** Проверка AC8: `git diff -- src/tests/PgWorker.IntegrationTests/E2e/E2eMoveScenarios.cs | grep -E '^[+-].*(FromSeconds|FromMinutes)'` — только переносы, числа парами совпадают; эталон сверки — таблица п.1 этой задачи: 12 значений (360, 60, 60, 120, 15, 20, 20, 15, 120, 60, 180, 180), включая ДВА 20-с окна `move-frozen-denied` / `move-frozen-resumed`.
- [ ] **Шаг 4.5.** Коммит: `git commit -m "test(t29): Move — все ожидания через E2ePhase (sub_rb counts, provisioning state-ключи в тиках) + catch→MarkFailed (spec §3.2–3.3, §4.1, §4.6)"`.

**Выход:** класс печатает `[PHASE]`/`[PHASE-TICK]` по всем фазам; упавший сценарий останавливает, а не удаляет окружение.

**Проверка:** сборка + grep-гейт AC8; функционально — AC10.

**Связь со spec:** §3.2, §3.3, §4.1, §4.6; AC1–AC4, AC8.

---

### Задача 5: `E2eWalStreamPromoteScenarios` — фазы через `E2ePhase`, дамп в `ArtifactsDir`

**Вход:** задача 1 закоммичена.

**Действие (файлы):**
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eWalStreamPromoteScenarios.cs`

**Interfaces (потребляет):** `E2ePhase.WaitAsync` (задача 1).

1. **Миграция 6 ожиданий** (все длинные фазы класса — иначе AC3 «любая фаза > 60 с» не покрывается):

| Место | Имя фазы | Бюджет | Прогресс |
|---|---|---|---|
| `StartWalScenarioAsync` provisioning | `wal-provisioning` | 360 с | — |
| `MasterPgAsync` primary-резолв | `wal-master-resolved` | 120 с | — |
| Fact `bothAgents` | `wal-agents-up` | 300 с | — (condition сам зовёт docker ps — существующее поведение, в тики не дублируем) |
| `PrimaryTimelineAsync` TLI≥2 | `wal-tli-ready` | 120 с | `tli=<текущий>` |
| Fact `history` | `wal-history-present` | 300 с | листинг S3 |
| Fact `glueOk` | `wal-chain-glued` | 300 с | листинг S3 |

2. **Прогресс листинга S3** (spec §3.4: размер, max-TLI сегментов, наличие `.history`; S3-листинг в тиках — не чаще 1 раза в 5 с — интервал тиков):
```csharp
progress: async () =>
{
    var listed = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
    if (!listed.IsSuccess || listed.Value.Count == 0)
        return "listing=пусто";
    var names = listed.Value.Select(o => o.Name).ToList();
    var maxTli = names.Where(n => !n.EndsWith(".history", StringComparison.Ordinal))
        .Select(n => Convert.ToUInt32(n[..8], 16)).DefaultIfEmpty(0).Max();
    return $"objects={names.Count}, maxTli=0x{maxTli:x}, history={(names.Any(n => n.EndsWith(".history", StringComparison.Ordinal)) ? "есть" : "нет")}";
}
```
(делегат одинаков для `wal-history-present` и `wal-chain-glued`; живость агентов — в failed/slow-phase-сбор `CollectDiagnosticsAsync`, не в тиках.)
3. **Дамп в ArtifactsDir** (`WalScenarioDiagDumpAsync`, строка 261): `/tmp/pgw-diag-{cluster}.txt` → `Path.Combine(Fx.ArtifactsDir, $"wal-diag-{cluster}.txt")`; строку `[DIAG] дамп: …` вывести с новым путём (внутри метода уже try/catch «лучшие усилия» — сохранить).

- [ ] **Шаг 5.1.** Миграция 6 ожиданий (таблица п.1) + прогресс-делегат листинга (п.2).
- [ ] **Шаг 5.2.** Дамп → `ArtifactsDir/wal-diag-<cluster>.txt` (п.3).
- [ ] **Шаг 5.3.** Проверка — точечная сборка: 0 errors / 0 warnings.
- [ ] **Шаг 5.4.** Проверка AC8: `git diff -- src/tests/PgWorker.IntegrationTests/E2e/E2eWalStreamPromoteScenarios.cs | grep -E '^[+-].*(FromSeconds|FromMinutes)'` — переносы без изменения чисел (360/120/300/300/300/120).
- [ ] **Шаг 5.5.** Коммит: `git commit -m "test(t29): WalStream-Promote — фазы через E2ePhase с прогрессом листинга S3, дамп в ArtifactsDir (spec §3.4, §4.6)"`.

**Выход:** фазы history/склейки (300 с) дают отчёт по порогу 60 с и динамику листинга; дамп живёт в каталоге телеметрии прогона.

**Проверка:** сборка + grep-гейт; функционально — AC10.

**Связь со spec:** §3.4, §4.1, §4.6; AC1–AC3.

---

### Задача 6: `E2eHaEtcdScenarios` — по-пробная телеметрия, контекстный ассерт вместо NRE

**Вход:** задача 1 закоммичена.

**Действие (файлы):**
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eHaEtcdScenarios.cs`

**Interfaces (потребляет):** `E2ePhase.WaitAsync` (задача 1).

1. **По-пробная телеметрия** (цикл проб 5×2 с, строки 52–61; класс без `ITestOutputHelper` — печать через `Console.WriteLine`; пробы остаются жёсткими — НЕ ретрай, spec §2/§3.5):
```csharp
for (var probe = 0; probe < 5; probe++)
{
    var healthz = await Fx.HealthzOkAsync(h1);
    var apiKeys = (await RangeAsync("/pgworker/api/")).Count;
    var master = await GetOrNullAsync($"/clusters/{cluster}/shards/shard1/master");
    Console.WriteLine(
        $"[PROBE] ha-etcd {probe + 1}/5: healthz={healthz}, api={apiKeys}, " +
        $"master={(master is { Value.Length: > 0 } ? $"len={master.Value.Length}" : (master is null ? "absent" : "empty"))}");
    healthz.Should().BeTrue($"healthz воркера жив на всём интервале отказа (проба {probe + 1}/5)");
    apiKeys.Should().BeGreaterThan(0, "keepalive-ключ API публикуется (надзор жив)");
    master.Should().NotBeNull($"master-ключ жив (проба {probe + 1}/5); фактически: отсутствует");
    master!.Value.Should().NotBeNullOrEmpty(
        $"master-ключ жив (проба {probe + 1}/5); фактически: len={master.Value.Length}");
    await Task.Delay(TimeSpan.FromSeconds(2), ct);
}
```
(семантика ассертов сохранена: `NotBeEmpty` ≡ `Count > 0` / `Value.Length > 0`; пустой ключ даёт внятный фейл с длиной, а не NRE из `!.Value`).
2. **Миграция 3 ожиданий** (AC3 — все три фазы длиннее 60 с, бюджеты дословно): `haetcd-provisioning` 360 с; `haetcd-shard3-registered` 360 с + прогресс state-ключей shard3 и work-ключа:
```csharp
progress: async () =>
{
    var a = (await GetOrNullAsync($"/clusters/{cluster}/shards/shard3/nodes/shard3a/state"))?.Value ?? "-";
    var b = (await GetOrNullAsync($"/clusters/{cluster}/shards/shard3/nodes/shard3b/state"))?.Value ?? "-";
    var dsn = await GetOrNullAsync($"/clusters/{cluster}/shards/shard3/dsn") is null ? "нет" : "есть";
    var work = (await GetOrNullAsync($"/pgworker/work/{cluster}"))?.Value ?? "нет";
    return $"shard3: {a}/{b}, dsn={dsn}, work={Trunc(work)}";
}
```
(`Trunc` — локальная обрезка длинного work-JSON до ~120 симв., чтобы тик читался; `haetcd-node-back` 60 с — прогресс не обязателен (docker-etcdctl в condition остаётся, в тики не идёт)).

- [ ] **Шаг 6.1.** По-пробная телеметрия + контекстные ассерты (п.1).
- [ ] **Шаг 6.2.** Миграция 3 ожиданий + прогресс shard3 (п.2).
- [ ] **Шаг 6.3.** Проверка — точечная сборка: 0 errors / 0 warnings.
- [ ] **Шаг 6.4.** Проверка AC8: `git diff -- src/tests/PgWorker.IntegrationTests/E2e/E2eHaEtcdScenarios.cs | grep -E '^[+-].*(FromSeconds|Task\.Delay)'` — пробы остались 5×2 с (`FromSeconds(2)`, цикл `< 5`), бюджеты 360/360/60 без изменений.
- [ ] **Шаг 6.5.** Коммит: `git commit -m "test(t29): HaEtcd — по-пробная телеметрия master/api/healthz, пустой master-ключ → контекстный ассерт вместо NRE, фазы через E2ePhase (spec §3.5)"`.

**Выход:** каждая проба видна в журнале; провал инварианта — фейл с контекстом (номер пробы, длина ключа), не NRE.

**Проверка:** сборка + grep-гейт; функционально — AC10.

**Связь со spec:** §3.5, §4.1; AC1, AC3.

---

### Задача 7: `E2eWorkerCertScenarios` — `WaitForDiscoveryAsync` через `E2ePhase`

**Вход:** задача 1 закоммичена.

**Действие (файлы):**
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eWorkerCertScenarios.cs`

**Interfaces (потребляет):** `E2ePhase.WaitAsync` (задача 1).

1. `WaitForDiscoveryAsync(fx, ct)` → добавить параметр процесса для прогресса «процесс жив» и мигрировать ожидание (бюджет 30 с дословно):
```csharp
private static async Task<(string Url, string Thumbprint)> WaitForDiscoveryAsync(
    E2eEnvironment fx, HostInstance host, CancellationToken ct)
{
    // … (поиск ключей — дословно как сейчас) …
    var found = await E2ePhase.WaitAsync(fx, "cert-discovery", async () =>
    {
        // … тело условия дословно …
    }, TimeSpan.FromSeconds(30), ct,
        progress: async () =>
            $"alive={!host.Process.HasExited}, api-keys={await CountApiKeysAsync(fx, ct)}");
    found.Should().BeTrue("дискавери-ключ с cert_thumbprint обязан появиться");
    return (url!, actualThumb!);
}
```
где `CountApiKeysAsync` — лёгкий range-подсчёт `/pgworker/api/` (etcd-чтение, без docker). Вызовы в сценарии 1 обновить на передачу хоста: `p1`, `p2`, `p3` соответственно.
2. Ожидание graceful-выхода (`exited`, 30 с) — тоже фаза класса:
```csharp
var exited = await E2ePhase.WaitAsync(fx, "cert-restart-exit",
    () => Task.FromResult(p1.Process.HasExited), TimeSpan.FromSeconds(30), ct);
```
(условие и ассерт — без изменений).

- [ ] **Шаг 7.1.** Миграция `WaitForDiscoveryAsync` (+host, +прогресс) и `exited` (пп.1–2).
- [ ] **Шаг 7.2.** Проверка — точечная сборка: 0 errors / 0 warnings.
- [ ] **Шаг 7.3.** Проверка AC8: diff по файлу — `FromSeconds(30)` в обеих точках без изменений.
- [ ] **Шаг 7.4.** Коммит: `git commit -m "test(t29): WorkerCert — discovery-ожидание через E2ePhase с прогрессом (процесс жив, api-ключи) (spec §3.7)"`.

**Выход:** провал 30-с окна оставляет `[PHASE] cert-discovery` с budget, тики живости процесса и failed-phase-сбор.

**Проверка:** сборка; функционально — AC10.

**Связь со spec:** §3.7, §4.1, §4.4; AC1–AC4.

---

### Задача 8: `E2eSecondInstanceScenarios` — build с меткой старта и logFile, ожидания через `E2ePhase`

**Вход:** задачи 1–2 закоммичены (`logFile` доступен).

**Действие (файлы):**
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eSecondInstanceScenarios.cs`

**Interfaces (потребляет):** `E2ePhase.WaitAsync` (задача 1), `RunProcessAsync(..., logFile)` (задача 2).

1. **Build: метка ДО старта + полный лог при таймауте** (строки 56–58; бюджет — прежний дефолт docker-CLI 120 с, передаётся явно, число не меняется):
```csharp
// [PHASE]-метка ДО старта: тихая фаза без метки старта нарушает канон
// статических фаз (spec §3.6); полный вывод при kill по бюджету — в
// ArtifactsDir (RunProcessAsync logFile, t29 §4.5).
Console.Error.WriteLine($"[PHASE] build {image}: старт (контекст корня репо, бюджет 120 c)…");
var buildSw = Stopwatch.StartNew();
await E2eFixture.RunProcessAsync("docker",
    ["build", "-q", "-f", $"{root}/docker/PgWorker.Dockerfile", "-t", image, root],
    ct, timeout: TimeSpan.FromMinutes(2),
    logFile: Path.Combine(Fx.ArtifactsDir, $"process-build-{tag}.log"));
Console.Error.WriteLine($"[PHASE] build {image}: {buildSw.Elapsed.TotalSeconds:F0} c");
```
2. **Миграция 4 ожиданий** (бюджеты дословно): `si2-instances-up` 100 с + прогресс `api={n}, instances={m}` (два range-подсчёта etcd); `si2-provisioning` 360 с; `si2-a3-started` 120 с; `si2-takeover-done` 360 с + прогресс state-ключей shard3 (по образцу задачи 6).

- [ ] **Шаг 8.1.** Build: метка старта + `logFile` + явный 120-с бюджет (п.1).
- [ ] **Шаг 8.2.** Миграция 4 ожиданий с прогрессами (п.2).
- [ ] **Шаг 8.3.** Проверка — точечная сборка: 0 errors / 0 warnings.
- [ ] **Шаг 8.4.** Проверка AC8: diff — бюджеты 100/360/120/360 и `FromMinutes(2)` без изменений.
- [ ] **Шаг 8.5.** Коммит: `git commit -m "test(t29): SecondInstance — build с [PHASE]-стартом и logFile, ожидания через E2ePhase (spec §3.6, §4.5)"`.

**Выход:** таймаут build оставляет хвост в исключении и полный вывод в `process-build-<tag>.log`; фазы класса в едином формате.

**Проверка:** сборка; функционально — AC6/AC10 (если build уложится — полный лог всё равно пишется, метки старта/финиша в журнале).

**Связь со spec:** §3.6, §4.1, §4.5; AC2, AC6.

---

### Задача 9: Не-E2E — транзиенты ретрая etcd и телеметрия попыток

**Вход:** задачи 1–8 закоммичены.

**Действие (файлы):**
- Modify: `src/tests/PgWorker.IntegrationTests/Backups/OwnEtcd.cs` (`WaitReadyAsync`, строки 75–98)
- Modify: `src/tests/PgWorker.IntegrationTests/Etcd/EtcdFixture.cs` (`WaitReadyAsync`, строки 76–99)
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs` (probe-цикл готовности etcd-контура в `StartOnceAsync`, строки 358–382)

**Interfaces (потребляет):** — (правки самостоятельны).

1. **`OwnEtcd.WaitReadyAsync` / `EtcdFixture.WaitReadyAsync`** — одинаковая правка (бюджет 30×1 с НЕ меняется; паттерн — как `StartHostOnPortAsync`-readiness, где отмена ловится ТОЛЬКО при `!ct.IsCancellationRequested`):
```csharp
private async Task WaitReadyAsync(CancellationToken ct)
{
    using var probe = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
    var attempts = 0;
    var lastLatencyMs = 0.0;
    for (var i = 0; i < 30; i++)
    {
        attempts++;
        var sw = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            using var response = await probe.PostAsync(
                Endpoint + "/v3/maintenance/status",
                new StringContent("{}", Encoding.UTF8, "application/json"),
                ct);
            if (response.IsSuccessStatusCode)
                return;
        }
        catch (HttpRequestException)
        {
            // etcd ещё поднимается — повтор пробы (не сон: блокирующий ретрай)
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            // Клиентский таймаут пробы (3 c) при холодном/медленном старте
            // контейнера — «ещё не готов», как в StartHostOnPortAsync-readiness;
            // отмена теста продолжает всплывать. Бюджет 30×1 c неизменен (t29 §4.7).
        }

        lastLatencyMs = sw.Elapsed.TotalMilliseconds;
        await Task.Delay(1000, ct);
    }

    throw new InvalidOperationException(
        $"etcd {ContainerName} не поднялся за 30 c (попыток {attempts}, латентность последней пробы {lastLatencyMs:0} мс)");
}
```
(`TaskCanceledException` наследует `OperationCanceledException` — одного catch достаточно; в `EtcdFixture` сообщение — про `Endpoint`, как сейчас.)
2. **`E2eEnvironment.StartOnceAsync` probe-цикл** — поведение catch (глотает всё) НЕ меняется; добавляется телеметрия попыток в сообщение фейла:
```csharp
var probeAttempts = 0;
var lastProbeLatencyMs = 0.0;
var ready = await E2eFixture.WaitForAsync(async () =>
{
    probeAttempts++;
    var sw = System.Diagnostics.Stopwatch.StartNew();
    foreach (var url in endpoints)
    {
        try
        {
            // … пробы дословно …
        }
        catch (Exception)
        {
            lastProbeLatencyMs = sw.Elapsed.TotalMilliseconds;
            return false; // ещё поднимается / кворум не собран
        }
    }
    lastProbeLatencyMs = sw.Elapsed.TotalMilliseconds;
    return true;
}, TimeSpan.FromSeconds(100), ct);
ready.Should().BeTrue($"etcd-контур ({(haEtcd ? 3 : 1)} узла) обязан собраться за 100 c "
    + $"(проб сделано {probeAttempts}, латентность последней {lastProbeLatencyMs:0} мс)");
```

- [ ] **Шаг 9.1.** Правка `OwnEtcd.WaitReadyAsync` (п.1, вариант с `ContainerName`).
- [ ] **Шаг 9.2.** Правка `EtcdFixture.WaitReadyAsync` (п.1, вариант с `Endpoint`).
- [ ] **Шаг 9.3.** Телеметрия попыток в probe-цикле `StartOnceAsync` (п.2).
- [ ] **Шаг 9.4.** Проверка — точечная сборка: 0 errors / 0 warnings.
- [ ] **Шаг 9.5.** Проверка AC8: diff по трём файлам — `for (var i = 0; i < 30; i++)`, `Task.Delay(1000, ct)`, `Timeout = TimeSpan.FromSeconds(3)`, `FromSeconds(100)` без изменений.
- [ ] **Шаг 9.6.** Коммит: `git commit -m "test(t29): не-E2E ретрай готовности etcd — клиентский таймаут пробы = «ещё не готов», телеметрия попыток/латентности в фейле (OwnEtcd, EtcdFixture, probe-цикл E2eEnvironment) (spec §4.7)"`.

**Выход:** транзиентный 3-с таймаут пробы не роняет тест из ретрай-цикла; реальный фейл содержит попытки и латентность.

**Проверка:** сборка; функционально — контрольный не-E2E прогон в задаче 11 п.3 (AC7).

**Связь со spec:** §3.8, §4.7; AC7.

---

### Задача 10: Документация — `docs/e2e-launch.md`

**Вход:** задачи 1–9 закоммичены.

**Действие (файлы):**
- Modify: `docs/e2e-launch.md`

**Interfaces (потребляет):** — (документация канонизирует сделанное).

1. **§2 «Отчёт по любой фазе дольше 60 с»** — дополнить (текущее состояние, без истории):
   - фазовые ожидания t29-классов и все новые сценарии — через единый `E2ePhase.WaitAsync`: формат `[PHASE] <phase>: ok=…, elapsed=<N.N>s, budget=<N>s[, progress=…]`, прогресс-тики `[PHASE-TICK] <phase> t=<N.N>s: <снимок>` каждые 5 с (короткие окна 10–15 с дают 2–3 строки динамики);
   - прогресс-делегаты — только дешёвые (etcd-чтения, лёгкие SQL-скаляры, S3-листинг); docker-CLI в тиках запрещён;
   - провал окна (ok=False) → немедленный сбор `failed-phase-*` до возврата сценарию (дополняет порог 60 с: флейк-окна 10–30 с правилом 60 с не покрывались);
   - каждая строка дублируется в `phases.log` (UTC) — онлайн-наблюдение и «последний успешный шаг».
2. **Таблица «Где что лежит после прогона»** — новые строки: `phases.log` (фазовый журнал, UTC), `process-<slug>.log` (полный вывод процесса при таймауте/для build-вызовов), `/tmp/pgw-e2e-static-process-*.log` (полный вывод статических build/publish — рядом с журналом статических фаз, вне каталогов прогонов: статические ассеты процесса живут до окружений), `wal-diag-*.txt` (дампы Wal-сценариев), `*-failed-phase-*.txt` (сбор в момент провала фазы).
3. **§1** — пункт про вывод процессов: таймаут `RunProcessAsync` оставляет хвост вывода в исключении, полный — в `process-*.log` (build-вызовы подключены).
4. Абзац «Состояние реализации» — обновить: канон фазовых ожиданий — `E2ePhase` (t29-классы мигрированы; копии `WaitPhaseAsync` в restore/supervisor-классах — легальный прецедент до отдельной миграции); убрать оговорку «фазовые ожидания пока обёрнуты в restore-сценариях». Исторических пассажей и атрибуций не добавлять (правило AGENTS.md).

- [ ] **Шаг 10.1.** Внести правки (пп.1–4).
- [ ] **Шаг 10.2.** Проверка: файл перечитан целиком — формат строк в доке совпадает с реализацией задачи 1 (включая `budget=` и `[PHASE-TICK]`), таблица путей соответствует фактическим именам файлов (`phases.log`, `process-<slug>.log`, `/tmp/pgw-e2e-static-process-*.log`, `wal-diag-<cluster>.txt`, `*-failed-phase-*.txt`).
- [ ] **Шаг 10.3.** Коммит: `git commit -m "docs(t29): e2e-launch — канон E2ePhase (единый формат с budget, PHASE-TICK, failed-phase-сбор), phases.log/process-логи в таблице артефактов (spec §6)"`.

**Выход:** действующие правила телеметрии описывают новый контур; канон для новых сценариев — `E2ePhase`.

**Проверка:** сверка с реализацией (шаг 10.2).

**Связь со spec:** §6; AC9.

---

### Задача 11: Приёмка — сборка, AC8-гейт бюджетов, контрольные прогоны (AC7, AC10)

**Вход:** задачи 1–10 закоммичены.

**Действие (файлы):** правок кода нет (кроме исправлений, если гейты найдут нарушение — тогда фикс и повтор гейта).

**Interfaces (потребляет):** всё предыдущее.

1. **Полная сборка решения**:
   ```bash
   dotnet build src/PgWorker.slnx -c Release
   ```
   Критерий: 0 Error(s), 0 Warning(s).
2. **AC8 — гейт неизменности бюджетов** (по всему диффу ветки):
   ```bash
   git diff main..HEAD -- src/tests/PgWorker.IntegrationTests | grep -E '^[+-].*(FromSeconds|FromMinutes|FromMilliseconds|Task\.Delay|CancelAfter)' 
   ```
   Критерий (двухчастный):
   - все СУЩЕСТВУЮЩИЕ бюджеты — перенос парами «−/+» без изменения числа. Контрольные значения: HaEtcd 5×2 с, sub_rb 15 с, AC5 10 с и ассерт 5 с, WorkerCert 30 с, build 120 с, WaitReady 30×1 с, probe 3 с, etcd-контур 100 с; полный эталон — таблицы фаз задач 3–8.
   - НОВЫЕ числа (строки «+» без пары «−») допустимы ТОЛЬКО из белого списка — телеметрия/диагностика, не бюджеты ожиданий:
     - `E2ePhase` (задача 1): SlowPhaseThreshold `FromSeconds(60)`, TickInterval `FromSeconds(5)`, PollInterval `FromMilliseconds(500)`;
     - `DrainAsync` (задача 2): страховка дренажа убитого процесса `WaitAsync(TimeSpan.FromSeconds(5))`;
     - docker build SecondInstance (задача 8): явный `FromMinutes(2)` — перенос дефолтного 120-с бюджета в вызов с `logFile` (сама перегрузка с дефолтом `FromMinutes(2)` остаётся в `E2eFixture` — число то же).
   Всё, что не является парным переносом существующего бюджета и не входит в белый список, — нарушение AC8: фикс и повтор гейта. Особо: ассерты (`Elapsed.Should`, `Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(5)…)`) — нетронуты.
3. **AC7 — контрольный прогон не-E2E серии** (потребители `OwnEtcd`/`EtcdFixture`; последовательны `NonE2eCollection`):
   ```bash
   DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~RestoreDrillProcessTests|FullyQualifiedName~EtcdContractTests|FullyQualifiedName~EtcdCoordinationTests"
   ```
   Критерий: зелёный (транзиентный таймаут пробы не роняет ретрай-цикл). Дождаться финальной строки; после неё — страховочная проверка чистоты (п.6).
4. **AC10 — прогон E2E t29-фильтром на каноническом N=5** (`xunit.runner.json` уже 5; на нормальном рабочем фоне — никаких условий и ожиданий):
   ```bash
   DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~E2eScenarios.Acceptance|FullyQualifiedName~E2eMoveScenarios|FullyQualifiedName~E2eWalStreamPromoteScenarios|FullyQualifiedName~E2eHaEtcdScenarios|FullyQualifiedName~E2eWorkerCertScenarios|FullyQualifiedName~E2eSecondInstanceScenarios"
   ```
   Критерии (зелёность НЕ обязательна — флейки остаются до отдельной задачи):
   - каждая фаза каждого класса оставила `[PHASE] … budget=…`-строку в журнале и в `phases.log`;
   - `phases.log` существует в каждом `/tmp/pgw-e2e-artifacts-<guid>/` прогона:
     ```bash
     ls /tmp/pgw-e2e-artifacts-*/phases.log
     grep -hc '\[PHASE\]' /tmp/pgw-e2e-artifacts-*/phases.log
     ```
   - упавшие сценарии: разбор ПО ТЕЛЕМЕТРИИ без перезапуска (журнал, `phases.log`, `failed-phase-*`, docker-логи артефактов, `host-*.log`); формулировка причин — в отчёт задачи; перезапуск — только по согласию пользователя (spec §2, docs/e2e-launch.md §4).
5. **AC5/AC6 по артефактам прогона п.4**: в журнале Acceptance видны отдельные строки `ac5-docker-stop` и `ac5-failover` (elapsed последнего ≤ 5 с — ассерт); при таймауте build (если случился) — хвост в исключении + полный вывод в `process-build-<tag>.log`. Если build уложился — AC6 подтверждается код-ревью пути (задача 2) + фактом `process-build-*.log` в артефактах (полный вывод пишется и при успехе).
6. **Зачистка между сериями и после** (AGENTS.md): после финальной строки КАЖДОЙ серии (п.3 → п.4; и любых повторных):
   ```bash
   docker ps -a --format '{{.Names}}' | grep -c 'pgw-'
   docker network ls --format '{{.Name}}' | grep -cE 'pgw-|kfw-net'
   ```
   Оба счётчика 0 — можно запускать следующую серию. Ненулевые (упавшие сценарии оставили контейнеры намеренно) — разбор по логам, затем ручная зачистка по `README-cleanup.txt` из каталога артефактов упавшего прогона, повторная проверка до нуля. Новую серию поверх незачищенной НЕ запускать.
7. **Итог приёмки** — краткий отчёт в `docs/superpowers/2026-10-07-flaky-tests-telemetry/journal.md` (журнал задачи; больше никакие `docs/**` не правятся): результаты пп.1–6, список фаз с budget по артефактам, причины упавших (если были). Финальный коммит (если правки не потребовались — отчёт без коммита):
   ```bash
   git add -A && git commit -m "test(t29): приёмка — сборка 0/0, AC8-гейт бюджетов, не-E2E контроль зелёный, E2E t29-прогон N=5: телеметрия полная (spec §8)"
   ```

**Выход:** все AC проверены; ветка готова к code-review и мерж-гейту dev-flow.

**Проверка:** сами гейты пп.1–6.

**Связь со spec:** §7 фаза 5, §8 целиком (AC1–AC10).

---

### Задача 12: `E2eWalStreamMasterDownScenarios` — все ожидания и repair-цикл через `E2ePhase`, дамп в `ArtifactsDir`

**Вход (предусловие):** задачи 1–11 исполнены (в ветке есть `E2ePhase`); база диффа — `ce0bf7c`.

**Действие (файлы):**
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eWalStreamMasterDownScenarios.cs`

**Interfaces (потребляет):** `E2ePhase.WaitAsync(fx, phase, condition, budget, ct, progress)` — уже в коде (задача 1).

1. **Миграция всех 7 `E2eFixture.WaitForAsync`** (бюджеты ДОСЛОВНО, AC8; у `MasterPgAsync` фаза с одним именем — метод вызывается из факта дважды, до/после `docker stop`; контексты различимы по соседним строкам `[PHASE] wal-ac4: docker stop …` / `новый primary …`):

| Место | Имя фазы | Бюджет | Прогресс |
|---|---|---|---|
| `StartWalScenarioAsync` provisioning | `wal-ac4-provisioning` | 360 с | — |
| `MasterPgAsync` primary-резолв (вызов ×2) | `wal-ac4-master-resolved` | 120 с | — |
| Fact `bothAgents` | `wal-ac4-agents-up` | 300 с | — (condition сам зовёт docker ps — существующее поведение, в тики не дублируем) |
| repair-цикл (см. п.2) | `wal-ac4-repair` | 600 с (окна 30 с) | `unreachable=…, окно N` |
| Fact `grew` (delivery-гейт) | `wal-ac4-delivery-grew` | 300 с | листинг S3 (п.3) |
| Fact `keySeen` (wal-ключ) | `wal-ac4-key-seen` | 30 с | — |
| Fact `newAgent` | `wal-ac4-new-agent` | 300 с | — (docker ps в condition остаётся) |
| `PrimaryTimelineAsync` (хелпер; в факте НЕ вызывается — мигрируется для единообразия класса) | `wal-ac4-tli-ready` | 120 с | `tli=<текущий>` |

2. **Repair-цикл** (бюджет 600 с и 30-с окна — ДОСЛОВНО, spec §2/§3.9; окно живёт ВНУТРИ condition — проверка факта раз в 30 с, как сейчас; статус/номер окна — в замыкании, прогресс форматирует их БЕЗ новых чтений; тик `E2ePhase` печатается по завершении condition — фактическая частота ≈ строка на окно). Ассерта на результат цикла в коде НЕТ — итог используется только в строке телеметрии (не добавляем новый ассерт):
```csharp
TestContext.Current.TestOutputHelper?.WriteLine(
    "[PHASE] wal-ac4: ожидание снятия unreachable (repair-контур надзора)");
var stillUnreachable = true;
var window = 0;
var repaired = await E2ePhase.WaitAsync(Fx, "wal-ac4-repair", async () =>
{
    var workKv = await GetOrNullAsync($"/pgworker/work/{cluster}");
    stillUnreachable = workKv?.Value?.Contains("unreachable") == true;
    if (!stillUnreachable)
        return true;
    window++;
    await Task.Delay(TimeSpan.FromSeconds(30), ct); // 30-с окно проверки — дословно, полл не учащается
    return false;
}, TimeSpan.FromSeconds(600), ct,
    progress: () => Task.FromResult($"unreachable={(stillUnreachable ? "да" : "нет")}, окно {window}"));
TestContext.Current.TestOutputHelper?.WriteLine(
    $"[PHASE] wal-ac4: repair-фаза завершена/снята (repaired={repaired}) — замеры ключа");
```
3. **Прогресс delivery-гейта** (`wal-ac4-delivery-grew`; динамика листинга wal/-префикса, spec §3.9 п.2; листинг локального MinIO — S3 в тиках не чаще 5 с — интервал тиков):
```csharp
progress: async () =>
{
    var listed = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
    return listed.IsSuccess
        ? $"objects={listed.Value.Count} (before={beforeList.Value.Count})"
        : "listing=ошибка";
}
```
Живость агентов-реплик — в failed/slow-phase-сбор `CollectDiagnosticsAsync` (docker ps в тиках ЗАПРЕЩЁН).
4. **Дамп в `ArtifactsDir`** (`WalScenarioDiagDumpAsync`, копия дампа Promote — тот же дефект §4.6): `/tmp/pgw-diag-{cluster}.txt` → `Path.Combine(Fx.ArtifactsDir, $"wal-diag-{cluster}.txt")`; строку `[DIAG] дамп: …` вывести с новым путём (try/catch «лучшие усилия» внутри метода сохранить). `MarkFailed` в catch — уже есть, не трогаем.

- [ ] **Шаг 12.1.** Миграция 7 ожиданий (таблица п.1) + repair-цикл через `E2ePhase` (п.2) + прогресс delivery-гейта (п.3).
- [ ] **Шаг 12.2.** Дамп → `ArtifactsDir/wal-diag-<cluster>.txt` (п.4).
- [ ] **Шаг 12.3.** Проверка — точечная сборка: `dotnet build src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release` → 0 errors / 0 warnings.
- [ ] **Шаг 12.4.** Проверка AC8: `git diff ce0bf7c -- src/tests/PgWorker.IntegrationTests/E2e/E2eWalStreamMasterDownScenarios.cs | grep -E '^[+-].*(FromSeconds|FromMinutes|Task\.Delay)'` — только парные переносы без изменения чисел: 360, 300, 120×2, 600, 30 (окна repair), 300, 30, 300, 120 (tli-хелпер).
- [ ] **Шаг 12.5.** Коммит: `git commit -m "test(t29): WalStream-MasterDown — все ожидания и repair-цикл через E2ePhase (прогресс доставки по листингу S3, unreachable/окно N), дамп в ArtifactsDir (spec §3.9, §4.6)"`.

**Выход:** 16-минутный монолитный факт телеметрируется пофазно: provisioning/агенты/резолв мастера ×2/repair (окна видны)/доставка/ключ/новый агент — `[PHASE]` с budget, тики repair и delivery, failed/slow-phase-сбор, дамп в каталоге телеметрии.

**Проверка:** сборка + grep-гейт; функционально — задача 14 п.3 (AC10).

**Связь со spec:** §3.9 (слепые зоны 1–4), §4.1, §4.3–4.4, §4.6; AC1–AC3, AC8.

---

### Задача 13: `E2eStrictScenarios` — оба теста через `E2ePhase`

**Вход (предусловие):** задачи 1–11 исполнены; база диффа — `ce0bf7c`. Независима от задачи 12.

**Действие (файлы):**
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eStrictScenarios.cs`

**Interfaces (потребляет):** `E2ePhase.WaitAsync` (задача 1).

1. **Миграция всех 6 `E2eFixture.WaitForAsync`** (бюджеты ДОСЛОВНО, AC8; `WaitForDiscoveryAsync` — общий хелпер, вызывается из обоих тестов — миграция в одном месте покрывает обе точки):

| Тест | Место | Имя фазы | Бюджет | Прогресс (ТОЛЬКО etcd-чтения) |
|---|---|---|---|---|
| 1 | `provisioned` (dsn обоих шардов) | `strict-provisioning` | 360 с | dsn/state-ключи нод (п.2) |
| 1 | `active` (config без state) | `strict-active` | 60 с | config-state (п.3) |
| 1 | `converged` (Patroni strict=false) | `strict-dcs-converged` | 360 с | work-ключ из etcd (п.4) |
| 1+2 | `WaitForDiscoveryAsync` | `strict-discovery` | 30 с | `api-keys=N` |
| 2 | `provisioned` | `strict-provisioning` | 360 с | как тест 1 |
| 2 | `active` | `strict-active` | 60 с | config-state |

Точки исполнения — 7 (discovery дважды); имена фаз у повторяющихся гейтов одинаковы (различимы по времени в `phases.log`). Patroni-проба `GET /config` остаётся ТОЛЬКО условием фазы `strict-dcs-converged` (существующее поведение); в тиках — etcd-чтения, Patroni-пробу в тики НЕ добавляем (гигиена нагрузки телеметрии, spec §9). Ассерты и окна — без изменений.

2. **Прогресс provisioning** (state-ключи нод и dsn из etcd; число нод — из ключа replicas: shard2 однорепликный в тесте 2):
```csharp
progress: async () =>
{
    var parts = new List<string>();
    foreach (var shard in new[] { "shard1", "shard2" })
    {
        var replicasKv = await GetOrNullAsync($"/clusters/{Cluster}/shards/{shard}/replicas");
        var replicas = int.TryParse(replicasKv?.Value, out var r) ? r : 2;
        var states = new List<string>();
        for (var i = 0; i < replicas; i++)
            states.Add((await GetOrNullAsync(
                $"/clusters/{Cluster}/shards/{shard}/nodes/{shard}{(char)('a' + i)}/state"))?.Value ?? "-");
        var dsn = await GetOrNullAsync($"/clusters/{Cluster}/shards/{shard}/dsn") is null ? "нет" : "есть";
        parts.Add($"{shard}: [{string.Join(",", states)}] dsn={dsn}");
    }
    return string.Join("; ", parts);
}
```
3. **Прогресс Active-гейта** (какой state висит — слепая зона §3.10 п.2; `ConfigStateFieldAsync` уже в классе):
```csharp
progress: async () => $"state={await ConfigStateFieldAsync(Cluster) ?? "нет (Active)"}"
```
4. **Прогресс конвергенции** (etcd work-ключ: фаза надзора и ушёл ли strict-патч — слепая зона §3.10 п.4):
```csharp
progress: async () =>
{
    var work = (await GetOrNullAsync($"/pgworker/work/{Cluster}"))?.Value;
    if (work is null)
        return "work=нет";
    try
    {
        using var doc = JsonDocument.Parse(work);
        var phase = doc.RootElement.TryGetProperty("phase", out var p) ? p.GetString() : "-";
        var err = doc.RootElement.TryGetProperty("last_error", out var e) ? e.GetString() : null;
        return $"work.phase={phase}, strict-патч={(err is not null && err.Contains("synchronous_mode_strict", StringComparison.Ordinal) ? "есть" : "нет")}";
    }
    catch (JsonException)
    {
        return "work=<не-JSON>";
    }
}
```
5. **Прогресс discovery** (внутри `WaitForDiscoveryAsync`, после миграции ожидания на `E2ePhase.WaitAsync(Fx, "strict-discovery", …, TimeSpan.FromSeconds(30), ct, progress: …)`):
```csharp
progress: async () => $"api-keys={(await G.RangeAsync(Endpoint, "/pgworker/api/", ct)).Value.Count}"
```

- [ ] **Шаг 13.1.** Миграция 6 ожиданий (таблица п.1) с прогрессами пп.2–5.
- [ ] **Шаг 13.2.** Проверка — точечная сборка: `dotnet build src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release` → 0 errors / 0 warnings.
- [ ] **Шаг 13.3.** Проверка AC8: `git diff ce0bf7c -- src/tests/PgWorker.IntegrationTests/E2e/E2eStrictScenarios.cs | grep -E '^[+-].*(FromSeconds|FromMinutes|Task\.Delay)'` — только парные переносы: 360, 60, 360, 30 (discovery), 360, 60.
- [ ] **Шаг 13.4.** Коммит: `git commit -m "test(t29): Strict — оба теста через E2ePhase (прогресс: config-state, state-ключи нод, dsn, work-ключ конвергенции) (spec §3.10)"`.

**Выход:** голый класс телеметрируется: гейты provisioning/Active/converged/discovery несут `[PHASE]` с budget, тики показывают висящий state и застрявшие ноды; провал 60-с окна Active оставляет failed-phase-картину (AC4 — окно 60 с < порога 60 с закрывается немедленным сбором).

**Проверка:** сборка + grep-гейт; функционально — задача 14 п.3 (AC10).

**Связь со spec:** §3.10 (слепые зоны 1–4), §4.1, §4.3–4.4; AC1–AC4, AC8.

---

### Задача 14: Приёмка-дополнение (улики §3.9/§3.10) — сборка, AC8-гейт, расширенный AC10-прогон

**Вход (предусловие):** задачи 12–13 закоммичены (после `ce0bf7c`).

**Действие (файлы):** правок кода нет (кроме исправлений, если гейты найдут нарушение — тогда фикс и повтор гейта).

**Interfaces (потребляет):** задачи 12–13.

1. **Полная сборка решения**:
   ```bash
   dotnet build src/PgWorker.slnx -c Release
   ```
   Критерий: 0 Error(s), 0 Warning(s).
2. **AC8 — гейт по диффу задач 12–13** (база `ce0bf7c`; белый список новых чисел НЕ действует — он исчерпан кодом задач 1–2, в этом диффе допустимы ТОЛЬКО парные переносы без изменения чисел):
   ```bash
   git diff ce0bf7c..HEAD -- src/tests/PgWorker.IntegrationTests/E2e/E2eWalStreamMasterDownScenarios.cs src/tests/PgWorker.IntegrationTests/E2e/E2eStrictScenarios.cs | grep -E '^[+-].*(FromSeconds|FromMinutes|FromMilliseconds|Task\.Delay|CancelAfter)'
   ```
   Критерий: каждая пара −/+ — перенос; контрольные числа: WalStream_MasterDown 360/300/120×2/600 (окна 30 с)/300/30/300 (+120 tli-хелпер); Strict 360/60/360 (тест 1)/30 с discovery/360/60. Ассерты (`Should().BeTrue("кластер перешёл в Active…")`, `newMasterNode.Should().NotBe…`, `chain.IsContinuous.Should()…`) — нетронуты.
3. **AC10 — прогон E2E t29-фильтром ПОЛНОГО перечня §3** (8 классов; канонический N=5; нормальный рабочий фон — без условий и ожиданий; для классов задач 3–8 — контрольное повторное покрытие, для 12–13 — первичное):
   ```bash
   DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~E2eScenarios.Acceptance|FullyQualifiedName~E2eMoveScenarios|FullyQualifiedName~E2eWalStreamPromoteScenarios|FullyQualifiedName~E2eWalStreamMasterDownScenarios|FullyQualifiedName~E2eHaEtcdScenarios|FullyQualifiedName~E2eWorkerCertScenarios|FullyQualifiedName~E2eSecondInstanceScenarios|FullyQualifiedName~E2eStrictScenarios"
   ```
   Критерии (зелёность НЕ обязательна): каждая фаза каждого класса (включая `wal-ac4-*` и `strict-*`) оставила `[PHASE] … budget=…`; `phases.log` заполнен в каждом `/tmp/pgw-e2e-artifacts-<guid>/`:
   ```bash
   ls /tmp/pgw-e2e-artifacts-*/phases.log
   grep -hc '\[PHASE\]' /tmp/pgw-e2e-artifacts-*/phases.log
   ```
   Упавшие — разбор ПО ТЕЛЕМЕТРИИ без перезапуска (журнал, `phases.log`, `failed-phase-*`, docker-логи, `host-*.log`, `wal-diag-*`); причины — в отчёт; перезапуск — только по согласию пользователя.
4. **Чистота до/после серии** (AGENTS.md): ПЕРЕД стартом серии и после финальной строки:
   ```bash
   docker ps -a --format '{{.Names}}' | grep -c 'pgw-'
   docker network ls --format '{{.Name}}' | grep -cE 'pgw-|kfw-net'
   ```
   Оба счётчика 0 до старта и после зачистки; ненулевые после прогона (упавшие сценарии оставляют объекты намеренно) — разбор, ручная зачистка по `README-cleanup.txt`, повторная проверка до нуля. Новую серию поверх незачищенной НЕ запускать.
5. **Итог** — краткий отчёт в `docs/superpowers/2026-10-07-flaky-tests-telemetry/journal.md`: результаты пп.1–4, список фаз новых классов с budget по артефактам, причины упавших (если были). Коммит (если правки не потребовались — отчёт без коммита):
   ```bash
   git add -A && git commit -m "test(t29): приёмка-дополнение §3.9/§3.10 — сборка 0/0, AC8-гейт, E2E t29-прогон N=5 (8 классов): телеметрия полная (spec §8)"
   ```

**Выход:** AC1–AC4/AC8/AC10 подтверждены на полном перечне §3 (включая оба новых класса); ветка готова к code-review и мерж-гейту.

**Проверка:** сами гейты пп.1–4.

**Связь со spec:** §3.9, §3.10, §7 фаза 2 (хвост миграции) и фаза 5, §8 (AC1/2/3/8/10).

---

## Соответствие задач критериям приёмки (spec §8)

| AC | Чем закрывается |
|---|---|
| AC1 место сбоя видно | задачи 1, 3–8, 12–13 (фазы+тики+failed-phase, `phases.log` — последний шаг) |
| AC2 единый формат + phases.log | задачи 1, 3–8, 10, 12–13 |
| AC3 порог 60 с всюду в t29 | задача 1 (перенос порога в `E2ePhase`), 3–8 и 12–13 (все длинные фазы мигрированы), 11 п.4 / 14 п.3 |
| AC4 провал окна = картина момента | задача 1 (`failed-phase-*`), 11 п.4 / 14 п.3 |
| AC5 AC5-разложение | задача 3, 11 п.5 |
| AC6 build с хвостом | задачи 2, 8, 11 п.5 |
| AC7 ретрай готовности etcd | задача 9, 11 п.3 |
| AC8 бюджеты не тронуты | инвариант каждой задачи + гейты 11 п.2 и 14 п.2 |
| AC9 документация | задача 10 |
| AC10 валидация телеметрии | 11 п.4 / 14 п.3 (t29-фильтр — весь перечень §3, включая WalStreamMasterDown и Strict; N=5, полнота телеметрии) |

## Порядок исполнения и зависимости

**Статус:** задачи 1–11 исполнены (коммиты `592a6f7`…`ca1524f`, base `ce0bf7c`) — НЕ переисполнять. Исполнять только задачи 12–14.

```
Исполнено (1–11):  1 (E2ePhase) ─┬─> 3 (AC5) ─> 4 (Move) ─> 5 (WalStreamPromote) ─> 6 (HaEtcd) ─> 7 (WorkerCert) ─┐
                                 └─> 2 (RunProcessAsync) ─────────────────────────> 8 (SecondInstance) ─┘
                    9 (не-E2E) — после 1–8; 10 (док) — после 9; 11 (приёмка) — после 10.

Дополняется (12–14): 12 (MasterDown) ─┬─> 14 (приёмка-дополнение)
                      13 (Strict) ─────┘
12 и 13 независимы друг от друга, обе — от базы ce0bf7c; 14 — после 12–13.
```

Порядок миграции классов (3→8, затем 12→13) зафиксирован spec §7 по частоте флейка; менять нельзя.
