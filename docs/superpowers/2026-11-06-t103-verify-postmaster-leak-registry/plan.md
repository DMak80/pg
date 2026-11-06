# t103: жизненный цикл postmaster verify/restore-check + сверка id итога — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Устранить два дефекта t103 — утечку postmaster restore/drill-операции приёмника (Д1) и запись итога проверки полного/заявки restore из чужой операции общего ops-реестра (Д2).

**Architecture:** Сначала правится канон `arch/` (правила жизненного цикла постгресса и сверки id снимка), затем код зеркалит: приёмник (`RestoreOperation`, `Program.cs`) — защита каталога/остановка при любом исходе/гашение при остановке хоста; воркер (`BackupVerifyProcess`, `RestoreProcess`) — сверка id снимка перед вердиктом.

**Tech Stack:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`), xUnit, AAA-комментарии в тестах.

**Spec:** `docs/superpowers/2026-11-06-t103-verify-postmaster-leak-registry/spec.md` (в этом же worktree; план аргументируется от spec — исполнитель читает оба файла).

## Global Constraints

- P/Invoke ЗАПРЕЩЁН (AGENTS.md): управление постгрессом — только `pg_ctl` через `ProcessRunner`, файлы — штатный .NET API.
- Бюджеты системы не меняются; новые бюджеты остановки: `stop -w -t 10` на попытку, две попытки; shutdown-ожидание ≤ 15 с (spec §7).
- Документация — по-русски, идентификаторы — по-английски; комментарии в каноне — текущим тоном окружающего текста, БЕЗ исторических пассажей (без «t103», дат, инцидентов — правила AGENTS.md).
- Общий ops-реестр (одна ops-операция на процесс, 409-гвард, TryBegin поверх терминального) НЕ меняется; схема имён `backup-drill/<id>` и путь `/tmp/restore-pg.log` НЕ меняются.
- Существующие таймауты/бюджеты тестов не трогать; все тесты чистят за собой (docker-контейнеры/сети между сериями).
- Коммит в feature-ветке worktree после каждого шага; в `main` не коммумитить.

## Review Focus

Пять рисков, которые тесты шагов обязаны накрыть (владелец — шаг в скобках):

1. Существующие снимки-хелперы тестов без `Id` (null) после введения сверки трактуются как чужие — все API-хелперы тестов обязаны нести id кандидата (шаг 6).
2. Нестопаемый сирота: pre-start защита обязана дать честный failed (не transient-маркер downloading — воркер бы ретраил вечно) и НЕ звать `pg_ctl start` (шаг 3).
3. Чистка scratch при живом постгрессе: finally обязан пытаться остановить ДО `Directory.Delete` (шаг 4 — порядок в журнале вызовов).
4. `pg_ctl status` не-0 ≠ всегда «не работает» (exit 4 — нет доступа): защита трактует любой не-0 как «не работает» → pid-файл срезается → `pg_ctl start` сам пере-проверит lock; поведение принято (spec §4.1 п.1), отдельной ветки не вводить.
5. Чужой running-снимок в verify-процессе: реланч-POST получит 409 — это норма (IsConflict), кандидат остаётся PENDING, лив-лока нет (шаг 6).

---

### Шаг 1. Канон Д1: жизненный цикл postmaster (arch/pgnode)

**Вход:** spec §4.1–§4.2 одобрен; файлы канона не менялись с Фазы 1.

**Действие** (сильная модель — вплетение в существующий текст, тон окружения, без истории):
- `arch/pgnode/06-restore-drill.md`: в §2 (механика restore; drill §3 наследует) после описания подготовки (права `chmod 700`, перед `pg_ctl start`) добавить блок «жизненный цикл postmaster» с четырьмя правилами по spec §4.1: (1) защита целевого каталога перед стартом — `pg_ctl status`, работает → `stop -m fast -w` с коротким бюджетом, при неудаче `-m immediate -w`; не остановился — операция failed с честной причиной; не работает → снять остаточный `postmaster.pid`; (2) лог `/tmp/restore-pg.log` очищается перед `pg_ctl start` операции — диагностика провала (хвост в `last_error`) содержит только строки своей инкарнации; (3) остановка при любом исходе (успех/провал любой фазы после старта/отмена/провал старта) ДО чистки scratch (drill) и возврата Patroni (restore); неудалённый остаток НЕ блокирует терминальный исход — дочистится следующим запуском через защиту каталога; (4) остановка процесса приёмника гасит активную ops-операцию (отмена + остановка постгресса с ожиданием) — сирот не оставляет; жёсткий SIGKILL закрывается защитой каталога при следующем запуске.
- `arch/pgnode/processes/d-restore.md`: в §1 фаза `recovering` — одна строка инварианта (старт в подготовленный каталог: постгресс прошлой инкарнации остановлен; при любом исходе постгресс операции остановлен до чистки/возврата Patroni) + ссылка на 06-restore-drill §2 (соседний уровень — ссылка корректна).
- `arch/pgnode/processes/e-drill.md`: в §2 («Отличия от restore», пункт про чистку scratch) — чистка scratch выполняется ПОСЛЕ остановки постгресса операции + ссылка на 06-restore-drill §2.

**Выход:** канон описывает жизненный цикл постгресса; двойного описания механики нет (детали — только в 06, процессы держат инвариант и ссылку).

**Проверка:** чтение трёх файлов: правила (1)–(4) присутствуют в 06; d-restore/e-drill содержат по одной строке-инварианту со ссылкой; исторических пассажей нет. Коммит: `git add arch/ && git commit -m "arch(pgnode): postmaster lifecycle rules for restore/drill (t103 spec §4.1-4.2)"`.

**Связь со spec:** §4.1, §4.2. Подпунктов нет (документация вплетением — работа сильной модели).

---

### Шаг 2. Канон Д2: сверка id итога операции (arch/backups)

**Вход:** шаг 1 закоммичен.

**Действие** (сильная модель):
- `arch/backups/01-full-backup.md` §7: после абзаца про статус `GET /api/ops/verify` добавить правило сверки снимка по `id` (симметрично формулировке g-drill §1): ops-реестр один на процесс — GET может вернуть чужой/прошлый снимок (напр., терминал restore/drill-операции с текстом pg_ctl); снимок с чужим `id` — доказательства итога НЕТ (ни OK, ни FAILED) — vanished-семантика: повторный POST идемпотентно поверх терминального снимка, чужой running — ожидание освобождения реестра.
- `arch/pgnode/processes/c-verify.md` §2 («Исходы»): строка — снимок с чужим `id` — не итог проверки, супервиз перезапускает операцию vanished-семантикой (ссылка на 01-full-backup §7 — родительский уровень).
- `arch/backups/processes/f-restore.md` §2: в абзаце про 409 при запуске («снимок 409 не различает свою/чужую») дополнить: супервиз операции сверяет `id` снимка с backup_id выбранного полного заявки; чужой id в любом состоянии — чужой итог/фаза: verdиктов и переходов по нему нет (REJOINING от чужого succeeded без system_id запрещён), запуск откладывается до освобождения реестра (transient, семантика 409).

**Выход:** канон фиксирует сверку id для verify и restore-супервизов.

**Проверка:** чтение трёх файлов — правила на месте, тон сохранён, без дублей. Коммит: `git add arch/ && git commit -m "arch(backups): verify/restore supervisors must match snapshot id (t103 spec §4.3-4.4)"`.

**Связь со spec:** §4.3, §4.4. Подпунктов нет.

---

### Шаг 3. RestoreOperation: защита каталога pre-start + очистка лога (TDD)

**Вход:** канон шага 1 закоммичен; `src/PgWorker.PgNode/Operations/RestoreOperation.cs` не менялся.

**Действие** (сильная модель): написать три failing-юнит-теста (подпункты 3.3), реализовать `PostmasterRunningAsync`/`TryStopPostmasterAsync` (подпункт 3.4), вставить вызов защиты + снятие pid + очистку лога (подпункт 3.5), обновить задетый ассерт HappyPath (подпункт 3.6). Порядок TDD: тесты → прогон (падают) → реализация → прогон (зелёные). Сам выполняет: прогон тестов после каждого подпункта, разбор падений, итоговое ревью соответствия канону шага 1.

Точки вставки (реальный код):
- Вызов защиты — в `RunAsync` ПОСЛЕ `phase = OperationPhases.Recovering; Update(OperationStates.Running, phase);` и ПЕРЕД `const int recoveryStartAttempts = 3;`.
- Методы — после `StopBestEffortAsync` (перед `TailAsync`).
- Ветка финального провала старта (`Fail(phase, $"pg_ctl start failed: ...")`) — отдельный stop НЕ добавлять: закрывается finally-стопом шага 4 (spec §5.1 последний абзац).

**Выход:** операция до старта постгресса останавливает сироту/снимает stale pid/очищает лог; юниты зелёные.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --filter "FullyQualifiedName~RestoreOperationTests"` — все (старые и новые) зелёные.

**Связь со spec:** §4.1 п.1–2, §5.1.

Подпункты:

- [ ] **3.1. Расширить фейк `FakeRunner` в `src/tests/PgWorker.UnitTests/PgNode/RestoreOperationTests.cs`** — программируемые исходы `pg_ctl status`/`stop` и LSN-пробы. В классе `FakeRunner` после поля `public bool PsqlPollHang;` добавь три поля:
```csharp
// Защита каталога (t103): программируемые исходы pg_ctl. PgCtlStatusExit —
// ответ status (0 — postmaster работает, дефолт 3 — не работает);
// PgCtlStopExits — очередь исходов stop (по одному на вызов; пусто — 0).
// Успешный stop останавливает «postmaster»: следующие status — не работает.
// LsnReply — ответ psql-пробы pg_current_wal_lsn (пустая строка — провал
// ветки снятия LSN ПОСЛЕ старта постгресса).
public int PgCtlStatusExit = 3;
public readonly Queue<int> PgCtlStopExits = [];
public string LsnReply = "0/2000028";
```
  Затем в методе `RunAsync` класса замени ветку `case "pg_ctl":` (сейчас возвращает `(StartExitCode, "", "pg_ctl: could not start server")` при `args.Contains(" start") && StartExitCode != 0`, иначе `(0, "", "")`) на:
```csharp
case "pg_ctl":
    if (args.Contains(" status"))
        return (PgCtlStatusExit,
            PgCtlStatusExit == 0 ? "pg_ctl: server is running (PID: 331)" : "pg_ctl: no server running", "");
    if (args.Contains(" stop"))
    {
        var stopExit = PgCtlStopExits.Count > 0 ? PgCtlStopExits.Dequeue() : 0;
        if (stopExit == 0)
            PgCtlStatusExit = 3; // успешный stop останавливает «postmaster»
        return (stopExit, "", "");
    }
    if (args.Contains(" start") && StartExitCode != 0)
        return (StartExitCode, "", "pg_ctl: could not start server");
    PgCtlStatusExit = 0; // успешный старт ЗАПУСКАЕТ «postmaster» операции
    return (0, "", "");
```
  И в том же методе `RunAsync` в ветке `case "psql":` замени возвращаемую строку `return (0, args.Contains("pg_is_in_recovery") ? "f" : "0/2000028", "");` на `return (0, args.Contains("pg_is_in_recovery") ? "f" : LsnReply, "");` (ветки PsqlPollHang/PsqlPollFailsBeforeOk выше по коду не трогать).

- [ ] **3.2. Добавить drill-режим в хелпер запроса** того же файла: замени сигнатуру `private static PgWorker.PgNode.Operations.StartRestoreRequest Request(string? targetTime = null)` на `Request(string? targetTime = null, string? drillId = null)` и в теле — именованный аргумент `Id: null` на `Id: drillId`. Остальное тело не менять.

- [ ] **3.3. Добавить три юнит-теста** в класс `RestoreOperationTests` (после метода `HappyPath_IncidentEditsAndCallOrder_ThenSucceeded`; стиль AAA-комментариев как у соседей; хелперы `NewPgdata()`, `Create(root, runner, ...)`, `Request(...)` уже есть; снимок реестра — `registrySnapshot(op)` как в соседних тестах):
  1. `PreStart_StopsOrphanPostmaster_ThenSucceeds` — Arrange: `NewPgdata()`, в `Path.Combine(pgdata, "postmaster.pid")` записать строку `"331 ..."`; `FakeRunner { PgCtlStatusExit = 0 }` (сирота работает), `PgCtlStopExits` пуст (stop успешен). Act: `await op.RunAsync(CancellationToken.None)`. Assert: снимок `succeeded`; в `runner.Invocations` есть элемент `FileName == "pg_ctl"` с `Args.Contains("stop")` и `Args.Contains("fast")`, И он идёт ДО первого элемента с `Args.Contains("start")`; файла `postmaster.pid` больше нет.
  2. `PreStart_RemovesStalePidFile_WhenPostmasterDead` — Arrange: `NewPgdata()`, файл `postmaster.pid` записан; `FakeRunner` дефолтный (status → 3). Act: `RunAsync`. Assert: `succeeded`; файла `postmaster.pid` нет; в `Invocations` ровно один вызов `pg_ctl` со `" status"` ДО старта.
  3. `PreStart_UnstoppableOrphan_FailsHonest_WithoutStart` — Arrange: `NewPgdata()`; `FakeRunner { PgCtlStatusExit = 0 }`, `PgCtlStopExits = new Queue<int>([1, 1, 1, 1])` (все stop провальны). Act: `RunAsync`. Assert: снимок `failed`; `snap.Error` содержит `"cannot be stopped"`; `snap.Phase == "recovering"` (НЕ downloading — не transient-маркер); в `Invocations` НЕТ вызова `pg_ctl` со `Args.Contains("start")`.

- [ ] **3.4. Добавить два метода в `src/PgWorker.PgNode/Operations/RestoreOperation.cs`** после метода `StopBestEffortAsync` (перед `TailAsync`), дословно:
```csharp
// pg_ctl status: exit 0 — postmaster каталога работает (контракт pg_ctl);
// прочие исходы — не работает/недоступен (lock пере-проверит сам pg_ctl start).
private async Task<bool> PostmasterRunningAsync(string pgdata, CancellationToken token)
    => (await _runner.RunAsync(PgCtl(pgdata, "status"), token)).ExitCode == 0;

/// <summary>Остановка postmaster каталога (fast → immediate, каждая -w -t 10)
/// (arch/pgnode/06-restore-drill.md §2 — защита каталога и финальная
/// остановка): true — postmaster не работает (или не работал); false —
/// неостановим.</summary>
private async Task<bool> TryStopPostmasterAsync(string pgdata, CancellationToken token)
{
    if (!await PostmasterRunningAsync(pgdata, token))
        return true;
    foreach (var mode in new[] { "fast", "immediate" })
    {
        var stopPsi = PgCtl(pgdata, "stop");
        stopPsi.ArgumentList.Add("-m");
        stopPsi.ArgumentList.Add(mode);
        stopPsi.ArgumentList.Add("-w");
        stopPsi.ArgumentList.Add("-t");
        stopPsi.ArgumentList.Add("10");
        await _runner.RunAsync(stopPsi, token);
        if (!await PostmasterRunningAsync(pgdata, token))
            return true;
    }

    return false;
}
```

- [ ] **3.5. Вставить вызов защиты в `RunAsync`** того же файла: найди строки
```csharp
            phase = OperationPhases.Recovering;
            Update(OperationStates.Running, phase);
            const int recoveryStartAttempts = 3;
```
  и между `Update(OperationStates.Running, phase);` и `const int recoveryStartAttempts = 3;` вставь дословно:
```csharp
            // Защита целевого каталога перед стартом (arch/pgnode/06-restore-drill.md
            // §2): postmaster-сирота прошлой инкарнации (пережил рестарт приёмника)
            // останавливается ДО старта; неостановим — честный failed (не
            // transient: не данные и не транспорт); мёртвому — снимается
            // остаточный postmaster.pid (иначе pg_ctl start откажет от старта).
            if (!await TryStopPostmasterAsync(pgdata, token))
            {
                Fail(OperationPhases.Recovering,
                    $"postmaster in {pgdata} cannot be stopped (fast and immediate exhausted)");
                return;
            }

            var postmasterPid = Path.Combine(pgdata, "postmaster.pid");
            if (File.Exists(postmasterPid))
                File.Delete(postmasterPid);

            // Лог инкарнации (файл общий у операций ноды): диагностика провала
            // старта — хвост restore-pg.log в last_error — содержит строки
            // только своей инкарнации.
            try { File.Delete("/tmp/restore-pg.log"); } catch { /* файла нет — норма */ }
```

- [ ] **3.6. Обновить задетый ассерт в `HappyPath_IncidentEditsAndCallOrder_ThenSucceeded`** того же тест-файла: строку
```csharp
        var start = runner.Invocations.First(i => i.FileName == "pg_ctl");
```
  замени на
```csharp
        var start = runner.Invocations.First(
            i => i.FileName == "pg_ctl" && i.Args.Contains("start"));
```
  (выборка теперь обязана различать start-вызов от новых status/stop вызовов; соседний `ContainInOrder("chmod", "pg_ctl", "psql", "psql", "pg_ctl")` остаётся валидным).

- [ ] **3.7. Скомпилировать и прогнать**: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --filter "FullyQualifiedName~RestoreOperationTests"` — ожидание: сборка без warnings (TreatWarningsAsErrors), все тесты PASS. Любой красный тест из числа СУЩЕСТВУЮЩИХ, упавший на новом порядке вызовов (например, берущий `First` по `pg_ctl`), — исправить ассерт аналогично 3.6 (фильтровать по `Args.Contains("start")`/`"status"`/`"stop")`); менять прод-код ради ассерта нельзя.

---

### Шаг 4. RestoreOperation: finally-стоп перед чисткой/возвратом Patroni (TDD)

**Вход:** шаг 3 зелёный (`TryStopPostmasterAsync` существует).

**Действие** (сильная модель): failing-тест порядка finally (подпункт 4.1), вставка finally-стопа (подпункт 4.2), прогон. Сам: ревью, что finally не меняет терминальный снимок (стоп — best-effort, результат игнорируется).

**Выход:** при любом исходе операции постгресс останавливается до `Directory.Delete` (drill) и до `supervisorctl start spilo` (restore).

**Проверка:** тот же фильтр `RestoreOperationTests` — зелёные, включая новый тест.

**Связь со spec:** §4.1 п.3, §5.1 (finally).

Подпункты:

- [ ] **4.1. Добавить два юнит-теста finally-порядка** в `RestoreOperationTests` (стиль AAA). Общая подготовка обоих: в класс тестов добавь `private const string DrillId = "20260101000001Z";` рядом с существующей `private const string Id = "20260101000000Z";`; drill-операция создаётся напрямую (хелпер `Create` не годится — он ставит restore-режим): Arrange-блок
```csharp
var root = NewPgdata();
var pgdata = Path.Combine(root, "pgroot", "data");
var registry = new OperationRegistry();
registry.TryBegin(new OperationSnapshot(DrillId, "running", "downloading"));
var op = new RestoreOperation(Request(drillId: DrillId), runner, registry,
    pgdata: pgdata, _ => new FakeTarS3());
```
  Cleanup обоих: `if (Directory.Exists(root)) Directory.Delete(root, recursive: true);`.
  1. `DrillFinally_FailureAfterStart_StopsPostmasterBeforeScratchRemoval` — ГЛАВНЫЙ кейс: провал операции ПОСЛЕ старта постгресса, постгресс ЖИВ, finally обязан пытаться остановить его ДО удаления scratch. Arrange: `var runner = new FakeRunner { LsnReply = "", PgCtlStopExits = new Queue<int>([1, 1, 1]) };` — семантика фейка (см. подпункт 3.1): старт успешен → `PgCtlStatusExit` станет 0 (постгресс операции жив); поллинг `pg_is_in_recovery` вернёт `"f"` (выход из цикла); LSN-проба вернёт пусто → прод-код идёт в ветку `pg_current_wal_lsn: empty result` (Fail + `StopBestEffortAsync` stop fast → исход 1 из очереди, постгресс жив); все три stop-исхода в очереди провальные (1) — постгресс «неостановим», finally-стоп честно исчерпывает fast → immediate. Act: `await op.RunAsync(CancellationToken.None)`. Assert: (i) снимок `failed`, `snap.Error` содержит `"pg_current_wal_lsn"`; (ii) в `runner.Invocations` есть вызов `FileName == "pg_ctl"` с `Args.Contains("stop")` и `Args.Contains("fast")`, ИНДЕКС которого БОЛЬШЕ индекса последнего вызова `FileName == "psql"` (stop-попытки идут после провала LSN-пробы); (iii) есть вызов `pg_ctl stop` с `Args.Contains("immediate")` (finally эскалирует режим при живом постгрессе); (iv) последний вызов `pg_ctl` в журнале — со `Args.Contains("status")` (заключительная проверка finally) с индексом БОЛЬШЕ индекса последнего `psql`; (v) `Directory.Exists(pgdata)` — false: неостановимый остаток НЕ заблокировал чистку scratch (best-effort удаление выполнилось).
  2. `DrillFinally_StartFailure_FinallyGuardRunsAfterFailure` — второй кейс (провал старта): Arrange: `var runner = new FakeRunner { StartExitCode = 1 };` (старт провален, постгресс не запускался — `PgCtlStatusExit` остаётся 3). Act: `await op.RunAsync(CancellationToken.None)`. Assert: снимок `failed`; каталог `pgdata` отсутствует (drill-чистка выполнилась); индекс последнего вызова `pg_ctl` со `Args.Contains("status")` БОЛЬШЕ индекса вызова со `Args.Contains("start")` (finally-защита отработала после провала старта).

- [ ] **4.2. Вставить finally-стоп в `RunAsync`** файла `src/PgWorker.PgNode/Operations/RestoreOperation.cs`: в блоке `finally` (после `catch (Exception e) { Fail(phase, ...); }`) найди начало
```csharp
        finally
        {
            // Patroni ноды — обратно при ЛЮБОМ исходе RESTORE (там он был
```
  и вставь НЕПОСРЕДСТВЕННО ПОСЛЕ строки `finally\n        {` (до комментария про Patroni) дословно:
```csharp
            // Postmaster операции остановлен при ЛЮБОМ исходе ДО возврата
            // Patroni (restore) и чистки scratch (drill) — arch/pgnode/
            // 06-restore-drill.md §2: неостановимый остаток НЕ блокирует
            // терминальный исход (каталог дочистит защита каталога следующего
            // запуска); терминальный снимок не трогаем.
            try { await TryStopPostmasterAsync(_pgdata, CancellationToken.None); }
            catch { /* best-effort */ }
```

- [ ] **4.3. Прогнать**: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --filter "FullyQualifiedName~RestoreOperationTests"` — все PASS.

---

### Шаг 5. Program.cs: гашение активной ops-операции при остановке приёмника

**Вход:** шаг 4 зелёный; `src/PgWorker.PgNode/Program.cs` не менялся.

**Действие** (сильная модель): вставка shutdown-хука (подпункт 5.1); сам — сборка и ревью, что хук не мешает штатному запуску (`Register` срабатывает только на ApplicationStopping).

**Выход:** при graceful-остановке pgnode активная ops-операция отменяется (kill дерева child-процессов + `pg_ctl stop -m fast`) с ожиданием ≤ 15 с; юнит-прогон приёмника не задет (верификация — E2E шага 8).

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` — сборка чистая (0 warnings).

**Связь со spec:** §4.1 п.4, §5.2.

Подпункты:

- [ ] **5.1. Вставить shutdown-хук в `MapOpsRoutes`** файла `src/PgWorker.PgNode/Program.cs`: найди в методе `MapOpsRoutes` строку
```csharp
        IReceiverOperation? active = null;
```
  и сразу после неё вставь дословно:
```csharp
        // Остановка хоста гасит активную ops-операцию (arch/pgnode/06-restore-drill.md
        // §2): CancelAsync отменяет задачу (runner гасит дерево child-процессов) и
        // останавливает postmaster операции — сирот приёмник не оставляет.
        // Ожидание ≤15 c; жёсткий SIGKILL supervisord закроет остаток защитой
        // каталога при следующем запуске.
        app.Lifetime.ApplicationStopping.Register(() =>
        {
            try
            {
                (active?.CancelAsync() ?? Task.CompletedTask)
                    .WaitAsync(TimeSpan.FromSeconds(15))
                    .GetAwaiter().GetResult();
            }
            catch
            {
                // best-effort: нестопаемый остаток подберёт защита каталога
            }
        });
```
  Затем: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` — ожидание: `Build succeeded`, 0 Warning(s) 0 Error(s). Если компилятор не видит `Task.CompletedTask`/`WaitAsync` — using уже есть (`System.Threading.Tasks` через implicit usings) — ничего дописывать не нужно; при ошибке имени `app.Lifetime` — проверить, что вставка внутри метода `MapOpsRoutes(WebApplication app)`, а не снаружи.

---

### Шаг 6. BackupVerifyProcess: сверка id снимка (TDD)

**Вход:** канон шага 2 закоммичен; `src/PgWorker.Backups/Process/BackupVerifyProcess.cs` и `src/tests/PgWorker.IntegrationTests/Backups/BackupVerifyProcessTests.cs` не менялись.

**Действие** (сильная модель): failing-интеграционные тесты (подпункт 6.3), вставка сверки (подпункт 6.1), приведение API-хелперов тестов к снимкам с id (подпункт 6.2), прогон; сам — разбор падений существующих тестов на чужих кандидатах (например, кластеры с кандидатом `20260911090000Z` — передать фактический id в хелпер) и ревью соответствия канону шага 2.

Точка вставки (реальный код): в `TickShardAsync` — между блоком `if (reply.IsTransient || reply.Status is not { } snapshot) { ... }` и `switch (snapshot.State)`.

**Выход:** чужой снимок (id ≠ кандидата) не меняет verify-статус полного; выполняется повторный POST; свой снимок — прежнее поведение.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter "FullyQualifiedName~BackupVerifyProcessTests"` — все зелёные (20 старых + 2 новых). После серии — зачистка docker-контейнеров/сетей теста (OwnEtcd умирает с Fact'ом, страхующий `docker ps --filter name=pgw-ee-` — пусто).

**Связь со spec:** §4.3, §5.3.

Подпункты:

- [ ] **6.1. Вставить сверку в `src/PgWorker.Backups/Process/BackupVerifyProcess.cs`**, метод `TickShardAsync`: найди строки
```csharp
        if (reply.IsTransient || reply.Status is not { } snapshot)
        {
            // Приёмник ноды недостижим/5xx — transient (тик повторит GET).
            observe(cluster, shard, "transient");
            return;
        }

        switch (snapshot.State)
```
  и между закрывающей `}` блока и `switch (snapshot.State)` вставь дословно:
```csharp
        // Сверка снимка по id (arch/backups/01-full-backup.md §7): ops-реестр
        // приёмника ОДИН на процесс — GET может вернуть чужой/прошлый снимок
        // (напр., терминал restore/drill-операции с текстом pg_ctl). Чужой id —
        // чужой итог: вердикта по нему нет, кандидат перезапускается
        // vanished-семантикой (повторный POST поверх терминального снимка;
        // чужой running ответит 409 — тик повторит).
        if (!string.Equals(snapshot.Id, full.Id, StringComparison.Ordinal))
        {
            logger.LogWarning(
                "{Op} {cluster}/{shard}: verify {id} — registry holds a foreign snapshot (id={SnapId}, state={State}); relaunching via the vanished path",
                Op, cluster, shard, id, snapshot.Id ?? "<null>", snapshot.State);
            await journal.WritePhaseAsync(cluster, Op, $"foreign-snapshot/{shard}/{id}",
                claims.InstanceId,
                $"receiver registry holds operation {snapshot.Id ?? "<null>"} ({snapshot.State}) — not the verify candidate", ct);
            var relaunch = await receiver.StartVerifyAsync(
                receiverUrl, StartRequest(cluster, shard, full, pool), ct);
            if (!relaunch.IsAccepted && !relaunch.IsConflict)
                observe(cluster, shard, "transient");
            return;
        }
```

- [ ] **6.2. Дать снимкам тестов id кандидата** в `src/tests/PgWorker.IntegrationTests/Backups/BackupVerifyProcessTests.cs`: конструктор `ReceiverStatus` — `(State, Phase, Id = null, ...)`, третий позиционный аргумент — id. Замени три хелпера (строки над классом `FakeReceiverClient`):
```csharp
    private static ReceiverReply ApiOk(string id = "20260911120000Z") =>
        new(200, new ReceiverStatus("succeeded", "verifying", id), null);
    private static ReceiverReply ApiDownloadTransient(string error, string id = "20260911120000Z") =>
        new(200, new ReceiverStatus("failed", "downloading", id, Error: error), null);
    private static ReceiverReply ApiVerifyFailed(string error, string id = "20260911120000Z") =>
        new(200, new ReceiverStatus("failed", "verifying", id, Error: error), null);
```
  Дефолт `20260911120000Z` покрывает большинство тестов (кандидат почти везде этот); тесты, чей кандидат имеет ДРУГОЙ id (ищи по `Completed("20260911090000Z"` и `Completed(id,` с переменной), — передать фактический id аргументом в соответствующем вызове хелпера. Скомпилировать: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/tests/PgWorker.IntegrationTests -c Release --nologo` — Build succeeded.

- [ ] **6.3. Добавить два интеграционных теста** в `BackupVerifyProcessTests` (по образцу `TickStartPostRefusedTransientStatusUntouched`: `await using var fx = await OwnEtcd.StartAsync("verify", ct); Fx = fx; await SeedAsync(...);`, ключ кандидата пишется руками для ассерта «не изменён»; каждый тест — СВОЙ префикс кластера `vf1`/`vf2`):
  1. `ForeignFailedSnapshot_CandidateUntouched_Relaunched` — Arrange: кластер `vf1`, кандидат `Completed("20260911120000Z", "000000010000000000000001", new BackupVerify(BackupVerifyStatus.Pending, null))`, ключ `/pgworker/backups/vf1/shard1/full/20260911120000Z` записан сериализованным кандидатом (`BackupStatusJson.Serialize`); `Receiver.GetReply = new ReceiverReply(200, new ReceiverStatus("failed", "recovering", "20261106060228Z", Error: "pg_ctl start failed: another server might be running"), null)`; `Receiver.StartReply` — дефолтный 202. Act: `TickAsync(BuildSnap("vf1"), BackupsOf("vf1", candidate), ct)`. Assert: значение ключа БАЙТ-В-БАЙТ равно записанному (`after.Should().Be(before)`); `Receiver.StartBodies` содержит ровно один элемент с `BackupId == "20260911120000Z"` (реланч выполнен); verifyObserver не получил `"failed"` (собрать outcomes-список как в соседнем тесте и проверить отсутствие `"failed"`).
  2. `ForeignSucceededSnapshot_NotMarkedOk` — Arrange: кластер `vf2`, тот же кандидат PENDING с записанным ключом; `Receiver.GetReply = new ReceiverReply(200, new ReceiverStatus("succeeded", "verifying", "20261106060228Z"), null)` (чужой succeeded). Act: `TickAsync`. Assert: ключ не изменён (verify остался PENDING, не OK); `Receiver.StartBodies` содержит один POST с `BackupId` кандидата; outcomes не содержит `"ok"`.
  Оба теста — AAA-комментарии; после `await using var fx` — стандартный паттерн DisposeAsync teardown OwnEtcd.

- [ ] **6.4. Прогнать серию**: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter "FullyQualifiedName~BackupVerifyProcessTests"` — все PASS; упавшие существующие из-за несения id — править вызовы хелперов (6.2), не прод-код.

---

### Шаг 7. RestoreProcess: сверка id снимка (TDD)

**Вход:** канон шага 2 закоммичен; `src/PgWorker.Backups/Process/RestoreProcess.cs` не менялся.

**Действие** (сильная модель): failing-тест (подпункт 7.2), вставка сверки (подпункт 7.1), прогон; сам — ревью семантики: чужой id → transient-ожидание (никаких переходов заявки, POST не шлём — фаза заявки вернётся к повтору штатной 409/vanished-механикой).

Точка вставки (реальный код): `SuperviseRestoreOpAsync` — между `var status = reply.Status; if (status is null) return ...;` и `switch (status.State)`.

**Выход:** чужой succeeded/failed/cancelled/running не меняет заявку restore (нет ложного REJOINING/FAILED); свой снимок — прежнее поведение.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter "FullyQualifiedName~RestoreProcessTests"` — все зелёные; зачистка контейнеров после серии.

**Связь со spec:** §4.4, §5.4.

Подпункты:

- [ ] **7.1. Вставить сверку в `src/PgWorker.Backups/Process/RestoreProcess.cs`**, метод `SuperviseRestoreOpAsync`: найди строки
```csharp
        var status = reply.Status;
        if (status is null)
            return Result<ProcessOutcome>.Success(ProcessOutcome.InProgress);

        switch (status.State)
```
  и между `if (status is null) ...` и `switch (status.State)` вставь дословно:
```csharp
        // Сверка снимка по id (arch/backups/processes/f-restore.md §2):
        // ops-реестр приёмника ОДИН на процесс — GET может вернуть чужой/
        // прошлый снимок (напр., терминал verify/drill-операции). Чужой id —
        // чужой итог/фаза: переходов заявки по нему нет (REJOINING от чужого
        // succeeded без system_id запер бы bootstrap) — ждём освобождения
        // реестра (transient, семантика 409).
        if (!string.Equals(status.Id, op.BackupId, StringComparison.Ordinal))
        {
            return await TransientAsync(cluster, $"foreign-snapshot/{shard.Name}/{op.Id}",
                $"receiver registry holds operation {status.Id ?? "<null>"} ({status.State}) — not this restore", ct);
        }
```

- [ ] **7.2. Добавить два интеграционных теста** в `src/tests/PgWorker.IntegrationTests/Backups/RestoreProcessTests.cs`. Общий скелет Arrange обоих — ТОЧНО по соседним RUNNING-тестам этого же файла (поля класса `_receiver`/`_claims`, хелперы `SeedAsync`/`BuildProcess`/`SeedRestoreAsync`): `await SeedAsync(<cluster>);` → `var process = BuildProcess(new FakeBackupS3(), new TestDriver(new StubScaleDriver(), new FakeBackupEngine()));` → `_receiver.GetReply = <снимок>;` → заявка через `SeedRestoreAsync(<cluster>, "shard1", <id>, backupId: "20260910120000Z", state: RestoreStatus.Running, startedUnix: TimeProvider.System.GetUtcNow().ToUnixTimeSeconds() - 60, phase: "recovering", phaseUnix: TimeProvider.System.GetUtcNow().ToUnixTimeSeconds() - 50)` — ВАЖНО: без `phase` заявка RUNNING уйдёт в ветку демонтажа (`switch (op.Phase)`: `null/""` → DemolishStepAsync) и до GET-супервиза не дойдётся; → claim-гвард тика `(await _claims.TryClaimClusterAsync(<cluster>, ct)).Value.Should().BeTrue();`. Act в обоих: `(await process.TickAsync(BuildSnap(), await BackupsFromEtcdAsync(<cluster>), ct)).IsSuccess.Should().BeTrue();`. Каждый тест — СВОЙ свежий префикс кластера и свой id заявки (по образцу соседей: id вида `"20260911121006Z"`/`"20260911121007Z"`):
  1. `ForeignSucceededSnapshot_RequestNotRejoining` — снимок: `new ReceiverReply(200, new ReceiverStatus("succeeded", "verifying", "20260911120000Z"), null)` (чужой id ≠ backup_id заявки, без `RestoredToLsn`/`SystemId`). Assert: `ReadRestoresAsync(<cluster>, "shard1")` — заявка `State == RestoreStatus.Running` (НЕ Rejoining), `Phase == "recovering"` (не изменён), `SystemId` — null; `_receiver.StartCalls == 0` (POST не шлём — ожидание освобождения реестра).
  2. `ForeignFailedSnapshot_RequestNotFailed` — снимок: `new ReceiverReply(200, new ReceiverStatus("failed", "recovering", "20261106060228Z", Error: "pg_ctl start failed: another server might be running"), null)` (чужой failed — снимок drill-операции). Assert: заявка `State == RestoreStatus.Running` (НЕ Failed), `Phase == "recovering"` (не изменён), `Error` — null; `_receiver.StartCalls == 0`.
  Оба — AAA-комментарии.

- [ ] **7.3. Прогнать серию**: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter "FullyQualifiedName~RestoreProcessTests"` — все PASS.

---

### Шаг 8. E2E-сценарий Drill_ReceiverRestart_PostmasterNotLeaked

**Вход:** шаги 3–7 закоммичены; E2ePrepare контур доступен; `E2eDrillScenarios` — существующий файл-паттерн.

**Действие** (сильная модель, целиком — без подпунктов; контекст E2E-инфраструктуры не переносим в подпункты): новый `[Fact] Drill_ReceiverRestart_PostmasterNotLeaked` в `src/tests/PgWorker.E2eTests/E2e/E2eDrillScenarios.cs` по образцу `Drill_Succeeds_CleansUp` (свой cluster `bkdrll{Fx.ClusterTag}r` — уникальный суффикс, `CleanupPreviousDrillClustersAsync`, `SeedClusterAsync`, policy c `"drill":{"interval_days":1}` и `"verify":{"on_create":true}` (нужен verify-кандидат для ассерта (д)), `StartBackupHostAsync`, `cts.CancelAfter(TimeSpan.FromMinutes(4))` — потолок: штатный дрилл ~90 c + рестарт + повторный прогон):
1. Дождаться COMPLETED-полного и ключа drill в RUNNING с фазой `recovering` (поллинг etcd `/pgworker/backups/<cluster>/shard1/drill` на `"phase":"recovering"`; бюджет 120 с).
2. `supervisorctl stop pgnode` ПЕРВОЙ ноды шарда (контейнер первой ноды: как в E2eSupervisorSelfHealScenarios — `Fx.RunDockerAsync(["exec", <node>, "supervisorctl", "stop", "pgnode"], ct)`; имя контейнера — по фильтру `pgw-<cluster>-shard1-shard1a`), выдержка 2 с, `supervisorctl start pgnode`.
3. Дождаться терминального итога дрилла (SUCCEEDED/FAILED без `phase`; бюджет 180 с — vanished-перезапуск повторяет скачивание+recovery).
4. Ассерты: (а) итог дрилла (его etcd-ключ, поле `error`) НЕ содержит `"another server might be running"` и `"postmaster.pid"`; (б) в логах контура НЕТ строки `another postmaster` — проверить оба источника: postmaster-лог операции `docker exec <node> cat /tmp/restore-pg.log` (файл мог отсутствовать до рестарта — отсутствие тоже чисто) и docker-лог контейнера ноды `docker logs <node>` (обе выгрузки — в строку, ассерт `Should().NotContain("another postmaster")`); (в) на ноде нет постгресса в drill-каталоге: `docker exec <node> pg_ctl status -D /home/postgres/pgdata/backup-drill/<drill-id>/pgdata` отвечает не-0 (или каталога нет — проверить `docker exec <node> test -d <путь>`); (г) scratch-каталоги `/home/postgres/pgdata/backup-drill/*` на ноде отсутствуют после терминала; (д) записи полных (etcd `/pgworker/backups/<cluster>/shard1/full/`) не содержат `verify` со `state":"FAILED` и текстом `pg_ctl start failed` (чужой итог).
5. Телеметрия упавшего ожидания — по образцу `Drill_Succeeds_CleansUp` (дамп в `/tmp/pgw-diag-<cluster>.txt`).

**Выход:** регресс-сценарий t103 зелёный; при повторении исходного дефекта падает ассертом (а)/(б)/(в)/(д).

**Проверка:** прогон в составе гейта шага 9 (точечно не запускать до полной серии шага 9 — правило двух отдельных команд).

**Связь со spec:** §5.5 (E2E), критерий 3.

---

### Шаг 9. Верификация: серии и E2E-гейт

**Вход:** шаги 1–8 закоммичены; docker-хост свободен (нет поднятого dev-станда и остатков серий).

**Действие** (сильная модель; команды — в фоне, лог каждой серии — в файл `/tmp/t103-<series>.log`; между сериями — дожидаться финальной строки и зачищать остатки `docker ps -a`/`docker network ls | grep -c <префикс>`):
1. Юниты: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release` — 0 failed.
2. Зачистка → интеграционные: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release` — 0 failed.
3. Статическая подготовка E2E: `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release` — штамп готовности.
4. Сборка (ОТДЕЛЬНОЙ командой, свежесть бинаря — часть гейта): `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` — Build succeeded, 0 warnings.
5. E2E (только `--no-build`): `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.E2eTests -c Release --no-build --filter "FullyQualifiedName~Drill_|FullyQualifiedName~Backup_ChainBroken_SelfHeals"` — все Passed (Drill_Succeeds_CleansUp, Drill_Failed_OnCorruptedWal, Drill_ReceiverRestart_PostmasterNotLeaked, Backup_ChainBroken_SelfHeals).
6. Итог серии: зачистка остаточных контейнеров/сетей прогона; упавший тест НЕ перезапускать — разбор по логам (`/tmp/pgw-e2e-artifacts-*`, `$TMPDIR` тоже проверить), фикс причины, затем повтор ШАГОМ целиком.

**Выход:** все серии зелёные на свежем Release.

**Проверка:** итоговые строки серий: `Failed: 0` (юниты/интеграция), `Passed: N, Failed: 0` (E2E).

**Связь со spec:** §6.5, критерий 4.

---

### Шаг 10. Роадмап: удаление пункта t103

**Вход:** шаг 9 зелёный; ветка готова к мержу.

**Действие** (сильная модель): удалить пункт из `arch/roadmap/bugs.md` (подпункт 10.1); убедиться, что в `AGENTS.md`/`arch/**`/`docs/**` (кроме `docs/superpowers/`) упоминаний t103 не осталось (`grep -rn "t103" --exclude-dir=superpowers` — пусто в arch/AGENTS.md; папка логов `arch/roadmap/bugs/t103-...` вне git — не трогать). Коммит вместе с финальным состоянием ветки — этот коммит становится мерж-коммитом в main (правило роадмапа: тег удаляется ТЕМ ЖЕ мерж-коммитом).

**Выход:** `arch/roadmap/bugs.md` без пункта t103; история — в git и docs/superpowers/.

**Проверка:** `grep -c "t103" arch/roadmap/bugs.md` → 0; `git status` чистый после коммита.

**Связь со spec:** §5.6, критерий 5.

Подпункты:

- [ ] **10.1. Удалить пункт из `arch/roadmap/bugs.md`** (worktree): первая строка пункта начинается РОВНО с `- **\`t103-verify-postmaster-leak-registry\`**` (без ведущих пробелов; с этого места начинается список пунктов после шапки — заголовок `# Bugs (pg)` и два вводных абзаца «Падения тестов…» / «Каждому пункту…»). Подпункты пункта (хронология и пр.) идут ниже с отступом `  - ` — они часть пункта. Удалить ВСЕ строки файла начиная со строки `- **\`t103-verify-postmaster-leak-registry\`**` и до КОНЦА файла (t103 — единственный и последний пункт). Шапку (заголовок и вводные абзацы) оставить без изменений. Результат — файл из одних только шапочных строк. Скомпилировать нечего (markdown); проверка: `grep -c "t103" arch/roadmap/bugs.md` выводит `0`.

---

## Порядок исполнения и зависимости

Шаги 1–2 (канон) — первыми, строго до кода (arch-first). Шаги 3–5 (приёмник) — после 1. Шаги 6–7 (воркер) — после 2 (не зависят от 3–5). Шаг 8 — после 3–7. Шаг 9 — после всех. Шаг 10 — после 9. Каждый шаг завершается коммитом в feature-ветке (`git add -A && git commit -m "<осмысленное сообщение>"`); шаги 1 и 2 — отдельные коммиты канона ДО кодовых.
