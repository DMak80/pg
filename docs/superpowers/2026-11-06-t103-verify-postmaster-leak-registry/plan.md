# t103: доработка по MR-ревью — жизненный цикл postmaster и сверка id/kind итога — план

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Закрыть blocker + 3 major MR-ревью (Р1 shutdown≠отмена, Р2 время жизни active, Р3 demolished-повтор при чужом снимке restore, Р4 вид операции `kind` в контракте снимка) поверх уже слитой в ветку первой реализации t103.

**Architecture:** Сначала уточняется канон `arch/` (разделение каналов отмены, `kind` снимка, demolished-повтор), затем код: приёмник (метод `ShutdownAsync`, вид операции в снимке, гварды живости в MapOpsRoutes) и воркер (сверка `id`+`kind`, сброс фазы заявки при чужом снимке). Первая реализация (защита каталога, finally-стоп, сверка id, E2E-сценарий) уже в ветке — план описывает ТОЛЬКО дельту.

**Tech Stack:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`), xUnit, AAA-комментарии в тестах.

**Spec:** `docs/superpowers/2026-11-06-t103-verify-postmaster-leak-registry/spec.md` (в этом же worktree; план аргументируется от spec — исполнитель читает оба файла).

## Global Constraints

- Объём — строго решения spec по Р1–Р4; minors/nits ревью (#5–#36, в т.ч. `stopwaitsecs`, guard-after-tar) — ВНЕ объёма, не трогать.
- Бюджеты не меняются: shutdown-ожидание ≤ 15 c (прежний), stop-бюджеты первой реализации — без изменений, E2E-бюджеты — без изменений.
- P/Invoke ЗАПРЕЩЁН (AGENTS.md).
- Общий ops-реестр (одна ops-операция на процесс, 409-гвард, TryBegin поверх терминального) НЕ меняется; операторский cancel API — контракт прежний (`cancelled` = «операции не было»).
- Снимок без `kind` (старый приёмник) = чужой; обратная совместимость контракта НЕ поддерживается.
- Полный бэкап (`FullBackupOperation`, full-реестр, MapFullRoutes) — вне области: поведение не меняется (новому методу интерфейса — тривиальная реализация).
- Документация — по-русски, идентификаторы — по-английски; комментарии канона — текущим тоном окружающего текста, БЕЗ исторических пассажей (без «t103», дат, номеров ревью).
- Коммит в feature-ветке worktree после каждого шага; в `main` не коммитить. Роадмап-гейт t103 уже исполнен веткой — шага по `arch/roadmap/` НЕТ.
- Все команды — в фоне (`run_in_background`), ожидания ≤ 30 с, длинные логи — в файл `/tmp/t103d-<series>.log`.

## Review Focus

Пять рисков доработки, которые тесты шагов обязаны накрыть (владелец — шаг в скобках):

1. Существующие тесты с «своими» снимками после сверки `kind` становятся чужими (kind null) — ВСЕ снимки своих id в интеграционных тестах обязаны нести свой kind (шаги 6, 7).
2. `ShutdownAsync`, случайно реализованный как `CancelAsync`, публикует `cancelled` — тесты обязаны фиксировать НЕ-cancelled снимок, вызов именно shutdown-канала и (для операторского канала verify) наоборот публикацию `cancelled` (шаги 4, 8).
3. Обнуление `active` без ReferenceEquals-гварда затирает более новую операцию; гвард без сверки id пускает отмену по чужому running-снимку — тест cancel-after-terminal обязан краснеть (шаги 5, 8).
4. Demolished-сброс без последующего POST = прежнее зависание: тест обязан доказать повторный запуск ВТОРЫМ тиком, а не только смену фазы (шаг 7).
5. `kind` в TryBegin, но не в Update/Fail операции (Update заменяет снимок целиком) — вид теряется после первого же обновления; юнит-тесты шага 4 ассертят `Kind` снимка напрямую, API-тесты шага 8 — компиляционную целостность контракта (шаги 4, 8).

---

### Шаг 1. Канон Р1/Р2: остановка приёмника ≠ операторская отмена (arch-first)

**Вход:** spec §4.1–§4.2 одобрён; файлы канона не менялись с первой итерации.

**Действие** (сильная модель — вплетение в существующий текст, тон окружения, без истории):
- `arch/pgnode/06-restore-drill.md` §2 п.5.4 («Остановка приёмника»): переформулировать по spec §4.1 — гашение активной ops-операции при остановке процесса приёмника (отмена задачи + остановка postmaster, ожидание ≤ 15 с) — ВНУТРЕННИЙ канал, отдельный от операторской отмены: снимок НЕ перезаписывается (никакого `cancelled`), реестр умирает вместе с процессом; операторская отмена (`POST /api/ops/cancel`, внешний канал) — прежняя семантика `state=cancelled`; жёсткий SIGKILL закрывает защита каталога (п. 5.1). Вторым предложением: отмена/гашение применяется ТОЛЬКО к живой (running) операции; завершённая операция (терминальный снимок) не отменяется и не останавливается повторно.
- `arch/pgnode/02-api.md` §3, абзац «GET-снимок несёт `id` операции реестра»: дополнить по spec §4.2 — снимок несёт `id` И `kind` (вид операции); ТАБЛИЦА значений `kind` (`verify`/`restore`/`drill` — с источником id каждого вида, по spec §4.2); супервизоры принимают исход ТОЛЬКО по снимку своей операции (собственный `id` И собственный `kind`); снимок с чужим `id`/`kind` ИЛИ без `kind` (контракт старого приёмника) — доказательства итога НЕТ (поставка воркера и приёмника единая, строгая трактовка); отмена применяется к running-операции, остановка приёмника снимок не перезаписывает (ссылка на 06-restore-drill §2 п.5.4 — соседний уровень, допустима).

**Выход:** канон разделяет каналы отмены и фиксирует `kind` снимка; дублей механики нет (детали гашения — только в 06, контракт — только в 02-api).

**Проверка:** чтение обоих файлов: правила на месте, таблица kind со всеми тремя значениями, исторических пассажей нет. Коммит: `git add arch/ && git commit -m "arch(pgnode): shutdown is not operator cancel; snapshot kind (t103 review)"`.

**Связь со spec:** §4.1, §4.2. Подпунктов нет (документация вплетением — работа сильной модели).

---

### Шаг 2. Канон Р3/Р4: сверка id+kind и demolished-повтор (arch-first)

**Вход:** шаг 1 закоммичен.

**Действие** (сильная модель):
- `arch/backups/01-full-backup.md` §7, правило сверки снимка (добавленное первой итерацией): чужим является снимок с чужим `id` ИЛИ чужим `kind` (не `verify`) ИЛИ без `kind` — доказательства итога проверки НЕТ: vanished-семантика (повторный POST поверх терминального; чужой running — 409, ожидание освобождения реестра).
- `arch/pgnode/processes/c-verify.md` §2 («Исходы»), строка про чужой `id`: дополнить — чужой `kind` (restore/drill того же полного) и отсутствие `kind` — тоже «не итог проверки», супервиз перезапускает операцию vanished-семантикой (ссылка на 01-full-backup §7 сохраняется).
- `arch/backups/processes/f-restore.md` §2, абзац сверки id: два уточнения по spec §4.4 — (1) чужой снимок = чужой `id` ИЛИ чужой `kind` (не `restore`) ИЛИ отсутствие `kind`; (2) чужой снимок в ЛЮБОМ состоянии (в т.ч. чужой running) — сброс фазы заявки в `demolished` + transient: команда повторяется следующим тиком (симметрично ветке vanished 404; живая чужая операция ответит 409 — семантика освобождения реестра). Ожидание чужого снимка БЕЗ сброса фазы запрещено (диспетчер в фазах launching/downloading/recovering команду не повторяет — заявка висела бы до статусного бюджета).

**Выход:** канон фиксирует сверку id+kind для обоих супервизов и demolished-повтор.

**Проверка:** чтение трёх файлов — формулировки на месте, тон сохранён, без дублей. Коммит: `git add arch/ && git commit -m "arch(backups): verify/restore match snapshot id and kind; foreign restore resets phase (t103 review)"`.

**Связь со spec:** §4.3, §4.4. Подпунктов нет.

---

### Шаг 3. Контракт kind: OperationKinds, OperationSnapshot, OpsStatusResponse, ReceiverStatus

**Вход:** канон шагов 1–2 закоммичен.

**Действие** (сильная модель: подпункты 3.1–3.4 дословны; после каждого — компиляция не обязательна, одна в 3.5). Размещение констант `OperationKinds` — в `PgWorker.Backups` (НЕ в PgNode.Operations): ссылка проекта PgWorker.Backups → PgWorker.PgNode запрещена (запрет цикла ссылок зафиксирован в комментарии WalReceiverApiClient.cs; PgNode уже импортирует Backups) — константы видны и приёмнику, и воркеру из одного места. ВАЖНО: `PgWorker.Backups` экспортирует типы, одноимённые с `PgWorker.PgNode.Operations` (`ErrorResponse`, `StartVerifyRequest` и др.) — в Program.cs namespace подключается ТОЛЬКО using-ALIAS единственного типа (подпункт 5.1), обычный `using PgWorker.Backups;` там дал бы CS0104.

**Выход:** контракт снимка несёт вид операции; сборка чистая.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` — Build succeeded, 0 Warning(s).

**Связь со spec:** §5.1 (контракты), §5.3 (ReceiverStatus), решение 13.

Подпункты:

- [ ] **3.1. Добавить константы вида операции** в файл `src/PgWorker.Backups/WalReceiverApiClient.cs`, сразу ПОСЛЕ закрывающей скобки record `ReceiverStatus` (перед record `StartPgDumpRequest`) вставь дословно:
```csharp
/// <summary>Константы вида ops-операции приёмника (arch/pgnode/02-api.md
/// §3): заполняет приёмник при TryBegin и каждом обновлении снимка;
/// полный бэкап (full-реестр) вида не имеет. Супервизоры сверяют вид со
/// своей операцией; снимок без вида (контракт старого приёмника) —
/// чужой для сверки (поставка воркера и приёмника единая).</summary>
public static class OperationKinds
{
    public const string Verify = "verify";
    public const string Restore = "restore";
    public const string Drill = "drill";
}
```

- [ ] **3.2. Добавить параметр Kind в OperationSnapshot** в файле `src/PgWorker.PgNode/Operations/OperationSnapshot.cs`: замени строки
```csharp
    // Сквозной sha256 tar-объекта полного (hex) — поле etcd-статуса sha256.
    string? Sha256 = null);
```
  на
```csharp
    // Сквозной sha256 tar-объекта полного (hex) — поле etcd-статуса sha256.
    string? Sha256 = null,
    // Вид ops-операции (OperationKinds; arch/pgnode/02-api.md §3):
    // verify/restore/drill — заполняет приёмник; у полного — null.
    // Update заменяет снимок целиком — операции обязаны переносить вид.
    string? Kind = null);
```

- [ ] **3.3. Добавить поле Kind в OpsStatusResponse и передачу из OpsStatus.** В файле `src/PgWorker.PgNode/Operations/ApiContracts.cs` замени закрывающую часть record `OpsStatusResponse`
```csharp
    [property: JsonPropertyName("started_unix")] long? StartedUnix = null,
    [property: JsonPropertyName("error")] string? Error = null);
```
  на
```csharp
    [property: JsonPropertyName("started_unix")] long? StartedUnix = null,
    [property: JsonPropertyName("error")] string? Error = null,
    // Вид операции реестра (arch/pgnode/02-api.md §3): сверка «своя/чужая»
    // супервизорами — по id И kind.
    [property: JsonPropertyName("kind")] string? Kind = null);
```
  В файле `src/PgWorker.PgNode/Program.cs` в методе `OpsStatus` (строки ~398–404) замени конструктор ответа
```csharp
            ? Results.Json(new OpsStatusResponse(
                    snapshot.State, snapshot.Phase, snapshot.Id,
                    snapshot.RestoredToLsn, snapshot.SystemId,
                    snapshot.StartedUnix, snapshot.Error),
```
  на
```csharp
            ? Results.Json(new OpsStatusResponse(
                    snapshot.State, snapshot.Phase, snapshot.Id,
                    snapshot.RestoredToLsn, snapshot.SystemId,
                    snapshot.StartedUnix, snapshot.Error, snapshot.Kind),
```

- [ ] **3.4. Добавить поле Kind в ReceiverStatus** в файле `src/PgWorker.Backups/WalReceiverApiClient.cs`: замени закрывающую часть record
```csharp
    [property: JsonPropertyName("bytes_done")] long? BytesDone = null,
    [property: JsonPropertyName("parts_done")] long? PartsDone = null,
    [property: JsonPropertyName("sha256")] string? Sha256 = null);
```
  на
```csharp
    [property: JsonPropertyName("bytes_done")] long? BytesDone = null,
    [property: JsonPropertyName("parts_done")] long? PartsDone = null,
    [property: JsonPropertyName("sha256")] string? Sha256 = null,
    // Вид операции реестра приёмника (arch/pgnode/02-api.md §3): сверка
    // «своя/чужая» супервизором — по id И kind; null (снимок старого
    // приёмника) трактуется как чужой.
    [property: JsonPropertyName("kind")] string? Kind = null);
```

- [ ] **3.5. Скомпилировать**: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` (в фоне, лог в `/tmp/t103d-build3.log`) — Build succeeded, 0 Warning(s). Коммит: `git add -A && git commit -m "contracts: operation kind in snapshot/status (t103 review)"`.

---

### Шаг 4. Операции приёмника и тестовые двойники: канал ShutdownAsync и kind в снимках (TDD)

**Вход:** шаг 3 закоммичен; `RestoreOperation.cs`/`VerifyOperation.cs`/`IReceiverOperation.cs` не менялись.

**Действие** (сильная модель: failing-тесты 4.1/4.5 → реализация 4.2/4.3 → тестовый двойник IntegrationTests 4.4 → verify-тесты 4.5 → прогон 4.6). Суть правок: `ShutdownAsync` — внутренний канал гашения (флаг `_shutdown` выставляется ДО `_cts.Cancel()`, catch OCE публикует `cancelled` только при операторской отмене); каждый `Update`/`Fail` операций несёт свой kind. Подпункт 4.4 обязателен до любого прогона IntegrationTests: правка интерфейса 4.2 оставляет единственный тестовый `IReceiverOperation` (FakeOp) нереализованным — проект не собирается (CS0535).

**Выход:** shutdown не публикует `cancelled`; операторский cancel — прежний контракт (в т.ч. verify); снимки операций несут kind; IntegrationTests собирается.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --filter "FullyQualifiedName~RestoreOperationTests|FullyQualifiedName~VerifyOperationTests"` — все PASS; `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/tests/PgWorker.IntegrationTests -c Release --nologo` — Build succeeded.

**Связь со spec:** §4.1 (п.5.4), §5.1, §5.5 (юниты); решения 10, 13.

Подпункты:

- [ ] **4.1. Добавить failing-юнит-тест гашения в `src/tests/PgWorker.UnitTests/PgNode/RestoreOperationTests.cs`** — сразу ПОСЛЕ метода `CancelDuringRecoveryPoll_StateCancelled_NotFailed` (его закрывающая скобка перед следующим `[Fact]`) вставь дословно:
```csharp
    // AAA (гашение при остановке хоста ≠ операторская отмена): shutdown
    // гасит задачу и postmaster, но снимок НЕ перезаписывается cancelled —
    // после смерти процесса реестра нет, воркер идёт vanished-путём 404.
    [Fact]
    public async Task ShutdownDuringRecoveryPoll_SnapshotNotCancelled()
    {
        // Arrange: psql-поллинг висит (гасится отменой); постгресс операции
        // ЖИВ (PgCtlStatusExit = 0) — гашение обязано его остановить.
        var root = NewPgdata();
        var pgdata = Path.Combine(root, "pgroot", "data");
        var runner = new FakeRunner { PsqlPollHang = true, PgCtlStatusExit = 0 };
        var registry = new OperationRegistry();
        registry.TryBegin(new OperationSnapshot(Id, "running", "downloading"));
        var op = new RestoreOperation(Request(), runner, registry,
            pgdata, _ => new FakeTarS3());
        var run = op.RunAsync(CancellationToken.None);
        SpinWait.SpinUntil(() => runner.Invocations.Any(i =>
                    i.FileName == "psql" && i.Args.Contains("pg_is_in_recovery")),
                TimeSpan.FromSeconds(10))
            .Should().BeTrue("the operation reaches the recovery poll before the shutdown");

        // Act: гашение при остановке хоста приёмника.
        await op.ShutdownAsync();
        await run;

        // Assert: снимок остался running своей фазы — никакого cancelled;
        // вид операции перенесён (Update не теряет kind).
        var snap = registrySnapshot(op)!;
        snap.State.Should().Be("running",
            "shutdown is an internal channel — the operator-cancel verdict is not published");
        snap.Error.Should().BeNull();
        snap.Kind.Should().Be("restore");
        // Assert: гашение остановило живой постгресс операции (stop fast).
        runner.Invocations.Should().Contain(i =>
            i.FileName == "pg_ctl" && i.Args.Contains("stop") && i.Args.Contains("fast"),
            "shutdown must stop the live postmaster of the operation");
        Directory.Delete(root, recursive: true);
    }
```
  Прогони фильтр `FullyQualifiedName~ShutdownDuringRecoveryPoll` — ожидание: ошибка компиляции «ShutdownAsync не определён» (это и есть failing-состояние TDD).

- [ ] **4.2. Реализовать канал в RestoreOperation и интерфейсе.** В файле `src/PgWorker.PgNode/Operations/IReceiverOperation.cs` замени тело интерфейса
```csharp
public interface IReceiverOperation
{
    Task RunAsync(CancellationToken ct);

    Task CancelAsync();
}
```
  на
```csharp
public interface IReceiverOperation
{
    Task RunAsync(CancellationToken ct);

    Task CancelAsync();

    /// <summary>Гашение при остановке хоста приёмника (arch/pgnode/
    /// 06-restore-drill.md §2): внутренний канал, ОТЛИЧНЫЙ от операторской
    /// отмены — гасит задачу и postmaster операции, но снимок НЕ
    /// перезаписывается (после смерти процесса реестра нет — воркер идёт
    /// vanished-путём 404 → повтор POST).</summary>
    Task ShutdownAsync();
}
```
  В файле `src/PgWorker.PgNode/Operations/RestoreOperation.cs`:
  1) после строки `private readonly CancellationTokenSource _cts = new();` вставь
```csharp
    private volatile bool _shutdown; // гашение хоста ≠ операторская отмена
```
  2) сразу ПОСЛЕ метода `CancelAsync` (его закрывающая скобка перед комментарием «// Обновление снимка с переносом started_unix») вставь
```csharp
    /// <summary>Гашение при остановке хоста приёмника (arch/pgnode/
    /// 06-restore-drill.md §2): тот же стоп, что у CancelAsync, но БЕЗ
    /// операторской семантики — снимок НЕ перезаписывается cancelled;
    /// флаг ставится ДО отмены, чтобы catch отмены различил каналы.</summary>
    public async Task ShutdownAsync()
    {
        _shutdown = true;
        _cts.Cancel();
        await StopBestEffortAsync(_pgdata);
    }
```
  3) в catch-блоке
```csharp
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            // Операторская отмена (cancel API, внешний канал): kill дерева уже
            // сделан runner'ом; pg_ctl stop best-effort; снимок state=cancelled —
            // НЕ failed (отмена = «операции не было», arch/backups/03-restore.md; воркер
            // отличает её от vanished: запись заявки/ключ дрилла удаляются).
            await StopBestEffortAsync(_pgdata);
            Update(OperationStates.Cancelled, phase, error: "cancelled by operator");
        }
```
  замени ПОСЛЕДНЮЮ строку блока на
```csharp
            // Гашение при остановке хоста (_shutdown) снимок НЕ перезаписывает.
            if (!_shutdown)
                Update(OperationStates.Cancelled, phase, error: "cancelled by operator");
```
  4) в методе `Update` замени
```csharp
        => _registry.Update(new OperationSnapshot(
            _request.Id ?? _request.BackupId, state, phase,
            RestoredToLsn: restoredToLsn, SystemId: systemId,
            StartedUnix: _registry.Current?.StartedUnix, Error: error));
```
  на
```csharp
        => _registry.Update(new OperationSnapshot(
            _request.Id ?? _request.BackupId, state, phase,
            RestoredToLsn: restoredToLsn, SystemId: systemId,
            StartedUnix: _registry.Current?.StartedUnix, Error: error,
            Kind: _isDrill ? OperationKinds.Drill : OperationKinds.Restore));
```

- [ ] **4.3. Реализовать канал в VerifyOperation и FullBackupOperation.** В файле `src/PgWorker.PgNode/Operations/VerifyOperation.cs`:
  1) после строки `private readonly CancellationTokenSource _cts = new();` вставь
```csharp
    private volatile bool _shutdown; // гашение хоста ≠ операторская отмена
```
  2) сразу ПОСЛЕ метода `CancelAsync` (перед комментарием «// Манифест из хвоста объекта») вставь
```csharp
    /// <summary>Гашение при остановке хоста приёмника (arch/pgnode/
    /// 06-restore-drill.md §2): прерывает чтение потока/сокета; снимок НЕ
    /// перезаписывается cancelled — операторская семантика только у
    /// cancel API.</summary>
    public Task ShutdownAsync()
    {
        _shutdown = true;
        _cts.Cancel();
        return Task.CompletedTask;
    }
```
  3) в catch-блоке
```csharp
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            // Операторская отмена: снимок cancelled — НЕ failed (отмена не
            // суждение о данных); воркер вернёт кандидата в PENDING.
            Update(OperationStates.Cancelled, CurrentPhase(), "cancelled by operator");
        }
```
  замени закрывающую строку вызова на
```csharp
            // Гашение при остановке хоста (_shutdown) снимок НЕ перезаписывает.
            if (!_shutdown)
                Update(OperationStates.Cancelled, CurrentPhase(), "cancelled by operator");
```
  4) в методе `Update` замени
```csharp
        => _registry.Update(new OperationSnapshot(
            _request.BackupId, state, phase,
            StartedUnix: _registry.Current?.StartedUnix, Error: error));
```
  на
```csharp
        => _registry.Update(new OperationSnapshot(
            _request.BackupId, state, phase,
            StartedUnix: _registry.Current?.StartedUnix, Error: error,
            Kind: OperationKinds.Verify));
```
  и в методе `Fail` того же файла замени
```csharp
        => _registry.Update(new OperationSnapshot(
            _request.BackupId, OperationStates.Failed, phase,
            StartedUnix: _registry.Current?.StartedUnix, Error: error));
```
  на
```csharp
        => _registry.Update(new OperationSnapshot(
            _request.BackupId, OperationStates.Failed, phase,
            StartedUnix: _registry.Current?.StartedUnix, Error: error,
            Kind: OperationKinds.Verify));
```
  В файле `src/PgWorker.PgNode/Operations/FullBackupOperation.cs` сразу ПОСЛЕ метода `CancelAsync` (его закрывающая скобка перед doc-комментарием `CopyStreamAsync`) вставь
```csharp
    /// <summary>Гашение при остановке хоста (интерфейс ops-канала,
    /// arch/pgnode/06-restore-drill.md §2): полный бэкап живёт в СВОЁМ
    /// реестре и ops-каналом не гасится — реализация отменяет задачу без
    /// операторской семантики (безопасна при любом вызове).</summary>
    public Task ShutdownAsync()
    {
        _cts.Cancel();
        return Task.CompletedTask;
    }
```

- [ ] **4.4. Расширить тестовый двойник IntegrationTests (компилируемость интерфейса).** В файле `src/tests/PgWorker.IntegrationTests/Backups/BackupApiOperationTests.cs` — FakeOp единственный тестовый `IReceiverOperation` этого проекта; после 4.2 без этого подпункта IntegrationTests не собирается (CS0535). Замени класс
```csharp
    private sealed class FakeOp(OperationRegistry? registry = null, string? id = null) : IReceiverOperation
    {
        public OperationSnapshot? Snapshot => null;
        public Task RunAsync(CancellationToken ct) => Task.CompletedTask;
        public Task CancelAsync()
        {
            registry?.Update(new OperationSnapshot(
                id ?? "op", "cancelled", "downloading", Error: "cancelled by operator"));
            return Task.CompletedTask;
        }
    }
```
  на
```csharp
    private sealed class FakeOp(OperationRegistry? registry = null, string? id = null) : IReceiverOperation
    {
        private readonly TaskCompletionSource _finished = new();

        // Счётчики каналов отмены (гашение хоста ≠ операторская отмена).
        public int CancelCalls;
        public int ShutdownCalls;

        public OperationSnapshot? Snapshot => null;

        public Task RunAsync(CancellationToken ct) => _finished.Task;

        public Task CancelAsync()
        {
            CancelCalls++;
            registry?.Update(new OperationSnapshot(
                id ?? "op", "cancelled", "downloading", Error: "cancelled by operator"));
            return Task.CompletedTask;
        }

        // Гашение при остановке хоста: снимок НЕ перезаписывается cancelled.
        public Task ShutdownAsync()
        {
            ShutdownCalls++;
            _finished.TrySetResult();
            return Task.CompletedTask;
        }

        // Завершение операции тестом (терминал + выход задачи).
        public void Finish(string state)
        {
            registry?.Update(new OperationSnapshot(id ?? "op", state, "downloading"));
            _finished.TrySetResult();
        }
    }
```
  В классе `FakeOpsFactory` замени строки
```csharp
        public IReceiverOperation CreateVerify(StartVerifyRequest request)
        {
            created.Add(request.BackupId);
            registry.Update(new OperationSnapshot(request.BackupId, "running", "downloading"));
            return new FakeOp(registry, request.BackupId);
        }

        public IReceiverOperation CreateRestore(StartRestoreRequest request)
        {
            created.Add(request.BackupId);
            registry.Update(new OperationSnapshot(request.Id ?? request.BackupId, "running", "recovering"));
            return new FakeOp(registry, request.Id ?? request.BackupId);
        }
```
  на
```csharp
        public FakeOp? Last { get; private set; }

        public IReceiverOperation CreateVerify(StartVerifyRequest request)
        {
            created.Add(request.BackupId);
            registry.Update(new OperationSnapshot(request.BackupId, "running", "downloading"));
            return Last = new FakeOp(registry, request.BackupId);
        }

        public IReceiverOperation CreateRestore(StartRestoreRequest request)
        {
            created.Add(request.BackupId);
            registry.Update(new OperationSnapshot(request.Id ?? request.BackupId, "running", "recovering"));
            return Last = new FakeOp(registry, request.Id ?? request.BackupId);
        }
```
  В методе `Build()` замени строку
```csharp
                services.AddSingleton<IOpsOperationFactory>(
                    new FakeOpsFactory(opsRegistry, created));
```
  на
```csharp
                var fakeOps = new FakeOpsFactory(opsRegistry, created);
                services.AddSingleton<IOpsOperationFactory>(fakeOps);
```
  и в кортеж возврата `Build()` добавь фабрику (порядок существующих элементов НЕ менять — добавить в конец перед `etcd`):
```csharp
        return (factory, factory.CreateClient(), fullRegistry, opsRegistry, created, fakeOps, etcd);
```
  с обновлением сигнатуры
```csharp
    private static (WebApplicationFactory<global::PgWorker.PgNode.Program> Factory, HttpClient Client,
        OperationRegistry FullRegistry, OpsOperationRegistry OpsRegistry, List<string> Created,
        FakeOpsFactory FakeOps, FakeEtcd Etcd) Build()
```
  и поправь ВСЕ существующие вызовы `Build()` в тестах класса: деструктуризации `var (_, client, _, _, _, _) = Build();` → `var (_, client, _, _, _, _, _) = Build();` (лишний `_`).
  Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/tests/PgWorker.IntegrationTests -c Release --nologo` (в фоне, лог в `/tmp/t103d-build44.log`) — Build succeeded.

- [ ] **4.5. Добавить юнит-тесты verify (гашение + операторская отмена) в `src/tests/PgWorker.UnitTests/PgNode/VerifyOperationTests.cs`.** Сначала расширь фейк `FakeTarS3` (файл тот же): после строки `public bool FailStreamRead { get; init; }` вставь
```csharp
        // Чтение тела висит до отмены токена (модель живого S3-чтения).
        public bool HangStreamRead { get; init; }

        // Счётчик открытых стримов — предусловие «операция дошла до verifying».
        public int StreamsOpened;
```
  в методе `GetObjectStreamAsync` замени
```csharp
        public Task<Result<(Stream Body, long Size)>> GetObjectStreamAsync(
            string cluster, string shard, string key, CancellationToken ct)
            => Task.FromResult(Result<(Stream Body, long Size)>.Success(
                (FailStreamRead ? new FailingStream() : new MemoryStream(tar), (long)tar.Length)));
```
  на
```csharp
        public Task<Result<(Stream Body, long Size)>> GetObjectStreamAsync(
            string cluster, string shard, string key, CancellationToken ct)
        {
            StreamsOpened++;
            Stream body = FailStreamRead ? new FailingStream()
                : HangStreamRead ? new HangingStream()
                : new MemoryStream(tar);
            return Task.FromResult(Result<(Stream Body, long Size)>.Success(
                (body, (long)tar.Length)));
        }
```
  и сразу ПОСЛЕ класса `FailingStream` (перед закрывающей скобкой файла) вставь
```csharp
    /// <summary>Поток, висящий на чтении до отмены токена (живое S3-чтение).</summary>
    private sealed class HangingStream : Stream
    {
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override async ValueTask<int> ReadAsync(
            Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            return 0;
        }

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
```
  Затем в класс тестов (после любого `[Fact]`-метода, например после `ValidSet_Succeeded_RangeFromZero`) вставь ДВА теста:
```csharp
    // AAA (гашение при остановке хоста ≠ операторская отмена): чтение
    // прерывается, снимок НЕ перезаписывается cancelled (операторская
    // семантика — только у cancel API).
    [Fact]
    public async Task ShutdownDuringVerifyPass_SnapshotNotCancelled()
    {
        // Arrange: валидный набор; чтение тела verifying висит до отмены.
        var tar = BuildTar([Version, Control, Label, Segment], [Version, Control, Label]);
        var s3 = new FakeTarS3(tar) { HangStreamRead = true };
        var op = Create(s3);
        var run = op.RunAsync(CancellationToken.None);
        SpinWait.SpinUntil(() => s3.StreamsOpened > 0, TimeSpan.FromSeconds(10))
            .Should().BeTrue("the operation reaches the verifying pass before the shutdown");

        // Act: гашение при остановке хоста приёмника.
        await op.ShutdownAsync();
        await run;

        // Assert: снимок остался running своей фазы — никакого cancelled;
        // вид операции перенесён (Update не теряет kind).
        var snap = op.Snapshot!;
        snap.State.Should().Be("running",
            "shutdown is an internal channel — the operator-cancel verdict is not published");
        snap.Error.Should().BeNull();
        snap.Kind.Should().Be("verify");
    }

    // AAA (операторский канал остаётся операторским): cancel API verify
    // публикует cancelled с канонической причиной (регресс-гвард
    // разделения каналов — парный к ShutdownDuringVerifyPass).
    [Fact]
    public async Task CancelDuringVerifyPass_StateCancelled()
    {
        // Arrange: тот же висящий verifying-проход.
        var tar = BuildTar([Version, Control, Label, Segment], [Version, Control, Label]);
        var s3 = new FakeTarS3(tar) { HangStreamRead = true };
        var op = Create(s3);
        var run = op.RunAsync(CancellationToken.None);
        SpinWait.SpinUntil(() => s3.StreamsOpened > 0, TimeSpan.FromSeconds(10))
            .Should().BeTrue("the operation reaches the verifying pass before the cancel");

        // Act: операторская отмена (cancel API).
        await op.CancelAsync();
        await run;

        // Assert: снимок cancelled с канонической причиной.
        var snap = op.Snapshot!;
        snap.State.Should().Be("cancelled",
            "the operator channel keeps publishing the operator verdict");
        snap.Error.Should().Be("cancelled by operator");
    }
```

- [ ] **4.6. Прогнать серию**: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --filter "FullyQualifiedName~RestoreOperationTests|FullyQualifiedName~VerifyOperationTests"` (в фоне, лог в `/tmp/t103d-unit4.log`) — все PASS, включая новые `ShutdownDuring*`/`CancelDuringVerifyPass` и существующий `CancelDuringRecoveryPoll_StateCancelled_NotFailed`. Коммит: `git add -A && git commit -m "pgnode ops: shutdown channel distinct from operator cancel; kind in snapshots (t103 review)"`.

---

### Шаг 5. Program.cs MapOpsRoutes: время жизни active и гварды живости

**Вход:** шаг 4 зелёный (ShutdownAsync в интерфейсе; IntegrationTests собирается).

**Действие** (сильная модель: подпункты 5.1–5.4 дословны). Суть: пара `active`/`activeId`; continuation-обнуление с гвартом ReferenceEquals; хук и cancel-маршрут гасят/отменяют ТОЛЬКО running-снимок именно этой операции; TryBegin-снимки несут kind.

**Выход:** завершённая операция недостижима для хука/cancel (404), shutdown зовёт ShutdownAsync; снимки запуска несут kind.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` — Build succeeded, 0 Warning(s); поведение — тесты шага 8.

**Связь со spec:** §5.2; решения 10, 11.

Подпункты:

- [ ] **5.1. Добавить using-alias и заменить блок active/хук** в `src/PgWorker.PgNode/Program.cs`. После строки `using PgWorker.Etcd.S3;` (блок using-ов вверху файла) вставь
```csharp
using OperationKinds = PgWorker.Backups.OperationKinds;
```
  (именно ALIAS, НЕ `using PgWorker.Backups;` — namespace экспортирует типы, одноимённые с `PgWorker.PgNode.Operations` (`ErrorResponse`, `StartVerifyRequest`, `StartRestoreRequest` и др.), обычный using дал бы неоднозначность CS0104 по всему файлу). В методе `MapOpsRoutes` замени строки ОТ `// Активная операция для cancel (снимок — в реестре, экземпляр — здесь).` КОНЦАМ хука (закрывающая `});` блока `app.Lifetime.ApplicationStopping.Register`) включительно на
```csharp
        // Активная операция для cancel/shutdown (снимок — в реестре,
        // экземпляр — здесь); activeId — id её TryBegin-снимка (гвард
        // «running-снимок именно этой операции»).
        IReceiverOperation? active = null;
        string? activeId = null;

        // Остановка хоста гасит ЖИВУЮ ops-операцию (arch/pgnode/
        // 06-restore-drill.md §2): ShutdownAsync — внутренний канал,
        // ОТЛИЧНЫЙ от операторской отмены: гасит задачу (runner убивает
        // дерево child-процессов) и останавливает postmaster операции, но
        // снимок НЕ перезаписывается (после смерти процесса реестра нет —
        // воркер идёт vanished-путём 404 → повтор POST). Завершённая
        // операция (терминальный снимок/обнулённая ссылка) не гасится.
        // Ожидание ≤15 c; жёсткий SIGKILL supervisord закроет остаток
        // защитой каталога при следующем запуске.
        var opsRegistry = app.Services.GetRequiredService<OpsOperationRegistry>();
        app.Lifetime.ApplicationStopping.Register(() =>
        {
            try
            {
                if (active is not { } op || activeId is not { } id)
                    return;
                if (opsRegistry.Current is not { State: OperationStates.Running } cur || cur.Id != id)
                    return;
                op.ShutdownAsync()
                    .WaitAsync(TimeSpan.FromSeconds(15))
                    .GetAwaiter().GetResult();
            }
            catch
            {
                // best-effort: нестопаемый остаток подберёт защита каталога
            }
        });
```

- [ ] **5.2. TryBegin c kind и continuation-обнуление в POST /api/ops/verify.** В том же методе замени строки хендлера ОТ `if (!registry.TryBegin(new OperationSnapshot(` (первое вхождение — verify) ДО `_ = Task.Run(() => operation.RunAsync(CancellationToken.None));` (первое вхождение) включительно на
```csharp
            if (!registry.TryBegin(new OperationSnapshot(
                    body.BackupId, OperationStates.Running, OperationPhases.Downloading,
                    StartedUnix: DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                    Kind: OperationKinds.Verify)))
                return Results.Json(
                    new ErrorResponse("operation already active"), JsonOpts, statusCode: 409);

            var operation = factory.CreateVerify(body);
            active = operation;
            activeId = body.BackupId;
            // Ссылка живёт до завершения задачи операции; ReferenceEquals не
            // затирает более новую (arch/pgnode/06-restore-drill.md §2).
            _ = Task.Run(() => operation.RunAsync(CancellationToken.None))
                .ContinueWith(_ =>
                {
                    if (ReferenceEquals(active, operation))
                    {
                        active = null;
                        activeId = null;
                    }
                });
```

- [ ] **5.3. То же в POST /api/ops/restore.** Замени строки второго хендлера ОТ `if (!registry.TryBegin(new OperationSnapshot(` (второе вхождение — restore, с `body.Id ?? body.BackupId`) ДО `_ = Task.Run(() => operation.RunAsync(CancellationToken.None));` (второе вхождение) включительно на
```csharp
            if (!registry.TryBegin(new OperationSnapshot(
                    body.Id ?? body.BackupId, OperationStates.Running, OperationPhases.Downloading,
                    StartedUnix: DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                    Kind: body.Id is null ? OperationKinds.Restore : OperationKinds.Drill)))
                return Results.Json(
                    new ErrorResponse("operation already active"), JsonOpts, statusCode: 409);

            var operation = factory.CreateRestore(body);
            active = operation;
            activeId = body.Id ?? body.BackupId;
            // Ссылка живёт до завершения задачи операции; ReferenceEquals не
            // затирает более новую (arch/pgnode/06-restore-drill.md §2).
            _ = Task.Run(() => operation.RunAsync(CancellationToken.None))
                .ContinueWith(_ =>
                {
                    if (ReferenceEquals(active, operation))
                    {
                        active = null;
                        activeId = null;
                    }
                });
```

- [ ] **5.4. Гвард живости в cancel-маршруте.** Замени тело маршрута
```csharp
        app.MapPost("/api/ops/cancel", async (OpsOperationRegistry registry) =>
        {
            if (registry.Current is null || active is null)
                return Results.Json(
                    new ErrorResponse("no active operation"), JsonOpts, statusCode: 404);
            await active.CancelAsync();
            return Results.Json(new { state = "cancelling" }, JsonOpts, statusCode: 202);
        });
```
  на
```csharp
        app.MapPost("/api/ops/cancel", async (OpsOperationRegistry registry) =>
        {
            // Отмена — ТОЛЬКО живой (running) операции этой ссылки
            // (arch/pgnode/06-restore-drill.md §2): завершённая не
            // отменяется и не останавливается повторно.
            if (registry.Current is not { State: OperationStates.Running } cur
                || active is not { } op || activeId != cur.Id)
                return Results.Json(
                    new ErrorResponse("no active operation"), JsonOpts, statusCode: 404);
            await op.CancelAsync();
            return Results.Json(new { state = "cancelling" }, JsonOpts, statusCode: 202);
        });
```
  Скомпилируй: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` (в фоне, лог в `/tmp/t103d-build5.log`) — Build succeeded. Коммит: `git add -A && git commit -m "pgnode: ops active lifetime guard; hook calls ShutdownAsync; TryBegin carries kind (t103 review)"`.

---

### Шаг 6. BackupVerifyProcess: сверка id + kind (TDD)

**Вход:** шаг 3 закоммичен (ReceiverStatus.Kind); шаг 4 завершён (правка интерфейса требует 4.4 для сборки IntegrationTests).

**Действие** (сильная модель: failing-тест 6.1 → реализация+хелперы 6.2–6.3 → прогон 6.4).

**Выход:** снимок чужого вида (или без kind) не даёт verify-вердикта — реланч vanished-семантикой; свои снимки несут kind.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter "FullyQualifiedName~BackupVerifyProcessTests"` — все PASS.

**Связь со spec:** §5.3; решение 13.

Подпункты:

- [ ] **6.1. Добавить failing-тест чужого вида с тем же id** в `src/tests/PgWorker.IntegrationTests/Backups/BackupVerifyProcessTests.cs` — сразу ПЕРЕД методом `ForeignFailedSnapshot_CandidateUntouched_Relaunched` вставь (скелет — копия `ForeignSucceededSnapshot_NotMarkedOk`, свой префикс кластера `vf3`):
```csharp
    // AAA (сверка вида операции): restore-снимок ТОГО ЖЕ полного (id
    // совпадает, kind чужой) — не итог проверки: OK не ставится, операция
    // перезапущена vanished-семантикой.
    [Fact]
    public async Task ForeignKindSameId_NotMarkedOk()
    {
        // Arrange: PENDING-кандидат и его ключ; приёмник отдаёт succeeded-
        // снимок restore-операции С ТЕМ ЖЕ id (kind=restore).
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("vf3");
        var s3 = new FakeBackupS3();
        var candidate = Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null));
        var key = "/pgworker/backups/vf3/shard1/full/20260911120000Z";
        var before = BackupStatusJson.Serialize(candidate);
        await Fx.Gateway.PutAsync(Fx.Endpoint, key, before, null, ct);
        Receiver.GetReply = new ReceiverReply(200,
            new ReceiverStatus("succeeded", "recovering", "20260911120000Z",
                Kind: OperationKinds.Restore), null);
        var outcomes = new List<(string C, string S, string Result)>();
        var process = BuildProcess("vf3", s3, verifyObserver: (c, s, r) => outcomes.Add((c, s, r)));

        // Act: тик поллинга — снимок чужого вида.
        (await process.TickAsync(BuildSnap("vf3"), BackupsOf("vf3", candidate), ct))
            .IsSuccess.Should().BeTrue();

        // Assert: verify НЕ получил OK — ключ не изменён (PENDING).
        var after = (await Fx.Gateway.GetAsync(Fx.Endpoint, key, ct)).Value!.Value;
        after.Should().Be(before, "a restore snapshot of the same full is not the verify outcome");

        // Assert: реланч своего кандидата, observe ok не эмитился.
        Receiver.StartBodies.Should().ContainSingle()
            .Which.BackupId.Should().Be("20260911120000Z");
        outcomes.Should().NotContain(o => o.Result == "ok");
    }
```
  Прогони фильтр `FullyQualifiedName~ForeignKindSameId_NotMarkedOk` — ожидание: FAIL (ложный OK: сверка пока только по id).

- [ ] **6.2. Реализовать сверку вида.** В файле `src/PgWorker.Backups/Process/BackupVerifyProcess.cs` замени блок сверки
```csharp
        if (!string.Equals(snapshot.Id, full.Id, StringComparison.Ordinal))
        {
```
  на
```csharp
        if (!string.Equals(snapshot.Id, full.Id, StringComparison.Ordinal)
            || snapshot.Kind != OperationKinds.Verify)
        {
```
  и в том же блоке строку лога
```csharp
                "{Op} {cluster}/{shard}: verify {id} — registry holds a foreign snapshot (id={SnapId}, state={State}); relaunching via the vanished path",
                Op, cluster, shard, id, snapshot.Id ?? "<null>", snapshot.State);
```
  на
```csharp
                "{Op} {cluster}/{shard}: verify {id} — registry holds a foreign snapshot (id={SnapId}, kind={Kind}, state={State}); relaunching via the vanished path",
                Op, cluster, shard, id, snapshot.Id ?? "<null>", snapshot.Kind ?? "<null>", snapshot.State);
```

- [ ] **6.3. Дать своим снимкам вид verify** в `src/tests/PgWorker.IntegrationTests/Backups/BackupVerifyProcessTests.cs` (иначе после 6.2 существующие тесты с подходящим id станут «чужими»). Замени три хелпера перед классом `FakeReceiverClient`:
```csharp
    private static ReceiverReply ApiOk(string id = "20260911120000Z") =>
        new(200, new ReceiverStatus("succeeded", "verifying", id), null);
    private static ReceiverReply ApiDownloadTransient(string error, string id = "20260911120000Z") =>
        new(200, new ReceiverStatus("failed", "downloading", id, Error: error), null);
    private static ReceiverReply ApiVerifyFailed(string error, string id = "20260911120000Z") =>
        new(200, new ReceiverStatus("failed", "verifying", id, Error: error), null);
```
  на
```csharp
    private static ReceiverReply ApiOk(string id = "20260911120000Z") =>
        new(200, new ReceiverStatus("succeeded", "verifying", id, Kind: OperationKinds.Verify), null);
    private static ReceiverReply ApiDownloadTransient(string error, string id = "20260911120000Z") =>
        new(200, new ReceiverStatus("failed", "downloading", id, Error: error, Kind: OperationKinds.Verify), null);
    private static ReceiverReply ApiVerifyFailed(string error, string id = "20260911120000Z") =>
        new(200, new ReceiverStatus("failed", "verifying", id, Error: error, Kind: OperationKinds.Verify), null);
```
  Плюс ТРИ одиночных снимка со СВОИМ id (тесты с кандидатом `20260911090000Z` и cancelled-тест): в строках с
```csharp
            new ReceiverStatus("cancelled", "verifying", "20260911120000Z", Error: "cancelled by operator"), null);
```
  добавь `Kind: OperationKinds.Verify` перед закрывающей скобкой статуса;
```csharp
            new ReceiverStatus("running", "verifying", Id: "20260911090000Z", StartedUnix: now - 7 * 3600), null);
```
  → добавь `, Kind: OperationKinds.Verify`;
```csharp
            new ReceiverStatus("running", "downloading", "20260911090000Z"), null);
```
  → `new ReceiverStatus("running", "downloading", "20260911090000Z", Kind: OperationKinds.Verify), null);`.
  Снимки vf1/vf2 (чужой id `20261106060228Z`) и дефолты FakeReceiverClient (без id, GET не читает) НЕ трогать.

- [ ] **6.4. Прогнать серию**: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter "FullyQualifiedName~BackupVerifyProcessTests"` (в фоне, лог в `/tmp/t103d-vf.log`) — все PASS (новый kind-кейс зелёный, vf1/vf2 и остальные — без регресса). Упавший существующий из-за отсутствия kind у снимка со своим id — правь снимок теста (добавь `Kind: OperationKinds.Verify`), НЕ прод-код. Коммит: `git add -A && git commit -m "backups verify: match snapshot id and kind (t103 review)"`.

---

### Шаг 7. RestoreProcess: сверка id + kind и demolished-повтор (TDD)

**Вход:** шаг 3 закоммичен; шаг 4 завершён (сборка IntegrationTests).

**Действие** (сильная модель: переписанные failing-тесты 7.1, новый 7.2 → реализация 7.3 → kind у своих снимков 7.4 → прогон 7.5).

**Выход:** чужой снимок (id/kind/без kind) сбрасывает Phase в demolished и повторяет POST; снимки вида verify не дают REJOINING/FAILED.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter "FullyQualifiedName~RestoreProcessTests"` — все PASS.

**Связь со spec:** §5.4; решения 12, 13, 14.

Подпункты:

- [ ] **7.1. Переписать fr1** в `src/tests/PgWorker.IntegrationTests/Backups/RestoreProcessTests.cs`: в методе `ForeignSucceededSnapshot_RequestNotRejoining` (кластер `fr1`) замени блок ассертов
```csharp
        // Assert — заявка RUNNING/recovering без system_id; POST не слался.
        var request = (await ReadRestoresAsync("fr1", "shard1")).Single(r => r.Id == op.Id);
        request.State.Should().Be(RestoreStatus.Running,
            "a foreign succeeded snapshot is not the outcome of this restore");
        request.Phase.Should().Be("recovering");
        request.SystemId.Should().BeNull();
        _receiver.StartCalls.Should().Be(0,
            "the launch returns through the 409/vanished mechanics, not from the supervise tick");
```
  на
```csharp
        // Assert — заявка RUNNING с фазой demolished (сброс под повтор
        // команды); вердикта нет, system_id не тронут.
        var request = (await ReadRestoresAsync("fr1", "shard1")).Single(r => r.Id == op.Id);
        request.State.Should().Be(RestoreStatus.Running,
            "a foreign succeeded snapshot is not the outcome of this restore");
        request.Phase.Should().Be("demolished",
            "the launch is repeated via the demolished reset, not awaited in the supervise phase");
        request.SystemId.Should().BeNull();

        // Act 2 — повторный тик из demolished повторяет POST запуска.
        (await process.TickAsync(BuildSnap("fr1"), await BackupsFromEtcdAsync("fr1"), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 2 — команда переотправлена (своя операция стартует заново).
        _receiver.StartCalls.Should().Be(1,
            "the next tick from the demolished phase re-sends the launch command");
```

- [ ] **7.2. Переписать fr2 и добавить kind-кейс.** В методе `ForeignFailedSnapshot_RequestNotFailed` (кластер `fr2`) замени блок ассертов
```csharp
        // Assert — заявка RUNNING/recovering без error; POST не слался.
        var request = (await ReadRestoresAsync("fr2", "shard1")).Single(r => r.Id == op.Id);
        request.State.Should().Be(RestoreStatus.Running,
            "a foreign failed snapshot does not fail this restore");
        request.Phase.Should().Be("recovering");
        request.Error.Should().BeNull();
        _receiver.StartCalls.Should().Be(0);
```
  на
```csharp
        // Assert — заявка RUNNING с фазой demolished; FAILED не ставится.
        var request = (await ReadRestoresAsync("fr2", "shard1")).Single(r => r.Id == op.Id);
        request.State.Should().Be(RestoreStatus.Running,
            "a foreign failed snapshot does not fail this restore");
        request.Phase.Should().Be("demolished",
            "the launch is repeated via the demolished reset, not awaited in the supervise phase");
        request.Error.Should().BeNull();

        // Act 2 — повторный тик из demolished повторяет POST запуска.
        (await process.TickAsync(BuildSnap("fr2"), await BackupsFromEtcdAsync("fr2"), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 2 — команда переотправлена.
        _receiver.StartCalls.Should().Be(1);
```
  Сразу ПОСЛЕ метода `ForeignFailedSnapshot_RequestNotFailed` (перед закрывающей скобкой класса) вставь новый тест (скелет — копия fr1; кластер `fr3`, свой id заявки):
```csharp
    // AAA (сверка вида операции): verify-снимок ТОГО ЖЕ полного (id
    // совпадает с backup_id заявки, kind чужой) — не итог restore:
    // REJOINING не ставится, фаза сбрасывается под повторный запуск.
    [Fact]
    public async Task ForeignKindSameBackupId_NotRejoining()
    {
        // Arrange — RUNNING (фаза recovering); GET succeeded с backup_id
        // заявки, но видом verify (без system_id).
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("fr3");
        var process = BuildProcess(new FakeBackupS3(),
            new TestDriver(new StubScaleDriver(), new FakeBackupEngine()));
        _receiver.GetReply = new ReceiverReply(200,
            new ReceiverStatus("succeeded", "verifying", "20260910120000Z",
                Kind: OperationKinds.Verify), null);
        var op = await SeedRestoreAsync("fr3", "shard1", "20260911121017Z",
            backupId: "20260910120000Z", state: RestoreStatus.Running,
            startedUnix: TimeProvider.System.GetUtcNow().ToUnixTimeSeconds() - 60,
            phase: "recovering",
            phaseUnix: TimeProvider.System.GetUtcNow().ToUnixTimeSeconds() - 50);
        (await _claims.TryClaimClusterAsync("fr3", ct)).Value.Should().BeTrue();

        // Act — тик супервиза: снимок чужого вида с тем же id.
        (await process.TickAsync(BuildSnap("fr3"), await BackupsFromEtcdAsync("fr3"), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — заявка RUNNING/demolished без system_id (не REJOINING).
        var request = (await ReadRestoresAsync("fr3", "shard1")).Single(r => r.Id == op.Id);
        request.State.Should().Be(RestoreStatus.Running,
            "a verify snapshot of the same full is not the outcome of this restore");
        request.Phase.Should().Be("demolished");
        request.SystemId.Should().BeNull();

        // Act 2 + Assert 2 — повторный тик переотправляет команду.
        (await process.TickAsync(BuildSnap("fr3"), await BackupsFromEtcdAsync("fr3"), ct))
            .IsSuccess.Should().BeTrue();
        _receiver.StartCalls.Should().Be(1);
    }
```
  Прогони фильтры `FullyQualifiedName~ForeignSucceededSnapshot|FullyQualifiedName~ForeignFailedSnapshot|FullyQualifiedName~ForeignKindSameBackupId` — ожидание: НОВЫЙ FAIL (REJOINING по id-совпадению), переписанные fr1/fr2 FAIL (фаза ещё recovering).

- [ ] **7.3. Реализовать сверку вида и demolished-сброс.** В файле `src/PgWorker.Backups/Process/RestoreProcess.cs` замени блок
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
  на
```csharp
        // Сверка снимка по id и виду операции (arch/backups/processes/
        // f-restore.md §2): ops-реестр приёмника ОДИН на процесс — GET
        // может вернуть чужой/прошлый снимок (напр., терминал
        // verify/drill-операции ТОГО ЖЕ полного: id совпадает, вид — нет).
        // Чужой id ИЛИ чужой вид (в т.ч. снимок без kind — контракт старого
        // приёмника) — чужой итог/фаза: переходов заявки по нему нет
        // (REJOINING от чужого succeeded без system_id запер бы bootstrap).
        // Фаза сбрасывается в "demolished" + transient — команда повторится
        // следующим тиком (симметрично ветке vanished 404; живая чужая
        // операция ответит 409 — семантика освобождения реестра).
        if (!string.Equals(status.Id, op.BackupId, StringComparison.Ordinal)
            || status.Kind != OperationKinds.Restore)
        {
            var putReset = await PutStatusAsync(cluster, shard.Name, op with { Phase = "demolished" }, ct);
            if (!putReset.IsSuccess)
                return Result<ProcessOutcome>.Failed(putReset.Error!);
            return await TransientAsync(cluster, $"foreign-snapshot/{shard.Name}/{op.Id}",
                $"receiver registry holds operation {status.Id ?? "<null>"} (kind {status.Kind ?? "<null>"}, {status.State}) — not this restore", ct);
        }
```

- [ ] **7.4. Дать своим снимкам вид restore** в том же тест-файле: во ВСЕХ одиночных `new ReceiverStatus(` с id `"20260910120000Z"` (строки существующих тестов: succeeded/recovering с `RestoredToLsn`, failed/recovering с `Error: "boom"`, running/recovering, running/downloading, cancelled/recovering, failed/downloading `Error: "s3 throttled"`, succeeded с `RestoredToLsn: "0/42"`) добавь именованный аргумент `Kind: OperationKinds.Restore` (к `Error`/`RestoredToLsn`-вариантам — после них через запятую; к коротким `new ReceiverStatus("running", "recovering", "20260910120000Z"), null)` → `new ReceiverStatus("running", "recovering", "20260910120000Z", Kind: OperationKinds.Restore), null)`). Снимки fr1/fr2 (чужой id), дефолты FakeReceiverClient (без id — GET не читает) и два running-снимка без id (читаются только вне GET-супервиза) НЕ трогать.

- [ ] **7.5. Прогнать серию**: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter "FullyQualifiedName~RestoreProcessTests"` (в фоне, лог в `/tmp/t103d-fr.log`) — все PASS. Упавший существующий из-за отсутствия kind у снимка со своим id — правь снимок (добавь `Kind: OperationKinds.Restore`), НЕ прод-код. Коммит: `git add -A && git commit -m "backups restore: match snapshot id and kind; foreign snapshot resets phase to demolished (t103 review)"`.

---

### Шаг 8. API-тесты MapOpsRoutes: shutdown-хук и cancel-after-terminal

**Вход:** шаги 4–5 закоммичены; тестовые двойники (FakeOp/FakeOpsFactory/Build) уже расширены подпунктом 4.4 — этот шаг добавляет ТОЛЬКО тесты.

**Действие** (сильная модель: подпункты 8.1–8.3 дословны). Суть: два новых теста — «остановка хоста гасит живую операцию без cancelled» и «cancel после терминала — 404 без вызова».

**Выход:** хук и гварды живости закодированы тестами (краснеют при откате шага 5).

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter "FullyQualifiedName~BackupApiOperationTests"` — все PASS.

**Связь со spec:** §5.5 (интеграционные), решения 10, 11.

Подпункты:

- [ ] **8.1. Добавить тест остановки хоста** в `src/tests/PgWorker.IntegrationTests/Backups/BackupApiOperationTests.cs` — в конец класса (перед закрывающей скобкой) вставь:
```csharp
    // AAA (гашение при остановке хоста ≠ операторская отмена): StopAsync
    // хоста гасит ЖИВУЮ операцию каналом ShutdownAsync — снимок НЕ
    // перезаписывается cancelled; воркер после смерти процесса идёт
    // vanished-путём 404, не операторской веткой.
    [Fact]
    public async Task HostShutdown_ActiveOp_NotCancelled()
    {
        var ct = TestContext.Current.CancellationToken;
        // Arrange: живая verify-операция (фейк висит в RunAsync).
        var (factory, client, _, opsRegistry, _, fakeOps, _) = Build();
        var body = new StartVerifyRequest(Id,
            new S3Context(["http://s3a:9000"], "ak", "sk", "backups", "c1/s1"));
        ((int)(await client.PostAsJsonAsync("/api/ops/verify", body, ct)).StatusCode)
            .Should().Be(202);
        var op = fakeOps.Last!;
        await using var _ = factory;

        // Act: остановка хоста приёмника.
        await factory.Server.Host.StopAsync(ct);

        // Assert: гашение каналом shutdown, БЕЗ операторской ветки.
        op.ShutdownCalls.Should().Be(1, "host shutdown must go through the shutdown channel");
        op.CancelCalls.Should().Be(0, "host shutdown is not an operator cancel");
        opsRegistry.Current!.State.Should().Be("running",
            "shutdown does not publish cancelled — the registry dies with the process");
    }
```

- [ ] **8.2. Добавить тест cancel-after-terminal** — следом вставь:
```csharp
    // AAA (отмена только живой операции): завершённая операция (терминал +
    // выход задачи) недоступна для cancel — 404 без вызова CancelAsync;
    // повторный stop/restart pgnode не шлёт стоп по PGDATA завершённого
    // restore.
    [Fact]
    public async Task CancelAfterTerminal_Returns404_NoCancelCall()
    {
        var ct = TestContext.Current.CancellationToken;
        // Arrange: операция дошла до терминала и завершила задачу.
        var (factory, client, _, opsRegistry, _, fakeOps, _) = Build();
        await using var _ = factory;
        var body = new StartVerifyRequest(Id,
            new S3Context(["http://s3a:9000"], "ak", "sk", "backups", "c1/s1"));
        ((int)(await client.PostAsJsonAsync("/api/ops/verify", body, ct)).StatusCode)
            .Should().Be(202);
        var op = fakeOps.Last!;
        op.Finish("succeeded");
        SpinWait.SpinUntil(() => opsRegistry.Current?.State == "succeeded",
            TimeSpan.FromSeconds(10)).Should().BeTrue();

        // Act: операторская отмена завершённой операции.
        var cancel = await client.PostAsync("/api/ops/cancel", content: null, ct);

        // Assert: 404, канал отмены не звался.
        ((int)cancel.StatusCode).Should().Be(404,
            "a terminal operation is not cancellable — the active reference is gone");
        op.CancelCalls.Should().Be(0);
    }
```

- [ ] **8.3. Прогнать серию**: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter "FullyQualifiedName~BackupApiOperationTests"` (в фоне, лог в `/tmp/t103d-api.log`) — все PASS. Коммит: `git add -A && git commit -m "pgnode api tests: host shutdown channel and cancel-after-terminal guard (t103 review)"`.

---

### Шаг 9. Верификация: серии и E2E-гейт

**Вход:** шаги 1–8 закоммичены; docker-хост свободен (нет поднятого dev-станда и остатков серий).

**Действие** (сильная модель; все команды — в фоне, лог каждой серии — в файл `/tmp/t103d-<series>.log`; после каждой серии — дожидаться финальной строки и зачищать остатки `docker ps -a`/`docker network ls`; между сериями — только own-only чистка):
1. Юниты: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release` — 0 failed.
2. Зачистка → интеграционные: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release` — 0 failed.
3. Статическая подготовка E2E: `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release` — штамп готовности.
4. Сборка (ОТДЕЛЬНОЙ командой, свежесть бинаря — часть гейта): `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` — Build succeeded, 0 warnings.
5. E2E (только `--no-build`): `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.E2eTests -c Release --no-build --filter "FullyQualifiedName~Drill_|FullyQualifiedName~Backup_ChainBroken_SelfHeals"` — все Passed (E2E-сценарий доработкой не менялся — регресс-гейт Р1: удаление ключа дрилла при stop pgnode невозможно).
6. Итог серии: зачистка остаточных контейнеров/сетей прогона; упавший тест НЕ перезапускать — разбор по логам (`/tmp/pgw-e2e-artifacts-*`, `$TMPDIR` тоже проверить), фикс причины, затем повтор ШАГОМ целиком.

**Выход:** все серии зелёные на свежем Release; финальный коммит ветки (роадмап-гейт t103 исполнен ранее — отдельных действий нет; убедиться `grep -rn "t103" arch/ AGENTS.md docs/ --exclude-dir=superpowers` пуст вне допустимых мест).

**Проверка:** итоговые строки серий: `Failed: 0` (юниты/интеграция), `Passed: N, Failed: 0` (E2E); `git status` чистый.

**Связь со spec:** §6.5, критерии 6–8.

---

## Порядок исполнения и зависимости

Шаги 1–2 (канон) — первыми, строго до кода (arch-first). Шаг 3 (контракт kind) — после 1–2, до 4/5/6/7. Шаг 4 (операции) — после 3; подпункт 4.4 восстанавливает сборку IntegrationTests (правка интерфейса 4.2 ломает единственный тестовый IReceiverOperation) — поэтому шаги 6/7 обязаны идти ПОСЛЕ шага 4, хотя их прод-код зависит только от шага 3. Шаг 5 (Program) — после 4 (нужен ShutdownAsync в интерфейсе). Шаг 6 (verify-супервиз) и шаг 7 (restore-супервиз) — после 4. Шаг 8 (API-тесты) — после 4–5 (двойники готовы в 4.4, гварды — в шаге 5). Шаг 9 — после всех. Каждый шаг завершается коммитом в feature-ветке; шаги 1 и 2 — отдельные коммиты канона ДО кодовых.
