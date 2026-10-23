# t02-pool-failover-selfheal — план реализации

> **Для исполнителя (dev-flow, Фаза 6):** план исполняет субагент coder —
> сильная модель, работает один и делает ВСЮ работу сам, включая подпункты
> (для него подпункты — обычные инструкции). Подпункты оформлены как
> самодостаточные задания для слабой модели (без контекста проекта) — формат
> сохраняется на случай смены режима исполнения. Шаги отмечаются чекбоксами
> (`- [ ]`); выполненные шаги помечены `[x]` с фактическими коммитами.

**Статус (честная летопись):** шаги 1–5 ВЫПОЛНЕНЫ и закоммичены (HEAD
`3f22ceb8`); отладка дефекта B вскрыла два независимых корня шторма
пересъёмов (A — дубляж `wal/<seg>/<seg>` файловой `mc cp --recursive`; B —
соло-листинг в drill/verify против канона union) — оба закрыты в ветке, spec
дополнен (решения 15–18, п. 4.4.2/4.5/4.6/4.7, критерий 8.4 переформулирован
по достижимому). Осталось: docs-коммит канона/spec/плана (шаг 6), host-лог
итога verify (шаг 7), повтор серий (шаг 8), мерж-гейт (шаг 9).

**Цель:** E2E-сценарий `Pool_FailoverList_And_Exhaustion_SelfHeal` стабильно
сходится: после исчерпания write-списка пула (DEGRADED + маркер
`s3-pool-exhausted`) и подъёма ноды контроль возвращает wal-статус в ACTIVE в
бюджете гейта; устранены вечный цикл пересъёмов полных (корни A/B +
checkpoint=fast) и медленная реакция контроля на исцеление (контроль каждый
тик при exhaustion-DEGRADED); итог verify каждого полного наблюдаем из
host-лога воркера (arch/19 §2 «Наблюдаемость итога verify»).

**Архитектура:** (1) Позиционный детектор `IsPoolExhaustionActive` — маркер
актуален, пока последняя `pool_exhausted`-строка лога мастер-агента новее
последней `heartbeat`-строки; оба t36-блока контроля на детекторе, чтение
лога — один docker-вызов за проход; exhaustion-DEGRADED — контроль каждый
тик, симметрично BROKEN. (2) Пересъёмы: `--checkpoint=fast` (spread-дыра) +
`upload_file` без `--recursive` (корень A: ключ `wal/<seg>` ровно) +
`PoolBackupS3.ListUnionAsync` в drill/verify (корень B: union по всем нодам
пула). (3) `BackupVerifyProcess`: строка host-лога с итогом verify
(OK/FAILED + причина) при фиксации итога каждого полного. Arch-first: канон
`arch/19-backups.md` — до кода, в истории ветки.

**Стек:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`),
xUnit + FluentAssertions, интеграционные тесты на реальном etcd (fake
docker/SQL/S3-драйверы), docker-E2E.

**Spec:** `docs/superpowers/2026-10-23-t02-pool-failover-selfheal/spec.md`
(план аргументирует от spec — исполнитель читает оба).

**Worktree:** `/Users/demakaev/ZCodeProject/worktrees/feat-t02-pool-failover-selfheal`
(вся работа — только внутри; ветка `feat-t02-pool-failover-selfheal`).

## Глобальные ограничения (spec §6 — действуют на каждый шаг)

- НЕ менять: формат stdout-маркеров приёмника (`WalReceiverMarkers`), контракт
  wal-ключа etcd (поле `error` как было), механику breaker/перебора
  `PoolBackupS3`, инвариант подтверждения-по-доставке, тестовый сценарий AC4.
- `AgentLogTail = 200` не трогать; никаких таймеров/порогов свежести маркера —
  только позиционное сравнение строк.
- Union-листинг НЕ распространять на прочие листинги: канон arch/19 §5 делает
  union исключением для контроля цепочки WAL и verify/drill (`wal/`,
  `full/<id>/pg_wal/`); инвентарь-листинги остаются соло, точечные GET —
  перебор до найденного.
- Контракт `upload` каталогов (`mc cp --recursive`) не меняется; правка только
  файлового случая (сегменты `wal/`).
- Юнит-тесты — комментарии AAA; документация/комментарии — по-русски,
  идентификаторы — на английском.
- Сборка и запуск тестов — двумя ОТДЕЛЬНЫМИ командами; тесты — только с
  `--no-build` и env-префиксом при самой команде; перед docker-E2E —
  `dotnet run --project src/tests/PgWorker.E2ePrepare -c Release`; зачистка
  контейнеров/сетей после КАЖДОЙ серии.
- Коммиты — в feature-ветке свободно (в main — только по явной команде
  пользователя).

## Review Focus

Для оставшегося кода (шаг 7 — verify-лог) — классы входов/отказов, которые
spec подразумевает, но которые основные тесты не проверяют; каждый закрыт
тестом:

1. Итог OK от джоба (exit 0 + `ok:true`) — строка host-лога с
   `<C>/<X>` и id → тест `SupervisorOkLogsOutcomeToHostLog` (шаг 7).
2. Итог FAILED от джоба (`phase=verify`, причина `pg_verifybackup`) — строка
   с причиной → тест `SupervisorVerifyPhaseFailedLogsOutcome` (шаг 7).
3. Итог FAILED цепочечной проверки (без джоба; причина — границы дыры) —
   строка с причиной → тест `ChainFailedLogsOutcome` (шаг 7).
4. Ровно ОДИН итог на полный: transient-пути verify (S3-fail,
   download-retry, verify-timeout) итоговой строки НЕ пишут — итог только
   терминальный OK/FAILED → ассерт `HaveCount(1)` в
   `ChainFailedLogsOutcome` (шаг 7).
5. Регресс-база выполненных шагов (детектор/контроль/расписание, union,
   `upload_file`) остаётся зелёной после правки verify-лога → серии шага 8
   (AC4 ×2 + пул AC1–AC8 + WAL + Scale) и юниты шага 7.

---

### Шаг 1. [x] ВЫПОЛНЕН. Канон arch/19 §3 + §2(checkpoint=fast)

**Коммит:** `7aa138cb` — «docs(arch): arch/19 — актуальность маркера
s3-pool-exhausted (снятие heartbeat'ом, контроль каждый тик) и
pg_basebackup --checkpoint=fast».

Сделано по исходному плану (п. 4.1, 4.2.1 spec): §3 — абзац «Правила
непрерывности» дополнен актуальностью маркера (снятие heartbeat'ом) и
контролем каждый тик для exhaustion-DEGRADED; §2 — строка «Инструмент»
переведена на `--checkpoint=fast` с обоснованием. Канон в истории ДО кода.
Незакоммиченное расширение §2 («Наблюдаемость итога verify», п. 4.2.2 spec) —
фиксируется шагом 6.

### Шаг 2. [x] ВЫПОЛНЕН. Детектор `IsPoolExhaustionActive` + юнит-тесты

**Коммит:** `d315abe8` — «feat(backups): WalStreamProcess — позиционный
детектор актуальности pool_exhausted (снятие heartbeat'ом) + юнит-тесты».

Сделано по исходному плану (п. 4.3.1, 4.7 spec): `InternalsVisibleTo
PgWorker.IntegrationTests`, детектор после `ParseTliJumpMarker` (матч —
подстрока `pool_exhausted` с подчёркиванием; дефисная форма
`s3-pool-exhausted` в result-JSON не матчится), 5 тестов
`PoolExhaustionDetector_*` в `WalStreamProcessTests.cs`.

### Шаг 3. [x] ВЫПОЛНЕН. Контроль: детектор в t36-блоках, одно чтение, расписание

**Коммиты:** `14191d92` — «fix(backups): WalStreamProcess — детектор
актуальности pool_exhausted в t36-блоках, одно чтение лога за проход,
контроль каждый тик при exhaustion-DEGRADED»; `9b74d2a9` — «test(backups):
ExhaustionDegradedEveryTickHealsInOneTick — прогревочный тик заполняет
_lastVerifyUnix (тест дискриминирует bypass расписания: RED без bypass,
GREEN с ним)».

Сделано по исходному плану (п. 4.3.2, 4.3.3 spec) + усиление отладки:
тест расписания получил прогревочный тик (заполнение `_lastVerifyUnix` до
целевого) — теперь реально доказывает bypass расписания для
exhaustion-DEGRADED (без правила — RED). Тесты
`ExhaustionMarker_ThenHeartbeatHealsActive`,
`ExhaustionDegradedEveryTickHealsInOneTick`,
`ExhaustionMarker_AgentLogReadFailure_NoPhantomDegradation` — в
`WalStreamProcessTests.cs`.

### Шаг 4. [x] ВЫПОЛНЕН. Entrypoint: checkpoint=fast + файловая mc cp (корень A)

**Коммиты:** `c8204e83` — «fix(backup): pg_basebackup --checkpoint=fast …»
(п. 4.4.1 spec); `cf2d1ded` — «fix(backup): дубли сегментов в wal/ — файловая
mc cp без --recursive (ключ wal/<seg> ровно; --recursive на файле добавлял
basename: wal/<seg>/<seg> невидим union-листингу — вечный gap drill/verify и
шторм пересъёмов)» (п. 4.4.2 spec, решение 15); попутно `e6e58d09` —
«fix(docker): wal-образы на базе aspnet:10.0 — транзитивный
FrameworkReference AspNetCore (Backups → Shared.Metrics) требует ASP.NET Core
runtime» (фикс сборки образов, выявлен отладкой E2E).

Сделано: строка 24 `--checkpoint=fast`; функция `upload_file` — `mc cp` БЕЗ
`--recursive` для сегментов в `wal/` (ключ `wal/<seg>` ровно); прежний
`upload` с `--recursive` остаётся для каталога `full/<id>/`. Один скрипт на
оба Dockerfile. Отладкой установлено (решение 18): fast НЕ был фактической
причиной шторма (корни A/B), но механика spread-дыры реальна — правка
остаётся, отката нет.

### Шаг 5. [x] ВЫПОЛНЕН. Union-листинг drill/verify (корень B)

**Коммит:** `3f22ceb8` — «fix(backups): union-листинг в drill/verify вместо
соло первой живой ноды …» (п. 4.5 spec, решение 16).

Сделано: `PoolBackupS3.ListUnionAsync(cluster, shard, prefix)` — обобщение
`ListWalUnionAsync` (делегирует с `"wal/"`): union по всем read-целям, дедуп
по имени, успех ≥1 узла, исчерпание — маркер `s3-pool-exhausted`;
`BackupVerifyProcess` — оба листинга цепочки на union (`wal/` +
`full/<id>/pg_wal/`); `RestoreDrillProcess` — валидационный листинг на
`ListWalUnionAsync`; `TickShardAsync` принимает `PoolBackupS3`. Юниты union —
в `src/tests/PgWorker.UnitTests/Backups/PoolBackupS3Tests.cs` (уже в ветке,
п. 4.7 spec). Попутно `49bb4cc8` — docs(roadmap): заведён ОТДЕЛЬНЫЙ баг
`t04-tligap-writeifchanged-flaky` (юнит-фейл на границе секунд, вне области
t02 — правило «фейл вне области — отдельная задача»).

**Фактические итоги шагов 1–5 на HEAD `3f22ceb8`:** юниты 72/72; AC4 ×2
зелёные (без шторма пересъёмов); регресс пул AC1–AC8 / WAL / Scale —
зелёный. Повтор серий после шага 7 — шаг 8.

---

### Шаг 6. Docs-коммит: канон §2 (verify-лог) + spec + план (arch-first для шага 7)

**Вход:** шаги 1–5 закоммичены (HEAD `3f22ceb8`); в рабочем дереве
незакоммичены правки `arch/19-backups.md` (§2 «Наблюдаемость итога verify»)
и `docs/superpowers/2026-10-23-t02-pool-failover-selfheal/spec.md`
(корни A/B, п. 4.4.2/4.5/4.6/4.7, решения 15–18, критерий 8.4); план
обновлён этим файлом.

**Действие** (сильная модель, подпунктов нет): docs-коммит по паттерну
`0b8a6470` (`docs(superpowers): …`), включающий ТРИ файла:
`arch/19-backups.md`, `docs/superpowers/2026-10-23-t02-pool-failover-selfheal/spec.md`,
`docs/superpowers/2026-10-23-t02-pool-failover-selfheal/plan.md`.
Сообщение: `docs(superpowers): t02 — корни A/B шторма пересъёмов и host-лог
итога verify в spec/plan; arch/19 §2 — наблюдаемость итога verify`. Канон §2
(verify-лог) в истории ДО кода шага 7 — arch-first, критерий 8.5.

**Выход:** канон «Наблюдаемость итога verify» и дополненный spec зафиксированы
коммитом; рабочее дерево чистое (кроме предстоящего кода шага 7).

**Проверка:** `git status --short` — пусто; `git show --stat HEAD` — ровно
три файла; `git log --oneline -- arch/19-backups.md | head -1` — docs-коммит
раньше коммита кода `BackupVerifyProcess` (появится в шаге 7).

**Связь со spec:** п. 4.2.2, решения 15–18 (летопись), критерий 8.5.

### Шаг 7. Host-лог итога verify в `BackupVerifyProcess` (TDD)

**Вход:** шаг 6 закоммичен (канон arch/19 §2 «Наблюдаемость итога verify» в
истории до кода).

**Действие** (сильная модель): тесты до/вместе с реализацией (TDD, фаза 6
spec). Механика — подпункты 7.1–7.4; coder исполняет сам и ревьюит. Три
точки фиксации терминального итога в `src/PgWorker.Backups/Process/BackupVerifyProcess.cs`
(итог = OK/FAILED; transient-пути итог НЕ пишут):
- `SuperviseAsync`, ветка OK (exit 0 + `ok:true`) — после journal-фазы
  `verified-ok/{shard}/{id}`: `LogInformation` «verify {id} OK»;
- `SuperviseAsync`, ветка FAILED от джоба (`phase=verify`) — после
  journal-фазы `verify-failed/{shard}/{id}`: `LogError` с причиной `err`;
- `WritePermanentAsync` (цепочечная проверка: дыра, нет wal_start, битое
  имя) — после journal-фазы `verify-failed/{shard}/{full.Id}`: `LogError` с
  причиной `error`.

Формат строк — по образцу существующей строки
`logger.LogInformation("{Op} {cluster}/{shard}: verify job {job} started (id {id})", …)`
(п. 4.6 spec): `<Op> <C>/<X>: verify <id> OK|FAILED: <причина>`; OK →
`LogInformation`, FAILED → `LogError`. Джоб-checksums и цепочечная проверка
сводятся к одному итогу на полный (текст шага 7.3 — точный).

**Выход:** итог каждого verify наблюдаем из host-лога воркера (канон §2);
три пути итога покрыты тестами со spy-логгером; сборка без warnings.

**Проверка:**
1) `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` — успешно;
2) `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~BackupVerifyProcessTests" > /tmp/t02-step7-unit.log 2>&1; echo "exit=$?"` —
   `exit=0`, все зелёные (новые +3: `SupervisorOkLogsOutcomeToHostLog`,
   `SupervisorVerifyPhaseFailedLogsOutcome`, `ChainFailedLogsOutcome`);
   регресса существующих нет.

**Связь со spec:** п. 4.6, фаза 6, решение 17; критерий 8.4 (п. «итог
каждого verify наблюдаем в host.log»).

**Подпункты (слабой модели; coder исполняет сам):**

- [ ] **7.1. Spy-логгер и параметризация `BuildProcess` в
  `src/tests/PgWorker.IntegrationTests/Backups/BackupVerifyProcessTests.cs`.**
  1. Сразу ПЕРЕД строкой `    internal sealed class FakeVerifyEngine : IDockerEngine`
  добавь вложенный в класс тестов spy-логгер:

```csharp
    // ── t02 (arch/19 §2 «Наблюдаемость итога verify»): spy-логгер — снимает
    //    форматированные строки (итог verify наблюдаем из host-лога воркера) ──
    internal sealed class SpyLogger : Microsoft.Extensions.Logging.ILogger<BackupVerifyProcess>
    {
        public readonly List<string> Lines = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(
            Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId,
            TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => Lines.Add(formatter(state, exception));
    }
```

  2. Найди в этом же файле метод `BuildProcess`:

```csharp
    private BackupVerifyProcess BuildProcess(
        string cluster, FakeVerifyDriver driver, IBackupS3 s3, BackupsRuntimeOptions? options = null,
        Action<string, string, string>? verifyObserver = null)
        => new(
```

  и замени его сигнатуру (тело не трогай) на:

```csharp
    private BackupVerifyProcess BuildProcess(
        string cluster, FakeVerifyDriver driver, IBackupS3 s3, BackupsRuntimeOptions? options = null,
        Action<string, string, string>? verifyObserver = null,
        Microsoft.Extensions.Logging.ILogger<BackupVerifyProcess>? logger = null)
        => new(
```

  а в теле — строку

```csharp
            Microsoft.Extensions.Logging.Abstractions.NullLogger<BackupVerifyProcess>.Instance,
```

  замени на:

```csharp
            logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<BackupVerifyProcess>.Instance,
```

- [ ] **7.2. Добавить три юнит-теста итога-в-лог в
  `src/tests/PgWorker.IntegrationTests/Backups/BackupVerifyProcessTests.cs`.**
  В самом конце класса `BackupVerifyProcessTests` (перед его закрывающей `}` —
  последняя строка файла) добавь:

```csharp
    // ---- t02 (arch/19 §2 «Наблюдаемость итога verify»): итог — строка host-лога ----

    // AAA: итог OK от джоба (exit 0 + ok:true) — строка host-лога воркера с
    // кластером/шардом и id полного (журнал остаётся летописью после
    // deprovisioning'а etcd-ключей).
    [Fact]
    public async Task SupervisorOkLogsOutcomeToHostLog()
    {
        // Arrange — джоб exited с ok:true; spy-логгер снимает строки
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("hl1");
        var s3 = new FakeBackupS3();
        var engine = await StartJobAsync("hl1", s3);
        var name = "pgw-backup-verify-hl1-shard1-20260911120000Z";
        engine.Containers[name] = engine.Containers[name] with { State = "exited", ExitCode = 0, Logs = "{\"ok\":true}" };
        var logger = new SpyLogger();
        var process = BuildProcess("hl1", new FakeVerifyDriver(engine), s3, logger: logger);

        // Act — тик супервиза итога
        (await process.TickAsync(BuildSnap("hl1"), BackupsOf("hl1",
            Completed("20260911120000Z", "000000010000000000000001",
                new BackupVerify(BackupVerifyStatus.Pending, null))), ct)).IsSuccess.Should().BeTrue();

        // Assert — строка итога OK с <C>/<X> и id полного
        logger.Lines.Should().Contain(l =>
            l.Contains("hl1/shard1", StringComparison.Ordinal)
            && l.Contains("verify 20260911120000Z OK", StringComparison.Ordinal));
    }

    // AAA: итог FAILED от джоба (phase=verify, ненулевой pg_verifybackup) —
    // строка host-лога с причиной из verify.error.
    [Fact]
    public async Task SupervisorVerifyPhaseFailedLogsOutcome()
    {
        // Arrange — джоб exited с result-JSON phase=verify
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("hl2");
        var s3 = new FakeBackupS3();
        var engine = await StartJobAsync("hl2", s3);
        var name = "pgw-backup-verify-hl2-shard1-20260911120000Z";
        engine.Containers[name] = engine.Containers[name] with
        {
            State = "exited", ExitCode = 1,
            Logs = "{\"ok\":false,\"phase\":\"verify\",\"error\":\"checksum mismatch failed\"}",
        };
        var logger = new SpyLogger();
        var process = BuildProcess("hl2", new FakeVerifyDriver(engine), s3, logger: logger);

        // Act — тик супервиза итога
        (await process.TickAsync(BuildSnap("hl2"), BackupsOf("hl2",
            Completed("20260911120000Z", "000000010000000000000001",
                new BackupVerify(BackupVerifyStatus.Pending, null))), ct)).IsSuccess.Should().BeTrue();

        // Assert — строка итога FAILED с причиной
        logger.Lines.Should().Contain(l =>
            l.Contains("verify 20260911120000Z FAILED", StringComparison.Ordinal)
            && l.Contains("checksum mismatch failed", StringComparison.Ordinal));
    }

    // AAA: итог FAILED цепочечной проверки (без джоба) — строка host-лога с
    // причиной (границы дыры); РОВНО один итог на полный — transient-пути
    // итоговой строки не пишут.
    [Fact]
    public async Task ChainFailedLogsOutcome()
    {
        // Arrange — wal/: 1,3 (дыра на 2); набор pg_wal/3; spy-логгер
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("hl3");
        var engine = new FakeVerifyEngine();
        var driver = new FakeVerifyDriver(engine);
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("hl3", "shard1", "000000010000000000000001"));
        s3.Objects.Add(("hl3", "shard1", "000000010000000000000003"));
        s3.PrefixedObjects.Add(("hl3", "shard1", "full/20260911120000Z/pg_wal/000000010000000000000003"));
        var logger = new SpyLogger();
        var process = BuildProcess("hl3", driver, s3, logger: logger);

        // Act — тик: дыра цепочки → permanent FAILED без джоба
        (await process.TickAsync(BuildSnap("hl3"), BackupsOf("hl3",
            Completed("20260911120000Z", "000000010000000000000001",
                new BackupVerify(BackupVerifyStatus.Pending, null))), ct)).IsSuccess.Should().BeTrue();

        // Assert — ровно одна итоговая строка FAILED с границей дыры
        logger.Lines.Should().ContainSingle(l =>
                l.Contains("verify 20260911120000Z FAILED", StringComparison.Ordinal)
                && l.Contains("000000010000000000000002", StringComparison.Ordinal),
            "exactly one terminal outcome line per full — transient paths log no outcome");
    }
```

  Используемые хелперы (`OwnEtcd.StartAsync`, `SeedAsync`, `StartJobAsync`,
  `BuildProcess`, `BuildSnap`, `BackupsOf`, `Completed`, `FakeVerifyEngine`,
  `FakeVerifyDriver`, `FakeBackupS3`) уже есть в этом файле выше — новых
  using не нужно. Тесты станут зелёными после подпункта 7.3.

- [ ] **7.3. Добавить строки итога в `src/PgWorker.Backups/Process/BackupVerifyProcess.cs`.**
  Три точечные вставки (каждая — сразу ПОСЛЕ указанного вызова
  `journal.WritePhaseAsync`, по образцу строки «verify job … started»):

  1. В методе `SuperviseAsync` найди ветку OK:

```csharp
            await journal.WritePhaseAsync(cluster, Op, $"verified-ok/{shard}/{id}",
                claims.InstanceId, null, ct);
```

  и сразу после неё добавь строку:

```csharp
            logger.LogInformation("{Op} {cluster}/{shard}: verify {id} OK", Op, cluster, shard, id);
```

  2. Там же в `SuperviseAsync` найди ветку FAILED от джоба:

```csharp
            await journal.WritePhaseAsync(cluster, Op, $"verify-failed/{shard}/{id}",
                claims.InstanceId, err, ct);
```

  и сразу после неё добавь строку:

```csharp
            logger.LogError("{Op} {cluster}/{shard}: verify {id} FAILED: {Error}", Op, cluster, shard, id, err);
```

  3. В методе `WritePermanentAsync` найди:

```csharp
        await journal.WritePhaseAsync(cluster, Op, $"verify-failed/{shard}/{full.Id}",
            claims.InstanceId, error, ct);
```

  и сразу после неё добавь строку:

```csharp
        logger.LogError("{Op} {cluster}/{shard}: verify {full.Id} FAILED: {Error}", Op, cluster, shard, full.Id, error);
```

  Больше в файле ничего не менять (transient-пути — без итоговой строки).

- [ ] **7.4. Скомпилировать и прогнать тесты.**
  Из корня репозитория по очереди:
  1) `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
     — ожидаемый результат: сборка успешна, 0 ошибок, 0 warnings;
  2) `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~BackupVerifyProcessTests" > /tmp/t02-step7-unit.log 2>&1; echo "exit=$?"`
     — ожидаемый результат: `exit=0`, «Passed! - Failed: 0»; новые
     `SupervisorOkLogsOutcomeToHostLog`, `SupervisorVerifyPhaseFailedLogsOutcome`,
     `ChainFailedLogsOutcome` зелёные; все существующие тесты
     `BackupVerifyProcessTests` зелёные (регресса нет).

**Коммит шага 7:** `feat(backups): BackupVerifyProcess — итог verify (OK/FAILED + причина) строкой host-лога воркера (arch/19 §2) + юнит-тесты`.

---

### Шаг 8. Серии: повтор после verify-лога (сборка Release, юниты, E2ePrepare, docker-E2E)

**Вход:** шаг 7 закоммичен (код воркера изменился — серии обязаны пройти на
свежем бинаре).

**Действие** (сильная модель — наблюдение, разбор, зачистка; подпунктов
нет). Протокол прежний: команды из корня worktree; вывод каждой команды —
ПОЛНОСТЬЮ в файл лога; запуск в фоне с фиксацией task_id; мониторинг — старт
в течение 15 с по логу, далее проверка каждые 2 минуты; env-префикс В САМОЙ
команде; после КАЖДОЙ серии — зачистка (`docker ps -a --filter name=pgw-` —
пусто; осиротевшие `kfw-net-*`/`pgw-*-net` при нуле контейнеров —
`docker network prune -f`); следующая серия — только после финальной строки
предыдущей и зачистки.

Замечание по образам: шаг 7 меняет только код C# (Dockerfile/entrypoint не
трогаются); тем не менее `E2ePrepare` обязателен перед сериями по правилам
AGENTS.md — он пересоберёт e2e-образы с новым publish воркера (инкрементально)
и впишет штамп готовности.

Порядок:

1. Сборка (отдельная команда): `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo > /tmp/t02-s8-build.log 2>&1; echo "exit=$?"` — `exit=0`.
2. Юниты затронутых областей: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~BackupVerifyProcessTests" > /tmp/t02-s8-unit-verify.log 2>&1; echo "exit=$?"` — `exit=0`.
3. E2ePrepare ДО docker-серий: `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release > /tmp/t02-s8-prepare.log 2>&1; echo "exit=$?"` — `exit=0`, штамп готовности. Затем `docker network prune -f`.
4. AC4 — ДВА повторных прогона (критерий 8.2), между ними зачистка:
   `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter FullyQualifiedName~Pool_FailoverList_And_Exhaustion_SelfHeal > /tmp/t02-s8-ac4-run1.log 2>&1; echo "exit=$?"`
   затем та же команда с логом `run2`. Ожидание: оба `exit=0`.
5. Пул-регресс AC1–AC8 (критерий 8.3):
   `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~E2e.Pool" > /tmp/t02-s8-pool.log 2>&1; echo "exit=$?"` — `exit=0`.
6. WAL-стрим серии: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~E2eWalStream" > /tmp/t02-s8-walstream.log 2>&1; echo "exit=$?"` — `exit=0`.
7. Кейс-маркер: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter FullyQualifiedName~Scale_AddEmptyShard > /tmp/t02-s8-scale.log 2>&1; echo "exit=$?"` — `exit=0`.
8. Разбор артефактов зелёного AC4-прогона по критерию 8.4 (БЕЗ перезапуска
   тестов; каждое утверждение — со ссылкой на файл/строку лога, фиксируется в
   журнале задачи; артефакты — `$TMPDIR/pgw-e2e-artifacts-*`, host.log воркера
   и docker-логи):
   a. контрольный переход `Degraded→Active` шарда после подъёма ноды — строка
      host.log воркера `control DEGRADED→ACTIVE` (grep -n);
   b. пересъёмов НЕТ: нет серий запусков полных каждые ~10 c — строки
      старта полных в host.log разрежены (только по расписанию/`IsDue`);
      подсчёт с таймингами;
   c. drill/verify без WAL chain gap: в артефактах нет строк `WAL chain gap`
      и `set contains no pg_wal WAL segments` (grep по host.log/journal;
      пусто);
   d. verify-джобы стартуют штатно: строки `verify job … started` в host.log;
   e. итог каждого verify наблюдаем в host.log: строки
      `verify <id> OK` / `verify <id> FAILED: <причина>` (новый лог шага 7);
   f. `verify.state=OK` — если verify успевает в окно телеметрии сценария;
      PENDING к концу Act B — допустимый исход (наблюдаемость обеспечивает
      host.log-итог, решение 17).

Упавший тест: teardown помечает контур `MarkFailed()` — контейнер
останавливается, но не удаляется; разбор по `README-cleanup.txt` артефактов,
ручная зачистка ПОСЛЕ разбора. Перезапуск упавших без полного анализа логов
ЗАПРЕЩЁН. Фейл серии вне области задачи — пункт в `arch/roadmap/bugs.md`
(прецедент `t04` из шага 5), работа продолжается.

**Выход:** все серии зелёные на свежем бинаре с verify-логом; все шесть
утверждений 8.4 подтверждены ссылками на строки логов.

**Проверка:** `exit=0` каждой серии; п. 8a–8f выполнены и зафиксированы.

**Связь со spec:** фаза 7; критерии 8.1–8.4 (8.4 — в новой редакции);
решение 17 (достижимость `verify.state=OK`, PENDING допустим).

**Коммит шага 8:** правок кода нет; при необходимости точечных правок после
разбора — отдельные коммиты с повтором затронутых серий.

---

### Шаг 9. Мерж-гейт: удаление пункта t02 из роадмапа (тем же коммитом, что и мерж в main)

**Вход:** шаг 8 завершён, все серии зелёные; пользователь ЯВНО потребовал
мерж в main (без этого шаг не исполняется — AGENTS.base п. 7).

**Действие** (сильная модель):
1. В `arch/roadmap/bugs.md` удалить пункт `t02-pool-failover-selfheal`
   ЦЕЛИКОМ (блок от строки `- **\`t02-pool-failover-selfheal\`** — …` до
   конца пункта включительно). Из списков/`←`-зависимостей других пунктов тег
   удалить, если остался (проверка: `grep -rn "t02-pool-failover-selfheal" arch/roadmap/`).
2. Удалить локальную подпапку логов
   `arch/roadmap/bugs/t02-pool-failover-selfheal/`, ЕСЛИ она существует
   локально (правило трека bugs: пункт удаляется вместе со своей подпапкой).
   Подпапка ВНЕ git (в worktree её нет — артефакты прогона лежат в `$TMPDIR`,
   spec п. 4.8); отсутствие — не ошибка, шаг пропускается.
3. Правки arch/19 (шаги 1 и 6) уже в ветке — входят в мерж-коммит; история
   фиксирует канон до кода для ОБЕИХ правок §2 (checkpoint=fast и host-лог
   итога verify) — критерий 8.5.

**Выход:** пункт t02 удалён из роадмапа тем же коммитом, что и мерж в main;
локальная подпапка логов (если была) удалена.

**Проверка:** в мерж-коммите diff `arch/roadmap/bugs.md` — только удаление
пункта t02; `grep -c "t02-pool-failover-selfheal" arch/roadmap/bugs.md` → 0;
`ls arch/roadmap/bugs/t02-pool-failover-selfheal/ 2>/dev/null` — пусто.

**Связь со spec:** п. 4.8; критерий 8.5 (обе правки §2 — в истории до кода).

---

## Самопроверка плана (исполнена составителем; каскад правки учтён)

- **Покрытие spec:** 4.1→шаг 1 [x]; 4.2.1→шаг 1 [x]; 4.2.2→шаг 6 (канон,
  уже в рабочем дереве) + шаг 7 (код); 4.3→шаги 2–3 [x]; 4.4.1→шаг 4 [x];
  4.4.2→шаг 4 [x] (`cf2d1ded`); 4.5→шаг 5 [x] (`3f22ceb8`, юниты union в
  ветке); 4.6→шаг 7; 4.7→шаги 2–3 [x] + шаг 5 [x] + шаг 7 (verify-лог);
  4.8→шаг 9; фазы 1–5→шаги 1–5 [x]; фаза 6→шаг 7; фаза 7→шаг 8; решения
  1–18 отражены (15→`upload_file`; 16→union; 17→verify-лог + достижимость
  8.4; 18→fast остаётся, откат нет). Шагов вне scope spec нет.
- **Развилки (шаги 6–9):** тексты трёх строк лога и места вставок — точные
  (после соответствующих `journal.WritePhaseAsync`, по образцу started-строки;
  OK → `LogInformation`, FAILED → `LogError` — п. 4.6 spec); spy-логгер и
  параметризация `BuildProcess` — дословные (образец `IDisposable?
  BeginScope` из `NullLogger`, компилируется под `TreatWarningsAsErrors`);
  имена тестов/кластеров (`hl1`–`hl3`) — из свободных; docs-коммит — три
  файла, сообщение по паттерну `0b8a6470`, ДО кода шага 7 (arch-first,
  критерий 8.5); серии — фильтры из реальных имён классов; критерий 8.4 —
  шесть утверждений с привязкой к строкам логов, PENDING допустим (решение
  17). Точек выбора при реализации не остаётся.
- **Типы/сигнатуры** (шаг 7): сверены с кодом — `BackupVerifyProcess`
  принимает `ILogger<BackupVerifyProcess> logger` (конструктор, строка 35);
  вставки в `SuperviseAsync` (OK-ветка после `verified-ok/{shard}/{id}`,
  FAILED-ветка после `verify-failed/{shard}/{id}` с `err`) и в
  `WritePermanentAsync` (после `verify-failed/{shard}/{full.Id}` с `error`,
  параметры `cluster`/`shard`/`full` доступны); тестовые хелперы
  `OwnEtcd.StartAsync`, `SeedAsync`, `StartJobAsync`, `BuildProcess`,
  `BuildSnap`, `BackupsOf`, `Completed`, `FakeVerifyEngine.Containers`
  (record `ContainerRec` — `with`-мутация), `FakeVerifyDriver`,
  `FakeBackupS3` — существуют в `BackupVerifyProcessTests.cs`;
  `BackupVerify(BackupVerifyStatus.Pending, null)` — реальный конструктор.
- **E2E-гигиена:** сборка отдельной командой; `--no-build` + env-префикс при
  команде; E2ePrepare до серий (пересоберёт образы с новым кодом воркера);
  зачистка после каждой серии; перезапуск упавших — только после разбора
  логов.
