# t13-backup-minor-hardening — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Цель:** устранить две точки хрупкости подсистемы бэкапов из code-review t07 — форс-мьют `LastUploadedUnix!.Value` в ветке «слот исчез» WalStreamProcess (A1) и возрастной бюджет после transient-гвардов в `BackupProcess.SuperviseActiveAsync` (A2).

**Архитектура:** два точечных фикса без изменения контракта etcd: (A1) defensive-фолбэк `clock-сейчас` для параметра `baseUnix` + journal-заметка `wal-key-invalid/<X>` при битом ключе; (A2) перенос возрастного гварда `IsTimedOut` в начало итерации супервизии (вердикт `FAILED job-timeout` — факт etcd, docker-доступ не нужен) + cleanup best-effort с journal-пометкой при недоступном источнике. Arch-first: сперва уточнения канона `arch/19-backups.md` (§3/§6), затем TDD-код.

**Стек:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`), xUnit + FluentAssertions; интеграция — testcontainers-etcd (`EtcdFixture`, общий контейнер коллекции).

**Spec:** [`spec.md`](spec.md) (этот же каталог). План аргументируется от спеки; исполнители читают оба документа.

## Глобальные ограничения

- Контракт etcd (`arch/19` §4) НЕ меняется: новые поля/форматы/состояния не вводятся; AdminPanel не затрагивается (spec §2, §3.4, §5).
- Никаких новых конфиг-опций: бюджеты `Job:FullTimeoutSec` и паттерны journal — существующие (spec §5).
- Поведение при целых инвариантах — идентично сегодняшнему, кроме порядка вычисления `nowUnix`; существующие тесты остаются зелёными без правки ожиданий (spec §1, AC4).
- `RestoreProcess`/`BackupVerifyProcess` — вне скоупа (spec §3.4).
- Гварды парсеров wal-ключа (`BackupsParser.TryParseWal`, `WalStatusWriter.Parse`) — не трогаем (spec §3.4).
- Все правки — ТОЛЬКО в worktree `/Users/demakaev/ZCodeProject/worktrees/feat-t13-backup-minor-hardening` (ветка `feat-t13-backup-minor-hardening`); главный репозиторий не трогаем (spec §5).
- Комментарии/документация — по-русски; идентификаторы — английские (AGENTS.base §7). Тесты — AAA-комментарии.
- В feature-ветке коммитим свободно (AGENTS.base §6); мерж в main — только по явной команде пользователя.

---

## Структура задач

| Task | Что делает | Фаза spec §4 |
|---|---|---|
| Task 0 | arch/19 §6 + §3 — два уточнения канона (arch-first) | Фаза 0 |
| Task 1 | A1: WalStreamProcess «слот исчез» — фолбэк + journal (TDD: интеграционный тест) | Фаза 1 |
| Task 2 | A2: BackupProcess.SuperviseActiveAsync — бюджет первым + cleanup best-effort (TDD: юнит-тесты) | Фаза 2 |
| Task 3 | Прогоны (build/юниты/интеграция/E2E-маркер) + зачистка docker | Фаза 3 |

Мерж-гейт (вне Tasks): при мерже в main тем же мерж-коммитом снять roadmap-запись `t13-backup-minor-hardening` из `arch/roadmap/backup.md` ГЛАВНОГО репозитория (в worktree записи нет — она не закоммичена в main; spec §5, AC5).

---

### Task 0: arch/19 — уточнения §6 и §3 (arch-first)

**Files:**
- Modify: `arch/19-backups.md` (§6, bullet «Бюджеты зависших джобов (t07)»; §3, абзац «Правила непрерывности»)

**Interfaces:**
- Consumes: ничего (первая правка ветки).
- Produces: канонические формулировки, на которые ссылаются комментарии кода в Task 1/2 («t13, arch/19 §3» / «t13, arch/19 §6»).

- [ ] **Шаг 0.1: §6 — возрастной бюджет первее transient-гвардов**

  - Вход: worktree чист (кроме `docs/superpowers/...`), файл `arch/19-backups.md` открыт.
  - Действие: в §6 «Источник, HA-контур и ресурсные лимиты», bullet «**Бюджеты зависших джобов (t07)**», после фразы «→ `FAILED` `error="job-timeout: <age> с > FullTimeoutSec"` → переснятие по общему бэкоффу.» (и до предложения про Verify-джоб) вставить продолжение абзаца:

    ```
    Возрастной бюджет — ПЕРВЫЙ гвард супервизии активного (t13): вердикт
    `FAILED` по возрасту — самостоятельный факт etcd (`started_unix` +
    часы воркера), docker-доступ не требуется; transient-пропуск источника
    (portalloc/engine/list) бюджет НЕ откладывает (джобу с возрастом >
    6 ч нечем оправдаться). Kill+rm — best-effort при доступном хосте
    источника, при недоступном — journal-пометка «cleanup пропущен» в той
    же записи `job-timeout/<X>/<id>` (осиротевший контейнер с
    детерминированным именем ничего не блокирует: id уникален, `FAILED`
    уже в etcd; PLANNED без контейнера — FAILED без kill, как раньше).
    ```

  - Выход: §6 фиксирует приоритет возрастного гварда и best-effort-.cleanup.
  - Проверка: `grep -n "ПЕРВЫЙ гвард супервизии" arch/19-backups.md` — ровно одно совпадение.
  - Spec: §4 Фаза 0 (первое уточнение), решение 2.

- [ ] **Шаг 0.2: §3 — defensive-фолбэк при битом last_uploaded_unix**

  - Вход: шаг 0.1 выполнен.
  - Действие: в §3, абзац «**Правила непрерывности**», после фразы «Слот при BROKEN: жив — не трогаем (копит WAL в пределах `max_slot_wal_keep_size`), исчез — пересоздаётся immediate+reserved сразу же (к старту пересъёма полного слот держит позицию ≤ wal_start нового полного; механика первого старта).» вставить предложения:

    ```
    Инвариант записи при «слот исчез» (t13): `last_uploaded_unix` в живом
    (ACTIVE/DEGRADED) ключе обязан существовать (гварды парсеров — первая
    линия); при нарушении (ручная правка/будущий писатель при ослабленном
    гварде — прецедент ed1561b) ветка не крашится тиком: defensive-фолбэк
    времени (clock-сейчас) — только defensive-значение параметра расчёта,
    в BROKEN-запись now() не попадает (запись строится из живого ключа),
    битый ключ маркируется journal-заметкой `wal-key-invalid/<X>`;
    BROKEN/пересоздание слота работают как обычно.
    ```

  - Выход: §3 фиксирует вторую линию обороны на случай рассинхрона писателя/парсера.
  - Проверка: `grep -n "wal-key-invalid" arch/19-backups.md` — ровно одно совпадение.
  - Spec: §4 Фаза 0 (второе уточнение), §2 «Факт над записью, но без краха тика», решение 1.

- [ ] **Шаг 0.3: коммит arch-first**

  - Вход: шаги 0.1–0.2 выполнены, других правок в worktree нет.
  - Действие:

    ```bash
    cd /Users/demakaev/ZCodeProject/worktrees/feat-t13-backup-minor-hardening
    git add arch/19-backups.md
    git commit -m "docs(arch): backups §3/§6 — возрастной бюджет первее transient-гвардов (FAILED по факту etcd started_unix, cleanup best-effort), defensive-фолбэк при битом last_uploaded_unix (t13)"
    ```

  - Выход: канон обновлён до кода; ветка содержит arch-коммит.
  - Проверка: `git log --oneline -1` — arch-коммит; `git status --short` — чисто (кроме `docs/superpowers/`).
  - Spec: §2 «Arch-first».

---

### Task 1: A1 — WalStreamProcess, ветка «слот исчез» (фолбэк + journal)

**Files:**
- Modify: `src/PgWorker.Backups/WalStreamProcess.cs` (шаг (3) `TickShardAsync`, строки ~167–181; комментарий `BreakAsync`, строки ~463–472)
- Test: `src/tests/PgWorker.IntegrationTests/Backups/WalStreamProcessTests.cs` (новый тест в конец класса)

**Interfaces:**
- Consumes: `WalStreamState.LastUploadedUnix` (`long?`), `WorkJournal.WritePhaseAsync(cluster, op, phase, instance, lastError, ct)`, `TimeProvider clock` (ctor-параметр процесса), `logger` (опциональный ctor-параметр).
- Produces: поведение (без новых API): при `chainKnown && wal.State is Active or Degraded` и `wal.LastUploadedUnix is null` ветка пишет journal-фазу `wal-key-invalid/<shard>` и продолжает `BreakAsync` с `baseUnix = clock.GetUtcNow().ToUnixTimeSeconds()`. Контракт `BreakAsync` не меняется.

- [ ] **Шаг 1.1: красный интеграционный тест (нарушение инварианта писателя + слот исчез)**

  - Вход: Task 0 закоммичен; `EtcdFixture`-инфраструктура работает (testcontainers).
  - Действие: в `WalStreamProcessTests` (после теста `Слот_исчез_пишет_BROKEN_и_пересоздает_слот`, имя кластера `cb5` — не занято) добавить тест. Парсеры НЕ задействованы: битый `WalStreamState` передаётся только в `backups`-аргумент тика (снапшот-объект в память, spec §3.3); чтение ключа после тика — raw GET (парсер `WalStatusWriter.Parse` отверг бы ключ без `last_uploaded_unix` — это первая линия, лечение — вторая):

    ```csharp
    // AAA (t13 AC1): инвариант писателя нарушен (ACTIVE с last_uploaded_unix=null
    // — ручная правка/иной писатель) + слот исчез: тик НЕ падает в shard-error —
    // BROKEN (запись из живого ключа, now()-фолбэк в неё не попадает) + слот
    // пересоздан + агент остановлен + journal-заметка wal-key-invalid/<X>.
    // Битый wal в etcd не сидируется: снапшот-объект передан тику напрямую,
    // парсеры не задействованы (spec §3.3); чтение ключа — raw (парсер отверг
    // бы ключ без last_uploaded_unix — гвард остаётся первой линией, spec §3.4).
    [Fact]
    public async Task Слот_исчез_при_битом_last_uploaded_unix_не_роняет_тик()
    {
        // Arrange — wal ACTIVE с LastUploadedUnix = null (инвариант писателя
        // нарушен), слота в FakeSql нет, агент жив; цепочка объектов в S3 есть
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cb5");
        (await _claims.TryClaimClusterAsync("cb5", ct)).Value.Should().BeTrue("клэйм — предусловие тика");
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cb5", 1, 2);
        var driver = new StubScaleDriver();
        driver.BackupAgentObjects.Add(new PgWorker.Docker.Engine.DockerContainer(
            "id-agent-cb5", ["/pgw-backup-wal-cb5-shard1"], "running", "img"));
        var invalidWal = new WalStreamState(
            WalStreamStatus.Active, "pgw_bkp_cb5_shard1", "shard1a",
            "000000010000000000000001", "000000010000000000000002",
            "000000010000000000000002", null, 1, null); // LastUploadedUnix = null!
        var process = BuildProcess(Options(), sql, s3, driver);
        var backups = new ClusterBackups("cb5", null,
            new Dictionary<string, ShardBackups>
            {
                ["shard1"] = new(FullShard("000000010000000000000001").Full, invalidWal),
            });

        // Act
        var result = await process.TickAsync(BuildSnap("cb5"), backups, ct);

        // Assert — тик успешен (не shard-error); ключ BROKEN с error про слот и
        // границей = last_uploaded_segment; now()-фолбэк в запись не попал
        // (last_uploaded_unix в JSON отсутствует); слот пересоздан; агент
        // остановлен; журнал — wal-key-invalid, НЕ shard-error
        result.IsSuccess.Should().BeTrue();
        var walKv = await fixture.Gateway.GetAsync(
            fixture.Endpoint, "/pgworker/backups/cb5/shard1/wal", ct);
        walKv.Value.Should().NotBeNull("BROKEN пишется даже из битого ключа");
        walKv.Value!.Value.Should().Contain("\"state\":\"BROKEN\"").And.Contain("слот");
        walKv.Value.Value.Should().Contain(
            "\"chain_start_segment\":\"000000010000000000000002\"",
            "граница разрыва — last_uploaded_segment на момент обнаружения");
        walKv.Value.Value.Should().NotContain("\"last_uploaded_unix\"",
            "запись строится из живого wal: now()-фолбэк в ключ не попадает (spec §2)");
        sql.Slots.Should().ContainKey("pgw_bkp_cb5_shard1", "слот пересоздаётся при BROKEN (spec §3.2)");
        driver.RemovedBackupAgents.Should().Contain("pgw-backup-wal-cb5-shard1");
        var journal = await fixture.Gateway.GetAsync(fixture.Endpoint, "/pgworker/work/cb5", ct);
        journal.Value!.Value.Should().Contain("wal-key-invalid/shard1");
        journal.Value.Value.Should().NotContain("shard-error");
    }
    ```

  - Выход: тест в коде; на текущем коде он красный.
  - Проверка: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t13-backup-minor-hardening && DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter "FullyQualifiedName~WalStreamProcessTests.Слот_исчез_при_битом"` — FAIL: wal-ключа нет (форс-мьют `LastUploadedUnix!.Value` → `InvalidOperationException` → пер-шардовый catch → `shard-error`, ключ/слот не пишутся).
  - Spec: §3.1, §3.3 (интеграция), AC1; TDD §4 Фаза 1.

- [ ] **Шаг 1.2: правка WalStreamProcess — фолбэк + journal-заметка**

  - Вход: шаг 1.1 — тест красный зафиксирован.
  - Действие: в `src/PgWorker.Backups/WalStreamProcess.cs`, ветка «слот исчез» шага (3) (`if (chainKnown && wal!.State is WalStreamStatus.Active or WalStreamStatus.Degraded)`), заменить блок вызова `BreakAsync` на:

    ```csharp
    if (chainKnown && wal!.State is WalStreamStatus.Active or WalStreamStatus.Degraded)
    {
        // t13 (arch/19 §3): инвариант писателя (last_uploaded_unix в живом
        // ACTIVE/DEGRADED-ключе) может быть нарушен (ручная правка ключа/
        // будущий писатель при ослабленном гварде парсера — прецедент
        // ed1561b): битый ключ не роняет тик форс-мьютом. Фолбэк
        // clock-сейчас — defensive-значение ТОЛЬКО параметра baseUnix:
        // BreakAsync потребляет его лишь при wal == null (запись с нуля),
        // здесь wal != null (chainKnown) — в BROKEN-запись now() НЕ попадает
        // («факт над записью», ревью Ф4-2 №2); факт битого ключа — в журнале.
        if (wal.LastUploadedUnix is null)
        {
            var invalid = $"last_uploaded_unix отсутствует — битый ключ /pgworker/backups/{cluster}/{shard.Name}/wal "
                + "(ручная правка/иной писатель); ветка «слот исчез» идёт с фолбэком времени";
            logger?.LogWarning("backup-wal {Cluster}/{Shard}: {Message}", cluster, shard.Name, invalid);
            await journal.WritePhaseAsync(cluster, Op, $"wal-key-invalid/{shard.Name}",
                claims.InstanceId, invalid, ct);
        }
        // BROKEN + слот пересоздаётся immediate+reserved СРАЗУ (t07, spec §3.2):
        // к старту пересъёма полного слот уже держит позицию ≤ wal_start нового.
        await BreakAsync(cluster, shard.Name, wal, slot, masterRef, adminDsn, ct,
            baseStart: wal.LastUploadedSegment is { Length: > 0 }
                ? wal.LastUploadedSegment : wal.ChainStartSegment,
            baseLast: wal.LastUploadedSegment,
            baseUnix: wal.LastUploadedUnix ?? clock.GetUtcNow().ToUnixTimeSeconds(),
            error: $"слот {slot} исчез при живой цепочке (инвалидация max_slot_wal_keep_size?) — пересними полный бэкап",
            recreateSlot: true);
        return;
    }
    ```

    (Текст заметки `invalid` — дословно из spec §3.1: «last_uploaded_unix отсутствует — битый ключ /pgworker/backups/&lt;C&gt;/&lt;X&gt;/wal (ручная правка/иной писатель); ветка «слот исчез» идёт с фолбэком времени».)

    Дополнить комментарий-инвариант над `BreakAsync` (перед `private async Task<WalStreamState> BreakAsync(...)`, в существующий блок комментария) строками:

    ```csharp
    // Инвариант baseUnix (t13): параметр потребляется ТОЛЬКО при wal == null
    // (создание записи с нуля); при живом wal запись строится из него —
    // now()-фолбэк вызывающего в ключ не попадает. Если будущая правка начнёт
    // использовать baseUnix при живом wal — место пересмотреть: подмена факта
    // now()-временем запрещена («факт над записью», arch/19 §3).
    ```

  - Выход: ветка «слот исчез» переживает `LastUploadedUnix == null`; диагноз виден оператору (journal + warning-лог).
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release` — 0 errors/warnings.
  - Spec: §3.1 (правка + инвариант-комментарий), AC1.

- [ ] **Шаг 1.3: зелёный прогон + регресс WalStreamProcessTests**

  - Вход: шаг 1.2 собран.
  - Действие: прогнать весь класс (новый + 17 существующих тестов).
  - Выход: A1 готово; регресс подтверждён.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter "FullyQualifiedName~WalStreamProcessTests"` — все PASS, включая `Супервиз_...`/`Контроль_...` без правок ожиданий (AC4). После серии (правило AGENTS.md): зачистка docker-контейнеров тестов + `docker network prune -f` (контейнеры dev-стенда `as-*`/`adminpanel`, если подняты, не трогать).
  - Spec: AC1, AC4; §4 Фаза 1.

- [ ] **Шаг 1.4: коммит A1**

  - Вход: шаг 1.3 зелёный.
  - Действие:

    ```bash
    git add src/PgWorker.Backups/WalStreamProcess.cs \
      src/tests/PgWorker.IntegrationTests/Backups/WalStreamProcessTests.cs
    git commit -m "fix(backups): WalStreamProcess «слот исчез» — фолбэк времени при last_uploaded_unix=null + journal wal-key-invalid/<X> вместо вечного shard-error (t13 A1, arch/19 §3)"
    ```

  - Выход: A1 в истории ветки.
  - Проверка: `git log --oneline -1`; `git status --short` — чисто.
  - Spec: §4 Фаза 1.

---

### Task 2: A2 — BackupProcess.SuperviseActiveAsync (бюджет первым + cleanup best-effort)

**Files:**
- Modify: `src/PgWorker.Backups/Process/BackupProcess.cs` (метод `SuperviseActiveAsync`, строки ~266–313)
- Test: `src/tests/PgWorker.UnitTests/Backups/BackupProcessTests.cs` (два новых теста в конец класса)

**Interfaces:**
- Consumes: `SupervisionTimeouts.IsTimedOut(startedUnix, nowUnix, timeoutSec)` (`SupervisionTimeouts.cs:14`), `addresses: IReadOnlyDictionary<string, NodeAddress>` (ключ `"<shard>/<node>"`), `driver.EngineFor(host)`, `BackupNames.ContainerName/VolumeName/FullKey`, `FakeBackupEngine.ListFails`.
- Produces: поведение (без новых API): таймаут-гвард — первый в теле цикла; `nowUnix` вычисляется в начале итерации; внутри таймаут-ветки cleanup best-effort (резолв source/engine/list только для cleanup); journal `job-timeout/<shard>/<id>` получает суффикс `"; cleanup пропущен: источник недоступен"` в `lastError` при пропущенном cleanup.

- [ ] **Шаг 2.1: красный юнит-тест (источник исчез навсегда + возраст > бюджета)**

  - Вход: Task 1 закоммичен; `Rig`/`FakeBackupEngine` доступны (`BackupProcessTests.cs`).
  - Действие: в конец `BackupProcessTests` добавить тест. Сид `NewRig()` кладёт portalloc с `shard1/shard1a` и `shard1/shard1b` — нода `shard1z` в portalloc отсутствует (модель «replace ноды/рассинхрон portalloc при живом шарде»):

    ```csharp
    // AAA (t13 AC2): источник джоба исчез из portalloc НАВСЕГДА (replace ноды/
    // рассинхрон при живом шарде), RUNNING старше бюджета: вердикт FAILED по
    // возрасту — самостоятельный факт etcd (started_unix + часы воркера),
    // docker-доступ не нужен → тик ставит FAILED job-timeout, cleanup
    // пропускается (движок не тронут), journal несёт пометку. До t13 такой
    // джоб застревал в вечном RUNNING и держал инвариант «один активный».
    [Fact]
    public async Task Источник_исчез_при_возрасте_свыше_бюджета_FAILED_без_cleanup()
    {
        // Arrange — RUNNING started = now-7h (бюджет 6 ч дефолт), node = shard1z:
        // ноды нет в portalloc (источник исчез навсегда), порт-аллок жив
        var rig = await NewRig();
        var now = TimeProvider.System.GetUtcNow();
        var active = new FullBackupState("20260908030000Z", FullBackupStatus.Running,
            "shard1z", BackupSourceRole.Master, Unix(now.AddHours(-7)), null, null, null, null, null);

        // Act
        var outcome = await rig.Process.TickAsync(
            await Snapshot(rig.Etcd), BackupsOf(active), CancellationToken.None);

        // Assert — FAILED job-timeout по факту возраста; cleanup пропущен
        // (контейнер/движок не тронуты — Engine.Removed пуст); journal —
        // job-timeout/<shard>/<id> с пометкой о пропущенном cleanup
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        var parsed = BackupsParser.Parse(
            (IReadOnlyList<Kv>)[new Kv(FullKey("20260908030000Z"),
                rig.Etcd.Store[FullKey("20260908030000Z")].Value, 1)], out var errors);
        errors.Should().BeEmpty();
        var failed = parsed.Value[0].Shards["shard1"].Full.Single();
        failed.State.Should().Be(FullBackupStatus.Failed);
        failed.Error.Should().Contain("job-timeout");
        rig.Engine.Removed.Should().BeEmpty("источник недоступен — cleanup пропущен (t13 AC3)");
        rig.Engine.RemovedVolumes.Should().BeEmpty();
        var entry = (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!;
        entry.Phase.Should().Be("job-timeout/shard1/20260908030000Z");
        entry.LastError.Should().Contain("job-timeout").And.Contain("cleanup пропущен");
    }
    ```

  - Выход: тест в коде; на текущем порядке гвардов красный.
  - Примечание (ревью Ф4-LOW): следствие переноса для PLANNED (spec §3.2: «PLANNED с возрастом > бюджета при недоступном источнике → FAILED job-timeout без kill») отдельным мини-тестом НЕ покрывается сознательно: супервизия активных — один цикл `fulls.Where(f.State is Planned or Running or Uploading)`, таймаут-гвард до State-ветвления, тест 2.1 исполняет тот же код-путь (после `continue` State-ветвление не достигается); spec §3.3 и AC2 отдельного PLANNED-теста не требуют.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --filter "FullyQualifiedName~BackupProcessTests.Источник_исчез"` — FAIL: `source is null → continue` до таймаут-гварда — ключ `/pgworker/backups/shop/shard1/full/...` в etcd отсутствует (статус не пишется).
  - Spec: §3.2 п.1, §3.3 (юнит №1), AC2; TDD §4 Фаза 2.

- [ ] **Шаг 2.2: перенос гварда + cleanup best-effort в SuperviseActiveAsync**

  - Вход: шаг 2.1 — тест красный зафиксирован.
  - Действие: в `src/PgWorker.Backups/Process/BackupProcess.cs`, метод `SuperviseActiveAsync` — заменить НАЧАЛО тела цикла (от `foreach (var active ...)` до конца существующего таймаут-блока `continue;` включительно, строки ~271–313) на приведённый ниже код. Всё ПОСЛЕ него (PLANNED-запуск / created-довыгон / vanished / running / exited) — не трогать. Переменные `source`/`engine`/`name` выносятся до гварда и переиспользуются обеими ветками (их прежние объявления между `foreach` и `list` — удалить); cleanup-переменная называется `cleanupList`, внешняя остаётся `list` (имя `list` в обеих областях дало бы CS0844):

    ```csharp
    foreach (var active in fulls.Where(f => f.State
                 is FullBackupStatus.Planned or FullBackupStatus.Running or FullBackupStatus.Uploading))
    {
        // t13 (arch/19 §6): возрастной бюджет — ПЕРВЫЙ гвард: вердикт FAILED
        // по возрасту — самостоятельный факт etcd (started_unix + часы
        // воркера), docker-доступ не нужен; transient-пропуск источника
        // (portalloc/engine/list) бюджет НЕ откладывает (джобу с возрастом >
        // 6 ч нечем оправдаться). journal-before-manipulations: FAILED
        // пишется ДО cleanup.
        var nowUnix = time.GetUtcNow().ToUnixTimeSeconds();
        var source = addresses.GetValueOrDefault($"{shard.Name}/{active.Node}");
        var engine = source is null ? null : driver.EngineFor(source.Host);
        var name = BackupNames.ContainerName(cluster, shard.Name, active.Id);

        if (SupervisionTimeouts.IsTimedOut(active.StartedUnix, nowUnix, options.JobFullTimeoutSec))
        {
            var timedOut = active with
            {
                State = FullBackupStatus.Failed,
                FinishedUnix = nowUnix,
                Error = $"job-timeout: {nowUnix - active.StartedUnix} с > {options.JobFullTimeoutSec}",
            };
            var putTimeout = await PutAsync(
                BackupNames.FullKey(cluster, shard.Name, active.Id), BackupStatusJson.Serialize(timedOut), ct);
            if (!putTimeout.IsSuccess)
                return putTimeout;

            // Cleanup best-effort (t13): kill+rm — только при доступном
            // источнике (source/engine/list); недоступен → cleanup
            // пропускается (осиротевший контейнер с детерминированным именем
            // ничего не держит: id уникален, FAILED уже в etcd), пометка — в
            // lastError той же journal-записи. PLANNED без контейнера —
            // FAILED без kill (как до t13), БЕЗ пометки (cleanup не нужен,
            // а не пропущен).
            string cleanupNote = "";
            if (engine is null)
            {
                cleanupNote = "; cleanup пропущен: источник недоступен";
            }
            else
            {
                var cleanupList = await engine.ListContainersAsync(name, all: true, ct);
                if (!cleanupList.IsSuccess)
                    cleanupNote = "; cleanup пропущен: источник недоступен";
                else if (cleanupList.Value.Any(c => c.Names.Contains(name)))
                    await CleanupJobAsync(engine, cluster, shard.Name, active.Id, ct); // kill+rm контейнера и volume
            }
            await journal.WritePhaseAsync(cluster, Op, $"job-timeout/{shard.Name}/{active.Id}",
                claims.InstanceId, timedOut.Error + cleanupNote, ct);
            continue;
        }

        // хост джоба — хост ноды-источника из portalloc (node-факт статуса);
        // нода исчезла из portalloc → transient: следующий тик.
        if (source is null)
            continue;
        if (engine is null)
            continue;

        var list = await engine.ListContainersAsync(name, all: true, ct);
        if (!list.IsSuccess)
            continue; // transient transport-отказ: статус не меняем (arch/19 §2)

        var found = list.Value.FirstOrDefault(c => c.Names.Contains(name));

        // ... далее БЕЗ ИЗМЕНЕНИЙ существующий код метода: PLANNED-запуск /
        // created-довыгон / vanished / running / exited
    ```

    Удаляемые из старого кода строки: прежние объявления `var source = addresses.FirstOrDefault(...)...` / `if (source is null) continue;` / `var engine = driver.EngineFor(...)` / `if (engine is null) continue;` / прежний `var name = ...` (между `foreach` и `list`), СТАРЫЙ таймаут-блок (комментарий «t07 (arch/19 §6) ...» + `var nowUnix = ...` + `if (SupervisionTimeouts.IsTimedOut(...)) { ... continue; }`) и прежний `var list`/`var found` — всё это уже вошло в новый код выше.

  - Выход: `nowUnix` в начале итерации; таймаут-гвард первый; cleanup best-effort внутри таймаут-ветки; поведение НЕ-истёкших активных не изменилось.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release` — 0 errors/warnings.
  - Spec: §3.2 п.1–2, AC2, AC3.

- [ ] **Шаг 2.3: зелёный прогон нового теста + второй тест (list-отказ)**

  - Вход: шаг 2.2 собран.
  - Действие: прогнать тест шага 2.1 (зелёный), затем добавить в конец `BackupProcessTests` второй тест (фейк поддерживает `ListFails`; spec §3.3 «опционально, при поддержке фейка» — поддерживает):

    ```csharp
    // AAA (t13 AC3): возраст свыше бюджета + list-отказ (transient источника
    // при доступном portalloc/engine): FAILED всё равно ставится — возраст
    // самодостаточен; cleanup пропускается с пометкой в той же journal-записи
    [Fact]
    public async Task List_отказ_при_возрасте_свыше_бюджета_FAILED_и_cleanup_пропущен()
    {
        // Arrange — RUNNING started = now-7h на живой ноде shard1a, контейнер
        // running («вечный» джоб), но list по движку падает (transport-отказ)
        var rig = await NewRig();
        var now = TimeProvider.System.GetUtcNow();
        var active = new FullBackupState("20260908030000Z", FullBackupStatus.Running,
            "shard1a", BackupSourceRole.Master, Unix(now.AddHours(-7)), null, null, null, null, null);
        var name = BackupNames.ContainerName("shop", "shard1", "20260908030000Z");
        rig.Engine.Containers[name] = new("cnt-listfail", "running", -1, "{\"phase\":\"basebackup\"}");
        rig.Engine.ListFails = true;

        // Act
        var outcome = await rig.Process.TickAsync(
            await Snapshot(rig.Etcd), BackupsOf(active), CancellationToken.None);

        // Assert — FAILED job-timeout (transient list больше не откладывает
        // бюджет); контейнер не тронут (cleanup пропущен); journal — пометка
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.ToString());
        var parsed = BackupsParser.Parse(
            (IReadOnlyList<Kv>)[new Kv(FullKey("20260908030000Z"),
                rig.Etcd.Store[FullKey("20260908030000Z")].Value, 1)], out var errors);
        errors.Should().BeEmpty();
        var failed = parsed.Value[0].Shards["shard1"].Full.Single();
        failed.State.Should().Be(FullBackupStatus.Failed);
        failed.Error.Should().Contain("job-timeout");
        rig.Engine.Removed.Should().BeEmpty("list-отказ — cleanup пропущен (t13 AC3)");
        rig.Engine.RemovedVolumes.Should().BeEmpty();
        var entry = (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!;
        entry.Phase.Should().Be("job-timeout/shard1/20260908030000Z");
        entry.LastError.Should().Contain("cleanup пропущен");
    }
    ```

  - Выход: оба новых теста зелёные.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --filter "FullyQualifiedName~BackupProcessTests.Источник_исчез"` — PASS; `--filter "FullyQualifiedName~BackupProcessTests.List_отказ"` — PASS.
  - Spec: §3.2 следствия переноса (transient list на истёкшем → FAILED), §3.3 (юнит №2), AC2, AC3.

- [ ] **Шаг 2.4: регресс юнитов Backups (без правки ожиданий)**

  - Вход: шаг 2.3 зелёный.
  - Действие: полный прогон юнитов подсистемы бэкапов.
  - Выход: регресс подтверждён: `Супервиз_полный_старше_бюджета_FAILED_jobtimeout_и_переснятие` (живой источник: FAILED + kill/rm + journal без пометки) и `TransportError_KeepsStatus` (возраст < бюджета: list-отказ — статус не тронут) зелёные без правок.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --filter "FullyQualifiedName~PgWorker.UnitTests.Backups"` — все PASS.
  - Spec: AC4.

- [ ] **Шаг 2.5: коммит A2**

  - Вход: шаг 2.4 зелёный.
  - Действие:

    ```bash
    git add src/PgWorker.Backups/Process/BackupProcess.cs \
      src/tests/PgWorker.UnitTests/Backups/BackupProcessTests.cs
    git commit -m "fix(backups): SuperviseActiveAsync — возрастной бюджет первым гвардом (FAILED job-timeout по факту etcd started_unix без docker-доступа; вечный RUNNING при исчезнувшем источнике больше не держит инвариант одного активного), cleanup best-effort с journal-пометкой при недоступном источнике (t13 A2, arch/19 §6)"
    ```

  - Выход: A2 в истории ветки.
  - Проверка: `git log --oneline -1`; `git status --short` — чисто.
  - Spec: §4 Фаза 2.

---

### Task 3: прогоны, E2E-маркер, зачистка

**Files:**
- Без правок кода (только прогоны); правки возможны исключительно по итогам падений — с анализом (не перезапускать упавшие без разбора, правило AGENTS.md «Телеметрия E2E»).

**Interfaces:**
- Consumes: Tasks 0–2 закоммичены.
- Produces: зелёные гейты AC5.

- [ ] **Шаг 3.1: build Release (warnings-as-errors)**

  - Вход: Tasks 0–2 в ветке.
  - Действие: полная сборка.
  - Выход: бинарь Release собран (включая E2eFixture-сборку шага 3.4).
  - Проверка: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t13-backup-minor-hardening && DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release` — `Build succeeded`, 0 Warning(s) (TreatWarningsAsErrors).
  - Spec: AC5.

- [ ] **Шаг 3.2: юниты Backups**

  - Вход: шаг 3.1 зелёный.
  - Действие: прогон серии.
  - Выход: юнит-гейт зелёный.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --filter "FullyQualifiedName~PgWorker.UnitTests.Backups"` — все PASS.
  - Spec: AC5.

- [ ] **Шаг 3.3: интеграция Backups + зачистка**

  - Вход: шаг 3.2 зелёный.
  - Действие: прогон серии (testcontainers: own-etcd/own-minio поднимают контейнеры сами, teardown — в фикстурах); после финальной строки серии — зачистка: `docker rm -f $(docker ps -aq)` (НЕ трогать `as-*`/`adminpanel` поднятого dev-станда, если стенд поднят) + `docker network prune -f` (сети per-cluster ryuk не подбирает — правило AGENTS.md).
  - Выход: интеграционный гейт зелёный; docker-хост чист от тестовых контейнеров/сетей.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter "FullyQualifiedName~PgWorker.IntegrationTests.Backups"` — все PASS; затем `docker ps -aq | wc -l` — 0 (или только `as-*`/`adminpanel` стенда); `docker network ls | grep -cE 'kfw-net|pgw-.*-net'` — 0.
  - Spec: AC5, §4 Фаза 3.

- [ ] **Шаг 3.4: E2E-маркер мерж-гейта (свежий Release) + зачистка**

  - Вход: шаг 3.3 зелёный; внешние образы заранее зеркалированы (`dev-stand/images/pull-images.sh`, если E2E-окружение их не найдёт).
  - Действие: прогон кейс-маркера (задача трогает код воркеров; provisioning/portalloc/moves не меняются — полный E2eFixture не требуется, spec §4 Фаза 3). E2eFixture соберёт Release сам (инкрементальный no-op). После финальной строки — зачистка как в шаге 3.3. Падение — НЕ перезапускать без анализа: снять docker-логи/inspect и `host.log` (телеметрия `/tmp/pgw-e2e-artifacts-<guid>/`), сформулировать причины, только затем решение.
  - Выход: E2E-маркер зелёный на свежем бинаре; docker-хост чист.
  - Проверка: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard` — PASS; затем зачистка как в шаге 3.3 (`docker ps -aq | wc -l` — 0/только стенд; `docker network prune -f`).
  - Spec: AC5, §4 Фаза 3; правило AGENTS.md «E2E на свежем Release».

- [ ] **Шаг 3.5: итог ветки**

  - Вход: шаги 3.1–3.4 зелёные.
  - Действие: сверить историю ветки (3 коммита: arch, A1, A2) и чистоту worktree; roadmap-запись `t13-backup-minor-hardening` в worktree НЕ трогаем — она живёт незакоммиченной в главном репозитории и снимается из `arch/roadmap/backup.md` тем же мерж-коммитом при мерже в main (по явной команде пользователя).
  - Выход: ветка готова к ревью/мержу; мерж-гейт зафиксирован.
  - Проверка: `git log --oneline -4` — arch/A1/A2-коммиты; `git status --short` — чисто (кроме `docs/superpowers/`).
  - Spec: AC5 (roadmap), §5 (правки только в worktree).

---

## Self-review (выполнен автором плана)

1. **Покрытие spec:** §3.1 → Task 1; §3.2 → Task 2; §3.3 (юниты: тест «источник исчез» — шаг 2.1, опциональный list-отказ — шаг 2.3, регресс — шаг 2.4; интеграция — шаг 1.1); §3.4 — границы соблюдены (Restore/Verify-процессы и парсеры не тронуты); §4 Фаза 0 → Task 0; Фаза 3 → Task 3; AC1–AC5 распределены по проверкам шагов. Пробелов нет.
2. **Плейсхолдеры:** отсутствуют — каждый шаг содержит точный код/текст/команду.
3. **Типовая консистентность:** `IsTimedOut(long, long, long)`, `WritePhaseAsync(cluster, op, phase, instance, lastError, ct)`, `FullBackupState(...)`-конструктор (10 позиционных параметров, как в существующих тестах; `BackupsModel.cs:78–88`), `WalStreamState(...)` (9 полей), `LastUploadedUnix` — `long?`; имена тестов кластеров `cb5` не конфликтует с существующими (`c1`, `cc1`–`cc9`, `cb1`–`cb4`, `cm1`, `cr9`, `cca`).
