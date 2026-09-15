# t10-rs-dr-master-readiness-gate — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Ревизия: v3 — синхронизация с исполнением (коммит `2102259`).** Исполнение Фазы 6 вскрыло ошибку контракта v2: проба `pg_is_in_recovery()::text` со строковым сравнением `"f"` провалила первый полный E2E во всех 3 сценариях — PostgreSQL 18 (spilo-18) для `boolean::text` возвращает `"false"`, а не `"f"` (как в PG≤17) — строковый паттерн не сходился бы никогда. Решение пользователя (AskUserQuestion в ходе исполнения, коммит `2102259`): проба `SELECT pg_is_in_recovery()` БЕЗ `::text`, сравнение **boxed bool `false`**. Spec синхронизирован (§4.2/§8/шапка, resume spec-writer). Фактическое состояние: Tasks 1–5 исполнены (коммиты `6ec0608` arch, `5a61a21` компонент A, `f2690da` компонент B, `0b6f174` компонент C, `2102259` фикс контракта; прогоны: юниты 870/870, интеграция 210/210, `E2eRestoreScenarios` 3/3 за 14 м 1 с, маркер `Scale_AddEmptyShard` 1/1); Task 6 (снятие тега из roadmap) — мерж-гейт, впереди. Детали — в секции «Self-review v3» в конце плана.
>
> **История ревизий:** v2 — закрытие замечаний ревью plan↔spec Фазы 4 (№1 wal-JSON, №2 проверка Task 1, №3 диапазон roadmap, №4 фиксация ::text — **отменено в v3**, см. Self-review v3).

**Goal:** Устранить системный флейк E2E rs-dr тремя уровнями: прод-вычистка `archive_mode`/`archive_command` из pristine auto.conf в restore-джобе (корень), прод-гейт SQL-готовности мастера до `COMPLETED` в `RestoreProcess.RejoinAsync`, тестовый гейт + ретрай чтения в сценарии rs-dr.

**Architecture:** Правка контракта `arch/19-backups.md` §3.5 идёт первой (arch-first). Затем три независимых компонента: bash-строки в inline-скрипте `RestoreJobCommand` (A), SQL-проба готовности мастера по admin-DSN первой ноды на существующем трекере `_rejoinWaitSince` с бюджетом `PatroniBootSec` (B), гейт `WaitPhaseAsync("master-ready")` + ретрай-цикл чтения по эталону rs-latest в `E2eRestoreScenarios` (C). Планировщик `BackupProcess` и сценарии rs-latest/rs-time не меняются.

**Осознанное уточнение spec (v3, синхронизировано с кодом коммита `2102259`):** SQL-проба компонента B — `SELECT pg_is_in_recovery()` **без** `::text`; Npgsql для PG-boolean возвращает **boxed bool**, гейт пропускает при `Value is false` (диагностический текст не-готовности — `pg_is_in_recovery != false`). **ПРИМЕЧАНИЕ PG18** (инцидент первого E2E-прогона t10): `boolean::text` в PostgreSQL 18 возвращает `"false"`, а не `"f"` — любой строковый паттерн (`"f"` и тем более `"false"` как контракт) в пробе НЕ использовать. Семантика гейта («пропускать только при мастере вне recovery») не менялась со времён brainstorming; правка коснулась только представления значения. Контракт зафиксирован в spec §4.2 (правка по итогам исполнения, Фаза 6) и §8 (фейк: transient-вид «висит в recovery» — boxed bool `true`).

**Tech Stack:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`), xUnit + FluentAssertions, Npgsql, docker testcontainers (E2E).

**Spec:** `docs/superpowers/2026-09-15-t10-rs-dr-master-readiness-gate/spec.md` (в этом же каталоге; план аргументируется от spec — исполнитель читает оба).

**Worktree:** `/Users/demakaev/ZCodeProject/worktrees/fix-t10-rs-dr-master-readiness-gate` — все пути ниже относительны его корня. Коммиты — в текущую ветку worktree (в feature-ветке коммитим свободно, мерж в `main` — только по явной просьбе пользователя).

## Global Constraints

- .NET 10, `LangVersion=latest`, `Nullable=enable`, **`TreatWarningsAsErrors=true`** → Release-сборка обязана идти с 0 warnings.
- Комментарии в коде и документация — по-русски; идентификаторы — по-английски; попадать в тон окружающего текста (spec §6).
- Тесты — с AAA-комментариями (Arrange/Act/Assert) — правило AGENTS пользователя.
- НЕТ новых etcd-ключей, новых статусов restore, новых конфиг-параметров (spec §6). Гейт B живёт на существующем бюджете `PatroniBootSec` и трекере `_rejoinWaitSince`.
- SQL-проба гейта B — только `SELECT pg_is_in_recovery()` без `::text`, сравнение boxed bool `false` (см. «Осознанное уточнение spec» в шапке; PG18: `boolean::text` = `"false"` — строковые паттерны не использовать).
- `BackupProcess` НЕ трогаем; rs-latest/rs-time НЕ трогаем (spec §3.2, §3.4, §6).
- Порты динамические: никаких хардкодов хост-портов; гейты ходят по DSN/portalloc (spec §6).
- Прогоны серий: юниты → интеграция → E2E; после КАЖДОЙ серии — зачистка контейнеров и СЕТЕЙ до старта следующей; дожидаться финальной строки прогона (AGENTS.md).
- E2E-телеметрия — `docs/e2e-launch.md`: новый ожидания-участок только через `WaitPhaseAsync`; упавший сценарий — `MarkFailed()` (stop, не delete); перезапуск упавших — только после полного анализа логов и согласия пользователя.
- **Процессное требование пользователя:** длинные команды (сборки, тест-серии) — ТОЛЬКО фоновый запуск с короткими циклами опроса (≤30 с, максимум ≤120 с); E2E-прогоны анализируются ОНЛАЙН — читать лог/артефакты по мере появления, при первых признаках падения разбирать в процессе, не дожидаясь финала серии (spec §7.6).
- `memory.md` не трогать; вопросы — только через `AskUserQuestion` с паузой.

---

## File Structure (карта изменений)

| Файл | Тип | Ответственность |
|---|---|---|
| `arch/19-backups.md` §3.5 | Modify | контракт restore: вычистка archive-параметров + SQL-гейт до COMPLETED (Task 1) |
| `src/PgWorker.Backups/Restore/RestoreJobCommand.cs` | Modify | скрипт джобы: sed-вычистка + chown после возврата pristine (Task 2) |
| `src/tests/PgWorker.UnitTests/Backups/RestoreJobCommandTests.cs` | Modify | юнит-ассерты содержания и порядка (Task 2) |
| `src/PgWorker.Backups/Process/RestoreProcess.cs` | Modify | конструктор `ISqlExecutor db`, проба+`MasterSqlWaitAsync` в `RejoinAsync`, снятие sql-ключа трекера при COMPLETED (Task 3) |
| `src/PgWorker.App/Program.cs` (~:487) | Modify | wiring `ISqlExecutor` в RestoreProcess (Task 3) |
| `src/tests/PgWorker.IntegrationTests/Backups/RestoreProcessTests.cs` | Modify | `FakeDb` + `BuildProcess(..., db:)` + 4 новых кейса гейта (Task 3) |
| `src/tests/PgWorker.IntegrationTests/E2e/E2eRestoreScenarios.cs` | Modify | rs-dr: гейт master-ready + ретрай финального чтения (Task 4) |
| `arch/roadmap/pgworker.md` | Modify | снятие тега t10 (мерж-гейт, Task 6) |

---

## Task 1: arch-first — контракт §3.5 (вычистка + SQL-гейт)

**Вход (предусловие):** worktree чист (или изменения закоммичены), spec прочитан.

**Действие (файлы/изменения):** `arch/19-backups.md`, два дополнения в §3.5 — до любого кода (AGENTS.base §1: сначала arch, затем код).

**Выход:** контракт обновлён; код задач 2–3 зеркалит его.

**Проверка:** `git diff arch/19-backups.md` показывает ровно два дополнения; `grep -c "t10" arch/19-backups.md` → **≥ 2** (обе вставки помечены t10: в 1.1 — «инцидент t10», в 1.2 — «SQL-гейт мастера (t10)»).

**Связь со spec:** Фаза 0 (§5), дизайн §4.1/§4.2 (контрактные формулировки).

- [ ] **Шаг 1.1. Дополнить абзац «Restore-джоб» (механика джоба)**

В абзаце, начинающемся «**Restore-джоб**: ephemeral контейнер», после фрагмента «(4) `pg_ctl stop -m fast`, убирает recovery-остатки из `auto.conf`.» дополнить то же предложение (до «Прогресс — stdout-маркеры»):

```markdown
При возврате pristine `auto.conf` из него вычищаются управляющие Patroni
параметры архивации (`archive_mode`/`archive_command`): pristine снят
`pg_basebackup`-ом с ноды-источника и несёт её `archive_mode`, а Patroni
нового HA-scope навязывает свой → reload для `archive_mode` недостаточен →
«Pending restart» → отложенный рестарт postmaster рвёт соединения клиентов
сразу после restore (инцидент t10, 2026-09-15). WAL-архивация системы —
внешний агент t03, `archive_mode` постгреса не используется.
```

- [ ] **Шаг 1.2. Дополнить абзац «RestoreProcess» (условия COMPLETED)**

В том же §3.5, в абзаце «**RestoreProcess** (машина тика…)», фрагмент «) → все ноды RUNNING → `COMPLETED` (пост-обработка:» заменить на:

```markdown
) → все ноды RUNNING → SQL-гейт мастера (t10): проба фактического
постгреса лидера восстановленного шарда `pg_is_in_recovery() = false` по
admin-DSN первой ноды; Patroni-пробы живы и в переходном окне рестарта
postmaster, а `COMPLETED` обязан означать закрытое окно — после него
планировщик полных немедленно стартует пересъём, тесты/панель читают.
Бюджет — `PatroniBootSec`; исчерпан → permanent-FAILED «мастер не принял
SQL» → `COMPLETED` (пост-обработка:
```

- [ ] **Шаг 1.3. Коммит**

```bash
git add arch/19-backups.md
git commit -m "arch(19): §3.5 — вычистка archive-параметров pristine auto.conf и SQL-гейт мастера до COMPLETED (t10)"
```

---

## Task 2: Компонент A — вычистка archive-параметров в restore-джобе (TDD)

**Вход:** Task 1 закоммичен.

**Действие:** `RestoreJobCommand.cs` — после `mv "$AUTO.orig" "$AUTO"` (:141) добавить sed-вычистку `archive_mode`/`archive_command` + `chown 101:101 "$AUTO"`; юнит-тест на содержание и порядок.

**Выход:** pristine auto.conf, возвращаемый джобой, не несёт рестарт-требующих archive-параметров источника; владелец файла — 101:101.

**Проверка:** `dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~RestoreJobCommandTests` — зелёный, включая новый кейс.

**Связь со spec:** §4.1 (компонент A), §5 Фаза 1, §7.4.

- [ ] **Шаг 2.1. Написать падающий юнит-тест**

В `src/tests/PgWorker.UnitTests/Backups/RestoreJobCommandTests.cs` добавить (стиль файла — `@""`-литералы и IndexOf-порядок, прецеденты `:34–37`):

```csharp
    // AAA: pristine auto.conf после возврата (mv) вычищается от управляющих
    // Patroni параметров архивации (инцидент t10: archive_mode источника →
    // «Pending restart» нового HA-scope → рестарт postmaster рвёт соединения);
    // sed -i пересоздаёт файл под root — chown 101:101 после sed обязателен
    // (блок chown -R выше по скрипту уже прошёл).
    [Fact]
    public void Build_PristineAutoConf_CleansPatroniArchiveParams()
    {
        // Arrange / Act
        var script = RestoreJobCommand.Build()[2];

        // Assert — вычистка archive_mode/archive_command из возвращённого pristine
        script.Should().Contain(
            @"sed -i '/^archive_mode[[:space:]=]/d;/^archive_command[[:space:]=]/d' ""$AUTO""");
        // вычистка — ПОСЛЕ возврата pristine: применяется к итоговому auto.conf
        script.IndexOf("sed -i '/^archive_mode", StringComparison.Ordinal)
            .Should().BeGreaterThan(
                script.IndexOf("mv \"$AUTO.orig\" \"$AUTO\"", StringComparison.Ordinal),
                "вычистка применяется к возвращённому pristine auto.conf");
        // chown — ПОСЛЕ sed: sed -i пересоздаёт файл под root
        script.IndexOf("chown 101:101 \"$AUTO\"", StringComparison.Ordinal)
            .Should().BeGreaterThan(
                script.IndexOf("sed -i '/^archive_mode", StringComparison.Ordinal),
                "sed -i пересоздаёт файл под root — владелец 101 возвращается после");
    }
```

- [ ] **Шаг 2.2. Прогнать — убедиться в FAIL**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release \
  --filter FullyQualifiedName~RestoreJobCommandTests
```

Ожидание: FAIL `Build_PristineAutoConf_CleansPatroniArchiveParams` («Expected script to contain "sed -i '/^archive_mode…"»), остальные кейсы класса зелёные.

- [ ] **Шаг 2.3. Реализовать вычистку в скрипте**

В `src/PgWorker.Backups/Restore/RestoreJobCommand.cs` между строками `mv "$AUTO.orig" "$AUTO"` (:141) и `sed -i '/^local all all trust$/d' …` (:142) вставить:

```bash
        # Вычистка управляющих Patroni параметров архивации (инцидент t10,
        # 2026-09-15): pristine auto.conf снят pg_basebackup-ом с ноды-источника
        # и несёт её archive_mode=on; Patroni нового HA-scope навязывает свой
        # archive_mode=None → reload для archive_mode недостаточен → «Pending
        # restart» → отложенный рестарт postmaster рвёт соединения клиентов
        # сразу после restore. WAL-архивация в системе — внешний pg_receivewal
        # t03, archive_mode постгреса не используется: вычистка бэкап-контур
        # не ломает. sed -i пересоздаёт файл под root — chown обязателен
        # (блок chown -R выше уже прошёл).
        sed -i '/^archive_mode[[:space:]=]/d;/^archive_command[[:space:]=]/d' "$AUTO"
        chown 101:101 "$AUTO"
```

- [ ] **Шаг 2.4. Прогнать — убедиться в PASS (весь класс)**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release \
  --filter FullyQualifiedName~RestoreJobCommandTests
```

Ожидание: PASS все кейсы, включая новый и старые (`Build_RecoveryTargetsAndCleanup` не сломан — `mv`-строка не менялась).

- [ ] **Шаг 2.5. Коммит**

```bash
git add src/PgWorker.Backups/Restore/RestoreJobCommand.cs \
        src/tests/PgWorker.UnitTests/Backups/RestoreJobCommandTests.cs
git commit -m "feat(backups): restore-джоб вычищает archive_mode/archive_command из pristine auto.conf + chown 101 (t10, корень 57P01)"
```

---

## Task 3: Компонент B — SQL-гейт мастера перед COMPLETED (TDD)

> Статус (v3): **исполнен в Фазе 6** — коммиты `f2690da` (гейт по плану v2, строкочный контракт `"f"`) и `2102259` (фикс контракта на boxed bool `false` — отклонение от v2, согласовано пользователем через AskUserQuestion по итогам разбора фейлов E2E Шага 5.5). Код шагов ниже приведён по фактическому состоянию worktree (коммит `2102259`). Чекбоксы отмечены исполненными.

**Вход:** Task 2 закоммичен.

**Действие:** `RestoreProcess.cs` — конструктор получает `ISqlExecutor db`; в `RejoinAsync` между `allStarted` и постановкой нод `RUNNING` — SQL-проба `SELECT pg_is_in_recovery()` (без `::text`, сравнение boxed bool `false`) по `ShardEndpoints.AdminDsn(firstAddr, snap.Config.DbName, secrets)`; не готова → `MasterSqlWaitAsync` (трекер `_rejoinWaitSince` с суффиксом `/sql`, бюджет `thresholds.PatroniBootSec`, warn каждые ~10 тиков, исчерпан → permanent-FAILED со снимком Patroni-диагностики); при COMPLETED снимать и sql-ключ трекера. Wiring: `Program.cs`. Тесты: `FakeDb` + 4 кейса.

**Интерфейсы (Produces/Consumes):**
- Consumes: `ISqlExecutor.ExecuteScalarAsync(string dsn, string sql, CancellationToken ct) → Task<Result<object?>>` (существующий, `PgWorker.Provisioning.Sql`); `ShardEndpoints.AdminDsn(NodeAddress, string dbname, InstallSecrets) → string` (существующий); `_rejoinWaitSince`, `TransientAsync`, `FailPermanentAsync`, `probe.GetClusterAsync` — существующие члены RestoreProcess.
- Produces: конструктор `RestoreProcess(…, ShardProbe probe, ISqlExecutor db, ThresholdsOptions thresholds, …)` — параметр `db` строго между `probe` и `thresholds` (обновляются Program.cs и RestoreProcessTests.BuildProcess).

**Выход:** `COMPLETED` означает SQL-готовность мастера; не-готовность — REJOINING без мутаций; бюджет — permanent-FAILED «мастер не принял SQL».

**Проверка:** `dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~RestoreProcessTests` — зелёный (4 новых + все старые кейсы). **Исполнено в Фазе 6:** интеграционная серия 210/210 (после коммита `2102259`), включая кейсы t10-а…г.

**Связь со spec:** §4.2 (компонент B, в ред. правки по итогам исполнения — boxed bool `false`), §5 Фаза 2, §7.4; риски §8 (фейк покрывает оба transient-вида — `true` и FailScalar; старые кейсы — тривиальный фейк «готова» `false`).

- [x] **Шаг 3.1. Компилируемая грань: конструктор + wiring + дефолт тестов**

3.1a. `src/PgWorker.Backups/Process/RestoreProcess.cs`: добавить `using PgWorker.Provisioning.Sql;` в usings; в первичном конструкторе между `ShardProbe probe,` и `ThresholdsOptions thresholds,` вставить `ISqlExecutor db,`.

3.1b. `src/PgWorker.App/Program.cs`, регистрация RestoreProcess (:476–492): после строки `sp.GetRequiredService<ShardProbe>(),` (в этом блоке) добавить:

```csharp
    sp.GetRequiredService<ISqlExecutor>(),
```

3.1c. `src/tests/PgWorker.IntegrationTests/Backups/RestoreProcessTests.cs`: добавить `using PgWorker.Provisioning.Sql;`; расширить `BuildProcess` и добавить фейк (v3: `Scalar` — `object?` с boxed bool, дефолт `false` = «готова»):

```csharp
    private RestoreProcess BuildProcess(FakeBackupS3 s3, IClusterDriver driver,
        TimeProvider? clock = null, HttpMessageHandler? patroni = null,
        BackupsRuntimeOptions? options = null, ISqlExecutor? db = null)
        => new(fixture.Gateway, [fixture.Endpoint], driver, s3, _claims,
            new WorkJournal("/pgworker", fixture.Gateway, [fixture.Endpoint]), options ?? Options(),
            new InstallSecrets("su-pw", "sb-pw", "adm-pw", "mov-pw"),
            new EtcdEndpoints([fixture.Endpoint]), new StubAppSecret(),
            new ShardProbe(new HttpClient(patroni ?? new DeadHandler())),
            db ?? new FakeDb(),
            new ThresholdsOptions(600, 1800, PatroniBootSec: 600),
            clock ?? TimeProvider.System);
```

```csharp
    // SQL-фейк гейта мастера (t10): Scalar — ответ pg_is_in_recovery()
    // (boxed-bool от Npgsql: false — готова, дефолт: старые кейсы ведут себя
    // как раньше; true — висит в recovery), FailScalar — транспорт рвётся.
    // Оба transient-вида гейта идут одним путём ожидания (spec t10 §8).
    private sealed class FakeDb : ISqlExecutor
    {
        public object? Scalar { get; set; } = false;
        public bool FailScalar { get; set; }

        public Task<Result> ExecuteAsync(string dsn, string sql, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result<object?>> ExecuteScalarAsync(string dsn, string sql, CancellationToken ct)
            => Task.FromResult(FailScalar
                ? Result<object?>.Failed(new ApplicationException("connection refused (fake)"))
                : Result<object?>.Success(Scalar));

        public Task<Result> EnsureDatabaseAsync(string dsn, string dbname, CancellationToken ct)
            => Task.FromResult(Result.Success());
    }
```

3.1d. Прогнать старые кейсы — поведение не изменилось (гейта ещё нет, фейк не зовётся):

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release \
  --filter FullyQualifiedName~RestoreProcessTests
```

Ожидание: PASS все существующие кейсы (транзитная компилируемая грань).

- [x] **Шаг 3.2. Написать 4 новых кейса (красные)**

Добавить в `RestoreProcessTests` после блока «── REJOINING: ensure нод + пробы + COMPLETED ──». ⚠️ Сид wal-ключа — ТОЛЬКО полный JSON со всеми обязательными полями `BackupsParser.TryParseWal` (slot/master_node/chain_start_segment/last_received_segment/last_uploaded_segment/last_uploaded_unix) по прецеденту кейса `Rejoin_лидер_избран_…` (:818–820): минимальный `{"state":"ACTIVE"}` дал бы ошибки парсинга, и хелпер `BackupsFromEtcdAsync` (ассерт `errors.Should().BeEmpty()`) упал бы ДО `TickAsync` (v2/№1). Фейк: transient-вид «висит в recovery» — `Scalar = true` (boxed bool, не строка `"t"`), успех — `Scalar = false` (v3/коммит `2102259`):

```csharp
    // ── REJOINING: SQL-гейт мастера перед COMPLETED (t10) ──

    // AAA (t10-а): Patroni-пробы готовы, но мастер «висит в recovery»
    // (pg_is_in_recovery=true) → гейт не пускает: статус REJOINING, ноды не
    // переводятся в RUNNING, wal-ключ жив; тик InProgress, журнал —
    // master-sql-wait (без мутаций — тик повторит).
    [Fact]
    public async Task Rejoin_мастер_в_recovery_гейт_не_пускает_без_мутаций()
    {
        // Arrange — Patroni готов (лидер running, реплика creating replica),
        // SQL-фейк: pg_is_in_recovery = true
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("c1");
        await SeedTwoNodeAllocAsync();
        // wal-ключ — полный JSON контракта TryParseWal (минимальный {"state":…}
        // ломает BackupsParser — прецедент :818-820)
        await fixture.Gateway.PutAsync(fixture.Endpoint, "/pgworker/backups/c1/shard1/wal",
            """{"state":"ACTIVE","slot":"s","master_node":"shard1a","chain_start_segment":"000000010000000000000002","last_received_segment":"000000010000000000000004","last_uploaded_segment":"000000010000000000000004","last_uploaded_unix":1}""",
            null, ct);
        var driver = new TestDriver(new StubScaleDriver(), new FakeBackupEngine());
        var process = BuildProcess(new FakeBackupS3(), driver,
            patroni: new PatroniHandler { Ready = true }, db: new FakeDb { Scalar = true });
        var op = await SeedRestoreAsync("c1", "shard1", "20260911122100Z",
            backupId: "20260910120000Z", state: RestoreStatus.Rejoining);
        (await _claims.TryClaimClusterAsync("c1", ct)).Value.Should().BeTrue();

        // Act
        var result = await process.TickAsync(BuildTwoNodeSnap(), await BackupsFromEtcdAsync("c1"), ct);

        // Assert — ожидание, никаких мутаций завершения
        result.IsSuccess.Should().BeTrue(result.Error?.ToString());
        result.Value.Should().Be(ProcessOutcome.InProgress);
        (await ReadRestoresAsync("c1", "shard1")).Single(r => r.Id == op.Id)
            .State.Should().Be(RestoreStatus.Rejoining);
        (await fixture.Gateway.GetAsync(fixture.Endpoint,
            "/clusters/c1/shards/shard1/nodes/shard1a/state", ct)).Value
            .Should().BeNull("RUNNING ставится только после SQL-гейта (t10)");
        (await fixture.Gateway.GetAsync(fixture.Endpoint, "/pgworker/backups/c1/shard1/wal", ct))
            .Value.Should().NotBeNull("wal-ключ удаляется только с COMPLETED");
        (await fixture.Gateway.GetAsync(fixture.Endpoint, "/pgworker/work/c1", ct)).Value!.Value
            .Should().Contain("master-sql-wait/shard1/");
    }

    // AAA (t10-б): Patroni готов + мастер принял SQL (pg_is_in_recovery=false)
    // → гейт пропускает: ноды RUNNING, COMPLETED, wal-ключ удалён.
    [Fact]
    public async Task Rejoin_мастер_принял_SQL_гейт_пропускает_до_COMPLETED()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("c1");
        await SeedTwoNodeAllocAsync();
        // wal-ключ — полный JSON контракта TryParseWal (минимальный {"state":…}
        // ломает BackupsParser — прецедент :818-820)
        await fixture.Gateway.PutAsync(fixture.Endpoint, "/pgworker/backups/c1/shard1/wal",
            """{"state":"ACTIVE","slot":"s","master_node":"shard1a","chain_start_segment":"000000010000000000000002","last_received_segment":"000000010000000000000004","last_uploaded_segment":"000000010000000000000004","last_uploaded_unix":1}""",
            null, ct);
        var inner = new StubScaleDriver();
        var driver = new TestDriver(inner, new FakeBackupEngine());
        var process = BuildProcess(new FakeBackupS3(), driver,
            patroni: new PatroniHandler { Ready = true }, db: new FakeDb { Scalar = false });
        var op = await SeedRestoreAsync("c1", "shard1", "20260911122101Z",
            backupId: "20260910120000Z", state: RestoreStatus.Rejoining);
        (await _claims.TryClaimClusterAsync("c1", ct)).Value.Should().BeTrue();

        // Act
        (await process.TickAsync(BuildTwoNodeSnap(), await BackupsFromEtcdAsync("c1"), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — COMPLETED с закрытым окном
        inner.EnsuredNodes.Should().BeEquivalentTo(["shard1/shard1a", "shard1/shard1b"]);
        (await ReadRestoresAsync("c1", "shard1")).Single(r => r.Id == op.Id)
            .State.Should().Be(RestoreStatus.Completed);
        (await fixture.Gateway.GetAsync(fixture.Endpoint, "/pgworker/backups/c1/shard1/wal", ct))
            .Value.Should().BeNull("COMPLETED сбрасывает цепочку (AC4)");
    }

    // AAA (t10-в): SQL-гейт не сходится дольше PatroniBootSec → permanent-FAILED
    // «мастер не принял SQL» (фиксированные часы: тик 1 фиксирует since, тик 2
    // после +700 с > 600).
    [Fact]
    public async Task Rejoin_SQL_гейт_сверх_бюджета_permanent_FAILED()
    {
        // Arrange — транспорт рвётся (connection refused)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("c1");
        await SeedTwoNodeAllocAsync();
        var clock = new MutableClock();
        var driver = new TestDriver(new StubScaleDriver(), new FakeBackupEngine());
        var process = BuildProcess(new FakeBackupS3(), driver, clock: clock,
            patroni: new PatroniHandler { Ready = true }, db: new FakeDb { FailScalar = true });
        var op = await SeedRestoreAsync("c1", "shard1", "20260911122102Z",
            backupId: "20260910120000Z", state: RestoreStatus.Rejoining);
        (await _claims.TryClaimClusterAsync("c1", ct)).Value.Should().BeTrue();

        // Act — тик 1 (since зафиксирован), тик 2 после бюджета
        (await process.TickAsync(BuildTwoNodeSnap(), await BackupsFromEtcdAsync("c1"), ct))
            .Value.Should().Be(ProcessOutcome.InProgress);
        clock.Now = clock.Now.AddSeconds(700);
        (await process.TickAsync(BuildTwoNodeSnap(), await BackupsFromEtcdAsync("c1"), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — permanent-FAILED с причиной и диагностикой
        var failed = (await ReadRestoresAsync("c1", "shard1")).Single(r => r.Id == op.Id);
        failed.State.Should().Be(RestoreStatus.Failed);
        failed.Error.Should().Contain("мастер не принял SQL за 600 с");
        failed.Error.Should().Contain("connection refused (fake)");
        failed.Error.Should().Contain("members=", "снимок Patroni-диагностики в причине");
    }

    // AAA (t10-г): takeover — инстанс A застрял в SQL-ожидании (REJOINING в
    // etcd — истина), инстанс B (свежий, без in-memory трекера) продолжает по
    // статусу: мастер принял SQL → COMPLETED (waitKey стабилен:
    // {cluster}/{shard}/{op.Id} — от инстанса не зависит).
    [Fact]
    public async Task Rejoin_SQL_гейт_takeover_продолжает_по_статусу()
    {
        // Arrange — A: Patroni готов, SQL не готова
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("c1");
        await SeedTwoNodeAllocAsync();
        var processA = BuildProcess(new FakeBackupS3(),
            new TestDriver(new StubScaleDriver(), new FakeBackupEngine()),
            patroni: new PatroniHandler { Ready = true }, db: new FakeDb { Scalar = true });
        var op = await SeedRestoreAsync("c1", "shard1", "20260911122103Z",
            backupId: "20260910120000Z", state: RestoreStatus.Rejoining);
        (await _claims.TryClaimClusterAsync("c1", ct)).Value.Should().BeTrue();
        (await processA.TickAsync(BuildTwoNodeSnap(), await BackupsFromEtcdAsync("c1"), ct))
            .Value.Should().Be(ProcessOutcome.InProgress);

        // Act — B: тот же etcd-статус, SQL готова
        var innerB = new StubScaleDriver();
        var processB = BuildProcess(new FakeBackupS3(), new TestDriver(innerB, new FakeBackupEngine()),
            patroni: new PatroniHandler { Ready = true }, db: new FakeDb { Scalar = false });
        (await processB.TickAsync(BuildTwoNodeSnap(), await BackupsFromEtcdAsync("c1"), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — B довёл до COMPLETED с ensure нод
        innerB.EnsuredNodes.Should().NotBeEmpty("инстанс B продолжает ensure по etcd-статусу");
        (await ReadRestoresAsync("c1", "shard1")).Single(r => r.Id == op.Id)
            .State.Should().Be(RestoreStatus.Completed);
    }
```

- [x] **Шаг 3.3. Прогнать новые кейсы — убедиться в FAIL**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release \
  --filter "FullyQualifiedName~RestoreProcessTests&FullyQualifiedName~Rejoin"
```

Ожидание (стадия red): FAIL кейсов (t10-а) и (t10-в) (гейта нет — тик сразу доводит до COMPLETED, ассерты REJOINING/FAILED не сходятся); (t10-б) и (t10-г) могут быть зелёными — это регрессионные фиксации поведения, их краснота не обязательна.

- [x] **Шаг 3.4. Реализовать гейт в RejoinAsync (код — по фактическому состоянию worktree, коммит `2102259`)**

`src/PgWorker.Backups/Process/RestoreProcess.cs`. Между блоком `if (!allStarted) return await RejoinWaitAsync(…)` (:608–609) и циклом `foreach (var node in ordered)` постановки `RUNNING` (:614) вставить:

```csharp
        // SQL-гейт мастера (t10, arch/19 §3.5): Patroni-пробы живы и в
        // переходном окне рестарта postmaster (57P01 у клиентов), а COMPLETED
        // обязан означать закрытое окно — после него планировщик полных
        // немедленно стартует пересъём, тесты/панель читают. Проба фактического
        // постгреса ПЕРВОЙ ноды (firstReady выше уже требует её лидерство):
        // pg_is_in_recovery()=false закрывает и crash recovery после рестарта
        // (Patroni /primary там уже 200). Запрос БЕЗ ::text — Npgsql-скаляр
        // PG-boolean приходит boxed-bool и сравнивается с false: строковый
        // контракт «"f"» не сработал на PG18 (boolean::text там «false», а не
        // «f», как в PG≤17) — осознанное уточнение spec t10 §4.2 по фактам
        // прогона E2E (решение пользователя, 2026-09-15).
        var adminDsn = ShardEndpoints.AdminDsn(firstAddr, snap.Config.DbName, secrets);
        var masterProbe = await db.ExecuteScalarAsync(adminDsn, "SELECT pg_is_in_recovery()", ct);
        if (masterProbe is not { IsSuccess: true, Value: false })
            return await MasterSqlWaitAsync(cluster, shard.Name, op, waitKey, firstAddr,
                masterProbe.IsSuccess ? "pg_is_in_recovery != false" : masterProbe.Error!.Message, ct);
```

И перед `_rejoinWaitSince.TryRemove(waitKey, out _);` (:635, успешный COMPLETED-путь) добавить снятие sql-ключа:

```csharp
        _rejoinWaitSince.TryRemove($"{waitKey}/sql", out _);
```

Добавить метод рядом с `RejoinWaitAsync` (после него):

```csharp
    // Бюджет SQL-ожидания мастера (t10): образец RejoinWaitAsync — тот же
    // трекер _rejoinWaitSince (суффикс «/sql» отделяет от Patroni-ожидания),
    // тот же бюджет PatroniBootSec; телеметрия каждые ~10 тиков — окно обязано
    // объясняться журналом без перезапуска. Никаких мутаций до готовности
    // (TransientAsync — тик повторит пробу). Бюджет исчерпан → permanent-FAILED
    // со снимком Patroni-диагностики (по образцу RejoinWaitAsync).
    private async Task<Result<ProcessOutcome>> MasterSqlWaitAsync(
        string cluster, string shard, RestoreOperationState op, string waitKey,
        NodeAddress? firstAddr, string? lastError, CancellationToken ct)
    {
        var sqlKey = $"{waitKey}/sql";
        var now = NowUnix();
        var since = _rejoinWaitSince.GetOrAdd(sqlKey, now);
        var waited = now - since;
        if (waited > thresholds.PatroniBootSec)
        {
            _rejoinWaitSince.TryRemove(sqlKey, out _);
            var snapshot = "";
            if (firstAddr is { } addr)
            {
                var diag = await probe.GetClusterAsync(addr, ct);
                snapshot = diag.IsSuccess
                    ? "; members=" + string.Join(",",
                        diag.Value.Select(m => $"{m.Name}:{m.Role}:{m.State}"))
                    : "; probe=" + diag.Error!.Message;
            }
            await FailPermanentAsync(cluster, shard, op,
                $"мастер не принял SQL за {thresholds.PatroniBootSec} с (проба: {lastError ?? "-"}){snapshot}", ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }

        if (waited > 0 && waited % 10 == 0)
            logger?.LogWarning(
                "backup-restore {Cluster}/{Shard}/{Op}: master-sql-wait {Waited}s/{Budget}s, addr {Addr}: {Error}",
                cluster, shard, op.Id, waited, thresholds.PatroniBootSec,
                firstAddr?.ToString() ?? "-", lastError ?? "-");

        return await TransientAsync(cluster, $"master-sql-wait/{shard}/{op.Id}", lastError, ct);
    }
```

- [x] **Шаг 3.5. Прогнать весь RestoreProcessTests — PASS** *(исполнено в Фазе 6)*

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release \
  --filter FullyQualifiedName~RestoreProcessTests
```

Ожидание: PASS — 4 новых кейса и ВСЕ старые (старые идут с дефолтным фейком «готова» `Scalar = false`, их поведение не меняется — spec §8). Факт Фазы 6: после коммита `2102259` интеграционная серия зелёная — 210/210, включая кейсы t10-а…г.

- [x] **Шаг 3.6. Коммит** *(исполнено в Фазе 6)*

```bash
git add src/PgWorker.Backups/Process/RestoreProcess.cs src/PgWorker.App/Program.cs \
        src/tests/PgWorker.IntegrationTests/Backups/RestoreProcessTests.cs
git commit -m "feat(backups): SQL-гейт мастера (pg_is_in_recovery) перед COMPLETED restore — бюджет PatroniBootSec, permanent-FAILED при исчерпании (t10)"
```

Факт Фазы 6: гейт закоммичен двумя коммитами — `f2690da` (реализация по плану v2, строкочный контракт `"f"`) и `2102259` (фикс контракта на boxed bool `false` после разбора фейлов E2E; согласовано пользователем через AskUserQuestion).

---

## Task 4: Компонент C — тестовый гейт rs-dr + ретрай чтения

**Вход:** Task 3 закоммичен.

**Действие:** `src/tests/PgWorker.IntegrationTests/E2e/E2eRestoreScenarios.cs`, сценарий `Restore_NewCluster_FromSourcePrefix`: после `RestoreWithRetryAsync(cluster, "shard2", …)` (:330–331) и перед финальным чтением (:335–340) — гейт `WaitPhaseAsync("master-ready", …)` (поллинг `MasterPgAsync` → admin-DSN → `ScalarAsync("SELECT 1")` в try/catch `NpgsqlException`, бюджет 120 с — эталон `E2eBackupScenarios` backup_exec-гвард) и ретрай финального чтения 5×8 с (эталон rs-latest :114–127). Окружение/teardown не меняются.

**Выход:** rs-dr не читает данные, пока мастер не принял SQL; финальное чтение ретраится по эталону rs-latest.

**Проверка:** сборка зелёная (`dotnet build -c Release`); runtime-проверка — Task 5.5. rs-latest/rs-time не затронуты (`git diff` — только один сценарий).

**Связь со spec:** §4.3 (компонент C), §5 Фаза 3, §7.1; ограничение §3.2 (только rs-dr).

- [ ] **Шаг 4.1. Вставить гейт и ретрай в сценарий rs-dr**

Заменить в `E2eRestoreScenarios.cs` фрагмент (строки ~333–340):

```csharp
            // Assert — оба restore COMPLETED (RestoreWithRetry не вернёт FAILED);
            // контрольные строки на месте
            var (drHost, drPort) = await MasterPgAsync(cluster, "shard1", ct);
            var drDsn = DatabaseProvisioner.BuildAdminDsn(drHost, drPort, cluster,
                new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
            var rows = await ScalarAsync(drDsn, "SELECT count(*) FROM dr_probe", ct);
            rows.Should().Be("7", "данные совпадают с моментом бэкапа (RPO = точка полного): "
                + await DumpDiagnosticsAsync(cluster, "shard1"));
```

на:

```csharp
            // Assert — оба restore COMPLETED (RestoreWithRetry не вернёт FAILED);
            // контрольные строки на месте.

            // Гейт готовности мастера (t10): COMPLETED не гарантирует закрытия
            // рестарт-окна postmaster (Npgsql 57P01) — SQL-проба SELECT 1 до
            // финального чтения (эталон — backup_exec-гвард E2eBackupScenarios;
            // docs/e2e-launch.md §2: новый ожидания-участок — только через
            // WaitPhaseAsync). Бюджет 120 с согласован с PatroniBootSec хоста.
            var masterReady = await WaitPhaseAsync("master-ready", async () =>
            {
                try
                {
                    var (gHost, gPort) = await MasterPgAsync(cluster, "shard1", ct);
                    var gDsn = DatabaseProvisioner.BuildAdminDsn(gHost, gPort, cluster,
                        new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
                    await ScalarAsync(gDsn, "SELECT 1", ct);
                    return true;
                }
                catch (NpgsqlException)
                {
                    return false; // рестарт-окно — поллинг повторит
                }
            }, TimeSpan.FromSeconds(120), ct);
            masterReady.Should().BeTrue(
                "мастер обязан принять SQL до финального чтения (гейт t10): "
                + await DumpDiagnosticsAsync(cluster, "shard1"));

            // Финальное чтение с ретраем (эталон rs-latest): даже после гейта
            // Patroni может доводить конфиг мастера (рестарт рвёт соединения) —
            // это переходный оконный артефакт rejoin'а.
            var (drHost, drPort) = await MasterPgAsync(cluster, "shard1", ct);
            var drDsn = DatabaseProvisioner.BuildAdminDsn(drHost, drPort, cluster,
                new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
            var rows = "";
            for (var attempt = 1; attempt <= 5; attempt++)
            {
                try
                {
                    rows = await ScalarAsync(drDsn, "SELECT count(*) FROM dr_probe", ct);
                    break;
                }
                catch (NpgsqlException) when (attempt < 5)
                {
                    await Task.Delay(8000, ct);
                }
            }

            rows.Should().Be("7", "данные совпадают с моментом бэкапа (RPO = точка полного): "
                + await DumpDiagnosticsAsync(cluster, "shard1"));
```

- [ ] **Шаг 4.2. Сборка (0 warnings)**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
```

Ожидание: Build succeeded, 0 Warning(s) (TreatWarningsAsErrors).

- [ ] **Шаг 4.3. Коммит**

```bash
git add src/tests/PgWorker.IntegrationTests/E2e/E2eRestoreScenarios.cs
git commit -m "test(e2e): rs-dr — гейт master-ready (SQL-проба SELECT 1 через WaitPhaseAsync) + ретрай финального чтения по эталону rs-latest (t10)"
```

---

## Task 5: Прогоны серий — фон + ОНЛАЙН-анализ + зачистка после каждой

**Вход:** Tasks 1–4 закоммичены.

**Действие:** серия за серией на свежем Release: build → юниты → интеграция (без E2e) → docker-E2E маркер `Scale_AddEmptyShard` → docker-E2E `E2eRestoreScenarios` (все 3 сценария). КАЖДАЯ длинная команда — фоновый запуск; мониторинг — короткими циклами опроса (≤30 с, максимум ≤120 с), логи/артефакты читаются ОНЛАЙН по мере появления; при первых признаках падения — разбор в процессе, НЕ дожидаясь финала серии. После КАЖДОЙ серии (дождавшись финальной строки) — зачистка контейнеров и сетей; следующая серия — только поверх зачищенной.

**Выход:** зелёные серии; артефактный факт устранения корня (критерий §7.5).

**Проверка:** критерии приёмки spec §7.1–7.6 (см. «Критерии приёмки» ниже).

**Связь со spec:** §5 Фаза 4, §7 (все критерии), §6 (телеметрия/таймауты).

**Общие правила исполнения (обязательны для каждого шага ниже):**
- Команда — фоном (`run_in_background`); опрос вывода — блокирующими чтениями по 30 с (никогда не спать >30 с подряд); читать вывод ПО МЕРЕ появления.
- E2E-падение → `MarkFailed`-режим: разбор по `/tmp/pgw-e2e-artifacts-<guid>/` (container-*.log, host-*.log, slow-phase-*), README-cleanup.txt; перезапуск — ТОЛЬКО после полного анализа и согласия пользователя.
- Зачистка после серии (страховочный гейт, docs/e2e-isolation.md:158): проверить, что не снесён dev-стенд (его контейнеры не трогаем), удалить остаточные тестовые контейнеры прогонов (по именам/guid из логов серии) и выполнить `docker network prune -f`; каталоги артефактов `/tmp/pgw-e2e-artifacts-*` НЕ удалять до конца разбора.

- [ ] **Шаг 5.1. Сборка Release (фон)**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
```

Ожидание: `Build succeeded`, `0 Warning(s)`, `0 Error(s)`.

- [ ] **Шаг 5.2. Юниты (фон → онлайн-мониторинг → зачистка)**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release \
  --filter FullyQualifiedName~PgWorker.UnitTests --no-build
```

Ожидание: все зелёные, вкл. `RestoreJobCommandTests.Build_PristineAutoConf_CleansPatroniArchiveParams`. После финальной строки — зачистка (юниты docker не поднимают — контрольный `docker ps -a` на остатки + `docker network prune -f`).

- [ ] **Шаг 5.3. Интеграция без E2E (фон → онлайн-мониторинг → зачистка)**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release \
  --filter "FullyQualifiedName~PgWorker.IntegrationTests&FullyQualifiedName!~E2e" --no-build
```

Ожидание: все зелёные, вкл. 4 новых кейса `RestoreProcessTests` (t10-а…г). После финальной строки — зачистка контейнеров/сетей серии.

- [ ] **Шаг 5.4. Docker-E2E маркер `Scale_AddEmptyShard` (фон → онлайн → зачистка)**

Меняется код воркера — правило AGENTS.md (минимум-маркер; полный E2eFixture-прогон при изменении provisioning/restore-процессов — следующий шаг):

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release \
  --filter FullyQualifiedName~Scale_AddEmptyShard --no-build
```

Онлайн: следить за `[PHASE]`-строками и журналом; фаза >60 с — артефакты slow-phase снимаются автоматически, прочитать их. После финальной строки — зачистка контейнеров/сетей серии.

- [ ] **Шаг 5.5. Docker-E2E `E2eRestoreScenarios` — все 3 сценария (фон → онлайн → разбор артефактов → зачистка)**

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release \
  --filter FullyQualifiedName~E2eRestoreScenarios --no-build
```

Онлайн-мониторинг в процессе: журнал теста (строки `[PHASE]`, в т.ч. новая `master-ready` в rs-dr), `host.log` воркеров (должна пройти фаза `master-sql-wait/...` при необходимости), docker-логи нод после restore-джоб.

После зелёного финала — проверка критерия §7.5 по артефактам прогона (каталог из строки `e2e[rs-dr]: телеметрия … → …`):

```bash
# (1) «Pending restart» после restore-джоб больше не возникает — факт устранения корня
grep -rn "Setting 'Pending restart' flag" /tmp/pgw-e2e-artifacts-<guid>/ || echo "OK: Pending restart отсутствует"
# (2) плановые full-джобы после restore не фейлятся в переходном окне
grep -rln "pg_basebackup failed" /tmp/pgw-e2e-artifacts-<guid>/ || echo "OK: full-джобы без фейлов окна"
```

Ожидание: оба grep пусты (echo OK). После разбора — зачистка контейнеров/сетей серии.

- [ ] **Шаг 5.6. Коммит (если прогоны потребовали правок — по фактам разбора; зелёный прогон правок не требует)**

---

## Task 6: Снятие тега t10 из roadmap (мерж-гейт)

**Вход:** Tasks 1–5 завершены, все серии зелёные, артефактный факт §7.5 подтверждён.

**Действие:** удалить пункт `t10-rs-dr-master-readiness-gate` из `arch/roadmap/pgworker.md` — строки **18–25** (весь пункт от `- **\`t10-rs-dr-master-readiness-gate\`** — доработка E2E-сценария rs-dr` до `(логи \`/tmp/t09-rerun/pg-main-baseline.log\`).` включительно). Проверить отсутствие `←`-зависимостей на t10 в остальных файлах `arch/roadmap/` (сегодня их нет — grep пуст; если появились — убрать и их, тем же коммитом). Никаких пометок «закрыта/реализована» — пункт удаляется.

**Выход:** roadmap не содержит несделанных ссылок на t10; коммит уйдёт в мерж (правило: тег снимается тем же коммитом мержа в `main`).

**Проверка:** `grep -rn "t10-rs-dr" arch/roadmap/` — пусто.

**Связь со spec:** §5 Фаза 5; правило AGENTS.md «Roadmap — только несделанные задачи».

- [ ] **Шаг 6.1. Удалить пункт и проверить зависимости**

```bash
# правка arch/roadmap/pgworker.md — удалить пункт t10 целиком (строки 18-25)
grep -rn "t10-rs-dr" arch/roadmap/   # ожидание: пусто
```

- [ ] **Шаг 6.2. Коммит**

```bash
git add arch/roadmap/pgworker.md
git commit -m "chore(roadmap): снять тег t10-rs-dr-master-readiness-gate (задача исполнена)"
```

Мерж в `main` и пуш — ТОЛЬКО по отдельной явной просьбе пользователя (AGENTS.base §6); на гейте мержа пользователю показывается итог прогона Task 5.

---

## Критерии приёмки (spec §7 — проверяются по ходу и в конце)

1. rs-dr зелёный на свежем Release (Шаг 5.5): `PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~E2eRestoreScenarios`.
2. Docker-E2E мерж-гейт: маркер `Scale_AddEmptyShard` (Шаг 5.4) + полный `E2eRestoreScenarios` (Шаг 5.5) зелёные.
3. Полные серии юнитов и интеграции зелёные (Шаги 5.2–5.3); после каждой серии — зачистка контейнеров/сетей.
4. Новые кейсы A/B зелёные (Шаги 2.4, 3.5).
5. По артефактам E2E: нет `Setting 'Pending restart' flag` после restore-джоб; плановые full-джобы не фейлятся в окне (Шаг 5.5).
6. Онлайн-анализ логов при всех прогонах — правило исполнено (раздел Global Constraints и преамбула Task 5).

## Риски исполнения (из spec §8 — что делать, если всплывёт)

- Новое «Pending restart (X)» после фикса A в прогонах 5.4/5.5 → расширить sed-строку той же правкой по факту (YAGNI: отдельного обобщения не делать).
- Кейс (t10-а) недетерминирован → проверить, что фейк покрывает ОБА transient-вида (`Scalar = true` и FailScalar) — оба через `FakeDb`.
- rs-time: гейт B удлиняет путь до COMPLETED не более чем на `PatroniBootSec` — бюджет wal-reinit 300 с имеет запас; при фейле rs-time по таймауту — разбирать по журналу, не расширять бюджеты самовольно.

## Self-review v2 — закрытие замечаний ревью plan↔spec (Фаза 4, CHANGES_REQUESTED)

- **№1 (high) — сид wal-ключа в кейсах (t10-а)/(t10-б): ЗАКРЫТО.** Оба кейса сидируют wal-ключ полным JSON контракта `BackupsParser.TryParseWal` (обязательные slot/master_node/chain_start_segment/last_received_segment/last_uploaded_segment/last_uploaded_unix) — точная копия прецедента `RestoreProcessTests.cs:818-820`; в Шаге 3.2 добавлено предупреждение ⚠️ против минимального `{"state":"ACTIVE"}` (ошибки парсинга → `errors.Should().BeEmpty()` в `BackupsFromEtcdAsync` упал бы до `TickAsync`). Кейсы (t10-в)/(t10-г) wal-ключ не сидируют намеренно: их префикс `/pgworker/backups/c1/` содержит только restore-ключ, `BackupsParser` парсится без ошибок — прецедент существующих кейсов `Rejoin_сверх_PatroniBootSec_permanent_FAILED` и `Takeover_новый_инстанс_продолжает_по_статусу` (сид только portalloc), а кейсам (в)/(г) ассерты по wal-ключу не нужны ((в) проверяет FAILED, (г) — COMPLETED через ensure-вызовы).
- **№2 (low) — проверка Задачи 1: ЗАКРЫТО.** Проверка сменена с литерала «инцидент t10» на подсчёт: `grep -c "t10" arch/19-backups.md` → ≥ 2 (вставка 1.1 содержит «инцидент t10», вставка 1.2 — «SQL-гейт мастера (t10)»; текст вставок согласован со spec и не менялся).
- **№3 (low) — диапазон строк пункта t10 в roadmap: ЗАКРЫТО.** Задача 6 и Шаг 6.1 указывают строки 18–25 (пункт заканчивается «(логи /tmp/t09-rerun/pg-main-baseline.log).» на строке 25).
- **№4 (рекомендация) — фиксация ::text как осознанного уточнения spec: ЗАКРЫТО в v2, ОТМЕНЕНО в v3.** Предложенное v2 решение (`::text` + строка `"f"`) опровергнуто исполнением: PG18 для `boolean::text` возвращает `"false"` — строковый паттерн не сходился бы никогда (все 3 сценария E2E упали на первом полном прогоне). Фактический контракт (коммит `2102259`, решение пользователя): проба без `::text`, сравнение boxed bool `false`. См. Self-review v3.

## Self-review v3 — синхронизация с исполнением (Фаза 6, коммит `2102259`)

- **Что произошло:** реализация по плану v2 (`pg_is_in_recovery()::text` + строковый паттерн `Value: "f"`) прошла юниты/интеграцию (фейк возвращал строку — паттерн сходился), но провалила первый полный E2E (Шаг 5.5) во всех 3 сценариях: реальный PostgreSQL 18 (spilo-18) для `boolean::text` возвращает `"false"`, а не `"f"` (поведение PG≤17) — прод-гейт не сходился никогда, restore зависали в `master-sql-wait` до бюджета → FAILED. Разбор фейлов выполнен онлайн по артефактам до перезапуска (правило docs/e2e-launch.md §4).
- **Решение пользователя** (AskUserQuestion в ходе исполнения, зафиксировано коммитом `2102259` и правкой spec): проба `SELECT pg_is_in_recovery()` БЕЗ `::text`; Npgsql возвращает boxed bool; гейт пропускает при `Value is false`; диагностический текст не-готовности — `pg_is_in_recovery != false`. Spec синхронизирован resume spec-writer'ом (шапка «Правка по итогам исполнения», §4.2, §8).
- **Правки плана v3:** (1) секция «Осознанное уточнение spec» в шапке переписана под фактический контракт с ПРИМЕЧАНИЕМ PG18 и ссылкой на коммит `2102259`/правку spec §4.2; (2) Global Constraints — строка про пробу заменена на boxed bool `false` без `::text`; (3) Шаг 3.4 — код и комментарий приведены к фактическому состоянию worktree (свёрено с `src/PgWorker.Backups/Process/RestoreProcess.cs`, коммит `2102259`); фейк `FakeDb.Scalar` — `object?` (дефолт `false`), transient-вид «висит в recovery» — `Scalar = true` (не строка `"t"`), успех — `Scalar = false`; кейс-комментарии «(pg_is_in_recovery=true/false)»; (4) Шаги 3.1–3.6 помечены исполненными (`[x]`) с фактами: интеграционная серия 210/210 после `2102259`, коммиты `f2690da` + `2102259`; (5) Проверка Task 3 дополнена фактом исполнения.
- **Статус остальных задач (факт, для навигации):** Task 1 — коммит `6ec0608`; Task 2 — `5a61a21` (юниты 870/870); Task 4 — `0b6f174`; Task 5 — прогоны зелёные: интеграция 210/210, `E2eRestoreScenarios` 3/3 (14 м 1 с), маркер `Scale_AddEmptyShard` 1/1 (критерии §7.1–7.4 подтверждены; §7.5 — по артефактам прогона). Task 6 (снятие тега из roadmap) — НЕ исполнен, мерж-гейт впереди.
- **Шаги 3.x исполнены в Фазе 6 с отклонением от v2** (`::text`/"f" → boxed bool false), отклонение согласовано пользователем; типовая согласованность плана с кодом восстановлена (паттерн `Value: false`, текст `pg_is_in_recovery != false`, фейк `object?`), консистентность остальных секций (Tasks 1/2/4/5/6, критерии, риски) сохранена без изменений.
