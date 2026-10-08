# t19-slot-auto-recreate — план реализации

> **Для исполняющих агентов:** ОБЯЗАТЕЛЬНЫЙ SUB-SKILL: superpowers:subagent-driven-development
> (рекомендуется) или superpowers:executing-plans — исполнять по задаче за раз.
> Шаги используют синтаксис чекбоксов (`- [ ]`) для отслеживания.

**Цель:** автоматическое лечение потерянных wal-слотов (`wal_status='lost'`) воркером
вместо ручного операторского разбора: lost одного источника при живом втором —
авто-recreate слота без BROKEN; lost/исчезновение всех источников — существующий
BROKEN-путь t07 с пересъёмом полного.

**Архитектура:** SQL-слой (`IWalSqlExecutor`) расширяется зондом
`SlotProbeAsync` (существование + `wal_status`) и идемпотентными
`EnsureSlotAliveAsync`/`RecreateSlotAsync`; `WalStreamProcess` шаг (3) переводит
правило «слот исчез» с `All(!Exists)` на `All(!Alive)` (`alive = Exists && WalStatus != "lost"`),
добавляет ветку «lost при живом втором источнике» (recreate + журнальная фаза
`slot-recreate/<X>`, не BROKEN); панель меняет remedy `slot-wal-lost` на
`WorkerAuto`; runbook получает раздел «Потеря wal-слота». Приёмник, RecreateNodeHandler,
NodeSupervisor, etcd-схема — не меняются.

**Стек:** .NET 10, C# (`LangVersion=latest`, `Nullable=enable`,
`TreatWarningsAsErrors=true`), Npgsql, xunit.v3 + FluentAssertions (AAA),
testcontainers (динамические порты), docker E2E на свежем Release.

**Спека:** `docs/superpowers/2026-10-08-t19-slot-auto-recreate/spec.md`
(исполнитель читает spec и этот план; план аргументируется от spec).

Все пути в плане — от корня worktree `/Users/demakaev/ZCodeProject/worktrees/feat-t19-slot-auto-recreate`.

## Глобальные ограничения (из spec §5 и AGENTS.md — обязательны для каждой задачи)

- .NET 10, `Nullable=enable`, `TreatWarningsAsErrors=true`: сборка обязана быть
  0 warning / 0 error; все новые публичные члены — русские doc-комментарии.
- НЕ трогаем: `RecreateNodeHandler`, `NodeSupervisor`, HA-надзор,
  `src/PgWorker.WalReceiver` (образ `pgworker-wal`), SQL-пробу панели.
- НЕТ новых etcd-ключей/полей ключа `wal`; НЕТ конфиг-опций (автоматика — дефолт).
- Панель остаётся читателем; словарь таблицы arch/adminpanel/03-panels.md не меняется.
- Тесты docker: порты только динамические (`WithPortBinding(..., assignRandomHostPort:
  true)` + `GetMappedPublicPort`, либо зонд свободного порта) — никаких литералов.
  Таймауты бута фикстур ≤ 100 c.
- Каждый docker-тест полностью чистит за собой (teardown при любом исходе, own-only).
  После КАЖДОЙ тестовой серии — зачистка контейнеров/сетей (см. Задачу 7).
- Телеметрия E2E: `[PHASE]`-строки на фазы, дамп диагностики в catch, `MarkFailed()`,
  перезапуск упавших тестов для выяснения «что было» — запрещён.
- В `docs/**`/`arch/**` — только текущее/планируемое состояние: в пользовательских
  текстах (runbook, правило панели) НЕ пишем атрибуцию «(t19)» (мерж-коммит задачи
  вычищает теги слитых задач — пишем сразу без тега).
- Тесты — AAA-комментарии (`// Arrange`, `// Act`, `// Assert`).
- arch/19-backups.md уже обновлён фазой spec (arch-first): §3 «Слот», «Правила
  непрерывности», §10 — при расхождении реализации с каноном правится канон в той
  же задаче, где расхождение возникло.

---

## Задача 1 — SQL-слой: `SlotProbeAsync` / `EnsureSlotAliveAsync` / `RecreateSlotAsync` + механическая миграция вызовов

**Вход (предусловие):** ветка worktree чистая; spec прочитан; текущий
`IWalSqlExecutor` имеет `SlotExistsAsync`/`EnsureSlotAsync` (используются
`WalStreamProcess` шаг (3) и `BreakAsync`).

**Действие (файлы):**
- Modify: `src/PgWorker.Backups/Sql/IWalSqlExecutor.cs` — новый контракт.
- Modify: `src/PgWorker.Backups/Sql/NpgsqlWalSqlExecutor.cs` — реализация.
- Modify: `src/PgWorker.Backups/WalStreamProcess.cs` — ТОЛЬКО механическая
  миграция вызовов (поведение не меняется: `wal_status` пока не участвует в решении).
- Modify: `src/tests/PgWorker.IntegrationTests/Backups/FakeBackupDeps.cs` —
  `FakeWalSqlExecutor` под новый контракт (+ `LostByDsn`, `Calls`).
- Modify: `src/tests/PgWorker.IntegrationTests/Backups/WalSqlTests.cs` —
  существующий Fact на новый API.

**Выход:** контракт трёх методов жив, всё собирается 0/0, существующие
`WalStreamProcessTests` зелёные (регресс-базис), `WalSqlTests` зелёный на docker.

**Проверка:** `dotnet build src/PgWorker.slnx -c Debug` → 0 warning/0 error;
`DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Debug
--filter "FullyQualifiedName~WalSqlTests|FullyQualifiedName~WalStreamProcessTests"`
→ все PASS; после серии — зачистка (Задача 7, шаг Г).

**Связь со spec:** §3.1 (полностью), критерий 3/4 (частично — API), Ф4-Ф1.

**Interfaces (производит):**

```csharp
Task<Result<(bool Exists, string? WalStatus)>> SlotProbeAsync(string adminDsn, string slot, CancellationToken ct);
Task<Result> EnsureSlotAliveAsync(string adminDsn, string slot, CancellationToken ct);
Task<Result> RecreateSlotAsync(string adminDsn, string slot, CancellationToken ct);
Task<Result<(string Lsn, int Tli)>> CurrentWalAsync(string adminDsn, CancellationToken ct); // без изменений
```

- [ ] **Шаг 1: Обновить WalSqlTests на новый контракт (failing-тест)**

Существующий Fact `EnsureSlot_идемпотентен_CurrentWal_возвращает_lsn_и_tli`
переписать (контейнер postgres:18-alpine оставить как есть — динамический порт,
`pg_isready` ≤ 45 c):

```csharp
[Fact]
public async Task Probe_EnsureAlive_Recreate_идемпотентны_на_живом_и_отсутствующем_слоте()
{
    // Arrange
    DockerTrait.SkipIfUnavailable();
    var ct = TestContext.Current.CancellationToken;
    await using var postgres = new ContainerBuilder("postgres:18-alpine")
        .WithEnvironment("POSTGRES_PASSWORD", Password)
        .WithPortBinding(5432, assignRandomHostPort: true)
        .WithWaitStrategy(Wait.ForUnixContainer().UntilCommandIsCompleted(
            "pg_isready -U postgres",
            w => w.WithTimeout(TimeSpan.FromSeconds(45)))) // ≤ 100 c: падаем быстро
        .Build();
    await postgres.StartAsync(ct);
    var dsn = $"Host=localhost;Port={postgres.GetMappedPublicPort(5432)};" +
              $"Username=postgres;Password={Password};Database=postgres;SSL Mode=Disable";
    var sql = new NpgsqlWalSqlExecutor();

    // Act
    var before = await sql.SlotProbeAsync(dsn, "pgw_bkp_test", ct);
    var ensured = await sql.EnsureSlotAliveAsync(dsn, "pgw_bkp_test", ct);
    var again = await sql.EnsureSlotAliveAsync(dsn, "pgw_bkp_test", ct);
    var probe = await sql.SlotProbeAsync(dsn, "pgw_bkp_test", ct);
    var wal = await sql.CurrentWalAsync(dsn, ct);
    // recreate на отсутствующем слоте: drop без undefined_object-ошибки → create
    var recreatedMissing = await sql.RecreateSlotAsync(dsn, "pgw_bkp_other", ct);
    var probeOther = await sql.SlotProbeAsync(dsn, "pgw_bkp_other", ct);

    // Assert
    before.Value.Exists.Should().BeFalse();
    before.Value.WalStatus.Should().BeNull("строки нет — статус отсутствует");
    ensured.IsSuccess.Should().BeTrue();
    again.IsSuccess.Should().BeTrue("повтор при живом слоте — идемпотентность");
    probe.Value.Exists.Should().BeTrue();
    probe.Value.WalStatus.Should().NotBe("lost", "свежий reserved-слот жив");
    wal.IsSuccess.Should().BeTrue();
    wal.Value.Lsn.Should().MatchRegex("^[0-9A-F]+/[0-9A-F]+$");
    wal.Value.Tli.Should().BeGreaterThanOrEqualTo(1);
    recreatedMissing.IsSuccess.Should().BeTrue("drop отсутствующего (undefined_object) — норм");
    probeOther.Value.Exists.Should().BeTrue();
}
```

- [ ] **Шаг 2: Запустить — убедиться в провале компиляции**

Run: `dotnet build src/PgWorker.slnx -c Debug`
Ожидание: FAIL — `SlotProbeAsync`/`EnsureSlotAliveAsync`/`RecreateSlotAsync` не существуют.

- [ ] **Шаг 3: Новый контракт `IWalSqlExecutor`**

Файл целиком:

```csharp
using PgWorker.Core;

namespace PgWorker.Backups.Sql;

/// <summary>SQL-слой WAL-подсистемы к мастеру шарда (Npgsql, admin-DSN — билдер
/// ShardEndpoints.AdminDsn): зонд/ensure/recreate слота + LSN-зонд лага. Ретраи —
/// тиками процесса (transient), здесь их нет (образец IMoveSqlExecutor).</summary>
public interface IWalSqlExecutor
{
    /// <summary>Зонд слота: (Exists, WalStatus) из pg_replication_slots. Строки
    /// нет → (false, null); существующий слот с null-статусом (старые PG/edge) —
    /// трактуется вызывающим как живой: лечим только явный 'lost' (arch/19 §3).</summary>
    Task<Result<(bool Exists, string? WalStatus)>> SlotProbeAsync(string adminDsn, string slot, CancellationToken ct);

    /// <summary>Идемпотентно, self-contained (зонд внутри): отсутствует → create
    /// (immediate+reserved); существует и wal_status='lost' → drop+create; жив —
    /// не трогать. Вызывающим предварительный зонд не нужен.</summary>
    Task<Result> EnsureSlotAliveAsync(string adminDsn, string slot, CancellationToken ct);

    /// <summary>Пересоздание: drop (undefined_object — норм) + create
    /// immediate+reserved (тот же вызов, что ensure). Идемпотентен; ошибка —
    /// Result.Failed (transient, тик повторит).</summary>
    Task<Result> RecreateSlotAsync(string adminDsn, string slot, CancellationToken ct);

    /// <summary>Текущая позиция записи мастера: (pg_current_wal_lsn()::text, timeline_id).</summary>
    Task<Result<(string Lsn, int Tli)>> CurrentWalAsync(string adminDsn, CancellationToken ct);
}
```

- [ ] **Шаг 4: Реализация `NpgsqlWalSqlExecutor`**

`SlotExistsAsync`/`EnsureSlotAsync` заменить (create-тело переиспользовано из
ensure в приватный `CreateSlotAsync`; `CurrentWalAsync` не трогать):

```csharp
public async Task<Result<(bool Exists, string? WalStatus)>> SlotProbeAsync(string adminDsn, string slot, CancellationToken ct)
{
    try
    {
        await using var connection = new NpgsqlConnection(adminDsn);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand(
            "SELECT wal_status FROM pg_replication_slots WHERE slot_name = $1", connection)
        {
            Parameters = { new() { Value = slot } },
        };
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            return Result<(bool, string?)>.Success((false, null)); // строки нет
        // null-статус существующего слота (старые PG/edge) — как живой: лечим только 'lost'
        var status = reader.IsDBNull(0) ? null : reader.GetString(0);
        return Result<(bool, string?)>.Success((true, status));
    }
    catch (Exception e)
    {
        return Result<(bool, string?)>.Failed(new ApplicationException($"слот-зонд {slot}: {e.Message}", e));
    }
}

public async Task<Result> EnsureSlotAliveAsync(string adminDsn, string slot, CancellationToken ct)
{
    var probe = await SlotProbeAsync(adminDsn, slot, ct);
    if (!probe.IsSuccess)
        return Result.Failed(probe.Error!);
    if (!probe.Value.Exists)
        return await CreateSlotAsync(adminDsn, slot, ct); // отсутствует → create
    if (probe.Value.WalStatus == "lost")
        return await RecreateSlotAsync(adminDsn, slot, ct); // потерян → drop+create
    return Result.Success(); // жив — не трогать
}

public async Task<Result> RecreateSlotAsync(string adminDsn, string slot, CancellationToken ct)
{
    try
    {
        await using var connection = new NpgsqlConnection(adminDsn);
        await connection.OpenAsync(ct);
        try
        {
            await using var drop = new NpgsqlCommand(
                "SELECT pg_drop_replication_slot($1)", connection)
            {
                Parameters = { new() { Value = slot } },
            };
            await drop.ExecuteNonQueryAsync(ct);
        }
        catch (PostgresException e) when (e.SqlState == "42704") // undefined_object
        {
            // слота нет — drop пропущен, create ниже создаст
        }

        return await CreateSlotAsync(adminDsn, slot, ct);
    }
    catch (Exception e)
    {
        return Result.Failed(new ApplicationException($"recreate слота {slot}: {e.Message}", e));
    }
}

// immediate+reserved (arch/19 §3): повтор при живом слоте — duplicate_object →
// успех (идемпотентность); общий create для ensure/recreate.
private static async Task<Result> CreateSlotAsync(string adminDsn, string slot, CancellationToken ct)
{
    try
    {
        await using var connection = new NpgsqlConnection(adminDsn);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand(
            "SELECT pg_create_physical_replication_slot($1, true)", connection)
        {
            Parameters = { new() { Value = slot } },
        };
        await command.ExecuteNonQueryAsync(ct);
        return Result.Success();
    }
    catch (PostgresException e) when (e.SqlState == "42710") // duplicate_object
    {
        return Result.Success(); // слот уже есть — идемпотентность
    }
    catch (Exception e)
    {
        return Result.Failed(new ApplicationException($"create слота {slot}: {e.Message}", e));
    }
}
```

Примечание: сигнатура кортежа в `Result` — `Result<(bool Exists, string? WalStatus)>`
допустима именованным кортежем; при необходимости компилятора писать
`Result<(bool, string?)>` в статических вызовах — приводить единообразно.

- [ ] **Шаг 5: Механическая миграция `WalStreamProcess` (без новой логики)**

- Строки шага (3) (сборка `slotProbes`): `var exists = await sql.SlotExistsAsync(dsn, slot, ct);`
  → `var probe = await sql.SlotProbeAsync(dsn, slot, ct);` с проверкой
  `if (!probe.IsSuccess) throw new ApplicationException($"слот-зонд {slot}@{src.Node}: {probe.Error!.Message}");`
  и `slotProbes.Add((src, dsn, probe.Value.Exists));` — тип списка пока не меняем
  (логика `All(!Exists)` остаётся до Задачи 3).
- Оба вызова `sql.EnsureSlotAsync(...)` (шаг (3), ensure-ветки) и вызов в
  `BreakAsync` → `sql.EnsureSlotAliveAsync(...)` (тексты исключений оставить прежними).

- [ ] **Шаг 6: `FakeWalSqlExecutor` под новый контракт (полный, с lost-состоянием)**

Заменить класс в `FakeBackupDeps.cs` (тело `FakeBackupS3`/`FakePatroni` не трогать):

```csharp
// Фейковый SQL-слой: слоты в памяти per-инстансу (t27: ключ — adminDsn источника,
// слоты разных нод независимы), LSN управляется тестом (AAA-Act). LostByDsn —
// подмножество слотов в wal_status='lost' (t19); Calls — журнал мутаций
// (create/drop) для ассертов идемпотентности.
public sealed class FakeWalSqlExecutor : IWalSqlExecutor
{
    public ConcurrentDictionary<string, HashSet<string>> SlotsByDsn { get; } = new();

    // Слоты в статусе lost (подмножество SlotsByDsn того же DSN).
    public ConcurrentDictionary<string, HashSet<string>> LostByDsn { get; } = new();

    // Журнал мутаций слота: ("create"|"drop", dsn, slot) — для ассертов
    // «нулевых мутаций» и «drop+create».
    public List<(string Op, string Dsn, string Slot)> Calls { get; } = [];

    public (string Lsn, int Tli) Current { get; set; } = ("0/1000000", 1);

    public Task<Result<(bool Exists, string? WalStatus)>> SlotProbeAsync(string adminDsn, string slot, CancellationToken ct)
        => Task.FromResult(Result<(bool, string?)>.Success(
            SlotsByDsn.TryGetValue(adminDsn, out var slots) && slots.Contains(slot)
                ? (true, IsLost(adminDsn, slot) ? "lost" : "reserved")
                : (false, null)));

    public Task<Result> EnsureSlotAliveAsync(string adminDsn, string slot, CancellationToken ct)
    {
        var exists = SlotsByDsn.TryGetValue(adminDsn, out var slots) && slots.Contains(slot);
        if (exists && !IsLost(adminDsn, slot))
            return Task.FromResult(Result.Success()); // жив — не трогать
        return RecreateSlotAsync(adminDsn, slot, ct); // отсутствует/lost → drop-допуск + create
    }

    public Task<Result> RecreateSlotAsync(string adminDsn, string slot, CancellationToken ct)
    {
        if (IsLost(adminDsn, slot))
        {
            Calls.Add(("drop", adminDsn, slot));
            LostByDsn.GetOrAdd(adminDsn, _ => []).Remove(slot);
        }

        Calls.Add(("create", adminDsn, slot));
        SlotsByDsn.GetOrAdd(adminDsn, _ => []).Add(slot);
        return Task.FromResult(Result.Success());
    }

    public Task<Result<(string Lsn, int Tli)>> CurrentWalAsync(string adminDsn, CancellationToken ct)
        => Task.FromResult(Result<(string, int)>.Success(Current));

    private bool IsLost(string adminDsn, string slot)
        => LostByDsn.TryGetValue(adminDsn, out var lost) && lost.Contains(slot);
}
```

Существующие тесты, сидирующие `SlotsByDsn.GetOrAdd(...).Add(slot)`, продолжают
работать без правок (lost никем не выставлен → поведение прежнее).

- [ ] **Шаг 7: Сборка + прогоны (зелёный)**

Run: `dotnet build src/PgWorker.slnx -c Debug` → 0/0.
Run: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~WalSqlTests|FullyQualifiedName~WalStreamProcessTests"`
→ все PASS (включая регресс-семейство «слот исчез»: `Слот_исчез_...`,
`Контроль_инвалидация_слота_...`, `Слот_исчез_правило_всех_нод`).
После серии — зачистка (шаг Г Задачи 7).

- [ ] **Шаг 8: Commit**

```bash
git add -A
git commit -m "feat(t19): SQL-слой слота — зонд SlotProbeAsync (существование+wal_status), EnsureSlotAliveAsync (create/recreate/не-трогать), RecreateSlotAsync (drop+create immediate+reserved); механическая миграция WalStreamProcess/FakeWalSqlExecutor (spec §3.1)"
```

---

## Задача 2 — Интеграция lost-рецепта на реальном PG

**Вход:** Задача 1 слита (контракт жив, сборка 0/0).

**Действие (файлы):**
- Modify: `src/tests/PgWorker.IntegrationTests/Backups/WalSqlTests.cs` — новый Fact.

**Выход:** фактический `wal_status='lost'` воспроизводится и зондируется;
`EnsureSlotAliveAsync` лечит lost идемпотентно; «жив — не трогать» подтверждено
неизменностью `restart_lsn`.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test
src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~WalSqlTests"` → PASS
(оба Fact); зачистка серии.

**Связь со spec:** §4 Ф1 (lost-рецепт), критерий 4.

- [ ] **Шаг 1: Добавить Fact (failing — поведение ещё не проверено end-to-end на lost)**

В `WalSqlTests` (тот же файл; образец OwnPostgres — guid-имя, динамический порт,
teardown-ассерт чистоты внутри фикстуры):

```csharp
[Fact]
public async Task Lost_слот_зонд_видит_lost_EnsureSlotAlive_лечит_идемпотентно()
{
    // Arrange — OwnPostgres: физический reserved-слот фикстуры при старте
    DockerTrait.SkipIfUnavailable();
    var ct = TestContext.Current.CancellationToken;
    await using var postgres = await OwnPostgres.StartAsync("wal-sql-lost", ct);
    var sql = new NpgsqlWalSqlExecutor();

    // малый потолок WAL под слотом: срез при превышении + checkpoint
    await using (var conn = new NpgsqlConnection(postgres.AdminDsn))
    {
        await conn.OpenAsync(ct);
        await using var alter = new NpgsqlCommand(
            "ALTER SYSTEM SET max_slot_wal_keep_size = '16MB'", conn);
        await alter.ExecuteNonQueryAsync(ct);
        await using var reload = new NpgsqlCommand("SELECT pg_reload_conf()", conn);
        await reload.ExecuteNonQueryAsync(ct);
    }

    // Act/Assert 1 — живой слот: EnsureSlotAlive НЕ пересоздаёт (restart_lsn на месте)
    var lsnBefore = await postgres.ReadRestartLsnAsync(ct);
    (await sql.EnsureSlotAliveAsync(postgres.AdminDsn, OwnPostgres.Slot, ct))
        .IsSuccess.Should().BeTrue();
    (await postgres.ReadRestartLsnAsync(ct)).Should().Be(lsnBefore,
        "живой слот ensure-alive не трогает (spec §3.1: жив — не трогать)");

    // Act 2 — доводим слот до lost: циклы 64 MiB INSERT + pg_switch_wal + CHECKPOINT
    var lost = false;
    var budget = DateTimeOffset.UtcNow.AddSeconds(60);
    while (!lost && DateTimeOffset.UtcNow < budget)
    {
        await using var conn = new NpgsqlConnection(postgres.AdminDsn);
        await conn.OpenAsync(ct);
        await using (var create = new NpgsqlCommand(
            "CREATE TABLE IF NOT EXISTS wal_load(id bigserial, payload text)", conn))
            await create.ExecuteNonQueryAsync(ct);
        for (var i = 0; i < 4; i++)
        {
            await using var insert = new NpgsqlCommand(
                "INSERT INTO wal_load(payload) SELECT repeat('x', 1048576) FROM generate_series(1, 16)", conn);
            await insert.ExecuteNonQueryAsync(ct);
            await using var switchWal = new NpgsqlCommand("SELECT pg_switch_wal()", conn);
            await switchWal.ExecuteScalarAsync(ct);
        }

        await using var checkpoint = new NpgsqlCommand("CHECKPOINT", conn);
        await checkpoint.ExecuteNonQueryAsync(ct);
        var probe = await sql.SlotProbeAsync(postgres.AdminDsn, OwnPostgres.Slot, ct);
        probe.IsSuccess.Should().BeTrue();
        lost = probe.Value is { Exists: true, WalStatus: "lost" };
    }

    // Assert 2 — зонд видит lost (критерий 4)
    lost.Should().BeTrue("64 MiB WAL при потолке 16 MiB + checkpoint обязаны срезать слот");

    // Act 3 — лечение: EnsureSlotAlive → recreate; повтор — идемпотентность
    (await sql.EnsureSlotAliveAsync(postgres.AdminDsn, OwnPostgres.Slot, ct))
        .IsSuccess.Should().BeTrue();
    (await sql.EnsureSlotAliveAsync(postgres.AdminDsn, OwnPostgres.Slot, ct))
        .IsSuccess.Should().BeTrue("повтор на свежем слоте — идемпотентность");

    // Assert 3 — слот жив (не lost), существует
    var healed = await sql.SlotProbeAsync(postgres.AdminDsn, OwnPostgres.Slot, ct);
    healed.Value.Exists.Should().BeTrue();
    healed.Value.WalStatus.Should().NotBe("lost", "recreate возвращает живой reserved-слот");
}
```

Примечание: `WalSqlTests` уже в namespace `PgWorker.IntegrationTests.Backups`
(OwnPostgres доступен); в usings файла НЕТ `using Npgsql;` — новый Fact использует
`NpgsqlConnection` напрямую, добавить директиву `using Npgsql;` к usings файла
(после `using FluentAssertions;`, в алфавитном порядке блока usings).

- [ ] **Шаг 2: Прогон → зелёный; при «не дожидается lost» — анализ без перезапуска вслепую**

Run: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~WalSqlTests"`.
Если lost не достигнут за 60 c — снять docker-логи контейнера
(`docker logs pgw-pg-<guid из имени>`) и проверить
`SELECT settings FROM pg_settings WHERE name='max_slot_wal_keep_size'` —
устранить причину, затем повторный прогон (это отладка нового теста, не
«перезапуск упавшего E2E»).

- [ ] **Шаг 3: Commit**

```bash
git add -A
git commit -m "test(t19): интеграция lost-рецепта на реальном PG — max_slot_wal_keep_size=16MB + генерация WAL/checkpoint до wal_status=lost, EnsureSlotAlive лечит идемпотентно, живой слот не трогается (restart_lsn) (spec §4 Ф1, критерий 4)"
```

---

## Задача 3 — Правило `WalStreamProcess` шаг (3): alive, BROKEN-дискриминация, авто-recreate при живом втором источнике

**Вход:** Задачи 1–2 слиты.

**Действие (файлы):**
- Modify: `src/PgWorker.Backups/WalStreamProcess.cs` — шаг (3).
- Modify: `src/tests/PgWorker.IntegrationTests/Backups/WalStreamProcessTests.cs` —
  4 новых Fact + доводка существующих при необходимости.

**Выход:** правило «слот исчез» обобщено до «слот не-alive» (исчез ИЛИ lost);
lost одного источника при живом втором — recreate + журнальная фаза `slot-recreate/<X>`
без BROKEN; lost всех при живой цепочке — BROKEN с error-дискриминацией lost;
ensure-ветка лечит lost; здоровый слот — нулевые мутации.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test
src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~WalStreamProcessTests"` →
все PASS (новые + существующие).

**Связь со spec:** §3.2, критерии 1, 2, 3, 7; Ф4-Ф2.

- [ ] **Шаг 1: Новые failing-тесты (4 Fact) в `WalStreamProcessTests`**

Кейс A — критерий 1 (lost одного при живом втором → НЕ BROKEN + recreate + журнал).
Вставить после `Слот_исчез_правило_всех_нод`:

```csharp
// AAA (t19 AC1): lost-слот мастера при живом sync — тик НЕ пишет BROKEN,
// слот мастера пересоздан (drop+create), журнальная фаза slot-recreate/<X>.
[Fact]
public async Task Lost_слот_мастера_при_живом_sync_не_BROKEN_recreate_журнал()
{
    // Arrange — два источника (мастер shard1a + sync shard1b); слот мастера
    // существует, но lost; слот sync жив; ключ wal ACTIVE (цепочка жива)
    var ct = TestContext.Current.CancellationToken;
    var patroni = await FakePatroni.StartAsync(SyncClusterJson, ct);
    await using var patroniOwner = patroni;
    var cluster = $"sl1{Guid.NewGuid().ToString("N")[..6]}";
    await SeedTwoNodeAsync(new FakeWalSqlExecutor(), patroni, cluster);
    (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
    var sql = new FakeWalSqlExecutor();
    var masterDsn = SourceDsn(16001, cluster);
    sql.SlotsByDsn.GetOrAdd(masterDsn, _ => []).Add($"pgw_bkp_{cluster}_shard1");
    sql.LostByDsn.GetOrAdd(masterDsn, _ => []).Add($"pgw_bkp_{cluster}_shard1");
    sql.SlotsByDsn.GetOrAdd(SourceDsn(16002, cluster), _ => []).Add($"pgw_bkp_{cluster}_shard1");
    var s3 = new FakeBackupS3();
    SeedSegments(s3, cluster, 1, 2);
    var writer = new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]);
    var liveWal = new WalStreamState(
        WalStreamStatus.Active, $"pgw_bkp_{cluster}_shard1", "shard1a",
        "000000010000000000000001", "000000010000000000000002",
        "000000010000000000000002", 1757500000, 0, null);
    await writer.WriteIfChangedAsync(cluster, "shard1", liveWal, ct);
    var driver = new StubScaleDriver();
    var process = BuildProcess(Options(verify: 3600), sql, s3, driver);
    var backups = new ClusterBackups(cluster, null,
        new Dictionary<string, ShardBackups>
        {
            ["shard1"] = new(FullShard("000000010000000000000001").Full, liveWal),
        });

    // Act
    (await process.TickAsync(BuildTwoNodeSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();

    // Assert — не BROKEN; слот мастера пересоздан (drop+create); журнал пишет
    // фазу ДО мутации; агент мастера не снимался
    var wal = await ReadWal(cluster);
    wal!.State.Should().NotBe(WalStreamStatus.Broken,
        "lost одного источника при живом втором — не BROKEN (spec §3.2)");
    sql.Calls.Should().Contain(c => c.Op == "drop" && c.Dsn == masterDsn,
        "FakeSql фиксирует drop потерянного слота");
    sql.Calls.Should().Contain(c => c.Op == "create" && c.Dsn == masterDsn,
        "FakeSql фиксирует create пересозданного слота");
    sql.LostByDsn[masterDsn].Should().NotContain($"pgw_bkp_{cluster}_shard1",
        "после recreate слот жив");
    var journal = await fixture.Gateway.GetAsync(
        fixture.Endpoint, $"/pgworker/work/{cluster}", ct);
    journal.Value!.Value.Should().Contain("slot-recreate/shard1");
    journal.Value.Value.Should().Contain("wal_status=lost");
    driver.RemovedBackupAgents.Should().NotContain(
        n => n.Contains("shard1", StringComparison.Ordinal), "агенты не трогаются");
}

// AAA (t19 AC7): тик на здоровых (не lost) слотах — нулевые мутации слотов
// и никаких журнальных фаз slot-recreate.
[Fact]
public async Task Тик_на_здоровых_слотах_нулевые_мутации()
{
    // Arrange — два источника, оба слота живы; ключ ACTIVE; цепочка сплошная
    var ct = TestContext.Current.CancellationToken;
    var patroni = await FakePatroni.StartAsync(SyncClusterJson, ct);
    await using var patroniOwner = patroni;
    var cluster = $"sl2{Guid.NewGuid().ToString("N")[..6]}";
    await SeedTwoNodeAsync(new FakeWalSqlExecutor(), patroni, cluster);
    (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
    var sql = new FakeWalSqlExecutor();
    sql.SlotsByDsn.GetOrAdd(SourceDsn(16001, cluster), _ => []).Add($"pgw_bkp_{cluster}_shard1");
    sql.SlotsByDsn.GetOrAdd(SourceDsn(16002, cluster), _ => []).Add($"pgw_bkp_{cluster}_shard1");
    var s3 = new FakeBackupS3();
    SeedSegments(s3, cluster, 1, 2);
    var driver = new StubScaleDriver();
    var process = BuildProcess(Options(), sql, s3, driver);
    var backups = new ClusterBackups(cluster, null,
        new Dictionary<string, ShardBackups>
        {
            ["shard1"] = FullShard("000000010000000000000001"),
        });

    // Act — два тика (контроль + супервиз)
    (await process.TickAsync(BuildTwoNodeSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();
    (await process.TickAsync(BuildTwoNodeSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();

    // Assert — ни одного вызова мутации слота; журнал без slot-recreate
    sql.Calls.Should().BeEmpty("здоровый слот не трогается (критерий 7)");
    var journal = await fixture.Gateway.GetAsync(
        fixture.Endpoint, $"/pgworker/work/{cluster}", ct);
    (journal.Value?.Value ?? "").Should().NotContain("slot-recreate");
}
```

Кейс B — критерий 2 (lost ВСЕХ при живой цепочке → BROKEN с «lost» в error) —
добавить после кейса A:

```csharp
// AAA (t19 AC2): lost на ВСЕХ источниках при живом (ACTIVE) ключе — BROKEN,
// recreateSlot на мастере, error различает lost от «исчез».
[Fact]
public async Task Lost_слотов_всех_источников_при_живой_цепочке_BROKEN()
{
    // Arrange — оба слота существуют и lost; ключ ACTIVE; агент жив
    var ct = TestContext.Current.CancellationToken;
    var patroni = await FakePatroni.StartAsync(SyncClusterJson, ct);
    await using var patroniOwner = patroni;
    var cluster = $"sl3{Guid.NewGuid().ToString("N")[..6]}";
    await SeedTwoNodeAsync(new FakeWalSqlExecutor(), patroni, cluster);
    (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
    var sql = new FakeWalSqlExecutor();
    foreach (var port in new[] { 16001, 16002 })
    {
        var dsn = SourceDsn(port, cluster);
        sql.SlotsByDsn.GetOrAdd(dsn, _ => []).Add($"pgw_bkp_{cluster}_shard1");
        sql.LostByDsn.GetOrAdd(dsn, _ => []).Add($"pgw_bkp_{cluster}_shard1");
    }
    var s3 = new FakeBackupS3();
    SeedSegments(s3, cluster, 1, 2);
    var driver = new StubScaleDriver();
    var writer = new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]);
    var liveWal = new WalStreamState(
        WalStreamStatus.Active, $"pgw_bkp_{cluster}_shard1", "shard1a",
        "000000010000000000000001", "000000010000000000000002",
        "000000010000000000000002", 1757500000, 0, null);
    await writer.WriteIfChangedAsync(cluster, "shard1", liveWal, ct);
    var process = BuildProcess(Options(), sql, s3, driver);
    var backups = new ClusterBackups(cluster, null,
        new Dictionary<string, ShardBackups>
        {
            ["shard1"] = new(FullShard("000000010000000000000001").Full, liveWal),
        });

    // Act
    (await process.TickAsync(BuildTwoNodeSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();

    // Assert — BROKEN, error содержит lost; слот мастера пересоздан (recreateSlot)
    var wal = await ReadWal(cluster);
    wal!.State.Should().Be(WalStreamStatus.Broken);
    wal.Error.Should().Contain("lost", "error различает потерю и исчезновение (spec §3.2)");
    sql.Calls.Should().Contain(c => c.Op == "drop" && c.Dsn == SourceDsn(16001, cluster),
        "BreakAsync recreateSlot: lost-слот мастера пересоздаётся immediate+reserved");
    driver.RemovedBackupAgents.Should().NotBeEmpty("BROKEN останавливает агентов");
}
```

Кейс C — критерий 3 (ensure-ветка: BROKEN-ключ + lost-слот → recreate, не
duplicate-пропуск) — однонодовая конфигурация:

```csharp
// AAA (t19 AC3): ensure-ветка (ключ wal = BROKEN) + существующий lost-слот —
// EnsureSlotAlive recreates (не пропускает по идемпотентности), новый BROKEN не пишется.
[Fact]
public async Task Ensure_ветка_BROKEN_ключ_lost_слот_recreate()
{
    // Arrange — однонодовый кластер; wal-состояние BROKEN передано тиком (как
    // в cb-кейсах: и ключ в etcd, и backups-аргумент); слот есть, но lost
    var ct = TestContext.Current.CancellationToken;
    await SeedAsync("sl4");
    (await _claims.TryClaimClusterAsync("sl4", ct)).Value.Should().BeTrue();
    var sql = new FakeWalSqlExecutor();
    var masterDsn = SourceDsn(16001, "sl4");
    sql.SlotsByDsn.GetOrAdd(masterDsn, _ => []).Add("pgw_bkp_sl4_shard1");
    sql.LostByDsn.GetOrAdd(masterDsn, _ => []).Add("pgw_bkp_sl4_shard1");
    var s3 = new FakeBackupS3(); // пустой префикс wal/
    var driver = new StubScaleDriver();
    var brokenWal = new WalStreamState(
        WalStreamStatus.Broken, "pgw_bkp_sl4_shard1", "shard1a",
        "000000010000000000000001", "000000010000000000000001",
        "000000010000000000000001", 1757500000, null, "дыра WAL-цепочки");
    var writer = new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]);
    await writer.WriteIfChangedAsync("sl4", "shard1", brokenWal, ct);
    var process = BuildProcess(Options(), sql, s3, driver);
    var backups = new ClusterBackups("sl4", null,
        new Dictionary<string, ShardBackups> { ["shard1"] = new([], brokenWal) });

    // Act
    var result = await process.TickAsync(BuildSnap("sl4"), backups, ct);

    // Assert — ensure-ветка полечила lost (drop+create), тик успешен
    result.IsSuccess.Should().BeTrue();
    sql.Calls.Should().Contain(c => c.Op == "drop" && c.Dsn == masterDsn,
        "lost в ensure-ветке лечится recreate, а не пропуском (критерий 3)");
    sql.Calls.Should().Contain(c => c.Op == "create" && c.Dsn == masterDsn);
    sql.LostByDsn[masterDsn].Should().NotContain("pgw_bkp_sl4_shard1");

    // Новый BROKEN не пишется: слот лечится тихо, ключ wal — прежний BROKEN
    // (контроль при пустом S3 не имеет наблюдения и ключ не перезаписывает)
    var walAfter = await ReadWal("sl4");
    walAfter!.State.Should().Be(WalStreamStatus.Broken,
        "ensure-ветка не меняет статус ключа — лечение слота без BROKEN-записи");
    walAfter.Error.Should().Be("дыра WAL-цепочки",
        "исходная BROKEN-запись нетронута — новой записи не создавалось");
}
```

- [ ] **Шаг 2: Прогон новых тестов → FAIL (правило ещё не различает lost)**

Run: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~WalStreamProcessTests"`
Ожидание: серия FAIL — кейсы A/B/C падают (BROKEN пишется при lost одного;
error без «lost»; ensure-ветка пропускает lost), кейс D может уже PASS;
существующие — PASS.

- [ ] **Шаг 3: Реализация правила в `WalStreamProcess` шаге (3)**

Заменить блок шага (3) (от `var slotProbes = new List<(WalSource Src, string Dsn, bool Exists)>();`
до конца обеих ensure-веток включительно) на:

```csharp
// (3) Слот per-instance (t27 §3.3 п.3; t19 arch/19 §3): имя слота одно и то же
//     на КАЖДОЙ ноде-источнике (слоты разных инстансов независимы); зонд —
//     существование И wal_status: alive = существует и НЕ lost. BROKEN —
//     слоты ВСЕХ источников исчезли ИЛИ потеряны при живой цепочке (живой слот
//     на любой ноде держит WAL); lost одного при живом втором — авто-recreate
//     без BROKEN (потерянный агент вернётся от хвоста S3 ретраем приёмника).
var slotProbes = new List<(WalSource Src, string Dsn, bool Exists, string? WalStatus)>();
foreach (var src in sources)
{
    var dsn = ShardEndpoints.AdminDsn(src.Addr, snap.Config.DbName, secrets);
    var probe = await sql.SlotProbeAsync(dsn, slot, ct);
    if (!probe.IsSuccess)
        throw new ApplicationException($"слот-зонд {slot}@{src.Node}: {probe.Error!.Message}");
    slotProbes.Add((src, dsn, probe.Value.Exists, probe.Value.WalStatus));
}
// null-статус существующего слота (старые PG/edge) — живой: лечим только явный lost
static bool Alive((WalSource Src, string Dsn, bool Exists, string? WalStatus) p)
    => p.Exists && p.WalStatus != "lost";

if (slotProbes.All(p => !Alive(p)))
{
    if (chainKnown && wal!.State is WalStreamStatus.Active or WalStreamStatus.Degraded)
    {
        // (гвард битого last_uploaded_unix — блок оставить без изменений, как сейчас)

        // BROKEN + слот пересоздаётся immediate+reserved СРАЗУ (t07); error
        // различает «исчез» / «потерян (lost)» по факту зонда (t19)
        var anyExists = slotProbes.Any(p => p.Exists);
        await BreakAsync(cluster, shard.Name, wal, slot, masterRef, adminDsn, ct,
            baseStart: wal.LastUploadedSegment is { Length: > 0 }
                ? wal.LastUploadedSegment : wal.ChainStartSegment,
            baseLast: wal.LastUploadedSegment,
            baseUnix: wal.LastUploadedUnix ?? clock.GetUtcNow().ToUnixTimeSeconds(),
            error: anyExists
                ? $"слот {slot} потерян (wal_status=lost) на всех источниках при живой цепочке — пересними полный бэкап"
                : $"слот {slot} исчез при живой цепочке (инвалидация max_slot_wal_keep_size?) — пересними полный бэкап",
            recreateSlot: true);
        return;
    }

    // Первый старт ИЛИ BROKEN-ключ: ensure-alive на КАЖДОМ источнике
    // (отсутствует → create; lost → recreate — пустой префикс wal/ + lost лечится
    // recreate без BROKEN, свежий слот держит текущую позицию).
    foreach (var dead in slotProbes)
    {
        var ensured = await sql.EnsureSlotAliveAsync(dead.Dsn, slot, ct);
        if (!ensured.IsSuccess)
            throw new ApplicationException($"ensure слота {slot}@{dead.Src.Node}: {ensured.Error!.Message}");
    }
}
else
{
    // lost при живом втором источнике — НЕ BROKEN: recreate потерянного
    // (journal-before-manipulations, arch/17: фаза ДО мутации — видимость
    // действия при падении между drop и create; тик повторит).
    foreach (var lost in slotProbes.Where(p => p.Exists && p.WalStatus == "lost"))
    {
        await journal.WritePhaseAsync(cluster, Op, $"slot-recreate/{shard.Name}", claims.InstanceId,
            $"{lost.Src.Node}: слот {slot} wal_status=lost — слот пересоздан, агент вернётся от хвоста S3", ct);
        var recreated = await sql.RecreateSlotAsync(lost.Dsn, slot, ct);
        if (!recreated.IsSuccess)
            throw new ApplicationException($"recreate слота {slot}@{lost.Src.Node}: {recreated.Error!.Message}");
    }

    // отсутствующие на живых источниках → create (ensure-идемпотентность, t27)
    foreach (var missing in slotProbes.Where(p => !p.Exists))
    {
        var created = await sql.EnsureSlotAliveAsync(missing.Dsn, slot, ct);
        if (!created.IsSuccess)
            throw new ApplicationException($"ensure слота {slot}@{missing.Src.Node}: {created.Error!.Message}");
    }
}
```

Гвард битого `last_uploaded_unix` (блок `if (wal.LastUploadedUnix is null) {...}`) —
перенести внутрь BROKEN-ветки без изменений (он относится к BreakAsync-вызову).
`BreakAsync` уже мигрировал на `EnsureSlotAliveAsync` в Задаче 1 — не трогать.

- [ ] **Шаг 4: Прогон всего класса → PASS (новые + регресс)**

Run: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~WalStreamProcessTests"`
Ожидание: все PASS; в частности регресс-семейство «слот исчез»
(`Слот_исчез_пишет_BROKEN_и_пересоздает_слот`, `Контроль_инвалидация_слота_пишет_BROKEN_стоп_агента_AC4b`,
`Слот_исчез_при_битом_last_uploaded_unix_не_роняет_тик`, `Слот_исчез_правило_всех_нод`)
— зелёное (критерий 2, «регресса нет»). Зачистка серии.

- [ ] **Шаг 5: Commit**

```bash
git add -A
git commit -m "feat(t19): правило WalStreamProcess — зонд alive (существование+wal_status): lost одного источника при живом втором — авто-recreate + журнальная фаза slot-recreate/<X> без BROKEN; lost всех при живой цепочке — BROKEN с error-дискриминацией lost/исчез; ensure-ветка лечит lost (spec §3.2)"
```

---

## Задача 4 — Панель: remedy `slot-wal-lost` → WorkerAuto + тесты

**Вход:** Задачи 1–3 слиты (воркер лечит — тексты ремеди становятся правдой).

**Действие (файлы):**
- Modify: `src/AdminPanel.Core/Alerting/Rules/SlotWalLostRule.cs`.
- Modify: `src/tests/AdminPanel.UnitTests/HaAlertRulesTests.cs` — ассерты remedy.

**Выход:** алерт `slot-wal-lost` несёт `AlertRemedy.WorkerAuto` и текст без ручного
разбора; алерт остаётся страховкой (гаснет после лечения).

**Проверка:** `dotnet test src/PgWorker.slnx -c Debug --filter
"FullyQualifiedName~AlertHintRemedyTests|FullyQualifiedName~HaAlertRulesTests"` →
все PASS.

**Связь со spec:** §3.3 (панель), критерий 6, Ф4-Ф3.

- [ ] **Шаг 1: Failing-ассерты в `HaAlertRulesTests.SlotWalLost_LostSlot_Critical`**

В существующий Fact после `alert.Id.Should()...` добавить:

```csharp
alert.Remedy.Should().Be(AlertRemedy.WorkerAuto,
    "lost-слот лечит воркер автоматически — ручной разбор не нужен");
alert.RemedyText.Should().Contain("воркер");
alert.Hint.Should().NotContain("runbook", "ручной разбор не предлагается");
```

Run: `dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~SlotWalLost_LostSlot"`
→ FAIL (Remedy ещё OperatorRunbook).

- [ ] **Шаг 2: Правило `SlotWalLostRule` — заменить конструктор алерта**

```csharp
yield return new Alert(
    $"{KindName}:{cluster.Name}/{shard.Name}/{slot.SlotName}",
    AlertSeverity.Critical,
    KindName,
    $"{cluster.Name}/{shard.Name}/{slot.SlotName}",
    $"слот {slot.SlotName} шарда {cluster.Name}/{shard.Name}: wal_status=lost — WAL срезан, источник догонит только пересозданием (P4)",
    new Dictionary<string, string> { ["walStatus"] = "lost" },
    null,
    "wal_status=lost: WAL срезан — воркер пересоздаёт слот сам (recreate + возврат агента от хвоста S3); алерт гаснет после лечения",
    AlertRemedy.WorkerAuto,
    "воркер пересоздаёт lost-слот автоматически; алерт висит — воркер не лечит: журнал backup-wal / healthz");
```

- [ ] **Шаг 3: Прогон панели → PASS**

Run: `dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~AlertHintRemedyTests|FullyQualifiedName~HaAlertRulesTests"`
→ все PASS (инвариант «Hint/RemedyText непустые» — по-прежнему).

- [ ] **Шаг 4: Commit**

```bash
git add -A
git commit -m "feat(t19): панель slot-wal-lost — ремеди WorkerAuto (воркер пересоздаёт слот сам), hint/remedy-text без ручного разбора; алерт — страховка, гаснет после лечения (spec §3.3)"
```

---

## Задача 5 — Документация: runbook, docs/adminpanel, сверка arch/19

**Вход:** Задачи 1–4 слиты (описываем фактическое поведение).

**Действие (файлы):**
- Modify: `docs/runbook.md` — новый раздел.
- Modify: `docs/adminpanel/03-probes-alerts.md` — примечание к `slot-wal-lost`.
- Verify: `arch/19-backups.md` §3/§10 (обновлён arch-first в фазе spec) — сверить
  формулировки с реализацией; при расхождении — правка канона в этом же шаге.

**Выход:** runbook описывает автоматику и границы RPO; docs панели синхронны с
remedy; канон соответствует коду.

**Проверка:** тексты на месте; в них нет атрибуций «(tNN)» и исторических
пассажей; `git diff --stat` — только три файла docs/arch.

**Связь со spec:** §3.3 (runbook/docs), критерий 6/8.

- [ ] **Шаг 1: Раздел runbook**

В `docs/runbook.md` после раздела «Вторые инстансы воркеров» (рядом с
эксплуатационными темами; не в разделах про registry) добавить:

```markdown
## Потеря wal-слота (`wal_status='lost'`)

Что происходит: при длительном неподтверждении приёма (S3 недоступен — приёмник
в backpressure) WAL под слотом `pgw_bkp_<C>_<X>` копится на источнике до потолка
`max_slot_wal_keep_size`; затем PG срезает WAL и слот переходит в
`wal_status='lost'` — стрим через него невозможен (нужные сегменты удалены).

Что делает воркер (тик контроля WAL, зонд на КАЖДОМ источнике — мастер и
sync-standby):

- lost ОДНОГО источника при живом втором — слот потерянного пересоздаётся
  (drop + create immediate+reserved) автоматически, журнальная фаза
  `slot-recreate/<X>` (op `backup-wal` в `/pgworker/work/<C>`); потерянный агент
  возвращается сам от хвоста S3 (вечный ретрай приёмника), живой продолжает
  доставку — BROKEN не ставится, цепочка не рвётся;
- lost/исчезновение слотов ВСЕХ источников при живой цепочке — wal-ключ BROKEN,
  агенты останавливаются, воркер переснимает полный бэкап; архивация
  восстанавливается от `wal_start` нового полного.

Границы (RPO): неподтверждённый хвост WAL теряется в момент среза PG — свойство
аварии до любой автоматики; автоматика ускоряет RTO восстановления архивации.

Ручная чистка слота не требуется. Если алерт `slot-wal-lost` висит после
автолечения (слот не lost, но алерт жив) — воркер не лечит: смотрите журнал
`backup-wal` (`/pgworker/work/<C>`) и healthz воркера.
```

- [ ] **Шаг 2: Примечание в `docs/adminpanel/03-probes-alerts.md`**

После строки-примечания `(`inventory-mismatch` ...)` добавить:

```markdown
- `slot-wal-lost` — страховка: lost-слот пересоздаёт воркер автоматически
  (recreate + возврат агента от хвоста S3); ремеди `WorkerAuto`, алерт гаснет
  после лечения; висящий алерт = воркер не лечит (журнал backup-wal / healthz).
```

- [ ] **Шаг 3: Сверка `arch/19-backups.md`**

Прочитать §3 («Слот», «Правила непрерывности») и §10 (строка «Слот при смерти
агентов копит WAL»): формулировки обязаны совпадать с реализацией Задачи 3
(alive-зонд; lost одного ≠ BROKEN; lost/исчезновение всех → BROKEN; пустой
префикс + lost → recreate). Расхождения — исправить в этом же шаге (минимальной
правкой, без исторических атрибуций).

- [ ] **Шаг 4: Commit**

```bash
git add -A
git commit -m "docs(t19): runbook — раздел «Потеря wal-слота (автоматика)»: механика среза, лечение воркером, границы RPO; docs/adminpanel — slot-wal-lost ремеди WorkerAuto; arch/19 сверен с реализацией (spec §3.3)"
```

---

## Задача 6 — E2E: сценарий автолечения lost-слота (docker, двойная архивация)

**Вход:** Задачи 1–5 слиты; docker-демон доступен; образы E2E собираются
фикстурой (Release-сборка на хосте — инкрементальная).

**Действие (файлы):**
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eWalStreamScenarios.cs` —
  новый Fact + приватный хелпер чтения `wal_status`.

**Выход:** E2E-сценарий доказывает критерий 5: lost-слот мастера при живой
sync-архивации лечится воркером без BROKEN, без дыры цепочки, с журнальной фазой
`slot-recreate` и возвратом доставки.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test
src/PgWorker.slnx -c Release --filter "FullyQualifiedName~WalStream_SlotLostAutoRecreate"`
→ PASS; `[PHASE]`-строки в выводе; teardown чист (guid-контур окружения).

**Связь со spec:** §4 Ф4, критерий 5; мерж-гейт AGENTS (E2E на свежем Release).

Механика сценария (детерминизация): снос агента мастера воркер компенсирует
супервизом за один тик (ScanIntervalSec=1 в E2E) — слот не успеет потеряться.
Поэтому между сносом агента и возвратом воркера воспроизводим прод-условие
потери: воркер останавливается (`HostInstance.Kill()` — отработанный паттерн
«смерть контроллера», E2eScenarios), живой sync-агент продолжает доставку,
WAL генерится до `wal_status='lost'`, затем воркер возвращается и лечит своим
тиком. Это ровно механика spec §1 («слот потерян при неподтверждении; живой
агент продолжает доставку»), ничего в системе не мокается.

- [ ] **Шаг 1: Хелпер чтения wal_status (в конец класса, рядом с `MasterPgAsync`)**

```csharp
// wal_status слота бэкапа на указанной ноде (portalloc pg-порт): null = слота нет.
private async Task<string?> SlotWalStatusAsync(string cluster, string shard, string node, CancellationToken ct)
{
    var kv = await G.GetAsync(Endpoint, $"/pgworker/portalloc/{cluster}", ct);
    kv.Value.Should().NotBeNull("portalloc пишется при provisioning");
    var entries = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(kv.Value!.Value)!;
    var dsn = DatabaseProvisioner.BuildAdminDsn("localhost",
        entries[$"{shard}/{node}"].GetProperty("pg").GetInt32(), cluster,
        new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
    await using var conn = new NpgsqlConnection(dsn);
    await conn.OpenAsync(ct);
    await using var cmd = new NpgsqlCommand(
        "SELECT wal_status::text FROM pg_replication_slots WHERE slot_name = @slot", conn)
    {
        Parameters = { new() { ParameterName = "slot", Value = $"pgw_bkp_{cluster}_{shard}" } },
    };
    return (string?)await cmd.ExecuteScalarAsync(ct);
}
```

- [ ] **Шаг 2: Новый Fact (полный код)**

Вставить после `WalStream_DualArchiving_TwoAgents_OnePrefix`:

```csharp
// AAA (критерий 5): lost-слот мастера при живой sync-архивации — воркер лечит
// сам (recreate + журнальная фаза slot-recreate), ключ НЕ BROKEN, агент
// возвращается, цепочка WalChain непрерывна, last_uploaded растёт без дыры.
// Детерминизация: между сносом агента мастера и возвратом воркера (Kill/старт —
// паттерн «смерть контроллера») живой sync-агент продолжает доставку, WAL
// генерится до фактического wal_status='lost'.
[Fact]
public async Task WalStream_SlotLostAutoRecreate_NoBroken()
{
    // Arrange — двухнодовый шард (двойная архивация), оба агента живы, доставка идёт
    DockerTrait.SkipIfUnavailable();
    var ct = TestContext.Current.CancellationToken;
    var (fx, app, cluster, adminDsn, masterNode) = await StartWalScenarioAsync("wal-t19", "shopt19", ct);
    await using var envOwner = fx;
    await using var appOwner = app; // kill воркера ниже — dispose идемпотентен
    var slot = $"pgw_bkp_{cluster}_shard1";
    var agentName = $"pgw-backup-wal-{cluster}-shard1-{masterNode}";
    var writer = new PgWorker.Backups.WalStatusWriter(G, [Endpoint]);
    var hostEndpoint = Fx.S3Endpoint.Replace(
        "host.docker.internal:", "localhost:", StringComparison.Ordinal);
    await using var backupS3 = new PgWorker.Backups.BackupS3(
        new PgWorker.Backups.BackupsRuntimeOptions
        {
            Enabled = true,
            S3Endpoint = hostEndpoint,
            S3Bucket = Bucket,
            S3AccessKey = "minioadmin",
            S3SecretKey = "minioadmin",
            S3PathStyle = true,
        });

    try
    {
        // Нагрузка ПЕРВАЯ — Patroni выбирает sync при живом потоке репликации (AC3)
        await GenerateWalAsync(adminDsn, ct);
        var bothAgents = await E2eFixture.WaitForAsync(async () =>
        {
            var ps = await Fx.RunDockerAsync(
            [
                "ps", "--filter", $"name=pgw-backup-wal-{cluster}-shard1-",
                "--format", "{{.Names}} {{.State}}",
            ], ct);
            return ps.Split('\n', StringSplitOptions.RemoveEmptyEntries)
                .Count(l => l.EndsWith("running")) >= 2;
        }, TimeSpan.FromSeconds(300), ct);
        bothAgents.Should().BeTrue("двойная архивация — оба агента обязаны подняться");

        // Малый потолок WAL под слотом на мастере (динамика reload — без рестарта)
        await using (var conn = new NpgsqlConnection(adminDsn))
        {
            await conn.OpenAsync(ct);
            await using var alter = new NpgsqlCommand(
                "ALTER SYSTEM SET max_slot_wal_keep_size = '16MB'", conn);
            await alter.ExecuteNonQueryAsync(ct);
            await using var reload = new NpgsqlCommand("SELECT pg_reload_conf()", conn);
            await reload.ExecuteNonQueryAsync(ct);
        }

        // Фиксация «до»: ключ ACTIVE и его last_uploaded (согласованный срез)
        var before = await E2eFixture.WaitForAsync(async () =>
        {
            var read = await writer.ReadAsync(cluster, "shard1", ct);
            return read.IsSuccess
                   && read.Value is { State: PgWorker.Etcd.Parsing.WalStreamStatus.Active };
        }, TimeSpan.FromSeconds(120), ct);
        before.Should().BeTrue("до потери — ключ ACTIVE");
        var beforeWal = (await writer.ReadAsync(cluster, "shard1", ct)).Value!;

        // Act 1 — снос агента мастера + стоп воркера (супервиз не вернёт агента
        // до воспроизведения lost; sync-агент жив — доставка продолжается)
        Console.WriteLine($"[PHASE] wal-t19: docker rm -f {agentName} + kill воркера");
        await Fx.RunDockerAsync(["rm", "-f", agentName], ct);
        app.Kill();

        // Act 2 — генерация WAL до фактического lost (поллинг зонда; INSERT-нагрузка
        // вместо pgbench — pgbench недоступен в образе ноды, эффект идентичен)
        var lost = await E2eFixture.WaitForAsync(async () =>
        {
            for (var i = 0; i < 2; i++)
            {
                await using var conn = new NpgsqlConnection(adminDsn);
                await conn.OpenAsync(ct);
                await using var insert = new NpgsqlCommand(
                    "INSERT INTO wal_load(payload) SELECT repeat('x', 1048576) FROM generate_series(1, 32)", conn);
                await insert.ExecuteNonQueryAsync(ct);
                await using var switchWal = new NpgsqlCommand("SELECT pg_switch_wal()", conn);
                await switchWal.ExecuteScalarAsync(ct);
            }

            await using (var conn = new NpgsqlConnection(adminDsn))
            {
                await conn.OpenAsync(ct);
                await using var checkpoint = new NpgsqlCommand("CHECKPOINT", conn);
                await checkpoint.ExecuteNonQueryAsync(ct);
            }
            return await SlotWalStatusAsync(cluster, "shard1", masterNode, ct) == "lost";
        }, TimeSpan.FromSeconds(180), ct);
        Console.WriteLine($"[PHASE] wal-t19: слот мастера lost={lost} (бюджет 180 c)");
        lost.Should().BeTrue("64 MiB WAL при потолке 16 MiB + checkpoint обязаны срезать слот мастера");

        // Act 3 — возврат воркера: тик лечит (recreate слота + журнал slot-recreate,
        // супервиз поднимает агента мастера; клэйм убитого истекает ≤ 15-20 c)
        Console.WriteLine("[PHASE] wal-t19: старт воркера — автолечение");
        await using var app2Owner = await StartWalHostAsync("wal-t19b", ct);

        // Assert 1 — журнальная фаза slot-recreate
        var journaled = await E2eFixture.WaitForAsync(async () =>
        {
            var workKv = await GetOrNullAsync($"/pgworker/work/{cluster}");
            return workKv?.Value?.Contains("slot-recreate/shard1") == true;
        }, TimeSpan.FromSeconds(120), ct);
        journaled.Should().BeTrue("воркер обязан записать фазу slot-recreate (spec §3.2)");

        // Assert 2 — слот мастера жив (не lost)
        var healed = await E2eFixture.WaitForAsync(
            async () => await SlotWalStatusAsync(cluster, "shard1", masterNode, ct) is not ("lost" or null),
            TimeSpan.FromSeconds(120), ct);
        healed.Should().BeTrue("recreate возвращает живой слот");

        // Assert 3 — агент мастера снова running (супервиз)
        var agentBack = await E2eFixture.WaitForAsync(async () =>
        {
            var ps = await Fx.RunDockerAsync(
                ["ps", "--filter", $"name=^{agentName}$", "--format", "{{.State}}"], ct);
            return ps.Trim() == "running";
        }, TimeSpan.FromSeconds(120), ct);
        agentBack.Should().BeTrue("супервиз поднимает агента мастера после recreate");

        // Assert 4 — ключ wal: BROKEN не возникал (поллинг срезом), финал ACTIVE;
        // last_uploaded СТРОГО растёт от «до потери» (sync доставлял + мастер вернулся)
        PgWorker.Etcd.Parsing.WalStreamStatus? sawStatus = null;
        var recovered = await E2eFixture.WaitForAsync(async () =>
        {
            var read = await writer.ReadAsync(cluster, "shard1", ct);
            if (!read.IsSuccess || read.Value is null)
                return false;
            sawStatus = read.Value.State;
            if (sawStatus == PgWorker.Etcd.Parsing.WalStreamStatus.Broken)
                return true; // немедленный фейл ниже
            return sawStatus == PgWorker.Etcd.Parsing.WalStreamStatus.Active;
        }, TimeSpan.FromSeconds(180), ct);
        sawStatus.Should().NotBe(PgWorker.Etcd.Parsing.WalStreamStatus.Broken,
            "lost одного источника при живом втором — BROKEN запрещён (критерий 5)");
        recovered.Should().BeTrue("ключ обязан вернуться в ACTIVE");
        var afterWal = (await writer.ReadAsync(cluster, "shard1", ct)).Value!;
        var beforeSeg = PgWorker.Backups.WalFileName.TryParse(beforeWal.LastUploadedSegment)!.Value;
        var afterSeg = PgWorker.Backups.WalFileName.TryParse(afterWal.LastUploadedSegment)!.Value;
        ((long)afterSeg.Log * 256 + afterSeg.Seg).Should().BeGreaterThan(
            (long)beforeSeg.Log * 256 + beforeSeg.Seg,
            "last_uploaded_segment растёт без дыры (живой sync достраивал хвост)");

        // Assert 5 — цепочка WalChain непрерывна от chain_start
        var list = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
        list.IsSuccess.Should().BeTrue();
        var chain = PgWorker.Backups.WalChain.Check(
            PgWorker.Backups.WalFileName.TryParse(afterWal.ChainStartSegment)!.Value,
            list.Value.Select(o => o.Name));
        chain.IsContinuous.Should().BeTrue(chain.GapError ?? "цепочка непрерывна после автолечения");
    }
    catch (Exception ex)
    {
        Fx.MarkFailed(); // артефакты телеметрии переживают teardown (канон e2e-launch)
        await WalScenarioDiagDumpAsync(cluster, "t19: " + ex.Message);
        throw;
    }
}
```

Замечания исполнителю:
- `StartWalHostAsync` уже даёт `VerifyIntervalSec=2`, `StaleSec=600`,
  `LagMaxSegments=100000` — пороги применимы; секреты/образы — фикстурой.
- Если Assert 4 ловит «BROKEN возникал» — до разбора снять wal-ключ, журнал и
  host.log (это уже делает `WalScenarioDiagDumpAsync`); перезапуск сценария
  для выяснения «что было» запрещён (AGENTS «Телеметрия E2E»).
- Если lost не достигается за 180 c — проверить применение
  `max_slot_wal_keep_size` (SELECT из pg_settings) и журнал sync-агента:
  живой sync не влияет на потерю мастер-слота, но тишина репликации укажет на
  не выбранного sync (тогда bothAgents выше уже упал бы).

- [ ] **Шаг 3: Прогон на Release (E2eFixture соберёт Release сам, инкрементально)**

Run: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~WalStream_SlotLostAutoRecreate"`
Ожидание: PASS; в выводе — `[PHASE] wal-t19: ...` строки; после прогона
окружение снесено (guid-контур), ручная зачистка — шаг Г Задачи 7.

- [ ] **Шаг 4: Commit**

```bash
git add -A
git commit -m "test(t19): E2E автолечения lost-слота — двойная архивация, снос агента мастера + стоп воркера, WAL до фактического wal_status=lost, возврат воркера: журнал slot-recreate, слот жив, агент поднят, ключ без BROKEN, цепочка непрерывна, last_uploaded растёт (spec §4 Ф4, критерий 5)"
```

---

## Задача 7 — Мерж-гейт: полная верификация на свежем Release + зачистка серий

**Вход:** Задачи 1–6 слиты.

**Действие:** последовательные серии с зачисткой ПОСЛЕ КАЖДОЙ (AGENTS:
контейнеры/сети/тома искажают следующую серию; никогда не запускать следующую
серию поверх незачищенной предыдущей).

**Выход:** все серии зелёные на свежем Release; артефактов серий нет.

**Проверка:** команды ниже + пустые счётчики残留-контейнеров/сетей.

**Связь со spec:** критерии 1–7 (сквозная верификация), мерж-гейт AGENTS
(E2E на свежем Release: кейс-маркер + wal-кейс t19).

- [ ] **Шаг А: Сборка Release 0/0**

```bash
dotnet build src/PgWorker.slnx -c Release
```
Ожидание: 0 warning / 0 error (TreatWarningsAsErrors).

- [ ] **Шаг Б: Юниты (все влияемые)**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~AdminPanel.UnitTests"
```
Ожидание: PASS (включая AlertHintRemedyTests/HaAlertRulesTests). Зачистка не
нужна (docker не поднимается) — но убедиться, что фильтр не зацепил docker-тесты.

- [ ] **Шаг В: Не-E2E интеграции (docker)**

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~WalSqlTests|FullyQualifiedName~WalStreamProcessTests"
```
Ожидание: PASS. Дождаться финальной строки → шаг Г.

- [ ] **Шаг Г: Зачистка серии (после КАЖДОЙ docker-серии)**

```bash
docker ps -a --format '{{.Names}}' | grep -c 'pgw-'
docker network ls --format '{{.Name}}' | grep -cE 'pgw-.*-net|kfw-net'
docker network prune -f
```
Ожидание: оба счётчика `0` (осиротевших нет; `pgw-pg-*` контейнеры
testcontainers подбирает ryuk, сети per-cluster — движком, вручную prune).
Повторять `grep -c` до нуля перед следующей серией.

- [ ] **Шаг Д: E2E мерж-гейт на свежем Release**

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~Scale_AddEmptyShard"
# зачистка (шаг Г), затем:
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~WalStream_SlotLostAutoRecreate"
```
Ожидание: оба PASS (по AGENTS: кейс-маркер + wal-кейс задачи; сборка Release
фикстурой инкрементальна). Между прогонами — шаг Г. Упавший сценарий:
`MarkFailed` сохраняет окружение — разбор по логам без перезапуска.

- [ ] **Шаг Е: Финальная сверка плана со spec (Self-Review)**

Пройти критерии приёмки 1–8 spec и убедиться: каждый закрыт задачей 1–6
(критерий 8 — arch/19 уже обновлён spec-фазой, сверен в Задаче 5). Пробел —
добавить задачу, не откладывать.

- [ ] **Шаг Ж: Commit (если остались правки само-ревью)**

```bash
git add -A && git commit -m "chore(t19): мерж-гейт — серии Release зелёные, зачистка контуров, сверка плана со spec"
```
(Если правок нет — без коммита.)

---

## Само-ревью плана (выполнено при написании)

- **Покрытие spec:** §3.1 → Задача 1; §3.2 → Задача 3 (+миграция BreakAsync в
  Задаче 1); §3.3 → Задачи 4–5; §3.4 (приёмник не трогаем) — отражено в
  ограничениях; §4 Ф1–Ф4 → Задачи 1–2 / 3 / 4–5 / 6–7; критерии 1,2,3,7 →
  Задача 3 (тесты A/B/C/D); критерий 4 → Задача 2; критерий 5 → Задачи 6–7;
  критерий 6 → Задачи 4–5; критерий 8 → Задача 5 (сверка).
- **Отклонения от буквы spec с обоснованием:** (1) «юнит-тесты SQL-текстов» —
  исполняются интеграцией на реальном PG (WalSqlTests): в проекте нет
  Npgsql-моков, тексты реально прогоняются; юнит-поведение правила —
  WalStreamProcessTests на FakeSql (соответствует сложившейся структуре тестов).
  (2) Нагрузка INSERT+pg_switch_wal вместо pgbench — pgbench отсутствует в
  образе ноды; эффект (форсированное закрытие сегментов) идентичен, образец —
  AC2/AC3 того же файла. (3) В пользовательских текстах (правило панели,
  runbook) атрибуция «(t19)» опущена — правило AGENTS о исторических пассажах
  в docs/arch; мерж-коммит не должен оставлять теги слитой задачи.
  (4) E2E-сценарий дополняет «снос агента» остановкой/возвратом воркера — без
  этого супервиз (ScanIntervalSec=1) возвращает агента раньше среза и lost
  невоспроизводим; стоп/старт — существующий паттерн E2eScenarios («смерть
  контроллера»), система не мокается.
- **Типы/сигнатуры:** `SlotProbeAsync` возвращает `Result<(bool Exists, string?
  WalStatus)>` — единообразно в интерфейсе, Npgsql-реализации, фейке и вызове
  процесса; фазы журнала `slot-recreate/{shard.Name}` — одинаково в коде и
  ассертах (Задачи 3/6); `LostByDsn`/`Calls` фейка — только в тестах.
