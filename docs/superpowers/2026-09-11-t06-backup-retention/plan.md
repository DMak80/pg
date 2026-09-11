# t06-backup-retention — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Цель:** управление жизненным циклом бэкапов шардов и местом в хранилище: GFS-ретенция полных, чистка WAL ниже стартовой точки, монитор занятости bucket, API приёма per-cluster политики, панельные алерты.

**Архитектура:** ретенция — новый процесс `RetentionProcess` в `src/PgWorker.Backups/Retention/`, встраиваемый в Active-ветку `ReconcileLoop` после `backup-wal`; весь отбор/планы чистки — чистые функции `RetentionPlanner` без I/O под полным юнит-покрытием; удаление только через транзитную фазу `DELETING` (journal-before-manipulations, идемпотентные доводки); S3-delete появляется только в ретенционных путях (R4). Панель читает глобальный ключ `/pgworker/backups/storage` и зажигает `backup-storage-quota`/`backup-deleting-stuck`.

**Стек:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`, LangVersion latest), AWSSDK.S3 (уже подключён t03, новых пакетов нет), testcontainers (MinIO/etcd), xUnit v3 + FluentAssertions, AAA-комментарии в тестах.

**Spec:** [spec.md](spec.md) — план аргументируется от спецификации; исполнители читают оба документа. Канон: `arch/19-backups.md` §3/§4/§5/§8/§9/§10, `arch/adminpanel/02-etcd-contract.md` §2.3.1.

**Worktree:** `/Users/demakaev/ZCodeProject/worktrees/feat-t06-backup-retention` (ветка `feat-t06-backup-retention`). Все команды ниже — из корня worktree.

## Global Constraints

- .NET 10, `Nullable=enable`, `TreatWarningsAsErrors=true`; версии пакетов — только `Directory.Packages.props` (новых пакетов НЕТ — AWSSDK.S3 уже подключён).
- Язык: документация/комментарии/сообщения — русский; идентификаторы — английский; комментарии тестов — AAA-нотация (`// Arrange`, `// Act`, `// Assert`).
- Тесты: docker-порты ТОЛЬКО динамические (`WithPortBinding(..., assignRandomHostPort: true)` + `GetMappedPublicPort`), никаких литералов хост-портов; таймауты фикстур ≤ 100 с; каждый сценарий полностью чистит за собой (teardown при любом исходе, проверка чистоты — ассерт); после КАЖДОЙ тестовой серии — `docker rm -f $(docker ps -aq)` (не трогая контейнеры dev-стенда `as-*`/`adminpanel`, если подняты) + `docker network prune -f`.
- E2E — на свежем Release (`PGW_TEST_DOCKER=1`); обязательный мерж-гейт-маркер `Scale_AddEmptyShard`.
- Не трогать: HA-контур нод, Patroni, механику t02/t03 (планировщик/агент/контроль цепочки), префиксы чужих писателей; S3-удаления не появляются ни в каком пути, кроме ретенции (R4).
- Воркер НЕ удаляет: активные полные (PLANNED/RUNNING/UPLOADING), последний COMPLETED (guard), WAL ≥ стартовой точки оставляемых, `.history` стартового/новейших TLI, объекты при `Enabled=false`, объекты кластеров без etcd-ключей (сироты — t07).
- Поведение по умолчанию не меняется: `Backups:Enabled=false` (дефолт) — ретенция no-op, существующие тесты зелёны без правки ожиданий.
- Каждый task заканчивается коммитом; сообщения — в стиле репозитория (`feat(backups): …`, `test(e2e): …`).

---

### Task 0: Ф0 — канон + spec первым коммитом

**Вход:** в worktree незакоммичены правки канона spec-агентом: `arch/19-backups.md` (§3/§4/§5/§8/§9/§10), `arch/adminpanel/02-etcd-contract.md` (§2.3.1) и каталог `docs/superpowers/2026-09-11-t06-backup-retention/` (spec.md; plan.md появится этим task'ом).

**Действие:** закоммитить канон + spec + plan одним коммитом — arch-first требует, чтобы канон шёл в ветке ДО кода (spec §2 п.1, AC10).

**Выход:** в ветке `feat-t06-backup-retention` есть коммит с правками канона; `git status` чист (после записи plan.md).

**Проверка:** `git status --short` — пусто; `git log --oneline -1` — коммит вида `docs(arch): канон ретенции t06 (arch/19 §3/§4/§5/§8/§9/§10 + adminpanel/02 §2.3.1) + spec/plan`.

**Spec:** §4 Ф0, AC10.

- [ ] **Шаг 0.1:** убедиться, что plan.md записан в `docs/superpowers/2026-09-11-t06-backup-retention/plan.md`
- [ ] **Шаг 0.2:** `git add arch/19-backups.md arch/adminpanel/02-etcd-contract.md docs/superpowers/2026-09-11-t06-backup-retention/ && git commit -m "docs(arch): канон ретенции t06 — arch/19 §3/§4/§5/§8/§9/§10, adminpanel/02 §2.3.1 + spec/plan"`

---

### Task 1: Ф1 — `RetentionPlanner.SelectKeep` (GFS-календарь + guard + verify-приоритет)

**Files:**
- Create: `src/PgWorker.Backups/Retention/RetentionPlanner.cs`
- Test: `src/tests/PgWorker.UnitTests/Backups/RetentionPlannerTests.cs`

**Interfaces:**
- Consumes: `PgWorker.Etcd.Parsing.FullBackupState` (Id, State, StartedUnix, WalStartSegment, Verify), `BackupPolicy` (RetentionDays/RetentionWeeks/RetentionMonths) — оба уже существуют (`src/PgWorker.Etcd/Parsing/BackupsModel.cs`); `System.Globalization.ISOWeek`.
- Produces (для Task 2, 5):
  ```csharp
  namespace PgWorker.Backups;

  public sealed record RetentionSelection(
      IReadOnlySet<string> Keep,      // id оставляемых COMPLETED-полных
      IReadOnlyList<string> Delete);  // id кандидатов на удаление, старейшие/FAILED-verify вперёд

  public static partial class RetentionPlanner
  {
      public static RetentionSelection SelectKeep(
          IReadOnlyList<FullBackupState> fulls, BackupPolicy policy, long nowUnix);
  }
  ```

**Вход:** Task 0 закоммичен; модель `FullBackupState`/`BackupPolicy` существует.

**Действие:** чистая функция GFS-отбора без I/O (spec §3.1):

```csharp
using System.Globalization;
using PgWorker.Etcd.Parsing;

namespace PgWorker.Backups;

/// <summary>Итог GFS-отбора (t06, arch/19 §4): Keep — id оставляемых COMPLETED,
/// Delete — кандидаты на удаление в порядке исполнения (verify-FAILED первыми,
/// далее по возрастанию started_unix — место освобождается от самого старого).</summary>
public sealed record RetentionSelection(
    IReadOnlySet<string> Keep,
    IReadOnlyList<string> Delete);

/// <summary>Чистые функции ретенции (t06, arch/19 §4/§5): без I/O, момент —
/// аргументом (TimeProvider не нужен). Полное юнит-покрытие — риск «удалили
/// нужное» закрывается тестами детерминированности (spec §2 п.5).</summary>
public static partial class RetentionPlanner
{
    /// <summary>GFS-отбор по календарю UTC (spec §3.1): дневные — все COMPLETED
    /// последних retention.days календарных суток (включая текущую); недельные —
    /// последний COMPLETED каждой из retention.weeks свежейших предыдущих
    /// ISO-недель; месячные — аналогично по календарным месяцам. Активные
    /// (PLANNED/RUNNING/UPLOADING) и DELETING в отборе не участвуют. Guard:
    /// самый свежий COMPLETED — всегда в Keep; если после отбора Delete непусто,
    /// а Keep пуст — старейший кандидат возвращается в Keep.</summary>
    public static RetentionSelection SelectKeep(
        IReadOnlyList<FullBackupState> fulls, BackupPolicy policy, long nowUnix)
    {
        var now = DateTimeOffset.FromUnixTimeSeconds(nowUnix).UtcDateTime;
        var completed = fulls
            .Where(f => f.State == FullBackupStatus.Completed)
            .OrderBy(f => f.StartedUnix)
            .ToList();

        var keep = new HashSet<string>();

        // Дневные: все COMPLETED последних retention.days календарных суток UTC
        // (окно [today − (days−1) .. today] по дате started_unix).
        var dayCutoff = DateOnly.FromDateTime(now).AddDays(-(policy.RetentionDays - 1));
        var dailyWindow = completed
            .Where(f => DateOnly.FromDateTime(
                DateTimeOffset.FromUnixTimeSeconds(f.StartedUnix).UtcDateTime) >= dayCutoff)
            .ToList();
        foreach (var f in dailyWindow)
            keep.Add(f.Id);

        // Недельные: полные вне дневного окна группируются по ISO-неделе UTC;
        // из каждой из retention.weeks свежейших групп — последний (max started_unix).
        AddCalendarPoint(completed, dailyWindow, keep, policy.RetentionWeeks, ISOWeekYearOf);

        // Месячные: аналогично по календарному месяцу UTC.
        AddCalendarPoint(completed, dailyWindow, keep, policy.RetentionMonths, MonthOf);

        var freshest = completed.MaxBy(f => f.StartedUnix);
        if (freshest is not null)
            keep.Add(freshest.Id); // guard: самый свежий COMPLETED — всегда

        var delete = completed
            .Where(f => !keep.Contains(f.Id))
            .OrderBy(f => f.Verify is { State: BackupVerifyStatus.Failed } ? 0 : 1)
            .ThenBy(f => f.StartedUnix)
            .Select(f => f.Id)
            .ToList();

        // Guard-доводка: удалять нечего, если не остаётся ни одного COMPLETED
        // (последний валидный не удаляется — spec §3.1, AC2).
        if (delete.Count > 0 && keep.Count == 0)
        {
            var rescued = delete[0];
            keep.Add(rescued);
            delete.RemoveAt(0);
        }

        return new RetentionSelection(keep, delete);
    }
}
```

Точные приватные хелперы (положить в тот же файл, ниже `SelectKeep`):

```csharp
    // Группа ISO-недели: (ISO-week-year, номер недели). Год — ОБЯЗАТЕЛЬНО
    // ISOWeek.GetYear, НЕ календарный d.Year: дни на стыке календарных годов
    // одной ISO-недели (2024-12-30 и 2025-01-05 — обе ISO-неделя 1 ISO-2025;
    // 2027-01-01..03 — ISO-неделя 53 ISO-2026) при календарном годе рвутся на
    // ДВЕ группы — расщеплённая неделя расходует два слота retention.weeks,
    // реальная недельная точка вытесняется и удаляется (замечание ревью Ф4 №1).
    private static (int Year, int Week) ISOWeekYearOf(FullBackupState f)
    {
        var d = DateTimeOffset.FromUnixTimeSeconds(f.StartedUnix).UtcDateTime;
        return (ISOWeek.GetYear(d), ISOWeek.GetWeekOfYear(d));
    }

    private static (int Year, int Month) MonthOf(FullBackupState f)
    {
        var d = DateTimeOffset.FromUnixTimeSeconds(f.StartedUnix).UtcDateTime;
        return (d.Year, d.Month);
    }

    // Универсальная гранула (недели/месяцы): вне дневного окна → группы по
    // календарному периоду UTC → N свежейших групп (сравнение (Year, Num)
    // лексикографически) → из каждой последний COMPLETED (max started_unix).
    // counter = 0 → гранула исключена (spec AC1: weeks=0/months=0).
    private static void AddCalendarPoint<TGroup>(
        List<FullBackupState> completed, List<FullBackupState> dailyWindow,
        HashSet<string> keep, int counter,
        Func<FullBackupState, TGroup> groupOf)
        where TGroup : IComparable<TGroup>
    {
        if (counter <= 0)
            return;

        var dailyIds = dailyWindow.Select(f => f.Id).ToHashSet();
        var outsideDaily = completed
            .Where(f => !dailyIds.Contains(f.Id))
            .GroupBy(groupOf)
            .OrderByDescending(g => g.Key)
            .Take(counter);
        foreach (var group in outsideDaily)
        {
            var last = group.MaxBy(f => f.StartedUnix); // ПОСЛЕДНИЙ периода (AC1)
            if (last is not null)
                keep.Add(last.Id);
        }
    }
```

Замечание к `AddCalendarPoint`: фильтр «вне дневного окна» — по множеству `Id`, а не `dailyWindow.Contains(f)` (record-семантика `Contains` по значению — дубликаты записей с одинаковыми полями дали бы ложное «в окне»).

**Выход:** `RetentionPlanner.SelectKeep` компилируется; типы `RetentionSelection` доступны следующим задачам.

**Проверка:** `dotnet build src/PgWorker.slnx -c Release` — 0 warnings/errors (сборка до тестов).

**Spec:** §3.1 (SelectKeep), §4 Ф1, AC1, AC2 (guard-часть).

- [ ] **Шаг 1.1:** создать `RetentionPlanner.cs` с `SelectKeep` (+хелперы), пересобрать: `dotnet build src/PgWorker.slnx -c Release`
- [ ] **Шаг 1.2:** написать юниты GFS-календаря (класс `RetentionPlannerTests`, xUnit + FluentAssertions, AAA). Фиксированный момент `now` и хелпер-конструктор записей:

```csharp
using PgWorker.Backups;
using PgWorker.Etcd.Parsing;
using FluentAssertions;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// GFS-отбор RetentionPlanner.SelectKeep (t06, spec §3.1/AC1): календарь UTC,
// границы недель/месяцев (вкл. годовые границы ISO-недель — с дискриминацией
// бага на weeks>=2), guard последнего COMPLETED (вкл. verify-FAILED),
// verify-приоритет порядка удаления.
public class RetentionPlannerTests
{
    // Фиксированный момент: среда 2026-09-09 12:00:00 UTC.
    private static readonly DateTimeOffset Now = new(2026, 9, 9, 12, 0, 0, TimeSpan.Zero);
    private static long Unix(DateTimeOffset t) => t.ToUnixTimeSeconds();
    private static long NowUnix => Unix(Now);

    private static readonly BackupPolicy Policy = new(7, 4, 6, 86400, verifyOnCreate: false);

    // COMPLETED-полный с заданным стартом (остальные поля — нейтральные).
    private static FullBackupState Full(string id, DateTimeOffset started, BackupVerify? verify = null)
        => new(id, FullBackupStatus.Completed, "n1", BackupSourceRole.Replica,
            Unix(started), Unix(started) + 60, "000000010000000000000001", 1024, null, verify);

    // Проверка Keep/Delete по id в фиксированный момент Now.
    private static (IReadOnlySet<string> Keep, IReadOnlyList<string> Delete) Select(
        BackupPolicy policy, params FullBackupState[] fulls)
    {
        var selection = RetentionPlanner.SelectKeep(fulls, policy, NowUnix);
        return (selection.Keep, selection.Delete);
    }

    // То же с переопределяемым моментом (календарные кейсы на других годах).
    private static (IReadOnlySet<string> Keep, IReadOnlyList<string> Delete) SelectAt(
        DateTimeOffset now, BackupPolicy policy, params FullBackupState[] fulls)
    {
        var selection = RetentionPlanner.SelectKeep(fulls, policy, Unix(now));
        return (selection.Keep, selection.Delete);
    }
```

Обязательные кейсы (каждый — отдельный `[Fact]`, все с AAA-комментариями):
  - `Дневные_все_свежие_остаются`: 3 полных за сегодня/вчера/позавчера при days=7 → все в Keep, Delete пуст.
  - `Недельная_точка_последний_в_неделе`: 2 полных в одной ISO-недели старше дневного окна (напр. 2026-08-19 10:00 и 2026-08-21 18:00, weeks=1) → в Keep только вторая (max started_unix), первая в Delete (AC1: последний, не первый).
  - `Месячная_точка_последний_в_месяце`: аналогично для календарного месяца (2026-06-02 / 2026-06-28, months=1) → остаётся 2026-06-28.
  - `Граница_недели`: полные в соседних ISO-неделях (2026-08-23 вс. и 2026-08-24 пн.) при weeks=1 → остаётся понедельник (свежейшая группа), воскресенье — Delete (AC1: границы недель).
  - `Граница_месяца`: 2026-07-31 и 2026-08-01 при months=1 → остаётся август.
  - `Годовая_граница_ISO_недели` (ревью Ф4 №1, уточнение итерации 2 — weeks=2): now = 2025-01-08; полные 2024-12-30 (пн) и 2025-01-05 (вс) — ОДНА ISO-неделя 1 ISO-2025 (`ISOWeek.GetYear` обеих дат = 2025); days=1, weeks=2, months=0. Корректная реализация: одна группа → ОДИН слот недель, второй слот пуст → Keep = {2025-01-05}, Delete = [2024-12-30]. Багованная (календарный d.Year): две группы занимают ОБА слота weeks=2 → Keep = {2024-12-30, 2025-01-05}, Delete = [] — ассерт `Delete == [2024-12-30]` падает. Именно weeks≥2 дискриминирует баг: при weeks=1 свежейшая группа даёт того же представителя в обеих реализациях (проверено расчётом: keep=[2025-01-05] в обеих).
  - `Годовая_граница_ISO_недели_53` (ревью Ф4 №1, уточнение итерации 2 — weeks=2): now = 2027-01-04 (пн); полные 2026-12-28 (пн) и 2027-01-01 (пт) — обе ISO-неделя 53 ISO-2026; days=1, weeks=2, months=0. Корректно: одна группа → Keep = {2027-01-01}, Delete = [2026-12-28]; баг: две группы занимают оба слота → Delete = [] — ассерт падает.
  - `Weeks0_исключает_гранулу`: полные 3 недели назад, weeks=0, months=0, days=1 → все старые в Delete.
  - `Активные_и_DELETING_вне_отбора`: Running/Uploading/Planned/Deleting записи с любыми датами → не влияют на Keep/Delete (могут быть «свежее» — не спасают старые).
  - `Guard_единственный_просроченный_остается`: один COMPLETED 40 дней назад, days=1/weeks=0/months=0 → Keep содержит его, Delete пуст (AC2).
  - `Guard_единственный_Completed_VerifyFailed_остается` (ревью Ф4 №5): единственный COMPLETED 40 дней назад с `Verify = new BackupVerify(BackupVerifyStatus.Failed, null)` → остаётся в Keep, Delete пуст — guard «≥1 COMPLETED» держится и над verify-FAILED (AC2, вторая фраза).
  - `Guard_самый_свежий_всегда_в_Keep`: старая пачка + самый свежий вне любого окна (напр. вчера 23:59 при days=1) — самый свежий в Keep.
  - `VerifyFailed_первые_в_Delete`: два кандидата на удаление — старший OK и младший с `new BackupVerify(BackupVerifyStatus.Failed, null)` → Delete[0] = verify-FAILED (порядок, не состав).
  - `Пустой_набор`: fulls пуст → Keep/Delete пустые.
  - `Политика_0_0_0_с_guard`: days=1, weeks=0, months=0, единственный COMPLETED сегодня → в Keep; второй COMPLETED 40 дней назад → в Delete.
  - `Високосность`: now = 2028-02-29 12:00Z, полные 2028-02-28 и 2028-02-01 при days=7 → оба в Keep (февраль без сбоев).
- [ ] **Шаг 1.3:** прогнать: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --filter FullyQualifiedName~RetentionPlannerTests` — все зелёные.
- [ ] **Шаг 1.4:** коммит: `git add src/PgWorker.Backups/Retention/RetentionPlanner.cs src/tests/PgWorker.UnitTests/Backups/RetentionPlannerTests.cs && git commit -m "feat(backups): GFS-отбор ретенции SelectKeep — календарь UTC, guard, verify-приоритет (t06 Ф1)"`

---

### Task 2: Ф1 — `SelectWalForDeletion`, `EvaluateStorage`, `SelectFailedForPrune`, `StorageStatusJson`

**Files:**
- Modify: `src/PgWorker.Backups/Retention/RetentionPlanner.cs` (добавить функции)
- Create: `src/PgWorker.Backups/Retention/StorageStatusJson.cs`
- Test: `src/tests/PgWorker.UnitTests/Backups/RetentionPlannerTests.cs` (добавить)
- Test: `src/tests/PgWorker.UnitTests/Backups/StorageStatusJsonTests.cs`

**Interfaces:**
- Consumes: `WalFileName` (`src/PgWorker.Backups/Model/WalFileName.cs`: TryParse/TryParseHistory/IsPartial/Log/Seg/Tli/SegsPerLog), `FullBackupState`, `BackupVerifyStatus`.
- Produces (для Task 3, 5):
  ```csharp
  public static IReadOnlyList<string> SelectWalForDeletion(
      IReadOnlyList<string> objectNames, WalFileName cutoff);

  public enum StorageState { Ok, Warn, Crit }
  public sealed record StorageVerdict(StorageState State, long UsedBytes, long QuotaBytes, double? UsedPercent);
  public static StorageVerdict EvaluateStorage(long usedBytes, long quotaBytes, int warnPercent, int critPercent);

  public static IReadOnlyList<string> SelectFailedForPrune(
      IReadOnlyList<FullBackupState> fulls, int keepFailed);
  ```
  `StorageStatusJson`:
  ```csharp
  namespace PgWorker.Backups;
  public sealed record StorageStatus(
      long UsedBytes, long QuotaBytes, double? UsedPercent, StorageState State, long UpdatedUnix);
  public static class StorageStatusJson
  {
      public static string Serialize(StorageStatus status);   // {"used_bytes":…,"quota_bytes"?,"used_percent"?,"state":"OK|WARN|CRIT","updated_unix":…}
  }
  ```

**Вход:** Task 1 смержён в ветку (класс `RetentionPlanner` существует).

**Действие:** остальные чистые функции ретенции + сериализация ключа хранилища.

`SelectWalForDeletion` (правила spec §3.1 + канон §5 «Ретенционная чистка»):

```csharp
    /// <summary>План чистки WAL (spec §3.1): сегменты строго ниже cutoff (TLI
    /// ниже ИЛИ тот же TLI с позицией log·256+seg ниже cutoff) и `.history`
    /// TLI ниже стартового — на удаление. Сам cutoff, сегменты выше/новее,
    /// `.history` cutoff-TLI и новее, `.partial` и нераспознаваемые имена —
    /// НЕ входят (консервативно: объект TLI&gt;cutoff позицией ниже cutoff не
    /// трогаем — контроль t03 его игнорирует, удалять незачем).</summary>
    public static IReadOnlyList<string> SelectWalForDeletion(
        IReadOnlyList<string> objectNames, WalFileName cutoff)
    {
        var result = new List<string>();
        foreach (var name in objectNames)
        {
            if (WalFileName.TryParse(name) is { } segment)
            {
                var below = segment.Tli < cutoff.Tli
                    || (segment.Tli == cutoff.Tli
                        && (long)(segment.Log * WalFileName.SegsPerLog + segment.Seg)
                           < (long)(cutoff.Log * WalFileName.SegsPerLog + cutoff.Seg));
                if (below)
                    result.Add(name);
            }
            else if (WalFileName.TryParseHistory(name) is { } tli && tli < cutoff.Tli)
                result.Add(name); // .history старых TLI — сегменты их диапазонов удалены
        }

        return result;
    }
```

`EvaluateStorage` + `SelectFailedForPrune`:

```csharp
    /// <summary>Вердикт занятости (spec §3.1): квота 0/не задана → OK без
    /// процентов; иначе usedPercent &gt;= crit → CRIT, &gt;= warn → WARN, иначе OK.</summary>
    public static StorageVerdict EvaluateStorage(long usedBytes, long quotaBytes, int warnPercent, int critPercent)
    {
        if (quotaBytes <= 0)
            return new StorageVerdict(StorageState.Ok, usedBytes, 0, null);

        var percent = Math.Round(usedBytes * 100.0 / quotaBytes, 2);
        var state = percent >= critPercent ? StorageState.Crit
            : percent >= warnPercent ? StorageState.Warn
            : StorageState.Ok;
        return new StorageVerdict(state, usedBytes, quotaBytes, percent);
    }

    /// <summary>Гигиена FAILED-истории (spec §3.3 п.5): держать последние
    /// keepFailed по started_unix, старше — del. Бэкофф t02 не ломается: n
    /// остаётся ≤ keepFailed, а BaseSec·2^(n−1) упирается в MaxSec раньше
    /// границы (AC5).</summary>
    public static IReadOnlyList<string> SelectFailedForPrune(
        IReadOnlyList<FullBackupState> fulls, int keepFailed)
        => fulls
            .Where(f => f.State == FullBackupStatus.Failed)
            .OrderByDescending(f => f.StartedUnix)
            .Skip(Math.Max(0, keepFailed))
            .OrderBy(f => f.StartedUnix) // старейшие вперёд (порядок исполнения)
            .Select(f => f.Id)
            .ToList();
```

`StorageStatusJson.cs` (по образцу `BackupStatusJson`/`WalStatusWriter.ToJson`):

```csharp
using System.Text.Json;

namespace PgWorker.Backups;

/// <summary>Занятость хранилища установки — ключ /pgworker/backups/storage
/// (t06, arch/19 §4): глобальный, пишет ретенционный проход при изменении;
/// quota_bytes=0 → поля квоты опускаются, state=OK.</summary>
public sealed record StorageStatus(
    long UsedBytes, long QuotaBytes, double? UsedPercent, StorageState State, long UpdatedUnix);

public static class StorageStatusJson
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);

    public static string Serialize(StorageStatus status)
    {
        var o = new Dictionary<string, object?>
        {
            ["used_bytes"] = status.UsedBytes,
            ["state"] = StateName(status.State),
            ["updated_unix"] = status.UpdatedUnix,
        };
        if (status.QuotaBytes > 0)
        {
            o["quota_bytes"] = status.QuotaBytes;
            o["used_percent"] = status.UsedPercent;
        }

        return JsonSerializer.Serialize(o, Options);
    }

    public static string StateName(StorageState state) => state switch
    {
        StorageState.Warn => "WARN",
        StorageState.Crit => "CRIT",
        _ => "OK",
    };
}
```

**Выход:** все чистые функции ретенции + JSON ключа storage готовы; `RetentionPlanner` — `static partial class` (расширять дальше не будем — финальный вид `public static partial class` допустим, но можно убрать `partial` после Task 2, если не добавляем методов; оставь `partial` — безвредно).

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --filter "FullyQualifiedName~RetentionPlannerTests|FullyQualifiedName~StorageStatusJsonTests"` — зелёные.

**Spec:** §3.1 (SelectWalForDeletion, EvaluateStorage), §3.4 (StorageStatusJson), §4 Ф1, AC1/AC4 (юнит-часть)/AC5/AC6 (юнит-часть).

- [ ] **Шаг 2.1:** добавить `SelectWalForDeletion`/`EvaluateStorage`/`SelectFailedForPrune` в `RetentionPlanner.cs`; создать `StorageStatusJson.cs`; `dotnet build src/PgWorker.slnx -c Release`.
- [ ] **Шаг 2.2:** юниты `SelectWalForDeletion` (в `RetentionPlannerTests`):
  - `Сегменты_ниже_cutoff_удалены`: cutoff = `WalFileName(1, 0, 5)`; имена `…00000003`(seg3), `…00000004` → в результате; `…00000005` (сам cutoff), `…00000006`, `…000000FF` — нет (AC4).
  - `Граница_log_сег_FF`: cutoff = `WalFileName(1, 1, 0)`; `0000000100000000000000FF` (log0 seg255, позиция 255 < 256) → удалён.
  - `TLI_ниже_удалены_выше_не_тронуты`: cutoff = `WalFileName(2, 0, 3)`; `0000000100000000000000AA` (tli1 ниже) → удалён; `000000030000000000000001` (tli3 выше, позиция ниже cutoff) → НЕ удалён (консервативность).
  - `History_старых_TLI_удалены`: `00000001.history` при cutoff tli=2 → удалён; `00000002.history` (cutoff-TLI) и `00000003.history` → живы; `.partial` → жив.
- [ ] **Шаг 2.3:** юниты `EvaluateStorage`:
  - `Квота_0_без_процентов`: `EvaluateStorage(123, 0, 80, 90)` → State=Ok, QuotaBytes=0, UsedPercent=null.
  - `Пороги_Warn_Crit`: used/quota = 79.9/80/90 → Ok; ровно 80 → Warn; ровно 90 → Crit; 99 → Crit.
- [ ] **Шаг 2.4:** юниты `SelectFailedForPrune` + AC5: 30 FAILED после COMPLETED, keepFailed=20 → ровно 10 старейших id; затем ассерт того, что бэкофф не сброшен:

```csharp
    // AAA: чистка FAILED до KeepFailed не сбрасывает бэкофф t02 — окно попытки
    // остаётся MaxSec-capped и на полной, и на почищенной истории (AC5).
    [Fact]
    public void Prune_до_KeepFailed_не_меняет_BackoffPassed()
    {
        // Arrange — 30 FAILED после последнего COMPLETED (BaseSec=300, MaxSec=3600)
        var fulls = new List<FullBackupState> { Full("done", Now.AddDays(-3)) };
        for (var i = 0; i < 30; i++)
            fulls.Add(Full($"f{i:00}", Now.AddMinutes(-60 + i)) with { State = FullBackupStatus.Failed });
        var prunedIds = RetentionPlanner.SelectFailedForPrune(fulls, keepFailed: 20).ToHashSet();
        var pruned = fulls.Where(f => !prunedIds.Contains(f.Id)).ToList();
        var nowUnix = Unix(Now.AddMinutes(10));

        // Act — бэкофф на истории до/после чистки
        var before = BackupPlanner.BackoffPassed(fulls, 300, 3600, nowUnix);
        var after = BackupPlanner.BackoffPassed(pruned, 300, 3600, nowUnix);

        // Assert — вердикт одинаков (n=30 и n=20 дают одинаково capped-окно)
        before.Should().Be(after);
    }
```
  (`BackupPlanner` — `src/PgWorker.Backups/Process/BackupPlanner.cs`, уже существует; using `PgWorker.Backups` уже есть.)
- [ ] **Шаг 2.5:** юниты `StorageStatusJson` (новый класс `StorageStatusJsonTests`): сериализация с квотой содержит `used_bytes`/`quota_bytes`/`used_percent`/`state`/`updated_unix` (строковые ассерты `Contains("\"used_bytes\":")`); без квоты — полей квоты нет и `state:"OK"`; WARN/CRIT имена.
- [ ] **Шаг 2.6:** прогон фильтром из Проверки; коммит `feat(backups): чистые функции WAL-чистки/вердикта хранилища/гигиены FAILED + JSON storage (t06 Ф1)`.

---

### Task 3: Ф2 — S3: `ListPrefixAsync` + `DeleteKeysAsync` (интеграция MinIO)

**Files:**
- Modify: `src/PgWorker.Backups/BackupS3.cs` (интерфейс + реализация)
- Modify: `src/PgWorker.App/Program.cs` (делегаты `ReloadableBackupS3` + `DisabledBackupS3`)
- Modify: `src/tests/PgWorker.IntegrationTests/Backups/FakeBackupDeps.cs` (расширения)
- Test: `src/tests/PgWorker.IntegrationTests/Backups/BackupS3Tests.cs` (добавить)

**Interfaces:**
- Consumes: существующий `IBackupS3` (`BucketExistsAsync`, `ListWalAsync`), `BackupsRuntimeOptions`.
- Produces (для Task 5, 10):

  ```csharp
  // src/PgWorker.Backups/BackupS3.cs — добавить рядом с WalObject:
  /// <summary>Объект bucket с размером (ретенционные list'ы t06, arch/19 §5).</summary>
  public sealed record S3ObjectInfo(string Key, long SizeBytes, DateTimeOffset LastModified);

  // интерфейс IBackupS3 дополняется:
  /// <summary>list-objects-v2 с пагинацией по произвольному префиксу (full/&lt;id&gt;/,
  /// wal/, "" — весь bucket) с размерами; maxKeysPerTest — инъекция страницы.</summary>
  Task<Result<IReadOnlyList<S3ObjectInfo>>> ListPrefixAsync(
      string prefix, int? maxKeysPerTest = null, CancellationToken ct = default);

  /// <summary>batch-delete (DeleteObjects, чанки ≤1000); идемпотентно —
  /// отсутствие ключа в ответе не ошибка (повтор прохода безопасен).</summary>
  Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default);
  ```

**Вход:** Task 2 закоммичен.

**Действие:**
1. Реализация в `BackupS3` (list — копия цикла `ListWalAsync`, но без обрезки имени: ключ целиком; delete — `DeleteObjectsAsync` чанками по 1000; пустой список — `Result.Success()`):

```csharp
    public async Task<Result<IReadOnlyList<S3ObjectInfo>>> ListPrefixAsync(
        string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
    {
        try
        {
            var result = new List<S3ObjectInfo>();
            string? token = null;
            do
            {
                var request = new ListObjectsV2Request
                {
                    BucketName = _bucket,
                    Prefix = prefix,
                    ContinuationToken = token,
                };
                if (maxKeysPerTest is { } maxKeys)
                    request.MaxKeys = maxKeys;
                var page = await _client.ListObjectsV2Async(request, ct);
                foreach (var obj in page.S3Objects)
                    result.Add(new S3ObjectInfo(obj.Key, obj.Size, obj.LastModified));
                token = page.IsTruncated is true ? page.NextContinuationToken : null;
            }
            while (token is not null);

            return Result<IReadOnlyList<S3ObjectInfo>>.Success(result);
        }
        catch (Exception e)
        {
            return Result<IReadOnlyList<S3ObjectInfo>>.Failed(new ApplicationException(
                $"S3 list {prefix}: {e.Message}", e));
        }
    }

    public async Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default)
    {
        if (keys.Count == 0)
            return Result.Success();
        try
        {
            foreach (var chunk in keys.Chunk(1000))
            {
                var request = new DeleteObjectsRequest
                {
                    BucketName = _bucket,
                    Objects = chunk.Select(k => new KeyVersion { Key = k }).ToList(),
                };
                var response = await _client.DeleteObjectsAsync(request, ct);
                // Идемпотентность: отсутствующие ключи не приходят в ответ — не ошибка.
                if (response.DeleteErrors is { Count: > 0 } errors)
                    return Result.Failed(new ApplicationException(
                        $"S3 batch-delete: {errors[0].Key}: {errors[0].Message}"));
            }

            return Result.Success();
        }
        catch (Exception e)
        {
            return Result.Failed(new ApplicationException($"S3 batch-delete: {e.Message}", e));
        }
    }
```

2. `ReloadableBackupS3` в `src/PgWorker.App/Program.cs` — добавить делегаты (по образцу существующих):

```csharp
    public async Task<PgWorker.Core.Result<IReadOnlyList<S3ObjectInfo>>> ListPrefixAsync(
        string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
        => await (await CurrentAsync()).ListPrefixAsync(prefix, maxKeysPerTest, ct);

    public async Task<PgWorker.Core.Result> DeleteKeysAsync(
        IReadOnlyList<string> keys, CancellationToken ct = default)
        => await (await CurrentAsync()).DeleteKeysAsync(keys, ct);
```
   и в `DisabledBackupS3` — заглушки `Failed(new ApplicationException("Backups:Enabled=false"))`.

3. `FakeBackupS3` (`src/tests/PgWorker.IntegrationTests/Backups/FakeBackupDeps.cs`) — реализовать новые методы: хранилище объектов расширить до полных ключей. Заметь: текущее поле `Objects` — `List<(string Cluster, string Shard, string Name)>`; добавь параллельное `public List<(string Key, long SizeBytes)> PrefixObjects { get; } = [];` для объектов произвольных префиксов + `public List<string> DeletedKeys { get; } = [];` и флаг `public bool FailNextDelete { get; set; }` (для AC3-сценария сбоя S3). `ListPrefixAsync` возвращает `PrefixObjects` (плюс wal-объекты, сведённые в ключи `$"{o.Cluster}/{o.Shard}/wal/{o.Name}"`) с фильтром `Key.StartsWith(prefix)`; `DeleteKeysAsync` удаляет из обоих хранилищ и пишет `DeletedKeys`.

**Выход:** `IBackupS3` умеет list с размерами по любому префиксу и batch-delete; все реализации (BackupS3, Reloadable, Disabled, Fake) синхронны с интерфейсом.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter FullyQualifiedName~BackupS3Tests` — зелёные (фикстура MinioCollection поднимет MinIO с динамическим портом). После серии: `docker rm -f $(docker ps -aq) 2>/dev/null; docker network prune -f`.

**Spec:** §3.2, §4 Ф2, R4 (delete только здесь появляется).

- [ ] **Шаг 3.1:** расширить `IBackupS3`/`BackupS3` (+`S3ObjectInfo`), делегаты `ReloadableBackupS3`/`DisabledBackupS3`, `FakeBackupS3`; `dotnet build src/PgWorker.slnx -c Release` (компиляция всей цепочки реализаций).
- [ ] **Шаг 3.2:** интеграционные тесты в `BackupS3Tests` (MinIO, сид прямым AWSSDK-клиентом — образец `SeedClient`; AAA):
  - `ListPrefix_размеры_и_полные_ключи`: положить объекты `c9/shard1/full/20260901/base.tar` (тело 10 байт) и `c9/shard1/wal/000000010000000000000001`; `ListPrefixAsync("c9/shard1/full/")` → один объект, `SizeBytes == 10`, `Key` полный.
  - `ListPrefix_пустой_префикс_весь_bucket`: объекты в 2 кластер-префиксах → `ListPrefixAsync("")` возвращает все (в т.ч. чужие — база used_bytes).
  - `ListPrefix_пагинация`: 5 объектов, `maxKeysPerTest: 2` → все 5.
  - `DeleteKeys_удаляет_и_идемпотентен`: удалить 2 ключа → list пуст; повторный `DeleteKeysAsync` тех же ключей → Success (несуществующие — не ошибка).
  - `DeleteKeys_пустой_список`: Success без вызова S3.
  - `DeleteKeys_чанки`: 3 объекта, `maxKeysPerTest` тут не при чём — чанк-размер не инъекцируется; вместо этого ассерт, что 1002 ключа удаляются (сид циклом 1002 маленьких объектов, потом list пуст). Если сид 1002 объектов медленный в MinIO (> ~30 c) — заменить на 1001.
- [ ] **Шаг 3.3:** прогон, зачистка (`docker rm -f $(docker ps -aq) 2>/dev/null; docker network prune -f`), коммит `feat(backups): ListPrefixAsync/DeleteKeysAsync — S3 list с размерами + batch-delete (t06 Ф2)`.

---

### Task 4: Ф3 — опции `Retention`/`Quota` + валидация + дефолт-политика в runtime

**Files:**
- Modify: `src/PgWorker.App/Options.cs` (`BackupsOptions` + новые классы)
- Modify: `src/PgWorker.Backups/Options.cs` (`BackupsRuntimeOptions` — именованные параметры в конец)
- Modify: `src/PgWorker.App/appsettings.json` (секция Backups — новые подсекции с дефолтами)
- Test: `src/tests/PgWorker.UnitTests/App/BackupsOptionsTests.cs` (добавить)

**Interfaces:**
- Consumes: `BackupsOptions`/`BackupsRuntimeOptions` (существующие).
- Produces (для Task 5, 6, 10):

  ```csharp
  // src/PgWorker.App/Options.cs
  /// <summary>Параметры ретенционного прохода (t06, arch/19 §9): период и
  /// FAILED-глубина. Имя НЕ BackupsRetentionOptions — то занято GFS-гранулами
  /// политики (BackupsPolicyOptions.Retention).</summary>
  public sealed class BackupsRetentionPassOptions
  {
      public int IntervalSec { get; set; } = 600;
      public int KeepFailed { get; set; } = 20;
  }

  /// <summary>Квота bucket установки (t06, arch/19 §9): Bytes=0 — квота не задана.</summary>
  public sealed class BackupsQuotaOptions
  {
      public long Bytes { get; set; }
      public int WarnPercent { get; set; } = 80;
      public int CritPercent { get; set; } = 90;
  }
  ```
  В `BackupsOptions` добавить свойства `public BackupsRetentionPassOptions Retention { get; set; } = new();` и `public BackupsQuotaOptions Quota { get; set; } = new();`, расширить `ToRuntime()` и `IsValid()` (см. ниже).
  В `BackupsRuntimeOptions` добавить в КОНЕЦ позиционной записи именованные параметры (образец t03 — «record расширялся с обеих сторон»):
  ```csharp
      // t06 (arch/19 §9): ретенция и квота; дефолт-политика GFS для кластеров без policy-ключа.
      int PolicyRetentionDays = 7,
      int PolicyRetentionWeeks = 4,
      int PolicyRetentionMonths = 6,
      int RetentionIntervalSec = 600,
      int RetentionKeepFailed = 20,
      long QuotaBytes = 0,
      int QuotaWarnPercent = 80,
      int QuotaCritPercent = 90)
  ```

**Вход:** Task 3 закоммичен.

**Действие:**
1. Классы и свойства выше.
2. `ToRuntime()` — дописать именованные аргументы:
   ```csharp
   PolicyRetentionDays: Policy.Retention.Days,
   PolicyRetentionWeeks: Policy.Retention.Weeks,
   PolicyRetentionMonths: Policy.Retention.Months,
   RetentionIntervalSec: Retention.IntervalSec,
   RetentionKeepFailed: Retention.KeepFailed,
   QuotaBytes: Quota.Bytes,
   QuotaWarnPercent: Quota.WarnPercent,
   QuotaCritPercent: Quota.CritPercent);
   ```
3. `IsValid()` — конъюнкция с существующим телом:
   ```csharp
   public bool IsValid()
       => (!Enabled
           || (!string.IsNullOrWhiteSpace(S3.Endpoint)
               && !string.IsNullOrWhiteSpace(S3.Bucket)
               && !string.IsNullOrWhiteSpace(S3.AccessKey)
               && !string.IsNullOrWhiteSpace(S3.SecretKey)
               && !string.IsNullOrWhiteSpace(Job.Image)))
       && Quota.WarnPercent < Quota.CritPercent
       && Quota.CritPercent <= 100
       && Retention.IntervalSec >= 60
       && Retention.KeepFailed >= 5;
   ```
   (валидация ретенции/квоты действует и при `Enabled=false` — мусорный конфиг виден на старте; дефолты проходят, существующий тест `Default_DisabledAndValid_PathStyleTrue` остаётся зелёным).
4. `appsettings.json` — в секцию `Backups` добавить:
   ```json
   "Retention": { "IntervalSec": 600, "KeepFailed": 20 },
   "Quota": { "Bytes": 0, "WarnPercent": 80, "CritPercent": 90 }
   ```
5. Сообщение `Program.cs` `.Validate(...)` не менять (текст останется про S3-комплект; при желании дописать через `; ` краткое «Retention/Quota диапазоны» — опционально, не обязательно).

**Выход:** конфиг `PgWorker:Backups:Retention`/`Quota` считывается, валидируется fail-fast и проксируется в `BackupsRuntimeOptions`.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --filter FullyQualifiedName~BackupsOptionsTests` — зелёные (старые 4 теста не правятся).

**Spec:** §3.7, §4 Ф3 (опции), канон §9.

- [ ] **Шаг 4.1:** правки `Options.cs` (обa файла), `appsettings.json`; `dotnet build src/PgWorker.slnx -c Release`.
- [ ] **Шаг 4.2:** добавить юниты (AAA): `Quota_WarnAboveCrit_Invalid` (Warn=90,Crit=80 → `IsValid()==false`); `Quota_CritAbove100_Invalid`; `Retention_IntervalBelow60_Invalid`; `KeepFailedBelow5_Invalid`; `Дефолты_валиды` (`new BackupsOptions().IsValid()` — true, повторяет существующий кейс с новыми полями); `ToRuntime_несёт_ретенцию_и_квоту` (задать экзотические значения → сверить все 8 полей runtime).
- [ ] **Шаг 4.3:** прогон фильтром; коммит `feat(backups): опции Retention/Quota + fail-fast валидация + дефолт-политика в runtime (t06 Ф3)`.

---

### Task 5: Ф3 — `RetentionProcess` + интеграционные тесты

**Files:**
- Create: `src/PgWorker.Backups/Retention/RetentionProcess.cs`
- Test: `src/tests/PgWorker.IntegrationTests/Backups/RetentionProcessTests.cs`

**Interfaces:**
- Consumes: `RetentionPlanner` (Task 1–2), `IBackupS3.ListPrefixAsync/DeleteKeysAsync` (Task 3), `BackupsRuntimeOptions` (Task 4), `IEtcdGateway` (`PutAsync/GetAsync/DeleteAsync(endpoint, keyOrPrefix, prefix, ct)`), `ClaimStore.IsMine`, `WorkJournal.WritePhaseAsync`, `BackupNames.FullKey`, `BackupStatusJson.Serialize`, `ProcessOutcome` (PgWorker.Core), образец `BackupProcess` (клэйм/guard/failover-put) и `WalStreamProcess` (try/catch на шард).
- Produces (для Task 6):
  ```csharp
  namespace PgWorker.Backups;

  public sealed class RetentionProcess(
      IEtcdGateway etcd,
      string[] endpoints,
      IBackupS3 s3,
      ClaimStore claims,
      WorkJournal journal,
      BackupsRuntimeOptions options,
      TimeProvider time,
      ILogger<RetentionProcess> logger)
  {
      public Task<Result<ProcessOutcome>> TickAsync(
          ClusterSnapshot snap, ClusterBackups? backups, CancellationToken ct);
  }
  ```

**Вход:** Task 4 закоммичен.

**Действие:** машина одного тика per-cluster (spec §3.3). Каркас с точной логикой:

```csharp
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using PgWorker.Backups.Job;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;

namespace PgWorker.Backups;

/// <summary>Ретенционный проход (t06, arch/19 §4): под клэймом &lt;C&gt; по
/// расписанию Retention:IntervalSec для каждого шарда — доводка DELETING,
/// GFS-отбор (один кандидат-полный в DELETING за проход), чистка WAL ниже
/// cutoff оставляемых, гигиена FAILED; после шардов — монитор занятости
/// /pgworker/backups/storage. Enabled=false → no-op. Ошибка шарда не роняет
/// остальные (образец WalStreamProcess). Единственный удаляющий S3-объектов
/// воркера (R4).</summary>
public sealed class RetentionProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IBackupS3 s3,
    ClaimStore claims,
    WorkJournal journal,
    BackupsRuntimeOptions options,
    TimeProvider time,
    ILogger<RetentionProcess> logger)
{
    private const string Op = "backups-retention";

    // Расписание per-cluster (ключ — имя кластера). Монитор занятости —
    // ОТДЕЛЬНОЕ поле: имя кластера "storage" валидно по regex
    // ^[a-z][a-z0-9_]{0,62}$ — общий словарь коллидировал бы, и расписание
    // кластера с монитором взаимно отодвигали бы друг друга (ревью Ф4-2 №2).
    private readonly ConcurrentDictionary<string, long> _lastPassUnix = [];

    private long _lastStoragePassUnix;

    public async Task<Result<ProcessOutcome>> TickAsync(
        ClusterSnapshot snap, ClusterBackups? backups, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Мутации /pgworker/backups/* — только держатель клэйма (arch/19 §4).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"{Op} {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        // G0: подсистема выключена — no-op (AC8).
        if (!options.Enabled)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        // Только Active-кластер (spec §3.3 G0).
        if (snap.Config.State != ClusterState.Active)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        var nowUnix = time.GetUtcNow().ToUnixTimeSeconds();

        // S: расписание per-cluster; не время — только storage-монитор.
        if (nowUnix - _lastPassUnix.GetOrAdd(cluster, 0L) >= options.RetentionIntervalSec)
        {
            _lastPassUnix[cluster] = nowUnix;

            // Дефолт-политика: policy-ключ кластера ?? конфиг (spec §3.3 п.2).
            var policy = backups?.Policy ?? new BackupPolicy(
                options.PolicyRetentionDays, options.PolicyRetentionWeeks,
                options.PolicyRetentionMonths, options.FullMaxAgeSec, options.VerifyOnCreate);
            var shards = backups?.Shards
                         ?? (IReadOnlyDictionary<string, ShardBackups>)new Dictionary<string, ShardBackups>();

            foreach (var shard in snap.Shards)
            {
                if (!shards.TryGetValue(shard.Name, out var shardBackups))
                    continue; // шард без ключей бэкапов → skip (сироты — t07)

                try
                {
                    await TickShardAsync(cluster, shard.Name, shardBackups, policy, nowUnix, ct);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    // ошибка шарда не роняет остальные (образец WalStreamProcess)
                    logger.LogError(ex, "{Op} {Cluster}/{Shard}: {Message}", Op, cluster, shard.Name, ex.Message);
                    await journal.WritePhaseAsync(cluster, Op, "shard-error", claims.InstanceId,
                        $"{shard.Name}: {ex.Message}", ct);
                }
            }
        }

        // Монитор занятости (spec §3.4) — per-instance расписание.
        var storageResult = await MonitorStorageAsync(nowUnix, ct);
        if (!storageResult.IsSuccess)
            return Result<ProcessOutcome>.Failed(storageResult.Error!);

        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }
```

`TickShardAsync` — шаги 1–5 спецификации §3.3:

```csharp
    private async Task TickShardAsync(
        string cluster, string shard, ShardBackups shardBackups, BackupPolicy policy,
        long nowUnix, CancellationToken ct)
    {
        var fulls = shardBackups.Full;

        // (1) DELETING-доводка: list full/<id>/ → delete → list пуст → del ключа.
        foreach (var deleting in fulls.Where(f => f.State == FullBackupStatus.Deleting))
            await FinishDeletionAsync(cluster, shard, deleting, ct);

        // (2) GFS-отбор COMPLETED-полных.
        var selection = RetentionPlanner.SelectKeep(fulls, policy, nowUnix);

        // (3) Один кандидат за проход (spec §3.3 п.3): journal-before-manipulations.
        if (selection.Delete.Count > 0)
        {
            var candidate = fulls.First(f => f.Id == selection.Delete[0]);
            var marked = candidate with { State = FullBackupStatus.Deleting };
            var put = await PutAsync(BackupNames.FullKey(cluster, shard, candidate.Id),
                BackupStatusJson.Serialize(marked), ct);
            if (!put.IsSuccess)
                throw new ApplicationException($"put DELETING: {put.Error!.Message}");
            await FinishDeletionAsync(cluster, shard, marked, ct);
        }

        // (4) Чистка WAL: cutoff = min(wal_start оставляемых COMPLETED — Keep ∪
        // неподавленные шагом 3, т.е. все текущие COMPLETED минус удалённый).
        var remaining = fulls.Where(f =>
            f.State == FullBackupStatus.Completed
            && !(selection.Delete.Count > 0 && f.Id == selection.Delete[0]));
        var cutoffs = remaining
            .Select(f => WalFileName.TryParse(f.WalStartSegment ?? ""))
            .Where(w => w is not null)
            .Select(w => w!.Value)
            .OrderBy(w => w.Name, StringComparer.Ordinal)
            .Cast<WalFileName?>()
            .FirstOrDefault();
        if (cutoffs is { } cutoff)
        {
            var listed = await s3.ListPrefixAsync($"{cluster}/{shard}/wal/", ct: ct);
            if (!listed.IsSuccess)
                throw new ApplicationException($"list wal: {listed.Error!.Message}");
            var doomed = RetentionPlanner.SelectWalForDeletion(
                listed.Value.Select(o => o.Key.Split('/')[^1]).ToList(), cutoff);
            if (doomed.Count > 0)
            {
                var keys = doomed.Select(n => $"{cluster}/{shard}/wal/{n}").ToList();
                var deleted = await s3.DeleteKeysAsync(keys, ct);
                if (!deleted.IsSuccess)
                    throw new ApplicationException($"delete wal: {deleted.Error!.Message}");
                await journal.WritePhaseAsync(cluster, Op, $"wal-trimmed/{shard}/{doomed.Count}",
                    claims.InstanceId, null, ct);
            }
        }

        // (5) Гигиена FAILED: держать последние Retention:KeepFailed.
        var prune = RetentionPlanner.SelectFailedForPrune(fulls, options.RetentionKeepFailed);
        foreach (var id in prune)
        {
            var deleted = await DeleteAsync(BackupNames.FullKey(cluster, shard, id), ct);
            if (!deleted.IsSuccess)
                throw new ApplicationException($"del FAILED {id}: {deleted.Error!.Message}");
        }

        if (prune.Count > 0)
            await journal.WritePhaseAsync(cluster, Op, $"failed-pruned/{shard}/{prune.Count}",
                claims.InstanceId, null, ct);
    }
```

`FinishDeletionAsync` + монитор + failover-обёртки:

```csharp
    // Доводка DELETING (идемпотентна): list префикса → есть объекты →
    // batch-delete → list пуст → del etcd-ключа. transient-отказ S3 —
    // исключение наверх (пер-шардовый catch): статус остаётся DELETING,
    // следующий проход повторит (spec §3.3 п.1, AC3).
    private async Task FinishDeletionAsync(
        string cluster, string shard, FullBackupState deleting, CancellationToken ct)
    {
        var prefix = $"{cluster}/{shard}/full/{deleting.Id}/";
        var listed = await s3.ListPrefixAsync(prefix, ct: ct);
        if (!listed.IsSuccess)
            throw new ApplicationException($"list {prefix}: {listed.Error!.Message}");
        if (listed.Value.Count > 0)
        {
            var keys = listed.Value.Select(o => o.Key).ToList();
            var deleted = await s3.DeleteKeysAsync(keys, ct);
            if (!deleted.IsSuccess)
                throw new ApplicationException($"delete {prefix}: {deleted.Error!.Message}");
            var recheck = await s3.ListPrefixAsync(prefix, ct: ct);
            if (!recheck.IsSuccess || recheck.Value.Count > 0)
                throw new ApplicationException($"удаление {prefix} не завершилось — повторит следующий проход");
        }

        var del = await DeleteAsync(BackupNames.FullKey(cluster, shard, deleting.Id), ct);
        if (!del.IsSuccess)
            throw new ApplicationException($"del ключа {deleting.Id}: {del.Error!.Message}");
        await journal.WritePhaseAsync(cluster, Op, $"deleted-full/{shard}/{deleting.Id}",
            claims.InstanceId, null, ct);
    }

    // Монитор занятости (spec §3.4): list ВЕСЬ bucket (вкл. чужие/осиротевшие
    // префиксы) → used → EvaluateStorage → ключ при изменении (строковое
    // сравнение — образец WalStatusWriter.WriteIfChangedAsync). Отдельное
    // поле-расписание, не словарь кластеров (коллизия "storage" — см. поле);
    // параллельные тики разных кластеров могут гоняться за поле — двойной list
    // безвреден (put при изменении идемпотентен; тики одного кластера
    // последовательны — ReconcileLoop).
    private async Task<Result> MonitorStorageAsync(long nowUnix, CancellationToken ct)
    {
        if (nowUnix - _lastStoragePassUnix < options.RetentionIntervalSec)
            return Result.Success();
        _lastStoragePassUnix = nowUnix;

        var listed = await s3.ListPrefixAsync("", ct: ct);
        if (!listed.IsSuccess)
            return Result.Failed(listed.Error!); // transient: повтор тика

        var used = listed.Value.Sum(o => o.SizeBytes);
        var verdict = RetentionPlanner.EvaluateStorage(
            used, options.QuotaBytes, options.QuotaWarnPercent, options.QuotaCritPercent);
        var payload = StorageStatusJson.Serialize(new StorageStatus(
            verdict.UsedBytes, verdict.QuotaBytes, verdict.UsedPercent, verdict.State, nowUnix));

        foreach (var endpoint in endpoints)
        {
            var current = await etcd.GetAsync(endpoint, "/pgworker/backups/storage", ct);
            if (!current.IsSuccess)
                continue; // failover
            if (current.Value is { } kv && kv.Value == payload)
                return Result.Success(); // без изменений — не пишем
            var put = await etcd.PutAsync(endpoint, "/pgworker/backups/storage", payload, null, ct);
            if (put.IsSuccess)
                return Result.Success();
        }

        return Result.Failed(new ApplicationException("запись /pgworker/backups/storage не удалась"));
    }

    // Failover-обёртки (образец BackupProcess.PutAsync / WalStreamProcess.GetAsync).
    private async Task<Result> PutAsync(string key, string value, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.PutAsync(endpoint, key, value, null, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

    private async Task<Result> DeleteAsync(string key, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.DeleteAsync(endpoint, key, prefix: false, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }
```

**Выход:** процесс ретенции полностью реализован (доводка/отбор/удаление/WAL/гигиена/монитор).

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter FullyQualifiedName~RetentionProcessTests` — зелёные. После серии: зачистка docker (см. Task 3).

**Spec:** §3.3, §3.4, §4 Ф3, AC2/AC3/AC4 (интеграционная часть, включая поднятие `chain_start`)/AC5/AC6 (воркер-часть)/AC8.

- [ ] **Шаг 5.1:** создать `RetentionProcess.cs`; `dotnet build src/PgWorker.slnx -c Release`.
- [ ] **Шаг 5.2:** интеграционные тесты `RetentionProcessTests` — реальный etcd (`EtcdFixture`, коллекция `EtcdCollection`) + `FakeBackupS3` (Task 3). SQL у ретенции нет, `FakeWalSqlExecutor` нужен только кейсу контроля t03 (см. ниже). Образец обвязки — `WalStreamProcessTests` (`SeedAsync` с чисткой префикса `/pgworker/backups/<C>/` и claims, `BuildProcess`, тест-опции `BackupsRuntimeOptions(Enabled: true, …, RetentionIntervalSec: 0, …)` — интервал 0 в runtime-опциях допустим: валидация старта в App-options на него не влияет, тесты строят runtime напрямую). Обязательные сценарии (все AAA):
  - `Доводка_DELETING_при_сбое_S3`: сид — ключ COMPLETED с просроченной датой + объекты `full/<id>/`; первый тик с `s3.FailNextDelete = true` → статус стал DELETING, объекты/ключ живы (сбой S3 — transient, статус не трогаем повторно); второй тик без сбоя → объекты удалены (`DeletedKeys`), etcd-ключ удалён (AC3).
  - `Policy_из_ключа_кластера_действует`: `ClusterBackups` с `Policy = new BackupPolicy(1, 0, 0, 86400, false)` и двумя COMPLETED (сегодня/40 дней назад) → тик → старый удалён, свежий жив (per-cluster политика замещает дефолт конфига — хвост AC7 «применённая политика действует»).
  - `Один_кандидат_за_проход`: 2 просроченных COMPLETED → после одного тика удалён ровно один (старейший), второй ключ жив (AC3).
  - `Guard_единственный_COMPLETED_не_удаляется`: один просроченный COMPLETED + его S3-объекты → тик → ключ жив, `DeletedKeys` пуст (AC2).
  - `WAL_чистка_ниже_cutoff`: COMPLETED с `wal_start_segment = 000000010000000000000005`; S3: сегменты 3,4 (ниже), 5 (cutoff), 6 (выше), `00000001.history` (ниже TLI нет — cutoff tli=1 → жив); тик (просроченных полных нет) → удалены 3,4; 5, 6, history живы (AC4).
  - `Чистка_WAL_поднимает_chain_start_контроля_t03` (ревью Ф4 №2, AC4-хвост): сид — COMPLETED-полный с `wal_start_segment = 000000010000000000000005`; wal-ключ шарда с `chain_start_segment = 000000010000000000000003` (ниже cutoff; писать через `new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]).WriteIfChangedAsync(cluster, shard, new WalStreamState(Active, slot, masterNode, chainStart: "…0003", lastReceived: "…0007", lastUploaded: "…0007", lastUploadedUnix: <свежий>, lag: 0, error: null), ct)`); общий `FakeBackupS3` засеян wal-объектами (сегменты 3–7, через `Objects` — контроль t03 читает их же через `ListWalAsync`, ретенция — через `ListPrefixAsync`). Act: тик `RetentionProcess` (сегменты 3,4 удалены из фейка) → тик `WalStreamProcess` (по образцу `WalStreamProcessTests.BuildProcess`: тот же `FakeBackupS3` + `FakeWalSqlExecutor` + `StubScaleDriver`, опции `WalVerifyIntervalSec: 0`; стаб-драйвер у соседнего тест-класса общий для сборки — если приватен, скопировать). Assert: ключ wal — `chain_start_segment == 000000010000000000000005` (поднялся до cutoff — контроль пересчитал от оставшихся), `state == ACTIVE`, error пуст (непрерывная цепочка от нового старта, без chain-broken).
  - `WAL_без_полных_не_чистится`: только wal-объекты, полных нет → тик → ничего не удалено.
  - `FAILED_гигиена`: 7 FAILED + `RetentionKeepFailed: 5` → удалены 2 старейших ключа, 5 свежих живы; `BackupPlanner.BackoffPassed` на оставшейся истории возвращает тот же вердикт, что на полной (AC5, интеграционная перекличка юниту Task 2).
  - `Enabled_false_no_op`: `Enabled: false`, живые ключи/объекты → тик → ничего не удалено, ключ storage не писался (AC8).
  - `Клэйм_не_наш_отказ`: без `TryClaimClusterAsync` → `TickAsync` возвращает Failed (гвард).
  - `Ключ_storage_пишется_при_изменении`: `PrefixObjects` с 2 объектами (размеры 10+20) + `QuotaBytes: 100, WarnPercent: 80, CritPercent: 90` → тик → ключ `/pgworker/backups/storage` содержит `"used_bytes":30`, `"quota_bytes":100`, `"used_percent":30`, `"state":"OK"`; повторный тик (без изменений) не меняет значение (сравнить сырую строку до/после — идемпотентность put).
  - `Квота_0_без_полей_квоты`: `QuotaBytes: 0` → ключ содержит `used_bytes`, НЕ содержит `quota_bytes`, `state:"OK"` (AC6).
  - `Не_Active_кластер`: snap с `ClusterState.TO_REMOVE` → no-op.
- [ ] **Шаг 5.3:** прогон, зачистка, коммит `feat(backups): RetentionProcess — DELETING-доводка, GFS-удаление, WAL-чистка, FAILED-гигиена, монитор storage (t06 Ф3)`.

---

### Task 6: Ф3 — wiring: `IClusterProcesses.RetentionAsync` + `ReconcileLoop` + DI

**Files:**
- Modify: `src/PgWorker.App/Loops/ClusterProcesses.cs` (интерфейс + реализация)
- Modify: `src/PgWorker.App/Loops/ReconcileLoop.cs` (Active-ветка)
- Modify: `src/PgWorker.App/Program.cs` (DI `RetentionProcess`)
- Modify: `src/tests/PgWorker.UnitTests/App/ReconcileLoopTests.cs` (расширить `FakeProcesses`)

**Interfaces:**
- Consumes: `RetentionProcess.TickAsync` (Task 5).
- Produces:
  ```csharp
  // IClusterProcesses — добавить:
  /// <summary>Ретенция бэкапов (t06, arch/19 §4): после backup-wal, до repair;
  /// guard Enabled — выключенная подсистема no-op.</summary>
  Task<Result<ProcessOutcome>> RetentionAsync(
      ClusterSnapshot snap, IReadOnlyList<ClusterBackups> backups, CancellationToken ct);
  ```

**Вход:** Task 5 закоммичен.

**Действие:**
1. `ClusterProcesses`: метод в интерфейсе + реализация:
   ```csharp
   public Task<Result<ProcessOutcome>> RetentionAsync(
       ClusterSnapshot snap, IReadOnlyList<ClusterBackups> backups, CancellationToken ct)
       => retention.TickAsync(snap, backups.FirstOrDefault(b => b.Cluster == snap.Config.Cluster), ct);
   ```
   (в primary-конструктор `ClusterProcesses` добавить параметр `PgWorker.Backups.RetentionProcess retention`).
2. `ReconcileLoop.ProcessClusterAsync` — между `backup-wal` и `repair` (spec §3.3: ПОСЛЕ backup-wal, до repair):
   ```csharp
   // Ретенция (t06, arch/19 §4): после backup-wal (чистит только то, что не
   // нужно оставляемым полным), до repair; тик короткий (один кандидат/проход,
   // batch-чанки). Выключенная подсистема не зовётся вовсе (AC8).
   if (options.CurrentValue.Backups.Enabled)
       await RunClusterOpAsync(cluster, "backups-retention",
           () => processes.RetentionAsync(snap, backups, ct), ct);
   ```
3. `Program.cs` — DI рядом с WalStreamProcess:
   ```csharp
   // Ретенция (t06, arch/19 §4): GFS/WAL-чистка/гигиена + монитор хранилища;
   // Enabled=false — no-op (гвард в процессе дублирует guard цикла).
   builder.Services.AddSingleton(sp => new PgWorker.Backups.RetentionProcess(
       sp.GetRequiredService<IEtcdGateway>(),
       sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
       sp.GetRequiredService<IBackupS3>(),
       sp.GetRequiredService<ClaimStore>(),
       sp.GetRequiredService<WorkJournal>(),
       sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Backups.ToRuntime(),
       sp.GetRequiredService<TimeProvider>(),
       sp.GetRequiredService<ILoggerFactory>().CreateLogger<PgWorker.Backups.RetentionProcess>()));
   ```
4. `ReconcileLoopTests.FakeProcesses` — добавить реализацию `RetentionAsync` (no-op `ProcessOutcome.Done`), иначе не скомпилируется; добавить один юнит-кейс: при `Backups.Enabled=false` `RetentionAsync` не вызывается / при true — вызывается после WalStream (по счётчику фейка, образец существующих кейсов порядка операций).

**Выход:** ретенция встроена в цикл воркера под guard Enabled.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --filter "FullyQualifiedName~ReconcileLoopTests"` — зелёные; `dotnet build src/PgWorker.slnx -c Release`.

**Spec:** §3.3 (вызов из ReconcileLoop), §4 Ф3 (wiring), AC8.

- [ ] **Шаг 6.1:** правки 4 файлов; сборка.
- [ ] **Шаг 6.2:** юниты ReconcileLoop (Enabled-гвард + порядок); прогон.
- [ ] **Шаг 6.3:** коммит `feat(backups): wiring ретенции в ReconcileLoop после backup-wal (t06 Ф3)`.

---

### Task 7: Ф4 — API `POST /api/clusters/{cluster}/backups/policy`

**Files:**
- Create: `src/PgWorker.App/Api/Operations/BackupsPolicyHandler.cs`
- Modify: `src/PgWorker.App/Api/ApiModule.cs` (маршрут)
- Modify: `src/PgWorker.App/Api/Operations/WorkerApiExceptions.cs` (исключение валидации)
- Modify: `src/PgWorker.App/Program.cs` (DI)
- Test: `src/tests/PgWorker.IntegrationTests/Api/BackupsPolicyApiTests.cs`

**Interfaces:**
- Consumes: образец `RotateClusterSecretsHandler` (кластер-гвард по `/clusters/<C>/config`, `EtcdFailover.CallAsync`, regex имени кластера), `ClusterNotFoundException`/`EtcdWriteUnavailableException`, `PgWorker.Core.Writing.ValidationError` (field/message — как в `CreateClusterValidationException`).
- Produces:
  ```csharp
  // WorkerApiExceptions.cs — добавить:
  // Валидация policy не прошла: 400 с errors по полям (t06, arch/19 §4).
  public sealed class BackupsPolicyValidationException(
      IReadOnlyList<PgWorker.Core.Writing.ValidationError> errors)
      : Exception("политика бэкапов некорректна")
  {
      public IReadOnlyList<PgWorker.Core.Writing.ValidationError> Errors { get; } = errors;
  }
  ```
  Хендлер (полный код):

```csharp
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using PgWorker.Core;
using PgWorker.Core.Writing;
using PgWorker.Etcd.Client;

namespace PgWorker.App.Api.Operations;

// Приём per-cluster политики бэкапов (t06, arch/19 §4): тело канона
// {"retention":{"days":..,"weeks":..,"months":..},"full_max_age_sec":..,
// "verify":{"on_create":..}}; отсутствующие retention-поля → дефолты, тело
// ЦЕЛИКОМ замещает политику (put полного значения). Применение — следующим
// тиком планировщика t02/ретенции (etcd-снапшот, нотификации нет).
public sealed partial class BackupsPolicyHandler(IEtcdGateway gateway, string[] endpoints)
{
    [GeneratedRegex("^[a-z][a-z0-9_]{0,62}$")]
    private static partial Regex ClusterPattern();

    private sealed record PolicyBody(
        [property: JsonPropertyName("retention")] RetentionBody? Retention,
        [property: JsonPropertyName("full_max_age_sec")] long? FullMaxAgeSec,
        [property: JsonPropertyName("verify")] VerifyBody? Verify);

    private sealed record RetentionBody(
        [property: JsonPropertyName("days")] int? Days,
        [property: JsonPropertyName("weeks")] int? Weeks,
        [property: JsonPropertyName("months")] int? Months);

    private sealed record VerifyBody(
        [property: JsonPropertyName("on_create")] bool? OnCreate);

    public async Task<Result<string>> HandleAsync(string cluster, string rawBody, CancellationToken ct)
    {
        // 1) Каноническое имя кластера.
        if (!ClusterPattern().IsMatch(cluster))
            return Result<string>.Failed(new ClusterNotFoundException(cluster));

        // 2) Гвард кластера: /clusters/<C>/config отсутствует → 404 (spec §3.5).
        var config = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.RangeAsync(endpoint, $"/clusters/{cluster}/config", ct), ct);
        if (!config.IsSuccess)
            return Result<string>.Failed(new EtcdWriteUnavailableException());
        if (config.Value.All(kv => kv.Key != $"/clusters/{cluster}/config"))
            return Result<string>.Failed(new ClusterNotFoundException(cluster));

        // 3) Разбор тела: мусорный JSON → 400 (валидация); отсутствующие поля → дефолты.
        PolicyBody? body;
        try
        {
            body = JsonSerializer.Deserialize<PolicyBody>(rawBody,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = false });
        }
        catch (JsonException)
        {
            return Result<string>.Failed(new BackupsPolicyValidationException(
                [new ValidationError("body", "тело запроса — JSON вида {\"retention\":{...},\"full_max_age_sec\":..,\"verify\":{\"on_create\":..}}")]));
        }

        if (body is null)
            return Result<string>.Failed(new BackupsPolicyValidationException(
                [new ValidationError("body", "тело запроса обязательно")]));

        // 4) Валидация диапазонов (400 с перечнем): days [1..365], weeks [0..52],
        //    months [0..120], full_max_age_sec >= 600, on_create — bool из JSON-схемы.
        var errors = new List<ValidationError>();
        var days = body.Retention?.Days ?? 7;
        var weeks = body.Retention?.Weeks ?? 4;
        var months = body.Retention?.Months ?? 6;
        var maxAge = body.FullMaxAgeSec ?? 86400;
        var onCreate = body.Verify?.OnCreate ?? true;
        if (days is < 1 or > 365)
            errors.Add(new("retention.days", "дневная гранула — целое в [1..365]"));
        if (weeks is < 0 or > 52)
            errors.Add(new("retention.weeks", "недельная гранула — целое в [0..52]"));
        if (months is < 0 or > 120)
            errors.Add(new("retention.months", "месячная гранула — целое в [0..120]"));
        if (maxAge < 600)
            errors.Add(new("full_max_age_sec", "минимум 600 c"));
        if (errors.Count > 0)
            return Result<string>.Failed(new BackupsPolicyValidationException(errors));

        // 5) Put полного значения формата канона (тело замещает политику целиком).
        var payload = JsonSerializer.Serialize(new Dictionary<string, object>
        {
            ["retention"] = new Dictionary<string, object> { ["days"] = days, ["weeks"] = weeks, ["months"] = months },
            ["full_max_age_sec"] = maxAge,
            ["verify"] = new Dictionary<string, object> { ["on_create"] = onCreate },
        });
        var put = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.PutAsync(endpoint, $"/pgworker/backups/{cluster}/policy", payload, null, ct), ct);
        if (!put.IsSuccess)
            return Result<string>.Failed(new EtcdWriteUnavailableException());

        return Result<string>.Success(payload);
    }
}
```

   (если сигнатура `ValidationError` отличается — посмотреть её в `PgWorker.Core.Writing` и подставить фактическую; это конструктор `(string Field, string Message)`.)

Маршрут в `ApiModule.MapWorkerApi` (после secrets/rotate):

```csharp
        // POST /api/clusters/{cluster}/backups/policy — per-cluster политика
        // ретенции/бэкапов (t06, arch/19 §4): валидация + put policy-ключа;
        // применяется следующим тиком планировщика/ретенции. Мусорный JSON — 400.
        endpoints.MapPost("/api/clusters/{cluster}/backups/policy", async (
            string cluster, HttpRequest http, BackupsPolicyHandler handler, CancellationToken ct) =>
        {
            string rawBody;
            using (var reader = new StreamReader(http.Body))
                rawBody = await reader.ReadToEndAsync(ct);
            var result = await handler.HandleAsync(cluster, rawBody, ct);
            if (result.IsSuccess)
                return Results.Ok(result.Value);

            return result.Error switch
            {
                BackupsPolicyValidationException validation => Results.Problem(
                    statusCode: StatusCodes.Status400BadRequest,
                    title: "Validation failed",
                    detail: result.Error.Message,
                    extensions: new Dictionary<string, object?>
                    {
                        ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }),
                    }),
                ClusterNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Cluster not found",
                    detail: result.Error.Message),
                EtcdWriteUnavailableException => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable",
                    detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                    detail: result.Error!.Message),
            };
        });
```

DI в `Program.cs` (по образцу соседних):
```csharp
builder.Services.AddSingleton(sp => new BackupsPolicyHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints));
```

**Выход:** API принимает политику, кладёт policy-ключ формата канона.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter FullyQualifiedName~BackupsPolicyApiTests` — зелёные.

**Spec:** §3.5, §4 Ф4, AC7 (валидационная часть).

- [ ] **Шаг 7.1:** хендлер + исключение + маршрут + DI; сборка. Проверить фактическую сигнатуру `ValidationError` (`grep -rn "record ValidationError" src/PgWorker.Core/Writing/`) и подставить при расхождении.
- [ ] **Шаг 7.2:** API-интеграционные тесты `BackupsPolicyApiTests` (коллекция `PgApiCollection`, сид `ApiTestSeed.SeedActiveClusterAsync`, образец `RecreateRotateApiTests`; AAA):
  - `Валидное_тело_200_и_ключ_в_etcd`: `{"retention":{"days":3,"weeks":2,"months":1},"full_max_age_sec":7200,"verify":{"on_create":false}}` → 200; ключ `/pgworker/backups/<C>/policy` равен телу канона; парсер t01 (`BackupsParser.Parse` воркера) читает его без parseErrors с retention 3/2/1 (AC7).
  - `Опущенные_поля_дефолты`: `{}` → 200, ключ содержит days=7/weeks=4/months=6/full_max_age_sec=86400/on_create=true.
  - `Невалидные_диапазоны_400_с_перечнем`: days=400, weeks=99, full_max_age_sec=1 → 400, errors содержит все три поля.
  - `Мусорный_JSON_400`: тело `not-json` → 400.
  - `Несуществующий_кластер_404`: имя без config-ключа → 404.
- [ ] **Шаг 7.3:** прогон, зачистка docker, коммит `feat(api): POST /api/clusters/{C}/backups/policy — приём per-cluster политики бэкапов (t06 Ф4)`.

---

### Task 8: Ф5 — панель: парсер storage + DELETING-полные + снапшот

**Files:**
- Modify: `src/AdminPanel.Core/BackupInfo.cs` (`DeletingFullInfo`, расширение `ClusterBackupsInfo`)
- Modify: `src/AdminPanel.Core/BackupsInfo.cs` (`BackupStorageState`, `BackupStorageInfo`)
- Modify: `src/AdminPanel.Core/EtcdSnapshot.cs` (поле `BackupStorage`)
- Modify: `src/AdminPanel.Etcd/Parsing/BackupsParser.cs` (storage-ключ + DELETING-полные)
- Modify: `src/AdminPanel.Etcd/SnapshotBuilder.cs` (прокинуть `backups.Storage`)
- Modify: `src/AdminPanel.Etcd/SnapshotRefresher.cs` (`FailTick`: `previous?.BackupStorage`)
- Test: `src/tests/AdminPanel.UnitTests/BackupsParserTests.cs` (добавить)

**Interfaces:**
- Consumes: панельный `BackupsParser`/`BackupsParseResult`/`ClusterBackupsInfo`, `EtcdSnapshot`, `KeyParseError`.
- Produces (для Task 9):
  ```csharp
  // src/AdminPanel.Core/BackupsInfo.cs
  public enum BackupStorageState { Ok, Warn, Crit }

  /// <summary>Занятость bucket бэкапов из глобального ключа
  /// /pgworker/backups/storage (t06; дубль воркерной модели — осознанный,
  /// унификация t08-unify-adminpanel-duplicates).</summary>
  public sealed record BackupStorageInfo(
      long UsedBytes, long? QuotaBytes, double? UsedPercent,
      BackupStorageState State, long UpdatedUnix);

  // src/AdminPanel.Core/BackupInfo.cs
  /// <summary>DELETING-полный (t06): возраст для backup-deleting-stuck.</summary>
  public sealed record DeletingFullInfo(string Id, long StartedUnix, long? FinishedUnix);

  // ClusterBackupsInfo — добавить именованный параметр в конец:
  public sealed record ClusterBackupsInfo(
      string Cluster,
      long? FullMaxAgeSec,
      IReadOnlyDictionary<string, long?> ShardLastCompletedUnix,
      IReadOnlyDictionary<string, WalStreamInfo?>? Shards = null,
      // t06: DELETING-полные per-shard (застарелые → алерт backup-deleting-stuck).
      IReadOnlyDictionary<string, IReadOnlyList<DeletingFullInfo>>? DeletingFulls = null);

  // EtcdSnapshot — добавить поле в конец (default null — существующие вызовы не ломаются):
  public sealed record EtcdSnapshot(
      …, int UnknownKeyCount,
      BackupStorageInfo? BackupStorage = null);
  ```

**Вход:** Task 7 закоммичен.

**Действие:**
1. Модели выше.
2. `BackupsParser` (панельный, `src/AdminPanel.Etcd/Parsing/BackupsParser.cs`):
   - ветка storage-ключа ставится в начале цикла ПЕРЕД существующим гвардом `if (segments.Length < 5 || …) continue;` — иначе 4-сегментный ключ `/pgworker/backups/storage` (`["", "pgworker", "backups", "storage"]`) до ветки не дойдёт (замечание ревью Ф4 №3). Код:

   ```csharp
   foreach (var kv in kvs)
   {
       var segments = kv.Key.Split('/');

       // Глобальный ключ /pgworker/backups/storage (t06): ДО гварда длины —
       // у него 4 сегмента, гвард "< 5 → continue" его не пропускает.
       if (segments.Length == 4 && segments[3] == "storage")
       {
           try
           {
               using var doc = JsonDocument.Parse(kv.Value);
               var root = doc.RootElement;
               var used = Long(root, "used_bytes");
               var updated = Long(root, "updated_unix");
               var state = String(root, "state") switch
               {
                   "OK" => BackupStorageState.Ok,
                   "WARN" => BackupStorageState.Warn,
                   "CRIT" => BackupStorageState.Crit,
                   _ => (BackupStorageState?)null,
               };
               if (used is null || updated is null || state is null)
                   errors.Add(new(kv.Key, "битый storage-статус (used_bytes/updated_unix/state)"));
               else
                   storage = new BackupStorageInfo(
                       used.Value, Long(root, "quota_bytes"), Double(root, "used_percent"),
                       state.Value, updated.Value);
           }
           catch (JsonException e)
           {
               errors.Add(new(kv.Key, $"битый JSON storage: {e.Message}"));
           }

           continue;
       }

       if (segments.Length < 5 || segments[1] != "pgworker" || segments[2] != "backups")
           continue; // чужой префикс
       … // существующая логика
   }
   ```
   (`storage` — локальная `BackupStorageInfo? storage = null;`; `Double` — маленький хелпер по образцу `Long` для `used_percent`; результат попадает в новое поле `BackupsParseResult.Storage`, добавляемое в конец record со `= null`.)
   - full-ветка: при `state=="DELETING"` собрать `DeletingFullInfo(id, started_unix, finished_unix)` в per-cluster словарь `deleting[cluster][shard]`. `started_unix` обязателен по НОВОМУ правилу ретенции: текущая full-ветка панельного парсера его НЕ проверяет (читает только `state` и `finished_unix` для COMPLETED) — отсутствующий/битый `started_unix` у DELETING-записи → KeyParseError + пропуск записи; при сборке `ClusterBackupsInfo` прокинуть `DeletingFulls` (пустой словарь, если нет).
3. `SnapshotBuilder.Build`: последний аргумент → `BackupStorage: backups.Storage` (добавить в конец позиционного `new EtcdSnapshot(...)`).
4. `SnapshotRefresher.FailTick`: последний аргумент → `previous?.BackupStorage` (переживает отказный тик, как Backups).

**Выход:** панельный снапшот несёт `BackupStorage` и DELETING-полные; битые значения толерантно пропускаются.

**Проверка:** `dotnet test src/tests/AdminPanel.UnitTests -c Release` — все зелёные (включая старые: `BackupsParserTests`, `AlertEngineTests`, `TestSnapshots`-потребители).

**Spec:** §3.6 (парсер), §4 Ф5.

- [ ] **Шаг 8.1:** модели + парсер + снапшот (4 файла); `dotnet build src/PgWorker.slnx -c Release` (панель в том же solution).
- [ ] **Шаг 8.2:** юниты `BackupsParserTests` (AAA): валидный storage-ключ → Storage с полями и state WARN; без квоты → QuotaBytes/UsedPercent null; битый JSON storage → KeyParseError, Storage null; `state:"BROKEN"` → KeyParseError; DELETING-полный попадает в `DeletingFulls[shard]` с id/started/finished(null-толерантно); DELETING без `started_unix` → KeyParseError + пропуск (новое правило ветки); storage-ключ не создаёт псевдо-кластер в `Clusters` (ассерт: 4-сегментный ключ парсится ВЕТКОЙ ДО гварда — ревью Ф4 №3).
- [ ] **Шаг 8.3:** прогон AdminPanel.UnitTests; коммит `feat(panel): parse ключа /pgworker/backups/storage + DELETING-полные в снапшот (t06 Ф5)`.

---

### Task 9: Ф5 — панельные правила `backup-storage-quota` + `backup-deleting-stuck`

**Files:**
- Create: `src/AdminPanel.Core/Alerting/Rules/BackupStorageQuotaRule.cs`
- Create: `src/AdminPanel.Core/Alerting/Rules/BackupDeletingStuckRule.cs`
- Modify: `src/AdminPanel.Core/Alerting/AlertsOptions.cs` (вложенные `Backups`)
- Test: `src/tests/AdminPanel.UnitTests/BackupStorageQuotaRuleTests.cs`
- Test: `src/tests/AdminPanel.UnitTests/BackupDeletingStuckRuleTests.cs`

**Interfaces:**
- Consumes: `BackupStorageInfo`/`BackupStorageState`/`DeletingFullInfo` (Task 8), `IAlertRule`/`Alert`/`AlertContext`/`EtcdSnapshot`, `TestSnapshots.Healthy`, атрибут `[InjectAsSingleton(typeof(IAlertRule))]` (автоскан — ручной регистрации в AlertEngine нет).
- Produces: два правила kind=`backup-storage-quota`, `backup-deleting-stuck`.

**Вход:** Task 8 закоммичен.

**Действие:**
1. `AlertsOptions` — добавить:
   ```csharp
   // backup-deleting-stuck (t06): возраст DELETING-полного без завершения
   // (ретенция не может довести удаление — S3-отказ и т.п.).
   public BackupsAlertsOptions Backups { get; set; } = new();

   /// <summary>Пороги алертов бэкапов (t06, arch/19 §4).</summary>
   public sealed class BackupsAlertsOptions
   {
       public int DeletingStaleSec { get; set; } = 21600; // 6 ч
   }
   ```
2. Правила (полный код; порядок хвостовых аргументов Alert — сигнатура `(…, SinceUnix, Hint, Remedy, RemedyText)`: Hint — что не так/инвариант, RemedyText — конкретное действие оператора; замечание ревью Ф4 №4):

```csharp
using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Core.Alerting.Rules;

// backup-storage-quota (t06, arch/19 §4): занятость bucket по state глобального
// ключа /pgworker/backups/storage; ключа нет → молчим (подсистема не включена).
// Реакции автоматикой нет — действует оператор (расширить квоту/ужать ретенцию).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class BackupStorageQuotaRule : IAlertRule
{
    public const string KindName = "backup-storage-quota";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        var storage = snapshot.BackupStorage;
        if (storage is null)
            yield break; // ключа нет — подсистема не включена

        if (storage.State is not (BackupStorageState.Warn or BackupStorageState.Crit))
            yield break; // OK — молчим

        var severity = storage.State == BackupStorageState.Crit
            ? AlertSeverity.Critical
            : AlertSeverity.Warning;
        var percent = storage.UsedPercent is { } p ? $"{p:0.##}%" : "н/д";
        var quota = storage.QuotaBytes is { } q ? $"{q} б" : "не задана";
        yield return new Alert(
            $"{KindName}:storage",
            severity,
            KindName,
            "backups-storage",
            $"хранилище бэкапов: занято {storage.UsedBytes} б из квоты {quota} ({percent}) — состояние {storage.State}",
            new Dictionary<string, string>
            {
                ["usedBytes"] = storage.UsedBytes.ToString(),
                ["quotaBytes"] = storage.QuotaBytes?.ToString() ?? string.Empty,
                ["usedPercent"] = storage.UsedPercent?.ToString() ?? string.Empty,
                ["state"] = storage.State.ToString(),
            },
            null, // SinceUnix — проставляет AlertEngine
            "ключ пишет ретенционный проход PgWorker по расписанию Retention:IntervalSec (list всего bucket); занятость — факт, реакции автоматикой нет",
            AlertRemedy.OperatorRunbook,
            "расширь квоту (PgWorker:Backups:Quota:Bytes) или ужать политику ретенции кластера (POST /api/clusters/<C>/backups/policy)");
    }
}
```

```csharp
using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Core.Alerting.Rules;

// backup-deleting-stuck (warning, t06, arch/19 §4): полный в DELETING дольше
// Alerts:Backups:DeletingStaleSec (возраст по finished_unix, иначе started_unix)
// — ретенция не может довести удаление (S3-отказ и т.п.).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class BackupDeletingStuckRule(IOptions<AlertsOptions> options) : IAlertRule
{
    public const string KindName = "backup-deleting-stuck";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        var nowUnix = context.NowUtc.ToUnixTimeSeconds();
        var staleSec = options.Value.Backups.DeletingStaleSec > 0
            ? options.Value.Backups.DeletingStaleSec
            : 21600;
        foreach (var backups in snapshot.Backups)
        foreach (var (shard, fulls) in backups.DeletingFulls ?? new Dictionary<string, IReadOnlyList<DeletingFullInfo>>())
        {
            var stuck = fulls
                .Select(f => (Full: f, Age: nowUnix - (f.FinishedUnix ?? f.StartedUnix)))
                .Where(p => p.Age > staleSec)
                .OrderBy(p => p.Age)
                .FirstOrDefault();
            if (stuck.Full is null)
                continue;

            yield return new Alert(
                $"{KindName}:{backups.Cluster}/{shard}",
                AlertSeverity.Warning,
                KindName,
                $"{backups.Cluster}/{shard}",
                $"полный бэкап {stuck.Full.Id} шарда {shard} кластера {backups.Cluster} висит в DELETING {stuck.Age} c (порог {staleSec} c) — ретенция не может довести удаление",
                new Dictionary<string, string>
                {
                    ["id"] = stuck.Full.Id,
                    ["ageSeconds"] = stuck.Age.ToString(),
                    ["staleSeconds"] = staleSec.ToString(),
                },
                null, // SinceUnix — проставляет AlertEngine
                "удаление идемпотентно: каждый ретенционный проход повторяет list+delete префикса full/<id>/ — затор означает transient-отказ S3",
                AlertRemedy.WorkerAuto,
                "проверь доступность S3 (endpoint/креды/сеть) и журналы /pgworker/work/<C> (phase=backups-retention) — повторные проходы доведут удаление сами");
        }
    }
}
```

**Выход:** оба правила в автоскане AlertEngine; конфиг порога `Alerts:Backups:DeletingStaleSec`.

**Проверка:** `dotnet test src/tests/AdminPanel.UnitTests -c Release` — зелёные (включая `AlertHintRemedyTests` — Hint/RemedyText непустые и по назначению, и `AutoRegistrationTests`).

**Spec:** §3.6 (правила), §4 Ф5, AC6 (панельная часть).

- [ ] **Шаг 9.1:** правила + опции; сборка.
- [ ] **Шаг 9.2:** юниты `BackupStorageQuotaRuleTests` (по образцу `BackupFullStaleRuleTests`, `TestSnapshots.Healthy(Now) with { BackupStorage = … }`; AAA): нет ключа → пусто; OK → пусто; WARN → warning с used/percent в Message; CRIT → critical; в WARN-кейсе сверить `Hint` содержит «ретенционный проход», `RemedyText` содержит «квоту» (порядок аргументов — ревью Ф4 №4).
- [ ] **Шаг 9.3:** юниты `BackupDeletingStuckRuleTests`: DELETING свежий (finished час назад, порог дефолт) → пусто; DELETING застарелый по finished_unix (7 ч) → warning с id; finished_unix нет, started_unix 7 ч назад → warning (fallback возраста); несколько DELETING — алерт один, старейший id; нет DELETING → пусто.
- [ ] **Шаг 9.4:** прогон AdminPanel.UnitTests; коммит `feat(panel): правила backup-storage-quota и backup-deleting-stuck (t06 Ф5)`.

---

### Task 10: Ф6 — E2E `Retention_TrimsToGfs` + мерж-гейт-маркер

**Files:**
- Create: `src/tests/PgWorker.IntegrationTests/E2e/E2eRetentionScenarios.cs`

**Interfaces:**
- Consumes: `E2eEnvironment.StartAsync(slug, withMinio: true)` (своя сеть/etcd/MinIO, динамические порты, полный teardown с ассертом чистоты в `DisposeAsync`), `E2eFixture.WaitForAsync`, `Fx.RunDockerAsync` (mc-контейнер для посева объектов), `PgWorker.Backups.BackupS3` (host-клиент list/delete — как в `WalStream_UploadsSegmentsContinuously`), `BackupNames.FullKey`, `BackupStatusJson`, сид-хелперы `SeedClusterAsync`/`StartBackupHostAsync` из `E2eBackupScenarios` (скопировать локально — файлы сценариев независимы; паттерн репо — копия хелпера в файле сценария, прецедент `E2eRotateScenarios.SeedClusterAsync`).

**Вход:** Task 6–9 закоммичены; `docker rm -f $(docker ps -aq) 2>/dev/null; docker network prune -f` выполнены (стенд `as-*`/`adminpanel` не трогать, если поднят).

**Действие:** сценарий `Retention_TrimsToGfs` (spec §4 Ф6). Ключевые механики:

1. Окружение: `E2eEnvironment.StartAsync("bk-ret", withMinio: true)`; сид кластера `bkret`; policy-ключ `{"retention":{"days":1,"weeks":0,"months":0},"full_max_age_sec":600,"verify":{"on_create":false}}`; хост через `StartBackupHostAsync`-копию (env как в `E2eBackupScenarios` + `PgWorker__Backups__Policy__FullMaxAgeSec=600`).
2. Дождаться реального COMPLETED полного на shard1 (образец `Backup_FullDaily_Completes`, бюджет 300 c); прочитать его `wal_start_segment` (обозначь `realWalStart`) и `Id`.
3. Посев синтетики (всё через etcd-Gateway и mc-контейнер `McLsAsync`-образцом):
   - etcd-ключ старого COMPLETED-полного (id вида `20260802000000Z`), `started_unix` ~40 дней назад, `wal_start_segment` — НИЖЕ `realWalStart` на несколько сегментов (посчитать в тесте от `WalFileName.TryParse(realWalStart)`: seg−2 с учётом перехода log/seg — если seg<2, взять log−1/seg=0xFE; тело статуса писать `BackupStatusJson.Serialize(new FullBackupState(...))`).
   - его S3-объекты: mc-посев (запуск mc-контейнера с `-c "mc alias set … && echo synthetic >/tmp/f && mc cp /tmp/f t/<bucket>/<C>/shard1/full/<old-id>/base.tar"`).
   - WAL-объекты строго ниже `realWalStart`: 2 сегмента, посчитанные в тесте (seg−4, seg−3 от реального старта; имена `WalFileName`-хелпером теста).
4. Первый ретенционный проход происходит сразу (расписание `IntervalSec=600`: первый тик — `now − 0 ≥ 600`), второй через 600 c — НЕ ждём: policy days=1 ⇒ старый полный — кандидат; первый проход удалит его (ОДИН кандидат — ОДИН проход), WAL-чистка cutoff = `realWalStart` удалит посевные seg−4/seg−3. Посев ОДНОГО старого COMPLETED-полного достаточен: инвариант «несколько кандидатов удаляются по одному за проход» покрыт интеграцией Task 5 (`Один_кандидат_за_проход`); E2E проверяет инварианты на живом кластере (guard/фазы/storage).
5. Ассерты (`WaitForAsync`, бюджет 240 c):
   - etcd-ключ старого полного исчез; ключ реального COMPLETED жив.
   - host-клиент `BackupS3.ListPrefixAsync` — префикс `full/<old-id>/` пуст; `full/<realId>/` жив; WAL: посевные сегменты ниже cutoff исчезли; все живые wal-объекты ≥ cutoff (сегмент `realWalStart` может отсутствовать в wal/-префиксе до закрытия — ассерт «все живые ≥ cutoff»).
   - ключ `/pgworker/backups/storage` существует, `used_bytes > 0`, `state == "OK"` (квота в E2E не задана).
   - полный teardown уже встроен в `E2eEnvironment.DisposeAsync` (ассерт чистоты сети/контейнеров).
6. Мерж-гейт-маркер: после зелёного `Retention_TrimsToGfs` прогнать `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard` (E2eFixture соберёт Release инкрементально; `PGW_TEST_E2E_NOBUILD=1` НЕ использовать).

**Выход:** E2E-сценарий ретенции зелёный на свежем Release.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Retention_TrimsToGfs` — PASS; затем маркер `Scale_AddEmptyShard` — PASS; после серий: `docker rm -f $(docker ps -aq) 2>/dev/null; docker network prune -f` (контейнеры dev-стенда не трогать).

**Spec:** §4 Ф6, AC2/AC3/AC4/AC6 (E2E-часть), AC9 (маркер).

- [ ] **Шаг 10.1:** написать `E2eRetentionScenarios.cs` (сценарий + локальные копии хелперов `SeedClusterAsync`/`StartBackupHostAsync`; AAA-комментарии; никаких хардкод-портов).
- [ ] **Шаг 10.2:** прогон сценария; при провале — диагностика по образцу `WalStream_UploadsSegmentsContinuously` (дамп журнала `/pgworker/work/<C>`, ключей, mc-ls в сообщение исключения), не ослаблять ассерты.
- [ ] **Шаг 10.3:** прогон маркера `Scale_AddEmptyShard`; зачистка серий.
- [ ] **Шаг 10.4:** коммит `test(e2e): Retention_TrimsToGfs — GFS-чистка, WAL-trim, guard, ключ storage на живом кластере (t06 Ф6)`.

---

### Task 11: Ф7 — мерж-гейт: полный прогон + roadmap + финальный коммит

**Files:**
- Modify: `arch/roadmap/backup.md` (снять `t06-backup-retention`)

**Interfaces:** — (финальный гейт).

**Вход:** Task 10 закоммичен.

**Действие:**
1. Полный прогон на свежем Release, по сериям с зачисткой после каждой (`docker rm -f $(docker ps -aq) 2>/dev/null; docker network prune -f`, dev-стенд `as-*`/`adminpanel` не трогать):
   - юниты: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release`
   - юниты панели: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Release`
   - интеграция PgWorker: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release`
   - интеграция AdminPanel: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.IntegrationTests -c Release`.
2. Roadmap-гейт (AGENTS.md): мерж-коммит снимает тег — удалить из `arch/roadmap/backup.md` пункт `t06-backup-retention` И `←`-зависимость из пункта t07 (`← t04-backup-verify, t05-backup-restore, t06-backup-retention` → убрать `t06-backup-retention`; если t04/t05 ещё не смержены — оставить их). Никаких пометок «закрыта» — история в git.
3. Финальный коммит: `git add arch/roadmap/backup.md && git commit -m "roadmap-гейт: снятие тега t06-backup-retention (мерж в main)"`.
4. Готовность к ревью перед `main` (dev-flow: ревью план↔spec по чек-листам; `superpowers:requesting-code-review`).

**Выход:** ветка полностью зелёная, roadmap чист, ветка готова к ревью/мержу.

**Проверка:** все серии зелёные; `grep -rn "t06-backup-retention" arch/roadmap/` — пусто; `git status --short` — чист.

**Spec:** §4 Ф7, AC8/AC9/AC10.

- [ ] **Шаг 11.1:** прогон серий с зачисткой между ними; зафиксировать результаты.
- [ ] **Шаг 11.2:** roadmap-правка + коммит.
- [ ] **Шаг 11.3:** итоговая сводка для ревью (что изменилось, где смотреть).

---

## Самопроверка плана (выполнена автором; обновлена по итогам двух итераций ревью Фазы 4)

**Замечания ревью Фазы 4, итерация 1 — все закрыты:**
1. ISO-группировка недель — `ISOWeekYearOf` возвращает `(ISOWeek.GetYear(d), ISOWeek.GetWeekOfYear(d))`; добавлены юниты годовых границ ISO-недель.
2. AC4-хвост — в Task 5 кейс `Чистка_WAL_поднимает_chain_start_контроля_t03`: тик ретенции → тик `WalStreamProcess` → `chain_start` поднялся до cutoff, state=ACTIVE.
3. Ветка storage-ключа панельного парсера — ДО гварда `segments.Length < 5 → continue`.
4. Аргументы `Alert` — порядок `(…, SinceUnix, Hint, Remedy, RemedyText)` восстановлен в обоих правилах.
5. Guard над verify-FAILED — юнит `Guard_единственный_Completed_VerifyFailed_остается`.

**Замечания ревью Фазы 4, итерация 2 — закрыты:**
1. [medium] Юниты годовых границ переписаны на `weeks=2`: корректная реализация (одна группа → один слот) даёт `Delete=[2024-12-30]` / `[2026-12-28]`, багованная (расщепление заняло бы оба слота) — `Delete=[]`; ассерты падают на баге. При `weeks=1` обе реализации дают одинаковый Keep (подтверждено расчётом на реальном `ISOWeek`: A weeks=1 keep=[2025-01-05] в обеих) — старая формулировка кейсов была недостаточна, неверная фраза-обоснование удалена. «Опциональное усиление» (третий полный 2024-12-20 из ISO-2024-W51 при weeks=2) из замечания НЕ применено осознанно: по расчёту (B weeks=2) обе реализации дают одинаковый `keep=[2024-12-20, 2025-01-05]` — фрагмент календарного года `(2024, W1)` при лексикографической сортировке кортежей ниже `(2024, W51)` и слот не крадёт, т.е. дополнение МАСКИРУЕТ дискриминацию вместо усиления.
2. [low] Магический ключ `"storage"` в `_lastPassUnix` заменён отдельным полем `long _lastStoragePassUnix` (имя кластера `storage` валидно по regex — общий словарь коллидировал бы; комментарий в коде + benign-гонка параллельных тиков кластеров задокументирована: двойной list безвреден, put при изменении идемпотентен).
3. [неблокирующее] Формулировка Task 8 о `started_unix` уточнена: обязательность — НОВОЕ правило ретенции (текущая full-ветка панельного парсера `started_unix` не валидирует); добавлен юнит Шага 8.2 на DELETING без `started_unix` → KeyParseError.
4. [косметика] `AddCalendarPoint` очищен от неиспользуемых параметров `weeks`/`now` (сигнатура: `completed, dailyWindow, keep, counter, groupOf`; вызовы обновлены).

**Покрытие spec:** §3.1 → Task 1–2; §3.2 → Task 3; §3.3 → Task 5–6; §3.4 → Task 5 (MonitorStorageAsync) + Task 2 (JSON); §3.5 → Task 7; §3.6 → Task 8–9; §3.7 → Task 4 (+ панельный порог Task 9); §4 Ф0–Ф7 → Task 0–11; AC1 → Task 1 (включая годовые границы ISO-недель с дискриминацией бага на weeks=2); AC2 → Task 1 (guard, вкл. verify-FAILED) + Task 5 (интеграция) + Task 10 (E2E); AC3 → Task 5; AC4 → Task 2 (юниты) + Task 5 (интеграция: чистка + поднятие `chain_start` контролем t03) + Task 10 (E2E-объекты); AC5 → Task 2 (юнит) + Task 5 (интеграция); AC6 → Task 2/5 (воркер) + Task 8–9 (панель); AC7 → Task 7 + Task 5 (`Policy_из_ключа_кластера_действует`); AC8 → Task 4 (дефолты) + Task 5 (no-op) + Task 6 (guard цикла) + Task 11 (полный прогон без правки ожиданий); AC9 → Task 10–11; AC10 → Task 0.

**Известные упрощения против буквы spec:** (1) имя `BackupsRetentionPassOptions` вместо `BackupsRetentionOptions` из §3.8 — имя занято GFS-гранулами политики (BackupsPolicyOptions.Retention), конфликт разрешён явно; (2) E2E сеет ОДИН синтетический полный (не два) — «один кандидат за проход» + бюджет сценария; инвариант нескольких кандидатов покрыт интеграцией Task 5; (3) `.history` в E2E не сеется — реальный TLI=1 не даёт «старых TLI»; покрыто юнитами Task 2.

**Типы/сигнатуры сверены:** `RetentionSelection` (Task 1→5), `S3ObjectInfo`/`ListPrefixAsync`/`DeleteKeysAsync` (Task 3→5/10), `BackupsRuntimeOptions` +8 полей (Task 4→5/6), `RetentionProcess.TickAsync(ClusterSnapshot, ClusterBackups?, ct)` (Task 5→6), `BackupStorageInfo`/`DeletingFullInfo`/`EtcdSnapshot.BackupStorage` (Task 8→9), `Alert(…, SinceUnix, Hint, Remedy, RemedyText)` (Task 9), policy-JSON канона §4 (Task 7 ↔ парсер t01).

**Опасные места для исполнителя:**
- `ISOWeekYearOf` — год ТОЛЬКО через `ISOWeek.GetYear` (не календарный `d.Year`); юниты годовых границ обязаны использовать `weeks=2` (при `weeks=1` свежейшая группа даёт того же представителя в обеих реализациях — тест не дискриминирует баг; ревью Ф4-2 №1).
- `_lastPassUnix` — только per-cluster; расписание монитора хранилища — отдельное поле `_lastStoragePassUnix` (коллизия с валидным именем кластера `storage`; ревью Ф4-2 №2).
- `EtcdSnapshot`/`ClusterBackupsInfo`/`BackupsParseResult` — позиционные records: новые поля ТОЛЬКО в конец со значениями по умолчанию (Task 8), иначе посыплются `TestSnapshots`/`FailTick`/`SnapshotBuilder`.
- Ветка storage-ключа панельного парсера — ДО гварда `segments.Length < 5` (ревью Ф4-1 №3).
- Хвостовые аргументы `Alert` — `(SinceUnix=null, Hint, Remedy, RemedyText)`, Hint — инвариант, RemedyText — действие (ревью Ф4-1 №4).
- `IBackupS3` расширяется в четырёх местах сразу (Task 3): `BackupS3`, `ReloadableBackupS3`+`DisabledBackupS3` (Program.cs), `FakeBackupS3` — пропустишь одно, сборка упадёт (это гейт, не проблема).
- `ValidationError` — сверить фактическую сигнатуру перед Task 7 (шаг 7.1).
- В `AddCalendarPoint` фильтр «вне дневного окна» — по множеству id (record-семантика `Contains` по значению — ловушка, описана в Task 1).
- Тесты `RetentionProcessTests` используют runtime-опции напрямую: `RetentionIntervalSec: 0` в тестах легален (валидация `IntervalSec ≥ 60` — только на старте App через `BackupsOptions.IsValid()`), но в конфигах хостов (E2E) — только ≥ 60.
