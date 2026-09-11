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

    private static readonly BackupPolicy Policy = new(7, 4, 6, 86400, VerifyOnCreate: false);

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

    // AC1: дневная гранула — все COMPLETED последних retention.days календарных
    // суток UTC (включая текущую) остаются, Delete пуст.
    [Fact]
    public void Дневные_все_свежие_остаются()
    {
        // Arrange — полные за сегодня/вчера/позавчера при days=7
        var fulls = new[]
        {
            Full("d1", Now),
            Full("d2", Now.AddDays(-1)),
            Full("d3", Now.AddDays(-2)),
        };

        // Act — отбор
        var (keep, delete) = Select(new BackupPolicy(7, 4, 6, 86400, false), fulls);

        // Assert — все три в Keep, удалять нечего
        keep.Should().BeEquivalentTo(["d1", "d2", "d3"]);
        delete.Should().BeEmpty();
    }

    // AC1: недельная точка — ПОСЛЕДНИЙ (max started_unix) COMPLETED ISO-недели,
    // не первый.
    [Fact]
    public void Недельная_точка_последний_в_неделе()
    {
        // Arrange — два полных в одной ISO-неделе старше дневного окна (days=1);
        // 2026-08-19 и 2026-08-21 — ISO-неделя 34 ISO-2026
        var policy = new BackupPolicy(1, 1, 0, 86400, false);
        var fulls = new[]
        {
            Full("w-first", new DateTimeOffset(2026, 8, 19, 10, 0, 0, TimeSpan.Zero)),
            Full("w-last", new DateTimeOffset(2026, 8, 21, 18, 0, 0, TimeSpan.Zero)),
        };

        // Act — отбор
        var (keep, delete) = Select(policy, fulls);

        // Assert — остаётся последний недели, первый — кандидат
        keep.Should().BeEquivalentTo(["w-last"]);
        delete.Should().BeEquivalentTo(["w-first"]);
    }

    // AC1: месячная точка — ПОСЛЕДНИЙ COMPLETED календарного месяца.
    [Fact]
    public void Месячная_точка_последний_в_месяце()
    {
        // Arrange — два полных в июне 2026, days=1/weeks=0/months=1
        var policy = new BackupPolicy(1, 0, 1, 86400, false);
        var fulls = new[]
        {
            Full("m-first", new DateTimeOffset(2026, 6, 2, 9, 0, 0, TimeSpan.Zero)),
            Full("m-last", new DateTimeOffset(2026, 6, 28, 20, 0, 0, TimeSpan.Zero)),
        };

        // Act — отбор
        var (keep, delete) = Select(policy, fulls);

        // Assert — остаётся июньский от 28-го
        keep.Should().BeEquivalentTo(["m-last"]);
        delete.Should().BeEquivalentTo(["m-first"]);
    }

    // AC1: граница ISO-недель — воскресенье и понедельник в РАЗНЫХ группах;
    // остаётся свежайшая (понедельник).
    [Fact]
    public void Граница_недели()
    {
        // Arrange — 2026-08-23 (вс, ISO-неделя 34) и 2026-08-24 (пн, неделя 35)
        var policy = new BackupPolicy(1, 1, 0, 86400, false);
        var fulls = new[]
        {
            Full("sun", new DateTimeOffset(2026, 8, 23, 22, 0, 0, TimeSpan.Zero)),
            Full("mon", new DateTimeOffset(2026, 8, 24, 8, 0, 0, TimeSpan.Zero)),
        };

        // Act — отбор
        var (keep, delete) = Select(policy, fulls);

        // Assert — воскресенье предыдущей недели в Delete
        keep.Should().BeEquivalentTo(["mon"]);
        delete.Should().BeEquivalentTo(["sun"]);
    }

    // AC1: граница месяцев — июль и август в разных группах.
    [Fact]
    public void Граница_месяца()
    {
        // Arrange — 2026-07-31 и 2026-08-01, months=1
        var policy = new BackupPolicy(1, 0, 1, 86400, false);
        var fulls = new[]
        {
            Full("jul", new DateTimeOffset(2026, 7, 31, 12, 0, 0, TimeSpan.Zero)),
            Full("aug", new DateTimeOffset(2026, 8, 1, 0, 30, 0, TimeSpan.Zero)),
        };

        // Act — отбор
        var (keep, delete) = Select(policy, fulls);

        // Assert — июльская точка в Delete
        keep.Should().BeEquivalentTo(["aug"]);
        delete.Should().BeEquivalentTo(["jul"]);
    }

    // Годовая граница ISO-недель (ревью Ф4 №1, итерация 2 — weeks=2
    // дискриминирует баг календарного года): 2024-12-30 (пн) и 2025-01-05 (вс)
    // — ОДНА ISO-неделя 1 ISO-2025. Корректная реализация: одна группа →
    // ОДИН слот недель → Delete = [2024-12-30]. Багованная (d.Year): две
    // группы занимают оба слота weeks=2 → Delete = [] — ассерт падает.
    [Fact]
    public void Годовая_граница_ISO_недели()
    {
        // Arrange — days=1, weeks=2, months=0; now = 2025-01-08 (ср)
        var policy = new BackupPolicy(1, 2, 0, 86400, false);
        var fulls = new[]
        {
            Full("old", new DateTimeOffset(2024, 12, 30, 12, 0, 0, TimeSpan.Zero)),
            Full("new", new DateTimeOffset(2025, 1, 5, 12, 0, 0, TimeSpan.Zero)),
        };

        // Act — отбор
        var (keep, delete) = SelectAt(
            new DateTimeOffset(2025, 1, 8, 12, 0, 0, TimeSpan.Zero), policy, fulls);

        // Assert — одна ISO-группа: остаётся представитель 2025-01-05
        keep.Should().BeEquivalentTo(["new"]);
        delete.Should().BeEquivalentTo(["old"]);
    }

    // Годовая граница ISO-недель 53 (ревью Ф4 №1, итерация 2 — weeks=2):
    // 2026-12-28 (пн) и 2027-01-01 (пт) — обе ISO-неделя 53 ISO-2026.
    // Корректно: одна группа → Delete = [2026-12-28]; баг: две группы →
    // Delete = [] — ассерт падает.
    [Fact]
    public void Годовая_граница_ISO_недели_53()
    {
        // Arrange — days=1, weeks=2, months=0; now = 2027-01-04 (пн)
        var policy = new BackupPolicy(1, 2, 0, 86400, false);
        var fulls = new[]
        {
            Full("old", new DateTimeOffset(2026, 12, 28, 12, 0, 0, TimeSpan.Zero)),
            Full("new", new DateTimeOffset(2027, 1, 1, 12, 0, 0, TimeSpan.Zero)),
        };

        // Act — отбор
        var (keep, delete) = SelectAt(
            new DateTimeOffset(2027, 1, 4, 12, 0, 0, TimeSpan.Zero), policy, fulls);

        // Assert — одна ISO-группа 53: остаётся представитель 2027-01-01
        keep.Should().BeEquivalentTo(["new"]);
        delete.Should().BeEquivalentTo(["old"]);
    }

    // AC1: weeks=0 исключает недельную гранулу — старые полные вне дневного
    // окна удаляются (guard не удерживает: есть свежий в Keep).
    [Fact]
    public void Weeks0_исключает_гранулу()
    {
        // Arrange — days=1/weeks=0/months=0: свежий полный (удержит guard и
        // даст непустой Keep) + три старых (3 недели назад)
        var policy = new BackupPolicy(1, 0, 0, 86400, false);
        var fulls = new[]
        {
            Full("fresh", Now.AddDays(-1)),
            Full("old1", Now.AddDays(-21)),
            Full("old2", Now.AddDays(-22)),
            Full("old3", Now.AddDays(-23)),
        };

        // Act — отбор
        var (keep, delete) = Select(policy, fulls);

        // Assert — все старые в Delete, свежий в Keep
        keep.Should().BeEquivalentTo(["fresh"]);
        delete.Should().BeEquivalentTo(["old1", "old2", "old3"]);
    }

    // Активные (PLANNED/RUNNING/UPLOADING) и DELETING — вне отбора: «свежие»
    // активные не спасают старые COMPLETED.
    [Fact]
    public void Активные_и_DELETING_вне_отбора()
    {
        // Arrange — COMPLETED 30 дней назад + Running/Uploading/Planned/Deleting
        // с сегодняшними датами; days=1/weeks=0/months=0
        var policy = new BackupPolicy(1, 0, 0, 86400, false);
        var old = Full("old30", Now.AddDays(-30));
        var running = Full("running", Now) with { State = FullBackupStatus.Running };
        var uploading = Full("uploading", Now) with { State = FullBackupStatus.Uploading };
        var planned = Full("planned", Now) with { State = FullBackupStatus.Planned };
        var deleting = Full("deleting", Now) with { State = FullBackupStatus.Deleting };

        // Act — отбор
        var (keep, delete) = Select(policy, old, running, uploading, planned, deleting);

        // Assert — guard спасает старый (единственный COMPLETED), Delete пуст
        keep.Should().BeEquivalentTo(["old30"]);
        delete.Should().BeEmpty();
    }

    // AC2 (guard): единственный COMPLETED просрочен — остаётся, Delete пуст.
    [Fact]
    public void Guard_единственный_просроченный_остается()
    {
        // Arrange — один COMPLETED 40 дней назад, days=1/weeks=0/months=0
        var policy = new BackupPolicy(1, 0, 0, 86400, false);
        var fulls = new[] { Full("lone", Now.AddDays(-40)) };

        // Act — отбор
        var (keep, delete) = Select(policy, fulls);

        // Assert — guard-доводка вернула его в Keep
        keep.Should().BeEquivalentTo(["lone"]);
        delete.Should().BeEmpty();
    }

    // AC2 (ревью Ф4 №5): guard «≥1 COMPLETED» держится и над verify-FAILED —
    // единственный COMPLETED с verify.state=FAILED не удаляется.
    [Fact]
    public void Guard_единственный_Completed_VerifyFailed_остается()
    {
        // Arrange — один COMPLETED 40 дней назад с verify=FAILED
        var policy = new BackupPolicy(1, 0, 0, 86400, false);
        var fulls = new[]
        {
            Full("lone", Now.AddDays(-40),
                verify: new BackupVerify(BackupVerifyStatus.Failed, null)),
        };

        // Act — отбор
        var (keep, delete) = Select(policy, fulls);

        // Assert — guard сильнее verify-приоритета
        keep.Should().BeEquivalentTo(["lone"]);
        delete.Should().BeEmpty();
    }

    // Guard: самый свежий COMPLETED — всегда в Keep, даже вне любого окна.
    [Fact]
    public void Guard_самый_свежий_всегда_в_Keep()
    {
        // Arrange — старая пачка + свежий «вчера 23:59» при days=1 (вне
        // дневного окна «сегодня»)
        var policy = new BackupPolicy(1, 1, 1, 86400, false);
        var fresh = Full("fresh", Now.AddDays(-1).AddMinutes(-1)); // вчера 11:59
        var fulls = new[]
        {
            Full("m1", Now.AddMonths(-1)),
            Full("w1", Now.AddDays(-8)),
            fresh,
        };

        // Act — отбор
        var (keep, delete) = Select(policy, fulls);

        // Assert — свежий в Keep несмотря на пустое дневное окно
        keep.Should().Contain("fresh");
    }

    // Порядок Delete: verify-FAILED — первыми, далее по возрастанию started_unix.
    [Fact]
    public void VerifyFailed_первые_в_Delete()
    {
        // Arrange — days=1/weeks=0/months=0: свежий полный в дневном окне
        // (держит guard) + два кандидата вне окон: старший OK и младший FAILED
        var policy = new BackupPolicy(1, 0, 0, 86400, false);
        var fresh = Full("fresh", Now);
        var okOld = Full("ok-old", Now.AddMonths(-2));
        var failedYounger = Full("failed-younger", Now.AddMonths(-1),
            verify: new BackupVerify(BackupVerifyStatus.Failed, null));

        // Act — отбор
        var (keep, delete) = Select(policy, fresh, okOld, failedYounger);

        // Assert — FAILED младший идёт первым, далее старший OK по возрасту
        delete.Should().HaveCount(2);
        delete[0].Should().Be("failed-younger");
        delete[1].Should().Be("ok-old");
        keep.Should().BeEquivalentTo(["fresh"]);
    }

    // Пустой вход — пустой выход (без исключений).
    [Fact]
    public void Пустой_набор()
    {
        // Arrange — пустой список полных

        // Act — отбор
        var (keep, delete) = Select(Policy);

        // Assert — оба пустые
        keep.Should().BeEmpty();
        delete.Should().BeEmpty();
    }

    // Политика 0/0/0: дневная гранула удерживает свежий, старый уходит.
    [Fact]
    public void Политика_0_0_0_с_guard()
    {
        // Arrange — days=1/weeks=0/months=0; один COMPLETED сегодня + один 40 дней
        var policy = new BackupPolicy(1, 0, 0, 86400, false);
        var fulls = new[]
        {
            Full("today", Now),
            Full("ancient", Now.AddDays(-40)),
        };

        // Act — отбор
        var (keep, delete) = Select(policy, fulls);

        // Assert — свежий остался, древний — кандидат
        keep.Should().BeEquivalentTo(["today"]);
        delete.Should().BeEquivalentTo(["ancient"]);
    }

    // Високосность: 2028-02-29 — валидная дата календаря UTC, февраль без сбоев.
    [Fact]
    public void Високосность()
    {
        // Arrange — now = 2028-02-29 12:00Z, полные 2028-02-28 и 2028-02-01;
        // политика 7/4/6: 28-е в дневном окне, 01-е — точка февраля
        var fulls = new[]
        {
            Full("feb28", new DateTimeOffset(2028, 2, 28, 12, 0, 0, TimeSpan.Zero)),
            Full("feb01", new DateTimeOffset(2028, 2, 1, 12, 0, 0, TimeSpan.Zero)),
        };

        // Act — отбор (дефолтная политика 7/4/6: 01-е удерживает месячная/недельная
        // точка февраля, 28-е — дневное окно; високосная арифметика без сбоев)
        var (keep, delete) = SelectAt(
            new DateTimeOffset(2028, 2, 29, 12, 0, 0, TimeSpan.Zero), Policy, fulls);

        // Assert — оба в дневном окне 2028-02-23..29
        keep.Should().BeEquivalentTo(["feb28", "feb01"]);
        delete.Should().BeEmpty();
    }

    // ---- SelectWalForDeletion (AC4, юнит-часть) ----

    // Строго ниже cutoff — на удаление; сам cutoff/выше/дальше — не входят.
    [Fact]
    public void Сегменты_ниже_cutoff_удалены()
    {
        // Arrange — cutoff = tli1/log0/seg5; объекты seg3, seg4 (ниже), seg5
        // (сам cutoff), seg6, segFF (выше)
        var cutoff = new WalFileName(1, 0, 5);
        var names = new[]
        {
            "000000010000000000000003",
            "000000010000000000000004",
            "000000010000000000000005",
            "000000010000000000000006",
            "0000000100000000000000FF",
        };

        // Act — план чистки
        var doomed = RetentionPlanner.SelectWalForDeletion(names, cutoff);

        // Assert — только строго нижние
        doomed.Should().BeEquivalentTo([
            "000000010000000000000003",
            "000000010000000000000004",
        ]);
    }

    // Переход log-границы: позиция сравнивается как log·256+seg, а не посегментно.
    [Fact]
    public void Граница_log_сег_FF()
    {
        // Arrange — cutoff = tli1/log1/seg0; FF (log0/seg255) — позиция 255 < 256
        var cutoff = new WalFileName(1, 1, 0);
        var names = new[] { "0000000100000000000000FF", "000000010000000100000000" };

        // Act — план чистки
        var doomed = RetentionPlanner.SelectWalForDeletion(names, cutoff);

        // Assert — FF ниже cutoff, сам cutoff (log1/seg0) — нет
        doomed.Should().BeEquivalentTo(["0000000100000000000000FF"]);
    }

    // TLI ниже — удаляются; TLI выше позицией ниже cutoff — НЕ трогаются
    // (консервативность: контроль t03 объекты чужого TLI игнорирует).
    [Fact]
    public void TLI_ниже_удалены_выше_не_тронуты()
    {
        // Arrange — cutoff = tli2/log0/seg3; tli1 ниже, tli3 выше (позиция ниже)
        var cutoff = new WalFileName(2, 0, 3);
        var names = new[]
        {
            "0000000100000000000000AA",
            "000000030000000000000001",
        };

        // Act — план чистки
        var doomed = RetentionPlanner.SelectWalForDeletion(names, cutoff);

        // Assert — удалён только tli1
        doomed.Should().BeEquivalentTo(["0000000100000000000000AA"]);
    }

    // .history старых TLI — на удаление; cutoff-TLI и новее, .partial, мусор — живы.
    [Fact]
    public void History_старых_TLI_удалены()
    {
        // Arrange — cutoff tli=2: history1 (ниже) удалён, history2/history3 живы;
        // .partial и нераспознаваемое имя не трогаются
        var cutoff = new WalFileName(2, 0, 0);
        var names = new[]
        {
            "00000001.history",
            "00000002.history",
            "00000003.history",
            "000000010000000000000005.partial",
            "not-a-wal-name",
        };

        // Act — план чистки
        var doomed = RetentionPlanner.SelectWalForDeletion(names, cutoff);

        // Assert — только .history tli1
        doomed.Should().BeEquivalentTo(["00000001.history"]);
    }

    // ---- EvaluateStorage (AC6, юнит-часть) ----

    // Квота 0/не задана → OK без процентов.
    [Fact]
    public void Квота_0_без_процентов()
    {
        // Arrange — used=123, квота не задана

        // Act — вердикт
        var verdict = RetentionPlanner.EvaluateStorage(123, 0, 80, 90);

        // Assert — Ok, квота 0, проценты отсутствуют
        verdict.State.Should().Be(StorageState.Ok);
        verdict.QuotaBytes.Should().Be(0);
        verdict.UsedPercent.Should().BeNull();
        verdict.UsedBytes.Should().Be(123);
    }

    // Пороги: >= warn → WARN, >= crit → CRIT, ниже — OK (границы включительно).
    [Theory]
    [InlineData(799, StorageState.Ok)]   // 79.9%
    [InlineData(800, StorageState.Warn)] // ровно 80%
    [InlineData(900, StorageState.Crit)] // ровно 90%
    [InlineData(990, StorageState.Crit)] // 99%
    public void Пороги_Warn_Crit(long used, StorageState expected)
    {
        // Arrange — квота 1000, пороги 80/90

        // Act — вердикт
        var verdict = RetentionPlanner.EvaluateStorage(used, 1000, 80, 90);

        // Assert — состояние по порогу
        verdict.State.Should().Be(expected);
        verdict.UsedPercent.Should().NotBeNull();
        verdict.QuotaBytes.Should().Be(1000);
    }

    // ---- SelectFailedForPrune (AC5) ----

    // Держим последние keepFailed, старейшие — на удаление.
    [Fact]
    public void Prune_держит_последние_KeepFailed()
    {
        // Arrange — 30 FAILED с нарастающим started_unix, keepFailed=20
        var fulls = new List<FullBackupState>();
        for (var i = 0; i < 30; i++)
            fulls.Add(Full($"f{i:00}", Now.AddMinutes(-60 + i)) with { State = FullBackupStatus.Failed });

        // Act — план чистки
        var pruned = RetentionPlanner.SelectFailedForPrune(fulls, keepFailed: 20);

        // Assert — ровно 10 старейших, по возрастанию started_unix
        pruned.Should().HaveCount(10);
        pruned.Should().BeInAscendingOrder(x => x, because: "порядок исполнения — старейшие вперёд");
        pruned[0].Should().Be("f00");
        pruned[^1].Should().Be("f09");
    }

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
}
