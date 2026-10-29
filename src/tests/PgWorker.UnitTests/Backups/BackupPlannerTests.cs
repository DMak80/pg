using System.Globalization;
using PgWorker.Backups;
using PgWorker.Etcd.Parsing;

namespace PgWorker.UnitTests.Backups;

// Чистые решения планировщика полных бэкапов (arch/19 §2): due по
// возрасту последнего COMPLETED, бэкофф min(Base·2^(n−1), Max), инвариант
// одного активного, суффикс коллизии id.
public class BackupPlannerTests
{
    private static readonly DateTime Now = new(2026, 9, 10, 3, 0, 0, DateTimeKind.Utc);

    private static long Unix(DateTime t)
        => new DateTimeOffset(t).ToUnixTimeSeconds();

    private static FullBackupState Full(string id, FullBackupStatus state, long started, long? finished = null,
        BackupVerify? verify = null)
        => new(id, state, "n1", BackupSourceRole.Replica, started, finished, null, null, null, verify);

    // AAA: COMPLETED+verify FAILED не даёт свежести → IsDue=true (AC6)
    [Fact]
    public void IsDue_FreshButBrokenFull_Due()
    {
        // Arrange — COMPLETED час назад (в окне), но verify FAILED
        var fulls = new[]
        {
            Full("20260911110000Z", FullBackupStatus.Completed, Unix(Now.AddHours(-1).AddMinutes(-5)),
                Unix(Now.AddHours(-1)), verify: new BackupVerify(BackupVerifyStatus.Failed, Unix(Now.AddHours(-1)), "bad")),
        };

        // Act / Assert
        BackupPlanner.IsDue(fulls, fullMaxAgeSec: 86400, nowUnix: Unix(Now))
            .Should().BeTrue("a failed verify does not count as freshness");
    }

    // AAA: валидный = verify null | PENDING | OK — все три дают свежесть
    [Fact]
    public void IsDue_AllValidKinds_ExtinguishDue()
    {
        // Arrange — три конфигурации свежего COMPLETED (час назад, окно 86400)
        var fresh = Unix(Now.AddHours(-1).AddMinutes(-5));
        var finished = Unix(Now.AddHours(-1));
        var noVerify = new[] { Full("20260911110000Z", FullBackupStatus.Completed, fresh, finished) };
        var pending = new[] { Full("20260911110001Z", FullBackupStatus.Completed, fresh, finished,
            verify: new BackupVerify(BackupVerifyStatus.Pending, null)) };
        var ok = new[] { Full("20260911110002Z", FullBackupStatus.Completed, fresh, finished,
            verify: new BackupVerify(BackupVerifyStatus.Ok, finished)) };

        // Act / Assert — каждая конфигурация сама по себе гасит due (в окне)
        BackupPlanner.IsDue(noVerify, 86400, Unix(Now)).Should().BeFalse();
        BackupPlanner.IsDue(pending, 86400, Unix(Now)).Should().BeFalse();
        BackupPlanner.IsDue(ok, 86400, Unix(Now)).Should().BeFalse();
    }

    // AAA: бэкофф n считает и verify-фейлы: COMPLETED+FAILED-verify после последнего
    // валидного — попытка, окно растёт (AC6)
    [Fact]
    public void BackoffPassed_VerifyFailIncrementsN()
    {
        // Arrange — старый валидный COMPLETED (2 дня назад) + свежий COMPLETED с
        // verify FAILED (100 c назад), Base=300
        var fulls = new[]
        {
            Full("20260909030000Z", FullBackupStatus.Completed, Unix(Now.AddDays(-2)), Unix(Now.AddDays(-2).AddMinutes(5))),
            Full("20260911025840Z", FullBackupStatus.Completed, Unix(Now) - 100, Unix(Now) - 100,
                verify: new BackupVerify(BackupVerifyStatus.Failed, Unix(Now), "bad")),
        };

        // Act / Assert — n=1: окно Base=300 c от последней попытки (verify-фейл считается)
        BackupPlanner.BackoffPassed(fulls, 300, 3600, Unix(Now)).Should().BeFalse();
        BackupPlanner.BackoffPassed(fulls, 300, 3600, Unix(Now) + 301).Should().BeTrue();
    }

    // AAA: due — нет COMPLETED (первое включение/все FAILED)
    [Fact]
    public void IsDue_NoCompleted_True()
    {
        // Arrange — только FAILED-история
        var fulls = new[] { Full("20260909030000Z", FullBackupStatus.Failed, Unix(Now.AddDays(-1))) };

        // Act / Assert
        BackupPlanner.IsDue(fulls, fullMaxAgeSec: 86400, nowUnix: Unix(Now)).Should().BeTrue();
    }

    // AAA: после COMPLETED-restore полный старее restore → due даже при живом
    // wal-ключе (WalStream восстанавливает ключ быстрее тика — инцидент гейта);
    // полный НОВЕЕ restore — НЕ due.
    [Fact]
    public void IsDue_FullOlderThanRestore_Due_IndependentOfWalKey()
    {
        // Arrange — валидный COMPLETED за час до restore; restore завершён 5 мин назад
        var fulls = new[]
        {
            Full("20260911100000Z", FullBackupStatus.Completed, Unix(Now.AddHours(-1).AddMinutes(-5)),
                Unix(Now.AddHours(-1))),
        };
        var restoreFinished = Unix(Now.AddMinutes(-5));

        // Act / Assert — живой wal-ключ больше не гасит due: факт restore сильнее
        BackupPlanner.IsDue(fulls, 86400, Unix(Now), restoreFinished)
            .Should().BeTrue("the full describes the shard's former life — it is re-shot");
        BackupPlanner.IsDue(fulls, 86400, Unix(Now), restoreFinished)
            .Should().BeTrue();
    }

    [Fact]
    public void IsDue_FullNewerThanRestore_NotDue()
    {
        // Arrange — restore час назад, свежий переснятый полный 5 минут назад
        var restoreFinished = Unix(Now.AddHours(-1));
        var fulls = new[]
        {
            Full("20260911100000Z", FullBackupStatus.Completed, Unix(Now.AddMinutes(-10)),
                Unix(Now.AddMinutes(-5))),
        };

        // Act / Assert — переснятый после restore полный валиден; null-параметр
        // (restore без finished/старый формат) — прежнее rolling-правило
        BackupPlanner.IsDue(fulls, 86400, Unix(Now), restoreFinished)
            .Should().BeFalse();
        BackupPlanner.IsDue(fulls, 86400, Unix(Now), null).Should().BeFalse();
    }

    // AAA: due — последний COMPLETED старше full_max_age_sec
    [Fact]
    public void IsDue_LastCompletedOlderThanMaxAge_True()
    {
        // Arrange — COMPLETED сутки+минуту назад при пороге 86400
        var fulls = new[] { Full("20260909025500Z", FullBackupStatus.Completed, Unix(Now.AddDays(-1).AddMinutes(-5)), Unix(Now.AddDays(-1).AddMinutes(-1))) };

        // Act / Assert
        BackupPlanner.IsDue(fulls, fullMaxAgeSec: 86400, nowUnix: Unix(Now)).Should().BeTrue();
    }

    // AAA: не due — свежий COMPLETED (finished_unix внутри окна)
    [Fact]
    public void IsDue_FreshCompleted_False()
    {
        // Arrange — завершён час назад
        var fulls = new[] { Full("20260910020000Z", FullBackupStatus.Completed, Unix(Now.AddHours(-1).AddMinutes(-5)), Unix(Now.AddHours(-1))) };

        // Act / Assert
        BackupPlanner.IsDue(fulls, fullMaxAgeSec: 86400, nowUnix: Unix(Now)).Should().BeFalse();
    }

    // AAA: wal-ключа нет, но свежий валидный COMPLETED есть — НЕ due :
    // приёмник выгружает только ЗАКРЫТЫЕ сегменты, поэтому на шарде без
    // нагрузки ключ wal появляется позже первого полного; пересъём в этом
    // окне бессмыслен (серия полных подряд ничего не добавляет) — цепочку
    // заведёт контроль от wal_start_segment свежего COMPLETED при первом
    // донесённом сегменте (инвариант «поднятый шард имеет валидную цепочку
    // или свежий полный», arch/19 §2).
    [Fact]
    public void IsDue_NoWalKey_FreshFull_ExtinguishesDue()
    {
        // Arrange — COMPLETED 5 минут назад; wal-ключ ещё не появился
        // (первый закрытый сегмент не донесён) — контекст отсутствия ключа
        // отражён вызовом BackupProcess (параметр удалён из IsDue)
        var fulls = new[] { Full("20260910025900Z", FullBackupStatus.Completed, Unix(Now.AddMinutes(-5)), Unix(Now.AddMinutes(-1))) };

        // Act / Assert
        BackupPlanner.IsDue(fulls, fullMaxAgeSec: 86400, nowUnix: Unix(Now))
            .Should().BeFalse("a fresh COMPLETED will already start the chain — a reshoot is pointless");
    }

    // AAA: живой wal-ключ — прежнее rolling-правило по возрасту
    [Fact]
    public void IsDue_WalKeyAlive_DueByAge()
    {
        // Arrange — COMPLETED старше порога, ключ жив
        var fulls = new[] { Full("20260909030000Z", FullBackupStatus.Completed, Unix(Now.AddDays(-1)), Unix(Now.AddDays(-1).AddMinutes(5))) };

        // Act / Assert
        BackupPlanner.IsDue(fulls, fullMaxAgeSec: 100, nowUnix: Unix(Now))
            .Should().BeTrue();
    }

    // AAA: бэкофф — первая неудача ждёт BaseSec с последней попытки
    [Fact]
    public void Backoff_FirstFailure_WaitsBaseSec()
    {
        // Arrange — один FAILED 100 c назад, Base=300
        var fulls = new[]
        {
            Full("20260908030000Z", FullBackupStatus.Completed, Unix(Now.AddDays(-2)), Unix(Now.AddDays(-2).AddMinutes(5))),
            Full("20260910025820Z", FullBackupStatus.Failed, Unix(Now) - 100),
        };

        // Act / Assert — 100 < 300: гвард держит; сквозь 300 c — отпускает.
        BackupPlanner.BackoffPassed(fulls, 300, 3600, Unix(Now)).Should().BeFalse();
        BackupPlanner.BackoffPassed(fulls, 300, 3600, Unix(Now) + 201).Should().BeTrue();
    }

    // AAA: бэкофф — экспонента с капом MaxSec
    [Fact]
    public void Backoff_ExponentialCappedAtMax()
    {
        // Arrange — 5 FAILED после последнего COMPLETED, последняя 100 c назад:
        // ожидание min(300·2^4, 3600) = 3600.
        var fulls = new[]
        {
            Full("20260905030000Z", FullBackupStatus.Completed, Unix(Now.AddDays(-5)), Unix(Now.AddDays(-5).AddMinutes(5))),
        };
        for (var i = 1; i <= 5; i++)
            fulls = fulls.Append(Full($"2026090{i}030000Z", FullBackupStatus.Failed, Unix(Now.AddDays(-5).AddHours(i)))).ToArray();
        fulls = fulls.Append(Full("20260910025820Z", FullBackupStatus.Failed, Unix(Now) - 100)).ToArray();

        // Act / Assert — 100 < 3600: держит; через 3501 c — отпускает.
        BackupPlanner.BackoffPassed(fulls, 300, 3600, Unix(Now)).Should().BeFalse();
        BackupPlanner.BackoffPassed(fulls, 300, 3600, Unix(Now) + 3501).Should().BeTrue();
    }

    // AAA: бэкофф — нет FAILED после COMPLETED: попытка не откладывается
    [Fact]
    public void Backoff_NoFailures_PassesImmediately()
    {
        // Arrange — свежих FAILED нет
        var fulls = new[] { Full("20260908030000Z", FullBackupStatus.Completed, Unix(Now.AddDays(-2)), Unix(Now.AddDays(-2).AddMinutes(5))) };

        // Act / Assert
        BackupPlanner.BackoffPassed(fulls, 300, 3600, Unix(Now)).Should().BeTrue();
    }

    // AAA: антишторм (arch/19 §2) — НЕпокрывший COMPLETED (wal_start ниже
    // границы разрыва) считается попыткой бэкоффа: пока redo/restart-точка
    // источника не перешла границу, повторный полный снимается с той же позиции
    // — без пользы (шторм «полный каждые 10 с»); предикат caller'а учитывает
    // только непокрывшие, покрывший COMPLETED попыткой НЕ является
    [Fact]
    public void Backoff_UncoveredCompleted_CountsAsAttempt()
    {
        // Arrange — старый валидный COMPLETED + свежий COMPLETED с wal_start
        // ниже границы (последняя попытка 100 c назад), граница «1/001»
        var uncoveredFull = Full("20260910025820Z", FullBackupStatus.Completed, Unix(Now) - 100, Unix(Now) - 90)
            with { WalStartSegment = "000000020000000100000000" };
        var fulls = new FullBackupState[]
        {
            Full("20260908030000Z", FullBackupStatus.Completed, Unix(Now.AddDays(-2)), Unix(Now.AddDays(-2).AddMinutes(5))),
            uncoveredFull,
        };

        // Act / Assert — без предиката: шторм (попыток нет, пропуск немедленный);
        // с предикатом: 100 < 300 — держит; сквозь 300 c — отпускает.
        BackupPlanner.BackoffPassed(fulls, 300, 3600, Unix(Now)).Should().BeTrue(
            "without the uncovered predicate a covered-in-name COMPLETED storms the retake");
        var uncovered = new Func<FullBackupState, bool>(f =>
            f.State == FullBackupStatus.Completed
            && f.WalStartSegment is { Length: > 0 } ws
            && string.CompareOrdinal(ws, "000000020000000100000001") < 0);
        BackupPlanner.BackoffPassed(fulls, 300, 3600, Unix(Now), uncovered).Should().BeFalse(
            "an uncovered COMPLETED below the gap boundary is a backoff attempt");
        BackupPlanner.BackoffPassed(fulls, 300, 3600, Unix(Now) + 201, uncovered).Should().BeTrue();
    }

    // AAA: инвариант — максимум один активный (PLANNED/RUNNING/UPLOADING)
    [Theory]
    [InlineData(FullBackupStatus.Planned, true)]
    [InlineData(FullBackupStatus.Running, true)]
    [InlineData(FullBackupStatus.Uploading, true)]
    [InlineData(FullBackupStatus.Completed, false)]
    [InlineData(FullBackupStatus.Failed, false)]
    [InlineData(FullBackupStatus.Deleting, false)]
    public void HasActive_ByState(FullBackupStatus state, bool expected)
    {
        // Arrange / Act / Assert
        BackupPlanner.HasActive([Full("id", state, Unix(Now))]).Should().Be(expected);
    }

    // AAA: WAL_CHAIN_ERROR-цепочка → due безусловно, даже при свежем валидном полном (AC1)
    [Fact]
    public void IsDue_WalChainBroken_DueUnconditionally()
    {
        // Arrange — свежий валидный COMPLETED (час назад, окно 86400), но wal=WAL_CHAIN_ERROR
        var fresh = Unix(Now.AddHours(-1).AddMinutes(-5));
        var fulls = new[]
        {
            Full("20260911110000Z", FullBackupStatus.Completed, fresh, Unix(Now.AddHours(-1)),
                verify: new BackupVerify(BackupVerifyStatus.Ok, Unix(Now.AddHours(-1)))),
        };

        // Act / Assert
        BackupPlanner.IsDue(fulls, 86400, Unix(Now),
                walChainBroken: true)
            .Should().BeTrue("a chain gap is only cured by re-shooting the full");
    }

    // AAA: transient-деградация НЕ триггерит пересъём (AC3): walChainBroken=false
    // при несвежем полном — due по возрасту, при свежем — не due
    [Fact]
    public void IsDue_WalChainBrokenFalse_FreshFull_NotDue()
    {
        // Arrange — свежий валидный COMPLETED, wal жив (ACTIVE/DEGRADED — не важно)
        var fresh = Unix(Now.AddHours(-1).AddMinutes(-5));
        var fulls = new[]
        {
            Full("20260911110100Z", FullBackupStatus.Completed, fresh, Unix(Now.AddHours(-1))),
        };

        // Act / Assert
        BackupPlanner.IsDue(fulls, 86400, Unix(Now),
                walChainBroken: false)
            .Should().BeFalse("DEGRADED due to lag/silence — transient, no reshoot is triggered");
    }

    // AAA (AC3): живой непокрытый tli-gap → due безусловно, даже при свежем
    // валидном полном (пересъём — единственное лечение RPO-зазора)
    [Fact]
    public void IsDue_WalGapUncovered_True()
    {
        // Arrange — свежий валидный COMPLETED (час назад, окно 86400), gap не покрыт
        var fresh = Unix(Now.AddHours(-1).AddMinutes(-5));
        var fulls = new[]
        {
            Full("20260911110000Z", FullBackupStatus.Completed, fresh, Unix(Now.AddHours(-1)),
                verify: new BackupVerify(BackupVerifyStatus.Ok, Unix(Now.AddHours(-1)))),
        };

        // Act / Assert
        BackupPlanner.IsDue(fulls, 86400, Unix(Now),
                walGapUncovered: true)
            .Should().BeTrue("an uncovered tli-gap is only cured by re-shooting the full");
    }

    // AAA : покрытие отключает сигнал — свежий валидный полный при
    // walGapUncovered=false — не due (шторма пересъёмов нет)
    [Fact]
    public void IsDue_WalGapCovered_FreshFull_NotDue()
    {
        // Arrange — свежий валидный COMPLETED, gap покрыт (walGapUncovered=false)
        var fresh = Unix(Now.AddHours(-1).AddMinutes(-5));
        var fulls = new[]
        {
            Full("20260911110100Z", FullBackupStatus.Completed, fresh, Unix(Now.AddHours(-1))),
        };

        // Act / Assert
        BackupPlanner.IsDue(fulls, 86400, Unix(Now),
                walGapUncovered: false)
            .Should().BeFalse("a covered gap switches the signal off — no reshoot storm");
    }

    // AAA: id — YYYYMMDDHHMMSSZ UTC; коллизия в пределах шарда → суффикс
    [Fact]
    public void NextId_Collision_SuffixIncrement()
    {
        // Arrange — id этой секунды уже занят (и -2 тоже)
        var baseId = Now.ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture) + "Z";
        var existing = new[] { baseId, baseId + "-2", "20260908030000Z" };

        // Act
        var next = BackupPlanner.NextId(existing, Now);

        // Assert
        next.Should().Be(baseId + "-3");
    }

    [Fact]
    public void NextId_NoCollision_PlainTimestamp()
    {
        // Arrange — секунда свободна
        var existing = new[] { "20260908030000Z" };

        // Act
        var next = BackupPlanner.NextId(existing, Now);

        // Assert
        next.Should().Be("20260910030000Z");
    }

    // AAA: — unix последнего валидного: COMPLETED с verify FAILED не считается,
    // finished_unix приоритетен, толерантность started_unix как в IsDue
    [Fact]
    public void LastValidUnix_LastValid_finished_Or_started()
    {
        // Arrange — старый валидный (без verify), новее — COMPLETED c verify FAILED
        var validFinished = Unix(Now.AddHours(-2));
        var fulls = new[]
        {
            Full("20260911010000Z", FullBackupStatus.Completed,
                Unix(Now.AddHours(-2).AddMinutes(-5)), validFinished),
            Full("20260911020000Z", FullBackupStatus.Completed, Unix(Now.AddHours(-1)), Unix(Now.AddHours(-1)),
                verify: new BackupVerify(BackupVerifyStatus.Failed, Unix(Now.AddHours(-1)), "bad")),
        };

        // Act / Assert — валиден только первый: его finished_unix
        BackupPlanner.LastValidUnix(fulls).Should().Be(validFinished);
    }

    // AAA: — валидного нет вовсе (только FAILED-статусы/verify) → null
    [Fact]
    public void LastValidUnix_NoValidOnes_Null()
    {
        // Arrange
        var fulls = new[]
        {
            Full("20260911020000Z", FullBackupStatus.Failed, Unix(Now.AddHours(-1))),
            Full("20260911030000Z", FullBackupStatus.Completed, Unix(Now.AddHours(-1)), Unix(Now.AddHours(-1)),
                verify: new BackupVerify(BackupVerifyStatus.Failed, Unix(Now), "bad")),
        };

        // Act / Assert
        BackupPlanner.LastValidUnix(fulls).Should().BeNull();
    }

    // AAA: — finished_unix отсутствует → толерантно started_unix (образец IsDue)
    [Fact]
    public void LastValidUnix_WithoutFinished_TakesStarted()
    {
        // Arrange — COMPLETED ещё без finished_unix (аномалия ключа)
        var started = Unix(Now.AddHours(-1));
        var fulls = new[] { Full("20260911030000Z", FullBackupStatus.Completed, started) };

        // Act / Assert
        BackupPlanner.LastValidUnix(fulls).Should().Be(started);
    }
}
