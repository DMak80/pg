using System.Globalization;
using PgWorker.Backups;
using PgWorker.Etcd.Parsing;

namespace PgWorker.UnitTests.Backups;

// Чистые решения планировщика полных бэкапов (arch/19 §2, t02): due по
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
    public void IsDue_СвежийНоБитыйПолный_Due()
    {
        // Arrange — COMPLETED час назад (в окне), но verify FAILED
        var fulls = new[]
        {
            Full("20260911110000Z", FullBackupStatus.Completed, Unix(Now.AddHours(-1).AddMinutes(-5)),
                Unix(Now.AddHours(-1)), verify: new BackupVerify(BackupVerifyStatus.Failed, Unix(Now.AddHours(-1)), "bad")),
        };

        // Act / Assert
        BackupPlanner.IsDue(fulls, 86400, Unix(Now)).Should().BeTrue("проваленный verify свежестью не считается");
    }

    // AAA: валидный = verify null | PENDING | OK — все три дают свежесть
    [Fact]
    public void IsDue_ВсеВидыВалидных_ГасятDue()
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
    public void BackoffPassed_VerifyФейлУвеличиваетN()
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
        BackupPlanner.IsDue(fulls, 86400, Unix(Now)).Should().BeTrue();
    }

    // AAA: due — последний COMPLETED старше full_max_age_sec
    [Fact]
    public void IsDue_LastCompletedOlderThanMaxAge_True()
    {
        // Arrange — COMPLETED сутки+минуту назад при пороге 86400
        var fulls = new[] { Full("20260909025500Z", FullBackupStatus.Completed, Unix(Now.AddDays(-1).AddMinutes(-5)), Unix(Now.AddDays(-1).AddMinutes(-1))) };

        // Act / Assert
        BackupPlanner.IsDue(fulls, 86400, Unix(Now)).Should().BeTrue();
    }

    // AAA: не due — свежий COMPLETED (finished_unix внутри окна)
    [Fact]
    public void IsDue_FreshCompleted_False()
    {
        // Arrange — завершён час назад
        var fulls = new[] { Full("20260910020000Z", FullBackupStatus.Completed, Unix(Now.AddHours(-1).AddMinutes(-5)), Unix(Now.AddHours(-1))) };

        // Act / Assert
        BackupPlanner.IsDue(fulls, 86400, Unix(Now)).Should().BeFalse();
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
}
