using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Etcd.Parsing;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Чистые решения бюджетов зависших джобов (t07, arch/19 §6): отбор активных
// полных-кандидатов таймаута по возрасту; COMPLETED/FAILED не в счёт.
public class SupervisionTimeoutsTests
{
    private static readonly DateTime Now = new(2026, 9, 11, 12, 0, 0, DateTimeKind.Utc);

    private static long Unix(DateTime t) => new DateTimeOffset(t).ToUnixTimeSeconds();

    private static FullBackupState Full(string id, FullBackupStatus state, long started,
        long? finished = null)
        => new(id, state, "n1", BackupSourceRole.Replica, started, finished, null, null, null, null);

    // AAA: активный полный старше бюджета — кандидат таймаута (AC5)
    [Fact]
    public void SelectTimedOut_ВозрастВышеBudgeta_ОтбираетАктивных()
    {
        // Arrange — RUNNING начат 7 ч назад (бюджет 6 ч); COMPLETED/FAILED не в счёт
        var now = Unix(Now);
        var fulls = new[]
        {
            Full("20260911060000Z", FullBackupStatus.Running, now - 7 * 3600),
            Full("20260910060000Z", FullBackupStatus.Completed, now - 40 * 3600, now - 39 * 3600),
            Full("20260911050000Z", FullBackupStatus.Failed, now - 8 * 3600, now - 7 * 3600),
        };

        // Act
        var timedOut = SupervisionTimeouts.SelectTimedOut(fulls, now, 21600);

        // Assert — только активный RUNNING
        timedOut.Select(f => f.Id).Should().Equal("20260911060000Z");
    }

    // AAA: все активные младше бюджета → пусто (ложных таймаутов нет)
    [Fact]
    public void SelectTimedOut_ВсеМладшеBudgeta_Пусто()
    {
        // Arrange — RUNNING/PLANNED начаты час назад (бюджет 6 ч)
        var now = Unix(Now);
        var fulls = new[]
        {
            Full("20260911100000Z", FullBackupStatus.Running, now - 3600),
            Full("20260911103000Z", FullBackupStatus.Planned, now - 1800),
        };

        // Act
        var timedOut = SupervisionTimeouts.SelectTimedOut(fulls, now, 21600);

        // Assert
        timedOut.Should().BeEmpty();
    }

    // AAA: IsTimedOut — строго больше (пограничный тик не карает)
    [Theory]
    [InlineData(21600, 21600, false)]   // возраст = бюджету
    [InlineData(21601, 21600, true)]    // возраст = бюджет + 1 c
    [InlineData(0, 21600, false)]       // только создан
    public void IsTimedOut_СтрогоБольше(long ageSec, long timeoutSec, bool expected)
    {
        // Arrange
        var now = Unix(Now);

        // Act / Assert
        SupervisionTimeouts.IsTimedOut(now - ageSec, now, timeoutSec).Should().Be(expected);
    }
}
