using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Backups.Supervisor;
using PgWorker.Etcd.Parsing;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Чистый отбор мусора (t07, arch/19 §4): full/<id>/ в S3 без etcd-ключа —
// мусор (восстановление не выберет — выбора кандидатов нет); с ключом
// (любой state) — не мусор.
public class SupervisorPlannerTests
{
    // AAA (AC6): full/<id>/ в S3 без etcd-ключа — мусор; с ключом (любой state) — нет
    [Fact]
    public void SelectUnownedFulls_только_без_ключа()
    {
        // Arrange
        var etcd = new[]
        {
            new FullBackupState("20260911110000Z", FullBackupStatus.Completed, "n1",
                BackupSourceRole.Replica, 1757500000, 1757500300, "s", 1, null, null),
            new FullBackupState("20260911120000Z", FullBackupStatus.Failed, "n1",
                BackupSourceRole.Replica, 1757504000, 1757504100, null, null, "err", null),
        };

        // Act
        var swept = SupervisorPlanner.SelectUnownedFulls(
            ["20260911110000Z", "20260911120000Z", "20260911130000Z", "20260911140000Z"], etcd);

        // Assert — только префиксы без etcd-владельца
        swept.Should().Equal("20260911130000Z", "20260911140000Z");
    }

    // AAA: пустой S3 — пусто; ключей нет вовсе — ВСЕ S3-id мусор (выбора нет)
    [Fact]
    public void SelectUnownedFulls_ПустыеСписки()
    {
        // Arrange
        var etcd = new[]
        {
            new FullBackupState("20260911110000Z", FullBackupStatus.Completed, "n1",
                BackupSourceRole.Replica, 1757500000, 1757500300, "s", 1, null, null),
        };

        // Act / Assert
        SupervisorPlanner.SelectUnownedFulls([], etcd).Should().BeEmpty();
        // ключей нет вовсе — единственный S3-id без владельца (мусор)
        SupervisorPlanner.SelectUnownedFulls(["20260911120000Z"], [])
            .Should().ContainSingle().Which.Should().Be("20260911120000Z");
    }
}
