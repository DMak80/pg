using PgWorker.Backups.Drill;
using PgWorker.Etcd.Parsing;

namespace PgWorker.UnitTests.Backups;

// Сериализация статуса дрилла (t02-restore-drill): JSON канона arch/19 §4 —
// ключ /pgworker/backups/<C>/<X>/drill; null-поля не пишутся (по факту).
public class DrillStatusJsonTests
{
    // AAA: сериализация статуса дрилла в формат канона arch/19 §4 —
    // обязательные state/id/backup_id/started_unix; null-поля не пишутся.
    [Fact]
    public void Serialize_Running_Minimal_WritesCoreFields()
    {
        // Arrange
        var state = new DrillState(
            "20261001120000Z", DrillStatus.Running, "20261001090000Z", 1760000000);

        // Act
        var json = DrillStatusJson.Serialize(state);

        // Assert
        json.Should().Contain(@"""state"":""RUNNING""").And.Contain(@"""id"":""20261001120000Z""")
            .And.Contain(@"""backup_id"":""20261001090000Z""").And.Contain(@"""started_unix"":1760000000");
        json.Should().NotContain("finished_unix").And.NotContain("phase")
            .And.NotContain("restored_to_lsn").And.NotContain("error");
    }

    // AAA: терминальный ключ с фазой сноса — phase+finished_unix пишутся.
    [Fact]
    public void Serialize_TerminalWithPhase_WritesPhaseAndFinished()
    {
        // Arrange
        var state = new DrillState(
            "id1", DrillStatus.Failed, "b1", 1760000000,
            FinishedUnix: 1760000300, Phase: "cleaning", Error: "boom");

        // Act
        var json = DrillStatusJson.Serialize(state);

        // Assert
        json.Should().Contain(@"""state"":""FAILED""").And.Contain(@"""phase"":""cleaning""")
            .And.Contain(@"""finished_unix"":1760000300").And.Contain(@"""error"":""boom""");
    }

    // AAA: успешный дрилл фиксирует LSN восстановления.
    [Fact]
    public void Serialize_Succeeded_WritesRestoredToLsn()
    {
        // Arrange
        var state = new DrillState(
            "id1", DrillStatus.Succeeded, "b1", 1760000000,
            FinishedUnix: 1760000300, RestoredToLsn: "0/3000028");

        // Act
        var json = DrillStatusJson.Serialize(state);

        // Assert
        json.Should().Contain(@"""state"":""SUCCEEDED""").And.Contain(@"""restored_to_lsn"":""0/3000028""");
    }

    // AAA: провалившийся дрилл несёт причину в error.
    [Fact]
    public void Serialize_Failed_WritesError()
    {
        // Arrange
        var state = new DrillState(
            "id1", DrillStatus.Failed, "b1", 1760000000,
            FinishedUnix: 1760000300, Error: "exit 1");

        // Act
        var json = DrillStatusJson.Serialize(state);

        // Assert
        json.Should().Contain(@"""state"":""FAILED""").And.Contain(@"""error"":""exit 1""");
        json.Should().NotContain("restored_to_lsn");
    }
}
