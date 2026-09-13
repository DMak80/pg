using PgWorker.Backups.Restore;

namespace PgWorker.UnitTests.Backups;

// Парсер stdout restore-джоба (t05 §3.3): фазы downloading/recovering + result
// JSON, шум pg_ctl/mc игнорируется (протокол t02).
public class RestoreJobLogTests
{
    // AAA: фаза + result из смеси строк шума.
    [Fact]
    public void Parse_PhasesAndResult_FromMixedNoise()
    {
        // Arrange — шум pg/mc + маркеры restore-джоба.
        var logs = """
            {"phase":"downloading"}
            mc: download of 300 objects
            {"phase":"recovering"}
            waiting for server to start.... done
            {"ok":true,"restored_to_lsn":"0/3000028"}
            """;

        // Act
        var markers = RestoreJobLog.Parse(logs);

        // Assert
        markers.Phase.Should().Be("recovering");
        markers.Result.Should().NotBeNull();
        markers.Result!.Ok.Should().BeTrue();
        markers.Result.RestoredToLsn.Should().Be("0/3000028");
        markers.Result.Error.Should().BeNull();
    }

    // AAA: провал — result ok=false с ошибкой одной строкой.
    [Fact]
    public void Parse_FailureResult()
    {
        // Arrange / Act
        var markers = RestoreJobLog.Parse("{\"ok\":false,\"error\":\"recovery budget exceeded (60 s)\"}\n");

        // Assert
        markers.Result.Should().NotBeNull();
        markers.Result!.Ok.Should().BeFalse();
        markers.Result.Error.Should().Be("recovery budget exceeded (60 s)");
    }

    // AAA: system_id (щит re-bootstrap) читается из result; старый формат без
    // поля — SystemId null.
    [Fact]
    public void Parse_SystemId_Optional()
    {
        // Arrange — новый формат джоба со system_id.
        var withSysId = RestoreJobLog.Parse(
            "{\"ok\":true,\"restored_to_lsn\":\"0/9\",\"system_id\":\"7684914368175407176\"}\n");

        // Act / Assert
        withSysId.Result!.SystemId.Should().Be("7684914368175407176");

        // Arrange — старый формат (образ до 2026-09-13).
        var legacy = RestoreJobLog.Parse("{\"ok\":true,\"restored_to_lsn\":\"0/9\"}\n");

        // Act / Assert
        legacy.Result!.SystemId.Should().BeNull();
    }

    // AAA: битый JSON-шум игнорируется, маркеров нет.
    [Fact]
    public void Parse_OnlyNoise_NoMarkers()
    {
        // Arrange / Act
        var markers = RestoreJobLog.Parse("not json\n{\"broken\": ,}\nplain text");

        // Assert
        markers.Phase.Should().BeNull();
        markers.Result.Should().BeNull();
    }
}
