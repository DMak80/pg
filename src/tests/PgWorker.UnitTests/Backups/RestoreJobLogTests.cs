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
