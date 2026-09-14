using PgWorker.Backups;
using PgWorker.Backups.Job;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;

namespace PgWorker.UnitTests.Backups;

// Сериализация статуса полного в JSON канона (arch/19 §4): воркер —
// единственный писатель; round-trip через BackupsParser.
public class BackupStatusJsonTests
{
    // AAA: PLANNED без wal_start — опциональные поля отсутствуют
    [Fact]
    public void Serialize_Planned_RoundTrip()
    {
        // Arrange
        var state = new FullBackupState(
            "20260910030000Z", FullBackupStatus.Planned, "pgw-c-s1-b", BackupSourceRole.Replica,
            1757463600, null, null, null, null, null);

        // Act
        var json = BackupStatusJson.Serialize(state);
        var parsed = BackupsParser.Parse(
            (IReadOnlyList<Kv>)[new Kv(BackupNames.FullKey("c", "s1", state.Id), json, 1)], out var errors);

        // Assert — поля канона на месте, лишних нет.
        errors.Should().BeEmpty();
        var full = parsed.Value[0].Shards["s1"].Full.Should().ContainSingle().Subject;
        full.State.Should().Be(FullBackupStatus.Planned);
        full.Node.Should().Be("pgw-c-s1-b");
        full.Role.Should().Be(BackupSourceRole.Replica);
        full.StartedUnix.Should().Be(1757463600);
        full.WalStartSegment.Should().BeNull();
        full.Verify.Should().BeNull();
        json.Should().NotContain("wal_start_segment").And.NotContain("finished_unix");
    }

    // AAA: COMPLETED — все итоговые поля + verify PENDING
    [Fact]
    public void Serialize_CompletedWithVerify_RoundTrip()
    {
        // Arrange
        var state = new FullBackupState(
            "20260910030000Z", FullBackupStatus.Completed, "pgw-c-s1-a", BackupSourceRole.Master,
            1757463600, 1757464000, "000000010000000000000042", 1048576, null,
            new BackupVerify(BackupVerifyStatus.Pending, null));

        // Act
        var json = BackupStatusJson.Serialize(state);
        var parsed = BackupsParser.Parse(
            (IReadOnlyList<Kv>)[new Kv(BackupNames.FullKey("c", "s1", state.Id), json, 1)], out var errors);

        // Assert
        errors.Should().BeEmpty();
        var full = parsed.Value[0].Shards["s1"].Full.Should().ContainSingle().Subject;
        full.State.Should().Be(FullBackupStatus.Completed);
        full.FinishedUnix.Should().Be(1757464000);
        full.WalStartSegment.Should().Be("000000010000000000000042");
        full.SizeBytes.Should().Be(1048576);
        full.Verify!.State.Should().Be(BackupVerifyStatus.Pending);
        json.Should().Contain("\"state\":\"COMPLETED\"").And.Contain("\"role\":\"master\"");
    }

    // AAA: FAILED-verify сериализуется с error и checked_unix (канон arch/19 §4)
    [Fact]
    public void Serialize_VerifyFailed_ErrorИCheckedUnix()
    {
        // Arrange
        var full = new FullBackupState("20260911120000Z", FullBackupStatus.Completed, "n1",
            BackupSourceRole.Replica, 1757500000, 1757500300, "000000010000000000000001",
            1024, null, new BackupVerify(BackupVerifyStatus.Failed, 1757500600, "дыра WAL-цепочки: ожидался X, найден Y"));

        // Act
        var json = BackupStatusJson.Serialize(full);

        // Assert — verify.error присутствует, checked_unix пишется и при FAILED
        using var doc = System.Text.Json.JsonDocument.Parse(json);
        var verify = doc.RootElement.GetProperty("verify");
        verify.GetProperty("state").GetString().Should().Be("FAILED");
        verify.GetProperty("checked_unix").GetInt64().Should().Be(1757500600);
        verify.GetProperty("error").GetString().Should().Contain("дыра WAL-цепочки");
    }

    // AAA: verify без error — поля error в JSON нет (опционально, как остальные nullable)
    [Fact]
    public void Serialize_VerifyБезError_ПоляНет()
    {
        // Arrange
        var full = new FullBackupState("20260911120000Z", FullBackupStatus.Completed, "n1",
            BackupSourceRole.Replica, 1757500000, 1757500300, null, null, null,
            new BackupVerify(BackupVerifyStatus.Ok, 1757500600));

        // Act
        var json = BackupStatusJson.Serialize(full);

        // Assert
        using var doc = System.Text.Json.JsonDocument.Parse(json);
        doc.RootElement.GetProperty("verify").TryGetProperty("error", out _).Should().BeFalse();
    }

    // AAA: FAILED — error, без wal/size
    [Fact]
    public void Serialize_FailedWithError_RoundTrip()
    {
        // Arrange
        var state = new FullBackupState(
            "20260910030000Z", FullBackupStatus.Failed, "pgw-c-s1-b", BackupSourceRole.Replica,
            1757463600, 1757463700, null, null, "pg_basebackup failed: connection reset", null);

        // Act
        var json = BackupStatusJson.Serialize(state);
        var parsed = BackupsParser.Parse(
            (IReadOnlyList<Kv>)[new Kv(BackupNames.FullKey("c", "s1", state.Id), json, 1)], out var errors);

        // Assert
        errors.Should().BeEmpty();
        parsed.Value[0].Shards["s1"].Full.Should().ContainSingle()
            .Which.Error.Should().Be("pg_basebackup failed: connection reset");
    }
}
