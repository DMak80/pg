using PgWorker.Backups;
using PgWorker.Etcd.Parsing;
using Shared.Etcd.Client;

namespace PgWorker.UnitTests.Backups;

// Парсер статуса полного (spec t01 §3.7): поля sha256 и progress{bytes,parts}
// читаются толерантно — отсутствие/порча дают null без отбрасывания записи.
public class BackupsParserFullProgressTests
{
    private const string Key = "/pgworker/backups/c/s1/full/20260101000000Z";

    // AAA: progress{bytes,parts} + sha256 читаются в FullBackupState.
    [Fact]
    public void Parse_WithSha256AndProgress()
    {
        // Arrange
        const string raw = """
            {"state":"RUNNING","node":"n1","role":"master","started_unix":1757463600,
             "sha256":"abcdef","progress":{"bytes":4242,"parts":3}}
            """;

        // Act
        var parsed = BackupsParser.Parse(
            (IReadOnlyList<Kv>)[new Kv(Key, raw, 1)], out var errors);

        // Assert
        errors.Should().BeEmpty();
        var full = parsed.Value[0].Shards["s1"].Full.Should().ContainSingle().Subject;
        full.Sha256.Should().Be("abcdef");
        full.ProgressBytes.Should().Be(4242);
        full.ProgressParts.Should().Be(3);
    }

    // AAA: без sha256/progress поля null (терминальные состояния).
    [Fact]
    public void Parse_WithoutSha256AndProgress_Nulls()
    {
        // Arrange
        const string raw = """
            {"state":"COMPLETED","node":"n1","role":"master","started_unix":1757463600,
             "finished_unix":1757464000,"size_bytes":1024}
            """;

        // Act
        var parsed = BackupsParser.Parse(
            (IReadOnlyList<Kv>)[new Kv(Key, raw, 1)], out var errors);

        // Assert
        errors.Should().BeEmpty();
        var full = parsed.Value[0].Shards["s1"].Full.Single();
        full.Sha256.Should().BeNull();
        full.ProgressBytes.Should().BeNull();
        full.ProgressParts.Should().BeNull();
    }

    // AAA: битый progress (не объект) — поля null, запись НЕ отбрасывается.
    [Fact]
    public void Parse_BrokenProgress_KeepsRecord()
    {
        // Arrange
        const string raw = """
            {"state":"RUNNING","node":"n1","role":"master","started_unix":1757463600,
             "progress":"garbage"}
            """;

        // Act
        var parsed = BackupsParser.Parse(
            (IReadOnlyList<Kv>)[new Kv(Key, raw, 1)], out var errors);

        // Assert
        errors.Should().BeEmpty();
        var full = parsed.Value[0].Shards["s1"].Full.Should().ContainSingle().Subject;
        full.ProgressBytes.Should().BeNull();
        full.ProgressParts.Should().BeNull();
    }
}
