using FluentAssertions;
using PgWorker.Backups.EtcdExport;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Meta-JSON слепка etcd (t08, spec §3.2): полный формат с revision, пропуск
// опциональной ревизии, круговорот сериализация→парсинг, битый JSON → null.
public class EtcdExportMetaJsonTests
{
    // AAA: полный формат — revision присутствует; uploaded_unix — ФАКТИЧЕСКОЕ время
    // put'а объекта (в отличие от «покрытия» last_uploaded_unix статус-ключа, §3.2)
    [Fact]
    public void Serialize_полный_формат()
    {
        // Act
        var json = EtcdSnapshotMetaJson.Serialize(new EtcdSnapshotMeta("abc123", 2048, 42, 1759330000, 1759330001, "inst-A"));
        // Assert — snake_case, revision на месте
        json.Should().Contain("\"sha256\":\"abc123\"").And.Contain("\"size_bytes\":2048")
            .And.Contain("\"revision\":42").And.Contain("\"taken_unix\":1759330000")
            .And.Contain("\"uploaded_unix\":1759330001").And.Contain("\"instance\":\"inst-A\"");
    }

    // AAA: revision=null — поле ОПУСКАЕТСЯ (best-effort ревизии, слепок не виноват)
    [Fact]
    public void Serialize_без_ревизии_опускает_поле()
    {
        // Act
        var json = EtcdSnapshotMetaJson.Serialize(new EtcdSnapshotMeta("abc", 1, null, 2, 3, "i"));
        // Assert
        json.Should().NotContain("\"revision\"");
    }

    // AAA: парсинг 1:1; битый → null
    [Fact]
    public void Parse_круговорот_и_битый()
    {
        // Arrange
        var meta = new EtcdSnapshotMeta("abc123", 2048, null, 1759330000, 1759330001, "inst-A");
        // Act/Assert
        EtcdSnapshotMetaJson.Parse(EtcdSnapshotMetaJson.Serialize(meta)).Should().Be(meta);
        EtcdSnapshotMetaJson.Parse("{oops").Should().BeNull();
    }
}
