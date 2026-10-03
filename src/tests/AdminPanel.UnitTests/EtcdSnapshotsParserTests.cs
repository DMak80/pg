using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Shared.Etcd.Client;
using Xunit;

namespace AdminPanel.UnitTests;

// Парсер /pgworker/etcd-snapshots (t08, adminpanel/02 §2.3.1): полный формат,
// толерантность к отсутствующим полям, битый JSON — parseError (правила молчат).
public class EtcdSnapshotsParserTests
{
    // AAA: полный формат → модель 1:1
    [Fact]
    public void Parse_полный_ключ()
    {
        // Arrange
        var kv = new Kv("/pgworker/etcd-snapshots",
            """{"enabled":true,"state":"OK","last_uploaded_unix":1759330000,"last_object":"etcd/snapshot-20261001-120000.db","last_sha256":"abc","size_bytes":2048,"interval_min":360}""", 1);

        // Act
        var (parsed, errors) = EtcdSnapshotsParser.Parse(kv);

        // Assert
        errors.Should().BeEmpty();
        var info = parsed!; // Parse возвращает nullable-ссылку (не Nullable<T>): .Value нет
        info.Enabled.Should().BeTrue();
        info.State.Should().Be("OK");
        info.LastUploadedUnix.Should().Be(1759330000);
        info.IntervalMin.Should().Be(360);
    }

    // AAA: FAILED c error — модель несёт ошибку
    [Fact]
    public void Parse_случай_FAILED_с_ошибкой()
    {
        // Arrange
        var kv = new Kv("/pgworker/etcd-snapshots", """{"enabled":true,"state":"FAILED","error":"S3 недоступен"}""", 1);

        // Act
        var (parsed, errors) = EtcdSnapshotsParser.Parse(kv);

        // Assert
        errors.Should().BeEmpty();
        var info = parsed!;
        info.State.Should().Be("FAILED");
        info.Error.Should().Be("S3 недоступен");
        info.LastUploadedUnix.Should().BeNull();
    }

    // AAA: битый JSON — parseError-запись, модель null (правила молчат)
    [Fact]
    public void Parse_битый_json_parseError()
    {
        // Arrange
        var kv = new Kv("/pgworker/etcd-snapshots", "{не-json", 1);

        // Act
        var (info, errors) = EtcdSnapshotsParser.Parse(kv);

        // Assert
        info.Should().BeNull();
        errors.Should().ContainSingle(e => e.Key == "/pgworker/etcd-snapshots");
    }

    // AAA: ключа нет (null) — «выгрузка не включена», без ошибок
    [Fact]
    public void Parse_нет_ключа_null()
    {
        // Act
        var (info, errors) = EtcdSnapshotsParser.Parse(null);

        // Assert
        info.Should().BeNull();
        errors.Should().BeEmpty();
    }
}
