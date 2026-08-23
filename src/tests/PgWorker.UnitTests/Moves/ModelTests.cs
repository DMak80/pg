using System.Text.Json;
using PgWorker.Moves;

namespace PgWorker.UnitTests.Moves;

public class ModelTests
{
    // AAA: сериализация статуса — формат 1:1 со скриптами (spec §4.2, Д6)
    [Fact]
    public void MoveStatus_Serialize_MatchesScriptFormat()
    {
        // Arrange
        var s = new MoveStatus("bucket_42", MoveStates.Syncing, "shard1", "shard2",
            1770000000, 1770000100, "copy-wait");

        // Act
        var json = JsonDocument.Parse(s.Serialize()).RootElement;

        // Assert
        json.GetProperty("bucket").GetString().Should().Be("bucket_42");
        json.GetProperty("state").GetString().Should().Be("SYNCING");
        json.GetProperty("owner").GetString().Should().Be("shard1");
        json.GetProperty("target").GetString().Should().Be("shard2");
        json.GetProperty("started_unix").GetInt64().Should().Be(1770000000);
        json.GetProperty("updated_unix").GetInt64().Should().Be(1770000100);
        json.GetProperty("phase").GetString().Should().Be("copy-wait");
    }

    // AAA: парсинг заявки — все поля, дефолты опциональных
    [Fact]
    public void MoveRequest_Parse_FullAndMinimal()
    {
        // Arrange
        var full = """{"op":"move","to":"shard2","skip_reverse":true,"resume":true,"force":true,"requested_unix":1770000000,"requested_by":"op"}""";

        // Act
        var parsed = MoveRequest.Parse("bucket_42", full);

        // Assert
        parsed.Value!.Op.Should().Be(MoveOp.Move);
        parsed.Value.To.Should().Be("shard2");
        parsed.Value.SkipReverse.Should().BeTrue();
        parsed.Value.Resume.Should().BeTrue();
        parsed.Value.Force.Should().BeTrue();
        parsed.Value.RequestedBy.Should().Be("op");

        // Arrange
        var minimal = """{"op":"rollback","requested_unix":5}""";

        // Act
        var min = MoveRequest.Parse("bucket_7", minimal);

        // Assert
        min.Value!.Op.Should().Be(MoveOp.Rollback);
        min.Value.To.Should().BeNull();
        min.Value.Force.Should().BeFalse();
    }

    // AAA: битый/чужой JSON и неизвестный op — Result.Failed (заявка будет отвергнута)
    [Theory]
    [InlineData("not json")]
    [InlineData("""{"op":"nonsense","requested_unix":1}""")]
    public void MoveRequest_Parse_RejectsGarbage(string raw)
    {
        // Act
        var parsed = MoveRequest.Parse("bucket_42", raw);

        // Assert
        parsed.IsSuccess.Should().BeFalse("битая заявка не должна молча съедаться");
    }

    // AAA: имена ключей/артефактов — конвенция скриптов
    [Fact]
    public void MoveNames_KeysAndArtifacts_MatchScripts()
    {
        // Assert
        MoveNames.Pub("bucket_42").Should().Be("pub_bucket_42");
        MoveNames.Sub("bucket_42").Should().Be("sub_bucket_42");
        MoveNames.PubRb("bucket_42").Should().Be("pub_bucket_42_rb");
        MoveNames.SubRb("bucket_42").Should().Be("sub_bucket_42_rb");
        MoveNames.RoutingKey("shop", "bucket_42").Should().Be("/clusters/shop/buckets/routing/bucket_42");
        MoveNames.StatusKey("shop", "bucket_42").Should().Be("/clusters/shop/buckets/status/bucket_42");
        MoveNames.MoveKey("shop", "bucket_42").Should().Be("/pgworker/moves/shop/bucket_42");
        MoveNames.MovesPrefix("shop").Should().Be("/pgworker/moves/shop/");
        MoveNames.ValidateIdentifier("bucket_42").Should().BeTrue();
        MoveNames.ValidateIdentifier("B;DROP").Should().BeFalse();
    }
}
