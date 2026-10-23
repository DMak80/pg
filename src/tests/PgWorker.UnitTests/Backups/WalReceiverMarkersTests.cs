using System.Text.Json;
using FluentAssertions;
using PgWorker.WalReceiver;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Stdout-JSON-маркеры протокола агента (t27, arch/19 §3.1): однострочный JSON,
// LSN в PG-формате X/Y (hex), секретов в result-маркере нет.
public class WalReceiverMarkersTests
{
    [Fact]
    public void Starting_ExactlyExpectedJson()
    {
        // Arrange / Act
        var json = WalReceiverMarkers.Starting();

        // Assert
        json.Should().Be("""{"phase":"starting"}""");
    }

    [Fact]
    public void Streaming_ContainsWalStart_InPgFormat()
    {
        // Arrange
        var lsn = (1UL << 32) | 0xABC00000;

        // Act
        var json = WalReceiverMarkers.Streaming(lsn);

        // Assert
        json.Should().Contain("\"phase\":\"streaming\"");
        json.Should().Contain("\"wal_start\":\"1/ABC00000\"");
    }

    [Fact]
    public void Delivering_ContainsSegmentName()
    {
        // Arrange
        const string segment = "000000010000000000000001";

        // Act
        var json = WalReceiverMarkers.Delivering(segment);

        // Assert
        json.Should().Contain("\"phase\":\"delivering\"");
        json.Should().Contain($"\"segment\":\"{segment}\"");
    }

    [Fact]
    public void Heartbeat_X_Y_hex_Format()
    {
        // Arrange
        var confirmed = 0x1234_0000_0000UL;

        // Act
        var json = WalReceiverMarkers.Heartbeat(confirmed);

        // Assert
        json.Should().Be("""{"heartbeat":"1234/0"}""");
    }

    [Fact]
    public void Result_ok_NoError_AndLastSegment()
    {
        // Arrange
        const string last = "000000010000000000000005";

        // Act
        var json = WalReceiverMarkers.Result(true, null, last);

        // Assert
        var doc = JsonDocument.Parse(json);
        doc.RootElement.GetProperty("ok").GetBoolean().Should().BeTrue();
        doc.RootElement.GetProperty("last_delivered").GetString().Should().Be(last);
        doc.RootElement.TryGetProperty("error", out _).Should().BeFalse();
    }

    [Fact]
    public void Result_fail_ContainsError_WithoutSecrets()
    {
        // Arrange
        const string error = "required env PG_PASSWORD is empty; required env S3_SECRET_KEY is empty";

        // Act
        var json = WalReceiverMarkers.Result(false, error, null);

        // Assert — сам текст ошибки агента секретов не содержит (значения env в
        // тексты ошибок парсером не включаются), маркер не добавляет своих
        json.Should().Contain("\"ok\":false");
        json.Should().Contain("error");
        json.Should().NotContain("secret-password");
        json.Should().NotContain("secret-key");
    }

    [Fact]
    public void TliBackfill_ContainsPhaseFromSegmentAndToTli()
    {
        // Arrange
        const string segment = "000000010000000000000003";

        // Act
        var json = WalReceiverMarkers.TliBackfill(segment, 2);

        // Assert — Contain-ассерты каждого поля (порядок полей нефункционален)
        json.Should().Contain("\"phase\":\"tli_backfill\"");
        json.Should().Contain("\"from_segment\":\"000000010000000000000003\"");
        json.Should().Contain("\"to_tli\":2");
    }

    [Fact]
    public void TliJump_ContainsPhaseBoundariesAndBothTlis()
    {
        // Arrange
        const string fromSegment = "000000010000000000000003";
        const string toSegment = "000000020000000000000007";

        // Act
        var json = WalReceiverMarkers.TliJump(fromSegment, 1, 2, toSegment);

        // Assert — Contain-ассерты каждого поля (порядок полей нефункционален)
        json.Should().Contain("\"phase\":\"tli_jump\"");
        json.Should().Contain("\"from_segment\":\"000000010000000000000003\"");
        json.Should().Contain("\"from_tli\":1");
        json.Should().Contain("\"to_tli\":2");
        json.Should().Contain("\"to_segment\":\"000000020000000000000007\"");
    }
}
