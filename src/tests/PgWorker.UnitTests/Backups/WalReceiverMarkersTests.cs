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
    public void Starting_ровно_ожидаемый_json()
    {
        // Arrange / Act
        var json = WalReceiverMarkers.Starting();

        // Assert
        json.Should().Be("""{"phase":"starting"}""");
    }

    [Fact]
    public void Streaming_содержит_wal_start_в_PG_формате()
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
    public void Delivering_содержит_имя_сегмента()
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
    public void Heartbeat_формат_X_Y_hex()
    {
        // Arrange
        var confirmed = 0x1234_0000_0000UL;

        // Act
        var json = WalReceiverMarkers.Heartbeat(confirmed);

        // Assert
        json.Should().Be("""{"heartbeat":"1234/0"}""");
    }

    [Fact]
    public void Result_ok_без_ошибки_и_с_последним_сегментом()
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
    public void Result_fail_содержит_ошибку_без_секретов()
    {
        // Arrange
        const string error = "обязательный env PG_PASSWORD пуст; обязательный env S3_SECRET_KEY пуст";

        // Act
        var json = WalReceiverMarkers.Result(false, error, null);

        // Assert — сам текст ошибки агента секретов не содержит (значения env в
        // тексты ошибок парсером не включаются), маркер не добавляет своих
        json.Should().Contain("\"ok\":false");
        json.Should().Contain("error");
        json.Should().NotContain("secret-password");
        json.Should().NotContain("secret-key");
    }
}
