using KafkaWorker.App.Api;
using Xunit;

namespace KafkaWorker.UnitTests.App;

// Парсинг value ключа /workers/api_tls/kafkaworker (spec §3.2 п.1): JSON
// {cert_pem, key_pem} → пара PEM; битый JSON/отсутствие полей — исключение
// (fail-fast старта, тихий fallback на env маскирует проблему). Копия
// PgWorker.UnitTests/App/WorkerApiCertReaderTests (worker-agnostic ридер).
public class WorkerApiCertReaderTests
{
    [Fact]
    public void ParsePayload_ValidJson_ReturnsPair()
    {
        // Arrange
        var json = """{"cert_pem":"-----BEGIN CERTIFICATE-----\nX\n-----END CERTIFICATE-----","key_pem":"-----BEGIN PRIVATE KEY-----\nY\n-----END PRIVATE KEY-----"}""";

        // Act
        var (cert, key) = WorkerApiCertReader.ParsePayload(json);

        // Assert
        cert.Should().Contain("BEGIN CERTIFICATE");
        key.Should().Contain("BEGIN PRIVATE KEY");
    }

    [Fact]
    public void ParsePayload_BrokenJson_Throws()
    {
        // Arrange / Act / Assert
        Assert.ThrowsAny<Exception>(() => WorkerApiCertReader.ParsePayload("{not-json"));
    }

    [Fact]
    public void ParsePayload_MissingFields_Throws()
    {
        // Arrange: JSON без cert_pem — управляемый ключ обязан быть полным
        // Act / Assert
        Assert.ThrowsAny<Exception>(() => WorkerApiCertReader.ParsePayload("""{"key_pem":"Y"}"""));
    }
}
