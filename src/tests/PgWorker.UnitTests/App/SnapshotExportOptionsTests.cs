using PgWorker.App;
using FluentAssertions;
using Xunit;

namespace PgWorker.UnitTests.App;

// Fail-fast включения экспорта etcd-снапшотов (t08, spec §3.3): Enabled=true
// требует полный S3-комплект из Backups:S3 (Backups:Enabled НЕ требуется);
// диапазоны — всегда; false — подсистема не активна (нулевое влияние, §2.10).
public class SnapshotExportOptionsTests
{
    private static BackupsS3Options S3(bool full = true) => new()
    {
        Endpoint = full ? "http://minio:9000" : "",
        Bucket = full ? "b" : "",
        AccessKey = full ? "k" : "",
        SecretKey = full ? "s" : "",
    };

    // AAA: дефолты — выключено, валидно при пустом S3 (нулевое влияние)
    [Fact]
    public void Выключено_по_умолчанию_валидно_без_S3()
    {
        // Act
        var valid = new SnapshotExportOptions().IsValid(S3(full: false));

        // Assert
        valid.Should().BeTrue();
    }

    // AAA: Enabled=true + полный S3 — валидно даже при Backups:Enabled=false
    [Fact]
    public void Включено_с_полным_S3_валидно()
    {
        // Arrange
        var options = new SnapshotExportOptions { Enabled = true };

        // Act/Assert
        options.IsValid(S3()).Should().BeTrue("контроль-плейн не зависит от подсистемы бэкапов PG");
    }

    // AAA: Enabled=true + пустой S3-комплект — ошибка старта
    [Fact]
    public void Включено_без_S3_невалидно()
    {
        // Arrange
        var options = new SnapshotExportOptions { Enabled = true };

        // Act/Assert
        options.IsValid(S3(full: false)).Should().BeFalse();
    }

    // AAA: диапазоны — всегда (мусорный конфиг виден на старте)
    [Theory]
    [InlineData(0, 300, 30)]   // RetentionObjects < 1
    [InlineData(28, 0, 30)]    // RetryIntervalSec <= 0
    [InlineData(28, 300, 0)]   // TimeoutSec <= 0
    public void Диапазоны_невалидны_всегда(int retention, int retrySec, int timeoutSec)
    {
        // Arrange
        var options = new SnapshotExportOptions
            { Enabled = false, RetentionObjects = retention, RetryIntervalSec = retrySec, TimeoutSec = timeoutSec };

        // Act/Assert
        options.IsValid(S3(full: false)).Should().BeFalse();
    }
}
