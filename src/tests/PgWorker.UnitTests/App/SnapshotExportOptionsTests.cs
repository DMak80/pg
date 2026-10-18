using PgWorker.App;
using FluentAssertions;
using Xunit;

namespace PgWorker.UnitTests.App;

// Fail-fast включения экспорта etcd-снапшотов (t08, spec §3.3; t36 — собственная
// пара S3): Enabled=true требует полную пару PgWorker:Snapshots:Export:S3 (env
// PGW_SNAPSHOTS_S3_* — НЕ креды пулов бэкапов, arch/14 §6/§8); диапазоны —
// всегда; false — подсистема не активна (нулевое влияние, §2.10).
public class SnapshotExportOptionsTests
{
    private static SnapshotExportS3Options S3(bool full = true) => new()
    {
        Endpoint = full ? "http://owns3:9000" : "",
        Bucket = full ? "pgworker-etcd-snapshots" : "",
        AccessKey = full ? "k" : "",
        SecretKey = full ? "s" : "",
    };

    // AAA: дефолты — выключено, валидно при пустой паре (нулевое влияние)
    [Fact]
    public void DisabledByDefault_ValidWithoutS3()
    {
        // Act
        var valid = new SnapshotExportOptions().IsValid();

        // Assert
        valid.Should().BeTrue();
    }

    // AAA: Enabled=true + полная пара — валидно даже при Backups:Enabled=false
    [Fact]
    public void Enabled_WithFullPair_Valid()
    {
        // Arrange
        var options = new SnapshotExportOptions { Enabled = true, S3 = S3() };

        // Act/Assert
        options.IsValid().Should().BeTrue("the control plane does not depend on the PG backup subsystem");
    }

    // AAA: Enabled=true + пустая пара — ошибка старта (fail-fast)
    [Fact]
    public void Enabled_WithoutPair_Invalid()
    {
        // Arrange
        var options = new SnapshotExportOptions { Enabled = true, S3 = S3(full: false) };

        // Act/Assert
        options.IsValid().Should().BeFalse();
    }

    // AAA: диапазоны — всегда (мусорный конфиг виден на старте)
    [Theory]
    [InlineData(0, 300, 30)]   // RetentionObjects < 1
    [InlineData(28, 0, 30)]    // RetryIntervalSec <= 0
    [InlineData(28, 300, 0)]   // TimeoutSec <= 0
    public void Ranges_AlwaysInvalid(int retention, int retrySec, int timeoutSec)
    {
        // Arrange
        var options = new SnapshotExportOptions
            { Enabled = false, RetentionObjects = retention, RetryIntervalSec = retrySec, TimeoutSec = timeoutSec };

        // Act/Assert
        options.IsValid().Should().BeFalse();
    }
}
