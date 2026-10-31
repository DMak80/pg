using PgWorker.App;
using FluentAssertions;
using Xunit;

namespace PgWorker.UnitTests.App;

// Fail-fast включения экспорта etcd-снапшотов (spec §3.3; — собственная
// пара S3): Enabled=true требует полную пару PgWorker:Snapshots:Export:S3 (env
// PGW_SNAPSHOTS_S3_* — НЕ креды пулов бэкапов, arch/14 §6/§8); диапазоны —
// всегда; false — подсистема не активна (нулевое влияние, §2.10).
public class SnapshotExportOptionsTests
{
    private static SnapshotExportS3Options S3(
        string endpoints = "http://owns3:9000",
        string bucket = "pgworker-etcd-snapshots",
        string accessKey = "k",
        string secretKey = "s") => new()
    {
        Endpoints = endpoints,
        Bucket = bucket,
        AccessKey = accessKey,
        SecretKey = secretKey,
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
        var options = new SnapshotExportOptions { Enabled = true, S3 = S3(endpoints: "", bucket: "", accessKey: "", secretKey: "") };

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

    // AAA: Endpoints — запятая-список: валиден список из ≥1 непустого URI.
    [Fact]
    public void Enabled_EndpointsList_Valid()
    {
        // Arrange
        var options = new SnapshotExportOptions
        {
            Enabled = true,
            S3 = S3(endpoints: "http://a:9000,http://b:9000"),
        };

        // Act/Assert
        options.IsValid().Should().BeTrue();
    }

    // AAA: Enabled=true при списке, пустом после сплита (пустая строка или
    // одни разделители/пробелы), — невалиден: fail-fast, не молчаливый
    // экспорт «в никуда» (реалистичный случай — деплой со старым env-именем:
    // Endpoints пуст, остальные поля пары заполнены — их имена не менялись).
    [Theory]
    [InlineData("")]
    [InlineData(" , ")]
    [InlineData(",")]
    public void Enabled_EndpointsEmptyList_Invalid(string endpoints)
    {
        // Arrange — пара полная, кроме списка целей (изоляция проверки).
        var options = new SnapshotExportOptions
        {
            Enabled = true,
            S3 = S3(endpoints: endpoints),
        };

        // Act/Assert
        options.IsValid().Should().BeFalse();
    }
}
