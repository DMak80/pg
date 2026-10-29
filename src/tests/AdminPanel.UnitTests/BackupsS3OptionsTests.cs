using AdminPanel.Probes.S3;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// BackupsS3Options — конфиг-POCO без кредов и целей : цели инвентаря
// резолвятся из снапшота S3 (S3TargetsResolver → S3StorageClient.UpdateTargets);
// в options остались только bucket/region/path-style и периодика тика.
public class BackupsS3OptionsTests
{
    // AAA: значения по умолчанию — bucket пуст, path-style включён (ownS3),
    // тик 60 с, таймаут 5 с.
    [Fact]
    public void Defaults_EmptyBucket_PathStyleOn_Tick60_Timeout5()
    {
        // Arrange + Act — POCO-биндинг по умолчанию.
        var options = new BackupsS3Options();

        // Assert
        options.S3.Bucket.Should().BeEmpty();
        options.S3.Region.Should().BeNull();
        options.S3.PathStyle.Should().BeTrue();
        options.IntervalSec.Should().Be(60);
        options.TimeoutSec.Should().Be(5);
    }

    // AAA: биндинг читает bucket/region/path-style и периодику; полей кредов и
    // endpoints у S3Options нет — креды per-cluster в etcd (arch/23 §6).
    [Fact]
    public void Binding_BucketRegionPathStyle_Timings()
    {
        // Arrange + Act — явные значения конфигурации.
        var options = new BackupsS3Options
        {
            S3 = new BackupsS3Options.S3Options
            {
                Bucket = "pgworker-backups",
                Region = "us-east-1",
                PathStyle = false,
            },
            IntervalSec = 30,
            TimeoutSec = 10,
        };

        // Assert
        options.S3.Bucket.Should().Be("pgworker-backups");
        options.S3.Region.Should().Be("us-east-1");
        options.S3.PathStyle.Should().BeFalse();
        options.IntervalSec.Should().Be(30);
        options.TimeoutSec.Should().Be(10);
    }
}
