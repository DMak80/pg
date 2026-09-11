using PgWorker.App;

namespace PgWorker.UnitTests.App;

// Каркас конфигурации бэкапов (arch/19 §9, t01): default выключен и валиден,
// включение требует полный S3-комплект — fail-fast старта (образец TLS
// arch/14 §2.2.1). Процессной логики в t01 нет — только options.
public class BackupsOptionsTests
{
    [Fact]
    public void Default_DisabledAndValid_PathStyleTrue()
    {
        // Arrange — дефолтная секция (appsettings без Backups).
        var options = new BackupsOptions();

        // Act / Assert — поведение воркера не меняется: подсистема выключена.
        options.Enabled.Should().BeFalse();
        options.IsValid().Should().BeTrue();
        options.S3.PathStyle.Should().BeTrue(); // клиент MinIO-режима (arch/19 §5)
    }

    [Fact]
    public void Enabled_EmptyS3_Invalid()
    {
        // Arrange — включили, но S3-комплект не задан.
        var options = new BackupsOptions { Enabled = true };

        // Act / Assert — Program.cs ValidateOnStart уронит старт.
        options.IsValid().Should().BeFalse();
    }

    [Fact]
    public void Enabled_CompleteS3_Valid()
    {
        // Arrange — полный per-install S3-комплект (env-секреты arch/19 §7).
        var options = new BackupsOptions
        {
            Enabled = true,
            S3 = new BackupsS3Options
            {
                Endpoint = "http://host.docker.internal:9000",
                Region = null,
                Bucket = "pgworker-backups",
                AccessKey = "minioadmin",
                SecretKey = "minioadmin",
            },
        };

        // Act / Assert
        options.IsValid().Should().BeTrue();
    }

    [Fact]
    public void Enabled_MissingSecretKey_Invalid()
    {
        // Arrange — частичная конфигурация: endpoint/bucket/key есть, секрета нет.
        var options = new BackupsOptions
        {
            Enabled = true,
            S3 = new BackupsS3Options
            {
                Endpoint = "http://host.docker.internal:9000",
                Bucket = "pgworker-backups",
                AccessKey = "minioadmin",
            },
        };

        // Act / Assert
        options.IsValid().Should().BeFalse();
    }

    [Fact]
    public void Defaults_JobRetry_CanonValues()
    {
        // Arrange / Act — дефолты t02 (arch/19 §9): образ джоба + бэкофф 300/3600.
        var options = new BackupsOptions();

        // Assert
        options.Job.Image.Should().Be("pgworker-backup:dev");
        options.Retry.BaseSec.Should().Be(300);
        options.Retry.MaxSec.Should().Be(3600);
    }

    [Fact]
    public void Enabled_EmptyJobImage_Invalid()
    {
        // Arrange — включили с S3-комплектом, но образ джоба не задан.
        var options = new BackupsOptions
        {
            Enabled = true,
            S3 = new BackupsS3Options
            {
                Endpoint = "http://host.docker.internal:9000",
                Bucket = "pgworker-backups",
                AccessKey = "minioadmin",
                SecretKey = "minioadmin",
            },
            Job = new BackupsJobOptions { Image = "" },
        };

        // Act / Assert — fail-fast старта (Program.cs ValidateOnStart).
        options.IsValid().Should().BeFalse();
    }

    [Fact]
    public void Defaults_PolicyStagingAgent_CanonValues()
    {
        // Arrange / Act — дефолты каркаса (арх/19 §4/§6/§9).
        var options = new BackupsOptions();

        // Assert — GFS 7/4/6, суточное окно, verify при создании; staging-
        // каталог; квота и лимиты джобов — null (без лимита, образец request_*
        // нод arch/14 §2.4 п.4).
        options.Policy.Retention.Days.Should().Be(7);
        options.Policy.Retention.Weeks.Should().Be(4);
        options.Policy.Retention.Months.Should().Be(6);
        options.Policy.FullMaxAgeSec.Should().Be(86400);
        options.Policy.VerifyOnCreate.Should().BeTrue();
        options.Staging.Dir.Should().Be("/backup-staging");
        options.Staging.QuotaBytes.Should().BeNull();
        options.Agent.Cpu.Should().BeNull();
        options.Agent.Mem.Should().BeNull();
        options.S3.Endpoint.Should().BeEmpty();
    }

    [Fact]
    public void ToRuntime_склеивает_все_секции_и_advertised_fallback()
    {
        // Arrange
        var options = new BackupsOptions
        {
            Job = new BackupsJobOptions { Image = "pgworker-backup:test" },
            S3 = new BackupsS3Options
            {
                Endpoint = "http://localhost:9000",
                AdvertisedEndpoint = "http://host.docker.internal:9000",
                Bucket = "b", AccessKey = "a", SecretKey = "s",
            },
            Staging = new BackupsStagingOptions { Dir = "/st", QuotaBytes = 1024 },
            Agent = new BackupsAgentOptions { Cpu = 0.5, Mem = 512 },
            Wal = new BackupsWalOptions { VerifyIntervalSec = 5, LagMaxSegments = 10, StaleSec = 60 },
        };

        // Act
        var runtime = options.ToRuntime();

        // Assert
        runtime.JobImage.Should().Be("pgworker-backup:test");
        runtime.AgentS3Endpoint.Should().Be("http://host.docker.internal:9000");
        runtime.WalVerifyIntervalSec.Should().Be(5);
        runtime.WalLagMaxSegments.Should().Be(10);
        runtime.WalStaleSec.Should().Be(60);
        runtime.StagingQuotaBytes.Should().Be(1024);
    }

    [Fact]
    public void ToRuntime_без_advertised_берет_endpoint_как_есть()
    {
        // Arrange
        var options = new BackupsOptions
        {
            S3 = new BackupsS3Options { Endpoint = "http://minio:9000", Bucket = "b", AccessKey = "a", SecretKey = "s" },
        };

        // Act
        var runtime = options.ToRuntime();

        // Assert
        runtime.AgentS3Endpoint.Should().Be("http://minio:9000");
        runtime.WalVerifyIntervalSec.Should().Be(30); // дефолты канона §9
        runtime.WalLagMaxSegments.Should().Be(1024);
        runtime.WalStaleSec.Should().Be(300);
    }
}
