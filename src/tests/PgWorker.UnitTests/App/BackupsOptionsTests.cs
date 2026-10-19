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
        // Arrange — включённая подсистема: из per-install констант — Bucket
        // (креды пула — per-cluster secrets etcd, arch/19 §7).
        var options = new BackupsOptions
        {
            Enabled = true,
            S3 = new BackupsS3Options
            {
                Bucket = "pgworker-backups",
            },
        };

        // Act / Assert
        options.IsValid().Should().BeTrue();
    }

    // arch/19 §7: конфиг-креды (Endpoint/AccessKey/SecretKey) больше НЕ
    // влияют на валидность — креды пула читаются из /s3/clusters/<C>/secrets;
    // достаточно Bucket.
    [Fact]
    public void Enabled_BucketOnly_ValidWithoutConfigCreds()
    {
        // Arrange — включили с одним Bucket (без endpoint/кредов в конфиге).
        var options = new BackupsOptions
        {
            Enabled = true,
            S3 = new BackupsS3Options { Bucket = "pgworker-backups" },
        };

        // Act / Assert — креды из secrets-ключа пула → валидно.
        options.IsValid().Should().BeTrue();
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
                Bucket = "pgworker-backups",
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

        // Assert — GFS 1/1/1 — целевая схема (t18: последний + недельная +
        // месячная позиции), суточное окно, verify при создании; staging-каталог;
        // квота и лимиты джобов — null (без лимита, образец request_* нод
        // arch/14 §2.4 п.4).
        options.Policy.Retention.Days.Should().Be(1);
        options.Policy.Retention.Weeks.Should().Be(1);
        options.Policy.Retention.Months.Should().Be(1);
        options.Policy.FullMaxAgeSec.Should().Be(86400);
        options.Policy.VerifyOnCreate.Should().BeTrue();
        options.Staging.Dir.Should().Be("/backup-staging");
        options.Staging.QuotaBytes.Should().BeNull();
        options.Agent.Cpu.Should().BeNull();
        options.Agent.Mem.Should().BeNull();
    }

    [Fact]
    public void ToRuntime_MergesAllSections_AndAdvertisedFallback()
    {
        // Arrange — из per-install констант только bucket/CA (креды — пул и
        // его per-cluster secrets-ключ etcd).
        var options = new BackupsOptions
        {
            Job = new BackupsJobOptions { Image = "pgworker-backup:test" },
            S3 = new BackupsS3Options
            {
                Bucket = "b",
            },
            Staging = new BackupsStagingOptions { Dir = "/st", QuotaBytes = 1024 },
            Agent = new BackupsAgentOptions { Cpu = 0.5, Mem = 512 },
            Wal = new BackupsWalOptions { VerifyIntervalSec = 5, LagMaxSegments = 10, StaleSec = 60 },
        };

        // Act
        var runtime = options.ToRuntime();

        // Assert
        runtime.JobImage.Should().Be("pgworker-backup:test");
        runtime.WalVerifyIntervalSec.Should().Be(5);
        runtime.WalLagMaxSegments.Should().Be(10);
        runtime.WalStaleSec.Should().Be(60);
        runtime.StagingQuotaBytes.Should().Be(1024);
    }

    // t36: endpoint/креды в runtime не переносятся — цели даёт пул per-cluster.
    [Fact]
    public void ToRuntime_WithoutEndpointCreds_IsValid()
    {
        // Arrange
        var options = new BackupsOptions
        {
            S3 = new BackupsS3Options { Bucket = "b" },
        };

        // Act
        var runtime = options.ToRuntime();

        // Assert
        runtime.S3Bucket.Should().Be("b");
        runtime.WalVerifyIntervalSec.Should().Be(30); // дефолты канона §9
        runtime.WalLagMaxSegments.Should().Be(1024);
        runtime.WalStaleSec.Should().Be(300);
    }

    // AAA: CaBundlePath прокидывается в runtime (t42, arch/19 §7) — null по умолчанию
    [Fact]
    public void ToRuntime_Carries_CaBundlePath()
    {
        var options = new PgWorkerOptions
        {
            Backups = new BackupsOptions { S3 = new BackupsS3Options { CaBundlePath = "/tls/ca.pem" } },
        };
        options.Backups.ToRuntime().S3CaBundlePath.Should().Be("/tls/ca.pem");
    }

    [Fact]
    public void ToRuntime_WithoutCaBundle_YieldsNull()
    {
        new BackupsOptions().ToRuntime().S3CaBundlePath.Should().BeNull();
    }

    // ---- t06: ретенция/квота ----

    [Fact]
    public void Quota_WarnAboveCrit_Invalid()
    {
        // Arrange — Warn >= Crit (90/80) — мусорные пороги.
        var options = new BackupsOptions
        {
            Quota = new BackupsQuotaOptions { Bytes = 1000, WarnPercent = 90, CritPercent = 80 },
        };

        // Act / Assert — fail-fast старта.
        options.IsValid().Should().BeFalse();
    }

    [Fact]
    public void Quota_CritAbove100_Invalid()
    {
        // Arrange — Crit > 100 вне допустимого диапазона.
        var options = new BackupsOptions
        {
            Quota = new BackupsQuotaOptions { Bytes = 1000, WarnPercent = 80, CritPercent = 101 },
        };

        // Act / Assert
        options.IsValid().Should().BeFalse();
    }

    [Fact]
    public void Retention_IntervalBelow60_Invalid()
    {
        // Arrange — период ретенционного прохода короче минуты.
        var options = new BackupsOptions
        {
            Retention = new BackupsRetentionPassOptions { IntervalSec = 59, KeepFailed = 20 },
        };

        // Act / Assert
        options.IsValid().Should().BeFalse();
    }

    [Fact]
    public void KeepFailedBelow5_Invalid()
    {
        // Arrange — слишком мелкая FAILED-глубина (риск бэкофф-сдвига t02).
        var options = new BackupsOptions
        {
            Retention = new BackupsRetentionPassOptions { IntervalSec = 600, KeepFailed = 4 },
        };

        // Act / Assert
        options.IsValid().Should().BeFalse();
    }

    [Fact]
    public void Defaults_Valid_AndCanonical()
    {
        // Arrange — дефолтная секция (appsettings без ретенции/квоты).
        var options = new BackupsOptions();

        // Act / Assert — дефолты t06 (arch/19 §9) валидны и соответствуют канону.
        options.IsValid().Should().BeTrue();
        options.Retention.IntervalSec.Should().Be(600);
        options.Retention.KeepFailed.Should().Be(20);
        options.Quota.Bytes.Should().Be(0);
        options.Quota.WarnPercent.Should().Be(80);
        options.Quota.CritPercent.Should().Be(90);
    }

    [Fact]
    public void ToRuntime_CarriesRetention_AndQuota()
    {
        // Arrange — экзотические значения всех новых полей.
        var options = new BackupsOptions
        {
            Policy = new BackupsPolicyOptions
            {
                Retention = new BackupsRetentionOptions { Days = 3, Weeks = 2, Months = 1 },
                FullMaxAgeSec = 7200,
                VerifyOnCreate = false,
            },
            Retention = new BackupsRetentionPassOptions { IntervalSec = 3600, KeepFailed = 7 },
            Quota = new BackupsQuotaOptions { Bytes = 1_000_000, WarnPercent = 70, CritPercent = 95 },
        };

        // Act
        var runtime = options.ToRuntime();

        // Assert — все 8 новых полей runtime проксированы
        runtime.PolicyRetentionDays.Should().Be(3);
        runtime.PolicyRetentionWeeks.Should().Be(2);
        runtime.PolicyRetentionMonths.Should().Be(1);
        runtime.RetentionIntervalSec.Should().Be(3600);
        runtime.RetentionKeepFailed.Should().Be(7);
        runtime.QuotaBytes.Should().Be(1_000_000);
        runtime.QuotaWarnPercent.Should().Be(70);
        runtime.QuotaCritPercent.Should().Be(95);
    }

    // ---- t07: супервизор и бюджеты зависших джобов ----

    // AAA (spec §3.6): отрицательные таймауты/TTL супервизора — fail-fast старта
    [Theory]
    [InlineData(-1, 604800, 21600, 21600, 86400)]  // IntervalSec < 0
    [InlineData(600, -1, 21600, 21600, 86400)]     // OrphanTtlSec < 0
    [InlineData(600, 604800, 0, 21600, 86400)]     // FullTimeoutSec = 0
    [InlineData(600, 604800, 21600, -5, 86400)]    // VerifyTimeoutSec < 0
    [InlineData(600, 604800, 21600, 21600, 0)]     // RestoreTimeoutSec = 0
    public void IsValid_NegativeBudgets_FailFast(
        int interval, long ttl, int full, int verify, int restore)
    {
        // Arrange
        var options = new BackupsOptions
        {
            Enabled = true,
            S3 = new BackupsS3Options
            {
                Bucket = "pgworker-backups",
            },
            Supervisor = new BackupsSupervisorOptions { IntervalSec = interval, OrphanTtlSec = ttl },
        };
        options.Job.FullTimeoutSec = full;
        options.Job.VerifyTimeoutSec = verify;
        options.Job.RestoreTimeoutSec = restore;

        // Act / Assert
        options.IsValid().Should().BeFalse("negative/zero budgets are garbage config");
    }

    // AAA: OrphanTtlSec=0 — валиден (только алерт, без авто-удаления)
    [Fact]
    public void IsValid_TtlZero_Valid()
    {
        // Arrange — дефолты + TTL сирот 0 (авто-удаление выключено)
        var options = new BackupsOptions
        {
            Supervisor = new BackupsSupervisorOptions { IntervalSec = 600, OrphanTtlSec = 0 },
        };

        // Act / Assert
        options.IsValid().Should().BeTrue("OrphanTtlSec=0 — alert-only mode, the config is valid");
    }

    // AAA: дефолты t07 каноничны (arch/19 §9) и ToRuntime их проксирует
    [Fact]
    public void Supervisor_AndBudgets_Defaults_Canonical()
    {
        // Arrange
        var options = new BackupsOptions();

        // Act
        var runtime = options.ToRuntime();

        // Assert — Supervisor {600, 7 сут} + Job {6 ч / 6 ч / 24 ч}
        options.Supervisor.IntervalSec.Should().Be(600);
        options.Supervisor.OrphanTtlSec.Should().Be(604800);
        options.Job.FullTimeoutSec.Should().Be(21600);
        options.Job.VerifyTimeoutSec.Should().Be(21600);
        options.Job.RestoreTimeoutSec.Should().Be(86400);
        runtime.SupervisorIntervalSec.Should().Be(600);
        runtime.SupervisorOrphanTtlSec.Should().Be(604800);
        runtime.JobFullTimeoutSec.Should().Be(21600);
        runtime.JobVerifyTimeoutSec.Should().Be(21600);
        runtime.JobRestoreTimeoutSec.Should().Be(86400);
    }

    // ---- reliability t02: дрилл восстановимости (arch/19 §9) ----

    // AAA: Drill-секция склеивается в runtime (дефолты 1/21600)
    [Fact]
    public void ToRuntime_Drill_DefaultsOneDaySixHours()
    {
        // Arrange — дефолтная секция (appsettings без Drill).
        var options = new BackupsOptions();

        // Act
        var runtime = options.ToRuntime();

        // Assert — дефолты канона arch/19 §9.
        options.Drill.IntervalDays.Should().Be(1);
        options.Drill.TimeoutSec.Should().Be(21600);
        runtime.DrillIntervalDays.Should().Be(1);
        runtime.DrillTimeoutSec.Should().Be(21600);
    }

    // AAA: отрицательные IntervalDays/TimeoutSec — fail-fast IsValid
    [Theory]
    [InlineData(-1, 21600)]
    [InlineData(1, -5)]
    public void IsValid_NegativeDrill_Fails(int intervalDays, int timeoutSec)
    {
        // Arrange
        var options = new BackupsOptions
        {
            Drill = new BackupsDrillOptions { IntervalDays = intervalDays, TimeoutSec = timeoutSec },
        };

        // Act / Assert — Program.cs ValidateOnStart уронит старт.
        options.IsValid().Should().BeFalse("negative Drill budgets are garbage config");
    }

    // AAA: IntervalDays=0 валиден (глобальное выключение новых запусков)
    [Fact]
    public void IsValid_ZeroInterval_Valid()
    {
        // Arrange — дриллы выключены глобально; доводка активных продолжается.
        var options = new BackupsOptions
        {
            Drill = new BackupsDrillOptions { IntervalDays = 0, TimeoutSec = 21600 },
        };

        // Act / Assert
        options.IsValid().Should().BeTrue("0 is a legitimate switch-off of new runs");
        options.ToRuntime().DrillIntervalDays.Should().Be(0);
    }

    // ---- t27: Wal:AgentImage (arch/19 §3/§9) ----

    // AAA: дефолт образа wal-агента — pgworker-wal:dev (arch/19 §9)
    [Fact]
    public void Wal_AgentImage_Default_IsCanonical()
    {
        // Arrange
        var options = new BackupsOptions();

        // Act / Assert
        options.Wal.AgentImage.Should().Be("pgworker-wal:dev");
        options.ToRuntime().WalAgentImage.Should().Be("pgworker-wal:dev");
    }

    // AAA: Enabled=true с пустым AgentImage — мусорный конфиг (fail-fast старта)
    [Fact]
    public void Wal_AgentImage_Empty_Invalid()
    {
        // Arrange — включили с полным S3-комплектом, но образ агента не задан
        var options = new BackupsOptions
        {
            Enabled = true,
            S3 = new BackupsS3Options
            {
                Bucket = "pgworker-backups",
            },
            Wal = new BackupsWalOptions { AgentImage = "" },
        };

        // Act / Assert
        options.IsValid().Should().BeFalse();
    }

    // AAA: ToRuntime переносит AgentImage (пер-инсталльная настройка образа)
    [Fact]
    public void Wal_AgentImage_ToRuntime_Carries()
    {
        // Arrange
        var options = new BackupsOptions
        {
            Wal = new BackupsWalOptions { AgentImage = "pgworker-wal:e2e" },
        };

        // Act / Assert
        options.ToRuntime().WalAgentImage.Should().Be("pgworker-wal:e2e");
    }
}
