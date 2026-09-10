using PgWorker.Backups;
using PgWorker.Backups.Job;
using PgWorker.Core.Model;

namespace PgWorker.UnitTests.Backups;

// Спецификация джоба полного бэкапа (arch/19 §2, t02): детерминированные
// имена, env-контракт образа, tmpfs-квота против named volume.
public class BackupJobSpecTests
{
    private readonly BackupsRuntimeOptions _opts = new()
    {
        Enabled = true,
        S3Endpoint = "http://host.docker.internal:9000",
        S3Bucket = "pgworker-backups",
        S3AccessKey = "ak",
        S3SecretKey = "sk",
        JobImage = "pgworker-backup:dev",
        StagingDir = "/backup-staging",
    };

    // AAA: имена контейнера/volume детерминированы (takeover-супервизия)
    [Fact]
    public void Names_Deterministic()
    {
        // Arrange / Act / Assert
        BackupNames.ContainerName("demo", "s1", "20260910030000Z")
            .Should().Be("pgw-backup-full-demo-s1-20260910030000Z");
        BackupNames.VolumeName("demo", "s1", "20260910030000Z")
            .Should().Be("pgw-backup-demo-s1-20260910030000Z");
        BackupNames.FullKey("demo", "s1", "20260910030000Z")
            .Should().Be("/pgworker/backups/demo/s1/full/20260910030000Z");
    }

    // AAA: env джоба — полный контракт образа (DSN от backup_exec, S3, prefix)
    [Fact]
    public void Build_Env_DsnS3Prefix()
    {
        // Arrange — источник: advertised-хост реплики из portalloc.
        var source = new NodeAddress("host.docker.internal", new NodePorts(15432, 18432, 16932));

        // Act
        var spec = BackupJobSpec.Build(_opts, "demo", "s1", "20260910030000Z", source, "pw");

        // Assert
        spec.Image.Should().Be("pgworker-backup:dev");
        spec.Hostname.Should().Be("pgw-backup-full-demo-s1-20260910030000Z");
        spec.Ports.Should().BeEmpty("джоб — клиент без публикации портов");
        spec.Env["PGW_BK_DSN"].Should().Be(
            "host=host.docker.internal port=15432 user=backup_exec password=pw sslmode=require");
        spec.Env["PGW_BK_ID"].Should().Be("20260910030000Z");
        spec.Env["PGW_BK_S3_ENDPOINT"].Should().Be("http://host.docker.internal:9000");
        spec.Env["PGW_BK_S3_BUCKET"].Should().Be("pgworker-backups");
        spec.Env["PGW_BK_S3_ACCESS_KEY"].Should().Be("ak");
        spec.Env["PGW_BK_S3_SECRET_KEY"].Should().Be("sk");
        spec.Env["PGW_BK_PREFIX"].Should().Be("demo/s1");
        spec.Env["PGW_BK_STAGING_DIR"].Should().Be("/backup-staging");
        spec.ExtraHosts.Should().Contain("host.docker.internal:host-gateway");
        spec.Label.Should().Be("demo");
    }

    // AAA: квота задана → tmpfs size= (жёсткий ENOSPC), без named volume
    [Fact]
    public void Build_QuotaSet_TmpfsWithoutVolume()
    {
        // Arrange
        var opts = _opts with { StagingQuotaBytes = 1024 * 1024 * 1024 };

        // Act
        var spec = BackupJobSpec.Build(opts, "demo", "s1", "id1",
            new NodeAddress("h", new NodePorts(5432, 8008, 6432)), "pw");

        // Assert
        spec.Tmpfs.Should().ContainKey("/backup-staging")
            .WhoseValue.Should().Be("size=1073741824");
        spec.VolumeName.Should().BeEmpty();
    }

    // AAA: квота null → named volume на диске (реактивный ENOSPC)
    [Fact]
    public void Build_NoQuota_NamedVolume()
    {
        // Arrange / Act
        var spec = BackupJobSpec.Build(_opts, "demo", "s1", "id1",
            new NodeAddress("h", new NodePorts(5432, 8008, 6432)), "pw");

        // Assert
        spec.Tmpfs.Should().BeNull();
        spec.VolumeName.Should().Be("pgw-backup-demo-s1-id1");
        spec.VolumeDest.Should().Be("/backup-staging");
    }
}
