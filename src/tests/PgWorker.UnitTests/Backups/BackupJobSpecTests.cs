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
        var spec = BackupJobSpec.Build(_opts, "demo", "s1", "20260910030000Z", source, "pw",
            ["http://h1:9000", "http://h2:9000"], "ak", "sk");

        // Assert
        spec.Image.Should().Be("pgworker-backup:dev");
        spec.Hostname.Should().Be("pgw-backup-full-demo-s1-20260910030000Z");
        spec.Ports.Should().BeEmpty("джоб — клиент без публикации портов");
        spec.Env!["PGW_BK_DSN"].Should().Be(
            "host=host.docker.internal port=15432 user=backup_exec password=pw sslmode=require");
        spec.Env!["PGW_BK_ID"].Should().Be("20260910030000Z");
        // t36: env несёт СПИСОК endpoints по роли источника (порядок = приоритет).
        spec.Env!["PGW_BK_S3_ENDPOINTS"].Should().Be("http://h1:9000,http://h2:9000");
        spec.Env!.Should().NotContainKey("PGW_BK_S3_ENDPOINT");
        spec.Env!["PGW_BK_S3_BUCKET"].Should().Be("pgworker-backups");
        spec.Env!["PGW_BK_S3_ACCESS_KEY"].Should().Be("ak");
        spec.Env!["PGW_BK_S3_SECRET_KEY"].Should().Be("sk");
        spec.Env!["PGW_BK_PREFIX"].Should().Be("demo/s1");
        spec.Env!["PGW_BK_STAGING_DIR"].Should().Be("/backup-staging");
        spec.ExtraHosts.Should().Contain("host.docker.internal:host-gateway")
            .And.Contain("local:host-gateway", "зарезервированное имя docker-хоста стенда");
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
            new NodeAddress("h", new NodePorts(5432, 8008, 6432)), "pw",
            ["http://h1:9000"], "ak", "sk");

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
            new NodeAddress("h", new NodePorts(5432, 8008, 6432)), "pw",
            ["http://h1:9000"], "ak", "sk");

        // Assert
        spec.Tmpfs.Should().BeNull();
        spec.VolumeName.Should().Be("pgw-backup-demo-s1-id1");
        spec.VolumeDest.Should().Be("/backup-staging");
    }

    // AAA: verify-спека — полный PGW_BK_* env-комплект (вкл. REGION) + MC_HOST_pgw
    // (креды только env), Cmd — полная замена ENTRYPOINT, лимиты Agent { Cpu, Mem },
    // портов нет, restart no (t04, arch/19 §5, spec §3.2)
    [Fact]
    public void VerifyJobSpec_EnvContract_и_Контейнер()
    {
        // Arrange
        var opts = new BackupsRuntimeOptions
        {
            Enabled = true, S3Endpoint = "http://minio:9000", S3Region = "us-east-1", S3Bucket = "bkt",
            S3AccessKey = "ak", S3SecretKey = "sk", JobImage = "pgworker-backup:dev",
            StagingDir = "/backup-staging", AgentCpu = 0.5, AgentMem = 536870912,
        };

        // Act
        var spec = VerifyJobSpec.Build(opts, "c1", "shard1", "20260911120000Z",
            ["http://minio:9000"], "ak", "sk");

        // Assert
        spec.Image.Should().Be("pgworker-backup:dev");
        // t36: env чтения — список нод пула + креды профиля.
        spec.Env![VerifyJobCommand.EnvS3Endpoints].Should().Be("http://minio:9000");
        spec.Env![VerifyJobCommand.EnvS3AccessKey].Should().Be("ak");
        spec.Env![VerifyJobCommand.EnvS3SecretKey].Should().Be("sk");
        spec.Env!["PGW_BK_S3_BUCKET"].Should().Be("bkt");
        spec.Env!["PGW_BK_PREFIX"].Should().Be("c1/shard1");
        spec.Env!["PGW_BK_ID"].Should().Be("20260911120000Z");
        spec.Env!["PGW_BK_STAGING_DIR"].Should().Be("/backup-staging");
        spec.Cmd.Should().BeEquivalentTo(VerifyJobCommand.Build(), o => o.WithStrictOrdering());
        spec.CpuCores.Should().Be(0.5, "лимиты Agent { Cpu, Mem } — как у джоба t02 (spec §3.2)");
        spec.MemoryBytes.Should().Be(536870912);
        spec.Ports.Should().BeEmpty();
        spec.RestartPolicy.Should().Be("no");
        spec.ExtraHosts.Should().BeEquivalentTo(["host.docker.internal:host-gateway", "local:host-gateway"]);
        spec.Label.Should().Be("c1");
    }

    // AAA: квота staging → tmpfs size=; без квоты → named volume
    [Fact]
    public void VerifyJobSpec_Квота_Tmpfs()
    {
        // Arrange
        var opts = new BackupsRuntimeOptions { StagingQuotaBytes = 1024, StagingDir = "/backup-staging" };

        // Act
        var withQuota = VerifyJobSpec.Build(opts, "c1", "s1", "id1", ["http://m:9000"], "ak", "sk");
        var noQuota = VerifyJobSpec.Build(opts with { StagingQuotaBytes = null }, "c1", "s1", "id1", ["http://m:9000"], "ak", "sk");

        // Assert — ENOSPC → download-phase transient (арх. §6 канона)
        withQuota.Tmpfs!["/backup-staging"].Should().Be("size=1024");
        withQuota.VolumeName.Should().BeEmpty();
        noQuota.VolumeName.Should().Be(BackupNames.VerifyVolumeName("c1", "s1", "id1"));
    }

    // AAA: CaBundlePath задан → env несёт S3_CA_PEM значением PEM (t42, arch/19 §7)
    [Fact]
    public void Build_CaBundle_ПередаётPemЗначением()
    {
        var caPath = Path.Combine(Path.GetTempPath(), $"ca-{Guid.NewGuid():N}.pem");
        File.WriteAllText(caPath, "-----BEGIN CERTIFICATE-----\nxyz\n-----END CERTIFICATE-----\n");
        var spec = BackupJobSpec.Build(_opts with { S3CaBundlePath = caPath },
            "demo", "s1", "id1", new NodeAddress("h", new NodePorts(5432, 8008, 6432)), "pw",
            ["http://h1:9000"], "ak", "sk");
        spec.Env!["S3_CA_PEM"].Should().Be("-----BEGIN CERTIFICATE-----\nxyz\n-----END CERTIFICATE-----");
    }

    // AAA: без CaBundlePath env-набор прежний — S3_CA_PEM отсутствует (юнит-диф, AC5)
    [Fact]
    public void Build_БезCaBundle_НетS3CaPem()
    {
        var spec = BackupJobSpec.Build(_opts, "demo", "s1", "id1",
            new NodeAddress("h", new NodePorts(5432, 8008, 6432)), "pw",
            ["http://h1:9000"], "ak", "sk");
        spec.Env!.Should().NotContainKey("S3_CA_PEM");
    }

    // AAA: verify-спека — CaBundlePath задан → S3_CA_PEM значением; без — нет
    [Fact]
    public void VerifyJobSpec_CaBundle_ПередаётPemЗначением()
    {
        var caPath = Path.Combine(Path.GetTempPath(), $"ca-{Guid.NewGuid():N}.pem");
        File.WriteAllText(caPath, "-----BEGIN CERTIFICATE-----\nxyz\n-----END CERTIFICATE-----\n");
        var spec = VerifyJobSpec.Build(_opts with { S3CaBundlePath = caPath }, "c1", "s1", "id1", ["http://m:9000"], "ak", "sk");
        spec.Env!["S3_CA_PEM"].Should().Be("-----BEGIN CERTIFICATE-----\nxyz\n-----END CERTIFICATE-----");
    }

    [Fact]
    public void VerifyJobSpec_БезCaBundle_НетS3CaPem()
    {
        var spec = VerifyJobSpec.Build(_opts, "c1", "s1", "id1", ["http://m:9000"], "ak", "sk");
        spec.Env!.Should().NotContainKey("S3_CA_PEM");
    }

    // AAA (t41): сеть джоба полного по режиму — контур (без extra_hosts,
    // адресация по именам сети) или прод (default bridge + фиксированные
    // extra_hosts).
    [Theory]
    [InlineData(null, null, new[] { "host.docker.internal:host-gateway", "local:host-gateway" })]
    [InlineData("adminpanel-stand-net", "adminpanel-stand-net", null)]
    public void Network_ByContourMode(string? contour, string? expectedNet, string[]? expectedHosts)
    {
        // Arrange
        var opts = _opts with { ContourNetwork = contour };

        // Act
        var spec = BackupJobSpec.Build(opts, "demo", "shard1", "20260911a",
            new NodeAddress("shard1a", new NodePorts(5432, 8008, 6432)), "pwd",
            ["http://h1:9000"], "ak", "sk");

        // Assert
        spec.Network.Should().Be(expectedNet);
        if (expectedHosts is null)
            spec.ExtraHosts.Should().BeNull("контур-режим: extra_hosts не применяются");
        else
            spec.ExtraHosts.Should().BeEquivalentTo(expectedHosts);
    }

    // AAA (t41): verify-джоб — та же таблица режимов сети/extra_hosts.
    [Theory]
    [InlineData(null, null, new[] { "host.docker.internal:host-gateway", "local:host-gateway" })]
    [InlineData("adminpanel-stand-net", "adminpanel-stand-net", null)]
    public void VerifyNetwork_ByContourMode(string? contour, string? expectedNet, string[]? expectedHosts)
    {
        // Arrange
        var opts = new BackupsRuntimeOptions { ContourNetwork = contour };

        // Act
        var spec = VerifyJobSpec.Build(opts, "c1", "shard1", "id1", ["http://m:9000"], "ak", "sk");

        // Assert
        spec.Network.Should().Be(expectedNet);
        if (expectedHosts is null)
            spec.ExtraHosts.Should().BeNull("контур-режим: extra_hosts не применяются");
        else
            spec.ExtraHosts.Should().BeEquivalentTo(expectedHosts);
    }
}
