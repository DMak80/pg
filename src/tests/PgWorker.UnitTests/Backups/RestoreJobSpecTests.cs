using PgWorker.Backups;
using PgWorker.Backups.Restore;

namespace PgWorker.UnitTests.Backups;

// Спецификация restore-джоба (t05 §3.3): data-volume первой ноды монтируется
// в /restore, env — S3-комплект + цель + пути PGDATA; никаких паролей PG.
public class RestoreJobSpecTests
{
    // AAA: спека монтирует data-volume первой ноды в /restore, env — S3-комплект + цель + пути.
    [Fact]
    public void Build_DataVolumeEnvAndLimits()
    {
        // Arrange
        var opts = new BackupsRuntimeOptions(
            Enabled: true, S3Endpoint: "http://minio:9000", S3Bucket: "bkt",
            S3AccessKey: "ak", S3SecretKey: "sk", JobImage: "pgworker-backup:dev",
            AgentCpu: 2, AgentMem: 4_000_000_000, RestoreRecoveryTimeoutSec: 77);

        // Act
        var spec = RestoreJobSpec.Build(opts, "c1", "shard1", "id9", "pgw-c1-shard1-shard1a-data",
            "2026-09-11T10:00:00Z", "srcc", "srcx");

        // Assert
        spec.Image.Should().Be("pgworker-backup:dev");
        spec.VolumeName.Should().Be("pgw-c1-shard1-shard1a-data");
        spec.VolumeDest.Should().Be("/restore");
        spec.Ports.Should().BeEmpty();
        spec.Cmd.Should().Equal(RestoreJobCommand.Build());
        spec.RestartPolicy.Should().Be("no");
        spec.ExtraHosts.Should().Equal("host.docker.internal:host-gateway", "local:host-gateway");
        spec.Env["SRC_PREFIX"].Should().Be("srcc/srcx");
        spec.Env["BACKUP_ID"].Should().Be("id9");
        spec.Env["S3_BUCKET"].Should().Be("bkt");
        spec.Env["TARGET_TIME"].Should().Be("2026-09-11T10:00:00Z");
        spec.Env["PGW_RECOVERY_TIMEOUT_SEC"].Should().Be("77");
        spec.Env["PGW_RESTORE_DATA_DIR"].Should().Be("/restore");
        // внутри тома pgroot/data (volume-корень узла /home/postgres/pgdata)
        spec.Env["PGW_RESTORE_PGDATA"].Should().Be("/restore/pgroot/data");
        spec.Env["MC_HOST_pgwbkp"].Should().Contain("ak:sk@minio:9000");
        spec.Hostname.Should().Be(BackupNames.RestoreContainerName("c1", "shard1", "id9"));
        spec.Label.Should().Be("c1");
        spec.Network.Should().BeNull();
        spec.Tmpfs.Should().BeNull();
        spec.Env.Should().NotContainKey("PGW_BK_DSN"); // никаких паролей PG
    }

    // AAA: цель latest — TARGET_TIME пустая строка (скрипт не пишет recovery_target_time).
    [Fact]
    public void Build_LatestTarget_EmptyTargetTime()
    {
        // Arrange
        var opts = new BackupsRuntimeOptions(S3Endpoint: "http://minio:9000", S3Bucket: "bkt",
            S3AccessKey: "ak", S3SecretKey: "sk");

        // Act
        var spec = RestoreJobSpec.Build(opts, "c1", "shard1", "id9", "vol", "", "srcc", "srcx");

        // Assert
        spec.Env["TARGET_TIME"].Should().BeEmpty();
    }
}
