using PgWorker.Backups;
using PgWorker.Backups.Drill;
using PgWorker.Backups.Restore;

namespace PgWorker.UnitTests.Backups;

// Спецификация drill-джоба (t02-restore-drill, arch/19 §3.6): СВОЙ ephemeral
// volume (имя = имя контейнера), изоляция без сети/портов, механика t05 —
// latest (TARGET_TIME=""), бюджет recovery из RestoreRecoveryTimeoutSec.
public class DrillJobSpecTests
{
    private static BackupsRuntimeOptions Opts() =>
        new(Enabled: true, S3Endpoint: "http://minio:9000", S3Bucket: "bkt",
            S3AccessKey: "ak", S3SecretKey: "sk", JobImage: "pgworker-backup:dev",
            AgentCpu: 2, AgentMem: 4_000_000_000, RestoreRecoveryTimeoutSec: 1800);

    // AAA: спека дрилла — свой ephemeral volume с именем джоба, PGDATA
    // /drill/pgroot/data, latest (TARGET_TIME=""), бюджет recovery.
    [Fact]
    public void Build_MountsOwnDrillVolume_AndLatestTarget()
    {
        // Arrange
        var opts = Opts();

        // Act
        var spec = DrillJobSpec.Build(opts, "c1", "shard1", "id7");

        // Assert
        spec.VolumeName.Should().Be(BackupNames.DrillVolumeName("c1", "shard1", "id7"));
        spec.VolumeDest.Should().Be("/drill");
        spec.Env!["PGW_RESTORE_DATA_DIR"].Should().Be("/drill");
        spec.Env!["PGW_RESTORE_PGDATA"].Should().Be("/drill/pgroot/data");
        spec.Env!["BACKUP_ID"].Should().Be("id7");
        spec.Env!["SRC_PREFIX"].Should().Be("c1/shard1");
        spec.Env!["TARGET_TIME"].Should().BeEmpty();
        spec.Env!["PGW_RECOVERY_TIMEOUT_SEC"].Should().Be("1800");
        spec.Hostname.Should().Be(BackupNames.DrillContainerName("c1", "shard1", "id7"));
    }

    // AAA: изоляция — без сети, без портов, рестарт no, inline-команда, лимиты Agent.
    [Fact]
    public void Build_Isolated_NoPortsNoNetwork_RestartNo()
    {
        // Arrange
        var opts = Opts();

        // Act
        var spec = DrillJobSpec.Build(opts, "c1", "shard1", "id7");

        // Assert
        spec.Ports.Should().BeEmpty();
        spec.Network.Should().BeNull();
        spec.RestartPolicy.Should().Be("no");
        spec.ResetEntrypoint.Should().BeTrue();
        spec.CpuCores.Should().Be(opts.AgentCpu);
        spec.MemoryBytes.Should().Be(opts.AgentMem);
        spec.ExtraHosts.Should().Equal("host.docker.internal:host-gateway", "local:host-gateway");
    }

    // AAA: команда джоба — механика t05 (обёртка, решение гейта плана).
    [Fact]
    public void Command_DelegatesToRestoreJobCommand()
    {
        // Act
        var drill = DrillJobCommand.Build();
        var restore = RestoreJobCommand.Build();

        // Assert
        drill.Should().Equal(restore);
    }

    // AAA: парсер логов — обёртка RestoreJobLog (фазы downloading/recovering + result).
    [Fact]
    public void LogParses_DelegatesToRestoreJobLog()
    {
        // Arrange
        var logs = "шум pg_ctl\n{\"phase\":\"recovering\"}\n{\"ok\":true,\"restored_to_lsn\":\"0/42\"}\n";

        // Act
        var markers = DrillJobLog.Parse(logs);

        // Assert
        markers.Phase.Should().Be("recovering");
        markers.Result.Should().NotBeNull();
        markers.Result!.Ok.Should().BeTrue();
        markers.Result.RestoredToLsn.Should().Be("0/42");
    }
}
