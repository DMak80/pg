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
        new(Enabled: true, S3Bucket: "bkt",
            AgentCpu: 2, AgentMem: 4_000_000_000, RestoreRecoveryTimeoutSec: 1800);

    // AAA: спека дрилла — свой ephemeral volume с именем джоба, PGDATA
    // /drill/pgroot/data, latest (TARGET_TIME=""), бюджет recovery.
    [Fact]
    public void Build_MountsOwnDrillVolume_AndLatestTarget()
    {
        // Arrange
        var opts = Opts();

        // Act
        var spec = DrillJobSpec.Build(opts, "c1", "shard1", "id7", "20261001090000Z", ["http://m:9000"], "ak", "sk");

        // Assert
        spec.VolumeName.Should().Be(BackupNames.DrillVolumeName("c1", "shard1", "id7"));
        spec.VolumeDest.Should().Be("/drill");
        spec.Env!["PGW_RESTORE_DATA_DIR"].Should().Be("/drill");
        spec.Env!["PGW_RESTORE_PGDATA"].Should().Be("/drill/pgroot/data");
        spec.Env!["BACKUP_ID"].Should().Be("20261001090000Z");
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
        var spec = DrillJobSpec.Build(opts, "c1", "shard1", "id7", "20261001090000Z", ["http://m:9000"], "ak", "sk");

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

    // AAA: CaBundlePath задан → env несёт S3_CA_PEM значением (t42, arch/19 §7)
    [Fact]
    public void Build_CaBundle_ПередаётPemЗначением()
    {
        var caPath = Path.Combine(Path.GetTempPath(), $"ca-{Guid.NewGuid():N}.pem");
        File.WriteAllText(caPath, "-----BEGIN CERTIFICATE-----\nxyz\n-----END CERTIFICATE-----\n");

        // Act
        var spec = DrillJobSpec.Build(Opts() with { S3CaBundlePath = caPath },
            "c1", "shard1", "id7", "20261001090000Z", ["http://m:9000"], "ak", "sk");

        // Assert
        spec.Env!["S3_CA_PEM"].Should().Be("-----BEGIN CERTIFICATE-----\nxyz\n-----END CERTIFICATE-----");
    }

    // AAA: без CaBundlePath env-набор прежний — S3_CA_PEM отсутствует (юнит-диф, AC5)
    [Fact]
    public void Build_БезCaBundle_НетS3CaPem()
    {
        // Arrange / Act
        var spec = DrillJobSpec.Build(Opts(), "c1", "shard1", "id7", "20261001090000Z", ["http://m:9000"], "ak", "sk");

        // Assert
        spec.Env!.Should().NotContainKey("S3_CA_PEM");
    }

    // AAA (t41): сеть drill-джоба по режиму — контур (без extra_hosts) или
    // прод (default bridge + фиксированные host-переходы).
    [Theory]
    [InlineData(null, null, new[] { "host.docker.internal:host-gateway", "local:host-gateway" })]
    [InlineData("adminpanel-stand-net", "adminpanel-stand-net", null)]
    public void Network_ByContourMode(string? contour, string? expectedNet, string[]? expectedHosts)
    {
        // Arrange
        var opts = Opts() with { ContourNetwork = contour };

        // Act
        var spec = DrillJobSpec.Build(opts, "c1", "shard1", "id7", "20261001090000Z", ["http://m:9000"], "ak", "sk");

        // Assert
        spec.Network.Should().Be(expectedNet);
        if (expectedHosts is null)
            spec.ExtraHosts.Should().BeNull("контур-режим: extra_hosts не применяются");
        else
            spec.ExtraHosts.Should().BeEquivalentTo(expectedHosts);
    }
}
