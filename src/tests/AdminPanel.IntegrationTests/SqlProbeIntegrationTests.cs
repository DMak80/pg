using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using AdminPanel.Probes;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Npgsql;
using Xunit;

namespace AdminPanel.IntegrationTests;

// SQL-проба против живого postgres:18 (spec §9.6): каталог, слоты/лаги, ошибки,
// HA-правила на живом runtime. Хост "pg" в DSN закрывается HostMap на контейнер —
// ровно сценарий стенда (arch/04 §2.3).
public class SqlProbeIntegrationTests(PostgresFixture fixture) : IClassFixture<PostgresFixture>
{
    private static ShardInfo Shard() => new(
        "s1", "host=pg port=5432 dbname=postgres user=postgres",
        ["pg"], 5432, "postgres", "postgres", 1, null, [], null);

    private SqlProbe Probe(string? password = null)
    {
        var options = new ProbesOptions
        {
            HostMap = new Dictionary<string, string> { ["pg:5432"] = $"127.0.0.1:{fixture.Port}" },
            TimeoutSeconds = 5,
        };
        if (password is not null)
            options.Password = password;
        return new SqlProbe(Options.Create(options), TimeProvider.System);
    }

    private static ClusterInfo DemoCluster(ShardInfo shard) => new(
        "demo", "demo", 16, null, ClusterState.Active, [shard],
        [.. Enumerable.Range(0, 16).Select(i => new BucketInfo(i, "s1", BucketState.Active, null))],
        []);

    // Идемпотентный сид: контейнер один на класс (IClassFixture), Arrange зовётся
    // несколькими тестами — повторное создание слота даст 42710, поэтому guard.
    private async Task SeedSchemasAndSlotAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync(ct);
        await using var schemas = new NpgsqlCommand(
            string.Join(";", Enumerable.Range(0, 16).Select(i => $"create schema if not exists bucket_{i}")),
            connection);
        await schemas.ExecuteNonQueryAsync(ct);
        await using var slotExists = new NpgsqlCommand(
            "select 1 from pg_replication_slots where slot_name = 't06_slot'", connection);
        if (await slotExists.ExecuteScalarAsync(ct) is null)
        {
            await using var slot = new NpgsqlCommand(
                "select pg_create_logical_replication_slot('t06_slot', 'pgoutput')",
                connection);
            await slot.ExecuteScalarAsync(ct);
        }
    }

    private async Task GenerateWalAsync()
    {
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await using var table = new NpgsqlCommand(
            "create table if not exists wal_gen(payload text)", connection);
        await table.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        await using var insert = new NpgsqlCommand(
            "insert into wal_gen select repeat('x', 1000) from generate_series(1, 100)", connection);
        await insert.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task SqlProbe_ReadsCatalogFromLivePostgres()
    {
        // Arrange
        await SeedSchemasAndSlotAsync();

        // Act
        var result = await Probe().ProbeAsync(DemoCluster(Shard()), Shard(), CancellationToken.None);

        // Assert: IsInRecovery false, инвентарь 16, слот виден, реплик нет (spec §9.6).
        result.Result.Ok.Should().BeTrue();
        result.Runtime.Error.Should().BeNull();
        result.Runtime.IsInRecovery.Should().BeFalse();
        result.Runtime.BucketSchemas.Should().HaveCount(16);
        var slot = result.Runtime.Slots.Single(s => s.SlotName == "t06_slot");
        slot.SlotType.Should().Be("logical");
        slot.Active.Should().BeFalse();
        slot.WalStatus.Should().NotBeNullOrEmpty();
        result.Runtime.Standbies.Should().BeEmpty();
        result.Runtime.Subscriptions.Should().BeEmpty();
    }

    [Fact]
    public async Task SqlProbe_GeneratesWal_SlotLagGrows()
    {
        // Arrange: слот есть, WAL генерируется — подтверждённого flush нет.
        await SeedSchemasAndSlotAsync();
        var before = await Probe().ProbeAsync(DemoCluster(Shard()), Shard(), CancellationToken.None);
        await GenerateWalAsync();

        // Act
        var after = await Probe().ProbeAsync(DemoCluster(Shard()), Shard(), CancellationToken.None);

        // Assert: лаг слота появился/вырос (проводка pg_wal_lsn_diff живьём).
        var lagBefore = before.Runtime.Slots.Single(s => s.SlotName == "t06_slot").LagBytes ?? 0;
        var lagAfter = after.Runtime.Slots.Single(s => s.SlotName == "t06_slot").LagBytes;
        lagAfter.Should().BeGreaterThan(lagBefore);
    }

    [Fact]
    public async Task SqlProbe_UnreachableHost_ErrorRuntime()
    {
        // Arrange: HostMap ведёт на закрытый порт — ошибка подключения целиком на шард
        // (категория отказа spec §9.6: проверяется форма Error-runtime, не тип исключения;
        // неверный пароль на trust-стенде не отвергается сервером, поэтому недостижим).
        var options = new ProbesOptions
        {
            HostMap = new Dictionary<string, string> { ["pg:5432"] = "127.0.0.1:1" },
        };
        var probe = new SqlProbe(Options.Create(options), TimeProvider.System);

        // Act
        var result = await probe.ProbeAsync(DemoCluster(Shard()), Shard(), CancellationToken.None);

        // Assert: отказ целиком на шард — Error, списки пустые, IsInRecovery null (spec §3.7).
        result.Result.Ok.Should().BeFalse();
        result.Runtime.Error.Should().NotBeNullOrEmpty();
        result.Runtime.BucketSchemas.Should().BeEmpty();
        result.Runtime.IsInRecovery.Should().BeNull();
    }

    [Fact]
    public async Task AlertRules_OnLiveRuntime()
    {
        // Arrange: снапшот с живым runtime (без реплик, 16/16 схем) + движок t06.
        await SeedSchemasAndSlotAsync();
        var probeResult = await Probe().ProbeAsync(DemoCluster(Shard()), Shard(), CancellationToken.None);
        var runtime = probeResult.Runtime;
        var shard = Shard() with { Runtime = runtime };
        var snapshot = new EtcdSnapshot(
            DateTimeOffset.UtcNow,
            TestSnapshotEtcd(),
            [DemoCluster(shard)],
            [], [], [], [], [], 0);

        // Act
        var alerts = new AlertEngine(
        [
            new SlotWalLostRule(),
            new SlotLagHighRule(Options.Create(new AlertsOptions { ReplicaLagBytes = long.MaxValue })),
            new SyncStandbyMissingRule(),
            new InventoryMismatchRule(),
        ]).Evaluate(snapshot, null, DateTimeOffset.UtcNow, 3).ToList();

        // Assert: sync-standby-missing есть; инвентарь 16/16 — mismatch нет;
        // порог лага maxed-out — лаг-алерта нет (изоляция условий).
        alerts.Should().ContainSingle(a => a.Id == "sync-standby-missing:demo/s1");
        alerts.Should().NotContain(a => a.Kind == "inventory-mismatch");

        // Act-2: схема удалена — появляется inventory-mismatch (missing bucket_15).
        await using (var connection = new NpgsqlConnection(fixture.ConnectionString))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            await using var drop = new NpgsqlCommand("drop schema bucket_15 cascade", connection);
            await drop.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        var afterDrop = await Probe().ProbeAsync(DemoCluster(Shard()), Shard(), CancellationToken.None);
        var snapshot2 = snapshot with
        {
            Clusters = [DemoCluster(shard with { Runtime = afterDrop.Runtime })],
        };
        var alerts2 = new InventoryMismatchRule()
            .Evaluate(snapshot2, new AlertContext(null, DateTimeOffset.UtcNow, 3)).ToList();

        // Assert-2
        alerts2.Should().ContainSingle()
            .Which.Details!["missing"].Should().Be("bucket_15");
    }

    [Fact]
    public async Task AlertRules_SlotLagReproduced_LowThreshold()
    {
        // Arrange: живой лаг + заниженный порог — каталогный алерт воспроизводится
        // без генерации 16 МБ WAL (spec §16).
        await SeedSchemasAndSlotAsync();
        await GenerateWalAsync();
        var probeResult = await Probe().ProbeAsync(DemoCluster(Shard()), Shard(), CancellationToken.None);
        var shard = Shard() with { Runtime = probeResult.Runtime };
        var snapshot = new EtcdSnapshot(
            DateTimeOffset.UtcNow,
            TestSnapshotEtcd(),
            [DemoCluster(shard)],
            [], [], [], [], [], 0);

        // Act
        var alerts = new SlotLagHighRule(Options.Create(new AlertsOptions { ReplicaLagBytes = 1 }))
            .Evaluate(snapshot, new AlertContext(null, DateTimeOffset.UtcNow, 3)).ToList();

        // Assert
        alerts.Should().ContainSingle().Which.Id.Should().Be("slot-lag-high:demo/s1/t06_slot");
    }

    // Минимальный живой EtcdStatus для снапшотов правил (reachable, без alarm'ов).
    private static EtcdStatus TestSnapshotEtcd()
        => new(true, [], [], [], null, false, DateTimeOffset.UtcNow, 0);
}
