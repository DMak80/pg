using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using FluentAssertions;
using Npgsql;
using PgWorker.Backups.Sql;
using PgWorker.IntegrationTests.Docker;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

// Слот-SQL против живого postgres (testcontainers, динамический порт): идемпотентный
// ensure + LSN-зонд — t03 spec Ф3 (шаги 3/7).
[Collection(NonE2eCollection.Name)]
public class WalSqlTests
{
    private const string Password = "pgw-test-su";

    [Fact]
    public async Task Probe_EnsureAlive_Recreate_идемпотентны_на_живом_и_отсутствующем_слоте()
    {
        // Arrange
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = new ContainerBuilder("postgres:18-alpine")
            .WithEnvironment("POSTGRES_PASSWORD", Password)
            .WithPortBinding(5432, assignRandomHostPort: true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilCommandIsCompleted(
                "pg_isready -U postgres",
                w => w.WithTimeout(TimeSpan.FromSeconds(45)))) // ≤ 100 c: падаем быстро
            .Build();
        await postgres.StartAsync(ct);
        var dsn = $"Host=localhost;Port={postgres.GetMappedPublicPort(5432)};" +
                  $"Username=postgres;Password={Password};Database=postgres;SSL Mode=Disable";
        var sql = new NpgsqlWalSqlExecutor();

        // Act
        var before = await sql.SlotProbeAsync(dsn, "pgw_bkp_test", ct);
        var ensured = await sql.EnsureSlotAliveAsync(dsn, "pgw_bkp_test", ct);
        var again = await sql.EnsureSlotAliveAsync(dsn, "pgw_bkp_test", ct);
        var probe = await sql.SlotProbeAsync(dsn, "pgw_bkp_test", ct);
        var wal = await sql.CurrentWalAsync(dsn, ct);
        // recreate на отсутствующем слоте: drop без undefined_object-ошибки → create
        var recreatedMissing = await sql.RecreateSlotAsync(dsn, "pgw_bkp_other", ct);
        var probeOther = await sql.SlotProbeAsync(dsn, "pgw_bkp_other", ct);

        // Assert
        before.Value.Exists.Should().BeFalse();
        before.Value.WalStatus.Should().BeNull("строки нет — статус отсутствует");
        ensured.IsSuccess.Should().BeTrue();
        again.IsSuccess.Should().BeTrue("повтор при живом слоте — идемпотентность");
        probe.Value.Exists.Should().BeTrue();
        probe.Value.WalStatus.Should().NotBe("lost", "свежий reserved-слот жив");
        wal.IsSuccess.Should().BeTrue();
        wal.Value.Lsn.Should().MatchRegex("^[0-9A-F]+/[0-9A-F]+$");
        wal.Value.Tli.Should().BeGreaterThanOrEqualTo(1);
        recreatedMissing.IsSuccess.Should().BeTrue("drop отсутствующего (undefined_object) — норм");
        probeOther.Value.Exists.Should().BeTrue();
    }

    [Fact]
    public async Task Lost_слот_зонд_видит_lost_EnsureSlotAlive_лечит_идемпотентно()
    {
        // Arrange — OwnPostgres: физический reserved-слот фикстуры при старте
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = await OwnPostgres.StartAsync("wal-sql-lost", ct);
        var sql = new NpgsqlWalSqlExecutor();

        // малый потолок WAL под слотом: срез при превышении + checkpoint
        await using (var conn = new NpgsqlConnection(postgres.AdminDsn))
        {
            await conn.OpenAsync(ct);
            await using var alter = new NpgsqlCommand(
                "ALTER SYSTEM SET max_slot_wal_keep_size = '16MB'", conn);
            await alter.ExecuteNonQueryAsync(ct);
            await using var reload = new NpgsqlCommand("SELECT pg_reload_conf()", conn);
            await reload.ExecuteNonQueryAsync(ct);
        }

        // Act/Assert 1 — живой слот: EnsureSlotAlive НЕ пересоздаёт (restart_lsn на месте)
        var lsnBefore = await postgres.ReadRestartLsnAsync(ct);
        (await sql.EnsureSlotAliveAsync(postgres.AdminDsn, OwnPostgres.Slot, ct))
            .IsSuccess.Should().BeTrue();
        (await postgres.ReadRestartLsnAsync(ct)).Should().Be(lsnBefore,
            "живой слот ensure-alive не трогает (spec §3.1: жив — не трогать)");

        // Act 2 — доводим слот до lost: циклы 64 MiB INSERT + pg_switch_wal + CHECKPOINT
        var lost = false;
        var budget = DateTimeOffset.UtcNow.AddSeconds(60);
        while (!lost && DateTimeOffset.UtcNow < budget)
        {
            await using var conn = new NpgsqlConnection(postgres.AdminDsn);
            await conn.OpenAsync(ct);
            await using (var create = new NpgsqlCommand(
                "CREATE TABLE IF NOT EXISTS wal_load(id bigserial, payload text)", conn))
                await create.ExecuteNonQueryAsync(ct);
            for (var i = 0; i < 4; i++)
            {
                await using var insert = new NpgsqlCommand(
                    "INSERT INTO wal_load(payload) SELECT repeat('x', 1048576) FROM generate_series(1, 16)", conn);
                await insert.ExecuteNonQueryAsync(ct);
                await using var switchWal = new NpgsqlCommand("SELECT pg_switch_wal()", conn);
                await switchWal.ExecuteScalarAsync(ct);
            }

            await using var checkpoint = new NpgsqlCommand("CHECKPOINT", conn);
            await checkpoint.ExecuteNonQueryAsync(ct);
            var probe = await sql.SlotProbeAsync(postgres.AdminDsn, OwnPostgres.Slot, ct);
            probe.IsSuccess.Should().BeTrue();
            lost = probe.Value is { Exists: true, WalStatus: "lost" };
        }

        // Assert 2 — зонд видит lost (критерий 4)
        lost.Should().BeTrue("64 MiB WAL при потолке 16 MiB + checkpoint обязаны срезать слот");

        // Act 3 — лечение: EnsureSlotAlive → recreate; повтор — идемпотентность
        (await sql.EnsureSlotAliveAsync(postgres.AdminDsn, OwnPostgres.Slot, ct))
            .IsSuccess.Should().BeTrue();
        (await sql.EnsureSlotAliveAsync(postgres.AdminDsn, OwnPostgres.Slot, ct))
            .IsSuccess.Should().BeTrue("повтор на свежем слоте — идемпотентность");

        // Assert 3 — слот жив (не lost), существует
        var healed = await sql.SlotProbeAsync(postgres.AdminDsn, OwnPostgres.Slot, ct);
        healed.Value.Exists.Should().BeTrue();
        healed.Value.WalStatus.Should().NotBe("lost", "recreate возвращает живой reserved-слот");
    }
}
