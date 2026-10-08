using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using FluentAssertions;
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
}
