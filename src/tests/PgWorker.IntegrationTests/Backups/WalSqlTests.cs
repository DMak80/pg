using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using FluentAssertions;
using PgWorker.Backups.Sql;
using PgWorker.IntegrationTests.Docker;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

// Слот-SQL против живого postgres (testcontainers, динамический порт): идемпотентный
// ensure + LSN-зонд — t03 spec Ф3 (шаги 3/7).
public class WalSqlTests
{
    private const string Password = "pgw-test-su";

    [Fact]
    public async Task EnsureSlot_идемпотентен_CurrentWal_возвращает_lsn_и_tli()
    {
        // Arrange
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = new ContainerBuilder("postgres:17-alpine")
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
        var before = await sql.SlotExistsAsync(dsn, "pgw_bkp_test", ct);
        var created = await sql.EnsureSlotAsync(dsn, "pgw_bkp_test", ct);
        var again = await sql.EnsureSlotAsync(dsn, "pgw_bkp_test", ct);
        var exists = await sql.SlotExistsAsync(dsn, "pgw_bkp_test", ct);
        var wal = await sql.CurrentWalAsync(dsn, ct);

        // Assert
        before.Value.Should().BeFalse();
        created.IsSuccess.Should().BeTrue();
        again.IsSuccess.Should().BeTrue("повтор create при живом слоте — идемпотентность (duplicate_object)");
        exists.Value.Should().BeTrue();
        wal.IsSuccess.Should().BeTrue();
        wal.Value.Lsn.Should().MatchRegex("^[0-9A-F]+/[0-9A-F]+$");
        wal.Value.Tli.Should().BeGreaterThanOrEqualTo(1);
    }
}
