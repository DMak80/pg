using PgWorker.Core;
using PgWorker.Moves;
using PgWorker.UnitTests.Provisioning;
using Xunit;

namespace PgWorker.UnitTests.Moves;

// MoveDdl (t01 задача 10, Д3): DDL-перенос — pg_dump --schema-only через docker
// exec в мастер-контейнере источника, применение Npgsql-батчем, гранты app-роли,
// сверка инвентаря P5. StubSql — записывающий мок (в задаче 11 станет FakeMoveSql).
public class MoveDdlTests
{
    // Записывающий мок SQL-исполнителя: все вызовы в Calls, ответы конфигурируются.
    private sealed class StubSql : IMoveSqlExecutor
    {
        public readonly List<(string Dsn, string Sql)> Calls = [];
        public Func<string, Result>? ExecuteResult { get; set; }
        public Func<string, Result<IReadOnlyList<string>>>? ListResult { get; set; }

        public Task<Result<object?>> ScalarAsync(string dsn, string sql, CancellationToken ct)
        {
            Calls.Add((dsn, sql));
            return Task.FromResult(Result<object?>.Success(null));
        }

        public Task<Result<IReadOnlyList<string>>> ListAsync(string dsn, string sql, CancellationToken ct)
        {
            Calls.Add((dsn, sql));
            return Task.FromResult(ListResult is { } f ? f(dsn) : Result<IReadOnlyList<string>>.Success([]));
        }

        public Task<Result> ExecuteAsync(string dsn, string sql, CancellationToken ct)
        {
            Calls.Add((dsn, sql));
            return Task.FromResult(ExecuteResult is { } f ? f(dsn) : Result.Success());
        }

        public Task<Result> ExecuteTransactionalAsync(string dsn, string sql, int lockTimeoutSec, CancellationToken ct)
        {
            Calls.Add((dsn, sql));
            return Task.FromResult(Result.Success());
        }
    }

    // AAA: команда pg_dump — флаги как в скрипте шага 1 (schema-only, no-owner, no-privileges)
    [Fact]
    public async Task DumpAsync_ExecsPgDumpInNodeContainer()
    {
        // Arrange
        var driver = new Fakes.FakeDriver { ExecResult = (_, cmd) => Result<string>.Success("-- ddl") };
        var ddl = new MoveDdl(driver, new StubSql());

        // Act
        var dump = await ddl.DumpAsync("shop", "shard1", "shard1a", "shop", "bucket_42", CancellationToken.None);

        // Assert
        dump.Value.Should().Be("-- ddl");
        driver.Executed.Should().ContainSingle().Which.Cmd.Should().BeEquivalentTo(
            ["su", "postgres", "-c", "pg_dump --schema-only --no-owner --no-privileges --schema=bucket_42 shop"]);
    }

    // AAA: невалидное имя бакета — исключение до docker exec (SQL/shell-инъекция)
    [Theory]
    [InlineData("B;DROP TABLE x")]
    [InlineData("bucket-42")]
    public async Task DumpAsync_RejectsInvalidBucket(string bad)
    {
        // Arrange
        var driver = new Fakes.FakeDriver { ExecResult = (_, cmd) => Result<string>.Success("-- ddl") };
        var ddl = new MoveDdl(driver, new StubSql());

        // Act
        var act = () => ddl.DumpAsync("shop", "shard1", "shard1a", "shop", bad, CancellationToken.None);

        // Assert
        await act.Should().ThrowAsync<ArgumentException>("имя бакета подставляется в shell-команду");
        driver.Executed.Should().BeEmpty("exec не должен уходить с невалидным идентификатором");
    }

    // AAA: dump-провал драйвера прокидывается (shard недоступен — transient тика)
    [Fact]
    public async Task DumpAsync_DriverFails_Propagates()
    {
        // Arrange
        var driver = new Fakes.FakeDriver
        {
            ExecResult = (_, cmd) => Result<string>.Failed(new ApplicationException("docker exec failed")),
        };
        var ddl = new MoveDdl(driver, new StubSql());

        // Act
        var dump = await ddl.DumpAsync("shop", "shard1", "shard1a", "shop", "bucket_42", CancellationToken.None);

        // Assert
        dump.IsSuccess.Should().BeFalse();
    }

    // AAA: применение DDL — батч ExecuteAsync на DSN приёмника
    [Fact]
    public async Task ApplyAsync_ExecutesBatchOnTargetDsn()
    {
        // Arrange
        var sql = new StubSql();
        var ddl = new MoveDdl(new Fakes.FakeDriver(), sql);

        // Act
        var applied = await ddl.ApplyAsync("Host=dst", "-- ddl", CancellationToken.None);

        // Assert
        applied.IsSuccess.Should().BeTrue();
        sql.Calls.Should().ContainSingle().Which.Should().Be(("Host=dst", "-- ddl"));
    }

    // AAA: гранты app-роли на приёмнике — USAGE + DML + sequences (grant_app_role)
    [Fact]
    public async Task GrantAppOnSchemaAsync_GrantsUsageDmlSequencesToApp()
    {
        // Arrange
        var sql = new StubSql();
        var ddl = new MoveDdl(new Fakes.FakeDriver(), sql);

        // Act
        var granted = await ddl.GrantAppOnSchemaAsync("Host=dst", "bucket_42", CancellationToken.None);

        // Assert
        granted.IsSuccess.Should().BeTrue();
        var call = sql.Calls.Should().ContainSingle().Subject;
        call.Dsn.Should().Be("Host=dst");
        call.Sql.Should().Contain("GRANT USAGE ON SCHEMA bucket_42 TO app");
        call.Sql.Should().Contain("GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA bucket_42 TO app");
        call.Sql.Should().Contain("GRANT USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA bucket_42 TO app");
    }

    // AAA: сверка инвентаря P5 — построчное сравнение источников списков
    [Fact]
    public async Task InventoryMatchesAsync_EqualLists_True_Differs_False()
    {
        // Arrange — на обоих DSN одинаковый инвентарь
        var sql = new StubSql
        {
            ListResult = dsn => Result<IReadOnlyList<string>>.Success(["r|items", "S|seq1"]),
        };
        var ddl = new MoveDdl(new Fakes.FakeDriver(), sql);

        // Act
        var equal = await ddl.InventoryMatchesAsync("Host=src", "Host=dst", "bucket_42", CancellationToken.None);

        // Assert
        equal.Value.Should().BeTrue("инвентарии источник/приёмник совпали");

        // Arrange — приёмник потерял sequence (дрейф P5)
        sql.ListResult = dsn => Result<IReadOnlyList<string>>.Success(
            dsn == "Host=src" ? ["r|items", "S|seq1"] : ["r|items"]);

        // Act
        var differs = await ddl.InventoryMatchesAsync("Host=src", "Host=dst", "bucket_42", CancellationToken.None);

        // Assert
        differs.Value.Should().BeFalse("неполная копия — мораторий DDL нарушен");
        sql.Calls.Should().HaveCount(4, "по два List-запроса на каждую сверку");
    }

    // AAA: сбой чтения инвентаря — Failed (transient тика), не ложное «сошлось»
    [Fact]
    public async Task InventoryMatchesAsync_SourceReadFails_Failed()
    {
        // Arrange
        var sql = new StubSql
        {
            ListResult = dsn => Result<IReadOnlyList<string>>.Failed(new ApplicationException("shard down")),
        };
        var ddl = new MoveDdl(new Fakes.FakeDriver(), sql);

        // Act
        var match = await ddl.InventoryMatchesAsync("Host=src", "Host=dst", "bucket_42", CancellationToken.None);

        // Assert
        match.IsSuccess.Should().BeFalse("недоступный шард — не «сошлось», а сбой");
    }
}
