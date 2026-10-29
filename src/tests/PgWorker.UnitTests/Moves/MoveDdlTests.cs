using PgWorker.Backups;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Moves;
using PgWorker.Provisioning.Endpoints;
using Xunit;

namespace PgWorker.UnitTests.Moves;

// MoveDdl (t01 задача 10, Д3): DDL-перенос — pg_dump --schema-only командой
// приёмнику ноды-источника (канал arch/25 №22), применение Npgsql-батчем,
// гранты app-роли, сверка инвентаря P5. StubSql — записывающий мок.
public class MoveDdlTests
{
    // Записывающий мок клиента приёмника: адрес и тело pgdump-вызова в Calls.
    private sealed class StubReceiver : IWalReceiverApiClient
    {
        public readonly List<(string Url, StartPgDumpRequest Body)> Dumps = [];
        public Func<StartPgDumpRequest, NodeReply<PgDumpResponse>>? DumpResult { get; set; }

        public Task<NodeReply<PgDumpResponse>> PgDumpAsync(string baseUrl, StartPgDumpRequest body, CancellationToken ct)
        {
            Dumps.Add((baseUrl, body));
            return Task.FromResult(DumpResult is { } f ? f(body) : new NodeReply<PgDumpResponse>(200, new PgDumpResponse("-- ddl"), null));
        }

        public Task<NodeReply<HbaEnsureResponse>> EnsureHbaAsync(string baseUrl, CancellationToken ct)
            => Task.FromResult(new NodeReply<HbaEnsureResponse>(200, new HbaEnsureResponse(false, false), null));

        public Task<NodeReply<PgDataResponse>> GetPgDataAsync(string baseUrl, CancellationToken ct)
            => Task.FromResult(new NodeReply<PgDataResponse>(200, new PgDataResponse("present"), null));

        public Task<ReceiverReply> StartFullAsync(string baseUrl, string id, CancellationToken ct)
            => Task.FromResult(new ReceiverReply(202, null, null));
        public Task<ReceiverReply> GetFullAsync(string baseUrl, CancellationToken ct)
            => Task.FromResult(new ReceiverReply(404, null, null));
        public Task<ReceiverReply> CancelFullAsync(string baseUrl, CancellationToken ct)
            => Task.FromResult(new ReceiverReply(202, null, null));
        public Task<ReceiverReply> StartVerifyAsync(string baseUrl, StartVerifyRequest body, CancellationToken ct)
            => Task.FromResult(new ReceiverReply(202, null, null));
        public Task<ReceiverReply> GetVerifyAsync(string baseUrl, CancellationToken ct)
            => Task.FromResult(new ReceiverReply(404, null, null));
        public Task<ReceiverReply> StartRestoreAsync(string baseUrl, StartRestoreRequest body, CancellationToken ct)
            => Task.FromResult(new ReceiverReply(202, null, null));
        public Task<ReceiverReply> GetRestoreAsync(string baseUrl, CancellationToken ct)
            => Task.FromResult(new ReceiverReply(404, null, null));
        public Task<ReceiverReply> CancelOpsAsync(string baseUrl, CancellationToken ct)
            => Task.FromResult(new ReceiverReply(202, null, null));
    }

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

    // AAA: pgdump-команда приёмнику ноды-источника — адрес по имени ноды
    // (pgw-<C>-<X>-<n>:8080), схема и БД — в теле (канон arch/25 №22)
    [Fact]
    public async Task DumpAsync_RequestsReceiverOfSourceNode()
    {
        // Arrange
        var receiver = new StubReceiver();
        var ddl = new MoveDdl(receiver, new StubSql());

        // Act
        var dump = await ddl.DumpAsync("shop", "shard1", "shard1a", "shop", "bucket_42", CancellationToken.None);

        // Assert
        dump.IsSuccess.Should().BeTrue();
        dump.Body!.Ddl.Should().Be("-- ddl");
        receiver.Dumps.Should().ContainSingle();
        receiver.Dumps[0].Url.Should().Be("http://pgw-shop-shard1-shard1a:8080");
        receiver.Dumps[0].Body.Schema.Should().Be("bucket_42");
        receiver.Dumps[0].Body.DbName.Should().Be("shop");
    }

    // AAA (усвоение §5 J): object-нода в portalloc — адрес приёмника по имени
    // её контейнера (WalReceiverAddress.ForNode)
    [Fact]
    public void HasAdoptedNodes_MixedShard_True()
    {
        // Arrange: в шарде есть object-нода.
        var addresses = new Dictionary<string, NodeAddress>
        {
            ["s2/s2a"] = new("local", new NodePorts(5435, 8021, 0), "as-s2a"),
            ["s2/s2b"] = new("local", new NodePorts(5436, 8022, 0)),
        };

        // Act / Assert
        ShardEndpoints.HasAdoptedNodes("s2", addresses).Should().BeTrue();
        ShardEndpoints.HasAdoptedNodes("s1", addresses).Should().BeFalse();
    }

    // AAA: невалидное имя бакета — исключение до HTTP-вызова (SQL/shell-инъекция)
    [Theory]
    [InlineData("B;DROP TABLE x")]
    [InlineData("bucket-42")]
    public async Task DumpAsync_RejectsInvalidBucket(string bad)
    {
        // Arrange
        var receiver = new StubReceiver();
        var ddl = new MoveDdl(receiver, new StubSql());

        // Act
        var act = () => ddl.DumpAsync("shop", "shard1", "shard1a", "shop", bad, CancellationToken.None);

        // Assert
        await act.Should().ThrowAsync<ArgumentException>("the bucket name is substituted into the dump request");
        receiver.Dumps.Should().BeEmpty("invalid identifiers must not reach the receiver");
    }

    // AAA: psql-метакоманды (\restrict/\unrestrict — pg_dump PG17.2+/18) вырезаются
    // перед применением: серверный протокол их не понимает (42601)
    [Fact]
    public async Task ApplyAsync_StripsPsqlMetaCommands()
    {
        // Arrange — дамп PG18 с охранными метакомандами pg_dump
        var sql = new StubSql();
        var ddl = new MoveDdl(new StubReceiver(), sql);
        var dump = """
            \restrict 01f0f4c6
            SET statement_timeout = 0;
            CREATE SCHEMA bucket_42;
            \unrestrict 01f0f4c6
            """;

        // Act
        var apply = await ddl.ApplyAsync("dsn", dump, CancellationToken.None);

        // Assert
        apply.IsSuccess.Should().BeTrue();
        var sent = sql.Calls.Should().ContainSingle().Subject.Sql;
        sent.Should().NotContain("\\restrict", "psql meta-commands must not reach the SQL batch");
        sent.Should().NotContain("\\unrestrict", "psql meta-commands must not reach the SQL batch");
        sent.Should().Contain("CREATE SCHEMA bucket_42;", "the dump SQL is preserved");
    }

    // AAA: pgdump-провал приёмника — неуспех (transient тика), DDL не применён
    [Fact]
    public async Task DumpAsync_ReceiverFails_Propagates()
    {
        // Arrange
        var receiver = new StubReceiver
        {
            DumpResult = _ => new NodeReply<PgDumpResponse>(500, null, "pg_dump failed"),
        };
        var ddl = new MoveDdl(receiver, new StubSql());

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
        var ddl = new MoveDdl(new StubReceiver(), sql);

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
        var ddl = new MoveDdl(new StubReceiver(), sql);

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
        var ddl = new MoveDdl(new StubReceiver(), sql);

        // Act
        var equal = await ddl.InventoryMatchesAsync("Host=src", "Host=dst", "bucket_42", CancellationToken.None);

        // Assert
        equal.Value.Should().BeTrue("source/target inventories matched");

        // Arrange — приёмник потерял sequence (дрейф P5)
        sql.ListResult = dsn => Result<IReadOnlyList<string>>.Success(
            dsn == "Host=src" ? ["r|items", "S|seq1"] : ["r|items"]);

        // Act
        var differs = await ddl.InventoryMatchesAsync("Host=src", "Host=dst", "bucket_42", CancellationToken.None);

        // Assert
        differs.Value.Should().BeFalse("an incomplete copy — the DDL moratorium is violated");
        sql.Calls.Should().HaveCount(4, "two List queries per verification");
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
        var ddl = new MoveDdl(new StubReceiver(), sql);

        // Act
        var match = await ddl.InventoryMatchesAsync("Host=src", "Host=dst", "bucket_42", CancellationToken.None);

        // Assert
        match.IsSuccess.Should().BeFalse("an unreachable shard is not \"matched\" but a failure");
    }
}
