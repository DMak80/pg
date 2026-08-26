using Microsoft.Extensions.Logging;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Etcd.Coordination;
using PgWorker.Moves;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Probes;
using PgWorker.UnitTests.Provisioning;

namespace PgWorker.UnitTests.Moves;

// Тест-даблы процессов переезда (t01 задача 11): записывающий мок SQL-исполнителя
// с резолвером по тексту SQL + общий стенд MoveProcess-тестов (задачи 11/13).

/// <summary>
/// Мок IMoveSqlExecutor: все вызовы в Calls (dsn, sql); ответы конфигурируются
/// резолверами по тексту SQL. Резолвер при необходимости смотрит Calls-контекст
/// (LastDsn) — пробы mover-роли ключуются по DSN (mover-DSN отличается от admin-DSN).
/// Исключение из ScalarResolver/ListResolver → Result.Failed (имитация недоступности).
/// </summary>
internal sealed class FakeMoveSql : IMoveSqlExecutor
{
    public readonly List<(string Dsn, string Sql)> Calls = [];

    // Ответ по тексту SQL (по плану); для ключования по шарду — LastDsn в замыкании теста.
    public Func<string, object?> ScalarResolver { get; set; } = _ => null;

    public Func<string, Result>? ExecuteResult { get; set; }

    // Транзакция заморозки (CutoverSequence, задача 12): по тексту SQL.
    public Func<string, Result>? TransactionalResult { get; set; }

    public Func<string, IReadOnlyList<string>> ListResolver { get; set; } = _ => [];

    // DSN последнего вызова (записывается ДО вызова резолвера — доступен в его замыкании).
    public string LastDsn => Calls.Count > 0 ? Calls[^1].Dsn : string.Empty;

    public Task<Result<object?>> ScalarAsync(string dsn, string sql, CancellationToken ct)
    {
        Calls.Add((dsn, sql));
        return Task.FromResult(Result<object?>.From(() => ScalarResolver(sql)));
    }

    public Task<Result<IReadOnlyList<string>>> ListAsync(string dsn, string sql, CancellationToken ct)
    {
        Calls.Add((dsn, sql));
        return Task.FromResult(Result<IReadOnlyList<string>>.From(() => ListResolver(sql)));
    }

    public Task<Result> ExecuteAsync(string dsn, string sql, CancellationToken ct)
    {
        Calls.Add((dsn, sql));
        return Task.FromResult(ExecuteResult is { } f ? f(sql) : Result.Success());
    }

    public Task<Result> ExecuteTransactionalAsync(string dsn, string sql, int lockTimeoutSec, CancellationToken ct)
    {
        Calls.Add((dsn, sql));
        return Task.FromResult(TransactionalResult is { } f ? f(sql) : Result.Success());
    }
}

/// <summary>
/// Общий стенд MoveProcess-тестов: кластер shop из двух шардов (bucket_42 живёт на
/// shard1, заявки едут на shard2), portalloc/master-ключи для ShardEndpoints,
/// реальный ClaimStore/WorkJournal поверх FakeEtcd, зелёный префлайт-резолвер.
/// </summary>
internal static class MoveRig
{
    public const string Ep = "http://etcd:2379";

    public static readonly InstallSecrets Secrets = new("su-pw", "sb-pw", "app-pw", "adm-pw", "mov-pw");

    // DSN стенда: админ (postgres) источника/приёмника и mover-пробы (bucket_mover).
    public const string SrcDsn = "Host=h1;Port=15000;Database=shop;Username=postgres;Password=su-pw;SSL Mode=Require;Trust Server Certificate=true";
    public const string DstDsn = "Host=h1;Port=15002;Database=shop;Username=postgres;Password=su-pw;SSL Mode=Require;Trust Server Certificate=true";
    // Mover-DSN: multi-host с разными портами — парами host:port (Npgsql не
    // принимает список портов в Port=; см. ShardEndpoints.MoverNpgsqlDsn);
    // Target Session Attributes=read-write — целимся в писателя источника.
    public const string MoverDsn = "Host=h1:15000,h2:15001;Database=shop;Username=bucket_mover;Password=mov-pw;SSL Mode=Require;Trust Server Certificate=true;Target Session Attributes=read-write";
    public const string DstDsnKey = "host=h1,h2 port=15002,15003 dbname=shop user=bucket_admin";

    public static ClusterSnapshot Snap() => new(
        new ClusterConfig("shop", 6, "shop", 1755900000, ClusterState.Active),
    [
        Shard("shard1", "host=h1,h2 port=15000,15001 dbname=shop user=bucket_admin", "shard1a:18000"),
        Shard("shard2", DstDsnKey, "shard2a:18002"),
    ],
        [new BucketRoute(42, "shard1", null)]);

    private static ShardSpec Shard(string name, string dsn, string master) => new(
        name, 2, dsn, master,
        [new NodeSpec(name, name + "a", NodeState.Running), new NodeSpec(name, name + "b", NodeState.Running)]);

    public static void SeedTopology(Fakes.FakeEtcd etcd)
    {
        etcd.Seed("/pgworker/portalloc/shop", Portalloc.Serialize(new Dictionary<string, NodeAddress>
        {
            ["shard1/shard1a"] = new("h1", new NodePorts(15000, 18000, 16500)),
            ["shard1/shard1b"] = new("h2", new NodePorts(15001, 18001, 16501)),
            ["shard2/shard2a"] = new("h1", new NodePorts(15002, 18002, 16502)),
            ["shard2/shard2b"] = new("h2", new NodePorts(15003, 18003, 16503)),
        }));
        etcd.Seed("/clusters/shop/shards/shard1/master", "shard1a:18000");
        etcd.Seed("/clusters/shop/shards/shard2/master", "shard2a:18002");
        etcd.Seed(MoveNames.RoutingKey("shop", "bucket_42"), "shard1");
    }

    public static void SeedMoveRequest(Fakes.FakeEtcd etcd, string json =
        """{"op":"move","to":"shard2","requested_unix":100}""")
        => etcd.Seed(MoveNames.MoveKey("shop", "bucket_42"), json);

    /// <summary>Конфигурация зелёного префлайт-резолвера (отличия от дефолта — параметрами).</summary>
    internal sealed record PreflightSql(
        string WalLevel = "logical",
        bool SchemaOnSource = true,
        bool SchemaOnDst = false,
        bool MoverRoleOk = true,
        string SyncStandbyNames = "ANY 1 (shard2b)",
        long SyncStandbyCount = 1,
        bool SrcAdminDown = false,
        bool DstAdminDown = false,
        bool MoverDown = false,
        string? EmptySchemaGen = null,
        long EmptySchemaRows = 0,
        string SubSyncReady = "3/3",
        long SubOnDstCount = 0,
        IReadOnlyList<string>? InventorySrc = null,
        IReadOnlyList<string>? InventoryDst = null);

    // Зелёный резолвер M0/M1–M3: wal_level=logical, слоты свободны, mover-роль с
    // REPLICATION, sync-standby приёмника жив, схема есть на источнике/нет на приёмнике.
    public static FakeMoveSql SqlOf(PreflightSql? p = null)
    {
        p ??= new PreflightSql();
        var fake = new FakeMoveSql();
        fake.ScalarResolver = sql =>
        {
            var dsn = fake.LastDsn;
            if (p.SrcAdminDown && dsn == SrcDsn)
                throw new ApplicationException("shard1 (admin) недоступен");
            if (p.DstAdminDown && dsn == DstDsn)
                throw new ApplicationException("shard2 (admin) недоступен");
            if (p.MoverDown && dsn == MoverDsn)
                throw new ApplicationException("shard1 (mover) недоступен");

            return sql switch
            {
                // Схема на источнике/приёмнике (admin-DSN различаются портом мастера).
                var s when s.Contains("to_regnamespace") => dsn == SrcDsn ? p.SchemaOnSource : p.SchemaOnDst,
                var s when s.Contains("name = 'wal_level'") => p.WalLevel,
                var s when s.Contains("max_replication_slots") => 10L,
                var s when s.Contains("max_wal_senders") => 10L,
                var s when s.Contains("pg_replication_slots") => 0L, // занятые + lost
                var s when s.Contains("pg_stat_replication") => p.SyncStandbyCount,
                var s when s.Contains("rolsuper") => p.MoverRoleOk,
                var s when s.Contains("synchronous_standby_names") => p.SyncStandbyNames,
                var s when s.Contains("pg_publication") => 0L,
                // Готовность подписки (M3): «ready/total» — до ветки pg_subscription
                // (SubSyncReady тоже содержит FROM pg_subscription).
                var s when s.Contains("srsubstate") => p.SubSyncReady,
                // Остатки прошлого переезда (_rb) — всегда чисты; прямая подписка — на приёмнике.
                var s when s.Contains("pub_bucket_42_rb") => 0L,
                var s when s.Contains("sub_bucket_42_rb") => 0L,
                var s when s.Contains("pg_subscription") => p.SubOnDstCount,
                // Проверка пустоты схемы приёмника: первый скаляр — генератор, второй — сумма строк.
                var s when s.Contains("coalesce(string_agg('(SELECT count(*)") => p.EmptySchemaGen switch
                {
                    null => throw new ApplicationException("схема приёмника не пустая — генератор не звался"),
                    var gen => gen,
                },
                var s when s.Contains("(SELECT count(*)") => p.EmptySchemaRows,
                _ => 1L, // SELECT 1 доступности и прочие безымянные скаляры
            };
        };
        fake.ListResolver = sql => sql.Contains("c.relkind IN ('r','S','v','m','p')") // инвентарь P5
            ? fake.LastDsn == SrcDsn ? p.InventorySrc ?? [] : p.InventoryDst ?? []
            : []; // SequenceNames и прочие списки
        return fake;
    }

    /// <summary>
    /// Cutover-слой поверх префлайт-резолвера стенда (задачи 14–15): таблицы схемы,
    /// слот догнал, сверки строк по DSN источник/приёмник. Параметры — расхождения.
    /// </summary>
    public static void CutoverLayer(
        FakeMoveSql sql, bool caughtUp = true, long srcRows = 50, long dstRows = 50,
        string tables = "bucket_42.\"items\"")
    {
        var preflight = sql.ScalarResolver;
        sql.ScalarResolver = s => s switch
        {
            var x when x.Contains("string_agg(format('%I.%I'") => tables,
            var x when x.Contains("bool_and(active") => caughtUp,
            var x when x.Contains("count(*) FROM bucket_42.") =>
                sql.LastDsn == SrcDsn ? srcRows : dstRows,
            _ => preflight(s),
        };
    }

    /// <summary>Стенд: etcd-сид + SQL-мок + клэйм (claim=true) + MoveProcess со снапшот-очередью.</summary>
    internal sealed record Rig(
        Fakes.FakeEtcd Etcd,
        FakeMoveSql Sql,
        Fakes.FakeDriver Driver,
        ClaimStore Claims,
        WorkJournal Journal,
        MoveProcess Process,
        MoveStatusStore Status,
        MoveRequestsStore Requests,
        List<int> SnapshotCalls);

    public static async Task<Rig> NewAsync(
        PreflightSql? preflight = null, bool claim = true, MoveStatus? seededStatus = null,
        string requestJson = """{"op":"move","to":"shard2","requested_unix":100}""",
        bool seedRequest = true, bool failoverSlots = true, TimeProvider? clock = null,
        MovesRuntimeOptions? runtime = null, ILogger<MoveProcess>? logger = null,
        params Result[] snapshotResults)
    {
        var etcd = new Fakes.FakeEtcd();
        SeedTopology(etcd);
        if (seedRequest)
            SeedMoveRequest(etcd, requestJson);
        if (seededStatus is { } status)
            etcd.Seed(MoveNames.StatusKey("shop", status.Bucket), status.Serialize());

        var sql = SqlOf(preflight);
        var driver = new Fakes.FakeDriver { ExecResult = (_, _) => Result<string>.Success("-- ddl") };
        var claims = new ClaimStore([Ep], etcd, TimeProvider.System);
        if (claim)
        {
            var claimed = await claims.TryClaimClusterAsync("shop", CancellationToken.None);
            claimed.Value.Should().BeTrue("клэйм кластера обязан пройти на пустом FakeEtcd");
        }

        var journal = new WorkJournal(etcd, [Ep]);
        var shards = new ShardEndpoints(etcd, [Ep], new ShardProbe(new HttpClient()));
        var snapshots = new List<int>();
        var queue = new Queue<Result>(snapshotResults);
        var process = new MoveProcess(
            etcd, [Ep], sql, new MoveDdl(driver, sql), driver, shards, claims, journal, Secrets,
            runtime ?? new MovesRuntimeOptions(FailoverSlots: failoverSlots),
            clock ?? TimeProvider.System,
            logger: logger,
            snapshot: ct =>
            {
                snapshots.Add(1);
                return Task.FromResult(queue.Count > 0 ? queue.Dequeue() : Result.Success());
            });

        return new Rig(etcd, sql, driver, claims, journal, process,
            new MoveStatusStore(etcd, [Ep]), new MoveRequestsStore(etcd, [Ep]), snapshots);
    }
}
