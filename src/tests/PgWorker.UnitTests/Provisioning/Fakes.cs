using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Planning;
using PgWorker.Docker.Drivers;
using PgWorker.Core.Templates;
using PgWorker.Etcd.Client;
using PgWorker.Provisioning.Sql;

namespace PgWorker.UnitTests.Provisioning;

// Тест-даблы процессов (задачи 19–22): etcd-имитация с честными
// mod_revision/version, записывающий мок драйвера и мок SQL-исполнителя.
internal static class Fakes
{
    // etcd в памяти: Put инкрементирует mod_revision; txn-compare честно
    // сверяет Version/Value/ModRevision (нужно P1-portalloc и P4-config).
    internal sealed class FakeEtcd : IEtcdGateway
    {
        internal sealed record Entry(string Value, long ModRevision, long Version);

        public readonly Dictionary<string, Entry> Store = [];
        public readonly List<TxnRequest> Txns = [];
        private long _rev;
        private long _lease;

        public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
        {
            var kvs = Store
                .Where(p => p.Key.StartsWith(prefix, StringComparison.Ordinal))
                .Select(p => new Kv(p.Key, p.Value.Value, (ulong)p.Value.ModRevision))
                .ToList();
            return Task.FromResult(Result<IReadOnlyList<Kv>>.Success(kvs));
        }

        public Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
            => Task.FromResult(Result<Kv?>.Success(
                Store.TryGetValue(key, out var e) ? new Kv(key, e.Value, (ulong)e.ModRevision) : null));

        public Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
        {
            Store[key] = new Entry(value, ++_rev, Store.TryGetValue(key, out var old) ? old.Version + 1 : 1);
            return Task.FromResult(Result.Success());
        }

        public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
        {
            foreach (var key in Store.Keys.Where(k => prefix
                         ? k.StartsWith(keyOrPrefix, StringComparison.Ordinal)
                         : k == keyOrPrefix).ToList())
            {
                Store.Remove(key);
            }

            return Task.FromResult(Result.Success());
        }

        public Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct)
        {
            Txns.Add(req);
            var succeeded = req.Compare.All(c => c.Target switch
            {
                TxnTarget.Version => Store.TryGetValue(c.Key, out var e) ? e.Version == c.Num : c.Num == 0,
                TxnTarget.Value => Store.TryGetValue(c.Key, out var e) && e.Value == c.Arg,
                TxnTarget.ModRevision => Store.TryGetValue(c.Key, out var e) && e.ModRevision == c.Num,
                _ => false,
            });
            if (succeeded)
                foreach (var op in req.Success)
                    Apply(op);
            return Task.FromResult(Result<TxnResult>.Success(new TxnResult(succeeded)));
        }

        private void Apply(TxnOp op)
        {
            switch (op)
            {
                case TxnOp.Put put:
                    PutAsync(string.Empty, put.Key, put.Value, put.Lease, CancellationToken.None).GetAwaiter().GetResult();
                    break;
                case TxnOp.Delete del:
                    DeleteAsync(string.Empty, del.Key, del.Prefix, CancellationToken.None).GetAwaiter().GetResult();
                    break;
            }
        }

        public Task<Result<long>> LeaseGrantAsync(string endpoint, int ttlSec, CancellationToken ct)
            => Task.FromResult(Result<long>.Success(++_lease));

        public Task<Result> LeaseRevokeAsync(string endpoint, long lease, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result> LeaseKeepaliveAsync(string endpoint, long lease, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<byte[]>.Success([1, 2, 3]));

        // Утилита тестов: простой Put вне txn (сборка сида).
        public void Seed(string key, string value) =>
            Store[key] = new Entry(value, ++_rev, Store.TryGetValue(key, out var old) ? old.Version + 1 : 1);
    }

    // Записывающий мок драйвера кластера: порядок вызовов проверяют тесты.
    internal sealed class FakeDriver : IClusterDriver
    {
        public readonly List<string> EnsuredNodes = [];
        public readonly List<string> RemovedNodes = [];
        public readonly List<string> StoppedNodes = [];
        public List<string> NodeObjects = [];
        public Func<string, Result>? EnsureResultByNode { get; set; }
        public bool RemoveFailsOnce { get; set; }
        private bool _removeFailed;
        public IReadOnlyList<HostInfo> Hosts = [new HostInfo("h1", 0), new HostInfo("h2", 0)];
        public IReadOnlySet<(string Host, int Port)> BusyPorts = new HashSet<(string, int)>();

        public Task<Result<IReadOnlyList<HostInfo>>> GetHostsAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<HostInfo>>.Success(Hosts));

        public Task<Result<IReadOnlySet<(string Host, int Port)>>> GetBusyPortsAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlySet<(string Host, int Port)>>.Success(BusyPorts));

        public Task<Result> EnsureNodeAsync(ShardTopology topology, string nodeName, NodeAddress addr,
            InstallSecrets secrets, EtcdEndpoints etcd, CancellationToken ct)
        {
            EnsuredNodes.Add($"{topology.Shard}/{nodeName}");
            return Task.FromResult(EnsureResultByNode is { } f ? f(nodeName) : Result.Success());
        }

        public Task<Result> RemoveNodeAsync(string cluster, string shard, string nodeName, CancellationToken ct)
        {
            if (RemoveFailsOnce && !_removeFailed)
            {
                _removeFailed = true; // первый вызов падает (docker-хост недоступен)
                return Task.FromResult(Result.Failed(new ApplicationException("docker: connection refused")));
            }

            RemovedNodes.Add($"{shard}/{nodeName}");
            // docker больше не видит объект (guard D2 читает список заново)
            NodeObjects.Remove($"pgw-{cluster}-{shard}-{nodeName}");
            return Task.FromResult(Result.Success());
        }

        public Task<Result> StopNodeAsync(string cluster, string shard, string nodeName, CancellationToken ct)
        {
            StoppedNodes.Add($"{shard}/{nodeName}");
            return Task.FromResult(Result.Success());
        }

        public Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<string>>.Success(NodeObjects));
    }

    // Мок SQL: запоминает DSN/SQL вызовов (порядок journal-before-SQL проверяют тесты).
    internal sealed class FakeSql : ISqlExecutor
    {
        public readonly List<(string Dsn, string Sql)> Executed = [];
        public readonly List<(string Dsn, string DbName)> EnsuredDatabases = [];
        public Func<Result>? ExecuteResult { get; set; }

        public Task<Result> ExecuteAsync(string dsn, string sql, CancellationToken ct)
        {
            Executed.Add((dsn, sql));
            return Task.FromResult(ExecuteResult is { } f ? f() : Result.Success());
        }

        public Task<Result<object?>> ExecuteScalarAsync(string dsn, string sql, CancellationToken ct)
            => Task.FromResult(Result<object?>.Success(null));

        public Task<Result> EnsureDatabaseAsync(string dsn, string dbname, CancellationToken ct)
        {
            EnsuredDatabases.Add((dsn, dbname));
            return Task.FromResult(Result.Success());
        }
    }
}
