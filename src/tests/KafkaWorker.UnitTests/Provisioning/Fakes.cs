using KafkaWorker.Core;
using KafkaWorker.Core.Planning;
using KafkaWorker.Docker.Drivers;
using KafkaWorker.Etcd.Client;

namespace KafkaWorker.UnitTests.Provisioning;

// Тест-даблы процессов KafkaWorker (порт Fakes PgWorker): etcd-имитация с
// честными mod_revision/version и записывающий мок драйвера брокеров.
internal static class Fakes
{
    // etcd в памяти: Put инкрементирует mod_revision; txn-compare честно
    // сверяет Version/Value/ModRevision (нужно portalloc и config-K5).
    internal sealed class FakeEtcd : IEtcdGateway
    {
        internal sealed record Entry(string Value, long ModRevision, long Version);

        public readonly Dictionary<string, Entry> Store = [];
        public readonly List<TxnRequest> Txns = [];
        public Action<string>? OnPut { get; set; }

        // Гонка «панель пишет между read и txn»: вызывается ДО compare —
        // тест успевает переписать ключ и сломать ModRevisionEqual.
        public Action<TxnRequest>? OnTxnBeforeCompare { get; set; }

        private long _rev;
        private long _lease;
        private readonly object _gate = new();

        public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
        {
            List<Kv> kvs;
            lock (_gate)
            {
                kvs = Store
                    .Where(p => p.Key.StartsWith(prefix, StringComparison.Ordinal))
                    .Select(p => new Kv(p.Key, p.Value.Value, (ulong)p.Value.ModRevision))
                    .ToList();
            }

            return Task.FromResult(Result<IReadOnlyList<Kv>>.Success(kvs));
        }

        public Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
        {
            Kv? kv;
            lock (_gate)
            {
                kv = Store.TryGetValue(key, out var e) ? new Kv(key, e.Value, (ulong)e.ModRevision) : null;
            }

            return Task.FromResult(Result<Kv?>.Success(kv));
        }

        public Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
        {
            lock (_gate)
            {
                Store[key] = new Entry(value, ++_rev, Store.TryGetValue(key, out var old) ? old.Version + 1 : 1);
            }

            OnPut?.Invoke(key);
            return Task.FromResult(Result.Success());
        }

        public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
        {
            lock (_gate)
            {
                foreach (var key in Store.Keys.Where(k => prefix
                             ? k.StartsWith(keyOrPrefix, StringComparison.Ordinal)
                             : k == keyOrPrefix).ToList())
                {
                    Store.Remove(key);
                }
            }

            return Task.FromResult(Result.Success());
        }

        public Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct)
        {
            bool succeeded;
            lock (_gate)
            {
                Txns.Add(req);
                OnTxnBeforeCompare?.Invoke(req);
                succeeded = req.Compare.All(c => c.Target switch
                {
                    TxnTarget.Version => Store.TryGetValue(c.Key, out var e) ? e.Version == c.Num : c.Num == 0,
                    TxnTarget.Value => Store.TryGetValue(c.Key, out var e) && e.Value == c.Arg,
                    TxnTarget.ModRevision => Store.TryGetValue(c.Key, out var e) && e.ModRevision == c.Num,
                    _ => false,
                });
                if (succeeded)
                    foreach (var op in req.Success)
                        Apply(op);
            }

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
        {
            lock (_gate)
            {
                return Task.FromResult(Result<long>.Success(++_lease));
            }
        }

        public Task<Result> LeaseRevokeAsync(string endpoint, long lease, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result> LeaseKeepaliveAsync(string endpoint, long lease, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<byte[]>.Success([1, 2, 3]));

        public Task<Result<long>> StatusAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<long>.Success(42));

        public Task<Result> CompactAsync(string endpoint, long revision, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result> DefragmentAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result.Success());

        // Утилита тестов: простой Put вне txn (сборка сида).
        public void Seed(string key, string value)
        {
            lock (_gate)
            {
                Store[key] = new Entry(value, ++_rev, Store.TryGetValue(key, out var old) ? old.Version + 1 : 1);
            }
        }
    }

    // Записывающий мок драйвера брокеров: порядок вызовов и env проверяют тесты.
    internal sealed class FakeKafkaDriver : IClusterDriver
    {
        private readonly object _gate = new();

        public readonly List<KafkaNodeSpec> Ensured = [];
        public readonly List<KafkaNodeSpec> AllEnsured = []; // включая повторы имён (rolling-фазы)
        public readonly List<(string Node, bool RemoveVolume)> Removed = [];
        public List<string> NodeObjects = []; // имена kfw-<C>-<b>
        public Func<string, Result>? EnsureResultByNode { get; set; }
        public bool RemoveFailsOnce { get; set; }
        private bool _removeFailed;
        public IReadOnlyList<HostInfo> Hosts = [new HostInfo("h1", 0)];
        public IReadOnlySet<(string Host, int Port)> BusyPorts = new HashSet<(string, int)>();

        public Task<Result<IReadOnlyList<HostInfo>>> GetHostsAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<HostInfo>>.Success(Hosts));

        public Task<Result<IReadOnlySet<(string Host, int Port)>>> GetBusyPortsAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlySet<(string Host, int Port)>>.Success(BusyPorts));

        public Task<Result> EnsureNodeAsync(KafkaNodeSpec spec, CancellationToken ct)
        {
            if (EnsureResultByNode is { } f)
            {
                var result = f(spec.NodeName);
                if (!result.IsSuccess)
                    return Task.FromResult(result);
            }

            lock (_gate)
            {
                AllEnsured.Add(spec);
                if (Ensured.All(e => e.NodeName != spec.NodeName))
                    Ensured.Add(spec);
                var name = $"kfw-{spec.Cluster}-{spec.NodeName}";
                if (!NodeObjects.Contains(name))
                    NodeObjects.Add(name); // docker теперь видит объект
            }

            return Task.FromResult(Result.Success());
        }

        public Task<Result> RemoveNodeAsync(string cluster, string nodeName, bool removeVolume, CancellationToken ct)
        {
            if (RemoveFailsOnce && !_removeFailed)
            {
                _removeFailed = true; // первый вызов падает (docker-хост недоступен)
                return Task.FromResult(Result.Failed(new ApplicationException("docker: connection refused")));
            }

            lock (_gate)
            {
                Removed.Add((nodeName, removeVolume));
                NodeObjects.Remove($"kfw-{cluster}-{nodeName}");
            }

            return Task.FromResult(Result.Success());
        }

        public Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct)
        {
            List<string> objects;
            lock (_gate)
            {
                objects = NodeObjects.Where(n => n.StartsWith($"kfw-{cluster}-", StringComparison.Ordinal))
                    .OrderBy(n => n, StringComparer.Ordinal).ToList();
            }

            return Task.FromResult(Result<IReadOnlyList<string>>.Success(objects));
        }
    }
}
