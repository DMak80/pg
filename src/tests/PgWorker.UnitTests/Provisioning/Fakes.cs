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
    // Потокобезопасен: процессы реально параллелят шарды/ноды
    // (Parallel.ForEachAsync), обычный Dictionary терял записи (флaky-тесты).
    internal sealed class FakeEtcd : IEtcdGateway
    {
        internal sealed record Entry(string Value, long ModRevision, long Version);

        public readonly Dictionary<string, Entry> Store = [];
        public readonly List<TxnRequest> Txns = [];
        public Action<string>? OnPut { get; set; }

        // Сбой-инъекция: prefix → исключение (имитация широкого сбоя шлюза,
        // когда gateway бросает, а не возвращает Result.Failed).
        public Func<string, Exception?>? RangeFault { get; set; }

        // Сбой-инъекция txn (t90: ошибка захвата PortAllocLock → Result.Failed).
        public Func<TxnRequest, Result<TxnResult>>? TxnFault { get; set; }

        private long _rev;
        private long _lease;
        private readonly object _gate = new();

        public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
        {
            if (RangeFault?.Invoke(prefix) is { } fault)
                throw fault;
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
            if (TxnFault?.Invoke(req) is { } failed)
                return Task.FromResult(failed);
            bool succeeded;
            lock (_gate)
            {
                Txns.Add(req);
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
        {
            lock (_gate)
            {
                Keepalives.Add(lease);
            }

            return Task.FromResult(Result.Success());
        }

        public readonly List<long> Keepalives = [];

        public Task<Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<byte[]>.Success([1, 2, 3]));

        public readonly List<string> StatusCalls = [];

        public long StatusRevision { get; set; } = 42;

        public Task<Result<long>> StatusAsync(string endpoint, CancellationToken ct)
        {
            StatusCalls.Add(endpoint);
            return Task.FromResult(Result<long>.Success(StatusRevision));
        }

        public readonly List<(string Endpoint, long Revision)> CompactCalls = [];

        public Task<Result> CompactAsync(string endpoint, long revision, CancellationToken ct)
        {
            CompactCalls.Add((endpoint, revision));
            return Task.FromResult(Result.Success());
        }

        public readonly List<string> DefragmentCalls = [];

        public Task<Result> DefragmentAsync(string endpoint, CancellationToken ct)
        {
            DefragmentCalls.Add(endpoint);
            return Task.FromResult(Result.Success());
        }

        // Утилита тестов: простой Put вне txn (сборка сида).
        public void Seed(string key, string value)
        {
            lock (_gate)
            {
                Store[key] = new Entry(value, ++_rev, Store.TryGetValue(key, out var old) ? old.Version + 1 : 1);
            }
        }
    }

    // Записывающий мок драйвера кластера: порядок вызовов проверяют тесты.
    // Потокобезопасен: EnsureNode идёт параллельно по нодам/шардам —
    // обычные List-ы теряли записи (флaky-тесты).
    internal sealed class FakeDriver : IClusterDriver
    {
        private readonly object _gate = new();

        public readonly List<string> EnsuredNodes = [];
        public readonly List<(string Node, NodeResources? Resources)> EnsuredDetails = [];
        public readonly List<string> RemovedNodes = [];
        public readonly List<string> StoppedNodes = [];
        public readonly List<(string Node, IReadOnlyList<string> Cmd)> Executed = [];
        public List<string> NodeObjects = [];
        public Func<string, Result>? EnsureResultByNode { get; set; }
        public Func<string, IReadOnlyList<string>, Result<string>>? ExecResult { get; set; }
        public bool RemoveFailsOnce { get; set; }
        private bool _removeFailed;
        public IReadOnlyList<HostInfo> Hosts = [new HostInfo("h1", 0), new HostInfo("h2", 0)];
        public IReadOnlySet<(string Host, int Port)> BusyPorts = new HashSet<(string, int)>();

        public Task<Result<IReadOnlyList<HostInfo>>> GetHostsAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<HostInfo>>.Success(Hosts));

        public Task<Result<IReadOnlySet<(string Host, int Port)>>> GetBusyPortsAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlySet<(string Host, int Port)>>.Success(BusyPorts));

        public Task<Result> EnsureNodeAsync(ShardTopology topology, string nodeName, NodeAddress addr,
            InstallSecrets secrets, EtcdEndpoints etcd, NodeResources? resources, CancellationToken ct)
        {
            lock (_gate)
            {
                EnsuredNodes.Add($"{topology.Shard}/{nodeName}");
                EnsuredDetails.Add((nodeName, resources));
            }

            return Task.FromResult(EnsureResultByNode is { } f ? f(nodeName) : Result.Success());
        }

        public Task<Result> RemoveNodeAsync(string cluster, string shard, string nodeName, CancellationToken ct)
        {
            if (RemoveFailsOnce && !_removeFailed)
            {
                _removeFailed = true; // первый вызов падает (docker-хост недоступен)
                return Task.FromResult(Result.Failed(new ApplicationException("docker: connection refused")));
            }

            lock (_gate)
            {
                RemovedNodes.Add($"{shard}/{nodeName}");
                // docker больше не видит объект (guard D2 читает список заново)
                NodeObjects.Remove($"pgw-{cluster}-{shard}-{nodeName}");
            }

            return Task.FromResult(Result.Success());
        }

        public Task<Result> StopNodeAsync(string cluster, string shard, string nodeName, CancellationToken ct)
        {
            lock (_gate)
            {
                StoppedNodes.Add($"{shard}/{nodeName}");
            }

            return Task.FromResult(Result.Success());
        }

        public Task<Result<string>> ExecNodeAsync(
            string cluster, string shard, string node, IReadOnlyList<string> cmd, CancellationToken ct)
        {
            lock (_gate)
            {
                Executed.Add(($"{shard}/{node}", cmd));
            }

            return Task.FromResult(ExecResult is { } f
                ? f(node, cmd)
                : Result<string>.Success(string.Empty));
        }

        // Инспекция усыновления (adopt-repair T3): фиксированная карта находок;
        // пустая карта = docker-хосты не видят ни одной ноды (тихий skip).
        public IReadOnlyDictionary<string, DiscoveredNode> InspectResult { get; set; }
            = new Dictionary<string, DiscoveredNode>();

        // сбой-инъекция: docker-хост недоступен (Д2: transport-провал инспекции — transient).
        public Exception? InspectFault { get; set; }

        public readonly List<(string Container, IReadOnlyList<string> Cmd)> ContainerExecs = [];

        public Task<Result<IReadOnlyDictionary<string, DiscoveredNode>>> InspectNodesAsync(
            string cluster, IReadOnlyCollection<string> nodeNames, CancellationToken ct)
            => InspectFault is { } fault
                ? Task.FromResult(Result<IReadOnlyDictionary<string, DiscoveredNode>>.Failed(fault))
                : Task.FromResult(Result<IReadOnlyDictionary<string, DiscoveredNode>>.Success(
                    (IReadOnlyDictionary<string, DiscoveredNode>)InspectResult
                        .Where(p => nodeNames.Contains(p.Key))
                        .ToDictionary(p => p.Key, p => p.Value)));

        // Д3: карта присутствия данных по имени ноды (default Present — чистка запрещена).
        public Func<string, DataPresence> DataPresenceByNode { get; set; } = _ => DataPresence.Present;

        public Task<Result<DataPresence>> NodeDataPresenceAsync(string cluster, string shard, string node, CancellationToken ct)
            => Task.FromResult(Result<DataPresence>.Success(DataPresenceByNode(node)));

        public Task<Result<string>> ExecContainerAsync(string containerName, IReadOnlyList<string> cmd, CancellationToken ct)
        {
            lock (_gate)
            {
                ContainerExecs.Add((containerName, cmd));
            }

            return Task.FromResult(Result<string>.Success(string.Empty));
        }

        public Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct)
        {
            List<string> objects;
            lock (_gate)
            {
                // Контракт реального PlainClusterDriver (ревью Фазы 7): список
                // строится docker-фильтром по префиксу pgw-<C>- — object-контейнеры
                // усыновлённых нод (as-*) сюда НЕ попадают никогда. Фейк обязан
                // вести себя так же, иначе тесты маскируют дефекты матчинга.
                var prefix = $"pgw-{cluster}-";
                objects = NodeObjects.Where(n => n.StartsWith(prefix, StringComparison.Ordinal)).ToList();
            }

            return Task.FromResult(Result<IReadOnlyList<string>>.Success(objects));
        }
    }

    // Мок SQL: запоминает DSN/SQL вызовов (порядок journal-before-SQL проверяют тесты).
    // Потокобезопасен: шарды провижинятся параллельно (Parallel.ForEachAsync).
    internal sealed class FakeSql : ISqlExecutor
    {
        private readonly object _gate = new();

        public readonly List<(string Dsn, string Sql)> Executed = [];
        public readonly List<(string Dsn, string Sql)> Scalars = [];
        public readonly List<(string Dsn, string DbName)> EnsuredDatabases = [];
        public Func<Result>? ExecuteResult { get; set; }
        public Func<string, Result>? ExecuteResultByDsn { get; set; }
        public Func<string, Result<object?>>? ScalarResultByDsn { get; set; }
        public Action<string>? OnExecute { get; set; }

        public Task<Result> ExecuteAsync(string dsn, string sql, CancellationToken ct)
        {
            lock (_gate)
            {
                Executed.Add((dsn, sql));
            }

            OnExecute?.Invoke(dsn);
            return Task.FromResult(ExecuteResultByDsn is { } byDsn ? byDsn(dsn)
                : ExecuteResult is { } f ? f() : Result.Success());
        }

        public Task<Result<object?>> ExecuteScalarAsync(string dsn, string sql, CancellationToken ct)
        {
            lock (_gate)
            {
                Scalars.Add((dsn, sql)); // t06: гварды ролей идут скалярами — трекаем их
            }

            return Task.FromResult(ScalarResultByDsn is { } byDsn ? byDsn(dsn)
                : Result<object?>.Success(null));
        }

        public Task<Result> EnsureDatabaseAsync(string dsn, string dbname, CancellationToken ct)
        {
            lock (_gate)
            {
                EnsuredDatabases.Add((dsn, dbname));
            }

            return Task.FromResult(EnsureResultByDsn is { } byDsn ? byDsn(dsn, dbname) : Result.Success());
        }

        // ensure-инжекция по dsn (живой-Ф7': целевая БД отсутствует — 3D000,
        // postgres-подключение — успех): проверяет, КАКОЙ dsn использует процесс.
        public Func<string, string, Result>? EnsureResultByDsn { get; set; }
    }
}
