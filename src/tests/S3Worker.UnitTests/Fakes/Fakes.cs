using Shared.Core;
using Shared.Etcd.Client;

namespace S3Worker.UnitTests;

// Тест-даблы процессов S3Worker (порт Fakes ValkeyWorker/KafkaWorker): FakeEtcd —
// честная etcd-имитация (mod_revision/version/txn-compare). Docker-фейк-драйвер —
// Task 6 (Fakes.cs расширяется по задачам).
internal static class Fakes
{
    // etcd в памяти: Put инкрементирует mod_revision; txn-compare честно
    // сверяет Version/Value/ModRevision (нужно portalloc-lock и config-txn).
    internal sealed class FakeEtcd : IEtcdGateway
    {
        internal sealed record Entry(string Value, long ModRevision, long Version);

        public readonly Dictionary<string, Entry> Store = [];
        public readonly List<TxnRequest> Txns = [];
        public Action<string>? OnPut { get; set; }

        // Журнал операций в порядке вызова (тесты порядка секций: «docker до
        // etcd» X2, «клэйм portalloc до чтения занятости» — arch/23 §8).
        public List<string> Ops { get; } = [];

        private readonly List<string>? _sharedOps;

        public FakeEtcd(List<string>? sharedOps = null) => _sharedOps = sharedOps;

        private void Note(string op)
        {
            Ops.Add(op);
            _sharedOps?.Add(op);
        }

        // Транспортный отказ range (живой-Ф7-тесты): префикс → исключение (обёрнуто в Failed).
        public Func<string, Exception?>? RangeFault { get; set; }

        // Отказ снятия снапшота (SnapshotLoop-тесты).
        public Func<Exception?>? SnapshotFault { get; set; }

        // Гонка «панель пишет между read и txn»: вызывается ДО compare —
        // тест успевает переписать ключ и сломать ModRevisionEqual.
        public Action<TxnRequest>? OnTxnBeforeCompare { get; set; }

        // Сбой-инъекция txn (отказ захвата PortAllocLock): фильтр по
        // содержимому — null = txn исполняется штатно.
        public Func<TxnRequest, Exception?>? TxnFault { get; set; }

        // Сбой-инъекция put (отказ записи между фазами): фильтр по
        // ключу — null = put исполняется штатно.
        public Func<string, Exception?>? PutFault { get; set; }

        private long _rev;
        private long _lease;
        private readonly object _gate = new();

        public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
        {
            if (RangeFault is { } fault && fault(prefix) is { } ex)
                return Task.FromResult(Result<IReadOnlyList<Kv>>.Failed(ex));

            List<Kv> kvs;
            lock (_gate)
            {
                Note($"range:{prefix}");
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
                Note($"get:{key}");
                kv = Store.TryGetValue(key, out var e) ? new Kv(key, e.Value, (ulong)e.ModRevision) : null;
            }

            return Task.FromResult(Result<Kv?>.Success(kv));
        }

        public Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
        {
            if (PutFault?.Invoke(key) is { } putEx)
                return Task.FromResult(Result.Failed(putEx));

            lock (_gate)
            {
                Note($"put:{key}");
                Store[key] = new Entry(value, ++_rev, Store.TryGetValue(key, out var old) ? old.Version + 1 : 1);
            }

            OnPut?.Invoke(key);
            return Task.FromResult(Result.Success());
        }

        public Action<string>? OnDelete { get; set; }

        public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
        {
            lock (_gate)
            {
                Note($"del:{keyOrPrefix}");
                foreach (var key in Store.Keys.Where(k => prefix
                             ? k.StartsWith(keyOrPrefix, StringComparison.Ordinal)
                             : k == keyOrPrefix).ToList())
                {
                    Store.Remove(key);
                }
            }

            OnDelete?.Invoke(keyOrPrefix);
            return Task.FromResult(Result.Success());
        }

        public Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct)
        {
            if (TxnFault?.Invoke(req) is { } txnEx)
                return Task.FromResult(Result<TxnResult>.Failed(txnEx));

            bool succeeded;
            lock (_gate)
            {
                Note($"txn:{string.Join(',', req.Compare.Select(c => $"{c.Target}:{c.Key}"))}");
                Txns.Add(req);
                OnTxnBeforeCompare?.Invoke(req);
                succeeded = req.Compare.All(c => c.Target switch
                {
                    TxnTarget.Version => c.Pred == TxnPredicate.Greater
                        ? Store.TryGetValue(c.Key, out var ge) && ge.Version > c.Num // Exists
                        : Store.TryGetValue(c.Key, out var e) ? e.Version == c.Num : c.Num == 0,
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
        {
            if (SnapshotFault is { } fault && fault() is { } ex)
                return Task.FromResult(Result<byte[]>.Failed(ex));

            return Task.FromResult(Result<byte[]>.Success([1, 2, 3]));
        }

        public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<EtcdStatusPayload>.Success(new EtcdStatusPayload(null, null, null, null, null, 42)));

        public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<EtcdMember>>.Success([]));

        public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<EtcdAlarm>>.Success([]));

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

    /// <summary>
    /// Драйвер-дублёр (порт FakeDriver valkey): имя объекта s3w-&lt;C&gt;-node&lt;k&gt; →
    /// факт инспекта; hosts/busy/ensure-журнал для процессов; флаги сбоев — слепота.
    /// </summary>
    internal sealed class FakeS3Driver : S3Worker.Docker.Drivers.IS3ClusterDriver
    {
        public sealed record NodeFact(string Host, int S3HostPort, bool Running = true);

        public readonly Dictionary<string, NodeFact> Nodes = [];

        public IReadOnlyList<S3Worker.Docker.Drivers.S3HostAttributes> Hosts { get; set; } =
            [new S3Worker.Docker.Drivers.S3HostAttributes("h1", 0, new Dictionary<string, string>())];

        public HashSet<(string Host, int Port)> BusyPorts { get; } = new();

        public readonly List<S3Worker.Docker.Drivers.S3NodeSpec> Ensured = [];
        public readonly List<string> Removed = [];

        // env-факт живых нод (сверка ротации R): cluster/node → env.
        public readonly Dictionary<string, IReadOnlyDictionary<string, string>> Envs = [];

        public bool InspectFault { get; set; }

        public bool HostsFault { get; set; }

        // docker-факт лимитов живых нод (автоконверге C).
        public Shared.Docker.NodeLimits? Limits { get; set; }

        public int S3ContainerPort => 9000;

        public Task<Result<IReadOnlyList<S3Worker.Docker.Drivers.S3HostAttributes>>> GetHostsAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<S3Worker.Docker.Drivers.S3HostAttributes>>.Success(
                HostsFault
                    ? throw new ApplicationException("docker-хосты недоступны")
                    : Hosts));

        public Task<Result<IReadOnlySet<(string Host, int Port)>>> GetBusyPortsAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlySet<(string Host, int Port)>>.Success(
                (IReadOnlySet<(string Host, int Port)>)BusyPorts));

        public Task<Result> EnsureNodeAsync(
            S3Worker.Docker.Drivers.S3NodeSpec spec, string? contourNetwork, CancellationToken ct)
        {
            Ensured.Add(spec);
            Nodes[$"{spec.Cluster}/{spec.NodeName}"] = new NodeFact(spec.Host, spec.S3HostPort);
            return Task.FromResult(Result.Success());
        }

        public Task<Result> RemoveNodeAsync(string cluster, string node, bool removeVolume, CancellationToken ct)
        {
            Removed.Add($"{cluster}/{node}");
            Nodes.Remove($"{cluster}/{node}");
            return Task.FromResult(Result.Success());
        }

        public Task<Result<S3Worker.Docker.Drivers.S3NodeInspection?>> InspectNodeAsync(
            string cluster, string node, CancellationToken ct)
        {
            if (InspectFault)
                return Task.FromResult(Result<S3Worker.Docker.Drivers.S3NodeInspection?>.Failed(
                    new ApplicationException("docker-хост молчит")));

            return Task.FromResult(Result<S3Worker.Docker.Drivers.S3NodeInspection?>.Success(
                Nodes.TryGetValue($"{cluster}/{node}", out var f)
                    ? new S3Worker.Docker.Drivers.S3NodeInspection(f.Host, f.S3HostPort, f.Running)
                    : null));
        }

        public Task<Result<Shared.Docker.NodeLimits?>> NodeResourcesAsync(string cluster, string node, CancellationToken ct)
            => Task.FromResult(Result<Shared.Docker.NodeLimits?>.Success(
                Nodes.ContainsKey($"{cluster}/{node}") ? Limits : null));

        public Task<Result<IReadOnlyDictionary<string, string>?>> NodeEnvAsync(string cluster, string node, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyDictionary<string, string>?>.Success(
                Envs.TryGetValue($"{cluster}/{node}", out var env) ? env : null));

        public Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<string>>.Success(
                Nodes.Keys.Where(k => k.StartsWith($"{cluster}/", StringComparison.Ordinal))
                    .Select(k => $"s3w-{k.Replace('/', '-')}")
                    .ToList()));
    }

    /// <summary>Клиент ownS3-ноды-дублёр: health-флаг, usage-факт, бакеты в памяти;
    /// флаг сбоя — транзиент-ветки процессов. Mesh-расширения (arch/24 §5 G):
    /// программируемые исходы status/join/delete/denylist + журнал вызовов
    /// (защёлки assertions).</summary>
    internal sealed class FakeNodeClient : S3Worker.Core.OwnS3.IOwnS3NodeClient
    {
        public bool Health { get; set; } = true;

        public bool UsageFault { get; set; }

        public S3Worker.Core.OwnS3.OwnS3NodeUsage Usage { get; set; } = new(1, 2);

        public readonly HashSet<(string Host, int Port, string Bucket)> Buckets = [];

        // Mesh: программируемые факты/исходы (StatusByPort — per-инстанс, ключ = S3-порт).
        public S3Worker.Core.OwnS3.OwnS3ReplicationStatus Status { get; set; } =
            new(false, Array.Empty<string>());

        public readonly Dictionary<int, S3Worker.Core.OwnS3.OwnS3ReplicationStatus> StatusByPort = [];

        public S3Worker.Core.OwnS3.OwnS3JoinOutcome JoinOutcome { get; set; } =
            new(S3Worker.Core.OwnS3.OwnS3JoinKind.Joined);

        public bool DeleteTargetResult { get; set; } = true;

        public bool DenyListPushed { get; set; } = true;

        // Журнал вызовов mesh-методов (кто, куда, что).
        public readonly List<(string Op, string Host, int Port, string? Arg)> MeshCalls = [];

        // Последнее тело deny-перезаписи (entries последнего PUT).
        public IReadOnlyList<(string Name, string PublicKeyPem)>? LastDenyEntries { get; private set; }

        public Task<Result<bool>> HealthAsync(string host, int port, CancellationToken ct)
            => Task.FromResult(Result<bool>.Success(Health));

        public Task<Result<bool>> BucketExistsAsync(
            string host, int port, S3Worker.Core.OwnS3.OwnS3NodeSecrets creds, string bucket, CancellationToken ct)
            => Task.FromResult(Result<bool>.Success(Buckets.Contains((host, port, bucket))));

        public Task<Result> EnsureBucketAsync(
            string host, int port, S3Worker.Core.OwnS3.OwnS3NodeSecrets creds, string bucket, CancellationToken ct)
        {
            Buckets.Add((host, port, bucket));
            return Task.FromResult(Result.Success());
        }

        public Task<Result<S3Worker.Core.OwnS3.OwnS3NodeUsage>> GetUsageAsync(
            string host, int port, S3Worker.Core.OwnS3.OwnS3NodeSecrets creds, CancellationToken ct)
            => Task.FromResult(UsageFault
                ? Result<S3Worker.Core.OwnS3.OwnS3NodeUsage>.Failed(new ApplicationException("грань usage недоступна"))
                : Result<S3Worker.Core.OwnS3.OwnS3NodeUsage>.Success(Usage));

        public Task<Result<S3Worker.Core.OwnS3.OwnS3ReplicationStatus>> GetReplicationStatusAsync(
            string host, int port, S3Worker.Core.OwnS3.OwnS3NodeSecrets creds, CancellationToken ct)
        {
            MeshCalls.Add(("status", host, port, null));
            // Зонд-ветка t33: пер-порт статус с Neighbors (счётчики доставки);
            // NeighborNames-ветка G не тронута.
            if (StatusResultByPort.TryGetValue(port, out var withNeighbors))
                return Task.FromResult(Result<S3Worker.Core.OwnS3.OwnS3ReplicationStatus>.Success(withNeighbors));
            return Task.FromResult(Result<S3Worker.Core.OwnS3.OwnS3ReplicationStatus>.Success(
                StatusByPort.TryGetValue(port, out var perPort) ? perPort : Status));
        }

        public Task<Result<S3Worker.Core.OwnS3.OwnS3JoinOutcome>> AdminJoinAsync(
            string host, int port, S3Worker.Core.OwnS3.OwnS3NodeSecrets creds, string endpoint, CancellationToken ct)
        {
            MeshCalls.Add(("join", host, port, endpoint));
            return Task.FromResult(Result<S3Worker.Core.OwnS3.OwnS3JoinOutcome>.Success(JoinOutcome));
        }

        public Task<Result<bool>> DeleteTargetAsync(
            string host, int port, S3Worker.Core.OwnS3.OwnS3NodeSecrets creds, string name, CancellationToken ct)
        {
            MeshCalls.Add(("delete-target", host, port, name));
            return Task.FromResult(Result<bool>.Success(DeleteTargetResult));
        }

        public Task<Result<bool>> PutDenyListAsync(
            string host, int port, IReadOnlyList<(string Name, string PublicKeyPem)> entries,
            S3Worker.Core.OwnS3.IssuedPkiMaterial orchestrator, CancellationToken ct)
        {
            MeshCalls.Add(("denylist", host, port, string.Join(",", entries.Select(e => e.Name))));
            LastDenyEntries = entries;
            return Task.FromResult(Result<bool>.Success(DenyListPushed));
        }

        // Зонд F (t33): программируемые probe/sync-report/listing по S3-порту
        // ноды + журнал вызовов (защёлки assertions). Ключ маркера "" = null
        // (первая страница; string? нельзя TKey — notnull-ограничение).
        public readonly Dictionary<int, S3Worker.Core.OwnS3.OwnS3ProbeReport> ProbeByPort = [];
        public readonly Dictionary<int, Dictionary<string, S3Worker.Core.OwnS3.OwnS3SyncReportPage>> SyncReportPages = [];
        public readonly Dictionary<int, IReadOnlyList<S3Worker.Core.OwnS3.OwnS3ListEntry>> Listings = [];
        // Многостраничный листинг (ключ "" = первая страница, токен-цепочка —
        // для тестов независимых бюджетов сторон).
        public readonly Dictionary<int, Dictionary<string, S3Worker.Core.OwnS3.OwnS3ListPage>> ListingPages = [];
        public readonly Dictionary<int, S3Worker.Core.OwnS3.OwnS3ReplicationStatus> StatusResultByPort = []; // статус с Neighbors (per-инстанс)
        public readonly List<(string Op, int Port, string? Arg)> ProbeCalls = [];

        public Task<Result<S3Worker.Core.OwnS3.OwnS3ProbeReport>> ProbeAsync(
            string host, int port, S3Worker.Core.OwnS3.OwnS3NodeSecrets creds, CancellationToken ct)
        {
            ProbeCalls.Add(("probe", port, null));
            return Task.FromResult(ProbeByPort.TryGetValue(port, out var report)
                ? Result<S3Worker.Core.OwnS3.OwnS3ProbeReport>.Success(report)
                : Result<S3Worker.Core.OwnS3.OwnS3ProbeReport>.Failed(
                    new ApplicationException("probe не запрограммирован")));
        }

        public Task<Result<S3Worker.Core.OwnS3.OwnS3SyncReportPage>> GetSyncReportPageAsync(
            string host, int port, S3Worker.Core.OwnS3.OwnS3NodeSecrets creds, string bucket, string? marker, CancellationToken ct)
        {
            ProbeCalls.Add(("sync-report", port, marker));
            return Task.FromResult(
                SyncReportPages.TryGetValue(port, out var pages) && pages.TryGetValue(marker ?? "", out var page)
                    ? Result<S3Worker.Core.OwnS3.OwnS3SyncReportPage>.Success(page)
                    : Result<S3Worker.Core.OwnS3.OwnS3SyncReportPage>.Failed(
                        new ApplicationException("страница sync-report не запрограммирована")));
        }

        public Task<Result<S3Worker.Core.OwnS3.OwnS3ListPage>> ListObjectEntriesAsync(
            string host, int port, S3Worker.Core.OwnS3.OwnS3NodeSecrets creds, string bucket, string? continuationToken, CancellationToken ct)
        {
            ProbeCalls.Add(("listing", port, continuationToken));
            // Многостраничный листинг по токен-цепочке (если задан), иначе —
            // одна страница (NextContinuationToken=null).
            if (ListingPages.TryGetValue(port, out var pages)
                && pages.TryGetValue(continuationToken ?? "", out var page))
                return Task.FromResult(Result<S3Worker.Core.OwnS3.OwnS3ListPage>.Success(page));
            return Task.FromResult(Listings.TryGetValue(port, out var entries)
                ? Result<S3Worker.Core.OwnS3.OwnS3ListPage>.Success(new(entries, null))
                : Result<S3Worker.Core.OwnS3.OwnS3ListPage>.Failed(
                    new ApplicationException("листинг не запрограммирован")));
        }
    }
}
