using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PgWorker.Backups;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Planning;
using PgWorker.Core.Tuning;
using PgWorker.Docker.Drivers;
using PgWorker.Core.Templates;
using Shared.Etcd.Client;
using PgWorker.Provisioning.Sql;
using PgWorker.Provisioning.Processes;

namespace PgWorker.UnitTests.Provisioning;

// Тест-даблы процессов (задачи 19–22): etcd-имитация с честными
// mod_revision/version, записывающий мок драйвера и мок SQL-исполнителя.
internal static class Fakes
{
    // Фабрика PGTune-входов по умолчанию (дефолты PgWorker:Pgtune) — стаб
    // для конструкторов процессов в тестах (spec.md §4.3).
    internal static PgtuneInputsFactory PgtuneFactory() => new(
        new PgtuneSettings(18, "oltp", "ssd", "mid_ram", 60,
            new HashSet<string>(StringComparer.Ordinal)),
        NullLogger<PgtuneInputsFactory>.Instance);

    // Настройки PgWorker:Pgtune для конструкторов процессов (дефолты опций);
    // отдельная сущность от PgtuneFactory — NodeSupervisor получает её
    // конструкторно для ExcludeParams (t11 spec §4.3 п.3).
    internal static PgtuneSettings PgtuneSettings() => new(
        18, "oltp", "ssd", "mid_ram", 60,
        new HashSet<string>(StringComparer.Ordinal));

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

        // Гонка RMW (t06, образец KafkaWorker Fakes): конкурентная запись ДО compare.
        public Action<TxnRequest>? OnTxnBeforeCompare { get; set; }

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
            OnTxnBeforeCompare?.Invoke(req); // t06: инжекция гонки до compare
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

        public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
        {
            StatusCalls.Add(endpoint);
            return Task.FromResult(Result<EtcdStatusPayload>.Success(new EtcdStatusPayload(null, null, null, null, null, (ulong)StatusRevision)));
        }

        public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<EtcdMember>>.Success([]));

        public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<EtcdAlarm>>.Success([]));

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
    // Стаб ensure кластерных кредов (t22): шаг пересоздания REST-TLS берёт пару
// из РЕЗУЛЬТАТА ensure (легаси-кластер без ключа в снапшоте).
internal sealed class FakeSecretEnsurer : IClusterSecretEnsurer
{
    public const string RestPassword = "EnsuredRest0Pass0000000000000000A";

    public Task<Result<ClusterCredentials>> EnsureAsync(
        string cluster, ClusterConfig config, CancellationToken ct)
        => Task.FromResult(Result<ClusterCredentials>.Success(new ClusterCredentials(
            new AppCredentials("app", "app-pw"), "mover-pw",
            new AppCredentials("bucket_admin", "admin-pw"), "backup-pw",
            RestPassword)));
}

// Фейк docker-движка для шага пересоздания REST-TLS (t22): трекер stop/rm
// контейнера (volume НЕ трогается — RemoveVolumeAsync считает вызовы).
internal sealed class FakeContainerEngine : IDockerEngine
{
    public readonly List<string> Stopped = [];
    public readonly List<string> Removed = [];
    public readonly List<string> RemovedVolumes = [];

    public Task<Result> StopContainerAsync(string idOrName, int timeoutSec, CancellationToken ct)
    {
        Stopped.Add(idOrName);
        return Task.FromResult(Result.Success());
    }

    public Task<Result> RemoveContainerAsync(string idOrName, bool force, CancellationToken ct)
    {
        Removed.Add(idOrName);
        return Task.FromResult(Result.Success());
    }

    public Task<Result> RemoveVolumeAsync(string name, CancellationToken ct)
    {
        RemovedVolumes.Add(name);
        return Task.FromResult(Result.Success());
    }

    // Остальные члены — стабы (шаг их не использует).
    public Task<Result> PingAsync(CancellationToken ct) => Task.FromResult(Result.Success());
    public Task<Result<IReadOnlyList<DockerContainer>>> ListContainersAsync(string namePrefix, bool all, CancellationToken ct) => Task.FromResult(Result<IReadOnlyList<DockerContainer>>.Success([]));
    public Task<Result<DockerContainerInspect>> InspectContainerAsync(string id, CancellationToken ct) => Task.FromResult(Result<DockerContainerInspect>.Failed(new NotSupportedException()));
    public Task<Result<string>> GetContainerLogsAsync(string idOrName, int tail, CancellationToken ct) => Task.FromResult(Result<string>.Success(""));
    public Task<Result> CreateContainerAsync(ContainerSpec spec, string name, CancellationToken ct) => Task.FromResult(Result.Success());
    public Task<Result> StartContainerAsync(string idOrName, CancellationToken ct) => Task.FromResult(Result.Success());
    public Task<Result<string>> ExecAsync(string containerId, IReadOnlyList<string> cmd, CancellationToken ct) => Task.FromResult(Result<string>.Success(""));
    public Task<Result> EnsureNetworkAsync(string name, CancellationToken ct) => Task.FromResult(Result.Success());
    public Task<Result> DeleteNetworkAsync(string name, CancellationToken ct) => Task.FromResult(Result.Success());
    public Task<Result> NetworkConnectAsync(string network, string container, CancellationToken ct) => Task.FromResult(Result.Success());
    public Task<Result<bool>> VolumeExistsAsync(string name, CancellationToken ct) => Task.FromResult(Result<bool>.Success(true));
    public Task<Result> EnsureVolumeAsync(string name, CancellationToken ct) => Task.FromResult(Result.Success());
    public Task<Result> DeleteVolumeAsync(string name, CancellationToken ct) => Task.FromResult(Result.Success());
    public Task<Result> PutVolumeArchiveAsync(string name, byte[] tar, string image, CancellationToken ct) => Task.FromResult(Result.Success());
    public Task<Result<byte[]?>> GetVolumeArchiveAsync(string name, string image, CancellationToken ct) => Task.FromResult(Result<byte[]?>.Success(null));
    public Task<Result<IReadOnlyList<DockerSwarmNode>>> ListNodesAsync(CancellationToken ct) => Task.FromResult(Result<IReadOnlyList<DockerSwarmNode>>.Success([]));
    public Task<Result> CreateServiceAsync(ServiceSpec spec, CancellationToken ct) => Task.FromResult(Result.Success());
    public Task<Result> RemoveServiceAsync(string name, CancellationToken ct) => Task.FromResult(Result.Success());
    public Task<Result<IReadOnlyList<string>>> ListServicesAsync(string namePrefix, CancellationToken ct) => Task.FromResult(Result<IReadOnlyList<string>>.Success([]));
    public Task<Result<IReadOnlyList<DockerTask>>> ListTasksAsync(string serviceName, CancellationToken ct) => Task.FromResult(Result<IReadOnlyList<DockerTask>>.Success([]));
    public Task<Result<IReadOnlySet<(string Host, int Port)>>> BusyPortsAsync(CancellationToken ct) => Task.FromResult(Result<IReadOnlySet<(string, int)>>.Success(new HashSet<(string, int)>()));
    public Task<Result<NodeLimits?>> InspectContainerResourcesAsync(string name, CancellationToken ct) => Task.FromResult(Result<NodeLimits?>.Success(null));
    public Task<Result<NodeLimits?>> InspectServiceResourcesAsync(string name, CancellationToken ct) => Task.FromResult(Result<NodeLimits?>.Success(null));
    public Task<Result<IReadOnlyDictionary<string, string>?>> InspectContainerEnvAsync(string idOrName, CancellationToken ct) => Task.FromResult(Result<IReadOnlyDictionary<string, string>?>.Success(null));
    public Task<Result<IReadOnlyDictionary<string, string>?>> InspectServiceEnvAsync(string name, CancellationToken ct) => Task.FromResult(Result<IReadOnlyDictionary<string, string>?>.Success(null));
    public Task<Result<IReadOnlyList<string>?>> InspectContainerCmdAsync(string idOrName, CancellationToken ct) => Task.FromResult(Result<IReadOnlyList<string>?>.Success(null));
    public Task<Result<IReadOnlyList<string>?>> InspectServiceCmdAsync(string name, CancellationToken ct) => Task.FromResult(Result<IReadOnlyList<string>?>.Success(null));
    public Task<Result<DockerNodeEndpoint?>> InspectNodeEndpointAsync(string name, int containerPort, CancellationToken ct) => Task.FromResult(Result<DockerNodeEndpoint?>.Success(null));
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

internal sealed class FakeDriver : IClusterDriver
    {
        private readonly object _gate = new();

        // Plain-семантика по умолчанию; тесты swarm-поведения ставят false
        // (InspectNodesAsync при этом возвращает пусто — «нода не видна»).
        public bool SupportsRunningInspection { get; set; } = true;

        public readonly List<string> EnsuredNodes = [];
        public readonly List<(string Node, NodeResources? Resources)> EnsuredDetails = [];
        public readonly List<bool> EnsuredSyncStrict = []; // t06: strict из EnsureNode-вызовов
        // t22: env живых контейнеров (ключ "<shard>/<node>" → env-словарь; нет
        // записи — пустой) + переданный restPassword каждого EnsureNode-вызова.
        public readonly Dictionary<string, IReadOnlyDictionary<string, string>> NodeEnvs = new();
        public readonly Dictionary<string, string> EnsuredRestPasswords = new();
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

        // t03: контейнеры WAL-агентов — фиксация ensure/remove + мутабельная карта.
        public readonly List<string> EnsuredBackupAgents = [];
        public readonly List<string> RemovedBackupAgents = [];
        public List<DockerContainer> BackupAgentObjects = [];

        public Task<Result> EnsureBackupAgentAsync(
            string cluster, string shard, string node, ContainerSpec spec, string host, CancellationToken ct)
        {
            lock (_gate)
            {
                var name = BackupAgentNames.Container(cluster, shard, node);
                EnsuredBackupAgents.Add(name);
                if (BackupAgentObjects.All(c => !c.Names.Contains("/" + name)))
                    BackupAgentObjects.Add(new DockerContainer($"id-{name}", ["/" + name], "running", spec.Image));
            }

            return Task.FromResult(Result.Success());
        }

        public Task<Result> RemoveBackupAgentsAsync(string cluster, string? shard, CancellationToken ct)
        {
            lock (_gate)
            {
                var prefix = BackupAgentNames.Prefix(cluster);
                var names = BackupAgentObjects
                    .SelectMany(c => c.Names)
                    .Select(n => n.TrimStart('/'))
                    .Where(n => n.StartsWith(prefix, StringComparison.Ordinal))
                    .Where(n => shard is null || n.Split('-')[0] == shard)
                    .Distinct()
                    .ToList();
                foreach (var name in names)
                {
                    RemovedBackupAgents.Add(name);
                    BackupAgentObjects.RemoveAll(c => c.Names.Any(n => n.TrimStart('/') == name));
                }
            }

            return Task.FromResult(Result.Success());
        }

        public Task<Result<IReadOnlyList<DockerContainer>>> ListBackupAgentsAsync(
            string cluster, CancellationToken ct)
        {
            lock (_gate)
            {
                var prefix = BackupAgentNames.Prefix(cluster);
                IReadOnlyList<DockerContainer> result = BackupAgentObjects
                    .Where(c => c.Names.Any(n => n.TrimStart('/').StartsWith(prefix, StringComparison.Ordinal)))
                    .ToList();
                return Task.FromResult(Result<IReadOnlyList<DockerContainer>>.Success(result));
            }
        }

        public Task<Result<IReadOnlyList<HostInfo>>> GetHostsAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<HostInfo>>.Success(Hosts));

        public Task<Result<IReadOnlySet<(string Host, int Port)>>> GetBusyPortsAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlySet<(string Host, int Port)>>.Success(BusyPorts));

        public Task<Result> EnsureNodeAsync(ShardTopology topology, string nodeName, NodeAddress addr,
            InstallSecrets secrets, EtcdEndpoints etcd, NodeResources? resources, PgTuneResult? tuning,
            bool syncStrict, string restPassword, CancellationToken ct)
        {
            lock (_gate)
            {
                EnsuredNodes.Add($"{topology.Shard}/{nodeName}");
                EnsuredDetails.Add((nodeName, resources));
                EnsuredSyncStrict.Add(syncStrict); // t06: значение фиксируется для ассертов
                EnsuredRestPasswords[$"{topology.Shard}/{nodeName}"] = restPassword;
                // Пересоздание шагом REST-TLS — новый контейнер несёт полный env
                // (серт + hash фактической пары): тестовая семантика «ensure пишет env».
                NodeEnvs[$"{topology.Shard}/{nodeName}"] = new Dictionary<string, string>
                {
                    [RestRotation.EnvCert] = "-----BEGIN CERTIFICATE-----\nunit\n-----END CERTIFICATE-----\n",
                    [RestRotation.EnvPasswordHash] = RestRotation.PasswordHash(restPassword),
                };
            }

            return Task.FromResult(EnsureResultByNode is { } f ? f(nodeName) : Result.Success());
        }

        // t22: env живого контейнера (шаг пересоздания). Дефолт — «TLS уже на
        // месте» (существующие тесты надзора: шаг no-op); кейсы шага затирают
        // NodeEnvs управляемым env (легаси без серта / чужой hash).
        public bool DefaultNodeEnvIsLegacy { get; set; }
        // t22-ротация: оверрайд env инспекции (кейс «нода несёт hash(pending)» —
        // тесты ротатора читают журнал напрямую).
        public Func<string, string, IReadOnlyDictionary<string, string>>? InspectEnvOverride { get; set; }
        public Task<Result<IReadOnlyDictionary<string, string>>> InspectNodeEnvAsync(
            string cluster, string shard, string nodeName, CancellationToken ct)
        {
            if (InspectEnvOverride is { } over)
                return Task.FromResult(Result<IReadOnlyDictionary<string, string>>.Success(over(shard, nodeName)));
            if (NodeEnvs.TryGetValue($"{shard}/{nodeName}", out var env))
                return Task.FromResult(Result<IReadOnlyDictionary<string, string>>.Success(env));
            return Task.FromResult(Result<IReadOnlyDictionary<string, string>>.Success(
                DefaultNodeEnvIsLegacy
                    ? (IReadOnlyDictionary<string, string>)new Dictionary<string, string>()
                    : new Dictionary<string, string>
                    {
                        [RestRotation.EnvCert] = "-----BEGIN CERTIFICATE-----\nunit\n-----END CERTIFICATE-----\n",
                    }));
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

        // t02: чистка джобов бэкапов (D2) — фейк помнит вызов, движки не нужны.
        public bool RemoveBackupJobsCalled { get; private set; }

        // t05: чистка restore-джобов шарда (remove-shard) — помним вызовы.
        public List<(string Cluster, string Shard)> RemovedRestoreJobs { get; } = [];

        public IDockerEngine? Engine { get; set; }
        public IDockerEngine? EngineFor(string host) => Engine;

        public Task<Result> RemoveBackupJobsAsync(string cluster, CancellationToken ct)
        {
            RemoveBackupJobsCalled = true;
            return Task.FromResult(Result.Success());
        }

        public Task<Result> RemoveRestoreJobsAsync(string cluster, string shard, CancellationToken ct)
        {
            RemovedRestoreJobs.Add((cluster, shard));
            return Task.FromResult(Result.Success());
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

        // t02: ответ гварда роли зависит от SQL (роль есть → null, нет → CREATE-текст)
        public Func<string, string, Result<object?>>? ScalarResultBySql { get; set; }

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

            return Task.FromResult(ScalarResultBySql is { } bySql ? bySql(dsn, sql)
                : ScalarResultByDsn is { } byDsn ? byDsn(dsn)
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

    // Записывающий ILogger: фиксирует отформатированные сообщения — проверка
    // warning-логов процессов (t11: skip pgtune-конвергенции по заявке).
    internal sealed class RecordingLogger<T> : ILogger<T>
    {
        public readonly List<(LogLevel Level, string Message)> Entries = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state,
            Exception? exception, Func<TState, Exception?, string> formatter)
            => Entries.Add((logLevel, formatter(state, exception)));
    }

    // Заглушка S3 для юнит-тестов подсистемы бэкапов (аналог приватной
    // DisabledBackupS3 из Program.cs): каждый вызов — Failed «выключено»;
    // sweeper при Backups:Enabled=false в S3 не ходит — стаб только для DI.
    internal sealed class DisabledBackupS3Stub : IBackupS3
    {
        private static Result<T> Off<T>()
            => Result<T>.Failed(new ApplicationException("Backups:Enabled=false"));

        public Task<Result<bool>> BucketExistsAsync(CancellationToken ct)
            => Task.FromResult(Off<bool>());

        public Task<Result<IReadOnlyList<WalObject>>> ListWalAsync(
            string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
            => Task.FromResult(Off<IReadOnlyList<WalObject>>());

        public Task<Result<IReadOnlyList<S3ObjectInfo>>> ListPrefixAsync(
            string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
            => Task.FromResult(Off<IReadOnlyList<S3ObjectInfo>>());

        public Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default)
            => Task.FromResult(Result.Failed(new ApplicationException("Backups:Enabled=false")));

        public Task<Result> PutObjectAsync(string key, byte[] data, string? sha256, CancellationToken ct = default)
            => Task.FromResult(Result.Failed(new ApplicationException("Backups:Enabled=false")));

        public Task<Result<IReadOnlyList<WalObject>>> ListAsync(
            string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
            => Task.FromResult(Off<IReadOnlyList<WalObject>>());

        public Task<Result<string>> GetObjectAsync(
            string cluster, string shard, string key, CancellationToken ct = default)
            => Task.FromResult(Off<string>());

        public Task<Result<IReadOnlyList<string>>> ListFullsAsync(
            string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
            => Task.FromResult(Off<IReadOnlyList<string>>());

        public Task<Result<string>> DownloadTextAsync(
            string cluster, string shard, string objectKey, CancellationToken ct = default)
            => Task.FromResult(Off<string>());
    }
}
