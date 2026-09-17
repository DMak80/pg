using Shared.Core;
using Shared.Core.Planning;
using Shared.Etcd.Client;
using ValkeyWorker.Core.Model;
using ValkeyWorker.Core.Valkey;
using ValkeyWorker.Docker.Drivers;

namespace ValkeyWorker.UnitTests.Provisioning;

// Тест-даблы процессов ValkeyWorker (порт Fakes KafkaWorker): FakeEtcd —
// честная etcd-имитация (mod_revision/version/txn-compare), FakeDriver —
// in-memory docker-драйвер с подменой факта (сверка V3, автоконверге, S7),
// FakeValkeyConnection — in-memory модель ноды (AUTH/ACL-окно двух паролей).
internal static class Fakes
{
    // etcd в памяти: Put инкрементирует mod_revision; txn-compare честно
    // сверяет Version/Value/ModRevision (нужно portalloc и config-V5).
    internal sealed class FakeEtcd : IEtcdGateway
    {
        internal sealed record Entry(string Value, long ModRevision, long Version);

        public readonly Dictionary<string, Entry> Store = [];
        public readonly List<TxnRequest> Txns = [];
        public Action<string>? OnPut { get; set; }

        // Журнал операций в порядке вызова (тесты порядка секций: «клэйм
        // portalloc до чтения занятости» — arch/20 §3). SharedOps — общий
        // журнал с другими фейками (единый порядок для теста).
        public List<string> Ops { get; } = [];

        private readonly List<string>? _sharedOps;

        public FakeEtcd(List<string>? sharedOps = null) => _sharedOps = sharedOps;

        private void Note(string op)
        {
            Ops.Add(op);
            _sharedOps?.Add(op);
        }

        // Транспортный отказ range (живой-Ф7-тесты, t09): префикс → исключение (обёрнуто в Failed).
        public Func<string, Exception?>? RangeFault { get; set; }

        // Отказ снятия снапшота (SnapshotLoop-тесты, t09).
        public Func<Exception?>? SnapshotFault { get; set; }

        // Гонка «панель пишет между read и txn»: вызывается ДО compare —
        // тест успевает переписать ключ и сломать ModRevisionEqual.
        public Action<TxnRequest>? OnTxnBeforeCompare { get; set; }

        // Сбой-инъекция txn (ошибка захвата PortAllocLock → Result.Failed):
        // фильтр по содержимому — null = txn исполняется штатно.
        public Func<TxnRequest, Result<TxnResult>?>? TxnFault { get; set; }

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
            if (TxnFault?.Invoke(req) is { } failed)
                return Task.FromResult(failed);

            bool succeeded;
            lock (_gate)
            {
                Note($"txn:{string.Join(',', req.Compare.Select(c => $"{c.Target}:{c.Key}"))}");
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
    /// In-memory docker-драйвер: контейнеры {имя → факт}, подмена args/лимитов
    /// живого контейнера (кейсы V3 «существующий с иными args → пересоздание»),
    /// настраиваемая занятость портов, флаги «движок недоступен» (слепой inspect
    /// → Result.Failed) и «контейнер снесён» (объекта нет — положительное
    /// свидетельство смерти).
    /// </summary>
    internal sealed class FakeDriver : IClusterDriver
    {
        private readonly object _gate = new();

        public sealed record ContainerFact(
            string Host, int HostPort, decimal? Cpu, long? Mem, IReadOnlyList<string> Args, string Image, string? Id);

        // Имя → факт последнего Ensure. Удалённые исчезают (S7 «объекта нет»).
        public readonly Dictionary<string, ContainerFact> Containers = [];

        public readonly List<ValkeyNodeSpec> Ensured = [];
        public readonly List<string> Removed = [];
        public IReadOnlyList<HostInfo> Hosts = [new HostInfo("h1", 0)];
        public IReadOnlySet<(string Host, int Port)> BusyPorts = new HashSet<(string, int)>();

        // Слепой inspect (docker-хост молчит) — ошибки тика, пересозданий нет.
        public bool ResourcesFault { get; set; }

        public bool ArgsFault { get; set; }

        public bool EndpointFault { get; set; }

        // Контейнер есть, но не running (stop) — жив, но PING не отвечает.
        public readonly HashSet<string> Stopped = [];

        // Журнал вызовов (порядок: чтение занятости ПОСЛЕ захвата portalloc-клэйма).
        // SharedOps — общий журнал с FakeEtcd (единый порядок для теста).
        public List<string> Ops { get; } = [];

        public List<string>? SharedOps { get; set; }

        private void Note(string op)
        {
            Ops.Add(op);
            SharedOps?.Add(op);
        }

        public Task<Result<IReadOnlyList<HostInfo>>> GetHostsAsync(CancellationToken ct)
        {
            lock (_gate)
            {
                Note("docker:hosts");
            }

            return Task.FromResult(Result<IReadOnlyList<HostInfo>>.Success(Hosts));
        }

        public Task<Result<IReadOnlySet<(string Host, int Port)>>> GetBusyPortsAsync(CancellationToken ct)
        {
            lock (_gate)
            {
                Note("docker:busy-ports");
            }

            return Task.FromResult(Result<IReadOnlySet<(string Host, int Port)>>.Success(BusyPorts));
        }

        public Task<Result> EnsureNodeAsync(ValkeyNodeSpec spec, CancellationToken ct)
        {
            lock (_gate)
            {
                var name = PlainClusterDriver.NodeName(spec.Cluster, spec.NodeName);
                Ensured.Add(spec);
                Containers[name] = new ContainerFact(
                    spec.Host, spec.ClientHostPort, spec.CpuCores, spec.MemoryBytes, spec.Args, spec.Image,
                    Guid.NewGuid().ToString("N")[..12]);
                Stopped.Remove(name);
            }

            return Task.FromResult(Result.Success());
        }

        public Action<string>? OnRemove { get; set; }

        public Task<Result> RemoveNodeAsync(string cluster, string nodeName, CancellationToken ct)
        {
            var name = PlainClusterDriver.NodeName(cluster, nodeName);
            lock (_gate)
            {
                Containers.Remove(name);
                Stopped.Remove(name);
                Removed.Add(name);
            }

            try
            {
                OnRemove?.Invoke(name);
            }
            catch (Exception ex)
            {
                return Task.FromResult(Result.Failed(ex));
            }

            return Task.FromResult(Result.Success());
        }

        public Task<Result<NodeLimits?>> NodeResourcesAsync(string cluster, string nodeName, CancellationToken ct)
        {
            if (ResourcesFault)
                return Task.FromResult(Result<NodeLimits?>.Failed(new ApplicationException("docker host mute")));

            lock (_gate)
            {
                var fact = Containers.GetValueOrDefault(PlainClusterDriver.NodeName(cluster, nodeName));
                return Task.FromResult(Result<NodeLimits?>.Success(
                    fact is null ? null : new NodeLimits(fact.Cpu, fact.Mem)));
            }
        }

        public Task<Result<NodeEndpointInspection?>> InspectNodeEndpointAsync(
            string cluster, string nodeName, CancellationToken ct)
        {
            if (EndpointFault)
                return Task.FromResult(Result<NodeEndpointInspection?>.Failed(new ApplicationException("docker host mute")));

            lock (_gate)
            {
                var name = PlainClusterDriver.NodeName(cluster, nodeName);
                var fact = Containers.GetValueOrDefault(name);
                // Реальный docker: у остановленного контейнера PortBindings
                // персистят — endpoint не-null, Running=false (положительное
                // свидетельство живого docker-факта, отказ пробы = молчание).
                return Task.FromResult(Result<NodeEndpointInspection?>.Success(
                    fact is null
                        ? null
                        : new NodeEndpointInspection(
                            fact.Host, fact.HostPort, Running: !Stopped.Contains(name))));
            }
        }

        public Task<Result<IReadOnlyList<string>?>> NodeArgsAsync(string cluster, string nodeName, CancellationToken ct)
        {
            if (ArgsFault)
                return Task.FromResult(Result<IReadOnlyList<string>?>.Failed(new ApplicationException("docker host mute")));

            lock (_gate)
            {
                var fact = Containers.GetValueOrDefault(PlainClusterDriver.NodeName(cluster, nodeName));
                return Task.FromResult(Result<IReadOnlyList<string>?>.Success(fact?.Args));
            }
        }

        public Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct)
        {
            lock (_gate)
            {
                var prefix = $"vwk-{cluster}-";
                return Task.FromResult(Result<IReadOnlyList<string>>.Success(
                    (IReadOnlyList<string>)[.. Containers.Keys.Where(k => k.StartsWith(prefix, StringComparison.Ordinal)).OrderBy(k => k, StringComparer.Ordinal)]));
            }
        }
    }

    /// <summary>
    /// In-memory модель valkey-ноды: пользователи {user → множество паролей},
    /// конфиг {param → value}; AUTH проверяет пару; PING требует валидную пару;
    /// CONFIG/ACL по образцу; переключатели «молчит» (таймаут) и «сбой соединения».
    /// </summary>
    internal sealed class FakeValkeyConnection : IValkeyConnection
    {
        public sealed class UserAccount
        {
            public HashSet<string> Passwords { get; } = [];

            public List<string> Rules { get; } = [];

            public bool On { get; set; } = true;
        }

        public readonly Dictionary<string, UserAccount> Users = [];

        public readonly Dictionary<string, string> Config = [];

        // Журнал вызовов (тесты сверяют окно двух паролей E1–E3).
        public readonly List<(string User, IReadOnlyList<string> Args)> SetUserCalls = [];

        public bool Silent { get; set; } // нода молчит (бюджет V4/UNREACHABLE)

        public bool ConnectionFault { get; set; } // слепая проба (S7)

        // Тесты полного прогона: креды генерирует ensure — пароль заранее
        // неизвестен; true = AUTH принимает любую пару (нода «собрана»).
        public bool TrustAnyPassword { get; set; }

        // Хук на каждый вызов команды (двигает FixedTimeProvider в тестах V4-бюджета).
        public Action? OnCommand { get; set; }

        // Отказ ACL SETUSER с вызова с этим индексом (краш между фазами ротации:
        // E3 — второй вызов тика). null — отказов нет.
        public int? SetUserFailFromIndex { get; set; }

        private void BeforeCommand() => OnCommand?.Invoke();

        private bool AuthOk(ValkeyEndpoint ep)
            => TrustAnyPassword
               || (Users.TryGetValue(ep.User, out var account) && account.Passwords.Contains(ep.Password));

        public UserAccount AddUser(string name, string password, params string[] rules)
        {
            var account = new UserAccount();
            account.Passwords.Add(password);
            account.Rules.AddRange(rules);
            Users[name] = account;
            return account;
        }

        // Слепая проба (S7): соединение не установилось — сетевой отказ.
        private Result Fail() => Result.Failed(new ApplicationException("valkey connection failed"));

        private Result<T> Blind<T>() where T : notnull
            => Result<T>.Failed(new ApplicationException("valkey connection failed"));

        // Нода молчит: соединение установилось, ответа нет (таймаут пробы).
        private Result SilentFail() => Result.Failed(new TimeoutException("valkey не ответил за бюджет пробы"));

        private Result<T> SilentBlind<T>() where T : notnull
            => Result<T>.Failed(new TimeoutException("valkey не ответил за бюджет пробы"));

        public Task<Result> PingAsync(ValkeyEndpoint ep, CancellationToken ct)
        {
            BeforeCommand();
            if (ConnectionFault)
                return Task.FromResult(Fail());
            if (Silent)
                return Task.FromResult(SilentFail());
            if (!AuthOk(ep))
                return Task.FromResult(Result.Failed(new ApplicationException("AUTH failed")));
            return Task.FromResult(Result.Success());
        }

        public Task<Result<IReadOnlyDictionary<string, string>>> ConfigGetAsync(
            ValkeyEndpoint ep, string parameter, CancellationToken ct)
        {
            BeforeCommand();
            if (ConnectionFault)
                return Task.FromResult(Blind<IReadOnlyDictionary<string, string>>());
            if (Silent)
                return Task.FromResult(SilentBlind<IReadOnlyDictionary<string, string>>());
            if (!AuthOk(ep))
                return Task.FromResult(Result<IReadOnlyDictionary<string, string>>.Failed(
                    new ApplicationException("AUTH failed")));
            var dict = new Dictionary<string, string>();
            if (Config.TryGetValue(parameter, out var value))
                dict[parameter] = value;
            return Task.FromResult(Result<IReadOnlyDictionary<string, string>>.Success(
                (IReadOnlyDictionary<string, string>)dict));
        }

        public Task<Result> ConfigSetAsync(ValkeyEndpoint ep, string parameter, string value, CancellationToken ct)
        {
            BeforeCommand();
            if (ConnectionFault)
                return Task.FromResult(Fail());
            if (Silent)
                return Task.FromResult(SilentFail());
            if (!AuthOk(ep))
                return Task.FromResult(Result.Failed(new ApplicationException("AUTH failed")));
            Config[parameter] = value;
            return Task.FromResult(Result.Success());
        }

        public Task<Result<IReadOnlyList<string>>> AclListAsync(ValkeyEndpoint ep, CancellationToken ct)
        {
            BeforeCommand();
            if (ConnectionFault)
                return Task.FromResult(Blind<IReadOnlyList<string>>());
            if (Silent)
                return Task.FromResult(SilentBlind<IReadOnlyList<string>>());
            if (!AuthOk(ep))
                return Task.FromResult(Result<IReadOnlyList<string>>.Failed(
                    new ApplicationException("AUTH failed")));
            var rules = Users.Select(u =>
            {
                var on = u.Value.On ? "on" : "off";
                var passwords = string.Join(' ', u.Value.Passwords.Select(p => $"#{p}"));
                var rights = u.Value.Rules.Count > 0 ? ' ' + string.Join(' ', u.Value.Rules) : "";
                return $"user {u.Key} {on} {passwords}{rights}";
            }).ToList();
            return Task.FromResult(Result<IReadOnlyList<string>>.Success((IReadOnlyList<string>)rules));
        }

        public Task<Result> AclSetUserAsync(ValkeyEndpoint ep, IReadOnlyList<string> args, CancellationToken ct)
        {
            BeforeCommand();
            if (ConnectionFault)
                return Task.FromResult(Fail());
            if (Silent)
                return Task.FromResult(SilentFail());
            if (!AuthOk(ep))
                return Task.FromResult(Result.Failed(new ApplicationException("AUTH failed")));
            if (SetUserFailFromIndex is { } from && SetUserCalls.Count >= from)
                return Task.FromResult(Result.Failed(new ApplicationException("ACL SETUSER failed")));

            // args: [user, модификаторы…] — модель по образцу valkey.
            var target = args[0];
            if (!Users.TryGetValue(target, out var changed))
                changed = AddUser(target, "");
            for (var i = 1; i < args.Count; i++)
            {
                var arg = args[i];
                if (arg.StartsWith('>')
                    && arg.Length > 1)
                    changed.Passwords.Add(arg[1..]);
                else if (arg.StartsWith('<')
                    && arg.Length > 1)
                    changed.Passwords.Remove(arg[1..]);
                else if (arg is "on" or "off")
                    changed.On = arg == "on";
                else if (!changed.Rules.Contains(arg))
                    changed.Rules.Add(arg);
            }

            SetUserCalls.Add((target, args));
            return Task.FromResult(Result.Success());
        }
    }
}
