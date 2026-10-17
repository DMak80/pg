using PgWorker.Etcd.S3;
using Shared.Core;
using Shared.Etcd.Client;

namespace PgWorker.UnitTests.Etcd;

// S3PoolStore (t36, arch/23 §7): сборка пула из словаря Kv-заглушек —
// имена нод из ключей заявки, адреса из portalloc; нода без portalloc-записи
// не выпадает из пула (Host=null); нет config → null.
public class S3PoolStoreParseTests
{
    // AAA: полный набор ключей → пул с адресами и PlacedOn (state из листа).
    [Fact]
    public async Task Read_FullKeySet_PoolWithAddresses()
    {
        // Arrange — config Active, заявка двух нод (state+placement),
        // placed_on, portalloc на обе, endpoints-строка.
        var store = new S3PoolStore(
            new FakeGateway(new Dictionary<string, string>
            {
                ["/s3/clusters/backups/config"] =
                    """{"platform":"owns3","nodes":2,"secret_profile":"backups-main","created_unix":1}""",
                ["/s3/clusters/backups/nodes/east1/state"] = "\"RUNNING\"",
                ["/s3/clusters/backups/nodes/east1/placement"] = """{"docker_host":"h1"}""",
                ["/s3/clusters/backups/nodes/east1/placed_on"] = "\"h1\"",
                ["/s3/clusters/backups/nodes/east2/state"] = "\"RUNNING\"",
                ["/s3/clusters/backups/nodes/east2/placement"] = """{"docker_host":"h2"}""",
                ["/s3/clusters/backups/nodes/east2/placed_on"] = "\"h2\"",
                ["/s3/clusters/backups/endpoints"] = "h1:18001,h2:18002",
                ["/s3worker/portalloc/backups"] =
                    """{"east1":{"host":"h1","s3":18001},"east2":{"host":"h2","s3":18002}}""",
            }),
            ["http://etcd:2379"]);

        // Act
        var result = await store.ReadAsync("backups", CancellationToken.None);

        // Assert — пул собран: адреса/PlacedOn на месте, endpoints в порядке ключа.
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().NotBeNull();
        var pool = result.Value!;
        pool.Name.Should().Be("backups");
        pool.SecretProfile.Should().Be("backups-main");
        pool.ToRemove.Should().BeFalse();
        pool.Endpoints.Should().Equal("h1:18001", "h2:18002");
        pool.Nodes.Should().HaveCount(2);
        var east1 = pool.Nodes.Should().Contain(n => n.Name == "east1").Subject;
        east1.Host.Should().Be("h1");
        east1.S3Port.Should().Be(18001);
        east1.PlacedOn.Should().Be("h1");
        east1.State.Should().Be("RUNNING");
        pool.Nodes.Should().Contain(n => n.Name == "east2" && n.Host == "h2" && n.S3Port == 18002);
    }

    // AAA: инстанс с placement без portalloc-записи → нода в Nodes с Host=null
    // (заявлена, не провижинена — не выпадает из пула, arch/19 §4).
    [Fact]
    public async Task Read_NodeWithoutPortAlloc_KeptWithNullAddress()
    {
        // Arrange — portalloc знает только east1; east2 заявлена, адреса нет.
        var store = new S3PoolStore(
            new FakeGateway(new Dictionary<string, string>
            {
                ["/s3/clusters/backups/config"] =
                    """{"platform":"owns3","nodes":2,"secret_profile":"p","created_unix":1}""",
                ["/s3/clusters/backups/nodes/east1/state"] = "\"RUNNING\"",
                ["/s3/clusters/backups/nodes/east1/placement"] = """{"docker_host":"h1"}""",
                ["/s3/clusters/backups/nodes/east2/state"] = "\"PROVISIONING\"",
                ["/s3/clusters/backups/nodes/east2/placement"] = """{"docker_host":"h2"}""",
                ["/s3/clusters/backups/endpoints"] = "h1:18001",
                ["/s3worker/portalloc/backups"] = """{"east1":{"host":"h1","s3":18001}}""",
            }),
            ["http://etcd:2379"]);

        // Act
        var result = await store.ReadAsync("backups", CancellationToken.None);

        // Assert — east2 в пуле с null-адресом.
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().NotBeNull();
        var pool = result.Value!;
        pool.Nodes.Should().HaveCount(2);
        var east2 = pool.Nodes.Should().Contain(n => n.Name == "east2").Subject;
        east2.Host.Should().BeNull();
        east2.S3Port.Should().BeNull();
    }

    // AAA: нет config → null (пула нет; не Failed).
    [Fact]
    public async Task Read_NoConfig_ReturnsNull()
    {
        // Arrange — пустой etcd.
        var store = new S3PoolStore(
            new FakeGateway(new Dictionary<string, string>()),
            ["http://etcd:2379"]);

        // Act
        var result = await store.ReadAsync("missing", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeNull();
    }

    // AAA: ListPoolNamesAsync — уникальные <C> из config-ключей, сортированы.
    [Fact]
    public async Task ListPoolNames_ConfigKeysOnly()
    {
        // Arrange — config двух пулов + посторонние ключи (nodes/endpoints).
        var store = new S3PoolStore(
            new FakeGateway(new Dictionary<string, string>
            {
                ["/s3/clusters/beta/config"] = "{}",
                ["/s3/clusters/alpha/config"] = "{}",
                ["/s3/clusters/alpha/endpoints"] = "h1:1",
                ["/s3/clusters/alpha/nodes/n1/state"] = "\"RUNNING\"",
            }),
            ["http://etcd:2379"]);

        // Act
        var result = await store.ListPoolNamesAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Equal("alpha", "beta");
    }

    // Фейк gateway: словарь ключ→значение; range по префиксу — точное
    // совпадение либо префикс (достаточно для схемы /s3/).
    private sealed class FakeGateway(Dictionary<string, string> data) : IEtcdGateway
    {
        public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
        {
            var kvs = data
                .Where(kv => kv.Key == prefix || kv.Key.StartsWith(prefix, StringComparison.Ordinal))
                .Select(kv => new Kv(kv.Key, kv.Value, 1))
                .OrderBy(kv => kv.Key, StringComparer.Ordinal)
                .ToList();
            return Task.FromResult(Result<IReadOnlyList<Kv>>.Success(kvs));
        }

        public Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
            => Task.FromResult(Result<Kv?>.Success(
                data.TryGetValue(key, out var v) ? new Kv(key, v, 1) : null));

        public Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct)
            => Task.FromResult(Result<TxnResult>.Success(new TxnResult(true)));

        public Task<Result<long>> LeaseGrantAsync(string endpoint, int ttlSec, CancellationToken ct)
            => Task.FromResult(Result<long>.Success(1));

        public Task<Result> LeaseRevokeAsync(string endpoint, long lease, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result> LeaseKeepaliveAsync(string endpoint, long lease, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<byte[]>.Success([]));

        public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
            => throw new NotSupportedException();

        public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<EtcdMember>>.Success([]));

        public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<EtcdAlarm>>.Success([]));

        public Task<Result> CompactAsync(string endpoint, long revision, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result> DefragmentAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result.Success());
    }
}
