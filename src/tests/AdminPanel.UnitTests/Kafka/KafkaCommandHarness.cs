using AdminPanel.Api.Operations.Kafka;
using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using AdminPanel.Infrastructure;
using FluentAssertions;

namespace AdminPanel.UnitTests.Kafka;

// Тест-обвязка kafka-команд: etcd в памяти с mod_revision/version и честным
// txn-compare (version + mod_revision), общий снапшот-стор с живым endpoint.
internal static class KafkaCommandHarness
{
    internal const string Endpoint = "http://etcd:2379";

    internal sealed class FakeKafkaEtcd : IEtcdGateway
    {
        internal sealed record Entry(string Value, long ModRevision, long Version);

        public readonly Dictionary<string, Entry> Store = [];
        public readonly List<(IReadOnlyList<TxnCompare> Compares, IReadOnlyList<KvPut> Puts)> Txns = [];
        public Action? OnTxn; // хук тестов: конкурентная запись до compare
        private long _rev;
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

        public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<EtcdStatusPayload>.Success(new(null, null, null, null, null)));

        public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<EtcdMember>>.Success([]));

        public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<EtcdAlarm>>.Success([]));

        public Task<Result> PutAsync(string endpoint, string key, string value, CancellationToken ct)
        {
            lock (_gate)
            {
                Store[key] = new Entry(value, ++_rev, Store.TryGetValue(key, out var old) ? old.Version + 1 : 1);
            }

            return Task.FromResult(Result.Success());
        }

        public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
        {
            lock (_gate)
            {
                foreach (var key in Store.Keys.Where(k => prefix
                             ? k.StartsWith(keyOrPrefix, StringComparison.Ordinal)
                             : k == keyOrPrefix).ToList())
                    Store.Remove(key);
            }

            return Task.FromResult(Result.Success());
        }

        public Task<Result<TxnResult>> TxnAsync(
            string endpoint, IReadOnlyList<TxnCompare> compares, IReadOnlyList<KvPut> puts, CancellationToken ct)
        {
            bool succeeded;
            lock (_gate)
            {
                OnTxn?.Invoke();
                Txns.Add((compares, puts));
                succeeded = compares.All(c => c.ModRevision is { } mod
                    ? Store.TryGetValue(c.Key, out var e) && e.ModRevision == mod
                    : Store.TryGetValue(c.Key, out var v)
                        ? v.Version == c.Version
                        : c.Version == 0);
                if (succeeded)
                    foreach (var put in puts)
                        Store[put.Key] = new Entry(
                            put.Value, ++_rev, Store.TryGetValue(put.Key, out var old) ? old.Version + 1 : 1);
            }

            return Task.FromResult(Result<TxnResult>.Success(new TxnResult(succeeded)));
        }

        // Сид прямым PUT (сборка состояния кластера в тестах).
        public void Seed(string key, string value)
        {
            Store[key] = new Entry(value, ++_rev, Store.TryGetValue(key, out var old) ? old.Version + 1 : 1);
        }
    }

    // Общий снапшот-стор с живым endpoint (pg-базис Healthy + ActiveEndpoint).
    internal static SnapshotStore StoreWithEndpoint()
    {
        var store = new SnapshotStore();
        store.Replace(TestSnapshots.Healthy(DateTimeOffset.UnixEpoch) with
        {
            Etcd = new EtcdStatus(
                true, [new EtcdEndpoint(Endpoint, true, 1, "3.5.21", null, null, null, null, [])],
                [], [], Endpoint, false, DateTimeOffset.UnixEpoch, 0),
        });
        return store;
    }

    // Сид Active kafka-кластера events: config + 3 брокера (broker1..3 controller) + endpoints.
    internal static void SeedActiveCluster(FakeKafkaEtcd etcd, int brokers = 3)
    {
        etcd.Seed($"/kafka/clusters/events/config",
            $$"""{"brokers":{{brokers}},"replication_factor":3,"min_insync_replicas":2,"default_partitions":12,"default_retention_ms":604800000,"created_unix":1756500000}""");
        for (var k = 1; k <= brokers; k++)
        {
            etcd.Seed($"/kafka/clusters/events/brokers/broker{k}/state", "RUNNING");
            etcd.Seed($"/kafka/clusters/events/brokers/broker{k}/role", k <= Math.Min(3, brokers) ? "controller" : "broker");
            etcd.Seed($"/kafka/clusters/events/brokers/broker{k}/resources",
                """{"cpu":"2","mem":"4Gi","disk":"40Gi"}""");
        }

        etcd.Seed("/kafka/clusters/events/endpoints", "h1:16000,h1:16001,h1:16002");
        etcd.Seed("/kafka/clusters/events/app_user", "app");
        etcd.Seed("/kafka/clusters/events/app_password", "OldPassword0123456789abcdef");
    }
}
