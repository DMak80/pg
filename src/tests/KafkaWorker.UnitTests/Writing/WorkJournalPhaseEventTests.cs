using KafkaWorker.Core;
using KafkaWorker.Etcd.Client;
using KafkaWorker.Etcd.Coordination;
using Xunit;

namespace KafkaWorker.UnitTests.Writing;

// Событие WorkJournal.PhaseWritten (t04, seam S2, зеркало PgWorker): канал
// фазовых записей для метрик (arch/18 §2.2). WriteSupervisionAsync событие
// НЕ эмитит (supervise подавлен в сериях; решение ревью Ф4-2).
public class WorkJournalPhaseEventTests
{
    // Мини-имитация etcd в памяти с настраиваемым результатом Put.
    private sealed class FakeGateway : IEtcdGateway
    {
        public Dictionary<string, string> Store = [];

        // true — все Put падают (etcd недоступен).
        public bool FailPuts;

        public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
        {
            var kvs = Store
                .Where(p => p.Key.StartsWith(prefix, StringComparison.Ordinal))
                .Select(p => new Kv(p.Key, p.Value, 1))
                .ToList();
            return Task.FromResult(Result<IReadOnlyList<Kv>>.Success(kvs));
        }

        public Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
            => Task.FromResult(Result<Kv?>.Success(
                Store.TryGetValue(key, out var v) ? new Kv(key, v, 1) : null));

        public Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
        {
            if (FailPuts)
                return Task.FromResult(Result.Failed(new HttpRequestException("etcd недоступен")));
            Store[key] = value;
            return Task.FromResult(Result.Success());
        }

        public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct)
            => Task.FromResult(Result<TxnResult>.Success(new TxnResult(true)));

        public Task<Result<long>> LeaseGrantAsync(string endpoint, int ttlSec, CancellationToken ct)
            => Task.FromResult(Result<long>.Success(100));

        public Task<Result> LeaseRevokeAsync(string endpoint, long lease, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result> LeaseKeepaliveAsync(string endpoint, long lease, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<byte[]>.Success([]));

        public Task<Result<long>> StatusAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<long>.Success(1));

        public Task<Result> CompactAsync(string endpoint, long revision, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result> DefragmentAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result.Success());
    }

    [Fact]
    public async Task WriteAsync_Success_EmitsEvent_WithClusterOpPhase()
    {
        // Arrange: журнал над живым фейком etcd; подписка собирает события
        var gateway = new FakeGateway();
        var journal = new WorkJournal(gateway, ["http://fake"]);
        var events = new List<WorkJournal.WorkPhaseEntry>();
        journal.PhaseWritten += e => events.Add(e);

        // Act
        var result = await journal.WriteAsync("demo", "provision", "started", "i1", null, TestContext.Current.CancellationToken);

        // Assert: успешная фазовая запись → событие с фактическим (cluster, op, phase)
        result.IsSuccess.Should().BeTrue();
        events.Should().ContainSingle(e => e.Cluster == "demo" && e.Op == "provision" && e.Phase == "started");
    }

    [Fact]
    public async Task WriteAsync_FailedPut_DoesNotEmitEvent()
    {
        // Arrange: Put падает (etcd недоступен)
        var gateway = new FakeGateway { FailPuts = true };
        var journal = new WorkJournal(gateway, ["http://fake"]);
        var emitted = false;
        journal.PhaseWritten += _ => emitted = true;

        // Act
        var result = await journal.WriteAsync("demo", "provision", "started", "i1", null, TestContext.Current.CancellationToken);

        // Assert: событие после НЕуспешной записи не эмитится — метрики не врут
        result.IsSuccess.Should().BeFalse();
        emitted.Should().BeFalse();
    }

    [Fact]
    public async Task WriteSupervisionAsync_Success_DoesNotEmitEvent()
    {
        // Arrange: надзор — не фазовый процесс (arch/18 §2.2, ревью Ф4-2).
        var gateway = new FakeGateway();
        var journal = new WorkJournal(gateway, ["http://fake"]);
        var emitted = false;
        journal.PhaseWritten += _ => emitted = true;

        // Act
        var result = await journal.WriteSupervisionAsync("demo", "i1", new Dictionary<string, long>(), null, TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeTrue();
        emitted.Should().BeFalse();
    }

    [Fact]
    public async Task WriteAsync_TerminalCrashed_EmitsEvent()
    {
        // Arrange: терминальные фазы (crashed) приходят тем же путём WriteAsync
        var gateway = new FakeGateway();
        var journal = new WorkJournal(gateway, ["http://fake"]);
        var events = new List<WorkJournal.WorkPhaseEntry>();
        journal.PhaseWritten += events.Add;

        // Act
        var result = await journal.WriteAsync("demo", "reassign", "crashed", "i1", "boom", TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeTrue();
        events.Should().ContainSingle(e => e.Cluster == "demo" && e.Op == "reassign" && e.Phase == "crashed");
    }
}
