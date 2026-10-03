using Shared.Etcd.Maintenance;
using FluentAssertions;
using Xunit;

namespace Shared.Etcd.UnitTests.Maintenance;

// SnapshotJob + опциональный sink (t08, spec §3.4): экспорт вызывается после
// записи локального файла с best-effort ревизией; сбой sink (Result.Failed и
// исключение) НЕ роняет снятие; sink=null — прежнее поведение.
public class SnapshotJobSinkTests
{
    private const string Ep = "http://etcd:2379";

    private sealed class FakeEtcd(byte[] snapshot) : Shared.Etcd.Client.IEtcdGateway
    {
        public Task<Shared.Core.Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Shared.Core.Result<byte[]>.Success(snapshot));
        public Task<Shared.Core.Result<Shared.Etcd.Client.EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Shared.Core.Result<Shared.Etcd.Client.EtcdStatusPayload>.Success(
                new Shared.Etcd.Client.EtcdStatusPayload(null, null, null, null, null, 77)));
        // остальные члены интерфейса — Success-заглушки (не нужны сценарию)
        public Task<Shared.Core.Result<IReadOnlyList<Shared.Etcd.Client.Kv>>> RangeAsync(string e, string p, CancellationToken ct)
            => Task.FromResult(Shared.Core.Result<IReadOnlyList<Shared.Etcd.Client.Kv>>.Success([]));
        public Task<Shared.Core.Result<Shared.Etcd.Client.Kv?>> GetAsync(string e, string k, CancellationToken ct)
            => Task.FromResult(Shared.Core.Result<Shared.Etcd.Client.Kv?>.Success(null));
        public Task<Shared.Core.Result> PutAsync(string e, string k, string v, long? l, CancellationToken ct)
            => Task.FromResult(Shared.Core.Result.Success());
        public Task<Shared.Core.Result> DeleteAsync(string e, string k, bool p, CancellationToken ct)
            => Task.FromResult(Shared.Core.Result.Success());
        public Task<Shared.Core.Result<Shared.Etcd.Client.TxnResult>> TxnAsync(string e, Shared.Etcd.Client.TxnRequest r, CancellationToken ct)
            => Task.FromResult(Shared.Core.Result<Shared.Etcd.Client.TxnResult>.Success(new Shared.Etcd.Client.TxnResult(true)));
        public Task<Shared.Core.Result<long>> LeaseGrantAsync(string e, int t, CancellationToken ct)
            => Task.FromResult(Shared.Core.Result<long>.Success(1));
        public Task<Shared.Core.Result> LeaseRevokeAsync(string e, long l, CancellationToken ct)
            => Task.FromResult(Shared.Core.Result.Success());
        public Task<Shared.Core.Result> LeaseKeepaliveAsync(string e, long l, CancellationToken ct)
            => Task.FromResult(Shared.Core.Result.Success());
        public Task<Shared.Core.Result<IReadOnlyList<Shared.Etcd.Client.EtcdMember>>> MemberListAsync(string e, CancellationToken ct)
            => Task.FromResult(Shared.Core.Result<IReadOnlyList<Shared.Etcd.Client.EtcdMember>>.Success([]));
        public Task<Shared.Core.Result<IReadOnlyList<Shared.Etcd.Client.EtcdAlarm>>> AlarmAsync(string e, CancellationToken ct)
            => Task.FromResult(Shared.Core.Result<IReadOnlyList<Shared.Etcd.Client.EtcdAlarm>>.Success([]));
        public Task<Shared.Core.Result> CompactAsync(string e, long r, CancellationToken ct)
            => Task.FromResult(Shared.Core.Result.Success());
        public Task<Shared.Core.Result> DefragmentAsync(string e, CancellationToken ct)
            => Task.FromResult(Shared.Core.Result.Success());
    }

    private sealed class RecordingSink : ISnapshotSink
    {
        public List<(string FileName, byte[] Data, long? Revision)> Calls { get; } = [];
        public bool Throw { get; set; }
        public Task<Shared.Core.Result> ExportAsync(string snapshotFileName, byte[] data, long? revision, CancellationToken ct)
        {
            if (Throw) throw new ApplicationException("sink взорвался");
            Calls.Add((snapshotFileName, data, revision));
            return Task.FromResult(Shared.Core.Result.Success());
        }
    }

    private static string TempDir() => Directory.CreateTempSubdirectory("snap-sink-").FullName;

    // AAA: снятие вызывает sink именем файла, байтами и ревизией StatusAsync
    [Fact]
    public async Task TakeAsync_вызывает_sink_после_локальной_записи()
    {
        // Arrange
        var sink = new RecordingSink();
        var job = new SnapshotJob(new FakeEtcd([1, 2, 3]), [Ep], TempDir(), 10, 60, sink);

        // Act
        var shot = await job.TakeAsync(TestContext.Current.CancellationToken);

        // Assert — путь локального файла (прежний контракт) + вызов sink
        shot.IsSuccess.Should().BeTrue();
        File.Exists(shot.Value).Should().BeTrue();
        sink.Calls.Should().ContainSingle(c => c.FileName.StartsWith("snapshot-") && c.FileName.EndsWith(".db")
            && c.Data.SequenceEqual(new byte[] { 1, 2, 3 }) && c.Revision == 77);
    }

    // AAA: Result.Failed sink не роняет снятие (локальный файл — истина снятия)
    [Fact]
    public async Task TakeAsync_сбой_sink_не_роняет_снятие()
    {
        // Arrange
        var sink = new FailingSink();
        var job = new SnapshotJob(new FakeEtcd([1]), [Ep], TempDir(), 10, 60, sink);

        // Act
        var shot = await job.TakeAsync(TestContext.Current.CancellationToken);

        // Assert
        shot.IsSuccess.Should().BeTrue("сбой выгрузки — транзиент, доводка догонит (spec §2.3)");
        File.Exists(shot.Value).Should().BeTrue();
    }

    // AAA: исключение реализации sink — тоже не роняет снятие
    [Fact]
    public async Task TakeAsync_исключение_sink_не_роняет_снятие()
    {
        // Arrange
        var sink = new RecordingSink { Throw = true };
        var job = new SnapshotJob(new FakeEtcd([1]), [Ep], TempDir(), 10, 60, sink);

        // Act
        var shot = await job.TakeAsync(TestContext.Current.CancellationToken);

        // Assert
        shot.IsSuccess.Should().BeTrue();
    }

    // AAA: sink=null — прежнее поведение (KafkaWorker/ValkeyWorker)
    [Fact]
    public async Task TakeAsync_без_sink_пишет_файл()
    {
        // Arrange
        var job = new SnapshotJob(new FakeEtcd([9]), [Ep], TempDir(), 10, 60);

        // Act
        var shot = await job.TakeAsync(TestContext.Current.CancellationToken);

        // Assert
        shot.IsSuccess.Should().BeTrue();
        var bytes = await File.ReadAllBytesAsync(shot.Value, TestContext.Current.CancellationToken);
        bytes.Should().Equal(9);
    }

    private sealed class FailingSink : ISnapshotSink
    {
        public Task<Shared.Core.Result> ExportAsync(string snapshotFileName, byte[] data, long? revision, CancellationToken ct)
            => Task.FromResult(Shared.Core.Result.Failed(new ApplicationException("S3 недоступен")));
    }
}
