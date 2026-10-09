using FluentAssertions;
using Metrics.SdGenerator;
using Microsoft.Extensions.Logging.Abstractions;
using Shared.Core;
using Shared.Etcd.Client;
using Xunit;

namespace Metrics.SdGenerator.UnitTests;

// Цикл генератора: failover-Range → Map → WriteIfChanged → MarkSuccess;
// консервативная свежесть (ошибка etcd — файл/метрика не тронуты).
public class SdGeneratorLoopTests : IDisposable
{
    // Фейк gateway: словарь возвращаемых Result по endpoint.
    private sealed class FakeEtcdGateway : IEtcdGateway
    {
        private readonly Dictionary<string, Result<IReadOnlyList<Kv>>> _ranges = [];

        public void SetRange(string endpoint, Result<IReadOnlyList<Kv>> result) => _ranges[endpoint] = result;

        public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
            => Task.FromResult(_ranges.TryGetValue(endpoint, out var result)
                ? result
                : Result<IReadOnlyList<Kv>>.Failed(new EtcdUnreachableException($"{endpoint}: нет мока")));

        public Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result<long>> LeaseGrantAsync(string endpoint, int ttlSec, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result> LeaseRevokeAsync(string endpoint, long lease, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result> LeaseKeepaliveAsync(string endpoint, long lease, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result> CompactAsync(string endpoint, long revision, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result> DefragmentAsync(string endpoint, CancellationToken ct)
            => throw new NotSupportedException();
    }

    private const string KvValue =
        """{"shard1/shard1a":{"host":"h1","pg":5432,"patroni":8008,"doorman":6432}}""";

    private readonly List<IDisposable> _disposables = [];
    private readonly List<string> _dirs = [];

    private (SdGeneratorLoop Loop, SdGeneratorMetrics Metrics, string Path, FakeEtcdGateway Etcd) NewLoop(
        params string[] endpoints)
    {
        // Arrange: harness цикла — temp-файл, свой Meter (standalone), фейк etcd.
        var dir = Path.Combine(Path.GetTempPath(), "sd-loop-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        _dirs.Add(dir);
        var path = Path.Combine(dir, "sd.json");
        var etcd = new FakeEtcdGateway();
        var meter = new System.Diagnostics.Metrics.Meter("sd-unit");
        _disposables.Add(meter);
        var metrics = new SdGeneratorMetrics(meter, TimeProvider.System);
        var loop = new SdGeneratorLoop(
            etcd, endpoints, new SdFileWriter(path), metrics,
            NullLogger<SdGeneratorLoop>.Instance);
        return (loop, metrics, path, etcd);
    }

    public void Dispose()
    {
        foreach (var d in _disposables)
            d.Dispose();
        foreach (var dir in _dirs.Where(Directory.Exists))
            Directory.Delete(dir, recursive: true);
    }

    [Fact]
    public async Task Tick_Success_WritesFileAndMetric()
    {
        // Arrange
        var (loop, metrics, path, etcd) = NewLoop("ep1");
        var kv = new Kv("/pgworker/portalloc/c1", KvValue, ModRevision: 1);
        etcd.SetRange("ep1", Result<IReadOnlyList<Kv>>.Success([kv]));
        var before = TimeProvider.System.GetUtcNow().ToUnixTimeSeconds();

        // Act
        var ok = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert: файл = Serialize(групп), метрика живая
        ok.Should().BeTrue();
        File.ReadAllText(path).Should().Be(TargetMapping.Serialize(
            [new SdTargetGroup("h1:8008", "c1", "shard1", "shard1a")]));
        metrics.LastSuccessUnix.Should().NotBeNull().And.BeGreaterThanOrEqualTo(before);
    }

    [Fact]
    public async Task Tick_EmptyPrefix_IsSuccessWithEmptyArray()
    {
        // Arrange: кластеров нет — пустой file_sd, тик валиден
        var (loop, metrics, path, etcd) = NewLoop("ep1");
        etcd.SetRange("ep1", Result<IReadOnlyList<Kv>>.Success([]));

        // Act
        var ok = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert
        ok.Should().BeTrue();
        File.ReadAllText(path).Should().Be("[]");
        metrics.LastSuccessUnix.Should().NotBeNull();
    }

    [Fact]
    public async Task Tick_EtcdError_KeepsFileAndMetricUntouched()
    {
        // Arrange: файл pre-written «X», успехов ещё не было (метрика null)
        var (loop, metrics, path, etcd) = NewLoop("ep1", "ep2");
        await File.WriteAllTextAsync(path, "X", TestContext.Current.CancellationToken);
        etcd.SetRange("ep1", Result<IReadOnlyList<Kv>>.Failed(new EtcdUnreachableException("ep1 лежит")));
        etcd.SetRange("ep2", Result<IReadOnlyList<Kv>>.Failed(new EtcdUnreachableException("ep2 лежит")));
        var contentBefore = await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken);

        // Act
        var ok = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert: файл байт-в-байт прежний, метрика не тронута
        ok.Should().BeFalse();
        (await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken)).Should().Equal(contentBefore);
        metrics.LastSuccessUnix.Should().BeNull();
    }

    [Fact]
    public async Task Tick_Failover_FallsBackToSecondEndpoint()
    {
        // Arrange: ep1 недоступен, ep2 отвечает
        var (loop, _, path, etcd) = NewLoop("ep1", "ep2");
        etcd.SetRange("ep1", Result<IReadOnlyList<Kv>>.Failed(new EtcdUnreachableException("ep1 лежит")));
        etcd.SetRange("ep2", Result<IReadOnlyList<Kv>>.Success(
            [new Kv("/pgworker/portalloc/c1", KvValue, ModRevision: 1)]));

        // Act
        var ok = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert
        ok.Should().BeTrue();
        File.ReadAllText(path).Should().Contain("h1:8008");
    }

    [Fact]
    public async Task Tick_SameContent_DoesNotTouchFile()
    {
        // Arrange: одинаковый range на два тика
        var (loop, _, path, etcd) = NewLoop("ep1");
        etcd.SetRange("ep1", Result<IReadOnlyList<Kv>>.Success(
            [new Kv("/pgworker/portalloc/c1", KvValue, ModRevision: 1)]));

        // Act
        (await loop.TickAsync(TestContext.Current.CancellationToken)).Should().BeTrue();
        var mtimeBefore = File.GetLastWriteTimeUtc(path);
        await Task.Delay(20, TestContext.Current.CancellationToken);
        var second = await loop.TickAsync(TestContext.Current.CancellationToken);

        // Assert: контент не изменился — mtime не дёргается
        second.Should().BeTrue();
        File.GetLastWriteTimeUtc(path).Should().Be(mtimeBefore);
    }
}
