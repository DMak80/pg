using System.Diagnostics;
using System.Net.Http;
using Microsoft.Extensions.Logging.Abstractions;
using OwnS3.Replication;
using OwnS3.Replication.Transport;

namespace OwnS3.UnitTests.Replication;

// Фейк-транспорт probe: per-имя исходы ping, управляемые исключения и задержка
// (кейсы параллельности); остальные операции грани — вне контекста probe.
public sealed class FakeProbeTransport : IReplicationTransport
{
    public Dictionary<string, PingResult> Results { get; } = new();
    public HashSet<string> Throwers { get; } = new();
    public TimeSpan Delay { get; set; } = TimeSpan.Zero;

    public Task<Result<PingResult>> PingAsync(TargetRecord target, TimeSpan timeout, CancellationToken ct)
    {
        if (Throwers.Contains(target.Name))
            return Task.FromResult(Result<PingResult>.Failed(
                new PeerRejectedException("connection refused (тест)")));
        if (Delay <= TimeSpan.Zero)
            return Task.FromResult(Result<PingResult>.Success(
                Results.GetValueOrDefault(target.Name, new PingResult(true, 1))));
        return Task.Run(async () =>
        {
            await Task.Delay(Delay, ct);
            return Result<PingResult>.Success(
                Results.GetValueOrDefault(target.Name, new PingResult(true, 1)));
        }, ct);
    }

    public Task<Result<long>> SendNotificationsAsync(TargetRecord t, NotificationBatchRequest b, CancellationToken ct) =>
        throw new NotSupportedException("probe не рассылает");

    public Task<Result<FetchOutcome>> FetchObjectAsync(TargetRecord t, string bucket, string key, CancellationToken ct) =>
        throw new NotSupportedException("probe не фетчит");

    public Task<Result<ReplicationListingResponse>> ListAsync(TargetRecord t, string bucket, string? marker,
        int maxKeys, CancellationToken ct) => throw new NotSupportedException("probe не листит");

    public Task<Result<IReadOnlyList<(string Name, long CreationUnixMs)>>> ListingBucketsAsync(TargetRecord t,
        CancellationToken ct) => throw new NotSupportedException("probe не листит");
}

// ReplicationProber (глава 07 §2): параллельный ping снапшота targets, фильтр
// одного соседа, гашение per-target исключений, маркер 400 неизвестного имени.
// Dispose чистит temp-каталоги targets.json (правило полной самоочистки тестов).
public class ReplicationProberTests : IDisposable
{
    private readonly List<string> _tempDirs = [];

    private (ReplicationProber Prober, FakeProbeTransport Transport) Prober(
        ReplicationOptions? options = null, params string[] names)
    {
        var sysDir = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));
        _tempDirs.Add(sysDir);
        var targets = OwnS3.Replication.ReplicationTargets.Load(sysDir,
            names.Select(n => new TargetOptions(n, $"http://{n}:9000", "ak-" + n, "sk-" + n)),
            NullLogger<OwnS3.Replication.ReplicationTargets>.Instance);
        var transport = new FakeProbeTransport();
        return (new ReplicationProber(targets, transport, options ?? new ReplicationOptions()), transport);
    }

    public void Dispose()
    {
        // Teardown: temp-каталоги targets.json удаляются при любом исходе
        foreach (var dir in _tempDirs)
            if (Directory.Exists(dir))
                Directory.Delete(dir, recursive: true);
    }

    [Fact]
    public async Task AllNeighborsReachable_200_RttPropagated()
    {
        // Arrange — два соседа, оба отвечают 200
        var (prober, transport) = Prober(names: ["peer-a", "peer-b"]);
        transport.Results["peer-a"] = new PingResult(true, 4);
        transport.Results["peer-b"] = new PingResult(true, 7);

        // Act
        var report = (await prober.ProbeAsync(null, TestContext.Current.CancellationToken)).Throw().Value;

        // Assert — active=true, оба reachable, rttMs прокинуты из транспорта
        report.Active.Should().BeTrue();
        report.Targets.Should().HaveCount(2);
        report.Targets.Should().Contain(o => o.Name == "peer-a" && o.Reachable && o.RttMs == 4);
        report.Targets.Should().Contain(o => o.Name == "peer-b" && o.Reachable && o.RttMs == 7);
    }

    [Fact]
    public async Task Non200Neighbor_ReachableFalseWithRtt()
    {
        // Arrange — сосед отвечает HTTP, но не 200 (403/400 — линк не работает)
        var (prober, transport) = Prober(names: ["peer-a"]);
        transport.Results["peer-a"] = new PingResult(false, 12);

        // Act
        var report = (await prober.ProbeAsync(null, TestContext.Current.CancellationToken)).Throw().Value;

        // Assert — reachable=false, rttMs присутствует («отвечает, но не пускает»)
        report.Targets.Single().Reachable.Should().BeFalse();
        report.Targets.Single().RttMs.Should().Be(12);
    }

    [Fact]
    public async Task TransportError_ReachableFalseWithoutRtt()
    {
        // Arrange — транспортное исключение (обрыв/refused)
        var (prober, transport) = Prober(names: ["peer-a"]);
        transport.Throwers.Add("peer-a");

        // Act
        var report = (await prober.ProbeAsync(null, TestContext.Current.CancellationToken)).Throw().Value;

        // Assert — отказ транспорта погашен исходом: reachable=false, rttMs отсутствует
        report.Targets.Single().Reachable.Should().BeFalse();
        report.Targets.Single().RttMs.Should().BeNull();
    }

    [Fact]
    public async Task TargetFilter_ExactlyOneOutcome()
    {
        // Arrange — три соседа; фильтр по имени peer-b
        var (prober, _) = Prober(names: ["peer-a", "peer-b", "peer-c"]);

        // Act
        var report = (await prober.ProbeAsync("peer-b", TestContext.Current.CancellationToken)).Throw().Value;

        // Assert — ровно один исход, ровно запрошенный сосед
        report.Targets.Should().ContainSingle(o => o.Name == "peer-b");
        report.Active.Should().BeTrue();
    }

    [Fact]
    public async Task UnknownTargetName_FailedUnknownTarget()
    {
        // Arrange — имя вне состава
        var (prober, _) = Prober(names: ["peer-a"]);

        // Act
        var probe = await prober.ProbeAsync("ghost", TestContext.Current.CancellationToken);

        // Assert — Failed(UnknownReplicationTargetException): хендлер отвечает 400 (глава 07 §2)
        (!probe.IsSuccess).Should().BeTrue();
        probe.Error.Should().BeOfType<UnknownReplicationTargetException>();
    }

    [Fact]
    public async Task EmptySnapshot_InactiveEmptyTargets()
    {
        // Arrange — соседей нет (пустой seed)
        var (prober, _) = Prober();

        // Act
        var report = (await prober.ProbeAsync(null, TestContext.Current.CancellationToken)).Throw().Value;

        // Assert — осмысленный «соседей нет»: active=false, пустой список
        report.Active.Should().BeFalse();
        report.Targets.Should().BeEmpty();
    }

    [Fact]
    public async Task ParallelPings_DurationIsMaxNotSum()
    {
        // Arrange — три соседа, каждый пинг 200 мс (через задержку фейка)
        var (prober, transport) = Prober(names: ["peer-a", "peer-b", "peer-c"]);
        transport.Delay = TimeSpan.FromMilliseconds(200);
        var sw = Stopwatch.StartNew();

        // Act
        var report = (await prober.ProbeAsync(null, TestContext.Current.CancellationToken)).Throw().Value;
        sw.Stop();

        // Assert — параллельность: ответ probe ≈ max per-target, не sum
        // (3×200 мс последовательно = 600 мс; бюджет с запасом на планировщик)
        report.Targets.Should().HaveCount(3);
        sw.ElapsedMilliseconds.Should().BeLessThan(500,
            "пинги идут параллельно: задержка ответа = max, не sum (глава 07 §2)");
    }
}
