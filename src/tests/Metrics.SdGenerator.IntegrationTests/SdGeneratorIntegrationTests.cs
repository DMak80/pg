using System.Diagnostics.Metrics;
using Metrics.SdGenerator;
using Microsoft.Extensions.Logging.Abstractions;
using Shared.Etcd.Client;

namespace Metrics.SdGenerator.IntegrationTests;

// Цикл генератора на ЖИВОМ etcd (spec §6.2): put → таргет в файле, del → исчез,
// недоступный etcd — файл/last_success не тронуты, восстановление — догоняет,
// hosted-сервис освежает файл по интервалу.
public class SdGeneratorIntegrationTests
{
    private const string PortallocValue =
        """{"shard1/shard1a":{"host":"h1","pg":1,"patroni":8008,"doorman":0}}""";
    private const string PortallocKey = "/pgworker/portalloc/c1";

    // Harness цикла: реальный EtcdGateway, temp-файл, свой Meter (ctor-канон), NullLogger.
    private sealed record LoopHarness(
        SdGeneratorLoop Loop, SdGeneratorMetrics Metrics, string Path, Meter Meter) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            Meter.Dispose();
        }
    }

    private static LoopHarness NewLoop(string endpoint)
    {
        var dir = Path.Combine(Path.GetTempPath(), "sd-it-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var path = Path.Combine(dir, "sd.json");
        var meter = new Meter("sd-it");
        var metrics = new SdGeneratorMetrics(meter, TimeProvider.System);
        var loop = new SdGeneratorLoop(
            new EtcdGateway(new HttpClient()), [endpoint], new SdFileWriter(path), metrics,
            NullLogger<SdGeneratorLoop>.Instance);
        return new LoopHarness(loop, metrics, path, meter);
    }

    private static async Task<bool> LoopAsync(LoopHarness harness, CancellationToken ct)
        => await harness.Loop.TickAsync(ct);

    // Ожидание поллом 100 мс (≤500 мс — канон AGENTS.md) с общим бюджетом.
    private static async Task WaitForAsync(Func<bool> condition, TimeSpan budget, string what)
    {
        var deadline = DateTime.UtcNow + budget;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
                return;
            await Task.Delay(100);
        }
        condition().Should().BeTrue(what);
    }

    [Fact]
    public async Task Put_Portalloc_TargetAppears()
    {
        // Arrange: живой etcd, пустой префикс
        var ct = TestContext.Current.CancellationToken;
        await using var fx = new EtcdFixture();
        await fx.StartAsync(ct);
        await using var harness = NewLoop(fx.Endpoint);

        // Act: put portalloc → тик
        (await fx.Gateway.PutAsync(fx.Endpoint, PortallocKey, PortallocValue, null, ct)).IsSuccess.Should().BeTrue();
        var ok = await LoopAsync(harness, ct);

        // Assert: таргет и все три лейбла в файле, метрика живая
        ok.Should().BeTrue();
        var file = await File.ReadAllTextAsync(harness.Path, ct);
        file.Should().Contain("h1:8008");
        file.Should().Contain("\"cluster\":\"c1\"");
        file.Should().Contain("\"shard\":\"shard1\"");
        file.Should().Contain("\"node\":\"shard1a\"");
        harness.Metrics.LastSuccessUnix.Should().NotBeNull();
    }

    [Fact]
    public async Task Delete_TargetDisappears()
    {
        // Arrange: таргет в файле
        var ct = TestContext.Current.CancellationToken;
        await using var fx = new EtcdFixture();
        await fx.StartAsync(ct);
        await using var harness = NewLoop(fx.Endpoint);
        (await fx.Gateway.PutAsync(fx.Endpoint, PortallocKey, PortallocValue, null, ct)).IsSuccess.Should().BeTrue();
        (await LoopAsync(harness, ct)).Should().BeTrue();

        // Act: delete ключа → тик
        (await fx.Gateway.DeleteAsync(fx.Endpoint, PortallocKey, prefix: false, ct)).IsSuccess.Should().BeTrue();
        (await LoopAsync(harness, ct)).Should().BeTrue();

        // Assert: файл = []
        (await File.ReadAllTextAsync(harness.Path, ct)).Should().Be("[]");
    }

    [Fact]
    public async Task EtcdDown_FileAndLastSuccessUntouched_ThenCatchUp()
    {
        // Arrange: fixture НЕ стартована (порт из зонда) — тик идёт на мёртвый порт;
        // файл pre-written «X», успехов ещё не было
        var ct = TestContext.Current.CancellationToken;
        var port = EtcdFixture.ReserveHostPort();
        await using var fx = new EtcdFixture(port);
        await using var harness = NewLoop(fx.Endpoint);
        await File.WriteAllTextAsync(harness.Path, "X", ct);
        var contentBefore = await File.ReadAllBytesAsync(harness.Path, ct);

        // Act 1: тик на мёртвый порт
        var dead = await LoopAsync(harness, ct);

        // Assert 1: тик false, файл байт-в-байт прежний, метрика не тронута
        dead.Should().BeFalse();
        (await File.ReadAllBytesAsync(harness.Path, ct)).Should().Equal(contentBefore);
        harness.Metrics.LastSuccessUnix.Should().BeNull();

        // Act 2: etcd ожил на ТОМ ЖЕ порту → put → тик
        await fx.StartAsync(ct);
        (await fx.Gateway.PutAsync(fx.Endpoint, PortallocKey, PortallocValue, null, ct)).IsSuccess.Should().BeTrue();
        var alive = await LoopAsync(harness, ct);

        // Assert 2: догоняет — файл обновлён
        alive.Should().BeTrue();
        (await File.ReadAllTextAsync(harness.Path, ct)).Should().Contain("h1:8008");
        harness.Metrics.LastSuccessUnix.Should().NotBeNull();
    }

    [Fact]
    public async Task EmptyPrefix_EmptyArray_IsSuccess()
    {
        // Arrange: живой etcd без ключей portalloc
        var ct = TestContext.Current.CancellationToken;
        await using var fx = new EtcdFixture();
        await fx.StartAsync(ct);
        await using var harness = NewLoop(fx.Endpoint);

        // Act
        var ok = await LoopAsync(harness, ct);

        // Assert: пустой префикс — валидный [] и успех
        ok.Should().BeTrue();
        (await File.ReadAllTextAsync(harness.Path, ct)).Should().Be("[]");
        harness.Metrics.LastSuccessUnix.Should().NotBeNull();
    }

    [Fact]
    public async Task HostedService_RefreshesWithinInterval()
    {
        // Arrange: hosted-сервис с интервалом 1 с (без ручных тиков)
        var ct = TestContext.Current.CancellationToken;
        await using var fx = new EtcdFixture();
        await fx.StartAsync(ct);
        await using var harness = NewLoop(fx.Endpoint);
        var options = new SdGeneratorOptions
        {
            RefreshIntervalSec = 1,
            OutputPath = harness.Path,
            Etcd = { Endpoints = [fx.Endpoint] },
        };
        var hosted = new SdGeneratorHostedService(
            options, harness.Loop, NullLogger<SdGeneratorHostedService>.Instance);
        try
        {
            await hosted.StartAsync(ct);

            // Act: put portalloc
            (await fx.Gateway.PutAsync(fx.Endpoint, PortallocKey, PortallocValue, null, ct)).IsSuccess.Should().BeTrue();

            // Assert: файл появился без ручного тика (≤10 с)
            await WaitForAsync(
                () => File.Exists(harness.Path)
                    && File.ReadAllText(harness.Path).Contains("h1:8008"),
                TimeSpan.FromSeconds(10), "hosted-сервис не освежил файл за 10 c");
        }
        finally
        {
            await hosted.StopAsync(CancellationToken.None);
        }
    }
}
