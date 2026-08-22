using System.Net;
using System.Net.Sockets;
using System.Text;
using AdminPanel.Core;
using AdminPanel.Probes;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.IntegrationTests;

// Patroni-проба против локального HTTP-стаба (spec §9.5): HostMap e2e, своя запись
// member'а, ошибки транспорта/отсутствия записи. Стаб — кроссплатформенный
// HttpListener, отдаёт Patroni /cluster JSON на любой GET.
public class PatroniRestProbeTests : IAsyncLifetime
{
    // Инлайн-копия фикстуры patroni-cluster.json (integration-сборка не видит файлы UnitTests).
    private const string ClusterJson = """
        {"members":[
          {"name":"s1a","host":"10.0.0.11","port":5432,"role":"master","state":"running","timeline":1,"lag":0},
          {"name":"s1b","host":"10.0.0.12","port":5432,"role":"replica","state":"streaming","timeline":2,"lag":4096},
          {"name":"s1c","host":"10.0.0.13","port":5432,"role":"replica","state":"stopped","timeline":1,"lag":null}
        ]}
        """;

    private readonly HttpListener _server = new();
    private int _port;

    public async ValueTask InitializeAsync()
    {
        // Свободный порт: захват TcpListener(0), затем HttpListener на нём.
        var probe = new TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        _port = ((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();

        _server.Prefixes.Add($"http://127.0.0.1:{_port}/");
        _server.Start();
        _ = Task.Run(ServeAsync);
    }

    public ValueTask DisposeAsync()
    {
        _server.Stop();
        return ValueTask.CompletedTask;
    }

    private async Task ServeAsync()
    {
        while (_server.IsListening)
        {
            HttpListenerContext context;
            try
            {
                context = await _server.GetContextAsync();
            }
            catch (Exception)
            {
                return; // слушатель остановлен тестом
            }

            try
            {
                var body = Encoding.UTF8.GetBytes(ClusterJson);
                context.Response.ContentType = "application/json";
                context.Response.ContentLength64 = body.Length;
                await context.Response.OutputStream.WriteAsync(body);
            }
            catch (Exception)
            {
                // клиент оборвал соединение — не роняем стаб
            }
            finally
            {
                context.Response.Close();
            }
        }
    }

    private static HaScope Scope() => new(
        "demo-s1", "demo", "s1", true, "s1a", null, true,
        [Member("s1a"), Member("s1b"), Member("zz")],
        null);

    private static HaMember Member(string name)
        => new(name, name, 5432, null, null, null, null, null, null);

    private PatroniRestProbe Probe(Dictionary<string, string>? hostMap = null) => new(
        new HttpClient { Timeout = TimeSpan.FromSeconds(3) },
        Options.Create(new ProbesOptions { HostMap = hostMap ?? [] }),
        TimeProvider.System);

    [Fact]
    public async Task Probe_MapsHostAndParsesSelfEntry()
    {
        // Arrange: s1a:8008 маппится на стаб.
        var probe = Probe(new Dictionary<string, string> { ["s1a:8008"] = $"127.0.0.1:{_port}" });

        // Act
        var result = await probe.ProbeAsync(Scope(), Member("s1a"), CancellationToken.None);

        // Assert: своя запись; latency измерена; target kind по §3.14.
        result.Enrichment.Role.Should().Be("master");
        result.Enrichment.State.Should().Be("running");
        result.Enrichment.Timeline.Should().Be(1L);
        result.Enrichment.Error.Should().BeNull();
        result.Result.Ok.Should().BeTrue();
        result.Result.Target.Should().Be("demo-s1/s1a");
        result.Result.Kind.Should().Be("patroni");
        result.Result.LatencyMs.Should().BePositive();
    }

    [Fact]
    public async Task Probe_AnotherMember_PicksOwnEntry()
    {
        // Arrange
        var probe = Probe(new Dictionary<string, string> { ["s1b:8008"] = $"127.0.0.1:{_port}" });

        // Act
        var result = await probe.ProbeAsync(Scope(), Member("s1b"), CancellationToken.None);

        // Assert: запись s1b — другие timeline/лаг, чем у s1a.
        result.Enrichment.Role.Should().Be("replica");
        result.Enrichment.Timeline.Should().Be(2L);
        result.Enrichment.LagBytes.Should().Be(4096L);
    }

    [Fact]
    public async Task Probe_MemberMissingInResponse_Error()
    {
        // Arrange: member "zz" в ответе стаба нет (spec §3.4).
        var probe = Probe(new Dictionary<string, string> { ["zz:8008"] = $"127.0.0.1:{_port}" });

        // Act
        var result = await probe.ProbeAsync(Scope(), Member("zz"), CancellationToken.None);

        // Assert
        result.Result.Ok.Should().BeFalse();
        result.Enrichment.Error.Should().Contain("не найден");
    }

    [Fact]
    public async Task Probe_DeadPort_ReturnsError()
    {
        // Arrange: HostMap ведёт на закрытый порт.
        var probe = Probe(new Dictionary<string, string> { ["s1a:8008"] = "127.0.0.1:1" });

        // Act
        var result = await probe.ProbeAsync(Scope(), Member("s1a"), CancellationToken.None);

        // Assert: ошибка целиком в результат, enrichment с Error, лагов нет (spec §3.5).
        result.Result.Ok.Should().BeFalse();
        result.Result.Error.Should().NotBeNullOrEmpty();
        result.Enrichment.Timeline.Should().BeNull();
        result.Enrichment.LagBytes.Should().BeNull();
    }

    [Fact]
    public async Task Probe_UnmappedHost_FailsWithOriginalHost()
    {
        // Arrange: хост без записи карты — идёт на исходный адрес :8008 (identity,
        // unit-покрыт HostMapResolverTests); .invalid не резолвится — отказ транспорта.
        var probe = Probe();

        // Act
        var result = await probe.ProbeAsync(Scope(), Member("s1a"), CancellationToken.None);

        // Assert: identity-ветка не падает, даёт штатный failed-результат.
        result.Result.Ok.Should().BeFalse();
        result.Result.Target.Should().Be("demo-s1/s1a");
        result.Enrichment.Error.Should().NotBeNullOrEmpty();
    }
}
