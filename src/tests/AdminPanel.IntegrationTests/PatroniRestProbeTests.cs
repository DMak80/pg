using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using AdminPanel.Core;
using AdminPanel.Etcd.Workers;
using AdminPanel.Probes;
using FluentAssertions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.IntegrationTests;

// Patroni-проба против локального Kestrel https-стаба (t22, arch/02 §6.1):
// верификация цепочки к per-install ServerCA (hostname не сверяется —
// P17-канон), серт из ЧУЖОГО CA — транспортный отказ TLS, отсутствие записи
// member — PatroniProbeException-путь. HostMap e2e, User-Agent «AdminPanel».
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

    // Два независимых CA: доверенный панели и чужой (сервер WITHOUT доверия).
    private static readonly (string CaPem, string CaKeyPem) TrustedCa = WorkersApiTests.TestPki.GenerateCa();
    private static readonly (string CaPem, string CaKeyPem) ForeignCa = WorkersApiTests.TestPki.GenerateCa();

    private WebApplication _trusted = null!;
    private WebApplication _foreign = null!;
    private int _trustedPort;
    private int _foreignPort;
    private string? _seenUserAgent;

    public async ValueTask InitializeAsync()
    {
        _trustedPort = FreePort();
        _foreignPort = FreePort();
        _trusted = await StartStabAsync(TrustedCa, _trustedPort, CaptureUserAgent);
        _foreign = await StartStabAsync(ForeignCa, _foreignPort, null);
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync(_trusted);
        await StopAsync(_foreign);
    }

    private void CaptureUserAgent(HttpContext ctx)
        => _seenUserAgent = ctx.Request.Headers.UserAgent.ToString();

    // Kestrel https-стаб: серт SAN 127.0.0.1 из переданного CA, Patroni
    // /cluster JSON на любой GET (клиент hostname не сверяет — только цепочку).
    private static async Task<WebApplication> StartStabAsync(
        (string CaPem, string CaKeyPem) ca, int port, Action<HttpContext>? onTouch)
    {
        var (certPem, keyPem) = WorkersApiTests.TestPki.Issue(ca.CaPem, ca.CaKeyPem, "127.0.0.1");
        // PFX round-trip: эфемерный ключ CreateFromPem macOS-сервером не читается.
        using var ephemeral = X509Certificate2.CreateFromPem(certPem, keyPem);
        var serverCert = System.Security.Cryptography.X509Certificates.X509CertificateLoader
            .LoadPkcs12(ephemeral.Export(X509ContentType.Pfx), null);

        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(o =>
            o.Listen(IPAddress.Loopback, port, lo => lo.UseHttps(serverCert)));
        var app = builder.Build();
        app.MapGet("/cluster", ctx =>
        {
            onTouch?.Invoke(ctx);
            ctx.Response.ContentType = "application/json";
            return ctx.Response.WriteAsync(ClusterJson);
        });
        await app.StartAsync();
        return app;
    }

    private static async Task StopAsync(WebApplication app)
    {
        await app.StopAsync();
        await app.DisposeAsync();
    }

    // Свободный порт: захват TcpListener(0), затем Kestrel на нём.
    private static int FreePort()
    {
        var probe = new TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        var port = ((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();
        return port;
    }

    private static HaScope Scope() => new(
        "demo-s1", "demo", "s1", true, "s1a", null, true, null, null, null,
        [Member("s1a"), Member("s1b"), Member("zz")],
        null);

    private static HaMember Member(string name)
        => new(name, name, 5432, null, null, null, null, null, null, null);

    // Проба с TLS-handler'ом панели: доверие TrustedCa (паттерн BuildTlsHandler
    // из ModuleExtensions — typed HttpClient "patroni").
    private PatroniRestProbe Probe(Dictionary<string, string>? hostMap = null) => new(
        new HttpClient(PatroniRestProbe.BuildTlsHandler(
            new WorkerTlsOptions { ServerCaPem = TrustedCa.CaPem }))
        {
            Timeout = TimeSpan.FromSeconds(3),
        },
        Options.Create(new ProbesOptions { HostMap = hostMap ?? [] }),
        TimeProvider.System);

    [Fact]
    public async Task Probe_HttpsTrustedCa_ParsesSelfEntryAndSendsUserAgent()
    {
        // Arrange: s1a:5432 (host:member-port) маппится на https-стаб доверенного CA.
        var probe = Probe(new Dictionary<string, string> { ["s1a:5432"] = $"127.0.0.1:{_trustedPort}" });

        // Act
        var result = await probe.ProbeAsync(Scope(), Member("s1a"), CancellationToken.None);

        // Assert: TLS прошёл цепочкой к CA; своя запись; идентификация панели
        // в access-логах Patroni (spec §3.22) получена сервером.
        result.Enrichment.Role.Should().Be("master");
        result.Enrichment.State.Should().Be("running");
        result.Enrichment.Timeline.Should().Be(1L);
        result.Enrichment.Error.Should().BeNull();
        result.Result.Ok.Should().BeTrue();
        result.Result.Target.Should().Be("demo-s1/s1a");
        result.Result.Kind.Should().Be("patroni");
        result.Result.LatencyMs.Should().BePositive();
        _seenUserAgent.Should().Be("AdminPanel");
    }

    [Fact]
    public async Task Probe_HttpsForeignCa_TlsTransportFailure()
    {
        // Arrange: стаб с сертом ЧУЖОГО CA — цепочка к per-install CA не строится.
        var probe = Probe(new Dictionary<string, string> { ["s1a:5432"] = $"127.0.0.1:{_foreignPort}" });

        // Act
        var result = await probe.ProbeAsync(Scope(), Member("s1a"), CancellationToken.None);

        // Assert: отказ транспорта TLS целиком в результат пробы (не тика).
        result.Result.Ok.Should().BeFalse();
        result.Result.Error.Should().NotBeNullOrEmpty();
        result.Enrichment.Error.Should().NotBeNullOrEmpty();
        result.Enrichment.Timeline.Should().BeNull();
    }

    [Fact]
    public async Task Probe_MemberMissingInResponse_Error()
    {
        // Arrange: member "zz" в ответе стаба нет (spec §3.4); ключ — host:member-port.
        var probe = Probe(new Dictionary<string, string> { ["zz:5432"] = $"127.0.0.1:{_trustedPort}" });

        // Act
        var result = await probe.ProbeAsync(Scope(), Member("zz"), CancellationToken.None);

        // Assert
        result.Result.Ok.Should().BeFalse();
        result.Enrichment.Error.Should().Contain("не найден");
    }

    [Fact]
    public async Task Probe_DeadPort_ReturnsError()
    {
        // Arrange: HostMap ведёт на закрытый порт (ключ — host:member-port).
        var probe = Probe(new Dictionary<string, string> { ["s1a:5432"] = "127.0.0.1:1" });

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
