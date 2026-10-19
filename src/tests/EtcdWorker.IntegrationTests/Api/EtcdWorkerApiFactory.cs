using EtcdWorker.App;
using EtcdWorker.IntegrationTests.Docker;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Shared.Etcd.Client;
using Xunit;

namespace EtcdWorker.IntegrationTests.Api;

// WAF-хост EtcdWorker с настоящим etcd (окружение фикстуры) и выключенными
// фоновыми циклами (порт S3ApiFactory): loops не нужны для API-мутаций.
// AllowInsecureHttp — конфиг in-memory (WAF применяется при Build); mTLS —
// MtlsApiTests на реальном сокете.
public class EtcdWorkerApiFactory(Action<IServiceCollection>? decorate = null) : WebApplicationFactory<Program>
{
    // Endpoint фикстуры известен после старта etcd — WAF строит хост лениво
    // (первый CreateClient), поле читается в ConfigureWebHost к этому моменту.
    public string EtcdEndpoint { get; set; } = "http://localhost:1";

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Development");
        builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["EtcdWorker:Etcd:Endpoints:0"] = EtcdEndpoint,
            ["EtcdWorker:Docker:Hosts:0:Name"] = "local",
            ["EtcdWorker:Docker:Hosts:0:Endpoint"] = "unix:///var/run/does-not-exist.sock",
            ["EtcdWorker:Docker:Network"] = "etcdw-net",
            ["EtcdWorker:Api:AdvertiseUrl"] = "https://localhost:9997",
            // mTLS — MtlsApiTests на реальном сокете; WAF-фабрика — insecure HTTP.
            ["EtcdWorker:Api:Tls:AllowInsecureHttp"] = "true",
        }));
        builder.ConfigureServices(services =>
        {
            services.RemoveAll<IHostedService>(); // Keepalive не стартует
            decorate?.Invoke(services);
        });
    }
}

// Collection-fixture API-тестов: один etcd + одна WAF-фабрика на Api-класс.
public sealed class EtcdApiFixture : IAsyncLifetime
{
    public EtcdWorkerEnvironment Env { get; } = new();

    public EtcdWorkerApiFactory Factory { get; } = new();

    public async ValueTask InitializeAsync()
    {
        await Env.InitializeAsync();
        Factory.EtcdEndpoint = Env.Endpoint;
    }

    public async ValueTask DisposeAsync()
    {
        await Factory.DisposeAsync();
        await Env.DisposeAsync();
    }
}

[CollectionDefinition(Name)]
public sealed class EtcdApiCollection : ICollectionFixture<EtcdApiFixture>
{
    public const string Name = "etcdw-api";
}

// Гарантированный env WAF-хостов сборки (порт S3ApiFactory): Program.Main
// читает конфиг TLS раньше слияния WAF-конфига — AllowInsecureHttp обязан
// жить в env процесса.
internal static class TestEnv
{
    [System.Runtime.CompilerServices.ModuleInitializer]
    internal static void Set()
        => Environment.SetEnvironmentVariable("EtcdWorker__Api__Tls__AllowInsecureHttp", "true");
}

// Декоратор RMW-гонки (кейс PutConfig_RmwRace_503): первый Get /etcd/config
// возвращает СТАРУЮ ревизию, но перед этим тихо переписывает ключ (новая
// mod_revision) — txn ModRevisionEqual(старая) гарантированно проигрывает.
internal sealed class RacingGetGateway(IEtcdGateway inner) : IEtcdGateway
{
    private bool _fired;

    public async Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
    {
        var got = await inner.GetAsync(endpoint, key, ct);
        if (!_fired && got.IsSuccess && got.Value is { } kv && key == "/etcd/config")
        {
            _fired = true;
            // Подмена между чтением и txn: та же value — новая mod_revision.
            await inner.PutAsync(endpoint, key, kv.Value, lease: null, ct);
        }

        return got;
    }

    public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
        => inner.RangeAsync(endpoint, prefix, ct);

    public Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
        => inner.PutAsync(endpoint, key, value, lease, ct);

    public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
        => inner.DeleteAsync(endpoint, keyOrPrefix, prefix, ct);

    public Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct)
        => inner.TxnAsync(endpoint, req, ct);

    public Task<Result<long>> LeaseGrantAsync(string endpoint, int ttlSec, CancellationToken ct)
        => inner.LeaseGrantAsync(endpoint, ttlSec, ct);

    public Task<Result> LeaseRevokeAsync(string endpoint, long lease, CancellationToken ct)
        => inner.LeaseRevokeAsync(endpoint, lease, ct);

    public Task<Result> LeaseKeepaliveAsync(string endpoint, long lease, CancellationToken ct)
        => inner.LeaseKeepaliveAsync(endpoint, lease, ct);

    public Task<Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct)
        => inner.SnapshotSaveAsync(endpoint, ct);

    public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
        => inner.StatusAsync(endpoint, ct);

    public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
        => inner.MemberListAsync(endpoint, ct);

    public Task<Result<EtcdMember>> MemberAddAsync(string endpoint, IReadOnlyList<string> peerUrls, bool isLearner, CancellationToken ct)
        => inner.MemberAddAsync(endpoint, peerUrls, isLearner, ct);

    public Task<Result> MemberRemoveAsync(string endpoint, ulong memberId, CancellationToken ct)
        => inner.MemberRemoveAsync(endpoint, memberId, ct);

    public Task<Result> MemberPromoteAsync(string endpoint, ulong memberId, CancellationToken ct)
        => inner.MemberPromoteAsync(endpoint, memberId, ct);

    public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
        => inner.AlarmAsync(endpoint, ct);

    public Task<Result> CompactAsync(string endpoint, long revision, CancellationToken ct)
        => inner.CompactAsync(endpoint, revision, ct);

    public Task<Result> DefragmentAsync(string endpoint, CancellationToken ct)
        => inner.DefragmentAsync(endpoint, ct);
}
