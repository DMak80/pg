using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using EtcdWorker.Etcd;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Shared.Etcd.Client;
using Xunit;

namespace EtcdWorker.IntegrationTests.Api;

// WAF-интеграции API EtcdWorker (spec §7.2-API): PUT /api/etcd/config —
// 200/400/409/503 (RMW-гонка), POST /api/restart — 202. Окружение — свой etcd
// (EtcdApiFixture), фоновые циклы выключены.
[Collection(EtcdApiCollection.Name)]
public sealed class EtcdApiTests(EtcdApiFixture fx)
{
    private static async Task<HttpResponseMessage> PutConfigAsync(HttpClient client, int nodes)
        => await client.PutAsJsonAsync("/api/etcd/config", new { nodes },
            TestContext.Current.CancellationToken);

    // AAA: валидная декларация (external seed=1 + N≥2) — 200, ключ /etcd/config
    // записан; повторная замена сохраняет created_unix исходной декларации
    [Fact]
    public async Task PutConfig_Valid_200()
    {
        DockerTrait.SkipIfUnavailable();
        using var client = fx.Factory.CreateClient();

        // Act
        var first = await PutConfigAsync(client, 2);
        var firstCfg = await ReadConfigAsync();
        var second = await PutConfigAsync(client, 3);
        var secondCfg = await ReadConfigAsync();

        // Assert
        first.StatusCode.Should().Be(HttpStatusCode.OK);
        second.StatusCode.Should().Be(HttpStatusCode.OK);
        (await second.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken))
            .GetProperty("nodes").GetInt32().Should().Be(3);
        firstCfg.Should().NotBeNull();
        secondCfg.Should().NotBeNull();
        secondCfg!.Nodes.Should().Be(3);
        secondCfg.CreatedUnix.Should().Be(firstCfg!.CreatedUnix,
            "a repeated declaration replaces nodes only — created_unix of the first declaration is kept");
    }

    private async Task<EtcdConfigJson.EtcdConfig?> ReadConfigAsync()
    {
        var kv = await fx.Env.Gateway.GetAsync(fx.Env.Endpoint, EtcdDomainKeys.ConfigKey,
            TestContext.Current.CancellationToken);
        kv.IsSuccess.Should().BeTrue(kv.Error?.Message);
        return EtcdConfigJson.Parse(kv.Value!.Value);
    }

    // AAA: N вне 0..7 — 400 ProblemDetails (гвард валидации до etcd-обращений)
    [Theory]
    [InlineData(-1)]
    [InlineData(8)]
    public async Task PutConfig_NegativeAndEight_400(int nodes)
    {
        DockerTrait.SkipIfUnavailable();
        using var client = fx.Factory.CreateClient();

        // Act
        var response = await PutConfigAsync(client, nodes);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    // AAA: external=1 (только seed) + N=1 → итог < 3 — 409 кворум-гварда
    [Fact]
    public async Task PutConfig_BelowQuorum_409()
    {
        DockerTrait.SkipIfUnavailable();
        using var client = fx.Factory.CreateClient();

        // Act
        var response = await PutConfigAsync(client, 1);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    // AAA: etcd недоступен (мёртвый endpoint) — 503, не 500
    [Fact]
    public async Task PutConfig_EtcdDown_503()
    {
        DockerTrait.SkipIfUnavailable();
        using var dead = new EtcdWorkerApiFactory { EtcdEndpoint = "http://localhost:1" };
        using var client = dead.CreateClient();

        // Act
        var response = await PutConfigAsync(client, 3);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
    }

    // AAA: RMW-гонка — подмена ключа между чтением и txn (RacingGetGateway
    // переписывает ключ до возврата старой ревизии) — 503, ретрай клиентом
    [Fact]
    public async Task PutConfig_RmwRace_503()
    {
        DockerTrait.SkipIfUnavailable();
        var env = fx.Env;
        // Сид: ключ уже существует (ветка ModRevisionEqual)
        await env.Gateway.PutAsync(env.Endpoint, "/etcd/config",
            EtcdConfigJson.Serialize(2, 1756500000), lease: null, TestContext.Current.CancellationToken);
        using var racing = new EtcdWorkerApiFactory(services =>
        {
            services.RemoveAll<IEtcdGateway>();
            services.AddSingleton<IEtcdGateway>(_ =>
                new RacingGetGateway(new EtcdGateway(new HttpClient { Timeout = TimeSpan.FromSeconds(7.5) })));
        }) { EtcdEndpoint = env.Endpoint };
        using var client = racing.CreateClient();

        // Act
        var response = await PutConfigAsync(client, 3);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
    }

    // AAA: POST /api/restart — 202 (graceful self-stop; отдельная фабрика —
    // рестарт не мешает остальным кейсам)
    [Fact]
    public async Task Restart_202()
    {
        DockerTrait.SkipIfUnavailable();
        using var restartable = new EtcdWorkerApiFactory { EtcdEndpoint = fx.Env.Endpoint };
        using var client = restartable.CreateClient();

        // Act
        var response = await client.PostAsync("/api/restart", content: null,
            TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Accepted);
    }
}
