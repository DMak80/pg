using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using FluentAssertions;
using Xunit;

namespace S3Worker.IntegrationTests.E2e;

// Takeover-E2E S3Worker (spec §11 сценарий 7, arch/17): kill ДЕРЖАТЕЛЯ клэйма
// посреди provisioning (P2: ноды PROVISIONING, контейнер east1 есть) —
// выживший инстанс пере-захватывает клэйм (TTL ≤ 15 с) и доигрывает
// provisioning до Active (endpoints/config финальны), без двойного
// контроллера (клэйм один — instance выжившего). Держатель резолвится по
// etcd-фактам ПЕРЕД kill (claims.instance → api-ключ → url → порт →
// сопоставление с Api1/Api2Port). Изоляция/телеметрия — docs/e2e-isolation.md,
// docs/e2e-launch.md. Гейт: PGW_TEST_DOCKER=1.
public class S3E2eTakeoverTests
{
    [Fact]
    public async Task Kill_ClaimHolderMidProvision_SurvivorFinishesToActive()
    {
        // Arrange: гейт запуска — без docker-режима E2E скипается.
        if (Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") != "1")
        {
            Assert.Skip("PGW_TEST_DOCKER=1 not set — docker E2E skipped");
            return;
        }

        // Arrange A1: двухинстансный контур (сеть + etcd + ew1/ew2 + общий TLS).
        await using var fx = await S3E2eEnvironment.StartTwoAsync("takeover");
        var cluster = $"e2e{fx.ClusterTag}";
        using var api1 = fx.CreateApiHttpClient(fx.Api1BaseUrl);
        var ct = TestContext.Current.CancellationToken;

        try
        {
            // A1: оба воркера живы — по 2 ключа api/instances (≤ 100 с).
            await fx.WaitPhaseAsync("takeover-instances-up", async () =>
                (await RangeAsync(fx, "/s3worker/api/", ct)).Count == 2
                && (await RangeAsync(fx, "/s3worker/instances/", ct)).Count == 2,
                TimeSpan.FromSeconds(100), ct);

            // A2: API ew1 готов; заявка N=2 через Api1.
            await fx.WaitPhaseAsync("takeover-wait-worker", async () =>
            {
                try
                {
                    using var health = await api1.GetAsync("/healthz", ct);
                    return health.StatusCode == HttpStatusCode.OK;
                }
                catch (HttpRequestException)
                {
                    return false;
                }
            }, TimeSpan.FromSeconds(30), ct);

            await S3E2eScaleTests.CreateClusterAsync(fx, api1, cluster, ct, "east1", "east2");

            // A3: якорь «посередине» provisioning — P2: journal add/provision
            // в полёте, east1 PROVISIONING, контейнер east1 существует; клэйм
            // ещё у жертвы (work.instance = не-выживший гарантируем на kill).
            await fx.WaitPhaseAsync("takeover-mid-provision", async () =>
                await GetOrNullAsync(fx,
                    $"/s3/clusters/{cluster}/nodes/east1/state", ct) == "PROVISIONING"
                && await fx.ContainerAliveAsync($"s3w-{cluster}-east1")
                && await GetOrNullAsync(fx, $"/s3worker/claims/{cluster}", ct) is not null,
                TimeSpan.FromSeconds(60), ct);

            // A4: резолв ДЕРЖАТЕЛЯ по etcd-фактам и kill (docker rm -f).
            var claimJson = await GetOrNullAsync(fx, $"/s3worker/claims/{cluster}", ct);
            claimJson.Should().NotBeNull("claim exists right before the kill");
            var holderInstance = JsonStringField(claimJson, "instance");
            holderInstance.Should().NotBeNullOrEmpty("claims.instance must be non-empty");

            var holderUrl = await GetOrNullAsync(fx, $"/s3worker/api/{holderInstance}", ct);
            holderUrl.Should().NotBeNull($"api key of holder {holderInstance} must exist");
            var holderPort = new Uri(JsonStringField(holderUrl, "url")!).Port;
            var matched = new[] { fx.Api1Port, fx.Api2Port }.Count(p => p == holderPort);
            matched.Should().Be(1,
                $"holder port {holderPort} must match exactly one api port ({fx.Api1Port}/{fx.Api2Port})");

            var holderName = holderPort == fx.Api1Port ? fx.Worker1Name : fx.Worker2Name;
            var survivorName = holderPort == fx.Api1Port ? fx.Worker2Name : fx.Worker1Name;
            var survivorPort = holderPort == fx.Api1Port ? fx.Api2Port : fx.Api1Port;

            await fx.RunDockerAsync(["rm", "-f", holderName], ct);

            // Идентификатор survivor'а: api-ключ с url-портом выжившего.
            string? survivorId = null;
            foreach (var kv in await RangeAsync(fx, "/s3worker/api/", ct))
            {
                var url = JsonStringField(kv.Value, "url");
                if (url is not null && new Uri(url).Port == survivorPort)
                    survivorId = kv.Key.Split('/')[^1];
            }
            survivorId.Should().NotBeNullOrEmpty(
                $"api key of the survivor (port {survivorPort}) must exist");

            // A5: takeover — мертвец гаснет с lease (≤ 15 с + запас), выживший
            // пере-захватывает клэйм и доигрывает provisioning до Active
            // (суммарный бюджет ≤ 40 с): ноды RUNNING, endpoints 2 адреса,
            // config канонический без state.
            await fx.WaitPhaseAsync("takeover-finish", async () =>
                (await RangeAsync(fx, "/s3worker/api/", ct)).Count == 1
                && await GetOrNullAsync(fx, $"/s3/clusters/{cluster}/nodes/east1/state", ct) == "RUNNING"
                && await GetOrNullAsync(fx, $"/s3/clusters/{cluster}/nodes/east2/state", ct) == "RUNNING"
                && S3E2eScaleTests.EndpointsOf(
                        await GetOrNullAsync(fx, $"/s3/clusters/{cluster}/endpoints", ct)).Length == 2
                && await JsonConfigWithoutStateAsync(fx, cluster, ct),
                TimeSpan.FromSeconds(40), ct);

            // I: двойной контроллер невозможен — клэйм один, instance = survivor.
            var claimAfter = await GetOrNullAsync(fx, $"/s3worker/claims/{cluster}", ct);
            claimAfter.Should().NotBeNull("claim exists after takeover");
            JsonStringField(claimAfter, "instance").Should().Be(survivorId,
                "claim re-acquired by the survivor (double controller impossible)");

            // Финал: DELETE кластера → survivor доигрывает демонтаж (канон
            // самоочистки: ноды/тома снимает воркер, teardown ассертит чистоту).
            var survivorApiPort = survivorPort;
            using var survivorApi = fx.CreateApiHttpClient($"https://localhost:{survivorApiPort}");
            await fx.WaitPhaseAsync("takeover-survivor-api", async () =>
            {
                try
                {
                    using var health = await survivorApi.GetAsync("/healthz", ct);
                    return health.StatusCode == HttpStatusCode.OK;
                }
                catch (HttpRequestException)
                {
                    return false;
                }
            }, TimeSpan.FromSeconds(30), ct);
            using var deleted = await survivorApi.DeleteAsync($"/api/s3/clusters/{cluster}", ct);
            deleted.StatusCode.Should().Be(HttpStatusCode.Accepted);
            await fx.WaitPhaseAsync("takeover-wait-clean", async () =>
                (await fx.Gateway.RangeAsync(fx.EtcdEndpoint,
                    $"/s3/clusters/{cluster}/", ct)).Value.Count == 0
                && !await fx.ContainerAliveAsync($"s3w-{cluster}-east1")
                && !await fx.ContainerAliveAsync($"s3w-{cluster}-east2")
                && !await fx.VolumeExistsAsync($"s3w-{cluster}-east1-data")
                && !await fx.VolumeExistsAsync($"s3w-{cluster}-east2-data"),
                TimeSpan.FromSeconds(60), ct);

            Console.WriteLine($"[PHASE] takeover-done: holder={holderName} killed, survivor={survivorName} finished provisioning and deprovisioning");
        }
        catch
        {
            // docs/e2e-launch.md §3: упавший сценарий — teardown остановит
            // контейнеры, но не удалит (разбор по артефактам).
            fx.MarkFailed();
            throw;
        }
    }

    // ===== Локальные хелперы сценария =====

    private static async Task<string?> GetOrNullAsync(
        S3E2eEnvironment fx, string key, CancellationToken ct)
        => (await fx.Gateway.GetAsync(fx.EtcdEndpoint, key, ct)).Value?.Value;

    private static async Task<IReadOnlyList<Shared.Etcd.Client.Kv>> RangeAsync(
        S3E2eEnvironment fx, string prefix, CancellationToken ct)
        => (await fx.Gateway.RangeAsync(fx.EtcdEndpoint, prefix, ct)).Value;

    private static string? JsonStringField(string? json, string field)
    {
        if (json is null)
            return null;
        try
        {
            return JsonDocument.Parse(json).RootElement.GetProperty(field).GetString();
        }
        catch (KeyNotFoundException)
        {
            return null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    // config существует и state снят (Active, канон arch/23 §2.1).
    private static async Task<bool> JsonConfigWithoutStateAsync(
        S3E2eEnvironment fx, string cluster, CancellationToken ct)
    {
        var config = await GetOrNullAsync(fx, $"/s3/clusters/{cluster}/config", ct);
        return config is not null && !config.Contains("\"state\"");
    }
}
