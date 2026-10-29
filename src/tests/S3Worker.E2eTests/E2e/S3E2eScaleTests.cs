using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using FluentAssertions;
using Xunit;

namespace S3Worker.IntegrationTests.E2e;

// Масштабные мутации E2E S3Worker (spec §11 сценарии 4–5): E (remove-node:
// снос контейнера+volume, del ключей ноды, portalloc без ноды, RMW endpoints,
// config.nodes актуализирован; гварды 409 последняя нода / 404 чужая) и
// B (deprovision X0–X3: пустой домен + пустая координация + нет контейнеров/
// томов; 404 повтор). Изоляция/телеметрия — docs/e2e-isolation.md,
// docs/e2e-launch.md.
public class S3E2eScaleTests
{
    // Сценарий 4 RemoveNode (spec §5.2, arch/23 §2.2): Active N=2 → DELETE
    // nodes/east2 → 202 → снос контейнера+volume east2, del ключей ноды,
    // portalloc без east2, endpoints = 1 адрес, config.nodes = 1. Докомплект
    // гвардов: delete последней ноды → 409; повтор delete east2 → 404.
    [Fact]
    public async Task RemoveNode_ShrinksClusterWithGuards()
    {
        await using var fx = await S3E2eEnvironment.StartAsync("remnode");
        var cluster = $"e2e{fx.ClusterTag}";
        using var api = fx.CreateApiHttpClient();
        var ct = TestContext.Current.CancellationToken;

        try
        {
            await WaitWorkerAsync(fx, api, ct);
            await CreateClusterAsync(fx, api, cluster, ct, "east1", "east2");
            await WaitClusterActiveAsync(fx, cluster, ct, "east1", "east2");

            // Act: заявка вывода east2 → 202 (TO_REMOVE поставлен синхронно).
            using var removed = await api.DeleteAsync(
                $"/api/s3/clusters/{cluster}/nodes/east2", ct);
            var removedBody = await removed.Content.ReadAsStringAsync(ct);
            removed.StatusCode.Should().Be(HttpStatusCode.Accepted, removedBody);

            // [PHASE] wait-remove: E-процесс довёл демонтаж до конца.
            await fx.WaitPhaseAsync("wait-remove", async () =>
            {
                if (await fx.ContainerAliveAsync($"s3w-{cluster}-east2")
                    || await fx.VolumeExistsAsync($"s3w-{cluster}-east2-data"))
                    return false;
                if (await GetOrNullAsync(fx,
                        $"/s3/clusters/{cluster}/nodes/east2/state", ct) is not null)
                    return false;
                var alloc = await GetOrNullAsync(fx, $"/s3worker/portalloc/{cluster}", ct);
                if (alloc is null || Json(alloc).TryGetProperty("east2", out _))
                    return false;
                var current = await GetOrNullAsync(fx, $"/s3/clusters/{cluster}/endpoints", ct);
                if (EndpointsOf(current).Length != 1)
                    return false;
                return JsonNodeCount(await GetOrNullAsync(fx,
                           $"/s3/clusters/{cluster}/config", ct)) == 1;
            }, TimeSpan.FromSeconds(60), ct);

            // Assert 1: endpoints = 1 адрес — прежний east1 (advertised).
            var endpoints = await GetOrNullAsync(fx, $"/s3/clusters/{cluster}/endpoints", ct);
            endpoints.Should().NotBeNull("endpoints up to date after the E-final");
            endpoints!.Split(',')[0].Should().StartWith("host.docker.internal:");

            // Assert 2: гварды — delete последней ноды → 409 (arch/23 §2: 1..9).
            using var lastNode = await api.DeleteAsync(
                $"/api/s3/clusters/{cluster}/nodes/east1", ct);
            lastNode.StatusCode.Should().Be(HttpStatusCode.Conflict,
                "last node of the cluster cannot be removed");

            // Assert 3: докомплект — повтор delete уже снесённой east2 → 404.
            using var again = await api.DeleteAsync(
                $"/api/s3/clusters/{cluster}/nodes/east2", ct);
            again.StatusCode.Should().Be(HttpStatusCode.NotFound, "node east2 no longer exists");

            // Финал: DELETE кластера → демонтаж (канон самоочистки).
            await DeleteClusterAndWaitCleanAsync(fx, api, cluster, ct, "east1");
        }
        catch
        {
            // docs/e2e-launch.md §3: упавший сценарий — teardown остановит
            // контейнеры, но не удалит (разбор по артефактам).
            fx.MarkFailed();
            throw;
        }
    }

    // Сценарий 5 Deprovision (spec §5.3, arch/24 §5 B X0–X3): Active N=2 →
    // DELETE кластера → 202 → etcd-префикс кластера пуст, координация
    // (work/rotations/portalloc/claims) снята, контейнеров/томов нет;
    // повтор DELETE → 404.
    [Fact]
    public async Task Deprovision_TearsDownToTerminalClean()
    {
        await using var fx = await S3E2eEnvironment.StartAsync("deprovision");
        var cluster = $"e2e{fx.ClusterTag}";
        using var api = fx.CreateApiHttpClient();
        var ct = TestContext.Current.CancellationToken;

        try
        {
            await WaitWorkerAsync(fx, api, ct);
            await CreateClusterAsync(fx, api, cluster, ct, "east1", "east2");
            await WaitClusterActiveAsync(fx, cluster, ct, "east1", "east2");

            // Act: заявка демонтажа → 202 (config.state=TO_REMOVE синхронно).
            using var deleted = await api.DeleteAsync($"/api/s3/clusters/{cluster}", ct);
            var deletedBody = await deleted.Content.ReadAsStringAsync(ct);
            deleted.StatusCode.Should().Be(HttpStatusCode.Accepted, deletedBody);

            // [PHASE] wait-deprovision: X0–X3 — терминальная чистота
            // (journal-запись done У демонтажа ЕСТЬ — arch/24 §5 B фаза X3,
            // в отличие от valkey; терминал = done + чистота).
            await fx.WaitPhaseAsync("wait-deprovision", async () =>
            {
                var work = await GetOrNullAsync(fx, $"/s3worker/work/{cluster}", ct);
                if (work is not null && !work.Contains("\"phase\":\"done\""))
                    return false;
                return (await fx.Gateway.RangeAsync(fx.EtcdEndpoint,
                            $"/s3/clusters/{cluster}/", ct)).Value.Count == 0
                    && await GetOrNullAsync(fx, $"/s3worker/rotations/{cluster}", ct) is null
                    && await GetOrNullAsync(fx, $"/s3worker/portalloc/{cluster}", ct) is null
                    && await GetOrNullAsync(fx, $"/s3worker/claims/{cluster}", ct) is null
                    && !await fx.ContainerAliveAsync($"s3w-{cluster}-east1")
                    && !await fx.ContainerAliveAsync($"s3w-{cluster}-east2")
                    && !await fx.VolumeExistsAsync($"s3w-{cluster}-east1-data")
                    && !await fx.VolumeExistsAsync($"s3w-{cluster}-east2-data");
            }, TimeSpan.FromSeconds(60), ct);

            // Assert: повтор DELETE → 404 (кластера нет).
            using var again = await api.DeleteAsync($"/api/s3/clusters/{cluster}", ct);
            again.StatusCode.Should().Be(HttpStatusCode.NotFound, "cluster torn down");

            // Ассерт чистоты фикстуры покрывает docker-остатки (DisposeAsync).
        }
        catch
        {
            fx.MarkFailed();
            throw;
        }
    }

    // ===== Общие хелперы сценариев =====

    internal static async Task WaitWorkerAsync(S3E2eEnvironment fx, HttpClient api, CancellationToken ct)
        => await fx.WaitPhaseAsync("wait-worker", async () =>
        {
            try
            {
                using var health = await api.GetAsync("/healthz", ct);
                return health.StatusCode == HttpStatusCode.OK;
            }
            catch (HttpRequestException)
            {
                return false;
            }
        }, TimeSpan.FromSeconds(30), ct);

    // create c instances[] (arch/24 §1.1): имена инстансов — из списка
    // (произвольные, паттерн кластера; дефолт east1/east2 — 2 инстанса).
    internal static async Task CreateClusterAsync(
        S3E2eEnvironment fx, HttpClient api, string cluster, CancellationToken ct,
        params string[] instances)
    {
        if (instances.Length == 0)
            instances = ["east1", "east2"];
        using var created = await api.PostAsJsonAsync("/api/s3/clusters",
            new
            {
                name = cluster,
                platform = "owns3",
                instances = instances
                    .Select(name => (object)new
                    {
                        name,
                        placement = new { dockerHost = "local" },
                    }).ToArray(),
                resources = new { cpu = 0.5m, memGi = 1, diskGi = 5 },
            }, ct);
        var body = await created.Content.ReadAsStringAsync(ct);
        created.StatusCode.Should().Be(HttpStatusCode.Created, body);
    }

    // [PHASE] wait-active: ноды RUNNING + endpoints + config без state.
    internal static async Task WaitClusterActiveAsync(
        S3E2eEnvironment fx, string cluster, CancellationToken ct, params string[] nodes)
        => await fx.WaitPhaseAsync($"active-{string.Join('-', nodes)}", async () =>
        {
            foreach (var node in nodes)
            {
                var state = await GetOrNullAsync(fx,
                    $"/s3/clusters/{cluster}/nodes/{node}/state", ct);
                if (state != "RUNNING" || !await fx.ContainerAliveAsync($"s3w-{cluster}-{node}"))
                    return false;
            }

            var endpoints = await GetOrNullAsync(fx, $"/s3/clusters/{cluster}/endpoints", ct);
            if (string.IsNullOrWhiteSpace(endpoints))
                return false;
            var config = await GetOrNullAsync(fx, $"/s3/clusters/{cluster}/config", ct);
            return config is not null && !config.Contains("\"state\"");
        }, TimeSpan.FromSeconds(115), ct);

    // DELETE кластера → 202 → wait-clean (демонтаж воркером, канон самоочистки).
    internal static async Task DeleteClusterAndWaitCleanAsync(
        S3E2eEnvironment fx, HttpClient api, string cluster,
        CancellationToken ct, params string[] nodes)
    {
        using var deleted = await api.DeleteAsync($"/api/s3/clusters/{cluster}", ct);
        deleted.StatusCode.Should().Be(HttpStatusCode.Accepted);
        await fx.WaitPhaseAsync("wait-clean", async () =>
        {
            foreach (var node in nodes)
            {
                if (await fx.ContainerAliveAsync($"s3w-{cluster}-{node}")
                    || await fx.VolumeExistsAsync($"s3w-{cluster}-{node}-data"))
                    return false;
            }

            var domain = await fx.Gateway.RangeAsync(fx.EtcdEndpoint,
                $"/s3/clusters/{cluster}/", ct);
            var alloc = await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                $"/s3worker/portalloc/{cluster}", ct);
            return domain.Value.Count == 0 && alloc.Value is null;
        }, TimeSpan.FromSeconds(60), ct);
    }

    internal static async Task<string?> GetOrNullAsync(
        S3E2eEnvironment fx, string key, CancellationToken ct)
        => (await fx.Gateway.GetAsync(fx.EtcdEndpoint, key, ct)).Value?.Value;

    internal static string[] EndpointsOf(string? endpoints)
        => (endpoints ?? string.Empty).Split(',', StringSplitOptions.RemoveEmptyEntries);

    internal static JsonElement Json(string? json)
    {
        using var doc = JsonDocument.Parse(json ?? "{}");
        return doc.RootElement.Clone();
    }

    internal static int? JsonNodeCount(string? configJson)
    {
        if (configJson is null)
            return null;
        try
        {
            using var doc = JsonDocument.Parse(configJson);
            return doc.RootElement.TryGetProperty("nodes", out var nodes) ? nodes.GetInt32() : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
