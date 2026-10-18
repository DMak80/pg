using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Amazon;
using Amazon.S3;
using FluentAssertions;
using S3Worker.IntegrationTests.S3;
using Xunit;

namespace S3Worker.IntegrationTests.E2e;

// Docker-E2E S3Worker (spec §11 сценарии 1–3, §12 фаза 11): свежий Release
// s3worker:e2e-{run} → API-create → контейнеры s3w-<C>-node{k} + тома данных
// → дискавери (config без state, endpoints host:port, placed_on) → S3-проба
// root-кредом (bucket per-install) → идемпотентность тиков → add-node (RMW
// endpoints, config.nodes актуализируется). Изоляция по docs/e2e-isolation.md
// (guid во всех именах, полный teardown, ассерт чистоты), телеметрия по
// docs/e2e-launch.md ([PHASE]-фазы, артефакты до удаления). Гейт: PGW_TEST_DOCKER=1.
public class S3E2eLifecycleTests
{
    // Сценарий 1 Provisioning (spec §5.1, §2.1): create nodes=2 → 201 →
    // P0–P5 воркером до Active: config канонический без state, обе ноды
    // RUNNING на docker-хосте local, endpoints — advertised host:port пары,
    // контейнеры/тома нод существуют, bucket per-install на каждой ноде,
    // /healthz нод отвечает.
    [Fact]
    public async Task Provisioning_TwoNodesReachActiveWithBuckets()
    {
        // Arrange: гейт запуска — без docker-режима E2E скипается.
        if (Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") != "1")
        {
            Assert.Skip("PGW_TEST_DOCKER=1 not set — docker E2E skipped");
            return;
        }

        await using var fx = await S3E2eEnvironment.StartAsync("provision");
        var cluster = $"e2e{fx.ClusterTag}";
        using var api = fx.CreateApiHttpClient();
        var ct = TestContext.Current.CancellationToken;

        try
        {
            // [PHASE] wait-worker: /healthz по mTLS готов (бюджет ≤ 30 с).
            await WaitWorkerAsync(fx, api, ct);

            // Act: создание кластера через API → 201 (заявка, arch/23 §2).
            using var created = await api.PostAsJsonAsync("/api/s3/clusters",
                new
                {
                    name = cluster,
                    platform = "owns3",
                    secretProfile = S3E2eEnvironment.ProfileName,
                    instances = new object[]
                    {
                        new { name = "east1", placement = new { dockerHost = "local" } },
                        new { name = "east2", placement = new { dockerHost = "local" } },
                    },
                    resources = new { cpu = 0.5m, memGi = 1, diskGi = 5 },
                }, ct);
            var createdBody = await created.Content.ReadAsStringAsync(ct);
            created.StatusCode.Should().Be(HttpStatusCode.Created, createdBody);

            // [PHASE] wait-provision: обе ноды RUNNING + контейнеры живы (≤ 100 c + тики).
            await WaitClusterActiveAsync(fx, cluster, ct, "east1", "east2");

            // Assert 1: config — канонический JSON Active (state снят, nodes=2).
            var config = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                $"/s3/clusters/{cluster}/config", ct)).Value!.Value;
            config.Should().NotContain("\"state\"", "Active — state removed by the worker");
            using (var doc = JsonDocument.Parse(config))
            {
                doc.RootElement.GetProperty("platform").GetString().Should().Be("owns3");
                doc.RootElement.GetProperty("nodes").GetInt32().Should().Be(2);
                doc.RootElement.GetProperty("secret_profile").GetString()
                    .Should().Be(S3E2eEnvironment.ProfileName);
            }

            // Assert 2: placed_on = local (placement-селектор резолвнут).
            foreach (var node in new[] { "east1", "east2" })
            {
                (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                    $"/s3/clusters/{cluster}/nodes/{node}/placed_on", ct)).Value!.Value
                    .Should().Be("local", $"{node} placed on the docker_host=local selector");
            }

            // Assert 3: endpoints — advertised-пары host.docker.internal:P для обеих нод
            // (формат arch/23 §2.1; литералов портов нет — из portalloc).
            var endpoints = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                $"/s3/clusters/{cluster}/endpoints", ct)).Value!.Value;
            var addresses = endpoints.Split(',');
            addresses.Should().HaveCount(2);
            addresses.Should().OnlyContain(a => a.StartsWith("host.docker.internal:"));
            var alloc = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                $"/s3worker/portalloc/{cluster}", ct)).Value!.Value;
            using (var allocDoc = JsonDocument.Parse(alloc))
            {
                foreach (var address in addresses)
                {
                    var port = int.Parse(address.Split(':')[1]);
                    allocDoc.RootElement.EnumerateObject()
                        .Any(n => n.Value.GetProperty("s3").GetInt32() == port)
                        .Should().BeTrue($"port {address} comes from portalloc");
                }
            }

            // Assert 4: контейнеры + тома данных нод существуют.
            foreach (var node in new[] { "east1", "east2" })
            {
                (await fx.ContainerAliveAsync($"s3w-{cluster}-{node}"))
                    .Should().BeTrue($"container s3w-{cluster}-{node} brought up by the worker");
                (await fx.VolumeExistsAsync($"s3w-{cluster}-{node}-data"))
                    .Should().BeTrue($"data volume s3w-{cluster}-{node}-data created by the worker");
            }

            // Assert 5: bucket per-install на каждой ноде (S3-проба root-кредом
            // по host-порту ноды из portalloc) + /healthz ноды 200.
            foreach (var address in addresses)
            {
                var port = int.Parse(address.Split(':')[1]);
                (await ListBucketsWithRootAsync(port, ct)).Should()
                    .Contain(S3E2eEnvironment.BucketName,
                        $"bucket {S3E2eEnvironment.BucketName} created by the orchestrator on :{port}");
                using var health = await new HttpClient().GetAsync(
                    $"http://{address}/healthz", ct);
                health.StatusCode.Should().Be(HttpStatusCode.OK, $"/healthz of node {address}");
            }

            // Act 6: DELETE → 202 → демонтаж воркером (канон самоочистки:
            // ноды/тома снимает воркер, teardown фикстуры ассертит чистоту).
            await DeleteClusterAndWaitCleanAsync(fx, api, cluster, ct, "east1", "east2");
        }
        catch
        {
            // docs/e2e-launch.md §3: упавший сценарий — teardown остановит
            // контейнеры, но не удалит (разбор по артефактам).
            fx.MarkFailed();
            throw;
        }
    }

    // Сценарий 2 Идемпотентность (spec §5.1): после Active повторные тики НЕ
    // меняют portalloc/endpoints/config (после снятия state новых записей нет;
    // допускается только usage/надзор) и НЕ пересоздают контейнеры.
    [Fact]
    public async Task Idempotency_TicksDoNotRewriteActiveFacts()
    {
        // Arrange: гейт запуска — без docker-режима E2E скипается.
        if (Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") != "1")
        {
            Assert.Skip("PGW_TEST_DOCKER=1 not set — docker E2E skipped");
            return;
        }

        await using var fx = await S3E2eEnvironment.StartAsync("idempotent");
        var cluster = $"e2e{fx.ClusterTag}";
        using var api = fx.CreateApiHttpClient();
        var ct = TestContext.Current.CancellationToken;

        try
        {
            await WaitWorkerAsync(fx, api, ct);

            // Arrange: кластер N=1 доведён до Active (канон сценария 1).
            using var created = await api.PostAsJsonAsync("/api/s3/clusters",
                new
                {
                    name = cluster,
                    platform = "owns3",
                    secretProfile = S3E2eEnvironment.ProfileName,
                    instances = new object[] { new { name = "east1", placement = new { dockerHost = "local" } } },
                    resources = new { cpu = 0.5m, memGi = 1, diskGi = 5 },
                }, ct);
            created.StatusCode.Should().Be(HttpStatusCode.Created);
            await WaitClusterActiveAsync(fx, cluster, ct, "east1");

            var allocBefore = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                $"/s3worker/portalloc/{cluster}", ct)).Value!.Value;
            var endpointsBefore = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                $"/s3/clusters/{cluster}/endpoints", ct)).Value!.Value;
            var configBefore = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                $"/s3/clusters/{cluster}/config", ct)).Value!;
            var containerIdBefore = await fx.ContainerIdAsync($"s3w-{cluster}-east1");
            containerIdBefore.Should().NotBeEmpty();

            // Act: 3 последовательных тика воркера (ScanIntervalSec=1 + запас
            // на сдвиг фазы) — заявки исчерпаны, работают только надзорные лупы.
            await Task.Delay(TimeSpan.FromSeconds(3.5), ct);

            // Assert: значения стабильны, контейнер не пересоздан.
            (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                $"/s3worker/portalloc/{cluster}", ct)).Value!.Value.Should().Be(allocBefore);
            (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                $"/s3/clusters/{cluster}/endpoints", ct)).Value!.Value.Should().Be(endpointsBefore);
            var configAfter = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                $"/s3/clusters/{cluster}/config", ct)).Value!;
            configAfter.Value.Should().Be(configBefore.Value, "config is canonical — no rewrite");
            configAfter.ModRevision.Should().Be(configBefore.ModRevision,
                "after state removal there are no new config writes (idempotency)");
            (await fx.ContainerIdAsync($"s3w-{cluster}-east1")).Should().Be(containerIdBefore,
                "Active ticks do not re-create the container");

            // Act: DELETE → 202 → демонтаж воркером (канон самоочистки).
            await DeleteClusterAndWaitCleanAsync(fx, api, cluster, ct, "east1");
        }
        catch
        {
            fx.MarkFailed();
            throw;
        }
    }

    // Сценарий 3 AddNode (spec §5.2, arch/23 §2.2): Active N=2 → POST /nodes →
    // 201 → west1 RUNNING ≤ бюджета; endpoints RMW — 3 адреса, прежние на
    // месте; config.nodes актуализирован до 3; volume west1 есть, bucket
    // на west1 создан.
    [Fact]
    public async Task AddNode_GrowsClusterToThreeNodes()
    {
        // Arrange: гейт запуска — без docker-режима E2E скипается.
        if (Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") != "1")
        {
            Assert.Skip("PGW_TEST_DOCKER=1 not set — docker E2E skipped");
            return;
        }

        await using var fx = await S3E2eEnvironment.StartAsync("addnode");
        var cluster = $"e2e{fx.ClusterTag}";
        using var api = fx.CreateApiHttpClient();
        var ct = TestContext.Current.CancellationToken;

        try
        {
            await WaitWorkerAsync(fx, api, ct);

            // Arrange: кластер N=2 до Active (канон сценария 1).
            using var created = await api.PostAsJsonAsync("/api/s3/clusters",
                new
                {
                    name = cluster,
                    platform = "owns3",
                    secretProfile = S3E2eEnvironment.ProfileName,
                    instances = new object[]
                    {
                        new { name = "east1", placement = new { dockerHost = "local" } },
                        new { name = "east2", placement = new { dockerHost = "local" } },
                    },
                    resources = new { cpu = 0.5m, memGi = 1, diskGi = 5 },
                }, ct);
            created.StatusCode.Should().Be(HttpStatusCode.Created);
            await WaitClusterActiveAsync(fx, cluster, ct, "east1", "east2");
            var endpointsBefore = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                $"/s3/clusters/{cluster}/endpoints", ct)).Value!.Value;

            // Act: add-node west1 → 201 (ответ {cluster,node}).
            using var added = await api.PostAsJsonAsync(
                $"/api/s3/clusters/{cluster}/nodes",
                new { name = "west1", placement = new { dockerHost = "local" } }, ct);
            var addedBody = await added.Content.ReadAsStringAsync(ct);
            added.StatusCode.Should().Be(HttpStatusCode.Created, addedBody);

            // [PHASE] wait-add: конвергенция D — west1 RUNNING + endpoints 3
            // адреса + config.nodes=3 (финал D: RMW endpoints, потом config).
            await fx.WaitPhaseAsync("add-converged", async () =>
            {
                var state = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                    $"/s3/clusters/{cluster}/nodes/west1/state", ct)).Value?.Value;
                if (state != "RUNNING" || !await fx.ContainerAliveAsync($"s3w-{cluster}-west1"))
                    return false;
                var endpoints = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                    $"/s3/clusters/{cluster}/endpoints", ct)).Value?.Value;
                if (endpoints?.Split(',').Length != 3)
                    return false;
                var config = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                    $"/s3/clusters/{cluster}/config", ct)).Value?.Value;
                return config is not null && config.Contains("\"nodes\":3");
            }, TimeSpan.FromSeconds(115), ct);

            // Assert 1: endpoints RMW — 3 адреса, прежние 2 на месте (arch/23 §2.2).
            var endpointsAfter = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                $"/s3/clusters/{cluster}/endpoints", ct)).Value!.Value;
            var addresses = endpointsAfter.Split(',');
            addresses.Should().HaveCount(3, endpointsAfter);
            endpointsAfter.Should().Contain(endpointsBefore.Split(',')[0]);
            endpointsAfter.Should().Contain(endpointsBefore.Split(',')[1]);

            // Assert 2: config.nodes актуализирован до 3 (txn по mod_revision).
            var config = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                $"/s3/clusters/{cluster}/config", ct)).Value!.Value;
            using (var doc = JsonDocument.Parse(config))
            {
                doc.RootElement.GetProperty("nodes").GetInt32().Should().Be(3);
                doc.RootElement.TryGetProperty("state", out _).Should().BeFalse();
            }

            // Assert 3: volume west1 существует; bucket создан и на west1
            // (порт west1 — из portalloc: {"west1":{"host":…,"s3":p}}).
            (await fx.VolumeExistsAsync($"s3w-{cluster}-west1-data"))
                .Should().BeTrue("data volume of west1 created by the worker");
            var alloc = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                $"/s3worker/portalloc/{cluster}", ct)).Value!.Value;
            int node3Port;
            using (var allocDoc = JsonDocument.Parse(alloc))
            {
                node3Port = allocDoc.RootElement.GetProperty("west1")
                    .GetProperty("s3").GetInt32();
            }

            (await ListBucketsWithRootAsync(node3Port, ct))
                .Should().Contain(S3E2eEnvironment.BucketName, "bucket on west1 created");

            // Act 4: DELETE → 202 → демонтаж воркером (канон самоочистки).
            await DeleteClusterAndWaitCleanAsync(fx, api, cluster, ct, "east1", "east2", "west1");
        }
        catch
        {
            fx.MarkFailed();
            throw;
        }
    }

    // [PHASE] wait-worker: /healthz по mTLS готов (бюджет ≤ 30 с).
    private static async Task WaitWorkerAsync(S3E2eEnvironment fx, HttpClient api, CancellationToken ct)
        => await fx.WaitPhaseAsync("wait-worker", async () =>
        {
            try
            {
                using var health = await api.GetAsync("/healthz", ct);
                return health.StatusCode == HttpStatusCode.OK;
            }
            catch (HttpRequestException)
            {
                return false; // Kestrel ещё поднимается
            }
        }, TimeSpan.FromSeconds(30), ct);

    // [PHASE] wait-active: все ноды RUNNING + endpoints записан + config.state
    // снят — канонический факт Active (arch/23 §2: воркер снимает state ПОСЛЕ
    // подъёма нод и записи endpoints; бюджет ≤ 100 c + тики — NodeBootSec).
    private static async Task WaitClusterActiveAsync(
        S3E2eEnvironment fx, string cluster, CancellationToken ct, params string[] nodes)
    {
        await fx.WaitPhaseAsync($"active-{string.Join('-', nodes)}", async () =>
        {
            foreach (var node in nodes)
            {
                var state = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                    $"/s3/clusters/{cluster}/nodes/{node}/state", ct)).Value?.Value;
                if (state != "RUNNING" || !await fx.ContainerAliveAsync($"s3w-{cluster}-{node}"))
                    return false;
            }

            var endpoints = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                $"/s3/clusters/{cluster}/endpoints", ct)).Value?.Value;
            if (string.IsNullOrWhiteSpace(endpoints))
                return false;
            var config = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                $"/s3/clusters/{cluster}/config", ct)).Value?.Value;
            return config is not null && !config.Contains("\"state\"");
        }, TimeSpan.FromSeconds(115), ct);
    }

    // DELETE кластера → 202 → wait-clean: контейнеры нод мертвы, тома данных
    // сняты, префикс /s3/clusters/<C>/ пуст, portalloc снят (процесс X0–X3).
    private static async Task DeleteClusterAndWaitCleanAsync(
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

    // S3-клиент к ноде root-кредом (path-style; advertised-хост резолвим с хоста).
    private static async Task<List<string>> ListBucketsWithRootAsync(int port, CancellationToken ct)
    {
        using var s3 = new AmazonS3Client(
            new Amazon.Runtime.BasicAWSCredentials(
                S3E2eEnvironment.RootUser, S3E2eEnvironment.RootPassword),
            new AmazonS3Config
            {
                ServiceURL = $"http://host.docker.internal:{port}",
                AuthenticationRegion = RegionEndpoint.USEast1.SystemName,
                ForcePathStyle = true,
                Timeout = TimeSpan.FromSeconds(10),
                MaxErrorRetry = 2,
            });
        var list = await s3.ListBucketsAsync(ct);
        return list.Buckets.Select(b => b.BucketName).ToList();
    }
}
