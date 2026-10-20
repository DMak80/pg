using System.Net;
using System.Text.Json;
using Amazon;
using Amazon.S3;
using FluentAssertions;
using Xunit;

namespace S3Worker.IntegrationTests.E2e;

// Надзор E2E S3Worker (spec §11 сценарий 6, arch/24 §5 C): пересоздание
// убитой ноды на том же placed_on (volume жив — тест-объект читается),
// UNREACHABLE по молчащему health (NodeDeadSec=15, деструктива нет),
// usage-ключ кормится тиками надзора, восстановление после unpause.
// Гейт: PGW_TEST_DOCKER=1.
public class S3E2eSupervisionTests
{
    [Fact]
    public async Task Supervision_RebuildsKilledNodeAndMarksUnreachable()
    {
        // Arrange: гейт запуска — без docker-режима E2E скипается.
        if (Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") != "1")
        {
            Assert.Skip("PGW_TEST_DOCKER=1 not set — docker E2E skipped");
            return;
        }

        await using var fx = await S3E2eEnvironment.StartAsync("supervise");
        var cluster = $"e2e{fx.ClusterTag}";
        using var api = fx.CreateApiHttpClient();
        var ct = TestContext.Current.CancellationToken;

        try
        {
            await S3E2eScaleTests.WaitWorkerAsync(fx, api, ct);
            await S3E2eScaleTests.CreateClusterAsync(fx, api, cluster, ct, "east1");
            await S3E2eScaleTests.WaitClusterActiveAsync(fx, cluster, ct, "east1");

            // Arrange: тест-объект в bucket ноды (volume переживёт пересоздание).
            var endpointsValue = await S3E2eScaleTests.GetOrNullAsync(fx,
                $"/s3/clusters/{cluster}/endpoints", ct);
            var endpoints = S3E2eScaleTests.EndpointsOf(endpointsValue)[0];
            var probePort = int.Parse(endpoints.Split(':')[1]);
            await PutTestObjectAsync(fx, cluster, probePort, ct);

            var placedOn = await S3E2eScaleTests.GetOrNullAsync(fx,
                $"/s3/clusters/{cluster}/nodes/east1/placed_on", ct);
            placedOn.Should().Be("local");

            // Act 1: kill ноды → тик надзора пересоздаёт на том же placed_on
            // (PROVISIONING → RUNNING), volume сохранён — тест-объект читается.
            // Гвард устаревшего чтения: etcd-значение state==RUNNING обязан
            // подкреплять СВЕЖИЙ usage-ключ (updated_unix позже сноса) — usage
            // пишет ТОЛЬКО health-ok тик надзора живой ноды (arch/24 §5 C).
            // Без гварда поллинг ловит RUNNING до сноса, пока пересозданный
            // контейнер ещё в PROVISIONING: пауза такой ноды клинит сценарий —
            // трек PROVISIONING заморожен (чужой процесс), add-node ловит
            // 409 на start запаузенного (docker отказывает rm paused) —
            // UNREACHABLE не наступает никогда (флейк wait-unreachable).
            var removedAtUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            await fx.RunDockerAsync(["rm", "-f", $"s3w-{cluster}-east1"], ct);
            await fx.WaitPhaseAsync("wait-rebuild", async () =>
            {
                if (await S3E2eScaleTests.GetOrNullAsync(fx,
                        $"/s3/clusters/{cluster}/nodes/east1/state", ct) != "RUNNING")
                    return false;
                var usage = await S3E2eScaleTests.GetOrNullAsync(fx,
                    $"/s3/clusters/{cluster}/nodes/east1/usage", ct);
                if (usage is null)
                    return false;
                using var doc = JsonDocument.Parse(usage);
                return doc.RootElement.GetProperty("updated_unix").GetInt64() > removedAtUnix;
            }, TimeSpan.FromSeconds(60), ct);

            (await S3E2eScaleTests.GetOrNullAsync(fx,
                    $"/s3/clusters/{cluster}/nodes/east1/placed_on", ct))
                .Should().Be(placedOn, "re-creation — on the same placed_on (node volume lives on the host)");
            (await ListTestObjectsAsync(fx, cluster, probePort, ct)).Should().Contain("s3w-e2e-supervise",
                "test object survived node re-creation (volume preserved)");

            // Act 2: pause → health молчит → UNREACHABLE по NodeDeadSec=15
            // (ждать ≤ 40 c); деструктива нет: контейнер существует, volume есть.
            await fx.RunDockerAsync(["pause", $"s3w-{cluster}-east1"], ct);
            await fx.WaitPhaseAsync("wait-unreachable", async () =>
                await S3E2eScaleTests.GetOrNullAsync(fx,
                    $"/s3/clusters/{cluster}/nodes/east1/state", ct) == "UNREACHABLE",
                TimeSpan.FromSeconds(40), ct);
            (await fx.ContainerAliveAsync($"s3w-{cluster}-east1")).Should().BeTrue(
                "UNREACHABLE — no destructive action: container alive (paused)");
            (await fx.VolumeExistsAsync($"s3w-{cluster}-east1-data")).Should().BeTrue(
                "UNREACHABLE — no destructive action: volume in place");

            // Act 3: unpause → health отвечает → RUNNING возвращается.
            await fx.RunDockerAsync(["unpause", $"s3w-{cluster}-east1"], ct);
            await fx.WaitPhaseAsync("wait-recovered", async () =>
                await S3E2eScaleTests.GetOrNullAsync(fx,
                    $"/s3/clusters/{cluster}/nodes/east1/state", ct) == "RUNNING",
                TimeSpan.FromSeconds(40), ct);

            // Act 4: usage-ключ кормится тиками надзора живой ноды
            // (HealthIntervalSec=3): updated_unix растёт, used/total > 0
            // (при UNREACHABLE usage заморожен — грань Task 2 пишется по живым).
            var usageBefore = await S3E2eScaleTests.GetOrNullAsync(fx,
                $"/s3/clusters/{cluster}/nodes/east1/usage", ct);
            await fx.WaitPhaseAsync("wait-usage-update", async () =>
            {
                var usage = await S3E2eScaleTests.GetOrNullAsync(fx,
                    $"/s3/clusters/{cluster}/nodes/east1/usage", ct);
                if (usage is null || usage == usageBefore)
                    return false;
                using var doc = JsonDocument.Parse(usage);
                var root = doc.RootElement;
                return root.GetProperty("used_bytes").GetInt64() > 0
                    && root.GetProperty("total_bytes").GetInt64() > 0;
            }, TimeSpan.FromSeconds(20), ct);

            // Финал: DELETE кластера → демонтаж (канон самоочистки).
            await S3E2eScaleTests.DeleteClusterAndWaitCleanAsync(fx, api, cluster, ct, "east1");
        }
        catch
        {
            // docs/e2e-launch.md §3: упавший сценарий — teardown остановит
            // контейнеры, но не удалит (разбор по артефактам).
            fx.MarkFailed();
            throw;
        }
    }

    // S3-клиент к ноде root-кредом (path-style).
    private static async Task<AmazonS3Client> NewS3ClientAsync(
        S3E2eEnvironment fx, string cluster, int port, CancellationToken ct)
    {
        // Креды — per-cluster secrets-ключ etcd (arch/23 §2); без печати.
        var (rootUser, rootPassword) = await fx.RootPairOfAsync(cluster, ct);
        return new AmazonS3Client(
            new Amazon.Runtime.BasicAWSCredentials(rootUser, rootPassword),
            new AmazonS3Config
            {
                ServiceURL = $"http://host.docker.internal:{port}",
                AuthenticationRegion = RegionEndpoint.USEast1.SystemName,
                ForcePathStyle = true,
                Timeout = TimeSpan.FromSeconds(10),
                MaxErrorRetry = 2,
            });
    }

    private static async Task PutTestObjectAsync(
        S3E2eEnvironment fx, string cluster, int port, CancellationToken ct)
    {
        using var s3 = await NewS3ClientAsync(fx, cluster, port, ct);
        await s3.PutObjectAsync(new Amazon.S3.Model.PutObjectRequest
        {
            BucketName = S3E2eEnvironment.BucketName,
            Key = "s3w-e2e-supervise",
            ContentBody = "supervision-probe",
        }, ct);
    }

    private static async Task<List<string>> ListTestObjectsAsync(
        S3E2eEnvironment fx, string cluster, int port, CancellationToken ct)
    {
        using var s3 = await NewS3ClientAsync(fx, cluster, port, ct);
        var list = await s3.ListObjectsV2Async(new Amazon.S3.Model.ListObjectsV2Request
        {
            BucketName = S3E2eEnvironment.BucketName,
        }, ct);
        return list.S3Objects.Select(o => o.Key).ToList();
    }
}
