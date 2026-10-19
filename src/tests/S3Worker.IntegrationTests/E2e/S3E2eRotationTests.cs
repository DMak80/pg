using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Amazon;
using Amazon.S3;
using FluentAssertions;
using Xunit;

namespace S3Worker.IntegrationTests.E2e;

// Ротация и sync-probe E2E S3Worker (spec §11 сценарии 8–9):
// 8 — ротация = перевыпуск per-cluster кредов в secrets-ключе etcd
// (arch/23 §8): root_user сохраняется, root_password/ключи перевыпускаются,
// ноды rolling-пересоздаются с новым env; НОВАЯ root-пара допущена нодами
// (200), СТАРАЯ — отклонена (403); docker-env нод несёт нового root_user
// (сравнение в коде БЕЗ печати);
// 9 — sync-probe (t33): POST → 202 + заявка /s3worker/probes/<C>, тик зонда
// исполняет её (пустой меш — links {} OK / SYNCED без пар) и сносит заявку.
// Гейт: PGW_TEST_DOCKER=1.
public class S3E2eRotationTests
{
    [Fact]
    public async Task Rotation_ReissuesSecretsAndRecreatesNodes()
    {
        // Arrange: гейт запуска — без docker-режима E2E скипается.
        if (Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") != "1")
        {
            Assert.Skip("PGW_TEST_DOCKER=1 not set — docker E2E skipped");
            return;
        }

        await using var fx = await S3E2eEnvironment.StartAsync("rotate");
        var cluster = $"e2e{fx.ClusterTag}";
        using var api = fx.CreateApiHttpClient();
        var ct = TestContext.Current.CancellationToken;

        try
        {
            await S3E2eScaleTests.WaitWorkerAsync(fx, api, ct);
            await S3E2eScaleTests.CreateClusterAsync(fx, api, cluster, ct, "east1", "east2");
            await S3E2eScaleTests.WaitClusterActiveAsync(fx, cluster, ct, "east1", "east2");

            // Baseline: root-пара из secrets-ключа (значения — только в память,
            // БЕЗ печати); docker-env нод несёт этого root_user.
            var (rootUser, rootPassword) = await fx.RootPairOfAsync(cluster, ct);
            rootUser.Should().Be($"s3w-{cluster}-root", "canonical root_user of the cluster");
            var id1Before = await fx.ContainerIdAsync($"s3w-{cluster}-east1");
            var id2Before = await fx.ContainerIdAsync($"s3w-{cluster}-east2");
            EnvOf(await fx.ContainerEnvJsonAsync($"s3w-{cluster}-east1"))["OWNS3_ROOT_USER"]
                .Should().Be(rootUser, "nodes carry the per-cluster secrets env");

            // Act: заявка ротации → 201 → воркер перевыпускает креды в
            // secrets-ключе и rolling-пересоздаёт ноды (arch/23 §8).
            using var rotated = await api.PostAsync(
                $"/api/s3/clusters/{cluster}/rotations", content: null, ct);
            var rotatedBody = await rotated.Content.ReadAsStringAsync(ct);
            rotated.StatusCode.Should().Be(HttpStatusCode.Created, rotatedBody);

            await fx.WaitPhaseAsync("wait-rotation", async () =>
            {
                if (await GetOrNullAsync(fx, $"/s3worker/rotations/{cluster}", ct) is not null)
                    return false;
                var work = await GetOrNullAsync(fx, $"/s3worker/work/{cluster}", ct);
                if (work is not null && !work.Contains("\"phase\":\"done\""))
                    return false;
                return await fx.ContainerIdAsync($"s3w-{cluster}-east1") != id1Before
                    && await fx.ContainerIdAsync($"s3w-{cluster}-east2") != id2Before
                    && await GetOrNullAsync(fx, $"/s3/clusters/{cluster}/nodes/east1/state", ct) == "RUNNING"
                    && await GetOrNullAsync(fx, $"/s3/clusters/{cluster}/nodes/east2/state", ct) == "RUNNING";
            }, TimeSpan.FromSeconds(90), ct);

            // Assert 1: secrets-ключ перевыпущен — root_user сохранён,
            // root_password изменился (сравнение в коде, БЕЗ печати: провал
            // ассерта не выведет значение в лог/trx).
            var (rootUserAfter, rootPasswordAfter) = await fx.RootPairOfAsync(cluster, ct);
            rootUserAfter.Should().Be(rootUser, "rotation keeps the canonical root_user");
            (!string.Equals(rootPasswordAfter, rootPassword, StringComparison.Ordinal))
                .Should().BeTrue("rotation re-issues the root password");
            EnvOf(await fx.ContainerEnvJsonAsync($"s3w-{cluster}-east1"))["OWNS3_ROOT_USER"]
                .Should().Be(rootUserAfter, "re-created nodes carry the re-issued env");

            // Assert 2: ноды допускают НОВУЮ пару и отклоняют СТАРУЮ —
            // GetBucketLocation (порт из endpoints — актуален).
            var endpoints = S3E2eScaleTests.EndpointsOf(
                await GetOrNullAsync(fx, $"/s3/clusters/{cluster}/endpoints", ct));
            foreach (var address in endpoints)
            {
                var port = int.Parse(address.Split(':')[1]);
                (await RootCallCodeAsync(port, rootUserAfter, rootPasswordAfter, ct))
                    .Should().Be(HttpStatusCode.OK, $"re-issued pair admitted to :{port}");
                (await RootCallCodeAsync(port, rootUser, rootPassword, ct))
                    .Should().Be(HttpStatusCode.Forbidden, $"pre-rotation pair rejected on :{port}");
            }

            // Финал: DELETE кластера → демонтаж (канон самоочистки).
            await S3E2eScaleTests.DeleteClusterAndWaitCleanAsync(fx, api, cluster, ct, "east1", "east2");
        }
        catch
        {
            // docs/e2e-launch.md §3: упавший сценарий — teardown остановит
            // контейнеры, но не удалит (разбор по артефактам).
            fx.MarkFailed();
            throw;
        }
    }

    // Сценарий 9 sync-probe (arch/24 §1.1, t33): POST → 202 + заявка
    // /s3worker/probes/&lt;C&gt;; тик зонда исполняет её (для кластера без линков —
    // пустая матрица OK / SYNCED без пар) и сносит заявку.
    [Fact]
    public async Task SyncProbe_RequestExecuted_EmptyMeshKeys()
    {
        // Arrange: гейт запуска — без docker-режима E2E скипается.
        if (Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") != "1")
        {
            Assert.Skip("PGW_TEST_DOCKER=1 not set — docker E2E skipped");
            return;
        }

        await using var fx = await S3E2eEnvironment.StartAsync("syncprobe");
        var cluster = $"e2e{fx.ClusterTag}";
        using var api = fx.CreateApiHttpClient();
        var ct = TestContext.Current.CancellationToken;

        try
        {
            await S3E2eScaleTests.WaitWorkerAsync(fx, api, ct);
            await S3E2eScaleTests.CreateClusterAsync(fx, api, cluster, ct, "east1");
            await S3E2eScaleTests.WaitClusterActiveAsync(fx, cluster, ct, "east1");

            // Act: sync-probe → 202 (заявка put-if-absent, t33).
            using var probed = await api.PostAsync(
                $"/api/s3/clusters/{cluster}/sync-probe", content: null, ct);
            probed.StatusCode.Should().Be(HttpStatusCode.Accepted);

            // Assert: тик зонда исполнил заявку — ключи канона пустого меша
            // (links {} = OK; pairs [] = SYNCED), заявка снесена.
            await fx.WaitPhaseAsync("sync-probed", async () =>
                await GetOrNullAsync(fx, $"/s3/clusters/{cluster}/connectivity", ct) is not null
                && await GetOrNullAsync(fx, $"/s3/clusters/{cluster}/sync", ct) is not null
                && await GetOrNullAsync(fx, $"/s3worker/probes/{cluster}", ct) is null,
                TimeSpan.FromSeconds(60), ct);

            // Финал: DELETE кластера → демонтаж (канон самоочистки).
            await S3E2eScaleTests.DeleteClusterAndWaitCleanAsync(fx, api, cluster, ct, "east1");
        }
        catch
        {
            fx.MarkFailed();
            throw;
        }
    }

    // ===== Локальные хелперы сценариев =====

    private static async Task<string?> GetOrNullAsync(
        S3E2eEnvironment fx, string key, CancellationToken ct)
        => (await fx.Gateway.GetAsync(fx.EtcdEndpoint, key, ct)).Value?.Value;

    // Env контейнера как словарь (docker inspect {{json .Config.Env}}).
    private static IReadOnlyDictionary<string, string> EnvOf(string raw)
    {
        var env = new Dictionary<string, string>();
        foreach (var item in JsonDocument.Parse(raw).RootElement.EnumerateArray())
        {
            var line = item.GetString()!;
            var eq = line.IndexOf('=');
            if (eq > 0)
                env[line[..eq]] = line[(eq + 1)..];
        }

        return env;
    }

    // Код ответа S3 ListBuckets с заданной root-парой (403/200 без печати кред).
    private static async Task<HttpStatusCode> RootCallCodeAsync(
        int port, string user, string password, CancellationToken ct)
    {
        try
        {
            using var s3 = new AmazonS3Client(
                new Amazon.Runtime.BasicAWSCredentials(user, password),
                new AmazonS3Config
                {
                    ServiceURL = $"http://host.docker.internal:{port}",
                    AuthenticationRegion = RegionEndpoint.USEast1.SystemName,
                    ForcePathStyle = true,
                    Timeout = TimeSpan.FromSeconds(10),
                    MaxErrorRetry = 0,
                });
            await s3.ListBucketsAsync(ct);
            return HttpStatusCode.OK;
        }
        catch (AmazonS3Exception ex)
        {
            return ex.StatusCode;
        }
    }
}
