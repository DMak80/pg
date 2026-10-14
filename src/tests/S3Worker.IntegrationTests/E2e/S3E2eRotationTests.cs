using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Amazon;
using Amazon.S3;
using FluentAssertions;
using Xunit;

namespace S3Worker.IntegrationTests.E2e;

// Ротация и sync-probe E2E S3Worker (spec §11 сценарии 8–9):
// 8 — rolling-пересоздание нод с env ТЕКУЩЕГО профиля кластера (оператор
// обновляет env и перезапускает воркер, затем подаёт заявку): первая пара
// совпадает с env воркера (docker-env инспекция, сравнение в коде БЕЗ печати),
// после смены env воркера и повторной заявки ноды пересозданы с НОВЫМИ
// root-кредами (новая пара — 200, старая — 403);
// 9 — sync-probe 202-заглушка: ключей connectivity/sync в домене не появляется.
// Гейт: PGW_TEST_DOCKER=1.
public class S3E2eRotationTests
{
    // Вторая root-пара профиля (сменяемая env-грань воркера).
    private const string RotatedUser = "e2e-root2";
    private const string RotatedPassword = "e2e-root2-pwd";

    [Fact]
    public async Task Rotation_RecreatesNodesWithCurrentProfileSecrets()
    {
        // Arrange: гейт запуска — без docker-режима E2E скипается.
        if (Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") != "1")
        {
            Assert.Skip("PGW_TEST_DOCKER=1 не задан — docker-E2E пропущен");
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

            // Baseline: env нод несёт root-пару воркера (сравнение В КОДЕ —
            // значения в лог не печатаются).
            var id1Before = await fx.ContainerIdAsync($"s3w-{cluster}-east1");
            var id2Before = await fx.ContainerIdAsync($"s3w-{cluster}-east2");
            EnvOf(await fx.ContainerEnvJsonAsync($"s3w-{cluster}-east1"))["OWNS3_ROOT_USER"]
                .Should().Be(S3E2eEnvironment.RootUser, "ноды созданы с env профиля воркера");

            // Act 1: заявка ротации с НЕИЗМЕНЁННЫМИ секретами (env нод уже
            // правильный) → matches-сверка: заявка гасится БЕЗ пересозданий
            // (spec §14.4 «повторный запрос — no-op по факту»; arch/24 §4:
            // rolling трогает только ноды с чужим env).
            using var rotated = await api.PostAsync(
                $"/api/s3/clusters/{cluster}/rotations", content: null, ct);
            var rotatedBody = await rotated.Content.ReadAsStringAsync(ct);
            rotated.StatusCode.Should().Be(HttpStatusCode.Created, rotatedBody);

            await fx.WaitPhaseAsync("wait-rotation-noop", async () =>
            {
                if (await GetOrNullAsync(fx, $"/s3worker/rotations/{cluster}", ct) is not null)
                    return false;
                var work = await GetOrNullAsync(fx, $"/s3worker/work/{cluster}", ct);
                return work is null || work.Contains("\"phase\":\"done\"");
            }, TimeSpan.FromSeconds(60), ct);

            // Assert 1: ноды НЕ пересозданы (env уже правильный), env нод ==
            // env воркера, ноды живы.
            (await fx.ContainerIdAsync($"s3w-{cluster}-east1")).Should().Be(id1Before,
                "env нод уже на профиле — matches no-op без пересозданий");
            (await fx.ContainerIdAsync($"s3w-{cluster}-east2")).Should().Be(id2Before,
                "env нод уже на профиле — matches no-op без пересозданий");
            EnvOf(await fx.ContainerEnvJsonAsync($"s3w-{cluster}-east1"))["OWNS3_ROOT_USER"]
                .Should().Be(S3E2eEnvironment.RootUser);

            // Act 2: смена значений — оператор обновляет env и перезапускает
            // воркер (та же пара env, вторая root-пара), затем новая заявка.
            await fx.RecreateWorkerAsync(0, new Dictionary<string, string>
            {
                ["S3W_NODE_SECRETS__BACKUPS-MAIN__ROOT_USER"] = RotatedUser,
                ["S3W_NODE_SECRETS__BACKUPS-MAIN__ROOT_PASSWORD"] = RotatedPassword,
            }, ct);
            await fx.WaitPhaseAsync("wait-worker-recreated", async () =>
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

            using var rotated2 = await api.PostAsync(
                $"/api/s3/clusters/{cluster}/rotations", content: null, ct);
            rotated2.StatusCode.Should().Be(HttpStatusCode.Created);
            var id1Mid = await fx.ContainerIdAsync($"s3w-{cluster}-east1");
            var id2Mid = await fx.ContainerIdAsync($"s3w-{cluster}-east2");
            await fx.WaitPhaseAsync("wait-rotation2", async () =>
            {
                if (await GetOrNullAsync(fx, $"/s3worker/rotations/{cluster}", ct) is not null)
                    return false;
                var work = await GetOrNullAsync(fx, $"/s3worker/work/{cluster}", ct);
                if (work is not null && !work.Contains("\"phase\":\"done\""))
                    return false;
                return await fx.ContainerIdAsync($"s3w-{cluster}-east1") != id1Mid
                    && await fx.ContainerIdAsync($"s3w-{cluster}-east2") != id2Mid
                    && await S3E2eScaleTests.GetOrNullAsync(fx,
                            $"/s3/clusters/{cluster}/nodes/east1/state", ct) == "RUNNING"
                    && await S3E2eScaleTests.GetOrNullAsync(fx,
                            $"/s3/clusters/{cluster}/nodes/east2/state", ct) == "RUNNING";
            }, TimeSpan.FromSeconds(90), ct);

            // Assert 2: ноды с НОВЫМИ root-кредами — GetBucketLocation НОВОЙ
            // парой → 200, СТАРОЙ → 403 (порт из endpoints — актуален).
            var endpoints = S3E2eScaleTests.EndpointsOf(
                await GetOrNullAsync(fx, $"/s3/clusters/{cluster}/endpoints", ct));
            foreach (var address in endpoints)
            {
                var port = int.Parse(address.Split(':')[1]);
                (await RootCallCodeAsync(port, RotatedUser, RotatedPassword, ct))
                    .Should().Be(HttpStatusCode.OK, $"новая пара допущена к :{port}");
                (await RootCallCodeAsync(port, S3E2eEnvironment.RootUser,
                        S3E2eEnvironment.RootPassword, ct))
                    .Should().Be(HttpStatusCode.Forbidden, $"старая пара отклонена на :{port}");
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
    public async Task SyncProbe_ЗаявкаИсполнена_КлючиПустогоМеша()
    {
        // Arrange: гейт запуска — без docker-режима E2E скипается.
        if (Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") != "1")
        {
            Assert.Skip("PGW_TEST_DOCKER=1 не задан — docker-E2E пропущен");
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
