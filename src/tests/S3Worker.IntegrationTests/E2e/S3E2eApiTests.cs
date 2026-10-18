using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using FluentAssertions;
using Xunit;

namespace S3Worker.IntegrationTests.E2e;

// API-валидации E2E S3Worker (spec §11 сценарий 10, §6 таблица кодов):
// 400 — name/platform/nodes/placement/resources (по кейсу на каждый);
// 404 — чужой кластер / чужая нода (add-node на несуществующий кластер);
// 409 — create-повтор на живом имени и delete последней ноды N=1
// (гварды nodes<9 при add-node закрыты юнитами и интеграционными —
// подъём N=9 в E2E неоправданно долог); rotation-повтор → 200;
// 422 — несуществующий secretProfile с перечнем имён в detail, значения
// секретов в теле ответа отсутствуют. Гейт: PGW_TEST_DOCKER=1.
public class S3E2eApiTests
{
    [Fact]
    public async Task ApiValidation_ReturnsCanonicalCodes()
    {
        // Arrange: гейт запуска — без docker-режима E2E скипается.
        if (Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") != "1")
        {
            Assert.Skip("PGW_TEST_DOCKER=1 not set — docker E2E skipped");
            return;
        }

        await using var fx = await S3E2eEnvironment.StartAsync("apitest");
        var cluster = $"e2e{fx.ClusterTag}";
        using var api = fx.CreateApiHttpClient();
        var ct = TestContext.Current.CancellationToken;

        try
        {
            await S3E2eScaleTests.WaitWorkerAsync(fx, api, ct);

            // Assert 1: 400 — по кейсу на каждый бракованный аргумент.
            (await CreateCodeAsync(api, name: "Bad-Name", ct: ct))
                .Should().Be(HttpStatusCode.BadRequest, "name: uppercase/hyphen outside the canon");
            (await CreateCodeAsync(api, platform: "pg", ct: ct))
                .Should().Be(HttpStatusCode.BadRequest, "platform — owns3 only");
            (await CreateCodeAsync(api, instances: Enumerable.Range(0, 10)
                    .Select(i => (object)new { name = $"n{i}", placement = new { dockerHost = "local" } })
                    .ToArray(), ct: ct))
                .Should().Be(HttpStatusCode.BadRequest, "instances outside 0..9 (max)");
            (await CreateCodeAsync(api, instances: new object[] { new { name = "east1" } }, ct: ct))
                .Should().Be(HttpStatusCode.BadRequest, "placement missing — nowhere to place");
            (await CreateCodeAsync(api, instances: new object[] { new { name = "orchestrator", placement = new { dockerHost = "local" } } }, ct: ct))
                .Should().Be(HttpStatusCode.BadRequest, "orchestrator reserved (orchestrator PKI)");
            (await CreateCodeAsync(api, cpu: 99m, ct: ct))
                .Should().Be(HttpStatusCode.BadRequest, "cpu outside 0.01..64 (max)");

            // Assert 2: 404 — чужой кластер (delete) и чужая нода (add-node).
            using var alien = await api.DeleteAsync("/api/s3/clusters/no-such-cluster", ct);
            alien.StatusCode.Should().Be(HttpStatusCode.NotFound, "cluster does not exist");
            using var alienNode = await api.PostAsJsonAsync(
                "/api/s3/clusters/no-such-cluster/nodes",
                new { name = "east1", placement = new { dockerHost = "local" } }, ct);
            alienNode.StatusCode.Should().Be(HttpStatusCode.NotFound, "add-node on a foreign name");

            // Arrange: живой кластер N=1 до Active (для 409/200-веток).
            await S3E2eScaleTests.CreateClusterAsync(fx, api, cluster, ct, "east1");
            await S3E2eScaleTests.WaitClusterActiveAsync(fx, cluster, ct, "east1");

            // Assert 3: 409 — create-повтор на живом имени и delete последней ноды.
            (await CreateCodeAsync(api, name: cluster, ct: ct))
                .Should().Be(HttpStatusCode.Conflict, "duplicate of a live name");
            using var lastNode = await api.DeleteAsync(
                $"/api/s3/clusters/{cluster}/nodes/east1", ct);
            lastNode.StatusCode.Should().Be(HttpStatusCode.Conflict,
                "last node of an N=1 cluster cannot be removed");

            // Assert 4: rotation-повтор → 200 (первая заявка жива — no-op).
            using var first = await api.PostAsync(
                $"/api/s3/clusters/{cluster}/rotations", content: null, ct);
            first.StatusCode.Should().Be(HttpStatusCode.Created);
            using var second = await api.PostAsync(
                $"/api/s3/clusters/{cluster}/rotations", content: null, ct);
            second.StatusCode.Should().Be(HttpStatusCode.OK, "repeat with a live request — no-op");

            // Assert 5: 422 — несуществующий secretProfile: перечень имён в
            // detail, ЗНАЧЕНИЯ секретов в теле ответа отсутствуют.
            using var noProfile = await api.PostAsJsonAsync("/api/s3/clusters",
                new
                {
                    name = $"{cluster}x",
                    platform = "owns3",
                    nodes = 1,
                    secretProfile = "no-such-profile",
                    placements = new object[] { new { dockerHost = "local" } },
                    resources = new { cpu = 0.5m, memGi = 1, diskGi = 5 },
                }, ct);
            noProfile.StatusCode.Should().Be(HttpStatusCode.UnprocessableEntity);
            var body = await noProfile.Content.ReadAsStringAsync(ct);
            body.Should().Contain("BACKUPS-MAIN", "detail carries the list of available profiles");
            body.Should().NotContain(S3E2eEnvironment.RootPassword,
                "secret values do not leak into the response");
            body.Should().NotContain(S3E2eEnvironment.WriterSecretKey,
                "secret values do not leak into the response");

            // Финал: заявка ротации жива — кластер сносится delete'ом
            // (ротация-заявка умирает вместе с координацией X2/X3).
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

    // POST create c переопределяемыми полями (валидационные кейсы, instances[]).
    private static async Task<HttpStatusCode> CreateCodeAsync(
        HttpClient api,
        string? name = null,
        string? platform = null,
        object[]? instances = null,
        decimal? cpu = null,
        CancellationToken ct = default)
    {
        using var response = await api.PostAsJsonAsync("/api/s3/clusters",
            new
            {
                name = name ?? $"val{Guid.NewGuid().ToString("N")[..8]}",
                platform = platform ?? "owns3",
                secretProfile = S3E2eEnvironment.ProfileName,
                instances = instances ?? new object[] { new { name = "east1", placement = new { dockerHost = "local" } } },
                resources = new { cpu = cpu ?? 0.5m, memGi = 1, diskGi = 5 },
            }, ct);
        return response.StatusCode;
    }
}
