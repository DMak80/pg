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
            Assert.Skip("PGW_TEST_DOCKER=1 не задан — docker-E2E пропущен");
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
                .Should().Be(HttpStatusCode.BadRequest, "name: верхний регистр/дефис вне канона");
            (await CreateCodeAsync(api, platform: "pg", ct: ct))
                .Should().Be(HttpStatusCode.BadRequest, "platform — только owns3");
            (await CreateCodeAsync(api, nodes: 0, ct: ct))
                .Should().Be(HttpStatusCode.BadRequest, "nodes вне 1..9 (мин)");
            (await CreateCodeAsync(api, placements: Array.Empty<object>(), ct: ct))
                .Should().Be(HttpStatusCode.BadRequest, "placement пуст — размещать некуда");
            (await CreateCodeAsync(api, cpu: 99m, ct: ct))
                .Should().Be(HttpStatusCode.BadRequest, "cpu вне 0.01..64 (макс)");

            // Assert 2: 404 — чужой кластер (delete) и чужая нода (add-node).
            using var alien = await api.DeleteAsync("/api/s3/clusters/no-such-cluster", ct);
            alien.StatusCode.Should().Be(HttpStatusCode.NotFound, "кластера нет");
            using var alienNode = await api.PostAsJsonAsync(
                "/api/s3/clusters/no-such-cluster/nodes",
                new { placement = new { dockerHost = "local" } }, ct);
            alienNode.StatusCode.Should().Be(HttpStatusCode.NotFound, "add-node на чужое имя");

            // Arrange: живой кластер N=1 до Active (для 409/200-веток).
            await S3E2eScaleTests.CreateClusterAsync(fx, api, cluster, nodes: 1, ct);
            await S3E2eScaleTests.WaitClusterActiveAsync(fx, cluster, ct, "node1");

            // Assert 3: 409 — create-повтор на живом имени и delete последней ноды.
            (await CreateCodeAsync(api, name: cluster, ct: ct))
                .Should().Be(HttpStatusCode.Conflict, "дубль живого имени");
            using var lastNode = await api.DeleteAsync(
                $"/api/s3/clusters/{cluster}/nodes/node1", ct);
            lastNode.StatusCode.Should().Be(HttpStatusCode.Conflict,
                "последняя нода кластера N=1 невыводима");

            // Assert 4: rotation-повтор → 200 (первая заявка жива — no-op).
            using var first = await api.PostAsync(
                $"/api/s3/clusters/{cluster}/rotations", content: null, ct);
            first.StatusCode.Should().Be(HttpStatusCode.Created);
            using var second = await api.PostAsync(
                $"/api/s3/clusters/{cluster}/rotations", content: null, ct);
            second.StatusCode.Should().Be(HttpStatusCode.OK, "повтор при живой заявке — no-op");

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
            body.Should().Contain("BACKUPS-MAIN", "detail несёт перечень доступных профилей");
            body.Should().NotContain(S3E2eEnvironment.RootPassword,
                "значения секретов в ответ не попадают");
            body.Should().NotContain(S3E2eEnvironment.WriterSecretKey,
                "значения секретов в ответ не попадают");

            // Финал: заявка ротации жива — кластер сносится delete'ом
            // (ротация-заявка умирает вместе с координацией X2/X3).
            await S3E2eScaleTests.DeleteClusterAndWaitCleanAsync(fx, api, cluster, ct, "node1");
        }
        catch
        {
            // docs/e2e-launch.md §3: упавший сценарий — teardown остановит
            // контейнеры, но не удалит (разбор по артефактам).
            fx.MarkFailed();
            throw;
        }
    }

    // POST create c переопределяемыми полями (валидационные кейсы).
    private static async Task<HttpStatusCode> CreateCodeAsync(
        HttpClient api,
        string? name = null,
        string? platform = null,
        int? nodes = null,
        object[]? placements = null,
        decimal? cpu = null,
        CancellationToken ct = default)
    {
        using var response = await api.PostAsJsonAsync("/api/s3/clusters",
            new
            {
                name = name ?? $"val{Guid.NewGuid().ToString("N")[..8]}",
                platform = platform ?? "owns3",
                nodes = nodes ?? 1,
                secretProfile = S3E2eEnvironment.ProfileName,
                placements = placements ?? new object[] { new { dockerHost = "local" } },
                resources = new { cpu = cpu ?? 0.5m, memGi = 1, diskGi = 5 },
            }, ct);
        return response.StatusCode;
    }
}
