using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Amazon.S3;
using Amazon.S3.Model;
using FluentAssertions;
using Xunit;

namespace S3Worker.IntegrationTests.E2e;

// Docker-E2E зонда F (spec §8.3, arch/24 §5 F, t33): штатная синхронность,
// прогон по команде (заявка put-if-absent → 202), недоступная нода (матрица
// по канону arch/23 §3: строк нет у неответивших; одиночный offline-линк без
// живых → DOWN), отставание → догон (lag_objects → SYNCED), deny-инстанс
// (пустая матрица = OK). Все ожидания — поллинг WaitPhaseAsync ([PHASE]);
// изоляция/телеметрия/teardown — в S3E2eEnvironment; гейт PGW_TEST_DOCKER=1.
public class S3E2eSyncProbeTests
{
    [Fact]
    public async Task SteadyState_MatrixOk_Synced()
    {
        // Arrange: кластер 2 инстанса + links {east1,east2}; объект в bucket.
        if (Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") != "1")
        {
            Assert.Skip("PGW_TEST_DOCKER=1 not set — docker E2E skipped");
            return;
        }

        await using var fx = await S3E2eEnvironment.StartAsync("sync-basic");
        var cluster = $"sync{fx.ClusterTag}";
        using var api = fx.CreateApiHttpClient();
        var ct = TestContext.Current.CancellationToken;

        try
        {
            await S3E2eScaleTests.WaitWorkerAsync(fx, api, ct);
            await S3E2eScaleTests.CreateClusterAsync(fx, api, cluster, ct, "east1", "east2");
            (await api.PutAsJsonAsync($"/api/s3/clusters/{cluster}/links",
                new { links = new[] { new { a = "east1", b = "east2" } } }, ct))
                .StatusCode.Should().Be(HttpStatusCode.Accepted);
            await S3E2eScaleTests.WaitClusterActiveAsync(fx, cluster, ct, "east1", "east2");

            var ports = Json(await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3worker/portalloc/{cluster}", ct));
            var objectKey = $"sync/{Guid.NewGuid():N}.txt";
            using (var s3 = S3Client(PortOf(ports, "east1")))
                await s3.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = S3E2eEnvironment.BucketName,
                    Key = objectKey,
                    ContentBody = "sync-e2e",
                }, ct);
            await fx.WaitPhaseAsync("replicated", async () =>
            {
                try
                {
                    using var s3 = S3Client(PortOf(ports, "east2"));
                    await s3.GetObjectAsync(new GetObjectRequest
                    {
                        BucketName = S3E2eEnvironment.BucketName,
                        Key = objectKey,
                    }, ct);
                    return true;
                }
                catch (AmazonS3Exception)
                {
                    return false;
                }
            }, TimeSpan.FromSeconds(150), ct);

            // Act: прогоны по команде до штатного исхода (команда идемпотентна;
            // транзиент свёртки под нагрузкой — LAGGING одной пробы, ретрай
            // по spec §3.10). Свежесть checked_unix ≥ tPost отсекает стелые
            // снимки периодических прогонов.
            var tPost = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 5;
            await fx.WaitPhaseAsync("probed", async () =>
            {
                (await PostSyncProbeAsync(api, cluster, "e2e", ct)).Should().Be(HttpStatusCode.Accepted);
                var connectivity = await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3/clusters/{cluster}/connectivity", ct);
                var sync = await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3/clusters/{cluster}/sync", ct);
                return connectivity is not null && sync is not null
                    && long.Parse(Json(connectivity).GetProperty("checked_unix").GetRawText()) >= tPost
                    && long.Parse(Json(sync).GetProperty("checked_unix").GetRawText()) >= tPost
                    && Json(connectivity).GetProperty("state").GetString() == "OK"
                    && Json(sync).GetProperty("state").GetString() == "SYNCED";
            }, TimeSpan.FromSeconds(90), ct);

            // Заявка последнего POST снесена прогоном, который её увидел
            // (тик держателя ~1 c).
            await fx.WaitPhaseAsync("request-cleaned", async () =>
                await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3worker/probes/{cluster}", ct) is null,
                TimeSpan.FromSeconds(30), ct);

            // Assert: connectivity OK с полной матрицей; sync SYNCED 0/0 (без
            // lag-полей); заявка снесена.
            var connectivity = Json(await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3/clusters/{cluster}/connectivity", ct));
            connectivity.GetProperty("state").GetString().Should().Be("OK");
            connectivity.GetProperty("links").GetProperty("east1").GetProperty("east2").GetString().Should().Be("ok");
            connectivity.GetProperty("links").GetProperty("east2").GetProperty("east1").GetString().Should().Be("ok");
            var sync = Json(await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3/clusters/{cluster}/sync", ct));
            sync.GetProperty("state").GetString().Should().Be("SYNCED");
            var pairs = sync.GetProperty("pairs");
            pairs.GetArrayLength().Should().Be(1);
            pairs[0].GetProperty("from").GetString().Should().Be("east1");
            pairs[0].GetProperty("to").GetString().Should().Be("east2");
            pairs[0].GetProperty("missing").GetInt64().Should().Be(0);
            pairs[0].GetProperty("extra").GetInt64().Should().Be(0);
            pairs[0].TryGetProperty("lag_bytes", out _).Should().BeFalse("steady-state sync without lag");
            (await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3worker/probes/{cluster}", ct))
                .Should().BeNull("request removed by the run");

            await S3E2eScaleTests.DeleteClusterAndWaitCleanAsync(fx, api, cluster, ct, "east1", "east2");
        }
        catch
        {
            fx.MarkFailed();
            throw;
        }
    }

    [Fact]
    public async Task OnCommand_RequestRunDelete_IdempotentRepeat()
    {
        // Arrange: кластер с линком; первый прогон уже был (ключи с T1).
        if (Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") != "1")
        {
            Assert.Skip("PGW_TEST_DOCKER=1 not set — docker E2E skipped");
            return;
        }

        await using var fx = await S3E2eEnvironment.StartAsync("sync-command");
        var cluster = $"cmd{fx.ClusterTag}";
        using var api = fx.CreateApiHttpClient();
        var ct = TestContext.Current.CancellationToken;

        try
        {
            await S3E2eScaleTests.WaitWorkerAsync(fx, api, ct);
            await S3E2eScaleTests.CreateClusterAsync(fx, api, cluster, ct, "east1", "east2");
            (await api.PutAsJsonAsync($"/api/s3/clusters/{cluster}/links",
                new { links = new[] { new { a = "east1", b = "east2" } } }, ct))
                .StatusCode.Should().Be(HttpStatusCode.Accepted);
            await S3E2eScaleTests.WaitClusterActiveAsync(fx, cluster, ct, "east1", "east2");

            // Первый прогон (по команде) — ключи с checked_unix T1.
            (await PostSyncProbeAsync(api, cluster, "e2e-first", ct)).Should().Be(HttpStatusCode.Accepted);
            await fx.WaitPhaseAsync("probed1", async () =>
                await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3/clusters/{cluster}/connectivity", ct) is not null,
                TimeSpan.FromSeconds(60), ct);
            var t1 = long.Parse(Json(await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3/clusters/{cluster}/connectivity", ct))
                .GetProperty("checked_unix").GetRawText());

            // Act: два POST подряд при живой ветке (первый ставит заявку, второй
            // — already → 202 идемпотентно).
            (await PostSyncProbeAsync(api, cluster, "e2e-1", ct)).Should().Be(HttpStatusCode.Accepted);
            (await PostSyncProbeAsync(api, cluster, "e2e-2", ct)).Should().Be(HttpStatusCode.Accepted);

            // Assert: ключи обновились (checked_unix обоих > T1), заявка снесена
            // (requested_by первого — интеграционный тест SyncProbeApiTests).
            await fx.WaitPhaseAsync("probed2", async () =>
            {
                var connectivity = await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3/clusters/{cluster}/connectivity", ct);
                var sync = await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3/clusters/{cluster}/sync", ct);
                return connectivity is not null && sync is not null
                    && long.Parse(Json(connectivity).GetProperty("checked_unix").GetRawText()) > t1
                    && long.Parse(Json(sync).GetProperty("checked_unix").GetRawText()) > t1;
            }, TimeSpan.FromSeconds(60), ct);
            (await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3worker/probes/{cluster}", ct)).Should().BeNull();

            await S3E2eScaleTests.DeleteClusterAndWaitCleanAsync(fx, api, cluster, ct, "east1", "east2");
        }
        catch
        {
            fx.MarkFailed();
            throw;
        }
    }

    [Fact]
    public async Task UnreachableNode_NoRow_PairNotVerified()
    {
        // Arrange: живой кластер с линком и доставленным объектом.
        if (Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") != "1")
        {
            Assert.Skip("PGW_TEST_DOCKER=1 not set — docker E2E skipped");
            return;
        }

        await using var fx = await S3E2eEnvironment.StartAsync("sync-dead");
        var cluster = $"dead{fx.ClusterTag}";
        using var api = fx.CreateApiHttpClient();
        var ct = TestContext.Current.CancellationToken;

        try
        {
            await S3E2eScaleTests.WaitWorkerAsync(fx, api, ct);
            await S3E2eScaleTests.CreateClusterAsync(fx, api, cluster, ct, "east1", "east2");
            (await api.PutAsJsonAsync($"/api/s3/clusters/{cluster}/links",
                new { links = new[] { new { a = "east1", b = "east2" } } }, ct))
                .StatusCode.Should().Be(HttpStatusCode.Accepted);
            await S3E2eScaleTests.WaitClusterActiveAsync(fx, cluster, ct, "east1", "east2");
            var ports = Json(await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3worker/portalloc/{cluster}", ct));
            var objectKey = $"dead/{Guid.NewGuid():N}.txt";
            using (var s3 = S3Client(PortOf(ports, "east1")))
                await s3.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = S3E2eEnvironment.BucketName,
                    Key = objectKey,
                    ContentBody = "dead-e2e",
                }, ct);
            await fx.WaitPhaseAsync("replicated", async () =>
            {
                try
                {
                    using var s3 = S3Client(PortOf(ports, "east2"));
                    await s3.GetObjectAsync(new GetObjectRequest
                    {
                        BucketName = S3E2eEnvironment.BucketName,
                        Key = objectKey,
                    }, ct);
                    return true;
                }
                catch (AmazonS3Exception)
                {
                    return false;
                }
            }, TimeSpan.FromSeconds(150), ct);

            // Act 1: east2 молчит (pause — надзор доводит ОСТАНОВЛЕННЫЙ контейнер
            // стартом, UNREACHABLE не наступит; канон — SupervisionTests)
            // → прогон по команде.
            await fx.RunDockerAsync(["pause", $"s3w-{cluster}-east2"], ct);
            (await PostSyncProbeAsync(api, cluster, "e2e", ct)).Should().Be(HttpStatusCode.Accepted);

            // Assert 1 (до UNREACHABLE): у east1 линк offline (пинг соседа мёртв);
            // строки east2 нет (probe-вызов не отвечает); единственный линк без
            // живых → DOWN по канону arch/23 §3; probe_error называет east2.
            await fx.WaitPhaseAsync("degraded", async () =>
            {
                var connectivity = await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3/clusters/{cluster}/connectivity", ct);
                if (connectivity is null)
                    return false;
                var json = Json(connectivity);
                return json.GetProperty("state").GetString() == "DOWN"
                    && json.GetProperty("links").TryGetProperty("east1", out var row)
                    && row.TryGetProperty("east2", out var link) && link.GetString() == "offline"
                    && !json.GetProperty("links").TryGetProperty("east2", out _);
            }, TimeSpan.FromSeconds(60), ct);
            Json(await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3/clusters/{cluster}/connectivity", ct))
                .GetProperty("probe_error").GetString().Should().Contain("east2");

            // Act 2: надзор C объявляет east2 UNREACHABLE → прогон по команде.
            // Бюджет 300 c: silence-часики стартуют ПЕРВЫМ медленным тиком ветки,
            // маркировка (NodeDeadSec=15 после firstSeen) — вторым; ветка с
            // молчащей нодой длится 45-120 c (supervisor/mesh/probe — по 7.5-с
            // таймауту на грань, без внутренних ретраев — поллинг-инвариант;
            // прогон 3: firstSeen 6741/6781, три ветки к исчерпанию 150 c).
            await fx.WaitPhaseAsync("unreachable", async () =>
                await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3/clusters/{cluster}/nodes/east2/state", ct) == "UNREACHABLE",
                TimeSpan.FromSeconds(300), ct);
            (await PostSyncProbeAsync(api, cluster, "e2e", ct)).Should().Be(HttpStatusCode.Accepted);

            // Assert 2: UNREACHABLE не опрашивается — состав матрицы тот же
            // (targets east1 не меняются от stop, строка east1→east2 offline →
            // DOWN); пара {east1,east2} остаётся (объединение targets), но сверка
            // невозможна — sync-report east2 недоступен → пар в pairs нет,
            // last_error, агрегат LAGGING (канон arch/23 §3: пара существует при
            // записи хотя бы у одной опрошенной стороны).
            await fx.WaitPhaseAsync("uncovered", async () =>
            {
                var sync = await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3/clusters/{cluster}/sync", ct);
                if (sync is null)
                    return false;
                var json = Json(sync);
                return json.GetProperty("state").GetString() == "LAGGING"
                    && json.GetProperty("pairs").GetArrayLength() == 0
                    && json.TryGetProperty("last_error", out _);
            }, TimeSpan.FromSeconds(60), ct);

            await S3E2eScaleTests.DeleteClusterAndWaitCleanAsync(fx, api, cluster, ct, "east1", "east2");
        }
        catch
        {
            fx.MarkFailed();
            throw;
        }
    }

    [Fact]
    public async Task LagCatchUp_LaggingWithQueue_SyncedAfterDelivery()
    {
        // Arrange: кластер с линком; объект в east1 при остановленном east2.
        if (Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") != "1")
        {
            Assert.Skip("PGW_TEST_DOCKER=1 not set — docker E2E skipped");
            return;
        }

        await using var fx = await S3E2eEnvironment.StartAsync("sync-lag");
        var cluster = $"lag{fx.ClusterTag}";
        using var api = fx.CreateApiHttpClient();
        var ct = TestContext.Current.CancellationToken;

        try
        {
            await S3E2eScaleTests.WaitWorkerAsync(fx, api, ct);
            await S3E2eScaleTests.CreateClusterAsync(fx, api, cluster, ct, "east1", "east2");
            (await api.PutAsJsonAsync($"/api/s3/clusters/{cluster}/links",
                new { links = new[] { new { a = "east1", b = "east2" } } }, ct))
                .StatusCode.Should().Be(HttpStatusCode.Accepted);
            await S3E2eScaleTests.WaitClusterActiveAsync(fx, cluster, ct, "east1", "east2");
            var ports = Json(await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3worker/portalloc/{cluster}", ct));

            // Батч объектов при молчащем приёмнике: окно fetch-очереди должно
            // пережить несколько снимков зонда — доставка с персистентными
            // курсорами обгоняет зонд при малых батчах (прогон 2: 5 объектов
            // доставлены до первого пост-командного прогона).
            await fx.RunDockerAsync(["pause", $"s3w-{cluster}-east2"], ct);
            var objectKeys = Enumerable.Range(0, 100)
                .Select(_ => $"lag/{Guid.NewGuid():N}.txt").ToList();
            using (var s3 = S3Client(PortOf(ports, "east1")))
                foreach (var key in objectKeys)
                    await s3.PutObjectAsync(new PutObjectRequest
                    {
                        BucketName = S3E2eEnvironment.BucketName,
                        Key = key,
                        ContentBody = "lag-e2e",
                    }, ct);

            // Act 1: возврат east2 (unpause → health-возврат надзором).
            await fx.RunDockerAsync(["unpause", $"s3w-{cluster}-east2"], ct);
            await fx.WaitPhaseAsync("running", async () =>
                await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3/clusters/{cluster}/nodes/east2/state", ct) == "RUNNING",
                TimeSpan.FromSeconds(115), ct);

            // Act 2: прогоны по команде (каждый — свежий снимок fetch-очереди
            // приёмника) до наблюдения LAGGING с missing>0 и lag_objects>0.
            await fx.WaitPhaseAsync("lagging", async () =>
            {
                (await PostSyncProbeAsync(api, cluster, "e2e", ct)).Should().Be(HttpStatusCode.Accepted);
                var sync = await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3/clusters/{cluster}/sync", ct);
                if (sync is null)
                    return false;
                var json = Json(sync);
                if (json.GetProperty("state").GetString() != "LAGGING"
                    || json.GetProperty("pairs").GetArrayLength() == 0)
                    return false;
                var pair = json.GetProperty("pairs")[0];
                return pair.GetProperty("missing").GetInt64() > 0
                    && pair.TryGetProperty("lag_objects", out var lag) && lag.GetInt64() > 0;
            }, TimeSpan.FromSeconds(60), ct);

            // Act 3: доставка ВСЕГО батча (R4, бюджет 150 c) → прогон → SYNCED.
            await fx.WaitPhaseAsync("delivered", async () =>
            {
                try
                {
                    using var s3 = S3Client(PortOf(ports, "east2"));
                    foreach (var key in objectKeys)
                    {
                        var found = await s3.GetObjectAsync(new GetObjectRequest
                        {
                            BucketName = S3E2eEnvironment.BucketName,
                            Key = key,
                        }, ct);
                        using var reader = new StreamReader(found.ResponseStream);
                        if ((await reader.ReadToEndAsync(ct)) != "lag-e2e")
                            return false;
                    }

                    return true;
                }
                catch (AmazonS3Exception)
                {
                    return false;
                }
            }, TimeSpan.FromSeconds(150), ct);
            await fx.WaitPhaseAsync("synced", async () =>
            {
                (await PostSyncProbeAsync(api, cluster, "e2e", ct)).Should().Be(HttpStatusCode.Accepted);
                var sync = await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3/clusters/{cluster}/sync", ct);
                if (sync is null)
                    return false;
                var json = Json(sync);
                return json.GetProperty("state").GetString() == "SYNCED"
                    && json.GetProperty("pairs").GetArrayLength() == 1
                    && json.GetProperty("pairs")[0].GetProperty("missing").GetInt64() == 0;
            }, TimeSpan.FromSeconds(60), ct);

            // Assert: связность живых нод не рвалась доставкой — матрица ok
            // после финального прогона (см. synced-фазу — прогон уже был).
            var connectivity = Json(await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3/clusters/{cluster}/connectivity", ct));
            connectivity.GetProperty("state").GetString().Should().Be("OK");
            connectivity.GetProperty("links").GetProperty("east1").GetProperty("east2").GetString().Should().Be("ok");
            connectivity.GetProperty("links").GetProperty("east2").GetProperty("east1").GetString().Should().Be("ok");

            await S3E2eScaleTests.DeleteClusterAndWaitCleanAsync(fx, api, cluster, ct, "east1", "east2");
        }
        catch
        {
            fx.MarkFailed();
            throw;
        }
    }

    [Fact]
    public async Task DenyInstance_EmptyMatrixOk_SyncedNoPairs()
    {
        // Arrange: кластер с линком и доставленным объектом; deny east2.
        if (Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") != "1")
        {
            Assert.Skip("PGW_TEST_DOCKER=1 not set — docker E2E skipped");
            return;
        }

        await using var fx = await S3E2eEnvironment.StartAsync("sync-deny");
        var cluster = $"deny{fx.ClusterTag}";
        using var api = fx.CreateApiHttpClient();
        var ct = TestContext.Current.CancellationToken;

        try
        {
            await S3E2eScaleTests.WaitWorkerAsync(fx, api, ct);
            await S3E2eScaleTests.CreateClusterAsync(fx, api, cluster, ct, "east1", "east2");
            (await api.PutAsJsonAsync($"/api/s3/clusters/{cluster}/links",
                new { links = new[] { new { a = "east1", b = "east2" } } }, ct))
                .StatusCode.Should().Be(HttpStatusCode.Accepted);
            await S3E2eScaleTests.WaitClusterActiveAsync(fx, cluster, ct, "east1", "east2");
            var ports = Json(await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3worker/portalloc/{cluster}", ct));
            var objectKey = $"deny/{Guid.NewGuid():N}.txt";
            using (var s3 = S3Client(PortOf(ports, "east1")))
                await s3.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = S3E2eEnvironment.BucketName,
                    Key = objectKey,
                    ContentBody = "deny-e2e",
                }, ct);
            await fx.WaitPhaseAsync("replicated", async () =>
            {
                try
                {
                    using var s3 = S3Client(PortOf(ports, "east2"));
                    await s3.GetObjectAsync(new GetObjectRequest
                    {
                        BucketName = S3E2eEnvironment.BucketName,
                        Key = objectKey,
                    }, ct);
                    return true;
                }
                catch (AmazonS3Exception)
                {
                    return false;
                }
            }, TimeSpan.FromSeconds(150), ct);

            // Act: deny east2 → доводка G сносит связи (202; поллинг статуса east1).
            (await api.PostAsync($"/api/s3/clusters/{cluster}/nodes/east2/deny", null, ct))
                .StatusCode.Should().Be(HttpStatusCode.Accepted);
            await fx.WaitPhaseAsync("isolated", async () =>
            {
                var status = await ReplicationStatusOrNullAsync(PortOf(ports, "east1"), ct);
                return status is not null
                    && !status.Value.GetProperty("neighbors").EnumerateArray().Any();
            }, TimeSpan.FromSeconds(90), ct);

            // Прогон по команде → ключи обновлены.
            (await PostSyncProbeAsync(api, cluster, "e2e", ct)).Should().Be(HttpStatusCode.Accepted);
            await fx.WaitPhaseAsync("probed", async () =>
            {
                var connectivity = await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3/clusters/{cluster}/connectivity", ct);
                var sync = await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3/clusters/{cluster}/sync", ct);
                return connectivity is not null && sync is not null
                    && Json(connectivity).GetProperty("state").GetString() == "OK"
                    && !Json(connectivity).GetProperty("links").EnumerateObject().Any();
            }, TimeSpan.FromSeconds(60), ct);

            // Assert: пустая матрица = OK (отсутствие репликации — не дефект
            // зонда); sync SYNCED без пар.
            var connectivity = Json(await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3/clusters/{cluster}/connectivity", ct));
            connectivity.GetProperty("state").GetString().Should().Be("OK");
            connectivity.GetProperty("links").EnumerateObject().Should().BeEmpty();
            var sync = Json(await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3/clusters/{cluster}/sync", ct));
            sync.GetProperty("state").GetString().Should().Be("SYNCED");
            sync.GetProperty("pairs").GetArrayLength().Should().Be(0);

            await S3E2eScaleTests.DeleteClusterAndWaitCleanAsync(fx, api, cluster, ct, "east1", "east2");
        }
        catch
        {
            fx.MarkFailed();
            throw;
        }
    }

    // ── Хелперы сценария ──

    // S3-клиент по хост-порту из portalloc (паттерн MeshTests — приватная копия).
    private static IAmazonS3 S3Client(int port) => new AmazonS3Client(
        new Amazon.Runtime.BasicAWSCredentials(S3E2eEnvironment.RootUser, S3E2eEnvironment.RootPassword),
        new AmazonS3Config
        {
            ServiceURL = $"http://localhost:{port}",
            ForcePathStyle = true,
            Timeout = TimeSpan.FromSeconds(10),
            MaxErrorRetry = 1,
        });

    private static int PortOf(JsonElement alloc, string instance)
        => alloc.GetProperty(instance).GetProperty("s3").GetInt32();

    private static JsonElement Json(string? json) =>
        JsonDocument.Parse(json ?? "{}").RootElement.Clone();

    // Прогон по команде (X-Requested-By — источник requested_by заявки).
    private static async Task<HttpStatusCode> PostSyncProbeAsync(
        HttpClient api, string cluster, string requestedBy, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, $"/api/s3/clusters/{cluster}/sync-probe");
        request.Headers.TryAddWithoutValidation("X-Requested-By", requestedBy);
        using var response = await api.SendAsync(request, ct);
        return response.StatusCode;
    }

    // Репликация-статус ownS3 (SigV4 root) — паттерн ReplicationStatusOrNullAsync
    // MeshTests; null при любом отказе (транзиент — поллинг продолжит).
    private static async Task<JsonElement?> ReplicationStatusOrNullAsync(int port, CancellationToken ct)
    {
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, $"http://localhost:{port}/admin/replication/status");
            var signed = S3Worker.Core.OwnS3.SigV4GetSigner.Sign(
                "/admin/replication/status", $"localhost:{port}",
                S3E2eEnvironment.RootUser, S3E2eEnvironment.RootPassword);
            request.Headers.TryAddWithoutValidation("Authorization", signed.Authorization);
            request.Headers.TryAddWithoutValidation("x-amz-date", signed.AmzDate);
            request.Headers.TryAddWithoutValidation("x-amz-content-sha256", signed.ContentSha256);
            using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(7.5) };
            using var response = await client.SendAsync(request, ct);
            if (!response.IsSuccessStatusCode)
                return null;
            return JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct)).RootElement.Clone();
        }
        catch (Exception ex) when (ex is HttpRequestException or OperationCanceledException
            && !ct.IsCancellationRequested)
        {
            return null;
        }
    }
}
