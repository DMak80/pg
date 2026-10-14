using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Amazon.S3;
using Amazon.S3.Model;
using FluentAssertions;
using S3Worker.Core.OwnS3;
using Xunit;

namespace S3Worker.IntegrationTests.E2e;

// Docker-E2E mesh-управления (spec §8.3, arch/24 §5 G): PKI-провижининг +
// репликация по links, переживание переезда (тот же сертификат), deny/allow
// (изоляция доводкой G), частичный граф, 0 инстансов. S3-клиент — AWSSDK
// root-креды профиля по host-портам из portalloc; админ-грань — SigV4 root.
// Все ожидания — поллинг WaitPhaseAsync (бюджет в [PHASE]); гейт PGW_TEST_DOCKER=1.
public class S3E2eMeshTests
{
    [Fact]
    public async Task PkiПровижининг_РепликацияПоЛинкам()
    {
        // Arrange: окружение + кластер 2 инстанса с графом east1↔east2.
        if (Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") != "1")
        {
            Assert.Skip("PGW_TEST_DOCKER=1 не задан — docker-E2E пропущен");
            return;
        }

        await using var fx = await S3E2eEnvironment.StartAsync("mesh-basic");
        var cluster = $"mesh{fx.ClusterTag}";
        using var api = fx.CreateApiHttpClient();
        var ct = TestContext.Current.CancellationToken;

        try
        {
            await S3E2eScaleTests.WaitWorkerAsync(fx, api, ct);
            await S3E2eScaleTests.CreateClusterAsync(fx, api, cluster, ct, "east1", "east2");
            (await api.PutAsJsonAsync($"/api/s3/clusters/{cluster}/links",
                new { links = new[] { new { a = "east1", b = "east2" } } }, ct))
                .StatusCode.Should().Be(HttpStatusCode.Accepted);

            // Обе ноды RUNNING (P2: PKI + env; P4/G: join по links).
            await S3E2eScaleTests.WaitClusterActiveAsync(fx, cluster, ct, "east1", "east2");

            // Assert 1: docker inspect env — репликационный материал задан.
            var env1 = EnvOf(await fx.ContainerEnvJsonAsync($"s3w-{cluster}-east1"));
            env1["OWNS3_REPLICATION_CERT"].Should().Contain("publicKeyPem", "сертификат инстанса значениями");
            env1["OWNS3_REPLICATION_PRIVATE_KEY"].Should().Contain("BEGIN PRIVATE KEY");
            env1["OWNS3_REPLICATION_TRUST_CA"].Should().Contain("publicKeyPem");
            var ports = Json(await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3worker/portalloc/{cluster}", ct));
            var port1 = ports.GetProperty("east1").GetProperty("s3").GetInt32();
            env1["OWNS3_REPLICATION_ADVERTISED_ENDPOINT"]
                .Should().Be($"http://host.docker.internal:{port1}");

            // Assert 2: статус обеих нод — сосед числится (join доводится G тиками
            // после RUNNING — поллинг с бюджетом, не мгновенный факт).
            await fx.WaitPhaseAsync("joined", async () =>
            {
                var s1 = await ReplicationStatusOrNullAsync(fx, PortOf(ports, "east1"), ct);
                var s2 = await ReplicationStatusOrNullAsync(fx, PortOf(ports, "east2"), ct);
                return s1 is not null && NeighborsOf(s1.Value).Contains("east2")
                    && s2 is not null && NeighborsOf(s2.Value).Contains("east1");
            }, TimeSpan.FromSeconds(60), ct);

            // Assert 3: репликация доставлена — объект в east1 появляется на east2.
            var objectKey = $"mesh/{Guid.NewGuid():N}.txt";
            using (var s3 = S3Client(PortOf(ports, "east1")))
                await s3.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = S3E2eEnvironment.BucketName,
                    Key = objectKey,
                    ContentBody = "mesh-e2e",
                }, ct);
            var delivered = await fx.WaitPhaseAsync("replicated", async () =>
            {
                try
                {
                    using var s3 = S3Client(PortOf(ports, "east2"));
                    var found = await s3.GetObjectAsync(new GetObjectRequest
                    {
                        BucketName = S3E2eEnvironment.BucketName,
                        Key = objectKey,
                    }, ct);
                    using var reader = new StreamReader(found.ResponseStream);
                    return (await reader.ReadToEndAsync(ct)) == "mesh-e2e";
                }
                catch (AmazonS3Exception)
                {
                    return false; // ещё не доставлено
                }
            }, TimeSpan.FromSeconds(150), ct);
            delivered.Should().BeGreaterThan(TimeSpan.Zero, "объект доставлен в бюджет");

            await S3E2eScaleTests.DeleteClusterAndWaitCleanAsync(fx, api, cluster, ct, "east1", "east2");
        }
        catch
        {
            fx.MarkFailed();
            throw;
        }
    }

    [Fact]
    public async Task Переезд_СертификатТотЖе_СвязьЖива_ДанныеЖивы()
    {
        // Arrange: кластер с линком и объектом.
        if (Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") != "1")
        {
            Assert.Skip("PGW_TEST_DOCKER=1 не задан — docker-E2E пропущен");
            return;
        }

        await using var fx = await S3E2eEnvironment.StartAsync("mesh-move");
        var cluster = $"move{fx.ClusterTag}";
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
            var objectKey = $"move/{Guid.NewGuid():N}.txt";
            using (var s3 = S3Client(PortOf(ports, "east1")))
                await s3.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = S3E2eEnvironment.BucketName,
                    Key = objectKey,
                    ContentBody = "persist-me",
                }, ct);

            // Снятие сертификата ДО сноса (env живого контейнера).
            var certBefore = EnvOf(await fx.ContainerEnvJsonAsync($"s3w-{cluster}-east1"))["OWNS3_REPLICATION_CERT"];

            // Act: снос контейнера east1 — надзор пересоздаёт (тот же volume).
            await fx.RunDockerAsync(["rm", "-f", $"s3w-{cluster}-east1"], ct);

            // Assert: пересоздан, env-сертификат ИДЕНТИЧЕН (ключи переживают переезд).
            await fx.WaitPhaseAsync("recreated", async () =>
                await fx.ContainerAliveAsync($"s3w-{cluster}-east1")
                && (await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3/clusters/{cluster}/nodes/east1/state", ct)) == "RUNNING",
                TimeSpan.FromSeconds(115), ct);
            var certAfter = EnvOf(await fx.ContainerEnvJsonAsync($"s3w-{cluster}-east1"))["OWNS3_REPLICATION_CERT"];
            certAfter.Should().Be(certBefore, "контейнер пересоздан с тем же сертификатом из etcd");

            // Связь жива (доводка G / курсоры сохранены), объект не потерян.
            var restored = await fx.WaitPhaseAsync("link-restored", async () =>
            {
                var status = await ReplicationStatusOrNullAsync(fx, PortOf(ports, "east1"), ct);
                return status is not null && NeighborsOf(status.Value).Contains("east2");
            }, TimeSpan.FromSeconds(60), ct);
            restored.Should().BeGreaterThan(TimeSpan.Zero, "связь восстановлена доводкой");

            using var s3Again = S3Client(PortOf(ports, "east1"));
            var fetched = await s3Again.GetObjectAsync(new GetObjectRequest
            {
                BucketName = S3E2eEnvironment.BucketName,
                Key = objectKey,
            }, ct);
            using var reader = new StreamReader(fetched.ResponseStream);
            (await reader.ReadToEndAsync(ct)).Should().Be("persist-me", "volume пережил переезд");

            await S3E2eScaleTests.DeleteClusterAndWaitCleanAsync(fx, api, cluster, ct, "east1", "east2");
        }
        catch
        {
            fx.MarkFailed();
            throw;
        }
    }

    [Fact]
    public async Task DenyAllow_Изоляция_ВосстановлениеДоводкой()
    {
        // Arrange: кластер с линком.
        if (Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") != "1")
        {
            Assert.Skip("PGW_TEST_DOCKER=1 не задан — docker-E2E пропущен");
            return;
        }

        await using var fx = await S3E2eEnvironment.StartAsync("mesh-deny");
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

            // Act: deny east2 — отзыв ключа (202; denylist в etcd).
            (await api.PostAsync($"/api/s3/clusters/{cluster}/nodes/east2/deny", null, ct))
                .StatusCode.Should().Be(HttpStatusCode.Accepted);

            // Assert 1: связи снесены доводкой G (записей нет на обеих сторонах),
            // denylist доведён (сертификат east2 отозван: join 403/502).
            await fx.WaitPhaseAsync("deny-applied", async () =>
            {
                var s1 = await ReplicationStatusOrNullAsync(fx, PortOf(ports, "east1"), ct);
                var s2 = await ReplicationStatusOrNullAsync(fx, PortOf(ports, "east2"), ct);
                return s1 is not null && !NeighborsOf(s1.Value).Any()
                    && s2 is not null && !NeighborsOf(s2.Value).Any();
            }, TimeSpan.FromSeconds(60), ct);

            // Ручной admin-join С ОТЗЫВАННОЙ стороны (initiator east2): приёмник
            // east1 проверяет имя/ключ вызывающего по deny-list → 403
            // (owns3/07 §6 — deny по имени ИЛИ ключу вызывающего).
            // Доведение denylist (не тот же факт, что снос связей: deny-перезапись
            // и delete-пара — разные операции тика): ручной join честен после того,
            // как отзыва достоверно на приёмнике.
            await fx.WaitPhaseAsync("denylist-pushed", async () =>
            {
                var d1 = await ReplicationDenyListOrNullAsync(fx, PortOf(ports, "east1"), ct);
                var d2 = await ReplicationDenyListOrNullAsync(fx, PortOf(ports, "east2"), ct);
                return d1 is not null && NamesOf(d1.Value).Contains("east2")
                    && d2 is not null && NamesOf(d2.Value).Contains("east2");
            }, TimeSpan.FromSeconds(30), ct);

            var manualJoin = await AdminJoinAsync(fx, PortOf(ports, "east2"), PortOf(ports, "east1"), ct);
            manualJoin.Should().NotBe(HttpStatusCode.OK,
                "отозванный инстанс не может join (deny-list)");

            // Act 2: allow — снятие отзыва; связь восстанавливается доводкой G.
            (await api.PostAsync($"/api/s3/clusters/{cluster}/nodes/east2/allow", null, ct))
                .StatusCode.Should().Be(HttpStatusCode.Accepted);
            await fx.WaitPhaseAsync("allow-restored", async () =>
            {
                var s1 = await ReplicationStatusOrNullAsync(fx, PortOf(ports, "east1"), ct);
                var s2 = await ReplicationStatusOrNullAsync(fx, PortOf(ports, "east2"), ct);
                return s1 is not null && NeighborsOf(s1.Value).Contains("east2")
                    && s2 is not null && NeighborsOf(s2.Value).Contains("east1");
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
    public async Task ЧастичныйГраф_СоставПоДекларации()
    {
        // Arrange: 3 инстанса, links только {east1,east2} и {east1,west1}.
        if (Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") != "1")
        {
            Assert.Skip("PGW_TEST_DOCKER=1 не задан — docker-E2E пропущен");
            return;
        }

        await using var fx = await S3E2eEnvironment.StartAsync("mesh-partial");
        var cluster = $"part{fx.ClusterTag}";
        using var api = fx.CreateApiHttpClient();
        var ct = TestContext.Current.CancellationToken;

        try
        {
            await S3E2eScaleTests.WaitWorkerAsync(fx, api, ct);
            await S3E2eScaleTests.CreateClusterAsync(fx, api, cluster, ct, "east1", "east2", "west1");
            (await api.PutAsJsonAsync($"/api/s3/clusters/{cluster}/links",
                new
                {
                    links = new[]
                    {
                        new { a = "east1", b = "east2" },
                        new { a = "east1", b = "west1" },
                    },
                }, ct)).StatusCode.Should().Be(HttpStatusCode.Accepted);
            await S3E2eScaleTests.WaitClusterActiveAsync(fx, cluster, ct, "east1", "east2", "west1");

            var ports = Json(await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3worker/portalloc/{cluster}", ct));
            await fx.WaitPhaseAsync("partial-graph", async () =>
            {
                var a = await ReplicationStatusOrNullAsync(fx, PortOf(ports, "east1"), ct);
                var b = await ReplicationStatusOrNullAsync(fx, PortOf(ports, "east2"), ct);
                var c = await ReplicationStatusOrNullAsync(fx, PortOf(ports, "west1"), ct);
                return a is not null && NeighborsOf(a.Value).SetEquals(["east2", "west1"])
                    && b is not null && NeighborsOf(b.Value).SetEquals(["east1"])
                    && c is not null && NeighborsOf(c.Value).SetEquals(["east1"]);
            }, TimeSpan.FromSeconds(90), ct);

            // Это НЕ дефект: full-mesh не инвариант (arch/23 §2) — доводка строит
            // ровно заданный links; состав выше проверен (у east2/west1 нет связи).

            await S3E2eScaleTests.DeleteClusterAndWaitCleanAsync(fx, api, cluster, ct, "east1", "east2", "west1");
        }
        catch
        {
            fx.MarkFailed();
            throw;
        }
    }

    [Fact]
    public async Task НольИнстансов_ДобавлениеИЛинки()
    {
        // Arrange: кластер-декларация без инстансов.
        if (Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") != "1")
        {
            Assert.Skip("PGW_TEST_DOCKER=1 не задан — docker-E2E пропущен");
            return;
        }

        await using var fx = await S3E2eEnvironment.StartAsync("mesh-empty");
        var cluster = $"empt{fx.ClusterTag}";
        using var api = fx.CreateApiHttpClient();
        var ct = TestContext.Current.CancellationToken;

        try
        {
            await S3E2eScaleTests.WaitWorkerAsync(fx, api, ct);
            using (var created = await api.PostAsJsonAsync("/api/s3/clusters",
                new
                {
                    name = cluster,
                    platform = "owns3",
                    secretProfile = S3E2eEnvironment.ProfileName,
                    instances = Array.Empty<object>(),
                    resources = new { cpu = 0.5m, memGi = 1, diskGi = 5 },
                }, ct))
            {
                created.StatusCode.Should().Be(HttpStatusCode.Created);
            }

            // Provisioning 0 инстансов: config nodes=0 без state; endpoints НЕ пишется.
            await fx.WaitPhaseAsync("empty-active", async () =>
            {
                var config = await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3/clusters/{cluster}/config", ct);
                return config is not null && !config.Contains("\"state\"")
                    && config.Contains("\"nodes\":0");
            }, TimeSpan.FromSeconds(30), ct);
            (await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3/clusters/{cluster}/endpoints", ct))
                .Should().BeNull("endpoints существует ⟺ ≥1 инстанс (arch/23 §2)");

            // Act: добавление двух инстансов в живой кластер + links.
            foreach (var name in new[] { "east1", "east2" })
            {
                using var added = await api.PostAsJsonAsync($"/api/s3/clusters/{cluster}/nodes",
                    new { name, placement = new { dockerHost = "local" } }, ct);
                added.StatusCode.Should().Be(HttpStatusCode.Created);
            }

            (await api.PutAsJsonAsync($"/api/s3/clusters/{cluster}/links",
                new { links = new[] { new { a = "east1", b = "east2" } } }, ct))
                .StatusCode.Should().Be(HttpStatusCode.Accepted);
            await S3E2eScaleTests.WaitClusterActiveAsync(fx, cluster, ct, "east1", "east2");

            // join работает: объект реплицируется.
            var ports = Json(await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3worker/portalloc/{cluster}", ct));
            var objectKey = $"empty/{Guid.NewGuid():N}.txt";
            using (var s3 = S3Client(PortOf(ports, "east1")))
                await s3.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = S3E2eEnvironment.BucketName,
                    Key = objectKey,
                    ContentBody = "after-grow",
                }, ct);
            await fx.WaitPhaseAsync("replicated-after-grow", async () =>
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

            await S3E2eScaleTests.DeleteClusterAndWaitCleanAsync(fx, api, cluster, ct, "east1", "east2");
        }
        catch
        {
            fx.MarkFailed();
            throw;
        }
    }

    // ── Хелперы сценария ──

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

    private static HashSet<string> NeighborsOf(JsonElement status) => status
        .GetProperty("neighbors").EnumerateArray()
        .Select(n => n.GetProperty("name").GetString()!)
        .ToHashSet(StringComparer.Ordinal);

    // Админ-грань ownS3 (GET /admin/replication/status) — SigV4 root-креды;
    // advertised-хост host.docker.internal с хоста теста = localhost; null при
    // любом отказе (транзиент — поллинг продолжит).
    private static async Task<JsonElement?> ReplicationStatusOrNullAsync(
        S3E2eEnvironment fx, int port, CancellationToken ct)
    {
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, $"http://localhost:{port}/admin/replication/status");
            var signed = SigV4GetSigner.Sign(
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
        catch (Exception ex) when (ex is HttpRequestException or System.OperationCanceledException
            && !ct.IsCancellationRequested)
        {
            return null;
        }
    }

    // Админ-грань: GET /admin/replication/denylist (SigV4 root) — имена отзывов;
    // null при любом отказе (транзиент — поллинг продолжит, не исключение).
    private static async Task<JsonElement?> ReplicationDenyListOrNullAsync(
        S3E2eEnvironment fx, int port, CancellationToken ct)
    {
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, $"http://localhost:{port}/admin/replication/denylist");
            var signed = SigV4GetSigner.Sign(
                "/admin/replication/denylist", $"localhost:{port}",
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
        catch (Exception ex) when (ex is HttpRequestException or System.OperationCanceledException
            && !ct.IsCancellationRequested)
        {
            return null;
        }
    }

    // Имена отзывов из denylist-ответа ownS3 {"entries":[{name,…}]} (owns3/07 §2).
    private static HashSet<string> NamesOf(JsonElement denyList) => denyList
        .GetProperty("entries").EnumerateArray()
        .Select(e => e.TryGetProperty("name", out var n) ? n.GetString() ?? "" : "")
        .Where(n => n.Length > 0)
        .ToHashSet(StringComparer.Ordinal);

    // Ручной admin-join (SigV4 root): «initiator» — east1, endpoint соседа.
    private static async Task<HttpStatusCode> AdminJoinAsync(
        S3E2eEnvironment fx, int initiatorPort, int neighborPort, CancellationToken ct)
    {
        var body = JsonSerializer.SerializeToUtf8Bytes(new
        {
            endpoint = $"http://host.docker.internal:{neighborPort}",
        });
        using var request = new HttpRequestMessage(HttpMethod.Post, $"http://localhost:{initiatorPort}/admin/replication/join")
        {
            Content = new ByteArrayContent(body),
        };
        var signed = SigV4GetSigner.SignRequest("POST", "/admin/replication/join", $"localhost:{initiatorPort}",
            S3E2eEnvironment.RootUser, S3E2eEnvironment.RootPassword);
        request.Headers.TryAddWithoutValidation("Authorization", signed.Authorization);
        request.Headers.TryAddWithoutValidation("x-amz-date", signed.AmzDate);
        request.Headers.TryAddWithoutValidation("x-amz-content-sha256", signed.ContentSha256);
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
        using var response = await client.SendAsync(request, ct);
        return response.StatusCode;
    }

    private static IReadOnlyDictionary<string, string> EnvOf(string envJson)
    {
        var env = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var item in JsonDocument.Parse(envJson).RootElement.EnumerateArray())
        {
            var raw = item.GetString();
            if (raw is null)
                continue;
            var separator = raw.IndexOf('=');
            if (separator > 0)
                env[raw[..separator]] = raw[(separator + 1)..];
        }

        return env;
    }
}
