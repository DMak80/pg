using System.Net;
using AdminPanel.Probes.S3;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Health-проба MinioS3Client на ownS3-контур (t41, arch/adminpanel/02 §2.5):
// /minio/health/live 404 ИЛИ 403 → фолбэк GET /healthz (403 — канонический
// ответ ownS3 на анонимный запрос вне /healthz и /metrics, arch/owns3/03/05;
// 404 — несуществующий путь MinIO-контура); cluster 404/403 → null. S3-API
// (ListBuckets) не предмет кейсов — endpoint фиктивный (127.0.0.1:9,
// connection refused), apiOk закономерно false; HTTP-пробы обслуживает
// фейковая IHttpClientFactory.
public class MinioS3ClientHealthTests
{
    // Фейк-хендлер: absolutePath → (статус, тело); пути вне мапы — 404;
    // deadEndpoint — каждый запрос падает («нет ответа»). Requests копит
    // запрошенные пути — база ассертов «вторая проба не выполнялась».
    private sealed class FakeS3Handler(
        Dictionary<string, (HttpStatusCode Status, string Body)> routes,
        bool deadEndpoint = false) : HttpMessageHandler
    {
        public List<string> Requests { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Requests.Add(request.RequestUri!.AbsolutePath);
            if (deadEndpoint)
                throw new HttpRequestException("нет ответа");
            var (status, body) = routes.TryGetValue(
                request.RequestUri!.AbsolutePath, out var route)
                ? route
                : (HttpStatusCode.NotFound, "");
            return Task.FromResult(new HttpResponseMessage(status)
            {
                Content = new StringContent(body),
            });
        }
    }

    private sealed class FakeHttpClientFactory(FakeS3Handler handler)
        : IHttpClientFactory
    {
        public HttpClient CreateClient(string name) => new(handler, disposeHandler: false);
    }

    private static MinioS3Client Client(FakeS3Handler handler) => new(
        new MinioOptions
        {
            S3 = new MinioOptions.MinioS3Options
            {
                // Фиктивный endpoint: S3-API недостижим мгновенно
                // (connection refused) — apiOk=false вне предмета кейсов.
                Endpoint = "http://127.0.0.1:9",
                Bucket = "pgworker-backups",
                AccessKey = "k",
                SecretKey = "s",
            },
            TimeoutSec = 1,
        },
        new FakeHttpClientFactory(handler));

    // AAA (a): ownS3-маршрут — анонимная проба /minio/health/live = 403
    // (AccessDenied, arch/owns3/03), канонический /healthz = 200 → live
    // доказан фолбэком; cluster 403 → null (вне MinIO-контура).
    [Fact]
    public async Task Health_OwnS3_403_Fallback200_LiveOk_ClusterNull()
    {
        // Arrange — ownS3-контур: MinIO-пути отвечают 403, /healthz — 200
        var handler = new FakeS3Handler(new Dictionary<string, (HttpStatusCode, string)>
        {
            ["/minio/health/live"] = (HttpStatusCode.Forbidden, ""),
            ["/minio/health/cluster"] = (HttpStatusCode.Forbidden, ""),
            ["/healthz"] = (HttpStatusCode.OK, ""),
        });

        // Act
        var health = await Client(handler).GetHealthAsync(CancellationToken.None);

        // Assert — liveOk через фолбэк; clusterOk null; drives нет; пробы
        // прошли ровно в порядке live → healthz → cluster.
        health.LiveOk.Should().BeTrue();
        health.ClusterOk.Should().BeNull();
        health.Drives.Should().BeNull();
        handler.Requests.Should().Equal(
            "/minio/health/live", "/healthz", "/minio/health/cluster");
    }

    // AAA (b): второй статус-триггер фолбэка — 404 (несуществующий путь)
    // + /healthz 200 → liveOk=true; cluster без маршрута (404) → null.
    [Fact]
    public async Task Health_OwnS3_404_Fallback200_LiveOk_ClusterNull()
    {
        // Arrange — только /healthz жив; live/cluster пути отсутствуют (404)
        var handler = new FakeS3Handler(new Dictionary<string, (HttpStatusCode, string)>
        {
            ["/healthz"] = (HttpStatusCode.OK, ""),
        });

        // Act
        var health = await Client(handler).GetHealthAsync(CancellationToken.None);

        // Assert
        health.LiveOk.Should().BeTrue();
        health.ClusterOk.Should().BeNull();
        health.Drives.Should().BeNull();
    }

    // AAA (c): MinIO-контур — live/cluster 200 (+drives-тело) — прежнее
    // поведение; вторая проба НЕ выполняется (ровно два запроса, /healthz
    // не запрашивался — ошибочный фолбэк поймал бы счётчик).
    [Fact]
    public async Task Health_Minio_Live200_NoFallback_ClusterWithDrives()
    {
        // Arrange — живой MinIO-контур
        var handler = new FakeS3Handler(new Dictionary<string, (HttpStatusCode, string)>
        {
            ["/minio/health/live"] = (HttpStatusCode.OK, ""),
            ["/minio/health/cluster"] = (HttpStatusCode.OK,
                """{"healthyDrives":4,"offlineDrives":0,"healingDrives":0,"totalDrives":4}"""),
        });

        // Act
        var health = await Client(handler).GetHealthAsync(CancellationToken.None);

        // Assert — liveOk без фолбэка; clusterOk true; drives распарсены
        health.LiveOk.Should().BeTrue();
        health.ClusterOk.Should().BeTrue();
        health.Drives.Should().NotBeNull();
        health.Drives!.TotalDrives.Should().Be(4);
        handler.Requests.Should().Equal(
            "/minio/health/live", "/minio/health/cluster");
    }

    // AAA (d): мёртвый endpoint — все пробы без ответа → liveOk=false,
    // clusterOk=null; вторая проба НЕ выполняется (запрошены только live
    // и cluster).
    [Fact]
    public async Task Health_DeadEndpoint_LiveDown_NoSecondProbe()
    {
        // Arrange — транспорт падает на каждый запрос
        var handler = new FakeS3Handler([], deadEndpoint: true);

        // Act
        var health = await Client(handler).GetHealthAsync(CancellationToken.None);

        // Assert
        health.LiveOk.Should().BeFalse();
        health.ClusterOk.Should().BeNull();
        handler.Requests.Should().Equal(
            "/minio/health/live", "/minio/health/cluster");
    }

    // AAA: оба live-пути 404 → liveOk=false (down), clusterOk=null.
    [Fact]
    public async Task Health_BothLivePaths404_LiveDown()
    {
        // Arrange — хранилище отвечает HTTP, но ни один live-путь не 200
        var handler = new FakeS3Handler([]);

        // Act
        var health = await Client(handler).GetHealthAsync(CancellationToken.None);

        // Assert
        health.LiveOk.Should().BeFalse();
        health.ClusterOk.Should().BeNull();
    }

    // AAA: фолбэк отвечает 503 → liveOk=false (down по канону /healthz).
    [Fact]
    public async Task Health_OwnS3_Fallback503_LiveDown()
    {
        // Arrange — live-путь 404, /healthz отвечает 503
        var handler = new FakeS3Handler(new Dictionary<string, (HttpStatusCode, string)>
        {
            ["/healthz"] = (HttpStatusCode.ServiceUnavailable, ""),
        });

        // Act
        var health = await Client(handler).GetHealthAsync(CancellationToken.None);

        // Assert
        health.LiveOk.Should().BeFalse();
        health.ClusterOk.Should().BeNull();
    }
}
