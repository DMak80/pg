using System.Net;
using AdminPanel.Probes.S3;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Health-проба MinioS3Client на ownS3-контур (t41, arch/adminpanel/02 §2.5):
// /minio/health/live 404 → фолбэк GET /healthz (канон ownS3); cluster 404 →
// null («эндпоинт не поддерживается»). S3-API-часть (ListBuckets) не предмет
// кейсов — endpoint фиктивный (127.0.0.1:9, connection refused), apiOk
// закономерно false; HTTP-пробы обслуживает фейковая IHttpClientFactory.
public class MinioS3ClientHealthTests
{
    // Фейк-хендлер: absolutePath → (статус, тело); пути вне мапы — 404
    // (семантика «пути нет» для проверки, что фолбэк не вызывается зря).
    private sealed class FakeS3Handler(Dictionary<string, (HttpStatusCode Status, string Body)> routes)
        : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
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

    // AAA: ownS3-контур — MinIO-специфичных путей нет (404), канонический
    // /healthz отвечает 200 → live доказан фолбэком, cluster вне MinIO null.
    [Fact]
    public async Task Health_OwnS3_LiveFallback200_ClusterNull()
    {
        // Arrange — только /healthz жив; /minio/health/* отсутствуют (404)
        var handler = new FakeS3Handler(new Dictionary<string, (HttpStatusCode, string)>
        {
            ["/healthz"] = (HttpStatusCode.OK, ""),
        });

        // Act
        var health = await Client(handler).GetHealthAsync(CancellationToken.None);

        // Assert — liveOk через фолбэк; clusterOk null; drives нет
        health.LiveOk.Should().BeTrue();
        health.ClusterOk.Should().BeNull();
        health.Drives.Should().BeNull();
    }

    // AAA: MinIO-контур — оба эндпоинта 200 (+drives-тело): прежнее
    // поведение, фолбэк не срабатывает (мапа не содержит /healthz — его
    // ошибочный вызов дал бы 404 и сломал бы liveOk).
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
        // Arrange — /minio/health/live нет (404), /healthz отвечает 503
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
