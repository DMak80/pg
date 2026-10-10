using System.Net;
using OwnS3.App;

namespace OwnS3.IntegrationTests.Api;

// Метрики, healthz и структурный лог (arch/18 + arch/owns3/05 §5 + spec §3.4 п.8).
// Том на класс (IClassFixture, свой temp-том с teardown фабрикой).
public sealed class MetricsHealthScenarios(OwnS3AppFactory factory) : IClassFixture<OwnS3AppFactory>
{
    private OwnS3TestClient NewClient() => new(factory.CreateClient());

    [Fact]
    public async Task Healthz_Returns200_WithoutSignature()
    {
        // Arrange / Act
        var response = await factory.CreateClient().GetAsync("/healthz", TestContext.Current.CancellationToken);

        // Assert: служебный путь — вне S3-конвейера (шаг 0); том валиден
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task Metrics_ContainsDiskGaugeSeries()
    {
        // Arrange / Act: немедленный проход VolumeCleanupService уже заполнил
        // gauge при старте хоста; серии появляются в scrape ближайшим тиком
        // периодического reader OTel
        var metrics = await ScrapeUntilContainsAsync("ownS3_disk_used_bytes", "ownS3_disk_total_bytes");

        // Assert: каталожные серии диска (канон 05 §5; критерий §10.6)
        metrics.Should().Contain("ownS3_disk_used_bytes");
        metrics.Should().Contain("ownS3_disk_total_bytes");
    }

    [Fact]
    public async Task Metrics_Endpoint_ContainsOperationSeries()
    {
        // Arrange: несколько запросов GetObject (404) + GetBucketLocation (200)
        var client = NewClient();
        await client.SendSignedAsync("GET", "/bucket/key");
        await client.SendSignedAsync("GET", "/bucket?location");

        // Act: /metrics не требует подписи; серии и лейблы появляются в scrape
        // ближайшим тиком периодического reader OTel
        var metrics = await ScrapeUntilContainsAsync(
            "ownS3_requests_total",
            "ownS3_request_duration_seconds",
            "operation=\"GetObject\"",
            "operation=\"GetBucketLocation\"");

        // Assert: финальные имена серий с operation-метками (факт-форма лейблов OTel)
        metrics.Should().Contain("code=\"404\"");
        metrics.Should().Contain("code=\"200\"");
    }

    [Fact]
    public async Task Metrics_SeriesPresentAfterRepeatedRequests()
    {
        // Arrange: два запроса GetObject; фиксируется присутствие серии и
        // фактическая форма лейблов (прецедент Shared.Metrics.UnitTests).
        var client = NewClient();
        var metricsClient = factory.CreateClient();

        // Act
        await client.SendSignedAsync("GET", "/bucket/key");
        await client.SendSignedAsync("GET", "/bucket/key");
        var body = await ScrapeUntilContainsAsync("ownS3_requests_total");

        // Assert: серия GetObject/404 присутствует
        body.Should().Contain("ownS3_requests_total{otel_scope_name=\"ownS3\",code=\"404\",operation=\"GetObject\"}");
    }

    [Fact]
    public async Task ListTypeValueOne_RoutesToListObjectsV1()
    {
        // Arrange: GET /{bucket}?list-type=1 — list-type-дискриминатор СО
        // ЗНАЧЕНИЕМ «2»; иное значение → маршрут v1 (глава 02, референс);
        // серия метрик в общем хосте неоднозначна (V2-запросы соседних
        // сценариев) — операция фиксируется структурным логом этого запроса
        var client = NewClient();

        // Act
        var response = await client.SendSignedAsync("GET", "/bucket?list-type=1");
        var requestId = response.Headers.GetValues("x-amz-request-id").Single();

        // Assert: лог этого requestId — операция ListObjects (v1), не V2
        factory.LogEntries.Should().Contain(e =>
            !string.IsNullOrEmpty(e.Message) && e.Message.Contains(requestId) && e.Message.Contains("operation=ListObjects bucket="));
    }

    [Fact]
    public async Task StructuredLog_ContainsAllSevenFields()
    {
        // Arrange
        var client = NewClient();

        // Act
        var response = await client.SendSignedAsync("GET", "/bucket/key");
        var requestId = response.Headers.GetValues("x-amz-request-id").Single();
        // WAF-хост синхронен — запись лога уже выполнена к моментю ответа
        await Task.Yield();

        // Assert: семь полей spec §3.4 п.8
        var entries = factory.LogEntries.Where(e => e.Message?.Contains(requestId) == true).ToList();
        entries.Should().NotBeEmpty();
        var message = entries[0].Message!;
        message.Should().Contain("operation=GetObject");
        message.Should().Contain("bucket=bucket");
        message.Should().Contain("key=key");
        message.Should().Contain("method=GET");
        message.Should().Contain("status=404");
        message.Should().Contain("durationMs=");
    }

    // Scrape до появления всех паттернов: OTel Prometheus-экспортёр отдаёт
    // снапшоты периодического reader'а — серии попадают в вывод ближайшим тиком.
    // Бюджет 5 с (защита от зависания), шаг 100 мс.
    private async Task<string> ScrapeUntilContainsAsync(params string[] patterns)
    {
        var client = factory.CreateClient();
        var deadline = Environment.TickCount64 + 5_000;
        while (true)
        {
            var body = await client.GetStringAsync("/metrics", TestContext.Current.CancellationToken);
            if (patterns.All(p => body.Contains(p, StringComparison.Ordinal))
                || Environment.TickCount64 >= deadline)
                return body;
            await Task.Delay(100, TestContext.Current.CancellationToken);
        }
    }
}
