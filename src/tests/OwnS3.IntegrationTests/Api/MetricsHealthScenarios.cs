using System.Net;
using OwnS3.App;

namespace OwnS3.IntegrationTests.Api;

// Метрики, healthz и структурный лог (arch/18 + arch/owns3/05 §5 + spec §3.4 п.8).
[Collection(OwnS3TestCollection.Name)]
public sealed class MetricsHealthScenarios(OwnS3AppFactory factory)
{
    private OwnS3TestClient NewClient() => new(factory.CreateClient());

    [Fact]
    public async Task Healthz_Returns200_WithoutSignature()
    {
        // Arrange / Act
        var response = await factory.CreateClient().GetAsync("/healthz", TestContext.Current.CancellationToken);

        // Assert: служебный путь — вне S3-конвейера (шаг 0)
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task Metrics_Endpoint_ContainsOperationSeries()
    {
        // Arrange: несколько запросов GetObject (500) + GetBucketLocation (200)
        var client = NewClient();
        await client.SendSignedAsync("GET", "/bucket/key");
        await client.SendSignedAsync("GET", "/bucket?location");

        // Act: /metrics не требует подписи
        var response = await factory.CreateClient().GetAsync("/metrics", TestContext.Current.CancellationToken);
        var metrics = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

        // Assert: финальные имена серий с operation-метками (факт-форма лейблов OTel)
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        metrics.Should().Contain("ownS3_requests_total");
        metrics.Should().Contain("operation=\"GetObject\"");
        metrics.Should().Contain("code=\"500\"");
        metrics.Should().Contain("ownS3_request_duration_seconds");
        metrics.Should().Contain("operation=\"GetBucketLocation\"");
        metrics.Should().Contain("code=\"200\"");
    }

    [Fact]
    public async Task Metrics_SeriesPresentAfterRepeatedRequests()
    {
        // Arrange: два запроса GetObject; OTel Prometheus-экспортёр отдаёт
        // снапшоты периодического reader'а — мгновенного роста между scrape
        // in-memory хоста нет, фиксируется присутствие серий и фактическая
        // форма лейблов (прецедент Shared.Metrics.UnitTests — Contain-ассерты).
        var client = NewClient();
        var metricsClient = factory.CreateClient();

        // Act
        await client.SendSignedAsync("GET", "/bucket/key");
        var first = await metricsClient.GetStringAsync("/metrics", TestContext.Current.CancellationToken);
        await client.SendSignedAsync("GET", "/bucket/key");
        var second = await metricsClient.GetStringAsync("/metrics", TestContext.Current.CancellationToken);

        // Assert: серия GetObject/500 присутствует в обоих scrape
        foreach (var body in new[] { first, second })
        {
            body.Should().Contain("ownS3_requests_total{otel_scope_name=\"ownS3\",code=\"500\",operation=\"GetObject\"}");
        }
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
        message.Should().Contain("status=500");
        message.Should().Contain("durationMs=");
    }
}
