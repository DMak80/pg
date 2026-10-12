using System.Net;
using System.Text;
using System.Text.Json;
using OwnS3.Replication;
using OwnS3.Replication.Transport;

namespace OwnS3.IntegrationTests.Api;

// Полный словарь метрик репликации (t35, глава 07 §10): экспорт при активной
// репликации; без соседей ни одной ownS3_replication_-серии (Existing-регресс).
public sealed class MetricsReplicationScenarios(ReplicationAppFactory factory) :
    IClassFixture<ReplicationAppFactory>
{
    // Серия «без соседей серий нет» — юнит-тестом MeterListener (детерминированно:
    // Meter("ownS3") процессно-именной, scrape чужого хоста в общем процессе
    // захватывает серии активных фабрик — Existing-регресс проверяется на уровне
    // инструментов, не транспорта).
    [Fact]
    public async Task WithNeighbor_ReplicationSeriesExported()
    {
        // Arrange — сосед задан; клиентская операция кладёт событие (outbox_bytes > 0),
        // приём батча инкрементирует applied_total (точка inbox)
        var client = new OwnS3TestClient(factory.CreateClient());
        await client.SendSignedAsync("PUT", "/met-repl", credentials: OwnS3TestClient.Admin());
        var put = await client.SendSignedAsync("PUT", "/met-repl/k", payloadString: Hash("v"),
            body: "v"u8.ToArray());
        put.StatusCode.Should().Be(HttpStatusCode.OK);
        var batch = JsonSerializer.Serialize(new NotificationBatchRequest("mesh-a",
        [
            new ReplicationEvent(1200, ReplicationEventType.BucketCreate, "met-bucket", "", 100, "", 0, "", "peer"),
        ]), ReplicationTransportJson.Options);
        await client.SendSignedAsync("POST", "/replication/notifications",
            payloadString: Hash(batch), body: Encoding.UTF8.GetBytes(batch),
            credentials: new OwnS3TestClient.Credentials(OwnS3AppFactory.ReplicationAccessKey,
                OwnS3AppFactory.ReplicationSecretKey));

        // Act — scrape до появления ключевых серий (гейт 5 с — читатель OTel периодический)
        var metrics = await ScrapeUntilContainsAsync("ownS3_replication_outbox_bytes",
            "ownS3_replication_queue_depth", "ownS3_replication_applied_total");

        // Assert — словарь главы 07 §10: outbox, окно notifier, исходы применения
        metrics.Should().Contain("ownS3_replication_outbox_bytes");
        metrics.Should().Contain("ownS3_replication_queue_depth");
        metrics.Should().Contain("ownS3_replication_applied_total");
    }

    private static string Hash(string body) =>
        Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(Encoding.UTF8.GetBytes(body)))
            .ToLowerInvariant();

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
