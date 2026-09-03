using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Core;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// GET /api/clusters/{c}: поле work — журнал /pgworker/work/<C> (t07, спека
// §4.3): последняя запись процесса воркера; null при отсутствии журнала.
[Collection("api")]
public class ClusterDetailsWorkApiTests(AuthWebFactory factory)
{
    [Fact]
    public async Task ClusterDetails_WorkJournal_MappedToDto()
    {
        // Arrange — кластерный снапшот + одна запись work-журнала
        factory.Snapshot = InspectionSnapshots.Clustered(
                factory.Time.GetUtcNow(), factory.Time.GetUtcNow())
            with
        {
            PgWorkerWork =
            [
                new WorkJournalInfo("demo", "rollback", "rejected", "i-1",
                    factory.Time.GetUtcNow().ToUnixTimeSeconds() - 30,
                    "нет обратной подписки bucket_0 — откат только полным re-copy",
                    null, null, null),
            ],
        };
        using var client = await ApiTestLogin.LoginAsync(factory);

        // Act
        using var response = await client.GetAsync("/api/clusters/demo",
            TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        dto.GetProperty("work").ValueKind.Should().Be(JsonValueKind.Object);
        dto.GetProperty("work").GetProperty("op").GetString().Should().Be("rollback");
        dto.GetProperty("work").GetProperty("phase").GetString().Should().Be("rejected");
        dto.GetProperty("work").GetProperty("lastError").GetString().Should().Contain("re-copy");
    }

    [Fact]
    public async Task ClusterDetails_NoWorkJournal_WorkNull()
    {
        // Arrange — снапшот без записей work
        factory.Snapshot = InspectionSnapshots.Clustered(
            factory.Time.GetUtcNow(), factory.Time.GetUtcNow());
        using var client = await ApiTestLogin.LoginAsync(factory);

        // Act
        using var response = await client.GetAsync("/api/clusters/demo",
            TestContext.Current.CancellationToken);

        // Assert
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        dto.GetProperty("work").ValueKind.Should().Be(JsonValueKind.Null);
    }
}
