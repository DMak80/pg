using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Core;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// GET /api/etcd/domain (adminpanel/02 §2.3.5): домен EtcdWorker из снапшота —
// паттерн InspectionApiTests (TestSnapshotStore общего хоста, без etcd).
[Collection("api")]
public class EtcdDomainApiTests
{
    private readonly AuthWebFactory _factory;

    public EtcdDomainApiTests(AuthWebFactory factory) => _factory = factory;

    private Task<HttpClient> LoginAsync() => ApiTestLogin.LoginAsync(_factory);

    [Fact]
    public async Task Domain_WithSnapshot_ReturnsDto()
    {
        // Arrange — логин до установки снапшота (окно rate-limiter'а), затем
        // снапшот с доменом: декларация 2, узлы 1 RUNNING/local и
        // 2 PROVISIONING/—, статус DR-слепков OK.
        using var client = await LoginAsync();
        _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.Utc) with
        {
            EtcdDomain = new EtcdDomainInfo(2, 1756000000L,
            [
                new ManagedNodeView(1, "RUNNING", "local"),
                new ManagedNodeView(2, "PROVISIONING", null),
            ]),
            EtcdSnapshots = new EtcdSnapshotExportInfo(
                "OK", 1759330000L, "etcd/snapshot-x.db", "abc123", 2048L, 360, null),
        };

        // Act
        using var response = await client.GetAsync("/api/etcd/domain", TestContext.Current.CancellationToken);

        // Assert — 200; декларация/узлы/статус слепков в camelCase-DTO
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        dto.GetProperty("declaredNodes").GetInt32().Should().Be(2);
        dto.GetProperty("createdUnix").GetInt64().Should().Be(1756000000L);
        var nodes = dto.GetProperty("nodes").EnumerateArray().ToList();
        nodes.Should().HaveCount(2);
        nodes[0].GetProperty("k").GetInt32().Should().Be(1);
        nodes[0].GetProperty("state").GetString().Should().Be("RUNNING");
        nodes[0].GetProperty("placedOn").GetString().Should().Be("local");
        nodes[1].GetProperty("k").GetInt32().Should().Be(2);
        nodes[1].GetProperty("state").GetString().Should().Be("PROVISIONING");
        nodes[1].GetProperty("placedOn").ValueKind.Should().Be(JsonValueKind.Null);
        dto.GetProperty("snapshots").GetProperty("state").GetString().Should().Be("OK");
    }

    [Fact]
    public async Task PutEtcdConfig_NodesOutOfRange_400WithoutGateway()
    {
        // Arrange — nodes вне 0..7: панельная валидация (OperationsModule)
        // отвечает 400 ProblemDetails ДО похода в воркер (хост без docker —
        // живых ключей нет; 400 должен прийти именно от гварда панели, а не
        // 503 «живых инстансов нет»).
        using var client = await LoginAsync();

        // Act
        using var response = await client.PutAsJsonAsync("/api/etcd/config", new { nodes = -1 },
            TestContext.Current.CancellationToken);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

        // Assert — 400 с телом ProblemDetails панели
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        body.Should().Contain("Invalid body").And.Contain("nodes must be an integer in 0..7");
    }
}
