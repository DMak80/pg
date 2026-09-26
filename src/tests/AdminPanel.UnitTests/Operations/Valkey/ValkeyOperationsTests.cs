using System.Text.Json;
using AdminPanel.Api.Inspection;
using AdminPanel.Api.Operations;
using AdminPanel.Api.Operations.Valkey;
using AdminPanel.Core;
using AdminPanel.Core.Valkey;
using AdminPanel.Etcd.Workers;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests.Operations;

// Valkey-мапперы инспекции (arch/03 §8.2) и прокси-команды мутаций
// (arch/02 §11.2): путь/метод/тело к valkeyworker, коды 1:1, X-Requested-By.
public class ValkeyOperationsTests
{
    // Стаб шлюза: помнит вызовы, отвечает заготовленно/бросает исключение
    // (паттерн WorkerProxyCommandTests.StubWorkerApi).
    private sealed class StubWorkerApi : IWorkerApiGateway
    {
        public sealed record Call(string Worker, HttpMethod Method, string Path, object? Body, string? RequestedBy);

        public List<Call> Calls { get; } = [];

        public Func<Call, WorkerApiResult>? Respond { get; set; }

        public Exception? Throw { get; set; }

        public Task<WorkerApiResult> SendAsync(
            string worker, HttpMethod method, string path, object? body, string? requestedBy, CancellationToken ct)
        {
            var call = new Call(worker, method, path, body, requestedBy);
            Calls.Add(call);
            if (Throw is not null)
                throw Throw;
            return Task.FromResult(Respond is not null
                ? Respond(call)
                : new WorkerApiResult(204, null));
        }

        public Task<IReadOnlyList<WorkerApiInstanceResult>> SendAllAsync(
            string worker, HttpMethod method, string path, object? body, string? requestedBy, CancellationToken ct)
            => Task.FromResult<IReadOnlyList<WorkerApiInstanceResult>>([]);
    }

    private static readonly DateTimeOffset Now = new(2026, 9, 17, 12, 0, 0, TimeSpan.Zero);

    // Arrange: снапшот с Active-кластером (нода RUNNING, live=true, ротация app).
    private static ValkeySnapshot SnapshotWithLiveCluster() => new(
        Now, EtcdReachable: true, ConsecutiveFailures: 0,
        [
            new ValkeyClusterInfo(
                "live", ValkeyClusterState.Active, 1, 536870912, "allkeys-lru", 1756500000,
                "host.docker.internal:17001",
                [new ValkeyNodeInfo("node1", "RUNNING", 1m, 1, 10, Live: true, ProbeError: null)],
                new ValkeyRotationTicket("live", "app", 1756500123, "seed")),
        ],
        [], [], [], [], [], [], 0);

    // Act: MapSummaries/MapDetails. Assert: nodesRunning=1, state ACTIVE,
    // rotation.role="app", nodesList[].live=true; креды в DTO отсутствуют (нет полей).
    [Fact]
    public void Mappers_Snapshot_ToDto()
    {
        // Arrange
        var snapshot = SnapshotWithLiveCluster();

        // Act
        var summary = ValkeyMappers.MapSummaries(snapshot).Single();
        var details = ValkeyMappers.MapDetails(snapshot.Clusters[0]);

        // Assert
        summary.Name.Should().Be("live");
        summary.State.Should().Be("ACTIVE");
        summary.NodesRunning.Should().Be(1);
        summary.NodesTotal.Should().Be(1);
        summary.RotationPending.Should().BeTrue();
        summary.MaxmemoryBytes.Should().Be(536870912);
        details.Rotation!.Role.Should().Be("app");
        details.Rotation.RequestedBy.Should().Be("seed");
        details.NodesList.Single().Live.Should().BeTrue();
        // Кредов в DTO нет ни в каком виде (архитектурный запрет, arch/02 §11.1).
        var dtoJson = JsonSerializer.Serialize(details, new JsonSerializerOptions(JsonSerializerDefaults.Web));
        dtoJson.Should().NotContainAny("admin", "password", "app_user", "app_password", "admin_password");
    }

    // Arrange: valkey-снапшота нет (null — до первого тика refresher'а).
    // Act: OverviewMapper.MapValkey(null). Assert: null (сводка отсутствует, фронт
    // показывает «—», не 0 — кластеры не «пропали»).
    [Fact]
    public void MapValkey_NullSnapshot_ReturnsNull()
    {
        // Arrange / Act
        var summary = OverviewMapper.MapValkey(null);

        // Assert
        summary.Should().BeNull();
    }

    // Arrange: снапшот с 1 кластером; Alerts: valkey-node-not-running (critical),
    // valkey-endpoints-missing (critical), worker-api-unreachable (critical,
    // target valkeyworker), valkey-rotation-pending (info).
    // Act: OverviewMapper.MapValkey(snapshot). Assert: ClustersTotal == 1,
    // ClustersCritical == 2 — worker-api-unreachable НЕ считается (сознательное
    // отличие от MapKafka, arch/03 §8.1; зафиксировано тестом).
    [Fact]
    public void MapValkey_CountsOnlyClusterCriticalKinds()
    {
        // Arrange
        var snapshot = SnapshotWithLiveCluster() with
        {
            Alerts =
            [
                new Alert("valkey-node-not-running:live/node1", AlertSeverity.Critical,
                    "valkey-node-not-running", "live/node1", "m", null, null, "", AlertRemedy.WorkerAuto, ""),
                new Alert("valkey-endpoints-missing:other", AlertSeverity.Critical,
                    "valkey-endpoints-missing", "other", "m", null, null, "", AlertRemedy.WorkerAuto, ""),
                new Alert("worker-api-unreachable:valkeyworker", AlertSeverity.Critical,
                    "worker-api-unreachable", "valkeyworker", "m", null, null, "", AlertRemedy.WorkerAuto, ""),
                new Alert("valkey-rotation-pending:live", AlertSeverity.Info,
                    "valkey-rotation-pending", "live", "m", null, null, "", AlertRemedy.WorkerAuto, ""),
            ],
        };

        // Act
        var summary = OverviewMapper.MapValkey(snapshot);

        // Assert
        summary.Should().NotBeNull();
        summary!.ClustersTotal.Should().Be(1);
        summary.ClustersCritical.Should().Be(2);
    }

    // Arrange: стаб IWorkerApiGateway отвечает 200+JSON тела воркера.
    // Act: 5 команд (create/delete/config/resources/rotate). Assert: путь/метод/тело
    // запроса к "valkeyworker" верны (стаб фиксирует), DTO десериализованы, rotate
    // передаёт requestedBy → X-Requested-By.
    [Fact]
    public async Task Commands_ProxyToValkeyWorker_WithExactPaths()
    {
        var api = new StubWorkerApi
        {
            Respond = call => call.Path.Contains("/password/rotate")
                ? new WorkerApiResult(202,
                    """{"cluster":"live","role":"app","requestedUnix":1756500123,"requestedBy":"opsuser"}""")
                : new WorkerApiResult(200, """{"cluster":"live","ok":true}"""),
        };

        // Act: пять прокси-команд.
        await new CreateValkeyClusterCommandHandler(api).Handle(
            new CreateValkeyClusterCommand(new CreateValkeyClusterRequest(
                "live", MaxmemoryBytes: 536870912, MaxmemoryPolicy: "allkeys-lru")),
            CancellationToken.None);
        await new DeleteValkeyClusterCommandHandler(api).Handle(
            new DeleteValkeyClusterCommand("live"), CancellationToken.None);
        await new UpdateValkeyConfigCommandHandler(api).Handle(
            new UpdateValkeyConfigCommand("live", new ValkeyConfigUpdateRequest(MaxmemoryBytes: 134217728)),
            CancellationToken.None);
        await new UpdateValkeyResourcesCommandHandler(api).Handle(
            new UpdateValkeyResourcesCommand("live", "node1", new ValkeyResourcesUpdateRequest(Cpu: 2m, MemGi: 2, DiskGi: 20)),
            CancellationToken.None);
        var rotate = await new RotateValkeyPasswordCommandHandler(api).Handle(
            new RotateValkeyPasswordCommand("live", "app", "opsuser"), CancellationToken.None);

        // Assert: пути/методы/воркер — 1:1 с API воркера (arch/02 §11.2).
        Paths(api).Should().Equal(
            "POST /api/valkey/clusters",
            "DELETE /api/valkey/clusters/live",
            "PUT /api/valkey/clusters/live/config",
            "PUT /api/valkey/clusters/live/nodes/node1/resources",
            "POST /api/valkey/clusters/live/password/rotate");
        api.Calls.Should().OnlyContain(c => c.Worker == "valkeyworker");
        // Тело create ушло в API (серилизованное стабом).
        api.Calls[0].Body.Should().NotBeNull();
        // rotate: 202, оператор — заголовком X-Requested-By (arch/02 §11.2).
        rotate.IsSuccess.Should().BeTrue();
        rotate.Value.RequestedBy.Should().Be("opsuser");
        api.Calls[4].RequestedBy.Should().Be("opsuser");
    }

    // Arrange: стаб кидает WorkerApiUnavailableException. Act: команда.
    // Assert: Result неуспешен (модуль вернёт 503).
    [Fact]
    public async Task Commands_WorkerUnavailable_Fails()
    {
        var api = new StubWorkerApi { Throw = new WorkerApiUnavailableException("valkeyworker") };
        var handler = new CreateValkeyClusterCommandHandler(api);

        // Act
        var result = await handler.Handle(
            new CreateValkeyClusterCommand(new CreateValkeyClusterRequest("live")),
            CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<WorkerApiUnavailableException>();
    }

    // t07 (02 §11.2-6): мутация №6 — заявка ротации CA проксируется в API
    // воркера (POST, тело пустое, оператор — X-Requested-By).
    [Fact]
    public async Task RotateValkeyCa_ProxiesToWorkerApi()
    {
        // Arrange — стаб IWorkerApiGateway: 202 + JSON тела воркера
        var api = new StubWorkerApi
        {
            Respond = _ => new WorkerApiResult(202,
                """{"cluster":"live","requestedUnix":1756500123,"requestedBy":"opsuser"}"""),
        };

        // Act
        var result = await new RotateValkeyCaCommandHandler(api).Handle(
            new RotateValkeyCaCommand("live", "opsuser"), CancellationToken.None);

        // Assert — POST в API воркера, тело пустое, оператор — в заголовке
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var call = Assert.Single(api.Calls);
        call.Method.Should().Be(HttpMethod.Post);
        call.Path.Should().Be("/api/valkey/clusters/live/ca/rotate");
        call.RequestedBy.Should().Be("opsuser");
        call.Body.Should().BeNull();
        result.Value.Cluster.Should().Be("live");
        result.Value.RequestedUnix.Should().Be(1756500123);
        result.Value.RequestedBy.Should().Be("opsuser");
    }

    // «METHOD path» по зафиксированным вызовам стаба.
    private static IReadOnlyList<string> Paths(StubWorkerApi api)
        => [.. api.Calls.Select(c => $"{c.Method.Method} {c.Path}")];
}
