using System.Text.Json;
using AdminPanel.Api.Operations;
using AdminPanel.Etcd.Workers;
using AdminPanel.Infrastructure;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests.Operations;

// Прокси-хендлеры pg-мутаций (task etcd-via-worker-api): панель не пишет в
// etcd — команды уходят в API PgWorker; ответы/ошибки проксируются 1:1.
public class WorkerProxyCommandTests
{
    // Стаб шлюза: помнит вызовы, отвечает заготовленно/бросает исключение.
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
    }

    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    [Fact]
    public async Task CreateCluster_201WithBody_ReturnsDto()
    {
        // Arrange
        var api = new StubWorkerApi
        {
            Respond = _ => new WorkerApiResult(201,
                """{"name":"smoke","dbName":"smoke","sharded":true,"bucketsCount":4,"shardsTotal":2,"replicas":2,"requestCpu":"0.5","requestMem":"8Gi","requestDisk":"100Gi","state":"NOT_INITIALIZED"}"""),
        };
        var handler = new CreateClusterCommandHandler(api);
        var command = new CreateClusterCommand(new CreateClusterRequest("smoke", 4, 2, 2, 0.5m, 8, 100));

        // Act
        var result = await handler.Handle(command, CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Name.Should().Be("smoke");
        result.Value.State.Should().Be("NOT_INITIALIZED");
        api.Calls.Should().ContainSingle().Which.Should().Match<StubWorkerApi.Call>(c =>
            c.Worker == "pgworker" && c.Method == HttpMethod.Post && c.Path == "/api/clusters"
            && c.RequestedBy == null); // у create нет оператора — заголовок не шлётся
    }

    [Fact]
    public async Task CreateCluster_409ProblemDetails_FailedWithStatus()
    {
        // Arrange
        var api = new StubWorkerApi
        {
            Respond = _ => new WorkerApiResult(409,
                """{"title":"Cluster already exists","status":409,"detail":"кластер smoke уже существует"}"""),
        };
        var handler = new CreateClusterCommandHandler(api);

        // Act
        var result = await handler.Handle(
            new CreateClusterCommand(new CreateClusterRequest("smoke", 4, 2, 2, 0.5m, 8, 100)),
            CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        var problem = result.Error.Should().BeOfType<WorkerProblemDetails>().Subject;
        problem.StatusCode.Should().Be(409);
        problem.Body.Should().Contain("Cluster already exists");
    }

    [Fact]
    public async Task CreateCluster_400WithErrorsArray_FailedWithStatus()
    {
        // Arrange — errors-массив приходит от воркера уже в каноническом виде
        var api = new StubWorkerApi
        {
            Respond = _ => new WorkerApiResult(400,
                """{"title":"Validation failed","status":400,"detail":"параметры некорректны","errors":{"buckets":["бакеты: целое 1..8192"]}}"""),
        };
        var handler = new CreateClusterCommandHandler(api);

        // Act
        var result = await handler.Handle(
            new CreateClusterCommand(new CreateClusterRequest("smoke", 0, 2, 2, 0.5m, 8, 100)),
            CancellationToken.None);

        // Assert
        var problem = result.Error.Should().BeOfType<WorkerProblemDetails>().Subject;
        problem.StatusCode.Should().Be(400);
        problem.Body.Should().Contain("\"errors\"");
    }

    [Fact]
    public async Task GatewayUnavailable_FailedWithUnavailableException()
    {
        // Arrange — живых ключей/URL нет → модуль панели ответит 503
        var api = new StubWorkerApi { Throw = new WorkerApiUnavailableException("pgworker") };
        var handler = new CreateClusterCommandHandler(api);

        // Act
        var result = await handler.Handle(
            new CreateClusterCommand(new CreateClusterRequest("smoke", 4, 2, 2, 0.5m, 8, 100)),
            CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<WorkerApiUnavailableException>();
    }

    [Fact]
    public async Task MoveBuckets_SendsOperatorIdentityAndBody()
    {
        // Arrange — панель строит команду с оператором сессии; шлюз шлёт
        // X-Requested-By (воркер пишет в requested_by заявок, spec §3.7)
        var api = new StubWorkerApi
        {
            Respond = _ => new WorkerApiResult(201,
                """{"cluster":"demo","from":"shard1","to":"shard2","queued":[3,7],"skipped":[]}"""),
        };
        var handler = new MoveBucketsCommandHandler(api);
        var command = new MoveBucketsCommand("demo", "shard1", "shard2", [3, 7], "opsuser");

        // Act
        var result = await handler.Handle(command, CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Queued.Should().Equal(3, 7);
        api.Calls.Should().ContainSingle().Which.Should().Match<StubWorkerApi.Call>(c =>
            c.Worker == "pgworker" && c.Method == HttpMethod.Post && c.Path == "/api/clusters/demo/moves"
            && c.RequestedBy == "opsuser");
    }

    [Fact]
    public async Task RotateAppPassword_SendsOperatorIdentity()
    {
        // Arrange
        var api = new StubWorkerApi
        {
            Respond = _ => new WorkerApiResult(201,
                """{"cluster":"demo","requestedUnix":1756000000,"requestedBy":"opsuser"}"""),
        };
        var handler = new RotateAppPasswordCommandHandler(api);

        // Act
        var result = await handler.Handle(
            new RotateAppPasswordCommand("demo", "opsuser"), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.RequestedBy.Should().Be("opsuser");
        api.Calls.Should().ContainSingle().Which.Should().Match<StubWorkerApi.Call>(c =>
            c.Path == "/api/clusters/demo/app-password/rotate" && c.RequestedBy == "opsuser");
    }

    [Fact]
    public async Task DeleteCluster_204EmptyBody_Success()
    {
        // Arrange — воркер отвечает 204 без тела; DTO модуль не использует
        var api = new StubWorkerApi { Respond = _ => new WorkerApiResult(204, null) };
        var handler = new DeleteClusterCommandHandler(api);

        // Act
        var result = await handler.Handle(new DeleteClusterCommand("demo"), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        api.Calls.Should().ContainSingle().Which.Should().Match<StubWorkerApi.Call>(c =>
            c.Method == HttpMethod.Delete && c.Path == "/api/clusters/demo" && c.RequestedBy == null);
    }

    [Fact]
    public async Task ShardsAndRecreate_SendsNullOperatorAndRightPaths()
    {
        // Arrange — shards/recreate не содержат requested_by: оператор null
        var api = new StubWorkerApi
        {
            Respond = call => call.Method == HttpMethod.Post
                ? new WorkerApiResult(201, """{"cluster":"demo","name":"shard3","replicas":2,"requestCpu":"0.5","requestMem":"8Gi","requestDisk":"100Gi","state":"NOT_INITIALIZED"}""")
                : new WorkerApiResult(204, null),
        };
        var add = new AddShardCommandHandler(api);
        var delete = new DeleteShardCommandHandler(api);
        var recreate = new RecreateNodeCommandHandler(api);

        // Act
        var added = await add.Handle(
            new AddShardCommand("demo", new AddShardRequest(2, 0.5m, 8, 100)), CancellationToken.None);
        var deleted = await delete.Handle(new DeleteShardCommand("demo", "shard3"), CancellationToken.None);
        var recreated = await recreate.Handle(new RecreateNodeCommand("demo-s1", "s1a", "hard"), CancellationToken.None);

        // Assert
        added.IsSuccess.Should().BeTrue();
        added.Value.Name.Should().Be("shard3");
        deleted.IsSuccess.Should().BeTrue();
        recreated.IsSuccess.Should().BeTrue();
        api.Calls.Should().SatisfyRespectively(
            c => (c.Path, c.Method, c.RequestedBy).Should().Be(("/api/clusters/demo/shards", HttpMethod.Post, (string?)null)),
            c => (c.Path, c.Method, c.RequestedBy).Should().Be(("/api/clusters/demo/shards/shard3", HttpMethod.Delete, (string?)null)),
            c => (c.Path, c.Method, c.RequestedBy).Should().Be(("/api/ha/demo-s1/nodes/s1a/recreate", HttpMethod.Post, (string?)null)));
    }
}
