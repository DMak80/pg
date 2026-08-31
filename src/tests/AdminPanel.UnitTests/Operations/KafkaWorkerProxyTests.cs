using AdminPanel.Api.Operations;
using AdminPanel.Api.Operations.Kafka;
using AdminPanel.Etcd.Workers;
using AdminPanel.Infrastructure;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests.Operations;

// Прокси-хендлеры kafka-мутаций (task etcd-via-worker-api): панель не пишет в
// etcd — команды уходят в API KafkaWorker; smoke на репрезентативных мутациях
// (остальной контракт покрыт тестами воркера) + идентичность оператора.
public class KafkaWorkerProxyTests
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

    [Fact]
    public async Task CreateCluster_201WithBody_ReturnsDtoAndNullOperator()
    {
        // Arrange
        var api = new StubWorkerApi
        {
            Respond = _ => new WorkerApiResult(201,
                """{"name":"events","state":"NOT_INITIALIZED","brokers":3,"replicationFactor":3,"minInSyncReplicas":2,"defaultPartitions":12,"defaultRetentionMs":604800000,"cpu":"2","memGi":"4Gi","diskGi":"40Gi"}"""),
        };
        var handler = new CreateKafkaClusterCommandHandler(api);
        var command = new CreateKafkaClusterCommand(new CreateKafkaClusterRequest("events"));

        // Act
        var result = await handler.Handle(command, CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Name.Should().Be("events");
        result.Value.State.Should().Be("NOT_INITIALIZED");
        api.Calls.Should().ContainSingle().Which.Should().Match<StubWorkerApi.Call>(c =>
            c.Worker == "kafkaworker" && c.Method == HttpMethod.Post && c.Path == "/api/kafka/clusters"
            && c.RequestedBy == null); // у create кластера нет оператора
    }

    [Fact]
    public async Task CreateCluster_409ProblemDetails_FailedWithStatus()
    {
        // Arrange
        var api = new StubWorkerApi
        {
            Respond = _ => new WorkerApiResult(409,
                """{"title":"Cluster already exists","status":409,"detail":"kafka-кластер events уже существует"}"""),
        };
        var handler = new CreateKafkaClusterCommandHandler(api);

        // Act
        var result = await handler.Handle(
            new CreateKafkaClusterCommand(new CreateKafkaClusterRequest("events")), CancellationToken.None);

        // Assert
        var problem = result.Error.Should().BeOfType<WorkerProblemDetails>().Subject;
        problem.StatusCode.Should().Be(409);
        problem.Body.Should().Contain("Cluster already exists");
    }

    [Fact]
    public async Task GatewayUnavailable_FailedWithUnavailableException()
    {
        // Arrange — живых ключей нет → модуль панели ответит 503
        var api = new StubWorkerApi { Throw = new WorkerApiUnavailableException("kafkaworker") };
        var handler = new RotateKafkaPasswordCommandHandler(api);

        // Act
        var result = await handler.Handle(
            new RotateKafkaPasswordCommand("events", "opsuser"), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<WorkerApiUnavailableException>();
    }

    [Fact]
    public async Task RotatePassword_SendsOperatorIdentity()
    {
        // Arrange — оператор сессии уходит заголовком X-Requested-By (spec §3.7)
        var api = new StubWorkerApi
        {
            Respond = _ => new WorkerApiResult(201,
                """{"cluster":"events","requestedUnix":1756000000,"requestedBy":"opsuser"}"""),
        };
        var handler = new RotateKafkaPasswordCommandHandler(api);

        // Act
        var result = await handler.Handle(
            new RotateKafkaPasswordCommand("events", "opsuser"), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        api.Calls.Should().ContainSingle().Which.Should().Match<StubWorkerApi.Call>(c =>
            c.Method == HttpMethod.Post && c.Path == "/api/kafka/clusters/events/app-password/rotate"
            && c.RequestedBy == "opsuser");
    }

    [Fact]
    public async Task UpsertTopicDesired_SendsOperatorIdentity()
    {
        // Arrange
        var api = new StubWorkerApi
        {
            Respond = _ => new WorkerApiResult(200,
                """{"cluster":"events","topic":"orders","partitions":16,"retentionMs":null,"minInSyncReplicas":null}"""),
        };
        var handler = new UpsertTopicDesiredCommandHandler(api);

        // Act
        var result = await handler.Handle(
            new UpsertTopicDesiredCommand("events", "orders", new TopicDesiredRequest(Partitions: 16), "opsuser"),
            CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Topic.Should().Be("orders");
        api.Calls.Should().ContainSingle().Which.Should().Match<StubWorkerApi.Call>(c =>
            c.Method == HttpMethod.Put && c.Path == "/api/kafka/clusters/events/topics/orders"
            && c.RequestedBy == "opsuser");
    }

    [Fact]
    public async Task CreateTopic_SendsOperatorIdentity()
    {
        // Arrange
        var api = new StubWorkerApi
        {
            Respond = _ => new WorkerApiResult(201,
                """{"cluster":"events","topic":"audit","partitions":12,"replicationFactor":3}"""),
        };
        var handler = new CreateKafkaTopicCommandHandler(api);

        // Act
        var result = await handler.Handle(
            new CreateKafkaTopicCommand("events", new CreateTopicRequest("audit"), "opsuser"),
            CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        api.Calls.Should().ContainSingle().Which.Should().Match<StubWorkerApi.Call>(c =>
            c.Method == HttpMethod.Post && c.Path == "/api/kafka/clusters/events/topics"
            && c.RequestedBy == "opsuser");
    }

    [Fact]
    public async Task Rebalance_PostSendsOperator_DeleteReturns404()
    {
        // Arrange — POST с оператором 201; DELETE отмены: заявки нет → 404
        var api = new StubWorkerApi
        {
            Respond = call => call.Method == HttpMethod.Post
                ? new WorkerApiResult(201,
                    """{"cluster":"events","requestedUnix":1756000000,"requestedBy":"opsuser"}""")
                : new WorkerApiResult(404,
                    """{"title":"Not found","status":404,"detail":"заявка ребалансировки events не найдена"}"""),
        };
        var request = new RequestKafkaRebalanceCommandHandler(api);
        var cancel = new CancelKafkaRebalanceCommandHandler(api);

        // Act
        var posted = await request.Handle(
            new RequestKafkaRebalanceCommand("events", "opsuser"), CancellationToken.None);
        var cancelled = await cancel.Handle(
            new CancelKafkaRebalanceCommand("events"), CancellationToken.None);

        // Assert
        posted.IsSuccess.Should().BeTrue();
        var problem = cancelled.Error.Should().BeOfType<WorkerProblemDetails>().Subject;
        problem.StatusCode.Should().Be(404);
        api.Calls.Should().SatisfyRespectively(
            c => (c.Path, c.RequestedBy).Should().Be(("/api/kafka/clusters/events/rebalance", "opsuser")),
            c => (c.Path, c.Method).Should().Be(("/api/kafka/clusters/events/rebalance", HttpMethod.Delete)));
    }

    [Fact]
    public async Task TopicLifecycle_Cancel204AndClusterMutations_Smoke()
    {
        // Arrange — репрезентативные пути/коды (204 cancel create, 204 delete broker)
        var api = new StubWorkerApi { Respond = _ => new WorkerApiResult(204, null) };
        var cancelCreate = new CancelTopicLifecycleCommandHandler(api);
        var deleteBroker = new RemoveKafkaBrokerCommandHandler(api);
        var deleteCluster = new DeleteKafkaClusterCommandHandler(api);

        // Act
        var cancelled = await cancelCreate.Handle(
            new CancelTopicLifecycleCommand("events", "audit", "create"), CancellationToken.None);
        var broker = await deleteBroker.Handle(
            new RemoveKafkaBrokerCommand("events", "broker2"), CancellationToken.None);
        var cluster = await deleteCluster.Handle(
            new DeleteKafkaClusterCommand("ghost"), CancellationToken.None);

        // Assert
        cancelled.IsSuccess.Should().BeTrue();
        broker.IsSuccess.Should().BeTrue();
        cluster.IsSuccess.Should().BeTrue();
        api.Calls.Select(c => (c.Path, c.RequestedBy)).Should().Equal(
            ("/api/kafka/clusters/events/topics/audit/desired.create", (string?)null),
            ("/api/kafka/clusters/events/brokers/broker2", (string?)null),
            ("/api/kafka/clusters/ghost", (string?)null));
    }
}
