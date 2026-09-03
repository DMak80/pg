using System.Text.Json;
using AdminPanel.Api.Operations;
using AdminPanel.Etcd.Workers;
using AdminPanel.Infrastructure;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests.Operations;

// Прокси-команды move-ops (t07): панель не пишет в etcd — команды уходят в API
// PgWorker; ответы/ошибки проксируются 1:1 (ProblemDetails как есть).
public class MoveOpsProxyCommandTests
{
    // Стаб шлюза: помнит вызовы, отвечает заготовленно (порт WorkerProxyCommandTests).
    private sealed class StubWorkerApi : IWorkerApiGateway
    {
        public sealed record Call(string Worker, HttpMethod Method, string Path, object? Body, string? RequestedBy);

        public List<Call> Calls { get; } = [];

        public Func<Call, WorkerApiResult>? Respond { get; set; }

        public Task<WorkerApiResult> SendAsync(
            string worker, HttpMethod method, string path, object? body, string? requestedBy, CancellationToken ct)
        {
            var call = new Call(worker, method, path, body, requestedBy);
            Calls.Add(call);
            return Task.FromResult(Respond is not null ? Respond(call) : new WorkerApiResult(204, null));
        }
    }

    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    [Fact]
    public async Task Rollback_201_ReturnsDtoAndSendsOperator()
    {
        // Arrange
        var api = new StubWorkerApi
        {
            Respond = _ => new WorkerApiResult(201,
                """{"cluster":"c","queued":[0],"skipped":[]}"""),
        };
        var handler = new RollbackBucketsCommandHandler(api);

        // Act
        var result = await handler.Handle(new RollbackBucketsCommand("c", [0], "admin"), CancellationToken.None);

        // Assert — DTO 1:1 + путь/оператор
        result.IsSuccess.Should().BeTrue();
        result.Value.Queued.Should().Equal(0);
        api.Calls.Should().ContainSingle().Which.Should().Match<StubWorkerApi.Call>(c =>
            c.Worker == "pgworker" && c.Method == HttpMethod.Post
            && c.Path == "/api/clusters/c/moves/rollback" && c.RequestedBy == "admin");
    }

    [Fact]
    public async Task Finalize_409ProblemDetails_FailedWithStatus()
    {
        // Arrange
        var api = new StubWorkerApi
        {
            Respond = _ => new WorkerApiResult(409,
                """{"title":"Move ops rejected","status":409,"detail":"шард c/shard1 — текущий владелец bucket_0, убирать нечего"}"""),
        };
        var handler = new FinalizeBucketCommandHandler(api);

        // Act
        var result = await handler.Handle(
            new FinalizeBucketCommand("c", 0, "shard1", "admin"), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        var problem = result.Error.Should().BeOfType<WorkerProblemDetails>().Subject;
        problem.StatusCode.Should().Be(409);
        problem.Body.Should().Contain("убирать нечего");
    }

    [Fact]
    public async Task Abort_SendsForceOnlyWhenTrue()
    {
        // Arrange — сериализованное тело проверяем через прокси-вызов
        var api = new StubWorkerApi { Respond = _ => new WorkerApiResult(201,
            """{"cluster":"c","bucket":0,"force":true}""") };
        var handler = new AbortBucketCommandHandler(api);

        // Act
        var result = await handler.Handle(new AbortBucketCommand("c", 0, true, "admin"), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Force.Should().BeTrue();
        var body = JsonSerializer.Serialize(api.Calls.Single().Body, Json);
        body.Should().Contain("\"force\":true");
    }

    [Fact]
    public async Task Cancel_204_NoEtcdWrites()
    {
        // Arrange — воркер отвечает 204 без тела (образец delete-мутаций)
        var api = new StubWorkerApi();
        var handler = new CancelMoveTicketCommandHandler(api);

        // Act
        var result = await handler.Handle(new CancelMoveTicketCommand("c", "bucket_0", "admin"), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        api.Calls.Should().ContainSingle().Which.Should().Match<StubWorkerApi.Call>(c =>
            c.Method == HttpMethod.Delete && c.Path == "/api/clusters/c/moves/bucket_0");
    }
}
