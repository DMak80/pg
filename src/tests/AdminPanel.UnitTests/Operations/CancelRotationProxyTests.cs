using AdminPanel.Api.Operations;
using AdminPanel.Api.Operations.Kafka;
using AdminPanel.Api.Operations.Valkey;
using AdminPanel.Etcd.Workers;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests.Operations;

// Прокси-хендлеры u17 (отмена живых заявок ротаций kafka/valkey) и u-restore:
// успех → 204/202-семантика Result; недоступность API воркера → 503-исключение
// в Error; ProblemDetails воркера — телом ошибки как есть. AAA.
public class CancelRotationProxyTests
{
    // Стаб шлюза (образец WorkerProxyCommandTests.StubWorkerApi).
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
            => throw new NotSupportedException("broadcast не участвует в прокси этих команд");
    }

    [Fact]
    public async Task CancelKafkaRotation_204_NoContentSuccess()
    {
        // Arrange — воркер ответил 204 без тела
        var api = new StubWorkerApi();
        var handler = new CancelKafkaRotationCommandHandler(api);

        // Act
        var result = await handler.Handle(
            new CancelKafkaRotationCommand("events", "app-password/rotate"), CancellationToken.None);

        // Assert — DELETE без requestedBy (заявка не создаётся), путь канонический
        result.IsSuccess.Should().BeTrue();
        api.Calls.Should().ContainSingle().Which.Should().Match<StubWorkerApi.Call>(c =>
            c.Worker == "kafkaworker" && c.Method == HttpMethod.Delete
            && c.Path == "/api/kafka/clusters/events/app-password/rotate" && c.RequestedBy == null);
    }

    [Fact]
    public async Task CancelKafkaRotation_409InProgress_ProblemDetailsAsError()
    {
        // Arrange — начатая ротация: воркер 409 с ProblemDetails
        var api = new StubWorkerApi
        {
            Respond = _ => new WorkerApiResult(409,
                """{"title":"Rotation rejected","status":409,"detail":"rotate rotation of cluster events is already executing"}"""),
        };
        var handler = new CancelKafkaRotationCommandHandler(api);

        // Act
        var result = await handler.Handle(
            new CancelKafkaRotationCommand("events", "ca/rotate"), CancellationToken.None);

        // Assert — ProblemDetails воркера как ошибка прокси (тело как есть,
        // модуль зеркалит его ProblemDetails-ответом панели)
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<WorkerProblemDetails>()
            .Which.StatusCode.Should().Be(409);
        ((WorkerProblemDetails)result.Error!).Body.Should().Contain("already executing");
    }

    [Fact]
    public async Task CancelValkeyRotation_204_Success()
    {
        // Arrange
        var api = new StubWorkerApi();
        var handler = new CancelValkeyRotationCommandHandler(api);

        // Act
        var result = await handler.Handle(
            new CancelValkeyRotationCommand("cache", "password/rotate"), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        api.Calls.Should().ContainSingle().Which.Should().Match<StubWorkerApi.Call>(c =>
            c.Worker == "valkeyworker" && c.Method == HttpMethod.Delete
            && c.Path == "/api/valkey/clusters/cache/password/rotate");
    }

    [Fact]
    public async Task CancelRotation_ApiUnavailable_503Exception()
    {
        // Arrange — живых инстансов нет (WorkerApiUnavailableException шлюза)
        var api = new StubWorkerApi { Throw = new WorkerApiUnavailableException("no live kafkaworker instances") };
        var handler = new CancelKafkaRotationCommandHandler(api);

        // Act
        var result = await handler.Handle(
            new CancelKafkaRotationCommand("events", "admin-password/rotate"), CancellationToken.None);

        // Assert — 503-семантика модуля (Error → 503)
        result.Error.Should().BeOfType<WorkerApiUnavailableException>();
    }

    [Fact]
    public async Task RestoreShard_202WithBody_ReturnsWorkerJson()
    {
        // Arrange — воркер ответил 202 JSON-DTO restore-заявки
        var api = new StubWorkerApi
        {
            Respond = _ => new WorkerApiResult(202,
                """{"id":"r-1","cluster":"demo","shard":"s1","state":"PLANNED","requestedUnix":1761900000}"""),
        };
        var handler = new RestoreShardCommandHandler(api);

        // Act
        var result = await handler.Handle(
            new RestoreShardCommand("demo", "s1", "s1", "2026-01-01T12:00:00Z", "ops"), CancellationToken.None);

        // Assert — тело как есть (JsonElement), requested_by = оператор сессии
        result.IsSuccess.Should().BeTrue();
        result.Value.GetProperty("state").GetString().Should().Be("PLANNED");
        api.Calls.Should().ContainSingle().Which.Should().Match<StubWorkerApi.Call>(c =>
            c.Worker == "pgworker" && c.Method == HttpMethod.Post
            && c.Path == "/api/clusters/demo/shards/s1/restore" && c.RequestedBy == "ops");
    }

    [Fact]
    public async Task RestoreShard_ApiUnavailable_503Exception()
    {
        // Arrange
        var api = new StubWorkerApi { Throw = new WorkerApiUnavailableException("no live pgworker instances") };
        var handler = new RestoreShardCommandHandler(api);

        // Act
        var result = await handler.Handle(
            new RestoreShardCommand("demo", "s1", "s1", null, "ops"), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<WorkerApiUnavailableException>();
    }
}
