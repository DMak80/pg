using System.Text.Json;
using AdminPanel.Api.Operations;
using AdminPanel.Etcd.Workers;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests.Operations;

// Прокси-команды сирот (t04, adminpanel/02 §9.10): панель не пишет в etcd —
// hold/unhold/delete уходят в API PgWorker; ответы/ошибки проксируются 1:1
// (ProblemDetails как есть, 202 — DTO воркера camelCase).
public class OrphansCommandsTests
{
    // Стаб шлюза: помнит вызовы, отвечает заготовленно (порт MoveOpsProxyCommandTests).
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

        public Task<IReadOnlyList<WorkerApiInstanceResult>> SendAllAsync(
            string worker, HttpMethod method, string path, object? body, string? requestedBy, CancellationToken ct)
        {
            var response = Respond is not null ? Respond(new Call(worker, method, path, body, requestedBy)) : new WorkerApiResult(204, null);
            return Task.FromResult<IReadOnlyList<WorkerApiInstanceResult>>([new("stub-instance", response, null)]);
        }
    }

    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    // AAA (AC7): hold → POST /api/backups/orphans/c1/s1/hold, requestedBy передан
    [Fact]
    public async Task Hold_PostsPathAndRequestedBy()
    {
        // Arrange
        var api = new StubWorkerApi();
        var handler = new HoldOrphanCommandHandler(api);

        // Act
        var result = await handler.Handle(new HoldOrphanCommand("c1", "s1", "panel"), CancellationToken.None);

        // Assert — путь/метод/оператор; 204 без тела — DTO default
        result.IsSuccess.Should().BeTrue();
        api.Calls.Should().ContainSingle().Which.Should().Match<StubWorkerApi.Call>(c =>
            c.Worker == "pgworker" && c.Method == HttpMethod.Post
            && c.Path == "/api/backups/orphans/c1/s1/hold"
            && c.RequestedBy == "panel" && c.Body == null);
    }

    // AAA (AC7): unhold → DELETE .../hold, тела нет, requestedBy null (заголовок не шлётся)
    [Fact]
    public async Task Unhold_DeletesPath()
    {
        // Arrange
        var api = new StubWorkerApi();
        var handler = new UnholdOrphanCommandHandler(api);

        // Act
        var result = await handler.Handle(new UnholdOrphanCommand("c1", "s1"), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        api.Calls.Should().ContainSingle().Which.Should().Match<StubWorkerApi.Call>(c =>
            c.Method == HttpMethod.Delete
            && c.Path == "/api/backups/orphans/c1/s1/hold"
            && c.RequestedBy == null && c.Body == null);
    }

    // AAA (AC7): delete → POST .../delete с телом {"confirm":"c1/s1"}; 202 → DTO
    // воркера десериализуется camelCase
    [Fact]
    public async Task Delete_PostsConfirmBody_202Dto()
    {
        // Arrange
        var api = new StubWorkerApi
        {
            Respond = _ => new WorkerApiResult(202,
                """{"prefix":"c1/s1","requestedUnix":1757764800,"requestedBy":"operator"}"""),
        };
        var handler = new DeleteOrphanCommandHandler(api);

        // Act
        var result = await handler.Handle(
            new DeleteOrphanCommand("c1", "s1", "c1/s1", "operator"), CancellationToken.None);

        // Assert — DTO 1:1 + тело confirm + оператор
        result.IsSuccess.Should().BeTrue();
        result.Value.Prefix.Should().Be("c1/s1");
        result.Value.RequestedUnix.Should().Be(1757764800);
        result.Value.RequestedBy.Should().Be("operator");
        var call = api.Calls.Should().ContainSingle().Subject;
        call.Method.Should().Be(HttpMethod.Post);
        call.Path.Should().Be("/api/backups/orphans/c1/s1/delete");
        call.RequestedBy.Should().Be("operator");
        JsonSerializer.Serialize(call.Body!, Json).Should().Contain("""{"confirm":"c1/s1"}""");
    }

    // AAA (AC7): 409 ProblemDetails воркера → WorkerProblemDetails, тело как есть
    [Fact]
    public async Task Delete_409ProblemDetails_ProxiedAsIs()
    {
        // Arrange
        var api = new StubWorkerApi
        {
            Respond = _ => new WorkerApiResult(409,
                """{"title":"Orphan delete rejected","status":409,"detail":"сирота c1/s1 уже в DELETING — удаление идёт, доводку не остановить"}"""),
        };
        var handler = new DeleteOrphanCommandHandler(api);

        // Act
        var result = await handler.Handle(
            new DeleteOrphanCommand("c1", "s1", "c1/s1", "operator"), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        var problem = result.Error.Should().BeOfType<WorkerProblemDetails>().Subject;
        problem.StatusCode.Should().Be(409);
        problem.Body.Should().Contain("DELETING");
    }
}
