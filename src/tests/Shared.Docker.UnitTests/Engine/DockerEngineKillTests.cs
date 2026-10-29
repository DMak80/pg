using System.Net;
using System.Text;
using FluentAssertions;
using Shared.Docker;
using Xunit;

namespace Shared.Docker.UnitTests.Engine;

// KillContainerAsync — жёсткая остановка spilo-нод (arch/14 §6/§5 D E3):
// SIGKILL, exited-факт немедленный — поллинга нет; 404 = успех (идемпотентность);
// транспортный отказ/ошибка демона — Result.Failed, не исключение.
public class DockerEngineKillTests
{
    private sealed class FakeHandler(Func<HttpRequestMessage, HttpResponseMessage> responder) : HttpMessageHandler
    {
        public readonly List<(string Method, string Url)> Requests = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Requests.Add((request.Method.Method, request.RequestUri!.PathAndQuery));
            return Task.FromResult(responder(request));
        }
    }

    private static HttpResponseMessage Json(string body, HttpStatusCode code = HttpStatusCode.OK) => new()
    {
        StatusCode = code,
        Content = new StringContent(body, Encoding.UTF8, "application/json"),
    };

    private static DockerEngine Engine(FakeHandler handler)
        => new(new HttpClient(handler) { BaseAddress = new Uri("http://docker") }, "h1");

    // AAA: kill успешен (204) — единственный POST /containers/{id}/kill
    [Fact]
    public async Task Kill_Success_SendsSinglePost()
    {
        // Arrange
        var handler = new FakeHandler(_ => new HttpResponseMessage(HttpStatusCode.NoContent));

        // Act
        var result = await Engine(handler).KillContainerAsync("pgw-shop-shard1-shard1a", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        handler.Requests.Should().ContainSingle();
        handler.Requests.Single().Method.Should().Be("POST");
        handler.Requests.Single().Url.Should().Be("/v1.44/containers/pgw-shop-shard1-shard1a/kill");
    }

    // AAA: 404 — контейнера нет — успех (идемпотентность kill-пути spilo)
    [Fact]
    public async Task Kill_MissingContainer404_Success()
    {
        // Arrange
        var handler = new FakeHandler(_ => Json("""{"message":"No such container"}""", HttpStatusCode.NotFound));

        // Act
        var result = await Engine(handler).KillContainerAsync("pgw-shop", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
    }

    // AAA: 409 — контейнер не running (уже остановлен) — успех: целевое
    //      состояние kill-пути «не живой» достигнуто (recovery UNREACHABLE:
    //      супервизор kill'ит exited-контейнер)
    [Fact]
    public async Task Kill_NotRunning409_Success()
    {
        // Arrange
        var handler = new FakeHandler(_ => Json(
            """{"message":"cannot kill container: pgw-shop: container is not running"}""", HttpStatusCode.Conflict));

        // Act
        var result = await Engine(handler).KillContainerAsync("pgw-shop", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue("a not-running container is already in the target state");
    }

    // AAA: ошибка демона (500) — Result.Failed, НЕ исключение (контракт
    //      Task<Result> — вызыватели классифицируют исходы по Result)
    [Fact]
    public async Task Kill_DaemonError_FailedNotThrown()
    {
        // Arrange
        var handler = new FakeHandler(_ => Json(
            """{"message":"driver: device is busy"}""", HttpStatusCode.InternalServerError));

        // Act
        var result = await Engine(handler).KillContainerAsync("pgw-shop", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse("a daemon error is a Failed result, not a thrown exception");
        result.Error.Should().BeOfType<DockerHttpException>();
    }
}
