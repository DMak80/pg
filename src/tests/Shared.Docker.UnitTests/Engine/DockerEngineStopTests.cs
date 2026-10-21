using System.Net;
using System.Text;
using FluentAssertions;
using Shared.Docker;
using Xunit;

namespace Shared.Docker.UnitTests.Engine;

// StopContainerAsync — поллинг до exited-факта (arch/14 §6): блокирующий
// POST /stop?t=N живого контейнера (graceful-shutdown) длится дольше
// пер-попыточного клиентского таймаута — клиентский обрыв НЕ неудача;
// итерации (GET-статус / повтор идемпотентного POST) в бюджете
// t + один клиентский таймаут; 304/404 = успех (идемпотентность карантина E3).
public class DockerEngineStopTests
{
    // Async-фейк: умеет задержку ответа дольше HttpClient.Timeout (задержки
    // ОТМЕНЯЕМЫЕ — HttpClient.Timeout «мягкий»: отменяет linked-токен, но не
    // прерывает handler, игнорирующий ct; реальный SocketsHttpHandler отмену
    // уважает — фейк обязан вести себя так же).
    private sealed class SlowHandler(
        Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> responder) : HttpMessageHandler
    {
        public readonly List<(string Method, string Url)> Requests = [];

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken ct)
        {
            Requests.Add((request.Method.Method, request.RequestUri!.PathAndQuery));
            return await responder(request, ct);
        }
    }

    private static HttpResponseMessage Json(string body, HttpStatusCode code = HttpStatusCode.OK) => new()
    {
        StatusCode = code,
        Content = new StringContent(body, Encoding.UTF8, "application/json"),
    };

    private static readonly string Running =
        """{"Id":"c1","State":{"Running":true,"ExitCode":null,"StartedAt":"2026-01-01T00:00:00Z"},"Config":{"Hostname":"h"}}""";

    private static readonly string Stopped =
        """{"Id":"c1","State":{"Running":false,"ExitCode":0,"StartedAt":"2026-01-01T00:00:00Z"},"Config":{"Hostname":"h"}}""";

    private static DockerEngine Engine(SlowHandler handler, TimeSpan clientTimeout)
        => new(new HttpClient(handler)
        {
            BaseAddress = new Uri("http://docker"),
            Timeout = clientTimeout,
        }, "h1");

    // AAA: клиентский таймаут первого POST /stop (демон ещё останавливает) →
    //      GET-статус exited → успех, без повторного POST
    [Fact]
    public async Task Stop_ClientTimeoutThenExited_Success()
    {
        // Arrange — stop молчит 5 с (клиентский таймаут 0.5 с), статус уже exited
        var handler = new SlowHandler(async (req, ct) =>
        {
            if (req.RequestUri!.PathAndQuery.Contains("/stop"))
                await Task.Delay(TimeSpan.FromSeconds(5), ct);
            return Json(Stopped);
        });

        // Act
        var result = await Engine(handler, TimeSpan.FromMilliseconds(500))
            .StopContainerAsync("pgw-shop", 10, CancellationToken.None);

        // Assert: POST оборван клиентским таймаутом, GET увидел exited
        result.IsSuccess.Should().BeTrue("the daemon finished the stop after the client timeout");
        handler.Requests.Select(r => r.Url.Split('?')[0]).Should().Equal(
            "/v1.44/containers/pgw-shop/stop",
            "/v1.44/containers/pgw-shop/json");
    }

    // AAA: таймаут POST → статус ещё running → повторный идемпотентный POST
    //      отвечает 204 — контейнер остановлен
    [Fact]
    public async Task Stop_TimeoutThenRetryPost_Success()
    {
        // Arrange — первый stop молчит 5 с, статус running; второй stop — 204
        var stopCalls = 0;
        var handler = new SlowHandler(async (req, ct) =>
        {
            var path = req.RequestUri!.PathAndQuery;
            if (path.Contains("/stop") && Interlocked.Increment(ref stopCalls) == 1)
            {
                await Task.Delay(TimeSpan.FromSeconds(5), ct);
                return Json(Stopped); // не будет прочитан — клиент оборвёт запрос
            }
            if (path.Contains("/stop"))
                return new HttpResponseMessage(HttpStatusCode.NoContent);
            return Json(Running);
        });

        // Act
        var result = await Engine(handler, TimeSpan.FromMilliseconds(500))
            .StopContainerAsync("pgw-shop", 10, CancellationToken.None);

        // Assert: итерация 1 (POST-таймаут + GET running) → итерация 2 (POST 204)
        result.IsSuccess.Should().BeTrue();
        stopCalls.Should().Be(2, "the timed-out POST is retried idempotently");
    }

    // AAA: 304/404 от POST /stop — успех без поллинга (идемпотентность E3)
    [Theory]
    [InlineData(HttpStatusCode.NotModified)]
    [InlineData(HttpStatusCode.NotFound)]
    public async Task Stop_AlreadyStoppedOrMissing_Success(HttpStatusCode code)
    {
        // Arrange
        var handler = new SlowHandler((_, _) =>
            Task.FromResult(Json("""{"message":"already"}""", code)));

        // Act
        var result = await Engine(handler, TimeSpan.FromSeconds(2))
            .StopContainerAsync("pgw-shop", 10, CancellationToken.None);

        // Assert — один вызов, без итераций
        result.IsSuccess.Should().BeTrue();
        handler.Requests.Should().ContainSingle();
    }

    // AAA: бюджет (t + клиентский таймаут) исчерпан без exited-факта — Failed
    [Fact]
    public async Task Stop_BudgetExhausted_Failed()
    {
        // Arrange — каждый POST молчит 5 с, статус всегда running; t=0 →
        // бюджет = один клиентский таймаут (0.5 с) — быстрый отказ
        var handler = new SlowHandler(async (req, ct) =>
        {
            if (req.RequestUri!.PathAndQuery.Contains("/stop"))
                await Task.Delay(TimeSpan.FromSeconds(5), ct);
            return Json(Running);
        });

        // Act
        var result = await Engine(handler, TimeSpan.FromMilliseconds(500))
            .StopContainerAsync("pgw-shop", 0, CancellationToken.None);

        // Assert — exited-факта нет, бюджет вышел: Failed (не вечный цикл)
        result.IsSuccess.Should().BeFalse("no exited fact within the polling budget");
    }

    // AAA: сбой docker-демона (500) — Result.Failed, НЕ исключение (контракт
    //      Task<Result>: вызыватели без обёртки — NodeSupervisor — классифицируют
    //      crashed/failed по Result, исключение из движка недопустимо)
    [Fact]
    public async Task Stop_DaemonError_FailedNotThrown()
    {
        // Arrange — POST /stop отвечает 500 (демон перегружен остановкой)
        var handler = new SlowHandler((_, _) => Task.FromResult(Json(
            """{"message":"driver: device is busy"}""", HttpStatusCode.InternalServerError)));

        // Act
        var result = await Engine(handler, TimeSpan.FromSeconds(2))
            .StopContainerAsync("pgw-shop", 10, CancellationToken.None);

        // Assert — DockerHttpException из SendAsync поймана контрактом метода
        result.IsSuccess.Should().BeFalse("a daemon error is a Failed result, not a thrown exception");
        result.Error.Should().BeOfType<DockerHttpException>();
    }
}
