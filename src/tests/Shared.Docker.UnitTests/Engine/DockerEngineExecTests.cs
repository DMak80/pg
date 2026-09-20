using System.Net;
using System.Text;
using FluentAssertions;
using Shared.Docker;
using Xunit;

namespace Shared.Docker.UnitTests.Engine;

// Docker exec (t01 задача 8): POST /containers/{id}/exec → /exec/{id}/start
// (raw-stream демультиплексирование) → /exec/{id}/json.
// Драйверные PlainDriver-кейсы — в PgWorker.UnitTests (домен pg, t07).
public class DockerEngineExecTests
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

    // Фрейм raw-stream: [stream-type,0,0,0, size BE32] + payload.
    private static byte[] Frame(byte type, string payload)
    {
        var body = Encoding.UTF8.GetBytes(payload);
        return
        [
            type, 0, 0, 0,
            (byte)(body.Length >> 24), (byte)(body.Length >> 16), (byte)(body.Length >> 8), (byte)body.Length,
            .. body,
        ];
    }

    private static byte[] Concat(params byte[][] chunks) => [.. chunks.SelectMany(c => c)];

    // Полная exec-цепочка Engine API: create → start (raw-stream) → inspect.
    private static HttpResponseMessage ExecChain(HttpRequestMessage request, byte[] stream, int exitCode)
    {
        var path = request.RequestUri!.PathAndQuery;
        if (request.Method.Method == "POST" && path.Contains("/exec", StringComparison.Ordinal) && path.EndsWith("/start", StringComparison.Ordinal))
        {
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new ByteArrayContent(stream),
            };
        }

        if (request.Method.Method == "GET" && path.Contains("/exec/", StringComparison.Ordinal) && path.EndsWith("/json", StringComparison.Ordinal))
        {
            return Json($"{{\"ExitCode\":{exitCode}}}");
        }

        if (request.Method.Method == "POST" && path.Contains("/exec", StringComparison.Ordinal))
        {
            return Json("""{"Id":"e1"}""", HttpStatusCode.Created);
        }

        return Json("""{"message":"unexpected"}""", HttpStatusCode.BadRequest);
    }

    private static DockerEngine NewEngine(FakeHandler handler)
        => new(new HttpClient(handler) { BaseAddress = new Uri("http://docker") }, "h1");

    // AAA: exec возвращает demultiplexed stdout
    [Fact]
    public async Task ExecAsync_ReturnsStdout()
    {
        // Arrange — stdout-фрейм (type 1) и stderr-фрейм (type 2): в stdout уходит только первый
        var stream = Concat(Frame(0x01, "hello"), Frame(0x02, "warn"));
        var handler = new FakeHandler(req => ExecChain(req, stream, exitCode: 0));
        var engine = NewEngine(handler);

        // Act
        var result = await engine.ExecAsync("cid1", ["pg_dump", "--schema-only"], CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be("hello");
        handler.Requests.Select(r => r.Url).Should().Equal(
            "/v1.44/containers/cid1/exec",
            "/v1.44/exec/e1/start",
            "/v1.44/exec/e1/json");
    }

    // AAA: ненулевой exit — Result.Failed со stderr в сообщении
    [Fact]
    public async Task ExecAsync_NonZeroExit_Fails()
    {
        // Arrange — exit 1 + stderr-фрейм (type 2)
        var stream = Concat(Frame(0x01, "out"), Frame(0x02, "boom: relation not found"));
        var handler = new FakeHandler(req => ExecChain(req, stream, exitCode: 1));
        var engine = NewEngine(handler);

        // Act
        var result = await engine.ExecAsync("cid1", ["pg_dump"], CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse("exit != 0 — команда не выполнилась");
        result.Error!.Message.Should().Contain("exit 1");
        result.Error!.Message.Should().Contain("boom: relation not found");
    }

    // Канон-суперсет t07 §7.3: сообщение ошибки содержит И stderr, И stdout
    // (Kafka-CLI 4.x печатает диагностику, включая stack trace, в stdout).
    [Fact]
    public async Task ExecAsync_NonZeroExit_MessageContainsStdoutAndStderr()
    {
        // Arrange — exit 1 + оба фрейма
        var stream = Concat(Frame(0x01, "stdout diagnostics: trace"), Frame(0x02, "stderr boom"));
        var handler = new FakeHandler(req => ExecChain(req, stream, exitCode: 1));
        var engine = NewEngine(handler);

        // Act
        var result = await engine.ExecAsync("cid1", ["kafka-cli"], CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("stderr boom");
        result.Error!.Message.Should().Contain("stdout diagnostics: trace");
    }
}
