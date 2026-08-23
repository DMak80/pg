using System.Net;
using System.Text;
using PgWorker.Docker.Drivers;
using PgWorker.Docker.Engine;
using PgWorker.Core;
using Xunit;

namespace PgWorker.UnitTests.Docker;

// Docker exec (t01 задача 8): POST /containers/{id}/exec → /exec/{id}/start
// (raw-stream демультиплексирование) → /exec/{id}/json; драйверный резолв
// контейнера ноды по имени pgw-<C>-<X>-<n> на хостах plain-режима.
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

    // AAA: имя контейнера драйвером — pgw-<C>-<X>-<n>, поиск по хостам plain
    [Fact]
    public async Task PlainDriver_ExecNode_ResolvesContainerByPattern()
    {
        // Arrange — контейнер ноды живёт на хосте h2 (h1 его не видит)
        HttpResponseMessage RespondH1(HttpRequestMessage req)
            => req.RequestUri!.PathAndQuery.StartsWith("/v1.44/containers/json", StringComparison.Ordinal)
                ? Json("[]")
                : Json("""{"message":"no such container"}""", HttpStatusCode.NotFound);

        HttpResponseMessage RespondH2(HttpRequestMessage req)
        {
            if (req.RequestUri!.PathAndQuery.StartsWith("/v1.44/containers/json", StringComparison.Ordinal))
            {
                return Json(
                    """
                    [{"Id":"cid-h2","Names":["/pgw-shop-shard1-shard1a"],"Image":"i","State":"running"}]
                    """);
            }

            return ExecChain(req, Frame(0x01, "hi"), exitCode: 0);
        }

        var factory = new SelectiveFactory(new Dictionary<string, HttpMessageHandler>
        {
            ["fake://h1"] = new FakeHandler(RespondH1),
            ["fake://h2"] = new FakeHandler(RespondH2),
        });
        var driver = new PlainClusterDriver(
            [new HostEndpoint("h1", "fake://h1"), new HostEndpoint("h2", "fake://h2")],
            factory, enableDoorman: false);

        // Act
        var result = await driver.ExecNodeAsync("shop", "shard1", "shard1a", ["echo", "hi"], CancellationToken.None);

        // Assert: контейнер найден на h2, exec пошёл в его id
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be("hi");
        var h2 = factory.Handlers["fake://h2"].Should().BeOfType<FakeHandler>().Subject;
        h2.Requests.Should().Contain(r => r.Method == "POST" && r.Url == "/v1.44/containers/cid-h2/exec");
    }

    // AAA: контейнера ноды нет ни на одном хосте — Failed «контейнер не найден»
    [Fact]
    public async Task PlainDriver_ExecNode_NoContainerAnywhere_Fails()
    {
        // Arrange — оба хоста пусты
        var factory = new SelectiveFactory(new Dictionary<string, HttpMessageHandler>
        {
            ["fake://h1"] = new FakeHandler(_ => Json("[]")),
            ["fake://h2"] = new FakeHandler(_ => Json("[]")),
        });
        var driver = new PlainClusterDriver(
            [new HostEndpoint("h1", "fake://h1"), new HostEndpoint("h2", "fake://h2")],
            factory, enableDoorman: false);

        // Act
        var result = await driver.ExecNodeAsync("shop", "shard1", "shard1a", ["true"], CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse("контейнера нет — exec невозможен");
        result.Error!.Message.Should().Contain("pgw-shop-shard1-shard1a");
    }

    // Фабрика движков по endpoint'у хоста (plain: per-host клиенты).
    private sealed class SelectiveFactory(IReadOnlyDictionary<string, HttpMessageHandler> handlers) : DockerEngineFactory
    {
        public IReadOnlyDictionary<string, HttpMessageHandler> Handlers { get; } = handlers;

        public override IDockerEngine Create(string endpoint, string? hostAlias = null)
            => new DockerEngine(new HttpClient(Handlers[endpoint]) { BaseAddress = new Uri("http://docker") }, hostAlias);
    }
}
