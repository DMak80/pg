using System.Net;
using System.Text;
using FluentAssertions;
using S3Worker.Core.OwnS3;
using Xunit;

namespace S3Worker.UnitTests.OwnS3;

// Грань ownS3-ноды (arch/23 §5): health/usage — на мок-handler'е (наш HttpClient);
// bucket-ветки — через loopback-сервер (AWSSDK строит СВОЙ HttpClient — мок
// handler'а он не видит); контракты таймаутов — поллинг-инвариант (7.5 c).
public class OwnS3NodeClientTests
{
    private const string RootUser = "root";
    private const string RootPassword = "pw";

    private static OwnS3NodeSecrets Creds() =>
        new(RootUser, RootPassword, []);

    private sealed class StubHandler(Func<HttpRequestMessage, HttpResponseMessage> responder) : HttpMessageHandler
    {
        public readonly List<(string Method, string Url, string? Authorization)> Requests = [];

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Requests.Add((
                request.Method.Method,
                request.RequestUri!.ToString(),
                request.Headers.TryGetValues("Authorization", out var a) ? a.First() : null));
            return responder(request);
        }
    }

    private static OwnS3NodeClient NewClient(StubHandler handler)
        => new(new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(7.5) });

    // Loopback-сервер на свободном порту для AWSSDK-веток (юнит-скорость).
    private sealed class LoopbackServer(Func<HttpListenerRequest, (int Status, string Body)> respond) : IDisposable
    {
        private readonly HttpListener _listener = new();

        public string Url { get; } = $"http://127.0.0.1:{FreePort()}/";

        public LoopbackServer Start()
        {
            _listener.Prefixes.Add(Url);
            _listener.Start();
            _ = Task.Run(async () =>
            {
                while (_listener.IsListening)
                {
                    try
                    {
                        var context = await _listener.GetContextAsync();
                        var (status, body) = respond(context.Request);
                        context.Response.StatusCode = status;
                        var buffer = Encoding.UTF8.GetBytes(body);
                        await context.Response.OutputStream.WriteAsync(buffer);
                        context.Response.Close();
                    }
                    catch (ObjectDisposedException)
                    {
                        break;
                    }
                    catch (HttpListenerException)
                    {
                        break; // listener остановлен посреди ожидания
                    }
                }
            });
            return this;
        }

        public void Dispose()
        {
            try
            {
                _listener.Stop();
                _listener.Close();
            }
            catch (ObjectDisposedException)
            {
            }
        }

        private static int FreePort()
        {
            var socket = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
            socket.Start();
            var port = ((IPEndPoint)socket.LocalEndpoint).Port;
            socket.Stop();
            return port;
        }
    }

    // AAA: health 200 → true.
    [Fact]
    public async Task Health_200_True()
    {
        // Arrange
        var handler = new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.OK));
        var client = NewClient(handler);

        // Act
        var result = await client.HealthAsync("h1", 9000, TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeTrue();
    }

    // AAA: health 500 → false (транзиент, не исключение).
    [Fact]
    public async Task Health_500_False()
    {
        // Arrange
        var handler = new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.InternalServerError));
        var client = NewClient(handler);

        // Act
        var result = await client.HealthAsync("h1", 9000, TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeFalse();
    }

    // AAA: EnsureBucket — 404 → PutBucket → Success; повторный exists → без PutBucket.
    [Fact]
    public async Task EnsureBucket_Absent_Creates_IdempotentSkip()
    {
        // Arrange: GetBucketLocation 404 → PutBucket 200; затем exists → true.
        var putCalls = 0;
        var exists = false;
        using var server = new LoopbackServer(req =>
        {
            if (req.HttpMethod == "GET" && req.Url!.Query.Contains("location"))
                return exists
                    ? (200, """<?xml version="1.0"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>""")
                    : (404, """<?xml version="1.0"?><Error><Code>NoSuchBucket</Code><Message>no</Message></Error>""");
            if (req.HttpMethod == "PUT")
            {
                Interlocked.Increment(ref putCalls);
                exists = true;
                return (200, "");
            }

            return (400, "");
        }).Start();
        var client = new OwnS3NodeClient(new HttpClient { Timeout = TimeSpan.FromSeconds(7.5) });
        var port = new Uri(server.Url).Port;

        // Act: первое создание (404 → PutBucket), второе (exists → skip).
        var first = await client.EnsureBucketAsync("127.0.0.1", port, Creds(), "demo-bucket", TestContext.Current.CancellationToken);
        var second = await client.EnsureBucketAsync("127.0.0.1", port, Creds(), "demo-bucket", TestContext.Current.CancellationToken);

        // Assert: оба Success; PutBucket вызван ровно один раз.
        first.IsSuccess.Should().BeTrue(first.Error?.Message);
        second.IsSuccess.Should().BeTrue();
        putCalls.Should().Be(1);
    }

    // AAA: PutBucket 409 BucketAlreadyOwnedByYou → Success (идемпотентность).
    [Fact]
    public async Task EnsureBucket_409AlreadyOwned_Success()
    {
        // Arrange: exists=false, PutBucket отвечает 409 already owned.
        using var server = new LoopbackServer(req =>
        {
            if (req.HttpMethod == "GET" && req.Url!.Query.Contains("location"))
                return (404, """<?xml version="1.0"?><Error><Code>NoSuchBucket</Code><Message>no</Message></Error>""");
            if (req.HttpMethod == "PUT")
                return (409, """<?xml version="1.0"?><Error><Code>BucketAlreadyOwnedByYou</Code><Message>own</Message></Error>""");
            return (400, "");
        }).Start();
        var client = new OwnS3NodeClient(new HttpClient { Timeout = TimeSpan.FromSeconds(7.5) });
        var port = new Uri(server.Url).Port;

        // Act
        var result = await client.EnsureBucketAsync("127.0.0.1", port, Creds(), "demo-bucket", TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
    }

    // AAA: usage 200 → разбор camelCase; 500/null-поля → Failed.
    [Fact]
    public async Task Usage_200_ParsesCamelCase_500_Failed()
    {
        // Arrange: два handler'а — валидный и отказ.
        var good = new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent("""{"mode":"XL Single","usedBytes":10,"totalBytes":20}""", Encoding.UTF8, "application/json"),
        });
        var bad = new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.InternalServerError));
        var client = NewClient(good);
        var badClient = NewClient(bad);

        // Act
        var usage = await client.GetUsageAsync("h1", 9000, Creds(), TestContext.Current.CancellationToken);
        var failed = await badClient.GetUsageAsync("h1", 9000, Creds(), TestContext.Current.CancellationToken);

        // Assert: поля разобраны; отказ — Failed (транзиент).
        usage.IsSuccess.Should().BeTrue();
        usage.Value.Should().Be(new OwnS3NodeUsage(10, 20));
        failed.IsSuccess.Should().BeFalse();
        // Запрос подписан SigV4 (Authorization присутствует).
        good.Requests.Should().ContainSingle()
            .Which.Authorization.Should().StartWith("AWS4-HMAC-SHA256");
    }

    // AAA: usage null-поля (диск недоступен) → Failed.
    [Fact]
    public async Task Usage_NullDiskFields_Failed()
    {
        // Arrange: owns3/05 §7 — недоступный диск → null-поля.
        var handler = new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent("""{"mode":"XL Single","usedBytes":null,"totalBytes":null}""", Encoding.UTF8, "application/json"),
        });
        var client = NewClient(handler);

        // Act
        var result = await client.GetUsageAsync("h1", 9000, Creds(), TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeFalse();
    }

    // AAA: контракт таймаутов — HttpClient клиента 7.5 c (поллинг-инвариант).
    [Fact]
    public void Client_Timeout_7500ms()
    {
        // Arrange + Act
        var client = NewClient(new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.OK)));

        // Assert
        client.Timeout.Should().Be(TimeSpan.FromSeconds(7.5));
    }
}
