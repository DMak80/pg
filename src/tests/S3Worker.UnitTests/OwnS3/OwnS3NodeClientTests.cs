using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using FluentAssertions;
using OwnS3.Protocol.PeerAuth;
using OwnS3.Protocol.Requests;
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

        public readonly List<string> Bodies = [];

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Requests.Add((
                request.Method.Method,
                request.RequestUri!.ToString(),
                request.Headers.TryGetValues("Authorization", out var a) ? a.First() : null));
            if (request.Content is not null)
                Bodies.Add(await request.Content.ReadAsStringAsync(ct));
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
        // Arrange: ListBuckets пусто → PutBucket 200; затем bucket в списке.
        var putCalls = 0;
        var exists = false;
        using var server = new LoopbackServer(req =>
        {
            if (req.HttpMethod == "GET" && req.Url!.AbsolutePath == "/")
                return exists
                    ? (200, """<?xml version="1.0"?><ListAllMyBucketsResult><Buckets><Bucket><Name>demo-bucket</Name></Bucket></Buckets></ListAllMyBucketsResult>""")
                    : (200, """<?xml version="1.0"?><ListAllMyBucketsResult><Buckets/></ListAllMyBucketsResult>""");
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
            if (req.HttpMethod == "GET" && req.Url!.AbsolutePath == "/")
                return (200, """<?xml version="1.0"?><ListAllMyBucketsResult><Buckets/></ListAllMyBucketsResult>""");
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

    // ============ mesh-расширения (arch/24 §5 G, spec §5.1/§8.1.6) ============

    // AAA: status 200 → active + имена соседей; SigV4-подпись присутствует.
    [Fact]
    public async Task ReplicationStatus_200_ParsesNeighbors()
    {
        // Arrange
        var handler = new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(
                """{"active":true,"outbox":{"bytes":1},"neighbors":[{"name":"east2","auth":"key","online":true}]}""",
                Encoding.UTF8, "application/json"),
        });
        var client = NewClient(handler);

        // Act
        var result = await client.GetReplicationStatusAsync("h1", 9000, Creds(), TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        result.Value.Active.Should().BeTrue();
        result.Value.NeighborNames.Should().BeEquivalentTo("east2");
        handler.Requests.Should().ContainSingle()
            .Which.Authorization.Should().StartWith("AWS4-HMAC-SHA256");
    }

    // AAA: status 502 → Failed (транзиент тика).
    [Fact]
    public async Task ReplicationStatus_502_Failed()
    {
        // Arrange
        var client = NewClient(new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.BadGateway)));

        // Act
        var result = await client.GetReplicationStatusAsync("h1", 9000, Creds(), TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeFalse();
    }

    // AAA: join 200 → Joined с именем пира из тела; 409 → Conflict; 403 → Refused;
    // 502 → Transient (G различает исходы, §3.5).
    [Theory]
    [InlineData(200, OwnS3JoinKind.Joined, "east2")]
    [InlineData(409, OwnS3JoinKind.Conflict, null)]
    [InlineData(403, OwnS3JoinKind.Refused, null)]
    [InlineData(502, OwnS3JoinKind.Transient, null)]
    public async Task AdminJoin_MapsStatusesToOutcomes(int status, OwnS3JoinKind kind, string? peer)
    {
        // Arrange: 200 отдаёт {"joined":true,"peer":"east2"}.
        var handler = new StubHandler(_ => new HttpResponseMessage((HttpStatusCode)status)
        {
            Content = new StringContent("""{"joined":true,"peer":"east2"}""", Encoding.UTF8, "application/json"),
        });
        var client = NewClient(handler);

        // Act
        var result = await client.AdminJoinAsync("h1", 9000, Creds(), "http://b:9000", TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Kind.Should().Be(kind);
        result.Value.PeerName.Should().Be(peer);
        // Тело — {"endpoint": …} camelCase (ownS3-парсер регистрочувствителен —
        // PascalCase даёт 400 malformed, регресс E2E); метод POST.
        handler.Requests.Should().ContainSingle()
            .Which.Method.Should().Be("POST");
        handler.Bodies.Should().ContainSingle().Which.Should().Be(
            "{\"endpoint\":\"http://b:9000\"}");
    }

    // AAA: join — транспортный отказ → Transient (не исключение наружу).
    [Fact]
    public async Task AdminJoin_Transport_Transient()
    {
        // Arrange: handler рвёт соединение.
        var handler = new StubHandler(_ => throw new HttpRequestException("connection refused"));
        var client = NewClient(handler);

        // Act
        var result = await client.AdminJoinAsync("h1", 9000, Creds(), "http://b:9000", TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Kind.Should().Be(OwnS3JoinKind.Transient);
    }

    // AAA: delete-target 202 → true; 404 → false (идемпотентность); 500 → Failed.
    [Theory]
    [InlineData(202, true)]
    [InlineData(404, false)]
    public async Task DeleteTarget_MapsStatuses(int status, bool removed)
    {
        // Arrange
        var handler = new StubHandler(_ => new HttpResponseMessage((HttpStatusCode)status));
        var client = NewClient(handler);

        // Act
        var result = await client.DeleteTargetAsync("h1", 9000, Creds(), "east2", TestContext.Current.CancellationToken);

        // Assert: Success с removed-фактом (404 — идемпотентный «уже нет»).
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(removed);
        handler.Requests.Should().ContainSingle()
            .Which.Url.Should().Contain("/admin/replication/targets/east2");
    }

    [Fact]
    public async Task DeleteTarget_500_Failed()
    {
        // Arrange
        var client = NewClient(new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.InternalServerError)));

        // Act
        var result = await client.DeleteTargetAsync("h1", 9000, Creds(), "east2", TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeFalse();
    }

    // AAA: deny-перезапись 202 → true; 403 → false (trust не принят — transient);
    // 400 → Failed. Тело — {"entries":[{name,publicKeyPem}]}, подпись OWNS3-PEER.
    [Theory]
    [InlineData(202, true)]
    [InlineData(403, false)]
    public async Task PutDenyList_MapsStatuses(int status, bool pushed)
    {
        // Arrange
        var handler = new StubHandler(_ => new HttpResponseMessage((HttpStatusCode)status));
        var client = NewClient(handler);

        // Act
        var result = await client.PutDenyListAsync(
            "h1", 9000, [("west1", "-----BEGIN PUBLIC KEY-----x")], Orchestrator(), TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(pushed);
        var request = handler.Requests.Should().ContainSingle().Which;
        request.Method.Should().Be("PUT");
        request.Url.Should().Contain("/replication/denylist");
        request.Authorization.Should().StartWith("OWNS3-PEER-ECC-P256-SHA256");
    }

    [Fact]
    public async Task PutDenyList_400_Failed()
    {
        // Arrange
        var client = NewClient(new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.BadRequest)));

        // Act
        var result = await client.PutDenyListAsync(
            "h1", 9000, [("west1", "-----BEGIN PUBLIC KEY-----x")], Orchestrator(), TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeFalse();
    }

    // AAA: подпись PUT /replication/denylist проходит PeerRequestVerifier ownS3
    // (совместимость контуров, spec §8.1.6) — модель по образцу тестов JoinPeerService.
    [Fact]
    public void PutDenyList_Signature_PassesOwnS3PeerVerifier()
    {
        // Arrange: материал оркестратора (PEM-ключ ↔ сертификат согласованы).
        var orchestrator = Orchestrator();
        var body = """{"entries":[{"name":"west1","publicKeyPem":"-----BEGIN PUBLIC KEY-----x"}]}"""u8.ToArray();
        var at = new DateTimeOffset(2026, 10, 14, 12, 0, 0, TimeSpan.Zero);

        // Act: построение запроса (внутренняя точка подписи) → модель грани.
        using var request = OwnS3NodeClient.BuildPeerSignedRequest(
            HttpMethod.Put, "neighbor", 9000, "/replication/denylist", body, orchestrator, at);
        var model = new S3RequestModel
        {
            Method = "PUT",
            RawPath = "/replication/denylist",
            RawQuery = "",
            Headers = S3HeaderCollection.FromPairs(
                ("Authorization", Header(request, "Authorization")),
                ("x-owns3-peer-date", Header(request, "x-owns3-peer-date")),
                ("x-owns3-peer-content-sha256", Header(request, "x-owns3-peer-content-sha256")),
                ("host", Header(request, "host"))),
            Host = Header(request, "host"),
            OpenBody = () => new MemoryStream(body),
        };
        using var key = ECDsa.Create();
        key.ImportFromPem(orchestrator.PrivateKeyPem);
        var time = new FixedTimeProvider { Utc = at };
        var verify = new PeerRequestVerifier(time).Verify(model, key);

        // Assert: Ok с Credential=имя оркестратора (взаимная совместимость).
        var ok = verify.Should().BeOfType<PeerVerifyResult.Ok>().Which;
        ok.PeerName.Should().Be(orchestrator.Certificate.Name);
    }

    private static string Header(HttpRequestMessage request, string name)
        => request.Headers.TryGetValues(name, out var values) ? values.First() : throw new InvalidOperationException(name);

    // Материал оркестратора: CA → сертификат orchestrator (PEM-ключ реальный).
    private static IssuedPkiMaterial Orchestrator()
    {
        var (caJson, caPem) = PeerCertificateAuthority.CreateCa("backups");
        using var doc = System.Text.Json.JsonDocument.Parse(caJson);
        var anchor = new PeerTrustAnchor(
            "backups", doc.RootElement.GetProperty("publicKeyPem").GetString()!);
        var (cert, keyPem) = PeerCertificateAuthority.IssueServer(
            "orchestrator", anchor, caPem, DateTimeOffset.UtcNow, expiresDays: 0);
        return new IssuedPkiMaterial(cert, cert.ToJson(), keyPem);
    }
}
