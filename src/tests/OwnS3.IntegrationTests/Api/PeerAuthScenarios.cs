using System.Globalization;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using OwnS3.Protocol.PeerAuth;
using OwnS3.Replication;
using OwnS3.Replication.Transport;

namespace OwnS3.IntegrationTests.Api;

// Статический PeerAuth-материал инсталляции (t39): CA + identity узла + серты пиров
// — общие ассеты сценариев (не рантайм-стейт), генерируются PeerCertificateAuthority
// один раз. Срок сертификатов — от НАСТОЯЩЕГО времени: PeerIdentityLoader (fail-fast
// старта хоста) сверяет срок с системными часами, а хост-верификаторы (FixedTimeProvider
// 2013 года) видят ещё действующий срок — обе стороны довольны одним материалом.
internal static class PeerAuthMaterial
{
    public static PeerTrustAnchor Ca { get; }
    public static string CaPrivatePem { get; }
    public static PeerIdentity Own { get; }
    public static string OwnPrivatePem { get; }
    public static PeerCertificate PeerXCert { get; }
    public static ECDsa PeerXKey { get; }
    public static PeerCertificate PeerYCert { get; }
    public static ECDsa PeerYKey { get; }
    public static PeerCertificate TriggerCert { get; }
    public static ECDsa TriggerKey { get; }
    public static PeerTrustAnchor ForeignCa { get; }
    public static string ForeignCaPrivatePem { get; }

    static PeerAuthMaterial()
    {
        var (caJson, caPem) = PeerCertificateAuthority.CreateCa("owns3-test-ca");
        Ca = PeerTrustAnchor.FromJson(caJson);
        CaPrivatePem = caPem;
        var now = DateTimeOffset.UtcNow;

        (var ownCert, OwnPrivatePem) = PeerCertificateAuthority.IssueServer("owns3-own", Ca, caPem, now);
        Own = NewIdentity(ownCert, OwnPrivatePem);

        (PeerXCert, var peerXPem) = PeerCertificateAuthority.IssueServer("peer-x", Ca, caPem, now);
        PeerXKey = NewKey(peerXPem);

        var (foreignCaJson, foreignCaPem) = PeerCertificateAuthority.CreateCa("foreign-ca");
        var foreignCa = PeerTrustAnchor.FromJson(foreignCaJson);
        ForeignCa = foreignCa;
        ForeignCaPrivatePem = foreignCaPem;
        (PeerYCert, var peerYPem) = PeerCertificateAuthority.IssueServer("peer-y", foreignCa, foreignCaPem, now);
        PeerYKey = NewKey(peerYPem);

        (TriggerCert, var triggerPem) = PeerCertificateAuthority.IssueServer("peer-trigger", Ca, caPem, now);
        TriggerKey = NewKey(triggerPem);
    }

    private static ECDsa NewKey(string privatePem)
    {
        var key = ECDsa.Create();
        key.ImportFromPem(privatePem);
        return key;
    }

    private static PeerIdentity NewIdentity(PeerCertificate cert, string privatePem) =>
        new(cert.Name, cert, NewKey(privatePem), cert.ToJson());
}

// Фейк-сосед для join-trigger (исходящая сторона): записывает запросы, отвечает
// запрограммированным ответом (прецедент юнитов JoinPeerServiceTests).
public sealed class FakeNeighborHandler(Func<HttpRequestMessage, HttpResponseMessage> reply) : HttpMessageHandler
{
    public HttpRequestMessage? Last { get; private set; }
    public string? LastBody { get; private set; }
    public int Calls { get; private set; }

    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        Calls++;
        Last = request;
        LastBody = request.Content is null
            ? null
            : request.Content.ReadAsStringAsync(cancellationToken).GetAwaiter().GetResult();
        return Task.FromResult(reply(request));
    }
}

// Фабрика PeerAuth-хоста (t39): identity/trust/advertised — статический материал
// в in-memory конфиге (env-поставка §4.6); legacy-состав peer-a/b/c наследуется
// от ReplicationAppFactory (регресс). Наследники переопределяют материал по кейсам
// (без identity / без trust / без advertised) и подменяют исходящий транспорт.
public class PeerAuthAppFactory : ReplicationAppFactory
{
    private readonly FakeNeighborHandler? _neighborHandler;

    // Фабрика без фейк-соседа (штатный JoinPeerService с identity/trust).
    // Второй ctor — protected: class-fixture xUnit требует один публичный ctor.
    public PeerAuthAppFactory() { }

    // Фабрика с фейк-соседом: JoinPeerService подменяется на сервис с фейк-HttpClient.
    protected PeerAuthAppFactory(FakeNeighborHandler neighborHandler) => _neighborHandler = neighborHandler;

    // Фейк-сосед (для наследников, программирующих ответы).
    protected FakeNeighborHandler? Neighbor => _neighborHandler;

    protected virtual PeerIdentity? IdentityMaterial => PeerAuthMaterial.Own;
    protected virtual PeerTrustAnchor? TrustMaterial => PeerAuthMaterial.Ca;
    protected virtual string? AdvertisedEndpointMaterial => "http://owns3-own:9000";

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        base.ConfigureWebHost(builder);
        builder.ConfigureAppConfiguration((_, cfg) =>
        {
            var config = new Dictionary<string, string?>();
            if (IdentityMaterial is { } own)
            {
                config["OwnS3:Replication:Identity:Certificate"] = own.CertificateJson;
                config["OwnS3:Replication:Identity:PrivateKeyPem"] = PeerAuthMaterial.OwnPrivatePem;
            }
            if (TrustMaterial is { } trust)
                config["OwnS3:Replication:TrustCa:Certificate"] = trust.ToJson();
            if (AdvertisedEndpointMaterial is { } advertised)
                config["OwnS3:Replication:AdvertisedEndpoint"] = advertised;
            if (config.Count > 0)
                cfg.AddInMemoryCollection(config);
        });
        var handler = _neighborHandler;
        if (handler is null)
            return;
        var identity = IdentityMaterial;
        var trust = TrustMaterial;
        var advertised = AdvertisedEndpointMaterial;
        builder.ConfigureTestServices(services =>
        {
            services.RemoveAll<JoinPeerService>();
            services.AddSingleton(sp => new JoinPeerService(
                identity, trust,
                sp.GetRequiredService<ReplicationTargets>(),
                sp.GetRequiredService<DenyList>(),
                new HttpClient(handler), sp.GetRequiredService<TimeProvider>(),
                sp.GetRequiredService<ILogger<JoinPeerService>>(), advertised));
        });
    }
}

// Входящий join без trust-anchor (ручной режим состава) — 403 (spec §4.4 п.3).
public sealed class PeerAuthNoTrustAppFactory : PeerAuthAppFactory
{
    protected override PeerTrustAnchor? TrustMaterial => null;
}

// Replication-грань peer-схемы (t39, глава 07 §6/spec §4.2–§4.4), join (входящий и
// исходящий), удаление с отзывом, deny-list, статус auth; fail-fast битого identity.
public sealed class PeerAuthScenarios : IClassFixture<PeerAuthAppFactory>
{
    private readonly PeerAuthAppFactory _factory;

    public PeerAuthScenarios(PeerAuthAppFactory factory) => _factory = factory;

    private static readonly JsonSerializerOptions Json = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    private OwnS3TestClient Client => new(_factory.CreateClient());

    private OwnS3TestClient.Credentials ReplicationCreds =>
        new(OwnS3AppFactory.ReplicationAccessKey, OwnS3AppFactory.ReplicationSecretKey);

    private static string CertBase64Of(PeerCertificate cert) =>
        Convert.ToBase64String(Encoding.UTF8.GetBytes(cert.ToJson()));

    private static string PayloadHash(string body) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(body))).ToLowerInvariant();

    private static string Batch(params ReplicationEvent[] events) =>
        JsonSerializer.Serialize(new NotificationBatchRequest("mesh-a", events),
            ReplicationTransportJson.Options);

    private static ReplicationEvent BucketCreate(long seqNo, string bucket, long modMs) =>
        new(seqNo, ReplicationEventType.BucketCreate, bucket, "", modMs, "", 0, "", "mesh-a");

    // Peer-подписанный запрос к replication-грани (заголовки схемы §4.2 вручную —
    // по образцу raw-SigV4 клиента ReplicationScenarios; дата хоста по умолчанию).
    private async Task<HttpResponseMessage> SendPeerAsync(string method, string pathAndQuery,
        string body, ECDsa signingKey, string credential, string? certificateBase64 = null,
        DateTimeOffset? at = null)
    {
        var client = Client;
        var payloadHash = PayloadHash(body);
        var date = at ?? OwnS3AppFactory.HostTime;
        var peerDate = date.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture);
        var host = client.Http.BaseAddress!.Authority;
        var path = pathAndQuery;
        var query = string.Empty;
        var queryIndex = pathAndQuery.IndexOf('?');
        if (queryIndex >= 0)
        {
            path = pathAndQuery[..queryIndex];
            query = pathAndQuery[(queryIndex + 1)..];
        }
        var (authorization, _, _) = PeerRequestSigner.Sign(signingKey, credential, method, path, query,
            new List<(string, string)>
            {
                ("host", host),
                ("x-owns3-peer-date", peerDate),
                ("x-owns3-peer-content-sha256", payloadHash),
            }, payloadHash, date);

        using var request = new HttpRequestMessage(new HttpMethod(method), pathAndQuery);
        if (body.Length > 0)
            request.Content = new ByteArrayContent(Encoding.UTF8.GetBytes(body));
        request.Headers.TryAddWithoutValidation("x-owns3-peer-date", peerDate);
        request.Headers.TryAddWithoutValidation("x-owns3-peer-content-sha256", payloadHash);
        request.Headers.TryAddWithoutValidation("Authorization", authorization);
        if (certificateBase64 is not null)
            request.Headers.TryAddWithoutValidation("X-OwnS3-Peer-Certificate", certificateBase64);
        return await client.Http.SendAsync(request, TestContext.Current.CancellationToken);
    }

    // Входящий join от peer-x (сертовый ключ, credential peer-x); заголовок
    // сертификата — по умолчанию, кейс MissingCertHeader шлёт без него.
    private Task<HttpResponseMessage> SendJoinAsync(string advertisedEndpoint,
        bool withCertificate = true) =>
        SendPeerAsync("POST", "/replication/join",
            JsonSerializer.Serialize(new { advertisedEndpoint }, Json),
            PeerAuthMaterial.PeerXKey, "peer-x",
            withCertificate ? CertBase64Of(PeerAuthMaterial.PeerXCert) : null);

    // Key-запись peer-x в составе НЕ конфигом (env-seed legacy-only): PUT admin-гранью
    // с ПОЛНЫМ списком (legacy-наследие peer-a/b/c + key peer-x) — spec §4.5 «PUT
    // targets работает для обоих видов».
    private async Task<HttpResponseMessage> EnsureKeyTargetAsync(OwnS3TestClient admin)
    {
        var payload = JsonSerializer.Serialize(new
        {
            targets = new object[]
            {
                new
                {
                    name = "peer-a", endpoint = "http://127.0.0.1:9", auth = "legacy",
                    accessKey = OwnS3AppFactory.ReplicationAccessKey,
                    secretKey = OwnS3AppFactory.ReplicationSecretKey,
                },
                new
                {
                    name = "peer-b", endpoint = "http://127.0.0.1:9", auth = "legacy",
                    accessKey = ReplicationAppFactory.PeerBAccessKey,
                    secretKey = ReplicationAppFactory.PeerBSecretKey,
                },
                new
                {
                    name = "peer-c", endpoint = "http://127.0.0.1:9", auth = "legacy",
                    accessKey = ReplicationAppFactory.PeerCAccessKey,
                    secretKey = ReplicationAppFactory.PeerCSecretKey,
                },
                new
                {
                    name = "peer-x", endpoint = "http://peer-x:9000", auth = "key",
                    publicKeyPem = PeerAuthMaterial.PeerXCert.PublicKeyPem,
                },
            },
        }, Json);
        var response = await admin.SendSignedAsync("PUT", "/admin/replication/targets",
            payloadString: PayloadHash(payload), body: Encoding.UTF8.GetBytes(payload),
            credentials: OwnS3TestClient.Admin());
        response.StatusCode.Should().Be(HttpStatusCode.Accepted);
        return response;
    }

    private async Task<JsonElement> StatusAsync(OwnS3TestClient client, string path = "/admin/replication/status")
    {
        var response = await client.SendSignedAsync("GET", path, credentials: OwnS3TestClient.Admin());
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        return JsonDocument.Parse(await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .RootElement;
    }

    // ── replication-грань, peer-схема (после EnsureKeyTargetAsync) ──

    [Fact]
    public async Task Ping_KeyNeighborSigned_200()
    {
        // Arrange — peer-x в составе (key-запись)
        var client = Client;
        await EnsureKeyTargetAsync(client);

        // Act — GET /replication/ping с peer-подписью peer-x
        var response = await SendPeerAsync("GET", "/replication/ping", string.Empty,
            PeerAuthMaterial.PeerXKey, "peer-x");

        // Assert — 200 (глава 07 §6: обе схемы на грани)
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task Ping_TamperedSignature_403SignatureDoesNotMatch()
    {
        // Arrange — peer-x в составе; подпись корректной структуры, но неверная
        var client = Client;
        await EnsureKeyTargetAsync(client);

        // Act — ping с подменённой Signature (64 нулевых байта)
        var tampered = await SendTamperedPingAsync(client);

        // Assert — 403 SignatureDoesNotMatch (каталог главы 03 §5)
        tampered.StatusCode.Should().Be(HttpStatusCode.Forbidden);
        (await tampered.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .Should().Contain("SignatureDoesNotMatch");
    }

    // Подпись валидной структурой с нулевой сигнатурой (тампер по проводу).
    private Task<HttpResponseMessage> SendTamperedPingAsync(OwnS3TestClient client)
    {
        var payloadHash = PayloadHash(string.Empty);
        var peerDate = OwnS3AppFactory.HostTime.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture);
        var host = client.Http.BaseAddress!.Authority;
        // Arrange — корректная структура, подпись 64 нулевых байта (неверная)
        var authorization =
            $"{PeerRequestSigner.Algorithm} Credential=peer-x, " +
            $"SignedHeaders={string.Join(";", PeerRequestSigner.SignedHeaderNames)}, " +
            $"Signature={Convert.ToBase64String(new byte[64])}";
        var request = new HttpRequestMessage(HttpMethod.Get, "/replication/ping");
        request.Headers.TryAddWithoutValidation("x-owns3-peer-date", peerDate);
        request.Headers.TryAddWithoutValidation("x-owns3-peer-content-sha256", payloadHash);
        request.Headers.TryAddWithoutValidation("Authorization", authorization);
        return client.Http.SendAsync(request, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Ping_OtherKeyForKnownPeer_403SignatureDoesNotMatch()
    {
        // Arrange — peer-x в составе (ключ peer-x в записи), подпись ключом peer-y
        var client = Client;
        await EnsureKeyTargetAsync(client);

        // Act — Credential=peer-x, подпись чужим ключом
        var response = await SendPeerAsync("GET", "/replication/ping", string.Empty,
            PeerAuthMaterial.PeerYKey, "peer-x");

        // Assert — запись найдена, ключ не тот → 403 SignatureDoesNotMatch
        response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
        (await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .Should().Contain("SignatureDoesNotMatch");
    }

    [Fact]
    public async Task Ping_UnknownPeerName_403()
    {
        // Arrange — Credential=ghost: записи нет
        var client = Client;
        await EnsureKeyTargetAsync(client);

        // Act
        var response = await SendPeerAsync("GET", "/replication/ping", string.Empty,
            PeerAuthMaterial.PeerXKey, "ghost");

        // Assert — не числится соседом → 403 AccessDenied (зеркало SigV4-ветки)
        response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
        (await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .Should().Contain("AccessDenied");
    }

    [Fact]
    public async Task Ping_Skew_403RequestTimeTooSkewed()
    {
        // Arrange — peer-x в составе; дата подписи HostTime+16 минут
        var client = Client;
        await EnsureKeyTargetAsync(client);

        // Act
        var response = await SendPeerAsync("GET", "/replication/ping", string.Empty,
            PeerAuthMaterial.PeerXKey, "peer-x", at: OwnS3AppFactory.HostTime.AddMinutes(16));

        // Assert — skew ±15 мин → 403 RequestTimeTooSkewed
        response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
        (await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .Should().Contain("RequestTimeTooSkewed");
    }

    [Fact]
    public async Task Ping_MalformedAuthorization_400()
    {
        // Arrange — мусорный OWNS3-PEER-заголовок (префикс схемы, без Credential)
        var client = Client;
        using var request = new HttpRequestMessage(HttpMethod.Get, "/replication/ping");
        request.Headers.TryAddWithoutValidation("Authorization", "OWNS3-PEER-мусор");

        // Act
        var response = await client.Http.SendAsync(request, TestContext.Current.CancellationToken);

        // Assert — CredentialOf → null → 400 AuthorizationHeaderMalformed (глава 07 §6)
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .Should().Contain("AuthorizationHeaderMalformed");
    }

    [Fact]
    public async Task Notifications_KeyNeighborSigned_Accepted()
    {
        // Arrange — peer-x в составе; батч с собственным seqNo-пространством
        var client = Client;
        await EnsureKeyTargetAsync(client);
        var batch = Batch(BucketCreate(900, "repl-peer", 100));

        // Act — POST notifications peer-подписью peer-x
        var response = await SendPeerAsync("POST", "/replication/notifications", batch,
            PeerAuthMaterial.PeerXKey, "peer-x");

        // Assert — 200 + acceptedToSeqNo; бакет применён (источник = peer-x)
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        JsonDocument.Parse(body).RootElement.GetProperty("acceptedToSeqNo").GetInt64().Should().Be(900);
        var buckets = await client.SendSignedAsync("GET", "/", credentials: OwnS3TestClient.Writer());
        (await buckets.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .Should().Contain("repl-peer");
    }

    [Fact]
    public async Task ClientEdge_PeerHeader_Still400InvalidRequest()
    {
        // Act — клиентская S3-грань с OWNS3-PEER-* Authorization (принцип 3 spec)
        var client = Client;
        using var request = new HttpRequestMessage(HttpMethod.Put, "/some-bucket/some-key");
        request.Content = new ByteArrayContent("x"u8.ToArray());
        request.Headers.TryAddWithoutValidation("Authorization",
            $"{PeerRequestSigner.Algorithm} Credential=peer-x, " +
            "SignedHeaders=host;x-owns3-peer-date;x-owns3-peer-content-sha256, Signature=AAAA");

        // Assert — прежний 400 InvalidRequest (глава 03 §1: алгоритм не поддерживается)
        var response = await client.Http.SendAsync(request, TestContext.Current.CancellationToken);
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .Should().Contain("InvalidRequest");
    }

    [Fact]
    public async Task SigV4_LegacyPeer_Regression200()
    {
        // Arrange — полный состав (legacy + key) через PUT admin-грани
        var client = Client;
        await EnsureKeyTargetAsync(client);
        var batch = Batch(BucketCreate(950, "repl-legacy", 100));

        // Act — legacy-пир peer-a по SigV4: ping + notifications
        var ping = await client.SendSignedAsync("GET", "/replication/ping", credentials: ReplicationCreds);
        var notifications = await client.SendSignedAsync("POST", "/replication/notifications",
            payloadString: PayloadHash(batch), body: Encoding.UTF8.GetBytes(batch),
            credentials: ReplicationCreds);

        // Assert — обе операции 200 (регресс legacy-ветки при активном peer-контуре)
        ping.StatusCode.Should().Be(HttpStatusCode.OK);
        notifications.StatusCode.Should().Be(HttpStatusCode.OK);
        JsonDocument.Parse(await notifications.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .RootElement.GetProperty("acceptedToSeqNo").GetInt64().Should().Be(950);
    }

    // ── join, входящая сторона (replication-грань, POST /replication/join) ──

    [Fact]
    public async Task JoinIncoming_ValidCert_AddsKeyTarget()
    {
        // Arrange — СОБСТВЕННАЯ фабрика: peer-x ещё не числится (join от нечислящегося пира)
        using var factory = new PeerAuthAppFactory();
        var client = new OwnS3TestClient(factory.CreateClient());

        // Act — join peer-x с валидным сертификатом против trust-anchor хоста
        var response = await SendJoinOnAsync(client, "http://peer-x:9000");

        // Assert — 200 {name, advertisedEndpoint, certificate} + подпись тела
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Headers.Should().ContainKey("X-OwnS3-Peer-Signature");
        var body = JsonDocument.Parse(
            await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement;
        body.GetProperty("name").GetString().Should().Be("owns3-own");
        body.GetProperty("advertisedEndpoint").GetString().Should().Be("http://owns3-own:9000");
        body.GetProperty("certificate").GetString().Should().NotBeNullOrEmpty();

        // Assert — peer-x в составе с auth=key (статус)
        var neighbors = await NeighborsOfAsync(client);
        var peerX = neighbors.EnumerateArray()
            .FirstOrDefault(n => n.GetProperty("name").GetString() == "peer-x");
        peerX.ValueKind.Should().Be(JsonValueKind.Object);
        peerX.GetProperty("auth").GetString().Should().Be("key");
    }

    [Fact]
    public async Task JoinIncoming_NoTrustAnchor_403()
    {
        // Arrange — фабрика без trust-anchor (ручной режим состава)
        using var factory = new PeerAuthNoTrustAppFactory();
        var client = new OwnS3TestClient(factory.CreateClient());

        // Act
        var response = await SendJoinOnAsync(client, "http://peer-x:9000");

        // Assert — 403 (spec §4.4 п.3)
        response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
    }

    [Fact]
    public async Task JoinIncoming_DeniedPeer_403()
    {
        // Arrange — СОБСТВЕННАЯ фабрика: состав с peer-x, затем отзыв
        using var factory = new PeerAuthAppFactory();
        var client = new OwnS3TestClient(factory.CreateClient());
        await EnsureKeyTargetOnAsync(client);
        (await DeleteTargetAsync(client, "peer-x", revoke: true)).StatusCode
            .Should().Be(HttpStatusCode.Accepted);

        // Act — повторный join отозванного пира
        var response = await SendJoinOnAsync(client, "http://peer-x:9000");

        // Assert — deny-list по имени → 403 (spec §4.4/§4.5)
        response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
    }

    [Fact]
    public async Task JoinIncoming_ConflictingKey_409()
    {
        // Arrange — СОБСТВЕННАЯ фабрика: peer-x в составе с ДРУГИМ публичным ключом
        using var factory = new PeerAuthAppFactory();
        var client = new OwnS3TestClient(factory.CreateClient());
        await PutKeyTargetAsync(client, "peer-x", ECDsa.Create(ECCurve.NamedCurves.nistP256)
            .ExportSubjectPublicKeyInfoPem());

        // Act — join с НАСТОЯЩИМ сертификатом peer-x
        var response = await SendJoinOnAsync(client, "http://peer-x:9000");

        // Assert — имя числится с другим ключом → 409 JSON {"error"}
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        (await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .Should().Contain("error");
    }

    [Fact]
    public async Task JoinIncoming_SameKey_IdempotentEndpointUpdate()
    {
        // Arrange — СОБСТВЕННАЯ фабрика: peer-x с тем же ключом, endpoint «старый»
        using var factory = new PeerAuthAppFactory();
        var client = new OwnS3TestClient(factory.CreateClient());
        await PutKeyTargetAsync(client, "peer-x", PeerAuthMaterial.PeerXCert.PublicKeyPem,
            endpoint: "http://peer-x-old:9000");

        // Act — повторный join тем же ключом с новым advertised endpoint
        var response = await SendJoinOnAsync(client, "http://peer-x-new:9000");

        // Assert — 200 (идемпотентное обновление endpoint, spec §4.4 п.3; endpoint
        // в статусе не отдаётся — единственность записи peer-x по имени)
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var neighbors = await NeighborsOfAsync(client);
        neighbors.EnumerateArray().Count(n => n.GetProperty("name").GetString() == "peer-x").Should().Be(1);
    }

    [Fact]
    public async Task JoinIncoming_MissingCertHeader_400()
    {
        // Act — join без заголовка X-OwnS3-Peer-Certificate
        var response = await SendJoinAsync("http://peer-x:9000", withCertificate: false);

        // Assert — 400 с пояснением
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .Should().Contain("X-OwnS3-Peer-Certificate");
    }

    [Fact]
    public async Task JoinIncoming_MalformedAuthorization_400()
    {
        // Arrange — СОБСТВЕННАЯ фабрика; мусорная структура OWNS3-PEER-Authorization
        // (валидный префикс схемы) на join-маршруте, сертификат/тело — валидные
        using var factory = new PeerAuthAppFactory();
        var client = new OwnS3TestClient(factory.CreateClient());
        using var request = new HttpRequestMessage(HttpMethod.Post, "/replication/join");
        request.Content = new ByteArrayContent(Encoding.UTF8.GetBytes(
            JsonSerializer.Serialize(new { advertisedEndpoint = "http://peer-x:9000" }, Json)));
        request.Headers.TryAddWithoutValidation("Authorization", "OWNS3-PEER-мусор");
        request.Headers.TryAddWithoutValidation("X-OwnS3-Peer-Certificate",
            CertBase64Of(PeerAuthMaterial.PeerXCert));

        // Act
        var response = await client.Http.SendAsync(request, TestContext.Current.CancellationToken);

        // Assert — 400 AuthorizationHeaderMalformed (spec §4.2/глава 07 §6; выровнено
        // с peer-веткой middleware), XML-формат грани
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .Should().Contain("AuthorizationHeaderMalformed");
    }

    [Fact]
    public async Task JoinIncoming_InvalidAdvertisedEndpoint_400()
    {
        // Arrange — СОБСТВЕННАЯ фабрика; мусорный advertisedEndpoint в теле join
        using var factory = new PeerAuthAppFactory();
        var client = new OwnS3TestClient(factory.CreateClient());

        // Act — join с невалидным advertisedEndpoint
        var response = await SendJoinOnAsync(client, "не-uri");

        // Assert — 400 до записи в состав (иначе ArgumentException из
        // AddOrReplace → 500, spec §4.4 п.3)
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .Should().Contain("advertisedEndpoint");
    }

    // ── join-trigger, исходящая сторона (POST /admin/replication/join, фейк-сосед) ──

    [Fact]
    public async Task JoinTrigger_NoIdentity_400()
    {
        // Arrange — СОБСТВЕННАЯ фабрика без identity (trust/advertised заданы)
        using var factory = new PeerAuthTriggerFactory(withIdentity: false);
        var client = new OwnS3TestClient(factory.CreateClient());

        // Act
        var response = await AdminJoinAsync(client, "http://fake-neighbor:9000");

        // Assert — 400 {"error": пояснение} (spec §4.4 п.1); сосед НЕ вызывался
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .Should().Contain("error");
        factory.Handler.Calls.Should().Be(0);
    }

    [Fact]
    public async Task JoinTrigger_NoAdvertised_400()
    {
        // Arrange — фабрика без advertised endpoint
        using var factory = new PeerAuthTriggerFactory(withAdvertised: false);
        var client = new OwnS3TestClient(factory.CreateClient());

        // Act / Assert — 400; сосед НЕ вызывался
        var response = await AdminJoinAsync(client, "http://fake-neighbor:9000");
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        factory.Handler.Calls.Should().Be(0);
    }

    [Fact]
    public async Task JoinTrigger_NoTrust_400()
    {
        // Arrange — у инициатора нет trust-anchor (сертификат ответа соседа не проверить)
        using var factory = new PeerAuthTriggerFactory(withTrust: false);
        var client = new OwnS3TestClient(factory.CreateClient());

        // Act / Assert — 400; сосед НЕ вызывался (spec §4.4 п.1)
        var response = await AdminJoinAsync(client, "http://fake-neighbor:9000");
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        factory.Handler.Calls.Should().Be(0);
    }

    [Fact]
    public async Task JoinTrigger_PeerRefused403_Proxied()
    {
        // Arrange — фейк-сосед отвечает 403
        using var factory = new PeerAuthTriggerFactory(_ => new HttpResponseMessage(HttpStatusCode.Forbidden));
        var client = new OwnS3TestClient(factory.CreateClient());

        // Act
        var response = await AdminJoinAsync(client, "http://fake-neighbor:9000");

        // Assert — отказ соседа проксируется тем же кодом (spec §4.4 п.4)
        response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
        factory.Handler.Calls.Should().Be(1);
    }

    [Fact]
    public async Task JoinTrigger_InvalidResponse_502()
    {
        // Arrange — фейк-сосед: 200 с телом, подписанным ЧУЖИМ ключом
        using var factory = new PeerAuthTriggerFactory(_ =>
            JoinOkResponse(PeerAuthMaterial.TriggerCert, PeerAuthMaterial.PeerYKey,
                "peer-trigger", "http://peer-trigger:9000"));
        var client = new OwnS3TestClient(factory.CreateClient());

        // Act / Assert — невалидная подпись ответа → 502 (состав не менялся)
        var response = await AdminJoinAsync(client, "http://fake-neighbor:9000");
        response.StatusCode.Should().Be(HttpStatusCode.BadGateway);
    }

    [Fact]
    public async Task JoinTrigger_InvalidEndpoint_400()
    {
        // Arrange — фабрика с фейк-соседом; мусорный endpoint в админ-теле
        using var factory = new PeerAuthTriggerFactory();
        var client = new OwnS3TestClient(factory.CreateClient());

        // Act
        var response = await AdminJoinAsync(client, "не-uri");

        // Assert — 400-конфигурация (не UriFormatException → 500); сосед НЕ вызывался
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .Should().Contain("error");
        factory.Handler.Calls.Should().Be(0);
    }

    [Fact]
    public async Task JoinTrigger_PeerEmptyAdvertised_502()
    {
        // Arrange — фейк-сосед: валидный 200 (серт + подпись), но advertisedEndpoint
        // пуст — сосед без своего OWNS3_REPLICATION_ADVERTISED_ENDPOINT
        using var factory = new PeerAuthTriggerFactory(_ =>
            JoinOkResponse(PeerAuthMaterial.TriggerCert, PeerAuthMaterial.TriggerKey,
                "peer-trigger", string.Empty));
        var client = new OwnS3TestClient(factory.CreateClient());

        // Act
        var response = await AdminJoinAsync(client, "http://fake-neighbor:9000");

        // Assert — 502 (вина соседа, spec §4.4 п.4); состав не менялся
        response.StatusCode.Should().Be(HttpStatusCode.BadGateway);
        var neighbors = await NeighborsOfAsync(client);
        neighbors.EnumerateArray()
            .Any(n => n.GetProperty("name").GetString() == "peer-trigger").Should().BeFalse();
    }

    [Fact]
    public async Task JoinTrigger_Success_AddsPeerTarget()
    {
        // Arrange — фейк-сосед: валидный 200 (серт peer-trigger против НАШЕГО CA + подпись)
        using var factory = new PeerAuthTriggerFactory(_ =>
            JoinOkResponse(PeerAuthMaterial.TriggerCert, PeerAuthMaterial.TriggerKey,
                "peer-trigger", "http://peer-trigger:9000"));
        var client = new OwnS3TestClient(factory.CreateClient());

        // Act
        var response = await AdminJoinAsync(client, "http://fake-neighbor:9000");

        // Assert — 200 {"joined":true,"peer":"peer-trigger"}; сосед в составе auth=key
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var body = JsonDocument.Parse(
            await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement;
        body.GetProperty("joined").GetBoolean().Should().BeTrue();
        body.GetProperty("peer").GetString().Should().Be("peer-trigger");
        var neighbors = await NeighborsOfAsync(client);
        var added = neighbors.EnumerateArray()
            .FirstOrDefault(n => n.GetProperty("name").GetString() == "peer-trigger");
        added.ValueKind.Should().Be(JsonValueKind.Object);
        added.GetProperty("auth").GetString().Should().Be("key");
    }

    // ── удаление / deny-list / статус ──

    [Fact]
    public async Task DeleteTarget_UnknownName_404()
    {
        // Act
        var response = await DeleteTargetAsync(Client, "ghost", revoke: false);

        // Assert — неизвестное имя → 404
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task DeleteTarget_LegacyRevoke_400()
    {
        // Arrange — legacy-запись peer-a в составе
        var client = Client;
        await EnsureKeyTargetAsync(client);

        // Act / Assert — revoke для legacy → 400 (креды отлучаются удалением записи)
        var response = await DeleteTargetAsync(client, "peer-a", revoke: true);
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task DeleteTarget_KeyRevoke_BlocksJoin_202()
    {
        // Arrange — СОБСТВЕННАЯ фабрика: peer-x в составе, отзыв
        using var factory = new PeerAuthAppFactory();
        var client = new OwnS3TestClient(factory.CreateClient());
        await EnsureKeyTargetOnAsync(client);

        // Act — delete с revoke=true
        var response = await DeleteTargetAsync(client, "peer-x", revoke: true);

        // Assert — 202; повторный входящий join блокирован deny-list'ом (403)
        response.StatusCode.Should().Be(HttpStatusCode.Accepted);
        (await SendJoinOnAsync(client, "http://peer-x:9000")).StatusCode
            .Should().Be(HttpStatusCode.Forbidden);
    }

    [Fact]
    public async Task DenyRemove_AllowsJoinAgain()
    {
        // Arrange — СОБСТВЕННАЯ фабрика: отзыв peer-x
        using var factory = new PeerAuthAppFactory();
        var client = new OwnS3TestClient(factory.CreateClient());
        await EnsureKeyTargetOnAsync(client);
        (await DeleteTargetAsync(client, "peer-x", revoke: true)).StatusCode
            .Should().Be(HttpStatusCode.Accepted);

        // Act — снятие отзыва, затем повторный join
        var denyRemove = await client.SendSignedAsync("DELETE", "/admin/replication/denylist/peer-x",
            credentials: OwnS3TestClient.Admin());
        var join = await SendJoinOnAsync(client, "http://peer-x:9000");

        // Assert — 202 на снятие; join снова 200 (spec §4.5)
        denyRemove.StatusCode.Should().Be(HttpStatusCode.Accepted);
        join.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task DenyList_Get_ListsEntries()
    {
        // Arrange — СОБСТВЕННАЯ фабрика: отзыв peer-x
        using var factory = new PeerAuthAppFactory();
        var client = new OwnS3TestClient(factory.CreateClient());
        await EnsureKeyTargetOnAsync(client);
        (await DeleteTargetAsync(client, "peer-x", revoke: true)).StatusCode
            .Should().Be(HttpStatusCode.Accepted);

        // Act
        var response = await client.SendSignedAsync("GET", "/admin/replication/denylist",
            credentials: OwnS3TestClient.Admin());

        // Assert — 200, запись {name, publicKeyPem, revokedAtUnixMs}
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var entries = JsonDocument.Parse(
            await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement
            .GetProperty("entries");
        entries.GetArrayLength().Should().Be(1);
        entries[0].GetProperty("name").GetString().Should().Be("peer-x");
        entries[0].GetProperty("publicKeyPem").GetString().Should()
            .Be(PeerAuthMaterial.PeerXCert.PublicKeyPem);
        entries[0].GetProperty("revokedAtUnixMs").GetInt64().Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task Status_PerTargetAuthField()
    {
        // Arrange — состав legacy + key через PUT admin-грани
        var client = Client;
        await EnsureKeyTargetAsync(client);

        // Act
        var neighbors = await NeighborsOfAsync(client);

        // Assert — auth per-target: legacy у peer-a, key у peer-x
        neighbors.EnumerateArray().First(n => n.GetProperty("name").GetString() == "peer-a")
            .GetProperty("auth").GetString().Should().Be("legacy");
        neighbors.EnumerateArray().First(n => n.GetProperty("name").GetString() == "peer-x")
            .GetProperty("auth").GetString().Should().Be("key");
    }

    [Fact]
    public async Task DeleteTarget_Plain_202_CursorKept()
    {
        // Arrange — СОБСТВЕННАЯ фабрика: peer-x в составе, курсор-файл на томе
        using var factory = new PeerAuthAppFactory();
        var client = new OwnS3TestClient(factory.CreateClient());
        await EnsureKeyTargetOnAsync(client);
        var cursorsDir = Path.Combine(factory.TempVolumeDir, ".owns3.sys", "replication", "cursors");
        Directory.CreateDirectory(cursorsDir);
        File.WriteAllText(Path.Combine(cursorsDir, "peer-x.json"), "{}");

        // Act — delete БЕЗ revoke
        var response = await DeleteTargetAsync(client, "peer-x", revoke: false);

        // Assert — 202; курсор соседа не тронут (возврат продолжится с места, §4.5);
        // deny-list пуст (отзыва не было)
        response.StatusCode.Should().Be(HttpStatusCode.Accepted);
        File.Exists(Path.Combine(cursorsDir, "peer-x.json")).Should().BeTrue();
        File.ReadAllText(Path.Combine(cursorsDir, "peer-x.json")).Should().Be("{}");
        Directory.GetFiles(Path.Combine(factory.TempVolumeDir, ".owns3.sys", "replication"))
            .Should().NotContain(f => f.EndsWith("denylist.json", StringComparison.Ordinal));
    }

    // ── хелперы «на указанном клиенте» (собственные фабрики кейсов) ──

    private static Task<HttpResponseMessage> SendJoinOnAsync(OwnS3TestClient client,
        string advertisedEndpoint) =>
        SendPeerOnAsync(client, "POST", "/replication/join",
            JsonSerializer.Serialize(new { advertisedEndpoint }, Json),
            PeerAuthMaterial.PeerXKey, "peer-x", CertBase64Of(PeerAuthMaterial.PeerXCert));

    private static async Task<HttpResponseMessage> SendPeerOnAsync(OwnS3TestClient client,
        string method, string pathAndQuery, string body, ECDsa signingKey, string credential,
        string? certificateBase64, DateTimeOffset? at = null)
    {
        var payloadHash = PayloadHash(body);
        var date = at ?? OwnS3AppFactory.HostTime;
        var peerDate = date.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture);
        var host = client.Http.BaseAddress!.Authority;
        var (authorization, _, _) = PeerRequestSigner.Sign(signingKey, credential, method,
            pathAndQuery, "",
            new List<(string, string)>
            {
                ("host", host),
                ("x-owns3-peer-date", peerDate),
                ("x-owns3-peer-content-sha256", payloadHash),
            }, payloadHash, date);
        using var request = new HttpRequestMessage(new HttpMethod(method), pathAndQuery);
        if (body.Length > 0)
            request.Content = new ByteArrayContent(Encoding.UTF8.GetBytes(body));
        request.Headers.TryAddWithoutValidation("x-owns3-peer-date", peerDate);
        request.Headers.TryAddWithoutValidation("x-owns3-peer-content-sha256", payloadHash);
        request.Headers.TryAddWithoutValidation("Authorization", authorization);
        if (certificateBase64 is not null)
            request.Headers.TryAddWithoutValidation("X-OwnS3-Peer-Certificate", certificateBase64);
        return await client.Http.SendAsync(request, TestContext.Current.CancellationToken);
    }

    private static async Task<HttpResponseMessage> EnsureKeyTargetOnAsync(OwnS3TestClient client)
    {
        var payload = JsonSerializer.Serialize(new
        {
            targets = new object[]
            {
                new
                {
                    name = "peer-a", endpoint = "http://127.0.0.1:9", auth = "legacy",
                    accessKey = OwnS3AppFactory.ReplicationAccessKey,
                    secretKey = OwnS3AppFactory.ReplicationSecretKey,
                },
                new
                {
                    name = "peer-b", endpoint = "http://127.0.0.1:9", auth = "legacy",
                    accessKey = ReplicationAppFactory.PeerBAccessKey,
                    secretKey = ReplicationAppFactory.PeerBSecretKey,
                },
                new
                {
                    name = "peer-c", endpoint = "http://127.0.0.1:9", auth = "legacy",
                    accessKey = ReplicationAppFactory.PeerCAccessKey,
                    secretKey = ReplicationAppFactory.PeerCSecretKey,
                },
                new
                {
                    name = "peer-x", endpoint = "http://peer-x:9000", auth = "key",
                    publicKeyPem = PeerAuthMaterial.PeerXCert.PublicKeyPem,
                },
            },
        }, Json);
        var response = await client.SendSignedAsync("PUT", "/admin/replication/targets",
            payloadString: PayloadHash(payload), body: Encoding.UTF8.GetBytes(payload),
            credentials: OwnS3TestClient.Admin());
        response.StatusCode.Should().Be(HttpStatusCode.Accepted);
        return response;
    }

    // Одиночная key-запись (кейсы конфликта/идемпотентности).
    private static async Task<HttpResponseMessage> PutKeyTargetAsync(OwnS3TestClient client,
        string name, string publicKeyPem, string endpoint = "http://peer-x:9000")
    {
        var payload = JsonSerializer.Serialize(new
        {
            targets = new object[]
            {
                new { name, endpoint, auth = "key", publicKeyPem },
            },
        }, Json);
        var response = await client.SendSignedAsync("PUT", "/admin/replication/targets",
            payloadString: PayloadHash(payload), body: Encoding.UTF8.GetBytes(payload),
            credentials: OwnS3TestClient.Admin());
        response.StatusCode.Should().Be(HttpStatusCode.Accepted);
        return response;
    }

    private static async Task<HttpResponseMessage> DeleteTargetAsync(OwnS3TestClient client,
        string name, bool revoke) =>
        await client.SendSignedAsync("DELETE", $"/admin/replication/targets/{name}{(revoke ? "?revoke=true" : "")}",
            credentials: OwnS3TestClient.Admin());

    private static async Task<HttpResponseMessage> AdminJoinAsync(OwnS3TestClient client, string endpoint)
    {
        var payload = JsonSerializer.Serialize(new { endpoint }, Json);
        return await client.SendSignedAsync("POST", "/admin/replication/join",
            payloadString: PayloadHash(payload), body: Encoding.UTF8.GetBytes(payload),
            credentials: OwnS3TestClient.Admin());
    }

    private static async Task<JsonElement> NeighborsOfAsync(OwnS3TestClient client)
    {
        var status = await client.SendSignedAsync("GET", "/admin/replication/status",
            credentials: OwnS3TestClient.Admin());
        status.StatusCode.Should().Be(HttpStatusCode.OK);
        return JsonDocument.Parse(
            await status.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement
            .GetProperty("neighbors");
    }

    // Валидный join-ответ фейк-соседа: серт + подпись тела его ключом (P1363).
    private static HttpResponseMessage JoinOkResponse(PeerCertificate neighborCert, ECDsa neighborKey,
        string name, string advertised)
    {
        var body = JsonSerializer.Serialize(new
        {
            name,
            advertisedEndpoint = advertised,
            certificate = CertBase64Of(neighborCert),
        }, Json);
        var response = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new ByteArrayContent(Encoding.UTF8.GetBytes(body)),
        };
        response.Headers.TryAddWithoutValidation("X-OwnS3-Peer-Signature",
            Convert.ToBase64String(neighborKey.SignData(Encoding.UTF8.GetBytes(body),
                HashAlgorithmName.SHA256, DSASignatureFormat.IeeeP1363FixedFieldConcatenation)));
        return response;
    }
}

// Фабрика join-trigger кейсов: фейк-сосед + флаги материала (без identity/trust/advertised).
public sealed class PeerAuthTriggerFactory : PeerAuthAppFactory
{
    public FakeNeighborHandler Handler { get; }

    private readonly bool _withIdentity, _withTrust, _withAdvertised;

    public PeerAuthTriggerFactory(Func<HttpRequestMessage, HttpResponseMessage>? reply = null,
        bool withIdentity = true, bool withTrust = true, bool withAdvertised = true)
        : base(new FakeNeighborHandler(reply ?? (_ => new HttpResponseMessage(HttpStatusCode.OK))))
    {
        _withIdentity = withIdentity;
        _withTrust = withTrust;
        _withAdvertised = withAdvertised;
        Handler = Neighbor!;
    }

    protected override PeerIdentity? IdentityMaterial => _withIdentity ? PeerAuthMaterial.Own : null;
    protected override PeerTrustAnchor? TrustMaterial => _withTrust ? PeerAuthMaterial.Ca : null;
    protected override string? AdvertisedEndpointMaterial => _withAdvertised ? "http://owns3-own:9000" : null;
}

// Fail-fast старта хоста на битом identity-материале (покрытие Task 11): ключ не
// соответствует публичному ключу сертификата — отказ старта по прецеденту
// FailFastScenarios (исключение всплывает из CreateClient; в Program — LogCritical).
public sealed class PeerAuthFailFastScenarios
{
    [Fact]
    public void FailFast_BrokenIdentityMaterial_RefusesStart()
    {
        // Arrange — фабрика: Identity.Certificate = серт own, PrivateKeyPem = ключ другой пары
        using var factory = new PeerAuthBrokenIdentityAppFactory();

        // Act + Assert — CreateClient() бросает (отказ старта); ни один endpoint не отвечает
        var act = () => factory.CreateClient();
        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void FailFast_BrokenDenyListFile_RefusesStart()
    {
        // Arrange — СУЩЕСТВУЮЩАЯ инсталляция: том с валидным volume.json (инициализирован
        // прошлым стартом) и битым вручную denylist.json; DenyList резолвится fail-fast
        // ПОСЛЕ storage-блока — том проходит валидацию хранения, отказ даёт deny-list
        var volumeDir = Path.Combine(Path.GetTempPath(), "owns3-peer-waf-" + Guid.NewGuid().ToString("N"));
        var replDir = Path.Combine(volumeDir, ".owns3.sys", "replication");
        Directory.CreateDirectory(replDir);
        File.WriteAllText(Path.Combine(volumeDir, ".owns3.sys", "volume.json"),
            """{"magic":"OWNS3-VOL","formatVersion":1,"volumeId":"t39-failfast","mode":"XL Single"}""");
        File.WriteAllText(Path.Combine(replDir, "denylist.json"), "{битый json");
        try
        {
            using var factory = new DenyListBrokenAppFactory(volumeDir);

            // Act + Assert — CreateClient() бросает (отказ старта, прецедент
            // FailFastScenarios), а не InvalidOperationException на каждый запрос
            var act = () => factory.CreateClient();
            act.Should().Throw<InvalidOperationException>();
        }
        finally
        {
            // Teardown: temp-том удаляется при любом исходе
            if (Directory.Exists(volumeDir))
                Directory.Delete(volumeDir, recursive: true);
        }
    }

    private sealed class PeerAuthBrokenIdentityAppFactory : WebApplicationFactory<Program>
    {
        private readonly string _volumeDir =
            Path.Combine(Path.GetTempPath(), "owns3-peer-waf-" + Guid.NewGuid().ToString("N"));

        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Testing");
            builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["OwnS3:DataDir"] = _volumeDir,
                ["OwnS3:Root:User"] = "root",
                ["OwnS3:Root:Password"] = "rootpassword",
                ["OwnS3:Replication:Identity:Certificate"] = PeerAuthMaterial.Own.CertificateJson,
                ["OwnS3:Replication:Identity:PrivateKeyPem"] = PeerAuthMaterial.ForeignCaPrivatePem,
            }));
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (disposing)
            {
                try
                {
                    if (Directory.Exists(_volumeDir))
                        Directory.Delete(_volumeDir, recursive: true);
                }
                catch (IOException)
                {
                    // хост мог ещё держать файлы — том в temp, ОС уберёт
                }
            }
        }
    }

    // Фабрика fail-fast битого deny-list: том (с заранее записанным битым
    // denylist.json) задаёт кейс; PeerAuth-материал не задаётся (не влияет).
    private sealed class DenyListBrokenAppFactory(string volumeDir) : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Testing");
            builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["OwnS3:DataDir"] = volumeDir,
                ["OwnS3:Root:User"] = "root",
                ["OwnS3:Root:Password"] = "rootpassword",
            }));
        }
    }
}
