using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using OwnS3.Protocol.Auth;
using OwnS3.Protocol.Errors;
using OwnS3.Protocol.PeerAuth;
using OwnS3.Protocol.Requests;
using OwnS3.Replication;
using OwnS3.Replication.Transport;

namespace OwnS3.UnitTests.Replication;

// Join-handshake (t39, глава 07 §2/spec §4.4): обе стороны на фейк-транспорте
// (исходящая — запрограммированные ответы HttpMessageHandler, прецедент
// ReplicationProber; входящая — прямые вызовы HandleIncoming). CA/deny/конфликт/
// идемпотентность, проксирование 403/409, 502 на невалидный ответ.
public sealed class JoinPeerServiceFixture : IDisposable
{
    public string Dir { get; } = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        // Teardown: temp-каталог удаляется при любом исходе
        if (Directory.Exists(Dir))
            Directory.Delete(Dir, recursive: true);
    }
}

public class JoinPeerServiceTests : IDisposable
{
    private readonly JoinPeerServiceFixture _fixture = new();

    // Фиксированное «сейчас» (skew ±15 мин верификатора — от At).
    private static readonly DateTimeOffset At = new(2026, 10, 14, 12, 0, 0, TimeSpan.Zero);

    // Тело join-запроса входящей стороны; advertised соседа для ассертов.
    private const string JoinBody = """{"advertisedEndpoint":"http://newcomer:9000"}""";
    private const string AdvertisedSelf = "http://mesh-a:9000";

    public void Dispose()
    {
        // Изоляция кейсов: temp-каталог на кейс
        if (Directory.Exists(_fixture.Dir))
            Directory.Delete(_fixture.Dir, recursive: true);
    }

    // ── материал инсталляции: CA + identity стороны (те же классы, что CLI) ──

    private static (PeerTrustAnchor Ca, string CaPrivatePem) NewCa(string name = "owns3-ca")
    {
        var (json, pem) = PeerCertificateAuthority.CreateCa(name);
        return (PeerTrustAnchor.FromJson(json), pem);
    }

    private static PeerIdentity MakeIdentity(string name, PeerTrustAnchor ca, string caPem,
        DateTimeOffset? at = null, int expiresDays = PeerCertificateAuthority.DefaultExpiresDays)
    {
        var (cert, pem) = PeerCertificateAuthority.IssueServer(name, ca, caPem, at ?? At, expiresDays);
        var key = ECDsa.Create();
        key.ImportFromPem(pem);
        return new PeerIdentity(cert.Name, cert, key, cert.ToJson());
    }

    private static string CertBase64(PeerIdentity side) =>
        Convert.ToBase64String(Encoding.UTF8.GetBytes(side.CertificateJson));

    // ── контур: targets/deny на temp-каталоге, сервис на фейк-хендлере ──

    private (ReplicationTargets Targets, DenyList Deny) NewState(TimeProvider tp)
    {
        var targets = ReplicationTargets.Load(_fixture.Dir, [], NullLogger<ReplicationTargets>.Instance);
        var deny = new DenyList(Path.Combine(_fixture.Dir, "denylist.json"), tp);
        return (targets, deny);
    }

    private static JoinPeerService Service(PeerIdentity? identity, PeerTrustAnchor? trust,
        ReplicationTargets targets, DenyList deny, FakeHandler? handler = null,
        string? advertised = AdvertisedSelf, TimeProvider? tp = null, HttpClient? http = null) =>
        new(identity, trust, targets, deny,
            http ?? new HttpClient(handler ?? new FakeHandler((_, _) => new HttpResponseMessage(HttpStatusCode.OK))),
            tp ?? new FixedTimeProvider(At),
            NullLogger<JoinPeerService>.Instance, advertised);

    // ── тело join-ответа соседа: та же stable-сериализация, что у сервиса ──

    private sealed record JoinResponseBody(string Name, string AdvertisedEndpoint, string Certificate);

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private static string ResponseBodyJson(string name, string advertised, string certificateBase64) =>
        JsonSerializer.Serialize(new JoinResponseBody(name, advertised, certificateBase64), JsonOpts);

    private static HttpResponseMessage JoinOkResponse(PeerCertificate peerCert, ECDsa signKey,
        string name, string advertised)
    {
        var body = ResponseBodyJson(name, advertised,
            Convert.ToBase64String(Encoding.UTF8.GetBytes(peerCert.ToJson())));
        var response = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new ByteArrayContent(Encoding.UTF8.GetBytes(body)),
        };
        response.Headers.TryAddWithoutValidation("X-OwnS3-Peer-Signature",
            Convert.ToBase64String(signKey.SignData(Encoding.UTF8.GetBytes(body),
                HashAlgorithmName.SHA256, DSASignatureFormat.IeeeP1363FixedFieldConcatenation)));
        return response;
    }

    private static HttpResponseMessage JsonStatusResponse(HttpStatusCode status, string json) =>
        new(status) { Content = new ByteArrayContent(Encoding.UTF8.GetBytes(json)) };

    // Подписанная модель входящего join-запроса (ключ серта пира — по умолчанию).
    private static S3RequestModel SignedJoinModel(ECDsa signingKey, string peerName)
    {
        var payloadHash = SigV4Core.HexSha256(Encoding.UTF8.GetBytes(JoinBody));
        var (auth, peerDate, _) = PeerRequestSigner.Sign(signingKey, peerName, "POST",
            "/replication/join", "",
            [("host", "neighbor:9000"),
                ("x-owns3-peer-date", PeerDateOf(At)),
                ("x-owns3-peer-content-sha256", payloadHash)],
            payloadHash, At);
        return new S3RequestModel
        {
            Method = "POST",
            RawPath = "/replication/join",
            RawQuery = "",
            Headers = S3HeaderCollection.FromPairs(
                ("Authorization", auth),
                ("x-owns3-peer-date", peerDate),
                ("x-owns3-peer-content-sha256", payloadHash),
                ("host", "neighbor:9000")),
            Host = "neighbor:9000",
            OpenBody = () => new MemoryStream(Encoding.UTF8.GetBytes(JoinBody)),
        };
    }

    private static string PeerDateOf(DateTimeOffset at) =>
        at.ToString("yyyyMMdd'T'HHmmss'Z'", System.Globalization.CultureInfo.InvariantCulture);

    // ── исходящая сторона (JoinAsync) ──

    [Fact]
    public async Task JoinAsync_Success_AddsTargetAndReturnsPeer()
    {
        // Arrange — CA инсталляции; мы (mesh-a) и сосед (peer-b) — его сертификаты;
        // фейк отвечает валидным 200 (серт + подпись тела ключом соседа)
        var (ca, caPem) = NewCa();
        var me = MakeIdentity("mesh-a", ca, caPem);
        var peer = MakeIdentity("peer-b", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var (targets, deny) = NewState(tp);
        var handler = new FakeHandler((_, _) => JoinOkResponse(peer.Certificate, peer.Key,
            "peer-b", "http://peer-b:9000"));
        var service = Service(me, ca, targets, deny, handler, tp: tp);

        // Act
        var result = await service.JoinAsync("http://neighbor:9000", TestContext.Current.CancellationToken);

        // Assert — Success; targets дополнен key-записью с публичным ключом серта
        result.IsSuccess.Should().BeTrue();
        result.Value.Name.Should().Be("peer-b");
        result.Value.Endpoint.Should().Be("http://peer-b:9000");
        var added = targets.Snapshot.Should().ContainSingle().Which;
        added.Auth.Should().Be(TargetRecord.AuthKey);
        added.PublicKeyPem.Should().Be(peer.Certificate.PublicKeyPem);
    }

    [Fact]
    public async Task JoinAsync_NoIdentity_400config()
    {
        // Arrange — identity не задан; фейк считает вызовы
        var (ca, caPem) = NewCa();
        var peer = MakeIdentity("peer-b", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var (targets, deny) = NewState(tp);
        var handler = new FakeHandler((_, _) => JoinOkResponse(peer.Certificate, peer.Key,
            "peer-b", "http://peer-b:9000"));
        var service = Service(null, ca, targets, deny, handler, tp: tp);

        // Act
        var result = await service.JoinAsync("http://neighbor:9000", TestContext.Current.CancellationToken);

        // Assert — Failed(JoinBadConfigurationException); HTTP не зовётся
        result.Error.Should().BeOfType<JoinBadConfigurationException>();
        handler.Calls.Should().Be(0);
    }

    [Fact]
    public async Task JoinAsync_NoAdvertised_400config()
    {
        // Arrange — advertised endpoint не задан
        var (ca, caPem) = NewCa();
        var me = MakeIdentity("mesh-a", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var (targets, deny) = NewState(tp);
        var handler = new FakeHandler((_, _) => new HttpResponseMessage(HttpStatusCode.OK));
        var service = Service(me, ca, targets, deny, handler, advertised: null, tp: tp);

        // Act
        var result = await service.JoinAsync("http://neighbor:9000", TestContext.Current.CancellationToken);

        // Assert — Failed(JoinBadConfigurationException); HTTP не зовётся
        result.Error.Should().BeOfType<JoinBadConfigurationException>();
        handler.Calls.Should().Be(0);
    }

    [Fact]
    public async Task JoinAsync_NoTrust_400config()
    {
        // Arrange — trust-anchor не задан (spec §4.4 п.1: без него инициатор
        // не проверит сертификат ответа соседа — fail-fast)
        var (ca, caPem) = NewCa();
        var me = MakeIdentity("mesh-a", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var (targets, deny) = NewState(tp);
        var handler = new FakeHandler((_, _) => new HttpResponseMessage(HttpStatusCode.OK));
        var service = Service(me, null, targets, deny, handler, tp: tp);

        // Act
        var result = await service.JoinAsync("http://neighbor:9000", TestContext.Current.CancellationToken);

        // Assert — Failed(JoinBadConfigurationException); HTTP не зовётся
        result.Error.Should().BeOfType<JoinBadConfigurationException>();
        handler.Calls.Should().Be(0);
    }

    [Fact]
    public async Task JoinAsync_Peer403_Proxied()
    {
        // Arrange — сосед отвечает 403 (ручной режим состава)
        var (ca, caPem) = NewCa();
        var me = MakeIdentity("mesh-a", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var (targets, deny) = NewState(tp);
        var handler = new FakeHandler((_, _) => JsonStatusResponse(HttpStatusCode.Forbidden,
            """{"error":"trust-anchor не задан"}"""));
        var service = Service(me, ca, targets, deny, handler, tp: tp);

        // Act
        var result = await service.JoinAsync("http://neighbor:9000", TestContext.Current.CancellationToken);

        // Assert — отказ соседа проксируется тем же кодом (403)
        result.Error.Should().BeOfType<JoinRefusedException>();
    }

    [Fact]
    public async Task JoinAsync_Peer409_Proxied()
    {
        // Arrange — сосед отвечает 409 (конфликт имени)
        var (ca, caPem) = NewCa();
        var me = MakeIdentity("mesh-a", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var (targets, deny) = NewState(tp);
        var handler = new FakeHandler((_, _) => JsonStatusResponse(HttpStatusCode.Conflict,
            """{"error":"имя числится с другим ключом"}"""));
        var service = Service(me, ca, targets, deny, handler, tp: tp);

        // Act
        var result = await service.JoinAsync("http://neighbor:9000", TestContext.Current.CancellationToken);

        // Assert — конфликт соседа проксируется (409)
        result.Error.Should().BeOfType<JoinConflictException>();
    }

    [Fact]
    public async Task JoinAsync_InvalidSignature_502()
    {
        // Arrange — 200 с телом, подписанным ДРУГИМ ключом
        var (ca, caPem) = NewCa();
        var me = MakeIdentity("mesh-a", ca, caPem);
        var peer = MakeIdentity("peer-b", ca, caPem);
        var stranger = MakeIdentity("stranger", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var (targets, deny) = NewState(tp);
        var handler = new FakeHandler((_, _) => JoinOkResponse(peer.Certificate, stranger.Key,
            "peer-b", "http://peer-b:9000"));
        var service = Service(me, ca, targets, deny, handler, tp: tp);

        // Act
        var result = await service.JoinAsync("http://neighbor:9000", TestContext.Current.CancellationToken);

        // Assert — 502; состав не менялся
        result.Error.Should().BeOfType<JoinBadGatewayException>();
        targets.Snapshot.Should().BeEmpty();
    }

    [Fact]
    public async Task JoinAsync_CertAgainstOtherCa_502()
    {
        // Arrange — серт соседа от чужого CA (инициатор верифицирует против СВОЕГО trust)
        var (ca, caPem) = NewCa();
        var me = MakeIdentity("mesh-a", ca, caPem);
        var (foreignCa, foreignCaPem) = NewCa("other-ca");
        var peer = MakeIdentity("peer-b", foreignCa, foreignCaPem);
        var tp = new FixedTimeProvider(At);
        var (targets, deny) = NewState(tp);
        var handler = new FakeHandler((_, _) => JoinOkResponse(peer.Certificate, peer.Key,
            "peer-b", "http://peer-b:9000"));
        var service = Service(me, ca, targets, deny, handler, tp: tp);

        // Act
        var result = await service.JoinAsync("http://neighbor:9000", TestContext.Current.CancellationToken);

        // Assert — 502 (join не состоялся, состав не менялся)
        result.Error.Should().BeOfType<JoinBadGatewayException>();
        targets.Snapshot.Should().BeEmpty();
    }

    [Fact]
    public async Task JoinAsync_Transport_502()
    {
        // Arrange — фейк кидает HttpRequestException (обрыв транспорта)
        var (ca, caPem) = NewCa();
        var me = MakeIdentity("mesh-a", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var (targets, deny) = NewState(tp);
        var handler = new FakeHandler((_, _) => throw new HttpRequestException("connection refused"));
        var service = Service(me, ca, targets, deny, handler, tp: tp);

        // Act
        var result = await service.JoinAsync("http://neighbor:9000", TestContext.Current.CancellationToken);

        // Assert — 502
        result.Error.Should().BeOfType<JoinBadGatewayException>();
    }

    [Fact]
    public async Task JoinAsync_InvalidEndpoint_400config()
    {
        // Arrange — мусорный endpoint из админ-тела (Uri.TryCreate не проходит)
        var (ca, caPem) = NewCa();
        var me = MakeIdentity("mesh-a", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var (targets, deny) = NewState(tp);
        var handler = new FakeHandler((_, _) => new HttpResponseMessage(HttpStatusCode.OK));
        var service = Service(me, ca, targets, deny, handler, tp: tp);

        // Act
        var result = await service.JoinAsync("не-uri", TestContext.Current.CancellationToken);

        // Assert — Failed(JoinBadConfigurationException) (400-конфигурация, не 500);
        // HTTP не зовётся
        result.Error.Should().BeOfType<JoinBadConfigurationException>();
        handler.Calls.Should().Be(0);
    }

    [Fact]
    public async Task JoinAsync_PeerAdvertisedInvalid_502_CompositionUnchanged()
    {
        // Arrange — валидный 200 соседа, но advertisedEndpoint в ответе невалиден
        // (сосед без своего OWNS3_REPLICATION_ADVERTISED_ENDPOINT)
        var (ca, caPem) = NewCa();
        var me = MakeIdentity("mesh-a", ca, caPem);
        var peer = MakeIdentity("peer-b", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var (targets, deny) = NewState(tp);
        var handler = new FakeHandler((_, _) => JoinOkResponse(peer.Certificate, peer.Key,
            "peer-b", "не-uri"));
        var service = Service(me, ca, targets, deny, handler, tp: tp);

        // Act
        var result = await service.JoinAsync("http://neighbor:9000", TestContext.Current.CancellationToken);

        // Assert — 502 (вина соседа); состав не менялся (Validate бросает до персиста)
        result.Error.Should().BeOfType<JoinBadGatewayException>();
        targets.Snapshot.Should().BeEmpty();
    }

    [Fact]
    public async Task JoinAsync_TransportTimeoutCancellation_502()
    {
        // Arrange — фейк кидает TaskCanceledException (таймаут транспорта) при
        // НЕ отменённом внешнем ct
        var (ca, caPem) = NewCa();
        var me = MakeIdentity("mesh-a", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var (targets, deny) = NewState(tp);
        var handler = new FakeHandler((_, _) => throw new TaskCanceledException("timeout"));
        var service = Service(me, ca, targets, deny, handler, tp: tp);

        // Act
        var result = await service.JoinAsync("http://neighbor:9000", TestContext.Current.CancellationToken);

        // Assert — 502 (не проброс): таймаут — исход операции, а не graceful stop
        result.Error.Should().BeOfType<JoinBadGatewayException>();
    }

    [Fact]
    public void Service_CapsDefaultHttpClientTimeout_At10s()
    {
        // Arrange — HttpClient с дефолтным таймаутом 100 с
        using var http = new HttpClient();
        var (ca, caPem) = NewCa();
        var me = MakeIdentity("mesh-a", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var (targets, deny) = NewState(tp);

        // Act — сервис конструируется с этим HttpClient (кап — самим сервисом,
        // по образцу ReplicationHttpClient)
        _ = Service(me, ca, targets, deny, tp: tp, http: http);

        // Assert — таймаут капнут 10 с: «зависший» сосед не держит admin-join 100 с
        http.Timeout.Should().Be(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task JoinAsync_RequestCarriesCertAndSignature()
    {
        // Arrange — сосед отвечает 200; ловим исходящий запрос
        var (ca, caPem) = NewCa();
        var me = MakeIdentity("mesh-a", ca, caPem);
        var peer = MakeIdentity("peer-b", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var (targets, deny) = NewState(tp);
        var handler = new FakeHandler((_, _) => JoinOkResponse(peer.Certificate, peer.Key,
            "peer-b", "http://peer-b:9000"));
        var service = Service(me, ca, targets, deny, handler, tp: tp);

        // Act
        await service.JoinAsync("http://neighbor:9000", TestContext.Current.CancellationToken);

        // Assert — маршрут, peer-подпись с НАШИМ именем, наш сертификат,
        // тело содержит advertised endpoint
        handler.Last.Should().NotBeNull();
        handler.Last!.RequestUri!.PathAndQuery.Should().Be("/replication/join");
        var auth = handler.Last.Headers.GetValues("Authorization").Single();
        auth.Should().StartWith("OWNS3-PEER-ECC-P256-SHA256 Credential=mesh-a, ");
        handler.Last.Headers.GetValues("X-OwnS3-Peer-Certificate").Single()
            .Should().Be(CertBase64(me));
        Encoding.UTF8.GetString(handler.LastBody!).Should().Contain(AdvertisedSelf);
    }

    // ── входящая сторона (HandleIncoming) ──

    [Fact]
    public void Incoming_ValidCert_AddsKeyRecord()
    {
        // Arrange — принимающий (mesh-a) с CA; пир (peer-b) шлёт подписанный join
        var (ca, caPem) = NewCa();
        var me = MakeIdentity("mesh-a", ca, caPem);
        var peer = MakeIdentity("peer-b", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var (targets, deny) = NewState(tp);
        var service = Service(me, ca, targets, deny, tp: tp);

        // Act
        var result = service.HandleIncoming(SignedJoinModel(peer.Key, peer.Name),
            CertBase64(peer), "http://newcomer:9000");

        // Assert — Success; запись {name серта, endpoint: advert, auth=key}
        result.IsSuccess.Should().BeTrue();
        var added = targets.Snapshot.Should().ContainSingle().Which;
        added.Name.Should().Be("peer-b");
        added.Endpoint.Should().Be("http://newcomer:9000");
        added.Auth.Should().Be(TargetRecord.AuthKey);
        added.PublicKeyPem.Should().Be(peer.Certificate.PublicKeyPem);
    }

    [Fact]
    public void Incoming_NoTrust_403()
    {
        // Arrange — trust-anchor не задан (ручной режим состава)
        var (ca, caPem) = NewCa();
        var me = MakeIdentity("mesh-a", ca, caPem);
        var peer = MakeIdentity("peer-b", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var (targets, deny) = NewState(tp);
        var service = Service(me, null, targets, deny, tp: tp);

        // Act
        var result = service.HandleIncoming(SignedJoinModel(peer.Key, peer.Name),
            CertBase64(peer), "http://newcomer:9000");

        // Assert — 403-семантика
        result.Error.Should().BeOfType<JoinRefusedException>();
    }

    [Fact]
    public void Incoming_NoIdentity_403()
    {
        // Arrange — identity не задан: нечем подписать ответ
        var (ca, caPem) = NewCa();
        var peer = MakeIdentity("peer-b", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var (targets, deny) = NewState(tp);
        var service = Service(null, ca, targets, deny, tp: tp);

        // Act
        var result = service.HandleIncoming(SignedJoinModel(peer.Key, peer.Name),
            CertBase64(peer), "http://newcomer:9000");

        // Assert — 403-семантика
        result.Error.Should().BeOfType<JoinRefusedException>();
    }

    [Fact]
    public void Incoming_OtherCa_403()
    {
        // Arrange — серт пира от чужого CA
        var (ca, caPem) = NewCa();
        var me = MakeIdentity("mesh-a", ca, caPem);
        var (foreignCa, foreignCaPem) = NewCa("other-ca");
        var peer = MakeIdentity("peer-b", foreignCa, foreignCaPem);
        var tp = new FixedTimeProvider(At);
        var (targets, deny) = NewState(tp);
        var service = Service(me, ca, targets, deny, tp: tp);

        // Act
        var result = service.HandleIncoming(SignedJoinModel(peer.Key, peer.Name),
            CertBase64(peer), "http://newcomer:9000");

        // Assert
        result.Error.Should().BeOfType<JoinRefusedException>();
    }

    [Fact]
    public void Incoming_Expired_403()
    {
        // Arrange — истёкший серт пира (выпуск 2 дня назад, срок 1 день)
        var (ca, caPem) = NewCa();
        var me = MakeIdentity("mesh-a", ca, caPem);
        var peer = MakeIdentity("peer-b", ca, caPem,
            at: At.AddDays(-2), expiresDays: 1);
        var tp = new FixedTimeProvider(At);
        var (targets, deny) = NewState(tp);
        var service = Service(me, ca, targets, deny, tp: tp);

        // Act
        var result = service.HandleIncoming(SignedJoinModel(peer.Key, peer.Name),
            CertBase64(peer), "http://newcomer:9000");

        // Assert
        result.Error.Should().BeOfType<JoinRefusedException>();
    }

    [Fact]
    public void Incoming_DeniedByName_403()
    {
        // Arrange — отзыв по имени (с другим ключом)
        var (ca, caPem) = NewCa();
        var me = MakeIdentity("mesh-a", ca, caPem);
        var peer = MakeIdentity("peer-b", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var (targets, deny) = NewState(tp);
        deny.Add("peer-b", "-----BEGIN PUBLIC KEY-----\nOTHER\n-----END PUBLIC KEY-----\n");
        var service = Service(me, ca, targets, deny, tp: tp);

        // Act
        var result = service.HandleIncoming(SignedJoinModel(peer.Key, peer.Name),
            CertBase64(peer), "http://newcomer:9000");

        // Assert
        result.Error.Should().BeOfType<JoinRefusedException>();
    }

    [Fact]
    public void Incoming_DeniedByKey_403()
    {
        // Arrange — отзыв по публичному ключу (под другим именем)
        var (ca, caPem) = NewCa();
        var me = MakeIdentity("mesh-a", ca, caPem);
        var peer = MakeIdentity("peer-b", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var (targets, deny) = NewState(tp);
        deny.Add("другое-имя", peer.Certificate.PublicKeyPem);
        var service = Service(me, ca, targets, deny, tp: tp);

        // Act
        var result = service.HandleIncoming(SignedJoinModel(peer.Key, peer.Name),
            CertBase64(peer), "http://newcomer:9000");

        // Assert
        result.Error.Should().BeOfType<JoinRefusedException>();
    }

    [Fact]
    public void Incoming_RequestSignatureMismatch_403signature()
    {
        // Arrange — join-запрос подписан ЧУЖИМ ключом (серт при этом валидный пира)
        var (ca, caPem) = NewCa();
        var me = MakeIdentity("mesh-a", ca, caPem);
        var peer = MakeIdentity("peer-b", ca, caPem);
        var stranger = MakeIdentity("stranger", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var (targets, deny) = NewState(tp);
        var service = Service(me, ca, targets, deny, tp: tp);

        // Act
        var result = service.HandleIncoming(SignedJoinModel(stranger.Key, peer.Name),
            CertBase64(peer), "http://newcomer:9000");

        // Assert — отказ c исходным кодом каталога подписи (грань отдаст 403
        // SignatureDoesNotMatch, а не AccessDenied)
        var refused = result.Error.Should().BeOfType<JoinRefusedException>().Which;
        refused.ErrorCode.Should().Be(S3ErrorCode.SignatureDoesNotMatch);
    }

    [Fact]
    public void Incoming_SameNameSameKey_IdempotentEndpointUpdate()
    {
        // Arrange — пир уже числится (старый endpoint, тот же ключ)
        var (ca, caPem) = NewCa();
        var me = MakeIdentity("mesh-a", ca, caPem);
        var peer = MakeIdentity("peer-b", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var (targets, deny) = NewState(tp);
        targets.AddOrReplace(new TargetRecord("peer-b", "http://old:9000", TargetRecord.AuthKey,
            PublicKeyPem: peer.Certificate.PublicKeyPem));

        // Act — повторный join с новым advertised endpoint
        var service = Service(me, ca, targets, deny, tp: tp);
        var result = service.HandleIncoming(SignedJoinModel(peer.Key, peer.Name),
            CertBase64(peer), "http://newcomer:9000");

        // Assert — идемпотентное обновление: записей 1, endpoint обновлён
        result.IsSuccess.Should().BeTrue();
        var record = targets.Snapshot.Should().ContainSingle().Which;
        record.Endpoint.Should().Be("http://newcomer:9000");
        record.PublicKeyPem.Should().Be(peer.Certificate.PublicKeyPem);
    }

    [Fact]
    public void Incoming_SameNameOtherKey_409()
    {
        // Arrange — имя числится с ДРУГИМ публичным ключом (второй выпуск серта)
        var (ca, caPem) = NewCa();
        var me = MakeIdentity("mesh-a", ca, caPem);
        var peer = MakeIdentity("peer-b", ca, caPem);
        var reissued = MakeIdentity("peer-b", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var (targets, deny) = NewState(tp);
        targets.AddOrReplace(new TargetRecord("peer-b", "http://old:9000", TargetRecord.AuthKey,
            PublicKeyPem: reissued.Certificate.PublicKeyPem));
        var service = Service(me, ca, targets, deny, tp: tp);

        // Act
        var result = service.HandleIncoming(SignedJoinModel(peer.Key, peer.Name),
            CertBase64(peer), "http://newcomer:9000");

        // Assert — 409
        result.Error.Should().BeOfType<JoinConflictException>();
    }

    [Fact]
    public void Incoming_ResponseSigned()
    {
        // Arrange — принимающий с identity; валидный join пира
        var (ca, caPem) = NewCa();
        var me = MakeIdentity("mesh-a", ca, caPem);
        var peer = MakeIdentity("peer-b", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var (targets, deny) = NewState(tp);
        var service = Service(me, ca, targets, deny, tp: tp);

        // Act
        var result = service.HandleIncoming(SignedJoinModel(peer.Key, peer.Name),
            CertBase64(peer), "http://newcomer:9000");

        // Assert — подпись тела (та же сериализация name+advertised+certificate)
        // проверяется публичным ключом НАШЕГО identity (spec §4.4 п.3)
        var accept = result.Value;
        var expectedBody = ResponseBodyJson(accept.Name, accept.AdvertisedEndpoint,
            accept.CertificateBase64);
        using var myKey = ECDsa.Create();
        myKey.ImportFromPem(me.Certificate.PublicKeyPem);
        myKey.VerifyData(Encoding.UTF8.GetBytes(expectedBody),
            Convert.FromBase64String(accept.SignatureBase64),
            HashAlgorithmName.SHA256, DSASignatureFormat.IeeeP1363FixedFieldConcatenation)
            .Should().BeTrue();
    }

    [Fact]
    public void Incoming_DoesNotTouchCursors()
    {
        // Arrange — каталог cursors/ в состоянии репликации; валидный join
        var (ca, caPem) = NewCa();
        var me = MakeIdentity("mesh-a", ca, caPem);
        var peer = MakeIdentity("peer-b", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var (targets, deny) = NewState(tp);
        var cursorsDir = Path.Combine(_fixture.Dir, "cursors");
        Directory.CreateDirectory(cursorsDir);
        var service = Service(me, ca, targets, deny, tp: tp);

        // Act
        var result = service.HandleIncoming(SignedJoinModel(peer.Key, peer.Name),
            CertBase64(peer), "http://newcomer:9000");

        // Assert — join курсоры не меняет (spec §4.4): файлов в cursors/ нет
        result.IsSuccess.Should().BeTrue();
        Directory.GetFiles(cursorsDir).Should().BeEmpty();
    }

    // Фейк-хендлер: записи запроса (URI/заголовки/тело) + счётчик вызовов +
    // запрограммированный ответ (прецедент тестируемости «без хоста»).
    internal sealed class FakeHandler(Func<HttpRequestMessage, byte[]?, HttpResponseMessage> responder)
        : HttpMessageHandler
    {
        public HttpRequestMessage? Last;
        public byte[]? LastBody;
        public int Calls;

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref Calls);
            Last = request;
            LastBody = request.Content is null
                ? null
                : await request.Content.ReadAsByteArrayAsync(cancellationToken);
            return responder(request, LastBody);
        }
    }
}
