using System.Net;
using System.Security.Cryptography;
using System.Text;
using OwnS3.Protocol.Auth;
using OwnS3.Protocol.PeerAuth;
using OwnS3.Protocol.Requests;
using OwnS3.Replication;
using OwnS3.Replication.Transport;

namespace OwnS3.UnitTests.Replication;

// Выбор схемы подписи транспорта по TargetRecord.Auth (t39, глава 07 §6/spec §4.7):
// key-записи — peer-подпись identity; legacy — SigV4 (регресс); key без identity —
// терминальный Failed ДО BuildRequest (не исключение сквозь границу).
public class ReplicationHttpClientTests
{
    private static readonly DateTimeOffset At = new(2026, 10, 14, 12, 0, 0, TimeSpan.Zero);

    // Identity отправителя (материал — PeerCertificateAuthority, как CLI).
    private static readonly PeerIdentity Identity = MakeIdentity();

    private static PeerIdentity MakeIdentity()
    {
        var (caJson, caPem) = PeerCertificateAuthority.CreateCa("owns3-ca");
        var ca = PeerTrustAnchor.FromJson(caJson);
        var (cert, pem) = PeerCertificateAuthority.IssueServer("mesh-a", ca, caPem, At);
        var key = ECDsa.Create();
        key.ImportFromPem(pem);
        return new PeerIdentity(cert.Name, cert, key, cert.ToJson());
    }

    private static readonly TargetRecord KeyTarget =
        new("peer-k", "http://neighbor:9000", TargetRecord.AuthKey,
            PublicKeyPem: Identity.Certificate.PublicKeyPem);

    private static readonly TargetRecord LegacyTarget =
        new("peer-a", "http://neighbor:9000", TargetRecord.AuthLegacy, "ak", "sk");

    private static ReplicationHttpClient Transport(FakeHandler handler) =>
        new(new HttpClient(handler), new FixedTimeProvider(At), Identity);

    [Fact]
    public async Task Ping_KeyTargetWithIdentity_SignedAsPeer()
    {
        // Arrange — key-цель; транспорт с identity; фейк отвечает 200
        var handler = new FakeHandler(_ => new HttpResponseMessage(HttpStatusCode.OK));

        // Act
        var result = await Transport(handler).PingAsync(KeyTarget, TimeSpan.FromSeconds(1),
            TestContext.Current.CancellationToken);

        // Assert — peer-подпись с именем identity; peer-заголовки на месте; x-amz-date НЕТ
        result.IsSuccess.Should().BeTrue();
        var auth = handler.Last!.Headers.GetValues("Authorization").Single();
        auth.Should().StartWith("OWNS3-PEER-ECC-P256-SHA256 Credential=mesh-a, ");
        handler.Last.Headers.Contains("x-owns3-peer-date").Should().BeTrue();
        handler.Last.Headers.Contains("x-owns3-peer-content-sha256").Should().BeTrue();
        handler.Last.Headers.Contains("x-amz-date").Should().BeFalse();
    }

    [Fact]
    public async Task Ping_LegacyTarget_SignedAsSigV4()
    {
        // Arrange — legacy-цель (регресс прежней механики)
        var handler = new FakeHandler(_ => new HttpResponseMessage(HttpStatusCode.OK));

        // Act
        var result = await Transport(handler).PingAsync(LegacyTarget, TimeSpan.FromSeconds(1),
            TestContext.Current.CancellationToken);

        // Assert — прежний формат AWS4-HMAC-SHA256
        result.IsSuccess.Should().BeTrue();
        handler.Last!.Headers.GetValues("Authorization").Single()
            .Should().StartWith("AWS4-HMAC-SHA256 Credential=ak/");
        handler.Last.Headers.Contains("x-amz-date").Should().BeTrue();
    }

    [Fact]
    public async Task Ping_KeyTargetWithoutIdentity_FailedPeerRejected()
    {
        // Arrange — key-цель, identity НЕТ; фейк считает вызовы
        var handler = new FakeHandler(_ => new HttpResponseMessage(HttpStatusCode.OK));
        var transport = new ReplicationHttpClient(new HttpClient(handler), new FixedTimeProvider(At));

        // Act
        var result = await transport.PingAsync(KeyTarget, TimeSpan.FromSeconds(1),
            TestContext.Current.CancellationToken);

        // Assert — терминальный Failed, HTTP не зовётся
        result.Error.Should().BeOfType<PeerRejectedException>();
        handler.Calls.Should().Be(0);
    }

    [Fact]
    public async Task Notifications_KeyTargetWithoutIdentity_FailedPeerRejected()
    {
        // Arrange — key-цель без identity; пре-чек ДО BuildRequest обязателен:
        // throw из BuildRequest вылетел бы сквозь метод (вызов вне try) и уронил
        // бы цикл нотификатора вместо Failed → бэкофф
        var handler = new FakeHandler(_ => new HttpResponseMessage(HttpStatusCode.OK));
        var transport = new ReplicationHttpClient(new HttpClient(handler), new FixedTimeProvider(At));

        // Act
        var result = await transport.SendNotificationsAsync(KeyTarget,
            new NotificationBatchRequest("test-host", []), TestContext.Current.CancellationToken);

        // Assert — Failed, HTTP не зовётся
        result.Error.Should().BeOfType<PeerRejectedException>();
        handler.Calls.Should().Be(0);
    }

    [Fact]
    public async Task Notifications_KeyTarget_BodyHashInHeader()
    {
        // Arrange — батч с телом; фейк отвечает ACK
        var handler = new FakeHandler(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new ByteArrayContent("{\"acceptedToSeqNo\":5}"u8.ToArray()),
        });

        // Act
        var result = await Transport(handler).SendNotificationsAsync(KeyTarget,
            new NotificationBatchRequest("test-host", []), TestContext.Current.CancellationToken);

        // Assert — x-owns3-peer-content-sha256 == hex SHA-256 фактического тела
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(5);
        var expectedHash = SigV4Core.HexSha256(handler.LastBody!);
        handler.Last!.Headers.GetValues("x-owns3-peer-content-sha256").Single()
            .Should().Be(expectedHash);
    }

    [Fact]
    public async Task KeySignature_PassesOwnVerifier()
    {
        // Arrange — key-цель с телом (notifications); ловим запрос фейком
        var handler = new FakeHandler(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new ByteArrayContent("{\"acceptedToSeqNo\":1}"u8.ToArray()),
        });

        // Act
        var result = await Transport(handler).SendNotificationsAsync(KeyTarget,
            new NotificationBatchRequest("test-host", []), TestContext.Current.CancellationToken);

        // Assert — roundtrip грани: заголовки пойманного запроса → PeerRequestVerifier → Ok
        result.IsSuccess.Should().BeTrue();
        var host = handler.Last!.RequestUri!.Authority;
        var model = new S3RequestModel
        {
            Method = "POST",
            RawPath = handler.Last.RequestUri.AbsolutePath,
            RawQuery = handler.Last.RequestUri.Query.TrimStart('?'),
            Headers = S3HeaderCollection.FromPairs(
                ("Authorization", handler.Last.Headers.GetValues("Authorization").Single()),
                ("x-owns3-peer-date", handler.Last.Headers.GetValues("x-owns3-peer-date").Single()),
                ("x-owns3-peer-content-sha256",
                    handler.Last.Headers.GetValues("x-owns3-peer-content-sha256").Single()),
                ("host", host)),
            Host = host,
            OpenBody = () => new MemoryStream(handler.LastBody!),
        };
        var verify = new PeerRequestVerifier(new FixedTimeProvider(At)).Verify(model, Identity.Key);
        verify.Should().BeOfType<PeerVerifyResult.Ok>();
    }

    // Фейк-хендлер: ловит запрос (заголовки/тело) и отвечает по программе.
    internal sealed class FakeHandler(Func<HttpRequestMessage, HttpResponseMessage> responder)
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
            return responder(request);
        }
    }
}
