using System.Security.Cryptography;
using System.Text;
using OwnS3.Protocol.Auth;
using OwnS3.Protocol.Errors;
using OwnS3.Protocol.PeerAuth;
using OwnS3.Protocol.Requests;

namespace OwnS3.UnitTests.PeerAuth;

// Схема подписи запроса OWNS3-PEER-ECC-P256-SHA256 (глава 07 §6, spec §4.2):
// roundtrip через СОБСТВЕННЫЙ PeerRequestVerifier, skew ±15 мин,
// malformed-отказы, формат подписи P1363 base64.
public class PeerRequestSignerTests
{
    // Пара ключей схемы (подпись/проверка) и чужой ключ для негативов.
    private static readonly ECDsa Key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
    private static readonly ECDsa OtherKey = ECDsa.Create(ECCurve.NamedCurves.nistP256);
    private static readonly DateTimeOffset At = new(2026, 10, 14, 12, 0, 0, TimeSpan.Zero);

    private const string PeerName = "peer-a";

    [Fact]
    public void Sign_VerifyRoundtrip_Ok()
    {
        // Arrange — POST /replication/notifications с JSON-телом
        var payload = SigV4Core.HexSha256("{\"originHostId\":\"mesh-a\",\"events\":[]}"u8.ToArray());

        // Act — подпись и проверка собственным верификатором
        var (auth, peerDate, contentSha) = PeerRequestSigner.Sign(Key, PeerName, "POST",
            "/replication/notifications", "",
            [("host", "neighbor:9000"), ("x-owns3-peer-date", PeerDateOf(At)),
                ("x-owns3-peer-content-sha256", payload)],
            payload, At);
        var result = new PeerRequestVerifier(new FixedTimeProvider(At))
            .Verify(BuildModel("POST", "/replication/notifications", "", auth, peerDate, contentSha), Key);

        // Assert — Ok с именем пира из Credential
        var ok = result.Should().BeOfType<PeerVerifyResult.Ok>().Which;
        ok.PeerName.Should().Be(PeerName);
    }

    [Fact]
    public void Sign_GetWithQuery_Roundtrip()
    {
        // Arrange — GET с query (листинг соседа)
        var payload = SigV4Core.EmptySha256;

        // Act
        var (auth, peerDate, contentSha) = PeerRequestSigner.Sign(Key, PeerName, "GET",
            "/replication/listing", "bucket=b&marker=m",
            [("host", "neighbor:9000"), ("x-owns3-peer-date", PeerDateOf(At)),
                ("x-owns3-peer-content-sha256", payload)],
            payload, At);
        var result = new PeerRequestVerifier(new FixedTimeProvider(At))
            .Verify(BuildModel("GET", "/replication/listing", "bucket=b&marker=m", auth, peerDate, contentSha), Key);

        // Assert — canonical query согласован между signer и verifier
        result.Should().BeOfType<PeerVerifyResult.Ok>();
    }

    [Fact]
    public void Verify_TamperedPayload_SignatureDoesNotMatch()
    {
        // Arrange — подписали hash-A, в модели заголовок с hash-B
        var payloadA = SigV4Core.HexSha256("body-A"u8.ToArray());
        var payloadB = SigV4Core.HexSha256("body-B"u8.ToArray());
        var (auth, peerDate, _) = PeerRequestSigner.Sign(Key, PeerName, "POST", "/replication/notifications", "",
            [("host", "neighbor:9000"), ("x-owns3-peer-date", PeerDateOf(At)),
                ("x-owns3-peer-content-sha256", payloadA)],
            payloadA, At);

        // Act — проверка с подменённым хэшем тела
        var result = new PeerRequestVerifier(new FixedTimeProvider(At))
            .Verify(BuildModel("POST", "/replication/notifications", "", auth, peerDate, payloadB), Key);

        // Assert — 403 SignatureDoesNotMatch
        var fail = result.Should().BeOfType<PeerVerifyResult.Fail>().Which;
        fail.Code.Should().Be(S3ErrorCode.SignatureDoesNotMatch);
    }

    [Fact]
    public void Verify_TamperedPath_SignatureDoesNotMatch()
    {
        // Arrange — подпись пути A, проверка пути B
        var payload = SigV4Core.EmptySha256;
        var (auth, peerDate, contentSha) = PeerRequestSigner.Sign(Key, PeerName, "GET", "/replication/listing", "",
            [("host", "neighbor:9000"), ("x-owns3-peer-date", PeerDateOf(At)),
                ("x-owns3-peer-content-sha256", payload)],
            payload, At);

        // Act — путь подменён
        var result = new PeerRequestVerifier(new FixedTimeProvider(At))
            .Verify(BuildModel("GET", "/replication/object/b/k", "", auth, peerDate, contentSha), Key);

        // Assert — 403 SignatureDoesNotMatch
        var fail = result.Should().BeOfType<PeerVerifyResult.Fail>().Which;
        fail.Code.Should().Be(S3ErrorCode.SignatureDoesNotMatch);
    }

    [Fact]
    public void Verify_Skew_TooSkewed()
    {
        // Arrange — подпись в At
        var payload = SigV4Core.EmptySha256;
        var (auth, peerDate, contentSha) = PeerRequestSigner.Sign(Key, PeerName, "GET", "/replication/listing", "",
            [("host", "neighbor:9000"), ("x-owns3-peer-date", PeerDateOf(At)),
                ("x-owns3-peer-content-sha256", payload)],
            payload, At);

        // Act — верификатор на 16 минут позже
        var result = new PeerRequestVerifier(new FixedTimeProvider(At.AddMinutes(16)))
            .Verify(BuildModel("GET", "/replication/listing", "", auth, peerDate, contentSha), Key);

        // Assert — 403 RequestTimeTooSkewed
        var fail = result.Should().BeOfType<PeerVerifyResult.Fail>().Which;
        fail.Code.Should().Be(S3ErrorCode.RequestTimeTooSkewed);
    }

    [Fact]
    public void Verify_Skew_Within15Min_Ok()
    {
        // Arrange — подпись в At
        var payload = SigV4Core.EmptySha256;
        var (auth, peerDate, contentSha) = PeerRequestSigner.Sign(Key, PeerName, "GET", "/replication/listing", "",
            [("host", "neighbor:9000"), ("x-owns3-peer-date", PeerDateOf(At)),
                ("x-owns3-peer-content-sha256", payload)],
            payload, At);

        // Act — верификатор на 14 минут позже
        var result = new PeerRequestVerifier(new FixedTimeProvider(At.AddMinutes(14)))
            .Verify(BuildModel("GET", "/replication/listing", "", auth, peerDate, contentSha), Key);

        // Assert — в допуске ±15 мин
        result.Should().BeOfType<PeerVerifyResult.Ok>();
    }

    [Theory]
    [InlineData("OWNS3-PEER-ECC-P256-SHA256 Credential=x Signature=y")]                       // нет SignedHeaders
    [InlineData("OWNS3-PEER-ECC-P256-SHA256 Credential=x, SignedHeaders=host, Signature=y")]  // неполный список
    [InlineData("OWNS3-PEER-FOO Credential=x")]                                               // чужой алгоритм семейства
    [InlineData("мусор")]
    public void Verify_MalformedAuthorization_AuthorizationHeaderMalformed(string header)
    {
        // Arrange — модель с битым Authorization и валидными остальными заголовками
        var payload = SigV4Core.EmptySha256;

        // Act
        var result = new PeerRequestVerifier(new FixedTimeProvider(At))
            .Verify(BuildModel("GET", "/replication/listing", "", header, PeerDateOf(At), payload), Key);

        // Assert — 400 AuthorizationHeaderMalformed
        var fail = result.Should().BeOfType<PeerVerifyResult.Fail>().Which;
        fail.Code.Should().Be(S3ErrorCode.AuthorizationHeaderMalformed);
    }

    [Fact]
    public void Verify_WrongKey_SignatureDoesNotMatch()
    {
        // Arrange — подпись ключом A
        var payload = SigV4Core.EmptySha256;
        var (auth, peerDate, contentSha) = PeerRequestSigner.Sign(Key, PeerName, "GET", "/replication/listing", "",
            [("host", "neighbor:9000"), ("x-owns3-peer-date", PeerDateOf(At)),
                ("x-owns3-peer-content-sha256", payload)],
            payload, At);

        // Act — проверка ключом B
        var result = new PeerRequestVerifier(new FixedTimeProvider(At))
            .Verify(BuildModel("GET", "/replication/listing", "", auth, peerDate, contentSha), OtherKey);

        // Assert — 403 SignatureDoesNotMatch
        var fail = result.Should().BeOfType<PeerVerifyResult.Fail>().Which;
        fail.Code.Should().Be(S3ErrorCode.SignatureDoesNotMatch);
    }

    [Fact]
    public void Signature_IsP1363Base64_64Bytes()
    {
        // Arrange/Act — подпись GET-запроса, разбор Signature= из Authorization
        var payload = SigV4Core.EmptySha256;
        var (auth, _, _) = PeerRequestSigner.Sign(Key, PeerName, "GET", "/replication/listing", "",
            [("host", "neighbor:9000"), ("x-owns3-peer-date", PeerDateOf(At)),
                ("x-owns3-peer-content-sha256", payload)],
            payload, At);
        var signaturePart = auth.Split("Signature=")[1];

        // Assert — P1363 r||s fixed-length: 64 байта
        Convert.FromBase64String(signaturePart).Should().HaveCount(64);
    }

    [Fact]
    public void Authorization_Format_FixedSignedHeaders()
    {
        // Arrange/Act — подпись запроса
        var payload = SigV4Core.EmptySha256;
        var (auth, _, _) = PeerRequestSigner.Sign(Key, PeerName, "GET", "/replication/listing", "",
            [("host", "neighbor:9000"), ("x-owns3-peer-date", PeerDateOf(At)),
                ("x-owns3-peer-content-sha256", payload)],
            payload, At);

        // Assert — префикс алгоритма и ровно фиксированный список заголовков
        auth.Should().StartWith("OWNS3-PEER-ECC-P256-SHA256 Credential=peer-a, ");
        auth.Should().Contain("SignedHeaders=host;x-owns3-peer-date;x-owns3-peer-content-sha256, Signature=");
    }

    [Theory]
    [InlineData("OWNS3-PEER-ECC-P256-SHA256 Credential=peer-a, SignedHeaders=host, Signature=AA==")] // валидная структура — имя
    [InlineData("OWNS3-PEER-ECC-P256-SHA256 Signature=AA==")]                                        // нет Credential — null
    [InlineData("мусор")]                                                                            // нет Credential — null
    public void CredentialOf_ParsesName_OrNull(string header)
    {
        // Arrange — модель только с Authorization (разбор ДО резолва записи)
        var model = new S3RequestModel
        {
            Method = "GET",
            RawPath = "/replication/listing",
            RawQuery = "",
            Headers = S3HeaderCollection.FromPairs(("Authorization", header), ("host", "neighbor:9000")),
            Host = "neighbor:9000",
            OpenBody = () => Stream.Null,
        };

        // Act
        var credential = PeerRequestVerifier.CredentialOf(model);

        // Assert — имя из Credential или null при malformed
        if (header.Contains("Credential=peer-a"))
            credential.Should().Be("peer-a");
        else
            credential.Should().BeNull();
    }

    private static string PeerDateOf(DateTimeOffset at) =>
        at.ToString("yyyyMMdd'T'HHmmss'Z'", System.Globalization.CultureInfo.InvariantCulture);

    private static S3RequestModel BuildModel(string method, string path, string query, string authorization,
        string peerDate, string contentSha) =>
        new()
        {
            Method = method,
            RawPath = path,
            RawQuery = query,
            Headers = S3HeaderCollection.FromPairs(
                ("Authorization", authorization),
                ("x-owns3-peer-date", peerDate),
                ("x-owns3-peer-content-sha256", contentSha),
                ("host", "neighbor:9000")),
            Host = "neighbor:9000",
            OpenBody = () => Stream.Null,
        };
}
