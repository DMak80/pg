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

// Входящая перезапись deny-list (глава 07 §2/§6, PUT /replication/denylist):
// до-join контур по прецеденту JoinPeerServiceTests — реальные мини-PKI
// материалы, подпись PeerRequestSigner, fake-модель запроса; порядок проверок
// §4.2 (конфигурация → сертификат → доверие → подпись → тело).
public sealed class DenyListReplaceServiceFixture : IDisposable
{
    public string Dir { get; } = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));

    public string DenyPath => Path.Combine(Dir, "denylist.json");

    public void Dispose()
    {
        // Teardown: temp-каталог удаляется при любом исходе
        if (Directory.Exists(Dir))
            Directory.Delete(Dir, recursive: true);
    }
}

public class DenyListReplaceServiceTests : IDisposable
{
    private readonly DenyListReplaceServiceFixture _fixture = new();

    // Фиксированное «сейчас» (skew ±15 мин верификатора — от At).
    private static readonly DateTimeOffset At = new(2026, 10, 14, 12, 0, 0, TimeSpan.Zero);

    public void Dispose()
    {
        // Изоляция кейсов: temp-каталог на кейс
        if (Directory.Exists(_fixture.Dir))
            Directory.Delete(_fixture.Dir, recursive: true);
    }

    // ── материал: CA + identity вызывающего/принимающего (как JoinPeerServiceTests) ──

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

    // Валидный PEM записи списка (ECDSA P-256).
    private static string ValidPem()
    {
        using var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        return key.ExportSubjectPublicKeyInfoPem();
    }

    // Тело перезаписи {entries:[{name, publicKeyPem}]} (camelCase, §4.1).
    private static string BodyOf(params (string Name, string PublicKeyPem)[] entries) =>
        JsonSerializer.Serialize(new
        {
            entries = entries.Select(e => new { name = e.Name, publicKeyPem = e.PublicKeyPem }).ToList(),
        });

    // Подписанная модель PUT /replication/denylist (по образцу SignedJoinModel).
    private static S3RequestModel SignedModel(ECDsa signingKey, string callerName, string body,
        DateTimeOffset? at = null)
    {
        var date = at ?? At;
        var payloadHash = SigV4Core.HexSha256(Encoding.UTF8.GetBytes(body));
        var (auth, peerDate, _) = PeerRequestSigner.Sign(signingKey, callerName, "PUT",
            "/replication/denylist", "",
            [("host", "neighbor:9000"),
                ("x-owns3-peer-date", PeerDateOf(date)),
                ("x-owns3-peer-content-sha256", payloadHash)],
            payloadHash, date);
        return new S3RequestModel
        {
            Method = "PUT",
            RawPath = "/replication/denylist",
            RawQuery = "",
            Headers = S3HeaderCollection.FromPairs(
                ("Authorization", auth),
                ("x-owns3-peer-date", peerDate),
                ("x-owns3-peer-content-sha256", payloadHash),
                ("host", "neighbor:9000")),
            Host = "neighbor:9000",
            OpenBody = () => new MemoryStream(Encoding.UTF8.GetBytes(body)),
        };
    }

    private static string PeerDateOf(DateTimeOffset at) =>
        at.ToString("yyyyMMdd'T'HHmmss'Z'", System.Globalization.CultureInfo.InvariantCulture);

    private DenyListReplaceService Service(PeerIdentity? identity, PeerTrustAnchor? trust,
        DenyList deny, TimeProvider? tp = null) =>
        new(identity, trust, deny, tp ?? new FixedTimeProvider(At),
            NullLogger<DenyListReplaceService>.Instance);

    private static ReadOnlyMemory<byte> BodyBytes(string body) =>
        new(Encoding.UTF8.GetBytes(body));

    // ── валидный вызов ──

    [Fact]
    public void Valid_ReplaceApplied_CountReturned()
    {
        // Arrange — принимающий (mesh-a) с CA; вызывающий-оркестратор (operator)
        // с сертификатом того же CA (не числится соседом — неважно для метода)
        var (ca, caPem) = NewCa();
        var me = MakeIdentity("mesh-a", ca, caPem);
        var caller = MakeIdentity("operator", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        var service = Service(me, ca, deny, tp);
        var (name, pem) = ("peer-b", ValidPem());
        var (name2, pem2) = ("peer-c", ValidPem());

        // Act
        var result = service.HandleIncoming(
            SignedModel(caller.Key, caller.Name, BodyOf((name, pem), (name2, pem2))),
            CertBase64(caller), BodyBytes(BodyOf((name, pem), (name2, pem2))));

        // Assert — Success; состав = тело; count = 2; время — серверное (At)
        result.IsSuccess.Should().BeTrue();
        result.Value.Count.Should().Be(2);
        deny.Snapshot.Select(e => e.Name).Should().BeEquivalentTo(name, name2);
        deny.Snapshot.Should().OnlyContain(e => e.RevokedAtUnixMs == At.ToUnixTimeMilliseconds());
    }

    [Fact]
    public void Valid_EmptyEntries_ClearsList()
    {
        // Arrange — действующий отзыв; вызывающий не отозван
        var (ca, caPem) = NewCa();
        var me = MakeIdentity("mesh-a", ca, caPem);
        var caller = MakeIdentity("operator", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        deny.Add("old", ValidPem());
        var service = Service(me, ca, deny, tp);

        // Act — пустой entries: снятие всех отзывов
        var result = service.HandleIncoming(SignedModel(caller.Key, caller.Name, BodyOf()),
            CertBase64(caller), BodyBytes(BodyOf()));

        // Assert — Success; список пуст
        result.IsSuccess.Should().BeTrue();
        result.Value.Count.Should().Be(0);
        deny.Snapshot.Should().BeEmpty();
    }

    // ── конфигурация ──

    [Fact]
    public void NoIdentity_Refused()
    {
        // Arrange — identity принимающего не задан (ручной режим) — как join: 403
        var (ca, caPem) = NewCa();
        var caller = MakeIdentity("operator", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        var service = Service(null, ca, deny, tp);

        // Act
        var result = service.HandleIncoming(SignedModel(caller.Key, caller.Name, BodyOf()),
            CertBase64(caller), BodyBytes(BodyOf()));

        // Assert
        result.Error.Should().BeOfType<DenyReplaceRefusedException>();
    }

    // ── доверие сертификату ──

    [Fact]
    public void NoTrust_Refused()
    {
        // Arrange — trust-anchor не задан (ручной режим состава)
        var (ca, caPem) = NewCa();
        var caller = MakeIdentity("operator", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        var service = Service(MakeIdentity("mesh-a", ca, caPem), null, deny, tp);

        // Act
        var result = service.HandleIncoming(SignedModel(caller.Key, caller.Name, BodyOf()),
            CertBase64(caller), BodyBytes(BodyOf()));

        // Assert
        result.Error.Should().BeOfType<DenyReplaceRefusedException>();
    }

    // ── доверие сертификату ──

    [Fact]
    public void OtherCaCert_Refused()
    {
        // Arrange — сертификат вызывающего от чужого CA
        var (ca, caPem) = NewCa();
        var (foreignCa, foreignCaPem) = NewCa("other-ca");
        var caller = MakeIdentity("operator", foreignCa, foreignCaPem);
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        var service = Service(MakeIdentity("mesh-a", ca, caPem), ca, deny, tp);

        // Act
        var result = service.HandleIncoming(SignedModel(caller.Key, caller.Name, BodyOf()),
            CertBase64(caller), BodyBytes(BodyOf()));

        // Assert
        result.Error.Should().BeOfType<DenyReplaceRefusedException>();
    }

    [Fact]
    public void ExpiredCert_Refused()
    {
        // Arrange — истёкший сертификат вызывающего (выпуск 2 дня назад, срок 1)
        var (ca, caPem) = NewCa();
        var caller = MakeIdentity("operator", ca, caPem, at: At.AddDays(-2), expiresDays: 1);
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        var service = Service(MakeIdentity("mesh-a", ca, caPem), ca, deny, tp);

        // Act
        var result = service.HandleIncoming(SignedModel(caller.Key, caller.Name, BodyOf()),
            CertBase64(caller), BodyBytes(BodyOf()));

        // Assert
        result.Error.Should().BeOfType<DenyReplaceRefusedException>();
    }

    [Fact]
    public void UnparsableCert_Refused()
    {
        // Arrange — заголовок сертификата — base64 мусора (не JSON)
        var (ca, caPem) = NewCa();
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        var service = Service(MakeIdentity("mesh-a", ca, caPem), ca, deny, tp);

        // Act
        var result = service.HandleIncoming(SignedModel(ECDsa.Create(), "operator", BodyOf()),
            Convert.ToBase64String(Encoding.UTF8.GetBytes("не-сертификат")), BodyBytes(BodyOf()));

        // Assert — FromJson бросает → отказ доверия (403), не 400
        result.Error.Should().BeOfType<DenyReplaceRefusedException>();
    }

    // ── deny-list вызывающего ──

    [Fact]
    public void DeniedCallerByName_Refused()
    {
        // Arrange — вызывающий отозван по имени (ключ другой)
        var (ca, caPem) = NewCa();
        var caller = MakeIdentity("operator", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        deny.Add("operator", "-----BEGIN PUBLIC KEY-----\nOTHER\n-----END PUBLIC KEY-----\n");
        var service = Service(MakeIdentity("mesh-a", ca, caPem), ca, deny, tp);

        // Act
        var result = service.HandleIncoming(SignedModel(caller.Key, caller.Name, BodyOf()),
            CertBase64(caller), BodyBytes(BodyOf()));

        // Assert — отозванный не переписывает список отзывов
        result.Error.Should().BeOfType<DenyReplaceRefusedException>();
        deny.Snapshot.Should().ContainSingle().Which.Name.Should().Be("operator");
    }

    [Fact]
    public void DeniedCallerByKey_Refused()
    {
        // Arrange — вызывающий отозван по публичному ключу (имя другое)
        var (ca, caPem) = NewCa();
        var caller = MakeIdentity("operator", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        deny.Add("другое-имя", caller.Certificate.PublicKeyPem);
        var service = Service(MakeIdentity("mesh-a", ca, caPem), ca, deny, tp);

        // Act
        var result = service.HandleIncoming(SignedModel(caller.Key, caller.Name, BodyOf()),
            CertBase64(caller), BodyBytes(BodyOf()));

        // Assert
        result.Error.Should().BeOfType<DenyReplaceRefusedException>();
        deny.Snapshot.Should().ContainSingle().Which.Name.Should().Be("другое-имя");
    }

    // ── подпись запроса ──

    [Fact]
    public void SignatureByOtherKey_RefusedWithCatalogCode()
    {
        // Arrange — запрос подписан ЧУЖИМ ключом (сертификат при этом валидный)
        var (ca, caPem) = NewCa();
        var caller = MakeIdentity("operator", ca, caPem);
        var stranger = MakeIdentity("stranger", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        var service = Service(MakeIdentity("mesh-a", ca, caPem), ca, deny, tp);

        // Act
        var result = service.HandleIncoming(SignedModel(stranger.Key, caller.Name, BodyOf()),
            CertBase64(caller), BodyBytes(BodyOf()));

        // Assert — код каталога подписи (грань отдаст 403 SignatureDoesNotMatch)
        var refused = result.Error.Should().BeOfType<DenyReplaceRefusedException>().Which;
        refused.ErrorCode.Should().Be(S3ErrorCode.SignatureDoesNotMatch);
    }

    [Fact]
    public void SkewedDate_RefusedWithCatalogCode()
    {
        // Arrange — дата подписи At+16 минут (допуск ±15)
        var (ca, caPem) = NewCa();
        var caller = MakeIdentity("operator", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        var service = Service(MakeIdentity("mesh-a", ca, caPem), ca, deny, tp);

        // Act
        var result = service.HandleIncoming(
            SignedModel(caller.Key, caller.Name, BodyOf(), at: At.AddMinutes(16)),
            CertBase64(caller), BodyBytes(BodyOf()));

        // Assert
        var refused = result.Error.Should().BeOfType<DenyReplaceRefusedException>().Which;
        refused.ErrorCode.Should().Be(S3ErrorCode.RequestTimeTooSkewed);
    }

    [Fact]
    public void MalformedAuthorization_RefusedWith400Code()
    {
        // Arrange — Authorization валидного префикса OWNS3-PEER-, но мусорная
        // структура (без Credential): Verify → AuthorizationHeaderMalformed
        var (ca, caPem) = NewCa();
        var caller = MakeIdentity("operator", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        var service = Service(MakeIdentity("mesh-a", ca, caPem), ca, deny, tp);
        var model = new S3RequestModel
        {
            Method = "PUT",
            RawPath = "/replication/denylist",
            RawQuery = "",
            Headers = S3HeaderCollection.FromPairs(
                ("Authorization", "OWNS3-PEER-мусор"),
                ("x-owns3-peer-date", PeerDateOf(At)),
                ("x-owns3-peer-content-sha256", SigV4Core.HexSha256(Encoding.UTF8.GetBytes(BodyOf()))),
                ("host", "neighbor:9000")),
            Host = "neighbor:9000",
            OpenBody = () => new MemoryStream(Encoding.UTF8.GetBytes(BodyOf())),
        };

        // Act
        var result = service.HandleIncoming(model, CertBase64(caller), BodyBytes(BodyOf()));

        // Assert — код AuthorizationHeaderMalformed (грань отдаст 400 XML)
        var refused = result.Error.Should().BeOfType<DenyReplaceRefusedException>().Which;
        refused.ErrorCode.Should().Be(S3ErrorCode.AuthorizationHeaderMalformed);
    }

    // ── порядок: доверие ДО тела (принцип 7) ──

    [Fact]
    public void GarbageBody_UntrustedCert_RefusedNotBadRequest()
    {
        // Arrange — мусорное тело + сертификат чужого CA
        var (ca, caPem) = NewCa();
        var (foreignCa, foreignCaPem) = NewCa("other-ca");
        var caller = MakeIdentity("operator", foreignCa, foreignCaPem);
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        var service = Service(MakeIdentity("mesh-a", ca, caPem), ca, deny, tp);

        // Act
        var result = service.HandleIncoming(
            SignedModel(caller.Key, caller.Name, "{мусор не json"),
            CertBase64(caller), BodyBytes("{мусор не json"));

        // Assert — отказ доверия (403), тело не разбирается и не рулит кодом
        result.Error.Should().BeOfType<DenyReplaceRefusedException>();
    }

    // ── тело (после доверия) ──

    [Fact]
    public void MalformedJson_BadRequest_ListUntouched()
    {
        // Arrange — действующий отзыв; тело — не-JSON
        var (ca, caPem) = NewCa();
        var caller = MakeIdentity("operator", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        deny.Add("keep", ValidPem());
        var service = Service(MakeIdentity("mesh-a", ca, caPem), ca, deny, tp);

        // Act
        var result = service.HandleIncoming(
            SignedModel(caller.Key, caller.Name, "{мусор"),
            CertBase64(caller), BodyBytes("{мусор"));

        // Assert — 400-тип; Replace НЕ вызван (снапшот/файл прежние)
        result.Error.Should().BeOfType<DenyReplaceBadRequestException>();
        deny.Snapshot.Should().ContainSingle().Which.Name.Should().Be("keep");
    }

    [Fact]
    public void EntriesMissing_BadRequest()
    {
        // Arrange — корректный JSON без поля entries
        var (ca, caPem) = NewCa();
        var caller = MakeIdentity("operator", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        var service = Service(MakeIdentity("mesh-a", ca, caPem), ca, deny, tp);

        // Act
        var result = service.HandleIncoming(
            SignedModel(caller.Key, caller.Name, """{"noEntries":true}"""),
            CertBase64(caller), BodyBytes("""{"noEntries":true}"""));

        // Assert
        result.Error.Should().BeOfType<DenyReplaceBadRequestException>();
        deny.Snapshot.Should().BeEmpty();
    }

    [Theory]
    [InlineData("dup-names")]
    [InlineData("dup-keys")]
    [InlineData("broken-pem")]
    [InlineData("empty-fields")]
    public void InvalidEntries_BadRequest_ListUntouched(string kind)
    {
        // Arrange — действующий отзыв; невалидный состав по виду кейса
        // (валидация Replace → ArgumentException → 400-тип)
        var (ca, caPem) = NewCa();
        var caller = MakeIdentity("operator", ca, caPem);
        var tp = new FixedTimeProvider(At);
        var deny = new DenyList(_fixture.DenyPath, tp);
        deny.Add("keep", ValidPem());
        var service = Service(MakeIdentity("mesh-a", ca, caPem), ca, deny, tp);
        var pemA = ValidPem();
        var pemB = ValidPem();
        var body = kind switch
        {
            "dup-names" => BodyOf(("n1", pemA), ("n1", pemB)),
            "dup-keys" => BodyOf(("n1", pemA), ("n2", pemA)),
            "broken-pem" => BodyOf(("n1", "-----BEGIN PUBLIC KEY-----\nZZZ\n-----END PUBLIC KEY-----\n")),
            _ => JsonSerializer.Serialize(new
            {
                entries = new object[] { new { name = "", publicKeyPem = "" } },
            }),
        };

        // Act
        var result = service.HandleIncoming(SignedModel(caller.Key, caller.Name, body),
            CertBase64(caller), BodyBytes(body));

        // Assert — 400-тип; снапшот/файл не тронуты (валидация ДО персиста)
        result.Error.Should().BeOfType<DenyReplaceBadRequestException>();
        deny.Snapshot.Should().ContainSingle().Which.Name.Should().Be("keep");
        new DenyList(_fixture.DenyPath, tp).Snapshot.Should().ContainSingle().Which.Name.Should().Be("keep");
    }
}
