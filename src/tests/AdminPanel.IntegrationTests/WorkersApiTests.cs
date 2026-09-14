using System.Net;
using System.Net.Http.Json;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Core.Kafka;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Parsing;
using AdminPanel.Etcd.Workers;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// Грань «Воркеры» (spec §3.3 п.4, §4.1–§4.4): статусы применения, generate
// (txn 409 на живом ключе), PUT/DELETE, 422-изоляция исходящих, рестарт-прокси.
[Collection("workers-cert")]
public class WorkersApiTests(WorkersCertFixture fx)
{
    private readonly string _etcd = fx.Etcd.Endpoint;

    // Свежий клиент + логин (rate-limiter окно двигаем, cookie в клиенте).
    private async Task<HttpClient> LoginAsync()
    {
        fx.Factory.Time.Utc += TimeSpan.FromSeconds(61);
        var client = fx.Factory.CreateClient();
        var login = await client.PostAsJsonAsync(
            "/api/auth/login",
            new { username = "admin", password = "adminpw" },
            TestContext.Current.CancellationToken);
        login.StatusCode.Should().Be(HttpStatusCode.NoContent);
        return client;
    }

    // Предочистка ключа серта сценария (кейсы независимы от порядка).
    private async Task DeleteKeysAsync()
    {
        var gateway = new EtcdGateway(new HttpClient());
        await gateway.DeleteAsync(_etcd, "/workers/api_tls/", prefix: true, TestContext.Current.CancellationToken);
    }

    private async Task<string?> GetKeyAsync(string key)
    {
        var gateway = new EtcdGateway(new HttpClient());
        var range = await gateway.RangeAsync(_etcd, key, TestContext.Current.CancellationToken);
        if (!range.IsSuccess || range.Value.Count == 0)
            return null;
        return range.Value[0].Value;
    }

    // Базис снапшота (локальный: TestSnapshots — internal юнит-сборки).
    // Перегрузка с cert — метаданные целевого серта: статусы applied/pending
    // считаются от него.
    private static EtcdSnapshot PgSnapshot(params WorkerEndpoint[] endpoints) => PgSnapshot(null, endpoints);

    private static EtcdSnapshot PgSnapshot(WorkerApiCert? cert, params WorkerEndpoint[] endpoints) => new(
        DateTimeOffset.UtcNow,
        new EtcdStatus(true,
            [new EtcdEndpoint("http://etcd1:2379", true, 3, "3.5.21", 20480, 42, 17, 3, [])],
            [new EtcdMember(42, "etcd1", ["http://etcd1:2380"], ["http://etcd1:2379"])],
            [], "http://etcd1:2379", false, DateTimeOffset.UtcNow, 0),
        [], [], [], [], [], [.. endpoints], [], [], [], [], [], 0,
        WorkerApiCert: cert);

    private static readonly (string CaPem, string CaKeyPem) Ca = TestPki.GenerateCa();

    // ===== GET /api/workers: статусы (spec §3.3 п.4) =====

    [Fact]
    public async Task Get_NoCertKey_Unmanaged()
    {
        // Arrange: снапшот с инстансом, ключа нет
        await DeleteKeysAsync();
        fx.Factory.Snapshot = PgSnapshot(new WorkerEndpoint("i1", "http://h:1", 5));
        fx.Factory.KafkaSnapshot = null;
        using var client = await LoginAsync();

        // Act
        var body = await client.GetStringAsync("/api/workers", TestContext.Current.CancellationToken);

        // Assert: инстанс жив, целевого серта нет → unmanaged
        body.Should().Contain("\"instance\":\"i1\"");
        body.Should().Contain("\"applyStatus\":\"unmanaged\"");
        body.Should().NotContain("\"targetCert\":{");
    }

    [Fact]
    public async Task Get_InstanceWithoutThumbprint_Unknown()
    {
        // Arrange: ключ есть, инстанс thumbprint не сообщает
        await DeleteKeysAsync();
        var (certPem, keyPem) = TestPki.Issue(Ca.CaPem, Ca.CaKeyPem, "unknown-t");
        var meta = await PutCertAsync(certPem, keyPem);
        fx.Factory.Snapshot = PgSnapshot(meta, new WorkerEndpoint("i1", "http://h:1", 5));
        fx.Factory.KafkaSnapshot = null;
        using var client = await LoginAsync();

        // Act
        var body = await client.GetStringAsync("/api/workers", TestContext.Current.CancellationToken);

        // Assert: старая версия → unknown
        body.Should().Contain("\"applyStatus\":\"unknown\"");
    }

    [Fact]
    public async Task Get_ThumbprintMatchesTarget_Applied()
    {
        // Arrange: thumbprint инстанса == целевому
        await DeleteKeysAsync();
        var (certPem, keyPem) = TestPki.Issue(Ca.CaPem, Ca.CaKeyPem, "applied-t");
        var meta = await PutCertAsync(certPem, keyPem);
        var thumb = ThumbprintOf(certPem);
        fx.Factory.Snapshot = PgSnapshot(meta, new WorkerEndpoint("i1", "http://h:1", 5, CertThumbprint: thumb));
        fx.Factory.KafkaSnapshot = null;
        using var client = await LoginAsync();

        // Act
        var body = await client.GetStringAsync("/api/workers", TestContext.Current.CancellationToken);

        // Assert
        body.Should().Contain("\"applyStatus\":\"applied\"");
        body.Should().Contain(thumb);
    }

    [Fact]
    public async Task Get_ThumbprintDiffers_PendingRestart()
    {
        // Arrange: thumbprint инстанса отличается от целевого
        await DeleteKeysAsync();
        var (certPem, keyPem) = TestPki.Issue(Ca.CaPem, Ca.CaKeyPem, "pending-t");
        var meta = await PutCertAsync(certPem, keyPem);
        fx.Factory.Snapshot = PgSnapshot(meta, new WorkerEndpoint("i1", "http://h:1", 5, CertThumbprint: new string('a', 64)));
        fx.Factory.KafkaSnapshot = null;
        using var client = await LoginAsync();

        // Act
        var body = await client.GetStringAsync("/api/workers", TestContext.Current.CancellationToken);

        // Assert
        body.Should().Contain("\"applyStatus\":\"pending restart\"");
    }

    [Fact]
    public async Task Get_PemNotLeaked()
    {
        // Arrange: ключ серта записан; снапшот с целевым сертом
        await DeleteKeysAsync();
        var (certPem, keyPem) = TestPki.Issue(Ca.CaPem, Ca.CaKeyPem, "leak-t");
        await PutCertAsync(certPem, keyPem);
        fx.Factory.Snapshot = PgSnapshot();
        fx.Factory.KafkaSnapshot = null;
        using var client = await LoginAsync();

        // Act
        var body = await client.GetStringAsync("/api/workers", TestContext.Current.CancellationToken);

        // Assert: PEM-материалы наружу не уходят (§7 кр.8)
        body.Should().NotContain("cert_pem");
        body.Should().NotContain("key_pem");
        body.Should().NotContain("BEGIN PRIVATE KEY");
    }

    // ===== generate (spec §4.1) =====

    [Fact]
    public async Task Generate_NoKey_201TxnPut()
    {
        // Arrange: ключа нет, живой инстанс с advertise-URL
        await DeleteKeysAsync();
        fx.Factory.Snapshot = PgSnapshot(new WorkerEndpoint("i1", "https://w1.local:8080", 5));
        fx.Factory.KafkaSnapshot = null;
        using var client = await LoginAsync();

        // Act
        var response = await client.PostAsync("/api/workers/pgworker/api-cert/generate", null, TestContext.Current.CancellationToken);

        // Assert: 201 + ключ реально записан в etcd, restartRequired=true
        response.StatusCode.Should().Be(HttpStatusCode.Created);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        using var json = JsonDocument.Parse(body);
        json.RootElement.GetProperty("thumbprint").GetString().Should().HaveLength(64);
        json.RootElement.GetProperty("restartRequired").GetBoolean().Should().BeTrue();
        var stored = await GetKeyAsync("/workers/api_tls/pgworker");
        stored.Should().NotBeNull().And.Contain("cert_pem");
    }

    [Fact]
    public async Task Generate_LiveKey_409()
    {
        // Arrange: ключ уже живёт — повторная генерация отклонена
        await DeleteKeysAsync();
        var (certPem, keyPem) = TestPki.Issue(Ca.CaPem, Ca.CaKeyPem, "gen-409");
        await PutCertAsync(certPem, keyPem);
        fx.Factory.Snapshot = PgSnapshot();
        fx.Factory.KafkaSnapshot = null;
        using var client = await LoginAsync();

        // Act
        var response = await client.PostAsync("/api/workers/pgworker/api-cert/generate", null, TestContext.Current.CancellationToken);

        // Assert: 409 «уже управляется»
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        (await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .Should().Contain("уже управляется");
    }

    [Fact]
    public async Task Generate_NoInstances_WarningSanLocalhost()
    {
        // Arrange: живых инстансов нет
        await DeleteKeysAsync();
        fx.Factory.Snapshot = PgSnapshot();
        fx.Factory.KafkaSnapshot = null;
        using var client = await LoginAsync();

        // Act
        var response = await client.PostAsync("/api/workers/pgworker/api-cert/generate", null, TestContext.Current.CancellationToken);

        // Assert: 201 + warning о localhost-SAN
        response.StatusCode.Should().Be(HttpStatusCode.Created);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        using var json = JsonDocument.Parse(body);
        json.RootElement.TryGetProperty("warning", out var warning).Should().BeTrue();
        warning.GetString().Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task Generate_Instances_SanFromAdvertiseUrls()
    {
        // Arrange: инстанс с hostname-URL — хост попадает в SAN
        await DeleteKeysAsync();
        fx.Factory.Snapshot = PgSnapshot(new WorkerEndpoint("i1", "https://w1.local:8080", 5));
        fx.Factory.KafkaSnapshot = null;
        using var client = await LoginAsync();

        // Act
        var response = await client.PostAsync("/api/workers/pgworker/api-cert/generate", null, TestContext.Current.CancellationToken);
        response.StatusCode.Should().Be(HttpStatusCode.Created);

        // Assert: SAN содержит хост advertise + localhost
        var stored = await GetKeyAsync("/workers/api_tls/pgworker");
        using var doc = JsonDocument.Parse(stored!);
        var pem = doc.RootElement.GetProperty("cert_pem").GetString()!;
        using var cert = X509Certificate2.CreateFromPem(pem);
        var san = cert.Extensions.OfType<X509SubjectAlternativeNameExtension>().Single();
        san.EnumerateDnsNames().Should().Contain("w1.local").And.Contain("localhost");
    }

    [Fact]
    public async Task Generate_UnknownWorker_404_EtcdUntouched()
    {
        // Arrange: worker=foo
        await DeleteKeysAsync();
        fx.Factory.Snapshot = PgSnapshot();
        fx.Factory.KafkaSnapshot = null;
        using var client = await LoginAsync();

        // Act
        var response = await client.PostAsync("/api/workers/foo/api-cert/generate", null, TestContext.Current.CancellationToken);

        // Assert: 404, ключ /workers/api_tls/foo не появился
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await GetKeyAsync("/workers/api_tls/foo")).Should().BeNull();
    }

    // ===== PUT (spec §4.2) =====

    [Fact]
    public async Task Put_ValidLeaf_201Overwrite()
    {
        // Arrange: валидная пара → 201; повтор PUT другого серта — безусловная замена
        await DeleteKeysAsync();
        var (first, firstKey) = TestPki.Issue(Ca.CaPem, Ca.CaKeyPem, "put-1");
        await PutCertAsync(first, firstKey);
        var (second, secondKey) = TestPki.Issue(Ca.CaPem, Ca.CaKeyPem, "put-2");
        fx.Factory.Snapshot = PgSnapshot();
        fx.Factory.KafkaSnapshot = null;
        using var client = await LoginAsync();

        // Act
        var response = await client.PutAsJsonAsync("/api/workers/pgworker/api-cert",
            new { cert_pem = second, key_pem = secondKey }, TestContext.Current.CancellationToken);

        // Assert: 201, ключ заменён на второй серт (сверяем сам серт в value)
        response.StatusCode.Should().Be(HttpStatusCode.Created);
        var stored = await GetKeyAsync("/workers/api_tls/pgworker");
        StoredThumbprint(stored!).Should().Be(ThumbprintOf(second));
    }

    [Fact]
    public async Task Put_BrokenPair_400_EtcdUntouched()
    {
        // Arrange: ключ с валидной парой; PUT мусора
        await DeleteKeysAsync();
        var (certPem, keyPem) = TestPki.Issue(Ca.CaPem, Ca.CaKeyPem, "put-broken");
        await PutCertAsync(certPem, keyPem);
        var storedBefore = await GetKeyAsync("/workers/api_tls/pgworker");
        fx.Factory.Snapshot = PgSnapshot();
        fx.Factory.KafkaSnapshot = null;
        using var client = await LoginAsync();

        // Act
        var response = await client.PutAsJsonAsync("/api/workers/pgworker/api-cert",
            new { cert_pem = "мусор", key_pem = "мусор" }, TestContext.Current.CancellationToken);

        // Assert: 400, ключ в etcd не изменился
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await GetKeyAsync("/workers/api_tls/pgworker")).Should().Be(storedBefore);
    }

    [Fact]
    public async Task Put_Expired_400()
    {
        // Arrange: NotAfter в прошлом
        await DeleteKeysAsync();
        var (certPem, keyPem) = TestPki.Issue(Ca.CaPem, Ca.CaKeyPem, "put-exp", notAfter: DateTimeOffset.UtcNow.AddDays(-1));
        fx.Factory.Snapshot = PgSnapshot();
        fx.Factory.KafkaSnapshot = null;
        using var client = await LoginAsync();

        // Act
        var response = await client.PutAsJsonAsync("/api/workers/pgworker/api-cert",
            new { cert_pem = certPem, key_pem = keyPem }, TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Put_NoSan_400()
    {
        // Arrange: лист без SAN
        await DeleteKeysAsync();
        var (certPem, keyPem) = TestPki.Issue(Ca.CaPem, Ca.CaKeyPem, "put-nosan", san: false);
        fx.Factory.Snapshot = PgSnapshot();
        fx.Factory.KafkaSnapshot = null;
        using var client = await LoginAsync();

        // Act
        var response = await client.PutAsJsonAsync("/api/workers/pgworker/api-cert",
            new { cert_pem = certPem, key_pem = keyPem }, TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Put_UnknownWorker_404_EtcdUntouched()
    {
        // Arrange: worker=foo
        await DeleteKeysAsync();
        var (certPem, keyPem) = TestPki.Issue(Ca.CaPem, Ca.CaKeyPem, "put-foo");
        fx.Factory.Snapshot = PgSnapshot();
        fx.Factory.KafkaSnapshot = null;
        using var client = await LoginAsync();

        // Act
        var response = await client.PutAsJsonAsync("/api/workers/foo/api-cert",
            new { cert_pem = certPem, key_pem = keyPem }, TestContext.Current.CancellationToken);

        // Assert: 404 (НЕ 201), ключ foo не появился
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await GetKeyAsync("/workers/api_tls/foo")).Should().BeNull();
    }

    // ===== 422-изоляция (spec §4.3) =====

    [Fact]
    public async Task Put_CaCert_422_EtcdUntouched()
    {
        // Arrange: CA-серт как кандидат
        await DeleteKeysAsync();
        fx.Factory.Snapshot = PgSnapshot();
        fx.Factory.KafkaSnapshot = null;
        var (caPem, caKeyPem) = TestPki.GenerateCa();
        using var client = await LoginAsync();

        // Act
        var response = await client.PutAsJsonAsync("/api/workers/pgworker/api-cert",
            new { cert_pem = caPem, key_pem = caKeyPem }, TestContext.Current.CancellationToken);

        // Assert: 422 с явным title, ключ НЕ записан
        response.StatusCode.Should().Be(HttpStatusCode.UnprocessableEntity);
        var problem = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        problem.Should().Contain("влияет на коммуникации воркеров с их подчинёнными сервисами");
        (await GetKeyAsync("/workers/api_tls/pgworker")).Should().BeNull();
    }

    [Fact]
    public async Task Put_ClientAuthEku_422_EtcdUntouched()
    {
        // Arrange: лист с clientAuth в EKU — пригоден в исходящих
        await DeleteKeysAsync();
        fx.Factory.Snapshot = PgSnapshot();
        fx.Factory.KafkaSnapshot = null;
        var (certPem, keyPem) = TestPki.Issue(Ca.CaPem, Ca.CaKeyPem, "put-2eku",
            eku: [TestPki.ServerAuth, TestPki.ClientAuth]);
        using var client = await LoginAsync();

        // Act
        var response = await client.PutAsJsonAsync("/api/workers/pgworker/api-cert",
            new { cert_pem = certPem, key_pem = keyPem }, TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.UnprocessableEntity);
        (await GetKeyAsync("/workers/api_tls/pgworker")).Should().BeNull();
    }

    [Fact]
    public async Task Put_KafkaCaPem_422_EtcdUntouched()
    {
        // Arrange: пара CA кладётся в стаб kafka-кредов фабрики И как кандидат PUT
        await DeleteKeysAsync();
        fx.Factory.Snapshot = PgSnapshot();
        fx.Factory.KafkaSnapshot = null;
        var (caPem, caKeyPem) = TestPki.GenerateCa();
        fx.Factory.KafkaSecrets.Current = new Dictionary<string, KafkaClusterSecrets>
        { ["c1"] = new("c1", "u", "p", caPem) };
        using var client = await LoginAsync();

        // Act
        var response = await client.PutAsJsonAsync("/api/workers/pgworker/api-cert",
            new { cert_pem = caPem, key_pem = caKeyPem }, TestContext.Current.CancellationToken);

        // Assert: 422 «влияет на исходящие», ключ НЕ записан
        response.StatusCode.Should().Be(HttpStatusCode.UnprocessableEntity);
        (await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .Should().Contain("влияет на коммуникации воркеров с их подчинёнными сервисами");
        (await GetKeyAsync("/workers/api_tls/pgworker")).Should().BeNull();
    }

    // ===== DELETE (spec §4.2) =====

    [Fact]
    public async Task Delete_LiveKey_204_KeyGone()
    {
        // Arrange: ключ живёт
        await DeleteKeysAsync();
        var (certPem, keyPem) = TestPki.Issue(Ca.CaPem, Ca.CaKeyPem, "del-1");
        await PutCertAsync(certPem, keyPem);
        fx.Factory.Snapshot = PgSnapshot();
        fx.Factory.KafkaSnapshot = null;
        using var client = await LoginAsync();

        // Act
        var response = await client.DeleteAsync("/api/workers/pgworker/api-cert", TestContext.Current.CancellationToken);

        // Assert: 204, ключ удалён
        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
        (await GetKeyAsync("/workers/api_tls/pgworker")).Should().BeNull();
    }

    [Fact]
    public async Task Delete_MissingKey_404()
    {
        // Arrange: ключа нет
        await DeleteKeysAsync();
        fx.Factory.Snapshot = PgSnapshot();
        fx.Factory.KafkaSnapshot = null;
        using var client = await LoginAsync();

        // Act
        var response = await client.DeleteAsync("/api/workers/pgworker/api-cert", TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Delete_UnknownWorker_404()
    {
        // Arrange: мусорный ключ foo записан в etcd руками
        await DeleteKeysAsync();
        var gateway = new EtcdGateway(new HttpClient());
        await gateway.PutAsync(_etcd, "/workers/api_tls/foo", "x", TestContext.Current.CancellationToken);
        fx.Factory.Snapshot = PgSnapshot();
        fx.Factory.KafkaSnapshot = null;
        using var client = await LoginAsync();

        // Act
        var response = await client.DeleteAsync("/api/workers/foo/api-cert", TestContext.Current.CancellationToken);

        // Assert: 404, мусорный ключ нетронут (панель его не пишет и не трогает)
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        (await GetKeyAsync("/workers/api_tls/foo")).Should().NotBeNull();
    }

    // ===== restart (spec §4.4) =====

    [Fact]
    public async Task Restart_ProxiesToAllInstances_202()
    {
        // Arrange: живые инстансы; стаб WorkerApi отвечает 202
        fx.Factory.Snapshot = PgSnapshot(
            new WorkerEndpoint("i1", "https://w1:8080", 5),
            new WorkerEndpoint("i2", "https://w2:8080", 6));
        fx.Factory.KafkaSnapshot = null;
        fx.Factory.WorkerApi.Reset();
        using var client = await LoginAsync();

        // Act
        var response = await client.PostAsync("/api/workers/pgworker/restart", null, TestContext.Current.CancellationToken);

        // Assert: 202, результат — от стаба SendAllAsync (маркер "stub-instance")
        response.StatusCode.Should().Be(HttpStatusCode.Accepted);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        body.Should().Contain("\"instance\":\"stub-instance\"");
        body.Should().Contain("\"accepted\":true");
    }

    [Fact]
    public async Task Restart_NoLiveEndpoints_503()
    {
        // Arrange: стаб бросает Unavailable (живых ключей нет — семантика
        // реального гейтвея покрыта юнитами SendAllAsync)
        fx.Factory.WorkerApi.Reset();
        fx.Factory.WorkerApi.Throw = new WorkerApiUnavailableException("pgworker");
        fx.Factory.Snapshot = PgSnapshot();
        fx.Factory.KafkaSnapshot = null;
        using var client = await LoginAsync();

        // Act
        var response = await client.PostAsync("/api/workers/pgworker/restart", null, TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
    }

    [Fact]
    public async Task Restart_UnknownWorker_404()
    {
        // Arrange
        fx.Factory.Snapshot = PgSnapshot();
        fx.Factory.KafkaSnapshot = null;
        using var client = await LoginAsync();

        // Act
        var response = await client.PostAsync("/api/workers/foo/restart", null, TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Actions_WithoutCookie_401()
    {
        // Arrange: неаутентифицированный клиент
        fx.Factory.Snapshot = null;
        fx.Factory.KafkaSnapshot = null;
        using var client = fx.Factory.CreateClient();

        // Act / Assert: default-deny на всех пяти эндпоинтах
        (await client.GetAsync("/api/workers", TestContext.Current.CancellationToken)).StatusCode
            .Should().Be(HttpStatusCode.Unauthorized);
        (await client.PostAsync("/api/workers/pgworker/api-cert/generate", null, TestContext.Current.CancellationToken)).StatusCode
            .Should().Be(HttpStatusCode.Unauthorized);
        (await client.PutAsync("/api/workers/pgworker/api-cert", null, TestContext.Current.CancellationToken)).StatusCode
            .Should().Be(HttpStatusCode.Unauthorized);
        (await client.DeleteAsync("/api/workers/pgworker/api-cert", TestContext.Current.CancellationToken)).StatusCode
            .Should().Be(HttpStatusCode.Unauthorized);
        (await client.PostAsync("/api/workers/pgworker/restart", null, TestContext.Current.CancellationToken)).StatusCode
            .Should().Be(HttpStatusCode.Unauthorized);
    }

    // ===== Хелперы =====

    // Пишет пару в ключ серта и возвращает метаданные (тем же парсером, что
    // продовый refresher: метаданные снапшота = разобранный ключ, не PEM).
    private async Task<WorkerApiCert?> PutCertAsync(string certPem, string keyPem)
    {
        var gateway = new EtcdGateway(new HttpClient());
        var value = JsonSerializer.Serialize(new { cert_pem = certPem, key_pem = keyPem });
        var put = await gateway.PutAsync(_etcd, "/workers/api_tls/pgworker", value, TestContext.Current.CancellationToken);
        put.IsSuccess.Should().BeTrue();
        var range = await gateway.RangeAsync(_etcd, "/workers/api_tls/pgworker", TestContext.Current.CancellationToken);
        range.IsSuccess.Should().BeTrue();
        return WorkerCertParser.Parse("/workers/api_tls/pgworker", range.Value.Count > 0 ? range.Value[0] : null).Cert;
    }

    private static string ThumbprintOf(string certPem)
    {
        using var cert = X509Certificate2.CreateFromPem(certPem);
        return Convert.ToHexString(SHA256.HashData(cert.RawData)).ToLowerInvariant();
    }

    // Thumbprint серта, записанного в value ключа (парсим cert_pem из JSON).
    private static string StoredThumbprint(string storedJson)
    {
        using var doc = JsonDocument.Parse(storedJson);
        var pem = doc.RootElement.GetProperty("cert_pem").GetString()!;
        return ThumbprintOf(pem);
    }

    // PKI-хелпер файла: GenerateCa + Issue (SAN по умолчанию, EKU serverAuth).
    internal static class TestPki
    {
        public const string ServerAuth = "1.3.6.1.5.5.7.3.1";
        public const string ClientAuth = "1.3.6.1.5.5.7.3.2";

        public static (string CaPem, string CaKeyPem) GenerateCa()
        {
            using var rsa = RSA.Create(2048);
            var request = new CertificateRequest(
                "CN=test-ca", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
            // NotBefore с запасом: Issue может запрашивать просроченные окна листа.
            using var ca = request.CreateSelfSigned(
                DateTimeOffset.UtcNow.AddDays(-10), DateTimeOffset.UtcNow.AddYears(10));
            return (ca.ExportCertificatePem(), ca.GetRSAPrivateKey()!.ExportPkcs8PrivateKeyPem());
        }

        public static (string CertPem, string KeyPem) Issue(
            string caPem, string caKeyPem, string commonName,
            string[]? eku = null, bool san = true, DateTimeOffset? notAfter = null)
        {
            using var ca = X509Certificate2.CreateFromPem(caPem);
            using var caKey = RSA.Create();
            caKey.ImportFromPem(caKeyPem);
            using var caWithKey = ca.CopyWithPrivateKey(caKey);
            using var leafKey = RSA.Create(2048);
            var request = new CertificateRequest(
                $"CN={commonName}", leafKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            if (san)
            {
                var sanBuilder = new SubjectAlternativeNameBuilder();
                sanBuilder.AddDnsName(commonName);
                request.CertificateExtensions.Add(sanBuilder.Build());
            }
            var ekuOids = new OidCollection();
            foreach (var o in eku ?? [ServerAuth])
                ekuOids.Add(new Oid(o));
            request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(ekuOids, critical: false));
            var notAfterClamped = notAfter ?? DateTimeOffset.UtcNow.AddDays(30);
            if (notAfterClamped > caWithKey.NotAfter)
                notAfterClamped = caWithKey.NotAfter;
            // notBefore всегда ПОЗЖЕ NotBefore эмиттера (CertificateRequest.Create
            // кидает ArgumentException иначе), в т.ч. для просроченных листов.
            var notBefore = notAfterClamped < DateTimeOffset.UtcNow
                ? notAfterClamped.AddMinutes(-30)
                : DateTimeOffset.UtcNow.AddMinutes(-30);
            if (notBefore < caWithKey.NotBefore)
                notBefore = caWithKey.NotBefore.AddMinutes(1);
            using var cert = request.Create(caWithKey, notBefore, notAfterClamped, RandomNumberGenerator.GetBytes(8));
            return (cert.ExportCertificatePem(), leafKey.ExportPkcs8PrivateKeyPem());
        }
    }
}
