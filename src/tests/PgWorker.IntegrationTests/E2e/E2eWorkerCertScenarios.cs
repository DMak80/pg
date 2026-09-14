using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using Shared.Etcd.Client;
using PgWorker.IntegrationTests.Docker;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E полного цикла управляемого серта API (spec §5 Ф4, §7 кр.1/2/6/7):
// генерация пары → ключ /workers/api_tls/pgworker → старт воркера на etcd-серте
// → замена ключа → pending (факт на грани старый) → POST /api/restart →
// повторный подъём на новом серте → DELETE ключа → рестарт → env-фоллбек
// (unmanaged). Отдельный сценарий: битый ключ — fail-fast старта.
// Изоляция: per-сценарный E2eEnvironment (своя сеть/etcd, guid-имена, полный
// teardown при любом исходе — docs/e2e-isolation.md). Прод-«политика docker
// restart: unless-stopped» имитируется повторным StartHostAsync того же
// extraEnv — E2E-хосты не контейнеры, перезапускает их сам сценарий.
[Collection("e2e-serial")]
public class E2eWorkerCertScenarios
{
    private const string ManagedKey = "/workers/api_tls/pgworker";
    private const string DiscoveryPrefix = "/pgworker/api/";

    private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };

    // ===== Сценарий 1: полный жизненный цикл =====

    [Fact]
    public async Task WorkerCert_Lifecycle_AppliedPendingRestartEnvFallback()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("worker-cert", ct: ct);
        try
        {
            // Arrange: ДВЕ пары (старая/новая) для управляемого ключа + env-пара
            // для фоллбека. PGW_API_TLS_CERT/KEY перекрываются extraEnv: env-пару
            // выбираем СВОЮ (её thumbprint знает тест). CLIENT_CA НЕ перекрываем —
            // клиентский серт mTLS выпускаем от фикстурного CA (иначе проба
            // готовности фикстуры перестаёт проходить хендшейк).
            var (clientPem, clientKeyPem) = E2eTestPki.Issue(
                E2eEnvironment.InstallCaPem, E2eEnvironment.InstallCaKeyPem,
                "certsc-client", ["certsc-client"], ip: null);
            var (oldPem, oldKey) = SelfSignedPair("pgworker-old");
            var (newPem, newKey) = SelfSignedPair("pgworker-new");
            var (envPem, envKey) = SelfSignedPair("pgworker-env");
            var oldThumb = ThumbprintOf(oldPem);
            var newThumb = ThumbprintOf(newPem);
            var envThumb = ThumbprintOf(envPem);
            var extraEnv = new Dictionary<string, string>
            {
                ["PGW_API_TLS_CERT"] = envPem,
                ["PGW_API_TLS_KEY"] = envKey,
            };

            // ---------- Act 1: ключ со СТАРОЙ парой → старт воркера ----------
            await PutManagedKeyAsync(fx, oldPem, oldKey, ct);
            await using var p1 = await fx.StartHostAsync("cert-p1", extraEnv: extraEnv, ct: ct);

            // Assert 1: грань на etcd-серте — /healthz 200 ТОЛЬКО при доверии
            // старому thumbprint; дискавери-ключ сообщает старый thumbprint.
            var (oldUrl, discoveryOld) = await WaitForDiscoveryAsync(fx, ct);
            discoveryOld.Should().Be(oldThumb, "дискавери-ключ обязан нести thumbprint применённого серта");
            using (var client = TlsClient(clientPem, clientKeyPem, oldThumb))
            {
                var response = await client.GetAsync($"{oldUrl}/healthz", ct);
                response.StatusCode.Should().Be(HttpStatusCode.OK);
            }

            // ---------- Act 2: ключ заменён на НОВУЮ пару ----------
            await PutManagedKeyAsync(fx, newPem, newKey, ct);

            // Assert 2: применение — только рестартом (spec §2 п.2): грань всё
            // ещё на старом серте — хендшейк с доверием НОВОМУ thumb отказан,
            // со СТАРЫМ — проходит.
            using (var trustNew = TlsClient(clientPem, clientKeyPem, newThumb))
                await Assert.ThrowsAnyAsync<Exception>(
                    () => trustNew.GetAsync($"{oldUrl}/healthz", ct));
            using (var trustOld = TlsClient(clientPem, clientKeyPem, oldThumb))
            {
                var still = await trustOld.GetAsync($"{oldUrl}/healthz", ct);
                still.StatusCode.Should().Be(HttpStatusCode.OK, "грань живёт на старом серте до рестарта");
            }

            // ---------- Act 3: POST /api/restart → self-stop → подъём ----------
            int exitCode;
            using (var restartClient = TlsClient(clientPem, clientKeyPem, oldThumb))
            {
                restartClient.DefaultRequestHeaders.Add("X-Requested-By", "e2e-cert-scenario");
                using var response = await restartClient.PostAsync($"{oldUrl}/api/restart", null, ct);
                response.StatusCode.Should().Be(HttpStatusCode.Accepted);
                var body = await response.Content.ReadAsStringAsync(ct);
                body.Should().Contain("restarting");
            }

            var exited = await E2eFixture.WaitForAsync(
                () => Task.FromResult(p1.Process.HasExited), TimeSpan.FromSeconds(30), ct);
            exited.Should().BeTrue("graceful self-stop обязан завершить процесс после ~1 c");
            exitCode = p1.Process.HasExited ? p1.Process.ExitCode : -1;
            exitCode.Should().Be(0, "штатный graceful stop — exit 0");

            // docker-политика restart: unless-stopped в e2e имитируется новым
            // инстансом с тем же контуром: при старте перечитан etcd-ключ.
            await using var p2 = await fx.StartHostAsync("cert-p2", extraEnv: extraEnv, ct: ct);
            var (newUrl, discoveryNew) = await WaitForDiscoveryAsync(fx, ct);
            discoveryNew.Should().Be(newThumb, "дискавери-ключ переподставился с новым thumbprint");
            using (var client = TlsClient(clientPem, clientKeyPem, newThumb))
            {
                var response = await client.GetAsync($"{newUrl}/healthz", ct);
                response.StatusCode.Should().Be(HttpStatusCode.OK, "грань поднялась на новом серте");
            }

            // ---------- Act 4: DELETE ключа → рестарт → env-фоллбек ----------
            var deleted = await fx.Gateway.DeleteAsync(fx.EtcdEndpoint, ManagedKey, prefix: false, ct);
            deleted.IsSuccess.Should().BeTrue("ключ управляемого серта должен удаляться");
            p2.Kill(); // рестарт: смерть процесса + повторный подъём = политика docker
            await using var p3 = await fx.StartHostAsync("cert-p3", extraEnv: extraEnv, ct: ct);

            var (envUrl, discoveryEnv) = await WaitForDiscoveryAsync(fx, ct);
            discoveryEnv.Should().Be(envThumb, "после удаления ключа воркер на env-серте (unmanaged)");
            using (var client = TlsClient(clientPem, clientKeyPem, envThumb))
            {
                var response = await client.GetAsync($"{envUrl}/healthz", ct);
                response.StatusCode.Should().Be(HttpStatusCode.OK);
            }
        }
        catch (Exception)
        {
            // Телеметрия E2E (docs/e2e-launch.md): упавший сценарий оставляет
            // окружение для разбора (teardown остановит, но не удалит).
            fx.MarkFailed();
            throw;
        }
    }

    // ===== Сценарий 2: битый ключ — fail-fast старта =====

    [Fact]
    public async Task WorkerCert_BrokenKey_StartFails()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("worker-cert-broken", ct: ct);
        try
        {
            // Arrange: ключ с битым JSON — явное намерение оператора, тихий
            // fallback на env запрещён (spec §3.2 п.1).
            var put = await fx.Gateway.PutAsync(
                fx.EtcdEndpoint, ManagedKey, "{not-json", null, ct);
            put.IsSuccess.Should().BeTrue();
            var (clientPem, clientKeyPem) = E2eTestPki.Issue(
                E2eEnvironment.InstallCaPem, E2eEnvironment.InstallCaKeyPem,
                "certbroken-client", ["certbroken-client"], ip: null);
            var (envPem, envKey) = SelfSignedPair("pgworker-broken-env");
            var extraEnv = new Dictionary<string, string>
            {
                ["PGW_API_TLS_CERT"] = envPem,
                ["PGW_API_TLS_KEY"] = envKey,
            };

            // Act: старт воркера — читатель ДО Kestrel обязан уронить процесс.
            var started = await Record.ExceptionAsync(()
                => fx.StartHostAsync("cert-broken", extraEnv: extraEnv, ct: ct));

            // Assert: процесс упал с сообщением про ключ (exit != 0, ≤30 с —
            // бюджет readiness фикстуры).
            started.Should().BeOfType<ApplicationException>(
                "битый управляемый ключ обязан fail-fast старта");
            started!.Message.Should().Contain("упал при старте");
            started.Message.Should().Contain(ManagedKey);
        }
        catch (Exception)
        {
            fx.MarkFailed();
            throw;
        }
    }

    // ===== Хелперы =====

    // Самоподписанный лист-пара (SAN localhost/127.0.0.1, EKU serverAuth):
    // материалы управляемого ключа сценария — доверие по thumbprint.
    private static (string CertPem, string KeyPem) SelfSignedPair(string commonName)
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            $"CN={commonName}", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var san = new SubjectAlternativeNameBuilder();
        san.AddDnsName("localhost");
        san.AddIpAddress(System.Net.IPAddress.Loopback);
        request.CertificateExtensions.Add(san.Build());
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
            [new Oid("1.3.6.1.5.5.7.3.1")], critical: false));
        using var cert = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddDays(30));
        return (cert.ExportCertificatePem(), rsa.ExportPkcs8PrivateKeyPem());
    }

    private static string ThumbprintOf(string certPem)
    {
        using var cert = X509Certificate2.CreateFromPem(certPem);
        return Convert.ToHexString(SHA256.HashData(cert.RawData)).ToLowerInvariant();
    }

    // PUT ключа /workers/api_tls/pgworker — панельный путь имитируем прямым
    // put: панель — единственный писатель, формат value идентичен spec §3.1.
    private static async Task PutManagedKeyAsync(
        E2eEnvironment fx, string certPem, string keyPem, CancellationToken ct)
    {
        var value = JsonSerializer.Serialize(new { cert_pem = certPem, key_pem = keyPem });
        var put = await fx.Gateway.PutAsync(fx.EtcdEndpoint, ManagedKey, value, null, ct);
        put.IsSuccess.Should().BeTrue();
    }

    // Дискавери-ключи /pgworker/api/<id>: ждём появления ключа (lease-подстановка
    // после старта) и возвращаем ФАКТИЧЕСКИЙ url+thumbprint НОВЕЙШЕГО инстанса
    // (максимум since_unix — старый ключ мог доживать по TTL lease): ассерты
    // наверху сверяют факт с ожиданием, а не эхируют вход.
    private static async Task<(string Url, string Thumbprint)> WaitForDiscoveryAsync(
        E2eEnvironment fx, CancellationToken ct)
    {
        string? url = null;
        string? actualThumb = null;
        var found = await E2eFixture.WaitForAsync(async () =>
        {
            var range = await fx.Gateway.RangeAsync(fx.EtcdEndpoint, DiscoveryPrefix, ct);
            if (!range.IsSuccess)
                return false;
            long bestSince = -1;
            foreach (var kv in range.Value)
            {
                try
                {
                    using var doc = JsonDocument.Parse(kv.Value);
                    var root = doc.RootElement;
                    if (!root.TryGetProperty("cert_thumbprint", out var thumb)
                        || thumb.GetString() is not { Length: > 0 } t
                        || !root.TryGetProperty("url", out var jsonUrl)
                        || jsonUrl.GetString() is not { Length: > 0 } u)
                        continue;
                    var since = root.TryGetProperty("since_unix", out var s)
                        && s.ValueKind == JsonValueKind.Number
                        ? s.GetInt64()
                        : 0;
                    if (since > bestSince)
                    {
                        bestSince = since;
                        url = u;
                        actualThumb = t;
                    }
                }
                catch (JsonException)
                {
                    // чужой/переходный формат — пропускаем
                }
            }

            return bestSince >= 0;
        }, TimeSpan.FromSeconds(30), ct);
        found.Should().BeTrue("дискавери-ключ с cert_thumbprint обязан появиться");
        return (url!, actualThumb!);
    }

    // mTLS-клиент, доверяющий серверу ТОЛЬКО по SHA-256 thumbprint (панель
    // сама записала серт — spec §3.3 п.3); клиентский серт — от scenario-CA.
    private static HttpClient TlsClient(string clientPem, string clientKeyPem, string trustedThumb)
    {
        var pair = X509Certificate2.CreateFromPem(clientPem, clientKeyPem);
        var clientCert = X509CertificateLoader.LoadPkcs12(pair.Export(X509ContentType.Pkcs12), null);
        return new HttpClient(new SocketsHttpHandler
        {
            SslOptions = new System.Net.Security.SslClientAuthenticationOptions
            {
                // TLS 1.2: macOS SslStream не шлёт клиентские серты в TLS 1.3.
                EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12,
                ClientCertificates = [clientCert],
                RemoteCertificateValidationCallback = (_, cert, _, _) =>
                    cert is X509Certificate2 c2
                    && Convert.ToHexString(SHA256.HashData(c2.RawData)).ToLowerInvariant() == trustedThumb,
            },
        })
        { Timeout = TimeSpan.FromSeconds(5) };
    }
}
