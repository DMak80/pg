using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
using Shared.Etcd.Client;
using PgWorker.IntegrationTests.Docker;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E REST-TLS Patroni (t22, spec §8 п.3): :8008 https-only (серт ноды из
// per-contour CA), basic-auth мутаций (401 без креда / 200 с per-cluster
// парой), P11 мастер-ключ через https /primary после рестарта мастера, шаг
// конвергенции легаси-контейнера (без SSL_RESTAPI_*) с сохранением volume.
// Изолированное окружение E2eEnvironment — guid во всех именах, полный
// teardown при любом исходе, ассерт чистоты (docs/e2e-isolation.md).
[Collection("e2e-serial")]
public class E2ePatroniRestTlsScenarios
{
    private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };

    private E2eEnvironment Fx = null!;

    private string Endpoint => Fx.EtcdEndpoint;

    private EtcdGateway G => Fx.Gateway;

    [Fact]
    public async Task E2ePatroniRestTls_HttpsOnlyAuthAndP11()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        await using var fx = await E2eEnvironment.StartAsync("rest-tls", ct: ct);
        Fx = fx;
        var cluster = $"rtls{Fx.ClusterTag}";
        try
        {
            // NodeDeadSec=60: рестарт/миграция ноды не обязана проходить под
            // дефолтные 6 c e2e — spilo-бут с существующим PGDATA занимает
            // больше, rebuild по недоступности снёс бы volume (не наша цель).
            var extraEnv = new Dictionary<string, string>
            {
                ["PgWorker__Thresholds__NodeDeadSec"] = "60",
            };

            // Arrange: сид (NOT_INITIALIZED, 1 шард × 2 ноды) → воркер → Active.
            await SeedClusterAsync(cluster);
            string replica;
            await using (var p1 = await Fx.StartHostAsync("rtls-p1", extraEnv: extraEnv, ct: ct))
            {
                var provisioned = await E2ePhase.WaitAsync(fx, "rtls-provisioning",
                    () => ActiveAsync(cluster), TimeSpan.FromSeconds(360), ct);
                provisioned.Should().BeTrue($"provisioning обязан дойти до Active; work={await WorkDumpAsync(cluster, ct)}");

                var addresses = await PortallocAsync(cluster);
                addresses.Should().ContainKey("shard1/shard1a");
                var patroniPort = addresses["shard1/shard1a"].Patroni;
                patroniPort.Should().BePositive("порт Patroni ноды опубликован");

                // (а) http-only отказ: голый HTTP на TLS-порт не отвечает 200
                // (короткий таймаут: сервер рвёт соединение сразу).
                var plainRefused = false;
                try
                {
                    using var plainClient = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
                    using var plain = await plainClient.GetAsync(
                        $"http://127.0.0.1:{patroniPort}/cluster", ct);
                    plainRefused = !plain.IsSuccessStatusCode;
                }
                catch (Exception)
                {
                    plainRefused = true; // транспорт схлопнулся на TLS-порте
                }

                plainRefused.Should().BeTrue("голый HTTP на :8008 не работает (транспорт — TLS)");

                // (а) https с ca.pem контура → 200 + JSON членов.
                using var https = E2eEnvironment.CreatePatroniHttpsClient();
                using var clusterResponse = await https.GetAsync($"https://127.0.0.1:{patroniPort}/cluster", ct);
                clusterResponse.StatusCode.Should().Be(HttpStatusCode.OK);
                var body = await clusterResponse.Content.ReadAsStringAsync(ct);
                body.Should().Contain("members", "ответ — Patroni /cluster JSON");

                // (б) серт ноды верифицируется цепочкой против CA контура.
                var leafDer = await CaptureServerCertificateAsync(patroniPort, ct);
                leafDer.Should().NotBeNull("TLS-хендшейк отдаёт серверный серт");
                ChainValidAgainstInstallCa(leafDer!).Should().BeTrue("серт ноды выпущен per-contour CA");

                // (в) мутации — basic-auth per-cluster: без Authorization — 401.
                var restPassword = (await GetOrNullAsync($"/clusters/{cluster}/rest_password"))!.Value;
                restPassword.Should().NotBeNullOrWhiteSpace("ensure положил седьмой ключ");
                using var noAuth = new HttpRequestMessage(HttpMethod.Patch,
                    $"https://127.0.0.1:{patroniPort}/config")
                {
                    Content = new StringContent("{\"ttl\":30}", Encoding.UTF8, "application/json"),
                };
                using var refused = await https.SendAsync(noAuth, ct);
                refused.StatusCode.Should().Be(HttpStatusCode.Unauthorized,
                    "unsafe-эндпоинт без basic-auth закрыт");

                // (в) с Authorization: Basic patroni:<rest_password> — 200.
                using var withAuth = new HttpRequestMessage(HttpMethod.Patch,
                    $"https://127.0.0.1:{patroniPort}/config")
                {
                    Content = new StringContent("{\"ttl\":30}", Encoding.UTF8, "application/json"),
                };
                withAuth.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue(
                    "Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes($"patroni:{restPassword}")));
                using var patched = await https.SendAsync(withAuth, ct);
                patched.StatusCode.Should().Be(HttpStatusCode.OK,
                    "с per-cluster парой мутация проходит");

                // (д) P11: рестарт мастера → мастер-ключ жив ≤60 c (callback
                // on_start узнал роль через https /primary, lease TTL 5 c).
                var master = await PrimaryNodeAsync(cluster, ct);
                master.Should().NotBeNullOrWhiteSpace("primary резолвится пробой /primary");
                replica = master == "shard1a" ? "shard1b" : "shard1a";
                var masterContainer = $"pgw-{cluster}-shard1-{master}";
                await Fx.RunDockerAsync(["restart", masterContainer], ct);
                var keyAlive = await E2eFixture.WaitForAsync(async () =>
                    (await GetOrNullAsync($"/clusters/{cluster}/shards/shard1/master")) is { Value.Length: > 0 },
                    TimeSpan.FromSeconds(60), ct);
                keyAlive.Should().BeTrue(
                    "мастер-ключ вернулся после рестарта — master-lease прошёл https /primary (P11)");

                // (е) конвергенция легаси: маркер в volume реплики (не лидера).
                var replicaContainer = $"pgw-{cluster}-shard1-{replica}";
                await Fx.RunDockerAsync(["exec", replicaContainer,
                    "touch", "/home/postgres/pgdata/resttls-marker"], ct);
            }

            // Воркер остановлен: подмена РЕПЛИКИ легаси-контейнером без гонки.
            var legacy = $"pgw-{cluster}-shard1-{replica}";
            var legacyVolume = $"{legacy}-data";
            var legacyAddr = (await PortallocAsync(cluster))[$"shard1/{replica}"];
            await Fx.RunDockerAsync(["rm", "-f", legacy], ct);
            await Fx.RunDockerAsync(
                ["run", "-d", "--name", legacy, "--entrypoint", "sh",
                 "-p", $"{legacyAddr.Pg}:5432", "-p", $"{legacyAddr.Patroni}:8008",
                 "-v", $"{legacyVolume}:/home/postgres/pgdata",
                 E2eEnvironment.NodeImage, "-c", "sleep 900"], ct);
            var legacyEnv = await Fx.RunDockerAsync(
                ["inspect", "-f", "{{range .Config.Env}}{{println .}}{{end}}", legacy], ct);
            legacyEnv.Should().NotContain("SSL_RESTAPI_CERTIFICATE", "легаси-контейнер без REST-TLS env");

            await using (var p2 = await Fx.StartHostAsync("rtls-p2", extraEnv: extraEnv, ct: ct))
            {
                // Тик надзора обязан пересоздать легаси-ноду с REST-TLS env.
                var migrated = await E2ePhase.WaitAsync(fx, "rtls-convergence",
                    async () => (await Fx.RunDockerAsync(
                        ["inspect", "-f", "{{range .Config.Env}}{{println .}}{{end}}", legacy], ct))
                    .Contains("SSL_RESTAPI_CERTIFICATE"),
                    TimeSpan.FromSeconds(120), ct);
                migrated.Should().BeTrue("шаг конвергенции обязан довести легаси-ноду до TLS-env");

                // Volume пережил пересоздание (маркер на месте).
                var marker = await Fx.RunDockerAsync(
                    ["exec", legacy, "test", "-f", "/home/postgres/pgdata/resttls-marker"], ct);
                marker.Should().BeEmpty("volume и данные пережили миграцию (exec код 0)");

                // Кластер снова Active после миграции.
                var activeAgain = await E2ePhase.WaitAsync(fx, "rtls-active-again",
                    () => ActiveAsync(cluster), TimeSpan.FromSeconds(360), ct);
                activeAgain.Should().BeTrue($"кластер Active после миграции; work={await WorkDumpAsync(cluster, ct)}");
            }
        }
        catch
        {
            // docs/e2e-launch.md §3: упавший сценарий — окружение остановить,
            // не удалить (телеметрия в артефактах + живые объекты для разбора).
            Fx.MarkFailed();
            throw;
        }
    }

    // ===== хелперы =====

    private async Task SeedClusterAsync(string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/config",
            $$"""{"buckets":2,"dbname":"{{cluster}}","created_unix":1755800000,"state":"NOT_INITIALIZED","bucket_admin_password":"{{E2eFixture.BucketAdminPassword}}"}""",
            null, ct);
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard1/replicas", "2", null, ct);
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard1/nodes/shard1a/state", "NOT_INITIALIZED", null, ct);
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard1/nodes/shard1b/state", "NOT_INITIALIZED", null, ct);
        await G.PutAsync(Endpoint, $"/service/{cluster}-shard1/request_cpu", "2", null, ct);
        await G.PutAsync(Endpoint, $"/service/{cluster}-shard1/request_mem", "2Gi", null, ct);
        for (var i = 0; i < 2; i++)
        {
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/routing/bucket_{i}", "shard1", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/status/bucket_{i}",
                """{"state":"NOT_INITIALIZED"}""", null, ct);
        }
    }

    private async Task<bool> ActiveAsync(string cluster)
    {
        var config = await GetOrNullAsync($"/clusters/{cluster}/config");
        if (config is null || JsonSerializer
                .Deserialize<Dictionary<string, JsonElement>>(config.Value)!.ContainsKey("state"))
            return false;
        if (await GetOrNullAsync($"/clusters/{cluster}/shards/shard1/dsn") is null)
            return false;
        foreach (var node in new[] { "shard1a", "shard1b" })
            if ((await GetOrNullAsync($"/clusters/{cluster}/shards/shard1/nodes/{node}/state"))?.Value != "RUNNING")
                return false;

        return (await RangeAsync($"/clusters/{cluster}/buckets/status/")).Count == 0;
    }

    private async Task<Kv?> GetOrNullAsync(string key)
        => (await G.GetAsync(Endpoint, key, TestContext.Current.CancellationToken)).Value;

    private async Task<IReadOnlyList<Kv>> RangeAsync(string prefix)
        => (await G.RangeAsync(Endpoint, prefix, TestContext.Current.CancellationToken)).Value;

    private async Task<Dictionary<string, NodeAddr>> PortallocAsync(string cluster)
    {
        var kv = await GetOrNullAsync($"/pgworker/portalloc/{cluster}");
        if (kv is null)
            return [];
        return JsonSerializer.Deserialize<Dictionary<string, NodeAddr>>(kv.Value, Json) ?? [];
    }

    private sealed record NodeAddr(string Host, int Pg, int Patroni, int Doorman);

    // Фактический primary шарда: нода, отвечающая 200 на /primary (https).
    private async Task<string?> PrimaryNodeAsync(string cluster, CancellationToken ct)
    {
        var addresses = await PortallocAsync(cluster);
        foreach (var (key, addr) in addresses
                     .Where(p => p.Key.StartsWith("shard1/", StringComparison.Ordinal))
                     .OrderBy(p => p.Key, StringComparer.Ordinal))
        {
            try
            {
                using var client = E2eEnvironment.CreatePatroniHttpsClient();
                using var response = await client.GetAsync($"https://127.0.0.1:{addr.Patroni}/primary", ct);
                if (response.IsSuccessStatusCode)
                    return key.Split('/')[1];
            }
            catch (Exception)
            {
                // рестарт/бут ноды — не primary, пробуем следующую
            }
        }

        return null;
    }

    private async Task<string> WorkDumpAsync(string cluster, CancellationToken ct)
        => (await GetOrNullAsync($"/pgworker/work/{cluster}"))?.Value ?? "<нет work-ключа>";

    // Серверный серт ноды: перехват в TLS-хендшейке (без доверия — только
    // захват). Внутри callback копируем DER: хендл сертификата живёт лишь
    // пока открыт поток (после Dispose чтение бросает invalid handle).
    private static async Task<byte[]?> CaptureServerCertificateAsync(int port, CancellationToken ct)
    {
        using var tcp = new TcpClient();
        await tcp.ConnectAsync(IPAddress.Loopback, port, ct);
        using var ssl = new SslStream(tcp.GetStream(), leaveInnerStreamOpen: false);
        byte[]? captured = null;
        var options = new SslClientAuthenticationOptions
        {
            TargetHost = "127.0.0.1",
            RemoteCertificateValidationCallback = (_, certificate, _, _) =>
            {
                captured = certificate?.GetRawCertData();
                return true; // захват без доверия
            },
        };
        await ssl.AuthenticateAsClientAsync(options, ct);
        return captured;
    }

    // Цепочка серта к per-contour CA (CustomRootTrust, без hostname); вход —
    // DER из хендшейка (ре-импорт — чистый хендл, macOS-паттерн).
    private static bool ChainValidAgainstInstallCa(byte[] leafDer)
    {
        using var ca = X509Certificate2.CreateFromPem(E2eEnvironment.InstallCaPem);
        using var leaf = System.Security.Cryptography.X509Certificates.X509CertificateLoader
            .LoadCertificate(leafDer);
        using var chain = new X509Chain();
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.Add(ca);
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        return chain.Build(leaf);
    }
}
