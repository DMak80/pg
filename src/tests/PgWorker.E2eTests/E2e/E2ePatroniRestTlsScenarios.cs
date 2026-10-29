using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E REST-TLS Patroni (t22, spec §8 п.3): :8008 https-only (серт ноды из
// per-contour CA), basic-auth мутаций (401 без креда / 200 с per-cluster
// парой), P11 мастер-ключ через https /primary после рестарта мастера, шаг
// конвергенции легаси-контейнера (без SSL_RESTAPI_*) с сохранением volume.
// Изолированное окружение E2eEnvironment — guid во всех именах, полный
// teardown при любом исходе, ассерт чистоты (docs/e2e-isolation.md).
[Collection("e2e-serial")]
public sealed class E2ePatroniRestTlsScenarios(E2ePatroniRestTlsScenariosFx Ctx) : IClassFixture<E2ePatroniRestTlsScenariosFx>
{
    private E2eEnvironment Fx => Ctx.Fx;

    private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };


    private string Endpoint => Fx.EtcdEndpoint;

    private EtcdGateway G => Fx.Gateway;

    [Fact]
    public async Task E2ePatroniRestTls_HttpsOnlyAuthAndP11()
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMinutes(3)); // потолок теста 3 мин: неверное ожидание не может длиться дольше (приказ владельца)
        var ct = cts.Token;
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
                var provisioned = await E2ePhase.WaitAsync(Fx, "rtls-provisioning",
                    E2eEnvironment.ProvisionStuckFailFast(Fx, GetOrNullAsync, cluster, () => ActiveAsync(cluster), shards: ["shard1"]), TimeSpan.FromSeconds(90), ct);
                provisioned.Should().BeTrue($"provisioning must reach Active; work={await WorkDumpAsync(cluster, ct)}");

                var addresses = await PortallocAsync(cluster);
                addresses.Should().ContainKey("shard1/shard1a");
                // Публикация REST ноды на хост: localhost:{динамический порт}
                // (docs/e2e-isolation.md §2; hostname серта не сверяется — цепочка).
                var addr1a = addresses["shard1/shard1a"];

                // (а) http-only отказ: голый HTTP на TLS-порт не отвечает 200
                // (короткий таймаут: сервер рвёт соединение сразу).
                var plainRefused = false;
                try
                {
                    using var plainClient = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
                    using var plain = await plainClient.GetAsync(
                        $"http://localhost:{addr1a.Patroni}/cluster", ct);
                    plainRefused = !plain.IsSuccessStatusCode;
                }
                catch (Exception)
                {
                    plainRefused = true; // транспорт схлопнулся на TLS-порте
                }

                plainRefused.Should().BeTrue("plain HTTP on :8008 does not work (the transport is TLS)");

                // (а) https с ca.pem контура → 200 + JSON членов.
                var https = Fx.NewPatroniHttpClient();
                using var clusterResponse = await https.GetAsync($"https://localhost:{addr1a.Patroni}/cluster", ct);
                clusterResponse.StatusCode.Should().Be(HttpStatusCode.OK);
                var body = await clusterResponse.Content.ReadAsStringAsync(ct);
                body.Should().Contain("members", "the response is a Patroni /cluster JSON");

                // (б) серт ноды верифицируется цепочкой против CA контура.
                var leafDer = await CaptureServerCertificateAsync("localhost", addr1a.Patroni, ct);
                leafDer.Should().NotBeNull("the TLS handshake yields the server certificate");
                ChainValidAgainstInstallCa(leafDer!).Should().BeTrue("the node certificate is issued by the per-contour CA");

                // (в) мутации — basic-auth per-cluster: без Authorization — 401.
                var restPassword = (await GetOrNullAsync($"/clusters/{cluster}/rest_password"))!.Value;
                restPassword.Should().NotBeNullOrWhiteSpace("ensure put the seventh key");
                using var noAuth = new HttpRequestMessage(HttpMethod.Patch,
                    $"https://localhost:{addr1a.Patroni}/config")
                {
                    Content = new StringContent("{\"ttl\":30}", Encoding.UTF8, "application/json"),
                };
                using var refused = await https.SendAsync(noAuth, ct);
                refused.StatusCode.Should().Be(HttpStatusCode.Unauthorized,
                    "the unsafe endpoint without basic-auth is closed");

                // (в) с Authorization: Basic patroni:<rest_password> — 200.
                using var withAuth = new HttpRequestMessage(HttpMethod.Patch,
                    $"https://localhost:{addr1a.Patroni}/config")
                {
                    Content = new StringContent("{\"ttl\":30}", Encoding.UTF8, "application/json"),
                };
                withAuth.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue(
                    "Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes($"patroni:{restPassword}")));
                using var patched = await https.SendAsync(withAuth, ct);
                patched.StatusCode.Should().Be(HttpStatusCode.OK,
                    "with the per-cluster pair the mutation goes through");

                // (д) P11: рестарт мастера → мастер-ключ жив ≤60 c (callback
                // on_start узнал роль через https /primary, lease TTL 5 c).
                var master = await PrimaryNodeAsync(cluster, ct);
                master.Should().NotBeNullOrWhiteSpace("the primary is resolved by the /primary probe");
                replica = master == "shard1a" ? "shard1b" : "shard1a";
                var masterContainer = $"pgw-{cluster}-shard1-{master}";
                await Fx.RunDockerAsync(["restart", masterContainer], ct);
                var keyAlive = await E2eFixture.WaitForAsync(async () =>
                    (await GetOrNullAsync($"/clusters/{cluster}/shards/shard1/master")) is { Value.Length: > 0 },
                    TimeSpan.FromSeconds(60), ct);
                keyAlive.Should().BeTrue(
                    "the master key returned after the restart — the master-lease passed https /primary (P11)");

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
            // Легаси-контейнер имитирует ОПЕРАТОРСКУЮ ноду: тройка из portalloc
            // публикуется, как у чужого контейнера в проде (адопция — не тестовая
            // инфраструктура; порты — слоты окна контура, коллизий нет).
            await Fx.RunDockerAsync(
                ["run", "-d", "--name", legacy, "--entrypoint", "sh",
                 "-p", $"{legacyAddr.Pg}:5432", "-p", $"{legacyAddr.Patroni}:8008",
                 "-v", $"{legacyVolume}:/home/postgres/pgdata",
                 E2eEnvironment.NodeImage, "-c", "sleep 900"], ct);
            var legacyEnv = await Fx.RunDockerAsync(
                ["inspect", "-f", "{{range .Config.Env}}{{println .}}{{end}}", legacy], ct);
            legacyEnv.Should().NotContain("SSL_RESTAPI_CERTIFICATE", "a legacy container without REST-TLS env");

            await using (var p2 = await Fx.StartHostAsync("rtls-p2", extraEnv: extraEnv, ct: ct))
            {
                // Тик надзора обязан пересоздать легаси-ноду с REST-TLS env.
                // t41-фикс гоночного окна: env читается и у Created-контейнера
                // (между create и start recreate-пути) — критерий конвергенции
                // обязан требовать ЖИВУЮ ноду, иначе marker-exec ниже падает
                // на незапущенном контейнере.
                var migrated = await E2ePhase.WaitAsync(Fx, "rtls-convergence",
                    async () =>
                    {
                        var state = await InspectStatusOrNullAsync(legacy, ct);
                        if (state != "running")
                            return false;
                        var env = await Fx.RunDockerAsync(
                            ["inspect", "-f", "{{range .Config.Env}}{{println .}}{{end}}", legacy], ct);
                        return env.Contains("SSL_RESTAPI_CERTIFICATE");
                    },
                    TimeSpan.FromSeconds(90), ct);
                migrated.Should().BeTrue("the convergence step must bring the legacy node to a live node with TLS env");

                // Volume пережил пересоздание (маркер на месте).
                var marker = await Fx.RunDockerAsync(
                    ["exec", legacy, "test", "-f", "/home/postgres/pgdata/resttls-marker"], ct);
                marker.Should().BeEmpty("the volume and data survived the migration (exec code 0)");

                // Кластер снова Active после миграции.
                var activeAgain = await E2ePhase.WaitAsync(Fx, "rtls-active-again",
                    E2eEnvironment.ProvisionStuckFailFast(Fx, GetOrNullAsync, cluster, () => ActiveAsync(cluster), shards: ["shard1"]), TimeSpan.FromSeconds(90), ct);
                activeAgain.Should().BeTrue($"the cluster is Active after the migration; work={await WorkDumpAsync(cluster, ct)}");
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

    // Статус контейнера; null = ещё не создан (воркер не дошёл до recreate).
    // Для поллинга «no such object» — «не готово», а не фатально: легаси-нода
    // перед ожиданием удаляется, и до пересоздания inspect закономерно 404.
    private async Task<string?> InspectStatusOrNullAsync(string container, CancellationToken ct)
    {
        try
        {
            return (await Fx.RunDockerAsync(
                ["inspect", "-f", "{{.State.Status}}", container], ct)).Trim();
        }
        catch (ApplicationException e) when (e.Message.Contains("no such object", StringComparison.Ordinal))
        {
            return null;
        }
    }

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
                var client = Fx.NewPatroniHttpClient();
                using var response = await client.GetAsync(
                    $"https://localhost:{addr.Patroni}/primary", ct);
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
        => (await GetOrNullAsync($"/pgworker/work/{cluster}"))?.Value ?? "<no work key>";

    // Серверный серт ноды: перехват в TLS-хендшейке (без доверия — только
    // захват). Внутри callback копируем DER: хендл сертификата живёт лишь
    // пока открыт поток (после Dispose чтение бросает invalid handle).
    private static async Task<byte[]?> CaptureServerCertificateAsync(string nodeHost, int port, CancellationToken ct)
    {
        using var tcp = new TcpClient();
        await tcp.ConnectAsync(nodeHost, port, ct);
        using var ssl = new SslStream(tcp.GetStream(), leaveInnerStreamOpen: false);
        byte[]? captured = null;
        var options = new SslClientAuthenticationOptions
        {
            TargetHost = nodeHost,
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
    private bool ChainValidAgainstInstallCa(byte[] leafDer)
    {
        using var ca = X509Certificate2.CreateFromPem(Fx.CaPem);
        using var leaf = System.Security.Cryptography.X509Certificates.X509CertificateLoader
            .LoadCertificate(leafDer);
        using var chain = new X509Chain();
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.Add(ca);
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        return chain.Build(leaf);
    }
}

/// <summary>Компаньон-фикстура окружения класса (полный контур на класс:
/// сеть/etcd/CA/окно портов; docs/e2e-isolation.md §1).</summary>
public sealed class E2ePatroniRestTlsScenariosFx : E2eContourFixture
{
    protected override string Slug => "rest-tls";
}
