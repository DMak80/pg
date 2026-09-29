using System.Net;
using System.Net.Http.Json;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using Shared.Etcd.Client;
using PgWorker.IntegrationTests.Docker;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E strict-режима (t06, spec §6.7): bootstrap несёт strict кластера в
// SPILO_CONFIGURATION и DCS-конфиг; мутация PUT /api/clusters/{c}/config
// (mTLS, тело {"synchronousModeStrict":bool}) — 204; конвергенция DCS тиком
// надзора приводит живой Patroni к false БЕЗ рестартов нод; включение strict
// кластеру с однорепликным шардом — 400 (errors.syncStrict). Изоляция —
// docs/e2e-isolation.md: guid-окружение на Fact, полный teardown при любом
// исходе, телеметрия /tmp/pgw-e2e-artifacts-<guid>/ (E2eEnvironment).
public class E2eStrictScenarios
{
    // Уникальное имя кластера на прогон ({slug}{тег прогона}, docs/e2e-isolation.md §1).
    private string Cluster => $"strict{Fx.ClusterTag}";

    private E2eEnvironment Fx = null!;

    private string Endpoint => Fx.EtcdEndpoint;

    private EtcdGateway G => Fx.Gateway;

    // AAA (t06, AC §6.7 п.1–4): strict-кластер (replicas=2) после подъёма —
    // bootstrap.dcs SPILO_CONFIGURATION и живой GET /config несут
    // synchronous_mode_strict: true; PUT false → 204; конвергенция DCS в
    // пределах тиков надзора приводит /config к false без рестартов нод.
    [Fact]
    public async Task Strict_BootstrapCarriesStrict_MutationConvergesWithoutRestarts()
    {
        // Arrange — окружение + strict-кластер (replicas=2) + воркер-хост.
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("strict-sync", ct: ct);
        Fx = fx;
        try
        {
            await SeedClusterAsync(Cluster, strict: true, shard2Replicas: 2);
            await using var app = await Fx.StartHostAsync("strict", ct: ct);
            var provisioned = await E2eFixture.WaitForAsync(async () =>
                await GetOrNullAsync($"/clusters/{Cluster}/shards/shard1/dsn") is not null
                && await GetOrNullAsync($"/clusters/{Cluster}/shards/shard2/dsn") is not null,
                TimeSpan.FromSeconds(360), ct);
            provisioned.Should().BeTrue("кластер поднялся");

            // Ожидание Active: P4 provisioning переписывает config каноническим
            // JSON БЕЗ поля state (arch/14 §5 A, Д1) — мутация разрешена только
            // Active-кластерам (PUT до этого перехода = 409).
            var active = await E2eFixture.WaitForAsync(async () =>
                await ConfigStateFieldAsync(Cluster) is null, TimeSpan.FromSeconds(60), ct);
            active.Should().BeTrue("кластер перешёл в Active (config без state)");

            // Assert 1 (bootstrap): SPILO_CONFIGURATION первой ноды несёт
            // synchronous_mode_strict: true (t06 — значение из config кластера).
            var spilo = (await ContainerEnvAsync($"pgw-{Cluster}-shard1-shard1a", ct))["SPILO_CONFIGURATION"];
            spilo.Should().Contain("synchronous_mode_strict: true")
                .And.Contain("synchronous_mode: true");

            // Assert 2 (DCS): GET /config первой ноды (порт из portalloc) —
            // живой Patroni-конфиг несёт strict=true (парсинг: ответ Patroni —
            // pretty-JSON с пробелами, подстрочный поиск ненадёжен).
            var addresses = await PortallocAsync(Cluster);
            var shard1a = addresses["shard1/shard1a"];
            (await PatroniSyncStrictAsync(shard1a.Patroni, ct)).Should().BeTrue("bootstrap принёс strict в DCS");

            // Акт подготовки к ассерту «без рестартов»: StartedAt нод кластера
            // и набор контейнеров фиксируются ДО мутации.
            var nodes = addresses.Keys.Select(k => $"pgw-{Cluster}-{k.Replace('/', '-')}").ToList();
            var startedAtBefore = await StartedAtAsync(nodes, ct);

            // Act 3 (мутация): PUT к API воркера — URL и thumbprint серта из
            // дискавери-ключа /pgworker/api/<id>, mTLS-клиент (серт от install
            // CA, доверие серверу — по thumbprint дискавери).
            var (apiUrl, serverThumb) = await WaitForDiscoveryAsync(ct);
            using var client = ApiClient(serverThumb);
            var put = await client.PutAsJsonAsync($"{apiUrl}/api/clusters/{Cluster}/config",
                new { synchronousModeStrict = false }, ct);
            put.StatusCode.Should().Be(HttpStatusCode.NoContent, "мутация принята (204)");

            // Assert 4 (конвергенция): в пределах нескольких тиков надзора
            // GET /config несёт false — БЕЗ рестартов нод (набор контейнеров тот
            // же, StartedAt не свежее момента PUT); журнал несёт фазу
            // dcs-converge с патчем по strict.
            var converged = await E2eFixture.WaitForAsync(async () =>
                await PatroniSyncStrictAsync(shard1a.Patroni, ct) == false,
                TimeSpan.FromSeconds(360), ct);
            converged.Should().BeTrue("конвергенция DCS привела /config к false");

            var startedAtAfter = await StartedAtAsync(nodes, ct);
            startedAtAfter.Keys.Should().Equal(startedAtBefore.Keys, "набор контейнеров не менялся (нет рестартов)");
            foreach (var (name, startedAt) in startedAtAfter)
                startedAt.Should().Be(startedAtBefore[name], $"нода {name} не перезапускалась (StartedAt прежний)");

            var work = (await GetOrNullAsync($"/pgworker/work/{Cluster}"))!.Value;
            // Журнал фаз — JSON (WorkJournal): вложенный diff-патч лежит в
            // строковом поле last_error, где кавычки экранированы внешней
            // сериализацией — подстрочный поиск по сырому значению не находит
            // патч; разбираем документ и ищем strict в распакованном last_error.
            using var workDoc = JsonDocument.Parse(work);
            var workRoot = workDoc.RootElement;
            workRoot.GetProperty("phase").GetString().Should().Be("dcs-converge");
            workRoot.GetProperty("last_error").GetString().Should()
                .Contain("\"synchronous_mode_strict\":false");
        }
        catch
        {
            fx.MarkFailed(); // teardown остановит контейнеры, но не удалит (разбор по логам)
            throw;
        }
        finally
        {
            await fx.DisposeAsync();
        }
    }

    // AAA (t06, AC §6.7 п.5): strict=false кластер с однорепликным шардом —
    // попытка включения strict → 400 (errors.syncStrict); запись в etcd не
    // выполняется (значение остаётся false).
    [Fact]
    public async Task Strict_EnableWithSingleReplicaShard_400()
    {
        // Arrange — окружение + кластер strict=false, shard2 replicas=1.
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("strict-sync", ct: ct);
        Fx = fx;
        try
        {
            await SeedClusterAsync(Cluster, strict: false, shard2Replicas: 1);
            await using var app = await Fx.StartHostAsync("strict-400", ct: ct);
            var provisioned = await E2eFixture.WaitForAsync(async () =>
                await GetOrNullAsync($"/clusters/{Cluster}/shards/shard1/dsn") is not null
                && await GetOrNullAsync($"/clusters/{Cluster}/shards/shard2/dsn") is not null,
                TimeSpan.FromSeconds(360), ct);
            provisioned.Should().BeTrue("кластер поднялся (в т.ч. однорепликный shard2)");

            // Ожидание Active: PUT разрешён только Active-кластерам (config без
            // state — P4 provisioning, arch/14 §5 A), иначе 409.
            var active = await E2eFixture.WaitForAsync(async () =>
                await ConfigStateFieldAsync(Cluster) is null, TimeSpan.FromSeconds(60), ct);
            active.Should().BeTrue("кластер перешёл в Active (config без state)");

            // Act — включение strict кластеру с однорепликным шардом.
            var (apiUrl, serverThumb) = await WaitForDiscoveryAsync(ct);
            using var client = ApiClient(serverThumb);
            var put = await client.PutAsJsonAsync($"{apiUrl}/api/clusters/{Cluster}/config",
                new { synchronousModeStrict = true }, ct);

            // Assert — 400 с errors.syncStrict; значение в etcd не изменилось.
            put.StatusCode.Should().Be(HttpStatusCode.BadRequest);
            var problem = await put.Content.ReadFromJsonAsync<JsonElement>(ct);
            problem.GetProperty("errors").GetProperty("syncStrict")
                .GetArrayLength().Should().BeGreaterThan(0);
            (await GetOrNullAsync($"/clusters/{Cluster}/config"))!.Value
                .Should().Contain("\"synchronous_mode_strict\":false");
        }
        catch
        {
            fx.MarkFailed(); // teardown остановит контейнеры, но не удалит (разбор по логам)
            throw;
        }
        finally
        {
            await fx.DisposeAsync();
        }
    }

    // ===== Хелперы (копии образцов E2eRotateScenarios/E2ePgtuneScenarios/
    // E2eWorkerCertScenarios — файлы сценариев независимы, паттерн репо) =====

    // Сид кластера в стиле панели (образец E2eRotateScenarios.SeedClusterAsync)
    // с полем synchronous_mode_strict в config (t06) и переменным числом реплик
    // shard2 (sub-кейс однорепликного шарда).
    private async Task SeedClusterAsync(string cluster, bool strict, int shard2Replicas)
    {
        var ct = TestContext.Current.CancellationToken;
        var config = $$"""
            {"buckets":2,"dbname":"{{cluster}}","created_unix":1755800000,"state":"NOT_INITIALIZED","synchronous_mode_strict":{{(strict ? "true" : "false")}},"bucket_admin_password":"{{E2eFixture.BucketAdminPassword}}"}
            """;
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/config", config, null, ct);
        foreach (var (shard, replicas) in new[] { ("shard1", 2), ("shard2", shard2Replicas) })
        {
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/replicas",
                replicas.ToString(), null, ct);
            for (var r = 0; r < replicas; r++)
                await G.PutAsync(Endpoint,
                    $"/clusters/{cluster}/shards/{shard}/nodes/{shard}{(char)('a' + r)}/state",
                    "NOT_INITIALIZED", null, ct);
            // Заявки ресурсов панель-создания (arch/14 §2.1 п.4): pgtune-фаза
            // provisioning требует их обязательно.
            await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_cpu", "2", null, ct);
            await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_mem", "8Gi", null, ct);
        }

        for (var i = 0; i < 2; i++)
        {
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/routing/bucket_{i}", $"shard{i + 1}", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/status/bucket_{i}",
                """{"state":"NOT_INITIALIZED"}""", null, ct);
        }
    }

    private async Task<Kv?> GetOrNullAsync(string key)
        => (await G.GetAsync(Endpoint, key, TestContext.Current.CancellationToken)).Value;

    // Поле state из config кластера (null — Active: P4 provisioning пишет
    // канонический JSON без state, arch/14 §5 A Д1).
    private async Task<string?> ConfigStateFieldAsync(string cluster)
    {
        var kv = await GetOrNullAsync($"/clusters/{cluster}/config");
        if (kv is null)
            return "absent";
        using var doc = JsonDocument.Parse(kv.Value);
        return doc.RootElement.TryGetProperty("state", out var state)
            && state.ValueKind == JsonValueKind.String ? state.GetString() : null;
    }

    // portalloc окружения (образец E2eScenarios.PortallocAsync): "shard/noda" → адреса.
    private sealed record NodeAddr(string Host, int Pg, int Patroni, int Doorman);

    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    private async Task<Dictionary<string, NodeAddr>> PortallocAsync(string cluster)
    {
        var kv = await GetOrNullAsync($"/pgworker/portalloc/{cluster}");
        if (kv is null)
            return [];
        return JsonSerializer.Deserialize<Dictionary<string, NodeAddr>>(kv.Value, Json) ?? [];
    }

    private static readonly HttpClient PatroniHttp = new() { Timeout = TimeSpan.FromSeconds(3) };

    // GET /config живого Patroni (образец E2eScenarios:523): порт из portalloc.
    // Ответ Patroni — pretty-JSON (пробелы после двоеточий), поэтому strict
    // читается разбором JSON, а не подстрочным поиском; null — поля нет/не-bool.
    private static async Task<bool?> PatroniSyncStrictAsync(int patroniPort, CancellationToken ct)
    {
        using var response = await PatroniHttp.GetAsync($"http://localhost:{patroniPort}/config", ct);
        response.IsSuccessStatusCode.Should().BeTrue("Patroni /config доступен");
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct));
        return doc.RootElement.TryGetProperty("synchronous_mode_strict", out var strict)
            && strict.ValueKind is JsonValueKind.True or JsonValueKind.False
                ? strict.GetBoolean()
                : null;
    }

    // env контейнера ноды (образец E2ePgtuneScenarios.ContainerEnvAsync).
    private async Task<Dictionary<string, string>> ContainerEnvAsync(string name, CancellationToken ct)
    {
        var json = await Fx.RunDockerAsync(["inspect", name, "--format", "{{json .Config.Env}}"], ct);
        var list = JsonSerializer.Deserialize<List<string>>(json, Json) ?? [];
        return list
            .Select(e => e.Split('=', 2))
            .Where(p => p.Length == 2)
            .GroupBy(p => p[0], StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.First()[1], StringComparer.Ordinal);
    }

    // StartedAt контейнеров нод (ассерт «без рестартов»): имя → StartedAt.
    private async Task<Dictionary<string, string>> StartedAtAsync(
        IReadOnlyList<string> names, CancellationToken ct)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var name in names)
            result[name] = await Fx.RunDockerAsync(["inspect", "-f", "{{.State.StartedAt}}", name], ct);
        return result;
    }

    // Дискавери API воркера (образец E2eWorkerCertScenarios.WaitForDiscoveryAsync):
    // ждём ключ /pgworker/api/<id>, возвращаем ФАКТИЧЕСКИЙ url и thumbprint
    // применённого серверного серта (новейший инстанс по since_unix).
    private async Task<(string Url, string Thumbprint)> WaitForDiscoveryAsync(CancellationToken ct)
    {
        string? url = null;
        string? thumb = null;
        var found = await E2eFixture.WaitForAsync(async () =>
        {
            var range = await G.RangeAsync(Endpoint, "/pgworker/api/", ct);
            if (!range.IsSuccess)
                return false;
            long bestSince = -1;
            foreach (var kv in range.Value)
            {
                try
                {
                    using var doc = JsonDocument.Parse(kv.Value);
                    var root = doc.RootElement;
                    if (!root.TryGetProperty("url", out var jsonUrl)
                        || jsonUrl.GetString() is not { Length: > 0 } u)
                        continue;
                    var since = root.TryGetProperty("since_unix", out var s)
                        && s.ValueKind == JsonValueKind.Number ? s.GetInt64() : 0;
                    if (since <= bestSince)
                        continue;
                    bestSince = since;
                    url = u;
                    thumb = root.TryGetProperty("cert_thumbprint", out var t)
                        ? t.GetString() : null;
                }
                catch (JsonException)
                {
                    // переходный/чужой формат — пропускаем
                }
            }

            return url is not null;
        }, TimeSpan.FromSeconds(30), ct);
        found.Should().BeTrue("дискавери-ключ API воркера обязан появиться");
        thumb.Should().NotBeNullOrEmpty("воркер публикует thumbprint применённого серта");
        return (url!, thumb!);
    }

    // mTLS-клиент API (образец E2eWorkerCertScenarios.TlsClient): клиентский
    // серт от install CA (5-й аргумент ip без дефолта — передаётся явно),
    // доверие серверу — по thumbprint из дискавери (X509Chain к PEM-CA на
    // macOS не строится — оба образца репо обходят цепочку); TLS 1.2 (macOS
    // SslStream не шлёт клиентские серты в TLS 1.3).
    private static HttpClient ApiClient(string trustedThumb)
    {
        var (clientPem, clientKeyPem) = E2eTestPki.Issue(
            E2eEnvironment.InstallCaPem, E2eEnvironment.InstallCaKeyPem,
            "e2e-strict", ["localhost", "127.0.0.1"], ip: null);
        var pair = X509Certificate2.CreateFromPem(clientPem, clientKeyPem);
        var clientCert = X509CertificateLoader.LoadPkcs12(pair.Export(X509ContentType.Pkcs12), null);
        return new HttpClient(new SocketsHttpHandler
        {
            SslOptions = new System.Net.Security.SslClientAuthenticationOptions
            {
                EnabledSslProtocols = SslProtocols.Tls12,
                ClientCertificates = [clientCert],
                RemoteCertificateValidationCallback = (_, cert, _, _) =>
                    cert is X509Certificate2 server
                    && Convert.ToHexString(
                        System.Security.Cryptography.SHA256.HashData(server.RawData))
                        .ToLowerInvariant() == trustedThumb,
            },
        })
        { Timeout = TimeSpan.FromSeconds(10) };
    }
}
