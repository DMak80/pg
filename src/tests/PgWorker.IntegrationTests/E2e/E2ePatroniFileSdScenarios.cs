using System.Text.Json;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Volumes;
using PgWorker.IntegrationTests.Docker;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// docker-E2E file_sd-канала (t15, spec §4 Ф4/Д4, §6.3): реальный кластер PgWorker
// в изолированном окружении + настоящий Prometheus + sd-generator в ЕДИНОЙ сети
// окружения класса (t15 ревизия 3): etcd/sd-generator/prometheus/ноды вместе,
// воркер с PgWorker__Docker__ScrapeNetwork = сеть окружения, скрейп
// patroni-nodes по <alias>:8008, host-форвардинга в контуре нет
// (кластер → таргеты up → канон-минимум patroni_* в TSDB → демонтаж →
// таргеты исчезли). Контур мониторинга сценария (volume, sd-generator,
// тестовый Prometheus) чистит сам — finally при любом исходе; docker-логи обоих
// контейнеров снимаются в артефакты ДО удаления (канон телеметрии e2e-launch).
// Сборка образа sdgenerator:e2e — лениво из этого сценария (EnsureSdImageAsync):
// чужие E2E-серии и кейс-маркер гейта её не платят.
public class E2ePatroniFileSdScenarios
{
    private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };

    // Окружение Fact'а (своя сеть/etcd); создаётся в начале сценария.
    private E2eEnvironment Fx = null!;

    private string Endpoint => Fx.EtcdEndpoint;

    private EtcdGateway G => Fx.Gateway;

    [Fact]
    public async Task E2ePatroniFileSd_ClusterTargetsUpSeriesDismantle()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        await using var fx = await E2eEnvironment.StartAsync("patroni-file-sd", ct: ct);
        Fx = fx;
        // Уникальное имя кластера на прогон: движковые контейнеры/тома (pgw-<C>-*)
        // опознаются teardown'ом окружения по своему тегу.
        var cluster = $"pfsd{Fx.ClusterTag}";

        IVolume? sdVol = null;
        IContainer? sdGen = null;
        IContainer? prom = null;
        try
        {
            // ---------- контур мониторинга (только этого сценария) ----------
            Console.WriteLine($"[PHASE] e2e[{Fx.Slug}]: сборка образа {E2eEnvironment.SdImage} (ленивая, первая серия платит)…");
            await E2eEnvironment.EnsureSdImageAsync(ct);

            sdVol = new VolumeBuilder().WithName($"pgw-sd-{Fx.ClusterTag}").Build();
            await sdVol.CreateAsync(ct);
            // Тестовый prometheus.yml: короткие интервалы (фаза не дорожает),
            // file_sd на volume + самоскрейп генератора по network-alias.
            var promConfigPath = Path.Combine(Fx.ArtifactsDir, "prometheus.yml");
            await File.WriteAllTextAsync(promConfigPath, """
                global:
                  scrape_interval: 3s
                  evaluation_interval: 3s
                scrape_configs:
                  - job_name: patroni-nodes
                    scheme: http
                    file_sd_configs:
                      - files: ["/etc/prometheus/sd/patroni-nodes.json"]
                        refresh_interval: 3s
                  - job_name: sd-generator
                    static_configs:
                      - targets: ["sd-generator:8080"]
                """, ct);

            // Контур мониторинга — единая сеть окружения класса (t15 ревизия 3):
            // sd-generator читает etcd по alias e2e-etcdN:2379, prometheus скрейпит
            // ноды по <alias>:8008 — host-форвардинга в контуре НЕТ вообще
            // (WithExtraHost отсутствует: регрессия ловится конструктивно —
            // резолва нет → скрейп умер → тест красный).
            var sdGenEnv = new Dictionary<string, string>
            {
                ["SdGenerator__OutputPath"] = "/sd/patroni-nodes.json",
                ["SdGenerator__RefreshIntervalSec"] = "2",
            };
            for (var i = 0; i < Fx.EtcdEndpoints.Count; i++)
                sdGenEnv[$"SdGenerator__Etcd__Endpoints__{i}"] = $"http://e2e-etcd{i + 1}:2379";

            // sd-generator: alias обязателен — static-таргет «sd-generator:8080»
            // резолвится по имени/alias, а случайное имя контейнеру даёт testcontainers.
            sdGen = new ContainerBuilder(E2eEnvironment.SdImage)
                .WithName($"pgw-sdgen-{Fx.ClusterTag}")
                .WithNetwork(Fx.Net)
                .WithNetworkAliases("sd-generator")
                .WithVolumeMount($"pgw-sd-{Fx.ClusterTag}", "/sd")
                .WithEnvironment(sdGenEnv)
                .Build();

            prom = new ContainerBuilder("prom/prometheus:v3.14.0")
                .WithName($"pgw-prom-{Fx.ClusterTag}")
                .WithNetwork(Fx.Net)
                .WithVolumeMount($"pgw-sd-{Fx.ClusterTag}", "/etc/prometheus/sd", AccessMode.ReadOnly)
                .WithBindMount(promConfigPath, "/etc/prometheus/prometheus.yml")
                .WithPortBinding(9090, assignRandomHostPort: true)
                .Build();

            // ---------- старт контура мониторинга ----------
            Console.WriteLine($"[PHASE] e2e[{Fx.Slug}]: старт контейнеров контура (sd-generator + prometheus)…");
            await sdGen.StartAsync(ct);
            await prom.StartAsync(ct);
            var promPort = prom.GetMappedPublicPort(9090);
            using var promHttp = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };

            // ---------- кластер ----------
            Console.WriteLine($"[PHASE] e2e[{Fx.Slug}]: сид кластера {cluster} + провижининг (≤360 c)…");
            await SeedClusterAsync(cluster);
            // ScrapeNetwork = имя сети окружения класса (t15 ревизия 3, spec §3.5):
            // движок Ensure-attach'ит ноды к сети окружения поверх pgw-net-<C>,
            // portalloc несёт alias/net, генератор строит <alias>:8008.
            await using var p1 = await Fx.StartHostAsync("s1", ct: ct, extraEnv: new Dictionary<string, string>
            {
                ["PgWorker__Docker__ScrapeNetwork"] = Fx.NetName,
            });

            var provisioned = await E2eFixture.WaitForAsync(
                () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(360), ct);
            provisioned.Should().BeTrue("provisioning кластера должен дойти до Active (dsn/RUNNING/без status)");

            // ---------- сетевая идентичность (Д1-контур в E2E) ----------
            // portalloc: все 4 записи с alias вида pgw-<cluster>-… и net = сети
            // окружения; membership каждой ноды в сети — docker inspect
            // (spec §6.4 «inspect подтверждает membership», обязательный ассерт).
            Console.WriteLine($"[PHASE] e2e[{Fx.Slug}]: сетевая идентичность portalloc + membership нод…");
            var aliases = await PatroniAliasesAsync(cluster);
            foreach (var alias in aliases)
            {
                var inspect = await Fx.RunDockerAsync(
                    ["inspect", alias, "--format", "{{json .NetworkSettings.Networks}}"], ct);
                inspect.Should().Contain($"\"{Fx.NetName}\"",
                    $"нода {alias} подключена к сети окружения {Fx.NetName} (Ensure-attach движка)");
            }

            // ---------- таргеты up ----------
            Console.WriteLine($"[PHASE] e2e[{Fx.Slug}]: ожидание таргетов patroni-nodes up…");
            var targetsUp = await E2eFixture.WaitForAsync(
                () => PatroniTargetsUpAsync(promHttp, promPort, aliases),
                TimeSpan.FromSeconds(120), ct);
            targetsUp.Should().BeTrue("таргеты patroni-nodes обязаны появиться и быть up");

            // Канон ревизии 3: ни один scrapeUrl (обе джобы: patroni-nodes и
            // sd-generator) не содержит host.docker.internal — скрейп целиком
            // сетевой, host-форвардинга в контуре нет.
            var allUrls = await AllScrapeUrlsAsync(promHttp, promPort);
            allUrls.Should().NotBeEmpty("activeTargets прометея непусты");
            allUrls.Should().NotContain(u => u.Contains("host.docker.internal", StringComparison.Ordinal),
                "ни один таргет не на host-форвардинге (канон t15 ревизии 3)");

            // ---------- канон-минимум серий (M3-факт) ----------
            Console.WriteLine($"[PHASE] e2e[{Fx.Slug}]: фиксация фактического словаря patroni_* + самоскрейп генератора…");
            var factSeries = await E2eFixture.WaitForAsync(
                async () => await PatroniSeriesFactAsync(promHttp, promPort) is not null,
                TimeSpan.FromSeconds(60), ct);
            factSeries.Should().BeTrue(
                "при up-скрейпе таргетов серии patroni_* обязаны появиться в TSDB (факт для arch/18 §2.5)");
            var fact = await PatroniSeriesFactAsync(promHttp, promPort);
            fact!.Should().NotBeEmpty("нативные серии Patroni — словарь arch/18 §2.5 (M3-факт)");
            // M3-факт — в артефакты прогона: xUnit буферизует Console, файл виден
            // сразу и остаётся в дампе (фиксация словаря arch/18 §2.5 по факту).
            await File.WriteAllTextAsync(
                Path.Combine(Fx.ArtifactsDir, "patroni-series-fact.txt"),
                string.Join("\n", fact), ct);
            Console.WriteLine(
                $"[PHASE] e2e[{Fx.Slug}]: факт словаря patroni_*: {string.Join(", ", fact)}");

            // Канон-минимум arch/18 §2.5 (M3-факт прогона: словарь Patroni 4.x —
            // роли отдельными сериями, patroni_primary вместо master,
            // patroni_postgres_timeline и patroni_xlog_replayed_timestamp).
            foreach (var series in new[]
                     {
                         "patroni_primary", "patroni_replica", "patroni_sync_standby",
                         "patroni_postgres_timeline", "patroni_xlog_replayed_timestamp",
                         "patroni_version", "patroni_postgres_running",
                         "sd_generator_last_success_timestamp_seconds",
                     })
            {
                var present = await E2eFixture.WaitForAsync(
                    () => SeriesPresentAsync(promHttp, promPort, series),
                    TimeSpan.FromSeconds(60), ct);
                present.Should().BeTrue($"серия {series} обязана быть в TSDB (канон-минимум arch/18 §2.5 / §5.4)");
            }

            // ---------- демонтаж ----------
            Console.WriteLine($"[PHASE] e2e[{Fx.Slug}]: демонтаж кластера → таргеты patroni-nodes исчезают…");
            await SetToRemoveAsync(cluster);
            var gone = await E2eFixture.WaitForAsync(
                () => DismantledAsync(cluster, promHttp, promPort),
                TimeSpan.FromSeconds(240), ct);
            gone.Should().BeTrue("после демонтажа portalloc-ключ исчез, а таргеты patroni-nodes = 0");
        }
        catch
        {
            // docs/e2e-launch.md §3: упавший сценарий — окружение ОСТАНОВИТЬ, не удалить.
            Fx.MarkFailed();
            throw;
        }
        finally
        {
            // Teardown контура сценария при любом исходе; телеметрия прежде удалений.
            // Шаг 0 — ПОЛНЫЙ дамп окружения (docker logs+inspect ВСЕХ СВОИХ
            // контейнеров: etcd/ноды/prom/sdgen + host.log воркеров) ДО любого
            // удаления — «что произошло» отвечает по логам без перезапуска
            // (docs/e2e-launch.md); жёсткий kill процесса теста — единственный
            // случай, когда этот шаг не выполняется.
            Console.WriteLine($"[PHASE] e2e[{Fx.Slug}]: teardown — телеметрия, затем удаления…");
            await Fx.CollectDiagnosticsAsync("scenario-finally");

            foreach (var c in new[] { prom, sdGen })
            {
                if (c is null)
                    continue;
                try
                {
                    await File.WriteAllTextAsync(
                        Path.Combine(Fx.ArtifactsDir, $"container-{c.Name.TrimStart('/')}.log"),
                        await E2eFixture.RunDockerAsync(["logs", "--timestamps", c.Name], CancellationToken.None),
                        CancellationToken.None);
                }
                catch
                {
                    // логи — «лучшими усилиями», не роняют teardown
                }
            }

            foreach (var c in new[] { prom, sdGen })
                if (c is not null)
                    await c.DisposeAsync(); // stop + rm
            if (sdVol is not null)
                await sdVol.DisposeAsync();

            // Ассерт чистоты контура сценария: volume file_sd удалён.
            var leftVolumes = await E2eFixture.RunDockerAsync(
                ["volume", "ls", "-q", "--filter", $"name=pgw-sd-{Fx.ClusterTag}"], CancellationToken.None);
            leftVolumes.Should().BeEmpty("volume file_sd контура сценария удалён teardown'ом");
        }
    }

    // ===== Хелперы (копии E2eScenarios/E2eScaleScenarios, scoped на кластер) =====

    private async Task<Kv?> GetOrNullAsync(string key)
        => (await G.GetAsync(Endpoint, key, TestContext.Current.CancellationToken)).Value;

    private async Task<IReadOnlyList<Kv>> RangeAsync(string prefix)
        => (await G.RangeAsync(Endpoint, prefix, TestContext.Current.CancellationToken)).Value;

    // Сид кластера в стиле панели (02 §9.1): config NOT_INITIALIZED, 2 шарда ×
    // replicas=2, routing/status всех N.
    private async Task SeedClusterAsync(string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/config",
            $$"""{"buckets":6,"dbname":"{{cluster}}","created_unix":1755800000,"state":"NOT_INITIALIZED","bucket_admin_password":"{{E2eFixture.BucketAdminPassword}}"}""",
            null, ct);
        foreach (var shard in new[] { "shard1", "shard2" })
        {
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/replicas", "2", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}a/state", "NOT_INITIALIZED", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}b/state", "NOT_INITIALIZED", null, ct);
            await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_cpu", "2", null, ct);
            await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_mem", "2Gi", null, ct);
        }

        for (var i = 0; i < 6; i++)
        {
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/routing/bucket_{i}", $"shard{i % 2 + 1}", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/status/bucket_{i}",
                """{"state":"NOT_INITIALIZED"}""", null, ct);
        }
    }

    // Панель переводит кластер в TO_REMOVE (§4.2: перезапись config со state).
    private async Task SetToRemoveAsync(string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        var current = await G.GetAsync(Endpoint, $"/clusters/{cluster}/config", ct);
        current.Value.Should().NotBeNull();
        var doc = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(current.Value!.Value)!;
        doc["state"] = JsonSerializer.SerializeToElement("TO_REMOVE");
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/config",
            JsonSerializer.Serialize(doc), null, ct);
    }

    // Условие готовности provisioning: dsn у всех шардов, все ноды RUNNING,
    // status-ключей нет, config без state (Д1).
    private async Task<bool> ProvisionedAsync(string cluster)
    {
        var config = await GetOrNullAsync($"/clusters/{cluster}/config");
        if (config is null || JsonSerializer
                .Deserialize<Dictionary<string, JsonElement>>(config.Value)!.ContainsKey("state"))
            return false;
        foreach (var shard in new[] { "shard1", "shard2" })
        {
            if (await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/dsn") is null)
                return false;
            foreach (var node in new[] { $"{shard}a", $"{shard}b" })
                if ((await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/nodes/{node}/state"))?.Value != "RUNNING")
                    return false;
        }

        return (await RangeAsync($"/clusters/{cluster}/buckets/status/")).Count == 0;
    }

    // Контейнерный порт Patroni REST — константа контракта ноды; синхрон с
    // TargetMapping.PatroniRestPort (DTO генератора независим от Core).
    private const int PatroniRestPort = 8008;

    private sealed record PortallocEntry(string Host, int Pg, int Patroni, int Doorman,
        string? Alias = null, string? Net = null);

    // Сетевые alias живых нод из portalloc (t15 ревизия 3): записи канонического
    // провижининга при заданном ScrapeNetwork несут alias = pgw-<C>-<X>-<n> и
    // net = имя сети окружения класса — доказательство Д1-контура в E2E.
    private async Task<List<string>> PatroniAliasesAsync(string cluster)
    {
        var kv = await GetOrNullAsync($"/pgworker/portalloc/{cluster}");
        kv.Should().NotBeNull("portalloc кластера записан провижинингом");
        var dict = JsonSerializer.Deserialize<Dictionary<string, PortallocEntry>>(kv!.Value, Json) ?? [];
        var live = dict.Where(p => p.Value.Patroni > 0).ToList();
        live.Should().HaveCount(4, "portalloc кластера: 2 шарда × 2 реплики с живым Patroni");
        foreach (var (key, entry) in live)
        {
            entry.Alias.Should().NotBeNullOrEmpty(
                $"запись {key} несёт alias (ScrapeNetwork задан —decorate точек записи)");
            entry.Alias.Should().StartWith($"pgw-{cluster}-", "alias — полное docker-имя ноды");
            entry.Net.Should().Be(Fx.NetName, "net записи = имя сети окружения класса");
        }

        return live.Select(p => p.Value.Alias!).ToList();
    }

    // Таргеты patroni-nodes: пары (health, scrapeUrl) — материализуются ВНУТРИ
    // жизни JsonDocument (JsonElement из using-документа после Dispose бросает
    // ObjectDisposedException — факт прогона 1117a3db).
    private async Task<List<(string? Health, string? Url)>> PatroniTargetsAsync(HttpClient http, int promPort)
    {
        using var doc = JsonDocument.Parse(
            await http.GetStringAsync($"http://localhost:{promPort}/api/v1/targets"));
        return doc.RootElement.GetProperty("data").GetProperty("activeTargets").EnumerateArray()
            .Where(t => t.GetProperty("labels").TryGetProperty("job", out var job)
                        && job.GetString() == "patroni-nodes")
            .Select(t => (t.GetProperty("health").GetString(),
                          t.GetProperty("scrapeUrl").GetString()))
            .ToList();
    }

    // scrapeUrl ВСЕХ активных таргетов (обе джобы) — прямой канон-ассерт
    // ревизии 3: host-форвардинга нет ни в одной джобе.
    private async Task<List<string>> AllScrapeUrlsAsync(HttpClient http, int promPort)
    {
        using var doc = JsonDocument.Parse(
            await http.GetStringAsync($"http://localhost:{promPort}/api/v1/targets"));
        return doc.RootElement.GetProperty("data").GetProperty("activeTargets").EnumerateArray()
            .Select(t => t.GetProperty("scrapeUrl").GetString())
            .Cast<string>()
            .ToList();
    }

    // Все таргеты patroni-nodes up, scrapeUrl — сетевые http://<alias>:8008:
    // Uri.Host ∈ alias'ам portalloc, Uri.Port — контейнерный порт контракта
    // (host-публикации записи в таргете НЕ участвуют).
    private async Task<bool> PatroniTargetsUpAsync(HttpClient http, int promPort, List<string> aliases)
    {
        List<(string? Health, string? Url)> targets;
        try
        {
            targets = await PatroniTargetsAsync(http, promPort);
        }
        catch (Exception)
        {
            return false; // прометеус ещё поднимается
        }

        if (targets.Count < 4)
            return false;
        foreach (var (health, url) in targets)
        {
            if (health != "up")
                return false;
            if (url is null || !Uri.TryCreate(url, UriKind.Absolute, out var u))
                return false;
            if (!aliases.Contains(u.Host))
                return false;
            if (u.Port != PatroniRestPort)
                return false;
        }

        return true;
    }

    // Серия непуста в TSDB (мгновенный вектор /api/v1/query).
    private async Task<bool> SeriesPresentAsync(HttpClient http, int promPort, string series)
    {
        try
        {
            using var doc = JsonDocument.Parse(await http.GetStringAsync(
                $"http://localhost:{promPort}/api/v1/query?query={Uri.EscapeDataString(series)}"));
            return doc.RootElement.GetProperty("data").GetProperty("result").GetArrayLength() > 0;
        }
        catch (Exception)
        {
            return false; // транзиентный сбой чтения — попытка повторится поллом
        }
    }

    // Факт словаря arch/18 §2.5: фактические имена серий patroni_* этого
    // Patroni (M3 — словарь фиксируется прогоном, спека §6.5); null — TSDB
    // ещё пуста (полл повторит).
    private async Task<IReadOnlyList<string>?> PatroniSeriesFactAsync(HttpClient http, int promPort)
    {
        try
        {
            using var doc = JsonDocument.Parse(await http.GetStringAsync(
                $"http://localhost:{promPort}/api/v1/label/__name__/values"));
            var names = doc.RootElement.GetProperty("data").EnumerateArray()
                .Select(n => n.GetString())
                .Where(n => n is not null && n.StartsWith("patroni_", StringComparison.Ordinal))
                .Cast<string>()
                .ToList();
            return names.Count > 0 ? names : null;
        }
        catch (Exception)
        {
            return null; // транзиентный сбой чтения — попытка повторится поллом
        }
    }

    // Демонтаж завершён: portalloc-ключ исчез И таргеты patroni-nodes = 0.
    private async Task<bool> DismantledAsync(string cluster, HttpClient http, int promPort)
    {
        if (await GetOrNullAsync($"/pgworker/portalloc/{cluster}") is not null)
            return false;
        try
        {
            return (await PatroniTargetsAsync(http, promPort)).Count == 0;
        }
        catch (Exception)
        {
            return false;
        }
    }
}
