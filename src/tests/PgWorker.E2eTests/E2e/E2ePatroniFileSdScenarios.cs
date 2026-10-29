using System.Text.Json;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Volumes;
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
public sealed class E2ePatroniFileSdScenarios(E2ePatroniFileSdScenariosFx Ctx) : IClassFixture<E2ePatroniFileSdScenariosFx>
{
    private E2eEnvironment Fx => Ctx.Fx;

    private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };


    private string Endpoint => Fx.EtcdEndpoint;

    private EtcdGateway G => Fx.Gateway;

    [Fact]
    public async Task E2ePatroniFileSd_ClusterTargetsUpSeriesDismantle()
    {
        var ct = TestContext.Current.CancellationToken;
        // Уникальное имя кластера на прогон: движковые контейнеры/тома (pgw-<C>-*)
        // опознаются teardown'ом окружения по своему тегу.
        var cluster = $"pfsd{Fx.ClusterTag}";

        // Имена объектов контура мониторинга (own-only teardown по тегу прогона);
        // binds — хост-пути (демон резолвит их на хосте).
        var sdVolName = $"pgw-sd-{Fx.ClusterTag}";
        var sdGenName = $"pgw-sdgen-{Fx.ClusterTag}";
        var promName = $"pgw-prom-{Fx.ClusterTag}";
        var promUrl = $"http://{promName}:9090"; // внутрисетевой, без публикаций
        // Bind-пути резолвит демон хоста: артефакты окружения — ArtifactsDir.
        var hostArtifactsDir = Fx.ArtifactsDir;
        try
        {
            // ---------- контур мониторинга (только этого сценария) ----------
            Console.WriteLine($"[PHASE] e2e[{Fx.Slug}]: building image {E2eEnvironment.SdImage} (lazy, the first series pays)…");
            await E2eEnvironment.EnsureSdImageAsync(ct);

            await Fx.RunDockerAsync(["volume", "create", sdVolName], ct);
            // Тестовый prometheus.yml: короткие интервалы (фаза не дорожает),
            // file_sd на volume + самоскрейп генератора по network-alias.
            var promConfigPath = Path.Combine(Fx.ArtifactsDir, "prometheus.yml");
            await File.WriteAllTextAsync(promConfigPath, """
                global:
                  scrape_interval: 3s
                  evaluation_interval: 3s
                scrape_configs:
                  - job_name: patroni-nodes
                    scheme: https
                    tls_config:
                      ca_file: /etc/prometheus/ca.pem
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
            // резолвится по имени/alias в сети контура.
            var sdGenArgs = new List<string>
            {
                "run", "-d", "--name", sdGenName,
                "--network", Fx.NetName, "--network-alias", "sd-generator",
                "-v", $"{sdVolName}:/sd",
            };
            sdGenArgs.AddRange(sdGenEnv.Select(p => new[] { "-e", $"{p.Key}={p.Value}" }).SelectMany(x => x));
            sdGenArgs.Add(E2eEnvironment.SdImage);

            var promArgs = new List<string>
            {
                "run", "-d", "--name", promName,
                "--network", Fx.NetName,
                "-v", $"{sdVolName}:/etc/prometheus/sd:ro",
                // Bind — ХОСТОВЫЕ пути факта (раннер: hostArtifacts ↔ /artifacts,
                // файлы факта — в подкаталоге <runId>; bind несуществующего
                // корневого пути docker подменяет каталогом — «not a directory»).
                // CA контура для https-скрейпа patroni-nodes (t22: rest-ca.pem
                // фаза 1 пишет в каталог артефактов при старте окружения).
                "-v", $"{Path.Combine(hostArtifactsDir, "prometheus.yml")}:/etc/prometheus/prometheus.yml:ro",
                "-v", $"{Path.Combine(hostArtifactsDir, "rest-ca.pem")}:/etc/prometheus/ca.pem:ro",
                "prom/prometheus:v3.14.0",
            };

            // ---------- старт контура мониторинга ----------
            Console.WriteLine($"[PHASE] e2e[{Fx.Slug}]: starting contour containers (sd-generator + prometheus)…");
            await Fx.RunDockerAsync([.. sdGenArgs], ct);
            await Fx.RunDockerAsync([.. promArgs], ct);
            using var promHttp = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };

            // ---------- кластер ----------
            Console.WriteLine($"[PHASE] e2e[{Fx.Slug}]: seeding cluster {cluster} + provisioning (≤360 s)…");
            await SeedClusterAsync(cluster);
            // ScrapeNetwork = имя сети окружения класса (t15 ревизия 3, spec §3.5):
            // движок Ensure-attach'ит ноды к сети окружения поверх pgw-net-<C>,
            // portalloc несёт alias/net, генератор строит <alias>:8008.
            await using var p1 = await Fx.StartHostAsync("s1", ct: ct, extraEnv: new Dictionary<string, string>
            {
                ["PgWorker__Docker__ScrapeNetwork"] = Fx.NetName,
            });

            var provisioned = await E2eFixture.WaitForAsync(
                E2eEnvironment.ProvisionStuckFailFast(Fx, GetOrNullAsync, cluster, () => ProvisionedAsync(cluster)), TimeSpan.FromSeconds(360), ct);
            provisioned.Should().BeTrue("provisioning of the cluster must reach Active (dsn/RUNNING/no status)");

            // ---------- сетевая идентичность (Д1-контур в E2E) ----------
            // portalloc: все 4 записи с alias вида pgw-<cluster>-… и net = сети
            // окружения; membership каждой ноды в сети — docker inspect
            // (spec §6.4 «inspect подтверждает membership», обязательный ассерт).
            Console.WriteLine($"[PHASE] e2e[{Fx.Slug}]: network identity portalloc + node membership…");
            var aliases = await PatroniAliasesAsync(cluster);
            foreach (var alias in aliases)
            {
                var inspect = await Fx.RunDockerAsync(
                    ["inspect", alias, "--format", "{{json .NetworkSettings.Networks}}"], ct);
                inspect.Should().Contain($"\"{Fx.NetName}\"",
                    $"node {alias} is attached to the environment network {Fx.NetName} (engine Ensure-attach)");
            }

            // ---------- таргеты up ----------
            Console.WriteLine($"[PHASE] e2e[{Fx.Slug}]: waiting for the patroni-nodes up targets…");
            var targetsUp = await E2eFixture.WaitForAsync(
                () => PatroniTargetsUpAsync(promHttp, promUrl, aliases),
                TimeSpan.FromSeconds(120), ct);
            targetsUp.Should().BeTrue("the patroni-nodes targets must appear and be up");

            // Канон ревизии 3: ни один scrapeUrl (обе джобы: patroni-nodes и
            // sd-generator) не содержит host.docker.internal — скрейп целиком
            // сетевой, host-форвардинга в контуре нет.
            var allUrls = await AllScrapeUrlsAsync(promHttp, promUrl);
            allUrls.Should().NotBeEmpty("the prometheus activeTargets are not empty");
            allUrls.Should().NotContain(u => u.Contains("host.docker.internal", StringComparison.Ordinal),
                "no target is on host forwarding (t15 revision 3 canon)");

            // ---------- канон-минимум серий (M3-факт) ----------
            Console.WriteLine($"[PHASE] e2e[{Fx.Slug}]: fixing the actual patroni_* dictionary + generator self-scrape…");
            var factSeries = await E2eFixture.WaitForAsync(
                async () => await PatroniSeriesFactAsync(promHttp, promUrl) is not null,
                TimeSpan.FromSeconds(60), ct);
            factSeries.Should().BeTrue(
                "with an up-scrape of the series targets, patroni_* must appear in the TSDB (the fact for arch/18 §2.5)");
            var fact = await PatroniSeriesFactAsync(promHttp, promUrl);
            fact!.Should().NotBeEmpty("native Patroni series — the arch/18 §2.5 dictionary (M3 fact)");
            // M3-факт — в артефакты прогона: xUnit буферизует Console, файл виден
            // сразу и остаётся в дампе (фиксация словаря arch/18 §2.5 по факту).
            await File.WriteAllTextAsync(
                Path.Combine(Fx.ArtifactsDir, "patroni-series-fact.txt"),
                string.Join("\n", fact), ct);
            Console.WriteLine(
                $"[PHASE] e2e[{Fx.Slug}]: the patroni_* dictionary fact: {string.Join(", ", fact)}");

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
                    () => SeriesPresentAsync(promHttp, promUrl, series),
                    TimeSpan.FromSeconds(60), ct);
                present.Should().BeTrue($"the series {series} must be in the TSDB (arch/18 §2.5 / §5.4 canon minimum)");
            }

            // ---------- демонтаж ----------
            Console.WriteLine($"[PHASE] e2e[{Fx.Slug}]: cluster dismantling → the patroni-nodes targets disappear…");
            await SetToRemoveAsync(cluster);
            var gone = await E2eFixture.WaitForAsync(
                () => DismantledAsync(cluster, promHttp, promUrl),
                TimeSpan.FromSeconds(240), ct);
            gone.Should().BeTrue("after the dismantling the portalloc key is gone and the patroni-nodes targets = 0");
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
            Console.WriteLine($"[PHASE] e2e[{Fx.Slug}]: teardown — telemetry first, then removals…");
            await Fx.CollectDiagnosticsAsync("scenario-finally");

            foreach (var c in new[] { promName, sdGenName })
            {
                try
                {
                    await File.WriteAllTextAsync(
                        Path.Combine(Fx.ArtifactsDir, $"container-{c}.log"),
                        await E2eFixture.RunDockerLogsAsync(["--timestamps", c], CancellationToken.None),
                        CancellationToken.None);
                }
                catch
                {
                    // логи — «лучшими усилиями», не роняют teardown
                }
            }

            // Контейнеры контура и volume — по ИМЕНАМ (переменные «успешного
            // подъёма» не присвоены при раннем падении старта, а созданные
            // контейнеры держат volume); rm по отсутствующему — не ошибка.
            foreach (var c in new[] { promName, sdGenName })
            {
                try
                {
                    await Fx.RunDockerAsync(["rm", "-f", c], CancellationToken.None); // stop + rm
                }
                catch (ApplicationException e) when (e.Message.Contains("No such container", StringComparison.Ordinal))
                {
                    // контур не дошёл до создания — уже чисто
                }
            }

            try
            {
                await Fx.RunDockerAsync(["volume", "rm", "-f", sdVolName], CancellationToken.None);
            }
            catch (ApplicationException e) when (e.Message.Contains("no such volume", StringComparison.Ordinal))
            {
                // volume не создавался — уже чисто
            }

            // Ассерт чистоты контура сценария: volume file_sd удалён.
            var leftVolumes = await E2eFixture.RunDockerAsync(
                ["volume", "ls", "-q", "--filter", $"name=pgw-sd-{Fx.ClusterTag}"], CancellationToken.None);
            leftVolumes.Should().BeEmpty("the file_sd volume of the scenario contour is removed by the teardown");
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
        kv.Should().NotBeNull("the cluster portalloc is written by provisioning");
        var dict = JsonSerializer.Deserialize<Dictionary<string, PortallocEntry>>(kv!.Value, Json) ?? [];
        var live = dict.Where(p => p.Value.Patroni > 0).ToList();
        live.Should().HaveCount(4, "the cluster portalloc: 2 shards × 2 replicas with a live Patroni");
        foreach (var (key, entry) in live)
        {
            entry.Alias.Should().NotBeNullOrEmpty(
                $"the record {key} carries the alias (ScrapeNetwork is set — decorate of the write points)");
            entry.Alias.Should().StartWith($"pgw-{cluster}-", "the alias is the full docker name of the node");
            entry.Net.Should().Be(Fx.NetName, "the record net = the name of the class environment network");
        }

        return live.Select(p => p.Value.Alias!).ToList();
    }

    // Таргеты patroni-nodes: пары (health, scrapeUrl) — материализуются ВНУТРИ
    // жизни JsonDocument (JsonElement из using-документа после Dispose бросает
    // ObjectDisposedException — факт прогона 1117a3db).
    private async Task<List<(string? Health, string? Url)>> PatroniTargetsAsync(HttpClient http, string promUrl)
    {
        using var doc = JsonDocument.Parse(
            await http.GetStringAsync($"{promUrl}/api/v1/targets"));
        return doc.RootElement.GetProperty("data").GetProperty("activeTargets").EnumerateArray()
            .Where(t => t.GetProperty("labels").TryGetProperty("job", out var job)
                        && job.GetString() == "patroni-nodes")
            .Select(t => (t.GetProperty("health").GetString(),
                          t.GetProperty("scrapeUrl").GetString()))
            .ToList();
    }

    // scrapeUrl ВСЕХ активных таргетов (обе джобы) — прямой канон-ассерт
    // ревизии 3: host-форвардинга нет ни в одной джобе.
    private async Task<List<string>> AllScrapeUrlsAsync(HttpClient http, string promUrl)
    {
        using var doc = JsonDocument.Parse(
            await http.GetStringAsync($"{promUrl}/api/v1/targets"));
        return doc.RootElement.GetProperty("data").GetProperty("activeTargets").EnumerateArray()
            .Select(t => t.GetProperty("scrapeUrl").GetString())
            .Cast<string>()
            .ToList();
    }

    // Все таргеты patroni-nodes up, scrapeUrl — сетевые https://<alias>:8008
    // (tls_config ca_file контура; alias = SAN серта ноды): Uri.Host ∈ alias'ам
    // portalloc, Uri.Port — контейнерный порт контракта (host-публикации записи
    // в таргете НЕ участвуют).
    private async Task<bool> PatroniTargetsUpAsync(HttpClient http, string promUrl, List<string> aliases)
    {
        List<(string? Health, string? Url)> targets;
        try
        {
            targets = await PatroniTargetsAsync(http, promUrl);
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
    private async Task<bool> SeriesPresentAsync(HttpClient http, string promUrl, string series)
    {
        try
        {
            using var doc = JsonDocument.Parse(await http.GetStringAsync(
                $"{promUrl}/api/v1/query?query={Uri.EscapeDataString(series)}"));
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
    private async Task<IReadOnlyList<string>?> PatroniSeriesFactAsync(HttpClient http, string promUrl)
    {
        try
        {
            using var doc = JsonDocument.Parse(await http.GetStringAsync(
                $"{promUrl}/api/v1/label/__name__/values"));
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
    private async Task<bool> DismantledAsync(string cluster, HttpClient http, string promUrl)
    {
        if (await GetOrNullAsync($"/pgworker/portalloc/{cluster}") is not null)
            return false;
        try
        {
            return (await PatroniTargetsAsync(http, promUrl)).Count == 0;
        }
        catch (Exception)
        {
            return false;
        }
    }
}

/// <summary>Компаньон-фикстура окружения класса (полный контур на класс:
/// сеть/etcd/CA/окно портов; docs/e2e-isolation.md §1).</summary>
public sealed class E2ePatroniFileSdScenariosFx : E2eContourFixture
{
    protected override string Slug => "patroni-file-sd";
}
