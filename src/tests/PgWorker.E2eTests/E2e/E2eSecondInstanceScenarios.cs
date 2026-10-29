using System.Diagnostics;
using System.Text.Json;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// Docker-E2E второго инстанса (t07 spec §4.5): инстансы PgWorker — КОНТЕЙНЕРЫ
// образа pgworker:e2e-<tag> (деплой-слой), docker kill первого посреди A3
// add-shard — выживший контейнер доносит шард (takeover по lease-клэймам,
// БЕЗ правок кода воркера). Каноны docs/e2e-isolation.md / docs/e2e-launch.md:
// окружение E2eEnvironment (свой etcd/сеть на прогон), guid-имена всех
// объектов, полный teardown при любом исходе + ассерт чистоты; телеметрия в
// /tmp/pgw-e2e-artifacts-<guid>/ (MarkFailed — стоп без удаления).
public sealed class E2eSecondInstanceScenarios(E2eSecondInstanceScenariosFx Ctx) : IClassFixture<E2eSecondInstanceScenariosFx>
{
    private E2eEnvironment Fx => Ctx.Fx;


    private string Endpoint => Fx.EtcdEndpoint;

    private EtcdGateway G => Fx.Gateway;

    [Fact]
    public async Task SecondInstance_ContainerKillMidAdd_SurvivorFinishesNoDuplicates()
    {
        var ct = TestContext.Current.CancellationToken;

        // Идентификаторы прогона: имена контейнеров содержат тег кластера —
        // teardown окружения опознаёт их как СВОИ (OwnName) и удаляет.
        var tag = Fx.ClusterTag;
        var cluster = $"si2w{tag}";
        // Образ воркера — ОБЩИЙ pgworker:e2e (фаза 1 собрала из актуального
        // publish; сборка образа в раннере невозможна — нет SDK/исходников).
        var image = E2eEnvironment.WorkerImage;
        var w1 = $"pgw-ew1-{tag}";
        var w2 = $"pgw-ew2-{tag}";

        // PKI сценария (PECULIARITY: статический пакет фикстуры приватен —
        // свой CA/серты снимают зависимость; воркеры слушают mTLS на
        // внутрисетевых алиасах, тест ходит только по etcd-ключам).
        var ca = E2eTestPki.GenerateCa("si2");
        var (serverCertW1, serverKeyW1) = E2eTestPki.Issue(
            ca.CaPem, ca.CaKeyPem, "pgworker", [w1], ip: null);
        var (serverCertW2, serverKeyW2) = E2eTestPki.Issue(
            ca.CaPem, ca.CaKeyPem, "pgworker", [w2], ip: null);

        try
        {
            // ---------- Arrange: два инстанса-контейнера ----------
            // Инстансы w1/w2 — КОНТЕЙНЕРЫ в сети контура (Вариант A): API —
            // внутрисетевой :8443 по алиасу (публикаций на хост нет).
            await RunWorkerContainerAsync(w1, image, serverCertW1, serverKeyW1, ca, ct);
            await RunWorkerContainerAsync(w2, image, serverCertW2, serverKeyW2, ca, ct);

            // Готовность ОБОИХ: по 2 lease-ключа api/instances (start-бюджет
            // <=100 c; ключ жив = процесс поднялся, etcd-keepalive тикает).
            var bothUp = await E2ePhase.WaitAsync(Fx, "si2-instances-up", async () =>
                (await RangeAsync("/pgworker/api/")).Count == 2
                && (await RangeAsync("/pgworker/instances/")).Count == 2,
                TimeSpan.FromSeconds(100), ct,
                progress: async () =>
                {
                    var api = (await RangeAsync("/pgworker/api/")).Count;
                    var instances = (await RangeAsync("/pgworker/instances/")).Count;
                    return $"api={api}, instances={instances}";
                });
            bothUp.Should().BeTrue("both container instances must publish the discovery keys within 100 s");

            // ---------- Arrange: живой кластер + add-декларация shard3 ----------
            await SeedClusterAsync(cluster);
            var provisioned = await E2ePhase.WaitAsync(Fx, "si2-provisioning",
                E2eEnvironment.ProvisionStuckFailFast(Fx, GetOrNullAsync, cluster, () => ProvisionedAsync(cluster)), TimeSpan.FromSeconds(360), ct);
            provisioned.Should().BeTrue("provisioning of the cluster must reach Active before add starts");

            await SeedAddDeclarationAsync(cluster, "shard3", ct);
            var a3Started = await E2ePhase.WaitAsync(Fx, "si2-a3-started",
                () => DockerHasAsync($"pgw-{cluster}-shard3-"), TimeSpan.FromSeconds(120), ct);
            a3Started.Should().BeTrue("the first instance must start A3 (the shard3 container appeared)");

            // ---------- Act: docker kill ПЕРВОГО контейнера воркера ----------
            // --restart no: не воскреснет сам — доносит ТОЛЬКО выживший.
            await Fx.RunDockerAsync(["kill", w1], ct);

            // ---------- Assert: выживший донёс, дублей нет, клэйм у него ----------
            var finished = await E2ePhase.WaitAsync(Fx, "si2-takeover-done",
                () => ShardRegisteredAsync(cluster, "shard3"), TimeSpan.FromSeconds(360), ct,
                // Прогресс state-ключей shard3 (по образцу HaEtcd, t29 §3.5).
                progress: async () =>
                {
                    var a = (await GetOrNullAsync($"/clusters/{cluster}/shards/shard3/nodes/shard3a/state"))?.Value ?? "-";
                    var b = (await GetOrNullAsync($"/clusters/{cluster}/shards/shard3/nodes/shard3b/state"))?.Value ?? "-";
                    var dsn = await GetOrNullAsync($"/clusters/{cluster}/shards/shard3/dsn") is null ? "absent" : "present";
                    var work = (await GetOrNullAsync($"/pgworker/work/{cluster}"))?.Value ?? "absent";
                    return $"shard3: {a}/{b}, dsn={dsn}, work={Trunc(work)}";
                });
            finished.Should().BeTrue(
                $"the surviving container must carry shard3 after the takeover; work={await WorkDumpAsync(cluster, ct)}");

            var containers = await ListContainerNamesAsync($"pgw-{cluster}-shard3-", all: true);
            containers.Should().HaveCount(2, "exactly 2 containers of the new shard (no duplicates after the takeover)");

            var apiKeys = await RangeAsync("/pgworker/api/");
            apiKeys.Should().HaveCount(1, "after killing the first exactly one discovery key is alive (the lease expired)");
            (await RangeAsync("/pgworker/instances/")).Should().HaveCount(1, "the instance key of the first expired together with its lease");
            var survivor = apiKeys[0].Key.Split('/')[^1];

            var claim = (await GetOrNullAsync($"/pgworker/claims/{cluster}"))?.Value;
            claim.Should().NotBeNull("the claim of the live cluster must exist after the takeover");
            using (var doc = JsonDocument.Parse(claim!))
                doc.RootElement.GetProperty("instance").GetString().Should().Be(survivor,
                    "the cluster claim is held by the surviving instance — supervision migrated second (work journal: the phases are finished by the instance id from the claim)");

            // ---------- Teardown-подготовка (успех): свои контейнеры ----------
            // Образ общий (pgworker:e2e) — НЕ удаляется.
            await Fx.RunDockerAsync(["rm", "-f", w1, w2], ct);
        }
        catch
        {
            // Телеметрия e2e-launch: упавший сценарий помечаем — teardown
            // ОСТАНОВИТ контейнеры (не удалит: разбор по живым объектам).
            Fx.MarkFailed();
            throw;
        }
    }

    // Запуск инстанса-контейнера PgWorker: ВНУТРИСЕТВОЙ (сеть контура, алиас =
    // имя контейнера), docker.sock (воркер управляет нодами в контуре),
    // mTLS PEM-значениями (без volume), уникальный AdvertiseUrl по алиасу;
    // env-набор — как у воркеров фикстуры StartHostAsync (быстрые тики).
    private async Task RunWorkerContainerAsync(
        string name, string image,
        string serverCert, string serverKey, (string CaPem, string CaKeyPem) ca,
        CancellationToken ct)
    {
        var env = new Dictionary<string, string>
        {
            // Секреты установки (Д7) — как у воркеров фикстуры.
            ["PGW_PG_SUPERUSER_PASSWORD"] = E2eFixture.SuPassword,
            ["PGW_PG_STANDBY_PASSWORD"] = E2eFixture.StandbyPassword,
            ["PGW_BUCKET_ADMIN_PASSWORD"] = E2eFixture.BucketAdminPassword,
            ["PGW_BUCKET_MOVER_PASSWORD"] = E2eFixture.MoverPassword,

            // etcd окружения: воркер в сети контура — внутрисетевой алиас;
            // Advertised — тот же (Patroni-ноды в той же сети).
            ["PgWorker__Etcd__Endpoints__0"] = Endpoint,
            ["PgWorker__Etcd__AdvertisedEndpoints__0"] = Endpoint,
            ["PgWorker__Docker__Mode"] = "Plain",
            ["PgWorker__Docker__ContourNetwork"] = Fx.NetName,
            ["PgWorker__Docker__Hosts__0__Name"] = "host.docker.internal",
            ["PgWorker__Docker__Hosts__0__Endpoint"] = "unix:///var/run/docker.sock",
            // PortRange — окно контура (публикации троек нод — канон продукта);
            // аллокаторы двух воркеров согласованы общим etcd (arch/14 §2.4).
            ["PgWorker__Docker__PortRange__From"] = Fx.RemainingPortRange.From.ToString(),
            ["PgWorker__Docker__PortRange__To"] = Fx.RemainingPortRange.To.ToString(),
            ["PgWorker__Docker__Images__Node"] = E2eEnvironment.NodeImage,
            ["PgWorker__Docker__EnableDoorman"] = "false",

            // Ускоренные циклы/пороги e2e (ассерты ждут секунды, не минуты).
            ["PgWorker__Loops__ScanIntervalSec"] = "1",
            ["PgWorker__Loops__KeepaliveSec"] = "1",
            ["PgWorker__Loops__ErrorDelayMs"] = "500",
            ["PgWorker__Loops__SnapshotIntervalMin"] = "360",
            ["PgWorker__Thresholds__NodeDeadSec"] = "6",
            ["PgWorker__Thresholds__ShardDeadSec"] = "5",
            ["PgWorker__Thresholds__PatroniBootSec"] = "300",
            ["PgWorker__Parallelism__MaxClusters"] = "2",
            ["PgWorker__Snapshots__Dir"] = "/snapshots",
            ["PgWorker__Snapshots__RetentionFiles"] = "10",

            // mTLS API (t03): PEM-дуализм env освобождает от volume; уникальный
            // AdvertiseUrl per инстанс (t07: одинаковые URL гасят оба ключа).
            ["PGW_API_TLS_CERT"] = serverCert,
            ["PGW_API_TLS_KEY"] = serverKey,
            ["PGW_API_TLS_CLIENT_CA"] = ca.CaPem,
            ["PgWorker__Api__Tls__AllowInsecureHttp"] = "false",
            ["PgWorker__Api__AdvertiseUrl"] = $"https://{name}:8443",

            // REST-TLS нод (t22): per-install CA — fail-fast старта без пары
            // CA/CA_KEY (как у воркеров фикстуры; http-режима нет).
            ["PGW_REST_TLS_CA"] = Fx.CaPem,
            ["PGW_REST_TLS_CA_KEY"] = Fx.CaKeyPem,

            ["ASPNETCORE_URLS"] = "https://+:8443",
            ["DOTNET_ENVIRONMENT"] = "Production",
        };

        var args = new List<string>
        {
            "run", "-d", "--name", name, "--restart", "no",
            "--network", Fx.NetName,
            "--network-alias", name,
            "-v", "/var/run/docker.sock:/var/run/docker.sock",
        };
        args.AddRange(env.Select(p => new[] { "-e", $"{p.Key}={p.Value}" }).SelectMany(x => x));
        args.Add(image);
        await Fx.RunDockerAsync([.. args], ct);
    }

    // ===== Хелперы (приёмы E2eScaleScenarios, scoped на кластер) =====

    // Обрезка длинного work-JSON до ~120 симв., чтобы прогресс-тик читался.
    private static string Trunc(string value)
        => value.Length <= 120 ? value : value[..120] + "…";

    private async Task<Shared.Etcd.Client.Kv?> GetOrNullAsync(string key)
        => (await G.GetAsync(Endpoint, key, TestContext.Current.CancellationToken)).Value;

    private async Task<IReadOnlyList<Shared.Etcd.Client.Kv>> RangeAsync(string prefix)
        => (await G.RangeAsync(Endpoint, prefix, TestContext.Current.CancellationToken)).Value;

    private async Task<string> WorkDumpAsync(string cluster, CancellationToken ct)
        => (await GetOrNullAsync($"/pgworker/work/{cluster}"))?.Value ?? "absent";

    // Кластер Active: config без state, dsn всех нод, ноды RUNNING, статусов нет.
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

    // Шард поднят и зарегистрирован: dsn записан, все ноды RUNNING.
    private async Task<bool> ShardRegisteredAsync(string cluster, string shard)
    {
        if (await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/dsn") is null)
            return false;
        foreach (var node in new[] { $"{shard}a", $"{shard}b" })
            if ((await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/nodes/{node}/state"))?.Value != "RUNNING")
                return false;

        return true;
    }

    // Сид кластера в стиле панели (копия E2eScaleScenarios.SeedClusterAsync).
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

    // Add-декларация в стиле панели: replicas + nodes + request_*, БЕЗ dsn.
    private async Task SeedAddDeclarationAsync(string cluster, string shard, CancellationToken ct)
    {
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/replicas", "2", null, ct);
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}a/state", "NOT_INITIALIZED", null, ct);
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}b/state", "NOT_INITIALIZED", null, ct);
        await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_cpu", "2", null, ct);
        await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_mem", "2Gi", null, ct);
        await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_disk", "10Gi", null, ct);
    }

    private async Task<bool> DockerHasAsync(string prefix)
        => (await ListContainerNamesAsync(prefix)).Count > 0;

    private async Task<List<string>> ListContainerNamesAsync(string prefix, bool all = false)
    {
        var ct = TestContext.Current.CancellationToken;
        var args = new List<string> { "ps", "--format", "{{.Names}}" };
        if (all)
            args.Add("-a");
        args.AddRange(["--filter", $"name={prefix}"]);
        var output = await Fx.RunDockerAsync([.. args], ct);
        return output.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(n => n.StartsWith(prefix, StringComparison.Ordinal))
            .ToList();
    }
}

/// <summary>Компаньон-фикстура окружения класса (полный контур на класс:
/// сеть/etcd/CA/окно портов; docs/e2e-isolation.md §1).</summary>
public sealed class E2eSecondInstanceScenariosFx : E2eContourFixture
{
    protected override string Slug => "second-instance";
}
