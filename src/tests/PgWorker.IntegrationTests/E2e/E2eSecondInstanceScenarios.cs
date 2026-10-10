using System.Diagnostics;
using System.Text.Json;
using PgWorker.IntegrationTests.Docker;
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
public class E2eSecondInstanceScenarios
{
    private E2eEnvironment Fx = null!;

    private string Endpoint => Fx.EtcdEndpoint;

    private EtcdGateway G => Fx.Gateway;

    [Fact]
    public async Task SecondInstance_ContainerKillMidAdd_SurvivorFinishesNoDuplicates()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        await using var fx = await E2eEnvironment.StartAsync("second-instance", ct: ct);
        Fx = fx;
        var root = E2eFixture.FindRoot(AppContext.BaseDirectory);

        // Идентификаторы прогона: имена контейнеров/тег образа содержат тег
        // кластера — teardown окружения опознаёт их как СВОИ (OwnName) и
        // удаляет; телеметрия снимает их логи до удаления.
        var tag = Fx.ClusterTag;
        var cluster = $"si2w{tag}";
        var image = $"pgworker:e2e-{tag}";
        var w1 = $"pgw-ew1-{tag}";
        var w2 = $"pgw-ew2-{tag}";

        // PKI сценария (PECULIARITY: статический пакет фикстуры приватен —
        // свой CA/серты снимают зависимость; клиент доверяет только своей CA).
        var ca = E2eTestPki.GenerateCa("si2");
        var (serverCert, serverKey) = E2eTestPki.Issue(
            ca.CaPem, ca.CaKeyPem, "pgworker", ["localhost", "127.0.0.1", "host.docker.internal"], ip: null);
        var (clientPem, clientKeyPem) = E2eTestPki.Issue(
            ca.CaPem, ca.CaKeyPem, "si2-healthcheck", ["si2-healthcheck"], ip: null);

        try
        {
            // ---------- Arrange: образ + два инстанса-контейнера ----------
            // Образ воркера: сборка в сценарии, тег с идентификатором прогона
            // (кэш слоёв делает повторные прогоны быстрыми; свежий код всегда).
            // [PHASE]-метка ДО старта: тихая фаза без метки старта нарушает канон
            // статических фаз (spec §3.6); полный вывод при kill по бюджету — в
            // ArtifactsDir (RunProcessAsync logFile, t29 §4.5); бюджет 120 c —
            // прежний дефолт docker-CLI, передаётся явно (число не меняется).
            Console.Error.WriteLine($"[PHASE] build {image}: старт (контекст корня репо, бюджет 120 c)…");
            var buildSw = Stopwatch.StartNew();
            await E2eFixture.RunProcessAsync("docker",
                ["build", "-q", "-f", $"{root}/docker/PgWorker.Dockerfile", "-t", image, root],
                ct, timeout: TimeSpan.FromMinutes(2),
                logFile: Path.Combine(Fx.ArtifactsDir, $"process-build-{tag}.log"));
            Console.Error.WriteLine($"[PHASE] build {image}: {buildSw.Elapsed.TotalSeconds:F0} c");

            // Порты w1/w2 — последовательные слоты ОКНА контура (t24, spec §1.5):
            // FreePort() вне окна гонялся бы за теми же базами, что и окна
            // параллельных контуров — межконтурная гонка bind'ов.
            var p1 = Fx.ReserveWindowPort();
            var p2 = Fx.ReserveWindowPort();
            await RunWorkerContainerAsync(w1, image, p1, serverCert, serverKey, ca, ct);
            await RunWorkerContainerAsync(w2, image, p2, serverCert, serverKey, ca, ct);

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
            bothUp.Should().BeTrue("оба контейнерных инстанса обязаны опубликовать дискавери-ключи за 100 c");

            // ---------- Arrange: живой кластер + add-декларация shard3 ----------
            await SeedClusterAsync(cluster);
            var provisioned = await E2ePhase.WaitAsync(Fx, "si2-provisioning",
                () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(360), ct);
            provisioned.Should().BeTrue("provisioning кластера должен дойти до Active до старта add");

            await SeedAddDeclarationAsync(cluster, "shard3", ct);
            var a3Started = await E2ePhase.WaitAsync(Fx, "si2-a3-started",
                () => DockerHasAsync($"pgw-{cluster}-shard3-"), TimeSpan.FromSeconds(120), ct);
            a3Started.Should().BeTrue("первый инстанс должен начать A3 (появился контейнер shard3)");

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
                    var dsn = await GetOrNullAsync($"/clusters/{cluster}/shards/shard3/dsn") is null ? "нет" : "есть";
                    var work = (await GetOrNullAsync($"/pgworker/work/{cluster}"))?.Value ?? "нет";
                    return $"shard3: {a}/{b}, dsn={dsn}, work={Trunc(work)}";
                });
            finished.Should().BeTrue(
                $"выживший контейнер должен донести shard3 после takeover; work={await WorkDumpAsync(cluster, ct)}");

            var containers = await ListContainerNamesAsync($"pgw-{cluster}-shard3-", all: true);
            containers.Should().HaveCount(2, "контейнеров нового шарда ровно 2 (нет дублей после takeover)");

            var apiKeys = await RangeAsync("/pgworker/api/");
            apiKeys.Should().HaveCount(1, "после kill первого жив ровно один дискавери-ключ (lease погас)");
            (await RangeAsync("/pgworker/instances/")).Should().HaveCount(1, "instance-ключ первого погас вместе с lease");
            var survivor = apiKeys[0].Key.Split('/')[^1];

            var claim = (await GetOrNullAsync($"/pgworker/claims/{cluster}"))?.Value;
            claim.Should().NotBeNull("клэйм живого кластера обязан существовать после takeover");
            using (var doc = JsonDocument.Parse(claim!))
                doc.RootElement.GetProperty("instance").GetString().Should().Be(survivor,
                    "клэйм кластера держит выживший инстанс — надзор мигрировал вторым (work-журнал: фазы доигрывает instance-ид из клэйма)");

            // ---------- Teardown-подготовка (успех): свои контейнеры и образ ----------
            await Fx.RunDockerAsync(["rm", "-f", w1, w2], ct);
            await Fx.RunDockerAsync(["image", "rm", "-f", image], ct);
        }
        catch
        {
            // Телеметрия e2e-launch: упавший сценарий помечаем — teardown
            // ОСТАНОВИТ контейнеры (не удалит: разбор по живым объектам).
            fx.MarkFailed();
            throw;
        }
    }

    // Запуск инстанса-контейнера PgWorker: динамический хост-порт, docker.sock
    // (воркер управляет нодами), extra_hosts host-gateway (etcd/ноды через
    // host.docker.internal), mTLS PEM-значениями (без volume), уникальный
    // AdvertiseUrl; env-набор — как у хост-инстансов фикстуры (быстрые тики).
    private async Task RunWorkerContainerAsync(
        string name, string image, int hostPort,
        string serverCert, string serverKey, (string CaPem, string CaKeyPem) ca,
        CancellationToken ct)
    {
        var etcdHost = Endpoint.Replace("localhost:", "host.docker.internal:", StringComparison.Ordinal);
        var env = new Dictionary<string, string>
        {
            // Секреты установки (Д7) — как у хост-инстансов фикстуры.
            ["PGW_PG_SUPERUSER_PASSWORD"] = E2eFixture.SuPassword,
            ["PGW_PG_STANDBY_PASSWORD"] = E2eFixture.StandbyPassword,
            ["PGW_BUCKET_ADMIN_PASSWORD"] = E2eFixture.BucketAdminPassword,
            ["PGW_BUCKET_MOVER_PASSWORD"] = E2eFixture.MoverPassword,

            // etcd окружения: из контейнера — публикацией на хост; Advertised —
            // для Patroni-нод (те ходят из своих контейнеров).
            ["PgWorker__Etcd__Endpoints__0"] = etcdHost,
            ["PgWorker__Etcd__AdvertisedEndpoints__0"] = etcdHost,
            ["PgWorker__Docker__Mode"] = "Plain",
            ["PgWorker__Docker__Hosts__0__Name"] = "host.docker.internal",
            ["PgWorker__Docker__Hosts__0__Endpoint"] = "unix:///var/run/docker.sock",
            // PortRange — остаток ОКНА контура (t24, spec §1.5): тот же контракт,
            // что у хост-воркеров StartHostAsync — окно за вычетом инфраструктуры
            // контура (etcd/MinIO/API-порты/w1/w2); литеральные диапазоны
            // (одинаковые всем контурам) запрещены — межконтурная гонка bind'ов.
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
            ["PgWorker__Api__AdvertiseUrl"] = $"https://host.docker.internal:{hostPort}",

            // REST-TLS нод (t22): per-install CA — fail-fast старта без пары
            // CA/CA_KEY (как у хостовых инстансов фикстуры; http-режима нет).
            ["PGW_REST_TLS_CA"] = E2eEnvironment.InstallCaPem,
            ["PGW_REST_TLS_CA_KEY"] = E2eEnvironment.InstallCaKeyPem,

            ["ASPNETCORE_URLS"] = "https://+:8080",
            ["DOTNET_ENVIRONMENT"] = "Production",
        };

        var args = new List<string>
        {
            "run", "-d", "--name", name, "--restart", "no",
            "-p", $"{hostPort}:8080",
            "-v", "/var/run/docker.sock:/var/run/docker.sock",
            "--add-host", "local:host-gateway",
            "--add-host", "host.docker.internal:host-gateway",
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
        => (await GetOrNullAsync($"/pgworker/work/{cluster}"))?.Value ?? "нет";

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
