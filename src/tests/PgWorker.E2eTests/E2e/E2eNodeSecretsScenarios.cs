using System.Text.Json;
using System.Text.RegularExpressions;
using Npgsql;
using FluentAssertions;
using PgWorker.Core.Model;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E t05: нода без секретов в docker-env; доставка через хендшейк по
// статусной машине агента PgNode (spec §6.2/§6.2a/§6.2b/§6.5/§9.5/§9.6).
// Каркас — по образцу E2eScaleScenarios: guid-кластер, Fx.StartHostAsync,
// SeedClusterAsync, бюджеты через E2eFixture.WaitForAsync, полный teardown
// фикстуры, динамические порты, свои etcd-префиксы (docs/e2e-isolation.md);
// телеметрия — docs/e2e-launch.md.
public sealed class E2eNodeSecretsScenarios(E2eNodeSecretsFx Ctx) : IClassFixture<E2eNodeSecretsFx>
{
    // Окружение контура (сеть/etcd/CA/порты) — из компаньон-фикстуры.
    private E2eEnvironment Fx => Ctx.Fx;

    private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };

    private string Endpoint => Fx.EtcdEndpoint;

    private EtcdGateway G => Fx.Gateway;

    private async Task<Kv?> GetOrNullAsync(string key)
        => (await G.GetAsync(Endpoint, key, TestContext.Current.CancellationToken)).Value;

    // ===== 1) Негативный docker inspect (spec §6.5/§9.5) =====

    [Fact]
    public async Task NodeSecrets_DockerInspectHasNoSecrets()
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMinutes(6));
        var ct = cts.Token;
        var cluster = $"nsec{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        await using var p1 = await Fx.StartHostAsync("s1", ct: ct);
        var provisioned = await E2eFixture.WaitForAsync(
            () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(90), ct);
        provisioned.Should().BeTrue("provisioning must reach Active via the secret handshake");

        // Запрещённые env-ключи (spec §3.1) и фактические секретные значения
        // прогона — подстрочный ассерт docker inspect всех нод кластера.
        var restPassword = (await GetOrNullAsync($"/clusters/{cluster}/rest_password"))?.Value;
        var containers = (await E2eFixture.RunDockerAsync(
            ["ps", "-aq", "--filter", $"name=pgw-{cluster}-"], ct))
            .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        containers.Should().NotBeEmpty("the cluster nodes are up");
        foreach (var container in containers)
        {
            var envJson = await E2eFixture.RunDockerAsync(
                ["inspect", container, "--format", "{{json .Config.Env}}"], ct);
            string[] forbiddenKeys =
            [
                "PGUSER_SUPERUSER", "PGPASSWORD_SUPERUSER", "PGUSER_STANDBY",
                "PGPASSWORD_STANDBY", "PGW_BUCKET_ADMIN_USER", "PGW_BUCKET_ADMIN_PASSWORD",
                "PGW_BUCKET_MOVER_PASSWORD", "SSL_RESTAPI_CERTIFICATE",
                "SSL_RESTAPI_PRIVATE_KEY", "PGW_REST_PASSWORD_HASH",
                "SPILO_CONFIGURATION", "DOORMAN_CONFIG",
            ];
            foreach (var key in forbiddenKeys)
                envJson.Should().NotContain($"{key}=", "node env carries no secrets (spec §3.1)");
            envJson.Should().NotContain(E2eFixture.SuPassword);
            envJson.Should().NotContain(E2eFixture.StandbyPassword);
            envJson.Should().NotContain(E2eFixture.BucketAdminPassword);
            envJson.Should().NotContain(E2eFixture.MoverPassword);
            if (restPassword is { Length: > 0 } rest)
                envJson.Should().NotContain(rest, "rest_password lives only in the etcd bootstrap key");
            envJson.Should().Contain("DOORMAN_CONFIG_TEMPLATE=");
            envJson.Should().Contain("${PGW_SU_PASSWORD}");
            envJson.Should().Contain("PGW_AGENT_CLIENT_CA=");
        }
    }

    // ===== 2) Хендшейк → ready (spec §6.2a/§9.6) =====

    [Fact]
    public async Task NodeSecrets_Handshake_ReachesReady()
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMinutes(6));
        var ct = cts.Token;
        var cluster = $"nhsd{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        await using var p1 = await Fx.StartHostAsync("s1", ct: ct);
        var provisioned = await E2eFixture.WaitForAsync(
            () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(90), ct);
        provisioned.Should().BeTrue("provisioning must reach Active via the secret handshake");

        // Все статусные ключи нод: state=ready, applied_revision задан,
        // компоненты pg/doorman = ready (бюджет 120 c).
        var allReady = await E2eFixture.WaitForAsync(async () =>
        {
            var statuses = (await G.RangeAsync(Endpoint,
                $"/pgworker/nodes/{cluster}/", TestContext.Current.CancellationToken)).Value;
            var nodes = statuses.Where(kv => kv.Key.EndsWith("/status", StringComparison.Ordinal)).ToList();
            if (nodes.Count < 4)
                return false; // 2 шарда × 2 ноды
            return nodes.All(kv =>
            {
                try
                {
                    using var doc = JsonDocument.Parse(kv.Value);
                    var root = doc.RootElement;
                    return root.GetProperty("state").GetString() == "ready"
                        && root.TryGetProperty("applied_revision", out var rev)
                        && rev.ValueKind == JsonValueKind.Number
                        && root.GetProperty("components").GetProperty("pg").GetString() == "ready"
                        && root.GetProperty("components").GetProperty("doorman").GetString() == "ready";
                }
                catch (JsonException)
                {
                    return false;
                }
            });
        }, TimeSpan.FromSeconds(120), ct);
        allReady.Should().BeTrue("all nodes reach ready via the secret handshake");

        // SELECT через опубликованный :6432 (sslmode=require) — клиентский
        // вход пулера работает (проба заодно валидирует цепочку auth_query).
        var dsn0 = (await GetOrNullAsync($"/clusters/{cluster}/shards/shard1/dsn"))!.Value;
        var hosts = RegexHosts(dsn0);
        var appPassword = await Fx.GetAppPasswordAsync(cluster, ct);
        var throughDoorman = await E2eFixture.WaitForAsync(async () =>
        {
            foreach (var (host, doorman) in hosts)
            {
                try
                {
                    await using var con = new Npgsql.NpgsqlConnection(
                        $"Host={host};Port={doorman};Database={cluster};Username=app;" +
                        $"Password={appPassword};Timeout=5;SSL Mode=Require");
                    await con.OpenAsync(ct);
                    await using var cmd = new Npgsql.NpgsqlCommand("SELECT 1", con);
                    return await cmd.ExecuteScalarAsync(ct) is 1;
                }
                catch (Exception e) when (e is not OperationCanceledException)
                {
                    // ещё поднимается/рестарт порт-паблиш — следующая проба
                }
            }
            return false;
        }, TimeSpan.FromSeconds(60), ct);
        throughDoorman.Should().BeTrue("SELECT through the published doorman :6432 works");
    }

    // ===== 3) Рестарт ноды — конвергенция тиком (spec §6.2) =====

    [Fact]
    public async Task NodeSecrets_NodeRestart_ConvergedBySupervisorTick()
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMinutes(8));
        var ct = cts.Token;
        var cluster = $"nrc{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        await using var p1 = await Fx.StartHostAsync("s1", ct: ct);
        var provisioned = await E2eFixture.WaitForAsync(
            () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(90), ct);
        provisioned.Should().BeTrue("provisioning must reach Active via the secret handshake");

        // SELECT через doorman жив (нода отработала до рестарта).
        var appPassword = await Fx.GetAppPasswordAsync(cluster, ct);
        var hosts = RegexHosts((await GetOrNullAsync($"/clusters/{cluster}/shards/shard1/dsn"))!.Value);
        var (container, host, doorman) = await NodeEndpointAsync(cluster, "shard1", ct);

        // Act: docker restart ноды → lease-ключ статуса исчез; сервисы стоят
        // (supervisord autostart только pgnode) — SELECT-проба падает.
        await E2eFixture.RunDockerAsync(["restart", container], ct);
        var leaseGone = await E2eFixture.WaitForAsync(async () =>
        {
            var statuses = (await G.RangeAsync(Endpoint,
                $"/pgworker/nodes/{cluster}/shard1/", ct)).Value;
            return !statuses.Any(kv =>
                kv.Key.EndsWith("/status", StringComparison.Ordinal)
                && kv.Key.Contains($"/{container.Split('-')[^1]}/", StringComparison.Ordinal));
        }, TimeSpan.FromSeconds(30), ct);
        leaseGone.Should().BeTrue("the lease-key of the restarted node is gone (~TTL 15 c)");
        var probeDown = await E2eFixture.WaitForAsync(async () =>
        {
            try
            {
                await using var con = new Npgsql.NpgsqlConnection(
                    $"Host={host};Port={doorman};Database={cluster};Username=app;" +
                    $"Password={appPassword};Timeout=3;SSL Mode=Require");
                await con.OpenAsync(ct);
                return false; // ещё жив — ждём падения
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                return true; // сервисы стоят
            }
        }, TimeSpan.FromSeconds(30), ct);
        probeDown.Should().BeTrue("the services are down after the restart (autostart only pgnode)");

        // Assert: конвергенция тиком — статус снова ready, SELECT жив
        // (хендшейк заново; статус-ключ агента бэкапов жив, §6.7).
        var readyAgain = await E2eFixture.WaitForAsync(async () =>
        {
            var status = await GetOrNullAsync(
                $"/pgworker/nodes/{cluster}/shard1/{container.Split('-')[^1]}/status");
            if (status is null)
                return false;
            try
            {
                using var doc = JsonDocument.Parse(status.Value);
                return doc.RootElement.GetProperty("state").GetString() == "ready";
            }
            catch (JsonException)
            {
                return false;
            }
        }, TimeSpan.FromSeconds(90), ct);
        readyAgain.Should().BeTrue("the supervisor tick converged the node (handshake again)");
        var selectAlive = await E2eFixture.WaitForAsync(async () =>
        {
            try
            {
                await using var con = new Npgsql.NpgsqlConnection(
                    $"Host={host};Port={doorman};Database={cluster};Username=app;" +
                    $"Password={appPassword};Timeout=5;SSL Mode=Require");
                await con.OpenAsync(ct);
                await using var cmd = new Npgsql.NpgsqlCommand("SELECT 1", con);
                return await cmd.ExecuteScalarAsync(ct) is 1;
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                return false;
            }
        }, TimeSpan.FromSeconds(60), ct);
        selectAlive.Should().BeTrue("SELECT through doorman works after the convergence");
    }

    // ===== 4) start_failed → пересоздание с новым диском (spec §6.2b/§9.6) =====

    [Fact]
    public async Task NodeSecrets_StartFailed_RecreatesNodeWithNewDisk()
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMinutes(10));
        var ct = cts.Token;
        var cluster = $"nsf{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        await using var p1 = await Fx.StartHostAsync("s1", ct: ct);
        var provisioned = await E2eFixture.WaitForAsync(
            () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(90), ct);
        provisioned.Should().BeTrue("provisioning must reach Active via the secret handshake");

        var container = (await E2eFixture.RunDockerAsync(
            ["ps", "--format", "{{.Names}}", "--filter", $"name=pgw-{cluster}-shard1-shard1a"], ct)).Trim();
        container.Should().NotBeEmpty("the node shard1a is up");
        var idBefore = (await E2eFixture.RunDockerAsync(
            ["inspect", "-f", "{{.Id}}", container], ct)).Trim();
        var startedBefore = (await E2eFixture.RunDockerAsync(
            ["inspect", "-f", "{{.State.StartedAt}}", container], ct)).Trim();

        // Маркер на диск: после пересоздания его нет (volume новый).
        await E2eFixture.RunDockerAsync(
            ["exec", container, "touch", "/home/postgres/pgdata/MARKER"], ct);

        // Act (spec §6.2b): инжекция сбоя старта pg_doorman (rw-слой переживает
        // restart) + рестарт → хендшейк заново → старт pg_doorman падает ×3 →
        // start_failed → надзор пересоздаёт с новым диском.
        var mvOut = await E2eFixture.RunDockerAsync(
            ["exec", container, "mv", "/usr/local/bin/pg_doorman", "/usr/local/bin/pg_doorman.bak"], ct);
        mvOut.Should().NotContain("No such file", "the mv must rename the pooler binary");
        (await DockerFileExistsAsync(container, "/usr/local/bin/pg_doorman", ct))
            .Should().BeFalse("the pooler binary must be gone after the mv");
        // Рестарт pgnode-ПРОЦЕССА SIGKILL'ом (supervisorctl restart гасит
        // контейнер: killasgroup pgnode накрывает группу supervisord):
        // unexpected-kill → autorestart=unexpected перезапускает pgnode; tmpfs
        // цел (контейнер жив) — pgnode перечитает ключ и повторит фазы.
        var pgnodePid = (await E2eFixture.RunDockerAsync(
            ["exec", container, "pgrep", "-x", "pgnode"], ct))
            .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)[0];
        pgnodePid.Should().NotBeEmpty("the pgnode process is running");
        await E2eFixture.RunDockerAsync(["exec", container, "kill", "-9", pgnodePid], ct);

        // pgnode перезапущен supervisord'ом (autorestart=unexpected) и
        // перепубликовал not_initialized.
        var pgnodeUp = await E2eFixture.WaitForAsync(async () =>
            (await E2eFixture.RunDockerAsync(
                ["exec", container, "supervisorctl", "status", "pgnode"], ct))
            .Contains("RUNNING"), TimeSpan.FromSeconds(30), ct);
        pgnodeUp.Should().BeTrue("pgnode is restarted by supervisord (unexpected exit)");

        var startFailed = await E2eFixture.WaitForAsync(async () =>
        {
            var status = await GetOrNullAsync(
                $"/pgworker/nodes/{cluster}/shard1/shard1a/status");
            if (status is null)
                return false;
            try
            {
                using var doc = JsonDocument.Parse(status.Value);
                var root = doc.RootElement;
                return root.GetProperty("state").GetString() == "start_failed"
                    && root.TryGetProperty("components", out var comp)
                    && comp.GetProperty("doorman").GetString() == "failed"
                    && root.TryGetProperty("error", out var err)
                    && err.GetString() is { Length: > 0 };
            }
            catch (JsonException)
            {
                return false;
            }
        }, TimeSpan.FromSeconds(120), ct);
        startFailed.Should().BeTrue("the node reaches start_failed (doorman start fails ×3)");

        // Assert: пересоздание — container id/StartedAt изменились, статус
        // ready, SELECT жив, маркер исчез (volume новый).
        var recreated = await E2eFixture.WaitForAsync(async () =>
        {
            var id = (await E2eFixture.RunDockerAsync(
                ["inspect", "-f", "{{.Id}}", container], ct)).Trim();
            return id != idBefore;
        }, TimeSpan.FromSeconds(150), ct);
        recreated.Should().BeTrue("the supervisor recreated the node (new container id)");

        var ready = await E2eFixture.WaitForAsync(async () =>
        {
            var status = await GetOrNullAsync(
                $"/pgworker/nodes/{cluster}/shard1/shard1a/status");
            if (status is null)
                return false;
            try
            {
                using var doc = JsonDocument.Parse(status.Value);
                return doc.RootElement.GetProperty("state").GetString() == "ready";
            }
            catch (JsonException)
            {
                return false;
            }
        }, TimeSpan.FromSeconds(150), ct);
        ready.Should().BeTrue("the recreated node reaches ready via the handshake again");

        var startedAfter = (await E2eFixture.RunDockerAsync(
            ["inspect", "-f", "{{.State.StartedAt}}", container], ct)).Trim();
        startedAfter.Should().NotBe(startedBefore, "the container was started anew");

        (await DockerFileExistsAsync(container, "/home/postgres/pgdata/MARKER", ct))
            .Should().BeFalse("the marker is gone: the volume is new");
    }

    // ===== Хелперы (копии паттерна E2eScaleScenarios, scoped на кластер) =====

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

    private async Task<bool> ProvisionedAsync(string cluster)
    {
        var config = await GetOrNullAsync($"/clusters/{cluster}/config");
        if (config is null || JsonSerializer
                .Deserialize<Dictionary<string, JsonElement>>(config.Value, Json)!.ContainsKey("state"))
            return false;
        foreach (var shard in new[] { "shard1", "shard2" })
        {
            if (await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/dsn") is null)
                return false;
            foreach (var node in new[] { $"{shard}a", $"{shard}b" })
            {
                var state = await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/nodes/{node}/state");
                if (state is null || state.Value != "RUNNING")
                    return false;
            }
        }
        return true;
    }

    private sealed record NodeAddr(string Host, int Pg, int Patroni, int Doorman);

    private async Task<Dictionary<string, NodeAddr>> PortallocAsync(string cluster)
    {
        var kv = await GetOrNullAsync($"/pgworker/portalloc/{cluster}");
        if (kv is null)
            return [];
        return JsonSerializer.Deserialize<Dictionary<string, NodeAddr>>(kv.Value, Json) ?? [];
    }

    // Пары (host, doorman-порт) shard1 из dsn (публикация портов на хост).
    private static List<(string Host, int Doorman)> RegexHosts(string dsn)
    {
        var hosts = System.Text.RegularExpressions.Regex.Match(dsn, "host=([^ ]+)").Groups[1].Value.Split(',');
        var ports = System.Text.RegularExpressions.Regex.Match(dsn, "port=([^ ]+)").Groups[1].Value.Split(',');
        return hosts.Zip(ports, (h, p) => (h, int.TryParse(p, out var v) ? v : 0)).ToList();
    }

    // docker exec test → bool: ненулевой exit (RunProcessAsync бросает) = false.
    private static async Task<bool> DockerFileExistsAsync(string container, string path, CancellationToken ct)
    {
        try
        {
            await E2eFixture.RunDockerAsync(["exec", container, "test", "-f", path], ct);
            return true;
        }
        catch (ApplicationException)
        {
            return false;
        }
    }

    // Контейнер/публикация doorman ноды шарда (из portalloc: host + doorman-порт).
    private async Task<(string Container, string Host, int Doorman)> NodeEndpointAsync(
        string cluster, string shard, CancellationToken ct)
    {
        var addresses = await PortallocAsync(cluster);
        var entry = addresses.First(p => p.Key.StartsWith($"{shard}/", StringComparison.Ordinal));
        var node = entry.Key.Split('/')[1];
        return ($"pgw-{cluster}-{shard}-{node}", entry.Value.Host, entry.Value.Doorman);
    }
}

public sealed class E2eNodeSecretsFx : E2eContourFixture
{
    protected override string Slug => "node-secret";
}
