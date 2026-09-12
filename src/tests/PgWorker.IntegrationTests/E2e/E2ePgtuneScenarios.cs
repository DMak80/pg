using System.Text.Json;
using PgWorker.Etcd.Client;
using PgWorker.IntegrationTests.Docker;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E-сценарии PGTune (spec.md §4.6, AC §7 п.2/3/4) на живом стенде —
// изолированное окружение E2eEnvironment на каждый Fact (своя сеть/etcd,
// guid-имена, own-only teardown + ассерт чистоты — docs/e2e-isolation.md;
// динамические хост-порты; etcd-префикс сценария умирает вместе с etcd):
// 1) provision → SPILO_CONFIGURATION нод — merge(PGTune ∪ канон) от заявок
//    request_*, doorman-бюджет синхронизирован, env нод одного прохода идентичен;
// 2) пересчёт от актуальных заявок: смена request_mem → пересоздание ноды
//    надзором даёт env от НОВОЙ заявки; новых ключей etcd нет;
// 3) отсутствие request_mem/request_cpu → расчёт от DefaultTotalMemoryBytes,
//    параллельные параметры в YAML отсутствуют.
public class E2ePgtuneScenarios
{
    private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };

    // Окружение Fact'а (своя сеть/etcd); создаётся в начале каждого сценария.
    private E2eEnvironment Fx = null!;

    private string Endpoint => Fx.EtcdEndpoint;

    private EtcdGateway G => Fx.Gateway;

    [Fact]
    public async Task Pgtune_Provision_EnvCarriesMergedParamsAndSyncedDoorman()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        await using var fx = await E2eEnvironment.StartAsync("pgtune-provision", ct: ct);
        Fx = fx;
        // Уникальное имя кластера на прогон (own-only teardown по тегу прогона).
        var cluster = $"pshop{Fx.ClusterTag}";

        // Arrange: сид с заявками 4Gi/2cpu → расчёт PGTune: shared_buffers 1GB,
        // max_connections 60 (Connections=60 из опций по умолчанию).
        await SeedClusterAsync(cluster, requestMem: "4Gi", ct);
        // Doorman включаем ТОЛЬКО ради env-ассерта бюджета (образ e2e без
        // бинарника — обёртка supervisord тихо выходит, узел поднимается).
        await using var host = await Fx.StartHostAsync("s1",
            extraEnv: new Dictionary<string, string> { ["PgWorker__Docker__EnableDoorman"] = "true" }, ct: ct);

        // Act: provisioning до Active.
        var provisioned = await E2eFixture.WaitForAsync(
            () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(360), ct);
        provisioned.Should().BeTrue("provisioning кластера должен дойти до Active; " +
                                    $"work={await WorkDumpAsync(cluster, ct)}");

        // Assert: SPILO_CONFIGURATION каждой ноды — merge(PGTune ∪ канон):
        // PGTune-значения от заявки 4Gi, канон P3 поверх, exclude по умолчанию.
        foreach (var shard in new[] { "shard1", "shard2" })
        foreach (var node in new[] { $"{shard}a", $"{shard}b" })
        {
            var env = await ContainerEnvAsync($"pgw-{cluster}-{shard}-{node}", ct);
            var spilo = env["SPILO_CONFIGURATION"];
            spilo.Should().Contain("max_connections: \"60\"", $"pgw-{cluster}-{shard}-{node}");
            spilo.Should().Contain("shared_buffers: \"1GB\"", $"pgw-{cluster}-{shard}-{node}");
            spilo.Should().Contain("effective_cache_size: \"3GB\"", $"pgw-{cluster}-{shard}-{node}");
            // Канон PgWorker поверх PGTune (P3): wal_level/walsenders от PgWorker.
            spilo.Should().Contain("wal_level: logical", $"pgw-{cluster}-{shard}-{node}");
            spilo.Should().Contain("max_wal_senders: \"10\"", $"pgw-{cluster}-{shard}-{node}");
            // Exclude по умолчанию (опции воркера): параметр не пишется вовсе.
            spilo.Should().NotContain("io_method", $"pgw-{cluster}-{shard}-{node}");
            spilo.Should().NotContain("io_workers", $"pgw-{cluster}-{shard}-{node}");
        }

        // Assert: env нод одного прохода идентичен (SPILO_CONFIGURATION не несёт
        // per-node ключей PGW_NODE_HOST/PGW_NODE_NAME — они отдельно в env).
        var spilos = new List<string>();
        foreach (var shard in new[] { "shard1", "shard2" })
        foreach (var node in new[] { $"{shard}a", $"{shard}b" })
            spilos.Add((await ContainerEnvAsync($"pgw-{cluster}-{shard}-{node}", ct))["SPILO_CONFIGURATION"]);
        spilos.Distinct().Should().ContainSingle("все ноды одного прохода получают один расчёт");

        // Assert: doorman-бюджет синхронизирован от рассчитанного max_connections
        // (P15: 60 − 5 = 55, пол 10).
        var doorman = (await ContainerEnvAsync($"pgw-{cluster}-shard1-shard1a", ct))["DOORMAN_CONFIG"];
        doorman.Should().Contain("max_db_connections = 55");
        doorman.Should().Contain("default_pool_size = 55");
    }

    [Fact]
    public async Task Pgtune_Recreate_Node_RecomputedFromActualRequests()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        await using var fx = await E2eEnvironment.StartAsync("pgtune-recreate", ct: ct);
        Fx = fx;
        var cluster = $"rshop{Fx.ClusterTag}";
        var node = $"pgw-{cluster}-shard1-shard1b";

        // Arrange: живой кластер с заявкой 4Gi → env нод от 4Gi (shared_buffers 1GB).
        await SeedClusterAsync(cluster, requestMem: "4Gi", ct);
        await using var host = await Fx.StartHostAsync("s1", ct: ct);
        var provisioned = await E2eFixture.WaitForAsync(
            () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(360), ct);
        provisioned.Should().BeTrue("provisioning кластера должен дойти до Active; " +
                                    $"work={await WorkDumpAsync(cluster, ct)}");

        var envBefore = (await ContainerEnvAsync(node, ct))["SPILO_CONFIGURATION"];
        envBefore.Should().Contain("shared_buffers: \"1GB\"", "исходный расчёт — от заявки 4Gi");

        // Снимок имён ключей etcd ДО (граница контракта: воркер не пишет параметры).
        var keysBefore = await AllKeyNamesAsync(ct);

        // Act: перезаписать заявку request_mem → 8Gi ДО сноса контейнера (без гонки:
        // пересоздание надзором прочитает уже НОВУЮ заявку), затем снести контейнер
        // ноды — сверка декларации (arch/14 §5 C) пересоздаёт её.
        (await G.PutAsync(Endpoint, $"/service/{cluster}-shard1/request_mem", "8Gi", null, ct))
            .IsSuccess.Should().BeTrue("заявка request_mem должна перезаписаться");
        await Fx.RunDockerAsync(["rm", "-f", node], ct);

        var recreated = await E2eFixture.WaitForAsync(async () =>
        {
            try
            {
                // Контейнер существует И несёт env от новой заявки (старый env 1GB).
                var env = await ContainerEnvAsync(node, ct);
                return env.TryGetValue("SPILO_CONFIGURATION", out var spilo)
                       && spilo.Contains("shared_buffers: \"2GB\"", StringComparison.Ordinal);
            }
            catch (ApplicationException)
            {
                return false; // контейнера ещё нет — надзор пересоздаст следующим тиком
            }
        }, TimeSpan.FromSeconds(120), ct);
        recreated.Should().BeTrue($"пересозданная нода должна получить env от заявки 8Gi; " +
                                  $"work={await WorkDumpAsync(cluster, ct)}");

        // Assert: env отличается от прежнего (пересчёт от АКТУАЛЬНОЙ заявки).
        var envAfter = (await ContainerEnvAsync(node, ct))["SPILO_CONFIGURATION"];
        envAfter.Should().NotBe(envBefore, "env пересозданной ноды пересчитан от новой заявки");
        envAfter.Should().Contain("max_connections: \"60\"");
        envAfter.Should().Contain("wal_level: logical");

        // Assert: etcd-контракт неизменен — новых ключей вне известного HA-churn
        // не появилось: /service/<scope>/* — состояние Patroni; shards/<X>/master —
        // P11 lease-канал лидерства (мигрирует при пересоздании ноды). Оба канала
        // существовали до pgtune; pg-параметры воркер в etcd не пишет.
        var fresh = (await AllKeyNamesAsync(ct))
            .Except(keysBefore)
            .Where(k => !k.StartsWith("/service/", StringComparison.Ordinal))
            .Where(k => !k.EndsWith("/master", StringComparison.Ordinal))
            .ToList();
        fresh.Should().BeEmpty("параметры не фиксируются в etcd — новых ключей нет");
    }

    [Fact]
    public async Task Pgtune_NoRequests_EnvFromOptionDefaults()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        await using var fx = await E2eEnvironment.StartAsync("pgtune-defaults", ct: ct);
        Fx = fx;
        var cluster = $"dshop{Fx.ClusterTag}";

        // Arrange: сид БЕЗ request_mem/request_cpu (нечитаемая заявка → null).
        await SeedClusterAsync(cluster, requestMem: null, ct);
        await using var host = await Fx.StartHostAsync("s1", ct: ct);

        // Act: provisioning до Active.
        var provisioned = await E2eFixture.WaitForAsync(
            () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(360), ct);
        provisioned.Should().BeTrue("provisioning кластера должен дойти до Active; " +
                                    $"work={await WorkDumpAsync(cluster, ct)}");

        // Assert: env нод рассчитан от DefaultTotalMemoryBytes (8GiB): 2GB/6GB/60;
        // request_cpu нет → cpuNum не задан → параллельные параметры отсутствуют
        // вовсе (никаких суррогатных дефолтов).
        foreach (var shard in new[] { "shard1", "shard2" })
        foreach (var node in new[] { $"{shard}a", $"{shard}b" })
        {
            var spilo = (await ContainerEnvAsync($"pgw-{cluster}-{shard}-{node}", ct))["SPILO_CONFIGURATION"];
            spilo.Should().Contain("shared_buffers: \"2GB\"", $"pgw-{cluster}-{shard}-{node}");
            spilo.Should().Contain("effective_cache_size: \"6GB\"", $"pgw-{cluster}-{shard}-{node}");
            spilo.Should().Contain("max_connections: \"60\"", $"pgw-{cluster}-{shard}-{node}");
            spilo.Should().NotContain("max_worker_processes", $"pgw-{cluster}-{shard}-{node}");
            spilo.Should().NotContain("max_parallel_workers", $"pgw-{cluster}-{shard}-{node}");
            spilo.Should().NotContain("io_workers", $"pgw-{cluster}-{shard}-{node}");
        }
    }

    // ===== Хелперы (приёмы E2eScaleScenarios, scoped на кластер) =====

    // Сид кластера в стиле панели (02 §9.1): заявки request_* опциональны —
    // сценарий отсутствия заявок сеет без них.
    private async Task SeedClusterAsync(string cluster, string? requestMem, CancellationToken ct)
    {
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/config",
            $$"""{"buckets":6,"dbname":"{{cluster}}","created_unix":1755800000,"state":"NOT_INITIALIZED","bucket_admin_password":"{{E2eFixture.BucketAdminPassword}}"}""",
            null, ct);
        foreach (var shard in new[] { "shard1", "shard2" })
        {
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/replicas", "2", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}a/state", "NOT_INITIALIZED", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}b/state", "NOT_INITIALIZED", null, ct);
            if (requestMem is not null)
            {
                await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_cpu", "2", null, ct);
                await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_mem", requestMem, null, ct);
            }
        }

        for (var i = 0; i < 6; i++)
        {
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/routing/bucket_{i}", $"shard{i % 2 + 1}", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/status/bucket_{i}",
                """{"state":"NOT_INITIALIZED"}""", null, ct);
        }
    }

    // Provisioning доведён до Active: config без state, dsn/RUNNING у всех нод,
    // status-ключи сняты.
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

    private async Task<Kv?> GetOrNullAsync(string key)
        => (await G.GetAsync(Endpoint, key, TestContext.Current.CancellationToken)).Value;

    private async Task<IReadOnlyList<Kv>> RangeAsync(string prefix)
        => (await G.RangeAsync(Endpoint, prefix, TestContext.Current.CancellationToken)).Value;

    private async Task<string> WorkDumpAsync(string cluster, CancellationToken ct)
        => (await GetOrNullAsync($"/pgworker/work/{cluster}"))?.Value ?? "нет";

    // Все имена ключей etcd окружения (prefix "/" — весь ключевой namespace).
    private async Task<List<string>> AllKeyNamesAsync(CancellationToken ct)
        => (await G.RangeAsync(Endpoint, "/", ct)).Value.Select(kv => kv.Key).ToList();

    // Env контейнера ноды: docker inspect → Config.Env (KEY=VALUE).
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
}
