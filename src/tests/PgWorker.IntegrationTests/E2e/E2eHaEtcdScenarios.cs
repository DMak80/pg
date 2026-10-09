using System.Text.Json;
using FluentAssertions;
using PgWorker.IntegrationTests.Docker;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// HA-etcd контур E2E (t09, spec §8): отказ ОДНОГО узла 3-нодового контура
// посреди надзора — не-событие. Изоляция/телеметрия — docs/e2e-isolation.md,
// docs/e2e-launch.md (guid-имена, own-only teardown, MarkFailed — стоп без
// удаления). Проверки после отказа узла №1 идут через выживший endpoint
// (EtcdEndpoints[1]) — активный URL узла №1 мёртв по построению.
public class E2eHaEtcdScenarios
{
    private E2eEnvironment Fx = null!;

    private string Endpoint => Fx.EtcdEndpoints[1]; // выживший узел при отказе №1

    private EtcdGateway G => Fx.Gateway;

    [Fact]
    public async Task HaEtcd_KillNodeMidAddShard_SupervisionAndMasterLeaseSurvive()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        await using var fx = await E2eEnvironment.StartAsync("ha-etcd", haEtcd: true, ct: ct);
        Fx = fx;
        var cluster = $"haetcd{Fx.ClusterTag}";
        try
        {
            // Arrange: 3-узловой контур (кворум собран фикстурой), воркер на
            // списке endpoints, кластер запровиженен, master-ключ жив.
            await SeedClusterAsync(cluster);
            await using var h1 = await Fx.StartHostAsync("ha1", ct: ct);
            var provisioned = await E2ePhase.WaitAsync(Fx, "haetcd-provisioning",
                () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(360), ct);
            provisioned.Should().BeTrue("provisioning обязан дойти до Active на 3-узловом контуре");
            Fx.EtcdEndpoints.Should().HaveCount(3);
            (await Fx.EtcdctlAsync("member", "list")).Split('\n', StringSplitOptions.RemoveEmptyEntries)
                .Should().HaveCount(3, "member list = 3 члена до отказа");

            // Act: останавливаем узел №1 (кворум 2/3 живёт) и СРАЗУ добавляем шард —
            // provisioning идёт через уцелевшие endpoints (failover воркера).
            await Fx.StopEtcdNodeAsync(0, ct);
            await SeedAddDeclarationAsync(cluster, "shard3", ct);

            // Assert (интервал отказа): healthz жив, lease-ключи API не гаснут,
            // master-ключ пережил отказ (непрерывно жив > 2×TTL 5 c — пишет
            // через второй endpoint, задача 4). По-пробная телеметрия (t29 §3.5):
            // каждая проба видна в журнале; пробы остаются жёсткими — НЕ ретрай.
            for (var probe = 0; probe < 5; probe++)
            {
                var healthz = await Fx.HealthzOkAsync(h1);
                var apiKeys = (await RangeAsync("/pgworker/api/")).Count;
                var master = await GetOrNullAsync($"/clusters/{cluster}/shards/shard1/master");
                Console.WriteLine(
                    $"[PROBE] ha-etcd {probe + 1}/5: healthz={healthz}, api={apiKeys}, " +
                    $"master={(master is { Value.Length: > 0 } ? $"len={master.Value.Length}" : (master is null ? "absent" : "empty"))}");
                healthz.Should().BeTrue($"healthz воркера жив на всём интервале отказа (проба {probe + 1}/5)");
                apiKeys.Should().BeGreaterThan(0, "keepalive-ключ API публикуется (надзор жив)");
                master.Should().NotBeNull($"master-ключ жив (проба {probe + 1}/5); фактически: отсутствует");
                master!.Value.Should().NotBeNullOrEmpty(
                    $"master-ключ жив (проба {probe + 1}/5); фактически: len={master.Value.Length}");
                await Task.Delay(TimeSpan.FromSeconds(2), ct);
            }

            var added = await E2ePhase.WaitAsync(Fx, "haetcd-shard3-registered",
                () => ShardRegisteredAsync(cluster, "shard3"), TimeSpan.FromSeconds(360), ct,
                // Прогресс: state-ключи нод shard3 + work-ключ (обрезан до ~120
                // симв., чтобы тик читался одной строкой).
                progress: async () =>
                {
                    var a = (await GetOrNullAsync($"/clusters/{cluster}/shards/shard3/nodes/shard3a/state"))?.Value ?? "-";
                    var b = (await GetOrNullAsync($"/clusters/{cluster}/shards/shard3/nodes/shard3b/state"))?.Value ?? "-";
                    var dsn = await GetOrNullAsync($"/clusters/{cluster}/shards/shard3/dsn") is null ? "нет" : "есть";
                    var work = (await GetOrNullAsync($"/pgworker/work/{cluster}"))?.Value ?? "нет";
                    return $"shard3: {a}/{b}, dsn={dsn}, work={Trunc(work)}";
                });
            added.Should().BeTrue($"add-shard доведён через уцелевшие endpoints; work={await WorkDumpAsync(cluster, ct)}");

            // Возврат узла: member list снова 3, health 3/3 (узел догоняет кластер).
            await Fx.StartEtcdNodeAsync(0, ct);
            var back = await E2ePhase.WaitAsync(Fx, "haetcd-node-back", async () =>
            {
                var members = (await Fx.EtcdctlAsync("member", "list"))
                    .Split('\n', StringSplitOptions.RemoveEmptyEntries);
                return members.Length == 3 && members.All(m => m.Contains("started"));
            }, TimeSpan.FromSeconds(60), ct);
            back.Should().BeTrue("узел №1 обязан вернуться в кворум (data-dir пережил stop)");
        }
        catch (Exception)
        {
            // Телеметрия (docs/e2e-launch.md): упавший сценарий помечается —
            // teardown ОСТАНАВЛИВАЕТ контейнеры, не удаляя (разбор по логам).
            Fx.MarkFailed();
            throw;
        }
    }

    // ===== Хелперы (копии приёмов E2eScaleScenarios — приватные там) =====

    // Обрезка длинного work-JSON до ~120 симв., чтобы прогресс-тик читался.
    private static string Trunc(string value)
        => value.Length <= 120 ? value : value[..120] + "…";

    private async Task<Shared.Etcd.Client.Kv?> GetOrNullAsync(string key)
        => (await G.GetAsync(Endpoint, key, TestContext.Current.CancellationToken)).Value;

    private async Task<IReadOnlyList<Shared.Etcd.Client.Kv>> RangeAsync(string prefix)
        => (await G.RangeAsync(Endpoint, prefix, TestContext.Current.CancellationToken)).Value;

    private async Task<string> WorkDumpAsync(string cluster, CancellationToken ct)
        => (await GetOrNullAsync($"/pgworker/work/{cluster}"))?.Value ?? "нет";

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

    private async Task<bool> ShardRegisteredAsync(string cluster, string shard)
    {
        if (await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/dsn") is null)
            return false;
        foreach (var node in new[] { $"{shard}a", $"{shard}b" })
            if ((await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/nodes/{node}/state"))?.Value != "RUNNING")
                return false;

        return true;
    }

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

    private async Task SeedAddDeclarationAsync(string cluster, string shard, CancellationToken ct)
    {
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/replicas", "2", null, ct);
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}a/state", "NOT_INITIALIZED", null, ct);
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}b/state", "NOT_INITIALIZED", null, ct);
        await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_cpu", "2", null, ct);
        await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_mem", "2Gi", null, ct);
        await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_disk", "10Gi", null, ct);
    }
}
