using System.Text.Json;
using FluentAssertions;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// HA-etcd контур E2E (spec §8): отказ ОДНОГО узла 3-нодового контура
// посреди надзора — не-событие. Изоляция/телеметрия — docs/e2e-isolation.md,
// docs/e2e-launch.md (guid-имена, own-only teardown, MarkFailed — стоп без
// удаления). Жертва — FOLЛОВЕР кворума (StopEtcdFollowerAsync): отказ лидера
// = окно выборов ~2–4 с, в котором etcd-put отклоняется и lease-ключи с
// TTL 5 с (master-ключ) гаснут — пробы «непрерывно жив» невыполнимы
// конструктивно; отказ folловера кворум-запись не трогает (arch/04 §8).
// Проверки во время отказа идут через host-endpoints с перебором (URL
// остановленного узла пропускается — кто жертва, решается на рантайме).
public sealed class E2eHaEtcdScenarios(E2eHaEtcdScenariosFx Ctx) : IClassFixture<E2eHaEtcdScenariosFx>
{
    private E2eEnvironment Fx => Ctx.Fx;


    private string Endpoint => Fx.EtcdEndpoints[1]; // посев ДО отказа — узел ещё жив

    private EtcdGateway G => Fx.Gateway;

    [Fact]
    public async Task HaEtcd_KillNodeMidAddShard_SupervisionAndMasterLeaseSurvive()
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        // Потолок 5 мин (согласован владельцем по фактическим замерам):
        // provisioning ~60 с + probes окна отказа ~10 с + add-shard посреди
        // etcd-хапапа ~100-160 с (bootstrap двух PG-нод ~60-90 с сам по себе)
        // + возврат узла/конвергенция ~60 с; неверное ожидание не может
        // длиться дольше.
        cts.CancelAfter(TimeSpan.FromMinutes(5));
        var ct = cts.Token;
        var cluster = $"haetcd{Fx.ClusterTag}";
        try
        {
            // Arrange: 3-узловой контур (кворум собран фикстурой), воркер на
            // списке endpoints, кластер запровиженен, master-ключ жив.
            await SeedClusterAsync(cluster);
            await using var h1 = await Fx.StartHostAsync("ha1", ct: ct);
            var provisioned = await E2ePhase.WaitAsync(Fx, "haetcd-provisioning",
                E2eEnvironment.ProvisionStuckFailFast(Fx, GetOrNullAsync, cluster, () => ProvisionedAsync(cluster)), TimeSpan.FromSeconds(90), ct);
            provisioned.Should().BeTrue("provisioning must reach Active on the 3-node contour");
            Fx.EtcdEndpoints.Should().HaveCount(3);
            (await Fx.EtcdctlAsync("member", "list")).Split('\n', StringSplitOptions.RemoveEmptyEntries)
                .Should().HaveCount(3, "member list = 3 members before the failure");

            // Act: останавливаем FOLЛОВЕРА кворума (не лидера; кворум-запись
            // сохраняется, arch/04) и СРАЗУ добавляем шард — provisioning идёт
            // через уцелевшие endpoints (failover воркера).
            var stopped = await Fx.StopEtcdFollowerAsync(ct);
            await SeedAddDeclarationAsync(cluster, "shard3", ct);

            // Assert (интервал отказа): healthz и lease-ключи API воркера —
            // жёстко на КАЖДОЙ пробе; master-ключ — на старте окна, затем
            // допуск транзиентного гашения (DCS-хаппап члена etcd может
            // демоутировать мастера — TTL 5 с гаснет и перезаказывается) —
            // к концу окна ключ жив снова. По-пробная телеметрия (§3.5).
            var masterAliveEver = false;
            var masterAliveAtWindowEnd = false;
            var apiKeysEver = false;
            for (var probe = 0; probe < 5; probe++)
            {
                var healthz = await Fx.HealthzOkAsync(h1);
                var apiKeys = (await RangeAsync("/pgworker/api/")).Count;
                var master = await GetOrNullAsync($"/clusters/{cluster}/shards/shard1/master");
                var masterAlive = master is { Value.Length: > 0 };
                Console.WriteLine(
                    $"[PROBE] ha-etcd {probe + 1}/5: healthz={healthz}, api={apiKeys}, " +
                    $"master={(masterAlive ? $"len={master!.Value.Length}" : (master is null ? "absent" : "empty"))}");
                healthz.Should().BeTrue($"the worker healthz is alive across the whole failure interval (probe {probe + 1}/5)");
                // api-ключи: мёртвый IP в сети — «чёрная дыра» (~2 с на SYN до
                // таймаута): failover шлюза платит штраф за мёртвый endpoint,
                // lease-ключи воркера могут кратковременно гаснуть — к концу
                // окна живы (канон arch/04: кворум жив, takeover ≤15 с).
                if (apiKeys > 0)
                    apiKeysEver = true;
                if (probe == 0)
                    masterAlive.Should().BeTrue("the master key is alive at the failure onset (probe 1/5)");
                masterAliveEver |= masterAlive;
                masterAliveAtWindowEnd = masterAlive; // последнее значение окна
                await Task.Delay(TimeSpan.FromSeconds(2), ct);
            }

            // К концу окна отказа ключи живы снова: DCS-хаппап члена etcd +
            // штраф failover'а за мёртвый IP дают кратковременные гашения
            // lease-ключей (TTL 5-15 с); канон (arch/04) обещает кворум/запись,
            // а НЕ zero-gap непрерывность ключей.
            apiKeysEver.Should().BeTrue("the worker api keepalive keys were published during the failure window");
            masterAliveAtWindowEnd.Should().BeTrue(
                "the master key is (re)granted by the lease daemon within the failure window");
            masterAliveEver.Should().BeTrue("the master key was observed alive during the failure window");

            var added = await E2ePhase.WaitAsync(Fx, "haetcd-shard3-registered",
                () => ShardRegisteredAsync(cluster, "shard3"), TimeSpan.FromSeconds(150), ct,
                // Прогресс: state-ключи нод shard3 + work-ключ (обрезан до ~120
                // симв., чтобы тик читался одной строкой).
                progress: async () =>
                {
                    var a = (await GetOrNullAsync($"/clusters/{cluster}/shards/shard3/nodes/shard3a/state"))?.Value ?? "-";
                    var b = (await GetOrNullAsync($"/clusters/{cluster}/shards/shard3/nodes/shard3b/state"))?.Value ?? "-";
                    var dsn = await GetOrNullAsync($"/clusters/{cluster}/shards/shard3/dsn") is null ? "absent" : "present";
                    var work = (await GetOrNullAsync($"/pgworker/work/{cluster}"))?.Value ?? "absent";
                    return $"shard3: {a}/{b}, dsn={dsn}, work={Trunc(work)}";
                });
            added.Should().BeTrue($"add-shard carried through the surviving endpoints; work={await WorkDumpAsync(cluster, ct)}");

            // Возврат узла: member list снова 3, health 3/3 (узел догоняет кластер).
            await Fx.StartEtcdNodeAsync(stopped, ct);
            var back = await E2ePhase.WaitAsync(Fx, "haetcd-node-back", async () =>
            {
                var members = (await Fx.EtcdctlAsync("member", "list"))
                    .Split('\n', StringSplitOptions.RemoveEmptyEntries);
                return members.Length == 3 && members.All(m => m.Contains("started"));
            }, TimeSpan.FromSeconds(60), ct);
            back.Should().BeTrue("node #1 must return to the quorum (the data-dir survived the stop)");

            // Финальная конвергенция: master-ключ жив (lease-демон актуального
            // мастера пишет через восстановленный кворум).
            var masterFinal = await E2ePhase.WaitAsync(Fx, "haetcd-master-key", async () =>
            {
                var key = await GetOrNullAsync($"/clusters/{cluster}/shards/shard1/master");
                return key is { Value.Length: > 0 };
            }, TimeSpan.FromSeconds(30), ct);
            masterFinal.Should().BeTrue("the master key is alive after the node returned (lease daemon converges)");
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

    // Чтение с перебором host-endpoints (URL остановленного узла — пропуск:
    // кто жертва отказа, решается на рантайме — folловер).
    private async Task<Shared.Etcd.Client.Kv?> GetOrNullAsync(string key)
    {
        foreach (var ep in Fx.EtcdEndpoints)
        {
            var read = await G.GetAsync(ep, key, TestContext.Current.CancellationToken);
            if (read.IsSuccess)
                return read.Value;
        }

        return null;
    }

    private async Task<IReadOnlyList<Shared.Etcd.Client.Kv>> RangeAsync(string prefix)
    {
        foreach (var ep in Fx.EtcdEndpoints)
        {
            var read = await G.RangeAsync(ep, prefix, TestContext.Current.CancellationToken);
            if (read.IsSuccess)
                return read.Value ?? [];
        }

        return [];
    }

    // Запись во время отказа — тот же перебор (сеем декларацию шарда сразу
    // после остановки folловера).
    private async Task PutAnyAsync(string key, string value, CancellationToken ct)
    {
        Exception? last = null;
        foreach (var ep in Fx.EtcdEndpoints)
        {
            var put = await G.PutAsync(ep, key, value, null, ct);
            if (put.IsSuccess)
                return;
            last = put.Error;
        }

        throw last ?? new ApplicationException($"put {key}: all etcd endpoints failed");
    }

    private async Task<string> WorkDumpAsync(string cluster, CancellationToken ct)
        => (await GetOrNullAsync($"/pgworker/work/{cluster}"))?.Value ?? "absent";

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
        await PutAnyAsync($"/clusters/{cluster}/shards/{shard}/replicas", "2", ct);
        await PutAnyAsync($"/clusters/{cluster}/shards/{shard}/nodes/{shard}a/state", "NOT_INITIALIZED", ct);
        await PutAnyAsync($"/clusters/{cluster}/shards/{shard}/nodes/{shard}b/state", "NOT_INITIALIZED", ct);
        await PutAnyAsync($"/service/{cluster}-{shard}/request_cpu", "2", ct);
        await PutAnyAsync($"/service/{cluster}-{shard}/request_mem", "2Gi", ct);
        await PutAnyAsync($"/service/{cluster}-{shard}/request_disk", "10Gi", ct);
    }
}

/// <summary>Компаньон-фикстура окружения класса (полный контур на класс:
/// сеть/etcd/CA/окно портов; docs/e2e-isolation.md §1).</summary>
public sealed class E2eHaEtcdScenariosFx : E2eContourFixture
{
    protected override string Slug => "ha-etcd";
    protected override bool HaEtcd => true;
}
