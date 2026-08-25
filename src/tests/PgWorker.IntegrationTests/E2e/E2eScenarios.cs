using System.Diagnostics;
using System.Text.Json;
using Npgsql;
using PgWorker.Etcd.Client;
using PgWorker.IntegrationTests.Docker;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E-сценарии приёмки spec §11 (задача 26) на живом стенде: etcd (Testcontainers)
// + pgworker-node (docker) + PgWorker.App хост-процессами. Сценарий последователен
// (AC2→AC7 на общих стенде/кластере): provisioning + O2 → takeover →
// deprovisioning → failover/rebuild → эвакуация → клэймы/снапшоты-лидер.
[Collection(E2eCollection.Name)]
public class E2eScenarios(E2eFixture fixture)
{
    private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };

    private string Endpoint => fixture.EtcdEndpoint;

    private EtcdGateway G => fixture.Gateway;

    [Fact]
    public async Task Acceptance_Scenario_Ac2_To_Ac7()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // ---------- AC2: provisioning e2e + O2 ----------
        await SeedClusterAsync("shop");
        await using var p1 = await fixture.StartHostAsync("p1", ct: ct);

        var provisioned = await E2eFixture.WaitForAsync(
            () => ProvisionedAsync("shop"), TimeSpan.FromSeconds(360), ct);
        provisioned.Should().BeTrue("provisioning должен дойти до DONE (dsn/RUNNING/без status/state)");

        await AssertProvisioningResultAsync("shop", ct);

        // ---------- AC3: takeover (kill посреди provisioning) ----------
        await SeedClusterAsync("shop2");
        var started = await E2eFixture.WaitForAsync(
            () => DockerHasAsync("pgw-shop2-"), TimeSpan.FromSeconds(120), ct);
        started.Should().BeTrue("первый инстанс должен начать provisioning shop2");

        p1.Kill(); // смерть контроллера посреди работы (клэйм истечёт ≤15 с)
        await using var p2 = await fixture.StartHostAsync("p2", ct: ct);

        var taken = await E2eFixture.WaitForAsync(
            () => ProvisionedAsync("shop2"), TimeSpan.FromSeconds(360), ct);
        taken.Should().BeTrue("второй инстанс должен донести shop2 до DONE (takeover)");

        var shop2Containers = await ListContainerNamesAsync("pgw-shop2-");
        shop2Containers.Should().HaveCount(4, "дублей контейнеров после takeover быть не должно");

        // ---------- AC4: deprovisioning ----------
        await SetToRemoveAsync("shop2");

        var deprovisioned = await E2eFixture.WaitForAsync(
            () => DeprovisionedAsync("shop2"), TimeSpan.FromSeconds(180), ct);
        deprovisioned.Should().BeTrue("deprovisioning должен удалить контейнеры/volume/ключи и снять клэйм");

        // ---------- AC5: failover/rebuild ----------
        await AssertFailoverRebuildAsync("shop", "shard1", ct);

        // ---------- AC6: эвакуация ----------
        await AssertEvacuationAsync("shop", "shard2", "shard1", p2.SnapshotsDir, ct);

        // ---------- AC7: клэймы + снапшоты только лидером ----------
        await AssertClaimsAndLeaderSnapshotsAsync(p2, ct);
    }

    // ===== Хелперы сида/чтения etcd =====

    // Сид кластера в стиле панели (02 §9.1): config NOT_INITIALIZED, 2 шарда ×
    // replicas=2, routing/status всех N, заявки request_*.
    private async Task SeedClusterAsync(string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        var config = $$"""
            {"buckets":6,"dbname":"{{cluster}}","created_unix":1755800000,"state":"NOT_INITIALIZED","bucket_admin_password":"{{E2eFixture.BucketAdminPassword}}"}
            """;
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/config", config, null, ct);
        foreach (var shard in new[] { "shard1", "shard2" })
        {
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/replicas", "2", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}a/state", "NOT_INITIALIZED", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}b/state", "NOT_INITIALIZED", null, ct);
            await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_cpu", "2", null, ct);
            // 8Gi: лимит памяти применяется к контейнеру (rework №5); 2G хватало
            // бы только заявке — Spilo с shared_buffers=2GB был бы убит cgroup.
            await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_mem", "8Gi", null, ct);
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

    // ===== Проверки AC =====

    // Условие готовности provisioning (AC2/AC3): dsn у всех шардов, все ноды
    // RUNNING, status-ключей нет, config без state (Д1).
    private async Task<bool> ProvisionedAsync(string cluster)
    {
        var config = await GetOrNullAsync($"/clusters/{cluster}/config");
        if (config is null)
            return false;
        if (JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(config.Value)!.ContainsKey("state"))
            return false;

        foreach (var shard in new[] { "shard1", "shard2" })
        {
            if (await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/dsn") is null)
                return false;
            foreach (var node in new[] { $"{shard}a", $"{shard}b" })
            {
                var state = await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/nodes/{node}/state");
                if (state?.Value != "RUNNING")
                    return false;
            }
        }

        return (await RangeAsync($"/clusters/{cluster}/buckets/status/")).Count == 0;
    }

    // Полные проверки результата provisioning (AC2): контейнеры, анти-аффинити
    // портов, O2 multi-host DSN probe, схемы/роли в PG.
    private async Task AssertProvisioningResultAsync(string cluster, CancellationToken ct)
    {
        // docker ps: 4 контейнера pgw-<C>-*; host-порты уникальны (план №5:
        // анти-аффинити на одном docker-хосте вырождается в «порты разные»).
        var names = await ListContainerNamesAsync($"pgw-{cluster}-");
        names.Should().HaveCount(4);
        var ports = await fixture.RunDockerAsync(
        ["ps", "--filter", $"name=pgw-{cluster}-", "--format", "{{.Ports}}"], ct);
        // IPv4/IPv6-байндинги дублируют порт — извлекаем host-порты регуляркой.
        var published = System.Text.RegularExpressions.Regex.Matches(ports, @":(\d+)->")
            .Select(m => m.Groups[1].Value)
            .ToList();
        // IPv4/IPv6-байндинги дублируют каждую пару — уникальных должно быть 8.
        published.Distinct().Should().HaveCount(8,
            "по 2 publish-порта (pg+patroni) на 4 ноды, все host-порты уникальны");

        // O2 (решение плана №9): probe SELECT 1 по DSN ИЗ КЛЮЧА как записан
        // (multi-host, порты разные, без пароля) + Password из сид-секрета.
        // Разделители libpq (пробелы) конвертируются в ';' для Npgsql — сами
        // хосты/порты (multi-host, разные порты) проверяются как записаны.
        var dsnKv = await GetOrNullAsync($"/clusters/{cluster}/shards/shard1/dsn");
        dsnKv.Should().NotBeNull();
        var dsn = dsnKv!.Value;
        dsn.Should().Contain(",", "multi-host DSN: обе ноды шарда (разные порты)")
            .And.Contain("user=bucket_admin").And.Contain("password=");
        // Npgsql не поддерживает СПИСОК портов (только один порт на все хосты),
        // поэтому multi-host DSN с разными портами пробуем пофрагментно:
        // каждая пара host:port из ключа должна отвечать на SELECT 1 (libpq
        // такой DSN поддерживает целиком — формат ключа остаётся libpq).
        var hosts = System.Text.RegularExpressions.Regex.Match(dsn, "host=([^ ]+)").Groups[1].Value.Split(',');
        var pgPorts = System.Text.RegularExpressions.Regex.Match(dsn, "port=([^ ]+)").Groups[1].Value.Split(',');
        hosts.Should().HaveCount(2);
        pgPorts.Should().HaveCount(2, "порты нод различаются (анти-аффинити на одном хосте)");
        foreach (var (host, port) in hosts.Zip(pgPorts))
        {
            // Реплика в первый момент может рестартовать Patroni (bootstrap) —
            // даём обеим нодам бюджет на готовность.
            var alive = await E2eFixture.WaitForAsync(async () =>
            {
                try
                {
                    await using var con = new NpgsqlConnection(
                        $"Host={host};Port={port};Database=shop;Username=bucket_admin;Password={E2eFixture.BucketAdminPassword};Timeout=5");
                    await con.OpenAsync(ct);
                    await using var cmd = new NpgsqlCommand("SELECT 1", con);
                    return await cmd.ExecuteScalarAsync(ct) is 1;
                }
                catch (NpgsqlException)
                {
                    return false; // рестарт/ещё не готова — повторим
                }
            }, TimeSpan.FromSeconds(60), ct);
            alive.Should().BeTrue($"нода {host}:{port} отвечает по DSN-параметрам из ключа");
        }

        // Роли и схемы на мастерах обоих шардов (P2.3–P2.4).
        foreach (var shard in new[] { "shard1", "shard2" })
        {
            var master = await WaitForMasterAsync(cluster, shard, ct);
            var adminDsn = $"Host=localhost;Port={master.Pg};Database=postgres;Username=postgres;Password={E2eFixture.SuPassword}";
            var roles = await SqlListAsync(adminDsn,
                "SELECT rolname FROM pg_roles WHERE rolname IN ('app','bucket_admin','bucket_mover') ORDER BY 1", ct);
            roles.Should().BeEquivalentTo(["app", "bucket_admin", "bucket_mover"],
                $"роли бакетного слоя созданы на мастере {shard}");

            // Схемы живут в БД кластера (роли — кластерные, видно из postgres).
            string[] expected = shard == "shard1" ? ["bucket_0", "bucket_2", "bucket_4"] : ["bucket_1", "bucket_3", "bucket_5"];
            var schemasDsn = $"Host=localhost;Port={master.Pg};Database={cluster};Username=postgres;Password={E2eFixture.SuPassword}";
            var schemas = await SqlListAsync(schemasDsn,
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'bucket_%' ORDER BY 1", ct);
            schemas.Should().BeEquivalentTo(expected, $"схемы бакетов {shard} созданы по routing");
        }
    }

    // AC5: docker stop лидера → master-ключ обновлён; остановленный пересоздан
    // (REBUILDING→RUNNING); pg_is_in_recovery расходится по нодам шарда.
    private async Task AssertFailoverRebuildAsync(string cluster, string shard, CancellationToken ct)
    {
        var masterBefore = await WaitForMasterAsync(cluster, shard, ct);
        var container = $"pgw-{cluster}-{shard}-{masterBefore.Node}";
        (await ContainerStatusAsync(container, ct)).Should().Be("running", "лидер до failover работает");

        var sw = Stopwatch.StartNew();
        await fixture.RunDockerAsync(["stop", container], ct);

        // Master-ключ обновляется (Patroni failover, P11: callback + reconciler).
        var flipped = await E2eFixture.WaitForAsync(async () =>
        {
            var key = await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/master");
            return key?.Value != $"{masterBefore.Host}:{masterBefore.Doorman}" && key is not null;
        }, TimeSpan.FromSeconds(30), ct);
        sw.Stop();
        flipped.Should().BeTrue("master-ключ должен обновиться после failover (P11: callback + reconciler)");
        sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(30), "failover-окно Patroni ttl=5/loop_wait=2 + сверка");

        // Rebuild: остановленный контейнер пересоздаётся, нода RUNNING.
        var rebuilt = await E2eFixture.WaitForAsync(async () =>
        {
            var state = await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/nodes/{masterBefore.Node}/state");
            return state?.Value == "RUNNING";
        }, TimeSpan.FromSeconds(300), ct);
        rebuilt.Should().BeTrue("остановленная нода должна быть пересоздана и вернуться в RUNNING");
        (await ContainerStatusAsync(container, ct)).Should().Be("running", "контейнер ноды пересоздан");

        // Реплика догоняет: на шарде ровно один мастер (pg_is_in_recovery).
        var addresses = await PortallocAsync(cluster);
        var nodes = addresses.Where(p => p.Key.StartsWith($"{shard}/")).ToList();
        nodes.Should().HaveCount(2);
        // Реплика догоняет (rebuild мог пересоздать контейнер прямо между
        // проверками) — собираем картину с ретраями до стабильного результата.
        var stable = await E2eFixture.WaitForAsync(async () =>
        {
            try
            {
                var recoveries = new List<bool>();
                foreach (var (_, addr) in nodes)
                {
                    var dsn = $"Host=localhost;Port={addr.Pg};Database=postgres;Username=postgres;Password={E2eFixture.SuPassword}";
                    recoveries.Add(await SqlScalarAsync(dsn, "SELECT pg_is_in_recovery()", ct) == "True");
                }

                return recoveries.Count(v => v) == 1;
            }
            catch (NpgsqlException)
            {
                return false; // нода рестартует (rebuild/патрони) — повторим
            }
        }, TimeSpan.FromSeconds(120), ct);
        stable.Should().BeTrue("ровно одна нода шарда — мастер, вторая в recovery (реплика догнала)");
    }

    // AC6: stop всех нод шарда → после ShardDeadSec (5 с) эвакуация бакетов на
    // живой шард; возврат нод → карантин (stop без удаления, QUARANTINED).
    private async Task AssertEvacuationAsync(
        string cluster, string deadShard, string aliveShard, string snapshotsDir, CancellationToken ct)
    {
        foreach (var container in await ListContainerNamesAsync($"pgw-{cluster}-{deadShard}-"))
            await fixture.RunDockerAsync(["stop", container], ct);

        // Эвакуация: journal DONE (E4) после ShardDeadSec.
        var evacuated = await E2eFixture.WaitForAsync(async () =>
        {
            var journal = await GetOrNullAsync($"/pgworker/evacuations/{cluster}/{deadShard}");
            if (journal is null)
                return false;
            var state = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(journal.Value) ?? [];
            return state.TryGetValue("state", out var s) && s.GetString() == "DONE";
        }, TimeSpan.FromSeconds(120), ct);
        evacuated.Should().BeTrue("эвакуация должна завершиться после ShardDeadSec");

        var routing = (await RangeAsync($"/clusters/{cluster}/buckets/routing/"))
            .ToDictionary(kv => kv.Key, kv => kv.Value);
        routing.Values.Should().OnlyContain(v => v == aliveShard,
            "владение бакетами умершего шарда переведено на живой (E2)");

        // Схемы эвакуированных бакетов созданы на живом шарде (E1).
        var master = await WaitForMasterAsync(cluster, aliveShard, ct);
        var schemasDsn = $"Host=localhost;Port={master.Pg};Database={cluster};Username=postgres;Password={E2eFixture.SuPassword}";
        var schemas = await SqlListAsync(schemasDsn,
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'bucket_%' ORDER BY 1", ct);
        schemas.Should().HaveCount(6, "все 6 схем бакетов теперь на живом шарде");

        // Ноды мёртвого шарда — QUARANTINED (E3).
        foreach (var node in new[] { $"{deadShard}a", $"{deadShard}b" })
        {
            var state = await GetOrNullAsync($"/clusters/{cluster}/shards/{deadShard}/nodes/{node}/state");
            state?.Value.Should().Be("QUARANTINED", $"{node} эвакуирована — карантин");
        }

        // Снапшоты «до» и «после» (P12): в каталоге держателя клэйма.
        Directory.GetFiles(snapshotsDir, "snapshot-*.db")
            .Should().HaveCountGreaterThanOrEqualTo(2, "снапшоты до/после эвакуации сняты");

        // Возврат шарда: docker start вручную → PgWorker останавливает (P1-призраки).
        foreach (var container in await ListContainerNamesAsync($"pgw-{cluster}-{deadShard}-", all: true))
            await fixture.RunDockerAsync(["start", container], ct);

        var quarantined = await E2eFixture.WaitForAsync(async () =>
        {
            var journal = await GetOrNullAsync($"/pgworker/evacuations/{cluster}/{deadShard}");
            if (journal is null)
                return false;
            var state = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(journal.Value) ?? [];
            return state.TryGetValue("state", out var s) && s.GetString() == "QUARANTINED"
                && state.ContainsKey("returned_unix");
        }, TimeSpan.FromSeconds(120), ct);
        if (!quarantined)
        {
            var current = await GetOrNullAsync($"/pgworker/evacuations/{cluster}/{deadShard}");
            Assert.Fail($"возврат шарда не зафиксирован за 120с; journal: {current?.Value ?? "нет"}; " +
                        $"статусы контейнеров: {string.Join(",", await Task.WhenAll(
                            (await ListContainerNamesAsync($"pgw-{cluster}-{deadShard}-", all: true))
                            .Select(async n => $"{n}={await ContainerStatusAsync(n, ct)}")))}");
        }

        foreach (var container in await ListContainerNamesAsync($"pgw-{cluster}-{deadShard}-", all: true))
        {
            var status = await ContainerStatusAsync(container, ct);
            status.Should().Be("exited", $"PgWorker останавливает вернувшиеся ноды ({container}), данные не тронуты");
        }

        // Данные на месте: volume мёртвого шарда живы (E3 — ничего не удаляем).
        var volumes = await fixture.RunDockerAsync(
        ["volume", "ls", "-q", "--filter", $"name=pgw-{cluster}-{deadShard}-"], ct);
        volumes.Split('\n', StringSplitOptions.RemoveEmptyEntries).Should().HaveCount(2);
    }

    // AC7: два инстанса — кластер обрабатывает один; снапшоты снимает только лидер.
    private async Task AssertClaimsAndLeaderSnapshotsAsync(HostInstance previous, CancellationToken ct)
    {
        previous.Kill(); // предыдущий держатель уходит — лидерство и клэймы переизберутся

        await using var p3 = await fixture.StartHostAsync("p3", snapshotIntervalMin: 1, ct: ct);
        await using var p4 = await fixture.StartHostAsync("p4", snapshotIntervalMin: 1, ct: ct);

        // Лидер выбран ровно один (Д2: снапшоты — singleton-работа).
        var hasLeader = await E2eFixture.WaitForAsync(
            async () => await GetOrNullAsync("/pgworker/leader") is not null,
            TimeSpan.FromSeconds(30), ct);
        hasLeader.Should().BeTrue("лидер снапшотов должен быть выбран");

        // Снапшоты снимает ТОЛЬКО лидер: файл появился ровно в одном каталоге.
        var instances = new[] { p3, p4 };
        var shot = await E2eFixture.WaitForAsync(
            () => Task.FromResult(instances.Count(i =>
                Directory.GetFiles(i.SnapshotsDir, "snapshot-*.db").Length > 0) > 0),
            TimeSpan.FromSeconds(60), ct);
        shot.Should().BeTrue("лидер снимает регулярные снапшоты (SnapshotLoop)");

        await Task.Delay(TimeSpan.FromSeconds(5), ct);
        instances.Count(i => Directory.GetFiles(i.SnapshotsDir, "snapshot-*.db").Length > 0)
            .Should().Be(1, "не-лидер снапшоты не снимает (§11.7)");

        // Кластер обрабатывает ровно один инстанс: work.instance стабилен.
        var seen = new HashSet<string>();
        for (var i = 0; i < 3; i++)
        {
            var work = await GetOrNullAsync("/pgworker/work/shop");
            work.Should().NotBeNull();
            seen.Add(JsonSerializer
                .Deserialize<Dictionary<string, JsonElement>>(work!.Value)!["instance"].GetString()!);
            await Task.Delay(TimeSpan.FromSeconds(3), ct);
        }

        seen.Should().HaveCount(1, "кластер shop обрабатывает только один инстанс (клэймы Д2)");
    }

    // ===== Хелперы docker/sql =====

    private async Task<bool> DockerHasAsync(string prefix)
        => (await ListContainerNamesAsync(prefix)).Count > 0;

    private async Task<List<string>> ListContainerNamesAsync(string prefix, bool all = false)
    {
        var ct = TestContext.Current.CancellationToken;
        var args = new List<string> { "ps", "--format", "{{.Names}}" };
        if (all)
            args.Add("-a");
        args.AddRange(["--filter", $"name={prefix}"]);
        var output = await fixture.RunDockerAsync([.. args], ct);
        return output.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(n => n.StartsWith(prefix, StringComparison.Ordinal))
            .ToList();
    }

    private async Task<string> ContainerStatusAsync(string name, CancellationToken ct)
        => await fixture.RunDockerAsync(["inspect", "-f", "{{.State.Status}}", name], ct);

    // Master-ключ шарда → адрес ноды из portalloc (host:doorman → шард/нода).
    private sealed record MasterAddr(string Node, string Host, int Pg, int Doorman);

    private async Task<MasterAddr> WaitForMasterAsync(string cluster, string shard, CancellationToken ct)
    {
        for (var i = 0; i < 60; i++)
        {
            var key = await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/master");
            if (key is { Value.Length: > 0 })
            {
                var parts = key.Value.Split(':');
                var addresses = await PortallocAsync(cluster);
                var match = addresses.FirstOrDefault(p =>
                    p.Key.StartsWith($"{shard}/") && p.Value.Doorman.ToString() == parts[^1]);
                if (match.Key is { Length: > 0 })
                    return new MasterAddr(match.Key.Split('/')[1], match.Value.Host, match.Value.Pg, match.Value.Doorman);
            }

            await Task.Delay(1000, ct);
        }

        throw new ApplicationException($"мастер {cluster}/{shard} не найден за 60 с");
    }

    private static async Task<List<string>> SqlListAsync(string dsn, string sql, CancellationToken ct)
    {
        await using var con = new NpgsqlConnection($"{dsn};Timeout=10");
        await con.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(sql, con);
        var result = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            result.Add(reader.GetString(0));
        return result;
    }

    private static async Task<string> SqlScalarAsync(string dsn, string sql, CancellationToken ct)
    {
        await using var con = new NpgsqlConnection($"{dsn};Timeout=10");
        await con.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(sql, con);
        return (await cmd.ExecuteScalarAsync(ct))?.ToString() ?? "";
    }

    // Условие готовности deprovisioning (AC4): контейнеров/volume нет, префиксы
    // etcd пусты, клэйм снят ЯВНО (не ждём TTL — ревизия 2 плана №5).
    private async Task<bool> DeprovisionedAsync(string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        if ((await ListContainerNamesAsync($"pgw-{cluster}-", all: true)).Count > 0)
            return false;

        var volumes = await fixture.RunDockerAsync(
        ["volume", "ls", "-q", "--filter", $"name=pgw-{cluster}-"], ct);
        if (volumes.Length > 0)
            return false;

        if ((await RangeAsync($"/clusters/{cluster}/")).Count > 0)
            return false;
        if ((await RangeAsync($"/service/{cluster}-")).Count > 0)
            return false;
        if (await GetOrNullAsync($"/pgworker/claims/{cluster}") is not null)
            return false;

        return true;
    }
}

// Один e2e-стенд на все сценарии (последовательность AC2→AC7).
[CollectionDefinition(Name)]
public sealed class E2eCollection : ICollectionFixture<E2eFixture>
{
    public const string Name = "e2e";
}
