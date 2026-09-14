using System.Text.Json;
using Npgsql;
using Shared.Etcd.Client;
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
//    параллельные параметры в YAML отсутствуют;
// 4) конвергенция pg-параметров живого шарда (t11): смена заявки → PATCH DCS
//    без пересоздания, динамика применена живым PG, postmaster → pending_restart.
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
    public async Task Pgtune_ClaimsRequired_ProvisioningFailsUntilClaimSeeded()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        await using var fx = await E2eEnvironment.StartAsync("pgtune-claims", ct: ct);
        Fx = fx;
        var cluster = $"dshop{Fx.ClusterTag}";

        // Arrange: сид БЕЗ заявок request_* (SeedClusterAsync сеет их только при
        // requestMem != null) — заявки ОБЯЗАТЕЛЬНЫ (arch/14 §2.1 п.4).
        await SeedClusterAsync(cluster, requestMem: null, ct);
        await using var host = await Fx.StartHostAsync("s1", ct: ct);

        // Act: подождать фейл фазы provisioning без заявки (journal-ошибка
        // содержит request_mem; FailAsync пишет error.Message в /pgworker/work).
        var journaled = await E2eFixture.WaitForAsync(async () =>
            (await WorkDumpAsync(cluster, ct)).Contains("request_mem", StringComparison.Ordinal),
            TimeSpan.FromSeconds(180), ct);

        // Assert: без обязательной заявки кластер НЕ поднялся — конфиг всё ещё
        // со state=NOT_INITIALIZED (Active снимает state-ключ).
        journaled.Should().BeTrue("без обязательной заявки request_mem provisioning обязан падать с journal-ошибкой; " +
                                  $"work={await WorkDumpAsync(cluster, ct)}");
        (await GetOrNullAsync($"/clusters/{cluster}/config"))!.Value
            .Should().Contain("NOT_INITIALIZED", "кластер без обязательных заявок не может стать Active");

        // Act: посеять ОБЯЗАТЕЛЬНЫЕ заявки → транзиент-ретрай подхватывает их
        // следующим тиком provisioning.
        foreach (var shard in new[] { "shard1", "shard2" })
        {
            (await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_cpu", "2", null, ct))
                .IsSuccess.Should().BeTrue("заявка request_cpu должна записаться");
            (await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_mem", "8Gi", null, ct))
                .IsSuccess.Should().BeTrue("заявка request_mem должна записаться");
        }

        var provisioned = await E2eFixture.WaitForAsync(
            () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(360), ct);
        provisioned.Should().BeTrue("после посева обязательных заявок provisioning должен дойти до Active; " +
                                    $"work={await WorkDumpAsync(cluster, ct)}");

        // Assert: env нод рассчитан от заявки 8Gi: 2GB/6GB/60; заявка cpu 2 < 4 →
        // параллельные параметры отсутствуют (никаких суррогатных дефолтов).
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

    // E2E-сценарий конвергенции pg-параметров (t11 spec §4.4, AC §7 п.1–3):
    // живой кластер на заявке 8Gi → перезапись request_mem 16Gi → тик надзора
    // патчит DCS: (а) GET /config несёт пересчитанные параметры (динамика +
    // postmaster); (б) динамический параметр фактически применён живым PG
    // (reload в пределах loop_wait); (в) нода с изменённым postmaster —
    // pending_restart=true, рестарта/пересоздания нет; (г) идемпотентность —
    // mod_revision /service/<scope>/config стабилен после конвергенции
    // (не второй регулярный писатель); (д) возврат заявки 8Gi — патч вниз.
    [Fact]
    public async Task Pgtune_Convergence_RequestMemChanged_DcsPatchedLivePgReloaded()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        await using var fx = await E2eEnvironment.StartAsync("pgtune-converge", ct: ct);
        Fx = fx;
        var cluster = $"cshop{Fx.ClusterTag}";
        var scope = $"{cluster}-shard1";

        // Arrange: живой кластер на заявке 8Gi (расчёт: shared_buffers 2GB,
        // effective_cache_size 6GB = 8Gi×3/4).
        await SeedClusterAsync(cluster, requestMem: "8Gi", ct);
        await using var host = await Fx.StartHostAsync("s1", ct: ct);
        var provisioned = await E2eFixture.WaitForAsync(
            () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(360), ct);
        provisioned.Should().BeTrue("provisioning кластера должен дойти до Active; " +
                                    $"work={await WorkDumpAsync(cluster, ct)}");

        var master = await MasterInfoAsync(cluster, "shard1", ct);
        (await SqlScalarAsync(master.Dsn,
                "SELECT current_setting('effective_cache_size')", ct))
            .Should().Be("6GB", "bootstrap-расчёт от заявки 8Gi применён при инициализации");
        // Бутовое значение postmaster-параметра: Spilo при инициализации
        // пересчитывает shared_buffers из памяти контейнера, поэтому живое
        // значение — НЕ литерал DCS ("2GB"), а автотюнинг (напр. "1983MB";
        // инцидент первого прогона t11, 2026-09-14). Для проверки «postmaster
        // не применён до рестарта» фиксируем фактическое бутовое значение.
        var sharedBuffersAtBoot = await SqlScalarAsync(
            master.Dsn, "SELECT current_setting('shared_buffers')", ct);

        // Act: перезаписать заявку 8Gi → 16Gi (расчёт: effective_cache_size 12GB —
        // динамика; shared_buffers 4GB — postmaster) — ЖИВОЙ шард, ноды не трогаем.
        (await G.PutAsync(Endpoint, $"/service/{scope}/request_mem", "16Gi", null, ct))
            .IsSuccess.Should().BeTrue("заявка request_mem должна перезаписаться");

        // Assert (а): GET /config несёт пересчитанные параметры (динамика +
        // postmaster) — тик надзора патчит DCS одним документом.
        var configUpdated = await E2eFixture.WaitForAsync(async () =>
            await GetPatroniParameterAsync(master.PatroniPort, "shared_buffers", ct) == "4GB"
            && await GetPatroniParameterAsync(master.PatroniPort, "effective_cache_size", ct) == "12GB",
            TimeSpan.FromSeconds(120), ct);
        configUpdated.Should().BeTrue("тик надзора обязан патчить DCS-конфиг от новой заявки; " +
                                      $"work={await WorkDumpAsync(cluster, ct)}");

        // Assert (б): динамический параметр фактически применён живым PG без
        // рестарта (Patroni reload в пределах loop_wait; поллинг).
        var reloaded = await E2eFixture.WaitForAsync(async () =>
            await SqlScalarAsync(master.Dsn,
                "SELECT current_setting('effective_cache_size')", ct) == "12GB",
            TimeSpan.FromSeconds(60), ct);
        reloaded.Should().BeTrue("динамический параметр применяется Patroni без рестарта ноды");

        // Assert (в): postmaster-параметр помечен pending_restart=true (GET
        // /patroni), рестарт воркером НЕ инициируется: контейнеры живы;
        // применённое значение ещё 2GB (bootstrap-расчёт).
        var pending = await E2eFixture.WaitForAsync(async () =>
            (await GetPatroniFieldAsync(master.PatroniPort, "pending_restart", ct)) == "true",
            TimeSpan.FromSeconds(60), ct);
        pending.Should().BeTrue("Patroni обязан пометить postmaster-расхождение pending_restart");
        (await SqlScalarAsync(master.Dsn, "SELECT current_setting('shared_buffers')", ct))
            .Should().Be(sharedBuffersAtBoot,
                "postmaster-параметр НЕ применён до рестарта (решение 2026-09-14)");
        (await ListContainerNamesAsync($"pgw-{cluster}-shard1-", all: true))
            .Should().HaveCount(2, "воркер не пересоздаёт и не рестартует ноды конвергенцией");

        // Assert (г): идемпотентность — сначала конфиг ОСЕДАЕТ (mod_revision
        // не меняется 5 с: серия конвергенций при смене заявки завершена;
        // журнал фаз не годится — /pgworker/work перезаписывается каждым
        // тиком), затем mod_revision etcd-ключа /service/<scope>/config
        // стабилен окно в несколько тиков надзора (не второй регулярный
        // писатель).
        var settled = await ConfigModRevisionSettledAsync(
            scope, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(30), ct);
        settled.Should().BeTrue("конфиг должен перестать мутировать после конвергенции; " +
                                $"work={await WorkDumpAsync(cluster, ct)}");
        var stable = await ConfigModRevisionStableAsync(scope, TimeSpan.FromSeconds(15), ct);
        stable.Should().BeTrue("повторные тики не патчат конвергентный конфиг");

        // Act (д): возврат заявки 16Gi → 8Gi — патч ВНИЗ отрабатывает.
        (await G.PutAsync(Endpoint, $"/service/{scope}/request_mem", "8Gi", null, ct))
            .IsSuccess.Should().BeTrue("заявка request_mem должна вернуться к 8Gi");
        var rolledBack = await E2eFixture.WaitForAsync(async () =>
            await GetPatroniParameterAsync(master.PatroniPort, "shared_buffers", ct) == "2GB"
            && await GetPatroniParameterAsync(master.PatroniPort, "effective_cache_size", ct) == "6GB",
            TimeSpan.FromSeconds(120), ct);
        rolledBack.Should().BeTrue("уменьшение значений тоже конвергируется (патч вниз); " +
                                   $"work={await WorkDumpAsync(cluster, ct)}");
    }

    // ===== Хелперы (приёмы E2eScaleScenarios, scoped на кластер) =====

    private static readonly HttpClient PatroniHttp = new() { Timeout = TimeSpan.FromSeconds(3) };

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

    // Значение параметра из GET /config Patroni-ноды. JSON разбирается, а не
    // ищется подстрокой: Patroni сериализует ответ json.dumps'ом С ПРОБЕЛАМИ
    // ({"shared_buffers": "4GB"}) — компактные Contains не совпадают никогда
    // (разбор по инциденту первого прогона t11, 2026-09-14).
    private static async Task<string?> GetPatroniParameterAsync(int patroniPort, string name, CancellationToken ct)
    {
        using var response = await PatroniHttp.GetAsync($"http://localhost:{patroniPort}/config", ct);
        response.IsSuccessStatusCode.Should().BeTrue($"GET /config → HTTP {(int)response.StatusCode}");
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct));
        var root = doc.RootElement;
        if (!root.TryGetProperty("postgresql", out var postgresql)
            || postgresql.ValueKind != JsonValueKind.Object
            || !postgresql.TryGetProperty("parameters", out var parameters)
            || parameters.ValueKind != JsonValueKind.Object
            || !parameters.TryGetProperty(name, out var value))
            return null;
        return value.ValueKind switch
        {
            JsonValueKind.String => value.GetString(),
            JsonValueKind.Number => value.GetRawText(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            _ => null,
        };
    }

    // Поле корня GET /patroni (например, pending_restart) raw-текстом ("true").
    private static async Task<string?> GetPatroniFieldAsync(int patroniPort, string field, CancellationToken ct)
    {
        using var response = await PatroniHttp.GetAsync($"http://localhost:{patroniPort}/patroni", ct);
        if (!response.IsSuccessStatusCode)
            return null;
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct));
        return doc.RootElement.TryGetProperty(field, out var value) ? value.GetRawText() : null;
    }

    // mod_revision /service/<scope>/config не меняется окно quiet (конфиг
    // осел — серия конвергенций завершена); false — бюджет истёк при живой
    // мутации конфига (пинг-понг патчей) или ключа нет.
    private async Task<bool> ConfigModRevisionSettledAsync(
        string scope, TimeSpan quiet, TimeSpan budget, CancellationToken ct)
    {
        var deadline = DateTimeOffset.UtcNow + budget;
        var lastChange = DateTimeOffset.UtcNow;
        var revision = (await GetOrNullAsync($"/service/{scope}/config"))?.ModRevision;
        if (revision is null)
            return false;
        while (DateTimeOffset.UtcNow < deadline)
        {
            await Task.Delay(1000, ct);
            var current = (await GetOrNullAsync($"/service/{scope}/config"))?.ModRevision;
            if (current is null)
                return false;
            if (current != revision)
            {
                revision = current;
                lastChange = DateTimeOffset.UtcNow;
            }
            else if (DateTimeOffset.UtcNow - lastChange >= quiet)
                return true;
        }

        return false;
    }

    // mod_revision etcd-ключа /service/<scope>/config стабилен окно window
    // (перечитываем каждые 2 с; Patroni пишет конфиг только при изменении).
    private async Task<bool> ConfigModRevisionStableAsync(string scope, TimeSpan window, CancellationToken ct)
    {
        var revision = (await GetOrNullAsync($"/service/{scope}/config"))?.ModRevision;
        if (revision is null)
            return false;
        var deadline = DateTimeOffset.UtcNow + window;
        while (DateTimeOffset.UtcNow < deadline)
        {
            await Task.Delay(2000, ct);
            if ((await GetOrNullAsync($"/service/{scope}/config"))?.ModRevision != revision)
                return false;
        }

        return true;
    }

    // SQL-скаляр мастера шарда (паттерн E2eScaleScenarios.SqlScalarAsync).
    private static async Task<string> SqlScalarAsync(string dsn, string sql, CancellationToken ct)
    {
        await using var con = new NpgsqlConnection(
            $"{dsn};Timeout=10;SSL Mode=Require;Trust Server Certificate=true");
        await con.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(sql, con);
        return (await cmd.ExecuteScalarAsync(ct))?.ToString() ?? "";
    }

    private sealed record NodeAddr(string Host, int Pg, int Patroni, int Doorman);

    private async Task<Dictionary<string, NodeAddr>> PortallocAsync(string cluster)
    {
        var kv = await GetOrNullAsync($"/pgworker/portalloc/{cluster}");
        if (kv is null)
            return [];
        return JsonSerializer.Deserialize<Dictionary<string, NodeAddr>>(kv.Value, Json) ?? [];
    }

    private sealed record MasterInfo(string Node, int Port, int PatroniPort, string Dsn);

    // Мастер шарда: резолв по контракту arch/14 §5 C — проба /primary по
    // patroni-портам portalloc (приём E2eScaleScenarios.MasterInfoAsync:
    // матч master-ключа по doorman-порту недискриминантен при EnableDoorman=false).
    private async Task<MasterInfo> MasterInfoAsync(string cluster, string shard, CancellationToken ct)
    {
        for (var i = 0; i < 60; i++)
        {
            var key = await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/master");
            if (key is { Value.Length: > 0 })
            {
                var addresses = await PortallocAsync(cluster);
                foreach (var (nodeKey, addr) in addresses
                             .Where(p => p.Key.StartsWith($"{shard}/", StringComparison.Ordinal))
                             .OrderBy(p => p.Key, StringComparer.Ordinal))
                {
                    try
                    {
                        using var response = await PatroniHttp.GetAsync(
                            $"http://localhost:{addr.Patroni}/primary", ct);
                        if (!response.IsSuccessStatusCode)
                            continue;
                        var node = nodeKey.Split('/')[1];
                        return new MasterInfo(node, addr.Pg, addr.Patroni,
                            $"Host=localhost;Port={addr.Pg};Database={cluster};Username=postgres;Password={E2eFixture.SuPassword}");
                    }
                    catch (Exception)
                    {
                        // сетевой сбой пробы (рестарт/ещё не готова) — не primary
                    }
                }
            }

            await Task.Delay(1000, ct);
        }

        throw new ApplicationException($"мастер {cluster}/{shard} не найден за 60 с");
    }

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
