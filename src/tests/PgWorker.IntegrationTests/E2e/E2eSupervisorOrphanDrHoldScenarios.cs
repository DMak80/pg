using System.Diagnostics;
using System.Text.Json;
using Npgsql;
using PgWorker.Core.Templates;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.IntegrationTests.Docker;
using PgWorker.Provisioning.Sql;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;


// E2E DR-hold сирот (t24/ревизия 8 — разбиение E2eSupervisorScenarios;
// хелперы — копии): OrphanDrHold (~4-5 мин).
// E2eEnvironment (своя сеть/etcd/MinIO, динамические порты, полный teardown
// в DisposeAsync — docs/e2e-isolation.md; телеметрия — docs/e2e-launch.md).
// Два сценария AC10: (1) ChainBroken_SelfHeals — дыра ВНУТРИ цепочки живого
// кластера → wal=BROKEN → планировщик переснимает полный → цепь непрерывна от
// нового wal_start → ACTIVE, агент жив; (2) OrphanRegistry_TtlDelete —
// чужой префикс без владельца → реестр /pgworker/backups/orphans (OBSERVED) →
// сжатый TTL → объекты удалены, запись погашена.
public class E2eSupervisorOrphanDrHoldScenarios
{
    private const string Bucket = "pgworker-backups";

    // Окружение Fact'а (своя сеть/etcd/MinIO); создаётся в начале сценария.
    private E2eEnvironment Fx = null!;

    private string Endpoint => Fx.EtcdEndpoint;

    private EtcdGateway G => Fx.Gateway;


    [Fact]
    public async Task Backup_OrphanDrHold_ПолныйПереживаетTtl_ЗаявкаЧистит()
    {
        // Arrange 1 — окружение bk-drh (withMinio), воркер с OrphanTtlSec=120,
        //   Supervisor IntervalSec=60 (StartOrphanHostAsync — сжатое время)
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await RunScenarioAsync("bk-drh", async fx =>
        {
        Fx = fx;
        var cluster = $"bkdrh{Fx.ClusterTag}";
        var ghost = $"ghost{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        await using var app = await StartOrphanHostAsync(cluster, ct);

        // Arrange 2 — provisioning DONE; посев СИРОТЫ С ВАЛИДНЫМ ПОЛНЫМ:
        //   manifest — критерий DR-выбора (arch/19 §3.5/§5)
        var provisioned = await WaitPhaseAsync("provisioning",
            () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(360), ct);
        provisioned.Should().BeTrue("provisioning обязан дойти до DONE");
        await McCpAsync($"{ghost}/shard1/full/20260911110000Z/base.tar", "dr-data");
        await McCpAsync($"{ghost}/shard1/full/20260911110000Z/backup_manifest", "manifest");

        // Act 1 — реестр: запись с has_valid_full=true (WaitFor)
        var observed = await WaitPhaseAsync("orphan-has-full", async () =>
        {
            var raw = await GetOrNullAsync("/pgworker/backups/orphans");
            return raw?.Value.Contains($"{ghost}/shard1") == true
                   && raw.Value.Contains("\"has_valid_full\":true");
        }, TimeSpan.FromSeconds(180), ct);
        observed.Should().BeTrue(
            $"сирота обязана попасть в реестр с has_valid_full=true: [{(await GetOrNullAsync("/pgworker/backups/orphans"))?.Value}]");

        // Act 2 — окно выживания: WaitFor, пока с момента first_seen записи
        // прошло ≥ TTL (120 c) + один проход (60 c) = 180 c — к этому моменту
        // sweeper УЖЕ обязан был удалить незащищённую (поллинг по часам хоста;
        // сна >30 c в тесте нет — условие опрашивается тиками WaitFor)
        long firstSeen = 0;
        var window = await WaitPhaseAsync("dr-ttl-window", async () =>
        {
            var raw = await GetOrNullAsync("/pgworker/backups/orphans");
            if (raw is null || !raw.Value.Contains($"{ghost}/shard1"))
                return false;
            using var doc = JsonDocument.Parse(raw.Value);
            firstSeen = doc.RootElement.GetProperty("orphans").EnumerateArray()
                .Single(o => o.GetProperty("prefix").GetString() == $"{ghost}/shard1")
                .GetProperty("first_seen_unix").GetInt64();
            return DateTimeOffset.UtcNow.ToUnixTimeSeconds() >= firstSeen + 120 + 60;
        }, TimeSpan.FromSeconds(360), ct);
        window.Should().BeTrue("окно истёкшего TTL + один проход обязано наступить");

        // Assert 1 — объекты ЖИВЫ, запись OBSERVED с has_valid_full (AC1)
        (await McLsAsync($"{ghost}/shard1/")).Should().HaveCountGreaterThanOrEqualTo(2,
            "DR-источник с валидным полным не удаляется автоматикой НИКОГДА");
        var registryAfter = (await GetOrNullAsync("/pgworker/backups/orphans"))!.Value;
        registryAfter.Should().Contain($"{ghost}/shard1")
            .And.Contain("OBSERVED").And.Contain("\"has_valid_full\":true",
            "запись жива под автоправилом");

        // Act 3 — заявка явного удаления (ручной путь канона, runbook §2);
        //   after-окно: TTL уже истёк, заявка приоритетнее — ближайший проход
        await G.PutAsync(Endpoint,
            $"/pgworker/backups/orphan-deletes/{ghost}/shard1",
            $$"""{"requested_unix":{{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}},"requested_by":"operator"}""",
            null, ct);

        // Assert — sweeper исполнил: объекты удалены, запись и заявка погашены
        var swept = await WaitPhaseAsync("orphan-delete-by-request", async () =>
        {
            var listed = await McLsAsync($"{ghost}/shard1/");
            var raw = await GetOrNullAsync("/pgworker/backups/orphans");
            var req = await GetOrNullAsync($"/pgworker/backups/orphan-deletes/{ghost}/shard1");
            return listed.Count == 0
                   && (raw is null || !raw.Value.Contains($"{ghost}/shard1"))
                   && req is null;
        }, TimeSpan.FromSeconds(300), ct);
        swept.Should().BeTrue("заявка удаляет защищённую сироту; запись и заявка гасятся вместе");
        }, ct);
    }

    // ===== Хелперы (копии образца E2eRetentionScenarios — файлы сценариев
    // независимы, паттерн репо) =====

    // Политика телеметрии (docs/e2e-launch.md, образец E2eRestoreScenarios):
    // упавший тест помечает окружение MarkFailed — teardown ОСТАНАВЛИВАЕТ
    // контейнеры, но не удаляет (host.log/тома остаются для разбора «что
    // произошло»); перезапуск ради логов запрещён; зачистка — вручную.
    private async Task RunScenarioAsync(
        string slug, Func<E2eEnvironment, Task> body, CancellationToken ct)
    {
        var fx = await E2eEnvironment.StartAsync(slug, withMinio: true, ct: ct);
        Fx = fx;
        try
        {
            await body(fx);
        }
        catch
        {
            fx.MarkFailed();
            throw;
        }
        finally
        {
            await fx.DisposeAsync();
        }
    }

    // Реальный COMPLETED полный (без требований к позиции wal_start).
    private async Task<string> WaitForCompletedFullAsync(string cluster)
    {
        var completed = await WaitPhaseAsync("first-full-completed",
            async () => (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains("COMPLETED")),
            TimeSpan.FromSeconds(300), TestContext.Current.CancellationToken);
        completed.Should().BeTrue("реальный полный обязан дойти до COMPLETED");
        var done = (await FullKeysAsync(cluster, "shard1")).Single(f => f.Value.Contains("COMPLETED"));
        return done.Key.Split('/').Last();
    }

    // Ожидание фазы с телеметрией (docs/e2e-launch.md): длительность в журнал
    // теста всегда; фаза дольше 60 с — docker-логи окружения снимаются в
    // артефакты немедленно (отчёт «почему так долго» собирается по логам,
    // без перезапуска), независимо от исхода фазы.
    private async Task<bool> WaitPhaseAsync(
        string phase, Func<Task<bool>> condition, TimeSpan timeout, CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        var ok = await E2eFixture.WaitForAsync(condition, timeout, ct);
        sw.Stop();
        Console.WriteLine($"[PHASE] {phase}: ok={ok}, elapsed={sw.Elapsed.TotalSeconds:F1}s");
        if (sw.Elapsed > TimeSpan.FromSeconds(60))
            await Fx.CollectDiagnosticsAsync($"slow-phase-{phase}-{(int)sw.Elapsed.TotalSeconds}s");
        return ok;
    }

    // Воркер самолечения: S3 на локальный MinIO, ускоренный бэкофф, ретенция
    // выключена (IntervalSec большой — чистка не мешает сценарию), Wal-контроль
    // каждые 5 c (скорость BROKEN), сверка супервизора раз в 60 c.
    private Task<HostInstance> StartSupervisorHostAsync(string name, CancellationToken ct)
        => Fx.StartHostAsync(name, extraEnv: new Dictionary<string, string>
        {
            ["PgWorker__Backups__Enabled"] = "true",
            ["PgWorker__Backups__S3__Endpoint"] = Fx.S3Endpoint,
            ["PgWorker__Backups__S3__Bucket"] = Bucket,
            ["PgWorker__Backups__S3__AccessKey"] = "minioadmin",
            ["PgWorker__Backups__S3__SecretKey"] = "minioadmin",
            ["PgWorker__Backups__Job__Image"] = E2eEnvironment.JobImage,
            ["PgWorker__Backups__Wal__AgentImage"] = E2eEnvironment.WalImage,
            ["PgWorker__Backups__Retry__BaseSec"] = "2",
            ["PgWorker__Backups__Retry__MaxSec"] = "4",
            ["PgWorker__Backups__Policy__FullMaxAgeSec"] = "3600",
            ["PgWorker__Backups__Policy__VerifyOnCreate"] = "false",
            ["PgWorker__Backups__Retention__IntervalSec"] = "3600",
            ["PgWorker__Backups__Wal__VerifyIntervalSec"] = "5",
            ["PgWorker__Backups__Supervisor__IntervalSec"] = "60",
        }, ct: ct);

    // Воркер сценария сирот: реестр+TTL в сжатом времени; verify off.
    private Task<HostInstance> StartOrphanHostAsync(string name, CancellationToken ct)
        => Fx.StartHostAsync(name, extraEnv: new Dictionary<string, string>
        {
            ["PgWorker__Backups__Enabled"] = "true",
            ["PgWorker__Backups__S3__Endpoint"] = Fx.S3Endpoint,
            ["PgWorker__Backups__S3__Bucket"] = Bucket,
            ["PgWorker__Backups__S3__AccessKey"] = "minioadmin",
            ["PgWorker__Backups__S3__SecretKey"] = "minioadmin",
            ["PgWorker__Backups__Job__Image"] = E2eEnvironment.JobImage,
            ["PgWorker__Backups__Wal__AgentImage"] = E2eEnvironment.WalImage,
            ["PgWorker__Backups__Policy__VerifyOnCreate"] = "false",
            ["PgWorker__Backups__Supervisor__IntervalSec"] = "60",
            ["PgWorker__Backups__Supervisor__OrphanTtlSec"] = "120",
        }, ct: ct);

    private async Task<IReadOnlyList<Kv>> FullKeysAsync(string cluster, string shard)
        => (await G.RangeAsync(Endpoint, $"/pgworker/backups/{cluster}/{shard}/full/",
            TestContext.Current.CancellationToken)).Value;

    private async Task<Kv?> GetOrNullAsync(string key)
        => (await G.GetAsync(Endpoint, key, TestContext.Current.CancellationToken)).Value;

    private Task<string> RunDockerAsync(string[] args)
        => Fx.RunDockerAsync(args, TestContext.Current.CancellationToken);

    // mc rm одного объекта (mc в сети MinIO окружения).
    private Task McRmAsync(string key)
        => Fx.RunDockerAsync(
        [
            "run", "--rm", "--network", Fx.NetName, "--entrypoint", "/bin/sh", E2eEnvironment.McImage,
            "-c", $"mc alias set t http://e2e-minio:9000 minioadmin minioadmin >/dev/null"
                  + $" && mc rm t/{Bucket}/{key}",
        ], TestContext.Current.CancellationToken);

    // mc ls префикса → список имён объектов (после удаления — пусто).
    private async Task<List<string>> McLsAsync(string prefix)
    {
        var output = await Fx.RunDockerAsync(
        [
            "run", "--rm", "--network", Fx.NetName, "--entrypoint", "/bin/sh", E2eEnvironment.McImage,
            "-c", $"mc alias set t http://e2e-minio:9000 minioadmin minioadmin >/dev/null"
                  + $" && mc ls --recursive t/{Bucket}/{prefix} || true",
        ], TestContext.Current.CancellationToken);
        return output
            .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(l => l.Length > 0)
            .ToList();
    }

    // mc-посев объекта с фиксированным телом (образец E2eRetentionScenarios).
    private Task McCpAsync(string key, string content)
        => Fx.RunDockerAsync(
        [
            "run", "--rm", "--network", Fx.NetName, "--entrypoint", "/bin/sh", E2eEnvironment.McImage,
            "-c", $"mc alias set t http://e2e-minio:9000 minioadmin minioadmin >/dev/null"
                  + $" && echo {content} >/tmp/f && mc cp /tmp/f t/{Bucket}/{key}",
        ], TestContext.Current.CancellationToken);

    // Сид кластера (копия E2eRotateScenarios.SeedClusterAsync).
    private async Task SeedClusterAsync(string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        var config = $$"""
            {"buckets":2,"dbname":"{{cluster}}","created_unix":1755800000,"state":"NOT_INITIALIZED","bucket_admin_password":"{{E2eFixture.BucketAdminPassword}}"}
            """;
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/config", config, null, ct);
        foreach (var shard in new[] { "shard1", "shard2" })
        {
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/replicas", "2", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}a/state", "NOT_INITIALIZED", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}b/state", "NOT_INITIALIZED", null, ct);
            await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_cpu", "2", null, ct);
            await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_mem", "8Gi", null, ct);
        }

        for (var i = 0; i < 2; i++)
        {
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/routing/bucket_{i}", $"shard{i + 1}", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/status/bucket_{i}",
                """{"state":"NOT_INITIALIZED"}""", null, ct);
        }
    }

    // Published pg-порт мастера шарда из portalloc (копия MasterPgAsync —
    // резолв фактического primary по пробам Patroni /primary).
    private static readonly HttpClient PatroniHttp = new() { Timeout = TimeSpan.FromSeconds(3) };

    private async Task<(string Host, int Port)> MasterPgAsync(string cluster, string shard, CancellationToken ct)
    {
        var kv = await G.GetAsync(Endpoint, $"/pgworker/portalloc/{cluster}", ct);
        kv.Value.Should().NotBeNull("portalloc пишется при provisioning");
        var entries = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(kv.Value!.Value)!;
        string? primary = null;
        var resolved = await E2eFixture.WaitForAsync(async () =>
        {
            foreach (var (key, addr) in entries
                         .Where(p => p.Key.StartsWith($"{shard}/", StringComparison.Ordinal))
                         .OrderBy(p => p.Key, StringComparer.Ordinal))
            {
                try
                {
                    using var response = await PatroniHttp.GetAsync(
                        $"http://localhost:{addr.GetProperty("patroni").GetInt32()}/primary", ct);
                    if (response.IsSuccessStatusCode)
                    {
                        primary = key.Split('/')[1];
                        return true;
                    }
                }
                catch (Exception)
                {
                    // проба (рестарт/ещё не готова) — следующая нода
                }
            }

            return false;
        }, TimeSpan.FromSeconds(120), ct);
        resolved.Should().BeTrue("primary шарда обязан определиться пробами Patroni");
        var entry = entries[$"{shard}/{primary}"];
        return (entry.GetProperty("host").GetString()!, entry.GetProperty("pg").GetInt32());
    }

    // Закрытие count сегментов WAL. ВАЖНО (инцидент прогона 2026-09-13):
    // голый pg_switch_wal на пустой базе — NO-OP (PostgreSQL не создаёт пустые
    // сегменты: «has no effect if there has been no WAL traffic since the last
    // WAL switch»), 22 вызова дали ~5 сегментов, кончик цепи замер, гейт
    // глубины цепи честно истёк. Поэтому перед каждым переключением пишем
    // РЕАЛЬНЫЙ WAL — pg_logical_emit_message (wal_level=logical в кластере,
    // таблиц не требует): ~17 МБ на сегмент гарантированно закрывает его.
    private static async Task SwitchWalsAsync(string adminDsn, int count, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(adminDsn);
        await conn.OpenAsync(ct);
        for (var i = 0; i < count; i++)
        {
            for (var j = 0; j < 17; j++)
            {
                await using var message = new NpgsqlCommand(
                    "SELECT pg_logical_emit_message(false, 'e2e', repeat('w', 1048576))", conn);
                await message.ExecuteScalarAsync(ct);
            }
            await using var switchWal = new NpgsqlCommand("SELECT pg_switch_wal()", conn);
            await switchWal.ExecuteScalarAsync(ct);
        }
    }

    // Имя сегмента (24 hex) → LSN «X/Y» начала сегмента (для pg_wal_lsn_diff).
    private static string LsnOf(string name)
    {
        var log = Convert.ToUInt32(name.Substring(8, 8), 16);
        var seg = Convert.ToUInt32(name.Substring(16, 8), 16);
        var bytes = (log * 0x100UL + seg) * 16L * 1024 * 1024;
        return $"{bytes >> 32:X}/{bytes & 0xFFFFFFFF:X}";
    }

    // Published pg-порты ОБОИХ нод шарда1 из portalloc (host опускаем — ходим
    // с хоста по localhost, как MasterPgAsync).
    private async Task<List<(string Node, int Port)>> Shard1PortsAsync(string cluster)
    {
        var kv = await G.GetAsync(Endpoint, $"/pgworker/portalloc/{cluster}",
            TestContext.Current.CancellationToken);
        kv.Value.Should().NotBeNull("portalloc пишется при provisioning");
        var entries = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(kv.Value!.Value)!;
        return entries
            .Where(p => p.Key.StartsWith("shard1/", StringComparison.Ordinal))
            .Select(p => (p.Key.Split('/')[1], p.Value.GetProperty("pg").GetInt32()))
            .ToList();
    }

    // Условие готовности provisioning: config без state + dsn + нода RUNNING.
    private async Task<bool> ProvisionedAsync(string cluster)
    {
        var config = await GetOrNullAsync($"/clusters/{cluster}/config");
        if (config is null)
            return false;
        if (JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(config.Value)!.ContainsKey("state"))
            return false;
        var dsn = await GetOrNullAsync($"/clusters/{cluster}/shards/shard1/dsn");
        var node = await GetOrNullAsync($"/clusters/{cluster}/shards/shard1/nodes/shard1a/state");
        return dsn is not null && node is { Value: "RUNNING" };
    }
}

