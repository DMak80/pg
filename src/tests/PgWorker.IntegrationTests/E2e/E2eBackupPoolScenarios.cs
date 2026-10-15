using System.Diagnostics;
using System.Text.Json;
using Npgsql;
using PgWorker.Backups;
using PgWorker.Core.Templates;
using PgWorker.Etcd.Parsing;
using PgWorker.IntegrationTests.Docker;
using PgWorker.Provisioning.Sql;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E пула S3 (t36, spec §9 AC1–AC8): инстансы пула — ownS3-ноды окружения
// (SeedS3PoolAsync), воркер с env-профилем кредов, policy с секцией s3.
// Каждый Fact — ПОЛНОСТЬЮ СВОЁ окружение (своя сеть/etcd/ownS3 — канон
// e2e-isolation §1), teardown при любом исходе + ассерт чистоты в фикстуре.
// AC1 — «пул не выбран → молчание», выбор policy → COMPLETED;
// AC2 — раскладка записи по спискам мастера/реплики (full строго на своей ноде);
// AC3 — однонодовый пул: оба потока + restore;
// AC4 — перебор списка записи (failover) и исчерпание с маркером + самозаживление;
// AC5 — promote: агент пересоздаётся со списком мастера, .history на ноде мастера;
// AC6 — чтение с любой ноды (mc-mirror эмуляция mesh + стоп ноды → restore);
// AC7 — слепки на отдельной паре (свой bucket) + fail-fast без пары;
// AC8 — валидация policy-API по полям (400) + restore из source_pool чужого пула.
public abstract class E2eBackupPoolScenarios
{
    protected const string Bucket = "pgworker-backups";
    protected const string SnapshotBucket = "pgworker-etcd-snapshots";

    // Пул E2E (etcd у Fact'а свой — имя может быть постоянным). Вторая нода —
    // alias в сети окружения (StartExtraS3NodeAsync("e2e-owns3-2")).
    protected const string Pool = "bkpool";
    protected const string Node2Alias = "e2e-owns3-2";

    protected E2eEnvironment Fx = null!;
    protected string Endpoint => Fx.EtcdEndpoint;
    protected EtcdGateway G => Fx.Gateway;

    protected static readonly HttpClient PatroniHttp = E2eEnvironment.CreatePatroniHttpsClient();

    // ===== хелперы (прецедент копирования — E2eRestoreScenarios/E2eWalStreamScenarios) =====

    protected static string PolicyKey(string cluster) => $"/pgworker/backups/{cluster}/policy";

    // mTLS policy-API воркера: тело канона, ответ (status, body) — кейсы 400 AC8.
    protected static async Task<(System.Net.HttpStatusCode Code, string Body)> PutPolicyApiAsync(
        HttpClient api, HostInstance app, string cluster, string body, CancellationToken ct)
    {
        using var content = new StringContent(body, System.Text.Encoding.UTF8, "application/json");
        using var response = await api.PostAsync(
            $"https://127.0.0.1:{app.ApiPort}/api/clusters/{cluster}/backups/policy",
            content, ct);
        return (response.StatusCode, await response.Content.ReadAsStringAsync(ct));
    }

    protected static string HostPort(string endpoint)
        => endpoint.Replace("http://", "", StringComparison.Ordinal).TrimEnd('/');

    // Воркер с включённой подсистемой: пул — policy.s3 (etcd), bucket и env-профиль
    // кредов — env (t36); образы e2e, ускоренный бэкофф, короткие пороги WAL.
    protected Task<HostInstance> StartPoolHostAsync(string name, CancellationToken ct)
        => Fx.StartHostAsync(name, extraEnv: new Dictionary<string, string>
        {
            ["PgWorker__Backups__Enabled"] = "true",
            ["PgWorker__Backups__S3__Bucket"] = Bucket,
            [$"PGW_BACKUP_S3_PROFILES__{E2eEnvironment.PoolProfile}__ACCESS_KEY"] = E2eEnvironment.S3WriterKey,
            [$"PGW_BACKUP_S3_PROFILES__{E2eEnvironment.PoolProfile}__SECRET_KEY"] = E2eEnvironment.S3WriterSecret,
            ["PgWorker__Backups__Job__Image"] = E2eEnvironment.JobImage,
            ["PgWorker__Backups__Wal__AgentImage"] = E2eEnvironment.WalImage,
            ["PgWorker__Backups__Retry__BaseSec"] = "2",
            ["PgWorker__Backups__Retry__MaxSec"] = "4",
            ["PgWorker__Backups__Wal__VerifyIntervalSec"] = "2",
            ["PgWorker__Backups__Wal__StaleSec"] = "600",
            ["PgWorker__Backups__Wal__LagMaxSegments"] = "100000",
            ["PgWorker__Backups__Restore__RecoveryTimeoutSec"] = "600",
            ["PgWorker__Thresholds__PatroniBootSec"] = "120",
        }, ct: ct);

    protected async Task SeedClusterAsync(string cluster)
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
            await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_mem", "2Gi", null, ct);
        }

        for (var i = 0; i < 2; i++)
        {
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/routing/bucket_{i}", $"shard{i + 1}", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/status/bucket_{i}",
                """{"state":"NOT_INITIALIZED"}""", null, ct);
        }
    }

    // Policy кластера со списками записи (формат spec §5).
    protected async Task PutPolicyAsync(string cluster, IReadOnlyList<string> master, IReadOnlyList<string> replica)
    {
        var ct = TestContext.Current.CancellationToken;
        await G.PutAsync(Endpoint, PolicyKey(cluster),
            $"{{\"full_max_age_sec\":3600,\"s3\":{{\"pool\":\"{Pool}\","
            + $"\"write_master\":[{string.Join(",", master.Select(n => $"\"{n}\""))}],"
            + $"\"write_replica\":[{string.Join(",", replica.Select(n => $"\"{n}\""))}]}}}}",
            null, ct);
    }

    // Инспекция ноды пула по её endpoint: mc ls --recursive bucket/prefix.
    protected Task<string> McLsOnNodeAsync(string hostPort, string prefix)
        => Fx.RunMcAsync(
            $"mc alias set t http://{hostPort} {E2eEnvironment.S3RootUser} {E2eEnvironment.S3RootPassword} >/dev/null"
            + $" && mc ls --recursive t/{Bucket}/{prefix} || true",
            TestContext.Current.CancellationToken);

    // Имена объектов из листинга ноды (последний токен строки — путь).
    protected async Task<IReadOnlyList<string>> McKeysOnNodeAsync(string hostPort, string prefix)
    {
        var listing = await McLsOnNodeAsync(hostPort, prefix);
        return listing.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(l => l[(l.LastIndexOf(' ') + 1)..])
            .ToList();
    }

    protected async Task<IReadOnlyList<Kv>> FullKeysAsync(string cluster, string shard)
        => (await G.RangeAsync(Endpoint, $"/pgworker/backups/{cluster}/{shard}/full/",
            TestContext.Current.CancellationToken)).Value;

    protected async Task<Kv?> GetOrNullAsync(string key)
        => (await G.GetAsync(Endpoint, key, TestContext.Current.CancellationToken)).Value;

    // Первый COMPLETED-полный шарда (ключ статуса).
    protected async Task<Kv> WaitCompletedFullAsync(string cluster, CancellationToken ct)
    {
        Kv? done = null;
        var completed = await E2eFixture.WaitForAsync(async () =>
        {
            done = (await FullKeysAsync(cluster, "shard1")).FirstOrDefault(f => f.Value.Contains("COMPLETED"));
            return done is not null;
        }, TimeSpan.FromSeconds(300), ct);
        completed.Should().BeTrue("полный обязан дойти до COMPLETED");
        return done!;
    }

    // Факт доставки: в wal/ ноды пула есть ≥1 сегмент (running-агентов мало —
    // под параллельной нагрузкой uploads запаздывают за стартом контейнеров).
    protected async Task<bool> WaitWalUploadedAsync(string cluster, string hostPort, CancellationToken ct)
        => await E2eFixture.WaitForAsync(async () =>
        {
            var keys = await McKeysOnNodeAsync(hostPort, $"{cluster}/shard1/wal/");
            return keys.Count > 0;
        }, TimeSpan.FromSeconds(480), ct);

    protected async Task<bool> WaitBothWalAgentsAsync(string cluster, CancellationToken ct)
        => await E2eFixture.WaitForAsync(async () =>
        {
            var ps = await Fx.RunDockerAsync(
            [
                "ps", "--filter", $"name=pgw-backup-wal-{cluster}-shard1-",
                "--format", "{{.Names}} {{.State}}",
            ], ct);
            return ps.Split('\n', StringSplitOptions.RemoveEmptyEntries).Count(l => l.EndsWith("running")) >= 2;
        }, TimeSpan.FromSeconds(480), ct);

    protected async Task<bool> WaitProvisionedAsync(string cluster, CancellationToken ct)
        => await E2eFixture.WaitForAsync(async () =>
        {
            var config = await GetOrNullAsync($"/clusters/{cluster}/config");
            if (config is null
                || JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(config.Value)!
                    .ContainsKey("state"))
                return false;
            var dsn = await GetOrNullAsync($"/clusters/{cluster}/shards/shard1/dsn");
            var node = await GetOrNullAsync($"/clusters/{cluster}/shards/shard1/nodes/shard1a/state");
            return dsn is not null && node is { Value: "RUNNING" };
        }, TimeSpan.FromSeconds(360), ct);

    protected static async Task ExecAsync(string adminDsn, string sql, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(adminDsn);
        await conn.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    // WAL-нагрузка под admin/superuser (образец E2eWalStreamPromoteScenarios).
    protected static async Task GenerateWalAsync(string adminDsn, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(adminDsn);
        await conn.OpenAsync(ct);
        await using (var create = new NpgsqlCommand(
            "CREATE TABLE IF NOT EXISTS wal_load(id bigserial, payload text)", conn))
            await create.ExecuteNonQueryAsync(ct);
        for (var i = 0; i < 30; i++)
        {
            await using var insert = new NpgsqlCommand(
                "INSERT INTO wal_load(payload) SELECT repeat('x', 1048576) FROM generate_series(1, 8)", conn);
            await insert.ExecuteNonQueryAsync(ct);
            await using var switchWal = new NpgsqlCommand("SELECT pg_switch_wal()", conn);
            await switchWal.ExecuteScalarAsync(ct);
        }
    }

    protected static async Task BackgroundWalLoadAsync(string adminDsn, CancellationToken ct)
    {
        try
        {
            await using var conn = new NpgsqlConnection(adminDsn);
            await conn.OpenAsync(ct);
            await using (var create = new NpgsqlCommand(
                "CREATE TABLE IF NOT EXISTS wal_load(id bigserial, payload text)", conn))
                await create.ExecuteNonQueryAsync(ct);
            while (!ct.IsCancellationRequested)
            {
                await using var insert = new NpgsqlCommand(
                    "INSERT INTO wal_load(payload) SELECT repeat('x', 1048576) FROM generate_series(1, 4)", conn);
                await insert.ExecuteNonQueryAsync(ct);
                await using var switchWal = new NpgsqlCommand("SELECT pg_switch_wal()", conn);
                await switchWal.ExecuteScalarAsync(ct);
                await Task.Delay(15000, ct);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // штатная остановка нагрузки
        }
        catch (Exception) when (ct.IsCancellationRequested)
        {
            // соединение могло умереть вместе со сценарием — не маскируем отмену
        }
    }

    protected static async Task SwitchWalsAsync(string adminDsn, int count, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(adminDsn);
        await conn.OpenAsync(ct);
        for (var i = 0; i < count; i++)
        {
            await using var switchWal = new NpgsqlCommand("SELECT pg_switch_wal()", conn);
            await switchWal.ExecuteScalarAsync(ct);
        }
    }

    // Published pg-порт мастера шарда из portalloc; резолв primary — пробы
    // Patroni /primary (копия E2eWalStreamPromoteScenarios.MasterPgAsync).
    protected async Task<(string Host, int Port, string Node)> MasterPgAsync(
        string cluster, string shard, CancellationToken ct)
    {
        var kv = await G.GetAsync(Endpoint, $"/pgworker/portalloc/{cluster}", ct);
        kv.Value.Should().NotBeNull("portalloc пишется при provisioning");
        var entries = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(kv.Value!.Value)!;
        string? primary = null;
        // Порт Patroni — ФАКТ docker-биндинга (docker port), не portalloc:
        // portalloc-план может расходиться с фактическими публикациями нод
        // (прогон 2026-10-15: portalloc.patroni=15117, факт=15115 — пробы били
        // в чужой сервис и ловили SSL-error, primary «не находился»).
        var probeAttempt = 0;
        var probeSummary = "проб не было";
        var resolved = await E2eFixture.WaitForAsync(async () =>
        {
            probeAttempt++;
            var lines = new List<string>();
            foreach (var (key, addr) in entries
                         .Where(p => p.Key.StartsWith($"{shard}/", StringComparison.Ordinal))
                         .OrderBy(p => p.Key, StringComparer.Ordinal))
            {
                var node = key.Split('/')[1];
                var container = $"pgw-{cluster}-shard1-{node}";
                int port;
                try
                {
                    var portOut = await Fx.RunDockerAsync(["port", container, "8008"], ct);
                    var m = System.Text.RegularExpressions.Regex.Match(
                        portOut, @":(\d+)$", System.Text.RegularExpressions.RegexOptions.Multiline);
                    if (!m.Success)
                    {
                        lines.Add($"{node}:нет-публикации");
                        continue; // нода ещё без публикации — следующий такт
                    }
                    port = int.Parse(m.Groups[1].Value);
                }
                catch (Exception e)
                {
                    lines.Add($"{node}:{e.Message.Split('\n')[0]}");
                    continue; // контейнер ещё не создан — следующий такт
                }

                try
                {
                    using var response = await PatroniHttp.GetAsync(
                        $"https://localhost:{port}/primary", ct);
                    if (response.IsSuccessStatusCode)
                    {
                        primary = node;
                        return true;
                    }
                    lines.Add($"{node}:{port}→{(int)response.StatusCode}");
                }
                catch (Exception e)
                {
                    lines.Add($"{node}:{port}→{e.GetType().Name}:{e.Message.Split('\n')[0]}");
                }
            }

            probeSummary = $"attempt {probeAttempt}: [{string.Join("; ", lines)}]";
            return false;
        }, TimeSpan.FromSeconds(240), ct);
        resolved.Should().BeTrue($"primary шарда обязан определиться пробами Patroni (docker port 8008); {probeSummary}");
        var entry = entries[$"{shard}/{primary}"];
        return (entry.GetProperty("host").GetString()!, entry.GetProperty("pg").GetInt32(), primary!);
    }

    // PUT PLANNED-заявки restore прямым ключом (прецедент E2eRestoreScenarios).
    protected async Task<string> PutRestorePlannedAsync(
        string cluster, string shard, string target, string? source = null,
        CancellationToken ct = default)
    {
        var id = $"{DateTimeOffset.UtcNow:yyyyMMddHHmmss}Z-{shard}";
        var op = new RestoreOperationState(id, RestoreStatus.Planned, "",
            source ?? $"{cluster}/{shard}", target, $"{shard}a",
            DateTimeOffset.UtcNow.ToUnixTimeSeconds(), "e2e");
        await G.PutAsync(Endpoint, PgWorker.Backups.BackupNames.RestoreKey(cluster, shard, id),
            PgWorker.Backups.Restore.RestoreStatusJson.Serialize(op), null, ct);
        return id;
    }

    // Поллинг restore-ключа до терминального статуса (упрощённый образец).
    protected async Task<Dictionary<string, JsonElement>> WaitRestoreAsync(
        string cluster, string shard, string id, TimeSpan budget, CancellationToken ct)
    {
        Dictionary<string, JsonElement>? status = null;
        var done = await E2eFixture.WaitForAsync(async () =>
        {
            var raw = await GetOrNullAsync(PgWorker.Backups.BackupNames.RestoreKey(cluster, shard, id));
            if (raw is null)
                return false;
            status = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(raw.Value);
            return status is not null && (status["state"].GetString() is "COMPLETED" or "FAILED");
        }, budget, ct);
        if (!done || status!["state"].GetString() == "FAILED")
            throw new ApplicationException(
                $"restore {cluster}/{shard}/{id} не дошёл до COMPLETED: "
                + (status is null ? "ключа нет" : status["state"].GetString())
                + (status is { } s && s.TryGetValue("error", out var err) ? $", error={err.GetString()}" : ""));
        return status!;
    }

    protected async Task<Dictionary<string, JsonElement>> RestoreLatestAsync(
        string cluster, string shard, CancellationToken ct)
    {
        var id = await PutRestorePlannedAsync(cluster, shard, "latest", ct: ct);
        return await WaitRestoreAsync(cluster, shard, id, TimeSpan.FromSeconds(420), ct);
    }
}

// AC1 (молчание без выбора пула → policy с s3 → COMPLETED (AC1)) — отдельный класс: свой guid-контур; коллекции классов
// не сериализуют сценарии между собой (параллельный запуск E2E, ≤5 живых
// контуров — канон docs/e2e-launch.md).
public sealed class PoolNotSelectedScenarios : E2eBackupPoolScenarios
{
    // ===== AC1 =====

    // AAA: policy без s3 → полный не планируется (ключей нет за окно);.policy
    // с секцией s3 → полный COMPLETED, объекты full/ на ноде пула (AC1).
    [Fact]
    public async Task Pool_NotSelected_Silent_ThenPolicy_Completes()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("pool-ac1", withS3Node: true, ct: ct);
        Fx = fx;
        var cluster = $"pac1{Fx.ClusterTag}";
        var ep1 = HostPort(Fx.S3Endpoint);
        try
        {
            // Arrange — пул сидится, policy БЕЗ секции s3; воркер живёт.
            await SeedClusterAsync(cluster);
            await Fx.SeedS3PoolAsync(Pool, ct);
            await G.PutAsync(Endpoint, PolicyKey(cluster),
                """{"full_max_age_sec":3600}""", null, ct);
            await using var app = await StartPoolHostAsync("pac1", ct);
            var provisioned = await WaitProvisionedAsync(cluster, ct);
            provisioned.Should().BeTrue("provisioning до окна наблюдения");

            // Act/Assert 1 — «пул не выбран» = молчание: full-ключей нет за 45 c.
            await Task.Delay(TimeSpan.FromSeconds(45), ct);
            (await FullKeysAsync(cluster, "shard1")).Should().BeEmpty(
                "policy без s3 — подсистема кластера не пишет (arch/19 §4)");

            // Act 2 — policy с секцией s3 (применяется следующим тиком).
            await G.PutAsync(Endpoint, PolicyKey(cluster),
                $"{{\"full_max_age_sec\":3600,{E2eEnvironment.PoolS3(Pool)}}}", null, ct);

            // Assert 2 — COMPLETED ≤ 300 c, объекты на ноде пула.
            var done = await WaitCompletedFullAsync(cluster, ct);
            var id = done.Key.Split('/').Last();
            var listing = await McLsOnNodeAsync(ep1, $"{cluster}/shard1/");
            listing.Should().Contain($"full/{id}/backup_manifest",
                "объекты полного — на ноде пула (инспекция по endpoint ноды)");
        }
        catch
        {
            Fx.MarkFailed();
            throw;
        }
    }
}

// AC2 (раскладка записи мастер/реплика по своим нодам (AC2)) — отдельный класс: свой guid-контур; коллекции классов
// не сериализуют сценарии между собой (параллельный запуск E2E, ≤5 живых
// контуров — канон docs/e2e-launch.md).
public sealed class PoolMasterReplicaSplitScenarios : E2eBackupPoolScenarios
{
    // ===== AC2 =====

    // AAA: пул из двух нод, write_master=[node1], write_replica=[node2]:
    // каждый агент доставил сегменты на СВОЮ ноду; полный с реплики (role=replica)
    // — объекты full/<id>/ СТРОГО на node2 (на node1 их нет).
    [Fact]
    public async Task Pool_MasterReplicaSplit_ObjectsOnTheirNodes()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("pool-ac2", withS3Node: true, ct: ct);
        Fx = fx;
        var cluster = $"pac2{Fx.ClusterTag}";
        var ep1 = HostPort(Fx.S3Endpoint);
        try
        {
            // Arrange — вторая нода пула + двухнодовые списки.
            var ep2 = HostPort(await Fx.StartExtraS3NodeAsync(Node2Alias, ct));
            await Fx.SeedS3PoolAsync(Pool, ct, [ep2]);
            await SeedClusterAsync(cluster);
            await PutPolicyAsync(cluster, ["node1"], ["node2"]);
            await using var app = await StartPoolHostAsync("pac2", ct);
            var provisioned = await WaitProvisionedAsync(cluster, ct);
            provisioned.Should().BeTrue("provisioning до wal-фазы");

            // WAL-нагрузка ПЕРВАЯ (Patroni выбирает sync при живом потоке —
            // прецедент E2eWalStreamScenarios), оба агента поднимаются.
            var (host, port, _) = await MasterPgAsync(cluster, "shard1", ct);
            var adminDsn = DatabaseProvisioner.BuildAdminDsn("localhost", port, cluster,
                new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
            using var load = new CancellationTokenSource();
            var loader = BackgroundWalLoadAsync(adminDsn, load.Token);
            var bothAgents = await WaitBothWalAgentsAsync(cluster, ct);
            bothAgents.Should().BeTrue("master- и sync-агенты живы (sync выбран)");
            await GenerateWalAsync(adminDsn, ct);
            (await WaitWalUploadedAsync(cluster, ep1, ct)).Should().BeTrue(
                "мастер-агент доставил сегменты на node1");

            // Act/Assert — полный с реплики: role=replica, объекты на node2,
            // на node1 префикса full/ нет.
            var done = await WaitCompletedFullAsync(cluster, ct);
            var status = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(done.Value)!;
            status["role"].GetString().Should().Be("replica", "sync-реплика выбрана — источник полного реплика");
            var id = done.Key.Split('/').Last();
            var onNode2 = await McLsOnNodeAsync(ep2, $"{cluster}/shard1/");
            onNode2.Should().Contain($"full/{id}/backup_manifest",
                "write_replica=[node2] — полный доставлен на вторую ноду");
            var keysOnNode1 = await McKeysOnNodeAsync(ep1, $"{cluster}/shard1/");
            keysOnNode1.Should().NotContain(k => k.StartsWith($"full/{id}/"),
                "write_master не участвует в записи репликового полного");
            // Каждый агент доставил сегменты на свою ноду (list по endpoint каждой).
            keysOnNode1.Should().Contain(k => k.StartsWith("wal/"), "master-агент пишет на node1");
            (await McKeysOnNodeAsync(ep2, $"{cluster}/shard1/"))
                .Should().Contain(k => k.StartsWith("wal/"), "sync-агент пишет на node2");
            await load.CancelAsync();
            try { await loader; } catch { /* фон остановлен */ }
        }
        catch
        {
            Fx.MarkFailed();
            throw;
        }
    }
}

// AC3 (однонодовый пул: оба потока + restore (AC3)) — отдельный класс: свой guid-контур; коллекции классов
// не сериализуют сценарии между собой (параллельный запуск E2E, ≤5 живых
// контуров — канон docs/e2e-launch.md).
public sealed class PoolSingleNodeScenarios : E2eBackupPoolScenarios
{
    // ===== AC3 =====

    // AAA: однонодовый пул — оба агента пишут на node1, полный COMPLETED,
    // restore latest COMPLETED (перебор чтения по единственной ноде).
    [Fact]
    public async Task Pool_SingleNode_BothStreams_RestoreOk()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("pool-ac3", withS3Node: true, ct: ct);
        Fx = fx;
        var cluster = $"pac3{Fx.ClusterTag}";
        try
        {
            await SeedClusterAsync(cluster);
            await Fx.SeedS3PoolAsync(Pool, ct);
            await PutPolicyAsync(cluster, ["node1"], ["node1"]);
            await using var app = await StartPoolHostAsync("pac3", ct);
            (await WaitProvisionedAsync(cluster, ct)).Should().BeTrue();

            var (host, port, _) = await MasterPgAsync(cluster, "shard1", ct);
            var adminDsn = DatabaseProvisioner.BuildAdminDsn("localhost", port, cluster,
                new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
            await GenerateWalAsync(adminDsn, ct);
            using var load = new CancellationTokenSource();
            var loader = BackgroundWalLoadAsync(adminDsn, load.Token);
            (await WaitBothWalAgentsAsync(cluster, ct)).Should().BeTrue("оба агента подняты");
            var ep1 = HostPort(Fx.S3Endpoint);
            (await WaitWalUploadedAsync(cluster, ep1, ct)).Should().BeTrue(
                "оба потока доставляют сегменты на единственную ноду");
            (await WaitCompletedFullAsync(cluster, ct)).Should().NotBeNull();

            // Act/Assert — restore latest из пула: COMPLETED.
            var restore = await RestoreLatestAsync(cluster, "shard1", ct);
            restore["state"].GetString().Should().Be("COMPLETED", "restore из однонодового пула");
            await load.CancelAsync();
            try { await loader; } catch { /* фон остановлен */ }
        }
        catch
        {
            Fx.MarkFailed();
            throw;
        }
    }
}

// AC4 (failover списка записи и исчерпание с маркером (AC4)) — отдельный класс: свой guid-контур; коллекции классов
// не сериализуют сценарии между собой (параллельный запуск E2E, ≤5 живых
// контуров — канон docs/e2e-launch.md).
public sealed class PoolFailoverExhaustionScenarios : E2eBackupPoolScenarios
{
    // ===== AC4 =====

    // AAA: список записи [node1,node2] — стоп node1 → мастер-агент доезжает до
    // node2 (стрим жив, сегменты едут на node2); список [node1] — стоп node1 →
    // wal DEGRADED с маркером s3-pool-exhausted; подъём node1 → ACTIVE (AC4).
    [Fact]
    public async Task Pool_FailoverList_And_Exhaustion_SelfHeal()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("pool-ac4", withS3Node: true, ct: ct);
        Fx = fx;
        var cluster = $"pac4{Fx.ClusterTag}";
        var writer = new WalStatusWriter(G, [Endpoint]);
        try
        {
            // Arrange — двухнодовый пул, список записи с перебором; поток жив.
            var ep2 = HostPort(await Fx.StartExtraS3NodeAsync(Node2Alias, ct));
            await Fx.SeedS3PoolAsync(Pool, ct, [ep2]);
            await SeedClusterAsync(cluster);
            await PutPolicyAsync(cluster, ["node1", "node2"], ["node2", "node1"]);
            var app = await StartPoolHostAsync("pac4", ct);
            (await WaitProvisionedAsync(cluster, ct)).Should().BeTrue();
            var (_, port, _) = await MasterPgAsync(cluster, "shard1", ct);
            var adminDsn = DatabaseProvisioner.BuildAdminDsn("localhost", port, cluster,
                new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
            using var load = new CancellationTokenSource();
            var loader = BackgroundWalLoadAsync(adminDsn, load.Token);
            (await WaitBothWalAgentsAsync(cluster, ct)).Should().BeTrue("агенты до отказа ноды");
            var ep1 = HostPort(Fx.S3Endpoint);
            (await WaitWalUploadedAsync(cluster, ep1, ct)).Should().BeTrue(
                "сегменты доставлены на node1 до инъекции отказа");

            // Act A — стоп node1: мастер-агент доезжает до node2.
            await Fx.RunDockerAsync(["stop", Fx.S3NodeContainerName], ct);
            var survived = await E2eFixture.WaitForAsync(async () =>
            {
                var read = await writer.ReadAsync(cluster, "shard1", ct);
                if (!read.IsSuccess || read.Value is null)
                    return false;
                if (read.Value.State != WalStreamStatus.Active)
                    return false;
                // Свежие сегменты обязаны появиться на ВЫЖИВШЕЙ ноде (node2).
                var onNode2 = await McKeysOnNodeAsync(ep2, $"{cluster}/shard1/wal/");
                return onNode2.Count > 0;
            }, TimeSpan.FromSeconds(300), ct);
            survived.Should().BeTrue(
                "стрим продолжился на node2 (перебор списка записи, RPO: слот подтверждён после put)");
            await Fx.RunDockerAsync(["start", Fx.S3NodeContainerName], ct);

            // Act B — список [node1]: рестарт воркера (агенты пересоздаются с новым
            // env — детерминизм без надежды на recreate по смене policy на лету),
            // стоп node1 → исчерпание: DEGRADED + маркер; подъём → ACTIVE.
            await app.DisposeAsync();
            await PutPolicyAsync(cluster, ["node1"], ["node2"]);
            var app2 = await StartPoolHostAsync("pac4b", ct);
            await using var app2Owner = app2;
            (await WaitBothWalAgentsAsync(cluster, ct)).Should().BeTrue("агенты с новым env подняты");
            await Fx.RunDockerAsync(["stop", Fx.S3NodeContainerName], ct);
            var degraded = await E2eFixture.WaitForAsync(async () =>
            {
                var read = await writer.ReadAsync(cluster, "shard1", ct);
                return read is { IsSuccess: true } && read.Value is { State: WalStreamStatus.Degraded }
                    && read.Value.Error?.Contains("s3-pool-exhausted", StringComparison.Ordinal) == true;
            }, TimeSpan.FromSeconds(300), ct);
            degraded.Should().BeTrue("исчерпание списка — DEGRADED с маркером s3-pool-exhausted");
            await Fx.RunDockerAsync(["start", Fx.S3NodeContainerName], ct);
            var healed = await E2eFixture.WaitForAsync(async () =>
            {
                var read = await writer.ReadAsync(cluster, "shard1", ct);
                return read is { IsSuccess: true } && read.Value is { State: WalStreamStatus.Active };
            }, TimeSpan.FromSeconds(300), ct);
            healed.Should().BeTrue("подъём ноды — самозаживление в ACTIVE (ретрай-цикл)");
            await load.CancelAsync();
            try { await loader; } catch { /* фон остановлен */ }
        }
        catch
        {
            Fx.MarkFailed();
            throw;
        }
    }
}

// AC5 (promote: агент пересоздан со списком мастера (AC5)) — отдельный класс: свой guid-контур; коллекции классов
// не сериализуют сценарии между собой (параллельный запуск E2E, ≤5 живых
// контуров — канон docs/e2e-launch.md).
public sealed class PoolRoleSwitchScenarios : E2eBackupPoolScenarios
{
    // ===== AC5 =====

    // AAA: promote слейва (stop мастера → Patroni failover) → агент новой роли
    // пересоздаётся со списком МАСТЕРА; .history нового TLI и свежие сегменты —
    // на ноде master-списка; цепочка склеена, ключ ACTIVE (AC5).
    [Fact]
    public async Task Pool_RoleSwitch_AgentRecreatedWithMasterList()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("pool-ac5", withS3Node: true, ct: ct);
        Fx = fx;
        var cluster = $"pac5{Fx.ClusterTag}";
        var writer = new WalStatusWriter(G, [Endpoint]);
        var ep1 = HostPort(Fx.S3Endpoint);
        try
        {
            var ep2 = HostPort(await Fx.StartExtraS3NodeAsync(Node2Alias, ct));
            await Fx.SeedS3PoolAsync(Pool, ct, [ep2]);
            await SeedClusterAsync(cluster);
            await PutPolicyAsync(cluster, ["node1"], ["node2"]);
            await using var app = await StartPoolHostAsync("pac5", ct);
            (await WaitProvisionedAsync(cluster, ct)).Should().BeTrue();
            var (_, port, masterNode) = await MasterPgAsync(cluster, "shard1", ct);
            var adminDsn = DatabaseProvisioner.BuildAdminDsn("localhost", port, cluster,
                new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
            using var load = new CancellationTokenSource();
            var loader = BackgroundWalLoadAsync(adminDsn, load.Token);
            (await WaitBothWalAgentsAsync(cluster, ct)).Should().BeTrue("оба агента до promote");
            await GenerateWalAsync(adminDsn, ct); // база цепочки TLI 1
            (await WaitWalUploadedAsync(cluster, ep1, ct)).Should().BeTrue(
                "сегменты TLI 1 доставлены до promote");

            // Act — потеря мастера → promote реплики (механика .history —
            // образец E2eWalStreamPromoteScenarios).
            await Fx.RunDockerAsync(["stop", $"pgw-{cluster}-shard1-{masterNode}"], ct);
            var (_, newMasterPort, _) = await MasterPgAsync(cluster, "shard1", ct);
            var newMasterDsn = DatabaseProvisioner.BuildAdminDsn("localhost", newMasterPort, cluster,
                new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
            await SwitchWalsAsync(newMasterDsn, 2, ct);

            // Assert — новый мастер пишет на ноду master-списка: .history и/или
            // свежие сегменты TLI 2 появляются на node1; цепочка ACTIVE непрерывна.
            var switched = await E2eFixture.WaitForAsync(async () =>
            {
                var onNode1 = await McKeysOnNodeAsync(ep1, $"{cluster}/shard1/wal/");
                if (!onNode1.Any(k => k.EndsWith(".history", StringComparison.Ordinal)))
                    return false;
                var read = await writer.ReadAsync(cluster, "shard1", ct);
                if (read is not { IsSuccess: true } || read.Value is not { State: WalStreamStatus.Active })
                    return false;
                var chainStart = WalFileName.TryParse(read.Value.ChainStartSegment);
                if (chainStart is null)
                    return false;
                var listed = await Fx.RunMcAsync(
                    $"mc alias set t http://{ep1} {E2eEnvironment.S3RootUser} {E2eEnvironment.S3RootPassword} >/dev/null"
                    + $" && mc ls --recursive t/{Bucket}/{cluster}/shard1/wal/ || true");
                var names = listed.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                    .Select(l => l[(l.LastIndexOf(' ') + 1)..]).ToList();
                return WalChain.CheckWithRestart(chainStart.Value, names).IsContinuous;
            }, TimeSpan.FromSeconds(300), ct);
            switched.Should().BeTrue(
                "агент новой роли пишет на ноду master-списка (node1), .history в S3, цепочка непрерывна");
            // На нode2 мастер-поток после promote не пишется: .history только на node1.
            var historyOnNode2 = await McKeysOnNodeAsync(ep2, $"{cluster}/shard1/wal/");
            historyOnNode2.Count(k => k.EndsWith(".history", StringComparison.Ordinal))
                .Should().BeLessThan(
                    (await McKeysOnNodeAsync(ep1, $"{cluster}/shard1/wal/"))
                    .Count(k => k.EndsWith(".history", StringComparison.Ordinal)),
                "новый TLI-переход обслуживает master-список (node1)");
            await load.CancelAsync();
            try { await loader; } catch { /* фон умер вместе с мастером — ожидаемо */ }
        }
        catch
        {
            Fx.MarkFailed();
            throw;
        }
    }
}

// AC6 (чтение с живой ноды после стопа (AC6)) — отдельный класс: свой guid-контур; коллекции классов
// не сериализуют сценарии между собой (параллельный запуск E2E, ≤5 живых
// контуров — канон docs/e2e-launch.md).
public sealed class PoolReadAnyNodeScenarios : E2eBackupPoolScenarios
{
    // ===== AC6 =====

    // AAA: данные писались на node1; mc mirror n1→n2 (эмуляция mesh-доставки);
    // стоп node1 → restore читает со второй ноды пула (перебор ReadAll) —
    // COMPLETED (AC6, spec §9: mirror — эмуляция отставшей реплики пула).
    [Fact]
    public async Task Pool_ReadAnyNode_RestoreAfterNodeStop()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("pool-ac6", withS3Node: true, ct: ct);
        Fx = fx;
        var cluster = $"pac6{Fx.ClusterTag}";
        try
        {
            var ep2 = HostPort(await Fx.StartExtraS3NodeAsync(Node2Alias, ct));
            await Fx.SeedS3PoolAsync(Pool, ct, [ep2]);
            await SeedClusterAsync(cluster);
            // Пишем всё на node1 (оба списка — node1).
            await PutPolicyAsync(cluster, ["node1"], ["node1"]);
            await using var app = await StartPoolHostAsync("pac6", ct);
            (await WaitProvisionedAsync(cluster, ct)).Should().BeTrue();
            var (_, port, _) = await MasterPgAsync(cluster, "shard1", ct);
            var adminDsn = DatabaseProvisioner.BuildAdminDsn("localhost", port, cluster,
                new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
            await ExecAsync(adminDsn, "CREATE TABLE pool_probe(id int, note text)", ct);
            await ExecAsync(adminDsn, "INSERT INTO pool_probe SELECT g, 'keeper' FROM generate_series(1, 5) g", ct);
            (await WaitBothWalAgentsAsync(cluster, ct)).Should().BeTrue();
            await GenerateWalAsync(adminDsn, ct);
            var done = await WaitCompletedFullAsync(cluster, ct);
            done.Should().NotBeNull();

            // Act — mirror содержимого bucket node1 → node2, затем стоп node1.
            var ep1 = HostPort(Fx.S3Endpoint);
            await Fx.RunMcAsync(
                $"mc alias set s http://{ep1} {E2eEnvironment.S3RootUser} {E2eEnvironment.S3RootPassword} >/dev/null"
                + $" && mc alias set d http://{ep2} {E2eEnvironment.S3RootUser} {E2eEnvironment.S3RootPassword} >/dev/null"
                + $" && mc mirror --overwrite s/{Bucket} d/{Bucket}", ct);
            await Fx.RunDockerAsync(["stop", Fx.S3NodeContainerName], ct);

            // Assert — restore из пула при мёртвой node1: COMPLETED (чтение c node2).
            var restore = await RestoreLatestAsync(cluster, "shard1", ct);
            restore["state"].GetString().Should().Be("COMPLETED",
                "перебор ReadAll доезжает до живой ноды пула (mesh-копия)");
            await Fx.RunDockerAsync(["start", Fx.S3NodeContainerName], ct);
        }
        catch
        {
            Fx.MarkFailed();
            throw;
        }
    }
}

// AC7 (слепки на отдельной паре + fail-fast (AC7)) — отдельный класс: свой guid-контур; коллекции классов
// не сериализуют сценарии между собой (параллельный запуск E2E, ≤5 живых
// контуров — канон docs/e2e-launch.md).
public sealed class PoolSnapshotsOwnPairScenarios : E2eBackupPoolScenarios
{
    // ===== AC7 =====

    // AAA: Enabled + полная пара → слепки уходят в СВОЙ bucket pgworker-etcd-
    // snapshots (префикс etcd/), статус-ключ OK; Enabled БЕЗ пары → воркер
    // падает стартом (fail-fast, в логе имя секции) (AC7).
    [Fact]
    public async Task Snapshots_OwnPair_BucketAndFailFast()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("pool-ac7", withS3Node: true, ct: ct);
        Fx = fx;
        try
        {
            // Arrange — bucket слепков на ownS3-ноды окружения (свой, не пуловый).
            var ep1 = HostPort(Fx.S3Endpoint);
            await Fx.RunMcAsync(
                $"mc alias set t http://{ep1} {E2eEnvironment.S3RootUser} {E2eEnvironment.S3RootPassword} >/dev/null"
                + $" && mc mb --ignore-existing t/{SnapshotBucket}", ct);

            // Act 1 — воркер со слепочной парой (localhost-вид: sink из процесса).
            var localhost = Fx.S3Endpoint.Replace(
                "host.docker.internal:", "localhost:", StringComparison.Ordinal);
            await using var app = await Fx.StartHostAsync("pac7", extraEnv: new Dictionary<string, string>
            {
                ["PgWorker__Snapshots__Export__Enabled"] = "true",
                ["PgWorker__Snapshots__Export__RetryIntervalSec"] = "5",
                ["PgWorker__Snapshots__Export__S3__Endpoint"] = localhost,
                ["PgWorker__Snapshots__Export__S3__Bucket"] = SnapshotBucket,
                ["PgWorker__Snapshots__Export__S3__AccessKey"] = E2eEnvironment.S3RootUser,
                ["PgWorker__Snapshots__Export__S3__SecretKey"] = E2eEnvironment.S3RootPassword,
            }, ct: ct);

            // Assert 1 — статус OK, пара db+meta в своём bucket (префикс etcd/).
            var ok = await E2eFixture.WaitForAsync(async () =>
                (await G.GetAsync(Endpoint, "/pgworker/etcd-snapshots", ct)).Value?.Value
                    .Contains("\"state\":\"OK\"") == true,
                TimeSpan.FromSeconds(120), ct);
            ok.Should().BeTrue("слепочная пара обслуживает экспорт");
            var listing = await Fx.RunMcAsync(
                $"mc alias set t http://{ep1} {E2eEnvironment.S3RootUser} {E2eEnvironment.S3RootPassword} >/dev/null"
                + $" && mc ls --recursive t/{SnapshotBucket}/etcd/ || true", ct);
            listing.Should().Contain(".db").And.Contain(".meta.json",
                "пара слепка — в СВОЁМ bucket с префиксом etcd/");

            // Act 2 — Enabled БЕЗ пары: процесс падает стартом (fail-fast),
            // диагностика называет секцию.
            var thrown = await Assert.ThrowsAnyAsync<ApplicationException>(() =>
                Fx.StartHostAsync("pac7nofail", extraEnv: new Dictionary<string, string>
                {
                    ["PgWorker__Snapshots__Export__Enabled"] = "true",
                }, ct: ct));
            thrown.Message.Should().Contain("Snapshots:Export",
                "fail-fast старта называет секцию (arch/14 §8)");
        }
        catch
        {
            Fx.MarkFailed();
            throw;
        }
    }
}

// AC8 (policy-API 400 по полям + restore source_pool (AC8)) — отдельный класс: свой guid-контур; коллекции классов
// не сериализуют сценарии между собой (параллельный запуск E2E, ≤5 живых
// контуров — канон docs/e2e-launch.md).
public sealed class PoolPolicyValidationScenarios : E2eBackupPoolScenarios
{
    // ===== AC8 =====

    // AAA: policy-API — 400 по полям (пул не найден / TO_REMOVE / нода не в пуле /
    // дубликат / пустой список; заявленный без portalloc инстанс — ВАЛИДЕН);
    // restore из source_pool чужого пула — COMPLETED (AC8).
    [Fact]
    public async Task PolicyValidation_400_ByField_And_SourcePool_Restore()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("pool-ac8", withS3Node: true, ct: ct);
        Fx = fx;
        var cluster = $"pac8{Fx.ClusterTag}";
        try
        {
            var ep2 = HostPort(await Fx.StartExtraS3NodeAsync(Node2Alias, ct));
            await Fx.SeedS3PoolAsync(Pool, ct, [ep2]);
            // Пул в состоянии TO_REMOVE + пул с Заявленной нодой без portalloc.
            await G.PutAsync(Endpoint, "/s3/clusters/toremove/config",
                """{"platform":"owns3","nodes":1,"secret_profile":"backups-main","created_unix":1760000000,"state":"TO_REMOVE"}""",
                null, ct);
            await G.PutAsync(Endpoint, "/s3/clusters/claiming/config",
                """{"platform":"owns3","nodes":1,"secret_profile":"backups-main","created_unix":1760000000}""",
                null, ct);
            await G.PutAsync(Endpoint, "/s3/clusters/claiming/nodes/node1/placement",
                """{"docker_host":"local"}""", null, ct);
            await SeedClusterAsync(cluster);
            await Fx.SeedS3PoolAsync(Pool, ct);
            await PutPolicyAsync(cluster, ["node1"], ["node1"]);
            await using var app = await StartPoolHostAsync("pac8", ct);
            (await WaitProvisionedAsync(cluster, ct)).Should().BeTrue();

            // Act/Assert 1 — валидация по полям (mTLS policy-API воркера).
            using var api = E2eEnvironment.CreateMtlsApiClient();
            var (code, body) = await PutPolicyApiAsync(api, app, cluster, 
                """{"full_max_age_sec":3600,"s3":{"pool":"ghost","write_master":["node1"],"write_replica":["node1"]}}""", ct);
            code.Should().Be(System.Net.HttpStatusCode.BadRequest);
            body.Should().Contain("ghost").And.Contain("s3.pool", "ошибка — по полю s3.pool");
            (code, body) = await PutPolicyApiAsync(api, app, cluster,
                """{"full_max_age_sec":3600,"s3":{"pool":"toremove","write_master":["node1"],"write_replica":["node1"]}}""", ct);
            code.Should().Be(System.Net.HttpStatusCode.BadRequest, "пул в TO_REMOVE — 400");
            (code, body) = await PutPolicyApiAsync(api, app, cluster,
                $"{{\"full_max_age_sec\":3600,\"s3\":{{\"pool\":\"{Pool}\",\"write_master\":[\"nosuch\"],\"write_replica\":[\"node1\"]}}}}", ct);
            code.Should().Be(System.Net.HttpStatusCode.BadRequest);
            body.Should().Contain("nosuch", "ошибка называет ноду вне пула");
            (code, _) = await PutPolicyApiAsync(api, app, cluster,
                $"{{\"full_max_age_sec\":3600,\"s3\":{{\"pool\":\"{Pool}\",\"write_master\":[\"node1\",\"node1\"],\"write_replica\":[\"node1\"]}}}}", ct);
            code.Should().Be(System.Net.HttpStatusCode.BadRequest, "дубликат в списке — 400");
            (code, body) = await PutPolicyApiAsync(api, app, cluster,
                $"{{\"full_max_age_sec\":3600,\"s3\":{{\"pool\":\"{Pool}\",\"write_master\":[],\"write_replica\":[\"node1\"]}}}}", ct);
            code.Should().Be(System.Net.HttpStatusCode.BadRequest);
            body.Should().Contain("s3.write_master", "пустой список — 400 по полю");
            // Инстанс заявлен (placement есть), portalloc-записи нет — policy ВАЛИДНА
            // (адрес появится при provisioning, API не отвечает 400 — spec §4.3).
            (code, _) = await PutPolicyApiAsync(api, app, cluster,
                """{"full_max_age_sec":3600,"s3":{"pool":"claiming","write_master":["node1"],"write_replica":["node1"]}}""", ct);
            ((int)code).Should().BeInRange(200, 299, $"заявленная без portalloc нода не инвалидирует policy: {body}");
            // Возврат рабочей policy (замещение целиком).
            string finalBody;
            (code, finalBody) = await PutPolicyApiAsync(api, app, cluster,
                $"{{\"full_max_age_sec\":3600,\"s3\":{{\"pool\":\"{Pool}\",\"write_master\":[\"node1\"],\"write_replica\":[\"node1\"]}}}}", ct);
            ((int)code).Should().BeInRange(200, 299, $"валидная policy принимается: {finalBody}");

            // Act 2 — DR: полный лежит в пуле «bkpool» (node1 жив); чтение DR-источника
            // задаётся source_pool. Сносим контур кластера и поднимаем заново
            // (данные только в S3 — DR), policy кластера больше нет.
            (await WaitCompletedFullAsync(cluster, ct)).Should().NotBeNull();
            // Снос контура: state=TO_REMOVE → /clusters/<C>/ и /pgworker/backups/<C>/ пусты.
            var config = await G.GetAsync(Endpoint, $"/clusters/{cluster}/config", ct);
            var doc = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(config.Value!.Value)!;
            doc["state"] = JsonSerializer.SerializeToElement("TO_REMOVE");
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/config", JsonSerializer.Serialize(doc), null, ct);
            var deprovisioned = await E2eFixture.WaitForAsync(async () =>
            {
                var left = await G.RangeAsync(Endpoint, $"/clusters/{cluster}/", ct);
                return left.IsSuccess && left.Value.Count == 0;
            }, TimeSpan.FromSeconds(300), ct);
            deprovisioned.Should().BeTrue("контур A снесён — данные только в S3");
            await SeedClusterAsync(cluster);
            (await WaitProvisionedAsync(cluster, ct)).Should().BeTrue("кластер поднят заново (пустые шарды)");

            // source_pool: чтение из «bkpool», собственный пул кластера — bkpool2.
            var restoreId = $"{DateTimeOffset.UtcNow:yyyyMMddHHmmss}Z-shard1";
            var op = new RestoreOperationState(restoreId, RestoreStatus.Planned, "",
                $"{cluster}/shard1", "latest", "shard1a",
                DateTimeOffset.UtcNow.ToUnixTimeSeconds(), "e2e", SourcePool: Pool);
            await G.PutAsync(Endpoint, PgWorker.Backups.BackupNames.RestoreKey(cluster, "shard1", restoreId),
                PgWorker.Backups.Restore.RestoreStatusJson.Serialize(op), null, ct);
            var restore = await WaitRestoreAsync(cluster, "shard1", restoreId,
                TimeSpan.FromSeconds(420), ct);
            restore["state"].GetString().Should().Be("COMPLETED",
                "DR-restore читает из source_pool (чужой префикс — другой пул)");
        }
        catch
        {
            Fx.MarkFailed();
            throw;
        }
    }
}
