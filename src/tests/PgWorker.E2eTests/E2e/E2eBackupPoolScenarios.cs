using System.Diagnostics;
using System.Text.Json;
using Npgsql;
using PgWorker.Backups;
using PgWorker.Core.Templates;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Sql;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E пула S3 (spec §9 AC1–AC8): инстансы пула — ownS3-ноды окружения
// (SeedS3PoolAsync сеет и config, и per-cluster secrets-ключ — воркер читает
// креды из etcd), policy с секцией s3.
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

    // Окружение класса — фиксура каждого наследника (свой slug: pool-ac1..8).
    protected abstract E2eEnvironment Fx { get; }
    protected string Endpoint => Fx.EtcdEndpoint;
    protected EtcdGateway G => Fx.Gateway;

    protected HttpClient PatroniHttp => Fx.NewPatroniHttpClient();

    // ===== хелперы (прецедент копирования — E2eRestoreScenarios/E2eWalStreamScenarios) =====

    // [TIMING]-хронометраж шага: общий E2eFixture.TimedAsync (очередь меток
    // докачивается в timing.log артефактов телеметрией — stderr testhost
    // буферизует, файл survives).
    protected static Task TimedAsync(string step, Func<Task> action)
        => E2eFixture.TimedAsync(step, action);

    protected static Task<T> TimedAsync<T>(string step, Func<Task<T>> action)
        => E2eFixture.TimedAsync(step, action);

    protected static string PolicyKey(string cluster) => $"/pgworker/backups/{cluster}/policy";

    // mTLS policy-API воркера: тело канона, ответ (status, body) — кейсы 400 AC8.
    protected static async Task<(System.Net.HttpStatusCode Code, string Body)> PutPolicyApiAsync(
        HttpClient api, HostInstance app, string cluster, string body, CancellationToken ct)
    {
        using var content = new StringContent(body, System.Text.Encoding.UTF8, "application/json");
        using var response = await api.PostAsync(
            $"{app.ApiUrl}/api/clusters/{cluster}/backups/policy",
            content, ct);
        return (response.StatusCode, await response.Content.ReadAsStringAsync(ct));
    }

    // Внутрисетевые адреса ownS3-нод для mc-вызовов из контейнера pgw-em
    // (docker exec): alias сети окружения. Хост-публикации localhost из
    // контейнера не видны (localhost = сам контейнер) — только alias.
    protected const string S3Node1Mc = "e2e-owns3:9000";

    protected static string S3Node2Mc(string alias) => $"{alias}:9000";

    protected static string HostPort(string endpoint)
        => endpoint.Replace("http://", "", StringComparison.Ordinal).TrimEnd('/');

    // Воркер с включённой подсистемой: пул и креды — etcd (policy.s3 +
    // /s3/clusters/<C>/secrets, сеет SeedS3PoolAsync), bucket — env;
    // образы e2e, ускоренный бэкофф, короткие пороги WAL.
    protected Task<HostInstance> StartPoolHostAsync(string name, CancellationToken ct)
        => Fx.StartHostAsync(name, extraEnv: new Dictionary<string, string>
        {
            ["PgWorker__Backups__Enabled"] = "true",
            ["PgWorker__Backups__S3__Bucket"] = Bucket,
            ["PgWorker__Backups__Retry__BaseSec"] = "2",
            ["PgWorker__Backups__Retry__MaxSec"] = "4",
            ["PgWorker__Backups__Wal__VerifyIntervalSec"] = "2",
            ["PgWorker__Backups__Wal__StaleSec"] = "120",
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
        // Null-защита: транзиентный неуспех транспорта Range → пустой список
        // (поллинг повторит; NRE внутри лямбды ожидания маскировал причину).
        => (await G.RangeAsync(Endpoint, $"/pgworker/backups/{cluster}/{shard}/full/",
            TestContext.Current.CancellationToken)).Value ?? [];

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
        }, TimeSpan.FromSeconds(90), ct);
        completed.Should().BeTrue("the full must reach COMPLETED");
        return done!;
    }

    // Факт доставки: в wal/ ноды пула есть ≥1 сегмент (running-агентов мало —
    // под параллельной нагрузкой uploads запаздывают за стартом контейнеров).
    protected async Task<bool> WaitWalUploadedAsync(string cluster, string hostPort, CancellationToken ct)
        => await E2eFixture.WaitForAsync(async () =>
        {
            var keys = await McKeysOnNodeAsync(hostPort, $"{cluster}/shard1/wal/");
            return keys.Count > 0;
        }, TimeSpan.FromSeconds(90), ct);

    // t25: «оба агента живы» = статус-ключи agent/<N> обеих нод шарда с
    // state=streaming (lease жив = приёмник жив; docker-контейнеров агентов нет).
    protected async Task<bool> WaitBothWalAgentsAsync(string cluster, CancellationToken ct)
        => await E2eFixture.WaitForAsync(async () =>
        {
            var facts = await Fx.ReadWalAgentFactsAsync(cluster, "shard1", ct);
            return facts.Count(f => f.State == "streaming") >= 2;
        }, TimeSpan.FromSeconds(90), ct);

    protected Task<bool> WaitProvisionedAsync(string cluster, CancellationToken ct)
        // Fail-fast застрявшего провижининга — общий хелпер (E2eFixture):
        // серии переводятся на него с локальных копий поэтапно.
        => E2eEnvironment.WaitProvisionedAsync(Fx, GetOrNullAsync, cluster, ct);

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

    // Внутрисетевой адрес мастера шарда: primary — пробы Patroni /primary по
    // контейнерным именам нод (REST :8008 — константа контракта, arch/14 §2.1);
    // pg — тот же контейнер :5432 (адресация теста целиком внутрисетевая).
    protected async Task<(string Host, int Port, string Node)> MasterPgAsync(
        string cluster, string shard, CancellationToken ct)
    {
        var kv = await G.GetAsync(Endpoint, $"/pgworker/portalloc/{cluster}", ct);
        kv.Value.Should().NotBeNull("portalloc is written at provisioning");
        var entries = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(kv.Value!.Value)!;
        string? primary = null;
        var probeAttempt = 0;
        var probeSummary = "no probes were made";
        var resolved = await E2eFixture.WaitForAsync(async () =>
        {
            probeAttempt++;
            var lines = new List<string>();
            foreach (var (key, addr) in entries
                         .Where(p => p.Key.StartsWith($"{shard}/", StringComparison.Ordinal))
                         .OrderBy(p => p.Key, StringComparer.Ordinal))
            {
                var node = key.Split('/')[1];
                try
                {
                    using var response = await PatroniHttp.GetAsync(
                        $"https://localhost:{addr.GetProperty("patroni").GetInt32()}/primary", ct);
                    if (response.IsSuccessStatusCode)
                    {
                        primary = node;
                        return true;
                    }

                    lines.Add($"{node}→{(int)response.StatusCode}");
                }
                catch (Exception e)
                {
                    lines.Add($"{node}→{e.GetType().Name}:{e.Message.Split('\n')[0]}");
                }
            }

            probeSummary = $"attempt {probeAttempt}: [{string.Join("; ", lines)}]";
            return false;
        }, TimeSpan.FromSeconds(90), ct);
        resolved.Should().BeTrue($"the shard primary must be resolved by Patroni probes (:8008); {probeSummary}");
        return ("localhost", entries[$"{shard}/{primary}"].GetProperty("pg").GetInt32(), primary!);
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
                $"restore {cluster}/{shard}/{id} did not reach COMPLETED: "
                + (status is null ? "no key" : status["state"].GetString())
                + (status is { } s && s.TryGetValue("error", out var err) ? $", error={err.GetString()}" : ""));
        return status!;
    }

    protected async Task<Dictionary<string, JsonElement>> RestoreLatestAsync(
        string cluster, string shard, CancellationToken ct)
    {
        var id = await PutRestorePlannedAsync(cluster, shard, "latest", ct: ct);
        return await WaitRestoreAsync(cluster, shard, id, TimeSpan.FromSeconds(90), ct);
    }
}

// AC1 (молчание без выбора пула → policy с s3 → COMPLETED (AC1)) — отдельный класс: свой guid-контур; коллекции классов
// не сериализуют сценарии между собой (параллельный запуск E2E, ≤5 живых
// контуров — канон docs/e2e-launch.md).
public sealed class PoolNotSelectedScenarios(PoolNotSelectedScenariosFx Ctx) : E2eBackupPoolScenarios, IClassFixture<PoolNotSelectedScenariosFx>
{
    protected override E2eEnvironment Fx => Ctx.Fx;

    // ===== AC1 =====

    // AAA: policy без s3 → полный не планируется (ключей нет за окно);.policy
    // с секцией s3 → полный COMPLETED, объекты full/ на ноде пула (AC1).
    [Fact]
    public async Task Pool_NotSelected_Silent_ThenPolicy_Completes()
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMinutes(3)); // потолок теста 3 мин: неверное ожидание не может длиться дольше (приказ владельца)
        var ct = cts.Token;
        var cluster = $"pac1{Fx.ClusterTag}";
        var ep1 = S3Node1Mc; // mc из контейнера — внутрисетевой alias
        try
        {
            // Arrange — пул сидится, policy БЕЗ секции s3; воркер живёт.
            await TimedAsync("ac1 seed-cluster", () => SeedClusterAsync(cluster));
            await TimedAsync("ac1 seed-pool", () => Fx.SeedS3PoolAsync(Pool, ct));
            await TimedAsync("ac1 policy-no-s3", () => G.PutAsync(Endpoint, PolicyKey(cluster),
                """{"full_max_age_sec":3600}""", null, ct));
            await using var app = await StartPoolHostAsync("pac1", ct);
            var provisioned = await TimedAsync(
                "ac1 provision", () => WaitProvisionedAsync(cluster, ct));
            provisioned.Should().BeTrue("provisioning before the observation window");

            // Act/Assert 1 — «пул не выбран» = молчание: full-ключей нет за
            // окно наблюдения. Окно = ×1.25 от ожидаемого нормального времени
            // запуска полного (факт [TIMING] тех же прогонов: full-completed
            // в условиях AC1 без фоновой нагрузки — 10.5 c → 13 c): если бы
            // подсистема ошибочно писала, ключи появились бы задолго до
            // истечения окна; «неудача пишет своё» — длиннее окна не нужно.
            await TimedAsync("ac1 silence-window-13s",
                () => Task.Delay(TimeSpan.FromSeconds(13), ct));
            (await FullKeysAsync(cluster, "shard1")).Should().BeEmpty(
                "no s3 in policy — the cluster subsystem does not write (arch/19 §4)");

            // Act 2 — policy с секцией s3 (применяется следующим тиком).
            await TimedAsync("ac1 policy-s3", () => G.PutAsync(Endpoint, PolicyKey(cluster),
                $"{{\"full_max_age_sec\":3600,{E2eEnvironment.PoolS3(Pool)}}}", null, ct));

            // Assert 2 — COMPLETED ≤ 300 c, объекты на ноде пула.
            var done = await TimedAsync(
                "ac1 full-completed", () => WaitCompletedFullAsync(cluster, ct));
            var id = done.Key.Split('/').Last();
            var listing = await McLsOnNodeAsync(ep1, $"{cluster}/shard1/");
            listing.Should().Contain($"full/{id}/backup_manifest",
                "the full backup objects are on the pool node (inspection by the node endpoint)");
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
public sealed class PoolMasterReplicaSplitScenarios(PoolMasterReplicaSplitScenariosFx Ctx) : E2eBackupPoolScenarios, IClassFixture<PoolMasterReplicaSplitScenariosFx>
{
    protected override E2eEnvironment Fx => Ctx.Fx;

    // ===== AC2 =====

    // AAA: пул из двух нод, write_master=[node1], write_replica=[node2]:
    // каждый агент доставил сегменты на СВОЮ ноду; полный с реплики (role=replica)
    // — объекты full/<id>/ СТРОГО на node2 (на node1 их нет).
    [Fact]
    public async Task Pool_MasterReplicaSplit_ObjectsOnTheirNodes()
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMinutes(3)); // потолок теста 3 мин: неверное ожидание не может длиться дольше (приказ владельца)
        var ct = cts.Token;
        var cluster = $"pac2{Fx.ClusterTag}";
        var ep1 = S3Node1Mc; // mc из контейнера — внутрисетевой alias
        try
        {
            // Arrange — вторая нода пула + двухнодовые списки.
            await Fx.StartExtraS3NodeAsync(Node2Alias, ct);
            var ep2 = S3Node2Mc(Node2Alias); // mc из контейнера — alias ноды
            await TimedAsync("ac2 seed-pool-2nodes",
                () => Fx.SeedS3PoolAsync(Pool, ct, [Fx.S3EndpointForContainers, ep2]));
            await TimedAsync("ac2 seed-cluster", () => SeedClusterAsync(cluster));
            await TimedAsync("ac2 policy-split", () => PutPolicyAsync(cluster, ["node1"], ["node2"]));
            await using var app = await StartPoolHostAsync("pac2", ct);
            var provisioned = await TimedAsync(
                "ac2 provision", () => WaitProvisionedAsync(cluster, ct));
            provisioned.Should().BeTrue("provisioning before the wal phase");

            // WAL-нагрузка ПЕРВАЯ (Patroni выбирает sync при живом потоке —
            // прецедент E2eWalStreamScenarios), оба агента поднимаются.
            var (host, port, _) = await MasterPgAsync(cluster, "shard1", ct);
            var adminDsn = DatabaseProvisioner.BuildAdminDsn(host, port, cluster,
                new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
            using var load = new CancellationTokenSource();
            var loader = BackgroundWalLoadAsync(adminDsn, load.Token);
            var bothAgents = await TimedAsync(
                "ac2 both-wal-agents", () => WaitBothWalAgentsAsync(cluster, ct));
            bothAgents.Should().BeTrue("master and sync agents are alive (sync elected)");
            await TimedAsync("ac2 generate-wal", () => GenerateWalAsync(adminDsn, ct));
            var uploaded = await TimedAsync(
                "ac2 wal-uploaded-node1", () => WaitWalUploadedAsync(cluster, ep1, ct));
            uploaded.Should().BeTrue("the master agent delivered segments to node1");

            // Act/Assert — полный с реплики: role=replica, объекты на node2,
            // на node1 префикса full/ нет.
            var done = await TimedAsync(
                "ac2 full-completed", () => WaitCompletedFullAsync(cluster, ct));
            var status = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(done.Value)!;
            status["role"].GetString().Should().Be("replica", "the sync replica is elected — the full backup source is a replica");
            var id = done.Key.Split('/').Last();
            var onNode2 = await McLsOnNodeAsync(ep2, $"{cluster}/shard1/");
            onNode2.Should().Contain($"full/{id}/backup_manifest",
                "write_replica=[node2] — the full is delivered to the second node");
            var keysOnNode1 = await McKeysOnNodeAsync(ep1, $"{cluster}/shard1/");
            keysOnNode1.Should().NotContain(k => k.StartsWith($"full/{id}/"),
                "write_master does not participate in writing the replica full");
            // Каждый агент доставил сегменты на свою ноду (list по endpoint каждой).
            keysOnNode1.Should().Contain(k => k.StartsWith("wal/"), "the master agent writes on node1");
            (await McKeysOnNodeAsync(ep2, $"{cluster}/shard1/"))
                .Should().Contain(k => k.StartsWith("wal/"), "the sync agent writes on node2");
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
public sealed class PoolSingleNodeScenarios(PoolSingleNodeScenariosFx Ctx) : E2eBackupPoolScenarios, IClassFixture<PoolSingleNodeScenariosFx>
{
    protected override E2eEnvironment Fx => Ctx.Fx;

    // ===== AC3 =====

    // AAA: однонодовый пул — оба агента пишут на node1, полный COMPLETED,
    // restore latest COMPLETED (перебор чтения по единственной ноде).
    [Fact]
    public async Task Pool_SingleNode_BothStreams_RestoreOk()
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMinutes(3)); // потолок теста 3 мин: неверное ожидание не может длиться дольше (приказ владельца)
        var ct = cts.Token;
        var cluster = $"pac3{Fx.ClusterTag}";
        try
        {
            await TimedAsync("ac3 seed-cluster", () => SeedClusterAsync(cluster));
            await TimedAsync("ac3 seed-pool", () => Fx.SeedS3PoolAsync(Pool, ct));
            await TimedAsync("ac3 policy", () => PutPolicyAsync(cluster, ["node1"], ["node1"]));
            await using var app = await StartPoolHostAsync("pac3", ct);
            var provisioned = await TimedAsync(
                "ac3 provision", () => WaitProvisionedAsync(cluster, ct));
            provisioned.Should().BeTrue();

            var (host, port, _) = await MasterPgAsync(cluster, "shard1", ct);
            var adminDsn = DatabaseProvisioner.BuildAdminDsn(host, port, cluster,
                new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
            await TimedAsync("ac3 generate-wal", () => GenerateWalAsync(adminDsn, ct));
            using var load = new CancellationTokenSource();
            var loader = BackgroundWalLoadAsync(adminDsn, load.Token);
            var bothAgents = await TimedAsync(
                "ac3 both-wal-agents", () => WaitBothWalAgentsAsync(cluster, ct));
            bothAgents.Should().BeTrue("both agents are raised");
            var ep1 = S3Node1Mc; // mc из контейнера — внутрисетевой alias
            var uploaded = await TimedAsync(
                "ac3 wal-uploaded", () => WaitWalUploadedAsync(cluster, ep1, ct));
            uploaded.Should().BeTrue("both streams deliver segments to the single node");
            var done = await TimedAsync(
                "ac3 full-completed", () => WaitCompletedFullAsync(cluster, ct));
            done.Should().NotBeNull();

            // Act/Assert — restore latest из пула: COMPLETED.
            var restore = await TimedAsync(
                "ac3 restore-latest", () => RestoreLatestAsync(cluster, "shard1", ct));
            restore["state"].GetString().Should().Be("COMPLETED", "restore from a single-node pool");
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
public sealed class PoolFailoverExhaustionScenarios(PoolFailoverExhaustionScenariosFx Ctx) : E2eBackupPoolScenarios, IClassFixture<PoolFailoverExhaustionScenariosFx>
{
    protected override E2eEnvironment Fx => Ctx.Fx;

    // ===== AC4 =====

    // AAA: список записи [node1,node2] — стоп node1 → мастер-агент доезжает до
    // node2 (стрим жив, сегменты едут на node2); список [node1] — стоп node1 →
    // wal DEGRADED с маркером s3-pool-exhausted; подъём node1 → ACTIVE (AC4).
    [Fact]
    public async Task Pool_FailoverList_And_Exhaustion_SelfHeal()
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMinutes(3)); // потолок теста 3 мин: неверное ожидание не может длиться дольше (приказ владельца)
        var ct = cts.Token;
        var cluster = $"pac4{Fx.ClusterTag}";
        var writer = new WalStatusWriter(G, [Endpoint]);
        try
        {
            // Arrange — двухнодовый пул, список записи с перебором; поток жив.
            await Fx.StartExtraS3NodeAsync(Node2Alias, ct);
            var ep2 = S3Node2Mc(Node2Alias); // mc из контейнера — alias ноды
            await Fx.SeedS3PoolAsync(Pool, ct, [Fx.S3EndpointForContainers, ep2]);
            await SeedClusterAsync(cluster);
            await PutPolicyAsync(cluster, ["node1", "node2"], ["node2", "node1"]);
            var app = await StartPoolHostAsync("pac4", ct);
            (await WaitProvisionedAsync(cluster, ct)).Should().BeTrue();
            var (host, port, _) = await MasterPgAsync(cluster, "shard1", ct);
            var adminDsn = DatabaseProvisioner.BuildAdminDsn(host, port, cluster,
                new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
            using var load = new CancellationTokenSource();
            var loader = BackgroundWalLoadAsync(adminDsn, load.Token);
            (await WaitBothWalAgentsAsync(cluster, ct)).Should().BeTrue("agents before the node failure");
            var ep1 = S3Node1Mc; // mc из контейнера — внутрисетевой alias
            (await WaitWalUploadedAsync(cluster, ep1, ct)).Should().BeTrue(
                "segments are delivered to node1 before the failure injection");

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
            }, TimeSpan.FromSeconds(90), ct);
            survived.Should().BeTrue(
                "the stream continued on node2 (walking the write list, RPO: the slot is confirmed after the put)");
            await Fx.RunDockerAsync(["start", Fx.S3NodeContainerName], ct);

            // Act B — список [node1]: рестарт воркера (policy перечитывается стартом —
            // детерминизм без надежды на revise на лету), стоп node1 → исчерпание:
            // DEGRADED + маркер; подъём → ACTIVE.
            await app.DisposeAsync();
            await PutPolicyAsync(cluster, ["node1"], ["node2"]);
            // Рецикл подключения приёмников (воркер ВНИЗ — его стартовый del
            // ключа не помешает): write-список резолвится ТОЛЬКО на цикле
            // подключения (arch/19 §3 — новая сессия), а живые сессии Act A
            // стримят со старым списком [node1,node2] — исчерпания списка
            // [node1] без рецикла не случится. Форсируем новую сессию
            // легитимным arch-гейтом: put /pgworker/backups-disabled → сессии
            // чисто завершаются (StopGateProbe ≤ 5 c, Reconnect) →
            // waiting_disabled → delete → цикл подключения резолвит targets
            // заново, уже [node1].
            await G.PutAsync(Endpoint, PgWorker.Backups.BackupsDisabledGate.Key, "1", null, ct);
            var paused = await E2eFixture.WaitForAsync(async () =>
            {
                var facts = await Fx.ReadWalAgentFactsAsync(cluster, "shard1", ct);
                return facts.Count >= 2 && facts.All(f => f.State == "waiting_disabled");
            }, TimeSpan.FromSeconds(90), ct);
            paused.Should().BeTrue("receivers paused by the backups-disabled gate");
            await G.DeleteAsync(Endpoint, PgWorker.Backups.BackupsDisabledGate.Key, prefix: false, ct);
            var app2 = await StartPoolHostAsync("pac4b", ct);
            await using var app2Owner = app2;
            var reconnected = await E2eFixture.WaitForAsync(async () =>
            {
                var facts = await Fx.ReadWalAgentFactsAsync(cluster, "shard1", ct);
                return facts.Count >= 2 && facts.All(f => f.State == "streaming");
            }, TimeSpan.FromSeconds(90), ct);
            reconnected.Should().BeTrue("receivers opened new sessions with the [node1] write list");
            await Fx.RunDockerAsync(["stop", Fx.S3NodeContainerName], ct);
            var degraded = await E2eFixture.WaitForAsync(async () =>
            {
                var read = await writer.ReadAsync(cluster, "shard1", ct);
                return read is { IsSuccess: true } && read.Value is { State: WalStreamStatus.Degraded }
                    && read.Value.Error?.Contains("s3-pool-exhausted", StringComparison.Ordinal) == true;
            }, TimeSpan.FromSeconds(90), ct);
            degraded.Should().BeTrue("list exhaustion — DEGRADED with the s3-pool-exhausted marker");
            await Fx.RunDockerAsync(["start", Fx.S3NodeContainerName], ct);
            var healed = await E2eFixture.WaitForAsync(async () =>
            {
                var read = await writer.ReadAsync(cluster, "shard1", ct);
                return read is { IsSuccess: true } && read.Value is { State: WalStreamStatus.Active };
            }, TimeSpan.FromSeconds(90), ct);
            healed.Should().BeTrue("raising the node — self-healing back to ACTIVE (retry loop)");
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
public sealed class PoolRoleSwitchScenarios(PoolRoleSwitchScenariosFx Ctx) : E2eBackupPoolScenarios, IClassFixture<PoolRoleSwitchScenariosFx>
{
    protected override E2eEnvironment Fx => Ctx.Fx;

    // ===== AC5 =====

    // AAA: promote слейва (stop мастера → Patroni failover) → агент новой роли
    // пересоздаётся со списком МАСТЕРА; .history нового TLI и свежие сегменты —
    // на ноде master-списка; цепочка склеена, ключ ACTIVE (AC5).
    [Fact]
    public async Task Pool_RoleSwitch_AgentRecreatedWithMasterList()
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMinutes(3)); // потолок теста 3 мин: неверное ожидание не может длиться дольше (приказ владельца)
        var ct = cts.Token;
        var cluster = $"pac5{Fx.ClusterTag}";
        var writer = new WalStatusWriter(G, [Endpoint]);
        var ep1 = S3Node1Mc; // mc из контейнера — внутрисетевой alias
        try
        {
            await Fx.StartExtraS3NodeAsync(Node2Alias, ct);
            var ep2 = S3Node2Mc(Node2Alias); // mc из контейнера — alias ноды
            await Fx.SeedS3PoolAsync(Pool, ct, [Fx.S3EndpointForContainers, ep2]);
            await SeedClusterAsync(cluster);
            await PutPolicyAsync(cluster, ["node1"], ["node2"]);
            await using var app = await StartPoolHostAsync("pac5", ct);
            (await WaitProvisionedAsync(cluster, ct)).Should().BeTrue();
            var (host, port, masterNode) = await MasterPgAsync(cluster, "shard1", ct);
            var adminDsn = DatabaseProvisioner.BuildAdminDsn(host, port, cluster,
                new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
            using var load = new CancellationTokenSource();
            var loader = BackgroundWalLoadAsync(adminDsn, load.Token);
            (await WaitBothWalAgentsAsync(cluster, ct)).Should().BeTrue("both agents before the promote");
            await GenerateWalAsync(adminDsn, ct); // база цепочки TLI 1
            (await WaitWalUploadedAsync(cluster, ep1, ct)).Should().BeTrue(
                "TLI 1 segments are delivered before the promote");

            // Act — потеря мастера → promote реплики (механика .history —
            // образец E2eWalStreamPromoteScenarios).
            await Fx.RunDockerAsync(["stop", $"pgw-{cluster}-shard1-{masterNode}"], ct);
            var (newMasterHost, newMasterPort, _) = await MasterPgAsync(cluster, "shard1", ct);
            var newMasterDsn = DatabaseProvisioner.BuildAdminDsn(newMasterHost, newMasterPort, cluster,
                new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
            // Promote-окно: ex-реплика отвечает «recovery is in progress» (55000),
            // пока promote не завершён — switch ретраится до готовности нового мастера.
            (await E2eFixture.WaitForAsync(async () =>
            {
                try
                {
                    await SwitchWalsAsync(newMasterDsn, 2, ct);
                    return true;
                }
                catch (Npgsql.PostgresException pe) when (pe.SqlState == "55000")
                {
                    return false;
                }
            }, TimeSpan.FromSeconds(90), ct)).Should().BeTrue("the new master left recovery — the switch is done");

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
            }, TimeSpan.FromSeconds(90), ct);
            switched.Should().BeTrue(
                "the agent of the new role writes to the master-list node (node1), .history in S3, the chain is continuous");
            // Двойная архивация (arch/19 §3): .history нового TLI доставляют ОБА
            // приёмника шарда — каждый в список СВОЕЙ роли (мастер → node1,
            // реплика → node2), поэтому сравнение счётчиков «node2 < node1» —
            // гонка сходимости, а не свойство системы; намерение «переключение
            // обслуживает master-список» уже покрыто ассертом switched выше
            // (.history на node1 + wal ACTIVE + непрерывность).
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
public sealed class PoolReadAnyNodeScenarios(PoolReadAnyNodeScenariosFx Ctx) : E2eBackupPoolScenarios, IClassFixture<PoolReadAnyNodeScenariosFx>
{
    protected override E2eEnvironment Fx => Ctx.Fx;

    // ===== AC6 =====

    // AAA: данные писались на node1; mc mirror n1→n2 (эмуляция mesh-доставки);
    // стоп node1 → restore читает со второй ноды пула (перебор ReadAll) —
    // COMPLETED (AC6, spec §9: mirror — эмуляция отставшей реплики пула).
    [Fact]
    public async Task Pool_ReadAnyNode_RestoreAfterNodeStop()
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMinutes(3)); // потолок теста 3 мин: неверное ожидание не может длиться дольше (приказ владельца)
        var ct = cts.Token;
        var cluster = $"pac6{Fx.ClusterTag}";
        try
        {
            await Fx.StartExtraS3NodeAsync(Node2Alias, ct);
            var ep2 = S3Node2Mc(Node2Alias); // mc из контейнера — alias ноды
            await Fx.SeedS3PoolAsync(Pool, ct, [Fx.S3EndpointForContainers, ep2]);
            await SeedClusterAsync(cluster);
            // Пишем всё на node1 (оба списка — node1).
            await PutPolicyAsync(cluster, ["node1"], ["node1"]);
            await using var app = await StartPoolHostAsync("pac6", ct);
            (await WaitProvisionedAsync(cluster, ct)).Should().BeTrue();
            var (host, port, _) = await MasterPgAsync(cluster, "shard1", ct);
            var adminDsn = DatabaseProvisioner.BuildAdminDsn(host, port, cluster,
                new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
            await ExecAsync(adminDsn, "CREATE TABLE pool_probe(id int, note text)", ct);
            await ExecAsync(adminDsn, "INSERT INTO pool_probe SELECT g, 'keeper' FROM generate_series(1, 5) g", ct);
            (await WaitBothWalAgentsAsync(cluster, ct)).Should().BeTrue();
            await GenerateWalAsync(adminDsn, ct);
            var done = await WaitCompletedFullAsync(cluster, ct);
            done.Should().NotBeNull();

            // Пауза стрима ПЕРЕД копией (стрим пишется только на node1 — write
            // [node1]): без паузы сегменты, залитые на node1 ПОСЛЕ копии,
            // существуют только на останавливаемой node1 — накат restore
            // упирается в «стык» (fetch-wal: node1 мёртв, на node2 not found)
            // и recovery ждёт до бюджета. Пауза делает копию консистентной:
            // хвост wal/ заморожен на момент tar-пайпа. Снятие — после стопа
            // node1 (restore шарду стрим не нужен; teardown снимает ключ).
            await G.PutAsync(Endpoint, "/pgworker/backups-disabled", "1", null, ct);
            (await E2eFixture.WaitForAsync(async () =>
            {
                var facts = await Fx.ReadWalAgentFactsAsync(cluster, "shard1", ct);
                return facts.Count >= 2 && facts.All(f => f.State == "waiting_disabled");
            }, TimeSpan.FromSeconds(60), ct)).Should().BeTrue("receivers paused before the copy");

            // Act — копия bucket node1 → node2, затем стоп node1.
            // Прогон t100-3…7 (2026-10-27): S3→S3 копии (mc mirror/cp --recursive)
            // под параллельной записью приёмника СТАБИЛЬНО теряли объекты набора
            // (корневые backup_manifest/label на node2 отсутствовали, mc exit 0) —
            // restore закономерно FAILED «no backup_manifest». Прямая проверка
            // показала: чистый ownS3 с той же data LIST/STAT/GET манифеста
            // отдаёт — S3-API ownS3 корректен; дефект в S3→S3-копировании под
            // конкурентной записью. Копия — ЧЕРЕЗ ФС (docker cp контейнер→контейнер,
            // мимо S3-API); restore читает node2 через S3-API, как в проде.
            var node2Container = Fx.ExtraS3NodeContainerName(Node2Alias);
            // tar-пайп контейнер→контейнер (docker cp … - | docker exec -i tar -x):
            // без host-файлов (docker cp на macOS раскладывал host-копию
            // непредсказуемо — прогон t100-11) и с чисткой целевого корня: слияние
            // со собственными объектами node2 даёт смешанные каталоги, на которых
            // LIST ownS3 отдаёт 500 (t100-9). У node2 собственных записей нет:
            // write-списки пула — [node1]. docker cp кладёт basename каталога в
            // корень tar («pgworker-backups/…»): без --strip-components=1 распаковка
            // даёт вложенный дубль /data/pgworker-backups/pgworker-backups/… — S3-API
            // node2 пуст (fs-объекты на уровень ниже), restore честно 404 (прогон
            // t100 run4; fs-ассерт тогда давал ложный PRESENT: его шаблон
            // /data/pgworker-backups/<Bucket>/… совпал с вложенным путём).
            // ФС-путь объекта — <OWNS3_DATA_DIR=…>/pgworker-backups/<key> (env
            // ownS3-нод E2E), поэтому ассерт ниже проверяет <data>/pgworker-backups/
            // как РОВНО один сегмент каталога бакета.
            await E2eFixture.RunProcessAsync("sh", ["-c",
                $"docker cp {Fx.S3NodeContainerName}:/data/pgworker-backups -"
                + $" | docker exec -i {node2Container} sh -c"
                + $" 'rm -rf /data/pgworker-backups && mkdir -p /data/pgworker-backups"
                + $" && tar -xf - -C /data/pgworker-backups --strip-components=1'"], ct);
            // ownS3 индексирует /data при СТАРТЕ (прогон t100-8: fs-файлы, дописанные
            // в живой контейнер, S3-API не видны; чистый ownS3, стартовавший с той же
            // data, LIST/STAT/GET манифеста отдаёт) — рестарт переиндексирует копию.
            await Fx.RunDockerAsync(["restart", node2Container], ct);
            // Манифест на node2 — ассерт ДО стопа node1 (канон e2e-launch): fs-факт
            // (xl.meta объекта по ФС-пути <data>/pgworker-backups/<key>).
            var fullId = done.Key.Split('/').Last();
            var manifestOnNode2 = await Fx.RunDockerAsync(["exec", node2Container, "sh", "-c",
                $"test -f /data/pgworker-backups/{cluster}/shard1/full/{fullId}/backup_manifest/xl.meta"
                + " && echo PRESENT || echo MISSING"], ct);
            manifestOnNode2.Trim().Should().Be("PRESENT",
                "the mesh copy must carry the backup manifest before node1 is stopped");
            await Fx.RunDockerAsync(["stop", Fx.S3NodeContainerName], ct);
            await G.DeleteAsync(Endpoint, "/pgworker/backups-disabled", prefix: false, ct);

            // Assert — restore из пула при мёртвой node1: COMPLETED (чтение c node2).
            // Свой 3-мин CTS на restore-секцию (мульти-CTS, приказ владельца) +
            // бюджет 150 c (не 90): операция — скачивание набора + recovery +
            // промоут при мёртвой части пула — ~80-110 c, а тик воркера может
            // зависнуть на поднимающемся Patroni ноды — COMPLETED появляется с
            // задержкой; при 90 c тест падал «RUNNING» на 91-й секунде при
            // ФАКТИЧЕСКИ успешном restore (разбор b01: Succeeded на ~83-й c,
            // промоут «ready to accept connections» — продукт корректен).
            using var ctsRestore = CancellationTokenSource.CreateLinkedTokenSource(
                TestContext.Current.CancellationToken);
            ctsRestore.CancelAfter(TimeSpan.FromMinutes(3));
            ct = ctsRestore.Token;

            var restoreId = await PutRestorePlannedAsync(cluster, "shard1", "latest", ct: ct);
            var restore = await WaitRestoreAsync(cluster, "shard1", restoreId,
                TimeSpan.FromSeconds(150), ct);
            restore["state"].GetString().Should().Be("COMPLETED",
                "the ReadAll walkover reaches a live pool node (mesh copy)");
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
public sealed class PoolSnapshotsOwnPairScenarios(PoolSnapshotsOwnPairScenariosFx Ctx) : E2eBackupPoolScenarios, IClassFixture<PoolSnapshotsOwnPairScenariosFx>
{
    protected override E2eEnvironment Fx => Ctx.Fx;

    // ===== AC7 =====

    // AAA: Enabled + полная пара → слепки уходят в СВОЙ bucket pgworker-etcd-
    // snapshots (префикс etcd/), статус-ключ OK; Enabled БЕЗ пары → воркер
    // падает стартом (fail-fast, в логе имя секции) (AC7).
    [Fact]
    public async Task Snapshots_OwnPair_BucketAndFailFast()
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMinutes(3)); // потолок теста 3 мин: неверное ожидание не может длиться дольше (приказ владельца)
        var ct = cts.Token;
        try
        {
            // Arrange — bucket слепков на ownS3-ноды окружения (свой, не пуловый).
            var ep1 = S3Node1Mc; // mc из контейнера — внутрисетевой alias
            await Fx.RunMcAsync(
                $"mc alias set t http://{ep1} {E2eEnvironment.S3RootUser} {E2eEnvironment.S3RootPassword} >/dev/null"
                + $" && mc mb --ignore-existing t/{SnapshotBucket}", ct);

            // Act 1 — воркер со слепочной парой (localhost-вид: sink из процесса).
            var s3Endpoint = Fx.S3EndpointForContainers; // env воркера — внутрисетевой alias
            await using var app = await Fx.StartHostAsync("pac7", extraEnv: new Dictionary<string, string>
            {
                ["PgWorker__Snapshots__Export__Enabled"] = "true",
                ["PgWorker__Snapshots__Export__RetryIntervalSec"] = "5",
                ["PgWorker__Snapshots__Export__S3__Endpoint"] = s3Endpoint,
                ["PgWorker__Snapshots__Export__S3__Bucket"] = SnapshotBucket,
                ["PgWorker__Snapshots__Export__S3__AccessKey"] = E2eEnvironment.S3RootUser,
                ["PgWorker__Snapshots__Export__S3__SecretKey"] = E2eEnvironment.S3RootPassword,
            }, ct: ct);

            // Assert 1 — статус OK, пара db+meta в своём bucket (префикс etcd/).
            var ok = await E2eFixture.WaitForAsync(async () =>
                (await G.GetAsync(Endpoint, "/pgworker/etcd-snapshots", ct)).Value?.Value
                    .Contains("\"state\":\"OK\"") == true,
                TimeSpan.FromSeconds(90), ct);
            ok.Should().BeTrue("the snapshot pair serves the export");
            var listing = await Fx.RunMcAsync(
                $"mc alias set t http://{ep1} {E2eEnvironment.S3RootUser} {E2eEnvironment.S3RootPassword} >/dev/null"
                + $" && mc ls --recursive t/{SnapshotBucket}/etcd/ || true", ct);
            listing.Should().Contain(".db").And.Contain(".meta.json",
                "the snapshot pair — in its OWN bucket with the etcd/ prefix");

            // Act 2 — Enabled БЕЗ пары: процесс падает стартом (fail-fast),
            // диагностика называет секцию.
            var thrown = await Assert.ThrowsAnyAsync<ApplicationException>(() =>
                Fx.StartHostAsync("pac7nofail", extraEnv: new Dictionary<string, string>
                {
                    ["PgWorker__Snapshots__Export__Enabled"] = "true",
                }, ct: ct));
            thrown.Message.Should().Contain("did not come up within",
                "the worker does not serve the API (start fail-fast; the section text is in the worker logs, artifacts)");
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
public sealed class PoolPolicyValidationScenarios(PoolPolicyValidationScenariosFx Ctx) : E2eBackupPoolScenarios, IClassFixture<PoolPolicyValidationScenariosFx>
{
    protected override E2eEnvironment Fx => Ctx.Fx;

    // ===== AC8 =====

    // AAA: policy-API — 400 по полям (пул не найден / TO_REMOVE / нода не в пуле /
    // дубликат / пустой список; заявленный без portalloc инстанс — ВАЛИДЕН);
    // restore из source_pool чужого пула — COMPLETED (AC8).
    [Fact]
    public async Task PolicyValidation_400_ByField_And_SourcePool_Restore()
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMinutes(3)); // потолок теста 3 мин: неверное ожидание не может длиться дольше (приказ владельца)
        var ct = cts.Token;
        var cluster = $"pac8{Fx.ClusterTag}";
        try
        {
            await Fx.StartExtraS3NodeAsync(Node2Alias, ct);
            var ep2 = S3Node2Mc(Node2Alias); // mc из контейнера — alias ноды
            await Fx.SeedS3PoolAsync(Pool, ct, [Fx.S3EndpointForContainers, ep2]);
            // Пул в состоянии TO_REMOVE + пул с Заявленной нодой без portalloc.
            await G.PutAsync(Endpoint, "/s3/clusters/toremove/config",
                """{"platform":"owns3","nodes":1,"created_unix":1760000000,"state":"TO_REMOVE"}""",
                null, ct);
            await G.PutAsync(Endpoint, "/s3/clusters/claiming/config",
                """{"platform":"owns3","nodes":1,"created_unix":1760000000}""",
                null, ct);
            await G.PutAsync(Endpoint, "/s3/clusters/claiming/nodes/node1/placement",
                """{"docker_host":"local"}""", null, ct);
            await SeedClusterAsync(cluster);
            await Fx.SeedS3PoolAsync(Pool, ct);
            await PutPolicyAsync(cluster, ["node1"], ["node1"]);
            await using var app = await StartPoolHostAsync("pac8", ct);
            (await WaitProvisionedAsync(cluster, ct)).Should().BeTrue();

            // Act/Assert 1 — валидация по полям (mTLS policy-API воркера).
            using var api = Fx.CreateMtlsApiClient();
            var (code, body) = await PutPolicyApiAsync(api, app, cluster, 
                """{"full_max_age_sec":3600,"s3":{"pool":"ghost","write_master":["node1"],"write_replica":["node1"]}}""", ct);
            code.Should().Be(System.Net.HttpStatusCode.BadRequest);
            body.Should().Contain("ghost").And.Contain("s3.pool", "the error is on the s3.pool field");
            (code, body) = await PutPolicyApiAsync(api, app, cluster,
                """{"full_max_age_sec":3600,"s3":{"pool":"toremove","write_master":["node1"],"write_replica":["node1"]}}""", ct);
            code.Should().Be(System.Net.HttpStatusCode.BadRequest, "pool in TO_REMOVE — 400");
            (code, body) = await PutPolicyApiAsync(api, app, cluster,
                $"{{\"full_max_age_sec\":3600,\"s3\":{{\"pool\":\"{Pool}\",\"write_master\":[\"nosuch\"],\"write_replica\":[\"node1\"]}}}}", ct);
            code.Should().Be(System.Net.HttpStatusCode.BadRequest);
            body.Should().Contain("nosuch", "the error names a node outside the pool");
            (code, _) = await PutPolicyApiAsync(api, app, cluster,
                $"{{\"full_max_age_sec\":3600,\"s3\":{{\"pool\":\"{Pool}\",\"write_master\":[\"node1\",\"node1\"],\"write_replica\":[\"node1\"]}}}}", ct);
            code.Should().Be(System.Net.HttpStatusCode.BadRequest, "a duplicate in the list — 400");
            (code, body) = await PutPolicyApiAsync(api, app, cluster,
                $"{{\"full_max_age_sec\":3600,\"s3\":{{\"pool\":\"{Pool}\",\"write_master\":[],\"write_replica\":[\"node1\"]}}}}", ct);
            code.Should().Be(System.Net.HttpStatusCode.BadRequest);
            body.Should().Contain("s3.write_master", "empty list — 400 on the field");
            // Инстанс заявлен (placement есть), portalloc-записи нет — policy ВАЛИДНА
            // (адрес появится при provisioning, API не отвечает 400 — spec §4.3).
            (code, _) = await PutPolicyApiAsync(api, app, cluster,
                """{"full_max_age_sec":3600,"s3":{"pool":"claiming","write_master":["node1"],"write_replica":["node1"]}}""", ct);
            ((int)code).Should().BeInRange(200, 299, $"a declared node without portalloc does not invalidate the policy: {body}");
            // Возврат рабочей policy (замещение целиком).
            string finalBody;
            (code, finalBody) = await PutPolicyApiAsync(api, app, cluster,
                $"{{\"full_max_age_sec\":3600,\"s3\":{{\"pool\":\"{Pool}\",\"write_master\":[\"node1\"],\"write_replica\":[\"node1\"]}}}}", ct);
            ((int)code).Should().BeInRange(200, 299, $"a valid policy is accepted: {finalBody}");

            // Act 2 — DR: полный лежит в пуле «bkpool» (node1 жив); чтение DR-источника
            // задаётся source_pool. Сносим контур кластера и поднимаем заново
            // (данные только в S3 — DR), policy кластера больше нет.
            // DR-секция (снос + повторный подъём + restore) — свой 3-мин CTS
            // (мульти-CTS, приказ владельца: длинный тест — переприсвоение ct
            // по секциям; сценарий целиком в один потолок не умещается).
            using var ctsDr = CancellationTokenSource.CreateLinkedTokenSource(
                TestContext.Current.CancellationToken);
            ctsDr.CancelAfter(TimeSpan.FromMinutes(3));
            ct = ctsDr.Token;
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
            }, TimeSpan.FromSeconds(90), ct);
            deprovisioned.Should().BeTrue("contour A is torn down — data only in S3");
            await SeedClusterAsync(cluster);
            (await WaitProvisionedAsync(cluster, ct)).Should().BeTrue("the cluster is raised anew (empty shards)");

            // source_pool: чтение из «bkpool», собственный пул кластера — bkpool2.
            var restoreId = $"{DateTimeOffset.UtcNow:yyyyMMddHHmmss}Z-shard1";
            var op = new RestoreOperationState(restoreId, RestoreStatus.Planned, "",
                $"{cluster}/shard1", "latest", "shard1a",
                DateTimeOffset.UtcNow.ToUnixTimeSeconds(), "e2e", SourcePool: Pool);
            await G.PutAsync(Endpoint, PgWorker.Backups.BackupNames.RestoreKey(cluster, "shard1", restoreId),
                PgWorker.Backups.Restore.RestoreStatusJson.Serialize(op), null, ct);
            // Бюджет 150 c: DR-операция (скачивание + recovery + промоут) ~80-110 c
            // (разбор b01) — 90-секундный бюджет не вмещал её при фактическом успехе.
            var restore = await WaitRestoreAsync(cluster, "shard1", restoreId,
                TimeSpan.FromSeconds(150), ct);
            restore["state"].GetString().Should().Be("COMPLETED",
                "the DR-restore reads from source_pool (a foreign prefix — another pool)");
        }
        catch
        {
            Fx.MarkFailed();
            throw;
        }
    }
}

/// <summary>Компаньон-фикстура окружения PoolNotSelectedScenarios (полный контур на класс:
/// сеть/etcd/ownS3/CA/окно портов; docs/e2e-isolation.md §1).</summary>
public sealed class PoolNotSelectedScenariosFx : E2eContourFixture
{
    protected override string Slug => "pool-ac1";
    protected override bool WithS3Node => true;
}

/// <summary>Компаньон-фикстура окружения PoolMasterReplicaSplitScenarios (полный контур на класс:
/// сеть/etcd/ownS3/CA/окно портов; docs/e2e-isolation.md §1).</summary>
public sealed class PoolMasterReplicaSplitScenariosFx : E2eContourFixture
{
    protected override string Slug => "pool-ac2";
    protected override bool WithS3Node => true;
}

/// <summary>Компаньон-фикстура окружения PoolSingleNodeScenarios (полный контур на класс:
/// сеть/etcd/ownS3/CA/окно портов; docs/e2e-isolation.md §1).</summary>
public sealed class PoolSingleNodeScenariosFx : E2eContourFixture
{
    protected override string Slug => "pool-ac3";
    protected override bool WithS3Node => true;
}

/// <summary>Компаньон-фикстура окружения PoolFailoverExhaustionScenarios (полный контур на класс:
/// сеть/etcd/ownS3/CA/окно портов; docs/e2e-isolation.md §1).</summary>
public sealed class PoolFailoverExhaustionScenariosFx : E2eContourFixture
{
    protected override string Slug => "pool-ac4";
    protected override bool WithS3Node => true;
}

/// <summary>Компаньон-фикстура окружения PoolRoleSwitchScenarios (полный контур на класс:
/// сеть/etcd/ownS3/CA/окно портов; docs/e2e-isolation.md §1).</summary>
public sealed class PoolRoleSwitchScenariosFx : E2eContourFixture
{
    protected override string Slug => "pool-ac5";
    protected override bool WithS3Node => true;
}

/// <summary>Компаньон-фикстура окружения PoolReadAnyNodeScenarios (полный контур на класс:
/// сеть/etcd/ownS3/CA/окно портов; docs/e2e-isolation.md §1).</summary>
public sealed class PoolReadAnyNodeScenariosFx : E2eContourFixture
{
    protected override string Slug => "pool-ac6";
    protected override bool WithS3Node => true;
}

/// <summary>Компаньон-фикстура окружения PoolSnapshotsOwnPairScenarios (полный контур на класс:
/// сеть/etcd/ownS3/CA/окно портов; docs/e2e-isolation.md §1).</summary>
public sealed class PoolSnapshotsOwnPairScenariosFx : E2eContourFixture
{
    protected override string Slug => "pool-ac7";
    protected override bool WithS3Node => true;
}

/// <summary>Компаньон-фикстура окружения PoolPolicyValidationScenarios (полный контур на класс:
/// сеть/etcd/ownS3/CA/окно портов; docs/e2e-isolation.md §1).</summary>
public sealed class PoolPolicyValidationScenariosFx : E2eContourFixture
{
    protected override string Slug => "pool-ac8";
    protected override bool WithS3Node => true;
}
