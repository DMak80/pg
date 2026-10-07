using System.Text.Json;
using Npgsql;
using PgWorker.Core.Templates;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.IntegrationTests.Docker;
using PgWorker.Provisioning.Sql;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E WAL-потока, promote/TLI (t24/ревизия 8 — разбиение E2eBackupScenarios):
// монолитный одиночный факт (2-6 мин) — отдельный класс.
// E2E полных бэкапов (t02, spec §7.1–7.4): изолированное окружение E2eEnvironment
// (своя docker-сеть, свой etcd, СВОЙ MinIO — withMinio:true; порт динамический)
// + образ pgworker-backup:e2e + живой кластер воркером с Backups:Enabled=true.
// COMPLETED с полными полями и объектами в S3; FAILED по недоступному S3 с
// переснятием новым id; deprovisioning чистит джобы и префикс; ротация
// backup_password включает backup_exec. Каждый Fact — своё окружение: методы
// оставляют после себя Active-кластеры, и без per-method изоляции воркер
// следующего Fact'а подхватывал бы чужие джобы (инцидент Release). Имя кластера сценария —
// {slug}{Fx.ClusterTag} (уникально на прогон, docs/e2e-isolation.md §1):
// движковые контейнеры/тома pgw-*-<C>-* опознаются teardown'ом окружения.
public class E2eWalStreamPromoteScenarios
{
    private const string Bucket = "pgworker-backups";

    // Окружение Fact'а (своя сеть/etcd/MinIO); создаётся в начале каждого сценария.
    private E2eEnvironment Fx = null!;

    private string Endpoint => Fx.EtcdEndpoint;

    private EtcdGateway G => Fx.Gateway;

    // непрерывна; ключ ACTIVE.
    [Fact]
    public async Task WalStream_Promote_TliHistory_ChainGlued()
    {
        // Arrange — окружение, оба агента, нагрузка, смерть мастера → promote
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        var (fx, app, cluster, adminDsn, masterNode) = await StartWalScenarioAsync("wal-ac5", "shopac5", ct);
        await using var envOwner = fx;
        await using var appOwner = app;
        var hostEndpoint = Fx.S3Endpoint.Replace(
            "host.docker.internal:", "localhost:", StringComparison.Ordinal);
        await using var backupS3 = new PgWorker.Backups.BackupS3(
            new PgWorker.Backups.BackupsRuntimeOptions
            {
                Enabled = true,
                S3Endpoint = hostEndpoint,
                S3Bucket = Bucket,
                S3AccessKey = "minioadmin",
                S3SecretKey = "minioadmin",
                S3PathStyle = true,
            });
        var writer = new PgWorker.Backups.WalStatusWriter(G, [Endpoint]);
        var bothAgents = await E2ePhase.WaitAsync(Fx, "wal-agents-up", async () =>
        {
            var ps = await Fx.RunDockerAsync(
            [
                "ps", "--filter", $"name=pgw-backup-wal-{cluster}-shard1-",
                "--format", "{{.Names}} {{.State}}",
            ], ct);
            return ps.Split('\n', StringSplitOptions.RemoveEmptyEntries).Count(l => l.EndsWith("running")) >= 2;
        }, TimeSpan.FromSeconds(300), ct);
        bothAgents.Should().BeTrue("оба агента до promote");
        using var load = new CancellationTokenSource();
        var loader = BackgroundWalLoadAsync(adminDsn, load.Token);
        await GenerateWalAsync(adminDsn, ct); // база цепочки TLI 1 до promote

        try
        {
            // Act — жёсткая потеря мастер-ноды: Patroni failover реплики открывает
            // новый timeline (механика .history та же, что при любом promote)
            var (_, _, currentMaster) = await MasterPgAsync(cluster, "shard1", ct);
            Console.WriteLine($"[PHASE] wal-ac5: docker stop pgw-{cluster}-shard1-{currentMaster}");
            await Fx.RunDockerAsync(["stop", $"pgw-{cluster}-shard1-{currentMaster}"], ct);
            var (newMasterHost, newMasterPort, _) = await MasterPgAsync(cluster, "shard1", ct); // ждём новый primary
            var tli = await PrimaryTimelineAsync(cluster, "shard1", ct);
            tli.Should().BeGreaterThanOrEqualTo(2u, "failover открывает новый timeline");

            // TLI-граница обязана попасть в ЗАКРЫТЫЙ сегмент: фоновый лоадер умер
            // вместе со старым мастером, idle-база закрывает сегмент минутами —
            // форсируем переключение на новом мастере (образец Drill_Failed_OnCorruptedWal).
            var newMasterDsn = DatabaseProvisioner.BuildAdminDsn(newMasterHost, newMasterPort, cluster,
                new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
            await SwitchWalsAsync(newMasterDsn, 2, ct);

            // Прогресс листинга S3 (t29 §3.4): размер, max-TLI сегментов, наличие
            // .history; S3-листинг в тиках — не чаще 1 раза в 5 с (интервал
            // тиков). Делегат общий для фаз history/склейки.
            Func<Task<string>> walListingProgress = async () =>
            {
                var listed = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
                if (!listed.IsSuccess || listed.Value.Count == 0)
                    return "listing=пусто";
                var names = listed.Value.Select(o => o.Name).ToList();
                var maxTli = names.Where(n => !n.EndsWith(".history", StringComparison.Ordinal))
                    .Select(n => Convert.ToUInt32(n[..8], 16)).DefaultIfEmpty(0u).Max();
                return $"objects={names.Count}, maxTli=0x{maxTli:x}, history={(names.Any(n => n.EndsWith(".history", StringComparison.Ordinal)) ? "есть" : "нет")}";
            };

            // Assert — .history нового TLI в S3 (приёмник запрашивает TIMELINE_HISTORY)
            var history = await E2ePhase.WaitAsync(Fx, "wal-history-present", async () =>
            {
                var listed = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
                return listed.IsSuccess && listed.Value.Any(o => o.Name == $"{tli:x8}.history");
            }, TimeSpan.FromSeconds(300), ct, progress: walListingProgress);
            history.Should().BeTrue($"wal/{tli:x8}.history загружен приёмником (AC5)");

            // Цепочка склеена через TLI-переход; ключ ACTIVE
            var glueOk = await E2ePhase.WaitAsync(Fx, "wal-chain-glued", async () =>
            {
                var read = await writer.ReadAsync(cluster, "shard1", ct);
                if (!read.IsSuccess || read.Value is null) return false;
                var listed = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
                if (!listed.IsSuccess || listed.Value.Count == 0) return false;
                var chainStart = PgWorker.Backups.WalFileName.TryParse(
                    read.Value.ChainStartSegment);
                if (chainStart is null) return false;
                var chain = PgWorker.Backups.WalChain.CheckWithRestart(
                    chainStart.Value, listed.Value.Select(o => o.Name));
                return chain.IsContinuous
                    && read.Value.State == PgWorker.Etcd.Parsing.WalStreamStatus.Active;
            }, TimeSpan.FromSeconds(300), ct, progress: walListingProgress);
            glueOk.Should().BeTrue("цепочка CheckWithRestart непрерывна, ключ ACTIVE (AC5)");
        }
        catch (Exception ex)
        {
            Fx.MarkFailed(); // артефакты телеметрии переживают teardown
            await WalScenarioDiagDumpAsync(cluster, "AC5: " + ex.Message);
            throw;
        }
        finally
        {
            await load.CancelAsync();
            try
            {
                await loader;
            }
            catch
            {
                // фон умер вместе с соединением — ожидаемо после смерти мастера
            }
        }
    }

    // AAA: on_create-verify (AC1): COMPLETED → verify PENDING → verify-джоб
    // pgw-backup-verify-* → verify.state=OK + checked_unix; парсеры без parseErrors

    private async Task<string?> AgentContainerIdAsync(string agentName, CancellationToken ct)
    {
        var ids = await Fx.RunDockerAsync(
            ["ps", "-a", "--format", "{{.ID}}", "--filter", $"name=^{agentName}$"], ct);
        return ids.Split('\n', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault()?.Trim();
    }

    // TLI текущего primary шарда: Patroni GET /cluster ноды (timeline члена-лидера).

    private async Task<uint> PrimaryTimelineAsync(string cluster, string shard, CancellationToken ct)
    {
        var kv = await G.GetAsync(Endpoint, $"/pgworker/portalloc/{cluster}", ct);
        kv.Value.Should().NotBeNull("portalloc пишется при provisioning");
        var entries = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(kv.Value!.Value)!
            .Where(p => p.Key.StartsWith($"{shard}/", StringComparison.Ordinal)).ToList();
        // Опрос ВСЕХ нод (после docker stop мастер-ноды её patroni-порт закрыт);
        // ждём именно TLI ≥ 2: promote мог случиться только-что — /cluster члена
        // показывает его timeline не мгновенно (длительность failover-процесса).
        uint tli = 0;
        await E2ePhase.WaitAsync(Fx, "wal-tli-ready", async () =>
        {
            foreach (var (key, addr) in entries)
            {
                try
                {
                    using var response = await PatroniHttp.GetAsync(
                        $"http://localhost:{addr.GetProperty("patroni").GetInt32()}/cluster", ct);
                    if (!response.IsSuccessStatusCode) continue;
                    using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct));
                    foreach (var member in doc.RootElement.GetProperty("members").EnumerateArray())
                    {
                        var role = member.GetProperty("role").GetString();
                        if (role is "master" or "leader" or "primary"
                            && member.TryGetProperty("timeline", out var t)
                            && t.TryGetUInt32(out var value)
                            && value > tli)
                        {
                            tli = value;
                        }
                    }
                }
                catch (Exception)
                {
                    // нода ещё не готова — следующая
                }
            }

            return tli >= 2;
        }, TimeSpan.FromSeconds(120), ct,
            progress: () => Task.FromResult($"tli={tli}"));
        return tli;
    }

    /// <summary>Гарантированный дамп диагностики Wal-сценария В САМОМ catch (до
    /// teardown): агенты, wal/work-ключи, Patroni, хвост host.log воркера —

    private async Task WalScenarioDiagDumpAsync(string cluster, string reason)
    {
        try
        {
            var agentsPs = await Fx.RunDockerAsync(
                ["ps", "-a", "--format", "{{.Names}} {{.State}}",
                 "--filter", $"name=pgw-backup-wal-{cluster}-"], TestContext.Current.CancellationToken);
            var walKv = await GetOrNullAsync($"/pgworker/backups/{cluster}/shard1/wal");
            var workKv = await GetOrNullAsync($"/pgworker/work/{cluster}");
            var claimKv = await GetOrNullAsync($"/pgworker/claims/{cluster}");
            var patroni = await PatroniClusterDumpAsync(cluster, TestContext.Current.CancellationToken);
            var hostLog = "";
            foreach (var host in Fx.Hosts)
                try
                {
                    var lines = File.ReadAllLines(Path.Combine(host.SnapshotsDir, "host.log"));
                    var noise = new[]
                    {
                        "ClientHandler", "LogicalHandler", "HTTP request", "HTTP response",
                        "End processing HTTP", "Received HTTP", "Sending HTTP",
                        "PgtuneInputsFactory", "wal_compression", "AwaitableSocketAsyncEventArgs",
                    };
                    var interesting = lines
                        .Where(l => !noise.Any(n => l.Contains(n, StringComparison.Ordinal)))
                        .Where(l => l.Contains("backup", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("wal", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("agent", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("error", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("warn", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("fail", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("exception", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("claim", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("supervise", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("repair", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("evacuat", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("provision", StringComparison.OrdinalIgnoreCase))
                        .ToList();
                    var opLines = lines
                        .Where(l => l.Contains("backup-wal", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("WalStreamProcess", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("supervise", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("repair", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("provision ", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("эвакуа", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("REBUILDING", StringComparison.OrdinalIgnoreCase))
                        .ToList();
                    hostLog += $"\n== {host.Name} OPS({opLines.Count}):\n{string.Join("\n", opLines)}" +
                        $"\nTAIL({interesting.Count}):\n" +
                        string.Join("\n", interesting[^Math.Min(40, interesting.Count)..]);
                }
                catch (Exception e)
                {
                    hostLog += $"\n== {host.Name}: host.log недоступен: {e.Message}";
                }

            var agentLogs = "";
            foreach (var line in agentsPs.Split('\n', StringSplitOptions.RemoveEmptyEntries))
            {
                var name = line.Split(' ')[0];
                var logs = await Fx.RunDockerAsync(["logs", "--tail", "60", name],
                    TestContext.Current.CancellationToken);
                agentLogs += $"\n== {name}:\n{logs}";
            }

            var dump =
                $"reason={reason}\nagents=[{agentsPs.Replace('\n', ';')}]\n" +
                $"wal=[{walKv?.Value ?? "-"}]\nwork=[{workKv?.Value ?? "-"}]\n" +
                $"claim=[{claimKv?.Value ?? "-"}]\npatroni={patroni}\nAGENT.LOGS:{agentLogs}\nHOST.LOG:{hostLog}\n";
            // Дамп — в каталог телеметрии прогона (t29 §3.4): рядом с
            // phases.log/docker-логами, а не в /tmp мимо артефактов.
            var dumpPath = Path.Combine(Fx.ArtifactsDir, $"wal-diag-{cluster}.txt");
            await File.WriteAllTextAsync(dumpPath, dump);
            Console.WriteLine($"[DIAG] дамп: {dumpPath}");
        }
        catch
        {
            // дамп — лучшие усилия, не маскируем исходный сбой сценария
        }
    }

    private async Task<string> PatroniClusterDumpAsync(string cluster, CancellationToken ct)
    {
        try
        {
            var kv = await G.GetAsync(Endpoint, $"/pgworker/portalloc/{cluster}", ct);
            var entries = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(kv.Value!.Value)!
                .Where(p => p.Key.StartsWith("shard1/", StringComparison.Ordinal)).ToList();
            foreach (var (key, addr) in entries)
            {
                try
                {
                    using var response = await PatroniHttp.GetAsync(
                        $"http://localhost:{addr.GetProperty("patroni").GetInt32()}/cluster", ct);
                    if (!response.IsSuccessStatusCode) continue;
                    var body = await response.Content.ReadAsStringAsync(ct);
                    return $"[{key}] " + body[..Math.Min(700, body.Length)];
                }
                catch (Exception)
                {
                    // нода недоступна — следующая
                }
            }

            return "<ни одна нода shard1 не отвечает /cluster>";
        }
        catch (Exception e)
        {
            return $"<patroni недоступен: {e.Message}>";
        }
    }

    /// <summary>Подъём полного WAL-контура сценария: окружение с MinIO, кластер,
    /// воркер (Backups:Enabled + Wal:AgentImage), ожидание provisioning, DSN мастера.
    /// </summary>
    // Владение окружением и воркером — у Fact (DisposeAsync при любом исходе);

    private async Task<(E2eEnvironment Env, HostInstance App, string Cluster, string AdminDsn, string MasterNode)>
        StartWalScenarioAsync(string slug, string clusterPrefix, CancellationToken ct)
    {
        var phaseStart = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        Console.WriteLine($"[PHASE] {slug}: start (unix={phaseStart})");
        var fx = await E2eEnvironment.StartAsync(slug, withMinio: true, ct: ct);
        Fx = fx;
        Console.WriteLine($"[PHASE] {slug}: окружение поднято за " +
            (DateTimeOffset.UtcNow.ToUnixTimeSeconds() - phaseStart) + " c (вкл. Release-сборку и образы)");
        var cluster = $"{clusterPrefix}{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        var app = await StartWalHostAsync(slug, ct);
        var provisioned = await E2ePhase.WaitAsync(Fx, "wal-provisioning",
            () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(360), ct);
        provisioned.Should().BeTrue("provisioning обязан дойти до DONE до WAL-нагрузки");
        Console.WriteLine($"[PHASE] {slug}: provisioning DONE за " +
            (DateTimeOffset.UtcNow.ToUnixTimeSeconds() - phaseStart) + " c от старта");
        var (_, pgPort, masterNode) = await MasterPgAsync(cluster, "shard1", ct);
        var adminDsn = DatabaseProvisioner.BuildAdminDsn("localhost", pgPort, cluster,
            new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
        return (fx, app, cluster, adminDsn, masterNode);
    }

    // AAA (AC2 — ядро задачи): под WAL-нагрузкой docker rm -f агента мастера —
    // супервиз пересоздаёт агента, цепочка S3 непрерывна, BROKEN не возникает,

    // Воркер с ВКЛЮЧЁННОЙ подсистемой бэкапов: t02-комплект джобов + t03 Wal-поток.
    // S3 для воркера — published порт (localhost), для агентов/джобов — advertised
    // (host.docker.internal); пороги WAL-контроля — короткие.
    private Task<HostInstance> StartWalHostAsync(string name, CancellationToken ct)
        => Fx.StartHostAsync(name, extraEnv: new Dictionary<string, string>
        {
            ["PgWorker__Backups__Enabled"] = "true",
            ["PgWorker__Backups__S3__Endpoint"] = Fx.S3Endpoint.Replace(
                "host.docker.internal:", "localhost:", StringComparison.Ordinal),
            ["PgWorker__Backups__S3__AdvertisedEndpoint"] = Fx.S3Endpoint,
            ["PgWorker__Backups__S3__Bucket"] = Bucket,
            ["PgWorker__Backups__S3__AccessKey"] = "minioadmin",
            ["PgWorker__Backups__S3__SecretKey"] = "minioadmin",
            ["PgWorker__Backups__Job__Image"] = E2eEnvironment.JobImage,
            ["PgWorker__Backups__Wal__AgentImage"] = E2eEnvironment.WalImage,
            ["PgWorker__Backups__Retry__BaseSec"] = "2",
            ["PgWorker__Backups__Retry__MaxSec"] = "4",
            ["PgWorker__Backups__Wal__VerifyIntervalSec"] = "2",
            ["PgWorker__Backups__Wal__StaleSec"] = "600",
            ["PgWorker__Backups__Wal__LagMaxSegments"] = "100000",
        }, ct: ct);

    // Закрытие count сегментов на мастере (логические сообщения + pg_switch_wal):
    // idle-база закрывает сегмент 16 МБ минутами — TLI-границы/хвосты форсируются
    // (образец E2eRestoreScenarios.SwitchWalsAsync).
    private static async Task SwitchWalsAsync(string adminDsn, int count, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(adminDsn);
        await conn.OpenAsync(ct);
        for (var i = 0; i < count; i++)
        {
            await using var switchWal = new NpgsqlCommand("SELECT pg_switch_wal()", conn);
            await switchWal.ExecuteScalarAsync(ct);
        }
    }

    private async Task<IReadOnlyList<Kv>> FullKeysAsync(string cluster, string shard)
        => (await G.RangeAsync(Endpoint, $"/pgworker/backups/{cluster}/{shard}/full/",
            TestContext.Current.CancellationToken)).Value;

    private async Task<Kv?> GetOrNullAsync(string key)
        => (await G.GetAsync(Endpoint, key, TestContext.Current.CancellationToken)).Value;

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

    // Published pg-порт мастера шарда из portalloc (host-клиент: localhost).
    // Резолв фактического primary — по пробам Patroni /primary (t02-подход
    // WaitForMasterAsync: master-ключ host:0 при EnableDoorman=false
    // недискриминантен); primary появляется после dsn/RUNNING — ждём.
    private static readonly HttpClient PatroniHttp = new() { Timeout = TimeSpan.FromSeconds(3) };

    private async Task<(string Host, int Port, string Node)> MasterPgAsync(string cluster, string shard, CancellationToken ct)
    {
        var kv = await G.GetAsync(Endpoint, $"/pgworker/portalloc/{cluster}", ct);
        kv.Value.Should().NotBeNull("portalloc пишется при provisioning");
        var entries = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(kv.Value!.Value)!;
        string? primary = null;
        var resolved = await E2ePhase.WaitAsync(Fx, "wal-master-resolved", async () =>
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
        return (entry.GetProperty("host").GetString()!, entry.GetProperty("pg").GetInt32(),
            primary!);

    }

    // Нагрузка под admin/superuser: CREATE TABLE + 30 циклов INSERT больших строк

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

    private static async Task BackgroundWalLoadAsync(string adminDsn, CancellationToken ct)
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
                // Раз в ~15 c: сегменты закрываются ЗАПОЛНЕННЫМИ (сотни пустых
                // 16 MiB-put'ов секундного цикла давали гигабайты перекладки).
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

    // + pg_switch_wal (superuser-only) — форсированное закрытие сегментов 16 МБ.
    private static async Task GenerateWalAsync(string adminDsn, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(adminDsn);
        await conn.OpenAsync(ct);
        try
        {
            await using (var create = new NpgsqlCommand(
                "CREATE TABLE IF NOT EXISTS wal_load(id bigserial, payload text)", conn))
                await create.ExecuteNonQueryAsync(ct);
        }
        catch (Npgsql.PostgresException e) when (e.SqlState == "23505")
        {
            // Гонка CREATE IF NOT EXISTS двух сессий (catalog-индекс): таблица
            // создаётся другой сессией — не ошибка нагрузки.
        }
        for (var i = 0; i < 30; i++)
        {
            await using var insert = new NpgsqlCommand(
                "INSERT INTO wal_load(payload) SELECT repeat('x', 1048576) FROM generate_series(1, 8)", conn);
            await insert.ExecuteNonQueryAsync(ct);
            await using var switchWal = new NpgsqlCommand("SELECT pg_switch_wal()", conn);
            await switchWal.ExecuteScalarAsync(ct);
        }
    }

}
