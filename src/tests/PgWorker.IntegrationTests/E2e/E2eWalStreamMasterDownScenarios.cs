using System.Text.Json;
using Npgsql;
using PgWorker.Core.Templates;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.IntegrationTests.Docker;
using PgWorker.Provisioning.Sql;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E WAL-потока, отказ мастера (t24/ревизия 8 — разбиение E2eBackupScenarios):
// монолитный одиночный факт (~11 мин, §7) — отдельный класс.
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
public class E2eWalStreamMasterDownScenarios
{
    private const string Bucket = "pgworker-backups";

    // Окружение Fact'а (своя сеть/etcd/MinIO); создаётся в начале каждого сценария.
    private E2eEnvironment Fx = null!;

    private string Endpoint => Fx.EtcdEndpoint;

    private EtcdGateway G => Fx.Gateway;

    // после promote воркер пересоздаёт агентов на новых ролях; ключ не BROKEN.
    [Fact]
    public async Task WalStream_MasterDown_ReplicaAgentContinues()
    {
        // Arrange — окружение, оба агента живы, нагрузка фоном
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        var (fx, app, cluster, adminDsn, masterNode) = await StartWalScenarioAsync("wal-ac4", "shopac4", ct);
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
        var bothAgents = await E2ePhase.WaitAsync(Fx, "wal-ac4-agents-up", async () =>
        {
            var ps = await Fx.RunDockerAsync(
            [
                "ps", "--filter", $"name=pgw-backup-wal-{cluster}-shard1-",
                "--format", "{{.Names}} {{.State}}",
            ], ct);
            return ps.Split('\n', StringSplitOptions.RemoveEmptyEntries).Count(l => l.EndsWith("running")) >= 2;
        }, TimeSpan.FromSeconds(300), ct);
        bothAgents.Should().BeTrue("оба агента до смерти мастера");
        using var load = new CancellationTokenSource();
        var loader = BackgroundWalLoadAsync(adminDsn, load.Token);
        var beforeList = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
        beforeList.IsSuccess.Should().BeTrue();

        try
        {
            // Act — жёсткая потеря мастер-ноды (multi-host-риск «машина отключилась
            // целиком»): никто не делает управляемый switchover — Patroni сам
            // проводит failover силами выжившей реплики.
            var (_, _, currentMaster) = await MasterPgAsync(cluster, "shard1", ct);
            Console.WriteLine($"[PHASE] wal-ac4: docker stop pgw-{cluster}-shard1-{currentMaster}");
            await Fx.RunDockerAsync(["stop", $"pgw-{cluster}-shard1-{currentMaster}"], ct);
            var (_, _, newMasterNode) = await MasterPgAsync(cluster, "shard1", ct);
            newMasterNode.Should().NotBe(currentMaster, "после смерти мастера primary — реплика");
            Console.WriteLine($"[PHASE] wal-ac4: новый primary {newMasterNode}");

            // Фаза стабилизации: надзор пометил упавшую ноду unreachable и чинит её
            // repair-контуром (восстановление ноды — продуктовое поведение, минуты);
            // WalStream честно уходит в restore-гвард, пока шард восстанавливается.
            // Ждём снятия unreachable циклом 30-секундных окон (каждое окно —
            // проверка факта; суммарный лимит 600 c на восстановление ноды).
            // Окно живёт ВНУТРИ condition — проверка факта раз в 30 с, как раньше;
            // статус/номер окна — в замыкании, прогресс форматирует их БЕЗ новых
            // чтений; тик E2ePhase печатается по завершении condition — фактическая
            // частота ≈ строка на окно (t29 §3.9). Ассерта на результат цикла нет —
            // итог только в строке телеметрии (новый ассерт не добавляем).
            TestContext.Current.TestOutputHelper?.WriteLine(
                "[PHASE] wal-ac4: ожидание снятия unreachable (repair-контур надзора)");
            var stillUnreachable = true;
            var window = 0;
            var repaired = await E2ePhase.WaitAsync(Fx, "wal-ac4-repair", async () =>
            {
                var workKv = await GetOrNullAsync($"/pgworker/work/{cluster}");
                stillUnreachable = workKv?.Value?.Contains("unreachable") == true;
                if (!stillUnreachable)
                    return true;
                window++;
                await Task.Delay(TimeSpan.FromSeconds(30), ct); // 30-с окно проверки — дословно, полл не учащается
                return false;
            }, TimeSpan.FromSeconds(600), ct,
                progress: () => Task.FromResult($"unreachable={(stillUnreachable ? "да" : "нет")}, окно {window}"));
            TestContext.Current.TestOutputHelper?.WriteLine(
                $"[PHASE] wal-ac4: repair-фаза завершена/снята (repaired={repaired}) — замеры ключа");

            // Assert — доставка продолжается (новые сегменты после смерти мастера),
            // цепочка непрерывна (CheckWithRestart), ключ не BROKEN
            var grew = await E2ePhase.WaitAsync(Fx, "wal-ac4-delivery-grew", async () =>
            {
                var list = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
                return list.IsSuccess && list.Value.Count > beforeList.Value.Count;
            }, TimeSpan.FromSeconds(300), ct,
                // Динамика листинга wal/-префикса по тикам (t29 §3.9): растёт ли
                // число сегментов; S3-листинг не чаще интервала тиков (5 с).
                progress: async () =>
                {
                    var listed = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
                    return listed.IsSuccess
                        ? $"objects={listed.Value.Count} (before={beforeList.Value.Count})"
                        : "listing=ошибка";
                });
            grew.Should().BeTrue("агент реплики продолжает доставку после смерти мастера");

            // Чтение ключа — поллингом (etcd-транспорт может дать transient-отказ:
            // ReadAsync Failed → Value null; факт ждём, не фиксируем одноразовым чтением)
            PgWorker.Etcd.Parsing.WalStreamState? keyState = null;
            var keySeen = await E2ePhase.WaitAsync(Fx, "wal-ac4-key-seen", async () =>
            {
                var read = await writer.ReadAsync(cluster, "shard1", ct);
                if (!read.IsSuccess || read.Value is null)
                    return false;
                keyState = read.Value;
                return true;
            }, TimeSpan.FromSeconds(30), ct);
            keySeen.Should().BeTrue("wal-ключ обязан существовать после failover");
            keyState!.State.Should().NotBe(
                PgWorker.Etcd.Parsing.WalStreamStatus.Broken, "ключ не BROKEN (AC4)");
            var list = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
            var chain = PgWorker.Backups.WalChain.CheckWithRestart(
                PgWorker.Backups.WalFileName.TryParse(keyState!.ChainStartSegment)!.Value,
                list.Value!.Select(o => o.Name));
            chain.IsContinuous.Should().BeTrue(chain.GapError ?? "цепочка склеена через TLI");

            // После promote воркер пересоздаёт агентов на новых ролях: running-агент
            // новой мастер-ноды ≤ 300 c (смена источника — пересоздание).
            // Живость агентов — в failed/slow-phase-сбор, docker ps в тиках
            // запрещён (t29 §4.1).
            var newAgent = await E2ePhase.WaitAsync(Fx, "wal-ac4-new-agent", async () =>
            {
                var ps = await Fx.RunDockerAsync(
                    ["ps", "--format", "{{.Names}} {{.State}}",
                     "--filter", $"name=pgw-backup-wal-{cluster}-shard1-{newMasterNode}"], ct);
                return ps.Contains("running");
            }, TimeSpan.FromSeconds(300), ct);
            newAgent.Should().BeTrue($"агент новой мастер-ноды {newMasterNode} running");
        }
        catch (Exception ex)
        {
            Fx.MarkFailed(); // артефакты телеметрии переживают teardown
            await WalScenarioDiagDumpAsync(cluster, "AC4: " + ex.Message);
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
                // фон умер вместе с соединением к мёртвому мастеру — ожидаемо
            }
        }
    }

    // AAA (AC5): promote реплики — TLI ≥ 2; объект wal/<tli>.history в S3
    // (приёмник грузит TIMELINE_HISTORY — вердикт Ф2); цепочка CheckWithRestart
    // непрерывна; ключ ACTIVE.

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
        await E2ePhase.WaitAsync(Fx, "wal-ac4-tli-ready", async () =>
        {
            foreach (var (key, addr) in entries)
            {
                try
                {
                    using var response = await PatroniHttp.GetAsync(
                        $"https://localhost:{addr.GetProperty("patroni").GetInt32()}/cluster", ct);
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
            // Дамп — в каталог телеметрии прогона (t29 §3.9): рядом с
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
                        $"https://localhost:{addr.GetProperty("patroni").GetInt32()}/cluster", ct);
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
        var provisioned = await E2ePhase.WaitAsync(Fx, "wal-ac4-provisioning",
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
            await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_mem", "2Gi", null, ct);
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
    // t22: Patroni REST нод — https (цепочка к per-contour CA, без hostname)
    private static readonly HttpClient PatroniHttp = E2eEnvironment.CreatePatroniHttpsClient();

    private async Task<(string Host, int Port, string Node)> MasterPgAsync(string cluster, string shard, CancellationToken ct)
    {
        var kv = await G.GetAsync(Endpoint, $"/pgworker/portalloc/{cluster}", ct);
        kv.Value.Should().NotBeNull("portalloc пишется при provisioning");
        var entries = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(kv.Value!.Value)!;
        string? primary = null;
        var resolved = await E2ePhase.WaitAsync(Fx, "wal-ac4-master-resolved", async () =>
        {
            foreach (var (key, addr) in entries
                         .Where(p => p.Key.StartsWith($"{shard}/", StringComparison.Ordinal))
                         .OrderBy(p => p.Key, StringComparer.Ordinal))
            {
                try
                {
                    using var response = await PatroniHttp.GetAsync(
                        $"https://localhost:{addr.GetProperty("patroni").GetInt32()}/primary", ct);
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
