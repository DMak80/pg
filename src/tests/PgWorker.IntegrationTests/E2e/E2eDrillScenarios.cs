using System.Text.Json;
using Npgsql;
using PgWorker.Core.Templates;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.IntegrationTests.Docker;
using PgWorker.Provisioning.Sql;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E тренировочных восстановлений (t24/ревизия 8 — разбиение
// E2eBackupScenarios): Drill_Succeeds/Drill_Failed (~2,5 мин суммарно).
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
public class E2eDrillScenarios
{
    private const string Bucket = "pgworker-backups";

    // Окружение Fact'а (своя сеть/etcd/MinIO); создаётся в начале каждого сценария.
    private E2eEnvironment Fx = null!;

    private string Endpoint => Fx.EtcdEndpoint;

    private EtcdGateway G => Fx.Gateway;

    // контур снесён (контейнер/volume отсутствуют, phase снят).
    [Fact]
    public async Task Drill_Succeeds_CleansUp()
    {
        // Arrange — кластер bkdrll<тег> + policy (verify off, drill включён) + воркер.
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("bk-drill", withMinio: true, ct: ct);
        Fx = fx;
        var cluster = $"bkdrll{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        await G.PutAsync(Endpoint, $"/pgworker/backups/{cluster}/policy",
            """{"full_max_age_sec":86400,"verify":{"on_create":false},"drill":{"interval_days":1}}""", null, ct);
        await using var app = await StartBackupHostAsync("bkdrill", ct);

        // Act 1 — COMPLETED-полный (бюджет 300 c)
        var completed = await E2eFixture.WaitForAsync(
            async () => (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains("COMPLETED")),
            TimeSpan.FromSeconds(300), ct);
        completed.Should().BeTrue("полный обязан сняться");

        // Act 2 — дрилл дошёл до SUCCEEDED и довёл снос (чистый итог: phase снят)
        // — бюджет 600 c: скачать + накат; phase снимается тиком сноса ПОСЛЕ
        // вердикта, поэтому ждём именно БЕЗ phase (не гонка за вердиктом).
        var succeeded = await E2eFixture.WaitForAsync(async () =>
        {
            var kv = await G.GetAsync(Endpoint, $"/pgworker/backups/{cluster}/shard1/drill", ct);
            return kv.Value?.Value.Contains("\"SUCCEEDED\"") == true
                   && !kv.Value.Value.Contains("phase");
        }, TimeSpan.FromSeconds(600), ct);
        if (!succeeded)
        {
            // Телеметрия упавшего дрилла: дамп до teardown (канон e2e-launch)
            var drillKv = await GetOrNullAsync($"/pgworker/backups/{cluster}/shard1/drill");
            var walKv = await GetOrNullAsync($"/pgworker/backups/{cluster}/shard1/wal");
            var workKv = await GetOrNullAsync($"/pgworker/work/{cluster}");
            var fulls = await FullKeysAsync(cluster, "shard1");
            var hostLog = "";
            foreach (var host in Fx.Hosts)
                try
                {
                    var lines = File.ReadAllLines(Path.Combine(host.SnapshotsDir, "host.log"));
                    var interesting = lines.Where(l =>
                        !l.Contains("ClientHandler") && !l.Contains("LogicalHandler")
                        && !l.Contains("HTTP request") && !l.Contains("HTTP response")
                        && !l.Contains("End processing HTTP") && !l.Contains("Received HTTP")
                        && !l.Contains("Sending HTTP") && !l.Contains("PgtuneInputsFactory")
                        && !l.Contains("wal_compression")).ToList();
                    hostLog += $"\n== {host.Name} ({interesting.Count}):\n" +
                        string.Join("\n", interesting[^Math.Min(60, interesting.Count)..]);
                }
                catch (Exception e)
                {
                    hostLog += $"\n== {host.Name}: host.log недоступен: {e.Message}";
                }

            await File.WriteAllTextAsync($"/tmp/pgw-diag-{cluster}.txt",
                $"reason=drill not SUCCEEDED\nfulls=[{string.Join("; ", fulls.Select(f => f.Key + "=" + f.Value[..Math.Min(160, f.Value.Length)]))}]\n" +
                $"drill=[{drillKv?.Value ?? "-"}]\nwal=[{walKv?.Value ?? "-"}]\nwork=[{workKv?.Value ?? "-"}]\nHOST.LOG:{hostLog}\n",
                TestContext.Current.CancellationToken);
        }

        succeeded.Should().BeTrue("первый дрилл стартует немедленно и обязан выйти из recovery");

        // Assert — restored_to_lsn в ключе; чистый итог: без phase
        var drill = (await G.GetAsync(Endpoint, $"/pgworker/backups/{cluster}/shard1/drill", ct)).Value!.Value;
        drill.Should().Contain("\"restored_to_lsn\":\"", "LSN восстановления фиксируется");
        drill.Should().NotContain("phase", "чистый терминальный итог — контур снесён");

        // Assert — прод-ноды шарда живы весь дрилл (AC2: изоляция — Patroni-контур не тронут)
        var nodes = await Fx.RunDockerAsync(
            ["ps", "--format", "{{.Names}}", "--filter", $"name=pgw-{cluster}-shard1-"], ct);
        nodes.Length.Should().BeGreaterThan(0, "дрилл не демонтирует прод-ноды шарда (state RUNNING)");

        // Assert — контейнер и volume дрилла отсутствуют (ассерт чистоты, AC4/AC2)
        var cleaned = await E2eFixture.WaitForAsync(async () =>
        {
            var containers = await Fx.RunDockerAsync(
                ["ps", "-a", "--format", "{{.Names}}", "--filter", $"name=pgw-backup-drill-{cluster}-"], ct);
            var volumes = await Fx.RunDockerAsync(
                ["volume", "ls", "-q", "--filter", $"name=pgw-backup-drill-{cluster}-"], ct);
            return containers.Length == 0 && volumes.Length == 0;
        }, TimeSpan.FromSeconds(60), ct);
        cleaned.Should().BeTrue("drill-контур сносится после итога");

        // Assert — оба парсера без ошибок; панельные правила молчат на успехе (AC5)
        var kvs = (await G.RangeAsync(Endpoint, $"/pgworker/backups/{cluster}/", ct)).Value;
        var parsed = BackupsParser.Parse(kvs, out var parseErrors);
        parseErrors.Should().BeEmpty();
        var panel = AdminPanel.Etcd.Parsing.BackupsParser.Parse(kvs);
        panel.Errors.Should().BeEmpty();
        var panelCluster = panel.Clusters.Single(c => c.Cluster == cluster);
        var failedAlerts = new AdminPanel.Core.Alerting.Rules.BackupDrillFailedRule().Evaluate(
            SnapshotWith(panelCluster, cluster, "shard1"), DefaultAlertContext()).ToList();
        failedAlerts.Should().BeEmpty("SUCCEEDED — без алерта провала");
        var staleAlerts = new AdminPanel.Core.Alerting.Rules.BackupDrillStaleRule().Evaluate(
            SnapshotWith(panelCluster, cluster, "shard1"), DefaultAlertContext()).ToList();
        staleAlerts.Should().BeEmpty("свежий SUCCEEDED — без алерта молчания");
    }

    // AAA (AC1/AC3/AC5): дыра WAL-цепочки → валидационный FAILED без джоба
    // (контейнера нет, ключ без phase) + панельный алерт backup-drill-failed.
    // Дрилы выключены глобально env-ом; включаются policy-полем — тестирует и
    // применение policy на лету.
    [Fact]
    public async Task Drill_Failed_OnCorruptedWal()
    {
        // Arrange — кластер bkdrfl<тег>; дрилы выключены глобально (env IntervalDays=0).
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("bk-drillf", withMinio: true, ct: ct);
        Fx = fx;
        var cluster = $"bkdrfl{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        await using var app = await StartBackupHostAsync("bkdrillf", ct,
            extraEnv: new Dictionary<string, string>
            {
                ["PgWorker__Backups__Drill__IntervalDays"] = "0",
            });

        // Arrange — COMPLETED-полный снялся; дрилов нет (выключены env-ом).
        var completed = await E2eFixture.WaitForAsync(
            async () => (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains("COMPLETED")),
            TimeSpan.FromSeconds(300), ct);
        completed.Should().BeTrue("полный обязан сняться");
        var before = await G.GetAsync(Endpoint, $"/pgworker/backups/{cluster}/shard1/drill", ct);
        before.Value.Should().BeNull("IntervalDays=0 — запусков нет");

        // Arrange — цепочка после wal_start хвостом ≥2 сегментов: idle-DB закрывает
        // сегмент 16 МБ минутами, поэтому форсируем pg_switch_wal на мастере.
        // ВАЖНО: wal/ содержит сегменты и НИЖЕ wal_start полного (агент начинает
        // поток раньше бэкапа) — дыра «второй объект списка» может лечь ниже
        // chain_start и цепью не быть (прогон 2026-10-01: дрилл легитимно
        // SUCCEEDED за 6 c). Жертва — СТРОГО Next(wal_start) полного, дыра —
        // при наличии более позднего сегмента.
        var (_, pgPort, _) = await MasterPgAsync(cluster, "shard1", ct);
        var adminDsn = DatabaseProvisioner.BuildAdminDsn("localhost", pgPort, cluster,
            new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
        await using (var conn = new NpgsqlConnection(adminDsn))
        {
            await conn.OpenAsync(ct);
            for (var i = 0; i < 4; i++)
                await using (var cmd = new NpgsqlCommand("SELECT pg_switch_wal()", conn))
                    await cmd.ExecuteScalarAsync(ct);
        }

        var hostEndpoint = Fx.S3Endpoint.Replace(
            "host.docker.internal:", "localhost:", StringComparison.Ordinal);
        var corruptS3 = new PgWorker.Backups.BackupS3(new PgWorker.Backups.BackupsRuntimeOptions
        {
            Enabled = true,
            S3Endpoint = hostEndpoint,
            S3AdvertisedEndpoint = Fx.S3Endpoint,
            S3Bucket = Bucket,
            S3AccessKey = "minioadmin",
            S3SecretKey = "minioadmin",
            S3PathStyle = true,
            JobImage = E2eEnvironment.JobImage,
        });
        
        // Act — жертва = САМ wal_start COMPLETED-полного (валидация дрилла идёт
        // от него; он гарантированно в wal/ — дублирование t02). Дыра обязана
        // иметь «найдено» — сегмент позже wal_start (гвард ниже). ВАЖНО: сегменты
        // НИЖЕ wal_start (агент начинает поток раньше бэкапа) цепью не являются —
        // дыра «второй объект списка» может лечь ниже chain_start и Check её не
        // видит (прогон 2026-10-01: дрилл легитимно SUCCEEDED за 6 c). Затем
        // включаем дриллы policy-полем (применяется следующим тиком без рестарта
        // — AC7). Пересъём BROKEN-цепочки занимает минуты — дыра живёт до
        // валидации дрилла (тик 1 с).
        var done = (await FullKeysAsync(cluster, "shard1")).Single(f => f.Value.Contains("COMPLETED"));
        var fullStatus = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(done.Value)!;
        var victim = PgWorker.Backups.WalFileName.TryParse(
            fullStatus["wal_start_segment"].GetString()!)!.Value;
        var victimReady = await E2eFixture.WaitForAsync(async () =>
        {
            var list = await corruptS3.ListWalAsync(cluster, "shard1", ct: ct);
            if (!list.IsSuccess)
                return false;
            var positions = list.Value
                .Select(o => PgWorker.Backups.WalFileName.TryParse(o.Name))
                .OfType<PgWorker.Backups.WalFileName>()
                .Distinct()
                .ToList();
            return positions.Any(w => (w.Tli, w.Log, w.Seg) == (victim.Tli, victim.Log, victim.Seg))
                   && positions.Any(w => w.Tli > victim.Tli
                       || (w.Tli == victim.Tli
                           && (w.Log > victim.Log
                               || (w.Log == victim.Log && w.Seg > victim.Seg))));
        }, TimeSpan.FromSeconds(300), ct);
        if (!victimReady)
        {
            // диагностика провала доставки жертвы (без перезапуска)
            var walObjectsDiag = await corruptS3.ListWalAsync(cluster, "shard1", ct: ct);
            var walKvDiag = await GetOrNullAsync($"/pgworker/backups/{cluster}/shard1/wal");
            var workKvDiag = await GetOrNullAsync($"/pgworker/work/{cluster}");
            var agentsDiag = await Fx.RunDockerAsync(
                ["ps", "-a", "--format", "{{.Names}} {{.State}}", "--filter", $"name=pgw-backup-wal-{cluster}-"], ct);
            throw new ApplicationException(
                $"жертва {victim.Name} не доставлена: " +
                $"walObjects=[{string.Join(",", walObjectsDiag.IsSuccess ? walObjectsDiag.Value.Select(o => o.Name) : [])}] " +
                $"wal=[{walKvDiag?.Value ?? "-"}] " +
                $"journal=[{workKvDiag?.Value[..Math.Min(400, workKvDiag?.Value.Length ?? 0)]}] " +
                $"agents=[{agentsDiag.Replace('\n', ';')}]");
        }
        // Останавливаем WAL-агента ДО порчи: иначе гонка самозалечивания — BROKEN
        // → пересъём/агент успевают вернуть сегмент до валидации дрилла (тик 1 с;
        // прогон мерж-гейта 2026-10-01: дрилл легитимно SUCCEEDED на зажившей
        // цепи). Остановленный агент ничего не ре-аплоадит; планировщик BROKEN-
        // пересъёма завершается ПОСЛЕ валидации дрилла (дыра живёт ≥1 тик).
        var agents = await Fx.RunDockerAsync(
            ["ps", "--format", "{{.Names}}", "--filter", $"name=pgw-backup-wal-{cluster}-shard1"], ct);
        var agentName = agents.Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(n => n.Trim()).FirstOrDefault();
        agentName.Should().NotBeNullOrEmpty("агент shard1 жив — предусловие стопа");
        await Fx.RunDockerAsync(["stop", agentName!], ct);

        (await corruptS3.DeleteKeysAsync([$"{cluster}/shard1/wal/{victim.Name}"], ct))
            .IsSuccess.Should().BeTrue("сегмент-жертва обязан удалиться");
        await G.PutAsync(Endpoint, $"/pgworker/backups/{cluster}/policy",
            """{"full_max_age_sec":86400,"verify":{"on_create":false},"drill":{"interval_days":1}}""", null, ct);

        // Assert — валидационный FAILED (бюджет 180 c: тик + валидация list).
        var failed = await E2eFixture.WaitForAsync(async () =>
        {
            var kv = await G.GetAsync(Endpoint, $"/pgworker/backups/{cluster}/shard1/drill", ct);
            return kv.Value?.Value.Contains("\"FAILED\"") == true;
        }, TimeSpan.FromSeconds(180), ct);
        if (!failed)
        {
            // диагностика провала (без перезапуска): ключ дрилла, журнал, wal, полные
            var drillKv = await GetOrNullAsync($"/pgworker/backups/{cluster}/shard1/drill");
            var workKv = await GetOrNullAsync($"/pgworker/work/{cluster}");
            var walKv = await GetOrNullAsync($"/pgworker/backups/{cluster}/shard1/wal");
            var fullsNow = await FullKeysAsync(cluster, "shard1");
            var walNow = await corruptS3.ListWalAsync(cluster, "shard1", ct: ct);
            throw new ApplicationException(
                $"валидационный FAILED не зафиксирован: drill=[{drillKv?.Value ?? "-"}] " +
                $"journal=[{workKv?.Value[..Math.Min(400, workKv?.Value.Length ?? 0)]}] " +
                $"wal=[{walKv?.Value ?? "-"}] " +
                $"fulls=[{string.Join(";", fullsNow.Select(f => f.Value[..Math.Min(120, f.Value.Length)]))}] " +
                $"walObjects=[{string.Join(",", walNow.IsSuccess ? walNow.Value.Select(o => o.Name) : [])}]");
        }
        var drill = (await G.GetAsync(Endpoint, $"/pgworker/backups/{cluster}/shard1/drill", ct)).Value!.Value;
        // границы дыры — ASCII-имена сегментов (кириллица в JSON экранируется
        // сериализатором как \uXXXX — текст «дыра» в сыром значении не ищется);
        // FAILED без phase — чистый терминальный итог (джоб не запускался).
        // Границы динамические (жертва — сам wal_start, Act выше): «ожидался» —
        // wal_start жертвы, «найден» — ближайший доставленный сегмент после неё.
        var after = await corruptS3.ListWalAsync(cluster, "shard1", ct: ct);
        after.IsSuccess.Should().BeTrue("лист wal после порчи доступен");
        var found = after.Value
            .Select(o => o.Name)
            .Where(n => string.CompareOrdinal(n, victim.Name) > 0)
            .OrderBy(n => n, StringComparer.Ordinal)
            .FirstOrDefault();
        found.Should().NotBeNull("после жертвы есть более поздний сегмент — дыра имеет «найдено»");
        drill.Should().Contain(victim.Name, "граница дыры — wal_start")
            .And.Contain(found!, "граница дыры — найденный сегмент")
            .And.NotContain("phase", "без джоба — чистый итог");

        // Assert — контейнеров дрилла нет вовсе (джоб не запускался).
        var containers = await Fx.RunDockerAsync(
            ["ps", "-a", "--format", "{{.Names}}", "--filter", $"name=pgw-backup-drill-{cluster}-"], ct);
        containers.Should().BeEmpty("джоб не запускался — валидационный отказ");

        // Assert — панельный алерт провала горит на фактических kvs кластера.
        var kvs = (await G.RangeAsync(Endpoint, $"/pgworker/backups/{cluster}/", ct)).Value;
        var panelCluster = AdminPanel.Etcd.Parsing.BackupsParser.Parse(kvs)
            .Clusters.Single(c => c.Cluster == cluster);
        var alerts = new AdminPanel.Core.Alerting.Rules.BackupDrillFailedRule().Evaluate(
            SnapshotWith(panelCluster, cluster, "shard1"), DefaultAlertContext()).ToList();
        alerts.Should().ContainSingle(a => a.Kind == "backup-drill-failed", "провал дрилла — critical-алерт");
    }

    // Минимальный снапшот панели для правил дрилла («панельный алерт» в E2E:
    // фактические kvs кластера кормят правило — образец панельных юнитов).
    private static AdminPanel.Core.EtcdSnapshot SnapshotWith(
        AdminPanel.Core.ClusterBackupsInfo panelCluster, string activeCluster, string shard)
        => new(
            DateTimeOffset.UtcNow,
            new AdminPanel.Core.EtcdStatus(true, [], [], [], null, false, DateTimeOffset.UtcNow, 0),
            [new AdminPanel.Core.ClusterInfo(
                activeCluster, activeCluster, 2, 1755800000, AdminPanel.Core.ClusterState.Active,
                [new AdminPanel.Core.ShardInfo(
                    shard, "", [""], 0, null, null, 1, null, [], null)],
                [], [])],
            [], [], [], [panelCluster], [], [], [], [], [], [], 0);

    private static AdminPanel.Core.Alerting.AlertContext DefaultAlertContext()
        => new(null, DateTimeOffset.UtcNow, 3);

    // AAA (AC1/AC3/AC4): первый дрилл стартует сам после COMPLETED-полного,

    // Воркер с включённой подсистемой бэкапов: S3 на локальный MinIO,
    // образ pgworker-backup:e2e, ускоренный бэкофф.
    private Task<HostInstance> StartBackupHostAsync(
        string name, CancellationToken ct, string? s3EndpointOverride = null,
        IReadOnlyDictionary<string, string>? extraEnv = null)
    {
        var env = new Dictionary<string, string>
        {
            ["PgWorker__Backups__Enabled"] = "true",
            ["PgWorker__Backups__S3__Endpoint"] = s3EndpointOverride ?? Fx.S3Endpoint,
            ["PgWorker__Backups__S3__Bucket"] = Bucket,
            ["PgWorker__Backups__S3__AccessKey"] = "minioadmin",
            ["PgWorker__Backups__S3__SecretKey"] = "minioadmin",
            ["PgWorker__Backups__Job__Image"] = E2eEnvironment.JobImage,
            ["PgWorker__Backups__Wal__AgentImage"] = E2eEnvironment.WalImage,
            ["PgWorker__Backups__Retry__BaseSec"] = "2",
            ["PgWorker__Backups__Retry__MaxSec"] = "4",
        };
        foreach (var (key, value) in extraEnv ?? new Dictionary<string, string>())
            env[key] = value;
        return Fx.StartHostAsync(name, extraEnv: env, ct: ct);
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

}
