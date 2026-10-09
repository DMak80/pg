using System.Text.Json;
using Npgsql;
using PgWorker.Core.Templates;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.IntegrationTests.Docker;
using PgWorker.Provisioning.Sql;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E WAL-потока, базовые сценарии (t24/ревизия 8 — разбиение
// E2eBackupScenarios): непрерывная доставка, пересоздание агента под
// нагрузкой, двойное архивирование (~3,5 мин суммарно).
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
public class E2eWalStreamScenarios
{
    private const string Bucket = "pgworker-backups";

    // Окружение Fact'а (своя сеть/etcd/MinIO); создаётся в начале каждого сценария.
    private E2eEnvironment Fx = null!;

    private string Endpoint => Fx.EtcdEndpoint;

    private EtcdGateway G => Fx.Gateway;

    // AAA: WAL-поток (t03/t27, AC1/AC8): provisioned кластер + INSERT/pg_switch_wal
    // → сегменты в MinIO, ключ wal ACTIVE с chain_start/last_uploaded, цепочка
    // непрерывна, running-агент мастера, слот pgw_bkp_<C>_<X> на мастере, поле
    // agents в ключе (супервиз-факты per-node, t27 §3.5).
    [Fact]
    public async Task WalStream_UploadsSegmentsContinuously()
    {
        // Arrange — гейт docker; skip-защита: общий образ t02/t03 обязан быть в дереве
        DockerTrait.SkipIfUnavailable();
        if (!File.Exists(Path.Combine(
                E2eFixture.FindRoot(AppContext.BaseDirectory), "docker", "PgWorker.Backup.Dockerfile")))
            Assert.Skip("docker/PgWorker.Backup.Dockerfile отсутствует — общий образ джобов/агентов приходит из t02");
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("wal-stream", withMinio: true, ct: ct);
        Fx = fx;
        var cluster = $"shopb{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        await using var app = await StartWalHostAsync("walstream", ct);

        var provisioned = await E2eFixture.WaitForAsync(
            () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(360), ct);
        provisioned.Should().BeTrue("provisioning должен дойти до DONE до WAL-нагрузки");

        // Мастер shard1 (published pg-порт) + admin/superuser-DSN: нагрузка и
        // pg_switch_wal (superuser-only) выполняются одним соединением — как
        // t02 SqlListAsync (dsn-креды app_user недоступны тесту напрямую).
        var (_, pgPort, masterNodeUp) = await MasterPgAsync(cluster, "shard1", ct);
        var adminDsn = DatabaseProvisioner.BuildAdminDsn("localhost", pgPort, cluster,
            new InstallSecrets(E2eFixture.SuPassword, "", "", ""));

        // Act — INSERT-нагрузка: большие строки + pg_switch_wal форсируют закрытие
        // сегментов 16 МБ (без таймаутов, spec §5).
        await GenerateWalAsync(adminDsn, ct);

        // BackupS3 host-клиент: воркер ходит на localhost (published порт MinIO),
        // агентам отдаётся advertised (Fx.S3Endpoint = host.docker.internal:<порт>).
        var hostEndpoint = Fx.S3Endpoint.Replace(
            "host.docker.internal:", "localhost:", StringComparison.Ordinal);
        var backupS3 = new PgWorker.Backups.BackupS3(new PgWorker.Backups.BackupsRuntimeOptions
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

        // Assert 1 — бюджет 120 c: сегменты в MinIO, все без .partial (AC1)
        List<PgWorker.Backups.WalObject> listed = [];
        var uploaded = await E2eFixture.WaitForAsync(async () =>
        {
            var list = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
            if (!list.IsSuccess)
                return false;
            listed = [.. list.Value];
            return listed.Count >= 2;
        }, TimeSpan.FromSeconds(120), ct);
        if (!uploaded)
        {
            // диагностика провала доставки: журнал воркера, ключ wal, агенты, креды
            var workKv = await GetOrNullAsync($"/pgworker/work/{cluster}");
            var walKv = await GetOrNullAsync($"/pgworker/backups/{cluster}/shard1/wal");
            var pwKv = await GetOrNullAsync($"/clusters/{cluster}/backup_password");
            var agentsPs = await Fx.RunDockerAsync(
                ["ps", "-a", "--format", "{{.Names}} {{.State}}", "--filter", $"name=pgw-backup-wal-{cluster}-"], ct);
            var jobLogs = "(агентов нет)";
            foreach (var line in agentsPs.Split('\n', StringSplitOptions.RemoveEmptyEntries))
            {
                var name = line.Split(' ')[0];
                var logs = await Fx.RunDockerAsync(["logs", "--tail", "12", name], ct);
                jobLogs += $"\n== {name}: {logs[..Math.Min(700, logs.Length)]}";
            }

            throw new ApplicationException(
                $"сегменты не доставлены: journal=[{workKv?.Value[..Math.Min(400, workKv.Value.Length)]}] " +
                $"wal=[{walKv?.Value ?? "-"}] backup_password={(pwKv is null ? "-" : "есть")} " +
                $"agents=[{agentsPs.Replace('\n', ';')}] {jobLogs}");
        }
        uploaded.Should().BeTrue("сегменты обязаны появиться в MinIO за бюджет (AC1)");
        listed.Select(o => o.Name).Should().OnlyContain(n => !n.EndsWith(".partial"));

        // Assert 2 — ключ wal ACTIVE и СОГЛАСОВАННЫЙ снапшот «ключ × list»: воркер
        // и агент живут, пары (wal, listed) берутся поллингом до совпадения
        // last_uploaded из ключа с содержимым свежего листа (иначе ассерт сравнивает
        // разные моменты времени живого потока).
        var writer = new PgWorker.Backups.WalStatusWriter(G, [Endpoint]);
        PgWorker.Etcd.Parsing.WalStreamState wal = null!;
        var consistent = await E2eFixture.WaitForAsync(async () =>
        {
            var read = await writer.ReadAsync(cluster, "shard1", ct);
            if (!read.IsSuccess || read.Value is not { State: PgWorker.Etcd.Parsing.WalStreamStatus.Active })
                return false;
            wal = read.Value;
            var list = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
            if (!list.IsSuccess)
                return false;
            listed = [.. list.Value];
            return listed.Select(o => o.Name).Contains(wal.LastUploadedSegment);
        }, TimeSpan.FromSeconds(120), ct);
        consistent.Should().BeTrue("ключ wal обязан перейти в ACTIVE (AC2) и совпасть с S3-фактом");
        wal.Slot.Should().Be($"pgw_bkp_{cluster}_shard1");
        wal.MasterNode.Should().NotBeEmpty();
        wal.ChainStartSegment.Should().NotBeEmpty();
        (DateTimeOffset.UtcNow.ToUnixTimeSeconds() - wal.LastUploadedUnix!.Value)
            .Should().BeLessThan(120, "last_uploaded_unix свежий (поток жив)");

        // Assert 3 — цепочка непрерывна от chain_start (AC3-факт): поллинг —
        // одиночный list ловит момент частичной доставки (сегменты грузятся
        // последовательно, list между ними видит временную «дыру»).
        var chainStart = PgWorker.Backups.WalFileName.TryParse(wal.ChainStartSegment)!.Value;
        var continuous = await E2eFixture.WaitForAsync(async () =>
        {
            var list = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
            if (!list.IsSuccess)
                return false;
            listed = [.. list.Value];
            var check = PgWorker.Backups.WalChain.Check(chainStart, listed.Select(o => o.Name));
            return check.IsContinuous;
        }, TimeSpan.FromSeconds(120), ct);
        continuous.Should().BeTrue("цепочка от chain_start обязана стать непрерывной");

        // Assert 4 — running-агент МАСТЕРА (per-node имя t27) + слот на мастере (AC1)
        var (_, _, masterNode) = await MasterPgAsync(cluster, "shard1", ct);
        var agents = await Fx.RunDockerAsync(
            ["ps", "--filter", $"name=pgw-backup-wal-{cluster}-shard1-", "--format", "{{.Names}} {{.State}}"], ct);
        agents.Should().Contain($"pgw-backup-wal-{cluster}-shard1-{masterNode} running");
        // t27 §3.5: ключ wal несёт супервиз-факты agents (мастер running).
        // Перечитываем ключ поллингом: контроль пишет wal, супервиз добавляет
        // agents тем же тиком — снапшот Assert 2 может быть пойман между двумя
        // put (гонка живого потока, как в Assert 2).
        var agentsInKey = await E2eFixture.WaitForAsync(async () =>
        {
            var read = await writer.ReadAsync(cluster, "shard1", ct);
            return read.IsSuccess && read.Value?.Agents is { } facts
                   && facts.Any(a =>
                       a.Node == masterNode && a.State == PgWorker.Etcd.Parsing.WalAgentPresence.Running);
        }, TimeSpan.FromSeconds(30), ct);
        agentsInKey.Should().BeTrue("agents-факты пишутся супервизом (t27): мастер running");
        await using var adminConn = new NpgsqlConnection(adminDsn);
        await adminConn.OpenAsync(ct);
        await using var slotCmd = new NpgsqlCommand(
            "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = @slot)",
            adminConn) { Parameters = { new() { ParameterName = "slot", Value = $"pgw_bkp_{cluster}_shard1" } } };
        ((bool)(await slotCmd.ExecuteScalarAsync(ct))!).Should().BeTrue("слот создан воркером на мастере (AC1)");

        // Assert 5 — каркасный парсер t01/t02 читает wal-ключ без parseErrors (AC2)
        var kvs = (await G.RangeAsync(Endpoint, $"/pgworker/backups/{cluster}/", ct)).Value;
        var parsed = BackupsParser.Parse(kvs, out var parseErrors);
        parseErrors.Should().BeEmpty(
            $"парсер обязан читать ключи воркера (в т.ч. wal с agents t27): " +
            $"ключи=[{string.Join("; ", kvs.Select(k => k.Key))}] ошибки=[{string.Join("; ", parseErrors)}]");
        parsed.Value.Should().Contain(b => b.Cluster == cluster);
    }

    // ── t27 Ф4: E2E двойной архивации (AC2–AC5; каждый Fact — свой guid-контур) ──

    // Фоновая WAL-нагрузка: цикл INSERT+pg_switch_wal до отмены (пересоздание

    // ключ возвращается в ACTIVE (потеря недоставленного невозможна по построению:
    // слот удерживает WAL, новый агент досыпает от хвоста S3).
    [Fact]
    public async Task WalStream_AgentRecreate_UnderLoad_NoGap()
    {
        // Arrange — окружение, кластер, воркер, фоновая нагрузка ≥ 2 мин
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        var (fx, app, cluster, adminDsn, masterNode) = await StartWalScenarioAsync("wal-ac2", "shopac2", ct);
        await using var envOwner = fx;
        await using var appOwner = app;
        using var load = new CancellationTokenSource();
        var loader = BackgroundWalLoadAsync(adminDsn, load.Token);
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
        var agentName = $"pgw-backup-wal-{cluster}-shard1-{masterNode}";
        var writer = new PgWorker.Backups.WalStatusWriter(G, [Endpoint]);

        try
        {
            // Дождаться первой доставки (агент жив, ≥1 сегмент в S3)
            var firstListCount = 0;
            var firstList = await E2eFixture.WaitForAsync(async () =>
            {
                var list = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
                if (!list.IsSuccess || list.Value.Count == 0)
                    return false;
                firstListCount = list.Value.Count;
                return true;
            }, TimeSpan.FromSeconds(180), ct);
            if (!firstList)
            {
                // Диагностика провала первой доставки (канон e2e-launch: «что
                // произошло» — без перезапуска): агенты, журнал воркера, wal-ключ,
                // хвост host.log (HostInstance ещё жив — teardown в dispose).
                var agentsPs = await Fx.RunDockerAsync(
                    ["ps", "-a", "--format", "{{.Names}} {{.State}}",
                     "--filter", $"name=pgw-backup-wal-{cluster}-"], ct);
                var agentLogs = "";
                foreach (var line in agentsPs.Split('\n', StringSplitOptions.RemoveEmptyEntries))
                {
                    var name = line.Split(' ')[0];
                    var logs = await Fx.RunDockerAsync(["logs", "--tail", "25", name], ct);
                    agentLogs += $"\n== {name}: {logs[..Math.Min(900, logs.Length)]}";
                }

                var walKv = await GetOrNullAsync($"/pgworker/backups/{cluster}/shard1/wal");
                var workKv = await GetOrNullAsync($"/pgworker/work/{cluster}");
                var claimKv = await GetOrNullAsync($"/pgworker/claims/{cluster}");
                var hostLog = Fx.Hosts is { Count: > 0 }
                    ? string.Join("\n", Fx.Hosts.Select(h =>
                    {
                        try
                        {
                            // Значимые строки (без etcd-HTTP-шума): backup/wal/агент/ошибки
                            var lines = File.ReadAllLines(Path.Combine(h.SnapshotsDir, "host.log"));
                            var interesting = lines.Where(l =>
                                !l.Contains("HTTP request") && !l.Contains("HTTP response")
                                && !l.Contains("End processing HTTP")
                                && !l.Contains("PgtuneInputsFactory")
                                && !l.Contains("wal_compression")
                                && !l.Contains("Connection reset by peer")
                                && !l.Contains("AwaitableSocketAsyncEventArgs"))
                                .Where(l => l.Contains("backup", StringComparison.OrdinalIgnoreCase)
                                    || l.Contains("wal", StringComparison.OrdinalIgnoreCase)
                                    || l.Contains("agent", StringComparison.OrdinalIgnoreCase)
                                    || l.Contains("error", StringComparison.OrdinalIgnoreCase)
                                    || l.Contains("warn", StringComparison.OrdinalIgnoreCase)
                                    || l.Contains("fail", StringComparison.OrdinalIgnoreCase)
                                    || l.Contains("exception", StringComparison.OrdinalIgnoreCase)
                                    || l.Contains("claim", StringComparison.OrdinalIgnoreCase)
                                    || l.Contains("supervise", StringComparison.OrdinalIgnoreCase))
                                .ToList();
                            // op-логи (их немного, они решают «почему молчит подсистема»)
                            var opLines = lines
                                .Where(l => l.Contains("backup-wal", StringComparison.OrdinalIgnoreCase)
                                    || l.Contains("WalStreamProcess", StringComparison.OrdinalIgnoreCase)
                                    || l.Contains("pgw-backup-wal", StringComparison.OrdinalIgnoreCase)
                                    || l.Contains("supervise", StringComparison.OrdinalIgnoreCase)
                                    || l.Contains("клэйм", StringComparison.OrdinalIgnoreCase))
                                .ToList();
                            // Полный хвост (120 строк, без фильтров) — полный контекст
                            return $"[{h.Name}] OPS({opLines.Count}):\n{string.Join("\n", opLines)}" +
                                $"\nFULLTAIL:\n" +
                                string.Join("\n", lines[^Math.Min(120, lines.Length)..]);
                        }
                        catch (Exception e)
                        {
                            return $"<host.log недоступен: {e.Message}>";
                        }
                    }))
                    : "<hosts нет>";
                throw new ApplicationException(
                    "первая доставка не случилась за бюджет: " +
                    $"agents=[{agentsPs.Replace('\n', ';')}] wal=[{walKv?.Value ?? "-"}] " +
                    $"journal=[{workKv?.Value?[..Math.Min(400, workKv.Value?.Length ?? 0)]}] " +
                    $"claim=[{claimKv?.Value ?? "-"}] {agentLogs}\nHOST.LOG:\n{hostLog}");
            }

            firstList.Should().BeTrue("первая доставка до пересоздания");
            firstListCount.Should().BeGreaterThan(0);
            var idBefore = await AgentContainerIdAsync(agentName, ct);
            idBefore.Should().NotBeNullOrEmpty($"агент мастера {agentName} обязан быть жив");

            // Act — docker rm -f агента мастера ПОД нагрузкой
            Console.WriteLine($"[PHASE] wal-ac2: docker rm -f {agentName}");
            await Fx.RunDockerAsync(["rm", "-f", agentName], ct);

            // Assert — агент пересоздан супервизом (то же имя, контейнер новый)
            var recreated = await E2eFixture.WaitForAsync(async () =>
            {
                var id = await AgentContainerIdAsync(agentName, ct);
                return id is not null && id != idBefore;
            }, TimeSpan.FromSeconds(120), ct);
            recreated.Should().BeTrue("супервиз тика обязан пересоздать агента (restart-policy no)");

            // ≥1 новый сегмент после рескрета; цепочка непрерывна (WalChain union)
            var grew = await E2eFixture.WaitForAsync(async () =>
            {
                var list = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
                return list.IsSuccess && list.Value.Count > firstListCount;
            }, TimeSpan.FromSeconds(120), ct);
            grew.Should().BeTrue("новый агент досыпает от хвоста S3 — доставка продолжается");

            // Ключ: BROKEN НЕ возникал весь сценарий (поллинг срезом); финал ACTIVE
            PgWorker.Etcd.Parsing.WalStreamStatus? sawStatus = null;
            var backActive = await E2eFixture.WaitForAsync(async () =>
            {
                var read = await writer.ReadAsync(cluster, "shard1", ct);
                if (!read.IsSuccess || read.Value is null)
                    return false;
                sawStatus = read.Value.State;
                if (sawStatus == PgWorker.Etcd.Parsing.WalStreamStatus.Broken)
                    return true; // немедленный фейл ниже
                return sawStatus == PgWorker.Etcd.Parsing.WalStreamStatus.Active;
            }, TimeSpan.FromSeconds(120), ct);
            sawStatus.Should().NotBe(
                PgWorker.Etcd.Parsing.WalStreamStatus.Broken,
                "доставка от хвоста S3 исключает дыру — BROKEN запрещён (AC2)");
            backActive.Should().BeTrue("ключ wal возвращается в ACTIVE");
            var list = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
            list.IsSuccess.Should().BeTrue();
            var wal = await writer.ReadAsync(cluster, "shard1", ct);
            wal.Value.Should().NotBeNull();
            var chain = PgWorker.Backups.WalChain.Check(
                PgWorker.Backups.WalFileName.TryParse(wal.Value!.ChainStartSegment)!.Value,
                list.Value.Select(o => o.Name));
            chain.IsContinuous.Should().BeTrue(chain.GapError ?? "цепочка непрерывна");
        }
        catch (Exception ex)
        {
            Fx.MarkFailed(); // артефакты телеметрии переживают teardown (канон e2e-launch)
            await WalScenarioDiagDumpAsync(cluster, "AC2: " + ex.Message);
            throw;
        }
        finally
        {
            await load.CancelAsync();
            try
            {
                await loader; // нагрузка гасится отменой
            }
            catch
            {
                // фон уже умер (соединение/отмена) — сценарий завершается
            }
        }
    }

    // AAA (AC3-E2E): двухнодовый шард (sync-standby появится сам, SyncStrict=true) —
    // ДВА running-агента (мастер + sync), оба пишут в один префикс wal/, цепочка

    // ДВА running-агента (мастер + sync), оба пишут в один префикс wal/, цепочка
    // непрерывна (union), ключ ACTIVE с agents обеих нод, слоты на ОБОИХ нодах.
    [Fact]
    public async Task WalStream_DualArchiving_TwoAgents_OnePrefix()
    {
        // Arrange
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        var (fx, app, cluster, adminDsn, masterNode) = await StartWalScenarioAsync("wal-ac3", "shopac3", ct);
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

        // WAL-нагрузка ПЕРВАЯ: Patroni выбирает sync-реплику при живом потоке
        // репликации — без нагрузки sync не выберется и второй агент не поднимется.
        await GenerateWalAsync(adminDsn, ct);
        using var load = new CancellationTokenSource();
        var loader = BackgroundWalLoadAsync(adminDsn, load.Token);

        try
        {
        // Act — ждём ДВА running-агента (sync-standby появится сам) ≤ 300 c
        var bothAgents = await E2eFixture.WaitForAsync(async () =>
        {
            var ps = await Fx.RunDockerAsync(
            [
                "ps", "--filter", $"name=pgw-backup-wal-{cluster}-shard1-",
                "--format", "{{.Names}} {{.State}}",
            ], ct);
            return ps.Split('\n', StringSplitOptions.RemoveEmptyEntries).Count(l => l.EndsWith("running")) >= 2;
        }, TimeSpan.FromSeconds(300), ct);
        if (!bothAgents)
        {
            var agentsPs = await Fx.RunDockerAsync(
                ["ps", "-a", "--format", "{{.Names}} {{.State}}",
                 "--filter", $"name=pgw-backup-wal-{cluster}-"], ct);
            var walKv = await GetOrNullAsync($"/pgworker/backups/{cluster}/shard1/wal");
            var patroniState = await PatroniClusterDumpAsync(cluster, ct);
            var workKv = await GetOrNullAsync($"/pgworker/work/{cluster}");
            throw new ApplicationException(
                "два агента не поднялись: " +
                $"agents=[{agentsPs.Replace('\n', ';')}] wal=[{walKv?.Value?[..Math.Min(600, walKv.Value?.Length ?? 0)]}] " +
                $"work=[{workKv?.Value?[..Math.Min(1200, workKv.Value?.Length ?? 0)]}] " +
                $"patroni={patroniState[..Math.Min(700, patroniState.Length)]}");
        }

        bothAgents.Should().BeTrue("двойная архивация: агенты мастера и sync-standby (AC3)");

        // Нагрузка + доставка: сегменты растут, ключ ACTIVE, agents — обе ноды
        await GenerateWalAsync(adminDsn, ct);
        var delivered = await E2eFixture.WaitForAsync(async () =>
        {
            var list = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
            if (!list.IsSuccess || list.Value.Count < 2) return false;
            var read = await writer.ReadAsync(cluster, "shard1", ct);
            if (!read.IsSuccess || read.Value is not { State: PgWorker.Etcd.Parsing.WalStreamStatus.Active })
                return false;
            // agents — обе ноды running
            var nodes = read.Value.Agents?
                .Where(a => a.State == PgWorker.Etcd.Parsing.WalAgentPresence.Running)
                .Select(a => a.Node).ToHashSet();
            return nodes is { Count: >= 2 };
        }, TimeSpan.FromSeconds(180), ct);
        delivered.Should().BeTrue("ключ ACTIVE + agents обеих нод (AC3)");

        // Цепочка непрерывна от chain_start (union одного префикса)
        var wal = (await writer.ReadAsync(cluster, "shard1", ct)).Value!;
        var list = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
        list.IsSuccess.Should().BeTrue();
        var chain = PgWorker.Backups.WalChain.Check(
            PgWorker.Backups.WalFileName.TryParse(wal.ChainStartSegment)!.Value,
            list.Value.Select(o => o.Name));
        chain.IsContinuous.Should().BeTrue(chain.GapError ?? "union-цепочка непрерывна");

        // Слоты pgw_bkp_<C>_shard1 на ОБОИХ нодах (published pg-порты из portalloc)
        var kv = await G.GetAsync(Endpoint, $"/pgworker/portalloc/{cluster}", ct);
        var entries = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(kv.Value!.Value)!
            .Where(p => p.Key.StartsWith("shard1/", StringComparison.Ordinal)).ToList();
        foreach (var (key, addr) in entries)
        {
            var dsn = DatabaseProvisioner.BuildAdminDsn("localhost", addr.GetProperty("pg").GetInt32(),
                cluster, new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
            await using var conn = new NpgsqlConnection(dsn);
            await conn.OpenAsync(ct);
            await using var cmd = new NpgsqlCommand(
                "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = @slot)", conn)
            {
                Parameters = { new() { ParameterName = "slot", Value = $"pgw_bkp_{cluster}_shard1" } },
            };
            ((bool)(await cmd.ExecuteScalarAsync(ct))!)
                .Should().BeTrue($"слот существует на ноде {key} (AC3, per-instance)");

            // AC1-инвариант каждого источника (буква критерия «каждого»):
            // restart_lsn слота ≤ конца последнего доставленного в S3 сегмента —
            // подтверждение только по факту put. Хвост — СВЕЖИЙ list на момент
            // зонда: нагрузка живая, цепочка растёт.
            await using var lsnCmd = new NpgsqlCommand(
                "SELECT restart_lsn::text FROM pg_replication_slots WHERE slot_name = @slot", conn)
            {
                Parameters = { new() { ParameterName = "slot", Value = $"pgw_bkp_{cluster}_shard1" } },
            };
            var restartRaw = (string)(await lsnCmd.ExecuteScalarAsync(ct))!;
            var parts = restartRaw.Split('/');
            var restartLsn = (ulong.Parse(parts[0], System.Globalization.NumberStyles.HexNumber) << 32)
                             | ulong.Parse(parts[1], System.Globalization.NumberStyles.HexNumber);
            var fresh = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
            fresh.IsSuccess.Should().BeTrue("AC1-зонд: свежий list хвоста");
            var tail = fresh.Value
                .Select(o => PgWorker.Backups.WalFileName.TryParse(o.Name))
                .Where(w => w is not null).Select(w => w!.Value)
                .OrderByDescending(w => (long)w.Log * 256 + w.Seg).First();
            restartLsn.Should().BeLessOrEqualTo(
                PgWorker.WalReceiver.S3TailResolver.EndLsn(tail),
                $"AC1 на ноде {key}: restart_lsn {restartRaw} ≤ конца доставленного {tail.Name}");
        }
        }
        catch (Exception ex)
        {
            Fx.MarkFailed(); // артефакты телеметрии переживают teardown
            await WalScenarioDiagDumpAsync(cluster, "AC3: " + ex.Message);
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
                // фон умер вместе с соединением — не маскируем исход
            }
        }
    }

    // AAA (критерий 5): lost-слот мастера при живой sync-архивации — воркер лечит
    // сам (recreate + журнальная фаза slot-recreate), ключ НЕ BROKEN, агент
    // возвращается, цепочка WalChain непрерывна, last_uploaded растёт без дыры.
    // Детерминизация: между сносом агента мастера и возвратом воркера (Kill/старт —
    // паттерн «смерть контроллера») живой sync-агент продолжает доставку, WAL
    // генерится до фактического wal_status='lost'.
    [Fact]
    public async Task WalStream_SlotLostAutoRecreate_NoBroken()
    {
        // Arrange — двухнодовый шард (двойная архивация), оба агента живы, доставка идёт
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        var (fx, app, cluster, adminDsn, masterNode) = await StartWalScenarioAsync("wal-t19", "shopt19", ct);
        await using var envOwner = fx;
        await using var appOwner = app; // kill воркера ниже — dispose идемпотентен
        var slot = $"pgw_bkp_{cluster}_shard1";
        var agentName = $"pgw-backup-wal-{cluster}-shard1-{masterNode}";
        var writer = new PgWorker.Backups.WalStatusWriter(G, [Endpoint]);
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

        try
        {
            // Нагрузка ПЕРВАЯ — Patroni выбирает sync при живом потоке репликации (AC3)
            await GenerateWalAsync(adminDsn, ct);
            var bothAgents = await E2eFixture.WaitForAsync(async () =>
            {
                var ps = await Fx.RunDockerAsync(
                [
                    "ps", "--filter", $"name=pgw-backup-wal-{cluster}-shard1-",
                    "--format", "{{.Names}} {{.State}}",
                ], ct);
                return ps.Split('\n', StringSplitOptions.RemoveEmptyEntries)
                    .Count(l => l.EndsWith("running")) >= 2;
            }, TimeSpan.FromSeconds(300), ct);
            bothAgents.Should().BeTrue("двойная архивация — оба агента обязаны подняться");

            // Малый потолок WAL под слотом на мастере (динамика reload — без рестарта)
            await using (var conn = new NpgsqlConnection(adminDsn))
            {
                await conn.OpenAsync(ct);
                await using var alter = new NpgsqlCommand(
                    "ALTER SYSTEM SET max_slot_wal_keep_size = '16MB'", conn);
                await alter.ExecuteNonQueryAsync(ct);
                await using var reload = new NpgsqlCommand("SELECT pg_reload_conf()", conn);
                await reload.ExecuteNonQueryAsync(ct);
            }

            // Фиксация «до»: ключ ACTIVE и его last_uploaded (согласованный срез)
            var before = await E2eFixture.WaitForAsync(async () =>
            {
                var read = await writer.ReadAsync(cluster, "shard1", ct);
                return read.IsSuccess
                       && read.Value is { State: PgWorker.Etcd.Parsing.WalStreamStatus.Active };
            }, TimeSpan.FromSeconds(120), ct);
            before.Should().BeTrue("до потери — ключ ACTIVE");
            var beforeWal = (await writer.ReadAsync(cluster, "shard1", ct)).Value!;

            // Act 1 — снос агента мастера + стоп воркера (супервиз не вернёт агента
            // до воспроизведения lost; sync-агент жив — доставка продолжается)
            Console.WriteLine($"[PHASE] wal-t19: docker rm -f {agentName} + kill воркера");
            await Fx.RunDockerAsync(["rm", "-f", agentName], ct);
            app.Kill();

            // Act 2 — генерация WAL до фактического lost (поллинг зонда; INSERT-нагрузка
            // вместо pgbench — pgbench недоступен в образе ноды, эффект идентичен)
            var lost = await E2eFixture.WaitForAsync(async () =>
            {
                for (var i = 0; i < 2; i++)
                {
                    await using var conn = new NpgsqlConnection(adminDsn);
                    await conn.OpenAsync(ct);
                    await using var insert = new NpgsqlCommand(
                        "INSERT INTO wal_load(payload) SELECT repeat('x', 1048576) FROM generate_series(1, 32)", conn);
                    await insert.ExecuteNonQueryAsync(ct);
                    await using var switchWal = new NpgsqlCommand("SELECT pg_switch_wal()", conn);
                    await switchWal.ExecuteScalarAsync(ct);
                }

                await using (var conn = new NpgsqlConnection(adminDsn))
                {
                    await conn.OpenAsync(ct);
                    await using var checkpoint = new NpgsqlCommand("CHECKPOINT", conn);
                    await checkpoint.ExecuteNonQueryAsync(ct);
                }
                return await SlotWalStatusAsync(cluster, "shard1", masterNode, ct) == "lost";
            }, TimeSpan.FromSeconds(180), ct);
            Console.WriteLine($"[PHASE] wal-t19: слот мастера lost={lost} (бюджет 180 c)");
            lost.Should().BeTrue("64 MiB WAL при потолке 16 MiB + checkpoint обязаны срезать слот мастера");

            // Act 3 — возврат воркера: тик лечит (recreate слота + журнал slot-recreate,
            // супервиз поднимает агента мастера; клэйм убитого истекает ≤ 15-20 c)
            Console.WriteLine("[PHASE] wal-t19: старт воркера — автолечение");
            await using var app2Owner = await StartWalHostAsync("wal-t19b", ct);

            // Assert 1 — журнальная фаза slot-recreate
            var journaled = await E2eFixture.WaitForAsync(async () =>
            {
                var workKv = await GetOrNullAsync($"/pgworker/work/{cluster}");
                return workKv?.Value?.Contains("slot-recreate/shard1") == true;
            }, TimeSpan.FromSeconds(120), ct);
            journaled.Should().BeTrue("воркер обязан записать фазу slot-recreate (spec §3.2)");

            // Assert 2 — слот мастера жив (не lost)
            var healed = await E2eFixture.WaitForAsync(
                async () => await SlotWalStatusAsync(cluster, "shard1", masterNode, ct) is not ("lost" or null),
                TimeSpan.FromSeconds(120), ct);
            healed.Should().BeTrue("recreate возвращает живой слот");

            // Assert 3 — агент мастера снова running (супервиз)
            var agentBack = await E2eFixture.WaitForAsync(async () =>
            {
                var ps = await Fx.RunDockerAsync(
                    ["ps", "--filter", $"name=^{agentName}$", "--format", "{{.State}}"], ct);
                return ps.Trim() == "running";
            }, TimeSpan.FromSeconds(120), ct);
            agentBack.Should().BeTrue("супервиз поднимает агента мастера после recreate");

            // Assert 4 — ключ wal: BROKEN не возникал (поллинг срезом), финал ACTIVE;
            // last_uploaded СТРОГО растёт от «до потери» (sync доставлял + мастер вернулся)
            PgWorker.Etcd.Parsing.WalStreamStatus? sawStatus = null;
            var recovered = await E2eFixture.WaitForAsync(async () =>
            {
                var read = await writer.ReadAsync(cluster, "shard1", ct);
                if (!read.IsSuccess || read.Value is null)
                    return false;
                sawStatus = read.Value.State;
                if (sawStatus == PgWorker.Etcd.Parsing.WalStreamStatus.Broken)
                    return true; // немедленный фейл ниже
                return sawStatus == PgWorker.Etcd.Parsing.WalStreamStatus.Active;
            }, TimeSpan.FromSeconds(180), ct);
            sawStatus.Should().NotBe(PgWorker.Etcd.Parsing.WalStreamStatus.Broken,
                "lost одного источника при живом втором — BROKEN запрещён (критерий 5)");
            recovered.Should().BeTrue("ключ обязан вернуться в ACTIVE");
            var afterWal = (await writer.ReadAsync(cluster, "shard1", ct)).Value!;
            var beforeSeg = PgWorker.Backups.WalFileName.TryParse(beforeWal.LastUploadedSegment)!.Value;
            var afterSeg = PgWorker.Backups.WalFileName.TryParse(afterWal.LastUploadedSegment)!.Value;
            ((long)afterSeg.Log * 256 + afterSeg.Seg).Should().BeGreaterThan(
                (long)beforeSeg.Log * 256 + beforeSeg.Seg,
                "last_uploaded_segment растёт без дыры (живой sync достраивал хвост)");

            // Assert 5 — цепочка WalChain непрерывна от chain_start
            var list = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
            list.IsSuccess.Should().BeTrue();
            var chain = PgWorker.Backups.WalChain.Check(
                PgWorker.Backups.WalFileName.TryParse(afterWal.ChainStartSegment)!.Value,
                list.Value.Select(o => o.Name));
            chain.IsContinuous.Should().BeTrue(chain.GapError ?? "цепочка непрерывна после автолечения");
        }
        catch (Exception ex)
        {
            Fx.MarkFailed(); // артефакты телеметрии переживают teardown (канон e2e-launch)
            await WalScenarioDiagDumpAsync(cluster, "t19: " + ex.Message);
            throw;
        }
    }

    // AAA (AC4): остановка мастера — агент sync-реплики продолжает цепочку без дыр;
    // после promote воркер пересоздаёт агентов на новых ролях; ключ не BROKEN.

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
        await E2eFixture.WaitForAsync(async () =>
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
        }, TimeSpan.FromSeconds(120), ct);
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
            await File.WriteAllTextAsync($"/tmp/pgw-diag-{cluster}.txt", dump);
            Console.WriteLine($"[DIAG] дамп: /tmp/pgw-diag-{cluster}.txt");
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
        // Потолок 600 с: provisioning на перегруженной машине доходит до DONE за
        // 7–8 попыток (ноды живы, но readiness-пробы patroni прогреваются дольше
        // бюджета); зелёный путь не меняется — опрос выходит по факту DONE.
        var provisioned = await E2eFixture.WaitForAsync(
            () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(600), ct);
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
        var resolved = await E2eFixture.WaitForAsync(async () =>
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

    // wal_status слота бэкапа на указанной ноде (portalloc pg-порт): null = слота нет.
    private async Task<string?> SlotWalStatusAsync(string cluster, string shard, string node, CancellationToken ct)
    {
        var kv = await G.GetAsync(Endpoint, $"/pgworker/portalloc/{cluster}", ct);
        kv.Value.Should().NotBeNull("portalloc пишется при provisioning");
        var entries = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(kv.Value!.Value)!;
        var dsn = DatabaseProvisioner.BuildAdminDsn("localhost",
            entries[$"{shard}/{node}"].GetProperty("pg").GetInt32(), cluster,
            new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
        await using var conn = new NpgsqlConnection(dsn);
        await conn.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(
            "SELECT wal_status::text FROM pg_replication_slots WHERE slot_name = @slot", conn)
        {
            Parameters = { new() { ParameterName = "slot", Value = $"pgw_bkp_{cluster}_{shard}" } },
        };
        return (string?)await cmd.ExecuteScalarAsync(ct);
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
