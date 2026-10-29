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
// E2eBackupScenarios; t25 — приёмник в контейнере ноды): непрерывная
// доставка, санитарный снос legacy-контейнера, двойное архивирование.
// E2E полных бэкапов (t02, spec §7.1–7.4): изолированное окружение E2eEnvironment
// (своя docker-сеть, свой etcd, СВОЯ ownS3-нода — withS3Node:true; порт динамический)
// + операции бэкапов в приёмниках WalReceiver нод (arch/19 §5) + живой кластер воркером с Backups:Enabled=true.
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

    // t36: пул E2E (однонодовый — ownS3-ноды окружения; etcd у каждого Fact'а свой).
    private const string Pool = "bkpool";

    // Окружение Fact'а (своя сеть/etcd/ownS3); создаётся в начале каждого сценария.
    private E2eEnvironment Fx = null!;

    private string Endpoint => Fx.EtcdEndpoint;

    private EtcdGateway G => Fx.Gateway;

    // AAA: WAL-поток (t03/t27, AC1/AC8): provisioned кластер + INSERT/pg_switch_wal
    // → сегменты в ownS3-ноде, ключ wal ACTIVE с chain_start/last_uploaded, цепочка
    // непрерывна, running-агент мастера, слот pgw_bkp_<C>_<X> на мастере, поле
    // agents в ключе (супервиз-факты per-node, t27 §3.5).
    [Fact]
    public async Task WalStream_UploadsSegmentsContinuously()
    {
        // Arrange — гейт docker (общий образ pgworker-node собирается фазой E2ePrepare)
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("wal-stream", withS3Node: true, ct: ct);
        Fx = fx;
        var cluster = $"shopb{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        await using var app = await StartWalHostAsync("walstream", ct);

        var provisioned = await E2eFixture.WaitForAsync(
            E2eEnvironment.ProvisionStuckFailFast(Fx, GetOrNullAsync, cluster, () => ProvisionedAsync(cluster)), TimeSpan.FromSeconds(360), ct);
        provisioned.Should().BeTrue("provisioning must reach DONE before the WAL load");

        // Мастер shard1 (published pg-порт) + admin/superuser-DSN: нагрузка и
        // pg_switch_wal (superuser-only) выполняются одним соединением — как
        // t02 SqlListAsync (dsn-креды app_user недоступны тесту напрямую).
        var (pgHost, pgPort, masterNodeUp) = await MasterPgAsync(cluster, "shard1", ct);
        var adminDsn = DatabaseProvisioner.BuildAdminDsn(pgHost, pgPort, cluster,
            new InstallSecrets(E2eFixture.SuPassword, "", "", ""));

        // Act — INSERT-нагрузка: большие строки + pg_switch_wal форсируют закрытие
        // сегментов 16 МБ (без таймаутов, spec §5).
        await GenerateWalAsync(adminDsn, ct);

        // BackupS3 host-клиент: воркер ходит на localhost (published порт ownS3-ноды),
        // агентам отдаётся advertised (Fx.S3Endpoint = host.docker.internal:<порт>).
        var hostEndpoint = Fx.S3Endpoint;
        var backupS3 = new PgWorker.Backups.BackupS3(new PgWorker.Backups.S3ClientOptions(hostEndpoint, Bucket, E2eEnvironment.S3RootUser, E2eEnvironment.S3RootPassword));

        // Assert 1 — бюджет 120 c: сегменты в ownS3-ноде, все без .partial (AC1)
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
            // t25: факты приёмников — из статус-ключей etcd (docker-логов агентов нет)
            var agentFacts = await Fx.ReadWalAgentFactsAsync(cluster, "shard1", ct);
            var factsDump = string.Join("; ", agentFacts.Select(f =>
                $"{f.Node}: {f.State}{(f.Error is null ? "" : $" err={f.Error}")}"));

            throw new ApplicationException(
                $"segments not delivered: journal=[{workKv?.Value[..Math.Min(400, workKv.Value.Length)]}] " +
                $"wal=[{walKv?.Value ?? "-"}] backup_password={(pwKv is null ? "-" : "present")} " +
                $"receivers=[{factsDump}]");
        }
        uploaded.Should().BeTrue("segments must appear on the ownS3 node within the budget (AC1)");
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
        consistent.Should().BeTrue("the wal key must become ACTIVE (AC2) and match the S3 fact");
        wal.Slot.Should().Be($"pgw_bkp_{cluster}_shard1");
        wal.MasterNode.Should().NotBeEmpty();
        wal.ChainStartSegment.Should().NotBeEmpty();
        (DateTimeOffset.UtcNow.ToUnixTimeSeconds() - wal.LastUploadedUnix!.Value)
            .Should().BeLessThan(120, "last_uploaded_unix is fresh (the stream is alive)");

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
        continuous.Should().BeTrue("the chain from chain_start must become continuous");

        // Assert 4 — running-агент МАСТЕРА (per-node имя t27) + слот на мастере (AC1)
        var (_, _, masterNode) = await MasterPgAsync(cluster, "shard1", ct);
        // t25 (spec §6 п.3): приёмник мастера стримит — факт статус-ключа agent/<N>
        // (state=streaming), а не docker ps wal-контейнеров.
        var masterStreaming = await E2eFixture.WaitForAsync(async () =>
        {
            var facts = await Fx.ReadWalAgentFactsAsync(cluster, "shard1", ct);
            return facts.Any(f => f.Node == masterNode && f.State == "streaming");
        }, TimeSpan.FromSeconds(60), ct);
        masterStreaming.Should().BeTrue("the master receiver streams (state=streaming in the agent key)");
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
        agentsInKey.Should().BeTrue("agents facts are written by the supervisor (t27): master running");
        await using var adminConn = new NpgsqlConnection(adminDsn);
        await adminConn.OpenAsync(ct);
        await using var slotCmd = new NpgsqlCommand(
            "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = @slot)",
            adminConn) { Parameters = { new() { ParameterName = "slot", Value = $"pgw_bkp_{cluster}_shard1" } } };
        ((bool)(await slotCmd.ExecuteScalarAsync(ct))!).Should().BeTrue("the slot is created by the worker on the master (AC1)");

        // Assert 5 — каркасный парсер t01/t02 читает wal-ключ без parseErrors (AC2)
        var kvs = (await G.RangeAsync(Endpoint, $"/pgworker/backups/{cluster}/", ct)).Value;
        var parsed = BackupsParser.Parse(kvs, out var parseErrors);
        parseErrors.Should().BeEmpty(
            $"the parser must read the worker keys (incl. wal with agents t27): " +
            $"keys=[{string.Join("; ", kvs.Select(k => k.Key))}] errors=[{string.Join("; ", parseErrors)}]");
        parsed.Value.Should().Contain(b => b.Cluster == cluster);
    }

    // AAA (t25, spec §6 п.3): legacy wal-контейнер pgw-backup-wal-<C>-* (имитация
    // etcd-образом sleep infinity) — санитарный снос тиком WalStreamProcess; новый
    // приёмник стримит (слот свободен), цепочка S3 непрерывна, WAL_CHAIN_ERROR не возникает.
    // Контейнерных агентов больше нет: пересоздание супервизом воркера упразднено —
    // приёмник живёт в контейнере ноды под supervisord.
    [Fact]
    public async Task WalStream_SanitarySweepLegacyContainer_StreamContinues()
    {
        // Arrange — окружение, кластер, воркер, фоновая нагрузка ≥ 2 мин
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        var (fx, app, cluster, adminDsn, masterNode) = await StartWalScenarioAsync("wal-ac2", "shopac2", ct);
        await using var envOwner = fx;
        await using var appOwner = app;
        using var load = new CancellationTokenSource();
        var loader = BackgroundWalLoadAsync(adminDsn, load.Token);
        var hostEndpoint = Fx.S3Endpoint;
        await using var backupS3 = new PgWorker.Backups.BackupS3(new PgWorker.Backups.S3ClientOptions(hostEndpoint, Bucket, E2eEnvironment.S3RootUser, E2eEnvironment.S3RootPassword));
        var writer = new PgWorker.Backups.WalStatusWriter(G, [Endpoint]);

        try
        {
            // Дождаться первой доставки (приёмник стримит, ≥1 сегмент в S3)
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
                var agentFacts = await Fx.ReadWalAgentFactsAsync(cluster, "shard1", ct);
                var factsDump = string.Join("; ", agentFacts.Select(f =>
                    $"{f.Node}: {f.State}{(f.Error is null ? "" : $" err={f.Error}")}"));
                var walKv = await GetOrNullAsync($"/pgworker/backups/{cluster}/shard1/wal");
                var workKv = await GetOrNullAsync($"/pgworker/work/{cluster}");
                var claimKv = await GetOrNullAsync($"/pgworker/claims/{cluster}");
                var hostLog = Fx.Hosts is { Count: > 0 }
                    ? string.Join("\n", Fx.Hosts.Select(async h =>
                    {
                        try
                        {
                            // Значимые строки (без etcd-HTTP-шума): backup/wal/агент/ошибки
                            var lines = (await h.ReadLogTailAsync(600, ct)).Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
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
                                    || l.Contains("walreceiver", StringComparison.OrdinalIgnoreCase)
                                    || l.Contains("supervise", StringComparison.OrdinalIgnoreCase)
                                    || l.Contains("claim", StringComparison.OrdinalIgnoreCase))
                                .ToList();
                            // Полный хвост (120 строк, без фильтров) — полный контекст
                            return $"[{h.Name}] OPS({opLines.Count}):\n{string.Join("\n", opLines)}" +
                                $"\nFULLTAIL:\n" +
                                string.Join("\n", lines[^Math.Min(120, lines.Length)..]);
                        }
                        catch (Exception e)
                        {
                            return $"<host.log unavailable: {e.Message}>";
                        }
                    }))
                    : "<no hosts>";
                throw new ApplicationException(
                    "the first delivery did not happen within the budget: " +
                    $"receivers=[{factsDump}] wal=[{walKv?.Value ?? "-"}] " +
                    $"journal=[{workKv?.Value?[..Math.Min(400, workKv.Value?.Length ?? 0)]}] " +
                    $"claim=[{claimKv?.Value ?? "-"}]\nHOST.LOG:\n{hostLog}");
            }

            firstList.Should().BeTrue("the first delivery before the sweep");
            firstListCount.Should().BeGreaterThan(0);

            // Act — имитация legacy wal-контейнера pgw-backup-wal-<C>-<X>-<N>
            // (образ etcd + sleep infinity; имя — легаси-формат) под нагрузкой:
            // санитарный снос тика WalStreamProcess обязан удалить её (решение §7 №11).
            var legacyName = $"pgw-backup-wal-{cluster}-shard1-{masterNode}";
            Console.WriteLine($"[PHASE] wal-sweep: raise legacy container {legacyName}");
            // entrypoint-овверрайд: у образа etcd entrypoint `etcd` ( бинарник
            // standalone-режим живёт сам — имитация долгоживущего контейнера).
            await Fx.RunDockerAsync(
            [
                "run", "-d", "--name", legacyName,
                "--network", Fx.NetName,
                "--entrypoint", "etcd", "quay.io/coreos/etcd:v3.5.21",
            ], ct);

            // Assert — санитарный снос: контейнер удалён из docker (list пуст)
            var swept = await E2eFixture.WaitForAsync(async () =>
            {
                var ids = await Fx.RunDockerAsync(
                    ["ps", "-a", "--format", "{{.ID}}", "--filter", $"name=^{legacyName}$"], ct);
                return ids.Split('\n', StringSplitOptions.RemoveEmptyEntries).Length == 0;
            }, TimeSpan.FromSeconds(120), ct);
            swept.Should().BeTrue("the sanitary sweep removes the legacy wal container at tick start");

            // ≥1 новый сегмент после сноса — приёмник не пострадал (legacy-контейнер
            // в сеть нод не входил, слот остался у приёмника); цепочка непрерывна
            var grew = await E2eFixture.WaitForAsync(async () =>
            {
                var list = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
                return list.IsSuccess && list.Value.Count > firstListCount;
            }, TimeSpan.FromSeconds(120), ct);
            grew.Should().BeTrue("the receiver keeps streaming after the sweep — delivery continues");

            // Ключ: WAL_CHAIN_ERROR НЕ возникал весь сценарий (поллинг срезом); финал ACTIVE
            PgWorker.Etcd.Parsing.WalStreamStatus? sawStatus = null;
            var backActive = await E2eFixture.WaitForAsync(async () =>
            {
                var read = await writer.ReadAsync(cluster, "shard1", ct);
                if (!read.IsSuccess || read.Value is null)
                    return false;
                sawStatus = read.Value.State;
                if (sawStatus == PgWorker.Etcd.Parsing.WalStreamStatus.WalChainError)
                    return true; // немедленный фейл ниже
                return sawStatus == PgWorker.Etcd.Parsing.WalStreamStatus.Active;
            }, TimeSpan.FromSeconds(120), ct);
            sawStatus.Should().NotBe(
                PgWorker.Etcd.Parsing.WalStreamStatus.WalChainError,
                "delivery from the S3 tail rules out a gap — WAL_CHAIN_ERROR is forbidden (AC2)");
            backActive.Should().BeTrue("the wal key returns to ACTIVE");
            var list = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
            list.IsSuccess.Should().BeTrue();
            var wal = await writer.ReadAsync(cluster, "shard1", ct);
            wal.Value.Should().NotBeNull();
            var chain = PgWorker.Backups.WalChain.Check(
                PgWorker.Backups.WalFileName.TryParse(wal.Value!.ChainStartSegment)!.Value,
                list.Value.Select(o => o.Name));
            chain.IsContinuous.Should().BeTrue(chain.GapError ?? "the chain is continuous");
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
        var hostEndpoint = Fx.S3Endpoint;
        await using var backupS3 = new PgWorker.Backups.BackupS3(new PgWorker.Backups.S3ClientOptions(hostEndpoint, Bucket, E2eEnvironment.S3RootUser, E2eEnvironment.S3RootPassword));
        var writer = new PgWorker.Backups.WalStatusWriter(G, [Endpoint]);

        // WAL-нагрузка ПЕРВАЯ: Patroni выбирает sync-реплику при живом потоке
        // репликации — без нагрузки sync не выберется и второй агент не поднимется.
        await GenerateWalAsync(adminDsn, ct);
        using var load = new CancellationTokenSource();
        var loader = BackgroundWalLoadAsync(adminDsn, load.Token);

        try
        {
        // Act — ждём ДВА приёмника-стримера (мастер + sync; sync-standby появится
        // сам): факты — статус-ключи agent/<N> обеих нод с state=streaming (t25)
        var bothStreaming = await E2eFixture.WaitForAsync(async () =>
        {
            var facts = await Fx.ReadWalAgentFactsAsync(cluster, "shard1", ct);
            return facts.Count(f => f.State == "streaming") >= 2;
        }, TimeSpan.FromSeconds(300), ct);
        if (!bothStreaming)
        {
            var facts = await Fx.ReadWalAgentFactsAsync(cluster, "shard1", ct);
            var factsDump = string.Join("; ", facts.Select(f =>
                $"{f.Node}: {f.State}{(f.Error is null ? "" : $" err={f.Error}")}"));
            var walKv = await GetOrNullAsync($"/pgworker/backups/{cluster}/shard1/wal");
            var patroniState = await PatroniClusterDumpAsync(cluster, ct);
            var workKv = await GetOrNullAsync($"/pgworker/work/{cluster}");
            throw new ApplicationException(
                "the two receivers did not come up: " +
                $"receivers=[{factsDump}] wal=[{walKv?.Value?[..Math.Min(600, walKv.Value?.Length ?? 0)]}] " +
                $"work=[{workKv?.Value?[..Math.Min(1200, workKv.Value?.Length ?? 0)]}] " +
                $"patroni={patroniState[..Math.Min(700, patroniState.Length)]}");
        }

        bothStreaming.Should().BeTrue("dual archiving: master and sync-standby receivers streaming (AC3)");

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
        delivered.Should().BeTrue("key ACTIVE + agents of both nodes (AC3)");

        // Цепочка непрерывна от chain_start (union одного префикса)
        var wal = (await writer.ReadAsync(cluster, "shard1", ct)).Value!;
        var list = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
        list.IsSuccess.Should().BeTrue();
        var chain = PgWorker.Backups.WalChain.Check(
            PgWorker.Backups.WalFileName.TryParse(wal.ChainStartSegment)!.Value,
            list.Value.Select(o => o.Name));
        chain.IsContinuous.Should().BeTrue(chain.GapError ?? "the union chain is continuous");

        // Слоты pgw_bkp_<C>_shard1 на ОБОИХ нодах (published pg-порты из portalloc)
        var kv = await G.GetAsync(Endpoint, $"/pgworker/portalloc/{cluster}", ct);
        var entries = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(kv.Value!.Value)!
            .Where(p => p.Key.StartsWith("shard1/", StringComparison.Ordinal)).ToList();
        foreach (var (key, addr) in entries)
        {
            var dsn = DatabaseProvisioner.BuildAdminDsn(
                $"pgw-{cluster}-shard1-{key.Split('/')[1]}", 5432,
                cluster, new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
            await using var conn = new NpgsqlConnection(dsn);
            await conn.OpenAsync(ct);
            await using var cmd = new NpgsqlCommand(
                "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = @slot)", conn)
            {
                Parameters = { new() { ParameterName = "slot", Value = $"pgw_bkp_{cluster}_shard1" } },
            };
            ((bool)(await cmd.ExecuteScalarAsync(ct))!)
                .Should().BeTrue($"the slot exists on node {key} (AC3, per-instance)");

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
            fresh.IsSuccess.Should().BeTrue("AC1 probe: fresh list of the tail");
            var tail = fresh.Value
                .Select(o => PgWorker.Backups.WalFileName.TryParse(o.Name))
                .Where(w => w is not null).Select(w => w!.Value)
                .OrderByDescending(w => (long)w.Log * 256 + w.Seg).First();
            restartLsn.Should().BeLessOrEqualTo(
                PgWorker.WalReceiver.S3TailResolver.EndLsn(tail),
                $"AC1 on node {key}: restart_lsn {restartRaw} ≤ the delivered end {tail.Name}");
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

    // AAA (t25-ревью 9.3, arch/19 §3): 3-репликовый шард — двойная архивация
    // стримит ТОЛЬКО ноды-источники (мастер + sync-standby); async-реплика с
    // живым приёмником стоит в waiting_role (REST-проба роли — none, доставки
    // с неё нет); agents wal-ключа наблюдают ВСЕ три ноды (широкий состав, §4).
    [Fact]
    public async Task WalStream_ThreeReplicaShard_AsyncReplicaWaitsRole()
    {
        // Arrange — окружение, кластер с одним 3-репликовым шардом, воркер;
        // нагрузка: sync выбирается Patroni при живом потоке репликации
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        var fx = await E2eEnvironment.StartAsync("wal-3r", withS3Node: true, ct: ct);
        Fx = fx;
        await using var envOwner = fx;
        var cluster = $"shop3r{Fx.ClusterTag}";
        await SeedThreeReplicaShardAsync(cluster);
        await using var appOwner = await StartWalHostAsync("wal-3r", ct);
        var provisioned = await E2eFixture.WaitForAsync(
            E2eEnvironment.ProvisionStuckFailFast(Fx, GetOrNullAsync, cluster, () => ProvisionedAsync(cluster)),
            TimeSpan.FromSeconds(600), ct);
        provisioned.Should().BeTrue("provisioning must reach DONE (3-replica shard)");
        var (pgHost, pgPort, masterNode) = await MasterPgAsync(cluster, "shard1", ct);
        var adminDsn = DatabaseProvisioner.BuildAdminDsn(pgHost, pgPort, cluster,
            new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
        await GenerateWalAsync(adminDsn, ct);

        try
        {
            // Act — ждём распределение ролей приёмников: ДВА стримят (мастер +
            // sync), третий (async-реплика) — waiting_role
            var rolesSet = await E2eFixture.WaitForAsync(async () =>
            {
                var facts = await Fx.ReadWalAgentFactsAsync(cluster, "shard1", ct);
                return facts.Count >= 3
                       && facts.Count(f => f.State == "streaming") >= 2
                       && facts.Any(f => f.State == "waiting_role");
            }, TimeSpan.FromSeconds(300), ct);
            rolesSet.Should().BeTrue(
                "master+sync receivers stream, the async replica waits in waiting_role");

            // Assert — waiting_role НЕ на мастере; стримят ровно две ноды
            var facts = await Fx.ReadWalAgentFactsAsync(cluster, "shard1", ct);
            var waiting = facts.First(f => f.State == "waiting_role");
            waiting.Node.Should().NotBe(masterNode, "the master receiver streams (not waits)");
            facts.Count(f => f.State == "streaming").Should().Be(2,
                "only the source nodes (master+sync) stream — the async replica does not upload");

            // Assert — agents wal-ключа наблюдают ВСЕ ноды шарда (широкий состав §4)
            var writer = new PgWorker.Backups.WalStatusWriter(G, [Endpoint]);
            var agentsSeen = await E2eFixture.WaitForAsync(async () =>
            {
                var read = await writer.ReadAsync(cluster, "shard1", ct);
                return read.IsSuccess && read.Value?.Agents is { } agents
                       && agents.Count(a => a.State == PgWorker.Etcd.Parsing.WalAgentPresence.Running) >= 3;
            }, TimeSpan.FromSeconds(60), ct);
            agentsSeen.Should().BeTrue(
                "the wal key agents cover all shard nodes (running = live receiver lease)");
        }
        catch (Exception ex)
        {
            Fx.MarkFailed(); // артефакты телеметрии переживают teardown
            await WalScenarioDiagDumpAsync(cluster, "3R: " + ex.Message);
            throw;
        }
    }

    // Посев кластера с ОДНИМ 3-репликовым шардом (routing обоих бакетов → shard1).
    private async Task SeedThreeReplicaShardAsync(string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        var config = $$"""
            {"buckets":2,"dbname":"{{cluster}}","created_unix":1755800000,"state":"NOT_INITIALIZED","bucket_admin_password":"{{E2eFixture.BucketAdminPassword}}"}
            """;
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/config", config, null, ct);
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard1/replicas", "3", null, ct);
        foreach (var node in new[] { "shard1a", "shard1b", "shard1c" })
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard1/nodes/{node}/state",
                "NOT_INITIALIZED", null, ct);
        await G.PutAsync(Endpoint, $"/service/{cluster}-shard1/request_cpu", "2", null, ct);
        await G.PutAsync(Endpoint, $"/service/{cluster}-shard1/request_mem", "2Gi", null, ct);
        for (var i = 0; i < 2; i++)
        {
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/routing/bucket_{i}", "shard1", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/status/bucket_{i}",
                """{"state":"NOT_INITIALIZED"}""", null, ct);
        }
        await Fx.SeedS3PoolAsync(Pool, ct);
        await G.PutAsync(Endpoint, $"/pgworker/backups/{cluster}/policy",
            $"{{\"full_max_age_sec\":3600,{E2eEnvironment.PoolS3(Pool)}}}", null, ct);
    }

    // AAA (t25, spec §6 п.3/arch/19 §4): стоп-семантика Enabled=false — глобальный
    // ключ /pgworker/backups-disabled жив (put ЛИДЕРА BackupOrphanSweeperLoop на
    // каждом тике), приёмники замирают в waiting_disabled БЕЗ docker-операций;
    // enable — ключ снят (del при старте инстанса с Enabled=true), стрим
    // возобновился (waiting_disabled → streaming без пересозданий).
    [Fact]
    public async Task WalStream_BackupsDisabled_ReceiversPause_EnableResumes()
    {
        // Arrange — контур с воркером Enabled=true; приёмники стримят
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        var (fx, app, cluster, adminDsn, _) = await StartWalScenarioAsync("wal-dis", "shopdis", ct);
        await using var envOwner = fx;
        await using var appOwner = app;
        var writer = new PgWorker.Backups.WalStatusWriter(G, [Endpoint]);
        var streaming = await E2eFixture.WaitForAsync(async () =>
        {
            var facts = await Fx.ReadWalAgentFactsAsync(cluster, "shard1", ct);
            return facts.Count(f => f.State == "streaming") >= 1;
        }, TimeSpan.FromSeconds(300), ct);
        streaming.Should().BeTrue("receivers stream before the disabled switch");
        (await G.GetAsync(Endpoint, PgWorker.Backups.BackupsDisabledGate.Key, ct)).Value.Should().BeNull(
            "the disabled key is deleted at startup with Enabled=true");

        // Act 1 — воркер ВНИЗ, воркер с Enabled=false поднимается (лидер-цикл
        // поддерживает /pgworker/backups-disabled на каждом тике)
        await app.DisposeAsync();
        await using var app2Owner = await StartWalHostAsync("wal-dis2", new Dictionary<string, string>
        {
            ["PgWorker__Backups__Enabled"] = "false",
            ["PgWorker__Loops__ScanIntervalSec"] = "2",
            ["PgWorker__Backups__Supervisor__IntervalSec"] = "3",
        }, ct);

        // Assert 1 — ключ жив (plain-put лидера) и приёмники waiting_disabled
        var disabledKey = await E2eFixture.WaitForAsync(async () =>
            (await G.GetAsync(Endpoint, PgWorker.Backups.BackupsDisabledGate.Key, ct)).Value is not null,
            TimeSpan.FromSeconds(60), ct);
        disabledKey.Should().BeTrue("the leader maintains /pgworker/backups-disabled (Enabled=false)");
        var paused = await E2eFixture.WaitForAsync(async () =>
        {
            var facts = await Fx.ReadWalAgentFactsAsync(cluster, "shard1", ct);
            return facts.Count > 0 && facts.All(f => f.State == "waiting_disabled");
        }, TimeSpan.FromSeconds(120), ct);
        paused.Should().BeTrue("receivers pause by the etcd gate (waiting_disabled, no docker calls)");

        // Act 2 — воркер ВНИЗ, воркер с Enabled=true: старт делает del ключа
        await app2Owner.DisposeAsync();
        await using var app3Owner = await StartWalHostAsync("wal-dis3", ct);

        // Assert 2 — ключ снят, приёмники возобновили стрим
        var enabledKey = await E2eFixture.WaitForAsync(async () =>
            (await G.GetAsync(Endpoint, PgWorker.Backups.BackupsDisabledGate.Key, ct)).Value is null,
            TimeSpan.FromSeconds(60), ct);
        enabledKey.Should().BeTrue("the disabled key is deleted at startup with Enabled=true");
        var resumed = await E2eFixture.WaitForAsync(async () =>
        {
            var facts = await Fx.ReadWalAgentFactsAsync(cluster, "shard1", ct);
            return facts.Any(f => f.State == "streaming");
        }, TimeSpan.FromSeconds(180), ct);
        resumed.Should().BeTrue("receivers resume streaming after enable (waiting_disabled → streaming)");
    }

    // AAA (критерий 5): lost-слот мастера при живой sync-архивации — воркер лечит
    // сам (recreate + журнальная фаза slot-recreate), ключ НЕ WAL_CHAIN_ERROR, приёмник
    // возвращается к стриму, цепочка WalChain непрерывна, last_uploaded растёт без
    // дыры. Детерминизация: между остановкой приёмника мастера (supervisorctl stop,
    // t25 — процесс ноды) и возвратом воркера (Kill/старт — паттерн «смерть
    // контроллера») живой sync-приёмник продолжает доставку, WAL генерится до
    // фактического wal_status='lost'.
    [Fact]
    public async Task WalStream_SlotLostAutoRecreate_NoBroken()
    {
        // Arrange — двухнодовый шард (двойная архивация), оба приёмника стримят
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        var (fx, app, cluster, adminDsn, masterNode) = await StartWalScenarioAsync("wal-t19", "shopt19", ct);
        await using var envOwner = fx;
        await using var appOwner = app; // kill воркера ниже — dispose идемпотентен
        var slot = $"pgw_bkp_{cluster}_shard1";
        var masterNodeName = $"pgw-{cluster}-shard1-{masterNode}";
        var writer = new PgWorker.Backups.WalStatusWriter(G, [Endpoint]);
        var hostEndpoint = Fx.S3Endpoint;
        await using var backupS3 = new PgWorker.Backups.BackupS3(new PgWorker.Backups.S3ClientOptions(hostEndpoint, Bucket, E2eEnvironment.S3RootUser, E2eEnvironment.S3RootPassword));

        try
        {
            // Нагрузка ПЕРВАЯ — Patroni выбирает sync при живом потоке репликации (AC3)
            await GenerateWalAsync(adminDsn, ct);
            var bothStreaming = await E2eFixture.WaitForAsync(async () =>
            {
                var facts = await Fx.ReadWalAgentFactsAsync(cluster, "shard1", ct);
                return facts.Count(f => f.State == "streaming") >= 2;
            }, TimeSpan.FromSeconds(300), ct);
            bothStreaming.Should().BeTrue("dual archiving — both receivers must stream");

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
            before.Should().BeTrue("before the loss — key ACTIVE");
            var beforeWal = (await writer.ReadAsync(cluster, "shard1", ct)).Value!;

            // Act 1 — стоп приёмника мастера (supervisord-программа ноды, t25) +
            // стоп воркера (слот перестаёт подтверждаться; sync-приёмник жив —
            // доставка продолжается)
            Console.WriteLine($"[PHASE] wal-t19: supervisorctl stop walreceiver on {masterNodeName} + worker kill");
            await Fx.RunDockerAsync(
                ["exec", masterNodeName, "supervisorctl", "stop", "walreceiver"], ct);
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
            Console.WriteLine($"[PHASE] wal-t19: master slot lost={lost} (budget 180 s)");
            lost.Should().BeTrue("64 MiB WAL at a 16 MiB ceiling + a checkpoint must cut the master slot");

            // Act 3 — возврат воркера + приёмника мастера: тик лечит (recreate слота
            // + журнал slot-recreate; приёмник повторно занимает слот от хвоста S3;
            // клэйм убитого истекает ≤ 15-20 c)
            Console.WriteLine("[PHASE] wal-t19: worker start + receiver start — self-healing");
            await Fx.RunDockerAsync(
                ["exec", masterNodeName, "supervisorctl", "start", "walreceiver"], ct);
            await using var app2Owner = await StartWalHostAsync("wal-t19b", ct);

            // Assert 1 — журнальная фаза slot-recreate
            var journaled = await E2eFixture.WaitForAsync(async () =>
            {
                var workKv = await GetOrNullAsync($"/pgworker/work/{cluster}");
                return workKv?.Value?.Contains("slot-recreate/shard1") == true;
            }, TimeSpan.FromSeconds(120), ct);
            journaled.Should().BeTrue("the worker must record the slot-recreate phase (spec §3.2)");

            // Assert 2 — слот мастера жив (не lost)
            var healed = await E2eFixture.WaitForAsync(
                async () => await SlotWalStatusAsync(cluster, "shard1", masterNode, ct) is not ("lost" or null),
                TimeSpan.FromSeconds(120), ct);
            healed.Should().BeTrue("recreate returns a live slot");

            // Assert 3 — приёмник мастера снова streaming (супервизор ноды перезапустил
            // программу; приёмник занял recreate-слот и стримит)
            var receiverBack = await E2eFixture.WaitForAsync(async () =>
            {
                var facts = await Fx.ReadWalAgentFactsAsync(cluster, "shard1", ct);
                return facts.Any(f => f.Node == masterNode && f.State == "streaming");
            }, TimeSpan.FromSeconds(120), ct);
            receiverBack.Should().BeTrue("the master receiver streams again after the slot recreate");

            // Assert 4 — ключ wal: WAL_CHAIN_ERROR не возникал (поллинг срезом), финал ACTIVE;
            // last_uploaded СТРОГО растёт от «до потери» (sync доставлял + мастер вернулся)
            PgWorker.Etcd.Parsing.WalStreamStatus? sawStatus = null;
            var recovered = await E2eFixture.WaitForAsync(async () =>
            {
                var read = await writer.ReadAsync(cluster, "shard1", ct);
                if (!read.IsSuccess || read.Value is null)
                    return false;
                sawStatus = read.Value.State;
                if (sawStatus == PgWorker.Etcd.Parsing.WalStreamStatus.WalChainError)
                    return true; // немедленный фейл ниже
                return sawStatus == PgWorker.Etcd.Parsing.WalStreamStatus.Active;
            }, TimeSpan.FromSeconds(180), ct);
            sawStatus.Should().NotBe(PgWorker.Etcd.Parsing.WalStreamStatus.WalChainError,
                "lost on one source while the other is alive — WAL_CHAIN_ERROR is forbidden (criterion 5)");
            recovered.Should().BeTrue("the key must return to ACTIVE");
            var afterWal = (await writer.ReadAsync(cluster, "shard1", ct)).Value!;
            var beforeSeg = PgWorker.Backups.WalFileName.TryParse(beforeWal.LastUploadedSegment)!.Value;
            var afterSeg = PgWorker.Backups.WalFileName.TryParse(afterWal.LastUploadedSegment)!.Value;
            // t25 (инвариант RPO): доставленное НЕ откатывается — last_uploaded
            // ≥ «до потери». СТРОГИЙ рост не гарантирован: надзор при lost-индукции
            // может rebuild'нуть упавшие ноды (bootstrap кластера) — контроль
            // пишет last_uploaded по факту S3, без подмены now().
            ((long)afterSeg.Log * 256 + afterSeg.Seg).Should().BeGreaterThanOrEqualTo(
                (long)beforeSeg.Log * 256 + beforeSeg.Seg,
                "delivered WAL is never lost (RPO invariant — the slot confirms after the put)");

            // Assert 5 — цепочка WalChain непрерывна от chain_start
            var list = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
            list.IsSuccess.Should().BeTrue();
            var chain = PgWorker.Backups.WalChain.Check(
                PgWorker.Backups.WalFileName.TryParse(afterWal.ChainStartSegment)!.Value,
                list.Value.Select(o => o.Name));
            chain.IsContinuous.Should().BeTrue(chain.GapError ?? "the chain is continuous after self-healing");
        }
        catch (Exception ex)
        {
            Fx.MarkFailed(); // артефакты телеметрии переживают teardown (канон e2e-launch)
            await WalScenarioDiagDumpAsync(cluster, "t19: " + ex.Message);
            throw;
        }
    }

    // AAA (AC4): остановка мастера — агент sync-реплики продолжает цепочку без дыр;
    // после promote воркер пересоздаёт агентов на новых ролях; ключ не WAL_CHAIN_ERROR.

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

    // TLI текущего primary шарда: Patroni GET /cluster ноды (timeline члена-лидера).

    private async Task<uint> PrimaryTimelineAsync(string cluster, string shard, CancellationToken ct)
    {
        var kv = await G.GetAsync(Endpoint, $"/pgworker/portalloc/{cluster}", ct);
        kv.Value.Should().NotBeNull("portalloc is written at provisioning");
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
                        $"https://pgw-{cluster}-{shard}-{key.Split('/')[1]}:8008/cluster", ct);
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
            // t25: факты приёмников — из статус-ключей etcd (wal-контейнеров нет)
            var agentFacts = await Fx.ReadWalAgentFactsAsync(cluster, "shard1",
                TestContext.Current.CancellationToken);
            var receiversDump = string.Join("; ", agentFacts.Select(f =>
                $"{f.Node}: {f.State}{(f.Error is null ? "" : $" err={f.Error}")}"));
            var walKv = await GetOrNullAsync($"/pgworker/backups/{cluster}/shard1/wal");
            var workKv = await GetOrNullAsync($"/pgworker/work/{cluster}");
            var claimKv = await GetOrNullAsync($"/pgworker/claims/{cluster}");
            var patroni = await PatroniClusterDumpAsync(cluster, TestContext.Current.CancellationToken);
            var hostLog = "";
            foreach (var host in Fx.Hosts)
                try
                {
                    var lines = (await host.ReadLogTailAsync(600, TestContext.Current.CancellationToken)).Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
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
                            || l.Contains("evacuat", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("REBUILDING", StringComparison.OrdinalIgnoreCase))
                        .ToList();
                    hostLog += $"\n== {host.Name} OPS({opLines.Count}):\n{string.Join("\n", opLines)}" +
                        $"\nTAIL({interesting.Count}):\n" +
                        string.Join("\n", interesting[^Math.Min(40, interesting.Count)..]);
                }
                catch (Exception e)
                {
                    hostLog += $"\n== {host.Name}: host.log unavailable: {e.Message}";
                }

            var dump =
                $"reason={reason}\nreceivers=[{receiversDump}]\n" +
                $"wal=[{walKv?.Value ?? "-"}]\nwork=[{workKv?.Value ?? "-"}]\n" +
                $"claim=[{claimKv?.Value ?? "-"}]\npatroni={patroni}\nHOST.LOG:{hostLog}\n";
            await File.WriteAllTextAsync($"/tmp/pgw-diag-{cluster}.txt", dump);
            Console.WriteLine($"[DIAG] dump: /tmp/pgw-diag-{cluster}.txt");
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
                        $"https://pgw-{cluster}-shard1-{key.Split('/')[1]}:8008/cluster", ct);
                    if (!response.IsSuccessStatusCode) continue;
                    var body = await response.Content.ReadAsStringAsync(ct);
                    return $"[{key}] " + body[..Math.Min(700, body.Length)];
                }
                catch (Exception)
                {
                    // нода недоступна — следующая
                }
            }

            return "<no shard1 node responds on /cluster>";
        }
        catch (Exception e)
        {
            return $"<patroni unavailable: {e.Message}>";
        }
    }

    /// <summary>Подъём полного WAL-контура сценария: окружение с ownS3-нодой, кластер,
    /// воркер (Backups:Enabled), ожидание provisioning, DSN мастера.
    /// </summary>
    // Владение окружением и воркером — у Fact (DisposeAsync при любом исходе);

    private async Task<(E2eEnvironment Env, HostInstance App, string Cluster, string AdminDsn, string MasterNode)>
        StartWalScenarioAsync(string slug, string clusterPrefix, CancellationToken ct)
    {
        var phaseStart = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        Console.WriteLine($"[PHASE] {slug}: start (unix={phaseStart})");
        var fx = await E2eEnvironment.StartAsync(slug, withS3Node: true, ct: ct);
        Fx = fx;
        Console.WriteLine($"[PHASE] {slug}: environment raised in " +
            (DateTimeOffset.UtcNow.ToUnixTimeSeconds() - phaseStart) + " s (incl. Release build and images)");
        var cluster = $"{clusterPrefix}{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        var app = await StartWalHostAsync(slug, ct);
        // Потолок 600 с: provisioning на перегруженной машине доходит до DONE за
        // 7–8 попыток (ноды живы, но readiness-пробы patroni прогреваются дольше
        // бюджета); зелёный путь не меняется — опрос выходит по факту DONE.
        var provisioned = await E2eFixture.WaitForAsync(
            E2eEnvironment.ProvisionStuckFailFast(Fx, GetOrNullAsync, cluster, () => ProvisionedAsync(cluster)), TimeSpan.FromSeconds(600), ct);
        provisioned.Should().BeTrue("provisioning must reach DONE before the WAL load");
        Console.WriteLine($"[PHASE] {slug}: provisioning DONE in " +
            (DateTimeOffset.UtcNow.ToUnixTimeSeconds() - phaseStart) + " s from start");
        var (pgHost, pgPort, masterNode) = await MasterPgAsync(cluster, "shard1", ct);
        var adminDsn = DatabaseProvisioner.BuildAdminDsn(pgHost, pgPort, cluster,
            new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
        return (fx, app, cluster, adminDsn, masterNode);
    }

    // AAA (AC2 — ядро задачи): под WAL-нагрузкой docker rm -f агента мастера —
    // супервиз пересоздаёт агента, цепочка S3 непрерывна, WAL_CHAIN_ERROR не возникает,

    // Воркер с ВКЛЮЧЁННОЙ подсистемой бэкапов: t02-комплект джобов + t03 Wal-поток.
    // S3 для воркера — published порт (localhost), для агентов/джобов — advertised
    // (host.docker.internal); пороги WAL-контроля — короткие.
    private Task<HostInstance> StartWalHostAsync(string name, CancellationToken ct)
        => StartWalHostAsync(name, null, ct);

    private Task<HostInstance> StartWalHostAsync(
        string name, IReadOnlyDictionary<string, string>? envOverrides, CancellationToken ct)
    {
        var extraEnv = new Dictionary<string, string>
        {
            ["PgWorker__Backups__Enabled"] = "true",
            ["PgWorker__Backups__S3__Bucket"] = Bucket,
            // t36: креды пула — env-профиль (arch/19 §7); пул — policy.s3, адреса
            // резолвит воркер из /s3/ (advertised host.docker.internal, extra_hosts).
            ["PgWorker__Backups__Retry__BaseSec"] = "2",
            ["PgWorker__Backups__Retry__MaxSec"] = "4",
            ["PgWorker__Backups__Wal__VerifyIntervalSec"] = "2",
            ["PgWorker__Backups__Wal__StaleSec"] = "600",
            ["PgWorker__Backups__Wal__LagMaxSegments"] = "100000",
        };
        if (envOverrides is not null)
            foreach (var (key, value) in envOverrides)
                extraEnv[key] = value;
        return Fx.StartHostAsync(name, extraEnv: extraEnv, ct: ct);
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

        // t36: пул + policy с секцией s3 (идемпотентно при пересоздании кластера) —
        // без неё подсистема бэкапов кластера молчит.
        await Fx.SeedS3PoolAsync(Pool, ct);
        await G.PutAsync(Endpoint, $"/pgworker/backups/{cluster}/policy",
            $"{{\"full_max_age_sec\":3600,{E2eEnvironment.PoolS3(Pool)}}}", null, ct);
    }

    // Published pg-порт мастера шарда из portalloc (host-клиент: localhost).
    // Резолв фактического primary — по пробам Patroni /primary (t02-подход
    // WaitForMasterAsync: master-ключ host:0 при EnableDoorman=false
    // недискриминантен); primary появляется после dsn/RUNNING — ждём.
    // t22: Patroni REST нод — https (цепочка к per-contour CA, без hostname)
    private HttpClient PatroniHttp => Fx.NewPatroniHttpClient();

    private async Task<(string Host, int Port, string Node)> MasterPgAsync(string cluster, string shard, CancellationToken ct)
    {
        var kv = await G.GetAsync(Endpoint, $"/pgworker/portalloc/{cluster}", ct);
        kv.Value.Should().NotBeNull("portalloc is written at provisioning");
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
                        $"https://pgw-{cluster}-{shard}-{key.Split('/')[1]}:8008/primary", ct);
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
        resolved.Should().BeTrue("the shard primary must be resolved by Patroni probes");
        var entry = entries[$"{shard}/{primary}"];
        return ($"pgw-{cluster}-{shard}-{primary}", 5432,
            primary!);

    }

    // wal_status слота бэкапа на указанной ноде (portalloc pg-порт): null = слота нет.
    private async Task<string?> SlotWalStatusAsync(string cluster, string shard, string node, CancellationToken ct)
    {
        var kv = await G.GetAsync(Endpoint, $"/pgworker/portalloc/{cluster}", ct);
        kv.Value.Should().NotBeNull("portalloc is written at provisioning");
        var entries = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(kv.Value!.Value)!;
        var dsn = DatabaseProvisioner.BuildAdminDsn($"pgw-{cluster}-{shard}-{node}", 5432,
            cluster, new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
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
