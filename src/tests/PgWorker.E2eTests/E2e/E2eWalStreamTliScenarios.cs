using System.Globalization;
using System.Text.Json;
using Npgsql;
using PgWorker.Core.Templates;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Sql;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E tli-механики приёмника WAL (t24): переживание 58P01 после фейловера —
// досыпка старого таймлайна (RPO=0, AC1) и прыжок-fallback (RPO-зазор, AC2).
// Изолированное окружение E2eEnvironment (своя docker-сеть, свой etcd, СВОЯ
// ownS3-нода — withS3Node:true; порт динамический) + живой кластер воркером
// с Backups:Enabled=true. Имя кластера сценария — {slug}{Fx.ClusterTag}
// (уникально на прогон, docs/e2e-isolation.md §1). Каждый Fact — своё
// окружение: per-method изоляция (движковые контейнеры/тома pgw-*-<C>-*
// опознаются teardown'ом окружения); полный teardown при любом исходе.
public sealed class E2eWalStreamTliScenarios(E2eWalStreamTliScenariosFx Ctx) : IClassFixture<E2eWalStreamTliScenariosFx>
{
    private E2eEnvironment Fx => Ctx.Fx;

    private const string Bucket = "pgworker-backups";

    // t36: пул E2E (однонодовый — ownS3-ноды окружения; etcd у каждого Fact'а свой).
    private const string Pool = "bkpool";

    private string Endpoint => Fx.EtcdEndpoint;

    private EtcdGateway G => Fx.Gateway;

    // AC1 — главный путь (t24a, сходимость §3.7): пауза S3 (слот держит
    // недоставленное) → смерть мастера → promote (TLI 2, точка форка выше
    // хвоста S3) → возврат S3 → приёмник переживает tli-переход (58P01 →
    // досыпка/прыжок по фактической позиции). Ассерты — финальная сходимость:
    // сегменты TLI 1 выше прежнего хвоста доставлены, .history нового TLI в S3,
    // ключ ACTIVE без gap-поля, цепочка непрерывна. Промежуточные события
    // (58P01, stdout-маркеры) — телеметрия, не гейт.
    [Fact]
    public async Task WalStream_TliBackfill_ClosesGap_NoRpoLoss()
    {
        // Arrange — окружение, оба агента running, база цепочки TLI 1
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMinutes(3)); // потолок теста 3 мин: неверное ожидание не может длиться дольше (приказ владельца)
        var ct = cts.Token;
        var (fx, app, cluster, adminDsn, masterNode) = await StartWalScenarioAsync("wal-t24a", "shopt24a", ct);
        await using var appOwner = app;
        var hostEndpoint = Fx.S3Endpoint;
        await using var backupS3 = new PgWorker.Backups.BackupS3(new PgWorker.Backups.S3ClientOptions(hostEndpoint, Bucket, E2eEnvironment.S3RootUser, E2eEnvironment.S3RootPassword));
        var writer = new PgWorker.Backups.WalStatusWriter(G, [Endpoint]);
        var bothStreaming = await E2ePhase.WaitAsync(Fx, "wal-receivers-up", async () =>
        {
            // t25: факты приёмников — статус-ключи agent/<N> (state=streaming)
            var facts = await Fx.ReadWalAgentFactsAsync(cluster, "shard1", ct);
            return facts.Count(f => f.State == "streaming") >= 2;
        }, TimeSpan.FromSeconds(90), ct);
        bothStreaming.Should().BeTrue("both receivers streaming before the failover");
        await GenerateWalAsync(adminDsn, ct); // база цепочки TLI 1

        // Сегменты TLI 1 в S3 — непустой хвост ниже будущей точки форка
        string? tailBefore = null;
        var anchored = await E2ePhase.WaitAsync(Fx, "wal-tail-anchored", async () =>
        {
            var listed = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
            if (!listed.IsSuccess) return false;
            var tli1 = listed.Value.Select(o => o.Name)
                .Where(n => n.Length == 24 && n.StartsWith("00000001", StringComparison.Ordinal))
                .OrderBy(n => n, StringComparer.Ordinal).ToList();
            if (tli1.Count < 2) return false;
            tailBefore = tli1[^1];
            return true;
        }, TimeSpan.FromSeconds(90), ct);
        anchored.Should().BeTrue("the TLI 1 chain is anchored by closed segments before the outage");
        tailBefore.Should().NotBeNull();

        using var load = new CancellationTokenSource();
        var loader = BackgroundWalLoadAsync(adminDsn, load.Token);

        try
        {
            // [PHASE] пауза S3: доставка стоит, слот держит недоставленное
            Console.WriteLine($"[PHASE] wal-t24a: docker stop {Fx.S3NodeContainerName}");
            await Fx.RunDockerAsync(["stop", Fx.S3NodeContainerName], ct);

            // Недоставленная генерация: сегменты закрываются на ноде, слот держит
            await GenerateWalAsync(adminDsn, ct);
            // Выдержка: приёмник уходит в put-ретрай (backpressure), хвост фиксируется
            await Task.Delay(TimeSpan.FromSeconds(10), ct);

            // Жёсткая потеря мастер-ноды → promote: TLI 2, форк выше хвоста S3
            var (_, _, currentMaster) = await MasterPgAsync(cluster, "shard1", ct);
            Console.WriteLine($"[PHASE] wal-t24a: docker stop pgw-{cluster}-shard1-{currentMaster}");
            await Fx.RunDockerAsync(["stop", $"pgw-{cluster}-shard1-{currentMaster}"], ct);
            var (newMasterHost, newMasterPort, _) = await MasterPgAsync(cluster, "shard1", ct);
            var tli = await PrimaryTimelineAsync(cluster, "shard1", ct);
            tli.Should().BeGreaterThanOrEqualTo(2u, "failover opens a new timeline");

            // TLI-граница в закрытом сегменте (фоновый лоадер умер вместе с мастером)
            var newMasterDsn = DatabaseProvisioner.BuildAdminDsn(newMasterHost, newMasterPort, cluster,
                new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
            await SwitchWalsAsync(newMasterDsn, 2, ct);

            // [PHASE] возврат S3: приёмник досыпает удержанное, переживает 58P01
            Console.WriteLine($"[PHASE] wal-t24a: docker start {Fx.S3NodeContainerName}");
            await Fx.RunDockerAsync(["start", Fx.S3NodeContainerName], ct);

            // Прогресс листинга S3 (t29 §3.4): размер, max-TLI сегментов, наличие .history
            Func<Task<string>> walListingProgress = async () =>
            {
                var listed = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
                if (!listed.IsSuccess || listed.Value.Count == 0)
                    return "listing=empty";
                var names = listed.Value.Select(o => o.Name).ToList();
                var maxTli = names.Where(n => !n.EndsWith(".history", StringComparison.Ordinal))
                    .Select(n => Convert.ToUInt32(n[..8], 16)).DefaultIfEmpty(0u).Max();
                return $"objects={names.Count}, maxTli=0x{maxTli:x}, history={(names.Any(n => n.EndsWith(".history", StringComparison.Ordinal)) ? "present" : "absent")}";
            };

            // (а) досыпка: сегменты TLI 1 выше прежнего хвоста доставлены (RPO=0).
            // Промежуточные события (58P01, stdout-маркеры tli-процедуры) НЕ
            // ассертируются — гоночные (пересоздание агентов надзором, §3.2);
            // маркеры остаются в логах агентов артефактов как телеметрия.
            var backfilled = await E2ePhase.WaitAsync(Fx, "wal-tli-backfill-delivered", async () =>
            {
                var listed = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
                return listed.IsSuccess && listed.Value.Select(o => o.Name)
                    .Any(n => n.Length == 24 && n.StartsWith("00000001", StringComparison.Ordinal)
                        && string.CompareOrdinal(n, tailBefore) > 0);
            }, TimeSpan.FromSeconds(90), ct, progress: walListingProgress);
            backfilled.Should().BeTrue("old-timeline segments above the tail are backfilled (AC1)");

            // (б) .history нового TLI в S3
            var history = await E2ePhase.WaitAsync(Fx, "wal-history-present", async () =>
            {
                var listed = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
                return listed.IsSuccess && listed.Value.Any(o => o.Name == $"{tli:x8}.history");
            }, TimeSpan.FromSeconds(90), ct, progress: walListingProgress);
            history.Should().BeTrue($"wal/{tli:x8}.history is uploaded by the receiver (AC1)");

            // (в) ключ ACTIVE, цепочка непрерывна (CheckWithRestart)
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
                    && read.Value.State == WalStreamStatus.Active;
            }, TimeSpan.FromSeconds(90), ct, progress: walListingProgress);
            glueOk.Should().BeTrue("the CheckWithRestart chain is continuous, the key ACTIVE (AC1)");

            // Итоговые ассерты — ключ зазора НЕТ, ошибки tli-gap НЕТ, WAL_CHAIN_ERROR НЕТ
            var readFinal = await writer.ReadAsync(cluster, "shard1", ct);
            readFinal.Value!.Gap.Should().BeNull("backfill closes the gap — no RPO-loss marker (AC1)");
            readFinal.Value.Error.Should().NotContain("tli-gap");
            readFinal.Value.State.Should().NotBe(WalStreamStatus.WalChainError);
        }
        catch (Exception ex)
        {
            Fx.MarkFailed(); // артефакты телеметрии переживают teardown
            await WalScenarioDiagDumpAsync(cluster, "AC1: " + ex.Message);
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

    // t24b (AC2/AC3, E2E-часть) — сходимость после индуцированного среза
    // сегментов (spec §3.7): пауза S3 → записная генерация недоставленного WAL →
    // смерть мастера → promote → срез удержания (drop слота + 160 записных
    // переключений за wal_keep_size=2048 MB/128 сегментов) → возврат S3 →
    // контрольная INSERT. АССЕРТЫ — ТОЛЬКО финальная сходимость (5 инвариантов,
    // §3.7): кластер Active; ключ wal ACTIVE без живого gap-поля; цепочка
    // цела/склеена через TLI-границу (CheckWithRestart); COMPLETED-полный после
    // индукции с verify ≠ FAILED; доставка догнала контрольную вставку.
    // Промежуточные события (58P01, прыжок, stdout-маркеры, метка gap в моменте)
    // НЕ ассертируются — гоночные (пересоздание агентов надзором, §3.2);
    // маркеры — телеметрия артефактов. Несходимость в бюджет — расследование
    // по артефактам (канон docs/e2e-launch.md).
    [Fact]
    public async Task WalStream_TliJump_GapMarked_FullRetaken()
    {
        // Arrange — окружение, оба агента running, база цепочки TLI 1
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMinutes(3)); // потолок теста 3 мин: неверное ожидание не может длиться дольше (приказ владельца)
        var ct = cts.Token;
        var (fx, app, cluster, adminDsn, masterNode) = await StartWalScenarioAsync("wal-t24b", "shopt24b", ct);
        await using var appOwner = app;
        var hostEndpoint = Fx.S3Endpoint;
        await using var backupS3 = new PgWorker.Backups.BackupS3(new PgWorker.Backups.S3ClientOptions(hostEndpoint, Bucket, E2eEnvironment.S3RootUser, E2eEnvironment.S3RootPassword));
        var writer = new PgWorker.Backups.WalStatusWriter(G, [Endpoint]);
        var bothStreaming = await E2ePhase.WaitAsync(Fx, "wal-receivers-up", async () =>
        {
            // t25: факты приёмников — статус-ключи agent/<N> (state=streaming)
            var facts = await Fx.ReadWalAgentFactsAsync(cluster, "shard1", ct);
            return facts.Count(f => f.State == "streaming") >= 2;
        }, TimeSpan.FromSeconds(90), ct);
        bothStreaming.Should().BeTrue("both receivers streaming before the failover");
        await GenerateWalAsync(adminDsn, ct); // база цепочки TLI 1

        // Якорь TLI 1 в S3 — обязательный вход tli-процедуры: без него (гонка
        // «индукция раньше первой доставки TLI 1») хвоста ниже точки форка нет —
        // приёмники стартуют от свежих слотов без 58P01, прыжка/tli_jump не
        // возникает; якорь делает факт tli_jump в agent-ключе (ассерт ниже)
        // детерминированным (канон входа — wal-t24a «wal-tail-anchored»).
        var anchored = await E2ePhase.WaitAsync(Fx, "wal-tail-anchored", async () =>
        {
            var listed = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
            return listed.IsSuccess && listed.Value.Count(o =>
                o.Name.Length == 24 && o.Name.StartsWith("00000001", StringComparison.Ordinal)) >= 2;
        }, TimeSpan.FromSeconds(90), ct);
                anchored.Should().BeTrue("the TLI 1 chain must be anchored in S3 before the outage");

        // Секция outage+jump — свой бюджет 3 мин (приказ владельца: длинный
        // тест — переприсвоение ct по секциям, каждая секция ≤3 мин).
        using var ctsOutage = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        ctsOutage.CancelAfter(TimeSpan.FromMinutes(3));
        ct = ctsOutage.Token;

        using var load = new CancellationTokenSource();
        var loader = BackgroundWalLoadAsync(adminDsn, load.Token);

        try
        {
            // [PHASE] пауза S3 → записная генерация недоставленного WAL → смерть
            // мастера → promote
            Console.WriteLine($"[PHASE] wal-t24b: docker stop {Fx.S3NodeContainerName}");
            await Fx.RunDockerAsync(["stop", Fx.S3NodeContainerName], ct);
            // Недоставленная генерация — записная: pg_switch_wal на idle-базе НЕ
            // материализует сегмент — логическая запись в каждом сегменте.
            await SwitchWithRecordsAsync(adminDsn, 60, ct);
            await Task.Delay(TimeSpan.FromSeconds(10), ct); // приёмник в put-ретрае (backpressure)
            var (_, _, currentMaster) = await MasterPgAsync(cluster, "shard1", ct);
            Console.WriteLine($"[PHASE] wal-t24b: docker stop pgw-{cluster}-shard1-{currentMaster}");
            await Fx.RunDockerAsync(["stop", $"pgw-{cluster}-shard1-{currentMaster}"], ct);
            var (newMasterHost, newMasterPort, newMasterNode) = await MasterPgAsync(cluster, "shard1", ct);
            var tli = await PrimaryTimelineAsync(cluster, "shard1", ct);
            tli.Should().BeGreaterThanOrEqualTo(2u, "failover opens a new timeline");

            // [PHASE] срез удержания на новом мастере (§6 п.16): drop слота +
            // 160 записных переключений — суммарно с недоставленной генерацией
            // за wal_keep_size=2048 MB / 128 сегментов; сегменты старого TLI
            // (в т.ч. недоставленный хвост) перекручены; ensure воркера
            // пересоздаст слот уже на высокой позиции
            var newMasterDsn = DatabaseProvisioner.BuildAdminDsn(newMasterHost, newMasterPort, cluster,
                new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
            var slot = PgWorker.Backups.BackupNames.Slot(cluster, "shard1");
            // t25: слот активен, пока приёмник новой мастер-ноды стримит — на время
            // индукции среза удержания приёмник останавливается (supervisord-программа
            // ноды) и возвращается после пересоздания слота воркером (waiting_slot).
            var newMasterContainer = $"pgw-{cluster}-shard1-{newMasterNode}";
            await Fx.RunDockerAsync(
                ["exec", newMasterContainer, "supervisorctl", "stop", "walreceiver"], ct);
            await using (var conn = new NpgsqlConnection(newMasterDsn))
            {
                await conn.OpenAsync(ct);
                await using var drop = new NpgsqlCommand(
                    $"SELECT pg_drop_replication_slot('{slot}')", conn);
                await drop.ExecuteScalarAsync(ct);
            }
            Console.WriteLine($"[PHASE] wal-t24b: slot {slot} dropped, 160 recorded switches start");
            await SwitchWithRecordsAsync(newMasterDsn, 160, ct);
            // Переработка сегментов постгресом выполняется ТОЛЬКО на
            // checkpoint: без явного CHECKPOINT pg_wal продолжает держать
            // сегменты старого TLI, досыпка приёмника проходит успешно и
            // прыжка не происходит (разбор t097).
            await using (var cpConn = new NpgsqlConnection(newMasterDsn))
            {
                await cpConn.OpenAsync(ct);
                await using var cp = new NpgsqlCommand("CHECKPOINT", cpConn);
                await cp.ExecuteNonQueryAsync(ct);
            }
            Console.WriteLine("[PHASE] wal-t24b: 160 recorded switches + checkpoint done (old-tli segments recycled)");
            await Fx.RunDockerAsync(
                ["exec", newMasterContainer, "supervisorctl", "start", "walreceiver"], ct);

            // Индукция завершена — метка «свежести» полного (инвариант 4)
            var inductionDoneUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            // [PHASE] возврат S3
            Console.WriteLine($"[PHASE] wal-t24b: docker start {Fx.S3NodeContainerName}");
            await Fx.RunDockerAsync(["start", Fx.S3NodeContainerName], ct);

            // Контрольная INSERT — маркер «догона»: (1) запись; (2) controlLsn —
            // позиция контрольной записи, снимать ДО переключения (после него
            // позиция ушла бы в следующий незакрытый сегмент); (3) закрыть
            // сегмент контрольной записи (он будет доставлен приёмником).
            string? controlLsn;
            await using (var conn = new NpgsqlConnection(newMasterDsn))
            {
                await conn.OpenAsync(ct);
                await using var insert = new NpgsqlCommand(
                    "INSERT INTO wal_load(payload) VALUES ('r')", conn);
                await insert.ExecuteNonQueryAsync(ct);
                await using var lsn = new NpgsqlCommand(
                    "SELECT pg_current_wal_lsn()::text", conn);
                controlLsn = (string?)(await lsn.ExecuteScalarAsync(ct));
                await using var switchWal = new NpgsqlCommand("SELECT pg_switch_wal()", conn);
                await switchWal.ExecuteScalarAsync(ct);
            }
            controlLsn.Should().NotBeNullOrEmpty("the control-insert LSN is the catch-up marker");
            Console.WriteLine($"[PHASE] wal-t24b: control insert done, controlLsn={controlLsn}");

            // Прогресс-делегат: листинг S3 + ключ wal + ключи full (t29 §3.4)
            async Task<string> ConvergenceProgressAsync()
            {
                var listed = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
                var listing = !listed.IsSuccess || listed.Value.Count == 0
                    ? "listing=empty"
                    : $"objects={listed.Value.Count}";
                var wal = await writer.ReadAsync(cluster, "shard1", ct);
                var walState = wal is { IsSuccess: true, Value: { } w }
                    ? $"{w.State}{(w.Gap is null ? "" : "+gap")}"
                    : "wal=absent";
                var fulls = await FullKeysAsync(cluster, "shard1");
                var completedJson = fulls.Where(k => k.Value.Contains("\"state\":\"COMPLETED\"", StringComparison.Ordinal))
                    .Select(k => k.Value).ToList();
                // Диагностика полных (не гейт): wal_start последнего COMPLETED
                var lastWalStart = completedJson.Count == 0
                    ? "-"
                    : ParseWalStart(completedJson[^1]) ?? "-";
                return $"{listing}, wal={walState}, fulls={fulls.Count} " +
                    $"(completed={completedJson.Count}, last wal_start={lastWalStart})";
            }

            // Секция сходимости — свой бюджет 3 мин.
            using var ctsConv = CancellationTokenSource.CreateLinkedTokenSource(
                TestContext.Current.CancellationToken);
            ctsConv.CancelAfter(TimeSpan.FromMinutes(3));
            ct = ctsConv.Token;

            // 5 финальных инвариантов сходимости (§3.7) — общий бюджет 900 с:
            // итог при любом пути (прыжок с зазором + пересъём / досыпка без
            // зазора / зазор не возник); stdout-маркеры — телеметрия, не гейт.
            string? chainLastSegment = null;
            var converged = await E2ePhase.WaitAsync(Fx, "wal-converged", async () =>
            {
                // (1) кластер Active: config без state-артефакта демонтажа,
                // dsn жив, нода RUNNING (образец ProvisionedAsync)
                if (!await ProvisionedAsync(cluster))
                    return false;

                // (2)+(3)+(5) ключ wal ACTIVE без живого gap; цепь цела/склеена
                // от chain_start ключа; доставка догнала контрольную вставку
                var read = await writer.ReadAsync(cluster, "shard1", ct);
                if (read is not { IsSuccess: true, Value: { State: WalStreamStatus.Active, Gap: null } wal }
                    || wal.Error is { } we && we.Contains("tli-gap", StringComparison.Ordinal))
                    return false;

                var listed = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
                if (!listed.IsSuccess || listed.Value.Count == 0)
                    return false;
                var names = listed.Value.Select(o => o.Name).ToList();
                var chainStart = PgWorker.Backups.WalFileName.TryParse(wal.ChainStartSegment);
                if (chainStart is null)
                    return false;
                var chain = PgWorker.Backups.WalChain.CheckWithRestart(
                    chainStart.Value, names);
                if (!chain.IsContinuous || chain.LastSegment is not { } last)
                    return false;
                chainLastSegment = last.Name;

                // (4) каталог бэкапов здоров: COMPLETED-полный после индукции
                // с verify ≠ FAILED (null/PENDING/OK допустимы)
                var fulls = await FullKeysAsync(cluster, "shard1");
                var healthyFull = fulls.Any(k => FullAfterInduction(k.Value, inductionDoneUnix));
                if (!healthyFull)
                    return false;

                // (5) догон: конец непрерывной цепи ≥ сегмента контрольной вставки
                var checkTli = await PrimaryTimelineAsync(cluster, "shard1", ct);
                var control = PgWorker.Backups.WalFileName.FromLsn(
                    checkTli, controlLsn!);
                return string.CompareOrdinal(chainLastSegment, control.Name) >= 0;
            },
            // Канонический бюджет сходимости после прыжка — до 900 с (пересъём
            // полного + доставка); секция converged имеет свой 3-мин потолок —
            // wait внутри неё 170 с.
            TimeSpan.FromSeconds(170), ct, progress: ConvergenceProgressAsync);

            // Жёсткие ассерты по факту сходимости (детали — в debug-дампе фазы).
            // Сначала общий факт сходимости: при несходимости тест падает с
            // осмысленным сообщением, а не NRE на отсутствующем wal-ключе.
            converged.Should().BeTrue(
                "the system converges after the induced segment recycling: cluster Active, wal ACTIVE w/o gap, " +
                "chain glued, fresh COMPLETED full, delivery caught up to the control insert (§3.7 inv.1-5)");
            var finalRead = await writer.ReadAsync(cluster, "shard1", ct);
            finalRead.IsSuccess.Should().BeTrue("the wal key is readable at convergence");
            finalRead.Value!.State.Should().Be(WalStreamStatus.Active,
                "wal key is ACTIVE at convergence (§3.7 inv.2)");
            finalRead.Value.Gap.Should().BeNull("no live gap at convergence (§3.7 inv.2)");
            finalRead.Value.State.Should().NotBe(WalStreamStatus.WalChainError, "no WAL_CHAIN_ERROR at convergence (§3.7 inv.2)");
            chainLastSegment.Should().NotBeNull();
            var assertTli = await PrimaryTimelineAsync(cluster, "shard1", ct);
            var controlSegment = PgWorker.Backups.WalFileName.FromLsn(assertTli, controlLsn!);
            string.CompareOrdinal(chainLastSegment, controlSegment.Name)
                .Should().BeGreaterThanOrEqualTo(0,
                    "delivery caught up: last chain segment ≥ control-insert segment (§3.7 inv.5)");

            // 9.4 (t25-ревью): факт tli-прыжка наблюдаем в статус-ключе живого
            // приёмника (arch/19 §4): from — хвостовой сегмент старого TLI в S3,
            // to — сегмент прыжка, tli — новый таймлайн. Факт персистентен в
            // живом процессе до следующего прыжка, но нода-свидетель не
            // детерминирована: после failover/rebuild-гонки приёмники стартуют
            // с разными хвостами S3, прыжок делает тот, чей tail ниже точки
            // форка и чья досыпка получила 58P01. Поэтому читаем оба
            // agent-ключа и ассертер наблюдаемый прыжок.
            var agentPrefixPath = $"/pgworker/backups/{cluster}/shard1/agent/";
            var jumpSeen = await E2eFixture.WaitForAsync(async () =>
            {
                var read = await G.RangeAsync(Endpoint, agentPrefixPath, ct);
                return read.Value is { } keys
                       && keys.Any(kv => kv.Value.Contains("\"tli_jump\":", StringComparison.Ordinal));
            }, TimeSpan.FromSeconds(60), ct);
            jumpSeen.Should().BeTrue(
                "the tli_jump fact must be observable in a live receiver agent key");
            var agentRaw = (await G.RangeAsync(Endpoint, agentPrefixPath, ct)).Value!
                .First(kv => kv.Value.Contains("\"tli_jump\":", StringComparison.Ordinal)).Value;
            using var jumpDoc = System.Text.Json.JsonDocument.Parse(agentRaw);
            var jump = jumpDoc.RootElement.GetProperty("tli_jump");
            jump.GetProperty("from").GetString().Should().StartWith("00000001",
                "jump From is the old-TLI (TLI 1) tail segment in S3");
            jump.GetProperty("to").GetString().Should().NotBeNullOrEmpty(
                "jump To segment is set (the fallback start boundary)");
            jump.GetProperty("tli").GetUInt32().Should().BeGreaterThanOrEqualTo(2u,
                "jump Tli is the promoted timeline");
        }
        catch (Exception ex)
        {
            Fx.MarkFailed(); // артефакты телеметрии переживают teardown
            await WalScenarioDiagDumpAsync(cluster, "AC2/AC3: " + ex.Message);
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

    /// <summary>Инвариант 4 (§3.7): статус полного — COMPLETED, снятый после
    /// завершения индукции (started_unix ≥ метки), с verify ≠ FAILED
    /// (verify отсутствует / PENDING / OK допустимы).</summary>
    private static bool FullAfterInduction(string fullJson, long inductionDoneUnix)
    {
        using var doc = System.Text.Json.JsonDocument.Parse(fullJson);
        var root = doc.RootElement;
        if (!root.TryGetProperty("state", out var stateEl)
            || stateEl.GetString() != "COMPLETED")
            return false;
        if (!root.TryGetProperty("started_unix", out var startedEl)
            || !startedEl.TryGetInt64(out var startedUnix)
            || startedUnix < inductionDoneUnix)
            return false;
        if (!root.TryGetProperty("verify", out var verifyEl)
            || verifyEl.ValueKind != System.Text.Json.JsonValueKind.Object)
            return true; // verify отсутствует — валидного нет, FAILED нет
        return !(verifyEl.TryGetProperty("state", out var vState)
            && vState.GetString() == "FAILED");
    }

    /// <summary>wal_start_segment из статуса полного (строка JSON) — прогресс-
    /// диагностика полных (не гейт).</summary>
    private static string? ParseWalStart(string fullJson)
    {
        const string marker = "\"wal_start_segment\":\"";
        var i = fullJson.IndexOf(marker, StringComparison.Ordinal);
        if (i < 0)
            return null;
        var rest = fullJson[(i + marker.Length)..];
        var end = rest.IndexOf('"');
        return end < 0 ? null : rest[..end];
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
        await E2ePhase.WaitAsync(Fx, "wal-tli-ready", async () =>
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
        }, TimeSpan.FromSeconds(90), ct,
            progress: () => Task.FromResult($"tli={tli}"));
        return tli;
    }

    /// <summary>Гарантированный дамп диагностики Wal-сценария В САМОМ catch (до
    /// teardown): агенты, wal/work-ключи, Patroni, хвост host.log воркера —
    /// артефакт телеметрии в каталоге прогона.</summary>
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
            // Разбор «почему супервизия/пересъёмы молчат»: полные ключи шарда,
            // policy-ключ кластера, master-ключ шарда — состояние планировщика
            // видно из etcd (host.log молчаливые continue тика не несёт).
            var fullsDump = await FullKeysAsync(cluster, "shard1");
            var policyKv = await GetOrNullAsync($"/pgworker/backups/{cluster}/policy");
            var masterKv = await GetOrNullAsync($"/clusters/{cluster}/shards/shard1/master");
            var portallocKv = await GetOrNullAsync($"/pgworker/portalloc/{cluster}");
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

            var portallocDump = portallocKv?.Value is { } pa && pa.Length > 1600
                ? pa[..1600] + "…"
                : portallocKv?.Value ?? "-";
            var dump =
                $"reason={reason}\nreceivers=[{receiversDump}]\n" +
                $"wal=[{walKv?.Value ?? "-"}]\nwork=[{workKv?.Value ?? "-"}]\n" +
                $"claim=[{claimKv?.Value ?? "-"}]\npolicy=[{policyKv?.Value ?? "-"}]\n" +
                $"master=[{masterKv?.Value ?? "-"}]\n" +
                $"portalloc=[{portallocDump}]\n" +
                $"fulls={fullsDump.Count}:\n" +
                string.Join("\n", fullsDump.Select(k => $"  {k.Key}: {k.Value}")) + "\n" +
                $"patroni={patroni}\nHOST.LOG:{hostLog}\n";
            // Дамп — в каталог телеметрии прогона (t29 §3.4): рядом с
            // phases.log/docker-логами, а не в /tmp мимо артефактов.
            var dumpPath = Path.Combine(Fx.ArtifactsDir, $"wal-diag-{cluster}.txt");
            await File.WriteAllTextAsync(dumpPath, dump);
            Console.WriteLine($"[DIAG] dump: {dumpPath}");
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

            return "<no shard1 node responds on /cluster>";
        }
        catch (Exception e)
        {
            return $"<patroni unavailable: {e.Message}>";
        }
    }

    /// <summary>Подъём полного WAL-контура сценария: окружение с ownS3-нодой, кластер,
    /// воркер (Backups:Enabled), ожидание provisioning, DSN мастера.
    private async Task<(E2eEnvironment Env, HostInstance App, string Cluster, string AdminDsn, string MasterNode)>
        StartWalScenarioAsync(string slug, string clusterPrefix, CancellationToken ct)
    {
        var phaseStart = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        Console.WriteLine($"[PHASE] {slug}: start (unix={phaseStart})");
        Console.WriteLine($"[PHASE] {slug}: environment raised in " +
            (DateTimeOffset.UtcNow.ToUnixTimeSeconds() - phaseStart) + " s (incl. Release build and images)");
        var cluster = $"{clusterPrefix}{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        var app = await StartWalHostAsync(slug, ct);
        var provisioned = await E2ePhase.WaitAsync(Fx, "wal-provisioning",
            E2eEnvironment.ProvisionStuckFailFast(Fx, GetOrNullAsync, cluster, () => ProvisionedAsync(cluster)), TimeSpan.FromSeconds(90), ct);
        provisioned.Should().BeTrue("provisioning must reach DONE before the WAL load");
        Console.WriteLine($"[PHASE] {slug}: provisioning DONE in " +
            (DateTimeOffset.UtcNow.ToUnixTimeSeconds() - phaseStart) + " s from start");
        var (pgHost, pgPort, masterNode) = await MasterPgAsync(cluster, "shard1", ct);
        var adminDsn = DatabaseProvisioner.BuildAdminDsn(pgHost, pgPort, cluster,
            new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
        return (Fx, app, cluster, adminDsn, masterNode);
    }

    // Воркер с ВКЛЮЧЁННОЙ подсистемой бэкапов: t02-комплект джобов + t03 Wal-поток.
    // S3 для воркера — published порт (localhost), для агентов/джобов — advertised
    // (host.docker.internal); пороги WAL-контроля — короткие.
    private Task<HostInstance> StartWalHostAsync(string name, CancellationToken ct)
        => Fx.StartHostAsync(name, extraEnv: new Dictionary<string, string>
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

    // Переключения с крошечной записью: pg_switch_wal на idle-базе НЕ создаёт
    // новый сегмент (возвращается та же LSN) — перекрут wal_keep_size требует
    // логической записи в каждом сегменте (t24b-сценарий среза удержания).
    private static async Task SwitchWithRecordsAsync(string adminDsn, int count, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(adminDsn);
        await conn.OpenAsync(ct);
        await using (var create = new NpgsqlCommand(
            "CREATE TABLE IF NOT EXISTS wal_load(id bigserial, payload text)", conn))
            await create.ExecuteNonQueryAsync(ct);
        for (var i = 0; i < count; i++)
        {
            await using var insert = new NpgsqlCommand(
                "INSERT INTO wal_load(payload) VALUES ('r')", conn);
            await insert.ExecuteNonQueryAsync(ct);
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
        var resolved = await E2ePhase.WaitAsync(Fx, "wal-master-resolved", async () =>
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
        }, TimeSpan.FromSeconds(90), ct);
        resolved.Should().BeTrue("the shard primary must be resolved by Patroni probes");
        var entry = entries[$"{shard}/{primary}"];
        return ("localhost", entry.GetProperty("pg").GetInt32(),
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

/// <summary>Компаньон-фикстура окружения класса (полный контур на класс:
/// сеть/etcd/CA/окно портов; docs/e2e-isolation.md §1).</summary>
public sealed class E2eWalStreamTliScenariosFx : E2eContourFixture
{
    protected override string Slug => "wal-tli";
    protected override bool WithS3Node => true;
}
