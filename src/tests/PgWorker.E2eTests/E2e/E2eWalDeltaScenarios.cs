using System.Diagnostics;
using System.Globalization;
using System.Security.Cryptography;
using System.Text.Json;
using Npgsql;
using PgWorker.Backups;
using PgWorker.Backups.Restore;
using PgWorker.Core.Templates;
using PgWorker.Etcd.Backups;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Sql;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E дельта-доставки RPO-хвоста (t15, spec §5 Ф5, arch/pgnode/processes/
// g-wal-delta-assembly.md): изолированное окружение E2eRestoreScenariosFx
// (своя сеть/etcd/ownS3 по guid, динамические порты, полный teardown с ассертом
// чистоты — docs/e2e-isolation.md) + provisioned-кластер с бэкапами. Три
// сценария сборки кусков без живой логики писателя поверх реального префикса:
// (1) дыра в середине покрытия — восстановление до дыры, после отброшено;
// (2) конфликтное пересечение копий — режим выживания собирает консистентный
// префикс; (3) failover источника между выкладками дельт — куски обеих ролей
// одной жизни, restore доезжает до конца хвоста, номер жизни не меняется.
// Подмена кусочного семейства — ПОСЛЕ демонтажа restore-заявки (демонтаж
// сносит ноды с живыми приёмниками): иначе хвост зарастает выкладкой.
// Телеметрия — docs/e2e-launch.md ([PHASE]-строки, MarkFailed оставляет
// контейнеры для разбора по /tmp/pgw-e2e-artifacts-<guid>/).
public sealed class E2eWalDeltaScenarios(E2eRestoreScenariosFx Ctx) : IClassFixture<E2eRestoreScenariosFx>
{
    private E2eEnvironment Fx => Ctx.Fx;

    private const string Bucket = "pgworker-backups";

    // Пул E2E (однонодовый — ownS3-ноды окружения; etcd у каждого Fact'а свой).
    private const string Pool = "bkpool";

    private string Endpoint => Fx.EtcdEndpoint;

    private EtcdGateway G => Fx.Gateway;

    // ===== Сценарий 1: дыра в середине покрытия =====

    // AAA (spec §5 Ф5 п.1): полный + живой WAL-хвост → синтетический кусочный
    // набор поверх реального префикса цели: база [0..A) + дельта [B..C) выше A
    // (дыра [A..B)) → restore latest: зелёный, данные до дыры восстановлены,
    // после — отброшены, restored_to_lsn не перешагнул дыру.
    [Fact]
    public async Task Restore_Latest_WalDeltaHoleCoversPrefix()
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMinutes(3)); // потолок теста 3 мин: неверное ожидание не может длиться дольше (приказ владельца)
        var ct = cts.Token;
        await RunScenarioAsync(async fx =>
        {
            // Arrange — кластер + воркер с бэкап-комплектом + живой префикс цели
            var cluster = $"wdlh{fx.ClusterTag}";
            await SeedClusterAsync(cluster);
            await using var app = await StartRestoreHostAsync("wdlhole", ct);
            var target = await ArrangeDeltaTargetAsync(cluster, ct);

            // Arrange — координаты дыры в РЕАЛЬНОМ префиксе открытого сегмента:
            // A — конец последней полной записи первых 256 КиБ (PRE-строки ниже A),
            // дельта [B..C) — реальные байты далеко за дырой (POST-строки внутри
            // отброшенной части), поэтому «после дыры не восстановлено» проверяемо.
            var aOffset = WalPrefixScanner.ValidPrefixLength(
                target.Prefix, Math.Min(target.Prefix.Length, 256 * 1024));
            aOffset.Should().BeGreaterThan(0, "the real prefix carries complete records");
            ((ulong)aOffset).Should().BeGreaterThanOrEqualTo(
                (ulong)WalPrefixScanner.ValidPrefixLength(
                    target.Prefix, (int)(target.LsnPre - target.SegmentStart)),
                "PRE-строки лежат ДО начала дыры");
            var aAbs = target.SegmentStart + (ulong)aOffset;
            // Дыра 2 МиБ — с запасом шире любых записей промоута после восстановления
            var bAbs = aAbs + 2UL * 1024 * 1024;
            var cAbs = Math.Min(target.SegmentStart + (ulong)target.Prefix.Length, bAbs + 1024UL * 1024);
            cAbs.Should().BeGreaterThan(bAbs, "после дыры — диапазон с реальными байтами");
            target.LsnPost.Should().BeGreaterThan(cAbs,
                "POST-строки — за концом отброшенной дельты (дыра их накрывает)");
            Console.WriteLine($"[DELTA] {cluster}: target={target.Segment.Name} " +
                $"life={target.Life} A={aAbs:x} B={bAbs:x} C={cAbs:x} " +
                $"prefix={target.Prefix.Length} lsnPre={target.LsnPre:x} lsnPost={target.LsnPost:x}");

            // Act — заявка restore; ждём демонтаж (снос нод с приёмниками) и
            // подменяем семейство цели синтетикой (иначе хвост зарастает).
            using var ctsRestore = CancellationTokenSource.CreateLinkedTokenSource(
                TestContext.Current.CancellationToken);
            ctsRestore.CancelAfter(TimeSpan.FromMinutes(3)); // restore-секция — свой 3-мин CTS (мульти-CTS)
            ct = ctsRestore.Token;
            var restoreId = await PutRestorePlannedAsync(cluster, "shard1", "latest", ct: ct);
            await WaitDemolishedAsync(cluster, "shard1", restoreId, TimeSpan.FromSeconds(90), ct);
            var baseLen = (int)(aAbs - target.SegmentStart);
            var deltaFrom = (int)(bAbs - target.SegmentStart);
            var deltaLen = (int)(cAbs - bAbs);
            await ReplaceSegmentFamilyAsync(cluster, "shard1", target.Segment, [
                new PieceSpec(target.SegmentStart, aAbs, target.Life, true,
                    "hole-base", target.Prefix[..baseLen]),
                new PieceSpec(bAbs, cAbs, target.Life, true,
                    "hole-delta", target.Prefix[deltaFrom..(deltaFrom + deltaLen)]),
            ], ct);

            var status = await WaitRestoreAsync(
                cluster, "shard1", restoreId, TimeSpan.FromSeconds(150), ct);

            // Assert 1 — восстановление остановилось ДО начала дыры (restored_to_lsn
            // честно отражает собранный конец, g-wal-delta-assembly §4/§6)
            var restored = ParseLsn(status["restored_to_lsn"].GetString()!);
            restored.Should().BeGreaterThanOrEqualTo(target.LsnPre,
                "recovery прошёл PRE-данные реального префикса");
            restored.Should().BeLessThan(bAbs,
                $"дыра не перепрыгнута: собранный конец {restored:x} до начала дыры {bAbs:x}");

            // Assert 2 — данные: PRE-строки восстановлены, POST (за дырой) — нет
            var rows = await WaitProbeRowsAsync(cluster, ct);
            rows.Should().Be("5|5|0",
                $"восстановлено всё до дыры и ничего после (rows={rows})");
        }, ct);
    }

    // ===== Сценарий 2: конфликтное пересечение копий (выживание) =====

    // AAA (spec §5 Ф5 п.2): куски с КОНФЛИКТНЫМ пересечением — дубль диапазона с
    // испорченной копией (валидный дубль-компак несёт реальные байты) → штатная
    // жадная сборка фиксирует конфликт, режим выживания выбирает валидную
    // сборку → restore latest зелёный, данные до конца валидного покрытия на месте.
    [Fact]
    public async Task Restore_Latest_WalDeltaConflictSurvival()
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMinutes(3)); // потолок теста 3 мин: неверное ожидание не может длиться дольше (приказ владельца)
        var ct = cts.Token;
        await RunScenarioAsync(async fx =>
        {
            // Arrange — кластер + воркер + живой префикс цели
            var cluster = $"wdlc{fx.ClusterTag}";
            await SeedClusterAsync(cluster);
            await using var app = await StartRestoreHostAsync("wdlconf", ct);
            var target = await ArrangeDeltaTargetAsync(cluster, ct);

            // Arrange — конфликтная тройка: (а) база [0..E) — реальные байты;
            // (б) дубль-база [0..X) (компак позднее) — реальные байты, длиннее;
            // (в) порченая дельта [S..X): пересечение [S..E) с базой испорчено,
            // хвост [E..X) — мусор. Жадная сборка (m над r, затем start) берёт
            // базу первой и упирается в порчу пересечения → конфликт; режим
            // выживания применяет валидные куски (дубль-база стартует от начала
            // сегмента, поэтому исход не зависит от md5-порядка) → покрытие X.
            // Границы E/X — ТОЛЬКО концы записей потока: pg_current_wal_lsn()
            // отдаёт ВЫРОВНЕННУЮ (MAXALIGN) позицию следующей записи, а сборка
            // выживания скан-валидирует кусок до его конца — кусок, кончающийся
            // на добивке, отвергается всегда. Сканер реального префикса даёт
            // последний конец записи не выше LSN (commit PRE/POST-строк).
            var eOffset = WalPrefixScanner.ValidPrefixLength(
                target.Prefix, (int)(target.LsnPre - target.SegmentStart));
            var xOffset = WalPrefixScanner.ValidPrefixLength(
                target.Prefix, (int)(target.LsnPost - target.SegmentStart));
            eOffset.Should().BeGreaterThan(0, "PRE-строки лежат в реальном префиксе");
            (xOffset - eOffset).Should().BeGreaterThan(4096,
                "хвост валидного покрытия содержательнее заголовка записи");
            var eAbs = target.SegmentStart + (ulong)eOffset;
            var xAbs = target.SegmentStart + (ulong)xOffset;
            var sOffset = eOffset / 2;
            var sAbs = target.SegmentStart + (ulong)sOffset;
            var corrupt = target.Prefix[sOffset..xOffset];
            corrupt[16] ^= 0xFF; // порча пересечения [S..E) с базой
            Array.Fill(corrupt, (byte)0xA5, eOffset - sOffset, xOffset - eOffset); // хвост — мусор
            Console.WriteLine($"[DELTA] {cluster}: target={target.Segment.Name} " +
                $"life={target.Life} S={sAbs:x} E={eAbs:x} X={xAbs:x} " +
                $"lsnPre={target.LsnPre:x} lsnPost={target.LsnPost:x}");

            // Act — демонтаж restore-заявки, затем подмена семейства тройкой
            using var ctsRestore = CancellationTokenSource.CreateLinkedTokenSource(
                TestContext.Current.CancellationToken);
            ctsRestore.CancelAfter(TimeSpan.FromMinutes(3)); // restore-секция — свой 3-мин CTS (мульти-CTS)
            ct = ctsRestore.Token;
            var restoreId = await PutRestorePlannedAsync(cluster, "shard1", "latest", ct: ct);
            await WaitDemolishedAsync(cluster, "shard1", restoreId, TimeSpan.FromSeconds(90), ct);
            await ReplaceSegmentFamilyAsync(cluster, "shard1", target.Segment, [
                new PieceSpec(target.SegmentStart, eAbs, target.Life, true,
                    "conflict-base", target.Prefix[..eOffset]),
                new PieceSpec(target.SegmentStart, xAbs, target.Life, false,
                    "conflict-valid", target.Prefix[..xOffset]),
                new PieceSpec(sAbs, xAbs, target.Life, true,
                    "conflict-corrupt", corrupt),
            ], ct);

            var status = await WaitRestoreAsync(
                cluster, "shard1", restoreId, TimeSpan.FromSeconds(150), ct);

            // Assert 1 — выживание дотянуло до конца валидного покрытия (X)
            var restored = ParseLsn(status["restored_to_lsn"].GetString()!);
            restored.Should().BeGreaterThanOrEqualTo(xAbs,
                $"режим выживания выбрал валидную сборку до X={xAbs:x}");

            // Assert 2 — данные до конца валидного покрытия на месте (PRE+POST)
            var rows = await WaitProbeRowsAsync(cluster, ct);
            rows.Should().Be("10|5|5",
                $"валидный префикс собран консистентно, данные целиком (rows={rows})");
        }, ct);
    }

    // ===== Сценарий 3: failover источника между выкладками дельт =====

    // AAA (spec §5 Ф5 п.3): полный + живой поток → куски обеих ролей ОДНОЙ жизни
    // (приёмники нод разводятся по очереди — роль в имени куска детерминирована)
    // → Patroni failover источника (docker stop мастера — механика образца
    // E2eWalStreamPromoteScenarios) → данные нового таймлайна → restore latest
    // доезжает до конца хвоста; номер жизни failover'ом НЕ инкрементирован.
    [Fact]
    public async Task Restore_Latest_WalDeltaSourceFailover()
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMinutes(3)); // потолок теста 3 мин: неверное ожидание не может длиться дольше (приказ владельца)
        var ct = cts.Token;
        await RunScenarioAsync(async fx =>
        {
            // Arrange — кластер + воркер: провижининг, полный, координаты цепи
            var cluster = $"wdlsf{fx.ClusterTag}";
            await SeedClusterAsync(cluster);
            await using var app = await StartRestoreHostAsync("wdlsfail", ct);
            var provisioned = await WaitPhaseAsync("provisioning",
                E2eEnvironment.ProvisionStuckFailFast(Fx, GetOrNullAsync, cluster, () => ProvisionedAsync(cluster)),
                TimeSpan.FromSeconds(90), ct);
            provisioned.Should().BeTrue("provisioning must reach DONE before the backups");
            var (pgHost, pgPort, masterNode) = await MasterPgAsync(cluster, "shard1", ct);
            var adminDsn = DatabaseProvisioner.BuildAdminDsn(pgHost, pgPort, cluster,
                new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
            await ExecAsync(adminDsn, "CREATE TABLE wal_delta_probe(id int, note text)", ct);
            await ExecAsync(adminDsn, "CREATE TABLE IF NOT EXISTS wal_load(id bigserial, payload text)", ct);

            var completed = await WaitPhaseAsync("full-completed", async () =>
                (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains("COMPLETED")),
                TimeSpan.FromSeconds(90), ct);
            if (!completed)
                throw new ApplicationException("the full did not reach COMPLETED: "
                    + await DumpDiagnosticsAsync(cluster, "shard1"));
            var fullKv = (await FullKeysAsync(cluster, "shard1")).Single(f => f.Value.Contains("COMPLETED"));
            var full = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(fullKv.Value)!;
            var life = full.TryGetValue("chain_life", out var lifeEl) && lifeEl.TryGetUInt32(out var lifeValue)
                ? lifeValue : 0u;
            var chainStart = WalFileName.TryParse(full["wal_start_segment"].GetString()!);
            chainStart.HasValue.Should().BeTrue("wal_start_segment is filled from UPLOADING");
            var lifeBefore = await ReadWalLifeAsync(cluster, "shard1", ct);
            (lifeBefore?.ChainLife ?? 0u).Should().Be(life, "свежая цепь — жизнь 0 (ключа wal-life нет)");

            // Arrange — оба приёмника шарда живы (образец Promote)
            var bothStreaming = await WaitPhaseAsync("wal-receivers-up", async () =>
            {
                var facts = await Fx.ReadWalAgentFactsAsync(cluster, "shard1", ct);
                return facts.Count(f => f.State == "streaming") >= 2;
            }, TimeSpan.FromSeconds(90), ct);
            bothStreaming.Should().BeTrue("both receivers streaming before the failover");

            // Arrange — PRE-данные и куски ОБЕИХ ролей одной жизни: приёмники нод
            // разводятся по очереди (проигравший гонку за покрытие не выкладывает
            // ничего, поэтому роль загрузчика детерминируется заглушкой соседа).
            await ExecAsync(adminDsn,
                "INSERT INTO wal_delta_probe SELECT g, 'pre' FROM generate_series(1, 5) g", ct);
            var nodes = await Fx.ShardNodeContainerNamesAsync(cluster, "shard1", ct);
            nodes.Should().HaveCountGreaterThanOrEqualTo(2, "both shard nodes are provisioned");
            var masterContainer = $"pgw-{cluster}-shard1-{masterNode}";
            var replicaContainer = nodes.Single(n => n != masterContainer);

            // (m) заглушён приёмник реплики — выкладывает мастер
            await ControlNodeReceiverAsync(replicaContainer, "stop", ct);
            await ExecAsync(adminDsn, "INSERT INTO wal_load(payload) SELECT repeat('y', 262144)", ct);
            var masterRolePiece = await WaitPhaseAsync("delta-role-master", async () =>
                (await ListWalNamesAsync(cluster, "shard1"))
                    .Any(n => WalDeltaName.TryParse(n) is { } p && p.ChainLife == life && p.IsMaster),
                TimeSpan.FromSeconds(60), ct);
            masterRolePiece.Should().BeTrue("приёмник мастера выкладывает куски роли m жизни цепи: "
                + await DumpDiagnosticsAsync(cluster, "shard1"));

            // (r) заглушён приёмник мастера — выкладывает реплика (роль r)
            await ControlNodeReceiverAsync(replicaContainer, "start", ct);
            await ControlNodeReceiverAsync(masterContainer, "stop", ct);
            await Task.Delay(2000, ct); // админ-старт реплики / остановка мастера
            await ExecAsync(adminDsn, "INSERT INTO wal_load(payload) SELECT repeat('z', 262144)", ct);
            var replicaRolePiece = await WaitPhaseAsync("delta-role-replica", async () =>
                (await ListWalNamesAsync(cluster, "shard1"))
                    .Any(n => WalDeltaName.TryParse(n) is { } p && p.ChainLife == life && !p.IsMaster),
                TimeSpan.FromSeconds(60), ct);
            replicaRolePiece.Should().BeTrue("приёмник реплики выкладывает куски роли r ТОЙ ЖЕ жизни: "
                + await DumpDiagnosticsAsync(cluster, "shard1"));

            // Act — отдельный CTS: failover + данные нового таймлайна + гейт хвоста
            using var ctsFailover = CancellationTokenSource.CreateLinkedTokenSource(
                TestContext.Current.CancellationToken);
            ctsFailover.CancelAfter(TimeSpan.FromMinutes(3)); // секция failover+хвост — свой 3-мин CTS (мульти-CTS)
            ct = ctsFailover.Token;

            // Жёсткая потеря мастера: Patroni промоутит реплику, открывая новый TLI.
            await Fx.RunDockerAsync(["stop", masterContainer], ct);
            var (newHost, newPort, newNode) = await MasterPgAsync(cluster, "shard1", ct);
            newNode.Should().NotBe(masterNode, "after the failover the primary is the surviving replica");
            var newDsn = DatabaseProvisioner.BuildAdminDsn(newHost, newPort, cluster,
                new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
            var tli = await WaitTimelineAsync(cluster, "shard1", chainStart!.Value.Tli, ct);
            tli.Should().BeGreaterThan(chainStart!.Value.Tli, "failover opens a new timeline");

            // Данные НОВОГО таймлайна; свитч закрывает сегмент записи → финал в архив
            await ExecAsync(newDsn,
                "INSERT INTO wal_delta_probe SELECT g, 'post' FROM generate_series(6, 10) g", ct);
            var lsnPost = ParseLsn(await ScalarAsync(newDsn, "SELECT pg_current_wal_lsn()", ct));
            var postSegmentName = (await ScalarAsync(newDsn,
                "SELECT pg_walfile_name(pg_current_wal_lsn())", ct)).Trim();
            var postSegment = WalFileName.TryParse(postSegmentName);
            postSegment.HasValue.Should().BeTrue($"pg_walfile_name '{postSegmentName}' must parse");
            await SwitchWalsAsync(newDsn, 1, ct);

            // Гейт хвоста: финал нового таймлайна на/после сегмента POST-строк и
            // непрерывная цепь от wal_start — тот же валидатор, что у restore-заявки
            var tail = await WaitPhaseAsync("wal-tail-archived", async () =>
            {
                var names = await ListWalNamesAsync(cluster, "shard1");
                var hasPostFinal = names
                    .Select(n => WalFileName.TryParse(n))
                    .OfType<WalFileName>()
                    .Any(w => w.Tli >= tli && Position(w) >= Position(postSegment!.Value));
                return hasPostFinal && WalChain.Check(chainStart!.Value, names).IsContinuous;
            }, TimeSpan.FromSeconds(90), ct);
            tail.Should().BeTrue("финал нового таймлайна и непрерывная цепь обязаны лежать в архиве: "
                + await DumpDiagnosticsAsync(cluster, "shard1"));

            // Assert 1 — failover НЕ меняет жизнь цепочки (читаем ДО restore:
            // сама restore-заявка инкрементирует счётчик отдельно)
            var lifeAfterFailover = await ReadWalLifeAsync(cluster, "shard1", ct);
            lifeAfterFailover?.ChainLife.Should().Be(lifeBefore?.ChainLife,
                "failover источника не инкрементирует номер жизни");
            lifeAfterFailover.Should().BeNull("failover не заводит ключ wal-life");

            // Act — restore latest на непрерывной цепи
            using var ctsRestore = CancellationTokenSource.CreateLinkedTokenSource(
                TestContext.Current.CancellationToken);
            ctsRestore.CancelAfter(TimeSpan.FromMinutes(3)); // restore-секция — свой 3-мин CTS (мульти-CTS)
            ct = ctsRestore.Token;
            var restoreId = await PutRestorePlannedAsync(cluster, "shard1", "latest", ct: ct);
            var status = await WaitRestoreAsync(
                cluster, "shard1", restoreId, TimeSpan.FromSeconds(150), ct);

            // Assert 2 — latest доехал до конца хвоста (POST-записи нового TLI)
            var restored = ParseLsn(status["restored_to_lsn"].GetString()!);
            restored.Should().BeGreaterThanOrEqualTo(lsnPost,
                $"restore latest доезжает до конца хвоста (POST-позиция {lsnPost:x})");
            var rows = await WaitProbeRowsAsync(cluster, ct);
            rows.Should().Be("10|5|5",
                $"PRE (до failover) и POST (после) восстановлены целиком (rows={rows})");
        }, ct);
    }

    // ===== Общая подготовка сценариев дыры/конфликта =====

    /// <summary>Состояние цели сценария: кластер, DSN мастера, открытый
    /// сегмент-цель, номер жизни цепи, позиции PRE/POST-строк и реальный
    /// префикс сегмента из кусков приёмника.</summary>
    private sealed record DeltaTargetState(
        string Cluster, string AdminDsn, WalFileName Segment, ulong SegmentStart,
        uint Life, ulong LsnPre, ulong LsnPost, byte[] Prefix);

    // Догон позиции записи в открытом сегменте до atLeast (абсолютный LSN):
    // некомпрессируемые md5-строки (при wal_compression=on repeat('x') даёт
    // почти нулевой прирост WAL); цикл ограничен — цель внутри сегмента,
    // недостижение провалит последующий ассерт сегмента с диагностикой.
    private static async Task FillWalToAsync(string adminDsn, ulong atLeast, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(adminDsn);
        await conn.OpenAsync(ct);
        for (var i = 0; i < 12; i++)
        {
            await using (var pos = new NpgsqlCommand("SELECT pg_current_wal_lsn()", conn))
            {
                var current = ParseLsn(Convert.ToString(await pos.ExecuteScalarAsync(ct)) ?? "");
                if (current >= atLeast)
                    return;
            }
            await using var insert = new NpgsqlCommand(
                "INSERT INTO wal_load(payload) SELECT string_agg(md5(g::text || clock_timestamp()::text), '')" +
                " FROM generate_series(1, 40000) g", conn);
            await insert.ExecuteNonQueryAsync(ct);
        }
    }

    // Arrange дыры/конфликта (одна механика, spec §5 Ф5): провижининг →
    // COMPLETED-полный (номер жизни — из его статуса) → свитч на свежий открытый
    // сегмент-цель → PRE-строки → нагрузка за первые 256 КиБ → POST-строки →
    // снимок реального префикса кусков приёмника до POST-позиции.
    private async Task<DeltaTargetState> ArrangeDeltaTargetAsync(string cluster, CancellationToken ct)
    {
        var provisioned = await WaitPhaseAsync("provisioning",
            E2eEnvironment.ProvisionStuckFailFast(Fx, GetOrNullAsync, cluster, () => ProvisionedAsync(cluster)),
            TimeSpan.FromSeconds(90), ct);
        provisioned.Should().BeTrue("provisioning must reach DONE before the backups");

        var (pgHost, pgPort, _) = await MasterPgAsync(cluster, "shard1", ct);
        var adminDsn = DatabaseProvisioner.BuildAdminDsn(pgHost, pgPort, cluster,
            new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
        // Таблица-проба — в базе полного (создана ДО его съёма); строки — только
        // в WAL-хвосте целевого сегмента (что восстановлено/потеряно — ассерты).
        await ExecAsync(adminDsn, "CREATE TABLE wal_delta_probe(id int, note text)", ct);
        await ExecAsync(adminDsn, "CREATE TABLE IF NOT EXISTS wal_load(id bigserial, payload text)", ct);

        var completed = await WaitPhaseAsync("full-completed", async () =>
            (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains("COMPLETED")),
            TimeSpan.FromSeconds(90), ct);
        if (!completed)
            throw new ApplicationException("the full did not reach COMPLETED: "
                + await DumpDiagnosticsAsync(cluster, "shard1"));
        var fullKv = (await FullKeysAsync(cluster, "shard1")).Single(f => f.Value.Contains("COMPLETED"));
        var full = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(fullKv.Value)!;
        var life = full.TryGetValue("chain_life", out var lifeEl) && lifeEl.TryGetUInt32(out var lifeValue)
            ? lifeValue : 0u;
        var chainStart = WalFileName.TryParse(full["wal_start_segment"].GetString()!);
        chainStart.HasValue.Should().BeTrue("wal_start_segment is filled from UPLOADING");

        // Свежий открытый сегмент-цель: свитч закрывает текущий (его финал обязан
        // доехать — гейт цепи ниже), все дальнейшие записи ложатся в цель.
        await SwitchWalsAsync(adminDsn, 1, ct);
        await ExecAsync(adminDsn,
            "INSERT INTO wal_delta_probe SELECT g, 'pre' FROM generate_series(1, 5) g", ct);
        var lsnPre = ParseLsn(await ScalarAsync(adminDsn, "SELECT pg_current_wal_lsn()", ct));

        var segmentName = (await ScalarAsync(adminDsn,
            "SELECT pg_walfile_name(pg_current_wal_lsn())", ct)).Trim();
        var segment = WalFileName.TryParse(segmentName);
        segment.HasValue.Should().BeTrue($"pg_walfile_name '{segmentName}' must parse");
        var segmentStart = WalDeltaName.SegmentStartOf(segment!.Value);
        lsnPre.Should().BeGreaterThan(segmentStart, "PRE-строки — в целевом (открытом) сегменте");

        // Нагрузка дальше первых 256 КиБ: некомпрессируемые записи (md5-строки;
        // repeat('x') при wal_compression=on даёт почти нулевой прирост WAL) —
        // фиксируем позицию записи ≥ 4 МиБ внутри цели, ниже границы сегмента.
        await FillWalToAsync(adminDsn, segmentStart + 4UL * 1024 * 1024, ct);
        await ExecAsync(adminDsn,
            "INSERT INTO wal_delta_probe SELECT g, 'post' FROM generate_series(6, 10) g", ct);
        var lsnPost = ParseLsn(await ScalarAsync(adminDsn, "SELECT pg_current_wal_lsn()", ct));
        var postSegmentName = (await ScalarAsync(adminDsn,
            "SELECT pg_walfile_name(pg_current_wal_lsn())", ct)).Trim();
        postSegmentName.ToLowerInvariant().Should().Be(segment.Value.Name,
            "PRE/POST-строки и нагрузка лежат в ОДНОМ целевом сегменте");

        // Снимок реального префикса кусков приёмника: покрытие ≥ POST-позиции.
        var prefix = await WaitSegmentPrefixAsync(cluster, segment.Value, life, lsnPost - segmentStart, ct);

        // Гейт цепи финалов до заявки restore (цель открыта — финалов выше нет).
        var chainOk = await WalChainContinuousAsync(cluster, "shard1", chainStart!.Value, ct);
        chainOk.Should().BeTrue("the final chain to the target segment must be continuous: "
            + await DumpDiagnosticsAsync(cluster, "shard1"));

        Console.WriteLine($"[DELTA] {cluster}: target={segment.Value.Name} life={life}, " +
            $"prefix={prefix.Length} bytes, lsnPre={lsnPre:x}, lsnPost={lsnPost:x}");
        return new DeltaTargetState(cluster, adminDsn, segment.Value, segmentStart,
            life, lsnPre, lsnPost, prefix);
    }
    // ===== Хелперы (образцы — E2eRestoreScenarios/E2eDrillScenarios:
    // файлы сценариев независимы, паттерн репо) =====

    // Запуск сценария под политикой телеметрии (docs/e2e-launch.md): упавший
    // тест помечает окружение MarkFailed — teardown ОСТАНАВЛИВАЕТ контейнеры
    // (логи/тома остаются для разбора), зачистка — по README-cleanup.txt.
    private async Task RunScenarioAsync(Func<E2eEnvironment, Task> body, CancellationToken ct)
    {
        try
        {
            await body(Fx);
        }
        catch
        {
            Fx.MarkFailed();
            throw;
        }
    }

    // Ожидание фазы с телеметрией (docs/e2e-launch.md): длительность в журнал
    // всегда; фаза дольше 60 с — немедленный сбор docker-логов в артефакты.
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

    // Воркер с бэкап-комплектом (паттерн E2eRestoreScenarios.StartRestoreHostAsync):
    // креды пула — env-профиль, адреса нод воркер резолвит из /s3/; пороги
    // WAL-контроля короткие, бюджет rejoin накрывает вариации хоста.
    private Task<HostInstance> StartRestoreHostAsync(string name, CancellationToken ct)
        => Fx.StartHostAsync(name, extraEnv: new Dictionary<string, string>
        {
            ["PgWorker__Backups__Enabled"] = "true",
            ["PgWorker__Backups__S3__Bucket"] = Bucket,
            ["PgWorker__Backups__Retry__BaseSec"] = "2",
            ["PgWorker__Backups__Retry__MaxSec"] = "4",
            ["PgWorker__Backups__Wal__VerifyIntervalSec"] = "2",
            ["PgWorker__Backups__Wal__StaleSec"] = "600",
            ["PgWorker__Backups__Wal__LagMaxSegments"] = "100000",
            ["PgWorker__Backups__Restore__RecoveryTimeoutSec"] = "600",
            ["PgWorker__Thresholds__PatroniBootSec"] = "120",
        }, ct: ct);

    // Host-клиент BackupS3 (published порт): ассерты S3 и подмена кусков.
    private PgWorker.Backups.BackupS3 HostS3Client()
        => new(new PgWorker.Backups.S3ClientOptions(
            Fx.S3Endpoint,
            Bucket, E2eEnvironment.S3RootUser, E2eEnvironment.S3RootPassword));

    // PUT PLANNED-заявки restore прямым ключом (принятое отступление: прецедент
    // E2eRestoreScenarios; API-путь покрыт WAF-тестами RestoreApiTests).
    // backup_id пуст → валидация воркера резолвит новейший COMPLETED.
    private async Task<string> PutRestorePlannedAsync(
        string cluster, string shard, string target, string? source = null,
        CancellationToken ct = default)
    {
        var id = $"{DateTimeOffset.UtcNow:yyyyMMddHHmmss}Z-{shard}";
        var op = new RestoreOperationState(id, RestoreStatus.Planned, "",
            source ?? $"{cluster}/{shard}", target, $"{shard}a",
            DateTimeOffset.UtcNow.ToUnixTimeSeconds(), "e2e");
        await G.PutAsync(Endpoint, BackupNames.RestoreKey(cluster, shard, id),
            RestoreStatusJson.Serialize(op), null, ct);
        return id;
    }

    // Ожидание фазы ПОСЛЕ демонтажа restore-заявки: фаза пишется только после
    // DemolishAsync (arch/19 §3.5) — снесены ноды с живыми приёмниками, поэтому
    // подмена кусочного семейства цели больше не «зарастает» выкладкой.
    private async Task WaitDemolishedAsync(
        string cluster, string shard, string restoreId, TimeSpan budget, CancellationToken ct)
    {
        string? phase = null;
        string? state = null;
        string? error = null;
        var observed = await E2eFixture.WaitForAsync(async () =>
        {
            var raw = await GetOrNullAsync(BackupNames.RestoreKey(cluster, shard, restoreId));
            if (raw is null)
                return false;
            var status = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(raw.Value);
            if (status is null)
                return false;
            phase = status.TryGetValue("phase", out var p) ? p.GetString() : null;
            state = status.TryGetValue("state", out var s) ? s.GetString() : null;
            error = status.TryGetValue("error", out var e) ? e.GetString() : null;
            // FAILED до демонтажа — не ждём бюджет: исход разберёт основной ассерт
            return !string.IsNullOrEmpty(phase) || state == "FAILED";
        }, budget, ct);
        observed.Should().BeTrue(
            $"restore {cluster}/{shard}/{restoreId} must reach the demolish phase " +
            $"(state={state ?? "<none>"}, error={error ?? "<none>"}): "
            + await DumpDiagnosticsAsync(cluster, shard));
        phase.Should().NotBeNullOrEmpty(
            $"demolish must complete before the family replacement " +
            $"(state={state}, error={error ?? "<none>"})");
    }

    // Поллинг restore-ключа до терминального статуса (FAILED — исключение с
    // полным дампом; COMPLETED — словарь полей).
    private async Task<Dictionary<string, JsonElement>> WaitRestoreAsync(
        string cluster, string shard, string id, TimeSpan budget, CancellationToken ct)
    {
        Dictionary<string, JsonElement>? status = null;
        var sw = Stopwatch.StartNew();
        var done = await E2eFixture.WaitForAsync(async () =>
        {
            var raw = await GetOrNullAsync(BackupNames.RestoreKey(cluster, shard, id));
            if (raw is null)
                return false;
            status = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(raw.Value);
            return status is not null
                && status.TryGetValue("state", out var st)
                && st.GetString() is "COMPLETED" or "FAILED";
        }, budget, ct);
        sw.Stop();
        var state = status?.TryGetValue("state", out var s) == true ? s.GetString() : null;
        var error = status?.TryGetValue("error", out var e) == true ? e.GetString() : null;
        Console.WriteLine(
            $"[PHASE] restore {cluster}/{shard}/{id}: state={state ?? "<none>"}, " +
            $"elapsed={sw.Elapsed.TotalSeconds:F1}s");
        if (sw.Elapsed > TimeSpan.FromSeconds(60))
            await Fx.CollectDiagnosticsAsync($"slow-restore-{cluster}-{shard}-{(int)sw.Elapsed.TotalSeconds}s");
        if (!done || state != "COMPLETED")
            throw new ApplicationException(
                $"restore {cluster}/{shard}/{id} did not reach COMPLETED: "
                + (status is null ? "no key" : state)
                + (error is null ? "" : $", error={error}")
                + "; " + await DumpDiagnosticsAsync(cluster, shard));
        return status!;
    }

    // Строки пробы после COMPLETED: count | pre | post. Ретрай переходного окна
    // рестарта postmaster rejoin'а (NpgsqlException, вкл. 42P01 до готовности).
    private async Task<string> WaitProbeRowsAsync(string cluster, CancellationToken ct)
    {
        var (host, port, _) = await MasterPgAsync(cluster, "shard1", ct);
        var dsn = DatabaseProvisioner.BuildAdminDsn(host, port, cluster,
            new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
        var rows = "";
        var lastError = "";
        var ok = await E2eFixture.WaitForAsync(async () =>
        {
            try
            {
                rows = await ScalarAsync(dsn,
                    "SELECT count(*) || '|' || count(*) FILTER (WHERE note = 'pre')" +
                    " || '|' || count(*) FILTER (WHERE note = 'post') FROM wal_delta_probe", ct);
                return true;
            }
            catch (NpgsqlException ex)
            {
                lastError = ex.Message.Split('\n')[0];
                return false; // переходное окно рестарта postmaster — повторим
            }
        }, TimeSpan.FromSeconds(90), ct);
        ok.Should().BeTrue(
            $"the restored shard must answer the probe query (rows={rows}, error={lastError}): "
            + await DumpDiagnosticsAsync(cluster, "shard1"));
        return rows;
    }

    /// <summary>Кусок синтетического набора: границы в абсолютных LSN, жизнь,
    /// роль, метка для диагностики, байты.</summary>
    private sealed record PieceSpec(
        ulong Start, ulong End, uint Life, bool IsMaster, string Label, byte[] Bytes);

    // Подмена кусочного семейства сегмента (spec §5 Ф5): снос финала (если успел
    // появиться) и легитимных кусков/версий + выкладка набора теста под именами
    // контракта (границы/md5/роль/жизнь). Вызывается ТОЛЬКО после демонтажа:
    // при живых приёмниках хвост зарастает их выкладкой (RPO-гейт теста).
    private async Task ReplaceSegmentFamilyAsync(
        string cluster, string shard, WalFileName segment,
        IReadOnlyList<PieceSpec> pieces, CancellationToken ct)
    {
        await using var s3 = HostS3Client();
        var segStart = WalDeltaName.SegmentStartOf(segment);

        var listed = await s3.ListPrefixAsync($"{cluster}/{shard}/wal/{segment.Name}.", ct: ct);
        listed.IsSuccess.Should().BeTrue($"list {segment.Name} family: {listed.Error?.Message}");
        var doomed = listed.Value.Select(o => o.Key).ToList();
        var wal = await s3.ListWalAsync(cluster, shard, ct: ct);
        wal.IsSuccess.Should().BeTrue($"list wal: {wal.Error?.Message}");
        if (wal.Value.Any(o => o.Name == segment.Name))
            doomed.Add($"{cluster}/{shard}/wal/{segment.Name}");
        if (doomed.Count > 0)
            (await s3.DeleteKeysAsync(doomed, ct)).IsSuccess.Should().BeTrue(
                "the target segment family and final must be removed for the synthetic set");

        var expected = new List<string>();
        foreach (var piece in pieces)
        {
            piece.Start.Should().BeGreaterThanOrEqualTo(segStart, $"{piece.Label}: range inside the segment");
            piece.End.Should().BeLessThanOrEqualTo(segStart + (ulong)WalFileName.SegmentBytes,
                $"{piece.Label}: range inside the segment");
            piece.Bytes.Length.Should().Be((int)(piece.End - piece.Start),
                $"{piece.Label}: bytes length = end − start");
            var md5 = Convert.ToHexStringLower(MD5.HashData(piece.Bytes));
            var name = WalDeltaName.Key(segment, piece.Start, piece.End, piece.Life, piece.IsMaster, md5);
            var put = await s3.PutObjectMd5Async($"{cluster}/{shard}/wal/{name}", piece.Bytes, md5, ct);
            put.IsSuccess.Should().BeTrue($"put {piece.Label} piece {name}: {put.Error?.Message}");
            expected.Add(name);
        }

        // Гейт подмены: в семействе РОВНО наш набор (чужие куски невидимы сборке
        // иначе), финала нет — сборка обязана идти путём дельта-кусков.
        var after = await s3.ListPrefixAsync($"{cluster}/{shard}/wal/{segment.Name}.", ct: ct);
        after.IsSuccess.Should().BeTrue($"relist {segment.Name} family: {after.Error?.Message}");
        after.Value.Select(o => o.Key[(o.Key.LastIndexOf('/') + 1)..])
            .OrderBy(n => n, StringComparer.Ordinal)
            .Should().Equal(expected.OrderBy(n => n, StringComparer.Ordinal),
                "the target family is exactly the synthetic set");
        var walAfter = await s3.ListWalAsync(cluster, shard, ct: ct);
        walAfter.IsSuccess.Should().BeTrue($"relist wal: {walAfter.Error?.Message}");
        walAfter.Value.Select(o => o.Name).Should().NotContain(segment.Name,
            "no final object — fetch-wal must assemble the segment from the pieces");
        Console.WriteLine($"[DELTA] {cluster}: synthetic set of {segment.Name}: "
            + string.Join(", ", pieces.Select(p =>
                $"{p.Label}[{p.Start - segStart:x}..{p.End - segStart:x})")));
    }

    // Реальный префикс ОТКРЫТОГО сегмента из кусков приёмника: база+дельты
    // текущей жизни (покрытие по именам — та же арифметика, что у сборки; байты
    // двух нод в одинаковых диапазонах идентичны физическому потоку).
    private async Task<byte[]> WaitSegmentPrefixAsync(
        string cluster, WalFileName segment, uint life, ulong needed, CancellationToken ct)
    {
        byte[]? prefix = null;
        var validated = -1;
        var ok = await E2eFixture.WaitForAsync(async () =>
        {
            prefix = await SnapshotSegmentPrefixAsync(cluster, "shard1", segment, life, ct);
            // Скан-валидация снимка: покрытие приёмника — границы записей + CRC.
            validated = prefix.Length == 0
                ? 0 : WalPrefixScanner.ValidatedPrefixLength(prefix, prefix.Length);
            return prefix.Length > 0
                && validated == prefix.Length
                && (ulong)prefix.Length >= needed;
        }, TimeSpan.FromSeconds(90), ct);
        if (!ok)
            throw new ApplicationException(
                $"the receiver delta coverage of {segment.Name} did not reach {needed} bytes " +
                $"(got {prefix?.Length ?? 0}, crc-prefix={validated}): "
                + await DumpDiagnosticsAsync(cluster, "shard1"));
        return prefix!;
    }

    // Снимок префикса сегмента из кусков приёмника текущего номера жизни.
    private async Task<byte[]> SnapshotSegmentPrefixAsync(
        string cluster, string shard, WalFileName segment, uint life, CancellationToken ct)
    {
        await using var s3 = HostS3Client();
        var segStart = WalDeltaName.SegmentStartOf(segment);
        var listed = await s3.ListPrefixAsync($"{cluster}/{shard}/wal/{segment.Name}.", ct: ct);
        listed.IsSuccess.Should().BeTrue(
            $"wal delta listing of {segment.Name}: {listed.Error?.Message}");
        var pieces = new List<(WalDeltaName.Piece Piece, string Name)>();
        foreach (var obj in listed.Value)
        {
            var name = obj.Key[(obj.Key.LastIndexOf('/') + 1)..];
            if (WalDeltaName.TryParse(name) is not { } piece || piece.ChainLife != life)
                continue;
            if (piece.End - piece.Start != (ulong)obj.SizeBytes)
                continue; // битый по размеру — фильтр сборки
            pieces.Add((piece, name));
        }
        if (pieces.Count == 0)
            return [];
        pieces.Sort((a, b) => a.Piece.Start.CompareTo(b.Piece.Start));
        var coverage = segStart;
        bool extended;
        do
        {
            extended = false;
            foreach (var (piece, _) in pieces)
                if (piece.Start <= coverage && piece.End > coverage)
                {
                    coverage = piece.End;
                    extended = true;
                }
        }
        while (extended);
        if (coverage <= segStart)
            return [];
        var buffer = new byte[coverage - segStart];
        foreach (var (piece, name) in pieces)
        {
            if (piece.Start >= coverage)
                continue;
            var got = await s3.GetObjectBytesAsync(cluster, shard, $"wal/{name}", ct);
            got.IsSuccess.Should().BeTrue($"download delta {name}: {got.Error?.Message}");
            var from = (int)(piece.Start - segStart);
            var len = (int)(Math.Min(piece.End, coverage) - piece.Start);
            got.Value.Data.AsSpan(0, len).CopyTo(buffer.AsSpan(from, len));
        }
        return buffer;
    }

    // Приёмник ноды — supervisord-программа pgnode контейнера ноды (t25):
    // admin stop НЕ перезапускается супервизором (модель «агент мёртв»);
    // liveness ноды — Patroni-пробы, приёмник на неё не влияет.
    private Task ControlNodeReceiverAsync(string container, string action, CancellationToken ct)
        => Fx.RunDockerAsync(["exec", container, "supervisorctl", action, "pgnode"], ct);

    // Максимальный TLI архива (финалы/куски/history) — promote открыл новый
    // таймлайн и он доехал до S3 (сегменты нового TLI + <tli>.history).
    private async Task<uint> WaitTimelineAsync(
        string cluster, string shard, uint parentTli, CancellationToken ct)
    {
        var tli = parentTli;
        var ok = await E2eFixture.WaitForAsync(async () =>
        {
            var names = await ListWalNamesAsync(cluster, shard);
            var max = names
                .Select(n => WalFileName.TryParse(n) is { } w
                    ? w.Tli : WalFileName.TryParseHistory(n) ?? 0u)
                .DefaultIfEmpty(0u).Max();
            if (max <= parentTli)
                return false;
            tli = max;
            return true;
        }, TimeSpan.FromSeconds(90), ct);
        if (!ok)
            throw new ApplicationException(
                $"the new timeline (>{parentTli}) did not reach the archive: "
                + await DumpDiagnosticsAsync(cluster, shard));
        return tli;
    }

    // Непрерывность цепи финалов от wal_start полного (WalChain.Check — тот же
    // валидатор, что в restore-заявке): гонки пересоздания t03-агента ждутся
    // поллингом.
    private async Task<bool> WalChainContinuousAsync(
        string cluster, string shard, WalFileName walStartSegment, CancellationToken ct)
        => await E2eFixture.WaitForAsync(async () =>
        {
            var names = await ListWalNamesAsync(cluster, shard);
            return WalChain.Check(walStartSegment, names).IsContinuous;
        }, TimeSpan.FromSeconds(90), ct);

    // Счётчик жизни цепи из etcd (null — ключа нет: жизнь 0).
    private async Task<WalLifeState?> ReadWalLifeAsync(string cluster, string shard, CancellationToken ct)
    {
        var kv = await GetOrNullAsync(WalLifeKeys.Key(cluster, shard));
        return kv is null ? null : WalLifeStateJson.TryParse(kv.Value);
    }

    // Имена объектов wal/ (host-клиент, префикс shard'а) — фильтр Range Failed.
    private async Task<List<string>> ListWalNamesAsync(string cluster, string shard)
    {
        await using var s3 = HostS3Client();
        var listed = await s3.ListWalAsync(cluster, shard, ct: TestContext.Current.CancellationToken);
        if (!listed.IsSuccess)
            Console.Error.WriteLine($"e2e[archive]: LIST FAILED {listed.Error?.Message}");
        return listed.IsSuccess ? [.. listed.Value.Select(o => o.Name)] : [];
    }

    private async Task<IReadOnlyList<Kv>> FullKeysAsync(string cluster, string shard)
        => (await G.RangeAsync(Endpoint, $"/pgworker/backups/{cluster}/{shard}/full/",
            TestContext.Current.CancellationToken)).Value ?? [];

    private async Task<Kv?> GetOrNullAsync(string key)
        => (await G.GetAsync(Endpoint, key, TestContext.Current.CancellationToken)).Value;

    // Диагностика провала: журнал воркера, restore/full/wal/wal-life ключи шарда,
    // состояния нод, docker-объекты кластера — единой строкой в сообщение ассерта.
    private async Task<string> DumpDiagnosticsAsync(string cluster, string shard)
    {
        var workKv = await GetOrNullAsync($"/pgworker/work/{cluster}");
        var restoreKvs = await G.RangeAsync(
            Endpoint, $"/pgworker/backups/{cluster}/{shard}/restore/",
            TestContext.Current.CancellationToken);
        var fullKvs = await FullKeysAsync(cluster, shard);
        var walKv = await GetOrNullAsync($"/pgworker/backups/{cluster}/{shard}/wal");
        var lifeKv = await GetOrNullAsync(WalLifeKeys.Key(cluster, shard));
        var walNames = await ListWalNamesAsync(cluster, shard);
        var nodeStates = await G.RangeAsync(
            Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/",
            TestContext.Current.CancellationToken);
        var nodeDump = nodeStates.IsSuccess
            ? string.Join(";", (nodeStates.Value ?? []).Select(k =>
                $"{k.Key.Replace($"/clusters/{cluster}/shards/{shard}/nodes/", "")}={k.Value}"))
            : $"range failed: {nodeStates.Error?.Message}";
        string jobs;
        try
        {
            jobs = await Fx.RunDockerAsync(
            ["ps", "-a", "--format", "{{.Names}} {{.Status}}", "--filter", $"name={cluster}"],
            TestContext.Current.CancellationToken);
            jobs += " | volumes=" + await Fx.RunDockerAsync(
            ["volume", "ls", "--format", "{{.Name}}", "--filter", $"name={cluster}"],
            TestContext.Current.CancellationToken);
        }
        catch (Exception e)
        {
            jobs = $"docker ps failed: {e.Message}";
        }

        return $"journal=[{workKv?.Value ?? "<absent>"}] " +
               $"restores=[{string.Join(";", (restoreKvs.Value ?? []).Select(k => k.Value))}] " +
               $"fulls=[{string.Join(";", fullKvs.Select(k => k.Value))}] " +
               $"walKey=[{walKv?.Value ?? "<absent>"}] " +
               $"walLife=[{lifeKv?.Value ?? "<absent>"}] " +
               $"wal=[{string.Join(",", walNames.OrderBy(n => n, StringComparer.Ordinal))}] " +
               $"nodes=[{nodeDump}] jobs=[{jobs}]";
    }

    // LSN 'X/Y' (pg_current_wal_lsn / restored_to_lsn) → абсолютный ulong.
    private static ulong ParseLsn(string lsn)
    {
        var parts = lsn.Split('/');
        return (ulong.Parse(parts[0], NumberStyles.HexNumber, CultureInfo.InvariantCulture) << 32)
            | ulong.Parse(parts[1], NumberStyles.HexNumber, CultureInfo.InvariantCulture);
    }

    // Позиция сегмента в TLI (лог×256 + смещение) — сравнение архивных имён.
    private static long Position(WalFileName segment)
        => (long)segment.Log * WalFileName.SegsPerLog + segment.Seg;

    // Закрытие count сегментов на мастере (логические сообщения + pg_switch_wal):
    // idle-база закрывает сегмент 16 МиБ минутами — границы форсируются
    // (образец E2eRestoreScenarios.SwitchWalsAsync).
    private static async Task SwitchWalsAsync(string adminDsn, int count, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(adminDsn);
        await conn.OpenAsync(ct);
        for (var i = 0; i < count; i++)
        {
            await using var emit = new NpgsqlCommand(
                "SELECT pg_logical_emit_message(false, 'e2e', repeat('x', 128))", conn);
            await emit.ExecuteScalarAsync(ct);
            await using var switchWal = new NpgsqlCommand("SELECT pg_switch_wal()", conn);
            await switchWal.ExecuteScalarAsync(ct);
        }
    }

    private static async Task ExecAsync(string dsn, string sql, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(dsn);
        await conn.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string> ScalarAsync(string dsn, string sql, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(dsn);
        await conn.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(sql, conn);
        return Convert.ToString(await cmd.ExecuteScalarAsync(ct)) ?? "";
    }

    // Published pg-порт мастера шарда из portalloc (резолв фактического primary
    // по пробам Patroni /primary; host-клиент — localhost). Patroni REST — https
    // (цепочка к per-contour CA, без hostname).
    private HttpClient PatroniHttp => Fx.NewPatroniHttpClient();

    private async Task<(string Host, int Port, string Node)> MasterPgAsync(
        string cluster, string shard, CancellationToken ct)
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
        return ("localhost", entries[$"{shard}/{primary}"].GetProperty("pg").GetInt32(), primary!);
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

    // Сид кластера (копия E2eRestoreScenarios.SeedClusterAsync).
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

        // Пул + policy с секцией s3 (без неё подсистема бэкапов кластера молчит).
        await Fx.SeedS3PoolAsync(Pool, ct);
        await G.PutAsync(Endpoint, $"/pgworker/backups/{cluster}/policy",
            $"{{\"full_max_age_sec\":3600,{E2eEnvironment.PoolS3(Pool)}}}", null, ct);
    }
}
