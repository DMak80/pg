using System.Diagnostics;
using System.Text.Json;
using Npgsql;
using PgWorker.Backups.Restore;
using PgWorker.Core.Templates;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.IntegrationTests.Docker;
using PgWorker.Provisioning.Sql;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E восстанавливаемости через точку TLI-переключения (t28, arch/19 §10):
// полный бэкап на TLI1 → docker stop мастера (Patroni failover, TLI ≥ 2) →
// контрольные данные на новом TLI → строгий гейт смешанности (точка
// переключения ВНУТРИ сегмента, объект wal/ с байтами обеих таймлайнов
// доставлен) → ДВЕ restore-заявки на ОДНОЙ цепочке и ОДНОМ контуре класса
// (граница изоляции — сценарный класс, docs/e2e-isolation.md §1): (a) latest —
// полный хвост через точку; (b) PITR target_time — остановка в цели ПОСЛЕ
// переключения. Окружение целиком своё (сеть/etcd/MinIO по guid, динамические
// порты, teardown при любом исходе с ассертом чистоты); телеметрия —
// docs/e2e-launch.md ([PHASE]-строки, MarkFailed-режим оставляет контейнеры).
public sealed class TliSwitchContext : IAsyncLifetime
{
    private const string Bucket = "pgworker-backups";

    // Поля контура (spec §3.1): заполняются InitializeAsync, читаются Fact'ами.
    public E2eEnvironment Fx = null!;
    public string Cluster = null!;
    public string DsnBefore = null!;       // dsn-ключ до любых действий (AC2)
    public string OldBackupId = null!;     // исходный COMPLETED-полный на TLI1
    public string WalStartSegment = null!; // wal_start_segment исходного полного
    public uint Tli2;                      // таймлайн после failover (≥ 2)
    public DateTime TCut;                  // цель PITR: после post-switch, до after-cut
    public long AfterCutPos;               // позиция доставки after-cut (log*256+seg)
    public bool LatestRestoreSucceeded;    // исход Fact 1 — гейт skip'а Fact 2

    private string Endpoint => Fx.EtcdEndpoint;

    private EtcdGateway G => Fx.Gateway;

    public async ValueTask InitializeAsync()
    {
        // Гейт docker в фикстуре: без PGW_TEST_DOCKER=1 Fact'ы скипаются
        // DockerTrait'ом — контур не поднимаем (InitializeAsync выполняется
        // до первого Fact'а класса).
        if (Environment.GetEnvironmentVariable(DockerTrait.EnvVar) != "1")
            return;
        var ct = TestContext.Current.CancellationToken;
        try
        {
            await PrepareAsync(ct);
        }
        catch
        {
            Fx?.MarkFailed(); // Fx может быть ещё null (падение StartAsync) —
                              // гвард от NRE, маскирующего исходную причину
            throw;            // оба Fact'а падают ошибкой фикстуры (spec §3.2 шаг 6)
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Fx is null)
            return; // контур не поднимался (гейт docker выключен)
        // Teardown окружения при любом исходе: kill воркеров → dispose
        // etcd/MinIO → rm своих контейнеров/томов/сети → АССЕРТ ЧИСТОТЫ;
        // MarkFailed-режим останавливает, не удаляя (разбор по артефактам
        // /tmp/pgw-e2e-artifacts-<guid>/ + README-cleanup.txt).
        await Fx.DisposeAsync();
    }

    // Пометка «контур упал» для teardown окружения (e2e-launch): остановить,
    // не удалять. Fx может быть ещё null (падение StartAsync) — гвард.
    // Fact'ы зовут именно этот метод (не Fx.MarkFailed напрямую).
    public void MarkFailed() => Fx?.MarkFailed();

    // Вся подготовка контура (шаги 1–6 spec §3.2).
    private async Task PrepareAsync(CancellationToken ct)
    {
        // Шаг 1 — контур: окружение (сеть/etcd/MinIO по guid, динамические
        // порты), сид кластера tlsw<тег> (2 шарда × 2 реплики — как образцы),
        // воркер с ОБЪЕДИНЁННЫМ бэкап-комплектом (полные + WAL-агенты +
        // restore одновременно — spec §3.1); provisioning DONE ≤ 360 с.
        Fx = await E2eEnvironment.StartAsync("tlsw", withMinio: true, ct: ct);
        Cluster = $"tlsw{Fx.ClusterTag}";
        await SeedClusterAsync(Cluster);
        await StartTliSwitchHostAsync("tlsw", ct);
        var provisioned = await WaitPhaseAsync(
            "provisioning", () => ProvisionedAsync(Cluster), TimeSpan.FromSeconds(360), ct);
        provisioned.Should().BeTrue("provisioning должен дойти до DONE до бэкапов: "
            + await DumpDiagnosticsAsync(Cluster, "shard1"));

        // Эталон dsn-ключа ДО любых действий: эталон для ассерта «restore не
        // трогает portalloc/dsn» (AC2, Fact 1; образец rs-latest).
        DsnBefore = (await GetOrNullAsync($"/clusters/{Cluster}/shards/shard1/dsn"))!.Value!;
        DsnBefore.Should().NotBeNullOrEmpty("dsn пишется при provisioning");

        // Шаг 2 — полный на TLI1 + pre-wal (TLI1-хвост после точки полного).
        await SeedFullBackupAsync(ct);

        // Гейт синхронизации реплик до failover (spec §3.2 шаг 3, ОБЯЗАТЕЛЕН):
        // точка промоута определяется ПОЗИЦИЕЙ РЕПЛИКИ (replay_lsn), а не
        // мастера — отставшая реплика промоутнется ВНУТРИ уже закрытого
        // мастером сегмента (точка выйдет из открытого S — профиль не
        // смешанный). Ждём replay_lsn каждой реплики ≥ позиции мастера
        // (последняя запись — emit «t28» в SeedFullBackupAsync, строго внутри
        // ОТКРЫТОГО сегмента): тогда promote-точка внутри открытого S —
        // единственный носитель позиции S: смешанный объект под именем
        // родителя (§3.3, AC1).
        var synced = await WaitPhaseAsync("replica-sync", () => ReplicasSyncedAsync(ct),
            TimeSpan.FromSeconds(60), ct);
        synced.Should().BeTrue("реплики обязаны догнать позицию мастера до failover: "
            + await DumpDiagnosticsAsync(Cluster, "shard1"));

        // Шаги 3–5 — failover + данные нового TLI.
        await FailoverAsync(ct);
        await SeedNewTimelineAsync(ct);

        // Шаг 6 — строгий гейт смешанности: контур готов к заявкам.
        await AssertMixedSwitchAsync(ct);
        Console.WriteLine($"[PHASE] tlsw-ready: контур готов (OldBackupId={OldBackupId}, " +
            $"Tli2={Tli2}, T_cut={TCut:yyyy-MM-ddTHH:mm:ssZ})");
    }

    // Скачивание и разбор wal/<tli>.history (host-клиент GetObjectAsync +
    // WalHistory.Parse — строгий парсер t04). Последняя запись файла
    // описывает сам <tli>: родитель и точка переключения switchWALLSN.
    private async Task<PgWorker.Backups.WalHistoryEntry> HistoryEntryAsync(
        string cluster, string shard, uint tli, CancellationToken ct)
    {
        await using var s3 = HostS3Client();
        var got = await s3.GetObjectAsync(cluster, shard, $"wal/{tli:x8}.history", ct);
        got.IsSuccess.Should().BeTrue(
            $"history-объект wal/{tli:x8}.history доступен: {got.Error?.Message}");
        var entries = PgWorker.Backups.WalHistory.Parse(got.Value!);
        entries.Should().NotBeNull(
            $"history-файл wal/{tli:x8}.history строго разбирается: [{got.Value}]");
        return entries![^1];
    }

    // LSN «X/Y» → число (обе половины hex).
    private static ulong ParseLsn(string lsn)
    {
        var parts = lsn.Split('/');
        return (ulong.Parse(parts[0], System.Globalization.NumberStyles.HexNumber) << 32)
               | ulong.Parse(parts[1], System.Globalization.NumberStyles.HexNumber);
    }

    // Шаг 6 — СТРОГИЙ гейт смешанности (spec §3.3, AC1; решение user-review):
    // (1) switchWALLsn ВНУТРИ сегмента S (не на границе 16 МиБ — «на границе»
    //     покрытием t28 не считается: накат шёл бы по стыку сегментов);
    // (2) смешанный объект wal/0000000N…S доставлен;
    // (3) последний TLI1-сегмент == S−1 и TLI1-версии S в архиве НЕТ (мастер
    //     умер до закрытия — единственный носитель позиции S: объект TLI2);
    // (4) WalChain.Check(wal_start, list) непрерывен — тем же валидатором,
    //     которым restore-заявка пройдёт валидацию.
    // Вместе (3)+(4) финализируют требование шага 4 «TLI2-сегменты выше точки
    // переключения»: TLI2-хвост доставлен от самого S без дыр.
    // Провал ЛЮБОГО пункта — диагностическое исключение с дампом (буква
    // spec §3.3: history-содержимое, list wal/, ключи full/wal, Patroni
    // /cluster), НЕ ретрай: профиль прогона зафиксирован и либо смешанный,
    // либо контур честно красный с точной причиной.
    private async Task AssertMixedSwitchAsync(CancellationToken ct)
    {
        try
        {
            await AssertMixedSwitchCoreAsync(ct);
        }
        catch (Exception ex)
        {
            // Дамп провала: history-содержимое повторным чтением (лучшие
            // усилия — недоступность объекта могла быть самой причиной) +
            // DumpDiagnosticsAsync (list wal/, ключи full/wal, Patroni-scope,
            // журнал воркера, docker-объекты). Inner сохранён — стек ассерта
            // не теряется.
            var history = "";
            try
            {
                await using var s3 = HostS3Client();
                var got = await s3.GetObjectAsync(Cluster, "shard1", $"wal/{Tli2:x8}.history", ct);
                history = got.IsSuccess ? got.Value! : $"<недоступен: {got.Error?.Message}>";
            }
            catch (Exception he)
            {
                history = $"<сбой чтения: {he.Message}>";
            }

            throw new ApplicationException(
                $"гейт смешанности провален: {ex.Message}\nhistory=[{history}]\n"
                + await DumpDiagnosticsAsync(Cluster, "shard1"), ex);
        }
    }

    // Ассерты гейта (без обёртки диагностики).
    private async Task AssertMixedSwitchCoreAsync(CancellationToken ct)
    {
        var entry = await HistoryEntryAsync(Cluster, "shard1", Tli2, ct);
        entry.ParentTli.Should().Be(1u,
            $"последняя запись {Tli2:x8}.history — сам TLI2 от родителя TLI1: parent={entry.ParentTli}");
        var switchSeg = PgWorker.Backups.WalFileName.FromLsn(0, entry.SwitchLsn);
        (ParseLsn(entry.SwitchLsn) % (ulong)PgWorker.Backups.WalFileName.SegmentBytes)
            .Should().NotBe(0UL,
                $"точка переключения {entry.SwitchLsn} ВНУТРИ сегмента {switchSeg.Name} "
                + "(не на границе 16 МиБ) — профиль «на границе» покрытием t28 не считается");

        var names = await ListWalNamesAsync(Cluster, "shard1");
        var mixed = $"{Tli2:x8}{switchSeg.Log:x8}{switchSeg.Seg:x8}";
        names.Should().Contain(mixed,
            "смешанный объект (TLI1-префикс + END_OF_RECOVERY + TLI2-хвост) доставлен в wal/");

        var tli1Max = names
            .Select(n => PgWorker.Backups.WalFileName.TryParse(n))
            .Where(w => w is { Tli: 1 })
            .Select(w => (long)w!.Value.Log * 256 + w!.Value.Seg)
            .DefaultIfEmpty(-1).Max();
        var switchPos = (long)switchSeg.Log * 256 + switchSeg.Seg;
        tli1Max.Should().Be(switchPos - 1,
            "TLI1-цепочка кончается сегментом S−1 — мастер умер до закрытия S");
        names.Should().NotContain($"00000001{switchSeg.Log:x8}{switchSeg.Seg:x8}",
            "TLI1-версии S в архиве нет — единственный носитель позиции S: смешанный объект TLI2");

        var start = PgWorker.Backups.WalFileName.TryParse(WalStartSegment)!.Value;
        var check = PgWorker.Backups.WalChain.Check(start, names);
        check.IsContinuous.Should().BeTrue(check.GapError ??
            $"WalChain.Check от {WalStartSegment} непрерывен — тем же валидатором, что пройдёт restore-заявка");
    }

    // Шаг 3 (spec §3.2): жёсткая потеря мастера — docker stop (БЕЗ
    // switchover): Patroni сам промоутит выжившую sync-реплику (образец AC4
    // t27). Ожидание: новый primary (узел ≠ остановленному) + TLI ≥ 2 ≤ 120 с.
    private async Task FailoverAsync(CancellationToken ct)
    {
        var (_, _, master) = await MasterPgAsync(Cluster, "shard1", ct);
        Console.WriteLine($"[PHASE] tlsw-failover: docker stop pgw-{Cluster}-shard1-{master}");
        await Fx.RunDockerAsync(["stop", $"pgw-{Cluster}-shard1-{master}"], ct);
        var (host, port, newMaster) = await MasterPgAsync(Cluster, "shard1", ct);
        newMaster.Should().NotBe(master, "после смерти мастера primary — выжившая реплика");
        Console.WriteLine($"[PHASE] tlsw-failover: новый primary {newMaster}");
        Tli2 = await PrimaryTimelineAsync(Cluster, "shard1", ct);
        Tli2.Should().BeGreaterThanOrEqualTo(2u, "failover открывает новый timeline");
        _newMasterDsn = AdminDsn(host, port);
    }

    private string _newMasterDsn = null!;

    // Синхронизация реплик шарда: replay_lsn КАЖДОЙ реплики ≥ текущей позиции
    // записи мастера (pg_current_wal_lsn). Гейт профиля смешанности: мастер
    // держит сегмент S открытым, догнавшая реплика промоутнется внутри S.
    private async Task<bool> ReplicasSyncedAsync(CancellationToken ct)
    {
        var (mHost, mPort, master) = await MasterPgAsync(Cluster, "shard1", ct);
        var masterPos = ParseLsn(await ScalarAsync(
            AdminDsn(mHost, mPort), "SELECT pg_current_wal_lsn()", ct));
        var kv = await G.GetAsync(Endpoint, $"/pgworker/portalloc/{Cluster}", ct);
        var entries = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(kv.Value!.Value)!;
        foreach (var (key, addr) in entries
                     .Where(p => p.Key.StartsWith("shard1/", StringComparison.Ordinal)))
        {
            if (key.Split('/')[1] == master)
                continue; // сам мастер — не проверяется
            var replay = ParseLsn(await ScalarAsync(
                AdminDsn(addr.GetProperty("host").GetString()!,
                    addr.GetProperty("pg").GetInt32()),
                "SELECT pg_last_wal_replay_lsn()", ct));
            if (replay < masterPos)
                return false;
        }

        return true;
    }

    // Шаги 4–5 (spec §3.2): post-switch на новом TLI; доставка .history +
    // TLI2-хвоста по list-S3 (истина §2; repair-контур остановленной ноды НЕ
    // ждём — гвардом restore владеет заявка); фиксация T_cut (+ пауза 2 с —
    // разделение коммитов, rs-time-паттерн); after-cut ОБЯЗАНЫ лежать в
    // архиве (иначе их отсутствие в restore тривиально).
    private async Task SeedNewTimelineAsync(CancellationToken ct)
    {
        // Шаг 4 — данные на новом TLI + доставка TLI2-хвоста. Условие
        // усилено позицией: TLI2-именованный сегмент не ниже Next(последнего
        // доставленного TLI1-именованного; после доставки смешанного объекта
        // под именем родителя это Next(S)). Строгое «первый TLI2-именованный
        // = Next(S)» ассертит гейт §3.3 п.4 (позиция S — из .history).
        await ExecAsync(_newMasterDsn,
            "INSERT INTO tli_switch_probe SELECT g, 'post-switch' FROM generate_series(11, 15) g", ct);
        await SwitchWalsAsync(_newMasterDsn, 3, ct);
        var tailDelivered = await WaitPhaseAsync("tli2-tail", async () =>
        {
            var names = await ListWalNamesAsync(Cluster, "shard1");
            var tli1Max = names
                .Select(n => PgWorker.Backups.WalFileName.TryParse(n))
                .Where(w => w is { Tli: 1 })
                .Select(w => (long)w!.Value.Log * 256 + w!.Value.Seg)
                .DefaultIfEmpty(-1).Max();
            return names.Contains($"{Tli2:x8}.history")
                   && names.Any(n => PgWorker.Backups.WalFileName.TryParse(n) is { } w
                       && w.Tli == Tli2
                       && (long)w.Log * 256 + w.Seg > tli1Max);
        }, TimeSpan.FromSeconds(300), ct);
        tailDelivered.Should().BeTrue(
            $"wal/{Tli2:x8}.history и TLI2-сегменты выше точки переключения обязаны доставиться (агент реплики продолжает от хвоста): "
            + await DumpDiagnosticsAsync(Cluster, "shard1"));

        // Шаг 4 (конец) — фиксация цели PITR: T_cut строго между post-switch
        // и after-cut (пауза 2 с разделяет коммиты — неоднозначность цели
        // исключена, риск-таблица §7).
        TCut = DateTime.UtcNow;
        await Task.Delay(2000, ct);

        // Шаг 5 — after-cut: закрываем сегменты и ждём доставки позиции.
        // Цель = max(list) до INSERT + 3: switch ×3 закрывает сегмент с
        // after-cut и два следующих — доставка до posBefore+3 гарантирует,
        // что сегмент с after-cut записями доставлен.
        var posBefore = (await ListWalNamesAsync(Cluster, "shard1"))
            .Select(WalPosOf).DefaultIfEmpty(-1).Max();
        await ExecAsync(_newMasterDsn,
            "INSERT INTO tli_switch_probe SELECT g, 'after-cut' FROM generate_series(16, 18) g", ct);
        await SwitchWalsAsync(_newMasterDsn, 3, ct);
        AfterCutPos = posBefore + 3;
        // [PHASE]-телеметрия вокруг гейта доставки (семантика WaitPhaseAsync;
        // сам WalArchivedAsync — дословная копия образца, не трогаем):
        // строка в журнал всегда, фаза > 60 с — docker-логи в артефакты
        // (spec §3.6/AC4: ВСЕ ожидания — с [PHASE]-строками).
        var afterSw = Stopwatch.StartNew();
        var afterArchived = await WalArchivedAsync(Cluster, "shard1", AfterCutPos, ct);
        afterSw.Stop();
        Console.WriteLine($"[PHASE] after-cut-archived: ok={afterArchived}, elapsed={afterSw.Elapsed.TotalSeconds:F1}s");
        if (afterSw.Elapsed > TimeSpan.FromSeconds(60))
            await Fx.CollectDiagnosticsAsync($"slow-phase-after-cut-archived-{(int)afterSw.Elapsed.TotalSeconds}s");
        afterArchived.Should().BeTrue("сегменты с after-cut обязаны попасть в архив: "
            + await DumpDiagnosticsAsync(Cluster, "shard1"));
    }

    // Шаг 2 (spec §3.2): контрольная таблица + pre (уходят в полный) →
    // COMPLETED-полный с фиксацией OldBackupId/wal_start → pre-wal (должны
    // накатиться TLI1-хвостом через смешанный сегмент) → закрытие сегментов
    // по одному до непрерывности WalChain.Check от wal_start (паттерн
    // rs-latest против дыры на валидации) → последняя запись TLI1 в ОТКРЫТЫЙ
    // сегмент S (после этого НИ ОДНОГО pg_switch_wal до failover — точка
    // переключения окажется внутри S, «запись мастера живёт в открытом
    // сегменте S»).
    private async Task SeedFullBackupAsync(CancellationToken ct)
    {
        var (host, port, _) = await MasterPgAsync(Cluster, "shard1", ct);
        var dsn = AdminDsn(host, port);
        await ExecAsync(dsn, "CREATE TABLE tli_switch_probe(id int, note text)", ct);
        await ExecAsync(dsn,
            "INSERT INTO tli_switch_probe SELECT g, 'pre' FROM generate_series(1, 5) g", ct);
        await SwitchWalsAsync(dsn, 4, ct);
        var completed = await WaitPhaseAsync("full-completed", async () =>
            (await FullKeysAsync(Cluster, "shard1")).Any(f => f.Value.Contains("COMPLETED")),
            TimeSpan.FromSeconds(300), ct);
        if (!completed)
            throw new ApplicationException("полный не дошёл до COMPLETED: " +
                await DumpDiagnosticsAsync(Cluster, "shard1"));
        var completedFull = (await FullKeysAsync(Cluster, "shard1"))
            .Single(f => f.Value.Contains("COMPLETED"));
        OldBackupId = completedFull.Key.Split('/').Last();
        WalStartSegment = JsonSerializer
            .Deserialize<Dictionary<string, JsonElement>>(completedFull.Value)!
            ["wal_start_segment"].GetString()!;
        WalStartSegment.Should().NotBeNullOrEmpty("wal_start_segment заполняется с UPLOADING");

        // pre-wal: сегменты закрываются по одному, пока WalChain.Check от
        // wal_start не станет непрерывен (доставка — по list-S3, истина §2).
        // [PHASE]-телеметрия вручную (семантика WaitPhaseAsync): условие
        // сходится pg_switch_wal'ами — side-effect в поллинге WaitPhaseAsync
        // недопустим (switch каждые 500 мс), поэтому строка и slow-collect
        // вокруг цикла (spec §3.6/AC4: ВСЕ ожидания — с [PHASE]-строками).
        await ExecAsync(dsn,
            "INSERT INTO tli_switch_probe SELECT g, 'pre-wal' FROM generate_series(6, 10) g", ct);
        var chainSw = Stopwatch.StartNew();
        var chainDeadline = DateTime.UtcNow + TimeSpan.FromSeconds(180);
        var chainOk = false;
        while (!chainOk && DateTime.UtcNow < chainDeadline)
        {
            await SwitchWalsAsync(dsn, 1, ct);
            chainOk = await WalChainContinuousAsync(Cluster, "shard1", WalStartSegment!, ct);
        }
        chainSw.Stop();
        Console.WriteLine($"[PHASE] pre-wal-chain: ok={chainOk}, elapsed={chainSw.Elapsed.TotalSeconds:F1}s");
        if (chainSw.Elapsed > TimeSpan.FromSeconds(60))
            await Fx.CollectDiagnosticsAsync($"slow-phase-pre-wal-chain-{(int)chainSw.Elapsed.TotalSeconds}s");
        chainOk.Should().BeTrue("цепочка от wal_start обязана сойтись до failover: "
            + await DumpDiagnosticsAsync(Cluster, "shard1"));

        // Реальная запись TLI1 в открытом сегменте S — гарантия профиля
        // смешанности (риск-таблица spec §7: «записи после последнего switch
        // есть всегда» — делаем явно). БЕЗ pg_switch_wal: сегмент S остаётся
        // открытым до failover.
        await ExecAsync(dsn,
            "SELECT pg_logical_emit_message(false, 't28', repeat('x', 128))", ct);
    }

    // Admin-DSN по host:port мастера (креды su e2e-установки).
    private string AdminDsn(string host, int port)
        => DatabaseProvisioner.BuildAdminDsn(host, port, Cluster,
            new InstallSecrets(E2eFixture.SuPassword, "", "", ""));

    // Воркер t28: ОБЪЕДИНЁННЫЙ бэкап-комплект (spec §3.1): в образцах комплект
    // разнесён — StartRestoreHostAsync не задаёт Wal:AgentImage,
    // StartWalHostAsync не задаёт Restore:RecoveryTimeoutSec; t28 нужны и
    // полные, и WAL-агент, и restore ОДНОВРЕМЕННО. ОБА S3-поля — ОДНО значение
    // Fx.S3Endpoint (http://host.docker.internal:<порт>, БЕЗ замены на
    // localhost — паттерн StartRestoreHostAsync единственного зелёного контура
    // «полные + WAL + restore», spec §3.1): full-джоб получает СЫРОЙ
    // S3Endpoint в env (PGW_BK_S3_ENDPOINT, BackupJobSpec) и грузит бэкап
    // mc cp ИЗНУТРИ своего контейнера — localhost там недостижим; WAL- и
    // restore-агенты берут то же значение через AdvertisedEndpoint
    // (AgentS3Endpoint). Замена host.docker.internal → localhost — ТОЛЬКО в
    // host-клиентах сценария (HostS3Client): воркер-процесс на хосте
    // резолвит host.docker.internal.
    private Task<HostInstance> StartTliSwitchHostAsync(string name, CancellationToken ct)
        => Fx.StartHostAsync(name, extraEnv: new Dictionary<string, string>
        {
            ["PgWorker__Backups__Enabled"] = "true",
            ["PgWorker__Backups__S3__Endpoint"] = Fx.S3Endpoint,
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
            ["PgWorker__Backups__Restore__RecoveryTimeoutSec"] = "600",
            ["PgWorker__Thresholds__PatroniBootSec"] = "120",
        }, ct: ct);

    // ===== Хелперы (копии E2eRestoreScenarios/E2eBackupScenarios — файлы
    // сценариев независимы, паттерн репо; в общие утилиты не выносятся) =====

    // Ожидание фазы с телеметрией (docs/e2e-launch.md): [PHASE]-строка в
    // журнал всегда; фаза дольше 60 с — docker-логи окружения в артефакты.
    public async Task<bool> WaitPhaseAsync(
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

    // Host-клиент BackupS3 (published порт): ассерты и гейты по list-S3.
    // Замена host.docker.internal → localhost — ТОЛЬКО здесь (воркер-процесс
    // на хосте; джобы/агенты её НЕ получают — см. StartTliSwitchHostAsync).
    public PgWorker.Backups.BackupS3 HostS3Client()
        => new(new PgWorker.Backups.BackupsRuntimeOptions
        {
            Enabled = true,
            S3Endpoint = Fx.S3Endpoint.Replace(
                "host.docker.internal:", "localhost:", StringComparison.Ordinal),
            S3Bucket = Bucket,
            S3AccessKey = "minioadmin",
            S3SecretKey = "minioadmin",
            S3PathStyle = true,
        });

    // PUT PLANNED-заявки restore прямым ключом (принятое отступление образцов:
    // API-путь покрыт RestoreApiTests). backupId ОБЯЗАТЕЛЕН (инвариант t28):
    // после первой заявки новейший COMPLETED — уже TLI3-полный, а валидация
    // обязана идти от СТАРОГО полного через точку переключения.
    private async Task<string> PutRestorePlannedAsync(
        string cluster, string shard, string target, string backupId, CancellationToken ct)
    {
        var id = $"{DateTimeOffset.UtcNow:yyyyMMddHHmmss}Z-{shard}";
        var op = new RestoreOperationState(id, RestoreStatus.Planned, backupId,
            $"{cluster}/{shard}", target, $"{shard}a",
            DateTimeOffset.UtcNow.ToUnixTimeSeconds(), "e2e");
        await G.PutAsync(Endpoint, PgWorker.Backups.BackupNames.RestoreKey(cluster, shard, id),
            RestoreStatusJson.Serialize(op), null, ct);
        return id;
    }

    // Поллинг restore-ключа до терминального статуса. FAILED «дыра WAL-цепочки»
    // — внешняя гонка доставки: сигнал РЕТРАЯ новой заявкой (ChainGapException).
    private sealed class ChainGapException(string message) : ApplicationException(message);

    private async Task<Dictionary<string, JsonElement>?> WaitRestoreAsync(
        string cluster, string shard, string id, TimeSpan budget, CancellationToken ct)
    {
        Dictionary<string, JsonElement>? status = null;
        var sw = Stopwatch.StartNew();
        var done = await E2eFixture.WaitForAsync(async () =>
        {
            var raw = await GetOrNullAsync(PgWorker.Backups.BackupNames.RestoreKey(cluster, shard, id));
            if (raw is null)
                return false;
            status = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(raw.Value);
            return status is not null && (status["state"].GetString() is "COMPLETED" or "FAILED");
        }, budget, ct);
        sw.Stop();
        Console.WriteLine(
            $"[PHASE] restore {cluster}/{shard}/{id}: state={status?["state"].GetString()}, elapsed={sw.Elapsed.TotalSeconds:F1}s");
        if (sw.Elapsed > TimeSpan.FromSeconds(60))
            await Fx.CollectDiagnosticsAsync($"slow-restore-{cluster}-{shard}-{(int)sw.Elapsed.TotalSeconds}s");
        if (!done || status!["state"].GetString() == "FAILED")
        {
            var error = status?.TryGetValue("error", out var err) == true ? err.GetString() : null;
            if (error is not null && error.Contains("дыра WAL-цепочки", StringComparison.Ordinal))
                throw new ChainGapException($"restore {cluster}/{shard}/{id}: {error}");
            throw new ApplicationException(
                $"restore {cluster}/{shard}/{id} не дошёл до COMPLETED: "
                + (status is null ? "ключа нет" : status["state"].GetString())
                + (error is null ? "" : $", error={error}")
                + "; " + await DumpDiagnosticsAsync(cluster, shard));
        }

        return done ? status : null;
    }

    // Заявка restore с ретраем ≤ 3 на внешние гонки доставки (образец
    // rs-latest): FAILED «дыра» → новая заявка (демонтаж идемпотентен,
    // повторная заявка сама проводит демонтаж/джоб/rejoin). Бюджет 300 с.
    public async Task<Dictionary<string, JsonElement>> RestoreWithRetryAsync(
        string cluster, string shard, string target, string backupId, CancellationToken ct)
    {
        for (var attempt = 1; ; attempt++)
        {
            var restoreId = await PutRestorePlannedAsync(cluster, shard, target, backupId, ct);
            try
            {
                var status = await WaitRestoreAsync(cluster, shard, restoreId,
                    TimeSpan.FromSeconds(300), ct);
                return status!;
            }
            catch (ChainGapException) when (attempt < 3)
            {
                Console.Error.WriteLine(
                    $"e2e[restore]: {cluster}/{shard} попытка {attempt} — дыра цепочки (гонка доставки), повторяю заявку");
                await Task.Delay(3000, ct);
            }
        }
    }

    // Непрерывность цепочки от wal_start полного (WalChain.Check — ТОТ ЖЕ
    // валидатор, что в restore-заявке): заявку ставим только на
    // консистентной цепочке.
    private async Task<bool> WalChainContinuousAsync(
        string cluster, string shard, string walStartSegment, CancellationToken ct)
        => await E2eFixture.WaitForAsync(async () =>
        {
            var chainStart = PgWorker.Backups.WalFileName.TryParse(walStartSegment);
            if (chainStart is null)
                return true; // битый wal_start — не цепочечная проблема валидатора
            var names = await ListWalNamesAsync(cluster, shard);
            return PgWorker.Backups.WalChain.Check(chainStart.Value, names).IsContinuous;
        }, TimeSpan.FromSeconds(90), ct);

    // Диагностика провала: журнал воркера + restore-ключи/полные шарда + docker-объекты
    // кластера (фильтр по ТЕГУ кластера — substring ловит и pgw-backup-restore-*).
    public async Task<string> DumpDiagnosticsAsync(string cluster, string shard)
    {
        var workKv = await GetOrNullAsync($"/pgworker/work/{cluster}");
        var restoreKvs = await G.RangeAsync(
            Endpoint, $"/pgworker/backups/{cluster}/{shard}/restore/",
            TestContext.Current.CancellationToken);
        var fullKvs = await FullKeysAsync(cluster, shard);
        var walKv = await GetOrNullAsync($"/pgworker/backups/{cluster}/{shard}/wal");
        var walNames = await ListWalNamesAsync(cluster, shard);
        // Patroni-scope шарда целиком: рудименты прошлой жизни кластера
        // (initialize/status с чужим system-id) — прямой кандидат в причины
        // «контейнеры Up, а Patroni API молчит».
        var scopeKvs = await G.RangeAsync(
            Endpoint, $"/service/{cluster}-{shard}/", TestContext.Current.CancellationToken);
        var scopeDump = scopeKvs.IsSuccess
            ? string.Join(";", (scopeKvs.Value ?? []).Select(
                k => $"{k.Key.Replace($"/service/{cluster}-{shard}/", "")}={k.Value[..Math.Min(200, k.Value.Length)]}"))
            : $"range failed: {scopeKvs.Error?.Message}";
        // Состояния нод шарда: PROVISIONING-метка выдаёт EnsureDeclaredNodes
        // (супервиз) как создателя контейнера в гонке rejoin'а.
        var nodeStates = await G.RangeAsync(
            Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/", TestContext.Current.CancellationToken);
        var nodeDump = nodeStates.IsSuccess
            ? string.Join(";", (nodeStates.Value ?? []).Select(k =>
                $"{k.Key.Replace($"/clusters/{cluster}/shards/{shard}/nodes/", "")}={k.Value}"))
            : $"range failed: {nodeStates.Error?.Message}";
        string jobs;
        try
        {
            jobs = await Fx.RunDockerAsync(
            ["ps", "-a", "--format", "{{.ID}} {{.Names}} {{.Status}}",
                "--filter", $"name={cluster}"],
            TestContext.Current.CancellationToken);
            jobs += " | volumes=" + await Fx.RunDockerAsync(
            ["volume", "ls", "--format", "{{.Name}}", "--filter", $"name={cluster}"],
            TestContext.Current.CancellationToken);
        }
        catch (Exception e)
        {
            jobs = $"docker ps failed: {e.Message}";
        }

        // Журнал воркера целиком (политика телеметрии: без обрезок — по нему
        // отвечают «почему фаза не уложилась»; объёмы в e2e — единицы КБ).
        var journal = workKv?.Value ?? "<absent>";
        // PG-лог restore-джоба (/tmp/restore-pg.log в контейнере): какие сегменты
        // запрашивал restore_command, где сорвалось (t27-разбор: mc-404 существующего).
        var pgLog = "";
        try
        {
            var psJobs = await Fx.RunDockerAsync(
            ["ps", "-a", "--format", "{{.Names}}", "--filter", $"name=pgw-backup-restore-{cluster}-"],
            TestContext.Current.CancellationToken);
            foreach (var line in psJobs.Split('\n', StringSplitOptions.RemoveEmptyEntries))
            {
                var name = line.Trim().Split(' ')[0];
                var copied = Path.Combine(Path.GetTempPath(), $"pgw-restore-pg-{cluster}.log");
                try
                {
                    await E2eFixture.RunProcessAsync("docker", ["cp", $"{name}:/tmp/restore-pg.log", copied],
                        TestContext.Current.CancellationToken);
                    var text = File.ReadAllText(copied);
                    pgLog += $"\n== {name}:\n{text[..Math.Min(4000, text.Length)]}";
                }
                catch (Exception cp)
                {
                    pgLog += $"\n== {name}: лог недоступен ({cp.Message})";
                }

                // Видимость сегментов ИЗ ЖИВОГО джоба (тот же mc/alias/сеть):
                // t27-разбор — restore_command получил 404 на сегмент, который в S3 есть.
                try
                {
                    var live = await E2eFixture.RunProcessAsync("docker", ["exec", name, "bash", "-c",
                        "ls /tmp/.mc 2>/dev/null | head -3; for s in 000000010000000000000009 00000001000000000000000a 00000001000000000000000b; do " +
                        "mc stat \"pgwbkp/$S3_BUCKET/$SRC_PREFIX/wal/$s\" 2>&1 | head -2; done"],
                        TestContext.Current.CancellationToken);
                    pgLog += $"\n== {name} MC-STAT(живой):\n{live[..Math.Min(1500, live.Length)]}";
                }
                catch (Exception ex)
                {
                    pgLog += $"\n== {name} MC-STAT: недоступен ({ex.Message})";
                }
            }
        }
        catch (Exception e)
        {
            pgLog = $"\npg-log failed: {e.Message}";
        }

        return $"journal=[{journal}] " +
               $"restores=[{string.Join(";", (restoreKvs.Value ?? []).Select(k => k.Value))}] " +
               $"fulls=[{string.Join(";", fullKvs.Select(k => k.Value))}] " +
               $"walKey=[{walKv?.Value ?? "<absent>"}] " +
               $"wal=[{string.Join(",", walNames.OrderBy(n => n, StringComparer.Ordinal))}] " +
               $"patroniScope=[{scopeDump}] " +
               $"nodes=[{nodeDump}] " +
               $"jobs=[{jobs}]" +
               $"RESTORE.PG.LOG:{pgLog}";
    }

    // Range Failed → пустой список (поллинг повторит; null-краш в LINQ недопустим).
    public async Task<IReadOnlyList<Kv>> FullKeysAsync(string cluster, string shard)
        => (await G.RangeAsync(Endpoint, $"/pgworker/backups/{cluster}/{shard}/full/",
            TestContext.Current.CancellationToken)).Value ?? [];

    // Имена заархивированных объектов wal/ (host-клиент; истина доставки — S3).
    public async Task<List<string>> ListWalNamesAsync(string cluster, string shard)
    {
        await using var s3 = HostS3Client();
        var listed = await s3.ListWalAsync(cluster, shard, ct: TestContext.Current.CancellationToken);
        if (!listed.IsSuccess)
            Console.Error.WriteLine($"e2e[archive]: LIST FAILED {listed.Error?.Message}");
        return listed.IsSuccess ? [.. listed.Value.Select(o => o.Name)] : [];
    }

    // pg_logical_emit_message + pg_switch_wal × count: idle-база не
    // материализует пустые switch — перед каждым пишем реальную WAL-запись.
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

    // Позиция сегмента (лог×256 + смещение) — канон WalFileName.
    private static long WalPosOf(string segment)
    {
        var parsed = PgWorker.Backups.WalFileName.TryParse(segment);
        return parsed is { } w ? (long)w.Log * 256 + w.Seg : -1;
    }

    // ДОСТАВКА до позиции: последний закрытый сегмент обязан появиться в
    // wal/-префиксе (бюджет 180 с).
    private async Task<bool> WalArchivedAsync(
        string cluster, string shard, long targetPos, CancellationToken ct)
        => await E2eFixture.WaitForAsync(async () =>
        {
            var names = await ListWalNamesAsync(cluster, shard);
            return names.Select(WalPosOf).DefaultIfEmpty(-1).Max() >= targetPos;
        }, TimeSpan.FromSeconds(180), ct);

    public async Task<Kv?> GetOrNullAsync(string key)
        => (await G.GetAsync(Endpoint, key, TestContext.Current.CancellationToken)).Value;

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

    private static readonly HttpClient PatroniHttp = new() { Timeout = TimeSpan.FromSeconds(3) };

    // Published pg-порт мастера шарда из portalloc (host-клиент: localhost).
    // Резолв фактического primary — по пробам Patroni /primary (t02-подход
    // WaitForMasterAsync: master-ключ host:0 при EnableDoorman=false
    // недискриминантен); primary появляется после dsn/RUNNING — ждём.
    public async Task<(string Host, int Port, string Node)> MasterPgAsync(string cluster, string shard, CancellationToken ct)
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
        }, TimeSpan.FromSeconds(120), ct);
        return tli;
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

    // Сид кластера в стиле панели (копия E2eRestoreScenarios.SeedClusterAsync).
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
}

// Два Fact'а на ОДНОМ контуре класса (решение user-review): latest первым,
// target_time вторым — детерминированный порядок задаёт orderer (Task 4,
// инвариант §3.3). Падение подготовки → оба Fact'а красные ошибкой фикстуры.
public sealed class E2eTliSwitchScenarios(TliSwitchContext Ctx) : IClassFixture<TliSwitchContext>
{
    // AAA (Ф1-каркас): контур шагов 1–2 готов — окружение/provisioning DONE,
    // COMPLETED-полный зафиксирован, pre-wal доставлены (цепочка от wal_start
    // непрерывна). Тело расширяется фазами Ф2–Ф3 (задачи 2–4).
    [Fact]
    public async Task Restore_LatestThroughTliSwitch_PicksUpWholeTail()
    {
        DockerTrait.SkipIfUnavailable();
        try
        {
            Ctx.OldBackupId.Should().NotBeNullOrEmpty(
                "исходный COMPLETED-полный зафиксирован (шаг 2)");
            Ctx.WalStartSegment.Should().NotBeNullOrEmpty();
            Ctx.DsnBefore.Should().NotBeNullOrEmpty("dsn-эталон зафиксирован (шаг 1)");

            // Ф2-ассерты каркаса: failover состоялся, цель и after-cut зафиксированы
            Ctx.Tli2.Should().BeGreaterThanOrEqualTo(2u, "failover открыл новый timeline");
            Ctx.TCut.Should().BeAfter(DateTime.MinValue, "T_cut зафиксирован");
            Ctx.AfterCutPos.Should().BeGreaterThan(0, "позиция after-cut зафиксирована");
        }
        catch
        {
            Ctx.MarkFailed(); // контейнеры остаются для разбора (e2e-launch)
            throw;
        }
    }
}
