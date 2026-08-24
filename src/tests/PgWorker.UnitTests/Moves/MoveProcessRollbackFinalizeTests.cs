using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Moves;
using PgWorker.Provisioning.Processes;
using Xunit;

namespace PgWorker.UnitTests.Moves;

// Rollback (t01 задача 15, spec §6.3): зеркальный cutover с DropStatusOnFail
// («нет ключа = ACTIVE»), поиском sub_rb ровно на одном не-владельце; пост-flip:
// DROP SUBSCRIPTION sub_rb + DROP PUBLICATION pub_rb + ОБЯЗАТЕЛЬНАЯ разморозка
// вернувшегося владельца. Finalize (spec §6.4): SubscriptionDrop с fallback
// DISABLE → slot_name=NONE → DROP при недоступном источнике, добивание слотов
// (основной + осиротевшие tablesync), DROP SCHEMA на old — последним.
public class MoveProcessRollbackFinalizeTests
{
    private static readonly MovesRuntimeOptions Fast = new(PollIntervalSec: 0, FreezeWaitSec: 0);

    private const string RollbackRequest = """{"op":"rollback","requested_unix":100}""";

    // Снапшот после move: bucket_42 живёт на shard2 (откат поведёт на shard1).
    private static ClusterSnapshot RollbackSnap() => MoveRig.Snap() with
    {
        Routing = [new BucketRoute(42, "shard2", null)],
    };

    // Слой «после move»: sub_bucket_42_rb есть на shard1 (не-владельце), на shard2 — pub_rb.
    private static void ReverseLayer(FakeMoveSql sql)
    {
        var preflight = sql.ScalarResolver;
        sql.ScalarResolver = s => s switch
        {
            var x when x.Contains("pg_subscription") && s.Contains("sub_bucket_42_rb")
                => sql.LastDsn == MoveRig.SrcDsn ? 1L : 0L,
            var x when x.Contains("pg_publication") && s.Contains("pub_bucket_42_rb")
                => sql.LastDsn == MoveRig.DstDsn ? 1L : 0L,
            _ => preflight(s),
        };
    }

    // AAA: rollback из ACTIVE с живой обратной подпиской — зеркальный flip на
    //      shard1, срез sub_rb, DROP pub_rb, разморозка вернувшегося владельца, Done
    [Fact]
    public async Task Rollback_ActiveWithReverse_FlipsBackAndUnfreezes()
    {
        // Arrange — move завершён: routing=shard2, sub_rb на shard1, pub_rb на shard2
        var rig = await MoveRig.NewAsync(requestJson: RollbackRequest, runtime: Fast);
        rig.Etcd.Seed(MoveNames.RoutingKey("shop", "bucket_42"), "shard2");
        ReverseLayer(rig.Sql);
        MoveRig.CutoverLayer(rig.Sql);

        // Act
        var tick = await rig.Process.TickAsync(RollbackSnap(), CancellationToken.None);

        // Assert
        tick.Value.Should().Be(ProcessOutcome.Done, "откат завершён");
        rig.Etcd.Store[MoveNames.RoutingKey("shop", "bucket_42")].Value.Should().Be("shard1",
            "зеркальный cutover вернул владельца");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.StatusKey("shop", "bucket_42"),
            "flip удалил статус-ключ (нет ключа = ACTIVE)");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"),
            "успех — заявка удалена");
        rig.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.SrcDsn
            && c.Sql == "DROP SUBSCRIPTION sub_bucket_42_rb",
            "обратная подписка срезана на вернувшемся владельце");
        rig.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.DstDsn
            && c.Sql == "DROP PUBLICATION pub_bucket_42_rb",
            "обратная публикация удалена на бывшем владельце");
        rig.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.SrcDsn && c.Sql.Contains("GRANT INSERT"),
            "вернувшийся владелец разморожен (обязательный пост-шаг)");
    }

    // AAA: transient-сбой разморозки ПОСЛЕ flip — flip атомарно оставляет фазу
    //      rollback-post-flip в статус-ключе (заявка жива); повторный тик доводит
    //      GRANT и завершает откат, не путая доведение с новым откатом — иначе
    //      бакет остаётся замороженным навсегда без записи (P1, ревью №1)
    [Fact]
    public async Task Rollback_UnfreezeTransientFails_RetickCompletesGrantAndDone()
    {
        // Arrange — move завершён: routing=shard2, sub_rb на shard1, pub_rb на shard2;
        // разморозка вернувшегося владельца на первом тике падает (transient)
        var rig = await MoveRig.NewAsync(requestJson: RollbackRequest, runtime: Fast);
        rig.Etcd.Seed(MoveNames.RoutingKey("shop", "bucket_42"), "shard2");
        ReverseLayer(rig.Sql);
        MoveRig.CutoverLayer(rig.Sql);

        // Слой с памятью: артефакты исчезают после первого доведения (срез/удаление),
        // GRANT разморозки валим, пока unfreezeBroken=true.
        var unfreezeBroken = true;
        var subRbGone = false;
        var pubRbGone = false;
        var preflight = rig.Sql.ScalarResolver;
        rig.Sql.ScalarResolver = s => s switch
        {
            var x when x.Contains("pg_subscription") && s.Contains("sub_bucket_42_rb")
                => !subRbGone && rig.Sql.LastDsn == MoveRig.SrcDsn ? 1L : 0L,
            var x when x.Contains("pg_publication") && s.Contains("pub_bucket_42_rb")
                => !pubRbGone && rig.Sql.LastDsn == MoveRig.DstDsn ? 1L : 0L,
            _ => preflight(s),
        };
        rig.Sql.ExecuteResult = s =>
        {
            if (rig.Sql.LastDsn == MoveRig.SrcDsn && s.Contains("GRANT INSERT") && unfreezeBroken)
                return Result.Failed(new ApplicationException("владелец недоступен (transient)"));
            if (s == "DROP SUBSCRIPTION sub_bucket_42_rb")
                subRbGone = true;
            if (s == "DROP PUBLICATION pub_bucket_42_rb")
                pubRbGone = true;
            return Result.Success();
        };

        // Act — тик 1: flip состоялся, доведение упало на разморозке
        var tick1 = await rig.Process.TickAsync(RollbackSnap(), CancellationToken.None);

        // Assert — transient: заявка жива, фаза доведения ПЕРСИСТИРОВАНА (маркер «flip был»)
        tick1.IsSuccess.Should().BeFalse("сбой разморозки — не успех тика");
        tick1.Error!.Message.Should().Contain("не разморозить", "причина — transient разморозки");
        rig.Etcd.Store[MoveNames.RoutingKey("shop", "bucket_42")].Value.Should().Be("shard1",
            "flip прошёл до сбоя — routing уже вернулся");
        rig.Etcd.Store.Should().ContainKey(MoveNames.MoveKey("shop", "bucket_42"),
            "transient-сбой оставляет заявку живой");
        rig.Etcd.Store.Should().ContainKey(MoveNames.StatusKey("shop", "bucket_42"),
            "фаза rollback-post-flip записана — повторный тик отличит доведение от нового отката");
        var status = MoveStatus.Parse(rig.Etcd.Store[MoveNames.StatusKey("shop", "bucket_42")].Value);
        status.Value.Phase.Should().Be(MovePhases.RollbackPostFlip, "фаза доведения отката");
        status.Value.Owner.Should().Be("shard1", "владелец после flip — вернувшийся шард");
        status.Value.Target.Should().Be("shard2", "target — бывший владелец (остатки доберёт finalize)");

        // Act — тик 2 (свежий снапшот: routing=shard1): доведение без повторного cutover
        unfreezeBroken = false;
        var callsAfterTick1 = rig.Sql.Calls.Count;
        var tick2 = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert — GRANT доведён, заявка Done/удалена, статус снят (= ACTIVE)
        tick2.Value.Should().Be(ProcessOutcome.Done, "доведение завершает откат");
        var retickCalls = rig.Sql.Calls.Skip(callsAfterTick1).ToList();
        retickCalls.Should().Contain(c => c.Dsn == MoveRig.SrcDsn && c.Sql.Contains("GRANT INSERT"),
            "разморозка владельца доведена повторным тиком");
        retickCalls.Should().NotContain(c => c.Sql.Contains("REVOKE"),
            "повторного cutover (заморозки) не было — только пост-шаги");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"),
            "успех — заявка удалена");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.StatusKey("shop", "bucket_42"),
            "нет ключа = ACTIVE — семантика сохранена после доведения");
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.Phase.Should().Be("done", "журнал фиксирует завершение отката");
    }

    // AAA: откат доводится (фаза rollback-post-flip в статус-ключе) — параллельная
    //      move-заявка НЕ встраивается повторным cutover, а отвергается: сначала
    //      дожидаемся доведения отката (по образцу гварда ABORTING, ревью №1)
    [Fact]
    public async Task Move_WhileRollbackPostFlipStatus_RejectsInsteadOfCutover()
    {
        // Arrange — статус доведения отката жив, заявка move на тот же бакет
        var rig = await MoveRig.NewAsync(seededStatus: new MoveStatus(
            "bucket_42", MoveStates.Frozen, "shard1", "shard2", 111, 122, MovePhases.RollbackPostFlip));
        MoveRig.CutoverLayer(rig.Sql);

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        tick.IsSuccess.Should().BeFalse("move не резюмируется в cutover поверх доведения отката");
        tick.Error!.Message.Should().Contain("откат", "подсказка — сначала довести откат");
        rig.Etcd.Store[MoveNames.RoutingKey("shop", "bucket_42")].Value.Should().Be("shard1",
            "routing не тронут — повторного flip не было");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"),
            "permanent-отказ удаляет заявку");
    }

    // AAA: обратной подписки нет нигде — permanent «откат только полным re-copy»
    [Fact]
    public async Task Rollback_NoReverseAnywhere_RejectsPermanent()
    {
        // Arrange — артефактов обратной репликации нет (дефолтный резолвер: 0)
        var rig = await MoveRig.NewAsync(requestJson: RollbackRequest, runtime: Fast);
        rig.Etcd.Seed(MoveNames.RoutingKey("shop", "bucket_42"), "shard2");

        // Act
        var tick = await rig.Process.TickAsync(RollbackSnap(), CancellationToken.None);

        // Assert
        tick.IsSuccess.Should().BeFalse("без обратной подписки отката нет — только re-copy");
        tick.Error!.Message.Should().Contain("re-copy", "подсказка: повторный move после abort");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"),
            "permanent-отказ удаляет заявку");
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.Phase.Should().Be("rejected");
    }

    // AAA: finalize при недоступном источнике подписки — DROP локально через
    //      DISABLE → slot_name=NONE → DROP; слот-сирота на old добит (ревью №3)
    [Fact]
    public async Task Finalize_SourceUnavailable_SubDroppedLocally_OrphanSlotKilled()
    {
        // Arrange — move завершён (routing=shard2, old=shard1): прямой DROP падает
        // (источник подписки недоступен), fallback проходит; слот на old неактивен
        var rig = await MoveRig.NewAsync(
            requestJson: """{"op":"finalize","old_shard":"shard1","requested_unix":100}""",
            runtime: Fast);
        rig.Etcd.Seed(MoveNames.RoutingKey("shop", "bucket_42"), "shard2");
        // Первый DROP падает (удалённый срез слота невозможен); повторный — после
        // отвязки слота (slot_name=NONE) — проходит: локальный DROP источника не требует.
        var dropCalls = 0;
        rig.Sql.ExecuteResult = s =>
        {
            if (s != "DROP SUBSCRIPTION sub_bucket_42")
                return Result.Success();
            dropCalls++;
            return dropCalls == 1
                ? Result.Failed(new ApplicationException("источник подписки недоступен"))
                : Result.Success();
        };
        FinalizeLayer(rig.Sql, subOnOwner: true, mainSlotOnOld: true, orphanSlotActive: false);

        // Act
        var tick = await rig.Process.TickAsync(RollbackSnap(), CancellationToken.None);

        // Assert
        tick.Value.Should().Be(ProcessOutcome.Done, "локальный срез достаточен для уборки");
        var sqls = rig.Sql.Calls.Select(c => c.Sql).ToList();
        sqls.Should().ContainInOrder(
        [
            "ALTER SUBSCRIPTION sub_bucket_42 DISABLE",
            "ALTER SUBSCRIPTION sub_bucket_42 SET (slot_name = NONE)",
            "DROP SUBSCRIPTION sub_bucket_42",
            "SELECT pg_drop_replication_slot('sub_bucket_42')",
        ], "fallback-цепочка среза локально + добивание слота-сироты на источнике");
    }

    // AAA: finalize убирает артефакты в порядке скрипта — подписки → публикации →
    //      слоты-сироты tablesync → DROP SCHEMA (последним, с данными)
    [Fact]
    public async Task Finalize_OrderAndOrphans()
    {
        // Arrange — после move: sub_rb и pub на old (shard1), sub и pub_rb у
        // владельца (shard2), схема на old, неактивный orphan-слот tablesync
        var rig = await MoveRig.NewAsync(
            requestJson: """{"op":"finalize","old_shard":"shard1","requested_unix":100}""",
            runtime: Fast);
        rig.Etcd.Seed(MoveNames.RoutingKey("shop", "bucket_42"), "shard2");
        FinalizeLayer(rig.Sql, subOnOwner: true, mainSlotOnOld: false, orphanSlotActive: false);

        // Act
        var tick = await rig.Process.TickAsync(RollbackSnap(), CancellationToken.None);

        // Assert
        tick.Value.Should().Be(ProcessOutcome.Done, "артефакты убраны за один тик");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"),
            "успех — заявка удалена");
        var sqls = rig.Sql.Calls.Select(c => c.Sql).ToList();
        sqls.Should().ContainInOrder(
        [
            "DROP SUBSCRIPTION sub_bucket_42_rb",
            "DROP SUBSCRIPTION sub_bucket_42",
            "DROP PUBLICATION pub_bucket_42",
            "DROP PUBLICATION pub_bucket_42_rb",
            "SELECT pg_drop_replication_slot('sub_bucket_42_sync_1234')",
            "DROP SCHEMA bucket_42 CASCADE",
        ], "порядок: подписки (держат слоты/WAL) → публикации → слоты → схема");
    }

    // AAA: активный orphan-слот пропускается (кто-то читает) — но схема дропается
    [Fact]
    public async Task Finalize_ActiveOrphanSlot_Skipped()
    {
        // Arrange — orphan-слот активен (walsender ещё держит)
        var rig = await MoveRig.NewAsync(
            requestJson: """{"op":"finalize","old_shard":"shard1","requested_unix":100}""",
            runtime: Fast);
        rig.Etcd.Seed(MoveNames.RoutingKey("shop", "bucket_42"), "shard2");
        FinalizeLayer(rig.Sql, subOnOwner: true, mainSlotOnOld: false, orphanSlotActive: true);

        // Act
        var tick = await rig.Process.TickAsync(RollbackSnap(), CancellationToken.None);

        // Assert
        tick.Value.Should().Be(ProcessOutcome.Done, "активный слот — предупреждение, не блокер");
        rig.Sql.Calls.Should().NotContain(c => c.Sql.Contains("pg_drop_replication_slot"),
            "активный слот не дропается — разберётся оператор");
        rig.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.SrcDsn
            && c.Sql == "DROP SCHEMA bucket_42 CASCADE",
            "схема дропается независимо от слота (поведение скрипта)");
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.LastError.Should().Contain("sub_bucket_42_sync_1234",
            "пропуск активного слота — громко в журнал");
    }

    // Слой «артефакты после move» поверх префлайт-резолвера: у владельца (shard2)
    // sub + pub_rb, на old (shard1) — sub_rb + pub + схема + orphan-слот tablesync.
    // mainSlotOnOld: основной слот остался на old (fallback-срез подписки).
    private static void FinalizeLayer(
        FakeMoveSql sql, bool subOnOwner, bool mainSlotOnOld, bool orphanSlotActive)
    {
        var preflight = sql.ScalarResolver;
        sql.ScalarResolver = s => s switch
        {
            // Подписки: sub_bucket_42 у владельца (shard2), sub_rb на old (shard1).
            var x when x.Contains("pg_subscription") && s.Contains("sub_bucket_42_rb")
                => sql.LastDsn == MoveRig.SrcDsn ? 1L : 0L,
            var x when x.Contains("pg_subscription")
                => sql.LastDsn == MoveRig.DstDsn && subOnOwner ? 1L : 0L,
            // Публикации: pub на old, pub_rb у владельца.
            var x when x.Contains("pg_publication") && s.Contains("pub_bucket_42_rb")
                => sql.LastDsn == MoveRig.DstDsn ? 1L : 0L,
            var x when x.Contains("pg_publication")
                => sql.LastDsn == MoveRig.SrcDsn ? 1L : 0L,
            // Основной слот на old: остался только при fallback-срезе подписки; неактивен.
            var x when s == "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'sub_bucket_42'"
                => mainSlotOnOld ? 1L : 0L,
            var x when s == "SELECT active FROM pg_replication_slots WHERE slot_name = 'sub_bucket_42'" => false,
            // Активность orphan-слота tablesync (P8).
            var x when s == "SELECT active FROM pg_replication_slots WHERE slot_name = 'sub_bucket_42_sync_1234'"
                => orphanSlotActive,
            // Схема: есть на old (SrcDsn), нет у владельца.
            var x when s.Contains("to_regnamespace") => sql.LastDsn == MoveRig.SrcDsn,
            _ => preflight(s),
        };
        sql.ListResolver = s => s.Contains("sub_bucket_42_sync_") ? ["sub_bucket_42_sync_1234"] : [];
    }
}
