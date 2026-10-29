using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Moves;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Probes;
using PgWorker.Provisioning.Processes;
using PgWorker.UnitTests.Provisioning;
using Xunit;

namespace PgWorker.UnitTests.Moves;

// AbortSequence (spec §6.5 — порт abort-move.sh): журнал ABORTING
// ДО манипуляций (blocked → db-cleanup → failed/done), защита свежести
// AbortMinAgeSec/force, идемпотентная уборка subs → slots → pubs → re-GRANT →
// [доведение sequences при routing==target] → DROP SCHEMA не-владельцев,
// контрольная инвентаризация, возврат ACTIVE (del статус-ключа + del своей заявки).
public class AbortSequenceTests
{
    private static readonly MovesRuntimeOptions Opt = new(PollIntervalSec: 0, FreezeWaitSec: 0);

    private static MoveRequest AbortRequest(bool force = true) =>
        new("bucket_42", MoveOp.Abort, null, null, false, false, force, 100, "test");

    // Снапшот с заданным владельцем bucket_42 (routing — авторитет для abort).
    private static ClusterSnapshot SnapOf(string routing) => MoveRig.Snap() with
    {
        Routing = [new BucketRoute(42, routing, null)],
    };

    // Инвентарь артефактов «сорванного переезда»: до flip (routing=shard1) —
    // pub+слот+схема на shard1, sub+схема на shard2; reverse-набор — доведение
    // после зависшего flip (routing=shard2: sub_rb+pub на shard1, pub_rb+слот rb на shard2).
    private sealed record Artifacts(
        bool SubOnShard2 = true,
        bool SubRbOnShard1 = false,
        bool PubOnShard1 = true,
        bool PubRbOnShard2 = false,
        bool SlotSubOnShard1 = true,
        bool SlotSubRbOnShard2 = false,
        bool SlotActive = false,
        bool SchemaShard1 = true,
        bool SchemaShard2 = true,
        bool Shard2Down = false,
        bool PubStubborn = false,
        long SeqIssued = 100,
        long SeqNext = 5,
        IReadOnlyList<string>? Sequences = null);

    private sealed record Rig(
        Fakes.FakeEtcd Etcd, FakeMoveSql Sql, AbortSequence Abort, MoveStatusStore Status,
        MoveRequestsStore Requests, WorkJournal Journal, ClaimStore Claims,
        StepClock Clock, List<string> Events);

    private static Rig NewRig(string routing, Artifacts? artifacts = null, long updated = 100)
    {
        var a = artifacts ?? new Artifacts();
        var etcd = new Fakes.FakeEtcd();
        MoveRig.SeedTopology(etcd);
        etcd.Seed(MoveNames.RoutingKey("shop", "bucket_42"), routing);
        etcd.Seed(MoveNames.StatusKey("shop", "bucket_42"), new MoveStatus(
            "bucket_42", MoveStates.Syncing, "shard1", "shard2", 111, updated, "copy-wait").Serialize());
        etcd.Seed(MoveNames.MoveKey("shop", "bucket_42"), AbortRequest().Serialize());

        var events = new List<string>();
        etcd.OnPut = key =>
        {
            if (key == MoveNames.StatusKey("shop", "bucket_42"))
                events.Add("journal");
        };

        var sql = new FakeMoveSql();
        // Удалённые уборкой объекты: exists-проверки контрольной инвентаризации
        // видят БД ПОСЛЕ DROP'ов (идемпотентная модель, а не статичный снимок).
        // Раздельные наборы удалений: прямой DROP подписки падает (источник
        // недоступен) — fallback-срез локально, слот-сирота ОСТАЁТСЯ и добивается
        // фазой слотов (ревью №3); exists-проверки видят БД после DROP'ов.
        var removedSubs = new HashSet<string>();
        var removedSlots = new HashSet<string>();
        var removedPubs = new HashSet<string>();
        var removedSchemas = new HashSet<string>();
        var subDropAttempts = new HashSet<string>();
        sql.ExecuteResult = s =>
        {
            if (s.StartsWith("DROP SUBSCRIPTION "))
            {
                var name = s["DROP SUBSCRIPTION ".Length..];
                if (subDropAttempts.Add(name))
                    return Result.Failed(new ApplicationException("subscription source unavailable"));
                removedSubs.Add(name); // локальный срез после slot_name=NONE: слот — сирота
            }
            else if (s.StartsWith("DROP PUBLICATION "))
            {
                // PubStubborn: DROP «выполняется», но объект не исчезает (остаток)
                if (!a.PubStubborn)
                    removedPubs.Add(s["DROP PUBLICATION ".Length..]);
            }
            else if (s.StartsWith("DROP SCHEMA "))
                removedSchemas.Add(sql.LastDsn);
            else if (s.Contains("pg_drop_replication_slot('"))
                removedSlots.Add(s.Split('\'')[1]);
            return Result.Success();
        };
        var preflight = sql.ScalarResolver;
        sql.ScalarResolver = s =>
        {
            var dsn = sql.LastDsn;
            if (a.Shard2Down && dsn == MoveRig.DstDsn)
                throw new ApplicationException("shard2 (admin) unreachable");
            return s switch
            {
                var x when x == "SELECT 1" => 1L, // доступность шарда (scan_artifacts)
                var x when x.Contains("pg_subscription") && s.Contains("sub_bucket_42_rb")
                    => a.SubRbOnShard1 && dsn == MoveRig.SrcDsn && !removedSubs.Contains("sub_bucket_42_rb") ? 1L : 0L,
                var x when x.Contains("pg_subscription")
                    => a.SubOnShard2 && dsn == MoveRig.DstDsn && !removedSubs.Contains("sub_bucket_42") ? 1L : 0L,
                var x when x.Contains("pg_publication") && s.Contains("pub_bucket_42_rb")
                    => a.PubRbOnShard2 && dsn == MoveRig.DstDsn && !removedPubs.Contains("pub_bucket_42_rb") ? 1L : 0L,
                var x when x.Contains("pg_publication")
                    => (a.PubStubborn || a.PubOnShard1) && dsn == MoveRig.SrcDsn && !removedPubs.Contains("pub_bucket_42") ? 1L : 0L,
                var x when s == "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'sub_bucket_42'"
                    => a.SlotSubOnShard1 && dsn == MoveRig.SrcDsn && !removedSlots.Contains("sub_bucket_42") ? 1L : 0L,
                var x when s == "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'sub_bucket_42_rb'"
                    => a.SlotSubRbOnShard2 && dsn == MoveRig.DstDsn && !removedSlots.Contains("sub_bucket_42_rb") ? 1L : 0L,
                var x when s.StartsWith("SELECT active FROM pg_replication_slots") => a.SlotActive,
                var x when s.Contains("to_regnamespace") => dsn == MoveRig.SrcDsn
                    ? a.SchemaShard1 && !removedSchemas.Contains(MoveRig.SrcDsn)
                    : a.SchemaShard2 && !removedSchemas.Contains(MoveRig.DstDsn),
                var x when s.Contains("ELSE last_value - 1 END") => a.SeqIssued,
                var x when s.Contains("THEN last_value + 1") => a.SeqNext,
                _ => preflight(s),
            };
        };
        sql.ListResolver = s => s switch
        {
            var x when x.Contains("s.relkind = 'S'") => a.Sequences ?? ["seq1"],
            var x when x.Contains("_sync_") => [],
            _ => [],
        };

        var claims = new ClaimStore("/pgworker", [MoveRig.Ep], etcd, TimeProvider.System);
        claims.TryClaimClusterAsync("shop", CancellationToken.None).GetAwaiter().GetResult();
        var journal = new WorkJournal("/pgworker", etcd, [MoveRig.Ep]);
        var status = new MoveStatusStore(etcd, [MoveRig.Ep]);
        var shards = new ShardEndpoints(etcd, [MoveRig.Ep], new ShardProbe(new HttpClient()));
        var abort = new AbortSequence(sql, status, new MoveRequestsStore(etcd, [MoveRig.Ep]),
            journal, shards, MoveRig.Secrets);
        return new Rig(etcd, sql, abort, status, new MoveRequestsStore(etcd, [MoveRig.Ep]),
            journal, claims, new StepClock(), events);
    }

    // AAA: журнал ABORTING/db-cleanup с планом записан ДО первой SQL-манипуляции:
    //      срез подписок упал — журнал уже в статус-ключе (P7-самодокументирование)
    [Fact]
    public async Task Abort_JournalBeforeManipulations()
    {
        // Arrange — сорванный переезд, DROP не проходит (источник подписки недоступен)
        var rig = NewRig("shard1");
        rig.Sql.ExecuteResult = s =>
        {
            if (s.Contains("DROP SUBSCRIPTION"))
            {
                rig.Events.Add("drop-sub");
                return Result.Failed(new ApplicationException("subscription source unavailable"));
            }
            return Result.Success();
        };

        // Act
        var tick = await rig.Abort.RunAsync(
            SnapOf("shard1"), "bucket_42", AbortRequest(), rig.Claims, rig.Clock, Opt, CancellationToken.None);

        // Assert
        tick.IsSuccess.Should().BeFalse("the cut failed — transient, a repeated tick will continue");
        var raw = rig.Etcd.Store[MoveNames.StatusKey("shop", "bucket_42")].Value;
        var j = AbortJournal.Parse(raw).Value!;
        j.Phase.Should().Be("db-cleanup", "the journal was written before the SQL attempt and not overwritten");
        j.Plan.Should().Contain(p => p.Kind == "sub" && p.Name == "sub_bucket_42",
            "the plan includes the discovered subscriptions");
        j.Plan.Should().Contain(p => p.Kind == "schema" && p.Shard == "shard2",
            "the non-owner schema is in the cleanup plan");
        rig.Events.IndexOf("journal").Should().BeLessThan(rig.Events.IndexOf("drop-sub"),
            "★ the journal entry is strictly BEFORE the first DB manipulation");
    }

    // AAA: недоступный шард — инвентаризация неполна: журнал ABORTING/blocked с
    //      unreachable_shards, уборка не начиналась, Transient
    [Fact]
    public async Task Abort_UnreachableShard_BlockedJournal()
    {
        // Arrange — приёмник недоступен (его артефакты не видны)
        var rig = NewRig("shard1", new Artifacts(Shard2Down: true));

        // Act
        var tick = await rig.Abort.RunAsync(
            SnapOf("shard1"), "bucket_42", AbortRequest(), rig.Claims, rig.Clock, Opt, CancellationToken.None);

        // Assert
        tick.IsSuccess.Should().BeFalse("with an incomplete picture we do not start cleanup");
        var j = AbortJournal.Parse(rig.Etcd.Store[MoveNames.StatusKey("shop", "bucket_42")].Value).Value!;
        j.Phase.Should().Be("blocked");
        j.UnreachableShards.Should().Contain("shard2", "the blocking shard is in the journal");
        rig.Sql.Calls.Should().NotContain(c => c.Sql.Contains("DROP SUBSCRIPTION"),
            "no DB manipulations happened");
    }

    // AAA: свежий статус без force — mover возможно жив: Transient-ожидание,
    //      статус НЕ переведён в ABORTING
    [Fact]
    public async Task Abort_FreshMoveWithoutForce_Waits()
    {
        // Arrange — updated_unix = сейчас (age 0 < AbortMinAgeSec=120)
        var rig = NewRig("shard1", updated: 1770000000);

        // Act
        var tick = await rig.Abort.RunAsync(
            SnapOf("shard1"), "bucket_42", AbortRequest(force: false), rig.Claims, rig.Clock, Opt,
            CancellationToken.None);

        // Assert
        tick.IsSuccess.Should().BeFalse("protection against killing a live move");
        tick.Error!.Message.Should().Contain("AbortMinAgeSec", "hint: force bypasses the protection");
        var status = await rig.Status.GetAsync("shop", "bucket_42", CancellationToken.None);
        status.Value!.State.Should().Be(MoveStates.Syncing, "the move is still alive — ABORTING is not written");
    }

    // AAA: force + полный набор артефактов — уборка в порядке скрипта (subs →
    //      slots → pubs → GRANT → DROP SCHEMA), статус удалён, заявка удалена, Done
    [Fact]
    public async Task Abort_Force_CleansEverything_ActiveAgain()
    {
        // Arrange — зависший flip: routing=shard2 (target), артефакты на обоих шардах
        var rig = NewRig("shard2", new Artifacts(
            SubRbOnShard1: true, PubRbOnShard2: true, SlotSubRbOnShard2: true));

        // Act
        var tick = await rig.Abort.RunAsync(
            SnapOf("shard2"), "bucket_42", AbortRequest(), rig.Claims, rig.Clock, Opt, CancellationToken.None);

        // Assert
        tick.Value.Should().Be(ProcessOutcome.Done, "cleanup finished");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.StatusKey("shop", "bucket_42"),
            "the status key is deleted — the bucket is ACTIVE again at the owner");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"),
            "its own request (op=abort) is deleted");
        var sqls = rig.Sql.Calls.Select(c => c.Sql).ToList();
        var firstDropSub = new[] { "DROP SUBSCRIPTION sub_bucket_42_rb", "DROP SUBSCRIPTION sub_bucket_42" }
            .Min(name => sqls.IndexOf(name));
        var lastDropSub = new[] { "DROP SUBSCRIPTION sub_bucket_42_rb", "DROP SUBSCRIPTION sub_bucket_42" }
            .Max(name => sqls.IndexOf(name));
        var firstSlotDrop = sqls.FindIndex(s => s.Contains("pg_drop_replication_slot"));
        var firstDropPub = new[] { "DROP PUBLICATION pub_bucket_42_rb", "DROP PUBLICATION pub_bucket_42" }
            .Min(name => sqls.IndexOf(name));
        var grant = sqls.FindIndex(s => s.Contains("GRANT INSERT"));
        var dropSchema = sqls.IndexOf("DROP SCHEMA bucket_42 CASCADE");
        lastDropSub.Should().BeLessThan(firstSlotDrop, "subscriptions are cut before slots");
        firstSlotDrop.Should().BeLessThan(firstDropPub, "slots — before publications");
        firstDropPub.Should().BeLessThan(grant, "publications — before the owner re-GRANT");
        grant.Should().BeLessThan(dropSchema, "owner unfreeze — before schema removal");
        rig.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.DstDsn && c.Sql.Contains("GRANT INSERT"),
            "the owner (shard2) is unfrozen");
        rig.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.SrcDsn && c.Sql == "DROP SCHEMA bucket_42 CASCADE",
            "the schema is cut on the non-owner (shard1)");
    }

    // AAA: routing уже указывает на target (flip прошёл, статус завис) без force —
    //      permanent: доведение перевода — осознанное решение
    [Fact]
    public async Task Abort_RoutingEqualsTarget_NoForce_Rejects()
    {
        // Arrange
        var rig = NewRig("shard2");

        // Act
        var tick = await rig.Abort.RunAsync(
            SnapOf("shard2"), "bucket_42", AbortRequest(force: false), rig.Claims, rig.Clock, Opt,
            CancellationToken.None);

        // Assert
        tick.IsSuccess.Should().BeFalse();
        tick.Error!.Message.Should().Contain("force", "hint: abort would become move finalization");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"),
            "a permanent refusal deletes the request");
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.Phase.Should().Be("rejected");
    }

    // AAA: routing==target + force — режим ДОВЕДЕНИЯ: sequences владельца
    //      доводятся до выданных на старом шарде (P6, только вперёд) ДО drop schema
    [Fact]
    public async Task Abort_RoutingEqualsTarget_Force_SyncsSequencesBeforeDrop()
    {
        // Arrange — владелец (shard2) отстаёт: next 5 против issued 100
        var rig = NewRig("shard2", new Artifacts(
            SubRbOnShard1: true, PubRbOnShard2: true, SlotSubRbOnShard2: true));

        // Act
        var tick = await rig.Abort.RunAsync(
            SnapOf("shard2"), "bucket_42", AbortRequest(), rig.Claims, rig.Clock, Opt, CancellationToken.None);

        // Assert
        tick.Value.Should().Be(ProcessOutcome.Done);
        rig.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.DstDsn
            && c.Sql == """SELECT setval('bucket_42."seq1"', 100, true)""",
            "setval for the owner up to the value issued on the old shard (forward only)");
        var sqls = rig.Sql.Calls.Select(c => c.Sql).ToList();
        sqls.IndexOf("""SELECT setval('bucket_42."seq1"', 100, true)""").Should()
            .BeLessThan(sqls.IndexOf("DROP SCHEMA bucket_42 CASCADE"),
                "finalization — BEFORE the old schema is dropped (otherwise issued cannot be read)");
    }

    // AAA: схема ВЛАДЕЛЬЦА никогда не дропается — DROP SCHEMA только на не-владельце
    [Fact]
    public async Task Abort_OwnerSchemaNeverDropped()
    {
        // Arrange — владелец shard2, схема есть на обоих шардах
        var rig = NewRig("shard2", new Artifacts(
            SubRbOnShard1: true, PubRbOnShard2: true, SlotSubRbOnShard2: true));

        // Act
        var tick = await rig.Abort.RunAsync(
            SnapOf("shard2"), "bucket_42", AbortRequest(), rig.Claims, rig.Clock, Opt, CancellationToken.None);

        // Assert
        tick.Value.Should().Be(ProcessOutcome.Done);
        rig.Sql.Calls.Where(c => c.Sql.Contains("DROP SCHEMA")).Should().ContainSingle()
            .Which.Dsn.Should().Be(MoveRig.SrcDsn, "DROP SCHEMA — only with the non-owner DSN (shard1)");
    }

    // AAA: контрольная инвентаризация нашла остаток — журнал ABORTING/failed + Transient
    [Fact]
    public async Task Abort_LeftoverFailsControl()
    {
        // Arrange — публикация «не удаляется» (exists всегда true — остаток)
        var rig = NewRig("shard1", new Artifacts(PubStubborn: true));

        // Act
        var tick = await rig.Abort.RunAsync(
            SnapOf("shard1"), "bucket_42", AbortRequest(), rig.Claims, rig.Clock, Opt, CancellationToken.None);

        // Assert
        tick.IsSuccess.Should().BeFalse("a leftover after cleanup — operator attention");
        var j = AbortJournal.Parse(rig.Etcd.Store[MoveNames.StatusKey("shop", "bucket_42")].Value).Value!;
        j.Phase.Should().Be("failed");
        j.LastError.Should().Contain("pub_bucket_42", "the leftover is named in the journal");
    }

    // Детерминированные часы (защита свежести по updated_unix, Д12).
    private sealed class StepClock : TimeProvider
    {
        private DateTimeOffset _now = DateTimeOffset.FromUnixTimeSeconds(1770000000);

        public override DateTimeOffset GetUtcNow() => _now;
    }
}
