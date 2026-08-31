using PgWorker.Core.Model;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.Moves;
using Xunit;

namespace PgWorker.UnitTests.Moves;

// Модель + классификация репарации брошенных переездов (adopt-repair spec §3.5):
// парсер несёт phase/updated_unix статус-ключа; MR1-таблица state × routing ×
// возраст → синтетическая заявка / отсутствие действия.
public class MoveRepairClassifierTests
{
    // AAA (spec §3.5 модель): снапшот-парсер несёт phase и updated_unix
    // статус-ключа — возраст брошенного статуса без дополнительных чтений etcd.
    [Fact]
    public void ParseClusters_StatusCarriesPhaseAndUpdatedUnix()
    {
        // Arrange: routing + статус SYNCING/copy с updated_unix.
        var kvs = new List<Kv>
        {
            new("/clusters/demo/config", """{"buckets":1,"dbname":"demo","created_unix":1755900000}""", 1),
            new("/clusters/demo/buckets/routing/bucket_3", "s1", 2),
            new("/clusters/demo/buckets/status/bucket_3",
                """{"bucket":"bucket_3","state":"SYNCING","owner":"s1","target":"s2","started_unix":1755840000,"updated_unix":1755850000,"phase":"copy"}""", 3),
        };

        // Act
        var parsed = ClusterSnapshotParser.ParseClusters(kvs, out _);

        // Assert
        var route = parsed.Value.Single(c => c.Config.Cluster == "demo").Routing.Single(r => r.Id == 3);
        route.MovePhase.Should().Be("copy");
        route.MoveUpdatedUnix.Should().Be(1755850000);
    }

    // Стенд классификации: дефолтные пороги (Stale 600 / Frozen 120).
    private static readonly MovesRuntimeOptions Opts = new();

    private static BucketRoute Route(int id, BucketMoveState state, string? target,
        string? phase = null, long? updated = null)
        => new(id, "s1", state, MoveTarget: target, MoveSource: "s1", MovePhase: phase, MoveUpdatedUnix: updated);

    // AAA (MR1): ABORTING протухший (900 > 600) → abort без force (resuming-доводка).
    [Fact]
    public void Classify_StaleAborting_AbortNoForce()
    {
        // Arrange
        var route = Route(7, BucketMoveState.Aborting, "s1", phase: "cleanup", updated: 1000);

        // Act
        var repair = MoveRepairProcess.Classify(route, routingOwner: "s1", nowUnix: 1900, Opts);

        // Assert
        repair.Should().NotBeNull();
        repair!.Op.Should().Be(MoveOp.Abort);
        repair.Force.Should().BeFalse();
        repair.RequestedBy.Should().Be("pgworker-repair");
        repair.Bucket.Should().Be("bucket_7");
    }

    // AAA (MR1): SYNCING, routing==owner, 700 > 600 → abort без force (свежесть пройдёт сама).
    [Fact]
    public void Classify_SyncingOwnerStale_AbortNoForce()
    {
        // Arrange
        var route = Route(3, BucketMoveState.Syncing, "s2", phase: "copy", updated: 1000);

        // Act
        var repair = MoveRepairProcess.Classify(route, routingOwner: "s1", nowUnix: 1700, Opts);

        // Assert
        repair.Should().NotBeNull();
        repair!.Op.Should().Be(MoveOp.Abort);
        repair.Force.Should().BeFalse();
    }

    // AAA (MR1): SYNCING, routing==target — flip прошёл, статус завис → abort force
    // (без force AbortSequence даёт permanent-отказ — цикл).
    [Fact]
    public void Classify_SyncingRoutingTargetStale_AbortForce()
    {
        // Arrange
        var route = Route(3, BucketMoveState.Syncing, "s2", phase: "copy", updated: 1000);

        // Act
        var repair = MoveRepairProcess.Classify(route, routingOwner: "s2", nowUnix: 1700, Opts);

        // Assert
        repair.Should().NotBeNull();
        repair!.Op.Should().Be(MoveOp.Abort);
        repair.Force.Should().BeTrue();
    }

    // AAA (MR1): FROZEN, routing==owner, 200 > 120 → abort без force (уборка + re-GRANT).
    [Fact]
    public void Classify_FrozenOwnerFrozenSec_AbortNoForce()
    {
        // Arrange
        var route = Route(11, BucketMoveState.Frozen, "s2", phase: "cutover-wait", updated: 1000);

        // Act
        var repair = MoveRepairProcess.Classify(route, routingOwner: "s1", nowUnix: 1200, Opts);

        // Assert
        repair.Should().NotBeNull();
        repair!.Op.Should().Be(MoveOp.Abort);
        repair.Force.Should().BeFalse();
    }

    // AAA (MR1): FROZEN, routing==target, 200 > 120 → abort force (доведение перевода).
    [Fact]
    public void Classify_FrozenRoutingTargetFrozenSec_AbortForce()
    {
        // Arrange
        var route = Route(11, BucketMoveState.Frozen, "s2", phase: "cutover-wait", updated: 1000);

        // Act
        var repair = MoveRepairProcess.Classify(route, routingOwner: "s2", nowUnix: 1200, Opts);

        // Assert
        repair.Should().NotBeNull();
        repair!.Op.Should().Be(MoveOp.Abort);
        repair.Force.Should().BeTrue();
    }

    // AAA (MR1): фаза rollback-post-flip → заявка rollback (доведение отката по sub_rb).
    [Fact]
    public void Classify_RollbackPostFlipPhase_RollbackOp()
    {
        // Arrange
        var route = Route(5, BucketMoveState.Syncing, "s2", phase: MovePhases.RollbackPostFlip, updated: 1000);

        // Act
        var repair = MoveRepairProcess.Classify(route, routingOwner: "s2", nowUnix: 1200, Opts);

        // Assert
        repair.Should().NotBeNull();
        repair!.Op.Should().Be(MoveOp.Rollback);
    }

    // AAA (MR1): свежий статус (возраст < порога) — нет действия.
    [Fact]
    public void Classify_FreshStatus_Null()
    {
        // Arrange: ABORTING, возраст 100 < 600.
        var route = Route(7, BucketMoveState.Aborting, "s1", phase: "cleanup", updated: 1000);

        // Act
        var repair = MoveRepairProcess.Classify(route, routingOwner: "s1", nowUnix: 1100, Opts);

        // Assert
        repair.Should().BeNull();
    }

    // AAA (MR1): NOT_INITIALIZED — домен P3, не трогаем.
    [Fact]
    public void Classify_NotInitialized_Null()
    {
        // Arrange
        var route = Route(9, BucketMoveState.NotInitialized, null, updated: 1000);

        // Act
        var repair = MoveRepairProcess.Classify(route, routingOwner: "s1", nowUnix: 99000, Opts);

        // Assert
        repair.Should().BeNull();
    }
}
