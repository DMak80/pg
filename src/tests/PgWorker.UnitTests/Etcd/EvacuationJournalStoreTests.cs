using PgWorker.Etcd.Coordination;
using PgWorker.UnitTests.Provisioning;
using Xunit;

namespace PgWorker.UnitTests.Etcd;

// EvacuationJournalStore (t09): Pg-доменный журнал эвакуаций /pgworker/evacuations/<C>/<X> —
// выделен из WorkJournal при переносе координации в Shared.Etcd (t09, spec §4.1).
public class EvacuationJournalStoreTests
{
    private const string Ep = "http://etcd:2379";

    // AAA: round-trip журнала эвакуации — все поля переживают запись/чтение.
    [Fact]
    public async Task RoundTrip_EvacuationJournal()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        var store = new EvacuationJournalStore(etcd, [Ep]);
        var original = new EvacuationJournal(
            new Dictionary<int, string> { [0] = "shard1", [3] = "shard1" },
            "shard-dead",
            1755900000,
            "QUARANTINED",
            1755900600);

        // Act
        await store.WriteAsync("shop", "shard2", original, CancellationToken.None);
        var result = await store.ReadAsync("shop", "shard2", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().NotBeNull();
        var journalRead = result.Value!;
        journalRead.Buckets.Should().BeEquivalentTo(original.Buckets);
        journalRead.Reason.Should().Be("shard-dead");
        journalRead.State.Should().Be("QUARANTINED");
        journalRead.ReturnedUnix.Should().Be(1755900600);
    }
}
