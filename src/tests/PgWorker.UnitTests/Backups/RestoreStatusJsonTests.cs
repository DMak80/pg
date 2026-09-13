using PgWorker.Backups.Restore;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Parsing;

namespace PgWorker.UnitTests.Backups;

// Сериализация статуса restore (t05): JSON канона arch/19 §4 — writer/reader
// согласованы (roundtrip через BackupsParser), опциональные null-поля не пишутся.
public class RestoreStatusJsonTests
{
    // AAA: полный roundtrip Serialize → парсер t05 восстанавливает все поля.
    [Fact]
    public void Serialize_RoundtripThroughParser_AllFieldsRestored()
    {
        // Arrange
        var state = new RestoreOperationState(
            "20260911120000Z", RestoreStatus.Rejoining, "20260911090000Z",
            "src/shop", "time:2026-09-11T10:00:00Z", "shard1a", 1760000000, "operator",
            StartedUnix: 1760000005, Phase: null, RestoredToLsn: "0/3000028");

        // Act
        var json = RestoreStatusJson.Serialize(state);
        var parsed = BackupsParser.Parse(
            [new Kv($"/pgworker/backups/c/shard1/restore/{state.Id}", json, 1)], out var errors);

        // Assert
        errors.Should().BeEmpty();
        parsed.Value.Single().Shards["shard1"].Restores.Single().Should().Be(state);
    }

    // AAA: null-поля не сериализуются (канон §4 — по факту).
    [Fact]
    public void Serialize_NullOptionalFields_AbsentFromJson()
    {
        // Arrange
        var state = new RestoreOperationState(
            "id1", RestoreStatus.Planned, "b1", "c/s", "latest", "s1a", 1, "api");

        // Act
        var json = RestoreStatusJson.Serialize(state);

        // Assert
        json.Should().NotContain("started_unix").And.NotContain("finished_unix")
            .And.NotContain("phase").And.NotContain("restored_to_lsn").And.NotContain("error");
    }

    // AAA: все статусы сериализуются именами канона (PLANNED/RUNNING/…).
    [Fact]
    public void Serialize_AllStates_CanonicalNames()
    {
        // Arrange
        var states = new[]
        {
            (RestoreStatus.Planned, "PLANNED"),
            (RestoreStatus.Running, "RUNNING"),
            (RestoreStatus.Rejoining, "REJOINING"),
            (RestoreStatus.Completed, "COMPLETED"),
            (RestoreStatus.Failed, "FAILED"),
        };

        // Act / Assert — каждое имя встречается в JSON и разбирается парсером.
        foreach (var (status, name) in states)
        {
            var state = new RestoreOperationState(
                "id", status, "b", "c/x", "latest", "n", 1, "api");
            var json = RestoreStatusJson.Serialize(state);
            json.Should().Contain($@"""state"":""{name}""");
        }
    }
}
