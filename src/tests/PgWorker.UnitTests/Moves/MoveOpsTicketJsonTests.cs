using System.Text.Json;
using System.Text.Json.Serialization;
using PgWorker.App.Api.Operations;
using PgWorker.Moves;
using Xunit;

namespace PgWorker.UnitTests.Moves;

// Канонический JSON заявок (t07, спека §7): тело, которое пишет handler,
// парсится процессом (MoveRequest.Parse) — контракт постановки ↔ исполнения.
public class MoveOpsTicketJsonTests
{
    private static readonly JsonSerializerOptions Json = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    [Fact]
    public void RollbackTicketBody_ParsesAsMoveRequest()
    {
        // Arrange — тело, которое пишет RollbackBucketsHandler
        var json = JsonSerializer.Serialize(
            new MoveTickets.TicketBody("rollback", null, null, null, 1756000100, "ops"), Json);

        // Act
        var parsed = MoveRequest.Parse("bucket_0", json);

        // Assert
        parsed.IsSuccess.Should().BeTrue();
        parsed.Value.Op.Should().Be(MoveOp.Rollback);
        parsed.Value.To.Should().BeNull();
        parsed.Value.OldShard.Should().BeNull();
        parsed.Value.Force.Should().BeFalse();
        parsed.Value.RequestedUnix.Should().Be(1756000100);
        parsed.Value.RequestedBy.Should().Be("ops");
    }

    [Fact]
    public void FinalizeTicketBody_ParsesAsMoveRequest()
    {
        // Arrange
        var json = JsonSerializer.Serialize(
            new MoveTickets.TicketBody("finalize", null, "shard2", null, 1756000101, "ops"), Json);

        // Act
        var parsed = MoveRequest.Parse("bucket_0", json);

        // Assert
        parsed.Value.Op.Should().Be(MoveOp.Finalize);
        parsed.Value.OldShard.Should().Be("shard2");
    }

    [Fact]
    public void AbortTicketBody_ForceTrueWrites_ForceFalseOmitted()
    {
        // Arrange / Act — force:true пишется; отсутствие (null — так готовит
        // handler: force ? true : null) опускается и парсится как false (§4.2)
        var forced = JsonSerializer.Serialize(
            new MoveTickets.TicketBody("abort", null, null, true, 1756000102, "ops"), Json);
        var calm = JsonSerializer.Serialize(
            new MoveTickets.TicketBody("abort", null, null, null, 1756000103, "ops"), Json);

        // Assert
        forced.Should().Contain("\"force\":true");
        calm.Should().NotContain("\"force\"");
        MoveRequest.Parse("bucket_0", calm).Value.Force.Should().BeFalse();
        MoveRequest.Parse("bucket_0", forced).Value.Force.Should().BeTrue();
    }
}
