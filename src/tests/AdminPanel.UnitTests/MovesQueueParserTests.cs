using AdminPanel.Core;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Парсер очереди заявок /pgworker/moves/ на реальных фрагментах тел заявок
// (формат MoveRequest из ../pg; arch/02 §2.3.1, §7).
public class MovesQueueParserTests
{
    [Fact]
    public void Parse_RealFixture_TicketsAndErrors()
    {
        // Arrange
        var kv = EtcdFixtures.LoadKv("moves-queue.json");

        // Act
        var result = MovesQueueParser.Parse(kv);

        // Assert: 6 заявок — все канонические op, поля прочитаны толерантно
        result.Tickets.Should().HaveCount(6);
        result.Tickets.Should().Contain(new MoveTicket(
            "demo", "bucket_3", 3, "move", "shard2", 1755850000L, "ops"));
        result.Tickets.Should().Contain(t => t.Cluster == "demo" && t.Bucket == "bucket_5"
            && t.Op == "rollback" && t.To == "shard1" && t.RequestedUnix == 1755850060L);
        result.Tickets.Should().Contain(t => t.Bucket == "bucket_7" && t.Op == "finalize"
            && t.To == null && t.RequestedBy == null);        // to/requested_by отсутствуют
        result.Tickets.Should().Contain(t => t.Bucket == "bucket_9" && t.Op == "abort");
        result.Tickets.Should().Contain(t => t.Bucket == "weird" && t.BucketId == null); // неканонический leaf
        result.Tickets.Should().Contain(t => t.Cluster == "shop" && t.Bucket == "bucket_1");

        // Assert: 4 ошибки разбора — ключи названы, тикет не создан
        result.Errors.Should().HaveCount(4);
        result.Errors.Select(e => e.Key).Should().BeEquivalentTo(
        [
            "/pgworker/moves/demo/bucket_11", // неизвестный op "dance"
            "/pgworker/moves/demo/bucket_13", // битый JSON
            "/pgworker/moves/demo/bucket_15", // нет поля op
            "/pgworker/moves/broken",         // не /pgworker/moves/<C>/<bucket> (4 сегмента)
        ]);
        result.Errors.Should().OnlyContain(e => e.Reason.Length > 0);
    }

    [Fact]
    public void Parse_Empty_NoTicketsNoErrors()
    {
        // Arrange / Act / Assert
        MovesQueueParser.Parse([]).Tickets.Should().BeEmpty();
        MovesQueueParser.Parse([]).Errors.Should().BeEmpty();
    }
}
