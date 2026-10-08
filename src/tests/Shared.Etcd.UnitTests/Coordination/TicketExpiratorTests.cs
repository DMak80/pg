using Shared.Etcd.Coordination;

namespace Shared.Etcd.UnitTests;

// TicketExpirator (t10, arch/15 §4 / arch/20 §3): возрастная экспирация
// не-начатой заявки под тройным гвардом §3.1 — journal expired
// (journal-before-manipulations) → ОДНА txn [compare Exists][del заявку][put ticket_outcomes].
public class TicketExpiratorTests
{
    private const string Ep = "http://etcd:2379";
    private const long Now = 1_757_000_000;

    private static (TicketExpirator Expirator, FakeCoordinationGateway Gateway, WorkJournal Journal) Rig()
    {
        var gateway = new FakeCoordinationGateway();
        return (new TicketExpirator(gateway, [Ep]), gateway, new WorkJournal("/kafkaworker", gateway, [Ep]));
    }

    [Fact]
    public void ParseAudit_ValidPayload_ExtractsUnixAndBy()
    {
        // Arrange: payload заявки с аудитом.
        var payload = """{"requested_unix":1757000100,"requested_by":"admin"}""";

        // Act
        var audit = TicketOutcomes.ParseAudit(payload);

        // Assert
        audit.Should().Be(new TicketRequestAudit(1757000100, "admin"));
    }

    [Theory]
    [InlineData("""{"requested_by":"admin"}""")]        // нет requested_unix
    [InlineData("""{"requested_unix":"soon"}""")]       // не число
    [InlineData("""{oops""")]                           // битый JSON
    [InlineData("""null""")]                            // не объект
    public void ParseAudit_BrokenPayload_ReturnsNull(string payload)
    {
        // Act: любой битый payload. Assert: null — возраст 0, NOT expiry.
        TicketOutcomes.ParseAudit(payload).Should().BeNull();
    }

    [Fact]
    public async Task TryExpire_AgeAboveTimeout_JournalExpiredAndTxnDelPutOutcome()
    {
        // Arrange: заявка возрастом 100 с при пороге 60; исходов нет; гварды пройдены.
        var (expirator, gateway, journal) = Rig();
        gateway.Seed("/kafkaworker/rotations/events",
            $$"""{"requested_unix":{{Now - 100}},"requested_by":"admin"}""");

        // Act
        var result = await expirator.TryExpireAsync(
            journal, "events", "rotate", "inst1",
            TicketOutcomes.Key("/kafkaworker", "events"), TicketOutcomes.KindPasswordApp,
            "/kafkaworker/rotations/events", gateway.Store["/kafkaworker/rotations/events"],
            "waiting-cluster", 60, Now, mutationLive: false, CancellationToken.None);

        // Assert: снята; journal expired с возрастом и причиной; txn удалила
        // заявку и поставила исход (camelCase).
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeTrue();
        var state = await journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("expired");
        state.Value.LastError.Should().Be("ticket age=100s reason=waiting-cluster");
        gateway.Store.Should().NotContainKey("/kafkaworker/rotations/events");
        var outcome = gateway.Store[TicketOutcomes.Key("/kafkaworker", "events")];
        outcome.Should().Contain("\"kind\":\"password-app\"");
        outcome.Should().Contain("\"outcome\":\"expired\"");
        outcome.Should().Contain("\"reason\":\"waiting-cluster\"");
        outcome.Should().Contain($"\"requested_unix\":{Now - 100}");
        outcome.Should().Contain("\"requested_by\":\"admin\"");
        outcome.Should().Contain($"\"finished_unix\":{Now}");
    }

    [Fact]
    public async Task TryExpire_AgeBelowTimeout_ReturnsFalseWithoutMutations()
    {
        // Arrange: заявка возрастом 10 с при пороге 60.
        var (expirator, gateway, journal) = Rig();
        gateway.Seed("/kafkaworker/rotations/events", $$"""{"requested_unix":{{Now - 10}}}""");

        // Act
        var result = await expirator.TryExpireAsync(
            journal, "events", "rotate", "inst1",
            TicketOutcomes.Key("/kafkaworker", "events"), TicketOutcomes.KindPasswordApp,
            "/kafkaworker/rotations/events", gateway.Store["/kafkaworker/rotations/events"],
            "waiting-cluster", 60, Now, mutationLive: false, CancellationToken.None);

        // Assert: заявка жива — обычный waiting (вызывающий пишет фазу сам);
        // txn не подавалась, исхода нет.
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeFalse();
        gateway.Txns.Should().BeEmpty();
        gateway.Store.Should().ContainKey("/kafkaworker/rotations/events");
        gateway.Store.Should().NotContainKey(TicketOutcomes.Key("/kafkaworker", "events"));
    }

    [Fact]
    public async Task TryExpire_MutationLive_ReturnsFalseWithoutMutationsAtAnyAge()
    {
        // Arrange: заявка возрастом 5000 с (далеко за порогом), но третий
        // предикат гварда ложен — процесс в незавершённой мутационной фазе.
        var (expirator, gateway, journal) = Rig();
        gateway.Seed("/kafkaworker/rotations/events", $$"""{"requested_unix":{{Now - 5000}}}""");

        // Act
        var result = await expirator.TryExpireAsync(
            journal, "events", "rotate", "inst1",
            TicketOutcomes.Key("/kafkaworker", "events"), TicketOutcomes.KindPasswordApp,
            "/kafkaworker/rotations/events", gateway.Store["/kafkaworker/rotations/events"],
            "waiting-cluster", 60, Now, mutationLive: true, CancellationToken.None);

        // Assert: начатое не снимается никогда — заявка жива, journal чист,
        // txn не подавалась (вызывающий пишет обычный waiting).
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeFalse();
        (await journal.ReadAsync("events", CancellationToken.None)).Value.Should().BeNull();
        gateway.Txns.Should().BeEmpty();
        gateway.Store.Should().ContainKey("/kafkaworker/rotations/events");
    }

    [Fact]
    public async Task TryExpire_BrokenPayload_ReturnsFalseWithoutJournal()
    {
        // Arrange: payload без requested_unix.
        var (expirator, gateway, journal) = Rig();
        gateway.Seed("/kafkaworker/rotations/events", """{"requested_by":"admin"}""");

        // Act
        var result = await expirator.TryExpireAsync(
            journal, "events", "rotate", "inst1",
            TicketOutcomes.Key("/kafkaworker", "events"), TicketOutcomes.KindPasswordApp,
            "/kafkaworker/rotations/events", gateway.Store["/kafkaworker/rotations/events"],
            "waiting-cluster", 60, Now, mutationLive: false, CancellationToken.None);

        // Assert: параноидальный отказ от снятия — заявка жива, journal чист.
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeFalse();
        (await journal.ReadAsync("events", CancellationToken.None)).Value.Should().BeNull();
        gateway.Store.Should().ContainKey("/kafkaworker/rotations/events");
    }

    [Fact]
    public async Task WriteDone_PutsOutcomeDone()
    {
        // Arrange: экспиратор + аудит заявки (+ null-аудит рестарт-хвоста).
        var (expirator, gateway, _) = Rig();

        // Act
        var done = await expirator.WriteDoneAsync(
            TicketOutcomes.Key("/kafkaworker", "events"), TicketOutcomes.KindCa,
            new TicketRequestAudit(1757000100, "it"), Now, CancellationToken.None);
        var tail = await expirator.WriteDoneAsync(
            TicketOutcomes.Key("/kafkaworker", "events"), TicketOutcomes.KindCa,
            null, Now, CancellationToken.None);

        // Assert: outcome=done перезаписью; null-аудит — requested_unix = finishedUnix,
        // requested_by опущен.
        done.IsSuccess.Should().BeTrue();
        tail.IsSuccess.Should().BeTrue();
        gateway.Store[TicketOutcomes.Key("/kafkaworker", "events")]
            .Should().Contain("\"kind\":\"ca\"").And.Contain("\"outcome\":\"done\"")
            .And.Contain($"\"requested_unix\":{Now}").And.NotContain("\"requested_by\"");
    }
}
