using EtcdWorker.Core;
using Shared.Etcd.Client;

namespace EtcdWorker.UnitTests.Core;

// Юнит-тесты решений по членству: выбор кандидата на удаление
// (PickRemoveCandidate), кворум-предохранитель (QuorumRemoveGuard)
// и детект мёртвой замены (DetectDeadReplacement).
public class MembershipDecisionTests
{
    // Фиксированное «сейчас» для детерминированных проверок порогов.
    private const long NowUnix = 1_000_000;

    // Хелпер: managed-член с номером K и id etcd; name=null = unstarted-член.
    private static ManagedMember Member(int k, ulong id, string? name) =>
        new(k, new EtcdMember(id, name, [$"http://m{id}:2380"], [$"http://m{id}:2379"]));

    // Хелпер: факт мёртвой замены для члена.
    private static ManagedDeadFact DeadFact(ManagedMember member, long? ProblemSinceUnix, bool EnvironmentMissing) =>
        new(member, ProblemSinceUnix, EnvironmentMissing);

    // --- PickRemoveCandidate ---

    [Fact]
    public void PickRemoveCandidate_EmptyManaged_ReturnsNull()
    {
        // Arrange — управляемых членов нет
        var managed = Array.Empty<ManagedMember>();

        // Act
        var candidate = MembershipDecisions.PickRemoveCandidate(managed, leaderId: null);

        // Assert — кандидата нет
        candidate.Should().BeNull();
    }

    [Fact]
    public void PickRemoveCandidate_SingleMemberIsLeader_ReturnsNull()
    {
        // Arrange — единственный управляемый член и есть лидер
        var leader = Member(1, 42, "m42");

        // Act
        var candidate = MembershipDecisions.PickRemoveCandidate([leader], leaderId: 42);

        // Assert — удалять некого: все управляемые — лидер
        candidate.Should().BeNull();
    }

    [Fact]
    public void PickRemoveCandidate_LeaderIsK1_ReturnsK3()
    {
        // Arrange — k=1 лидер, k=3 обычный член
        var k1 = Member(1, 42, "m42");
        var k3 = Member(3, 44, "m44");

        // Act
        var candidate = MembershipDecisions.PickRemoveCandidate([k1, k3], leaderId: 42);

        // Assert — не-лидер в контуре один
        candidate.Should().BeSameAs(k3);
    }

    [Fact]
    public void PickRemoveCandidate_NoLeader_ReturnsOldestK()
    {
        // Arrange — лидер не известен (null)
        var k1 = Member(1, 42, "m42");
        var k3 = Member(3, 44, "m44");

        // Act
        var candidate = MembershipDecisions.PickRemoveCandidate([k3, k1], leaderId: null);

        // Assert — без лидера берём старейший (минимальный) K
        candidate.Should().BeSameAs(k1);
    }

    // --- QuorumRemoveGuard ---

    [Fact]
    public void QuorumRemoveGuard_ThreeMembersAfterRemove_Allowed()
    {
        // Act + Assert — 3 члена после удаления: кворум сохраняется
        MembershipDecisions.QuorumRemoveGuard(totalMembersAfterRemove: 3).Should().BeTrue();
    }

    [Fact]
    public void QuorumRemoveGuard_TwoMembersAfterRemove_Denied()
    {
        // Act + Assert — 2 члена после удаления: кворума нет, удаление запрещено
        MembershipDecisions.QuorumRemoveGuard(totalMembersAfterRemove: 2).Should().BeFalse();
    }

    // --- DetectDeadReplacement ---

    [Fact]
    public void DetectDeadReplacement_ProblemYoungerThanThreshold_ReturnsNull()
    {
        // Arrange — проблема 299 с назад при пороге 300 с: порог не истёк
        var k1 = Member(1, 42, "m42");
        var facts = new[] { DeadFact(k1, ProblemSinceUnix: NowUnix - 299, EnvironmentMissing: true) };

        // Act
        var candidate = MembershipDecisions.DetectDeadReplacement(facts, nodeDeadSec: 300, leaderId: null, nowUnix: NowUnix);

        // Assert — кандидат не выбирается
        candidate.Should().BeNull();
    }

    [Fact]
    public void DetectDeadReplacement_UnstartedMemberYoungerThanThreshold_ReturnsNull()
    {
        // Arrange — unstarted-член (Name=null) с проблемой 299 с назад при пороге 300 с
        var k1 = Member(1, 42, name: null);
        var facts = new[] { DeadFact(k1, ProblemSinceUnix: NowUnix - 299, EnvironmentMissing: true) };

        // Act
        var candidate = MembershipDecisions.DetectDeadReplacement(facts, nodeDeadSec: 300, leaderId: null, nowUnix: NowUnix);

        // Assert — кейс порога для unstarted-члена идентичен обычному
        candidate.Should().BeNull();
    }

    [Fact]
    public void DetectDeadReplacement_ProblemAtThresholdAndEnvironmentMissing_ReturnsCandidate()
    {
        // Arrange — проблема ровно 300 с назад (порог истёк) и окружение ноды отсутствует
        var k1 = Member(1, 42, "m42");
        var facts = new[] { DeadFact(k1, ProblemSinceUnix: NowUnix - 300, EnvironmentMissing: true) };

        // Act
        var candidate = MembershipDecisions.DetectDeadReplacement(facts, nodeDeadSec: 300, leaderId: null, nowUnix: NowUnix);

        // Assert — член признаётся мёртвой заменой
        candidate.Should().BeSameAs(k1);
    }

    [Fact]
    public void DetectDeadReplacement_NoProblemSince_ReturnsNull()
    {
        // Arrange — момент возникновения проблемы неизвестен
        var k1 = Member(1, 42, "m42");
        var facts = new[] { DeadFact(k1, ProblemSinceUnix: null, EnvironmentMissing: true) };

        // Act
        var candidate = MembershipDecisions.DetectDeadReplacement(facts, nodeDeadSec: 300, leaderId: null, nowUnix: NowUnix);

        // Assert — без фиксации момента проблемы замена не детектится
        candidate.Should().BeNull();
    }

    [Fact]
    public void DetectDeadReplacement_EnvironmentPresent_ReturnsNull()
    {
        // Arrange — порог истёк, но окружение на месте (supervisor восстановил ноду)
        var k1 = Member(1, 42, "m42");
        var facts = new[] { DeadFact(k1, ProblemSinceUnix: NowUnix - 300, EnvironmentMissing: false) };

        // Act
        var candidate = MembershipDecisions.DetectDeadReplacement(facts, nodeDeadSec: 300, leaderId: null, nowUnix: NowUnix);

        // Assert — восстановленная нода не кандидат
        candidate.Should().BeNull();
    }

    [Fact]
    public void DetectDeadReplacement_CandidateIsLeader_ReturnsNull()
    {
        // Arrange — член с истёкшим порогом, но он лидер контура
        var k1 = Member(1, 42, "m42");
        var facts = new[] { DeadFact(k1, ProblemSinceUnix: NowUnix - 300, EnvironmentMissing: true) };

        // Act
        var candidate = MembershipDecisions.DetectDeadReplacement(facts, nodeDeadSec: 300, leaderId: 42, nowUnix: NowUnix);

        // Assert — лидера не заменяем
        candidate.Should().BeNull();
    }

    [Fact]
    public void DetectDeadReplacement_MultipleCandidates_ReturnsMinimalK()
    {
        // Arrange — два кандидата (k=5 и k=2), лидер — внешний член (id 99)
        var k5 = Member(5, 45, "m45");
        var k2 = Member(2, 43, "m43");
        var facts = new[]
        {
            DeadFact(k5, ProblemSinceUnix: NowUnix - 300, EnvironmentMissing: true),
            DeadFact(k2, ProblemSinceUnix: NowUnix - 400, EnvironmentMissing: true),
        };

        // Act
        var candidate = MembershipDecisions.DetectDeadReplacement(facts, nodeDeadSec: 300, leaderId: 99, nowUnix: NowUnix);

        // Assert — из кандидатов берём минимальный K
        candidate.Should().BeSameAs(k2);
    }
}
