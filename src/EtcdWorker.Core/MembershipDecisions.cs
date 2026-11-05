using Shared.Etcd.Client;

namespace EtcdWorker.Core;

/// <summary>Факт наблюдения supervisor за managed-узлом: ProblemSinceUnix —
/// метка ПЕРВОГО наблюдения проблемы члена (unstarted ИЛИ health-молчание;
/// null — проблем нет); EnvironmentMissing — окружение не восстановлено
/// (контейнер отсутствует/машина недоступна/volume потерян).</summary>
public sealed record ManagedDeadFact(
    ManagedMember Member, long? ProblemSinceUnix, bool EnvironmentMissing);

/// <summary>Чистые решения доводки состава (arch/etcdworker/02-membership.md).</summary>
public static class MembershipDecisions
{
    /// <summary>Номер нового узла: максимальный существующий + 1 (имена не
    /// переиспользуются, сквозная нумерация); пусто → 1.</summary>
    public static int NextNumber(IReadOnlyList<int> existingNumbers)
        => existingNumbers.Count == 0 ? 1 : existingNumbers.Max() + 1;

    /// <summary>Кандидат вывода (arch/etcdworker/02-membership.md): старейший по номеру НЕ лидер;
    /// null — кандидата нет (пусто/все — лидер).</summary>
    public static ManagedMember? PickRemoveCandidate(
        IReadOnlyList<ManagedMember> managed, ulong? leaderId)
        => managed
            .Where(m => leaderId is not { } leader || m.Member.Id != leader)
            .OrderBy(m => m.K)
            .FirstOrDefault();

    /// <summary>Кворум-гвард remove: после вывода останется ≥ 3 членов.</summary>
    public static bool QuorumRemoveGuard(int totalMembersAfterRemove)
        => totalMembersAfterRemove >= 3;

    /// <summary>Детект авто-замены (решение 2, arch/etcdworker/02-membership.md): проблема члена
    /// (unstarted ИЛИ health-молчание) наблюдается ≥ NodeDeadSec И окружение не
    /// восстановлено; не лидер. Порог — для ОБОИХ случаев: unstarted-ветка тоже
    /// гейтится меткой первого наблюдения (ProblemSinceUnix) — немедленной
    /// замены нет; метку трекает supervisor (шаг 13).</summary>
    public static ManagedMember? DetectDeadReplacement(
        IReadOnlyList<ManagedDeadFact> facts, int nodeDeadSec, ulong? leaderId, long nowUnix)
        => facts
            .Where(f => f.ProblemSinceUnix is { } since
                        && nowUnix - since >= nodeDeadSec
                        && f.EnvironmentMissing
                        && (leaderId is not { } leader || f.Member.Member.Id != leader))
            .OrderBy(f => f.Member.K)
            .FirstOrDefault()?.Member;
}
