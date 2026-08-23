using PgWorker.Core.Model;

namespace PgWorker.Core.Planning;

/// <summary>Назначение эвакуации: бакет переезжает с мёртвого шарда на живой.</summary>
public sealed record EvacuationAssignment(int BucketId, string FromShard, string ToShard);

/// <summary>
/// Планировщик аварийной эвакуации (spec §6.4 D, Д6): живые шарды получают
/// бакеты умершего сбалансированно — round-robin по возрастанию id, целевые
/// шарды в порядке имени (детерминизм). Guard'ы:
///  - любой бакет кластера в статусе SYNCING/FROZEN/ABORTING → Result.Failed
///    (незавершённый переезд блокирует эвакуацию — его подписка/схема могут
///    быть связаны с умершим шардом);
///  - живых шардов нет → Result.Failed;
///  - бакет без owner → пропускается (дыра карты — вне ответственности эвакуатора).
/// </summary>
public static class EvacuationPlanner
{
    public static Result<IReadOnlyList<EvacuationAssignment>> Plan(
        IReadOnlyList<BucketRoute> routing,
        string deadShard,
        IReadOnlyList<string> aliveShards)
    {
        // Guard: незавершённый переезд — сначала разбор оператором (abort/finalize).
        var moving = routing.FirstOrDefault(r => r.Status is not null);
        if (moving is not null)
            return Result<IReadOnlyList<EvacuationAssignment>>.Failed(
                new InvalidOperationException(
                    $"EvacuationPlanner: бакет {moving.Id} в статусе {moving.Status} — " +
                    "незавершённый переезд, эвакуация заблокирована"));

        // Guard: эвакуировать некуда.
        if (aliveShards.Count == 0)
            return Result<IReadOnlyList<EvacuationAssignment>>.Failed(
                new InvalidOperationException(
                    $"EvacuationPlanner: нет живых шардов для эвакуации {deadShard}"));

        // Round-robin по живым шардам (по имени) для бакетов dead-шарда по возрастанию id.
        var targets = aliveShards.OrderBy(s => s).ToList();
        var assignments = new List<EvacuationAssignment>();
        var cursor = 0;
        foreach (var route in routing.Where(r => r.Owner == deadShard).OrderBy(r => r.Id))
        {
            assignments.Add(new EvacuationAssignment(route.Id, deadShard, targets[cursor % targets.Count]));
            cursor++;
        }

        return Result<IReadOnlyList<EvacuationAssignment>>.Success(assignments);
    }
}
