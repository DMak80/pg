using PgWorker.Core.Model;

namespace PgWorker.App.Loops;

/// <summary>Кандидаты scale-прохода Active-ветки (t06 spec §5.1): чистая функция над снапшотом.</summary>
public sealed record ShardScaleCandidates(IReadOnlyList<string> Remove, IReadOnlyList<string> Add);

/// <summary>
/// Детекция шардов для Add/RemoveShardProcess: remove — маркер
/// shards/&lt;X&gt;/state=TO_REMOVE; add — declared-ноды (nodes.Count &gt; 0) без dsn.
/// Шард может быть в обоих списках (помечен и не поднят): remove-проход идёт
/// первым и демонтирует его (Д5), AddShardProcess дополнительно guard'ит ToRemove.
/// </summary>
public static class ShardScaleClassifier
{
    public static ShardScaleCandidates Detect(ClusterSnapshot snap)
    {
        var remove = snap.Shards.Where(s => s.ToRemove).Select(s => s.Name).ToList();
        var add = snap.Shards.Where(s => s.Nodes.Count > 0 && s.Dsn is null).Select(s => s.Name).ToList();
        return new ShardScaleCandidates(remove, add);
    }
}
