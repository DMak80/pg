using PgWorker.Backups;

namespace PgWorker.Backups.EtcdExport;

/// <summary>Чистый отбор ретенции S3-выгрузки etcd (t08, arch/19 §5): из list
/// префикса etcd/ выбирает ключи к batch-delete — объекты пар, старше N последних
/// (сортировка id по Ordinal = хронология), и одиночные .meta.json без .db (мусор).
/// Guard по построению: удаляется только старее оставляемых — ≥1 полная пара
/// остаётся всегда (при N ≥ 1, валидация старта).</summary>
public static class EtcdExportRetention
{
    public const string Prefix = "etcd/";

    /// <summary>Ключи к удалению. Пары задают id (файл .db); id вне последних
    /// retentionObjects — вся пара (db+meta); meta без пары — всегда.</summary>
    public static IReadOnlyList<string> Select(IReadOnlyList<S3ObjectInfo> objects, int retentionObjects)
    {
        var ids = new SortedSet<string>(StringComparer.Ordinal);
        var metaWithoutDb = new List<string>();
        foreach (var o in objects)
        {
            var name = o.Key.StartsWith(Prefix, StringComparison.Ordinal)
                ? o.Key[Prefix.Length..]
                : null;
            if (name is null)
                continue; // чужой ключ — не наш префикс (посторонний не трогаем)
            if (name.EndsWith(".meta.json", StringComparison.Ordinal))
            {
                var id = name[..^".meta.json".Length];
                if (!objects.Any(x => x.Key == $"{Prefix}{id}.db"))
                    metaWithoutDb.Add(o.Key); // сирота-meta — мусор
            }
            else if (name.EndsWith(".db", StringComparison.Ordinal))
                ids.Add(name[..^".db".Length]);
        }

        var stale = new List<string>(metaWithoutDb);
        foreach (var id in ids.Take(Math.Max(0, ids.Count - retentionObjects)))
        {
            stale.Add($"{Prefix}{id}.db");
            stale.Add($"{Prefix}{id}.meta.json");
        }

        return stale;
    }
}
