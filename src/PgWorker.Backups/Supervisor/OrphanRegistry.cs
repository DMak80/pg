using System.Text.Json;
using System.Text.Json.Serialization;

namespace PgWorker.Backups.Supervisor;

/// <summary>Состояние записи реестра сирот (arch/19 §4, t07): OBSERVED —
/// наблюдается, ждёт TTL; DELETING — идёт batch-delete префикса (доводка).</summary>
public enum OrphanState
{
    Observed,
    Deleting,
}

/// <summary>Одна запись реестра сирот: осиротевший S3-префикс «&lt;C&gt;/&lt;X&gt;»
/// (kind: shard — кластер жив, шарда нет; cluster — кластер исчез), суммарный
/// размер, время первого наблюдения (TTL отсчитывается от него — переносится
/// merge'ем), состояние, признак валидного полного (DR-hold, reliability t04:
/// сирота с валидным полным автоматикой не удаляется никогда).</summary>
public sealed record OrphanEntry(
    string Prefix, string Kind, long SizeBytes, long FirstSeenUnix, OrphanState State,
    bool HasValidFull = false);

/// <summary>Чистые функции реестра сирот (t07, arch/19 §4): группировка объектов
/// S3 по префиксам нашей формы (с детектом валидных полных — DR-hold, reliability
/// t04), merge с переносом first_seen/has_valid_full и гвардом воскресения
/// владельца, отбор TTL-кандидата с hold-множеством, JSON 1:1 с каноном, ключи и
/// JSON per-префиксных hold/заявок удаления. Реестр пишет ТОЛЬКО глобальный лидер
/// /pgworker/leader; hold/заявки пишет только API воркера атомарными put/del.
/// Само значение реестра — вложенный record Registry: static class и record не
/// могут носить одно имя в одном namespace (сигнатуры плана свёрнуты в
/// OrphanRegistry.Registry).</summary>
public static class OrphanRegistry
{
    /// <summary>Глобальный ключ реестра — вне per-cluster префиксов
    /// (D2-чистки deprovisioning не касается), как /pgworker/backups/storage.</summary>
    public const string Key = "/pgworker/backups/orphans";

    /// <summary>Префикс per-префиксных hold-ключей сирот (arch/19 §4, t04):
    /// put/del делает ТОЛЬКО API воркера; sweeper читает при TTL-отборе и гасит
    /// ключи несирот (санитар прохода).</summary>
    public const string HoldsPrefix = "/pgworker/backups/orphan-holds/";

    /// <summary>Префикс per-префиксных заявок явного удаления сирот (arch/19 §4,
    /// t04): put делает ТОЛЬКО API воркера; sweeper исполняет заявку ближайшим
    /// проходом (обходит hold и автоправило) и гасит ключ по исполнении/погашении.</summary>
    public const string DeletesPrefix = "/pgworker/backups/orphan-deletes/";

    /// <summary>Реестр осиротевших S3-префиксов установки — значение глобального
    /// ключа /pgworker/backups/orphans (формат arch/19 §4). Пишет ТОЛЬКО
    /// глобальный лидер-проход (BackupOrphanSweeper); панель читает
    /// (алерт backup-orphan).</summary>
    public sealed record Registry(IReadOnlyList<OrphanEntry> Orphans, long UpdatedUnix);

    /// <summary>Результат группировки list bucket: суммы размеров по префиксам
    /// «&lt;C&gt;/&lt;X&gt;/» нашей формы + множество префиксов с валидным полным
    /// (объект full/&lt;id&gt;/backup_manifest — критерий DR-выбора, arch/19 §3.5).</summary>
    public sealed record PrefixGrouping(
        IReadOnlyDictionary<string, long> Sizes,
        IReadOnlySet<string> FullPrefixes);

    // Имена кластеров/шардов нашей формы (arch/14): посторонние корневые объекты
    // bucket'а в реестр не попадают (шум списка сирот, spec §6).
    private static readonly System.Text.RegularExpressions.Regex NameRegex =
        new("^[a-z][a-z0-9_]{0,62}$", System.Text.RegularExpressions.RegexOptions.Compiled);

    private static readonly JsonSerializerOptions Json = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false,
        Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    private sealed record RegistryPayload(
        [property: JsonPropertyName("orphans")] IReadOnlyList<OrphanPayload>? Orphans,
        [property: JsonPropertyName("updated_unix")] long? UpdatedUnix);

    private sealed record OrphanPayload(
        [property: JsonPropertyName("prefix")] string? Prefix,
        [property: JsonPropertyName("kind")] string? Kind,
        [property: JsonPropertyName("size_bytes")] long? SizeBytes,
        [property: JsonPropertyName("first_seen_unix")] long? FirstSeenUnix,
        [property: JsonPropertyName("state")] string? State,
        // has_valid_full — DR-hold (reliability t04); nullable: старый ключ без
        // поля парсится (AC9) как false, писать всегда как bool.
        [property: JsonPropertyName("has_valid_full")] bool? HasValidFull);

    private sealed record HoldPayload(
        [property: JsonPropertyName("set_unix")] long SetUnix,
        [property: JsonPropertyName("set_by")] string SetBy);

    private sealed record DeletePayload(
        [property: JsonPropertyName("requested_unix")] long RequestedUnix,
        [property: JsonPropertyName("requested_by")] string RequestedBy);

    /// <summary>Группировка list-объектов (один проход): суммы размеров по
    /// префиксам «&lt;C&gt;/&lt;X&gt;/» нашей формы (имена валидны по regex;
    /// посторонние корневые объекты вне реестра) + детект валидных полных —
    /// ключ full/&lt;id&gt;/backup_manifest (5 сегментов, 3-й full, последний
    /// backup_manifest) — критерий DR-выбора, из уже снятого list, ноль лишних
    /// запросов к S3 (arch/19 §4, DR-hold t04).</summary>
    public static PrefixGrouping GroupPrefixes(IReadOnlyList<S3ObjectInfo> objects)
    {
        var sizes = new Dictionary<string, long>(StringComparer.Ordinal);
        var fulls = new HashSet<string>(StringComparer.Ordinal);
        foreach (var o in objects)
        {
            // ключ «<C>/<X>/…»: минимум 3 сегмента («<C>/<X>/<что-то>»).
            var parts = o.Key.Split('/');
            if (parts.Length < 3
                || !NameRegex.IsMatch(parts[0]) || !NameRegex.IsMatch(parts[1]))
                continue;
            var prefix = $"{parts[0]}/{parts[1]}";
            sizes[prefix] = sizes.TryGetValue(prefix, out var s) ? s + o.SizeBytes : o.SizeBytes;
            if (parts.Length == 5 && parts[2] == "full"
                && parts[4] == "backup_manifest")
                fulls.Add(prefix);
        }

        return new PrefixGrouping(sizes, fulls);
    }

    /// <summary>Merge наблюдения с текущим реестром: новый префикс → OBSERVED
    /// (first_seen = now); существующий → перенос first_seen (TTL от первого
    /// наблюдения) + обновление size/kind + has_valid_full из свежего наблюдения
    /// (ненаблюдаемый сохраняет прежнее — защита переживает transient list);
    /// воскресший (владелец появился в etcd) → запись удаляется, идущая
    /// DELETING-доводка отменяется.</summary>
    public static Registry Merge(
        Registry? current,
        IReadOnlyDictionary<string, long> observedSizes,
        IReadOnlySet<string> observedFulls,
        IReadOnlySet<(string Cluster, string Shard)> liveShards,
        IReadOnlySet<string> liveClusters,
        long nowUnix)
    {
        var entries = new List<OrphanEntry>();
        var existing = current?.Orphans ?? [];

        // Существующие записи: живой владелец → запись гаснет (доводка отменяется);
        // наблюдаемый → перенос first_seen, обновление size/kind, пересчёт
        // has_valid_full; ненаблюдаемый сирота → сохраняется как есть (исчезновение
        // из list = transient list'а, has_valid_full переносится).
        foreach (var e in existing)
        {
            var (cluster, shard) = SplitPrefix(e.Prefix);
            if (cluster is null || shard is null)
                continue; // битый префикс не реанимируем — следующий проход пересоберёт
            if (liveClusters.Contains(cluster) && liveShards.Contains((cluster, shard)))
                continue; // владелец жив — не сирота

            if (observedSizes.TryGetValue(e.Prefix, out var size))
            {
                entries.Add(e with
                {
                    SizeBytes = size,
                    Kind = liveClusters.Contains(cluster) ? "shard" : "cluster",
                    HasValidFull = observedFulls.Contains(e.Prefix),
                });
            }
            else
            {
                entries.Add(e);
            }
        }

        // Новые наблюдения — OBSERVED с first_seen = now.
        var known = entries.Select(e => e.Prefix).ToHashSet(StringComparer.Ordinal);
        foreach (var (prefix, size) in observedSizes)
        {
            if (known.Contains(prefix))
                continue;
            var (cluster, shard) = SplitPrefix(prefix);
            if (cluster is null || shard is null)
                continue;
            if (liveClusters.Contains(cluster) && liveShards.Contains((cluster, shard)))
                continue; // владелец жив — не сирота
            entries.Add(new OrphanEntry(
                prefix, liveClusters.Contains(cluster) ? "shard" : "cluster", size, nowUnix,
                OrphanState.Observed, observedFulls.Contains(prefix)));
        }

        entries.Sort((a, b) => string.CompareOrdinal(a.Prefix, b.Prefix));
        return new Registry(entries, nowUnix);
    }

    /// <summary>Кандидат TTL-обработки за проход (тик короткий): первый DELETING
    /// (доводка — приоритет) либо первый OBSERVED с возрастом &gt; ttl, у которого
    /// НЕТ валидного полного (DR-hold: сирота с полным автоматикой не удаляется
    /// никогда, arch/19 §4) и НЕТ живого hold-ключа (heldPrefixes). ttl = 0 →
    /// только доводка DELETING, новых кандидатов нет (авто-удаление выключено);
    /// кандидатов нет → null.</summary>
    public static string? SelectTtlCandidate(
        Registry registry, long ttlSec, long nowUnix, IReadOnlySet<string> heldPrefixes)
    {
        var deleting = registry.Orphans.FirstOrDefault(e => e.State == OrphanState.Deleting);
        if (deleting is not null)
            return deleting.Prefix;

        if (ttlSec <= 0)
            return null;

        return registry.Orphans
            .FirstOrDefault(e => e.State == OrphanState.Observed
                                 && nowUnix - e.FirstSeenUnix > ttlSec
                                 && !e.HasValidFull
                                 && !heldPrefixes.Contains(e.Prefix))
            ?.Prefix;
    }

    /// <summary>Ключ hold-флага сироты (arch/19 §4, t04):
    /// /pgworker/backups/orphan-holds/&lt;C&gt;/&lt;X&gt;.</summary>
    public static string HoldKey(string prefix) => $"{HoldsPrefix}{prefix}";

    /// <summary>Ключ заявки явного удаления сироты (arch/19 §4, t04):
    /// /pgworker/backups/orphan-deletes/&lt;C&gt;/&lt;X&gt;.</summary>
    public static string DeleteKey(string prefix) => $"{DeletesPrefix}{prefix}";

    /// <summary>Значение hold-ключа — формат канона arch/19 §4 (строго,
    /// без лишних полей): {"set_unix":T,"set_by":"…"}.</summary>
    public static string HoldToJson(long setUnix, string setBy)
        => JsonSerializer.Serialize(new HoldPayload(setUnix, setBy), Json);

    /// <summary>Значение ключа заявки — формат канона arch/19 §4 (строго):
    /// {"requested_unix":T,"requested_by":"…"}.</summary>
    public static string DeleteToJson(long requestedUnix, string requestedBy)
        => JsonSerializer.Serialize(new DeletePayload(requestedUnix, requestedBy), Json);

    /// <summary>Сериализация в формат ключа arch/19 §4 (snake_case).</summary>
    public static string ToJson(Registry registry)
        => JsonSerializer.Serialize(new RegistryPayload(
            registry.Orphans
                .Select(e => new OrphanPayload(
                    e.Prefix, e.Kind, e.SizeBytes, e.FirstSeenUnix,
                    e.State == OrphanState.Deleting ? "DELETING" : "OBSERVED",
                    e.HasValidFull))
                .ToList(),
            registry.UpdatedUnix), Json);

    /// <summary>Разбор значения ключа; битый JSON/поля → null (следующий проход
    /// пересоберёт реестр заново — ключ перезапишется merged-наблюдением).
    /// Отсутствие has_valid_full (ключ старого кода) — валидно: false, ближайший
    /// проход пересчитает (AC9).</summary>
    public static Registry? Parse(string raw)
    {
        try
        {
            var payload = JsonSerializer.Deserialize<RegistryPayload>(raw, Json);
            if (payload?.Orphans is null || payload.UpdatedUnix is not { } updated)
                return null;
            var entries = new List<OrphanEntry>();
            foreach (var o in payload.Orphans)
            {
                if (o?.Prefix is not { Length: > 0 } prefix
                    || o.Kind is not { Length: > 0 } kind
                    || o.SizeBytes is not { } size
                    || o.FirstSeenUnix is not { } seen
                    || o.State is not { Length: > 0 } state)
                    return null;
                var parsed = state switch
                {
                    "DELETING" => OrphanState.Deleting,
                    "OBSERVED" => OrphanState.Observed,
                    _ => (OrphanState?)null, // неизвестный state — битый ключ
                };
                if (parsed is not { } orphanState)
                    return null;
                entries.Add(new OrphanEntry(
                    prefix, kind, size, seen, orphanState, o.HasValidFull ?? false));
            }

            return new Registry(entries, updated);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>Сравнение наблюдаемой части (без updated_unix): put ключа — только
    /// при изменении записей (безделье не пишет, образец WalStatusWriter);
    /// has_valid_full входит в сравнение (пересчёт — повод для put).</summary>
    public static bool SameOrphans(Registry a, Registry b)
        => a.Orphans.Count == b.Orphans.Count
           && a.Orphans
               .Zip(b.Orphans, (x, y) => (x, y))
               .All(p => string.CompareOrdinal(p.x.Prefix, p.y.Prefix) == 0
                         && p.x.Kind == p.y.Kind && p.x.SizeBytes == p.y.SizeBytes
                         && p.x.FirstSeenUnix == p.y.FirstSeenUnix
                         && p.x.State == p.y.State
                         && p.x.HasValidFull == p.y.HasValidFull);

    // «<C>/<X>» → компоненты; не-нашей-формы → null (в реестр не попадает).
    private static (string? Cluster, string? Shard) SplitPrefix(string prefix)
    {
        var parts = prefix.Split('/');
        if (parts.Length != 2 || !NameRegex.IsMatch(parts[0]) || !NameRegex.IsMatch(parts[1]))
            return (null, null);
        return (parts[0], parts[1]);
    }
}
