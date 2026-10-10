using System.Buffers.Text;
using System.Text;
using Microsoft.Extensions.Logging;

namespace OwnS3.Storage;

// Обход bucket-дерева ленивым k-way merge «следующих ключей поддеревьев»
// (спека §4.5). Порядок эмитов — строгий байтовый порядок полных ключей
// (Utf8ByteOrder, P9). Битый xl.meta — warning и пропуск ключа (листинг не
// роняет один битый объект).
internal sealed class ListWalker(XlVolume volume, ILogger? logger = null)
{
    public ListPage Walk(string bucket, ListQuery query)
    {
        var maxKeys = query.MaxKeys ?? 1000;
        if (maxKeys == 0)
            return new ListPage([], [], IsTruncated: false, null, null, KeyCount: 0);

        // Маркер: V2 — continuation-token (декод) ?? start-after (токен выигрывает);
        // V1/Versions — marker (хендлер t36 кладёт туда key-marker)
        string? after = null;
        if (query.Variant == ListVariant.V2)
        {
            if (query.ContinuationToken is not null)
            {
                try
                {
                    after = Encoding.UTF8.GetString(Base64Url.DecodeFromChars(query.ContinuationToken));
                }
                catch (FormatException)
                {
                    throw new XlInvalidArgumentException("Invalid continuation token");
                }
            }
            else if (query.StartAfter is not null)
                after = query.StartAfter;
        }
        else
            after = query.Marker;

        var prefix = query.Prefix ?? string.Empty;
        var delimiter = query.Delimiter;
        var bucketRoot = Path.Combine(volume.Root, bucket);
        var cursor = (IKeyCursor)new DirectoryCursor(bucketRoot, ownKey: null, prefix: string.Empty);

        var contents = new List<ListEntry>();
        var prefixes = new List<CommonPrefixEntry>();
        string? lastEmitted = null;
        string? lastPrefix = null;
        var truncated = false;
        while (true)
        {
            var key = cursor.PeekKey;
            if (key is null)
                break; // поддерево исчерпано
            // Строго после маркера (единая семантика сравнения — Utf8ByteOrder)
            if (after is not null && Utf8ByteOrder.Compare(key, after) <= 0)
            {
                cursor.MoveNext();
                continue;
            }
            // Префикс — байтовым сравнением (спуск по сегментам префикса —
            // опциональная оптимизация, корректность — фильтром)
            if (!Utf8ByteOrder.StartsWith(key, prefix))
            {
                cursor.MoveNext();
                continue;
            }
            // Свёртка delimiter → CommonPrefixes (дедуп: ключи упорядочены,
            // одинаковые префиксы идут подряд)
            if (delimiter is not null && key[prefix.Length..].Contains(delimiter, StringComparison.Ordinal))
            {
                var idx = key.IndexOf(delimiter, prefix.Length, StringComparison.Ordinal);
                var commonPrefix = key[..(idx + delimiter.Length)];
                if (commonPrefix != lastPrefix)
                {
                    prefixes.Add(new CommonPrefixEntry(commonPrefix));
                    lastPrefix = commonPrefix;
                    lastEmitted = commonPrefix;
                    if (contents.Count + prefixes.Count >= maxKeys)
                    {
                        truncated = true;
                        break;
                    }
                }
                cursor.MoveNext();
                continue;
            }
            // Эмит объекта: чтение xl.meta (битый — warning + пропуск)
            XlMetaRecord meta;
            try
            {
                meta = XlMetaFile.Read(ObjectDirOf(bucketRoot, key), out _);
            }
            catch (XlIntegrityException ex)
            {
                logger?.LogWarning(ex, "листинг: битый xl.meta ключа {Key} — ключ пропущен", key);
                cursor.MoveNext();
                continue;
            }
            catch (FileNotFoundException)
            {
                cursor.MoveNext(); // конкурентно удалён — не роняет обход
                continue;
            }
            contents.Add(new ListEntry(key, '"' + meta.ETag + '"', meta.Size, meta.ModTime));
            lastEmitted = key;
            if (contents.Count + prefixes.Count >= maxKeys)
            {
                truncated = true;
                break;
            }
            cursor.MoveNext();
        }

        // NextMarker (v1) — только при delimiter (без него клиент продолжает по
        // последнему Contents/Key); V2 — base64url(lastEmitted). Variant на обход
        // не влияет — обёртки добавляет App.
        string? nextMarker = null;
        string? nextToken = null;
        if (truncated && lastEmitted is not null)
        {
            if (query.Variant == ListVariant.V2)
                nextToken = Base64Url.EncodeToString(Encoding.UTF8.GetBytes(lastEmitted));
            else if (delimiter is not null)
                nextMarker = lastEmitted;
        }
        return new ListPage(contents, prefixes, truncated, nextMarker, nextToken,
            contents.Count + prefixes.Count);
    }

    // Каталог объекта по декодированному ключу.
    private static string ObjectDirOf(string bucketRoot, string key) =>
        Path.Combine([bucketRoot, .. XlPathEncoder.EncodePath(key).Split('/')]);
}

// Курсор поддерева: минимальный ещё не эмитнутый ключ (null — исчерпан).
internal interface IKeyCursor
{
    string? PeekKey { get; }
    void MoveNext();
}

// Курсор каталога с ключом-префиксом P поддерева ("" для корня бакета):
//  1) собственный ключ каталога — если в нём есть xl.meta: для каталога-сегмента
//     это P без завершающего «/», для маркерного (имя «…__XLDIR__») — сам P
//     (завершающий «/» — часть ключа); собственный ключ минимален в поддереве
//     (любой другой ключ поддерева длиннее с тем же началом);
//  2) дети — ленивые DirectoryCursor; сливаются k-way merge по PeekKey
//     (Utf8ByteOrder) — по ПОЛНЫМ ключам, не по именам: ребёнок «!z» даёт
//     ключи P+"!z…" — раньше ребёнка «m» (байт '!'(0x21) < '/'(0x2F)).
internal sealed class DirectoryCursor : IKeyCursor
{
    private readonly string _dir;
    private readonly string _prefix;      // префикс всех ключей поддерева
    private readonly string? _ownKey;     // собственный ключ каталога (маркерованный или без «/»)
    private List<IKeyCursor>? _children;
    private bool _ownConsumed;

    public DirectoryCursor(string dir, string? ownKey, string prefix)
    {
        _dir = dir;
        _ownKey = ownKey;
        _prefix = prefix;
    }

    public string? PeekKey
    {
        get
        {
            EnsureLoaded();
            string? best = _ownConsumed || _ownKey is null || !File.Exists(Path.Combine(_dir, "xl.meta"))
                ? null
                : _ownKey;
            if (_children is null)
                return best;
            foreach (var child in _children)
            {
                var key = child.PeekKey;
                if (key is null)
                    continue;
                if (best is null || Utf8ByteOrder.Compare(key, best) < 0)
                    best = key;
            }
            return best;
        }
    }

    public void MoveNext()
    {
        var best = PeekKey;
        if (best is null)
            return;
        if (!_ownConsumed && best == _ownKey)
        {
            _ownConsumed = true;
            return;
        }
        if (_children is not null)
            foreach (var child in _children)
                if (child.PeekKey == best)
                {
                    child.MoveNext();
                    return;
                }
    }

    // Ленивая загрузка детей: имена декодируются; маркер каталога даёт часть
    // ключа с завершающим «/» (его собственный ключ = префикс поддерева),
    // обычный сегмент — префикс P+имя+"/" и собственный ключ P+имя.
    private void EnsureLoaded()
    {
        if (_children is not null)
            return;
        var children = new List<IKeyCursor>();
        foreach (var sub in Directory.EnumerateDirectories(_dir))
        {
            var decoded = XlPathEncoder.DecodeSegments([Path.GetFileName(sub)]);
            if (decoded.EndsWith('/'))
                children.Add(new DirectoryCursor(sub, _prefix + decoded, _prefix + decoded));
            else
                children.Add(new DirectoryCursor(sub, _prefix + decoded, _prefix + decoded + "/"));
        }
        _children = children;
    }
}
