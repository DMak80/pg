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
        var cursor = (IKeyCursor)new DirectoryCursor(bucketRoot, ownKey: null, prefix: string.Empty, after);

        var contents = new List<ListEntry>();
        var prefixes = new List<CommonPrefixEntry>();
        string? lastEmitted = null;
        string? lastPrefix = null;
        var limitReached = false; // эмитов ровно maxKeys — lookahead решает IsTruncated
        var truncated = false;
        while (true)
        {
            var key = cursor.PeekKey;
            if (key is null)
                break; // поддерево исчерпано — следующей эмиссии нет
            // Префикс — байтовым сравнением (спуск по сегментам префикса —
            // опциональная оптимизация, корректность — фильтром)
            if (!Utf8ByteOrder.StartsWith(key, prefix))
            {
                cursor.MoveNext();
                continue;
            }
            // Свёртка delimiter → CommonPrefixes (дедуп: ключи упорядочены,
            // одинаковые префиксы идут подряд)
            string? commonPrefix = null;
            if (delimiter is not null && key[prefix.Length..].Contains(delimiter, StringComparison.Ordinal))
            {
                var idx = key.IndexOf(delimiter, prefix.Length, StringComparison.Ordinal);
                commonPrefix = key[..(idx + delimiter.Length)];
            }
            // Маркер — против КАНДИДАТА эмиссии (CP после свёртки, не сырого
            // ключа): продолжение строго после возвращённого CP — иначе дети
            // свёрнутого префикса пере-эмитили бы тот же CP на следующей
            // странице (канон 02 §3). Единая семантика сравнения — Utf8ByteOrder.
            var candidate = commonPrefix ?? key;
            if (after is not null && Utf8ByteOrder.Compare(candidate, after) <= 0)
            {
                cursor.MoveNext();
                continue;
            }
            if (commonPrefix is not null)
            {
                if (commonPrefix == lastPrefix)
                {
                    cursor.MoveNext(); // дубликатный CP — не эмиссия
                    continue;
                }
                if (limitReached)
                {
                    truncated = true; // следующий РЕАЛЬНЫЙ эмит существует
                    break;
                }
                prefixes.Add(new CommonPrefixEntry(commonPrefix));
                lastPrefix = commonPrefix;
                lastEmitted = commonPrefix;
                if (contents.Count + prefixes.Count >= maxKeys)
                    limitReached = true;
                cursor.MoveNext();
                continue;
            }
            if (limitReached)
            {
                truncated = true; // эмитов ровно maxKeys и есть следующий объект
                break;
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
                limitReached = true;
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
//  1) собственный ключ каталога — если в нём есть xl.meta (наличие кэшируется
//     при загрузке): для каталога-сегмента это P без завершающего «/», для
//     маркерного (имя «…__XLDIR__») — сам P; собственный ключ минимален в
//     поддереве (любой другой ключ поддерева длиннее с тем же началом);
//  2) дети — ленивые DirectoryCursor; живые сливаются кучей по ТЕКУЩЕМУ PeekKey
//     (Utf8ByteOrder, по ПОЛНЫМ ключам — не по именам: ребёнок «!z» даёт ключи
//     P+"!z…" раньше ребёнка «m», байт '!'(0x21) < '/'(0x2F)); O(log w) на шаг;
//  3) subtree-skip по маркеру: дети уровня отсортированы по декодированным
//     именам; ребёнок C пропускается целиком, ТОЛЬКО если следующий брат R с
//     лениво вычисленным minKey(R) ≤ after И имя C — НЕ байтовый префикс имени
//     R (иначе ключи вида «m0» после after=«m!z» терялись бы — инвариант
//     ревью M-2). Emit-фильтр Walk остаётся страховкой корректности.
internal sealed class DirectoryCursor : IKeyCursor
{
    // Куча детей по текущему PeekKey (строки-ключи уникальны).
    private static readonly IComparer<string> KeyComparer =
        Comparer<string>.Create(Utf8ByteOrder.Compare);

    private readonly string _dir;
    private readonly string? _ownKey;     // собственный ключ каталога (null — не объект/корень)
    private readonly string _prefix;      // префикс всех ключей поддерева
    private readonly string? _after;      // маркер для subtree-skip (null — нет)
    private PriorityQueue<DirectoryCursor, string>? _merge;
    private bool _hasOwnMeta;             // кэш наличия xl.meta (один stat на загрузку)
    private bool _ownConsumed;
    private bool _loaded;

    public DirectoryCursor(string dir, string? ownKey, string prefix, string? after)
    {
        _dir = dir;
        _ownKey = ownKey;
        _prefix = prefix;
        _after = after;
    }

    public string? PeekKey
    {
        get
        {
            EnsureLoaded();
            string? best = !_ownConsumed && _hasOwnMeta ? _ownKey : null;
            if (best is null)
                return _merge is { Count: > 0 } && _merge.TryPeek(out _, out var childKey) ? childKey : null;
            if (_merge is { Count: > 0 } && _merge.TryPeek(out _, out var minChildKey)
                && Utf8ByteOrder.Compare(minChildKey, best) < 0)
                return minChildKey;
            return best;
        }
    }

    public void MoveNext()
    {
        EnsureLoaded();
        var best = PeekKey;
        if (best is null)
            return;
        if (!_ownConsumed && _hasOwnMeta && best == _ownKey)
        {
            _ownConsumed = true;
            return;
        }
        if (_merge is { Count: > 0 } && _merge.TryPeek(out var child, out var childKey) && childKey == best)
        {
            _merge.Dequeue();
            child.MoveNext();
            var next = child.PeekKey;
            if (next is not null)
                _merge.Enqueue(child, next);
        }
    }

    // Ленивая загрузка: кэш собственного xl.meta + дети по возрастанию
    // декодированных имен с subtree-skip по маркеру.
    private void EnsureLoaded()
    {
        if (_loaded)
            return;
        _loaded = true;
        _hasOwnMeta = File.Exists(Path.Combine(_dir, "xl.meta"));
        var children = new List<(string Name, DirectoryCursor Cursor)>();
        foreach (var sub in Directory.EnumerateDirectories(_dir))
        {
            var decoded = XlPathEncoder.DecodeSegments([Path.GetFileName(sub)]);
            // Маркерный сегмент даёт часть ключа с завершающим «/» (его
            // собственный ключ = префикс поддерева), обычный — префикс P+имя+"/"
            // и собственный ключ P+имя.
            if (decoded.EndsWith('/'))
                children.Add((decoded, new DirectoryCursor(sub, _prefix + decoded, _prefix + decoded, _after)));
            else
                children.Add((decoded, new DirectoryCursor(sub, _prefix + decoded, _prefix + decoded + "/", _after)));
        }
        children.Sort((left, right) => Utf8ByteOrder.Compare(left.Name, right.Name));
        var merge = new PriorityQueue<DirectoryCursor, string>(children.Count, KeyComparer);
        for (var i = 0; i < children.Count; i++)
        {
            var current = children[i];
            if (_after is not null && i + 1 < children.Count)
            {
                var next = children[i + 1];
                // Инвариант M-2: skip только при не-префиксном имени; имя C —
                // байтовый префикс имени R → ключи C могут быть > after
                if (!Utf8ByteOrder.StartsWith(next.Name, current.Name)
                    && next.Cursor.PeekKey is { } nextMin
                    && Utf8ByteOrder.Compare(nextMin, _after) <= 0)
                    continue; // всё поддерево current ≤ after — пропущено целиком
            }
            if (current.Cursor.PeekKey is { } minKey)
                merge.Enqueue(current.Cursor, minKey);
        }
        _merge = merge;
    }
}
