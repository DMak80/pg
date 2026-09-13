namespace PgWorker.Backups;

/// <summary>Результат контроля цепочки: дыра с границами (для error-статуса) и
/// последний валидный сегмент (для last_uploaded).</summary>
public sealed record ChainResult(bool IsContinuous, string? GapError, WalFileName? LastSegment);

/// <summary>Gap-детектор непрерывности WAL-цепочки от chain_start (arch/19 §3,
/// правила t03 spec §3.1): последовательность Next() без пропусков; TLI-переход
/// валиден при наличии загруженного `&lt;newTLI&gt;.history` и первом сегменте нового
/// TLI ∈ {последний сегмент старого TLI, Next(последний)}; `.partial` игнорируется.
/// Полный LSN-разбор history — t04 (verify, CheckRange).</summary>
public static class WalChain
{
    public static ChainResult Check(WalFileName chainStart, IEnumerable<string> objectNames)
        => Walk(chainStart, null, objectNames, null);

    /// <summary>Check с перезапуском от нового TLI (t05 AC4): promote восстановленного
    /// шарда открывает новый timeline, а сегменты старого TLI легитимно обрезаны
    /// (restore откатил LSN назад — старый хвост не дописывается). Дыра на
    /// TLI-границе при таком профиле — не деградация: если от первого сегмента
    /// максимального TLI цепь непрерывна, она непрерывна. Настоящая дыра
    /// (внутри одного TLI или в самом новом TLI) детектируется как раньше.</summary>
    public static ChainResult CheckWithRestart(WalFileName chainStart, IEnumerable<string> objectNames)
    {
        var objects = objectNames as IReadOnlyList<string> ?? objectNames.ToList();
        var first = Walk(chainStart, null, objects, null);
        if (first.IsContinuous)
            return first;

        var segments = Collect(objects).Segments;
        var maxTli = segments.Count > 0 ? segments.Max(s => s.Tli) : chainStart.Tli;
        if (maxTli <= chainStart.Tli)
            return first; // другой истории нет — это настоящая дыра

        var restart = segments.First(s => s.Tli == maxTli);
        if ((restart.Tli, restart.Log, restart.Seg) == (chainStart.Tli, chainStart.Log, chainStart.Seg))
            return first; // уже стартовали с него
        var retried = Walk(restart, null, objects, null);
        return retried.IsContinuous ? retried : first;
    }

    /// <summary>Проверка диапазона [chainStart..end] (t04, arch/19 §3): end —
    /// последний сегмент набора full/&lt;id&gt;/pg_wal/ (точка бэкапа); сегмент end
    /// ОБЯЗАН присутствовать среди объектов wal/ (дублирование t02). TLI-переходы
    /// в диапазоне: при наличии содержимого history (TLI → текст) — строго по
    /// switchWALLSN; без содержимого — эвристика t03 (Check). Сегменты выше end
    /// игнорируются (дальше — домен wal-статуса t03).</summary>
    public static ChainResult CheckRange(
        WalFileName chainStart, WalFileName end, IEnumerable<string> objectNames,
        IReadOnlyDictionary<uint, string>? historyContents = null)
        => Walk(chainStart, end, objectNames, historyContents);

    // Общая проходка: Check — end=null (без закрытия диапазона), CheckRange — end
    // включительно (диапазон закрыт, когда цепочка дошла ровно до end).
    private static ChainResult Walk(
        WalFileName chainStart, WalFileName? end, IEnumerable<string> objectNames,
        IReadOnlyDictionary<uint, string>? historyContents)
    {
        var (segments, histories) = Collect(objectNames);
        WalFileName? last = null;
        var expected = chainStart;
        foreach (var segment in segments)
        {
            if (segment.Tli == expected.Tli)
            {
                if (segment.Log == expected.Log && segment.Seg == expected.Seg)
                {
                    // ожидаемый сегмент — цепочка продолжается
                    last = segment;
                    expected = segment.Next();
                    if (end is { } close && segment.Tli == close.Tli && Position(segment) == Position(close))
                        return new ChainResult(true, null, segment); // диапазон закрыт
                }
                else if (Position(segment) > Position(expected))
                {
                    // пропуск внутри TLI: границы дыры — ожидали/найдено
                    return new ChainResult(false,
                        $"дыра WAL-цепочки: ожидался {expected.Name}, найден {segment.Name}", last);
                }
                // сегмент меньше expected (дубли/хвост старого потока) — игнор
            }
            else if (segment.Tli > expected.Tli)
            {
                // переход: строго по содержимому history, иначе эвристика t03.
                // Проверку «segment == end» делаем ПОСЛЕ валидации перехода —
                // end может быть первым сегментом нового TLI.
                if (historyContents is not null
                    && historyContents.TryGetValue(segment.Tli, out var content)
                    && WalHistory.Parse(content) is { Count: > 0 } entries)
                {
                    var entry = entries[^1]; // последняя строка — сам TLI файла
                    if (entry.ParentTli != expected.Tli)
                        return new ChainResult(false,
                            $"TLI-переход {expected.Tli}→{segment.Tli}: history-файл {segment.Tli:x8}.history: " +
                            $"parent {entry.ParentTli} ≠ {expected.Tli}", last);
                    var switchPos = WalFileName.FromLsn(0, entry.SwitchLsn);
                    var inBounds = (switchPos.Log, switchPos.Seg) == (segment.Log, segment.Seg)
                        || (last is { } prev && (switchPos.Log, switchPos.Seg) == (prev.Log, prev.Seg));
                    if (!inBounds)
                        return new ChainResult(false,
                            $"TLI-переход {expected.Tli}→{segment.Tli}: switchWALLSN {entry.SwitchLsn} " +
                            $"вне границы сегментов {(last is { } strictPrev ? strictPrev.Name : "нет предыдущего")}/{segment.Name} " +
                            $"({segment.Tli:x8}.history)", last);
                }
                else
                {
                    // эвристика t03 (Check): history-объект + первый сегмент ∈ {last, Next(last)}
                    if (!histories.Contains(segment.Tli))
                        return new ChainResult(false,
                            $"TLI-переход {expected.Tli}→{segment.Tli} без {segment.Tli:x8}.history", last);
                    // сравнение ПОЗИЦИЙ (log/seg): TLI первого сегмента нового таймлайна
                    // отличен от прошлого — равенство по record-полям здесь ложно.
                    var lastOrNext = last is { } prev
                        ? (segment.Log, segment.Seg) == (prev.Log, prev.Seg)
                          || (segment.Log, segment.Seg) == (prev.Next().Log, prev.Next().Seg)
                        : false;
                    if (!lastOrNext)
                        return new ChainResult(false,
                            $"TLI-переход {expected.Tli}→{segment.Tli}: первый сегмент {segment.Name} " +
                            $"не совпадает с точкой переключения ({(last is { } prev2 ? prev2.Name : "нет предыдущего")})", last);
                }

                last = segment;
                expected = segment.Next();
                if (end is { } close2 && segment.Tli == close2.Tli && Position(segment) == Position(close2))
                    return new ChainResult(true, null, segment); // диапазон закрыт
            }
            // сегмент старого TLI после перехода — вне цепочки (сортировка даёт их
            // раньше; сюда попадают только дубли ниже expected) — игнор
        }

        if (end is not { } reached)
            return new ChainResult(true, null, last);

        // список кончился, end не достигнут: цепочка шла сплошно, но сегмент набора
        // не продублирован в wal/ (дублирование t02, spec §3.4)
        return new ChainResult(false,
            $"сегмент {reached.Name} из набора бэкапа отсутствует в wal/ (ожидался после " +
            $"{(last is { } l2 ? l2.Name : chainStart.Name)})", last);
    }

    // Разбор объектов: сегменты (сортировка имени == сортировка (Tli,Log,Seg) —
    // фиксированная ширина hex) + множество загруженных history-TLI.
    private static (List<WalFileName> Segments, HashSet<uint> Histories) Collect(IEnumerable<string> objectNames)
    {
        var segments = new List<WalFileName>();
        var histories = new HashSet<uint>();
        foreach (var name in objectNames)
        {
            if (WalFileName.IsPartial(name))
                continue; // незакрытый сегмент — не грузится агентом и не входит в цепочку
            if (WalFileName.TryParseHistory(name) is { } tli)
            {
                histories.Add(tli);
                continue;
            }
            if (WalFileName.TryParse(name) is { } segment)
                segments.Add(segment);
        }

        segments.Sort((a, b) => a.Tli != b.Tli ? a.Tli.CompareTo(b.Tli)
            : a.Log != b.Log ? a.Log.CompareTo(b.Log) : a.Seg.CompareTo(b.Seg));
        return (segments, histories);
    }

    // Позиция сегмента внутри TLI (сравнение/сортировка — как в Check).
    private static long Position(WalFileName s) => (long)(s.Log * WalFileName.SegsPerLog + s.Seg);
}
