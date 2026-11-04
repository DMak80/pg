namespace PgWorker.Backups;

/// <summary>Результат контроля цепочки: дыра с границами (для error-статуса) и
/// последний валидный сегмент (для last_uploaded).</summary>
public sealed record ChainResult(bool IsContinuous, string? GapError, WalFileName? LastSegment);

/// <summary>Gap-детектор непрерывности WAL-цепочки от chain_start (arch/19 §3,
/// правила spec §3.1): последовательность Next() без пропусков; TLI-переход
/// валиден при наличии загруженного `&lt;newTLI&gt;.history` и первом сегменте нового
/// TLI ∈ {последний сегмент старого TLI, Next(последний)}; `.partial` игнорируется.
/// Полный LSN-разбор history — (verify, CheckRange).</summary>
public static class WalChain
{
    public static ChainResult Check(WalFileName chainStart, IEnumerable<string> objectNames)
        => Walk(chainStart, null, objectNames, null);

    /// <summary>Стартовая точка контроля цепочки с ratchet (arch/19 §3):
    /// кандидат — wal_start новейшего verify-OK полного
    /// (RetentionPlanner.LatestVerifiedWalStart); ratchet — точка НИКОГДА не
    /// понижается ниже записанной границы разрыва (кандидаты ниже игнорируются:
    /// их цепь может быть цела — дыра выше). Кандидата нет → записанная точка.
    /// Чистая функция.</summary>
    public static WalFileName? RatchetedStart(WalFileName? recorded, WalFileName? candidate)
        => candidate is { } start
           && (recorded is null
               || string.CompareOrdinal(start.Name, recorded.Value.Name) >= 0)
            ? start
            : recorded;

    /// <summary>Хвост потока (arch/19 §3 «Истина прогресса»).</summary>
    public sealed record WalTailFact(string SegmentName, string ObjectName);

    /// <summary>Хвост потока — максимальный валидный префикс (arch/19 §3, arch/
    /// backups/processes/b-wal-control.md §1): финальный объект цепочки
    /// (chainLastFinal — результат Check/CheckWithRestart) ЛИБО покрытие
    /// дельта-кусков ТЕКУЩЕГО номера жизни по сегментам (покрытие по именам:
    /// база start=0, затем жадное примыкание start ≤ coverage &lt; end), ЛИБО
    /// лучшая t14-версия (`WalPartialName`) — версии учитываются ТОЛЬКО у
    /// сегмента без единого куска номера chainLife (переходное окно t14→t15,
    /// решение №39). Куски чужих номеров не рассматриваются вовсе. Сравнение
    /// по (Tli, log·256+seg); при равной позиции финал авторитетен. Чистая
    /// функция; null — кандидатов нет.</summary>
    public static WalTailFact? TailOf(
        WalFileName? chainLastFinal, IEnumerable<string> objectNames, uint chainLife)
    {
        // Куски текущего номера по сегментам (имена нужны для LastModified хвоста).
        var currentBySegment = new Dictionary<WalFileName, List<(WalDeltaName.Piece Piece, string Name)>>();
        // Лучшие .partial-версии (дедуп версий: длиннейшая EndLsn) — legacy-окно.
        var legacyBySegment = new Dictionary<WalFileName, (ulong EndLsn, string Name)>();
        foreach (var name in objectNames)
        {
            if (WalDeltaName.TryParse(name) is { } piece)
            {
                if (piece.ChainLife != chainLife)
                    continue; // чужой номер жизни — не кандидат вовсе
                if (!currentBySegment.TryGetValue(piece.Segment, out var list))
                    currentBySegment[piece.Segment] = list = [];
                list.Add((piece, name));
                continue;
            }

            if (WalPartialName.TryParse(name) is { } legacy
                && (!legacyBySegment.TryGetValue(legacy.Segment, out var best)
                    || legacy.EndLsn > best.EndLsn))
                legacyBySegment[legacy.Segment] = (legacy.EndLsn, name);
        }

        var candidates = new List<(WalFileName Segment, string ObjectName, bool IsFinal)>();
        if (chainLastFinal is { } final)
            candidates.Add((final, final.Name, IsFinal: true));
        foreach (var (segment, pieces) in currentBySegment)
            if (PieceCoverage(pieces) is { } coverage)
                candidates.Add((segment, coverage, IsFinal: false));
        foreach (var (segment, legacy) in legacyBySegment)
            if (!currentBySegment.ContainsKey(segment))
                candidates.Add((segment, legacy.Name, IsFinal: false)); // t14-версия — пока нет кусков жизни
        if (candidates.Count == 0)
            return null;

        // Максимум по (Tli, позиция); равная позиция — финал авторитетен.
        return candidates
            .OrderByDescending(c => ((long)c.Segment.Tli,
                (long)c.Segment.Log * WalFileName.SegsPerLog + c.Segment.Seg))
            .ThenByDescending(c => c.IsFinal)
            .Select(c => new WalTailFact(c.Segment.Name, c.ObjectName))
            .First();
    }

    // Покрытие дельта-кусков сегмента по именам: база start=0 даёт начальное
    // покрытие (max end), затем куски start ≤ coverage < end расширяют его
    // итеративно (отсортированы по start). Имя — кусок, достигший покрытия.
    // null — базы нет (куски без [0..end) покрытия не начинают).
    private static string? PieceCoverage(
        List<(WalDeltaName.Piece Piece, string Name)> pieces)
    {
        var ordered = pieces
            .OrderBy(p => p.Piece.Start)
            .ThenBy(p => p.Name, StringComparer.Ordinal)
            .ToList();
        // База жизни — кусок от НАЧАЛА сегмента (координаты кусков абсолютные).
        var segmentStart = WalDeltaName.SegmentStartOf(ordered[0].Piece.Segment);
        var coverage = segmentStart;
        string? name = null;
        foreach (var (piece, pieceName) in ordered)
            if (piece.Start == segmentStart && piece.End > coverage)
            {
                coverage = piece.End;
                name = pieceName;
            }

        if (name is null)
            return null; // базы (start=0) нет — покрытие жизни не начато

        bool extended;
        do
        {
            extended = false;
            foreach (var (piece, pieceName) in ordered)
                if (piece.Start <= coverage && piece.End > coverage)
                {
                    coverage = piece.End;
                    name = pieceName;
                    extended = true;
                }
        }
        while (extended);

        return name;
    }

    /// <summary>Check с перезапуском от нового TLI (AC4): promote восстановленного
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

    /// <summary>Проверка диапазона [chainStart..end] (arch/19 §3): end —
    /// последний сегмент набора full/&lt;id&gt;/pg_wal/ (точка бэкапа); сегмент end
    /// ОБЯЗАН присутствовать среди объектов wal/ (дублирование). TLI-переходы
    /// в диапазоне: при наличии содержимого history (TLI → текст) — строго по
    /// switchWALLSN; без содержимого — эвристика (Check). Сегменты выше end
    /// игнорируются (дальше — домен wal-статуса).</summary>
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
                        $"WAL chain gap: expected {expected.Name}, found {segment.Name}", last);
                }
                // сегмент меньше expected (дубли/хвост старого потока) — игнор
            }
            else if (segment.Tli > expected.Tli)
            {
                // переход: строго по содержимому history, иначе эвристика.
                // Проверку «segment == end» делаем ПОСЛЕ валидации перехода —
                // end может быть первым сегментом нового TLI.
                if (historyContents is not null
                    && historyContents.TryGetValue(segment.Tli, out var content)
                    && WalHistory.Parse(content) is { Count: > 0 } entries)
                {
                    var entry = entries[^1]; // последняя строка — сам TLI файла
                    if (entry.ParentTli != expected.Tli)
                        return new ChainResult(false,
                            $"TLI transition {expected.Tli}→{segment.Tli}: history file {segment.Tli:x8}.history: " +
                            $"parent {entry.ParentTli} ≠ {expected.Tli}", last);
                    var switchPos = WalFileName.FromLsn(0, entry.SwitchLsn);
                    var inBounds = (switchPos.Log, switchPos.Seg) == (segment.Log, segment.Seg)
                        || (last is { } prev && (switchPos.Log, switchPos.Seg) == (prev.Log, prev.Seg));
                    if (!inBounds)
                        return new ChainResult(false,
                            $"TLI transition {expected.Tli}→{segment.Tli}: switchWALLSN {entry.SwitchLsn} " +
                            $"outside segment bounds {(last is { } strictPrev ? strictPrev.Name : "no previous")}/{segment.Name} " +
                            $"({segment.Tli:x8}.history)", last);
                }
                else
                {
                    // эвристика (Check): history-объект + первый сегмент ∈ {last, Next(last)}
                    if (!histories.Contains(segment.Tli))
                        return new ChainResult(false,
                            $"TLI transition {expected.Tli}→{segment.Tli} without {segment.Tli:x8}.history", last);
                    // сравнение ПОЗИЦИЙ (log/seg): TLI первого сегмента нового таймлайна
                    // отличен от прошлого — равенство по record-полям здесь ложно.
                    var lastOrNext = last is { } prev
                        ? (segment.Log, segment.Seg) == (prev.Log, prev.Seg)
                          || (segment.Log, segment.Seg) == (prev.Next().Log, prev.Next().Seg)
                        : false;
                    if (!lastOrNext)
                        return new ChainResult(false,
                            $"TLI transition {expected.Tli}→{segment.Tli}: first segment {segment.Name} " +
                            $"does not match the switch point ({(last is { } prev2 ? prev2.Name : "no previous")})", last);
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
        // не продублирован в wal/ (дублирование, spec §3.4)
        return new ChainResult(false,
            $"segment {reached.Name} from the backup set is missing in wal/ or the backup set (expected after " +
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
