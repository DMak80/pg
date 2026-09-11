namespace PgWorker.Backups;

/// <summary>Результат контроля цепочки: дыра с границами (для error-статуса) и
/// последний валидный сегмент (для last_uploaded).</summary>
public sealed record ChainResult(bool IsContinuous, string? GapError, WalFileName? LastSegment);

/// <summary>Gap-детектор непрерывности WAL-цепочки от chain_start (arch/19 §3,
/// правила t03 spec §3.1): последовательность Next() без пропусков; TLI-переход
/// валиден при наличии загруженного `&lt;newTLI&gt;.history` и первом сегменте нового
/// TLI ∈ {последний сегмент старого TLI, Next(последний)}; `.partial` игнорируется.
/// Полный LSN-разбор history — t04 (verify).</summary>
public static class WalChain
{
    public static ChainResult Check(WalFileName chainStart, IEnumerable<string> objectNames)
    {
        // Разбор объектов: сегменты (сортировка имени == сортировка (Tli,Log,Seg) —
        // фиксированная ширина hex) + множество загруженных history-TLI.
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

        var expected = chainStart;
        WalFileName? last = null;
        foreach (var segment in segments)
        {
            if (segment.Tli == expected.Tli)
            {
                if (segment.Log == expected.Log && segment.Seg == expected.Seg)
                {
                    // ожидаемый сегмент — цепочка продолжается
                    last = segment;
                    expected = segment.Next();
                }
                else if ((long)(segment.Log * WalFileName.SegsPerLog + segment.Seg)
                         > (long)(expected.Log * WalFileName.SegsPerLog + expected.Seg))
                {
                    // пропуск внутри TLI: границы дыры — ожидали/найдено
                    return new ChainResult(false,
                        $"дыра WAL-цепочки: ожидался {expected.Name}, найден {segment.Name}", last);
                }
                // сегмент меньше expected (дубли/хвост старого потока) — игнор
            }
            else if (segment.Tli > expected.Tli)
            {
                // TLI-переход: обязательна history нового TLI; первый сегмент нового
                // TLI — точка переключения (== last, PG перезаписывает сегмент) или
                // следующий за ней (переход на границе).
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
                        $"не совпадает с точкой переключения ({(last is { } l ? l.Name : "нет предыдущего")})", last);
                last = segment;
                expected = segment.Next();
            }
            // сегмент старого TLI после перехода — вне цепочки (сортировка даёт их
            // раньше; сюда попадают только дубли ниже expected) — игнор
        }

        return new ChainResult(true, null, last);
    }
}
