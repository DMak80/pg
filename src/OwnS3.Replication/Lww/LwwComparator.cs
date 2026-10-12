// LWW-компаратор (t35, глава 07 §7): ключ сравнения (modTime, contentSha256);
// равенство modTime — детерминированный тай-брейк лексикографически больший sha
// (без координации); равные modTime при разных sha — журналируемый конфликт (R4).
namespace OwnS3.Replication.Lww;

public enum LwwOutcome
{
    IncomingWins,
    LocalWins,
    Identical,
}

public static class LwwComparator
{
    // Полная таблица исходов (глава 07 §7):
    // in.mod > local.mod → IncomingWins; < → LocalWins; == и sha == → Identical;
    // == и sha != → тай-брейк лексикографически больший sha; conflict=true.
    public static (LwwOutcome Outcome, bool Conflict) Compare(long inModMs, string inSha, long localModMs,
        string localSha)
    {
        if (inModMs > localModMs)
            return (LwwOutcome.IncomingWins, false);
        if (inModMs < localModMs)
            return (LwwOutcome.LocalWins, false);
        if (string.Equals(inSha, localSha, StringComparison.Ordinal))
            return (LwwOutcome.Identical, false);
        // Равные modTime, разные sha — конфликт: победитель по sha лексикографически
        return string.CompareOrdinal(inSha, localSha) > 0
            ? (LwwOutcome.IncomingWins, true)
            : (LwwOutcome.LocalWins, true);
    }
}
