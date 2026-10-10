namespace OwnS3.Storage;

public enum ConditionalOutcome { Proceed, NotModified, PreconditionFailed }

// Таблица conditional-исходов канона 02 §1 (даты уже спарсены App; невалидные
// отброшены): If-Match решает единолично; далее If-None-Match; далее
// If-Modified-Since (только read-семантика: Get/Head/GetObjectAttributes);
// далее If-Unmodified-Since. ETag-сверки — с ETag всего объекта (P8: кавычки
// нормализуются с обеих сторон).
public static class ConditionalEvaluator
{
    public static ConditionalOutcome Evaluate(ObjectConditions? conditions, string etag,
        DateTimeOffset lastModified, bool ifModifiedSinceApplies)
    {
        if (conditions is null)
            return ConditionalOutcome.Proceed;
        if (conditions.IfMatch is not null)
            return ETagMatches(conditions.IfMatch, etag) ? ConditionalOutcome.Proceed
                                                          : ConditionalOutcome.PreconditionFailed;
        if (conditions.IfNoneMatch is not null)
            return ETagMatches(conditions.IfNoneMatch, etag) ? ConditionalOutcome.NotModified
                                                              : ConditionalOutcome.Proceed;
        // modTime хранится в мс — сравнение с HTTP-датами по секундам
        var trimmed = new DateTimeOffset(lastModified.Ticks - lastModified.Ticks % TimeSpan.TicksPerSecond,
            lastModified.Offset);
        if (ifModifiedSinceApplies && conditions.IfModifiedSince is not null && trimmed <= conditions.IfModifiedSince)
            return ConditionalOutcome.NotModified;
        if (conditions.IfUnmodifiedSince is not null && trimmed > conditions.IfUnmodifiedSince)
            return ConditionalOutcome.PreconditionFailed;
        return ConditionalOutcome.Proceed;
    }

    // Значение заголовка (возможен список через запятую, кавычки нормализуются,
    // «*» — любой) против etag объекта; Ordinal.
    public static bool ETagMatches(string headerValue, string etag)
    {
        foreach (var raw in headerValue.Split(',', StringSplitOptions.TrimEntries))
        {
            if (raw == "*")
                return true;
            if (string.Equals(Unquote(raw), Unquote(etag), StringComparison.Ordinal))
                return true;
        }
        return false;
    }

    private static string Unquote(string value) =>
        value.Length >= 2 && value.StartsWith('"') && value.EndsWith('"')
            ? value[1..^1]
            : value;
}
