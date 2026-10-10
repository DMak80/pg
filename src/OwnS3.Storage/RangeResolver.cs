namespace OwnS3.Storage;

// Резолв одиночного Range канона 02 §1: a-b, a-, -N; обрезка end по размеру,
// суффикс длиннее объекта, пустой объект, If-Range (сильный ETag: совпал —
// применять Range; не совпал или дата-форма — 200 полным объектом).
public static class RangeResolver
{
    public static AppliedByteRange? Resolve(ByteRange? range, string? ifRange, string etag, long size)
    {
        if (ifRange is not null && !ConditionalEvaluator.ETagMatches(ifRange, etag))
            return null;                       // If-Range не совпал (или дата) → 200 полным
        if (range is null)
            return null;
        if (size == 0)
            throw new ObjectStoreException(ObjectStoreErrorCode.InvalidRange);
        if (range.Start is { } start)
        {
            if (start >= size)
                throw new ObjectStoreException(ObjectStoreErrorCode.InvalidRange);
            var end = range.End is null || range.End >= size ? size - 1 : range.End.Value;
            return new AppliedByteRange(start, end, size);
        }
        var suffix = -range.End!.Value;        // ParseRange кодирует bytes=-N как End=-N
        if (suffix <= 0)
            throw new ObjectStoreException(ObjectStoreErrorCode.InvalidRange);
        return suffix >= size ? new AppliedByteRange(0, size - 1, size)
                              : new AppliedByteRange(size - suffix, size - 1, size);
    }
}
