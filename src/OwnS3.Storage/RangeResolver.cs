namespace OwnS3.Storage;

// Резолв одиночного Range канона 02 §1: a-b, a-, -N; обрезка end по размеру,
// суффикс длиннее объекта, пустой объект. Заголовок условного диапазона
// каноном не поддерживается (02 §1): ответ определяется только Range.
public static class RangeResolver
{
    public static AppliedByteRange? Resolve(ByteRange? range, long size)
    {
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
