namespace OwnS3.Storage;

// Резолв одиночного Range канона 02 §1: a-b, a-, -N; обрезка end по размеру,
// суффикс длиннее объекта, пустой объект. Заголовок условного диапазона
// каноном не поддерживается (02 §1): ответ определяется только Range.
// Исход — по конвенции главы 01 §4: «Range не задан» — терминальный Success(null);
// некорректный диапазон — Failed(ObjectStoreException(InvalidRange)) — 416.
public static class RangeResolver
{
    public static Result<AppliedByteRange?> Resolve(ByteRange? range, long size)
    {
        if (range is null)
            return Result<AppliedByteRange?>.Success(null); // «Range не задан» — терминальное знание
        if (size == 0)
            return Result<AppliedByteRange?>.Failed(
                new ObjectStoreException(ObjectStoreErrorCode.InvalidRange));
        if (range.Start is { } start)
        {
            if (start >= size)
                return Result<AppliedByteRange?>.Failed(
                    new ObjectStoreException(ObjectStoreErrorCode.InvalidRange));
            var end = range.End is null || range.End >= size ? size - 1 : range.End.Value;
            return Result<AppliedByteRange?>.Success(new AppliedByteRange(start, end, size));
        }

        var suffix = -range.End!.Value;        // ParseRange кодирует bytes=-N как End=-N
        if (suffix <= 0)
            return Result<AppliedByteRange?>.Failed(
                new ObjectStoreException(ObjectStoreErrorCode.InvalidRange));
        return Result<AppliedByteRange?>.Success(
            suffix >= size ? new AppliedByteRange(0, size - 1, size)
                           : new AppliedByteRange(size - suffix, size - 1, size));
    }
}
