using System.Text;

namespace OwnS3.Storage;

// Строгий байтовый порядок UTF-8 (канон 02 §3, решение пользователя; P9):
// побайтовое сравнение UTF-8 представления. string Ordinal НЕ подходит —
// расходится с байтовым порядком на символах вне BMP (сурогаты D800-DFFF
// в UTF-16 ниже U+E000-U+FFFF, в UTF-8 — выше). Единый компаратор сортировки
// детей листинга, emit-фильтров, marker/continuation и ListBuckets.
public static class Utf8ByteOrder
{
    public static int Compare(string left, string right) =>
        CompareBytes(Encoding.UTF8.GetBytes(left), Encoding.UTF8.GetBytes(right));

    public static int CompareBytes(ReadOnlySpan<byte> l, ReadOnlySpan<byte> r)
    {
        var common = Math.Min(l.Length, r.Length);
        var cmp = l[..common].SequenceCompareTo(r[..common]);
        return cmp != 0 ? cmp : l.Length.CompareTo(r.Length);
    }

    // Байтовый префикс (границы символов UTF-8 самосинхронизируются — сравнение
    // байтов префикса корректно на любой позиции начала строки).
    public static bool StartsWith(string value, string prefix) =>
        Encoding.UTF8.GetBytes(value).AsSpan().StartsWith(Encoding.UTF8.GetBytes(prefix));

    // Байтовый суффикс (распознавание маркера-CP по delimiter-окончанию, М10).
    public static bool EndsWith(string value, string suffix) =>
        Encoding.UTF8.GetBytes(value).AsSpan().EndsWith(Encoding.UTF8.GetBytes(suffix));
}
