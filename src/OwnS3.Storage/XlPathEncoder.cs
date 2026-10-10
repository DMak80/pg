using System.Text;

namespace OwnS3.Storage;

// Ключ → кодированные сегменты xl-пути и обратно (канон 04 §1): percent-кодирование
// каждого сегмента по RFC 3986 + спецслучаи для корректных компонентов ФС-пути;
// ключ-маркер каталога (завершающий «/») — суффикс «__XLDIR__» последнего сегмента.
public static class XlPathEncoder
{
    private const string DirMarker = "__XLDIR__";

    // Ключ → кодированные сегменты пути (без bucket). Бросает
    // XlInvalidArgumentException("Key too long after encoding"), если кодированный
    // сегмент (включая приклеенный маркер) длиннее 255 байт.
    public static string[] EncodeSegments(string key)
    {
        // Пустые сегменты значимы: «a//b» → ["a","","b"].
        var segments = key.Split('/');
        var hasDirMarker = segments.Length > 1 && segments[^1].Length == 0;
        var count = hasDirMarker ? segments.Length - 1 : segments.Length;
        var encoded = new string[count];
        for (var i = 0; i < count; i++)
            encoded[i] = EncodeSegment(segments[i]);
        // Маркер приклеивается к последнему уже КОДИРОВАННОМУ сегменту: экранирование
        // литерального «__XLDIR__» выполнено выше — приклеенный маркер остаётся чистым.
        if (hasDirMarker && count > 0)
            encoded[^1] += DirMarker;
        // Лимит компонента пути ФС — ПОСЛЕ приклейки маркера (маркер удлиняет сегмент).
        foreach (var segment in encoded)
            if (Encoding.UTF8.GetByteCount(segment) > 255)
                throw new XlInvalidArgumentException("Key too long after encoding");
        return encoded;
    }

    // Кодированный относительный путь (сегменты через «/»).
    public static string EncodePath(string key) => string.Join("/", EncodeSegments(key));

    // Обратное преобразование (сегменты каталога → ключ); маркер каталога
    // раскрывается в завершающий «/». Порядок: суффикс-проверка на сыром сегменте,
    // спецслучай одиночного «%», затем percent-декод.
    public static string DecodeSegments(IReadOnlyList<string> segments)
    {
        var parts = new List<string>(segments.Count);
        foreach (var raw in segments)
        {
            var segment = raw;
            var isDirMarker = segment.EndsWith(DirMarker, StringComparison.Ordinal);
            if (isDirMarker)
                segment = segment[..^DirMarker.Length];
            // Спецслучай ДО percent-декода: сырой одиночный «%» — пустой сегмент
            // (литеральный «%» кодируется как «%25», коллизии нет).
            segment = segment == "%" ? "" : Uri.UnescapeDataString(segment);
            parts.Add(isDirMarker ? segment + "/" : segment);
        }
        return string.Join("/", parts);
    }

    // Сегмент → имя компонента ФС: RFC 3986 + спецслучаи канона 04 §1.
    private static string EncodeSegment(string segment)
    {
        var escaped = Uri.EscapeDataString(segment);
        return escaped switch
        {
            "" => "%",                     // пустой сегмент («a//b»)
            "." => "%2E",                  // литеральные «.»/«..» недопустимы в пути ФС
            ".." => "%2E%2E",
            // Результат, оканчивающийся на литеральный маркер, экранируется:
            // завершающий «_» → «%5F» (защита от ложного срабатывания маркера).
            _ when escaped.EndsWith(DirMarker, StringComparison.Ordinal)
                => escaped[..^1] + "%5F",
            _ => escaped
        };
    }
}
