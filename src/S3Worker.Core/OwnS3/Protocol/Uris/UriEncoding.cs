using System.Text;

namespace OwnS3.Protocol.Uris;

// Каноническое кодирование URI/query SigV4 (arch/owns3/03 §1, вкл. arch-правку 9):
// unreserved RFC 3986 (A-Z a-z 0-9 '-' '.' '_' '~') не кодируются; существующие
// %XX-тройки пути сохраняются как отправлено; прочее — %XX верхнего регистра.
// «Плюс как пробел» — form-семантика разбора, в канонизации не участвует:
// пробел → %20, литеральный '+' → %2B.
public static class UriEncoding
{
    /// <summary>Канонический URI: сегментное кодирование, '/' — разделитель.</summary>
    public static string EncodePath(string rawPath)
    {
        var sb = new StringBuilder(rawPath.Length + 16);
        var i = 0;
        while (i < rawPath.Length)
        {
            var c = rawPath[i];
            if (IsUnreserved(c) || c == '/')
            {
                sb.Append(c);
                i++;
            }
            else if (c == '%' && TryPeekTriple(rawPath, i, out var decoded))
            {
                // Валидная %XX-тройка сохраняется как отправлено.
                sb.Append(rawPath, i, 3);
                i += 3;
                _ = decoded;
            }
            else
            {
                AppendPercentEncoded(sb, c);
                i++;
            }
        }
        return sb.ToString();
    }

    /// <summary>Канонический query: сортировка пар до кодирования, RFC 3986.</summary>
    public static string EncodeQuery(IEnumerable<(string Name, string? Value)> pairs)
    {
        var sorted = pairs
            .Select(p => (Name: p.Name, Value: p.Value ?? string.Empty))
            .OrderBy(p => p.Name, StringComparer.Ordinal)
            .ThenBy(p => p.Value, StringComparer.Ordinal)
            .ToList();
        return string.Join("&", sorted.Select(p => $"{EncodeValue(p.Name)}={EncodeValue(p.Value)}"));
    }

    /// <summary>Кодирование ключа/значения query: пробел → %20, '+' → %2B.</summary>
    public static string EncodeValue(string value)
    {
        var sb = new StringBuilder(value.Length + 16);
        foreach (var c in value)
            AppendPercentEncoded(sb, c);
        return sb.ToString();
    }

    private static void AppendPercentEncoded(StringBuilder sb, char c)
    {
        if (IsUnreserved(c))
        {
            sb.Append(c);
            return;
        }
        if (c <= 0x7F)
        {
            sb.Append('%').Append(ToHexUpper(c));
        }
        else
        {
            // Многобайтовый символ — UTF-8-байты в %XX.
            var bytes = Encoding.UTF8.GetBytes(c.ToString());
            foreach (var b in bytes)
                sb.Append('%').Append(ToHexUpper((char)b));
        }
    }

    private static string ToHexUpper(char c) =>
        ((int)c).ToString("X2", System.Globalization.CultureInfo.InvariantCulture);

    private static bool IsUnreserved(char c) =>
        c is (>= 'A' and <= 'Z') or (>= 'a' and <= 'z') or (>= '0' and <= '9')
            or '-' or '.' or '_' or '~';

    private static bool TryPeekTriple(string s, int i, out char decoded)
    {
        decoded = '\0';
        if (i + 2 >= s.Length)
            return false;
        var h1 = FromHex(s[i + 1]);
        var h2 = FromHex(s[i + 2]);
        if (h1 < 0 || h2 < 0)
            return false;
        decoded = (char)((h1 << 4) | h2);
        return true;
    }

    private static int FromHex(char c) => c switch
    {
        >= '0' and <= '9' => c - '0',
        >= 'a' and <= 'f' => c - 'a' + 10,
        >= 'A' and <= 'F' => c - 'A' + 10,
        _ => -1,
    };
}
