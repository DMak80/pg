using System.Text;
using OwnS3.Protocol.Requests;
using OwnS3.Protocol.Uris;

namespace OwnS3.Protocol.Auth;

// Сборка canonical request SigV4 (arch/owns3/03 §1): метод, canonical URI
// (UriEncoding.EncodePath), canonical query (сортировка до кодирования,
// RFC 3986 — arch-правка 9), canonical headers (Trimall — arch-правка 7,
// многозначные — join запятой, host обязателен), signed headers, payload.
public static class CanonicalRequestBuilder
{
    public static string Build(S3RequestModel model, IReadOnlyList<string> signedHeaders, string payloadString)
    {
        var method = model.Method.ToUpperInvariant();
        var uri = UriEncoding.EncodePath(model.RawPath);
        var query = UriEncoding.EncodeQuery(QueryPairs(model.RawQuery));

        // Имена подписанных заголовков + обязательный host (канон §1 п.4);
        // неподписанный host расходит подпись (SignatureDoesNotMatch-семантика).
        var names = signedHeaders
            .Select(n => n.ToLowerInvariant())
            .Distinct()
            .Union(["host"])
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();

        var headersBlock = new StringBuilder();
        foreach (var name in names)
        {
            var values = name == "host"
                ? [model.Host]
                : model.Headers.Values(name).Select(TrimAll).ToList();
            headersBlock.Append(name).Append(':').Append(string.Join(",", values)).Append('\n');
        }

        var signedList = string.Join(";", names);
        return $"{method}\n{uri}\n{query}\n{headersBlock}\n{signedList}\n{payloadString}";
    }

    // Разбор сырой query-строки: form-семантика ('+' — пробел, %XX — декод),
    // далее пары сортируются и кодируются RFC 3986 (UriEncoding.EncodeQuery).
    public static IEnumerable<(string Name, string? Value)> QueryPairs(string rawQuery)
    {
        if (string.IsNullOrEmpty(rawQuery))
            yield break;
        foreach (var pairText in rawQuery.Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var eq = pairText.IndexOf('=');
            var rawName = eq < 0 ? pairText : pairText[..eq];
            var rawValue = eq < 0 ? null : pairText[(eq + 1)..];
            yield return (FormDecode(rawName), rawValue is null ? null : FormDecode(rawValue));
        }
    }

    // Trimall стандарта SigV4 (arch-правка 7; референс signV4TrimAll):
    // усечение краёв + схлопывание внутренних последовательностей пробелов.
    public static string TrimAll(string value) =>
        string.Join(" ", value.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries));

    private static string FormDecode(string value) =>
        Uri.UnescapeDataString(value.Replace('+', ' '));
}
