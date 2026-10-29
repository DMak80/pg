using System.Text.Json;
using Shared.Etcd.Client;

namespace PgWorker.Etcd.S3;

// Модель пула S3 для воркера (arch/23 §2/§7): PgWorker — ЧИТАТЕЛЬ /s3/,
// участником контракта не становится. Имена инстансов — из ключей заявки
// nodes/<I>/state|placement; portalloc — ТОЛЬКО источник адресов.

/// <summary>Нода пула: имя инстанса (из ключей nodes/&lt;I&gt;/state|placement),
/// состояние nodes/&lt;I&gt;/state, фактический хост nodes/&lt;I&gt;/placed_on;
/// Host/S3Port — из /s3worker/portalloc/&lt;C&gt; (null — нода заявлена, но не
/// провижинена: адрес появится, пул поднимается — arch/19 §4).</summary>
public sealed record S3PoolNode(
    string Name, string? State, string? PlacedOn, string? Host, int? S3Port);

/// <summary>Креды пула из /s3/clusters/&lt;C&gt;/secrets (ключ
/// name=backups, read-write; arch/23 §6/§7): null — ключа нет/битый/
/// нет ключа backups (резолвер даёт transient-диагностику, пул жив).</summary>
public sealed record S3PoolSecrets(string AccessKey, string SecretKey);

/// <summary>Пул S3 (имя = кластер /s3/clusters/&lt;C&gt;): ToRemove —
/// config.state == "TO_REMOVE" (отсутствие state = Active, arch/23 §2);
/// Endpoints — host:port в порядке endpoints-ключа (порядок чтения ReadAll).</summary>
public sealed record S3Pool(
    string Name, bool ToRemove,
    IReadOnlyList<S3PoolNode> Nodes, IReadOnlyList<string> Endpoints,
    S3PoolSecrets? Secrets = null);

/// <summary>Парсеры значений домена /s3/ (толерантность unknownKeys/parseError —
/// как у прочих доменов: битый JSON — null-результат с диагностикой, не краш).</summary>
public static class S3PoolModel
{
    /// <summary>Разобранный config пула: ToRemove — заявка удаления кластера.</summary>
    public sealed record S3PoolConfig(bool ToRemove);

    /// <summary>endpoints-строка → упорядоченные "host:port" (arch/23 §2:
    /// "h1:p1,h2:p2"; битые элементы пропускаются — порядок = порядок чтения).</summary>
    public static IReadOnlyList<string> ParseEndpoints(string raw)
    {
        var result = new List<string>();
        foreach (var item in raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            // Формат элемента — host:port; порт строго числовой (host может
            // быть именем сети контура). Битое — молча в обход (чужой мусор
            // не роняет читателя).
            var colon = item.LastIndexOf(':');
            if (colon <= 0 || colon == item.Length - 1)
                continue;
            if (!int.TryParse(item[(colon + 1)..], out var port) || port <= 0)
                continue;
            result.Add(item);
        }

        return result;
    }

    /// <summary>config-JSON → S3PoolConfig; битый JSON/типы → null
    /// (диагностика у вызывающего — parseError-путь).</summary>
    public static S3PoolConfig? ParseConfig(string raw)
    {
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
                return null;
            // state отсутствует = Active (arch/23 §2); ТОЛЬКО "TO_REMOVE"
            // считается заявкой удаления — прочие значения не инвалидируют пул.
            var toRemove = root.TryGetProperty("state", out var st)
                && st.ValueKind == JsonValueKind.String
                && st.GetString() == "TO_REMOVE";
            return new S3PoolConfig(toRemove);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>secrets-JSON → креды ключа backups (arch/23 §2.1); битый
    /// JSON/нет ключа backups/пустые значения → null (пул читается, креды
    /// даёт transient-диагностика резолвера).</summary>
    public static S3PoolSecrets? ParseSecrets(string raw)
    {
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object
                || !root.TryGetProperty("access_keys", out var keys)
                || keys.ValueKind != JsonValueKind.Array)
                return null;
            foreach (var item in keys.EnumerateArray())
            {
                // Формат элемента: {"name":"backups","access_key":"…",
                // "secret_key":"…",…}. Кандидат — элемент с name=="backups"
                // и непустыми строковыми access_key/secret_key; мусор — в обход
                // (чужой/битый элемент не роняет читателя). Несколько подходящих
                // — первый.
                if (item.ValueKind != JsonValueKind.Object
                    || !item.TryGetProperty("name", out var name)
                    || name.ValueKind != JsonValueKind.String
                    || name.GetString() != "backups"
                    || !item.TryGetProperty("access_key", out var accessKey)
                    || accessKey.ValueKind != JsonValueKind.String
                    || !item.TryGetProperty("secret_key", out var secretKey)
                    || secretKey.ValueKind != JsonValueKind.String)
                    continue;
                var access = accessKey.GetString();
                var secret = secretKey.GetString();
                if (string.IsNullOrEmpty(access) || string.IsNullOrEmpty(secret))
                    continue;
                return new S3PoolSecrets(access, secret);
            }

            return null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>portalloc-JSON (arch/23 §8) → адреса по именам инстансов;
    /// битые узлы молча пропускаются (паттерн PortAllocIndex).</summary>
    public static IReadOnlyDictionary<string, (string Host, int Port)> ParsePortAlloc(string raw)
    {
        var map = new Dictionary<string, (string, int)>();
        try
        {
            using var doc = JsonDocument.Parse(raw);
            foreach (var node in doc.RootElement.EnumerateObject())
            {
                // Обязательные поля host (строка) + s3 (число); мусор — skip.
                if (node.Value.ValueKind != JsonValueKind.Object
                    || !node.Value.TryGetProperty("host", out var host)
                    || host.ValueKind != JsonValueKind.String
                    || !node.Value.TryGetProperty("s3", out var s3)
                    || s3.ValueKind != JsonValueKind.Number
                    || !s3.TryGetInt32(out var port))
                    continue;
                map[node.Name] = (host.GetString()!, port);
            }
        }
        catch (JsonException)
        {
            // Битый portalloc → пустая карта: ноды останутся без адресов
            // (заявлены, не провижинены) — resolver скажет «пул поднимается».
        }

        return map;
    }

    /// <summary>Имена инстансов пула — union ключей nodes/&lt;I&gt;/state и
    /// nodes/&lt;I&gt;/placement (заявка = существование ключа, arch/19 §4);
    /// Ordinal-сортировка — детерминизм.</summary>
    public static IReadOnlyList<string> InstanceNames(IReadOnlyList<Kv> kvs)
    {
        var names = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var kv in kvs)
        {
            // Схема ключа: /s3/clusters/<C>/nodes/<I>/<leaf> (7 сегментов);
            // прочие ключи префикса (config/endpoints/…) — не заявка инстанса.
            var parts = kv.Key.Split('/');
            if (parts.Length != 7
                || parts[1] != "s3" || parts[2] != "clusters" || parts[4] != "nodes")
                continue;
            names.Add(parts[5]);
        }

        return [.. names];
    }
}
