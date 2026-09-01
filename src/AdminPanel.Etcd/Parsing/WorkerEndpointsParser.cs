using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

/// <summary>
/// Результат разбора ключей доступа: endpoints + ошибки разбора (кормят
/// key-malformed, тик не роняют). Позиционный record — деконструкция
/// `(endpoints, errors)` доступна как у tuple.
/// </summary>
public sealed record WorkerEndpointsParseResult(
    IReadOnlyList<WorkerEndpoint> Endpoints,
    IReadOnlyList<KeyParseError> Errors);

/// <summary>
/// Чистая функция: KV префикса /pgworker/api/ (или /kafkaworker/api/) → живые
/// endpoints API воркеров. Формат value (arch/02 §2.3.1/§2.3.2):
/// {"url":"...","instance":"...","since_unix":N}. Битый JSON/без url —
/// KeyParseError (тик не роняют, толерантность как у других парсеров).
/// Префикс-агностичен: id = лист после последнего «/».
/// </summary>
public static class WorkerEndpointsParser
{
    public static WorkerEndpointsParseResult Parse(IReadOnlyList<Kv> kvs)
    {
        var endpoints = new List<WorkerEndpoint>();
        var errors = new List<KeyParseError>();
        foreach (var kv in kvs)
        {
            var instanceId = kv.Key[(kv.Key.LastIndexOf('/') + 1)..];
            if (instanceId.Length == 0)
            {
                errors.Add(new(kv.Key, "пустой id инстанса в ключе"));
                continue;
            }

            try
            {
                using var doc = JsonDocument.Parse(kv.Value);
                var root = doc.RootElement;
                if (root.ValueKind != JsonValueKind.Object
                    || !root.TryGetProperty("url", out var url)
                    || url.ValueKind != JsonValueKind.String)
                {
                    errors.Add(new(kv.Key, "нет поля url"));
                    continue;
                }

                endpoints.Add(new WorkerEndpoint(
                    instanceId,
                    url.GetString()!,
                    root.TryGetProperty("since_unix", out var unix)
                        && unix.ValueKind == JsonValueKind.Number
                        ? unix.GetInt64()
                        : 0));
            }
            catch (JsonException e)
            {
                errors.Add(new(kv.Key, $"битый JSON: {e.Message}"));
            }
        }

        return new(endpoints, errors);
    }
}
