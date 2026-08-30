using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using KafkaWorker.Core;

namespace KafkaWorker.Etcd.Client;

// HTTP-ошибка gateway: не-2xx от /v3/*.
public sealed class EtcdHttpException(string endpoint, int statusCode, string body)
    : Exception($"etcd {endpoint} ответил {statusCode}: {body}");

// Реализация IEtcdGateway: HttpClient (именованный клиент DI), base64-кодирование ключей.
// Таймаут задаётся конфигурацией клиента, не здесь.
public sealed class EtcdGateway(HttpClient httpClient) : IEtcdGateway
{
    // etcd gateway сериализует int64 decimal-строками и не приводит proto-имена к camelCase.
    private static readonly JsonSerializerOptions Json = new()
    {
        NumberHandling = JsonNumberHandling.AllowReadingFromString,
    };

    public async Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
    {
        var body = new { key = ToB64(prefix), range_end = ToB64(PrefixEnd(prefix)) };
        var result = await Result<RangeResponse>.FromAsync(
            async () => await PostAsync<RangeResponse>(endpoint, "/v3/kv/range", body, ct));
        return result.Map(r => (IReadOnlyList<Kv>)(r.Kvs ?? [])
            .Select(k => new Kv(FromB64(k.Key), FromB64(k.Value), k.ModRevision))
            .ToList());
    }

    public async Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
    {
        // Точечный range: range_end = ключ с инкрементированным последним байтом.
        var body = new { key = ToB64(key), range_end = ToB64(PrefixEnd(key)) };
        var result = await Result<RangeResponse>.FromAsync(
            async () => await PostAsync<RangeResponse>(endpoint, "/v3/kv/range", body, ct));
        return result.Map(r => (r.Kvs ?? [])
            .Select(k => new Kv(FromB64(k.Key), FromB64(k.Value), k.ModRevision))
            .FirstOrDefault());
    }

    public async Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
    {
        object body = lease is { } l
            ? new { key = ToB64(key), value = ToB64(value), lease = l }
            : new { key = ToB64(key), value = ToB64(value) };
        return await Result.FromAsync(
            async () => await PostAsync<PutResponse>(endpoint, "/v3/kv/put", body, ct));
    }

    public async Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
    {
        object body = prefix
            ? new { key = ToB64(keyOrPrefix), range_end = ToB64(PrefixEnd(keyOrPrefix)) }
            : new { key = ToB64(keyOrPrefix) };
        return await Result.FromAsync(
            async () => await PostAsync<DeleteResponse>(endpoint, "/v3/kv/deleterange", body, ct));
    }

    public async Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct)
    {
        var body = new
        {
            compare = req.Compare.Select(CompareToDto).ToList(),
            success = req.Success.Select(OpToDto).ToList(),
            failure = req.Failure.Select(OpToDto).ToList(),
        };
        var result = await Result<TxnResponse>.FromAsync(
            async () => await PostAsync<TxnResponse>(endpoint, "/v3/kv/txn", body, ct));
        return result.Map(r => new TxnResult(r.Succeeded));
    }

    public async Task<Result<long>> LeaseGrantAsync(string endpoint, int ttlSec, CancellationToken ct)
    {
        var body = new { TTL = ttlSec };
        var result = await Result<LeaseGrantResponse>.FromAsync(
            async () => await PostAsync<LeaseGrantResponse>(endpoint, "/v3/lease/grant", body, ct));
        return result.Map(r => r.Id);
    }

    public async Task<Result> LeaseRevokeAsync(string endpoint, long lease, CancellationToken ct)
    {
        var body = new { ID = lease };
        return await Result.FromAsync(
            async () => await PostAsync<LeaseRevokeResponse>(endpoint, "/v3/lease/revoke", body, ct));
    }

    public async Task<Result> LeaseKeepaliveAsync(string endpoint, long lease, CancellationToken ct)
    {
        var body = new { ID = lease };
        var result = await Result<LeaseKeepaliveResponse>.FromAsync(
            async () => await PostAsync<LeaseKeepaliveResponse>(endpoint, "/v3/lease/keepalive", body, ct));
        // Один цикл продления: TTL<=0 в ответе — lease истёк (например, после разрыва).
        return result.Bind(r => r.Result is { Ttl: > 0 }
            ? Result.Success()
            : Result.Failed(new EtcdHttpException(endpoint, 404, $"lease {lease} не продлён (TTL=0)")));
    }

    public async Task<Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct)
    {
        var result = await Result<byte[]>.FromAsync(async () =>
        {
            // etcd 3.5.x: путь /v3/maintenance/snapshot (путь /v3/snapshot/save появился в 3.6+).
            using var response = await httpClient.PostAsJsonAsync(endpoint + "/v3/maintenance/snapshot", new { }, Json, ct);
            if (!response.IsSuccessStatusCode)
            {
                var errorBody = response.Content is null
                    ? string.Empty
                    : await response.Content.ReadAsStringAsync(ct);
                throw new EtcdHttpException(endpoint, (int)response.StatusCode, errorBody);
            }

            // Бинарный слепок БД (application/octet-stream), не JSON.
            return await response.Content.ReadAsByteArrayAsync(ct);
        });
        return result;
    }

    public async Task<Result<long>> StatusAsync(string endpoint, CancellationToken ct)
    {
        var result = await Result<StatusResponse>.FromAsync(
            async () => await PostAsync<StatusResponse>(endpoint, "/v3/maintenance/status", new { }, ct));
        return result.Map(r => (long)r.Header!.Revision);
    }

    public async Task<Result> CompactAsync(string endpoint, long revision, CancellationToken ct)
    {
        var body = new { revision };
        return await Result.FromAsync(
            async () => await PostAsync<CompactionResponse>(endpoint, "/v3/kv/compaction", body, ct));
    }

    public async Task<Result> DefragmentAsync(string endpoint, CancellationToken ct)
    {
        return await Result.FromAsync(
            async () => await PostAsync<DefragmentResponse>(endpoint, "/v3/maintenance/defragment", new { }, ct));
    }

    // Compare → protojson: target (enum: VERSION=0, MOD=2, VALUE=3), result (EQUAL=0/GREATER=1),
    // поле сравнения = цели (version/mod_revision/value).
    private static Dictionary<string, object> CompareToDto(TxnCompare c)
    {
        var dto = new Dictionary<string, object>
        {
            ["key"] = ToB64(c.Key),
            ["target"] = c.Target switch
            {
                TxnTarget.Version => 0,
                TxnTarget.ModRevision => 2,
                TxnTarget.Value => 3,
                _ => throw new InvalidOperationException($"неподдерживаемая цель txn-compare: {c.Target}"),
            },
            ["result"] = c.Pred == TxnPredicate.Greater ? 1 : 0,
        };
        switch (c.Target)
        {
            case TxnTarget.Version:
                dto["version"] = c.Num;
                break;
            case TxnTarget.ModRevision:
                dto["mod_revision"] = c.Num;
                break;
            case TxnTarget.Value:
                dto["value"] = ToB64(c.Arg);
                break;
            default:
                throw new InvalidOperationException($"неподдерживаемая цель txn-compare: {c.Target}");
        }

        return dto;
    }

    // Операция ветки txn → request_put / request_delete_range.
    private static Dictionary<string, object> OpToDto(TxnOp op) => op switch
    {
        TxnOp.Put p => new Dictionary<string, object>
        {
            ["request_put"] = p.Lease is { } lease
                ? new Dictionary<string, object>
                {
                    ["key"] = ToB64(p.Key), ["value"] = ToB64(p.Value), ["lease"] = lease,
                }
                : new Dictionary<string, object> { ["key"] = ToB64(p.Key), ["value"] = ToB64(p.Value) },
        },
        TxnOp.Delete d => new Dictionary<string, object>
        {
            ["request_delete_range"] = d.Prefix
                ? new Dictionary<string, object> { ["key"] = ToB64(d.Key), ["range_end"] = ToB64(PrefixEnd(d.Key)) }
                : new Dictionary<string, object> { ["key"] = ToB64(d.Key) },
        },
        _ => throw new InvalidOperationException($"неизвестная операция txn: {op}"),
    };

    private async Task<T> PostAsync<T>(string endpoint, string path, object body, CancellationToken ct)
    {
        using var response = await httpClient.PostAsJsonAsync(endpoint + path, body, Json, ct);
        if (!response.IsSuccessStatusCode)
        {
            // null-safe: отдельные серверы/заглушки присылают ответ без тела (Content = null)
            var errorBody = response.Content is null
                ? string.Empty
                : await response.Content.ReadAsStringAsync(ct);
            throw new EtcdHttpException(endpoint, (int)response.StatusCode, errorBody);
        }

        return await response.Content.ReadFromJsonAsync<T>(Json, ct)
            ?? throw new EtcdHttpException(endpoint, (int)response.StatusCode, "пустой ответ");
    }

    private static string ToB64(string value)
        => Convert.ToBase64String(Encoding.UTF8.GetBytes(value));

    private static string FromB64(string value)
        => Encoding.UTF8.GetString(Convert.FromBase64String(value));

    // range_end по префиксу: последний байт +1; переполнение 0xFF переносится влево.
    private static string PrefixEnd(string prefix)
    {
        var bytes = Encoding.UTF8.GetBytes(prefix);
        for (var i = bytes.Length - 1; i >= 0; i--)
        {
            if (bytes[i] != 0xFF)
            {
                bytes[i]++;
                return Encoding.UTF8.GetString(bytes[..(i + 1)]);
            }
        }

        return string.Empty; // префикс целиком из 0xFF: пустой range_end = «до конца»
    }

    // DTO ответов: имена полей по фактическим proto-именам etcd 3.5.
    private sealed class RangeResponse
    {
        [JsonPropertyName("kvs")]
        public List<RangeKv>? Kvs { get; set; }
    }

    private sealed class RangeKv
    {
        [JsonPropertyName("key")]
        public string Key { get; set; } = "";

        [JsonPropertyName("value")]
        public string Value { get; set; } = "";

        [JsonPropertyName("mod_revision")]
        public ulong ModRevision { get; set; }
    }

    private sealed class PutResponse
    {
        [JsonPropertyName("header")]
        public object? Header { get; set; }
    }

    private sealed class DeleteResponse
    {
        [JsonPropertyName("deleted")]
        public long Deleted { get; set; }
    }

    private sealed class TxnResponse
    {
        [JsonPropertyName("succeeded")]
        public bool Succeeded { get; set; }
    }

    private sealed class LeaseGrantResponse
    {
        [JsonPropertyName("ID")]
        public long Id { get; set; }
    }

    private sealed class LeaseRevokeResponse
    {
        [JsonPropertyName("header")]
        public object? Header { get; set; }
    }

    private sealed class LeaseKeepaliveResponse
    {
        [JsonPropertyName("result")]
        public LeaseKeepaliveResult? Result { get; set; }
    }

    private sealed class LeaseKeepaliveResult
    {
        [JsonPropertyName("TTL")]
        public long Ttl { get; set; }
    }

    private sealed class StatusResponse
    {
        [JsonPropertyName("header")]
        public StatusHeader? Header { get; set; }
    }

    private sealed class StatusHeader
    {
        // int64 в protojson → decimal-строка; AllowReadingFromString читает как long.
        [JsonPropertyName("revision")]
        public ulong Revision { get; set; }
    }

    private sealed class CompactionResponse
    {
        [JsonPropertyName("header")]
        public object? Header { get; set; }
    }

    private sealed class DefragmentResponse
    {
        [JsonPropertyName("header")]
        public object? Header { get; set; }
    }
}
