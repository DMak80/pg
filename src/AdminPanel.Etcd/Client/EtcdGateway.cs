using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Net.Http.Json;
using AdminPanel.Core;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Etcd.Client;

// HTTP-ошибка gateway: не-2xx от /v3/*.
public sealed class EtcdHttpException(string endpoint, int statusCode, string body)
    : Exception($"etcd {endpoint} ответил {statusCode}: {body}");

// Все живые endpoints не ответили (после failover).
public sealed class EtcdUnreachableException(string message) : Exception(message);

// Реализация IEtcdGateway: HttpClient из IHttpClientFactory (именованный "etcd", ModuleExtensions).
// Таймаут задаётся конфигурацией клиента, не здесь (spec §4.2).
[InjectAsSingleton(typeof(IEtcdGateway))]
public sealed class EtcdGateway(HttpClient httpClient) : IEtcdGateway
{
    public const string HttpClientName = "etcd";

    // etcd gateway сериализует int64 decimal-строками и не приводит proto-имена к camelCase (spec §3.17).
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

    public async Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
    {
        var result = await Result<StatusResponse>.FromAsync(
            async () => await PostAsync<StatusResponse>(endpoint, "/v3/maintenance/status", new { }, ct));
        return result.Map(r => new EtcdStatusPayload(r.Version, r.DbSize, r.Leader, r.RaftIndex, r.RaftTerm));
    }

    public async Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
    {
        var result = await Result<MemberListResponse>.FromAsync(
            async () => await PostAsync<MemberListResponse>(endpoint, "/v3/cluster/member/list", new { }, ct));
        return result.Map(r => (IReadOnlyList<EtcdMember>)(r.Members ?? [])
            .Select(m => new EtcdMember(m.Id, m.Name, m.PeerUrls ?? [], m.ClientUrls ?? []))
            .ToList());
    }

    public async Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
    {
        var result = await Result<AlarmResponse>.FromAsync(
            async () => await PostAsync<AlarmResponse>(endpoint, "/v3/maintenance/alarm", new { }, ct));
        return result.Map(r => (IReadOnlyList<EtcdAlarm>)(r.Alarms ?? [])
            .Select(a => new EtcdAlarm(a.MemberId, a.Type))
            .ToList());
    }

    public async Task<Result<TxnResult>> TxnAsync(
        string endpoint, IReadOnlyList<TxnCompare> compares, IReadOnlyList<KvPut> puts, CancellationToken ct)
    {
        var body = new
        {
            compare = compares.Select(c => new { key = ToB64(c.Key), version = c.Version }),
            success = puts.Select(p => new
            {
                request_put = new { key = ToB64(p.Key), value = ToB64(p.Value) },
            }),
        };
        var result = await Result<TxnResponse>.FromAsync(
            async () => await PostAsync<TxnResponse>(endpoint, "/v3/kv/txn", body, ct));
        return result.Map(r => new TxnResult(r.Succeeded));
    }

    public async Task<Result> PutAsync(string endpoint, string key, string value, CancellationToken ct)
    {
        var body = new { key = ToB64(key), value = ToB64(value) };
        return await Result.FromAsync(
            async () => await PostAsync<StatusResponse>(endpoint, "/v3/kv/put", body, ct));
    }

    public async Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
    {
        object body = prefix
            ? new { key = ToB64(keyOrPrefix), range_end = ToB64(PrefixEnd(keyOrPrefix)) }
            : new { key = ToB64(keyOrPrefix) };
        return await Result.FromAsync(
            async () => await PostAsync<StatusResponse>(endpoint, "/v3/kv/deleterange", body, ct));
    }

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

    // range_end по префиксу: последний байт +1; переполнение 0xFF переносится влево (spec §4.2).
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

    // DTO ответов: имена полей по фактическим proto-именам etcd 3.5 (mod_revision/dbSize/peerURLs…).
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

    private sealed class StatusResponse
    {
        [JsonPropertyName("version")]
        public string? Version { get; set; }

        [JsonPropertyName("dbSize")]
        public long? DbSize { get; set; }

        [JsonPropertyName("leader")]
        public ulong? Leader { get; set; }

        [JsonPropertyName("raftIndex")]
        public ulong? RaftIndex { get; set; }

        [JsonPropertyName("raftTerm")]
        public ulong? RaftTerm { get; set; }
    }

    private sealed class MemberListResponse
    {
        [JsonPropertyName("members")]
        public List<MemberDto>? Members { get; set; }
    }

    private sealed class MemberDto
    {
        [JsonPropertyName("ID")]
        public ulong Id { get; set; }

        [JsonPropertyName("name")]
        public string? Name { get; set; }

        [JsonPropertyName("peerURLs")]
        public List<string>? PeerUrls { get; set; }

        [JsonPropertyName("clientURLs")]
        public List<string>? ClientUrls { get; set; }
    }

    private sealed class AlarmResponse
    {
        [JsonPropertyName("alarms")]
        public List<AlarmDto>? Alarms { get; set; }
    }

    private sealed class TxnResponse
    {
        [JsonPropertyName("succeeded")]
        public bool Succeeded { get; set; }
    }

    private sealed class AlarmDto
    {
        [JsonPropertyName("memberID")]
        public ulong MemberId { get; set; }

        [JsonPropertyName("alarm")]
        public EtcdAlarmType Type { get; set; }
    }
}
