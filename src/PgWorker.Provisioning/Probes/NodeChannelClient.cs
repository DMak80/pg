using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace PgWorker.Provisioning.Probes;

/// <summary>Адрес mTLS-канала агента ноды (arch/25 №23): порт :8081 ВНУТРИ
/// docker-сети кластера, на хост не публикуется; имя — полное docker-имя ноды
/// (SAN серта, прецедент WalReceiverAddress).</summary>
public static class NodeChannelAddress
{
    public static string ForNode(string cluster, string shard, string node)
        => $"https://pgw-{cluster}-{shard}-{node}:8081";
}

/// <summary>Исход команды канала: StatusCode 0 — транспорт (включая отказ
/// TLS до чтения ключа агентом); 2xx — успех (Ok / applied_revision);
/// 400/409/5xx — transient для тиков воркера (ретрай тиком, spec §3.4).
/// Секретные значения в Error не попадают.</summary>
public sealed record ChannelReply(int StatusCode, bool Ok, long? AppliedRevision, string? Error)
{
    public bool IsSuccess => StatusCode is >= 200 and < 300;
    public bool IsTransient => StatusCode is 0 or >= 400;
}

/// <summary>mTLS-клиент канала доставки (spec §3.4): HandshakeAsync — команда
/// доставки (хендшейк старта, тело с per-install паролями PG + is_tool_start);
/// ApplyAsync — команда применения (ротации/замены, тело пустое, только
/// ready-нодам; воркер различает состояния по статусному ключу etcd).</summary>
public interface INodeChannelClient
{
    Task<ChannelReply> HandshakeAsync(
        string baseUrl, string suPassword, string standbyPassword,
        bool isToolStart, CancellationToken ct);
    Task<ChannelReply> ApplyAsync(string baseUrl, CancellationToken ct);
}

public sealed class NodeChannelClient(HttpClient http) : INodeChannelClient
{
    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    public async Task<ChannelReply> HandshakeAsync(
        string baseUrl, string suPassword, string standbyPassword,
        bool isToolStart, CancellationToken ct)
    {
        try
        {
            using var resp = await http.PostAsJsonAsync($"{baseUrl}/api/bootstrap",
                new WorkerPushBody(suPassword, standbyPassword, isToolStart), JsonOpts, ct);
            return await ReadAsync(resp, ct);
        }
        catch (Exception e)
        {
            return new ChannelReply(0, false, null, e.Message.Split('\n')[0]);
        }
    }

    public async Task<ChannelReply> ApplyAsync(string baseUrl, CancellationToken ct)
    {
        try
        {
            using var resp = await http.PostAsync($"{baseUrl}/api/bootstrap/apply", content: null, ct);
            return await ReadAsync(resp, ct);
        }
        catch (Exception e)
        {
            return new ChannelReply(0, false, null, e.Message.Split('\n')[0]);
        }
    }

    private static async Task<ChannelReply> ReadAsync(HttpResponseMessage resp, CancellationToken ct)
    {
        Console.Error.WriteLine(
            $"pgnode-channel: {(resp.RequestMessage?.Method.Method)} {resp.RequestMessage?.RequestUri} → {(int)resp.StatusCode}");
        if (!resp.IsSuccessStatusCode)
            return new ChannelReply((int)resp.StatusCode, false, null, null);
        try
        {
            var ok = await resp.Content.ReadFromJsonAsync<CommandBody>(JsonOpts, ct);
            return new ChannelReply((int)resp.StatusCode, true, ok?.AppliedRevision, null);
        }
        catch (JsonException)
        {
            return new ChannelReply((int)resp.StatusCode, false, null, "bad body");
        }
    }

    private sealed record WorkerPushBody(
        [property: JsonPropertyName("su_password")] string SuPassword,
        [property: JsonPropertyName("standby_password")] string StandbyPassword,
        [property: JsonPropertyName("is_tool_start")] bool IsToolStart);

    private sealed record CommandBody(
        [property: JsonPropertyName("applied_revision")] long? AppliedRevision);
}
