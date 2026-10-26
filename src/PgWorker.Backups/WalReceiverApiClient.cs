using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace PgWorker.Backups;

// JSON-контракты клиента — те же snake_case-имена, что у ApiContracts.cs
// приёмника (PgWorker.WalReceiver); дублирование записей между проектами —
// осознанное (запрет цикла ссылок: WalReceiver → Backups, обратной нет).
// Фактическая сверка контракта — API-тесты маршрутов приёмника (реальный
// HTTP-обмен).

/// <summary>Тело POST /api/backup/full (запуск операции полного).</summary>
public sealed record StartFullRequest([property: JsonPropertyName("id")] string Id);

/// <summary>S3-контекст tool-операций (секреты — ТОЛЬКО в POST-теле, arch/19 §7).</summary>
public sealed record S3Context(
    [property: JsonPropertyName("endpoints")] string[] Endpoints,
    [property: JsonPropertyName("access_key")] string AccessKey,
    [property: JsonPropertyName("secret_key")] string SecretKey,
    [property: JsonPropertyName("bucket")] string Bucket,
    [property: JsonPropertyName("prefix")] string Prefix,
    [property: JsonPropertyName("ca_pem")] string? CaPem = null);

public sealed record StartVerifyRequest(
    [property: JsonPropertyName("backup_id")] string BackupId,
    [property: JsonPropertyName("s3")] S3Context S3);

public sealed record StartRestoreRequest(
    [property: JsonPropertyName("backup_id")] string BackupId,
    [property: JsonPropertyName("target_time")] string? TargetTime,
    [property: JsonPropertyName("recovery_timeout_sec")] int RecoveryTimeoutSec,
    [property: JsonPropertyName("s3")] S3Context S3);

/// <summary>Снимок статуса из GET (поля spec t26 §3.2 / arch/19 §2/§5); id —
/// активная операция (тело 409 на POST тоже несёт её снимок) — сверка «своя/
/// чужая» в 409-ветке запуска: чужой id трактуется как не-наша операция.</summary>
public sealed record ReceiverStatus(
    [property: JsonPropertyName("state")] string State,
    [property: JsonPropertyName("phase")] string Phase,
    [property: JsonPropertyName("id")] string? Id = null,
    [property: JsonPropertyName("wal_start_segment")] string? WalStartSegment = null,
    [property: JsonPropertyName("size_bytes")] long? SizeBytes = null,
    [property: JsonPropertyName("restored_to_lsn")] string? RestoredToLsn = null,
    [property: JsonPropertyName("system_id")] string? SystemId = null,
    [property: JsonPropertyName("error")] string? Error = null);

/// <summary>Исход вызова API приёмника: StatusCode 0 = транспортный отказ/таймаут;
/// 5xx = приёмник ответил ошибкой; оба — transient для тиков воркера.</summary>
public sealed record ReceiverReply(int StatusCode, ReceiverStatus? Status, string? Error)
{
    public bool IsTransient => StatusCode is 0 or >= 500;
    public bool IsNotFound => StatusCode == 404;
    public bool IsConflict => StatusCode == 409;
    public bool IsGated => StatusCode == 503;
    public bool IsAccepted => StatusCode == 202;
}

/// <summary>Клиент HTTP API приёмника WalReceiver (spec t26 §3.2):
/// baseUrl — базовый URL приёмника (нода — alias:8080, tool — host:порт);
/// любой транспортный отказ/таймаут/5xx — transient-исход (IsTransient),
/// исключения наружу не пробрасываются.</summary>
public interface IWalReceiverApiClient
{
    Task<ReceiverReply> StartFullAsync(string baseUrl, string id, CancellationToken ct);
    Task<ReceiverReply> GetFullAsync(string baseUrl, CancellationToken ct);
    Task<ReceiverReply> CancelFullAsync(string baseUrl, CancellationToken ct);
    Task<ReceiverReply> StartVerifyAsync(string baseUrl, StartVerifyRequest body, CancellationToken ct);
    Task<ReceiverReply> GetVerifyAsync(string baseUrl, CancellationToken ct);
    Task<ReceiverReply> StartRestoreAsync(string baseUrl, StartRestoreRequest body, CancellationToken ct);
    Task<ReceiverReply> GetRestoreAsync(string baseUrl, CancellationToken ct);
    Task<ReceiverReply> CancelOpsAsync(string baseUrl, CancellationToken ct);
}

/// <summary>Прод-клиент (HttpClient с Timeout 10 с — per-вызов ~10 с, spec
/// §3.5): JSON POST/GET через System.Text.Json; 200/202 — тело в
/// ReceiverStatus; прочие коды — только StatusCode; исключение транспорта
/// (HttpRequestException/TaskCanceled) → ReceiverReply(0) — НЕ исключение
/// наружу.</summary>
public sealed class WalReceiverApiClient(HttpClient http) : IWalReceiverApiClient
{
    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    public async Task<ReceiverReply> StartFullAsync(string baseUrl, string id, CancellationToken ct)
        => await PostAsync($"{baseUrl}/api/backup/full", new StartFullRequest(id), ct);

    public async Task<ReceiverReply> GetFullAsync(string baseUrl, CancellationToken ct)
        => await GetAsync($"{baseUrl}/api/backup/full", ct);

    public async Task<ReceiverReply> CancelFullAsync(string baseUrl, CancellationToken ct)
        => await PostAsync<object?>($"{baseUrl}/api/backup/full/cancel", null, ct);

    public async Task<ReceiverReply> StartVerifyAsync(string baseUrl, StartVerifyRequest body, CancellationToken ct)
        => await PostAsync($"{baseUrl}/api/ops/verify", body, ct);

    public async Task<ReceiverReply> GetVerifyAsync(string baseUrl, CancellationToken ct)
        => await GetAsync($"{baseUrl}/api/ops/verify", ct);

    public async Task<ReceiverReply> StartRestoreAsync(string baseUrl, StartRestoreRequest body, CancellationToken ct)
        => await PostAsync($"{baseUrl}/api/ops/restore", body, ct);

    public async Task<ReceiverReply> GetRestoreAsync(string baseUrl, CancellationToken ct)
        => await GetAsync($"{baseUrl}/api/ops/restore", ct);

    public async Task<ReceiverReply> CancelOpsAsync(string baseUrl, CancellationToken ct)
        => await PostAsync<object?>($"{baseUrl}/api/ops/cancel", null, ct);

    private async Task<ReceiverReply> GetAsync(string url, CancellationToken ct)
    {
        try
        {
            using var resp = await http.GetAsync(url, ct);
            var status = await ReadStatusAsync(resp);
            return new ReceiverReply((int)resp.StatusCode, status, null);
        }
        catch (Exception e)
        {
            return new ReceiverReply(0, null, e.Message.Split('\n')[0]);
        }
    }

    private async Task<ReceiverReply> PostAsync<T>(string url, T? body, CancellationToken ct)
    {
        try
        {
            using var resp = body is null
                ? await http.PostAsync(url, content: null, ct)
                : await http.PostAsJsonAsync(url, body, JsonOpts, ct);
            var status = await ReadStatusAsync(resp);
            return new ReceiverReply((int)resp.StatusCode, status, null);
        }
        catch (Exception e)
        {
            return new ReceiverReply(0, null, e.Message.Split('\n')[0]);
        }
    }

    // Тело 200/202 — статус операции; 409 — снимок активной (id — корреляция
    // «своя/чужая» в 409-ветке запуска); прочие коды тела не разбираем.
    private static async Task<ReceiverStatus?> ReadStatusAsync(HttpResponseMessage resp)
    {
        if ((int)resp.StatusCode is not (200 or 202 or 409))
            return null;
        try
        {
            return await resp.Content.ReadFromJsonAsync<ReceiverStatus>(JsonOpts, default(CancellationToken));
        }
        catch
        {
            return null; // битое тело — код важнее
        }
    }
}

/// <summary>Резолв адреса API приёмника (arch/19 §2/§3.5, arch/14 §2.4):
/// нода — имя docker-контейнера :8080 в общей сети воркера и нод (канонические
/// pgw-&lt;C&gt;-&lt;X&gt;-&lt;n&gt;, object — имя её контейнера); tool-контейнер —
/// контур: имя :8080 в сети контура, прод: host:динамический порт из инспекта
/// публикации :8080.</summary>
public static class WalReceiverAddress
{
    /// <summary>Базовый URL API приёмника ноды: имя docker-контейнера :8080
    /// (канонические — pgw-&lt;C&gt;-&lt;X&gt;-&lt;n&gt;; object — NodeAddress.Object).</summary>
    public static string ForNode(string cluster, string shard, string node, string? objectContainer)
    {
        var container = objectContainer is { Length: > 0 } o
            ? o
            : $"pgw-{cluster}-{shard}-{node}";
        return $"http://{container}:8080";
    }

    /// <summary>Базовый URL API tool-контейнера: контур (ContourNetwork задан) —
    /// имя контейнера :8080 в сети контура; прод — host:&lt;динамический порт&gt;
    /// из инспекта публикации :8080.</summary>
    public static string ForTool(string containerName, string host, int? publishedPort, bool contourMode)
        => contourMode ? $"http://{containerName}:8080" : $"http://{host}:{publishedPort ?? 0}";
}
