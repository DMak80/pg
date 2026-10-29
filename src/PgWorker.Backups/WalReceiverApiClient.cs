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
    // id ЛИШЬ у drill-запуска (arch/19 §3.6): scratch-каталог на ноде
    // backup-drill/<id>/pgdata; restore (null) пишет в PGDATA ноды.
    [property: JsonPropertyName("id")] string? Id,
    [property: JsonPropertyName("target_time")] string? TargetTime,
    [property: JsonPropertyName("recovery_timeout_sec")] int RecoveryTimeoutSec,
    [property: JsonPropertyName("s3")] S3Context S3);

/// <summary>Снимок статуса из GET (поля spec t26 §3.2 / arch/19 §2/§5); id —
/// активная операция (тело 409 на POST тоже несёт её снимок) — сверка «своя/
/// чужая» в 409-ветке запуска: чужой id трактуется как не-наша операция;
/// started_unix — время TryBegin операции приёмника (возрастной бюджет
/// verify воркера, arch/19 §6 — docker-инспектов больше нет).</summary>
public sealed record ReceiverStatus(
    [property: JsonPropertyName("state")] string State,
    [property: JsonPropertyName("phase")] string Phase,
    [property: JsonPropertyName("id")] string? Id = null,
    [property: JsonPropertyName("wal_start_segment")] string? WalStartSegment = null,
    [property: JsonPropertyName("size_bytes")] long? SizeBytes = null,
    [property: JsonPropertyName("restored_to_lsn")] string? RestoredToLsn = null,
    [property: JsonPropertyName("system_id")] string? SystemId = null,
    [property: JsonPropertyName("started_unix")] long? StartedUnix = null,
    [property: JsonPropertyName("error")] string? Error = null);

/// <summary>Тело POST /api/node/pgdump (pg_dump схемы — arch/14 §Move M1).</summary>
public sealed record StartPgDumpRequest(
    [property: JsonPropertyName("schema")] string Schema,
    [property: JsonPropertyName("dbname")] string DbName);

/// <summary>Ответ 200 /api/node/pgdump — текст дампа схемы.</summary>
public sealed record PgDumpResponse([property: JsonPropertyName("ddl")] string Ddl);

/// <summary>Ответ 200 /api/node/hba-ensure: applied — вставлял ли строку
/// (и будил Patroni); bootstrap — нода ещё без PG_VERSION (арх/19 §7).</summary>
public sealed record HbaEnsureResponse(
    [property: JsonPropertyName("applied")] bool Applied,
    [property: JsonPropertyName("bootstrap")] bool Bootstrap);

/// <summary>Ответ 200 GET /api/node/pgdata — проба данных ноды (arch/14 Д3).</summary>
public sealed record PgDataResponse([property: JsonPropertyName("pg_version")] string PgVersion);

/// <summary>Исход node-операции (синхронный контракт): StatusCode 0 —
/// транспортный отказ/таймаут; 5xx — приёмник ответил ошибкой; оба —
/// transient для тиков воркера (как ReceiverReply).</summary>
public sealed record NodeReply<TBody>(int StatusCode, TBody? Body, string? Error)
    where TBody : class
{
    public bool IsTransient => StatusCode is 0 or >= 500;
    public bool IsSuccess => StatusCode is >= 200 and < 300;
}

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

/// <summary>Тело 5xx node-операций приёмника (дубль контракта ErrorResponse
/// ApiContracts.cs приёмника — дублирование записей между проектами
/// осознанное, запрет цикла ссылок).</summary>
public sealed record ErrorResponse([property: JsonPropertyName("error")] string Error);

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
    Task<NodeReply<HbaEnsureResponse>> EnsureHbaAsync(string baseUrl, CancellationToken ct);
    Task<NodeReply<PgDumpResponse>> PgDumpAsync(string baseUrl, StartPgDumpRequest body, CancellationToken ct);
    Task<NodeReply<PgDataResponse>> GetPgDataAsync(string baseUrl, CancellationToken ct);
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

    // Внешний канал операторской отмены ops-операций приёмника (arch/19 §5):
    // воркером используется и как best-effort stop при таймауте verify/drill
    // (arch/19 §6); restore-таймаут сносит первую ноду (операция уходит с ней).
    public async Task<ReceiverReply> CancelOpsAsync(string baseUrl, CancellationToken ct)
        => await PostAsync<object?>($"{baseUrl}/api/ops/cancel", null, ct);

    // Node-операции (arch/25 №22): синхронный контракт — 200 несёт тело
    // итога, 5xx/0 — transient-исход (причина — поле Error).
    public Task<NodeReply<HbaEnsureResponse>> EnsureHbaAsync(string baseUrl, CancellationToken ct)
        => PostNodeAsync<object?, HbaEnsureResponse>($"{baseUrl}/api/node/hba-ensure", null, ct);

    public Task<NodeReply<PgDumpResponse>> PgDumpAsync(string baseUrl, StartPgDumpRequest body, CancellationToken ct)
        => PostNodeAsync<StartPgDumpRequest, PgDumpResponse>($"{baseUrl}/api/node/pgdump", body, ct);

    public async Task<NodeReply<PgDataResponse>> GetPgDataAsync(string baseUrl, CancellationToken ct)
    {
        try
        {
            using var resp = await http.GetAsync($"{baseUrl}/api/node/pgdata", ct);
            var body = await ReadBodyAsync<PgDataResponse>(resp);
            return new NodeReply<PgDataResponse>((int)resp.StatusCode, body, null);
        }
        catch (Exception e)
        {
            return new NodeReply<PgDataResponse>(0, null, e.Message.Split('\n')[0]);
        }
    }

    private async Task<NodeReply<TBody>> PostNodeAsync<TReq, TBody>(
        string url, TReq? body, CancellationToken ct)
        where TBody : class
    {
        try
        {
            using var resp = body is null
                ? await http.PostAsync(url, content: null, ct)
                : await http.PostAsJsonAsync(url, body, JsonOpts, ct);
            var parsed = await ReadBodyAsync<TBody>(resp);
            var error = parsed is null && (int)resp.StatusCode >= 500
                ? await ReadErrorAsync(resp)
                : null;
            return new NodeReply<TBody>((int)resp.StatusCode, parsed, error);
        }
        catch (Exception e)
        {
            return new NodeReply<TBody>(0, null, e.Message.Split('\n')[0]);
        }
    }

    // Тело 200 — итог node-операции; прочие коды тела не несут.
    private static async Task<TBody?> ReadBodyAsync<TBody>(HttpResponseMessage resp)
        where TBody : class
    {
        if ((int)resp.StatusCode is not (200 or 201))
            return null;
        try
        {
            return await resp.Content.ReadFromJsonAsync<TBody>(JsonOpts, default(CancellationToken));
        }
        catch
        {
            return null; // битое тело — код важнее
        }
    }

    // Причина 5xx — однострочный error приёмника (диагностика тика).
    private static async Task<string?> ReadErrorAsync(HttpResponseMessage resp)
    {
        try
        {
            var err = await resp.Content.ReadFromJsonAsync<ErrorResponse>(JsonOpts, default(CancellationToken));
            return err?.Error;
        }
        catch
        {
            return null;
        }
    }

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
/// pgw-&lt;C&gt;-&lt;X&gt;-&lt;n&gt;, object — имя её контейнера); ВСЕ операции
/// бэкапов (полный/verify/restore/drill) — команды на приёмники нод.</summary>
public static class WalReceiverAddress
{
    /// <summary>Базовый URL API приёмника ноды: имя docker-контейнера :8080
    /// (канонические — pgw-&lt;C&gt;-&lt;X&gt;-&lt;n&gt;; object — NodeAddress.Object).</summary>
    public static string ForNode(string cluster, string shard, string node, string? objectContainer = null)
    {
        var container = objectContainer is { Length: > 0 } o
            ? o
            : $"pgw-{cluster}-{shard}-{node}";
        return $"http://{container}:8080";
    }
}
