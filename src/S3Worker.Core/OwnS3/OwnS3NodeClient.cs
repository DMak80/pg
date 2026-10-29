using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Amazon;
using Amazon.S3;
using OwnS3.Protocol.Auth;
using OwnS3.Protocol.PeerAuth;

namespace S3Worker.Core.OwnS3;

/// <summary>Занятость диска ноды из админ-грани GET /admin/storage
/// (owns3/05 §7 после Task 1: camelCase, null-поля → Failed — транзиент).</summary>
public sealed record OwnS3NodeUsage(long UsedBytes, long TotalBytes);

/// <summary>Состав mesh из GET /admin/replication/status (owns3/07 §10):
/// G — имена соседей (NeighborNames, проекция Neighbors); зонд F — per-neighbor
/// счётчики доставки (fetchQueueDepth и др.). Neighbors=[] по умолчанию —
/// обратная совместимость существующих вызовов (init-свойство, т.к. дефолт []
/// у позиционного параметра запрещён компилятором — CS1736).</summary>
public sealed record OwnS3ReplicationStatus(bool Active, IReadOnlyList<string> NeighborNames)
{
    public IReadOnlyList<OwnS3ReplicationNeighbor> Neighbors { get; init; } = [];
}

/// <summary>Один сосед из GET /admin/replication/status (owns3/07 §10):
/// зонд F — fetch-очередь доставки (lag_objects); остальные счётчики —
/// для будущего наблюдения. Null-поля грани → дефолты (0/false/null).</summary>
public sealed record OwnS3ReplicationNeighbor(
    string Name, long FetchQueueDepth, double? FetchLagSeconds, bool FetchPaused, bool ReconcileRunning);

/// <summary>Ответ POST /admin/replication/probe (owns3/07 §2): активность
/// репликации и живость настроенных линков (пинг соседей — не-мутирующий).</summary>
public sealed record OwnS3ProbeReport(bool Active, IReadOnlyList<OwnS3ProbeTarget> Targets);

/// <summary>Строка probe-отчёта (owns3/07 §2): rttMs присутствует только при
/// полученном ответе пира (null — таймаут/не отвечал).</summary>
public sealed record OwnS3ProbeTarget(string Name, bool Reachable, long? RttMs = null);

/// <summary>Страница GET /admin/replication/sync-report (owns3/07 §10): свёртка
/// листинга страницы (XOR FNV-1a пар (key, modTimeUnixMs)); nextMarker null/пусто — конец.</summary>
public sealed record OwnS3SyncReportPage(
    int Count, string? FirstKey, long FirstModTime, string? LastKey, long LastModTime,
    long TotalSize, string AggregateHash, string? NextMarker);

/// <summary>Запись листинга S3-грани ListObjectsV2: элемент сверки зонда —
/// (Key, ModTimeUnixMs) — та же пара, что в свёртке sync-report.</summary>
public sealed record OwnS3ListEntry(string Key, long ModTimeUnixMs, long Size);

/// <summary>Страница ListObjectsV2: nextContinuationToken null/пусто — конец листинга.</summary>
public sealed record OwnS3ListPage(IReadOnlyList<OwnS3ListEntry> Entries, string? NextContinuationToken);

/// <summary>Исходы admin-join (arch/24 §5 G): Joined — готово (имя пира из ответа);
/// Conflict — имя числится с другим ключом (409); Refused — отказ (403, в т.ч.
/// deny); Transient — прочие HTTP/транспорт/таймаут (ретрай тиком).</summary>
public enum OwnS3JoinKind
{
    Joined,
    Conflict,
    Refused,
    Transient,
}

public sealed record OwnS3JoinOutcome(OwnS3JoinKind Kind, string? PeerName = null);

/// <summary>
/// Грань ownS3-ноды для процессов S3Worker (arch/23 §5): health-проба
/// (без подписи), существование/создание bucket'а (AWSSDK, root-креды
/// профиля), занятость диска (SigV4 GET /admin/storage); mesh-управление
/// (arch/24 §5 G, spec §5.1): status/join/delete-target (SigV4 root-креды
/// админ-грани) и deny-перезапись (до-join контур OWNS3-PEER сертификатом
/// оркестратора — owns3/07 §6). Все таймауты — 7.5 с (поллинг-инвариант:
/// половина окна watchdog); ретраи — дело тика.
/// </summary>
public interface IOwnS3NodeClient
{
    // GET /healthz (без подписи): 200 → true; таймаут/5xx/404 → false
    // (транзиент, не исключение наружу).
    Task<Result<bool>> HealthAsync(string host, int port, CancellationToken ct);

    // exists-проба: ListBuckets root-кредом → имя в списке (HeadBucket через
    // HttpListener-стабы и GetBucketLocation ownS3 (LocationConstraint без
    // проверки) ненадёжны для различения отсутствующего bucket).
    Task<Result<bool>> BucketExistsAsync(string host, int port, OwnS3NodeSecrets creds, string bucket, CancellationToken ct);

    // Идемпотентно: exists → Success(skip); PutBucket;
    // 409 BucketAlreadyOwnedByYou → Success (owns3/02 CreateBucket).
    Task<Result> EnsureBucketAsync(string host, int port, OwnS3NodeSecrets creds, string bucket, CancellationToken ct);

    // SigV4 GET /admin/storage root-кредом → usedBytes/totalBytes;
    // null-поля/не-2xx → Failed (транзиент).
    Task<Result<OwnS3NodeUsage>> GetUsageAsync(string host, int port, OwnS3NodeSecrets creds, CancellationToken ct);

    // SigV4 GET /admin/replication/status → active + имена соседей (состав targets).
    Task<Result<OwnS3ReplicationStatus>> GetReplicationStatusAsync(string host, int port, OwnS3NodeSecrets creds, CancellationToken ct);

    // SigV4 POST /admin/replication/join {"endpoint": …} → исход (§3.5 G).
    Task<Result<OwnS3JoinOutcome>> AdminJoinAsync(string host, int port, OwnS3NodeSecrets creds, string endpoint, CancellationToken ct);

    // SigV4 DELETE /admin/replication/targets/{name}: 202 → true; 404 → false
    // (идемпотентность); прочее → Failed (транзиент).
    Task<Result<bool>> DeleteTargetAsync(string host, int port, OwnS3NodeSecrets creds, string name, CancellationToken ct);

    // PUT /replication/denylist (до-join контур, подпись OWNS3-PEER сертификатом
    // оркестратора): 202 → true; 403 → false (trust не принят — transient тика);
    // 400/транспорт → Failed.
    Task<Result<bool>> PutDenyListAsync(
        string host, int port, IReadOnlyList<(string Name, string PublicKeyPem)> entries,
        IssuedPkiMaterial orchestrator, CancellationToken ct);

    // POST /admin/replication/probe (без target — все соседи; owns3/07 §2):
    // 200 → отчёт пинга настроенных линков; транспорт/не-2xx → Failed
    // (транзиент — нода «не опрошена», зонд §3.6.1).
    Task<Result<OwnS3ProbeReport>> ProbeAsync(string host, int port, OwnS3NodeSecrets creds, CancellationToken ct);

    // GET /admin/replication/sync-report?bucket=&marker= (owns3/07 §10):
    // 200 → страница свёртки листинга; nextMarker null/пусто — конец;
    // не-2xx/транспорт/битый JSON → Failed (транзиент).
    Task<Result<OwnS3SyncReportPage>> GetSyncReportPageAsync(string host, int port, OwnS3NodeSecrets creds, string bucket, string? marker, CancellationToken ct);

    // ListObjectsV2 (AWSSDK, root-креды, по образцу EnsureBucketAsync):
    // страница записей (key, modTimeUnixMs, size); отказ листинга → Failed.
    Task<Result<OwnS3ListPage>> ListObjectEntriesAsync(string host, int port, OwnS3NodeSecrets creds, string bucket, string? continuationToken, CancellationToken ct);
}

/// <summary>Реализация: именованный HttpClient (Program: "owns3-node",
/// timeout 7.5 c) + AmazonS3Client per-node (ForcePathStyle, retries off —
/// ретраи делает тик).</summary>
public sealed class OwnS3NodeClient(HttpClient http) : IOwnS3NodeClient
{
    // Контракт поллинг-инварианта для юнит-теста (Program задаёт 7.5 c
    // именованному клиенту "owns3-node").
    internal TimeSpan Timeout => http.Timeout;

    public async Task<Result<bool>> HealthAsync(string host, int port, CancellationToken ct)
    {
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, $"http://{host}:{port}/healthz");
            using var response = await http.SendAsync(request, ct);
            return Result<bool>.Success(response.IsSuccessStatusCode);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            // таймаут HttpClient — молчание ноды (транзиент, не ошибка контура)
            return Result<bool>.Success(false);
        }
        catch (HttpRequestException)
        {
            // транспорт (отказ/рестарт ноды) — то же молчание
            return Result<bool>.Success(false);
        }
    }

    public async Task<Result<bool>> BucketExistsAsync(
        string host, int port, OwnS3NodeSecrets creds, string bucket, CancellationToken ct)
    {
        try
        {
            using var s3 = NewS3Client(host, port, creds);
            var list = await s3.ListBucketsAsync(ct);
            return Result<bool>.Success(
                list.Buckets.Any(b => string.Equals(b.BucketName, bucket, StringComparison.Ordinal)));
        }
        catch (AmazonS3Exception)
        {
            return Result<bool>.Success(false); // ошибка листинга не утверждает exists
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return Result<bool>.Failed(new ApplicationException($"bucket-probe {host}:{port}: timeout"));
        }
        catch (Exception ex)
        {
            return Result<bool>.Failed(new ApplicationException($"bucket-probe {host}:{port}: {ex.Message}", ex));
        }
    }

    public async Task<Result> EnsureBucketAsync(
        string host, int port, OwnS3NodeSecrets creds, string bucket, CancellationToken ct)
    {
        var exists = await BucketExistsAsync(host, port, creds, bucket, ct);
        if (!exists.IsSuccess)
            return Result.Failed(exists.Error!);
        if (exists.Value)
            return Result.Success(); // идемпотентность по факту

        try
        {
            using var s3 = NewS3Client(host, port, creds);
            await s3.PutBucketAsync(new Amazon.S3.Model.PutBucketRequest { BucketName = bucket }, ct);
            return Result.Success();
        }
        catch (AmazonS3Exception ex) when (
            ex.ErrorCode is "BucketAlreadyOwnedByYou" or "BucketAlreadyExists")
        {
            // owns3/02 CreateBucket: повторное создание — не ошибка
            return Result.Success();
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return Result.Failed(new ApplicationException($"bucket-ensure {host}:{port}: timeout"));
        }
        catch (Exception ex)
        {
            return Result.Failed(new ApplicationException($"bucket-ensure {host}:{port}: {ex.Message}", ex));
        }
    }

    public async Task<Result<OwnS3NodeUsage>> GetUsageAsync(
        string host, int port, OwnS3NodeSecrets creds, CancellationToken ct)
    {
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, $"http://{host}:{port}/admin/storage");
            var signed = SigV4GetSigner.Sign(
                "/admin/storage", $"{host}:{port}", creds.RootUser, creds.RootPassword);
            request.Headers.TryAddWithoutValidation("Authorization", signed.Authorization);
            request.Headers.TryAddWithoutValidation("x-amz-date", signed.AmzDate);
            request.Headers.TryAddWithoutValidation("x-amz-content-sha256", signed.ContentSha256);

            using var response = await http.SendAsync(request, ct);
            if (!response.IsSuccessStatusCode)
            {
                var errorBody = await response.Content.ReadAsStringAsync(ct);
                return Result<OwnS3NodeUsage>.Failed(new ApplicationException(
                    $"usage {host}:{port}: HTTP {(int)response.StatusCode} {(string.IsNullOrWhiteSpace(errorBody) ? "" : errorBody.Length > 300 ? errorBody[..300] : errorBody)}"));
            }

            var body = await response.Content.ReadAsStringAsync(ct);
            using var doc = System.Text.Json.JsonDocument.Parse(body);
            if (!doc.RootElement.TryGetProperty("usedBytes", out var used)
                || !doc.RootElement.TryGetProperty("totalBytes", out var total)
                || used.ValueKind != System.Text.Json.JsonValueKind.Number
                || total.ValueKind != System.Text.Json.JsonValueKind.Number)
            {
                // null-поля грани (диск недоступен) — транзиент (spec §5 C)
                return Result<OwnS3NodeUsage>.Failed(new ApplicationException(
                    $"usage {host}:{port}: disk fields missing/null"));
            }

            return Result<OwnS3NodeUsage>.Success(
                new OwnS3NodeUsage(used.GetInt64(), total.GetInt64()));
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return Result<OwnS3NodeUsage>.Failed(new ApplicationException($"usage {host}:{port}: timeout"));
        }
        catch (Exception ex) when (ex is HttpRequestException or System.Text.Json.JsonException)
        {
            return Result<OwnS3NodeUsage>.Failed(new ApplicationException($"usage {host}:{port}: {ex.Message}", ex));
        }
    }

    public async Task<Result<OwnS3ReplicationStatus>> GetReplicationStatusAsync(
        string host, int port, OwnS3NodeSecrets creds, CancellationToken ct)
    {
        try
        {
            using var request = SignAdmin("GET", host, port, "/admin/replication/status", creds);
            using var response = await http.SendAsync(request, ct);
            if (!response.IsSuccessStatusCode)
                return FailedHttp<OwnS3ReplicationStatus>("replication-status", host, port, response);

            var body = await response.Content.ReadAsStringAsync(ct);
            using var doc = JsonDocument.Parse(body);
            var root = doc.RootElement;
            var active = root.TryGetProperty("active", out var a) && a.ValueKind == JsonValueKind.True;
            var neighbors = new List<string>();
            var details = new List<OwnS3ReplicationNeighbor>();
            if (root.TryGetProperty("neighbors", out var list) && list.ValueKind == JsonValueKind.Array)
            {
                foreach (var n in list.EnumerateArray())
                {
                    if (n.TryGetProperty("name", out var name) && name.ValueKind == JsonValueKind.String)
                    {
                        var neighborName = name.GetString()!;
                        neighbors.Add(neighborName);
                        // Per-neighbor счётчики (owns3/07 §10): null/нет поля → дефолты
                        // (0/false/null) — толерантность к старым инстансам.
                        details.Add(new OwnS3ReplicationNeighbor(
                            neighborName,
                            LongOr(n, "fetchQueueDepth", 0),
                            DoubleOrNull(n, "fetchLagSeconds"),
                            BoolOr(n, "fetchPaused", false),
                            BoolOr(n, "reconcileRunning", false)));
                    }
                }
            }

            return Result<OwnS3ReplicationStatus>.Success(
                new OwnS3ReplicationStatus(active, neighbors) { Neighbors = details });
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return Result<OwnS3ReplicationStatus>.Failed(
                new ApplicationException($"replication-status {host}:{port}: timeout"));
        }
        catch (Exception ex) when (ex is HttpRequestException or JsonException)
        {
            return Result<OwnS3ReplicationStatus>.Failed(
                new ApplicationException($"replication-status {host}:{port}: {ex.Message}", ex));
        }
    }

    // POST /admin/replication/probe (owns3/07 §2, зонд §3.6.1): пинг настроенных
    // линков ноды — синхронный отчёт (probe не меняет состояние). Отказ опроса —
    // Failed (транзиент: нода «не опрошена», реконструкции нет).
    public async Task<Result<OwnS3ProbeReport>> ProbeAsync(string host, int port, OwnS3NodeSecrets creds, CancellationToken ct)
    {
        try
        {
            using var request = SignAdmin("POST", host, port, "/admin/replication/probe", creds, Array.Empty<byte>());
            using var response = await http.SendAsync(request, ct);
            if (!response.IsSuccessStatusCode)
                return FailedHttp<OwnS3ProbeReport>("probe", host, port, response);

            var body = await response.Content.ReadAsStringAsync(ct);
            using var doc = JsonDocument.Parse(body);
            var root = doc.RootElement;
            var active = root.TryGetProperty("active", out var act) && act.ValueKind == JsonValueKind.True;
            var targets = new List<OwnS3ProbeTarget>();
            if (root.TryGetProperty("targets", out var list) && list.ValueKind == JsonValueKind.Array)
            {
                foreach (var t in list.EnumerateArray())
                {
                    // rttMs может отсутствовать/null (пир не ответил) → null.
                    long? rtt = t.TryGetProperty("rttMs", out var rttEl) && rttEl.ValueKind == JsonValueKind.Number
                        ? rttEl.GetInt64()
                        : null;
                    if (t.TryGetProperty("name", out var name) && name.ValueKind == JsonValueKind.String
                        && t.TryGetProperty("reachable", out var reachable)
                        && (reachable.ValueKind == JsonValueKind.True || reachable.ValueKind == JsonValueKind.False))
                        targets.Add(new OwnS3ProbeTarget(name.GetString()!, reachable.GetBoolean(), rtt));
                }
            }

            return Result<OwnS3ProbeReport>.Success(new OwnS3ProbeReport(active, targets));
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return Result<OwnS3ProbeReport>.Failed(new ApplicationException($"probe {host}:{port}: timeout"));
        }
        catch (Exception ex) when (ex is HttpRequestException or JsonException)
        {
            return Result<OwnS3ProbeReport>.Failed(new ApplicationException($"probe {host}:{port}: {ex.Message}", ex));
        }
    }

    // GET /admin/replication/sync-report?bucket=&marker= (owns3/07 §10, зонд §3.6.2):
    // страница свёртки листинга. Query канонизуется и входит в подпись
    // (серверный CanonicalRequestBuilder включает RawQuery — без этого SignatureDoesNotMatch).
    public async Task<Result<OwnS3SyncReportPage>> GetSyncReportPageAsync(
        string host, int port, OwnS3NodeSecrets creds, string bucket, string? marker, CancellationToken ct)
    {
        try
        {
            // Значения null/пусто не шлются (первая страница — без marker).
            var query = SigV4GetSigner.CanonicalQuery(
                ("bucket", bucket), ("marker", string.IsNullOrEmpty(marker) ? null : marker));
            using var request = SignAdmin("GET", host, port, "/admin/replication/sync-report", creds, canonicalQuery: query);
            using var response = await http.SendAsync(request, ct);
            if (!response.IsSuccessStatusCode)
                return FailedHttp<OwnS3SyncReportPage>("sync-report", host, port, response);

            var body = await response.Content.ReadAsStringAsync(ct);
            using var doc = JsonDocument.Parse(body);
            var root = doc.RootElement;
            var nextMarker = StrOrNull(root, "nextMarker");
            return Result<OwnS3SyncReportPage>.Success(new OwnS3SyncReportPage(
                (int)LongOr(root, "count", 0),
                StrOrNull(root, "firstKey"),
                LongOr(root, "firstModTime", 0),
                StrOrNull(root, "lastKey"),
                LongOr(root, "lastModTime", 0),
                LongOr(root, "totalSize", 0),
                StrOrNull(root, "aggregateHash") ?? "",
                string.IsNullOrEmpty(nextMarker) ? null : nextMarker)); // null/пусто — конец листинга
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return Result<OwnS3SyncReportPage>.Failed(new ApplicationException($"sync-report {host}:{port}: timeout"));
        }
        catch (Exception ex) when (ex is HttpRequestException or JsonException)
        {
            return Result<OwnS3SyncReportPage>.Failed(new ApplicationException($"sync-report {host}:{port}: {ex.Message}", ex));
        }
    }

    // ListObjectsV2 (зонд §3.6.3): полный постраничный листинг bucket'а для
    // количественного diff при расхождении свёрток. Per-node клиент — как EnsureBucketAsync
    // (ForcePathStyle, retries off, таймаут 7.5 c — поллинг-инвариант).
    public async Task<Result<OwnS3ListPage>> ListObjectEntriesAsync(
        string host, int port, OwnS3NodeSecrets creds, string bucket, string? continuationToken, CancellationToken ct)
    {
        try
        {
            using var s3 = NewS3Client(host, port, creds);
            var response = await s3.ListObjectsV2Async(new Amazon.S3.Model.ListObjectsV2Request
            {
                BucketName = bucket,
                ContinuationToken = continuationToken, // null допустим — первая страница
                MaxKeys = 1000,
            }, ct);
            return Result<OwnS3ListPage>.Success(new OwnS3ListPage(
                response.S3Objects
                    // LastModified AWSSDK — DateTime (UTC): → unix-мс элемента сверки.
                    .Select(o => new OwnS3ListEntry(o.Key,
                        new DateTimeOffset(o.LastModified.ToUniversalTime(), TimeSpan.Zero).ToUnixTimeMilliseconds(),
                        o.Size))
                    .ToList(),
                string.IsNullOrEmpty(response.NextContinuationToken) ? null : response.NextContinuationToken));
        }
        catch (AmazonS3Exception ex)
        {
            return Result<OwnS3ListPage>.Failed(new ApplicationException(
                $"listing {host}:{port}/{bucket}: {ex.Message}", ex));
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return Result<OwnS3ListPage>.Failed(new ApplicationException($"listing {host}:{port}/{bucket}: timeout"));
        }
        catch (Exception ex)
        {
            return Result<OwnS3ListPage>.Failed(new ApplicationException(
                $"listing {host}:{port}/{bucket}: {ex.Message}", ex));
        }
    }

    public async Task<Result<OwnS3JoinOutcome>> AdminJoinAsync(
        string host, int port, OwnS3NodeSecrets creds, string endpoint, CancellationToken ct)
    {
        try
        {
            // camelCase-тело (контракт ownS3-грани, owns3/07 §2): парсер ownS3
            // регистрочувствителен — PascalCase по умолчанию даёт 400 malformed.
            var body = JsonSerializer.SerializeToUtf8Bytes(
                new JoinRequestBody(endpoint), AdminJoinJson);
            using var request = SignAdmin("POST", host, port, "/admin/replication/join", creds, body);
            using var response = await http.SendAsync(request, ct);
            return response.StatusCode switch
            {
                HttpStatusCode.OK => Result<OwnS3JoinOutcome>.Success(new OwnS3JoinOutcome(
                    OwnS3JoinKind.Joined, await ReadPeerNameAsync(response, ct))),
                HttpStatusCode.Conflict => Result<OwnS3JoinOutcome>.Success(new OwnS3JoinOutcome(OwnS3JoinKind.Conflict)),
                HttpStatusCode.Forbidden => Result<OwnS3JoinOutcome>.Success(new OwnS3JoinOutcome(OwnS3JoinKind.Refused)),
                _ => Result<OwnS3JoinOutcome>.Success(new OwnS3JoinOutcome(OwnS3JoinKind.Transient)),
            };
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return Result<OwnS3JoinOutcome>.Success(new OwnS3JoinOutcome(OwnS3JoinKind.Transient));
        }
        catch (HttpRequestException)
        {
            return Result<OwnS3JoinOutcome>.Success(new OwnS3JoinOutcome(OwnS3JoinKind.Transient));
        }
    }

    public async Task<Result<bool>> DeleteTargetAsync(
        string host, int port, OwnS3NodeSecrets creds, string name, CancellationToken ct)
    {
        try
        {
            using var request = SignAdmin("DELETE", host, port, $"/admin/replication/targets/{Uri.EscapeDataString(name)}", creds);
            using var response = await http.SendAsync(request, ct);
            return response.StatusCode switch
            {
                HttpStatusCode.Accepted => Result<bool>.Success(true),
                HttpStatusCode.NotFound => Result<bool>.Success(false), // идемпотентность
                _ => FailedHttp<bool>("delete-target", host, port, response),
            };
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return Result<bool>.Failed(new ApplicationException($"delete-target {host}:{port}: timeout"));
        }
        catch (HttpRequestException ex)
        {
            return Result<bool>.Failed(new ApplicationException($"delete-target {host}:{port}: {ex.Message}", ex));
        }
    }

    public async Task<Result<bool>> PutDenyListAsync(
        string host, int port, IReadOnlyList<(string Name, string PublicKeyPem)> entries,
        IssuedPkiMaterial orchestrator, CancellationToken ct)
    {
        try
        {
            using var buffer = new MemoryStream();
            using (var writer = new Utf8JsonWriter(buffer))
            {
                writer.WriteStartObject();
                writer.WriteStartArray("entries");
                foreach (var (name, publicKeyPem) in entries)
                {
                    writer.WriteStartObject();
                    writer.WriteString("name", name);
                    writer.WriteString("publicKeyPem", publicKeyPem);
                    writer.WriteEndObject();
                }

                writer.WriteEndArray();
                writer.WriteEndObject();
            }

            var body = buffer.ToArray();
            using var request = BuildPeerSignedRequest(HttpMethod.Put, host, port, "/replication/denylist", body, orchestrator);
            using var response = await http.SendAsync(request, ct);
            return response.StatusCode switch
            {
                HttpStatusCode.Accepted => Result<bool>.Success(true),
                HttpStatusCode.Forbidden => Result<bool>.Success(false), // trust не принят — transient тика
                _ => FailedHttp<bool>("denylist", host, port, response),
            };
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return Result<bool>.Failed(new ApplicationException($"denylist {host}:{port}: timeout"));
        }
        catch (Exception ex) when (ex is HttpRequestException or CryptographicException or FormatException)
        {
            return Result<bool>.Failed(new ApplicationException($"denylist {host}:{port}: {ex.Message}", ex));
        }
    }

    // Имя пира из тела admin-join-ответа {"joined":true,"peer":"…"} (не критично:
    // нечитаемое тело не ломает исход — G знает имя из links).
    private static async Task<string?> ReadPeerNameAsync(HttpResponseMessage response, CancellationToken ct)
    {
        try
        {
            using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct));
            return doc.RootElement.TryGetProperty("peer", out var peer) && peer.ValueKind == JsonValueKind.String
                ? peer.GetString()
                : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>
    /// Построение до-join запроса OWNS3-PEER (owns3/07 §6, по образцу
    /// JoinPeerService.JoinAsync): подпись приватным ключом оркестратора,
    /// сертификат в заголовке X-OwnS3-Peer-Certificate. internal — точка
    /// юнит-верификации совместимости PeerRequestVerifier (spec §8.1.6).
    /// </summary>
    internal static HttpRequestMessage BuildPeerSignedRequest(
        HttpMethod method, string host, int port, string path, byte[] body,
        IssuedPkiMaterial orchestrator, DateTimeOffset? at = null)
    {
        var payloadHash = SigV4Core.HexSha256(body);
        var instant = at ?? DateTimeOffset.UtcNow;
        var peerDate = instant.ToString("yyyyMMdd'T'HHmmss'Z'", System.Globalization.CultureInfo.InvariantCulture);
        using var key = ECDsa.Create();
        key.ImportFromPem(orchestrator.PrivateKeyPem);
        var (authorization, _, _) = PeerRequestSigner.Sign(key, orchestrator.Certificate.Name,
            method.Method, path, "",
            [("host", $"{host}:{port}"),
                ("x-owns3-peer-date", peerDate),
                ("x-owns3-peer-content-sha256", payloadHash)],
            payloadHash, instant);

        var request = new HttpRequestMessage(method, $"http://{host}:{port}{path}")
        {
            Content = new ByteArrayContent(body),
        };
        request.Headers.TryAddWithoutValidation("host", $"{host}:{port}");
        request.Headers.TryAddWithoutValidation("x-owns3-peer-date", peerDate);
        request.Headers.TryAddWithoutValidation("x-owns3-peer-content-sha256", payloadHash);
        request.Headers.TryAddWithoutValidation("Authorization", authorization);
        // Сертификат «на проводе» (owns3/07 §2): base64 от UTF-8 байтов JSON.
        request.Headers.TryAddWithoutValidation("X-OwnS3-Peer-Certificate",
            Convert.ToBase64String(Encoding.UTF8.GetBytes(orchestrator.CertificateJson)));
        return request;
    }

    // Тело admin-join-запроса {"endpoint": "<b-advertised>"}.
    private sealed record JoinRequestBody(string Endpoint);

    // camelCase-сериализация тел админ-грани (контракт owns3/07 §2).
    private static readonly JsonSerializerOptions AdminJoinJson = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    // SigV4-подпись админ-грани root-кредом профиля (метод/путь/тело — общий путь).
    // canonicalQuery — канонизованная query GET (sync-report); подписывается
    // и ставится в URL (сервер подписывает RawQuery).
    private static HttpRequestMessage SignAdmin(
        string method, string host, int port, string path, OwnS3NodeSecrets creds,
        byte[]? body = null, string? canonicalQuery = null)
    {
        var url = string.IsNullOrEmpty(canonicalQuery) ? path : $"{path}?{canonicalQuery}";
        var request = body is null
            ? new HttpRequestMessage(new HttpMethod(method), $"http://{host}:{port}{url}")
            : new HttpRequestMessage(new HttpMethod(method), $"http://{host}:{port}{url}")
            {
                Content = new ByteArrayContent(body),
            };
        var signed = SigV4GetSigner.SignRequest(method, path, $"{host}:{port}", creds.RootUser, creds.RootPassword,
            canonicalQuery: canonicalQuery);
        request.Headers.TryAddWithoutValidation("Authorization", signed.Authorization);
        request.Headers.TryAddWithoutValidation("x-amz-date", signed.AmzDate);
        request.Headers.TryAddWithoutValidation("x-amz-content-sha256", signed.ContentSha256);
        return request;
    }

    // JSON-хелперы толерантного парсинга граней (null/нет поля → дефолт).
    private static long LongOr(JsonElement element, string name, long fallback)
        => element.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.Number ? v.GetInt64() : fallback;

    private static double? DoubleOrNull(JsonElement element, string name)
        => element.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.Number ? v.GetDouble() : null;

    private static bool BoolOr(JsonElement element, string name, bool fallback)
        => element.TryGetProperty(name, out var v)
            && (v.ValueKind == JsonValueKind.True || v.ValueKind == JsonValueKind.False)
            ? v.GetBoolean()
            : fallback;

    private static string? StrOrNull(JsonElement element, string name)
        => element.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null;

    // Не-2xx админ-грани → Failed с кодом (транзиент для тика).
    private static Result<T> FailedHttp<T>(string op, string host, int port, HttpResponseMessage response)
        => Result<T>.Failed(new ApplicationException(
            $"{op} {host}:{port}: HTTP {(int)response.StatusCode}"));

    // per-node клиент: ForcePathStyle (адресные URL вида http://host:port/bucket),
    // retries off (ретраи — дело тика; таймауты 7.5 c поллинг-инварианта).
    private static AmazonS3Client NewS3Client(string host, int port, OwnS3NodeSecrets creds)
        => new(
            new Amazon.Runtime.BasicAWSCredentials(creds.RootUser, creds.RootPassword),
            new AmazonS3Config
            {
                ServiceURL = $"http://{host}:{port}",
                ForcePathStyle = true,
                Timeout = TimeSpan.FromSeconds(7.5),
                MaxErrorRetry = 0,
            });
}
