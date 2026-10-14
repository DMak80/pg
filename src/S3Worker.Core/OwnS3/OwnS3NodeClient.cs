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

/// <summary>Состав mesh инстанса из GET /admin/replication/status (owns3/07 §10):
/// G нужны только имена соседей-таргетов (состав, не счётчики — spec §3.5).</summary>
public sealed record OwnS3ReplicationStatus(bool Active, IReadOnlyList<string> NeighborNames);

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
            return Result<bool>.Failed(new ApplicationException($"bucket-probe {host}:{port}: таймаут"));
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
            return Result.Failed(new ApplicationException($"bucket-ensure {host}:{port}: таймаут"));
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
                    $"usage {host}:{port}: дисковые поля отсутствуют/null"));
            }

            return Result<OwnS3NodeUsage>.Success(
                new OwnS3NodeUsage(used.GetInt64(), total.GetInt64()));
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return Result<OwnS3NodeUsage>.Failed(new ApplicationException($"usage {host}:{port}: таймаут"));
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
            if (root.TryGetProperty("neighbors", out var list) && list.ValueKind == JsonValueKind.Array)
            {
                foreach (var n in list.EnumerateArray())
                    if (n.TryGetProperty("name", out var name) && name.ValueKind == JsonValueKind.String)
                        neighbors.Add(name.GetString()!);
            }

            return Result<OwnS3ReplicationStatus>.Success(new OwnS3ReplicationStatus(active, neighbors));
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return Result<OwnS3ReplicationStatus>.Failed(
                new ApplicationException($"replication-status {host}:{port}: таймаут"));
        }
        catch (Exception ex) when (ex is HttpRequestException or JsonException)
        {
            return Result<OwnS3ReplicationStatus>.Failed(
                new ApplicationException($"replication-status {host}:{port}: {ex.Message}", ex));
        }
    }

    public async Task<Result<OwnS3JoinOutcome>> AdminJoinAsync(
        string host, int port, OwnS3NodeSecrets creds, string endpoint, CancellationToken ct)
    {
        try
        {
            var body = JsonSerializer.SerializeToUtf8Bytes(new JoinRequestBody(endpoint));
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
            return Result<bool>.Failed(new ApplicationException($"delete-target {host}:{port}: таймаут"));
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
            return Result<bool>.Failed(new ApplicationException($"denylist {host}:{port}: таймаут"));
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

    // SigV4-подпись админ-грани root-кредом профиля (метод/путь/тело — общий путь).
    private static HttpRequestMessage SignAdmin(
        string method, string host, int port, string path, OwnS3NodeSecrets creds, byte[]? body = null)
    {
        var request = body is null
            ? new HttpRequestMessage(new HttpMethod(method), $"http://{host}:{port}{path}")
            : new HttpRequestMessage(new HttpMethod(method), $"http://{host}:{port}{path}")
            {
                Content = new ByteArrayContent(body),
            };
        var signed = SigV4GetSigner.SignRequest(method, path, $"{host}:{port}", creds.RootUser, creds.RootPassword);
        request.Headers.TryAddWithoutValidation("Authorization", signed.Authorization);
        request.Headers.TryAddWithoutValidation("x-amz-date", signed.AmzDate);
        request.Headers.TryAddWithoutValidation("x-amz-content-sha256", signed.ContentSha256);
        return request;
    }

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
