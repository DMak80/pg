using Amazon;
using Amazon.S3;

namespace S3Worker.Core.OwnS3;

/// <summary>Занятость диска ноды из админ-грани GET /admin/storage
/// (owns3/05 §7 после Task 1: camelCase, null-поля → Failed — транзиент).</summary>
public sealed record OwnS3NodeUsage(long UsedBytes, long TotalBytes);

/// <summary>
/// Грань ownS3-ноды для процессов S3Worker (arch/23 §5): health-проба
/// (без подписи), существование/создание bucket'а (AWSSDK, root-креды
/// профиля), занятость диска (SigV4 GET /admin/storage). Все таймауты —
/// 7.5 с (поллинг-инвариант: половина окна watchdog); ретраи — дело тика.
/// </summary>
public interface IOwnS3NodeClient
{
    // GET /healthz (без подписи): 200 → true; таймаут/5xx/404 → false
    // (транзиент, не исключение наружу).
    Task<Result<bool>> HealthAsync(string host, int port, CancellationToken ct);

    // exists-проба: GetBucketLocation → 200=true, 404=false.
    Task<Result<bool>> BucketExistsAsync(string host, int port, OwnS3NodeSecrets creds, string bucket, CancellationToken ct);

    // Идемпотентно: exists → Success(skip); PutBucket;
    // 409 BucketAlreadyOwnedByYou → Success (owns3/02 CreateBucket).
    Task<Result> EnsureBucketAsync(string host, int port, OwnS3NodeSecrets creds, string bucket, CancellationToken ct);

    // SigV4 GET /admin/storage root-кредом → usedBytes/totalBytes;
    // null-поля/не-2xx → Failed (транзиент).
    Task<Result<OwnS3NodeUsage>> GetUsageAsync(string host, int port, OwnS3NodeSecrets creds, CancellationToken ct);
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
            await s3.GetBucketLocationAsync(bucket, ct);
            return Result<bool>.Success(true);
        }
        catch (AmazonS3Exception ex) when (ex.ErrorCode == "NoSuchBucket")
        {
            return Result<bool>.Success(false);
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
                return Result<OwnS3NodeUsage>.Failed(new ApplicationException(
                    $"usage {host}:{port}: HTTP {(int)response.StatusCode}"));

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
