using System.Net;
using System.Text;
using System.Text.Json;
using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using OwnS3.Replication.Transport;
using OwnS3.UnitTests;

namespace OwnS3.IntegrationTests.E2e.Replication;

// Клиенты mesh-контура (t35, план Task 16): AWS SDK per-node (ForcePathStyle,
// динамический хост-порт) + raw-SigV4 клиент admin-грани и replication-операций
// (подпись — тестовый signer юнитов) + поллинг-хелперы (шаг ≤5 с в первых циклах).
public static class MeshClients
{
    public const string PeerAccessKey = OwnS3MeshE2eEnvironment.PeerAccessKey;
    public const string PeerSecretKey = OwnS3MeshE2eEnvironment.PeerSecretKey;
    public const string WriterAccessKey = OwnS3MeshE2eEnvironment.WriterAccessKey;
    public const string WriterSecretKey = OwnS3MeshE2eEnvironment.WriterSecretKey;
    public const string AdminAccessKey = OwnS3MeshE2eEnvironment.RootUser;
    public const string AdminSecretKey = OwnS3MeshE2eEnvironment.RootPassword;

    /// <summary>S3-клиент узла i (SDK, ForcePathStyle, свой endpoint; таймаут 10 c,
    /// flexible-checksums WHEN_REQUIRED — канон arch/owns3/03 §3).</summary>
    public static IAmazonS3 S3(OwnS3MeshE2eEnvironment env, int node, int timeoutSec = 10,
        int maxErrorRetry = 2) => new AmazonS3Client(new BasicAWSCredentials(WriterAccessKey, WriterSecretKey),
        new AmazonS3Config
        {
            ServiceURL = env.NodeHostUrl(node),
            ForcePathStyle = true,
            AuthenticationRegion = "us-east-1",
            Timeout = TimeSpan.FromSeconds(timeoutSec),
            MaxErrorRetry = maxErrorRetry,
            RequestChecksumCalculation = RequestChecksumCalculation.WHEN_REQUIRED,
        });

    /// <summary>CreateBucket админ-кредами (матрица 05 §3: CreateBucket — только admin).</summary>
    public static async Task CreateBucketAsync(OwnS3MeshE2eEnvironment env, int node, string bucket,
        CancellationToken ct)
    {
        using var s3 = new AmazonS3Client(new BasicAWSCredentials(AdminAccessKey, AdminSecretKey),
            new AmazonS3Config
            {
                ServiceURL = env.NodeHostUrl(node),
                ForcePathStyle = true,
                AuthenticationRegion = "us-east-1",
                Timeout = TimeSpan.FromSeconds(10),
                RequestChecksumCalculation = RequestChecksumCalculation.WHEN_REQUIRED,
            });
        await s3.PutBucketAsync(bucket, ct);
    }

    /// <summary>PUT объекта SDK-клиентом узла.</summary>
    public static async Task PutObjectAsync(OwnS3MeshE2eEnvironment env, int node, string bucket,
        string key, byte[] body, CancellationToken ct)
    {
        using var s3 = S3(env, node, timeoutSec: 5, maxErrorRetry: 0);
        await s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucket, Key = key,
            InputStream = new MemoryStream(body),
        }, ct);
    }

    /// <summary>GET объекта SDK-клиентом узла (тело или null при 404).</summary>
    public static async Task<byte[]?> GetObjectOrNullAsync(OwnS3MeshE2eEnvironment env, int node,
        string bucket, string key, CancellationToken ct)
    {
        using var s3 = S3(env, node);
        try
        {
            using var response = await s3.GetObjectAsync(bucket, key, ct);
            using var ms = new MemoryStream();
            await response.ResponseStream.CopyToAsync(ms, ct);
            return ms.ToArray();
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    /// <summary>DeleteObject SDK-клиентом узла.</summary>
    public static async Task DeleteObjectAsync(OwnS3MeshE2eEnvironment env, int node, string bucket,
        string key, CancellationToken ct)
    {
        using var s3 = S3(env, node);
        await s3.DeleteObjectAsync(bucket, key, ct);
    }

    /// <summary>DeleteObjects (пачка ключей) SDK-клиентом узла.</summary>
    public static async Task DeleteObjectsAsync(OwnS3MeshE2eEnvironment env, int node, string bucket,
        IReadOnlyList<string> keys, CancellationToken ct)
    {
        using var s3 = S3(env, node);
        await s3.DeleteObjectsAsync(new DeleteObjectsRequest
        {
            BucketName = bucket,
            Objects = [.. keys.Select(k => new KeyVersion { Key = k })],
        }, ct);
    }

    /// <summary>Multipart-цикл SDK-клиентом узла (parts частей, partSize байт) —
    /// результат CompleteMultipart (ETag составного).</summary>
    public static async Task<string> MultipartPutAsync(OwnS3MeshE2eEnvironment env, int node,
        string bucket, string key, int parts, int partSize, Func<int, byte[]> partBody,
        CancellationToken ct)
    {
        using var s3 = S3(env, node);
        var initiate = await s3.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = bucket, Key = key,
        }, ct);
        var etags = new List<PartETag>();
        try
        {
            for (var number = 1; number <= parts; number++)
            {
                var body = partBody(number);
                var upload = await s3.UploadPartAsync(new UploadPartRequest
                {
                    BucketName = bucket, Key = key, UploadId = initiate.UploadId,
                    PartNumber = number, InputStream = new MemoryStream(body),
                }, ct);
                etags.Add(new PartETag(number, upload.ETag));
            }
            var complete = await s3.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
            {
                BucketName = bucket, Key = key, UploadId = initiate.UploadId, PartETags = etags,
            }, ct);
            return complete.ETag;
        }
        catch
        {
            await s3.AbortMultipartUploadAsync(bucket, key, initiate.UploadId, CancellationToken.None);
            throw;
        }
    }

    /// <summary>HTTP-клиент узла с peer-кредами (replication-операции: батчи, фетч).</summary>
    public static MeshPeerClient PeerClient(OwnS3MeshE2eEnvironment env, int node) =>
        new(env.NodeHostUrl(node), PeerAccessKey, PeerSecretKey);

    /// <summary>HTTP-клиент узла с admin-кредами (админ-грань replication).</summary>
    public static MeshAdminClient AdminClient(OwnS3MeshE2eEnvironment env, int node) =>
        new(env.NodeHostUrl(node), AdminAccessKey, AdminSecretKey);

    /// <summary>Поллинг условия с шагом ≤5 с в первых циклах, ≤30 с далее
    /// (бюджет срастания 5 мин — глава 07 §1).</summary>
    public static async Task PollUntilAsync(Func<Task<bool>> condition, string what,
        int budgetSec = 300, CancellationToken ct = default)
    {
        var deadline = Environment.TickCount64 + budgetSec * 1000L;
        var stepMs = 5_000;
        while (true)
        {
            if (await condition())
                return;
            if (Environment.TickCount64 >= deadline)
                throw new TimeoutException($"условие не выполнено за {budgetSec} с: {what}");
            await Task.Delay(stepMs, ct);
            stepMs = Math.Min(stepMs * 2, 30_000);
        }
    }

    /// <summary>PUT /admin/replication/targets узлу (замена состава mesh).</summary>
    public static Task<HttpResponseMessage> AdminPutTargetsAsync(OwnS3MeshE2eEnvironment env, int node,
        IReadOnlyList<(string Name, string Endpoint)> targets, CancellationToken ct)
    {
        var body = JsonSerializer.Serialize(new
        {
            targets = targets.Select(t => new { name = t.Name, endpoint = t.Endpoint,
                accessKey = PeerAccessKey, secretKey = PeerSecretKey }).ToList(),
        });
        return AdminClient(env, node).SendAsync("PUT", "/admin/replication/targets", "", body, ct);
    }

    /// <summary>GET /admin/replication/status узла (JSON-строка).</summary>
    public static async Task<string> AdminGetStatusAsync(OwnS3MeshE2eEnvironment env, int node,
        CancellationToken ct)
    {
        var response = await AdminClient(env, node).SendAsync("GET", "/admin/replication/status", "", null, ct);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadAsStringAsync(ct);
    }

    /// <summary>POST /admin/replication/reconcile узлу (202).</summary>
    public static Task<HttpResponseMessage> AdminReconcileAsync(OwnS3MeshE2eEnvironment env, int node,
        CancellationToken ct) =>
        AdminClient(env, node).SendAsync("POST", "/admin/replication/reconcile", "", null, ct);

    /// <summary>GET /admin/replication/sync-report узла (JSON-строка).</summary>
    public static async Task<string> AdminSyncReportAsync(OwnS3MeshE2eEnvironment env, int node,
        string bucket, CancellationToken ct)
    {
        // query — ОТДЕЛЬНЫМ параметром (участвует в canonical query подписи)
        var response = await AdminClient(env, node).SendAsync(
            "GET", "/admin/replication/sync-report", $"bucket={Uri.EscapeDataString(bucket)}", null, ct);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadAsStringAsync(ct);
    }

    /// <summary>GET /metrics узла (текст Prometheus).</summary>
    public static async Task<string> GetMetricsAsync(OwnS3MeshE2eEnvironment env, int node, CancellationToken ct)
    {
        using var http = new HttpClient { BaseAddress = new Uri(env.NodeHostUrl(node)) };
        return await http.GetStringAsync("/metrics", ct);
    }
}

/// <summary>Raw-SigV4 HTTP-клиент mesh (подпись — TestSigV4Signer юнитов:
/// полный контроль сырого пути/заголовков, те же креды на все replication/admin пути).</summary>
public sealed class MeshPeerClient
{
    private readonly HttpClient _http;
    private readonly string _accessKey;
    private readonly string _secretKey;

    public MeshPeerClient(string baseUrl, string accessKey, string secretKey)
    {
        _http = new HttpClient { BaseAddress = new Uri(baseUrl) };
        _accessKey = accessKey;
        _secretKey = secretKey;
    }

    public async Task<HttpResponseMessage> SendAsync(string method, string pathAndQuery, string? query,
        string? body, CancellationToken ct)
    {
        var payloadBytes = body is null ? [] : Encoding.UTF8.GetBytes(body);
        var payloadHash = Convert.ToHexString(
            System.Security.Cryptography.SHA256.HashData(payloadBytes)).ToLowerInvariant();
        var date = DateTimeOffset.UtcNow;
        var host = _http.BaseAddress!.Authority;
        var headerList = new List<(string, string)>
        {
            ("host", host),
            ("x-amz-date", TestSigV4Signer.AmzDateOf(date)),
            ("x-amz-content-sha256", payloadHash),
        };
        var canonicalQuery = OwnS3.Protocol.Auth.CanonicalRequestBuilder.QueryPairs(query ?? "") is { } pairs
            ? OwnS3.Protocol.Uris.UriEncoding.EncodeQuery(pairs)
            : string.Empty;
        var signature = TestSigV4Signer.HeaderSignature(_secretKey, method, pathAndQuery, canonicalQuery,
            headerList, payloadHash, date, "us-east-1");
        var authorization = TestSigV4Signer.BuildAuthorization(_accessKey, "us-east-1", date,
            headerList.Select(h => h.Item1).Where(n => n != "host"), signature);

        using var request = new HttpRequestMessage(new HttpMethod(method),
            string.IsNullOrEmpty(query) ? pathAndQuery : $"{pathAndQuery}?{query}");
        if (payloadBytes.Length > 0)
            request.Content = new ByteArrayContent(payloadBytes);
        foreach (var (lowerName, value) in headerList.Where(h => h.Item1 != "host"))
            request.Headers.TryAddWithoutValidation(lowerName, value);
        request.Headers.TryAddWithoutValidation("Authorization", authorization);
        return await _http.SendAsync(request, ct);
    }
}

/// <summary>Админ-клиент = peer-клиент с админ-кредами.</summary>
public sealed class MeshAdminClient
{
    private readonly MeshPeerClient _inner;

    public MeshAdminClient(string baseUrl, string accessKey, string secretKey) =>
        _inner = new MeshPeerClient(baseUrl, accessKey, secretKey);

    public Task<HttpResponseMessage> SendAsync(string method, string pathAndQuery, string? query,
        string? body, CancellationToken ct) => _inner.SendAsync(method, pathAndQuery, query, body, ct);
}
