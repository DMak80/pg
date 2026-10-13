using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Text.Json;
using OwnS3.Protocol.Auth;

namespace OwnS3.Replication.Transport;

// Реализация IReplicationTransport поверх HttpClient (t35, глава 07 §6): подпись
// всех запросов SigV4Signer'ом с accessKey/secretKey цели; таймаут запроса 10 с;
// тело фетча — потоковым (HttpCompletionOption.ResponseHeadersRead).
public sealed class ReplicationHttpClient : IReplicationTransport
{
    public static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(10);

    private static readonly string[] SignedHeaderNames = ["host", "x-amz-date", "x-amz-content-sha256"];

    private readonly HttpClient _http;
    private readonly TimeProvider _timeProvider;

    public ReplicationHttpClient(HttpClient http, TimeProvider? timeProvider = null)
    {
        _http = http;
        _timeProvider = timeProvider ?? TimeProvider.System;
        if (_http.Timeout == default || _http.Timeout > RequestTimeout)
            _http.Timeout = RequestTimeout;
    }

    public async Task<long?> SendNotificationsAsync(TargetRecord target, NotificationBatchRequest batch,
        CancellationToken ct)
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(batch, ReplicationTransportJson.Options);
        var payloadHash = SigV4Core.HexSha256(payload);
        using var request = BuildRequest(target, HttpMethod.Post, "/replication/notifications", "",
            payload, payloadHash);
        using var response = await _http.SendAsync(request, HttpCompletionOption.ResponseContentRead, ct);
        if (response.StatusCode == HttpStatusCode.TooManyRequests)
            return null; // приёмный буфер переполнен — вызывающий бэкоффит (глава 07 §8)
        response.EnsureSuccessStatusCode();
        var accepted = JsonSerializer.Deserialize<NotificationAcceptedResponse>(
            await response.Content.ReadAsByteArrayAsync(ct), ReplicationTransportJson.Options);
        return accepted?.AcceptedToSeqNo;
    }

    public async Task<FetchResult> FetchObjectAsync(TargetRecord target, string bucket, string key,
        CancellationToken ct)
    {
        var path = $"/replication/object/{EscapeKeyPath(bucket)}/{EscapeKeyPath(key)}";
        using var request = BuildRequest(target, HttpMethod.Get, path, "", payload: null, payloadHash: null);
        var response = await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct);
        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            response.Dispose();
            // Ключ неизвестен — приёмник состояние не меняет (глава 07 §6)
            return new FetchResult(Body: null, ContentLength: 0, ModTimeUnixMs: 0, ContentSha256: string.Empty,
                ETag: string.Empty, ContentType: string.Empty, UserMetadata: FrozenEmpty, TombstoneUnixMs: null);
        }
        response.EnsureSuccessStatusCode();

        var headers = response.Headers.ToDictionary(h => h.Key, h => string.Join(",", h.Value),
            StringComparer.OrdinalIgnoreCase)
            .Union(response.Content.Headers.ToDictionary(h => h.Key, h => string.Join(",", h.Value),
                StringComparer.OrdinalIgnoreCase))
            .ToDictionary(p => p.Key, p => p.Value, StringComparer.OrdinalIgnoreCase);

        // Tombstone-ответ: ключ удалён, тела нет (глава 07 §6)
        if (headers.TryGetValue("x-owns3-repl-tombstone", out var tombstoneText)
            && long.TryParse(tombstoneText, out var tombstone))
        {
            var empty = await response.Content.ReadAsByteArrayAsync(ct);
            response.Dispose();
            return new FetchResult(Body: null, ContentLength: 0, ModTimeUnixMs: 0, ContentSha256: string.Empty,
                ETag: string.Empty, ContentType: string.Empty, UserMetadata: FrozenEmpty, TombstoneUnixMs: tombstone);
        }

        var userMetadata = headers.Where(h => h.Key.StartsWith("x-amz-meta-", StringComparison.OrdinalIgnoreCase))
            .ToDictionary(h => h.Key["x-amz-meta-".Length..], h => h.Value, StringComparer.Ordinal);
        headers.TryGetValue("x-owns3-repl-modtime", out var modTimeText);
        headers.TryGetValue("x-owns3-repl-sha256", out var sha);
        headers.TryGetValue("x-owns3-repl-etag", out var etag);
        var contentType = response.Content.Headers.ContentType?.ToString() ?? string.Empty;
        var body = await response.Content.ReadAsStreamAsync(ct);
        return new FetchResult(
            Body: body,
            ContentLength: response.Content.Headers.ContentLength ?? -1,
            ModTimeUnixMs: long.TryParse(modTimeText, out var modTime) ? modTime : 0,
            ContentSha256: sha ?? string.Empty,
            ETag: etag ?? string.Empty,
            ContentType: contentType,
            UserMetadata: userMetadata,
            TombstoneUnixMs: null);
    }

    public async Task<ReplicationListingResponse> ListAsync(TargetRecord target, string bucket, string? marker,
        int maxKeys, CancellationToken ct)
    {
        var query = BuildListingQuery(bucket, marker, maxKeys);
        var payload = await SendListingAsync(target, query, ct);
        return JsonSerializer.Deserialize<ReplicationListingResponse>(payload, ReplicationTransportJson.Options)
            ?? new ReplicationListingResponse([], null);
    }

    public async Task<IReadOnlyList<(string Name, long CreationUnixMs)>> ListingBucketsAsync(TargetRecord target,
        CancellationToken ct)
    {
        var payload = await SendListingAsync(target, string.Empty, ct);
        var response = JsonSerializer.Deserialize<ReplicationBucketsResponse>(payload, ReplicationTransportJson.Options)
            ?? new ReplicationBucketsResponse([]);
        return response.Buckets.Select(b => (b.Name, b.CreationDateUnixMs)).ToList();
    }

    // GET /replication/ping (глава 07 §2/§6): лёгкая подписанная проба линка.
    // rtt — от старта запроса до статус-строки (ResponseHeadersRead); per-target
    // таймаут — linked CTS поверх общего таймаута HttpClient 10 с (глава 07 §2).
    public async Task<PingResult> PingAsync(TargetRecord target, TimeSpan timeout, CancellationToken ct)
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(timeout);
        var sw = Stopwatch.StartNew();
        try
        {
            using var request = BuildRequest(target, HttpMethod.Get, "/replication/ping", "",
                payload: null, payloadHash: null);
            using var response = await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead,
                timeoutCts.Token);
            sw.Stop();
            // reachable строго при валидном 200 ping-грани (глава 07 §2);
            // rttMs фиксируется при любом HTTP-ответе (включая не-200)
            return new PingResult(response.StatusCode == HttpStatusCode.OK,
                (long)sw.Elapsed.TotalMilliseconds);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            // Per-target таймаут probe — HTTP-ответа нет, rttMs отсутствует
            return new PingResult(false, null);
        }
        catch (HttpRequestException)
        {
            // Транспортный обрыв (connection refused/DNS) — rttMs отсутствует
            return new PingResult(false, null);
        }
        // Отмена внешнего ct — OperationCanceledException пробрасывается вызывающему
    }

    // ── внутренняя механика ──

    private async Task<byte[]> SendListingAsync(TargetRecord target, string query, CancellationToken ct)
    {
        using var request = BuildRequest(target, HttpMethod.Get, "/replication/listing", query,
            payload: null, payloadHash: null);
        using var response = await _http.SendAsync(request, HttpCompletionOption.ResponseContentRead, ct);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadAsByteArrayAsync(ct);
    }

    private static string BuildListingQuery(string bucket, string? marker, int maxKeys)
    {
        var pairs = new List<string> { $"bucket={Uri.EscapeDataString(bucket)}" };
        if (!string.IsNullOrEmpty(marker))
            pairs.Add($"marker={Uri.EscapeDataString(marker)}");
        pairs.Add($"max-keys={maxKeys}");
        return string.Join("&", pairs);
    }

    private HttpRequestMessage BuildRequest(TargetRecord target, HttpMethod method, string path, string query,
        byte[]? payload, string? payloadHash)
    {
        var payloadHashValue = payloadHash ?? SigV4Core.EmptySha256; // GET без тела
        var endpoint = new Uri(target.Endpoint);
        var at = _timeProvider.GetUtcNow();
        var amzDate = at.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture);
        var headers = new List<(string Name, string Value)>
        {
            ("host", endpoint.Authority),
            ("x-amz-date", amzDate),
            ("x-amz-content-sha256", payloadHashValue),
        };
        var (authorization, _, _) = SigV4Signer.Sign(target.SecretKey, target.AccessKey, method.Method, path,
            query, headers, payloadHashValue, at);
        // URI строкой: UriBuilder{Path=...} сбрасывает нестандартный порт (80/443)
        var uriText = $"{endpoint.AbsoluteUri.TrimEnd('/')}{path}{(string.IsNullOrEmpty(query) ? "" : $"?{query}")}";
        var request = new HttpRequestMessage(method, new Uri(uriText));
        foreach (var (name, value) in headers)
            request.Headers.TryAddWithoutValidation(name, value);
        request.Headers.TryAddWithoutValidation("Authorization", authorization);
        if (payload is not null)
            request.Content = new ByteArrayContent(payload);
        return request;
    }

    // Ключ в пути: percent-кодирование сегментов с сохранением '/' (глава 03 §6).
    private static string EscapeKeyPath(string value) =>
        string.Join("/", value.Split('/').Select(Uri.EscapeDataString));

    private static readonly IReadOnlyDictionary<string, string> FrozenEmpty =
        new Dictionary<string, string>();
}
