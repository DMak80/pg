using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Text.Json;
using OwnS3.Protocol.Auth;
using OwnS3.Protocol.PeerAuth;

namespace OwnS3.Replication.Transport;

// Реализация IReplicationTransport поверх HttpClient (t35/t39, глава 07 §6): схема
// подписи — по виду записи цели. legacy — SigV4Signer с accessKey/secretKey цели;
// key — peer-подпись приватным ключом identity (схема OWNS3-PEER-ECC-P256-SHA256).
// Таймаут запроса 10 с; тело фетча — потоковым (ResponseHeadersRead).
public sealed class ReplicationHttpClient : IReplicationTransport
{
    public static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(10);

    private static readonly string[] SignedHeaderNames = ["host", "x-amz-date", "x-amz-content-sha256"];

    private readonly HttpClient _http;
    private readonly TimeProvider _timeProvider;
    private readonly PeerIdentity? _identity;

    public ReplicationHttpClient(HttpClient http, TimeProvider? timeProvider = null,
        PeerIdentity? identity = null)
    {
        _http = http;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _identity = identity;
        if (_http.Timeout == default || _http.Timeout > RequestTimeout)
            _http.Timeout = RequestTimeout;
    }

    public async Task<Result<long>> SendNotificationsAsync(TargetRecord target, NotificationBatchRequest batch,
        CancellationToken ct)
    {
        // Ключ-сосед без identity: исходящая peer-подпись невозможна — терминальный
        // отказ транспорта (бэкофф вызывающего), НЕ исключение сквозь границу.
        // Пре-чек обязателен ДО BuildRequest: здесь BuildRequest вызывается ВНЕ
        // try — throw вылетел бы из метода и уронил цикл нотификатора
        // (NotificationDispatcher зовёт transport без catch), нарушение
        // Result-контракта (глава 07 §11, конвенция 01 §4).
        if (target.Auth == TargetRecord.AuthKey && _identity is null)
            return Result<long>.Failed(new PeerRejectedException(
                $"нет identity для key-записи {target.Name} (env OWNS3_REPLICATION_CERT/_PRIVATE_KEY)"));
        var payload = JsonSerializer.SerializeToUtf8Bytes(batch, ReplicationTransportJson.Options);
        var payloadHash = SigV4Core.HexSha256(payload);
        using var request = BuildRequest(target, HttpMethod.Post, "/replication/notifications", "",
            payload, payloadHash);
        try
        {
            using var response = await _http.SendAsync(request, HttpCompletionOption.ResponseContentRead, ct);
            if (response.StatusCode == HttpStatusCode.TooManyRequests)
                return Result<long>.Failed(new PeerRejectedException(
                    $"сосед не принял батч: 429 (target={target.Name})")); // приёмный буфер переполнен — вызывающий бэкоффит (глава 07 §8)
            if (!response.IsSuccessStatusCode)
                return Result<long>.Failed(new PeerRejectedException(
                    $"HTTP {(int)response.StatusCode} от {target.Name}"));
            var accepted = JsonSerializer.Deserialize<NotificationAcceptedResponse>(
                await response.Content.ReadAsByteArrayAsync(ct), ReplicationTransportJson.Options);
            return Result<long>.Success(accepted?.AcceptedToSeqNo ?? 0);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw; // внешняя отмена — не исход операции (graceful stop)
        }
        catch (Exception ex)
        {
            return Result<long>.Failed(new PeerRejectedException(
                $"транспорт рассылки к {target.Name}: {ex.Message}"));
        }
    }

    public async Task<Result<FetchOutcome>> FetchObjectAsync(TargetRecord target, string bucket, string key,
        CancellationToken ct)
    {
        // Пре-чек «key без identity» — ДО BuildRequest (см. SendNotificationsAsync).
        if (target.Auth == TargetRecord.AuthKey && _identity is null)
            return Result<FetchOutcome>.Failed(new PeerRejectedException(
                $"нет identity для key-записи {target.Name} (env OWNS3_REPLICATION_CERT/_PRIVATE_KEY)"));
        var path = $"/replication/object/{EscapeKeyPath(bucket)}/{EscapeKeyPath(key)}";
        using var request = BuildRequest(target, HttpMethod.Get, path, "", payload: null, payloadHash: null);
        HttpResponseMessage response;
        try
        {
            response = await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw; // внешняя отмена — не исход операции
        }
        catch (Exception ex)
        {
            return Result<FetchOutcome>.Failed(new PeerRejectedException(
                $"транспорт фетча у {target.Name}: {ex.Message}"));
        }

        // ВАЖНО: response НЕ диспозится здесь — поток тела (Fetched) живёт дольше
        // вызова (фетчер читает его в применении и закрывает сам); диспоз внутрь
        // using закрыл бы тело до чтения. Не-успешные ветки диспозят явно.
        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            response.Dispose();
            // Ключ неизвестен — приёмник состояние не меняет (глава 07 §6);
            // терминальный Success-вариант
            return Result<FetchOutcome>.Success(new FetchOutcome.KeyNotFound());
        }
        if (!response.IsSuccessStatusCode)
        {
            response.Dispose();
            return Result<FetchOutcome>.Failed(new PeerRejectedException(
                $"HTTP {(int)response.StatusCode} от {target.Name}"));
        }

        var headers = response.Headers.ToDictionary(h => h.Key, h => string.Join(",", h.Value),
            StringComparer.OrdinalIgnoreCase)
            .Union(response.Content.Headers.ToDictionary(h => h.Key, h => string.Join(",", h.Value),
                StringComparer.OrdinalIgnoreCase))
            .ToDictionary(p => p.Key, p => p.Value, StringComparer.OrdinalIgnoreCase);

        // Tombstone-ответ: ключ удалён, тела нет (глава 07 §6)
        if (headers.TryGetValue("x-owns3-repl-tombstone", out var tombstoneText)
            && long.TryParse(tombstoneText, out var tombstone))
        {
            await response.Content.ReadAsByteArrayAsync(ct); // дочитать тело ответа
            response.Dispose();
            return Result<FetchOutcome>.Success(new FetchOutcome.Tombstone(tombstone));
        }

        var userMetadata = headers.Where(h => h.Key.StartsWith("x-amz-meta-", StringComparison.OrdinalIgnoreCase))
            .ToDictionary(h => h.Key["x-amz-meta-".Length..], h => h.Value, StringComparer.Ordinal);
        headers.TryGetValue("x-owns3-repl-modtime", out var modTimeText);
        headers.TryGetValue("x-owns3-repl-sha256", out var sha);
        headers.TryGetValue("x-owns3-repl-etag", out var etag);
        var contentType = response.Content.Headers.ContentType?.ToString() ?? string.Empty;
        var body = await response.Content.ReadAsStreamAsync(ct);
        return Result<FetchOutcome>.Success(new FetchOutcome.Fetched(
            body,
            response.Content.Headers.ContentLength ?? -1,
            long.TryParse(modTimeText, out var modTime) ? modTime : 0,
            sha ?? string.Empty,
            etag ?? string.Empty,
            contentType,
            userMetadata));
    }

    public async Task<Result<ReplicationListingResponse>> ListAsync(TargetRecord target, string bucket, string? marker,
        int maxKeys, CancellationToken ct)
    {
        // Пре-чек «key без identity» — ДО BuildRequest (см. SendNotificationsAsync).
        if (target.Auth == TargetRecord.AuthKey && _identity is null)
            return Result<ReplicationListingResponse>.Failed(new PeerRejectedException(
                $"нет identity для key-записи {target.Name} (env OWNS3_REPLICATION_CERT/_PRIVATE_KEY)"));
        var query = BuildListingQuery(bucket, marker, maxKeys);
        byte[] payload;
        try
        {
            payload = await SendListingAsync(target, query, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw; // внешняя отмена — не исход операции
        }
        catch (Exception ex)
        {
            return Result<ReplicationListingResponse>.Failed(ex is PeerRejectedException pre
                ? pre
                : new PeerRejectedException($"транспорт листинга у {target.Name}: {ex.Message}"));
        }

        return Result<ReplicationListingResponse>.Success(
            JsonSerializer.Deserialize<ReplicationListingResponse>(payload, ReplicationTransportJson.Options)
            ?? new ReplicationListingResponse([], null));
    }

    public async Task<Result<IReadOnlyList<(string Name, long CreationUnixMs)>>> ListingBucketsAsync(TargetRecord target,
        CancellationToken ct)
    {
        // Пре-чек «key без identity» — ДО BuildRequest (см. SendNotificationsAsync).
        if (target.Auth == TargetRecord.AuthKey && _identity is null)
            return Result<IReadOnlyList<(string Name, long CreationUnixMs)>>.Failed(
                new PeerRejectedException(
                    $"нет identity для key-записи {target.Name} (env OWNS3_REPLICATION_CERT/_PRIVATE_KEY)"));
        byte[] payload;
        try
        {
            payload = await SendListingAsync(target, string.Empty, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw; // внешняя отмена — не исход операции
        }
        catch (Exception ex)
        {
            return Result<IReadOnlyList<(string Name, long CreationUnixMs)>>.Failed(
                ex is PeerRejectedException pre
                    ? pre
                    : new PeerRejectedException($"транспорт листинга у {target.Name}: {ex.Message}"));
        }

        var response = JsonSerializer.Deserialize<ReplicationBucketsResponse>(payload, ReplicationTransportJson.Options)
            ?? new ReplicationBucketsResponse([]);
        return Result<IReadOnlyList<(string Name, long CreationUnixMs)>>.Success(
            response.Buckets.Select(b => (b.Name, b.CreationDateUnixMs)).ToList());
    }

    // GET /replication/ping (глава 07 §2/§6): лёгкая подписанная проба линка.
    // rtt — от старта запроса до статус-строки (ResponseHeadersRead); per-target
    // таймаут — linked CTS поверх общего таймаута HttpClient 10 с (глава 07 §2).
    public async Task<Result<PingResult>> PingAsync(TargetRecord target, TimeSpan timeout, CancellationToken ct)
    {
        // Пре-чек «key без identity» — ДО BuildRequest (см. SendNotificationsAsync).
        if (target.Auth == TargetRecord.AuthKey && _identity is null)
            return Result<PingResult>.Failed(new PeerRejectedException(
                $"нет identity для key-записи {target.Name} (env OWNS3_REPLICATION_CERT/_PRIVATE_KEY)"));
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
            // любой полученный HTTP-ответ — Success (rttMs не-null, конвенция 01 §4)
            return Result<PingResult>.Success(new PingResult(response.StatusCode == HttpStatusCode.OK,
                (long)sw.Elapsed.TotalMilliseconds));
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            // Per-target таймаут probe — HTTP-ответа нет — Failed
            return Result<PingResult>.Failed(new PeerRejectedException(
                $"probe {target.Name}: таймаут {timeout.TotalMilliseconds:F0} мс"));
        }
        catch (HttpRequestException ex)
        {
            // Транспортный обрыв (connection refused/DNS) — Failed
            return Result<PingResult>.Failed(new PeerRejectedException(
                $"probe {target.Name}: обрыв транспорта ({ex.Message})"));
        }
        // Отмена внешнего ct — OperationCanceledException пробрасывается вызывающему
    }

    // ── внутренняя механика ──

    private async Task<byte[]> SendListingAsync(TargetRecord target, string query, CancellationToken ct)
    {
        using var request = BuildRequest(target, HttpMethod.Get, "/replication/listing", query,
            payload: null, payloadHash: null);
        try
        {
            using var response = await _http.SendAsync(request, HttpCompletionOption.ResponseContentRead, ct);
            if (!response.IsSuccessStatusCode)
                throw new PeerRejectedException($"HTTP {(int)response.StatusCode} от {target.Name}");
            return await response.Content.ReadAsByteArrayAsync(ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw; // внешняя отмена — не исход операции
        }
        catch (PeerRejectedException)
        {
            throw; // уже типизированный отказ — наверх как Failed вызывающего
        }
        catch (Exception ex)
        {
            throw new PeerRejectedException($"транспорт листинга у {target.Name}: {ex.Message}");
        }
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
        List<(string Name, string Value)> headers;
        string authorization;
        if (target.Auth == TargetRecord.AuthKey)
        {
            // Схема OWNS3-PEER-ECC-P256-SHA256 для key-записей (t39, глава 07 §6):
            // подпись приватным ключом identity; identity != null гарантировано
            // пре-чеками всех публичных методов, throw — страховка будущих точек
            // вызова BuildRequest.
            var peerIdentity = _identity ?? throw new InvalidOperationException(
                $"key-запись {target.Name} без identity (env OWNS3_REPLICATION_CERT/_PRIVATE_KEY)");
            var peerDate = at.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture);
            headers =
            [
                ("host", endpoint.Authority),
                ("x-owns3-peer-date", peerDate),
                ("x-owns3-peer-content-sha256", payloadHashValue),
            ];
            (authorization, _, _) = PeerRequestSigner.Sign(peerIdentity.Key, peerIdentity.Name,
                method.Method, path, query, headers, payloadHashValue, at);
        }
        else
        {
            // legacy: SigV4-подпись кредами цели — прежняя механика байт-в-байт
            // (креды обязательны по валидации состава, глава 07 §2).
            var amzDate = at.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture);
            headers =
            [
                ("host", endpoint.Authority),
                ("x-amz-date", amzDate),
                ("x-amz-content-sha256", payloadHashValue),
            ];
            (authorization, _, _) = SigV4Signer.Sign(target.SecretKey!, target.AccessKey!, method.Method,
                path, query, headers, payloadHashValue, at);
        }
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
