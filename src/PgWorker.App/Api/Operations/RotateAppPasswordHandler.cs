using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using PgWorker.Core;
using PgWorker.Core.Writing;
using PgWorker.Etcd.Client;

namespace PgWorker.App.Api.Operations;

// Ответ 201 POST /api/clusters/{c}/app-password/rotate (арх-канон arch/02 §9.8; дубль осознан).
public sealed record AppPasswordRotatedDto(string Cluster, long RequestedUnix, string RequestedBy);

// Заявка ротации app-пароля через API воркера (task etcd-via-worker-api): порт
// панельного RotateAppPasswordCommandHandler; выполняет AppPasswordRotator
// (ALTER ROLE на всех шардах + атомарная замена app_password). requested_by —
// заголовок X-Requested-By, fallback "api" (у панели ClaimsPrincipal, spec §3.7).
public sealed partial class RotateAppPasswordHandler(IEtcdGateway gateway, string[] endpoints)
{
    // Канон тела заявки: snake_case (образец TicketBody MoveBucketsHandler).
    private static readonly JsonSerializerOptions TicketJson = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private sealed record TicketBody(
        [property: JsonPropertyName("requested_unix")] long RequestedUnix,
        [property: JsonPropertyName("requested_by")] string RequestedBy);

    // Имя кластера: ^[a-z][a-z0-9_]{0,62}$ (02 §9.3).
    [GeneratedRegex("^[a-z][a-z0-9_]{0,62}$")]
    private static partial Regex ClusterPattern();

    public async Task<Result<AppPasswordRotatedDto>> HandleAsync(string cluster, string requestedBy, CancellationToken ct)
    {
        // 1) Каноническое имя.
        if (!ClusterPattern().IsMatch(cluster))
            return Result<AppPasswordRotatedDto>.Failed(new ClusterNotFoundException(cluster));

        // 2) Config напрямую: нет → 404; state не Active → 409; битый → 503.
        var config = await ReadKeyAsync($"/clusters/{cluster}/config", ct);
        if (!config.IsSuccess)
            return Result<AppPasswordRotatedDto>.Failed(config.Error!);
        if (config.Value is null)
            return Result<AppPasswordRotatedDto>.Failed(new ClusterNotFoundException(cluster));
        string? state;
        try
        {
            state = ReadState(config.Value);
        }
        catch (JsonException)
        {
            return Result<AppPasswordRotatedDto>.Failed(new InvalidClusterConfigException(cluster));
        }
        if (state is not null)
            return Result<AppPasswordRotatedDto>.Failed(new ClusterNotActiveException(cluster, state));

        // 3) Живая заявка → 409 (после исполнения ключ исчезает — POST валиден).
        var key = $"/pgworker/rotations/{cluster}";
        var ticket = await ReadKeyAsync(key, ct);
        if (!ticket.IsSuccess)
            return Result<AppPasswordRotatedDto>.Failed(ticket.Error!);
        if (ticket.Value is not null)
            return Result<AppPasswordRotatedDto>.Failed(new RotationAlreadyRequestedException(cluster));

        // 4) Клэйм-txn: compare NotExists + put (образец §9.7 п.5).
        //    Проигрыш → 409; транспортный сбой → 503.
        var requestedUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var payload = JsonSerializer.Serialize(
            new TicketBody(requestedUnix, requestedBy), TicketJson);
        var txn = await EtcdFailover.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of([TxnCompare.NotExists(key)], [new TxnOp.Put(key, payload, null)]),
            ct));
        if (!txn.IsSuccess)
            return Result<AppPasswordRotatedDto>.Failed(new EtcdWriteUnavailableException());
        if (!txn.Value.Succeeded)
            return Result<AppPasswordRotatedDto>.Failed(new RotationAlreadyRequestedException(cluster));

        return Result<AppPasswordRotatedDto>.Success(
            new AppPasswordRotatedDto(cluster, requestedUnix, requestedBy));
    }

    private async Task<Result<string?>> ReadKeyAsync(string key, CancellationToken ct)
    {
        var range = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.RangeAsync(endpoint, key, ct));
        if (!range.IsSuccess)
            return Result<string?>.Failed(range.Error!);
        return Result<string?>.Success(range.Value.FirstOrDefault(kv => kv.Key == key)?.Value);
    }

    private static string? ReadState(string raw)
    {
        using var doc = JsonDocument.Parse(raw);
        return doc.RootElement.TryGetProperty("state", out var s) && s.ValueKind == JsonValueKind.String
            ? s.GetString()
            : null;
    }
}
