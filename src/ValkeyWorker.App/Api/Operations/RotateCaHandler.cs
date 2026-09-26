using System.Text.Json;
using Shared.Core;
using Shared.Etcd.Client;

namespace ValkeyWorker.App.Api.Operations;

// Ответ 202 POST /api/valkey/clusters/{c}/ca/rotate (арх-канон; дубль осознан).
public sealed record ValkeyCaRotatedDto(string Cluster, long RequestedUnix, string RequestedBy);

// Заявка ротации per-cluster CA/сертов через API воркера (t07, arch/21 §5 K):
// клэйм-txn /valkeyworker/ca_rotations/<C> NotExists; исполнение — CaRotator
// (фазы P/D/R/C). state-гейт: НЕ-Active (NOT_INITIALIZED/TO_REMOVE) → 409 —
// ротация не поднятого кластера бессмысленна (образец kafka, ОТЛИЧИЕ от
// ротации паролей). requested_by — заголовок X-Requested-By, fallback "api".
public sealed class RotateCaHandler(IEtcdGateway gateway, string[] endpoints, TimeProvider time)
{
    public async Task<Result<ValkeyCaRotatedDto>> HandleAsync(
        string cluster, string requestedBy, CancellationToken ct)
    {
        // Имя каноническое, иначе 404.
        if (!ValkeyLimits.ClusterPattern().IsMatch(cluster))
            return Result<ValkeyCaRotatedDto>.Failed(new ValkeyClusterNotFoundException(cluster));

        // Кластер существует; НЕ-Active → 409.
        var config = await ValkeyApiHelpers.ReadConfigAsync(gateway, endpoints, cluster, ct);
        if (config.Error is not null)
            return Result<ValkeyCaRotatedDto>.Failed(config.Error);
        if (config.Value is null)
            return Result<ValkeyCaRotatedDto>.Failed(new ValkeyClusterNotFoundException(cluster));
        if (config.Value.State is not null)
            return Result<ValkeyCaRotatedDto>.Failed(
                new ValkeyClusterNotActiveException(cluster, config.Value.State));

        // Живая заявка → 409 (после исполнения ключ исчезает — POST валиден).
        var key = $"/valkeyworker/ca_rotations/{cluster}";
        var ticket = await ValkeyApiHelpers.ReadKeyAsync(gateway, endpoints, key, ct);
        if (!ticket.IsSuccess)
            return Result<ValkeyCaRotatedDto>.Failed(ticket.Error!);
        if (ticket.Value is not null)
            return Result<ValkeyCaRotatedDto>.Failed(new ValkeyRotationAlreadyRequestedException(cluster));

        // Клэйм-txn: compare NotExists + put (протокол §9.8; БЕЗ role).
        var requestedUnix = time.GetUtcNow().ToUnixTimeSeconds();
        var txn = await ValkeyEtcdFailover.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.NotExists(key)],
                [new TxnOp.Put(
                    key, JsonSerializer.Serialize(new CaRotationTicketJson(requestedUnix, requestedBy)), null)]),
            ct));
        if (!txn.IsSuccess)
            return Result<ValkeyCaRotatedDto>.Failed(txn.Error!);
        if (!txn.Value.Succeeded)
            return Result<ValkeyCaRotatedDto>.Failed(new ValkeyRotationAlreadyRequestedException(cluster));

        return Result<ValkeyCaRotatedDto>.Success(
            new ValkeyCaRotatedDto(cluster, requestedUnix, requestedBy));
    }
}

// Заявка ротации CA (arch/20 §3): {"requested_unix","requested_by"} — без role.
public sealed record CaRotationTicketJson(
    [property: System.Text.Json.Serialization.JsonPropertyName("requested_unix")] long RequestedUnix,
    [property: System.Text.Json.Serialization.JsonPropertyName("requested_by")] string? RequestedBy);
