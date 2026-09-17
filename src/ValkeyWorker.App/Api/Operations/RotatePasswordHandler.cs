using System.Text.Json;
using Shared.Core;
using Shared.Etcd.Client;

namespace ValkeyWorker.App.Api.Operations;

// Ответ 202 POST /api/valkey/clusters/{c}/password/rotate (сигнатуры — порт kfw).
public sealed record ValkeyPasswordRotatedDto(string Cluster, string Role, long RequestedUnix, string RequestedBy);

// Заявка ротации пароля через API воркера (arch/21 §5 E): клэйм-txn
// /valkeyworker/rotations/<C> version==0; исполнение — PasswordRotator (окно
// двух паролей). requested_by — заголовок X-Requested-By, fallback "api".
public sealed class RotatePasswordHandler(IEtcdGateway gateway, string[] endpoints, TimeProvider time)
{
    public async Task<Result<ValkeyPasswordRotatedDto>> HandleAsync(
        string cluster, string role, string requestedBy, CancellationToken ct)
    {
        // Имя каноническое (§2), иначе 404.
        if (!ValkeyLimits.ClusterPattern().IsMatch(cluster))
            return Result<ValkeyPasswordRotatedDto>.Failed(new ValkeyClusterNotFoundException(cluster));

        // Роль канона (arch/21 §5 E): app | admin.
        if (role is not ("app" or "admin"))
            return Result<ValkeyPasswordRotatedDto>.Failed(new ValkeyValidationException(
                [new ValidationError("role", "role: app|admin")]));

        // Кластер существует (config-ключ есть); state-гейта нет — заявка
        // ротации легальна для любого существующего кластера (spec §4.7).
        var config = await ValkeyApiHelpers.ReadConfigAsync(gateway, endpoints, cluster, ct);
        if (config.Error is not null)
            return Result<ValkeyPasswordRotatedDto>.Failed(config.Error);
        if (config.Value is null)
            return Result<ValkeyPasswordRotatedDto>.Failed(new ValkeyClusterNotFoundException(cluster));

        // Живая заявка → 409 (после исполнения ключ исчезает — POST валиден).
        var key = $"/valkeyworker/rotations/{cluster}";
        var ticket = await ValkeyApiHelpers.ReadKeyAsync(gateway, endpoints, key, ct);
        if (!ticket.IsSuccess)
            return Result<ValkeyPasswordRotatedDto>.Failed(ticket.Error!);
        if (ticket.Value is not null)
            return Result<ValkeyPasswordRotatedDto>.Failed(new ValkeyRotationAlreadyRequestedException(cluster));

        // Клэйм-txn: compare NotExists + put.
        var requestedUnix = time.GetUtcNow().ToUnixTimeSeconds();
        var txn = await ValkeyEtcdFailover.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.NotExists(key)],
                [new TxnOp.Put(
                    key, JsonSerializer.Serialize(new RotationTicketJson(role, requestedUnix, requestedBy)), null)]),
            ct));
        if (!txn.IsSuccess)
            return Result<ValkeyPasswordRotatedDto>.Failed(txn.Error!);
        if (!txn.Value.Succeeded)
            return Result<ValkeyPasswordRotatedDto>.Failed(new ValkeyRotationAlreadyRequestedException(cluster));

        return Result<ValkeyPasswordRotatedDto>.Success(
            new ValkeyPasswordRotatedDto(cluster, role, requestedUnix, requestedBy));
    }
}

// Заявка ротации (arch/20 §3): {"role":"app"|"admin","requested_unix","requested_by"}.
public sealed record RotationTicketJson(
    [property: System.Text.Json.Serialization.JsonPropertyName("role")] string Role,
    [property: System.Text.Json.Serialization.JsonPropertyName("requested_unix")] long RequestedUnix,
    [property: System.Text.Json.Serialization.JsonPropertyName("requested_by")] string? RequestedBy);
