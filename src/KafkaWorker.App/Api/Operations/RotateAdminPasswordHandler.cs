using KafkaWorker.Core;
using KafkaWorker.Core.Writing;
using KafkaWorker.Etcd.Client;

namespace KafkaWorker.App.Api.Operations;

// Ответ 201 POST /api/kafka/clusters/{c}/admin-password/rotate (мутация №16,
// adminpanel/02 §10.2; дубль осознан).
public sealed record KafkaAdminPasswordRotatedDto(string Cluster, long RequestedUnix, string RequestedBy);

// Заявка ротации admin-пароля через API воркера (мутация №16, adminpanel/02
// §10.2): клэйм-txn /kafkaworker/admin_rotations/<C>; исполнение — PasswordRotator
// (роль admin) воркера (фазы A/B/C, arch/16 §5 H). Порт RotateAppPasswordHandler.
// requested_by — заголовок X-Requested-By (панель шлёт оператора), fallback
// "api" (у панели ClaimsPrincipal; значения etcd не меняются, spec §3.7).
public sealed class RotateAdminPasswordHandler(IEtcdGateway gateway, string[] endpoints, TimeProvider time)
{
    public async Task<Result<KafkaAdminPasswordRotatedDto>> HandleAsync(
        string cluster, string requestedBy, CancellationToken ct)
    {
        // Имя каноническое (§10.3), иначе 404.
        if (!KafkaLimits.ClusterPattern().IsMatch(cluster))
            return Result<KafkaAdminPasswordRotatedDto>.Failed(new KafkaClusterNotFoundException(cluster));

        var config = await KafkaApiHelpers.ReadConfigAsync(gateway, endpoints, cluster, ct);
        if (config.Error is not null)
            return Result<KafkaAdminPasswordRotatedDto>.Failed(config.Error);
        if (config.Value is null)
            return Result<KafkaAdminPasswordRotatedDto>.Failed(new KafkaClusterNotFoundException(cluster));
        if (config.Value.State is not null)
            return Result<KafkaAdminPasswordRotatedDto>.Failed(
                new KafkaClusterNotActiveException(cluster, config.Value.State));

        // Живая заявка → 409 (после исполнения ключ исчезает — POST валиден).
        var key = $"/kafkaworker/admin_rotations/{cluster}";
        var ticket = await KafkaApiHelpers.ReadKeyAsync(gateway, endpoints, key, ct);
        if (!ticket.IsSuccess)
            return Result<KafkaAdminPasswordRotatedDto>.Failed(ticket.Error!);
        if (ticket.Value is not null)
            return Result<KafkaAdminPasswordRotatedDto>.Failed(new KafkaRotationAlreadyRequestedException(cluster));

        // Клэйм-txn: compare NotExists + put (pg §9.8 один в один).
        var requestedUnix = time.GetUtcNow().ToUnixTimeSeconds();
        var txn = await EtcdFailover.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.NotExists(key)],
                [new TxnOp.Put(
                    key, new KafkaRotationTicketJson(requestedUnix, requestedBy).Serialize(), null)]),
            ct));
        if (!txn.IsSuccess)
            return Result<KafkaAdminPasswordRotatedDto>.Failed(txn.Error!);
        if (!txn.Value.Succeeded)
            return Result<KafkaAdminPasswordRotatedDto>.Failed(new KafkaRotationAlreadyRequestedException(cluster));

        return Result<KafkaAdminPasswordRotatedDto>.Success(
            new KafkaAdminPasswordRotatedDto(cluster, requestedUnix, requestedBy));
    }
}
