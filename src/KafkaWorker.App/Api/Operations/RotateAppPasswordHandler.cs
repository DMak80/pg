using KafkaWorker.Core;
using KafkaWorker.Core.Writing;
using KafkaWorker.Etcd.Client;

namespace KafkaWorker.App.Api.Operations;

// Ответ 201 POST /api/kafka/clusters/{c}/app-password/rotate (арх-канон; дубль осознан).
public sealed record KafkaPasswordRotatedDto(string Cluster, long RequestedUnix, string RequestedBy);

// Заявка ротации app-пароля через API воркера (arch/02 §10.2-8): клэйм-txn
// /kafkaworker/rotations/<C>; исполнение — AppPasswordRotator воркера (фазы
// A/B/C, arch/16 §5 H). Порт панельного RotateKafkaPasswordCommandHandler.
// requested_by — заголовок X-Requested-By (панель шлёт оператора), fallback
// "api" (у панели ClaimsPrincipal; значения etcd не меняются, spec §3.7).
public sealed class RotateAppPasswordHandler(IEtcdGateway gateway, string[] endpoints, TimeProvider time)
{
    public async Task<Result<KafkaPasswordRotatedDto>> HandleAsync(
        string cluster, string requestedBy, CancellationToken ct)
    {
        // Имя каноническое (§10.3), иначе 404.
        if (!KafkaLimits.ClusterPattern().IsMatch(cluster))
            return Result<KafkaPasswordRotatedDto>.Failed(new KafkaClusterNotFoundException(cluster));

        var config = await KafkaApiHelpers.ReadConfigAsync(gateway, endpoints, cluster, ct);
        if (config.Error is not null)
            return Result<KafkaPasswordRotatedDto>.Failed(config.Error);
        if (config.Value is null)
            return Result<KafkaPasswordRotatedDto>.Failed(new KafkaClusterNotFoundException(cluster));
        if (config.Value.State is not null)
            return Result<KafkaPasswordRotatedDto>.Failed(
                new KafkaClusterNotActiveException(cluster, config.Value.State));

        // Живая заявка → 409 (после исполнения ключ исчезает — POST валиден).
        var key = $"/kafkaworker/rotations/{cluster}";
        var ticket = await KafkaApiHelpers.ReadKeyAsync(gateway, endpoints, key, ct);
        if (!ticket.IsSuccess)
            return Result<KafkaPasswordRotatedDto>.Failed(ticket.Error!);
        if (ticket.Value is not null)
            return Result<KafkaPasswordRotatedDto>.Failed(new KafkaRotationAlreadyRequestedException(cluster));

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
            return Result<KafkaPasswordRotatedDto>.Failed(txn.Error!);
        if (!txn.Value.Succeeded)
            return Result<KafkaPasswordRotatedDto>.Failed(new KafkaRotationAlreadyRequestedException(cluster));

        return Result<KafkaPasswordRotatedDto>.Success(
            new KafkaPasswordRotatedDto(cluster, requestedUnix, requestedBy));
    }
}
