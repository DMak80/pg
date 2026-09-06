using KafkaWorker.Core;
using KafkaWorker.Core.Writing;
using KafkaWorker.Etcd.Client;

namespace KafkaWorker.App.Api.Operations;

// Ответ 201 POST /api/kafka/clusters/{c}/ca/rotate (арх-канон; дубль осознан).
public sealed record CaRotatedDto(string Cluster, long RequestedUnix, string RequestedBy);

// Заявка ротации per-cluster CA/сертов через API воркера (arch/02 §10.2-17, t07):
// клэйм-txn /kafkaworker/ca_rotations/<C>; исполнение — CaRotator воркера
// (фазы P/D/R/C, arch/16 §5 K). Порт панельного RotateCaCommandHandler.
// requested_by — заголовок X-Requested-By (панель шлёт оператора), fallback
// "api" (у панели ClaimsPrincipal; значения etcd не меняются, spec §3.7).
public sealed class RotateCaHandler(IEtcdGateway gateway, string[] endpoints, TimeProvider time)
{
    public async Task<Result<CaRotatedDto>> HandleAsync(
        string cluster, string requestedBy, CancellationToken ct)
    {
        // Имя каноническое (§10.3), иначе 404.
        if (!KafkaLimits.ClusterPattern().IsMatch(cluster))
            return Result<CaRotatedDto>.Failed(new KafkaClusterNotFoundException(cluster));

        var config = await KafkaApiHelpers.ReadConfigAsync(gateway, endpoints, cluster, ct);
        if (config.Error is not null)
            return Result<CaRotatedDto>.Failed(config.Error);
        if (config.Value is null)
            return Result<CaRotatedDto>.Failed(new KafkaClusterNotFoundException(cluster));
        if (config.Value.State is not null)
            return Result<CaRotatedDto>.Failed(
                new KafkaClusterNotActiveException(cluster, config.Value.State));

        // Живая заявка → 409 (после исполнения ключ исчезает — POST валиден).
        var key = $"/kafkaworker/ca_rotations/{cluster}";
        var ticket = await KafkaApiHelpers.ReadKeyAsync(gateway, endpoints, key, ct);
        if (!ticket.IsSuccess)
            return Result<CaRotatedDto>.Failed(ticket.Error!);
        if (ticket.Value is not null)
            return Result<CaRotatedDto>.Failed(new KafkaRotationAlreadyRequestedException(cluster));

        // Клэйм-txn: compare NotExists + put (протокол ротаций §9.8 один в один).
        var requestedUnix = time.GetUtcNow().ToUnixTimeSeconds();
        var txn = await EtcdFailover.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.NotExists(key)],
                [new TxnOp.Put(
                    key, new KafkaRotationTicketJson(requestedUnix, requestedBy).Serialize(), null)]),
            ct));
        if (!txn.IsSuccess)
            return Result<CaRotatedDto>.Failed(txn.Error!);
        if (!txn.Value.Succeeded)
            return Result<CaRotatedDto>.Failed(new KafkaRotationAlreadyRequestedException(cluster));

        return Result<CaRotatedDto>.Success(
            new CaRotatedDto(cluster, requestedUnix, requestedBy));
    }
}
