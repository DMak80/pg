using KafkaWorker.Core;
using KafkaWorker.Core.Writing;
using KafkaWorker.Etcd.Client;

namespace KafkaWorker.App.Api.Operations;

// Ответ 201 POST /api/kafka/clusters/{c}/rebalance (арх-канон; дубль осознан).
public sealed record KafkaRebalanceRequestedDto(string Cluster, long RequestedUnix, string RequestedBy);

// Ребалансировка партиций через API воркера (arch/02 §10.2-13/14): клэйм-txn
// /kafkaworker/rebalances/<C> (протокол ротаций §9.8 один в один); исполнение —
// PartitionReassigner воркера (arch/16 §5 I); отмена — del ключа (новые батчи
// не подаются, поданные Kafka доиграет сама). Порт панельных
// RequestKafkaRebalanceCommandHandler + CancelKafkaRebalanceCommandHandler.
// requested_by — заголовок X-Requested-By (панель шлёт оператора), fallback
// "api" (у панели ClaimsPrincipal; значения etcd не меняются, spec §3.7).
public sealed class RebalanceHandler(IEtcdGateway gateway, string[] endpoints, TimeProvider time)
{
    public async Task<Result<KafkaRebalanceRequestedDto>> RequestAsync(
        string cluster, string requestedBy, CancellationToken ct)
    {
        // Имя каноническое (§10.3), иначе 404.
        if (!KafkaLimits.ClusterPattern().IsMatch(cluster))
            return Result<KafkaRebalanceRequestedDto>.Failed(new KafkaClusterNotFoundException(cluster));

        var config = await KafkaApiHelpers.ReadConfigAsync(gateway, endpoints, cluster, ct);
        if (config.Error is not null)
            return Result<KafkaRebalanceRequestedDto>.Failed(config.Error);
        if (config.Value is null)
            return Result<KafkaRebalanceRequestedDto>.Failed(new KafkaClusterNotFoundException(cluster));
        if (config.Value.State is not null)
            return Result<KafkaRebalanceRequestedDto>.Failed(
                new KafkaClusterNotActiveException(cluster, config.Value.State));

        // Живая заявка → 409 (воркер снимет по сходимости).
        var key = $"/kafkaworker/rebalances/{cluster}";
        var ticket = await KafkaApiHelpers.ReadKeyAsync(gateway, endpoints, key, ct);
        if (!ticket.IsSuccess)
            return Result<KafkaRebalanceRequestedDto>.Failed(ticket.Error!);
        if (ticket.Value is not null)
            return Result<KafkaRebalanceRequestedDto>.Failed(
                new KafkaRebalanceAlreadyRequestedException(cluster));

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
            return Result<KafkaRebalanceRequestedDto>.Failed(txn.Error!);
        if (!txn.Value.Succeeded)
            return Result<KafkaRebalanceRequestedDto>.Failed(
                new KafkaRebalanceAlreadyRequestedException(cluster));

        return Result<KafkaRebalanceRequestedDto>.Success(
            new KafkaRebalanceRequestedDto(cluster, requestedUnix, requestedBy));
    }

    public async Task<Result> CancelAsync(string cluster, CancellationToken ct)
    {
        // Имя каноническое (§10.3), иначе 404.
        if (!KafkaLimits.ClusterPattern().IsMatch(cluster))
            return Result.Failed(new KafkaClusterNotFoundException(cluster));

        // Заявки нет → 404.
        var key = $"/kafkaworker/rebalances/{cluster}";
        var ticket = await KafkaApiHelpers.ReadKeyAsync(gateway, endpoints, key, ct);
        if (!ticket.IsSuccess)
            return Result.Failed(ticket.Error!);
        if (ticket.Value is null)
            return Result.Failed(new KafkaRebalanceNotFoundException(cluster));

        // Отмена безопасна: новых батчей не будет, поданные Kafka доиграет сама.
        var deleted = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.DeleteAsync(endpoint, key, prefix: false, ct));
        return deleted.IsSuccess ? Result.Success() : Result.Failed(deleted.Error!);
    }
}
