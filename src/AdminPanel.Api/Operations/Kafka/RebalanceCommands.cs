using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Writing;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Operations.Kafka;

// ===== 9/10. Ребалансировка партиций — заявка и отмена (t02, 02 §10.2-9/10;
// клэйм-txn как ротация: панель пишет заявку /kafkaworker/rebalances/<C>,
// воркер исполняет и снимает; панель никогда не зовёт Kafka-мутации) =====

public sealed record RequestKafkaRebalanceCommand(string Cluster, string RequestedBy)
    : ICommand<KafkaRebalanceRequestedDto>;

public sealed record KafkaRebalanceRequestedDto(string Cluster, long RequestedUnix, string RequestedBy);

public sealed class KafkaRebalanceAlreadyRequestedException(string cluster)
    : Exception($"ребалансировка партиций {cluster} уже запрошена — дождитесь исполнения или отмените");

[InjectAsScoped]
public sealed class RequestKafkaRebalanceCommandHandler(ISnapshotStore store, IEtcdGateway gateway)
    : ICommandHandler<RequestKafkaRebalanceCommand, KafkaRebalanceRequestedDto>
{
    public async ValueTask<Result<KafkaRebalanceRequestedDto>> Handle(
        RequestKafkaRebalanceCommand command, CancellationToken ct)
    {
        var cluster = command.Cluster;

        // Имя каноническое (§10.3), иначе 404.
        if (!KafkaLimits.ClusterPattern().IsMatch(cluster))
            return Result<KafkaRebalanceRequestedDto>.Failed(new KafkaClusterNotFoundException(cluster));

        if (KafkaCommandHelpers.ActiveEndpoint(store) is not { } endpoint)
            return Result<KafkaRebalanceRequestedDto>.Failed(new EtcdWriteUnavailableException());

        var config = await KafkaCommandHelpers.ReadConfigAsync(gateway, endpoint, cluster, ct);
        if (config.Error is not null)
            return Result<KafkaRebalanceRequestedDto>.Failed(config.Error);
        if (config.Value is null)
            return Result<KafkaRebalanceRequestedDto>.Failed(new KafkaClusterNotFoundException(cluster));
        if (config.Value.State is not null)
            return Result<KafkaRebalanceRequestedDto>.Failed(
                new KafkaClusterNotActiveException(cluster, config.Value.State));

        // Живая заявка → 409 (панель не перезаписывает; воркер снимет по сходимости).
        var key = $"/kafkaworker/rebalances/{cluster}";
        var ticket = await KafkaCommandHelpers.ReadKeyAsync(gateway, endpoint, key, ct);
        if (!ticket.IsSuccess)
            return Result<KafkaRebalanceRequestedDto>.Failed(new EtcdWriteUnavailableException());
        if (ticket.Value is not null)
            return Result<KafkaRebalanceRequestedDto>.Failed(new KafkaRebalanceAlreadyRequestedException(cluster));

        // Клэйм-txn: compare version==0 + put (pg §9.8 один в один).
        var requestedUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var txn = await gateway.TxnAsync(
            endpoint, [new TxnCompare(key, 0)],
            [new KvPut(key, new KafkaRotationTicketJson(requestedUnix, command.RequestedBy).Serialize())], ct);
        if (!txn.IsSuccess)
            return Result<KafkaRebalanceRequestedDto>.Failed(new EtcdWriteUnavailableException());
        if (!txn.Value.Succeeded)
            return Result<KafkaRebalanceRequestedDto>.Failed(new KafkaRebalanceAlreadyRequestedException(cluster));

        return Result<KafkaRebalanceRequestedDto>.Success(
            new KafkaRebalanceRequestedDto(cluster, requestedUnix, command.RequestedBy));
    }
}

public sealed record CancelKafkaRebalanceCommand(string Cluster) : ICommand<KafkaRebalanceCancelledDto>;

public sealed record KafkaRebalanceCancelledDto(string Cluster);

public sealed class KafkaRebalanceNotFoundException(string cluster)
    : Exception($"заявка ребалансировки {cluster} не найдена");

[InjectAsScoped]
public sealed class CancelKafkaRebalanceCommandHandler(ISnapshotStore store, IEtcdGateway gateway)
    : ICommandHandler<CancelKafkaRebalanceCommand, KafkaRebalanceCancelledDto>
{
    public async ValueTask<Result<KafkaRebalanceCancelledDto>> Handle(
        CancelKafkaRebalanceCommand command, CancellationToken ct)
    {
        var cluster = command.Cluster;

        // Имя каноническое (§10.3), иначе 404.
        if (!KafkaLimits.ClusterPattern().IsMatch(cluster))
            return Result<KafkaRebalanceCancelledDto>.Failed(new KafkaClusterNotFoundException(cluster));

        if (KafkaCommandHelpers.ActiveEndpoint(store) is not { } endpoint)
            return Result<KafkaRebalanceCancelledDto>.Failed(new EtcdWriteUnavailableException());

        // Заявки нет → 404.
        var key = $"/kafkaworker/rebalances/{cluster}";
        var ticket = await KafkaCommandHelpers.ReadKeyAsync(gateway, endpoint, key, ct);
        if (!ticket.IsSuccess)
            return Result<KafkaRebalanceCancelledDto>.Failed(new EtcdWriteUnavailableException());
        if (ticket.Value is null)
            return Result<KafkaRebalanceCancelledDto>.Failed(new KafkaRebalanceNotFoundException(cluster));

        // Отмена безопасна: новых батчей не будет, поданные Kafka доиграет сама.
        var deleted = await gateway.DeleteAsync(endpoint, key, prefix: false, ct);
        if (!deleted.IsSuccess)
            return Result<KafkaRebalanceCancelledDto>.Failed(new EtcdWriteUnavailableException());

        return Result<KafkaRebalanceCancelledDto>.Success(new KafkaRebalanceCancelledDto(cluster));
    }
}
