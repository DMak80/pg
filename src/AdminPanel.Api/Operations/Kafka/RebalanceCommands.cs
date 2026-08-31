using AdminPanel.Etcd.Workers;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Operations.Kafka;

// ===== 13–14. Ребалансировка партиций — заявка и отмена (t02, 02 §10.2-13/14):
// прокси в API KafkaWorker — заявку клэймит воркер, исполняет и снимает =====

public sealed record RequestKafkaRebalanceCommand(string Cluster, string RequestedBy)
    : ICommand<KafkaRebalanceRequestedDto>;

public sealed record KafkaRebalanceRequestedDto(string Cluster, long RequestedUnix, string RequestedBy);

[InjectAsScoped]
public sealed class RequestKafkaRebalanceCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<RequestKafkaRebalanceCommand, KafkaRebalanceRequestedDto>
{
    public async ValueTask<Result<KafkaRebalanceRequestedDto>> Handle(
        RequestKafkaRebalanceCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<KafkaRebalanceRequestedDto>(
            api, "kafkaworker", HttpMethod.Post, $"/api/kafka/clusters/{command.Cluster}/rebalance",
            body: null, command.RequestedBy, ct);
}

public sealed record CancelKafkaRebalanceCommand(string Cluster) : ICommand<KafkaRebalanceCancelledDto>;

public sealed record KafkaRebalanceCancelledDto(string Cluster);

[InjectAsScoped]
public sealed class CancelKafkaRebalanceCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<CancelKafkaRebalanceCommand, KafkaRebalanceCancelledDto>
{
    public async ValueTask<Result<KafkaRebalanceCancelledDto>> Handle(
        CancelKafkaRebalanceCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<KafkaRebalanceCancelledDto>(
            api, "kafkaworker", HttpMethod.Delete, $"/api/kafka/clusters/{command.Cluster}/rebalance",
            body: null, requestedBy: null, ct);
}
