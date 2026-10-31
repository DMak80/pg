using AdminPanel.Etcd.Workers;
using Shared.Core.CQRS;
using Shared.Core.DI;

namespace AdminPanel.Api.Operations.Kafka;

// Отмена живых заявок ротаций kafka (u17, arch/02 §10.2-18/19/20): панель —
// прокси DELETE в API KafkaWorker (del ключа заявки выполняет ТОЛЬКО воркер,
// D13). DELETE не создаёт заявку → requestedBy=null — консистентно образцу
// CancelKafkaRebalanceCommandHandler (осознанное решение).
public sealed record CancelKafkaRotationCommand(string Cluster, string Path)
    : ICommand<KafkaRotationCancelledDto>;

// 204 воркера без тела → dto default (паттерн CancelKafkaRebalanceCommand).
public sealed record KafkaRotationCancelledDto(string Cluster);

[InjectAsScoped]
public sealed class CancelKafkaRotationCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<CancelKafkaRotationCommand, KafkaRotationCancelledDto>
{
    public ValueTask<Result<KafkaRotationCancelledDto>> Handle(
        CancelKafkaRotationCommand command, CancellationToken ct)
        => WorkerProxy.SendAsync<KafkaRotationCancelledDto>(
            api, "kafkaworker", HttpMethod.Delete,
            $"/api/kafka/clusters/{command.Cluster}/{command.Path}",
            body: null, requestedBy: null, ct);
}
