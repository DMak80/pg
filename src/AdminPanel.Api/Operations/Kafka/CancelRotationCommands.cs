using AdminPanel.Etcd.Workers;
using Shared.Core.CQRS;
using Shared.Core.DI;

namespace AdminPanel.Api.Operations.Kafka;

// Отмена живых заявок ротаций kafka (u17, arch/02 §10.2-18/19/20): панель —
// прокси DELETE в API KafkaWorker (del ключа выполняет ТОЛЬКО воркер, D13).
// DELETE заявку не создаёт, но оператор (username сессии) уходит воркеру
// заголовком X-Requested-By: воркер пишет исход отмены в host-лог (аудит
// отмен — arch/02 §10.2). Имя кластера — каноническое (path-интерполяция
// прокси, §10.3): неканоническое — 404 до вызова воркера.
public sealed record CancelKafkaRotationCommand(string Cluster, string Path, string RequestedBy)
    : ICommand<KafkaRotationCancelledDto>;

// 204 воркера без тела → dto default (паттерн CancelKafkaRebalanceCommand).
public sealed record KafkaRotationCancelledDto(string Cluster);

[InjectAsScoped]
public sealed class CancelKafkaRotationCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<CancelKafkaRotationCommand, KafkaRotationCancelledDto>
{
    public ValueTask<Result<KafkaRotationCancelledDto>> Handle(
        CancelKafkaRotationCommand command, CancellationToken ct)
    {
        if (!CanonicalNames.IsCanonicalCluster(command.Cluster))
            return ValueTask.FromResult(
                Result<KafkaRotationCancelledDto>.Failed(CanonicalNames.NotFoundProblem(command.Cluster)));

        return WorkerProxy.SendAsync<KafkaRotationCancelledDto>(
            api, "kafkaworker", HttpMethod.Delete,
            $"/api/kafka/clusters/{command.Cluster}/{command.Path}",
            body: null, requestedBy: command.RequestedBy, ct);
    }
}
