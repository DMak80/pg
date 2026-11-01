using AdminPanel.Etcd.Workers;
using Shared.Core.CQRS;
using Shared.Core.DI;

namespace AdminPanel.Api.Operations.Valkey;

// Отмена живых заявок ротаций valkey (u17, arch/02 §11.2-7/8): панель —
// прокси DELETE в API ValkeyWorker (del ключа выполняет ТОЛЬКО воркер, D13).
// DELETE заявку не создаёт, но оператор (username сессии) уходит воркеру
// заголовком X-Requested-By: воркер пишет исход отмены в host-лог (аудит
// отмен — arch/02 §11.2). Имя кластера — каноническое (path-интерполяция
// прокси, §11.3): неканоническое — 404 до вызова воркера.
public sealed record CancelValkeyRotationCommand(string Cluster, string Path, string RequestedBy)
    : ICommand<ValkeyRotationCancelledDto>;

// 204 воркера без тела → dto default (паттерн CancelKafkaRebalanceCommand).
public sealed record ValkeyRotationCancelledDto(string Cluster);

[InjectAsScoped]
public sealed class CancelValkeyRotationCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<CancelValkeyRotationCommand, ValkeyRotationCancelledDto>
{
    public ValueTask<Result<ValkeyRotationCancelledDto>> Handle(
        CancelValkeyRotationCommand command, CancellationToken ct)
    {
        if (!CanonicalNames.IsCanonicalCluster(command.Cluster))
            return ValueTask.FromResult(
                Result<ValkeyRotationCancelledDto>.Failed(CanonicalNames.NotFoundProblem(command.Cluster)));

        return WorkerProxy.SendAsync<ValkeyRotationCancelledDto>(
            api, "valkeyworker", HttpMethod.Delete,
            $"/api/valkey/clusters/{command.Cluster}/{command.Path}",
            body: null, requestedBy: command.RequestedBy, ct);
    }
}
