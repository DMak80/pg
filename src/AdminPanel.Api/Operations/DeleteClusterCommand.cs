using AdminPanel.Etcd.Workers;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Operations;

// Команда удаления кластера — прокси в API PgWorker: воркер переводит
// config.state в TO_REMOVE (arch/02 §9.4); панель не пишет в etcd.
public sealed record DeleteClusterCommand(string Name) : ICommand<ClusterDeletedDto>;

// Результат DELETE /api/clusters/{name} (arch/03 §1.2); эндпоинт отвечает 204.
public sealed record ClusterDeletedDto(string Name, string State);

// Прокси: панель не пишет в etcd — команда уходит в API PgWorker (arch/14 §1.1);
// у delete нет оператора — requestedBy: null (заголовок не шлётся).
[InjectAsScoped]
public sealed class DeleteClusterCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<DeleteClusterCommand, ClusterDeletedDto>
{
    public async ValueTask<Result<ClusterDeletedDto>> Handle(DeleteClusterCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<ClusterDeletedDto>(
            api, "pgworker", HttpMethod.Delete, $"/api/clusters/{command.Name}",
            body: null, requestedBy: null, ct);
}
