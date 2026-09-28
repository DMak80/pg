using AdminPanel.Etcd.Workers;
using Shared.Core.CQRS;
using Shared.Core.DI;

namespace AdminPanel.Api.Operations;

// Тело PUT /api/clusters/{cluster}/config (t06, 02 §9.10): уходит в API
// PgWorker как есть; панель не валидирует — источник истины воркер.
public sealed record UpdateClusterConfigRequest(bool? SynchronousModeStrict);

public sealed record UpdateClusterConfigCommand(string Cluster, UpdateClusterConfigRequest Request)
    : ICommand<ClusterConfigUpdatedDto>;

// Панель отвечает 204 — DTO-заглушка (паттерн 204-мутаций DELETE; тело пустое).
public sealed record ClusterConfigUpdatedDto(string Cluster);

// Прокси: панель не пишет в etcd — команда уходит в API PgWorker (arch/14 §1.1);
// применение к Patroni — конвергенция DCS воркера («применит воркер», 02 §9.10).
[InjectAsScoped]
public sealed class UpdateClusterConfigCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<UpdateClusterConfigCommand, ClusterConfigUpdatedDto>
{
    public async ValueTask<Result<ClusterConfigUpdatedDto>> Handle(
        UpdateClusterConfigCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<ClusterConfigUpdatedDto>(
            api, "pgworker", HttpMethod.Put, $"/api/clusters/{command.Cluster}/config",
            command.Request, requestedBy: null, ct);
}
