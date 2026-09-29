using AdminPanel.Etcd.Workers;
using Shared.Core.CQRS;
using Shared.Core.DI;

namespace AdminPanel.Api.Operations;

// Сироты бэкапов: hold/unhold/delete-заявка (reliability t04, arch/02 §9.10) —
// прокси в API PgWorker; панель в etcd не пишет.
public sealed record HoldOrphanCommand(string Cluster, string Shard, string RequestedBy)
    : ICommand<OrphanMutatedDto>;

public sealed record UnholdOrphanCommand(string Cluster, string Shard)
    : ICommand<OrphanMutatedDto>;

public sealed record DeleteOrphanCommand(string Cluster, string Shard, string Confirm, string RequestedBy)
    : ICommand<OrphanDeleteAcceptedDto>;

// 204-ответы воркера без тела — DTO default (модуль его не читает).
public sealed record OrphanMutatedDto(string Prefix);

// 202-ответ заявки удаления: {prefix,requestedUnix,requestedBy} воркера.
public sealed record OrphanDeleteAcceptedDto(string Prefix, long RequestedUnix, string RequestedBy);

[InjectAsScoped]
public sealed class HoldOrphanCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<HoldOrphanCommand, OrphanMutatedDto>
{
    public async ValueTask<Result<OrphanMutatedDto>> Handle(HoldOrphanCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<OrphanMutatedDto>(
            api, "pgworker", HttpMethod.Post,
            $"/api/backups/orphans/{command.Cluster}/{command.Shard}/hold",
            body: null, requestedBy: command.RequestedBy, ct);
}

// unhold: DELETE идемпотентен, тела нет — оператора не шлём (как delete кластера).
[InjectAsScoped]
public sealed class UnholdOrphanCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<UnholdOrphanCommand, OrphanMutatedDto>
{
    public async ValueTask<Result<OrphanMutatedDto>> Handle(UnholdOrphanCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<OrphanMutatedDto>(
            api, "pgworker", HttpMethod.Delete,
            $"/api/backups/orphans/{command.Cluster}/{command.Shard}/hold",
            body: null, requestedBy: null, ct);
}

[InjectAsScoped]
public sealed class DeleteOrphanCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<DeleteOrphanCommand, OrphanDeleteAcceptedDto>
{
    public async ValueTask<Result<OrphanDeleteAcceptedDto>> Handle(DeleteOrphanCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<OrphanDeleteAcceptedDto>(
            api, "pgworker", HttpMethod.Post,
            $"/api/backups/orphans/{command.Cluster}/{command.Shard}/delete",
            body: new { confirm = $"{command.Cluster}/{command.Shard}" }, // confirm команды (сервер воркера перепроверит)
            requestedBy: command.RequestedBy, ct);
}
