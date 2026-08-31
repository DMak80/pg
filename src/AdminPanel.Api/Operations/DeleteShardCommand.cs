using AdminPanel.Etcd.Workers;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Operations;

// Команда демонтажа шарда — прокси в API PgWorker: воркер ставит one-way маркер
// shards/<X>/state=TO_REMOVE (arch/02 §9.6); очистку выполняет PgWorker.
public sealed record DeleteShardCommand(string Cluster, string Shard) : ICommand<ShardDeletedDto>;

public sealed record ShardDeletedDto(string Cluster, string Shard, string State);

// Прокси: панель не пишет в etcd — команда уходит в API PgWorker (arch/14 §1.1);
// у delete-shard нет оператора — requestedBy: null.
[InjectAsScoped]
public sealed class DeleteShardCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<DeleteShardCommand, ShardDeletedDto>
{
    public async ValueTask<Result<ShardDeletedDto>> Handle(DeleteShardCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<ShardDeletedDto>(
            api, "pgworker", HttpMethod.Delete, $"/api/clusters/{command.Cluster}/shards/{command.Shard}",
            body: null, requestedBy: null, ct);
}
