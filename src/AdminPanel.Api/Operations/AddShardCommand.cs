using AdminPanel.Etcd.Workers;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Operations;

// Тело POST /api/clusters/{cluster}/shards (arch/03 §1.3): биндится как JSON и
// уходит в API PgWorker как есть (валидирует воркер — источник истины, spec §3.4).
public sealed record AddShardRequest(int Replicas, decimal RequestCpu, int RequestMem, int RequestDisk);

// Команда добавления шарда — прокси в API PgWorker (arch/02 §9.5).
public sealed record AddShardCommand(string Cluster, AddShardRequest Request) : ICommand<ShardAddedDto>;

// Ответ 201 POST /api/clusters/{cluster}/shards (arch/03 §1.3; из ответа воркера).
public sealed record ShardAddedDto(
    string Cluster, string Name, int Replicas,
    string RequestCpu, string RequestMem, string RequestDisk, string State);

// Прокси: панель не пишет в etcd — команда уходит в API PgWorker (arch/14 §1.1);
// у add-shard нет оператора — requestedBy: null.
[InjectAsScoped]
public sealed class AddShardCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<AddShardCommand, ShardAddedDto>
{
    public async ValueTask<Result<ShardAddedDto>> Handle(AddShardCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<ShardAddedDto>(
            api, "pgworker", HttpMethod.Post, $"/api/clusters/{command.Cluster}/shards",
            command.Request, requestedBy: null, ct);
}
