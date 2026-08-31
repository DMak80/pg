using AdminPanel.Etcd.Workers;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Operations;

// Тело POST /api/clusters (arch/03 §1.1): биндится Minimal API как JSON и
// уходит в API PgWorker как есть (панель не валидирует — источник истины
// воркер, spec §3.4). Sharded: отсутствует/null = true — совместимость
// старых клиентов (arch/02 §9.3; нормализует воркер).
public sealed record CreateClusterRequest(
    string Name,
    int Buckets,
    int Shards,
    int Replicas,
    decimal RequestCpu,
    int RequestMem,
    int RequestDisk,
    bool? Sharded = null);

// Команда создания кластера — прокси в API PgWorker (arch/14 §1.1): панель
// не пишет в etcd; воркер выполняет claim-txn/PUT/компенсацию 1:1 (§9.2).
public sealed record CreateClusterCommand(CreateClusterRequest Request) : ICommand<ClusterCreatedDto>;

// Ответ 201 POST /api/clusters (arch/03 §1.1; десериализуется из ответа воркера).
public sealed record ClusterCreatedDto(
    string Name,
    string DbName,
    bool Sharded,
    int BucketsCount,
    int ShardsTotal,
    int Replicas,
    string RequestCpu,
    string RequestMem,
    string RequestDisk,
    string State);

// Прокси: панель не пишет в etcd — команда уходит в API PgWorker (arch/14 §1.1);
// у create нет оператора (requested_by ключи не пишет) — requestedBy: null,
// заголовок X-Requested-By не шлётся (шлюз Task 12).
[InjectAsScoped]
public sealed class CreateClusterCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<CreateClusterCommand, ClusterCreatedDto>
{
    public async ValueTask<Result<ClusterCreatedDto>> Handle(CreateClusterCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<ClusterCreatedDto>(
            api, "pgworker", HttpMethod.Post, "/api/clusters", command.Request,
            requestedBy: null, ct);
}
