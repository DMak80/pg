using AdminPanel.Etcd.Workers;
using Shared.Core.CQRS;
using Shared.Core.DI;

namespace AdminPanel.Api.Operations;

// Restore-заявка (u-restore, arch/02 §9.13): панель — прокси POST
// /api/clusters/{c}/shards/{x}/restore API PgWorker (воркер уже реализован —
// RestoreShardHandler, arch/backups/03-restore.md); панель в etcd не пишет. requestedBy —
// username сессии (X-Requested-By, аудит воркера). Имена кластера/шарда —
// канонические (path-интерполяция прокси, §9.3): неканонические — 404 до
// вызова воркера (тот же код, что отвечает воркер на неканоническое имя).
public sealed record RestoreShardCommand(
    string Cluster, string Shard, string Confirm, string? TargetTime, string RequestedBy)
    : ICommand<System.Text.Json.JsonElement>;

[InjectAsScoped]
public sealed class RestoreShardCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<RestoreShardCommand, System.Text.Json.JsonElement>
{
    public ValueTask<Result<System.Text.Json.JsonElement>> Handle(
        RestoreShardCommand command, CancellationToken ct)
    {
        if (!CanonicalNames.IsCanonicalCluster(command.Cluster)
            || !CanonicalNames.IsCanonicalShard(command.Shard))
            return ValueTask.FromResult(Result<System.Text.Json.JsonElement>.Failed(
                CanonicalNames.NotFoundProblem($"{command.Cluster}/{command.Shard}")));

        return WorkerProxy.SendElementAsync(
            api, "pgworker", HttpMethod.Post,
            $"/api/clusters/{command.Cluster}/shards/{command.Shard}/restore",
            body: new { confirm = command.Confirm, target_time = command.TargetTime },
            requestedBy: command.RequestedBy, ct);
    }
}
