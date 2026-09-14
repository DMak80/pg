using AdminPanel.Etcd.Workers;
using Shared.Core.CQRS;
using Shared.Core.DI;

namespace AdminPanel.Api.Operations;

// Заявка ротации per-cluster секретов (app + bucket_admin + bucket_mover, t02) —
// прокси в API PgWorker (арх-канон arch/02 §9.8): заявку клэймит воркер, выполняет
// ClusterSecretRotator. Панель сама в SQL ноды не ходит и креды не пишет/не читает.
public sealed record RotateClusterSecretsCommand(string Cluster, string RequestedBy)
    : ICommand<ClusterSecretsRotatedDto>;

public sealed record ClusterSecretsRotatedDto(string Cluster, long RequestedUnix, string RequestedBy);

// Прокси: панель не пишет в etcd — команда уходит в API PgWorker (arch/14 §1.1);
// requestedBy передаётся заголовком X-Requested-By — воркер пишет его в
// requested_by заявки (значения etcd не меняются, spec §3.7).
[InjectAsScoped]
public sealed class RotateClusterSecretsCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<RotateClusterSecretsCommand, ClusterSecretsRotatedDto>
{
    public async ValueTask<Result<ClusterSecretsRotatedDto>> Handle(
        RotateClusterSecretsCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<ClusterSecretsRotatedDto>(
            api, "pgworker", HttpMethod.Post, $"/api/clusters/{command.Cluster}/secrets/rotate",
            body: null, command.RequestedBy, ct);
}
