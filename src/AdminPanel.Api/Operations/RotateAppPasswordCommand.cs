using AdminPanel.Etcd.Workers;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Operations;

// Заявка ротации app-пароля — прокси в API PgWorker (арх-канон arch/02 §9.8):
// заявку клэймит воркер, выполняет AppPasswordRotator. Панель сама в SQL ноды
// не ходит и app_password не пишет/не читает.
public sealed record RotateAppPasswordCommand(string Cluster, string RequestedBy)
    : ICommand<AppPasswordRotatedDto>;

public sealed record AppPasswordRotatedDto(string Cluster, long RequestedUnix, string RequestedBy);

// Прокси: панель не пишет в etcd — команда уходит в API PgWorker (arch/14 §1.1);
// requestedBy передаётся заголовком X-Requested-By — воркер пишет его в
// requested_by заявки (значения etcd не меняются, spec §3.7).
[InjectAsScoped]
public sealed class RotateAppPasswordCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<RotateAppPasswordCommand, AppPasswordRotatedDto>
{
    public async ValueTask<Result<AppPasswordRotatedDto>> Handle(
        RotateAppPasswordCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<AppPasswordRotatedDto>(
            api, "pgworker", HttpMethod.Post, $"/api/clusters/{command.Cluster}/app-password/rotate",
            body: null, command.RequestedBy, ct);
}
