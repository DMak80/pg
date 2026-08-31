using AdminPanel.Etcd.Workers;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Operations;

// Команда пересоздания ноды — прокси в API PgWorker: воркер ставит маркер
// nodes/<n>/state=TO_RECREATE + режим nodes/<n>/recreate=soft|hard; rebuild
// выполнит NodeSupervisor (soft — switchover сначала; hard — снос сразу).
public sealed record RecreateNodeCommand(string Scope, string Node, string? Mode = null) : ICommand<NodeRecreatedDto>;

public sealed record NodeRecreatedDto(string Scope, string Node, string State, string Mode);

// Тело POST /api/ha/{scope}/nodes/{node}/recreate (mode опционален: soft).
public sealed record RecreateNodeRequest(string? Mode);

// Прокси: панель не пишет в etcd — команда уходит в API PgWorker (arch/14 §1.1);
// у recreate нет оператора (requested_by не участвует) — requestedBy: null.
[InjectAsScoped]
public sealed class RecreateNodeCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<RecreateNodeCommand, NodeRecreatedDto>
{
    public async ValueTask<Result<NodeRecreatedDto>> Handle(RecreateNodeCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<NodeRecreatedDto>(
            api, "pgworker", HttpMethod.Post, $"/api/ha/{command.Scope}/nodes/{command.Node}/recreate",
            command.Mode is null ? null : new RecreateNodeRequest(command.Mode),
            requestedBy: null, ct);
}
