using AdminPanel.Etcd.Workers;
using Shared.Core.CQRS;
using Shared.Core.DI;

namespace AdminPanel.Api.Operations.Etcd;

// ===== Мутация состава etcd-контура (adminpanel/02 §9.13) — ПРОКСИ в API
// EtcdWorker (arch/25 §1.1): панель не пишет в etcd; воркер валидирует
// (N — целое 0..7, кворум-гвард external+N ≥ 3) и замещает декларацию
// /etcd/config целиком. Коды 1:1: 200 успех, 400 валидация, 409 гвард
// кворума, 503 живых инстансов нет. =====

// Тело PUT /api/etcd/config (панель): {"nodes":N} — замещение целиком.
public sealed record PutEtcdConfigRequest(int Nodes);

public sealed record PutEtcdConfigCommand(int Nodes) : ICommand<EtcdConfigPanelDto>;

// Ответ 200 (зеркало тела воркера {"nodes":N}).
public sealed record EtcdConfigPanelDto(int Nodes);

[InjectAsScoped]
public sealed class PutEtcdConfigCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<PutEtcdConfigCommand, EtcdConfigPanelDto>
{
    public async ValueTask<Result<EtcdConfigPanelDto>> Handle(PutEtcdConfigCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<EtcdConfigPanelDto>(
            api, "etcdworker", HttpMethod.Put, "/api/etcd/config",
            new { nodes = command.Nodes }, requestedBy: null, ct);
}
