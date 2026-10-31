using AdminPanel.Etcd.Workers;
using Shared.Core.CQRS;
using Shared.Core.DI;

namespace AdminPanel.Api.Operations.Valkey;

// Отмена живых заявок ротаций valkey (u17, arch/02 §11.2-7/8): панель —
// прокси DELETE в API ValkeyWorker (del ключа выполняет ТОЛЬКО воркер, D13).
// DELETE не создаёт заявку → requestedBy=null — закреплено в spec §7 /
// arch 02 §10.2/§11.2: del без тела, аудит — host-лог воркера (образец
// CancelKafkaRebalanceCommandHandler).
public sealed record CancelValkeyRotationCommand(string Cluster, string Path)
    : ICommand<ValkeyRotationCancelledDto>;

// 204 воркера без тела → dto default (паттерн CancelKafkaRebalanceCommand).
public sealed record ValkeyRotationCancelledDto(string Cluster);

[InjectAsScoped]
public sealed class CancelValkeyRotationCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<CancelValkeyRotationCommand, ValkeyRotationCancelledDto>
{
    public ValueTask<Result<ValkeyRotationCancelledDto>> Handle(
        CancelValkeyRotationCommand command, CancellationToken ct)
        => WorkerProxy.SendAsync<ValkeyRotationCancelledDto>(
            api, "valkeyworker", HttpMethod.Delete,
            $"/api/valkey/clusters/{command.Cluster}/{command.Path}",
            body: null, requestedBy: null, ct);
}
