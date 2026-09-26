using AdminPanel.Etcd.Workers;
using Shared.Core.CQRS;
using Shared.Core.DI;

namespace AdminPanel.Api.Operations.Valkey;

// ===== Valkey-мутации — прокси в API ValkeyWorker (arch/02 §11.2, arch/21 §1.1):
// панель не пишет в etcd; воркер валидирует и пишет. Оператор (requestedBy) —
// только rotate (заявка с аудитом) — заголовком X-Requested-By. =====

// 1. Создание кластера (02 §11.2-1).
public sealed record CreateValkeyClusterCommand(CreateValkeyClusterRequest Request)
    : ICommand<ValkeyClusterCreatedDto>;

// Ответ 201 POST /api/valkey/clusters (arch/03 §8.2; поля — DTO воркера t02).
public sealed record ValkeyClusterCreatedDto(
    string Name, string State, int Nodes, long MaxmemoryBytes,
    string MaxmemoryPolicy, string Cpu, string MemGi, string DiskGi);

[InjectAsScoped]
public sealed class CreateValkeyClusterCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<CreateValkeyClusterCommand, ValkeyClusterCreatedDto>
{
    public async ValueTask<Result<ValkeyClusterCreatedDto>> Handle(
        CreateValkeyClusterCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<ValkeyClusterCreatedDto>(
            api, "valkeyworker", HttpMethod.Post, "/api/valkey/clusters",
            command.Request, requestedBy: null, ct);
}

// 2. Удаление — TO_REMOVE, демонтаж асинхронный (02 §11.2-2): панель отвечает 202.
public sealed record DeleteValkeyClusterCommand(string Cluster) : ICommand<ValkeyClusterDeletedDto>;

public sealed record ValkeyClusterDeletedDto(string Cluster);

[InjectAsScoped]
public sealed class DeleteValkeyClusterCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<DeleteValkeyClusterCommand, ValkeyClusterDeletedDto>
{
    public async ValueTask<Result<ValkeyClusterDeletedDto>> Handle(
        DeleteValkeyClusterCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<ValkeyClusterDeletedDto>(
            api, "valkeyworker", HttpMethod.Delete, $"/api/valkey/clusters/{command.Cluster}",
            body: null, requestedBy: null, ct);
}

// 3. Конфиг-мутация maxmemory_* (02 §11.2-3; converge D применит).
public sealed record UpdateValkeyConfigCommand(string Cluster, ValkeyConfigUpdateRequest Request)
    : ICommand<ValkeyConfigUpdatedDto>;

public sealed record ValkeyConfigUpdatedDto(string Cluster, long MaxmemoryBytes, string MaxmemoryPolicy);

[InjectAsScoped]
public sealed class UpdateValkeyConfigCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<UpdateValkeyConfigCommand, ValkeyConfigUpdatedDto>
{
    public async ValueTask<Result<ValkeyConfigUpdatedDto>> Handle(
        UpdateValkeyConfigCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<ValkeyConfigUpdatedDto>(
            api, "valkeyworker", HttpMethod.Put, $"/api/valkey/clusters/{command.Cluster}/config",
            command.Request, requestedBy: null, ct);
}

// 4. Ресурсы ноды (02 §11.2-4; автоконверге надзора — пересоздание контейнера).
public sealed record UpdateValkeyResourcesCommand(
    string Cluster, string Node, ValkeyResourcesUpdateRequest Request)
    : ICommand<ValkeyResourcesUpdatedDto>;

public sealed record ValkeyResourcesUpdatedDto(string Cluster, string Node, string Cpu, string MemGi, string DiskGi);

[InjectAsScoped]
public sealed class UpdateValkeyResourcesCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<UpdateValkeyResourcesCommand, ValkeyResourcesUpdatedDto>
{
    public async ValueTask<Result<ValkeyResourcesUpdatedDto>> Handle(
        UpdateValkeyResourcesCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<ValkeyResourcesUpdatedDto>(
            api, "valkeyworker", HttpMethod.Put,
            $"/api/valkey/clusters/{command.Cluster}/nodes/{command.Node}/resources",
            command.Request, requestedBy: null, ct);
}

// 5. Заявка ротации пароля app|admin (02 §11.2-5; окно двух паролей, без рестартов).
public sealed record RotateValkeyPasswordCommand(string Cluster, string Role, string RequestedBy)
    : ICommand<ValkeyPasswordRotatedDto>;

public sealed record ValkeyPasswordRotatedDto(
    string Cluster, string Role, long RequestedUnix, string RequestedBy);

[InjectAsScoped]
public sealed class RotateValkeyPasswordCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<RotateValkeyPasswordCommand, ValkeyPasswordRotatedDto>
{
    public async ValueTask<Result<ValkeyPasswordRotatedDto>> Handle(
        RotateValkeyPasswordCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<ValkeyPasswordRotatedDto>(
            api, "valkeyworker", HttpMethod.Post,
            $"/api/valkey/clusters/{command.Cluster}/password/rotate",
            new RotateValkeyPasswordRequest(command.Role), command.RequestedBy, ct);
}

// 6. Заявка ротации per-cluster CA/сертов (02 §11.2-6, t07; окно двойного
// доверия P/D/R/C исполняет CaRotator воркера; нода пересоздаётся).
public sealed record RotateValkeyCaCommand(string Cluster, string RequestedBy)
    : ICommand<ValkeyCaRotatedDto>;

public sealed record ValkeyCaRotatedDto(string Cluster, long RequestedUnix, string RequestedBy);

[InjectAsScoped]
public sealed class RotateValkeyCaCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<RotateValkeyCaCommand, ValkeyCaRotatedDto>
{
    public async ValueTask<Result<ValkeyCaRotatedDto>> Handle(
        RotateValkeyCaCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<ValkeyCaRotatedDto>(
            api, "valkeyworker", HttpMethod.Post,
            $"/api/valkey/clusters/{command.Cluster}/ca/rotate",
            body: null, command.RequestedBy, ct);
}
