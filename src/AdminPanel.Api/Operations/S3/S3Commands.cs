using AdminPanel.Etcd.Workers;
using Shared.Core.CQRS;
using Shared.Core.DI;

namespace AdminPanel.Api.Operations.S3;

// ===== S3-мутации — прокси в API S3Worker (arch/02 §12.2, arch/24 §1.1):
// панель не пишет в etcd; воркер валидирует и пишет. Оператор (requestedBy) —
// ротация и зонд (заявки с аудитом) — заголовком X-Requested-By. =====

// 1. Создание кластера (§12.2-1) — 201.
public sealed record CreateS3ClusterCommand(CreateS3ClusterPanelRequest Request)
    : ICommand<S3ClusterCreatedPanelDto>;

// Ответ 201 POST /api/s3/clusters (зеркало S3ClusterCreatedDto воркера).
public sealed record S3ClusterCreatedPanelDto(
    string Name, string State, int Nodes, string SecretProfile, IReadOnlyList<string> Instances);

[InjectAsScoped]
public sealed class CreateS3ClusterCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<CreateS3ClusterCommand, S3ClusterCreatedPanelDto>
{
    public async ValueTask<Result<S3ClusterCreatedPanelDto>> Handle(
        CreateS3ClusterCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<S3ClusterCreatedPanelDto>(
            api, "s3worker", HttpMethod.Post, "/api/s3/clusters",
            command.Request, requestedBy: null, ct);
}

// 2. Удаление — TO_REMOVE, демонтаж асинхронный (§12.2-2): 202 | 204 (already) —
// код зеркалится: SendWithStatusAsync, модуль маппит по StatusCode.
public sealed record DeleteS3ClusterCommand(string Cluster)
    : ICommand<WorkerRaw<S3ClusterDeletedPanelDto>>;

public sealed record S3ClusterDeletedPanelDto(string Cluster);

[InjectAsScoped]
public sealed class DeleteS3ClusterCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<DeleteS3ClusterCommand, WorkerRaw<S3ClusterDeletedPanelDto>>
{
    public async ValueTask<Result<WorkerRaw<S3ClusterDeletedPanelDto>>> Handle(
        DeleteS3ClusterCommand command, CancellationToken ct)
        => await WorkerProxy.SendWithStatusAsync<S3ClusterDeletedPanelDto>(
            api, "s3worker", HttpMethod.Delete, $"/api/s3/clusters/{command.Cluster}",
            body: null, requestedBy: null, ct);
}

// 3. Добавление инстанса (§12.2-3) — 201 {cluster, node}.
public sealed record AddS3NodeCommand(string Cluster, AddS3NodePanelRequest Request)
    : ICommand<S3NodeAddedPanelDto>;

public sealed record S3NodeAddedPanelDto(string Cluster, string Node);

[InjectAsScoped]
public sealed class AddS3NodeCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<AddS3NodeCommand, S3NodeAddedPanelDto>
{
    public async ValueTask<Result<S3NodeAddedPanelDto>> Handle(
        AddS3NodeCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<S3NodeAddedPanelDto>(
            api, "s3worker", HttpMethod.Post, $"/api/s3/clusters/{command.Cluster}/nodes",
            command.Request, requestedBy: null, ct);
}

// 4. Маркер демонтажа инстанса (§12.2-4, one-way): 202 | 204 (already).
public sealed record RemoveS3NodeCommand(string Cluster, string Node)
    : ICommand<WorkerRaw<S3NodeRemovedPanelDto>>;

public sealed record S3NodeRemovedPanelDto(string Cluster, string Node);

[InjectAsScoped]
public sealed class RemoveS3NodeCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<RemoveS3NodeCommand, WorkerRaw<S3NodeRemovedPanelDto>>
{
    public async ValueTask<Result<WorkerRaw<S3NodeRemovedPanelDto>>> Handle(
        RemoveS3NodeCommand command, CancellationToken ct)
        => await WorkerProxy.SendWithStatusAsync<S3NodeRemovedPanelDto>(
            api, "s3worker", HttpMethod.Delete,
            $"/api/s3/clusters/{command.Cluster}/nodes/{command.Node}",
            body: null, requestedBy: null, ct);
}

// 5. Ресурсы инстанса (§12.2-5) — 200 {cpu, mem, disk} (строки-каноны воркера).
public sealed record UpdateS3ResourcesCommand(
    string Cluster, string Node, UpdateS3ResourcesPanelRequest Request)
    : ICommand<S3ResourcesUpdatedPanelDto>;

public sealed record S3ResourcesUpdatedPanelDto(string Cpu, string Mem, string Disk);

[InjectAsScoped]
public sealed class UpdateS3ResourcesCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<UpdateS3ResourcesCommand, S3ResourcesUpdatedPanelDto>
{
    public async ValueTask<Result<S3ResourcesUpdatedPanelDto>> Handle(
        UpdateS3ResourcesCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<S3ResourcesUpdatedPanelDto>(
            api, "s3worker", HttpMethod.Put,
            $"/api/s3/clusters/{command.Cluster}/nodes/{command.Node}/resources",
            command.Request, requestedBy: null, ct);
}

// 6. Граф репликации — замена целиком (§12.2-6) — 202 (применит доводка G);
// код зеркалится через SendWithStatusAsync.
public sealed record PutS3LinksCommand(string Cluster, PutS3LinksPanelRequest Request)
    : ICommand<WorkerRaw<S3LinksPutPanelDto>>;

public sealed record S3LinksPutPanelDto(string Cluster);

[InjectAsScoped]
public sealed class PutS3LinksCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<PutS3LinksCommand, WorkerRaw<S3LinksPutPanelDto>>
{
    public async ValueTask<Result<WorkerRaw<S3LinksPutPanelDto>>> Handle(
        PutS3LinksCommand command, CancellationToken ct)
        => await WorkerProxy.SendWithStatusAsync<S3LinksPutPanelDto>(
            api, "s3worker", HttpMethod.Put, $"/api/s3/clusters/{command.Cluster}/links",
            command.Request, requestedBy: null, ct);
}

// 7. Отзыв ключа инстанса deny (§12.2-7): 202 | 204 (already).
public sealed record DenyS3NodeCommand(string Cluster, string Node)
    : ICommand<WorkerRaw<S3NodeDenyPanelDto>>;

public sealed record S3NodeDenyPanelDto(string Cluster, string Node);

[InjectAsScoped]
public sealed class DenyS3NodeCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<DenyS3NodeCommand, WorkerRaw<S3NodeDenyPanelDto>>
{
    public async ValueTask<Result<WorkerRaw<S3NodeDenyPanelDto>>> Handle(
        DenyS3NodeCommand command, CancellationToken ct)
        => await WorkerProxy.SendWithStatusAsync<S3NodeDenyPanelDto>(
            api, "s3worker", HttpMethod.Post,
            $"/api/s3/clusters/{command.Cluster}/nodes/{command.Node}/deny",
            body: null, requestedBy: null, ct);
}

// 8. Снятие отзыва allow (§12.2-8): 202 | 204 (already).
public sealed record AllowS3NodeCommand(string Cluster, string Node)
    : ICommand<WorkerRaw<S3NodeAllowPanelDto>>;

public sealed record S3NodeAllowPanelDto(string Cluster, string Node);

[InjectAsScoped]
public sealed class AllowS3NodeCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<AllowS3NodeCommand, WorkerRaw<S3NodeAllowPanelDto>>
{
    public async ValueTask<Result<WorkerRaw<S3NodeAllowPanelDto>>> Handle(
        AllowS3NodeCommand command, CancellationToken ct)
        => await WorkerProxy.SendWithStatusAsync<S3NodeAllowPanelDto>(
            api, "s3worker", HttpMethod.Post,
            $"/api/s3/clusters/{command.Cluster}/nodes/{command.Node}/allow",
            body: null, requestedBy: null, ct);
}

// 9. Заявка ротации секретов профиля (§12.2-9): 201 (создана) | 200 (уже стоит —
// идемпотентность воркера, 409 НЕ отдаётся); аудит requestedBy.
public sealed record RotateS3SecretsCommand(string Cluster, string RequestedBy)
    : ICommand<WorkerRaw<S3RotationRequestedPanelDto>>;

public sealed record S3RotationRequestedPanelDto(string Cluster);

[InjectAsScoped]
public sealed class RotateS3SecretsCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<RotateS3SecretsCommand, WorkerRaw<S3RotationRequestedPanelDto>>
{
    public async ValueTask<Result<WorkerRaw<S3RotationRequestedPanelDto>>> Handle(
        RotateS3SecretsCommand command, CancellationToken ct)
        => await WorkerProxy.SendWithStatusAsync<S3RotationRequestedPanelDto>(
            api, "s3worker", HttpMethod.Post, $"/api/s3/clusters/{command.Cluster}/rotations",
            body: null, command.RequestedBy, ct);
}

// 10. Внеплановый зонд (§12.2-10): 202; аудит.
public sealed record S3SyncProbeCommand(string Cluster, string RequestedBy)
    : ICommand<WorkerRaw<S3ProbeAcceptedPanelDto>>;

public sealed record S3ProbeAcceptedPanelDto(string Cluster);

[InjectAsScoped]
public sealed class S3SyncProbeCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<S3SyncProbeCommand, WorkerRaw<S3ProbeAcceptedPanelDto>>
{
    public async ValueTask<Result<WorkerRaw<S3ProbeAcceptedPanelDto>>> Handle(
        S3SyncProbeCommand command, CancellationToken ct)
        => await WorkerProxy.SendWithStatusAsync<S3ProbeAcceptedPanelDto>(
            api, "s3worker", HttpMethod.Post, $"/api/s3/clusters/{command.Cluster}/sync-probe",
            body: null, command.RequestedBy, ct);
}
