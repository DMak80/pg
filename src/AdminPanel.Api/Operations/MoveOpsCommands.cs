using AdminPanel.Etcd.Workers;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Operations;

// ===== rollback (arch/03 §1.7; протокол 02 §9.7.2) =====

// Тело POST: buckets nullable — null/отсутствие поля ловит валидатор воркера.
public sealed record RollbackBucketsRequest(IReadOnlyList<int>? Buckets);

// Ответ 201: queued поставлены сейчас, skipped — идентичные op=rollback стояли.
public sealed record RollbackQueuedDto(
    string Cluster, IReadOnlyList<int> Queued, IReadOnlyList<int> Skipped);

public sealed record RollbackBucketsCommand(
    string Cluster, IReadOnlyList<int> Buckets, string RequestedBy)
    : ICommand<RollbackQueuedDto>;

// Прокси: панель не пишет в etcd — команда уходит в API PgWorker (arch/14 §1.1).
[InjectAsScoped]
public sealed class RollbackBucketsCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<RollbackBucketsCommand, RollbackQueuedDto>
{
    public async ValueTask<Result<RollbackQueuedDto>> Handle(RollbackBucketsCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<RollbackQueuedDto>(
            api, "pgworker", HttpMethod.Post, $"/api/clusters/{command.Cluster}/moves/rollback",
            new RollbackBucketsRequest(command.Buckets), command.RequestedBy, ct);
}

// ===== finalize (arch/03 §1.8; протокол 02 §9.7.3) =====

public sealed record FinalizeBucketRequest(int Bucket, string OldShard);

public sealed record BucketFinalizeQueuedDto(string Cluster, int Bucket, string OldShard);

public sealed record FinalizeBucketCommand(
    string Cluster, int Bucket, string OldShard, string RequestedBy)
    : ICommand<BucketFinalizeQueuedDto>;

[InjectAsScoped]
public sealed class FinalizeBucketCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<FinalizeBucketCommand, BucketFinalizeQueuedDto>
{
    public async ValueTask<Result<BucketFinalizeQueuedDto>> Handle(FinalizeBucketCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<BucketFinalizeQueuedDto>(
            api, "pgworker", HttpMethod.Post, $"/api/clusters/{command.Cluster}/moves/finalize",
            new FinalizeBucketRequest(command.Bucket, command.OldShard), command.RequestedBy, ct);
}

// ===== abort (arch/03 §1.9; протокол 02 §9.7.4) =====

// force nullable: false — не шлём (воркер трактует отсутствие как false).
public sealed record AbortBucketRequest(int Bucket, bool? Force);

public sealed record BucketAbortQueuedDto(string Cluster, int Bucket, bool Force);

public sealed record AbortBucketCommand(
    string Cluster, int Bucket, bool Force, string RequestedBy)
    : ICommand<BucketAbortQueuedDto>;

[InjectAsScoped]
public sealed class AbortBucketCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<AbortBucketCommand, BucketAbortQueuedDto>
{
    public async ValueTask<Result<BucketAbortQueuedDto>> Handle(AbortBucketCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<BucketAbortQueuedDto>(
            api, "pgworker", HttpMethod.Post, $"/api/clusters/{command.Cluster}/moves/abort",
            new AbortBucketRequest(command.Bucket, command.Force ? true : null), command.RequestedBy, ct);
}

// ===== отмена заявки (arch/03 §1.9; протокол 02 §9.7.5) =====

// Воркер отвечает 204 без тела — DTO не читается (образец DeleteClusterCommand).
public sealed record MoveTicketCancelledDto(string Cluster, string Bucket);

public sealed record CancelMoveTicketCommand(
    string Cluster, string Bucket, string RequestedBy)
    : ICommand<MoveTicketCancelledDto>;

[InjectAsScoped]
public sealed class CancelMoveTicketCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<CancelMoveTicketCommand, MoveTicketCancelledDto>
{
    public async ValueTask<Result<MoveTicketCancelledDto>> Handle(CancelMoveTicketCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<MoveTicketCancelledDto>(
            api, "pgworker", HttpMethod.Delete, $"/api/clusters/{command.Cluster}/moves/{command.Bucket}",
            body: null, command.RequestedBy, ct);
}
