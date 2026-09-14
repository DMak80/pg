using AdminPanel.Core;
using AdminPanel.Core.Kafka;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Workers;
using Shared.Core.CQRS;
using Shared.Core.DI;

namespace AdminPanel.Api.Operations;

// WorkerNotFoundException — НЕ определяется здесь: общий тип живёт в
// AdminPanel.Etcd.Workers (WorkerCertService.cs) — его гвардят
// write-методы сервиса (generate/PUT/DELETE → 404 до KeyOf) и рестарт-хендлер.

// ===== Команды (CQRS по канону) =====

public sealed record GenerateWorkerApiCertCommand(string Worker, string RequestedBy)
    : ICommand<WorkerApiCertDto>;

public sealed record UploadWorkerApiCertCommand(string Worker, string CertPem, string KeyPem, string RequestedBy)
    : ICommand<WorkerApiCertDto>;

public sealed record DeleteWorkerApiCertCommand(string Worker) : ICommand<WorkerApiCertDeletedDto>;

public sealed record RestartWorkerCommand(string Worker, string RequestedBy) : ICommand<WorkerRestartDto>;

public sealed record WorkerApiCertDto(
    string Worker, string Thumbprint, long UpdatedUnix, string UpdatedBy,
    bool RestartRequired, string? Warning = null);

public sealed record WorkerApiCertDeletedDto(string Worker);

// ===== View-DTO (REST-контракт arch/adminpanel/03 §1/§3.7) =====

public sealed record WorkersViewDto(IReadOnlyList<WorkerViewDto> Workers);

public sealed record WorkerViewDto(string Worker, IReadOnlyList<WorkerInstanceDto> Instances, WorkerCertDto? TargetCert);

public sealed record WorkerInstanceDto(
    string Instance, string Url, long SinceUnix, string? Health, string? CertThumbprint,
    string ApplyStatus); // "applied" | "pending restart" | "unmanaged" | "unknown"

public sealed record WorkerCertDto(
    string Thumbprint, string Subject, string Issuer, IReadOnlyList<string> San,
    long NotBeforeUnix, long NotAfterUnix, long UpdatedUnix, string? UpdatedBy);

public sealed record WorkerRestartDto(IReadOnlyList<RestartInstanceResultDto> Results);

public sealed record RestartInstanceResultDto(string Instance, bool Accepted, string? Error);

// Генерация + txn-запись (spec §4.1): SAN — хосты живых advertise-URL.
[InjectAsScoped]
public sealed class GenerateWorkerApiCertCommandHandler(
    WorkerCertService certs, ISnapshotStore pg, IKafkaSnapshotReader kafka) : ICommandHandler<GenerateWorkerApiCertCommand, WorkerApiCertDto>
{
    public async ValueTask<Result<WorkerApiCertDto>> Handle(GenerateWorkerApiCertCommand c, CancellationToken ct)
    {
        var hosts = LiveHosts(c.Worker, pg.Current, kafka.Current);
        var write = await certs.GenerateAndPutAsync(c.Worker, hosts, c.RequestedBy, ct);
        return write.Map(w => ToDto(w, hosts.Count == 0
            ? "живых инстансов нет — SAN только localhost/127.0.0.1; после подъёма замените серт (PUT) для полного SAN"
            : null));
    }

    // Хосты живых advertise-URL этого воркера (spec §3.3 п.1). Неизвестный
    // воркер → пустой список (НЕ throw): 404 возвращает WorkerCertService —
    // исключение здесь пролетело бы мимо Result и роняло запрос в 500.
    internal static IReadOnlyList<string> LiveHosts(string worker, EtcdSnapshot? pg, KafkaSnapshot? kafka)
        => (worker switch
        {
            "pgworker" => pg?.PgWorkerEndpoints ?? [],
            "kafkaworker" => kafka?.WorkerEndpoints ?? [],
            _ => [], // 404 даст сервис (гвард до KeyOf)
        })
        .Select(e => WorkerCertService.HostOf(e.Url))
        .Where(h => h is not null)
        .Select(h => h!)
        .Distinct()
        .ToList();

    // DTO ответа 201 (spec §4.1 п.3): UpdatedUnix/UpdatedBy — из метаданных,
    // записанных сервисом (сервис пересобирает Meta с оператором до возврата);
    // restartRequired=true всегда: применение — только рестартом.
    internal static WorkerApiCertDto ToDto(WorkerCertWriteResult w, string? warning)
        => new(w.Worker, w.Meta.Thumbprint, w.Meta.UpdatedUnix, w.Meta.UpdatedBy ?? "adminpanel",
            RestartRequired: true, warning);
}

// PUT (spec §4.2): валидация §4.3 + 404 неизвестного воркера — внутри сервиса
// (гвард до KeyOf), хендлер дополнительной проверки не дублирует.
[InjectAsScoped]
public sealed class UploadWorkerApiCertCommandHandler(WorkerCertService certs)
    : ICommandHandler<UploadWorkerApiCertCommand, WorkerApiCertDto>
{
    public async ValueTask<Result<WorkerApiCertDto>> Handle(UploadWorkerApiCertCommand c, CancellationToken ct)
    {
        var write = await certs.PutAsync(c.Worker, c.CertPem, c.KeyPem, c.RequestedBy, ct);
        return write.Map(w => GenerateWorkerApiCertCommandHandler.ToDto(w, null));
    }
}

// DELETE (spec §4.2): 404 неизвестного воркера — там же, в сервисе.
[InjectAsScoped]
public sealed class DeleteWorkerApiCertCommandHandler(WorkerCertService certs)
    : ICommandHandler<DeleteWorkerApiCertCommand, WorkerApiCertDeletedDto>
{
    public async ValueTask<Result<WorkerApiCertDeletedDto>> Handle(DeleteWorkerApiCertCommand c, CancellationToken ct)
    {
        var delete = await certs.DeleteAsync(c.Worker, ct);
        return delete.Map(() => new WorkerApiCertDeletedDto(c.Worker));
    }
}

// Прокси рестарта на ВСЕ живые инстансы (spec §4.4). Сервис здесь не участвует —
// гвард worker явный (WorkerNotFoundException из AdminPanel.Etcd.Workers).
[InjectAsScoped]
public sealed class RestartWorkerCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<RestartWorkerCommand, WorkerRestartDto>
{
    public async ValueTask<Result<WorkerRestartDto>> Handle(RestartWorkerCommand c, CancellationToken ct)
    {
        if (c.Worker is not ("pgworker" or "kafkaworker"))
            return Result<WorkerRestartDto>.Failed(new WorkerNotFoundException(c.Worker));
        IReadOnlyList<WorkerApiInstanceResult> results;
        try
        {
            results = await api.SendAllAsync(c.Worker, HttpMethod.Post, "/api/restart", null, c.RequestedBy, ct);
        }
        catch (WorkerApiUnavailableException e)
        {
            return Result<WorkerRestartDto>.Failed(e);
        }

        return Result<WorkerRestartDto>.Success(new WorkerRestartDto(
            [.. results.Select(r => new RestartInstanceResultDto(
                r.Instance,
                r.Response is { StatusCode: >= 200 and < 300 },
                r.Error ?? (r.Response is { StatusCode: >= 200 and < 300 } ? null : $"HTTP {r.Response!.StatusCode}")))]));
    }
}
