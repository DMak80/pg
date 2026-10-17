using AdminPanel.Etcd.Workers;
using Shared.Core.CQRS;
using Shared.Core.DI;

namespace AdminPanel.Api.Operations;

// Мутация policy бэкапов (reliability t02, arch/02 §9.12): панель НЕ пишет
// в etcd — команда-прокси в POST policy-API воркера (arch/14 §1.1) ПОЛНЫМ
// телом (retention + full_max_age_sec + verify + drill; канон замещения
// целиком — панель в своей форме всегда шлёт полный набор полей, включая
// текущее значение drill).
// Тело формы панели (02 §9.12): полный набор полей policy; валидация диапазонов —
// дубль панели для UX (истина — сервер воркера).
public sealed record UpdateBackupsPolicyRequest(
    int RetentionDays, int RetentionWeeks, int RetentionMonths,
    long FullMaxAgeSec, bool VerifyOnCreate, int DrillIntervalDays,
    S3PolicyBody? S3); // t36: null — сброс секции (policy без s3)

/// <summary>Секция s3 формы «Пул бэкапов» (t36, arch/02 §9.12): null-поля —
/// валидация на воркере; весь body null — секция не шлётся (выключение).</summary>
public sealed record S3PolicyBody(
    string? Pool, IReadOnlyList<string>? WriteMaster, IReadOnlyList<string>? WriteReplica);

public sealed record UpdateBackupsPolicyCommand(
    string Cluster, int RetentionDays, int RetentionWeeks, int RetentionMonths,
    long FullMaxAgeSec, bool VerifyOnCreate, int DrillIntervalDays,
    S3PolicyBody? S3, string RequestedBy)
    : ICommand<string>;

[InjectAsScoped]
public sealed class UpdateBackupsPolicyCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<UpdateBackupsPolicyCommand, string>
{
    public async ValueTask<Result<string>> Handle(UpdateBackupsPolicyCommand c, CancellationToken ct)
        => await WorkerProxy.SendAsync<string>(api, "pgworker", HttpMethod.Post,
            $"/api/clusters/{c.Cluster}/backups/policy",
            body: BuildBody(c),
            requestedBy: c.RequestedBy, ct);

    // t36: s3 сериализуется только при non-null секции (сброс — отправкой
    // policy БЕЗ s3, arch/02 §9.12).
    internal static object BuildBody(UpdateBackupsPolicyCommand c)
    {
        var body = new Dictionary<string, object>
        {
            ["retention"] = new { days = c.RetentionDays, weeks = c.RetentionWeeks, months = c.RetentionMonths },
            ["full_max_age_sec"] = c.FullMaxAgeSec,
            ["verify"] = new { on_create = c.VerifyOnCreate },
            ["drill"] = new { interval_days = c.DrillIntervalDays },
        };
        if (c.S3 is { } s3)
            body["s3"] = new { pool = s3.Pool, write_master = s3.WriteMaster, write_replica = s3.WriteReplica };
        return body;
    }
}
