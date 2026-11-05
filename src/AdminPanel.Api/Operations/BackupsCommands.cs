using AdminPanel.Etcd.Workers;
using Shared.Core.CQRS;
using Shared.Core.DI;

namespace AdminPanel.Api.Operations;

// Мутация policy бэкапов (reliability, arch/02 §9.12): панель НЕ пишет
// в etcd — команда-прокси в POST policy-API воркера (arch/pgworker/07-api.md §1) ПОЛНЫМ
// телом (retention + full_max_age_sec + verify + drill; канон замещения
// целиком — панель в своей форме всегда шлёт полный набор полей, включая
// текущее значение drill).
// Тело формы панели (02 §9.12): полный набор полей policy; валидация диапазонов —
// дубль панели для UX (истина — сервер воркера).
public sealed record UpdateBackupsPolicyRequest(
    int RetentionDays, int RetentionWeeks, int RetentionMonths,
    long FullMaxAgeSec, bool VerifyOnCreate, int DrillIntervalDays,
    S3PolicyBody? S3,      // null — сброс секции (policy без s3)
    long? GarbageMinAgeSec = null); // u16: null — поле в policy не шлётся (дефолт воркера 600 с)

/// <summary>Секция s3 формы «Пул бэкапов» (arch/02 §9.12): null-поля —
/// валидация на воркере; весь body null — секция не шлётся (выключение).</summary>
public sealed record S3PolicyBody(
    string? Pool, IReadOnlyList<string>? WriteMaster, IReadOnlyList<string>? WriteReplica);

public sealed record UpdateBackupsPolicyCommand(
    string Cluster, int RetentionDays, int RetentionWeeks, int RetentionMonths,
    long FullMaxAgeSec, bool VerifyOnCreate, int DrillIntervalDays,
    S3PolicyBody? S3, string RequestedBy, long? GarbageMinAgeSec = null)
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

    // s3 сериализуется только при non-null секции (сброс — отправкой
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
        // u16 (arch/02 §9.12): поле задано → canonical policy-ключ; null —
        // поле НЕ шлётся (замещение целиком снимает пер-кластерный гейт).
        if (c.GarbageMinAgeSec is { } garbage)
            body["garbage_min_age_sec"] = garbage;
        return body;
    }
}
