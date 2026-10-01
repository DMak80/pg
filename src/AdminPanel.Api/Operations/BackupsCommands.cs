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
    long FullMaxAgeSec, bool VerifyOnCreate, int DrillIntervalDays);

public sealed record UpdateBackupsPolicyCommand(
    string Cluster, int RetentionDays, int RetentionWeeks, int RetentionMonths,
    long FullMaxAgeSec, bool VerifyOnCreate, int DrillIntervalDays, string RequestedBy)
    : ICommand<string>;

[InjectAsScoped]
public sealed class UpdateBackupsPolicyCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<UpdateBackupsPolicyCommand, string>
{
    public async ValueTask<Result<string>> Handle(UpdateBackupsPolicyCommand c, CancellationToken ct)
        => await WorkerProxy.SendAsync<string>(api, "pgworker", HttpMethod.Post,
            $"/api/clusters/{c.Cluster}/backups/policy",
            body: new
            {
                retention = new { days = c.RetentionDays, weeks = c.RetentionWeeks, months = c.RetentionMonths },
                full_max_age_sec = c.FullMaxAgeSec,
                verify = new { on_create = c.VerifyOnCreate },
                drill = new { interval_days = c.DrillIntervalDays },
            },
            requestedBy: c.RequestedBy, ct);
}
