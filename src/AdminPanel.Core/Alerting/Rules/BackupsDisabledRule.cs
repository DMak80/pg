using AdminPanel.Core.Alerting;
using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// backups-disabled (warning, u10): ключ /pgworker/backups-disabled жив —
// глобальный стоп бэкапов установки (приёмники всех кластеров замирают,
// arch/19 §4). Remedy OperatorRunbook: запуск — снять ключ (Backups:Enabled=true
// снимает его самим воркером) или включить подсистему; алерт гаснет первым
// тиком после исчезновения ключа ( панель — толерантный читатель: битый JSON —
// parseError-запись и алерта нет).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class BackupsDisabledRule : IAlertRule
{
    public const string KindName = "backups-disabled";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        if (snapshot.BackupsDisabled is not { } disabled)
            yield break; // ключа нет — бэкапы работают

        yield return new Alert(
            $"{KindName}:global",
            AlertSeverity.Warning,
            KindName,
            "global",
            "global backup stop: /pgworker/backups-disabled is alive — WAL receivers of all clusters are frozen"
            + $" (since unix {disabled.UpdatedUnix}"
            + (disabled.UpdatedBy is null ? "" : $", set by {disabled.UpdatedBy}") + ")",
            new Dictionary<string, string>
            {
                ["updatedUnix"] = disabled.UpdatedUnix.ToString(),
                ["updatedBy"] = disabled.UpdatedBy ?? string.Empty,
            },
            null,
            "глобальный стоп бэкапов: ключ /pgworker/backups-disabled — приёмники WAL всех кластеров замирают, "
            + "свежие полные не снимаются, RPO установки растёт; ключ ставит воркер при Backups:Enabled=false "
            + "(и снимает при true) либо операция сняла вручную; бэкапы считаются остановленными целиком",
            AlertRemedy.OperatorRunbook,
            "запуск — снять ключ /pgworker/backups-disabled или включить Backups:Enabled");
    }
}
