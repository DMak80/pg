using AdminPanel.Core.Alerting;
using Shared.Core.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Core.Alerting.Rules;

// backup-orphan (warning, t07, arch/19 §4): запись реестра сирот
// /pgworker/backups/orphans — S3-префикс без владельца в etcd. OBSERVED —
// показываем остаток TTL до авто-удаления; DELETING — «идёт удаление»;
// Alerts:Backups:OrphanTtlSec = 0 (воркерное авто-удаление выключено) —
// «удаление вручную», Remedy OperatorRunbook.
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class BackupOrphanRule(IOptions<AlertsOptions> options) : IAlertRule
{
    public const string KindName = "backup-orphan";

    // Каталожный дефолт — 7 суток (arch/19 §9); фолбэк при опечатке конфига.
    public const long DefaultOrphanTtlSec = 604800;

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        var orphans = snapshot.BackupOrphans;
        if (orphans is null)
            yield break; // ключа нет — подсистема сирот не включалась/реестр пуст

        var nowUnix = context.NowUtc.ToUnixTimeSeconds();
        var ttl = options.Value.Backups.OrphanTtlSec > 0
            ? options.Value.Backups.OrphanTtlSec
            : DefaultOrphanTtlSec;
        foreach (var entry in orphans.Orphans)
        {
            // Показ и сбоя, и действия воркера (канон arch/19 §4).
            string fate;
            var remedy = AlertRemedy.WorkerAuto;
            if (entry.State == "DELETING")
            {
                fate = "идёт удаление";
            }
            else if (ttl <= 0 || options.Value.Backups.OrphanTtlSec <= 0)
            {
                fate = "авто-удаление выключено (OrphanTtlSec=0) — удаление вручную";
                remedy = AlertRemedy.OperatorRunbook;
            }
            else
            {
                var left = entry.FirstSeenUnix + ttl - nowUnix;
                fate = left > 0
                    ? $"удаление по TTL через {Days(left)}"
                    : "удаление по TTL — следующий проход воркера";
            }

            yield return new Alert(
                $"{KindName}:{entry.Prefix}",
                AlertSeverity.Warning,
                KindName,
                entry.Prefix,
                $"сирота S3 {entry.Prefix} ({FormatBytes(entry.SizeBytes)}, {entry.Kind}, наблюдается с {entry.FirstSeenUnix}) — {fate}",
                new Dictionary<string, string>
                {
                    ["prefix"] = entry.Prefix,
                    ["kind"] = entry.Kind,
                    ["sizeBytes"] = entry.SizeBytes.ToString(),
                    ["firstSeenUnix"] = entry.FirstSeenUnix.ToString(),
                    ["state"] = entry.State,
                },
                null,
                "префикс <C>/<X> без владельца: кластер deprovisioned или шард удалён; DR-восстановление из S3 возможно до истечения TTL (source-override, runbook backup-restore.md)",
                remedy,
                remedy == AlertRemedy.OperatorRunbook
                    ? "удали префикс из S3 вручную (mc rm --recursive --force s3/<bucket>/<prefix>) либо выставь PgWorker:Backups:Supervisor:OrphanTtlSec>0 для авто-удаления"
                    : "действий не требуется: воркер удаляет префикс по TTL (Supervisor:OrphanTtlSec) и ведёт реестр /pgworker/backups/orphans");
        }
    }

    // Остаток TTL человекочитаемо: сутки/часы (остаток всегда > 0 на вызове).
    private static string Days(long seconds)
        => seconds >= 86400
            ? $"{seconds / 86400} сут"
            : $"{seconds / 3600} ч";

    private static string FormatBytes(long bytes)
        => bytes >= 1024L * 1024 * 1024
            ? $"{bytes / (1024L * 1024 * 1024)} ГиБ"
            : bytes >= 1024 * 1024
                ? $"{bytes / (1024 * 1024)} МиБ"
                : $"{bytes} Б";
}
