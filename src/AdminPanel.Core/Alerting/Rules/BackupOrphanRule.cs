using AdminPanel.Core.Alerting;
using Shared.Core.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Core.Alerting.Rules;

// backup-orphan (warning, arch/19 §4): запись реестра сирот
// /pgworker/backups/orphans — S3-префикс без владельца в etcd. Fate-приоритет
// (reliability, spec §3.5): DELETING «идёт удаление» (включая по заявке) →
// hold «защищён hold-флагом (<by>) — удаление только явной командой» →
// автозащита «защищён автоправилом (есть валидный полный) — удаление только
// явной командой» → заявка «к удалению заявкой оператора — ближайший проход» →
// прежний остаток TTL / OrphanTtlSec=0. Severity — warning для всех (сирота =
// разбор оператора, срочности нет: защита держит). Alerts:Backups:OrphanTtlSec
// = 0 (воркерное авто-удаление выключено) — «удаление вручную», OperatorRunbook.
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
            // Джойн защиты (reliability): hold-ключ/заявка по префиксу.
            OrphanHoldInfo? held = null;
            if (orphans.Holds is not null && orphans.Holds.TryGetValue(entry.Prefix, out var hold))
                held = hold;
            OrphanDeleteRequestInfo? deleteRequested = null;
            if (orphans.DeleteRequests is not null
                && orphans.DeleteRequests.TryGetValue(entry.Prefix, out var requested))
                deleteRequested = requested;

            // Показ и сбоя, и действия воркера (канон arch/19 §4).
            string fate;
            var remedy = AlertRemedy.WorkerAuto;
            var protectedByHoldOrFull = false;
            if (entry.State == "DELETING")
            {
                fate = "deletion in progress";
            }
            else if (held is not null)
            {
                fate = $"protected by hold flag ({held.SetBy}) — deletion only via explicit command";
                remedy = AlertRemedy.OperatorRunbook;
                protectedByHoldOrFull = true;
            }
            else if (entry.HasValidFull)
            {
                fate = "protected by auto-rule (valid full backup exists) — deletion only via explicit command";
                remedy = AlertRemedy.OperatorRunbook;
                protectedByHoldOrFull = true;
            }
            else if (deleteRequested is not null)
            {
                fate = "queued for deletion by operator request — next worker pass";
            }
            else if (ttl <= 0 || options.Value.Backups.OrphanTtlSec <= 0)
            {
                fate = "auto-deletion disabled (OrphanTtlSec=0) — manual deletion";
                remedy = AlertRemedy.OperatorRunbook;
            }
            else
            {
                var left = entry.FirstSeenUnix + ttl - nowUnix;
                fate = left > 0
                    ? $"TTL deletion in {Days(left)}"
                    : "TTL deletion — next worker pass";
            }

            var details = new Dictionary<string, string>
            {
                ["prefix"] = entry.Prefix,
                ["kind"] = entry.Kind,
                ["sizeBytes"] = entry.SizeBytes.ToString(),
                ["firstSeenUnix"] = entry.FirstSeenUnix.ToString(),
                ["state"] = entry.State,
                ["hasValidFull"] = entry.HasValidFull ? "true" : "false",
            };
            if (held is not null)
                details["heldBy"] = held.SetBy;
            if (deleteRequested is not null)
                details["deleteRequested"] = "true";

            yield return new Alert(
                $"{KindName}:{entry.Prefix}",
                AlertSeverity.Warning,
                KindName,
                entry.Prefix,
                $"S3 orphan {entry.Prefix} ({FormatBytes(entry.SizeBytes)}, {entry.Kind}, observed since {entry.FirstSeenUnix}) — {fate}",
                details,
                null,
                "ownerless prefix <C>/<X>: cluster deprovisioned or shard removed; protected prefix is a potential DR source (runbook backup-restore.md)",
                remedy,
                remedy == AlertRemedy.OperatorRunbook
                    ? protectedByHoldOrFull
                        ? $"delete via operator request (POST /api/backups/orphans/{entry.Prefix}/delete with confirm) if the data is not needed"
                        : "remove the prefix from S3 manually (mc rm --recursive --force s3/<bucket>/<prefix>) or set PgWorker:Backups:Supervisor:OrphanTtlSec>0 for auto-deletion"
                    : "no action required: the worker deletes the prefix by TTL (Supervisor:OrphanTtlSec) and maintains the /pgworker/backups/orphans registry");
        }
    }

    // Остаток TTL человекочитаемо: сутки/часы (остаток всегда > 0 на вызове).
    private static string Days(long seconds)
        => seconds >= 86400
            ? $"{seconds / 86400} d"
            : $"{seconds / 3600} h";

    private static string FormatBytes(long bytes)
        => bytes >= 1024L * 1024 * 1024
            ? $"{bytes / (1024L * 1024 * 1024)} GiB"
            : bytes >= 1024 * 1024
                ? $"{bytes / (1024 * 1024)} MiB"
                : $"{bytes} B";
}
