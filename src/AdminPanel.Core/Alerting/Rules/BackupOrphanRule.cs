using AdminPanel.Core.Alerting;
using Shared.Core.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Core.Alerting.Rules;

// backup-orphan (warning, t07, arch/19 §4): запись реестра сирот
// /pgworker/backups/orphans — S3-префикс без владельца в etcd. Fate-приоритет
// (reliability t04, spec §3.5): DELETING «идёт удаление» (включая по заявке) →
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
            // Джойн защиты (reliability t04): hold-ключ/заявка по префиксу.
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
                fate = "идёт удаление";
            }
            else if (held is not null)
            {
                fate = $"защищён hold-флагом ({held.SetBy}) — удаление только явной командой";
                remedy = AlertRemedy.OperatorRunbook;
                protectedByHoldOrFull = true;
            }
            else if (entry.HasValidFull)
            {
                fate = "защищён автоправилом (есть валидный полный) — удаление только явной командой";
                remedy = AlertRemedy.OperatorRunbook;
                protectedByHoldOrFull = true;
            }
            else if (deleteRequested is not null)
            {
                fate = "к удалению заявкой оператора — ближайший проход воркера";
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
                $"сирота S3 {entry.Prefix} ({FormatBytes(entry.SizeBytes)}, {entry.Kind}, наблюдается с {entry.FirstSeenUnix}) — {fate}",
                details,
                null,
                "префикс <C>/<X> без владельца: кластер deprovisioned или шард удалён; защищённый префикс — потенциальный DR-источник (runbook backup-restore.md)",
                remedy,
                remedy == AlertRemedy.OperatorRunbook
                    ? protectedByHoldOrFull
                        ? $"удали заявкой delete (POST /api/backups/orphans/{entry.Prefix}/delete с confirm), если данные не нужны"
                        : "удали префикс из S3 вручную (mc rm --recursive --force s3/<bucket>/<prefix>) либо выставь PgWorker:Backups:Supervisor:OrphanTtlSec>0 для авто-удаления"
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
