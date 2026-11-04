using AdminPanel.Core.Alerting;
using Shared.Core.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Core.Alerting.Rules;

// backup-wal-life-changed (t15, arch/adminpanel/03 §4, arch/backups/05 §1):
// смена номера жизни цепочки WAL шарда живого Active-кластера — событие, а не
// перманентное состояние: зажигается свежестью bumped_unix (< WalLifeChangedSec)
// и гаснет возрастом. Severity по причине инкремента: foreign-chain — warning
// (в префиксе чужая цепочка, приёмник заморожен — разбор оператором);
// restore/shard-rebuilt/нет причины — info (штатный разрыв цепочки).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class BackupWalLifeChangedRule(IOptions<AlertsOptions> options) : IAlertRule
{
    public const string KindName = "backup-wal-life-changed";

    // Каталожный дефолт — 1 ч; фолбэк при опечатке конфига.
    public const int DefaultWalLifeChangedSec = 3600;

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        var nowUnix = context.NowUtc.ToUnixTimeSeconds();
        var windowSec = options.Value.Backups.WalLifeChangedSec > 0
            ? options.Value.Backups.WalLifeChangedSec
            : DefaultWalLifeChangedSec;

        foreach (var backups in snapshot.Backups)
        foreach (var (shard, life) in backups.WalLife ?? new Dictionary<string, WalLifeInfo>())
        {
            // Событие: инкремент был и он свежий (гаснет возрастом, не висит вечно).
            if (life.BumpedUnix is not { } bumped || nowUnix - bumped >= windowSec)
                continue;

            // только живой Active-кластер (образец BackupWalTliGapRule)
            var cluster = snapshot.Clusters.FirstOrDefault(
                c => c.Name == backups.Cluster && c.State == ClusterState.Active
                     && c.Shards.Any(s => s.Name == shard));
            if (cluster is null)
                continue;

            var foreign = string.Equals(life.Reason, "foreign-chain", StringComparison.Ordinal);
            yield return new Alert(
                $"{KindName}:{backups.Cluster}/{shard}",
                foreign ? AlertSeverity.Warning : AlertSeverity.Info,
                KindName,
                $"{backups.Cluster}/{shard}",
                $"WAL chain life changed for shard {shard} of cluster {backups.Cluster}: " +
                $"life {life.ChainLife} (reason: {life.Reason ?? "unknown"}) — " +
                (foreign
                    ? "foreign WAL chain detected, receiver frozen, operator review required"
                    : "regular restore/rebuild"),
                new Dictionary<string, string>
                {
                    ["chain_life"] = life.ChainLife.ToString(),
                    ["bumped_unix"] = bumped.ToString(),
                    ["reason"] = life.Reason ?? "",
                },
                null, // SinceUnix — проставляет AlertEngine
                foreign
                    ? "the S3 prefix of the shard holds a WAL chain from another life: the receiver is frozen by the foreign-chain guard until the shard is restored or rebuilt"
                    : "the chain life increments only on a real chain break (restore/DR in the same prefix or shard rebuild) — delivered WAL guarantees are unaffected",
                foreign ? AlertRemedy.OperatorRunbook : AlertRemedy.WorkerAuto,
                foreign
                    ? "follow the restore runbook: verify the foreign chain in the shard prefix and rebuild/restore the shard so a new life starts"
                    : "no action required: deltas and tail of the previous life are swept, the new life starts clean");
        }
    }
}
