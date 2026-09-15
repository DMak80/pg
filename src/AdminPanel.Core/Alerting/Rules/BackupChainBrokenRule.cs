using AdminPanel.Core.Alerting;
using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// backup-chain-broken (critical, t07, arch/19 §3/§4): state=BROKEN wal-ключа
// шарда живого Active-кластера — разрыв цепочки; воркер сам лечит пересъёмом
// полного (планировщик arch/19 §2 реагирует на BROKEN). Текст показывает и
// сбой, и действие воркера. DEGRADED — transient, остаётся правилу
// wal-chain-broken (t03).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class BackupChainBrokenRule : IAlertRule
{
    public const string KindName = "backup-chain-broken";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var backups in snapshot.Backups)
        foreach (var (shard, wal) in backups.Shards ?? new Dictionary<string, WalStreamInfo?>())
        {
            if (wal is not { State: WalStreamInfoState.Broken })
                continue;

            // только живой Active-кластер: мёртвый кластер — не алерт, а сирота
            // (backup-orphan); демонтаж сам останавливает поток.
            var cluster = snapshot.Clusters.FirstOrDefault(
                c => c.Name == backups.Cluster && c.State == ClusterState.Active
                     && c.Shards.Any(s => s.Name == shard));
            if (cluster is null)
                continue;

            yield return new Alert(
                $"{KindName}:{backups.Cluster}/{shard}",
                AlertSeverity.Critical,
                KindName,
                $"{backups.Cluster}/{shard}",
                $"разрыв WAL-цепочки шарда {shard} кластера {backups.Cluster}: {wal.Error ?? "без причины"} — воркер переснимает полный бэкап",
                new Dictionary<string, string>
                {
                    ["state"] = "BROKEN",
                    ["slot"] = wal.Slot,
                    ["error"] = wal.Error ?? "",
                },
                null,
                "цепочка full+WAL невосстановима наращиванием: новая точка — только новый полный",
                AlertRemedy.WorkerAuto,
                "воркер переснимает полный бэкап и поднимет агента (t07); затяжное лечение видно по backup-full-stale/серии FAILED — разбор по runbook arch/19");
        }
    }
}
