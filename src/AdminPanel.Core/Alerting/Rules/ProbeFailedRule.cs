using AdminPanel.Core.Alerting;
using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// probe-failed — severity по цели (arch/03 §4; spec 2026-09-01 §3.1): SQL-проба
// шарда Active-кластера упала = critical («шард недоступен» — ни один хост DSN
// не принял подключение или writable-мастер не найден); Patroni-проба одного
// члена matched-скопа упала = warning; Patroni-пробы всех членов скопа упали =
// один critical на скоп (per-member warning не эмитятся — один факт, один
// алерт). Lifecycle-цели (кластеры/шарды NOT_INITIALIZED/TO_REMOVE) не
// алертятся — подъём/демонтаж не авария (прецедент shard-no-leader); пробы по
// ним продолжают ходить, runtime-ошибки остаются в UI деталей. Правило идёт от
// целей текущего снапшота — исчезнувшая цель не алертится.
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class ProbeFailedRule : IAlertRule
{
    public const string KindName = "probe-failed";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        var activeClusters = snapshot.Clusters
            .Where(c => c.State == ClusterState.Active)
            .ToDictionary(c => c.Name);

        // SQL: шард с DSN и упавшей пробой — шард недоступен (critical).
        foreach (var cluster in activeClusters.Values)
        foreach (var shard in cluster.Shards.Where(s => s.DsnHosts.Count > 0 && s.State == ShardState.Active))
        {
            var failed = Find(snapshot.Probes, "sql", $"{cluster.Name}/{shard.Name}");
            if (failed is not { Ok: false })
                continue; // тика не было / проба успешна — не авария (spec §3.1 п.1)

            yield return new Alert(
                $"{KindName}:sql:{cluster.Name}/{shard.Name}",
                AlertSeverity.Critical,
                KindName,
                $"sql:{cluster.Name}/{shard.Name}",
                $"SQL probe for shard {cluster.Name}/{shard.Name} failed: {failed.Error}",
                new Dictionary<string, string>
                {
                    ["kind"] = "sql",
                    ["target"] = $"{cluster.Name}/{shard.Name}",
                    ["error"] = failed.Error ?? string.Empty,
                    ["dsnHosts"] = string.Join(",", shard.DsnHosts),
                },
                null,
                "the panel could not connect to any DSN host of the shard or no writable master was found: the shard is entirely unavailable — either the cluster is down or unreachable from the panel network; SQL liveness is a prerequisite for live data (slots/lags/inventory)",
                AlertRemedy.OperatorRunbook,
                "check the shard node containers and the Patroni scope, and DSN host reachability from the panel network; the panel retries the probe on every tick");
        }

        // Patroni: matched-скоп Active-кластера и Active-шарда (scope.Shard —
        // remove-shard гасит ноды демонтажа: упавшие пробы членов TO_REMOVE-шарда
        // не авария, критерий 4; шард не найден/не задан — обрабатывается, как
        // матчит ScopeMatcher: по имени, состояние не смотрит). Результат есть
        // по каждому члену и все упали → один critical на скоп; иначе per-member warning.
        static bool ScopeTargetActive(HaScope s, IReadOnlyDictionary<string, ClusterInfo> active)
            => s.Cluster is not null
               && active.TryGetValue(s.Cluster, out var cluster)
               && (s.Shard is null
                   || cluster.Shards.FirstOrDefault(sh => sh.Name == s.Shard) is not { } shard
                   || shard.State == ShardState.Active);

        foreach (var scope in snapshot.HaScopes.Where(s => s.Matched && ScopeTargetActive(s, activeClusters)))
        {
            var results = scope.Members
                .Select(m => Find(snapshot.Probes, "patroni", $"{scope.Scope}/{m.Name}"))
                .ToList();

            if (results.All(r => r is null))
                continue; // тиков не было / проба выключена / членов нет — тишина

            if (results.All(r => r is { Ok: false }))
            {
                var first = results.OfType<ProbeResult>().First(r => !r.Ok);
                yield return new Alert(
                    $"{KindName}:patroni-scope:{scope.Scope}",
                    AlertSeverity.Critical,
                    KindName,
                    $"patroni-scope:{scope.Scope}",
                    $"Patroni scope {scope.Scope} is entirely unavailable: {results.Count(r => r is { Ok: false })}/{scope.Members.Count} probes failed ({first.Error})",
                    new Dictionary<string, string>
                    {
                        ["scope"] = scope.Scope,
                        ["cluster"] = scope.Cluster!,
                        ["shard"] = scope.Shard ?? string.Empty,
                        ["failed"] = results.Count(r => r is { Ok: false }).ToString(),
                        ["total"] = scope.Members.Count.ToString(),
                        ["error"] = first.Error ?? string.Empty,
                    },
                    null,
                    "no scope member responded on Patroni REST :8008 — the Patroni HA cluster is invisible to the panel: entirely unavailable or isolated from the panel network; REST liveness is a prerequisite for live HA data",
                    AlertRemedy.OperatorRunbook,
                    "check the patroni emulators/scope nodes (containers, network, stand HostMap) and Patroni liveness; the panel retries the probes on every tick");
                continue;
            }

            foreach (var member in scope.Members)
            {
                var failed = Find(snapshot.Probes, "patroni", $"{scope.Scope}/{member.Name}");
                if (failed is not { Ok: false })
                    continue;

                yield return new Alert(
                    $"{KindName}:patroni:{scope.Scope}/{member.Name}",
                    AlertSeverity.Warning,
                    KindName,
                    $"patroni:{scope.Scope}/{member.Name}",
                    $"patroni probe for {scope.Scope}/{member.Name} failed: {failed.Error}",
                    new Dictionary<string, string>
                    {
                        ["kind"] = "patroni",
                        ["target"] = $"{scope.Scope}/{member.Name}",
                        ["error"] = failed.Error ?? string.Empty,
                    },
                    null,
                    "the panel probe did not reach the target: probes (Patroni REST/SQL) run from the panel container, a failure means network unreachability or unhealthiness of the target; a successful probe is a prerequisite for live data",
                    AlertRemedy.OperatorRunbook,
                    "check target reachability from the panel network (stand services) and the target's own liveness; the panel retries the probe on the next tick");
            }
        }
    }

    // Lookup результата пробы тика по kind+target (строки — ordinal).
    private static ProbeResult? Find(IReadOnlyList<ProbeResult> probes, string kind, string target)
        => probes.FirstOrDefault(p => p.Kind == kind && p.Target == target);
}
