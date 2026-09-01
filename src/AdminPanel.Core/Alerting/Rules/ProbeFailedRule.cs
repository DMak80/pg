using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

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
                $"SQL-проба шарда {cluster.Name}/{shard.Name} не удалась: {failed.Error}",
                new Dictionary<string, string>
                {
                    ["kind"] = "sql",
                    ["target"] = $"{cluster.Name}/{shard.Name}",
                    ["error"] = failed.Error ?? string.Empty,
                    ["dsnHosts"] = string.Join(",", shard.DsnHosts),
                },
                null,
                "панель не смогла подключиться ни к одному хосту DSN шарда либо writable-мастер не найден: шард недоступен целиком — либо кластер лежит, либо недостижим из сети панели; SQL-живость — предусловие live-данных (слоты/лаги/инвентарь)",
                AlertRemedy.OperatorRunbook,
                "проверьте контейнеры нод шарда и Patroni-скоп, достижимость хостов DSN из сети панели; панель ретраит пробу каждым тиком");
        }

        // Patroni: matched-скоп Active-кластера. Результат есть по каждому
        // члену и все упали → один critical на скоп; иначе per-member warning.
        foreach (var scope in snapshot.HaScopes.Where(s => s.Matched && s.Cluster is not null
                     && activeClusters.ContainsKey(s.Cluster)))
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
                    $"Patroni-скоп {scope.Scope} недоступен целиком: {results.Count(r => r is { Ok: false })}/{scope.Members.Count} проб упали ({first.Error})",
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
                    "ни один член скопа не ответил на Patroni REST :8008 — HA-кластер Patroni невидим для панели: недоступен целиком либо изолирован от сети панели; REST-живость — предусловие live-данных HA",
                    AlertRemedy.OperatorRunbook,
                    "проверьте patroni-эмуляторы/ноды скопа (контейнеры, сеть, HostMap стенда) и живость Patroni; панель ретраит пробы каждым тиком");
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
                    $"проба patroni по {scope.Scope}/{member.Name} не удалась: {failed.Error}",
                    new Dictionary<string, string>
                    {
                        ["kind"] = "patroni",
                        ["target"] = $"{scope.Scope}/{member.Name}",
                        ["error"] = failed.Error ?? string.Empty,
                    },
                    null,
                    "проба панели не дошла до цели: пробы (Patroni REST/SQL) идут из контейнера панели, неудача означает сетевую недостижимость или нездоровье цели; успешная проба — предусловие live-данных",
                    AlertRemedy.OperatorRunbook,
                    "проверьте достижимость цели из сети панели (сервисы стенда) и живость самой цели; панель ретраит пробу следующим тиком");
            }
        }
    }

    // Lookup результата пробы тика по kind+target (строки — ordinal).
    private static ProbeResult? Find(IReadOnlyList<ProbeResult> probes, string kind, string target)
        => probes.FirstOrDefault(p => p.Kind == kind && p.Target == target);
}
