using AdminPanel.Core.Alerting;
using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// ha-member-not-streaming (warning): Patroni-проба успешна, но состояние члена
// не совпадает с ожиданием по роли: master → running, replica → streaming
// (arch/03 §4; spec §3.13 — прочие роли и упавшие пробы не проверяются).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class HaMemberNotStreamingRule : IAlertRule
{
    public const string KindName = "ha-member-not-streaming";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var scope in snapshot.HaScopes.Where(s => s.Matched))
        foreach (var member in scope.Members)
        {
            if (member.ProbeAtUtc is null || member.ProbeError is not null)
                continue; // данных нет или ошибка пробы (зона probe-failed)

            var expected = member.Role switch
            {
                "master" => "running",
                "replica" => "streaming",
                _ => null,
            };
            if (expected is null || member.State == expected)
                continue;

            yield return new Alert(
                $"{KindName}:{scope.Scope}/{member.Name}",
                AlertSeverity.Warning,
                KindName,
                $"{scope.Scope}/{member.Name}",
                $"member {member.Name} in scope {scope.Scope} is in state {member.State} (role {member.Role}, expected {expected})",
                new Dictionary<string, string>
                {
                    ["scope"] = scope.Scope,
                    ["member"] = member.Name,
                    ["role"] = member.Role!,
                    ["state"] = member.State!,
                    ["expected"] = expected,
                },
                null,
                "Patroni replica is not streaming: streaming replication is the foundation of HA, a lagging replica cannot take over in a failover; every replica of the scope must stream",
                AlertRemedy.WorkerAuto,
                "worker supervision will handle the rebuild (TO_RECREATE); if it stays stuck, check /service/<scope>/members and trigger the node recreate via the panel API");
        }
    }
}
