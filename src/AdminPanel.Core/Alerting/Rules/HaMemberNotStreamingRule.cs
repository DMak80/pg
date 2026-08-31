using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

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
                $"член {member.Name} scope {scope.Scope} в состоянии {member.State} (роль {member.Role}, ожидалось {expected})",
                new Dictionary<string, string>
                {
                    ["scope"] = scope.Scope,
                    ["member"] = member.Name,
                    ["role"] = member.Role!,
                    ["state"] = member.State!,
                    ["expected"] = expected,
                },
                null,
                "реплика Patroni не в streaming: потоковая репликация — основа HA, отставшая реплика не примет failover; каждая реплика скопа обязана стримить",
                AlertRemedy.WorkerAuto,
                "надзор воркера закроет rebuild (TO_RECREATE); висит — проверьте /service/<scope>/members и запустите recreate ноды через API панели");
        }
    }
}
