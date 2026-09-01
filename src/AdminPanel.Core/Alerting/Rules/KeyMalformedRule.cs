using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Core.Alerting.Rules;

// key-malformed (warning): ключ не разобран — по одному на ParseError (arch/03 §4; t03 §3.4).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class KeyMalformedRule : IAlertRule
{
    public const string KindName = "key-malformed";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var error in snapshot.ParseErrors)
            yield return new Alert(
                $"{KindName}:{error.Key}",
                AlertSeverity.Warning,
                KindName,
                error.Key,
                $"ключ не разобран: {error.Key}",
                new Dictionary<string, string> { ["reason"] = error.Reason },
                null,
                "ключ etcd не разобран парсером панели: битое значение не попадает в модель — UI слеп к этому ключу; формат значения каждого ключа — канон arch/02 §2",
                AlertRemedy.OperatorRunbook,
                "устраните источник битой записи (внешний писатель) и приведите значение к канону arch/02; повторный тик распарсит ключ");
    }
}
