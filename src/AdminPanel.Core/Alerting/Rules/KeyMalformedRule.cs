using AdminPanel.Core.Alerting;
using Shared.Core.DI;

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
                $"key not parsed: {error.Key}",
                new Dictionary<string, string> { ["reason"] = error.Reason },
                null,
                "etcd key not parsed by the panel parser: malformed value never reaches the model — the UI is blind to this key; the value format of every key follows the arch/02 §2 canon",
                AlertRemedy.OperatorRunbook,
                "eliminate the source of the malformed record (external writer) and align the value with the arch/02 canon; a subsequent tick will parse the key");
    }
}
