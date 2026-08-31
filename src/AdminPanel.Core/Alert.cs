namespace AdminPanel.Core;

// Алерт из AlertEngine (t04): стабильный id "kind:target" (arch/01 §3).
public enum AlertSeverity
{
    Info,
    Warning,
    Critical,
}

// Движитель закрытия алерта (arch/03 §4.1): кто обязан действовать.
public enum AlertRemedy
{
    /// <summary>Закроет сам воркер (provisioning/надзор/репарация/converge).</summary>
    WorkerAuto,

    /// <summary>Оператор через API панели (мутация-прокси в API воркера).</summary>
    OperatorApi,

    /// <summary>Оператор по runbook (контейнеры/сеть/etcd-контур — вручную).</summary>
    OperatorRunbook,
}

/// <summary>
/// Алерт с объяснением и движителем (arch/03 §4.1): Hint — что не так / как
/// должно быть / для чего ключ-инвариант; Remedy — кто закрывает; RemedyText —
/// конкретное действие. Поля обязательны: правил, оставляющих их пустыми, нет
/// (unit-инвариант AlertHintRemedyTests).
/// </summary>
public sealed record Alert(
    string Id,
    AlertSeverity Severity,
    string Kind,
    string Target,
    string Message,
    IReadOnlyDictionary<string, string>? Details,
    long? SinceUnix,
    string Hint,
    AlertRemedy Remedy,
    string RemedyText);
