namespace AdminPanel.Core;

// Алерт из AlertEngine (t04): стабильный id "kind:target" (arch/01 §3).
public enum AlertSeverity
{
    Info,
    Warning,
    Critical,
}

public sealed record Alert(
    string Id,
    AlertSeverity Severity,
    string Kind,
    string Target,
    string Message,
    IReadOnlyDictionary<string, string>? Details,
    long? SinceUnix);
