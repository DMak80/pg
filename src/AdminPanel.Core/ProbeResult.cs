namespace AdminPanel.Core;

// Результат live-пробы (t06): ok/error/latency по цели (arch/02 §6, минимальный контракт spec §3.5).
public sealed record ProbeResult(
    string Target,
    string Kind,
    bool Ok,
    double? LatencyMs,
    string? Error,
    DateTimeOffset AtUtc);
