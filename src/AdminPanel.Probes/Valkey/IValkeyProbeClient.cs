namespace AdminPanel.Probes.Valkey;

// Цель PING-пробы: адрес уже разрешён HostMapResolver'ом (host:port) + креды.
// CaPem (t06) — TLS-доверие пробы (per-cluster CA, arch/02 §11.1); null —
// миграция TLS не доиграна, TLS-проба невозможна.
public sealed record ValkeyProbeTarget(
    string Host, int Port, string AdminUser, string AdminPassword, string? CaPem = null);

// Клиент live-пробы: одна проба = одно короткоживущее соединение AUTH+PING.
public interface IValkeyProbeClient
{
    Task<Shared.Core.Result> PingAsync(ValkeyProbeTarget target, TimeSpan timeout, CancellationToken ct);
}
