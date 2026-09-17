namespace AdminPanel.Probes.Valkey;

// Цель PING-пробы: адрес уже разрешён HostMapResolver'ом (host:port) + креды.
public sealed record ValkeyProbeTarget(string Host, int Port, string AdminUser, string AdminPassword);

// Клиент live-пробы: одна проба = одно короткоживущее соединение AUTH+PING.
public interface IValkeyProbeClient
{
    Task<Shared.Core.Result> PingAsync(ValkeyProbeTarget target, TimeSpan timeout, CancellationToken ct);
}
