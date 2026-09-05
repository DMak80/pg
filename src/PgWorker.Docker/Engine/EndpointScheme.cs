namespace PgWorker.Docker.Engine;

// Разбор endpoint-схем Engine API (arch/14 §2.2, t03): unix://<path> |
// tcp://[host][:port] | ssh://[user@]host[:port]. Чистая функция — юнит-тесты
// без сети; дефолты портов: tcp — 2375 (plain Engine API), ssh — 22.
public sealed record EndpointScheme(string Scheme, string Host, int Port, string? User)
{
    public const string Unix = "unix";
    public const string Tcp = "tcp";
    public const string Ssh = "ssh";
    public const int DefaultTcpPort = 2375;
    public const int DefaultSshPort = 22;

    public static EndpointScheme Parse(string endpoint)
    {
        if (endpoint.StartsWith("unix://", StringComparison.Ordinal))
            return new EndpointScheme(Unix, endpoint["unix://".Length..], 0, null);

        foreach (var scheme in new[] { (Tcp, DefaultTcpPort), (Ssh, DefaultSshPort) })
        {
            if (!endpoint.StartsWith(scheme.Item1 + "://", StringComparison.Ordinal))
                continue;
            var rest = endpoint[(scheme.Item1.Length + 3)..];
            string? user = null;
            var at = rest.LastIndexOf('@');
            if (at >= 0)
            {
                user = rest[..at];
                rest = rest[(at + 1)..];
            }

            // порт — после последнего ':' (хосты — DNS/IPv4; IPv6-литералы вне канона §2.2)
            var port = scheme.Item2;
            var colon = rest.LastIndexOf(':');
            if (colon >= 0 && int.TryParse(rest[(colon + 1)..], out var explicitPort))
            {
                port = explicitPort;
                rest = rest[..colon];
            }

            if (string.IsNullOrEmpty(rest))
                throw new ApplicationException($"endpoint без хоста: {endpoint}");
            return new EndpointScheme(scheme.Item1, rest, port, user);
        }

        throw new ApplicationException(
            $"неизвестная схема endpoint: {endpoint} (ожидался unix://|tcp://|ssh://, arch/14 §2.2)");
    }
}
