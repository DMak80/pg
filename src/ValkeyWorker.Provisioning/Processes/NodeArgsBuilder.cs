namespace ValkeyWorker.Provisioning.Processes;

// Сборка аргументов контейнера ноды (arch/21 §2): ACL при старте + maxmemory +
// persistence off. Детерминизм: аргументы ТОЛЬКО из etcd-факта (декларация +
// креды) → пересоздание контейнера собирает актуальные пароли (spec §4.5.1).
public static class NodeArgsBuilder
{
    public static IReadOnlyList<string> Build(
        long maxmemoryBytes, string maxmemoryPolicy, string adminPassword, string appPassword)
        =>
        [
            "valkey-server",
            "--user", "default", "off",
            "--user", "admin", "on", $">{adminPassword}", "~*", "+@all",
            "--user", "app", "on", $">{appPassword}", "~*", "+@read", "+@write",
            "--maxmemory", maxmemoryBytes.ToString(System.Globalization.CultureInfo.InvariantCulture),
            "--maxmemory-policy", maxmemoryPolicy,
            "--save", "",
            "--appendonly", "no",
            // TLS (t06, arch/21 §2): тот же клиентский порт portalloc (контейнерный
            // 6379 слушает TLS), plain закрыт; серты — /tls (volume vwk-<C>-tls);
            // клиенты без сертификатов — принципалы из ACL.
            "--tls-port", "6379",
            "--port", "0",
            "--tls-cert-file", "/tls/node.crt",
            "--tls-key-file", "/tls/node.key",
            "--tls-ca-cert-file", "/tls/ca.pem",
            "--tls-auth-clients", "no",
            "--tls-replication", "no",
        ];
}
