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
        ];
}
