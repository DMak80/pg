using System.Globalization;

namespace ValkeyWorker.Core.Writing;

// Канонический JSON-контракт домена (arch/20 §2.1; pg §9.3 для ресурсов):
// единое место сборки значений ключей etcd — читатели (панель t03, дискавери
// t04) видят дословно канонические строки.
public static class ValkeyWriting
{
    // Канонический JSON config-ключа Active-вида — БЕЗ state (снимается
    // provisioning V5, txn compare mod_revision): snake_case, без пробелов.
    public static string ConfigJson(int nodes, long maxmemoryBytes, string policy, long createdUnix)
        => $$"""{"nodes":{{nodes}},"maxmemory_bytes":{{maxmemoryBytes}},"maxmemory_policy":"{{policy}}","created_unix":{{createdUnix}}}""";

    // Дискавери-ключ endpoints (arch/20 §2): "h:p" (nodes=1 — один адрес).
    public static string EndpointsValue(string host, int port)
        => $"{host}:{port}";

    // Каноническая decimal-строка invariant ("2", "0.5") — образец pg §9.3.
    public static string CanonicalCpu(decimal cpu)
        => cpu.ToString("0.#########", CultureInfo.InvariantCulture);

    // Заявка ресурсов ноды (pg §9.3: cpu — decimal-строка, mem/disk — только "<n>Gi").
    public static string ResourcesJson(decimal cpu, int memGi, int diskGi)
        => $$"""{"cpu":"{{CanonicalCpu(cpu)}}","mem":"{{memGi}}Gi","disk":"{{diskGi}}Gi"}""";

    /// <summary>8 значений maxmemory_policy канона (arch/20 §2; валидация — API).</summary>
    public static readonly IReadOnlySet<string> KnownPolicies = new HashSet<string>(
        [
            "allkeys-lru", "allkeys-lfu",
            "volatile-lru", "volatile-lfu",
            "allkeys-random", "volatile-random",
            "volatile-ttl", "noeviction",
        ]);
}
