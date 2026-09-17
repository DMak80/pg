using System.Globalization;
using System.Text.Json;
using Shared.Core;
using Shared.Etcd.Client;
using ValkeyWorker.Core.Model;
using ValkeyWorker.Core.Valkey;

namespace ValkeyWorker.Provisioning.Processes;

/// <summary>
/// ConfigConverger (arch/21 §5 D) — лёгкий шаг Active-ветки: CONFIG GET vs
/// декларация → CONFIG SET при отличии (без рестартов; маппинг
/// maxmemory_bytes→maxmemory, maxmemory_policy→maxmemory-policy); ACL-план:
/// ACL LIST vs канон (admin/app — on с правами §2; default off) →
/// идемпотентный ACL SETUSER-converge (пароли в ACL LIST не видны — сверка
/// прав и on/off-статуса, пароли — только окно ротации E). maxmemory_bytes ≥
/// mem-лимита → journal-warning (ответственность оператора, R3). Ошибка
/// соединения → Failed (тик повторится).
/// </summary>
public sealed class ConfigConverger(
    IValkeyConnection valkey, IEtcdGateway gateway, string[] endpoints, WorkJournal journal)
{
    public async Task<Result> TickAsync(ValkeyClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        // Active-ветка: endpoints + admin-кред обязательны.
        if (snap.Endpoints is null || snap.AdminUser is null || snap.AdminPassword is null)
            return Result.Failed(new ApplicationException(
                $"converge {cluster}: нет endpoints/admin-креда — converge невозможен"));

        var endpoint = ProcessCommon.ParseEndpoint(snap.Endpoints);
        var admin = new ValkeyEndpoint(endpoint.Host, endpoint.Port, snap.AdminUser, snap.AdminPassword);

        // CONFIG GET/SET maxmemory.
        var maxmemory = await valkey.ConfigGetAsync(admin, "maxmemory", ct);
        if (!maxmemory.IsSuccess)
            return Result.Failed(new ApplicationException(
                $"converge {cluster}: CONFIG GET maxmemory: {maxmemory.Error!.Message}", maxmemory.Error!));
        var declaredBytes = snap.Config?.MaxmemoryBytes ?? 0;
        if (maxmemory.Value.TryGetValue("maxmemory", out var liveBytes)
            && long.TryParse(liveBytes, out var liveValue)
            && liveValue != declaredBytes)
        {
            var setBytes = await valkey.ConfigSetAsync(
                admin, "maxmemory", declaredBytes.ToString(CultureInfo.InvariantCulture), ct);
            if (!setBytes.IsSuccess)
                return Result.Failed(new ApplicationException(
                    $"converge {cluster}: CONFIG SET maxmemory: {setBytes.Error!.Message}", setBytes.Error!));
        }

        // CONFIG GET/SET maxmemory-policy.
        var policy = await valkey.ConfigGetAsync(admin, "maxmemory-policy", ct);
        if (!policy.IsSuccess)
            return Result.Failed(new ApplicationException(
                $"converge {cluster}: CONFIG GET maxmemory-policy: {policy.Error!.Message}", policy.Error!));
        var declaredPolicy = snap.Config?.MaxmemoryPolicy ?? "allkeys-lru";
        if (policy.Value.TryGetValue("maxmemory-policy", out var livePolicy) && livePolicy != declaredPolicy)
        {
            var setPolicy = await valkey.ConfigSetAsync(admin, "maxmemory-policy", declaredPolicy, ct);
            if (!setPolicy.IsSuccess)
                return Result.Failed(new ApplicationException(
                    $"converge {cluster}: CONFIG SET maxmemory-policy: {setPolicy.Error!.Message}", setPolicy.Error!));
        }

        // ACL-план канона (arch/21 §5 D): admin/app — ВКЛЮЧЕНЫ с правами §2
        // (выключенный пользователь права имеет, но не работает — сверяем и
        // флаг on); default — off (включённый default — дыра безпарольного
        // входа). Пароли в ACL LIST не видны — не сверяем.
        var acl = await valkey.AclListAsync(admin, ct);
        if (!acl.IsSuccess)
            return Result.Failed(new ApplicationException(
                $"converge {cluster}: ACL LIST: {acl.Error!.Message}", acl.Error!));
        var plan = new Dictionary<string, string[]>
        {
            ["admin"] = ["~*", "+@all"],
            ["app"] = ["~*", "+@read", "+@write"],
        };
        foreach (var (user, rights) in plan)
        {
            var missing = MissingRights(acl.Value, user, rights);
            var enabled = HasFlag(acl.Value, user, "on");
            if (missing.Length > 0 || !enabled)
            {
                // on + весь план прав: SETUSER идемпотентен — недостающее
                // добавляет, присутствующее не трогает.
                var converge = await valkey.AclSetUserAsync(admin, [user, "on", .. rights], ct);
                if (!converge.IsSuccess)
                    return Result.Failed(new ApplicationException(
                        $"converge {cluster}: ACL SETUSER {user}: {converge.Error!.Message}", converge.Error!));
            }
        }

        // default off: включённый (или отсутствующий в списке) — конверге.
        if (!HasFlag(acl.Value, "default", "off"))
        {
            var converge = await valkey.AclSetUserAsync(admin, ["default", "off"], ct);
            if (!converge.IsSuccess)
                return Result.Failed(new ApplicationException(
                    $"converge {cluster}: ACL SETUSER default: {converge.Error!.Message}", converge.Error!));
        }

        // R3: maxmemory_bytes ≥ mem-лимита → journal-warning (ответственность
        // оператора). Лимит — из СВЕЖЕЙ декларации etcd (мутация могла прийти
        // между снапшотом и тиком; gateway — etcd-грань конвергера).
        var freshLimits = await ReadNodeLimitsAsync(cluster, "node1", ct);
        var limits = freshLimits ?? ProcessCommon.ParseResources(
            snap.Nodes.TryGetValue("node1", out var node1) ? node1.Resources : null);
        if (limits?.MemBytes is { } memBytes && declaredBytes >= memBytes)
        {
            await journal.WritePhaseAsync(
                cluster, "converge", "warning-maxmemory-mem", "",
                $"maxmemory_bytes ({declaredBytes}) >= mem-лимит ({memBytes}) — риск OOM-килла (R3)", ct);
        }

        return Result.Success();
    }

    // Права канона, которых нет в правиле пользователя ("user <name> <flags…>",
    // имя — вторым словом; пароли-хэши не права — не сравниваются).
    private static string[] MissingRights(IReadOnlyList<string> acl, string user, string[] rights)
    {
        string? rule = null;
        foreach (var entry in acl)
        {
            var words = entry.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (words.Length > 1 && words[1] == user)
            {
                rule = entry;
                break;
            }
        }

        if (rule is null)
            return rights; // пользователя нет — весь план
        var existing = rule.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        return rights.Where(right => !existing.Contains(right)).ToArray();
    }

    // Флаг-слово в правиле пользователя (on/off): правило отсутствует → false.
    private static bool HasFlag(IReadOnlyList<string> acl, string user, string flag)
    {
        foreach (var entry in acl)
        {
            var words = entry.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (words.Length > 1 && words[1] == user)
                return words.Contains(flag);
        }

        return false;
    }

    // Свежие лимиты ноды из etcd (failover); null — ключа нет/битый JSON.
    private async Task<(decimal? Cpu, long? MemBytes)?> ReadNodeLimitsAsync(
        string cluster, string node, CancellationToken ct)
    {
        Result<Kv?>? last = null;
        foreach (var endpoint in endpoints)
        {
            var kv = await gateway.GetAsync(endpoint, $"/valkey/clusters/{cluster}/nodes/{node}/resources", ct);
            if (!kv.IsSuccess)
            {
                last = kv;
                continue;
            }

            if (kv.Value is not { } entry)
                return null;
            try
            {
                using var doc = JsonDocument.Parse(entry.Value);
                var root = doc.RootElement;
                var cpu = root.TryGetProperty("cpu", out var c) && c.ValueKind == JsonValueKind.String
                          && decimal.TryParse(c.GetString(), NumberStyles.Number, CultureInfo.InvariantCulture, out var cpuValue)
                    ? cpuValue
                    : (decimal?)null;
                var memRaw = root.TryGetProperty("mem", out var m) && m.ValueKind == JsonValueKind.String
                    ? m.GetString()
                    : null;
                long? mem = memRaw is { } raw
                            && raw.EndsWith("Gi", StringComparison.Ordinal)
                            && long.TryParse(raw[..^2], out var gi)
                    ? gi * 1024L * 1024 * 1024
                    : null;
                return (cpu, mem);
            }
            catch (JsonException)
            {
                return null;
            }
        }

        return null;
    }
}
