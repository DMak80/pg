using System.Text.RegularExpressions;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Etcd.Client;
using PgWorker.Provisioning.Probes;
using PgWorker.Provisioning.Sql;

namespace PgWorker.Provisioning.Endpoints;

/// <summary>
/// Адресация шардов кластера (t01 задача 9, spec §5.1): чтение portalloc,
/// резолв мастера (master-ключ → ноды шарда → Patroni /cluster fallback;
/// вынос ResolveMasterAsync из BucketEvacuator в общий сервис) + DSN-билдеры:
/// admin-Npgsql (postgres), mover-libpq-conninfo (CREATE SUBSCRIPTION) и
/// mover-Npgsql-DSN (SQL-пробы роли bucket_mover, ревью №2).
/// </summary>
public sealed partial class ShardEndpoints(IEtcdGateway etcd, string[] endpoints, ShardProbe probe)
{
    // Роль подписок переезда (создаёт DatabaseProvisioner). Локальная константа:
    // MoveNames.MoverRole недоступен без циклической ссылки Moves↔Provisioning
    // (паттерн локального дубля Redact у DatabaseProvisioner).
    public const string MoverRole = "bucket_mover";

    // user=… в libpq-строке dsn-ключа (P2.5): пара key=value, отделённая
    // пробелом или началом строки — заменяется на mover-роль.
    [GeneratedRegex(@"(^| )user=[^ ]*", RegexOptions.CultureInvariant)]
    private static partial Regex UserRegex();

    // ── Чтение адресов ──

    // /pgworker/portalloc/<C>: ключа нет → пустой словарь (failover-обёртка).
    public async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> ReadPortAllocAsync(
        string cluster, CancellationToken ct)
    {
        Result<Kv?>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.GetAsync(endpoint, $"/pgworker/portalloc/{cluster}", ct);
            if (!result.IsSuccess)
            {
                last = result;
                continue;
            }

            if (result.Value is not { } kv)
                return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(
                    (IReadOnlyDictionary<string, NodeAddress>)new Dictionary<string, NodeAddress>());

            return Portalloc.Parse(cluster, kv.Value);
        }

        return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(last!.Error!);
    }

    // Мастер шарда для SQL (перенос из BucketEvacuator без изменения поведения):
    // master-ключ → поиск среди нод ЭТОГО шарда по имени, затем по host (host
    // неуникален — на нём ноды разных шардов) → Patroni /cluster fallback.
    public async Task<Result<NodeAddress?>> ResolveMasterAsync(
        ShardSpec shard, IReadOnlyDictionary<string, NodeAddress> addresses, CancellationToken ct)
    {
        var shardNodes = addresses
            .Where(p => p.Key.StartsWith($"{shard.Name}/", StringComparison.Ordinal))
            .ToDictionary(p => p.Key.Split('/')[1], p => p.Value);

        if (!string.IsNullOrWhiteSpace(shard.Master))
        {
            var left = shard.Master.Split(':')[0];
            var byName = shardNodes.FirstOrDefault(p => p.Key == left);
            if (byName.Value is not null)
                return Result<NodeAddress?>.Success(byName.Value);
            var byHost = shardNodes.FirstOrDefault(p => p.Value.Host == left);
            if (byHost.Value is not null)
                return Result<NodeAddress?>.Success(byHost.Value);
        }

        foreach (var node in shardNodes)
        {
            var members = await probe.GetClusterAsync(node.Value, ct);
            if (!members.IsSuccess)
                continue;
            // Patroni 3.x в /cluster называет мастера "leader" (legacy: "master").
            var master = members.Value.FirstOrDefault(m =>
                m.Role is "master" or "leader" or "primary" && m.State == "running");
            if (master is not null && shardNodes.TryGetValue(master.Name, out var addr))
                return Result<NodeAddress?>.Success(addr);
        }

        return Result<NodeAddress?>.Success(null);
    }

    // ── DSN-билдеры ──

    // Admin-DSN мастера (postgres): управляющий SQL переездов, как весь SQL-слой.
    public static string AdminDsn(NodeAddress master, string dbname, InstallSecrets secrets)
        => DatabaseProvisioner.BuildAdminDsn(master.Host, master.Ports.Pg, dbname, secrets);

    // libpq-conninfo для CREATE SUBSCRIPTION из dsn-ключа шарда (P2):
    // multi-host host=… port=… — семантический эквивалент HAProxy-входа
    // скриптов; user подменяется на bucket_mover, добавляется его пароль.
    public static string MoverConninfo(string shardDsn, InstallSecrets secrets)
    {
        var dsn = UserRegex().Replace(shardDsn, "$1user=" + MoverRole);
        if (!UserRegex().IsMatch(dsn))
            dsn += " user=" + MoverRole;
        return dsn + " password=" + secrets.MoverPassword;
    }

    // Npgsql-DSN для SQL-проб роли bucket_mover (spec §6.1 M0, ревью №2):
    // конвертация той же libpq-строки — сплит по пробелам → пары key=value →
    // маппинг host→Host, port→Port, dbname→Database, user→Username (mover).
    public static string MoverNpgsqlDsn(string shardDsn, InstallSecrets secrets)
    {
        var parts = new List<string>();
        var hasUser = false;
        foreach (var token in shardDsn.Split(' ', StringSplitOptions.RemoveEmptyEntries))
        {
            var eq = token.IndexOf('=');
            if (eq <= 0)
                continue;
            var value = token[(eq + 1)..];
            switch (token[..eq])
            {
                case "host":
                    parts.Add("Host=" + value);
                    break;
                case "port":
                    parts.Add("Port=" + value);
                    break;
                case "dbname":
                    parts.Add("Database=" + value);
                    break;
                case "user":
                    parts.Add("Username=" + MoverRole);
                    hasUser = true;
                    break;
            }
        }

        if (!hasUser)
            parts.Add("Username=" + MoverRole); // вход без user= — роль добавляется
        parts.Add("Password=" + secrets.MoverPassword);
        return string.Join(";", parts);
    }
}
