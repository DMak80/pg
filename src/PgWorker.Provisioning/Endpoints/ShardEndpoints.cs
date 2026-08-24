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
            var parts = shard.Master.Split(':');
            var byName = shardNodes.FirstOrDefault(p => p.Key == parts[0]);
            if (byName.Value is not null)
                return Result<NodeAddress?>.Success(byName.Value);
            // Формат писателей ключа (Patroni-callback и reconciler): <host>:<doormanPort>.
            // Host неуникален (single-docker-host стенды: все ноды localhost) —
            // ноду различает doorman-порт (e2e-факт t01).
            if (parts.Length == 2 && int.TryParse(parts[1], out var doorman))
            {
                var byHostPort = shardNodes.FirstOrDefault(
                    p => p.Value.Host == parts[0] && p.Value.Ports.Doorman == doorman);
                if (byHostPort.Value is not null)
                    return Result<NodeAddress?>.Success(byHostPort.Value);
            }
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
    // advertisedHost — как издатель виден ИЗ контейнера приёмника (single-host
    // стенды: host.docker.internal; null — адреса dsn-ключа как есть, прод).
    // Подмена ПОЭЛЕМЕНТНО: libpq требует соответствия числа host и port.
    public static string MoverConninfo(string shardDsn, InstallSecrets secrets, string? advertisedHost = null)
    {
        var dsn = UserRegex().Replace(shardDsn, "$1user=" + MoverRole);
        if (!UserRegex().IsMatch(dsn))
            dsn += " user=" + MoverRole;
        if (advertisedHost is { Length: > 0 })
            dsn = HostRegex().Replace(dsn, m =>
                (m.Value.StartsWith(' ') ? " " : "") + "host=" +
                string.Join(",", m.Value[(m.Value.IndexOf('=') + 1)..].Split(',').Select(_ => advertisedHost)));
        return dsn + " password=" + secrets.MoverPassword;
    }

    // host=… пары key=value conninfo (замена хостов издателя на advertised).
    [GeneratedRegex(@"(^| )host=[^ ]*", RegexOptions.CultureInvariant)]
    private static partial Regex HostRegex();

    // Npgsql-DSN для SQL-проб роли bucket_mover (spec §6.1 M0, ревью №2):
    // конвертация той же libpq-строки — сплит по пробелам → пары key=value →
    // маппинг host→Host, port→Port, dbname→Database, user→Username (mover).
    // Разные порты нод Npgsql принимает ТОЛЬКО парами «Host=h1:p1,h2:p2» —
    // список в Port= отвергается («Couldn't set port», e2e-факт t01).
    public static string MoverNpgsqlDsn(string shardDsn, InstallSecrets secrets)
    {
        string? host = null, port = null, dbname = null;
        foreach (var token in shardDsn.Split(' ', StringSplitOptions.RemoveEmptyEntries))
        {
            var eq = token.IndexOf('=');
            if (eq <= 0)
                continue;
            var value = token[(eq + 1)..];
            switch (token[..eq])
            {
                case "host":
                    host = value;
                    break;
                case "port":
                    port = value;
                    break;
                case "dbname":
                    dbname = value;
                    break;
            }
        }

        var parts = new List<string>();
        var hosts = host?.Split(',') ?? [];
        var ports = port?.Split(',') ?? [];
        if (hosts.Length > 1 && ports.Length == hosts.Length)
            parts.Add("Host=" + string.Join(",", hosts.Zip(ports, (h, p) => $"{h}:{p}")));
        else
        {
            if (hosts.Length > 0)
                parts.Add("Host=" + string.Join(",", hosts));
            if (ports.Length == 1)
                parts.Add("Port=" + ports[0]);
        }

        if (dbname is not null)
            parts.Add("Database=" + dbname);
        parts.Add("Username=" + MoverRole);
        parts.Add("Password=" + secrets.MoverPassword);
        return string.Join(";", parts);
    }
}
