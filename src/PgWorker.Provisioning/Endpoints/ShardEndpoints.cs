using System.Text.Json;
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

    // Мастер шарда для SQL (adopt-repair §3.3, arch/14 §5 F — цепочка):
    // (1) master-ключ → ноды шарда по имени (byName, включая усыновлённый
    // формат node:pg-port), затем по host:doorman (host неуникален) →
    // (2) HA-лидер контура /service/<C>-<X>/leader → имя → portalloc →
    // (3) Patroni /cluster fallback (по нодам с patroni≠0).
    public async Task<Result<NodeAddress?>> ResolveMasterAsync(
        string cluster, ShardSpec shard, IReadOnlyDictionary<string, NodeAddress> addresses, CancellationToken ct)
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
            // Ноду различает doorman-порт (уникален per-node, e2e-факт t01 — single-host
            // стенды: все ноды на одном хосте). Хост-часть может расходиться с portalloc
            // (advertised-режим, arch/16: ключ пишут ноды с env-хостом контейнера,
            // portalloc — advertised-имя) — резолв по порту, хост информативен.
            if (parts.Length == 2 && int.TryParse(parts[1], out var doorman) && doorman > 0)
            {
                var byDoormanPort = shardNodes.FirstOrDefault(p => p.Value.Ports.Doorman == doorman);
                if (byDoormanPort.Value is not null)
                    return Result<NodeAddress?>.Success(byDoormanPort.Value);
            }
        }

        // Шаг 2 (spec §3.3): HA-лидер контура — имя из /service/<C>-<X>/leader,
        // адрес ноды из portalloc; работает без Patroni-REST (усыновлённые шарды,
        // окно failover с протухшим master-ключом).
        var leader = await GetAsync($"/service/{cluster}-{shard.Name}/leader", ct);
        if (leader.IsSuccess && leader.Value is { } leaderKv)
        {
            try
            {
                using var doc = JsonDocument.Parse(leaderKv.Value);
                if (doc.RootElement.TryGetProperty("name", out var name)
                    && name.GetString() is { Length: > 0 } leaderName
                    && shardNodes.TryGetValue(leaderName, out var leaderAddr))
                    return Result<NodeAddress?>.Success(leaderAddr);
            }
            catch (JsonException)
            {
                // битый leader-ключ — просто идём дальше по цепочке
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

    // Точечный GET с failover-обёрткой (паттерн ReadPortAllocAsync).
    private async Task<Result<Kv?>> GetAsync(string key, CancellationToken ct)
    {
        Result<Kv?>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.GetAsync(endpoint, key, ct);
            if (!result.IsSuccess)
            {
                last = result;
                continue;
            }

            return result;
        }

        return last!;
    }

    // ── DSN-билдеры ──

    // Внешний ли шард-исполнитель подписок (spec §3.3): object-ноды живут вне
    // pgw-net и видят адреса dsn-ключа напрямую — подмена advertised ломает подключение.
    public static bool HasAdoptedNodes(string shard, IReadOnlyDictionary<string, NodeAddress> addresses)
        => addresses.Any(p => p.Key.StartsWith($"{shard}/", StringComparison.Ordinal) && p.Value.Object is not null);

    // Admin-DSN мастера (postgres): управляющий SQL переездов, как весь SQL-слой.
    public static string AdminDsn(NodeAddress master, string dbname, InstallSecrets secrets)
        => DatabaseProvisioner.BuildAdminDsn(master.Host, master.Ports.Pg, dbname, secrets);

    // libpq-conninfo для CREATE SUBSCRIPTION из dsn-ключа шарда (P2):
    // multi-host host=… port=… — семантический эквивалент HAProxy-входа
    // скриптов; user подменяется на bucket_mover, добавляется его пароль.
    // advertisedHost — как издатель виден ИЗ контейнера приёмника (single-host
    // стенды: host.docker.internal; null — адреса dsn-ключа как есть, прод).
    // Подмена ПОЭЛЕМЕНТНО: libpq требует соответствия числа host и port.
    // target_session_attrs=read-write ОБЯЗАТЕЛЕН (add-кластер, 2026-08-26):
    // без него libpq берёт первый доступный хост — replication-слот failover
    // создавался на стендбае источника («cannot enable failover … created on
    // the standby», 08P01); read-write и есть эквивалент HAProxy-входа, и
    // переподключение apply-worker'а после failover источника заново выбирает
    // писателя.
    public static string MoverConninfo(string shardDsn, InstallSecrets secrets, string? advertisedHost = null)
    {
        var dsn = UserRegex().Replace(shardDsn, "$1user=" + MoverRole);
        if (!UserRegex().IsMatch(dsn))
            dsn += " user=" + MoverRole;
        if (advertisedHost is { Length: > 0 })
            dsn = HostRegex().Replace(dsn, m =>
                (m.Value.StartsWith(' ') ? " " : "") + "host=" +
                string.Join(",", m.Value[(m.Value.IndexOf('=') + 1)..].Split(',').Select(_ => advertisedHost)));
        return dsn + " password=" + secrets.MoverPassword + " sslmode=require target_session_attrs=read-write";
    }

    // host=… пары key=value conninfo (замена хостов издателя на advertised).
    [GeneratedRegex(@"(^| )host=[^ ]*", RegexOptions.CultureInvariant)]
    private static partial Regex HostRegex();

    // Npgsql-DSN для SQL-проб роли bucket_mover (spec §6.1 M0, ревью №2):
    // конвертация той же libpq-строки — сплит по пробелам → пары key=value →
    // маппинг host→Host, port→Port, dbname→Database, user→Username (mover).
    // Разные порты нод Npgsql принимает ТОЛЬКО парами «Host=h1:p1,h2:p2» —
    // список в Port= отвергается («Couldn't set port», e2e-факт t01).
    // Target Session Attributes=read-write — Npgsql-эквивалент target_session_attrs
    // (та же причина, что у MoverConninfo): пробы издателя — про писателя
    // (wal_level/слоты/walsender'ы), первый хост может быть стендбаем. Значение
    // строго libpq-формой через дефис («ReadWrite» Npgsql отвергает — e2e-факт
    // add-кластера: «Couldn't set target session attributes»).
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
        parts.Add("SSL Mode=Require");
        parts.Add("Trust Server Certificate=true");
        parts.Add("Target Session Attributes=read-write");
        return string.Join(";", parts);
    }
}
