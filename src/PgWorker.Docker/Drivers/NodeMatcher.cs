using PgWorker.Core.Model;
using PgWorker.Docker.Engine;

namespace PgWorker.Docker.Drivers;

/// <summary>Нода, опознанная docker-инспекцией (spec §3.1, arch/14 §5 J AD1):
/// Host — docker-хост находки, Object — имя контейнера ноды; Patroni может
/// прийти из сайдкара (env NODE_NAME), Doorman=0 при отсутствии биндинга.</summary>
public sealed record DiscoveredNode(string NodeName, string Host, string Object, int Pg, int Patroni, int Doorman)
{
    public NodeAddress ToAddress() => new(Host, new NodePorts(Pg, Patroni, Doorman), Object);
}

/// <summary>Чистый матчинг контейнер↔нода (spec §3.1): контейнер = нода при
/// hostname==имя ИЛИ alias содержит имя; сайдкар Patroni — env NODE_NAME==имя;
/// порты — public-биндинги 5432/8008/6432; неоднозначность → имя пропускается
/// (безопасный отказ; журналирование пропуска — задача AdoptionProcess).</summary>
public static class NodeMatcher
{
    public static IReadOnlyDictionary<string, DiscoveredNode> Match(
        string dockerHost,
        IEnumerable<(DockerContainer Container, DockerContainerInspect Inspect)> containers,
        IReadOnlyCollection<string> nodeNames)
    {
        var names = new HashSet<string>(nodeNames, StringComparer.Ordinal);
        var candidates = new Dictionary<string, List<(DockerContainer C, DockerContainerInspect I)>>();
        foreach (var item in containers)
        {
            // Один контейнер может зваться именем ноды по hostname И alias —
            // кандидат добавляется единожды (иначе ложная неоднозначность).
            foreach (var name in NamesOf(item.Inspect).Distinct())
            {
                if (!names.Contains(name))
                    continue;
                if (!candidates.TryGetValue(name, out var list))
                    candidates[name] = list = [];
                if (!list.Contains(item))
                    list.Add(item);
            }
        }

        var result = new Dictionary<string, DiscoveredNode>();
        foreach (var (name, list) in candidates)
        {
            var nodeContainers = list.Where(IsNode).ToList();
            if (nodeContainers.Count != 1)
                continue; // 0 = только сайдкар, >1 = неоднозначность → пропуск (spec §3.1)

            var (c, i) = nodeContainers[0];
            var patroni = PublicPort(i, 8008);
            if (patroni == 0)
            {
                // Patroni-сайдкар этой ноды (стендовые эмуляторы hc*, env NODE_NAME).
                patroni = list.Where(s => !IsNode(s) && HasEnv(s.I, "NODE_NAME", name))
                    .Select(s => PublicPort(s.I, 8008)).FirstOrDefault(p => p > 0);
            }

            result[name] = new DiscoveredNode(name, dockerHost, c.Names[0],
                PublicPort(i, 5432), patroni, PublicPort(i, 6432));
        }

        return result;

        static IEnumerable<string> NamesOf(DockerContainerInspect i)
        {
            yield return i.Hostname;
            foreach (var alias in i.Aliases)
                yield return alias;
            // Patroni-сайдкар ноды (env NODE_NAME=<имя>) — кандидат имени ноды
            // (стендовые эмуляторы hc*): его 8008-биндинг = patroni-порт ноды.
            foreach (var e in i.Env)
                if (e.StartsWith("NODE_NAME=", StringComparison.Ordinal))
                    yield return e["NODE_NAME=".Length..];
        }

        static bool IsNode((DockerContainer C, DockerContainerInspect I) item)
            => item.I.Env.All(e => !e.StartsWith("NODE_NAME=", StringComparison.Ordinal));

        static bool HasEnv(DockerContainerInspect i, string key, string value)
            => i.Env.Any(e => e == $"{key}={value}");

        static int PublicPort(DockerContainerInspect i, int containerPort)
            => i.Ports.FirstOrDefault(p => p.ContainerPort == containerPort) is { } map ? map.HostPort : 0;
    }
}
