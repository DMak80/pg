using Shared.Etcd.Client;

namespace EtcdWorker.Core;

/// <summary>Managed-член контура: номер узла из peer-URL-паттерна + исходный член.</summary>
public sealed record ManagedMember(int K, EtcdMember Member);

/// <summary>Разбор member list (arch/25 §2/§3.1): каноническая форма peer-URL
/// managed-узла — константа http://etcdw-node-<k>:2380 (по ней панель
/// классифицирует managed/external); чек стенда матчит по host-префиксу
/// "http://etcdw-node-" — намеренно шире канона (ловит managed-looking URL
/// с отклонившимся портом); сам парсер намеренно консервативен — берёт подстроку
/// "/etcdw-node-<k>:" с любым scheme/портом, это механика ДООВОДКИ состава
/// (член, отклонившийся от канонической адресации, воркер всё равно обслужит),
/// имя члена не дискриминант; прочие — external.</summary>
public static class EtcdContourMembers
{
    public const string NamePrefix = "etcdw-node-";

    public static (IReadOnlyList<ManagedMember> Managed, IReadOnlyList<EtcdMember> External) Build(
        IReadOnlyList<EtcdMember> members)
    {
        var managed = new List<ManagedMember>();
        var external = new List<EtcdMember>();
        foreach (var m in members)
        {
            var k = ParseK(m);
            if (k is { } number)
                managed.Add(new ManagedMember(number, m));
            else
                external.Add(m);
        }

        return (managed, external);
    }

    // Номер <k> из peer-URL ".../etcdw-node-<k>:..." (int; битовый — null → external).
    public static int? ParseK(EtcdMember member)
    {
        foreach (var url in member.PeerUrls)
        {
            var idx = url.IndexOf($"/{NamePrefix}", StringComparison.Ordinal);
            if (idx < 0)
                continue;
            var rest = url[(idx + 1 + NamePrefix.Length)..];
            var colon = rest.IndexOf(':');
            if (colon <= 0)
                continue;
            return int.TryParse(rest[..colon], out var k) && k > 0 ? k : null;
        }

        return null;
    }
}
