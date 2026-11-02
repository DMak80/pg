namespace PgWorker.Core.Templates;

/// <summary>
/// Общий хелпер REST-грани Patroni (arch/14 §2.1/§5): константа REST-пары.
/// env-хеш механики упразднён t05 (rest-пара доставляется bootstrap-каналом,
/// не env — arch/14 §4); резолвор «эффективной пары» упразднён ETCD-FIRST
/// (arch/14 §5 I): effective = ключ etcd rest_password (с R2-txn — NEW).
/// </summary>
public static class RestRotation
{
    /// <summary>Username basic-auth Patroni REST — константа (Patroni требует
    /// пару username+password в конфиге каждой ноды; отдельного user-ключа нет).</summary>
    public const string RestUsername = "patroni";
}
