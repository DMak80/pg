namespace PgWorker.Core.Templates;

/// <summary>
/// Общий хелпер REST-грани Patroni (arch/pgworker/01-placement.md §1/§5): константа REST-пары.
/// env-хеш механики упразднён t05 (rest-пара доставляется bootstrap-каналом,
/// не env — arch/pgworker/04-security.md); резолвор «эффективной пары» упразднён ETCD-FIRST
/// (arch/pgworker/processes/i-secret-rotator.md I): effective = ключ etcd rest_password (с R2-txn — NEW).
/// </summary>
public static class RestRotation
{
    /// <summary>Username basic-auth Patroni REST — константа (Patroni требует
    /// пару username+password в конфиге каждой ноды; отдельного user-ключа нет).</summary>
    public const string RestUsername = "patroni";
}
