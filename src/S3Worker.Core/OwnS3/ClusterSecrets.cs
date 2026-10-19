namespace S3Worker.Core.OwnS3;

/// <summary>Имена статических access keys per-cluster secrets (arch/23 §6):
/// backups — read-write (джобы/агенты бэкапов PgWorker), panel — read-only
/// (грань «Хранилище бэкапов» панели).</summary>
public static class ClusterAccessKeyNames
{
    public const string Backups = "backups";
    public const string Panel = "panel";
}

/// <summary>Access key с именем (модель JSON secrets-ключа arch/23 §2.1).
/// Name — не секрет (диагностика, сверка состава); Policy ∈
/// read-write|read-only.</summary>
public sealed record ClusterAccessKey(
    string Name, string AccessKey, string SecretKey, string Policy);

/// <summary>Клиентские креды кластера — значение ключа
/// /s3/clusters/&lt;C&gt;/secrets (arch/23 §2/§6): root-пара админ-грани +
/// статические access keys. Значения передаются ТОЛЬКО в env контейнеров
/// инстансов и в S3-клиенты процессов; в журнал/логи/API — никогда.</summary>
public sealed record ClusterSecrets(
    string RootUser,
    string RootPassword,
    IReadOnlyList<ClusterAccessKey> AccessKeys)
{
    /// <summary>Проекция в env-модель инстанса (OWNS3_*): имена ключей env
    /// не передаются — платформе ownS3 нужны только пары значений.</summary>
    public OwnS3NodeSecrets ToNodeSecrets()
        => new(RootUser, RootPassword,
            [.. AccessKeys.Select(k => new OwnS3AccessKey(k.AccessKey, k.SecretKey, k.Policy))]);
}
