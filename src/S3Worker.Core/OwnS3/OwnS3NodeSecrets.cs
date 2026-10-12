namespace S3Worker.Core.OwnS3;

/// <summary>Статическая access key профиля (arch/23 §6): read-write для записи
/// бэкапов, read-only для панели (owns3/05 §6).</summary>
public sealed record OwnS3AccessKey(string AccessKey, string SecretKey, string Policy);

/// <summary>Креды ноды профиля: root-пара + опциональные access keys.
/// Значения НЕ попадают в etcd/журнал/лог/метрики — только в env контейнера
/// ноды (ToContainerEnv) и в S3-клиенты процессов. Модель — в Core: потребители
/// и processes (Provisioning), и клиент ноды (IOwnS3NodeClient).</summary>
public sealed record OwnS3NodeSecrets(
    string RootUser,
    string RootPassword,
    IReadOnlyList<OwnS3AccessKey> AccessKeys)
{
    // env ноды (arch/23 §6): прецедент формата OWNS3_ACCESS_KEYS__<i>__*
    // (owns3/05 §2, сервис owns3 deploy-compose).
    public IReadOnlyDictionary<string, string> ToContainerEnv()
    {
        var env = new Dictionary<string, string>
        {
            ["OWNS3_ROOT_USER"] = RootUser,
            ["OWNS3_ROOT_PASSWORD"] = RootPassword,
        };
        for (var i = 0; i < AccessKeys.Count; i++)
        {
            env[$"OWNS3_ACCESS_KEYS__{i}__ACCESSKEY"] = AccessKeys[i].AccessKey;
            env[$"OWNS3_ACCESS_KEYS__{i}__SECRETKEY"] = AccessKeys[i].SecretKey;
            env[$"OWNS3_ACCESS_KEYS__{i}__POLICY"] = AccessKeys[i].Policy;
        }

        return env;
    }
}
