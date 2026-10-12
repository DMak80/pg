using System.Text;

namespace S3Worker.Provisioning;

/// <summary>Статическая access key профиля (arch/23 §6): read-write для записи
/// бэкапов, read-only для панели (owns3/05 §6).</summary>
public sealed record OwnS3AccessKey(string AccessKey, string SecretKey, string Policy);

/// <summary>Креды ноды профиля: root-пара + опциональные access keys.
/// Значения НЕ попадают в etcd/журнал/лог/метрики — только в env контейнера
/// ноды (ToContainerEnv).</summary>
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

/// <summary>
/// Читатель per-install env-профилей кредов нод (arch/23 §6, arch/24 §4):
/// S3W_NODE_SECRETS__&lt;PROFILE&gt;__{ROOT_USER,ROOT_PASSWORD,ACCESS_KEYS__&lt;i&gt;__{ACCESSKEY,SECRETKEY,POLICY}}.
/// Диагностики — ТОЛЬКО имена профилей/переменных, никогда значения (spec §8);
/// полупары — ValidateEnvPairs (fail-fast старта), неполный профиль —
/// TryGet false (permanent-отказ P1 provisioning).
/// </summary>
public sealed class SecretProfileStore(Func<string?, string?>? getenv = null, IEnumerable<string>? envNames = null)
{
    private readonly Func<string?, string?> _getenv = getenv ?? (name
        => name is null ? null : Environment.GetEnvironmentVariable(name));

    // Имена env-переменных: инъекция для юнит-тестов, иначе процесс.
    private readonly Func<IReadOnlyList<string>> _envNames = envNames is null
        ? () => [.. Environment.GetEnvironmentVariables().Keys.Cast<string>()]
        : (() => [.. envNames]);

    private const string Prefix = "S3W_NODE_SECRETS__";

    // Перечень имён профилей: переменные формата S3W_NODE_SECRETS__<PROFILE>__*
    // — уникальные имена вторым сегментом (для 422-перечня — только имена).
    public IReadOnlyList<string> ProfileNames
    {
        get
        {
            var names = new SortedSet<string>(StringComparer.Ordinal);
            foreach (var name in _envNames())
            {
                if (!name.StartsWith(Prefix, StringComparison.Ordinal))
                    continue;
                var rest = name[Prefix.Length..];
                var separator = rest.IndexOf("__", StringComparison.Ordinal);
                if (separator > 0)
                    names.Add(rest[..separator]);
            }

            return [.. names];
        }
    }

    /// <summary>
    /// Чтение профиля: root-пара обязательна; access keys опциональны, но каждый
    /// ключ — полный (ACCESSKEY+SECRETKEY+POLICY, policy ∈ read-write|read-only).
    /// false → diagnostic (имена, без значений).
    /// </summary>
    public bool TryGet(string profile, out OwnS3NodeSecrets? secrets, out string? diagnostic)
    {
        secrets = null;
        diagnostic = null;

        var rootUser = _getenv($"{Prefix}{profile}__ROOT_USER");
        var rootPassword = _getenv($"{Prefix}{profile}__ROOT_PASSWORD");
        if (string.IsNullOrEmpty(rootUser) || string.IsNullOrEmpty(rootPassword))
        {
            diagnostic = $"профиль {profile} не найден или неполон (нужны ROOT_USER/ROOT_PASSWORD); доступны: {string.Join(", ", ProfileNames)}";
            return false;
        }

        var keys = new List<OwnS3AccessKey>();
        for (var i = 0; ; i++)
        {
            var accessKey = _getenv($"{Prefix}{profile}__ACCESS_KEYS__{i}__ACCESSKEY");
            var secretKey = _getenv($"{Prefix}{profile}__ACCESS_KEYS__{i}__SECRETKEY");
            var policy = _getenv($"{Prefix}{profile}__ACCESS_KEYS__{i}__POLICY");

            // Ключей нет вовсе — валидно (ключи опциональны).
            if (accessKey is null && secretKey is null && policy is null)
                break;

            if (string.IsNullOrEmpty(accessKey) || string.IsNullOrEmpty(secretKey)
                || (policy != "read-write" && policy != "read-only"))
            {
                diagnostic = $"профиль {profile}: неполный access key #{i} (нужны ACCESSKEY, SECRETKEY и POLICY=read-write|read-only)";
                return false;
            }

            keys.Add(new OwnS3AccessKey(accessKey, secretKey, policy));
        }

        secrets = new OwnS3NodeSecrets(rootUser, rootPassword, keys);
        return true;
    }

    /// <summary>
    /// Полупары (есть одна переменная пары, второй нет) — перечень имён профилей
    /// для fail-fast старта воркера (arch/24 §8 «неполные секретные пары»).
    /// </summary>
    public IReadOnlyList<string> ValidateEnvPairs()
    {
        var broken = new SortedSet<string>(StringComparer.Ordinal);
        var variables = _envNames()
            .Where(n => n.StartsWith(Prefix, StringComparison.Ordinal))
            .ToHashSet(StringComparer.Ordinal);

        bool Has(string name) => variables.Contains(name);

        // Группировка по профилю: root-пара (2) и access-key-тройки (3) —
        // любая частичная группа = полупара/неполный.
        foreach (var profile in ProfileNames)
        {
            var rootUser = Has($"{Prefix}{profile}__ROOT_USER");
            var rootPassword = Has($"{Prefix}{profile}__ROOT_PASSWORD");
            if (rootUser != rootPassword)
                broken.Add(profile);

            for (var i = 0; i < 16; i++)
            {
                var triple = new[]
                {
                    Has($"{Prefix}{profile}__ACCESS_KEYS__{i}__ACCESSKEY"),
                    Has($"{Prefix}{profile}__ACCESS_KEYS__{i}__SECRETKEY"),
                    Has($"{Prefix}{profile}__ACCESS_KEYS__{i}__POLICY"),
                };
                if (triple.Any(t => t) && !triple.All(t => t))
                    broken.Add(profile);
            }
        }

        return [.. broken];
    }

    // Диагностика без значений: строка профилей.
    internal static string Describe(IReadOnlyList<string> profiles)
    {
        var builder = new StringBuilder("доступные профили: ");
        builder.Append(string.Join(", ", profiles));
        return builder.ToString();
    }
}
