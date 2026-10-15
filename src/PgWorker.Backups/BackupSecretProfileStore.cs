namespace PgWorker.Backups;

/// <summary>
/// Читатель per-install env-профилей S3-кредов пулов (t36, arch/19 §7):
/// PGW_BACKUP_S3_PROFILES__&lt;PROFILE&gt;__{ACCESS_KEY,SECRET_KEY} — read-write
/// ключи профилей (пул ссылается профилем config.secret_profile — arch/23 §6).
/// Копия механики SecretProfileStore S3Worker (инъекция getenv/envNames для
/// юнит-тестов); диагностики — ТОЛЬКО имена, никогда значения (spec §8);
/// полупары — ValidateEnvPairs (fail-fast старта), неполный профиль —
/// TryGet false (transient-диагностика резолвера, arch/19 §4).
/// </summary>
public sealed class BackupSecretProfileStore(
    Func<string?, string?>? getenv = null, IEnumerable<string>? envNames = null)
{
    private readonly Func<string?, string?> _getenv = getenv ?? (name
        => name is null ? null : Environment.GetEnvironmentVariable(name));

    // Имена env-переменных: инъекция для юнит-тестов, иначе процесс.
    private readonly Func<IReadOnlyList<string>> _envNames = envNames is null
        ? () => [.. Environment.GetEnvironmentVariables().Keys.Cast<string>()]
        : (() => [.. envNames]);

    private const string Prefix = "PGW_BACKUP_S3_PROFILES__";

    /// <summary>Имена профилей: переменные формата
    /// PGW_BACKUP_S3_PROFILES__&lt;PROFILE&gt;__* — уникальные имена вторым
    /// сегментом (до первого «__»); Ordinal-сортировка — детерминизм.</summary>
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

    /// <summary>Чтение профиля: пара ACCESS_KEY+SECRET_KEY обязательна.
    /// false → diagnostic (только имена профиля/переменных, без значений).</summary>
    public bool TryGet(string profile, out string? accessKey, out string? secretKey, out string? diagnostic)
    {
        accessKey = null;
        secretKey = null;
        diagnostic = null;

        accessKey = _getenv($"{Prefix}{profile}__ACCESS_KEY");
        secretKey = _getenv($"{Prefix}{profile}__SECRET_KEY");
        if (string.IsNullOrEmpty(accessKey) || string.IsNullOrEmpty(secretKey))
        {
            diagnostic = $"профиль {profile} не найден или неполон (нужны ACCESS_KEY/SECRET_KEY); доступны: {string.Join(", ", ProfileNames)}";
            accessKey = null;
            secretKey = null;
            return false;
        }

        return true;
    }

    /// <summary>Полупары (есть одна переменная пары, второй нет) — перечень имён
    /// профилей для fail-fast старта воркера (arch/19 §9).</summary>
    public IReadOnlyList<string> ValidateEnvPairs()
    {
        var broken = new SortedSet<string>(StringComparer.Ordinal);
        var variables = _envNames()
            .Where(n => n.StartsWith(Prefix, StringComparison.Ordinal))
            .ToHashSet(StringComparer.Ordinal);

        foreach (var profile in ProfileNames)
        {
            var accessKey = variables.Contains($"{Prefix}{profile}__ACCESS_KEY");
            var secretKey = variables.Contains($"{Prefix}{profile}__SECRET_KEY");
            if (accessKey != secretKey)
                broken.Add(profile);
        }

        return [.. broken];
    }
}
