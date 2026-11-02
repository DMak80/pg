using System.Text.RegularExpressions;

namespace PgWorker.PgNode;

/// <summary>KEY=VALUE-строка для source-able env-файла tmpfs (spec §3.5):
/// значение алфавита [A-Za-z0-9._-] пишется как есть; прочее — sh-экранирование
/// одинарными кавычками (апостроф → '\''). Per-install пароли гарантированно
/// DSN-safe (InstallSecretsGuard воркера) — экранирование защитный слой.</summary>
public static class EnvironmentFile
{
    private static readonly Regex ShSafe = new("^[A-Za-z0-9._-]*$",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);

    /// <summary>Алфавит-чек значения секрета (MR-#23 fail-fast на агенте:
    /// тот же алфавит, что у воркера — пароль вне [A-Za-z0-9._-] отвергается
    /// рендером агента, не молча пишется).</summary>
    public static bool IsAlphabetSafe(string value) => ShSafe.IsMatch(value);

    public static string GenerateEnvironmentVariable(string key, string value)
        => ShSafe.IsMatch(value)
            ? $"{key}={value}"
            : $"{key}='{value.Replace("'", "'\\''")}'";
}
