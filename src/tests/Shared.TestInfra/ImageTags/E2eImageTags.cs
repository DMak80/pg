using System.Security.Cryptography;
using System.Text;

namespace Shared.TestInfra;

/// <summary>Суффикс per-branch тегов e2e-образов (docs/e2e-isolation.md §9):
/// санитизированное имя ветки — [a-z0-9._-]{1,32}; git недоступен или HEAD
/// detached — fallback {хэш8}: первые 8 hex SHA256 абсолютного пути корня.
/// Одна ветка — один набор образов (git-инвариант «одна ветка — один checkout»).</summary>
public static class E2eImageTags
{
    public static string SuffixFor(string repoRoot)
    {
        var branch = SanitizeBranch(ReadBranch(repoRoot));
        return branch.Length > 0 ? branch : FallbackHash(repoRoot);
    }

    /// <summary>Ветка для лог-суффиксов: санитизированное имя или пустая строка
    /// (git недоступен / detached). Отличается от SuffixFor отсутствием hash8.</summary>
    public static string BranchFor(string repoRoot) => SanitizeBranch(ReadBranch(repoRoot));

    /// <summary>Санитизация имени ветки: lower-case, недопустимые символы → '-',
    /// трим конечных '-', максимум 32 символа.</summary>
    public static string SanitizeBranch(string raw)
    {
        var chars = raw.Trim().ToLowerInvariant()
            .Select(c => char.IsAsciiLetterOrDigit(c) || c is '.' or '-' or '_' ? c : '-')
            .ToArray();
        var branch = new string(chars).TrimEnd('-');
        return branch.Length > 32 ? branch[..32].TrimEnd('-') : branch;
    }

    public static string FallbackHash(string root)
        => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(root))).ToLowerInvariant()[..8];

    private static string ReadBranch(string root)
    {
        try
        {
            var psi = new System.Diagnostics.ProcessStartInfo("git")
            {
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true,
                WorkingDirectory = root,
            };
            psi.ArgumentList.Add("rev-parse");
            psi.ArgumentList.Add("--abbrev-ref");
            psi.ArgumentList.Add("HEAD");
            using var process = new System.Diagnostics.Process { StartInfo = psi };
            process.Start();
            if (!process.WaitForExit(5000) || process.ExitCode != 0)
                return "";
            var output = process.StandardOutput.ReadToEnd().Trim();
            // detached HEAD: abbrev-ref печатает буквально "HEAD" — ветки нет.
            return output.Length == 0 || output == "HEAD" ? "" : output;
        }
        catch
        {
            return "";
        }
    }
}
