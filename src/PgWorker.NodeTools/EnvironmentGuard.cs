namespace PgWorker.NodeTools;

// Контроль CLI-путей: полный путь нормализуется и остаётся строго в
// разрешённом корне (CWE-22); симлинки не нормализуются здесь — контейнер
// получает значение только из фиксированного параметра CLI.
public static class EnvironmentGuard
{
    public static void EnsureFileUnderRoot(string path, string root, string kind)
        => EnsureFileUnderRoots(path, [root], kind);

    // Мульти-корневой вариант: путь обязан попасть хотя бы в один корень
    // (прод и тестовые/стендовые расположения одного и того же файла).
    public static void EnsureFileUnderRoots(string path, IReadOnlyList<string> roots, string kind)
    {
        var full = Path.GetFullPath(path);
        foreach (var root in roots)
        {
            var normalizedRoot = Path.GetFullPath(root);
            if (full == normalizedRoot
                || full.StartsWith(normalizedRoot + Path.DirectorySeparatorChar, StringComparison.Ordinal))
                return;
        }

        throw new ArgumentException(
            $"{kind} outside allowed roots [{string.Join(", ", roots)}]: {full}");
    }
}
