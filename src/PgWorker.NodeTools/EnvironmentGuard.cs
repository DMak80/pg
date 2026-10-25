namespace PgWorker.NodeTools;

// Контроль записи PID-файла: полный путь нормализуется и остаётся строго в
// разрешённом корне (CWE-22); симлинки не нормализуются здесь — контейнер
// получает значение только из фиксированного параметра CLI.
public static class EnvironmentGuard
{
    public static void EnsureFileUnderRoot(string path, string root, string kind)
    {
        var full = Path.GetFullPath(path);
        var normalizedRoot = Path.GetFullPath(root);
        if (full != normalizedRoot && !full.StartsWith(normalizedRoot + Path.DirectorySeparatorChar, StringComparison.Ordinal))
            throw new ArgumentException($"{kind} outside allowed root: {full}");
    }
}
