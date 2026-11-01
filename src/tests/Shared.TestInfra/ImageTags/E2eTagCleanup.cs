using System.Globalization;

namespace Shared.TestInfra;

/// <summary>Авто-уборка per-branch e2e-тегов старше 24 ч (docs/e2e-isolation.md
/// §9). Отбор — чистая функция (юнит-тестируема без docker); удаление —
/// `docker rmi` по одному, ЛЮБОЙ отказ (в т.ч. «image is being used by running
/// container») игнорируется. NEVER-список (owns3:dev, minio/mc, etcd, *:dev и
/// любые теги без префикса e2e-) отсечён фильтром «тег начинается с e2e-».</summary>
public static class E2eTagCleanup
{
    public static readonly TimeSpan MaxAge = TimeSpan.FromHours(24);

    /// <summary>Чистая функция отбора. Вход — вывод
    /// `docker images --format "{{.Repository}}:{{.Tag}}|{{.CreatedAt}}"`;
    /// время в формате «2006-01-02 15:04:05 -0700 MST». Отбор: репозиторий ∈
    /// repos, тег начинается с "e2e-", возраст &gt; age от nowUtc, тег ≠ currentTag
    /// (страховка от перекоса часов), строка неразбираема — пропускается.</summary>
    public static IReadOnlyList<string> SelectStaleE2eTags(
        string dockerImagesOutput, IReadOnlyCollection<string> repos,
        string currentTag, TimeSpan age, DateTime nowUtc)
    {
        var stale = new List<string>();
        foreach (var raw in dockerImagesOutput.Split(
                     '\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var sep = raw.LastIndexOf('|');
            if (sep <= 0 || !TryParseDockerCreated(raw[(sep + 1)..].Trim(), out var createdUtc))
                continue;
            var image = raw[..sep];
            var colon = image.LastIndexOf(':');
            if (colon <= 0)
                continue;
            var repo = image[..colon];
            var tag = image[(colon + 1)..];
            if (!repos.Contains(repo))
                continue;
            if (!tag.StartsWith("e2e-", StringComparison.Ordinal))
                continue;
            if (tag == currentTag)
                continue;
            if (nowUtc - createdUtc > age)
                stale.Add(image);
        }
        return stale;
    }

    /// <summary>Docker-обёртка: читает список образов, удаляет отобранные
    /// теги; ошибки списка/удаления не роняют вызов (телеметрия [PHASE]).</summary>
    public static async Task CleanupAsync(
        IReadOnlyCollection<string> repos, string currentTag,
        Func<string, string[], CancellationToken, Task<string>> runProcessAsync,
        CancellationToken ct)
    {
        string output;
        try
        {
            output = (await runProcessAsync("docker",
                ["images", "--format", "{{.Repository}}:{{.Tag}}|{{.CreatedAt}}"], ct)).Trim();
        }
        catch (Exception e)
        {
            Console.WriteLine(
                $"[PHASE] e2e-static: cleanup skipped — docker images failed: {e.Message.Split('\n')[0]}");
            return;
        }

        var stale = SelectStaleE2eTags(output, repos, currentTag, MaxAge, DateTime.UtcNow);
        foreach (var image in stale)
        {
            try
            {
                await runProcessAsync("docker", ["rmi", image], ct);
            }
            catch
            {
                // отказ rmi (образ занят живым контейнером и пр.) — игнорируется
            }
        }

        if (stale.Count > 0)
            Console.WriteLine($"[PHASE] e2e-static: cleanup removed {stale.Count} stale e2e tag(s)");
    }

    private static bool TryParseDockerCreated(string raw, out DateTime utc)
    {
        utc = DateTime.MinValue;
        // Формат docker («yyyy-MM-dd HH:mm:ss +HHMM TZ»): смещение без
        // двоеточия и буквенная зона в конце; штатный DateTimeOffset.TryParse
        // этот формат НЕ разбирает — состав разбираем вручную.
        var parts = raw.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length != 4 || parts[2].Length != 5 || (parts[2][0] is not '+' and not '-'))
            return false;
        var offset = $"{parts[2][..3]}:{parts[2][3..]}";
        if (!DateTime.TryParseExact(
                $"{parts[0]} {parts[1]} {offset}", "yyyy-MM-dd HH:mm:ss zzz",
                CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsed))
            return false;
        utc = parsed.ToUniversalTime();
        return true;
    }
}
