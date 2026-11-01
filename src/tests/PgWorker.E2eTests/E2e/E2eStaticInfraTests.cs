using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using FluentAssertions;
using Shared.TestInfra;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

/// <summary>Юнит-факты статической E2E-инфраструктуры (без docker):
/// резолв per-branch суффикса тега, атомарность штампа подготовки, чистая
/// функция отбора авто-уборки тегов, same-process-ожидание лока подготовки
/// (docs/e2e-isolation.md §9, spec §3.1-3.2).</summary>
public sealed class E2eStaticInfraTests
{
    [Fact]
    public void SanitizeBranch_LowercaseAndIllegalCharsReplaced()
    {
        // Arrange: имя ветки с заглавными и недопустимыми символами (пробел, /).
        var raw = "Fix_T90/E2E Session";

        // Act: санитизация.
        var sanitized = E2eImageTags.SanitizeBranch(raw);

        // Assert: lower-case, пробел и / заменены на '-', допустимое _ сохранено.
        sanitized.Should().Be("fix_t90-e2e-session");
    }

    [Fact]
    public void SanitizeBranch_LongBranchTruncatedTo32()
    {
        // Arrange: имя ветки из 40 символов 'a'.
        var raw = new string('a', 40);

        // Act: санитизация с лимитом 32.
        var sanitized = E2eImageTags.SanitizeBranch(raw);

        // Assert: строка обрезана до 32 'a'.
        sanitized.Should().Be(new string('a', 32));
    }

    [Fact]
    public void FallbackHash_EightHexOfSha256()
    {
        // Arrange: корень вне git (хэш считаем тем же кодом, что и реализация).
        const string root = "/tmp/x";

        // Act: fallback-хэш реализации vs SHA256(UTF8(root)) первые 8 hex.
        var actual = E2eImageTags.FallbackHash(root);
        var expected = Convert.ToHexString(
            SHA256.HashData(Encoding.UTF8.GetBytes(root))).ToLowerInvariant()[..8];

        // Assert: первые 8 hex SHA256 пути.
        actual.Should().Be(expected);
        actual.Should().HaveLength(8);
    }

    [Fact]
    public void SuffixFor_NonGitDir_FallsBackToPathHash()
    {
        // Arrange: временный каталог вне git-репозитория.
        var dir = Path.Combine(Path.GetTempPath(), $"pgw-tagtest-{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        try
        {
            // Act: суффикс по каталогу без git.
            var suffix = E2eImageTags.SuffixFor(dir);

            // Assert: fallback — хэш8 пути (ветки нет).
            suffix.Should().Be(E2eImageTags.FallbackHash(dir));
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void StampAtomic_WriteAndOverwrite_ParseableJson()
    {
        // Arrange: временный каталог и путь штампа.
        var dir = Path.Combine(Path.GetTempPath(), $"pgw-stamptest-{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        var path = Path.Combine(dir, ".prepared");
        try
        {
            // Act: первая запись, затем перезапись другим значением.
            E2eEnvironment.WriteStampAtomic(path, 1700000000000);
            using (var doc = JsonDocument.Parse(File.ReadAllText(path)))
            {
                // Assert: JSON {"stampMs"} читается.
                doc.RootElement.GetProperty("stampMs").GetInt64().Should().Be(1700000000000);
            }

            E2eEnvironment.WriteStampAtomic(path, 1700000000001);

            // Assert: перезаписано, временного файла не осталось.
            using var after = JsonDocument.Parse(File.ReadAllText(path));
            after.RootElement.GetProperty("stampMs").GetInt64().Should().Be(1700000000001);
            File.Exists(path + ".tmp").Should().BeFalse();
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task StampAtomic_ConcurrentWrites_NoCorruption()
    {
        // Arrange: путь штампа и 5 параллельных писателей.
        var dir = Path.Combine(Path.GetTempPath(), $"pgw-stamptest-{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        var path = Path.Combine(dir, ".prepared");
        try
        {
            // Act: конкурентные записи 0..4.
            await Task.WhenAll(Enumerable.Range(0, 5)
                .Select(i => Task.Run(() => E2eEnvironment.WriteStampAtomic(path, i))));

            // Assert: файл парсится, значение — одно из записанных.
            using var doc = JsonDocument.Parse(File.ReadAllText(path));
            doc.RootElement.GetProperty("stampMs").GetInt64()
                .Should().BeOneOf(0L, 1L, 2L, 3L, 4L);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void CleanupSelector_SelectsOnlyStaleOwnE2eTags()
    {
        // Arrange: вывод docker images — stale/свежая/чужой repo/не-e2e/битая
        // строка; currentTag "e2e-current" во входе НЕ встречается: свежая
        // строка (nowUtc минус 30 минут) отсекается возрастом (а не тождеством
        // тега), stale попадает в отбор.
        var nowUtc = new DateTime(2030, 1, 2, 0, 0, 0, DateTimeKind.Utc);
        var output = string.Join('\n',
            "pgworker:e2e-main|2020-01-01 00:00:00 +0000 UTC",
            "pgworker:e2e-main|2030-01-01 23:30:00 +0000 UTC",
            "other:e2e-old|2020-01-01 00:00:00 +0000 UTC",
            "pgworker:dev|2020-01-01 00:00:00 +0000 UTC",
            "pgworker:e2e-main|не-дата");

        // Act: отбор устаревших тегов репозитория pgworker.
        var stale = E2eTagCleanup.SelectStaleE2eTags(
            output, ["pgworker"], "e2e-current", TimeSpan.FromHours(24), nowUtc);

        // Assert: ровно одна stale-строка своего репозитория с тегом e2e-.
        stale.Should().BeEquivalentTo(["pgworker:e2e-main"]);
    }

    [Fact]
    public void CleanupSelector_ExcludesCurrentAndNeverTags()
    {
        // Arrange: тот же вход плюс строка текущего тега e2e-main2.
        var nowUtc = new DateTime(2030, 1, 2, 0, 0, 0, DateTimeKind.Utc);
        var output = string.Join('\n',
            "pgworker:e2e-main|2020-01-01 00:00:00 +0000 UTC",
            "pgworker:e2e-main2|2020-01-01 00:00:00 +0000 UTC",
            "other:e2e-old|2020-01-01 00:00:00 +0000 UTC",
            "pgworker:dev|2020-01-01 00:00:00 +0000 UTC",
            "pgworker:e2e-main|не-дата");

        // Act: отбор с currentTag = e2e-main2.
        var stale = E2eTagCleanup.SelectStaleE2eTags(
            output, ["pgworker"], "e2e-main2", TimeSpan.FromHours(24), nowUtc);

        // Assert: текущий тег исключён (страховка от перекоса часов),
        // NEVER-теги (без префикса e2e-) и чужие репозитории не отобраны.
        stale.Should().BeEquivalentTo(["pgworker:e2e-main"]);
        stale.Should().NotContain("pgworker:e2e-main2");
        stale.Should().NotContain(t => t.Contains(":dev", StringComparison.Ordinal));
    }

    [Fact]
    public async Task PrepareLock_SecondAcquirerWaitsForHolder()
    {
        // Same-process-ожидание детерминировано — .NET enforce'ит FileShare.None
        // на Unix через flock (конфликт действует и между потоками одного
        // процесса); кросс-процессную сериализацию ДВУХ процессов
        // PgWorker.E2ePrepare (spec §6.8) проверяет пункт 4 «Проверки» шага 4.
        // Arrange: временный lockPath; держатель A на 3 с, претендент B —
        // стартует через 500 мс.
        var dir = Path.Combine(Path.GetTempPath(), $"pgw-locktest-{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        var path = Path.Combine(dir, ".prepare.lock");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var ct = cts.Token;
        try
        {
            var holder = Task.Run(async () =>
            {
                var f = await E2ePrepareLock.AcquireAsync(path, ct);
                await Task.Delay(3000, ct);
                f.Dispose();
            }, ct);

            // Act: B ждёт слота и захватывает после освобождения.
            await Task.Delay(500, ct);
            var sw = Stopwatch.StartNew();
            var acquired = await E2ePrepareLock.AcquireAsync(path, ct);
            sw.Stop();

            // Assert: B получил лок, ждал ≥ 2500 мс; содержимое файла читаемо
            // после Dispose (FileShare.None держателя делает файл нечитаемым).
            acquired.Should().NotBeNull();
            sw.ElapsedMilliseconds.Should().BeGreaterThanOrEqualTo(2500);
            acquired.Dispose();
            var content = await File.ReadAllTextAsync(path, ct);
            content.Should().StartWith("pid=");
            await holder;
        }
        finally
        {
            cts.Cancel();
        }
    }
}
