using PgWorker.IntegrationTests.Docker;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

/// <summary>
/// Двухфазный запуск E2E-сценария (контур «Вариант A», docs/e2e-isolation.md §1).
/// Точка входа — <see cref="OrchestrateCurrentTestAsync"/>: её вызывает
/// E2eEnvironment.StartAsync, когда факт исполняется НЕ в раннере (хост).
/// - ФАЗА 1+оркестрация (хост): DockerTrait-гейт → статические ассеты →
///   StartAsync (изолированная сеть + etcd + ownS3, без публикаций на хост) →
///   docker run раннера (pgw-e2e-runner:e2e, сеть контура, volume артефактов +
///   снапшотов, docker.sock, env контура) с фильтром ровно ТЕКУЩЕГО факта
///   (TestContext.TestCase) → exit code + docker logs → teardown (упавший:
///   stop-режим) → исход: успех — Assert.Skip (тело факта на хосте не
///   исполняется; результат — в журнале раннера), фейл — ApplicationException.
/// - ТЕЛО СЦЕНАРИЯ (раннер-контейнер): PGW_E2E_INNER=1 → StartAsync возвращает
///   AttachFromEnv (подключение к контуру); тело против внутрисетевых адресов.
/// Граница запуска — один сценарный метод (не шире одного класса).
/// </summary>
public static class E2eContour
{
    /// <summary>Бюджет прогона раннера (docker run): сценарии с длинными фазами
    /// (restore recovery 600 с, mirror AC6, WalStaleSec) — десятки минут;
    /// зависший прогон умирает по бюджету с диагностикой, не висит вечно.</summary>
    private static readonly TimeSpan RunnerBudget = TimeSpan.FromMinutes(60);

    /// <summary>
    /// Оркестрация текущего факта в двухфазном контуре (только хост): поднять
    /// окружение, прогнать в раннер-контейнере ровно этот факт, снять результат.
    /// Всегда завершается исключением: Assert.Skip — факт исполнен в раннере
    /// зелёно; ApplicationException — фейл (с хвостом журнала раннера) или
    /// неполный teardown (изоляция — брак).
    /// </summary>
    public static async Task OrchestrateCurrentTestAsync(
        string slug, bool withS3Node, bool haEtcd, CancellationToken ct)
    {
        // Гейт docker-серии — на хосте (раннер получает PGW_TEST_DOCKER=1).
        DockerTrait.SkipIfUnavailable();
        var testCase = TestContext.Current.TestCase
            ?? throw new ApplicationException(
                "E2E-факт вне TestContext — оркестрация двухфазного контура невозможна");
        // TestClassName — ПОЛНОЕ имя типа (namespace+class).
        var fqn = $"{testCase.TestClassName}.{testCase.TestMethodName}";

        await E2eEnvironment.EnsureStaticAssetsAsync(ct);
        var env = await E2eEnvironment.StartEnvironmentAsync(slug, withS3Node, haEtcd, ct);
        var runnerName = $"pgw-er-{env.RunId}";
        Console.WriteLine($"[PHASE] e2e-contour {slug}: запуск раннера {runnerName} ({fqn})…");

        // Блокирующий run: exit code контейнера = исход факта. RunProcessAsync
        // при exit≠0 раннера бросает ApplicationException («… → N: …») — это
        // УПАВШИЙ ТЕСТ (контейнер завершился сам, не трогаем — stop-режим
        // teardown оставит его для разбора). Прочие исключения (бюджет,
        // daemon) — раннер мог остаться жив: добиваем rm -f.
        Exception? failure = null;
        try
        {
            var runArgs = new List<string>
            {
                "run", "--name", runnerName,
                "--network", env.NetName,
                "-v", $"{env.ArtifactsDir}:/artifacts",
                "-v", $"{env.SnapshotsHostDir}:/snaps",
                "-v", "/var/run/docker.sock:/var/run/docker.sock",
            };
            foreach (var (key, value) in env.RunnerEnv())
                runArgs.AddRange(["-e", $"{key}={value}"]);
            runArgs.Add(E2eEnvironment.RunnerImage);
            runArgs.AddRange(["-noColor", "-noLogo", "-method", fqn]);

            _ = await E2eFixture.RunProcessAsync("docker", [.. runArgs], ct, RunnerBudget);
        }
        catch (ApplicationException e) when (e.Message.Contains(" → ", StringComparison.Ordinal))
        {
            failure = e;
        }
        catch (Exception e)
        {
            failure = e;
            try
            {
                await E2eFixture.RunDockerAsync(["rm", "-f", runnerName], CancellationToken.None);
            }
            catch
            {
                // лучшими усилиями: teardown/assert чистоты увидит остаток
            }
        }

        // Лог раннера — в артефакты (полный тестовый журнал прогона) ДО
        // teardown'а (стоп-режим сохраняет контейнер, но лог надёжнее в файлах).
        string runnerLog;
        try
        {
            runnerLog = await E2eFixture.RunDockerAsync(["logs", runnerName], ct);
            await File.WriteAllTextAsync(Path.Combine(env.ArtifactsDir, "runner.log"), runnerLog);
        }
        catch (Exception e)
        {
            runnerLog = $"<runner logs unavailable: {e.Message}>";
        }

        // Fail-safe ложнозелёного прогона: exit 0 при «Total: 0» — фильтр
        // раннера не совпал ни с одним фактом (оркестрация сломана), прогон
        // НИЧЕГО не проверил. Падаем, а не докладываем успех.
        if (failure is null && runnerLog.Contains("Total: 0", StringComparison.Ordinal))
            failure = new ApplicationException(
                $"раннер не нашёл ни одного теста по фильтру «{fqn}» — оркестрация сломана");

        if (failure is not null)
            env.MarkFailed(); // teardown в stop-режиме: контейнеры стоят для разбора

        Exception? teardownFailure = null;
        try
        {
            await env.DisposeAsync();
        }
        catch (Exception e)
        {
            teardownFailure = e;
        }

        if (teardownFailure is not null)
            throw new ApplicationException(
                $"e2e[{slug}]: teardown окружения неполный (изоляция — брак, docs/e2e-isolation.md §5):"
                + $" {teardownFailure.Message}", teardownFailure);

        if (failure is not null)
            throw new ApplicationException(
                $"e2e[{slug}]: раннер завершился с ошибкой ({fqn}); хвост журнала:\n"
                + Tail(runnerLog, 60)
                + $"\n(полный журнал и телеметрия: {env.ArtifactsDir})", failure);

        // Зелёный исход: тело факта на хосте НЕ исполняется — Skip прерывает
        // факт; реальный результат сценария — в журнале раннера.
        Assert.Skip($"сценарий выполнен в раннере {runnerName} (журнал: {env.ArtifactsDir}/runner.log)");
    }

    // Последние строки журнала — для исключения (полный — в runner.log).
    private static string Tail(string text, int lines)
    {
        var all = text.Split(
            ['\r', '\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        return string.Join("\n", all.TakeLast(lines));
    }
}
