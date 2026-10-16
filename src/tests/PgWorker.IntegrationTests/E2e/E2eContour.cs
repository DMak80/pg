using System.Collections.Concurrent;
using PgWorker.IntegrationTests.Docker;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

/// <summary>
/// Двухфазный запуск E2E-сценария (контур «Вариант A», docs/e2e-isolation.md §1).
/// Точка входа — <see cref="OrchestrateCurrentTestAsync"/>: её вызывает
/// E2eEnvironment.StartAsync, когда факт исполняется НЕ в раннере (хост).
/// - ФАЗА 1+оркестрация (хост): DockerTrait-гейт → статические ассеты →
///   StartAsync (изолированная сеть + etcd + ownS3 + mc, без публикаций на
///   хост) → docker run раннера (pgw-e2e-runner:e2e, сеть контура, volume
///   артефактов + снапшотов, docker.sock, env контура) с фильтром «-class
///   FQN» — ВСЕ факты класса в одном раннере (скорость: docker run + прогрев
///   dotnet + discovery ~15-25 с на КЛАСС, а не на факт; канон изоляции —
///   «сценарный класс», у каждого сценария свой кластер с уникальным именем) →
///   exit code + docker logs → teardown (упавший: stop-режим) → исход:
///   успех — Assert.Skip (тела фактов на хосте не исполняются; результат —
///   в журнале раннера), фейл — ApplicationException.
/// - ТЕЛО СЦЕНАРИЯ (раннер-контейнер): PGW_E2E_INNER=1 → StartAsync возвращает
///   AttachFromEnv (подключение к контуру); тело против внутрисетевых адресов.
/// Координация фактов одного класса — словарь ClassRuns: первый факт запускает
/// фоновую задачу-оркестрацию (переживает факты: teardown доигрывает),
/// остальные ждут её исход и скипаются.
/// </summary>
public static class E2eContour
{
    /// <summary>Бюджет прогона раннера (docker run): сценарии с длинными фазами
    /// (restore recovery 600 с, mirror AC6, WalStaleSec) — десятки минут;
    /// зависший прогон умирает по бюджету с диагностикой, не висит вечно.</summary>
    private static readonly TimeSpan RunnerBudget = TimeSpan.FromMinutes(60);

    // Задачи-оркестрации по классу (первый факт класса создаёт, остальные ждут).
    private static readonly ConcurrentDictionary<string, Task<ClassRunOutcome>> ClassRuns = new();

    private sealed record ClassRunOutcome(bool Ok, string ArtifactsDir, string Error);

    /// <summary>
    /// Оркестрация КЛАССА текущего факта (только хост). Всегда завершается
    /// исключением: Assert.Skip — класс исполнен в раннере зелёно (тела фактов
    /// на хосте не исполняются); ApplicationException — фейл класса (с хвостом
    /// журнала раннера) или неполный teardown (изоляция — брак).
    /// </summary>
    public static async Task OrchestrateCurrentTestAsync(
        string slug, bool withS3Node, bool haEtcd, CancellationToken ct)
    {
        // Гейт docker-серии — на хосте (раннер получает PGW_TEST_DOCKER=1).
        DockerTrait.SkipIfUnavailable();
        var testCase = TestContext.Current.TestCase
            ?? throw new ApplicationException(
                "E2E-факт вне TestContext — оркестрация двухфазного контура невозможна");
        var classFqn = testCase.TestClassName
            ?? throw new ApplicationException("TestClassName не определён");

        // Фоновая задача: оркестрация переживает завершение фактов класса
        // (teardown контура обязан доиграть); токен — не токен факта.
        var run = ClassRuns.GetOrAdd(classFqn, _ => Task.Run(() =>
            RunClassAsync(classFqn, slug, withS3Node, haEtcd)));

        var outcome = await run.WaitAsync(ct);
        if (!outcome.Ok)
            throw new ApplicationException($"класс {classFqn}: {outcome.Error}");
        Assert.Skip($"класс исполнен в раннере (журнал: {outcome.ArtifactsDir}/runner.log)");
    }

    /// <summary>Оркестрация КЛАССА по явному имени (класс-фикстуры:
    /// TestContext.TestCase в InitializeAsync недоступен). Тот же исход.</summary>
    public static Task OrchestrateClassAsync(
        string classFqn, string slug, bool withS3Node, bool haEtcd, CancellationToken ct)
        => OrchestrateCurrentTestAsync(slug, withS3Node, haEtcd, ct);

    private static async Task<ClassRunOutcome> RunClassAsync(
        string classFqn, string slug, bool withS3Node, bool haEtcd)
    {
        try
        {
            var outcome = await OrchestrateCoreAsync(classFqn, slug, withS3Node, haEtcd);
            return outcome;
        }
        catch (Exception e)
        {
            // Исход класса-фейла: все факты класса краснеют с этой причиной.
            return new ClassRunOutcome(false, string.Empty, e.Message);
        }
    }

    private static async Task<ClassRunOutcome> OrchestrateCoreAsync(
        string fqn, string slug, bool withS3Node, bool haEtcd)
    {
        await E2eEnvironment.EnsureStaticAssetsAsync(CancellationToken.None);
        var env = await E2eEnvironment.StartEnvironmentAsync(slug, withS3Node, haEtcd, CancellationToken.None);
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
            // Все факты класса в одном раннере (группировка — скорость).
            runArgs.AddRange(["-noColor", "-noLogo", "-class", fqn]);

            _ = await E2eFixture.RunProcessAsync("docker", [.. runArgs], CancellationToken.None, RunnerBudget);
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
            runnerLog = await E2eFixture.RunDockerAsync(["logs", runnerName], CancellationToken.None);
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

        return new ClassRunOutcome(true, env.ArtifactsDir, string.Empty);
    }

    // Последние строки журнала — для исключения (полный — в runner.log).
    private static string Tail(string text, int lines)
    {
        var all = text.Split(
            ['\r', '\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        return string.Join("\n", all.TakeLast(lines));
    }
}
