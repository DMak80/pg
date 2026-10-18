using System.Diagnostics;
using PgWorker.IntegrationTests.E2e;

// Фаза подготовки E2E-статических ассетов — ОТДЕЛЬНЫЙ запуск ДО тестов,
// одним процессом (канон docs/e2e-launch.md): publish-выводы и docker-образы
// собираются здесь; тестовые процессы статику не строят — параллельные
// dotnet publish в общий каталог ломают друг друга. По успеху пишет штамп
// artifacts/e2e/.prepared — тесты сверяют его со свежестью исходников.
//
// Запуск: dotnet run --project src/tests/PgWorker.E2ePrepare -c Release
// Затем:  DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test … (серии,
//         можно параллельно — ститика уже готова и не пересобирается).

Console.WriteLine("[PHASE] e2e-prepare: building static assets (publish + docker images)…");
var sw = Stopwatch.StartNew();
using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

await E2eEnvironment.PrepareStaticAssetsAsync(cts.Token);
Console.WriteLine(
    $"[PHASE] e2e-prepare: done in {sw.Elapsed.TotalSeconds:F0} s — tests can be run (including in parallel)");
