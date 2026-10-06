using System.Diagnostics;
using PgWorker.IntegrationTests.E2eLoadGen;

// Диагностический нагрузочный генератор (t24, spec §5.1): K шумовых
// изолированных контуров на время D — управляемый «тяжёлый сосед» для
// E2E-серии. Телеметрия — в out-каталоге; teardown own-only при завершении
// по времени ИЛИ Ctrl-C.
try
{
    var opts = LoadGenOptions.Parse(args);
    Console.WriteLine(
        $"noise: старт k={opts.Count} profile={opts.Profile} d={opts.DurationMinutes}m out={opts.OutDir}");
    var contours = new List<NoiseContour>();
    using var cts = new CancellationTokenSource();
    Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };
    var sw = Stopwatch.StartNew();
    try
    {
        for (var i = 1; i <= opts.Count; i++)
        {
            contours.Add(await NoiseContour.CreateAsync(i, opts.Profile, opts.OutDir, cts.Token));
            Console.WriteLine($"noise: контур {i}/{opts.Count} поднят ({sw.Elapsed:hh\\:mm\\:ss})");
        }

        await Task.Delay(opts.Duration, cts.Token);
        Console.WriteLine($"noise: время истекло ({sw.Elapsed:hh\\:mm\\:ss}) — teardown");
    }
    finally
    {
        foreach (var contour in contours)
        {
            try
            {
                await contour.DisposeAsync();
            }
            catch (Exception e)
            {
                Console.Error.WriteLine($"noise: teardown контура неполный: {e.Message}");
            }
        }
    }

    Console.WriteLine($"noise: k={opts.Count} profile={opts.Profile} up={sw.Elapsed:hh\\:mm\\:ss} out={opts.OutDir}");
    return 0;
}
catch (ApplicationException e) // usage / невалидные аргументы / teardown-проблемы
{
    Console.Error.WriteLine(e.Message);
    return 2;
}
catch (OperationCanceledException)
{
    // Отмена по времени ИЛИ SIGINT/Ctrl-C (CancelKeyPress → cts.Cancel());
    // teardown контуров уже выполнен в finally — это подтверждение чистоты.
    Console.WriteLine("noise: отменён (SIGINT/Ctrl-C) — teardown выполнен в finally");
    return 0;
}
