namespace PgWorker.WalReceiver;

/// <summary>Entrypoint WAL-приёмника (t27, arch/19 §3.1): парсинг env-контракта,
/// невалидный env → result-маркер + код 5 (permanent). Врезка ядра
/// WalReceiverCore.RunAsync — Task 5 плана t27.</summary>
public static class Program
{
    public static int Main()
    {
        var options = WalReceiverEnv.Parse(Environment.GetEnvironmentVariable, out var errors);
        if (options is null)
        {
            Console.Out.WriteLine(WalReceiverMarkers.Result(false, string.Join("; ", errors), null));
            return 5;
        }

        // Заглушка до Task 5: env валиден — ядро ещё не врезано.
        Console.Out.WriteLine(WalReceiverMarkers.Starting());
        Console.Out.WriteLine(WalReceiverMarkers.Result(true, null, null));
        return 0;
    }
}
