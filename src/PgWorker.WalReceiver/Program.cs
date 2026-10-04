using PgWorker.Backups;

namespace PgWorker.WalReceiver;

/// <summary>Entrypoint WAL-приёмника (t27, arch/19 §3.1): парсинг env-контракта
/// (невалидный env → result-маркер + код 5 permanent), врезка ядра
/// WalReceiverCore.RunAsync с Npgsql-транспортом и BackupS3. Отмена — по
/// SIGTERM/SIGINT (супервиз воркера); недоставленное удерживается слотом.</summary>
public static class Program
{
    public static async Task<int> Main()
    {
        var options = WalReceiverEnv.Parse(Environment.GetEnvironmentVariable, out var errors);
        if (options is null)
        {
            Console.Out.WriteLine(WalReceiverMarkers.Result(false, string.Join("; ", errors), null));
            return 5;
        }

        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, eventArgs) =>
        {
            eventArgs.Cancel = true; // чистая отмена → result-маркер с кодом 0
            cts.Cancel();
        };
        AppDomain.CurrentDomain.ProcessExit += (_, _) => cts.Cancel();

        // BackupS3 из S3-полей приёмника (реюз математики/клиента Backups).
        var s3Options = new BackupsRuntimeOptions(
            S3Endpoint: options.S3Endpoint,
            S3Region: options.S3Region,
            S3Bucket: options.S3Bucket,
            S3AccessKey: options.S3AccessKey,
            S3SecretKey: options.S3SecretKey,
            S3PathStyle: options.S3PathStyle);
        await using var s3 = new BackupS3(s3Options);

        return await WalReceiverCore.RunAsync(
            options,
            new NpgsqlXLogSource(options),
            new NpgsqlSlotPositionReader(options),
            s3,
            Console.Out,
            cts.Token);
    }
}
