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

        // Доверие CA https S3-endpoint (t42, spec §5 fallback): PEM env материализуется
        // в файл (multi-host: файла хоста в контейнере нет); путь идёт в runtime-опции
        // BackupS3 — per-install CA доверяется фабрикой HttpClient (AWSSDK.Core env
        // AWS_CA_BUNDLE не читает, доказано E2E t42).
        string? s3CaBundlePath = null;
        if (options.S3CaPem is { Length: > 0 })
        {
            s3CaBundlePath = Path.Combine(Path.GetTempPath(), $"s3-ca-{Guid.NewGuid():N}.pem");
            await File.WriteAllTextAsync(s3CaBundlePath, options.S3CaPem + "\n", cts.Token);
        }

        // t36: пул-клиент из списков env (write — роль источника, read — все
        // ноды, порядок endpoints); пер-ЦЕЛЬНЫЙ таймаут — PutAttemptTimeout
        // (requestTimeout каждой цели: висящая нода не съедает перебор — AC4).
        await using var s3 = new PoolBackupS3(
            options.S3Endpoints, options.S3ReadEndpoints,
            options.S3AccessKey, options.S3SecretKey,
            options.S3Bucket, s3CaBundlePath,
            requestTimeout: WalReceiverCore.PutAttemptTimeout);

        return await WalReceiverCore.RunAsync(
            options,
            new NpgsqlXLogSource(options),
            new NpgsqlSlotPositionReader(options),
            s3,
            Console.Out,
            cts.Token);
    }
}
