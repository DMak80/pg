using System.Text;
using Shared.Core;

namespace PgWorker.Backups;

/// <summary>Чтение `.history`-объектов префикса `wal/` (TLI → текст): вход
/// строгой LSN-валидации TLI-переходов (arch/backups/processes/b-wal-control
/// §1, f-restore §2). Ошибка чтения TLI — пропуск: переход проверится
/// эвристикой номеров; содержимое мало и аддитивно.</summary>
public static class WalHistoryObjects
{
    public static async Task<Result<IReadOnlyDictionary<uint, string>>> ReadAsync(
        IBackupS3 s3, string cluster, string shard,
        IEnumerable<string> objectNames, CancellationToken ct)
    {
        var histories = new Dictionary<uint, string>();
        foreach (var name in objectNames)
        {
            if (WalFileName.TryParseHistory(name) is not { } tli || histories.ContainsKey(tli))
                continue;
            var got = await s3.GetObjectBytesAsync(cluster, shard, $"wal/{tli:x8}.history", ct);
            if (got.IsSuccess)
                histories[tli] = Encoding.UTF8.GetString(got.Value.Data);
        }

        return Result<IReadOnlyDictionary<uint, string>>.Success(histories);
    }
}
