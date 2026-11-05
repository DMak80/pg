using System.Text;
using PgWorker.Backups.Tar;

namespace PgWorker.Backups;

/// <summary>Чтение служебных сведений ЕДИНОГО tar-объекта полного (spec t01
/// §3.7): backup_label — ПЕРВАЯ запись архива (сервер кладёт его первым,
/// arch/pgnode/04 §2), поэтому 64 КиБ хвоста начала объекта покрывают
/// заголовок+содержимое с запасом (Range-чтение без полного скачивания).</summary>
public static class FullTarSet
{
    private const int LabelRangeBytes = 64 * 1024;

    /// <summary>Текст backup_label из объекта `full/&lt;id&gt;.tar`: первая
    /// запись — backup_label → её данные ASCII; иная/отсутствующая запись —
    /// Success(null); транспортный отказ Range — Failed (причина наружу).</summary>
    public static async Task<Result<string?>> ReadBackupLabelAsync(
        IStreamObjectS3 s3, string cluster, string shard, string backupId, CancellationToken ct)
    {
        var range = await s3.GetObjectRangeAsync(
            cluster, shard, $"full/{backupId}.tar", 0, LabelRangeBytes - 1, ct);
        if (!range.IsSuccess)
            return Result<string?>.Failed(range.Error!);

        try
        {
            var reader = new TarStreamReader(new MemoryStream(range.Value));
            var entry = await reader.ReadHeaderAsync(ct);
            if (entry is null || entry.Name != "backup_label" || entry.TypeFlag != TarTypeFlags.File)
                return Result<string?>.Success(null);
            var data = await reader.ReadAllDataAsync(ct);
            return Result<string?>.Success(Encoding.UTF8.GetString(data));
        }
        catch (TarFormatException)
        {
            // Не tar/битый заголовок — метки нет (валидация решает permanent).
            return Result<string?>.Success(null);
        }
    }
}
