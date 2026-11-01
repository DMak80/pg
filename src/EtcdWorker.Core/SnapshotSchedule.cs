namespace EtcdWorker.Core;

/// <summary>Чистое решение тика SnapshotLoop (arch/25 §4): плановый слепок
/// раз в IntervalMin; внеочередной — revision вырос относительно последней
/// выгрузки И выгрузка отстала ≥ MinIntervalSec (RPO ≈ 5 мин при активности,
/// без лишних слепков в тишине).</summary>
public static class SnapshotSchedule
{
    public static bool ShouldTake(
        long nowUnix,
        long? lastTakeUnix,
        long? lastUploadedUnix,
        long currentRevision,
        long? lastExportedRevision,
        int intervalMin,
        int minIntervalSec)
    {
        // Плановый: никогда не снимали либо прошло ≥ IntervalMin.
        if (lastTakeUnix is not { } taken || nowUnix - taken >= intervalMin * 60L)
            return true;

        // Внеочередной: есть изменения и выгрузка отстала ≥ MinIntervalSec
        // (никогда не выгружали — снимаем).
        return lastUploadedUnix is not { } uploaded
            ? true
            : currentRevision > (lastExportedRevision ?? long.MinValue)
              && nowUnix - uploaded >= minIntervalSec;
    }
}
