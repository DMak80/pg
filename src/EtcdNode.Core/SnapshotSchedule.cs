namespace EtcdNode.Core;

/// <summary>Чистое решение тика агента EtcdNode (arch/etcdnode/02-snapshots.md
/// §3, arch/etcdnode/processes/c-snapshot-export.md C2): плановый слепок раз в
/// IntervalMin; внеочередной — ТОЛЬКО важное изменение (пульсы не триггерят):
/// нет эталона — важно (снимаем сразу, RPO-консервативно); revision не вырос
/// относительно эталона — не важно; вырос — решает дифф карты (diffImportant,
/// вычисляется циклом ТОЛЬКО при росте revision).</summary>
public static class SnapshotSchedule
{
    public static bool ShouldTake(
        long nowUnix,
        long? lastTakeUnix,
        long? lastUploadedUnix,
        long currentRevision,
        long? baselineRevision,
        bool diffImportant,
        int intervalMin,
        int minIntervalSec)
    {
        // Плановый: никогда не снимали либо прошло ≥ IntervalMin.
        if (lastTakeUnix is not { } taken || nowUnix - taken >= intervalMin * 60L)
            return true;

        // Важность: нет эталона — важно; revision не вырос — не важно;
        // вырос — решает дифф карты.
        var important = baselineRevision is not { } baseline
            ? true
            : currentRevision > baseline && diffImportant;
        if (!important)
            return false;

        // Внеочередной: важно И выгрузка отстала ≥ MinIntervalSec
        // (никогда не выгружали — снимаем).
        return lastUploadedUnix is not { } uploaded
            ? true
            : nowUnix - uploaded >= minIntervalSec;
    }
}
