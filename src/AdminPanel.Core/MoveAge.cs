namespace AdminPanel.Core;

// Возраст не-ACTIVE статуса бакета: now − (updated_unix ?? started_unix) (spec §3.7, §4.4).
// Единая формула правил move-* и ClusterDetailsMapper — алерты и UI не расходятся.
public static class MoveAge
{
    // Штамп-база возраста: updated_unix, при отсутствии — started_unix.
    // null — бакет ACTIVE или оба штампа отсутствуют (битые данные видит key-malformed).
    public static long? Stamp(BucketInfo bucket)
        => bucket.State == BucketState.Active
            ? null
            : bucket.Move?.UpdatedUnix ?? bucket.Move?.StartedUnix;

    // Возраст в целых секундах от штампа-базы; null — базы нет (spec §3.7).
    public static long? Seconds(BucketInfo bucket, long nowUnix)
    {
        var stamp = Stamp(bucket);
        return stamp is null ? null : nowUnix - stamp.Value;
    }
}
