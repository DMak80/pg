using AdminPanel.Core.S3;

namespace AdminPanel.Etcd;

// Читатели s3-домена (инспекция API) — без блокировок.
public interface IS3SnapshotReader
{
    // До первого тика снапшота нет — потребители показывают «загрузку».
    S3Snapshot? Current { get; }
}

// Хранилище текущего снапшота s3 (порт ValkeySnapshotStore): писатель один —
// S3SnapshotRefresher; атомарная замена volatile-ссылки.
public interface IS3SnapshotStore : IS3SnapshotReader
{
    new S3Snapshot? Current { get; }

    void Replace(S3Snapshot snapshot);
}

// Регистрация — явно в ModuleExtensions.AddS3().
public sealed class S3SnapshotStore : IS3SnapshotStore
{
    private volatile S3Snapshot? _current;

    public S3Snapshot? Current => _current;

    public void Replace(S3Snapshot snapshot) => _current = snapshot;
}
