using AdminPanel.Core;

namespace AdminPanel.Probes.S3;

// Реализация Core.IMinioInventoryStore (t08): писатель один — MinioInventoryLoop,
// читают SnapshotRefresher (вносит в снапшот) и тесты. Volatile-замена ссылки —
// паттерн KafkaProbeStore/WorkerHealthStore (KV-тик не блокируется).
public sealed class MinioInventoryStore : IMinioInventoryStore
{
    private volatile MinioStorageInfo? _current;

    public MinioStorageInfo? Current => _current;

    public void Replace(MinioStorageInfo state) => _current = state;
}
