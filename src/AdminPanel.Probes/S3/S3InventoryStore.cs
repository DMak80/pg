using AdminPanel.Core;

namespace AdminPanel.Probes.S3;

// Реализация Core.IS3InventoryStore : писатель один — S3InventoryLoop,
// читают SnapshotRefresher (вносит в снапшот) и тесты. Volatile-замена ссылки —
// паттерн KafkaProbeStore/WorkerHealthStore (KV-тик не блокируется).
public sealed class S3InventoryStore : IS3InventoryStore
{
    private volatile S3StorageInfo? _current;

    public S3StorageInfo? Current => _current;

    public void Replace(S3StorageInfo state) => _current = state;
}
