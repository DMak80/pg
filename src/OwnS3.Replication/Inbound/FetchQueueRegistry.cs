using System.Collections.Concurrent;

namespace OwnS3.Replication.Inbound;

// Очереди фетчей по ИМЕНИ соседа-источника (sourceName = TargetRecord.Name приёмника;
// имя определяется по accessKey подписи батча — канон 3.1/07 §2 «идентификация соседа
// по его accessKey»; ключ не найден в targets → ReplicationHandlers отвечает 403).
public sealed class FetchQueueRegistry
{
    private readonly ConcurrentDictionary<string, FetchQueue> _queues = new(StringComparer.Ordinal);

    // Создаётся по требованию (первое уведомление источника).
    public FetchQueue ForSource(string sourceName) =>
        _queues.GetOrAdd(sourceName, static (_, capacity) => new FetchQueue(capacity), _options.FetchQueueMax);

    // Активные источники (статус, глава 07 §10).
    public IReadOnlyCollection<string> Sources => _queues.Keys.ToList();

    private readonly ReplicationOptions _options;

    public FetchQueueRegistry(ReplicationOptions options) => _options = options;
}
