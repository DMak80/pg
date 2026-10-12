using System.Threading.Channels;

namespace OwnS3.Replication.Inbound;

// Задача фетча (ключ, о котором уведомил источник; modTime — для метрик лага).
public sealed record FetchTask(string Bucket, string Key, long NotifiedModMs);

// Bounded in-memory очередь фетчей per-source (глава 07 §7): object-put ключи,
// ждущие фетча; полна → приём уведомлений отвечает 429 (backpressure).
public sealed class FetchQueue
{
    private readonly Channel<FetchTask> _channel;

    public FetchQueue(int capacity) =>
        _channel = Channel.CreateBounded<FetchTask>(new BoundedChannelOptions(capacity)
        {
            SingleReader = false,
            SingleWriter = false,
        });

    // false — очередь полна (429).
    public bool TryEnqueue(FetchTask task) => _channel.Writer.TryWrite(task);

    public bool TryDequeue(out FetchTask task) => _channel.Reader.TryRead(out task!);

    // Глубина очереди (метрика fetch_queue_depth).
    public int Depth => _channel.Reader.Count;
}
