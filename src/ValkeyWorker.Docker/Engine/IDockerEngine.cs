using Shared.Core;

namespace ValkeyWorker.Docker.Engine;

// Тонкий клиент Docker Engine API (порт IDockerEngine KafkaWorker). Каркас t02
// (фаза 2): только ping — нужен ServiceProbes/health; полная копия движка kfw —
// задача 4 (фаза 4 плана).
public interface IDockerEngine : IAsyncDisposable
{
    // GET /_ping — живость docker-хоста.
    Task<Result> PingAsync(CancellationToken ct);
}
