using Microsoft.Extensions.Options;
using KafkaWorker.App;

namespace KafkaWorker.UnitTests.App;

// IOptionsMonitor-дабл (порт PgWorker.UnitTests/App/TestSupport.cs): фиксированные
// настройки KafkaWorkerOptions для тестов циклов/health (t09).
internal sealed class FixedOptionsMonitor(KafkaWorkerOptions value) : IOptionsMonitor<KafkaWorkerOptions>
{
    public KafkaWorkerOptions CurrentValue => value;

    public IDisposable? OnChange(Action<KafkaWorkerOptions, string?> listener) => null;

    public KafkaWorkerOptions Get(string? name) => value;
}
