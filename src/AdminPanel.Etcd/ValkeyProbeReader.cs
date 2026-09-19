using AdminPanel.Core.Valkey;

namespace AdminPanel.Etcd;

// Live-пробы valkey (PING): Etcd-сборка знает только интерфейс; реализация-адаптер
// над реальным стором проб — в AdminPanel.Probes (паттерн IKafkaProbeReader).
public interface IValkeyProbeReader
{
    IReadOnlyList<ValkeyProbeResult>? Current { get; }
}
