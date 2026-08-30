using AdminPanel.Core.Kafka;

namespace AdminPanel.Etcd;

/// <summary>
/// Читатель live-состояния kafka-проб для снапшот-цикла (волна C): реализация —
/// адаптер над KafkaProbeStore из AdminPanel.Probes (регистрируется в AddProbes).
/// Через него refresher вносит runtime-данные (USR топиков, группы с лагами)
/// в KafkaClusterInfo — их видят алерты и инспекция.
/// </summary>
public interface IKafkaProbeReader
{
    KafkaProbeState? Current { get; }
}
