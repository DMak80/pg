namespace KafkaWorker.Provisioning.Kafka;

/// <summary>Целевой assignment одной партиции (элемент reassignment.json).</summary>
public sealed record ReassignMove(string Topic, int Partition, IReadOnlyList<int> Replicas);
