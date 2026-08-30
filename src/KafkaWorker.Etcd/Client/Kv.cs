namespace KafkaWorker.Etcd.Client;

// Декодированная пара KV: gateway снял base64, парсеры работают с plain-строками (spec §4.1).
public sealed record Kv(string Key, string Value, ulong ModRevision);
