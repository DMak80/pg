namespace EtcdNode.Core;

/// <summary>Объект bucket'а слепков (list-результат ретенции): ключ + размер
/// + время последней модификации (без зависимости от PgWorker.Backups).</summary>
public sealed record EtcdS3Object(string Key, long SizeBytes, DateTimeOffset LastModified);
