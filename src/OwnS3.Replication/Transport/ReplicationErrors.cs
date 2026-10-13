namespace OwnS3.Replication.Transport;

// Новые типизированные ошибки транспорта (конвенция главы 01 §4): кладутся в
// Result.Error, не летят исключениями через границу IReplicationTransport.

/// <summary>Сосед не принял рассылку: 429 (приёмный буфер переполнен),
/// не-200 ответ или обрыв транспорта — вызывающий бэкоффит (глава 07 §8).</summary>
public sealed class PeerRejectedException(string message) : Exception(message);

/// <summary>Probe: имя цели не входит в состав targets снапшота — маркер 400
/// для хендлера (глава 07 §2).</summary>
public sealed class UnknownReplicationTargetException(string message) : Exception(message);
