namespace PgWorker.PgNode.BaseBackup;

/// <summary>Одно сообщение wire-протокола PG v3: тип-байт + payload
/// (без заголовка длины — кодек пишет/читает его сам).</summary>
public sealed record PgMessage(byte Type, byte[] Payload);
