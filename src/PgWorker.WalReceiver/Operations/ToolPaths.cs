namespace PgWorker.WalReceiver.Operations;

/// <summary>Фиксированные пути tool-контейнера (spec t26 §3.4): Staging —
/// ephemeral-том tool-контейнера verify, точку монтирования задаёт воркер
/// (шаг 11) — константа пути внутри контейнера; Pgdata — Spilo-layout
/// (volume монтируется в /home/postgres/pgdata, данные — pgroot/data,
/// arch/14 §2.1).</summary>
public static class ToolPaths
{
    public const string Staging = "/staging";

    public const string Pgdata = "/home/postgres/pgdata/pgroot/data";
}
