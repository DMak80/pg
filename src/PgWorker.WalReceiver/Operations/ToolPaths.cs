namespace PgWorker.WalReceiver.Operations;

/// <summary>Фиксированные пути tool-контейнера (spec t26 §3.4): staging —
/// ephemeral-том tool-контейнера verify, точку монтирования задаёт воркер
/// (шаг 11) — константа пути внутри контейнера.</summary>
public static class ToolPaths
{
    public const string Staging = "/staging";
}
