using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace PgWorker.WalReceiver;

/// <summary>Однострочные JSON-маркеры stdout (t27, arch/19 §3.1, протокол джобов
/// §2): прогресс стрима/доставки, heartbeat с подтверждённой позицией и
/// result-JSON при выходе. Воркер парсирует только свои строки; секреты в
/// маркеры не попадают. LSN — PG-формат `X/Y` (hex).</summary>
public static class WalReceiverMarkers
{
    private static readonly JsonSerializerOptions Options = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        // Кириллица ошибок читаемой строкой (маркеры — не HTML-контекст).
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    /// <summary>LSN → PG-формат `X/Y`: старшие 32 бита / младшие (hex).</summary>
    public static string LsnText(ulong value)
        => $"{value >> 32:X}/{value & 0xFFFFFFFF:X}";

    private static string ToJson<T>(T marker)
        => JsonSerializer.Serialize(marker, Options);

    public static string Starting()
        => ToJson(new StartingMarker());

    public static string Streaming(ulong walStart)
        => ToJson(new StreamingMarker(LsnText(walStart)));

    public static string Delivering(string segment)
        => ToJson(new DeliveringMarker(segment));

    public static string Heartbeat(ulong confirmedLsn)
        => ToJson(new HeartbeatMarker(LsnText(confirmedLsn)));

    /// <summary>TLI-переход без истории (Npgsql не экспонирует TIMELINE_HISTORY):
    /// fallback-контроль воркера (docker-exec, arch/19 §3).</summary>
    public static string HistoryMissing(uint tli)
        => ToJson(new HistoryMissingMarker(tli));

    public static string Result(bool ok, string? error, string? lastDelivered)
        => ToJson(new ResultMarker(ok, error, lastDelivered));

    private sealed record StartingMarker([property: JsonPropertyName("phase")] string Phase = "starting");

    private sealed record StreamingMarker([property: JsonPropertyName("wal_start")] string WalStart)
    {
        [property: JsonPropertyName("phase")]
        public string Phase => "streaming";
    }

    private sealed record DeliveringMarker([property: JsonPropertyName("segment")] string Segment)
    {
        [property: JsonPropertyName("phase")]
        public string Phase => "delivering";
    }

    private sealed record HeartbeatMarker([property: JsonPropertyName("heartbeat")] string Heartbeat);

    private sealed record HistoryMissingMarker(
        [property: JsonPropertyName("history_missing")] string Tli)
    {
        public HistoryMissingMarker(uint tli) : this($"{tli}")
        {
        }

        [property: JsonPropertyName("phase")]
        public string Phase => "streaming";
    }

    private sealed record ResultMarker(
        [property: JsonPropertyName("ok")] bool Ok,
        [property: JsonPropertyName("error")] string? Error = null,
        [property: JsonPropertyName("last_delivered")] string? LastDelivered = null);
}
