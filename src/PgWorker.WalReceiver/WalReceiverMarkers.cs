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

    /// <summary>t36 (arch/19 §3): приёмник в ретрай-цикле исчерпания списка
    /// пула — маркер в stdout (контроль воркера ставит s3-pool-exhausted в
    /// error DEGRADED; алерт backup-pool-unreachable). Не чаще 1/5 c —
    /// период совпадает с ретраем put.</summary>
    public static string PoolExhausted()
        => ToJson(new PoolExhaustedMarker());

    /// <summary>tli-процедура (58P01): досыпка старого таймлайна от хвостового
    /// сегмента до точки переключения — зазора нет (RPO=0).</summary>
    public static string TliBackfill(string fromSegment, uint toTli)
        => ToJson(new TliBackfillMarker(fromSegment, toTli));

    /// <summary>tli-процедура: прыжок на текущий таймлайн сервера — сегменты
    /// (from_segment, to_segment] не будут доставлены (осознанный RPO-зазор);
    /// контроль воркера фиксирует поле gap по этому маркеру при from_tli != to_tli.</summary>
    public static string TliJump(string fromSegment, uint fromTli, uint toTli, string toSegment)
        => ToJson(new TliJumpMarker(fromSegment, fromTli, toTli, toSegment));

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

    private sealed record PoolExhaustedMarker(
        [property: JsonPropertyName("pool_exhausted")] bool Exhausted = true);

    private sealed record TliBackfillMarker(
        [property: JsonPropertyName("from_segment")] string FromSegment,
        [property: JsonPropertyName("to_tli")] uint ToTli)
    {
        [property: JsonPropertyName("phase")]
        public string Phase => "tli_backfill";
    }

    private sealed record TliJumpMarker(
        [property: JsonPropertyName("from_segment")] string FromSegment,
        [property: JsonPropertyName("from_tli")] uint FromTli,
        [property: JsonPropertyName("to_tli")] uint ToTli,
        [property: JsonPropertyName("to_segment")] string ToSegment)
    {
        [property: JsonPropertyName("phase")]
        public string Phase => "tli_jump";
    }
}
