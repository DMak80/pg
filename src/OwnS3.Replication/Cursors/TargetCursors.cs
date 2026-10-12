using System.Text.Json;

namespace OwnS3.Replication.Cursors;

// Watermark-курсоры приёма per-target (t35, глава 07 §5): cursors/<name>.json
// {"acceptedSeqNo": long, "mode": "tail"|"reconciling"} — позиция, до которой
// сосед ПОДТВЕРДИЛ ПРИЁМ уведомлений. Персист — fsync-дебаунс ~1 с и при
// Dispose (graceful-shutdown); потеря хвоста = повторная рассылка (идемпотентно —
// дедуп приёмника). Продвижение — только вперёд (монотонность).
public sealed class TargetCursors : IDisposable
{
    // Модель файла курсора.
    private sealed record CursorFile(long AcceptedSeqNo, string Mode);

    private readonly string _cursorsDir;
    private readonly TimeProvider _timeProvider;
    private readonly Dictionary<string, (long Accepted, string Mode)> _cursors = new(StringComparer.Ordinal);
    private readonly HashSet<string> _dirty = new(StringComparer.Ordinal);
    private readonly object _lock = new();
    private readonly ITimer? _debounceTimer;

    public TargetCursors(string cursorsDir, TimeProvider timeProvider)
    {
        _cursorsDir = cursorsDir;
        _timeProvider = timeProvider;
        // Дебаунс-персист ~1 с (глава 07 §5): частые ACK не пишут файл на каждый тик
        _debounceTimer = timeProvider.CreateTimer(static self => ((TargetCursors)self!).Flush(), this,
            TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
    }

    // Позиция приёма соседа; файла нет → -1 (источник решает: новый сосед → с конца журнала).
    public long GetAccepted(string targetName)
    {
        lock (_lock)
        {
            if (_cursors.TryGetValue(targetName, out var cursor))
                return cursor.Accepted;
            var file = Path.Combine(_cursorsDir, targetName + ".json");
            if (!File.Exists(file))
                return -1;
            var loaded = Load(targetName);
            _cursors[targetName] = loaded;
            return loaded.Accepted;
        }
    }

    // Продвижение по acceptedToSeqNo ответа приёма (глава 07 §6) — только вперёд:
    // попытка назад игнорируется (монотонность watermark).
    public void Advance(string targetName, long acceptedSeqNo)
    {
        lock (_lock)
        {
            var current = GetAccepted(targetName);
            if (acceptedSeqNo <= current)
                return;
            var mode = _cursors.TryGetValue(targetName, out var c) ? c.Mode : "tail";
            _cursors[targetName] = (acceptedSeqNo, mode);
            _dirty.Add(targetName);
        }
    }

    // Режим notifier'а соседа: "tail" (рассылка) | "reconciling".
    public void SetMode(string targetName, string mode)
    {
        lock (_lock)
        {
            var accepted = GetAccepted(targetName);
            _cursors[targetName] = (accepted, mode);
            _dirty.Add(targetName);
        }
    }

    // Синхронный fsync-персист всех грязных (tmp + rename).
    public void Flush()
    {
        lock (_lock)
        {
            if (_dirty.Count == 0)
                return;
            Directory.CreateDirectory(_cursorsDir);
            foreach (var name in _dirty)
            {
                if (!_cursors.TryGetValue(name, out var cursor))
                    continue;
                var path = Path.Combine(_cursorsDir, name + ".json");
                var tmp = path + ".tmp";
                File.WriteAllText(tmp, JsonSerializer.Serialize(new CursorFile(cursor.Accepted, cursor.Mode),
                    new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase }));
                File.Move(tmp, path, overwrite: true);
            }
            _dirty.Clear();
        }
    }

    public void Dispose()
    {
        // Graceful-shutdown: таймер остановлен, хвост персистится (глава 07 §5)
        _debounceTimer?.Dispose();
        Flush();
    }

    private (long Accepted, string Mode) Load(string targetName)
    {
        var text = File.ReadAllText(Path.Combine(_cursorsDir, targetName + ".json"));
        var file = JsonSerializer.Deserialize<CursorFile>(text,
            new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
        return file is null ? (-1, "tail") : (file.AcceptedSeqNo, file.Mode);
    }
}
