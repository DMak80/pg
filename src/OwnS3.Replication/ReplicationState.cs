using System.Text.Json;

namespace OwnS3.Replication;

// Маркер чистого завершения приёмника (t35, глава 07 §7):
// .owns3.sys/replication/state.json {"shutdownClean": bool}. Отсутствующий/битый/
// false на старте → стартовый reconcile (§9; «пустой том» — тем же механизмом).
public sealed class ReplicationState
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private sealed record StateFile(bool ShutdownClean);

    private readonly string _path;

    public ReplicationState(string path) => _path = path;

    // Читает при старте; отсутствующий/битый → false (пустой том — тоже false).
    public bool WasCleanShutdown()
    {
        if (!File.Exists(_path))
            return false;
        try
        {
            var state = JsonSerializer.Deserialize<StateFile>(File.ReadAllText(_path), JsonOptions);
            return state?.ShutdownClean == true;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    // При старте — сразу (до готовности).
    public void MarkDirty() => Persist(false);

    // После drain + fsync-точки остановки (graceful stop).
    public void MarkCleanAndPersist() => Persist(true);

    private void Persist(bool clean)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(_path)!);
        var tmp = _path + ".tmp";
        File.WriteAllText(tmp, JsonSerializer.Serialize(new StateFile(clean), JsonOptions));
        File.Move(tmp, _path, overwrite: true);
    }
}
