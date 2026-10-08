using System.Text;

namespace Metrics.SdGenerator;

// Атомарная запись file_sd-файла при diff (spec §2): контент не изменился —
// файл не трогается (mtime не дёргается впустую, Prometheus не перечитывает);
// изменился — запись во временный файл + атомарный rename.
public sealed class SdFileWriter(string path)
{
    /// <summary>true — файл перезаписан (tmp+rename); false — контент не изменился (mtime не тронут).</summary>
    public bool WriteIfChanged(string content)
    {
        var bytes = Encoding.UTF8.GetBytes(content);
        if (File.Exists(path) && File.ReadAllBytes(path).SequenceEqual(bytes))
            return false;

        // tmp рядом с целью — тот же том, rename атомарен.
        var tmp = path + ".tmp";
        File.WriteAllBytes(tmp, bytes);
        File.Move(tmp, path, overwrite: true);
        return true;
    }
}
