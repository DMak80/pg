using System.Runtime.InteropServices;

namespace OwnS3.Storage;

// fsync каталога (Unix): open+fsync+close; прочие платформы — no-op (best-effort,
// спека §4.3 п.2). P/Invoke через LibraryImport: partial-класс и явный маршалинг
// строкового параметра (LPStr).
internal static partial class DirectoryFsync
{
    private const int O_RDONLY = 0;

    [LibraryImport("libc", SetLastError = true)]
    private static partial int open([MarshalAs(UnmanagedType.LPStr)] string path, int flags);

    [LibraryImport("libc", SetLastError = true)]
    private static partial int fsync(int fd);

    [LibraryImport("libc", SetLastError = true)]
    private static partial int close(int fd);

    // Только Linux/macOS; ошибки libc проглатываются (best-effort).
    public static void Sync(string directoryPath)
    {
        if (!RuntimeInformation.IsOSPlatform(OSPlatform.Linux)
            && !RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            return;
        var fd = open(directoryPath, O_RDONLY);
        if (fd < 0)
            return;
        try
        {
            _ = fsync(fd);
        }
        finally
        {
            _ = close(fd);
        }
    }
}
