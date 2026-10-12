using System.Buffers.Binary;
using System.Text;

namespace OwnS3.Replication;

// Свёртка sync-report (глава 07 §10): порядко-независимая XOR 64-бит FNV-1a свёртка
// пар (key, modTimeUnixMs) — попарное сравнение отчётов двух серверов даёт вердикт
// «синхронно/расходится» без передачи полных листингов.
public static class SyncReportHash
{
    private const ulong FnvOffset = 14695981039346656037UL;
    private const ulong FnvPrime = 1099511628211UL;

    public static string Compute(IEnumerable<(string Key, long ModTimeUnixMs)> entries)
    {
        ulong hash = 0; // XOR-свёртка FNV-1a-хэшей пар — порядко-независима
        foreach (var (key, modTimeUnixMs) in entries)
            hash ^= Fnv1a64(key, modTimeUnixMs);
        return hash.ToString("x16");
    }

    private static ulong Fnv1a64(string key, long modTimeUnixMs)
    {
        var hash = FnvOffset;
        foreach (var b in Encoding.UTF8.GetBytes(key))
            hash = (hash ^ b) * FnvPrime;
        Span<byte> modBytes = stackalloc byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(modBytes, modTimeUnixMs);
        foreach (var b in modBytes)
            hash = (hash ^ b) * FnvPrime;
        return hash;
    }
}
