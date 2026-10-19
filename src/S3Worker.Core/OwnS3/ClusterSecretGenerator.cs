using System.Security.Cryptography;

namespace S3Worker.Core.OwnS3;

/// <summary>
/// Генератор значений per-cluster клиентских кредов S3 (arch/23 §6):
/// криптостойкий источник, 32 символа, алфавит [A-Za-z0-9] — без
/// спецсимволов, чтобы значение было безопасно для env контейнера,
/// JSON-значения etcd и SigV4 без экранирования.
/// </summary>
public static class ClusterSecretGenerator
{
    public const int Length = 32;

    private const string Alphabet =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    public static string Generate()
    {
        Span<char> chars = stackalloc char[Length];
        for (var i = 0; i < Length; i++)
            chars[i] = Alphabet[RandomNumberGenerator.GetInt32(Alphabet.Length)];
        return new string(chars);
    }
}
