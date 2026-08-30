using System.Security.Cryptography;

namespace KafkaWorker.Core.Model;

/// <summary>
/// Генератор per-cluster SASL-пароля приложения (arch/16 §4): криптостойкий
/// источник, 32 символа, алфавит [A-Za-z0-9] — без спецсимволов, чтобы
/// пароль был безопасен для JAAS-литералов, env и JSON без экранирования.
/// По образцу AppSecretGenerator PgWorker.
/// </summary>
public static class KafkaPasswordGenerator
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
