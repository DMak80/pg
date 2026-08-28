using System.Security.Cryptography;

namespace PgWorker.Core.Model;

/// <summary>
/// Генератор per-cluster пароля приложения (spec §4.1): криптостойкий
/// источник, 32 символа, алфавит [A-Za-z0-9] — без спецсимволов, чтобы
/// пароль был безопасен для SQL-литералов, libpq/Npgsql-строк, env и JSON
/// без экранирования.
/// </summary>
public static class AppSecretGenerator
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
