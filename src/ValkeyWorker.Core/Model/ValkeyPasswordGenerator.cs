using System.Security.Cryptography;

namespace ValkeyWorker.Core.Model;

// Генератор per-cluster ACL-паролей (arch/21 §4): 32 символа [A-Za-z0-9],
// CSPRNG (RandomNumberGenerator).
public static class ValkeyPasswordGenerator
{
    private const string Alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    public static string Generate()
    {
        var bytes = RandomNumberGenerator.GetBytes(32);
        return new string(bytes.Select(b => Alphabet[b % Alphabet.Length]).ToArray());
    }
}
