using System.Security.Cryptography;
using System.Text;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AdminPanel.Api.Auth;

// Проверка учётных данных единственного админа: constant-time, без rate-limit.
public interface IAdminAuthenticator
{
    bool Authenticate(string? username, string? password);
}

// Constant-time-проверка username+password по настройкам AdminPanel:Auth (spec t02 §7.1).
[InjectAsSingleton]
public sealed class AdminAuthenticator(IOptions<AuthOptions> options, ILogger<AdminAuthenticator> logger)
    : IAdminAuthenticator
{
    // Защита от тривиальных hash-значений.
    private const int MinHashLength = 16;

    public bool Authenticate(string? username, string? password)
    {
        var auth = options.Value;
        if (string.IsNullOrEmpty(auth.Username))
            return false;

        // Обе проверки выполняются всегда: время ответа не раскрывает, какое поле неверно.
        var usernameOk = FixedTimeEquals(username, auth.Username);
        var passwordOk = VerifyPassword(password, auth);
        return usernameOk & passwordOk;
    }

    // Приоритет PasswordHash над plain Password (arch/01 §4); пустой конфиг — fail-closed.
    private bool VerifyPassword(string? password, AuthOptions auth)
    {
        if (!string.IsNullOrEmpty(auth.PasswordHash))
            return VerifyPbkdf2(password, auth.PasswordHash);

        return !string.IsNullOrEmpty(auth.Password) && FixedTimeEquals(password, auth.Password);
    }

    // Формат $pbkdf2-sha256$<iterations>$<salt-b64>$<hash-b64>; битый формат — fail-closed.
    private bool VerifyPbkdf2(string? password, string configured)
    {
        var parts = configured.Split('$');
        if (parts.Length != 5 || parts[1] != "pbkdf2-sha256")
            return MalformedHash();

        if (!int.TryParse(parts[2], out var iterations) || iterations < 1)
            return MalformedHash();

        byte[] salt;
        byte[] expected;
        try
        {
            salt = Convert.FromBase64String(parts[3]);
            expected = Convert.FromBase64String(parts[4]);
        }
        catch (FormatException)
        {
            return MalformedHash();
        }

        if (expected.Length < MinHashLength)
            return MalformedHash();

        var actual = Rfc2898DeriveBytes.Pbkdf2(
            Encoding.UTF8.GetBytes(password ?? ""),
            salt,
            iterations,
            HashAlgorithmName.SHA256,
            expected.Length);
        return CryptographicOperations.FixedTimeEquals(actual, expected);
    }

    private bool MalformedHash()
    {
        logger.LogWarning("AdminPanel:Auth:PasswordHash имеет битый формат — логин отклоняется (fail-closed)");
        return false;
    }

    // Дайджесты дают равные длины — сравнение постоянно по времени для любых входов.
    private static bool FixedTimeEquals(string? a, string? b)
        => CryptographicOperations.FixedTimeEquals(
            SHA256.HashData(Encoding.UTF8.GetBytes(a ?? "")),
            SHA256.HashData(Encoding.UTF8.GetBytes(b ?? "")));
}
