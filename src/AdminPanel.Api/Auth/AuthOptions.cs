using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Auth;

// [Config]-POCO аутентификации: секция AdminPanel:Auth (arch/01 §6, spec t02 §6.1).
[Config("AdminPanel:Auth")]
public class AuthOptions
{
    public string? Username { get; set; }

    // Plain-пароль — только dev/стенд; в git не попадает.
    public string? Password { get; set; }

    // $pbkdf2-sha256$<iterations>$<salt-b64>$<hash-b64> — приоритет над Password.
    public string? PasswordHash { get; set; }

    public double SessionHours { get; set; } = 8;

    // true только для стенда по http (Secure-политика cookie).
    public bool AllowHttp { get; set; }
}
