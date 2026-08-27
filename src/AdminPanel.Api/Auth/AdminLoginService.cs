using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Auth;

// Статус попытки логина.
public enum LoginStatus
{
    Ok,
    InvalidCredentials,
    RateLimited,
}

// Результат попытки логина: статус + секунды до конца окна (для Retry-After).
public sealed record LoginResult(LoginStatus Status, int RetryAfterSeconds = 0);

// Оркестратор логина: rate-limit до проверки учётных данных (PBKDF2 дорог).
public interface IAdminLoginService
{
    LoginResult Login(string? username, string? password, string clientKey);
}

[InjectAsSingleton]
public sealed class AdminLoginService(ILoginRateLimiter rateLimiter, IAdminAuthenticator authenticator)
    : IAdminLoginService
{
    public LoginResult Login(string? username, string? password, string clientKey)
    {
        var decision = rateLimiter.TryAcquire(clientKey);
        if (!decision.Allowed)
            return new LoginResult(LoginStatus.RateLimited, decision.RetryAfterSeconds);

        return authenticator.Authenticate(username, password)
            ? new LoginResult(LoginStatus.Ok)
            : new LoginResult(LoginStatus.InvalidCredentials);
    }
}
