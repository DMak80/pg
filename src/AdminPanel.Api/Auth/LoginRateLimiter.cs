using System.Collections.Concurrent;
using Shared.Core.DI;

namespace AdminPanel.Api.Auth;

// Решение лимитера: разрешено ли и сколько секунд ждать до конца окна.
public sealed record LoginRateDecision(bool Allowed, int RetryAfterSeconds);

// Fixed-window лимитер попыток логина: 5 за 60 c на ключ клиента (spec §3.6).
public interface ILoginRateLimiter
{
    LoginRateDecision TryAcquire(string clientKey);
}

[InjectAsSingleton]
public sealed class LoginRateLimiter(TimeProvider timeProvider) : ILoginRateLimiter
{
    public const int MaxAttempts = 5;

    public static readonly TimeSpan Window = TimeSpan.FromSeconds(60);

    // Состояние окна на ключ: идентификатор окна + счётчик попыток.
    private readonly ConcurrentDictionary<string, (long WindowId, int Count)> _windows = new();

    public LoginRateDecision TryAcquire(string clientKey)
    {
        var now = timeProvider.GetUtcNow().UtcTicks;
        var windowId = now / Window.Ticks;
        var count = _windows.AddOrUpdate(
            clientKey,
            _ => (windowId, 1),
            (_, current) => current.WindowId == windowId ? (windowId, current.Count + 1) : (windowId, 1))
           .Count;
        EvictStaleWindows(windowId);
        return count <= MaxAttempts
            ? new LoginRateDecision(true, 0)
            : new LoginRateDecision(false, RetryAfterSeconds(now, windowId));
    }

    // Вытеснение окон прошлых интервалов: словарь не растёт безгранично по
    // уникальным clientKey. TryRemove пары (ключ, значение) — гонка с параллельным
    // обновлением того же ключа безопасна: свежезаписанное окно не сотрётся.
    private void EvictStaleWindows(long currentWindowId)
    {
        foreach (var (key, window) in _windows)
            if (window.WindowId < currentWindowId)
                _windows.TryRemove(KeyValuePair.Create(key, window));
    }

    // Остаток текущего окна в секундах (1..60) для заголовка Retry-After.
    private static int RetryAfterSeconds(long nowTicks, long windowId)
    {
        var windowEndTicks = (windowId + 1) * Window.Ticks;
        var left = (int)Math.Ceiling(TimeSpan.FromTicks(windowEndTicks - nowTicks).TotalSeconds);
        return Math.Max(left, 1);
    }
}

// Регистрация TimeProvider в DI: базовый тип TimeProvider резолвится в SystemTimeProvider.
[InjectAsSingleton(typeof(TimeProvider))]
public sealed class SystemTimeProvider() : TimeProvider;
