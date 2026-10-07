using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Shared.Core.Hosting;

public static class LoopWatchdogServiceCollectionExtensions
{
    /// <summary>Регистрация watchdog: синглтон + hosted-обёртка (паттерн циклов
    /// Program.cs воркеров). Требует ILoopsVitality и TimeProvider в DI.
    /// Enabled=false — не регистрирует ничего (поведение как до задачи).</summary>
    public static IServiceCollection AddLoopWatchdog(
        this IServiceCollection services,
        WatchdogOptions options,
        Action<IServiceProvider, string>? restartMark = null)
    {
        if (!options.Enabled)
            return services;

        services.AddSingleton(sp => new LoopWatchdog(
            sp.GetRequiredService<ILoopsVitality>(),
            sp.GetRequiredService<IHostApplicationLifetime>(),
            sp.GetRequiredService<ILogger<LoopWatchdog>>(),
            sp.GetRequiredService<TimeProvider>(),
            options,
            restartMark is null ? null : loop => restartMark(sp, loop)));
        services.AddHostedService(sp => sp.GetRequiredService<LoopWatchdog>());
        return services;
    }
}
