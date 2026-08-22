using System.Reflection;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.DependencyInjection;

namespace AdminPanel.Probes;

// Модуль live-проб: пока пуст, наполняется задачами t05+.
public static class ModuleExtensions
{
    private static Assembly Assembly => typeof(ModuleExtensions).Assembly;

    public static IServiceCollection AddProbes(this IServiceCollection services)
        => services.AutoRegistration(Assembly);
}
