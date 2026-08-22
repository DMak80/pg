using System.Reflection;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.DependencyInjection;

namespace AdminPanel.Infrastructure;

// Модуль каркаса: регистрирует все типы сборки через attribute-DI.
public static class ModuleExtensions
{
    private static Assembly Assembly => typeof(ModuleExtensions).Assembly;

    public static IServiceCollection AddInfrastructure(this IServiceCollection services)
        => services.AutoRegistration(Assembly);
}
