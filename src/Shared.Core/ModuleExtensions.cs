using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Shared.Core.DI;

namespace Shared.Core;

// Модуль каркаса: регистрирует все типы сборки через attribute-DI
// (замена панельного AdminPanel.Infrastructure.AddInfrastructure, t08).
public static class ModuleExtensions
{
    private static Assembly Assembly => typeof(ModuleExtensions).Assembly;

    public static IServiceCollection AddSharedCore(this IServiceCollection services)
        => services.AutoRegistration(Assembly);
}
