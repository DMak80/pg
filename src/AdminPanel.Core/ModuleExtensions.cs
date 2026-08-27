using System.Reflection;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.DependencyInjection;

namespace AdminPanel.Core;

// Модуль домена снапшота: пока пуст, наполняется задачами t02+.
public static class ModuleExtensions
{
    private static Assembly Assembly => typeof(ModuleExtensions).Assembly;

    public static IServiceCollection AddCore(this IServiceCollection services)
        => services.AutoRegistration(Assembly);
}
