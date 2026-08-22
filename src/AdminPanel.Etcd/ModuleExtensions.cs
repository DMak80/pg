using System.Reflection;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.DependencyInjection;

namespace AdminPanel.Etcd;

// Модуль etcd-клиента: пока пуст, наполняется задачами t03+.
public static class ModuleExtensions
{
    private static Assembly Assembly => typeof(ModuleExtensions).Assembly;

    public static IServiceCollection AddEtcd(this IServiceCollection services)
        => services.AutoRegistration(Assembly);
}
