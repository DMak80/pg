using System.Reflection;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.DependencyInjection;

namespace AdminPanel.Api;

// Модуль хоста: auth-сервисы и [Config]-POCO Api-сборки через attribute-DI (spec t02 §3.11).
public static class ModuleExtensions
{
    private static Assembly Assembly => typeof(ModuleExtensions).Assembly;

    public static IServiceCollection AddApi(this IServiceCollection services)
        => services.AutoRegistration(Assembly);
}
