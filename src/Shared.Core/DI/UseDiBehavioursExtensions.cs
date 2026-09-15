using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Shared.Core.DI;

// Включает DI-поведения авто-регистрации для сборок, передаваемых в AutoRegistration.
public static class UseDiBehavioursExtensions
{
    public static IServiceCollection UseDiBehaviours(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        new AutoRegistrationDiTypeBehaviour(services).UseBehaviour();
        new AutoRegistrationConfigDiTypeBehaviour(services, configuration).UseBehaviour();
        return services;
    }
}
