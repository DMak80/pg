using System.Reflection;
using Microsoft.Extensions.DependencyInjection;

namespace KafkaWorker.Core.DI;

public static class ServiceCollectionExtensions
{
    private static readonly HashSet<Assembly> _assemblies = new();
    private static readonly HashSet<DiTypeBehaviour> _behaviours = new();

    public static IEnumerable<Assembly> Assemblies => _assemblies;

    public static void UseBehaviour(this DiTypeBehaviour behaviour)
    {
        _behaviours.Add(behaviour);
    }

    public static IServiceCollection AutoRegistration(this IServiceCollection services)
        => services.AutoRegistration(AppDomain.CurrentDomain.GetAssemblies());

    public static IServiceCollection AutoRegistrationCurrentAssembly(this IServiceCollection services)
        => services.AutoRegistration(Assembly.GetCallingAssembly());

    public static IServiceCollection AutoRegistration(
        this IServiceCollection services,
        params IEnumerable<Assembly> assemblies)
    {
        assemblies
           .Where(assembly => !assembly.IsDynamic)
           .Where(_assemblies.Add)
           .Select(assembly => assembly.DefinedTypes.ToList())
           .Aggregate(
                _behaviours,
                static (behaviours, types) =>
                {
                    types.CheckBehaviour();
                    return behaviours;
                });
        return services;
    }

    public static IServiceCollection AutoRegistration(this IServiceCollection services, params IEnumerable<Type> types)
    {
        types.ToList().CheckBehaviour();
        return services;
    }

    private static void CheckBehaviour(this IReadOnlyCollection<Type> types)
    {
        foreach (var behaviour in _behaviours)
            behaviour.Handle(types);
    }
}