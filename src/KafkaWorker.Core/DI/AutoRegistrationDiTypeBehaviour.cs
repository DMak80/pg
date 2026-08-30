using Microsoft.Extensions.DependencyInjection;

namespace KafkaWorker.Core.DI;

public class AutoRegistrationDiTypeBehaviour(IServiceCollection services) : DiTypeBehaviour
{
    private InjectAsAttribute _attribute = new(ServiceLifetime.Singleton);

    protected override bool Filter(Type type)
    {
        return GetAttribute<InjectAsAttribute>(type).Apply(attr => _attribute = attr).IsSuccess;
    }

    protected override void Handle(Type type)
    {
        Register(services, type, _attribute);
    }

    private static void Register(IServiceCollection services, Type type, InjectAsAttribute attr)
    {
        var serviceDescriptor = new ServiceDescriptor(type, type, attr.Lifetime);
        services.Add(serviceDescriptor);

        var types = attr.Interfaces.Length > 0
            ? attr.Interfaces
            : type.GetInterfaces();
        if (type.IsGenericTypeDefinition)
            types = types
               .Where(t => t is { IsConstructedGenericType: true, GenericTypeArguments.Length: > 0, })
               .Select(x => x.GetGenericTypeDefinition())
               .ToArray();
        foreach (var iface in types)
        {
            var ifaceServiceDescriptor = type.IsGenericTypeDefinition
                ? new ServiceDescriptor(iface, type, attr.Lifetime)
                : new ServiceDescriptor(iface, sp => sp.GetService(type)!, attr.Lifetime);
            services.Add(ifaceServiceDescriptor);
        }
    }
}