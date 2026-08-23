using Microsoft.Extensions.DependencyInjection;

namespace PgWorker.Core.DI;

[AttributeUsage(AttributeTargets.Class)]
public class InjectAsAttribute(ServiceLifetime lifetime, params Type[] interfaces) : Attribute
{
    public ServiceLifetime Lifetime { get; } = lifetime;

    public Type[] Interfaces { get; } = interfaces;
}

public class InjectAsSingletonAttribute(params Type[] interfaces)
    : InjectAsAttribute(ServiceLifetime.Singleton, interfaces)
{
}

public class InjectAsTransientAttribute(params Type[] interfaces)
    : InjectAsAttribute(ServiceLifetime.Transient, interfaces)
{
}

public class InjectAsScopedAttribute(params Type[] interfaces) : InjectAsAttribute(ServiceLifetime.Scoped, interfaces)
{
}
//
// [AttributeUsage(AttributeTargets.Interface)]
// public class Injectable : Attribute
// {
// }