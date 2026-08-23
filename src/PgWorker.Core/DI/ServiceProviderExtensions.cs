using Microsoft.Extensions.DependencyInjection;

namespace PgWorker.Core.DI;

public static class ServiceProviderExtensions
{
    extension(IServiceProvider sp)
    {
        public Result<T> GetService<T>(Type serviceType)
            where T : class
        {
            try
            {
                return sp.GetRequiredService(serviceType) as T
                       ?? throw new InvalidTypeException(typeof(T), serviceType);
            }
            catch (Exception ex)
            {
                return ex;
            }
        }
    }
}

public class InvalidTypeException(Type tinterface, Type service)
    : ApplicationException($"Invalid type {service.FullName} for interface {tinterface.FullName}")
{
    public Type Interface { get; } = tinterface;

    public Type Service { get; } = service;
}