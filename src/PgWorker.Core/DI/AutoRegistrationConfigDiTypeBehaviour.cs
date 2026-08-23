using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace PgWorker.Core.DI;

public class AutoRegistrationConfigDiTypeBehaviour(IServiceCollection services, IConfiguration configuration)
    : DiTypeBehaviour
{
    private ConfigAttribute _attribute = new();

    protected override bool Filter(Type type)
    {
        return GetAttribute<ConfigAttribute>(type)
           .Apply(attr => _attribute = attr)
           .IsSuccess;
    }

    protected override void Handle(Type type)
    {
        var obj = Activator.CreateInstance(type);
        if (obj == null)
            throw new NullReferenceException($"Cannot create instance of {type.Name}");

        Register(services, configuration, (dynamic)obj, _attribute);
    }

    private static void Register<T>(IServiceCollection services, IConfiguration configuration, T _, ConfigAttribute attr)
        where T : class
    {
        var name = attr.Name ?? typeof(T).Name;
        services.Configure<T>(configuration.GetSection(name));
    }
}