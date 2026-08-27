using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Infrastructure.Contexts;

public interface IServiceProviderHelper
{
    bool IsGlobal(IServiceProvider sp);
}

[InjectAsSingleton]
public class ServiceProviderHelper(IServiceProvider serviceProvider) : IServiceProviderHelper
{
    public bool IsGlobal(IServiceProvider sp)
        => sp == serviceProvider;
}