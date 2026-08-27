using System.Diagnostics;
using AdminPanel.Infrastructure.Contexts;
using AdminPanel.Infrastructure.DI;
using AdminPanel.Infrastructure.Traces;
using Microsoft.Extensions.DependencyInjection;

namespace AdminPanel.Infrastructure.CQRS;

// Диспетчер запросов и команд: открывает scope при вызове из корневого провайдера и обрамляет выполнение Activity.
public interface IHandler
{
    ValueTask<Result<T>> HandleQuery<Q, T>(Q query, CancellationToken ct)
        where Q : IQuery<T>;

    ValueTask<Result<T>> HandleCommand<C, T>(C command, CancellationToken ct)
        where C : ICommand<T>;
}

[InjectAsTransient]
internal class Handler(IServiceProviderHelper spHelper, IServiceProvider sp) : IHandler
{
    public async ValueTask<Result<T>> HandleQuery<Q, T>(Q query, CancellationToken ct)
        where Q : IQuery<T>
    {
        Result<T> result = null!;
        await Tracing.ActivityT(
            TypeName<Q>(),
            ActivityKind.Server,
            () => Run(async isp =>
            {
                var handler = isp.GetRequiredService<IQueryHandler<Q, T>>();
                result = await handler.Handle(query, ct);
            }));
        return result;
    }

    public async ValueTask<Result<T>> HandleCommand<C, T>(C command, CancellationToken ct)
        where C : ICommand<T>
    {
        Result<T> result = null!;
        await Tracing.ActivityT(
            TypeName<C>(),
            ActivityKind.Server,
            () => Run(async isp =>
            {
                var handler = isp.GetRequiredService<ICommandHandler<C, T>>();
                result = await handler.Handle(command, ct);
            }));
        return result;
    }

    private async Task Run(Func<IServiceProvider, ValueTask> func)
    {
        if (spHelper.IsGlobal(sp))
        {
            using var scope = sp.CreateScope();
            await func(scope.ServiceProvider);
        }
        else
            await func(sp);
    }

    private static string TypeName<T>()
        => !typeof(T).IsGenericType
            ? typeof(T).Name
            : typeof(T).Name + string.Join(",", typeof(T).GenericTypeArguments.Select(x => x.Name));
}
