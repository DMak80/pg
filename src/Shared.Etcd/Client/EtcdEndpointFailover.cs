using Shared.Core;

namespace Shared.Etcd.Client;

// Единый etcd-endpoint-failover монорепо (паттерн arch/17): обход endpoints
// по кругу от startIndex (0 — с первого); первый успешный ответ — истина;
// все недоступны — ПОСЛЕДНЯЯ ошибка наружу (причина жива в статусе/логе);
// пустой список — EtcdUnreachableException. startIndex ≠ 0 — ротация старта
// от активного endpoint (снапшот-циклы панели).
public static class EtcdEndpointFailover
{
    public static Task<Result<T>> CallAsync<T>(
        IReadOnlyList<string> endpoints,
        Func<string, Task<Result<T>>> call,
        int startIndex = 0)
        => CallAsync(endpoints, (endpoint, _) => call(endpoint), CancellationToken.None, startIndex);

    public static Task<Result> CallAsync(
        IReadOnlyList<string> endpoints,
        Func<string, Task<Result>> call,
        int startIndex = 0)
        => CallAsync(endpoints, (endpoint, _) => call(endpoint), CancellationToken.None, startIndex);

    public static async Task<Result<T>> CallAsync<T>(
        IReadOnlyList<string> endpoints,
        Func<string, CancellationToken, Task<Result<T>>> call,
        CancellationToken ct,
        int startIndex = 0)
    {
        if (endpoints.Count == 0)
            return Result<T>.Failed(new EtcdUnreachableException("etcd endpoints are not set"));

        Exception? last = null;
        string? previous = null;
        for (var i = 0; i < endpoints.Count; i++)
        {
            var endpoint = endpoints[(startIndex + i) % endpoints.Count];
            var result = await call(endpoint, ct);
            if (result.IsSuccess)
            {
                if (previous is not null)
                    Console.WriteLine($"etcd: endpoint switch {previous} -> {endpoint}");
                return result;
            }

            previous = endpoint;
            last = result.Error;
        }

        return Result<T>.Failed(last ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

    public static async Task<Result> CallAsync(
        IReadOnlyList<string> endpoints,
        Func<string, CancellationToken, Task<Result>> call,
        CancellationToken ct,
        int startIndex = 0)
    {
        if (endpoints.Count == 0)
            return Result.Failed(new EtcdUnreachableException("etcd endpoints are not set"));

        Exception? last = null;
        string? previous = null;
        for (var i = 0; i < endpoints.Count; i++)
        {
            var endpoint = endpoints[(startIndex + i) % endpoints.Count];
            var result = await call(endpoint, ct);
            if (result.IsSuccess)
            {
                if (previous is not null)
                    Console.WriteLine($"etcd: endpoint switch {previous} -> {endpoint}");
                return result;
            }

            previous = endpoint;
            last = result.Error;
        }

        return Result.Failed(last ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }
}
