using PgWorker.Core;

namespace PgWorker.App.Api.Operations;

// Failover по endpoints для хендлеров API (task etcd-via-worker-api): первый
// успешный ответ выигрывает (паттерн WithFailoverAsync воркерских процессов);
// все недоступны → EtcdWriteUnavailableException = 503 (замена «активного
// endpoint из снапшота» панели на собственный список воркера).
internal static class EtcdFailover
{
    public static async Task<Result<T>> CallAsync<T>(string[] endpoints, Func<string, Task<Result<T>>> call)
    {
        foreach (var endpoint in endpoints)
        {
            var result = await call(endpoint);
            if (result.IsSuccess)
                return result;
        }

        return Result<T>.Failed(new EtcdWriteUnavailableException());
    }

    public static async Task<Result> CallAsync(string[] endpoints, Func<string, Task<Result>> call)
    {
        foreach (var endpoint in endpoints)
        {
            var result = await call(endpoint);
            if (result.IsSuccess)
                return result;
        }

        return Result.Failed(new EtcdWriteUnavailableException());
    }
}
