using Shared.Core;
using Shared.Etcd.Client;

namespace Metrics.SdGenerator;

// Failover по endpoints (паттерн EtcdFailover воркеров): первый успешный
// выигрывает; все недоступны — последняя ошибка наружу (тик = warning-лог).
internal static class EtcdFailover
{
    public static async Task<Result<T>> CallAsync<T>(string[] endpoints, Func<string, Task<Result<T>>> call)
    {
        Result<T>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await call(endpoint);
            if (result.IsSuccess)
                return result;
            last = result;
        }
        return last!;
    }
}
