using System.Collections.Concurrent;
using Shared.Core;

namespace Shared.Etcd.Client;

// Единый etcd-endpoint-failover монорепо (паттерн arch/17 S7): обход endpoints
// по кругу; первый успешный ответ — истина; все недоступны — ПОСЛЕДНЯЯ ошибка
// наружу (причина жива в статусе/логе); пустой список — EtcdUnreachableException.
// Sticky последнего живого + per-attempt бюджет (arch/17 S7): мёртвый IP —
// «чёрная дыра» транспорта (SYN висит до TCP-таймаута) — последовательный
// обход платил бы штраф каждым вызовом, пока список начинается с мёртвого;
// следующий вызов стартует с ПОСЛЕДНЕГО ответившего endpoint, каждая попытка
// ограничена бюджетом AttemptTimeout. startIndex ≠ 0 (ротация старта от
// активного endpoint, снапшот-циклы панели) — приоритет над sticky.
public static class EtcdEndpointFailover
{
    /// <summary>Бюджет ОДНОЙ попытки: мёртвый endpoint (висящий SYN) отрезается,
    /// контроль — следующему. Медленные ШТАТНЫЕ операции (Range больших
    /// префиксов) укладываются с запасом; переопределяется тестами.</summary>
    internal static TimeSpan AttemptTimeout { get; set; } = TimeSpan.FromSeconds(5);

    // Sticky: список endpoints → индекс последнего ответившего. Ключ — полная
    // строка списка (соседние воркеры/процессы не делят карту).
    private static readonly ConcurrentDictionary<string, int> StickyStart = new();

    private static string KeyOf(IReadOnlyList<string> endpoints) => string.Join("|", endpoints);

    public static Task<Result<T>> CallAsync<T>(
        IReadOnlyList<string> endpoints,
        Func<string, Task<Result<T>>> call,
        int startIndex = 0)
        => CallCore(endpoints, (endpoint, _) => call(endpoint), CancellationToken.None, startIndex);

    public static Task<Result> CallAsync(
        IReadOnlyList<string> endpoints,
        Func<string, Task<Result>> call,
        int startIndex = 0)
        => CallCoreNonGeneric(endpoints, (endpoint, _) => call(endpoint), CancellationToken.None, startIndex);

    public static Task<Result<T>> CallAsync<T>(
        IReadOnlyList<string> endpoints,
        Func<string, CancellationToken, Task<Result<T>>> call,
        CancellationToken ct,
        int startIndex = 0)
        => CallCore(endpoints, call, ct, startIndex);

    public static Task<Result> CallAsync(
        IReadOnlyList<string> endpoints,
        Func<string, CancellationToken, Task<Result>> call,
        CancellationToken ct,
        int startIndex = 0)
        => CallCoreNonGeneric(endpoints, call, ct, startIndex);

    private static async Task<Result<T>> CallCore<T>(
        IReadOnlyList<string> endpoints,
        Func<string, CancellationToken, Task<Result<T>>> call,
        CancellationToken ct,
        int startIndex)
    {
        if (endpoints.Count == 0)
            return Result<T>.Failed(new EtcdUnreachableException("etcd endpoints are not set"));

        var key = KeyOf(endpoints);
        var start = startIndex != 0 ? startIndex
            : StickyStart.TryGetValue(key, out var sticky) ? sticky : 0;

        Exception? last = null;
        string? previous = null;
        for (var i = 0; i < endpoints.Count; i++)
        {
            var index = (start + i) % endpoints.Count;
            var endpoint = endpoints[index];
            Result<T> result;
            try
            {
                using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                attemptCts.CancelAfter(AttemptTimeout);
                result = await call(endpoint, attemptCts.Token);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                // Бюджет попытки исчерпан (endpoint висит): transient — контроль
                // следующему; причина — в последней ошибке обхода.
                result = Result<T>.Failed(new TimeoutException(
                    $"etcd attempt did not answer within {AttemptTimeout.TotalSeconds:0}s: {endpoint}"));
            }

            if (result.IsSuccess)
            {
                StickyStart[key] = index;
                if (previous is not null)
                    Console.WriteLine($"etcd: endpoint switch {previous} -> {endpoint}");
                return result;
            }

            previous = endpoint;
            last = result.Error;
        }

        return Result<T>.Failed(last ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

    private static async Task<Result> CallCoreNonGeneric(
        IReadOnlyList<string> endpoints,
        Func<string, CancellationToken, Task<Result>> call,
        CancellationToken ct,
        int startIndex)
    {
        if (endpoints.Count == 0)
            return Result.Failed(new EtcdUnreachableException("etcd endpoints are not set"));

        var key = KeyOf(endpoints);
        var start = startIndex != 0 ? startIndex
            : StickyStart.TryGetValue(key, out var sticky) ? sticky : 0;

        Exception? last = null;
        string? previous = null;
        for (var i = 0; i < endpoints.Count; i++)
        {
            var index = (start + i) % endpoints.Count;
            var endpoint = endpoints[index];
            Result result;
            try
            {
                using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                attemptCts.CancelAfter(AttemptTimeout);
                result = await call(endpoint, attemptCts.Token);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                // Бюджет попытки исчерпан (endpoint висит): transient — контроль
                // следующему; причина — в последней ошибке обхода.
                result = Result.Failed(new TimeoutException(
                    $"etcd attempt did not answer within {AttemptTimeout.TotalSeconds:0}s: {endpoint}"));
            }

            if (result.IsSuccess)
            {
                StickyStart[key] = index;
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
