using System.Collections.Concurrent;
using System.Net.Http;
using System.Net.Sockets;
using Shared.Core;

namespace Shared.Etcd.Client;

// Единый etcd-endpoint-failover монорепо (паттерн arch/synchronization/03-fault-tolerance.md §S7): обход endpoints
// по кругу; первый успешный ответ — истина; все недоступны — ПОСЛЕДНЯЯ ошибка
// наружу (причина жива в статусе/логе); пустой список — EtcdUnreachableException.
// Sticky последнего живого + per-attempt бюджет + негативный кэш мёртвых
// (arch/synchronization/03-fault-tolerance.md §S7): мёртвый IP — «чёрная дыра» транспорта (SYN висит до
// TCP-таймаута) — последовательный обход платил бы штраф каждым вызовом, пока
// список начинается с мёртвого; следующий вызов стартует с ПОСЛЕДНЕГО
// ответившего endpoint, каждая попытка ограничена бюджетом AttemptTimeout.
// Sticky ключуется полной строкой списка, а списков у процесса несколько
// (клэйм/журнал/instance-ключ/health) — штраф гасится ПРОЦЕСС-ГЛОБАЛЬНОЙ
// картой мёртвых (негативный кэш с cooldown): неуспешная попытка
// (исчерпан бюджет / транспорт-отказ) помечает endpoint, обход ЛЮБОГО списка
// пропускает помеченных (все помечены — обычный круг), первый успех снимает.
// startIndex ≠ 0 (ротация старта, снапшот-циклы панели) — приоритет над sticky.
public static class EtcdEndpointFailover
{
    /// <summary>Бюджет ОДНОЙ попытки: мёртвый endpoint (висящий SYN) отрезается,
    /// контроль — следующему. Медленные ШТАТНЫЕ операции (Range больших
    /// префиксов) укладываются с запасом; переопределяется тестами.</summary>
    internal static TimeSpan AttemptTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Cooldown негативного кэша = 3 бюджета попытки (arch/synchronization/03-fault-tolerance.md §S7):
    /// переживает фазу тика из нескольких последовательных вызовов разных
    /// списков; переопределяется тестами.</summary>
    internal static TimeSpan DeadCooldown { get; set; } = TimeSpan.FromSeconds(15);

    // Sticky: список endpoints → индекс последнего ответившего. Ключ — полная
    // строка списка (соседние воркеры/процессы не делят карту).
    private static readonly ConcurrentDictionary<string, int> StickyStart = new();

    // Негативный кэш: endpoint → «не пытаться до момента». Процесс-глобален —
    // общий для всех списков/компонентов (в этом смысл: штраф платится ОДИН раз
    // на смерть, а не один раз на список).
    private static readonly ConcurrentDictionary<string, DateTimeOffset> DeadUntil = new();

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
        var anyLive = endpoints.Any(e => !IsDead(e));

        Exception? last = null;
        string? previous = null;
        for (var i = 0; i < endpoints.Count; i++)
        {
            var index = (start + i) % endpoints.Count;
            var endpoint = endpoints[index];
            if (anyLive && IsDead(endpoint))
                continue; // негативный кэш: смерть уже известна — штраф не платим
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
                MarkAlive(endpoint);
                StickyStart[key] = index;
                if (previous is not null)
                    Console.WriteLine($"etcd: endpoint switch {previous} -> {endpoint}");
                return result;
            }

            previous = endpoint;
            last = result.Error;
            MarkDead(endpoint, last);
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
        var anyLive = endpoints.Any(e => !IsDead(e));

        Exception? last = null;
        string? previous = null;
        for (var i = 0; i < endpoints.Count; i++)
        {
            var index = (start + i) % endpoints.Count;
            var endpoint = endpoints[index];
            if (anyLive && IsDead(endpoint))
                continue; // негативный кэш: смерть уже известна — штраф не платим
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
                MarkAlive(endpoint);
                StickyStart[key] = index;
                if (previous is not null)
                    Console.WriteLine($"etcd: endpoint switch {previous} -> {endpoint}");
                return result;
            }

            previous = endpoint;
            last = result.Error;
            MarkDead(endpoint, last);
        }

        return Result.Failed(last ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

    private static string KeyOf(IReadOnlyList<string> endpoints) => string.Join("|", endpoints);

    private static bool IsDead(string endpoint)
        => DeadUntil.TryGetValue(endpoint, out var until) && until > DateTimeOffset.UtcNow;

    // Пометка мёртвым — ТОЛЬКО транспорт-отказ (бюджет попытки/connect/сокет):
    // прикладной отказ живого etcd (HTTP-ответ с ошибкой, lease TTL=0) —
    // НЕ свидетельство смерти endpoint (arch/synchronization/03-fault-tolerance.md §S7: «не отвечает» ≠ смерть).
    private static void MarkDead(string endpoint, Exception? error)
    {
        if (error is not null && IsTransportLoss(error))
            DeadUntil[endpoint] = DateTimeOffset.UtcNow + DeadCooldown;
    }

    private static void MarkAlive(string endpoint)
        => DeadUntil.TryRemove(endpoint, out _);

    private static bool IsTransportLoss(Exception error)
    {
        for (var e = (Exception?)error; e is not null; e = e.InnerException)
            if (e is TimeoutException or HttpRequestException or IOException or SocketException)
                return true;
        return false;
    }

    // Изоляция статических карт между тестами (AttemptTimeout/DeadCooldown
    // переопределяются точечно; карты — сброс целиком).
    internal static void ResetForTests()
    {
        StickyStart.Clear();
        DeadUntil.Clear();
    }
}
