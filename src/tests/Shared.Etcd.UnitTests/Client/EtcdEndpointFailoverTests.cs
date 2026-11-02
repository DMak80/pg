using Shared.Etcd.Client;
using Xunit;

namespace Shared.Etcd.UnitTests.Client;

// Негативный кэш мёртвых endpoint'ов (arch/17 S7): штраф за мёртвый узел
// платится ОДИН раз на смерть, не один раз на список — sticky пер-список,
// а списков у процесса несколько (клэйм/журнал/instance-ключ/health), и без
// общей карты каждый платил свой «разовый» штраф (факт прогона ha-etcd:
// три вызова трёх списков в одной фазе тика — серия 5-с пауз, слепая зона
// watchdog). Прикладной отказ живого etcd мёртвым НЕ помечает.
public sealed class EtcdEndpointFailoverTests
{
    private const string Dead = "http://dead:2379";
    private const string Alive1 = "http://alive1:2379";
    private const string Alive2 = "http://alive2:2379";

    private static readonly CancellationToken Ct = CancellationToken.None;

    public EtcdEndpointFailoverTests()
    {
        EtcdEndpointFailover.ResetForTests();
        EtcdEndpointFailover.AttemptTimeout = TimeSpan.FromMilliseconds(300);
    }

    // Мёртвый SYN-узел: вызов висит до исчерпания бюджета попытки и бросает
    // OperationCanceledException — тот же путь, что HttpClient по attempt-cts.
    private static async Task<Result<object>> HangUntilCancelledAsync(CancellationToken ct)
    {
        await Task.Delay(Timeout.Infinite, ct);
        return Result<object>.Failed(new ApplicationException("unreachable")); // недостижимо
    }

    // Транспорт-отказ (connect refused/reset — мгновенный, как docker stop).
    private static Task<Result<object>> TransportFailAsync(string endpoint)
        => Task.FromResult(Result<object>.Failed(new HttpRequestException($"connect {endpoint} failed")));

    // Прикладной отказ ЖИВОГО etcd (HTTP-ответ с ошибкой).
    private static Task<Result<object>> AppFailAsync(string endpoint)
        => Task.FromResult(Result<object>.Failed(new EtcdHttpException(endpoint, 404, "lease gone")));

    [Fact]
    public async Task OtherList_DoesNotPayDeadPenalty_AfterFirstDiscovery()
    {
        // Arrange: бюджет попытки 0.3 c; мёртвый узел отвечает «висящим» вызовом.
        var first = new List<string>();
        var second = new List<string>();

        // Act: список A обнаруживает смерть (dead → alive1); список B (ДРУГОЙ)
        // содержит тот же мёртвый узел первым.
        await EtcdEndpointFailover.CallAsync([Dead, Alive1],
            (endpoint, ct) => { first.Add(endpoint); return endpoint == Dead ? HangUntilCancelledAsync(ct) : Task.FromResult(Result<object>.Success(new object())); }, Ct);
        await EtcdEndpointFailover.CallAsync([Dead, Alive2],
            (endpoint, _) => { second.Add(endpoint); return Task.FromResult(Result<object>.Success(new object())); }, Ct);

        // Assert: первый вызов заплатил штраф (dead в обходе), второй — НЕТ:
        // негативный кэш процесс-глобален, а не пер-список.
        first.Should().Equal([Dead, Alive1]);
        second.Should().Equal([Alive2]);
    }

    [Fact]
    public async Task Success_ClearsDeadMark_EndpointRetried()
    {
        // Arrange: смерть обнаружена списком A.
        await EtcdEndpointFailover.CallAsync([Dead, Alive1],
            (endpoint, ct) => endpoint == Dead ? HangUntilCancelledAsync(ct) : Task.FromResult(Result<object>.Success(new object())), Ct);

        // Act: узел ожил — успешный вызов ТОЛЬКО по нему.
        var healed = await EtcdEndpointFailover.CallAsync([Dead], (_, _) => Task.FromResult(Result<object>.Success(new object())), Ct);

        // Assert: следующий список снова пробует его ПЕРВЫМ (пометка снята,
        // sticky последнего успеха — на нём же).
        healed.IsSuccess.Should().BeTrue();
        var after = new List<string>();
        await EtcdEndpointFailover.CallAsync([Dead, Alive2],
            (endpoint, _) => { after.Add(endpoint); return Task.FromResult(Result<object>.Success(new object())); }, Ct);
        after.Should().Equal([Dead]);
    }

    [Fact]
    public async Task AllDead_CircleStillAttempts_Everyone()
    {
        // Arrange: оба узла помечены мёртвыми (транспорт-отказы).
        await EtcdEndpointFailover.CallAsync([Dead, Alive1],
            (endpoint, _) => TransportFailAsync(endpoint), Ct);

        // Act: повторный вызов по тому же списку.
        var attempts = new List<string>();
        var result = await EtcdEndpointFailover.CallAsync([Dead, Alive1],
            (endpoint, _) => { attempts.Add(endpoint); return TransportFailAsync(endpoint); }, Ct);

        // Assert: все помечены → обход НЕ блокируется, каждый попробован.
        result.IsSuccess.Should().BeFalse();
        attempts.Should().Equal([Dead, Alive1]);
    }

    [Fact]
    public async Task ApplicationFailure_DoesNotMarkDead()
    {
        // Arrange: живой etcd отвечает прикладной ошибкой (HTTP 404 lease gone).
        var first = new List<string>();
        var second = new List<string>();

        // Act: два вызова по РАЗНЫМ спискам с одним «прикладным» узлом.
        await EtcdEndpointFailover.CallAsync([Dead, Alive1],
            (endpoint, _) => { first.Add(endpoint); return AppFailAsync(endpoint); }, Ct);
        await EtcdEndpointFailover.CallAsync([Dead, Alive2],
            (endpoint, _) => { second.Add(endpoint); return AppFailAsync(endpoint); }, Ct);

        // Assert: прикладной отказ НЕ помечает — оба списка честно начинают
        // обход с первого элемента (транспорт жив).
        first.Should().Equal([Dead, Alive1]);
        second.Should().Equal([Dead, Alive2]);
    }

    [Fact]
    public async Task AttemptBudget_Timeout_IsTransportLoss_AndMarksDead()
    {
        // Arrange: попытка по мёртвому висит дольше бюджета (SYN-дыра).
        var attempts = new List<string>();

        // Act: dead даёт висящий вызов, бюджет 0.3 c; alive1 отвечает сразу.
        var result = await EtcdEndpointFailover.CallAsync([Dead, Alive1],
            (endpoint, ct) =>
            {
                attempts.Add(endpoint);
                return endpoint == Dead ? HangUntilCancelledAsync(ct) : Task.FromResult(Result<object>.Success(new object()));
            }, Ct);

        // Assert: исчерпание бюджета — транспорт-отказ (TimeoutException):
        // успех с alive1, повторный список dead уже не пытается.
        result.IsSuccess.Should().BeTrue();
        attempts.Should().Equal([Dead, Alive1]);
        var second = new List<string>();
        await EtcdEndpointFailover.CallAsync([Dead, Alive2],
            (endpoint, _) => { second.Add(endpoint); return Task.FromResult(Result<object>.Success(new object())); }, Ct);
        second.Should().Equal([Alive2]);
    }
}
