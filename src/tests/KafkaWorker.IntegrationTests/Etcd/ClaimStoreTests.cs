using FluentAssertions;
using KafkaWorker.Etcd.Client;
using KafkaWorker.Etcd.Coordination;
using Xunit;

namespace KafkaWorker.IntegrationTests.Etcd;

// Координация /kafkaworker/ на реальном etcd (арх-план A14; порт
// EtcdCoordinationTests PgWorker под kafka-префикс): exclusivity клэймов
// и takeover по истечении lease TTL.
[Collection(Kafka.KafkaCollection.Name)]
public class ClaimStoreTests(Kafka.KafkaClusterFixture fixture)
{
    private string Endpoint => fixture.Endpoint;

    private EtcdGateway Gateway => fixture.Gateway;

    private ClaimStore NewClaimStore() => new([Endpoint], Gateway, TimeProvider.System);

    [Fact]
    public async Task TwoClaimStores_MutualExclusion()
    {
        // Arrange: два «инстанса» KafkaWorker.
        var first = NewClaimStore();
        var second = NewClaimStore();
        var ct = TestContext.Current.CancellationToken;

        // Act: оба пытаются захватить кластер.
        var firstClaim = await first.TryClaimClusterAsync("events", ct);
        var secondClaim = await second.TryClaimClusterAsync("events", ct);

        // Assert: exclusivity — кластер обрабатывает один инстанс (arch/16 §6).
        firstClaim.IsSuccess.Should().BeTrue();
        firstClaim.Value.Should().BeTrue();
        secondClaim.IsSuccess.Should().BeTrue();
        secondClaim.Value.Should().BeFalse();

        await first.DisposeAsync();
        await second.DisposeAsync();
    }

    [Fact]
    public async Task ClaimExpiry_AfterLeaseTtl_SecondInstanceTakesOver()
    {
        // Arrange: «умерший» держатель — leased-ключ TTL 2 с, который никто не продлевает.
        var ct = TestContext.Current.CancellationToken;
        var grant = await Gateway.LeaseGrantAsync(Endpoint, 2, ct);
        grant.IsSuccess.Should().BeTrue();
        var claimTxn = await Gateway.TxnAsync(
            Endpoint,
            TxnRequest.Of(
                [TxnCompare.NotExists("/kafkaworker/claims/events")],
                [new TxnOp.Put("/kafkaworker/claims/events", """{"instance":"dead"}""", grant.Value)]),
            ct);
        claimTxn.Value.Succeeded.Should().BeTrue();

        // Act: lease истекает, etcd сам удаляет ключ; второй инстанс захватывает.
        await Task.Delay(3000, ct);
        var second = NewClaimStore();
        var reclaimed = await second.TryClaimClusterAsync("events", ct);

        // Assert: takeover ≤ TTL 15 с (spec §9.7) — здесь ускорено TTL 2 с.
        reclaimed.IsSuccess.Should().BeTrue();
        reclaimed.Value.Should().BeTrue();
        await second.DisposeAsync();
    }

    // AAA: инстанс ClaimStore с advertiseApiUrl ставит ключ /kafkaworker/api/<id>
    // (arch/16 §1.1) со значением-JSON url+instance; DisposeAsync гасит lease —
    // ключ исчезает. Store без await using: DisposeAsync не идемпотентен, зовём явно.
    [Fact]
    public async Task StartAsync_WithAdvertiseApiUrl_PutsApiDiscoveryKey()
    {
        // Arrange — префикс /kafkaworker/api/ в фикстурном etcd кроме нас никто не пишет
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var store = new ClaimStore(
            [Endpoint], Gateway, TimeProvider.System,
            advertiseApiUrl: "http://kafkaworker:8080");

        // Act — keepalive-цикл ставит ключи асинхронно
        await store.StartAsync(cts.Token);
        await Task.Delay(500, cts.Token);

        // Assert — контракт snake_case (arch/02 §2.3.2): {"url","instance","since_unix"};
        // NotContain-проверки ловят регрессию к PascalCase (PayloadJson без policy).
        var api = await Gateway.RangeAsync(Endpoint, "/kafkaworker/api/", cts.Token);
        api.IsSuccess.Should().BeTrue();
        var kv = api.Value.Should().ContainSingle().Subject;
        kv.Key.Should().Be($"/kafkaworker/api/{store.InstanceId}");
        kv.Value.Should().Contain("\"url\":\"http://kafkaworker:8080\"")
            .And.Contain($"\"instance\":\"{store.InstanceId}\"")
            .And.Contain("\"since_unix\":")
            .And.NotContain("\"Url\"").And.NotContain("\"Instance\"").And.NotContain("\"SinceUnix\"");

        // ключ на lease инстанса: DisposeAsync гасит lease — ключ исчезает
        await store.DisposeAsync();
        var goneApi = await Gateway.RangeAsync(Endpoint, "/kafkaworker/api/", cts.Token);
        goneApi.Value.Should().BeEmpty();
    }

    // Ревью Ф7 [impl, major] (зеркало EtcdCoordinationTests PgWorker): etcd
    // недоступен в момент старта воркера — ключ api не ставится вовсе; после
    // появления etcd keepalive-тик пере-ставит его сам (ретрай ~5с). Store
    // стартует ДО контейнера etcd на фиксированном порту (случайный порт docker
    // выделяет заново на каждый старт — стоп/старт Testcontainers не даёт).
    [Fact]
    public async Task StartAsync_EtcdDownAtStart_ApiKeyRecoveredAfterEtcdReturns()
    {
        // Arrange — свой etcd на зарезервированном порту; контейнер ещё НЕ запущен
        await using var fixture = new EtcdFixture(EtcdFixture.ReserveHostPort());
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        var store = new ClaimStore(
            [fixture.Endpoint], fixture.Gateway, TimeProvider.System,
            advertiseApiUrl: "http://kafkaworker:8080");

        // Act — старт при недоступном etcd: постановка молча падает (и чтение тоже)
        await store.StartAsync(cts.Token);
        await Task.Delay(1000, cts.Token);
        var whileDown = await fixture.Gateway.RangeAsync(fixture.Endpoint, "/kafkaworker/api/", cts.Token);
        whileDown.IsSuccess.Should().BeFalse("etcd не запущен — ключи не могли поставиться");

        // etcd появился — ключ ставится ретраем тика (несколько циклов ~5с)
        await fixture.InitializeAsync();
        List<Kv> kvs = [];
        for (var i = 0; i < 30; i++)
        {
            var api = await fixture.Gateway.RangeAsync(fixture.Endpoint, "/kafkaworker/api/", cts.Token);
            if (api.IsSuccess && api.Value.Count > 0)
            {
                kvs = [.. api.Value];
                break;
            }

            await Task.Delay(500, cts.Token);
        }

        // Assert — ключ api восстановлен, контракт snake_case (arch/02 §2.3.2)
        var kv = kvs.Should().ContainSingle().Subject;
        kv.Key.Should().Be($"/kafkaworker/api/{store.InstanceId}");
        kv.Value.Should().Contain("\"url\":\"http://kafkaworker:8080\"")
            .And.Contain($"\"instance\":\"{store.InstanceId}\"")
            .And.Contain("\"since_unix\":");

        // DisposeAsync гасит lease — ключ исчезает
        await store.DisposeAsync();
        var gone = await fixture.Gateway.RangeAsync(fixture.Endpoint, "/kafkaworker/api/", cts.Token);
        gone.Value.Should().BeEmpty();
    }
}
