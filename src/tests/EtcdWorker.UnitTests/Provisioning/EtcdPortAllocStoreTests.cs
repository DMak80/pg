using System.Text.Json;
using EtcdWorker.Provisioning;
using Shared.Etcd.Client;

namespace EtcdWorker.UnitTests.Provisioning;

// Юниты portalloc-стора etcd-нод на фейк-гейтвее (словарь): allocate на пустом
// ключе — наименьший свободный порт диапазона, busyPorts (docker) не выбираются,
// повторный allocate нового k — следующий порт, RMW-гонка между Get и txn —
// ретраи до успеха, release убирает поле и идемпотентен, запись — точный
// JSON {"1":{"host":"h","client":18100}} в единственном ключе /etcdworker/portalloc.
public class EtcdPortAllocStoreTests
{
    // Единственный ключ стора с картой k → NodePort (создаётся параллельно).
    private const string Key = "/etcdworker/portalloc";

    private const int From = 18100;
    private const int To = 18199;

    private static EtcdPortAllocStore NewStore(Fakes.FakeEtcdGateway fake)
        => new(fake, ["http://localhost:2379"]);

    [Fact]
    public async Task Allocate_EmptyKey_ReturnsSmallestFreePort()
    {
        // Arrange — ключа portalloc нет, docker-busy портов нет
        var fake = new Fakes.FakeEtcdGateway(new Dictionary<string, string>());
        var store = NewStore(fake);

        // Act
        var result = await store.AllocateAsync(1, "h", From, To, new HashSet<int>(), CancellationToken.None);

        // Assert — наименьший свободный порт диапазона с хостом из заявки
        result.IsSuccess.Should().BeTrue();
        result.Value.Client.Should().Be(From);
        result.Value.Host.Should().Be("h");
    }

    [Fact]
    public async Task Allocate_BusyDockerPorts_SkipsBusy()
    {
        // Arrange — docker занял два порта начала диапазона
        var fake = new Fakes.FakeEtcdGateway(new Dictionary<string, string>());
        var store = NewStore(fake);
        var busy = new HashSet<int> { 18100, 18101 };

        // Act
        var result = await store.AllocateAsync(1, "h", From, To, busy, CancellationToken.None);

        // Assert — занятые порты пропущены, выделен первый свободный
        result.IsSuccess.Should().BeTrue();
        result.Value.Client.Should().Be(18102);
    }

    [Fact]
    public async Task Allocate_SecondKey_TakesNextPort()
    {
        // Arrange — пустой ключ portalloc
        var fake = new Fakes.FakeEtcdGateway(new Dictionary<string, string>());
        var store = NewStore(fake);

        // Act — два allocate разных k подряд
        var first = await store.AllocateAsync(1, "h1", From, To, new HashSet<int>(), CancellationToken.None);
        var second = await store.AllocateAsync(2, "h2", From, To, new HashSet<int>(), CancellationToken.None);

        // Assert — второму k достался следующий порт, в ключе обе записи
        first.IsSuccess.Should().BeTrue();
        first.Value.Client.Should().Be(18100);
        second.IsSuccess.Should().BeTrue();
        second.Value.Client.Should().Be(18101);
        var all = await store.ReadAllAsync(CancellationToken.None);
        all.IsSuccess.Should().BeTrue();
        all.Value.Keys.Should().Equal(1, 2);
        all.Value[1].Host.Should().Be("h1");
        all.Value[2].Host.Should().Be("h2");
    }

    [Fact]
    public async Task Allocate_RaceBetweenGetAndTxn_RetriesUntilSuccess()
    {
        // Arrange — гонка RMW: первая txn проигрывает конкуренту (он успел
        // создать ключ и занять порт 18100), повторные txn успешны
        var fake = new Fakes.FakeEtcdGateway(new Dictionary<string, string>());
        var txnCalls = 0;
        fake.TxnOverride = req =>
        {
            txnCalls++;
            if (txnCalls == 1)
            {
                // Конкурент между Get и txn стора: создал ключ со своим портом.
                fake.Store[Key] = """{"9":{"host":"other","client":18100}}""";
                return Result<TxnResult>.Success(new TxnResult(false));
            }

            // Ретрай: compare сошёлся — применяем success-ветку txn к словарю.
            foreach (var op in req.Success)
                if (op is TxnOp.Put put)
                    fake.Store[put.Key] = put.Value;
            return Result<TxnResult>.Success(new TxnResult(true));
        };
        var store = NewStore(fake);

        // Act
        var result = await store.AllocateAsync(1, "h", From, To, new HashSet<int>(), CancellationToken.None);

        // Assert — на ретрае порт конкурента пропущен, в записи оба k
        result.IsSuccess.Should().BeTrue();
        result.Value.Client.Should().Be(18101);
        txnCalls.Should().BeGreaterThanOrEqualTo(2);
        fake.Store[Key].Should().Contain("\"9\"");
        fake.Store[Key].Should().Contain("\"1\"");
    }

    [Fact]
    public async Task Release_RemovesField_SecondReleaseSucceeds()
    {
        // Arrange — в ключе две записи (k=1 и k=2)
        var fake = new Fakes.FakeEtcdGateway(new Dictionary<string, string>
        {
            [Key] = """{"1":{"host":"h","client":18100},"2":{"host":"h","client":18101}}""",
        });
        var store = NewStore(fake);

        // Act
        var release = await store.ReleaseAsync(1, CancellationToken.None);

        // Assert — поле k=1 исчезло, k=2 остался
        release.IsSuccess.Should().BeTrue();
        var after = await store.ReadAllAsync(CancellationToken.None);
        after.IsSuccess.Should().BeTrue();
        after.Value.Keys.Should().Equal(2);
        var fields = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(fake.Store[Key]);
        fields.Should().NotBeNull();
        fields!.Keys.Should().Equal("2");

        // Act — повторный release уже отсутствующего k
        var repeat = await store.ReleaseAsync(1, CancellationToken.None);

        // Assert — идемпотентен: успех, карта не изменилась
        repeat.IsSuccess.Should().BeTrue();
        var afterRepeat = await store.ReadAllAsync(CancellationToken.None);
        afterRepeat.IsSuccess.Should().BeTrue();
        afterRepeat.Value.Keys.Should().Equal(2);
    }

    [Fact]
    public async Task Allocate_SingleEntry_ExactJsonFormat()
    {
        // Arrange — пустой ключ portalloc
        var fake = new Fakes.FakeEtcdGateway(new Dictionary<string, string>());
        var store = NewStore(fake);

        // Act
        var result = await store.AllocateAsync(1, "h", From, To, new HashSet<int>(), CancellationToken.None);

        // Assert — запись в ключе ровно в каноничном JSON-формате
        result.IsSuccess.Should().BeTrue();
        fake.Store[Key].Should().Be("""{"1":{"host":"h","client":18100}}""");
    }
}
