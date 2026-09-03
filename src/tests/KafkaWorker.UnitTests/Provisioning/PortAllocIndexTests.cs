using FluentAssertions;
using KafkaWorker.Core;
using KafkaWorker.Etcd.Client;
using KafkaWorker.Provisioning.Processes;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace KafkaWorker.UnitTests.Provisioning;

// PortAllocIndex (t91, arch/16 §2.1): busy = клиентские порты записей
// /kafkaworker/portalloc/* ЧУЖИХ кластеров; свой — исключается (закрепление,
// не занятость); чужой мусор любой формы (битый JSON, JSON без обязательных
// полей) — skip без ошибки (порт PgWorker-индекса, spec §3.2/§6).
public class PortAllocIndexTests
{
    private const string Ep = "http://etcd:2379";

    private static PortAllocIndex NewIndex(Fakes.FakeEtcd etcd)
        => new(etcd, [Ep], NullLogger<PortAllocIndex>.Instance);

    // AAA: записи чужих кластеров дают busy-кортежи (host, client) всех их нод.
    [Fact]
    public async Task ReadBusy_ForeignClusters_AreBusy()
    {
        // Arrange: два соседа по docker-хосту.
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/kafkaworker/portalloc/shop1",
            """{"broker1":{"host":"h1","client":16000},"broker2":{"host":"h1","client":16001}}""");
        etcd.Seed("/kafkaworker/portalloc/shop2",
            """{"broker1":{"host":"h2","client":16000}}""");

        // Act
        var busy = await NewIndex(etcd).ReadBusyAsync("events", CancellationToken.None);

        // Assert
        busy.IsSuccess.Should().BeTrue();
        busy.Value.Should().BeEquivalentTo(new HashSet<(string, int)>
        {
            ("h1", 16000), ("h1", 16001), ("h2", 16000),
        });
    }

    // AAA: свой кластер исключается — его portalloc переиспользуется аллокатором
    // как закрепление, а не занятость.
    [Fact]
    public async Task ReadBusy_OwnCluster_Excluded()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/kafkaworker/portalloc/events",
            """{"broker1":{"host":"h1","client":16000}}""");

        // Act
        var busy = await NewIndex(etcd).ReadBusyAsync("events", CancellationToken.None);

        // Assert: пусто — единственная запись принадлежит исключённому кластеру.
        busy.Value.Should().BeEmpty();
    }

    // AAA: битый JSON соседа — Warning + skip ключа: чужой мусор не роняет наш
    // provision (принцип PgWorker-индекса).
    [Fact]
    public async Task ReadBusy_MalformedNeighbour_Skipped()
    {
        // Arrange: один живой сосед, один битый.
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/kafkaworker/portalloc/shop1",
            """{"broker1":{"host":"h1","client":16000}}""");
        etcd.Seed("/kafkaworker/portalloc/broken", "{not-json");

        // Act
        var busy = await NewIndex(etcd).ReadBusyAsync("events", CancellationToken.None);

        // Assert: битый пропущен, валидный учтён.
        busy.IsSuccess.Should().BeTrue();
        busy.Value.Should().BeEquivalentTo(new HashSet<(string, int)> { ("h1", 16000) });
    }

    // AAA (ревью t91): валидный JSON соседа БЕЗ обязательных полей (host/client) —
    // skip ключа без ошибки: как и битый JSON, это чужой мусор, не повод ронять
    // наш тик в Failed (spec §3.2/§6 — эталон PgWorker не роняет чтение ни на
    // каком мусоре).
    [Fact]
    public async Task ReadBusy_ValidJsonMissingFields_Skipped()
    {
        // Arrange: сосед без поля host; сосед без поля client; валидный сосед.
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/kafkaworker/portalloc/nohost", """{"broker1":{"client":16000}}""");
        etcd.Seed("/kafkaworker/portalloc/noclient", """{"broker1":{"host":"h1"}}""");
        etcd.Seed("/kafkaworker/portalloc/shop1",
            """{"broker1":{"host":"h1","client":16005}}""");

        // Act
        var busy = await NewIndex(etcd).ReadBusyAsync("events", CancellationToken.None);

        // Assert: оба неполных ключа пропущены, валидный учтён.
        busy.IsSuccess.Should().BeTrue();
        busy.Value.Should().BeEquivalentTo(new HashSet<(string, int)> { ("h1", 16005) });
    }
}
