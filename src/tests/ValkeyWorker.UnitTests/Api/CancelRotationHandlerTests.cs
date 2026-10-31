using FluentAssertions;
using ValkeyWorker.App.Api.Operations;
using ValkeyWorker.UnitTests.Provisioning;
using Xunit;

namespace ValkeyWorker.UnitTests.Api;

// Отмены живых заявок ротаций valkey (arch/02 §11.2-7/8, u17): неканоническое
// имя → 404; заявки нет → NotFound; заявка есть, журнала нет → успех (ключ
// удалён условным txn); журнал op=rotate в мутационной фазе (e1-added) →
// InProgress (409); журнал done → успех. CA-хендлер зеркально
// (op=rotate-ca, мутационная фаза phase-p). AAA.
public class CancelRotationHandlerTests
{
    private const string Ep = "http://etcd:2379";

    // Неканоническое имя кластера — 404-исключение.
    [Fact]
    public async Task NonCanonicalName_ClusterNotFound()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        var handler = new CancelPasswordRotationHandler(etcd, [Ep]);

        // Act
        var result = await handler.HandleAsync("Bad_Name", TestContext.Current.CancellationToken);

        // Assert
        result.Error.Should().BeOfType<ValkeyClusterNotFoundException>();
    }

    // Заявки нет — NotFound.
    [Fact]
    public async Task NoTicket_NotFound()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        var handler = new CancelPasswordRotationHandler(etcd, [Ep]);

        // Act
        var result = await handler.HandleAsync("plain", TestContext.Current.CancellationToken);

        // Assert
        result.Error.Should().BeOfType<ValkeyRotationNotFoundException>();
    }

    // Заявка есть, журнала нет — «не начата»: del ключа.
    [Fact]
    public async Task TicketWithoutJournal_Deleted()
    {
        // Arrange — ключ один на обе роли (arch/02 §11.2-7)
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/valkeyworker/rotations/plain",
            """{"role":"app","requested_unix":1756500000,"requested_by":"ops"}""");
        var handler = new CancelPasswordRotationHandler(etcd, [Ep]);

        // Act
        var result = await handler.HandleAsync("plain", TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        result.Value.Cluster.Should().Be("plain");
        var gone = await etcd.GetAsync(Ep, "/valkeyworker/rotations/plain",
            TestContext.Current.CancellationToken);
        gone.Value.Should().BeNull();
    }

    // Журнал op=rotate в мутационной фазе e1-added — начата: 409.
    [Fact]
    public async Task PasswordJournalE1Added_InProgress()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/valkeyworker/rotations/mid",
            """{"role":"app","requested_unix":1756500000,"requested_by":"ops"}""");
        etcd.Seed("/valkeyworker/work/mid",
            """{"op":"rotate","phase":"e1-added","instance":"i1","updated_unix":1756500100}""");
        var handler = new CancelPasswordRotationHandler(etcd, [Ep]);

        // Act
        var result = await handler.HandleAsync("mid", TestContext.Current.CancellationToken);

        // Assert — заявка остаётся (начатая ротация доедет, D13)
        result.Error.Should().BeOfType<ValkeyRotationInProgressException>();
        var kept = await etcd.GetAsync(Ep, "/valkeyworker/rotations/mid",
            TestContext.Current.CancellationToken);
        kept.Value.Should().NotBeNull();
    }

    // Журнал op=rotate в терминальной фазе done — «не начата»: успех.
    [Fact]
    public async Task PasswordJournalDone_Deleted()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/valkeyworker/rotations/finished",
            """{"role":"admin","requested_unix":1756500000,"requested_by":"ops"}""");
        etcd.Seed("/valkeyworker/work/finished",
            """{"op":"rotate","phase":"done","instance":"i1","updated_unix":1756500200}""");
        var handler = new CancelPasswordRotationHandler(etcd, [Ep]);

        // Act
        var result = await handler.HandleAsync("finished", TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var gone = await etcd.GetAsync(Ep, "/valkeyworker/rotations/finished",
            TestContext.Current.CancellationToken);
        gone.Value.Should().BeNull();
    }

    // CA-хендлер: журнал rotate-ca в мутационной фазе phase-p → 409.
    [Fact]
    public async Task CaJournalPhaseP_InProgress()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/valkeyworker/ca_rotations/ca1",
            """{"requested_unix":1756500000,"requested_by":"ops"}""");
        etcd.Seed("/valkeyworker/work/ca1",
            """{"op":"rotate-ca","phase":"phase-p","instance":"i1","updated_unix":1756500100}""");

        // Act
        var result = await new CancelValkeyCaRotationHandler(etcd, [Ep])
            .HandleAsync("ca1", TestContext.Current.CancellationToken);

        // Assert
        result.Error.Should().BeOfType<ValkeyRotationInProgressException>();
    }

    // CA-хендлер: журнал done → успех (ключ удалён).
    [Fact]
    public async Task CaJournalDone_Deleted()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/valkeyworker/ca_rotations/ca2",
            """{"requested_unix":1756500000,"requested_by":"ops"}""");
        etcd.Seed("/valkeyworker/work/ca2",
            """{"op":"rotate-ca","phase":"done","instance":"i1","updated_unix":1756500200}""");

        // Act
        var result = await new CancelValkeyCaRotationHandler(etcd, [Ep])
            .HandleAsync("ca2", TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var gone = await etcd.GetAsync(Ep, "/valkeyworker/ca_rotations/ca2",
            TestContext.Current.CancellationToken);
        gone.Value.Should().BeNull();
    }
}
