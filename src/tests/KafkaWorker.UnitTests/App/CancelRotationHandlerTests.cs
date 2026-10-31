using FluentAssertions;
using KafkaWorker.App.Api.Operations;
using KafkaWorker.UnitTests.Provisioning;
using Xunit;

namespace KafkaWorker.UnitTests.App;

// Отмены живых заявок ротаций (arch/02 §10.2-18/19/20, u17): неканоническое
// имя → 404; заявки нет → NotFound; заявка есть, журнала нет → успех
// (ключ удалён условным txn); журнал в мутационной фазе → InProgress (409);
// журнал в терминальной фазе (done) → успех. CA-хендлер — зеркально
// (op=rotate-ca, мутационная фаза phase-p). AAA.
public class CancelRotationHandlerTests
{
    private const string Ep = "http://etcd:2379";

    // Неканоническое имя кластера — 404-исключение (до чтений etcd).
    [Fact]
    public async Task NonCanonicalName_ClusterNotFound()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        var handler = new CancelAppRotationHandler(etcd, [Ep]);

        // Act
        var result = await handler.HandleAsync("Bad_Name", TestContext.Current.CancellationToken);

        // Assert
        result.Error.Should().BeOfType<KafkaClusterNotFoundException>();
    }

    // Заявки нет — NotFound (идемпотентность повтора отсутствует осознанно).
    [Fact]
    public async Task NoTicket_NotFound()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        var handler = new CancelAppRotationHandler(etcd, [Ep]);

        // Act
        var result = await handler.HandleAsync("plain", TestContext.Current.CancellationToken);

        // Assert
        result.Error.Should().BeOfType<KafkaRotationNotFoundException>();
    }

    // Заявка есть, журнала нет — «не начата»: del ключа (txn по значению).
    [Fact]
    public async Task TicketWithoutJournal_Deleted()
    {
        // Arrange — живая заявка, work-журнала нет
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/kafkaworker/rotations/plain",
            """{"requested_unix":1756500000,"requested_by":"ops"}""");
        var handler = new CancelAppRotationHandler(etcd, [Ep]);

        // Act
        var result = await handler.HandleAsync("plain", TestContext.Current.CancellationToken);

        // Assert — успех, ключ заявки удалён
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        result.Value.Cluster.Should().Be("plain");
        var gone = await etcd.GetAsync(Ep, "/kafkaworker/rotations/plain",
            TestContext.Current.CancellationToken);
        gone.Value.Should().BeNull();
    }

    // Журнал op=rotate в мутационной фазе роли app — начата: 409 InProgress.
    [Fact]
    public async Task PasswordJournalPhaseA_InProgress()
    {
        // Arrange — заявка + журнал в фазе phase-a
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/kafkaworker/rotations/mid",
            """{"requested_unix":1756500000,"requested_by":"ops"}""");
        etcd.Seed("/kafkaworker/work/mid",
            """{"op":"rotate","phase":"phase-a","instance":"i1","updated_unix":1756500100}""");
        var handler = new CancelAppRotationHandler(etcd, [Ep]);

        // Act
        var result = await handler.HandleAsync("mid", TestContext.Current.CancellationToken);

        // Assert — начатая ротация доедет (D13): 409
        result.Error.Should().BeOfType<KafkaRotationInProgressException>();
        var kept = await etcd.GetAsync(Ep, "/kafkaworker/rotations/mid",
            TestContext.Current.CancellationToken);
        kept.Value.Should().NotBeNull("заявка не снимается при живой мутации");
    }

    // Журнал op=rotate в admin-фазе — заявка app не блокируется (роли раздельны),
    // а заявка admin блокируется admin:-фазой.
    [Fact]
    public async Task AdminPhase_BlocksAdminTicketOnly()
    {
        // Arrange — заявки обеих ролей + журнал в admin:phase-a
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/kafkaworker/rotations/mix",
            """{"requested_unix":1756500000,"requested_by":"ops"}""");
        etcd.Seed("/kafkaworker/admin_rotations/mix",
            """{"requested_unix":1756500001,"requested_by":"ops"}""");
        etcd.Seed("/kafkaworker/work/mix",
            """{"op":"rotate","phase":"admin:phase-a","instance":"i1","updated_unix":1756500100}""");

        // Act
        var app = await new CancelAppRotationHandler(etcd, [Ep])
            .HandleAsync("mix", TestContext.Current.CancellationToken);
        var admin = await new CancelAdminRotationHandler(etcd, [Ep])
            .HandleAsync("mix", TestContext.Current.CancellationToken);

        // Assert — app «не начата» (её фазы не шли), admin «начата»
        app.IsSuccess.Should().BeTrue(app.Error?.Message);
        admin.Error.Should().BeOfType<KafkaRotationInProgressException>();
    }

    // Журнал в терминальной фазе (done) — «не начата»: отмена разрешена.
    [Fact]
    public async Task PasswordJournalDone_Deleted()
    {
        // Arrange — заявка + журнал done (ротация уже исполнялась ранее)
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/kafkaworker/rotations/finished",
            """{"requested_unix":1756500000,"requested_by":"ops"}""");
        etcd.Seed("/kafkaworker/work/finished",
            """{"op":"rotate","phase":"done","instance":"i1","updated_unix":1756500200}""");
        var handler = new CancelAppRotationHandler(etcd, [Ep]);

        // Act
        var result = await handler.HandleAsync("finished", TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var gone = await etcd.GetAsync(Ep, "/kafkaworker/rotations/finished",
            TestContext.Current.CancellationToken);
        gone.Value.Should().BeNull();
    }

    // CA-хендлер зеркально: журнал rotate-ca в мутационной фазе phase-p → 409;
    // done → успех.
    [Fact]
    public async Task CaJournalPhaseP_InProgress()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/kafkaworker/ca_rotations/ca1",
            """{"requested_unix":1756500000,"requested_by":"ops"}""");
        etcd.Seed("/kafkaworker/work/ca1",
            """{"op":"rotate-ca","phase":"phase-p","instance":"i1","updated_unix":1756500100}""");

        // Act
        var result = await new CancelCaRotationHandler(etcd, [Ep])
            .HandleAsync("ca1", TestContext.Current.CancellationToken);

        // Assert
        result.Error.Should().BeOfType<KafkaRotationInProgressException>();
    }

    [Fact]
    public async Task CaJournalDone_Deleted()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/kafkaworker/ca_rotations/ca2",
            """{"requested_unix":1756500000,"requested_by":"ops"}""");
        etcd.Seed("/kafkaworker/work/ca2",
            """{"op":"rotate-ca","phase":"done","instance":"i1","updated_unix":1756500200}""");

        // Act
        var result = await new CancelCaRotationHandler(etcd, [Ep])
            .HandleAsync("ca2", TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var gone = await etcd.GetAsync(Ep, "/kafkaworker/ca_rotations/ca2",
            TestContext.Current.CancellationToken);
        gone.Value.Should().BeNull();
    }

    // Гонка с воркером между чтением и txn: compare по значению не сходится
    // (заявку переписали) — ветка повторного чтения: журнал «не начата»
    // (waiting-фаза) → повторный txn по свежему значению → успех.
    [Fact]
    public async Task Race_ValueChanged_RetryTxnByNewValue()
    {
        // Arrange — OnTxnBeforeCompare переписывает ключ ОДИН раз (первый txn
        // ломается compare, второй проходит по новому значению)
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/kafkaworker/rotations/race",
            """{"requested_unix":1756500000,"requested_by":"ops"}""");
        etcd.Seed("/kafkaworker/work/race",
            """{"op":"rotate","phase":"waiting-password-rotation","instance":"i1","updated_unix":1756500050}""");
        var raced = false;
        etcd.OnTxnBeforeCompare = _ =>
        {
            if (raced)
                return;
            raced = true;
            etcd.Seed("/kafkaworker/rotations/race",
                """{"requested_unix":1756500000,"requested_by":"other"}""");
        };
        var handler = new CancelAppRotationHandler(etcd, [Ep]);

        // Act
        var result = await handler.HandleAsync("race", TestContext.Current.CancellationToken);

        // Assert — повторный txn удалил ключ по свежему значению
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var gone = await etcd.GetAsync(Ep, "/kafkaworker/rotations/race",
            TestContext.Current.CancellationToken);
        gone.Value.Should().BeNull();
    }
}
