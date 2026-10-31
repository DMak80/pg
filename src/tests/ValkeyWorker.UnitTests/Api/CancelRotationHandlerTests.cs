using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using ValkeyWorker.App.Api.Operations;
using ValkeyWorker.UnitTests.Provisioning;
using Xunit;

namespace ValkeyWorker.UnitTests.Api;

// Отмены живых заявок ротаций valkey (arch/02 §11.2-7/8, u17): неканоническое
// имя → 404; заявки нет → NotFound; заявка есть, журнала/стейта нет → успех
// (ключ удалён условным txn); журнал op=rotate в мутационной фазе (e1-added/
// e2-committed) и СТЕЙТ доигрывания e1-pending → InProgress (409); журнал done
// → успех. CA-хендлер зеркально (op=rotate-ca, мутационная фаза phase-p).
// Гонка клэйма: запись журнала/стейта между гвардом и txn (compare по
// mod_revision не сошёлся) → перечитанный гвард → 409. AAA.
public class CancelRotationHandlerTests
{
    private const string Ep = "http://etcd:2379";
    private const string Op = "ops";

    // Мутационные фазы E-ротации: журнал (e1-added/e2-committed) и стейт
    // доигрывания (вкл. e1-pending — точка невозврата E1). CA — вне
    // {done, waiting-*}.
    public static TheoryData<string, string> MutationPhases => new()
    {
        { "pw-journal", "e1-added" },
        { "pw-journal", "e2-committed" },
        { "pw-state", "e1-pending" },
        { "pw-state", "e1-added" },
        { "pw-state", "e2-committed" },
        { "ca", "phase-p" },
        { "ca", "phase-d" },
        { "ca", "phase-r" },
        { "ca", "phase-r/node1" },
        { "ca", "committed" },
    };

    // Безопасные фазы: терминальные и waiting-передержки.
    public static TheoryData<string, string> SafePhases => new()
    {
        { "pw-journal", "done" },
        { "pw-journal", "waiting-cluster" },
        { "ca", "done" },
        { "ca", "waiting-cluster" },
    };

    private static CancelPasswordRotationHandler PwHandler(Fakes.FakeEtcd etcd)
        => new(etcd, [Ep], NullLogger<CancelPasswordRotationHandler>.Instance);

    private static CancelValkeyCaRotationHandler CaHandler(Fakes.FakeEtcd etcd)
        => new(etcd, [Ep], NullLogger<CancelValkeyCaRotationHandler>.Instance);

    // Неканоническое имя кластера — 404-исключение.
    [Fact]
    public async Task NonCanonicalName_ClusterNotFound()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        var handler = PwHandler(etcd);

        // Act
        var result = await handler.HandleAsync("Bad_Name", Op, TestContext.Current.CancellationToken);

        // Assert
        result.Error.Should().BeOfType<ValkeyClusterNotFoundException>();
    }

    // Заявки нет — NotFound.
    [Fact]
    public async Task NoTicket_NotFound()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        var handler = PwHandler(etcd);

        // Act
        var result = await handler.HandleAsync("plain", Op, TestContext.Current.CancellationToken);

        // Assert
        result.Error.Should().BeOfType<ValkeyRotationNotFoundException>();
    }

    // Заявка есть, журнала/стейта нет — «не начата»: del ключа.
    [Fact]
    public async Task TicketWithoutJournalOrState_Deleted()
    {
        // Arrange — ключ один на обе роли (arch/02 §11.2-7)
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/valkeyworker/rotations/plain",
            """{"role":"app","requested_unix":1756500000,"requested_by":"ops"}""");
        var handler = PwHandler(etcd);

        // Act
        var result = await handler.HandleAsync("plain", Op, TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        result.Value.Cluster.Should().Be("plain");
        var gone = await etcd.GetAsync(Ep, "/valkeyworker/rotations/plain",
            TestContext.Current.CancellationToken);
        gone.Value.Should().BeNull();
    }

    // Мутационная фаза журнала или стейта доигрывания — начата: 409, заявка
    // не снимается.
    [Theory]
    [MemberData(nameof(MutationPhases))]
    public async Task MutationJournalOrStatePhase_InProgress(string kind, string phase)
    {
        // Arrange — заявка + журнал/стейт в мутационной фазе
        var etcd = new Fakes.FakeEtcd();
        var (ticketKey, isCa) = kind == "ca"
            ? ("/valkeyworker/ca_rotations/mid", true)
            : ("/valkeyworker/rotations/mid", false);
        etcd.Seed(ticketKey,
            isCa
                ? """{"requested_unix":1756500000,"requested_by":"ops"}"""
                : """{"role":"app","requested_unix":1756500000,"requested_by":"ops"}""");
        if (kind == "pw-journal")
            etcd.Seed("/valkeyworker/work/mid",
                $$"""{"op":"rotate","phase":"{{phase}}","instance":"i1","updated_unix":1756500100}""");
        else if (kind == "pw-state")
            etcd.Seed("/valkeyworker/work/mid/rotation",
                $$"""{"phase":"{{phase}}","role":"app","old":"o","new":"n"}""");
        else
            etcd.Seed("/valkeyworker/work/mid",
                $$"""{"op":"rotate-ca","phase":"{{phase}}","instance":"i1","updated_unix":1756500100}""");

        // Act
        var result = await (isCa
            ? CaHandler(etcd).HandleAsync("mid", Op, TestContext.Current.CancellationToken)
            : PwHandler(etcd).HandleAsync("mid", Op, TestContext.Current.CancellationToken));

        // Assert — начатая ротация доедет (D13): 409, заявка жива
        result.Error.Should().BeOfType<ValkeyRotationInProgressException>(
            $"{kind} фаза {phase} должна блокировать отмену");
        var kept = await etcd.GetAsync(Ep, ticketKey, TestContext.Current.CancellationToken);
        kept.Value.Should().NotBeNull("заявка не снимается при живой мутации");
    }

    // Безопасная фаза (терминальная/waiting) — «не начата»: отмена разрешена.
    [Theory]
    [MemberData(nameof(SafePhases))]
    public async Task SafePhase_Deleted(string kind, string phase)
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        var (ticketKey, isCa) = kind == "ca"
            ? ("/valkeyworker/ca_rotations/finished", true)
            : ("/valkeyworker/rotations/finished", false);
        etcd.Seed(ticketKey,
            isCa
                ? """{"requested_unix":1756500000,"requested_by":"ops"}"""
                : """{"role":"admin","requested_unix":1756500000,"requested_by":"ops"}""");
        etcd.Seed("/valkeyworker/work/finished",
            $$"""{"op":"{{(isCa ? "rotate-ca" : "rotate")}}","phase":"{{phase}}","instance":"i1","updated_unix":1756500200}""");

        // Act
        var result = await (isCa
            ? CaHandler(etcd).HandleAsync("finished", Op, TestContext.Current.CancellationToken)
            : PwHandler(etcd).HandleAsync("finished", Op, TestContext.Current.CancellationToken));

        // Assert
        result.IsSuccess.Should().BeTrue($"{kind} фаза {phase} не блокирует отмену: {result.Error?.Message}");
        var gone = await etcd.GetAsync(Ep, ticketKey, TestContext.Current.CancellationToken);
        gone.Value.Should().BeNull();
    }

    // Гонка клэйма E-ротации (TOCTOU, arch/02 §11.2): ротатор пишет СТЕЙТ
    // e1-pending (первая запись старта, до SETUSER) между гвардом и txn →
    // compare по mod_revision стейта не сходится → перечитанный гвард → 409.
    [Fact]
    public async Task Race_StateClaimedBetweenGuardAndTxn_InProgress()
    {
        // Arrange — гвард видит «чисто»; первый txn ломается записью стейта.
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/valkeyworker/rotations/race",
            """{"role":"app","requested_unix":1756500000,"requested_by":"ops"}""");
        etcd.Seed("/valkeyworker/work/race",
            """{"op":"supervise","phase":"supervising","instance":"i1","updated_unix":1756500050}""");
        var claimed = false;
        etcd.OnTxnBeforeCompare = _ =>
        {
            if (claimed)
                return;
            claimed = true;
            etcd.Seed("/valkeyworker/work/race/rotation",
                """{"phase":"e1-pending","role":"app","old":"o","new":"n"}""");
        };
        var handler = PwHandler(etcd);

        // Act
        var result = await handler.HandleAsync("race", Op, TestContext.Current.CancellationToken);

        // Assert — гвард перечитал стейт: мутация началась → 409, ключ жив
        result.Error.Should().BeOfType<ValkeyRotationInProgressException>(
            "стейт e1-pending между гвардом и txn обязан закрываться compare по стейту");
        var kept = await etcd.GetAsync(Ep, "/valkeyworker/rotations/race",
            TestContext.Current.CancellationToken);
        kept.Value.Should().NotBeNull("начатая ротация доедет — заявка не снимается");
    }

    // Гонка со сменой значения заявки (журнал/стейт не менялись): повторный
    // txn по свежему значению → успех.
    [Fact]
    public async Task Race_ValueChanged_RetryTxnByNewValue()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/valkeyworker/rotations/race",
            """{"role":"app","requested_unix":1756500000,"requested_by":"ops"}""");
        etcd.Seed("/valkeyworker/work/race",
            """{"op":"rotate","phase":"waiting-cluster","instance":"i1","updated_unix":1756500050}""");
        var raced = false;
        etcd.OnTxnBeforeCompare = _ =>
        {
            if (raced)
                return;
            raced = true;
            etcd.Seed("/valkeyworker/rotations/race",
                """{"role":"app","requested_unix":1756500000,"requested_by":"other"}""");
        };
        var handler = PwHandler(etcd);

        // Act
        var result = await handler.HandleAsync("race", Op, TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var gone = await etcd.GetAsync(Ep, "/valkeyworker/rotations/race",
            TestContext.Current.CancellationToken);
        gone.Value.Should().BeNull();
    }
}
