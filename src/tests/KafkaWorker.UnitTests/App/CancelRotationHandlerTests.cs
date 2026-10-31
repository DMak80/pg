using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using KafkaWorker.App.Api.Operations;
using KafkaWorker.UnitTests.Provisioning;
using Xunit;

namespace KafkaWorker.UnitTests.App;

// Отмены живых заявок ротаций (arch/02 §10.2-18/19/20, u17): неканоническое
// имя → 404; заявки нет → NotFound; заявка есть, журнала нет → успех
// (ключ удалён условным txn); журнал в мутационной фазе → InProgress (409);
// журнал в терминальной фазе (done) → успех. Фазы — parameterized по спискам
// мутационных/безопасных фаз (дрейф предиката ротатора ловится тестом).
// Гонка клэйма: запись журнала между гвардом и txn (compare по mod_revision
// не сошёлся) → перечитанный гвард → 409. CA-хендлер — зеркально.
public class CancelRotationHandlerTests
{
    private const string Ep = "http://etcd:2379";
    private const string Op = "ops";

    // Мутационные фазы пароль-ротации (константы PasswordRotator: app —
    // legacy-имена, admin — с префиксом) и CA-ротации (вне {done, waiting-*}).
    public static TheoryData<string, string> MutationPhases => new()
    {
        { "app", "phase-a" },
        { "app", "rotated-commit" },
        { "app", "phase-c" },
        { "admin", "admin:phase-a" },
        { "admin", "admin:rotated-commit" },
        { "admin", "admin:phase-c" },
        { "ca", "phase-p" },
        { "ca", "phase-d" },
        { "ca", "phase-r" },
        { "ca", "committed" },
        { "ca", "phase-c" },
    };

    // Безопасные (не мутационные) фазы: терминальные и waiting-передержки.
    public static TheoryData<string, string> SafePhases => new()
    {
        { "app", "done" },
        { "app", "waiting-cluster" },
        { "admin", "done" },
        { "admin", "admin:waiting-password-rotation" },
        { "ca", "done" },
        { "ca", "waiting-cluster" },
    };

    private static CancelAppRotationHandler AppHandler(Fakes.FakeEtcd etcd) => new(etcd, [Ep], NullLogger<CancelAppRotationHandler>.Instance);
    private static CancelAdminRotationHandler AdminHandler(Fakes.FakeEtcd etcd) => new(etcd, [Ep], NullLogger<CancelAdminRotationHandler>.Instance);
    private static CancelCaRotationHandler CaHandler(Fakes.FakeEtcd etcd) => new(etcd, [Ep], NullLogger<CancelCaRotationHandler>.Instance);

    // Неканоническое имя кластера — 404-исключение (до чтений etcd).
    [Fact]
    public async Task NonCanonicalName_ClusterNotFound()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        var handler = AppHandler(etcd);

        // Act
        var result = await handler.HandleAsync("Bad_Name", Op, TestContext.Current.CancellationToken);

        // Assert
        result.Error.Should().BeOfType<KafkaClusterNotFoundException>();
    }

    // Заявки нет — NotFound (идемпотентность повтора отсутствует осознанно).
    [Fact]
    public async Task NoTicket_NotFound()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        var handler = AppHandler(etcd);

        // Act
        var result = await handler.HandleAsync("plain", Op, TestContext.Current.CancellationToken);

        // Assert
        result.Error.Should().BeOfType<KafkaRotationNotFoundException>();
    }

    // Заявка есть, журнала нет — «не начата»: del ключа (txn по значению + mod_rev).
    [Fact]
    public async Task TicketWithoutJournal_Deleted()
    {
        // Arrange — живая заявка, work-журнала нет
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/kafkaworker/rotations/plain",
            """{"requested_unix":1756500000,"requested_by":"ops"}""");
        var handler = AppHandler(etcd);

        // Act
        var result = await handler.HandleAsync("plain", Op, TestContext.Current.CancellationToken);

        // Assert — успех, ключ заявки удалён
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        result.Value.Cluster.Should().Be("plain");
        var gone = await etcd.GetAsync(Ep, "/kafkaworker/rotations/plain",
            TestContext.Current.CancellationToken);
        gone.Value.Should().BeNull();
    }

    // Мутационная фаза журнала — начата: 409 InProgress, заявка не снимается.
    [Theory]
    [MemberData(nameof(MutationPhases))]
    public async Task MutationJournalPhase_InProgress(string role, string phase)
    {
        // Arrange — заявка роли + журнал в мутационной фазе
        var etcd = new Fakes.FakeEtcd();
        var ticketKey = role switch
        {
            "app" => "/kafkaworker/rotations/mid",
            "admin" => "/kafkaworker/admin_rotations/mid",
            _ => "/kafkaworker/ca_rotations/mid",
        };
        var op = role == "ca" ? "rotate-ca" : "rotate";
        etcd.Seed(ticketKey, """{"requested_unix":1756500000,"requested_by":"ops"}""");
        etcd.Seed($"/kafkaworker/work/mid",
            $$"""{"op":"{{op}}","phase":"{{phase}}","instance":"i1","updated_unix":1756500100}""");

        // Act
        var result = await (role switch
        {
            "app" => AppHandler(etcd).HandleAsync("mid", Op, TestContext.Current.CancellationToken),
            "admin" => AdminHandler(etcd).HandleAsync("mid", Op, TestContext.Current.CancellationToken),
            _ => CaHandler(etcd).HandleAsync("mid", Op, TestContext.Current.CancellationToken),
        });

        // Assert — начатая ротация доедет (D13): 409, заявка жива
        result.Error.Should().BeOfType<KafkaRotationInProgressException>(
            $"{role} фаза {phase} должна блокировать отмену");
        var kept = await etcd.GetAsync(Ep, ticketKey, TestContext.Current.CancellationToken);
        kept.Value.Should().NotBeNull("заявка не снимается при живой мутации");
    }

    // Безопасная фаза журнала (терминальная/waiting) — «не начата»: отмена
    // разрешена, ключ удалён.
    [Theory]
    [MemberData(nameof(SafePhases))]
    public async Task SafeJournalPhase_Deleted(string role, string phase)
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        var ticketKey = role switch
        {
            "app" => "/kafkaworker/rotations/finished",
            "admin" => "/kafkaworker/admin_rotations/finished",
            _ => "/kafkaworker/ca_rotations/finished",
        };
        var op = role == "ca" ? "rotate-ca" : "rotate";
        etcd.Seed(ticketKey, """{"requested_unix":1756500000,"requested_by":"ops"}""");
        etcd.Seed($"/kafkaworker/work/finished",
            $$"""{"op":"{{op}}","phase":"{{phase}}","instance":"i1","updated_unix":1756500200}""");

        // Act
        var result = await (role switch
        {
            "app" => AppHandler(etcd).HandleAsync("finished", Op, TestContext.Current.CancellationToken),
            "admin" => AdminHandler(etcd).HandleAsync("finished", Op, TestContext.Current.CancellationToken),
            _ => CaHandler(etcd).HandleAsync("finished", Op, TestContext.Current.CancellationToken),
        });

        // Assert
        result.IsSuccess.Should().BeTrue($"{role} фаза {phase} не блокирует отмену: {result.Error?.Message}");
        var gone = await etcd.GetAsync(Ep, ticketKey, TestContext.Current.CancellationToken);
        gone.Value.Should().BeNull();
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
        var app = await AppHandler(etcd).HandleAsync("mix", Op, TestContext.Current.CancellationToken);
        var admin = await AdminHandler(etcd).HandleAsync("mix", Op, TestContext.Current.CancellationToken);

        // Assert — app «не начата» (её фазы не шли), admin «начата»
        app.IsSuccess.Should().BeTrue(app.Error?.Message);
        admin.Error.Should().BeOfType<KafkaRotationInProgressException>();
    }

    // Гонка клэйма (TOCTOU, arch/02 §10.2): ротатор пишет журнальную фазу
    // МЕЖДУ гвардом и txn → compare по mod_revision журнала не сходится →
    // перечитанный гвард видит мутационную фазу → 409, заявка НЕ удалена.
    [Fact]
    public async Task Race_JournalClaimedBetweenGuardAndTxn_InProgress()
    {
        // Arrange — журнал «не начата» (waiting) на момент гварда; первый txn
        // ломается записью мутационной фазы в журнал (OnTxnBeforeCompare).
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/kafkaworker/rotations/race",
            """{"requested_unix":1756500000,"requested_by":"ops"}""");
        etcd.Seed("/kafkaworker/work/race",
            """{"op":"rotate","phase":"waiting-cluster","instance":"i1","updated_unix":1756500050}""");
        var claimed = false;
        etcd.OnTxnBeforeCompare = _ =>
        {
            if (claimed)
                return;
            claimed = true;
            etcd.Seed("/kafkaworker/work/race",
                """{"op":"rotate","phase":"phase-a","instance":"i1","updated_unix":1756500099}""");
        };
        var handler = AppHandler(etcd);

        // Act
        var result = await handler.HandleAsync("race", Op, TestContext.Current.CancellationToken);

        // Assert — гвард перечитал журнал: мутация началась → 409, ключ жив
        result.Error.Should().BeOfType<KafkaRotationInProgressException>(
            "клэйм между гвардом и txn обязан закрываться compare по журналу");
        var kept = await etcd.GetAsync(Ep, "/kafkaworker/rotations/race",
            TestContext.Current.CancellationToken);
        kept.Value.Should().NotBeNull("начатая ротация доедет — заявка не снимается");
    }

    // Гонка со сменой ЗНАЧЕНИЯ заявки (без смены журнала): compare по значению
    // не сходится — ветка повторного чтения: журнал «не начата» → повторный
    // txn по свежему значению → успех.
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
        var handler = AppHandler(etcd);

        // Act
        var result = await handler.HandleAsync("race", Op, TestContext.Current.CancellationToken);

        // Assert — повторный txn удалил ключ по свежему значению
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var gone = await etcd.GetAsync(Ep, "/kafkaworker/rotations/race",
            TestContext.Current.CancellationToken);
        gone.Value.Should().BeNull();
    }

    // Записи аудита отмены — в host-лог воркера (arch/02 §10.2): ILogger
    // факультативен (null в юнит-тестах допустим).
    [Fact]
    public async Task NullLogger_IsAccepted()
    {
        // Arrange — успех без логгера не падает
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/kafkaworker/rotations/logless",
            """{"requested_unix":1756500000,"requested_by":"ops"}""");
        var handler = new CancelAppRotationHandler(etcd, [Ep], NullLogger<CancelAppRotationHandler>.Instance);

        // Act
        var result = await handler.HandleAsync("logless", Op, TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
    }
}
