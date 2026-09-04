using System.Diagnostics.Metrics;
using FluentAssertions;
using Shared.Metrics.Worker;

namespace Shared.Metrics.UnitTests;

// Юнит-тесты семантики инструментов воркер-паттерна (arch/18 §2.2): тики по ok,
// фазы (first-seen/сброс), терминальные/подавленные ops, возраст снапшота, клэймы.
public sealed class WorkerMetricsInstrumentationTests
{
    // Собственный FakeTimeProvider (новый пакет НЕ тащим, CPM чистый).
    private sealed class FakeTimeProvider : TimeProvider
    {
        public DateTimeOffset Now;
        public override DateTimeOffset GetUtcNow() => Now;
    }

    [Fact]
    public void LoopTick_OkTrue_UpdatesLastSuccess()
    {
        // Arrange
        var clock = new FakeTimeProvider { Now = DateTimeOffset.UnixEpoch.AddSeconds(1000) };
        using var meter = new Meter("TestWorker");
        using var sut = new WorkerMetricsInstrumentation(meter, clock);

        // Act
        sut.LoopTick("reconcile", ok: true);

        // Assert
        sut.DebugSnapshot().LastSuccess["reconcile"].Should().Be(1000);
    }

    [Fact]
    public void LoopTick_OkFalse_DoesNotMoveLastSuccess()
    {
        // Arrange
        var clock = new FakeTimeProvider { Now = DateTimeOffset.UnixEpoch.AddSeconds(1000) };
        using var meter = new Meter("TestWorker");
        using var sut = new WorkerMetricsInstrumentation(meter, clock);
        sut.LoopTick("reconcile", ok: true);

        // Act
        clock.Now = DateTimeOffset.UnixEpoch.AddSeconds(1010);
        sut.LoopTick("reconcile", ok: false);

        // Assert: ошибочный тик не двигает last_success (алерт «цикл умер» честный)
        sut.DebugSnapshot().LastSuccess["reconcile"].Should().Be(1000);
        sut.DebugSnapshot().LoopTicks[("reconcile", false)].Should().Be(1);
    }

    [Fact]
    public void ProcessPhase_SamePhase_KeepsFirstSeen()
    {
        // Arrange
        var t0 = DateTimeOffset.UnixEpoch.AddSeconds(5000);
        using var meter = new Meter("TestWorker");
        using var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);
        sut.ProcessPhase("demo", "provisioning", "started", t0);

        // Act: повторная запись той же фазы (журнал пишет фазу каждый тик)
        sut.ProcessPhase("demo", "provisioning", "started", t0.AddMinutes(5));

        // Assert: first-seen не сбрасывается — возраст фазы растёт честно
        sut.DebugSnapshot().Phases[("demo", "provisioning")].StartedAt.Should().Be(t0);
    }

    [Fact]
    public void ProcessFinished_RemovesPhaseSeries()
    {
        // Arrange
        using var meter = new Meter("TestWorker");
        using var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);
        sut.ProcessPhase("demo", "provisioning", "started", DateTimeOffset.UnixEpoch);

        // Act
        sut.ProcessFinished("demo", "provisioning");

        // Assert: серия сброшена — кардинальность только активные кластеры (M1)
        sut.DebugSnapshot().Phases.Should().BeEmpty();
    }

    [Fact]
    public void OnJournalPhase_FinalPhases_FinishAndCountOperation()
    {
        // Arrange: терминальные фазы фактического словаря (ревью Ф4-1):
        // done/failed/crashed/rejected/cancelled — все обязаны закрывать серию,
        // иначе вечная серия → ложный ProcessPhaseStuck.
        using var meter = new Meter("TestWorker");
        foreach (var phase in new[] { "done", "failed", "crashed", "rejected", "cancelled" })
        {
            using var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);
            sut.OnJournalPhase("demo", "move", "planned");

            // Act
            sut.OnJournalPhase("demo", "move", phase);

            // Assert: серия закрыта; операция посчитана (done → ok, прочие → error)
            sut.DebugSnapshot().Phases.Should().BeEmpty();
            var result = phase == "done" ? "ok" : "error";
            sut.DebugSnapshot().Operations[("move", result)].Should().Be(1);
        }
    }

    [Fact]
    public void OnJournalPhase_Rejected_MoveAndAbort_CloseSeries()
    {
        // Arrange: регрессия ревью Ф4-1 — rejected реален в словаре (MoveProcess:958,
        // AbortSequence:378, TopicSync:295): процесс, завершившийся rejected, обязан
        // получить ProcessFinished, иначе серия вечная.
        using var meter = new Meter("TestWorker");
        using var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);
        sut.OnJournalPhase("demo", "move", "post-flip");

        // Act
        sut.OnJournalPhase("demo", "move", "rejected");

        // Assert
        sut.DebugSnapshot().Phases.Should().BeEmpty();
        sut.DebugSnapshot().Operations[("move", "error")].Should().Be(1);
    }

    [Fact]
    public void OnJournalPhase_Skipped_IsIntermediate_DoesNotCloseSeries()
    {
        // Arrange: skipped у усыновления — ПРОМЕЖУТОЧНАЯ (AdoptionProcess.cs:128:
        // после skipped процесс продолжается и завершается done:180/failed:488).
        // Если объявить skipped терминальной — задвоится операция и порвётся живая фаза.
        using var meter = new Meter("TestWorker");
        using var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);
        sut.OnJournalPhase("demo", "adopt", "started");

        // Act
        sut.OnJournalPhase("demo", "adopt", "skipped");
        sut.OnJournalPhase("demo", "adopt", "repaired-portalloc");

        // Assert: серия жива (сменилась фаза, не закрылась); операция не задвоена
        sut.DebugSnapshot().Phases[("demo", "adopt")].Phase.Should().Be("repaired-portalloc");
        sut.DebugSnapshot().Operations.Should().BeEmpty();
    }

    [Fact]
    public void OnJournalPhase_SuppressedOps_EmitNoPhaseSeries()
    {
        // Arrange: ревью Ф4-2 — supervise (стационарные записи, часть через
        // WriteSupervisionAsync мимо события) и evacuate (только waiting-*) не имеют
        // терминальной фазы: фазовые серии для них НЕ эмитим — иначе вечно горящий
        // ProcessPhaseStuck; живость надзора закрывает WorkerLoopStalled.
        using var meter = new Meter("TestWorker");
        using var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);

        // Act
        sut.OnJournalPhase("demo", "supervise", "dcs-converge");
        sut.OnJournalPhase("demo", "evacuate", "waiting-alive");

        // Assert: подавлены полностью — ни серий, ни операций
        sut.DebugSnapshot().Phases.Should().BeEmpty();
        sut.DebugSnapshot().Operations.Should().BeEmpty();
    }

    [Fact]
    public void SnapshotTaken_AgeComputed_FromTimeProvider()
    {
        // Arrange
        var clock = new FakeTimeProvider { Now = DateTimeOffset.UnixEpoch.AddHours(3) };
        using var meter = new Meter("TestWorker");
        using var sut = new WorkerMetricsInstrumentation(meter, clock);
        sut.SnapshotTaken(DateTimeOffset.UnixEpoch.AddHours(1));

        // Act & Assert: возраст от TimeProvider (7200с), а не от времени записи
        sut.DebugSnapshot().SnapshotAgeSeconds.Should().Be(7200);
    }

    [Fact]
    public void ClaimsHeld_LastValueWins()
    {
        // Arrange
        using var meter = new Meter("TestWorker");
        using var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);

        // Act
        sut.ClaimsHeld(5);
        sut.ClaimsHeld(3);

        // Assert: гейдж хранит последнее значение
        sut.DebugSnapshot().ClaimsHeld.Should().Be(3);
    }
}
