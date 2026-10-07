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
    public void BackupWalLag_Серии_по_шардам_null_удаляет()
    {
        // Arrange — два шарда под наблюдением контрольного прохода WalStreamProcess
        using var meter = new Meter("TestWorker");
        using var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);

        // Act — два наблюдения, затем снятие серии одного шарда
        sut.BackupWalLag("c1", "s1", 5);
        sut.BackupWalLag("c1", "s2", 0);
        sut.BackupWalLag("c1", "s1", null);

        // Assert — серия s1 исчезла, s2 осталась
        sut.DebugSnapshot().WalLag.Keys.Should().ContainSingle(k => k.Cluster == "c1" && k.Shard == "s2");
        sut.DebugSnapshot().WalLag[("c1", "s2")].Should().Be(0);
    }

    // AAA: counter pgworker_backup_verify_total{cluster,shard,result} — лейблы
    // cluster/shard добавлены (миграция серии t14, arch/18 §2.7): инкременты по тройке
    [Fact]
    public void BackupVerify_ИнкрементыПоКластеруШардуРезультату()
    {
        // Arrange
        using var meter = new Meter("TestWorker");
        using var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);

        // Act — три ok + один failed, два шарда
        sut.BackupVerify("c1", "s1", "ok");
        sut.BackupVerify("c1", "s1", "ok");
        sut.BackupVerify("c1", "s2", "ok");
        sut.BackupVerify("c1", "s2", "failed");

        // Assert — тройка (cluster, shard, result): s2/ok не смешался с s1/ok
        sut.DebugSnapshot().BackupVerifyTotals[("c1", "s1", "ok")].Should().Be(2);
        sut.DebugSnapshot().BackupVerifyTotals[("c1", "s2", "ok")].Should().Be(1);
        sut.DebugSnapshot().BackupVerifyTotals[("c1", "s2", "failed")].Should().Be(1);
    }

    // AAA: counter pgworker_backup_restore_total{cluster,shard,result} — исходы restore
    [Fact]
    public void BackupRestore_ИнкрементыПоИсходам()
    {
        // Arrange
        using var meter = new Meter("TestWorker");
        using var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);

        // Act — ok и failed разных шардов
        sut.BackupRestore("c1", "s1", "ok");
        sut.BackupRestore("c1", "s1", "failed");
        sut.BackupRestore("c2", "s1", "ok");

        // Assert
        var d = sut.DebugSnapshot();
        d.BackupRestoreTotals[("c1", "s1", "ok")].Should().Be(1);
        d.BackupRestoreTotals[("c1", "s1", "failed")].Should().Be(1);
        d.BackupRestoreTotals[("c2", "s1", "ok")].Should().Be(1);
    }

    // AAA: counter pgworker_backup_drill_total{cluster,shard,result} — исходы дрилов
    [Fact]
    public void BackupDrill_ИнкрементыПоИсходам()
    {
        // Arrange
        using var meter = new Meter("TestWorker");
        using var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);

        // Act
        sut.BackupDrill("c1", "s1", "ok");
        sut.BackupDrill("c1", "s1", "ok");
        sut.BackupDrill("c1", "s2", "failed");

        // Assert
        var d = sut.DebugSnapshot();
        d.BackupDrillTotals[("c1", "s1", "ok")].Should().Be(2);
        d.BackupDrillTotals[("c1", "s2", "failed")].Should().Be(1);
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

    // AAA: t14 — набор тика замещает стейт кластера ЦЕЛИКОМ: age null → age-серия
    // отсутствует, max_age пишется всегда, ушедший из набора шард не эмитится
    [Fact]
    public void BackupFullAge_ЗамещениеНабора_nullУбираетAge_maxAgeВсегда()
    {
        // Arrange — тик 1: s1 с валидным, s2 без
        using var meter = new Meter("TestWorker");
        using var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);
        sut.BackupFullAge("c1", new Dictionary<string, (long? LastValidUnix, long MaxAgeSec)>
        {
            ["s1"] = (9_000, 86_400),
            ["s2"] = (null, 86_400),
        });

        // Act — тик 2: s2 получил валидный, s1 ушёл из набора
        sut.BackupFullAge("c1", new Dictionary<string, (long? LastValidUnix, long MaxAgeSec)>
        {
            ["s2"] = (9_500, 43_200),
        });

        // Assert — стейт кластера = последний набор: s1 исчез, s2 обновлён
        var d = sut.DebugSnapshot();
        d.FullAgeFinishedUnix.Keys.Should().ContainSingle(k => k.Cluster == "c1" && k.Shard == "s2");
        d.FullAgeFinishedUnix[("c1", "s2")].Should().Be(9_500);
        d.FullMaxAge.Keys.Should().ContainSingle(k => k.Cluster == "c1" && k.Shard == "s2");
        d.FullMaxAge[("c1", "s2")].Should().Be(43_200);
    }

    // AAA: t14 — age-серия шарда исчезает при null-факте, max_age остаётся
    [Fact]
    public void BackupFullAge_nullУбираетТолькоAge_шардыРазныхКластеровНезависимы()
    {
        // Arrange — два кластера
        using var meter = new Meter("TestWorker");
        using var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);
        sut.BackupFullAge("c1", new Dictionary<string, (long? LastValidUnix, long MaxAgeSec)> { ["s1"] = (100, 10) });
        sut.BackupFullAge("c2", new Dictionary<string, (long? LastValidUnix, long MaxAgeSec)> { ["s1"] = (200, 20) });

        // Act — c1/s1 потерял валидный
        sut.BackupFullAge("c1", new Dictionary<string, (long? LastValidUnix, long MaxAgeSec)> { ["s1"] = (null, 10) });

        // Assert
        var d = sut.DebugSnapshot();
        d.FullAgeFinishedUnix.Keys.Should().ContainSingle(k => k.Cluster == "c2" && k.Shard == "s1");
        d.FullMaxAge.Should().HaveCount(2); // max_age обоих кластеров жив
    }

    // AAA: t14 — uploaded-age хранится как unix-факт (now − age на момент
    // наблюдения — колбэк гейджа пересчитывает на каждом scrape); null удаляет
    [Fact]
    public void BackupWalUploadedAge_unixФакт_nullУдаляет()
    {
        // Arrange — часы на T=10_000
        var clock = new FakeTimeProvider { Now = DateTimeOffset.UnixEpoch.AddSeconds(10_000) };
        using var meter = new Meter("TestWorker");
        using var sut = new WorkerMetricsInstrumentation(meter, clock);

        // Act — наблюдение «сегмент загружен 42 с назад» → unix 9_958; затем снятие
        sut.BackupWalUploadedAge("c1", "s1", 42);
        sut.DebugSnapshot().WalUploadedUnix[("c1", "s1")].Should().Be(9_958);
        sut.BackupWalUploadedAge("c1", "s1", null);

        // Assert — серия исчезла
        sut.DebugSnapshot().WalUploadedUnix.Should().BeEmpty();
    }

    // AAA: t17 — единый марк-метод HaDurations: набор шардов тика замещает стейт
    // кластера целиком; null-факт — серия шарда исчезает (arch/18 §2.7)
    [Fact]
    public void HaDurations_ЗамещениеНабора_nullУбирает_СериюШардаИсчезает()
    {
        // Arrange
        using var meter = new Meter("test");
        var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);

        // Act: тик 1 — оба факта; тик 2 — failover закрылся (55с), rebuild ушёл (null)
        sut.HaDurations("c1", new Dictionary<string, (long?, long?)>
        {
            ["s1"] = (null, 40),
            ["s2"] = (10, null),
        });
        sut.HaDurations("c1", new Dictionary<string, (long?, long?)>
        {
            ["s1"] = (55, null),
        });

        // Assert: набор кластера перезаписан целиком; s2 исчез; null-факт не эмитится
        var state = sut.DebugSnapshot();
        state.HaFailoverDurations.Should().ContainKey(("c1", "s1")).WhoseValue.Should().Be(55);
        state.HaFailoverDurations.Should().NotContainKey(("c1", "s2"));
        state.HaRebuildDurations.Should().BeEmpty();
    }

    // AAA: t17 — кластеры независимы (failover/rebuild — независимые серии)
    [Fact]
    public void HaDurations_РазныеКластерыНезависимы()
    {
        // Arrange
        using var meter = new Meter("test");
        var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);

        // Act
        sut.HaDurations("c1", new Dictionary<string, (long?, long?)> { ["s1"] = (11, null) });
        sut.HaDurations("c2", new Dictionary<string, (long?, long?)> { ["s1"] = (null, 22) });

        // Assert
        var state = sut.DebugSnapshot();
        state.HaFailoverDurations[("c1", "s1")].Should().Be(11);
        state.HaRebuildDurations[("c2", "s1")].Should().Be(22);
    }

    [Fact]
    public void WatchdogRestart_CountsPerLoop()
    {
        // Arrange
        using var meter = new Meter("TestWorker");
        using var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);

        // Act: watchdog инициировал две остановки reconcile и одну keepalive
        sut.WatchdogRestart("reconcile");
        sut.WatchdogRestart("reconcile");
        sut.WatchdogRestart("keepalive");

        // Assert: counter per-loop (лейбл цикла), пассивность — без исключений
        sut.DebugSnapshot().WatchdogRestarts["reconcile"].Should().Be(2);
        sut.DebugSnapshot().WatchdogRestarts["keepalive"].Should().Be(1);
    }
}
