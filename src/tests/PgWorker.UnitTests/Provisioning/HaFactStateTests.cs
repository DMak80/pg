using PgWorker.Provisioning.Processes;
using Shared.Etcd.Coordination;

namespace PgWorker.UnitTests.Provisioning;

// HaFactState (arch/14 §5 C): чистые переходы HA-фактов — открытие/закрытие/
// флап-сброс/перезапись; без etcd и часов (времена приходят параметрами).
public class HaFactStateTests
{
    [Fact]
    public void FailoverDetected_ThenLeaderChanged_ClosesWithDuration()
    {
        // Arrange: открытие accelerated-факта
        var state = HaFactState.FromStored(null, null);

        // Act: открытие в тике детекции; лидер сменился на 75-й секунде
        state.FailoverDetected("s1", "n1", "accelerated", detectedUnix: 1000);
        state.LeaderChanged("s1", "n2", nowUnix: 1075);

        // Assert: закрытый факт с длительностью (окно детекции входит)
        var failover = state.ToRecord().LastFailover!;
        failover.Cause.Should().Be("accelerated");
        failover.DetectedUnix.Should().Be(1000);
        failover.ResolvedUnix.Should().Be(1075);
        failover.DurationSec.Should().Be(75);
    }

    [Fact]
    public void FailoverDetected_ElectionsCause_StaysOpen()
    {
        // Arrange/Act: ускорение не применено — промоушен ждёт Patroni
        var state = HaFactState.FromStored(null, null);
        state.FailoverDetected("s1", "n1", "elections", 1000);

        // Assert: открытое событие — без resolved/duration
        var failover = state.ToRecord().LastFailover!;
        failover.ResolvedUnix.Should().BeNull();
        failover.DurationSec.Should().BeNull();
    }

    [Fact]
    public void LeaderRecovered_SameLeader_DropsWithoutFixation()
    {
        // Arrange: открытый failover (транзиентный флап лидера)
        var state = HaFactState.FromStored(
            new HaSupervisionFact("s1", "n1", "accelerated", 1000), null);

        // Act: нода ожила, лидерство сохранила
        state.LeaderRecovered("s1", "n1", leader: "n1");

        // Assert: факта failover НЕ БЫЛО — запись удалена без фиксации
        state.ToRecord().LastFailover.Should().BeNull();
    }

    [Fact]
    public void LeaderChanged_SameLeader_DoesNotClose()
    {
        // Arrange: открытый failover, лидер всё ещё она (выборы идут)
        var state = HaFactState.FromStored(
            new HaSupervisionFact("s1", "n1", "elections", 1000), null);

        // Act: leader-ключ указывает на ту же ноду
        state.LeaderChanged("s1", "n1", nowUnix: 1100);

        // Assert: событие остаётся открытым
        state.ToRecord().LastFailover!.ResolvedUnix.Should().BeNull();
    }

    [Fact]
    public void FromStored_TakeoverContinues_FromDetected()
    {
        // Arrange: воркер упал в окне события — открытый факт в ключе
        var state = HaFactState.FromStored(
            new HaSupervisionFact("s2", "n9", "elections", 1000), null);

        // Act: takeover-инстанс видит смену лидера
        state.LeaderChanged("s2", "n3", nowUnix: 1300);

        // Assert: длительность от СОХРАНЁННОГО detected (не от рестарта)
        var failover = state.ToRecord().LastFailover!;
        failover.DurationSec.Should().Be(300);
    }

    [Fact]
    public void FailoverDetected_OtherShard_OverwritesLastFact()
    {
        // Arrange: закрытый факт шарда s1
        var state = HaFactState.FromStored(
            new HaSupervisionFact("s1", "n1", "elections", 1000, 1100, 100), null);

        // Act: новое событие на другом шарде (последний факт побеждает)
        state.FailoverDetected("s2", "n2", "accelerated", 2000);

        // Assert
        var failover = state.ToRecord().LastFailover!;
        failover.Shard.Should().Be("s2");
        failover.DetectedUnix.Should().Be(2000);
        failover.ResolvedUnix.Should().BeNull();
    }

    [Fact]
    public void FailoverDetected_SameShardNode_Ignored()
    {
        // Arrange/Act: повторная детекция того же события в следующем тике
        var state = HaFactState.FromStored(null, null);
        state.FailoverDetected("s1", "n1", "accelerated", 1000);
        state.FailoverDetected("s1", "n1", "elections", 1050);

        // Assert: detected первого тика, cause первоисточника
        var failover = state.ToRecord().LastFailover!;
        failover.DetectedUnix.Should().Be(1000);
        failover.Cause.Should().Be("accelerated");
    }

    [Fact]
    public void FailoverDetected_ClosedFactSameNode_OverwritesAsNewEvent()
    {
        // Arrange: закрытый failover той же ноды (уже было событие)
        var state = HaFactState.FromStored(null, null);
        state.FailoverDetected("s1", "n1", "accelerated", 1000);
        state.LeaderChanged("s1", "n2", nowUnix: 1075);

        // Act: нода СНОВА становится лидером и умирает — новое событие
        state.FailoverDetected("s1", "n1", "elections", 2000);

        // Assert: перезапись (spec §3.1 «новое событие перезаписывает поле»):
        // новый detected/cause, resolved сброшен — событие открыто
        var failover = state.ToRecord().LastFailover!;
        failover.Cause.Should().Be("elections");
        failover.DetectedUnix.Should().Be(2000);
        failover.ResolvedUnix.Should().BeNull();
        failover.DurationSec.Should().BeNull();
    }

    [Fact]
    public void RebuildDetected_ClosedFactSameNode_OverwritesAsNewEvent()
    {
        // Arrange: rebuild закрыт по NodeAlive — нода восстановилась
        var state = HaFactState.FromStored(null, null);
        state.RebuildDetected("s1", "n2", "auto-dead", 1000);
        state.NodeAlive("s1", "n2", nowUnix: 1150);

        // Act: нода умирает СНОВА — новый rebuild (иначе last_rebuild навсегда
        // показывает старое событие)
        state.RebuildDetected("s1", "n2", "operator-recreate", 3000);

        // Assert: новый detected/cause, resolved сброшен
        var rebuild = state.ToRecord().LastRebuild!;
        rebuild.Cause.Should().Be("operator-recreate");
        rebuild.DetectedUnix.Should().Be(3000);
        rebuild.ResolvedUnix.Should().BeNull();
        rebuild.DurationSec.Should().BeNull();
    }

    [Fact]
    public void RebuildDetected_ThenNodeAlive_Closes()
    {
        // Arrange/Act: rebuild auto-dead открыт, нода поднялась
        var state = HaFactState.FromStored(null, null);
        state.RebuildDetected("s1", "n2", "auto-dead", 1000);
        state.NodeAlive("s1", "n2", nowUnix: 1150);

        // Assert
        var rebuild = state.ToRecord().LastRebuild!;
        rebuild.ResolvedUnix.Should().Be(1150);
        rebuild.DurationSec.Should().Be(150);
    }

    [Fact]
    public void RebuildDetected_OperatorRecreate_DetectedFromMarkerTime()
    {
        // Arrange/Act: маркер TO_RECREATE живой ноды — трека нет, detected = момент исполнения
        var state = HaFactState.FromStored(null, null);
        state.RebuildDetected("s1", "n3", "operator-recreate", 5000);

        // Assert
        state.ToRecord().LastRebuild!.DetectedUnix.Should().Be(5000);
    }

    [Fact]
    public void NodeAlive_OtherShardOrClosed_DoesNothing()
    {
        // Arrange: открытый rebuild s1/n2; новое событие s3/n3 перезаписало
        // (последний факт побеждает) и закрылось первым живым тиком 2100
        var state = HaFactState.FromStored(null, null);
        state.RebuildDetected("s1", "n2", "auto-dead", 1000);
        state.RebuildDetected("s3", "n3", "auto-dead", 2000);
        state.NodeAlive("s3", "n3", 2100);

        // Act: живость чужой ноды и повторная живость закрытой
        state.NodeAlive("s2", "nX", 2200);
        state.NodeAlive("s3", "n3", 2300);

        // Assert: чужая нода ничего не меняет; s3/n3 не тронут (first-resolved:
        // повторная живость не перетёрла resolved/duration)
        var r = state.ToRecord();
        r.LastRebuild!.Shard.Should().Be("s3");
        r.LastRebuild.ResolvedUnix.Should().Be(2100);
        r.LastRebuild.DurationSec.Should().Be(100);
    }

    [Fact]
    public void Kinds_Independent()
    {
        // Arrange: умерший лидер — возможны ОБА факта на одном шарде
        var state = HaFactState.FromStored(null, null);

        // Act: failover (смена лидера) + rebuild (пересоздание ноды)
        state.FailoverDetected("s1", "n1", "accelerated", 1000);
        state.RebuildDetected("s1", "n1", "auto-dead", 1000);

        // Assert: поля независимы
        var r = state.ToRecord();
        r.LastFailover.Should().NotBeNull();
        r.LastRebuild.Should().NotBeNull();
    }
}
